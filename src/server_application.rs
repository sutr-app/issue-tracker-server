use super::*;

#[path = "server_application/maintenance.rs"]
mod maintenance;
#[path = "server_application/provenance.rs"]
mod provenance;

impl IssueTrackerApplication {
    pub(crate) async fn open(mut settings: ServerSettings) -> Result<Self, String> {
        normalize_runner_settings(&mut settings.tracker)?;
        let config = validate_runner_settings(&settings.tracker)?;
        let manager_config = settings
            .tracker
            .manager_settings
            .as_ref()
            .map(|value| manager::manager_config_for_tracker(value, config.data_root()))
            .transpose()?;
        // Validate both persisted roots before either store is opened
        // read-write. This prevents an incompatible Manager root from
        // initializing Tracker metadata as a side effect of startup.
        if let Some(manager_config) = manager_config.as_ref() {
            IssueTrackerManager::preflight(manager_config).map_err(|error| error.to_string())?;
        }
        let tracker = Tracker::open_with_source_evidence_limits(
            &config.data_root,
            config.source_evidence_limits,
        )
        .await
        .map_err(|error| error.to_string())?;
        let manager = manager_config
            .map(IssueTrackerManager::open)
            .transpose()
            .map_err(|error| error.to_string())?
            .map(|value| Arc::new(Mutex::new(value)));
        let driver = match manager.as_ref() {
            Some(manager) => Some(Arc::new(
                OutboxDriver::new(
                    Arc::clone(manager),
                    tracker.clone(),
                    config.clone(),
                    #[cfg(test)]
                    false,
                )
                .await,
            )),
            None => None,
        };
        if let Some(driver) = driver.as_ref() {
            // Normalize permits left by a previous process before the server
            // accepts requests.  The resident loop repeats this check with
            // its live-task registry, so cancelled in-process sends converge
            // without treating a still-running task as unknown.
            // A single corrupt operation is an item-level diagnosis, not a
            // root-opening failure. Keep the stores available, suppress new
            // durable permits through the persisted health flag, and let the
            // resident cycle retry the affected phase independently.
            let admin_recovery_failed = driver.recover_inflight_admin_dispatches().await.is_err();
            let capability_recovery_failed = driver
                .recover_inflight_capability_dispatches(100)
                .await
                .is_err();
            if admin_recovery_failed || capability_recovery_failed {
                driver.mark_unhealthy().await;
            }
        }
        let ownership_maintenance_interval_millis = config.ownership_maintenance_interval_millis;
        let ownership_recovery_batch_size = config.ownership_recovery_batch_size;
        Ok(Self {
            core: AppModule::new(tracker, config),
            manager,
            driver,
            maintenance: settings.maintenance,
            ownership_maintenance_interval_millis,
            ownership_recovery_batch_size,
        })
    }

    pub(crate) async fn dispatch_v1<A, R>(&self, method: &str, request: A) -> Result<R, String>
    where
        A: Message,
        R: Message + Default,
    {
        self.core.dispatch_v1(method, request).await
    }

    pub(crate) async fn dispatch_manager<A, R>(&self, method: &str, request: A) -> Result<R, String>
    where
        A: Message,
        R: Message + Default,
    {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        let response = match method {
            "manager_resolve_candidate" => {
                let args =
                    manager_v1::ResolveCandidateArgs::decode(request.encode_to_vec().as_slice())
                        .map_err(|error| error.to_string())?;
                let args = match self.prepare_confirm_source_evidence(args).await {
                    Ok(args) => args,
                    Err(error) => {
                        return R::decode(
                            manager_v1::ResolveCandidateResult {
                                outcome: Some(
                                    manager_v1::resolve_candidate_result::Outcome::Error(error),
                                ),
                            }
                            .encode_to_vec()
                            .as_slice(),
                        )
                        .map_err(|error| error.to_string());
                    }
                };
                let args = match self.prepare_issue_merge_provenance(args).await {
                    Ok(args) => args,
                    Err(error) => {
                        return R::decode(
                            manager_v1::ResolveCandidateResult {
                                outcome: Some(
                                    manager_v1::resolve_candidate_result::Outcome::Error(error),
                                ),
                            }
                            .encode_to_vec()
                            .as_slice(),
                        )
                        .map_err(|error| error.to_string());
                    }
                };
                return self.dispatch_manager_via_driver(method, args).await;
            }
            "manager_create_issue" => {
                let args = manager_v1::CreateIssueArgs::decode(request.encode_to_vec().as_slice())
                    .map_err(|error| error.to_string())?;
                let args = match self.prepare_create_issue_provenance(args).await {
                    Ok(args) => args,
                    Err(error) => {
                        return R::decode(
                            manager_v1::CreateIssueResult {
                                outcome: Some(manager_v1::create_issue_result::Outcome::Error(
                                    error,
                                )),
                            }
                            .encode_to_vec()
                            .as_slice(),
                        )
                        .map_err(|error| error.to_string());
                    }
                };
                return self.dispatch_manager_via_driver(method, args).await;
            }
            "manager_get_contract_info" => {
                let mut manager = manager.lock().await;
                let args =
                    manager_v1::GetContractInfoArgs::decode(request.encode_to_vec().as_slice())
                        .map_err(|error| error.to_string())?;
                let context = args.context.clone().unwrap_or_default();
                let args = if context.schema_fingerprint.is_empty()
                    && context.data_root_id.is_empty()
                    && context.contract_fingerprint.is_empty()
                {
                    manager_v1::GetContractInfoArgs {
                        context: Some(manager.runner_read_context(context.correlation_id)),
                    }
                } else {
                    args
                };
                manager.dispatch_manager(method, &args.encode_to_vec())?
            }
            "manager_get_issue_with_sync"
            | "manager_get_issue_relations"
            | "manager_list_issues_with_sync" => {
                let prepared = {
                    manager
                        .lock()
                        .await
                        .prepare_canonical_read(method, &request.encode_to_vec())
                };
                match prepared {
                    Ok(prepared) => {
                        manager::dispatch_prepared_canonical_read(self.core.tracker(), prepared)
                            .await
                    }
                    Err(result) => result,
                }
            }
            "manager_list_review_candidates" => {
                let prepared = {
                    manager
                        .lock()
                        .await
                        .prepare_review_candidate_read(&request.encode_to_vec())
                };
                match prepared {
                    Ok(prepared) => {
                        manager::dispatch_review_candidate_read(self.core.tracker(), prepared).await
                    }
                    Err(result) => result,
                }
            }
            "manager_list_ownership_diagnostics" => {
                let args = manager_v1::ListOwnershipDiagnosticsArgs::decode(
                    request.encode_to_vec().as_slice(),
                )
                .map_err(|error| error.to_string())?;
                let context = args.context.clone().unwrap_or_default();
                let mut manager = manager.lock().await;
                let identity = manager.config().manager_identity();
                if context.schema_fingerprint != identity.schema_fingerprint
                    || context.contract_fingerprint != identity.contract_fingerprint
                    || context.data_root_id != identity.data_root_id
                {
                    manager_v1::ListOwnershipDiagnosticsResult {
                        outcome: Some(
                            manager_v1::list_ownership_diagnostics_result::Outcome::Error(
                                manager_v1::ManagerError {
                                    code: manager_v1::ManagerErrorCode::MethodTargetMismatch as i32,
                                    message: "invalid trusted-consumer diagnostic context".into(),
                                    retryable: false,
                                    detail: None,
                                },
                            ),
                        ),
                    }
                    .encode_to_vec()
                } else {
                    let limits = manager.config().page_size_limits();
                    let requested = args.page.as_ref().map(|page| page.page_size).unwrap_or(0);
                    let page_size = limits.resolve(requested).min(100);
                    let generation = manager.storage().generation();
                    let snapshot_id = format!("ownership-diagnostics-{generation}");
                    let order_fingerprint = "target-id/class/evidence/generation";
                    let query_fingerprint = digest_bytes(
                        format!(
                            "{}\0{}",
                            args.target_kind.as_deref().unwrap_or_default(),
                            args.target_id.as_deref().unwrap_or_default()
                        )
                        .as_bytes(),
                    );
                    let codec = crate::manager::pagination::PageTokenCodec::new(
                        manager.storage().list_issues_with_sync_page_token_secret(),
                    );
                    let cursor = match args
                        .page
                        .as_ref()
                        .and_then(|page| page.page_token.as_deref())
                    {
                        Some(token) => match codec.decode(
                            token,
                            generation,
                            &snapshot_id,
                            order_fingerprint,
                            &query_fingerprint,
                            unix_now_millis(),
                        ) {
                            Ok(cursor) => Some(cursor),
                            Err(_) => {
                                return R::decode(
                                    manager_v1::ListOwnershipDiagnosticsResult {
                                        outcome: Some(
                                            manager_v1::list_ownership_diagnostics_result::Outcome::Error(
                                                manager_v1::ManagerError {
                                                    code: manager_v1::ManagerErrorCode::InvalidPageToken
                                                        as i32,
                                                    message: "invalid or expired diagnostics page token".into(),
                                                    retryable: false,
                                                    detail: None,
                                                },
                                            ),
                                        ),
                                    }
                                    .encode_to_vec()
                                    .as_slice(),
                                )
                                .map_err(|error| error.to_string());
                            }
                        },
                        None => None,
                    };
                    let offset = cursor.as_ref().map_or(0, |cursor| cursor.offset);
                    let diagnoses = manager
                        .storage_mut()
                        .repository()
                        .list_ownership_integrity_diagnoses(
                            args.target_kind.as_deref(),
                            args.target_id.as_deref(),
                            page_size,
                            offset,
                        )
                        .map_err(|error| error.to_string())?;
                    let next_page_token = (diagnoses.len() == page_size).then(|| {
                        codec.encode(&crate::manager::pagination::PageCursor {
                            offset: offset.saturating_add(diagnoses.len()),
                            generation,
                            snapshot_id: snapshot_id.clone(),
                            order_fingerprint: order_fingerprint.into(),
                            query_fingerprint: query_fingerprint.clone(),
                            expires_at_unix_epoch_millis: i64::MAX,
                        })
                    });
                    manager_v1::ListOwnershipDiagnosticsResult {
                        outcome: Some(
                            manager_v1::list_ownership_diagnostics_result::Outcome::Success(
                                manager_v1::OwnershipDiagnosticsPage {
                                    diagnoses,
                                    page: Some(manager_v1::PageInfo {
                                        next_page_token,
                                        total_size: None,
                                        read_revision: generation,
                                        snapshot: Some(manager_v1::PageSnapshot {
                                            read_revision_watermark: generation,
                                            snapshot_id: format!(
                                                "ownership-diagnostics-{generation}"
                                            ),
                                            stable_traversal: Some(manager_v1::StableTraversal {
                                                order_fingerprint:
                                                    "target-id/class/evidence/generation".into(),
                                            }),
                                            read_generation: generation,
                                        }),
                                    }),
                                },
                            ),
                        ),
                    }
                    .encode_to_vec()
                }
            }
            _ => return self.dispatch_manager_via_driver(method, request).await,
        };
        R::decode(response.as_slice()).map_err(|error| error.to_string())
    }

    pub(super) async fn dispatch_manager_via_driver<A, R>(
        &self,
        method: &str,
        request: A,
    ) -> Result<R, String>
    where
        A: Message,
        R: Message + Default,
    {
        let admitted = self.admit_manager_backend(method, request).await?;
        self.complete_manager_backend(method, admitted).await
    }

    /// Admit a durable backend mutation without waiting for its delivery.
    ///
    /// Callers that need to persist a sidecar relation keyed by the durable
    /// operation ID must do so between admission and completion.  Keeping the
    /// boundary here makes that ordering available without duplicating driver
    /// dispatch logic in domain services.
    pub(crate) async fn admit_manager_backend<A>(
        &self,
        method: &str,
        request: A,
    ) -> Result<Vec<u8>, String>
    where
        A: Message,
    {
        let driver = self
            .driver
            .as_ref()
            .ok_or("manager outbox driver is unavailable")?;
        driver.dispatch(method, &request.encode_to_vec()).await
    }

    /// Complete a previously admitted durable backend mutation.
    pub(crate) async fn complete_manager_backend<R>(
        &self,
        method: &str,
        admitted: Vec<u8>,
    ) -> Result<R, String>
    where
        R: Message + Default,
    {
        let driver = self
            .driver
            .as_ref()
            .ok_or("manager outbox driver is unavailable")?;
        let response = driver.complete_admitted(method, admitted).await?;
        R::decode(response.as_slice()).map_err(|error| error.to_string())
    }

    pub(crate) async fn dispatch_manager_for_gateway<A, R>(
        &self,
        gateway_id: &str,
        method: &str,
        request: A,
    ) -> Result<R, String>
    where
        A: Message,
        R: Message + Default,
    {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        if method == "manager_gateway_list_ready" {
            let args =
                manager_v1::ListIssuesWithSyncArgs::decode(request.encode_to_vec().as_slice())
                    .map_err(|error| error.to_string())?;
            return self.dispatch_gateway_ready_list(gateway_id, args).await;
        }
        if method == "manager_gateway_claim_issue" {
            let args = manager_v1::ClaimIssueArgs::decode(request.encode_to_vec().as_slice())
                .map_err(|error| error.to_string())?;
            return self.dispatch_gateway_claim(gateway_id, args).await;
        }
        if method == "manager_get_issue_with_sync" {
            return self.dispatch_manager(method, request).await;
        }
        let payload = request.encode_to_vec();
        if manager::requires_capability_dispatch(method, &payload) {
            return self.dispatch_manager_via_driver(method, request).await;
        }
        let response = manager
            .lock()
            .await
            .dispatch_manager_for_gateway(gateway_id, method, &payload)?;
        R::decode(response.as_slice()).map_err(|error| error.to_string())
    }

    pub(super) async fn dispatch_gateway_ready_list<R>(
        &self,
        gateway_id: &str,
        args: manager_v1::ListIssuesWithSyncArgs,
    ) -> Result<R, String>
    where
        R: Message + Default,
    {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        let page_token_secret = manager
            .lock()
            .await
            .storage_mut()
            .list_issues_with_sync_page_token_secret()
            .to_owned();
        let codec = manager::pagination::PageTokenCodec::new(page_token_secret.clone());
        let supplied_cursor = args
            .page
            .as_ref()
            .and_then(|page| page.page_token.as_deref())
            .map(|token| decode_gateway_ready_page_token(&codec, token))
            .transpose();
        let supplied_cursor = match supplied_cursor {
            Ok(cursor) => cursor,
            Err(error) => return decode_gateway_ready_error(error),
        };
        let tracker_generation = self.core.tracker().current_read_generation();
        let issues = self
            .core
            .tracker()
            .list()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|issue| crate::tracker::conversion::issue_proto(&issue))
            .collect::<Vec<_>>();
        if let Err(error) = self
            .core
            .tracker()
            .validate_read_generation(tracker_generation)
        {
            return decode_gateway_ready_error(error);
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis() as i64;
        let eligibility_evaluated_at_millis = supplied_cursor
            .as_ref()
            .map(|cursor| {
                GatewayReadySnapshot::decode(&cursor.snapshot_id)
                    .map(|snapshot| snapshot.eligibility_evaluated_at_millis)
            })
            .transpose();
        let eligibility_evaluated_at_millis = match eligibility_evaluated_at_millis {
            Ok(Some(value)) => value,
            Ok(None) => now,
            Err(error) => return decode_gateway_ready_error(error),
        };
        let (generation, page_size_limits, selection) = {
            let mut manager = manager.lock().await;
            let page_size_limits = manager.config().page_size_limits();
            let storage = manager.storage_mut();
            let generation = storage.generation();
            let selection = storage.repository().list_gateway_ready_issues(
                gateway_id,
                issues,
                eligibility_evaluated_at_millis,
            );
            (generation, page_size_limits, selection)
        };
        let response = match selection {
            Ok(selection) => {
                let binding =
                    GatewayReadyPageBinding::new(generation, tracker_generation, &selection);
                gateway_ready_page_with_limits(
                    &args,
                    selection,
                    page_token_secret,
                    gateway_id,
                    binding,
                    page_size_limits,
                )
            }
            Err(error) => manager_v1::ListIssuesWithSyncResult {
                outcome: Some(manager_v1::list_issues_with_sync_result::Outcome::Error(
                    manager_v1::ManagerError {
                        code: manager_v1::ManagerErrorCode::BackendUnavailable as i32,
                        message: error.to_string(),
                        retryable: true,
                        detail: None,
                    },
                )),
            }
            .encode_to_vec(),
        };
        R::decode(response.as_slice()).map_err(|error| error.to_string())
    }

    pub(super) async fn dispatch_gateway_claim<R>(
        &self,
        gateway_id: &str,
        mut args: manager_v1::ClaimIssueArgs,
    ) -> Result<R, String>
    where
        R: Message + Default,
    {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        if !manager.lock().await.write_admission_available() {
            let response = manager::manager_error_result(
                "manager_claim_issue",
                manager_v1::ManagerError {
                    code: manager_v1::ManagerErrorCode::BackendUnavailable as i32,
                    message: "manager write admission is blocked while ownership maintenance is unhealthy".into(),
                    retryable: true,
                    detail: None,
                },
            );
            return R::decode(response.as_slice()).map_err(|error| error.to_string());
        }
        let issue_id = uuid::Uuid::parse_str(&args.issue_id)
            .map_err(|_| "gateway claim issue_id is invalid".to_owned())?;
        let issue = crate::tracker::conversion::issue_proto(
            &self
                .core
                .tracker()
                .get(issue_id)
                .await
                .map_err(|error| error.to_string())?,
        );
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis() as i64;
        let (policy_generation, policy_result) = {
            let mut manager = manager.lock().await;
            let storage = manager.storage_mut();
            let generation = storage.generation();
            let policy_result = storage
                .repository()
                .claim_gateway_ready_issue(gateway_id, &issue, now);
            (generation, policy_result)
        };
        if let Err(error) = policy_result {
            let code = match error {
                manager::repository::GatewayReadinessError::Unregistered => {
                    manager_v1::ManagerErrorCode::BackendUnavailable
                }
                manager::repository::GatewayReadinessError::Manager(_) => {
                    manager_v1::ManagerErrorCode::PreconditionFailed
                }
                manager::repository::GatewayReadinessError::Repository(_) => {
                    manager_v1::ManagerErrorCode::BackendUnavailable
                }
            };
            let response = manager::manager_error_result(
                "manager_claim_issue",
                manager_v1::ManagerError {
                    code: code as i32,
                    message: error.to_string(),
                    retryable: false,
                    detail: None,
                },
            );
            return R::decode(response.as_slice()).map_err(|error| error.to_string());
        }
        if let Some(context) = args.context.as_mut() {
            context.method = "manager_claim_issue".into();
        }
        args.gateway_eligibility_generation = Some(policy_generation);
        self.dispatch_manager("manager_claim_issue", args).await
    }

    /// Starts one process-local capability epoch for a worker listener.
    pub(crate) async fn start_gateway_capability_epoch(
        &self,
        gateway_id: &str,
    ) -> Result<String, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .start_gateway_capability_epoch(gateway_id)
            .map_err(|error| error.to_string())
    }

    /// Durably reserves an idempotency key before a Gateway write is admitted.
    pub(crate) async fn reserve_gateway_replay(
        &self,
        gateway_id: &str,
        capability_epoch: &str,
        idempotency_key: &str,
        method: &str,
        request_digest: &str,
    ) -> Result<GatewayReplayDecision, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .reserve_gateway_replay(
                gateway_id,
                capability_epoch,
                idempotency_key,
                method,
                request_digest,
            )
            .map_err(|error| error.to_string())
    }

    /// Completes the successful response snapshot for a Gateway write.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn complete_gateway_replay(
        &self,
        gateway_id: &str,
        capability_epoch: &str,
        idempotency_key: &str,
        method: &str,
        request_digest: &str,
        response_schema: &str,
        response_snapshot: &[u8],
    ) -> Result<(), String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .complete_gateway_replay(
                gateway_id,
                capability_epoch,
                idempotency_key,
                method,
                request_digest,
                response_schema,
                response_snapshot,
            )
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn load_gateway_policy(
        &self,
        gateway_id: &str,
    ) -> Result<Option<manager_v1::GatewayRegistration>, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .load_gateway_policy(gateway_id)
            .map_err(|error| error.to_string())
    }

    /// Completes a reserved Gateway replay whose canonical commit was proven
    /// independently of the capability epoch that reserved it.
    pub(crate) async fn complete_gateway_replay_from_canonical(
        &self,
        gateway_id: &str,
        idempotency_key: &str,
        method: &str,
        request_digest: &str,
        response_schema: &str,
        response_snapshot: &[u8],
    ) -> Result<(), String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .complete_gateway_replay_from_canonical(
                gateway_id,
                idempotency_key,
                method,
                request_digest,
                response_schema,
                response_snapshot,
            )
            .map_err(|error| error.to_string())
    }

    /// Drops a replay reservation only when the Gateway has a definitive
    /// non-success outcome and no response snapshot was committed.
    pub(crate) async fn discard_gateway_replay_reservation(
        &self,
        gateway_id: &str,
        capability_epoch: &str,
        idempotency_key: &str,
        method: &str,
        request_digest: &str,
    ) -> Result<(), String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .discard_gateway_replay_reservation(
                gateway_id,
                capability_epoch,
                idempotency_key,
                method,
                request_digest,
            )
            .map_err(|error| error.to_string())
    }

    pub(super) async fn mark_maintenance_unhealthy(&self) {
        if let Some(manager) = &self.manager {
            let _ = manager.lock().await.set_ownership_maintenance_health(false);
        }
        if let Some(driver) = &self.driver {
            driver.mark_unhealthy().await;
        }
    }

    /// Reconcile at most one bounded page of leases.  This is deliberately
    /// independent from ready-list reads: a read must never repair canonical
    /// state behind a worker's back.
    #[allow(dead_code)]
    pub(crate) async fn run_ownership_maintenance(&self) -> Result<u32, String> {
        self.run_ownership_maintenance_stats()
            .await
            .map(|stats| stats.recovered)
    }

    pub(crate) async fn run_ownership_maintenance_stats(
        &self,
    ) -> Result<crate::tracker::ownership_maintenance::MaintenanceStats, String> {
        if let Some(driver) = self.driver.as_ref() {
            crate::tracker::ownership_maintenance::run_with_recovery_stats(
                self.core.tracker(),
                self.manager.as_ref(),
                self.ownership_recovery_batch_size,
                &|issue_id, revision, operation_id, state, reason| {
                    driver.recover_ownership_via_outbox(
                        issue_id,
                        revision,
                        operation_id,
                        state,
                        reason,
                    )
                },
            )
            .await
        } else if self.manager.is_some() {
            Err("manager-backed ownership maintenance requires the outbox driver".into())
        } else {
            crate::tracker::ownership_maintenance::run_with_recovery_stats(
                self.core.tracker(),
                None,
                self.ownership_recovery_batch_size,
                &|issue_id, revision, operation_id, _state, reason| async move {
                    self.core
                        .tracker()
                        .recover_claim(
                            issue_id,
                            revision,
                            false,
                            &operation_id,
                            "system-maintenance",
                            reason.as_str(),
                        )
                        .await
                        .map(|_| ())
                },
            )
            .await
        }
    }

    /// Resume manual-resolution requests that were durably admitted before a
    /// process stop.  The exact request bytes are replayed through the same
    /// trusted-consumer implementation; no decision is reconstructed from the
    /// mutable Issue. Requests that are still genuinely unconfirmed remain
    /// pending and keep the persisted health signal degraded for operator
    /// follow-up.
    pub(super) async fn recover_pending_resolutions(
        self: &Arc<Self>,
        limit: usize,
    ) -> Result<u32, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        let cursor = manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .resolution_recovery_cursor()
            .map_err(|error| error.to_string())?;
        let pending = manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .pending_resolution_requests_after(limit, cursor.as_deref())
            .map_err(|error| error.to_string())?;
        if pending.is_empty() {
            return Ok(0);
        }
        let recovery_server = GrpcServer::from_app_arc(Arc::clone(self));
        let mut recovered = 0_u32;
        let mut first_error = None;
        // Advance the cursor only after a row has been evaluated.  A process
        // stop between the page read and evaluation therefore replays the
        // same request safely, while a permanently corrupt row cannot starve
        // later requests on the next cycle.
        macro_rules! checkpoint_resolution {
            ($operation_id:expr) => {
                if let Err(error) = manager
                    .lock()
                    .await
                    .storage_mut()
                    .repository()
                    .set_resolution_recovery_cursor($operation_id)
                {
                    first_error.get_or_insert_with(|| error.to_string());
                }
            };
        }
        for request in pending {
            let args = match manager_v1::ResolveIssueOperationArgs::decode(
                request.request_payload.as_slice(),
            ) {
                Ok(args) => args,
                Err(error) => {
                    let message = format!(
                        "pending resolution {} is corrupt: {error}",
                        request.resolution_operation_id
                    );
                    match manager
                        .lock()
                        .await
                        .storage_mut()
                        .repository()
                        .finalize_resolution_failure(
                            &request.resolution_operation_id,
                            &request.request_digest,
                            manager_v1::ManagerErrorCode::ResolutionRequired,
                            &message,
                        ) {
                        Ok(_) => recovered = recovered.saturating_add(1),
                        Err(finalize_error) => {
                            first_error.get_or_insert(finalize_error.to_string());
                        }
                    }
                    checkpoint_resolution!(&request.resolution_operation_id);
                    continue;
                }
            };
            if digest_bytes(&request.request_payload) != request.request_digest
                || args.operation_id != request.source_operation_id
                || args.issue_id != request.issue_id
            {
                let message = format!(
                    "pending resolution {} failed integrity validation",
                    request.resolution_operation_id
                );
                match manager
                    .lock()
                    .await
                    .storage_mut()
                    .repository()
                    .finalize_resolution_failure(
                        &request.resolution_operation_id,
                        &request.request_digest,
                        manager_v1::ManagerErrorCode::ResolutionRequired,
                        &message,
                    ) {
                    Ok(_) => recovered = recovered.saturating_add(1),
                    Err(finalize_error) => {
                        first_error.get_or_insert(finalize_error.to_string());
                    }
                }
                checkpoint_resolution!(&request.resolution_operation_id);
                continue;
            }
            match recovery_server
                .resolve_issue_operation(Request::new(args))
                .await
            {
                Ok(response) => {
                    let terminal = match response.get_ref().outcome.as_ref() {
                        Some(manager_v1::resolve_issue_operation_result::Outcome::Success(_)) => {
                            true
                        }
                        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error)) => {
                            let code = manager_v1::ManagerErrorCode::try_from(error.code)
                                .unwrap_or(manager_v1::ManagerErrorCode::ResultUnconfirmed);
                            if matches!(code, manager_v1::ManagerErrorCode::ResultUnconfirmed) {
                                false
                            } else {
                                let already_terminal = manager
                                    .lock()
                                    .await
                                    .storage_mut()
                                    .repository()
                                    .get_operation(&request.resolution_operation_id)
                                    .ok()
                                    .flatten()
                                    .is_some_and(|operation| {
                                        matches!(
                                            manager_v1::DurableOperationState::try_from(
                                                operation.state
                                            ),
                                            Ok(manager_v1::DurableOperationState::Committed
                                                | manager_v1::DurableOperationState::Failed)
                                        )
                                    });
                                if already_terminal {
                                    true
                                } else {
                                    // Context/precondition failures can be
                                    // returned before the resolver reaches its
                                    // normal finalizer (for example after a root
                                    // identity change). Close the admitted
                                    // resolution instead of counting a response
                                    // while leaving its request permanently
                                    // PENDING.
                                    match manager
                                        .lock()
                                        .await
                                        .storage_mut()
                                        .repository()
                                        .finalize_resolution_failure(
                                            &request.resolution_operation_id,
                                            &request.request_digest,
                                            code,
                                            &error.message,
                                        ) {
                                        Ok(_) => true,
                                        Err(finalize_error) => {
                                            first_error
                                                .get_or_insert_with(|| finalize_error.to_string());
                                            false
                                        }
                                    }
                                }
                            }
                        }
                        None => false,
                    };
                    if terminal {
                        recovered = recovered.saturating_add(1);
                    }
                }
                Err(error) => {
                    // A missing marker, revision drift, or a temporary
                    // storage failure is retained as pending. It is surfaced
                    // through the persisted health check below and retried on
                    // the next resident cycle.
                    first_error.get_or_insert_with(|| error.to_string());
                }
            }
            checkpoint_resolution!(&request.resolution_operation_id);
        }
        if let Some(error) = first_error {
            // The caller has already completed every independent phase.  By
            // returning the first per-request error now, the cycle records a
            // degraded health state while still allowing unrelated delivery
            // and ownership scanning to finish in the same pass.
            return Err(error);
        }
        Ok(recovered)
    }

    /// Entry point used by the static runner, which owns an OutboxDriver but
    /// does not expose a gRPC service instance. Resolution itself only needs
    /// the already-open Tracker and Manager stores, so a short-lived façade
    /// can safely reuse the exact trusted-consumer implementation without
    /// opening another root or creating another resident task.
    #[allow(dead_code)]
    pub(crate) async fn recover_pending_resolutions_from_resources(
        tracker: Tracker,
        config: crate::tracker::config::RunnerConfig,
        manager: Arc<Mutex<IssueTrackerManager>>,
        limit: usize,
    ) -> Result<u32, String> {
        let ownership_maintenance_interval_millis = config.ownership_maintenance_interval_millis;
        let ownership_recovery_batch_size = config.ownership_recovery_batch_size;
        let app = Arc::new(IssueTrackerApplication {
            core: AppModule::new(tracker, config),
            manager: Some(manager),
            driver: None,
            maintenance: MaintenanceSettings::default(),
            ownership_maintenance_interval_millis,
            ownership_recovery_batch_size,
        });
        app.recover_pending_resolutions(limit).await
    }

    pub(crate) async fn manager_store_context(
        &self,
    ) -> Result<manager_v1::ManagerReadContext, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        Ok(manager
            .lock()
            .await
            .runner_read_context(uuid::Uuid::new_v4().to_string()))
    }

    pub(crate) async fn verify_awaiting_review_commit(
        &self,
        issue_id: uuid::Uuid,
        expected_revision: u64,
        actor: &str,
        gateway_request_digest: &str,
        completion: &v1::AwaitingReviewCompletion,
    ) -> Result<bool, String> {
        self.core
            .tracker()
            .verify_awaiting_review_commit(
                issue_id,
                expected_revision,
                actor,
                gateway_request_digest,
                completion,
            )
            .await
            .map_err(|error| error.to_string())
    }

    pub(super) async fn admin_marker_expectation(
        &self,
        operation_id: &str,
    ) -> Result<MarkerExpectation, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .backend_marker_expectation(operation_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "administrative marker expectation is missing".into())
    }

    pub(super) async fn recover_pending_admin_actions(&self, limit: usize) -> Result<u32, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        let pending = manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .pending_admin_request_snapshots(limit)
            .map_err(|error| error.to_string())?;
        let mut recovered = 0;
        let mut first_error = None;
        for (operation_id, bytes) in pending {
            let args = match manager_v1::ApplyIssueAdminActionArgs::decode(bytes.as_slice()) {
                Ok(args) => args,
                Err(_) => {
                    match manager::backend_dispatch::DurableBackendRequest::decode(bytes.as_slice())
                    {
                        Ok(envelope) => match envelope.admin_action.and_then(|value| value.args) {
                            Some(args) => args,
                            None => {
                                first_error.get_or_insert_with(|| {
                                    format!("administrative request {operation_id} has no action payload")
                                });
                                continue;
                            }
                        },
                        Err(error) => {
                            first_error.get_or_insert_with(|| {
                                format!("administrative request {operation_id} is corrupt: {error}")
                            });
                            continue;
                        }
                    }
                }
            };
            let request_digest = digest_bytes(&args.encode_to_vec());
            let Some(context) = args.context.as_ref() else {
                first_error.get_or_insert_with(|| {
                    format!("administrative request {operation_id} has no context")
                });
                continue;
            };
            let Ok(issue_id) = uuid::Uuid::parse_str(&args.issue_id) else {
                first_error.get_or_insert_with(|| {
                    format!("administrative request {operation_id} has an invalid issue ID")
                });
                continue;
            };
            let scope = match IdempotencyScope::new(
                context.contract_fingerprint.clone(),
                context.data_root_id.clone(),
                "manager_apply_issue_admin_action",
                context.target.clone(),
                context.idempotency_key.clone(),
            ) {
                Ok(scope) => scope,
                Err(error) => {
                    first_error.get_or_insert_with(|| {
                        format!(
                            "administrative request {operation_id} has an invalid scope: {error}"
                        )
                    });
                    continue;
                }
            };
            let marker = match self
                .core
                .tracker()
                .get_operation_record(&operation_id)
                .await
            {
                Ok(marker) => Some(marker),
                Err(TrackerError::NotFound(_)) => None,
                Err(error) => {
                    first_error.get_or_insert(error.to_string());
                    continue;
                }
            };
            let marker = if let Some(marker) = marker {
                marker
            } else {
                // A newly admitted request has a READY private outbox entry;
                // leave its first execution to the common dispatcher below
                // instead of bypassing the attempt/fence protocol here.
                let outbox = match manager.lock().await.recovery_outbox_entry(&operation_id) {
                    Ok(outbox) => outbox,
                    Err(error) => {
                        first_error.get_or_insert(error.to_string());
                        continue;
                    }
                };
                if let Some(outbox) = outbox {
                    // An acknowledged row with no marker means the southbound
                    // call ended at an unknown point.  Leaving the operation
                    // RECEIVED would make the private outbox permanently
                    // invisible to both normal delivery and startup
                    // recovery.  Close the outbox and fence the operation as
                    // UNCONFIRMED in one Manager transaction; a later marker
                    // reconciliation or human resolution can settle it.
                    if outbox.status == manager_v1::OutboxStatus::Acknowledged as i32
                        && let Some(mut operation) = manager
                            .lock()
                            .await
                            .storage_mut()
                            .repository()
                            .get_operation(&operation_id)
                            .ok()
                            .flatten()
                    {
                        operation.state = manager_v1::DurableOperationState::Unconfirmed as i32;
                        operation.resolution_required = true;
                        let Some(generation) = operation.generation.checked_add(1) else {
                            first_error.get_or_insert_with(|| {
                                format!(
                                    "administrative operation {operation_id} generation overflow"
                                )
                            });
                            continue;
                        };
                        operation.generation = generation;
                        let mut manager = manager.lock().await;
                        if let Err(error) = manager
                            .storage_mut()
                            .repository()
                            .finalize_admin_backend_dispatch(&operation, &outbox, &[])
                        {
                            first_error.get_or_insert(error.to_string());
                        }
                    }
                    if outbox.status == manager_v1::OutboxStatus::Acknowledged as i32 {
                        // Keep the row visible as degraded if the operation
                        // payload disappeared or the atomic finalizer lost a
                        // storage race. The next cycle can retry reconciliation.
                        let operation_exists = manager
                            .lock()
                            .await
                            .storage_mut()
                            .repository()
                            .get_operation(&operation_id)
                            .ok()
                            .flatten()
                            .is_some();
                        if !operation_exists {
                            first_error.get_or_insert_with(|| {
                                format!("administrative operation {operation_id} is missing")
                            });
                        }
                    }
                    continue;
                }
                // The original process stopped before Tracker commit. Re-run
                // the saved, idempotent request under the same operation ID.
                match self
                    .execute_saved_admin_action(&args, issue_id, &operation_id)
                    .await
                {
                    Ok(_) => match self
                        .core
                        .tracker()
                        .get_operation_record(&operation_id)
                        .await
                    {
                        Ok(marker) => marker,
                        // A successful call without a marker cannot prove its
                        // immutable result. Do not derive a receipt from the
                        // current Issue; fence this operation as unknown so
                        // maintenance cannot execute it again.
                        Err(_) => {
                            let _ = self
                                .mark_admin_action_unconfirmed(
                                    &scope,
                                    &request_digest,
                                    &operation_id,
                                )
                                .await;
                            first_error.get_or_insert_with(|| {
                                format!("administrative operation {operation_id} committed without a marker")
                            });
                            continue;
                        }
                    },
                    Err(error) if admin_action_rejected_before_mutation(&error) => {
                        if let Err(finalize_error) = self
                            .fail_admin_action_from_tracker_error(
                                &scope,
                                &request_digest,
                                &operation_id,
                                &error,
                            )
                            .await
                        {
                            first_error.get_or_insert(finalize_error);
                        }
                        continue;
                    }
                    Err(_) => {
                        // The request might have committed in Tracker before
                        // the process lost its response. Do not retry it from
                        // startup recovery until its durable marker is
                        // reconciled or a human resolves it.
                        first_error.get_or_insert_with(|| {
                            format!(
                                "administrative operation {operation_id} outcome is unconfirmed"
                            )
                        });
                        let _ = self
                            .mark_admin_action_unconfirmed(&scope, &request_digest, &operation_id)
                            .await;
                        continue;
                    }
                }
            };
            let expectation = match self.admin_marker_expectation(&operation_id).await {
                Ok(expectation) => expectation,
                Err(_) => {
                    first_error.get_or_insert_with(|| {
                        format!("administrative marker expectation {operation_id} is missing")
                    });
                    let _ = self
                        .mark_admin_action_unconfirmed(&scope, &request_digest, &operation_id)
                        .await;
                    continue;
                }
            };
            if verify_operation_marker(&marker, &expectation).is_err() {
                first_error.get_or_insert_with(|| {
                    format!("administrative marker {operation_id} does not match its expectation")
                });
                let _ = self
                    .mark_admin_action_unconfirmed(&scope, &request_digest, &operation_id)
                    .await;
                continue;
            }
            let receipt =
                match manager::backend_dispatch::admin_action_result_from_marker(&marker, &args) {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        first_error.get_or_insert_with(|| {
                            format!(
                                "administrative marker {operation_id} receipt is invalid: {error}"
                            )
                        });
                        continue;
                    }
                };
            let mutation = manager_v1::ManagerMutation {
                entity_revisions: vec![manager_v1::ManagerEntityRevision {
                    entity_kind: "issue".into(),
                    entity_id: args.issue_id.clone(),
                    revision: receipt.resulting_revision,
                }],
                result: Some(manager_v1::manager_mutation::Result::IssueAdminAction(
                    receipt,
                )),
                ..Default::default()
            };
            if self
                .complete_admin_action(&scope, &request_digest, &operation_id, &mutation)
                .await
                .is_ok()
            {
                recovered += 1;
            } else {
                first_error.get_or_insert_with(|| {
                    format!("administrative operation {operation_id} finalization failed")
                });
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(recovered)
        }
    }

    pub(super) async fn execute_saved_admin_action(
        &self,
        args: &manager_v1::ApplyIssueAdminActionArgs,
        issue_id: uuid::Uuid,
        operation_id: &str,
    ) -> Result<crate::tracker::Issue, TrackerError> {
        let context = args
            .context
            .as_ref()
            .ok_or_else(|| TrackerError::InvalidState("admin context is missing".into()))?;
        let actor_identity =
            crate::manager::backend_dispatch::admin_actor_identity(context, operation_id).map_err(
                |error| {
                    TrackerError::InvalidState(
                        match error {
                            crate::manager::backend_dispatch::AdminActorError::Missing => {
                                "admin actor is missing"
                            }
                            crate::manager::backend_dispatch::AdminActorError::InvalidKind => {
                                "admin actor kind is invalid"
                            }
                        }
                        .into(),
                    )
                },
            )?;
        let reason = context
            .actor_audit
            .as_ref()
            .and_then(|actor| actor.reason.as_deref())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| TrackerError::InvalidState("admin reason is missing".into()))?;
        let revision = args
            .expected_revision
            .ok_or_else(|| TrackerError::InvalidState("admin revision is missing".into()))?;
        match manager_v1::IssueAdminAction::try_from(args.action) {
            Ok(manager_v1::IssueAdminAction::Complete) => {
                let Some(manager_v1::apply_issue_admin_action_args::TerminalReport::Completion(
                    report,
                )) = args.terminal_report.as_ref()
                else {
                    return Err(TrackerError::InvalidState(
                        "completion report is missing".into(),
                    ));
                };
                self.core
                    .tracker()
                    .admin_terminate_with_overrides(
                        issue_id,
                        crate::tracker::IssueState::Done,
                        revision,
                        operation_id,
                        &actor_identity,
                        reason,
                        &v1::transition_issue_args::TerminalReport::Completion(report.clone()),
                        args.dependency_override
                            .as_ref()
                            .is_some_and(|value| value.enabled),
                        args.inconsistent_claim_resolution
                            .as_ref()
                            .is_some_and(|value| value.clear),
                        args.dependency_override
                            .as_ref()
                            .map(|value| value.reason.as_str()),
                        args.inconsistent_claim_resolution
                            .as_ref()
                            .map(|value| value.reason.as_str()),
                    )
                    .await
            }
            Ok(manager_v1::IssueAdminAction::Cancel) => {
                let Some(manager_v1::apply_issue_admin_action_args::TerminalReport::Cancellation(
                    report,
                )) = args.terminal_report.as_ref()
                else {
                    return Err(TrackerError::InvalidState(
                        "cancellation report is missing".into(),
                    ));
                };
                self.core
                    .tracker()
                    .admin_terminate_with_overrides(
                        issue_id,
                        crate::tracker::IssueState::Canceled,
                        revision,
                        operation_id,
                        &actor_identity,
                        reason,
                        &v1::transition_issue_args::TerminalReport::Cancellation(*report),
                        false,
                        args.inconsistent_claim_resolution
                            .as_ref()
                            .is_some_and(|value| value.clear),
                        None,
                        args.inconsistent_claim_resolution
                            .as_ref()
                            .map(|value| value.reason.as_str()),
                    )
                    .await
            }
            Ok(manager_v1::IssueAdminAction::Recover)
            | Ok(manager_v1::IssueAdminAction::Revoke) => {
                self.core
                    .tracker()
                    .recover_claim(
                        issue_id,
                        revision,
                        args.action == manager_v1::IssueAdminAction::Revoke as i32,
                        operation_id,
                        &actor_identity,
                        reason,
                    )
                    .await
            }
            _ => Err(TrackerError::InvalidState("admin action is invalid".into())),
        }
    }

    /// Reserve the northbound idempotency key before touching canonical state.
    /// A crash after this point is visible as a durable, unconfirmed operation
    /// instead of an invisible Tracker-only administrative mutation.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn admit_admin_action(
        &self,
        context: &manager_v1::ManagerWriteContext,
        scope: &IdempotencyScope,
        request_digest: String,
        operation_id: String,
        issue_id: String,
        _action: manager_v1::IssueAdminAction,
        _expected_revision: u64,
        request_snapshot: Vec<u8>,
    ) -> Result<Option<manager_v1::ApplyIssueAdminActionResult>, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        let mut manager = manager.lock().await;
        let decision = {
            let mut repository = manager.storage_mut().repository();
            match repository.reserve_idempotency(&scope.canonical(), &request_digest, &operation_id)
            {
                Ok(decision) => decision,
                Err(error)
                    if error.code == manager_v1::ManagerErrorCode::ResolutionRequired as i32
                        && repository
                            .abort_orphaned_idempotency_reservation(
                                &scope.canonical(),
                                &request_digest,
                            )
                            .map_err(|cleanup| cleanup.to_string())? =>
                {
                    repository
                        .reserve_idempotency(&scope.canonical(), &request_digest, &operation_id)
                        .map_err(|retry_error| retry_error.message)?
                }
                Err(error) => return Err(error.message),
            }
        };
        match decision {
            manager::gateway::IdempotencyDecision::Replay(payload) => {
                // The idempotency record is only an index.  The immutable
                // terminal snapshot is the durable result of record: a
                // partially finalized operation must never be presented as
                // a successful replay merely because an older idempotency
                // payload survived.  This also makes GetOperation and retry
                // observe the same receipt after a crash.
                let operation = manager
                    .storage_mut()
                    .repository()
                    .durable_operation_for_idempotency_scope(&scope.canonical())
                    .map_err(|error| error.message.clone())?
                    .ok_or("administrative replay has no durable operation")?;
                if operation.request_digest != request_digest
                    || operation.idempotency_scope != scope.canonical()
                {
                    return Err(
                        "administrative replay does not match its idempotency reservation".into(),
                    );
                }
                let response = manager_v1::ApplyIssueAdminActionResult::decode(payload.as_slice())
                    .map_err(|error| format!("administrative replay is corrupt: {error}"))?;
                match (
                    manager_v1::DurableOperationState::try_from(operation.state),
                    operation.terminal_snapshot.as_ref(),
                ) {
                    (
                        Ok(manager_v1::DurableOperationState::Committed),
                        Some(manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(
                            snapshot,
                        )),
                    ) if snapshot.schema
                        == "issue_tracker.manager.v1.ApplyIssueAdminActionResult" =>
                    {
                        let snapshot_response = manager_v1::ApplyIssueAdminActionResult::decode(
                            snapshot.payload.as_slice(),
                        )
                        .map_err(|error| {
                            format!("administrative terminal snapshot is corrupt: {error}")
                        })?;
                        if snapshot_response != response {
                            return Err(
                                "administrative idempotency result disagrees with terminal snapshot"
                                    .into(),
                            );
                        }
                    }
                    (
                        Ok(manager_v1::DurableOperationState::Failed),
                        Some(manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                            snapshot,
                        )),
                    ) if snapshot.schema == "issue_tracker.manager.v1.ManagerError" => {
                        let snapshot_error = manager_v1::ManagerError::decode(
                            snapshot.payload.as_slice(),
                        )
                        .map_err(|error| {
                            format!("administrative error snapshot is corrupt: {error}")
                        })?;
                        if !matches!(
                            response.outcome.as_ref(),
                            Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(error))
                                if *error == snapshot_error
                        ) {
                            return Err(
                                "administrative idempotency error disagrees with terminal snapshot"
                                    .into(),
                            );
                        }
                    }
                    (
                        Ok(
                            manager_v1::DurableOperationState::ManualResolution
                            | manager_v1::DurableOperationState::Arbitrated,
                        ),
                        Some(manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                            snapshot,
                        )),
                    ) if snapshot.schema == "issue_tracker.manager.v1.ManagerError" => {
                        let snapshot_error = manager_v1::ManagerError::decode(
                            snapshot.payload.as_slice(),
                        )
                        .map_err(|error| {
                            format!("administrative diagnosis snapshot is corrupt: {error}")
                        })?;
                        if !matches!(
                            response.outcome.as_ref(),
                            Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(error))
                                if *error == snapshot_error
                        ) {
                            return Err(
                                "administrative idempotency diagnosis disagrees with terminal snapshot"
                                    .into(),
                            );
                        }
                    }
                    _ => {
                        return Err("administrative replay has no valid terminal snapshot".into());
                    }
                }
                // A successful action and the integrity-diagnosis cleanup
                // are intentionally separate Manager transactions.  If the
                // process stopped after the action finalization commit (or a
                // transient SQLite error prevented the cleanup), the
                // idempotency row still contains the immutable action result
                // but the diagnosis remains open.  Retry that cleanup before
                // returning the replay so a later request can make progress;
                // never fail or rebuild the action result merely because the
                // optional cleanup is currently unavailable.
                // A diagnosis is resolved only by a committed successful
                // action.  Replaying a FAILED or MANUAL_RESOLUTION response
                // must not turn an unresolved defect into RESOLVED merely
                // because the request carries the same reference.
                if matches!(
                    manager_v1::DurableOperationState::try_from(operation.state),
                    Ok(manager_v1::DurableOperationState::Committed)
                ) && matches!(
                    response.outcome.as_ref(),
                    Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(_))
                ) && let Ok(Some(request)) = manager
                    .storage_mut()
                    .repository()
                    .admin_action_request(&operation.operation_id)
                    && let Some(reference) = request.ownership_diagnosis.as_ref()
                {
                    let _ = manager
                        .storage_mut()
                        .repository()
                        .resolve_ownership_integrity_diagnosis(reference, &operation.operation_id);
                }
                return Ok(Some(response));
            }
            manager::gateway::IdempotencyDecision::Pending { operation, .. } => {
                // `operation_id` is freshly generated for every retry.  The
                // reservation owns the original ID and must be used for all
                // marker lookups/finalization; otherwise a same-key retry
                // would look up an operation that was never persisted.
                let pending_operation_id = operation.operation_id.clone();
                drop(manager);
                let state = {
                    let mut manager = self
                        .manager
                        .as_ref()
                        .ok_or("manager is not configured")?
                        .lock()
                        .await;
                    manager
                        .storage_mut()
                        .repository()
                        .get_operation(&pending_operation_id)
                        .map_err(|error| error.to_string())?
                        .ok_or("administrative operation was not persisted")?
                        .state
                };
                if matches!(
                    manager_v1::DurableOperationState::try_from(state),
                    Ok(manager_v1::DurableOperationState::Unconfirmed
                        | manager_v1::DurableOperationState::ManualResolution)
                ) {
                    return Err(
                        "administrative operation has an unconfirmed Tracker outcome".into(),
                    );
                }
                match self
                    .core
                    .tracker()
                    .get_operation_record(&pending_operation_id)
                    .await
                {
                    Ok(marker) => {
                        let args = {
                            let mut manager = self
                                .manager
                                .as_ref()
                                .ok_or("manager is not configured")?
                                .lock()
                                .await;
                            manager
                                .storage_mut()
                                .repository()
                                .admin_action_request(&pending_operation_id)
                                .map_err(|error| error.to_string())?
                                .ok_or("administrative request snapshot is missing or invalid")?
                        };
                        let expectation =
                            self.admin_marker_expectation(&pending_operation_id).await?;
                        verify_operation_marker(&marker, &expectation).map_err(|error| {
                            format!("administrative marker does not match its admission: {error}")
                        })?;
                        let receipt = manager::backend_dispatch::admin_action_result_from_marker(
                            &marker, &args,
                        )
                        .map_err(|error| {
                            format!("administrative marker receipt is invalid: {error}")
                        })?;
                        let mutation = manager_v1::ManagerMutation {
                            entity_revisions: vec![manager_v1::ManagerEntityRevision {
                                entity_kind: "issue".into(),
                                entity_id: issue_id.clone(),
                                revision: receipt.resulting_revision,
                            }],
                            result: Some(manager_v1::manager_mutation::Result::IssueAdminAction(
                                receipt,
                            )),
                            ..Default::default()
                        };
                        let operation = self
                            .complete_admin_action(
                                scope,
                                &request_digest,
                                &pending_operation_id,
                                &mutation,
                            )
                            .await?;
                        let mut mutation = mutation;
                        mutation.operation = Some(operation);
                        return Ok(Some(manager_v1::ApplyIssueAdminActionResult {
                            outcome: Some(
                                manager_v1::apply_issue_admin_action_result::Outcome::Success(
                                    mutation,
                                ),
                            ),
                        }));
                    }
                    Err(TrackerError::NotFound(_)) => {
                        // The reservation is owned by the original
                        // operation, not by the fresh ID generated for this
                        // retry.  If the original outbox entry is still
                        // pending, drive that exact entry here so the caller
                        // never falls through to `process_operation` with an
                        // unrelated ID.
                        let Some(driver) = self.driver.as_ref() else {
                            return Err(
                                "administrative operation has no outbox driver for recovery".into(),
                            );
                        };
                        match driver.process_operation(&pending_operation_id).await {
                            Ok(Some(processed))
                                if processed.state
                                    == manager_v1::DurableOperationState::Committed =>
                            {
                                let mut response = manager_v1::ApplyIssueAdminActionResult::decode(
                                    processed.response_payload.as_slice(),
                                )
                                .map_err(|error| {
                                    format!("administrative recovery response is corrupt: {error}")
                                })?;
                                if let Some(
                                    manager_v1::apply_issue_admin_action_result::Outcome::Success(
                                        mutation,
                                    ),
                                ) = response.outcome.as_mut()
                                {
                                    mutation.operation = Some(processed.operation);
                                }
                                return Ok(Some(response));
                            }
                            Ok(Some(processed)) => {
                                return Err(format!(
                                    "administrative recovery is not confirmed: {}",
                                    processed.reason
                                ));
                            }
                            Ok(None) => {
                                return Err(
                                    "administrative outbox entry was not due for recovery".into()
                                );
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
            manager::gateway::IdempotencyDecision::New => {}
        }
        let config = manager.config().clone();
        let generation = manager
            .storage()
            .generation()
            .checked_add(1)
            .ok_or("manager generation overflow")?;
        let sequence = manager.storage().manager_sequence();
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis()
            .try_into()
            .map_err(|_| "system time exceeds i64 milliseconds")?;
        let operation = NewOperation {
            operation_id: operation_id.clone(),
            scope: Some(OperationScope::Issue(issue_id.clone())),
            method: "manager_apply_issue_admin_action".into(),
            idempotency_scope: scope.clone(),
            request_digest,
            request_schema: "issue_tracker.manager.v1.ApplyIssueAdminActionArgs".into(),
            // Administrative writes use the same deterministic operation ID
            // for the Manager record and the Tracker marker.  Keeping the
            // identity stable lets the generic outbox recovery reconcile a
            // commit that happened before the Manager response was saved.
            downstream_operation_id: Some(operation_id.clone()),
            generation,
            manager_sequence: sequence,
            causal_parent: None,
            affects_ready: true,
            sync_subject: Some(issue_id.clone()),
            affected_issue_ids: vec![issue_id],
            actor_audit: context.actor_audit.clone(),
            contract_fingerprint: config.manager_identity().contract_fingerprint.clone(),
            schema_fingerprint: config.manager_identity().schema_fingerprint.clone(),
            correlation_id: context.correlation_id.clone(),
            now_millis,
            replayable: true,
        }
        .build()
        .map_err(|error| error.to_string())?;
        let mutation = manager_v1::ManagerMutation {
            operation: Some(operation.clone()),
            ..Default::default()
        };
        let expectation = MarkerExpectation {
            operation_id: operation.downstream_operation_id.clone(),
            method_key: "apply_issue_admin_action".into(),
            scope: v1::OperationScope::Issue as i32,
            payload_fingerprint: crate::tracker::operation_log::marker_digest(&[
                "apply_issue_admin_action",
                &operation_id,
            ]),
            contract_fingerprint: contract_fingerprint(),
            affected_issue_ids: vec![
                operation
                    .scope
                    .as_ref()
                    .and_then(|scope| match scope {
                        manager_v1::durable_operation::Scope::IssueId(issue_id) => Some(issue_id),
                        _ => None,
                    })
                    .cloned()
                    .ok_or("administrative operation has no issue scope")?,
            ],
            entity_revisions: vec![EntityRevisionExpectation {
                issue_id: operation
                    .scope
                    .as_ref()
                    .and_then(|scope| match scope {
                        manager_v1::durable_operation::Scope::IssueId(issue_id) => Some(issue_id),
                        _ => None,
                    })
                    .cloned()
                    .ok_or("administrative operation has no issue scope")?,
                backend_id: operation
                    .scope
                    .as_ref()
                    .and_then(|scope| match scope {
                        manager_v1::durable_operation::Scope::IssueId(issue_id) => Some(issue_id),
                        _ => None,
                    })
                    .cloned()
                    .ok_or("administrative operation has no issue scope")?,
                resulting_revision: _expected_revision,
            }],
            result_replayable: true,
        };
        let mut repository = manager.storage_mut().repository();
        if let Err(error) = repository.commit_admin_admission(
            &mutation,
            &operation,
            &request_snapshot,
            &expectation,
        ) {
            // The idempotency reservation is created before the durable
            // operation/outbox transaction.  Roll it back when admission
            // validation or storage fails so the same request can be retried
            // instead of being stranded as a permanent Pending row.
            let _ = repository.abort_idempotency_reservation(
                &scope.canonical(),
                &operation.request_digest,
                &operation.operation_id,
            );
            return Err(error.to_string());
        }
        Ok(None)
    }

    pub(super) async fn complete_admin_action(
        &self,
        scope: &IdempotencyScope,
        request_digest: &str,
        operation_id: &str,
        mutation: &manager_v1::ManagerMutation,
    ) -> Result<manager_v1::DurableOperation, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        let mut manager = manager.lock().await;
        let mut repository = manager.storage_mut().repository();
        let mut operation = repository
            .get_operation(operation_id)
            .map_err(|error| error.to_string())?
            .ok_or("administrative operation was not persisted")?;
        if operation.request_digest != request_digest
            || operation.idempotency_scope != scope.canonical()
        {
            return Err(
                "administrative operation does not match its idempotency reservation".into(),
            );
        }
        // Keep the optional integrity-diagnosis reference from the admitted
        // request.  Its resolution is deliberately a second, CAS-bound
        // transaction after the action finalization; an action receipt must
        // remain durable even when diagnosis cleanup is temporarily down.
        let diagnosis_reference = repository
            .admin_action_request(operation_id)
            .map_err(|error| error.to_string())?
            .and_then(|request| request.ownership_diagnosis);
        // Two request tasks may have observed the same immutable Tracker
        // marker concurrently.  Once a terminal snapshot exists, completion
        // is already committed; return that operation without advancing its
        // generation or rebuilding a result from the mutable Issue.
        if matches!(
            manager_v1::DurableOperationState::try_from(operation.state),
            Ok(manager_v1::DurableOperationState::Committed)
        ) && operation.terminal_snapshot.is_some()
        {
            if let Some(reference) = diagnosis_reference.as_ref() {
                let _ = repository.resolve_ownership_integrity_diagnosis(reference, operation_id);
            }
            return Ok(operation);
        }
        operation.state = manager_v1::DurableOperationState::Committed as i32;
        if matches!(
            mutation.result.as_ref(),
            Some(manager_v1::manager_mutation::Result::IssueAdminAction(
                manager_v1::IssueAdminActionResult { changed: false, .. }
            ))
        ) {
            // A durable NO_CHANGE decision has a valid receipt but does not
            // leave the Issue in-flight. It must not suppress readiness.
            operation.affects_ready = false;
        }
        operation.generation = operation
            .generation
            .checked_add(1)
            .ok_or("administrative operation generation overflow")?;
        let mut completed_mutation = mutation.clone();
        completed_mutation.operation = Some(operation.clone());
        let response = manager_v1::ApplyIssueAdminActionResult {
            outcome: Some(
                manager_v1::apply_issue_admin_action_result::Outcome::Success(completed_mutation),
            ),
        };
        operation.terminal_snapshot = Some(
            manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(manager_v1::Snapshot {
                payload: response.encode_to_vec(),
                schema: "issue_tracker.manager.v1.ApplyIssueAdminActionResult".into(),
            }),
        );
        repository
            .finalize_admin_action(&operation, &response.encode_to_vec())
            .map_err(|error| error.to_string())?;
        if let Some(reference) = diagnosis_reference.as_ref() {
            // A stale or already-resolved diagnosis is intentionally not an
            // action failure: the action's immutable receipt is committed,
            // while the current diagnosis (if any) remains independently
            // fenced for operator review.  Storage errors are retried by the
            // next idempotent action replay and keep the diagnosis open.
            let _ = repository.resolve_ownership_integrity_diagnosis(reference, operation_id);
        }
        repository
            .get_operation(operation_id)
            .map_err(|error| error.to_string())?
            .ok_or("administrative operation disappeared after completion".into())
    }

    /// A validated Tracker rejection has no canonical side effect.  Record it
    /// as a terminal response so a retry cannot strand the admission in
    /// RECEIVED or keep the Issue excluded from readiness.
    pub(super) async fn fail_admin_action(
        &self,
        scope: &IdempotencyScope,
        request_digest: &str,
        operation_id: &str,
        code: manager_v1::ManagerErrorCode,
        message: String,
    ) -> Result<manager_v1::ApplyIssueAdminActionResult, String> {
        self.fail_admin_action_with_attributes(
            scope,
            request_digest,
            operation_id,
            code,
            message,
            &[],
            None,
            None,
            &[],
            None,
        )
        .await
    }

    pub(super) async fn fail_admin_action_from_tracker_error(
        &self,
        scope: &IdempotencyScope,
        request_digest: &str,
        operation_id: &str,
        error: &TrackerError,
    ) -> Result<manager_v1::ApplyIssueAdminActionResult, String> {
        let (
            invalid_attributes,
            observed_revision,
            lease_expires_at,
            blocker_issue_ids,
            revision_conflict,
        ) = match error {
            TrackerError::LeaseInconsistent {
                invalid_attributes,
                observed_revision,
                lease_expires_at,
            } => (
                invalid_attributes.as_slice(),
                *observed_revision,
                *lease_expires_at,
                None,
                None,
            ),
            TrackerError::InvalidLease {
                observed_revision,
                lease_expires_at,
            }
            | TrackerError::LeaseExpired {
                observed_revision,
                lease_expires_at,
            } => (&[][..], *observed_revision, *lease_expires_at, None, None),
            TrackerError::RevisionConflict { expected, actual } => {
                (&[][..], None, None, None, Some((*expected, *actual)))
            }
            TrackerError::Blocked {
                open_blocker_issue_ids,
            } => (
                &[][..],
                None,
                None,
                Some(open_blocker_issue_ids.as_slice()),
                None,
            ),
            _ => (&[][..], None, None, None, None),
        };
        let blocker_issue_ids = blocker_issue_ids
            .unwrap_or_default()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        self.fail_admin_action_with_attributes(
            scope,
            request_digest,
            operation_id,
            admin_manager_error_code(error),
            error.to_string(),
            invalid_attributes,
            observed_revision,
            lease_expires_at.and_then(lease_expiry_to_instant),
            &blocker_issue_ids,
            revision_conflict,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fail_admin_action_with_attributes(
        &self,
        scope: &IdempotencyScope,
        request_digest: &str,
        operation_id: &str,
        code: manager_v1::ManagerErrorCode,
        message: String,
        invalid_attributes: &[String],
        observed_revision: Option<u64>,
        lease_expires_at: Option<manager_v1::UtcInstant>,
        blocker_issue_ids: &[String],
        revision_conflict: Option<(u64, u64)>,
    ) -> Result<manager_v1::ApplyIssueAdminActionResult, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        let mut manager = manager.lock().await;
        let mut repository = manager.storage_mut().repository();
        let mut operation = repository
            .get_operation(operation_id)
            .map_err(|error| error.to_string())?
            .ok_or("administrative operation was not persisted")?;
        if operation.request_digest != request_digest
            || operation.idempotency_scope != scope.canonical()
        {
            return Err("administrative failure does not match its reservation".into());
        }
        // Another request may have finalized the same admitted operation while
        // this Tracker rejection was being converted into a typed failure.
        // Reuse that immutable response instead of advancing the generation or
        // replacing its audit/result snapshot a second time.
        if matches!(
            manager_v1::DurableOperationState::try_from(operation.state),
            Ok(manager_v1::DurableOperationState::Committed
                | manager_v1::DurableOperationState::Failed
                | manager_v1::DurableOperationState::ManualResolution
                | manager_v1::DurableOperationState::Arbitrated)
        ) {
            return match repository.reserve_idempotency(
                &scope.canonical(),
                request_digest,
                operation_id,
            ) {
                Ok(manager::gateway::IdempotencyDecision::Replay(payload)) => {
                    manager_v1::ApplyIssueAdminActionResult::decode(payload.as_slice())
                        .map_err(|error| format!("administrative replay is corrupt: {error}"))
                }
                Ok(_) => Err("terminal administrative operation has no replay receipt".into()),
                Err(error) => Err(error.message),
            };
        }
        operation.state = manager_v1::DurableOperationState::Failed as i32;
        operation.failure_class = manager_v1::FailureClass::Terminal as i32;
        operation.affects_ready = false;
        operation.generation = operation
            .generation
            .checked_add(1)
            .ok_or("administrative operation generation overflow")?;
        let error = manager_v1::ManagerError {
            code: code as i32,
            detail: if let Some((expected_revision, current_revision)) = revision_conflict {
                Some(manager_v1::manager_error::Detail::RevisionConflict(
                    manager_v1::RevisionConflict {
                        expected_revision,
                        current_revision,
                    },
                ))
            } else if invalid_attributes.is_empty()
                && observed_revision.is_none()
                && lease_expires_at.is_none()
                && blocker_issue_ids.is_empty()
            {
                admin_error_detail(code, &message)
            } else {
                admin_error_detail_with_attributes(
                    code,
                    &message,
                    invalid_attributes,
                    observed_revision,
                    lease_expires_at,
                    blocker_issue_ids,
                )
            },
            message,
            retryable: false,
        };
        let response = manager_v1::ApplyIssueAdminActionResult {
            outcome: Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(
                error.clone(),
            )),
        };
        operation.terminal_snapshot = Some(
            manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(manager_v1::Snapshot {
                payload: error.encode_to_vec(),
                schema: "issue_tracker.manager.v1.ManagerError".into(),
            }),
        );
        repository
            .finalize_admin_action(&operation, &response.encode_to_vec())
            .map_err(|error| error.to_string())?;
        Ok(response)
    }

    /// A transport or storage failure after dispatch has an unknown canonical
    /// outcome.  Keep readiness blocked and require marker reconciliation;
    /// it must never be finalized as an un-applied business rejection.
    pub(super) async fn mark_admin_action_unconfirmed(
        &self,
        scope: &IdempotencyScope,
        request_digest: &str,
        operation_id: &str,
    ) -> Result<(), String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        let mut manager = manager.lock().await;
        // Capture the outbox row before borrowing the repository.  An
        // acknowledged Tracker call whose marker cannot be verified must not
        // remain READY: re-delivering it after restart could apply a second
        // mutation with the same admission while the operation is already
        // fenced for manual resolution.
        let mut outbox = manager
            .recovery_outbox_entry(operation_id)
            .map_err(|error| error.to_string())?;
        let mut repository = manager.storage_mut().repository();
        let mut operation = repository
            .get_operation(operation_id)
            .map_err(|error| error.to_string())?
            .ok_or("administrative operation was not persisted")?;
        if operation.request_digest != request_digest
            || operation.idempotency_scope != scope.canonical()
        {
            return Err("administrative uncertainty does not match its reservation".into());
        }
        operation.state = manager_v1::DurableOperationState::Unconfirmed as i32;
        operation.resolution_required = true;
        operation.generation = operation
            .generation
            .checked_add(1)
            .ok_or("administrative operation generation overflow")?;
        if let Some(outbox) = outbox.as_mut() {
            outbox.status = manager_v1::OutboxStatus::Acknowledged as i32;
            outbox.lease_id = None;
            outbox.lease_expires_at = None;
            outbox.acknowledged_at = Some(manager_v1::UtcInstant {
                unix_epoch_millis: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|error| error.to_string())?
                    .as_millis()
                    .try_into()
                    .map_err(|_| "system time exceeds i64 milliseconds")?,
            });
            repository
                .finalize_admin_backend_dispatch(&operation, outbox, &[])
                .map(|_| ())
                .map_err(|error| error.to_string())
        } else {
            repository
                .update_operation(&operation)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
    }
}
