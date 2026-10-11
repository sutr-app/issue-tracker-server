use super::*;

impl OutboxDriver {
    /// Recover capability calls that have no outbox row. A PREPARED attempt
    /// proves that no southbound call was permitted and can be closed as a
    /// definitive FAILED result. A SENDING attempt proves only that the
    /// request may have crossed the process boundary; it becomes
    /// UNCONFIRMED and is reconciled against the immutable Tracker marker.
    pub(crate) async fn recover_inflight_capability_dispatches(
        &self,
        budget: usize,
    ) -> Result<u32, String> {
        let (candidates, protected, cursor) = {
            let mut manager = self.manager.lock().await;
            let protected = manager.active_capability_dispatch_ids()?;
            let cursor = manager
                .storage_mut()
                .repository()
                .capability_recovery_cursor()
                .map_err(|error| error.to_string())?;
            let candidates = manager
                .storage_mut()
                .repository()
                .capability_recovery_candidates(&protected, cursor.as_deref(), budget)
                .map_err(|error| error.to_string())?;
            (candidates, protected, cursor)
        };

        let mut recovered = 0u32;
        let mut first_error = None;
        let mut marker_operations = Vec::new();

        // Advance the persistent cursor only after this candidate has been
        // evaluated.  If the process stops while doing marker/manager work,
        // the candidate is replayed on restart (all finalizers are CAS
        // guarded); once evaluation finishes, even a malformed or protected
        // row is consumed so it cannot pin the bounded scan forever.
        macro_rules! checkpoint_and_continue {
            ($id:expr) => {{
                if let Err(error) = self
                    .manager
                    .lock()
                    .await
                    .storage_mut()
                    .repository()
                    .set_capability_recovery_cursor($id)
                {
                    first_error.get_or_insert(error.to_string());
                }
                continue;
            }};
        }
        for candidate in candidates.into_iter().take(budget) {
            let candidate_id = candidate.operation_id.clone();
            if protected.iter().any(|id| id == &candidate_id) {
                checkpoint_and_continue!(&candidate_id);
            }
            let Some(mut operation) = candidate.operation else {
                first_error.get_or_insert_with(|| {
                    candidate.error.unwrap_or_else(|| {
                        "capability recovery operation payload is missing".into()
                    })
                });
                checkpoint_and_continue!(&candidate_id);
            };
            let attempt_state = candidate.attempt_state;
            if attempt_state == crate::manager::repository::CAPABILITY_ATTEMPT_SENT_UNKNOWN {
                marker_operations.push(operation);
                checkpoint_and_continue!(&candidate_id);
            }
            let state = match crate::proto_types::manager_v1::DurableOperationState::try_from(
                operation.state,
            ) {
                Ok(state) => state,
                Err(error) => {
                    first_error
                        .get_or_insert(format!("invalid capability operation state: {error}"));
                    checkpoint_and_continue!(&candidate_id);
                }
            };
            if !matches!(
                state,
                crate::proto_types::manager_v1::DurableOperationState::Received
                    | crate::proto_types::manager_v1::DurableOperationState::Pending
            ) {
                checkpoint_and_continue!(&candidate_id);
            }
            if let Err(error) = crate::manager::operations::transition_operation(
                &mut operation,
                if attempt_state == crate::manager::repository::CAPABILITY_ATTEMPT_PREPARED {
                    crate::proto_types::manager_v1::DurableOperationState::Failed
                } else {
                    crate::proto_types::manager_v1::DurableOperationState::Unconfirmed
                },
                if attempt_state == crate::manager::repository::CAPABILITY_ATTEMPT_PREPARED {
                    Some(crate::proto_types::manager_v1::FailureClass::Terminal)
                } else {
                    None
                },
                Self::now_millis(),
            ) {
                first_error.get_or_insert(error.to_string());
                checkpoint_and_continue!(&candidate_id);
            }
            let is_prepared =
                attempt_state == crate::manager::repository::CAPABILITY_ATTEMPT_PREPARED;
            operation.resolution_required = !is_prepared;
            operation.affects_ready = !is_prepared;
            let error = crate::proto_types::manager_v1::ManagerError {
                code: if is_prepared {
                    crate::proto_types::manager_v1::ManagerErrorCode::BackendUnavailable as i32
                } else {
                    crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed as i32
                },
                message: if is_prepared {
                    "capability was prepared but not sent after restart; submit a new request"
                } else {
                    "capability delivery was interrupted; marker reconciliation is required"
                }
                .into(),
                retryable: !is_prepared,
                detail: None,
            };
            let result = crate::manager::capability_error_result(&operation.method, error);
            if is_prepared {
                operation.terminal_snapshot = Some(
                    crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                        crate::proto_types::manager_v1::Snapshot {
                            payload: result.clone(),
                            schema: crate::manager::capability_result_schema(&operation.method)
                                .into(),
                        },
                    ),
                );
            }
            let finalize_result = {
                let mut manager = self.manager.lock().await;
                manager
                    .storage_mut()
                    .repository()
                    .finalize_capability_operation(
                        &operation,
                        Some(&result),
                        if is_prepared {
                            crate::manager::repository::CAPABILITY_ATTEMPT_RESOLVED
                        } else {
                            crate::manager::repository::CAPABILITY_ATTEMPT_SENT_UNKNOWN
                        },
                        if is_prepared {
                            "REJECTED_BEFORE_MUTATION"
                        } else {
                            "SENT_UNKNOWN_RESTART"
                        },
                        Self::now_millis(),
                    )
                    .map_err(|error| error.to_string())
            };
            match finalize_result {
                Ok(_) => {
                    recovered = recovered.saturating_add(1);
                    if !is_prepared {
                        marker_operations.push(operation);
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
            if let Err(error) = self
                .manager
                .lock()
                .await
                .storage_mut()
                .repository()
                .set_capability_recovery_cursor(&candidate_id)
            {
                first_error.get_or_insert(error.to_string());
            }
        }

        // `cursor` is retained only to make the ownership explicit in this
        // phase: an empty/expired candidate set starts a fresh pass, while a
        // non-empty set checkpoints each evaluated row above.  Avoid a stale
        // value warning without introducing an in-memory cursor that would be
        // lost on restart.
        let _ = cursor;

        for mut operation in marker_operations {
            // Marker reconciliation is independent per operation.  A broken
            // expectation, missing payload, or finalization conflict must be
            // retained as a degraded-health signal, but it must not abort the
            // scan and starve later capability attempts.
            let result: Result<bool, String> = async {
                let expected = {
                    let mut manager = self.manager.lock().await;
                    manager
                        .storage_mut()
                        .repository()
                        .backend_marker_expectation(&operation.operation_id)
                        .map_err(|error| error.to_string())?
                };
                let marker = match self
                    .tracker
                    .get_operation_record(&operation.downstream_operation_id)
                    .await
                {
                    Ok(marker) => Some(marker),
                    Err(TrackerError::NotFound(_)) => None,
                    Err(error) => return Err(error.to_string()),
                };
                let Some(expected) = expected else {
                    return Err("capability marker expectation is missing".into());
                };
                let mut manager = self.manager.lock().await;
                let Some(current) = manager
                    .storage_mut()
                    .repository()
                    .get_operation(&operation.operation_id)
                    .map_err(|error| error.to_string())?
                else {
                    return Ok(false);
                };
                operation = current;
                if operation.state
                    != crate::proto_types::manager_v1::DurableOperationState::Unconfirmed as i32
                {
                    return Ok(false);
                }
                let Some(marker) = marker.as_ref() else {
                    return Ok(false);
                };
                let marker_matches = verify_operation_marker(marker, &expected).is_ok();
                if operation.method == "manager_claim_issue" && marker_matches {
                    crate::manager::operations::transition_operation(
                        &mut operation,
                        crate::proto_types::manager_v1::DurableOperationState::ManualResolution,
                        None,
                        Self::now_millis(),
                    )
                    .map_err(|error| error.to_string())?;
                    let error = crate::proto_types::manager_v1::ManagerError {
                        code: crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed
                            as i32,
                        message:
                            "claim marker is confirmed but the ephemeral lease handle was lost"
                                .into(),
                        retryable: false,
                        detail: None,
                    };
                    let result = crate::manager::capability_error_result(&operation.method, error);
                    manager
                        .storage_mut()
                        .repository()
                        .finalize_capability_operation(
                            &operation,
                            Some(&result),
                            crate::manager::repository::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
                            "MARKER_CONFIRMED_CAPABILITY_MISSING",
                            Self::now_millis(),
                        )
                        .map_err(|error| error.to_string())?;
                    return Ok(true);
                }
                let reconciled = reconcile::reconcile_recovered_marker(
                    &mut operation,
                    Some(marker),
                    Some(&expected),
                    Self::now_millis(),
                )
                .map_err(|error| error.to_string())?;
                if reconciled.state
                    == crate::proto_types::manager_v1::DurableOperationState::Committed
                {
                    let response = crate::manager::capability_success_result_from_marker(
                        &operation.method,
                        operation.clone(),
                        marker,
                    )?;
                    let durable = crate::manager::redacted_capability_response(
                        &operation.method,
                        &response,
                    )?;
                    operation.terminal_snapshot = Some(
                        crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(
                            crate::proto_types::manager_v1::Snapshot {
                                payload: durable.clone(),
                                schema: crate::manager::capability_result_schema(&operation.method)
                                    .into(),
                            },
                        ),
                    );
                    manager
                        .storage_mut()
                        .repository()
                        .finalize_capability_operation(
                            &operation,
                            Some(&durable),
                            crate::manager::repository::CAPABILITY_ATTEMPT_RESOLVED,
                            "MARKER_CONFIRMED_RESTART",
                            Self::now_millis(),
                        )
                        .map_err(|error| error.to_string())?;
                    return Ok(true);
                }
                if reconciled.state
                    == crate::proto_types::manager_v1::DurableOperationState::ManualResolution
                {
                    let error = crate::proto_types::manager_v1::ManagerError {
                        code: crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed
                            as i32,
                        message: "capability marker does not match the admitted request; manual resolution is required"
                            .into(),
                        retryable: false,
                        detail: None,
                    };
                    let result = crate::manager::capability_error_result(&operation.method, error);
                    manager
                        .storage_mut()
                        .repository()
                        .finalize_capability_operation(
                            &operation,
                            Some(&result),
                            crate::manager::repository::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
                            "MARKER_MISMATCH",
                            Self::now_millis(),
                        )
                        .map_err(|error| error.to_string())?;
                    return Ok(true);
                }
                Ok(false)
            }
            .await;
            match result {
                Ok(true) => recovered = recovered.saturating_add(1),
                Ok(false) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(recovered), Err)
    }

    pub(crate) async fn recover_unconfirmed(&self, budget: usize) -> Result<usize, String> {
        let mut recovered = 0;
        let mut recovery_error: Option<String> = None;
        let (_cursor, operation_ids) = {
            let mut manager = self.manager.lock().await;
            let repository = manager.storage_mut().repository();
            let cursor = repository
                .unconfirmed_recovery_cursor()
                .map_err(|error| error.to_string())?;
            let operation_ids = repository
                .unconfirmed_acknowledged_operations_page(cursor.as_deref(), budget)
                .map_err(|error| error.to_string())?;
            (cursor, operation_ids)
        };

        for (operation_id, created_at) in operation_ids.into_iter().take(budget) {
            let result = {
                let mut manager = self.manager.lock().await;
                manager
                    .storage_mut()
                    .repository()
                    .get_operation(&operation_id)
                    .map_err(|error| error.to_string())
            };
            match result {
                Ok(Some(operation)) => {
                    match self.reconcile_unconfirmed_operation(operation).await {
                        Ok(true) => recovered += 1,
                        Ok(false) => {}
                        Err(error) => {
                            recovery_error.get_or_insert(error);
                        }
                    }
                }
                Ok(None) => {
                    recovery_error.get_or_insert_with(|| {
                        format!("unconfirmed operation payload {operation_id} is missing")
                    });
                }
                Err(error) => {
                    recovery_error.get_or_insert(error);
                }
            }
            let mut manager = self.manager.lock().await;
            if let Err(error) = manager
                .storage_mut()
                .repository()
                .set_unconfirmed_recovery_cursor(created_at, &operation_id)
            {
                recovery_error.get_or_insert(error.to_string());
            }
        }
        if let Some(error) = recovery_error {
            Err(error)
        } else {
            Ok(recovered)
        }
    }

    async fn reconcile_unconfirmed_operation(
        &self,
        operation: crate::proto_types::manager_v1::DurableOperation,
    ) -> Result<bool, String> {
        let expected = {
            let mut manager = self.manager.lock().await;
            manager
                .storage_mut()
                .repository()
                .backend_marker_expectation(&operation.operation_id)
                .map_err(|error| error.to_string())?
        };
        let marker = match self
            .tracker
            .get_operation_record(&operation.downstream_operation_id)
            .await
        {
            Ok(marker) => Some(marker),
            Err(TrackerError::NotFound(_)) => None,
            Err(error) => return Err(error.to_string()),
        };

        let mut manager = self.manager.lock().await;
        let Some(mut current) = manager
            .storage_mut()
            .repository()
            .get_operation(&operation.operation_id)
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        if current.state
            != crate::proto_types::manager_v1::DurableOperationState::Unconfirmed as i32
        {
            // A request-bound or periodic pass won the race while the
            // tracker marker was being read. Never overwrite its result.
            return Ok(false);
        }
        let reconciled = reconcile::reconcile_recovered_marker(
            &mut current,
            marker.as_ref(),
            expected.as_ref(),
            Self::now_millis(),
        )
        .map_err(|error| error.to_string())?;
        if reconciled.state == crate::proto_types::manager_v1::DurableOperationState::Unconfirmed
            && matches!(reconciled.reason, ReconcileReason::DeliveryUnknown)
        {
            return Ok(false);
        }
        let outbox = manager
            .recovery_outbox_entry(&current.operation_id)
            .map_err(|error| error.to_string())?;
        // Administrative markers carry a typed immutable receipt in the
        // Tracker snapshot. Reconcile that receipt directly; the generic
        // backend finalizer would otherwise reconstruct a method-neutral
        // Issue mutation and lose the original action result after a crash.
        if current.method == "manager_apply_issue_admin_action"
            && reconciled.state == crate::proto_types::manager_v1::DurableOperationState::Committed
            && let Some(marker) = marker.as_ref()
        {
            let args = manager
                .storage_mut()
                .repository()
                .admin_action_request(&current.operation_id)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    "administrative request snapshot is missing during recovery".to_owned()
                })?;
            let receipt =
                crate::manager::backend_dispatch::admin_action_result_from_marker(marker, &args)
                    .map_err(|error| error.to_string())?;
            let response = crate::proto_types::manager_v1::ApplyIssueAdminActionResult {
                outcome: Some(
                    crate::proto_types::manager_v1::apply_issue_admin_action_result::Outcome::Success(
                        crate::proto_types::manager_v1::ManagerMutation {
                            operation: Some(current.clone()),
                            entity_revisions: vec![
                                crate::proto_types::manager_v1::ManagerEntityRevision {
                                    entity_kind: "issue".into(),
                                    entity_id: receipt.issue_id.clone(),
                                    revision: receipt.resulting_revision,
                                },
                            ],
                            result: Some(
                                crate::proto_types::manager_v1::manager_mutation::Result::IssueAdminAction(
                                    receipt,
                                ),
                            ),
                            ..Default::default()
                        },
                    ),
                ),
            };
            let response_payload = response.encode_to_vec();
            let mut repository = manager.storage_mut().repository();
            if let Some(outbox) = outbox {
                repository
                    .finalize_admin_backend_dispatch(&current, &outbox, &response_payload)
                    .map_err(|error| error.to_string())?;
            } else {
                let mut terminal = current.clone();
                terminal.terminal_snapshot = Some(
                    crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(
                        crate::proto_types::manager_v1::Snapshot {
                            payload: response_payload.clone(),
                            schema: "issue_tracker.manager.v1.ApplyIssueAdminActionResult".into(),
                        },
                    ),
                );
                repository
                    .finalize_admin_action(&terminal, &response_payload)
                    .map_err(|error| error.to_string())?;
            }
            return Ok(true);
        }
        let mappings = if reconciled.state
            == crate::proto_types::manager_v1::DurableOperationState::Committed
        {
            expected
                .as_ref()
                .map(|expected| {
                    manager
                        .issue_mappings_from_seeds(
                            expected
                                .entity_revisions
                                .iter()
                                .map(|revision| IssueMappingSeed {
                                    issue_id: revision.issue_id.clone(),
                                    backend_id: revision.backend_id.clone(),
                                    revision: revision.resulting_revision,
                                }),
                            current.generation,
                            current.created_at,
                            crate::proto_types::manager_v1::UtcInstant {
                                unix_epoch_millis: Self::now_millis(),
                            },
                        )
                        .map_err(|error| error.to_string())
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let mut repository = manager.storage_mut().repository();
        if let Some(outbox) = outbox {
            repository
                .finalize_backend_dispatch(&current, &outbox, &mappings)
                .map_err(|error| error.to_string())?;
        } else {
            // Capability operations have no outbox. Their marker can still
            // be reconciled, while durable backend operations normally take
            // the atomic operation/outbox path above.
            repository
                .update_operation(&current)
                .map_err(|error| error.to_string())?;
        }
        Ok(true)
    }
}
