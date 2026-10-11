use super::*;

impl GrpcServer {
    pub(crate) fn new(app: IssueTrackerApplication) -> Self {
        Self::from_app_arc(Arc::new(app))
    }

    pub(super) fn from_app_arc(app: Arc<IssueTrackerApplication>) -> Self {
        Self {
            app,
            capabilities: crate::gateway::LeaseCapabilities::default(),
            gateway_id: None,
            #[cfg(test)]
            gateway_test_faults: Default::default(),
        }
    }

    pub(super) fn for_gateway(&self, gateway_id: impl Into<Arc<str>>) -> Self {
        Self {
            app: Arc::clone(&self.app),
            capabilities: crate::gateway::LeaseCapabilities::default(),
            gateway_id: Some(gateway_id.into()),
            #[cfg(test)]
            gateway_test_faults: Arc::clone(&self.gateway_test_faults),
        }
    }

    pub(super) async fn for_gateway_worker(
        &self,
        gateway_id: impl Into<Arc<str>>,
    ) -> Result<Self, String> {
        let gateway_id = gateway_id.into();
        let epoch = self.app.start_gateway_capability_epoch(&gateway_id).await?;
        Ok(Self {
            app: Arc::clone(&self.app),
            capabilities: crate::gateway::LeaseCapabilities::for_epoch(epoch),
            gateway_id: Some(gateway_id),
            #[cfg(test)]
            gateway_test_faults: Arc::clone(&self.gateway_test_faults),
        })
    }

    pub(crate) fn gateway_id(&self) -> Option<&str> {
        self.gateway_id.as_deref()
    }

    pub(super) async fn abort_idempotency_reservation(
        &self,
        scope: &str,
        request_digest: &str,
        operation_id: &str,
    ) {
        if let Some(manager) = &self.app.manager {
            let mut manager = manager.lock().await;
            let _ = manager
                .storage_mut()
                .repository()
                .abort_idempotency_reservation(scope, request_digest, operation_id);
        }
    }

    pub(super) async fn apply_issue_admin_action(
        &self,
        request: Request<manager_v1::ApplyIssueAdminActionArgs>,
    ) -> Result<Response<manager_v1::ApplyIssueAdminActionResult>, Status> {
        let args = request.into_inner();
        let request_digest = digest_bytes(&args.encode_to_vec());
        let error = |code, message: String| {
            Response::new(manager_v1::ApplyIssueAdminActionResult {
                outcome: Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(
                    manager_v1::ManagerError {
                        code: code as i32,
                        message,
                        retryable: false,
                        detail: None,
                    },
                )),
            })
        };
        let Some(context) = args.context.as_ref() else {
            return Ok(error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "write context is required".into(),
            ));
        };
        if context.correlation_id.trim().is_empty() {
            return Ok(error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "correlation ID is required".into(),
            ));
        }
        let identity = match self.app.manager_store_context().await {
            Ok(value) => value,
            Err(message) => {
                return Ok(error(
                    manager_v1::ManagerErrorCode::BackendUnavailable,
                    message,
                ));
            }
        };
        if context.method != "manager_apply_issue_admin_action"
            || context.schema_fingerprint != identity.schema_fingerprint
            || context.contract_fingerprint != identity.contract_fingerprint
            || context.data_root_id != identity.data_root_id
            || context.target != format!("issue:{}", args.issue_id)
        {
            return Ok(error(
                manager_v1::ManagerErrorCode::MethodTargetMismatch,
                "administrative context does not match the issue action".into(),
            ));
        }
        if context.idempotency_key.trim().is_empty() {
            return Ok(error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "idempotency key is required".into(),
            ));
        }
        let Some(actor) = context.actor_audit.as_ref() else {
            return Ok(error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "actor audit is required".into(),
            ));
        };
        if actor
            .reason
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .is_none()
        {
            return Ok(error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "administrative reason is required".into(),
            ));
        }
        if actor.actor_id.trim().is_empty() || args.expected_revision.is_none() {
            return Ok(error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "actor ID and expected revision are required".into(),
            ));
        }
        let issue_id = match uuid::Uuid::parse_str(&args.issue_id) {
            Ok(value) if value.to_string() == args.issue_id => value,
            _ => {
                return Ok(error(
                    manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                    "issue_id must be a canonical UUID".into(),
                ));
            }
        };
        let scope = match IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            "manager_apply_issue_admin_action",
            context.target.clone(),
            context.idempotency_key.clone(),
        ) {
            Ok(scope) => scope,
            Err(scope_error) => {
                return Ok(error(
                    manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                    scope_error.to_string(),
                ));
            }
        };
        let operation_id = format!("admin-{}", digest_bytes(scope.canonical().as_bytes()));
        let action = match manager_v1::IssueAdminAction::try_from(args.action) {
            Ok(
                action @ (manager_v1::IssueAdminAction::Complete
                | manager_v1::IssueAdminAction::Cancel
                | manager_v1::IssueAdminAction::Recover
                | manager_v1::IssueAdminAction::Revoke),
            ) => action,
            _ => {
                return Ok(error(
                    manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                    "administrative action is invalid".into(),
                ));
            }
        };
        let actor_kind = v1::ActorKind::try_from(actor.actor_kind).ok();
        if matches!(
            action,
            manager_v1::IssueAdminAction::Complete
                | manager_v1::IssueAdminAction::Cancel
                | manager_v1::IssueAdminAction::Revoke
        ) && actor_kind != Some(v1::ActorKind::Human)
        {
            return Ok(error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "complete, cancel, and revoke require a human actor".into(),
            ));
        }
        if action == manager_v1::IssueAdminAction::Recover
            && actor_kind != Some(v1::ActorKind::Human)
        {
            return Ok(error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "public recover requires a human actor".into(),
            ));
        }
        // Validate every action-specific input before reserving the key.  An
        // invalid request must not leave a durable operation behind.
        match action {
            manager_v1::IssueAdminAction::Complete => {
                if !matches!(
                    args.terminal_report.as_ref(),
                    Some(manager_v1::apply_issue_admin_action_args::TerminalReport::Completion(_))
                ) || args
                    .dependency_override
                    .as_ref()
                    .is_some_and(|value| !value.enabled || value.reason.trim().is_empty())
                    || args
                        .inconsistent_claim_resolution
                        .as_ref()
                        .is_some_and(|value| !value.clear || value.reason.trim().is_empty())
                {
                    return Ok(error(
                        manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                        "complete action has invalid terminal or override fields".into(),
                    ));
                }
            }
            manager_v1::IssueAdminAction::Cancel => {
                if !matches!(
                    args.terminal_report.as_ref(),
                    Some(
                        manager_v1::apply_issue_admin_action_args::TerminalReport::Cancellation(_)
                    )
                ) || args.dependency_override.is_some()
                    || args
                        .inconsistent_claim_resolution
                        .as_ref()
                        .is_some_and(|value| !value.clear || value.reason.trim().is_empty())
                {
                    return Ok(error(
                        manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                        "cancel action has invalid terminal or override fields".into(),
                    ));
                }
            }
            manager_v1::IssueAdminAction::Recover | manager_v1::IssueAdminAction::Revoke => {
                if args.terminal_report.is_some()
                    || args.dependency_override.is_some()
                    || args.inconsistent_claim_resolution.is_some()
                {
                    return Ok(error(
                        manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                        "recovery actions do not accept terminal or override fields".into(),
                    ));
                }
            }
            _ => unreachable!("administrative action was validated above"),
        }
        if let Some(reference) = args.ownership_diagnosis.as_ref() {
            // Diagnosis references are repair selectors, never caller-owned
            // evidence.  Validate the shape before admission; the open-row
            // lookup is intentionally deferred until after idempotency
            // admission so a completed request can be replayed even after
            // its diagnosis has been resolved.
            if reference.target_kind != "issue"
                || reference.target_id != args.issue_id
                || reference.diagnosis_class.trim().is_empty()
                || reference.evidence_digest.trim().is_empty()
                || reference.diagnosis_generation == 0
                || reference.observation_token == 0
                || reference.repair_epoch == 0
            {
                return Ok(error(
                    manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                    "ownership diagnosis reference is invalid for this issue".into(),
                ));
            }
        }
        match self
            .app
            .admit_admin_action(
                context,
                &scope,
                request_digest.clone(),
                operation_id.clone(),
                issue_id.to_string(),
                action,
                args.expected_revision.unwrap(),
                args.encode_to_vec(),
            )
            .await
        {
            Ok(Some(replay)) => return Ok(Response::new(replay)),
            Ok(None) => {}
            Err(message) => {
                return Ok(error(admin_admission_error_code(&message), message));
            }
        }

        if let Some(reference) = args.ownership_diagnosis.as_ref() {
            // For a genuinely new request, require the exact open diagnosis
            // after the durable reservation.  A stale reference is a
            // definitive, pre-mutation business rejection and must therefore
            // be finalized as FAILED rather than leaving RECEIVED pending.
            let matches = self
                .app
                .manager
                .as_ref()
                .ok_or_else(|| Status::failed_precondition("manager is not configured"))?
                .lock()
                .await
                .storage_mut()
                .repository()
                .ownership_integrity_diagnosis_matches(reference)
                .map_err(|storage_error| Status::unavailable(storage_error.to_string()))?;
            if !matches {
                let response = self
                    .app
                    .fail_admin_action(
                        &scope,
                        &request_digest,
                        &operation_id,
                        manager_v1::ManagerErrorCode::ResolutionRequired,
                        "ownership diagnosis is stale, missing, or already resolved".into(),
                    )
                    .await
                    .map_err(Status::unavailable)?;
                return Ok(Response::new(response));
            }
        }

        // New administrative admissions are durable outbox entries. Drive
        // the entry through the same marker-aware dispatcher used by normal
        // backend writes; a missing driver is a fail-closed infrastructure
        // error rather than permission to mutate Tracker directly.
        if let Some(driver) = self.app.driver.as_ref() {
            match driver.process_operation(&operation_id).await {
                Ok(Some(processed))
                    if processed.state == manager_v1::DurableOperationState::Committed =>
                {
                    let mut response = manager_v1::ApplyIssueAdminActionResult::decode(
                        processed.response_payload.as_slice(),
                    )
                    .map_err(|error| Status::internal(error.to_string()))?;
                    if let Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(
                        mutation,
                    )) = response.outcome.as_mut()
                    {
                        mutation.operation = Some(processed.operation);
                    }
                    return Ok(Response::new(response));
                }
                Ok(Some(processed)) => {
                    let outcome = processed
                        .operation
                        .terminal_snapshot
                        .as_ref()
                        .and_then(|snapshot| match snapshot {
                            manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                                snapshot,
                            ) => manager_v1::ManagerError::decode(snapshot.payload.as_slice()).ok(),
                            _ => None,
                        })
                        .unwrap_or(manager_v1::ManagerError {
                            code: manager_v1::ManagerErrorCode::ResultUnconfirmed as i32,
                            message: processed.reason,
                            retryable: true,
                            detail: None,
                        });
                    return Ok(Response::new(manager_v1::ApplyIssueAdminActionResult {
                        outcome: Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(
                            outcome,
                        )),
                    }));
                }
                Ok(None) => {
                    return Ok(error(
                        manager_v1::ManagerErrorCode::ResultUnconfirmed,
                        "administrative outbox entry was not due for delivery".into(),
                    ));
                }
                Err(message) => {
                    return Ok(error(
                        manager_v1::ManagerErrorCode::ResultUnconfirmed,
                        message,
                    ));
                }
            }
        }

        Ok(error(
            manager_v1::ManagerErrorCode::BackendUnavailable,
            "manager outbox driver is unavailable".into(),
        ))
    }

    pub(super) async fn resolve_issue_operation(
        &self,
        request: Request<manager_v1::ResolveIssueOperationArgs>,
    ) -> Result<Response<manager_v1::ResolveIssueOperationResult>, Status> {
        let args = request.into_inner();
        let response_error = |code, message: String| manager_v1::ResolveIssueOperationResult {
            outcome: Some(manager_v1::resolve_issue_operation_result::Outcome::Error(
                manager_v1::ManagerError {
                    code: code as i32,
                    message,
                    retryable: false,
                    detail: None,
                },
            )),
        };
        let Some(context) = args.context.as_ref() else {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "write context is required".into(),
            )));
        };
        let identity = self
            .app
            .manager_store_context()
            .await
            .map_err(Status::failed_precondition)?;
        if context.correlation_id.trim().is_empty() {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "correlation ID is required".into(),
            )));
        }
        if context.method != "manager_resolve_issue_operation"
            || context.target != format!("issue:{}", args.issue_id)
            || context.schema_fingerprint != identity.schema_fingerprint
            || context.contract_fingerprint != identity.contract_fingerprint
            || context.data_root_id != identity.data_root_id
        {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::MethodTargetMismatch,
                "invalid trusted-consumer resolution context".into(),
            )));
        }
        if context.idempotency_key.trim().is_empty() {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "idempotency key is required".into(),
            )));
        }
        let Some(actor) = context.actor_audit.as_ref() else {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "actor audit is required".into(),
            )));
        };
        if actor.actor_id.trim().is_empty()
            || actor
                .reason
                .as_deref()
                .is_none_or(|reason| reason.trim().is_empty())
        {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "actor ID and reason are required".into(),
            )));
        }
        if v1::ActorKind::try_from(actor.actor_kind).ok() != Some(v1::ActorKind::Human) {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::MethodTargetMismatch,
                "resolution requires a trusted human actor".into(),
            )));
        }
        let Some(expected_operation_generation) = args.expected_operation_generation else {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "expected operation generation is required".into(),
            )));
        };
        let Some(expected_revision) = args.expected_revision else {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "expected revision is required".into(),
            )));
        };
        let issue_id = match uuid::Uuid::parse_str(&args.issue_id) {
            Ok(id) if id.to_string() == args.issue_id => id,
            _ => {
                return Ok(Response::new(response_error(
                    manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                    "issue_id must be a canonical UUID".into(),
                )));
            }
        };
        let mode = match manager_v1::IssueOperationResolutionMode::try_from(args.mode) {
            Ok(manager_v1::IssueOperationResolutionMode::HumanArbitration) => {
                manager_v1::IssueOperationResolutionMode::HumanArbitration
            }
            Ok(manager_v1::IssueOperationResolutionMode::EvidenceConfirmation) => {
                manager_v1::IssueOperationResolutionMode::EvidenceConfirmation
            }
            Ok(manager_v1::IssueOperationResolutionMode::Unspecified) | Err(_) => {
                return Ok(Response::new(response_error(
                    manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                    "resolution mode is invalid".into(),
                )));
            }
        };
        let arbitration_outcome =
            manager_v1::IssueOperationArbitrationOutcome::try_from(args.arbitration_outcome)
                .unwrap_or(manager_v1::IssueOperationArbitrationOutcome::Unspecified);
        let decision = if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration {
            match arbitration_outcome {
                manager_v1::IssueOperationArbitrationOutcome::Applied => true,
                manager_v1::IssueOperationArbitrationOutcome::NotApplied
                | manager_v1::IssueOperationArbitrationOutcome::UnknownSettled => false,
                manager_v1::IssueOperationArbitrationOutcome::Unspecified => {
                    return Ok(Response::new(response_error(
                        manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                        "arbitration outcome is required".into(),
                    )));
                }
            }
        } else {
            match manager_v1::IssueOperationResolutionDecision::try_from(args.decision) {
                Ok(manager_v1::IssueOperationResolutionDecision::ConfirmApplied) => true,
                Ok(manager_v1::IssueOperationResolutionDecision::ConfirmNotApplied) => false,
                _ => {
                    return Ok(Response::new(response_error(
                        manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                        "resolution decision is invalid".into(),
                    )));
                }
            }
        };
        let arbitration_fields_present = args.arbitration_outcome
            != manager_v1::IssueOperationArbitrationOutcome::Unspecified as i32
            || args.uncertainty_acknowledged
            || !args.arbitration_reason.trim().is_empty();
        if (mode == manager_v1::IssueOperationResolutionMode::HumanArbitration
            && args.decision != manager_v1::IssueOperationResolutionDecision::Unspecified as i32)
            || (mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation
                && arbitration_fields_present)
        {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "confirmation and arbitration fields cannot be mixed".into(),
            )));
        }
        if args.operation_id.is_empty()
            || (mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation
                && args.evidence_refs.is_empty())
            || (mode == manager_v1::IssueOperationResolutionMode::HumanArbitration
                && (!args.uncertainty_acknowledged || args.arbitration_reason.trim().is_empty()))
        {
            return Ok(Response::new(response_error(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "resolution mode fields are incomplete".into(),
            )));
        }
        let scope = IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            "manager_resolve_issue_operation",
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let request_digest = digest_bytes(&args.encode_to_vec());
        let resolution_request = args.encode_to_vec();
        let resolution_operation_id = format!(
            "issue-resolution-{}",
            digest_bytes(scope.canonical().as_bytes())
        );
        let manager = self
            .app
            .manager
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("manager is not configured"))?;
        // Validate the source operation's method before reserving the
        // resolution idempotency key.  Unsupported methods must have no
        // durable resolution request at all; otherwise a rejected request
        // would still occupy the key and make a later valid request appear
        // permanently pending or conflicting.
        {
            let mut manager_guard = manager.lock().await;
            if let Some(source_operation) = manager_guard
                .storage_mut()
                .repository()
                .get_operation(&args.operation_id)
                .map_err(|error| Status::internal(error.to_string()))?
                && ((mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation
                    && !is_resolvable_issue_operation_method(&source_operation.method))
                    || (mode == manager_v1::IssueOperationResolutionMode::HumanArbitration
                        && !matches!(
                            source_operation.scope,
                            Some(manager_v1::durable_operation::Scope::IssueId(_))
                        )))
            {
                return Ok(Response::new(response_error(
                    manager_v1::ManagerErrorCode::MethodTargetMismatch,
                    format!(
                        "operation method {} cannot be manually resolved",
                        source_operation.method
                    ),
                )));
            }
        }
        let replay = {
            let mut manager = manager.lock().await;
            let mut repository = manager.storage_mut().repository();
            match repository.reserve_idempotency(
                &scope.canonical(),
                &request_digest,
                &resolution_operation_id,
            ) {
                Ok(decision) => decision,
                Err(error)
                    if error.code == manager_v1::ManagerErrorCode::ResolutionRequired as i32
                        && repository
                            .abort_orphaned_idempotency_reservation(
                                &scope.canonical(),
                                &request_digest,
                            )
                            .map_err(|cleanup| Status::internal(cleanup.to_string()))? =>
                {
                    repository
                        .reserve_idempotency(
                            &scope.canonical(),
                            &request_digest,
                            &resolution_operation_id,
                        )
                        .map_err(|retry_error| Status::failed_precondition(retry_error.message))?
                }
                Err(error) => {
                    let code = manager_v1::ManagerErrorCode::try_from(error.code)
                        .unwrap_or(manager_v1::ManagerErrorCode::ResultUnconfirmed);
                    return Ok(Response::new(response_error(code, error.message)));
                }
            }
        };
        // A pending reservation owns the original operation ID. Keep that
        // immutable record through finalisation; retries must not create a
        // second resolution operation.
        let mut admitted_resolution_operation: Option<manager_v1::DurableOperation> = None;
        match replay {
            manager::gateway::IdempotencyDecision::Replay(payload) => {
                let replay = manager_v1::ResolveIssueOperationResult::decode(payload.as_slice())
                    .map_err(|error| {
                        Status::internal(format!("resolution replay is corrupt: {error}"))
                    })?;
                // The idempotency row is only an index.  Validate the
                // immutable resolution operation and its terminal receipt
                // before replaying the response; otherwise a stale or
                // partially written row could be presented as a confirmed
                // resolution after a restart.
                let mut manager = manager.lock().await;
                let operation = manager
                    .storage_mut()
                    .repository()
                    .get_operation(&resolution_operation_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .ok_or_else(|| {
                        Status::failed_precondition("resolution replay has no durable operation")
                    })?;
                if operation.method != "manager_resolve_issue_operation"
                    || operation.idempotency_scope != scope.canonical()
                    || operation.request_digest != request_digest
                {
                    return Err(Status::failed_precondition(
                        "resolution replay does not match its durable operation",
                    ));
                }
                match operation.state {
                    state if state == manager_v1::DurableOperationState::Committed as i32 => {
                        let Some(manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(
                            snapshot,
                        )) = operation.terminal_snapshot.as_ref()
                        else {
                            return Err(Status::failed_precondition(
                                "resolution replay has no terminal receipt",
                            ));
                        };
                        if snapshot.schema
                            != "issue_tracker.manager.v1.IssueOperationResolutionResult"
                        {
                            return Err(Status::failed_precondition(
                                "resolution replay has an invalid terminal receipt schema",
                            ));
                        }
                        let stored_receipt = manager_v1::IssueOperationResolutionResult::decode(
                            snapshot.payload.as_slice(),
                        )
                        .map_err(|error| {
                            Status::failed_precondition(format!(
                                "resolution terminal receipt is corrupt: {error}"
                            ))
                        })?;
                        let replay_receipt = match replay.outcome.as_ref() {
                            Some(manager_v1::resolve_issue_operation_result::Outcome::Success(
                                mutation,
                            )) => match mutation.result.as_ref() {
                                Some(
                                    manager_v1::manager_mutation::Result::IssueOperationResolution(
                                        receipt,
                                    ),
                                ) => receipt,
                                _ => {
                                    return Err(Status::failed_precondition(
                                        "resolution replay has no typed receipt",
                                    ));
                                }
                            },
                            _ => {
                                return Err(Status::failed_precondition(
                                    "resolution replay is not a successful receipt",
                                ));
                            }
                        };
                        if replay_receipt != &stored_receipt
                            || stored_receipt.operation_id != resolution_operation_id
                        {
                            return Err(Status::failed_precondition(
                                "resolution replay disagrees with terminal receipt",
                            ));
                        }
                    }
                    state
                        if state == manager_v1::DurableOperationState::Failed as i32
                            || state == manager_v1::DurableOperationState::Arbitrated as i32 =>
                    {
                        let Some(manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                            snapshot,
                        )) = operation.terminal_snapshot.as_ref()
                        else {
                            return Err(Status::failed_precondition(
                                "resolution replay has no terminal error",
                            ));
                        };
                        let stored_error =
                            manager_v1::ManagerError::decode(snapshot.payload.as_slice())
                                .map_err(|error| Status::failed_precondition(error.to_string()))?;
                        if !matches!(
                            replay.outcome.as_ref(),
                            Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error))
                                if *error == stored_error
                        ) {
                            return Err(Status::failed_precondition(
                                "resolution replay disagrees with terminal error",
                            ));
                        }
                    }
                    _ => {
                        return Err(Status::failed_precondition(
                            "resolution replay has no terminal state",
                        ));
                    }
                }
                return Ok(Response::new(replay));
            }
            manager::gateway::IdempotencyDecision::Pending { operation, .. } => {
                let resumable = {
                    let mut manager = manager.lock().await;
                    manager
                        .storage_mut()
                        .repository()
                        .resolution_request_matches(
                            &resolution_operation_id,
                            &args.operation_id,
                            &args.issue_id,
                            &resolution_request,
                            &request_digest,
                        )
                        .map_err(|error| Status::internal(error.to_string()))?
                };
                if !resumable {
                    return Ok(Response::new(response_error(
                        manager_v1::ManagerErrorCode::ResultUnconfirmed,
                        "resolution request is durably pending confirmation".into(),
                    )));
                }
                if operation.method != "manager_resolve_issue_operation"
                    || operation.idempotency_scope != scope.canonical()
                    || operation.request_digest != request_digest
                {
                    return Ok(Response::new(response_error(
                        manager_v1::ManagerErrorCode::ResultUnconfirmed,
                        "resolution reservation is inconsistent".into(),
                    )));
                }
                admitted_resolution_operation = Some(*operation);
            }
            manager::gateway::IdempotencyDecision::New => {}
        }
        macro_rules! reject_resolution {
            ($code:expr, $message:expr) => {{
                let message = ($message).to_string();
                if admitted_resolution_operation.is_some() {
                    if let Some(manager) = self.app.manager.as_ref() {
                        let mut manager = manager.lock().await;
                        match manager
                            .storage_mut()
                            .repository()
                            .finalize_resolution_failure(
                                &resolution_operation_id,
                                &request_digest,
                                $code,
                                &message,
                            ) {
                            Ok(payload) => {
                                let stored = manager_v1::ResolveIssueOperationResult::decode(
                                    payload.as_slice(),
                                )
                                .map_err(|error| {
                                    Status::failed_precondition(format!(
                                        "resolution failure receipt is corrupt: {error}"
                                    ))
                                })?;
                                return Ok(Response::new(stored));
                            }
                            Err(_) => {
                                // The evidence rejection is definitive, but
                                // the replay/fence receipt is not. Keep the
                                // request pending and surface an unavailable
                                // result so a retry can complete the same
                                // transaction.
                                return Err(Status::unavailable(
                                    "resolution failure could not be persisted",
                                ));
                            }
                        }
                    }
                } else {
                    self.abort_idempotency_reservation(
                        &scope.canonical(),
                        &request_digest,
                        &resolution_operation_id,
                    )
                    .await;
                }
                return Ok(Response::new(response_error($code, message)));
            }};
        }
        macro_rules! reject_resolution_locked {
            ($repository:expr, $code:expr, $message:expr) => {{
                let message = ($message).to_string();
                if admitted_resolution_operation.is_some() {
                    match $repository.finalize_resolution_failure(
                        &resolution_operation_id,
                        &request_digest,
                        $code,
                        &message,
                    ) {
                        Ok(payload) => {
                            let stored =
                                manager_v1::ResolveIssueOperationResult::decode(payload.as_slice())
                                    .map_err(|error| {
                                        Status::failed_precondition(format!(
                                            "resolution failure receipt is corrupt: {error}"
                                        ))
                                    })?;
                            return Ok(Response::new(stored));
                        }
                        Err(_) => {
                            return Err(Status::unavailable(
                                "resolution failure could not be persisted",
                            ));
                        }
                    }
                } else {
                    if $repository
                        .abort_idempotency_reservation(
                            &scope.canonical(),
                            &request_digest,
                            &resolution_operation_id,
                        )
                        .is_err()
                    {
                        return Err(Status::internal("resolution reservation cleanup failed"));
                    }
                }
                return Ok(Response::new(response_error($code, message)));
            }};
        }
        let (operation, admin_request) = {
            let mut manager = manager.lock().await;
            let mut repository = manager.storage_mut().repository();
            let Some(operation) = repository
                .get_operation(&args.operation_id)
                .map_err(|error| Status::internal(error.to_string()))?
            else {
                reject_resolution_locked!(
                    repository,
                    manager_v1::ManagerErrorCode::OperationNotFound,
                    "operation not found"
                );
            };
            let operation_state = manager_v1::DurableOperationState::try_from(operation.state)
                .unwrap_or(manager_v1::DurableOperationState::Unspecified);
            let operation_state_allowed =
                if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration {
                    matches!(
                        operation_state,
                        manager_v1::DurableOperationState::Received
                            | manager_v1::DurableOperationState::Pending
                            | manager_v1::DurableOperationState::Unconfirmed
                            | manager_v1::DurableOperationState::ManualResolution
                    ) || (operation_state == manager_v1::DurableOperationState::Failed
                        && operation.resolution_required)
                } else {
                    (decision
                        && matches!(
                            operation_state,
                            manager_v1::DurableOperationState::Unconfirmed
                                | manager_v1::DurableOperationState::ManualResolution
                        ))
                        || (!decision
                            && match operation_state {
                                manager_v1::DurableOperationState::Failed => {
                                    !operation.affects_ready
                                        && matches!(
                                operation.terminal_snapshot,
                                Some(
                                    manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                                        _
                                    )
                                )
                            )
                                }
                                manager_v1::DurableOperationState::Unconfirmed
                                | manager_v1::DurableOperationState::ManualResolution => {
                                    operation.resolution_required
                                }
                                _ => false,
                            })
                };
            if operation.generation != expected_operation_generation
                || operation.scope
                    != Some(manager_v1::durable_operation::Scope::IssueId(
                        args.issue_id.clone(),
                    ))
                || !operation_state_allowed
            {
                reject_resolution_locked!(
                    repository,
                    manager_v1::ManagerErrorCode::GenerationConflict,
                    "operation generation or state is not resolvable"
                );
            }
            let admin_request = if decision
                && mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation
                && operation.method == "manager_apply_issue_admin_action"
            {
                let request = repository
                    .admin_action_request(&operation.operation_id)
                    .map_err(|error| Status::internal(error.to_string()))?;
                match request {
                    Some(request) => Some(request),
                    None => {
                        reject_resolution_locked!(
                            repository,
                            manager_v1::ManagerErrorCode::ResolutionRequired,
                            "administrative request snapshot is missing or invalid"
                        );
                    }
                }
            } else {
                None
            };
            (operation, admin_request)
        };
        // Admit the immutable resolution request and its source fence before
        // reading Tracker evidence.  A definitive marker/finality rejection
        // must be replayable through fail_resolution; otherwise a client
        // retry can observe a fresh reservation and the original rejection
        // has no durable result.
        let source_generation = operation.generation;
        if admitted_resolution_operation.is_none() {
            let mut manager_guard = manager.lock().await;
            let config = manager_guard.config().clone();
            let resolution_generation = manager_guard
                .storage()
                .generation()
                .checked_add(1)
                .ok_or_else(|| Status::internal("manager generation overflow"))?;
            let resolution_sequence = manager_guard.storage().manager_sequence();
            let now_millis: i64 = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| Status::internal(error.to_string()))?
                .as_millis()
                .try_into()
                .map_err(|_| Status::internal("system time exceeds i64 milliseconds"))?;
            let operation = NewOperation {
                operation_id: resolution_operation_id.clone(),
                scope: Some(OperationScope::Issue(args.issue_id.clone())),
                method: "manager_resolve_issue_operation".into(),
                idempotency_scope: scope.clone(),
                request_digest: request_digest.clone(),
                request_schema: "issue_tracker.manager.v1.ResolveIssueOperationArgs".into(),
                downstream_operation_id: None,
                generation: resolution_generation,
                manager_sequence: resolution_sequence,
                causal_parent: Some(args.operation_id.clone()),
                affects_ready: false,
                sync_subject: Some(args.issue_id.clone()),
                affected_issue_ids: vec![args.issue_id.clone()],
                actor_audit: context.actor_audit.clone(),
                contract_fingerprint: config.manager_identity().contract_fingerprint.clone(),
                schema_fingerprint: config.manager_identity().schema_fingerprint.clone(),
                correlation_id: context.correlation_id.clone(),
                now_millis,
                replayable: true,
            }
            .build()
            .map_err(|error| Status::internal(error.to_string()))?;
            if let Err(error) = manager_guard
                .storage_mut()
                .repository()
                .commit_resolution_admission(
                    &operation,
                    &args.operation_id,
                    &args.issue_id,
                    source_generation,
                    expected_revision,
                    if decision {
                        manager_v1::IssueOperationResolutionDecision::ConfirmApplied as i32
                    } else {
                        manager_v1::IssueOperationResolutionDecision::ConfirmNotApplied as i32
                    },
                    &resolution_request,
                )
            {
                // The idempotency reservation was created before the source
                // fence.  If admission itself loses a race, remove that
                // reservation so a later retry is not stuck in Pending.
                let _ = manager_guard
                    .storage_mut()
                    .repository()
                    .abort_idempotency_reservation(
                        &scope.canonical(),
                        &request_digest,
                        &resolution_operation_id,
                    );
                return Err(Status::aborted(error.to_string()));
            }
            admitted_resolution_operation = Some(operation);
        }
        // Finality and marker expectations are checked only after durable
        // admission.  Permanent evidence failures can therefore be recorded
        // as FAILED resolution receipts; transient storage failures leave the
        // admitted request pending for the maintenance/retry path.
        let mut expectation = None;
        {
            let mut manager_guard = manager.lock().await;
            let mut repository = manager_guard.storage_mut().repository();
            if mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation
                && !decision
                && !repository
                    .confirm_not_applied_proven(&operation.operation_id)
                    .map_err(|error| Status::internal(error.to_string()))?
            {
                reject_resolution_locked!(
                    repository,
                    manager_v1::ManagerErrorCode::ResolutionRequired,
                    "server cannot prove that the operation is no longer executable"
                );
            }
            if decision && mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation {
                let Some(marker_expectation) = repository
                    .backend_marker_expectation(&args.operation_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                else {
                    reject_resolution_locked!(
                        repository,
                        manager_v1::ManagerErrorCode::ResolutionRequired,
                        "server-owned marker expectation is missing"
                    );
                };
                expectation = Some(marker_expectation);
            }
        }
        // Freeze Tracker writes before validating the Issue and keep that
        // observation stable through the bounded Manager resolution commit.
        // The Manager admission above has already released its lock.  The
        // final, short Manager lock below is acquired only after this guard
        // and no other dispatch path awaits Tracker while holding Manager.
        let _tracker_admission = self.app.core.tracker().acquire_admission_write().await;
        // Administrative actions execute directly against Tracker, so their
        // server-owned marker is keyed by the Manager operation ID rather
        // than a separately dispatched backend operation.  Other methods
        // retain their downstream marker identity.
        let applied_marker_operation_id = if operation.method == "manager_apply_issue_admin_action"
        {
            operation.operation_id.clone()
        } else {
            operation.downstream_operation_id.clone()
        };
        let expected_evidence = if decision {
            applied_marker_operation_id.clone()
        } else {
            format!(
                "manager-operation:{}:error-snapshot",
                operation.operation_id
            )
        };
        let mut known_evidence_refs = vec![expected_evidence.clone()];
        let diagnosis_reference = format!("manager-operation-diagnosis:{}", operation.operation_id);
        let diagnosis_reference_is_valid = operation.diagnosis.as_ref().is_some_and(|diagnosis| {
            diagnosis.operation_id == operation.operation_id
                && diagnosis.evidence_ref == diagnosis_reference
                && !diagnosis.evidence_digest.trim().is_empty()
        });
        let error_snapshot_reference_is_valid = matches!(
            operation.terminal_snapshot.as_ref(),
            Some(manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot))
                if snapshot.schema == "issue_tracker.manager.v1.ManagerError"
                    && manager_v1::ManagerError::decode(snapshot.payload.as_slice()).is_ok()
        );
        if let Some(diagnosis) = operation.diagnosis.as_ref()
            && !diagnosis.evidence_ref.trim().is_empty()
        {
            known_evidence_refs.push(diagnosis.evidence_ref.clone());
        }
        if args
            .evidence_refs
            .iter()
            .any(|reference| !known_evidence_refs.iter().any(|known| known == reference))
        {
            reject_resolution!(
                manager_v1::ManagerErrorCode::ManagerInvalidArgument,
                "evidence reference is unknown or bound to another operation"
            );
        }
        for reference in &args.evidence_refs {
            if reference == &applied_marker_operation_id {
                match self
                    .app
                    .core
                    .tracker()
                    .get_operation_record(&applied_marker_operation_id)
                    .await
                {
                    Ok(_) => {}
                    Err(TrackerError::NotFound(_)) => reject_resolution!(
                        manager_v1::ManagerErrorCode::ResolutionRequired,
                        "referenced Tracker marker is missing"
                    ),
                    Err(error) => return Err(Status::unavailable(error.to_string())),
                }
            } else if reference == &diagnosis_reference && !diagnosis_reference_is_valid {
                reject_resolution!(
                    manager_v1::ManagerErrorCode::ResolutionRequired,
                    "referenced operation diagnosis is invalid or unbound"
                );
            } else if reference == &expected_evidence
                && !decision
                && !error_snapshot_reference_is_valid
            {
                reject_resolution!(
                    manager_v1::ManagerErrorCode::ResolutionRequired,
                    "referenced error snapshot is missing"
                );
            }
        }
        if mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation
            && !args
                .evidence_refs
                .iter()
                .any(|reference| reference == &expected_evidence)
        {
            reject_resolution!(
                manager_v1::ManagerErrorCode::ResolutionRequired,
                "evidence does not bind a server-owned operation record"
            );
        }
        let issue = match self
            .app
            .core
            .tracker()
            .get_while_admission_held(issue_id)
            .await
        {
            Ok(issue) => issue,
            Err(TrackerError::NotFound(_)) => reject_resolution!(
                manager_v1::ManagerErrorCode::IssueNotFound,
                "issue not found"
            ),
            Err(error) => return Err(Status::unavailable(error.to_string())),
        };
        if issue.revision != expected_revision {
            reject_resolution!(
                manager_v1::ManagerErrorCode::RevisionConflict,
                "issue revision does not match"
            );
        }
        let (admin_receipt, ownership_source_result) = if decision
            && mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation
        {
            let marker = match self
                .app
                .core
                .tracker()
                .get_operation_record(&applied_marker_operation_id)
                .await
            {
                Ok(marker) => marker,
                Err(TrackerError::NotFound(_)) => reject_resolution!(
                    manager_v1::ManagerErrorCode::ResolutionRequired,
                    "server-owned operation marker is missing"
                ),
                Err(error) => return Err(Status::unavailable(error.to_string())),
            };
            if let Err(error) = manager::backend_adapter::verify_operation_marker(
                &marker,
                expectation
                    .as_ref()
                    .expect("applied resolution has expectation"),
            ) {
                reject_resolution!(
                    manager_v1::ManagerErrorCode::ResolutionRequired,
                    error.to_string()
                );
            }
            let admin_receipt = if let Some(request) = admin_request.as_ref() {
                match manager::backend_dispatch::admin_action_result_from_marker(&marker, request) {
                    Ok(receipt) => Some(receipt),
                    Err(error) => reject_resolution!(
                        manager_v1::ManagerErrorCode::ResolutionRequired,
                        error.to_string()
                    ),
                }
            } else {
                None
            };
            let ownership_result = if admin_receipt.is_none()
                && is_ownership_operation_method(&operation.method)
            {
                match ownership_result_response_from_marker(&operation, &marker) {
                    Ok(result) => Some(result),
                    Err(error) => {
                        reject_resolution!(manager_v1::ManagerErrorCode::ResolutionRequired, error)
                    }
                }
            } else {
                None
            };
            (admin_receipt, ownership_result)
        } else {
            (None, None)
        };
        // Install the durable Tracker-side fence only after all evidence
        // checks have succeeded.  A rejected resolution must not leave a
        // permanent fence that blocks a later valid request.  The admission
        // write guard remains held, so no delayed Tracker write can pass
        // between validation and fence persistence.
        // Keep the Manager lock while installing the Tracker fence and
        // re-checking the source generation.  The Tracker admission guard
        // above prevents concurrent canonical writes, while this lock closes
        // the other half of the race: an already-admitted dispatch cannot
        // advance the source generation after the check but before the fence
        // is durable.  If the generation is stale, no Tracker fence is
        // written and the rejected request leaves the source untouched.
        let mut manager = manager.lock().await;
        let (
            mut current,
            source_previous_state,
            source_previous_generation,
            previous_operation_snapshot,
            previous_replay_snapshot,
        ) = {
            let mut repository = manager.storage_mut().repository();
            let Some(current) = repository
                .get_operation(&args.operation_id)
                .map_err(|error| Status::internal(error.to_string()))?
            else {
                reject_resolution_locked!(
                    repository,
                    manager_v1::ManagerErrorCode::OperationNotFound,
                    "operation disappeared"
                );
            };
            if current.generation != expected_operation_generation {
                reject_resolution_locked!(
                    repository,
                    manager_v1::ManagerErrorCode::GenerationConflict,
                    "operation changed during verification"
                );
            }
            let source_previous_state = current.state;
            let source_previous_generation = current.generation;
            let previous_operation_snapshot = manager_v1::Snapshot {
                payload: current.encode_to_vec(),
                schema: "issue_tracker.manager.v1.DurableOperation".into(),
            };
            let previous_replay_payload = repository
                .operation_replay_snapshot(&current.operation_id, &current.request_digest)
                .map_err(|error| Status::internal(error.to_string()))?;
            let previous_replay_snapshot = match previous_replay_payload {
                Some(payload) => {
                    let schema =
                        manager::gateway::mutation_result_schema_for_method(&current.method)
                            .ok_or_else(|| {
                                Status::internal(format!(
                                    "unknown source operation method: {}",
                                    current.method
                                ))
                            })?;
                    Some(manager_v1::Snapshot {
                        payload,
                        schema: schema.into(),
                    })
                }
                None => None,
            };
            (
                current,
                source_previous_state,
                source_previous_generation,
                previous_operation_snapshot,
                previous_replay_snapshot,
            )
        };
        let mut fence_ids = if applied_marker_operation_id.trim().is_empty() {
            Vec::new()
        } else {
            vec![applied_marker_operation_id.clone()]
        };
        if operation.operation_id != applied_marker_operation_id {
            fence_ids.push(operation.operation_id.clone());
        }
        for fence_id in fence_ids {
            if let Err(error) = self
                .app
                .core
                .tracker()
                .persist_operation_fence(
                    issue_id,
                    &fence_id,
                    &resolution_operation_id,
                    &request_digest,
                )
                .await
            {
                return Err(Status::unavailable(format!(
                    "operation fence could not be persisted: {error}"
                )));
            }
        }
        let mut repository = manager.storage_mut().repository();
        // Admit a new resolution before changing the source operation. A
        // pending retry reuses the operation created by that admission.
        let mut resolution_operation = admitted_resolution_operation
            .take()
            .ok_or_else(|| Status::internal("resolution admission was not persisted"))?;
        if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration {
            current.state = manager_v1::DurableOperationState::Arbitrated as i32;
            current.failure_class = manager_v1::FailureClass::Terminal as i32;
            current.affects_ready = false;
        } else if decision {
            current.state = manager_v1::DurableOperationState::Committed as i32;
            current.failure_class = manager_v1::FailureClass::Unspecified as i32;
        } else {
            // ConfirmNotApplied is a human resolution of an uncertain or
            // manually fenced dispatch. Persist a terminal, non-ready source
            // operation even when the original operation had no error
            // snapshot (the usual UNCONFIRMED/MANUAL_RESOLUTION case). This
            // makes GetOperation and idempotent replay self-contained after a
            // successful resolution.
            current.state = manager_v1::DurableOperationState::Failed as i32;
            current.failure_class = manager_v1::FailureClass::Terminal as i32;
            current.affects_ready = false;
            if !matches!(
                current.terminal_snapshot,
                Some(manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(_))
            ) {
                current.terminal_snapshot = Some(
                    manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                        manager_v1::Snapshot {
                            schema: "issue_tracker.manager.v1.ManagerError".into(),
                            payload: manager_v1::ManagerError {
                                code: manager_v1::ManagerErrorCode::PreconditionFailed as i32,
                                message: "trusted consumer confirmed the operation was not applied"
                                    .into(),
                                retryable: false,
                                detail: None,
                            }
                            .encode_to_vec(),
                        },
                    ),
                );
            }
        }
        current.resolution_required = false;
        current.generation = current
            .generation
            .checked_add(1)
            .ok_or_else(|| Status::internal("operation generation overflow"))?;
        let source_operation_id = current.operation_id.clone();
        let resolution_diagnosis = {
            let diagnosis =
                current
                    .diagnosis
                    .get_or_insert_with(|| manager_v1::OperationDiagnosis {
                        operation_id: source_operation_id.clone(),
                        ..Default::default()
                    });
            diagnosis.operation_id = source_operation_id.clone();
            diagnosis.classification =
                if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration {
                    "HUMAN_ARBITRATION".into()
                } else if decision {
                    "EVIDENCE_CONFIRMED_APPLIED".into()
                } else {
                    "EVIDENCE_CONFIRMED_NOT_APPLIED".into()
                };
            diagnosis.resolution_state = "COMMITTED".into();
            diagnosis.resolution_operation_id = Some(resolution_operation_id.clone());
            diagnosis.replayability = "TERMINAL".into();
            diagnosis.recorded_at = Some(manager_v1::UtcInstant {
                unix_epoch_millis: unix_now_millis(),
            });
            diagnosis.clone()
        };
        let source_result = admin_receipt.as_ref().map(|receipt| {
            // Preserve the source operation's immutable result revision. The
            // resolution CAS revision is a separate observation.
            let resulting_revision = receipt.resulting_revision;
            // The terminal snapshot is the response's immutable source of
            // truth, but embedding that snapshot back into the operation in
            // the response would recurse forever.  Persist a sanitized
            // operation in the response and keep the full snapshot on the
            // durable operation row itself.
            let mut response_operation = current.clone();
            response_operation.terminal_snapshot = None;
            let mutation = manager_v1::ManagerMutation {
                operation: Some(response_operation),
                result: Some(manager_v1::manager_mutation::Result::IssueAdminAction(
                    receipt.clone(),
                )),
                entity_revisions: vec![manager_v1::ManagerEntityRevision {
                    entity_kind: "issue".into(),
                    entity_id: args.issue_id.clone(),
                    revision: resulting_revision,
                }],
                ..Default::default()
            };
            let response = manager_v1::ApplyIssueAdminActionResult {
                outcome: Some(
                    manager_v1::apply_issue_admin_action_result::Outcome::Success(mutation),
                ),
            }
            .encode_to_vec();
            current.terminal_snapshot = Some(
                manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(
                    manager_v1::Snapshot {
                        payload: response.clone(),
                        schema: "issue_tracker.manager.v1.ApplyIssueAdminActionResult".into(),
                    },
                ),
            );
            response
        });
        let mut source_result = source_result.or(ownership_source_result);
        // Preserve whether the original method-specific result was actually
        // available before arbitration synthesis.  A synthesized typed error
        // is useful for replay, but it must not erase the uncertainty fact
        // that the historical result was unavailable.
        let original_result_available = source_result.is_some();
        if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration
            && source_result.is_none()
        {
            // There is no trustworthy method-specific result to reconstruct
            // from the historical record.  Preserve a typed terminal error
            // for the original idempotency scope instead of fabricating a
            // success receipt; the arbitration receipt remains on the new
            // resolution operation.
            let arbitration_error = manager_v1::ManagerError {
                code: manager_v1::ManagerErrorCode::ResultUnconfirmed as i32,
                message: "operation settled by human arbitration; historical result is unavailable"
                    .into(),
                retryable: false,
                detail: Some(manager_v1::manager_error::Detail::OperationDiagnosis(
                    resolution_diagnosis.clone(),
                )),
            };
            current.terminal_snapshot = Some(
                manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                    manager_v1::Snapshot {
                        payload: arbitration_error.encode_to_vec(),
                        schema: "issue_tracker.manager.v1.ManagerError".into(),
                    },
                ),
            );
            source_result = manager::gateway::mutation_wrapper_for_method(&current.method)
                .map(|wrapper| wrapper(Err(arbitration_error)));
        }
        if let Some(source_result) = source_result.as_ref()
            && admin_receipt.is_none()
            && mode != manager_v1::IssueOperationResolutionMode::HumanArbitration
        {
            current.terminal_snapshot = Some(
                manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(
                    manager_v1::Snapshot {
                        payload: source_result.clone(),
                        schema: ownership_result_schema(&current.method).into(),
                    },
                ),
            );
        }
        let source_result_digest = source_result.as_ref().map(|payload| digest_bytes(payload));
        let evidence_status = if args.evidence_refs.is_empty() {
            manager_v1::IssueOperationResolutionEvidenceStatus::NoneSupplied
        } else if mode == manager_v1::IssueOperationResolutionMode::EvidenceConfirmation {
            manager_v1::IssueOperationResolutionEvidenceStatus::Verified
        } else {
            manager_v1::IssueOperationResolutionEvidenceStatus::PartiallyVerified
        };
        let mut verified_facts = vec![
            manager_v1::IssueOperationResolutionFact {
                reference: format!("operation-generation:{}", source_operation_id),
                classification: "SOURCE_GENERATION_CAS".into(),
                detail: format!(
                    "source generation {} matched at the resolution boundary",
                    source_previous_generation
                ),
            },
            manager_v1::IssueOperationResolutionFact {
                reference: format!("issue-revision:{}:{}", args.issue_id, issue.revision),
                classification: "ISSUE_REVISION_CAS".into(),
                detail: format!(
                    "Issue revision {} matched at the resolution boundary",
                    issue.revision
                ),
            },
        ];
        for reference in &args.evidence_refs {
            let classification = if reference == &applied_marker_operation_id {
                "MARKER_RECORD_PRESENT"
            } else if reference == &diagnosis_reference {
                "DIAGNOSIS_RECORD_VALID"
            } else if reference == &expected_evidence && !decision {
                "ERROR_SNAPSHOT_VALID"
            } else {
                "SERVER_OWNED_REFERENCE_VALID"
            };
            verified_facts.push(manager_v1::IssueOperationResolutionFact {
                reference: reference.clone(),
                classification: classification.into(),
                detail: "the referenced server-owned record was bound to the source operation"
                    .into(),
            });
        }
        let mut remaining_uncertainties = Vec::new();
        if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration {
            remaining_uncertainties.push("HUMAN_ARBITRATION_NOT_SERVER_PROVEN".into());
        }
        if arbitration_outcome == manager_v1::IssueOperationArbitrationOutcome::UnknownSettled {
            remaining_uncertainties.push("HISTORICAL_APPLICATION_STATUS_UNKNOWN".into());
        }
        if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration
            && !original_result_available
        {
            remaining_uncertainties.push("ORIGINAL_RESULT_UNAVAILABLE".into());
        }
        let receipt = manager_v1::IssueOperationResolutionResult {
            operation_id: resolution_operation_id.clone(),
            // Evidence confirmation and human arbitration are separate
            // conclusions.  In arbitration mode the legacy confirmation
            // field stays UNSPECIFIED; consumers must inspect the explicit
            // arbitration outcome instead of mistaking it for evidence.
            // `decision` is the evidence-confirmation field.  Arbitration has
            // its own three-valued outcome and must not be collapsed into
            // CONFIRM_NOT_APPLIED (especially for UNKNOWN_SETTLED).
            decision: if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration {
                manager_v1::IssueOperationResolutionDecision::Unspecified as i32
            } else {
                args.decision
            },
            resulting_state: current.state,
            resulting_generation: current.generation,
            verified_issue_revision: issue.revision,
            evidence_refs: args.evidence_refs,
            source_operation_id: args.operation_id.clone(),
            previous_state: source_previous_state,
            previous_generation: source_previous_generation,
            audit_ref: if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration {
                format!("manager-resolution-audit:{resolution_operation_id}")
            } else {
                expected_evidence
            },
            confirmed_at: Some(manager_v1::UtcInstant {
                unix_epoch_millis: unix_now_millis(),
            }),
            source_result_ref: source_result_digest.unwrap_or_default(),
            not_applied_cause: if mode == manager_v1::IssueOperationResolutionMode::HumanArbitration
            {
                format!("HUMAN_ARBITRATION_{arbitration_outcome:?}").to_uppercase()
            } else if !decision {
                "CONFIRMED_NOT_APPLIED".into()
            } else {
                String::new()
            },
            required_remediation: if !decision {
                "none".into()
            } else {
                String::new()
            },
            mode: mode as i32,
            arbitration_outcome: if mode
                == manager_v1::IssueOperationResolutionMode::HumanArbitration
            {
                arbitration_outcome as i32
            } else {
                manager_v1::IssueOperationArbitrationOutcome::Unspecified as i32
            },
            uncertainty_acknowledged: args.uncertainty_acknowledged,
            arbitration_reason: args.arbitration_reason.clone(),
            judgment_actor_id: context
                .actor_audit
                .as_ref()
                .map(|actor| actor.actor_id.clone())
                .unwrap_or_default(),
            evidence_status: evidence_status as i32,
            verified_facts,
            remaining_uncertainties,
            previous_operation_snapshot: Some(previous_operation_snapshot),
            previous_replay_snapshot,
        };
        // The source keeps (or, for CONFIRM_APPLIED, restores) its own
        // method-specific result. The human resolution has a separate
        // durable operation and receipt below; it must never replace the
        // source result.
        resolution_operation.state = manager_v1::DurableOperationState::Committed as i32;
        resolution_operation.generation = resolution_operation
            .generation
            .checked_add(1)
            .ok_or_else(|| Status::internal("resolution operation generation overflow"))?;
        resolution_operation.terminal_snapshot = Some(
            manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(manager_v1::Snapshot {
                payload: receipt.encode_to_vec(),
                schema: "issue_tracker.manager.v1.IssueOperationResolutionResult".into(),
            }),
        );
        let response = manager_v1::ResolveIssueOperationResult {
            outcome: Some(
                manager_v1::resolve_issue_operation_result::Outcome::Success(
                    manager_v1::ManagerMutation {
                        operation: Some(resolution_operation.clone()),
                        result: Some(
                            manager_v1::manager_mutation::Result::IssueOperationResolution(receipt),
                        ),
                        entity_revisions: vec![manager_v1::ManagerEntityRevision {
                            entity_kind: "issue".into(),
                            entity_id: args.issue_id.clone(),
                            revision: issue.revision,
                        }],
                        ..Default::default()
                    },
                ),
            ),
        };
        repository
            .commit_issue_operation_resolution(
                &current,
                &resolution_operation,
                source_result.as_deref(),
                &response.encode_to_vec(),
                &resolution_request,
                if decision {
                    manager_v1::IssueOperationResolutionDecision::ConfirmApplied as i32
                } else {
                    manager_v1::IssueOperationResolutionDecision::ConfirmNotApplied as i32
                },
                mode as i32,
                arbitration_outcome as i32,
                expected_revision,
            )
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    pub(crate) async fn dispatch_manager_for_bound_gateway<A, R>(
        &self,
        method: &str,
        request: A,
    ) -> Result<R, String>
    where
        A: Message,
        R: Message + Default,
    {
        let gateway_id = self
            .gateway_id()
            .ok_or("Gateway worker endpoint is not bound to a gateway")?;
        self.app
            .dispatch_manager_for_gateway(gateway_id, method, request)
            .await
    }

    pub(super) async fn v1<A, R>(
        &self,
        method: &str,
        request: Request<A>,
    ) -> Result<Response<R>, Status>
    where
        A: Message,
        R: Message + Default,
    {
        self.app
            .dispatch_v1(method, request.into_inner())
            .await
            .map(Response::new)
            .map_err(Status::internal)
    }

    pub(super) async fn manager<A, R>(
        &self,
        method: &str,
        request: Request<A>,
    ) -> Result<Response<R>, Status>
    where
        A: Message,
        R: Message + Default,
    {
        self.app
            .dispatch_manager(method, request.into_inner())
            .await
            .map(Response::new)
            .map_err(Status::internal)
    }

    pub(super) async fn gateway_control<A, R>(
        &self,
        method: &str,
        request: Request<A>,
    ) -> Result<Response<R>, Status>
    where
        A: Message,
        R: Message + Default,
    {
        let gateway_id = self
            .gateway_id()
            .ok_or_else(|| Status::failed_precondition("Gateway control endpoint is not bound"))?;
        self.app
            .dispatch_manager_for_gateway(gateway_id, method, request.into_inner())
            .await
            .map(Response::new)
            .map_err(Status::internal)
    }
}
