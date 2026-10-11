//! Runner-owned scheduling boundary for manager outbox delivery.

use super::{config::RunnerConfig, *};
use crate::manager::backend_adapter::verify_operation_marker;
use crate::manager::{
    IssueMappingSeed, IssueTrackerManager,
    backend_adapter::{EntityRevisionExpectation, MarkerExpectation},
    backend_dispatch::{DispatchResult, TaskChampionV1Dispatcher, TrackerV1Dispatcher},
    operations::{IdempotencyScope, NewOperation, OperationScope, digest_bytes},
    reconcile::{self, ReconcileReason},
    repository::OwnershipRecoveryState,
};
use prost::Message;
use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{self, Duration},
};

#[path = "outbox_driver/maintenance.rs"]
mod maintenance;
#[path = "outbox_driver/recovery.rs"]
mod recovery;

fn classify_ownership_recovery_manager_error(
    error: Option<&crate::proto_types::manager_v1::ManagerError>,
    expected_revision: u64,
    fallback_message: &str,
) -> TrackerError {
    use crate::proto_types::manager_v1::ManagerErrorCode;

    let message = error
        .map(|error| error.message.as_str())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or(fallback_message)
        .to_owned();
    let code = error.and_then(|error| ManagerErrorCode::try_from(error.code).ok());
    let diagnosis = error.and_then(|error| match error.detail.as_ref() {
        Some(
            crate::proto_types::manager_v1::manager_error::Detail::OwnershipIntegrityDiagnosis(
                diagnosis,
            ),
        ) => Some(diagnosis),
        _ => None,
    });
    let observed_revision = diagnosis.and_then(|diagnosis| diagnosis.observed_revision);
    let revision_conflict_current = error.and_then(|error| match error.detail.as_ref() {
        Some(crate::proto_types::manager_v1::manager_error::Detail::RevisionConflict(conflict)) => {
            Some(conflict.current_revision)
        }
        _ => None,
    });
    let lease_expires_at = diagnosis.and_then(|diagnosis| {
        diagnosis
            .lease_expires_at
            .as_ref()
            .and_then(|instant| u64::try_from(instant.unix_epoch_millis).ok())
            .map(|millis| millis / 1_000)
    });
    match code {
        Some(ManagerErrorCode::RevisionConflict | ManagerErrorCode::GenerationConflict) => {
            TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision_conflict_current
                    .or(observed_revision)
                    .unwrap_or(expected_revision),
            }
        }
        Some(ManagerErrorCode::OutboxLeaseExpired) => TrackerError::InvalidLease {
            lease_expires_at: None,
            observed_revision: None,
        },
        Some(ManagerErrorCode::PreconditionFailed) => {
            let inconsistent_attributes = diagnosis
                .filter(|diagnosis| diagnosis.diagnosis_class == "LEASE_INCONSISTENT")
                .map(|diagnosis| diagnosis.invalid_attributes.clone());
            let inconsistent = inconsistent_attributes.is_some()
                || message.to_ascii_lowercase().contains("inconsistent");
            if inconsistent {
                TrackerError::LeaseInconsistent {
                    invalid_attributes: inconsistent_attributes.unwrap_or_default(),
                    lease_expires_at,
                    observed_revision,
                }
            } else if message.to_ascii_lowercase().contains("lease") {
                TrackerError::InvalidLease {
                    lease_expires_at,
                    observed_revision,
                }
            } else {
                TrackerError::OwnershipRecoveryPermanent {
                    classification: "PERMANENT_RECOVERY_FAILURE".into(),
                    message,
                }
            }
        }
        Some(ManagerErrorCode::BackendUnavailable)
            if error.is_some_and(|error| {
                error.retryable
                    && matches!(
                        error.detail.as_ref(),
                        Some(
                            crate::proto_types::manager_v1::manager_error::Detail::OperationDiagnosis(
                                diagnosis,
                            )
                        ) if diagnosis.classification == "NOT_SENT_RETRY_EXHAUSTED"
                            && diagnosis.resolution_state == "ALL_ATTEMPTS_NOT_SENT"
                            && diagnosis.replayability == "NEW_READMISSION_ONLY"
                    )
            }) => TrackerError::OwnershipRecoveryRetryable(message),
        Some(ManagerErrorCode::RetryExhausted)
            if error.is_some_and(|error| {
                matches!(
                    error.detail.as_ref(),
                    Some(
                        crate::proto_types::manager_v1::manager_error::Detail::OperationDiagnosis(
                            diagnosis,
                        )
                    ) if diagnosis.classification == "NOT_SENT_RETRY_EXHAUSTED"
                        && diagnosis.resolution_state == "ALL_ATTEMPTS_NOT_SENT"
                        && diagnosis.replayability == "NEW_READMISSION_ONLY"
                )
            }) =>
        {
            TrackerError::OwnershipRecoveryRetryable(message)
        }
        Some(ManagerErrorCode::InvalidStateTransition)
        | Some(ManagerErrorCode::ManagerInvalidArgument)
        | Some(ManagerErrorCode::ManagerContractMismatch)
        | Some(ManagerErrorCode::SchemaMismatch)
        | Some(ManagerErrorCode::StoreIdentityMismatch)
        | Some(ManagerErrorCode::IssueNotFound)
        | Some(ManagerErrorCode::ResolutionRequired)
        | Some(ManagerErrorCode::PendingAcceptGuard)
        | None => TrackerError::OwnershipRecoveryPermanent {
            classification: match code {
                Some(ManagerErrorCode::IssueNotFound) => "ISSUE_NOT_FOUND",
                Some(ManagerErrorCode::SchemaMismatch)
                | Some(ManagerErrorCode::ManagerContractMismatch)
                | Some(ManagerErrorCode::StoreIdentityMismatch) => "CONTRACT_MISMATCH",
                Some(ManagerErrorCode::ResolutionRequired) => "RESOLUTION_REQUIRED",
                Some(ManagerErrorCode::PendingAcceptGuard) => "PENDING_ACCEPT_GUARD",
                _ => "PERMANENT_RECOVERY_FAILURE",
            }
            .into(),
            message,
        },
        Some(_) => TrackerError::OwnershipRecoveryPermanent {
            classification: "PERMANENT_RECOVERY_FAILURE".into(),
            message,
        },
    }
}

fn classify_ownership_recovery_runtime_error(message: String) -> TrackerError {
    // `process_operation` includes the southbound call and Manager
    // finalization. A generic runtime error therefore cannot prove that the
    // mutation was not sent or committed; retain the operation fence until a
    // marker/reconciliation path establishes the outcome.
    let _ = message;
    TrackerError::ResultUnconfirmed
}

fn ownership_maintenance_failed(
    result: &Result<super::ownership_maintenance::MaintenanceStats, String>,
) -> bool {
    result.as_ref().map(|stats| stats.failed).unwrap_or(true)
}

pub struct OutboxDriver {
    manager: Arc<Mutex<IssueTrackerManager>>,
    tracker: Tracker,
    dispatcher: TrackerV1Dispatcher,
    active_admin_dispatches: Arc<StdMutex<HashSet<String>>>,
    resolution_recovery_handler: Arc<StdMutex<Option<ResolutionRecoveryHandler>>>,
    #[cfg(test)]
    fail_initial_recovery: AtomicBool,
}

type ResolutionRecoveryHandler = Arc<dyn Fn(usize) -> ResolutionRecoveryFuture + Send + Sync>;
type ResolutionRecoveryFuture = Pin<Box<dyn Future<Output = Result<u32, String>> + Send>>;

/// Result of the shared recovery/delivery/ownership pass.  The server and the
/// static runner have different admission and resolution phases, but the
/// durable work after those phases is identical.  Keeping this report typed
/// lets both callers publish the same counters and health semantics without
/// duplicating the orchestration loop.
pub(crate) struct MaintenanceCycleReport {
    pub(crate) recovered_operations: usize,
    pub(crate) delivery_attempts: u32,
    pub(crate) confirmed_operations: u32,
    pub(crate) delivery_limit_reached: bool,
    pub(crate) recovery_error: Option<String>,
    pub(crate) delivery_error: Option<String>,
    pub(crate) ownership: Result<super::ownership_maintenance::MaintenanceStats, String>,
}

/// Removes a process-local send owner even when the RPC future is cancelled
/// or the final persistence step returns an error. The next maintenance pass
/// can then classify the durable attempt as SENT_UNKNOWN.
struct ActiveAdminDispatchGuard {
    active: Arc<StdMutex<HashSet<String>>>,
    operation_id: String,
}

impl Drop for ActiveAdminDispatchGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.operation_id);
        }
    }
}

/// Handle for the runner-owned resident maintenance task. Dropping the
/// handle aborts the task; callers that own a root should prefer `shutdown`
/// so the task observes its stop signal and joins before the root is closed.
pub struct MaintenanceHandle {
    stop: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl MaintenanceHandle {
    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(join) = self.join.take() {
            join.await.map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryDecision {
    Confirmed,
    Unconfirmed,
    ManualResolution,
}

#[cfg(test)]
fn recovery_decision(
    marker: Option<&v1::OperationRecord>,
    expected: &MarkerExpectation,
) -> RecoveryDecision {
    match marker {
        None => RecoveryDecision::Unconfirmed,
        Some(marker) if verify_operation_marker(marker, expected).is_ok() => {
            RecoveryDecision::Confirmed
        }
        Some(_) => RecoveryDecision::ManualResolution,
    }
}

#[cfg(test)]
#[path = "outbox_driver_tests.rs"]
mod tests;

impl OutboxDriver {
    pub async fn new(
        manager: Arc<Mutex<IssueTrackerManager>>,
        tracker: Tracker,
        config: RunnerConfig,
        #[cfg(test)] fail_initial_recovery: bool,
    ) -> Self {
        Self {
            manager,
            tracker: tracker.clone(),
            dispatcher: TrackerV1Dispatcher::new(tracker, config),
            active_admin_dispatches: Arc::new(StdMutex::new(HashSet::new())),
            resolution_recovery_handler: Arc::new(StdMutex::new(None)),
            #[cfg(test)]
            fail_initial_recovery: AtomicBool::new(fail_initial_recovery),
        }
    }

    /// Register the application-layer manual-resolution implementation. The
    /// driver owns scheduling and persistence, while the trusted consumer
    /// supplies the policy-aware resolver during composition. Keeping this as
    /// an injected callback avoids a Tracker→server module dependency.
    pub fn set_resolution_recovery_handler<F, Fut>(&self, handler: F) -> Result<(), String>
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<u32, String>> + Send + 'static,
    {
        let mut slot = self
            .resolution_recovery_handler
            .lock()
            .map_err(|_| "resolution recovery handler registry is poisoned".to_owned())?;
        *slot = Some(Arc::new(move |limit| Box::pin(handler(limit))));
        Ok(())
    }

    pub(crate) async fn mark_healthy(&self) {
        self.manager.lock().await.activate_outbox_driver();
    }

    /// Stops advertising durable delivery after a failed external maintenance cycle.
    pub(crate) async fn mark_unhealthy(&self) {
        self.manager.lock().await.deactivate_outbox_driver();
    }

    pub(crate) async fn record_phase_health(
        &self,
        phase: &str,
        healthy: bool,
        permit_enabled: bool,
        pending_gauge: u64,
        inconsistent_gauge: u64,
    ) {
        self.record_phase_health_with_deltas(
            phase,
            healthy,
            permit_enabled,
            pending_gauge,
            inconsistent_gauge,
            0,
            0,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_phase_health_with_deltas(
        &self,
        phase: &str,
        healthy: bool,
        permit_enabled: bool,
        pending_gauge: u64,
        inconsistent_gauge: u64,
        skipped_delta: u64,
        conflict_delta: u64,
    ) {
        self.record_phase_health_with_counts(
            phase,
            healthy,
            permit_enabled,
            pending_gauge,
            inconsistent_gauge,
            skipped_delta,
            conflict_delta,
            1,
            u64::from(healthy),
            0,
            0,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn record_phase_health_with_counts(
        &self,
        phase: &str,
        healthy: bool,
        permit_enabled: bool,
        pending_gauge: u64,
        inconsistent_gauge: u64,
        skipped_delta: u64,
        conflict_delta: u64,
        scanned_delta: u64,
        recovered_delta: u64,
        diagnosis_delta: u64,
        unconfirmed_delta: u64,
    ) {
        let mut manager = self.manager.lock().await;
        let mut repository = manager.storage_mut().repository();
        let gauges = repository.phase_health_gauges(phase).ok();
        let gauge_stale = gauges.is_none();
        let (pending_gauge, inconsistent_gauge) = gauges
            .map(|gauges| (gauges.pending, gauges.inconsistent))
            .unwrap_or((pending_gauge, inconsistent_gauge));
        let health = if healthy && !gauge_stale {
            crate::proto_types::manager_v1::HealthStatus::Healthy as i32
        } else {
            crate::proto_types::manager_v1::HealthStatus::Degraded as i32
        };
        let _ =
            repository.record_phase_health_update(crate::manager::repository::PhaseHealthUpdate {
                phase: phase.to_owned(),
                health,
                permit_enabled,
                pending_gauge,
                inconsistent_gauge,
                diagnosis_refs: Vec::new(),
                gauge_stale,
                scanned_delta,
                recovered_delta,
                diagnosis_delta,
                skipped_delta,
                conflict_delta,
                unconfirmed_delta,
            });
    }

    pub(crate) async fn process_one(&self) -> Result<Option<DispatchResult>, String> {
        self.process_prepared(None).await
    }

    pub(crate) async fn dispatch(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
        if crate::manager::requires_capability_dispatch(method, payload) {
            let preparation = {
                let mut manager = self.manager.lock().await;
                manager.prepare_lease_capability(method, payload, Self::now_millis())?
            };
            let prepared = match preparation {
                crate::manager::LeaseCapabilityPreparation::Immediate(result) => {
                    return Ok(result);
                }
                crate::manager::LeaseCapabilityPreparation::Dispatch(prepared) => prepared,
            };
            {
                let mut manager = self.manager.lock().await;
                manager
                    .storage_mut()
                    .repository()
                    .mark_capability_attempt_sending(
                        &prepared.operation.operation_id,
                        prepared.operation.generation,
                        Self::now_millis(),
                    )
                    .map_err(|error| error.to_string())?;
            }
            let response = self
                .dispatcher
                .dispatch_capability(&crate::manager::backend_dispatch::BackendDispatchRequest {
                    method: prepared.method.clone(),
                    payload: prepared.payload.clone(),
                    downstream_operation_id: Some(
                        prepared.operation.downstream_operation_id.clone(),
                    ),
                })
                .await;
            let mut manager = self.manager.lock().await;
            return manager.finalize_lease_capability(prepared, response);
        }
        self.manager.lock().await.dispatch_manager(method, payload)
    }

    pub(crate) async fn process_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<DispatchResult>, String> {
        self.process_prepared(Some(operation_id)).await
    }

    /// Reuse the trusted-consumer resolution implementation from the static
    /// resident runner.  The façade is built from the already-open resources
    /// and is dropped after the bounded pass; no second root or lock is
    /// opened.
    pub(crate) async fn recover_pending_resolutions(&self, limit: usize) -> Result<u32, String> {
        let handler = self
            .resolution_recovery_handler
            .lock()
            .map_err(|_| "resolution recovery handler registry is poisoned".to_owned())?
            .clone();
        match handler {
            Some(handler) => handler(limit).await,
            None => Err("resolution recovery handler is not configured".into()),
        }
    }

    /// Admit and execute the resident ownership repair through the same
    /// Manager-only administrative outbox used by the trusted consumer. The
    /// maintenance scanner owns only the per-Issue fence; it never mutates
    /// Tracker directly once a Manager root is present.
    pub(crate) async fn recover_ownership_via_outbox(
        &self,
        issue_id: uuid::Uuid,
        expected_revision: u64,
        operation_id: String,
        recovery_state: OwnershipRecoveryState,
        reason: super::ownership_maintenance::OwnershipRecoveryReason,
    ) -> Result<(), TrackerError> {
        let issue_id_string = issue_id.to_string();
        let reason = reason.as_str();
        let args = {
            let manager = self.manager.lock().await;
            let identity = manager.config().manager_identity();
            let context = crate::proto_types::manager_v1::ManagerWriteContext {
                method: "manager_apply_issue_admin_action".into(),
                target: format!("issue:{issue_id_string}"),
                schema_fingerprint: identity.schema_fingerprint.clone(),
                idempotency_key: operation_id.clone(),
                actor_audit: Some(crate::proto_types::manager_v1::ActorAudit {
                    actor_id: "system-maintenance".into(),
                    actor_kind: crate::v1::ActorKind::System as i32,
                    reason: Some(reason.into()),
                    ..Default::default()
                }),
                correlation_id: operation_id.clone(),
                data_root_id: identity.data_root_id.clone(),
                contract_fingerprint: identity.contract_fingerprint.clone(),
            };
            crate::proto_types::manager_v1::ApplyIssueAdminActionArgs {
                context: Some(context),
                issue_id: issue_id_string.clone(),
                expected_revision: Some(expected_revision),
                action: crate::proto_types::manager_v1::IssueAdminAction::Recover as i32,
                ..Default::default()
            }
        };
        let request = args.encode_to_vec();
        let request_digest = digest_bytes(&request);
        let context = args
            .context
            .as_ref()
            .ok_or_else(|| TrackerError::InvalidState("maintenance context is missing".into()))?;
        let scope = IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            "manager_apply_issue_admin_action",
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .map_err(|error| TrackerError::InvalidState(error.to_string()))?;
        if !recovery_state.in_flight
            || recovery_state.current_operation_id.as_deref() != Some(operation_id.as_str())
            || recovery_state.issue_id != issue_id_string
            || recovery_state.expected_revision != expected_revision
        {
            return Err(TrackerError::InvalidState(
                "ownership recovery admission state does not match the request".into(),
            ));
        }
        let mut manager = self.manager.lock().await;
        let existing = manager
            .storage_mut()
            .repository()
            .get_operation(&operation_id)
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?;
        if let Some(existing) = existing.as_ref() {
            if existing.method != "manager_apply_issue_admin_action"
                || existing.idempotency_scope != scope.canonical()
                || existing.request_digest != request_digest
                || existing.downstream_operation_id != operation_id
                || existing.scope
                    != Some(
                        crate::proto_types::manager_v1::durable_operation::Scope::IssueId(
                            issue_id_string.clone(),
                        ),
                    )
            {
                return Err(TrackerError::InvalidState(
                    "ownership recovery operation identity is inconsistent".into(),
                ));
            }
            match crate::proto_types::manager_v1::DurableOperationState::try_from(existing.state) {
                Ok(crate::proto_types::manager_v1::DurableOperationState::Committed) => {
                    return Ok(());
                }
                Ok(
                    crate::proto_types::manager_v1::DurableOperationState::Unconfirmed
                    | crate::proto_types::manager_v1::DurableOperationState::ManualResolution,
                ) => return Err(TrackerError::ResultUnconfirmed),
                Ok(crate::proto_types::manager_v1::DurableOperationState::Failed) => {
                    // A terminal failure is not necessarily a revision
                    // conflict.  Re-read its typed error so permanent input
                    // or contract failures are stopped while only an
                    // explicitly retryable backend failure gets backoff.
                    let error = existing
                        .terminal_snapshot
                        .as_ref()
                        .and_then(|snapshot| match snapshot {
                            crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                                snapshot,
                            ) => crate::proto_types::manager_v1::ManagerError::decode(
                                snapshot.payload.as_slice(),
                            )
                            .ok(),
                            _ => None,
                        });
                    return Err(classify_ownership_recovery_manager_error(
                        error.as_ref(),
                        expected_revision,
                        "ownership recovery operation is already terminally failed",
                    ));
                }
                _ => {}
            }
        }
        let had_existing_operation = existing.is_some();
        if existing.is_none() {
            let generation = manager
                .storage()
                .generation()
                .checked_add(1)
                .ok_or_else(|| TrackerError::InvalidState("manager generation overflow".into()))?;
            let sequence = manager.storage().manager_sequence();
            let now_millis = Self::now_millis();
            let operation = NewOperation {
                operation_id: operation_id.clone(),
                scope: Some(OperationScope::Issue(issue_id_string.clone())),
                method: "manager_apply_issue_admin_action".into(),
                idempotency_scope: scope.clone(),
                request_digest,
                request_schema: "issue_tracker.manager.v1.ApplyIssueAdminActionArgs".into(),
                downstream_operation_id: Some(operation_id.clone()),
                generation,
                manager_sequence: sequence,
                causal_parent: None,
                affects_ready: true,
                sync_subject: Some(issue_id_string.clone()),
                affected_issue_ids: vec![issue_id_string.clone()],
                actor_audit: context.actor_audit.clone(),
                contract_fingerprint: manager
                    .config()
                    .manager_identity()
                    .contract_fingerprint
                    .clone(),
                schema_fingerprint: manager
                    .config()
                    .manager_identity()
                    .schema_fingerprint
                    .clone(),
                correlation_id: context.correlation_id.clone(),
                now_millis,
                replayable: true,
            }
            .build()
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?;
            let expectation = MarkerExpectation {
                operation_id: operation_id.to_owned(),
                method_key: "apply_issue_admin_action".into(),
                scope: crate::v1::OperationScope::Issue as i32,
                payload_fingerprint: crate::tracker::operation_log::marker_digest(&[
                    "apply_issue_admin_action",
                    operation_id.as_str(),
                ]),
                contract_fingerprint: crate::tracker::config::contract_fingerprint(),
                affected_issue_ids: vec![issue_id_string.clone()],
                entity_revisions: vec![EntityRevisionExpectation {
                    issue_id: issue_id_string.clone(),
                    backend_id: issue_id_string.clone(),
                    resulting_revision: expected_revision,
                }],
                result_replayable: true,
            };
            let mutation = crate::proto_types::manager_v1::ManagerMutation {
                operation: Some(operation.clone()),
                ..Default::default()
            };
            manager
                .storage_mut()
                .repository()
                .commit_ownership_recovery_admission(
                    &recovery_state,
                    &mutation,
                    &operation,
                    &request,
                    &expectation,
                )
                .map_err(|error| TrackerError::InvalidState(error.to_string()))?;
        }
        drop(manager);
        let processed = self
            .process_operation(&operation_id)
            .await
            .map_err(classify_ownership_recovery_runtime_error)?;
        match processed {
            Some(result)
                if result.state
                    == crate::proto_types::manager_v1::DurableOperationState::Committed =>
            {
                Ok(())
            }
            Some(result) => {
                if result.state
                    == crate::proto_types::manager_v1::DurableOperationState::Unconfirmed
                    || result.state
                        == crate::proto_types::manager_v1::DurableOperationState::ManualResolution
                {
                    return Err(TrackerError::ResultUnconfirmed);
                }
                if let Some(
                    crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                        snapshot,
                    ),
                ) = result.operation.terminal_snapshot.as_ref()
                    && let Ok(error) = crate::proto_types::manager_v1::ManagerError::decode(
                        snapshot.payload.as_slice(),
                    )
                {
                    return Err(classify_ownership_recovery_manager_error(
                        Some(&error),
                        expected_revision,
                        &result.reason,
                    ));
                }
                let message = result.reason;
                Err(classify_ownership_recovery_manager_error(
                    None,
                    expected_revision,
                    &message,
                ))
            }
            None if had_existing_operation => Err(TrackerError::ResultUnconfirmed),
            None => Err(TrackerError::InvalidState(
                "ownership recovery outbox entry was not due".into(),
            )),
        }
    }

    /// Stage Manager-owned outbox state, release the Manager mutex while the
    /// Tracker is awaited, then reacquire it solely for finalization.  This
    /// prevents a slow or unavailable canonical store from blocking unrelated
    /// idempotency, read, and maintenance operations.
    async fn process_prepared(
        &self,
        operation_id: Option<&str>,
    ) -> Result<Option<DispatchResult>, String> {
        let preparation = {
            self.manager
                .lock()
                .await
                .prepare_outbox(operation_id, Self::now_millis())?
        };
        let Some(preparation) = preparation else {
            return Ok(None);
        };
        let mut prepared = match preparation {
            crate::manager::OutboxPreparation::Immediate(result) => return Ok(Some(result)),
            crate::manager::OutboxPreparation::Dispatch(prepared) => *prepared,
        };
        let lease_id = prepared
            .entry
            .lease_id
            .clone()
            .ok_or_else(|| "leased outbox has no lease ID".to_owned())?;

        // A diagnosis reference is part of the admitted administrative
        // request, not a hint that may be ignored by a restarted dispatcher.
        // Re-check it immediately before the first southbound call.  If a
        // different repair already resolved the diagnosis, close this
        // request as a definitive pre-mutation failure and never touch
        // Tracker.
        if prepared.operation.method == "manager_apply_issue_admin_action" {
            let diagnosis_matches = {
                let mut manager = self.manager.lock().await;
                let request = manager
                    .storage_mut()
                    .repository()
                    .admin_action_request(&prepared.operation.operation_id)
                    .map_err(|error| error.to_string())?;
                match request.and_then(|request| request.ownership_diagnosis) {
                    Some(reference) => manager
                        .storage_mut()
                        .repository()
                        .ownership_integrity_diagnosis_matches(&reference)
                        .map_err(|error| error.to_string())?,
                    None => true,
                }
            };
            if !diagnosis_matches {
                let mut operation = prepared.operation.clone();
                operation.state =
                    crate::proto_types::manager_v1::DurableOperationState::Failed as i32;
                operation.failure_class =
                    crate::proto_types::manager_v1::FailureClass::Terminal as i32;
                operation.affects_ready = false;
                operation.generation = operation
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| "administrative operation generation overflow".to_owned())?;
                let error = crate::proto_types::manager_v1::ManagerError {
                    code: crate::proto_types::manager_v1::ManagerErrorCode::ResolutionRequired
                        as i32,
                    message: "ownership diagnosis is stale, missing, or already resolved".into(),
                    retryable: false,
                    detail: None,
                };
                operation.terminal_snapshot = Some(
                    crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                        crate::proto_types::manager_v1::Snapshot {
                            schema: "issue_tracker.manager.v1.ManagerError".into(),
                            payload: error.encode_to_vec(),
                        },
                    ),
                );
                let response = crate::proto_types::manager_v1::ApplyIssueAdminActionResult {
                    outcome: Some(
                        crate::proto_types::manager_v1::apply_issue_admin_action_result::Outcome::Error(
                            error,
                        ),
                    ),
                };
                let response_payload = response.encode_to_vec();
                let operation = {
                    let mut manager = self.manager.lock().await;
                    manager
                        .storage_mut()
                        .repository()
                        .finalize_admin_backend_dispatch(
                            &operation,
                            &prepared.entry,
                            &response_payload,
                        )
                        .map_err(|error| error.to_string())?;
                    manager
                        .storage_mut()
                        .repository()
                        .get_operation(&prepared.operation.operation_id)
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| {
                            "administrative operation disappeared after rejection".to_owned()
                        })?
                };
                return Ok(Some(DispatchResult {
                    operation,
                    state: crate::proto_types::manager_v1::DurableOperationState::Failed,
                    reason: "ownership diagnosis is stale, missing, or already resolved".into(),
                    response_payload,
                    ephemeral_claim_handle: None,
                    issue_mappings: Vec::new(),
                }));
            }
        }
        let active_owner = if prepared.operation.method == "manager_apply_issue_admin_action" {
            let active_owner = {
                let mut active = self
                    .active_admin_dispatches
                    .lock()
                    .map_err(|_| "active admin dispatch registry is poisoned".to_owned())?;
                active.insert(prepared.operation.operation_id.clone());
                ActiveAdminDispatchGuard {
                    active: Arc::clone(&self.active_admin_dispatches),
                    operation_id: prepared.operation.operation_id.clone(),
                }
            };
            let lease_expires_at = prepared
                .entry
                .lease_expires_at
                .as_ref()
                .map(|value| value.unix_epoch_millis);
            self.manager
                .lock()
                .await
                .storage_mut()
                .repository()
                .record_admin_dispatch_attempt(
                    &prepared.operation.operation_id,
                    prepared.operation.generation,
                    &prepared.entry.outbox_id,
                    crate::proto_types::manager_v1::OutboxStatus::Dispatched as i32,
                    Some(&lease_id),
                    lease_expires_at,
                    None,
                    Some("SENDING"),
                )
                .map_err(|error| error.to_string())?;
            Some(active_owner)
        } else {
            None
        };
        let result = match crate::manager::backend_dispatch::dispatch_leased(
            &self.dispatcher,
            crate::manager::backend_dispatch::DispatchLeaseInput {
                entry: &mut prepared.entry,
                operation: &prepared.operation,
                request: prepared.request.clone(),
                expected: prepared.expected.clone(),
                lease_id: &lease_id,
                retry_budget: &prepared.retry_budget,
                now_millis: prepared.now_millis,
            },
        )
        .await
        {
            Ok(result) => result,
            Err(crate::manager::backend_dispatch::BackendDispatchError::InvalidRequest(_)) => {
                let (operation, _) = self
                    .manager
                    .lock()
                    .await
                    .storage_mut()
                    .repository()
                    .quarantine_outbox(
                        &prepared.operation,
                        &prepared.entry,
                        prepared.now_millis,
                        "backend outbox integrity validation failed",
                    )
                    .map_err(|error| error.to_string())?;
                return Ok(Some(crate::manager::quarantined_dispatch_result(
                    operation,
                    "backend outbox integrity validation failed",
                )));
            }
            Err(error) => return Err(error.to_string()),
        };
        self.manager
            .lock()
            .await
            .finalize_prepared_outbox(prepared, result.clone())?;
        drop(active_owner);
        Ok(Some(result))
    }

    /// Confirm an admitted backend mutation before returning it northbound.
    /// PENDING is never treated as success.
    pub(crate) async fn complete_admitted(
        &self,
        method: &str,
        admitted: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        let Some(operation_id) = pending_backend_operation_id(&admitted) else {
            return Ok(admitted);
        };
        let Some(processed) = self.process_operation(&operation_id).await? else {
            return Ok(crate::manager::manager_error_result(
                method,
                result_unconfirmed_error("admitted backend operation was not due for delivery"),
            ));
        };
        if processed.state != crate::proto_types::manager_v1::DurableOperationState::Committed {
            let error = match processed.operation.terminal_snapshot.as_ref() {
                Some(
                    crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
                        snapshot,
                    ),
                ) => crate::proto_types::manager_v1::ManagerError::decode(snapshot.payload.as_slice())
                    .unwrap_or_else(|_| result_unconfirmed_error(processed.reason.clone())),
                _ => result_unconfirmed_error(format!(
                    "backend operation {} is not confirmed: {}",
                    processed.operation.operation_id, processed.reason
                )),
            };
            return Ok(crate::manager::manager_error_result(method, error));
        }

        let mut mutation = ManagerMutationResult::decode(admitted.as_slice())
            .map_err(|error| error.to_string())?;
        let manager_mutation = mutation
            .mutation
            .as_mut()
            .ok_or_else(|| "backend admission result is missing mutation".to_owned())?;
        if let Some(
            crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(
                snapshot,
            ),
        ) = processed.operation.terminal_snapshot.as_ref()
            && let Ok(confirmed) =
                crate::proto_types::manager_v1::ManagerMutation::decode(snapshot.payload.as_slice())
            && confirmed.result.is_some()
        {
            manager_mutation.result = confirmed.result;
        }
        manager_mutation.operation = Some(processed.operation);
        Ok(mutation.encode_to_vec())
    }
}

#[derive(prost::Message)]
struct ManagerMutationResult {
    #[prost(message, optional, tag = "1")]
    mutation: Option<crate::proto_types::manager_v1::ManagerMutation>,
}

fn pending_backend_operation_id(result: &[u8]) -> Option<String> {
    ManagerMutationResult::decode(result)
        .ok()
        .and_then(|mutation| mutation.mutation)
        .and_then(|mutation| mutation.operation)
        .filter(|operation| {
            matches!(
                crate::proto_types::manager_v1::DurableOperationState::try_from(operation.state),
                Ok(crate::proto_types::manager_v1::DurableOperationState::Received)
                    | Ok(crate::proto_types::manager_v1::DurableOperationState::Pending)
            )
        })
        .map(|operation| operation.operation_id)
        .filter(|operation_id| !operation_id.is_empty())
}

fn result_unconfirmed_error(
    message: impl Into<String>,
) -> crate::proto_types::manager_v1::ManagerError {
    crate::proto_types::manager_v1::ManagerError {
        code: crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed as i32,
        message: message.into(),
        retryable: true,
        detail: None,
    }
}
