//! Narrow, read-only access to native Tracker evidence for the primary
//! trusted-consumer boundary.
#![allow(clippy::result_large_err)]

use crate::{
    manager::{self, IssueTrackerManager, gateway::manager_error},
    proto_types::{manager_v1 as pb, rpc_v1, v1},
    server::GrpcServer,
};
use prost::Message;
use tonic::{Request, Response, Status};

const MAX_TRUSTED_READ_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_ISSUE_HISTORY_PAGE_SIZE: u32 = 100;
const DURABLE_BACKEND_REQUEST_SCHEMA: &str = "issue_tracker.manager.v1.DurableBackendRequest";

struct BoundManagerOperation {
    operation: pb::DurableOperation,
    marker_expectation: manager::backend_adapter::MarkerExpectation,
}

fn history_error(error: pb::ManagerError) -> pb::ListIssueHistoryResult {
    pb::ListIssueHistoryResult {
        outcome: Some(pb::list_issue_history_result::Outcome::Error(error)),
    }
}

fn operation_error(error: pb::ManagerError) -> pb::GetDownstreamOperationResult {
    pb::GetDownstreamOperationResult {
        outcome: Some(pb::get_downstream_operation_result::Outcome::Error(error)),
    }
}

fn invalid_binding() -> pb::ManagerError {
    manager_error(
        pb::ManagerErrorCode::MethodTargetMismatch,
        "operation binding does not match the requested Issue",
        false,
    )
}

/// Storage faults are transient and must stay retryable; every other
/// repository error means the persisted binding evidence cannot be trusted.
fn binding_read_error(error: manager::repository::RepositoryError) -> pb::ManagerError {
    match error {
        manager::repository::RepositoryError::Storage(_)
        | manager::repository::RepositoryError::Sqlite(_) => manager_error(
            pb::ManagerErrorCode::BackendUnavailable,
            "Manager operation read is unavailable",
            true,
        ),
        _ => invalid_binding(),
    }
}

fn native_manager_error(error: &v1::IssueTrackerError) -> pb::ManagerError {
    let (code, message, retryable) = match v1::ErrorCode::try_from(error.code) {
        Ok(v1::ErrorCode::IssueNotFound) => (
            pb::ManagerErrorCode::IssueNotFound,
            "Issue was not found",
            false,
        ),
        Ok(v1::ErrorCode::OperationNotFound) => (
            pb::ManagerErrorCode::OperationNotFound,
            "downstream operation was not found",
            false,
        ),
        Ok(v1::ErrorCode::InvalidPageToken) => (
            pb::ManagerErrorCode::InvalidPageToken,
            "history page token is invalid",
            false,
        ),
        Ok(v1::ErrorCode::PageTokenExpired) => (
            pb::ManagerErrorCode::PageTokenExpired,
            "history page token has expired",
            false,
        ),
        Ok(v1::ErrorCode::ResourceLimitExceeded) => (
            pb::ManagerErrorCode::ResourceLimitExceeded,
            "native read exceeded its resource limit",
            false,
        ),
        Ok(v1::ErrorCode::BackendUnavailable) => (
            pb::ManagerErrorCode::BackendUnavailable,
            "native Tracker read is unavailable",
            true,
        ),
        _ => (
            pb::ManagerErrorCode::PreconditionFailed,
            "native Tracker rejected the read",
            false,
        ),
    };
    manager_error(code, message, retryable)
}

fn validate_issue_id(issue_id: &str) -> Result<(), pb::ManagerError> {
    if manager::review_candidates::canonical_uuid(issue_id) {
        Ok(())
    } else {
        Err(manager_error(
            pb::ManagerErrorCode::ManagerInvalidArgument,
            "issue_id must be a canonical UUID",
            false,
        ))
    }
}

/// Validates the trusted read context and derives the native request context
/// that carries only its correlation id.
async fn validate_context(
    manager: &tokio::sync::Mutex<IssueTrackerManager>,
    context: Option<&pb::ManagerReadContext>,
) -> Result<v1::RequestContext, pb::ManagerError> {
    let manager = manager.lock().await;
    manager::gateway::validate_read_context(&manager.gateway_config(), context)?;
    Ok(v1::RequestContext {
        correlation_id: context
            .map(|context| context.correlation_id.clone())
            .unwrap_or_default(),
        ..Default::default()
    })
}

async fn get_bound_manager_operation(
    manager: &tokio::sync::Mutex<IssueTrackerManager>,
    issue_id: &str,
    manager_operation_id: &str,
    downstream_operation_id: &str,
) -> Result<BoundManagerOperation, pb::ManagerError> {
    let mut manager = manager.lock().await;
    let repository = manager.storage_mut().repository();
    let operation = repository
        .get_operation(manager_operation_id)
        .map_err(binding_read_error)?
        .ok_or_else(invalid_binding)?;
    // Only the transition write is currently consumed as trusted evidence.
    if operation.operation_id != manager_operation_id
        || operation.scope != Some(pb::durable_operation::Scope::IssueId(issue_id.to_owned()))
        || operation.downstream_operation_id != downstream_operation_id
        || operation.method != "manager_transition_issue"
        || operation.request_schema != DURABLE_BACKEND_REQUEST_SCHEMA
        || operation.request_digest.is_empty()
    {
        return Err(invalid_binding());
    }

    let audit = operation.audit.as_ref().ok_or_else(invalid_binding)?;
    let request = repository
        .backend_request(manager_operation_id)
        .map_err(binding_read_error)?
        .ok_or_else(invalid_binding)?;
    if manager::backend_dispatch::validate_durable_request_snapshot(&operation.method, &request)
        .is_err()
    {
        return Err(invalid_binding());
    }
    let mut durable = manager::backend_dispatch::DurableBackendRequest::decode(request.as_slice())
        .map_err(|_| invalid_binding())?;
    let transition = durable
        .transition_issue
        .take()
        .ok_or_else(invalid_binding)?;
    let context = durable.context.as_ref().ok_or_else(invalid_binding)?;
    if durable.method != operation.method
        || transition.issue_id != issue_id
        || !matches!(
            v1::IssueState::try_from(transition.target_state),
            Ok(state) if state != v1::IssueState::Unspecified
        )
        || context.target != format!("issue:{issue_id}")
        || audit.method != operation.method
        || audit.target != context.target
        || audit.schema_fingerprint != context.schema_fingerprint
        || audit.actor_audit != context.actor_audit
        || audit.correlation_id != context.correlation_id
        || audit.data_root_id.is_empty()
        || audit.contract_fingerprint.is_empty()
        || audit.schema_fingerprint.is_empty()
        || audit.idempotency_key.is_empty()
    {
        return Err(invalid_binding());
    }

    let manager_request = pb::TransitionIssueArgs {
        context: Some(pb::ManagerWriteContext {
            method: audit.method.clone(),
            target: audit.target.clone(),
            schema_fingerprint: audit.schema_fingerprint.clone(),
            idempotency_key: audit.idempotency_key.clone(),
            actor_audit: audit.actor_audit.clone(),
            correlation_id: audit.correlation_id.clone(),
            data_root_id: audit.data_root_id.clone(),
            contract_fingerprint: audit.contract_fingerprint.clone(),
        }),
        issue_id: transition.issue_id,
        expected_revision: transition.expected_revision,
        target_state: transition.target_state,
        deferred_review_at: transition.deferred_review_at,
        claim_revision: transition.claim_revision,
        terminal_report: transition.terminal_report.map(|report| match report {
            manager::backend_dispatch::durable_transition_issue_request::TerminalReport::Completion(
                value,
            ) => pb::transition_issue_args::TerminalReport::Completion(value),
            manager::backend_dispatch::durable_transition_issue_request::TerminalReport::Cancellation(
                value,
            ) => pb::transition_issue_args::TerminalReport::Cancellation(value),
        }),
        ..Default::default()
    };
    let manager_request_payload = manager_request.encode_to_vec();
    // Fields omitted from the private durable envelope stay absent, so any unrecoverable input
    // presence (for example an empty optional lease handle) fails closed here.
    // `request_digest` hashes the original Manager RPC protobuf, not the private backend envelope.
    if manager::operations::digest_bytes(&manager_request_payload) != operation.request_digest {
        return Err(invalid_binding());
    }
    let mut derived_expectation =
        manager::backend_dispatch::derive_marker_expectation(&operation, &manager_request_payload)
            .map_err(|_| invalid_binding())?;
    // The stored expectation binds the native Tracker contract, not the Manager contract.
    derived_expectation.contract_fingerprint = crate::tracker::config::contract_fingerprint();
    let marker_expectation = repository
        .backend_marker_expectation(manager_operation_id)
        .map_err(binding_read_error)?
        .ok_or_else(invalid_binding)?;
    if marker_expectation != derived_expectation {
        return Err(invalid_binding());
    }

    Ok(BoundManagerOperation {
        operation,
        marker_expectation: derived_expectation,
    })
}

fn validate_native_operation(
    operation: &v1::OperationRecord,
    expected: &manager::backend_adapter::MarkerExpectation,
) -> Result<(), pb::ManagerError> {
    manager::backend_adapter::verify_operation_marker(operation, expected)
        .map_err(|_| invalid_binding())
}

fn request_too_large() -> pb::ManagerError {
    manager_error(
        pb::ManagerErrorCode::ResourceLimitExceeded,
        "trusted read request exceeds the 4 MiB limit",
        false,
    )
}

fn response_too_large() -> pb::ManagerError {
    manager_error(
        pb::ManagerErrorCode::ResourceLimitExceeded,
        "trusted read response exceeds the 4 MiB limit",
        false,
    )
}

fn bound_response<T: Message>(response: T, error: fn(pb::ManagerError) -> T) -> T {
    if response.encoded_len() > MAX_TRUSTED_READ_MESSAGE_BYTES {
        error(response_too_large())
    } else {
        response
    }
}

fn native_read_no_outcome() -> pb::ManagerError {
    manager_error(
        pb::ManagerErrorCode::BackendUnavailable,
        "native Tracker read returned no outcome",
        true,
    )
}

fn native_read_unavailable() -> pb::ManagerError {
    manager_error(
        pb::ManagerErrorCode::BackendUnavailable,
        "native Tracker read is unavailable",
        true,
    )
}

impl GrpcServer {
    /// Common admission for both trusted reads: bounded request size, a
    /// configured Manager, a valid read context, and a canonical Issue id.
    async fn admit_trusted_read(
        &self,
        encoded_len: usize,
        context: Option<&pb::ManagerReadContext>,
        issue_id: &str,
    ) -> Result<(&tokio::sync::Mutex<IssueTrackerManager>, v1::RequestContext), pb::ManagerError>
    {
        if encoded_len > MAX_TRUSTED_READ_MESSAGE_BYTES {
            return Err(request_too_large());
        }
        let manager = self.app.manager.as_ref().ok_or_else(|| {
            manager_error(
                pb::ManagerErrorCode::BackendUnavailable,
                "Manager is not configured",
                true,
            )
        })?;
        let native_context = validate_context(manager, context).await?;
        validate_issue_id(issue_id)?;
        Ok((manager, native_context))
    }

    async fn trusted_issue_history(
        &self,
        args: pb::ListIssueHistoryArgs,
    ) -> Result<v1::HistoryPage, pb::ManagerError> {
        let (manager, native_context) = self
            .admit_trusted_read(args.encoded_len(), args.context.as_ref(), &args.issue_id)
            .await?;
        let page = args.page.ok_or_else(|| {
            manager_error(
                pb::ManagerErrorCode::ManagerInvalidArgument,
                "page request is required",
                false,
            )
        })?;
        if page.page_size == 0 || page.page_size > MAX_ISSUE_HISTORY_PAGE_SIZE {
            return Err(manager_error(
                pb::ManagerErrorCode::ResourceLimitExceeded,
                "history page size must be between 1 and 100",
                false,
            ));
        }
        match (
            args.manager_operation_id.as_deref(),
            args.downstream_operation_id.as_deref(),
        ) {
            (None, None) => {}
            (Some(manager_operation_id), Some(downstream_operation_id))
                if !manager_operation_id.trim().is_empty()
                    && !downstream_operation_id.trim().is_empty() =>
            {
                get_bound_manager_operation(
                    manager,
                    &args.issue_id,
                    manager_operation_id,
                    downstream_operation_id,
                )
                .await?;
            }
            _ => {
                return Err(manager_error(
                    pb::ManagerErrorCode::ManagerInvalidArgument,
                    "Manager and downstream operation IDs must be supplied together",
                    false,
                ));
            }
        }

        let native = self
            .app
            .dispatch_v1::<_, v1::ListIssueHistoryResult>(
                "list_issue_history",
                v1::ListIssueHistoryArgs {
                    context: Some(native_context),
                    issue_id: args.issue_id,
                    page: Some(v1::PageRequest {
                        page_size: page.page_size,
                        page_token: page.page_token,
                    }),
                },
            )
            .await
            .map_err(|_| native_read_unavailable())?;
        match native.outcome {
            Some(v1::list_issue_history_result::Outcome::Success(history)) => Ok(history),
            Some(v1::list_issue_history_result::Outcome::Error(error)) => {
                Err(native_manager_error(&error))
            }
            None => Err(native_read_no_outcome()),
        }
    }

    async fn trusted_downstream_operation(
        &self,
        args: pb::GetDownstreamOperationArgs,
    ) -> Result<pb::TrustedDownstreamOperation, pb::ManagerError> {
        let (manager, native_context) = self
            .admit_trusted_read(args.encoded_len(), args.context.as_ref(), &args.issue_id)
            .await?;
        if args.manager_operation_id.trim().is_empty()
            || args.downstream_operation_id.trim().is_empty()
        {
            return Err(manager_error(
                pb::ManagerErrorCode::ManagerInvalidArgument,
                "Manager and downstream operation IDs are required",
                false,
            ));
        }

        // Verify the immutable Manager binding before asking Tracker to read
        // the root-global downstream operation ID.
        let manager_operation = get_bound_manager_operation(
            manager,
            &args.issue_id,
            &args.manager_operation_id,
            &args.downstream_operation_id,
        )
        .await?;
        let native = self
            .app
            .dispatch_v1::<_, v1::GetOperationResult>(
                "get_operation",
                v1::GetOperationArgs {
                    context: Some(native_context),
                    downstream_operation_id: args.downstream_operation_id,
                },
            )
            .await
            .map_err(|_| native_read_unavailable())?;
        let record = match native.outcome {
            Some(v1::get_operation_result::Outcome::Success(record)) => {
                validate_native_operation(&record, &manager_operation.marker_expectation)?;
                record
            }
            Some(v1::get_operation_result::Outcome::Error(error)) => {
                return Err(native_manager_error(&error));
            }
            None => return Err(native_read_no_outcome()),
        };
        let operation = manager_operation.operation;
        Ok(pb::TrustedDownstreamOperation {
            operation: Some(record),
            binding: Some(pb::ManagerDownstreamOperationBinding {
                manager_operation_id: operation.operation_id,
                issue_id: args.issue_id,
                downstream_operation_id: operation.downstream_operation_id,
                manager_request_digest: operation.request_digest,
                manager_request_schema: operation.request_schema,
            }),
        })
    }
}

#[async_trait::async_trait]
impl rpc_v1::manager_trusted_read_service_server::ManagerTrustedReadService for GrpcServer {
    async fn list_issue_history(
        &self,
        request: Request<pb::ListIssueHistoryArgs>,
    ) -> Result<Response<pb::ListIssueHistoryResult>, Status> {
        let response = match self.trusted_issue_history(request.into_inner()).await {
            Ok(history) => pb::ListIssueHistoryResult {
                outcome: Some(pb::list_issue_history_result::Outcome::Success(history)),
            },
            Err(error) => history_error(error),
        };
        Ok(Response::new(bound_response(response, history_error)))
    }

    async fn get_downstream_operation(
        &self,
        request: Request<pb::GetDownstreamOperationArgs>,
    ) -> Result<Response<pb::GetDownstreamOperationResult>, Status> {
        let response = match self
            .trusted_downstream_operation(request.into_inner())
            .await
        {
            Ok(operation) => pb::GetDownstreamOperationResult {
                outcome: Some(pb::get_downstream_operation_result::Outcome::Success(
                    operation,
                )),
            },
            Err(error) => operation_error(error),
        };
        Ok(Response::new(bound_response(response, operation_error)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker_expectation() -> manager::backend_adapter::MarkerExpectation {
        let issue_id = "06f621a4-4b55-4e09-94b6-2bcffdd235ad";
        manager::backend_adapter::MarkerExpectation {
            operation_id: "backend-op-test".into(),
            method_key: "transition_issue".into(),
            scope: v1::OperationScope::Issue as i32,
            payload_fingerprint: "transition-fingerprint".into(),
            contract_fingerprint: "tracker-contract".into(),
            affected_issue_ids: vec![issue_id.into()],
            entity_revisions: vec![manager::backend_adapter::EntityRevisionExpectation {
                issue_id: issue_id.into(),
                backend_id: issue_id.into(),
                resulting_revision: 1,
            }],
            result_replayable: true,
        }
    }

    fn native_marker(
        expected: &manager::backend_adapter::MarkerExpectation,
    ) -> v1::OperationRecord {
        v1::OperationRecord {
            downstream_operation_id: expected.operation_id.clone(),
            affected_issue_ids: expected.affected_issue_ids.clone(),
            payload_fingerprint: expected.payload_fingerprint.clone(),
            contract_fingerprint: expected.contract_fingerprint.clone(),
            scope: expected.scope,
            method_key: expected.method_key.clone(),
            entity_revisions: expected
                .entity_revisions
                .iter()
                .map(|revision| v1::OperationEntityRevision {
                    issue_id: revision.issue_id.clone(),
                    backend_id: revision.backend_id.clone(),
                    resulting_revision: revision.resulting_revision,
                })
                .collect(),
            commit_durability: v1::OperationCommitDurability::Durable as i32,
            result_replayability: v1::OperationResultReplayability::Replayable as i32,
            result_snapshot: Some(v1::OperationResultSnapshot::default()),
            fingerprint_input: Some(v1::OperationFingerprintInput {
                downstream_operation_id: expected.operation_id.clone(),
                method_key: expected.method_key.clone(),
                scope: expected.scope,
                payload_fingerprint: expected.payload_fingerprint.clone(),
            }),
            ..Default::default()
        }
    }

    fn assert_rejected_native_marker(
        marker: &v1::OperationRecord,
        expected: &manager::backend_adapter::MarkerExpectation,
    ) {
        assert!(matches!(
            validate_native_operation(marker, expected),
            Err(error)
                if error.code == pb::ManagerErrorCode::MethodTargetMismatch as i32
        ));
    }

    #[test]
    fn trusted_operation_reads_require_the_full_native_marker_expectation() {
        let expected = marker_expectation();
        let marker = native_marker(&expected);
        assert!(validate_native_operation(&marker, &expected).is_ok());

        let mut unknown_method_fallback = marker.clone();
        unknown_method_fallback.method_key = "unknown".into();
        assert_rejected_native_marker(&unknown_method_fallback, &expected);

        let mut wrong_method = marker.clone();
        wrong_method.method_key = "update_issue".into();
        assert_rejected_native_marker(&wrong_method, &expected);

        let mut wrong_scope = marker.clone();
        wrong_scope.scope = v1::OperationScope::Tracker as i32;
        assert_rejected_native_marker(&wrong_scope, &expected);

        let mut wrong_payload_fingerprint = marker.clone();
        wrong_payload_fingerprint.payload_fingerprint = "different-request".into();
        assert_rejected_native_marker(&wrong_payload_fingerprint, &expected);

        let mut wrong_contract_fingerprint = marker.clone();
        wrong_contract_fingerprint.contract_fingerprint = "different-contract".into();
        assert_rejected_native_marker(&wrong_contract_fingerprint, &expected);

        let mut wrong_affected_issue = marker.clone();
        wrong_affected_issue.affected_issue_ids = vec!["another-issue".into()];
        assert_rejected_native_marker(&wrong_affected_issue, &expected);

        let mut wrong_fingerprint_input = marker.clone();
        wrong_fingerprint_input
            .fingerprint_input
            .as_mut()
            .expect("native marker fingerprint input")
            .method_key = "update_issue".into();
        assert_rejected_native_marker(&wrong_fingerprint_input, &expected);

        let mut wrong_identity = marker.clone();
        wrong_identity.downstream_operation_id = "different-operation".into();
        assert_rejected_native_marker(&wrong_identity, &expected);

        let mut missing_fingerprint_input = marker.clone();
        missing_fingerprint_input.fingerprint_input = None;
        assert_rejected_native_marker(&missing_fingerprint_input, &expected);

        let mut missing_snapshot = marker;
        missing_snapshot.result_snapshot = None;
        assert_rejected_native_marker(&missing_snapshot, &expected);
    }

    #[test]
    fn binding_reads_keep_transient_storage_failures_retryable() {
        let busy = manager::repository::RepositoryError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        ));
        let error = binding_read_error(busy);
        assert_eq!(error.code, pb::ManagerErrorCode::BackendUnavailable as i32);
        assert!(error.retryable);

        // A tampered snapshot is a deterministic binding failure.
        let tampered = binding_read_error(manager::repository::RepositoryError::RequestSnapshot(
            "backend request integrity validation failed".into(),
        ));
        assert_eq!(
            tampered.code,
            pb::ManagerErrorCode::MethodTargetMismatch as i32
        );
        assert!(!tampered.retryable);
    }

    #[test]
    fn trusted_read_responses_are_replaced_when_the_encoded_body_exceeds_four_mib() {
        let oversized = pb::ListIssueHistoryResult {
            outcome: Some(pb::list_issue_history_result::Outcome::Success(
                v1::HistoryPage {
                    entries: vec![v1::IssueHistoryEntry {
                        reason: "x".repeat(MAX_TRUSTED_READ_MESSAGE_BYTES),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
        };

        let bounded = bound_response(oversized, history_error);
        assert!(matches!(
            bounded.outcome,
            Some(pb::list_issue_history_result::Outcome::Error(ref error))
                if error.code == pb::ManagerErrorCode::ResourceLimitExceeded as i32
                    && error.message == "trusted read response exceeds the 4 MiB limit"
        ));
        assert!(bounded.encoded_len() <= MAX_TRUSTED_READ_MESSAGE_BYTES);

        let oversized_operation = pb::GetDownstreamOperationResult {
            outcome: Some(pb::get_downstream_operation_result::Outcome::Success(
                pb::TrustedDownstreamOperation {
                    operation: Some(v1::OperationRecord {
                        payload_fingerprint: "x".repeat(MAX_TRUSTED_READ_MESSAGE_BYTES),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        };
        let bounded_operation = bound_response(oversized_operation, operation_error);
        assert!(matches!(
            bounded_operation.outcome,
            Some(pb::get_downstream_operation_result::Outcome::Error(ref error))
                if error.code == pb::ManagerErrorCode::ResourceLimitExceeded as i32
                    && error.message == "trusted read response exceeds the 4 MiB limit"
        ));
        assert!(bounded_operation.encoded_len() <= MAX_TRUSTED_READ_MESSAGE_BYTES);
    }
}
