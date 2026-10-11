//! Internal typed gateway for the manager contract.
//!
//! The gateway is deliberately not exposed through a plugin runner. It is the
//! admission boundary used by the future manager lifecycle: protobuf decode,
//! context checks, secret filtering, idempotency, and typed repository/backend
//! dispatch all happen here before a method reaches storage.
//!
//! ManagerError is the stable protobuf error outcome used throughout this
//! internal boundary. Boxing it here would only add conversion noise before
//! encoding the same wire value, so the typed contract is kept directly.
#![allow(clippy::result_large_err, clippy::needless_question_mark)]

use super::{
    operations::{MutationSnapshot, digest_bytes, new_operation_id},
    pagination::PageSizeLimits,
};
use crate::{proto_types::manager_v1 as pb, v1};
use prost::Message;

/// Configuration copied from the validated manager settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManagerGatewayConfig {
    pub contract_fingerprint: String,
    pub schema_fingerprint: String,
    pub data_root_id: String,
    pub maximum_payload_bytes: u64,
    pub root_operation_history_max_window_millis: i64,
    pub operation_history_v1_compat_enabled: bool,
    pub page_size_limits: PageSizeLimits,
    /// Whether normal durable Manager writes may be admitted. This is
    /// deliberately separate from read availability: an ownership
    /// maintenance failure must fail closed for new durable writes while
    /// still allowing reads and the administrative recovery paths to repair
    /// the condition.
    pub write_admission: bool,
}

/// Idempotency reservation result returned by a repository.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum IdempotencyDecision {
    New,
    /// The bytes are the complete typed result for the invoked manager method.
    Replay(Vec<u8>),
    /// The domain mutation was durably committed but the response was lost
    /// before its idempotency record was finalized. The gateway can rebuild
    /// the exact typed response from the immutable mutation snapshot.
    Pending {
        operation: Box<pb::DurableOperation>,
        mutation_snapshot: Box<pb::Snapshot>,
    },
}

/// Repository boundary required by the internal gateway.
///
/// Default implementations are intentionally typed failures.  A repository
/// can therefore be introduced incrementally without making an unimplemented
/// method look like a generic dispatcher error.
pub(crate) trait ManagerRepository {
    fn reserve_idempotency(
        &mut self,
        scope: &str,
        request_digest: &str,
        operation_id: &str,
    ) -> Result<IdempotencyDecision, pb::ManagerError>;

    fn complete_idempotency(
        &mut self,
        scope: &str,
        method: &str,
        request_digest: &str,
        operation_id: &str,
        snapshot: &pb::Snapshot,
    ) -> Result<pb::DurableOperation, pb::ManagerError>;

    fn store_idempotent_result(
        &mut self,
        scope: &str,
        request_digest: &str,
        operation_id: &str,
        result: &[u8],
    ) -> Result<(), pb::ManagerError>;

    /// Remove a reservation when a new request is rejected before any
    /// Manager operation is created. Existing replay/pending reservations
    /// must remain untouched so their durable result can still be read while
    /// new writes are blocked.
    fn abort_idempotency_reservation(
        &mut self,
        _scope: &str,
        _request_digest: &str,
        _operation_id: &str,
    ) -> Result<(), pb::ManagerError> {
        Err(backend_unavailable("manager_abort_idempotency_reservation"))
    }

    fn create_candidate(
        &mut self,
        _args: &pb::CreateCandidateArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        Err(backend_unavailable("manager_create_candidate"))
    }
    fn update_candidate(
        &mut self,
        _args: &pb::UpdateCandidateArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        Err(backend_unavailable("manager_update_candidate"))
    }
    fn resolve_candidate(
        &mut self,
        _args: &pb::ResolveCandidateArgs,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        Err(backend_unavailable("manager_resolve_candidate"))
    }

    #[allow(dead_code)]
    fn resolve_candidate_replay(
        &mut self,
        _scope: &str,
        _request_digest: &str,
    ) -> Result<Option<pb::ResolutionReceipt>, pb::ManagerError> {
        Err(backend_unavailable("manager_resolve_candidate"))
    }

    #[allow(dead_code)]
    fn validate_resolution_causal_parent(
        &self,
        _args: &pb::ResolveCandidateArgs,
    ) -> Result<(), pb::ManagerError> {
        Err(backend_unavailable("manager_resolve_candidate"))
    }

    #[allow(dead_code)]
    fn validate_resolution_admission(
        &self,
        _args: &pb::ResolveCandidateArgs,
        _context: &pb::ManagerWriteContext,
        _candidate: &pb::Candidate,
    ) -> Result<(), pb::ManagerError> {
        Err(backend_unavailable("manager_resolve_candidate"))
    }

    fn get_candidate(
        &self,
        _args: &pb::GetCandidateArgs,
    ) -> Result<pb::Candidate, pb::ManagerError> {
        Err(backend_unavailable("manager_get_candidate"))
    }
    fn list_candidates(
        &self,
        _args: &pb::ListCandidatesArgs,
        _page_size_limits: PageSizeLimits,
    ) -> Result<pb::CandidatePage, pb::ManagerError> {
        Err(backend_unavailable("manager_list_candidates"))
    }

    fn create_extraction_run(
        &mut self,
        _args: &pb::CreateExtractionRunArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        Err(backend_unavailable("manager_create_extraction_run"))
    }
    fn advance_extraction_run(
        &mut self,
        _args: &pb::AdvanceExtractionRunArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        Err(backend_unavailable("manager_advance_extraction_run"))
    }
    fn resume_extraction_run(
        &mut self,
        _args: &pb::ResumeExtractionRunArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        Err(backend_unavailable("manager_resume_extraction_run"))
    }
    fn get_extraction_run(
        &self,
        _args: &pb::GetExtractionRunArgs,
    ) -> Result<pb::ExtractionRun, pb::ManagerError> {
        Err(backend_unavailable("manager_get_extraction_run"))
    }
    fn list_extraction_runs(
        &self,
        _args: &pb::ListExtractionRunsArgs,
        _page_size_limits: PageSizeLimits,
    ) -> Result<pb::ExtractionRunPage, pb::ManagerError> {
        Err(backend_unavailable("manager_list_extraction_runs"))
    }

    fn get_operation(
        &self,
        _args: &pb::GetOperationArgs,
    ) -> Result<pb::DurableOperation, pb::ManagerError> {
        Err(backend_unavailable("manager_get_operation"))
    }

    /// Extension point for manager metadata supplied by the active service.
    fn get_contract_info(
        &self,
        _args: &pb::GetContractInfoArgs,
    ) -> Result<pb::ContractInfo, pb::ManagerError> {
        Err(backend_unavailable("manager_get_contract_info"))
    }

    /// Extension point for synchronized issue views supplied by the backend.
    fn get_issue_with_sync(
        &self,
        _args: &pb::GetIssueWithSyncArgs,
    ) -> Result<pb::IssueWithSync, pb::ManagerError> {
        Err(backend_unavailable("manager_get_issue_with_sync"))
    }
    fn list_issues_with_sync(
        &self,
        _args: &pb::ListIssuesWithSyncArgs,
    ) -> Result<Vec<pb::IssueWithSync>, pb::ManagerError> {
        Err(backend_unavailable("manager_list_issues_with_sync"))
    }
    fn list_operation_history(
        &self,
        _args: &pb::ListOperationHistoryArgs,
        _root_operation_history_max_window_millis: i64,
        _page_size_limits: PageSizeLimits,
    ) -> Result<pb::OperationHistoryPage, pb::ManagerError> {
        Err(backend_unavailable("manager_list_operation_history"))
    }
    fn list_operation_history_with_compat(
        &self,
        args: &pb::ListOperationHistoryArgs,
        root_operation_history_max_window_millis: i64,
        page_size_limits: PageSizeLimits,
        _operation_history_v1_compat_enabled: bool,
    ) -> Result<pb::OperationHistoryPage, pb::ManagerError> {
        self.list_operation_history(
            args,
            root_operation_history_max_window_millis,
            page_size_limits,
        )
    }

    /// Extension points for lifecycle operations owned by the backend service.
    fn retry_operation(
        &mut self,
        _args: &pb::RetryOperationArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        Err(backend_unavailable("manager_retry_operation"))
    }
    fn reconcile_operation(
        &mut self,
        _args: &pb::ReconcileOperationArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        Err(backend_unavailable("manager_reconcile_operation"))
    }

    /// Dispatch a canonical backend mutation after manager admission.
    fn dispatch_backend(
        &mut self,
        method: &str,
        _args: &[u8],
        _admission: &Admission,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        Err(backend_unavailable(method))
    }
}

/// Internal Gateway.  The type is generic so SQLite and in-memory test
/// repositories can be supplied without coupling this boundary to storage.
macro_rules! decode_mutation {
    ($gateway:expr, $args:ty, $payload:expr, $method:literal, $repo_method:ident, $wrapper:ident) => {{
        let args = match <$args>::decode($payload) {
            Ok(args) => args,
            Err(error) => {
                return Ok($gateway
                    .typed_error_for_method($method, invalid_argument(error.to_string()))?);
            }
        };
        let context = args.context.clone();
        return Ok($gateway.mutation(
            $method,
            context.as_ref(),
            $payload,
            $wrapper,
            |repository, _admission| repository.$repo_method(&args),
        ));
    }};
}

macro_rules! decode_read {
    ($gateway:expr, $args:ty, $payload:expr, $repo_method:ident, $result:path, $wrapper:ident, $value:ident) => {{
        let args = match <$args>::decode($payload) {
            Ok(args) => args,
            Err(error) => {
                return Ok(read_result::<$result>(
                    invalid_argument(error.to_string()),
                    0,
                ));
            }
        };
        if let Err(error) = $gateway.admit_read(args.context.as_ref()) {
            return Ok(read_result::<$result>(error, 0));
        }
        return Ok($wrapper($gateway.repository.$repo_method(&args)));
    }};
}

pub(crate) struct ManagerGateway<R> {
    repository: R,
    config: ManagerGatewayConfig,
}

impl<R: ManagerRepository> ManagerGateway<R> {
    pub(crate) fn new(repository: R, config: ManagerGatewayConfig) -> Self {
        Self { repository, config }
    }

    // Exposed only to in-crate gateway fixtures for observing repository
    // calls; production dispatch never inspects the adapter directly.
    #[allow(dead_code)]
    pub(crate) fn repository(&self) -> &R {
        &self.repository
    }

    #[allow(dead_code)]
    pub(crate) fn repository_mut(&mut self) -> &mut R {
        &mut self.repository
    }

    /// Decode and dispatch one manager method to its typed protobuf result.
    /// Unknown method names remain transport errors because there is no result
    /// protobuf from which a typed error can be constructed.
    pub(crate) fn dispatch(&mut self, method: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
        if payload.len() as u64 > self.config.maximum_payload_bytes {
            return self.typed_error_for_method(
                method,
                manager_error(
                    pb::ManagerErrorCode::ResourceLimitExceeded,
                    "manager request exceeds the configured payload limit",
                    false,
                ),
            );
        }
        match method {
            "manager_create_candidate" => self.create_candidate(payload),
            "manager_update_candidate" => self.update_candidate(payload),
            "manager_resolve_candidate" => self.resolve_candidate(payload),
            "manager_get_candidate" => self.get_candidate(payload),
            "manager_list_candidates" => self.list_candidates(payload),
            "manager_create_extraction_run" => self.create_extraction_run(payload),
            "manager_advance_extraction_run" => self.advance_extraction_run(payload),
            "manager_resume_extraction_run" => self.resume_extraction_run(payload),
            "manager_get_extraction_run" => self.get_extraction_run(payload),
            "manager_list_extraction_runs" => self.list_extraction_runs(payload),
            "manager_get_operation" => self.get_operation(payload),
            "manager_create_issue"
            | "manager_update_issue"
            | "manager_transition_issue"
            | "manager_claim_issue"
            | "manager_heartbeat_issue_claim"
            | "manager_release_issue_claim"
            | "manager_reconcile_expired_claim"
            | "manager_add_issue_relation"
            | "manager_remove_issue_relation"
            | "manager_add_issue_comment" => self.backend_mutation(method, payload),
            "manager_get_contract_info" => self.get_contract_info(payload),
            "manager_get_issue_with_sync" => self.get_issue_with_sync(payload),
            "manager_list_issues_with_sync" => self.list_issues_with_sync(payload),
            "manager_list_operation_history" => self.list_operation_history(payload),
            "manager_retry_operation" => self.retry_operation(payload),
            "manager_reconcile_operation" => self.reconcile_operation(payload),
            _ => Err(format!("unknown manager method: {method}")),
        }
    }

    fn mutation<F>(
        &mut self,
        method: &str,
        context: Option<&pb::ManagerWriteContext>,
        payload: &[u8],
        wrap: fn(Result<pb::ManagerMutation, pb::ManagerError>) -> Vec<u8>,
        operation: F,
    ) -> Vec<u8>
    where
        F: FnOnce(&mut R, &Admission) -> Result<pb::ManagerMutation, pb::ManagerError>,
    {
        let admission = match self.admit_write(method, context, payload) {
            Ok(value) => value,
            Err(error) => return wrap(Err(error)),
        };
        let mutation = match admission.decision {
            IdempotencyDecision::Replay(result) => return result,
            IdempotencyDecision::Pending {
                operation,
                mutation_snapshot,
            } => {
                let persisted_operation_id = operation.operation_id.clone();
                let mut mutation =
                    match pb::ManagerMutation::decode(mutation_snapshot.payload.as_slice()) {
                        Ok(value) => value,
                        Err(error) => {
                            return wrap(Err(manager_error(
                                pb::ManagerErrorCode::ResolutionRequired,
                                format!("stored idempotency mutation cannot be decoded: {error}"),
                                false,
                            )));
                        }
                    };
                mutation.operation = Some(*operation);
                let result = wrap(Ok(mutation));
                if let Err(error) = self.repository.store_idempotent_result(
                    &admission.scope,
                    &admission.request_digest,
                    &persisted_operation_id,
                    &result,
                ) {
                    return wrap(Err(error));
                }
                return result;
            }
            IdempotencyDecision::New => match operation(&mut self.repository, &admission) {
                Ok(value) => value,
                Err(error) => return wrap(Err(error)),
            },
        };
        let snapshot = match MutationSnapshot::from_mutation(&redact_canonical_issue(&mutation)) {
            Ok(snapshot) => snapshot.snapshot,
            Err(error) => {
                return wrap(Err(manager_error(
                    pb::ManagerErrorCode::ManagerInvalidArgument,
                    error.to_string(),
                    false,
                )));
            }
        };
        let operation = match self.repository.complete_idempotency(
            &admission.scope,
            method,
            &admission.request_digest,
            &admission.operation_id,
            &snapshot,
        ) {
            Ok(operation) => operation,
            Err(error) => return wrap(Err(error)),
        };
        let mut mutation = mutation;
        mutation.operation = Some(operation);
        let terminally_confirmed = mutation.operation.as_ref().is_some_and(|operation| {
            operation.state == pb::DurableOperationState::Committed as i32
        });
        let result = wrap(Ok(mutation));
        // Backend admissions remain PENDING until marker reconciliation.  A
        // result encoded at admission time is not a terminal backend result
        // and must never become replayable.  Manager-only mutations are
        // already CONFIRMED by their single repository transaction.
        if terminally_confirmed {
            if let Err(error) = self.repository.store_idempotent_result(
                &admission.scope,
                &admission.request_digest,
                &admission.operation_id,
                &result,
            ) {
                return wrap(Err(error));
            }
        }
        result
    }

    /// Admit a candidate acceptance without finalizing a typed result. The
    /// acceptance operation is pending until the backend marker is confirmed;
    /// persisting this response as an idempotency replay would expose an
    /// unconfirmed candidate decision after restart.
    fn backend_mutation(&mut self, method: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
        let error_result = |error| mutation_result(method, Err(error));
        if let Err(error) = super::backend_dispatch::validate_backend_admission(method, payload) {
            return Ok(error_result(backend_capability_unavailable_for(
                method, error,
            )));
        }
        let context = decode_write_context(method, payload);
        let context = match context {
            Ok(context) => context,
            Err(error) => return Ok(error_result(error)),
        };
        let wrap = mutation_wrapper_for_method(method)
            .ok_or_else(|| format!("unknown manager mutation method: {method}"))?;
        Ok(self.mutation(
            method,
            Some(&context),
            payload,
            wrap,
            |repository, admission| repository.dispatch_backend(method, payload, admission),
        ))
    }

    fn admit_write(
        &mut self,
        method: &str,
        context: Option<&pb::ManagerWriteContext>,
        payload: &[u8],
    ) -> Result<Admission, pb::ManagerError> {
        let context = context.ok_or_else(|| invalid_argument("write context is required"))?;
        validate_write_context(&self.config, method, context)?;
        if contains_secret(payload) {
            return Err(manager_error(
                pb::ManagerErrorCode::ManagerSecretDetected,
                "secret-like value detected before manager admission",
                false,
            ));
        }
        let scope = super::operations::IdempotencyScope::new(
            self.config.contract_fingerprint.clone(),
            self.config.data_root_id.clone(),
            method,
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .map_err(|error| invalid_argument(error.to_string()))?;
        let scope = scope.canonical();
        let request_digest = digest_bytes(payload);
        let operation_id = new_operation_id();
        let decision =
            self.repository
                .reserve_idempotency(&scope, &request_digest, &operation_id)?;
        if !self.config.write_admission && matches!(decision, IdempotencyDecision::New) {
            self.repository.abort_idempotency_reservation(
                &scope,
                &request_digest,
                &operation_id,
            )?;
            return Err(manager_error(
                pb::ManagerErrorCode::BackendUnavailable,
                "manager write admission is blocked while ownership maintenance is unhealthy",
                true,
            ));
        }
        Ok(Admission {
            scope,
            request_digest,
            operation_id,
            decision,
        })
    }

    pub(crate) fn admit_read(
        &self,
        context: Option<&pb::ManagerReadContext>,
    ) -> Result<(), pb::ManagerError> {
        validate_read_context(&self.config, context)
    }

    fn typed_error_for_method(
        &self,
        method: &str,
        error: pb::ManagerError,
    ) -> Result<Vec<u8>, String> {
        let result = match method {
            "manager_create_candidate" => {
                mutation_result_with::<pb::CreateCandidateResult>(error, true)
            }
            "manager_update_candidate" => {
                mutation_result_with::<pb::UpdateCandidateResult>(error, true)
            }
            "manager_resolve_candidate" => {
                mutation_result_with::<pb::ResolveCandidateResult>(error, true)
            }
            "manager_get_candidate" => read_result::<pb::GetCandidateResult>(error, 0),
            "manager_list_candidates" => read_result::<pb::ListCandidatesResult>(error, 0),
            "manager_create_extraction_run" => {
                mutation_result_with::<pb::CreateExtractionRunResult>(error, true)
            }
            "manager_advance_extraction_run" => {
                mutation_result_with::<pb::AdvanceExtractionRunResult>(error, true)
            }
            "manager_resume_extraction_run" => {
                mutation_result_with::<pb::ResumeExtractionRunResult>(error, true)
            }
            "manager_get_extraction_run" => read_result::<pb::GetExtractionRunResult>(error, 0),
            "manager_list_extraction_runs" => read_result::<pb::ListExtractionRunsResult>(error, 0),
            "manager_get_operation" => read_result::<pb::GetOperationResult>(error, 0),
            "manager_get_contract_info" => read_result::<pb::GetContractInfoResult>(error, 0),
            "manager_create_issue" => mutation_result_with::<pb::CreateIssueResult>(error, true),
            "manager_update_issue" => mutation_result_with::<pb::UpdateIssueResult>(error, true),
            "manager_transition_issue" => {
                mutation_result_with::<pb::TransitionIssueResult>(error, true)
            }
            "manager_claim_issue" => mutation_result_with::<pb::ClaimIssueResult>(error, true),
            "manager_heartbeat_issue_claim" => {
                mutation_result_with::<pb::HeartbeatIssueClaimResult>(error, true)
            }
            "manager_release_issue_claim" => {
                mutation_result_with::<pb::ReleaseIssueClaimResult>(error, true)
            }
            "manager_reconcile_expired_claim" => {
                mutation_result_with::<pb::ReconcileExpiredClaimResult>(error, true)
            }
            "manager_add_issue_relation" => {
                mutation_result_with::<pb::AddIssueRelationResult>(error, true)
            }
            "manager_remove_issue_relation" => {
                mutation_result_with::<pb::RemoveIssueRelationResult>(error, true)
            }
            "manager_add_issue_comment" => {
                mutation_result_with::<pb::AddIssueCommentResult>(error, true)
            }
            "manager_get_issue_with_sync" => read_result::<pb::GetIssueWithSyncResult>(error, 0),
            "manager_list_issues_with_sync" => {
                read_result::<pb::ListIssuesWithSyncResult>(error, 0)
            }
            "manager_list_operation_history" => {
                read_result::<pb::ListOperationHistoryResult>(error, 0)
            }
            "manager_retry_operation" => {
                mutation_result_with::<pb::RetryOperationResult>(error, true)
            }
            "manager_reconcile_operation" => {
                mutation_result_with::<pb::ReconcileOperationResult>(error, true)
            }
            _ => return Err(format!("unknown manager method: {method}")),
        };
        Ok(result)
    }

    // The following method bodies are deliberately small: they only decode,
    // validate the read context, and delegate the domain operation.
    fn create_candidate(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_mutation!(
            self,
            pb::CreateCandidateArgs,
            payload,
            "manager_create_candidate",
            create_candidate,
            create_candidate_result
        );
    }
    fn update_candidate(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_mutation!(
            self,
            pb::UpdateCandidateArgs,
            payload,
            "manager_update_candidate",
            update_candidate,
            update_candidate_result
        );
    }
    fn resolve_candidate(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        let args = match pb::ResolveCandidateArgs::decode(payload) {
            Ok(args) => args,
            Err(error) => {
                return Ok(self.typed_error_for_method(
                    "manager_resolve_candidate",
                    invalid_argument(error.to_string()),
                )?);
            }
        };
        let context = match args.context.as_ref() {
            Some(context) => context,
            None => {
                return self.typed_error_for_method(
                    "manager_resolve_candidate",
                    invalid_argument("write context is required"),
                );
            }
        };
        if let Err(error) =
            validate_write_context(&self.config, "manager_resolve_candidate", context)
        {
            return self.typed_error_for_method("manager_resolve_candidate", error);
        }
        if !self.config.write_admission {
            return self.typed_error_for_method(
                "manager_resolve_candidate",
                manager_error(
                    pb::ManagerErrorCode::BackendUnavailable,
                    "manager write admission is blocked while ownership maintenance is unhealthy",
                    true,
                ),
            );
        }
        if contains_secret(payload) {
            return self.typed_error_for_method(
                "manager_resolve_candidate",
                manager_error(
                    pb::ManagerErrorCode::ManagerSecretDetected,
                    "secret-like value detected before manager admission",
                    false,
                ),
            );
        }
        match self.repository.resolve_candidate(&args) {
            Ok(receipt) => Ok(pb::ResolveCandidateResult {
                outcome: Some(pb::resolve_candidate_result::Outcome::Success(receipt)),
            }
            .encode_to_vec()),
            Err(error) => self.typed_error_for_method("manager_resolve_candidate", error),
        }
    }
    fn create_extraction_run(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_mutation!(
            self,
            pb::CreateExtractionRunArgs,
            payload,
            "manager_create_extraction_run",
            create_extraction_run,
            create_extraction_run_result
        );
    }
    fn advance_extraction_run(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_mutation!(
            self,
            pb::AdvanceExtractionRunArgs,
            payload,
            "manager_advance_extraction_run",
            advance_extraction_run,
            advance_extraction_run_result
        );
    }
    fn resume_extraction_run(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_mutation!(
            self,
            pb::ResumeExtractionRunArgs,
            payload,
            "manager_resume_extraction_run",
            resume_extraction_run,
            resume_extraction_run_result
        );
    }

    fn get_candidate(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_read!(
            self,
            pb::GetCandidateArgs,
            payload,
            get_candidate,
            pb::GetCandidateResult,
            get_candidate_result,
            Candidate
        );
    }
    fn list_candidates(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        let args = match pb::ListCandidatesArgs::decode(payload) {
            Ok(args) => args,
            Err(error) => {
                return Ok(read_result::<pb::ListCandidatesResult>(
                    invalid_argument(error.to_string()),
                    0,
                ));
            }
        };
        if let Err(error) = self.admit_read(args.context.as_ref()) {
            return Ok(read_result::<pb::ListCandidatesResult>(error, 0));
        }
        let page = match self
            .repository
            .list_candidates(&args, self.config.page_size_limits)
        {
            Ok(page) => page,
            Err(error) => return Ok(read_result::<pb::ListCandidatesResult>(error, 0)),
        };
        Ok(list_candidates_result(page))
    }
    fn get_extraction_run(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_read!(
            self,
            pb::GetExtractionRunArgs,
            payload,
            get_extraction_run,
            pb::GetExtractionRunResult,
            get_extraction_run_result,
            ExtractionRun
        );
    }
    fn list_extraction_runs(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        let args = match pb::ListExtractionRunsArgs::decode(payload) {
            Ok(args) => args,
            Err(error) => {
                return Ok(read_result::<pb::ListExtractionRunsResult>(
                    invalid_argument(error.to_string()),
                    0,
                ));
            }
        };
        if let Err(error) = self.admit_read(args.context.as_ref()) {
            return Ok(read_result::<pb::ListExtractionRunsResult>(error, 0));
        }
        let page = match self
            .repository
            .list_extraction_runs(&args, self.config.page_size_limits)
        {
            Ok(page) => page,
            Err(error) => return Ok(read_result::<pb::ListExtractionRunsResult>(error, 0)),
        };
        Ok(list_extraction_runs_result(page))
    }
    fn get_operation(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_read!(
            self,
            pb::GetOperationArgs,
            payload,
            get_operation,
            pb::GetOperationResult,
            get_operation_result,
            DurableOperation
        );
    }

    fn get_contract_info(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_read!(
            self,
            pb::GetContractInfoArgs,
            payload,
            get_contract_info,
            pb::GetContractInfoResult,
            get_contract_info_result,
            ContractInfo
        );
    }
    fn get_issue_with_sync(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_read!(
            self,
            pb::GetIssueWithSyncArgs,
            payload,
            get_issue_with_sync,
            pb::GetIssueWithSyncResult,
            get_issue_with_sync_result,
            IssueWithSync
        );
    }
    fn list_issues_with_sync(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        let args = match pb::ListIssuesWithSyncArgs::decode(payload) {
            Ok(args) => args,
            Err(error) => {
                return Ok(read_result::<pb::ListIssuesWithSyncResult>(
                    invalid_argument(error.to_string()),
                    0,
                ));
            }
        };
        if let Err(error) = self.admit_read(args.context.as_ref()) {
            return Ok(read_result::<pb::ListIssuesWithSyncResult>(error, 0));
        }
        match self.repository.list_issues_with_sync(&args) {
            Ok(issues) => Ok(pb::ListIssuesWithSyncResult {
                outcome: Some(pb::list_issues_with_sync_result::Outcome::Success(
                    pb::IssueWithSyncPage {
                        issues,
                        page: None,
                        composite_snapshot: None,
                    },
                )),
            }
            .encode_to_vec()),
            Err(error) => Ok(read_result::<pb::ListIssuesWithSyncResult>(error, 0)),
        }
    }
    fn list_operation_history(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        let args = match pb::ListOperationHistoryArgs::decode(payload) {
            Ok(args) => args,
            Err(error) => {
                return Ok(read_result::<pb::ListOperationHistoryResult>(
                    invalid_argument(error.to_string()),
                    0,
                ));
            }
        };
        if let Err(error) = self.admit_read(args.context.as_ref()) {
            return Ok(read_result::<pb::ListOperationHistoryResult>(error, 0));
        }
        match self.repository.list_operation_history_with_compat(
            &args,
            self.config.root_operation_history_max_window_millis,
            self.config.page_size_limits,
            self.config.operation_history_v1_compat_enabled,
        ) {
            Ok(page) => Ok(pb::ListOperationHistoryResult {
                outcome: Some(pb::list_operation_history_result::Outcome::Success(page)),
            }
            .encode_to_vec()),
            Err(error) => Ok(read_result::<pb::ListOperationHistoryResult>(error, 0)),
        }
    }
    fn retry_operation(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_mutation!(
            self,
            pb::RetryOperationArgs,
            payload,
            "manager_retry_operation",
            retry_operation,
            retry_operation_result
        );
    }
    fn reconcile_operation(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        decode_mutation!(
            self,
            pb::ReconcileOperationArgs,
            payload,
            "manager_reconcile_operation",
            reconcile_operation,
            reconcile_operation_result
        );
    }
}

#[derive(Clone)]
pub(crate) struct Admission {
    pub(crate) scope: String,
    pub(crate) request_digest: String,
    pub(crate) operation_id: String,
    pub(crate) decision: IdempotencyDecision,
}

pub(crate) fn validate_write_context(
    config: &ManagerGatewayConfig,
    method: &str,
    context: &pb::ManagerWriteContext,
) -> Result<(), pb::ManagerError> {
    if context.method != method
        || context.schema_fingerprint != config.schema_fingerprint
        || context.contract_fingerprint != config.contract_fingerprint
        || context.data_root_id != config.data_root_id
        || context.target.trim().is_empty()
        || context.idempotency_key.trim().is_empty()
        || context.actor_audit.as_ref().is_none_or(|audit| {
            audit.actor_id.trim().is_empty()
                || matches!(
                    v1::ActorKind::try_from(audit.actor_kind),
                    Ok(v1::ActorKind::Unspecified) | Err(_)
                )
        })
    {
        return Err(manager_error(
            pb::ManagerErrorCode::MethodTargetMismatch,
            "manager write context does not match the invoked method or store",
            false,
        ));
    }
    Ok(())
}

pub(crate) fn validate_read_context(
    config: &ManagerGatewayConfig,
    context: Option<&pb::ManagerReadContext>,
) -> Result<(), pb::ManagerError> {
    let context = context.ok_or_else(|| invalid_argument("read context is required"))?;
    if context.schema_fingerprint != config.schema_fingerprint
        || context.contract_fingerprint != config.contract_fingerprint
        || context.data_root_id != config.data_root_id
    {
        return Err(manager_error(
            pb::ManagerErrorCode::MethodTargetMismatch,
            "manager read context does not match the active store",
            false,
        ));
    }
    Ok(())
}

fn decode_write_context(
    method: &str,
    payload: &[u8],
) -> Result<pb::ManagerWriteContext, pb::ManagerError> {
    let context = match method {
        "manager_create_issue" => pb::CreateIssueArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_update_issue" => pb::UpdateIssueArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_transition_issue" => pb::TransitionIssueArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_claim_issue" => pb::ClaimIssueArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_heartbeat_issue_claim" => pb::HeartbeatIssueClaimArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_release_issue_claim" => pb::ReleaseIssueClaimArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_reconcile_expired_claim" => pb::ReconcileExpiredClaimArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_add_issue_relation" => pb::AddIssueRelationArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_remove_issue_relation" => pb::RemoveIssueRelationArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        "manager_add_issue_comment" => pb::AddIssueCommentArgs::decode(payload)
            .ok()
            .and_then(|args| args.context),
        _ => None,
    };
    context.ok_or_else(|| {
        invalid_argument("write context is required or request could not be decoded")
    })
}

pub(crate) fn contains_secret(payload: &[u8]) -> bool {
    let lower = String::from_utf8_lossy(payload).to_ascii_lowercase();
    [
        "sk-",
        "bearer ",
        "api_key",
        "secret=",
        "token=",
        "password=",
        "ghp_",
        "github_pat_",
        "xoxb-",
        "-----begin ",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn redact_canonical_issue(mutation: &pb::ManagerMutation) -> pb::ManagerMutation {
    let mut redacted = mutation.clone();
    redacted.operation = None;
    match redacted.result.as_mut() {
        Some(pb::manager_mutation::Result::ClaimIssue(claim)) => {
            claim.current = None;
            claim.lease_handle.clear();
        }
        Some(pb::manager_mutation::Result::IssueCreation(result)) => {
            result.issue = None;
        }
        _ => {}
    }
    redacted
}

pub(crate) fn manager_error(
    code: pb::ManagerErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> pb::ManagerError {
    pb::ManagerError {
        code: code as i32,
        message: message.into(),
        retryable,
        detail: None,
    }
}
fn invalid_argument(message: impl Into<String>) -> pb::ManagerError {
    manager_error(pb::ManagerErrorCode::ManagerInvalidArgument, message, false)
}
fn backend_unavailable(method: &str) -> pb::ManagerError {
    manager_error(
        pb::ManagerErrorCode::BackendUnavailable,
        format!("backend dispatch for {method} is unavailable"),
        true,
    )
}

fn backend_capability_unavailable_for(method: &str, detail: String) -> pb::ManagerError {
    manager_error(
        pb::ManagerErrorCode::BackendUnavailable,
        format!("{method} is unavailable: {detail}"),
        false,
    )
}

fn mutation_result(
    _method: &str,
    outcome: Result<pb::ManagerMutation, pb::ManagerError>,
) -> Vec<u8> {
    mutation_result_with::<pb::CreateIssueResult>(
        outcome
            .err()
            .unwrap_or_else(|| invalid_argument("backend result unavailable")),
        true,
    )
}

pub(crate) trait TypedErrorResult: Message + Default {
    fn error(error: pb::ManagerError) -> Vec<u8>;
}

macro_rules! typed_error_result {
    ($result:path, $module:ident) => {
        impl TypedErrorResult for $result {
            fn error(error: pb::ManagerError) -> Vec<u8> {
                $result {
                    outcome: Some(pb::$module::Outcome::Error(error)),
                }
                .encode_to_vec()
            }
        }
    };
}

typed_error_result!(pb::CreateCandidateResult, create_candidate_result);
typed_error_result!(pb::UpdateCandidateResult, update_candidate_result);
typed_error_result!(pb::ResolveCandidateResult, resolve_candidate_result);
typed_error_result!(pb::CreateExtractionRunResult, create_extraction_run_result);
typed_error_result!(
    pb::AdvanceExtractionRunResult,
    advance_extraction_run_result
);
typed_error_result!(pb::ResumeExtractionRunResult, resume_extraction_run_result);
typed_error_result!(pb::CreateIssueResult, create_issue_result);
typed_error_result!(pb::UpdateIssueResult, update_issue_result);
typed_error_result!(pb::TransitionIssueResult, transition_issue_result);
typed_error_result!(pb::ClaimIssueResult, claim_issue_result);
typed_error_result!(pb::HeartbeatIssueClaimResult, heartbeat_issue_claim_result);
typed_error_result!(pb::ReleaseIssueClaimResult, release_issue_claim_result);
typed_error_result!(
    pb::ReconcileExpiredClaimResult,
    reconcile_expired_claim_result
);
typed_error_result!(pb::AddIssueRelationResult, add_issue_relation_result);
typed_error_result!(pb::RemoveIssueRelationResult, remove_issue_relation_result);
typed_error_result!(pb::AddIssueCommentResult, add_issue_comment_result);
typed_error_result!(pb::RetryOperationResult, retry_operation_result);
typed_error_result!(pb::ReconcileOperationResult, reconcile_operation_result);

typed_error_result!(pb::GetCandidateResult, get_candidate_result);
typed_error_result!(pb::ListCandidatesResult, list_candidates_result);
typed_error_result!(
    pb::ListReviewCandidatesResult,
    list_review_candidates_result
);
typed_error_result!(pb::GetExtractionRunResult, get_extraction_run_result);
typed_error_result!(pb::ListExtractionRunsResult, list_extraction_runs_result);
typed_error_result!(pb::GetOperationResult, get_operation_result);
typed_error_result!(pb::GetContractInfoResult, get_contract_info_result);
typed_error_result!(pb::GetIssueWithSyncResult, get_issue_with_sync_result);
typed_error_result!(pb::GetIssueRelationsResult, get_issue_relations_result);
typed_error_result!(pb::ListIssuesWithSyncResult, list_issues_with_sync_result);
typed_error_result!(
    pb::ListOperationHistoryResult,
    list_operation_history_result
);

fn mutation_result_with<T: TypedErrorResult>(error: pb::ManagerError, _mutation: bool) -> Vec<u8> {
    T::error(error)
}
pub(crate) fn read_result<T: TypedErrorResult>(error: pb::ManagerError, _kind: i32) -> Vec<u8> {
    T::error(error)
}

macro_rules! mutation_wrapper {
    ($name:ident, $result:path, $module:ident) => {
        fn $name(outcome: Result<pb::ManagerMutation, pb::ManagerError>) -> Vec<u8> {
            let outcome = Some(match outcome {
                Ok(value) => pb::$module::Outcome::Success(value),
                Err(error) => pb::$module::Outcome::Error(error),
            });
            $result { outcome }.encode_to_vec()
        }
    };
}
macro_rules! read_wrapper {
    ($name:ident, $result:path, $module:ident, $value:ident) => {
        fn $name(outcome: Result<pb::$value, pb::ManagerError>) -> Vec<u8> {
            let outcome = Some(match outcome {
                Ok(value) => pb::$module::Outcome::Success(value),
                Err(error) => pb::$module::Outcome::Error(error),
            });
            $result { outcome }.encode_to_vec()
        }
    };
}

mutation_wrapper!(
    create_candidate_result,
    pb::CreateCandidateResult,
    create_candidate_result
);
mutation_wrapper!(
    update_candidate_result,
    pb::UpdateCandidateResult,
    update_candidate_result
);
#[allow(dead_code)]
fn resolve_candidate_result(outcome: Result<pb::ResolutionReceipt, pb::ManagerError>) -> Vec<u8> {
    let outcome = Some(match outcome {
        Ok(value) => pb::resolve_candidate_result::Outcome::Success(value),
        Err(error) => pb::resolve_candidate_result::Outcome::Error(error),
    });
    pb::ResolveCandidateResult { outcome }.encode_to_vec()
}
mutation_wrapper!(
    create_extraction_run_result,
    pb::CreateExtractionRunResult,
    create_extraction_run_result
);
mutation_wrapper!(
    advance_extraction_run_result,
    pb::AdvanceExtractionRunResult,
    advance_extraction_run_result
);
mutation_wrapper!(
    resume_extraction_run_result,
    pb::ResumeExtractionRunResult,
    resume_extraction_run_result
);
mutation_wrapper!(
    create_issue_result,
    pb::CreateIssueResult,
    create_issue_result
);
mutation_wrapper!(
    update_issue_result,
    pb::UpdateIssueResult,
    update_issue_result
);
mutation_wrapper!(
    transition_issue_result,
    pb::TransitionIssueResult,
    transition_issue_result
);
mutation_wrapper!(claim_issue_result, pb::ClaimIssueResult, claim_issue_result);
mutation_wrapper!(
    heartbeat_issue_claim_result,
    pb::HeartbeatIssueClaimResult,
    heartbeat_issue_claim_result
);
mutation_wrapper!(
    release_issue_claim_result,
    pb::ReleaseIssueClaimResult,
    release_issue_claim_result
);
mutation_wrapper!(
    reconcile_expired_claim_result,
    pb::ReconcileExpiredClaimResult,
    reconcile_expired_claim_result
);
mutation_wrapper!(
    add_issue_relation_result,
    pb::AddIssueRelationResult,
    add_issue_relation_result
);
mutation_wrapper!(
    remove_issue_relation_result,
    pb::RemoveIssueRelationResult,
    remove_issue_relation_result
);
mutation_wrapper!(
    add_issue_comment_result,
    pb::AddIssueCommentResult,
    add_issue_comment_result
);
mutation_wrapper!(
    retry_operation_result,
    pb::RetryOperationResult,
    retry_operation_result
);
mutation_wrapper!(
    reconcile_operation_result,
    pb::ReconcileOperationResult,
    reconcile_operation_result
);
mutation_wrapper!(
    apply_issue_admin_action_result,
    pb::ApplyIssueAdminActionResult,
    apply_issue_admin_action_result
);
mutation_wrapper!(
    resolve_issue_operation_result,
    pb::ResolveIssueOperationResult,
    resolve_issue_operation_result
);

read_wrapper!(
    get_candidate_result,
    pb::GetCandidateResult,
    get_candidate_result,
    Candidate
);
read_wrapper!(
    get_extraction_run_result,
    pb::GetExtractionRunResult,
    get_extraction_run_result,
    ExtractionRun
);
read_wrapper!(
    get_operation_result,
    pb::GetOperationResult,
    get_operation_result,
    DurableOperation
);
read_wrapper!(
    get_contract_info_result,
    pb::GetContractInfoResult,
    get_contract_info_result,
    ContractInfo
);
read_wrapper!(
    get_issue_with_sync_result,
    pb::GetIssueWithSyncResult,
    get_issue_with_sync_result,
    IssueWithSync
);

fn list_candidates_result(value: pb::CandidatePage) -> Vec<u8> {
    pb::ListCandidatesResult {
        outcome: Some(pb::list_candidates_result::Outcome::Success(value)),
    }
    .encode_to_vec()
}
fn list_extraction_runs_result(value: pb::ExtractionRunPage) -> Vec<u8> {
    pb::ListExtractionRunsResult {
        outcome: Some(pb::list_extraction_runs_result::Outcome::Success(value)),
    }
    .encode_to_vec()
}

pub(crate) type MutationWrapper = fn(Result<pb::ManagerMutation, pb::ManagerError>) -> Vec<u8>;

pub(crate) fn mutation_wrapper_for_method(method: &str) -> Option<MutationWrapper> {
    match method {
        "manager_create_issue" => Some(create_issue_result),
        "manager_update_issue" => Some(update_issue_result),
        "manager_transition_issue" => Some(transition_issue_result),
        "manager_claim_issue" => Some(claim_issue_result),
        "manager_heartbeat_issue_claim" => Some(heartbeat_issue_claim_result),
        "manager_release_issue_claim" => Some(release_issue_claim_result),
        "manager_reconcile_expired_claim" => Some(reconcile_expired_claim_result),
        "manager_add_issue_relation" => Some(add_issue_relation_result),
        "manager_remove_issue_relation" => Some(remove_issue_relation_result),
        "manager_add_issue_comment" => Some(add_issue_comment_result),
        "manager_apply_issue_admin_action" => Some(apply_issue_admin_action_result),
        "manager_resolve_issue_operation" => Some(resolve_issue_operation_result),
        _ => None,
    }
}

/// Return the concrete protobuf wrapper used for a mutation method's replay
/// payload.  Replay snapshots are method-specific; recording a generic
/// placeholder schema would make the payload impossible to decode from an
/// audit receipt.
pub(crate) fn mutation_result_schema_for_method(method: &str) -> Option<&'static str> {
    super::methods::spec(method).map(|value| value.result_schema)
}
