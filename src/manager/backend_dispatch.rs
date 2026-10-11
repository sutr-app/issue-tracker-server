//! Southbound TaskChampion v1 dispatch coordination.
//!
//! The coordinator receives a leased outbox entry, sends the already-built
//! v1 request to a southbound adapter, and delegates commit-marker semantics
//! to [`super::reconcile`].  It carries no canonical Issue snapshot and never
//! persists an ephemeral claim lease handle.

use super::{
    backend_adapter::MarkerExpectation,
    operations::{digest_bytes, transition_operation},
    outbox::{self, OutboxError},
    reconcile::{self, ReconcileError, ReconcileReason, ReconcileResult},
};
use crate::{
    error::TrackerError,
    proto_types::manager_v1 as pb,
    tracker::{
        Tracker,
        actor::ActorIdentity,
        config::RunnerConfig,
        dispatch::dispatch_v1_method,
        terminal_report::{self, TerminalReportKind},
    },
    v1,
};
use async_trait::async_trait;
use prost::Message;
use thiserror::Error;

/// The minimal request passed from the manager gateway to the southbound v1
/// adapter. Payload bytes are an in-memory handoff only; the outbox stores a
/// snapshot reference and digest rather than these bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BackendDispatchRequest {
    pub method: String,
    pub payload: Vec<u8>,
    /// The manager-generated downstream ID is carried out of the durable
    /// operation row and is used only to build the canonical WriteContext.
    pub downstream_operation_id: Option<String>,
}

/// The only backend request shape permitted in durable manager storage.
///
/// It deliberately contains intent-level Issue write fields rather than a
/// canonical `v1::Issue`, and omits all lease capabilities.  The worker
/// reconstructs the southbound v1 request from this envelope after restart.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableBackendRequest {
    #[prost(string, tag = "1")]
    pub method: String,
    #[prost(message, optional, tag = "2")]
    pub context: Option<DurableWriteContext>,
    #[prost(message, optional, tag = "3")]
    pub create_issue: Option<DurableCreateIssueRequest>,
    #[prost(message, optional, tag = "4")]
    pub update_issue: Option<DurableUpdateIssueRequest>,
    #[prost(message, optional, tag = "5")]
    pub claim_issue: Option<DurableClaimIssueRequest>,
    #[prost(message, optional, tag = "6")]
    pub transition_issue: Option<DurableTransitionIssueRequest>,
    #[prost(message, optional, tag = "7")]
    pub reconcile_expired_claim: Option<DurableReconcileExpiredClaimRequest>,
    #[prost(message, optional, tag = "8")]
    pub add_issue_relation: Option<DurableAddIssueRelationRequest>,
    #[prost(message, optional, tag = "9")]
    pub remove_issue_relation: Option<DurableRemoveIssueRelationRequest>,
    #[prost(message, optional, tag = "10")]
    pub add_issue_comment: Option<DurableAddIssueCommentRequest>,
    /// Administrative actions use the same durable request boundary as
    /// ordinary backend writes.  Keeping the public manager arguments inside
    /// this private envelope preserves their exact idempotent intent without
    /// exposing an administrative capability to the v1 tracker API.
    #[prost(message, optional, tag = "11")]
    pub admin_action: Option<DurableAdminActionRequest>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableWriteContext {
    #[prost(string, tag = "1")]
    pub target: String,
    #[prost(string, tag = "2")]
    pub schema_fingerprint: String,
    #[prost(message, optional, tag = "3")]
    pub actor_audit: Option<pb::ActorAudit>,
    #[prost(string, tag = "4")]
    pub correlation_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableCreateIssueRequest {
    #[prost(string, tag = "1")]
    pub issue_id: String,
    #[prost(message, optional, tag = "2")]
    pub issue: Option<v1::IssueDraft>,
    #[prost(message, optional, tag = "3")]
    pub strong_detection_proof: Option<pb::StrongDetectionProof>,
    /// The admitted manager payload is retained only to replay server-owned
    /// provenance materialization; it is never sent southbound.
    #[prost(bytes = "vec", tag = "4")]
    pub admitted_manager_args: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableUpdateIssueRequest {
    #[prost(string, tag = "1")]
    pub issue_id: String,
    #[prost(uint64, optional, tag = "2")]
    pub expected_revision: Option<u64>,
    #[prost(int32, repeated, tag = "3")]
    pub update_mask: Vec<i32>,
    #[prost(message, optional, tag = "4")]
    pub patch: Option<pb::ManagerIssuePatch>,
    #[prost(int32, optional, tag = "5")]
    pub target_state: Option<i32>,
    #[prost(oneof = "durable_update_issue_request::TerminalReport", tags = "6, 7")]
    pub terminal_report: Option<durable_update_issue_request::TerminalReport>,
}

pub(crate) mod durable_update_issue_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TerminalReport {
        #[prost(message, tag = "6")]
        Completion(crate::v1::CompletionReport),
        #[prost(message, tag = "7")]
        Cancellation(crate::v1::CancellationReport),
    }
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableClaimIssueRequest {
    #[prost(string, tag = "1")]
    pub issue_id: String,
    #[prost(uint64, optional, tag = "2")]
    pub expected_revision: Option<u64>,
    #[prost(message, optional, tag = "3")]
    pub requested_lease: Option<pb::TimeSpan>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableTransitionIssueRequest {
    #[prost(string, tag = "1")]
    pub issue_id: String,
    #[prost(uint64, optional, tag = "2")]
    pub expected_revision: Option<u64>,
    #[prost(int32, tag = "3")]
    pub target_state: i32,
    #[prost(message, optional, tag = "4")]
    pub deferred_review_at: Option<pb::UtcInstant>,
    #[prost(uint64, optional, tag = "5")]
    pub claim_revision: Option<u64>,
    #[prost(
        oneof = "durable_transition_issue_request::TerminalReport",
        tags = "6, 7"
    )]
    pub terminal_report: Option<durable_transition_issue_request::TerminalReport>,
}

pub(crate) mod durable_transition_issue_request {
    use super::*;

    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(crate) enum TerminalReport {
        #[prost(message, tag = "6")]
        Completion(v1::CompletionReport),
        #[prost(message, tag = "7")]
        Cancellation(v1::CancellationReport),
    }
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableReconcileExpiredClaimRequest {
    #[prost(string, tag = "1")]
    pub issue_id: String,
    #[prost(uint64, optional, tag = "2")]
    pub expected_revision: Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub claim_revision: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableAddIssueRelationRequest {
    #[prost(string, tag = "1")]
    pub relation_id: String,
    #[prost(int32, tag = "2")]
    pub kind: i32,
    #[prost(string, tag = "3")]
    pub source_issue_id: String,
    #[prost(uint64, optional, tag = "4")]
    pub source_expected_revision: Option<u64>,
    #[prost(string, tag = "5")]
    pub target_issue_id: String,
    #[prost(uint64, optional, tag = "6")]
    pub target_expected_revision: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableRemoveIssueRelationRequest {
    #[prost(string, tag = "1")]
    pub relation_id: String,
    #[prost(string, tag = "2")]
    pub source_issue_id: String,
    #[prost(uint64, optional, tag = "3")]
    pub source_expected_revision: Option<u64>,
    #[prost(string, tag = "4")]
    pub target_issue_id: String,
    #[prost(uint64, optional, tag = "5")]
    pub target_expected_revision: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableAddIssueCommentRequest {
    #[prost(string, tag = "1")]
    pub issue_id: String,
    #[prost(uint64, optional, tag = "2")]
    pub expected_revision: Option<u64>,
    #[prost(string, tag = "3")]
    pub comment_id: String,
    #[prost(string, tag = "4")]
    pub body: String,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct DurableAdminActionRequest {
    #[prost(message, optional, tag = "1")]
    pub args: Option<pb::ApplyIssueAdminActionArgs>,
}

pub(crate) fn durable_request_from_manager(
    method: &str,
    payload: &[u8],
) -> Result<Vec<u8>, SouthboundError> {
    let request = match method {
        "manager_update_issue" => {
            let args = pb::UpdateIssueArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("update_issue decode: {error}"))
            })?;
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context),
                create_issue: None,
                update_issue: Some(DurableUpdateIssueRequest {
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    update_mask: args.update_mask,
                    patch: args.patch,
                    target_state: args.target_state,
                    terminal_report: args.terminal_report.map(|report| match report {
                        pb::update_issue_args::TerminalReport::Completion(value) => {
                            durable_update_issue_request::TerminalReport::Completion(value)
                        }
                        pb::update_issue_args::TerminalReport::Cancellation(value) => {
                            durable_update_issue_request::TerminalReport::Cancellation(value)
                        }
                    }),
                }),
                ..Default::default()
            }
        }
        "manager_claim_issue" => {
            let args = pb::ClaimIssueArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("claim_issue decode: {error}"))
            })?;
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context),
                create_issue: None,
                update_issue: None,
                claim_issue: Some(DurableClaimIssueRequest {
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    requested_lease: args.requested_lease,
                }),
                ..Default::default()
            }
        }
        "manager_transition_issue" => {
            let args = pb::TransitionIssueArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("transition_issue decode: {error}"))
            })?;
            if args
                .lease_handle
                .as_ref()
                .is_some_and(|handle| !handle.is_empty())
            {
                return Err(SouthboundError::not_sent(
                    "transition issue lease handle cannot cross the durable manager boundary",
                ));
            }
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context),
                create_issue: None,
                update_issue: None,
                claim_issue: None,
                transition_issue: Some(DurableTransitionIssueRequest {
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    target_state: args.target_state,
                    deferred_review_at: args.deferred_review_at,
                    claim_revision: args.claim_revision,
                    terminal_report: args.terminal_report.map(|report| match report {
                        pb::transition_issue_args::TerminalReport::Completion(value) => {
                            durable_transition_issue_request::TerminalReport::Completion(value)
                        }
                        pb::transition_issue_args::TerminalReport::Cancellation(value) => {
                            durable_transition_issue_request::TerminalReport::Cancellation(value)
                        }
                    }),
                }),
                ..Default::default()
            }
        }
        "manager_reconcile_expired_claim" => {
            let args = pb::ReconcileExpiredClaimArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("reconcile_expired_claim decode: {error}"))
            })?;
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context),
                create_issue: None,
                update_issue: None,
                claim_issue: None,
                transition_issue: None,
                reconcile_expired_claim: Some(DurableReconcileExpiredClaimRequest {
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    claim_revision: args.claim_revision,
                }),
                ..Default::default()
            }
        }
        "manager_add_issue_relation" => {
            let args = pb::AddIssueRelationArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("add_issue_relation decode: {error}"))
            })?;
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context),
                create_issue: None,
                update_issue: None,
                claim_issue: None,
                transition_issue: None,
                reconcile_expired_claim: None,
                add_issue_relation: Some(DurableAddIssueRelationRequest {
                    relation_id: args.relation_id,
                    kind: args.kind,
                    source_issue_id: args.source_issue_id,
                    source_expected_revision: args.source_expected_revision,
                    target_issue_id: args.target_issue_id,
                    target_expected_revision: args.target_expected_revision,
                }),
                ..Default::default()
            }
        }
        "manager_remove_issue_relation" => {
            let args = pb::RemoveIssueRelationArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("remove_issue_relation decode: {error}"))
            })?;
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context),
                create_issue: None,
                update_issue: None,
                claim_issue: None,
                transition_issue: None,
                reconcile_expired_claim: None,
                add_issue_relation: None,
                remove_issue_relation: Some(DurableRemoveIssueRelationRequest {
                    relation_id: args.relation_id,
                    source_issue_id: args.source_issue_id,
                    source_expected_revision: args.source_expected_revision,
                    target_issue_id: args.target_issue_id,
                    target_expected_revision: args.target_expected_revision,
                }),
                ..Default::default()
            }
        }
        "manager_add_issue_comment" => {
            let args = pb::AddIssueCommentArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("add_issue_comment decode: {error}"))
            })?;
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context),
                create_issue: None,
                update_issue: None,
                claim_issue: None,
                transition_issue: None,
                reconcile_expired_claim: None,
                add_issue_relation: None,
                remove_issue_relation: None,
                add_issue_comment: Some(DurableAddIssueCommentRequest {
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    comment_id: args.comment_id,
                    body: args.body,
                }),
                admin_action: None,
            }
        }
        "manager_create_issue" => {
            let args = pb::CreateIssueArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("create_issue decode: {error}"))
            })?;
            let admitted_manager_args = args.encode_to_vec();
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context),
                create_issue: Some(DurableCreateIssueRequest {
                    issue_id: args.issue_id,
                    issue: args.issue,
                    strong_detection_proof: args.strong_detection_proof,
                    admitted_manager_args,
                }),
                ..Default::default()
            }
        }
        "manager_apply_issue_admin_action" => {
            let args = pb::ApplyIssueAdminActionArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("apply_issue_admin_action decode: {error}"))
            })?;
            DurableBackendRequest {
                method: method.into(),
                context: durable_context(args.context.clone()),
                admin_action: Some(DurableAdminActionRequest { args: Some(args) }),
                ..Default::default()
            }
        }
        _ => {
            return Err(SouthboundError::not_sent(
                "backend operation requires unavailable secure capability handling",
            ));
        }
    };
    let encoded = request.encode_to_vec();
    if method == "manager_create_issue" {
        validate_durable_request_snapshot(method, &encoded)?;
    }
    Ok(encoded)
}

/// Returns a digest input with an ephemeral lease capability removed.
///
/// Capability requests are dispatched only by the in-process synchronous
/// path.  They may still receive an operation receipt, but neither the raw
/// handle nor a digest derived from it may cross that boundary into storage.
pub(crate) fn redacted_capability_request(
    method: &str,
    payload: &[u8],
) -> Result<Vec<u8>, SouthboundError> {
    match method {
        "manager_claim_issue" => {
            let args = pb::ClaimIssueArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("claim_issue decode: {error}"))
            })?;
            if args.issue_id.trim().is_empty() {
                return Err(SouthboundError::not_sent("issue ID is required"));
            }
            Ok(args.encode_to_vec())
        }
        "manager_heartbeat_issue_claim" => {
            let mut args = pb::HeartbeatIssueClaimArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("heartbeat_issue_claim decode: {error}"))
            })?;
            if args.lease_handle.is_empty() {
                return Err(SouthboundError::not_sent("lease handle is required"));
            }
            args.lease_handle.clear();
            Ok(args.encode_to_vec())
        }
        "manager_release_issue_claim" => {
            let mut args = pb::ReleaseIssueClaimArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("release_issue_claim decode: {error}"))
            })?;
            if args.lease_handle.is_empty() {
                return Err(SouthboundError::not_sent("lease handle is required"));
            }
            args.lease_handle.clear();
            Ok(args.encode_to_vec())
        }
        "manager_transition_issue" => {
            let mut args = pb::TransitionIssueArgs::decode(payload).map_err(|error| {
                SouthboundError::not_sent(format!("transition_issue decode: {error}"))
            })?;
            if args.awaiting_review.is_some() {
                if args.target_state != v1::IssueState::AwaitingReview as i32 {
                    return Err(SouthboundError::not_sent(
                        "awaiting-review evidence requires the dedicated target state",
                    ));
                }
                if args.lease_handle.as_ref().is_some_and(Vec::is_empty) {
                    return Err(SouthboundError::not_sent("lease handle is empty"));
                }
            } else {
                let handle = args.lease_handle.as_ref().ok_or_else(|| {
                    SouthboundError::not_sent("transition issue has no lease capability")
                })?;
                if handle.is_empty() {
                    return Err(SouthboundError::not_sent("lease handle is required"));
                }
            }
            // Absence and a redacted handle have one canonical representation
            // so replay digests cannot depend on a one-time capability.
            args.lease_handle = None;
            Ok(args.encode_to_vec())
        }
        _ => Err(SouthboundError::not_sent(
            "method is not an ephemeral lease capability",
        )),
    }
}

/// Reject requests that cannot be safely admitted to the durable outbox.
pub(crate) fn validate_backend_admission(method: &str, payload: &[u8]) -> Result<(), String> {
    let durable =
        durable_request_from_manager(method, payload).map_err(|error| error.to_string())?;
    validate_durable_request_snapshot(method, &durable).map_err(|error| error.to_string())
}

/// Validates a persisted request before it reaches the southbound dispatcher.
/// The manager database is not an input trust boundary: malformed envelopes
/// must be quarantined rather than consumed through the retry path.
pub(crate) fn validate_durable_request_snapshot(
    method: &str,
    payload: &[u8],
) -> Result<(), SouthboundError> {
    let envelope = DurableBackendRequest::decode(payload)
        .map_err(|error| SouthboundError::not_sent(format!("durable request decode: {error}")))?;
    if envelope.method != method {
        return Err(SouthboundError::not_sent("durable request method mismatch"));
    }
    if durable_operation_count(&envelope) != 1 {
        return Err(SouthboundError::not_sent(
            "durable request must contain exactly one operation",
        ));
    }
    let context = envelope
        .context
        .as_ref()
        .ok_or_else(|| SouthboundError::not_sent("durable write context is missing"))?;
    if context.target.trim().is_empty()
        || context.schema_fingerprint.trim().is_empty()
        || context.correlation_id.trim().is_empty()
        || context.actor_audit.is_none()
    {
        return Err(SouthboundError::not_sent(
            "durable write context is structurally invalid",
        ));
    }
    match method {
        "manager_create_issue" => {
            let create = envelope
                .create_issue
                .ok_or_else(|| SouthboundError::not_sent("durable create request is missing"))?;
            let issue = create
                .issue
                .as_ref()
                .ok_or_else(|| SouthboundError::not_sent("durable create draft is missing"))?;
            let source_evidence_present = !issue.source_evidence.is_empty();
            let human_explicit_without_evidence = issue.creation_basis
                == v1::CreationBasis::Explicit as i32
                && context
                    .actor_audit
                    .as_ref()
                    .is_some_and(|audit| audit.actor_kind == v1::ActorKind::Human as i32);
            if create.issue_id.trim().is_empty()
                || issue.title.trim().is_empty()
                || (!human_explicit_without_evidence && !source_evidence_present)
            {
                return Err(SouthboundError::not_sent(
                    "durable create request is structurally invalid",
                ));
            }
            Ok(())
        }
        "manager_update_issue" => {
            let update = envelope
                .update_issue
                .ok_or_else(|| SouthboundError::not_sent("durable update request is missing"))?;
            if update.issue_id.trim().is_empty()
                || update.expected_revision.is_none()
                || update.update_mask.is_empty()
                || update.patch.is_none()
            {
                return Err(SouthboundError::not_sent(
                    "durable update request is structurally invalid",
                ));
            }
            validate_durable_update_terminal_report(&update)?;
            Ok(())
        }
        "manager_claim_issue" => {
            let claim = envelope
                .claim_issue
                .ok_or_else(|| SouthboundError::not_sent("durable claim request is missing"))?;
            if claim.issue_id.trim().is_empty() || claim.expected_revision.is_none() {
                return Err(SouthboundError::not_sent(
                    "durable claim request is structurally invalid",
                ));
            }
            Ok(())
        }
        "manager_transition_issue" => {
            let transition = envelope.transition_issue.ok_or_else(|| {
                SouthboundError::not_sent("durable transition request is missing")
            })?;
            if transition.issue_id.trim().is_empty()
                || transition.expected_revision.is_none()
                || transition.target_state == v1::IssueState::Unspecified as i32
            {
                return Err(SouthboundError::not_sent(
                    "durable transition request is structurally invalid",
                ));
            }
            Ok(())
        }
        "manager_reconcile_expired_claim" => {
            let reconcile = envelope
                .reconcile_expired_claim
                .ok_or_else(|| SouthboundError::not_sent("durable reconcile request is missing"))?;
            if reconcile.issue_id.trim().is_empty()
                || reconcile.expected_revision.is_none()
                || reconcile.claim_revision.is_some_and(|claim_revision| {
                    Some(claim_revision) != reconcile.expected_revision
                })
            {
                return Err(SouthboundError::not_sent(
                    "durable reconcile request is structurally invalid",
                ));
            }
            Ok(())
        }
        "manager_add_issue_relation" => {
            let relation = envelope.add_issue_relation.ok_or_else(|| {
                SouthboundError::not_sent("durable add relation request is missing")
            })?;
            if relation.relation_id.trim().is_empty()
                || relation.source_issue_id.trim().is_empty()
                || relation.target_issue_id.trim().is_empty()
                || relation.source_expected_revision.is_none()
                || relation.target_expected_revision.is_none()
            {
                return Err(SouthboundError::not_sent(
                    "durable add relation request is structurally invalid",
                ));
            }
            Ok(())
        }
        "manager_remove_issue_relation" => {
            let relation = envelope.remove_issue_relation.ok_or_else(|| {
                SouthboundError::not_sent("durable remove relation request is missing")
            })?;
            if relation.relation_id.trim().is_empty()
                || relation.source_issue_id.trim().is_empty()
                || relation.target_issue_id.trim().is_empty()
                || relation.source_expected_revision.is_none()
                || relation.target_expected_revision.is_none()
            {
                return Err(SouthboundError::not_sent(
                    "durable remove relation request is structurally invalid",
                ));
            }
            Ok(())
        }
        "manager_add_issue_comment" => {
            let comment = envelope.add_issue_comment.ok_or_else(|| {
                SouthboundError::not_sent("durable add comment request is missing")
            })?;
            if comment.issue_id.trim().is_empty()
                || comment.expected_revision.is_none()
                || comment.comment_id.trim().is_empty()
                || comment.body.is_empty()
            {
                return Err(SouthboundError::not_sent(
                    "durable add comment request is structurally invalid",
                ));
            }
            Ok(())
        }
        "manager_apply_issue_admin_action" => {
            let action = envelope.admin_action.ok_or_else(|| {
                SouthboundError::not_sent("durable administrative action is missing")
            })?;
            let args = action.args.ok_or_else(|| {
                SouthboundError::not_sent("durable administrative arguments are missing")
            })?;
            let context = args.context.ok_or_else(|| {
                SouthboundError::not_sent("durable administrative context is missing")
            })?;
            if args.issue_id.trim().is_empty()
                || args.expected_revision.is_none()
                || args.action == pb::IssueAdminAction::Unspecified as i32
                || context.target.trim().is_empty()
                || context.correlation_id.trim().is_empty()
                || context.actor_audit.is_none()
            {
                return Err(SouthboundError::not_sent(
                    "durable administrative request is structurally invalid",
                ));
            }
            Ok(())
        }
        _ => Err(SouthboundError::not_sent(
            "durable request method is unsupported",
        )),
    }
}

fn validate_durable_update_terminal_report(
    update: &DurableUpdateIssueRequest,
) -> Result<(), SouthboundError> {
    let target_state = update
        .target_state
        .map(v1::IssueState::try_from)
        .transpose()
        .map_err(|_| SouthboundError::not_sent("durable update target_state is invalid"))?;
    if target_state == Some(v1::IssueState::Unspecified) {
        return Err(SouthboundError::not_sent(
            "durable update target_state is invalid",
        ));
    }
    let report_kind = match update.terminal_report.as_ref() {
        Some(durable_update_issue_request::TerminalReport::Completion(report)) => {
            TerminalReportKind::Completion {
                has_result: report.result.is_some(),
            }
        }
        Some(durable_update_issue_request::TerminalReport::Cancellation(report)) => {
            TerminalReportKind::Cancellation {
                has_classification: report.classification
                    != v1::CancellationClass::Unspecified as i32,
            }
        }
        None => TerminalReportKind::None,
    };
    terminal_report::validate(target_state.map(Into::into), report_kind)
        .map_err(|error| SouthboundError::not_sent(error.message()))
}

fn durable_operation_count(envelope: &DurableBackendRequest) -> usize {
    [
        envelope.create_issue.is_some(),
        envelope.update_issue.is_some(),
        envelope.claim_issue.is_some(),
        envelope.transition_issue.is_some(),
        envelope.reconcile_expired_claim.is_some(),
        envelope.add_issue_relation.is_some(),
        envelope.remove_issue_relation.is_some(),
        envelope.add_issue_comment.is_some(),
        envelope.admin_action.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count()
}

fn durable_context(context: Option<pb::ManagerWriteContext>) -> Option<DurableWriteContext> {
    context.map(|context| DurableWriteContext {
        target: context.target,
        schema_fingerprint: context.schema_fingerprint,
        actor_audit: context.actor_audit,
        correlation_id: context.correlation_id,
    })
}

/// A response from a TaskChampion v1 southbound adapter.
///
/// `ephemeral_claim_handle` is intentionally separate from the marker and is
/// only returned to the immediate caller. The coordinator never copies it
/// into an operation, outbox entry, or reconciliation record.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SouthboundResponse {
    pub marker: v1::OperationRecord,
    pub response_payload: Vec<u8>,
    pub ephemeral_claim_handle: Option<Vec<u8>>,
}

impl SouthboundResponse {
    pub(crate) fn committed(marker: v1::OperationRecord, response_payload: Vec<u8>) -> Self {
        Self {
            marker,
            response_payload,
            ephemeral_claim_handle: None,
        }
    }

    pub(crate) fn committed_claim(
        marker: v1::OperationRecord,
        response_payload: Vec<u8>,
        lease_handle: Vec<u8>,
    ) -> Self {
        Self {
            marker,
            response_payload,
            ephemeral_claim_handle: Some(lease_handle),
        }
    }
}

/// The southbound boundary is independent from `Tracker`; the manager gateway
/// can provide one adapter without coupling this state machine to
/// TaskChampion handles or runner lifecycle types.
#[async_trait]
pub(crate) trait TaskChampionV1Dispatcher: Send + Sync {
    async fn dispatch(
        &self,
        request: &BackendDispatchRequest,
    ) -> Result<SouthboundResponse, SouthboundError>;

    /// Dispatch a capability-bearing request without crossing the durable
    /// request-envelope boundary. Implementations that do not need a special
    /// translation can use the ordinary dispatcher as the in-memory adapter.
    async fn dispatch_capability(
        &self,
        request: &BackendDispatchRequest,
    ) -> Result<SouthboundResponse, SouthboundError> {
        self.dispatch(request).await
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum SouthboundError {
    #[error("tracker rejected the request")]
    TypedBusiness(Box<v1::IssueTrackerError>),
    #[error("backend request was not sent: {0}")]
    NotSent(String),
    #[error("backend delivery result is unknown: {0}")]
    DeliveryUnknown(String),
}

impl SouthboundError {
    pub(crate) fn not_sent(reason: impl Into<String>) -> Self {
        Self::NotSent(reason.into())
    }

    pub(crate) fn typed_business(error: v1::IssueTrackerError) -> Self {
        Self::TypedBusiness(Box::new(error))
    }

    pub(crate) fn delivery_unknown(reason: impl Into<String>) -> Self {
        Self::DeliveryUnknown(reason.into())
    }
}

/// A concrete southbound adapter backed by the canonical TaskChampion runner.
///
/// Manager protobufs are translated into the already-tested v1 wire types at
/// this boundary. The manager payload and claim handle remain in memory only;
/// TaskChampion owns the canonical Issue and its durable operation marker.
pub(crate) struct TrackerV1Dispatcher {
    tracker: Tracker,
    config: RunnerConfig,
}

impl TrackerV1Dispatcher {
    pub(crate) fn new(tracker: Tracker, config: RunnerConfig) -> Self {
        Self { tracker, config }
    }
}

/// Classify an administrative Tracker failure according to whether the
/// mutation is known not to have committed.  Only deterministic validation
/// failures may become a terminal manager failure.  Storage/transport and
/// post-commit failures remain delivery-unknown so an operator can reconcile
/// the Tracker marker after a crash instead of being told that the Issue is
/// safely unchanged.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AdminActorError {
    Missing,
    InvalidKind,
}

/// Builds the audit identity of an administrative action. Live dispatch and
/// restart recovery share it so both persist the same actor kind, agent
/// session, and correlation ID in canonical history.
pub(crate) fn admin_actor_identity(
    context: &pb::ManagerWriteContext,
    fallback_correlation_id: &str,
) -> Result<ActorIdentity, AdminActorError> {
    let actor = context
        .actor_audit
        .as_ref()
        .filter(|actor| !actor.actor_id.trim().is_empty())
        .ok_or(AdminActorError::Missing)?;
    // Administrative history must name an accountable actor kind.
    let kind = v1::ActorKind::try_from(actor.actor_kind)
        .ok()
        .filter(|kind| *kind != v1::ActorKind::Unspecified)
        .ok_or(AdminActorError::InvalidKind)?;
    let correlation_id = if context.correlation_id.trim().is_empty() {
        fallback_correlation_id
    } else {
        &context.correlation_id
    };
    Ok(
        ActorIdentity::new(actor.actor_id.clone(), kind).with_audit_context(
            actor.agent_session_id.clone(),
            Some(correlation_id.to_owned()),
        ),
    )
}

fn admin_tracker_error(error: TrackerError) -> SouthboundError {
    match error {
        deterministic @ (TrackerError::InvalidState(_)
        | TrackerError::InvalidStateTransition(_)
        | TrackerError::InvalidRelation(_)
        | TrackerError::NotFound(_)
        | TrackerError::RevisionConflict { .. }
        | TrackerError::InvalidLease { .. }
        | TrackerError::LeaseExpired { .. }
        | TrackerError::LeaseInconsistent { .. }
        | TrackerError::SecretRejected
        | TrackerError::Blocked { .. }
        | TrackerError::IdempotencyConflict
        | TrackerError::InvalidPageToken
        | TrackerError::PageTokenExpired { .. }) => SouthboundError::typed_business(
            crate::tracker::error_response::tracker_error(deterministic),
        ),
        ambiguous => SouthboundError::delivery_unknown(ambiguous.to_string()),
    }
}

#[async_trait]
impl TaskChampionV1Dispatcher for TrackerV1Dispatcher {
    async fn dispatch(
        &self,
        request: &BackendDispatchRequest,
    ) -> Result<SouthboundResponse, SouthboundError> {
        if request.method == "manager_apply_issue_admin_action"
            || request.method == "apply_issue_admin_action"
        {
            return self.dispatch_admin_action(request).await;
        }
        let (method, payload) = translate_manager_request(request)?;
        let response = dispatch_v1_method(&method, &payload, &self.tracker, &self.config)
            .await
            .map_err(SouthboundError::not_sent)?;
        extract_southbound_response(&method, response)
    }

    async fn dispatch_capability(
        &self,
        request: &BackendDispatchRequest,
    ) -> Result<SouthboundResponse, SouthboundError> {
        let (method, payload) = if canonical_backend_method(&request.method) == "transition_issue" {
            let args =
                pb::TransitionIssueArgs::decode(request.payload.as_slice()).map_err(|error| {
                    SouthboundError::not_sent(format!(
                        "transition_issue capability decode: {error}"
                    ))
                })?;
            if args.awaiting_review.is_some() {
                return self.dispatch_awaiting_review(request, args).await;
            }
            translate_capability_transition(request, args)?
        } else {
            translate_capability_request(request)?
        };
        let response = dispatch_v1_method(&method, &payload, &self.tracker, &self.config)
            .await
            .map_err(SouthboundError::not_sent)?;
        extract_southbound_response(&method, response)
    }
}

impl TrackerV1Dispatcher {
    async fn dispatch_awaiting_review(
        &self,
        request: &BackendDispatchRequest,
        args: pb::TransitionIssueArgs,
    ) -> Result<SouthboundResponse, SouthboundError> {
        let business = |error: TrackerError| {
            SouthboundError::typed_business(crate::tracker::error_response::tracker_error(error))
        };
        let downstream_operation_id = required_downstream_operation_id(request)?;
        if args.target_state != v1::IssueState::AwaitingReview as i32 {
            return Err(business(TrackerError::InvalidState(
                "awaiting-review evidence requires the dedicated target state".into(),
            )));
        }
        let completion = args.awaiting_review.ok_or_else(|| {
            SouthboundError::not_sent("awaiting-review completion evidence is missing")
        })?;
        let issue_id = uuid::Uuid::parse_str(&args.issue_id)
            .map_err(|_| business(TrackerError::InvalidState("issue ID is invalid".into())))?;
        let expected_revision = args.expected_revision.ok_or_else(|| {
            business(TrackerError::InvalidState(
                "expected revision is required".into(),
            ))
        })?;
        let write_context = write_context(
            "transition_issue",
            args.context.as_ref(),
            downstream_operation_id,
        )?;
        let call_context =
            crate::tracker::conversion::call_context(Some(&write_context)).map_err(business)?;
        let lease_handle = args
            .lease_handle
            .as_deref()
            .map(std::str::from_utf8)
            .transpose()
            .map_err(|_| {
                business(TrackerError::InvalidState(
                    "lease capability encoding is invalid".into(),
                ))
            })?;
        self.tracker
            .complete_execution_to_awaiting_review(
                issue_id,
                expected_revision,
                downstream_operation_id,
                lease_handle,
                args.claim_revision,
                &call_context.actor,
                call_context.session_id.as_deref(),
                &call_context.correlation_id,
                &completion,
            )
            .await
            .map_err(business)?;
        let marker = self
            .tracker
            .get_operation_record(downstream_operation_id)
            .await
            .map_err(|error| {
                SouthboundError::delivery_unknown(format!(
                    "awaiting-review marker lookup failed: {error}"
                ))
            })?;
        Ok(SouthboundResponse::committed(marker, Vec::new()))
    }

    /// Execute the private administrative request directly against the
    /// canonical Tracker.  It intentionally bypasses the public v1 handler:
    /// the Manager consumer has already authenticated the human/system actor,
    /// while the durable envelope keeps the exact intent available after a
    /// restart.  The Tracker marker is read back only to obtain the immutable
    /// receipt; the mutable Issue is never used to construct the response.
    async fn dispatch_admin_action(
        &self,
        request: &BackendDispatchRequest,
    ) -> Result<SouthboundResponse, SouthboundError> {
        let envelope = durable_envelope(request, "manager_apply_issue_admin_action")?;
        let args = envelope
            .admin_action
            .and_then(|value| value.args)
            .ok_or_else(|| SouthboundError::not_sent("administrative arguments are missing"))?;
        let issue_id = uuid::Uuid::parse_str(&args.issue_id)
            .map_err(|_| SouthboundError::not_sent("administrative issue ID is invalid"))?;
        let downstream_operation_id = required_downstream_operation_id(request)?;
        let context = args
            .context
            .as_ref()
            .ok_or_else(|| SouthboundError::not_sent("administrative actor audit is missing"))?;
        let actor_identity =
            admin_actor_identity(context, downstream_operation_id).map_err(|error| {
                SouthboundError::not_sent(match error {
                    AdminActorError::Missing => "administrative actor audit is missing",
                    AdminActorError::InvalidKind => "administrative actor kind is invalid",
                })
            })?;
        let reason = context
            .actor_audit
            .as_ref()
            .and_then(|actor| actor.reason.as_deref())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| SouthboundError::not_sent("administrative reason is missing"))?;
        let expected_revision = args.expected_revision.ok_or_else(|| {
            SouthboundError::not_sent("administrative expected revision is missing")
        })?;
        let action = pb::IssueAdminAction::try_from(args.action)
            .ok()
            .filter(|value| *value != pb::IssueAdminAction::Unspecified)
            .ok_or_else(|| SouthboundError::not_sent("administrative action is invalid"))?;

        match action {
            pb::IssueAdminAction::Complete | pb::IssueAdminAction::Cancel => {
                let (state, report) =
                    match args.terminal_report.as_ref() {
                        Some(pb::apply_issue_admin_action_args::TerminalReport::Completion(
                            value,
                        )) if action == pb::IssueAdminAction::Complete => (
                            crate::tracker::IssueState::Done,
                            v1::transition_issue_args::TerminalReport::Completion(value.clone()),
                        ),
                        Some(pb::apply_issue_admin_action_args::TerminalReport::Cancellation(
                            value,
                        )) if action == pb::IssueAdminAction::Cancel => (
                            crate::tracker::IssueState::Canceled,
                            v1::transition_issue_args::TerminalReport::Cancellation(value.clone()),
                        ),
                        _ => {
                            return Err(SouthboundError::not_sent(
                                "administrative terminal report does not match action",
                            ));
                        }
                    };
                self.tracker
                    .admin_terminate_with_overrides(
                        issue_id,
                        state,
                        expected_revision,
                        downstream_operation_id,
                        &actor_identity,
                        reason,
                        &report,
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
                    .map_err(admin_tracker_error)?
            }
            pb::IssueAdminAction::Recover | pb::IssueAdminAction::Revoke => self
                .tracker
                .recover_claim(
                    issue_id,
                    expected_revision,
                    action == pb::IssueAdminAction::Revoke,
                    downstream_operation_id,
                    &actor_identity,
                    reason,
                )
                .await
                .map_err(admin_tracker_error)?,
            pb::IssueAdminAction::Unspecified => {
                return Err(SouthboundError::not_sent(
                    "administrative action is invalid",
                ));
            }
        };
        let marker = self
            .tracker
            .get_operation_record(downstream_operation_id)
            .await
            .map_err(|error| {
                SouthboundError::delivery_unknown(format!(
                    "administrative marker lookup failed: {error}"
                ))
            })?;
        let receipt = admin_action_result_from_marker(&marker, &args)
            .map_err(|_| SouthboundError::delivery_unknown("administrative receipt is invalid"))?;
        // The Tracker return value can be a replay of a marker after a later
        // Issue write.  Use the immutable receipt revision for the Manager
        // mutation instead of that mutable current Issue revision.
        let resulting_revision = receipt.resulting_revision;
        let response = pb::ApplyIssueAdminActionResult {
            outcome: Some(pb::apply_issue_admin_action_result::Outcome::Success(
                pb::ManagerMutation {
                    entity_revisions: vec![pb::ManagerEntityRevision {
                        entity_kind: "issue".into(),
                        entity_id: args.issue_id.clone(),
                        revision: resulting_revision,
                    }],
                    result: Some(pb::manager_mutation::Result::IssueAdminAction(receipt)),
                    ..Default::default()
                },
            )),
        };
        Ok(SouthboundResponse::committed(
            marker,
            response.encode_to_vec(),
        ))
    }
}

/// Translate a manager capability call directly to the v1 request. Unlike the
/// durable path this intentionally keeps the lease handle in memory only.
fn translate_capability_request(
    request: &BackendDispatchRequest,
) -> Result<(String, Vec<u8>), SouthboundError> {
    let downstream_operation_id = required_downstream_operation_id(request)?;
    let method = canonical_backend_method(&request.method).to_owned();
    let context = |method: &str, value: Option<&pb::ManagerWriteContext>| {
        write_context(method, value, downstream_operation_id)
    };
    match method.as_str() {
        "claim_issue" => {
            let args = pb::ClaimIssueArgs::decode(request.payload.as_slice()).map_err(|error| {
                SouthboundError::not_sent(format!("claim_issue capability decode: {error}"))
            })?;
            Ok((
                method,
                v1::ClaimIssueArgs {
                    context: Some(context("claim_issue", args.context.as_ref())?),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    requested_lease: args.requested_lease.map(time_span),
                }
                .encode_to_vec(),
            ))
        }
        "heartbeat_issue_claim" => {
            let args = pb::HeartbeatIssueClaimArgs::decode(request.payload.as_slice()).map_err(
                |error| {
                    SouthboundError::not_sent(format!(
                        "heartbeat_issue_claim capability decode: {error}"
                    ))
                },
            )?;
            Ok((
                method,
                v1::HeartbeatIssueClaimArgs {
                    context: Some(context("heartbeat_issue_claim", args.context.as_ref())?),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    claim_revision: args.claim_revision,
                    lease_handle: args.lease_handle,
                    requested_extension: args.requested_extension.map(time_span),
                }
                .encode_to_vec(),
            ))
        }
        "release_issue_claim" => {
            let args =
                pb::ReleaseIssueClaimArgs::decode(request.payload.as_slice()).map_err(|error| {
                    SouthboundError::not_sent(format!(
                        "release_issue_claim capability decode: {error}"
                    ))
                })?;
            Ok((
                method,
                v1::ReleaseIssueClaimArgs {
                    context: Some(context("release_issue_claim", args.context.as_ref())?),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    claim_revision: args.claim_revision,
                    lease_handle: args.lease_handle,
                }
                .encode_to_vec(),
            ))
        }
        _ => Err(SouthboundError::not_sent(
            "method is not an ephemeral lease capability",
        )),
    }
}

/// Translate a non-awaiting-review transition capability. The caller has
/// already decoded the payload to route awaiting-review evidence elsewhere.
fn translate_capability_transition(
    request: &BackendDispatchRequest,
    args: pb::TransitionIssueArgs,
) -> Result<(String, Vec<u8>), SouthboundError> {
    let downstream_operation_id = required_downstream_operation_id(request)?;
    let terminal_report = args.terminal_report.map(|report| match report {
        pb::transition_issue_args::TerminalReport::Completion(value) => {
            v1::transition_issue_args::TerminalReport::Completion(value)
        }
        pb::transition_issue_args::TerminalReport::Cancellation(value) => {
            v1::transition_issue_args::TerminalReport::Cancellation(value)
        }
    });
    Ok((
        "transition_issue".to_owned(),
        v1::TransitionIssueArgs {
            context: Some(write_context(
                "transition_issue",
                args.context.as_ref(),
                downstream_operation_id,
            )?),
            issue_id: args.issue_id,
            expected_revision: args.expected_revision,
            target_state: args.target_state,
            lease_handle: args.lease_handle,
            deferred_review_at: args.deferred_review_at.map(instant),
            claim_revision: args.claim_revision,
            terminal_report,
        }
        .encode_to_vec(),
    ))
}

fn required_downstream_operation_id(
    request: &BackendDispatchRequest,
) -> Result<&str, SouthboundError> {
    request
        .downstream_operation_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SouthboundError::not_sent("downstream operation ID is required"))
}

fn translate_manager_request(
    request: &BackendDispatchRequest,
) -> Result<(String, Vec<u8>), SouthboundError> {
    let downstream_operation_id = required_downstream_operation_id(request)?;
    let method = canonical_backend_method(&request.method).to_owned();

    match method.as_str() {
        "create_issue" => {
            let envelope = durable_envelope(request, "manager_create_issue")?;
            let args = envelope
                .create_issue
                .ok_or_else(|| SouthboundError::not_sent("create issue envelope is missing"))?;
            let context = manager_context(envelope.context.as_ref());
            let context = write_context("create_issue", context.as_ref(), downstream_operation_id)?;
            Ok((
                method,
                v1::CreateIssueArgs {
                    context: Some(context),
                    issue_id: args.issue_id,
                    issue: args.issue,
                }
                .encode_to_vec(),
            ))
        }
        "update_issue" => {
            let envelope = durable_envelope(request, "manager_update_issue")?;
            let args = envelope
                .update_issue
                .ok_or_else(|| SouthboundError::not_sent("update issue envelope is missing"))?;
            let context = manager_context(envelope.context.as_ref());
            let context = write_context("update_issue", context.as_ref(), downstream_operation_id)?;
            let update_mask = args
                .update_mask
                .into_iter()
                .map(|field| field - pb::ManagerIssueField::Unspecified as i32)
                .collect();
            Ok((
                method,
                v1::UpdateIssueArgs {
                    context: Some(context),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    update_mask,
                    patch: args.patch.map(issue_patch),
                    target_state: args.target_state,
                    terminal_report: args.terminal_report.map(|report| match report {
                        durable_update_issue_request::TerminalReport::Completion(value) => {
                            v1::update_issue_args::TerminalReport::Completion(value)
                        }
                        durable_update_issue_request::TerminalReport::Cancellation(value) => {
                            v1::update_issue_args::TerminalReport::Cancellation(value)
                        }
                    }),
                }
                .encode_to_vec(),
            ))
        }
        "transition_issue" => {
            let envelope = durable_envelope(request, "manager_transition_issue")?;
            let args = envelope.transition_issue.ok_or_else(|| {
                SouthboundError::not_sent("durable transition request is missing")
            })?;
            let manager_context = manager_context(envelope.context.as_ref());
            let context = write_context(
                "transition_issue",
                manager_context.as_ref(),
                downstream_operation_id,
            )?;
            let terminal_report = args.terminal_report.map(|report| match report {
                durable_transition_issue_request::TerminalReport::Completion(value) => {
                    v1::transition_issue_args::TerminalReport::Completion(value)
                }
                durable_transition_issue_request::TerminalReport::Cancellation(value) => {
                    v1::transition_issue_args::TerminalReport::Cancellation(value)
                }
            });
            Ok((
                method,
                v1::TransitionIssueArgs {
                    context: Some(context),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    target_state: args.target_state,
                    lease_handle: None,
                    deferred_review_at: args.deferred_review_at.map(instant),
                    claim_revision: args.claim_revision,
                    terminal_report,
                }
                .encode_to_vec(),
            ))
        }
        "claim_issue" => {
            let envelope = durable_envelope(request, "manager_claim_issue")?;
            let args = envelope
                .claim_issue
                .ok_or_else(|| SouthboundError::not_sent("durable claim request is missing"))?;
            let manager_context = manager_context(envelope.context.as_ref());
            let context = write_context(
                "claim_issue",
                manager_context.as_ref(),
                downstream_operation_id,
            )?;
            Ok((
                method,
                v1::ClaimIssueArgs {
                    context: Some(context),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    requested_lease: args.requested_lease.map(time_span),
                }
                .encode_to_vec(),
            ))
        }
        "heartbeat_issue_claim" => {
            let args = pb::HeartbeatIssueClaimArgs::decode(request.payload.as_slice()).map_err(
                |error| SouthboundError::not_sent(format!("heartbeat_issue_claim decode: {error}")),
            )?;
            let context = write_context(
                "heartbeat_issue_claim",
                args.context.as_ref(),
                downstream_operation_id,
            )?;
            Ok((
                method,
                v1::HeartbeatIssueClaimArgs {
                    context: Some(context),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    claim_revision: args.claim_revision,
                    lease_handle: args.lease_handle,
                    requested_extension: args.requested_extension.map(time_span),
                }
                .encode_to_vec(),
            ))
        }
        "release_issue_claim" => {
            let args =
                pb::ReleaseIssueClaimArgs::decode(request.payload.as_slice()).map_err(|error| {
                    SouthboundError::not_sent(format!("release_issue_claim decode: {error}"))
                })?;
            let context = write_context(
                "release_issue_claim",
                args.context.as_ref(),
                downstream_operation_id,
            )?;
            Ok((
                method,
                v1::ReleaseIssueClaimArgs {
                    context: Some(context),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    claim_revision: args.claim_revision,
                    lease_handle: args.lease_handle,
                }
                .encode_to_vec(),
            ))
        }
        "reconcile_expired_claim" => {
            let envelope = durable_envelope(request, "manager_reconcile_expired_claim")?;
            let args = envelope
                .reconcile_expired_claim
                .ok_or_else(|| SouthboundError::not_sent("durable reconcile request is missing"))?;
            let manager_context = manager_context(envelope.context.as_ref());
            let context = write_context(
                "reconcile_expired_claim",
                manager_context.as_ref(),
                downstream_operation_id,
            )?;
            Ok((
                method,
                v1::ReconcileExpiredClaimArgs {
                    context: Some(context),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    claim_revision: args.claim_revision,
                }
                .encode_to_vec(),
            ))
        }
        "add_issue_relation" => {
            let envelope = durable_envelope(request, "manager_add_issue_relation")?;
            let args = envelope.add_issue_relation.ok_or_else(|| {
                SouthboundError::not_sent("durable add relation request is missing")
            })?;
            let manager_context = manager_context(envelope.context.as_ref());
            let context = write_context(
                "add_issue_relation",
                manager_context.as_ref(),
                downstream_operation_id,
            )?;
            Ok((
                method,
                v1::AddIssueRelationArgs {
                    context: Some(context),
                    relation_id: args.relation_id,
                    kind: args.kind,
                    source_issue_id: args.source_issue_id,
                    source_expected_revision: args.source_expected_revision,
                    target_issue_id: args.target_issue_id,
                    target_expected_revision: args.target_expected_revision,
                }
                .encode_to_vec(),
            ))
        }
        "remove_issue_relation" => {
            let envelope = durable_envelope(request, "manager_remove_issue_relation")?;
            let args = envelope.remove_issue_relation.ok_or_else(|| {
                SouthboundError::not_sent("durable remove relation request is missing")
            })?;
            let manager_context = manager_context(envelope.context.as_ref());
            let context = write_context(
                "remove_issue_relation",
                manager_context.as_ref(),
                downstream_operation_id,
            )?;
            Ok((
                method,
                v1::RemoveIssueRelationArgs {
                    context: Some(context),
                    relation_id: args.relation_id,
                    source_issue_id: args.source_issue_id,
                    source_expected_revision: args.source_expected_revision,
                    target_issue_id: args.target_issue_id,
                    target_expected_revision: args.target_expected_revision,
                }
                .encode_to_vec(),
            ))
        }
        "add_issue_comment" => {
            let envelope = durable_envelope(request, "manager_add_issue_comment")?;
            let args = envelope.add_issue_comment.ok_or_else(|| {
                SouthboundError::not_sent("durable add comment request is missing")
            })?;
            let manager_context = manager_context(envelope.context.as_ref());
            let context = write_context(
                "add_issue_comment",
                manager_context.as_ref(),
                downstream_operation_id,
            )?;
            Ok((
                method,
                v1::AddIssueCommentArgs {
                    context: Some(context),
                    issue_id: args.issue_id,
                    expected_revision: args.expected_revision,
                    comment_id: args.comment_id,
                    body: args.body,
                }
                .encode_to_vec(),
            ))
        }
        unsupported => Err(SouthboundError::not_sent(format!(
            "manager backend method is not supported by the v1 tracker adapter: {unsupported}"
        ))),
    }
}

fn write_context(
    method: &str,
    context: Option<&pb::ManagerWriteContext>,
    downstream_operation_id: &str,
) -> Result<v1::WriteContext, SouthboundError> {
    let context =
        context.ok_or_else(|| SouthboundError::not_sent("manager write context is required"))?;
    let audit = context
        .actor_audit
        .as_ref()
        .ok_or_else(|| SouthboundError::not_sent("actor audit is required"))?;
    let actor = super::backend_adapter::ManagerActor {
        actor_id: audit.actor_id.clone(),
        actor_kind: audit.actor_kind,
        display_name: audit.display_name.clone(),
        agent_session_id: audit.agent_session_id.clone(),
    };
    super::backend_adapter::build_downstream_context(
        &super::backend_adapter::DownstreamContextInput {
            method_key: method.into(),
            canonical_target: context.target.clone(),
            schema_fingerprint: context.schema_fingerprint.clone(),
            downstream_operation_id: downstream_operation_id.into(),
            actor,
            correlation_id: context.correlation_id.clone(),
            reason: audit.reason.clone().unwrap_or_default(),
        },
    )
    .map_err(|error| SouthboundError::not_sent(error.to_string()))
}

fn manager_context(context: Option<&DurableWriteContext>) -> Option<pb::ManagerWriteContext> {
    context.map(|context| pb::ManagerWriteContext {
        target: context.target.clone(),
        schema_fingerprint: context.schema_fingerprint.clone(),
        actor_audit: context.actor_audit.clone(),
        correlation_id: context.correlation_id.clone(),
        ..Default::default()
    })
}

fn durable_envelope(
    request: &BackendDispatchRequest,
    expected_method: &str,
) -> Result<DurableBackendRequest, SouthboundError> {
    let envelope = DurableBackendRequest::decode(request.payload.as_slice()).or_else(|_| {
        // Unit-level adapters may receive an in-memory northbound request.
        // Durable manager dispatch cannot take this path because repository
        // loading admits only the envelope schema.
        let bytes = durable_request_from_manager(expected_method, &request.payload)?;
        DurableBackendRequest::decode(bytes.as_slice())
            .map_err(|error| SouthboundError::not_sent(format!("durable request decode: {error}")))
    })?;
    if envelope.method == expected_method {
        return Ok(envelope);
    }
    let bytes = durable_request_from_manager(expected_method, &request.payload)?;
    DurableBackendRequest::decode(bytes.as_slice())
        .map_err(|error| SouthboundError::not_sent(format!("durable request decode: {error}")))
}

fn time_span(value: pb::TimeSpan) -> v1::TimeSpan {
    v1::TimeSpan {
        millis: value.millis,
    }
}

fn instant(value: pb::UtcInstant) -> v1::UtcInstant {
    v1::UtcInstant {
        unix_epoch_millis: value.unix_epoch_millis,
    }
}

fn issue_patch(value: pb::ManagerIssuePatch) -> v1::IssuePatch {
    v1::IssuePatch {
        title: value.title,
        description: value.description,
        kind: value.kind,
        priority: value.priority,
        labels: value.labels,
        acceptance_criteria: value.acceptance_criteria,
        contexts: value.contexts,
        confirmation_status: value.confirmation_status,
        provenance_metadata: value
            .provenance_metadata
            .into_iter()
            .map(|entry| v1::MetadataEntry {
                key: entry.key,
                value: entry.value,
            })
            .collect(),
        source_evidence: value.source_evidence,
        due_at_change: value.due_at_change.map(|change| match change {
            pb::manager_issue_patch::DueAtChange::DueAt(value) => {
                v1::issue_patch::DueAtChange::DueAt(instant(value))
            }
            pb::manager_issue_patch::DueAtChange::ClearDueAt(_) => {
                v1::issue_patch::DueAtChange::ClearDueAt(v1::ClearField {})
            }
        }),
        review_at_change: value.review_at_change.map(|change| match change {
            pb::manager_issue_patch::ReviewAtChange::ReviewAt(value) => {
                v1::issue_patch::ReviewAtChange::ReviewAt(instant(value))
            }
            pb::manager_issue_patch::ReviewAtChange::ClearReviewAt(_) => {
                v1::issue_patch::ReviewAtChange::ClearReviewAt(v1::ClearField {})
            }
        }),
    }
}

fn extract_southbound_response(
    method: &str,
    response: Vec<u8>,
) -> Result<SouthboundResponse, SouthboundError> {
    macro_rules! extract {
        ($result:ty, $outcome:ident, $success:ident, $marker:expr, $handle:expr) => {{
            let result = <$result>::decode(response.as_slice()).map_err(|error| {
                SouthboundError::delivery_unknown(format!("{method} response decode: {error}"))
            })?;
            match result.outcome {
                Some(crate::v1::$outcome::Outcome::Success(value)) => {
                    let marker = $marker(&value).ok_or_else(|| {
                        SouthboundError::delivery_unknown(
                            "successful tracker response has no operation marker",
                        )
                    })?;
                    Ok(SouthboundResponse {
                        marker,
                        response_payload: response,
                        ephemeral_claim_handle: $handle,
                    })
                }
                Some(crate::v1::$outcome::Outcome::Error(error)) => {
                    Err(SouthboundError::typed_business(error))
                }
                None => Err(SouthboundError::delivery_unknown(
                    "tracker response has no outcome",
                )),
            }
        }};
    }

    match method {
        "create_issue" => extract!(
            v1::CreateIssueResult,
            create_issue_result,
            IssueMutation,
            |value: &v1::IssueMutation| value.operation.clone(),
            None
        ),
        "update_issue" => extract!(
            v1::UpdateIssueResult,
            update_issue_result,
            IssueMutation,
            |value: &v1::IssueMutation| value.operation.clone(),
            None
        ),
        "transition_issue" => extract!(
            v1::TransitionIssueResult,
            transition_issue_result,
            IssueMutation,
            |value: &v1::IssueMutation| value.operation.clone(),
            None
        ),
        "claim_issue" => {
            let result = v1::ClaimIssueResult::decode(response.as_slice()).map_err(|error| {
                SouthboundError::delivery_unknown(format!("claim_issue response decode: {error}"))
            })?;
            match result.outcome {
                Some(v1::claim_issue_result::Outcome::Success(value)) => {
                    let mutation = value.mutation.ok_or_else(|| {
                        SouthboundError::delivery_unknown(
                            "successful claim response has no mutation marker",
                        )
                    })?;
                    let marker = mutation.operation.ok_or_else(|| {
                        SouthboundError::delivery_unknown(
                            "successful claim response has no operation marker",
                        )
                    })?;
                    Ok(SouthboundResponse::committed_claim(
                        marker,
                        response,
                        value.lease_handle,
                    ))
                }
                Some(v1::claim_issue_result::Outcome::Error(error)) => {
                    Err(SouthboundError::typed_business(error))
                }
                None => Err(SouthboundError::delivery_unknown(
                    "tracker response has no outcome",
                )),
            }
        }
        "heartbeat_issue_claim" => extract!(
            v1::HeartbeatIssueClaimResult,
            heartbeat_issue_claim_result,
            ClaimMutation,
            |value: &v1::ClaimMutation| value.operation.clone(),
            None
        ),
        "release_issue_claim" => extract!(
            v1::ReleaseIssueClaimResult,
            release_issue_claim_result,
            IssueMutation,
            |value: &v1::IssueMutation| value.operation.clone(),
            None
        ),
        "reconcile_expired_claim" => extract!(
            v1::ReconcileExpiredClaimResult,
            reconcile_expired_claim_result,
            IssueMutation,
            |value: &v1::IssueMutation| value.operation.clone(),
            None
        ),
        "add_issue_relation" => extract!(
            v1::AddIssueRelationResult,
            add_issue_relation_result,
            RelationMutation,
            |value: &v1::RelationMutation| value.operation.clone(),
            None
        ),
        "remove_issue_relation" => extract!(
            v1::RemoveIssueRelationResult,
            remove_issue_relation_result,
            RelationMutation,
            |value: &v1::RelationMutation| value.operation.clone(),
            None
        ),
        "add_issue_comment" => extract!(
            v1::AddIssueCommentResult,
            add_issue_comment_result,
            CommentMutation,
            |value: &v1::CommentMutation| value.operation.clone(),
            None
        ),
        _ => Err(SouthboundError::delivery_unknown(format!(
            "unsupported tracker response method: {method}"
        ))),
    }
}

/// The complete result returned to the manager repository adapter.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DispatchResult {
    pub operation: pb::DurableOperation,
    pub state: pb::DurableOperationState,
    pub reason: String,
    pub response_payload: Vec<u8>,
    pub ephemeral_claim_handle: Option<Vec<u8>>,
    /// Backend identity extracted from a verified marker for same-transaction
    /// IssueMapping persistence by the manager repository.
    pub issue_mappings: Vec<BackendIssueMapping>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BackendIssueMapping {
    pub issue_id: String,
    pub backend_id: String,
    pub revision: u64,
}

/// Derive the exact marker contract before admission.  The worker must never
/// infer a successful backend revision from a response after delivery.
pub(crate) fn derive_marker_expectation(
    operation: &pb::DurableOperation,
    request: &[u8],
) -> Result<MarkerExpectation, BackendDispatchError> {
    let downstream_operation_id = operation.downstream_operation_id.clone().trim().to_owned();
    if downstream_operation_id.is_empty() {
        return Err(BackendDispatchError::InvalidRequest(
            "downstream_operation_id",
        ));
    }
    let method_key = canonical_backend_method(&operation.method).to_owned();
    validate_manager_request_target(&method_key, request)?;
    let (scope, affected_issue_ids, entity_revisions) = match method_key.as_str() {
        "create_issue" => {
            let args = pb::CreateIssueArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                // TaskChampion initializes a newly created Issue at revision 0.
                vec![entity_revision(issue_id, 0)],
            )
        }
        "update_issue" => {
            let args = pb::UpdateIssueArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            let expected_revision = required_revision(args.expected_revision)?;
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                vec![entity_revision(
                    issue_id,
                    increment_revision(expected_revision)?,
                )],
            )
        }
        "transition_issue" => {
            let args = pb::TransitionIssueArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            let expected_revision = required_revision(args.expected_revision)?;
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                vec![entity_revision(
                    issue_id,
                    increment_revision(expected_revision)?,
                )],
            )
        }
        "claim_issue" => {
            let args = pb::ClaimIssueArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            let expected_revision = required_revision(args.expected_revision)?;
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                vec![entity_revision(
                    issue_id,
                    increment_revision(expected_revision)?,
                )],
            )
        }
        "heartbeat_issue_claim" => {
            let args = pb::HeartbeatIssueClaimArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            let expected_revision =
                matching_claim_revision(args.expected_revision, args.claim_revision)?;
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                vec![entity_revision(
                    issue_id,
                    increment_revision(expected_revision)?,
                )],
            )
        }
        "release_issue_claim" => {
            let args = pb::ReleaseIssueClaimArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            let expected_revision = required_revision(args.expected_revision)?;
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                vec![entity_revision(
                    issue_id,
                    increment_revision(expected_revision)?,
                )],
            )
        }
        "reconcile_expired_claim" => {
            let args = pb::ReconcileExpiredClaimArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            let expected_revision =
                reconcile_claim_revision(args.expected_revision, args.claim_revision)?;
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                vec![entity_revision(
                    issue_id,
                    increment_revision(expected_revision)?,
                )],
            )
        }
        "add_issue_relation" => {
            let args = pb::AddIssueRelationArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            relation_expectation(
                relation_anchor_for_add(args.kind)?,
                args.source_issue_id,
                args.source_expected_revision,
                args.target_issue_id,
                args.target_expected_revision,
            )?
        }
        "remove_issue_relation" => {
            let args = pb::RemoveIssueRelationArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            relation_expectation(
                relation_anchor_for_remove(
                    &args.relation_id,
                    &args.source_issue_id,
                    &args.target_issue_id,
                ),
                args.source_issue_id,
                args.source_expected_revision,
                args.target_issue_id,
                args.target_expected_revision,
            )?
        }
        "add_issue_comment" => {
            let args = pb::AddIssueCommentArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            let expected_revision = required_revision(args.expected_revision)?;
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                vec![entity_revision(
                    issue_id,
                    increment_revision(expected_revision)?,
                )],
            )
        }
        "apply_issue_admin_action" => {
            let args = pb::ApplyIssueAdminActionArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = required_issue_id(args.issue_id)?;
            let expected_revision = required_revision(args.expected_revision)?;
            if pb::IssueAdminAction::try_from(args.action)
                .ok()
                .filter(|action| *action != pb::IssueAdminAction::Unspecified)
                .is_none()
            {
                return Err(BackendDispatchError::InvalidRequest("action"));
            }
            // Administrative receipts may represent either NO_CHANGE at the
            // supplied revision or one atomic canonical mutation. The
            // verifier recognizes this method key and accepts exactly those
            // two values, never a later current Issue revision.
            (
                v1::OperationScope::Issue,
                vec![issue_id.clone()],
                vec![entity_revision(issue_id, expected_revision)],
            )
        }
        _ => return Err(BackendDispatchError::InvalidRequest("marker_expectation")),
    };
    let payload_fingerprint =
        crate::tracker::operation_log::marker_digest(&[&method_key, &downstream_operation_id]);
    Ok(MarkerExpectation {
        operation_id: downstream_operation_id,
        method_key,
        scope: scope as i32,
        payload_fingerprint,
        contract_fingerprint: operation
            .audit
            .as_ref()
            .map(|audit| audit.contract_fingerprint.clone())
            .filter(|value| !value.is_empty())
            .ok_or(BackendDispatchError::InvalidRequest("contract_fingerprint"))?,
        affected_issue_ids,
        entity_revisions,
        result_replayable: operation.result_replayability
            == v1::OperationResultReplayability::Replayable as i32,
    })
}

/// Rebuild the public administrative receipt from the immutable Tracker
/// marker, never from a later read of the mutable Issue.  A caller must first
/// verify the marker against the matching expectation; this helper only
/// translates the stored result shape.
pub(crate) fn admin_action_result_from_marker(
    marker: &v1::OperationRecord,
    args: &pb::ApplyIssueAdminActionArgs,
) -> Result<pb::IssueAdminActionResult, BackendDispatchError> {
    let action = pb::IssueAdminAction::try_from(args.action)
        .ok()
        .filter(|action| *action != pb::IssueAdminAction::Unspecified)
        .ok_or(BackendDispatchError::InvalidRequest("action"))?;
    let expected_revision = required_revision(args.expected_revision)?;
    let snapshot = marker
        .result_snapshot
        .as_ref()
        .ok_or(BackendDispatchError::InvalidRequest("result_snapshot"))?;
    if !snapshot.manager_result_payload.is_empty() {
        let receipt =
            pb::IssueAdminActionResult::decode(snapshot.manager_result_payload.as_slice())
                .map_err(|_| {
                    BackendDispatchError::InvalidRequest("typed administrative receipt")
                })?;
        if snapshot.manager_result_schema != "issue_tracker.manager.v1.IssueAdminActionResult"
            || snapshot.manager_result_digest != digest_bytes(&snapshot.manager_result_payload)
        {
            return Err(BackendDispatchError::InvalidRequest(
                "typed administrative receipt digest",
            ));
        }
        if receipt.issue_id != args.issue_id
            || receipt.audit_operation_id != marker.downstream_operation_id
            || receipt.action != action.as_str_name()
            || (receipt.previous_revision != expected_revision
                || receipt.resulting_revision < expected_revision
                || receipt.resulting_revision > increment_revision(expected_revision)?)
            || (receipt.changed
                != (receipt.resulting_revision == increment_revision(expected_revision)?))
            || (!receipt.changed && receipt.ownership_cleared)
        {
            return Err(BackendDispatchError::InvalidRequest(
                "typed administrative receipt identity",
            ));
        }
        if receipt.dependency_override_applied {
            if receipt.dependency_override_observations.is_empty()
                || receipt
                    .dependency_override_observations
                    .iter()
                    .any(|observation| {
                        observation.relation_id.trim().is_empty()
                            || uuid::Uuid::parse_str(&observation.dependency_issue_id).is_err()
                            || observation.observed_state == v1::IssueState::Unspecified as i32
                    })
            {
                return Err(BackendDispatchError::InvalidRequest(
                    "typed dependency override evidence",
                ));
            }
            let mut observed_issue_ids = receipt
                .dependency_override_observations
                .iter()
                .map(|observation| observation.dependency_issue_id.clone())
                .collect::<Vec<_>>();
            observed_issue_ids.sort_unstable();
            observed_issue_ids.dedup();
            let mut receipt_issue_ids = receipt.dependency_override_target_issue_ids.clone();
            receipt_issue_ids.sort_unstable();
            receipt_issue_ids.dedup();
            if observed_issue_ids != receipt_issue_ids {
                return Err(BackendDispatchError::InvalidRequest(
                    "typed dependency override targets",
                ));
            }
        }
        return Ok(receipt);
    }
    // A generic IssueMutation snapshot is insufficient for an administrative
    // receipt: it does not prove lease classification, override evidence, or
    // the exact terminal decision. Treat the missing typed payload as a
    // confirmation failure so callers enter reconciliation instead of
    // publishing a result reconstructed from mutable state.
    Err(BackendDispatchError::InvalidRequest(
        "typed administrative receipt is missing",
    ))
}

fn validate_manager_request_target(
    method: &str,
    request: &[u8],
) -> Result<(), BackendDispatchError> {
    let (context, issue_id) = match method {
        "create_issue" => {
            let args = pb::CreateIssueArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        "update_issue" => {
            let args = pb::UpdateIssueArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        "transition_issue" => {
            let args = pb::TransitionIssueArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        "claim_issue" => {
            let args = pb::ClaimIssueArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        "heartbeat_issue_claim" => {
            let args = pb::HeartbeatIssueClaimArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        "release_issue_claim" => {
            let args = pb::ReleaseIssueClaimArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        "reconcile_expired_claim" => {
            let args = pb::ReconcileExpiredClaimArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        "add_issue_relation" => {
            let args = pb::AddIssueRelationArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = relation_anchor_for_add(args.kind)?
                .select(args.source_issue_id, args.target_issue_id);
            (args.context, issue_id)
        }
        "remove_issue_relation" => {
            let args = pb::RemoveIssueRelationArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            let issue_id = relation_anchor_for_remove(
                &args.relation_id,
                &args.source_issue_id,
                &args.target_issue_id,
            )
            .select(args.source_issue_id, args.target_issue_id);
            (args.context, issue_id)
        }
        "add_issue_comment" => {
            let args = pb::AddIssueCommentArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        "apply_issue_admin_action" => {
            let args = pb::ApplyIssueAdminActionArgs::decode(request)
                .map_err(|_| BackendDispatchError::InvalidRequest("request"))?;
            (args.context, args.issue_id)
        }
        _ => return Err(BackendDispatchError::InvalidRequest("marker_expectation")),
    };
    // Gateway admission requires a write context. Keeping this helper
    // tolerant of absent context preserves marker recovery for legacy rows.
    let Some(context) = context else {
        return Ok(());
    };
    let issue_id = required_issue_id(issue_id)?;
    if context.target != format!("issue:{issue_id}") {
        return Err(BackendDispatchError::InvalidRequest("write_context.target"));
    }
    Ok(())
}

fn required_issue_id(issue_id: String) -> Result<String, BackendDispatchError> {
    if issue_id.trim().is_empty() {
        Err(BackendDispatchError::InvalidRequest("issue_id"))
    } else {
        Ok(issue_id)
    }
}

fn required_revision(revision: Option<u64>) -> Result<u64, BackendDispatchError> {
    revision.ok_or(BackendDispatchError::InvalidRequest("expected_revision"))
}

fn matching_claim_revision(
    expected_revision: Option<u64>,
    claim_revision: Option<u64>,
) -> Result<u64, BackendDispatchError> {
    let expected_revision = required_revision(expected_revision)?;
    let claim_revision =
        claim_revision.ok_or(BackendDispatchError::InvalidRequest("claim_revision"))?;
    if claim_revision != expected_revision {
        return Err(BackendDispatchError::InvalidRequest("claim_revision"));
    }
    Ok(expected_revision)
}

/// Reconciliation fences the issue revision, while the lease revision is an
/// optional diagnostic supplied by callers that observed the claim.  When it
/// is present it must still agree with the issue revision; omission is valid
/// because the tracker re-checks the complete lease classification at the
/// write boundary.
fn reconcile_claim_revision(
    expected_revision: Option<u64>,
    claim_revision: Option<u64>,
) -> Result<u64, BackendDispatchError> {
    let expected_revision = required_revision(expected_revision)?;
    if claim_revision.is_some_and(|claim_revision| claim_revision != expected_revision) {
        return Err(BackendDispatchError::InvalidRequest("claim_revision"));
    }
    Ok(expected_revision)
}

fn increment_revision(revision: u64) -> Result<u64, BackendDispatchError> {
    revision
        .checked_add(1)
        .ok_or(BackendDispatchError::InvalidRequest("expected_revision"))
}

fn entity_revision(
    issue_id: String,
    resulting_revision: u64,
) -> super::backend_adapter::EntityRevisionExpectation {
    super::backend_adapter::EntityRevisionExpectation {
        backend_id: issue_id.clone(),
        issue_id,
        resulting_revision,
    }
}

fn relation_expectation(
    anchor: RelationMarkerAnchor,
    source_issue_id: String,
    source_expected_revision: Option<u64>,
    target_issue_id: String,
    target_expected_revision: Option<u64>,
) -> Result<
    (
        v1::OperationScope,
        Vec<String>,
        Vec<super::backend_adapter::EntityRevisionExpectation>,
    ),
    BackendDispatchError,
> {
    let source_issue_id = required_issue_id(source_issue_id)?;
    let target_issue_id = required_issue_id(target_issue_id)?;
    let source_expected_revision = required_revision(source_expected_revision)?;
    let target_expected_revision = required_revision(target_expected_revision)?;
    let anchor_issue_id = anchor.select(source_issue_id, target_issue_id);
    let anchor_expected_revision =
        anchor.select(source_expected_revision, target_expected_revision);
    Ok((
        // Blocks mutate and mark the dependency holder (target); all other
        // relation kinds use the source as their durable marker anchor.
        v1::OperationScope::Issue,
        vec![anchor_issue_id.clone()],
        vec![entity_revision(
            anchor_issue_id,
            increment_revision(anchor_expected_revision)?,
        )],
    ))
}

/// The Issue whose revision and marker prove a relation mutation completed.
///
/// Blocks mutate the dependency holder (target); every other relation mutates
/// the source.  Keeping this policy in one type prevents admission, durable
/// persistence, and reconciliation from selecting different anchors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RelationMarkerAnchor {
    Source,
    Target,
}

impl RelationMarkerAnchor {
    pub(crate) fn select<T>(self, source: T, target: T) -> T {
        match self {
            Self::Source => source,
            Self::Target => target,
        }
    }
}

pub(crate) fn relation_anchor_for_add(
    kind: i32,
) -> Result<RelationMarkerAnchor, BackendDispatchError> {
    match v1::RelationKind::try_from(kind) {
        Ok(v1::RelationKind::Blocks) => Ok(RelationMarkerAnchor::Target),
        Ok(v1::RelationKind::ParentChild)
        | Ok(v1::RelationKind::Related)
        | Ok(v1::RelationKind::DiscoveredFrom)
        | Ok(v1::RelationKind::Supersedes) => Ok(RelationMarkerAnchor::Source),
        _ => Err(BackendDispatchError::InvalidRequest("relation_kind")),
    }
}

pub(crate) fn relation_anchor_for_remove(
    relation_id: &str,
    source_issue_id: &str,
    target_issue_id: &str,
) -> RelationMarkerAnchor {
    if relation_id == format!("blocks:{source_issue_id}:{target_issue_id}") {
        RelationMarkerAnchor::Target
    } else {
        RelationMarkerAnchor::Source
    }
}

pub(crate) fn expectation_snapshot(expected: &MarkerExpectation) -> v1::OperationRecord {
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
        result_replayability: if expected.result_replayable {
            v1::OperationResultReplayability::Replayable as i32
        } else {
            v1::OperationResultReplayability::NotReplayable as i32
        },
        ..Default::default()
    }
}

pub(crate) fn expectation_from_snapshot(
    snapshot: &v1::OperationRecord,
) -> Result<MarkerExpectation, BackendDispatchError> {
    Ok(MarkerExpectation {
        operation_id: snapshot.downstream_operation_id.clone(),
        method_key: snapshot.method_key.clone(),
        scope: snapshot.scope,
        payload_fingerprint: snapshot.payload_fingerprint.clone(),
        contract_fingerprint: snapshot.contract_fingerprint.clone(),
        affected_issue_ids: snapshot.affected_issue_ids.clone(),
        entity_revisions: snapshot
            .entity_revisions
            .iter()
            .map(
                |revision| super::backend_adapter::EntityRevisionExpectation {
                    issue_id: revision.issue_id.clone(),
                    backend_id: revision.backend_id.clone(),
                    resulting_revision: revision.resulting_revision,
                },
            )
            .collect(),
        result_replayable: snapshot.result_replayability
            == v1::OperationResultReplayability::Replayable as i32,
    })
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum BackendDispatchError {
    #[error("backend dispatch request is invalid: {0}")]
    InvalidRequest(&'static str),
    #[error("outbox transition failed: {0}")]
    Outbox(#[from] OutboxError),
    #[error("operation reconciliation failed: {0}")]
    Reconcile(#[from] ReconcileError),
}

/// Inputs required to dispatch one already-leased outbox entry.
///
/// Grouping the lease-bound values keeps the dispatch boundary explicit while
/// avoiding a call that can accidentally reorder related persistence facts.
pub(crate) struct DispatchLeaseInput<'a> {
    pub entry: &'a mut pb::OutboxEntry,
    pub operation: &'a pb::DurableOperation,
    pub request: BackendDispatchRequest,
    pub expected: MarkerExpectation,
    pub lease_id: &'a str,
    pub retry_budget: &'a pb::RetryBudget,
    pub now_millis: i64,
}

/// Dispatch one already-leased outbox entry and return a repository-ready
/// operation value. The caller persists the returned operation and outbox
/// entry in its own single-writer transaction.
pub(crate) async fn dispatch_leased<D: TaskChampionV1Dispatcher>(
    dispatcher: &D,
    input: DispatchLeaseInput<'_>,
) -> Result<DispatchResult, BackendDispatchError> {
    let DispatchLeaseInput {
        entry,
        operation,
        request,
        expected,
        lease_id,
        retry_budget,
        now_millis,
    } = input;
    // Administrative sends have a durable SENDING fence.  If the southbound
    // call outlives the short coordination lease, the lease expiry does not
    // prove that the task stopped; close or retry that same fenced attempt
    // instead of leaving a permanently DISPATCHED outbox row behind.
    let allow_expired_lease = operation.method == "manager_apply_issue_admin_action";
    if request.method.trim().is_empty() {
        return Err(BackendDispatchError::InvalidRequest("method"));
    }
    if entry.operation_id != operation.operation_id {
        return Err(BackendDispatchError::InvalidRequest("operation_id"));
    }
    let payload_digest = digest_bytes(&request.payload);
    if canonical_backend_method(&request.method) != canonical_backend_method(&expected.method_key) {
        return Err(BackendDispatchError::InvalidRequest("method"));
    }
    if entry.payload_digest != payload_digest {
        return Err(BackendDispatchError::InvalidRequest("payload_digest"));
    }
    if !matches!(
        pb::DurableOperationState::try_from(operation.state),
        Ok(pb::DurableOperationState::Received)
            | Ok(pb::DurableOperationState::Pending)
            | Ok(pb::DurableOperationState::Unconfirmed)
    ) {
        return Err(BackendDispatchError::InvalidRequest("operation_state"));
    }

    // Persisted operation state is advanced only in the value returned to the
    // caller; a not-sent request returns the original operation unchanged.
    let mut pending = operation.clone();
    if pending.state == pb::DurableOperationState::Received as i32 {
        transition_operation(
            &mut pending,
            pb::DurableOperationState::Pending,
            None,
            now_millis,
        )
        .map_err(|error| BackendDispatchError::Reconcile(ReconcileError::from(error)))?;
    }
    outbox::mark_dispatched(entry, lease_id, now_millis)?;

    let response = match dispatcher.dispatch(&request).await {
        Ok(response) => response,
        // A typed response can still describe an infrastructure failure
        // (for example BACKEND_UNAVAILABLE) rather than a validation
        // rejection.  Such a response is not proof that the canonical write
        // did not commit: keep the operation fenced and reconcile its marker
        // instead of incorrectly clearing readiness as FAILED.
        Err(SouthboundError::TypedBusiness(error)) if !typed_error_is_definitive(&error) => {
            acknowledge_after_dispatch(entry, lease_id, now_millis, allow_expired_lease)?;
            let reconciliation =
                reconcile::reconcile_marker(&mut pending, None, &expected, now_millis)?;
            return Ok(dispatch_result(
                pending,
                reconciliation,
                Vec::new(),
                None,
                &expected,
            ));
        }
        Err(SouthboundError::TypedBusiness(error)) => {
            outbox::dead_letter(
                entry,
                lease_id,
                super::error_mapping::manager_error_code(&error),
                now_millis,
            )?;
            transition_operation(
                &mut pending,
                pb::DurableOperationState::Failed,
                Some(pb::FailureClass::Terminal),
                now_millis,
            )
            .map_err(|error| BackendDispatchError::Reconcile(ReconcileError::from(error)))?;
            // A typed business rejection is proved to occur before the
            // canonical write boundary; it must not suppress readiness.
            pending.affects_ready = false;
            pending.terminal_snapshot = Some(
                pb::durable_operation::TerminalSnapshot::ErrorSnapshot(pb::Snapshot {
                    payload: manager_error_from_tracker(*error).encode_to_vec(),
                    schema: "issue_tracker.manager.v1.ManagerError".into(),
                }),
            );
            return Ok(DispatchResult {
                operation: pending,
                state: pb::DurableOperationState::Failed,
                reason: "tracker rejected the request".into(),
                response_payload: Vec::new(),
                ephemeral_claim_handle: None,
                issue_mappings: Vec::new(),
            });
        }
        Err(SouthboundError::NotSent(_reason)) => {
            if entry.attempt >= retry_budget.max_attempts {
                outbox::dead_letter(
                    entry,
                    lease_id,
                    pb::ManagerErrorCode::RetryExhausted,
                    now_millis,
                )?;
                transition_operation(
                    &mut pending,
                    pb::DurableOperationState::Failed,
                    Some(pb::FailureClass::Terminal),
                    now_millis,
                )
                .map_err(|error| BackendDispatchError::Reconcile(ReconcileError::from(error)))?;
                // All attempts are terminally exhausted without a send.
                pending.affects_ready = false;
                pending.terminal_snapshot = Some(
                    pb::durable_operation::TerminalSnapshot::ErrorSnapshot(pb::Snapshot {
                        schema: "issue_tracker.manager.v1.ManagerError".into(),
                        payload: pb::ManagerError {
                            code: pb::ManagerErrorCode::RetryExhausted as i32,
                            message: "backend dispatch retry budget exhausted".into(),
                            retryable: true,
                            detail: Some(pb::manager_error::Detail::OperationDiagnosis(
                                pb::OperationDiagnosis {
                                    operation_id: pending.operation_id.clone(),
                                    classification: "NOT_SENT_RETRY_EXHAUSTED".into(),
                                    evidence_ref: pending.operation_id.clone(),
                                    replayability: "NEW_READMISSION_ONLY".into(),
                                    resolution_state: "ALL_ATTEMPTS_NOT_SENT".into(),
                                    repair_condition:
                                        "retry with a new maintenance readmission after backoff"
                                            .into(),
                                    ..Default::default()
                                },
                            )),
                        }
                        .encode_to_vec(),
                    }),
                );
                return Ok(DispatchResult {
                    operation: pending,
                    state: pb::DurableOperationState::Failed,
                    reason: "backend dispatch retry budget exhausted".into(),
                    response_payload: Vec::new(),
                    ephemeral_claim_handle: None,
                    issue_mappings: Vec::new(),
                });
            }
            retry_after_dispatch(
                entry,
                lease_id,
                now_millis.saturating_add(retry_delay_millis(retry_budget)),
                pb::ManagerErrorCode::BackendUnavailable,
                now_millis,
                allow_expired_lease,
            )?;
            return Ok(DispatchResult {
                operation: operation.clone(),
                state: pb::DurableOperationState::Received,
                reason: "backend request was not sent; outbox retry scheduled".into(),
                response_payload: Vec::new(),
                ephemeral_claim_handle: None,
                issue_mappings: Vec::new(),
            });
        }
        Err(SouthboundError::DeliveryUnknown(_reason)) => {
            acknowledge_after_dispatch(entry, lease_id, now_millis, allow_expired_lease)?;
            let reconciliation =
                reconcile::reconcile_marker(&mut pending, None, &expected, now_millis)?;
            return Ok(dispatch_result(
                pending,
                reconciliation,
                Vec::new(),
                None,
                &expected,
            ));
        }
    };

    acknowledge_after_dispatch(entry, lease_id, now_millis, allow_expired_lease)?;
    let reconciliation =
        reconcile::reconcile_marker(&mut pending, Some(&response.marker), &expected, now_millis)?;
    Ok(dispatch_result(
        pending,
        reconciliation,
        response.response_payload,
        response.ephemeral_claim_handle,
        &expected,
    ))
}

fn acknowledge_after_dispatch(
    entry: &mut pb::OutboxEntry,
    lease_id: &str,
    now_millis: i64,
    allow_expired_lease: bool,
) -> Result<(), BackendDispatchError> {
    match outbox::acknowledge(entry, lease_id, now_millis) {
        Ok(()) => Ok(()),
        Err(OutboxError::LeaseExpired) if allow_expired_lease => {
            if entry.lease_id.as_deref() != Some(lease_id) {
                return Err(OutboxError::LeaseMismatch.into());
            }
            entry.status = pb::OutboxStatus::Acknowledged as i32;
            entry.acknowledged_at = Some(pb::UtcInstant {
                unix_epoch_millis: now_millis,
            });
            entry.lease_id = None;
            entry.lease_expires_at = None;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn retry_after_dispatch(
    entry: &mut pb::OutboxEntry,
    lease_id: &str,
    next_attempt_at_millis: i64,
    error_code: pb::ManagerErrorCode,
    now_millis: i64,
    allow_expired_lease: bool,
) -> Result<(), BackendDispatchError> {
    match outbox::retry(
        entry,
        lease_id,
        next_attempt_at_millis,
        error_code,
        now_millis,
    ) {
        Ok(()) => Ok(()),
        Err(OutboxError::LeaseExpired) if allow_expired_lease => {
            if entry.lease_id.as_deref() != Some(lease_id) {
                return Err(OutboxError::LeaseMismatch.into());
            }
            entry.status = pb::OutboxStatus::Ready as i32;
            entry.next_attempt_at = Some(pb::UtcInstant {
                unix_epoch_millis: next_attempt_at_millis,
            });
            entry.error_code = error_code as i32;
            entry.lease_id = None;
            entry.lease_expires_at = None;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn canonical_backend_method(method: &str) -> &str {
    method.strip_prefix("manager_").unwrap_or(method)
}

fn retry_delay_millis(retry_budget: &pb::RetryBudget) -> i64 {
    retry_budget
        .base_backoff
        .as_ref()
        .map(|value| i64::try_from(value.millis).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn manager_error_from_tracker(error: v1::IssueTrackerError) -> pb::ManagerError {
    let detail = tracker_precondition_detail(&error);
    let code = super::error_mapping::manager_error_code(&error);
    // Tracker may set retryable on a transport-shaped payload even when the
    // operation itself is a deterministic business rejection.  Manager
    // retries are reserved for backend unavailability; revision/lease/
    // dependency preconditions are terminal and must not re-enter delivery.
    let retryable = error.retryable && code == pb::ManagerErrorCode::BackendUnavailable;
    pb::ManagerError {
        code: code as i32,
        message: error.message,
        retryable,
        detail,
    }
}

/// Preserve lease/dependency preconditions as structured Manager diagnostics
/// instead of forcing consumers to parse human-readable messages.  The
/// Tracker error carries only safe observations (classification, expiry and
/// revision) and never a raw lease tuple; the Manager forwards the fields it
/// exposes in its diagnosis contract.
pub(crate) fn tracker_precondition_detail(
    error: &v1::IssueTrackerError,
) -> Option<pb::manager_error::Detail> {
    let code = v1::ErrorCode::try_from(error.code).ok()?;
    if code == v1::ErrorCode::RevisionConflict {
        if let Some(v1::issue_tracker_error::Detail::RevisionConflict(conflict)) =
            error.detail.as_ref()
        {
            return Some(pb::manager_error::Detail::RevisionConflict(
                pb::RevisionConflict {
                    expected_revision: conflict.expected_revision,
                    current_revision: conflict.current_revision,
                },
            ));
        }
        return None;
    }
    let mut invalid_attributes = Vec::new();
    let mut observed_revision = None;
    let mut lease_expires_at = None;
    let mut open_blocker_issue_ids = Vec::new();
    let (diagnosis_class, lease_classification, repair_condition) = match code {
        v1::ErrorCode::InvalidLease | v1::ErrorCode::LeaseExpired => {
            let (diagnosis_class, lease_classification) = match error.detail.as_ref() {
                Some(v1::issue_tracker_error::Detail::LeasePrecondition(precondition))
                    if precondition.classification == "INCONSISTENT" =>
                {
                    invalid_attributes.extend(precondition.invalid_attributes.iter().cloned());
                    lease_expires_at =
                        precondition
                            .lease_expires_at
                            .as_ref()
                            .map(|instant| pb::UtcInstant {
                                unix_epoch_millis: instant.unix_epoch_millis,
                            });
                    if precondition.observed_revision != 0 {
                        observed_revision = Some(precondition.observed_revision);
                    }
                    ("LEASE_INCONSISTENT", Some("INCONSISTENT"))
                }
                Some(v1::issue_tracker_error::Detail::LeasePrecondition(precondition))
                    if precondition.classification == "EXPIRED" =>
                {
                    lease_expires_at =
                        precondition
                            .lease_expires_at
                            .as_ref()
                            .map(|instant| pb::UtcInstant {
                                unix_epoch_millis: instant.unix_epoch_millis,
                            });
                    if precondition.observed_revision != 0 {
                        observed_revision = Some(precondition.observed_revision);
                    }
                    ("ACTIVE_LEASE", Some("EXPIRED"))
                }
                Some(v1::issue_tracker_error::Detail::LeasePrecondition(precondition)) => {
                    lease_expires_at =
                        precondition
                            .lease_expires_at
                            .as_ref()
                            .map(|instant| pb::UtcInstant {
                                unix_epoch_millis: instant.unix_epoch_millis,
                            });
                    if precondition.observed_revision != 0 {
                        observed_revision = Some(precondition.observed_revision);
                    }
                    ("ACTIVE_LEASE", Some("ACTIVE"))
                }
                _ if code == v1::ErrorCode::LeaseExpired => ("ACTIVE_LEASE", Some("EXPIRED")),
                _ => ("ACTIVE_LEASE", Some("ACTIVE")),
            };
            (
                diagnosis_class,
                lease_classification,
                "renew, release, or explicitly recover the ownership lease",
            )
        }
        v1::ErrorCode::OwnershipConflict => {
            if let Some(v1::issue_tracker_error::Detail::OwnershipConflict(conflict)) =
                error.detail.as_ref()
                && conflict.current_revision != 0
            {
                observed_revision = Some(conflict.current_revision);
            }
            (
                "ACTIVE_LEASE",
                Some("ACTIVE"),
                "wait for the current owner or use an authorized administrative recovery",
            )
        }
        v1::ErrorCode::BlockedByOpenIssue => {
            if let Some(v1::issue_tracker_error::Detail::BlockerConflict(conflict)) =
                error.detail.as_ref()
            {
                open_blocker_issue_ids = conflict.open_blocker_issue_ids.clone();
            }
            (
                "DEPENDENCY_BLOCKED",
                None,
                "resolve the unfinished blocking dependency before transitioning",
            )
        }
        _ => return None,
    };
    open_blocker_issue_ids.sort_unstable();
    open_blocker_issue_ids.dedup();
    let state = if diagnosis_class == "DEPENDENCY_BLOCKED" {
        "BLOCKED"
    } else {
        "OPEN"
    };
    Some(pb::manager_error::Detail::OwnershipIntegrityDiagnosis(
        pb::OwnershipIntegrityDiagnosis {
            diagnosis_class: diagnosis_class.into(),
            lease_classification: lease_classification.map(str::to_owned),
            repair_condition: repair_condition.into(),
            state: state.into(),
            invalid_attributes,
            observed_revision,
            lease_expires_at,
            open_blocker_issue_ids,
            ..Default::default()
        },
    ))
}

/// Return whether a typed Tracker response proves that validation rejected
/// the request before the canonical commit boundary.  Error codes describing
/// storage, transport, or an explicitly unknown result remain ambiguous and
/// must be reconciled through the immutable operation marker.
pub(crate) fn typed_error_is_definitive(error: &v1::IssueTrackerError) -> bool {
    matches!(
        v1::ErrorCode::try_from(error.code),
        Ok(v1::ErrorCode::InvalidArgument
            | v1::ErrorCode::InvalidMethod
            | v1::ErrorCode::InvalidPageToken
            | v1::ErrorCode::IssueNotFound
            | v1::ErrorCode::RelationNotFound
            | v1::ErrorCode::CommentNotFound
            | v1::ErrorCode::OperationNotFound
            | v1::ErrorCode::ArtifactNotFound
            | v1::ErrorCode::InvalidStateTransition
            | v1::ErrorCode::RevisionConflict
            | v1::ErrorCode::OwnershipConflict
            | v1::ErrorCode::InvalidLease
            | v1::ErrorCode::LeaseExpired
            | v1::ErrorCode::BlockedByOpenIssue
            | v1::ErrorCode::RelationCycle
            | v1::ErrorCode::IdempotencyConflict
            | v1::ErrorCode::SecretDetected
            | v1::ErrorCode::ContractMismatch
            | v1::ErrorCode::SchemaMismatch
            | v1::ErrorCode::LeaseNotExpired
            | v1::ErrorCode::ResourceLimitExceeded
            | v1::ErrorCode::PageTokenExpired)
    )
}

fn dispatch_result(
    operation: pb::DurableOperation,
    reconciliation: ReconcileResult,
    response_payload: Vec<u8>,
    ephemeral_claim_handle: Option<Vec<u8>>,
    expected: &MarkerExpectation,
) -> DispatchResult {
    let reason = match &reconciliation.reason {
        ReconcileReason::MarkerConfirmed => "backend marker confirmed".to_owned(),
        ReconcileReason::DeliveryUnknown => "backend delivery result is unknown".to_owned(),
        ReconcileReason::MarkerMismatch(mismatch) => {
            format!("backend marker requires manual resolution: {mismatch}")
        }
        ReconcileReason::MarkerExpectationMissing => {
            "backend marker expectation is missing; manual resolution is required".to_owned()
        }
    };
    DispatchResult {
        operation,
        state: reconciliation.state,
        reason,
        response_payload,
        ephemeral_claim_handle,
        issue_mappings: if reconciliation.state == pb::DurableOperationState::Committed {
            expected
                .entity_revisions
                .iter()
                .map(|revision| BackendIssueMapping {
                    issue_id: revision.issue_id.clone(),
                    backend_id: revision.backend_id.clone(),
                    revision: revision.resulting_revision,
                })
                .collect()
        } else {
            Vec::new()
        },
    }
}

#[cfg(test)]
#[path = "backend_dispatch_tests.rs"]
mod tests;
