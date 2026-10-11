//! SQLite persistence for manager-owned entities and durable operations.
#![allow(clippy::result_large_err, clippy::type_complexity)]
//!
//! The repository deliberately stores manager protobufs as opaque payloads.  A
//! small set of identity and revision columns is kept beside each payload for
//! CAS updates and stable traversal; canonical backend Issues never enter this
//! database.

use super::{
    backend_dispatch::{
        DurableBackendRequest, DurableCreateIssueRequest, expectation_from_snapshot,
        expectation_snapshot, relation_anchor_for_add, relation_anchor_for_remove,
        validate_durable_request_snapshot,
    },
    candidates::CandidateStore,
    extract_apply::{ExtractionCandidateIdentity, ExtractionSuppression},
    extraction::ExtractionRunStore,
    gateway::{IdempotencyDecision, ManagerRepository as GatewayRepository},
    model::ManagerCommit,
    operations::{
        IdempotencyScope, MANAGER_MUTATION_SNAPSHOT_SCHEMA, NewOperation, OperationHistoryDecision,
        OperationHistoryResolution, OperationScope, digest_bytes, transition_operation,
        validate_display_safe_text,
    },
    outbox,
    pagination::{
        HistoryCursor, HistoryCursorBoundary, OperationHistoryPageCursor,
        OperationHistoryPageTokenCodec, PageCursor, PageSizeLimits, PageTokenCodec,
        decode_bound_page_token, decode_operation_history_cursor, paginate,
    },
    source_labels::{
        append_candidate_provenance, copy_candidate_snapshots_to_issue, replace_manager_metadata,
        snapshots_from_manager_metadata, union_snapshots,
    },
    storage::{ManagerStorage, ManagerStorageError},
};
use crate::{proto_types::manager_v1 as pb, v1};
use prost::Message;
use rusqlite::{
    Connection, OptionalExtension, Transaction, params, params_from_iter, types::Value,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

/// The manager-side entities allowed in the repository.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EntityKind {
    Candidate,
    ExtractionRun,
    IssueMapping,
    Tombstone,
}

impl EntityKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::ExtractionRun => "extraction_run",
            Self::IssueMapping => "issue_mapping",
            Self::Tombstone => "tombstone",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredEntity {
    pub kind: EntityKind,
    pub entity_id: String,
    pub revision: u64,
    pub payload: Vec<u8>,
    pub payload_schema: String,
    pub payload_digest: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Persistent per-Issue ownership-recovery admission state.  This is kept in
/// manager metadata rather than canonical Tracker state so a runner restart
/// cannot reset a readmission number or shorten a backoff window.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct OwnershipRecoveryState {
    pub issue_id: String,
    pub expected_revision: u64,
    pub readmission_number: u64,
    pub current_operation_id: Option<String>,
    pub in_flight: bool,
    pub consecutive_failures: u32,
    pub next_attempt_at_millis: Option<i64>,
    pub last_failure_class: Option<String>,
    pub stopped: bool,
    /// Evidence that caused automatic re-admission to stop. These are kept
    /// separately from the short classification so operators can repair the
    /// exact condition without reconstructing it from the current Issue.
    pub stop_basis: Option<String>,
    pub repair_condition: Option<String>,
    pub cursor_diagnostic: Option<String>,
}

/// Durable request payload for an admitted manual Issue-operation
/// resolution. The payload is replayed after a process restart through the
/// same trusted-consumer path as the original request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingResolutionRequest {
    pub resolution_operation_id: String,
    pub source_operation_id: String,
    pub issue_id: String,
    pub request_payload: Vec<u8>,
    pub request_digest: String,
}

/// Immutable source and replay snapshots retained for one manual-resolution
/// commit. The record is append-only and is addressed by the resolution
/// operation ID carried in the typed receipt.
#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IssueOperationResolutionAudit {
    pub resolution_operation_id: String,
    pub source_operation_id: String,
    pub source_generation: u64,
    pub source_state: i32,
    pub source_operation_payload: Vec<u8>,
    pub source_replay_payload: Option<Vec<u8>>,
    pub resolution_result_payload: Vec<u8>,
}

/// Result of attempting to resolve an integrity diagnosis with a CAS-bound
/// administrative repair receipt.  A stale diagnosis is not an infrastructure
/// failure: it means a newer observation or repair owns the target now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnershipDiagnosisResolution {
    Resolved,
    Stale,
    Missing,
}

/// A compact, storage-safe projection of one resident-maintenance phase.
/// Counter and gauge semantics are kept separate so a cursor passing a
/// malformed row cannot masquerade as recovery.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PhaseHealthSnapshot {
    pub phase: String,
    pub health: i32,
    pub last_started_at: Option<i64>,
    pub last_finished_at: Option<i64>,
    pub last_failure_at: Option<i64>,
    pub last_recovered_at: Option<i64>,
    pub attempt_count: u64,
    pub success_count: u64,
    pub skipped_count: u64,
    pub conflict_count: u64,
    pub diagnosis_count: u64,
    pub unconfirmed_count: u64,
    pub pending_gauge: u64,
    pub inconsistent_gauge: u64,
    pub permit_enabled: bool,
    pub gauge_stale: bool,
    pub diagnosis_refs: Vec<String>,
}

/// Values required to persist one maintenance phase projection. This keeps
/// the counter and gauge fields together at the repository boundary and
/// avoids callers relying on a fragile positional argument order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PhaseHealthUpdate {
    pub phase: String,
    pub health: i32,
    pub permit_enabled: bool,
    pub pending_gauge: u64,
    pub inconsistent_gauge: u64,
    pub diagnosis_refs: Vec<String>,
    pub gauge_stale: bool,
    pub scanned_delta: u64,
    pub recovered_delta: u64,
    pub diagnosis_delta: u64,
    pub skipped_delta: u64,
    pub conflict_delta: u64,
    pub unconfirmed_delta: u64,
}

/// Point-in-time gauges used by resident maintenance health.  These are
/// intentionally recomputed from durable rows instead of using a boolean
/// "some error occurred" flag, so operators can distinguish one blocked
/// record from a saturated queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaintenanceHealthGauges {
    pub pending: u64,
    pub inconsistent: u64,
}

/// One row considered by the capability recovery scanner.  The operation
/// payload is optional because a corrupt or missing payload must not abort the
/// bounded scan; the scanner records the row-level error and advances its
/// cursor so later operations can still be reconciled.
#[derive(Clone, Debug)]
pub(crate) struct CapabilityRecoveryCandidate {
    pub operation_id: String,
    pub attempt_state: String,
    pub operation: Option<pb::DurableOperation>,
    pub error: Option<String>,
}

#[derive(Debug, Error)]
pub(crate) enum RepositoryError {
    #[error(transparent)]
    Storage(#[from] ManagerStorageError),
    #[error("manager repository database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("entity ID must not be empty")]
    EmptyEntityId,
    #[error("payload schema must not be empty")]
    EmptyPayloadSchema,
    #[error("payload is not permitted in manager storage: {0}")]
    ForbiddenPayload(String),
    #[error("entity revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("operation generation conflict: expected {expected}, actual {actual}")]
    GenerationConflict { expected: u64, actual: u64 },
    #[error("entity revision overflow")]
    RevisionOverflow,
    #[error("operation ID must not be empty")]
    EmptyOperationId,
    #[error("operation and mutation IDs do not match")]
    OperationMismatch,
    #[error(
        "ownership recovery projection for issue {issue_id} (operation {operation_id}) was not applied: {reason}"
    )]
    OwnershipProjectionNotApplied {
        issue_id: String,
        operation_id: String,
        reason: String,
    },
    #[error("idempotency scope conflicts with an existing operation")]
    IdempotencyConflict,
    #[error("operation already exists with a different request digest")]
    OperationConflict,
    #[error("outbox operation ID does not match the durable operation")]
    OutboxOperationMismatch,
    #[error("outbox operation is fenced by a pending resolution")]
    OutboxResolutionFenced,
    #[error("candidate auto-registration is already in flight")]
    CandidateAutoRegistrationInFlight,
    #[error("protobuf serialization failed: {0}")]
    Serialization(String),
    #[error("backend request snapshot is invalid: {0}")]
    RequestSnapshot(String),
}

/// A mutable view over the single-writer manager storage boundary.
pub(crate) struct ManagerRepository<'a> {
    storage: &'a mut ManagerStorage,
}

/// Durable execution-evidence states for lease-capability calls.  These
/// records deliberately contain only redacted request fingerprints; an
/// ephemeral lease handle must never cross the manager storage boundary.
pub(crate) const CAPABILITY_ATTEMPT_PREPARED: &str = "PREPARED";
pub(crate) const CAPABILITY_ATTEMPT_SENDING: &str = "SENDING";
pub(crate) const CAPABILITY_ATTEMPT_SENT_UNKNOWN: &str = "SENT_UNKNOWN";
pub(crate) const CAPABILITY_ATTEMPT_RESOLVED: &str = "RESOLVED";

/// All values needed to durably admit a candidate acceptance.  The candidate
/// remains PROPOSED until the canonical backend marker is confirmed.
pub(crate) struct PendingAcceptance<'a> {
    pub operation: &'a pb::DurableOperation,
    pub mutation: &'a pb::ManagerMutation,
    pub outbox: &'a pb::OutboxEntry,
    pub request: &'a [u8],
    pub expectation: &'a super::backend_adapter::MarkerExpectation,
    pub candidate_id: &'a str,
    pub candidate_revision: u64,
    pub history_decision: Option<ResolutionHistoryDecision<'a>>,
}

/// The confirmed acceptance commit set.  Supplying it as one value keeps the
/// gateway from accidentally publishing only one side of the decision.
pub(crate) struct ConfirmedAcceptance<'a> {
    pub operation: &'a pb::DurableOperation,
    pub outbox: &'a pb::OutboxEntry,
    pub candidate: &'a pb::Candidate,
    pub mapping: &'a pb::IssueMapping,
    pub typed_result: &'a [u8],
}

/// Gateway readiness selection metadata that a caller must bind into its
/// opaque page token before exposing a continuation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GatewayReadySelection {
    pub registration: pb::GatewayRegistration,
    pub eligibility_evaluated_at_millis: i64,
    pub issues: Vec<v1::Issue>,
}

/// Result of looking up a Gateway write idempotency key.
///
/// A successful response can only be replayed from the currently active
/// capability epoch. Older epochs deliberately surface as unconfirmed so a
/// restarted worker never mistakes an invalid capability for a live one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GatewayReplayDecision {
    New,
    /// A pending awaiting-review reservation may be completed only after the
    /// caller verifies its immutable canonical Tracker marker and history.
    AwaitingReviewRecovery,
    Replay {
        response_schema: String,
        response_snapshot: Vec<u8>,
    },
    Conflict,
    Unconfirmed,
}

/// Result of looking up an extraction apply idempotency key.
///
/// Unlike Gateway capability replay, extraction applies are durable across a
/// process restart.  The request digest is globally unique so callers that
/// retry with a different key still observe the original outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExtractionApplyReplayDecision {
    New,
    Replay {
        outcome: i32,
        run_id: Option<String>,
        skip_reason: Option<i32>,
    },
    Conflict,
}

/// Complete persistence input for one extraction apply transaction.
/// Grouping these values prevents replay identity and its audit projections
/// from being passed independently.
pub(crate) struct ExtractionApplyPersistence<'a> {
    pub items: Vec<EntityInput>,
    pub extraction_identities: &'a [ExtractionCandidateIdentity],
    pub run_id_for_suppressions: &'a str,
    pub suppressions: &'a [ExtractionSuppression],
    pub idempotency_key: &'a str,
    pub request_digest: &'a str,
    pub outcome: i32,
    pub run_id: Option<&'a str>,
    pub skip_reason: Option<i32>,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolutionSagaDecision {
    ConfirmAssociatedIssue = 1,
    CancelAssociatedIssue = 2,
    MergeIssue = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PostDispatchPlan {
    Acceptance,
    ResolutionSaga,
}

const EXTRACTION_AUTO_REGISTRATION_PENDING: i32 = 1;
const EXTRACTION_AUTO_REGISTRATION_COMMITTED: i32 = 2;
const EXTRACTION_AUTO_REGISTRATION_FAILED: i32 = 3;

/// Durable manager-side completion plan for an auto-detected Issue creation.
///
/// The canonical backend write is admitted before delivery, while this link
/// keeps the Candidate and ExtractionRun consequences in the same recovery
/// domain as that write.
#[derive(Clone, Debug)]
struct ExtractionAutoRegistration {
    run_id: String,
    candidate_id: String,
    issue_id: String,
    #[allow(dead_code)]
    expected_idempotency_key: String,
}

/// Server-generated capability that binds one reserved Candidate to its
/// internal create-Issue admission without making that binding guessable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExtractionAutoRegistrationKey {
    pub candidate_id: String,
    pub issue_id: String,
    pub idempotency_key: String,
}

#[derive(Clone, Copy)]
struct ExtractionReservationIdentity<'a> {
    issue_id: &'a str,
    idempotency_key: &'a str,
}

struct BackendAdmission<'a> {
    mutation: &'a pb::ManagerMutation,
    operation: &'a pb::DurableOperation,
    outbox: &'a pb::OutboxEntry,
    request: &'a [u8],
    request_schema: &'a str,
    expectation: &'a super::backend_adapter::MarkerExpectation,
    extraction_reservation: Option<ExtractionReservationIdentity<'a>>,
}

impl ResolutionSagaDecision {
    fn from_i32(value: i32) -> Result<Self, RepositoryError> {
        match value {
            1 => Ok(Self::ConfirmAssociatedIssue),
            2 => Ok(Self::CancelAssociatedIssue),
            3 => Ok(Self::MergeIssue),
            _ => Err(RepositoryError::RequestSnapshot(
                "resolution saga decision is invalid".into(),
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct ResolutionSaga {
    candidate_id: String,
    candidate_revision: u64,
    decision: ResolutionSagaDecision,
    issue_id: String,
    args: pb::ResolveCandidateArgs,
}

struct ResolutionSagaAdmission<'a> {
    args: &'a pb::ResolveCandidateArgs,
    context: &'a pb::ManagerWriteContext,
    candidate: &'a pb::Candidate,
    decision: ResolutionSagaDecision,
    backend_method: &'a str,
    backend_args: Vec<u8>,
    issue_id: &'a str,
}

#[derive(Clone, Debug)]
struct ResolutionHistoryAudit {
    resolution_kind: &'static str,
    reason_category: pb::ResolutionReasonCategory,
    rationale: Option<String>,
}

#[derive(Clone, Copy)]
pub(crate) struct ResolutionHistoryDecision<'a> {
    kind: pb::ResolutionDecisionKind,
    candidate_id: &'a str,
    candidate_revision: u64,
}

#[derive(Debug, Error)]
pub(crate) enum GatewayReadinessError {
    #[error("gateway is not registered")]
    Unregistered,
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error("gateway eligibility evaluation failed: {0}")]
    Manager(String),
}

mod backend_outbox;
mod domain;
mod durable;
mod durable_helpers;
mod extraction;
mod gateway_adapter;
mod gateway_policy;
mod issue_operation;
mod ownership;
mod phase_health;
mod resolution;

use domain::*;
use resolution::*;

pub(crate) use domain::{
    AWAITING_REVIEW_REPLAY_METHOD, AWAITING_REVIEW_REPLAY_SCHEMA, EntityInput, candidate_issue_id,
};

impl<'a> ManagerRepository<'a> {
    pub(crate) fn new(storage: &'a mut ManagerStorage) -> Self {
        Self { storage }
    }
}

#[cfg(test)]
#[path = "repository_tests.rs"]
mod tests;
