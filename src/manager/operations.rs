//! Durable manager operation primitives.
//!
//! This module deliberately does not perform persistence or dispatch.  It owns
//! the invariants that must be shared by every storage-backed gateway: the
//! external idempotency identity, the operation state machine, immutable
//! mutation snapshots, and the classification of manager methods.

use std::{collections::HashMap, fmt};

use prost::Message;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{proto_types::manager_v1 as pb, v1};

/// The schema name used for a serialized manager mutation snapshot.
pub const MANAGER_MUTATION_SNAPSHOT_SCHEMA: &str = "issue_tracker.manager.v1.ManagerMutation";

/// Error returned when a manager operation violates a durable-operation
/// invariant.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum OperationError {
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("unknown manager method: {0}")]
    UnknownMethod(String),
    #[error("idempotency payload conflicts with an existing operation")]
    IdempotencyConflict,
    #[error("invalid durable operation state: {0}")]
    InvalidState(i32),
    #[error("invalid durable operation transition: {from} -> {to}")]
    InvalidTransition { from: String, to: String },
    #[error("claim lease handle must not be persisted in a mutation snapshot")]
    LeaseHandleInSnapshot,
    #[error("operation scope is not valid for method {method}")]
    InvalidScope { method: String },
    #[error("root operation-history searches require both time boundaries")]
    MissingHistoryTimeWindow,
    #[error("operation-history time boundaries are invalid")]
    InvalidHistoryTimeWindow,
    #[error("operation-history time window exceeds the configured maximum")]
    HistoryTimeWindowTooLarge,
    #[error("direct_scope_only requires an Issue direct target")]
    InvalidDirectScopeOnly,
    #[error("{field} must be a non-empty display-safe string")]
    InvalidDisplayText { field: &'static str },
    #[error("{field} exceeds the display-safe character limit")]
    DisplayTextTooLong { field: &'static str },
    #[error("{field} contains a control character")]
    DisplayTextControlCharacter { field: &'static str },
}

/// The northbound manager classification used to decide whether an operation
/// enters the backend outbox.  Reads are intentionally not classified as
/// mutations and therefore return an error from [`classify_method`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchKind {
    /// The entity and terminal operation are committed in manager SQLite only.
    ManagerOnly,
    /// The operation changes the canonical backend and must use the outbox.
    Backend,
}

/// Return the dispatch class for every manager mutation method.
///
/// Keeping this table closed is important: a newly added method cannot
/// accidentally bypass operation/outbox admission by falling through to a
/// default branch.
pub fn classify_method(method: &str) -> Result<DispatchKind, OperationError> {
    let kind = match method {
        "manager_create_candidate"
        | "manager_update_candidate"
        | "manager_reject_candidate"
        | "manager_supersede_candidate"
        | "manager_create_extraction_run"
        | "manager_advance_extraction_run"
        | "manager_resume_extraction_run"
        | "manager_resolve_issue_operation"
        | "manager_apply_issue_admin_action" => DispatchKind::ManagerOnly,
        "manager_resolve_candidate"
        | "manager_create_issue"
        | "manager_update_issue"
        | "manager_transition_issue"
        | "manager_claim_issue"
        | "manager_heartbeat_issue_claim"
        | "manager_release_issue_claim"
        | "manager_reconcile_expired_claim"
        | "manager_add_issue_relation"
        | "manager_remove_issue_relation"
        | "manager_add_issue_comment" => DispatchKind::Backend,
        _ => return Err(OperationError::UnknownMethod(method.to_owned())),
    };
    Ok(kind)
}

/// The entity scope stored in `DurableOperation.scope`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationScope {
    Candidate(String),
    Evidence(String),
    ExtractionRun(String),
    Issue(String),
}

impl OperationScope {
    fn validate_id(id: &str, field: &'static str) -> Result<(), OperationError> {
        if id.is_empty() {
            Err(OperationError::EmptyField { field })
        } else {
            Ok(())
        }
    }

    fn into_proto(self) -> Result<pb::durable_operation::Scope, OperationError> {
        Ok(match self {
            Self::Candidate(id) => {
                Self::validate_id(&id, "candidate_id")?;
                pb::durable_operation::Scope::CandidateId(id)
            }
            Self::Evidence(id) => {
                Self::validate_id(&id, "evidence_id")?;
                pb::durable_operation::Scope::EvidenceId(id)
            }
            Self::ExtractionRun(id) => {
                Self::validate_id(&id, "extraction_run_id")?;
                pb::durable_operation::Scope::ExtractionRunId(id)
            }
            Self::Issue(id) => {
                Self::validate_id(&id, "issue_id")?;
                pb::durable_operation::Scope::IssueId(id)
            }
        })
    }
}

/// Root-global external idempotency identity.
///
/// The canonical representation uses length-prefixed components so that
/// delimiters in caller-provided IDs cannot create scope collisions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IdempotencyScope {
    pub contract_fingerprint: String,
    pub data_root_id: String,
    pub method: String,
    pub target: String,
    pub key: String,
}

impl IdempotencyScope {
    pub fn new(
        contract_fingerprint: impl Into<String>,
        data_root_id: impl Into<String>,
        method: impl Into<String>,
        target: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self, OperationError> {
        let scope = Self {
            contract_fingerprint: contract_fingerprint.into(),
            data_root_id: data_root_id.into(),
            method: method.into(),
            target: target.into(),
            key: key.into(),
        };
        for (field, value) in [
            ("contract_fingerprint", &scope.contract_fingerprint),
            ("data_root_id", &scope.data_root_id),
            ("method", &scope.method),
            ("target", &scope.target),
            ("idempotency_key", &scope.key),
        ] {
            if value.is_empty() {
                return Err(OperationError::EmptyField { field });
            }
        }
        Ok(scope)
    }

    pub fn canonical(&self) -> String {
        fn append(buffer: &mut String, value: &str) {
            use std::fmt::Write;
            let _ = write!(buffer, "{}:", value.len());
            buffer.push_str(value);
        }

        let mut canonical = String::new();
        append(&mut canonical, &self.contract_fingerprint);
        append(&mut canonical, &self.data_root_id);
        append(&mut canonical, &self.method);
        append(&mut canonical, &self.target);
        append(&mut canonical, &self.key);
        canonical
    }
}

/// A stored external-idempotency record.  It contains only the canonical
/// digest and operation ID; request payload bytes are never retained here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotencyRecord {
    pub scope: IdempotencyScope,
    pub request_digest: String,
    pub operation_id: String,
}

/// Result of checking an external idempotency request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotencyDecision {
    New,
    Replay { operation_id: String },
}

pub fn check_idempotency(
    existing: Option<&IdempotencyRecord>,
    scope: &IdempotencyScope,
    request_digest: &str,
) -> Result<IdempotencyDecision, OperationError> {
    if request_digest.is_empty() {
        return Err(OperationError::EmptyField {
            field: "request_digest",
        });
    }
    match existing {
        None => Ok(IdempotencyDecision::New),
        Some(record) if record.scope != *scope => Ok(IdempotencyDecision::New),
        Some(record) if record.scope == *scope && record.request_digest == request_digest => {
            Ok(IdempotencyDecision::Replay {
                operation_id: record.operation_id.clone(),
            })
        }
        Some(_) => Err(OperationError::IdempotencyConflict),
    }
}

/// SHA-256 digest used for request and immutable snapshot identity.
pub fn digest_bytes(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Input required to create a durable manager operation.
#[derive(Clone, Debug)]
pub struct NewOperation {
    pub operation_id: String,
    pub scope: Option<OperationScope>,
    pub method: String,
    pub idempotency_scope: IdempotencyScope,
    pub request_digest: String,
    pub request_schema: String,
    pub downstream_operation_id: Option<String>,
    pub generation: u64,
    pub manager_sequence: u64,
    pub causal_parent: Option<String>,
    pub affects_ready: bool,
    pub sync_subject: Option<String>,
    pub affected_issue_ids: Vec<String>,
    pub actor_audit: Option<pb::ActorAudit>,
    pub contract_fingerprint: String,
    pub schema_fingerprint: String,
    pub correlation_id: String,
    pub now_millis: i64,
    pub replayable: bool,
}

impl NewOperation {
    pub fn build(self) -> Result<pb::DurableOperation, OperationError> {
        if self.operation_id.is_empty() {
            return Err(OperationError::EmptyField {
                field: "operation_id",
            });
        }
        if self.method.is_empty() {
            return Err(OperationError::EmptyField { field: "method" });
        }
        if self.request_digest.is_empty() {
            return Err(OperationError::EmptyField {
                field: "request_digest",
            });
        }
        if self.request_schema.is_empty() {
            return Err(OperationError::EmptyField {
                field: "request_schema",
            });
        }
        if self.contract_fingerprint.is_empty() {
            return Err(OperationError::EmptyField {
                field: "contract_fingerprint",
            });
        }
        if self.idempotency_scope.contract_fingerprint != self.contract_fingerprint {
            return Err(OperationError::InvalidScope {
                method: self.method,
            });
        }
        // Candidate resolution owns the northbound idempotency scope while
        // dispatching a canonical create_issue operation downstream. Keeping
        // this explicit pair preserves replay identity without making
        // arbitrary method/scope combinations valid.
        if self.idempotency_scope.method != self.method
            && !(matches!(
                self.method.as_str(),
                "manager_create_issue" | "manager_update_issue" | "manager_transition_issue"
            ) && matches!(
                self.idempotency_scope.method.as_str(),
                "manager_accept_candidate" | "manager_resolve_candidate"
            ))
        {
            return Err(OperationError::InvalidScope {
                method: self.method,
            });
        }
        if self.schema_fingerprint.is_empty() {
            return Err(OperationError::EmptyField {
                field: "schema_fingerprint",
            });
        }
        if self.correlation_id.is_empty() {
            return Err(OperationError::EmptyField {
                field: "correlation_id",
            });
        }
        if let Some(parent) = &self.causal_parent {
            if parent.is_empty() {
                return Err(OperationError::EmptyField {
                    field: "causal_parent",
                });
            }
        }
        if let Some(subject) = &self.sync_subject {
            if subject.is_empty() {
                return Err(OperationError::EmptyField {
                    field: "sync_subject",
                });
            }
        }
        let kind = classify_method(&self.method)?;
        match (kind, self.downstream_operation_id.as_deref()) {
            (DispatchKind::Backend, None | Some("")) => {
                return Err(OperationError::EmptyField {
                    field: "downstream_operation_id",
                });
            }
            // The trusted primary consumer admits administrative actions into
            // the private durable outbox.  They remain ManagerOnly for the
            // public gateway/reflection surface, but carry a deterministic
            // downstream marker ID so the outbox can reconcile a crash after
            // the Tracker commit.
            (DispatchKind::ManagerOnly, Some(_))
                if self.method == "manager_apply_issue_admin_action" => {}
            (DispatchKind::ManagerOnly, Some(_)) => {
                return Err(OperationError::InvalidScope {
                    method: self.method,
                });
            }
            _ => {}
        }
        // A lease capability is deliberately not recoverable after the
        // invocation boundary is lost.  Its redacted operation receipt is
        // durable, but it must never be used to replay the capability.
        let ephemeral_capability =
            self.request_schema == "issue_tracker.manager.v1.EphemeralLeaseCapability";
        if (self.method == "manager_claim_issue" || ephemeral_capability) == self.replayable {
            return Err(OperationError::InvalidScope {
                method: self.method,
            });
        }
        let operation_scope = self.scope.map(OperationScope::into_proto).transpose()?;
        let mut actor_audit = self.actor_audit;
        if let Some(audit) = &mut actor_audit {
            audit.reason =
                validate_display_safe_text("actor_audit.reason", audit.reason.as_deref())?;
        }
        let now = Some(pb::UtcInstant {
            unix_epoch_millis: self.now_millis,
        });
        let replayability = if self.replayable {
            v1::OperationResultReplayability::Replayable
        } else {
            v1::OperationResultReplayability::NotReplayable
        };
        let downstream = self.downstream_operation_id.unwrap_or_default();
        Ok(pb::DurableOperation {
            operation_id: self.operation_id,
            scope: operation_scope,
            method: self.method.clone(),
            idempotency_scope: self.idempotency_scope.canonical(),
            request_digest: self.request_digest,
            request_schema: self.request_schema,
            downstream_operation_id: downstream,
            state: pb::DurableOperationState::Received as i32,
            failure_class: pb::FailureClass::Unspecified as i32,
            attempt: 0,
            manager_sequence: self.manager_sequence,
            generation: self.generation,
            causal_parent: self.causal_parent,
            superseded_by: None,
            affects_ready: self.affects_ready,
            sync_subject: self.sync_subject,
            sync_subject_generation: self.generation,
            affected_issue_ids: self.affected_issue_ids,
            terminal_snapshot: None,
            resolution_required: false,
            created_at: now.clone(),
            updated_at: now,
            commit_snapshot: None,
            audit: Some(pb::ManagerOperationAudit {
                method: self.method,
                target: self.idempotency_scope.target,
                data_root_id: self.idempotency_scope.data_root_id,
                contract_fingerprint: self.contract_fingerprint,
                schema_fingerprint: self.schema_fingerprint,
                idempotency_key: self.idempotency_scope.key,
                actor_audit,
                correlation_id: self.correlation_id,
                recorded_at: Some(pb::UtcInstant {
                    unix_epoch_millis: self.now_millis,
                }),
            }),
            commit_durability: v1::OperationCommitDurability::Durable as i32,
            result_replayability: replayability as i32,
            diagnosis: None,
        })
    }
}

/// Validate and apply one operation state transition.
pub fn transition_operation(
    operation: &mut pb::DurableOperation,
    target: pb::DurableOperationState,
    failure_class: Option<pb::FailureClass>,
    now_millis: i64,
) -> Result<(), OperationError> {
    let current = pb::DurableOperationState::try_from(operation.state)
        .map_err(|_| OperationError::InvalidState(operation.state))?;
    let allowed = match current {
        pb::DurableOperationState::Received => matches!(
            target,
            pb::DurableOperationState::Pending
                | pb::DurableOperationState::Failed
                | pb::DurableOperationState::Unconfirmed
        ),
        pb::DurableOperationState::Pending => matches!(
            target,
            pb::DurableOperationState::Committed
                | pb::DurableOperationState::Failed
                | pb::DurableOperationState::Unconfirmed
        ),
        pb::DurableOperationState::Failed => {
            pb::FailureClass::try_from(operation.failure_class).ok()
                == Some(pb::FailureClass::Retryable)
                && failure_class.is_none_or(|class| class == pb::FailureClass::Retryable)
                && target == pb::DurableOperationState::Received
        }
        pb::DurableOperationState::Unconfirmed => matches!(
            target,
            pb::DurableOperationState::Committed
                | pb::DurableOperationState::Failed
                | pb::DurableOperationState::ManualResolution
        ),
        pb::DurableOperationState::ManualResolution => {
            target == pb::DurableOperationState::Received
        }
        pb::DurableOperationState::Committed => false,
        // Human arbitration is terminal and can only be replaced by a new
        // operation against the current Issue; it is never retried in-place.
        pb::DurableOperationState::Arbitrated => false,
        pb::DurableOperationState::Unspecified => false,
    };
    if !allowed {
        return Err(OperationError::InvalidTransition {
            from: current.as_str_name().to_owned(),
            to: target.as_str_name().to_owned(),
        });
    }
    if target == pb::DurableOperationState::Failed {
        operation.failure_class = failure_class.unwrap_or(pb::FailureClass::Terminal) as i32;
    } else if target == pb::DurableOperationState::Received {
        operation.failure_class = pb::FailureClass::Unspecified as i32;
        operation.attempt = operation.attempt.saturating_add(1);
    }
    operation.state = target as i32;
    operation.updated_at = Some(pb::UtcInstant {
        unix_epoch_millis: now_millis,
    });
    if target == pb::DurableOperationState::ManualResolution {
        operation.resolution_required = true;
    }
    Ok(())
}

/// A canonical, redacted manager mutation snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationSnapshot {
    pub snapshot: pb::Snapshot,
    pub digest: String,
}

impl MutationSnapshot {
    pub fn from_mutation(mutation: &pb::ManagerMutation) -> Result<Self, OperationError> {
        let mut redacted = mutation.clone();
        if let Some(pb::manager_mutation::Result::ClaimIssue(claim)) = &mut redacted.result {
            // The claim handle is an ephemeral capability. It is intentionally
            // removed before bytes are hashed or handed to storage.
            claim.lease_handle.clear();
        }
        let payload = redacted.encode_to_vec();
        Ok(Self {
            digest: digest_bytes(&payload),
            snapshot: pb::Snapshot {
                payload,
                schema: MANAGER_MUTATION_SNAPSHOT_SCHEMA.to_owned(),
            },
        })
    }

    pub fn commit_record(
        &self,
        operation: &pb::DurableOperation,
        manager_sequence: u64,
        generation: u64,
        request_digest: impl Into<String>,
        committed_at_millis: i64,
    ) -> Result<pb::ManagerCommitSnapshot, OperationError> {
        if operation.operation_id.is_empty() {
            return Err(OperationError::EmptyField {
                field: "operation_id",
            });
        }
        Ok(pb::ManagerCommitSnapshot {
            snapshot_id: format!("snapshot-{}", self.digest),
            operation_id: operation.operation_id.clone(),
            manager_sequence,
            generation,
            request_digest: request_digest.into(),
            snapshot_digest: self.digest.clone(),
            snapshot: Some(self.snapshot.clone()),
            committed_at: Some(pb::UtcInstant {
                unix_epoch_millis: committed_at_millis,
            }),
        })
    }
}

/// Supplementary resolution audit data retained by the operation-history
/// projection. `DurableOperation` intentionally contains no request body, so
/// the decision-specific audit details must be supplied by the projection
/// rather than decoded from a private snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationHistoryResolution {
    /// Reject and cancel decisions expose their structured reason and optional
    /// explanation, but do not require a free-text actor reason.
    RejectOrCancel {
        operation_id: String,
        reason_category: pb::ResolutionReasonCategory,
        rationale: Option<String>,
    },
    /// A human review permits a create/confirmation without a verified
    /// manifest and must leave an explanation in the actor audit.
    HumanReview { operation_id: String },
}

/// Immutable candidate-resolution decision metadata retained separately from
/// the durable operation payload.  The public history projection exposes only
/// this closed classification and the candidate revision observed at admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationHistoryDecision {
    pub operation_id: String,
    pub kind: pb::ResolutionDecisionKind,
    pub candidate_id: String,
    pub candidate_revision: u64,
}

impl OperationHistoryResolution {
    fn operation_id(&self) -> &str {
        match self {
            Self::RejectOrCancel { operation_id, .. } | Self::HumanReview { operation_id } => {
                operation_id
            }
        }
    }
}

/// Validates and normalizes a string that is safe to display in operation
/// history. The caller must ensure that the value does not contain semantic
/// secrets such as raw source or prompts before it reaches this boundary.
pub fn validate_display_safe_text(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<String>, OperationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(OperationError::InvalidDisplayText { field });
    }
    if normalized.chars().count() > 1_024 {
        return Err(OperationError::DisplayTextTooLong { field });
    }
    if normalized.chars().any(char::is_control) {
        return Err(OperationError::DisplayTextControlCharacter { field });
    }
    Ok(Some(normalized.to_owned()))
}

/// Validates the operation-history filter before its SQL projection is read.
/// A root-wide traversal is deliberately bounded; direct-target traversals do
/// not require a time window because they are naturally limited by one entity.
pub fn validate_operation_history_filter(
    filter: &pb::OperationHistoryFilter,
    root_max_window_millis: i64,
) -> Result<(), OperationError> {
    if root_max_window_millis < 0 {
        return Err(OperationError::InvalidHistoryTimeWindow);
    }

    if let Some(target) = &filter.direct_target {
        let id = match target {
            pb::operation_history_filter::DirectTarget::CandidateId(id)
            | pb::operation_history_filter::DirectTarget::EvidenceId(id)
            | pb::operation_history_filter::DirectTarget::ExtractionRunId(id)
            | pb::operation_history_filter::DirectTarget::IssueId(id) => id,
        };
        if id.is_empty() {
            return Err(OperationError::EmptyField {
                field: "direct_target",
            });
        }
    }
    if filter.direct_scope_only
        && !matches!(
            filter.direct_target.as_ref(),
            Some(pb::operation_history_filter::DirectTarget::IssueId(_))
        )
    {
        return Err(OperationError::InvalidDirectScopeOnly);
    }
    if filter.method.as_deref().is_some_and(str::is_empty) {
        return Err(OperationError::EmptyField { field: "method" });
    }

    for state in &filter.states {
        match pb::DurableOperationState::try_from(*state) {
            Ok(pb::DurableOperationState::Unspecified) | Err(_) => {
                return Err(OperationError::InvalidState(*state));
            }
            Ok(_) => {}
        }
    }
    for decision_kind in &filter.decision_kinds {
        match pb::ResolutionDecisionKind::try_from(*decision_kind) {
            Ok(pb::ResolutionDecisionKind::Unspecified) | Err(_) => {
                return Err(OperationError::InvalidState(*decision_kind));
            }
            Ok(_) => {}
        }
    }

    let from = filter
        .created_at_from
        .as_ref()
        .map(|instant| instant.unix_epoch_millis);
    let to = filter
        .created_at_to
        .as_ref()
        .map(|instant| instant.unix_epoch_millis);
    if let (Some(from), Some(to)) = (from, to) {
        if from > to {
            return Err(OperationError::InvalidHistoryTimeWindow);
        }
        if filter.direct_target.is_none() && to.saturating_sub(from) > root_max_window_millis {
            return Err(OperationError::HistoryTimeWindowTooLarge);
        }
    } else if filter.direct_target.is_none() {
        return Err(OperationError::MissingHistoryTimeWindow);
    }
    Ok(())
}

/// Builds redacted, stable-order public history entries from durable
/// operations and a separately projected resolution-audit record.
///
/// The projection supplies only rationale for reject/cancel resolutions. It
/// is never inferred from operation snapshots, which can contain private
/// request or result data and must not be decoded for a read response.
pub fn build_operation_history_entries<'a>(
    operations: impl IntoIterator<Item = &'a pb::DurableOperation>,
    filter: &pb::OperationHistoryFilter,
    root_max_window_millis: i64,
    resolutions: &[OperationHistoryResolution],
) -> Result<Vec<pb::OperationHistoryEntry>, OperationError> {
    build_operation_history_entries_with_decisions(
        operations,
        filter,
        root_max_window_millis,
        resolutions,
        &[],
    )
}

/// Builds history entries with the optional resolution-decision projection.
/// Kept separate from resolution rationale because create, confirm, and merge
/// have a decision but intentionally no rationale category.
pub fn build_operation_history_entries_with_decisions<'a>(
    operations: impl IntoIterator<Item = &'a pb::DurableOperation>,
    filter: &pb::OperationHistoryFilter,
    root_max_window_millis: i64,
    resolutions: &[OperationHistoryResolution],
    decisions: &[OperationHistoryDecision],
) -> Result<Vec<pb::OperationHistoryEntry>, OperationError> {
    validate_operation_history_filter(filter, root_max_window_millis)?;
    let resolutions_by_operation = resolutions.iter().fold(
        HashMap::with_capacity(resolutions.len()),
        |mut index, resolution| {
            index.entry(resolution.operation_id()).or_insert(resolution);
            index
        },
    );
    let decisions_by_operation = decisions.iter().fold(
        HashMap::with_capacity(decisions.len()),
        |mut index, decision| {
            index
                .entry(decision.operation_id.as_str())
                .or_insert(decision);
            index
        },
    );

    let mut entries = operations
        .into_iter()
        .filter(|operation| {
            operation_matches_history_filter(operation, filter)
                && decision_matches_history_filter(
                    decisions_by_operation
                        .get(operation.operation_id.as_str())
                        .copied(),
                    filter,
                )
        })
        .map(|operation| {
            operation_history_entry(
                operation,
                resolutions_by_operation
                    .get(operation.operation_id.as_str())
                    .copied(),
                decisions_by_operation
                    .get(operation.operation_id.as_str())
                    .copied(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_unstable_by(|left, right| {
        right
            .created_at
            .as_ref()
            .map(|instant| instant.unix_epoch_millis)
            .cmp(
                &left
                    .created_at
                    .as_ref()
                    .map(|instant| instant.unix_epoch_millis),
            )
            .then_with(|| right.operation_id.cmp(&left.operation_id))
    });
    Ok(entries)
}

fn decision_matches_history_filter(
    decision: Option<&OperationHistoryDecision>,
    filter: &pb::OperationHistoryFilter,
) -> bool {
    filter.decision_kinds.is_empty()
        || decision.is_some_and(|decision| filter.decision_kinds.contains(&(decision.kind as i32)))
}

fn operation_matches_history_filter(
    operation: &pb::DurableOperation,
    filter: &pb::OperationHistoryFilter,
) -> bool {
    if filter
        .method
        .as_deref()
        .is_some_and(|method| method != operation.method)
    {
        return false;
    }
    if !filter.states.is_empty() && !filter.states.contains(&operation.state) {
        return false;
    }
    let Some(created_at) = operation
        .created_at
        .as_ref()
        .map(|instant| instant.unix_epoch_millis)
    else {
        return false;
    };
    if filter
        .created_at_from
        .as_ref()
        .is_some_and(|from| created_at < from.unix_epoch_millis)
        || filter
            .created_at_to
            .as_ref()
            .is_some_and(|to| created_at > to.unix_epoch_millis)
    {
        return false;
    }
    match &filter.direct_target {
        None => true,
        Some(pb::operation_history_filter::DirectTarget::CandidateId(candidate_id)) => {
            matches!(operation.scope.as_ref(), Some(pb::durable_operation::Scope::CandidateId(id)) if id == candidate_id)
        }
        Some(pb::operation_history_filter::DirectTarget::EvidenceId(evidence_id)) => {
            matches!(operation.scope.as_ref(), Some(pb::durable_operation::Scope::EvidenceId(id)) if id == evidence_id)
        }
        Some(pb::operation_history_filter::DirectTarget::ExtractionRunId(extraction_run_id)) => {
            matches!(operation.scope.as_ref(), Some(pb::durable_operation::Scope::ExtractionRunId(id)) if id == extraction_run_id)
        }
        Some(pb::operation_history_filter::DirectTarget::IssueId(issue_id)) => {
            matches!(operation.scope.as_ref(), Some(pb::durable_operation::Scope::IssueId(id)) if id == issue_id)
                || (!filter.direct_scope_only
                    && operation.affected_issue_ids.iter().any(|id| id == issue_id))
        }
    }
}

fn operation_history_entry(
    operation: &pb::DurableOperation,
    resolution: Option<&OperationHistoryResolution>,
    decision: Option<&OperationHistoryDecision>,
) -> Result<pb::OperationHistoryEntry, OperationError> {
    let state = pb::DurableOperationState::try_from(operation.state)
        .map_err(|_| OperationError::InvalidState(operation.state))?;
    let mut actor_audit = operation
        .audit
        .as_ref()
        .and_then(|audit| audit.actor_audit.clone());
    if let Some(audit) = &mut actor_audit {
        audit.reason = validate_display_safe_text("actor_audit.reason", audit.reason.as_deref())?;
    }
    let (resolution_reason_category, resolution_rationale) = match resolution {
        Some(OperationHistoryResolution::RejectOrCancel {
            reason_category,
            rationale,
            ..
        }) => {
            let category = *reason_category;
            if category == pb::ResolutionReasonCategory::Unspecified {
                return Err(OperationError::InvalidDisplayText {
                    field: "resolution_reason_category",
                });
            }
            (
                Some(category as i32),
                validate_display_safe_text("resolution_rationale", rationale.as_deref())?,
            )
        }
        Some(OperationHistoryResolution::HumanReview { .. }) => {
            if actor_audit
                .as_ref()
                .and_then(|audit| audit.reason.as_deref())
                .is_none()
            {
                return Err(OperationError::InvalidDisplayText {
                    field: "actor_audit.reason",
                });
            }
            (None, None)
        }
        None => (None, None),
    };
    let direct_target = operation.scope.as_ref().map(|scope| match scope {
        pb::durable_operation::Scope::CandidateId(id) => {
            pb::operation_history_entry::DirectTarget::CandidateId(id.clone())
        }
        pb::durable_operation::Scope::EvidenceId(id) => {
            pb::operation_history_entry::DirectTarget::EvidenceId(id.clone())
        }
        pb::durable_operation::Scope::ExtractionRunId(id) => {
            pb::operation_history_entry::DirectTarget::ExtractionRunId(id.clone())
        }
        pb::durable_operation::Scope::IssueId(id) => {
            pb::operation_history_entry::DirectTarget::IssueId(id.clone())
        }
    });
    let audit = operation.audit.as_ref();
    let (resolution_decision_kind, resolution_candidate_revision) = match decision {
        Some(decision) if decision.kind != pb::ResolutionDecisionKind::Unspecified => {
            match operation.scope.as_ref() {
                Some(pb::durable_operation::Scope::CandidateId(candidate_id))
                    if candidate_id == &decision.candidate_id =>
                {
                    (
                        Some(decision.kind as i32),
                        Some(decision.candidate_revision),
                    )
                }
                _ => {
                    return Err(OperationError::InvalidDisplayText {
                        field: "resolution_decision_candidate",
                    });
                }
            }
        }
        Some(_) => {
            return Err(OperationError::InvalidDisplayText {
                field: "resolution_decision_kind",
            });
        }
        None => (None, None),
    };
    Ok(pb::OperationHistoryEntry {
        operation_id: operation.operation_id.clone(),
        method: operation.method.clone(),
        state: state as i32,
        direct_target,
        created_at: operation.created_at.clone(),
        updated_at: operation.updated_at.clone(),
        actor_audit,
        correlation_id: audit.map_or_else(String::new, |audit| audit.correlation_id.clone()),
        causal_parent: operation.causal_parent.clone(),
        affected_issue_ids: operation.affected_issue_ids.clone(),
        resolution_required: operation.resolution_required,
        commit_durability: operation.commit_durability,
        result_replayability: operation.result_replayability,
        resolution_reason_category,
        resolution_rationale,
        resolution_decision_kind,
        candidate_revision_at_decision: resolution_candidate_revision,
    })
}

/// Generate a server-owned operation ID.  The caller persists it before
/// dispatch; no caller-supplied operation ID is accepted by the manager API.
pub fn new_operation_id() -> String {
    format!("op-{}", Uuid::new_v4())
}

impl fmt::Display for DispatchKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ManagerOnly => f.write_str("manager-only"),
            Self::Backend => f.write_str("backend"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(method: &str, key: &str) -> IdempotencyScope {
        IdempotencyScope::new(
            "contract",
            "root",
            method,
            if method == "manager_create_issue" {
                "issue:c1"
            } else {
                "candidate:c1"
            },
            key,
        )
        .expect("valid scope")
    }

    fn operation_input(method: &str) -> NewOperation {
        NewOperation {
            operation_id: "op-1".into(),
            scope: Some(if method == "manager_create_issue" {
                OperationScope::Issue("c1".into())
            } else {
                OperationScope::Candidate("c1".into())
            }),
            method: method.into(),
            idempotency_scope: scope(method, "key-1"),
            request_digest: "digest".into(),
            request_schema: "Request".into(),
            downstream_operation_id: (method.starts_with("manager_create_issue"))
                .then(|| "downstream-1".into()),
            generation: 1,
            manager_sequence: 1,
            causal_parent: None,
            affects_ready: false,
            sync_subject: None,
            affected_issue_ids: vec![],
            actor_audit: None,
            contract_fingerprint: "contract".into(),
            schema_fingerprint: "schema".into(),
            correlation_id: "corr".into(),
            now_millis: 100,
            replayable: true,
        }
    }

    #[test]
    fn operation_build_normalizes_and_rejects_display_audit_reasons() {
        let mut input = operation_input("manager_create_issue");
        input.actor_audit = Some(pb::ActorAudit {
            actor_id: "human".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("  reviewed manually  ".into()),
            ..Default::default()
        });
        let operation = input.build().expect("display-safe audit reason");
        assert_eq!(
            operation
                .audit
                .and_then(|audit| audit.actor_audit)
                .and_then(|audit| audit.reason),
            Some("reviewed manually".into())
        );

        let mut invalid = operation_input("manager_create_issue");
        invalid.actor_audit = Some(pb::ActorAudit {
            actor_id: "human".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("invalid\u{7}".into()),
            ..Default::default()
        });
        assert_eq!(
            invalid.build(),
            Err(OperationError::DisplayTextControlCharacter {
                field: "actor_audit.reason"
            })
        );
    }

    #[test]
    fn idempotency_scope_is_length_prefixed_and_detects_replay_or_conflict() {
        let first = IdempotencyRecord {
            scope: scope("manager_create_candidate", "key-1"),
            request_digest: "digest".into(),
            operation_id: "op-1".into(),
        };
        assert_eq!(
            check_idempotency(
                Some(&first),
                &scope("manager_create_candidate", "key-1"),
                "digest",
            ),
            Ok(IdempotencyDecision::Replay {
                operation_id: "op-1".into()
            })
        );
        assert_eq!(
            check_idempotency(
                Some(&first),
                &scope("manager_create_candidate", "key-1"),
                "other",
            ),
            Err(OperationError::IdempotencyConflict)
        );
        let a = IdempotencyScope::new("contract", "ab", "c", "d", "e").unwrap();
        let b = IdempotencyScope::new("contract", "a", "bc", "d", "e").unwrap();
        assert_ne!(a.canonical(), b.canonical());
    }

    #[test]
    fn operation_builds_backend_and_manager_only_records_with_expected_replayability() {
        let manager = operation_input("manager_create_candidate").build().unwrap();
        assert_eq!(manager.state, pb::DurableOperationState::Received as i32);
        assert!(manager.downstream_operation_id.is_empty());
        assert_eq!(
            manager.result_replayability,
            v1::OperationResultReplayability::Replayable as i32
        );
        let backend = operation_input("manager_create_issue").build().unwrap();
        assert_eq!(backend.downstream_operation_id, "downstream-1");
        assert_eq!(
            classify_method("manager_accept_candidate"),
            Err(OperationError::UnknownMethod(
                "manager_accept_candidate".into()
            ))
        );
    }

    #[test]
    fn unknown_methods_and_missing_downstream_identity_are_rejected() {
        assert_eq!(
            classify_method("manager_get_candidate"),
            Err(OperationError::UnknownMethod(
                "manager_get_candidate".into()
            ))
        );
        assert_eq!(
            operation_input("manager_create_issue")
                .into_with_downstream(None)
                .build()
                .unwrap_err(),
            OperationError::EmptyField {
                field: "downstream_operation_id"
            }
        );
    }

    #[test]
    fn operation_state_machine_distinguishes_retry_and_unknown_commit() {
        let mut operation = operation_input("manager_create_issue").build().unwrap();
        transition_operation(
            &mut operation,
            pb::DurableOperationState::Pending,
            None,
            101,
        )
        .unwrap();
        transition_operation(
            &mut operation,
            pb::DurableOperationState::Unconfirmed,
            None,
            102,
        )
        .unwrap();
        transition_operation(
            &mut operation,
            pb::DurableOperationState::ManualResolution,
            None,
            103,
        )
        .unwrap();
        assert!(
            transition_operation(
                &mut operation,
                pb::DurableOperationState::Committed,
                None,
                104,
            )
            .is_err()
        );

        let mut failed = operation_input("manager_create_issue").build().unwrap();
        transition_operation(
            &mut failed,
            pb::DurableOperationState::Failed,
            Some(pb::FailureClass::Retryable),
            105,
        )
        .unwrap();
        transition_operation(
            &mut failed,
            pb::DurableOperationState::Received,
            Some(pb::FailureClass::Retryable),
            106,
        )
        .unwrap();
        assert_eq!(failed.attempt, 1);

        transition_operation(&mut failed, pb::DurableOperationState::Pending, None, 107).unwrap();
        transition_operation(
            &mut failed,
            pb::DurableOperationState::Unconfirmed,
            None,
            108,
        )
        .unwrap();
        transition_operation(
            &mut failed,
            pb::DurableOperationState::ManualResolution,
            None,
            109,
        )
        .unwrap();
        transition_operation(&mut failed, pb::DurableOperationState::Received, None, 110).unwrap();
    }

    #[test]
    fn mutation_snapshot_redacts_ephemeral_claim_handle_and_is_digest_stable() {
        let mutation = pb::ManagerMutation {
            operation: None,
            entity: None,
            entity_revisions: vec![],
            result: Some(pb::manager_mutation::Result::ClaimIssue(
                pb::ManagerClaimIssueSuccess {
                    current: None,
                    lease_handle: b"secret-handle".to_vec(),
                },
            )),
        };
        let snapshot = MutationSnapshot::from_mutation(&mutation).unwrap();
        let decoded = pb::ManagerMutation::decode(snapshot.snapshot.payload.as_slice()).unwrap();
        match decoded.result {
            Some(pb::manager_mutation::Result::ClaimIssue(claim)) => {
                assert!(claim.lease_handle.is_empty());
            }
            _ => panic!("claim result must remain typed"),
        }
        assert!(!snapshot.digest.is_empty());
        assert!(
            !snapshot
                .snapshot
                .payload
                .windows(13)
                .any(|w| w == b"secret-handle")
        );
    }

    fn history_operation(
        operation_id: &str,
        created_at: i64,
        scope: pb::durable_operation::Scope,
    ) -> pb::DurableOperation {
        pb::DurableOperation {
            operation_id: operation_id.into(),
            scope: Some(scope),
            method: "manager_resolve_candidate".into(),
            state: pb::DurableOperationState::Committed as i32,
            created_at: Some(pb::UtcInstant {
                unix_epoch_millis: created_at,
            }),
            updated_at: Some(pb::UtcInstant {
                unix_epoch_millis: created_at + 1,
            }),
            affected_issue_ids: vec!["issue-related".into()],
            audit: Some(pb::ManagerOperationAudit {
                correlation_id: "correlation-1".into(),
                actor_audit: Some(pb::ActorAudit {
                    actor_id: "reviewer-1".into(),
                    reason: Some("  reviewed manually  ".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            request_digest: "request-digest-must-not-leak".into(),
            idempotency_scope: "idempotency-key-must-not-leak".into(),
            terminal_snapshot: Some(pb::durable_operation::TerminalSnapshot::ResultSnapshot(
                pb::Snapshot {
                    payload: b"raw-source-must-not-leak".to_vec(),
                    schema: "private".into(),
                },
            )),
            ..Default::default()
        }
    }

    #[test]
    fn operation_history_filters_inclusively_and_sorts_by_time_then_id() {
        let newest_b = history_operation(
            "op-b",
            20,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        let newest_a = history_operation(
            "op-a",
            20,
            pb::durable_operation::Scope::IssueId("issue-1".into()),
        );
        let older = history_operation(
            "op-c",
            10,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        let filter = pb::OperationHistoryFilter {
            direct_target: Some(pb::operation_history_filter::DirectTarget::IssueId(
                "issue-related".into(),
            )),
            method: Some("manager_resolve_candidate".into()),
            states: vec![pb::DurableOperationState::Committed as i32],
            created_at_from: Some(pb::UtcInstant {
                unix_epoch_millis: 10,
            }),
            created_at_to: Some(pb::UtcInstant {
                unix_epoch_millis: 20,
            }),
            decision_kinds: vec![],
            direct_scope_only: false,
        };

        let entries =
            build_operation_history_entries([&older, &newest_a, &newest_b], &filter, 90, &[])
                .expect("valid history query");

        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.operation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["op-b", "op-a", "op-c"]
        );
        assert!(matches!(
            entries[0].direct_target,
            Some(pb::operation_history_entry::DirectTarget::CandidateId(ref id)) if id == "candidate-1"
        ));
        assert_eq!(
            entries[0]
                .actor_audit
                .as_ref()
                .and_then(|audit| audit.reason.as_deref()),
            Some("reviewed manually")
        );
    }

    #[test]
    fn operation_history_direct_scope_only_requires_issue_and_excludes_affected_only() {
        let direct = history_operation(
            "direct",
            10,
            pb::durable_operation::Scope::IssueId("issue-1".into()),
        );
        let mut affected = history_operation(
            "affected",
            11,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        affected.affected_issue_ids = vec!["issue-1".into()];
        let filter = pb::OperationHistoryFilter {
            direct_target: Some(pb::operation_history_filter::DirectTarget::IssueId(
                "issue-1".into(),
            )),
            direct_scope_only: true,
            ..Default::default()
        };
        let entries = build_operation_history_entries([&direct, &affected], &filter, 90, &[])
            .expect("valid direct-only filter");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation_id, "direct");

        let invalid = pb::OperationHistoryFilter {
            direct_scope_only: true,
            ..Default::default()
        };
        assert!(matches!(
            validate_operation_history_filter(&invalid, 90),
            Err(OperationError::InvalidDirectScopeOnly)
        ));
    }

    #[test]
    fn root_history_requires_a_bounded_inclusive_time_window() {
        let filter = pb::OperationHistoryFilter::default();
        assert_eq!(
            validate_operation_history_filter(&filter, 90),
            Err(OperationError::MissingHistoryTimeWindow)
        );
        let inverted = pb::OperationHistoryFilter {
            created_at_from: Some(pb::UtcInstant {
                unix_epoch_millis: 11,
            }),
            created_at_to: Some(pb::UtcInstant {
                unix_epoch_millis: 10,
            }),
            ..Default::default()
        };
        assert_eq!(
            validate_operation_history_filter(&inverted, 90),
            Err(OperationError::InvalidHistoryTimeWindow)
        );
        let excessive = pb::OperationHistoryFilter {
            created_at_from: Some(pb::UtcInstant {
                unix_epoch_millis: 0,
            }),
            created_at_to: Some(pb::UtcInstant {
                unix_epoch_millis: 91,
            }),
            ..Default::default()
        };
        assert_eq!(
            validate_operation_history_filter(&excessive, 90),
            Err(OperationError::HistoryTimeWindowTooLarge)
        );
        assert_eq!(
            validate_operation_history_filter(
                &pb::OperationHistoryFilter {
                    direct_target: Some(pb::operation_history_filter::DirectTarget::CandidateId(
                        String::new()
                    ),),
                    ..Default::default()
                },
                90,
            ),
            Err(OperationError::EmptyField {
                field: "direct_target"
            })
        );
    }

    #[test]
    fn operation_history_rejects_unsafe_text_and_never_serializes_private_operation_fields() {
        assert_eq!(
            validate_display_safe_text("rationale", Some(" \n ")),
            Err(OperationError::InvalidDisplayText { field: "rationale" })
        );
        assert_eq!(
            validate_display_safe_text("actor_audit.reason", Some(&"x".repeat(1025))),
            Err(OperationError::DisplayTextTooLong {
                field: "actor_audit.reason"
            })
        );
        assert_eq!(
            validate_display_safe_text("rationale", Some("safe\u{0007}")),
            Err(OperationError::DisplayTextControlCharacter { field: "rationale" })
        );

        let operation = history_operation(
            "op-1",
            10,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        let entries = build_operation_history_entries(
            [&operation],
            &pb::OperationHistoryFilter {
                direct_target: Some(pb::operation_history_filter::DirectTarget::CandidateId(
                    "candidate-1".into(),
                )),
                ..Default::default()
            },
            90,
            &[],
        )
        .expect("valid history entry");
        let encoded = entries[0].encode_to_vec();
        for private in [
            b"request-digest-must-not-leak".as_slice(),
            b"idempotency-key-must-not-leak".as_slice(),
            b"raw-source-must-not-leak".as_slice(),
        ] {
            assert!(
                !encoded
                    .windows(private.len())
                    .any(|window| window == private)
            );
        }
    }

    #[test]
    fn history_keeps_rejection_rationale_separate_and_requires_human_review_reason() {
        let operation = history_operation(
            "op-resolution",
            10,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        let filter = pb::OperationHistoryFilter {
            direct_target: Some(pb::operation_history_filter::DirectTarget::CandidateId(
                "candidate-1".into(),
            )),
            ..Default::default()
        };
        let entries = build_operation_history_entries(
            [&operation],
            &filter,
            90,
            &[OperationHistoryResolution::RejectOrCancel {
                operation_id: "op-resolution".into(),
                reason_category: pb::ResolutionReasonCategory::NotActionable,
                rationale: Some("  duplicate report  ".into()),
            }],
        )
        .expect("safe rationale");
        assert_eq!(
            entries[0].resolution_reason_category,
            Some(pb::ResolutionReasonCategory::NotActionable as i32)
        );
        assert_eq!(
            entries[0].resolution_rationale.as_deref(),
            Some("duplicate report")
        );

        let mut without_reason = operation.clone();
        without_reason
            .audit
            .as_mut()
            .expect("test audit")
            .actor_audit
            .as_mut()
            .expect("test actor audit")
            .reason = None;
        assert_eq!(
            build_operation_history_entries(
                [&without_reason],
                &filter,
                90,
                &[OperationHistoryResolution::HumanReview {
                    operation_id: "op-resolution".into(),
                }],
            ),
            Err(OperationError::InvalidDisplayText {
                field: "actor_audit.reason"
            })
        );
    }

    #[test]
    fn history_uses_the_first_resolution_for_each_operation() {
        let first = history_operation(
            "op-first",
            20,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        let second = history_operation(
            "op-second",
            10,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        let filter = pb::OperationHistoryFilter {
            direct_target: Some(pb::operation_history_filter::DirectTarget::CandidateId(
                "candidate-1".into(),
            )),
            ..Default::default()
        };
        let entries = build_operation_history_entries(
            [&first, &second],
            &filter,
            90,
            &[
                OperationHistoryResolution::RejectOrCancel {
                    operation_id: "op-first".into(),
                    reason_category: pb::ResolutionReasonCategory::NotActionable,
                    rationale: Some("first value".into()),
                },
                OperationHistoryResolution::RejectOrCancel {
                    operation_id: "op-first".into(),
                    reason_category: pb::ResolutionReasonCategory::Duplicate,
                    rationale: Some("must be ignored".into()),
                },
                OperationHistoryResolution::RejectOrCancel {
                    operation_id: "op-second".into(),
                    reason_category: pb::ResolutionReasonCategory::Other,
                    rationale: None,
                },
            ],
        )
        .expect("valid history entries");

        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].resolution_reason_category,
            Some(pb::ResolutionReasonCategory::NotActionable as i32)
        );
        assert_eq!(
            entries[0].resolution_rationale.as_deref(),
            Some("first value")
        );
        assert_eq!(
            entries[1].resolution_reason_category,
            Some(pb::ResolutionReasonCategory::Other as i32)
        );
    }

    #[test]
    fn history_projects_and_filters_closed_resolution_decisions() {
        let create = history_operation(
            "op-create",
            20,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        let reject = history_operation(
            "op-reject",
            10,
            pb::durable_operation::Scope::CandidateId("candidate-1".into()),
        );
        let filter = pb::OperationHistoryFilter {
            direct_target: Some(pb::operation_history_filter::DirectTarget::CandidateId(
                "candidate-1".into(),
            )),
            decision_kinds: vec![pb::ResolutionDecisionKind::CreateIssue as i32],
            ..Default::default()
        };
        let entries = build_operation_history_entries_with_decisions(
            [&create, &reject],
            &filter,
            90,
            &[],
            &[
                OperationHistoryDecision {
                    operation_id: "op-create".into(),
                    kind: pb::ResolutionDecisionKind::CreateIssue,
                    candidate_id: "candidate-1".into(),
                    candidate_revision: 4,
                },
                OperationHistoryDecision {
                    operation_id: "op-reject".into(),
                    kind: pb::ResolutionDecisionKind::Reject,
                    candidate_id: "candidate-1".into(),
                    candidate_revision: 5,
                },
            ],
        )
        .expect("decision filter must be valid");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation_id, "op-create");
        assert_eq!(
            entries[0].resolution_decision_kind,
            Some(pb::ResolutionDecisionKind::CreateIssue as i32)
        );
        assert_eq!(entries[0].candidate_revision_at_decision, Some(4));

        assert_eq!(
            validate_operation_history_filter(
                &pb::OperationHistoryFilter {
                    direct_target: Some(pb::operation_history_filter::DirectTarget::CandidateId(
                        "candidate-1".into(),
                    ),),
                    decision_kinds: vec![pb::ResolutionDecisionKind::Unspecified as i32],
                    ..Default::default()
                },
                90,
            ),
            Err(OperationError::InvalidState(
                pb::ResolutionDecisionKind::Unspecified as i32
            ))
        );
    }

    trait WithDownstream {
        fn into_with_downstream(self, id: Option<String>) -> Self;
    }

    impl WithDownstream for NewOperation {
        fn into_with_downstream(mut self, id: Option<String>) -> Self {
            self.downstream_operation_id = id;
            self
        }
    }
}
