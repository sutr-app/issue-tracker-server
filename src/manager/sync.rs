//! Deterministic `sync_state` selection for canonical backend Issues.
//!
//! Sync state is a read projection over manager operations, never a copy of an
//! Issue.  The selector validates every causal chain before applying the state
//! precedence, so an incomplete or inconsistent operation history is safe by
//! default (`Unconfirmed`).

use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

use crate::proto_types::manager_v1;

pub(crate) const SELECTION_RULE_VERSION: &str = "issue-tracker-manager-sync/v1";
pub(crate) const SELECTION_PRECEDENCE_VERSION: &str =
    "unconfirmed,pending,failed,confirmed,arbitrated/v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OperationState {
    Received,
    Pending,
    Confirmed,
    Failed,
    Unconfirmed,
    ManualResolution,
    /// Human arbitration is terminal and intentionally does not affect
    /// readiness. It is still accepted by the read projection so one
    /// arbitrated operation cannot make every Issue sync read fail.
    Arbitrated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncState {
    Confirmed,
    Pending,
    Unconfirmed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CanonicalIssueState {
    Open,
    InProgress,
    Blocked,
    Deferred,
    Done,
    Canceled,
    AwaitingReview,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreationBasis {
    Explicit,
    AutoDetected,
    Imported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConfirmationStatus {
    Confirmed,
    Unreviewed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadyFacts {
    pub state: CanonicalIssueState,
    pub sync_state: SyncState,
    pub has_open_blocker: bool,
    pub has_active_claim: bool,
    pub claim_result_unconfirmed: bool,
    pub creation_basis: CreationBasis,
    pub confirmation_status: ConfirmationStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadyExclusion {
    StateNotOpen,
    OpenBlocker,
    ActiveClaim,
    ClaimResultUnconfirmed,
    SyncNotConfirmed,
    AutoDetectedUnreviewed,
}

/// Returns the first deterministic reason an Issue cannot be shown as
/// Lookback-ready.  `deferred` remains excluded even after its review date;
/// an explicit transition back to `open` is required.
pub(crate) fn ready_exclusion(facts: &ReadyFacts) -> Option<ReadyExclusion> {
    if facts.state != CanonicalIssueState::Open {
        return Some(ReadyExclusion::StateNotOpen);
    }
    if facts.has_open_blocker {
        return Some(ReadyExclusion::OpenBlocker);
    }
    if facts.has_active_claim {
        return Some(ReadyExclusion::ActiveClaim);
    }
    if facts.claim_result_unconfirmed {
        return Some(ReadyExclusion::ClaimResultUnconfirmed);
    }
    if facts.sync_state != SyncState::Confirmed {
        return Some(ReadyExclusion::SyncNotConfirmed);
    }
    if facts.creation_basis == CreationBasis::AutoDetected
        && facts.confirmation_status != ConfirmationStatus::Confirmed
    {
        return Some(ReadyExclusion::AutoDetectedUnreviewed);
    }
    None
}

pub(crate) fn is_lookback_ready(facts: &ReadyFacts) -> bool {
    ready_exclusion(facts).is_none()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectionReason {
    NoRelevantOperation,
    SelectedOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExclusionReason {
    None,
    Superseded,
    #[allow(dead_code)]
    NotAffectingReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerificationFailure {
    None,
    MissingOperation,
    BrokenCausalLink,
    Cycle,
    GenerationMismatch,
    SubjectMismatch,
    ScopeMismatch,
    FingerprintMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OperationScope {
    pub method: String,
    pub idempotency_scope: String,
    /// The durable operation's canonical target scope.  For an Issue
    /// mutation this is the Issue ID; relation operations use their
    /// backend-defined stable target scope.
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncOperation {
    pub operation_id: String,
    pub state: OperationState,
    pub generation: u64,
    pub manager_sequence: u64,
    pub affects_ready: bool,
    pub sync_subject: String,
    pub causal_parent: Option<String>,
    pub superseded_by: Option<String>,
    pub scope: OperationScope,
    pub affected_issue_ids: Vec<String>,
    /// The canonical payload digest is copied into a retry child unchanged.
    pub request_digest: String,
    /// Optional result digest is used to reject an explicitly contradictory
    /// replacement edge, but is not a replacement for marker verification.
    pub result_snapshot_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChainHeadEvidence {
    pub operation_id: String,
    pub causal_root_operation_id: String,
    pub generation: u64,
    pub manager_sequence: u64,
    pub state: OperationState,
    pub affects_ready: bool,
    pub sync_subject: String,
    pub ancestor_operation_ids: Vec<String>,
    pub excluded_operation_ids: Vec<String>,
    pub exclusion_reason: ExclusionReason,
    pub verification_failure: VerificationFailure,
    pub scope: OperationScope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncSelectionEvidence {
    pub selection_rule_version: String,
    pub sync_subject: String,
    pub candidate_operation_set_fingerprint: String,
    pub selected_operation_id: Option<String>,
    pub causal_root_operation_id: Option<String>,
    pub generation: u64,
    pub manager_sequence: u64,
    pub affects_ready: bool,
    pub selected_status: Option<OperationState>,
    pub selection_reason: SelectionReason,
    pub superseded_chain_fingerprint: String,
    pub excluded_operation_ids: Vec<String>,
    pub issue_ids: Vec<String>,
    pub chain_head_operation_ids: Vec<String>,
    pub chain_heads: Vec<ChainHeadEvidence>,
    pub chain_heads_fingerprint: String,
    pub selection_precedence_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncSelection {
    pub state: SyncState,
    pub evidence: SyncSelectionEvidence,
}

impl TryFrom<&manager_v1::DurableOperation> for SyncOperation {
    type Error = SyncError;

    fn try_from(operation: &manager_v1::DurableOperation) -> Result<Self, Self::Error> {
        let state = match manager_v1::DurableOperationState::try_from(operation.state) {
            Ok(manager_v1::DurableOperationState::Received) => OperationState::Received,
            Ok(manager_v1::DurableOperationState::Pending) => OperationState::Pending,
            Ok(manager_v1::DurableOperationState::Committed) => OperationState::Confirmed,
            Ok(manager_v1::DurableOperationState::Failed) => OperationState::Failed,
            Ok(manager_v1::DurableOperationState::Unconfirmed) => OperationState::Unconfirmed,
            Ok(manager_v1::DurableOperationState::ManualResolution) => {
                OperationState::ManualResolution
            }
            Ok(manager_v1::DurableOperationState::Arbitrated) => OperationState::Arbitrated,
            _ => {
                return Err(SyncError {
                    failure: VerificationFailure::MissingOperation,
                    operation_id: Some(operation.operation_id.clone()),
                });
            }
        };
        let target = match operation.scope.as_ref() {
            Some(manager_v1::durable_operation::Scope::IssueId(issue_id)) => issue_id.clone(),
            Some(manager_v1::durable_operation::Scope::CandidateId(candidate_id)) => {
                candidate_id.clone()
            }
            Some(manager_v1::durable_operation::Scope::EvidenceId(evidence_id)) => {
                evidence_id.clone()
            }
            Some(manager_v1::durable_operation::Scope::ExtractionRunId(run_id)) => run_id.clone(),
            None => String::new(),
        };
        Ok(Self {
            operation_id: operation.operation_id.clone(),
            state,
            generation: operation.generation,
            manager_sequence: operation.manager_sequence,
            affects_ready: operation.affects_ready,
            sync_subject: operation.sync_subject.clone().unwrap_or_default(),
            causal_parent: operation.causal_parent.clone(),
            superseded_by: operation.superseded_by.clone(),
            scope: OperationScope {
                method: operation.method.clone(),
                idempotency_scope: operation.idempotency_scope.clone(),
                target,
            },
            affected_issue_ids: operation.affected_issue_ids.clone(),
            request_digest: operation.request_digest.clone(),
            result_snapshot_digest: operation
                .commit_snapshot
                .as_ref()
                .map(|snapshot| snapshot.snapshot_digest.clone())
                .filter(|digest| !digest.is_empty()),
        })
    }
}

impl SyncSelectionEvidence {
    pub(crate) fn to_proto(&self) -> manager_v1::SyncSelectionEvidence {
        manager_v1::SyncSelectionEvidence {
            selection_rule_version: self.selection_rule_version.clone(),
            sync_subject: self.sync_subject.clone(),
            candidate_operation_set_fingerprint: self.candidate_operation_set_fingerprint.clone(),
            selected_operation_id: self.selected_operation_id.clone().unwrap_or_default(),
            causal_root_operation_id: self.causal_root_operation_id.clone().unwrap_or_default(),
            generation: self.generation,
            manager_sequence: self.manager_sequence,
            affects_ready: self.affects_ready,
            selected_status: self
                .selected_status
                .map(operation_state_proto)
                .unwrap_or_default(),
            selection_reason: selection_reason_proto(self.selection_reason),
            superseded_chain_fingerprint: self.superseded_chain_fingerprint.clone(),
            excluded_operation_ids: self.excluded_operation_ids.clone(),
            issue_ids: self.issue_ids.clone(),
            chain_head_operation_ids: self.chain_head_operation_ids.clone(),
            chain_heads: self.chain_heads.iter().map(chain_head_proto).collect(),
            chain_heads_fingerprint: self.chain_heads_fingerprint.clone(),
            selection_precedence_version: self.selection_precedence_version.clone(),
        }
    }
}

fn operation_state_proto(state: OperationState) -> i32 {
    match state {
        OperationState::Received => manager_v1::DurableOperationState::Received as i32,
        OperationState::Pending => manager_v1::DurableOperationState::Pending as i32,
        OperationState::Confirmed => manager_v1::DurableOperationState::Committed as i32,
        OperationState::Failed => manager_v1::DurableOperationState::Failed as i32,
        OperationState::Unconfirmed => manager_v1::DurableOperationState::Unconfirmed as i32,
        OperationState::ManualResolution => {
            manager_v1::DurableOperationState::ManualResolution as i32
        }
        OperationState::Arbitrated => manager_v1::DurableOperationState::Arbitrated as i32,
    }
}

fn selection_reason_proto(reason: SelectionReason) -> i32 {
    match reason {
        SelectionReason::NoRelevantOperation => {
            manager_v1::SyncSelectionReason::NoRelevantOperation as i32
        }
        SelectionReason::SelectedOperation => {
            manager_v1::SyncSelectionReason::SelectedOperation as i32
        }
    }
}

fn exclusion_reason_proto(reason: ExclusionReason) -> i32 {
    match reason {
        ExclusionReason::None => manager_v1::SyncChainExclusionReason::None as i32,
        ExclusionReason::Superseded => manager_v1::SyncChainExclusionReason::Superseded as i32,
        ExclusionReason::NotAffectingReady => {
            manager_v1::SyncChainExclusionReason::NotAffectingReady as i32
        }
    }
}

fn verification_failure_proto(failure: VerificationFailure) -> i32 {
    match failure {
        VerificationFailure::None => manager_v1::SyncChainVerificationFailure::None as i32,
        VerificationFailure::MissingOperation => {
            manager_v1::SyncChainVerificationFailure::MissingOperation as i32
        }
        VerificationFailure::BrokenCausalLink => {
            manager_v1::SyncChainVerificationFailure::BrokenCausalLink as i32
        }
        VerificationFailure::Cycle => manager_v1::SyncChainVerificationFailure::Cycle as i32,
        VerificationFailure::GenerationMismatch => {
            manager_v1::SyncChainVerificationFailure::GenerationMismatch as i32
        }
        VerificationFailure::SubjectMismatch => {
            manager_v1::SyncChainVerificationFailure::SubjectMismatch as i32
        }
        VerificationFailure::ScopeMismatch => {
            manager_v1::SyncChainVerificationFailure::ScopeMismatch as i32
        }
        VerificationFailure::FingerprintMismatch => {
            manager_v1::SyncChainVerificationFailure::FingerprintMismatch as i32
        }
    }
}

fn chain_head_proto(head: &ChainHeadEvidence) -> manager_v1::SyncChainHeadEvidence {
    manager_v1::SyncChainHeadEvidence {
        operation_id: head.operation_id.clone(),
        causal_root_operation_id: head.causal_root_operation_id.clone(),
        generation: head.generation,
        manager_sequence: head.manager_sequence,
        state: operation_state_proto(head.state),
        affects_ready: head.affects_ready,
        sync_subject: head.sync_subject.clone(),
        ancestor_operation_ids: head.ancestor_operation_ids.clone(),
        excluded_operation_ids: head.excluded_operation_ids.clone(),
        exclusion_reason: exclusion_reason_proto(head.exclusion_reason),
        verification_failure: verification_failure_proto(head.verification_failure),
        scope: Some(manager_v1::SyncOperationScope {
            scope: Some(manager_v1::sync_operation_scope::Scope::IssueId(
                head.scope.target.clone(),
            )),
            method: head.scope.method.clone(),
            idempotency_scope: head.scope.idempotency_scope.clone(),
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncError {
    pub failure: VerificationFailure,
    pub operation_id: Option<String>,
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "sync operation history verification failed: {:?}",
            self.failure
        )
    }
}

impl std::error::Error for SyncError {}

/// Selects a sync state from all manager operations for one Issue subject.
///
/// Operations which do not affect readiness are ignored.  They must not be
/// allowed to hide a pending or failed canonical Issue mutation merely by
/// being newer than it.
pub(crate) fn select_sync_state(
    operations: &[SyncOperation],
    sync_subject: &str,
    read_generation: u64,
) -> SyncSelection {
    let mut relevant = operations
        .iter()
        .filter(|operation| operation.affects_ready && operation.sync_subject == sync_subject)
        .cloned()
        .collect::<Vec<_>>();
    relevant.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));

    if relevant.is_empty() {
        return SyncSelection {
            state: SyncState::Confirmed,
            evidence: empty_evidence(sync_subject, read_generation),
        };
    }

    let mut evidence = base_evidence(&relevant, sync_subject, read_generation);
    let verification = verify_operations(&relevant, sync_subject);
    if let Err(error) = verification {
        evidence.selection_reason = SelectionReason::SelectedOperation;
        evidence.generation = relevant.iter().map(|op| op.generation).max().unwrap_or(0);
        evidence.manager_sequence = relevant
            .iter()
            .map(|op| op.manager_sequence)
            .max()
            .unwrap_or(0);
        evidence.chain_heads = vec![failure_head(&relevant, error.failure, error.operation_id)];
        evidence.chain_head_operation_ids = evidence
            .chain_heads
            .iter()
            .map(|head| head.operation_id.clone())
            .collect();
        evidence.chain_heads_fingerprint = fingerprint_chain_heads(&evidence.chain_heads);
        return SyncSelection {
            state: SyncState::Unconfirmed,
            evidence,
        };
    }

    let operations_by_id = relevant
        .iter()
        .map(|operation| (operation.operation_id.as_str(), operation))
        .collect::<HashMap<_, _>>();
    let heads = relevant
        .iter()
        .filter(|operation| operation.superseded_by.is_none())
        .collect::<Vec<_>>();

    evidence.selection_reason = SelectionReason::SelectedOperation;
    evidence.generation = heads.iter().map(|op| op.generation).max().unwrap_or(0);
    evidence.manager_sequence = heads
        .iter()
        .map(|op| op.manager_sequence)
        .max()
        .unwrap_or(0);
    evidence.chain_heads = heads
        .iter()
        .map(|head| chain_head(head, &operations_by_id))
        .collect();
    evidence
        .chain_heads
        .sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    evidence.chain_head_operation_ids = evidence
        .chain_heads
        .iter()
        .map(|head| head.operation_id.clone())
        .collect();
    evidence.causal_root_operation_id = (evidence.chain_heads.len() == 1)
        .then(|| evidence.chain_heads[0].causal_root_operation_id.clone());
    evidence.selected_operation_id =
        (evidence.chain_heads.len() == 1).then(|| evidence.chain_heads[0].operation_id.clone());
    evidence.selected_status =
        (evidence.chain_heads.len() == 1).then(|| evidence.chain_heads[0].state);
    evidence.excluded_operation_ids = evidence
        .chain_heads
        .iter()
        .flat_map(|head| head.excluded_operation_ids.iter().cloned())
        .collect();
    evidence.excluded_operation_ids.sort_unstable();
    evidence.superseded_chain_fingerprint = fingerprint_strings(&evidence.excluded_operation_ids);
    evidence.chain_heads_fingerprint = fingerprint_chain_heads(&evidence.chain_heads);

    SyncSelection {
        state: aggregate_state(&evidence.chain_heads),
        evidence,
    }
}

fn verify_operations(operations: &[SyncOperation], sync_subject: &str) -> Result<(), SyncError> {
    let mut by_id = HashMap::new();
    for operation in operations {
        if operation.operation_id.trim().is_empty() {
            return Err(SyncError {
                failure: VerificationFailure::MissingOperation,
                operation_id: None,
            });
        }
        if by_id
            .insert(operation.operation_id.as_str(), operation)
            .is_some()
        {
            return Err(SyncError {
                failure: VerificationFailure::BrokenCausalLink,
                operation_id: Some(operation.operation_id.clone()),
            });
        }
        if operation.sync_subject != sync_subject
            || !operation
                .affected_issue_ids
                .iter()
                .any(|issue_id| issue_id == sync_subject)
        {
            return Err(SyncError {
                failure: VerificationFailure::SubjectMismatch,
                operation_id: Some(operation.operation_id.clone()),
            });
        }
        if operation.generation == 0
            || operation.scope.method.trim().is_empty()
            || operation.scope.idempotency_scope.trim().is_empty()
            || operation.request_digest.trim().is_empty()
        {
            return Err(SyncError {
                failure: VerificationFailure::FingerprintMismatch,
                operation_id: Some(operation.operation_id.clone()),
            });
        }
    }

    // Detect a causal cycle before validating generation order.  A cycle is a
    // stronger and more actionable proof failure than the incidental reverse
    // generation encountered on one of its edges.
    for operation in operations {
        let mut seen = HashSet::new();
        let mut current = operation;
        while let Some(parent_id) = current.causal_parent.as_deref() {
            if !seen.insert(current.operation_id.as_str()) {
                return Err(SyncError {
                    failure: VerificationFailure::Cycle,
                    operation_id: Some(operation.operation_id.clone()),
                });
            }
            current = by_id.get(parent_id).ok_or_else(|| SyncError {
                failure: VerificationFailure::MissingOperation,
                operation_id: Some(operation.operation_id.clone()),
            })?;
        }
    }

    for operation in operations {
        if let Some(parent_id) = operation.causal_parent.as_deref() {
            let Some(parent) = by_id.get(parent_id) else {
                return Err(SyncError {
                    failure: VerificationFailure::MissingOperation,
                    operation_id: Some(operation.operation_id.clone()),
                });
            };
            if parent.superseded_by.as_deref() != Some(operation.operation_id.as_str()) {
                return Err(SyncError {
                    failure: VerificationFailure::BrokenCausalLink,
                    operation_id: Some(operation.operation_id.clone()),
                });
            }
            validate_edge(parent, operation)?;
        }
        if let Some(child_id) = operation.superseded_by.as_deref() {
            let Some(child) = by_id.get(child_id) else {
                return Err(SyncError {
                    failure: VerificationFailure::MissingOperation,
                    operation_id: Some(operation.operation_id.clone()),
                });
            };
            if child.causal_parent.as_deref() != Some(operation.operation_id.as_str()) {
                return Err(SyncError {
                    failure: VerificationFailure::BrokenCausalLink,
                    operation_id: Some(operation.operation_id.clone()),
                });
            }
            validate_edge(operation, child)?;
        }
    }

    Ok(())
}

fn validate_edge(parent: &SyncOperation, child: &SyncOperation) -> Result<(), SyncError> {
    if child.generation <= parent.generation {
        return Err(SyncError {
            failure: VerificationFailure::GenerationMismatch,
            operation_id: Some(child.operation_id.clone()),
        });
    }
    if parent.sync_subject != child.sync_subject
        || parent.affected_issue_ids != child.affected_issue_ids
    {
        return Err(SyncError {
            failure: VerificationFailure::SubjectMismatch,
            operation_id: Some(child.operation_id.clone()),
        });
    }
    if parent.scope != child.scope {
        return Err(SyncError {
            failure: VerificationFailure::ScopeMismatch,
            operation_id: Some(child.operation_id.clone()),
        });
    }
    if parent.request_digest != child.request_digest {
        return Err(SyncError {
            failure: VerificationFailure::FingerprintMismatch,
            operation_id: Some(child.operation_id.clone()),
        });
    }
    if parent
        .result_snapshot_digest
        .as_ref()
        .zip(child.result_snapshot_digest.as_ref())
        .is_some_and(|(parent_digest, child_digest)| parent_digest != child_digest)
    {
        return Err(SyncError {
            failure: VerificationFailure::FingerprintMismatch,
            operation_id: Some(child.operation_id.clone()),
        });
    }
    Ok(())
}

fn chain_head(
    head: &SyncOperation,
    operations: &HashMap<&str, &SyncOperation>,
) -> ChainHeadEvidence {
    let mut ancestors = Vec::new();
    let mut current = head;
    let causal_root_operation_id;
    while let Some(parent_id) = current.causal_parent.as_deref() {
        ancestors.push(parent_id.to_owned());
        // `verify_operations` has already checked this link.
        current = operations[parent_id];
    }
    causal_root_operation_id = current.operation_id.clone();
    ChainHeadEvidence {
        operation_id: head.operation_id.clone(),
        causal_root_operation_id,
        generation: head.generation,
        manager_sequence: head.manager_sequence,
        state: head.state,
        affects_ready: head.affects_ready,
        sync_subject: head.sync_subject.clone(),
        excluded_operation_ids: ancestors.clone(),
        ancestor_operation_ids: ancestors,
        exclusion_reason: if head.causal_parent.is_some() {
            ExclusionReason::Superseded
        } else {
            ExclusionReason::None
        },
        verification_failure: VerificationFailure::None,
        scope: head.scope.clone(),
    }
}

fn aggregate_state(heads: &[ChainHeadEvidence]) -> SyncState {
    if heads.iter().any(|head| {
        matches!(
            head.state,
            OperationState::Unconfirmed
                | OperationState::ManualResolution
                | OperationState::Arbitrated
        )
    }) {
        return SyncState::Unconfirmed;
    }
    if heads.iter().any(|head| {
        matches!(
            head.state,
            OperationState::Received | OperationState::Pending
        )
    }) {
        return SyncState::Pending;
    }
    if heads
        .iter()
        .any(|head| matches!(head.state, OperationState::Failed))
    {
        return SyncState::Failed;
    }
    SyncState::Confirmed
}

fn base_evidence(
    operations: &[SyncOperation],
    sync_subject: &str,
    read_generation: u64,
) -> SyncSelectionEvidence {
    let mut issue_ids = operations
        .iter()
        .flat_map(|operation| operation.affected_issue_ids.iter().cloned())
        .collect::<Vec<_>>();
    issue_ids.sort_unstable();
    issue_ids.dedup();
    SyncSelectionEvidence {
        selection_rule_version: SELECTION_RULE_VERSION.into(),
        sync_subject: sync_subject.into(),
        candidate_operation_set_fingerprint: fingerprint_operations(operations),
        selected_operation_id: None,
        causal_root_operation_id: None,
        generation: read_generation,
        manager_sequence: 0,
        affects_ready: true,
        selected_status: None,
        selection_reason: SelectionReason::NoRelevantOperation,
        superseded_chain_fingerprint: fingerprint_strings(&[]),
        excluded_operation_ids: Vec::new(),
        issue_ids,
        chain_head_operation_ids: Vec::new(),
        chain_heads: Vec::new(),
        chain_heads_fingerprint: fingerprint_chain_heads(&[]),
        selection_precedence_version: SELECTION_PRECEDENCE_VERSION.into(),
    }
}

fn empty_evidence(sync_subject: &str, read_generation: u64) -> SyncSelectionEvidence {
    base_evidence(&[], sync_subject, read_generation)
}

fn failure_head(
    operations: &[SyncOperation],
    failure: VerificationFailure,
    operation_id: Option<String>,
) -> ChainHeadEvidence {
    let operation = operation_id
        .as_deref()
        .and_then(|id| operations.iter().find(|op| op.operation_id == id))
        .unwrap_or(&operations[0]);
    ChainHeadEvidence {
        operation_id: operation.operation_id.clone(),
        causal_root_operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        manager_sequence: operation.manager_sequence,
        state: operation.state,
        affects_ready: operation.affects_ready,
        sync_subject: operation.sync_subject.clone(),
        ancestor_operation_ids: Vec::new(),
        excluded_operation_ids: Vec::new(),
        exclusion_reason: ExclusionReason::None,
        verification_failure: failure,
        scope: operation.scope.clone(),
    }
}

fn fingerprint_operations(operations: &[SyncOperation]) -> String {
    let mut values = operations
        .iter()
        .map(|operation| {
            format!(
                "{}\u{1f}{:?}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
                operation.operation_id,
                operation.state,
                operation.generation,
                operation.manager_sequence,
                operation.sync_subject,
                operation.causal_parent.as_deref().unwrap_or_default(),
                operation.superseded_by.as_deref().unwrap_or_default(),
                operation.request_digest,
            )
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    fingerprint_strings(&values)
}

fn fingerprint_chain_heads(heads: &[ChainHeadEvidence]) -> String {
    let values = heads
        .iter()
        .map(|head| {
            format!(
                "{}\u{1f}{}\u{1f}{}\u{1f}{:?}\u{1f}{:?}\u{1f}{:?}",
                head.operation_id,
                head.causal_root_operation_id,
                head.generation,
                head.state,
                head.exclusion_reason,
                head.verification_failure,
            )
        })
        .collect::<Vec<_>>();
    fingerprint_strings(&values)
}

fn fingerprint_strings(values: &[String]) -> String {
    let mut hasher = Sha256::new();
    for value in values {
        hasher.update(value.as_bytes());
        hasher.update([0x1f]);
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(id: &str, state: OperationState, generation: u64) -> SyncOperation {
        SyncOperation {
            operation_id: id.into(),
            state,
            generation,
            manager_sequence: generation,
            affects_ready: true,
            sync_subject: "issue-1".into(),
            causal_parent: None,
            superseded_by: None,
            scope: OperationScope {
                method: "manager_update_issue".into(),
                idempotency_scope: "root/issue-1".into(),
                target: "issue-1".into(),
            },
            affected_issue_ids: vec!["issue-1".into()],
            request_digest: "request-digest".into(),
            result_snapshot_digest: Some("result-digest".into()),
        }
    }

    #[test]
    fn no_relevant_operation_is_confirmed_with_explicit_reason() {
        let mut ignored = operation("manager-only", OperationState::Pending, 1);
        ignored.affects_ready = false;
        let result = select_sync_state(&[ignored], "issue-1", 4);
        assert_eq!(result.state, SyncState::Confirmed);
        assert_eq!(
            result.evidence.selection_reason,
            SelectionReason::NoRelevantOperation
        );
        assert!(result.evidence.chain_heads.is_empty());
    }

    #[test]
    fn arbitrated_operation_is_a_valid_non_ready_projection() {
        let operation = manager_v1::DurableOperation {
            operation_id: "arbitrated".into(),
            state: manager_v1::DurableOperationState::Arbitrated as i32,
            affects_ready: false,
            generation: 1,
            manager_sequence: 1,
            sync_subject: Some("issue-1".into()),
            affected_issue_ids: vec!["issue-1".into()],
            method: "manager_resolve_issue_operation".into(),
            idempotency_scope: "root/issue-1/arbitration".into(),
            request_digest: "request-digest".into(),
            ..Default::default()
        };
        let converted = SyncOperation::try_from(&operation).expect("arbitrated is known");
        assert_eq!(converted.state, OperationState::Arbitrated);
        let selection = select_sync_state(&[converted], "issue-1", 2);
        assert_eq!(selection.state, SyncState::Confirmed);
        assert_eq!(
            selection.evidence.selection_reason,
            SelectionReason::NoRelevantOperation
        );
    }

    fn ready_facts() -> ReadyFacts {
        ReadyFacts {
            state: CanonicalIssueState::Open,
            sync_state: SyncState::Confirmed,
            has_open_blocker: false,
            has_active_claim: false,
            claim_result_unconfirmed: false,
            creation_basis: CreationBasis::Explicit,
            confirmation_status: ConfirmationStatus::Confirmed,
        }
    }

    #[test]
    fn ready_requires_open_and_confirmed_sync() {
        let mut facts = ready_facts();
        assert!(is_lookback_ready(&facts));
        facts.state = CanonicalIssueState::Deferred;
        assert_eq!(ready_exclusion(&facts), Some(ReadyExclusion::StateNotOpen));
        facts = ready_facts();
        facts.sync_state = SyncState::Pending;
        assert_eq!(
            ready_exclusion(&facts),
            Some(ReadyExclusion::SyncNotConfirmed)
        );
    }

    #[test]
    fn ready_excludes_blockers_claims_and_unreviewed_auto_detection() {
        let mut facts = ready_facts();
        facts.has_open_blocker = true;
        assert_eq!(ready_exclusion(&facts), Some(ReadyExclusion::OpenBlocker));
        facts = ready_facts();
        facts.has_active_claim = true;
        assert_eq!(ready_exclusion(&facts), Some(ReadyExclusion::ActiveClaim));
        facts = ready_facts();
        facts.claim_result_unconfirmed = true;
        assert_eq!(
            ready_exclusion(&facts),
            Some(ReadyExclusion::ClaimResultUnconfirmed)
        );
        facts = ready_facts();
        facts.creation_basis = CreationBasis::AutoDetected;
        facts.confirmation_status = ConfirmationStatus::Unreviewed;
        assert_eq!(
            ready_exclusion(&facts),
            Some(ReadyExclusion::AutoDetectedUnreviewed)
        );
    }

    #[test]
    fn state_precedence_does_not_hide_pending_or_failed_heads() {
        let operations = vec![
            operation("confirmed", OperationState::Confirmed, 1),
            operation("pending", OperationState::Pending, 2),
            operation("failed", OperationState::Failed, 3),
        ];
        assert_eq!(
            select_sync_state(&operations, "issue-1", 4).state,
            SyncState::Pending
        );
        let operations = vec![
            operation("confirmed", OperationState::Confirmed, 1),
            operation("failed", OperationState::Failed, 2),
        ];
        assert_eq!(
            select_sync_state(&operations, "issue-1", 3).state,
            SyncState::Failed
        );
    }

    #[test]
    fn verified_retry_child_replaces_unconfirmed_parent() {
        let mut parent = operation("parent", OperationState::Unconfirmed, 1);
        parent.superseded_by = Some("child".into());
        let mut child = operation("child", OperationState::Confirmed, 2);
        child.causal_parent = Some("parent".into());
        let result = select_sync_state(&[parent, child], "issue-1", 3);
        assert_eq!(result.state, SyncState::Confirmed);
        assert_eq!(
            result.evidence.selected_operation_id.as_deref(),
            Some("child")
        );
        assert_eq!(result.evidence.excluded_operation_ids, vec!["parent"]);
    }

    #[test]
    fn missing_parent_fails_closed() {
        let mut child = operation("child", OperationState::Confirmed, 2);
        child.causal_parent = Some("missing".into());
        let result = select_sync_state(&[child], "issue-1", 3);
        assert_eq!(result.state, SyncState::Unconfirmed);
        assert_eq!(
            result.evidence.chain_heads[0].verification_failure,
            VerificationFailure::MissingOperation
        );
    }

    #[test]
    fn cycle_fails_closed_before_state_aggregation() {
        let mut first = operation("first", OperationState::Confirmed, 1);
        first.causal_parent = Some("second".into());
        first.superseded_by = Some("second".into());
        let mut second = operation("second", OperationState::Confirmed, 2);
        second.causal_parent = Some("first".into());
        second.superseded_by = Some("first".into());
        let result = select_sync_state(&[first, second], "issue-1", 3);
        assert_eq!(result.state, SyncState::Unconfirmed);
        assert_eq!(
            result.evidence.chain_heads[0].verification_failure,
            VerificationFailure::Cycle
        );
    }

    #[test]
    fn replacement_with_different_scope_is_unconfirmed() {
        let mut parent = operation("parent", OperationState::Pending, 1);
        parent.superseded_by = Some("child".into());
        let mut child = operation("child", OperationState::Confirmed, 2);
        child.causal_parent = Some("parent".into());
        child.scope.method = "manager_transition_issue".into();
        let result = select_sync_state(&[parent, child], "issue-1", 3);
        assert_eq!(result.state, SyncState::Unconfirmed);
        assert_eq!(
            result.evidence.chain_heads[0].verification_failure,
            VerificationFailure::ScopeMismatch
        );
    }

    #[test]
    fn invalid_selection_evidence_is_independent_of_input_order() {
        let mut first = operation("z-operation", OperationState::Confirmed, 1);
        first.causal_parent = Some("missing".into());
        let second = operation("a-operation", OperationState::Pending, 2);
        let forward = select_sync_state(&[first.clone(), second.clone()], "issue-1", 3);
        let reverse = select_sync_state(&[second, first], "issue-1", 3);
        assert_eq!(forward.state, SyncState::Unconfirmed);
        assert_eq!(forward.evidence, reverse.evidence);
    }
}
