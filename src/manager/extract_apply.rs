use super::candidates::{CandidateStore, metadata_value};
use super::extract_eval::{fingerprint, is_lexical_duplicate};
use super::extraction::ExtractionRunStore;
use super::source_labels::{replace_manager_metadata, snapshot_for_extraction};
use crate::proto_types::manager_v1 as pb;
use prost::Message;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

#[cfg(test)]
use super::extract_persist::{persist_apply, persist_complete_run, persist_mark_registered};

#[derive(Clone, Debug)]
pub struct ExtractCandidateInput {
    pub title: String,
    pub description: String,
    pub kind: String,
    pub suggested_priority: String,
    pub suggested_state: String,
    pub unfinished_reason: String,
    pub completion: String,
    pub detection: String,
    pub confidence: f64,
    pub locator_memory_ids: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ApplyRequest {
    pub thread_id: String,
    pub summary_ref: String,
    pub snapshot_digest: String,
    pub extract_schema: String,
    pub policy: Option<pb::ExtractionPolicy>,
    pub model_id: String,
    pub prompt_version: String,
    pub allowed_messages: Vec<(String, String)>,
    pub input_message_ids: Vec<String>,
    pub candidates: Vec<ExtractCandidateInput>,
    pub mixed: bool,
    pub failure_reason: Option<String>,
    pub idempotency_key: String,
    pub source_labels: Vec<String>,
    /// Candidate IDs whose REJECTED exact-match suppression is bypassed once
    /// for this extraction run. The identities themselves remain immutable.
    pub rejected_candidate_ids_to_unsuppress: Vec<String>,
    /// Server-owned immutable identities loaded from the suppression projection.
    /// Callers must never supply these values directly.
    pub rejected_candidate_identities: Vec<ExtractionCandidateIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtractionCandidateIdentity {
    pub candidate_id: String,
    pub thread_id: String,
    pub snapshot_digest: String,
    pub fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtractionSuppression {
    pub input_index: u32,
    pub rejected_candidate_ids: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied,
    SkippedExtractionDisabled,
}

#[derive(Debug)]
pub(crate) enum ApplyError {
    InvalidArgument(String),
    Failed(String),
}

impl ApplyError {
    pub(crate) fn message(self) -> String {
        match self {
            Self::InvalidArgument(message) | Self::Failed(message) => message,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ApplyResult {
    pub outcome: ApplyOutcome,
    pub request_digest: String,
    pub snapshot_digest: String,
    pub run_id: String,
    pub run_state: i32,
    pub candidate_ids: Vec<String>,
    pub candidate_identities: Vec<ExtractionCandidateIdentity>,
    pub issue_ids: Vec<String>,
    pub registerable_candidate_ids: Vec<String>,
    pub persisted_locator_memory_ids: Vec<String>,
    pub suppressions: Vec<ExtractionSuppression>,
    pub checkpoint: String,
}

#[derive(Clone, Debug)]
pub struct ExistingHint {
    pub thread_id: Option<String>,
    pub kind: String,
    pub title: String,
    pub completion: String,
    pub snapshot_digest: Option<String>,
}

struct PreparedRequest {
    thread_id: String,
    summary_ref: String,
    snapshot_digest: String,
    extract_schema: String,
    policy: pb::ExtractionPolicy,
    policy_digest: String,
    request_digest: String,
    model_id: String,
    prompt_version: String,
    allowed_messages: Vec<(String, String)>,
    input_message_ids: Vec<String>,
    candidates: Vec<ExtractCandidateInput>,
    mixed: bool,
    failure_reason: Option<String>,
    source_labels: Vec<String>,
    rejected_candidate_ids_to_unsuppress: Vec<String>,
    rejected_candidate_identities: Vec<ExtractionCandidateIdentity>,
    suppression_plan: SuppressionPlan,
}

pub fn apply(
    runs: &mut ExtractionRunStore,
    candidates: &mut CandidateStore,
    existing: &[ExistingHint],
    request: ApplyRequest,
    now: pb::UtcInstant,
    mut register: impl FnMut(&ExtractCandidateInput, &str) -> Result<Option<String>, String>,
) -> Result<ApplyResult, ApplyError> {
    let request = prepare_request(request)?;
    if !request.policy.extraction_enabled {
        return Ok(ApplyResult {
            outcome: ApplyOutcome::SkippedExtractionDisabled,
            request_digest: request.request_digest,
            snapshot_digest: request.snapshot_digest,
            run_id: String::new(),
            run_state: pb::ExtractionRunState::Unspecified as i32,
            candidate_ids: Vec::new(),
            candidate_identities: Vec::new(),
            issue_ids: Vec::new(),
            registerable_candidate_ids: Vec::new(),
            persisted_locator_memory_ids: Vec::new(),
            suppressions: Vec::new(),
            checkpoint: String::new(),
        });
    }
    if let Some(existing_run) = runs.list().into_iter().find(|run| {
        run.request_digest == request.request_digest
            && matches!(
                pb::ExtractionRunState::try_from(run.state),
                Ok(pb::ExtractionRunState::Completed
                    | pb::ExtractionRunState::Unconfirmed
                    | pb::ExtractionRunState::Running
                    | pb::ExtractionRunState::Failed)
            )
    }) {
        return Ok(replay_existing(candidates, &existing_run));
    }

    let mut run = runs
        .create(
            &pb::ExtractionRunDraft {
                source: request.summary_ref.clone(),
                request_digest: request.request_digest.clone(),
                request_schema: request.extract_schema.clone(),
                ..Default::default()
            },
            now.clone(),
        )
        .map_err(|error| ApplyError::Failed(format!("{error:?}")))?;
    let checkpoint = serde_json::json!({
        "schema": "extraction_run_checkpoint.v1",
        "thread_id": request.thread_id,
        "summary_ref": request.summary_ref,
        "input_message_ids": request.input_message_ids,
        "policy_digest": request.policy_digest,
        "model": request.model_id,
        "prompt": request.prompt_version,
        "snapshot_digest": request.snapshot_digest,
    })
    .to_string();
    run = runs
        .advance(
            &run.run_id,
            run.revision,
            &pb::ExtractionRunAdvance {
                target_state: pb::ExtractionRunState::Running as i32,
                ..Default::default()
            },
            now.clone(),
        )
        .map_err(|error| ApplyError::Failed(format!("{error:?}")))?;

    let adopted = adopted_inputs(
        &request
            .candidates
            .iter()
            .enumerate()
            .filter(|(index, _)| !request.suppression_plan.excludes(*index))
            .collect::<Vec<_>>(),
        request.policy.extract_cap,
    );
    let truncation = pb::ExtractionTruncationRecord {
        input_candidate_count: u32::try_from(request.candidates.len())
            .map_err(|_| ApplyError::Failed("candidate count exceeds uint32".into()))?,
        adopted_input_indices: adopted.iter().map(|(index, _)| *index as u32).collect(),
        persisted_candidate_count: u32::try_from(adopted.len())
            .map_err(|_| ApplyError::Failed("candidate count exceeds uint32".into()))?,
        suppressions: request.suppression_plan.audit_records(),
    };
    run = runs
        .attach_apply_metadata(
            &run.run_id,
            request.policy.clone(),
            request.policy_digest.clone(),
            truncation,
        )
        .map_err(|error| ApplyError::Failed(format!("{error:?}")))?;

    if let Some(reason) = request.failure_reason.as_ref() {
        let failed = runs
            .advance(
                &run.run_id,
                run.revision,
                &pb::ExtractionRunAdvance {
                    target_state: pb::ExtractionRunState::Failed as i32,
                    error: Some(reason.clone()),
                    ..Default::default()
                },
                now,
            )
            .map_err(|error| ApplyError::Failed(format!("{error:?}")))?;
        return Ok(ApplyResult {
            outcome: ApplyOutcome::Applied,
            request_digest: request.request_digest.clone(),
            snapshot_digest: request.snapshot_digest.clone(),
            run_id: failed.run_id,
            run_state: failed.state,
            candidate_ids: Vec::new(),
            candidate_identities: Vec::new(),
            issue_ids: Vec::new(),
            registerable_candidate_ids: Vec::new(),
            persisted_locator_memory_ids: Vec::new(),
            suppressions: Vec::new(),
            checkpoint,
        });
    }

    let window: HashSet<&str> = request
        .input_message_ids
        .iter()
        .map(String::as_str)
        .collect();
    let digest_by_id: HashMap<&str, &str> = request
        .allowed_messages
        .iter()
        .map(|(id, digest)| (id.as_str(), digest.as_str()))
        .collect();

    let mut created_ids = Vec::new();
    let mut created_identities = Vec::new();
    let mut issue_ids = Vec::new();
    let mut registerable = Vec::new();
    let mut persisted_locator_memory_ids = Vec::new();
    let mut seen_locator_memory_ids = HashSet::new();
    let mut seen_fingerprints = HashSet::new();
    let mixed = request.mixed
        || request
            .candidates
            .iter()
            .any(|input| input_incomplete(input) || !locators_ok(input, &window, &digest_by_id));
    for (index, input) in adopted {
        for locator in &input.locator_memory_ids {
            if seen_locator_memory_ids.insert(locator.clone()) {
                persisted_locator_memory_ids.push(locator.clone());
            }
        }
        let kind = parse_kind(&input.kind).map_err(ApplyError::InvalidArgument)?;
        let print = fingerprint(&input.kind, &input.title, &input.completion);
        let duplicate_input = !seen_fingerprints.insert(print.clone());
        let issued = already_issued(candidates, &request.thread_id, &print);
        let candidate_id = format!("{}-{index}", run.run_id);
        if !duplicate_input && !issued {
            supersede_same_thread(candidates, &request.thread_id, &print, &candidate_id, &now);
        }
        let mut metadata = vec![
            pb::MetadataEntry {
                key: "thread_id".into(),
                value: request.thread_id.clone(),
            },
            pb::MetadataEntry {
                key: "unfinished_reason".into(),
                value: input.unfinished_reason.clone(),
            },
            pb::MetadataEntry {
                key: "fingerprint".into(),
                value: print.clone(),
            },
            pb::MetadataEntry {
                key: "snapshot_digest".into(),
                value: request.snapshot_digest.clone(),
            },
        ];
        replace_manager_metadata(
            &mut metadata,
            &[snapshot_for_extraction(
                request.thread_id.clone(),
                run.run_id.clone(),
                &request.source_labels,
            )
            .map_err(ApplyError::InvalidArgument)?],
        )
        .map_err(ApplyError::InvalidArgument)?;
        let created = candidates
            .create(
                &pb::CandidateDraft {
                    candidate_id: candidate_id.clone(),
                    title: input.title.clone(),
                    description: input.description.clone(),
                    kind,
                    suggested_priority: parse_priority(&input.suggested_priority)
                        .map_err(ApplyError::InvalidArgument)?,
                    suggested_state: parse_state(&input.suggested_state)
                        .map_err(ApplyError::InvalidArgument)?,
                    acceptance_criteria: vec![input.completion.clone()],
                    detection_basis: parse_detection(&input.detection)
                        .map_err(ApplyError::InvalidArgument)?,
                    extraction_run_id: run.run_id.clone(),
                    extraction_revision: run.revision,
                    source_evidence: input
                        .locator_memory_ids
                        .iter()
                        .map(|memory_id| crate::proto_types::v1::SourceEvidenceRef {
                            thread_id: request.thread_id.clone(),
                            memory_id: memory_id.clone(),
                        })
                        .collect(),
                    metadata,
                    ..Default::default()
                },
                now.clone(),
            )
            .map_err(|error| ApplyError::Failed(format!("{error:?}")))?;
        created_ids.push(created.candidate_id.clone());
        created_identities.push(ExtractionCandidateIdentity {
            candidate_id: created.candidate_id.clone(),
            thread_id: request.thread_id.clone(),
            snapshot_digest: request.snapshot_digest.clone(),
            fingerprint: print.clone(),
        });

        let duplicate = duplicate_input
            || issued
            || is_exact_duplicate(
                existing,
                candidates,
                &created.candidate_id,
                &request.snapshot_digest,
                &print,
            )
            || is_lexical_duplicate_of(
                existing,
                candidates,
                input,
                &request.thread_id,
                &created.candidate_id,
                &print,
            );
        let strong = is_strong(&request.policy, input, mixed, duplicate);
        let proof = strong.then(|| strong_proof(&request, &request.policy, input));
        let mut registered = None;
        if strong {
            registered = register(input, &request.thread_id).map_err(ApplyError::Failed)?;
            if registered.is_none() {
                registerable.push(created.candidate_id.clone());
            }
        }
        candidates
            .record_extraction_outcome(
                &created.candidate_id,
                proof,
                registered.clone(),
                now.clone(),
            )
            .map_err(|error| ApplyError::Failed(format!("{error:?}")))?;
        if let Some(issue_id) = registered {
            issue_ids.push(issue_id);
        }
    }

    if registerable.is_empty() {
        let completed = runs
            .advance(
                &run.run_id,
                run.revision,
                &pb::ExtractionRunAdvance {
                    target_state: pb::ExtractionRunState::Completed as i32,
                    progress_percent: Some(100),
                    checkpoint: Some(checkpoint.clone()),
                    ..Default::default()
                },
                now,
            )
            .map_err(|error| ApplyError::Failed(format!("{error:?}")))?;
        return Ok(ApplyResult {
            outcome: ApplyOutcome::Applied,
            request_digest: request.request_digest.clone(),
            snapshot_digest: request.snapshot_digest.clone(),
            run_id: completed.run_id,
            run_state: completed.state,
            candidate_ids: created_ids,
            candidate_identities: created_identities,
            issue_ids,
            registerable_candidate_ids: registerable,
            persisted_locator_memory_ids,
            suppressions: request.suppression_plan.suppressions.clone(),
            checkpoint,
        });
    }
    let _ = runs.attach_checkpoint(&run.run_id, &checkpoint);
    Ok(ApplyResult {
        outcome: ApplyOutcome::Applied,
        request_digest: request.request_digest.clone(),
        snapshot_digest: request.snapshot_digest.clone(),
        run_id: run.run_id,
        run_state: run.state,
        candidate_ids: created_ids,
        candidate_identities: created_identities,
        issue_ids,
        registerable_candidate_ids: registerable,
        persisted_locator_memory_ids,
        suppressions: request.suppression_plan.suppressions.clone(),
        checkpoint,
    })
}

fn replay_existing(candidates: &CandidateStore, existing_run: &pb::ExtractionRun) -> ApplyResult {
    let run_id = existing_run.run_id.clone();
    let mut issue_ids = Vec::new();
    let mut candidate_ids = Vec::new();
    let mut registerable = Vec::new();
    for candidate in candidates.list() {
        if candidate.extraction_run_id != run_id {
            continue;
        }
        candidate_ids.push(candidate.candidate_id.clone());
        if let Some(pb::candidate::SupersededBy::SupersedingIssueId(issue_id)) =
            candidate.superseded_by
        {
            issue_ids.push(issue_id);
        }
        if existing_run.state == pb::ExtractionRunState::Running as i32
            && candidate.status == pb::CandidateStatus::Proposed as i32
            && candidate.strong_detection_proof.is_some()
        {
            registerable.push(candidate.candidate_id);
        }
    }
    ApplyResult {
        outcome: ApplyOutcome::Applied,
        request_digest: existing_run.request_digest.clone(),
        snapshot_digest: checkpoint_snapshot_digest(existing_run),
        run_id,
        run_state: existing_run.state,
        candidate_ids,
        candidate_identities: Vec::new(),
        issue_ids,
        registerable_candidate_ids: registerable,
        persisted_locator_memory_ids: Vec::new(),
        suppressions: Vec::new(),
        checkpoint: existing_run.checkpoint.clone().unwrap_or_default(),
    }
}

fn checkpoint_snapshot_digest(run: &pb::ExtractionRun) -> String {
    run.checkpoint
        .as_deref()
        .and_then(|checkpoint| serde_json::from_str::<serde_json::Value>(checkpoint).ok())
        .and_then(|value| value.get("snapshot_digest")?.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn already_issued(candidates: &CandidateStore, thread_id: &str, print: &str) -> bool {
    candidates.list().iter().any(|candidate| {
        matches!(
            pb::CandidateStatus::try_from(candidate.status),
            Ok(pb::CandidateStatus::AutoRegistered | pb::CandidateStatus::Accepted)
        ) && metadata_value(candidate, "thread_id") == Some(thread_id)
            && metadata_value(candidate, "fingerprint") == Some(print)
    })
}

fn supersede_same_thread(
    candidates: &mut CandidateStore,
    thread_id: &str,
    print: &str,
    replacement_candidate_id: &str,
    now: &pb::UtcInstant,
) {
    let matches: Vec<String> = candidates
        .list()
        .into_iter()
        .filter(|candidate| {
            candidate.status == pb::CandidateStatus::Proposed as i32
                && metadata_value(candidate, "thread_id") == Some(thread_id)
                && metadata_value(candidate, "fingerprint") == Some(print)
        })
        .map(|candidate| candidate.candidate_id)
        .collect();
    for id in matches {
        let _ = candidates.supersede_proposed(&id, replacement_candidate_id, now.clone());
    }
}

fn input_incomplete(input: &ExtractCandidateInput) -> bool {
    input.title.is_empty() || input.unfinished_reason.is_empty() || input.completion.is_empty()
}

fn locators_ok(
    input: &ExtractCandidateInput,
    window: &HashSet<&str>,
    digest_by_id: &HashMap<&str, &str>,
) -> bool {
    !input.locator_memory_ids.is_empty()
        && input
            .locator_memory_ids
            .iter()
            .all(|id| window.contains(id.as_str()) && digest_by_id.contains_key(id.as_str()))
}

fn is_exact_duplicate(
    existing: &[ExistingHint],
    candidates: &CandidateStore,
    created_id: &str,
    snapshot_digest: &str,
    print: &str,
) -> bool {
    existing.iter().any(|hint| {
        hint.snapshot_digest.as_deref() == Some(snapshot_digest)
            && fingerprint(&hint.kind, &hint.title, &hint.completion) == print
    }) || candidates.list().iter().any(|candidate| {
        candidate.status != pb::CandidateStatus::Rejected as i32
            && candidate.candidate_id != created_id
            && metadata_value(candidate, "snapshot_digest") == Some(snapshot_digest)
            && metadata_value(candidate, "fingerprint") == Some(print)
    })
}

fn is_lexical_duplicate_of(
    existing: &[ExistingHint],
    candidates: &CandidateStore,
    input: &ExtractCandidateInput,
    thread_id: &str,
    created_id: &str,
    print: &str,
) -> bool {
    existing.iter().any(|hint| {
        let same_thread_fingerprint = hint.thread_id.as_deref() == Some(thread_id)
            && fingerprint(&hint.kind, &hint.title, &hint.completion) == print;
        !same_thread_fingerprint
            && is_lexical_duplicate(
                &input.kind,
                &input.title,
                &input.completion,
                &hint.kind,
                &hint.title,
                &hint.completion,
            )
    }) || candidates.list().iter().any(|other| {
        other.status != pb::CandidateStatus::Rejected as i32
            && other.candidate_id != created_id
            && metadata_value(other, "fingerprint") != Some(print)
            && is_lexical_duplicate(
                &input.kind,
                &input.title,
                &input.completion,
                &kind_name(other.kind),
                &other.title,
                other.acceptance_criteria.first().unwrap_or(&String::new()),
            )
    })
}

fn is_strong(
    policy: &pb::ExtractionPolicy,
    input: &ExtractCandidateInput,
    mixed: bool,
    duplicate: bool,
) -> bool {
    policy.auto_register_min_confidence.is_some()
        && !mixed
        && !duplicate
        && priority_in_range(
            parse_priority(&input.suggested_priority).unwrap_or_default(),
            policy.auto_register_priority_range.as_ref(),
        )
        && (policy.allowed_kinds.is_empty()
            || policy
                .allowed_kinds
                .iter()
                .any(|allowed| *allowed == parse_kind(&input.kind).unwrap_or_default()))
        && (!policy.explicit_only || input.detection == "EXPLICIT")
        && input.confidence >= policy.auto_register_min_confidence.unwrap_or_default()
}

fn strong_proof(
    request: &PreparedRequest,
    policy: &pb::ExtractionPolicy,
    input: &ExtractCandidateInput,
) -> pb::StrongDetectionProof {
    let factor = pb::ConfidenceFactor {
        model_id: request.model_id.clone(),
        model_version: request.prompt_version.clone(),
        rule_version: format!("policy-{}", policy.version),
        score: input.confidence,
        threshold: policy.auto_register_min_confidence.unwrap_or_default(),
        ..Default::default()
    };
    pb::StrongDetectionProof {
        model_id: request.model_id.clone(),
        model_version: request.prompt_version.clone(),
        rule_version: format!("policy-{}", policy.version),
        confidence: input.confidence,
        threshold: policy.auto_register_min_confidence.unwrap_or_default(),
        confidence_factors: vec![factor],
        raw_memory_verified: true,
        unfinished_work_verified: true,
        no_duplicate_verified: true,
    }
}

pub(crate) fn request_digest(request: &ApplyRequest) -> Result<String, String> {
    prepare_request(request.clone())
        .map(|prepared| prepared.request_digest)
        .map_err(ApplyError::message)
}

fn prepare_request(request: ApplyRequest) -> Result<PreparedRequest, ApplyError> {
    if request.thread_id.is_empty()
        || request.summary_ref.is_empty()
        || request.extract_schema.is_empty()
        || request.idempotency_key.is_empty()
    {
        return Err(ApplyError::InvalidArgument(
            "thread_id, summary_ref, extract_schema and idempotency_key are required".into(),
        ));
    }
    let policy = normalize_policy(request.policy.clone()).map_err(ApplyError::InvalidArgument)?;
    let source_labels = super::source_labels::canonical_labels(&request.source_labels)
        .map_err(ApplyError::InvalidArgument)?;
    if request
        .failure_reason
        .as_ref()
        .is_some_and(|reason| reason.trim().is_empty())
    {
        return Err(ApplyError::InvalidArgument(
            "failure_reason must not be empty when present".into(),
        ));
    }
    if request.failure_reason.is_some()
        && (!policy.extraction_enabled || !request.candidates.is_empty())
    {
        return Err(ApplyError::InvalidArgument(
            "failure_reason requires enabled extraction and zero candidates".into(),
        ));
    }
    validate_messages(&request.allowed_messages, &request.input_message_ids)
        .map_err(ApplyError::InvalidArgument)?;
    validate_unsuppress_ids(&request.rejected_candidate_ids_to_unsuppress)
        .map_err(ApplyError::InvalidArgument)?;
    for candidate in &request.candidates {
        validate_candidate(
            candidate,
            &request.allowed_messages,
            &request.input_message_ids,
        )
        .map_err(ApplyError::InvalidArgument)?;
    }
    let snapshot_digest = calculated_snapshot_digest(
        &request.summary_ref,
        &request.allowed_messages,
        &request.input_message_ids,
    );
    if !request.snapshot_digest.is_empty()
        && request.snapshot_digest != "workflow"
        && request.snapshot_digest != snapshot_digest
    {
        return Err(ApplyError::InvalidArgument(
            "snapshot_digest does not match the supplied snapshot".into(),
        ));
    }
    let policy_digest = digest_proto(&policy);
    let request_digest = canonical_request_digest(&request, &policy, &snapshot_digest);
    let mut prepared = PreparedRequest {
        thread_id: request.thread_id,
        summary_ref: request.summary_ref,
        snapshot_digest,
        extract_schema: request.extract_schema,
        policy,
        policy_digest,
        request_digest,
        model_id: request.model_id,
        prompt_version: request.prompt_version,
        allowed_messages: request.allowed_messages,
        input_message_ids: request.input_message_ids,
        candidates: request.candidates,
        mixed: request.mixed,
        failure_reason: request.failure_reason,
        source_labels,
        rejected_candidate_ids_to_unsuppress: request.rejected_candidate_ids_to_unsuppress,
        rejected_candidate_identities: request.rejected_candidate_identities,
        suppression_plan: SuppressionPlan::default(),
    };
    prepared.suppression_plan =
        rejected_exact_suppression_plan(&prepared).map_err(ApplyError::InvalidArgument)?;
    Ok(prepared)
}

fn validate_unsuppress_ids(ids: &[String]) -> Result<(), String> {
    let mut seen = HashSet::new();
    if ids
        .iter()
        .any(|candidate_id| candidate_id.is_empty() || !seen.insert(candidate_id.as_str()))
    {
        return Err(
            "rejected_candidate_ids_to_unsuppress contains an empty or duplicate ID".into(),
        );
    }
    Ok(())
}

fn normalize_policy(policy: Option<pb::ExtractionPolicy>) -> Result<pb::ExtractionPolicy, String> {
    let mut policy = policy.ok_or_else(|| "policy is required".to_string())?;
    if policy.version != 1 {
        return Err("policy version is unsupported".into());
    }
    if policy.extract_cap == 0 {
        return Err("extract_cap must be positive".into());
    }
    if let Some(threshold) = policy.auto_register_min_confidence {
        if !threshold.is_finite() || !(0.0..=1.0).contains(&threshold) {
            return Err("auto_register_min_confidence must be finite and between 0 and 1".into());
        }
    }
    let range = policy
        .auto_register_priority_range
        .take()
        .unwrap_or(pb::ExtractionPriorityRange {
            lower: pb::CandidatePriority::P0 as i32,
            upper: pb::CandidatePriority::P2 as i32,
        });
    let lower = canonical_priority(range.lower)?;
    let upper = canonical_priority(range.upper)?;
    if lower > upper {
        return Err("auto_register_priority_range is invalid".into());
    }
    policy.auto_register_priority_range = Some(pb::ExtractionPriorityRange { lower, upper });
    let mut kinds = policy.allowed_kinds.clone();
    for kind in &kinds {
        canonical_kind(*kind)?;
    }
    kinds.sort_unstable();
    kinds.dedup();
    if kinds.len() != policy.allowed_kinds.len() {
        return Err("allowed_kinds must not contain duplicates".into());
    }
    policy.allowed_kinds = kinds;
    Ok(policy)
}

fn canonical_priority(value: i32) -> Result<i32, String> {
    match pb::CandidatePriority::try_from(value) {
        Ok(
            pb::CandidatePriority::P0
            | pb::CandidatePriority::P1
            | pb::CandidatePriority::P2
            | pb::CandidatePriority::P3
            | pb::CandidatePriority::P4,
        ) => Ok(value),
        _ => Err("priority is invalid".into()),
    }
}

fn canonical_kind(value: i32) -> Result<(), String> {
    match pb::CandidateKind::try_from(value) {
        Ok(
            pb::CandidateKind::Task
            | pb::CandidateKind::Bug
            | pb::CandidateKind::Research
            | pb::CandidateKind::Decision,
        ) => Ok(()),
        _ => Err("allowed_kinds contains an invalid kind".into()),
    }
}

fn validate_messages(
    allowed_messages: &[(String, String)],
    input_message_ids: &[String],
) -> Result<(), String> {
    let mut ids = HashSet::new();
    for (id, digest) in allowed_messages {
        if id.is_empty() || digest.is_empty() || !ids.insert(id.as_str()) {
            return Err("allowed_messages contains an invalid or duplicate message".into());
        }
    }
    let mut inputs = HashSet::new();
    for id in input_message_ids {
        if id.is_empty() || !inputs.insert(id.as_str()) || !ids.contains(id.as_str()) {
            return Err("input_message_ids contains an invalid or duplicate message".into());
        }
    }
    Ok(())
}

fn validate_candidate(
    input: &ExtractCandidateInput,
    allowed_messages: &[(String, String)],
    input_message_ids: &[String],
) -> Result<(), String> {
    if input_incomplete(input)
        || !input.confidence.is_finite()
        || !(0.0..=1.0).contains(&input.confidence)
    {
        return Err("candidate is incomplete or has invalid confidence".into());
    }
    parse_kind(&input.kind)?;
    parse_priority(&input.suggested_priority)?;
    parse_state(&input.suggested_state)?;
    parse_detection(&input.detection)?;
    let window: HashSet<&str> = input_message_ids.iter().map(String::as_str).collect();
    let allowed: HashSet<&str> = allowed_messages.iter().map(|(id, _)| id.as_str()).collect();
    let mut locators = HashSet::new();
    if input.locator_memory_ids.is_empty()
        || input.locator_memory_ids.iter().any(|id| {
            id.is_empty()
                || !locators.insert(id.as_str())
                || !window.contains(id.as_str())
                || !allowed.contains(id.as_str())
        })
    {
        return Err("candidate locator is invalid".into());
    }
    Ok(())
}

fn calculated_snapshot_digest(
    summary_ref: &str,
    allowed_messages: &[(String, String)],
    input_message_ids: &[String],
) -> String {
    let mut messages = allowed_messages.to_vec();
    messages.sort_unstable();
    digest_json(&json!({
        "version": 1,
        "summary_ref": summary_ref,
        "input_message_ids": input_message_ids,
        "allowed_messages": messages,
    }))
}

fn canonical_request_digest(
    request: &ApplyRequest,
    policy: &pb::ExtractionPolicy,
    snapshot_digest: &str,
) -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(canonical_request_envelope(request, policy, snapshot_digest))
    )
}

fn canonical_request_envelope(
    request: &ApplyRequest,
    policy: &pb::ExtractionPolicy,
    snapshot_digest: &str,
) -> Vec<u8> {
    let mut messages = request.allowed_messages.clone();
    messages.sort_unstable();
    let mut envelope = b"issue-tracker/extraction-apply-request/v1\0".to_vec();
    append_envelope_string(&mut envelope, "thread_id", &request.thread_id);
    append_envelope_string(&mut envelope, "summary_ref", &request.summary_ref);
    append_envelope_string(&mut envelope, "snapshot_digest", snapshot_digest);
    append_envelope_string(&mut envelope, "extract_schema", &request.extract_schema);
    append_extraction_policy(&mut envelope, policy);
    append_envelope_string(&mut envelope, "model_id", &request.model_id);
    append_envelope_string(&mut envelope, "prompt_version", &request.prompt_version);
    let source_labels = super::source_labels::canonical_labels(&request.source_labels)
        .expect("request labels are validated before digesting");
    append_envelope_u32(
        &mut envelope,
        "source_labels_count",
        source_labels.len() as u32,
    );
    for label in source_labels {
        append_envelope_string(&mut envelope, "source_label", &label);
    }
    append_envelope_u32(
        &mut envelope,
        "allowed_messages_count",
        messages.len() as u32,
    );
    for (memory_id, content_digest) in messages {
        let mut message = Vec::new();
        append_envelope_string(&mut message, "memory_id", &memory_id);
        append_envelope_string(&mut message, "content_digest", &content_digest);
        append_envelope_bytes(&mut envelope, "allowed_message", &message);
    }
    append_envelope_u32(
        &mut envelope,
        "input_message_ids_count",
        request.input_message_ids.len() as u32,
    );
    for input_message_id in &request.input_message_ids {
        append_envelope_string(&mut envelope, "input_message_id", input_message_id);
    }
    append_envelope_u32(
        &mut envelope,
        "candidates_count",
        request.candidates.len() as u32,
    );
    for candidate in &request.candidates {
        let mut encoded = Vec::new();
        append_envelope_string(&mut encoded, "title", &candidate.title);
        append_envelope_string(&mut encoded, "description", &candidate.description);
        append_envelope_string(&mut encoded, "kind", &candidate.kind);
        append_envelope_string(
            &mut encoded,
            "suggested_priority",
            &candidate.suggested_priority,
        );
        append_envelope_string(&mut encoded, "suggested_state", &candidate.suggested_state);
        append_envelope_string(
            &mut encoded,
            "unfinished_reason",
            &candidate.unfinished_reason,
        );
        append_envelope_string(&mut encoded, "completion", &candidate.completion);
        append_envelope_string(&mut encoded, "detection", &candidate.detection);
        append_envelope_bytes(
            &mut encoded,
            "confidence",
            &candidate.confidence.to_bits().to_be_bytes(),
        );
        append_envelope_u32(
            &mut encoded,
            "locator_memory_ids_count",
            candidate.locator_memory_ids.len() as u32,
        );
        for locator in &candidate.locator_memory_ids {
            append_envelope_string(&mut encoded, "locator_memory_id", locator);
        }
        append_envelope_bytes(&mut envelope, "candidate", &encoded);
    }
    let mut unsuppress_ids = request.rejected_candidate_ids_to_unsuppress.clone();
    unsuppress_ids.sort_unstable();
    append_envelope_u32(
        &mut envelope,
        "rejected_candidate_ids_to_unsuppress_count",
        unsuppress_ids.len() as u32,
    );
    for candidate_id in unsuppress_ids {
        append_envelope_string(
            &mut envelope,
            "rejected_candidate_id_to_unsuppress",
            &candidate_id,
        );
    }
    append_envelope_bytes(&mut envelope, "mixed", &[u8::from(request.mixed)]);
    match &request.failure_reason {
        Some(reason) => {
            append_envelope_bytes(&mut envelope, "failure_reason_present", &[1]);
            append_envelope_string(&mut envelope, "failure_reason", reason);
        }
        None => append_envelope_bytes(&mut envelope, "failure_reason_present", &[0]),
    }
    envelope
}

fn append_extraction_policy(envelope: &mut Vec<u8>, policy: &pb::ExtractionPolicy) {
    let mut encoded = Vec::new();
    append_envelope_u32(&mut encoded, "version", policy.version);
    append_envelope_bytes(
        &mut encoded,
        "extraction_enabled",
        &[u8::from(policy.extraction_enabled)],
    );
    match policy.auto_register_min_confidence {
        Some(threshold) => {
            append_envelope_bytes(&mut encoded, "auto_register_min_confidence_present", &[1]);
            append_envelope_bytes(
                &mut encoded,
                "auto_register_min_confidence",
                &threshold.to_bits().to_be_bytes(),
            );
        }
        None => append_envelope_bytes(&mut encoded, "auto_register_min_confidence_present", &[0]),
    }
    let range = policy
        .auto_register_priority_range
        .as_ref()
        .expect("normalized policy has a priority range");
    append_envelope_bytes(
        &mut encoded,
        "auto_register_priority_range_lower",
        &range.lower.to_be_bytes(),
    );
    append_envelope_bytes(
        &mut encoded,
        "auto_register_priority_range_upper",
        &range.upper.to_be_bytes(),
    );
    append_envelope_bytes(
        &mut encoded,
        "explicit_only",
        &[u8::from(policy.explicit_only)],
    );
    append_envelope_u32(
        &mut encoded,
        "allowed_kinds_count",
        policy.allowed_kinds.len() as u32,
    );
    for kind in &policy.allowed_kinds {
        append_envelope_bytes(&mut encoded, "allowed_kind", &kind.to_be_bytes());
    }
    append_envelope_u32(&mut encoded, "extract_cap", policy.extract_cap);
    append_envelope_bytes(envelope, "policy", &encoded);
}

fn append_envelope_string(envelope: &mut Vec<u8>, name: &str, value: &str) {
    append_envelope_bytes(envelope, name, value.as_bytes());
}

fn append_envelope_u32(envelope: &mut Vec<u8>, name: &str, value: u32) {
    append_envelope_bytes(envelope, name, &value.to_be_bytes());
}

fn append_envelope_bytes(envelope: &mut Vec<u8>, name: &str, value: &[u8]) {
    envelope.extend_from_slice(&(name.len() as u32).to_be_bytes());
    envelope.extend_from_slice(name.as_bytes());
    envelope.extend_from_slice(&(value.len() as u64).to_be_bytes());
    envelope.extend_from_slice(value);
}

fn digest_proto(message: &impl Message) -> String {
    format!("sha256:{:x}", Sha256::digest(message.encode_to_vec()))
}

fn digest_json(value: &serde_json::Value) -> String {
    format!("sha256:{:x}", Sha256::digest(value.to_string().as_bytes()))
}

fn adopted_inputs<'a>(
    inputs: &[(usize, &'a ExtractCandidateInput)],
    cap: u32,
) -> Vec<(usize, &'a ExtractCandidateInput)> {
    let mut adopted = inputs.to_vec();
    adopted.sort_by(|(left_index, left), (right_index, right)| {
        priority_rank(&left.suggested_priority)
            .cmp(&priority_rank(&right.suggested_priority))
            .then_with(|| right.confidence.total_cmp(&left.confidence))
            .then_with(|| left_index.cmp(right_index))
    });
    adopted.truncate(cap as usize);
    adopted
}

fn rejected_exact_suppression_plan(request: &PreparedRequest) -> Result<SuppressionPlan, String> {
    let input_groups = request
        .candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| {
            (
                index,
                suppression_group(
                    &request.thread_id,
                    &request.snapshot_digest,
                    &fingerprint(&candidate.kind, &candidate.title, &candidate.completion),
                ),
            )
        })
        .collect::<Vec<_>>();
    let identities_by_id = request
        .rejected_candidate_identities
        .iter()
        .map(|identity| (identity.candidate_id.as_str(), identity))
        .collect::<HashMap<_, _>>();
    let mut bypassed_groups = HashSet::new();
    for candidate_id in &request.rejected_candidate_ids_to_unsuppress {
        let identity = identities_by_id.get(candidate_id.as_str()).ok_or_else(|| {
            "rejected_candidate_ids_to_unsuppress must reference a REJECTED candidate".to_string()
        })?;
        let group = suppression_group(
            &identity.thread_id,
            &identity.snapshot_digest,
            &identity.fingerprint,
        );
        if !input_groups
            .iter()
            .any(|(_, input_group)| input_group == &group)
        {
            return Err(
                "rejected_candidate_ids_to_unsuppress must match a current input group".into(),
            );
        }
        bypassed_groups.insert(group);
    }

    let mut rejected_by_group = HashMap::<SuppressionGroup, Vec<String>>::new();
    for identity in &request.rejected_candidate_identities {
        rejected_by_group
            .entry(suppression_group(
                &identity.thread_id,
                &identity.snapshot_digest,
                &identity.fingerprint,
            ))
            .or_default()
            .push(identity.candidate_id.clone());
    }
    let mut suppressions = Vec::new();
    let mut suppressed_input_indices = HashSet::new();
    for (input_index, group) in input_groups {
        if bypassed_groups.contains(&group) {
            continue;
        }
        let Some(mut rejected_candidate_ids) = rejected_by_group.get(&group).cloned() else {
            continue;
        };
        rejected_candidate_ids.sort_unstable();
        suppressed_input_indices.insert(input_index);
        suppressions.push(ExtractionSuppression {
            input_index: u32::try_from(input_index)
                .map_err(|_| "input index exceeds uint32".to_string())?,
            rejected_candidate_ids,
        });
    }
    Ok(SuppressionPlan {
        suppressed_input_indices,
        suppressions,
    })
}

#[derive(Default)]
struct SuppressionPlan {
    suppressed_input_indices: HashSet<usize>,
    suppressions: Vec<ExtractionSuppression>,
}

impl SuppressionPlan {
    fn excludes(&self, input_index: usize) -> bool {
        self.suppressed_input_indices.contains(&input_index)
    }

    fn audit_records(&self) -> Vec<pb::ExtractionSuppressionRecord> {
        self.suppressions
            .iter()
            .map(|suppression| pb::ExtractionSuppressionRecord {
                reason: pb::ExtractionSuppressionReason::RejectedExactMatch as i32,
                input_candidate_indices: vec![suppression.input_index],
                rejected_candidate_ids: suppression.rejected_candidate_ids.clone(),
            })
            .collect()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SuppressionGroup {
    thread_id: String,
    snapshot_digest: String,
    fingerprint: String,
}

fn suppression_group(
    thread_id: &str,
    snapshot_digest: &str,
    fingerprint: &str,
) -> SuppressionGroup {
    SuppressionGroup {
        thread_id: thread_id.into(),
        snapshot_digest: snapshot_digest.into(),
        fingerprint: fingerprint.into(),
    }
}

fn priority_in_range(priority: i32, range: Option<&pb::ExtractionPriorityRange>) -> bool {
    let Some(range) = range else {
        return false;
    };
    priority >= range.lower && priority <= range.upper
}

fn parse_kind(value: &str) -> Result<i32, String> {
    Ok(match value {
        "TASK" => pb::CandidateKind::Task as i32,
        "BUG" => pb::CandidateKind::Bug as i32,
        "RESEARCH" => pb::CandidateKind::Research as i32,
        "DECISION" => pb::CandidateKind::Decision as i32,
        _ => return Err("kind is invalid".into()),
    })
}

fn parse_priority(value: &str) -> Result<i32, String> {
    Ok(match value {
        "P0" => pb::CandidatePriority::P0 as i32,
        "P1" => pb::CandidatePriority::P1 as i32,
        "P2" => pb::CandidatePriority::P2 as i32,
        "P3" => pb::CandidatePriority::P3 as i32,
        "P4" => pb::CandidatePriority::P4 as i32,
        _ => return Err("priority is invalid".into()),
    })
}

fn parse_state(value: &str) -> Result<i32, String> {
    Ok(match value {
        "OPEN" => pb::CandidateSuggestedState::Open as i32,
        "IN_PROGRESS" => pb::CandidateSuggestedState::InProgress as i32,
        "BLOCKED" => pb::CandidateSuggestedState::Blocked as i32,
        "DEFERRED" => pb::CandidateSuggestedState::Deferred as i32,
        _ => return Err("state is invalid".into()),
    })
}

fn parse_detection(value: &str) -> Result<i32, String> {
    Ok(match value {
        "EXPLICIT" => pb::DetectionBasis::Explicit as i32,
        "INFERRED" => pb::DetectionBasis::Inferred as i32,
        "STRONG" => return Err("detection_basis STRONG is not accepted".into()),
        _ => return Err("detection is invalid".into()),
    })
}

pub(crate) fn kind_name(value: i32) -> String {
    match pb::CandidateKind::try_from(value) {
        Ok(pb::CandidateKind::Bug) => "BUG".into(),
        Ok(pb::CandidateKind::Research) => "RESEARCH".into(),
        Ok(pb::CandidateKind::Decision) => "DECISION".into(),
        _ => "TASK".into(),
    }
}

fn priority_rank(value: &str) -> u8 {
    match value {
        "P0" => 0,
        "P1" => 1,
        "P2" => 2,
        "P3" => 3,
        "P4" => 4,
        _ => 9,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> pb::UtcInstant {
        pb::UtcInstant {
            unix_epoch_millis: 1,
        }
    }

    fn policy(mode: &str) -> pb::ExtractionPolicy {
        pb::ExtractionPolicy {
            version: 1,
            extraction_enabled: true,
            auto_register_min_confidence: (mode == "AUTO_REGISTER").then_some(0.9),
            auto_register_priority_range: Some(pb::ExtractionPriorityRange {
                lower: pb::CandidatePriority::P0 as i32,
                upper: pb::CandidatePriority::P2 as i32,
            }),
            explicit_only: true,
            allowed_kinds: Vec::new(),
            extract_cap: 5,
        }
    }

    fn candidate() -> ExtractCandidateInput {
        ExtractCandidateInput {
            title: "Fix login".into(),
            description: "desc".into(),
            kind: "TASK".into(),
            suggested_priority: "P2".into(),
            suggested_state: "OPEN".into(),
            unfinished_reason: "broken".into(),
            completion: "login works".into(),
            detection: "EXPLICIT".into(),
            confidence: 0.95,
            locator_memory_ids: vec!["m1".into()],
        }
    }

    fn base_request(mode: &str) -> ApplyRequest {
        ApplyRequest {
            thread_id: "thread-1".into(),
            summary_ref: "summary:thread-1".into(),
            snapshot_digest: "workflow".into(),
            extract_schema: "lookback-issue-extract.v1".into(),
            policy: Some(policy(mode)),
            model_id: "memories-llm".into(),
            prompt_version: "v1".into(),
            allowed_messages: vec![("m1".into(), "sha256:m1".into())],
            input_message_ids: vec!["m1".into()],
            candidates: vec![candidate()],
            mixed: false,
            failure_reason: None,
            idempotency_key: "test-key".into(),
            source_labels: Vec::new(),
            rejected_candidate_ids_to_unsuppress: Vec::new(),
            rejected_candidate_identities: Vec::new(),
        }
    }

    #[test]
    fn failure_reason_writes_failed_run_without_candidates() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("AUTO_REGISTER");
        request.candidates.clear();
        request.failure_reason = Some("digest_mismatch".into());
        let result = apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
            Ok(None)
        })
        .unwrap();
        assert_eq!(result.run_state, pb::ExtractionRunState::Failed as i32);
        assert!(result.candidate_ids.is_empty());
    }

    #[test]
    fn source_labels_are_canonical_for_digest_and_saved_on_candidates() {
        let mut first = base_request("REVIEW_ONLY");
        first.source_labels = vec![
            " team:backend ".into(),
            "dir:repo".into(),
            "team:backend".into(),
        ];
        let mut second = base_request("REVIEW_ONLY");
        second.source_labels = vec!["dir:repo".into(), "team:backend".into()];
        assert_eq!(
            request_digest(&first).unwrap(),
            request_digest(&second).unwrap()
        );

        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let result = apply(&mut runs, &mut candidates, &[], first, now(), |_, _| {
            Ok(None)
        })
        .unwrap();
        let candidate = candidates.get(&result.candidate_ids[0]).unwrap();
        let snapshots =
            super::super::source_labels::snapshots_from_manager_metadata(&candidate.metadata)
                .unwrap()
                .unwrap();
        assert_eq!(snapshots[0].labels, ["dir:repo", "team:backend"]);
    }

    #[test]
    fn source_labels_reject_empty_values() {
        let mut request = base_request("REVIEW_ONLY");
        request.source_labels = vec![" \u{2003} ".into()];
        assert!(request_digest(&request).is_err());
    }

    #[test]
    fn review_only_keeps_proof_and_does_not_register() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let result = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("REVIEW_ONLY"),
            now(),
            |_, _| panic!("must not register"),
        )
        .unwrap();
        assert_eq!(result.issue_ids.len(), 0);
        let stored = candidates.get(&result.candidate_ids[0]).unwrap();
        assert!(stored.strong_detection_proof.is_none());
        assert_eq!(stored.status, pb::CandidateStatus::Proposed as i32);
    }

    #[test]
    fn disabled_policy_skips_without_creating_a_run() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("REVIEW_ONLY");
        request.policy.as_mut().unwrap().extraction_enabled = false;
        let result = apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
            panic!("disabled extraction must not register")
        })
        .unwrap();
        assert_eq!(result.outcome, ApplyOutcome::SkippedExtractionDisabled);
        assert!(runs.list().is_empty());
        assert!(candidates.list().is_empty());
    }

    #[test]
    fn rejected_identity_exact_group_suppresses_before_cap_and_persistence() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("REVIEW_ONLY");
        request.policy.as_mut().expect("policy").extract_cap = 1;
        let mut second = candidate();
        second.title = "Independent task".into();
        second.completion = "independent completion".into();
        request.candidates.push(second);
        request.rejected_candidate_identities = vec![ExtractionCandidateIdentity {
            candidate_id: "rejected-1".into(),
            thread_id: request.thread_id.clone(),
            snapshot_digest: calculated_snapshot_digest(
                &request.summary_ref,
                &request.allowed_messages,
                &request.input_message_ids,
            ),
            fingerprint: fingerprint(
                &request.candidates[0].kind,
                &request.candidates[0].title,
                &request.candidates[0].completion,
            ),
        }];

        let result = apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
            Ok(None)
        })
        .expect("apply succeeds");

        assert_eq!(result.candidate_ids.len(), 1);
        assert_eq!(candidates.list()[0].title, "Independent task");
        assert_eq!(result.suppressions.len(), 1);
        assert_eq!(result.suppressions[0].input_index, 0);
        assert_eq!(
            result.suppressions[0].rejected_candidate_ids,
            ["rejected-1"]
        );
    }

    #[test]
    fn unsuppressing_one_rejected_identity_bypasses_its_entire_group_once() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("REVIEW_ONLY");
        let snapshot_digest = calculated_snapshot_digest(
            &request.summary_ref,
            &request.allowed_messages,
            &request.input_message_ids,
        );
        let fingerprint = fingerprint(
            &request.candidates[0].kind,
            &request.candidates[0].title,
            &request.candidates[0].completion,
        );
        request.rejected_candidate_identities = ["rejected-1", "rejected-2"]
            .into_iter()
            .map(|candidate_id| ExtractionCandidateIdentity {
                candidate_id: candidate_id.into(),
                thread_id: request.thread_id.clone(),
                snapshot_digest: snapshot_digest.clone(),
                fingerprint: fingerprint.clone(),
            })
            .collect();
        request.rejected_candidate_ids_to_unsuppress = vec!["rejected-1".into()];

        let result = apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
            Ok(None)
        })
        .expect("one-run bypass succeeds");

        assert_eq!(result.candidate_ids.len(), 1);
        assert!(result.suppressions.is_empty());
    }

    #[test]
    fn unsuppress_rejects_empty_duplicate_and_non_matching_identity_before_run_creation() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("REVIEW_ONLY");
        request.rejected_candidate_ids_to_unsuppress = vec!["".into(), "".into()];

        assert!(
            apply(&mut runs, &mut candidates, &[], request, now(), |_, _| Ok(
                None
            ))
            .is_err()
        );
        assert!(runs.list().is_empty());
        assert!(candidates.list().is_empty());

        let mut request = base_request("REVIEW_ONLY");
        request.rejected_candidate_ids_to_unsuppress = vec!["unknown-rejected".into()];
        assert!(
            apply(&mut runs, &mut candidates, &[], request, now(), |_, _| Ok(
                None
            ))
            .is_err()
        );
        assert!(runs.list().is_empty());
        assert!(candidates.list().is_empty());
    }

    #[test]
    fn cap_uses_priority_confidence_and_input_order_and_persists_metadata() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("REVIEW_ONLY");
        request.policy.as_mut().unwrap().extract_cap = 2;
        let mut p1 = candidate();
        p1.title = "P1 low confidence".into();
        p1.suggested_priority = "P1".into();
        p1.confidence = 0.91;
        let mut p0_low = candidate();
        p0_low.title = "P0 low confidence".into();
        p0_low.suggested_priority = "P0".into();
        p0_low.confidence = 0.91;
        let mut p0_high = candidate();
        p0_high.title = "P0 high confidence".into();
        p0_high.suggested_priority = "P0".into();
        p0_high.confidence = 0.99;
        request.candidates = vec![p1, p0_low, p0_high];
        let result = apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
            Ok(None)
        })
        .unwrap();
        assert_eq!(result.candidate_ids, ["run-1-2", "run-1-1"]);
        let run = runs.get(&result.run_id).unwrap();
        assert_eq!(run.policy.as_ref().unwrap().extract_cap, 2);
        assert_eq!(run.truncation.unwrap().adopted_input_indices, [2, 1]);
    }

    #[test]
    fn allowed_kind_limits_auto_registration_without_losing_candidate() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("AUTO_REGISTER");
        request.policy.as_mut().unwrap().allowed_kinds = vec![pb::CandidateKind::Bug as i32];
        let result = apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
            panic!("disallowed kind must not register")
        })
        .unwrap();
        assert_eq!(result.candidate_ids.len(), 1);
        assert!(result.issue_ids.is_empty());
        assert!(
            candidates
                .get(&result.candidate_ids[0])
                .unwrap()
                .strong_detection_proof
                .is_none()
        );
    }

    #[test]
    fn auto_register_without_callback_lists_registerable_candidates() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let result = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("AUTO_REGISTER"),
            now(),
            |_, _| Ok(None),
        )
        .unwrap();
        assert_eq!(result.registerable_candidate_ids.len(), 1);
        assert!(result.issue_ids.is_empty());
        assert_eq!(result.run_state, pb::ExtractionRunState::Running as i32);
    }

    #[test]
    fn auto_register_creates_issue_when_strong() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let result = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("AUTO_REGISTER"),
            now(),
            |_, thread| {
                assert_eq!(thread, "thread-1");
                Ok(Some("issue-1".into()))
            },
        )
        .unwrap();
        assert_eq!(result.issue_ids, ["issue-1"]);
        let stored = candidates.get(&result.candidate_ids[0]).unwrap();
        assert_eq!(stored.status, pb::CandidateStatus::AutoRegistered as i32);
    }

    #[test]
    fn inferred_is_not_registered_when_explicit_only() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("AUTO_REGISTER");
        request.candidates[0].detection = "INFERRED".into();
        let result = apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
            panic!("must not register")
        })
        .unwrap();
        assert!(result.issue_ids.is_empty());
        assert_eq!(result.candidate_ids.len(), 1);
    }

    #[test]
    fn replay_same_digest_does_not_create_another_run() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let first = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("REVIEW_ONLY"),
            now(),
            |_, _| Ok(None),
        )
        .unwrap();
        let second = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("REVIEW_ONLY"),
            now(),
            |_, _| Ok(None),
        )
        .unwrap();
        assert_eq!(first.run_id, second.run_id);
        assert_eq!(runs.list().len(), 1);
    }

    #[test]
    fn request_digest_envelope_is_versioned_and_length_delimited() {
        let request = base_request("REVIEW_ONLY");
        let policy = normalize_policy(request.policy.clone()).expect("valid policy");
        let snapshot = calculated_snapshot_digest(
            &request.summary_ref,
            &request.allowed_messages,
            &request.input_message_ids,
        );
        let envelope = canonical_request_envelope(&request, &policy, &snapshot);
        assert!(envelope.starts_with(b"issue-tracker/extraction-apply-request/v1\0"));

        let mut ambiguous_if_concatenated = request.clone();
        ambiguous_if_concatenated.thread_id = "thread-12".into();
        ambiguous_if_concatenated.summary_ref = "summary:".into();
        let other = canonical_request_envelope(&ambiguous_if_concatenated, &policy, &snapshot);
        assert_ne!(envelope, other);
    }

    #[test]
    fn invalid_locator_rejects_without_persistence() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("AUTO_REGISTER");
        request.candidates.push(ExtractCandidateInput {
            title: "Outside".into(),
            locator_memory_ids: vec!["missing".into()],
            ..candidate()
        });
        assert!(
            apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
                panic!("must not register invalid input")
            })
            .is_err()
        );
        assert!(runs.list().is_empty());
        assert!(candidates.list().is_empty());
    }

    #[test]
    fn lexical_duplicate_and_over_cap_priority_stay_proposed() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("AUTO_REGISTER");
        request.candidates[0].suggested_priority = "P3".into();
        let result = apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
            panic!("must not register over-cap priority")
        })
        .unwrap();
        assert!(result.issue_ids.is_empty());

        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let result = apply(
            &mut runs,
            &mut candidates,
            &[ExistingHint {
                thread_id: Some("thread-other".into()),
                kind: "TASK".into(),
                title: "Fix login!".into(),
                completion: "login works.".into(),
                snapshot_digest: Some("sha256:other".into()),
            }],
            base_request("AUTO_REGISTER"),
            now(),
            |_, _| panic!("must not register lexical duplicate"),
        )
        .unwrap();
        assert!(result.issue_ids.is_empty());
        assert_eq!(result.candidate_ids.len(), 1);
    }

    #[test]
    fn same_thread_fingerprint_supersede_can_still_register() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let first = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("REVIEW_ONLY"),
            now(),
            |_, _| Ok(None),
        )
        .unwrap();
        let mut second_request = base_request("AUTO_REGISTER");
        second_request.allowed_messages[0].1 = "sha256:m1-other".into();
        let existing: Vec<ExistingHint> = candidates
            .list()
            .into_iter()
            .map(|candidate| ExistingHint {
                thread_id: metadata_value(&candidate, "thread_id").map(str::to_string),
                snapshot_digest: metadata_value(&candidate, "snapshot_digest").map(str::to_string),
                kind: kind_name(candidate.kind),
                title: candidate.title,
                completion: candidate
                    .acceptance_criteria
                    .first()
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect();
        let second = apply(
            &mut runs,
            &mut candidates,
            &existing,
            second_request,
            now(),
            |_, _| Ok(Some("issue-2".into())),
        )
        .unwrap();
        assert_eq!(second.issue_ids, ["issue-2"]);
        let old = candidates.get(&first.candidate_ids[0]).unwrap();
        assert_eq!(old.status, pb::CandidateStatus::Superseded as i32);
    }

    #[test]
    fn strong_detection_input_is_rejected() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("AUTO_REGISTER");
        request.candidates[0].detection = "STRONG".into();
        assert!(
            apply(&mut runs, &mut candidates, &[], request, now(), |_, _| Ok(
                None
            ),)
            .is_err()
        );
    }

    #[test]
    fn failed_run_does_not_block_later_success_on_same_digest() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut failed = base_request("AUTO_REGISTER");
        failed.candidates.clear();
        failed.failure_reason = Some("digest_mismatch".into());
        let first = apply(&mut runs, &mut candidates, &[], failed, now(), |_, _| {
            Ok(None)
        })
        .unwrap();
        assert_eq!(first.run_state, pb::ExtractionRunState::Failed as i32);

        let second = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("REVIEW_ONLY"),
            now(),
            |_, _| panic!("must not register"),
        )
        .unwrap();
        assert_ne!(first.run_id, second.run_id);
        assert_eq!(second.candidate_ids.len(), 1);
        assert_eq!(runs.list().len(), 2);

        let third = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("REVIEW_ONLY"),
            now(),
            |_, _| panic!("must not register"),
        )
        .unwrap();
        assert_eq!(third.run_id, second.run_id);
        assert_eq!(runs.list().len(), 2);
    }

    #[test]
    fn auto_registered_same_thread_fingerprint_is_saved_but_not_registered_again() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let first = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("AUTO_REGISTER"),
            now(),
            |_, _| Ok(Some("issue-1".into())),
        )
        .unwrap();
        assert_eq!(first.issue_ids, ["issue-1"]);

        let mut second_request = base_request("AUTO_REGISTER");
        second_request.allowed_messages[0].1 = "sha256:m1-other".into();
        let second = apply(
            &mut runs,
            &mut candidates,
            &[],
            second_request,
            now(),
            |_, _| panic!("must not register an already issued fingerprint"),
        )
        .unwrap();
        assert!(second.issue_ids.is_empty());
        assert_eq!(second.candidate_ids.len(), 1);
        assert_eq!(candidates.list().len(), 2);
        assert!(
            candidates
                .get(&second.candidate_ids[0])
                .unwrap()
                .strong_detection_proof
                .is_none()
        );
    }

    #[test]
    fn invalid_candidate_rejects_without_persistence() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let mut request = base_request("REVIEW_ONLY");
        request.mixed = true;
        request.candidates.insert(
            0,
            ExtractCandidateInput {
                title: String::new(),
                locator_memory_ids: vec!["m1".into()],
                ..candidate()
            },
        );
        assert!(
            apply(&mut runs, &mut candidates, &[], request, now(), |_, _| {
                panic!("must not register invalid input")
            })
            .is_err()
        );
        assert!(runs.list().is_empty());
        assert!(candidates.list().is_empty());
    }

    #[test]
    fn supersede_writes_replacement_candidate_id() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let first = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("REVIEW_ONLY"),
            now(),
            |_, _| Ok(None),
        )
        .unwrap();
        let mut second_request = base_request("AUTO_REGISTER");
        second_request.allowed_messages[0].1 = "sha256:m1-other".into();
        let second = apply(
            &mut runs,
            &mut candidates,
            &[],
            second_request,
            now(),
            |_, _| Ok(Some("issue-2".into())),
        )
        .unwrap();
        let old = candidates.get(&first.candidate_ids[0]).unwrap();
        assert_eq!(
            old.superseded_by,
            Some(pb::candidate::SupersededBy::SupersedingCandidateId(
                second.candidate_ids[0].clone()
            ))
        );
    }

    #[test]
    fn replay_running_auto_register_returns_same_registerable() {
        let mut runs = ExtractionRunStore::default();
        let mut candidates = CandidateStore::default();
        let first = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("AUTO_REGISTER"),
            now(),
            |_, _| Ok(None),
        )
        .unwrap();
        let second = apply(
            &mut runs,
            &mut candidates,
            &[],
            base_request("AUTO_REGISTER"),
            now(),
            |_, _| panic!("replay must not create another issue"),
        )
        .unwrap();
        assert_eq!(first.run_id, second.run_id);
        assert_eq!(
            second.registerable_candidate_ids,
            first.registerable_candidate_ids
        );
        assert_eq!(runs.list().len(), 1);
    }

    fn persist_storage() -> super::super::storage::ManagerStorage {
        let mut settings = crate::manager::config::tests::settings();
        settings.maximum_payload_bytes = 65_536;
        settings.maximum_repeated_item_count = 100;
        if let Some(limits) = settings.resource_limits.as_mut() {
            limits.max_string_bytes = 4096;
            limits.max_body_bytes = 65_536;
            limits.max_uri_bytes = 4096;
            limits.max_repeated_item_count = 100;
            limits.max_page_size = 100;
        }
        let config = crate::manager::config::ManagerConfig::from_proto(&settings).unwrap();
        super::super::storage::ManagerStorage::open(&config).unwrap()
    }

    #[test]
    fn persist_apply_uses_rejected_identity_projection_and_records_suppression_audit() {
        let mut storage = persist_storage();
        let first = persist_apply(&mut storage, base_request("REVIEW_ONLY"))
            .expect("persist initial extraction");
        {
            let mut repo = super::super::repository::ManagerRepository::new(&mut storage);
            let mut candidates = CandidateStore::from_candidates(repo.candidates().unwrap());
            let revision = candidates
                .get(&first.candidate_ids[0])
                .expect("created candidate")
                .revision;
            let rejected = candidates
                .reject(
                    &first.candidate_ids[0],
                    revision,
                    pb::CandidateRejectionReason::Other as i32,
                    "not applicable",
                    now(),
                )
                .expect("reject candidate");
            repo.put_proto(
                super::super::repository::EntityKind::Candidate,
                &rejected.candidate_id,
                rejected.revision,
                "issue_tracker.manager.v1.Candidate",
                &rejected,
            )
            .expect("persist rejection");
        }

        let mut retry = base_request("REVIEW_ONLY");
        retry.idempotency_key = "new-key".into();
        retry.prompt_version = "v2".into();
        {
            let repo = super::super::repository::ManagerRepository::new(&mut storage);
            let identities = repo
                .rejected_extraction_identities()
                .expect("load rejected identities");
            assert_eq!(identities.len(), 1);
            assert_eq!(identities[0].thread_id, "thread-1");
            assert_eq!(
                identities[0].fingerprint,
                fingerprint("TASK", "Fix login", "login works")
            );
        }
        let suppressed = persist_apply(&mut storage, retry).expect("persist suppressed extraction");

        assert!(suppressed.candidate_ids.is_empty());
        assert_eq!(suppressed.suppressions.len(), 1);
        assert_eq!(
            storage
                .raw_query_i64("SELECT COUNT(*) FROM manager_extraction_suppression_audit")
                .expect("suppression audit count"),
            1
        );
        assert_eq!(
            storage
                .raw_query_i64(
                    "SELECT COUNT(*) FROM manager_extraction_suppression_rejected_candidate"
                )
                .expect("suppression candidate audit count"),
            1
        );
    }

    #[test]
    fn persist_apply_copies_only_adopted_locator_refs_to_candidates() {
        let mut storage = persist_storage();
        let mut request = base_request("REVIEW_ONLY");
        request.policy.as_mut().unwrap().extract_cap = 1;
        request
            .allowed_messages
            .push(("m2".into(), "sha256:m2".into()));
        request.input_message_ids.push("m2".into());
        let mut higher_priority = candidate();
        higher_priority.title = "Urgent login fix".into();
        higher_priority.suggested_priority = "P0".into();
        higher_priority.locator_memory_ids = vec!["m2".into()];
        request.candidates.push(higher_priority);

        let result = persist_apply(&mut storage, request).expect("persist capped extraction");
        let repo = super::super::repository::ManagerRepository::new(&mut storage);
        assert_eq!(result.candidate_ids, ["run-1-1"]);
        let candidates = repo.candidates().expect("read candidates");
        assert_eq!(candidates[0].source_evidence[0].memory_id, "m2");
    }

    #[test]
    fn persist_apply_records_disabled_skip_without_extraction_entities() {
        let mut storage = persist_storage();
        let mut request = base_request("REVIEW_ONLY");
        request.policy.as_mut().unwrap().extraction_enabled = false;

        let result = persist_apply(&mut storage, request).expect("persist skipped extraction");
        assert_eq!(result.outcome, ApplyOutcome::SkippedExtractionDisabled);
        {
            let repo = super::super::repository::ManagerRepository::new(&mut storage);
            assert!(repo.extraction_runs().expect("read runs").is_empty());
            assert!(repo.candidates().expect("read candidates").is_empty());
        }
        assert_eq!(
            storage
                .raw_query_i64("SELECT COUNT(*) FROM manager_extraction_apply_outcome")
                .expect("count replay outcomes"),
            1
        );
    }

    #[test]
    fn persist_complete_run_after_marked_replay_completes() {
        let mut storage = persist_storage();
        let first = persist_apply(&mut storage, base_request("AUTO_REGISTER")).unwrap();
        assert_eq!(first.run_state, pb::ExtractionRunState::Running as i32);
        assert_eq!(first.registerable_candidate_ids.len(), 1);
        persist_mark_registered(
            &mut storage,
            &first.registerable_candidate_ids[0],
            "issue-1",
        )
        .unwrap();
        persist_mark_registered(
            &mut storage,
            &first.registerable_candidate_ids[0],
            "issue-1",
        )
        .unwrap();
        let replay = persist_apply(&mut storage, base_request("AUTO_REGISTER")).unwrap();
        assert_eq!(replay.run_id, first.run_id);
        assert!(replay.registerable_candidate_ids.is_empty());
        let completed =
            persist_complete_run(&mut storage, &replay.run_id, &replay.checkpoint).unwrap();
        assert_eq!(completed, pb::ExtractionRunState::Completed as i32);
    }
}
