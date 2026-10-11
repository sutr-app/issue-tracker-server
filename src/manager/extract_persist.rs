use super::candidates::CandidateStore;
use super::extract_apply::{ApplyError, ApplyOutcome, ApplyRequest, ApplyResult, apply};
use super::extraction::ExtractionRunStore;
use super::repository::{
    EntityInput, EntityKind, ExtractionApplyPersistence, ExtractionApplyReplayDecision,
    ManagerRepository,
};
use super::storage::ManagerStorage;
use crate::proto_types::manager_v1 as pb;
use prost::Message;

#[derive(Debug)]
pub(crate) enum PersistApplyError {
    InvalidArgument(String),
    IdempotencyConflict,
    Failed(String),
}

impl PersistApplyError {
    fn from_apply_error(error: ApplyError) -> Self {
        match error {
            ApplyError::InvalidArgument(message) => Self::InvalidArgument(message),
            ApplyError::Failed(message) => Self::Failed(message),
        }
    }

    fn failed(error: impl ToString) -> Self {
        Self::Failed(error.to_string())
    }
}

pub fn persist_apply(
    storage: &mut ManagerStorage,
    mut request: ApplyRequest,
) -> Result<ApplyResult, PersistApplyError> {
    let source_evidence_limits = storage.source_evidence_limits();
    let mut repo = ManagerRepository::new(storage);
    request.rejected_candidate_identities = repo
        .rejected_extraction_identities()
        .map_err(PersistApplyError::failed)?;
    let mut runs = ExtractionRunStore::from_runs(
        repo.extraction_runs()
            .map_err(|error| PersistApplyError::Failed(error.message))?,
    );
    let mut candidates = CandidateStore::from_candidates_with_limits(
        repo.candidates()
            .map_err(|error| PersistApplyError::Failed(error.message))?,
        source_evidence_limits,
    );
    let idempotency_key = request.idempotency_key.clone();
    let rejected_identities = request.rejected_candidate_identities.clone();
    let result = apply(
        &mut runs,
        &mut candidates,
        &[],
        request,
        utc_now(),
        |_, _| Ok(None),
    )
    .map_err(PersistApplyError::from_apply_error)?;
    let mut items = Vec::new();
    for run in runs.list() {
        items.push(entity_item(
            EntityKind::ExtractionRun,
            run.run_id.clone(),
            run.revision,
            "issue_tracker.manager.v1.ExtractionRun",
            &run,
        ));
    }
    for candidate in candidates.list() {
        items.push(entity_item(
            EntityKind::Candidate,
            candidate.candidate_id.clone(),
            candidate.revision,
            "issue_tracker.manager.v1.Candidate",
            &candidate,
        ));
    }
    let mut projection_identities = result.candidate_identities.clone();
    projection_identities.extend(rejected_identities);
    let (outcome, run_id, skip_reason) = match result.outcome {
        ApplyOutcome::Applied => (
            EXTRACTION_APPLY_OUTCOME_APPLIED,
            Some(result.run_id.as_str()),
            None,
        ),
        ApplyOutcome::SkippedExtractionDisabled => (
            EXTRACTION_APPLY_OUTCOME_SKIPPED,
            None,
            Some(EXTRACTION_APPLY_SKIP_REASON_DISABLED),
        ),
    };
    match repo
        .put_changed_entities_with_extraction_apply_replay(ExtractionApplyPersistence {
            items,
            extraction_identities: &projection_identities,
            run_id_for_suppressions: &result.run_id,
            suppressions: &result.suppressions,
            idempotency_key: &idempotency_key,
            request_digest: &result.request_digest,
            outcome,
            run_id,
            skip_reason,
        })
        .map_err(PersistApplyError::failed)?
    {
        ExtractionApplyReplayDecision::New | ExtractionApplyReplayDecision::Replay { .. } => {
            Ok(result)
        }
        ExtractionApplyReplayDecision::Conflict => Err(PersistApplyError::IdempotencyConflict),
    }
}

const EXTRACTION_APPLY_OUTCOME_APPLIED: i32 = 1;
const EXTRACTION_APPLY_OUTCOME_SKIPPED: i32 = 2;
const EXTRACTION_APPLY_SKIP_REASON_DISABLED: i32 = 1;

pub fn persist_mark_registered(
    storage: &mut ManagerStorage,
    candidate_id: &str,
    issue_id: &str,
) -> Result<(), String> {
    let source_evidence_limits = storage.source_evidence_limits();
    let mut repo = ManagerRepository::new(storage);
    let mut candidates = CandidateStore::from_candidates_with_limits(
        repo.candidates().map_err(|error| error.message.clone())?,
        source_evidence_limits,
    );
    let candidate = candidates
        .get(candidate_id)
        .map_err(|error| format!("{error:?}"))?;
    if candidate.status == pb::CandidateStatus::AutoRegistered as i32 {
        let same_issue = matches!(
            candidate.superseded_by.as_ref(),
            Some(pb::candidate::SupersededBy::SupersedingIssueId(existing))
                if existing == issue_id
        );
        return if same_issue {
            Ok(())
        } else {
            Err("candidate is already registered to a different issue".into())
        };
    }
    let proof = candidate.strong_detection_proof.clone();
    candidates
        .record_extraction_outcome(candidate_id, proof, Some(issue_id.into()), utc_now())
        .map_err(|error| format!("{error:?}"))?;
    let updated = candidates
        .get(candidate_id)
        .map_err(|error| format!("{error:?}"))?;
    repo.put_proto(
        EntityKind::Candidate,
        &updated.candidate_id,
        updated.revision,
        "issue_tracker.manager.v1.Candidate",
        &updated,
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub fn persist_complete_run(
    storage: &mut ManagerStorage,
    run_id: &str,
    checkpoint: &str,
) -> Result<i32, String> {
    let mut repo = ManagerRepository::new(storage);
    let mut runs = ExtractionRunStore::from_runs(
        repo.extraction_runs()
            .map_err(|error| error.message.clone())?,
    );
    let run = runs.get(run_id).map_err(|error| format!("{error:?}"))?;
    if run.state == pb::ExtractionRunState::Completed as i32
        || run.state == pb::ExtractionRunState::Failed as i32
        || run.state == pb::ExtractionRunState::Unconfirmed as i32
    {
        return Ok(run.state);
    }
    let completed = runs
        .advance(
            run_id,
            run.revision,
            &pb::ExtractionRunAdvance {
                target_state: pb::ExtractionRunState::Completed as i32,
                progress_percent: Some(100),
                checkpoint: Some(checkpoint.to_string()),
                ..Default::default()
            },
            utc_now(),
        )
        .map_err(|error| format!("{error:?}"))?;
    repo.put_proto(
        EntityKind::ExtractionRun,
        &completed.run_id,
        completed.revision,
        "issue_tracker.manager.v1.ExtractionRun",
        &completed,
    )
    .map_err(|error| error.to_string())?;
    Ok(completed.state)
}

fn entity_item<M: Message>(
    kind: EntityKind,
    entity_id: String,
    revision: u64,
    schema: &'static str,
    payload: &M,
) -> EntityInput {
    EntityInput {
        kind,
        entity_id,
        revision,
        payload_schema: schema.into(),
        payload: payload.encode_to_vec(),
    }
}

fn utc_now() -> pb::UtcInstant {
    pb::UtcInstant {
        unix_epoch_millis: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::PersistApplyError;
    use crate::manager::extract_apply::ApplyError;

    #[test]
    fn rejected_unsuppress_errors_are_typed_as_invalid_arguments() {
        let error = PersistApplyError::from_apply_error(ApplyError::InvalidArgument(
            "rejected_candidate_ids_to_unsuppress must reference a REJECTED candidate".into(),
        ));

        assert!(matches!(error, PersistApplyError::InvalidArgument(_)));
    }
}
