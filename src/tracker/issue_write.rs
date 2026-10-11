use super::{
    actor::ActorIdentity, artifact_fs::*, artifacts::set, config::SourceEvidenceLimits,
    operation_log::*, task_mapping::*, *,
};
use crate::proto_types::manager_v1;

const DEDICATED_TRANSITION_ERROR: &str =
    "awaiting_review requires the dedicated execution completion path";
const AWAITING_REVIEW_REASON: &str = "execution completed; awaiting review";

/// The Gateway writes this correlation id and recovery verifies it, so both
/// sides must derive it from the same request digest in the same format.
pub(crate) fn awaiting_review_gateway_correlation_id(gateway_request_digest: &str) -> String {
    format!("issue-gateway-awaiting-review/v1:{gateway_request_digest}")
}

// Shared by the commit and recovery-verification paths; any divergence in the
// input order would make every recovery check fail closed.
fn awaiting_review_marker_digest(
    id: Uuid,
    expected_revision: u64,
    operation_id: &str,
    actor_id: &str,
    session_id: &str,
    correlation_id: &str,
    completion_json: &str,
) -> String {
    marker_digest(&[
        "complete_execution_to_awaiting_review",
        id.to_string().as_str(),
        expected_revision.to_string().as_str(),
        operation_id,
        actor_id,
        session_id,
        correlation_id,
        completion_json,
    ])
}

/// Return stable Issue identifiers for every unresolved dependency that can
/// be represented as a canonical Issue.  TaskChampion's dependency map is
/// authoritative for the blocked predicate; malformed or non-Issue tasks are
/// deliberately omitted from the safe diagnostic rather than exposing raw
/// storage values.
async fn open_blocker_issue_ids(
    replica: &mut taskchampion::Replica<taskchampion::SqliteStorage>,
    task: &taskchampion::Task,
) -> Result<Vec<Uuid>, TrackerError> {
    let dependency_map = replica.dependency_map(true).await?;
    let dependencies: Vec<_> = dependency_map.dependencies(task.get_uuid()).collect();
    let tasks = replica.all_tasks().await?;
    let mut issue_ids = dependencies
        .into_iter()
        .filter_map(|dependency| {
            let dependency_task = tasks.get(&dependency)?;
            let issue_id = dependency_task
                .get_user_defined_attribute(ID_KEY)
                .and_then(|value| Uuid::parse_str(value).ok())?;
            let state = dependency_task
                .get_user_defined_attribute(STATE_KEY)
                .and_then(|value| IssueState::parse(value).ok());
            if matches!(state, Some(IssueState::Done | IssueState::Canceled)) {
                None
            } else {
                Some(issue_id)
            }
        })
        .collect::<Vec<_>>();
    issue_ids.sort_unstable();
    issue_ids.dedup();
    Ok(issue_ids)
}

/// Capture the complete, immutable blocker evidence required by a dependency
/// override.  Unlike the user-facing error helper above, this path fails
/// closed when an unresolved dependency cannot be represented as a canonical
/// Issue snapshot; applying an override without that evidence would make the
/// terminal decision impossible to audit later.
async fn collect_dependency_override_observations(
    replica: &mut taskchampion::Replica<taskchampion::SqliteStorage>,
    task: &taskchampion::Task,
) -> Result<Vec<manager_v1::DependencyOverrideObservation>, TrackerError> {
    let dependency_map = replica.dependency_map(true).await?;
    let dependencies: Vec<_> = dependency_map.dependencies(task.get_uuid()).collect();
    let tasks = replica.all_tasks().await?;
    let mut observations = Vec::new();
    for dependency in dependencies {
        let dependency_task = tasks.get(&dependency).ok_or_else(|| {
            TrackerError::InvalidState("dependency blocker task is missing".into())
        })?;
        let issue_id = dependency_task
            .get_user_defined_attribute(ID_KEY)
            .ok_or_else(|| {
                TrackerError::InvalidState("dependency blocker issue ID is missing".into())
            })
            .and_then(|value| {
                Uuid::parse_str(value).map_err(|_| {
                    TrackerError::InvalidState("dependency blocker issue ID is invalid".into())
                })
            })?;
        let state = dependency_task
            .get_user_defined_attribute(STATE_KEY)
            .ok_or_else(|| TrackerError::InvalidState("dependency blocker state is missing".into()))
            .and_then(IssueState::parse)?;
        if matches!(state, IssueState::Done | IssueState::Canceled) {
            continue;
        }
        observations.push(manager_v1::DependencyOverrideObservation {
            relation_id: relation_id(RelationKind::Blocks, dependency, task.get_uuid()),
            dependency_issue_id: issue_id.to_string(),
            observed_revision: revision(dependency_task)?,
            observed_state: crate::tracker::conversion::issue_state(state),
        });
    }
    observations.sort_by(|left, right| {
        left.relation_id
            .cmp(&right.relation_id)
            .then_with(|| left.dependency_issue_id.cmp(&right.dependency_issue_id))
    });
    observations.dedup_by(|left, right| {
        left.relation_id == right.relation_id
            && left.dependency_issue_id == right.dependency_issue_id
            && left.observed_revision == right.observed_revision
            && left.observed_state == right.observed_state
    });
    Ok(observations)
}

fn normalize_source_evidence(
    values: &[v1::SourceEvidenceRef],
    limits: SourceEvidenceLimits,
) -> Result<Vec<v1::SourceEvidenceRef>, TrackerError> {
    if values.len() as u64 > limits.max_repeated_item_count {
        return Err(TrackerError::InvalidState(
            "source evidence exceeds maximum repeated item count".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut normalized = Vec::with_capacity(values.len());
    for value in values {
        if value.thread_id.trim().is_empty() || value.memory_id.trim().is_empty() {
            return Err(TrackerError::InvalidState(
                "source evidence thread_id and memory_id are required".into(),
            ));
        }
        if value.thread_id.len() as u64 > limits.max_string_bytes
            || value.memory_id.len() as u64 > limits.max_string_bytes
        {
            return Err(TrackerError::InvalidState(
                "source evidence identifier exceeds maximum string bytes".into(),
            ));
        }
        if seen.insert((value.thread_id.clone(), value.memory_id.clone())) {
            normalized.push(value.clone());
        }
    }
    Ok(normalized)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod source_evidence_tests {
    use super::*;
    use tempfile::TempDir;

    const NORMALIZATION_LIMITS: SourceEvidenceLimits = SourceEvidenceLimits {
        max_string_bytes: 8,
        max_repeated_item_count: 3,
    };
    const LIMITS: SourceEvidenceLimits = SourceEvidenceLimits {
        max_string_bytes: 8,
        max_repeated_item_count: 2,
    };

    #[test]
    fn normalizes_in_order_and_rejects_blank_identifiers() {
        let first = v1::SourceEvidenceRef {
            thread_id: "thread-1".into(),
            memory_id: "memory-1".into(),
        };
        let second = v1::SourceEvidenceRef {
            thread_id: "thread-2".into(),
            memory_id: "memory-2".into(),
        };
        let normalized = normalize_source_evidence(
            &[first.clone(), first.clone(), second.clone()],
            NORMALIZATION_LIMITS,
        )
        .expect("valid references");
        assert_eq!(normalized, vec![first, second]);
        assert!(
            normalize_source_evidence(
                &[v1::SourceEvidenceRef {
                    thread_id: " ".into(),
                    memory_id: "memory".into(),
                }],
                NORMALIZATION_LIMITS,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_source_evidence_that_exceeds_resource_limits() {
        let reference = v1::SourceEvidenceRef {
            thread_id: "thread".into(),
            memory_id: "memory".into(),
        };
        assert!(
            normalize_source_evidence(
                &[reference.clone(), reference.clone(), reference.clone()],
                LIMITS,
            )
            .is_err()
        );
        assert!(
            normalize_source_evidence(
                &[v1::SourceEvidenceRef {
                    thread_id: "thread-id".into(),
                    memory_id: "memory".into(),
                }],
                LIMITS,
            )
            .is_err()
        );
        assert!(
            normalize_source_evidence(
                &[v1::SourceEvidenceRef {
                    thread_id: "thread".into(),
                    memory_id: "memory-id".into(),
                }],
                LIMITS,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn create_and_update_enforce_configured_source_evidence_limits() {
        let root = TempDir::new().expect("temporary tracker root");
        let tracker = Tracker::open_with_source_evidence_limits(root.path(), LIMITS)
            .await
            .expect("open tracker");
        let oversized_draft = v1::IssueDraft {
            title: "oversized source evidence".into(),
            kind: v1::IssueKind::Task as i32,
            priority: v1::IssuePriority::P2 as i32,
            creation_basis: v1::CreationBasis::Explicit as i32,
            confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
            initial_state: v1::IssueState::Open as i32,
            source_evidence: vec![v1::SourceEvidenceRef {
                thread_id: "thread-id".into(),
                memory_id: "memory".into(),
            }],
            ..Default::default()
        };
        assert!(
            tracker
                .create_with_draft(
                    Uuid::new_v4(),
                    "create-oversized-source-evidence",
                    &oversized_draft,
                    "human",
                    "create-oversized-source-evidence",
                )
                .await
                .is_err()
        );

        let draft = v1::IssueDraft {
            title: "bounded source evidence".into(),
            kind: v1::IssueKind::Task as i32,
            priority: v1::IssuePriority::P2 as i32,
            creation_basis: v1::CreationBasis::Explicit as i32,
            confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
            initial_state: v1::IssueState::Open as i32,
            source_evidence: vec![v1::SourceEvidenceRef {
                thread_id: "thread".into(),
                memory_id: "memory".into(),
            }],
            ..Default::default()
        };
        let issue = tracker
            .create_with_draft(
                Uuid::new_v4(),
                "create-bounded-source-evidence",
                &draft,
                "human",
                "create-bounded-source-evidence",
            )
            .await
            .expect("create bounded issue");
        let patch = v1::IssuePatch {
            source_evidence: vec![
                v1::SourceEvidenceRef {
                    thread_id: "thread".into(),
                    memory_id: "memory".into(),
                },
                v1::SourceEvidenceRef {
                    thread_id: "thread".into(),
                    memory_id: "memory".into(),
                },
                v1::SourceEvidenceRef {
                    thread_id: "thread".into(),
                    memory_id: "memory".into(),
                },
            ],
            ..Default::default()
        };
        assert!(
            tracker
                .update_issue_with_context(
                    issue.id,
                    &patch,
                    &[v1::IssueField::SourceEvidence as i32],
                    issue.revision,
                    "update-oversized-source-evidence",
                    "human",
                    None,
                    "update-oversized-source-evidence",
                    None,
                    None,
                )
                .await
                .is_err()
        );
    }
}

impl Tracker {
    pub async fn create(&self, idempotency_key: &str, title: &str) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        reject_secret([idempotency_key, title])?;
        let id = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("lookback:issue:{idempotency_key}").as_bytes(),
        );
        self.create_with_id(id, idempotency_key, title).await
    }

    pub async fn create_with_id(
        &self,
        id: Uuid,
        operation_id: &str,
        title: &str,
    ) -> Result<Issue, TrackerError> {
        let draft = v1::IssueDraft {
            title: title.to_owned(),
            kind: v1::IssueKind::Task as i32,
            priority: v1::IssuePriority::P2 as i32,
            creation_basis: v1::CreationBasis::Explicit as i32,
            confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
            initial_state: v1::IssueState::Open as i32,
            ..Default::default()
        };
        self.create_with_draft(
            id,
            operation_id,
            &draft,
            ActorIdentity::unknown(),
            operation_id,
        )
        .await
    }

    pub async fn create_with_draft(
        &self,
        id: Uuid,
        operation_id: &str,
        draft: &v1::IssueDraft,
        actor: impl Into<ActorIdentity>,
        correlation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.create_with_draft_context(id, operation_id, draft, actor, None, correlation_id)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_with_draft_context(
        &self,
        id: Uuid,
        operation_id: &str,
        draft: &v1::IssueDraft,
        actor: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        correlation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let actor = actor.into();
        reject_secret([
            operation_id,
            draft.title.as_str(),
            draft.description.as_str(),
            actor.actor_id(),
            correlation_id,
        ])?;
        if draft.title.trim().is_empty() {
            return Err(TrackerError::InvalidState("issue title is required".into()));
        }
        if draft.kind == v1::IssueKind::Unspecified as i32
            || draft.priority == v1::IssuePriority::Unspecified as i32
            || draft.creation_basis == v1::CreationBasis::Unspecified as i32
            || draft.confirmation_status == v1::ConfirmationStatus::Unspecified as i32
        {
            return Err(TrackerError::InvalidState(
                "issue kind, priority, creation_basis, and confirmation_status are required".into(),
            ));
        }
        let _admission = self.admission.read().await;
        // Keep create markers in a method-specific namespace so a caller may reuse
        // the downstream operation id for a later method without a false conflict.
        let create_operation_id = create_operation_id(operation_id);
        let draft_json = serde_json::to_string(draft).map_err(|error| {
            TrackerError::InvalidState(format!("issue draft encode failed: {error}"))
        })?;
        let create_digest = marker_digest(&[id.to_string().as_str(), draft_json.as_str()]);
        let mut replica = self.inner.lock().await;
        if let Some(existing) = replica.get_task(id).await? {
            if operation_replayed(&existing, &create_operation_id, &create_digest)? {
                return self.issue_from_task(&existing);
            }
            if existing.get_description() != draft.title {
                return Err(TrackerError::IdempotencyConflict);
            }
            return self.issue_from_task(&existing);
        }
        let mut ops = Operations::new();
        let mut task = replica.create_task(id, &mut ops).await?;
        task.set_description(draft.title.clone(), &mut ops)?;
        task.set_status(Status::Pending, &mut ops)?;
        set(&mut task, ID_KEY, &id.to_string(), &mut ops)?;
        set(&mut task, ENTITY_KEY, ISSUE_ENTITY, &mut ops)?;
        set(
            &mut task,
            SCHEMA_VERSION_KEY,
            MAPPING_SCHEMA_VERSION,
            &mut ops,
        )?;
        set(
            &mut task,
            "lb.task_fingerprint",
            &marker_digest(&[ISSUE_ENTITY, MAPPING_SCHEMA_VERSION]),
            &mut ops,
        )?;
        let state = match v1::IssueState::try_from(draft.initial_state)
            .map_err(|_| TrackerError::InvalidState("initial_state is invalid".into()))?
        {
            v1::IssueState::Unspecified => IssueState::Open,
            v1::IssueState::Open => IssueState::Open,
            v1::IssueState::InProgress => {
                return Err(TrackerError::InvalidState(
                    "issues must be claimed before entering in_progress".into(),
                ));
            }
            v1::IssueState::Blocked => IssueState::Blocked,
            v1::IssueState::Deferred => IssueState::Deferred,
            v1::IssueState::Done => IssueState::Done,
            v1::IssueState::Canceled => IssueState::Canceled,
            v1::IssueState::AwaitingReview => {
                return Err(TrackerError::InvalidState(
                    DEDICATED_TRANSITION_ERROR.into(),
                ));
            }
        };
        if matches!(state, IssueState::Deferred) && draft.review_at.is_none() {
            return Err(TrackerError::InvalidState(
                "deferred issues require review_at".into(),
            ));
        }
        let timestamp = now();
        let metadata = IssueMetadata {
            description: draft.description.clone(),
            kind: draft.kind,
            priority: draft.priority,
            labels: draft.labels.clone(),
            acceptance_criteria: draft.acceptance_criteria.clone(),
            contexts: draft
                .contexts
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<_, _>>()
                .map_err(|error| {
                    TrackerError::InvalidState(format!("context encode failed: {error}"))
                })?,
            due_at: draft.due_at.as_ref().map(|value| value.unix_epoch_millis),
            review_at: draft
                .review_at
                .as_ref()
                .map(|value| value.unix_epoch_millis),
            created_by: actor_value_with_session(&actor, session_id, correlation_id),
            created_at: timestamp,
            updated_at: timestamp,
            completed_at: None,
            creation_basis: draft.creation_basis,
            confirmation_status: draft.confirmation_status,
            provenance_metadata: draft
                .provenance_metadata
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<_, _>>()
                .map_err(|error| {
                    TrackerError::InvalidState(format!("provenance encode failed: {error}"))
                })?,
            source_evidence: normalize_source_evidence(
                &draft.source_evidence,
                self.source_evidence_limits,
            )?
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()
            .map_err(|error| {
                TrackerError::InvalidState(format!("source evidence encode failed: {error}"))
            })?,
        };
        set_metadata(&mut task, &metadata, &mut ops)?;
        set(&mut task, STATE_KEY, state.as_str(), &mut ops)?;
        task.set_status(state.native_status(), &mut ops)?;
        set(&mut task, REVISION_KEY, "0", &mut ops)?;
        set(&mut task, "lb.idempotency_key", operation_id, &mut ops)?;
        set_history_with_context(
            &mut task,
            &create_operation_id,
            "create",
            0,
            &create_digest,
            &actor,
            correlation_id,
            None,
            Some(state.as_str()),
            None,
            &mut ops,
        )?;
        set_history_session(&mut task, &create_operation_id, session_id, &mut ops)?;
        set_marker(&mut task, &create_operation_id, &create_digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }
    pub async fn update_title(
        &self,
        id: Uuid,
        title: &str,
        expected_revision: u64,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        reject_secret([title, operation_id])?;
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let digest = marker_digest(&[
            id.to_string().as_str(),
            "update_title",
            title,
            expected_revision.to_string().as_str(),
        ]);
        if operation_replayed(&task, operation_id, &digest)? {
            return self.issue_from_task(&task);
        }
        let revision = revision(&task)?;
        if revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision,
            });
        }
        let mut ops = Operations::new();
        task.set_description(title.to_owned(), &mut ops)?;
        set(
            &mut task,
            REVISION_KEY,
            &(revision + 1).to_string(),
            &mut ops,
        )?;
        set_history(
            &mut task,
            operation_id,
            "update",
            revision + 1,
            &digest,
            &mut ops,
        )?;
        set_marker(&mut task, operation_id, &digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_issue(
        &self,
        id: Uuid,
        patch: &v1::IssuePatch,
        update_mask: &[i32],
        expected_revision: u64,
        operation_id: &str,
        actor: &str,
        correlation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.update_issue_with_context(
            id,
            patch,
            update_mask,
            expected_revision,
            operation_id,
            actor,
            None,
            correlation_id,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_issue_with_context(
        &self,
        id: Uuid,
        patch: &v1::IssuePatch,
        update_mask: &[i32],
        expected_revision: u64,
        operation_id: &str,
        actor: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        correlation_id: &str,
        target_state: Option<i32>,
        terminal_report: Option<&v1::update_issue_args::TerminalReport>,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        reject_secret([
            operation_id,
            actor.actor_id(),
            session_id.unwrap_or_default(),
            correlation_id,
        ])?;
        if update_mask.is_empty() {
            return Err(TrackerError::InvalidState("update_mask is required".into()));
        }
        let mut seen = std::collections::HashSet::new();
        for field in update_mask {
            if !seen.insert(*field) {
                return Err(TrackerError::InvalidState(
                    "duplicate update_mask field".into(),
                ));
            }
            if *field == v1::IssueField::Unspecified as i32 {
                return Err(TrackerError::InvalidState(
                    "unspecified update field".into(),
                ));
            }
        }
        let target_state = target_state
            .map(v1::IssueState::try_from)
            .transpose()
            .map_err(|_| TrackerError::InvalidState("target_state is invalid".into()))?;
        if target_state == Some(v1::IssueState::Unspecified) {
            return Err(TrackerError::InvalidState("target_state is invalid".into()));
        }
        if target_state == Some(v1::IssueState::InProgress) {
            return Err(TrackerError::InvalidState(
                "issues must be claimed before entering in_progress".into(),
            ));
        }
        if target_state == Some(v1::IssueState::AwaitingReview) {
            return Err(TrackerError::InvalidState(
                DEDICATED_TRANSITION_ERROR.into(),
            ));
        }
        let report_kind = match terminal_report {
            Some(v1::update_issue_args::TerminalReport::Completion(report)) => {
                terminal_report::TerminalReportKind::Completion {
                    has_result: report.result.is_some(),
                }
            }
            Some(v1::update_issue_args::TerminalReport::Cancellation(report)) => {
                terminal_report::TerminalReportKind::Cancellation {
                    has_classification: report.classification
                        != v1::CancellationClass::Unspecified as i32,
                }
            }
            None => terminal_report::TerminalReportKind::None,
        };
        terminal_report::validate(target_state.map(Into::into), report_kind)
            .map_err(|error| TrackerError::InvalidState(error.message().into()))?;
        reject_update_terminal_report_secrets(terminal_report)?;
        let patch_json = serde_json::to_string(patch).map_err(|error| {
            TrackerError::InvalidState(format!("issue patch encode failed: {error}"))
        })?;
        let terminal_report_json = terminal_report
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?
            .unwrap_or_default();
        let digest = marker_digest(&[
            id.to_string().as_str(),
            "update_issue",
            patch_json.as_str(),
            &update_mask
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(","),
            &expected_revision.to_string(),
            &target_state
                .map(|state| state as i32)
                .unwrap_or_default()
                .to_string(),
            terminal_report_json.as_str(),
        ]);
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        if operation_replayed(&task, operation_id, &digest)? {
            return self.issue_from_task(&task);
        }
        let revision = revision(&task)?;
        if revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision,
            });
        }
        let mut metadata = metadata_from_task(&task)?;
        let old_state =
            IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        let lease = lease_state(&task, now());
        // An IN_PROGRESS issue may receive a metadata-only update while its
        // persisted claim is active and structurally valid.  Unowned,
        // expired, or inconsistent ownership must still be repaired explicitly
        // before an ordinary write can preserve that state.
        if (old_state == IssueState::InProgress && lease != LeaseState::Active)
            || (old_state != IssueState::InProgress && lease != LeaseState::None)
        {
            return Err(match lease {
                LeaseState::Expired => expired_lease_error(&task),
                LeaseState::Inconsistent => inconsistent_lease_error(&task),
                _ => invalid_lease_error(&task),
            });
        }
        if let Some(state) = target_state {
            lifecycle::validate_transition(old_state, issue_state_from_proto(state)?)?;
        }
        if target_state == Some(v1::IssueState::Done) && task.is_blocked() {
            return Err(TrackerError::Blocked {
                open_blocker_issue_ids: open_blocker_issue_ids(&mut replica, &task).await?,
            });
        }
        if target_state
            .is_some_and(|state| matches!(state, v1::IssueState::Done | v1::IssueState::Canceled))
            && lease != LeaseState::None
        {
            return Err(match lease {
                LeaseState::Expired => expired_lease_error(&task),
                LeaseState::Inconsistent => inconsistent_lease_error(&task),
                _ => invalid_lease_error(&task),
            });
        }
        let mut changed_fields = Vec::new();
        let mut ops = Operations::new();
        for field in update_mask {
            match v1::IssueField::try_from(*field) {
                Ok(v1::IssueField::Title) => {
                    let title = patch.title.as_deref().ok_or_else(|| {
                        TrackerError::InvalidState("title is required by update_mask".into())
                    })?;
                    if title.trim().is_empty() {
                        return Err(TrackerError::InvalidState("issue title is required".into()));
                    }
                    reject_secret([title])?;
                    task.set_description(title.to_owned(), &mut ops)?;
                    changed_fields.push("title".to_owned());
                }
                Ok(v1::IssueField::Description) => {
                    metadata.description = patch.description.clone().ok_or_else(|| {
                        TrackerError::InvalidState("description is required by update_mask".into())
                    })?;
                    reject_secret([metadata.description.as_str()])?;
                    changed_fields.push("description".to_owned());
                }
                Ok(v1::IssueField::Kind) => {
                    metadata.kind = patch.kind.ok_or_else(|| {
                        TrackerError::InvalidState("kind is required by update_mask".into())
                    })?;
                    changed_fields.push("kind".to_owned());
                }
                Ok(v1::IssueField::Priority) => {
                    metadata.priority = patch.priority.ok_or_else(|| {
                        TrackerError::InvalidState("priority is required by update_mask".into())
                    })?;
                    changed_fields.push("priority".to_owned());
                }
                Ok(v1::IssueField::Labels) => {
                    reject_secret(patch.labels.iter().map(String::as_str))?;
                    metadata.labels = patch.labels.clone();
                    changed_fields.push("labels".to_owned());
                }
                Ok(v1::IssueField::AcceptanceCriteria) => {
                    reject_secret(patch.acceptance_criteria.iter().map(String::as_str))?;
                    metadata.acceptance_criteria = patch.acceptance_criteria.clone();
                    changed_fields.push("acceptance_criteria".to_owned());
                }
                Ok(v1::IssueField::Contexts) => {
                    metadata.contexts = patch
                        .contexts
                        .iter()
                        .map(serde_json::to_value)
                        .collect::<Result<_, _>>()
                        .map_err(|error| {
                            TrackerError::InvalidState(format!("context encode failed: {error}"))
                        })?;
                    changed_fields.push("contexts".to_owned());
                }
                Ok(v1::IssueField::DueAt) => {
                    metadata.due_at = match patch.due_at_change.as_ref() {
                        Some(v1::issue_patch::DueAtChange::DueAt(value)) => {
                            Some(value.unix_epoch_millis)
                        }
                        Some(v1::issue_patch::DueAtChange::ClearDueAt(_)) => None,
                        None => {
                            return Err(TrackerError::InvalidState(
                                "due_at change is required".into(),
                            ));
                        }
                    };
                    changed_fields.push("due_at".to_owned());
                }
                Ok(v1::IssueField::ReviewAt) => {
                    metadata.review_at = match patch.review_at_change.as_ref() {
                        Some(v1::issue_patch::ReviewAtChange::ReviewAt(value)) => {
                            Some(value.unix_epoch_millis)
                        }
                        Some(v1::issue_patch::ReviewAtChange::ClearReviewAt(_)) => None,
                        None => {
                            return Err(TrackerError::InvalidState(
                                "review_at change is required".into(),
                            ));
                        }
                    };
                    changed_fields.push("review_at".to_owned());
                }
                Ok(v1::IssueField::ConfirmationStatus) => {
                    metadata.confirmation_status = patch.confirmation_status.ok_or_else(|| {
                        TrackerError::InvalidState(
                            "confirmation_status is required by update_mask".into(),
                        )
                    })?;
                    changed_fields.push("confirmation_status".to_owned());
                }
                Ok(v1::IssueField::ProvenanceMetadata) => {
                    metadata.provenance_metadata = patch
                        .provenance_metadata
                        .iter()
                        .map(serde_json::to_value)
                        .collect::<Result<_, _>>()
                        .map_err(|error| {
                            TrackerError::InvalidState(format!("provenance encode failed: {error}"))
                        })?;
                    changed_fields.push("provenance_metadata".to_owned());
                }
                Ok(v1::IssueField::SourceEvidence) => {
                    metadata.source_evidence = normalize_source_evidence(
                        &patch.source_evidence,
                        self.source_evidence_limits,
                    )?
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<Result<_, _>>()
                    .map_err(|error| {
                        TrackerError::InvalidState(format!(
                            "source evidence encode failed: {error}"
                        ))
                    })?;
                    changed_fields.push("source_evidence".to_owned());
                }
                _ => return Err(TrackerError::InvalidState("unknown update field".into())),
            }
        }
        let timestamp = now();
        metadata.updated_at = timestamp;
        if let Some(state) = target_state {
            let state = issue_state_from_proto(state)?;
            apply_current_state(&mut task, &mut metadata, state, timestamp, &mut ops)?;
            if state == IssueState::Canceled
                || (old_state == IssueState::Canceled && state == IssueState::Open)
            {
                clear_active_claim(&mut task, &mut ops)?;
            }
            changed_fields.push("state".to_owned());
        }
        set_metadata(&mut task, &metadata, &mut ops)?;
        set(
            &mut task,
            REVISION_KEY,
            &(revision + 1).to_string(),
            &mut ops,
        )?;
        set_history_with_context(
            &mut task,
            operation_id,
            if target_state.is_some() {
                "transition"
            } else {
                "update"
            },
            revision + 1,
            &digest,
            &actor,
            correlation_id,
            target_state.map(|_| old_state.as_str()),
            target_state
                .map(|state| issue_state_from_proto(state).map(IssueState::as_str))
                .transpose()?,
            None,
            &mut ops,
        )?;
        set_history_session(&mut task, operation_id, session_id, &mut ops)?;
        match terminal_report {
            Some(v1::update_issue_args::TerminalReport::Completion(report)) => {
                set_history_subject_fields(
                    &mut task,
                    operation_id,
                    &[(
                        "completion",
                        serde_json::to_value(report)
                            .map_err(|error| TrackerError::InvalidState(error.to_string()))?,
                    )],
                    &mut ops,
                )?;
            }
            Some(v1::update_issue_args::TerminalReport::Cancellation(report)) => {
                set_history_subject_fields(
                    &mut task,
                    operation_id,
                    &[(
                        "cancellation",
                        serde_json::to_value(report)
                            .map_err(|error| TrackerError::InvalidState(error.to_string()))?,
                    )],
                    &mut ops,
                )?;
            }
            None => {}
        }
        set_marker(&mut task, operation_id, &digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }

    pub async fn reconcile_expired_claim(
        &self,
        id: Uuid,
        expected_revision: u64,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.reconcile_expired_claim_with_context(
            id,
            expected_revision,
            operation_id,
            "system",
            "lease_expired",
        )
        .await
    }

    pub async fn reconcile_expired_claim_with_context(
        &self,
        id: Uuid,
        expected_revision: u64,
        operation_id: &str,
        actor: impl Into<ActorIdentity>,
        reason: &str,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        reject_secret([operation_id, actor.actor_id(), reason])?;
        if actor.actor_id().trim().is_empty() || reason.trim().is_empty() {
            return Err(TrackerError::InvalidState(
                "reconcile actor and reason are required".into(),
            ));
        }
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let digest = marker_digest(&[
            id.to_string().as_str(),
            "reconcile_expired_claim",
            expected_revision.to_string().as_str(),
        ]);
        if operation_replayed(&task, operation_id, &digest)? {
            return self.issue_from_task(&task);
        }
        let current_revision = revision(&task)?;
        if current_revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: current_revision,
            });
        }
        let lease = lease_state(&task, now());
        if lease != LeaseState::Expired {
            return Err(match lease {
                LeaseState::Inconsistent => inconsistent_lease_error(&task),
                _ => invalid_lease_error(&task),
            });
        }
        let old_state =
            IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        let next_state = if old_state == IssueState::InProgress {
            IssueState::Open
        } else {
            old_state
        };
        let mut ops = Operations::new();
        if next_state != old_state {
            set(&mut task, STATE_KEY, next_state.as_str(), &mut ops)?;
            task.set_status(next_state.native_status(), &mut ops)?;
        }
        set(
            &mut task,
            REVISION_KEY,
            &(current_revision + 1).to_string(),
            &mut ops,
        )?;
        clear_active_claim(&mut task, &mut ops)?;
        set_history_with_context(
            &mut task,
            operation_id,
            "lease_expired",
            current_revision + 1,
            &digest,
            &actor,
            operation_id,
            Some(old_state.as_str()),
            Some(next_state.as_str()),
            Some(reason),
            &mut ops,
        )?;
        set_marker(&mut task, operation_id, &digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }

    /// Recovers an unowned or expired claim.  An active or structurally
    /// inconsistent claim can only be cleared when the caller explicitly
    /// requests revocation; this keeps ordinary maintenance from stealing
    /// work or erasing forensic evidence.
    pub async fn recover_claim(
        &self,
        id: Uuid,
        expected_revision: u64,
        revoke: bool,
        operation_id: &str,
        actor: impl Into<ActorIdentity>,
        reason: &str,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        reject_secret([operation_id, actor.actor_id(), reason])?;
        if actor.actor_id().trim().is_empty() || reason.trim().is_empty() {
            return Err(TrackerError::InvalidState(
                "recovery actor and reason are required".into(),
            ));
        }
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let digest = marker_digest(&[
            id.to_string().as_str(),
            "recover_claim",
            revoke.to_string().as_str(),
            expected_revision.to_string().as_str(),
            actor.actor_id(),
            reason,
        ]);
        if operation_replayed(&task, operation_id, &digest)? {
            return self.issue_from_task(&task);
        }
        let current_revision = revision(&task)?;
        if current_revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: current_revision,
            });
        }
        let lease = lease_state(&task, now());
        let repaired_ownership_attributes = ownership_attributes_present(&task);
        if !revoke {
            match lease {
                LeaseState::Active => return Err(invalid_lease_error(&task)),
                LeaseState::Inconsistent => return Err(inconsistent_lease_error(&task)),
                _ => {}
            }
        }
        let old_state =
            IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        let next_state = if old_state == IssueState::InProgress {
            IssueState::Open
        } else {
            old_state
        };
        let observed_lease_classification = match lease {
            LeaseState::None => "NONE",
            LeaseState::Active => "ACTIVE",
            LeaseState::Expired => "EXPIRED",
            LeaseState::Inconsistent => "INCONSISTENT",
        };
        let action = if revoke {
            "ISSUE_ADMIN_ACTION_REVOKE"
        } else {
            "ISSUE_ADMIN_ACTION_RECOVER"
        };
        if lease == LeaseState::None && old_state != IssueState::InProgress {
            // A no-op recovery is still a durable administrative decision.
            // Persist its marker so a Manager crash can recover the immutable
            // result without consulting the later Issue state.  Do not append
            // an Issue change-history entry: NO_CHANGE must leave revision,
            // updated_at, and lifecycle history untouched.
            let mut ops = Operations::new();
            let receipt = manager_v1::IssueAdminActionResult {
                issue_id: id.to_string(),
                resulting_state: crate::tracker::conversion::issue_state(next_state),
                resulting_revision: current_revision,
                action: action.into(),
                changed: false,
                // NO_CHANGE has no state transition, so the optional field is
                // intentionally absent as required by the public contract.
                previous_state: None,
                previous_revision: current_revision,
                ownership_cleared: false,
                audit_operation_id: operation_id.into(),
                confirmed_at: Some(crate::tracker::conversion::instant(now())),
                observed_lease_classification: observed_lease_classification.into(),
                ..Default::default()
            };
            set_admin_receipt_payload(&mut task, operation_id, &receipt, &mut ops)?;
            set_marker(&mut task, operation_id, &digest, &mut ops)?;
            replica.commit_operations(ops).await?;
            self.advance_read_generation();
            return self.issue_from_task(&task);
        }
        let mut ops = Operations::new();
        if next_state != old_state {
            set(&mut task, STATE_KEY, next_state.as_str(), &mut ops)?;
            task.set_status(next_state.native_status(), &mut ops)?;
        }
        if lease != LeaseState::None {
            clear_active_claim(&mut task, &mut ops)?;
        }
        set(
            &mut task,
            REVISION_KEY,
            &(current_revision + 1).to_string(),
            &mut ops,
        )?;
        set_history_with_context(
            &mut task,
            operation_id,
            if revoke {
                "lease_revoked"
            } else {
                "lease_recovered"
            },
            current_revision + 1,
            &digest,
            &actor,
            operation_id,
            Some(old_state.as_str()),
            Some(next_state.as_str()),
            Some(reason),
            &mut ops,
        )?;
        set_history_subject_fields(
            &mut task,
            operation_id,
            &[(
                "ownership_cleared_attributes",
                serde_json::Value::Array(
                    repaired_ownership_attributes
                        .iter()
                        .cloned()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            )],
            &mut ops,
        )?;
        let receipt = manager_v1::IssueAdminActionResult {
            issue_id: id.to_string(),
            resulting_state: crate::tracker::conversion::issue_state(next_state),
            resulting_revision: current_revision + 1,
            action: action.into(),
            changed: true,
            previous_state: Some(crate::tracker::conversion::issue_state(old_state)),
            previous_revision: current_revision,
            ownership_cleared: lease != LeaseState::None,
            audit_operation_id: operation_id.into(),
            confirmed_at: Some(crate::tracker::conversion::instant(now())),
            inconsistent_claim_resolution_applied: revoke && lease == LeaseState::Inconsistent,
            // REVOKE is its own administrative action.  It may repair an
            // inconsistent tuple, but it is not the separate
            // `inconsistent_claim_resolution` request used by COMPLETE /
            // CANCEL.
            inconsistent_claim_resolution_requested: false,
            repaired_ownership_attributes,
            observed_lease_classification: observed_lease_classification.into(),
            inconsistent_claim_classification: if lease == LeaseState::Inconsistent {
                "INCONSISTENT_TUPLE".into()
            } else {
                String::new()
            },
            ..Default::default()
        };
        set_admin_receipt_payload(&mut task, operation_id, &receipt, &mut ops)?;
        set_marker(&mut task, operation_id, &digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }

    /// Performs the privileged, single-write terminal transition used by the
    /// trusted manager boundary.  It is intentionally separate from ordinary
    /// Transition: a lease capability is neither accepted nor inferred here.
    #[allow(clippy::too_many_arguments)]
    pub async fn admin_terminate(
        &self,
        id: Uuid,
        state: IssueState,
        expected_revision: u64,
        operation_id: &str,
        actor: &str,
        reason: &str,
        terminal_report: &v1::transition_issue_args::TerminalReport,
        dependency_override: bool,
        inconsistent_claim_resolution: bool,
    ) -> Result<Issue, TrackerError> {
        self.admin_terminate_with_overrides(
            id,
            state,
            expected_revision,
            operation_id,
            actor,
            reason,
            terminal_report,
            dependency_override,
            inconsistent_claim_resolution,
            None,
            None,
        )
        .await
    }

    /// Variant used by the trusted Manager boundary when the request carries
    /// explicit override reasons.  Keeping the legacy wrapper above avoids
    /// changing standalone Tracker callers while preserving the two reasons
    /// as separate server-owned audit fields.
    #[allow(clippy::too_many_arguments)]
    pub async fn admin_terminate_with_overrides(
        &self,
        id: Uuid,
        state: IssueState,
        expected_revision: u64,
        operation_id: &str,
        actor: impl Into<ActorIdentity>,
        reason: &str,
        terminal_report: &v1::transition_issue_args::TerminalReport,
        dependency_override: bool,
        inconsistent_claim_resolution: bool,
        dependency_override_reason: Option<&str>,
        inconsistent_claim_resolution_reason: Option<&str>,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        reject_secret([
            operation_id,
            actor.actor_id(),
            reason,
            dependency_override_reason.unwrap_or_default(),
            inconsistent_claim_resolution_reason.unwrap_or_default(),
        ])?;
        if actor.actor_id().trim().is_empty() || reason.trim().is_empty() {
            return Err(TrackerError::InvalidState(
                "administrative actor and reason are required".into(),
            ));
        }
        if dependency_override
            != dependency_override_reason.is_some_and(|value| !value.trim().is_empty())
            || inconsistent_claim_resolution
                != inconsistent_claim_resolution_reason
                    .is_some_and(|value| !value.trim().is_empty())
        {
            return Err(TrackerError::InvalidState(
                "administrative override reason does not match its flag".into(),
            ));
        }
        if !lifecycle::is_terminal(state) {
            return Err(TrackerError::InvalidState(
                "administrative termination requires a terminal state".into(),
            ));
        }
        let report_kind = match terminal_report {
            v1::transition_issue_args::TerminalReport::Completion(report) => {
                terminal_report::TerminalReportKind::Completion {
                    has_result: report.result.is_some(),
                }
            }
            v1::transition_issue_args::TerminalReport::Cancellation(report) => {
                terminal_report::TerminalReportKind::Cancellation {
                    has_classification: report.classification
                        != v1::CancellationClass::Unspecified as i32,
                }
            }
        };
        terminal_report::validate(Some(state.into()), report_kind)
            .map_err(|error| TrackerError::InvalidState(error.message().into()))?;
        let report_json = serde_json::to_string(terminal_report)
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?;
        let digest = marker_digest(&[
            id.to_string().as_str(),
            "transition",
            state.as_str(),
            expected_revision.to_string().as_str(),
            actor.actor_id(),
            reason,
            dependency_override.to_string().as_str(),
            inconsistent_claim_resolution.to_string().as_str(),
            dependency_override_reason.unwrap_or_default(),
            inconsistent_claim_resolution_reason.unwrap_or_default(),
            report_json.as_str(),
        ]);
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        if operation_replayed(&task, operation_id, &digest)? {
            return self.issue_from_task(&task);
        }
        let revision = revision(&task)?;
        if revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision,
            });
        }
        let old_state =
            IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        if lifecycle::is_terminal(old_state) {
            return Err(TrackerError::InvalidStateTransition(
                "issue is already terminal".into(),
            ));
        }
        let had_dependency_blocker = state == IssueState::Done && task.is_blocked();
        if had_dependency_blocker && !dependency_override {
            return Err(TrackerError::Blocked {
                open_blocker_issue_ids: open_blocker_issue_ids(&mut replica, &task).await?,
            });
        }
        let lease = lease_state(&task, now());
        let repaired_ownership_attributes = ownership_attributes_present(&task);
        if lease == LeaseState::Inconsistent && !inconsistent_claim_resolution {
            return Err(inconsistent_lease_error(&task));
        }
        let dependency_override_observations = if had_dependency_blocker || dependency_override {
            collect_dependency_override_observations(&mut replica, &task).await?
        } else {
            Vec::new()
        };
        let dependency_override_target_issue_ids = dependency_override_observations
            .iter()
            .map(|observation| observation.dependency_issue_id.clone())
            .collect::<Vec<_>>();
        let mut metadata = metadata_from_task(&task)?;
        let timestamp = now();
        let mut ops = Operations::new();
        metadata.updated_at = timestamp;
        apply_current_state(&mut task, &mut metadata, state, timestamp, &mut ops)?;
        clear_active_claim(&mut task, &mut ops)?;
        set_metadata(&mut task, &metadata, &mut ops)?;
        set(
            &mut task,
            REVISION_KEY,
            &(revision + 1).to_string(),
            &mut ops,
        )?;
        set_history_with_context(
            &mut task,
            operation_id,
            "apply_issue_admin_action",
            revision + 1,
            &digest,
            &actor,
            operation_id,
            Some(old_state.as_str()),
            Some(state.as_str()),
            Some(reason),
            &mut ops,
        )?;
        match terminal_report {
            v1::transition_issue_args::TerminalReport::Completion(report) => {
                set_history_subject_fields(
                    &mut task,
                    operation_id,
                    &[(
                        "completion",
                        serde_json::to_value(report)
                            .map_err(|error| TrackerError::InvalidState(error.to_string()))?,
                    )],
                    &mut ops,
                )?;
            }
            v1::transition_issue_args::TerminalReport::Cancellation(report) => {
                set_history_subject_fields(
                    &mut task,
                    operation_id,
                    &[(
                        "cancellation",
                        serde_json::to_value(report)
                            .map_err(|error| TrackerError::InvalidState(error.to_string()))?,
                    )],
                    &mut ops,
                )?;
            }
        }
        set_history_subject_fields(
            &mut task,
            operation_id,
            &[
                ("administrative", serde_json::Value::Bool(true)),
                (
                    "dependency_override_requested",
                    serde_json::Value::Bool(dependency_override),
                ),
                (
                    "inconsistent_claim_resolution_requested",
                    serde_json::Value::Bool(inconsistent_claim_resolution),
                ),
            ],
            &mut ops,
        )?;
        set_history_subject_fields(
            &mut task,
            operation_id,
            &[(
                "ownership_cleared_attributes",
                serde_json::Value::Array(
                    repaired_ownership_attributes
                        .iter()
                        .cloned()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            )],
            &mut ops,
        )?;
        let dependency_override_observations_json = dependency_override_observations
            .iter()
            .map(|observation| {
                serde_json::json!({
                    "relation_id": observation.relation_id,
                    "dependency_issue_id": observation.dependency_issue_id,
                    "observed_revision": observation.observed_revision,
                    "observed_state": observation.observed_state,
                })
            })
            .collect::<Vec<_>>();
        set_history_subject_fields(
            &mut task,
            operation_id,
            &[(
                "dependency_override_observations",
                serde_json::Value::Array(dependency_override_observations_json),
            )],
            &mut ops,
        )?;
        if let Some(override_reason) = dependency_override_reason {
            set_history_subject_fields(
                &mut task,
                operation_id,
                &[(
                    "dependency_override_reason",
                    serde_json::Value::String(override_reason.to_owned()),
                )],
                &mut ops,
            )?;
        }
        if let Some(resolution_reason) = inconsistent_claim_resolution_reason {
            set_history_subject_fields(
                &mut task,
                operation_id,
                &[(
                    "inconsistent_claim_resolution_reason",
                    serde_json::Value::String(resolution_reason.to_owned()),
                )],
                &mut ops,
            )?;
        }
        let observed_lease_classification = match lease {
            LeaseState::None => "NONE",
            LeaseState::Active => "ACTIVE",
            LeaseState::Expired => "EXPIRED",
            LeaseState::Inconsistent => "INCONSISTENT",
        };
        let receipt = manager_v1::IssueAdminActionResult {
            issue_id: id.to_string(),
            resulting_state: crate::tracker::conversion::issue_state(state),
            resulting_revision: revision + 1,
            action: if state == IssueState::Done {
                "ISSUE_ADMIN_ACTION_COMPLETE"
            } else {
                "ISSUE_ADMIN_ACTION_CANCEL"
            }
            .into(),
            changed: true,
            previous_state: Some(crate::tracker::conversion::issue_state(old_state)),
            previous_revision: revision,
            ownership_cleared: !repaired_ownership_attributes.is_empty(),
            audit_operation_id: operation_id.into(),
            confirmed_at: Some(crate::tracker::conversion::instant(timestamp)),
            inconsistent_claim_resolution_applied: inconsistent_claim_resolution
                && lease == LeaseState::Inconsistent,
            dependency_override_applied: had_dependency_blocker && dependency_override,
            inconsistent_claim_resolution_requested: inconsistent_claim_resolution,
            dependency_override_requested: dependency_override,
            repaired_ownership_attributes,
            observed_lease_classification: observed_lease_classification.into(),
            dependency_override_target_issue_ids,
            dependency_override_observations,
            inconsistent_claim_classification: if lease == LeaseState::Inconsistent {
                "INCONSISTENT_TUPLE".into()
            } else {
                String::new()
            },
        };
        set_admin_receipt_payload(&mut task, operation_id, &receipt, &mut ops)?;
        set_marker(&mut task, operation_id, &digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }

    /// Atomically move an actively claimed implementation run to
    /// AWAITING_REVIEW. This API is crate-private and is called only by the
    /// bound app Gateway capability dispatcher; generic transition handlers
    /// reject the target state.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn complete_execution_to_awaiting_review(
        &self,
        id: Uuid,
        expected_revision: u64,
        operation_id: &str,
        lease_handle: Option<&str>,
        claim_revision: Option<u64>,
        actor: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        correlation_id: &str,
        completion: &v1::AwaitingReviewCompletion,
    ) -> Result<(), TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        if operation_id.trim().is_empty() {
            return Err(TrackerError::InvalidState(
                "downstream operation id is required".into(),
            ));
        }
        validate_awaiting_review_completion(completion)?;
        if session_id != Some(completion.run_id.as_str()) {
            return Err(TrackerError::InvalidState(
                "awaiting_review actor session must match the run id".into(),
            ));
        }
        reject_secret([
            operation_id,
            actor.actor_id(),
            session_id.unwrap_or_default(),
            correlation_id,
        ])?;
        let completion_json = serde_json::to_string(completion)
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?;
        // Deliberately omit the one-time lease handle. The idempotency
        // authority is the bound Gateway key/request digest, and the mutation
        // result is replayable only for this exact run+report operation.
        let digest = awaiting_review_marker_digest(
            id,
            expected_revision,
            operation_id,
            actor.actor_id(),
            session_id.unwrap_or_default(),
            correlation_id,
            &completion_json,
        );
        // Encoded before taking the replica lock: the report may be large.
        // It must not be derived from completion_json, whose key order feeds
        // the marker digest.
        let completion_value = serde_json::to_value(completion).map_err(|error| {
            TrackerError::InvalidState(format!("awaiting-review report encode failed: {error}"))
        })?;

        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        if operation_replayed(&task, operation_id, &digest)? {
            return Ok(());
        }
        let current_revision = revision(&task)?;
        if current_revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: current_revision,
            });
        }
        let old_state =
            IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        if old_state != IssueState::InProgress {
            return Err(TrackerError::InvalidStateTransition(
                "only an in_progress issue can enter awaiting_review".into(),
            ));
        }
        let lease_handle = lease_handle.ok_or_else(|| invalid_lease_error(&task))?;
        validate_active_claim(&task, expected_revision, claim_revision, lease_handle)?;

        let timestamp = now();
        let mut operations = Operations::new();
        let mut metadata = metadata_from_task(&task)?;
        metadata.updated_at = timestamp;
        apply_current_state(
            &mut task,
            &mut metadata,
            IssueState::AwaitingReview,
            timestamp,
            &mut operations,
        )?;
        // State, history/evidence, revision, operation marker, and all four
        // persisted lease attributes share this one TaskChampion Operations
        // commit. Awaiting review is nonterminal, but is deliberately unclaimed.
        clear_active_claim(&mut task, &mut operations)?;
        set_metadata(&mut task, &metadata, &mut operations)?;
        let resulting_revision = current_revision
            .checked_add(1)
            .ok_or_else(|| TrackerError::InvalidState("revision overflow".into()))?;
        set(
            &mut task,
            REVISION_KEY,
            &resulting_revision.to_string(),
            &mut operations,
        )?;
        set_history_with_context(
            &mut task,
            operation_id,
            "transition",
            resulting_revision,
            &digest,
            &actor,
            correlation_id,
            Some(old_state.as_str()),
            Some(IssueState::AwaitingReview.as_str()),
            Some(AWAITING_REVIEW_REASON),
            &mut operations,
        )?;
        set_history_session(&mut task, operation_id, session_id, &mut operations)?;
        set_history_subject_fields(
            &mut task,
            operation_id,
            &[("awaiting_review", completion_value)],
            &mut operations,
        )?;
        set_marker(&mut task, operation_id, &digest, &mut operations)?;
        replica.commit_operations(operations).await?;
        self.advance_read_generation();
        Ok(())
    }

    /// Proves that a previously-reserved Gateway request committed by reading
    /// its immutable TaskChampion marker and history. This is read-only: it
    /// never replays the transition or recreates its consumed lease authority.
    pub(crate) async fn verify_awaiting_review_commit(
        &self,
        id: Uuid,
        expected_revision: u64,
        actor: &str,
        gateway_request_digest: &str,
        completion: &v1::AwaitingReviewCompletion,
    ) -> Result<bool, TrackerError> {
        self.ensure_healthy()?;
        let Some(resulting_revision) = expected_revision.checked_add(1) else {
            return Ok(false);
        };
        if actor.trim().is_empty() || gateway_request_digest.trim().is_empty() {
            return Ok(false);
        }
        let completion_json = serde_json::to_string(completion)
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?;
        let expected_correlation_id =
            awaiting_review_gateway_correlation_id(gateway_request_digest);
        let expected_completion = serde_json::to_value(completion)
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?;

        // get_task returns an owned snapshot, so the replica lock is released
        // before the O(history) JSON verification below.
        let Some(task) = self.inner.lock().await.get_task(id).await? else {
            return Ok(false);
        };
        if task.get_user_defined_attribute(ID_KEY) != Some(id.to_string().as_str()) {
            return Ok(false);
        }
        let mut matching_history: Option<(String, Value)> = None;
        for (key, value) in task.get_user_defined_attributes() {
            let Some(history_operation_id) = key.strip_prefix("lb.history.") else {
                continue;
            };
            let history: Value = serde_json::from_str(value).map_err(|error| {
                TrackerError::InvalidState(format!(
                    "invalid awaiting-review operation history: {error}"
                ))
            })?;
            if json_str(&history, "correlation_id") != Some(expected_correlation_id.as_str()) {
                continue;
            }
            if matching_history.is_some()
                || json_str(&history, "operation_id") != Some(history_operation_id)
            {
                return Ok(false);
            }
            matching_history = Some((history_operation_id.to_owned(), history));
        }
        let Some((operation_id, history)) = matching_history else {
            return Ok(false);
        };
        let Some(marker_value) =
            task.get_user_defined_attribute(&format!("lb.operation.{operation_id}"))
        else {
            return Ok(false);
        };
        let marker: Value = serde_json::from_str(marker_value).map_err(|error| {
            TrackerError::InvalidState(format!("invalid awaiting-review operation marker: {error}"))
        })?;
        let expected_digest = awaiting_review_marker_digest(
            id,
            expected_revision,
            &operation_id,
            actor,
            &completion.run_id,
            &expected_correlation_id,
            &completion_json,
        );

        let matches_history = json_str(&history, "kind") == Some("transition")
            && history.get("revision").and_then(Value::as_u64) == Some(resulting_revision)
            && json_str(&history, "payload_digest") == Some(&expected_digest)
            && json_str(&history, "contract") == Some(CONTRACT_VERSION)
            && json_str(&history, "actor") == Some(actor)
            && json_str(&history, "agent_session_id") == Some(completion.run_id.as_str())
            && json_str(&history, "correlation_id") == Some(expected_correlation_id.as_str())
            && json_str(&history, "old_state") == Some("in_progress")
            && json_str(&history, "new_state") == Some("awaiting_review")
            && json_str(&history, "reason") == Some(AWAITING_REVIEW_REASON)
            && history.get("awaiting_review") == Some(&expected_completion);
        let matches_marker = json_str(&marker, "status") == Some("committed")
            && json_str(&marker, "contract") == Some(CONTRACT_VERSION)
            && json_str(&marker, "schema_fingerprint") == Some(Self::schema_fingerprint().as_str())
            && marker.get("revision").and_then(Value::as_u64) == Some(resulting_revision)
            && json_str(&marker, "payload_digest") == Some(&expected_digest)
            && json_str(&marker, "result_replayability") == Some("replayable");
        if !matches_history || !matches_marker {
            return Ok(false);
        }

        let result_snapshot: v1::OperationResultSnapshot =
            serde::Deserialize::deserialize(marker.get("result").ok_or_else(|| {
                TrackerError::InvalidState(
                    "awaiting-review marker has no immutable result snapshot".into(),
                )
            })?)
            .map_err(|error| {
                TrackerError::InvalidState(format!(
                    "invalid awaiting-review result snapshot: {error}"
                ))
            })?;
        let Some(v1::operation_result_snapshot::Value::IssueMutation(mutation)) =
            result_snapshot.value
        else {
            return Ok(false);
        };
        let Some(issue) = mutation.current.and_then(|details| details.issue) else {
            return Ok(false);
        };
        Ok(issue.issue_id == id.to_string()
            && issue.state == v1::IssueState::AwaitingReview as i32
            && issue.revision == resulting_revision
            && issue.claim.is_none())
    }

    pub async fn transition(
        &self,
        id: Uuid,
        state: IssueState,
        expected_revision: u64,
        operation_id: &str,
        lease_handle: Option<&str>,
    ) -> Result<Issue, TrackerError> {
        self.transition_with_context(
            id,
            state,
            expected_revision,
            operation_id,
            lease_handle,
            lease_handle.map(|_| expected_revision),
            ActorIdentity::unknown(),
            None,
            operation_id,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transition_with_context(
        &self,
        id: Uuid,
        state: IssueState,
        expected_revision: u64,
        operation_id: &str,
        lease_handle: Option<&str>,
        claim_revision: Option<u64>,
        actor: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        correlation_id: &str,
        reason: Option<&str>,
        terminal_report: Option<&v1::transition_issue_args::TerminalReport>,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        reject_secret([
            operation_id,
            lease_handle.unwrap_or_default(),
            actor.actor_id(),
            correlation_id,
        ])?;
        if let Some(v1::transition_issue_args::TerminalReport::Completion(report)) = terminal_report
        {
            reject_completion_report_secrets(report)?;
        }
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let terminal_report_json = terminal_report
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?
            .unwrap_or_default();
        let transition_digest = marker_digest(&[
            id.to_string().as_str(),
            state.as_str(),
            &expected_revision.to_string(),
            lease_handle.unwrap_or_default(),
            reason.unwrap_or_default(),
            session_id.unwrap_or_default(),
            terminal_report_json.as_str(),
        ]);
        if operation_replayed(&task, operation_id, &transition_digest)? {
            return self.issue_from_task(&task);
        }
        let revision = revision(&task)?;
        let old_state =
            IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        if revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision,
            });
        }
        lifecycle::validate_transition(old_state, state)?;
        if lifecycle::is_dedicated_transition(state) {
            return Err(TrackerError::InvalidStateTransition(
                DEDICATED_TRANSITION_ERROR.into(),
            ));
        }
        if state == IssueState::InProgress {
            return Err(TrackerError::InvalidState(
                "issues must be claimed before entering in_progress".into(),
            ));
        }
        if matches!(state, IssueState::Done) && task.is_blocked() {
            return Err(TrackerError::Blocked {
                open_blocker_issue_ids: open_blocker_issue_ids(&mut replica, &task).await?,
            });
        }
        let lease = lease_state(&task, now());
        match lease_handle {
            Some(lease_handle) => {
                validate_active_claim(&task, expected_revision, claim_revision, lease_handle)?;
            }
            None if old_state == IssueState::InProgress && lease != LeaseState::Active => {
                return Err(match lease {
                    LeaseState::Expired => expired_lease_error(&task),
                    LeaseState::Inconsistent => inconsistent_lease_error(&task),
                    _ => invalid_lease_error(&task),
                });
            }
            None if lease == LeaseState::Active && lifecycle::is_terminal(state) => {
                // A terminal transition must carry the worker's capability
                // while an active claim is present.  BLOCKED/DEFERRED are
                // intentionally still allowed so a worker can relinquish
                // progress without silently completing the issue.
                return Err(invalid_lease_error(&task));
            }
            // A worker may deliberately move an in-progress item to
            // BLOCKED/DEFERRED while retaining its active claim; Release
            // then returns that state through the canonical flow. Only
            // malformed or expired tuples are rejected here. Terminal
            // transitions still require the matching capability below.
            None if lease == LeaseState::Inconsistent => {
                return Err(inconsistent_lease_error(&task));
            }
            None if lease == LeaseState::Expired => {
                return Err(expired_lease_error(&task));
            }
            None => {}
        }
        let mut ops = Operations::new();
        let mut metadata = metadata_from_task(&task)?;
        let timestamp = now();
        metadata.updated_at = timestamp;
        apply_current_state(&mut task, &mut metadata, state, timestamp, &mut ops)?;
        if lifecycle::is_terminal(state)
            || (old_state == IssueState::Canceled && state == IssueState::Open)
        {
            clear_active_claim(&mut task, &mut ops)?;
        }
        set_metadata(&mut task, &metadata, &mut ops)?;
        set(
            &mut task,
            REVISION_KEY,
            &(revision + 1).to_string(),
            &mut ops,
        )?;
        set_history_with_context(
            &mut task,
            operation_id,
            "transition",
            revision + 1,
            &transition_digest,
            &actor,
            correlation_id,
            Some(old_state.as_str()),
            Some(state.as_str()),
            reason,
            &mut ops,
        )?;
        set_history_session(&mut task, operation_id, session_id, &mut ops)?;
        match terminal_report {
            Some(v1::transition_issue_args::TerminalReport::Completion(report)) => {
                set_history_subject_fields(
                    &mut task,
                    operation_id,
                    &[(
                        "completion",
                        serde_json::to_value(report)
                            .map_err(|error| TrackerError::InvalidState(error.to_string()))?,
                    )],
                    &mut ops,
                )?;
            }
            Some(v1::transition_issue_args::TerminalReport::Cancellation(report)) => {
                set_history_subject_fields(
                    &mut task,
                    operation_id,
                    &[(
                        "cancellation",
                        serde_json::to_value(report)
                            .map_err(|error| TrackerError::InvalidState(error.to_string()))?,
                    )],
                    &mut ops,
                )?;
            }
            None => {}
        }
        set_marker(&mut task, operation_id, &transition_digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }
}

// Completion evidence is copied into both history and the replayable marker,
// so every free-form field is bounded independently of transport limits.
const MAX_AWAITING_REVIEW_RUN_ID_BYTES: usize = 256;
const MAX_AWAITING_REVIEW_REFERENCES: usize = 1_000;
const MAX_AWAITING_REVIEW_REFERENCE_BYTES: usize = 2_048;
const MAX_AWAITING_REVIEW_EVIDENCE: usize = 1_000;
const MAX_AWAITING_REVIEW_TEXT_BYTES: usize = 16 * 1024;

fn validate_awaiting_review_completion(
    completion: &v1::AwaitingReviewCompletion,
) -> Result<(), TrackerError> {
    if blank_or_control(&completion.run_id)
        || completion.run_id.len() > MAX_AWAITING_REVIEW_RUN_ID_BYTES
    {
        return Err(TrackerError::InvalidState(
            "awaiting_review run_id is invalid".into(),
        ));
    }
    let commit_bytes = completion.full_commit_id.as_bytes();
    if !matches!(commit_bytes.len(), 40 | 64) || !commit_bytes.iter().all(u8::is_ascii_hexdigit) {
        return Err(TrackerError::InvalidState(
            "awaiting_review requires a full commit id".into(),
        ));
    }
    if completion.evidence_references.is_empty()
        || completion.evidence_references.len() > MAX_AWAITING_REVIEW_REFERENCES
        || completion.evidence_references.iter().any(|reference| {
            blank_or_control(reference) || reference.len() > MAX_AWAITING_REVIEW_REFERENCE_BYTES
        })
    {
        return Err(TrackerError::InvalidState(
            "awaiting_review evidence references are required and must be bounded".into(),
        ));
    }
    reject_secret(
        [
            completion.run_id.as_str(),
            completion.full_commit_id.as_str(),
        ]
        .into_iter()
        .chain(completion.evidence_references.iter().map(String::as_str)),
    )?;
    let report = completion.report.as_ref().ok_or_else(|| {
        TrackerError::InvalidState("awaiting_review completion report is required".into())
    })?;
    match report.result.as_ref() {
        Some(v1::completion_report::Result::Verified(verified)) => {
            if verified.evidence.is_empty()
                || verified.evidence.len() > MAX_AWAITING_REVIEW_EVIDENCE
            {
                return Err(TrackerError::InvalidState(
                    "verified awaiting_review report requires bounded evidence".into(),
                ));
            }
            for evidence in &verified.evidence {
                let kind = v1::CompletionEvidenceKind::try_from(evidence.kind).map_err(|_| {
                    TrackerError::InvalidState("completion evidence kind is invalid".into())
                })?;
                if kind == v1::CompletionEvidenceKind::Unspecified
                    || !bounded_text(&evidence.summary)
                    || evidence
                        .uri
                        .as_deref()
                        .is_some_and(|uri| !bounded_text(uri))
                {
                    return Err(TrackerError::InvalidState(
                        "completion evidence must have a kind and safe reference".into(),
                    ));
                }
            }
        }
        Some(v1::completion_report::Result::Unverified(unverified)) => {
            if !bounded_text(&unverified.reason) || !bounded_text(&unverified.residual_risk) {
                return Err(TrackerError::InvalidState(
                    "unverified awaiting_review report requires a reason and residual risk".into(),
                ));
            }
        }
        None => {
            return Err(TrackerError::InvalidState(
                "awaiting_review completion report result is required".into(),
            ));
        }
    }
    reject_completion_report_secrets(report)
}

fn issue_state_from_proto(state: v1::IssueState) -> Result<IssueState, TrackerError> {
    match state {
        v1::IssueState::Open => Ok(IssueState::Open),
        v1::IssueState::InProgress => Ok(IssueState::InProgress),
        v1::IssueState::Blocked => Ok(IssueState::Blocked),
        v1::IssueState::Deferred => Ok(IssueState::Deferred),
        v1::IssueState::Done => Ok(IssueState::Done),
        v1::IssueState::Canceled => Ok(IssueState::Canceled),
        v1::IssueState::AwaitingReview => Ok(IssueState::AwaitingReview),
        v1::IssueState::Unspecified => {
            Err(TrackerError::InvalidState("target_state is invalid".into()))
        }
    }
}

fn apply_current_state(
    task: &mut taskchampion::Task,
    metadata: &mut IssueMetadata,
    state: IssueState,
    timestamp: u64,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    metadata.completed_at = lifecycle::is_terminal(state).then_some(timestamp);
    set(task, STATE_KEY, state.as_str(), ops)?;
    task.set_status(state.native_status(), ops)?;
    Ok(())
}

fn clear_active_claim(
    task: &mut taskchampion::Task,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    clear(task, LEASE_OWNER_KEY, ops)?;
    clear(task, LEASE_OWNER_CONTEXT_KEY, ops)?;
    clear(task, LEASE_HANDLE_KEY, ops)?;
    clear(task, LEASE_EXPIRES_KEY, ops)?;
    Ok(())
}

fn ownership_attributes_present(task: &taskchampion::Task) -> Vec<String> {
    [
        (LEASE_OWNER_KEY, "lb.lease_owner_hash"),
        (LEASE_OWNER_CONTEXT_KEY, "lb.lease_owner_context"),
        (LEASE_HANDLE_KEY, "lb.lease_handle_hash"),
        (LEASE_EXPIRES_KEY, "lb.lease_expires_at"),
    ]
    .into_iter()
    .filter(|(key, _)| task.get_user_defined_attribute(key).is_some())
    .map(|(_, name)| name.to_owned())
    .collect()
}

/// The persisted ownership tuple is evaluated as a whole.  In particular, an
/// absent expiry must not make a partially written claim look available.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeaseState {
    None,
    Active,
    Expired,
    Inconsistent,
}

pub(crate) fn lease_state(task: &taskchampion::Task, at: u64) -> LeaseState {
    lease_state_from_attributes(
        task.get_user_defined_attribute(LEASE_OWNER_KEY),
        task.get_user_defined_attribute(LEASE_OWNER_CONTEXT_KEY),
        task.get_user_defined_attribute(LEASE_HANDLE_KEY),
        task.get_user_defined_attribute(LEASE_EXPIRES_KEY),
        at,
    )
}

/// Classify a persisted lease without constructing a high-level Task.  The
/// maintenance scanner uses this form because Task construction can build a
/// dependency map for the entire working set.
pub(crate) fn lease_state_from_attributes(
    owner: Option<&str>,
    context: Option<&str>,
    handle: Option<&str>,
    expiry: Option<&str>,
    at: u64,
) -> LeaseState {
    let values = [owner, context, handle, expiry];
    if values.iter().all(|value| value.is_none()) {
        return LeaseState::None;
    }

    let (Some(owner), Some(context), Some(handle), Some(expiry)) =
        (values[0], values[1], values[2], values[3])
    else {
        return LeaseState::Inconsistent;
    };
    if !valid_lease_hash(owner) || !valid_lease_hash(handle) || expiry.is_empty() {
        return LeaseState::Inconsistent;
    }
    if !valid_owner_context(context) {
        return LeaseState::Inconsistent;
    }
    match expiry.parse::<u64>() {
        Ok(expiry) if expiry > at => LeaseState::Active,
        Ok(_) => LeaseState::Expired,
        Err(_) => LeaseState::Inconsistent,
    }
}

fn lease_observation(task: &taskchampion::Task) -> (Option<u64>, Option<u64>) {
    let lease_expires_at = task
        .get_user_defined_attribute(LEASE_EXPIRES_KEY)
        .and_then(|value| value.parse::<u64>().ok());
    let observed_revision = revision(task).ok();
    (lease_expires_at, observed_revision)
}

pub(crate) fn invalid_lease_error(task: &taskchampion::Task) -> TrackerError {
    let (lease_expires_at, observed_revision) = lease_observation(task);
    TrackerError::InvalidLease {
        lease_expires_at,
        observed_revision,
    }
}

pub(crate) fn expired_lease_error(task: &taskchampion::Task) -> TrackerError {
    let (lease_expires_at, observed_revision) = lease_observation(task);
    TrackerError::LeaseExpired {
        lease_expires_at,
        observed_revision,
    }
}

pub(crate) fn inconsistent_lease_error(task: &taskchampion::Task) -> TrackerError {
    let mut invalid_attributes = Vec::new();
    let owner = task.get_user_defined_attribute(LEASE_OWNER_KEY);
    let context = task.get_user_defined_attribute(LEASE_OWNER_CONTEXT_KEY);
    let handle = task.get_user_defined_attribute(LEASE_HANDLE_KEY);
    let expiry = task.get_user_defined_attribute(LEASE_EXPIRES_KEY);
    if owner.is_none_or(|value| !valid_lease_hash(value)) {
        invalid_attributes.push(LEASE_OWNER_KEY.into());
    }
    if context.is_some_and(|value| !valid_owner_context(value)) {
        invalid_attributes.push(LEASE_OWNER_CONTEXT_KEY.into());
    }
    if handle.is_none_or(|value| !valid_lease_hash(value)) {
        invalid_attributes.push(LEASE_HANDLE_KEY.into());
    }
    if expiry.is_none_or(|value| value.is_empty() || value.parse::<u64>().is_err()) {
        invalid_attributes.push(LEASE_EXPIRES_KEY.into());
    }
    let (lease_expires_at, observed_revision) = lease_observation(task);
    TrackerError::LeaseInconsistent {
        invalid_attributes,
        lease_expires_at,
        observed_revision,
    }
}

fn valid_lease_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_owner_context(value: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(value) else {
        return false;
    };
    value
        .as_object()
        .and_then(|object| object.get("actor_id"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|actor| !actor.trim().is_empty())
}

fn validate_active_claim(
    task: &taskchampion::Task,
    expected_revision: u64,
    claim_revision: Option<u64>,
    lease_handle: &str,
) -> Result<(), TrackerError> {
    if claim_revision != Some(expected_revision) {
        return Err(invalid_lease_error(task));
    }
    let lease = lease_state(task, now());
    if lease != LeaseState::Active
        || task.get_user_defined_attribute(LEASE_HANDLE_KEY) != Some(hash(lease_handle).as_str())
    {
        return Err(match lease {
            LeaseState::Expired => expired_lease_error(task),
            LeaseState::Inconsistent => inconsistent_lease_error(task),
            _ => invalid_lease_error(task),
        });
    }
    Ok(())
}

fn reject_update_terminal_report_secrets(
    terminal_report: Option<&v1::update_issue_args::TerminalReport>,
) -> Result<(), TrackerError> {
    if let Some(v1::update_issue_args::TerminalReport::Completion(report)) = terminal_report {
        reject_completion_report_secrets(report)?;
    }
    Ok(())
}

fn reject_completion_report_secrets(report: &v1::CompletionReport) -> Result<(), TrackerError> {
    match report.result.as_ref() {
        Some(v1::completion_report::Result::Verified(verified)) => {
            for evidence in &verified.evidence {
                reject_secret([
                    evidence.summary.as_str(),
                    evidence.uri.as_deref().unwrap_or_default(),
                ])?;
            }
        }
        Some(v1::completion_report::Result::Unverified(unverified)) => {
            reject_secret([
                unverified.reason.as_str(),
                unverified.residual_risk.as_str(),
            ])?;
        }
        None => {}
    }
    Ok(())
}

fn json_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn blank_or_control(value: &str) -> bool {
    value.trim().is_empty() || value.chars().any(char::is_control)
}

fn bounded_text(value: &str) -> bool {
    !blank_or_control(value) && value.len() <= MAX_AWAITING_REVIEW_TEXT_BYTES
}
