use super::super::proto_types::manager_v1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extraction_run_transitions_are_cas_protected() {
        let mut store = ExtractionRunStore::default();
        let run = store
            .create(
                &manager_v1::ExtractionRunDraft {
                    source: "memories".into(),
                    request_digest: "sha256:req".into(),
                    request_schema: "thread-summary.v1".into(),
                    ..Default::default()
                },
                now(1),
            )
            .unwrap();
        let running = store
            .advance(
                "run-1",
                0,
                &manager_v1::ExtractionRunAdvance {
                    target_state: manager_v1::ExtractionRunState::Running as i32,
                    progress_percent: Some(1),
                    ..Default::default()
                },
                now(2),
            )
            .unwrap();
        assert_eq!(running.revision, 1);
        assert!(matches!(
            store.advance(
                "run-1",
                0,
                &manager_v1::ExtractionRunAdvance {
                    target_state: manager_v1::ExtractionRunState::Completed as i32,
                    ..Default::default()
                },
                now(3),
            ),
            Err(ExtractionError::RevisionConflict { .. })
        ));
        let completed = store
            .advance(
                "run-1",
                1,
                &manager_v1::ExtractionRunAdvance {
                    target_state: manager_v1::ExtractionRunState::Completed as i32,
                    progress_percent: Some(100),
                    watermark: Some(4),
                    ..Default::default()
                },
                now(4),
            )
            .unwrap();
        assert_eq!(
            completed.state,
            manager_v1::ExtractionRunState::Completed as i32
        );
        assert_eq!(completed.progress_percent, 100);
        assert_eq!(run.run_id, "run-1");
    }

    #[test]
    fn restored_runs_continue_monotonic_ids_and_preserve_state() {
        let mut original = ExtractionRunStore::default();
        let first = original
            .create(
                &manager_v1::ExtractionRunDraft {
                    source: "memories".into(),
                    request_digest: "sha256:req".into(),
                    request_schema: "v1".into(),
                    ..Default::default()
                },
                now(1),
            )
            .unwrap();
        let mut restored = ExtractionRunStore::from_runs(vec![first]);
        let next = restored
            .create(
                &manager_v1::ExtractionRunDraft {
                    source: "memories".into(),
                    request_digest: "sha256:req-2".into(),
                    request_schema: "v1".into(),
                    ..Default::default()
                },
                now(2),
            )
            .unwrap();
        assert_eq!(next.run_id, "run-2");
    }

    #[test]
    fn terminal_runs_cannot_be_advanced_and_progress_is_bounded() {
        let mut store = ExtractionRunStore::default();
        store
            .create(
                &manager_v1::ExtractionRunDraft {
                    source: "memories".into(),
                    request_digest: "sha256:req".into(),
                    request_schema: "v1".into(),
                    ..Default::default()
                },
                now(1),
            )
            .unwrap();
        assert!(matches!(
            store.advance(
                "run-1",
                0,
                &manager_v1::ExtractionRunAdvance {
                    target_state: manager_v1::ExtractionRunState::Running as i32,
                    progress_percent: Some(101),
                    ..Default::default()
                },
                now(2),
            ),
            Err(ExtractionError::InvalidTransition(_))
        ));
        store
            .advance(
                "run-1",
                0,
                &manager_v1::ExtractionRunAdvance {
                    target_state: manager_v1::ExtractionRunState::Canceled as i32,
                    ..Default::default()
                },
                now(2),
            )
            .unwrap();
        assert!(matches!(
            store.advance(
                "run-1",
                1,
                &manager_v1::ExtractionRunAdvance {
                    target_state: manager_v1::ExtractionRunState::Running as i32,
                    ..Default::default()
                },
                now(3),
            ),
            Err(ExtractionError::InvalidTransition(_))
        ));
    }

    #[test]
    fn request_fields_are_filtered_for_secrets() {
        let mut store = ExtractionRunStore::default();
        assert!(matches!(
            store.create(
                &manager_v1::ExtractionRunDraft {
                    source: "memories".into(),
                    request_digest: "password=secret".into(),
                    request_schema: "v1".into(),
                    ..Default::default()
                },
                now(1),
            ),
            Err(ExtractionError::SecretRejected)
        ));
    }

    #[test]
    fn only_completed_runs_can_advance_success_checkpoint_or_watermark() {
        let mut store = ExtractionRunStore::default();
        store
            .create(
                &manager_v1::ExtractionRunDraft {
                    source: "memories".into(),
                    request_digest: "sha256:req".into(),
                    request_schema: "v1".into(),
                    ..Default::default()
                },
                now(1),
            )
            .unwrap();
        assert!(matches!(
            store.advance(
                "run-1",
                0,
                &manager_v1::ExtractionRunAdvance {
                    target_state: manager_v1::ExtractionRunState::Canceled as i32,
                    checkpoint: Some("checkpoint".into()),
                    watermark: Some(1),
                    ..Default::default()
                },
                now(2),
            ),
            Err(ExtractionError::InvalidTransition(_))
        ));
    }

    fn now(value: i64) -> manager_v1::UtcInstant {
        manager_v1::UtcInstant {
            unix_epoch_millis: value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExtractionError {
    NotFound(String),
    RevisionConflict { expected: u64, actual: u64 },
    InvalidTransition(&'static str),
    InvalidInput(&'static str),
    SecretRejected,
}

#[derive(Default)]
pub(crate) struct ExtractionRunStore {
    runs: std::collections::BTreeMap<String, manager_v1::ExtractionRun>,
    next_id: u64,
}

impl ExtractionRunStore {
    pub(crate) fn from_runs(runs: Vec<manager_v1::ExtractionRun>) -> Self {
        let next_id = runs
            .iter()
            .filter_map(|run| run.run_id.strip_prefix("run-")?.parse().ok())
            .max()
            .unwrap_or_default();
        Self {
            runs: runs
                .into_iter()
                .map(|run| (run.run_id.clone(), run))
                .collect(),
            next_id,
        }
    }
    pub(crate) fn create(
        &mut self,
        draft: &manager_v1::ExtractionRunDraft,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::ExtractionRun, ExtractionError> {
        if draft.source.is_empty()
            || draft.request_digest.is_empty()
            || draft.request_schema.is_empty()
        {
            return Err(ExtractionError::InvalidInput(
                "source, request_digest and request_schema are required",
            ));
        }
        validate_text(&draft.source)?;
        validate_text(&draft.request_digest)?;
        validate_text(&draft.request_schema)?;
        if let Some(parent) = &draft.causal_parent {
            validate_text(parent)?;
        }
        self.next_id += 1;
        let run = manager_v1::ExtractionRun {
            run_id: format!("run-{}", self.next_id),
            revision: 0,
            state: manager_v1::ExtractionRunState::Created as i32,
            source: draft.source.clone(),
            request_digest: draft.request_digest.clone(),
            request_schema: draft.request_schema.clone(),
            causal_parent: draft.causal_parent.clone(),
            created_at: Some(now.clone()),
            updated_at: Some(now),
            ..Default::default()
        };
        self.runs.insert(run.run_id.clone(), run.clone());
        Ok(run)
    }

    pub(crate) fn get(&self, id: &str) -> Result<manager_v1::ExtractionRun, ExtractionError> {
        self.runs
            .get(id)
            .cloned()
            .ok_or_else(|| ExtractionError::NotFound(id.into()))
    }

    pub(crate) fn list(&self) -> Vec<manager_v1::ExtractionRun> {
        self.runs.values().cloned().collect()
    }

    pub(crate) fn attach_checkpoint(
        &mut self,
        id: &str,
        checkpoint: &str,
    ) -> Result<(), ExtractionError> {
        let current = self.get(id)?;
        let mut updated = current;
        updated.checkpoint = Some(checkpoint.into());
        self.runs.insert(updated.run_id.clone(), updated);
        Ok(())
    }

    pub(crate) fn attach_apply_metadata(
        &mut self,
        id: &str,
        policy: manager_v1::ExtractionPolicy,
        policy_digest: String,
        truncation: manager_v1::ExtractionTruncationRecord,
    ) -> Result<manager_v1::ExtractionRun, ExtractionError> {
        let mut updated = self.get(id)?;
        updated.policy = Some(policy);
        updated.policy_digest = policy_digest;
        updated.truncation = Some(truncation);
        self.runs.insert(updated.run_id.clone(), updated.clone());
        Ok(updated)
    }

    pub(crate) fn advance(
        &mut self,
        id: &str,
        expected_revision: u64,
        advance: &manager_v1::ExtractionRunAdvance,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::ExtractionRun, ExtractionError> {
        let current = self.get(id)?;
        if current.revision != expected_revision {
            return Err(ExtractionError::RevisionConflict {
                expected: expected_revision,
                actual: current.revision,
            });
        }
        let target = manager_v1::ExtractionRunState::try_from(advance.target_state)
            .map_err(|_| ExtractionError::InvalidTransition("unknown target state"))?;
        let source = manager_v1::ExtractionRunState::try_from(current.state)
            .map_err(|_| ExtractionError::InvalidTransition("stored state is unknown"))?;
        if !allowed_transition(source, target) {
            return Err(ExtractionError::InvalidTransition(
                "state transition is not allowed",
            ));
        }
        if advance.progress_percent.is_some_and(|value| value > 100) {
            return Err(ExtractionError::InvalidTransition(
                "progress must be between 0 and 100",
            ));
        }
        if target == manager_v1::ExtractionRunState::Completed
            && advance.progress_percent != Some(100)
        {
            return Err(ExtractionError::InvalidTransition(
                "completed run must have 100 percent progress",
            ));
        }
        if target != manager_v1::ExtractionRunState::Completed
            && (advance.checkpoint.is_some() || advance.watermark.is_some())
        {
            return Err(ExtractionError::InvalidTransition(
                "checkpoint and watermark advance only on completion",
            ));
        }
        if target != manager_v1::ExtractionRunState::Failed
            && target != manager_v1::ExtractionRunState::Unconfirmed
            && advance.error.is_some()
        {
            return Err(ExtractionError::InvalidTransition(
                "error is only allowed for failed or unconfirmed runs",
            ));
        }
        if let Some(error) = &advance.error {
            validate_text(error)?;
        }
        if let Some(checkpoint) = &advance.checkpoint {
            validate_text(checkpoint)?;
        }
        let mut updated = current;
        updated.revision += 1;
        updated.state = target as i32;
        if let Some(progress) = advance.progress_percent {
            updated.progress_percent = progress;
        }
        if let Some(checkpoint) = &advance.checkpoint {
            updated.checkpoint = Some(checkpoint.clone());
        }
        if let Some(watermark) = advance.watermark {
            updated.watermark = watermark;
        }
        if let Some(error) = &advance.error {
            updated.error = Some(error.clone());
        }
        updated.updated_at = Some(now);
        self.runs.insert(id.into(), updated.clone());
        Ok(updated)
    }
}

fn allowed_transition(
    source: manager_v1::ExtractionRunState,
    target: manager_v1::ExtractionRunState,
) -> bool {
    use manager_v1::ExtractionRunState::*;
    matches!(
        (source, target),
        (Created, Running | Canceled)
            | (Running, Paused | Completed | Failed | Canceled)
            | (Paused, Running | Canceled)
            | (Failed, Running)
            | (Unconfirmed, Running)
    )
}

fn validate_text(value: &str) -> Result<(), ExtractionError> {
    let lower = value.to_ascii_lowercase();
    let markers = [
        "sk-",
        "bearer ",
        "api_key",
        "secret=",
        "token=",
        "password=",
        "ghp_",
        "xoxb-",
        "-----begin ",
    ];
    if markers.iter().any(|marker| lower.contains(marker)) {
        return Err(ExtractionError::SecretRejected);
    }
    Ok(())
}
