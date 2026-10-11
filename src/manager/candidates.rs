use super::{
    super::proto_types::{manager_v1, v1},
    config::SourceEvidenceLimits,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn draft() -> manager_v1::CandidateDraft {
        manager_v1::CandidateDraft {
            candidate_id: "candidate-1".into(),
            title: "Fix the parser".into(),
            description: "The parser fails on an empty input".into(),
            kind: manager_v1::CandidateKind::Bug as i32,
            suggested_priority: manager_v1::CandidatePriority::P2 as i32,
            suggested_state: manager_v1::CandidateSuggestedState::Open as i32,
            acceptance_criteria: vec!["empty input is accepted".into()],
            detection_basis: manager_v1::DetectionBasis::Explicit as i32,
            ..Default::default()
        }
    }

    fn now(value: i64) -> manager_v1::UtcInstant {
        manager_v1::UtcInstant {
            unix_epoch_millis: value,
        }
    }

    fn source_evidence_limits() -> SourceEvidenceLimits {
        SourceEvidenceLimits {
            max_string_bytes: 4,
            max_repeated_item_count: 2,
        }
    }

    fn source_reference(thread_id: &str, memory_id: &str) -> v1::SourceEvidenceRef {
        v1::SourceEvidenceRef {
            thread_id: thread_id.into(),
            memory_id: memory_id.into(),
        }
    }

    #[test]
    fn source_evidence_applies_count_and_string_limits_before_persistence() {
        let mut store = CandidateStore::with_limits(source_evidence_limits());
        let mut too_many = draft();
        too_many.source_evidence = vec![
            source_reference("t-1", "m-1"),
            source_reference("t-2", "m-2"),
            source_reference("t-3", "m-3"),
        ];
        assert!(matches!(
            store.create(&too_many, now(1)),
            Err(CandidateError::InvalidPatch(
                "source evidence exceeds the repeated-item limit"
            ))
        ));

        let mut too_long = draft();
        too_long.source_evidence = vec![source_reference("longer", "m-1")];
        assert!(matches!(
            store.create(&too_long, now(1)),
            Err(CandidateError::InvalidPatch(
                "source evidence ID exceeds the string limit"
            ))
        ));

        let mut accepted = draft();
        accepted.source_evidence = vec![
            source_reference("t-1", "m-1"),
            source_reference("t-1", "m-1"),
        ];
        let candidate = store.create(&accepted, now(1)).expect("within limits");
        assert_eq!(
            candidate.source_evidence,
            vec![source_reference("t-1", "m-1")]
        );

        assert!(matches!(
            store.update(
                &candidate.candidate_id,
                candidate.revision,
                &[manager_v1::CandidateField::SourceEvidence as i32],
                &manager_v1::CandidatePatch {
                    source_evidence: vec![
                        source_reference("t-1", "m-1"),
                        source_reference("t-2", "m-2"),
                        source_reference("t-3", "m-3")
                    ],
                    ..Default::default()
                },
                now(2),
            ),
            Err(CandidateError::InvalidPatch(
                "source evidence exceeds the repeated-item limit"
            ))
        ));

        let mut blank = draft();
        blank.candidate_id = "candidate-blank".into();
        blank.source_evidence = vec![source_reference(" ", "m-1")];
        assert!(matches!(
            store.create(&blank, now(1)),
            Err(CandidateError::InvalidPatch(
                "source evidence thread_id and memory_id are required"
            ))
        ));

        let mut ordered_store = CandidateStore::with_limits(SourceEvidenceLimits {
            max_string_bytes: 16,
            max_repeated_item_count: 8,
        });
        let second = source_reference("t-2", "m-2");
        let first = source_reference("t-1", "m-1");
        let third = source_reference("t-3", "m-3");
        let mut ordered = draft();
        ordered.source_evidence = vec![second.clone(), first.clone(), second.clone()];
        let created = ordered_store
            .create(&ordered, now(1))
            .expect("create normalizes references");
        assert_eq!(created.source_evidence, vec![second.clone(), first.clone()]);
        let updated = ordered_store
            .update(
                &created.candidate_id,
                created.revision,
                &[manager_v1::CandidateField::SourceEvidence as i32],
                &manager_v1::CandidatePatch {
                    source_evidence: vec![third.clone(), first.clone(), third.clone()],
                    ..Default::default()
                },
                now(2),
            )
            .expect("update normalizes references");
        assert_eq!(updated.source_evidence, vec![third, first]);
    }

    #[test]
    fn create_and_update_use_zero_based_revision_and_cas() {
        let mut store = CandidateStore::default();
        let candidate = store.create(&draft(), now(1)).unwrap();
        assert_eq!(candidate.revision, 0);

        let updated = store
            .update(
                "candidate-1",
                0,
                &[manager_v1::CandidateField::Title as i32],
                &manager_v1::CandidatePatch {
                    title: Some("Fix the empty-input parser".into()),
                    ..Default::default()
                },
                now(2),
            )
            .unwrap();
        assert_eq!(updated.revision, 1);
        assert_eq!(updated.title, "Fix the empty-input parser");
        assert!(matches!(
            store.update(
                "candidate-1",
                0,
                &[manager_v1::CandidateField::Title as i32],
                &manager_v1::CandidatePatch {
                    title: Some("stale".into()),
                    ..Default::default()
                },
                now(3),
            ),
            Err(CandidateError::RevisionConflict { .. })
        ));
    }

    #[test]
    fn restored_candidates_keep_their_revision_for_cas_updates() {
        let mut original = CandidateStore::default();
        let candidate = original.create(&draft(), now(1)).unwrap();
        let mut restored = CandidateStore::from_candidates(vec![candidate]);

        assert!(matches!(
            restored.update(
                "candidate-1",
                1,
                &[manager_v1::CandidateField::Title as i32],
                &manager_v1::CandidatePatch {
                    title: Some("stale".into()),
                    ..Default::default()
                },
                now(2),
            ),
            Err(CandidateError::RevisionConflict {
                expected: 1,
                actual: 0
            })
        ));
    }

    #[test]
    fn duplicate_update_mask_and_unknown_fields_are_rejected() {
        let mut store = CandidateStore::default();
        store.create(&draft(), now(1)).unwrap();
        assert!(matches!(
            store.update(
                "candidate-1",
                0,
                &[
                    manager_v1::CandidateField::Title as i32,
                    manager_v1::CandidateField::Title as i32
                ],
                &manager_v1::CandidatePatch::default(),
                now(2),
            ),
            Err(CandidateError::InvalidPatch(_))
        ));
        assert!(matches!(
            store.update(
                "candidate-1",
                0,
                &[99],
                &manager_v1::CandidatePatch::default(),
                now(2),
            ),
            Err(CandidateError::InvalidPatch(_))
        ));
    }

    #[test]
    fn metadata_and_acceptance_criteria_are_filtered_for_secrets() {
        let mut value = draft();
        value.acceptance_criteria = vec!["password=must-not-persist".into()];
        assert!(matches!(
            CandidateStore::default().create(&value, now(1)),
            Err(CandidateError::SecretRejected)
        ));
        let mut value = draft();
        value.metadata = vec![manager_v1::MetadataEntry {
            key: "token".into(),
            value: "sk-live".into(),
        }];
        assert!(matches!(
            CandidateStore::default().create(&value, now(1)),
            Err(CandidateError::SecretRejected)
        ));
    }

    #[test]
    fn create_rejects_detection_basis_strong() {
        let mut invalid = draft();
        invalid.detection_basis = manager_v1::DetectionBasis::Strong as i32;
        assert!(matches!(
            CandidateStore::default().create(&invalid, now(1)),
            Err(CandidateError::InvalidPatch(_))
        ));
    }

    #[test]
    fn creation_and_patch_reject_unspecified_or_unknown_enums() {
        let mut invalid = draft();
        invalid.kind = manager_v1::CandidateKind::Unspecified as i32;
        assert!(matches!(
            CandidateStore::default().create(&invalid, now(1)),
            Err(CandidateError::InvalidPatch(_))
        ));

        let mut store = CandidateStore::default();
        store.create(&draft(), now(1)).unwrap();
        assert!(matches!(
            store.update(
                "candidate-1",
                0,
                &[manager_v1::CandidateField::SuggestedPriority as i32],
                &manager_v1::CandidatePatch {
                    suggested_priority: Some(99),
                    ..Default::default()
                },
                now(2),
            ),
            Err(CandidateError::InvalidPatch(_))
        ));
    }

    #[test]
    fn changed_patch_clears_auto_registration_proof_but_noop_keeps_it() {
        let mut store = CandidateStore::default();
        let mut candidate = store.create(&draft(), now(1)).unwrap();
        candidate.strong_detection_proof = Some(manager_v1::StrongDetectionProof {
            model_id: "model-1".into(),
            ..Default::default()
        });
        store = CandidateStore::from_candidates(vec![candidate]);

        let noop = store
            .update(
                "candidate-1",
                0,
                &[manager_v1::CandidateField::Title as i32],
                &manager_v1::CandidatePatch {
                    title: Some("Fix the parser".into()),
                    ..Default::default()
                },
                now(2),
            )
            .unwrap();
        assert_eq!(noop.revision, 1);
        assert!(noop.strong_detection_proof.is_some());

        let changed = store
            .update(
                "candidate-1",
                1,
                &[manager_v1::CandidateField::Title as i32],
                &manager_v1::CandidatePatch {
                    title: Some("Fix the empty-input parser".into()),
                    ..Default::default()
                },
                now(3),
            )
            .unwrap();
        assert_eq!(changed.revision, 2);
        assert!(changed.strong_detection_proof.is_none());
    }

    #[test]
    fn common_filter_matches_reviewable_candidate_fields() {
        let mut candidate = CandidateStore::default()
            .create(&draft(), now(1))
            .expect("candidate");
        candidate.title = "Fix the Parser".into();
        candidate.description = "Accept empty input".into();
        candidate.kind = manager_v1::CandidateKind::Bug as i32;
        candidate.suggested_priority = manager_v1::CandidatePriority::P1 as i32;
        candidate.extraction_run_id = "run-1".into();
        candidate.source_evidence = vec![source_reference("thread-1", "memory-1")];

        assert!(candidate_matches_common(
            &candidate,
            &[manager_v1::CandidateKind::Bug as i32],
            &[manager_v1::CandidatePriority::P1 as i32],
            Some("run-1"),
            Some("parser"),
            Some(&source_reference("thread-1", "memory-1")),
        ));
        assert!(!candidate_matches_common(
            &candidate,
            &[manager_v1::CandidateKind::Task as i32],
            &[],
            None,
            None,
            None,
        ));
        assert!(!candidate_matches_common(
            &candidate,
            &[],
            &[],
            None,
            None,
            Some(&source_reference(" ", "memory-1")),
        ));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CandidateError {
    NotFound(String),
    RevisionConflict { expected: u64, actual: u64 },
    NotProposed,
    InvalidPatch(&'static str),
    InvalidAcceptance(&'static str),
    SecretRejected,
}

pub(crate) struct CandidateStore {
    candidates: std::collections::BTreeMap<String, manager_v1::Candidate>,
    accepting: std::collections::BTreeSet<String>,
    source_evidence_limits: SourceEvidenceLimits,
}

/// Match fields shared by the normal Candidate list and the trusted-consumer
/// review projection. Status remains a view-specific decision.
pub(crate) fn candidate_matches_common(
    candidate: &manager_v1::Candidate,
    kinds: &[i32],
    priorities: &[i32],
    extraction_run_id: Option<&str>,
    text_query: Option<&str>,
    source_evidence: Option<&v1::SourceEvidenceRef>,
) -> bool {
    (kinds.is_empty() || kinds.contains(&candidate.kind))
        && (priorities.is_empty() || priorities.contains(&candidate.suggested_priority))
        && extraction_run_id.is_none_or(|run_id| candidate.extraction_run_id == run_id)
        && text_query.is_none_or(|query| {
            let query = query.to_ascii_lowercase();
            candidate.title.to_ascii_lowercase().contains(&query)
                || candidate.description.to_ascii_lowercase().contains(&query)
        })
        && source_evidence.is_none_or(|reference| {
            !reference.thread_id.trim().is_empty()
                && !reference.memory_id.trim().is_empty()
                && candidate.source_evidence.iter().any(|candidate_reference| {
                    candidate_reference.thread_id == reference.thread_id
                        && candidate_reference.memory_id == reference.memory_id
                })
        })
}

impl Default for CandidateStore {
    fn default() -> Self {
        Self::with_limits(SourceEvidenceLimits {
            max_string_bytes: u64::MAX,
            max_repeated_item_count: u64::MAX,
        })
    }
}

impl CandidateStore {
    pub(crate) fn with_limits(source_evidence_limits: SourceEvidenceLimits) -> Self {
        Self {
            candidates: std::collections::BTreeMap::new(),
            accepting: std::collections::BTreeSet::new(),
            source_evidence_limits,
        }
    }

    pub(crate) fn from_candidates(candidates: Vec<manager_v1::Candidate>) -> Self {
        Self::from_candidates_with_limits(
            candidates,
            SourceEvidenceLimits {
                max_string_bytes: u64::MAX,
                max_repeated_item_count: u64::MAX,
            },
        )
    }

    pub(crate) fn from_candidates_with_limits(
        candidates: Vec<manager_v1::Candidate>,
        source_evidence_limits: SourceEvidenceLimits,
    ) -> Self {
        Self {
            candidates: candidates
                .into_iter()
                .map(|candidate| (candidate.candidate_id.clone(), candidate))
                .collect(),
            accepting: std::collections::BTreeSet::new(),
            source_evidence_limits,
        }
    }
    pub(crate) fn create(
        &mut self,
        draft: &manager_v1::CandidateDraft,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::Candidate, CandidateError> {
        validate_candidate_text(draft.title.as_str(), draft.description.as_str())?;
        validate_candidate_values(&draft.acceptance_criteria, &draft.metadata)?;
        validate_candidate_enums(
            draft.kind,
            draft.suggested_priority,
            draft.suggested_state,
            draft.detection_basis,
        )?;
        if draft.candidate_id.is_empty() || draft.title.trim().is_empty() {
            return Err(CandidateError::InvalidPatch(
                "candidate_id and title are required",
            ));
        }
        if self.candidates.contains_key(&draft.candidate_id) {
            return Err(CandidateError::InvalidPatch("candidate already exists"));
        }
        let candidate = manager_v1::Candidate {
            candidate_id: draft.candidate_id.clone(),
            title: draft.title.clone(),
            description: draft.description.clone(),
            kind: draft.kind,
            suggested_priority: draft.suggested_priority,
            suggested_state: draft.suggested_state,
            acceptance_criteria: draft.acceptance_criteria.clone(),
            detection_basis: draft.detection_basis,
            confidence_factors: draft.confidence_factors.clone(),
            revision: 0,
            source_evidence: normalize_source_evidence(
                &draft.source_evidence,
                self.source_evidence_limits,
            )?,
            extraction_run_id: draft.extraction_run_id.clone(),
            extraction_revision: draft.extraction_revision,
            duplicate_candidate_ids: draft.duplicate_candidate_ids.clone(),
            status: manager_v1::CandidateStatus::Proposed as i32,
            created_at: Some(now.clone()),
            updated_at: Some(now),
            metadata: draft.metadata.clone(),
            due_at: draft.due_at.clone(),
            ..Default::default()
        };
        self.candidates
            .insert(candidate.candidate_id.clone(), candidate.clone());
        Ok(candidate)
    }

    pub(crate) fn get(&self, id: &str) -> Result<manager_v1::Candidate, CandidateError> {
        self.candidates
            .get(id)
            .cloned()
            .ok_or_else(|| CandidateError::NotFound(id.into()))
    }

    pub(crate) fn list(&self) -> Vec<manager_v1::Candidate> {
        self.candidates.values().cloned().collect()
    }

    pub(crate) fn record_extraction_outcome(
        &mut self,
        id: &str,
        proof: Option<manager_v1::StrongDetectionProof>,
        auto_registered_issue_id: Option<String>,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::Candidate, CandidateError> {
        self.mutate_proposed(id, now, |updated| {
            updated.strong_detection_proof = proof;
            if let Some(issue_id) = auto_registered_issue_id {
                updated.status = manager_v1::CandidateStatus::AutoRegistered as i32;
                updated.superseded_by = Some(
                    manager_v1::candidate::SupersededBy::SupersedingIssueId(issue_id),
                );
            }
        })
    }

    pub(crate) fn supersede_proposed(
        &mut self,
        id: &str,
        replacement_candidate_id: &str,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::Candidate, CandidateError> {
        self.mutate_proposed(id, now, |updated| {
            updated.status = manager_v1::CandidateStatus::Superseded as i32;
            updated.superseded_by =
                Some(manager_v1::candidate::SupersededBy::SupersedingCandidateId(
                    replacement_candidate_id.into(),
                ));
        })
    }

    fn mutate_proposed(
        &mut self,
        id: &str,
        now: manager_v1::UtcInstant,
        mutate: impl FnOnce(&mut manager_v1::Candidate),
    ) -> Result<manager_v1::Candidate, CandidateError> {
        let current = self.get(id)?;
        if current.status != manager_v1::CandidateStatus::Proposed as i32 {
            return Err(CandidateError::NotProposed);
        }
        let mut updated = current;
        mutate(&mut updated);
        updated.revision += 1;
        updated.updated_at = Some(now);
        self.candidates
            .insert(updated.candidate_id.clone(), updated.clone());
        Ok(updated)
    }

    pub(crate) fn update(
        &mut self,
        id: &str,
        expected_revision: u64,
        update_mask: &[i32],
        patch: &manager_v1::CandidatePatch,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::Candidate, CandidateError> {
        let current = self
            .candidates
            .get(id)
            .ok_or_else(|| CandidateError::NotFound(id.into()))?;
        check_revision(current.revision, expected_revision)?;
        if current.status != manager_v1::CandidateStatus::Proposed as i32 {
            return Err(CandidateError::NotProposed);
        }
        let mut fields = std::collections::BTreeSet::new();
        for field in update_mask {
            if *field == manager_v1::CandidateField::Unspecified as i32
                || !fields.insert(*field)
                || manager_v1::CandidateField::try_from(*field).is_err()
            {
                return Err(CandidateError::InvalidPatch("update mask is not closed"));
            }
        }
        if update_mask.is_empty() {
            return Err(CandidateError::InvalidPatch("update mask is required"));
        }
        let mut updated = current.clone();
        for field in update_mask {
            match manager_v1::CandidateField::try_from(*field)
                .map_err(|_| CandidateError::InvalidPatch("unknown update field"))?
            {
                manager_v1::CandidateField::Title => {
                    updated.title = patch
                        .title
                        .clone()
                        .ok_or(CandidateError::InvalidPatch("title is missing"))?;
                    if updated.title.trim().is_empty() {
                        return Err(CandidateError::InvalidPatch("title is empty"));
                    }
                    validate_candidate_text(updated.title.as_str(), "")?;
                }
                manager_v1::CandidateField::Description => {
                    updated.description = patch
                        .description
                        .clone()
                        .ok_or(CandidateError::InvalidPatch("description is missing"))?;
                    validate_candidate_text("", updated.description.as_str())?;
                }
                manager_v1::CandidateField::Kind => {
                    let value = patch
                        .kind
                        .ok_or(CandidateError::InvalidPatch("kind is missing"))?;
                    validate_required_enum(
                        value,
                        manager_v1::CandidateKind::Unspecified as i32,
                        manager_v1::CandidateKind::try_from(value).is_ok(),
                        "kind is invalid",
                    )?;
                    updated.kind = value;
                }
                manager_v1::CandidateField::SuggestedPriority => {
                    let value = patch
                        .suggested_priority
                        .ok_or(CandidateError::InvalidPatch(
                            "suggested_priority is missing",
                        ))?;
                    validate_required_enum(
                        value,
                        manager_v1::CandidatePriority::Unspecified as i32,
                        manager_v1::CandidatePriority::try_from(value).is_ok(),
                        "suggested_priority is invalid",
                    )?;
                    updated.suggested_priority = value;
                }
                manager_v1::CandidateField::SuggestedState => {
                    let value = patch
                        .suggested_state
                        .ok_or(CandidateError::InvalidPatch("suggested_state is missing"))?;
                    validate_required_enum(
                        value,
                        manager_v1::CandidateSuggestedState::Unspecified as i32,
                        manager_v1::CandidateSuggestedState::try_from(value).is_ok(),
                        "suggested_state is invalid",
                    )?;
                    updated.suggested_state = value;
                }
                manager_v1::CandidateField::AcceptanceCriteria => {
                    validate_candidate_values(&patch.acceptance_criteria, &updated.metadata)?;
                    updated.acceptance_criteria = patch.acceptance_criteria.clone();
                }
                manager_v1::CandidateField::DueAt => {
                    updated.due_at = match patch.due_at_change.as_ref() {
                        Some(manager_v1::candidate_patch::DueAtChange::DueAt(value)) => {
                            Some(value.clone())
                        }
                        Some(manager_v1::candidate_patch::DueAtChange::ClearDueAt(_)) => None,
                        None => {
                            return Err(CandidateError::InvalidPatch("due_at is missing"));
                        }
                    }
                }
                manager_v1::CandidateField::SourceEvidence => {
                    updated.source_evidence = normalize_source_evidence(
                        &patch.source_evidence,
                        self.source_evidence_limits,
                    )?;
                }
                manager_v1::CandidateField::Unspecified => unreachable!(),
            }
        }
        // Any material human edit invalidates an extraction proof because the
        // proof was calculated from the pre-edit candidate content.
        if updated != *current {
            updated.strong_detection_proof = None;
        }
        updated.revision += 1;
        updated.updated_at = Some(now);
        self.candidates.insert(id.into(), updated.clone());
        Ok(updated)
    }

    pub(crate) fn begin_accept(
        &mut self,
        id: &str,
        expected_revision: u64,
    ) -> Result<(), CandidateError> {
        let candidate = self.get(id)?;
        check_revision(candidate.revision, expected_revision)?;
        if candidate.status != manager_v1::CandidateStatus::Proposed as i32 {
            return Err(CandidateError::NotProposed);
        }
        if !self.accepting.insert(id.into()) {
            return Err(CandidateError::InvalidAcceptance(
                "acceptance already in progress",
            ));
        }
        Ok(())
    }

    pub(crate) fn finish_accept(
        &mut self,
        id: &str,
        expected_revision: u64,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::Candidate, CandidateError> {
        if !self.accepting.contains(id) {
            return Err(CandidateError::InvalidAcceptance(
                "acceptance is not in progress",
            ));
        }
        let result = self.accept_inner(id, expected_revision, now);
        self.accepting.remove(id);
        result
    }

    pub(crate) fn accept(
        &mut self,
        id: &str,
        expected_revision: u64,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::Candidate, CandidateError> {
        self.begin_accept(id, expected_revision)?;
        self.finish_accept(id, expected_revision, now)
    }

    fn accept_inner(
        &mut self,
        id: &str,
        expected_revision: u64,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::Candidate, CandidateError> {
        let current = self.get(id)?;
        check_revision(current.revision, expected_revision)?;
        let mut accepted = current;
        accepted.revision += 1;
        accepted.status = manager_v1::CandidateStatus::Accepted as i32;
        accepted.decision_reason = manager_v1::CandidateDecisionReason::Accepted as i32;
        accepted.updated_at = Some(now);
        self.candidates.insert(id.into(), accepted.clone());
        Ok(accepted)
    }

    #[allow(dead_code)]
    pub(crate) fn reject(
        &mut self,
        id: &str,
        expected_revision: u64,
        reason: i32,
        rationale: &str,
        now: manager_v1::UtcInstant,
    ) -> Result<manager_v1::Candidate, CandidateError> {
        let current = self.get(id)?;
        check_revision(current.revision, expected_revision)?;
        if current.status != manager_v1::CandidateStatus::Proposed as i32 {
            return Err(CandidateError::NotProposed);
        }
        if manager_v1::CandidateRejectionReason::try_from(reason).is_err()
            || reason == manager_v1::CandidateRejectionReason::Unspecified as i32
            || rationale.trim().is_empty()
        {
            return Err(CandidateError::InvalidPatch(
                "rejection reason and rationale are required",
            ));
        }
        validate_candidate_text(rationale, "")?;
        let mut rejected = current;
        rejected.revision += 1;
        rejected.status = manager_v1::CandidateStatus::Rejected as i32;
        rejected.decision_reason = manager_v1::CandidateDecisionReason::Rejected as i32;
        rejected.decision_note = Some(rationale.into());
        rejected.updated_at = Some(now);
        self.candidates.insert(id.into(), rejected.clone());
        Ok(rejected)
    }
}

pub(crate) fn metadata_value<'a>(
    candidate: &'a manager_v1::Candidate,
    key: &str,
) -> Option<&'a str> {
    candidate
        .metadata
        .iter()
        .find(|entry| entry.key == key)
        .map(|entry| entry.value.as_str())
}

fn check_revision(actual: u64, expected: u64) -> Result<(), CandidateError> {
    (actual == expected)
        .then_some(())
        .ok_or(CandidateError::RevisionConflict { expected, actual })
}

fn validate_candidate_enums(
    kind: i32,
    priority: i32,
    state: i32,
    detection_basis: i32,
) -> Result<(), CandidateError> {
    validate_required_enum(
        kind,
        manager_v1::CandidateKind::Unspecified as i32,
        manager_v1::CandidateKind::try_from(kind).is_ok(),
        "kind is invalid",
    )?;
    validate_required_enum(
        priority,
        manager_v1::CandidatePriority::Unspecified as i32,
        manager_v1::CandidatePriority::try_from(priority).is_ok(),
        "suggested_priority is invalid",
    )?;
    validate_required_enum(
        state,
        manager_v1::CandidateSuggestedState::Unspecified as i32,
        manager_v1::CandidateSuggestedState::try_from(state).is_ok(),
        "suggested_state is invalid",
    )?;
    validate_required_enum(
        detection_basis,
        manager_v1::DetectionBasis::Unspecified as i32,
        manager_v1::DetectionBasis::try_from(detection_basis).is_ok(),
        "detection_basis is invalid",
    )?;
    if detection_basis == manager_v1::DetectionBasis::Strong as i32 {
        return Err(CandidateError::InvalidPatch(
            "detection_basis STRONG is not accepted on create or update",
        ));
    }
    Ok(())
}

fn validate_required_enum(
    value: i32,
    unspecified: i32,
    is_known: bool,
    message: &'static str,
) -> Result<(), CandidateError> {
    (is_known && value != unspecified)
        .then_some(())
        .ok_or(CandidateError::InvalidPatch(message))
}

fn validate_candidate_text(title: &str, description: &str) -> Result<(), CandidateError> {
    let lower = format!("{title}\n{description}").to_ascii_lowercase();
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
        return Err(CandidateError::SecretRejected);
    }
    Ok(())
}

fn validate_candidate_values(
    acceptance_criteria: &[String],
    metadata: &[manager_v1::MetadataEntry],
) -> Result<(), CandidateError> {
    for value in acceptance_criteria {
        validate_candidate_text("", value)?;
    }
    for entry in metadata {
        validate_candidate_text(&entry.key, &entry.value)?;
    }
    Ok(())
}

pub(crate) fn union_source_evidence(
    first: &[v1::SourceEvidenceRef],
    second: &[v1::SourceEvidenceRef],
    limits: SourceEvidenceLimits,
) -> Result<Vec<v1::SourceEvidenceRef>, CandidateError> {
    let mut seen = std::collections::HashSet::new();
    let mut normalized = Vec::new();
    for value in first.iter().chain(second) {
        validate_source_evidence_reference(value, limits)?;
        if seen.insert((value.thread_id.clone(), value.memory_id.clone())) {
            if u64::try_from(normalized.len()).unwrap_or(u64::MAX) >= limits.max_repeated_item_count
            {
                return Err(CandidateError::InvalidPatch(
                    "source evidence exceeds the repeated-item limit",
                ));
            }
            normalized.push(value.clone());
        }
    }
    Ok(normalized)
}

pub(crate) fn normalize_source_evidence(
    values: &[v1::SourceEvidenceRef],
    limits: SourceEvidenceLimits,
) -> Result<Vec<v1::SourceEvidenceRef>, CandidateError> {
    if u64::try_from(values.len()).unwrap_or(u64::MAX) > limits.max_repeated_item_count {
        return Err(CandidateError::InvalidPatch(
            "source evidence exceeds the repeated-item limit",
        ));
    }
    union_source_evidence(&[], values, limits)
}

fn validate_source_evidence_reference(
    value: &v1::SourceEvidenceRef,
    limits: SourceEvidenceLimits,
) -> Result<(), CandidateError> {
    if value.thread_id.trim().is_empty() || value.memory_id.trim().is_empty() {
        return Err(CandidateError::InvalidPatch(
            "source evidence thread_id and memory_id are required",
        ));
    }
    if u64::try_from(value.thread_id.len()).unwrap_or(u64::MAX) > limits.max_string_bytes
        || u64::try_from(value.memory_id.len()).unwrap_or(u64::MAX) > limits.max_string_bytes
    {
        return Err(CandidateError::InvalidPatch(
            "source evidence ID exceeds the string limit",
        ));
    }
    Ok(())
}
