//! Extraction apply and auto-registration persistence.

use super::*;

impl<'a> ManagerRepository<'a> {
    #[allow(dead_code)]
    pub(crate) fn extraction_apply_replay(
        &self,
        idempotency_key: &str,
        request_digest: &str,
    ) -> Result<ExtractionApplyReplayDecision, RepositoryError> {
        if idempotency_key.is_empty() || request_digest.is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "extraction apply idempotency key and request digest are required".into(),
            ));
        }
        self.storage
            .read(|connection| {
                lookup_extraction_apply_replay(connection, idempotency_key, request_digest)
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    /// Reads the immutable extraction identities for currently REJECTED
    /// candidates. Suppression never derives its group from mutable metadata.
    pub(crate) fn rejected_extraction_identities(
        &self,
    ) -> Result<Vec<ExtractionCandidateIdentity>, RepositoryError> {
        let rejected_ids = self
            .candidates()
            .map_err(|error| RepositoryError::RequestSnapshot(error.message))?
            .into_iter()
            .filter(|candidate| candidate.status == pb::CandidateStatus::Rejected as i32)
            .map(|candidate| candidate.candidate_id)
            .collect::<std::collections::HashSet<_>>();
        self.storage
            .read(|connection| {
                let mut statement = connection.prepare(
                    "SELECT candidate_id, thread_id, snapshot_digest, fingerprint
                       FROM manager_extraction_candidate_identity",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(ExtractionCandidateIdentity {
                        candidate_id: row.get(0)?,
                        thread_id: row.get(1)?,
                        snapshot_digest: row.get(2)?,
                        fingerprint: row.get(3)?,
                    })
                })?;
                let mut identities = Vec::new();
                for row in rows {
                    let identity = row?;
                    if rejected_ids.contains(&identity.candidate_id) {
                        identities.push(identity);
                    }
                }
                Ok(identities)
            })
            .map_err(RepositoryError::from)
    }

    /// Persist the intentionally skipped apply outcome together with its
    /// idempotency identity.  This is deliberately separate from an
    /// ExtractionRun: a disabled policy must not create run data, Candidates,
    /// or Evidence merely to make a retry safe.
    #[allow(dead_code)]
    pub(crate) fn record_skipped_extraction_apply(
        &mut self,
        idempotency_key: &str,
        request_digest: &str,
        outcome: i32,
        skip_reason: i32,
    ) -> Result<ExtractionApplyReplayDecision, RepositoryError> {
        self.put_changed_entities_with_extraction_apply_replay(ExtractionApplyPersistence {
            items: Vec::new(),
            extraction_identities: &[],
            run_id_for_suppressions: "",
            suppressions: &[],
            idempotency_key,
            request_digest,
            outcome,
            run_id: None,
            skip_reason: Some(skip_reason),
        })
    }

    /// Attach an already-admitted create-Issue operation to the extracted
    /// Candidate that it must finalize.  Callers must invoke this after
    /// durable admission and before an outbox worker may deliver the entry.
    #[allow(dead_code)]
    pub(crate) fn register_extraction_auto_registration(
        &mut self,
        operation_id: &str,
        run_id: &str,
        candidate_id: &str,
        issue_id: &str,
        expected_idempotency_key: &str,
    ) -> Result<(), RepositoryError> {
        if operation_id.trim().is_empty()
            || run_id.trim().is_empty()
            || candidate_id.trim().is_empty()
            || issue_id.trim().is_empty()
            || expected_idempotency_key.trim().is_empty()
        {
            return Err(RepositoryError::RequestSnapshot(
                "extraction auto-registration identity is required".into(),
            ));
        }
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                let (method, state): (String, i32) = transaction.query_row(
                    "SELECT method, state FROM manager_operation WHERE operation_id=?1",
                    params![operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                if method != "manager_create_issue"
                    || !matches!(
                        pb::DurableOperationState::try_from(state),
                        Ok(pb::DurableOperationState::Received
                            | pb::DurableOperationState::Pending)
                    )
                {
                    return Err(RepositoryError::RequestSnapshot(
                        "auto-registration requires a pending create-Issue operation".into(),
                    ));
                }
                ensure_extraction_auto_registration_eligible_in_transaction(
                    transaction,
                    run_id,
                    candidate_id,
                    "auto-registration candidate is not eligible",
                    "auto-registration run is not running",
                )?;
                let existing =
                    extraction_auto_registration_in_transaction(transaction, operation_id)?;
                if let Some(existing) = existing {
                    if existing.run_id == run_id
                        && existing.candidate_id == candidate_id
                        && existing.issue_id == issue_id
                        && existing.expected_idempotency_key == expected_idempotency_key
                    {
                        return Ok(());
                    }
                    return Err(RepositoryError::OperationConflict);
                }
                let now = now_millis();
                transaction.execute(
                    "INSERT INTO manager_extraction_auto_registration
                    (operation_id, run_id, candidate_id, issue_id, expected_idempotency_key,
                     state, failure_reason,
                     created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?7)",
                    params![
                        operation_id,
                        run_id,
                        candidate_id,
                        issue_id,
                        expected_idempotency_key,
                        EXTRACTION_AUTO_REGISTRATION_PENDING,
                        now,
                    ],
                )?;
                Ok(())
            })?;
        Ok(())
    }

    /// Reserve every auto-registration selected for one run before any of
    /// their backend operations are admitted. This keeps a successful first
    /// delivery from completing the run while later candidates are still
    /// awaiting admission.
    pub(crate) fn reserve_extraction_auto_registrations(
        &mut self,
        run_id: &str,
        registrations: &[(String, String)],
    ) -> Result<Vec<ExtractionAutoRegistrationKey>, RepositoryError> {
        if run_id.trim().is_empty() || registrations.is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "auto-registration run ID and registrations are required".into(),
            ));
        }
        let (resolved, _) =
            self.storage
                .write_with::<_, RepositoryError, _>(|transaction, _| {
                    ensure_extraction_auto_registration_run_is_running_in_transaction(
                        transaction,
                        run_id,
                        "auto-registration run is not running",
                    )?;
                    let mut candidate_ids = BTreeSet::new();
                    let mut issue_ids = BTreeSet::new();
                    let mut resolved = Vec::with_capacity(registrations.len());
                    for (candidate_id, issue_id) in registrations {
                        if candidate_id.trim().is_empty()
                            || issue_id.trim().is_empty()
                            || !candidate_ids.insert(candidate_id.as_str())
                            || !issue_ids.insert(issue_id.as_str())
                        {
                            return Err(RepositoryError::RequestSnapshot(
                                "auto-registration reservation identities are invalid".into(),
                            ));
                        }
                        let existing_link: Option<(String, String, String)> = transaction
                            .query_row(
                                "SELECT run_id, issue_id, expected_idempotency_key
                               FROM manager_extraction_auto_registration
                              WHERE candidate_id=?1",
                                params![candidate_id],
                                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                            )
                            .optional()?;
                        if let Some((existing_run, existing_issue, existing_key)) = existing_link {
                            if existing_run == run_id && existing_issue == *issue_id {
                                resolved.push(ExtractionAutoRegistrationKey {
                                    candidate_id: candidate_id.clone(),
                                    issue_id: issue_id.clone(),
                                    idempotency_key: existing_key,
                                });
                                continue;
                            }
                            return Err(RepositoryError::OperationConflict);
                        }
                        ensure_extraction_auto_registration_candidate_is_eligible_in_transaction(
                            transaction,
                            run_id,
                            candidate_id,
                            "auto-registration candidate is not eligible",
                        )?;
                    }
                    for (candidate_id, issue_id) in registrations {
                        let existing_link: Option<i64> = transaction
                            .query_row(
                                "SELECT 1 FROM manager_extraction_auto_registration
                              WHERE candidate_id=?1 AND run_id=?2 AND issue_id=?3",
                                params![candidate_id, run_id, issue_id],
                                |row| row.get(0),
                            )
                            .optional()?;
                        if existing_link.is_some() {
                            continue;
                        }
                        let existing: Option<(String, String, String)> = transaction
                            .query_row(
                                "SELECT run_id, candidate_id, expected_idempotency_key
                               FROM manager_extraction_auto_registration_reservation
                              WHERE issue_id=?1",
                                params![issue_id],
                                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                            )
                            .optional()?;
                        let idempotency_key = if let Some((
                            existing_run,
                            existing_candidate,
                            existing_key,
                        )) = existing
                        {
                            if existing_run == run_id && existing_candidate == *candidate_id {
                                existing_key
                            } else {
                                return Err(RepositoryError::OperationConflict);
                            }
                        } else {
                            let key = format!("extract-auto-registration-{}", Uuid::new_v4());
                            transaction.execute(
                                "INSERT INTO manager_extraction_auto_registration_reservation
                                (issue_id, run_id, candidate_id, expected_idempotency_key,
                                 created_at, updated_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                                params![issue_id, run_id, candidate_id, key, now_millis()],
                            )?;
                            key
                        };
                        resolved.push(ExtractionAutoRegistrationKey {
                            candidate_id: candidate_id.clone(),
                            issue_id: issue_id.clone(),
                            idempotency_key,
                        });
                    }
                    Ok(resolved)
                })?;
        Ok(resolved)
    }

    /// Terminally records an admission failure that occurred before an
    /// auto-registration operation could be linked to the run.
    pub(crate) fn fail_extraction_auto_registration_run(
        &mut self,
        run_id: &str,
        reason: &str,
    ) -> Result<(), RepositoryError> {
        if run_id.trim().is_empty() || reason.trim().is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "auto-registration run ID and failure reason are required".into(),
            ));
        }
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let run = load_entity_in_transaction::<pb::ExtractionRun>(
                    transaction,
                    EntityKind::ExtractionRun,
                    run_id,
                    "issue_tracker.manager.v1.ExtractionRun",
                )?;
                // Reservations without a linked operation can never be
                // admitted after a failed run. Remove them atomically so they
                // cannot keep already-admitted sibling operations on hold.
                transaction.execute(
                    "DELETE FROM manager_extraction_auto_registration_reservation WHERE run_id=?1",
                    params![run_id],
                )?;
                if run.state == pb::ExtractionRunState::Failed as i32 {
                    return Ok(());
                }
                if run.state != pb::ExtractionRunState::Running as i32 {
                    return Err(RepositoryError::RequestSnapshot(
                        "auto-registration run is not running".into(),
                    ));
                }
                let failed = ExtractionRunStore::from_runs(vec![run.clone()])
                    .advance(
                        run_id,
                        run.revision,
                        &pb::ExtractionRunAdvance {
                            target_state: pb::ExtractionRunState::Failed as i32,
                            error: Some(reason.into()),
                            ..Default::default()
                        },
                        utc_instant(now_millis()),
                    )
                    .map_err(|error| {
                        RepositoryError::RequestSnapshot(format!(
                            "auto-registration run failure failed: {error:?}"
                        ))
                    })?;
                let input = entity_input(
                    EntityKind::ExtractionRun,
                    &failed.run_id,
                    failed.revision,
                    "issue_tracker.manager.v1.ExtractionRun",
                    failed.encode_to_vec(),
                )?;
                write_entity_in_transaction(transaction, &input, Some(run.revision), stamp)
            })?;
        Ok(())
    }

    /// Atomically persist one extraction apply's entities and durable replay
    /// identity.  The replay row is committed with the entities so a crash can
    /// never acknowledge a run that was not made durable (or vice versa).
    pub(crate) fn put_changed_entities_with_extraction_apply_replay(
        &mut self,
        request: ExtractionApplyPersistence<'_>,
    ) -> Result<ExtractionApplyReplayDecision, RepositoryError> {
        let ExtractionApplyPersistence {
            items,
            extraction_identities,
            run_id_for_suppressions,
            suppressions,
            idempotency_key,
            request_digest,
            outcome,
            run_id,
            skip_reason,
        } = request;
        if idempotency_key.is_empty() || request_digest.is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "extraction apply idempotency key and request digest are required".into(),
            ));
        }
        let mut pending = Vec::new();
        for input in items {
            if self
                .get(input.kind, &input.entity_id)?
                .is_some_and(|stored| stored.revision == input.revision)
            {
                continue;
            }
            pending.push(input);
        }
        let suppression_identity_indices = suppression_identity_index(extraction_identities);
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let existing_key_digest =
                    extraction_apply_key_digest(transaction, idempotency_key)?;
                let decision =
                    lookup_extraction_apply_replay(transaction, idempotency_key, request_digest)?;
                if decision == ExtractionApplyReplayDecision::New {
                    for input in &pending {
                        write_entity_in_transaction(transaction, input, None, stamp)?;
                    }
                    for identity in extraction_identities {
                        transaction.execute(
                            "INSERT INTO manager_extraction_candidate_identity
                              (candidate_id, thread_id, snapshot_digest, fingerprint, created_at)
                             VALUES (?1, ?2, ?3, ?4, ?5)
                             ON CONFLICT(candidate_id) DO NOTHING",
                            params![
                                identity.candidate_id,
                                identity.thread_id,
                                identity.snapshot_digest,
                                identity.fingerprint,
                                now_millis(),
                            ],
                        )?;
                    }
                    for suppression in suppressions {
                        let input_index = i64::from(suppression.input_index);
                        let group = suppression_audit_identity(
                            extraction_identities,
                            &suppression_identity_indices,
                            suppression,
                        )?;
                        transaction.execute(
                            "INSERT INTO manager_extraction_suppression_audit
                              (run_id, input_index, reason, thread_id, snapshot_digest, fingerprint)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![
                                run_id_for_suppressions,
                                input_index,
                                pb::ExtractionSuppressionReason::RejectedExactMatch as i32,
                                group.thread_id,
                                group.snapshot_digest,
                                group.fingerprint,
                            ],
                        )?;
                        for candidate_id in &suppression.rejected_candidate_ids {
                            transaction.execute(
                                "INSERT INTO manager_extraction_suppression_rejected_candidate
                                  (run_id, input_index, reason, candidate_id)
                                 VALUES (?1, ?2, ?3, ?4)",
                                params![
                                    run_id_for_suppressions,
                                    input_index,
                                    pb::ExtractionSuppressionReason::RejectedExactMatch as i32,
                                    candidate_id,
                                ],
                            )?;
                        }
                    }
                    let now = now_millis();
                    transaction.execute(
                        "INSERT INTO manager_extraction_apply_outcome
                         (request_digest, outcome, run_id, skip_reason, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                        params![request_digest, outcome, run_id, skip_reason, now],
                    )?;
                    transaction.execute(
                        "INSERT INTO manager_extraction_apply_key
                         (idempotency_key, request_digest, created_at)
                     VALUES (?1, ?2, ?3)",
                        params![idempotency_key, request_digest, now],
                    )?;
                } else if matches!(decision, ExtractionApplyReplayDecision::Replay { .. })
                    && existing_key_digest.is_none()
                {
                    transaction.execute(
                        "INSERT INTO manager_extraction_apply_key
                         (idempotency_key, request_digest, created_at)
                     VALUES (?1, ?2, ?3)",
                        params![idempotency_key, request_digest, now_millis()],
                    )?;
                }
                Ok(decision)
            })
            .map(|(decision, _)| decision)
    }
}
