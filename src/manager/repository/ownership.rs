use super::durable_helpers::*;
use super::*;

impl<'a> ManagerRepository<'a> {
    /// Read the durable recovery index for one Issue. Recovery state is
    /// table-owned; the legacy metadata key is intentionally ignored so a
    /// stale JSON value can never reset a persisted backoff or readmission.
    pub(crate) fn ownership_recovery_state(
        &self,
        issue_id: &str,
    ) -> Result<Option<OwnershipRecoveryState>, RepositoryError> {
        self.storage
            .read(|connection| {
                let (contract, root): (String, String) = connection.query_row(
                    "SELECT
                        (SELECT value FROM manager_metadata WHERE key='contract_fingerprint'),
                        (SELECT value FROM manager_metadata WHERE key='data_root_id')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                connection
                    .query_row(
                        "SELECT issue_id, readmission_number, current_operation_id,
                                expected_revision, in_flight, consecutive_failures,
                                stopped, next_attempt_at, stop_classification,
                                stop_basis, repair_condition, cursor_diagnostic
                           FROM manager_ownership_recovery
                          WHERE contract_fingerprint=?1 AND data_root_id=?2 AND issue_id=?3",
                        params![contract, root, issue_id],
                        |row| {
                            let readmission_number: i64 = row.get(1)?;
                            let expected_revision: i64 = row.get(3)?;
                            let consecutive_failures: i64 = row.get(5)?;
                            Ok(OwnershipRecoveryState {
                                issue_id: row.get(0)?,
                                expected_revision: u64::try_from(expected_revision).map_err(
                                    |_| {
                                        rusqlite::Error::IntegralValueOutOfRange(
                                            3,
                                            expected_revision,
                                        )
                                    },
                                )?,
                                readmission_number: u64::try_from(readmission_number).map_err(
                                    |_| {
                                        rusqlite::Error::IntegralValueOutOfRange(
                                            1,
                                            readmission_number,
                                        )
                                    },
                                )?,
                                current_operation_id: row.get(2)?,
                                in_flight: row.get::<_, i64>(4)? != 0,
                                consecutive_failures: u32::try_from(consecutive_failures).map_err(
                                    |_| {
                                        rusqlite::Error::IntegralValueOutOfRange(
                                            5,
                                            consecutive_failures,
                                        )
                                    },
                                )?,
                                next_attempt_at_millis: row.get(7)?,
                                last_failure_class: row.get(8)?,
                                stopped: row.get::<_, i64>(6)? != 0,
                                stop_basis: row.get(9)?,
                                repair_condition: row.get(10)?,
                                cursor_diagnostic: row.get(11)?,
                            })
                        },
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    /// Atomically replace one Issue's recovery index and advance the manager
    /// fencing counters. The complete state is supplied, but the update is
    /// monotonic: a stale readmission or a late operation ID cannot overwrite
    /// a newer admission. This is the storage-level CAS used by both the
    /// resident server and static runner.
    pub(crate) fn set_ownership_recovery_state(
        &mut self,
        state: &OwnershipRecoveryState,
    ) -> Result<ManagerCommit, RepositoryError> {
        if state.issue_id.trim().is_empty() {
            return Err(RepositoryError::EmptyEntityId);
        }
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                upsert_ownership_recovery_state_in_transaction(transaction, state)?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Admit a maintenance RECOVER request and its per-Issue recovery index
    /// in one SQLite transaction.  The scanner may therefore stop after this
    /// boundary without leaving a readmission number that has no operation,
    /// or an operation that has no corresponding in-flight index.
    pub(crate) fn commit_ownership_recovery_admission(
        &mut self,
        state: &OwnershipRecoveryState,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        request: &[u8],
        expectation: &super::super::backend_adapter::MarkerExpectation,
    ) -> Result<ManagerCommit, RepositoryError> {
        if state.issue_id.trim().is_empty()
            || operation.method != "manager_apply_issue_admin_action"
            || operation.downstream_operation_id.trim().is_empty()
        {
            return Err(RepositoryError::OperationConflict);
        }
        let durable_request = super::super::backend_dispatch::durable_request_from_manager(
            "manager_apply_issue_admin_action",
            request,
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        super::super::backend_dispatch::validate_durable_request_snapshot(
            "manager_apply_issue_admin_action",
            &durable_request,
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        let snapshot = sanitize_mutation(mutation).encode_to_vec();
        let outbox = outbox::new_entry(
            format!("outbox-{}", operation.operation_id),
            operation,
            "taskchampion",
            operation.operation_id.clone(),
            digest_bytes(&durable_request),
            now_millis(),
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        let expectation =
            super::super::backend_dispatch::expectation_snapshot(expectation).encode_to_vec();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                upsert_ownership_recovery_state_in_transaction(transaction, state)?;
                insert_operation(
                    transaction,
                    operation,
                    &snapshot,
                    &digest_bytes(&snapshot),
                    stamp,
                )?;
                transaction.execute(
                    "UPDATE manager_operation SET request_snapshot=?2,
                     request_snapshot_schema=?3, request_snapshot_digest=?4,
                     marker_expectation=?5, marker_expectation_schema=?6
                     WHERE operation_id=?1",
                    params![
                        operation.operation_id,
                        durable_request,
                        "issue_tracker.manager.v1.DurableBackendRequest",
                        digest_bytes(&durable_request),
                        expectation,
                        "issue_tracker.v1.OperationRecord",
                    ],
                )?;
                insert_backend_request_index(
                    transaction,
                    operation,
                    "manager_apply_issue_admin_action",
                    "issue_tracker.manager.v1.DurableBackendRequest",
                    &durable_request,
                )?;
                insert_admin_expectation_index(
                    transaction,
                    operation,
                    &durable_request,
                    &expectation,
                )?;
                insert_idempotency(transaction, operation)?;
                insert_outbox(transaction, &outbox, stamp)?;
                insert_admin_dispatch_attempt(transaction, operation, &outbox)?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Apply terminal ownership-recovery projections that may have been
    /// written just before a process stopped. The projection is intentionally
    /// replayable: only a row matching the current readmission number and
    /// operation ID can clear the in-flight admission, and a source
    /// generation newer than the stored projection wins monotonically. Missing
    /// recovery-index rows and conditional updates matching zero rows are
    /// invariant violations that roll back the transaction.
    /// An operation row whose generation is `None` intentionally proceeds because
    /// no newer generation is available for comparison.
    pub(crate) fn recover_ownership_recovery_projections(
        &mut self,
    ) -> Result<u32, RepositoryError> {
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                let (contract, root) = manager_root_identity(transaction)?;
                let mut statement = transaction.prepare(
                    "SELECT issue_id, readmission_number, operation_id,
                            source_generation, terminal_state
                       FROM manager_ownership_recovery_projection
                      WHERE contract_fingerprint=?1 AND data_root_id=?2
                      ORDER BY issue_id, readmission_number, operation_id",
                )?;
                let rows = statement
                    .query_map(params![contract.as_str(), root.as_str()], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i32>(4)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                drop(statement);
                let mut applied = 0_u32;
                for (issue_id, readmission_number, operation_id, source_generation, state) in rows
                {
                    let current = transaction
                        .query_row(
                            "SELECT current_operation_id, readmission_number, in_flight
                               FROM manager_ownership_recovery
                              WHERE contract_fingerprint=?1 AND data_root_id=?2
                                AND issue_id=?3",
                            params![contract.as_str(), root.as_str(), issue_id.as_str()],
                            |row| {
                                Ok((
                                    row.get::<_, Option<String>>(0)?,
                                    row.get::<_, i64>(1)?,
                                    row.get::<_, i64>(2)?,
                                ))
                            },
                        )
                        .optional()?;
                    let Some((current_operation, current_readmission, in_flight)) = current else {
                        return Err(RepositoryError::OwnershipProjectionNotApplied {
                            issue_id,
                            operation_id,
                            reason: "recovery index row is missing".into(),
                        });
                    };
                    if current_readmission != readmission_number
                        || current_operation.as_deref() != Some(operation_id.as_str())
                    {
                        // A late projection from an older admission is kept
                        // for audit but must not touch a newer current index.
                        continue;
                    }
                    let operation_generation = transaction
                        .query_row(
                            "SELECT generation FROM manager_operation WHERE operation_id=?1",
                            params![operation_id.as_str()],
                            |row| row.get::<_, i64>(0),
                        )
                        .optional()?;
                    // A missing operation generation intentionally proceeds; handling that provenance is out of scope here.
                    if operation_generation.is_none_or(|generation| generation > source_generation) {
                        // The operation advanced after this projection was
                        // produced. Keep the index in-flight until a
                        // projection for that newer generation arrives.
                        continue;
                    }
                    let terminal = matches!(
                        pb::DurableOperationState::try_from(state),
                        Ok(pb::DurableOperationState::Committed)
                            | Ok(pb::DurableOperationState::Arbitrated)
                    );
                    if !terminal || in_flight == 0 {
                        continue;
                    }
                    let changed = transaction.execute(
                        "UPDATE manager_ownership_recovery
                            SET in_flight=0, current_operation_id=NULL,
                                consecutive_failures=CASE WHEN ?4=?5 OR ?4=?6 THEN 0 ELSE consecutive_failures END,
                                next_attempt_at=NULL, stopped=0,
                                stop_classification=NULL, stop_basis=NULL,
                                repair_condition=NULL, updated_at=?7
                          WHERE contract_fingerprint=?1 AND data_root_id=?2
                            AND issue_id=?3 AND readmission_number=?8
                            AND current_operation_id=?9 AND in_flight=1",
                        params![
                            contract.as_str(),
                            root.as_str(),
                            issue_id.as_str(),
                            state,
                            pb::DurableOperationState::Committed as i32,
                            pb::DurableOperationState::Arbitrated as i32,
                            now_millis(),
                            readmission_number,
                            operation_id.as_str(),
                        ],
                    )?;
                    if changed == 0 {
                        return Err(RepositoryError::OwnershipProjectionNotApplied {
                            issue_id,
                            operation_id,
                            reason: "conditional update matched no rows".into(),
                        });
                    }
                    applied = applied.saturating_add(1);
                    let _ = source_generation;
                }
                Ok(applied)
            })
            .map(|(applied, _commit)| applied)
    }

    /// The cursor is deliberately Manager-owned: it survives runner restarts
    /// without becoming canonical Issue state.
    pub(crate) fn ownership_maintenance_cursor(&self) -> Result<Option<String>, RepositoryError> {
        self.storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT value FROM manager_metadata WHERE key=?1",
                        params![Self::OWNERSHIP_MAINTENANCE_CURSOR_KEY],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    /// Record one safe ownership-integrity observation without touching the
    /// canonical Issue or creating a synthetic manager operation. The target
    /// identity and evidence digest are stable across periodic scans; an
    /// already-open diagnosis is refreshed, while a resolved diagnosis is
    /// reopened under a new generation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_ownership_integrity_diagnosis(
        &mut self,
        target_kind: &str,
        target_id: &str,
        diagnosis_class: &str,
        observed_revision: Option<u64>,
        lease_classification: Option<&str>,
        repair_condition: &str,
        invalid_attributes: &[String],
        lease_expires_at: Option<u64>,
        open_blocker_issue_ids: &[String],
    ) -> Result<ManagerCommit, RepositoryError> {
        self.record_ownership_integrity_diagnosis_with_state(
            target_kind,
            target_id,
            diagnosis_class,
            observed_revision,
            lease_classification,
            repair_condition,
            invalid_attributes,
            lease_expires_at,
            open_blocker_issue_ids,
            None,
        )
    }

    /// Record an integrity diagnosis and, when supplied, its recovery stop
    /// projection in the same SQLite transaction.  The combined boundary is
    /// used by resident maintenance so a diagnosis write failure cannot leave
    /// an unresolvable stopped Issue behind.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_ownership_integrity_diagnosis_and_recovery_state(
        &mut self,
        state: &OwnershipRecoveryState,
        target_kind: &str,
        target_id: &str,
        diagnosis_class: &str,
        observed_revision: Option<u64>,
        lease_classification: Option<&str>,
        repair_condition: &str,
        invalid_attributes: &[String],
        lease_expires_at: Option<u64>,
        open_blocker_issue_ids: &[String],
    ) -> Result<ManagerCommit, RepositoryError> {
        self.record_ownership_integrity_diagnosis_with_state(
            target_kind,
            target_id,
            diagnosis_class,
            observed_revision,
            lease_classification,
            repair_condition,
            invalid_attributes,
            lease_expires_at,
            open_blocker_issue_ids,
            Some(state),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record_ownership_integrity_diagnosis_with_state(
        &mut self,
        target_kind: &str,
        target_id: &str,
        diagnosis_class: &str,
        observed_revision: Option<u64>,
        lease_classification: Option<&str>,
        repair_condition: &str,
        invalid_attributes: &[String],
        lease_expires_at: Option<u64>,
        open_blocker_issue_ids: &[String],
        recovery_state: Option<&OwnershipRecoveryState>,
    ) -> Result<ManagerCommit, RepositoryError> {
        if target_kind.trim().is_empty()
            || target_id.trim().is_empty()
            || diagnosis_class.trim().is_empty()
            || repair_condition.trim().is_empty()
        {
            return Err(RepositoryError::RequestSnapshot(
                "ownership diagnosis identity is incomplete".into(),
            ));
        }
        let mut invalid_attributes = invalid_attributes.to_vec();
        invalid_attributes.sort_unstable();
        invalid_attributes.dedup();
        let invalid_attributes_json = serde_json::to_string(&invalid_attributes)
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        let mut open_blocker_issue_ids = open_blocker_issue_ids.to_vec();
        open_blocker_issue_ids.sort_unstable();
        open_blocker_issue_ids.dedup();
        let open_blocker_issue_ids_json = serde_json::to_string(&open_blocker_issue_ids)
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        let evidence_digest = digest_bytes(
            format!(
                "ownership-integrity\0{target_kind}\0{target_id}\0{diagnosis_class}\0{repair_condition}\0{}\0{}",
                invalid_attributes.join(","),
                open_blocker_issue_ids.join(","),
            )
            .as_bytes(),
        );
        let target_kind = target_kind.to_owned();
        let target_id = target_id.to_owned();
        let diagnosis_class = diagnosis_class.to_owned();
        let lease_classification = lease_classification.map(ToOwned::to_owned);
        let repair_condition = repair_condition.to_owned();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                let (contract, root) = manager_root_identity(transaction)?;
                let now = now_millis();
                let epoch: i64 = transaction
                    .query_row(
                        "SELECT diagnosis_epoch FROM manager_diagnosis_epoch
                           WHERE contract_fingerprint=?1 AND data_root_id=?2
                             AND target_kind=?3 AND target_id=?4",
                        params![&contract, &root, &target_kind, &target_id],
                        |row| row.get(0),
                    )
                    .optional()?
                    .unwrap_or(0_i64)
                    .checked_add(1)
                    .ok_or(RepositoryError::RevisionOverflow)?;
                transaction.execute(
                    "INSERT INTO manager_diagnosis_epoch
                        (contract_fingerprint, data_root_id, target_kind, target_id, diagnosis_epoch)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(contract_fingerprint, data_root_id, target_kind, target_id)
                     DO UPDATE SET diagnosis_epoch=excluded.diagnosis_epoch",
                    params![&contract, &root, &target_kind, &target_id, epoch],
                )?;
                let open_generation: Option<i64> = transaction
                    .query_row(
                        "SELECT MAX(diagnosis_generation)
                           FROM manager_ownership_integrity_diagnosis
                          WHERE contract_fingerprint=?1 AND data_root_id=?2
                            AND target_kind=?3 AND target_id=?4
                            AND diagnosis_class=?5 AND evidence_digest=?6
                            AND state NOT IN ('RESOLVED', 'STALE', 'SUPERSEDED')",
                        params![
                            &contract,
                            &root,
                            &target_kind,
                            &target_id,
                            &diagnosis_class,
                            &evidence_digest,
                        ],
                        |row| row.get(0),
                    )
                    .optional()?
                    .flatten();
                if let Some(generation) = open_generation {
                    transaction.execute(
                        "UPDATE manager_ownership_integrity_diagnosis
                            SET observation_token=?7, repair_epoch=?8,
                                observed_revision=?9, lease_classification=?10,
                                repair_condition=?11, invalid_attributes=?12,
                                lease_expires_at=?13, open_blocker_issue_ids=?14,
                                last_confirmed_at=?15
                          WHERE contract_fingerprint=?1 AND data_root_id=?2
                            AND target_kind=?3 AND target_id=?4
                            AND diagnosis_class=?5 AND evidence_digest=?6
                            AND diagnosis_generation=?16",
                        params![
                            &contract,
                            &root,
                            &target_kind,
                            &target_id,
                            &diagnosis_class,
                            &evidence_digest,
                            epoch,
                            epoch,
                            observed_revision.map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                            lease_classification,
                            &repair_condition,
                            &invalid_attributes_json,
                            lease_expires_at.map(|value| {
                                i64::try_from(value.saturating_mul(1_000)).unwrap_or(i64::MAX)
                            }),
                            &open_blocker_issue_ids_json,
                            now,
                            generation,
                        ],
                    )?;
                } else {
                    let generation: i64 = transaction
                        .query_row(
                            "SELECT COALESCE(MAX(diagnosis_generation), 0) + 1
                               FROM manager_ownership_integrity_diagnosis
                              WHERE contract_fingerprint=?1 AND data_root_id=?2
                                AND target_kind=?3 AND target_id=?4
                                AND diagnosis_class=?5 AND evidence_digest=?6",
                            params![
                                &contract,
                                &root,
                                &target_kind,
                                &target_id,
                                &diagnosis_class,
                                &evidence_digest,
                            ],
                            |row| row.get(0),
                        )?;
                    transaction.execute(
                        "INSERT INTO manager_ownership_integrity_diagnosis
                            (contract_fingerprint, data_root_id, target_kind, target_id,
                             diagnosis_class, evidence_digest, diagnosis_generation,
                             observation_token, repair_epoch, observed_revision,
                             lease_classification, repair_condition, state,
                             resolution_operation_id, first_detected_at, last_confirmed_at,
                             resolved_at, invalid_attributes, lease_expires_at,
                             open_blocker_issue_ids)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                                 ?12, 'OPEN', NULL, ?13, ?13, NULL, ?14, ?15, ?16)",
                        params![
                            &contract,
                            &root,
                            &target_kind,
                            &target_id,
                            &diagnosis_class,
                            &evidence_digest,
                            generation,
                            epoch,
                            epoch,
                            observed_revision.map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                            lease_classification,
                            &repair_condition,
                            now,
                            &invalid_attributes_json,
                            lease_expires_at.map(|value| {
                                i64::try_from(value.saturating_mul(1_000)).unwrap_or(i64::MAX)
                            }),
                            &open_blocker_issue_ids_json,
                        ],
                    )?;
                }
                if let Some(state) = recovery_state {
                    upsert_ownership_recovery_state_in_transaction(transaction, state)?;
                }
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Atomically stale lease-inconsistency diagnoses and clear the matching
    /// ownership-recovery stop after the canonical Issue no longer has an
    /// inconsistent lease.  A diagnosis reference is optional on human
    /// administrative actions, so this observation-based cleanup is needed
    /// for actions that repair the Issue without carrying that reference.
    pub(crate) fn stale_ownership_integrity_diagnosis_and_recovery_state(
        &mut self,
        state: &OwnershipRecoveryState,
        target_kind: &str,
        target_id: &str,
        observed_revision: u64,
    ) -> Result<ManagerCommit, RepositoryError> {
        if state.issue_id.trim().is_empty()
            || target_kind.trim().is_empty()
            || target_id.trim().is_empty()
        {
            return Err(RepositoryError::RequestSnapshot(
                "ownership repair identity is incomplete".into(),
            ));
        }
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                stale_ownership_integrity_diagnosis_in_transaction(
                    transaction,
                    target_kind,
                    target_id,
                    observed_revision,
                )?;
                upsert_ownership_recovery_state_in_transaction(transaction, state)?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Read safe integrity diagnoses for the trusted-consumer diagnostic
    /// endpoint. Raw TaskData, handles, and request payloads are excluded.
    pub(crate) fn list_ownership_integrity_diagnoses(
        &self,
        target_kind: Option<&str>,
        target_id: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<pb::OwnershipIntegrityDiagnosis>, RepositoryError> {
        let limit = i64::try_from(limit.min(100)).unwrap_or(100);
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        self.storage
            .read(|connection| {
                let mut statement = connection.prepare(
                    "SELECT target_kind, target_id, diagnosis_class, evidence_digest,
                            diagnosis_generation, observation_token, repair_epoch,
                            observed_revision, lease_classification, repair_condition,
                            state, resolution_operation_id, first_detected_at,
                            last_confirmed_at, resolved_at, invalid_attributes,
                            lease_expires_at, open_blocker_issue_ids
                       FROM manager_ownership_integrity_diagnosis
                      WHERE (?1 IS NULL OR target_kind=?1)
                        AND (?2 IS NULL OR target_id=?2)
                      ORDER BY target_id, diagnosis_class, evidence_digest,
                               diagnosis_generation DESC
                      LIMIT ?3 OFFSET ?4",
                )?;
                let rows =
                    statement.query_map(params![target_kind, target_id, limit, offset], |row| {
                        let instant = |value: i64| pb::UtcInstant {
                            unix_epoch_millis: value,
                        };
                        Ok(pb::OwnershipIntegrityDiagnosis {
                            target_kind: row.get(0)?,
                            target_id: row.get(1)?,
                            diagnosis_class: row.get(2)?,
                            evidence_digest: row.get(3)?,
                            diagnosis_generation: row.get::<_, i64>(4)?.try_into().unwrap_or(0),
                            observation_token: row.get::<_, i64>(5)?.try_into().unwrap_or(0),
                            repair_epoch: row.get::<_, i64>(6)?.try_into().unwrap_or(0),
                            observed_revision: row
                                .get::<_, Option<i64>>(7)?
                                .and_then(|value| value.try_into().ok()),
                            lease_classification: row.get(8)?,
                            repair_condition: row.get(9)?,
                            state: row.get(10)?,
                            resolution_operation_id: row.get(11)?,
                            first_detected_at: Some(instant(row.get(12)?)),
                            last_confirmed_at: Some(instant(row.get(13)?)),
                            resolved_at: row.get::<_, Option<i64>>(14)?.map(instant),
                            invalid_attributes: serde_json::from_str(
                                row.get::<_, String>(15)?.as_str(),
                            )
                            .unwrap_or_default(),
                            lease_expires_at: row.get::<_, Option<i64>>(16)?.map(instant),
                            open_blocker_issue_ids: serde_json::from_str(
                                row.get::<_, String>(17)?.as_str(),
                            )
                            .unwrap_or_default(),
                        })
                    })?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    /// Resolve exactly the diagnosis observed by an administrative repair.
    /// The observation token, repair epoch, generation, and evidence digest
    /// form one CAS boundary.  A later scan or a different repair therefore
    /// cannot be cleared by an old action receipt.  This write intentionally
    /// does not touch the canonical Issue or any other diagnosis row.
    pub(crate) fn resolve_ownership_integrity_diagnosis(
        &mut self,
        reference: &pb::OwnershipIntegrityDiagnosisReference,
        resolution_operation_id: &str,
    ) -> Result<super::OwnershipDiagnosisResolution, RepositoryError> {
        if reference.target_kind.trim().is_empty()
            || reference.target_id.trim().is_empty()
            || reference.diagnosis_class.trim().is_empty()
            || reference.evidence_digest.trim().is_empty()
            || resolution_operation_id.trim().is_empty()
        {
            return Err(RepositoryError::RequestSnapshot(
                "ownership diagnosis reference is incomplete".into(),
            ));
        }
        let reference = reference.clone();
        let resolution_operation_id = resolution_operation_id.to_owned();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                let (contract, root) = manager_root_identity(transaction)?;
                let now = now_millis();
                let changed = transaction.execute(
                    "UPDATE manager_ownership_integrity_diagnosis
                        SET state='RESOLVED', resolution_operation_id=?8,
                            resolved_at=?9, last_confirmed_at=?9
                      WHERE contract_fingerprint=?1 AND data_root_id=?2
                        AND target_kind=?3 AND target_id=?4
                        AND diagnosis_class=?5 AND evidence_digest=?6
                        AND diagnosis_generation=?7
                        AND observation_token=?10 AND repair_epoch=?11
                        AND state NOT IN ('RESOLVED', 'STALE', 'SUPERSEDED')",
                    params![
                        contract,
                        root,
                        reference.target_kind,
                        reference.target_id,
                        reference.diagnosis_class,
                        reference.evidence_digest,
                        i64::try_from(reference.diagnosis_generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        resolution_operation_id,
                        now,
                        i64::try_from(reference.observation_token)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(reference.repair_epoch)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                    ],
                )?;
                if changed == 1 {
                    return Ok(super::OwnershipDiagnosisResolution::Resolved);
                }
                let exists: bool = transaction.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM manager_ownership_integrity_diagnosis
                         WHERE contract_fingerprint=?1 AND data_root_id=?2
                           AND target_kind=?3 AND target_id=?4
                           AND diagnosis_class=?5 AND evidence_digest=?6
                           AND diagnosis_generation=?7
                    )",
                    params![
                        contract,
                        root,
                        reference.target_kind,
                        reference.target_id,
                        reference.diagnosis_class,
                        reference.evidence_digest,
                        i64::try_from(reference.diagnosis_generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                    ],
                    |row| row.get(0),
                )?;
                Ok(if exists {
                    super::OwnershipDiagnosisResolution::Stale
                } else {
                    super::OwnershipDiagnosisResolution::Missing
                })
            })
            .map(|(resolution, _commit)| resolution)
    }

    /// Check a diagnosis reference without changing it.  This is used during
    /// administrative admission so a stale repair request cannot reach the
    /// Tracker and then fail only after mutating canonical state.
    pub(crate) fn ownership_integrity_diagnosis_matches(
        &self,
        reference: &pb::OwnershipIntegrityDiagnosisReference,
    ) -> Result<bool, RepositoryError> {
        self.storage
            .read(|connection| {
                let (contract, root): (String, String) = connection.query_row(
                    "SELECT
                        (SELECT value FROM manager_metadata WHERE key='contract_fingerprint'),
                        (SELECT value FROM manager_metadata WHERE key='data_root_id')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let matches: bool = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM manager_ownership_integrity_diagnosis
                         WHERE contract_fingerprint=?1 AND data_root_id=?2
                           AND target_kind=?3 AND target_id=?4
                           AND diagnosis_class=?5 AND evidence_digest=?6
                           AND diagnosis_generation=?7 AND observation_token=?8
                           AND repair_epoch=?9 AND state='OPEN'
                    )",
                    params![
                        contract,
                        root,
                        reference.target_kind,
                        reference.target_id,
                        reference.diagnosis_class,
                        reference.evidence_digest,
                        i64::try_from(reference.diagnosis_generation).map_err(|_| {
                            rusqlite::Error::IntegralValueOutOfRange(
                                7,
                                reference.diagnosis_generation as i64,
                            )
                        })?,
                        i64::try_from(reference.observation_token).map_err(|_| {
                            rusqlite::Error::IntegralValueOutOfRange(
                                8,
                                reference.observation_token as i64,
                            )
                        })?,
                        i64::try_from(reference.repair_epoch).map_err(|_| {
                            rusqlite::Error::IntegralValueOutOfRange(
                                9,
                                reference.repair_epoch as i64,
                            )
                        })?,
                    ],
                    |row| row.get(0),
                )?;
                Ok(matches)
            })
            .map_err(RepositoryError::from)
    }

    pub(crate) fn set_ownership_maintenance_cursor(
        &mut self,
        issue_id: &str,
    ) -> Result<ManagerCommit, RepositoryError> {
        let issue_id = issue_id.to_owned();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                transaction.execute(
                    "INSERT INTO manager_metadata (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![Self::OWNERSHIP_MAINTENANCE_CURSOR_KEY, issue_id],
                )?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Return whether the resident ownership scanner has completed at least
    /// one full physical-UUID sweep since this Manager root was initialized.
    /// Missing metadata is deliberately false so a new or upgraded root
    /// cannot advertise readiness after only its first bounded page.
    pub(crate) fn ownership_maintenance_scan_complete(&self) -> Result<bool, RepositoryError> {
        self.storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT value FROM manager_metadata WHERE key=?1",
                        params![Self::OWNERSHIP_MAINTENANCE_SCAN_COMPLETE_KEY],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map(|value| value.as_deref() == Some("true"))
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    /// Persist completion of the initial ownership scan.  This is a monotonic
    /// readiness marker; subsequent cursor-rotated maintenance pages do not
    /// make an already initialized root transiently unhealthy.
    pub(crate) fn set_ownership_maintenance_scan_complete(
        &mut self,
        complete: bool,
    ) -> Result<ManagerCommit, RepositoryError> {
        let value = if complete { "true" } else { "false" };
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                transaction.execute(
                    "INSERT INTO manager_metadata (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![Self::OWNERSHIP_MAINTENANCE_SCAN_COMPLETE_KEY, value],
                )?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Read the persistent capability-recovery cursor.  A cursor is scoped to
    /// the Manager root through the metadata table and is intentionally based
    /// on the immutable operation ID rather than a mutable state or timestamp.
    pub(crate) fn capability_recovery_cursor(&self) -> Result<Option<String>, RepositoryError> {
        self.storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT value FROM manager_metadata WHERE key=?1",
                        params![Self::CAPABILITY_RECOVERY_CURSOR_KEY],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    pub(crate) fn set_capability_recovery_cursor(
        &mut self,
        operation_id: &str,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation_id.trim().is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "capability recovery cursor must not be empty".into(),
            ));
        }
        let operation_id = operation_id.to_owned();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                transaction.execute(
                    "INSERT INTO manager_metadata (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![Self::CAPABILITY_RECOVERY_CURSOR_KEY, operation_id],
                )?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Read the persistent cursor for acknowledged UNCONFIRMED operation
    /// reconciliation. The value is encoded as `created_at:operation_id` and
    /// is only used as a fairness boundary; operation state remains the
    /// authoritative eligibility check.
    pub(crate) fn unconfirmed_recovery_cursor(&self) -> Result<Option<String>, RepositoryError> {
        self.storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT value FROM manager_metadata WHERE key=?1",
                        params![Self::UNCONFIRMED_RECOVERY_CURSOR_KEY],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    pub(crate) fn set_unconfirmed_recovery_cursor(
        &mut self,
        created_at: i64,
        operation_id: &str,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation_id.trim().is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "unconfirmed recovery cursor operation ID must not be empty".into(),
            ));
        }
        let value = format!("{created_at}:{operation_id}");
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                transaction.execute(
                    "INSERT INTO manager_metadata (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![Self::UNCONFIRMED_RECOVERY_CURSOR_KEY, value],
                )?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Maintenance health is root-scoped operational state. Persist it apart
    /// from outbox delivery so a restart cannot make an unresolved raw-scan
    /// or cursor-checkpoint failure look healthy.
    pub(crate) fn ownership_maintenance_healthy(&self) -> Result<bool, RepositoryError> {
        self.storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT value FROM manager_metadata WHERE key=?1",
                        params![Self::OWNERSHIP_MAINTENANCE_HEALTH_KEY],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map(|value| value.as_deref().unwrap_or("true") == "true")
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    /// Return whether any durable ownership-recovery state still requires
    /// attention.  A stopped, in-flight, or backoff entry must keep the
    /// resident health signal degraded even when the current UUID page has
    /// no matching Issue (for example after a cursor rotation).
    pub(crate) fn ownership_maintenance_has_unresolved(&self) -> Result<bool, RepositoryError> {
        self.storage
            .read(|connection| {
                let (contract, root): (String, String) = connection.query_row(
                    "SELECT
                        (SELECT value FROM manager_metadata WHERE key='contract_fingerprint'),
                        (SELECT value FROM manager_metadata WHERE key='data_root_id')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let unresolved: i64 = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM manager_ownership_recovery
                         WHERE contract_fingerprint=?1 AND data_root_id=?2
                           AND (stopped <> 0 OR in_flight <> 0
                                OR next_attempt_at IS NOT NULL)
                    )",
                    params![contract, root],
                    |row| row.get(0),
                )?;
                Ok(unresolved != 0)
            })
            .map_err(RepositoryError::from)
    }

    /// The read-then-write is safe only because all callers are serialized under the manager mutex; do not call this outside that serialization.
    pub(crate) fn set_ownership_maintenance_health(
        &mut self,
        healthy: bool,
    ) -> Result<ManagerCommit, RepositoryError> {
        let persisted = self
            .storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT value FROM manager_metadata WHERE key=?1",
                        params![Self::OWNERSHIP_MAINTENANCE_HEALTH_KEY],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)?;
        if persisted.as_deref().map(|value| value == "true") == Some(healthy) {
            return Ok(ManagerCommit {
                manager_sequence: self.storage.manager_sequence(),
                generation: self.storage.generation(),
            });
        }
        let now = now_millis().to_string();
        let value = if healthy { "true" } else { "false" };
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                for (key, value) in [
                    (Self::OWNERSHIP_MAINTENANCE_HEALTH_KEY, value),
                    (
                        Self::OWNERSHIP_MAINTENANCE_HEALTH_UPDATED_AT_KEY,
                        now.as_str(),
                    ),
                ] {
                    transaction.execute(
                        "INSERT INTO manager_metadata (key, value) VALUES (?1, ?2)
                         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        params![key, value],
                    )?;
                }
                Ok(())
            })
            .map(|(_, commit)| commit)
    }
}
