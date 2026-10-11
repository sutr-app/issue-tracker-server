use super::durable_helpers::*;
use super::*;

impl<'a> ManagerRepository<'a> {
    /// Persist one resident-maintenance phase snapshot.  This projection is
    /// intentionally independent from the legacy boolean ownership flag so a
    /// successful delivery phase cannot erase a recovery or scan diagnosis.
    #[cfg(test)]
    pub(crate) fn record_phase_health(
        &mut self,
        phase: &str,
        health: i32,
        permit_enabled: bool,
        pending_gauge: u64,
        inconsistent_gauge: u64,
        diagnosis_refs: &[String],
    ) -> Result<ManagerCommit, RepositoryError> {
        self.record_phase_health_with_staleness(
            phase,
            health,
            permit_enabled,
            pending_gauge,
            inconsistent_gauge,
            diagnosis_refs,
            false,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_phase_health_with_staleness(
        &mut self,
        phase: &str,
        health: i32,
        permit_enabled: bool,
        pending_gauge: u64,
        inconsistent_gauge: u64,
        diagnosis_refs: &[String],
        gauge_stale: bool,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.record_phase_health_with_deltas(
            phase,
            health,
            permit_enabled,
            pending_gauge,
            inconsistent_gauge,
            diagnosis_refs,
            gauge_stale,
            0,
            0,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_phase_health_with_deltas(
        &mut self,
        phase: &str,
        health: i32,
        permit_enabled: bool,
        pending_gauge: u64,
        inconsistent_gauge: u64,
        diagnosis_refs: &[String],
        gauge_stale: bool,
        skipped_delta: u64,
        conflict_delta: u64,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.record_phase_health_with_counts(
            phase,
            health,
            permit_enabled,
            pending_gauge,
            inconsistent_gauge,
            diagnosis_refs,
            gauge_stale,
            1,
            u64::from(health == pb::HealthStatus::Healthy as i32),
            diagnosis_refs.len() as u64,
            skipped_delta,
            conflict_delta,
            0,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_phase_health_with_counts(
        &mut self,
        phase: &str,
        health: i32,
        permit_enabled: bool,
        pending_gauge: u64,
        inconsistent_gauge: u64,
        diagnosis_refs: &[String],
        gauge_stale: bool,
        scanned_delta: u64,
        recovered_delta: u64,
        diagnosis_delta: u64,
        skipped_delta: u64,
        conflict_delta: u64,
        unconfirmed_delta: u64,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.record_phase_health_update(PhaseHealthUpdate {
            phase: phase.to_owned(),
            health,
            permit_enabled,
            pending_gauge,
            inconsistent_gauge,
            diagnosis_refs: diagnosis_refs.to_vec(),
            gauge_stale,
            scanned_delta,
            recovered_delta,
            diagnosis_delta,
            skipped_delta,
            conflict_delta,
            unconfirmed_delta,
        })
    }

    /// Persist one phase projection using a typed update value.
    pub(crate) fn record_phase_health_update(
        &mut self,
        update: PhaseHealthUpdate,
    ) -> Result<ManagerCommit, RepositoryError> {
        let PhaseHealthUpdate {
            phase,
            health,
            permit_enabled,
            pending_gauge,
            inconsistent_gauge,
            diagnosis_refs,
            gauge_stale,
            scanned_delta,
            recovered_delta,
            diagnosis_delta,
            skipped_delta,
            conflict_delta,
            unconfirmed_delta,
        } = update;
        if phase.trim().is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "maintenance phase must not be empty".into(),
            ));
        }
        let refs = diagnosis_refs.join("\n");
        self.storage
            .write_untracked::<_, RepositoryError, _>(|transaction| {
                let (contract, root) = manager_root_identity(transaction)?;
                let now = now_millis();
                let previous: Option<(
                    i32,
                    Option<i64>,
                    Option<i64>,
                    Option<i64>,
                    Option<i64>,
                    i64,
                    i64,
                    i64,
                    i64,
                    i64,
                    i64,
                    i64,
                    i64,
                    i64,
                    i64,
                    Option<Vec<u8>>,
                )> = transaction
                    .query_row(
                        "SELECT health, last_started_at, last_finished_at,
                                last_failure_at, last_recovered_at,
                                attempt_count, success_count, skipped_count,
                                conflict_count, diagnosis_count, unconfirmed_count,
                                pending_gauge, inconsistent_gauge, permit_enabled,
                                gauge_stale, snapshot_payload
                           FROM manager_phase_health
                          WHERE contract_fingerprint=?1 AND data_root_id=?2 AND phase=?3",
                        params![&contract, &root, &phase],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                                row.get(6)?,
                                row.get(7)?,
                                row.get(8)?,
                                row.get(9)?,
                                row.get(10)?,
                                row.get(11)?,
                                row.get(12)?,
                                row.get(13)?,
                                row.get(14)?,
                                row.get(15)?,
                            ))
                        },
                    )
                    .optional()?;
                let previous_health = previous.as_ref().map(|value| value.0).unwrap_or(0);
                let attempt_count = previous
                    .as_ref()
                    .map(|value| value.5)
                    .unwrap_or(0)
                    .checked_add(i64::try_from(scanned_delta).unwrap_or(i64::MAX))
                    .ok_or(RepositoryError::RevisionOverflow)?;
                let success_count = previous
                    .as_ref()
                    .map(|value| value.6)
                    .unwrap_or(0)
                    .checked_add(i64::try_from(recovered_delta).unwrap_or(i64::MAX))
                    .ok_or(RepositoryError::RevisionOverflow)?;
                let diagnosis_count = previous
                    .as_ref()
                    .map(|value| value.9)
                    .unwrap_or(0)
                    .checked_add(i64::try_from(diagnosis_delta).unwrap_or(i64::MAX))
                    .ok_or(RepositoryError::RevisionOverflow)?;
                let skipped_count = previous
                    .as_ref()
                    .map(|value| value.7)
                    .unwrap_or(0)
                    .checked_add(i64::try_from(skipped_delta).unwrap_or(i64::MAX))
                    .ok_or(RepositoryError::RevisionOverflow)?;
                let conflict_count = previous
                    .as_ref()
                    .map(|value| value.8)
                    .unwrap_or(0)
                    .checked_add(i64::try_from(conflict_delta).unwrap_or(i64::MAX))
                    .ok_or(RepositoryError::RevisionOverflow)?;
                let unconfirmed_count = previous
                    .as_ref()
                    .map(|value| value.10)
                    .unwrap_or(0)
                    .checked_add(i64::try_from(unconfirmed_delta).unwrap_or(i64::MAX))
                    .ok_or(RepositoryError::RevisionOverflow)?;
                let last_failure_at = if health == pb::HealthStatus::Healthy as i32 {
                    previous.as_ref().and_then(|value| value.3)
                } else {
                    Some(now)
                };
                let last_recovered_at = if health == pb::HealthStatus::Healthy as i32
                    && previous_health != pb::HealthStatus::Healthy as i32
                {
                    Some(now)
                } else {
                    previous.as_ref().and_then(|value| value.4)
                };
                transaction.execute(
                    "INSERT INTO manager_phase_health
                        (contract_fingerprint, data_root_id, phase, health,
                         last_started_at, last_finished_at, last_failure_at,
                         last_recovered_at, attempt_count, success_count,
                         skipped_count, conflict_count, diagnosis_count,
                         unconfirmed_count, pending_gauge, inconsistent_gauge, permit_enabled,
                         gauge_stale, snapshot_payload, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?6)
                     ON CONFLICT(contract_fingerprint, data_root_id, phase)
                     DO UPDATE SET health=excluded.health,
                         last_started_at=excluded.last_started_at,
                         last_finished_at=excluded.last_finished_at,
                         last_failure_at=excluded.last_failure_at,
                         last_recovered_at=excluded.last_recovered_at,
                         attempt_count=excluded.attempt_count,
                         success_count=excluded.success_count,
                         skipped_count=excluded.skipped_count,
                         conflict_count=excluded.conflict_count,
                         diagnosis_count=excluded.diagnosis_count,
                         unconfirmed_count=excluded.unconfirmed_count,
                         pending_gauge=excluded.pending_gauge,
                         inconsistent_gauge=excluded.inconsistent_gauge,
                         permit_enabled=excluded.permit_enabled,
                         gauge_stale=excluded.gauge_stale,
                         snapshot_payload=excluded.snapshot_payload,
                         updated_at=excluded.updated_at",
                    params![
                        contract,
                        root,
                        phase,
                        health,
                        now,
                        now,
                        last_failure_at,
                        last_recovered_at,
                        attempt_count,
                        success_count,
                        skipped_count,
                        conflict_count,
                        diagnosis_count,
                        unconfirmed_count,
                        i64::try_from(pending_gauge).unwrap_or(i64::MAX),
                        i64::try_from(inconsistent_gauge).unwrap_or(i64::MAX),
                        permit_enabled as i32,
                        gauge_stale as i32,
                        refs.as_bytes(),
                    ],
                )?;
                let epoch: i64 = transaction
                    .query_row(
                        "SELECT value FROM manager_metadata WHERE key=?1",
                        params![Self::HEALTH_EPOCH_KEY],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0_i64)
                    .checked_add(1)
                    .ok_or(RepositoryError::RevisionOverflow)?;
                transaction.execute(
                    "INSERT INTO manager_metadata(key,value) VALUES (?1,?2)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![Self::HEALTH_EPOCH_KEY, epoch.to_string()],
                )?;
                Ok(())
            })?;
        Ok(ManagerCommit {
            manager_sequence: self.storage.manager_sequence(),
            generation: self.storage.generation(),
        })
    }
}
