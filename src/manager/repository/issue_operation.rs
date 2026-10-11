use super::durable_helpers::*;
use super::*;

impl<'a> ManagerRepository<'a> {
    pub(crate) fn abort_idempotency_reservation(
        &mut self,
        scope: &str,
        request_digest: &str,
        operation_id: &str,
    ) -> Result<(), RepositoryError> {
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                transaction.execute(
                    "DELETE FROM manager_idempotency
                       WHERE idempotency_scope=?1 AND request_digest=?2
                         AND operation_id=?3 AND result_snapshot IS NULL
                         AND NOT EXISTS (
                           SELECT 1 FROM manager_operation
                            WHERE operation_id=?3
                         )",
                    params![scope, request_digest, operation_id],
                )?;
                transaction.execute(
                    "DELETE FROM manager_resolution_request
                       WHERE resolution_operation_id=?1",
                    params![operation_id],
                )?;
                Ok(())
            })
            .map(|_| ())
    }

    /// Remove a reservation left behind by a process stop between the
    /// idempotency insert and the operation/outbox admission transaction.
    /// This is safe only when no operation row references the reservation;
    /// a terminal or in-flight operation is left untouched for normal
    /// reconciliation/manual resolution.
    pub(crate) fn abort_orphaned_idempotency_reservation(
        &mut self,
        scope: &str,
        request_digest: &str,
    ) -> Result<bool, RepositoryError> {
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                let changed = transaction.execute(
                    "DELETE FROM manager_idempotency
                       WHERE idempotency_scope=?1 AND request_digest=?2
                         AND result_snapshot IS NULL
                         AND NOT EXISTS (
                           SELECT 1 FROM manager_operation AS operation
                            WHERE operation.operation_id = manager_idempotency.operation_id
                         )",
                    params![scope, request_digest],
                )?;
                Ok(changed == 1)
            })
            .map(|(changed, _)| changed)
    }

    /// Atomically admit a manual Issue-operation resolution.  The operation,
    /// idempotency reservation, original request, and source-resolution fence
    /// are written together before any source operation is changed.  This is
    /// the recovery boundary for a process stop between verification and
    /// finalisation: a restart can discover the exact request and operation
    /// rather than seeing an unindexed pending reservation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_resolution_admission(
        &mut self,
        operation: &pb::DurableOperation,
        source_operation_id: &str,
        issue_id: &str,
        expected_source_generation: u64,
        expected_issue_revision: u64,
        decision: i32,
        request_payload: &[u8],
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.method != "manager_resolve_issue_operation"
            || operation.operation_id.is_empty()
            || operation.idempotency_scope.is_empty()
            || operation.request_digest.is_empty()
            || source_operation_id.is_empty()
            || issue_id.is_empty()
            || request_payload.is_empty()
            || digest_bytes(request_payload) != operation.request_digest
        {
            return Err(RepositoryError::RequestSnapshot(
                "resolution admission identity is incomplete".into(),
            ));
        }
        let mutation = sanitize_mutation(&pb::ManagerMutation {
            operation: Some(operation.clone()),
            ..Default::default()
        });
        let mutation_payload = mutation.encode_to_vec();
        let mutation_digest = digest_bytes(&mutation_payload);
        let operation = sanitize_operation(operation)?;
        let request_payload = request_payload.to_vec();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                insert_operation(
                    transaction,
                    &operation,
                    &mutation_payload,
                    &mutation_digest,
                    stamp,
                )?;
                insert_idempotency(transaction, &operation)?;
                let (contract, root) = manager_root_identity(transaction)?;
                let fence_token = digest_bytes(
                    format!(
                        "{}:{}:{}:{}",
                        source_operation_id,
                        operation.operation_id,
                        operation.request_digest,
                        expected_issue_revision
                    )
                    .as_bytes(),
                );
                let now = now_millis();
                transaction.execute(
                    "INSERT INTO manager_resolution_request
                        (contract_fingerprint, data_root_id, resolution_operation_id,
                         source_operation_id, issue_id, request_payload, request_digest,
                         state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'PENDING', ?8, ?8)",
                    params![
                        contract,
                        root,
                        operation.operation_id,
                        source_operation_id,
                        issue_id,
                        request_payload,
                        operation.request_digest,
                        now,
                    ],
                )?;
                // `resolution_fence` is intentionally one row per source
                // operation.  A completed resolution must not permanently
                // prevent a later, independently keyed resolution after an
                // operator has reopened the source.  Conversely, replacing
                // a pending fence would lose the only recovery request, so
                // reject that case explicitly instead of leaking a SQLite
                // PRIMARY KEY error.
                let existing_fence = transaction
                    .query_row(
                        "SELECT resolution_operation_id, state
                           FROM resolution_fence
                          WHERE contract_fingerprint=?1 AND data_root_id=?2
                            AND source_operation_id=?3",
                        params![contract, root, source_operation_id],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?)),
                    )
                    .optional()?;
                if let Some((existing_resolution_id, existing_state)) = existing_fence {
                    let terminal = matches!(
                        pb::DurableOperationState::try_from(existing_state),
                        Ok(pb::DurableOperationState::Committed
                            | pb::DurableOperationState::Failed
                            | pb::DurableOperationState::Arbitrated)
                    );
                    let request_terminal: bool = transaction.query_row(
                        "SELECT NOT EXISTS(
                            SELECT 1 FROM manager_resolution_request
                             WHERE resolution_operation_id=?1 AND state='PENDING'
                        )",
                        params![existing_resolution_id],
                        |row| row.get(0),
                    )?;
                    if existing_resolution_id != operation.operation_id
                        && (!terminal || !request_terminal)
                    {
                        return Err(RepositoryError::OperationConflict);
                    }
                    if existing_resolution_id != operation.operation_id {
                        transaction.execute(
                            "DELETE FROM resolution_fence
                              WHERE contract_fingerprint=?1 AND data_root_id=?2
                                AND source_operation_id=?3",
                            params![contract, root, source_operation_id],
                        )?;
                    }
                }
                transaction.execute(
                    "INSERT INTO resolution_fence
                        (contract_fingerprint, data_root_id, source_operation_id,
                         resolution_operation_id, fence_token, source_generation,
                         expected_issue_revision, decision, request_payload, request_digest,
                         state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)",
                    params![
                        contract,
                        root,
                        source_operation_id,
                        operation.operation_id,
                        fence_token,
                        i64::try_from(expected_source_generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(expected_issue_revision)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        decision,
                        request_payload,
                        operation.request_digest,
                        operation.state,
                        now,
                    ],
                )?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    pub(crate) fn resolution_request_matches(
        &self,
        resolution_operation_id: &str,
        source_operation_id: &str,
        issue_id: &str,
        request_payload: &[u8],
        request_digest: &str,
    ) -> Result<bool, RepositoryError> {
        self.storage
            .read(|connection| {
                let row = connection
                    .query_row(
                        "SELECT source_operation_id, issue_id, request_payload, request_digest
                           FROM manager_resolution_request
                          WHERE resolution_operation_id=?1",
                        params![resolution_operation_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                                row.get::<_, String>(3)?,
                            ))
                        },
                    )
                    .optional()?;
                Ok(row.is_some_and(
                    |(stored_source, stored_issue, stored_payload, stored_digest)| {
                        stored_source == source_operation_id
                            && stored_issue == issue_id
                            && stored_digest == request_digest
                            && stored_payload == request_payload
                            && digest_bytes(&stored_payload) == stored_digest
                    },
                ))
            })
            .map_err(RepositoryError::from)
    }

    /// Enumerate admitted manual-resolution requests that have not reached a
    /// terminal receipt.  The exact protobuf payload and digest are returned
    /// so restart recovery can replay the original request without deriving
    /// intent from mutable Issue state.
    #[allow(dead_code)]
    pub(crate) fn pending_resolution_requests(
        &self,
        limit: usize,
    ) -> Result<Vec<super::PendingResolutionRequest>, RepositoryError> {
        self.pending_resolution_requests_after(limit, None)
    }

    /// Return a rotated page of pending resolutions.  A transiently
    /// unconfirmed request must not occupy the first page forever while later
    /// requests are independently recoverable, so the cursor is checkpointed
    /// by the caller after each evaluated row.
    pub(crate) fn pending_resolution_requests_after(
        &self,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Vec<super::PendingResolutionRequest>, RepositoryError> {
        self.storage
            .read(|connection| {
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let page_limit = i64::try_from(limit).unwrap_or(i64::MAX);
                let mut requests = Vec::with_capacity(limit);
                let select = |statement: &mut rusqlite::Statement<'_>,
                              parameters: &[&dyn rusqlite::ToSql]| {
                    statement
                        .query_map(parameters, |row| {
                            Ok(super::PendingResolutionRequest {
                                resolution_operation_id: row.get(0)?,
                                source_operation_id: row.get(1)?,
                                issue_id: row.get(2)?,
                                request_payload: row.get(3)?,
                                request_digest: row.get(4)?,
                            })
                        })?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(ManagerStorageError::from)
                };
                if let Some(cursor) = cursor {
                    let mut statement = connection.prepare(
                        "SELECT resolution_operation_id, source_operation_id, issue_id,
                                request_payload, request_digest
                           FROM manager_resolution_request
                          WHERE state='PENDING' AND resolution_operation_id > ?1
                          ORDER BY resolution_operation_id LIMIT ?2",
                    )?;
                    requests.extend(select(&mut statement, &[&cursor, &page_limit])?);
                    if requests.len() < limit {
                        let remaining = i64::try_from(limit - requests.len()).unwrap_or(i64::MAX);
                        let mut statement = connection.prepare(
                            "SELECT resolution_operation_id, source_operation_id, issue_id,
                                    request_payload, request_digest
                               FROM manager_resolution_request
                              WHERE state='PENDING' AND resolution_operation_id <= ?1
                              ORDER BY resolution_operation_id LIMIT ?2",
                        )?;
                        requests.extend(select(&mut statement, &[&cursor, &remaining])?);
                    }
                } else {
                    let mut statement = connection.prepare(
                        "SELECT resolution_operation_id, source_operation_id, issue_id,
                                request_payload, request_digest
                           FROM manager_resolution_request
                          WHERE state='PENDING'
                          ORDER BY resolution_operation_id LIMIT ?1",
                    )?;
                    requests.extend(select(&mut statement, &[&page_limit])?);
                }
                requests.truncate(limit);
                Ok(requests)
            })
            .map_err(RepositoryError::from)
    }

    pub(crate) fn resolution_recovery_cursor(&self) -> Result<Option<String>, RepositoryError> {
        self.storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT value FROM manager_metadata WHERE key=?1",
                        params![Self::RESOLUTION_RECOVERY_CURSOR_KEY],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    pub(crate) fn set_resolution_recovery_cursor(
        &mut self,
        operation_id: &str,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation_id.trim().is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "resolution recovery cursor must not be empty".into(),
            ));
        }
        let operation_id = operation_id.to_owned();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                transaction.execute(
                    "INSERT INTO manager_metadata (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![Self::RESOLUTION_RECOVERY_CURSOR_KEY, operation_id],
                )?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    pub(crate) fn has_pending_resolution_requests(&self) -> Result<bool, RepositoryError> {
        self.storage
            .read(|connection| {
                let pending: i64 = connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM manager_resolution_request WHERE state='PENDING')",
                    [],
                    |row| row.get(0),
                )?;
                Ok(pending != 0)
            })
            .map_err(RepositoryError::from)
    }

    /// Return whether an administrative action is still awaiting dispatch,
    /// marker reconciliation, or explicit resolution.  Administrative rows
    /// are kept separate from ownership-recovery projections, so health must
    /// account for them explicitly instead of treating a successful delivery
    /// phase as proof that the primary permit is safe.
    pub(crate) fn has_pending_admin_actions(&self) -> Result<bool, RepositoryError> {
        self.storage
            .read(|connection| {
                let pending: i64 = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM manager_operation
                         WHERE method='manager_apply_issue_admin_action'
                           AND state IN (?1, ?2, ?3, ?4)
                    )",
                    params![
                        pb::DurableOperationState::Received as i32,
                        pb::DurableOperationState::Pending as i32,
                        pb::DurableOperationState::Unconfirmed as i32,
                        pb::DurableOperationState::ManualResolution as i32,
                    ],
                    |row| row.get(0),
                )?;
                Ok(pending != 0)
            })
            .map_err(RepositoryError::from)
    }

    /// Return point-in-time gauges for a resident maintenance phase.  Each
    /// query counts durable rows independently; no phase can overwrite
    /// another phase's health with a synthetic 0/1 value.
    pub(crate) fn phase_health_gauges(
        &self,
        phase: &str,
    ) -> Result<super::MaintenanceHealthGauges, RepositoryError> {
        self.storage
            .read(|connection| {
                let (pending, inconsistent): (i64, i64) = match phase {
                    "ownership_scan" => (
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_ownership_recovery
                              WHERE stopped <> 0 OR in_flight <> 0 OR next_attempt_at IS NOT NULL",
                            [],
                            |row| row.get(0),
                        )?,
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_ownership_integrity_diagnosis
                              WHERE state NOT IN ('RESOLVED', 'STALE', 'SUPERSEDED')
                                AND (lease_classification='INCONSISTENT'
                                     OR diagnosis_class LIKE '%INCONSISTENT%')",
                            [],
                            |row| row.get(0),
                        )?,
                    ),
                    "pending_resolution" => (
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_resolution_request
                              WHERE state='PENDING'",
                            [],
                            |row| row.get(0),
                        )?,
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_resolution_request
                              WHERE state='PENDING' AND resolution_operation_id IN
                                (SELECT operation_id FROM manager_operation
                                  WHERE state IN (?1, ?2))",
                            params![
                                pb::DurableOperationState::Unconfirmed as i32,
                                pb::DurableOperationState::ManualResolution as i32,
                            ],
                            |row| row.get(0),
                        )?,
                    ),
                    "admin_dispatch" | "admin_recovery" => (
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_operation
                              WHERE method='manager_apply_issue_admin_action'
                                AND state IN (?1, ?2, ?3, ?4)",
                            params![
                                pb::DurableOperationState::Received as i32,
                                pb::DurableOperationState::Pending as i32,
                                pb::DurableOperationState::Unconfirmed as i32,
                                pb::DurableOperationState::ManualResolution as i32,
                            ],
                            |row| row.get(0),
                        )?,
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_operation
                              WHERE method='manager_apply_issue_admin_action'
                                AND state IN (?1, ?2)",
                            params![
                                pb::DurableOperationState::Unconfirmed as i32,
                                pb::DurableOperationState::ManualResolution as i32,
                            ],
                            |row| row.get(0),
                        )?,
                    ),
                    "ownership_recovery" => (
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_capability_attempt
                              WHERE state IN (?1, ?2, ?3)",
                            params![
                                super::CAPABILITY_ATTEMPT_PREPARED,
                                super::CAPABILITY_ATTEMPT_SENDING,
                                super::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
                            ],
                            |row| row.get(0),
                        )?,
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_capability_attempt
                              WHERE state IN (?1, ?2)",
                            params![
                                super::CAPABILITY_ATTEMPT_SENDING,
                                super::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
                            ],
                            |row| row.get(0),
                        )?,
                    ),
                    "delivery" => (
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_outbox
                              WHERE status IN (?1, ?2)",
                            params![
                                pb::OutboxStatus::Ready as i32,
                                pb::OutboxStatus::Leased as i32,
                            ],
                            |row| row.get(0),
                        )?,
                        connection.query_row(
                            "SELECT COUNT(*) FROM manager_outbox
                              WHERE status=?1 AND lease_expires_at IS NOT NULL
                                AND lease_expires_at <= ?2",
                            params![pb::OutboxStatus::Leased as i32, now_millis()],
                            |row| row.get(0),
                        )?,
                    ),
                    _ => (0, 0),
                };
                Ok(super::MaintenanceHealthGauges {
                    pending: u64::try_from(pending).unwrap_or(u64::MAX),
                    inconsistent: u64::try_from(inconsistent).unwrap_or(u64::MAX),
                })
            })
            .map_err(RepositoryError::from)
    }

    /// ConfirmNotApplied is only safe after the server can prove that no
    /// delivery path can still execute the source operation.  The human
    /// evidence reference is necessary, but it is not sufficient: a READY,
    /// LEASED, or DISPATCHED outbox row (or a capability attempt that is
    /// still PREPARED/SENDING) would allow a delayed write after the
    /// confirmation.  Keep this check in the repository so every trusted
    /// consumer and restart recovery path uses the same durable proof.
    pub(crate) fn confirm_not_applied_proven(
        &self,
        operation_id: &str,
    ) -> Result<bool, RepositoryError> {
        if operation_id.trim().is_empty() {
            return Ok(false);
        }
        self.storage
            .read(|connection| {
                let (contract, root): (String, String) = connection.query_row(
                    "SELECT
                        (SELECT value FROM manager_metadata WHERE key='contract_fingerprint'),
                        (SELECT value FROM manager_metadata WHERE key='data_root_id')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let operation_state = connection
                    .query_row(
                        "SELECT state, affects_ready, error_snapshot,
                                operation_payload
                           FROM manager_operation
                          WHERE operation_id=?1",
                        params![operation_id],
                        |row| {
                            Ok((
                                row.get::<_, i32>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, Option<Vec<u8>>>(2)?,
                                row.get::<_, Option<Vec<u8>>>(3)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((state, affects_ready, error_snapshot, operation_payload)) =
                    operation_state
                else {
                    return Ok(false);
                };
                let resolution_required = operation_payload
                    .as_deref()
                    .and_then(|payload| pb::DurableOperation::decode(payload).ok())
                    .is_some_and(|operation| operation.resolution_required);
                let state_provable = match pb::DurableOperationState::try_from(state) {
                    Ok(pb::DurableOperationState::Failed) => {
                        affects_ready == 0 && error_snapshot.is_some()
                    }
                    Ok(
                        pb::DurableOperationState::Unconfirmed
                        | pb::DurableOperationState::ManualResolution,
                    ) => resolution_required,
                    _ => false,
                };
                if !state_provable {
                    return Ok(false);
                }
                let live_outbox: i64 = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM manager_outbox
                         WHERE operation_id=?1
                           AND status IN (?2, ?3, ?4)
                    )",
                    params![
                        operation_id,
                        pb::OutboxStatus::Ready as i32,
                        pb::OutboxStatus::Leased as i32,
                        pb::OutboxStatus::Dispatched as i32,
                    ],
                    |row| row.get(0),
                )?;
                if live_outbox != 0 {
                    return Ok(false);
                }
                // Absence of an attempt row is not evidence that a generic
                // backend request was never sent.  Require at least one
                // server-owned attempt record before the evidence-based path
                // can certify NOT_APPLIED; human arbitration intentionally
                // bypasses this proof and records the operator's judgment.
                let attempt_count: i64 = connection.query_row(
                    "SELECT
                        (SELECT COUNT(*) FROM admin_dispatch_attempt
                          WHERE contract_fingerprint=?1 AND data_root_id=?2
                            AND operation_id=?3)
                      + (SELECT COUNT(*) FROM manager_capability_attempt
                          WHERE contract_fingerprint=?1 AND data_root_id=?2
                            AND operation_id=?3)",
                    params![contract, root, operation_id],
                    |row| row.get(0),
                )?;
                if attempt_count == 0 {
                    // The absence of an admin/capability evidence row is not
                    // proof that a generic backend request was never
                    // delivered.  Require a persisted execution-evidence row
                    // before allowing CONFIRM_NOT_APPLIED.
                    return Ok(false);
                }
                let unproven_admin_attempt: i64 = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM admin_dispatch_attempt
                         WHERE contract_fingerprint=?1 AND data_root_id=?2
                           AND operation_id=?3
                           AND COALESCE(evidence_classification, '')
                               NOT IN ('NOT_SENT_RETRY', 'REJECTED_BEFORE_MUTATION')
                    )",
                    params![contract, root, operation_id],
                    |row| row.get(0),
                )?;
                if unproven_admin_attempt != 0 {
                    return Ok(false);
                }
                let unproven_capability_attempt: i64 = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM manager_capability_attempt
                         WHERE contract_fingerprint=?1 AND data_root_id=?2
                           AND operation_id=?3
                           AND (state <> ?4
                                OR COALESCE(evidence_classification, '')
                                   <> 'REJECTED_BEFORE_MUTATION')
                    )",
                    params![
                        contract,
                        root,
                        operation_id,
                        super::CAPABILITY_ATTEMPT_RESOLVED,
                    ],
                    |row| row.get(0),
                )?;
                if unproven_capability_attempt != 0 {
                    return Ok(false);
                }
                let live_admin_attempt: i64 = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM admin_dispatch_attempt
                         WHERE contract_fingerprint=?1 AND data_root_id=?2
                           AND operation_id=?3
                           AND state IN (?4, ?5, ?6)
                    )",
                    params![
                        contract,
                        root,
                        operation_id,
                        pb::OutboxStatus::Ready as i32,
                        pb::OutboxStatus::Leased as i32,
                        pb::OutboxStatus::Dispatched as i32,
                    ],
                    |row| row.get(0),
                )?;
                if live_admin_attempt != 0 {
                    return Ok(false);
                }
                let live_capability_attempt: i64 = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM manager_capability_attempt
                         WHERE contract_fingerprint=?1 AND data_root_id=?2
                           AND operation_id=?3
                           AND state IN (?4, ?5)
                    )",
                    params![
                        contract,
                        root,
                        operation_id,
                        super::CAPABILITY_ATTEMPT_PREPARED,
                        super::CAPABILITY_ATTEMPT_SENDING,
                    ],
                    |row| row.get(0),
                )?;
                Ok(live_capability_attempt == 0)
            })
            .map_err(RepositoryError::from)
    }

    /// Persist a definitive manual-resolution rejection.  Once a resolution
    /// admission exists its idempotency key must never remain pending: the
    /// rejection and its typed error receipt close the operation, request,
    /// fence, and replay index atomically.
    pub(crate) fn finalize_resolution_failure(
        &mut self,
        operation_id: &str,
        request_digest: &str,
        code: pb::ManagerErrorCode,
        message: &str,
    ) -> Result<Vec<u8>, RepositoryError> {
        let mut operation = self
            .get_operation(operation_id)?
            .ok_or(RepositoryError::OperationConflict)?;
        if operation.request_digest != request_digest
            || operation.method != "manager_resolve_issue_operation"
        {
            return Err(RepositoryError::OperationConflict);
        }
        // A concurrent resolver may have already closed this request.  Do
        // not advance the terminal generation again (or replace its receipt)
        // when a stale recovery pass reaches the finalizer after that commit.
        // Returning the persisted response makes the finalizer idempotent,
        // while a terminal row without a result remains a storage
        // inconsistency that must not be guessed around.
        if matches!(
            pb::DurableOperationState::try_from(operation.state),
            Ok(pb::DurableOperationState::Committed | pb::DurableOperationState::Failed)
        ) {
            let existing = self
                .storage
                .read(|connection| {
                    connection
                        .query_row(
                            "SELECT result_snapshot FROM manager_idempotency
                               WHERE operation_id=?1 AND request_digest=?2",
                            params![operation_id, request_digest],
                            |row| row.get::<_, Option<Vec<u8>>>(0),
                        )
                        .optional()
                        .map_err(ManagerStorageError::from)
                })
                .map_err(RepositoryError::from)?;
            if let Some(Some(result)) = existing {
                return Ok(result);
            }
            return Err(RepositoryError::OperationConflict);
        }
        let expected_payload_digest = digest_bytes(&operation.encode_to_vec());
        operation.state = pb::DurableOperationState::Failed as i32;
        operation.failure_class = pb::FailureClass::Terminal as i32;
        operation.affects_ready = false;
        operation.resolution_required = false;
        operation.generation = operation
            .generation
            .checked_add(1)
            .ok_or(RepositoryError::RevisionOverflow)?;
        let error = pb::ManagerError {
            code: code as i32,
            message: message.to_owned(),
            retryable: false,
            detail: None,
        };
        operation.terminal_snapshot = Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(
            pb::Snapshot {
                schema: "issue_tracker.manager.v1.ManagerError".into(),
                payload: error.encode_to_vec(),
            },
        ));
        let response = pb::ResolveIssueOperationResult {
            outcome: Some(pb::resolve_issue_operation_result::Outcome::Error(error)),
        };
        let response_payload = response.encode_to_vec();
        let operation = sanitize_operation(&operation)?;
        let operation_payload = operation.encode_to_vec();
        let (error_payload, error_schema, error_digest) = match operation.terminal_snapshot.as_ref()
        {
            Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot)) => (
                snapshot.payload.as_slice(),
                snapshot.schema.as_str(),
                digest_bytes(&snapshot.payload),
            ),
            _ => return Err(RepositoryError::OperationConflict),
        };
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let updated = transaction.execute(
                    "UPDATE manager_operation SET state=?2, generation=?3,
                        manager_sequence=?4, affects_ready=?5, result_snapshot=NULL,
                        error_snapshot=?6, result_snapshot_schema=NULL,
                        result_snapshot_digest=NULL, error_snapshot_schema=?7,
                        error_snapshot_digest=?8, operation_payload=?9,
                        operation_payload_schema=?10, operation_payload_digest=?11,
                        updated_at=?12 WHERE operation_id=?1
                          AND operation_payload_digest=?13",
                    params![
                        operation.operation_id,
                        operation.state,
                        i64::try_from(operation.generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(stamp.manager_sequence)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::from(operation.affects_ready),
                        error_payload,
                        error_schema,
                        error_digest,
                        operation_payload,
                        "issue_tracker.manager.v1.DurableOperation",
                        digest_bytes(&operation.encode_to_vec()),
                        operation_updated_at_millis(&operation, now_millis()),
                        expected_payload_digest,
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                super::domain::upsert_operation_history_projection(
                    transaction,
                    &operation,
                    operation_updated_at_millis(&operation, now_millis()),
                )?;
                let updated = transaction.execute(
                    "UPDATE manager_idempotency SET state=?2, result_snapshot=?3,
                        updated_at=?4 WHERE operation_id=?1 AND request_digest=?5
                        AND result_snapshot IS NULL",
                    params![
                        operation.operation_id,
                        operation.state,
                        response_payload,
                        now_millis(),
                        request_digest,
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                let request_changed = transaction.execute(
                    "UPDATE manager_resolution_request SET state='FAILED', updated_at=?2
                       WHERE resolution_operation_id=?1 AND state='PENDING'",
                    params![operation.operation_id, now_millis()],
                )?;
                if request_changed != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                transaction.execute(
                    "UPDATE resolution_fence SET state=?2, updated_at=?3
                       WHERE resolution_operation_id=?1",
                    params![operation.operation_id, operation.state, now_millis()],
                )?;
                Ok(())
            })
            .map(|(_, commit)| {
                let _ = commit;
                response_payload
            })
    }
}
