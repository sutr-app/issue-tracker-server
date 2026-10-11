//! Generic entity persistence, durable operations, and outbox recovery.

use super::durable_helpers::*;
use super::*;

impl<'a> ManagerRepository<'a> {
    pub(crate) const OWNERSHIP_MAINTENANCE_CURSOR_KEY: &'static str =
        "ownership_maintenance_cursor_issue_id";
    pub(crate) const OWNERSHIP_MAINTENANCE_SCAN_COMPLETE_KEY: &'static str =
        "ownership_maintenance_scan_complete";
    pub(crate) const CAPABILITY_RECOVERY_CURSOR_KEY: &'static str =
        "capability_recovery_cursor_operation_id";
    pub(crate) const UNCONFIRMED_RECOVERY_CURSOR_KEY: &'static str =
        "unconfirmed_recovery_cursor_created_at_operation_id";
    pub(crate) const RESOLUTION_RECOVERY_CURSOR_KEY: &'static str =
        "resolution_recovery_cursor_operation_id";
    pub(crate) const OWNERSHIP_MAINTENANCE_HEALTH_KEY: &'static str =
        "ownership_maintenance_healthy";
    pub(crate) const OWNERSHIP_MAINTENANCE_HEALTH_UPDATED_AT_KEY: &'static str =
        "ownership_maintenance_health_updated_at";
    pub(crate) const HEALTH_EPOCH_KEY: &'static str = "maintenance_health_epoch";
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_admin_dispatch_attempt(
        &mut self,
        operation_id: &str,
        attempt_generation: u64,
        outbox_id: &str,
        state: i32,
        lease_id: Option<&str>,
        lease_expires_at: Option<i64>,
        resolved_at: Option<i64>,
        evidence_classification: Option<&str>,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                let changed = transaction.execute(
                    "UPDATE admin_dispatch_attempt
                        SET state=?4, lease_id=?5, lease_expires_at=?6,
                            resolved_at=?7, evidence_classification=?8
                      WHERE operation_id=?1 AND attempt_generation=?2
                        AND outbox_id=?3",
                    params![
                        operation_id,
                        i64::try_from(attempt_generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        outbox_id,
                        state,
                        lease_id,
                        lease_expires_at,
                        resolved_at,
                        evidence_classification,
                    ],
                )?;
                if changed != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                // Mark the outbox as dispatched in the same transaction as
                // the attempt fence.  A restart must never reclaim this row
                // as a fresh lease: it is reconciled as SENT_UNKNOWN first.
                let outbox_changed = transaction.execute(
                    "UPDATE manager_outbox
                        SET status=?2, dispatched_at=COALESCE(dispatched_at, ?3)
                      WHERE outbox_id=?1 AND operation_id=?4
                        AND status=?5 AND lease_id=?6",
                    params![
                        outbox_id,
                        pb::OutboxStatus::Dispatched as i32,
                        now_millis(),
                        operation_id,
                        pb::OutboxStatus::Leased as i32,
                        lease_id,
                    ],
                )?;
                if outbox_changed != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Admit a capability-backed backend request and its outbox row in one
    /// durable transaction. The capability fingerprint and marker expectation
    /// are persisted with the operation so dispatch/recovery can validate the
    /// exact request after a restart.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_capability_admission(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        request: &[u8],
        method: &str,
        issue_id: &str,
        capability_fingerprint: &str,
        expectation: &super::super::backend_adapter::MarkerExpectation,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.method != method
            || operation.request_schema != "issue_tracker.manager.v1.EphemeralLeaseCapability"
            || operation.state != pb::DurableOperationState::Pending as i32
        {
            return Err(RepositoryError::OperationConflict);
        }
        let snapshot = sanitize_mutation(mutation).encode_to_vec();
        let snapshot_digest = digest_bytes(&snapshot);
        let operation = sanitize_operation(operation)?;
        let request = request.to_vec();
        let method = method.to_owned();
        let issue_id = issue_id.to_owned();
        let capability_fingerprint = capability_fingerprint.to_owned();
        let expectation = expectation_snapshot(expectation).encode_to_vec();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                insert_operation(transaction, &operation, &snapshot, &snapshot_digest, stamp)?;
                transaction.execute(
                    "UPDATE manager_operation
                        SET request_snapshot=?2, request_snapshot_schema=?3,
                            request_snapshot_digest=?4, marker_expectation=?5,
                            marker_expectation_schema=?6
                      WHERE operation_id=?1",
                    params![
                        operation.operation_id,
                        request,
                        "issue_tracker.manager.v1.EphemeralLeaseCapability",
                        digest_bytes(&request),
                        expectation,
                        "issue_tracker.v1.OperationRecord",
                    ],
                )?;
                insert_idempotency(transaction, &operation)?;
                insert_capability_attempt(
                    transaction,
                    &operation,
                    &method,
                    &issue_id,
                    &operation.request_digest,
                    &capability_fingerprint,
                )?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Acquire the durable send permission for one capability attempt.  The
    /// expected PREPARED state makes duplicate live tasks harmless.
    pub(crate) fn mark_capability_attempt_sending(
        &mut self,
        operation_id: &str,
        attempt_generation: u64,
        permitted_at: i64,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                let changed = transaction.execute(
                    "UPDATE manager_capability_attempt
                        SET state=?4, permitted_at=?5, sent_at=?5, updated_at=?5
                      WHERE operation_id=?1 AND attempt_generation=?2 AND state=?3",
                    params![
                        operation_id,
                        i64::try_from(attempt_generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        super::CAPABILITY_ATTEMPT_PREPARED,
                        super::CAPABILITY_ATTEMPT_SENDING,
                        permitted_at,
                    ],
                )?;
                if changed != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Close an execution-evidence row while updating the corresponding
    /// operation in the same SQLite transaction.  The optional replay result
    /// is already sanitized by the caller (notably, Claim never persists its
    /// one-time lease handle).
    pub(crate) fn finalize_capability_operation(
        &mut self,
        operation: &pb::DurableOperation,
        replay_result: Option<&[u8]>,
        evidence_state: &str,
        evidence_classification: &str,
        resolved_at: i64,
    ) -> Result<ManagerCommit, RepositoryError> {
        let operation = sanitize_operation(operation)?;
        let terminal = operation.terminal_snapshot.as_ref();
        if let Some(snapshot) = terminal {
            let snapshot = match snapshot {
                pb::durable_operation::TerminalSnapshot::ResultSnapshot(value)
                | pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value) => value,
            };
            validate_snapshot(snapshot)?;
        }
        if !matches!(
            evidence_state,
            super::CAPABILITY_ATTEMPT_RESOLVED | super::CAPABILITY_ATTEMPT_SENT_UNKNOWN
        ) {
            return Err(RepositoryError::RequestSnapshot(
                "invalid capability evidence terminal state".into(),
            ));
        }
        let payload = operation.encode_to_vec();
        let (result_snapshot, error_snapshot) = match terminal {
            Some(pb::durable_operation::TerminalSnapshot::ResultSnapshot(value)) => {
                (Some(value), None)
            }
            Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value)) => {
                (None, Some(value))
            }
            None => (None, None),
        };
        let replay_result = replay_result.map(Vec::from);
        let evidence_snapshot = format!(
            "{{\"operation_id\":\"{}\",\"state\":\"{}\",\"classification\":\"{}\"}}",
            operation.operation_id, operation.state, evidence_classification
        )
        .into_bytes();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                // A recovery/finalization caller may have read the
                // operation before a concurrent manual resolution or a
                // second resident pass changed it.  Decode the authoritative
                // payload and require the same per-operation generation and
                // a still-live source state before applying this result. The
                // global Manager generation is only an ordering stamp and is
                // deliberately not used for this CAS.
                let current_payload: Vec<u8> = transaction.query_row(
                    "SELECT operation_payload FROM manager_operation
                       WHERE operation_id=?1",
                    params![operation.operation_id],
                    |row| row.get(0),
                )?;
                let current = pb::DurableOperation::decode(current_payload.as_slice())
                    .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                if current.operation_id != operation.operation_id
                    || current.generation != operation.generation
                    || !matches!(
                        pb::DurableOperationState::try_from(current.state),
                        Ok(pb::DurableOperationState::Pending
                            | pb::DurableOperationState::Unconfirmed)
                    )
                {
                    return Err(RepositoryError::OperationConflict);
                }
                let current_payload_digest = digest_bytes(&current_payload);
                let changed = transaction.execute(
                    "UPDATE manager_operation SET state=?2,
                         attempt=?3, manager_sequence=?4, generation=?5,
                         affects_ready=?6, result_snapshot=?7, error_snapshot=?8,
                         updated_at=?9, result_snapshot_schema=?10,
                         result_snapshot_digest=?11, error_snapshot_schema=?12,
                         error_snapshot_digest=?13, operation_payload=?14,
                         operation_payload_schema=?15, operation_payload_digest=?16
                       WHERE operation_id=?1 AND operation_payload_digest=?17",
                    params![
                        operation.operation_id,
                        operation.state,
                        i64::from(operation.attempt),
                        i64::try_from(stamp.manager_sequence)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(stamp.generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::from(operation.affects_ready),
                        result_snapshot.map(|value| value.payload.as_slice()),
                        error_snapshot.map(|value| value.payload.as_slice()),
                        operation_updated_at_millis(&operation, resolved_at),
                        result_snapshot.map(|value| value.schema.as_str()),
                        result_snapshot.map(|value| digest_bytes(&value.payload)),
                        error_snapshot.map(|value| value.schema.as_str()),
                        error_snapshot.map(|value| digest_bytes(&value.payload)),
                        payload,
                        "issue_tracker.manager.v1.DurableOperation",
                        digest_bytes(&operation.encode_to_vec()),
                        current_payload_digest,
                    ],
                )?;
                if changed != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                let attempt_changed = transaction.execute(
                    "UPDATE manager_capability_attempt
                        SET state=?3, sent_at=COALESCE(sent_at, ?4),
                            resolved_at=?4, evidence_classification=?5,
                            evidence_snapshot=?6, updated_at=?4
                      WHERE operation_id=?1 AND attempt_generation=?2
                        AND state IN (?7, ?8, ?9)",
                    params![
                        operation.operation_id,
                        i64::try_from(operation.generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        evidence_state,
                        resolved_at,
                        evidence_classification,
                        evidence_snapshot,
                        super::CAPABILITY_ATTEMPT_PREPARED,
                        super::CAPABILITY_ATTEMPT_SENDING,
                        super::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
                    ],
                )?;
                if attempt_changed != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                if let Some(result) = replay_result {
                    let updated = transaction.execute(
                        "UPDATE manager_idempotency SET state=?2, result_snapshot=?3,
                          updated_at=?4
                          WHERE idempotency_scope=?1 AND request_digest=?5
                            AND operation_id=?6",
                        params![
                            operation.idempotency_scope,
                            operation.state,
                            result,
                            resolved_at,
                            operation.request_digest,
                            operation.operation_id,
                        ],
                    )?;
                    if updated != 1 {
                        return Err(RepositoryError::OperationConflict);
                    }
                }
                upsert_operation_history_projection(transaction, &operation, resolved_at)?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Administrative requests have no public backend RPC, but must retain
    /// their exact input before canonical Tracker state can be touched.
    pub(crate) fn commit_admin_admission(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        request: &[u8],
        expectation: &super::super::backend_adapter::MarkerExpectation,
    ) -> Result<ManagerCommit, RepositoryError> {
        // Administrative writes use the same restart-safe envelope and
        // outbox lifecycle as ordinary backend mutations.  The northbound
        // ApplyIssueAdminActionArgs are converted before opening the storage
        // transaction so malformed requests can never create a dispatchable
        // row.
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
        if operation.method != "manager_apply_issue_admin_action"
            || operation.downstream_operation_id.trim().is_empty()
        {
            return Err(RepositoryError::OperationConflict);
        }
        let snapshot = sanitize_mutation(mutation).encode_to_vec();
        let request = durable_request;
        let operation = operation.clone();
        let outbox = outbox::new_entry(
            format!("outbox-{}", operation.operation_id),
            &operation,
            "taskchampion",
            operation.operation_id.clone(),
            digest_bytes(&request),
            now_millis(),
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        let expectation =
            super::super::backend_dispatch::expectation_snapshot(expectation).encode_to_vec();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                insert_operation(
                    transaction,
                    &operation,
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
                        request,
                        "issue_tracker.manager.v1.DurableBackendRequest",
                        digest_bytes(&request),
                        expectation,
                        "issue_tracker.v1.OperationRecord",
                    ],
                )?;
                insert_backend_request_index(
                    transaction,
                    &operation,
                    "manager_apply_issue_admin_action",
                    "issue_tracker.manager.v1.DurableBackendRequest",
                    &request,
                )?;
                insert_admin_expectation_index(transaction, &operation, &request, &expectation)?;
                insert_idempotency(transaction, &operation)?;
                insert_outbox(transaction, &outbox, stamp)?;
                insert_admin_dispatch_attempt(transaction, &operation, &outbox)?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    pub(crate) fn pending_admin_request_snapshots(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, RepositoryError> {
        self.storage
            .read(|connection| {
                let mut statement = connection.prepare(
                    "SELECT operation_id, request_snapshot FROM manager_operation
                     WHERE method='manager_apply_issue_admin_action'
                       AND state IN (?1, ?2)
                       AND request_snapshot_schema IN (
                           'issue_tracker.manager.v1.ApplyIssueAdminActionArgs',
                           'issue_tracker.manager.v1.DurableBackendRequest'
                       )
                     ORDER BY created_at, operation_id LIMIT ?3",
                )?;
                statement
                    .query_map(
                        params![
                            pb::DurableOperationState::Received as i32,
                            pb::DurableOperationState::Pending as i32,
                            i64::try_from(limit).unwrap_or(i64::MAX)
                        ],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    /// Store an opaque manager protobuf and advance its manager revision.
    pub(crate) fn put_proto<M: Message>(
        &mut self,
        kind: EntityKind,
        entity_id: &str,
        revision: u64,
        payload_schema: &str,
        payload: &M,
    ) -> Result<ManagerCommit, RepositoryError> {
        let bytes = payload.encode_to_vec();
        self.put_entity(kind, entity_id, revision, payload_schema, &bytes)
    }

    /// Store an entity only when its current revision equals `expected_revision`.
    #[allow(dead_code)]
    pub(crate) fn put_proto_cas<M: Message>(
        &mut self,
        kind: EntityKind,
        entity_id: &str,
        expected_revision: u64,
        revision: u64,
        payload_schema: &str,
        payload: &M,
    ) -> Result<ManagerCommit, RepositoryError> {
        let bytes = payload.encode_to_vec();
        self.put_entity_cas(
            kind,
            entity_id,
            expected_revision,
            revision,
            payload_schema,
            &bytes,
        )
    }

    pub(crate) fn put_entity(
        &mut self,
        kind: EntityKind,
        entity_id: &str,
        revision: u64,
        payload_schema: &str,
        payload: &[u8],
    ) -> Result<ManagerCommit, RepositoryError> {
        self.write_entity(kind, entity_id, None, revision, payload_schema, payload)
    }

    #[allow(dead_code)]
    pub(crate) fn put_entity_cas(
        &mut self,
        kind: EntityKind,
        entity_id: &str,
        expected_revision: u64,
        revision: u64,
        payload_schema: &str,
        payload: &[u8],
    ) -> Result<ManagerCommit, RepositoryError> {
        self.write_entity(
            kind,
            entity_id,
            Some(expected_revision),
            revision,
            payload_schema,
            payload,
        )
    }

    pub(crate) fn get(
        &self,
        kind: EntityKind,
        entity_id: &str,
    ) -> Result<Option<StoredEntity>, RepositoryError> {
        if entity_id.is_empty() {
            return Err(RepositoryError::EmptyEntityId);
        }
        let kind_name = kind.as_str();
        Ok(self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT revision, payload, payload_schema, payload_digest,
                            created_at, updated_at
                       FROM manager_entity
                      WHERE entity_kind = ?1 AND entity_id = ?2",
                    params![kind_name, entity_id],
                    |row| {
                        Ok(StoredEntity {
                            kind,
                            entity_id: entity_id.to_owned(),
                            revision: row.get::<_, i64>(0)?.try_into().map_err(|_| {
                                rusqlite::Error::InvalidColumnType(
                                    0,
                                    "revision".into(),
                                    rusqlite::types::Type::Integer,
                                )
                            })?,
                            payload: row.get(1)?,
                            payload_schema: row.get(2)?,
                            payload_digest: row.get(3)?,
                            created_at: row.get(4)?,
                            updated_at: row.get(5)?,
                        })
                    },
                )
                .optional()
                .map_err(ManagerStorageError::from)
        })?)
    }

    pub(super) fn list_entities(
        &self,
        kind: EntityKind,
    ) -> Result<Vec<StoredEntity>, RepositoryError> {
        let kind_name = kind.as_str();
        Ok(self.storage.read(|connection| {
            let mut statement = connection.prepare(
                "SELECT entity_id, revision, payload, payload_schema, payload_digest,
                        created_at, updated_at
                   FROM manager_entity
                  WHERE entity_kind = ?1
                  ORDER BY entity_id",
            )?;
            let rows = statement.query_map(params![kind_name], |row| {
                Ok(StoredEntity {
                    kind,
                    entity_id: row.get(0)?,
                    revision: row.get::<_, i64>(1)?.try_into().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            1,
                            "revision".into(),
                            rusqlite::types::Type::Integer,
                        )
                    })?,
                    payload: row.get(2)?,
                    payload_schema: row.get(3)?,
                    payload_digest: row.get(4)?,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(ManagerStorageError::from)
        })?)
    }

    pub(crate) fn get_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<pb::DurableOperation>, RepositoryError> {
        if operation_id.is_empty() {
            return Err(RepositoryError::EmptyOperationId);
        }
        let payload = self
                .storage
                .read(|connection| {
                    connection
                        .query_row(
                            "SELECT operation_payload, operation_payload_schema, operation_payload_digest FROM manager_operation
                          WHERE operation_id = ?1",
                            params![operation_id],
                            |row| Ok((row.get::<_, Option<Vec<u8>>>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, Option<String>>(2)?)),
                        )
                        .optional()
                        .map_err(ManagerStorageError::from)
                })?;
        let Some((Some(bytes), Some(schema), Some(digest))) = payload else {
            return Ok(None);
        };
        if schema != "issue_tracker.manager.v1.DurableOperation" || digest_bytes(&bytes) != digest {
            return Err(RepositoryError::RequestSnapshot(
                "operation payload integrity validation failed".into(),
            ));
        }
        pb::DurableOperation::decode(bytes.as_slice())
            .map(Some)
            .map_err(|error| RepositoryError::Serialization(error.to_string()))
    }

    /// Return the immutable typed replay payload currently associated with an
    /// operation's idempotency reservation. Resolution receipts copy this
    /// value into their pre-resolution audit snapshot before replacing an
    /// uncertain replay result.
    pub(crate) fn operation_replay_snapshot(
        &self,
        operation_id: &str,
        request_digest: &str,
    ) -> Result<Option<Vec<u8>>, RepositoryError> {
        if operation_id.is_empty() || request_digest.is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "operation replay identity must not be empty".into(),
            ));
        }
        self.storage
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
            .map(|value| value.flatten())
            .map_err(RepositoryError::from)
    }

    /// Read the immutable audit row written at a manual-resolution commit.
    /// This is intentionally separate from the mutable source operation so a
    /// later arbitration cannot rewrite the historical boundary.
    #[allow(dead_code)]
    pub(crate) fn issue_operation_resolution_audit(
        &self,
        resolution_operation_id: &str,
    ) -> Result<Option<super::IssueOperationResolutionAudit>, RepositoryError> {
        if resolution_operation_id.is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "resolution operation ID must not be empty".into(),
            ));
        }
        let audit = self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT resolution_operation_id, source_operation_id,
                            source_generation, source_state,
                            source_operation_payload, source_replay_payload,
                            source_operation_payload_digest,
                            source_replay_digest, resolution_result_payload,
                            resolution_result_digest
                       FROM manager_issue_operation_resolution_audit
                      WHERE resolution_operation_id=?1",
                    params![resolution_operation_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i32>(3)?,
                            row.get::<_, Vec<u8>>(4)?,
                            row.get::<_, Option<Vec<u8>>>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, Option<String>>(7)?,
                            row.get::<_, Vec<u8>>(8)?,
                            row.get::<_, String>(9)?,
                        ))
                    },
                )
                .optional()
                .map_err(ManagerStorageError::from)
        })?;
        let Some((
            resolution_operation_id,
            source_operation_id,
            source_generation,
            source_state,
            source_operation_payload,
            source_replay_payload,
            source_operation_payload_digest,
            source_replay_digest,
            resolution_result_payload,
            resolution_result_digest,
        )) = audit
        else {
            return Ok(None);
        };
        let source_generation = u64::try_from(source_generation).map_err(|_| {
            RepositoryError::RequestSnapshot("resolution audit source generation is invalid".into())
        })?;
        let replay_digest_matches = source_replay_payload
            .as_ref()
            .map(|payload| digest_bytes(payload))
            == source_replay_digest;
        if digest_bytes(&source_operation_payload) != source_operation_payload_digest
            || !replay_digest_matches
            || digest_bytes(&resolution_result_payload) != resolution_result_digest
        {
            return Err(RepositoryError::RequestSnapshot(
                "resolution audit payload integrity validation failed".into(),
            ));
        }
        Ok(Some(super::IssueOperationResolutionAudit {
            resolution_operation_id,
            source_operation_id,
            source_generation,
            source_state,
            source_operation_payload,
            source_replay_payload,
            resolution_result_payload,
        }))
    }

    /// Return the safe diagnosis recorded when an operation payload was
    /// quarantined.  This read intentionally does not touch or decode the
    /// corrupt operation payload, allowing GetOperation to report a typed
    /// repair condition instead of manufacturing a DurableOperation.
    pub(crate) fn operation_diagnosis(
        &self,
        operation_id: &str,
    ) -> Result<Option<pb::OperationDiagnosis>, RepositoryError> {
        if operation_id.is_empty() {
            return Ok(None);
        }
        self.storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT classification, source_generation, attempt_generation,
                                evidence_ref, evidence_digest, replayability,
                                resolution_state, resolution_operation_id,
                                repair_condition, recorded_at
                           FROM manager_operation_diagnosis
                          WHERE operation_id=?1
                          ORDER BY diagnosis_generation DESC
                          LIMIT 1",
                        params![operation_id],
                        |row| {
                            Ok(pb::OperationDiagnosis {
                                operation_id: operation_id.to_owned(),
                                classification: row.get(0)?,
                                source_generation: row
                                    .get::<_, Option<i64>>(1)?
                                    .and_then(|value| value.try_into().ok()),
                                attempt_generation: row
                                    .get::<_, Option<i64>>(2)?
                                    .and_then(|value| value.try_into().ok()),
                                evidence_ref: row.get(3)?,
                                evidence_digest: row.get(4)?,
                                replayability: row.get(5)?,
                                resolution_state: row.get(6)?,
                                resolution_operation_id: row.get(7)?,
                                repair_condition: row.get(8)?,
                                recorded_at: Some(pb::UtcInstant {
                                    unix_epoch_millis: row.get(9)?,
                                }),
                            })
                        },
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)
    }

    /// Return delivery-unknown operations whose acknowledged outbox must be
    /// reconciled without any re-delivery after a process restart.
    #[allow(dead_code)]
    pub(crate) fn unconfirmed_acknowledged_operations(
        &self,
    ) -> Result<Vec<pb::DurableOperation>, RepositoryError> {
        let operation_ids = self
            .unconfirmed_acknowledged_operations_page(None, usize::MAX)?
            .into_iter()
            .map(|(operation_id, _)| operation_id)
            .collect::<Vec<_>>();
        operation_ids
            .into_iter()
            .map(|operation_id| {
                self.get_operation(&operation_id)?.ok_or_else(|| {
                    RepositoryError::RequestSnapshot(
                        "unconfirmed operation payload is missing".into(),
                    )
                })
            })
            .collect()
    }

    /// Return one cursor-ordered page of acknowledged UNCONFIRMED
    /// operations. When the cursor reaches the tail, the page wraps to the
    /// beginning so a permanently unresolved operation cannot starve later
    /// entries. The caller checkpoints each evaluated tuple, including rows
    /// whose marker is missing or malformed.
    pub(crate) fn unconfirmed_acknowledged_operations_page(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, i64)>, RepositoryError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let parsed_cursor = cursor.map(|value| {
            let (created_at, operation_id) = value.split_once(':').ok_or_else(|| {
                RepositoryError::RequestSnapshot(
                    "unconfirmed recovery cursor has an invalid format".into(),
                )
            })?;
            let created_at = created_at.parse::<i64>().map_err(|_| {
                RepositoryError::RequestSnapshot(
                    "unconfirmed recovery cursor has an invalid timestamp".into(),
                )
            })?;
            if operation_id.is_empty() {
                return Err(RepositoryError::RequestSnapshot(
                    "unconfirmed recovery cursor has an empty operation ID".into(),
                ));
            }
            Ok((created_at, operation_id.to_owned()))
        });
        let parsed_cursor = parsed_cursor.transpose()?;
        let page_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let read_page = |boundary: Option<(i64, &str)>, inclusive: bool| {
            self.storage.read(|connection| {
                let rows = if let Some((created_at, operation_id)) = boundary {
                    let predicate = if inclusive {
                        "(created_at < ?3 OR (created_at = ?3 AND operation_id <= ?4))"
                    } else {
                        "(created_at > ?3 OR (created_at = ?3 AND operation_id > ?4))"
                    };
                    let query = format!(
                        "SELECT operation_id, created_at
                           FROM manager_operation
                          WHERE state = ?1
                            AND EXISTS (
                                SELECT 1 FROM manager_outbox
                                 WHERE manager_outbox.operation_id = manager_operation.operation_id
                                   AND manager_outbox.status = ?2
                            )
                            AND {predicate}
                          ORDER BY created_at, operation_id
                          LIMIT ?5"
                    );
                    let mut statement = connection.prepare(&query)?;
                    statement
                        .query_map(
                            params![
                                pb::DurableOperationState::Unconfirmed as i32,
                                pb::OutboxStatus::Acknowledged as i32,
                                created_at,
                                operation_id,
                                page_limit,
                            ],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    let mut statement = connection.prepare(
                        "SELECT operation_id, created_at
                           FROM manager_operation
                          WHERE state = ?1
                            AND EXISTS (
                                SELECT 1 FROM manager_outbox
                                 WHERE manager_outbox.operation_id = manager_operation.operation_id
                                   AND manager_outbox.status = ?2
                            )
                          ORDER BY created_at, operation_id
                          LIMIT ?3",
                    )?;
                    statement
                        .query_map(
                            params![
                                pb::DurableOperationState::Unconfirmed as i32,
                                pb::OutboxStatus::Acknowledged as i32,
                                page_limit,
                            ],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                };
                Ok(rows)
            })
        };
        let mut rows = read_page(
            parsed_cursor
                .as_ref()
                .map(|(created_at, operation_id)| (*created_at, operation_id.as_str())),
            false,
        )?;
        if rows.len() < limit {
            if let Some((created_at, operation_id)) = parsed_cursor.as_ref() {
                rows.extend(read_page(Some((*created_at, operation_id.as_str())), true)?);
            }
        }
        rows.truncate(limit);
        Ok(rows)
    }

    /// Return capability attempts that need restart recovery. Capability
    /// writes intentionally have no outbox (their lease handle is
    /// ephemeral), so the attempt table is the durable recovery index.
    /// PREPARED/SENDING rows are normalized by the runner before it inspects
    /// the Tracker marker; SENT_UNKNOWN rows are then reconciled without a
    /// second capability delivery.
    pub(crate) fn capability_recovery_candidates(
        &self,
        _protected_operation_ids: &[String],
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<super::CapabilityRecoveryCandidate>, RepositoryError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = self.storage.read(|connection| {
            let page_limit = i64::try_from(limit).unwrap_or(i64::MAX);
            let states = [
                super::CAPABILITY_ATTEMPT_PREPARED,
                super::CAPABILITY_ATTEMPT_SENDING,
                super::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
            ];
            let operation_states = [
                pb::DurableOperationState::Received as i32,
                pb::DurableOperationState::Pending as i32,
                pb::DurableOperationState::Unconfirmed as i32,
            ];
            let fetch = |boundary: Option<(&str, i64)>,
                         before: bool,
                         page_limit: i64|
             -> Result<Vec<(String, String)>, ManagerStorageError> {
                let mut statement = match (boundary, before) {
                    (Some(_), true) => connection.prepare(
                        "SELECT attempt.operation_id, attempt.state
                           FROM manager_capability_attempt AS attempt
                           LEFT JOIN manager_operation AS operation
                             ON operation.operation_id = attempt.operation_id
                          WHERE attempt.state IN (?1, ?2, ?3)
                            AND (operation.state IN (?4, ?5, ?6)
                                 OR operation.operation_id IS NULL)
                            -- Include the cursor row at wrap-around.  The
                            -- forward query is strict, so this cannot
                            -- duplicate a row in the same page; it ensures a
                            -- sole permanently-failing candidate is retried
                            -- on the next cycle instead of disappearing.
                            AND (attempt.created_at < ?7
                                 OR (attempt.created_at = ?7 AND attempt.operation_id <= ?8))
                          ORDER BY attempt.created_at, attempt.operation_id
                          LIMIT ?9",
                    )?,
                    (Some(_), false) => connection.prepare(
                        "SELECT attempt.operation_id, attempt.state
                           FROM manager_capability_attempt AS attempt
                           LEFT JOIN manager_operation AS operation
                             ON operation.operation_id = attempt.operation_id
                          WHERE attempt.state IN (?1, ?2, ?3)
                            AND (operation.state IN (?4, ?5, ?6)
                                 OR operation.operation_id IS NULL)
                            AND (attempt.created_at > ?7
                                 OR (attempt.created_at = ?7 AND attempt.operation_id > ?8))
                          ORDER BY attempt.created_at, attempt.operation_id
                          LIMIT ?9",
                    )?,
                    (None, _) => connection.prepare(
                        "SELECT attempt.operation_id, attempt.state
                           FROM manager_capability_attempt AS attempt
                           LEFT JOIN manager_operation AS operation
                             ON operation.operation_id = attempt.operation_id
                          WHERE attempt.state IN (?1, ?2, ?3)
                            AND (operation.state IN (?4, ?5, ?6)
                                 OR operation.operation_id IS NULL)
                          ORDER BY attempt.created_at, attempt.operation_id
                          LIMIT ?7",
                    )?,
                };
                if let Some((operation_id, created_at)) = boundary {
                    let rows = statement
                        .query_map(
                            params![
                                states[0],
                                states[1],
                                states[2],
                                operation_states[0],
                                operation_states[1],
                                operation_states[2],
                                created_at,
                                operation_id,
                                page_limit,
                            ],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                        )?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(ManagerStorageError::from)?;
                    Ok(rows)
                } else {
                    let rows = statement
                        .query_map(
                            params![
                                states[0],
                                states[1],
                                states[2],
                                operation_states[0],
                                operation_states[1],
                                operation_states[2],
                                page_limit,
                            ],
                            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                        )?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(ManagerStorageError::from)?;
                    Ok(rows)
                }
            };

            let mut rows = if let Some(cursor) = cursor {
                let cursor_created_at = connection.query_row(
                    "SELECT MIN(created_at) FROM manager_capability_attempt
                          WHERE operation_id=?1",
                    params![cursor],
                    |row| row.get::<_, Option<i64>>(0),
                )?;
                if let Some(cursor_created_at) = cursor_created_at {
                    // The cursor is a fairness boundary, not a snapshot: a
                    // bounded page must not collect every corrupt row before
                    // it can reach later candidates.
                    fetch(Some((cursor, cursor_created_at)), false, page_limit)?
                } else {
                    // A pruned cursor starts a fresh pass. Treating a missing
                    // cursor as the minimum timestamp would skip all rows
                    // sharing the earliest timestamp.
                    fetch(None, false, page_limit)?
                }
            } else {
                fetch(None, false, page_limit)?
            };

            // Once the tail reaches the end, wrap to the beginning.  This is
            // what lets a permanently broken first row consume one budget
            // slot without starving healthy rows behind it on later cycles.
            if rows.len() < limit && cursor.is_some() {
                let remaining = i64::try_from(limit - rows.len()).unwrap_or(i64::MAX);
                if let Some(cursor) = cursor {
                    let cursor_created_at = connection.query_row(
                        "SELECT MIN(created_at) FROM manager_capability_attempt
                              WHERE operation_id=?1",
                        params![cursor],
                        |row| row.get::<_, Option<i64>>(0),
                    )?;
                    if let Some(cursor_created_at) = cursor_created_at {
                        rows.extend(fetch(Some((cursor, cursor_created_at)), true, remaining)?);
                    }
                }
            }
            rows.truncate(limit);
            Ok(rows)
        })?;
        // Protected IDs are retained in the page so the caller can advance
        // the cursor over them as well; filtering them here would let one
        // permanently live task pin the cursor at the same row forever.
        let candidates = rows
            .into_iter()
            .map(|(operation_id, attempt_state)| {
                let (operation, error) = match self.get_operation(&operation_id) {
                    Ok(Some(operation)) => (Some(operation), None),
                    Ok(None) => (
                        None,
                        Some("capability recovery operation payload is missing".into()),
                    ),
                    Err(error) => (None, Some(error.to_string())),
                };
                super::CapabilityRecoveryCandidate {
                    operation_id,
                    attempt_state,
                    operation,
                    error,
                }
            })
            .collect::<Vec<_>>();
        Ok(candidates)
    }

    /// Convert an administrative dispatch that was marked SENDING before a
    /// process stopped into an acknowledged, delivery-unknown operation.
    /// The outbox is deliberately closed here; the next recovery pass may
    /// inspect the immutable Tracker marker, but it must never send the same
    /// mutation a second time merely because the process was restarted.
    pub(crate) fn recover_inflight_admin_dispatches(
        &mut self,
        now_millis: i64,
        protected_operation_ids: &[String],
    ) -> Result<u32, RepositoryError> {
        let operation_ids = self.storage.read(|connection| {
            let mut statement = connection.prepare(
                "SELECT attempt.operation_id, attempt.outbox_id
                   FROM admin_dispatch_attempt AS attempt
                   JOIN manager_outbox AS outbox
                     ON outbox.operation_id = attempt.operation_id
                    AND outbox.outbox_id = attempt.outbox_id
                  WHERE attempt.state = ?1
                    AND outbox.status = ?1
                  ORDER BY attempt.operation_id",
            )?;
            let rows = statement
                .query_map(params![pb::OutboxStatus::Dispatched as i32], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(ManagerStorageError::from)
        })?;
        let mut recovered = 0_u32;
        let mut first_error: Option<RepositoryError> = None;
        for (operation_id, outbox_id) in operation_ids {
            if protected_operation_ids
                .iter()
                .any(|protected| protected == &operation_id)
            {
                continue;
            }
            let Some(mut operation) = (match self.get_operation(&operation_id) {
                Ok(operation) => operation,
                Err(error) => {
                    first_error.get_or_insert(error);
                    continue;
                }
            }) else {
                first_error.get_or_insert(RepositoryError::RequestSnapshot(
                    "administrative recovery operation payload is missing".into(),
                ));
                continue;
            };
            let Some(mut outbox) = (match self.get_outbox(&outbox_id) {
                Ok(outbox) => outbox,
                Err(error) => {
                    first_error.get_or_insert(error);
                    continue;
                }
            }) else {
                first_error.get_or_insert(RepositoryError::RequestSnapshot(
                    "administrative recovery outbox entry is missing".into(),
                ));
                continue;
            };
            let state = match pb::DurableOperationState::try_from(operation.state) {
                Ok(state) => state,
                Err(error) => {
                    first_error.get_or_insert(RepositoryError::RequestSnapshot(format!(
                        "invalid operation state: {error}"
                    )));
                    continue;
                }
            };
            if matches!(
                state,
                pb::DurableOperationState::Received | pb::DurableOperationState::Pending
            ) {
                if state == pb::DurableOperationState::Received {
                    if let Err(error) = transition_operation(
                        &mut operation,
                        pb::DurableOperationState::Pending,
                        None,
                        now_millis,
                    ) {
                        first_error
                            .get_or_insert(RepositoryError::RequestSnapshot(error.to_string()));
                        continue;
                    }
                }
                if let Err(error) = transition_operation(
                    &mut operation,
                    pb::DurableOperationState::Unconfirmed,
                    None,
                    now_millis,
                ) {
                    first_error.get_or_insert(RepositoryError::RequestSnapshot(error.to_string()));
                    continue;
                }
                outbox.status = pb::OutboxStatus::Acknowledged as i32;
                outbox.acknowledged_at = Some(pb::UtcInstant {
                    unix_epoch_millis: now_millis,
                });
                outbox.lease_id = None;
                outbox.lease_expires_at = None;
                match self.finalize_admin_unconfirmed(&operation, &outbox) {
                    Ok(_) => recovered = recovered.saturating_add(1),
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(recovered), Err)
    }

    pub(crate) fn get_outbox(
        &self,
        outbox_id: &str,
    ) -> Result<Option<pb::OutboxEntry>, RepositoryError> {
        if outbox_id.is_empty() {
            return Ok(None);
        }
        let row = self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT outbox_id, operation_id, topic, status, payload_snapshot_ref,
                            payload_digest, generation, attempt, lease_id, lease_expires_at,
                            next_attempt_at, dispatched_at, acknowledged_at, error_code, created_at
                       FROM manager_outbox WHERE outbox_id = ?1",
                    params![outbox_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i32>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, i64>(7)?,
                            row.get::<_, Option<String>>(8)?,
                            row.get::<_, Option<i64>>(9)?,
                            row.get::<_, Option<i64>>(10)?,
                            row.get::<_, Option<i64>>(11)?,
                            row.get::<_, Option<i64>>(12)?,
                            row.get::<_, i32>(13)?,
                            row.get::<_, i64>(14)?,
                        ))
                    },
                )
                .optional()
                .map_err(ManagerStorageError::from)
        })?;
        row.map(
            |(
                outbox_id,
                operation_id,
                topic,
                status,
                payload_snapshot_ref,
                payload_digest,
                generation,
                attempt,
                lease_id,
                lease_expires_at,
                next_attempt_at,
                dispatched_at,
                acknowledged_at,
                error_code,
                created_at,
            )| {
                Ok(pb::OutboxEntry {
                    outbox_id,
                    operation_id,
                    topic,
                    status,
                    payload_snapshot_ref,
                    payload_digest,
                    generation: generation
                        .try_into()
                        .map_err(|_| RepositoryError::RevisionOverflow)?,
                    attempt: attempt
                        .try_into()
                        .map_err(|_| RepositoryError::RevisionOverflow)?,
                    lease_id,
                    lease_expires_at: lease_expires_at.map(utc_instant),
                    next_attempt_at: next_attempt_at.map(utc_instant),
                    dispatched_at: dispatched_at.map(utc_instant),
                    acknowledged_at: acknowledged_at.map(utc_instant),
                    error_code,
                    created_at: Some(utc_instant(created_at)),
                })
            },
        )
        .transpose()
    }

    pub(crate) fn update_operation(
        &mut self,
        operation: &pb::DurableOperation,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.operation_id.is_empty() {
            return Err(RepositoryError::EmptyOperationId);
        }
        let operation = sanitize_operation(operation)?;
        let operation_payload = operation.encode_to_vec();
        let operation_payload_digest = digest_bytes(&operation_payload);
        let terminal = operation.terminal_snapshot.as_ref();
        if let Some(snapshot) = terminal {
            let snapshot = match snapshot {
                pb::durable_operation::TerminalSnapshot::ResultSnapshot(value)
                | pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value) => value,
            };
            validate_snapshot(snapshot)?;
        }
        let (result_snapshot, error_snapshot) = match terminal {
            Some(pb::durable_operation::TerminalSnapshot::ResultSnapshot(value)) => {
                (Some(value), None)
            }
            Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value)) => {
                (None, Some(value))
            }
            None => (None, None),
        };
        let (_, commit) =
            self.storage
                .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                    let updated = transaction.execute(
                        "UPDATE manager_operation SET method = ?2, idempotency_scope = ?3,
                         request_digest = ?4, state = ?5, attempt = ?6,
                         manager_sequence = ?7, generation = ?8, causal_parent = ?9,
                         sync_subject = ?10, affects_ready = ?11,
                         result_snapshot = ?12, error_snapshot = ?13,
                         updated_at = ?14, result_snapshot_schema = ?15,
                         result_snapshot_digest = ?16, error_snapshot_schema = ?17,
                         error_snapshot_digest = ?18, operation_payload = ?19,
                         operation_payload_schema = ?20, operation_payload_digest = ?21
                       WHERE operation_id = ?1",
                        params![
                            operation.operation_id,
                            operation.method,
                            operation.idempotency_scope,
                            operation.request_digest,
                            operation.state,
                            i64::from(operation.attempt),
                            i64::try_from(stamp.manager_sequence)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                            i64::try_from(stamp.generation)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                            operation.causal_parent,
                            operation.sync_subject,
                            i64::from(operation.affects_ready),
                            result_snapshot.map(|value| value.payload.as_slice()),
                            error_snapshot.map(|value| value.payload.as_slice()),
                            operation_updated_at_millis(&operation, now_millis()),
                            result_snapshot.map(|value| value.schema.as_str()),
                            result_snapshot.map(|value| digest_bytes(&value.payload)),
                            error_snapshot.map(|value| value.schema.as_str()),
                            error_snapshot.map(|value| digest_bytes(&value.payload)),
                            operation_payload,
                            "issue_tracker.manager.v1.DurableOperation",
                            operation_payload_digest,
                        ],
                    )?;
                    if updated != 1 {
                        return Err(RepositoryError::OperationConflict);
                    }
                    upsert_operation_history_projection(transaction, &operation, now_millis())?;
                    Ok(())
                })?;
        Ok(commit)
    }

    /// Finalize a directly dispatched administrative operation and its
    /// northbound replay receipt together.  Unlike normal backend dispatch,
    /// an administrative action has no public outbox entry, so splitting
    /// these writes would make a crash leave a terminal operation whose
    /// idempotency key can only report PENDING.
    pub(crate) fn finalize_admin_action(
        &mut self,
        operation: &pb::DurableOperation,
        response: &[u8],
    ) -> Result<ManagerCommit, RepositoryError> {
        let operation = sanitize_operation(operation)?;
        if operation.method != "manager_apply_issue_admin_action"
            || operation.idempotency_scope.is_empty()
            || operation.request_digest.is_empty()
        {
            return Err(RepositoryError::OperationConflict);
        }
        let terminal = operation
            .terminal_snapshot
            .as_ref()
            .ok_or(RepositoryError::OperationConflict)?;
        let (result, error) = match terminal {
            pb::durable_operation::TerminalSnapshot::ResultSnapshot(value) => (Some(value), None),
            pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value) => (None, Some(value)),
        };
        let snapshot = match terminal {
            pb::durable_operation::TerminalSnapshot::ResultSnapshot(value)
            | pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value) => value,
        };
        validate_snapshot(snapshot)?;
        let payload = operation.encode_to_vec();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let current_payload: Vec<u8> = transaction.query_row(
                    "SELECT operation_payload FROM manager_operation
                       WHERE operation_id=?1",
                    params![operation.operation_id],
                    |row| row.get(0),
                )?;
                let current = pb::DurableOperation::decode(current_payload.as_slice())
                    .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                let generation_matches = current.generation == operation.generation
                    || current.generation.checked_add(1) == Some(operation.generation);
                if current.operation_id != operation.operation_id
                    || !generation_matches
                    || matches!(
                        pb::DurableOperationState::try_from(current.state),
                        Ok(pb::DurableOperationState::Committed
                            | pb::DurableOperationState::Failed
                            | pb::DurableOperationState::ManualResolution)
                    )
                {
                    return Err(RepositoryError::OperationConflict);
                }
                let current_payload_digest = digest_bytes(&current_payload);
                let updated = transaction.execute(
                    "UPDATE manager_operation SET state=?2, generation=?3, manager_sequence=?4,
                     affects_ready=?5, result_snapshot=?6, error_snapshot=?7,
                     result_snapshot_schema=?8, result_snapshot_digest=?9,
                     error_snapshot_schema=?10, error_snapshot_digest=?11,
                     operation_payload=?12, operation_payload_schema=?13,
                     operation_payload_digest=?14, updated_at=?15
                       WHERE operation_id=?1 AND operation_payload_digest=?16",
                    params![
                        operation.operation_id,
                        operation.state,
                        i64::try_from(operation.generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(stamp.manager_sequence)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::from(operation.affects_ready),
                        result.map(|value| value.payload.as_slice()),
                        error.map(|value| value.payload.as_slice()),
                        result.map(|value| value.schema.as_str()),
                        result.map(|value| digest_bytes(&value.payload)),
                        error.map(|value| value.schema.as_str()),
                        error.map(|value| digest_bytes(&value.payload)),
                        payload,
                        "issue_tracker.manager.v1.DurableOperation",
                        digest_bytes(&operation.encode_to_vec()),
                        operation_updated_at_millis(&operation, now_millis()),
                        current_payload_digest,
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                upsert_operation_history_projection(transaction, &operation, now_millis())?;
                let updated = transaction.execute(
                    "UPDATE manager_idempotency SET state=?2, result_snapshot=?3, updated_at=?4
                       WHERE idempotency_scope=?1 AND request_digest=?5 AND operation_id=?6",
                    params![
                        operation.idempotency_scope,
                        operation.state,
                        response,
                        now_millis(),
                        operation.request_digest,
                        operation.operation_id,
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                // Legacy direct recovery may finalize a row that already has
                // the private admin outbox entry.  Close that entry in the
                // same transaction so the generic dispatcher cannot replay a
                // mutation after its immutable receipt was published.
                transaction.execute(
                    "UPDATE manager_outbox
                        SET status=?2, acknowledged_at=?3, lease_id=NULL,
                            lease_expires_at=NULL
                      WHERE operation_id=?1
                        AND status NOT IN (?4, ?5)",
                    params![
                        operation.operation_id,
                        if operation.state == pb::DurableOperationState::Committed as i32 {
                            pb::OutboxStatus::Acknowledged as i32
                        } else {
                            pb::OutboxStatus::DeadLetter as i32
                        },
                        now_millis(),
                        pb::OutboxStatus::Acknowledged as i32,
                        pb::OutboxStatus::DeadLetter as i32,
                    ],
                )?;
                let evidence_classification = match operation.state {
                    state if state == pb::DurableOperationState::Committed as i32 => "COMMITTED",
                    state if state == pb::DurableOperationState::Failed as i32 => {
                        "REJECTED_BEFORE_MUTATION"
                    }
                    _ => "MARKER_UNCONFIRMED",
                };
                let _attempt_changed = transaction.execute(
                    "UPDATE admin_dispatch_attempt
                        SET state=?2, resolved_at=?3,
                            evidence_classification=?4
                      WHERE operation_id=?1
                        AND attempt_generation <= ?5",
                    params![
                        operation.operation_id,
                        if operation.state == pb::DurableOperationState::Committed as i32 {
                            pb::OutboxStatus::Acknowledged as i32
                        } else {
                            pb::OutboxStatus::DeadLetter as i32
                        },
                        now_millis(),
                        evidence_classification,
                        i64::try_from(operation.generation).unwrap_or(i64::MAX),
                    ],
                )?;
                let (identity_contract, identity_root) = manager_root_identity(transaction)?;
                if operation.state == pb::DurableOperationState::Committed as i32
                    && let Some(issue_id) = operation.scope.as_ref().and_then(|scope| match scope {
                        pb::durable_operation::Scope::IssueId(issue_id) => Some(issue_id.as_str()),
                        _ => None,
                    })
                {
                    if let Some(readmission_number) = transaction
                        .query_row(
                            "SELECT readmission_number FROM manager_ownership_recovery
                              WHERE contract_fingerprint=?1 AND data_root_id=?2
                                AND issue_id=?3",
                            params![identity_contract.as_str(), identity_root.as_str(), issue_id],
                            |row| row.get::<_, i64>(0),
                        )
                        .optional()?
                    {
                        transaction.execute(
                            "INSERT INTO manager_ownership_recovery_projection
                                (contract_fingerprint, data_root_id, issue_id,
                                 readmission_number, operation_id, source_generation,
                                 terminal_state, projected_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                             ON CONFLICT(contract_fingerprint, data_root_id, issue_id,
                                         readmission_number, operation_id)
                             DO UPDATE SET source_generation=excluded.source_generation,
                                 terminal_state=excluded.terminal_state,
                                 projected_at=excluded.projected_at
                              WHERE excluded.source_generation >=
                                    manager_ownership_recovery_projection.source_generation",
                            params![
                                identity_contract.as_str(),
                                identity_root.as_str(),
                                issue_id,
                                readmission_number,
                                operation.operation_id,
                                i64::try_from(operation.generation)
                                    .map_err(|_| RepositoryError::RevisionOverflow)?,
                                operation.state,
                                now_millis(),
                            ],
                        )?;
                    }
                }
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Finalize an administrative operation delivered through the private
    /// backend outbox.  The southbound response is already a typed Manager
    /// result, while the Tracker marker has been verified by the dispatcher;
    /// this method attaches the durable operation to that exact mutation and
    /// publishes the operation snapshot, replay receipt, and outbox status in
    /// one SQLite transaction.
    pub(crate) fn finalize_admin_backend_dispatch(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        response_payload: &[u8],
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.method != "manager_apply_issue_admin_action"
            || operation.operation_id != outbox.operation_id
        {
            return Err(RepositoryError::OutboxOperationMismatch);
        }
        // A not-sent attempt is returned with the original RECEIVED
        // operation and an empty response.  The outbox state has already been
        // moved back to READY with its persistent backoff; only that state and
        // the attempt evidence need to be committed here.  Do not turn a
        // retryable transport failure into a terminal administrative error.
        if response_payload.is_empty()
            && operation.state == pb::DurableOperationState::Received as i32
        {
            let outbox = outbox.clone();
            let (_, commit) =
                self.storage
                    .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                        update_outbox_in_transaction(transaction, &outbox)?;
                        let changed = transaction.execute(
                            "UPDATE admin_dispatch_attempt
                            SET state=?2, lease_id=NULL, lease_expires_at=NULL,
                                evidence_classification=?3
                          WHERE operation_id=?1 AND attempt_generation=?4",
                            params![
                                operation.operation_id,
                                outbox.status,
                                "NOT_SENT_RETRY",
                                i64::try_from(operation.generation)
                                    .map_err(|_| RepositoryError::RevisionOverflow)?,
                            ],
                        )?;
                        if changed != 1 {
                            return Err(RepositoryError::OperationConflict);
                        }
                        Ok(())
                    })?;
            return Ok(commit);
        }
        // A delivery timeout or missing marker is deliberately retained as
        // UNCONFIRMED.  It is not a terminal failure and must remain eligible
        // for marker reconciliation/manual resolution; publishing the
        // generic ResultUnconfirmed error as FAILED here would incorrectly
        // clear the readiness fence and make a later retry unsafe.
        if response_payload.is_empty()
            && operation.state == pb::DurableOperationState::Unconfirmed as i32
        {
            return self.finalize_admin_unconfirmed(operation, outbox);
        }
        let mut operation = operation.clone();
        // `dispatch_leased` can obtain a southbound response while the
        // server-owned marker fails verification.  In that case the
        // response payload is deliberately not evidence for this admission:
        // it may describe a different operation which happened to use the
        // same backend.  Preserve the MANUAL_RESOLUTION fence and publish a
        // typed error instead of accidentally committing the returned admin
        // mutation.
        if operation.state != pb::DurableOperationState::Committed as i32 {
            let state = operation.state;
            let error = operation
                .terminal_snapshot
                .as_ref()
                .and_then(|snapshot| match snapshot {
                    pb::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot)
                        if snapshot.schema == "issue_tracker.manager.v1.ManagerError" =>
                    {
                        pb::ManagerError::decode(snapshot.payload.as_slice()).ok()
                    }
                    _ => None,
                })
                .unwrap_or_else(|| pb::ManagerError {
                    code: if state == pb::DurableOperationState::ManualResolution as i32 {
                        pb::ManagerErrorCode::ResolutionRequired as i32
                    } else {
                        pb::ManagerErrorCode::ResultUnconfirmed as i32
                    },
                    message: if state == pb::DurableOperationState::ManualResolution as i32 {
                        "administrative marker requires manual resolution".into()
                    } else {
                        "administrative outcome is not confirmed".into()
                    },
                    retryable: state != pb::DurableOperationState::ManualResolution as i32,
                    detail: None,
                });
            operation.terminal_snapshot = Some(
                pb::durable_operation::TerminalSnapshot::ErrorSnapshot(pb::Snapshot {
                    schema: "issue_tracker.manager.v1.ManagerError".into(),
                    payload: error.encode_to_vec(),
                }),
            );
            operation.affects_ready = state == pb::DurableOperationState::ManualResolution as i32;
            let response = pb::ApplyIssueAdminActionResult {
                outcome: Some(pb::apply_issue_admin_action_result::Outcome::Error(error)),
            };
            return self.finalize_admin_action(&operation, &response.encode_to_vec());
        }
        let response = if !response_payload.is_empty() {
            pb::ApplyIssueAdminActionResult::decode(response_payload).map_err(|error| {
                RepositoryError::Serialization(format!(
                    "administrative dispatch response decode: {error}"
                ))
            })?
        } else {
            let error = operation
                .terminal_snapshot
                .as_ref()
                .and_then(|snapshot| match snapshot {
                    pb::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot)
                        if snapshot.schema == "issue_tracker.manager.v1.ManagerError" =>
                    {
                        pb::ManagerError::decode(snapshot.payload.as_slice()).ok()
                    }
                    _ => None,
                })
                .unwrap_or(pb::ManagerError {
                    code: pb::ManagerErrorCode::ResultUnconfirmed as i32,
                    message: "administrative Tracker outcome is unconfirmed".into(),
                    retryable: true,
                    detail: None,
                });
            pb::ApplyIssueAdminActionResult {
                outcome: Some(pb::apply_issue_admin_action_result::Outcome::Error(error)),
            }
        };

        match response.outcome.as_ref() {
            Some(pb::apply_issue_admin_action_result::Outcome::Success(mutation)) => {
                let Some(pb::manager_mutation::Result::IssueAdminAction(receipt)) =
                    mutation.result.as_ref()
                else {
                    return Err(RepositoryError::Serialization(
                        "administrative success has no typed receipt".into(),
                    ));
                };
                if receipt.issue_id.is_empty()
                    || receipt.audit_operation_id != operation.downstream_operation_id
                {
                    return Err(RepositoryError::Serialization(
                        "administrative receipt identity is invalid".into(),
                    ));
                }
                operation.state = pb::DurableOperationState::Committed as i32;
                operation.affects_ready = receipt.changed;
                operation.terminal_snapshot = Some(
                    pb::durable_operation::TerminalSnapshot::ResultSnapshot(pb::Snapshot {
                        schema: "issue_tracker.manager.v1.ApplyIssueAdminActionResult".into(),
                        payload: {
                            let mut mutation = mutation.clone();
                            mutation.operation = Some(operation.clone());
                            pb::ApplyIssueAdminActionResult {
                                outcome: Some(
                                    pb::apply_issue_admin_action_result::Outcome::Success(mutation),
                                ),
                            }
                            .encode_to_vec()
                        },
                    }),
                );
                let terminal = operation
                    .terminal_snapshot
                    .as_ref()
                    .and_then(|snapshot| match snapshot {
                        pb::durable_operation::TerminalSnapshot::ResultSnapshot(value) => {
                            Some(value.payload.as_slice())
                        }
                        _ => None,
                    })
                    .ok_or(RepositoryError::OperationConflict)?;
                self.finalize_admin_action(&operation, terminal)
            }
            Some(pb::apply_issue_admin_action_result::Outcome::Error(error)) => {
                operation.state =
                    if operation.state == pb::DurableOperationState::ManualResolution as i32 {
                        pb::DurableOperationState::ManualResolution as i32
                    } else {
                        pb::DurableOperationState::Failed as i32
                    };
                operation.affects_ready =
                    operation.state == pb::DurableOperationState::ManualResolution as i32;
                operation.terminal_snapshot = Some(
                    pb::durable_operation::TerminalSnapshot::ErrorSnapshot(pb::Snapshot {
                        schema: "issue_tracker.manager.v1.ManagerError".into(),
                        payload: error.encode_to_vec(),
                    }),
                );
                let response = pb::ApplyIssueAdminActionResult {
                    outcome: Some(pb::apply_issue_admin_action_result::Outcome::Error(
                        error.clone(),
                    )),
                };
                self.finalize_admin_action(&operation, &response.encode_to_vec())
            }
            None => Err(RepositoryError::Serialization(
                "administrative dispatch response has no outcome".into(),
            )),
        }
    }

    /// Persist an acknowledged-but-unconfirmed administrative delivery
    /// without manufacturing a terminal error/result.  The outbox is closed
    /// (so it cannot be redelivered), while the operation remains fenced until
    /// a marker is found or a human resolves it.
    fn finalize_admin_unconfirmed(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
    ) -> Result<ManagerCommit, RepositoryError> {
        let operation = sanitize_operation(operation)?;
        let payload = operation.encode_to_vec();
        let (_, commit) =
            self.storage
                .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                    let current_payload: Vec<u8> = transaction.query_row(
                        "SELECT operation_payload FROM manager_operation
                           WHERE operation_id=?1",
                        params![operation.operation_id],
                        |row| row.get(0),
                    )?;
                    let current = pb::DurableOperation::decode(current_payload.as_slice())
                        .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                    let generation_matches = current.generation == operation.generation
                        || current.generation.checked_add(1) == Some(operation.generation);
                    if current.operation_id != operation.operation_id
                        || !generation_matches
                        || matches!(
                            pb::DurableOperationState::try_from(current.state),
                            Ok(pb::DurableOperationState::Committed
                                | pb::DurableOperationState::Failed
                                | pb::DurableOperationState::ManualResolution)
                        )
                    {
                        return Err(RepositoryError::OperationConflict);
                    }
                    let current_payload_digest = digest_bytes(&current_payload);
                    let changed = transaction.execute(
                        "UPDATE manager_operation SET state=?2, attempt=?3,
                         manager_sequence=?4, generation=?5, affects_ready=?6,
                         result_snapshot=NULL, error_snapshot=NULL,
                         result_snapshot_schema=NULL, result_snapshot_digest=NULL,
                         error_snapshot_schema=NULL, error_snapshot_digest=NULL,
                         operation_payload=?7, operation_payload_schema=?8,
                         operation_payload_digest=?9, updated_at=?10
                       WHERE operation_id=?1 AND operation_payload_digest=?11",
                        params![
                            operation.operation_id,
                            operation.state,
                            i64::from(operation.attempt),
                            i64::try_from(stamp.manager_sequence)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                            i64::try_from(operation.generation)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                            i64::from(operation.affects_ready),
                            payload,
                            "issue_tracker.manager.v1.DurableOperation",
                            digest_bytes(&operation.encode_to_vec()),
                            operation_updated_at_millis(&operation, now_millis()),
                            current_payload_digest,
                        ],
                    )?;
                    if changed != 1 {
                        return Err(RepositoryError::OperationConflict);
                    }
                    upsert_operation_history_projection(transaction, &operation, now_millis())?;
                    update_outbox_in_transaction(transaction, outbox)?;
                    let (contract, root) = manager_root_identity(transaction)?;
                    let attempt_changed = transaction.execute(
                        "UPDATE admin_dispatch_attempt
                        SET state=?2, resolved_at=?3,
                            evidence_classification=?4
                      WHERE contract_fingerprint=?5 AND data_root_id=?6
                        AND operation_id=?1 AND attempt_generation <= ?7",
                        params![
                            operation.operation_id,
                            pb::OutboxStatus::Acknowledged as i32,
                            now_millis(),
                            "SENT_UNKNOWN",
                            contract,
                            root,
                            i64::try_from(operation.generation)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                        ],
                    )?;
                    if attempt_changed == 0 {
                        return Err(RepositoryError::OperationConflict);
                    }
                    let changed = transaction.execute(
                        "UPDATE manager_idempotency
                        SET state=?2, result_snapshot=NULL, updated_at=?3
                      WHERE operation_id=?1 AND result_snapshot IS NULL",
                        params![operation.operation_id, operation.state, now_millis()],
                    )?;
                    if changed != 1 {
                        return Err(RepositoryError::OperationConflict);
                    }
                    Ok(())
                })?;
        Ok(commit)
    }

    /// Atomically publishes a manual Issue-operation resolution together with
    /// the source operation it settles.  A crash must not leave the source
    /// terminal while its resolution receipt is still pending.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_issue_operation_resolution(
        &mut self,
        source: &pb::DurableOperation,
        resolution: &pb::DurableOperation,
        source_result: Option<&[u8]>,
        resolution_result: &[u8],
        resolution_request: &[u8],
        decision: i32,
        resolution_mode: i32,
        arbitration_outcome: i32,
        expected_issue_revision: u64,
    ) -> Result<ManagerCommit, RepositoryError> {
        let source = sanitize_operation(source)?;
        let resolution = sanitize_operation(resolution)?;
        let resolution_mutation = sanitize_mutation(&pb::ManagerMutation {
            operation: Some(resolution.clone()),
            ..Default::default()
        })
        .encode_to_vec();
        let resolution_mutation_digest = digest_bytes(&resolution_mutation);
        let update = |transaction: &Transaction<'_>,
                      operation: &pb::DurableOperation,
                      manager_sequence: u64,
                      expected_payload_digest: &str|
         -> Result<(), RepositoryError> {
            let payload = operation.encode_to_vec();
            let terminal = operation.terminal_snapshot.as_ref();
            let (result, error) = match terminal {
                Some(pb::durable_operation::TerminalSnapshot::ResultSnapshot(snapshot)) => {
                    (Some(snapshot), None)
                }
                Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot)) => {
                    (None, Some(snapshot))
                }
                None => (None, None),
            };
            let changed = transaction.execute(
                "UPDATE manager_operation SET state=?2, generation=?3, manager_sequence=?4,
                 affects_ready=?5, result_snapshot=?6, error_snapshot=?7,
                 result_snapshot_schema=?8, result_snapshot_digest=?9,
                 error_snapshot_schema=?10, error_snapshot_digest=?11,
                 operation_payload=?12, operation_payload_schema=?13,
                 operation_payload_digest=?14, updated_at=?15
               WHERE operation_id=?1 AND operation_payload_digest=?16",
                params![
                    operation.operation_id,
                    operation.state,
                    i64::try_from(operation.generation)
                        .map_err(|_| RepositoryError::RevisionOverflow)?,
                    i64::try_from(manager_sequence)
                        .map_err(|_| RepositoryError::RevisionOverflow)?,
                    i64::from(operation.affects_ready),
                    result.map(|value| value.payload.as_slice()),
                    error.map(|value| value.payload.as_slice()),
                    result.map(|value| value.schema.as_str()),
                    result.map(|value| digest_bytes(&value.payload)),
                    error.map(|value| value.schema.as_str()),
                    error.map(|value| digest_bytes(&value.payload)),
                    payload,
                    "issue_tracker.manager.v1.DurableOperation",
                    digest_bytes(&operation.encode_to_vec()),
                    operation_updated_at_millis(operation, now_millis()),
                    expected_payload_digest,
                ],
            )?;
            if changed != 1 {
                return Err(RepositoryError::OperationConflict);
            }
            upsert_operation_history_projection(transaction, operation, now_millis())?;
            Ok(())
        };
        let (_, commit) =
            self.storage
                .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                    // Both operations are finalized by one Manager commit. Keep the
                    // denormalized sequence in their protobuf payloads and indexed
                    // columns identical so GetOperation and history projections never
                    // observe a mixed generation after a crash.
                    let mut source = source.clone();
                    let mut resolution = resolution.clone();
                    source.manager_sequence = stamp.manager_sequence;
                    resolution.manager_sequence = stamp.manager_sequence;
                    let source_current_payload: Vec<u8> = transaction.query_row(
                        "SELECT operation_payload FROM manager_operation
                   WHERE operation_id=?1",
                        params![source.operation_id],
                        |row| row.get(0),
                    )?;
                    let source_current =
                        pb::DurableOperation::decode(source_current_payload.as_slice())
                            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                    let source_generation_matches = source_current.generation == source.generation
                        || source_current.generation.checked_add(1) == Some(source.generation);
                    let source_state_resolvable = matches!(
                        pb::DurableOperationState::try_from(source_current.state),
                        Ok(pb::DurableOperationState::Failed
                            | pb::DurableOperationState::Unconfirmed
                            | pb::DurableOperationState::ManualResolution)
                    ) || (source.state
                        == pb::DurableOperationState::Arbitrated as i32
                        && matches!(
                            pb::DurableOperationState::try_from(source_current.state),
                            Ok(pb::DurableOperationState::Received
                                | pb::DurableOperationState::Pending)
                        ));
                    if source_current.operation_id != source.operation_id
                        || !source_generation_matches
                        || !source_state_resolvable
                    {
                        return Err(RepositoryError::OperationConflict);
                    }
                    let source_current_payload_digest = digest_bytes(&source_current_payload);
                    // Newer callers admit the resolution before touching the source.
                    // Keep an existence check here for stores created by the previous
                    // implementation so finalisation remains restart-safe without
                    // replacing an immutable operation or idempotency reservation.
                    let admitted: bool = transaction.query_row(
                        "SELECT EXISTS(SELECT 1 FROM manager_operation WHERE operation_id=?1)",
                        params![resolution.operation_id],
                        |row| row.get(0),
                    )?;
                    let resolution_current_payload_digest = if admitted {
                        let payload: Vec<u8> = transaction.query_row(
                            "SELECT operation_payload FROM manager_operation
                       WHERE operation_id=?1",
                            params![resolution.operation_id],
                            |row| row.get(0),
                        )?;
                        let current = pb::DurableOperation::decode(payload.as_slice())
                            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                        let generation_matches = current.generation == resolution.generation
                            || current.generation.checked_add(1) == Some(resolution.generation);
                        if current.operation_id != resolution.operation_id
                            || !generation_matches
                            || matches!(
                                pb::DurableOperationState::try_from(current.state),
                                Ok(pb::DurableOperationState::Committed
                                    | pb::DurableOperationState::Failed)
                            )
                        {
                            return Err(RepositoryError::OperationConflict);
                        }
                        digest_bytes(&payload)
                    } else {
                        digest_bytes(&resolution.encode_to_vec())
                    };
                    if !admitted {
                        insert_operation(
                            transaction,
                            &resolution,
                            &resolution_mutation,
                            &resolution_mutation_digest,
                            stamp,
                        )?;
                    }
                    insert_idempotency(transaction, &resolution)?;
                    let (contract, root) = manager_root_identity(transaction)?;
                    let fence_token = digest_bytes(
                        format!(
                            "{}:{}:{}",
                            source.operation_id, resolution.operation_id, resolution.request_digest
                        )
                        .as_bytes(),
                    );
                    let now = now_millis();
                    let fence_updated = transaction.execute(
                        "UPDATE resolution_fence SET state=?4, updated_at=?5
                  WHERE contract_fingerprint=?1 AND data_root_id=?2
                    AND source_operation_id=?3
                    AND resolution_operation_id=?6",
                        params![
                            contract,
                            root,
                            source.operation_id,
                            resolution.state,
                            now,
                            resolution.operation_id,
                        ],
                    )?;
                    if fence_updated == 0 {
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
                                source.operation_id,
                                resolution.operation_id,
                                fence_token,
                                i64::try_from(source.generation)
                                    .map_err(|_| RepositoryError::RevisionOverflow)?,
                                i64::try_from(expected_issue_revision)
                                    .map_err(|_| RepositoryError::RevisionOverflow)?,
                                decision,
                                resolution_request,
                                resolution.request_digest,
                                resolution.state,
                                now,
                            ],
                        )?;
                    }
                    let source_result_was_supplied = source_result.is_some();
                    let source_result = if let Some(source_result) = source_result {
                        source_result.to_vec()
                    } else if source.state == pb::DurableOperationState::Committed as i32 {
                        let mutation = Self::confirmed_backend_mutation(transaction, &source)?;
                        source.terminal_snapshot = Some(
                            pb::durable_operation::TerminalSnapshot::ResultSnapshot(pb::Snapshot {
                                payload: sanitize_mutation(&mutation).encode_to_vec(),
                                schema: MANAGER_MUTATION_SNAPSHOT_SCHEMA.into(),
                            }),
                        );
                        let mut wrapped = mutation;
                        wrapped.operation = Some(source.clone());
                        super::super::gateway::mutation_wrapper_for_method(&source.method)
                            .ok_or_else(|| {
                                RepositoryError::RequestSnapshot(format!(
                                    "unknown source operation method: {}",
                                    source.method
                                ))
                            })?(Ok(wrapped))
                    } else if let Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(
                        error,
                    )) = source.terminal_snapshot.as_ref()
                    {
                        let error = pb::ManagerError::decode(error.payload.as_slice())
                            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                        super::super::gateway::mutation_wrapper_for_method(&source.method)
                            .ok_or_else(|| {
                                RepositoryError::RequestSnapshot(format!(
                                    "unknown source operation method: {}",
                                    source.method
                                ))
                            })?(Err(error))
                    } else {
                        return Err(RepositoryError::OperationConflict);
                    };
                    let source_replay_payload: Option<Vec<u8>> = transaction
                        .query_row(
                            "SELECT result_snapshot FROM manager_idempotency
                               WHERE operation_id=?1 AND request_digest=?2",
                            params![source.operation_id, source.request_digest],
                            |row| row.get::<_, Option<Vec<u8>>>(0),
                        )
                        .optional()?
                        .flatten();
                    let source_replay_schema = source_replay_payload
                        .as_ref()
                        .map(|_| {
                            super::super::gateway::mutation_result_schema_for_method(&source.method)
                                .ok_or_else(|| {
                                    RepositoryError::RequestSnapshot(format!(
                                        "unknown source operation method: {}",
                                        source.method
                                    ))
                                })
                        })
                        .transpose()?
                        .map(str::to_owned);
                    let audit_created_at = now_millis();
                    let audit_inserted = transaction.execute(
                        "INSERT INTO manager_issue_operation_resolution_audit
                            (contract_fingerprint, data_root_id,
                             resolution_operation_id, source_operation_id,
                             source_generation, source_state,
                             source_operation_payload, source_operation_payload_schema,
                             source_operation_payload_digest,
                             source_replay_payload, source_replay_schema,
                             source_replay_digest, resolution_result_payload,
                             resolution_result_schema, resolution_result_digest,
                             created_at)
                         VALUES (
                             (SELECT value FROM manager_metadata WHERE key='contract_fingerprint'),
                             (SELECT value FROM manager_metadata WHERE key='data_root_id'),
                             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                         ON CONFLICT(contract_fingerprint, data_root_id,
                                     resolution_operation_id) DO NOTHING",
                        params![
                            resolution.operation_id,
                            source_current.operation_id,
                            i64::try_from(source_current.generation)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                            source_current.state,
                            source_current_payload.as_slice(),
                            "issue_tracker.manager.v1.DurableOperation",
                            source_current_payload_digest,
                            source_replay_payload.as_deref(),
                            source_replay_schema.as_deref(),
                            source_replay_payload
                                .as_ref()
                                .map(|payload| digest_bytes(payload)),
                            resolution_result,
                            "issue_tracker.manager.v1.ResolveIssueOperationResult",
                            digest_bytes(resolution_result),
                            audit_created_at,
                        ],
                    )?;
                    if audit_inserted == 0 {
                        let existing: (
                            String,
                            String,
                            i64,
                            i32,
                            Vec<u8>,
                            String,
                            String,
                            Option<Vec<u8>>,
                            Option<String>,
                            Option<String>,
                            Vec<u8>,
                            String,
                            String,
                        ) = transaction.query_row(
                            "SELECT source_operation_id, resolution_operation_id,
                                    source_generation, source_state,
                                    source_operation_payload,
                                    source_operation_payload_schema,
                                    source_operation_payload_digest,
                                    source_replay_payload, source_replay_schema,
                                    source_replay_digest, resolution_result_payload,
                                    resolution_result_schema, resolution_result_digest
                               FROM manager_issue_operation_resolution_audit
                              WHERE resolution_operation_id=?1",
                            params![resolution.operation_id],
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
                                ))
                            },
                        )?;
                        let existing_matches = existing.0 == source_current.operation_id
                            && existing.1 == resolution.operation_id
                            && existing.2
                                == i64::try_from(source_current.generation)
                                    .map_err(|_| RepositoryError::RevisionOverflow)?
                            && existing.3 == source_current.state
                            && existing.4 == source_current_payload
                            && existing.5 == "issue_tracker.manager.v1.DurableOperation"
                            && existing.6 == source_current_payload_digest
                            && existing.7 == source_replay_payload
                            && existing.8 == source_replay_schema
                            && existing.9
                                == source_replay_payload
                                    .as_ref()
                                    .map(|payload| digest_bytes(payload))
                            && existing.10 == resolution_result
                            && existing.11
                                == "issue_tracker.manager.v1.ResolveIssueOperationResult"
                            && existing.12 == digest_bytes(resolution_result);
                        if !existing_matches {
                            return Err(RepositoryError::OperationConflict);
                        }
                    }
                    update(
                        transaction,
                        &source,
                        stamp.manager_sequence,
                        &source_current_payload_digest,
                    )?;
                    update(
                        transaction,
                        &resolution,
                        stamp.manager_sequence,
                        &resolution_current_payload_digest,
                    )?;
                    // A source operation may itself be the subject of an
                    // earlier resolution request.  Close every such pending
                    // request in this same transaction so recovery cannot
                    // replay an operation that has already been arbitrated.
                    let pending_resolution_ids: Vec<String> = {
                        let mut statement = transaction.prepare(
                            "SELECT resolution_operation_id
                               FROM manager_resolution_request
                              WHERE state='PENDING'
                                AND (source_operation_id=?1
                                     OR resolution_operation_id=?1)",
                        )?;
                        statement
                            .query_map(params![source.operation_id.as_str()], |row| row.get(0))?
                            .collect::<Result<Vec<String>, _>>()?
                    };
                    for pending_id in pending_resolution_ids {
                        if pending_id == resolution.operation_id {
                            continue;
                        }
                        // The request row and its source fence are a pair. A
                        // superseding arbitration must close both, otherwise
                        // a later resolution admission can be blocked by a
                        // stale non-terminal fence even though the request
                        // itself is no longer recoverable.
                        transaction.execute(
                            "UPDATE resolution_fence
                                SET state=?2, updated_at=?3
                              WHERE resolution_operation_id=?1",
                            params![
                                pending_id.as_str(),
                                pb::DurableOperationState::Arbitrated as i32,
                                now_millis(),
                            ],
                        )?;
                        if pending_id == source.operation_id {
                            transaction.execute(
                                "UPDATE manager_resolution_request
                                    SET state='ARBITRATED', updated_at=?2
                                  WHERE resolution_operation_id=?1 AND state='PENDING'",
                                params![pending_id, now_millis()],
                            )?;
                            continue;
                        }
                        let Some(pending_payload) = transaction
                            .query_row(
                                "SELECT operation_payload FROM manager_operation
                                  WHERE operation_id=?1",
                                params![pending_id.as_str()],
                                |row| row.get::<_, Vec<u8>>(0),
                            )
                            .optional()?
                        else {
                            transaction.execute(
                                "UPDATE manager_resolution_request
                                    SET state='ARBITRATED', updated_at=?2
                                  WHERE resolution_operation_id=?1 AND state='PENDING'",
                                params![pending_id, now_millis()],
                            )?;
                            continue;
                        };
                        let mut pending_operation = pb::DurableOperation::decode(
                            pending_payload.as_slice(),
                        )
                        .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                        let pending_state =
                            pb::DurableOperationState::try_from(pending_operation.state)
                                .unwrap_or(pb::DurableOperationState::Unspecified);
                        if matches!(
                            pending_state,
                            pb::DurableOperationState::Committed
                                | pb::DurableOperationState::Failed
                                | pb::DurableOperationState::Arbitrated
                        ) {
                            transaction.execute(
                                "UPDATE manager_resolution_request
                                    SET state='ARBITRATED', updated_at=?2
                                  WHERE resolution_operation_id=?1 AND state='PENDING'",
                                params![pending_id, now_millis()],
                            )?;
                            continue;
                        }
                        let pending_payload_digest = digest_bytes(&pending_payload);
                        let pending_generation = pending_operation.generation;
                        let arbitration_updated_at = now_millis();
                        pending_operation.state = pb::DurableOperationState::Arbitrated as i32;
                        pending_operation.failure_class = pb::FailureClass::Terminal as i32;
                        pending_operation.affects_ready = false;
                        pending_operation.resolution_required = false;
                        pending_operation.updated_at = Some(utc_instant(arbitration_updated_at));
                        pending_operation.generation = pending_generation
                            .checked_add(1)
                            .ok_or(RepositoryError::RevisionOverflow)?;
                        let mut pending_diagnosis =
                            source
                                .diagnosis
                                .clone()
                                .unwrap_or_else(|| pb::OperationDiagnosis {
                                    operation_id: pending_id.clone(),
                                    ..Default::default()
                                });
                        pending_diagnosis.operation_id = pending_id.clone();
                        pending_diagnosis.classification = "ARBITRATED_SUPERSEDED".into();
                        pending_diagnosis.resolution_state = "COMMITTED".into();
                        pending_diagnosis.resolution_operation_id =
                            Some(resolution.operation_id.clone());
                        pending_operation.diagnosis = Some(pending_diagnosis.clone());
                        let pending_error = pb::ManagerError {
                            code: pb::ManagerErrorCode::ResultUnconfirmed as i32,
                            message: "resolution request superseded by human arbitration".into(),
                            retryable: false,
                            detail: Some(pb::manager_error::Detail::OperationDiagnosis(
                                pending_diagnosis,
                            )),
                        };
                        pending_operation.terminal_snapshot = Some(
                            pb::durable_operation::TerminalSnapshot::ErrorSnapshot(pb::Snapshot {
                                payload: pending_error.encode_to_vec(),
                                schema: "issue_tracker.manager.v1.ManagerError".into(),
                            }),
                        );
                        update(
                            transaction,
                            &pending_operation,
                            stamp.manager_sequence,
                            &pending_payload_digest,
                        )?;
                        let pending_response = pb::ResolveIssueOperationResult {
                            outcome: Some(pb::resolve_issue_operation_result::Outcome::Error(
                                pending_error,
                            )),
                        }
                        .encode_to_vec();
                        let changed = transaction.execute(
                            "UPDATE manager_idempotency SET state=?2, result_snapshot=?3,
                                updated_at=?4
                              WHERE operation_id=?1 AND request_digest=?5
                                AND result_snapshot IS NULL",
                            params![
                                pending_id.as_str(),
                                pending_operation.state,
                                pending_response,
                                arbitration_updated_at,
                                pending_operation.request_digest.as_str(),
                            ],
                        )?;
                        if changed != 1 {
                            return Err(RepositoryError::OperationConflict);
                        }
                        let changed = transaction.execute(
                            "UPDATE manager_resolution_request
                                SET state='ARBITRATED', updated_at=?2
                              WHERE resolution_operation_id=?1 AND state='PENDING'",
                            params![pending_id.as_str(), arbitration_updated_at],
                        )?;
                        if changed != 1 {
                            return Err(RepositoryError::OperationConflict);
                        }
                    }
                    let projection_time = now_millis();
                    upsert_operation_history_projection(transaction, &source, projection_time)?;
                    upsert_operation_history_projection(transaction, &resolution, projection_time)?;
                    // A manual resolution may settle an ownership-maintenance
                    // admission before the maintenance scanner gets another
                    // turn.  Record the terminal projection and release the
                    // matching in-flight fence in this same transaction so
                    // a restart cannot leave the Issue permanently stopped.
                    if let Some(pb::durable_operation::Scope::IssueId(issue_id)) =
                        source.scope.as_ref()
                    {
                        transaction.execute(
                            "INSERT INTO manager_ownership_recovery_projection
                                (contract_fingerprint, data_root_id, issue_id,
                                 readmission_number, operation_id, source_generation,
                                 terminal_state, projected_at)
                         SELECT ?1, ?2, issue_id, readmission_number, ?3, ?4, ?5, ?6
                           FROM manager_ownership_recovery
                          WHERE contract_fingerprint=?1 AND data_root_id=?2
                            AND issue_id=?7 AND current_operation_id=?3
                            AND in_flight=1
                         ON CONFLICT(contract_fingerprint, data_root_id, issue_id,
                                     readmission_number, operation_id)
                         DO UPDATE SET source_generation=excluded.source_generation,
                                       terminal_state=excluded.terminal_state,
                                       projected_at=excluded.projected_at",
                            params![
                                contract,
                                root,
                                source.operation_id,
                                i64::try_from(source.generation)
                                    .map_err(|_| RepositoryError::RevisionOverflow)?,
                                source.state,
                                projection_time,
                                issue_id,
                            ],
                        )?;
                        if source.state == pb::DurableOperationState::Arbitrated as i32
                            || decision
                                == pb::IssueOperationResolutionDecision::ConfirmNotApplied as i32
                        {
                            transaction.execute(
                                "UPDATE manager_ownership_recovery
                                    SET readmission_number=readmission_number+1,
                                        current_operation_id=NULL, in_flight=0,
                                        stopped=0, stop_classification=NULL,
                                        stop_basis=NULL, repair_condition=NULL,
                                        next_attempt_at=CASE
                                          WHEN stop_classification='BACKEND_UNAVAILABLE'
                                          THEN next_attempt_at ELSE NULL END,
                                        updated_at=?4
                                  WHERE contract_fingerprint=?1 AND data_root_id=?2
                                    AND issue_id=?3 AND current_operation_id=?5
                                    AND in_flight=1",
                                params![
                                    contract,
                                    root,
                                    issue_id,
                                    projection_time,
                                    source.operation_id
                                ],
                            )?;
                        }
                    }
                    let arbitration_mode = resolution_mode
                        == pb::IssueOperationResolutionMode::HumanArbitration as i32;
                    let attempt_evidence_classification = if arbitration_mode {
                        match arbitration_outcome {
                            value
                                if value
                                    == pb::IssueOperationArbitrationOutcome::Applied as i32 =>
                            {
                                "ARBITRATION_APPLIED"
                            }
                            value
                                if value
                                    == pb::IssueOperationArbitrationOutcome::NotApplied as i32 =>
                            {
                                "ARBITRATION_NOT_APPLIED"
                            }
                            value
                                if value
                                    == pb::IssueOperationArbitrationOutcome::UnknownSettled
                                        as i32 =>
                            {
                                "ARBITRATION_UNKNOWN_SETTLED"
                            }
                            _ => "ARBITRATION_UNSPECIFIED",
                        }
                    } else if decision
                        == pb::IssueOperationResolutionDecision::ConfirmApplied as i32
                    {
                        "CONFIRM_APPLIED"
                    } else {
                        "CONFIRM_NOT_APPLIED"
                    };
                    let capability_evidence_classification = if arbitration_mode {
                        match arbitration_outcome {
                            value
                                if value
                                    == pb::IssueOperationArbitrationOutcome::Applied as i32 =>
                            {
                                "RESOLUTION_ARBITRATION_APPLIED"
                            }
                            value
                                if value
                                    == pb::IssueOperationArbitrationOutcome::NotApplied as i32 =>
                            {
                                "RESOLUTION_ARBITRATION_NOT_APPLIED"
                            }
                            value
                                if value
                                    == pb::IssueOperationArbitrationOutcome::UnknownSettled
                                        as i32 =>
                            {
                                "RESOLUTION_ARBITRATION_UNKNOWN_SETTLED"
                            }
                            _ => "RESOLUTION_ARBITRATION_UNSPECIFIED",
                        }
                    } else if decision
                        == pb::IssueOperationResolutionDecision::ConfirmApplied as i32
                    {
                        "RESOLUTION_CONFIRM_APPLIED"
                    } else {
                        "RESOLUTION_CONFIRM_NOT_APPLIED"
                    };
                    // A manual resolution settles the original dispatch decision. In
                    // particular, CONFIRM_APPLIED must not leave a READY row that can
                    // replay the already-confirmed Tracker mutation, while
                    // CONFIRM_NOT_APPLIED must prevent any delayed attempt from being
                    // delivered after the source operation has been marked failed.
                    let terminal_outbox_status = if decision
                        == pb::IssueOperationResolutionDecision::ConfirmApplied as i32
                    {
                        pb::OutboxStatus::Acknowledged as i32
                    } else {
                        pb::OutboxStatus::DeadLetter as i32
                    };
                    transaction.execute(
                        "UPDATE manager_outbox
                    SET status=?2, lease_id=NULL, lease_expires_at=NULL,
                        acknowledged_at=CASE WHEN ?2=?3 THEN COALESCE(acknowledged_at, ?4)
                                             ELSE acknowledged_at END
                  WHERE operation_id=?1
                    AND status NOT IN (?3, ?5)",
                        params![
                            source.operation_id,
                            terminal_outbox_status,
                            pb::OutboxStatus::Acknowledged as i32,
                            now_millis(),
                            pb::OutboxStatus::DeadLetter as i32,
                        ],
                    )?;
                    transaction.execute(
                        "UPDATE admin_dispatch_attempt
                    SET state=?2, lease_id=NULL, lease_expires_at=NULL,
                        resolved_at=?3, evidence_classification=?4
                  WHERE operation_id=?1 AND attempt_generation <= ?5",
                        params![
                            source.operation_id,
                            terminal_outbox_status,
                            now_millis(),
                            attempt_evidence_classification,
                            i64::try_from(source.generation)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                        ],
                    )?;
                    transaction.execute(
                        "UPDATE manager_capability_attempt
                            SET state='RESOLVED',
                                resolved_at=?3, evidence_classification=?4,
                                evidence_snapshot=?5, updated_at=?3
                          WHERE operation_id=?1
                            AND attempt_generation <= ?2
                            AND state IN ('PREPARED', 'SENDING', 'SENT_UNKNOWN')",
                        params![
                            source.operation_id,
                            i64::try_from(source.generation)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                            now_millis(),
                            capability_evidence_classification,
                            resolution_result,
                        ],
                    )?;
                    if !source.idempotency_scope.is_empty() {
                        // A capability dispatch may have durably stored a
                        // typed ResultUnconfirmed wrapper before a human
                        // confirms the server-owned marker.  Human
                        // arbitration is also allowed for a FAILED operation
                        // that still requires resolution; its previous
                        // terminal replay must be replaced by the typed
                        // arbitration outcome, otherwise the source remains
                        // permanently stuck behind an idempotency conflict.
                        // Ordinary terminal rows remain immutable and still
                        // require NULL-or-identical idempotency payloads.
                        let replace_existing_replay_result = source_result_was_supplied
                            && (resolution_mode
                                == pb::IssueOperationResolutionMode::HumanArbitration as i32
                                || matches!(
                                    pb::DurableOperationState::try_from(source_current.state),
                                    Ok(pb::DurableOperationState::Unconfirmed
                                        | pb::DurableOperationState::ManualResolution)
                                ));
                        let changed = transaction.execute(
                    "UPDATE manager_idempotency SET state=?2, result_snapshot=?3, updated_at=?4
                       WHERE operation_id=?1 AND request_digest=?5
                         AND (result_snapshot IS NULL OR result_snapshot=?3 OR ?6=1)",
                    params![
                        source.operation_id,
                        source.state,
                        source_result,
                        operation_updated_at_millis(&source, now_millis()),
                        source.request_digest,
                        i32::from(replace_existing_replay_result),
                    ],
                )?;
                        if changed != 1 {
                            return Err(RepositoryError::OperationConflict);
                        }
                    }
                    let changed = transaction.execute(
                        "UPDATE manager_idempotency SET state=?2, result_snapshot=?3, updated_at=?4
                   WHERE operation_id=?1 AND request_digest=?5
                     AND (result_snapshot IS NULL OR result_snapshot=?3)",
                        params![
                            resolution.operation_id,
                            resolution.state,
                            resolution_result,
                            now_millis(),
                            resolution.request_digest,
                        ],
                    )?;
                    if changed != 1 {
                        return Err(RepositoryError::OperationConflict);
                    }
                    let request_changed = transaction.execute(
                        "UPDATE manager_resolution_request
                    SET state='COMMITTED', updated_at=?2
                  WHERE resolution_operation_id=?1 AND request_digest=?3
                    AND state='PENDING'",
                        params![
                            resolution.operation_id,
                            now_millis(),
                            resolution.request_digest
                        ],
                    )?;
                    if request_changed != 1 {
                        return Err(RepositoryError::OperationConflict);
                    }
                    Ok(())
                })?;
        Ok(commit)
    }

    /// Commit an entity, its operation/result snapshot, and an optional outbox
    /// row in the same SQLite transaction.
    #[cfg(test)]
    pub(crate) fn commit_mutation(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        outbox: Option<&pb::OutboxEntry>,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.commit_mutation_with_removal(mutation, operation, outbox, None, None, None, None)
    }

    pub(super) fn commit_mutation_with_history_resolution(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        typed_result: &[u8],
        history_audit: &ResolutionHistoryAudit,
        history_decision: ResolutionHistoryDecision<'_>,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.commit_mutation_with_removal(
            mutation,
            operation,
            None,
            None,
            Some(history_audit),
            Some(history_decision),
            Some(typed_result),
        )
    }

    pub(super) fn commit_candidate_update_mutation(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
    ) -> Result<ManagerCommit, RepositoryError> {
        let Some(pb::manager_mutation::Entity::Candidate(candidate)) = mutation.entity.as_ref()
        else {
            return Err(RepositoryError::OperationMismatch);
        };
        let input = entity_input(
            EntityKind::Candidate,
            &candidate.candidate_id,
            candidate.revision,
            "issue_tracker.manager.v1.Candidate",
            candidate.encode_to_vec(),
        )?;
        let snapshot = sanitize_mutation(mutation).encode_to_vec();
        let snapshot_digest = digest_bytes(&snapshot);
        let operation = operation.clone();
        let candidate = candidate.clone();
        let (_, commit) =
            self.storage
                .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                    let previous = load_entity_in_transaction::<pb::Candidate>(
                        transaction,
                        EntityKind::Candidate,
                        &candidate.candidate_id,
                        "issue_tracker.manager.v1.Candidate",
                    )?;
                    let proof_invalidated = previous.strong_detection_proof.is_some()
                        && candidate.strong_detection_proof.is_none();
                    if proof_invalidated {
                        ensure_candidate_auto_registration_not_in_flight(
                            transaction,
                            &candidate.candidate_id,
                        )?;
                    }
                    write_entity_in_transaction(
                        transaction,
                        &input,
                        Some(previous.revision),
                        stamp,
                    )?;
                    insert_operation(transaction, &operation, &snapshot, &snapshot_digest, stamp)?;
                    insert_idempotency(transaction, &operation)?;
                    if proof_invalidated {
                        invalidate_candidate_auto_registrations_in_transaction(
                            transaction,
                            stamp,
                            &candidate.candidate_id,
                        )?;
                    }
                    Ok(())
                })?;
        Ok(commit)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn commit_mutation_with_removal(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        outbox: Option<&pb::OutboxEntry>,
        removal: Option<&EntityRemoval>,
        history_audit: Option<&ResolutionHistoryAudit>,
        history_decision: Option<ResolutionHistoryDecision<'_>>,
        idempotency_result: Option<&[u8]>,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.operation_id.is_empty() {
            return Err(RepositoryError::EmptyOperationId);
        }
        if mutation
            .operation
            .as_ref()
            .is_some_and(|candidate| candidate.operation_id != operation.operation_id)
        {
            return Err(RepositoryError::OperationMismatch);
        }
        if outbox.is_some_and(|entry| entry.operation_id != operation.operation_id) {
            return Err(RepositoryError::OutboxOperationMismatch);
        }
        let sanitized = sanitize_mutation(mutation);
        let snapshot = sanitized.encode_to_vec();
        let snapshot_digest = digest_bytes(&snapshot);
        let entity = entity_from_mutation(mutation)?;
        let operation = operation.clone();
        let outbox = outbox.cloned();
        let idempotency_result = idempotency_result.map(Vec::from);

        let (_, commit) =
            self.storage
                .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                    if let Some(removal) = removal {
                        remove_entity_in_transaction(transaction, removal)?;
                    }
                    if let Some(entity) = &entity {
                        write_entity_in_transaction(transaction, entity, None, stamp)?;
                        if entity.kind == EntityKind::IssueMapping {
                            let mapping = pb::IssueMapping::decode(entity.payload.as_slice())
                                .map_err(|error| {
                                    RepositoryError::Serialization(error.to_string())
                                })?;
                            upsert_issue_mapping_index_in_transaction(
                                transaction,
                                &mapping,
                                &operation.operation_id,
                                now_millis(),
                            )?;
                        }
                    }
                    insert_operation(transaction, &operation, &snapshot, &snapshot_digest, stamp)?;
                    if let Some(history_audit) = history_audit {
                        insert_operation_history_resolution(
                            transaction,
                            &operation.operation_id,
                            history_audit,
                        )?;
                    }
                    if let Some(history_decision) = history_decision {
                        insert_operation_history_decision(
                            transaction,
                            &operation.operation_id,
                            history_decision,
                        )?;
                    }
                    insert_idempotency(transaction, &operation)?;
                    if let Some(result) = &idempotency_result {
                        let changed = transaction.execute(
                            "UPDATE manager_idempotency SET result_snapshot=?2, updated_at=?3
                              WHERE operation_id=?1 AND result_snapshot IS NULL",
                            params![operation.operation_id, result, now_millis()],
                        )?;
                        if changed != 1 {
                            return Err(RepositoryError::OperationConflict);
                        }
                    }
                    if let Some(outbox) = &outbox {
                        insert_outbox(transaction, outbox, stamp)?;
                    }
                    Ok(())
                })?;
        Ok(commit)
    }

    /// Admit an acceptance together with an immutable guard.  The guard is
    /// intentionally manager-private and prevents evidence/binding drift
    /// between admission and backend confirmation.
    pub(crate) fn admit_pending_acceptance(
        &mut self,
        pending: PendingAcceptance<'_>,
    ) -> Result<ManagerCommit, RepositoryError> {
        let operation = pending.operation;
        if operation.state != pb::DurableOperationState::Pending as i32
            || operation.operation_id.is_empty()
            || pending.candidate_id.is_empty()
        {
            return Err(RepositoryError::RequestSnapshot(
                "pending acceptance admission is invalid".into(),
            ));
        }
        validate_durable_request_snapshot(
            &durable_backend_method(pending.request)?,
            pending.request,
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        if digest_bytes(pending.request) != pending.outbox.payload_digest
            || pending.outbox.operation_id != operation.operation_id
        {
            return Err(RepositoryError::RequestSnapshot(
                "pending acceptance request does not match outbox".into(),
            ));
        }
        let snapshot = sanitize_mutation(pending.mutation).encode_to_vec();
        let expectation = expectation_snapshot(pending.expectation).encode_to_vec();
        let request = pending.request.to_vec();
        let operation = operation.clone();
        let outbox = pending.outbox.clone();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                insert_operation(
                    transaction,
                    &operation,
                    &snapshot,
                    &digest_bytes(&snapshot),
                    stamp,
                )?;
                transaction.execute(
                    "UPDATE manager_operation SET request_snapshot=?2, request_snapshot_schema=?3,
                 request_snapshot_digest=?4, marker_expectation=?5, marker_expectation_schema=?6
                 WHERE operation_id=?1",
                    params![
                        operation.operation_id,
                        request,
                        "issue_tracker.manager.v1.DurableBackendRequest",
                        digest_bytes(pending.request),
                        expectation,
                        "issue_tracker.v1.OperationRecord"
                    ],
                )?;
                insert_idempotency(transaction, &operation)?;
                insert_outbox(transaction, &outbox, stamp)?;
                transaction.execute(
                    "INSERT INTO manager_acceptance_guard
                    (operation_id, candidate_id, candidate_revision, manifest_digest, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        operation.operation_id,
                        pending.candidate_id,
                        i64::try_from(pending.candidate_revision)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        "",
                        now_millis()
                    ],
                )?;
                if let Some(decision) = pending.history_decision {
                    insert_operation_history_decision(
                        transaction,
                        &operation.operation_id,
                        decision,
                    )?;
                }
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Confirm every consequence of a candidate acceptance in one manager
    /// transaction.  A missing/stale guard rolls the whole decision back.
    pub(crate) fn finalize_confirmed_acceptance(
        &mut self,
        confirmed: ConfirmedAcceptance<'_>,
    ) -> Result<ManagerCommit, RepositoryError> {
        if confirmed.operation.state != pb::DurableOperationState::Committed as i32
            || confirmed.operation.operation_id != confirmed.outbox.operation_id
        {
            return Err(RepositoryError::OperationConflict);
        }
        let candidate_input = entity_input(
            EntityKind::Candidate,
            &confirmed.candidate.candidate_id,
            confirmed.candidate.revision,
            "issue_tracker.manager.v1.Candidate",
            confirmed.candidate.encode_to_vec(),
        )?;
        let mapping_input = entity_input(
            EntityKind::IssueMapping,
            &confirmed.mapping.issue_id,
            confirmed.mapping.revision,
            "issue_tracker.manager.v1.IssueMapping",
            confirmed.mapping.encode_to_vec(),
        )?;
        let mut operation = sanitize_operation(confirmed.operation)?;
        operation.terminal_snapshot = Some(
            pb::durable_operation::TerminalSnapshot::ResultSnapshot(pb::Snapshot {
                payload: confirmed.typed_result.to_vec(),
                schema: "issue_tracker.manager.v1.ResolveCandidateResult".into(),
            }),
        );
        let operation = sanitize_operation(&operation)?;
        let payload = operation.encode_to_vec();
        let outbox = confirmed.outbox.clone();
        self.storage.write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let (guard_candidate, guard_revision): (String, i64) = transaction.query_row(
                    "SELECT candidate_id, candidate_revision FROM manager_acceptance_guard WHERE operation_id=?1",
                    params![operation.operation_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
                if guard_candidate != confirmed.candidate.candidate_id
                    || u64::try_from(guard_revision).map_err(|_| RepositoryError::RevisionOverflow)?
                        != confirmed.candidate.revision.checked_sub(1).ok_or(RepositoryError::RevisionOverflow)?
                { return Err(RepositoryError::OperationConflict); }
                write_entity_in_transaction(transaction, &candidate_input,
                    Some(confirmed.candidate.revision.checked_sub(1).ok_or(RepositoryError::RevisionOverflow)?), stamp)?;
                write_entity_idempotently_in_transaction(transaction, &mapping_input)?;
                upsert_issue_mapping_index_in_transaction(
                    transaction,
                    confirmed.mapping,
                    &operation.operation_id,
                    now_millis(),
                )?;
                let changed = transaction.execute("UPDATE manager_operation
                SET state=?2, result_snapshot=?3, result_snapshot_schema=?4,
                    result_snapshot_digest=?5, operation_payload=?6,
                    operation_payload_digest=?7, updated_at=?8
                WHERE operation_id=?1",
                    params![
                        operation.operation_id,
                        operation.state,
                        confirmed.typed_result,
                        "issue_tracker.manager.v1.ResolveCandidateResult",
                        digest_bytes(confirmed.typed_result),
                        payload,
                        digest_bytes(&payload),
                        operation_updated_at_millis(&operation, now_millis()),
                    ])?;
                if changed != 1 { return Err(RepositoryError::OperationConflict); }
                upsert_operation_history_projection(transaction, &operation, now_millis())?;
                update_outbox_in_transaction(transaction, &outbox)?;
                let changed = transaction.execute("UPDATE manager_idempotency SET state=?2, result_snapshot=?3, updated_at=?4 WHERE operation_id=?1",
                    params![operation.operation_id, operation.state, confirmed.typed_result, now_millis()])?;
                if changed != 1 { return Err(RepositoryError::OperationConflict); }
                transaction.execute("DELETE FROM manager_acceptance_guard WHERE operation_id=?1", params![operation.operation_id])?;
                Ok(())
            }).map(|(_, commit)| commit)
    }

    /// Commit the supersede decision, any newly materialized target bindings,
    /// and its manager-only idempotency result as one transaction.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn commit_supersede_batch(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        source: &pb::Candidate,
        target: Option<&pb::Candidate>,
        typed_result: &[u8],
        history_decision: ResolutionHistoryDecision<'_>,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.state != pb::DurableOperationState::Committed as i32 {
            return Err(RepositoryError::OperationConflict);
        }
        let source_input = entity_input(
            EntityKind::Candidate,
            &source.candidate_id,
            source.revision,
            "issue_tracker.manager.v1.Candidate",
            source.encode_to_vec(),
        )?;
        let target_input = target
            .map(|candidate| {
                entity_input(
                    EntityKind::Candidate,
                    &candidate.candidate_id,
                    candidate.revision,
                    "issue_tracker.manager.v1.Candidate",
                    candidate.encode_to_vec(),
                )
            })
            .transpose()?;
        let snapshot = sanitize_mutation(mutation).encode_to_vec();
        let operation = sanitize_operation(operation)?;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let target_proof_invalidated = if let Some(target) = target {
                    let previous = load_entity_in_transaction::<pb::Candidate>(
                        transaction,
                        EntityKind::Candidate,
                        &target.candidate_id,
                        "issue_tracker.manager.v1.Candidate",
                    )?;
                    let invalidated = previous.strong_detection_proof.is_some()
                        && target.strong_detection_proof.is_none();
                    if invalidated {
                        ensure_candidate_auto_registration_not_in_flight(
                            transaction,
                            &target.candidate_id,
                        )?;
                    }
                    invalidated
                } else {
                    false
                };
                write_entity_in_transaction(
                    transaction,
                    &source_input,
                    Some(
                        source
                            .revision
                            .checked_sub(1)
                            .ok_or(RepositoryError::RevisionOverflow)?,
                    ),
                    stamp,
                )?;
                if let Some(target) = &target_input {
                    write_entity_in_transaction(
                        transaction,
                        target,
                        Some(
                            target
                                .revision
                                .checked_sub(1)
                                .ok_or(RepositoryError::RevisionOverflow)?,
                        ),
                        stamp,
                    )?;
                }
                insert_operation(
                    transaction,
                    &operation,
                    &snapshot,
                    &digest_bytes(&snapshot),
                    stamp,
                )?;
                insert_operation_history_decision(
                    transaction,
                    &operation.operation_id,
                    history_decision,
                )?;
                insert_idempotency(transaction, &operation)?;
                if target_proof_invalidated {
                    invalidate_candidate_auto_registrations_in_transaction(
                        transaction,
                        stamp,
                        &target
                            .expect("target exists when its proof was invalidated")
                            .candidate_id,
                    )?;
                }
                let changed = transaction.execute(
                    "UPDATE manager_idempotency SET result_snapshot=?2, updated_at=?3
                      WHERE operation_id=?1 AND result_snapshot IS NULL",
                    params![operation.operation_id, typed_result, now_millis()],
                )?;
                if changed != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    pub(super) fn write_entity(
        &mut self,
        kind: EntityKind,
        entity_id: &str,
        expected_revision: Option<u64>,
        revision: u64,
        payload_schema: &str,
        payload: &[u8],
    ) -> Result<ManagerCommit, RepositoryError> {
        validate_entity(kind, entity_id, revision, payload_schema, payload)?;
        let input = EntityInput {
            kind,
            entity_id: entity_id.to_owned(),
            revision,
            payload_schema: payload_schema.to_owned(),
            payload: payload.to_vec(),
        };
        let (_, commit) =
            self.storage
                .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                    write_entity_in_transaction(transaction, &input, expected_revision, stamp)
                })?;
        Ok(commit)
    }

    pub(crate) fn candidates(&self) -> Result<Vec<pb::Candidate>, pb::ManagerError> {
        self.list_entities(EntityKind::Candidate)
            .map_err(map_repository_error)?
            .into_iter()
            .map(|stored| decode_stored(stored, "issue_tracker.manager.v1.Candidate"))
            .collect()
    }

    pub(super) fn ensure_candidate_auto_registration_is_not_in_flight(
        &self,
        candidate_id: &str,
    ) -> Result<(), RepositoryError> {
        let in_flight = self.storage.read(|connection| {
            candidate_auto_registration_is_in_flight(connection, candidate_id)
                .map_err(ManagerStorageError::from)
        })?;
        if in_flight {
            return Err(RepositoryError::CandidateAutoRegistrationInFlight);
        }
        Ok(())
    }

    pub(crate) fn extraction_runs(&self) -> Result<Vec<pb::ExtractionRun>, pb::ManagerError> {
        self.list_entities(EntityKind::ExtractionRun)
            .map_err(map_repository_error)?
            .into_iter()
            .map(|stored| decode_stored(stored, "issue_tracker.manager.v1.ExtractionRun"))
            .collect()
    }
}

/// Map internal quarantine text to a stable, low-cardinality diagnosis
/// classification. The detailed repair condition remains in the durable
/// request/diagnosis row, while callers can aggregate by this value safely.
pub(crate) fn operation_diagnosis_class(reason: &str) -> String {
    let reason = reason.to_ascii_lowercase();
    if reason.contains("legacy") {
        "LEGACY_REQUEST".into()
    } else if reason.contains("marker") && reason.contains("missing") {
        "MARKER_MISSING".into()
    } else if reason.contains("marker") {
        "MARKER_INVALID".into()
    } else if reason.contains("request") || reason.contains("snapshot") {
        "REQUEST_SNAPSHOT_INVALID".into()
    } else if reason.contains("acceptance") {
        "ACCEPTANCE_REVALIDATION_FAILED".into()
    } else {
        "OPERATION_INTEGRITY_FAILURE".into()
    }
}
