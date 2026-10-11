//! Transaction-scoped helpers shared by the durable repository operations.
//!
//! Keeping index writes and ownership-state upserts outside the large
//! operation implementation makes their transaction boundary explicit and
//! gives recovery code one place to audit identity handling.

use super::*;

pub(super) fn manager_root_identity(
    transaction: &Transaction<'_>,
) -> Result<(String, String), RepositoryError> {
    let contract: String = transaction.query_row(
        "SELECT value FROM manager_metadata WHERE key='contract_fingerprint'",
        [],
        |row| row.get(0),
    )?;
    let root: String = transaction.query_row(
        "SELECT value FROM manager_metadata WHERE key='data_root_id'",
        [],
        |row| row.get(0),
    )?;
    if contract.is_empty() || root.is_empty() {
        return Err(RepositoryError::RequestSnapshot(
            "manager root identity is missing".into(),
        ));
    }
    Ok((contract, root))
}

/// Return whether a source operation has an unresolved manual-resolution
/// fence. The fence is checked inside the leasing transaction so a driver
/// cannot win a race between candidate selection and the lease write.
pub(super) fn has_pending_resolution_fence(
    transaction: &Transaction<'_>,
    operation_id: &str,
) -> Result<bool, RepositoryError> {
    let fenced = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM resolution_fence
              WHERE source_operation_id=?1
                AND state NOT IN (?2, ?3, ?4)
         )",
        params![
            operation_id,
            pb::DurableOperationState::Committed as i32,
            pb::DurableOperationState::Failed as i32,
            pb::DurableOperationState::Arbitrated as i32,
        ],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(fenced != 0)
}

pub(super) fn insert_backend_request_index(
    transaction: &Transaction<'_>,
    operation: &pb::DurableOperation,
    method: &str,
    request_schema: &str,
    payload: &[u8],
) -> Result<(), RepositoryError> {
    let (contract, root) = manager_root_identity(transaction)?;
    transaction.execute(
        "INSERT INTO manager_backend_request
            (contract_fingerprint, data_root_id, operation_id, method,
             request_schema, request_payload, request_digest, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
        params![
            contract,
            root,
            operation.operation_id,
            method,
            request_schema,
            payload,
            digest_bytes(payload),
            now_millis(),
        ],
    )?;
    Ok(())
}

pub(super) fn insert_admin_expectation_index(
    transaction: &Transaction<'_>,
    operation: &pb::DurableOperation,
    request_payload: &[u8],
    expectation_payload: &[u8],
) -> Result<(), RepositoryError> {
    let (contract, root) = manager_root_identity(transaction)?;
    let args = DurableBackendRequest::decode(request_payload)
        .map_err(|error| RepositoryError::Serialization(error.to_string()))?
        .admin_action
        .and_then(|value| value.args)
        .ok_or_else(|| {
            RepositoryError::RequestSnapshot("administrative request is missing".into())
        })?;
    let expectation = v1::OperationRecord::decode(expectation_payload)
        .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
    transaction.execute(
        "INSERT INTO admin_marker_expectation
            (contract_fingerprint, data_root_id, downstream_operation_id, operation_id,
             issue_id, action, expected_revision, request_digest,
             tracker_contract_fingerprint, tracker_schema_fingerprint, method_key,
             expectation_payload, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13)",
        params![
            contract,
            root,
            operation.downstream_operation_id,
            operation.operation_id,
            args.issue_id,
            args.action,
            args.expected_revision.unwrap_or_default(),
            operation.request_digest,
            expectation.contract_fingerprint,
            crate::tracker::Tracker::schema_fingerprint(),
            expectation.method_key,
            expectation_payload,
            now_millis(),
        ],
    )?;
    Ok(())
}

pub(super) fn insert_admin_dispatch_attempt(
    transaction: &Transaction<'_>,
    operation: &pb::DurableOperation,
    outbox: &pb::OutboxEntry,
) -> Result<(), RepositoryError> {
    let (contract, root) = manager_root_identity(transaction)?;
    transaction.execute(
        "INSERT INTO admin_dispatch_attempt
            (contract_fingerprint, data_root_id, operation_id, attempt_generation,
             outbox_id, state, lease_id, lease_expires_at, permitted_at, resolved_at,
             evidence_classification)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, NULL, NULL)",
        params![
            contract,
            root,
            operation.operation_id,
            i64::try_from(operation.generation).map_err(|_| RepositoryError::RevisionOverflow)?,
            outbox.outbox_id,
            outbox.status,
        ],
    )?;
    Ok(())
}

pub(super) fn insert_capability_attempt(
    transaction: &Transaction<'_>,
    operation: &pb::DurableOperation,
    method: &str,
    issue_id: &str,
    request_digest: &str,
    capability_fingerprint: &str,
) -> Result<(), RepositoryError> {
    let (contract, root) = manager_root_identity(transaction)?;
    if operation.operation_id.is_empty()
        || method.is_empty()
        || issue_id.is_empty()
        || request_digest.is_empty()
        || capability_fingerprint.is_empty()
    {
        return Err(RepositoryError::RequestSnapshot(
            "capability attempt identity is incomplete".into(),
        ));
    }
    transaction.execute(
        "INSERT INTO manager_capability_attempt
            (contract_fingerprint, data_root_id, operation_id, attempt_generation,
             method, issue_id, request_digest, capability_fingerprint, state,
             permitted_at, sent_at, resolved_at, evidence_classification,
             evidence_snapshot, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL, NULL, NULL, NULL, ?10, ?10)",
        params![
            contract,
            root,
            operation.operation_id,
            i64::try_from(operation.generation).map_err(|_| RepositoryError::RevisionOverflow)?,
            method,
            issue_id,
            request_digest,
            capability_fingerprint,
            super::CAPABILITY_ATTEMPT_PREPARED,
            now_millis(),
        ],
    )?;
    Ok(())
}

pub(super) fn upsert_ownership_recovery_state_in_transaction(
    transaction: &Transaction<'_>,
    state: &OwnershipRecoveryState,
) -> Result<(), RepositoryError> {
    let contract: String = transaction.query_row(
        "SELECT value FROM manager_metadata WHERE key='contract_fingerprint'",
        [],
        |row| row.get(0),
    )?;
    let root: String = transaction.query_row(
        "SELECT value FROM manager_metadata WHERE key='data_root_id'",
        [],
        |row| row.get(0),
    )?;
    let changed = transaction.execute(
        "INSERT INTO manager_ownership_recovery
            (contract_fingerprint, data_root_id, issue_id,
             readmission_number, current_operation_id, expected_revision,
             in_flight, consecutive_failures, stopped,
             stop_classification, stop_basis, repair_condition,
             next_attempt_at, cursor_diagnostic, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
         ON CONFLICT(contract_fingerprint, data_root_id, issue_id)
         DO UPDATE SET readmission_number=excluded.readmission_number,
             current_operation_id=excluded.current_operation_id,
             expected_revision=excluded.expected_revision,
             in_flight=excluded.in_flight,
             consecutive_failures=excluded.consecutive_failures,
             stopped=excluded.stopped,
             stop_classification=excluded.stop_classification,
             stop_basis=excluded.stop_basis,
             repair_condition=excluded.repair_condition,
             next_attempt_at=excluded.next_attempt_at,
             cursor_diagnostic=excluded.cursor_diagnostic,
             updated_at=excluded.updated_at
          WHERE excluded.readmission_number > manager_ownership_recovery.readmission_number
             OR (excluded.readmission_number = manager_ownership_recovery.readmission_number
                 AND (manager_ownership_recovery.current_operation_id IS excluded.current_operation_id
                     OR (manager_ownership_recovery.current_operation_id IS NULL
                          AND excluded.current_operation_id IS NOT NULL
                          AND manager_ownership_recovery.in_flight = 0)
                      OR (manager_ownership_recovery.current_operation_id IS NOT NULL
                          AND excluded.current_operation_id IS NULL
                          AND manager_ownership_recovery.in_flight = 1)))",
        params![
            contract,
            root,
            state.issue_id,
            i64::try_from(state.readmission_number).map_err(|_| RepositoryError::RevisionOverflow)?,
            state.current_operation_id,
            i64::try_from(state.expected_revision).map_err(|_| RepositoryError::RevisionOverflow)?,
            i64::from(state.in_flight),
            i64::from(state.consecutive_failures),
            i64::from(state.stopped),
            state.last_failure_class,
            state.stop_basis,
            state.repair_condition,
            state.next_attempt_at_millis,
            state.cursor_diagnostic,
            now_millis(),
        ],
    )?;
    if changed != 1 {
        return Err(RepositoryError::OperationConflict);
    }
    Ok(())
}

pub(super) fn stale_ownership_integrity_diagnosis_in_transaction(
    transaction: &Transaction<'_>,
    target_kind: &str,
    target_id: &str,
    observed_revision: u64,
) -> Result<(), RepositoryError> {
    let (contract, root) = manager_root_identity(transaction)?;
    let observed_revision =
        i64::try_from(observed_revision).map_err(|_| RepositoryError::RevisionOverflow)?;
    let now = now_millis();
    transaction.execute(
        "UPDATE manager_ownership_integrity_diagnosis
            SET state='STALE', observed_revision=?5,
                last_confirmed_at=?6, resolved_at=?6
          WHERE contract_fingerprint=?1 AND data_root_id=?2
            AND target_kind=?3 AND target_id=?4
            AND diagnosis_class='LEASE_INCONSISTENT'
            AND state='OPEN'",
        params![
            contract,
            root,
            target_kind,
            target_id,
            observed_revision,
            now
        ],
    )?;
    Ok(())
}
