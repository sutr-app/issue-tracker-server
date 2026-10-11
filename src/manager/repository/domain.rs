//! Shared repository persistence helpers and canonical candidate conversion.

use super::*;

/// Derive the canonical Issue identifier before backend admission.  UUIDv5
/// keeps retries and process restarts on the same candidate idempotent while
/// satisfying the tracker adapter's UUID boundary.
pub(crate) fn candidate_issue_id(candidate_id: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("issue-tracker-manager:candidate:{candidate_id}").as_bytes(),
    )
    .to_string()
}

fn candidate_expected_issue_state(
    candidate: &pb::Candidate,
) -> Result<(pb::CandidateSuggestedState, v1::IssueState), pb::ManagerError> {
    let suggested_state = pb::CandidateSuggestedState::try_from(candidate.suggested_state)
        .map_err(|_| domain_error("candidate suggested state is invalid"))?;
    let expected_state = match suggested_state {
        pb::CandidateSuggestedState::Open => v1::IssueState::Open,
        // Candidate state is an observation, not an execution lease.  A
        // canonical issue can enter IN_PROGRESS only through Claim.
        pb::CandidateSuggestedState::InProgress => v1::IssueState::Open,
        pb::CandidateSuggestedState::Blocked => v1::IssueState::Blocked,
        pb::CandidateSuggestedState::Deferred => v1::IssueState::Deferred,
        pb::CandidateSuggestedState::Unspecified => {
            return Err(domain_error("candidate suggested state is required"));
        }
    };
    Ok((suggested_state, expected_state))
}

pub(super) fn validate_candidate_issue_state(
    issue: &v1::IssueDraft,
    candidate: &pb::Candidate,
) -> Result<(), pb::ManagerError> {
    let (suggested_state, expected_state) = candidate_expected_issue_state(candidate)?;
    validate_candidate_issue_state_value(issue, suggested_state, expected_state)?;
    if expected_state == v1::IssueState::Deferred && issue.review_at.is_none() {
        return Err(domain_error("deferred candidate issues require review_at"));
    }
    Ok(())
}

fn validate_candidate_issue_state_value(
    issue: &v1::IssueDraft,
    suggested_state: pb::CandidateSuggestedState,
    expected_state: v1::IssueState,
) -> Result<(), pb::ManagerError> {
    let requested_state = match v1::IssueState::try_from(issue.initial_state)
        .map_err(|_| domain_error("issue initial state is invalid"))?
    {
        v1::IssueState::Unspecified => None,
        state => Some(state),
    };
    if let Some(requested_state) = requested_state {
        let in_progress_normalization = suggested_state == pb::CandidateSuggestedState::InProgress
            && requested_state == v1::IssueState::InProgress;
        if requested_state != expected_state && !in_progress_normalization {
            return Err(domain_error(
                "issue initial state must match candidate suggested state",
            ));
        }
    }
    Ok(())
}

pub(super) fn normalize_candidate_issue_state(
    issue: &mut v1::IssueDraft,
    candidate: &pb::Candidate,
) -> Result<(), pb::ManagerError> {
    let (suggested_state, expected_state) = candidate_expected_issue_state(candidate)?;
    validate_candidate_issue_state_value(issue, suggested_state, expected_state)?;
    issue.initial_state = expected_state as i32;
    if suggested_state == pb::CandidateSuggestedState::InProgress {
        if issue.provenance_metadata.iter().any(|entry| {
            matches!(
                entry.key.as_str(),
                super::super::source_labels::OWNERSHIP_NORMALIZED_FROM_KEY
                    | super::super::source_labels::OWNERSHIP_NORMALIZATION_REASON_KEY
            )
        }) {
            return Err(domain_error(
                "ownership normalization provenance is server-owned",
            ));
        }
        issue.provenance_metadata.extend([
            v1::MetadataEntry {
                key: super::super::source_labels::OWNERSHIP_NORMALIZED_FROM_KEY.into(),
                value: "IN_PROGRESS".into(),
            },
            v1::MetadataEntry {
                key: super::super::source_labels::OWNERSHIP_NORMALIZATION_REASON_KEY.into(),
                value: "candidate_suggested_state_is_not_a_claim".into(),
            },
        ]);
    }
    Ok(())
}

#[allow(dead_code)]
pub(super) fn candidate_issue_draft(
    candidate: &pb::Candidate,
) -> Result<v1::IssueDraft, pb::ManagerError> {
    let kind = match pb::CandidateKind::try_from(candidate.kind)
        .map_err(|_| domain_error("candidate kind is invalid"))?
    {
        pb::CandidateKind::Task => v1::IssueKind::Task,
        pb::CandidateKind::Bug => v1::IssueKind::Bug,
        pb::CandidateKind::Research => v1::IssueKind::Research,
        pb::CandidateKind::Decision => v1::IssueKind::Decision,
        pb::CandidateKind::Unspecified => return Err(domain_error("candidate kind is required")),
    };
    let priority = match pb::CandidatePriority::try_from(candidate.suggested_priority)
        .map_err(|_| domain_error("candidate priority is invalid"))?
    {
        pb::CandidatePriority::P0 => v1::IssuePriority::P0,
        pb::CandidatePriority::P1 => v1::IssuePriority::P1,
        pb::CandidatePriority::P2 => v1::IssuePriority::P2,
        pb::CandidatePriority::P3 => v1::IssuePriority::P3,
        pb::CandidatePriority::P4 => v1::IssuePriority::P4,
        pb::CandidatePriority::Unspecified => {
            return Err(domain_error("candidate priority is required"));
        }
    };
    let detection_basis = pb::DetectionBasis::try_from(candidate.detection_basis)
        .map_err(|_| domain_error("candidate detection basis is invalid"))?;
    match detection_basis {
        pb::DetectionBasis::Explicit
        | pb::DetectionBasis::Strong
        | pb::DetectionBasis::Inferred => {}
        pb::DetectionBasis::Unspecified => {
            return Err(domain_error("candidate detection basis is required"));
        }
    };
    let mut provenance_metadata = candidate
        .metadata
        .iter()
        .map(|entry| v1::MetadataEntry {
            key: entry.key.clone(),
            value: entry.value.clone(),
        })
        .collect::<Vec<_>>();
    super::super::source_labels::append_candidate_provenance(&mut provenance_metadata, candidate)
        .map_err(domain_error)?;
    let mut issue = v1::IssueDraft {
        title: candidate.title.clone(),
        description: candidate.description.clone(),
        kind: kind as i32,
        priority: priority as i32,
        acceptance_criteria: candidate.acceptance_criteria.clone(),
        due_at: candidate.due_at.as_ref().map(|value| v1::UtcInstant {
            unix_epoch_millis: value.unix_epoch_millis,
        }),
        // Acceptance is an explicit human decision. Even a strongly detected
        // candidate is therefore created as a confirmed explicit Issue.
        creation_basis: v1::CreationBasis::Explicit as i32,
        confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
        provenance_metadata,
        source_evidence: candidate.source_evidence.clone(),
        initial_state: v1::IssueState::Unspecified as i32,
        ..Default::default()
    };
    normalize_candidate_issue_state(&mut issue, candidate)?;
    Ok(issue)
}

pub(super) fn validate_candidate_acceptance_issue(
    issue: &v1::IssueDraft,
) -> Result<(), pb::ManagerError> {
    if issue.creation_basis != v1::CreationBasis::Explicit as i32
        || issue.confirmation_status != v1::ConfirmationStatus::Confirmed as i32
    {
        return Err(domain_error(
            "candidate acceptance must create an explicit confirmed issue",
        ));
    }
    Ok(())
}

pub(super) fn candidate_detection_proof(
    candidate: &pb::Candidate,
) -> Result<Option<pb::StrongDetectionProof>, pb::ManagerError> {
    if candidate.detection_basis == pb::DetectionBasis::Explicit as i32 {
        return Ok(None);
    }
    let first = candidate
        .confidence_factors
        .first()
        .ok_or_else(|| domain_error("auto-detected candidate lacks confidence proof"))?;
    let confidence = candidate
        .confidence_factors
        .iter()
        .map(|factor| factor.score)
        .fold(0.0_f64, f64::max);
    let threshold = candidate
        .confidence_factors
        .iter()
        .map(|factor| factor.threshold)
        .fold(0.0_f64, f64::max);
    Ok(Some(pb::StrongDetectionProof {
        model_id: first.model_id.clone(),
        model_version: first.model_version.clone(),
        rule_version: first.rule_version.clone(),
        confidence,
        threshold,
        confidence_factors: candidate.confidence_factors.clone(),
        raw_memory_verified: true,
        unfinished_work_verified: true,
        no_duplicate_verified: true,
    }))
}

#[derive(Clone, Debug)]
pub(crate) struct EntityInput {
    pub(crate) kind: EntityKind,
    pub(crate) entity_id: String,
    pub(crate) revision: u64,
    pub(crate) payload_schema: String,
    pub(crate) payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(super) struct EntityRemoval {
    pub(super) kind: EntityKind,
    pub(super) entity_id: String,
    pub(super) expected_revision: u64,
}

pub(super) fn validate_entity(
    kind: EntityKind,
    entity_id: &str,
    _revision: u64,
    payload_schema: &str,
    payload: &[u8],
) -> Result<(), RepositoryError> {
    if entity_id.is_empty() {
        return Err(RepositoryError::EmptyEntityId);
    }
    if payload_schema.is_empty() {
        return Err(RepositoryError::EmptyPayloadSchema);
    }
    let allowed = match kind {
        EntityKind::Candidate => {
            payload_schema == "issue_tracker.manager.v1.Candidate"
                && pb::Candidate::decode(payload).is_ok_and(|value| value.candidate_id == entity_id)
        }
        EntityKind::ExtractionRun => {
            payload_schema == "issue_tracker.manager.v1.ExtractionRun"
                && pb::ExtractionRun::decode(payload).is_ok_and(|value| value.run_id == entity_id)
        }
        EntityKind::IssueMapping => {
            payload_schema == "issue_tracker.manager.v1.IssueMapping"
                && pb::IssueMapping::decode(payload).is_ok_and(|value| value.issue_id == entity_id)
        }
        // Tombstones are internal records and are not accepted through the
        // generic protobuf write path.
        EntityKind::Tombstone => false,
    };
    if !allowed {
        return Err(RepositoryError::ForbiddenPayload(payload_schema.into()));
    }
    Ok(())
}

const GENERIC_MAPPING_OPERATION_ID: &str = "manager-entity-write";

pub(super) fn upsert_issue_mapping_index_in_transaction(
    transaction: &Transaction<'_>,
    mapping: &pb::IssueMapping,
    operation_id: &str,
    now: i64,
) -> Result<(), RepositoryError> {
    // Backend Issue refreshes often carry only the Issue identity and current
    // revision.  Preserve the existing Candidate association in that case;
    // otherwise an ordinary Issue transition would erase the review join.
    let inherited_candidate_id: Option<String> = if mapping
        .candidate_id
        .as_deref()
        .is_none_or(|id| id.trim().is_empty())
    {
        transaction
            .query_row(
                "SELECT candidate_id FROM manager_issue_mapping_index WHERE issue_id=?1",
                params![mapping.issue_id],
                |row| row.get(0),
            )
            .optional()?
    } else {
        None
    };
    let candidate_id = mapping
        .candidate_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .map(ToOwned::to_owned)
        .or(inherited_candidate_id);
    let Some(candidate_id) = candidate_id.as_deref() else {
        transaction.execute(
            "DELETE FROM manager_issue_mapping_index WHERE issue_id=?1",
            params![mapping.issue_id],
        )?;
        return Ok(());
    };
    if mapping.candidate_id.is_none() || mapping.candidate_id.as_deref().is_some_and(str::is_empty)
    {
        let mut enriched = mapping.clone();
        enriched.candidate_id = Some(candidate_id.to_owned());
        let payload = enriched.encode_to_vec();
        transaction.execute(
            "UPDATE manager_entity
                SET payload=?2, payload_digest=?3
              WHERE entity_kind='issue_mapping' AND entity_id=?1",
            params![mapping.issue_id, payload, digest_bytes(&payload)],
        )?;
    }
    transaction.execute(
        "DELETE FROM manager_issue_mapping_index
          WHERE issue_id=?1 OR candidate_id=?2",
        params![mapping.issue_id, candidate_id],
    )?;
    transaction.execute(
        "INSERT INTO manager_issue_mapping_index
             (candidate_id, issue_id, mapping_state, operation_id, created_at, updated_at)
         VALUES (?1, ?2, 2, ?3, ?4, ?4)
         ON CONFLICT(candidate_id) DO UPDATE SET
             issue_id=excluded.issue_id,
             mapping_state=excluded.mapping_state,
             operation_id=excluded.operation_id,
             updated_at=excluded.updated_at",
        params![candidate_id, mapping.issue_id, operation_id, now],
    )?;
    Ok(())
}

fn remove_issue_mapping_index_in_transaction(
    transaction: &Transaction<'_>,
    issue_id: &str,
) -> Result<(), RepositoryError> {
    transaction.execute(
        "DELETE FROM manager_issue_mapping_index WHERE issue_id=?1",
        params![issue_id],
    )?;
    Ok(())
}

pub(super) fn write_entity_in_transaction(
    transaction: &Transaction<'_>,
    input: &EntityInput,
    expected_revision: Option<u64>,
    stamp: ManagerCommit,
) -> Result<(), RepositoryError> {
    write_entity_in_transaction_with_operation(
        transaction,
        input,
        expected_revision,
        stamp,
        GENERIC_MAPPING_OPERATION_ID,
    )
}

pub(super) fn write_entity_in_transaction_with_operation(
    transaction: &Transaction<'_>,
    input: &EntityInput,
    expected_revision: Option<u64>,
    stamp: ManagerCommit,
    operation_id: &str,
) -> Result<(), RepositoryError> {
    validate_entity(
        input.kind,
        &input.entity_id,
        input.revision,
        &input.payload_schema,
        &input.payload,
    )?;
    let current: Option<i64> = transaction
        .query_row(
            "SELECT revision FROM manager_entity
                  WHERE entity_kind = ?1 AND entity_id = ?2",
            params![input.kind.as_str(), input.entity_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(expected) = expected_revision {
        let actual = current.unwrap_or(0);
        if actual != i64::try_from(expected).map_err(|_| RepositoryError::RevisionOverflow)? {
            return Err(RepositoryError::RevisionConflict {
                expected,
                actual: actual.try_into().unwrap_or_default(),
            });
        }
    } else if current
        .is_some_and(|value| value >= i64::try_from(input.revision).unwrap_or(i64::MAX))
    {
        return Err(RepositoryError::RevisionConflict {
            expected: current
                .and_then(|value| value.try_into().ok())
                .unwrap_or_default(),
            actual: current
                .and_then(|value| value.try_into().ok())
                .unwrap_or_default(),
        });
    }
    let now = now_millis();
    let digest = digest_bytes(&input.payload);
    transaction.execute(
        "INSERT INTO manager_entity
            (entity_kind, entity_id, revision, payload, payload_schema, payload_digest,
             created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
         ON CONFLICT(entity_kind, entity_id) DO UPDATE SET
             revision = excluded.revision,
             payload = excluded.payload,
             payload_schema = excluded.payload_schema,
             payload_digest = excluded.payload_digest,
             updated_at = excluded.updated_at",
        params![
            input.kind.as_str(),
            input.entity_id,
            i64::try_from(input.revision).map_err(|_| RepositoryError::RevisionOverflow)?,
            input.payload,
            input.payload_schema,
            digest,
            now,
        ],
    )?;
    transaction.execute(
        "INSERT INTO manager_entity_revision(entity_kind, entity_id, revision)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(entity_kind, entity_id) DO UPDATE SET revision = excluded.revision",
        params![
            input.kind.as_str(),
            input.entity_id,
            i64::try_from(input.revision).map_err(|_| RepositoryError::RevisionOverflow)?,
        ],
    )?;
    if input.kind == EntityKind::Tombstone {
        transaction.execute(
            "INSERT INTO manager_tombstone
                (tombstone_id, entity_id, revision, payload, payload_schema,
                 payload_digest, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
             ON CONFLICT(tombstone_id) DO UPDATE SET
                 revision = excluded.revision,
                 payload = excluded.payload,
                 payload_schema = excluded.payload_schema,
                 payload_digest = excluded.payload_digest,
                 updated_at = excluded.updated_at",
            params![
                input.entity_id,
                input.entity_id,
                i64::try_from(input.revision).map_err(|_| RepositoryError::RevisionOverflow)?,
                input.payload,
                input.payload_schema,
                digest,
                now,
            ],
        )?;
    }
    if input.kind == EntityKind::IssueMapping {
        let mapping = pb::IssueMapping::decode(input.payload.as_slice())
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        upsert_issue_mapping_index_in_transaction(transaction, &mapping, operation_id, now)?;
    }
    let _ = stamp;
    Ok(())
}

pub(super) fn load_entity_in_transaction<M: Message + Default>(
    transaction: &Transaction<'_>,
    kind: EntityKind,
    entity_id: &str,
    schema: &str,
) -> Result<M, RepositoryError> {
    let (payload, stored_schema): (Vec<u8>, String) = transaction
        .query_row(
            "SELECT payload, payload_schema FROM manager_entity
              WHERE entity_kind=?1 AND entity_id=?2",
            params![kind.as_str(), entity_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or_else(|| RepositoryError::RequestSnapshot(format!("{schema} is missing")))?;
    if stored_schema != schema {
        return Err(RepositoryError::RequestSnapshot(format!(
            "{schema} payload schema is invalid"
        )));
    }
    M::decode(payload.as_slice()).map_err(|error| RepositoryError::Serialization(error.to_string()))
}

pub(super) fn extraction_auto_registration_in_transaction(
    transaction: &Transaction<'_>,
    operation_id: &str,
) -> Result<Option<ExtractionAutoRegistration>, RepositoryError> {
    transaction
        .query_row(
            "SELECT run_id, candidate_id, issue_id, expected_idempotency_key
               FROM manager_extraction_auto_registration
              WHERE operation_id=?1",
            params![operation_id],
            |row| {
                Ok(ExtractionAutoRegistration {
                    run_id: row.get(0)?,
                    candidate_id: row.get(1)?,
                    issue_id: row.get(2)?,
                    expected_idempotency_key: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(RepositoryError::from)
}

pub(super) fn suppression_identity_index(
    identities: &[ExtractionCandidateIdentity],
) -> HashMap<&str, usize> {
    identities.iter().enumerate().fold(
        HashMap::with_capacity(identities.len()),
        |mut indices, (index, identity)| {
            // Keep the first position so lookup preserves the old linear scan
            // when corrupted input contains duplicate candidate IDs.
            indices
                .entry(identity.candidate_id.as_str())
                .or_insert(index);
            indices
        },
    )
}

pub(super) fn suppression_audit_identity<'a>(
    identities: &'a [ExtractionCandidateIdentity],
    identity_indices: &HashMap<&str, usize>,
    suppression: &ExtractionSuppression,
) -> Result<&'a ExtractionCandidateIdentity, RepositoryError> {
    let identity_index = suppression
        .rejected_candidate_ids
        .iter()
        .filter_map(|candidate_id| identity_indices.get(candidate_id.as_str()).copied())
        .min()
        .ok_or_else(|| {
            RepositoryError::RequestSnapshot("suppression audit has no immutable identity".into())
        })?;
    Ok(&identities[identity_index])
}

pub(super) fn candidate_auto_registration_is_in_flight(
    connection: &Connection,
    candidate_id: &str,
) -> Result<bool, rusqlite::Error> {
    connection.query_row(
        "SELECT EXISTS(
            SELECT 1
              FROM manager_extraction_auto_registration AS link
              JOIN manager_operation AS operation
                ON operation.operation_id = link.operation_id
         LEFT JOIN manager_outbox AS outbox
                ON outbox.operation_id = link.operation_id
             WHERE link.candidate_id = ?1
               AND link.state = ?2
               AND (operation.state = ?3
                 OR outbox.status IN (?4, ?5))
        )",
        params![
            candidate_id,
            EXTRACTION_AUTO_REGISTRATION_PENDING,
            pb::DurableOperationState::Unconfirmed as i32,
            pb::OutboxStatus::Leased as i32,
            pb::OutboxStatus::Dispatched as i32,
        ],
        |row| row.get(0),
    )
}

pub(super) fn ensure_candidate_auto_registration_not_in_flight(
    connection: &Connection,
    candidate_id: &str,
) -> Result<(), RepositoryError> {
    if candidate_auto_registration_is_in_flight(connection, candidate_id)? {
        return Err(RepositoryError::CandidateAutoRegistrationInFlight);
    }
    Ok(())
}

pub(super) fn invalidate_candidate_auto_registrations_in_transaction(
    transaction: &Transaction<'_>,
    stamp: ManagerCommit,
    candidate_id: &str,
) -> Result<(), RepositoryError> {
    let links = {
        let mut statement = transaction.prepare(
            "SELECT operation_id
               FROM manager_extraction_auto_registration
              WHERE candidate_id=?1 AND state=?2",
        )?;
        statement
            .query_map(
                params![candidate_id, EXTRACTION_AUTO_REGISTRATION_PENDING],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?
    };
    transaction.execute(
        "DELETE FROM manager_extraction_auto_registration_reservation WHERE candidate_id=?1",
        params![candidate_id],
    )?;
    for operation_id in links {
        let payload: Vec<u8> = transaction.query_row(
            "SELECT operation_payload FROM manager_operation WHERE operation_id=?1",
            params![operation_id],
            |row| row.get(0),
        )?;
        let mut operation = pb::DurableOperation::decode(payload.as_slice())
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        transition_operation(
            &mut operation,
            pb::DurableOperationState::Failed,
            Some(pb::FailureClass::Terminal),
            now_millis(),
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        let reason = "candidate changed; auto-registration proof was invalidated";
        let error = pb::ManagerError {
            code: pb::ManagerErrorCode::PreconditionFailed as i32,
            message: reason.into(),
            retryable: false,
            detail: None,
        };
        let error_payload = error.encode_to_vec();
        operation.terminal_snapshot = Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(
            pb::Snapshot {
                payload: error_payload.clone(),
                schema: "issue_tracker.manager.v1.ManagerError".into(),
            },
        ));
        let operation = sanitize_operation(&operation)?;
        let operation_payload = operation.encode_to_vec();
        if transaction.execute(
            "UPDATE manager_operation
                SET state=?2, attempt=?3, manager_sequence=?4, generation=?5,
                    updated_at=?6, result_snapshot=NULL, result_snapshot_schema=NULL,
                    result_snapshot_digest=NULL, error_snapshot=?7,
                    error_snapshot_schema=?8, error_snapshot_digest=?9,
                    operation_payload=?10, operation_payload_schema=?11,
                    operation_payload_digest=?12
              WHERE operation_id=?1",
            params![
                operation_id,
                operation.state,
                i64::from(operation.attempt),
                i64::try_from(stamp.manager_sequence)
                    .map_err(|_| RepositoryError::RevisionOverflow)?,
                i64::try_from(stamp.generation).map_err(|_| RepositoryError::RevisionOverflow)?,
                operation_updated_at_millis(&operation, now_millis()),
                error_payload,
                "issue_tracker.manager.v1.ManagerError",
                digest_bytes(&error_payload),
                operation_payload,
                "issue_tracker.manager.v1.DurableOperation",
                digest_bytes(&operation_payload),
            ],
        )? != 1
        {
            return Err(RepositoryError::OperationConflict);
        }
        upsert_operation_history_projection(
            transaction,
            &operation,
            operation_updated_at_millis(&operation, now_millis()),
        )?;
        if transaction.execute(
            "UPDATE manager_outbox
                SET status=?2, lease_id=NULL, lease_expires_at=NULL, next_attempt_at=NULL,
                    error_code=?3
              WHERE operation_id=?1 AND status=?4",
            params![
                operation_id,
                pb::OutboxStatus::DeadLetter as i32,
                pb::ManagerErrorCode::PreconditionFailed as i32,
                pb::OutboxStatus::Ready as i32,
            ],
        )? != 1
        {
            return Err(RepositoryError::CandidateAutoRegistrationInFlight);
        }
        transaction.execute(
            "UPDATE manager_idempotency
                SET state=?2, error_snapshot=?3, updated_at=?4
              WHERE operation_id=?1",
            params![
                operation_id,
                pb::DurableOperationState::Failed as i32,
                error_payload,
                now_millis(),
            ],
        )?;
        transaction.execute(
            "UPDATE manager_extraction_auto_registration
                SET state=?2, failure_reason=?3, updated_at=?4
              WHERE operation_id=?1 AND state=?5",
            params![
                operation_id,
                EXTRACTION_AUTO_REGISTRATION_FAILED,
                reason,
                now_millis(),
                EXTRACTION_AUTO_REGISTRATION_PENDING,
            ],
        )?;
    }
    Ok(())
}

pub(super) fn ensure_extraction_auto_registration_eligible_in_transaction(
    transaction: &Transaction<'_>,
    run_id: &str,
    candidate_id: &str,
    candidate_error: &'static str,
    run_error: &'static str,
) -> Result<(), RepositoryError> {
    ensure_extraction_auto_registration_candidate_is_eligible_in_transaction(
        transaction,
        run_id,
        candidate_id,
        candidate_error,
    )?;
    ensure_extraction_auto_registration_run_is_running_in_transaction(
        transaction,
        run_id,
        run_error,
    )
}

pub(super) fn ensure_extraction_auto_registration_candidate_is_eligible_in_transaction(
    transaction: &Transaction<'_>,
    run_id: &str,
    candidate_id: &str,
    error: &'static str,
) -> Result<(), RepositoryError> {
    let candidate = load_entity_in_transaction::<pb::Candidate>(
        transaction,
        EntityKind::Candidate,
        candidate_id,
        "issue_tracker.manager.v1.Candidate",
    )?;
    if candidate.extraction_run_id != run_id
        || candidate.status != pb::CandidateStatus::Proposed as i32
        || candidate.strong_detection_proof.is_none()
    {
        return Err(RepositoryError::RequestSnapshot(error.into()));
    }
    Ok(())
}

pub(super) fn ensure_extraction_auto_registration_run_is_running_in_transaction(
    transaction: &Transaction<'_>,
    run_id: &str,
    error: &'static str,
) -> Result<(), RepositoryError> {
    let run = load_entity_in_transaction::<pb::ExtractionRun>(
        transaction,
        EntityKind::ExtractionRun,
        run_id,
        "issue_tracker.manager.v1.ExtractionRun",
    )?;
    if run.state != pb::ExtractionRunState::Running as i32 {
        return Err(RepositoryError::RequestSnapshot(error.into()));
    }
    Ok(())
}

pub(super) fn consume_extraction_auto_registration_reservation_in_transaction(
    transaction: &Transaction<'_>,
    operation_id: &str,
    issue_id: &str,
    idempotency_key: &str,
) -> Result<(), RepositoryError> {
    let reservation: Option<(String, String, String)> = transaction
        .query_row(
            "SELECT run_id, candidate_id, expected_idempotency_key
               FROM manager_extraction_auto_registration_reservation
              WHERE issue_id=?1",
            params![issue_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((run_id, candidate_id, expected_idempotency_key)) = reservation else {
        return Ok(());
    };
    if expected_idempotency_key != idempotency_key {
        return Ok(());
    }
    ensure_extraction_auto_registration_eligible_in_transaction(
        transaction,
        &run_id,
        &candidate_id,
        "auto-registration reservation candidate is no longer eligible",
        "auto-registration reservation run is not running",
    )?;
    let now = now_millis();
    transaction.execute(
        "INSERT INTO manager_extraction_auto_registration
            (operation_id, run_id, candidate_id, issue_id, expected_idempotency_key,
             state, failure_reason, created_at, updated_at)
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
    if transaction.execute(
        "DELETE FROM manager_extraction_auto_registration_reservation WHERE issue_id=?1",
        params![issue_id],
    )? != 1
    {
        return Err(RepositoryError::OperationConflict);
    }
    Ok(())
}

pub(super) fn finalize_extraction_auto_registration_in_transaction(
    transaction: &Transaction<'_>,
    stamp: ManagerCommit,
    operation: &pb::DurableOperation,
    link: &ExtractionAutoRegistration,
) -> Result<(), RepositoryError> {
    let state = pb::DurableOperationState::try_from(operation.state).map_err(|_| {
        RepositoryError::RequestSnapshot("auto-registration operation state is invalid".into())
    })?;
    match state {
        pb::DurableOperationState::Committed => {
            let candidate = load_entity_in_transaction::<pb::Candidate>(
                transaction,
                EntityKind::Candidate,
                &link.candidate_id,
                "issue_tracker.manager.v1.Candidate",
            )?;
            match pb::CandidateStatus::try_from(candidate.status) {
                Ok(pb::CandidateStatus::Proposed) => {
                    let proof = candidate.strong_detection_proof.clone().ok_or_else(|| {
                        RepositoryError::RequestSnapshot(
                            "auto-registration candidate lost strong detection proof".into(),
                        )
                    })?;
                    let updated = CandidateStore::from_candidates(vec![candidate.clone()])
                        .record_extraction_outcome(
                            &link.candidate_id,
                            Some(proof),
                            Some(link.issue_id.clone()),
                            utc_instant(now_millis()),
                        )
                        .map_err(|error| {
                            RepositoryError::RequestSnapshot(format!(
                                "auto-registration candidate transition failed: {error:?}"
                            ))
                        })?;
                    let input = entity_input(
                        EntityKind::Candidate,
                        &updated.candidate_id,
                        updated.revision,
                        "issue_tracker.manager.v1.Candidate",
                        updated.encode_to_vec(),
                    )?;
                    write_entity_in_transaction(
                        transaction,
                        &input,
                        Some(candidate.revision),
                        stamp,
                    )?;
                }
                Ok(pb::CandidateStatus::AutoRegistered)
                    if matches!(
                        candidate.superseded_by,
                        Some(pb::candidate::SupersededBy::SupersedingIssueId(ref issue_id))
                            if issue_id == &link.issue_id
                    ) => {}
                _ => {
                    return Err(RepositoryError::RequestSnapshot(
                        "auto-registration candidate is no longer registrable".into(),
                    ));
                }
            }
            transaction.execute(
                "UPDATE manager_extraction_auto_registration
                    SET state=?2, failure_reason=NULL, updated_at=?3
                  WHERE operation_id=?1 AND state=?4",
                params![
                    operation.operation_id,
                    EXTRACTION_AUTO_REGISTRATION_COMMITTED,
                    now_millis(),
                    EXTRACTION_AUTO_REGISTRATION_PENDING,
                ],
            )?;
            let has_pending: bool = transaction.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM manager_extraction_auto_registration
                     WHERE run_id=?1 AND state=?2
                    UNION ALL
                    SELECT 1 FROM manager_extraction_auto_registration_reservation
                     WHERE run_id=?1
                 )",
                params![link.run_id, EXTRACTION_AUTO_REGISTRATION_PENDING],
                |row| row.get(0),
            )?;
            if !has_pending {
                let run = load_entity_in_transaction::<pb::ExtractionRun>(
                    transaction,
                    EntityKind::ExtractionRun,
                    &link.run_id,
                    "issue_tracker.manager.v1.ExtractionRun",
                )?;
                if run.state == pb::ExtractionRunState::Running as i32 {
                    let completed = ExtractionRunStore::from_runs(vec![run.clone()])
                        .advance(
                            &run.run_id,
                            run.revision,
                            &pb::ExtractionRunAdvance {
                                target_state: pb::ExtractionRunState::Completed as i32,
                                progress_percent: Some(100),
                                ..Default::default()
                            },
                            utc_instant(now_millis()),
                        )
                        .map_err(|error| {
                            RepositoryError::RequestSnapshot(format!(
                                "auto-registration run completion failed: {error:?}"
                            ))
                        })?;
                    let input = entity_input(
                        EntityKind::ExtractionRun,
                        &completed.run_id,
                        completed.revision,
                        "issue_tracker.manager.v1.ExtractionRun",
                        completed.encode_to_vec(),
                    )?;
                    write_entity_in_transaction(transaction, &input, Some(run.revision), stamp)?;
                }
            }
        }
        pb::DurableOperationState::Failed | pb::DurableOperationState::ManualResolution => {
            let reason = extraction_auto_registration_failure_reason(operation);
            transaction.execute(
                "UPDATE manager_extraction_auto_registration
                    SET state=?2, failure_reason=?3, updated_at=?4
                  WHERE operation_id=?1 AND state=?5",
                params![
                    operation.operation_id,
                    EXTRACTION_AUTO_REGISTRATION_FAILED,
                    reason,
                    now_millis(),
                    EXTRACTION_AUTO_REGISTRATION_PENDING,
                ],
            )?;
            // A terminal operation cannot admit the remaining candidates in this run.
            // Keeping their reservations would indefinitely block every linked outbox entry.
            transaction.execute(
                "DELETE FROM manager_extraction_auto_registration_reservation WHERE run_id=?1",
                params![link.run_id],
            )?;
            let run = load_entity_in_transaction::<pb::ExtractionRun>(
                transaction,
                EntityKind::ExtractionRun,
                &link.run_id,
                "issue_tracker.manager.v1.ExtractionRun",
            )?;
            if run.state == pb::ExtractionRunState::Running as i32 {
                let failed = ExtractionRunStore::from_runs(vec![run.clone()])
                    .advance(
                        &run.run_id,
                        run.revision,
                        &pb::ExtractionRunAdvance {
                            target_state: pb::ExtractionRunState::Failed as i32,
                            error: Some(reason),
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
                write_entity_in_transaction(transaction, &input, Some(run.revision), stamp)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn extraction_auto_registration_failure_reason(
    operation: &pb::DurableOperation,
) -> String {
    operation
        .terminal_snapshot
        .as_ref()
        .and_then(|snapshot| match snapshot {
            pb::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot) => {
                pb::ManagerError::decode(snapshot.payload.as_slice())
                    .ok()
                    .map(|error| error.message)
            }
            pb::durable_operation::TerminalSnapshot::ResultSnapshot(_) => None,
        })
        .filter(|reason| !reason.trim().is_empty())
        .unwrap_or_else(|| "auto-registration backend operation failed".into())
}

pub(super) fn lookup_extraction_apply_replay(
    connection: &Connection,
    idempotency_key: &str,
    request_digest: &str,
) -> Result<ExtractionApplyReplayDecision, rusqlite::Error> {
    let by_key = connection
        .query_row(
            "SELECT key.request_digest, outcome.outcome, outcome.run_id, outcome.skip_reason
               FROM manager_extraction_apply_key AS key
               JOIN manager_extraction_apply_outcome AS outcome
                 ON outcome.request_digest = key.request_digest
              WHERE key.idempotency_key = ?1",
            [idempotency_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i32>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i32>>(3)?,
                ))
            },
        )
        .optional()?;
    if let Some((existing_digest, outcome, run_id, skip_reason)) = by_key {
        return Ok(if existing_digest == request_digest {
            ExtractionApplyReplayDecision::Replay {
                outcome,
                run_id,
                skip_reason,
            }
        } else {
            ExtractionApplyReplayDecision::Conflict
        });
    }
    let by_digest = connection
        .query_row(
            "SELECT outcome, run_id, skip_reason
               FROM manager_extraction_apply_outcome
              WHERE request_digest = ?1",
            [request_digest],
            |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i32>>(2)?,
                ))
            },
        )
        .optional()?;
    Ok(by_digest.map_or(
        ExtractionApplyReplayDecision::New,
        |(outcome, run_id, skip_reason)| ExtractionApplyReplayDecision::Replay {
            outcome,
            run_id,
            skip_reason,
        },
    ))
}

pub(super) fn extraction_apply_key_digest(
    connection: &Connection,
    idempotency_key: &str,
) -> Result<Option<String>, rusqlite::Error> {
    connection
        .query_row(
            "SELECT request_digest
               FROM manager_extraction_apply_key
              WHERE idempotency_key = ?1",
            [idempotency_key],
            |row| row.get(0),
        )
        .optional()
}

pub(super) fn write_entity_idempotently_in_transaction(
    transaction: &Transaction<'_>,
    input: &EntityInput,
) -> Result<(), RepositoryError> {
    validate_entity(
        input.kind,
        &input.entity_id,
        input.revision,
        &input.payload_schema,
        &input.payload,
    )?;
    let current: Option<(i64, Vec<u8>)> = transaction
        .query_row(
            "SELECT revision, payload
               FROM manager_entity
              WHERE entity_kind = ?1 AND entity_id = ?2",
            params![input.kind.as_str(), input.entity_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((revision, payload)) = current {
        let revision = u64::try_from(revision).map_err(|_| RepositoryError::RevisionOverflow)?;
        if revision != input.revision
            || !equivalent_entity_payload(input.kind, &payload, &input.payload)
        {
            return Err(RepositoryError::RevisionConflict {
                expected: input.revision,
                actual: revision,
            });
        }
        return Ok(());
    }
    write_entity_in_transaction(
        transaction,
        input,
        None,
        ManagerCommit {
            manager_sequence: 0,
            generation: 0,
        },
    )
}

pub(super) fn equivalent_entity_payload(kind: EntityKind, left: &[u8], right: &[u8]) -> bool {
    if left == right {
        return true;
    }
    match kind {
        EntityKind::IssueMapping => {
            let Ok(mut left) = pb::IssueMapping::decode(left) else {
                return false;
            };
            let Ok(mut right) = pb::IssueMapping::decode(right) else {
                return false;
            };
            left.created_at = None;
            left.updated_at = None;
            right.created_at = None;
            right.updated_at = None;
            left == right
        }
        _ => false,
    }
}

pub(super) fn remove_entity_in_transaction(
    transaction: &Transaction<'_>,
    removal: &EntityRemoval,
) -> Result<(), RepositoryError> {
    let revision = transaction
        .query_row(
            "SELECT revision FROM manager_entity
                  WHERE entity_kind = ?1 AND entity_id = ?2",
            params![removal.kind.as_str(), removal.entity_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or(RepositoryError::OperationConflict)?;
    let actual = revision
        .try_into()
        .map_err(|_| RepositoryError::RevisionOverflow)?;
    if actual != removal.expected_revision {
        return Err(RepositoryError::RevisionConflict {
            expected: removal.expected_revision,
            actual,
        });
    }
    let deleted = transaction.execute(
        "DELETE FROM manager_entity WHERE entity_kind = ?1 AND entity_id = ?2",
        params![removal.kind.as_str(), removal.entity_id],
    )?;
    if deleted != 1 {
        return Err(RepositoryError::OperationConflict);
    }
    if removal.kind == EntityKind::IssueMapping {
        remove_issue_mapping_index_in_transaction(transaction, &removal.entity_id)?;
    }
    Ok(())
}

pub(super) fn entity_from_mutation(
    mutation: &pb::ManagerMutation,
) -> Result<Option<EntityInput>, RepositoryError> {
    let Some(entity) = &mutation.entity else {
        return Ok(None);
    };
    let (kind, entity_id, revision, payload_schema): (EntityKind, String, u64, &str) = match entity
    {
        pb::manager_mutation::Entity::Candidate(value) => (
            EntityKind::Candidate,
            value.candidate_id.clone(),
            value.revision,
            "issue_tracker.manager.v1.Candidate",
        ),
        pb::manager_mutation::Entity::ExtractionRun(value) => (
            EntityKind::ExtractionRun,
            value.run_id.clone(),
            value.revision,
            "issue_tracker.manager.v1.ExtractionRun",
        ),
        pb::manager_mutation::Entity::IssueMapping(value) => (
            EntityKind::IssueMapping,
            value.issue_id.clone(),
            value.revision,
            "issue_tracker.manager.v1.IssueMapping",
        ),
        pb::manager_mutation::Entity::DurableOperation(_) => return Ok(None),
    };
    let payload = match entity {
        pb::manager_mutation::Entity::Candidate(value) => value.encode_to_vec(),
        pb::manager_mutation::Entity::ExtractionRun(value) => value.encode_to_vec(),
        pb::manager_mutation::Entity::DurableOperation(value) => value.encode_to_vec(),
        pb::manager_mutation::Entity::IssueMapping(value) => value.encode_to_vec(),
    };
    validate_entity(kind, &entity_id, revision, payload_schema, &payload)?;
    Ok(Some(EntityInput {
        kind,
        entity_id,
        revision,
        payload_schema: payload_schema.into(),
        payload,
    }))
}

pub(super) fn entity_input(
    kind: EntityKind,
    entity_id: &str,
    revision: u64,
    payload_schema: &str,
    payload: Vec<u8>,
) -> Result<EntityInput, RepositoryError> {
    validate_entity(kind, entity_id, revision, payload_schema, &payload)?;
    Ok(EntityInput {
        kind,
        entity_id: entity_id.into(),
        revision,
        payload_schema: payload_schema.into(),
        payload,
    })
}

pub(super) fn sanitize_mutation(mutation: &pb::ManagerMutation) -> pb::ManagerMutation {
    let mut sanitized = mutation.clone();
    // The durable operation is stored in its own row; retaining the nested
    // copy would duplicate untrusted snapshots and could reintroduce a
    // canonical Issue through commit_snapshot.
    sanitized.operation = None;
    match sanitized.result.as_mut() {
        Some(pb::manager_mutation::Result::ClaimIssue(claim)) => {
            claim.current = None;
            claim.lease_handle.clear();
        }
        Some(pb::manager_mutation::Result::IssueCreation(result)) => {
            result.issue = None;
        }
        Some(pb::manager_mutation::Result::IssueOwnership(result)) => {
            // Canonical backend Issues are not manager-owned durable data.
            // Ownership receipts retain only the state/revision identity;
            // the full Issue may be returned by the live Tracker response.
            result.current = None;
        }
        _ => {}
    }
    sanitized
}

pub(super) fn sanitize_operation(
    operation: &pb::DurableOperation,
) -> Result<pb::DurableOperation, RepositoryError> {
    let mut sanitized = operation.clone();
    if let Some(commit_snapshot) = sanitized.commit_snapshot.as_mut() {
        if let Some(snapshot) = commit_snapshot.snapshot.as_mut() {
            sanitize_snapshot(snapshot)?;
            let digest = digest_bytes(&snapshot.payload);
            commit_snapshot.snapshot_id = format!("snapshot-{digest}");
            commit_snapshot.snapshot_digest = digest;
        }
    }
    if let Some(terminal_snapshot) = sanitized.terminal_snapshot.as_mut() {
        let snapshot = match terminal_snapshot {
            pb::durable_operation::TerminalSnapshot::ResultSnapshot(value)
            | pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value) => value,
        };
        sanitize_snapshot(snapshot)?;
    }
    Ok(sanitized)
}

pub(super) fn sanitize_snapshot(snapshot: &mut pb::Snapshot) -> Result<(), RepositoryError> {
    validate_snapshot(snapshot)?;
    if snapshot.schema == MANAGER_MUTATION_SNAPSHOT_SCHEMA {
        let mutation = pb::ManagerMutation::decode(snapshot.payload.as_slice())
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        snapshot.payload = sanitize_mutation(&mutation).encode_to_vec();
    }
    Ok(())
}

pub(super) fn insert_operation(
    transaction: &Transaction<'_>,
    operation: &pb::DurableOperation,
    snapshot: &[u8],
    snapshot_digest: &str,
    stamp: ManagerCommit,
) -> Result<(), RepositoryError> {
    let mut operation = sanitize_operation(operation)?;
    let now = now_millis();
    let fallback_timestamps = operation_timestamps(&operation, now);
    if operation.created_at.is_none() {
        operation.created_at = Some(utc_instant(fallback_timestamps.created_at));
    }
    if operation.updated_at.is_none() {
        operation.updated_at = Some(utc_instant(fallback_timestamps.updated_at));
    }
    let (candidate_id, evidence_id, extraction_run_id, issue_id, backup_id, export_id) =
        operation_scope(&operation);
    let terminal = operation.terminal_snapshot.as_ref();
    let (result_snapshot, error_snapshot) = match terminal {
        Some(pb::durable_operation::TerminalSnapshot::ResultSnapshot(value)) => (Some(value), None),
        Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value)) => (None, Some(value)),
        None => (None, None),
    };
    if let Some(snapshot) = result_snapshot.or(error_snapshot) {
        validate_snapshot(snapshot)?;
    }
    let operation_payload = operation.encode_to_vec();
    let operation_payload_digest = digest_bytes(&operation_payload);
    let operation_timestamps = operation_timestamps(&operation, now);
    transaction.execute(
        "INSERT INTO manager_operation
            (operation_id, method, idempotency_scope, request_digest, state, attempt,
             manager_sequence, generation, causal_parent, sync_subject, affects_ready,
             snapshot, result_snapshot, error_snapshot, created_at, updated_at, snapshot_schema,
             snapshot_digest, result_snapshot_schema, result_snapshot_digest,
             error_snapshot_schema, error_snapshot_digest, operation_payload,
             operation_payload_schema, operation_payload_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                 ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)",
        params![
            operation.operation_id,
            operation.method,
            operation.idempotency_scope,
            operation.request_digest,
            operation.state,
            i64::from(operation.attempt),
            i64::try_from(stamp.manager_sequence).map_err(|_| RepositoryError::RevisionOverflow)?,
            i64::try_from(stamp.generation).map_err(|_| RepositoryError::RevisionOverflow)?,
            operation.causal_parent,
            operation.sync_subject,
            i64::from(operation.affects_ready),
            snapshot,
            result_snapshot.map(|value| value.payload.as_slice()),
            error_snapshot.map(|value| value.payload.as_slice()),
            operation_timestamps.created_at,
            operation_timestamps.updated_at,
            MANAGER_MUTATION_SNAPSHOT_SCHEMA,
            snapshot_digest,
            result_snapshot.map(|value| value.schema.as_str()),
            result_snapshot.map(|value| digest_bytes(&value.payload)),
            error_snapshot.map(|value| value.schema.as_str()),
            error_snapshot.map(|value| digest_bytes(&value.payload)),
            operation_payload,
            "issue_tracker.manager.v1.DurableOperation",
            operation_payload_digest,
        ],
    )?;
    // Scope values are kept as indexed lookup columns without persisting a
    // canonical Issue shadow or any ephemeral claim capability.
    transaction.execute(
            "INSERT INTO manager_operation_scope
            (operation_id, candidate_id, evidence_id, extraction_run_id, issue_id, backup_id, export_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                operation.operation_id,
                candidate_id,
                evidence_id,
                extraction_run_id,
                issue_id,
                backup_id,
                export_id,
            ],
        )?;
    upsert_operation_history_projection(transaction, &operation, now)?;
    Ok(())
}

pub(super) fn upsert_operation_history_projection(
    transaction: &Transaction<'_>,
    operation: &pb::DurableOperation,
    timestamp: i64,
) -> Result<(), RepositoryError> {
    let (candidate_id, evidence_id, extraction_run_id, issue_id, _, _) = operation_scope(operation);
    let target = [
        ("candidate", candidate_id),
        ("evidence", evidence_id),
        ("extraction_run", extraction_run_id),
        ("issue", issue_id),
    ]
    .into_iter()
    .find_map(|(kind, id)| id.map(|id| (kind, id)));
    let (target_kind, target_id) = target
        .map(|(kind, id)| (Some(kind), Some(id)))
        .unwrap_or((None, None));
    let operation_timestamps = operation_timestamps(operation, timestamp);
    let mut projection_existed = false;
    if let Some((existing_kind, existing_id, existing_created_at)) = transaction
        .query_row(
            "SELECT direct_target_kind, direct_target_id, created_at
               FROM manager_operation_history_projection
              WHERE operation_id = ?1",
            params![operation.operation_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?
    {
        projection_existed = true;
        if existing_kind.as_deref() != target_kind
            || existing_id.as_deref() != target_id
            || (operation.created_at.is_some()
                && existing_created_at != operation_timestamps.created_at)
        {
            return Err(RepositoryError::RequestSnapshot(
                "operation history projection scope is immutable".into(),
            ));
        }
        let existing_issues = {
            let mut statement = transaction.prepare(
                "SELECT issue_id FROM manager_operation_history_issue
                  WHERE operation_id = ?1 ORDER BY issue_id",
            )?;
            statement
                .query_map(params![operation.operation_id], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<BTreeSet<_>, _>>()?
        };
        let expected_issues = operation
            .affected_issue_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if existing_issues != expected_issues {
            return Err(RepositoryError::RequestSnapshot(
                "operation history affected Issue set is immutable".into(),
            ));
        }
    }
    transaction.execute(
        "INSERT INTO manager_operation_history_projection
            (operation_id, method, state, direct_target_kind, direct_target_id,
             created_at, updated_at, resolution_required)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(operation_id) DO UPDATE SET
            method = excluded.method,
            state = excluded.state,
            updated_at = excluded.updated_at,
            resolution_required = excluded.resolution_required",
        params![
            operation.operation_id,
            operation.method,
            operation.state,
            target_kind,
            target_id,
            operation_timestamps.created_at,
            operation_timestamps.updated_at,
            i64::from(operation.resolution_required),
        ],
    )?;
    if !projection_existed {
        let affected_issue_ids = operation
            .affected_issue_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for issue_id in affected_issue_ids {
            if issue_id.trim().is_empty() {
                return Err(RepositoryError::RequestSnapshot(
                    "operation history affected Issue ID is empty".into(),
                ));
            }
            transaction.execute(
                "INSERT INTO manager_operation_history_issue (operation_id, issue_id)
                 VALUES (?1, ?2)",
                params![operation.operation_id, issue_id],
            )?;
        }
    }
    Ok(())
}

pub(super) fn resolution_history_audit(
    saga: &ResolutionSaga,
) -> Result<Option<ResolutionHistoryAudit>, RepositoryError> {
    match saga.decision {
        ResolutionSagaDecision::ConfirmAssociatedIssue | ResolutionSagaDecision::MergeIssue => {
            Ok(None)
        }
        ResolutionSagaDecision::CancelAssociatedIssue => {
            let Some(pb::resolve_candidate_args::Decision::CancelAssociatedIssue(cancel)) =
                saga.args.decision.as_ref()
            else {
                return Err(RepositoryError::RequestSnapshot(
                    "cancellation resolution saga has an invalid decision payload".into(),
                ));
            };
            Ok(Some(ResolutionHistoryAudit {
                resolution_kind: "cancel_associated_issue",
                reason_category: resolution_reason_category(cancel.reason_category)?,
                rationale: normalized_resolution_rationale(cancel.rationale.as_deref())?,
            }))
        }
    }
}

pub(super) fn resolution_history_decision(
    saga: &ResolutionSaga,
) -> Result<ResolutionHistoryDecision<'_>, RepositoryError> {
    let kind = match saga.decision {
        ResolutionSagaDecision::ConfirmAssociatedIssue => {
            pb::ResolutionDecisionKind::ConfirmAssociatedIssue
        }
        ResolutionSagaDecision::CancelAssociatedIssue => {
            pb::ResolutionDecisionKind::CancelAssociatedIssue
        }
        ResolutionSagaDecision::MergeIssue => pb::ResolutionDecisionKind::Merge,
    };
    Ok(ResolutionHistoryDecision {
        kind,
        candidate_id: &saga.candidate_id,
        candidate_revision: saga.candidate_revision,
    })
}

pub(super) fn resolution_reason_category(
    value: i32,
) -> Result<pb::ResolutionReasonCategory, RepositoryError> {
    match pb::ResolutionReasonCategory::try_from(value) {
        Ok(pb::ResolutionReasonCategory::Unspecified) | Err(_) => Err(
            RepositoryError::RequestSnapshot("resolution reason category is invalid".into()),
        ),
        Ok(category) => Ok(category),
    }
}

pub(super) fn normalized_resolution_rationale(
    value: Option<&str>,
) -> Result<Option<String>, RepositoryError> {
    validate_display_safe_text("resolution_rationale", value)
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))
}

pub(super) fn insert_operation_history_resolution(
    transaction: &Transaction<'_>,
    operation_id: &str,
    history_audit: &ResolutionHistoryAudit,
) -> Result<(), RepositoryError> {
    transaction.execute(
        "INSERT INTO manager_operation_history_resolution
            (operation_id, resolution_kind, reason_category, rationale)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            operation_id,
            history_audit.resolution_kind,
            history_audit.reason_category as i32,
            history_audit.rationale,
        ],
    )?;
    Ok(())
}

pub(super) fn insert_operation_history_decision(
    transaction: &Transaction<'_>,
    operation_id: &str,
    decision: ResolutionHistoryDecision<'_>,
) -> Result<(), RepositoryError> {
    if operation_id.is_empty()
        || decision.candidate_id.is_empty()
        || decision.kind == pb::ResolutionDecisionKind::Unspecified
    {
        return Err(RepositoryError::RequestSnapshot(
            "operation history decision is invalid".into(),
        ));
    }
    transaction.execute(
        "INSERT INTO manager_operation_history_decision
            (operation_id, decision_kind, candidate_id, candidate_revision)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            operation_id,
            decision.kind as i32,
            decision.candidate_id,
            i64::try_from(decision.candidate_revision)
                .map_err(|_| RepositoryError::RevisionOverflow)?,
        ],
    )?;
    Ok(())
}

pub(super) fn validate_snapshot(snapshot: &pb::Snapshot) -> Result<(), RepositoryError> {
    if snapshot.schema == "issue_tracker.v1.Issue" || snapshot.schema.ends_with(".Issue") {
        return Err(RepositoryError::ForbiddenPayload(snapshot.schema.clone()));
    }
    Ok(())
}

pub(super) fn insert_idempotency(
    transaction: &Transaction<'_>,
    operation: &pb::DurableOperation,
) -> Result<(), RepositoryError> {
    if operation.idempotency_scope.is_empty() {
        return Ok(());
    }
    let existing: Option<(String, String)> = transaction
        .query_row(
            "SELECT request_digest, operation_id FROM manager_idempotency
                  WHERE idempotency_scope = ?1",
            params![operation.idempotency_scope],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((digest, operation_id)) = existing {
        if digest != operation.request_digest || operation_id != operation.operation_id {
            return Err(RepositoryError::IdempotencyConflict);
        }
        return Ok(());
    }
    transaction.execute(
        "INSERT INTO manager_idempotency
            (idempotency_scope, request_digest, operation_id, state, result_snapshot,
             error_snapshot, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?6)",
        params![
            operation.idempotency_scope,
            operation.request_digest,
            operation.operation_id,
            operation.state,
            Option::<&[u8]>::None,
            now_millis(),
        ],
    )?;
    Ok(())
}

pub(super) fn insert_outbox(
    transaction: &Transaction<'_>,
    outbox: &pb::OutboxEntry,
    stamp: ManagerCommit,
) -> Result<(), RepositoryError> {
    let lease_expires_at = outbox
        .lease_expires_at
        .as_ref()
        .map(|value| value.unix_epoch_millis);
    let next_attempt_at = outbox
        .next_attempt_at
        .as_ref()
        .map(|value| value.unix_epoch_millis);
    let dispatched_at = outbox
        .dispatched_at
        .as_ref()
        .map(|value| value.unix_epoch_millis);
    let acknowledged_at = outbox
        .acknowledged_at
        .as_ref()
        .map(|value| value.unix_epoch_millis);
    let created_at = outbox
        .created_at
        .as_ref()
        .map(|value| value.unix_epoch_millis)
        .unwrap_or_else(now_millis);
    transaction.execute(
        "INSERT INTO manager_outbox
            (outbox_id, operation_id, topic, status, payload_snapshot_ref, payload_digest,
             generation, attempt, lease_id, lease_expires_at, next_attempt_at,
             dispatched_at, acknowledged_at, error_code, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            outbox.outbox_id,
            outbox.operation_id,
            outbox.topic,
            outbox.status,
            outbox.payload_snapshot_ref,
            outbox.payload_digest,
            i64::try_from(stamp.generation).map_err(|_| RepositoryError::RevisionOverflow)?,
            i64::from(outbox.attempt),
            outbox.lease_id,
            lease_expires_at,
            next_attempt_at,
            dispatched_at,
            acknowledged_at,
            outbox.error_code,
            created_at,
        ],
    )?;
    Ok(())
}

pub(super) fn update_outbox_in_transaction(
    transaction: &Transaction<'_>,
    outbox: &pb::OutboxEntry,
) -> Result<(), RepositoryError> {
    let changed = transaction.execute(
            "UPDATE manager_outbox SET status=?2, attempt=?3, lease_id=?4, lease_expires_at=?5, next_attempt_at=?6, dispatched_at=?7, acknowledged_at=?8, error_code=?9 WHERE outbox_id=?1 AND operation_id=?10",
            params![outbox.outbox_id, outbox.status, i64::from(outbox.attempt), outbox.lease_id,
                outbox.lease_expires_at.as_ref().map(|v| v.unix_epoch_millis), outbox.next_attempt_at.as_ref().map(|v| v.unix_epoch_millis),
                outbox.dispatched_at.as_ref().map(|v| v.unix_epoch_millis), outbox.acknowledged_at.as_ref().map(|v| v.unix_epoch_millis), outbox.error_code, outbox.operation_id],
        )?;
    if changed != 1 {
        return Err(RepositoryError::OutboxOperationMismatch);
    }
    Ok(())
}

pub(super) fn operation_scope(
    operation: &pb::DurableOperation,
) -> (
    Option<&str>,
    Option<&str>,
    Option<&str>,
    Option<&str>,
    Option<&str>,
    Option<&str>,
) {
    match operation.scope.as_ref() {
        Some(pb::durable_operation::Scope::CandidateId(value)) => {
            (Some(value), None, None, None, None, None)
        }
        Some(pb::durable_operation::Scope::EvidenceId(value)) => {
            (None, Some(value), None, None, None, None)
        }
        Some(pb::durable_operation::Scope::ExtractionRunId(value)) => {
            (None, None, Some(value), None, None, None)
        }
        Some(pb::durable_operation::Scope::IssueId(value)) => {
            (None, None, None, Some(value), None, None)
        }
        None => (None, None, None, None, None, None),
    }
}

#[derive(Debug)]
pub(super) struct StoredGatewayReplay {
    pub(super) rpc_method: String,
    pub(super) request_digest: String,
    pub(super) operation_state: i32,
    pub(super) response_schema: Option<String>,
    pub(super) response_snapshot: Option<Vec<u8>>,
}

pub(crate) const AWAITING_REVIEW_REPLAY_METHOD: &str = "complete_execution_to_awaiting_review";
pub(crate) const AWAITING_REVIEW_REPLAY_SCHEMA: &str =
    "issue_tracker.rpc.v1.IssueGatewayAwaitingReviewMutation";

pub(super) fn validate_gateway_replay_component(
    value: &str,
    name: &str,
) -> Result<(), RepositoryError> {
    if value.trim().is_empty() {
        return Err(RepositoryError::RequestSnapshot(format!(
            "gateway replay {name} must not be empty"
        )));
    }
    Ok(())
}

pub(super) fn validate_gateway_replay_input(
    endpoint_id: &str,
    capability_epoch: &str,
    idempotency_key: &str,
    rpc_method: &str,
    request_digest: &str,
) -> Result<(), RepositoryError> {
    validate_gateway_replay_component(endpoint_id, "endpoint ID")?;
    validate_gateway_replay_component(capability_epoch, "capability epoch")?;
    validate_gateway_replay_component(idempotency_key, "idempotency key")?;
    validate_gateway_replay_component(rpc_method, "RPC method")?;
    validate_gateway_replay_component(request_digest, "request digest")
}

pub(super) fn query_active_gateway_epoch(
    connection: &Connection,
    endpoint_id: &str,
) -> rusqlite::Result<Option<String>> {
    connection
        .query_row(
            "SELECT capability_epoch
               FROM manager_gateway_epoch
              WHERE endpoint_id = ?1",
            params![endpoint_id],
            |row| row.get(0),
        )
        .optional()
}

pub(super) fn query_gateway_replay(
    connection: &Connection,
    endpoint_id: &str,
    capability_epoch: &str,
    idempotency_key: &str,
) -> rusqlite::Result<Option<StoredGatewayReplay>> {
    connection
        .query_row(
            "SELECT rpc_method, request_digest, operation_state,
                    response_schema, response_snapshot
               FROM manager_gateway_replay_record
              WHERE endpoint_id = ?1
                AND capability_epoch = ?2
                AND idempotency_key = ?3",
            params![endpoint_id, capability_epoch, idempotency_key],
            |row| {
                Ok(StoredGatewayReplay {
                    rpc_method: row.get(0)?,
                    request_digest: row.get(1)?,
                    operation_state: row.get(2)?,
                    response_schema: row.get(3)?,
                    response_snapshot: row.get(4)?,
                })
            },
        )
        .optional()
}

pub(super) fn gateway_replay_decision_for_record(
    record: &StoredGatewayReplay,
    rpc_method: &str,
    request_digest: &str,
) -> GatewayReplayDecision {
    if record.rpc_method != rpc_method || record.request_digest != request_digest {
        return GatewayReplayDecision::Conflict;
    }
    match (&record.response_schema, &record.response_snapshot) {
        (Some(response_schema), Some(response_snapshot))
            if record.operation_state == pb::DurableOperationState::Committed as i32 =>
        {
            GatewayReplayDecision::Replay {
                response_schema: response_schema.clone(),
                response_snapshot: response_snapshot.clone(),
            }
        }
        _ if rpc_method == AWAITING_REVIEW_REPLAY_METHOD
            && record.operation_state == pb::DurableOperationState::Unspecified as i32 =>
        {
            GatewayReplayDecision::AwaitingReviewRecovery
        }
        _ => GatewayReplayDecision::Unconfirmed,
    }
}

pub(super) fn gateway_replay_old_epoch_decision(
    connection: &Connection,
    endpoint_id: &str,
    capability_epoch: &str,
    idempotency_key: &str,
    rpc_method: &str,
    request_digest: &str,
) -> rusqlite::Result<Option<GatewayReplayDecision>> {
    let (count, conflicts): (i64, i64) = connection.query_row(
        "SELECT COUNT(*),
                COALESCE(SUM(CASE
                    WHEN rpc_method <> ?4 OR request_digest <> ?5 THEN 1
                    ELSE 0
                END), 0)
           FROM manager_gateway_replay_record
          WHERE endpoint_id = ?1
            AND capability_epoch <> ?2
            AND idempotency_key = ?3",
        params![
            endpoint_id,
            capability_epoch,
            idempotency_key,
            rpc_method,
            request_digest,
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if count == 0 {
        return Ok(None);
    }
    if conflicts > 0 {
        return Ok(Some(GatewayReplayDecision::Conflict));
    }

    // Only this versioned completion returns an immutable receipt with no
    // capability. Other Gateway replay records remain scoped to their
    // process-local epoch because they may describe one-time lease authority.
    if rpc_method != AWAITING_REVIEW_REPLAY_METHOD || count != 1 {
        return Ok(Some(GatewayReplayDecision::Unconfirmed));
    }
    let (response_snapshot, response_schema, operation_state) = connection.query_row(
        "SELECT response_snapshot, response_schema, operation_state
           FROM manager_gateway_replay_record
          WHERE endpoint_id = ?1
            AND capability_epoch <> ?2
            AND idempotency_key = ?3
            AND rpc_method = ?4
            AND request_digest = ?5",
        params![
            endpoint_id,
            capability_epoch,
            idempotency_key,
            rpc_method,
            request_digest,
        ],
        |row| {
            Ok((
                row.get::<_, Option<Vec<u8>>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i32>(2)?,
            ))
        },
    )?;
    match (response_schema, response_snapshot) {
        (Some(response_schema), Some(response_snapshot))
            if response_schema == AWAITING_REVIEW_REPLAY_SCHEMA
                && !response_snapshot.is_empty()
                && operation_state == pb::DurableOperationState::Committed as i32 =>
        {
            Ok(Some(GatewayReplayDecision::Replay {
                response_schema,
                response_snapshot,
            }))
        }
        (None, None) if operation_state == pb::DurableOperationState::Unspecified as i32 => {
            Ok(Some(GatewayReplayDecision::AwaitingReviewRecovery))
        }
        _ => Ok(Some(GatewayReplayDecision::Unconfirmed)),
    }
}

pub(super) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct OperationTimestamps {
    pub(super) created_at: i64,
    pub(super) updated_at: i64,
}

/// Return the timestamp values carried by a durable payload. The fallback is
/// used only while writing legacy caller input; persisted history rows still
/// require both payload timestamps for deterministic traversal.
pub(super) fn operation_timestamps(
    operation: &pb::DurableOperation,
    fallback: i64,
) -> OperationTimestamps {
    OperationTimestamps {
        created_at: operation
            .created_at
            .as_ref()
            .map_or(fallback, |instant| instant.unix_epoch_millis),
        updated_at: operation
            .updated_at
            .as_ref()
            .map_or(fallback, |instant| instant.unix_epoch_millis),
    }
}

pub(super) fn operation_updated_at_millis(operation: &pb::DurableOperation, fallback: i64) -> i64 {
    operation_timestamps(operation, fallback).updated_at
}

pub(super) fn utc_instant(unix_epoch_millis: i64) -> pb::UtcInstant {
    pb::UtcInstant { unix_epoch_millis }
}

pub(super) fn gateway_error(
    code: pb::ManagerErrorCode,
    message: impl Into<String>,
) -> pb::ManagerError {
    pb::ManagerError {
        code: code as i32,
        message: message.into(),
        retryable: false,
        detail: None,
    }
}

pub(super) fn map_repository_error(error: RepositoryError) -> pb::ManagerError {
    let code = match &error {
        RepositoryError::RevisionConflict { .. } => pb::ManagerErrorCode::RevisionConflict,
        RepositoryError::GenerationConflict { .. } => pb::ManagerErrorCode::GenerationConflict,
        RepositoryError::IdempotencyConflict => pb::ManagerErrorCode::IdempotencyConflict,
        RepositoryError::CandidateAutoRegistrationInFlight => {
            pb::ManagerErrorCode::PreconditionFailed
        }
        RepositoryError::OperationConflict => pb::ManagerErrorCode::OperationConflict,
        RepositoryError::ForbiddenPayload(_) => pb::ManagerErrorCode::ManagerInvalidArgument,
        RepositoryError::EmptyOperationId => pb::ManagerErrorCode::OperationNotFound,
        _ => pb::ManagerErrorCode::ManagerInvalidArgument,
    };
    gateway_error(code, error.to_string())
}

pub(super) fn domain_error(message: impl Into<String>) -> pb::ManagerError {
    gateway_error(pb::ManagerErrorCode::ManagerInvalidArgument, message)
}

pub(super) fn validate_gateway_eligibility_generation(
    expected_generation: Option<u64>,
    current_generation: u64,
) -> Result<(), pb::ManagerError> {
    if expected_generation.is_some_and(|expected| expected != current_generation) {
        return Err(gateway_error(
            pb::ManagerErrorCode::PreconditionFailed,
            "gateway work eligibility changed; re-evaluate the claim",
        ));
    }
    Ok(())
}

pub(super) fn decode_stored<M: Message + Default>(
    stored: StoredEntity,
    expected_schema: &str,
) -> Result<M, pb::ManagerError> {
    if stored.payload_schema != expected_schema {
        return Err(domain_error(
            "stored manager payload schema is not permitted",
        ));
    }
    M::decode(stored.payload.as_slice()).map_err(|error| domain_error(error.to_string()))
}

pub(super) fn manager_mutation(entity: pb::manager_mutation::Entity) -> pb::ManagerMutation {
    pb::ManagerMutation {
        entity: Some(entity),
        ..Default::default()
    }
}

pub(super) fn durable_backend_method(request: &[u8]) -> Result<String, RepositoryError> {
    let envelope = DurableBackendRequest::decode(request)
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
    if envelope.method.trim().is_empty() {
        return Err(RepositoryError::RequestSnapshot(
            "durable backend request method is missing".into(),
        ));
    }
    Ok(envelope.method)
}

pub(super) fn validate_gateway_registration_input(
    endpoint_id: &str,
    gateway_id: &str,
    policy: &pb::WorkEligibilityPolicy,
) -> Result<(), RepositoryError> {
    if endpoint_id.trim().is_empty() || gateway_id.trim().is_empty() {
        return Err(RepositoryError::EmptyEntityId);
    }
    match pb::WorkEligibilityMode::try_from(policy.mode) {
        Ok(pb::WorkEligibilityMode::ReviewedOnly)
        | Ok(pb::WorkEligibilityMode::IncludeAutoRegistered) => Ok(()),
        Ok(pb::WorkEligibilityMode::Unspecified) | Err(_) => Err(RepositoryError::RequestSnapshot(
            "gateway work eligibility policy is invalid".into(),
        )),
    }
}

pub(super) fn manager_only_operation(
    method: &str,
    scope: Option<pb::durable_operation::Scope>,
    idempotency_scope: &str,
    request_digest: &str,
    operation_id: &str,
) -> pb::DurableOperation {
    let now = utc_instant(now_millis());
    pb::DurableOperation {
        operation_id: operation_id.to_owned(),
        scope,
        method: method.to_owned(),
        idempotency_scope: idempotency_scope.to_owned(),
        request_digest: request_digest.to_owned(),
        request_schema: format!("{method}.request"),
        state: pb::DurableOperationState::Received as i32,
        created_at: Some(now.clone()),
        updated_at: Some(now),
        commit_durability: crate::v1::OperationCommitDurability::Durable as i32,
        result_replayability: crate::v1::OperationResultReplayability::Replayable as i32,
        ..Default::default()
    }
}

pub(super) struct BackendWrite {
    pub(super) context: pb::ManagerWriteContext,
    pub(super) issue_id: String,
}

pub(super) fn normalize_direct_issue_create_source_evidence(
    request: &mut pb::CreateIssueArgs,
    limits: super::super::config::SourceEvidenceLimits,
) -> Result<(), pb::ManagerError> {
    let draft = request
        .issue
        .as_mut()
        .ok_or_else(|| domain_error("issue draft is required"))?;
    draft.source_evidence =
        super::super::candidates::normalize_source_evidence(&draft.source_evidence, limits)
            .map_err(|error| domain_error(format!("source evidence is invalid: {error:?}")))?;
    Ok(())
}

pub(super) fn normalize_direct_issue_update_source_evidence(
    request: &mut pb::UpdateIssueArgs,
    limits: super::super::config::SourceEvidenceLimits,
) -> Result<(), pb::ManagerError> {
    if !request
        .update_mask
        .contains(&(pb::ManagerIssueField::SourceEvidence as i32))
    {
        return Ok(());
    }
    let patch = request
        .patch
        .as_mut()
        .ok_or_else(|| domain_error("issue patch is required"))?;
    patch.source_evidence =
        super::super::candidates::normalize_source_evidence(&patch.source_evidence, limits)
            .map_err(|error| domain_error(format!("source evidence is invalid: {error:?}")))?;
    Ok(())
}

pub(super) fn validate_issue_creation_request(
    request: &DurableCreateIssueRequest,
    actor_audit: Option<&pb::ActorAudit>,
) -> Result<(), pb::ManagerError> {
    let draft = request
        .issue
        .as_ref()
        .ok_or_else(|| domain_error("issue draft is required"))?;
    if request.issue_id.trim().is_empty() || draft.title.trim().is_empty() {
        return Err(domain_error("issue ID and title are required"));
    }
    if draft.kind == v1::IssueKind::Unspecified as i32
        || draft.priority == v1::IssuePriority::Unspecified as i32
        || draft.creation_basis == v1::CreationBasis::Unspecified as i32
        || draft.confirmation_status == v1::ConfirmationStatus::Unspecified as i32
    {
        return Err(domain_error(
            "issue kind, priority, creation basis, and confirmation status are required",
        ));
    }
    if draft.creation_basis == v1::CreationBasis::AutoDetected as i32
        && draft.confirmation_status != v1::ConfirmationStatus::Unreviewed as i32
    {
        return Err(domain_error(
            "auto-detected issue creation must remain unreviewed",
        ));
    }
    validate_strong_detection_proof(draft, request.strong_detection_proof.as_ref())?;
    let human_explicit_without_evidence = draft.creation_basis
        == v1::CreationBasis::Explicit as i32
        && actor_audit.is_some_and(|audit| audit.actor_kind == v1::ActorKind::Human as i32);
    if !draft.source_evidence.is_empty() || human_explicit_without_evidence {
        Ok(())
    } else {
        Err(domain_error(
            "source evidence is required unless a human explicitly creates the issue",
        ))
    }
}

pub(super) fn validate_strong_detection_proof(
    draft: &v1::IssueDraft,
    proof: Option<&pb::StrongDetectionProof>,
) -> Result<(), pb::ManagerError> {
    let requires_proof = draft.creation_basis == v1::CreationBasis::AutoDetected as i32;
    let Some(proof) = proof else {
        return if requires_proof {
            Err(domain_error(
                "auto-detected issue creation requires strong detection proof",
            ))
        } else {
            Ok(())
        };
    };
    if proof.model_id.trim().is_empty()
        || proof.model_version.trim().is_empty()
        || proof.rule_version.trim().is_empty()
        || !proof.confidence.is_finite()
        || !proof.threshold.is_finite()
        || !(0.0..=1.0).contains(&proof.confidence)
        || !(0.0..=1.0).contains(&proof.threshold)
        || proof.confidence < proof.threshold
        || proof.confidence_factors.is_empty()
        || !proof.raw_memory_verified
        || !proof.unfinished_work_verified
        || !proof.no_duplicate_verified
        || proof.confidence_factors.iter().any(|factor| {
            factor.model_id.trim().is_empty()
                || factor.model_version.trim().is_empty()
                || factor.rule_version.trim().is_empty()
                || !factor.score.is_finite()
                || !factor.threshold.is_finite()
                || !(0.0..=1.0).contains(&factor.score)
                || !(0.0..=1.0).contains(&factor.threshold)
                || factor.score < factor.threshold
        })
    {
        return Err(domain_error("strong detection proof is invalid"));
    }
    Ok(())
}

pub(super) fn backend_write(method: &str, args: &[u8]) -> Result<BackendWrite, pb::ManagerError> {
    macro_rules! context {
        ($type:ty) => {
            <$type>::decode(args).ok().and_then(|v| {
                v.context.map(|context| BackendWrite {
                    context,
                    issue_id: v.issue_id,
                })
            })
        };
    }
    let value = match method {
        "manager_create_issue" => context!(pb::CreateIssueArgs),
        "manager_update_issue" => context!(pb::UpdateIssueArgs),
        "manager_transition_issue" => context!(pb::TransitionIssueArgs),
        "manager_claim_issue" => context!(pb::ClaimIssueArgs),
        "manager_heartbeat_issue_claim" => context!(pb::HeartbeatIssueClaimArgs),
        "manager_release_issue_claim" => context!(pb::ReleaseIssueClaimArgs),
        "manager_reconcile_expired_claim" => context!(pb::ReconcileExpiredClaimArgs),
        "manager_add_issue_relation" => pb::AddIssueRelationArgs::decode(args).ok().and_then(|v| {
            relation_anchor_for_add(v.kind).ok().and_then(|anchor| {
                v.context.map(|context| BackendWrite {
                    context,
                    issue_id: anchor.select(v.source_issue_id, v.target_issue_id),
                })
            })
        }),
        "manager_remove_issue_relation" => {
            pb::RemoveIssueRelationArgs::decode(args)
                .ok()
                .and_then(|v| {
                    let anchor = relation_anchor_for_remove(
                        &v.relation_id,
                        &v.source_issue_id,
                        &v.target_issue_id,
                    );
                    v.context.map(|context| BackendWrite {
                        context,
                        issue_id: anchor.select(v.source_issue_id, v.target_issue_id),
                    })
                })
        }
        "manager_add_issue_comment" => context!(pb::AddIssueCommentArgs),
        _ => None,
    };
    value
        .filter(|value| !value.issue_id.is_empty())
        .ok_or_else(|| domain_error("backend issue ID is required"))
}

#[cfg(test)]
mod operation_timestamp_tests {
    use super::*;

    #[test]
    fn payload_timestamps_are_used_independently_with_fallbacks() {
        let operation = pb::DurableOperation {
            created_at: Some(pb::UtcInstant {
                unix_epoch_millis: 11,
            }),
            updated_at: Some(pb::UtcInstant {
                unix_epoch_millis: 17,
            }),
            ..Default::default()
        };
        assert_eq!(
            operation_timestamps(&operation, 99),
            OperationTimestamps {
                created_at: 11,
                updated_at: 17,
            }
        );
        let missing_updated = pb::DurableOperation {
            created_at: Some(pb::UtcInstant {
                unix_epoch_millis: 11,
            }),
            ..Default::default()
        };
        assert_eq!(operation_timestamps(&missing_updated, 99).updated_at, 99);
    }
}
