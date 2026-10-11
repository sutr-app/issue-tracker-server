//! Trusted-consumer review candidate read model.
//!
//! The model deliberately keeps Candidate history immutable while deriving the
//! actionable view from manager mappings, backend Issue lifecycle, and durable
//! resolution operations.  It never performs a backend-wide Issue scan.

use super::{
    issue_mapping::IssueMappingRecord,
    operations::digest_bytes,
    pagination::{PageCursor, PageSizeLimits, PageTokenCodec},
    storage::{ManagerStorage, ManagerStorageError},
};
use crate::{
    error::TrackerError,
    proto_types::{manager_v1 as pb, v1},
    tracker::{Tracker, conversion::issue_proto},
};
use prost::Message;
use rusqlite::{OptionalExtension, params_from_iter};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use uuid::Uuid;

const ORDER_FINGERPRINT: &str = "candidate-id-asc";
const RESOLUTION_METHOD: &str = "manager_resolve_candidate";

#[derive(Debug, Error)]
pub(crate) enum ReviewCandidateReadError {
    #[error(transparent)]
    Storage(#[from] ManagerStorageError),
    #[error("review candidate projection integrity failure: {0}")]
    Integrity(String),
    #[error(transparent)]
    Tracker(#[from] TrackerError),
}

#[derive(Clone)]
enum MappingObservation {
    Missing,
    Invalid,
    Valid(pb::IssueMapping),
}

#[derive(Clone)]
pub(crate) struct ReviewCandidateReadSnapshot {
    pub(crate) generation: u64,
    candidates: Vec<pb::Candidate>,
    pending_candidates: HashSet<String>,
    mappings: HashMap<String, MappingObservation>,
    associated_issues: HashMap<String, crate::model::Issue>,
    missing_associated_issues: HashSet<String>,
    unknown_state_associated_issues: HashSet<String>,
    pub(crate) page_token_secret: String,
    pub(crate) page_size_limits: PageSizeLimits,
    pub(crate) query_fingerprint: String,
}

#[cfg(test)]
impl ReviewCandidateReadSnapshot {
    pub(crate) fn empty_for_test(generation: u64) -> Self {
        Self {
            generation,
            candidates: Vec::new(),
            pending_candidates: HashSet::new(),
            mappings: HashMap::new(),
            associated_issues: HashMap::new(),
            missing_associated_issues: HashSet::new(),
            unknown_state_associated_issues: HashSet::new(),
            page_token_secret: "secret".into(),
            page_size_limits: PageSizeLimits::new(2, 8),
            query_fingerprint: "query".into(),
        }
    }
}

pub(crate) fn snapshot_from_storage(
    storage: &ManagerStorage,
    args: &pb::ListReviewCandidatesArgs,
    page_size_limits: PageSizeLimits,
    manager_contract_fingerprint: &str,
) -> Result<ReviewCandidateReadSnapshot, ReviewCandidateReadError> {
    let stored_candidates: Vec<(Vec<u8>, String, String)> = storage.read(|connection| {
        let mut statement = connection.prepare(
            "SELECT payload, payload_schema, payload_digest
               FROM manager_entity
              WHERE entity_kind='candidate'
              ORDER BY entity_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(ManagerStorageError::from)
    })?;
    let mut candidates = Vec::with_capacity(stored_candidates.len());
    for (payload, schema, payload_digest) in stored_candidates {
        if schema != "issue_tracker.manager.v1.Candidate"
            || digest_bytes(&payload) != payload_digest
        {
            return Err(ReviewCandidateReadError::Integrity(
                "candidate payload schema or digest is invalid".into(),
            ));
        }
        let candidate = pb::Candidate::decode(payload.as_slice())
            .map_err(|error| ReviewCandidateReadError::Integrity(error.to_string()))?;
        if matches!(candidate.status, x if x == pb::CandidateStatus::Proposed as i32 || x == pb::CandidateStatus::AutoRegistered as i32)
            && candidate_matches(&candidate, args.filter.as_ref())
        {
            candidates.push(candidate);
        }
    }
    candidates.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));

    let candidate_ids = candidates
        .iter()
        .map(|candidate| candidate.candidate_id.clone())
        .collect::<Vec<_>>();
    let pending_candidates = read_pending_candidates(storage, &candidate_ids)?;
    let auto_ids = candidates
        .iter()
        .filter(|candidate| candidate.status == pb::CandidateStatus::AutoRegistered as i32)
        .map(|candidate| candidate.candidate_id.clone())
        .collect::<Vec<_>>();
    let mappings = read_mappings(storage, &auto_ids, manager_contract_fingerprint)?;

    Ok(ReviewCandidateReadSnapshot {
        generation: storage.generation(),
        candidates,
        pending_candidates,
        mappings,
        associated_issues: HashMap::new(),
        missing_associated_issues: HashSet::new(),
        unknown_state_associated_issues: HashSet::new(),
        page_token_secret: storage.list_issues_with_sync_page_token_secret().to_owned(),
        page_size_limits,
        query_fingerprint: query_fingerprint(args),
    })
}

pub(crate) async fn list_from_snapshot(
    tracker: &Tracker,
    snapshot: Arc<ReviewCandidateReadSnapshot>,
    args: &pb::ListReviewCandidatesArgs,
    pinned_read_registry: &Arc<StdMutex<super::read_session::PinnedReadRegistry>>,
) -> Result<pb::ReviewCandidatePage, ReviewCandidateReadError> {
    let supplied_token = args
        .page
        .as_ref()
        .and_then(|page| page.page_token.as_deref())
        .map(str::to_owned);
    let (mut snapshot, backend_generation, generation, snapshot_id, cursor) = match supplied_token
        .as_deref()
    {
        None => {
            let backend_generation = tracker.current_read_generation();
            let manager_generation = snapshot.generation;
            let generation =
                super::pagination::composite_generation(backend_generation, manager_generation);
            let snapshot_id =
                super::pagination::composite_snapshot_id(backend_generation, manager_generation);
            (snapshot, backend_generation, generation, snapshot_id, None)
        }
        Some(token) => {
            let codec = PageTokenCodec::new(snapshot.page_token_secret.clone());
            let cursor = codec.decode_authenticated(token)?;
            if cursor.snapshot_id.is_empty() {
                return Err(ReviewCandidateReadError::Tracker(
                    TrackerError::InvalidPageToken,
                ));
            }

            let current_backend_generation = tracker.current_read_generation();
            let current_generation = super::pagination::composite_generation(
                current_backend_generation,
                snapshot.generation,
            );
            // Boundary-mismatch expiry precedes pin validation, so its generation pair may come from the token/composite context rather than the pin; this is acceptable because non-generation expiry has no strict pair requirement.
            if cursor.order_fingerprint != ORDER_FINGERPRINT
                || cursor.query_fingerprint != snapshot.query_fingerprint
                || now_millis() > cursor.expires_at_unix_epoch_millis
            {
                return Err(ReviewCandidateReadError::Tracker(
                    TrackerError::PageTokenExpired {
                        token_generation: cursor.generation,
                        current_generation,
                    },
                ));
            }
            let pin = {
                let mut registry = pinned_read_registry.lock().map_err(|_| {
                    ReviewCandidateReadError::Integrity("pinned read registry is poisoned".into())
                })?;
                registry.lookup(&cursor.snapshot_id, &snapshot.query_fingerprint)
            };
            let Some(pin) = pin else {
                return Err(ReviewCandidateReadError::Tracker(
                    TrackerError::PageTokenExpired {
                        token_generation: cursor.generation,
                        current_generation,
                    },
                ));
            };
            if cursor.generation != pin.composite_generation {
                return Err(ReviewCandidateReadError::Tracker(
                    TrackerError::PageTokenExpired {
                        token_generation: cursor.generation,
                        current_generation: pin.composite_generation,
                    },
                ));
            }
            (
                pin.snapshot,
                pin.backend_generation,
                pin.composite_generation,
                cursor.snapshot_id.clone(),
                Some(cursor),
            )
        }
    };
    if cursor.is_none() {
        populate_associated_issue_snapshot(tracker, Arc::make_mut(&mut snapshot)).await?;
    }
    let manager_generation = snapshot.generation;
    let codec = PageTokenCodec::new(snapshot.page_token_secret.clone());
    let start = cursor.as_ref().map_or(0, |cursor| cursor.offset);
    if start > snapshot.candidates.len() {
        return Err(ReviewCandidateReadError::Tracker(
            TrackerError::InvalidPageToken,
        ));
    }
    let page_size = snapshot
        .page_size_limits
        .resolve(args.page.as_ref().map_or(0, |page| page.page_size));
    let mut rows = Vec::with_capacity(page_size);
    let mut next_offset = None;
    for (index, candidate) in snapshot.candidates.iter().enumerate().skip(start) {
        let Some(row) = build_row(&snapshot, candidate)? else {
            continue;
        };
        if rows.len() < page_size {
            rows.push(row);
        } else {
            next_offset = Some(index);
            break;
        }
    }
    if cursor.is_none() {
        tracker.validate_read_generation(backend_generation)?;
        let mut registry = pinned_read_registry.lock().map_err(|_| {
            ReviewCandidateReadError::Integrity("pinned read registry is poisoned".into())
        })?;
        registry.pin(
            snapshot_id.clone(),
            snapshot.query_fingerprint.clone(),
            Arc::clone(&snapshot),
            backend_generation,
            generation,
        );
    }

    let manager_snapshot = pb::PageSnapshot {
        read_revision_watermark: manager_generation,
        snapshot_id: format!("manager-{manager_generation}"),
        stable_traversal: Some(pb::StableTraversal {
            order_fingerprint: ORDER_FINGERPRINT.into(),
        }),
        read_generation: manager_generation,
    };
    let backend_snapshot = v1::PageSnapshot {
        read_revision_watermark: backend_generation,
        snapshot_id: format!("backend-{backend_generation}"),
        stable_traversal: Some(v1::StableTraversal {
            order_fingerprint: ORDER_FINGERPRINT.into(),
        }),
        read_generation: backend_generation,
    };
    let next_page_token = next_offset.map(|offset| {
        codec.encode(&PageCursor {
            offset,
            generation,
            snapshot_id: snapshot_id.clone(),
            order_fingerprint: ORDER_FINGERPRINT.into(),
            query_fingerprint: snapshot.query_fingerprint.clone(),
            expires_at_unix_epoch_millis: i64::MAX,
        })
    });
    Ok(pb::ReviewCandidatePage {
        rows,
        page: Some(pb::PageInfo {
            next_page_token,
            total_size: None,
            read_revision: manager_generation,
            snapshot: Some(manager_snapshot.clone()),
        }),
        composite_snapshot: Some(pb::CompositePageSnapshot {
            backend_snapshot: Some(backend_snapshot),
            manager_snapshot: Some(manager_snapshot),
        }),
    })
}

async fn populate_associated_issue_snapshot(
    tracker: &Tracker,
    snapshot: &mut ReviewCandidateReadSnapshot,
) -> Result<(), ReviewCandidateReadError> {
    let mut visited = HashSet::new();
    for mapping in snapshot
        .mappings
        .values()
        .filter_map(|observation| match observation {
            MappingObservation::Valid(mapping) => Some(mapping),
            MappingObservation::Missing | MappingObservation::Invalid => None,
        })
    {
        if !visited.insert(mapping.issue_id.clone()) {
            continue;
        }
        let Ok(id) = Uuid::parse_str(&mapping.issue_id) else {
            continue;
        };
        match tracker.get(id).await {
            Ok(issue) => {
                snapshot
                    .associated_issues
                    .insert(mapping.issue_id.clone(), issue);
            }
            Err(TrackerError::NotFound(_)) => {
                snapshot
                    .missing_associated_issues
                    .insert(mapping.issue_id.clone());
            }
            Err(TrackerError::InvalidState(message))
                if message.to_ascii_lowercase().contains("unknown state") =>
            {
                snapshot
                    .unknown_state_associated_issues
                    .insert(mapping.issue_id.clone());
            }
            Err(error) => return Err(ReviewCandidateReadError::Tracker(error)),
        }
    }
    Ok(())
}

fn build_row(
    snapshot: &ReviewCandidateReadSnapshot,
    candidate: &pb::Candidate,
) -> Result<Option<pb::ReviewCandidateRow>, ReviewCandidateReadError> {
    let pending = snapshot
        .pending_candidates
        .contains(&candidate.candidate_id);
    let resolution_state = if pending {
        pb::ReviewCandidateResolutionState::ResolutionPending as i32
    } else {
        pb::ReviewCandidateResolutionState::Actionable as i32
    };
    if candidate.status == pb::CandidateStatus::Proposed as i32 {
        return Ok(Some(review_row(
            candidate,
            None,
            None,
            pb::ReviewCandidateIntegrity::Normal as i32,
            resolution_state,
        )));
    }
    let observation = snapshot
        .mappings
        .get(&candidate.candidate_id)
        .cloned()
        .unwrap_or(MappingObservation::Missing);
    let (integrity, mapping, associated_issue, terminal) = match observation {
        MappingObservation::Missing => (
            pb::ReviewCandidateIntegrity::MappingMissing as i32,
            None,
            None,
            false,
        ),
        MappingObservation::Invalid => (
            pb::ReviewCandidateIntegrity::MappingInvalid as i32,
            None,
            None,
            false,
        ),
        MappingObservation::Valid(mapping) => {
            let _id = Uuid::parse_str(&mapping.issue_id).map_err(|_| {
                ReviewCandidateReadError::Integrity("mapping issue ID is invalid".into())
            })?;
            let Some(issue) = snapshot.associated_issues.get(&mapping.issue_id) else {
                if snapshot
                    .missing_associated_issues
                    .contains(&mapping.issue_id)
                {
                    return Ok(Some(review_row(
                        candidate,
                        Some(mapping),
                        None,
                        pb::ReviewCandidateIntegrity::IssueNotFound as i32,
                        resolution_state,
                    )));
                }
                if snapshot
                    .unknown_state_associated_issues
                    .contains(&mapping.issue_id)
                {
                    return Ok(Some(review_row(
                        candidate,
                        Some(mapping),
                        None,
                        pb::ReviewCandidateIntegrity::UnknownIssueState as i32,
                        resolution_state,
                    )));
                }
                // This is defensive and unreachable by construction because population covers every Valid mapping.
                return Err(ReviewCandidateReadError::Integrity(
                    "associated issue snapshot is missing".into(),
                ));
            };
            let proto = issue_proto(issue);
            if proto.issue_id != mapping.issue_id || proto.backend_id != mapping.backend_id {
                return Ok(Some(review_row(
                    candidate,
                    None,
                    None,
                    pb::ReviewCandidateIntegrity::MappingInvalid as i32,
                    resolution_state,
                )));
            }
            let state = match v1::IssueState::try_from(proto.state) {
                Ok(v1::IssueState::Unspecified) | Err(_) => {
                    return Ok(Some(review_row(
                        candidate,
                        Some(mapping),
                        None,
                        pb::ReviewCandidateIntegrity::UnknownIssueState as i32,
                        resolution_state,
                    )));
                }
                Ok(state) => state,
            };
            let terminal = matches!(state, v1::IssueState::Done | v1::IssueState::Canceled);
            (
                pb::ReviewCandidateIntegrity::Normal as i32,
                Some(mapping),
                Some(pb::ReviewIssueSummary {
                    issue_id: proto.issue_id,
                    lifecycle_state: proto.state,
                    revision: proto.revision,
                }),
                terminal,
            )
        }
    };
    // Terminal canonical Issues are never actionable, even while a stale or
    // in-flight resolution operation is present. `pending` only annotates rows
    // that remain eligible for display; it must not resurrect terminal history.
    if terminal {
        return Ok(None);
    }
    Ok(Some(review_row(
        candidate,
        mapping,
        associated_issue,
        integrity,
        resolution_state,
    )))
}

fn review_row(
    candidate: &pb::Candidate,
    mapping: Option<pb::IssueMapping>,
    associated_issue: Option<pb::ReviewIssueSummary>,
    integrity: i32,
    resolution_state: i32,
) -> pb::ReviewCandidateRow {
    pb::ReviewCandidateRow {
        candidate: Some(candidate.clone()),
        mapping,
        associated_issue,
        integrity,
        resolution_state,
    }
}

fn candidate_matches(
    candidate: &pb::Candidate,
    filter: Option<&pb::ReviewCandidateFilter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    super::candidates::candidate_matches_common(
        candidate,
        &filter.kinds,
        &filter.priorities,
        filter.extraction_run_id.as_deref(),
        filter.text_query.as_deref(),
        filter.source_evidence.as_ref(),
    )
}

fn query_fingerprint(args: &pb::ListReviewCandidatesArgs) -> String {
    let filter = args
        .filter
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    let context = args
        .context
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    digest_bytes(
        &[
            b"manager_list_review_candidates\0",
            filter.as_slice(),
            b"\0",
            context.as_slice(),
        ]
        .concat(),
    )
}

fn read_pending_candidates(
    storage: &ManagerStorage,
    candidate_ids: &[String],
) -> Result<HashSet<String>, ReviewCandidateReadError> {
    let mut pending = HashSet::new();
    for batch in candidate_ids.chunks(400) {
        pending.extend(read_pending_batch(storage, batch)?);
    }
    Ok(pending)
}

fn read_pending_batch(
    storage: &ManagerStorage,
    candidate_ids: &[String],
) -> Result<HashSet<String>, ReviewCandidateReadError> {
    if candidate_ids.is_empty() {
        return Ok(HashSet::new());
    }
    let placeholders = std::iter::repeat_n("?", candidate_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT scope.candidate_id, operation.operation_payload,
                operation.operation_payload_digest
           FROM manager_operation_scope scope
           LEFT JOIN manager_operation operation ON operation.operation_id=scope.operation_id
          WHERE scope.candidate_id IN ({placeholders})"
    );
    let rows = storage.read(|connection| {
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(candidate_ids.iter()), |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(ManagerStorageError::from)
    })?;
    let mut pending = HashSet::new();
    for (candidate_id, payload, payload_digest) in rows {
        let candidate_id = candidate_id.ok_or_else(|| {
            ReviewCandidateReadError::Integrity("operation scope candidate is missing".into())
        })?;
        let payload = payload.ok_or_else(|| {
            ReviewCandidateReadError::Integrity("operation payload is missing".into())
        })?;
        let payload_digest = payload_digest.ok_or_else(|| {
            ReviewCandidateReadError::Integrity("operation payload digest is missing".into())
        })?;
        if digest_bytes(&payload) != payload_digest {
            return Err(ReviewCandidateReadError::Integrity(
                "operation payload digest is invalid".into(),
            ));
        }
        let operation = pb::DurableOperation::decode(payload.as_slice())
            .map_err(|error| ReviewCandidateReadError::Integrity(error.to_string()))?;
        let scoped_id = match operation.scope.as_ref() {
            Some(pb::durable_operation::Scope::CandidateId(value)) if value == &candidate_id => {
                value
            }
            _ => {
                return Err(ReviewCandidateReadError::Integrity(
                    "operation scope does not match index".into(),
                ));
            }
        };
        if operation.method != RESOLUTION_METHOD {
            continue;
        }
        if operation_pending(&operation) {
            pending.insert(scoped_id.clone());
        }
    }
    Ok(pending)
}

fn operation_pending(operation: &pb::DurableOperation) -> bool {
    match pb::DurableOperationState::try_from(operation.state) {
        Ok(pb::DurableOperationState::Committed | pb::DurableOperationState::Arbitrated) => false,
        Ok(pb::DurableOperationState::Failed)
            if pb::FailureClass::try_from(operation.failure_class)
                .is_ok_and(|class| class == pb::FailureClass::Terminal) =>
        {
            false
        }
        Ok(pb::DurableOperationState::Failed)
            if pb::FailureClass::try_from(operation.failure_class)
                .is_ok_and(|class| class == pb::FailureClass::Retryable) =>
        {
            true
        }
        _ => true,
    }
}

fn read_mappings(
    storage: &ManagerStorage,
    candidate_ids: &[String],
    manager_contract_fingerprint: &str,
) -> Result<HashMap<String, MappingObservation>, ReviewCandidateReadError> {
    let mut result = HashMap::new();
    for batch in candidate_ids.chunks(400) {
        result.extend(read_mappings_batch(
            storage,
            batch,
            manager_contract_fingerprint,
        )?);
    }
    Ok(result)
}

fn read_mappings_batch(
    storage: &ManagerStorage,
    candidate_ids: &[String],
    manager_contract_fingerprint: &str,
) -> Result<HashMap<String, MappingObservation>, ReviewCandidateReadError> {
    let mut result = candidate_ids
        .iter()
        .cloned()
        .map(|id| (id, MappingObservation::Missing))
        .collect::<HashMap<_, _>>();
    if candidate_ids.is_empty() {
        return Ok(result);
    }
    let placeholders = std::iter::repeat_n("?", candidate_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT candidate_id, issue_id FROM manager_issue_mapping_index WHERE candidate_id IN ({placeholders})"
    );
    let indexed = storage.read(|connection| {
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(candidate_ids.iter()), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(ManagerStorageError::from)
    })?;
    for (candidate_id, issue_id) in indexed {
        let payload = storage.read(|connection| {
            connection.query_row(
                "SELECT payload, payload_schema, payload_digest FROM manager_entity WHERE entity_kind='issue_mapping' AND entity_id=?1",
                [&issue_id],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
            ).optional().map_err(ManagerStorageError::from)
        })?;
        let observation = match payload {
            Some((payload, schema, digest))
                if schema == "issue_tracker.manager.v1.IssueMapping"
                    && digest_bytes(&payload) == digest =>
            {
                match pb::IssueMapping::decode(payload.as_slice()) {
                    Ok(mapping)
                        if mapping.issue_id == issue_id
                            && mapping.candidate_id.as_deref() == Some(candidate_id.as_str())
                            && valid_mapping(
                                &mapping,
                                storage.backend_contract_fingerprint(),
                                manager_contract_fingerprint,
                            ) =>
                    {
                        MappingObservation::Valid(mapping)
                    }
                    _ => MappingObservation::Invalid,
                }
            }
            _ => MappingObservation::Invalid,
        };
        result.insert(candidate_id, observation);
    }
    Ok(result)
}

fn valid_mapping(
    mapping: &pb::IssueMapping,
    backend_fingerprint: &str,
    manager_fingerprint: &str,
) -> bool {
    canonical_uuid(&mapping.issue_id)
        && canonical_uuid(&mapping.backend_id)
        && mapping.backend_contract_fingerprint == backend_fingerprint
        && mapping.manager_contract_fingerprint == manager_fingerprint
        && IssueMappingRecord::from_proto(mapping).is_ok()
}

pub(crate) fn canonical_uuid(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().try_into().unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_operation_states_are_fail_closed() {
        let mut operation = pb::DurableOperation {
            state: pb::DurableOperationState::Committed as i32,
            ..Default::default()
        };
        assert!(!operation_pending(&operation));
        operation.state = pb::DurableOperationState::Failed as i32;
        operation.failure_class = pb::FailureClass::Terminal as i32;
        assert!(!operation_pending(&operation));
        operation.failure_class = pb::FailureClass::Retryable as i32;
        assert!(operation_pending(&operation));
        operation.failure_class = pb::FailureClass::Unspecified as i32;
        assert!(operation_pending(&operation));
    }

    #[test]
    fn review_filter_matches_existing_candidate_filter_semantics() {
        let candidate = pb::Candidate {
            title: "Fix Login".into(),
            description: "important".into(),
            kind: pb::CandidateKind::Bug as i32,
            suggested_priority: pb::CandidatePriority::P1 as i32,
            ..Default::default()
        };
        let filter = pb::ReviewCandidateFilter {
            text_query: Some("login".into()),
            ..Default::default()
        };
        assert!(candidate_matches(&candidate, Some(&filter)));
    }
}
