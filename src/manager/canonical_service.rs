//! Internal composition of canonical Tracker reads and manager coordination.
//!
//! This module intentionally does not implement the synchronous gateway trait:
//! Tracker reads are asynchronous and the composition root owns that bridge.
//! It never writes an Issue payload into manager storage.

use super::{
    issue_mapping::IssueMappingRecord,
    pagination::{PageCursor, PageSizeLimits, PageTokenCodec, decode_bound_page_token, paginate},
    storage::{ManagerStorage, ManagerStorageError},
    sync::{
        self, CanonicalIssueState, ConfirmationStatus, CreationBasis, ReadyFacts, SyncOperation,
    },
};

pub(crate) use super::pagination::{composite_generation, composite_snapshot_id};
use crate::{
    proto_types::{manager_v1 as pb, v1},
    tracker::{Tracker, conversion::issue_proto},
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub(crate) enum CanonicalServiceError {
    #[error("invalid issue ID")]
    InvalidIssueId,
    #[error(transparent)]
    Tracker(#[from] crate::error::TrackerError),
    #[error(transparent)]
    Storage(#[from] ManagerStorageError),
    #[error("stored issue mapping is invalid: {0}")]
    MappingDecode(String),
    #[error("stored durable operation cannot be selected: {0}")]
    Sync(String),
}

/// Async read service. The manager store contributes only operations/mappings;
/// the Issue itself always comes directly from `Tracker`.
#[allow(dead_code)]
pub(crate) struct CanonicalReadService<'a> {
    tracker: &'a Tracker,
    storage: &'a ManagerStorage,
    page_size_limits: PageSizeLimits,
}

/// Manager data captured before an asynchronous Tracker read starts.
///
/// Keeping only owned protobuf values here lets callers release the manager's
/// SQLite guard before awaiting the Tracker operation.
pub(crate) struct CanonicalReadSnapshot {
    generation: u64,
    operations: Vec<pb::DurableOperation>,
    mappings: HashMap<String, Vec<u8>>,
}

impl CanonicalReadSnapshot {
    pub(crate) fn composite_generation(&self, backend_generation: u64) -> u64 {
        composite_generation(backend_generation, self.generation)
    }

    pub(crate) fn composite_snapshot_id(&self, backend_generation: u64) -> String {
        composite_snapshot_id(backend_generation, self.generation)
    }
}

#[allow(dead_code)]
impl<'a> CanonicalReadService<'a> {
    pub(crate) fn new(
        tracker: &'a Tracker,
        storage: &'a ManagerStorage,
        page_size_limits: PageSizeLimits,
    ) -> Self {
        Self {
            tracker,
            storage,
            page_size_limits,
        }
    }

    pub(crate) async fn get_issue_with_sync(
        &self,
        issue_id: &str,
    ) -> Result<pb::IssueWithSync, CanonicalServiceError> {
        let snapshot = self.snapshot()?;
        Self::get_issue_with_sync_from_snapshot(self.tracker, &snapshot, issue_id).await
    }

    pub(crate) async fn get_issue_with_sync_from_snapshot(
        tracker: &Tracker,
        snapshot: &CanonicalReadSnapshot,
        issue_id: &str,
    ) -> Result<pb::IssueWithSync, CanonicalServiceError> {
        let id = Uuid::parse_str(issue_id).map_err(|_| CanonicalServiceError::InvalidIssueId)?;
        let issue = tracker.get(id).await?;
        let backend_lookback_ready = tracker
            .ready(now_seconds())
            .await?
            .iter()
            .any(|ready| ready.id == issue.id);
        Self::compose(issue_proto(&issue), snapshot, backend_lookback_ready)
    }

    pub(crate) async fn list_issues_with_sync(
        &self,
        args: &pb::ListIssuesWithSyncArgs,
    ) -> Result<pb::IssueWithSyncPage, CanonicalServiceError> {
        let snapshot = self.snapshot()?;
        let page_token_secret = list_page_token_secret(self.storage);
        Self::list_issues_with_sync_from_snapshot(
            self.tracker,
            &page_token_secret,
            &snapshot,
            args,
            self.page_size_limits,
        )
        .await
    }

    /// Reads backend Issues and manager projection data at independently
    /// recorded boundaries. The result advertises both boundaries because the
    /// stores cannot be read under one cross-store transaction.
    pub(crate) async fn list_issues_with_sync_from_snapshot(
        tracker: &Tracker,
        page_token_secret: &str,
        snapshot: &CanonicalReadSnapshot,
        args: &pb::ListIssuesWithSyncArgs,
        page_size_limits: PageSizeLimits,
    ) -> Result<pb::IssueWithSyncPage, CanonicalServiceError> {
        let backend_generation = tracker.current_read_generation();
        let issues = tracker.list().await?;
        let ready_issue_ids = tracker
            .ready(now_seconds())
            .await?
            .into_iter()
            .map(|issue| issue.id.to_string())
            .collect::<HashSet<_>>();
        tracker.validate_read_generation(backend_generation)?;

        let mut views = issues
            .into_iter()
            .map(|issue| {
                let backend_lookback_ready = ready_issue_ids.contains(&issue.id.to_string());
                Self::compose(issue_proto(&issue), snapshot, backend_lookback_ready)
            })
            .collect::<Result<Vec<_>, _>>()?;
        views = filter_and_sort_views(views, args);

        let manager_generation = snapshot.generation;
        let generation = composite_generation(backend_generation, manager_generation);
        let snapshot_id = composite_snapshot_id(backend_generation, manager_generation);
        let order_fingerprint = issue_order_fingerprint(args.sort);
        let query_fingerprint = list_query_fingerprint(args);
        let codec = list_page_token_codec(page_token_secret);
        let cursor = args
            .page
            .as_ref()
            .and_then(|page| page.page_token.as_deref())
            .map(|token| {
                decode_list_page_token(
                    &codec,
                    token,
                    generation,
                    &snapshot_id,
                    order_fingerprint,
                    &query_fingerprint,
                )
            })
            .transpose()?;
        let page_size = list_page_size(args, page_size_limits);
        let (start, end, has_more) = paginate(&views, page_size, cursor.as_ref());
        let next_page_token = has_more.then(|| {
            codec.encode(&PageCursor {
                offset: end,
                generation,
                snapshot_id: snapshot_id.clone(),
                order_fingerprint: order_fingerprint.into(),
                query_fingerprint: query_fingerprint.clone(),
                expires_at_unix_epoch_millis: i64::MAX,
            })
        });
        let backend_revision_watermark = views
            .iter()
            .filter_map(|view| view.issue.as_ref().map(|issue| issue.revision))
            .max()
            .unwrap_or_default();
        let backend_snapshot = v1::PageSnapshot {
            read_revision_watermark: backend_revision_watermark,
            snapshot_id: format!("backend-{backend_generation}"),
            stable_traversal: Some(v1::StableTraversal {
                order_fingerprint: order_fingerprint.into(),
            }),
            read_generation: backend_generation,
        };
        let manager_snapshot = pb::PageSnapshot {
            read_revision_watermark: manager_generation,
            snapshot_id: format!("manager-{manager_generation}"),
            stable_traversal: Some(pb::StableTraversal {
                order_fingerprint: order_fingerprint.into(),
            }),
            read_generation: manager_generation,
        };
        Ok(pb::IssueWithSyncPage {
            issues: views[start..end].to_vec(),
            page: Some(pb::PageInfo {
                next_page_token,
                total_size: Some(views.len() as u64),
                read_revision: manager_generation,
                snapshot: Some(manager_snapshot.clone()),
            }),
            composite_snapshot: Some(pb::CompositePageSnapshot {
                backend_snapshot: Some(backend_snapshot),
                manager_snapshot: Some(manager_snapshot),
            }),
        })
    }

    fn compose(
        issue: v1::Issue,
        snapshot: &CanonicalReadSnapshot,
        backend_lookback_ready: bool,
    ) -> Result<pb::IssueWithSync, CanonicalServiceError> {
        let sync_operations = snapshot
            .operations
            .iter()
            .map(SyncOperation::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| CanonicalServiceError::Sync(error.to_string()))?;
        let selection =
            sync::select_sync_state(&sync_operations, &issue.issue_id, snapshot.generation);
        let sync_state = sync_state_proto(selection.state);
        let state = canonical_state(issue.state).ok_or_else(|| {
            CanonicalServiceError::Sync("canonical issue state is invalid".into())
        })?;
        let selected = selection.evidence.selected_operation_id.clone();
        let operation_ids = selection.evidence.chain_head_operation_ids.clone();
        let mapping = snapshot.mapping(&issue.issue_id)?;
        let ready = combined_lookback_ready(
            backend_lookback_ready,
            &ReadyFacts {
                state,
                sync_state: selection.state,
                has_open_blocker: false, // Tracker.ready already applied the canonical blocker traversal.
                has_active_claim: issue
                    .claim
                    .as_ref()
                    .is_some_and(|claim| claim.lease_expires_at.is_some()),
                claim_result_unconfirmed: false,
                creation_basis: creation_basis(issue.creation_basis),
                confirmation_status: confirmation_status(issue.confirmation_status),
            },
        );
        Ok(pb::IssueWithSync {
            issue: Some(issue.clone()),
            sync: Some(pb::IssueSyncView {
                issue_id: issue.issue_id,
                sync_state,
                sync_subject_generation: selection.evidence.generation,
                operation_ids,
                resolution_required: matches!(
                    selection.state,
                    sync::SyncState::Unconfirmed | sync::SyncState::Failed
                ),
                selected_operation_id: selected,
                causal_root_operation_id: selection.evidence.causal_root_operation_id.clone(),
                selected_manager_sequence: selection.evidence.manager_sequence,
                selection_evidence: Some(selection.evidence.to_proto()),
                ..Default::default()
            }),
            lookback_ready: ready,
            mapping,
        })
    }

    pub(crate) fn snapshot(&self) -> Result<CanonicalReadSnapshot, CanonicalServiceError> {
        CanonicalReadSnapshot::from_storage(self.storage)
    }

    fn operations(&self) -> Result<Vec<pb::DurableOperation>, CanonicalServiceError> {
        self.storage
            .read(|connection| {
                let mut statement =
                    connection.prepare("SELECT operation_payload FROM manager_operation")?;
                let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
                rows.map(|row| {
                    let bytes = row?;
                    pb::DurableOperation::decode(bytes.as_slice())
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(ManagerStorageError::from)
            })
            .map_err(Into::into)
    }
}

impl CanonicalReadSnapshot {
    pub(crate) fn from_storage(
        storage: &ManagerStorage,
    ) -> Result<CanonicalReadSnapshot, CanonicalServiceError> {
        let operations = storage.read(|connection| {
            let mut statement =
                connection.prepare("SELECT operation_payload FROM manager_operation")?;
            let rows = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
            rows.map(|row| {
                let bytes = row?;
                pb::DurableOperation::decode(bytes.as_slice())
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(ManagerStorageError::from)
        })?;
        let mappings = storage.read(|connection| {
            let mut statement = connection.prepare(
                "SELECT entity_id, payload FROM manager_entity WHERE entity_kind = 'issue_mapping'",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?;
            rows.collect::<Result<HashMap<_, _>, rusqlite::Error>>()
                .map_err(ManagerStorageError::from)
        })?;
        Ok(CanonicalReadSnapshot {
            generation: storage.generation(),
            operations,
            mappings,
        })
    }
}

impl CanonicalReadSnapshot {
    fn mapping(&self, issue_id: &str) -> Result<Option<pb::IssueMapping>, CanonicalServiceError> {
        self.mappings
            .get(issue_id)
            .map(|bytes| decode_mapping(bytes))
            .transpose()
    }
}

fn decode_mapping(bytes: &[u8]) -> Result<pb::IssueMapping, CanonicalServiceError> {
    let mapping = pb::IssueMapping::decode(bytes)
        .map_err(|error| CanonicalServiceError::MappingDecode(error.to_string()))?;
    IssueMappingRecord::from_proto(&mapping)
        .map_err(|error| CanonicalServiceError::MappingDecode(error.to_string()))?;
    Ok(mapping)
}

fn combined_lookback_ready(backend_lookback_ready: bool, facts: &ReadyFacts) -> bool {
    backend_lookback_ready && sync::is_lookback_ready(facts)
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn filter_and_sort_views(
    mut views: Vec<pb::IssueWithSync>,
    args: &pb::ListIssuesWithSyncArgs,
) -> Vec<pb::IssueWithSync> {
    views.retain(|view| {
        view.issue
            .as_ref()
            .is_some_and(|issue| issue_matches(issue, args.issue_filter.as_ref()))
            && view_matches_sync_filter(view, args.sync_filter.as_ref())
    });
    match v1::IssueSort::try_from(args.sort).unwrap_or(v1::IssueSort::Unspecified) {
        v1::IssueSort::UpdatedDesc => views.sort_by_key(|view| {
            (
                std::cmp::Reverse(
                    view.issue
                        .as_ref()
                        .and_then(|issue| issue.updated_at.as_ref())
                        .map_or(i64::MIN, |instant| instant.unix_epoch_millis),
                ),
                issue_id_of(view),
            )
        }),
        v1::IssueSort::CreatedDesc => views.sort_by_key(|view| {
            (
                std::cmp::Reverse(
                    view.issue
                        .as_ref()
                        .and_then(|issue| issue.created_at.as_ref())
                        .map_or(i64::MIN, |instant| instant.unix_epoch_millis),
                ),
                issue_id_of(view),
            )
        }),
        v1::IssueSort::CreatedAsc => views.sort_by_key(|view| {
            (
                view.issue
                    .as_ref()
                    .and_then(|issue| issue.created_at.as_ref())
                    .map_or(i64::MAX, |instant| instant.unix_epoch_millis),
                issue_id_of(view),
            )
        }),
        v1::IssueSort::CanonicalReady | v1::IssueSort::Unspecified => {
            views.sort_by_key(issue_id_of);
        }
    }
    views
}

fn issue_id_of(view: &pb::IssueWithSync) -> String {
    view.issue
        .as_ref()
        .map(|issue| issue.issue_id.clone())
        .unwrap_or_default()
}

fn issue_matches(issue: &v1::Issue, filter: Option<&v1::IssueFilter>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    (filter.states.is_empty() || filter.states.contains(&issue.state))
        && (filter.kinds.is_empty() || filter.kinds.contains(&issue.kind))
        && (filter.priorities.is_empty() || filter.priorities.contains(&issue.priority))
        && (filter.creation_bases.is_empty()
            || filter.creation_bases.contains(&issue.creation_basis))
        && (filter.confirmation_statuses.is_empty()
            || filter
                .confirmation_statuses
                .contains(&issue.confirmation_status))
        && filter
            .labels_all
            .iter()
            .all(|label| issue.labels.contains(label))
        && filter
            .contexts
            .iter()
            .all(|context| issue.contexts.contains(context))
        && filter.text_query.as_ref().is_none_or(|query| {
            let query = query.to_ascii_lowercase();
            issue.title.to_ascii_lowercase().contains(&query)
                || issue.description.to_ascii_lowercase().contains(&query)
        })
        && filter.due_before.as_ref().is_none_or(|until| {
            issue
                .due_at
                .as_ref()
                .is_some_and(|due| due.unix_epoch_millis <= until.unix_epoch_millis)
        })
        && filter.updated_before.as_ref().is_none_or(|until| {
            issue
                .updated_at
                .as_ref()
                .is_some_and(|updated| updated.unix_epoch_millis <= until.unix_epoch_millis)
        })
}

fn view_matches_sync_filter(
    view: &pb::IssueWithSync,
    filter: Option<&pb::IssueWithSyncFilter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let sync = view.sync.as_ref();
    (filter.sync_states.is_empty()
        || sync.is_some_and(|sync| filter.sync_states.contains(&sync.sync_state)))
        && (filter.operation_ids.is_empty()
            || sync.is_some_and(|sync| {
                sync.operation_ids
                    .iter()
                    .any(|operation_id| filter.operation_ids.contains(operation_id))
            }))
        && filter
            .lookback_ready
            .is_none_or(|expected| view.lookback_ready == expected)
}

fn issue_order_fingerprint(sort: i32) -> &'static str {
    match v1::IssueSort::try_from(sort).unwrap_or(v1::IssueSort::Unspecified) {
        v1::IssueSort::UpdatedDesc => "updated-desc",
        v1::IssueSort::CreatedDesc => "created-desc",
        v1::IssueSort::CreatedAsc => "created-asc",
        v1::IssueSort::CanonicalReady | v1::IssueSort::Unspecified => "issue-id",
    }
}

fn list_query_fingerprint(args: &pb::ListIssuesWithSyncArgs) -> String {
    let issue_filter = args
        .issue_filter
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    let sync_filter = args
        .sync_filter
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    format!(
        "{:x}",
        Sha256::digest(
            [
                b"list-issues-with-sync\0".as_slice(),
                args.sort.to_string().as_bytes(),
                b"\0",
                issue_filter.as_slice(),
                b"\0",
                sync_filter.as_slice(),
            ]
            .concat()
        )
    )
}

pub(crate) fn list_page_token_secret(storage: &ManagerStorage) -> String {
    storage.list_issues_with_sync_page_token_secret().to_owned()
}

fn list_page_token_codec(secret: &str) -> PageTokenCodec {
    PageTokenCodec::new(secret)
}

fn decode_list_page_token(
    codec: &PageTokenCodec,
    token: &str,
    expected_generation: u64,
    expected_snapshot_id: &str,
    expected_order_fingerprint: &str,
    expected_query_fingerprint: &str,
) -> Result<PageCursor, crate::error::TrackerError> {
    decode_bound_page_token(
        codec,
        token,
        expected_generation,
        expected_snapshot_id,
        expected_order_fingerprint,
        expected_query_fingerprint,
        now_millis(),
    )
}

#[cfg(test)]
fn list_page_window(
    length: usize,
    page_size: usize,
    cursor: Option<&PageCursor>,
) -> (usize, usize, bool) {
    paginate(&vec![(); length], page_size, cursor)
}

fn list_page_size(args: &pb::ListIssuesWithSyncArgs, page_size_limits: PageSizeLimits) -> usize {
    page_size_limits.resolve(args.page.as_ref().map_or(0, |page| page.page_size))
}
fn sync_state_proto(value: sync::SyncState) -> i32 {
    match value {
        sync::SyncState::Confirmed => pb::SyncState::Confirmed as i32,
        sync::SyncState::Pending => pb::SyncState::Pending as i32,
        sync::SyncState::Unconfirmed => pb::SyncState::Unconfirmed as i32,
        sync::SyncState::Failed => pb::SyncState::Failed as i32,
    }
}
pub(crate) fn canonical_state(value: i32) -> Option<CanonicalIssueState> {
    match v1::IssueState::try_from(value).ok()? {
        v1::IssueState::Open => Some(CanonicalIssueState::Open),
        v1::IssueState::InProgress => Some(CanonicalIssueState::InProgress),
        v1::IssueState::Blocked => Some(CanonicalIssueState::Blocked),
        v1::IssueState::Deferred => Some(CanonicalIssueState::Deferred),
        v1::IssueState::Done => Some(CanonicalIssueState::Done),
        v1::IssueState::Canceled => Some(CanonicalIssueState::Canceled),
        v1::IssueState::AwaitingReview => Some(CanonicalIssueState::AwaitingReview),
        v1::IssueState::Unspecified => None,
    }
}
fn creation_basis(value: i32) -> CreationBasis {
    match v1::CreationBasis::try_from(value).unwrap_or(v1::CreationBasis::Unspecified) {
        v1::CreationBasis::AutoDetected => CreationBasis::AutoDetected,
        v1::CreationBasis::Imported => CreationBasis::Imported,
        _ => CreationBasis::Explicit,
    }
}
fn confirmation_status(value: i32) -> ConfirmationStatus {
    if value == v1::ConfirmationStatus::Confirmed as i32 {
        ConfirmationStatus::Confirmed
    } else {
        ConfirmationStatus::Unreviewed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue(id: &str, revision: u64, title: &str) -> v1::Issue {
        v1::Issue {
            issue_id: id.into(),
            revision,
            title: title.into(),
            state: v1::IssueState::Open as i32,
            kind: v1::IssueKind::Task as i32,
            priority: v1::IssuePriority::P1 as i32,
            confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
            updated_at: Some(v1::UtcInstant {
                unix_epoch_millis: revision as i64,
            }),
            ..Default::default()
        }
    }

    fn view(
        issue: v1::Issue,
        sync_state: pb::SyncState,
        lookback_ready: bool,
    ) -> pb::IssueWithSync {
        pb::IssueWithSync {
            issue: Some(issue),
            sync: Some(pb::IssueSyncView {
                sync_state: sync_state as i32,
                operation_ids: vec!["op-1".into()],
                ..Default::default()
            }),
            lookback_ready,
            ..Default::default()
        }
    }

    #[test]
    fn backend_readiness_is_required_for_an_individual_canonical_read() {
        let facts = ReadyFacts {
            state: CanonicalIssueState::Open,
            sync_state: sync::SyncState::Confirmed,
            has_open_blocker: false,
            has_active_claim: false,
            claim_result_unconfirmed: false,
            creation_basis: CreationBasis::Explicit,
            confirmation_status: ConfirmationStatus::Confirmed,
        };
        assert!(combined_lookback_ready(true, &facts));
        assert!(!combined_lookback_ready(false, &facts));
    }

    #[test]
    fn malformed_persisted_mapping_is_rejected_before_composition() {
        let mapping = pb::IssueMapping {
            issue_id: "issue-1".into(),
            backend_id: "".into(),
            revision: 0,
            backend_contract_fingerprint: "backend-v1".into(),
            manager_contract_fingerprint: "manager-v1".into(),
            ..Default::default()
        };
        assert!(matches!(
            decode_mapping(&mapping.encode_to_vec()),
            Err(CanonicalServiceError::MappingDecode(_))
        ));
    }

    #[test]
    fn list_with_sync_applies_issue_and_sync_filters_conjunctively() {
        let matching = view(issue("issue-a", 2, "Alpha"), pb::SyncState::Confirmed, true);
        let wrong_sync = view(issue("issue-b", 3, "Alpha"), pb::SyncState::Pending, true);
        let wrong_issue = view(issue("issue-c", 4, "Beta"), pb::SyncState::Confirmed, true);
        let args = pb::ListIssuesWithSyncArgs {
            issue_filter: Some(v1::IssueFilter {
                text_query: Some("alpha".into()),
                ..Default::default()
            }),
            sync_filter: Some(pb::IssueWithSyncFilter {
                sync_states: vec![pb::SyncState::Confirmed as i32],
                lookback_ready: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = filter_and_sort_views(vec![wrong_sync, matching.clone(), wrong_issue], &args);

        assert_eq!(result, vec![matching]);
    }

    #[test]
    fn list_with_sync_uses_requested_sort_and_page_window() {
        let oldest = view(issue("issue-a", 1, "A"), pb::SyncState::Confirmed, true);
        let newest = view(issue("issue-b", 2, "B"), pb::SyncState::Confirmed, true);
        let args = pb::ListIssuesWithSyncArgs {
            sort: v1::IssueSort::UpdatedDesc as i32,
            ..Default::default()
        };
        let values = filter_and_sort_views(vec![oldest.clone(), newest.clone()], &args);
        let (start, end, has_more) = list_page_window(values.len(), 1, None);

        assert_eq!(values, vec![newest, oldest]);
        assert_eq!((start, end, has_more), (0, 1, true));
    }

    #[test]
    fn list_with_sync_page_size_uses_the_default_and_clamps_to_the_maximum() {
        let limits = PageSizeLimits::new(2, 3);
        assert_eq!(
            list_page_size(&pb::ListIssuesWithSyncArgs::default(), limits),
            2
        );
        assert_eq!(
            list_page_size(
                &pb::ListIssuesWithSyncArgs {
                    page: Some(pb::PageRequest {
                        page_size: u32::MAX,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                limits,
            ),
            3
        );
    }

    #[test]
    fn continuation_token_expires_when_either_read_boundary_changes() {
        let codec = list_page_token_codec("test-secret");
        let cursor = super::PageCursor {
            offset: 1,
            generation: composite_generation(7, 11),
            snapshot_id: composite_snapshot_id(7, 11),
            order_fingerprint: "updated-desc".into(),
            query_fingerprint: "query".into(),
            expires_at_unix_epoch_millis: i64::MAX,
        };
        let token = codec.encode(&cursor);

        assert!(matches!(
            decode_list_page_token(
                &codec,
                &token,
                composite_generation(8, 11),
                &composite_snapshot_id(8, 11),
                "updated-desc",
                "query",
            ),
            Err(crate::error::TrackerError::PageTokenExpired { .. })
        ));
        assert!(matches!(
            decode_list_page_token(
                &codec,
                &token,
                composite_generation(7, 12),
                &composite_snapshot_id(7, 12),
                "updated-desc",
                "query",
            ),
            Err(crate::error::TrackerError::PageTokenExpired { .. })
        ));
    }
}
