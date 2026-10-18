use super::{
    actor::{persisted_kind, persisted_kind_value},
    conversion::{instant, issue_state},
    issue_write::{LeaseState, lease_state, lease_state_from_attributes},
    *,
};

/// Minimal raw lease view used by resident maintenance. Unlike [`Issue`], it
/// deliberately does not decode metadata, history, or relations, so one
/// malformed Issue projection cannot prevent other expired leases recovering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LeaseMaintenanceCandidate {
    /// Physical TaskChampion identity used for the maintenance cursor.
    /// Logical Issue IDs are user data and must not determine pagination.
    pub(crate) storage_id: Uuid,
    pub(crate) id: Uuid,
    pub(crate) revision: u64,
    pub(crate) state: IssueState,
    pub(crate) expires_at: Option<u64>,
    pub(crate) lease_state: LeaseState,
}

/// One raw maintenance-scan entry.  Maintenance deliberately reads less than
/// a complete Issue projection so corrupt metadata or history on one task
/// cannot hide later expired claims from the cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeaseMaintenanceScanEntry {
    Valid(LeaseMaintenanceCandidate),
    /// A physical row was consumed by the bounded scan but is not an Issue
    /// projection (or has disappeared between UUID enumeration and read).
    /// It is intentionally not an error: the cursor must still advance past
    /// it so unrelated Issue rows cannot be starved by metadata tasks.
    Skipped {
        storage_id: Uuid,
    },
    Invalid {
        storage_id: Uuid,
        issue_id: Uuid,
        classification: &'static str,
    },
}

/// A bounded physical-UUID page and whether it reached the end of the
/// current UUID sweep.  The end marker is required for readiness: a page
/// whose length equals the batch size may still be the final page when the
/// number of rows is an exact multiple of that size.
#[derive(Debug)]
pub(crate) struct LeaseMaintenanceScanPage {
    pub(crate) entries: Vec<LeaseMaintenanceScanEntry>,
    pub(crate) reached_end: bool,
}

impl LeaseMaintenanceScanEntry {
    pub(crate) fn cursor_id(self) -> Uuid {
        match self {
            Self::Valid(candidate) => candidate.storage_id,
            Self::Skipped { storage_id } => storage_id,
            Self::Invalid { storage_id, .. } => storage_id,
        }
    }
}

impl Tracker {
    pub async fn list_comments(&self, id: Uuid) -> Result<Vec<v1::IssueComment>, TrackerError> {
        self.ensure_healthy()?;
        let mut replica = self.inner.lock().await;
        let task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let mut comments = Vec::new();
        for (key, value) in task.get_user_defined_attributes() {
            if !key.starts_with("lb.comment.") {
                continue;
            }
            let record: Value = serde_json::from_str(value)
                .map_err(|error| TrackerError::InvalidState(format!("invalid comment: {error}")))?;
            let actor_id = record
                .get("actor")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned();
            let actor_kind = persisted_kind(&record)?;
            comments.push(v1::IssueComment {
                comment_id: record
                    .get("comment_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                issue_id: id.to_string(),
                body: record
                    .get("body")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                actor: Some(persisted_actor_context(
                    actor_id,
                    actor_kind,
                    record.get("agent_session_id"),
                )),
                created_at: Some(instant(
                    record
                        .get("timestamp")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                )),
                correlation_id: record
                    .get("correlation_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }
        comments.sort_by(|left, right| left.comment_id.cmp(&right.comment_id));
        Ok(comments)
    }

    pub async fn list_history(&self, id: Uuid) -> Result<Vec<v1::IssueHistoryEntry>, TrackerError> {
        self.ensure_healthy()?;
        let mut replica = self.inner.lock().await;
        let task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let mut entries = Vec::new();
        for (key, value) in task.get_user_defined_attributes() {
            if !key.starts_with("lb.history.") {
                continue;
            }
            let record: Value = serde_json::from_str(value)
                .map_err(|error| TrackerError::InvalidState(format!("invalid history: {error}")))?;
            let kind_name = record
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("update");
            let kind = match kind_name {
                "create" => v1::HistoryEventKind::IssueCreated,
                "transition"
                | "apply_issue_admin_action"
                | "lease_expired"
                | "lease_recovered"
                | "lease_revoked" => v1::HistoryEventKind::StateTransitioned,
                "claim" => v1::HistoryEventKind::Claimed,
                "heartbeat" => v1::HistoryEventKind::ClaimHeartbeat,
                "release" => v1::HistoryEventKind::ClaimReleased,
                "relate" => v1::HistoryEventKind::RelationAdded,
                "unrelate" => v1::HistoryEventKind::RelationRemoved,
                "comment" => v1::HistoryEventKind::CommentAdded,
                _ => v1::HistoryEventKind::IssueUpdated,
            };
            let old_state = record
                .get("old_state")
                .and_then(Value::as_str)
                .map(IssueState::parse)
                .transpose()?
                .map(issue_state);
            let new_state = record
                .get("new_state")
                .and_then(Value::as_str)
                .map(IssueState::parse)
                .transpose()?
                .map(issue_state);
            if kind == v1::HistoryEventKind::StateTransitioned && new_state.is_none() {
                return Err(TrackerError::InvalidState(
                    "state history entry is missing its resulting state".into(),
                ));
            }
            let awaiting_review = record
                .get("awaiting_review")
                .map(<v1::AwaitingReviewCompletion as serde::Deserialize>::deserialize)
                .transpose()
                .map_err(|error| {
                    TrackerError::InvalidState(format!(
                        "invalid awaiting-review history report: {error}"
                    ))
                })?;
            if new_state == Some(v1::IssueState::AwaitingReview as i32) && awaiting_review.is_none()
            {
                return Err(TrackerError::InvalidState(
                    "awaiting_review history entry is missing its run evidence".into(),
                ));
            }
            let subject_detail = if kind == v1::HistoryEventKind::CommentAdded {
                Some(v1::issue_history_entry::SubjectDetail::Comment(
                    v1::CommentHistoryDetail {
                        comment_id: record
                            .get("comment_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                    },
                ))
            } else if matches!(
                kind,
                v1::HistoryEventKind::RelationAdded | v1::HistoryEventKind::RelationRemoved
            ) {
                let relation_kind = match record
                    .get("relation_kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                {
                    "parent-child" => v1::RelationKind::ParentChild,
                    "blocks" => v1::RelationKind::Blocks,
                    "related" => v1::RelationKind::Related,
                    "discovered-from" => v1::RelationKind::DiscoveredFrom,
                    "supersedes" => v1::RelationKind::Supersedes,
                    _ => v1::RelationKind::Unspecified,
                };
                Some(v1::issue_history_entry::SubjectDetail::Relation(
                    v1::RelationHistoryDetail {
                        relation_id: record
                            .get("relation_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        kind: relation_kind as i32,
                        source_issue_id: record
                            .get("source_issue_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        target_issue_id: record
                            .get("target_issue_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                    },
                ))
            } else {
                None
            };
            let actor = match record.get("actor") {
                Some(Value::String(actor_id)) => {
                    let actor_kind = persisted_kind(&record)?;
                    Some(persisted_actor_context(
                        actor_id.to_owned(),
                        actor_kind,
                        record.get("agent_session_id"),
                    ))
                }
                Some(Value::Object(actor_record)) => {
                    let actor_id = actor_record
                        .get("actor_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            TrackerError::InvalidState("invalid history actor record".into())
                        })?;
                    let actor_kind = persisted_kind_value(
                        actor_record
                            .get("actor_kind")
                            .or_else(|| actor_record.get("kind"))
                            .or_else(|| record.get("actor_kind")),
                    )?;
                    Some(persisted_actor_context(
                        actor_id.to_owned(),
                        actor_kind,
                        actor_record
                            .get("agent_session_id")
                            .or_else(|| record.get("agent_session_id")),
                    ))
                }
                Some(_) => {
                    return Err(TrackerError::InvalidState(
                        "invalid history actor record".into(),
                    ));
                }
                None if record.get("actor_kind").is_some() => {
                    return Err(TrackerError::InvalidState(
                        "history actor record is incomplete".into(),
                    ));
                }
                None => None,
            };
            entries.push(v1::IssueHistoryEntry {
                history_id: key.to_owned(),
                issue_id: id.to_string(),
                downstream_operation_id: record
                    .get("operation_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                actor,
                occurred_at: Some(instant(
                    record
                        .get("timestamp")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                )),
                reason: record
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                correlation_id: record
                    .get("correlation_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                resulting_revision: record
                    .get("revision")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                kind: kind as i32,
                state_change: (kind == v1::HistoryEventKind::StateTransitioned).then_some(
                    v1::StateHistoryDetail {
                        old_state,
                        new_state: new_state.unwrap_or(v1::IssueState::Unspecified as i32),
                        completion: record
                            .get("completion")
                            .cloned()
                            .and_then(|value| serde_json::from_value(value).ok()),
                        cancellation: record
                            .get("cancellation")
                            .cloned()
                            .and_then(|value| serde_json::from_value(value).ok()),
                        awaiting_review,
                    },
                ),
                subject_detail,
            });
        }
        entries.sort_by(|left, right| left.history_id.cmp(&right.history_id));
        Ok(entries)
    }

    /// Read one Issue with its dependency relations resolved.
    pub async fn get(&self, id: Uuid) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let mut replica = self.inner.lock().await;
        let depmap = cached_dependency_map(&mut replica).await?;
        self.issue_from_replica(&mut replica, &depmap, id).await
    }

    /// Read several Issues under one replica lock and one dependency-map
    /// rebuild. Rebuilding the map scans the whole working set, so calling
    /// [`Self::get`] per Issue is quadratic in the number of Issues.
    ///
    /// Results are in request order and per-Issue failures stay per item, so
    /// callers can classify them exactly as they would individual `get` calls.
    pub(crate) async fn get_many(
        &self,
        ids: &[Uuid],
    ) -> Result<Vec<Result<Issue, TrackerError>>, TrackerError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_healthy()?;
        let mut replica = self.inner.lock().await;
        let depmap = cached_dependency_map(&mut replica).await?;
        let mut issues = Vec::with_capacity(ids.len());
        for &id in ids {
            issues.push(self.issue_from_replica(&mut replica, &depmap, id).await);
        }
        Ok(issues)
    }

    async fn issue_from_replica(
        &self,
        replica: &mut Replica<SqliteStorage>,
        depmap: &taskchampion::DependencyMap,
        id: Uuid,
    ) -> Result<Issue, TrackerError> {
        let task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        self.issue_from_task_with_dependencies(&task, Some(depmap))
    }

    pub async fn list(&self) -> Result<Vec<Issue>, TrackerError> {
        self.ensure_healthy()?;
        let mut replica = self.inner.lock().await;
        let depmap = cached_dependency_map(&mut replica).await?;
        let tasks = replica.all_tasks().await?;
        tasks
            .values()
            .filter(|task| task.get_user_defined_attribute(ID_KEY).is_some())
            .map(|task| self.issue_from_task_with_dependencies(task, Some(&depmap)))
            .collect()
    }

    /// Scan a bounded physical-UUID page for resident maintenance. UUID
    /// enumeration is the only O(N) operation; at most `limit` TaskData rows
    /// are decoded and malformed rows consume a page slot just like valid
    /// rows. The cursor is a physical TaskChampion UUID, never user metadata.
    pub(crate) async fn lease_maintenance_candidates_page_with_status(
        &self,
        now: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<LeaseMaintenanceScanPage, TrackerError> {
        self.ensure_healthy()?;
        let mut replica = self.inner.lock().await;
        // `Replica::all_tasks()` and `Replica::get_task()` construct high-level
        // Tasks and may calculate a dependency map over the entire working
        // set. Maintenance only needs raw mapping attributes, so enumerate
        // UUIDs and fetch TaskData one entry at a time instead.
        let mut uuids = replica.all_task_uuids().await?;
        uuids.sort_unstable_by_key(|uuid| uuid.as_bytes().to_owned());
        let start = cursor
            .and_then(|value| Uuid::parse_str(value).ok())
            .and_then(|cursor| {
                uuids
                    .iter()
                    .position(|uuid| uuid.as_bytes() > cursor.as_bytes())
            })
            .unwrap_or(0);
        let end_uuid = uuids.last().copied();
        let page_ids: Vec<_> = uuids.into_iter().skip(start).take(limit).collect();
        let reached_end = page_ids.last().copied() == end_uuid;
        let mut candidates = Vec::with_capacity(page_ids.len());
        for task_id in page_ids {
            let Some(task) = (match replica.get_task_data(task_id).await {
                Ok(task) => task,
                Err(_) => {
                    candidates.push(LeaseMaintenanceScanEntry::Invalid {
                        storage_id: task_id,
                        issue_id: task_id,
                        classification: "task_read_failed",
                    });
                    continue;
                }
            }) else {
                candidates.push(LeaseMaintenanceScanEntry::Skipped {
                    storage_id: task_id,
                });
                continue;
            };
            if task.get(ENTITY_KEY) != Some(ISSUE_ENTITY) {
                candidates.push(LeaseMaintenanceScanEntry::Skipped {
                    storage_id: task_id,
                });
                continue;
            }
            if task.get(SCHEMA_VERSION_KEY) != Some(MAPPING_SCHEMA_VERSION) {
                candidates.push(LeaseMaintenanceScanEntry::Invalid {
                    storage_id: task_id,
                    issue_id: task_id,
                    classification: "schema_mismatch",
                });
                continue;
            }
            let id = match task
                .get(ID_KEY)
                .and_then(|value| Uuid::parse_str(value).ok())
            {
                Some(id) => id,
                None => {
                    candidates.push(LeaseMaintenanceScanEntry::Invalid {
                        storage_id: task_id,
                        issue_id: task_id,
                        classification: "invalid_issue_id",
                    });
                    continue;
                }
            };
            if id != task_id {
                candidates.push(LeaseMaintenanceScanEntry::Invalid {
                    storage_id: task_id,
                    issue_id: id,
                    classification: "issue_id_mismatch",
                });
                continue;
            }
            let revision = match task.get(REVISION_KEY).and_then(|value| value.parse().ok()) {
                Some(revision) => revision,
                None => {
                    candidates.push(LeaseMaintenanceScanEntry::Invalid {
                        storage_id: task_id,
                        issue_id: id,
                        classification: "invalid_revision",
                    });
                    continue;
                }
            };
            let state = match task
                .get(STATE_KEY)
                .and_then(|value| IssueState::parse(value).ok())
            {
                Some(state) => state,
                None => {
                    candidates.push(LeaseMaintenanceScanEntry::Invalid {
                        storage_id: task_id,
                        issue_id: id,
                        classification: "invalid_state",
                    });
                    continue;
                }
            };
            let expires_at = task
                .get(LEASE_EXPIRES_KEY)
                .and_then(|value| value.parse().ok());
            let lease_state = lease_state_from_attributes(
                task.get(LEASE_OWNER_KEY),
                task.get(LEASE_OWNER_CONTEXT_KEY),
                task.get(LEASE_HANDLE_KEY),
                task.get(LEASE_EXPIRES_KEY),
                now,
            );
            candidates.push(LeaseMaintenanceScanEntry::Valid(
                LeaseMaintenanceCandidate {
                    storage_id: task_id,
                    id,
                    revision,
                    state,
                    expires_at,
                    lease_state,
                },
            ));
        }
        candidates.sort_by_key(|candidate| candidate.cursor_id());
        Ok(LeaseMaintenanceScanPage {
            entries: candidates,
            reached_end,
        })
    }

    /// Compatibility wrapper returning only the entries for callers that do
    /// not participate in readiness tracking.
    #[allow(dead_code)]
    pub(crate) async fn lease_maintenance_candidates_page(
        &self,
        now: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<LeaseMaintenanceScanEntry>, TrackerError> {
        Ok(self
            .lease_maintenance_candidates_page_with_status(now, cursor, limit)
            .await?
            .entries)
    }

    /// Compatibility helper used by the in-process tracker tests. Resident
    /// maintenance uses the cursor-aware page API above so production scans
    /// remain bounded and resumable.
    #[cfg(test)]
    pub(crate) async fn lease_maintenance_candidates(
        &self,
        now: u64,
    ) -> Result<Vec<LeaseMaintenanceScanEntry>, TrackerError> {
        self.lease_maintenance_candidates_page(now, None, usize::MAX)
            .await
    }

    pub async fn ready(&self, now: u64) -> Result<Vec<Issue>, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let mut replica = self.inner.lock().await;
        let depmap = cached_dependency_map(&mut replica).await?;
        let mut tasks: Vec<_> = replica
            .all_tasks()
            .await?
            .into_values()
            .filter(|task| task.get_user_defined_attribute(ID_KEY).is_some())
            .collect();
        tasks.sort_by_key(|task| {
            task.get_user_defined_attribute(ID_KEY)
                .and_then(|value| Uuid::parse_str(value).ok())
                .unwrap_or_else(|| task.get_uuid())
        });
        let mut ready = Vec::new();
        for task in tasks {
            // Checked before decoding so a leased task's projection is never
            // read; `task_is_ready` repeats it for single-Issue reads.
            if lease_state(&task, now) != LeaseState::None {
                continue;
            }
            let issue = self.issue_from_task_with_dependencies(&task, Some(&depmap))?;
            if task_is_ready(&mut replica, &task, &issue, now).await? {
                ready.push(issue);
            }
        }
        Ok(ready)
    }

    /// Read one Issue together with its `ready` membership without scanning
    /// every task, using the same readiness rules as [`Self::ready`].
    pub(crate) async fn get_with_ready(
        &self,
        id: Uuid,
        now: u64,
    ) -> Result<(Issue, bool), TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let mut replica = self.inner.lock().await;
        let depmap = cached_dependency_map(&mut replica).await?;
        let task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let issue = self.issue_from_task_with_dependencies(&task, Some(&depmap))?;
        let ready = task_is_ready(&mut replica, &task, &issue, now).await?;
        Ok((issue, ready))
    }

    /// Serialize every Issue and its history into a JSON export bundle, sorted
    /// so the output is deterministic for the same tracker state.
    pub async fn export(&self) -> Result<Vec<u8>, TrackerError> {
        self.ensure_healthy()?;
        let mut replica = self.inner.lock().await;
        let depmap = cached_dependency_map(&mut replica).await?;
        let tasks = replica.all_tasks().await?;
        let mut issues = Vec::new();
        let mut history = Vec::new();
        for task in tasks.values().filter(|task| {
            task.get_user_defined_attribute(ENTITY_KEY) == Some(ISSUE_ENTITY)
                && task.get_user_defined_attribute(ID_KEY).is_some()
        }) {
            let issue = self.issue_from_task_with_dependencies(task, Some(&depmap))?;
            for (key, value) in task.get_user_defined_attributes() {
                if key.starts_with("lb.history.") {
                    let record: Value = serde_json::from_str(value)
                        .map_err(|_| TrackerError::InvalidState("invalid history JSON".into()))?;
                    history.push(record);
                }
            }
            issues.push(issue);
        }
        issues.sort_by_key(|issue| issue.id);
        history.sort_by(|a, b| {
            a.get("operation_id")
                .and_then(Value::as_str)
                .cmp(&b.get("operation_id").and_then(Value::as_str))
        });
        serde_json::to_vec(&ExportBundle {
            contract: CONTRACT_VERSION.into(),
            schema_fingerprint: Self::schema_fingerprint(),
            issues,
            history,
            uri: None,
            provenance_fingerprint: None,
        })
        .map_err(|error| TrackerError::Backup(error.to_string()))
    }
}

fn persisted_actor_context(
    actor_id: String,
    kind: v1::ActorKind,
    agent_session_id: Option<&Value>,
) -> v1::ActorContext {
    v1::ActorContext {
        actor: Some(v1::ActorRef {
            actor_id,
            kind: kind as i32,
            ..Default::default()
        }),
        agent_session_id: agent_session_id
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    }
}

/// Reads reuse TaskChampion's cached dependency map instead of forcing a
/// rebuild, which scans the whole working set. The cache is safe because this
/// process holds the exclusive storage lock, every write goes through
/// `commit_operations` (which drops the cache), and restore/reopen replaces
/// the replica.
async fn cached_dependency_map(
    replica: &mut Replica<SqliteStorage>,
) -> Result<std::sync::Arc<taskchampion::DependencyMap>, TrackerError> {
    Ok(replica.dependency_map(false).await?)
}

/// The single readiness rule shared by `ready` and `get_with_ready`: an
/// identified Issue with no ownership tuple (a partial or expired lease needs
/// a recovery write first), open, confirmed, and not blocked.
async fn task_is_ready(
    replica: &mut Replica<SqliteStorage>,
    task: &taskchampion::Task,
    issue: &Issue,
    now: u64,
) -> Result<bool, TrackerError> {
    if task.get_user_defined_attribute(ID_KEY).is_none()
        || lease_state(task, now) != LeaseState::None
        || !matches!(issue.state, IssueState::Open)
        || issue.confirmation_status != v1::ConfirmationStatus::Confirmed as i32
    {
        return Ok(false);
    }
    // Blocking is looked up by the logical Issue ID; the held task already is
    // that task unless its physical UUID differs from the logical ID.
    if task.get_uuid() == issue.id {
        return Ok(!task.is_blocked());
    }
    let task = replica
        .get_task(issue.id)
        .await?
        .ok_or(TrackerError::NotFound(issue.id))?;
    Ok(!task.is_blocked())
}
