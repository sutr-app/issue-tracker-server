use super::{
    artifact_fs::*, config::CallContext, operation_log::*, operation_response::operation,
    task_mapping::*, *,
};

impl Tracker {
    pub async fn backup(&self, destination: impl AsRef<Path>) -> Result<(), TrackerError> {
        self.ensure_healthy()?;
        let _freeze = self.admission.write().await;
        let mut guard = self.inner.lock().await;
        let reopened_storage = SqliteStorage::new(&*self.root, AccessMode::ReadWrite, true)
            .await
            .map_err(TrackerError::from)?;
        let previous = std::mem::replace(&mut *guard, Replica::new(reopened_storage));
        drop(previous);
        let destination = destination.as_ref();
        checkpoint_sqlite(&self.root.join("taskchampion.sqlite3"))?;
        let _ = guard.all_tasks().await?;
        fs::create_dir_all(destination).map_err(|e| TrackerError::Backup(e.to_string()))?;
        let mut artifacts = serde_json::Map::new();
        for suffix in ["", "-wal", "-shm"] {
            let source = self.root.join(format!("taskchampion.sqlite3{suffix}"));
            if source.exists() {
                let filename = format!("taskchampion.sqlite3{suffix}");
                scan_secret_artifact(&source)?;
                fs::copy(&source, destination.join(&filename))
                    .map_err(|e| TrackerError::Backup(e.to_string()))?;
                artifacts.insert(
                    filename,
                    Value::String(file_digest(&source).map_err(TrackerError::Backup)?),
                );
            }
        }
        fs::write(
            destination.join("manifest.json"),
            serde_json::json!({
                "contract": CONTRACT_VERSION,
                "schema_fingerprint": Tracker::schema_fingerprint(),
                "artifacts": artifacts,
            })
            .to_string(),
        )
        .map_err(|e| TrackerError::Backup(e.to_string()))?;
        Ok(())
    }

    pub(crate) fn artifact_root(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    pub(crate) async fn create_backup_artifact(
        &self,
        operation_id: &str,
    ) -> Result<v1::Artifact, TrackerError> {
        reject_secret([operation_id])?;
        let artifact_id = format!("backup-{}", marker_digest(&[operation_id]));
        let path = self.artifact_root().join(&artifact_id);
        self.backup(&path).await?;
        artifact_from_directory(&self.root, &path, &artifact_id, v1::ArtifactKind::Backup)
    }

    pub(crate) async fn create_export_artifact(
        &self,
        operation_id: &str,
    ) -> Result<v1::Artifact, TrackerError> {
        reject_secret([operation_id])?;
        let artifact_id = format!("export-{}", marker_digest(&[operation_id]));
        let path = self.artifact_root().join(&artifact_id);
        fs::create_dir_all(&path).map_err(|error| TrackerError::Backup(error.to_string()))?;
        let bytes = self.export().await?;
        scan_secret_bytes(&bytes)?;
        fs::write(path.join("export.json"), bytes)
            .map_err(|error| TrackerError::Backup(error.to_string()))?;
        artifact_from_directory(&self.root, &path, &artifact_id, v1::ArtifactKind::Export)
    }

    pub(crate) fn list_artifact_metadata(&self) -> Result<Vec<v1::Artifact>, TrackerError> {
        let root = self.artifact_root();
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut artifacts = Vec::new();
        for entry in fs::read_dir(root).map_err(|error| TrackerError::Backup(error.to_string()))? {
            let entry = entry.map_err(|error| TrackerError::Backup(error.to_string()))?;
            if entry.path().is_dir() {
                let artifact_id = entry.file_name().to_string_lossy().into_owned();
                let kind = if artifact_id.starts_with("backup-") {
                    v1::ArtifactKind::Backup
                } else if artifact_id.starts_with("export-") {
                    v1::ArtifactKind::Export
                } else {
                    continue;
                };
                artifacts.push(artifact_from_directory(
                    &self.root,
                    &entry.path(),
                    &artifact_id,
                    kind,
                )?);
            }
        }
        artifacts.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
        Ok(artifacts)
    }

    pub(crate) async fn get_operation_record(
        &self,
        operation_id: &str,
    ) -> Result<v1::OperationRecord, TrackerError> {
        reject_secret([operation_id])?;
        let mut replica = self.inner.lock().await;
        let tasks = replica.all_tasks().await?;
        let markers = collect_operation_markers(tasks.values(), operation_id)?;
        match recover_relation_operation_record(&markers, operation_id)? {
            RelationOperationRecovery::Record(record) => Ok(*record),
            RelationOperationRecovery::MissingAnchor => Err(TrackerError::InvalidState(
                "relation anchor marker or history is missing".into(),
            )),
            RelationOperationRecovery::NotRelation => {
                recover_legacy_operation_record(&markers, operation_id)
            }
        }
    }

    pub async fn restore(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<(), TrackerError> {
        let backup = backup.as_ref();
        let destination = destination.as_ref();
        if !backup.is_dir() {
            return Err(TrackerError::Backup(
                "backup directory does not exist".into(),
            ));
        }
        if destination.exists() {
            return Err(TrackerError::Backup(
                "restore destination must not already exist".into(),
            ));
        }
        if destination.parent().is_none_or(|parent| !parent.is_dir()) {
            return Err(TrackerError::Backup(
                "restore destination parent must exist".into(),
            ));
        }
        let manifest: Value = serde_json::from_str(
            &fs::read_to_string(backup.join("manifest.json"))
                .map_err(|e| TrackerError::Backup(e.to_string()))?,
        )
        .map_err(|e| TrackerError::Backup(e.to_string()))?;
        if manifest.get("contract").and_then(Value::as_str) != Some(CONTRACT_VERSION) {
            return Err(TrackerError::Backup("contract mismatch".into()));
        }
        if manifest.get("schema_fingerprint").and_then(Value::as_str)
            != Some(Tracker::schema_fingerprint().as_str())
        {
            return Err(TrackerError::Backup("schema fingerprint mismatch".into()));
        }
        let artifacts = manifest
            .get("artifacts")
            .and_then(Value::as_object)
            .ok_or_else(|| TrackerError::Backup("artifact manifest is missing".into()))?;
        let allowed_artifacts = [
            "taskchampion.sqlite3",
            "taskchampion.sqlite3-wal",
            "taskchampion.sqlite3-shm",
        ];
        if !artifacts.contains_key(allowed_artifacts[0]) {
            return Err(TrackerError::Backup(
                "main database artifact is missing".into(),
            ));
        }
        for (filename, expected) in artifacts {
            if !allowed_artifacts.contains(&filename.as_str()) {
                return Err(TrackerError::Backup("unsupported artifact name".into()));
            }
            let expected = expected
                .as_str()
                .ok_or_else(|| TrackerError::Backup("invalid artifact digest".into()))?;
            let source = backup.join(filename);
            if !source.is_file() || file_digest(&source).map_err(TrackerError::Backup)? != expected
            {
                return Err(TrackerError::Backup(format!(
                    "artifact integrity check failed: {filename}"
                )));
            }
        }
        let parent = destination
            .parent()
            .ok_or_else(|| TrackerError::Backup("restore destination parent missing".into()))?;
        let staging = parent.join(format!(".lookback-issue-restore-{}", Uuid::new_v4()));
        fs::create_dir(&staging).map_err(|e| TrackerError::Backup(e.to_string()))?;
        for suffix in ["", "-wal", "-shm"] {
            let source = backup.join(format!("taskchampion.sqlite3{suffix}"));
            if source.exists() {
                fs::copy(
                    &source,
                    staging.join(format!("taskchampion.sqlite3{suffix}")),
                )
                .map_err(|e| TrackerError::Backup(e.to_string()))?;
            }
        }
        let restored = Tracker::open(&staging).await?;
        restored.list().await?;
        drop(restored);
        fs::rename(&staging, destination).map_err(|e| TrackerError::Backup(e.to_string()))?;
        Ok(())
    }

    /// Restore into this tracker only while all writes are frozen.
    /// The current database artifacts are retained until the replacement has been reopened and validated.
    pub async fn restore_current(&self, backup: impl AsRef<Path>) -> Result<(), TrackerError> {
        let _freeze = self.admission.write().await;
        let backup = backup.as_ref();
        let parent = self
            .root
            .parent()
            .ok_or_else(|| TrackerError::Backup("tracker root parent missing".into()))?;
        let staging = parent.join(format!(".lookback-issue-restore-{}", Uuid::new_v4()));
        Tracker::restore(backup, &staging).await?;
        verify_storage_root(&staging).await?;

        let rollback = parent.join(format!(".lookback-issue-rollback-{}", Uuid::new_v4()));
        fs::create_dir(&rollback).map_err(|e| TrackerError::Backup(e.to_string()))?;
        let stage_storage = SqliteStorage::new(&staging, AccessMode::ReadWrite, true)
            .await
            .map_err(TrackerError::from)?;
        let mut guard = self.inner.lock().await;
        let old_replica = std::mem::replace(&mut *guard, Replica::new(stage_storage));
        drop(old_replica);

        if let Err(error) = move_artifacts(&self.root, &rollback, true)
            .and_then(|_| move_artifacts(&staging, &self.root, false))
        {
            let recovered = move_artifacts(&rollback, &self.root, true).is_ok();
            if recovered {
                match SqliteStorage::new(&*self.root, AccessMode::ReadWrite, true).await {
                    Ok(storage) => {
                        *guard = Replica::new(storage);
                        if verify_storage_root(&self.root).await.is_err() {
                            self.fail_closed();
                        }
                    }
                    Err(_) => self.fail_closed(),
                }
            } else {
                self.fail_closed();
            }
            let _ = fs::remove_dir_all(&staging);
            let _ = fs::remove_dir_all(&rollback);
            drop(guard);
            return Err(error);
        }
        let new_storage = match SqliteStorage::new(&*self.root, AccessMode::ReadWrite, true).await {
            Ok(storage) => storage,
            Err(error) => {
                let recovered = move_artifacts(&rollback, &self.root, true).is_ok();
                if recovered {
                    match SqliteStorage::new(&*self.root, AccessMode::ReadWrite, true).await {
                        Ok(storage) => {
                            *guard = Replica::new(storage);
                            if verify_storage_root(&self.root).await.is_err() {
                                self.fail_closed();
                            }
                        }
                        Err(_) => self.fail_closed(),
                    }
                } else {
                    self.fail_closed();
                }
                let _ = fs::remove_dir_all(&staging);
                let _ = fs::remove_dir_all(&rollback);
                drop(guard);
                return Err(error.into());
            }
        };
        let new_replica = Replica::new(new_storage);
        *guard = new_replica;
        drop(guard);
        if let Err(error) = verify_storage_root(&self.root).await {
            let mut guard = self.inner.lock().await;
            let recovered = move_artifacts(&rollback, &self.root, true).is_ok();
            if recovered {
                match SqliteStorage::new(&*self.root, AccessMode::ReadWrite, true).await {
                    Ok(storage) => {
                        *guard = Replica::new(storage);
                        if verify_storage_root(&self.root).await.is_err() {
                            self.fail_closed();
                        }
                    }
                    Err(_) => self.fail_closed(),
                }
            } else {
                self.fail_closed();
            }
            drop(guard);
            return Err(error);
        }
        fs::remove_dir_all(&staging).map_err(|e| TrackerError::Backup(e.to_string()))?;
        fs::remove_dir_all(&rollback).map_err(|e| TrackerError::Backup(e.to_string()))?;
        self.advance_read_generation();
        Ok(())
    }

    pub(crate) fn issue_from_task(&self, task: &taskchampion::Task) -> Result<Issue, TrackerError> {
        self.issue_from_task_with_dependencies(task, None)
    }

    pub(crate) fn issue_from_task_with_dependencies(
        &self,
        task: &taskchampion::Task,
        depmap: Option<&taskchampion::DependencyMap>,
    ) -> Result<Issue, TrackerError> {
        validate_issue_task(task)?;
        let id = task
            .get_user_defined_attribute(ID_KEY)
            .and_then(|v| Uuid::parse_str(v).ok())
            .unwrap_or_else(|| task.get_uuid());
        let state =
            IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        let metadata = metadata_from_task(task)?;
        let mut relations = Vec::new();
        for relation in [
            RelationKind::ParentChild,
            RelationKind::Related,
            RelationKind::DiscoveredFrom,
            RelationKind::Supersedes,
        ] {
            let outgoing_key = format!("lb.relation.out.{}", relation.key());
            for target in relation_values(task, &outgoing_key)? {
                let metadata = relation_metadata(task, &relation_id(relation, id, target));
                relations.push(IssueRelation {
                    relation_id: relation_id(relation, id, target),
                    kind: relation,
                    source: id,
                    target,
                    created_at: metadata.0,
                    created_by: metadata.1,
                });
            }
            let incoming_key = format!("lb.relation.in.{}", relation.key());
            for source in relation_values(task, &incoming_key)? {
                let metadata = relation_metadata(task, &relation_id(relation, source, id));
                relations.push(IssueRelation {
                    relation_id: relation_id(relation, source, id),
                    kind: relation,
                    source,
                    target: id,
                    created_at: metadata.0,
                    created_by: metadata.1,
                });
            }
        }
        if let Some(depmap) = depmap {
            for target in depmap.dependents(id) {
                let metadata =
                    relation_metadata(task, &relation_id(RelationKind::Blocks, id, target));
                relations.push(IssueRelation {
                    relation_id: relation_id(RelationKind::Blocks, id, target),
                    kind: RelationKind::Blocks,
                    source: id,
                    target,
                    created_at: metadata.0,
                    created_by: metadata.1,
                });
            }
            for source in depmap.dependencies(id) {
                let metadata =
                    relation_metadata(task, &relation_id(RelationKind::Blocks, source, id));
                relations.push(IssueRelation {
                    relation_id: relation_id(RelationKind::Blocks, source, id),
                    kind: RelationKind::Blocks,
                    source,
                    target: id,
                    created_at: metadata.0,
                    created_by: metadata.1,
                });
            }
        }
        Ok(Issue {
            id,
            title: task.get_description().to_owned(),
            description: metadata.description,
            kind: metadata.kind,
            priority: metadata.priority,
            labels: metadata.labels,
            acceptance_criteria: metadata.acceptance_criteria,
            contexts: metadata.contexts,
            due_at: metadata.due_at,
            review_at: metadata.review_at,
            state,
            revision: revision(task)?,
            relations,
            created_by: metadata.created_by,
            created_at: metadata.created_at,
            updated_at: metadata.updated_at,
            completed_at: crate::tracker::lifecycle::is_terminal(state)
                .then_some(metadata.completed_at)
                .flatten(),
            creation_basis: metadata.creation_basis,
            confirmation_status: metadata.confirmation_status,
            provenance_metadata: metadata.provenance_metadata,
            source_evidence: metadata.source_evidence,
            lease_owner: task
                .get_user_defined_attribute(LEASE_OWNER_CONTEXT_KEY)
                .and_then(|value| serde_json::from_str(value).ok()),
            lease_expires_at: task
                .get_user_defined_attribute(LEASE_EXPIRES_KEY)
                .and_then(|v| v.parse().ok())
                .filter(|v| *v > 0),
            lease_handle: None,
        })
    }
}

struct OperationMarker {
    operation_id: String,
    issue_id: Option<String>,
    marker: Value,
    history: Option<Value>,
}

enum RelationOperationRecovery {
    NotRelation,
    MissingAnchor,
    Record(Box<v1::OperationRecord>),
}

fn collect_operation_markers<'a>(
    tasks: impl IntoIterator<Item = &'a taskchampion::Task>,
    operation_id: &str,
) -> Result<Vec<OperationMarker>, TrackerError> {
    tasks
        .into_iter()
        .filter_map(|task| operation_marker(task, operation_id).transpose())
        .collect()
}

fn operation_marker(
    task: &taskchampion::Task,
    operation_id: &str,
) -> Result<Option<OperationMarker>, TrackerError> {
    let Some(marker_operation_id) = [operation_id.to_owned(), create_operation_id(operation_id)]
        .into_iter()
        .find(|candidate| {
            task.get_user_defined_attribute(&format!("lb.operation.{candidate}"))
                .is_some()
        })
    else {
        return Ok(None);
    };
    let marker_value = task
        .get_user_defined_attribute(&format!("lb.operation.{marker_operation_id}"))
        .expect("marker operation ID was selected from this task");
    let marker = serde_json::from_str(marker_value).map_err(|error| {
        TrackerError::InvalidState(format!("invalid operation marker: {error}"))
    })?;
    let history = task
        .get_user_defined_attribute(&format!("lb.history.{marker_operation_id}"))
        .map(|value| {
            serde_json::from_str(value).map_err(|error| {
                TrackerError::InvalidState(format!("invalid operation history: {error}"))
            })
        })
        .transpose()?;
    Ok(Some(OperationMarker {
        operation_id: marker_operation_id,
        issue_id: task
            .get_user_defined_attribute(ID_KEY)
            .map(ToOwned::to_owned),
        marker,
        history,
    }))
}

fn recover_relation_operation_record(
    markers: &[OperationMarker],
    operation_id: &str,
) -> Result<RelationOperationRecovery, TrackerError> {
    let mut relation_marker_seen = false;
    for marker in markers {
        let Some(history) = marker.history.as_ref() else {
            continue;
        };
        let Some(history_kind) = history.get("kind").and_then(Value::as_str) else {
            continue;
        };
        if !matches!(history_kind, "relate" | "unrelate") {
            continue;
        }
        let (Some(source_issue_id), Some(target_issue_id), Some(relation_kind)) = (
            history.get("source_issue_id").and_then(Value::as_str),
            history.get("target_issue_id").and_then(Value::as_str),
            history.get("relation_kind").and_then(Value::as_str),
        ) else {
            return Ok(RelationOperationRecovery::Record(Box::new(
                manual_resolution_relation_record(history_kind, operation_id),
            )));
        };
        let anchor_issue_id = if relation_kind == RelationKind::Blocks.key() {
            let expected_relation_id = format!("blocks:{source_issue_id}:{target_issue_id}");
            if history.get("relation_id").and_then(Value::as_str)
                != Some(expected_relation_id.as_str())
                || history
                    .get("marker_anchor_issue_id")
                    .and_then(Value::as_str)
                    != Some(target_issue_id)
            {
                return Ok(RelationOperationRecovery::Record(Box::new(
                    manual_resolution_relation_record(history_kind, operation_id),
                )));
            }
            target_issue_id
        } else {
            source_issue_id
        };
        relation_marker_seen = true;
        let task_issue_id = marker
            .issue_id
            .as_deref()
            .ok_or_else(|| TrackerError::InvalidState("relation marker issue is missing".into()))?;
        if anchor_issue_id != task_issue_id {
            continue;
        }
        return relation_operation_record(
            marker,
            history,
            history_kind,
            anchor_issue_id,
            operation_id,
        )
        .map(Box::new)
        .map(RelationOperationRecovery::Record);
    }
    Ok(if relation_marker_seen {
        RelationOperationRecovery::MissingAnchor
    } else {
        RelationOperationRecovery::NotRelation
    })
}

fn relation_operation_record(
    marker: &OperationMarker,
    history: &Value,
    history_kind: &str,
    anchor_issue_id: &str,
    operation_id: &str,
) -> Result<v1::OperationRecord, TrackerError> {
    if history.get("operation_id").and_then(Value::as_str) != Some(&marker.operation_id) {
        return Err(TrackerError::InvalidState(
            "relation marker and history operation IDs differ".into(),
        ));
    }
    let marker_revision = marker
        .marker
        .get("revision")
        .and_then(Value::as_u64)
        .ok_or_else(|| TrackerError::InvalidState("relation marker revision is missing".into()))?;
    let history_revision = history
        .get("revision")
        .and_then(Value::as_u64)
        .ok_or_else(|| TrackerError::InvalidState("relation history revision is missing".into()))?;
    if marker_revision != history_revision
        || marker.marker.get("payload_digest").and_then(Value::as_str)
            != history.get("payload_digest").and_then(Value::as_str)
    {
        return Err(TrackerError::InvalidState(
            "relation marker and history conflict".into(),
        ));
    }
    let method_key = recover_marker_method(history_kind).expect("relation kind is known");
    let context = CallContext {
        operation_id: operation_id.to_owned(),
        ..Default::default()
    };
    let mut record = operation(&method_key, &context, None);
    record.scope = v1::OperationScope::Issue as i32;
    record.affected_issue_ids = vec![anchor_issue_id.to_owned()];
    record.resulting_revision = Some(marker_revision);
    record.payload_fingerprint = marker_digest(&[&method_key, operation_id]);
    record.result_replayability = if marker
        .marker
        .get("result_replayability")
        .and_then(Value::as_str)
        == Some("not_replayable")
    {
        v1::OperationResultReplayability::NotReplayable as i32
    } else {
        v1::OperationResultReplayability::Replayable as i32
    };
    record.result_snapshot = (record.result_replayability
        == v1::OperationResultReplayability::Replayable as i32)
        .then(v1::OperationResultSnapshot::default);
    record.fingerprint_input = Some(v1::OperationFingerprintInput {
        downstream_operation_id: operation_id.to_owned(),
        method_key,
        scope: record.scope,
        payload_fingerprint: record.payload_fingerprint.clone(),
    });
    record.entity_revisions = vec![v1::OperationEntityRevision {
        backend_id: anchor_issue_id.to_owned(),
        issue_id: anchor_issue_id.to_owned(),
        resulting_revision: marker_revision,
    }];
    Ok(record)
}

fn recover_legacy_operation_record(
    markers: &[OperationMarker],
    operation_id: &str,
) -> Result<v1::OperationRecord, TrackerError> {
    let mut issue_ids = Vec::new();
    let mut revision_value = None;
    let mut digest = None;
    let mut history_kind = None;
    let mut replayability = v1::OperationResultReplayability::Replayable as i32;
    let mut result_snapshot = None;
    for marker in markers {
        if let Some(issue_id) = &marker.issue_id {
            issue_ids.push(issue_id.clone());
        }
        revision_value = marker
            .marker
            .get("revision")
            .and_then(Value::as_u64)
            .or(revision_value);
        digest = marker
            .marker
            .get("payload_digest")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or(digest);
        history_kind = marker
            .history
            .as_ref()
            .and_then(|history| history.get("kind").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .or(history_kind);
        if marker
            .marker
            .get("result_replayability")
            .and_then(Value::as_str)
            == Some("not_replayable")
        {
            replayability = v1::OperationResultReplayability::NotReplayable as i32;
        }
        if let Some(result) = marker.marker.get("result")
            && !result.is_null()
        {
            let snapshot = serde_json::from_value::<v1::OperationResultSnapshot>(result.clone())
                .map_err(|error| {
                    TrackerError::InvalidState(format!("invalid marker result snapshot: {error}"))
                })?;
            if result_snapshot.replace(snapshot).is_some() {
                return Err(TrackerError::InvalidState(
                    "multiple issue markers carry the same operation snapshot".into(),
                ));
            }
        }
    }
    if issue_ids.is_empty() && digest.is_none() {
        return Err(TrackerError::NotFound(Uuid::nil()));
    }
    // Administrative NO_CHANGE markers intentionally have no history entry:
    // the decision must not change Issue lifecycle history, revision, or
    // updated_at.  Their typed receipt is nevertheless an immutable method
    // discriminator, so use it before falling back to the legacy history
    // mapping.  Without this branch a restart would reconstruct `unknown`
    // and fail the Manager marker expectation even though the marker is
    // complete and replayable.
    let method_key = result_snapshot
        .as_ref()
        .filter(|snapshot| {
            snapshot.manager_result_schema == "issue_tracker.manager.v1.IssueAdminActionResult"
        })
        .map(|_| "apply_issue_admin_action".to_owned())
        .or_else(|| history_kind.as_deref().and_then(recover_marker_method))
        .unwrap_or_else(|| "unknown".to_owned());
    let payload_fingerprint = if method_key == "unknown" {
        digest.unwrap_or_else(|| marker_digest(&[operation_id]))
    } else {
        marker_digest(&[&method_key, operation_id])
    };
    let context = CallContext {
        operation_id: operation_id.to_owned(),
        ..Default::default()
    };
    let mut record = operation(&method_key, &context, None);
    // Legacy durable markers persist their method, affected issues, and
    // resulting revision separately. Rebuild the same Issue-scoped proof
    // that the original response exposed so a restart can verify it
    // without resending an acknowledged operation.
    if !issue_ids.is_empty() {
        record.scope = v1::OperationScope::Issue as i32;
    }
    record.affected_issue_ids = issue_ids.clone();
    record.resulting_revision = revision_value;
    record.payload_fingerprint = payload_fingerprint;
    record.result_replayability = replayability;
    // A non-replayable marker may carry a redacted snapshot for audit and
    // manual resolution.  It is still advertised as NotReplayable, so normal
    // idempotent replay paths continue to reject it and never return a claim
    // handle.  Replayable markers, on the other hand, require a snapshot.
    record.result_snapshot = if replayability == v1::OperationResultReplayability::Replayable as i32
    {
        Some(result_snapshot.ok_or_else(|| TrackerError::ResultUnconfirmed)?)
    } else {
        result_snapshot
    };
    record.fingerprint_input = Some(v1::OperationFingerprintInput {
        downstream_operation_id: operation_id.to_owned(),
        method_key: method_key.clone(),
        scope: record.scope,
        payload_fingerprint: record.payload_fingerprint.clone(),
    });
    record.entity_revisions = issue_ids
        .into_iter()
        .map(|issue_id| v1::OperationEntityRevision {
            backend_id: issue_id.clone(),
            issue_id,
            resulting_revision: revision_value.unwrap_or_default(),
        })
        .collect();
    Ok(record)
}

fn manual_resolution_relation_record(
    history_kind: &str,
    operation_id: &str,
) -> v1::OperationRecord {
    let method_key = recover_marker_method(history_kind).expect("relation kind is known");
    let context = CallContext {
        operation_id: operation_id.to_owned(),
        ..Default::default()
    };
    // A tracker-scoped record can never satisfy a manager Issue expectation.
    // This represents legacy or contradictory relation metadata without
    // guessing an anchor and intentionally routes it to manual resolution.
    operation(&method_key, &context, None)
}

/// Recover the canonical method from legacy marker data. Older durable
/// TaskChampion markers persist the payload fingerprint but not method_key;
/// the fingerprint is deliberately bound to both values by operation_log.
fn recover_marker_method(history_kind: &str) -> Option<String> {
    match history_kind {
        "create" => Some("create_issue".to_owned()),
        "update" | "update_title" | "update_issue" => Some("update_issue".to_owned()),
        "transition" => Some("transition_issue".to_owned()),
        "claim" => Some("claim_issue".to_owned()),
        "heartbeat" => Some("heartbeat_issue_claim".to_owned()),
        "release" => Some("release_issue_claim".to_owned()),
        "reconcile_expired_claim" => Some("reconcile_expired_claim".to_owned()),
        // RECOVER / REVOKE are trusted-manager actions rather than worker
        // reconciliation. Preserve that distinction in the durable marker so
        // manual resolution can validate the admitted administrative intent.
        "lease_recovered" | "lease_revoked" => Some("apply_issue_admin_action".to_owned()),
        "apply_issue_admin_action" => Some("apply_issue_admin_action".to_owned()),
        "relate" => Some("add_issue_relation".to_owned()),
        "unrelate" => Some("remove_issue_relation".to_owned()),
        "comment" => Some("add_issue_comment".to_owned()),
        _ => None,
    }
}

pub(crate) fn set(
    task: &mut taskchampion::Task,
    key: &str,
    value: &str,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    task.set_user_defined_attribute(key, value, ops)
        .map_err(TrackerError::from)
}
