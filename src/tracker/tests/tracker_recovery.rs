use super::super::*;
use crate::tracker::artifact_fs::scan_secret_artifact;
use async_trait::async_trait;
use taskchampion::server::{
    AddVersionResult, GetVersionResult, HistorySegment, NIL_VERSION_ID, Server, Snapshot,
    SnapshotUrgency, VersionId,
};
use tempfile::TempDir;

#[derive(Default)]
struct LocalTaskChampionSyncServer {
    versions: Vec<(VersionId, VersionId, HistorySegment)>,
    snapshot: Option<(VersionId, Snapshot)>,
}

#[async_trait(?Send)]
impl Server for LocalTaskChampionSyncServer {
    async fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> Result<(AddVersionResult, SnapshotUrgency), taskchampion::Error> {
        let latest = self
            .versions
            .last()
            .map_or(NIL_VERSION_ID, |(version_id, _, _)| *version_id);
        if parent_version_id != latest {
            return Ok((
                AddVersionResult::ExpectedParentVersion(latest),
                SnapshotUrgency::None,
            ));
        }
        let version_id = Uuid::new_v4();
        self.versions
            .push((version_id, parent_version_id, history_segment));
        Ok((AddVersionResult::Ok(version_id), SnapshotUrgency::None))
    }

    async fn get_child_version(
        &mut self,
        parent_version_id: VersionId,
    ) -> Result<GetVersionResult, taskchampion::Error> {
        Ok(self
            .versions
            .iter()
            .find(|(_, parent, _)| *parent == parent_version_id)
            .map_or(
                GetVersionResult::NoSuchVersion,
                |(version_id, parent, segment)| GetVersionResult::Version {
                    version_id: *version_id,
                    parent_version_id: *parent,
                    history_segment: segment.clone(),
                },
            ))
    }

    async fn add_snapshot(
        &mut self,
        version_id: VersionId,
        snapshot: Snapshot,
    ) -> Result<(), taskchampion::Error> {
        self.snapshot = Some((version_id, snapshot));
        Ok(())
    }

    async fn get_snapshot(&mut self) -> Result<Option<(VersionId, Snapshot)>, taskchampion::Error> {
        Ok(self.snapshot.clone())
    }
}

#[test]
fn backup_artifact_scan_rejects_secret_sentinels() {
    let directory = TempDir::new().unwrap();
    let clean = directory.path().join("clean.bin");
    fs::write(&clean, b"ordinary issue title").unwrap();
    assert!(scan_secret_artifact(&clean).is_ok());
    let contaminated = directory.path().join("contaminated.bin");
    fs::write(&contaminated, b"password=do-not-persist").unwrap();
    assert!(matches!(
        scan_secret_artifact(&contaminated),
        Err(TrackerError::Backup(_))
    ));
}

#[tokio::test]
async fn backup_copies_database_artifacts_and_manifest() {
    let root = TempDir::new().unwrap();
    let backup = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    tracker.create("backup", "backup").await.unwrap();
    tracker.backup(backup.path()).await.unwrap();
    assert!(backup.path().join("taskchampion.sqlite3").exists());
    assert!(backup.path().join("manifest.json").exists());
    let restored = TempDir::new().unwrap();
    let restored_root = restored.path().join("restored-root");
    Tracker::restore(backup.path(), &restored_root)
        .await
        .unwrap();
    let reopened = Tracker::open(&restored_root).await.unwrap();
    assert_eq!(reopened.list().await.unwrap().len(), 1);
    assert!(matches!(
        Tracker::restore(backup.path(), restored.path()).await,
        Err(TrackerError::Backup(_))
    ));
}

#[tokio::test]
async fn awaiting_review_backup_restore_preserves_pending_status_evidence_and_denies_claim() {
    let root = TempDir::new().unwrap();
    let backup = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let (issue_id, committed_revision, completion) =
        super::create_awaiting_review_fixture(&tracker, "backup-awaiting-review", "backup-run")
            .await;
    tracker.backup(backup.path()).await.unwrap();
    drop(tracker);

    let restored_parent = TempDir::new().unwrap();
    let restored_root = restored_parent.path().join("restored-awaiting-root");
    Tracker::restore(backup.path(), &restored_root)
        .await
        .unwrap();
    let restored = Tracker::open(&restored_root).await.unwrap();
    let issue = restored.get(issue_id).await.unwrap();
    assert_eq!(issue.state, IssueState::AwaitingReview);
    assert_eq!(issue.revision, committed_revision);
    assert_eq!(issue.lease_handle, None);
    assert_eq!(issue.lease_owner, None);
    assert_eq!(issue.lease_expires_at, None);
    assert!(restored.ready(now()).await.unwrap().is_empty());

    let history = restored.list_history(issue_id).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|entry| {
                entry.downstream_operation_id == "backup-awaiting-review"
                    && entry.state_change.as_ref().is_some_and(|change| {
                        change.new_state == v1::IssueState::AwaitingReview as i32
                    })
            })
            .count(),
        1
    );
    assert!(history.iter().any(|entry| {
        entry.downstream_operation_id == "backup-awaiting-review"
            && entry
                .state_change
                .as_ref()
                .is_some_and(|change| change.awaiting_review.as_ref() == Some(&completion))
    }));
    let raw_status = {
        let mut replica = restored.inner.lock().await;
        replica
            .get_task(issue_id)
            .await
            .unwrap()
            .unwrap()
            .get_status()
    };
    assert_eq!(raw_status, taskchampion::Status::Pending);
    assert!(
        restored
            .claim(
                issue_id,
                "reviewer-must-not-claim",
                issue.revision,
                60,
                "claim-awaiting-review-after-restore",
            )
            .await
            .is_err(),
        "awaiting-review remains non-claimable after restore"
    );
}

#[tokio::test]
async fn awaiting_review_evidence_survives_taskchampion_sync_and_reopen() {
    let source_root = TempDir::new().unwrap();
    let source = Tracker::open(source_root.path()).await.unwrap();
    let (issue_id, committed_revision, completion) =
        super::create_awaiting_review_fixture(&source, "sync-awaiting-review", "sync-run").await;
    drop(source);

    // This fixture implements TaskChampion's public sync-server trait locally:
    // the production TaskChampion Replica performs the actual operation encode,
    // upload, download, and apply path without a fork or external service.
    let mut sync_server: Box<dyn Server> = Box::new(LocalTaskChampionSyncServer::default());
    let source_storage = SqliteStorage::new(source_root.path(), AccessMode::ReadWrite, false)
        .await
        .unwrap();
    let mut source_replica = Replica::new(source_storage);
    source_replica.sync(&mut sync_server, false).await.unwrap();
    drop(source_replica);

    let destination_root = TempDir::new().unwrap();
    let destination_storage =
        SqliteStorage::new(destination_root.path(), AccessMode::ReadWrite, true)
            .await
            .unwrap();
    let mut destination_replica = Replica::new(destination_storage);
    destination_replica
        .sync(&mut sync_server, false)
        .await
        .unwrap();
    drop(destination_replica);

    let destination = Tracker::open(destination_root.path()).await.unwrap();
    let issue = destination.get(issue_id).await.unwrap();
    assert_eq!(issue.state, IssueState::AwaitingReview);
    assert_eq!(issue.revision, committed_revision);
    assert!(issue.lease_handle.is_none());
    assert!(destination.ready(now()).await.unwrap().is_empty());
    let history = destination.list_history(issue_id).await.unwrap();
    assert!(history.iter().any(|entry| {
        entry.downstream_operation_id == "sync-awaiting-review"
            && entry.state_change.as_ref().is_some_and(|change| {
                change.new_state == v1::IssueState::AwaitingReview as i32
                    && change.awaiting_review.as_ref() == Some(&completion)
            })
    }));
    let raw_status = {
        let mut replica = destination.inner.lock().await;
        replica
            .get_task(issue_id)
            .await
            .unwrap()
            .unwrap()
            .get_status()
    };
    assert_eq!(raw_status, taskchampion::Status::Pending);
    assert!(
        destination
            .claim(
                issue_id,
                "reviewer-must-not-claim",
                issue.revision,
                60,
                "claim-awaiting-review-after-sync",
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn prior_v7_taskchampion_root_fixture_keeps_its_fixed_identity() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/taskchampion-v7-root-identity.json")).unwrap();
    let root = TempDir::new().unwrap();
    let storage = SqliteStorage::new(root.path(), AccessMode::ReadWrite, true)
        .await
        .unwrap();
    let mut replica = Replica::new(storage);
    let mut operations = Operations::new();
    let mut metadata = replica
        .create_task(metadata_id(), &mut operations)
        .await
        .unwrap();
    metadata
        .set_description("v7 fixture metadata".into(), &mut operations)
        .unwrap();
    metadata
        .set_status(Status::Completed, &mut operations)
        .unwrap();
    for (key, value) in [
        (ENTITY_KEY, fixture["entity"].as_str().unwrap()),
        (
            SCHEMA_VERSION_KEY,
            fixture["schema_version"].as_str().unwrap(),
        ),
        (
            "lb.schema_fingerprint",
            fixture["schema_fingerprint"].as_str().unwrap(),
        ),
        (
            "lb.contract_fingerprint",
            fixture["contract_fingerprint"].as_str().unwrap(),
        ),
        ("lb.task_fingerprint", "prior-v7-metadata-fixture"),
    ] {
        metadata
            .set_user_defined_attribute(key, value, &mut operations)
            .unwrap();
    }
    replica.commit_operations(operations).await.unwrap();
    drop(replica);

    assert_eq!(
        Tracker::contract_version(),
        fixture["contract"].as_str().unwrap()
    );
    assert_eq!(
        Tracker::schema_fingerprint(),
        fixture["schema_fingerprint"].as_str().unwrap()
    );
    let opened = Tracker::open(root.path()).await.unwrap();
    assert!(opened.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn canonical_awaiting_review_recovery_requires_exact_actor_report_and_marker_history() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("canonical-recovery", "recovery proof")
        .await
        .unwrap();
    let claimed = tracker
        .claim(
            issue.id,
            "fixture-worker",
            issue.revision,
            60,
            "canonical-recovery-claim",
        )
        .await
        .unwrap();
    let operation_id = "canonical-recovery-completion";
    let request_digest = "sha256:full-request-registration-identity";
    let actor = "issue-tracker:external-agent";
    let completion = super::awaiting_review_completion("canonical-recovery-run");
    let correlation_id =
        crate::tracker::issue_write::awaiting_review_gateway_correlation_id(request_digest);
    tracker
        .complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            operation_id,
            claimed.lease_handle.as_deref(),
            Some(claimed.revision),
            actor,
            Some(&completion.run_id),
            &correlation_id,
            &completion,
        )
        .await
        .unwrap();

    assert!(
        tracker
            .verify_awaiting_review_commit(
                issue.id,
                claimed.revision,
                actor,
                request_digest,
                &completion,
            )
            .await
            .unwrap()
    );
    assert!(
        !tracker
            .verify_awaiting_review_commit(
                issue.id,
                claimed.revision,
                "different-actor",
                request_digest,
                &completion,
            )
            .await
            .unwrap()
    );
    assert!(
        !tracker
            .verify_awaiting_review_commit(
                issue.id,
                claimed.revision,
                actor,
                "sha256:changed-policy-or-request",
                &completion,
            )
            .await
            .unwrap()
    );
    let changed_commit = v1::AwaitingReviewCompletion {
        full_commit_id: "c".repeat(40),
        ..completion.clone()
    };
    assert!(
        !tracker
            .verify_awaiting_review_commit(
                issue.id,
                claimed.revision,
                actor,
                request_digest,
                &changed_commit,
            )
            .await
            .unwrap()
    );

    {
        let mut replica = tracker.inner.lock().await;
        let mut task = replica.get_task(issue.id).await.unwrap().unwrap();
        let history_key = format!("lb.history.{operation_id}");
        let mut history: serde_json::Value =
            serde_json::from_str(task.get_user_defined_attribute(&history_key).unwrap()).unwrap();
        history["awaiting_review"]["full_commit_id"] = serde_json::json!("d".repeat(40));
        let mut operations = Operations::new();
        task.set_user_defined_attribute(&history_key, history.to_string(), &mut operations)
            .unwrap();
        replica.commit_operations(operations).await.unwrap();
    }
    assert!(
        !tracker
            .verify_awaiting_review_commit(
                issue.id,
                claimed.revision,
                actor,
                request_digest,
                &completion,
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn coordinated_restore_replaces_current_root_after_validation() {
    let root = TempDir::new().unwrap();
    let backup = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    tracker.create("before-restore", "before").await.unwrap();
    tracker.backup(backup.path()).await.unwrap();
    tracker.create("after-backup", "after").await.unwrap();
    assert_eq!(tracker.list().await.unwrap().len(), 2);
    tracker.restore_current(backup.path()).await.unwrap();
    let restored = tracker.list().await.unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].title, "before");
}

#[tokio::test]
async fn coordinated_restore_rejects_invalid_backup_without_changing_current_root() {
    let root = TempDir::new().unwrap();
    let backup = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    tracker.create("restore-invalid", "keep me").await.unwrap();
    tracker.backup(backup.path()).await.unwrap();
    fs::write(
        backup.path().join("manifest.json"),
        serde_json::json!({
            "contract": CONTRACT_VERSION,
            "schema_fingerprint": Tracker::schema_fingerprint(),
            "artifacts": {}
        })
        .to_string(),
    )
    .unwrap();

    assert!(matches!(
        tracker.restore_current(backup.path()).await,
        Err(TrackerError::Backup(_))
    ));
    let current = tracker.list().await.unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].title, "keep me");
}

#[tokio::test]
async fn restore_rejects_modified_backup_artifact() {
    let root = TempDir::new().unwrap();
    let backup = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    tracker.create("backup-integrity", "backup").await.unwrap();
    tracker.backup(backup.path()).await.unwrap();
    let database = backup.path().join("taskchampion.sqlite3");
    let mut bytes = fs::read(&database).unwrap();
    bytes.push(0);
    fs::write(database, bytes).unwrap();
    let destination_parent = TempDir::new().unwrap();
    let destination = destination_parent.path().join("restored");
    assert!(matches!(
        Tracker::restore(backup.path(), &destination).await,
        Err(TrackerError::Backup(_))
    ));
}

#[tokio::test]
async fn restore_rejects_manifest_path_traversal() {
    let root = TempDir::new().unwrap();
    let backup = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    tracker.create("restore-path", "restore").await.unwrap();
    tracker.backup(backup.path()).await.unwrap();
    fs::write(
        backup.path().join("manifest.json"),
        serde_json::json!({
            "contract": CONTRACT_VERSION,
            "schema_fingerprint": Tracker::schema_fingerprint(),
            "artifacts": {"../outside": "sha256:invalid"}
        })
        .to_string(),
    )
    .unwrap();
    let destination_parent = TempDir::new().unwrap();
    let destination = destination_parent.path().join("restored");
    assert!(matches!(
        Tracker::restore(backup.path(), &destination).await,
        Err(TrackerError::Backup(_))
    ));
}

#[tokio::test]
async fn duplicate_data_root_is_rejected() {
    let root = TempDir::new().unwrap();
    let first = Tracker::open(root.path()).await.unwrap();
    assert!(matches!(
        Tracker::open(root.path()).await,
        Err(TrackerError::RootLocked)
    ));
    drop(first);
    assert!(Tracker::open(root.path()).await.is_ok());
}

#[tokio::test]
async fn process_lock_rejects_peer_and_recovers_after_abort() {
    if let Ok(root) = std::env::var("LOOKBACK_LOCK_PROBE_ROOT") {
        assert!(matches!(
            Tracker::open(root).await,
            Err(TrackerError::RootLocked)
        ));
        return;
    }
    if let Ok(root) = std::env::var("LOOKBACK_LOCK_CRASH_ROOT") {
        let _tracker = Tracker::open(root).await.unwrap();
        std::process::abort();
    }

    let root = TempDir::new().unwrap();
    let first = Tracker::open(root.path()).await.unwrap();
    let probe = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tracker::tests::tracker_recovery::process_lock_rejects_peer_and_recovers_after_abort",
            "--nocapture",
        ])
        .env("LOOKBACK_LOCK_PROBE_ROOT", root.path())
        .status()
        .unwrap();
    assert!(probe.success());
    drop(first);

    let crash_root = TempDir::new().unwrap();
    let crash = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tracker::tests::tracker_recovery::process_lock_rejects_peer_and_recovers_after_abort",
            "--nocapture",
        ])
        .env("LOOKBACK_LOCK_CRASH_ROOT", crash_root.path())
        .status()
        .unwrap();
    assert!(!crash.success());
    assert!(Tracker::open(crash_root.path()).await.is_ok());
}

async fn read_attribute(tracker: &Tracker, id: Uuid, key: &str) -> serde_json::Value {
    let mut replica = tracker.inner.lock().await;
    let task = replica.get_task(id).await.unwrap().unwrap();
    serde_json::from_str(task.get_user_defined_attribute(key).unwrap()).unwrap()
}

#[tokio::test]
async fn canonical_awaiting_review_recovery_rejects_each_tampered_history_and_marker_field() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("tamper-recovery", "tamper").await.unwrap();
    let claimed = tracker
        .claim(
            issue.id,
            "fixture-worker",
            issue.revision,
            60,
            "tamper-claim",
        )
        .await
        .unwrap();
    let operation_id = "tamper-completion";
    let request_digest = "sha256:tamper-request";
    let actor = "issue-tracker:external-agent";
    let completion = super::awaiting_review_completion("tamper-run");
    let correlation_id =
        crate::tracker::issue_write::awaiting_review_gateway_correlation_id(request_digest);
    tracker
        .complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            operation_id,
            claimed.lease_handle.as_deref(),
            Some(claimed.revision),
            actor,
            Some(&completion.run_id),
            &correlation_id,
            &completion,
        )
        .await
        .unwrap();
    let verify = |expected_revision: u64| {
        tracker.verify_awaiting_review_commit(
            issue.id,
            expected_revision,
            actor,
            request_digest,
            &completion,
        )
    };
    assert!(verify(claimed.revision).await.unwrap());
    assert!(!verify(claimed.revision + 1).await.unwrap());
    assert!(!verify(u64::MAX).await.unwrap());

    let history_key = format!("lb.history.{operation_id}");
    let marker_key = format!("lb.operation.{operation_id}");
    let history = read_attribute(&tracker, issue.id, &history_key).await;
    let marker = read_attribute(&tracker, issue.id, &marker_key).await;
    let tampered_history = [
        ("kind", serde_json::json!("update")),
        ("revision", serde_json::json!(99)),
        ("actor", serde_json::json!("someone-else")),
        ("agent_session_id", serde_json::json!("other-run")),
        ("old_state", serde_json::json!("open")),
        ("new_state", serde_json::json!("done")),
        ("reason", serde_json::json!("other reason")),
        ("payload_digest", serde_json::json!("other-digest")),
    ];
    for (field, value) in tampered_history {
        let mut changed = history.clone();
        changed[field] = value;
        tracker
            .set_user_defined_attribute_for_test(issue.id, &history_key, &changed.to_string())
            .await
            .unwrap();
        assert!(!verify(claimed.revision).await.unwrap(), "history {field}");
    }
    tracker
        .set_user_defined_attribute_for_test(issue.id, &history_key, &history.to_string())
        .await
        .unwrap();

    let tampered_markers = [
        ("status", {
            let mut changed = marker.clone();
            changed["status"] = serde_json::json!("pending");
            changed
        }),
        ("revision", {
            let mut changed = marker.clone();
            changed["revision"] = serde_json::json!(99);
            changed
        }),
        ("payload_digest", {
            let mut changed = marker.clone();
            changed["payload_digest"] = serde_json::json!("other-digest");
            changed
        }),
        ("result_replayability", {
            let mut changed = marker.clone();
            changed["result_replayability"] = serde_json::json!("not_replayable");
            changed
        }),
        ("result", {
            let mut changed = marker.clone();
            changed["result"] = serde_json::json!({});
            changed
        }),
    ];
    for (field, changed) in tampered_markers {
        tracker
            .set_user_defined_attribute_for_test(issue.id, &marker_key, &changed.to_string())
            .await
            .unwrap();
        // Malformed evidence may fail closed with an error instead of false.
        assert!(
            !matches!(verify(claimed.revision).await, Ok(true)),
            "marker {field}"
        );
    }
    tracker
        .set_user_defined_attribute_for_test(issue.id, &marker_key, &marker.to_string())
        .await
        .unwrap();
    assert!(verify(claimed.revision).await.unwrap(), "restored evidence");

    // A second history entry claiming the same Gateway correlation makes the
    // proof ambiguous, so recovery must refuse it.
    let mut duplicate = history.clone();
    duplicate["operation_id"] = serde_json::json!("tamper-duplicate");
    tracker
        .set_user_defined_attribute_for_test(
            issue.id,
            "lb.history.tamper-duplicate",
            &duplicate.to_string(),
        )
        .await
        .unwrap();
    assert!(
        !verify(claimed.revision).await.unwrap(),
        "duplicate correlation"
    );
}
