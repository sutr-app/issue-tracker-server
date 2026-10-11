use super::super::*;
use crate::proto_types::manager_v1;
use crate::tracker::{
    artifact_fs::reject_secret,
    issue_read::LeaseMaintenanceScanEntry,
    issue_write::{LeaseState, lease_state_from_attributes},
    task_mapping::{metadata_from_task, relation_id, set_metadata},
};
use prost::Message;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[tokio::test]
async fn fenced_operation_is_rejected_after_restart_safe_tracker_record() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("fenced", "fenced operation").await.unwrap();
    let guard = tracker.acquire_admission_write().await;
    tracker
        .persist_operation_fence(
            issue.id,
            "downstream-fenced-operation",
            "resolution-1",
            "digest-1",
        )
        .await
        .unwrap();
    drop(guard);

    let error = tracker
        .transition(
            issue.id,
            IssueState::Blocked,
            issue.revision,
            "downstream-fenced-operation",
            None,
        )
        .await
        .expect_err("a fenced delayed operation must not mutate the issue");
    assert!(matches!(error, TrackerError::OperationFenced));
    let current = tracker.get(issue.id).await.unwrap();
    assert_eq!(current.revision, issue.revision);
    assert_eq!(current.state, IssueState::Open);
}

#[test]
fn lease_classification_requires_a_complete_owner_context_tuple() {
    let owner = super::super::hash("owner");
    let handle = super::super::hash("handle");
    assert_eq!(
        lease_state_from_attributes(Some(&owner), None, Some(&handle), Some("101"), 100,),
        LeaseState::Inconsistent
    );
    assert_eq!(
        lease_state_from_attributes(
            Some(&owner),
            Some(r#"{"actor_id":"worker"}"#),
            Some(&handle),
            Some("100"),
            100,
        ),
        LeaseState::Expired
    );
    assert_eq!(
        lease_state_from_attributes(Some("not-a-hash"), None, Some(&handle), Some("101"), 100,),
        LeaseState::Inconsistent
    );
    assert_eq!(
        lease_state_from_attributes(Some(&owner), Some("null"), Some(&handle), Some("101"), 100,),
        LeaseState::Inconsistent
    );
    assert_eq!(
        lease_state_from_attributes(Some(&owner), Some(r#"{}"#), Some(&handle), Some("101"), 100,),
        LeaseState::Inconsistent
    );
    assert_eq!(
        lease_state_from_attributes(
            Some(&owner),
            Some(r#"{"actor_id":" "}"#),
            Some(&handle),
            Some("101"),
            100,
        ),
        LeaseState::Inconsistent
    );
    assert_eq!(
        lease_state_from_attributes(
            Some(&owner),
            Some(r#"{"actor_id":42}"#),
            Some(&handle),
            Some("101"),
            100,
        ),
        LeaseState::Inconsistent
    );
    let uppercase_owner = owner.to_ascii_uppercase();
    assert_eq!(
        lease_state_from_attributes(
            Some(&uppercase_owner),
            None,
            Some(&handle),
            Some("101"),
            100,
        ),
        LeaseState::Inconsistent
    );
}

#[tokio::test]
async fn canonical_states_round_trip_and_candidate_is_rejected() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("state", "state test").await.unwrap();
    let issue = tracker
        .transition(issue.id, IssueState::Deferred, 0, "defer-1", None)
        .await
        .unwrap();
    assert_eq!(issue.state, IssueState::Deferred);
    assert!(IssueState::parse("candidate").is_err());
}

#[tokio::test]
async fn awaiting_review_is_one_pending_native_transition_with_replayable_evidence() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("awaiting", "awaiting review").await.unwrap();
    let claimed = tracker
        .claim(
            issue.id,
            "trusted-app",
            issue.revision,
            60,
            "awaiting-claim",
        )
        .await
        .unwrap();
    let handle = claimed.lease_handle.as_deref().unwrap();
    let completion = super::awaiting_review_completion("run-awaiting-1");

    let invalid = v1::AwaitingReviewCompletion {
        full_commit_id: "short".into(),
        ..completion.clone()
    };
    assert!(
        tracker
            .complete_execution_to_awaiting_review(
                issue.id,
                claimed.revision,
                "awaiting-operation-1",
                Some(handle),
                Some(claimed.revision),
                "trusted-app",
                Some("run-awaiting-1"),
                "gateway-correlation-1",
                &invalid,
            )
            .await
            .is_err()
    );
    let unchanged = tracker.get(issue.id).await.unwrap();
    assert_eq!(unchanged.state, IssueState::InProgress);
    assert_eq!(unchanged.revision, claimed.revision);
    assert!(unchanged.lease_owner.is_some());
    assert!(unchanged.lease_expires_at.is_some());

    tracker
        .complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            "awaiting-operation-1",
            Some(handle),
            Some(claimed.revision),
            "trusted-app",
            Some("run-awaiting-1"),
            "gateway-correlation-1",
            &completion,
        )
        .await
        .expect("valid active capability commits awaiting review");
    let committed = tracker.get(issue.id).await.unwrap();
    assert_eq!(committed.state, IssueState::AwaitingReview);
    assert_eq!(committed.revision, claimed.revision + 1);
    assert_eq!(committed.completed_at, None);
    assert_eq!(committed.lease_owner, None);
    assert_eq!(committed.lease_expires_at, None);
    assert_eq!(committed.lease_handle, None);
    assert!(tracker.ready(now()).await.unwrap().is_empty());

    let raw_task_status = {
        let mut replica = tracker.inner.lock().await;
        replica
            .get_task(issue.id)
            .await
            .unwrap()
            .unwrap()
            .get_status()
    };
    assert_eq!(raw_task_status, taskchampion::Status::Pending);
    let claim_attributes = {
        let mut replica = tracker.inner.lock().await;
        let task = replica.get_task(issue.id).await.unwrap().unwrap();
        [
            LEASE_OWNER_KEY,
            LEASE_OWNER_CONTEXT_KEY,
            LEASE_EXPIRES_KEY,
            LEASE_HANDLE_KEY,
        ]
        .map(|key| task.get_user_defined_attribute(key).is_some())
    };
    assert_eq!(claim_attributes, [false; 4]);

    let history = tracker.list_history(issue.id).await.unwrap();
    assert!(history.iter().any(|entry| {
        entry.state_change.as_ref().is_some_and(|change| {
            change.new_state == v1::IssueState::AwaitingReview as i32
                && change.awaiting_review.as_ref() == Some(&completion)
        })
    }));
    let first_marker = tracker
        .get_operation_record("awaiting-operation-1")
        .await
        .unwrap();
    assert_eq!(first_marker.resulting_revision, Some(committed.revision));

    // A committed operation can be replayed with its original idempotency
    // authority after its one-time capability has been consumed. It returns
    // the immutable original result and does not rewrite history/revision.
    tracker
        .complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            "awaiting-operation-1",
            None,
            Some(claimed.revision),
            "trusted-app",
            Some("run-awaiting-1"),
            "gateway-correlation-1",
            &completion,
        )
        .await
        .expect("same operation replays without an old lease handle");
    assert_eq!(
        tracker
            .get_operation_record("awaiting-operation-1")
            .await
            .unwrap(),
        first_marker
    );
    assert_eq!(
        tracker.get(issue.id).await.unwrap().revision,
        committed.revision
    );
}

#[tokio::test]
async fn awaiting_review_rejects_generic_transition_and_competing_claim_completions() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("awaiting race", "awaiting race")
        .await
        .unwrap();
    let generic = tracker
        .transition(
            issue.id,
            IssueState::AwaitingReview,
            issue.revision,
            "generic-awaiting-transition",
            None,
        )
        .await
        .expect_err("generic transition cannot enter the dedicated state");
    assert!(matches!(generic, TrackerError::InvalidStateTransition(_)));

    let claimed = tracker
        .claim(
            issue.id,
            "trusted-app",
            issue.revision,
            60,
            "awaiting-race-claim",
        )
        .await
        .unwrap();
    let handle = claimed.lease_handle.clone().unwrap();
    let run_one = super::awaiting_review_completion("run-race-1");
    let run_two = super::awaiting_review_completion("run-race-2");
    let (first, second) = tokio::join!(
        tracker.complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            "awaiting-race-operation-1",
            Some(&handle),
            Some(claimed.revision),
            "trusted-app",
            Some("run-race-1"),
            "gateway-correlation-1",
            &run_one,
        ),
        tracker.complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            "awaiting-race-operation-2",
            Some(&handle),
            Some(claimed.revision),
            "trusted-app",
            Some("run-race-2"),
            "gateway-correlation-2",
            &run_two,
        ),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(
        tracker.get(issue.id).await.unwrap().state,
        IssueState::AwaitingReview
    );
    assert_eq!(
        tracker
            .list_history(issue.id)
            .await
            .unwrap()
            .iter()
            .filter(|entry| {
                entry
                    .state_change
                    .as_ref()
                    .is_some_and(|change| change.new_state == v1::IssueState::AwaitingReview as i32)
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn active_in_progress_claim_allows_metadata_update() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("active update", "before").await.unwrap();
    let claimed = tracker
        .claim(issue.id, "worker", issue.revision, 60, "active-claim")
        .await
        .unwrap();
    let updated = tracker
        .update_issue_with_context(
            issue.id,
            &v1::IssuePatch {
                title: Some("after".into()),
                ..Default::default()
            },
            &[v1::IssueField::Title as i32],
            claimed.revision,
            "active-metadata-update",
            "worker",
            None,
            "active-update-correlation",
            None,
            None,
        )
        .await
        .expect("an active consistent claim permits metadata updates");
    assert_eq!(updated.state, IssueState::InProgress);
    assert_eq!(updated.title, "after");
    assert_eq!(updated.revision, claimed.revision + 1);
}

#[tokio::test]
async fn update_terminal_state_requires_and_records_matching_report() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("terminal update", "terminal update")
        .await
        .unwrap();
    let patch = v1::IssuePatch {
        title: Some("completed with report".into()),
        ..Default::default()
    };

    let error = tracker
        .update_issue_with_context(
            issue.id,
            &patch,
            &[v1::IssueField::Title as i32],
            issue.revision,
            "reject-terminal-update",
            "test",
            None,
            "correlation",
            Some(v1::IssueState::Done as i32),
            None,
        )
        .await
        .expect_err("terminal update without a completion report must fail");
    assert!(matches!(error, TrackerError::InvalidState(_)));

    let completion = v1::update_issue_args::TerminalReport::Completion(v1::CompletionReport {
        result: Some(v1::completion_report::Result::Verified(
            v1::VerifiedCompletion {
                evidence: vec![v1::CompletionEvidence {
                    kind: v1::CompletionEvidenceKind::OtherEvidence as i32,
                    summary: "confirmed through update".into(),
                    uri: None,
                }],
            },
        )),
    });
    let updated = tracker
        .update_issue_with_context(
            issue.id,
            &patch,
            &[v1::IssueField::Title as i32],
            issue.revision,
            "complete-terminal-update",
            "test",
            None,
            "correlation",
            Some(v1::IssueState::Done as i32),
            Some(&completion),
        )
        .await
        .expect("terminal update with a completion report must succeed");
    assert_eq!(updated.state, IssueState::Done);

    let history = tracker.list_history(issue.id).await.unwrap();
    assert!(history.iter().any(|entry| {
        entry
            .state_change
            .as_ref()
            .and_then(|change| change.completion.as_ref())
            .is_some()
    }));
}

#[tokio::test]
async fn ordinary_terminal_update_cannot_clear_an_inconsistent_claim() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("inconsistent", "inconsistent lease")
        .await
        .unwrap();
    {
        let mut replica = tracker.inner.lock().await;
        let mut task = replica.get_task(issue.id).await.unwrap().unwrap();
        let mut ops = Operations::new();
        task.set_user_defined_attribute(LEASE_HANDLE_KEY, "orphaned-handle", &mut ops)
            .unwrap();
        replica.commit_operations(ops).await.unwrap();
    }
    let cancellation =
        v1::update_issue_args::TerminalReport::Cancellation(v1::CancellationReport {
            classification: v1::CancellationClass::Other as i32,
        });
    let error = tracker
        .update_issue_with_context(
            issue.id,
            &v1::IssuePatch {
                title: Some("must not terminate".into()),
                ..Default::default()
            },
            &[v1::IssueField::Title as i32],
            issue.revision,
            "ordinary-terminal-inconsistent",
            "human",
            None,
            "test",
            Some(v1::IssueState::Canceled as i32),
            Some(&cancellation),
        )
        .await
        .expect_err("ordinary terminal updates must not erase inconsistent leases");
    assert!(matches!(
        error,
        TrackerError::LeaseInconsistent {
            invalid_attributes, ..
        }
            if invalid_attributes == vec![
                "lb.lease_owner_hash".to_owned(),
                "lb.lease_handle_hash".to_owned(),
                "lb.lease_expires_at".to_owned(),
            ]
    ));
    let current = tracker.get(issue.id).await.unwrap();
    assert_eq!(current.state, IssueState::Open);
    assert_eq!(current.revision, issue.revision);
}

#[tokio::test]
async fn canceled_issue_reopens_without_current_completion_but_keeps_history() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("reopen", "reopen").await.unwrap();
    let claimed = tracker
        .claim(
            issue.id,
            "worker",
            issue.revision,
            60,
            "claim-before-cancel",
        )
        .await
        .expect("claim must succeed before cancellation");
    let old_lease_handle = claimed
        .lease_handle
        .clone()
        .expect("claim must expose an ephemeral lease handle");
    let cancellation =
        v1::transition_issue_args::TerminalReport::Cancellation(v1::CancellationReport {
            classification: v1::CancellationClass::Other as i32,
        });
    let canceled = tracker
        .admin_terminate(
            issue.id,
            IssueState::Canceled,
            claimed.revision,
            "cancel-for-reopen",
            "human",
            "no longer relevant",
            &cancellation,
            false,
            false,
        )
        .await
        .expect("cancellation must succeed");
    assert!(canceled.completed_at.is_some());
    assert_eq!(canceled.lease_expires_at, None);

    let reopened = tracker
        .transition_with_context(
            issue.id,
            IssueState::Open,
            canceled.revision,
            "reopen-canceled",
            None,
            None,
            "human",
            None,
            "reopen-test",
            Some("reopened after review"),
            None,
        )
        .await
        .expect("canceled issue must reopen");
    assert_eq!(reopened.state, IssueState::Open);
    assert_eq!(reopened.completed_at, None);
    assert_eq!(reopened.lease_expires_at, None);
    assert!(matches!(
        tracker
            .transition_with_context(
                issue.id,
                IssueState::Deferred,
                reopened.revision,
                "stale-gateway-capability",
                Some(&old_lease_handle),
                Some(claimed.revision),
                "worker",
                None,
                "reopen-test",
                None,
                None,
            )
            .await,
        Err(TrackerError::InvalidLease { .. })
    ));

    let replay = tracker
        .transition_with_context(
            issue.id,
            IssueState::Open,
            canceled.revision,
            "reopen-canceled",
            None,
            None,
            "human",
            None,
            "reopen-test",
            Some("reopened after review"),
            None,
        )
        .await
        .expect("same reopen operation must replay");
    assert_eq!(replay.revision, reopened.revision);
    assert!(matches!(
        tracker
            .transition(
                issue.id,
                IssueState::Open,
                canceled.revision,
                "stale-reopen",
                None,
            )
            .await,
        Err(TrackerError::RevisionConflict { .. })
    ));
    assert!(matches!(
        tracker
            .transition(
                issue.id,
                IssueState::Deferred,
                canceled.revision,
                "reopen-canceled",
                None,
            )
            .await,
        Err(TrackerError::IdempotencyConflict)
    ));
    let reclaimed = tracker
        .claim(
            issue.id,
            "replacement-worker",
            reopened.revision,
            60,
            "claim-after-reopen",
        )
        .await
        .expect("reopened issue must be immediately claimable");
    assert_eq!(reclaimed.state, IssueState::InProgress);

    let history = tracker.list_history(issue.id).await.unwrap();
    assert!(history.iter().any(|entry| {
        entry
            .state_change
            .as_ref()
            .and_then(|change| change.cancellation.as_ref())
            .is_some()
    }));
    assert!(history.iter().any(|entry| {
        entry.state_change.as_ref().is_some_and(|change| {
            change.old_state == Some(v1::IssueState::Canceled as i32)
                && change.new_state == v1::IssueState::Open as i32
        })
    }));
}

#[tokio::test]
async fn done_to_open_is_rejected_through_transition_and_update() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("done-reopen", "done reopen").await.unwrap();
    let done = tracker
        .transition(issue.id, IssueState::Done, issue.revision, "finish", None)
        .await
        .unwrap();
    assert!(matches!(
        tracker
            .transition(
                issue.id,
                IssueState::Open,
                done.revision,
                "reopen-done",
                None
            )
            .await,
        Err(TrackerError::InvalidStateTransition(_))
    ));
    assert!(matches!(
        tracker
            .update_issue_with_context(
                issue.id,
                &v1::IssuePatch {
                    title: Some("still done".into()),
                    ..Default::default()
                },
                &[v1::IssueField::Title as i32],
                done.revision,
                "update-reopen-done",
                "human",
                None,
                "done-reopen",
                Some(v1::IssueState::Open as i32),
                None,
            )
            .await,
        Err(TrackerError::InvalidStateTransition(_))
    ));
}

#[tokio::test]
async fn active_claim_requires_a_lease_handle_to_complete() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("complete-claimed", "complete claimed")
        .await
        .unwrap();
    let claimed = tracker
        .claim(
            issue.id,
            "worker",
            issue.revision,
            60,
            "claim-for-completion",
        )
        .await
        .unwrap();
    let lease_handle = claimed.lease_handle.as_deref().unwrap();

    assert!(matches!(
        tracker
            .transition(
                issue.id,
                IssueState::Done,
                claimed.revision,
                "complete-without-lease-handle",
                None,
            )
            .await,
        Err(TrackerError::InvalidLease { .. })
    ));
    assert_eq!(
        tracker
            .transition(
                issue.id,
                IssueState::Done,
                claimed.revision,
                "complete-with-lease-handle",
                Some(lease_handle),
            )
            .await
            .unwrap()
            .state,
        IssueState::Done
    );
}

#[tokio::test]
async fn ordinary_human_transition_cannot_bypass_an_active_claim() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("human bypass", "human bypass")
        .await
        .unwrap();
    let claimed = tracker
        .claim(issue.id, "worker", issue.revision, 60, "claim")
        .await
        .unwrap();
    let report = v1::transition_issue_args::TerminalReport::Cancellation(v1::CancellationReport {
        classification: v1::CancellationClass::Other as i32,
    });

    assert!(matches!(
        tracker
            .transition_with_context(
                issue.id,
                IssueState::Canceled,
                claimed.revision,
                "human-cancel",
                None,
                None,
                "human",
                None,
                "human-cancel",
                Some("ordinary transition"),
                Some(&report),
            )
            .await,
        Err(TrackerError::InvalidLease { .. })
    ));
}

#[tokio::test]
async fn active_claim_is_required_for_terminal_transition_after_blocking() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("blocked claim", "blocked claim")
        .await
        .unwrap();
    let claimed = tracker
        .claim(issue.id, "worker", issue.revision, 60, "claim")
        .await
        .unwrap();
    let blocked = tracker
        .transition(
            issue.id,
            IssueState::Blocked,
            claimed.revision,
            "block-work",
            Some(claimed.lease_handle.as_deref().unwrap()),
        )
        .await
        .unwrap();

    assert!(matches!(
        tracker
            .transition(
                issue.id,
                IssueState::Done,
                blocked.revision,
                "complete-without-capability",
                None,
            )
            .await,
        Err(TrackerError::InvalidLease { .. })
    ));
}

#[tokio::test]
async fn active_issue_projection_hides_stale_completion_timestamp() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("stale-completion", "stale completion")
        .await
        .unwrap();
    let mut replica = tracker.inner.lock().await;
    let mut task = replica.get_task(issue.id).await.unwrap().unwrap();
    let mut metadata = metadata_from_task(&task).unwrap();
    metadata.completed_at = Some(123);
    let mut operations = Operations::new();
    set_metadata(&mut task, &metadata, &mut operations).unwrap();
    replica.commit_operations(operations).await.unwrap();
    drop(replica);

    let projected = tracker.get(issue.id).await.unwrap();
    assert_eq!(projected.state, IssueState::Open);
    assert_eq!(projected.completed_at, None);
}

#[tokio::test]
async fn metadata_gate_and_backend_neutral_export_are_available() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("export", "export me").await.unwrap();
    tracker
        .transition(
            issue.id,
            IssueState::Deferred,
            issue.revision,
            "export-transition",
            None,
        )
        .await
        .unwrap();
    let exported: ExportBundle = serde_json::from_slice(&tracker.export().await.unwrap()).unwrap();
    assert_eq!(exported.contract, CONTRACT_VERSION);
    assert_eq!(exported.schema_fingerprint, Tracker::schema_fingerprint());
    assert_eq!(exported.issues.len(), 1);
    assert_eq!(exported.issues[0].state, IssueState::Deferred);
    assert!(exported.history.iter().any(|record| {
        record.get("operation_id").and_then(Value::as_str) == Some("export-transition")
            && record.get("old_state").and_then(Value::as_str) == Some("open")
            && record.get("new_state").and_then(Value::as_str) == Some("deferred")
    }));
}

#[tokio::test]
async fn raw_maintenance_scan_reports_bad_entries_without_hiding_expired_claims() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let expired = tracker.create("expired", "expired lease").await.unwrap();
    tracker
        .claim(expired.id, "worker", expired.revision, 30, "expired-claim")
        .await
        .unwrap();
    let malformed = tracker.create("malformed", "bad raw state").await.unwrap();
    {
        let mut replica = tracker.inner.lock().await;
        let mut task = replica.get_task(malformed.id).await.unwrap().unwrap();
        let mut ops = Operations::new();
        task.set_user_defined_attribute(STATE_KEY, "not-a-state", &mut ops)
            .unwrap();
        replica.commit_operations(ops).await.unwrap();
    }

    let entries = tracker
        .lease_maintenance_candidates(u64::MAX)
        .await
        .unwrap();

    assert!(entries.iter().any(|entry| {
        matches!(
            entry,
            LeaseMaintenanceScanEntry::Valid(candidate)
                if candidate.id == expired.id && candidate.lease_state == LeaseState::Expired
        )
    }));
    assert!(entries.iter().any(|entry| {
        matches!(
            entry,
            LeaseMaintenanceScanEntry::Invalid {
                issue_id,
                classification: "invalid_state",
                ..
            } if *issue_id == malformed.id
        )
    }));
}

#[tokio::test]
async fn ownership_maintenance_assigns_a_stable_reason_per_candidate() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let expired = tracker
        .create("expired reason", "expired lease")
        .await
        .unwrap();
    tracker
        .claim(
            expired.id,
            "worker",
            expired.revision,
            0,
            "expired-reason-claim",
        )
        .await
        .unwrap();

    let unowned = tracker
        .create("unowned reason", "in progress without lease")
        .await
        .unwrap();
    {
        let mut replica = tracker.inner.lock().await;
        let mut task = replica.get_task(unowned.id).await.unwrap().unwrap();
        let mut ops = Operations::new();
        task.set_user_defined_attribute(STATE_KEY, IssueState::InProgress.as_str(), &mut ops)
            .unwrap();
        replica.commit_operations(ops).await.unwrap();
    }

    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_for_callback = Arc::clone(&observed);
    let stats = super::super::ownership_maintenance::run_with_recovery_stats(
        &tracker,
        None,
        10,
        &move |issue_id, _revision, _operation_id, _state, reason| {
            let observed = Arc::clone(&observed_for_callback);
            async move {
                observed
                    .lock()
                    .unwrap()
                    .push((issue_id, reason.as_str().to_owned()));
                Ok(())
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(stats.recovered, 2);
    let observed = observed.lock().unwrap();
    assert!(observed.contains(&(expired.id, "expired_claim".to_owned())));
    assert!(observed.contains(&(unowned.id, "unowned_in_progress".to_owned())));
}

#[tokio::test]
async fn ownership_maintenance_only_diagnoses_inconsistent_leases_and_continues() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let inconsistent = tracker
        .create("inconsistent lease", "diagnose only")
        .await
        .unwrap();
    {
        let mut replica = tracker.inner.lock().await;
        let mut task = replica.get_task(inconsistent.id).await.unwrap().unwrap();
        let mut ops = Operations::new();
        task.set_user_defined_attribute(LEASE_OWNER_KEY, "not-a-lease-hash", &mut ops)
            .unwrap();
        replica.commit_operations(ops).await.unwrap();
    }

    let expired = tracker
        .create(
            "expired after inconsistent",
            "still process later candidates",
        )
        .await
        .unwrap();
    tracker
        .claim(
            expired.id,
            "worker",
            expired.revision,
            0,
            "expired-after-inconsistent",
        )
        .await
        .unwrap();

    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_for_callback = Arc::clone(&observed);
    let stats = super::super::ownership_maintenance::run_with_recovery_stats(
        &tracker,
        None,
        10,
        &move |issue_id, _revision, _operation_id, _state, reason| {
            let observed = Arc::clone(&observed_for_callback);
            async move {
                observed
                    .lock()
                    .unwrap()
                    .push((issue_id, reason.as_str().to_owned()));
                Ok(())
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(stats.diagnoses, 1);
    assert_eq!(stats.recovered, 1);
    let observed = observed.lock().unwrap();
    assert!(
        !observed
            .iter()
            .any(|(issue_id, _)| *issue_id == inconsistent.id)
    );
    assert!(observed.contains(&(expired.id, "expired_claim".to_owned())));
}

#[tokio::test]
async fn metadata_corruption_fails_tracker_open() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    drop(tracker);
    let storage = SqliteStorage::new(root.path(), AccessMode::ReadWrite, true)
        .await
        .unwrap();
    let mut replica = Replica::new(storage);
    let mut tasks = replica.all_tasks().await.unwrap();
    let metadata = tasks
        .values_mut()
        .find(|task| task.get_user_defined_attribute(ENTITY_KEY) == Some(METADATA_ENTITY))
        .unwrap();
    let mut ops = Operations::new();
    metadata
        .set_user_defined_attribute(SCHEMA_VERSION_KEY, "999", &mut ops)
        .unwrap();
    replica.commit_operations(ops).await.unwrap();
    drop(replica);
    assert!(matches!(
        Tracker::open(root.path()).await,
        Err(TrackerError::InvalidState(_))
    ));
}

#[tokio::test]
async fn prior_mapping_schema_root_is_rejected_without_rewriting_it() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    drop(tracker);
    let storage = SqliteStorage::new(root.path(), AccessMode::ReadWrite, true)
        .await
        .unwrap();
    let mut replica = Replica::new(storage);
    let mut tasks = replica.all_tasks().await.unwrap();
    let metadata = tasks
        .values_mut()
        .find(|task| task.get_user_defined_attribute(ENTITY_KEY) == Some(METADATA_ENTITY))
        .unwrap();
    let mut ops = Operations::new();
    metadata
        .set_user_defined_attribute(SCHEMA_VERSION_KEY, "2", &mut ops)
        .unwrap();
    replica.commit_operations(ops).await.unwrap();
    drop(replica);

    assert!(matches!(
        Tracker::open(root.path()).await,
        Err(TrackerError::InvalidState(_))
    ));
    let reopened = SqliteStorage::new(root.path(), AccessMode::ReadWrite, true)
        .await
        .unwrap();
    let mut replica = Replica::new(reopened);
    let tasks = replica.all_tasks().await.unwrap();
    assert_eq!(
        tasks
            .values()
            .find(|task| task.get_user_defined_attribute(ENTITY_KEY) == Some(METADATA_ENTITY))
            .and_then(|task| task.get_user_defined_attribute(SCHEMA_VERSION_KEY)),
        Some("2")
    );
}

#[tokio::test]
async fn all_canonical_states_round_trip() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("states", "states").await.unwrap();
    let claimed = tracker
        .claim(issue.id, "state-owner", issue.revision, 30, "state-claim")
        .await
        .unwrap();
    let states = [
        IssueState::Blocked,
        IssueState::Deferred,
        IssueState::Open,
        IssueState::Canceled,
        IssueState::Done,
    ];
    assert_eq!(claimed.state, IssueState::InProgress);
    let mut revision = claimed.revision;
    for state in states {
        let operation_id = format!("state-{}", state.as_str());
        let updated = if state == IssueState::Canceled {
            tracker
                .admin_terminate(
                    issue.id,
                    state,
                    revision,
                    &operation_id,
                    "human",
                    "administrative cancellation",
                    &v1::transition_issue_args::TerminalReport::Cancellation(
                        v1::CancellationReport {
                            classification: v1::CancellationClass::Other as i32,
                        },
                    ),
                    false,
                    false,
                )
                .await
        } else {
            tracker
                .transition(issue.id, state, revision, &operation_id, None)
                .await
        }
        .unwrap();
        assert_eq!(updated.state, state);
        revision = updated.revision;
    }
}

#[tokio::test]
async fn ready_excludes_deferred_and_blocked_issues() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let blocker = tracker.create("a", "blocker").await.unwrap();
    let blocked = tracker.create("b", "blocked").await.unwrap();
    tracker
        .relate(blocker.id, RelationKind::Blocks, blocked.id, 0, 0, "rel-1")
        .await
        .unwrap();
    assert_eq!(tracker.ready(now()).await.unwrap().len(), 1);
    tracker
        .transition(blocker.id, IssueState::Done, 0, "done-1", None)
        .await
        .unwrap();
    assert_eq!(tracker.ready(now()).await.unwrap().len(), 1);
}

#[tokio::test]
async fn blocked_transition_reports_unresolved_dependency_issue_ids() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let blocker = tracker.create("blocker", "blocker").await.unwrap();
    let blocked = tracker.create("blocked", "blocked").await.unwrap();
    tracker
        .relate(
            blocker.id,
            RelationKind::Blocks,
            blocked.id,
            0,
            0,
            "blocker-rel",
        )
        .await
        .unwrap();

    let error = tracker
        .transition(blocked.id, IssueState::Done, 1, "blocked-done", None)
        .await
        .expect_err("an unresolved dependency must reject completion");
    assert!(matches!(
        error,
        TrackerError::Blocked {
            ref open_blocker_issue_ids
        } if *open_blocker_issue_ids == vec![blocker.id]
    ));

    let wire = crate::tracker::error_response::tracker_error(error);
    assert!(matches!(
        wire.detail,
        Some(v1::issue_tracker_error::Detail::BlockerConflict(conflict))
            if conflict.open_blocker_issue_ids == vec![blocker.id.to_string()]
    ));
}

#[tokio::test]
async fn blocks_operation_records_are_anchored_to_the_target_and_legacy_records_fail_closed() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let source = tracker.create("source", "source").await.unwrap();
    let target = tracker.create("target", "target").await.unwrap();

    tracker
        .relate_with_context(
            source.id,
            RelationKind::Blocks,
            target.id,
            source.revision,
            target.revision,
            "blocks-add",
            "actor",
            None,
            "correlation",
        )
        .await
        .unwrap();
    let add = tracker.get_operation_record("blocks-add").await.unwrap();
    assert_eq!(add.affected_issue_ids, vec![target.id.to_string()]);
    assert_eq!(add.entity_revisions[0].issue_id, target.id.to_string());
    assert_eq!(add.entity_revisions[0].resulting_revision, 1);

    tracker
        .unrelate_with_context(
            source.id,
            RelationKind::Blocks,
            target.id,
            source.revision,
            1,
            "blocks-remove",
            "actor",
            None,
            "correlation",
        )
        .await
        .unwrap();
    let remove = tracker.get_operation_record("blocks-remove").await.unwrap();
    assert_eq!(remove.affected_issue_ids, vec![target.id.to_string()]);
    assert_eq!(remove.entity_revisions[0].resulting_revision, 2);

    let mut replica = tracker.inner.lock().await;
    let mut target_task = replica.get_task(target.id).await.unwrap().unwrap();
    let mut operations = Operations::new();
    target_task
        .remove_user_defined_attribute("lb.history.blocks-remove", &mut operations)
        .unwrap();
    target_task
        .set_user_defined_attribute(
            "lb.history.blocks-remove",
            serde_json::json!({"kind": "unrelate"}).to_string(),
            &mut operations,
        )
        .unwrap();
    replica.commit_operations(operations).await.unwrap();
    drop(replica);

    let legacy = tracker.get_operation_record("blocks-remove").await.unwrap();
    assert_eq!(legacy.scope, v1::OperationScope::Tracker as i32);
    assert!(legacy.affected_issue_ids.is_empty());
}

#[tokio::test]
async fn non_blocks_operation_records_are_anchored_to_the_source() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let source = tracker.create("source", "source").await.unwrap();
    let target = tracker.create("target", "target").await.unwrap();

    tracker
        .relate_with_context(
            source.id,
            RelationKind::Related,
            target.id,
            source.revision,
            target.revision,
            "related-add",
            "actor",
            None,
            "correlation",
        )
        .await
        .unwrap();

    let record = tracker.get_operation_record("related-add").await.unwrap();
    assert_eq!(record.affected_issue_ids, vec![source.id.to_string()]);
    assert_eq!(record.entity_revisions[0].issue_id, source.id.to_string());
    assert_eq!(record.entity_revisions[0].resulting_revision, 1);
}

#[tokio::test]
async fn all_relations_round_trip_and_blocking_cycle_is_rejected() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let a = tracker.create("relation-a", "a").await.unwrap();
    let b = tracker.create("relation-b", "b").await.unwrap();
    for (index, relation) in [
        RelationKind::ParentChild,
        RelationKind::Related,
        RelationKind::DiscoveredFrom,
        RelationKind::Supersedes,
    ]
    .into_iter()
    .enumerate()
    {
        tracker
            .relate(
                a.id,
                relation,
                b.id,
                index as u64,
                index as u64,
                &format!("relation-{index}"),
            )
            .await
            .unwrap();
    }
    tracker
        .relate(a.id, RelationKind::Blocks, b.id, 4, 4, "blocks-1")
        .await
        .unwrap();
    assert!(
        tracker
            .relate(b.id, RelationKind::Blocks, a.id, 4, 4, "blocks-2")
            .await
            .is_err()
    );
    let issue = tracker.get(a.id).await.unwrap();
    assert_eq!(issue.relations.len(), 5);
    assert!(
        issue
            .relations
            .iter()
            .all(|relation| { relation.source == a.id && relation.target == b.id })
    );
    let reverse = tracker.get(b.id).await.unwrap();
    assert!(
        reverse
            .relations
            .iter()
            .all(|relation| { relation.source == a.id && relation.target == b.id })
    );
}

#[tokio::test]
async fn concurrent_claim_has_one_winner() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("claim", "claim").await.unwrap();
    let a = tracker.clone();
    let b = tracker.clone();
    let first = tokio::spawn(async move { a.claim(issue.id, "a", 0, 30, "claim-a").await });
    let second = tokio::spawn(async move { b.claim(issue.id, "b", 0, 30, "claim-b").await });
    let results = [first.await.unwrap(), second.await.unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
}

#[tokio::test]
async fn lease_release_and_revision_are_enforced() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("lease", "lease").await.unwrap();
    let claimed = tracker
        .claim(issue.id, "owner", 0, 30, "claim")
        .await
        .unwrap();
    assert!(
        tracker
            .release(
                issue.id,
                "other",
                "invalid",
                claimed.revision,
                "release-other"
            )
            .await
            .is_err()
    );
    let released = tracker
        .release(
            issue.id,
            "owner",
            claimed.lease_handle.as_deref().unwrap(),
            claimed.revision,
            "release",
        )
        .await
        .unwrap();
    assert_eq!(released.state, IssueState::Open);
    assert!(
        tracker
            .transition(issue.id, IssueState::Done, 0, "stale", None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn release_preserves_a_state_changed_after_the_claim() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("release-state", "release state")
        .await
        .unwrap();
    let claimed = tracker
        .claim(issue.id, "owner", issue.revision, 30, "claim")
        .await
        .unwrap();
    let blocked = tracker
        .transition(
            issue.id,
            IssueState::Blocked,
            claimed.revision,
            "block-after-claim",
            None,
        )
        .await
        .unwrap();

    let released = tracker
        .release(
            issue.id,
            "owner",
            claimed.lease_handle.as_deref().unwrap(),
            blocked.revision,
            "release-after-block",
        )
        .await
        .unwrap();

    assert_eq!(released.state, IssueState::Blocked);
    assert_eq!(released.revision, blocked.revision + 1);
    assert!(released.lease_expires_at.is_none());
}

#[tokio::test]
async fn release_returns_an_active_in_progress_claim_to_open() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("release-to-open", "release to open")
        .await
        .unwrap();
    let claimed = tracker
        .claim(issue.id, "owner", issue.revision, 30, "claim")
        .await
        .unwrap();

    let released = tracker
        .release(
            issue.id,
            "owner",
            claimed.lease_handle.as_deref().unwrap(),
            claimed.revision,
            "release",
        )
        .await
        .unwrap();

    assert_eq!(released.state, IssueState::Open);
    assert!(released.lease_expires_at.is_none());
}

#[tokio::test]
async fn recovery_refuses_active_claims_unless_explicitly_revoked() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("recover", "recover").await.unwrap();
    let claimed = tracker
        .claim(issue.id, "owner", issue.revision, 30, "claim")
        .await
        .unwrap();

    assert!(
        tracker
            .recover_claim(
                issue.id,
                claimed.revision,
                false,
                "recover-active",
                "system-maintenance",
                "expired ownership scan",
            )
            .await
            .is_err()
    );

    let revoked = tracker
        .recover_claim(
            issue.id,
            claimed.revision,
            true,
            "revoke-active",
            "administrator",
            "worker stopped",
        )
        .await
        .unwrap();
    assert_eq!(revoked.state, IssueState::Open);
    assert!(revoked.lease_expires_at.is_none());
    let history = tracker.list_history(issue.id).await.unwrap();
    let receipt = history
        .iter()
        .find(|entry| entry.downstream_operation_id == "revoke-active")
        .expect("recovery receipt must be retained in issue history");
    assert!(matches!(
        receipt.state_change.as_ref(),
        Some(v1::StateHistoryDetail {
            old_state: Some(state),
            new_state,
            ..
        }) if *state == v1::IssueState::InProgress as i32
            && *new_state == v1::IssueState::Open as i32
    ));
    let record = tracker.get_operation_record("revoke-active").await.unwrap();
    assert_eq!(record.method_key, "apply_issue_admin_action");
    assert!(matches!(
        record.result_snapshot,
        Some(v1::OperationResultSnapshot {
            value: Some(v1::operation_result_snapshot::Value::IssueMutation(
                v1::IssueMutationSnapshot {
                    previous_state: Some(state),
                    ..
                }
            )),
            ..
        }) if state == v1::IssueState::InProgress as i32
    ));
}

#[tokio::test]
async fn administrative_cancellation_ends_an_actively_claimed_issue_atomically() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("admin cancel", "admin cancel")
        .await
        .unwrap();
    let claimed = tracker
        .claim(issue.id, "owner", issue.revision, 30, "claim")
        .await
        .unwrap();
    let report = v1::transition_issue_args::TerminalReport::Cancellation(v1::CancellationReport {
        classification: v1::CancellationClass::Other as i32,
    });

    let canceled = tracker
        .admin_terminate(
            issue.id,
            IssueState::Canceled,
            claimed.revision,
            "admin-cancel",
            "administrator",
            "human cancellation",
            &report,
            false,
            false,
        )
        .await
        .unwrap();

    assert_eq!(canceled.state, IssueState::Canceled);
    assert_eq!(canceled.revision, claimed.revision + 1);
    assert!(canceled.lease_expires_at.is_none());
}

#[tokio::test]
async fn administrative_completion_without_claim_reports_no_ownership_repair() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("admin completion without claim", "no lease")
        .await
        .unwrap();
    let report = v1::transition_issue_args::TerminalReport::Completion(v1::CompletionReport {
        result: Some(v1::completion_report::Result::Verified(
            v1::VerifiedCompletion {
                evidence: vec![v1::CompletionEvidence {
                    kind: v1::CompletionEvidenceKind::OtherEvidence as i32,
                    summary: "completed without a claim".into(),
                    uri: None,
                }],
            },
        )),
    });

    let completed = tracker
        .admin_terminate(
            issue.id,
            IssueState::Done,
            issue.revision,
            "admin-complete-without-claim",
            "administrator",
            "human completion",
            &report,
            false,
            false,
        )
        .await
        .unwrap();
    assert_eq!(completed.state, IssueState::Done);
    assert!(completed.lease_expires_at.is_none());

    let marker = tracker
        .get_operation_record("admin-complete-without-claim")
        .await
        .unwrap();
    let receipt = manager_v1::IssueAdminActionResult::decode(
        marker
            .result_snapshot
            .as_ref()
            .expect("admin marker has a typed receipt")
            .manager_result_payload
            .as_slice(),
    )
    .unwrap();
    assert_eq!(receipt.observed_lease_classification, "NONE");
    assert!(!receipt.ownership_cleared);
    assert!(receipt.repaired_ownership_attributes.is_empty());
    assert!(matches!(
        marker
            .result_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.value.as_ref()),
        Some(v1::operation_result_snapshot::Value::IssueMutation(snapshot))
            if snapshot.changed_fields.is_empty()
    ));
}

#[tokio::test]
async fn administrative_completion_with_inconsistent_claim_resolution_is_atomic() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("inconsistent completion", "inconsistent completion")
        .await
        .unwrap();
    {
        let mut replica = tracker.inner.lock().await;
        let mut task = replica.get_task(issue.id).await.unwrap().unwrap();
        let mut ops = Operations::new();
        task.set_user_defined_attribute(LEASE_HANDLE_KEY, "orphaned-handle", &mut ops)
            .unwrap();
        replica.commit_operations(ops).await.unwrap();
    }
    let report = v1::transition_issue_args::TerminalReport::Completion(v1::CompletionReport {
        result: Some(v1::completion_report::Result::Verified(
            v1::VerifiedCompletion {
                evidence: vec![v1::CompletionEvidence {
                    kind: v1::CompletionEvidenceKind::OtherEvidence as i32,
                    summary: "completed after explicit lease repair".into(),
                    uri: None,
                }],
            },
        )),
    });

    let completed = tracker
        .admin_terminate_with_overrides(
            issue.id,
            IssueState::Done,
            issue.revision,
            "admin-inconsistent-complete",
            "administrator",
            "complete after explicit inconsistent lease resolution",
            &report,
            false,
            true,
            None,
            Some("clear the orphaned lease handle before completion"),
        )
        .await
        .expect("explicit lease resolution and completion must commit together");
    assert_eq!(completed.state, IssueState::Done);
    assert_eq!(completed.revision, issue.revision + 1);
    assert!(completed.lease_owner.is_none());
    assert!(completed.lease_handle.is_none());
    assert!(completed.lease_expires_at.is_none());

    let marker = tracker
        .get_operation_record("admin-inconsistent-complete")
        .await
        .unwrap();
    let receipt = manager_v1::IssueAdminActionResult::decode(
        marker
            .result_snapshot
            .as_ref()
            .expect("the atomic write must include the typed admin receipt")
            .manager_result_payload
            .as_slice(),
    )
    .unwrap();
    assert_eq!(receipt.observed_lease_classification, "INCONSISTENT");
    assert!(receipt.inconsistent_claim_resolution_applied);
    assert!(receipt.ownership_cleared);
    assert_eq!(
        receipt.repaired_ownership_attributes,
        vec!["lb.lease_handle_hash"]
    );
    assert!(
        tracker
            .claim(
                issue.id,
                "late-worker",
                completed.revision,
                30,
                "late-claim-after-admin-complete",
            )
            .await
            .is_err()
    );

    let history = tracker.list_history(issue.id).await.unwrap();
    let terminal = history
        .iter()
        .find(|entry| entry.downstream_operation_id == "admin-inconsistent-complete")
        .expect("the terminal transition must have one history record");
    assert!(matches!(
        terminal.state_change.as_ref(),
        Some(v1::StateHistoryDetail {
            old_state: Some(old_state),
            new_state,
            ..
        }) if *old_state == v1::IssueState::Open as i32
            && *new_state == v1::IssueState::Done as i32
    ));
}

#[tokio::test]
async fn administrative_termination_covers_all_nonterminal_states_and_lease_classes() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let states = [
        IssueState::Open,
        IssueState::InProgress,
        IssueState::Blocked,
        IssueState::Deferred,
    ];
    let lease_classes = ["NONE", "ACTIVE", "EXPIRED"];

    for state in states {
        for lease_class in lease_classes {
            for terminal_state in [IssueState::Done, IssueState::Canceled] {
                let issue = tracker
                    .create(
                        &format!("admin matrix {state:?} {lease_class} {terminal_state:?}"),
                        "administrative termination matrix",
                    )
                    .await
                    .unwrap();
                let mut current = issue.clone();
                let mut lease_handle = None;
                let needs_claim = state != IssueState::Open || lease_class != "NONE";
                if needs_claim {
                    current = tracker
                        .claim(
                            issue.id,
                            "matrix-owner",
                            current.revision,
                            30,
                            &format!("matrix-claim-{state:?}-{lease_class}-{terminal_state:?}"),
                        )
                        .await
                        .unwrap();
                    lease_handle = current.lease_handle.clone();
                }
                if state != IssueState::InProgress {
                    current = tracker
                        .transition(
                            issue.id,
                            state,
                            current.revision,
                            &format!("matrix-state-{state:?}-{lease_class}-{terminal_state:?}"),
                            None,
                        )
                        .await
                        .unwrap();
                }
                if lease_class == "NONE" {
                    if state == IssueState::InProgress {
                        let mut replica = tracker.inner.lock().await;
                        let mut task = replica.get_task(issue.id).await.unwrap().unwrap();
                        let mut ops = Operations::new();
                        for key in [
                            LEASE_OWNER_KEY,
                            LEASE_OWNER_CONTEXT_KEY,
                            LEASE_HANDLE_KEY,
                            LEASE_EXPIRES_KEY,
                        ] {
                            task.remove_user_defined_attribute(key, &mut ops).unwrap();
                        }
                        replica.commit_operations(ops).await.unwrap();
                    } else if state != IssueState::Open {
                        current = tracker
                            .release(
                                issue.id,
                                "matrix-owner",
                                lease_handle.as_deref().unwrap(),
                                current.revision,
                                &format!("matrix-release-{state:?}-{terminal_state:?}"),
                            )
                            .await
                            .unwrap();
                    }
                } else if lease_class == "EXPIRED" {
                    let mut replica = tracker.inner.lock().await;
                    let mut task = replica.get_task(issue.id).await.unwrap().unwrap();
                    let mut ops = Operations::new();
                    task.set_user_defined_attribute(LEASE_EXPIRES_KEY, "0", &mut ops)
                        .unwrap();
                    replica.commit_operations(ops).await.unwrap();
                }

                let terminal_report = if terminal_state == IssueState::Done {
                    v1::transition_issue_args::TerminalReport::Completion(v1::CompletionReport {
                        result: Some(v1::completion_report::Result::Verified(
                            v1::VerifiedCompletion {
                                evidence: vec![v1::CompletionEvidence {
                                    kind: v1::CompletionEvidenceKind::OtherEvidence as i32,
                                    summary: "matrix completion evidence".into(),
                                    uri: None,
                                }],
                            },
                        )),
                    })
                } else {
                    v1::transition_issue_args::TerminalReport::Cancellation(
                        v1::CancellationReport {
                            classification: v1::CancellationClass::Other as i32,
                        },
                    )
                };
                let operation_id =
                    format!("matrix-admin-{state:?}-{lease_class}-{terminal_state:?}");
                let terminated = tracker
                    .admin_terminate_with_overrides(
                        issue.id,
                        terminal_state,
                        current.revision,
                        &operation_id,
                        "administrator",
                        "terminate the issue after reviewing ownership state",
                        &terminal_report,
                        false,
                        false,
                        None,
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(terminated.state, terminal_state);
                assert_eq!(terminated.revision, current.revision + 1);
                assert!(terminated.lease_owner.is_none());
                assert!(terminated.lease_handle.is_none());
                assert!(terminated.lease_expires_at.is_none());
                let marker = tracker.get_operation_record(&operation_id).await.unwrap();
                let receipt = manager_v1::IssueAdminActionResult::decode(
                    marker
                        .result_snapshot
                        .as_ref()
                        .expect("matrix termination must persist a typed receipt")
                        .manager_result_payload
                        .as_slice(),
                )
                .unwrap();
                assert_eq!(receipt.observed_lease_classification, lease_class);
            }
        }
    }
}

#[tokio::test]
async fn administrative_completion_override_records_dependency_observations() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let blocker = tracker.create("blocker", "blocker").await.unwrap();
    let blocked = tracker.create("blocked", "blocked").await.unwrap();
    tracker
        .relate(
            blocker.id,
            RelationKind::Blocks,
            blocked.id,
            0,
            0,
            "override-rel",
        )
        .await
        .unwrap();
    let report = v1::transition_issue_args::TerminalReport::Completion(v1::CompletionReport {
        result: Some(v1::completion_report::Result::Verified(
            v1::VerifiedCompletion {
                evidence: vec![v1::CompletionEvidence {
                    kind: v1::CompletionEvidenceKind::OtherEvidence as i32,
                    summary: "override evidence".into(),
                    uri: None,
                }],
            },
        )),
    });

    let completed = tracker
        .admin_terminate_with_overrides(
            blocked.id,
            IssueState::Done,
            1,
            "admin-override-observation",
            "administrator",
            "dependency override approved",
            &report,
            true,
            false,
            Some("release blocker for emergency completion"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(completed.state, IssueState::Done);

    let marker = tracker
        .get_operation_record("admin-override-observation")
        .await
        .unwrap();
    let receipt = manager_v1::IssueAdminActionResult::decode(
        marker
            .result_snapshot
            .as_ref()
            .expect("admin marker has a typed receipt")
            .manager_result_payload
            .as_slice(),
    )
    .unwrap();
    assert_eq!(
        receipt.dependency_override_target_issue_ids,
        vec![blocker.id.to_string()]
    );
    assert_eq!(receipt.dependency_override_observations.len(), 1);
    let observation = &receipt.dependency_override_observations[0];
    assert_eq!(
        observation.relation_id,
        relation_id(RelationKind::Blocks, blocker.id, blocked.id)
    );
    assert_eq!(observation.dependency_issue_id, blocker.id.to_string());
    assert_eq!(observation.observed_revision, 0);
    assert_eq!(observation.observed_state, v1::IssueState::Open as i32);
}

#[tokio::test]
async fn administrative_termination_rejects_already_terminal_issue_as_state_transition() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("admin terminal", "admin terminal")
        .await
        .unwrap();
    let report = v1::transition_issue_args::TerminalReport::Cancellation(v1::CancellationReport {
        classification: v1::CancellationClass::Other as i32,
    });
    let canceled = tracker
        .admin_terminate(
            issue.id,
            IssueState::Canceled,
            issue.revision,
            "admin-terminal-first",
            "administrator",
            "first cancellation",
            &report,
            false,
            false,
        )
        .await
        .unwrap();
    let error = tracker
        .admin_terminate(
            issue.id,
            IssueState::Canceled,
            canceled.revision,
            "admin-terminal-second",
            "administrator",
            "second cancellation",
            &report,
            false,
            false,
        )
        .await
        .expect_err("a terminal issue cannot be terminated again");
    assert!(matches!(error, TrackerError::InvalidStateTransition(_)));
}

#[tokio::test]
async fn administrative_termination_replays_without_a_second_revision() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("admin replay", "admin replay")
        .await
        .unwrap();
    let report = v1::transition_issue_args::TerminalReport::Cancellation(v1::CancellationReport {
        classification: v1::CancellationClass::Other as i32,
    });
    let first = tracker
        .admin_terminate(
            issue.id,
            IssueState::Canceled,
            issue.revision,
            "admin-replay",
            "administrator",
            "idempotent cancellation",
            &report,
            false,
            false,
        )
        .await
        .unwrap();
    let replay = tracker
        .admin_terminate(
            issue.id,
            IssueState::Canceled,
            issue.revision,
            "admin-replay",
            "administrator",
            "idempotent cancellation",
            &report,
            false,
            false,
        )
        .await
        .unwrap();
    assert_eq!(replay.revision, first.revision);
    assert_eq!(replay.state, IssueState::Canceled);
}

#[tokio::test]
async fn administrative_termination_rejects_a_changed_replay_payload() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("admin conflict", "admin conflict")
        .await
        .unwrap();
    let report = v1::transition_issue_args::TerminalReport::Cancellation(v1::CancellationReport {
        classification: v1::CancellationClass::Other as i32,
    });
    tracker
        .admin_terminate(
            issue.id,
            IssueState::Canceled,
            issue.revision,
            "admin-conflict",
            "administrator",
            "first reason",
            &report,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(
        tracker
            .admin_terminate(
                issue.id,
                IssueState::Canceled,
                issue.revision,
                "admin-conflict",
                "administrator",
                "changed reason",
                &report,
                false,
                false,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cannot_create_or_transition_to_unowned_in_progress() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let draft = v1::IssueDraft {
        title: "invalid initial state".into(),
        kind: v1::IssueKind::Task as i32,
        priority: v1::IssuePriority::P2 as i32,
        creation_basis: v1::CreationBasis::Explicit as i32,
        confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
        initial_state: v1::IssueState::InProgress as i32,
        ..Default::default()
    };
    assert!(
        tracker
            .create_with_draft(
                Uuid::new_v4(),
                "create-in-progress",
                &draft,
                "human",
                "create"
            )
            .await
            .is_err()
    );

    let issue = tracker
        .create("transition-to-in-progress", "transition")
        .await
        .unwrap();
    assert!(
        tracker
            .transition(
                issue.id,
                IssueState::InProgress,
                issue.revision,
                "transition-in-progress",
                None,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn heartbeat_extends_lease_and_comments_are_append_only() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("heartbeat", "heartbeat").await.unwrap();
    let claimed = tracker
        .claim(issue.id, "owner", issue.revision, 1, "claim")
        .await
        .unwrap();
    let renewed = tracker
        .heartbeat(
            issue.id,
            "owner",
            claimed.lease_handle.as_deref().unwrap(),
            claimed.revision,
            30,
            "heartbeat",
        )
        .await
        .unwrap();
    assert!(renewed.lease_expires_at.unwrap() >= now() + 29);
    let commented = tracker
        .comment(issue.id, "first comment", renewed.revision, "comment-1")
        .await
        .unwrap();
    assert_eq!(commented.revision, renewed.revision + 1);
}

#[tokio::test]
async fn expired_lease_cannot_heartbeat_or_release() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("expired", "expired").await.unwrap();
    let claimed = tracker
        .claim(issue.id, "owner", 0, 0, "claim-expired")
        .await
        .unwrap();
    let handle = claimed.lease_handle.as_deref().unwrap();
    assert!(matches!(
        tracker
            .heartbeat(
                issue.id,
                "owner",
                handle,
                claimed.revision,
                30,
                "heartbeat-expired"
            )
            .await,
        Err(TrackerError::InvalidLease { .. })
    ));
    assert!(matches!(
        tracker
            .release(
                issue.id,
                "owner",
                handle,
                claimed.revision,
                "release-expired"
            )
            .await,
        Err(TrackerError::LeaseExpired { .. })
    ));
    // Listing is read-only; the expired claim is not ready until explicit reconciliation.
    assert!(tracker.ready(now() + 1).await.unwrap().is_empty());
    let reconciled = tracker
        .reconcile_expired_claim_with_context(
            issue.id,
            claimed.revision,
            "reconcile-expired",
            "maintenance",
            "expired lease recovery",
        )
        .await
        .unwrap();
    assert_eq!(reconciled.state, IssueState::Open);
    assert_eq!(tracker.ready(now() + 1).await.unwrap().len(), 1);
    let history = tracker.list_history(issue.id).await.unwrap();
    assert!(history.iter().any(|entry| {
        entry
            .actor
            .as_ref()
            .and_then(|actor| actor.actor.as_ref())
            .map(|actor| actor.actor_id.as_str())
            == Some("maintenance")
            && entry.reason == "expired lease recovery"
    }));
}

#[tokio::test]
async fn transition_replay_is_idempotent_and_payload_conflict_is_rejected() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("replay", "replay").await.unwrap();
    let first = tracker
        .transition(issue.id, IssueState::Deferred, 0, "same-operation", None)
        .await
        .unwrap();
    let replay = tracker
        .transition(issue.id, IssueState::Deferred, 0, "same-operation", None)
        .await
        .unwrap();
    assert_eq!(first.revision, replay.revision);
    assert!(matches!(
        tracker
            .transition(issue.id, IssueState::Open, 0, "same-operation", None)
            .await,
        Err(TrackerError::IdempotencyConflict)
    ));
}

#[tokio::test]
async fn every_write_replay_is_idempotent_and_release_clears_completion_guard() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("writes", "writes").await.unwrap();

    let claimed = tracker
        .claim_with_context(
            issue.id,
            "owner",
            Some("session-1"),
            issue.revision,
            30,
            "claim-op",
            "claim-correlation",
        )
        .await
        .unwrap();
    assert!(matches!(
        tracker
            .claim_with_context(
                issue.id,
                "owner",
                Some("session-1"),
                issue.revision,
                30,
                "claim-op",
                "claim-correlation",
            )
            .await,
        Err(TrackerError::ResultUnconfirmed)
    ));
    let owner = claimed.lease_owner.as_ref().unwrap();
    assert_eq!(owner["actor_id"], "owner");
    assert_eq!(owner["agent_session_id"], "session-1");
    let task = tracker
        .inner
        .lock()
        .await
        .get_task(issue.id)
        .await
        .unwrap()
        .unwrap();
    let lease_handle = claimed.lease_handle.as_deref().unwrap();
    assert!(
        task.get_user_defined_attributes()
            .all(|(_, value)| !value.contains(lease_handle))
    );
    let claim_operation = tracker.get_operation_record("claim-op").await.unwrap();
    assert_eq!(claim_operation.method_key, "claim_issue");
    assert_eq!(
        claim_operation.result_replayability,
        v1::OperationResultReplayability::NotReplayable as i32
    );
    let snapshot = claim_operation
        .result_snapshot
        .as_ref()
        .expect("claim keeps a redacted audit snapshot");
    let Some(v1::operation_result_snapshot::Value::IssueMutation(mutation)) =
        snapshot.value.as_ref()
    else {
        panic!("claim marker snapshot must describe the Issue mutation");
    };
    let claim_snapshot = mutation
        .current
        .as_ref()
        .and_then(|details| details.issue.as_ref())
        .expect("claim snapshot has the Issue");
    assert!(claim_snapshot.claim.is_some());
    assert!(
        claim_snapshot
            .claim
            .as_ref()
            .is_some_and(|claim| claim.owner.is_none())
    );

    let renewed = tracker
        .heartbeat(
            issue.id,
            "owner",
            lease_handle,
            claimed.revision,
            30,
            "heartbeat-op",
        )
        .await
        .unwrap();
    let heartbeat_replay = tracker
        .heartbeat(
            issue.id,
            "owner",
            lease_handle,
            claimed.revision,
            30,
            "heartbeat-op",
        )
        .await
        .unwrap();
    assert_eq!(heartbeat_replay.revision, renewed.revision);

    let commented = tracker
        .comment(issue.id, "once", renewed.revision, "comment-op")
        .await
        .unwrap();
    let comment_replay = tracker
        .comment(issue.id, "once", renewed.revision, "comment-op")
        .await
        .unwrap();
    assert_eq!(comment_replay.revision, commented.revision);

    let released = tracker
        .release(
            issue.id,
            "owner",
            claimed.lease_handle.as_deref().unwrap(),
            commented.revision,
            "release-op",
        )
        .await
        .unwrap();
    let release_replay = tracker
        .release(
            issue.id,
            "owner",
            claimed.lease_handle.as_deref().unwrap(),
            commented.revision,
            "release-op",
        )
        .await
        .unwrap();
    assert_eq!(release_replay.revision, released.revision);
    let completed = tracker
        .transition(
            released.id,
            IssueState::Done,
            released.revision,
            "done-after-release",
            None,
        )
        .await
        .unwrap();
    assert_eq!(completed.state, IssueState::Done);
}

#[tokio::test]
async fn operation_marker_snapshot_preserves_the_original_issue_after_later_updates() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("before", "snapshot").await.unwrap();
    let first = tracker
        .update_title(issue.id, "at-operation", issue.revision, "snapshot-update")
        .await
        .unwrap();
    tracker
        .update_title(issue.id, "later", first.revision, "later-update")
        .await
        .unwrap();

    let record = tracker
        .get_operation_record("snapshot-update")
        .await
        .unwrap();
    let Some(v1::OperationResultSnapshot {
        value: Some(v1::operation_result_snapshot::Value::IssueMutation(snapshot)),
        ..
    }) = record.result_snapshot
    else {
        panic!("replayable issue operation must retain an issue snapshot");
    };
    assert_eq!(
        snapshot
            .current
            .and_then(|details| details.issue)
            .map(|issue| issue.title),
        Some("at-operation".into())
    );
}

#[tokio::test]
async fn non_block_relations_support_reverse_lookup_and_removal() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let a = tracker.create("reverse-a", "a").await.unwrap();
    let b = tracker.create("reverse-b", "b").await.unwrap();
    tracker
        .relate(
            a.id,
            RelationKind::Related,
            b.id,
            a.revision,
            b.revision,
            "related-add",
        )
        .await
        .unwrap();
    assert!(
        tracker
            .get(b.id)
            .await
            .unwrap()
            .relations
            .iter()
            .any(|relation| {
                relation.kind == RelationKind::Related
                    && relation.source == a.id
                    && relation.target == b.id
            })
    );
    tracker
        .unrelate(a.id, RelationKind::Related, b.id, 1, 1, "related-remove")
        .await
        .unwrap();
    assert!(
        !tracker
            .get(b.id)
            .await
            .unwrap()
            .relations
            .iter()
            .any(|relation| {
                relation.kind == RelationKind::Related
                    && relation.source == a.id
                    && relation.target == b.id
            })
    );
}

#[tokio::test]
async fn secret_like_input_is_rejected_before_commit() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    assert!(matches!(
        tracker.create("secret", "sk-test-secret").await,
        Err(TrackerError::SecretRejected)
    ));
    assert!(tracker.list().await.unwrap().is_empty());
}

#[test]
fn common_credential_formats_are_rejected_without_echoing_values() {
    // Assembled at runtime so repository secret scanners do not flag these
    // intentionally fake credentials.
    let samples = [
        ["gh", "p_012345678901234567890123456789012345"].concat(),
        ["xo", "xb-1234567890-abcdef"].concat(),
        ["AK", "IAIOSFODNN7EXAMPLE"].concat(),
        ["-----BEGIN RSA ", "PRIVATE KEY-----"].concat(),
        "password=super-secret".to_owned(),
    ];
    for value in &samples {
        let error = reject_secret([value.as_str()]).unwrap_err();
        assert!(matches!(error, TrackerError::SecretRejected));
        assert!(!error.to_string().contains(value.as_str()));
    }
}

fn native_actor_write_context(
    operation_id: &str,
    actor_id: &str,
    kind: v1::ActorKind,
) -> v1::WriteContext {
    v1::WriteContext {
        request: Some(v1::RequestContext {
            caller: Some(v1::ActorContext {
                actor: Some(v1::ActorRef {
                    actor_id: actor_id.into(),
                    kind: kind as i32,
                    ..Default::default()
                }),
                agent_session_id: Some("session-actor-history".into()),
            }),
            correlation_id: format!("correlation-{operation_id}"),
        }),
        downstream_operation_id: operation_id.into(),
        reason: "actor history contract".into(),
    }
}

#[test]
fn unknown_wire_actor_kind_is_rejected_without_echoing_the_actor_id() {
    let mut context = native_actor_write_context(
        "unknown-wire-actor-kind",
        "actor-id-must-not-leak",
        v1::ActorKind::System,
    );
    context
        .request
        .as_mut()
        .unwrap()
        .caller
        .as_mut()
        .unwrap()
        .actor
        .as_mut()
        .unwrap()
        .kind = 99;

    let error = super::super::conversion::call_context(Some(&context))
        .err()
        .expect("unknown actor kinds must fail closed");
    assert!(matches!(error, TrackerError::InvalidState(_)));
    assert!(!error.to_string().contains("actor-id-must-not-leak"));
}

#[tokio::test]
async fn native_actor_kinds_round_trip_through_issue_history_and_reopen() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let actors = [
        ("system-audit", v1::ActorKind::System),
        ("human-audit", v1::ActorKind::Human),
        ("external-audit", v1::ActorKind::ExternalAgent),
        ("internal-audit", v1::ActorKind::InternalAgent),
    ];
    let mut issues = Vec::new();

    for (index, (actor_id, actor_kind)) in actors.into_iter().enumerate() {
        let issue = tracker
            .create(&format!("actor-history-{index}"), "actor history")
            .await
            .unwrap();
        let operation_id = format!("actor-history-transition-{index}");
        let args = v1::TransitionIssueArgs {
            context: Some(native_actor_write_context(
                &operation_id,
                actor_id,
                actor_kind,
            )),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            target_state: v1::IssueState::Blocked as i32,
            ..Default::default()
        };
        let response = super::super::handler::writes::dispatch(
            "transition_issue",
            &args.encode_to_vec(),
            &tracker,
        )
        .await
        .unwrap();
        let response = v1::TransitionIssueResult::decode(response.as_slice()).unwrap();
        assert!(matches!(
            response.outcome,
            Some(v1::transition_issue_result::Outcome::Success(_))
        ));

        if actor_kind == v1::ActorKind::System {
            let mut replay = args.clone();
            let replay_actor = replay
                .context
                .as_mut()
                .unwrap()
                .request
                .as_mut()
                .unwrap()
                .caller
                .as_mut()
                .unwrap()
                .actor
                .as_mut()
                .unwrap();
            replay_actor.actor_id = "later-human-replay".into();
            replay_actor.kind = v1::ActorKind::Human as i32;
            let replay_response = super::super::handler::writes::dispatch(
                "transition_issue",
                &replay.encode_to_vec(),
                &tracker,
            )
            .await
            .unwrap();
            assert!(matches!(
                v1::TransitionIssueResult::decode(replay_response.as_slice())
                    .unwrap()
                    .outcome,
                Some(v1::transition_issue_result::Outcome::Success(_))
            ));
        }

        let history_key = format!("lb.history.{operation_id}");
        let stored = {
            let mut replica = tracker.inner.lock().await;
            let task = replica.get_task(issue.id).await.unwrap().unwrap();
            let record = task.get_user_defined_attribute(&history_key).unwrap();
            serde_json::from_str::<serde_json::Value>(record).unwrap()
        };
        assert_eq!(stored["actor"], actor_id);
        assert_eq!(stored["actor_kind"], actor_kind as i32);

        let entry = tracker
            .list_history(issue.id)
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.downstream_operation_id == operation_id)
            .unwrap();
        let actor = entry.actor.unwrap();
        assert_eq!(actor.actor.as_ref().unwrap().actor_id, actor_id);
        assert_eq!(actor.actor.unwrap().kind, actor_kind as i32);
        assert_eq!(
            actor.agent_session_id.as_deref(),
            Some("session-actor-history")
        );
        assert_eq!(entry.correlation_id, format!("correlation-{operation_id}"));
        issues.push((issue.id, operation_id, actor_id, actor_kind));
    }

    drop(tracker);
    let reopened = Tracker::open(root.path()).await.unwrap();
    for (issue_id, operation_id, actor_id, actor_kind) in &issues {
        let entry = reopened
            .list_history(*issue_id)
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.downstream_operation_id == *operation_id)
            .unwrap();
        let actor = entry.actor.unwrap().actor.unwrap();
        assert_eq!(actor.actor_id, *actor_id);
        assert_eq!(actor.kind, *actor_kind as i32);
    }

    let backup = TempDir::new().unwrap();
    reopened.backup(backup.path()).await.unwrap();
    drop(reopened);
    let restored_parent = TempDir::new().unwrap();
    let restored_root = restored_parent.path().join("restored-actor-history");
    Tracker::restore(backup.path(), &restored_root)
        .await
        .unwrap();
    let restored = Tracker::open(&restored_root).await.unwrap();
    for (issue_id, operation_id, actor_id, actor_kind) in &issues {
        let entry = restored
            .list_history(*issue_id)
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.downstream_operation_id == *operation_id)
            .unwrap();
        let actor = entry.actor.unwrap().actor.unwrap();
        assert_eq!(actor.actor_id, *actor_id);
        assert_eq!(actor.kind, *actor_kind as i32);
    }
}

#[tokio::test]
async fn legacy_actor_history_defaults_to_internal_agent_and_unknown_kind_fails_closed() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker
        .create("legacy-actor", "legacy actor history")
        .await
        .unwrap();
    let history_key = "lb.history.create:legacy-actor";

    let mut record = {
        let mut replica = tracker.inner.lock().await;
        let task = replica.get_task(issue.id).await.unwrap().unwrap();
        serde_json::from_str::<serde_json::Value>(
            task.get_user_defined_attribute(history_key).unwrap(),
        )
        .unwrap()
    };
    record.as_object_mut().unwrap().remove("actor_kind");
    tracker
        .set_user_defined_attribute_for_test(issue.id, history_key, &record.to_string())
        .await
        .unwrap();
    let legacy = tracker.list_history(issue.id).await.unwrap();
    let legacy_actor = legacy[0].actor.as_ref().unwrap().actor.as_ref().unwrap();
    assert_eq!(legacy_actor.actor_id, "unknown");
    assert_eq!(legacy_actor.kind, v1::ActorKind::InternalAgent as i32);

    record["actor"] = serde_json::json!({
        "actor_id": "legacy-structured-actor",
        "agent_session_id": "legacy-structured-session",
    });
    tracker
        .set_user_defined_attribute_for_test(issue.id, history_key, &record.to_string())
        .await
        .unwrap();
    let structured_legacy = tracker.list_history(issue.id).await.unwrap();
    let structured_actor = structured_legacy[0].actor.as_ref().unwrap();
    assert_eq!(
        structured_actor.actor.as_ref().unwrap().actor_id,
        "legacy-structured-actor"
    );
    assert_eq!(
        structured_actor.actor.as_ref().unwrap().kind,
        v1::ActorKind::InternalAgent as i32
    );
    assert_eq!(
        structured_actor.agent_session_id.as_deref(),
        Some("legacy-structured-session")
    );

    record["actor"] = serde_json::json!({
        "actor_id": "actor-id-must-not-leak",
        "actor_kind": 99,
    });
    tracker
        .set_user_defined_attribute_for_test(issue.id, history_key, &record.to_string())
        .await
        .unwrap();
    let error = tracker.list_history(issue.id).await.unwrap_err();
    assert!(matches!(error, TrackerError::InvalidState(_)));
    assert!(!error.to_string().contains("actor-id-must-not-leak"));
    assert!(!error.to_string().contains("99"));

    record["actor"] = serde_json::Value::String("actor-id-must-not-leak".into());
    record["actor_kind"] = serde_json::json!(99);
    tracker
        .set_user_defined_attribute_for_test(issue.id, history_key, &record.to_string())
        .await
        .unwrap();
    let error = tracker.list_history(issue.id).await.unwrap_err();
    assert!(matches!(error, TrackerError::InvalidState(_)));
    assert!(!error.to_string().contains("actor-id-must-not-leak"));

    record["actor_kind"] = serde_json::Value::String("SYSTEM".into());
    tracker
        .set_user_defined_attribute_for_test(issue.id, history_key, &record.to_string())
        .await
        .unwrap();
    assert!(matches!(
        tracker.list_history(issue.id).await,
        Err(TrackerError::InvalidState(_))
    ));
}

#[tokio::test]
async fn context_free_writes_record_the_unknown_actor_as_unspecified() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let issue = tracker.create("before", "unknown actor").await.unwrap();
    let blocker = tracker.create("blocker", "unknown actor").await.unwrap();
    let issue = tracker
        .update_title(issue.id, "after", issue.revision, "context-free-update")
        .await
        .unwrap();
    let issue = tracker
        .transition(
            issue.id,
            IssueState::Blocked,
            issue.revision,
            "context-free-transition",
            None,
        )
        .await
        .unwrap();
    tracker
        .relate(
            blocker.id,
            RelationKind::Blocks,
            issue.id,
            blocker.revision,
            issue.revision,
            "context-free-relate",
        )
        .await
        .unwrap();
    let blocker = tracker.get(blocker.id).await.unwrap();
    let issue = tracker.get(issue.id).await.unwrap();
    tracker
        .unrelate(
            blocker.id,
            RelationKind::Blocks,
            issue.id,
            blocker.revision,
            issue.revision,
            "context-free-unrelate",
        )
        .await
        .unwrap();

    let mut checked = 0;
    for id in [issue.id, blocker.id] {
        for entry in tracker.list_history(id).await.unwrap() {
            let actor = entry
                .actor
                .as_ref()
                .and_then(|actor| actor.actor.as_ref())
                .expect("history actor");
            assert_eq!(
                actor.actor_id, "unknown",
                "{}",
                entry.downstream_operation_id
            );
            // An absent caller must never be recorded as a known internal agent.
            assert_eq!(
                actor.kind,
                v1::ActorKind::Unspecified as i32,
                "{}",
                entry.downstream_operation_id
            );
            checked += 1;
        }
    }
    // create x2, update, transition, relate, unrelate.
    assert!(checked >= 6, "only {checked} history entries were checked");

    let issue = tracker.get(issue.id).await.unwrap();
    tracker
        .comment(
            issue.id,
            "context-free",
            issue.revision,
            "context-free-comment",
        )
        .await
        .unwrap();
    let comments = tracker.list_comments(issue.id).await.unwrap();
    let author = comments
        .last()
        .and_then(|comment| comment.actor.as_ref())
        .and_then(|author| author.actor.as_ref())
        .expect("comment author");
    assert_eq!(author.actor_id, "unknown");
    assert_eq!(author.kind, v1::ActorKind::Unspecified as i32);
}

#[tokio::test]
async fn awaiting_review_rejects_non_review_exits_through_generic_writes() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let (id, revision, _) =
        super::create_awaiting_review_fixture(&tracker, "awaiting-exit", "awaiting-exit-run").await;

    let transition_error = tracker
        .transition(
            id,
            IssueState::Blocked,
            revision,
            "awaiting-exit-blocked",
            None,
        )
        .await
        .expect_err("awaiting review cannot be parked as blocked");
    assert!(matches!(
        transition_error,
        TrackerError::InvalidStateTransition(_)
    ));
    let update_error = tracker
        .update_issue_with_context(
            id,
            &v1::IssuePatch {
                title: Some("deferred review".into()),
                ..Default::default()
            },
            &[v1::IssueField::Title as i32],
            revision,
            "awaiting-exit-deferred",
            "reviewer",
            None,
            "correlation",
            Some(v1::IssueState::Deferred as i32),
            None,
        )
        .await
        .expect_err("awaiting review cannot be deferred");
    assert!(
        matches!(update_error, TrackerError::InvalidStateTransition(_)),
        "{update_error:?}"
    );
    let unchanged = tracker.get(id).await.unwrap();
    assert_eq!(unchanged.state, IssueState::AwaitingReview);
    assert_eq!(unchanged.revision, revision);

    // Sending the work back for rework remains a permitted review outcome.
    let reopened = tracker
        .transition(id, IssueState::Open, revision, "awaiting-exit-rework", None)
        .await
        .unwrap();
    assert_eq!(reopened.state, IssueState::Open);
}

async fn try_awaiting_review_completion(
    tracker: &Tracker,
    key: &str,
    completion: &v1::AwaitingReviewCompletion,
) -> (Uuid, Result<(), TrackerError>) {
    // Case labels are diagnostic only; neutral operation ids keep the
    // secret scanner from rejecting the fixture itself.
    let operation = format!(
        "bounds-{}",
        Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
    );
    let issue = tracker.create(&operation, "bounds").await.unwrap();
    let key = operation.as_str();
    let claimed = tracker
        .claim(
            issue.id,
            "bounds-worker",
            issue.revision,
            60,
            &format!("{key}-claim"),
        )
        .await
        .unwrap();
    let result = tracker
        .complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            &format!("{key}-complete"),
            claimed.lease_handle.as_deref(),
            Some(claimed.revision),
            "bounds-worker",
            Some(completion.run_id.as_str()),
            &format!("{key}-correlation"),
            completion,
        )
        .await;
    (issue.id, result)
}

fn with_verified_evidence(
    mut completion: v1::AwaitingReviewCompletion,
    evidence: Vec<v1::CompletionEvidence>,
) -> v1::AwaitingReviewCompletion {
    completion.report = Some(v1::CompletionReport {
        result: Some(v1::completion_report::Result::Verified(
            v1::VerifiedCompletion { evidence },
        )),
    });
    completion
}

fn with_unverified(
    mut completion: v1::AwaitingReviewCompletion,
    reason: String,
    residual_risk: String,
) -> v1::AwaitingReviewCompletion {
    completion.report = Some(v1::CompletionReport {
        result: Some(v1::completion_report::Result::Unverified(
            v1::UnverifiedCompletion {
                reason,
                residual_risk,
            },
        )),
    });
    completion
}

fn evidence(summary: String, uri: Option<String>) -> v1::CompletionEvidence {
    v1::CompletionEvidence {
        kind: v1::CompletionEvidenceKind::TestResult as i32,
        summary,
        uri,
    }
}

#[tokio::test]
async fn awaiting_review_completion_accepts_values_at_each_bound() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let base = |run_id: &str| super::awaiting_review_completion(run_id);
    let mut at_run_id = base(&"r".repeat(256));
    at_run_id.full_commit_id = "A".repeat(64);
    let mut at_references = base("run-references");
    at_references.evidence_references = (0..1_000)
        .map(|index| format!("artifact://{index}/{}", "x".repeat(2_000)))
        .collect();
    at_references.evidence_references[0] = "y".repeat(2_048);
    let cases = [
        ("bound-run-id", at_run_id),
        ("bound-references", at_references),
        (
            "bound-evidence-count",
            with_verified_evidence(
                base("run-evidence-count"),
                (0..1_000)
                    .map(|index| evidence(format!("check {index}"), None))
                    .collect(),
            ),
        ),
        (
            "bound-evidence-size",
            with_verified_evidence(
                base("run-evidence-size"),
                vec![evidence("s".repeat(16_384), Some("u".repeat(16_384)))],
            ),
        ),
        (
            "bound-unverified-size",
            with_unverified(
                base("run-unverified-size"),
                "r".repeat(16_384),
                "k".repeat(16_384),
            ),
        ),
    ];
    for (key, completion) in cases {
        let (id, result) = try_awaiting_review_completion(&tracker, key, &completion).await;
        assert!(result.is_ok(), "{key}: {result:?}");
        assert_eq!(
            tracker.get(id).await.unwrap().state,
            IssueState::AwaitingReview,
            "{key}"
        );
    }
}

#[tokio::test]
async fn awaiting_review_completion_rejects_values_beyond_each_bound_without_mutation() {
    let root = TempDir::new().unwrap();
    let tracker = Tracker::open(root.path()).await.unwrap();
    let base = |run_id: &str| super::awaiting_review_completion(run_id);
    let with_commit = |commit: String| {
        let mut completion = base("run-commit");
        completion.full_commit_id = commit;
        completion
    };
    let with_references = |references: Vec<String>| {
        let mut completion = base("run-references");
        completion.evidence_references = references;
        completion
    };
    let cases = [
        ("run-id-too-long", base(&"r".repeat(257))),
        ("run-id-blank", base("   ")),
        ("run-id-control", base("run\nid")),
        ("commit-39", with_commit("a".repeat(39))),
        ("commit-41", with_commit("a".repeat(41))),
        ("commit-63", with_commit("a".repeat(63))),
        ("commit-65", with_commit("a".repeat(65))),
        ("commit-not-hex", with_commit("g".repeat(40))),
        ("references-empty", with_references(vec![])),
        (
            "references-too-many",
            with_references(
                (0..1_001)
                    .map(|index| format!("artifact://{index}"))
                    .collect(),
            ),
        ),
        (
            "reference-too-long",
            with_references(vec!["x".repeat(2_049)]),
        ),
        ("reference-blank", with_references(vec!["  ".into()])),
        (
            "evidence-too-many",
            with_verified_evidence(
                base("run-evidence-count"),
                (0..1_001)
                    .map(|index| evidence(format!("check {index}"), None))
                    .collect(),
            ),
        ),
        (
            "evidence-empty",
            with_verified_evidence(base("run-evidence-empty"), vec![]),
        ),
        (
            "evidence-summary-too-long",
            with_verified_evidence(
                base("run-summary"),
                vec![evidence("s".repeat(16_385), None)],
            ),
        ),
        (
            "evidence-uri-too-long",
            with_verified_evidence(
                base("run-uri"),
                vec![evidence("ok".into(), Some("u".repeat(16_385)))],
            ),
        ),
        (
            "unverified-reason-too-long",
            with_unverified(base("run-reason"), "r".repeat(16_385), "risk".into()),
        ),
        (
            "unverified-risk-too-long",
            with_unverified(base("run-risk"), "reason".into(), "k".repeat(16_385)),
        ),
        (
            "unverified-risk-blank",
            with_unverified(base("run-blank-risk"), "reason".into(), " ".into()),
        ),
    ];
    for (key, completion) in cases {
        let (id, result) = try_awaiting_review_completion(&tracker, key, &completion).await;
        assert!(
            matches!(result, Err(TrackerError::InvalidState(_))),
            "{key}: {result:?}"
        );
        let unchanged = tracker.get(id).await.unwrap();
        assert_eq!(unchanged.state, IssueState::InProgress, "{key}");
        assert!(unchanged.lease_owner.is_some(), "{key}");
    }
}
