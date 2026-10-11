use super::*;
use crate::{
    manager::{
        backend_adapter::{EntityRevisionExpectation, MarkerExpectation},
        operations::{IdempotencyScope, NewOperation, OperationScope},
        outbox::{self, OutboxError},
    },
    proto_types::manager_v1 as pb,
    tracker::{
        Tracker,
        config::{RunnerConfig, contract_fingerprint},
        operation_log::marker_digest,
    },
    v1,
};
use tempfile::tempdir;
use uuid::Uuid;

fn operation() -> pb::DurableOperation {
    NewOperation {
        operation_id: "manager-op-1".into(),
        scope: Some(OperationScope::Issue("issue-1".into())),
        method: "manager_update_issue".into(),
        idempotency_scope: IdempotencyScope::new(
            "backend-contract",
            "root",
            "manager_update_issue",
            "issue:issue-1",
            "key",
        )
        .unwrap(),
        request_digest: "request-digest".into(),
        request_schema: "UpdateIssueArgs".into(),
        downstream_operation_id: Some("downstream-op-1".into()),
        generation: 1,
        manager_sequence: 1,
        causal_parent: None,
        affects_ready: true,
        sync_subject: Some("issue-1".into()),
        affected_issue_ids: vec!["issue-1".into()],
        actor_audit: None,
        contract_fingerprint: "backend-contract".into(),
        schema_fingerprint: "backend-schema".into(),
        correlation_id: "correlation-1".into(),
        now_millis: 100,
        replayable: true,
    }
    .build()
    .unwrap()
}

fn expected() -> MarkerExpectation {
    MarkerExpectation {
        operation_id: "downstream-op-1".into(),
        method_key: "manager_update_issue".into(),
        scope: v1::OperationScope::Issue as i32,
        payload_fingerprint: crate::manager::operations::digest_bytes(&[1, 2, 3]),
        contract_fingerprint: "backend-contract".into(),
        affected_issue_ids: vec!["issue-1".into()],
        entity_revisions: vec![EntityRevisionExpectation {
            issue_id: "issue-1".into(),
            backend_id: "backend-issue-1".into(),
            resulting_revision: 2,
        }],
        result_replayable: true,
    }
}

fn marker(expected: &MarkerExpectation) -> v1::OperationRecord {
    v1::OperationRecord {
        downstream_operation_id: expected.operation_id.clone(),
        affected_issue_ids: expected.affected_issue_ids.clone(),
        payload_fingerprint: expected.payload_fingerprint.clone(),
        contract_fingerprint: expected.contract_fingerprint.clone(),
        scope: expected.scope,
        method_key: expected.method_key.clone(),
        entity_revisions: vec![v1::OperationEntityRevision {
            issue_id: "issue-1".into(),
            backend_id: "backend-issue-1".into(),
            resulting_revision: 2,
        }],
        commit_durability: v1::OperationCommitDurability::Durable as i32,
        result_replayability: v1::OperationResultReplayability::Replayable as i32,
        result_snapshot: Some(v1::OperationResultSnapshot::default()),
        fingerprint_input: Some(v1::OperationFingerprintInput {
            downstream_operation_id: expected.operation_id.clone(),
            method_key: expected.method_key.clone(),
            scope: expected.scope,
            payload_fingerprint: expected.payload_fingerprint.clone(),
        }),
        ..Default::default()
    }
}

#[test]
fn manager_error_preserves_structured_lease_precondition() {
    let error = v1::IssueTrackerError {
        code: v1::ErrorCode::InvalidLease as i32,
        message: "lease is invalid or expired".into(),
        retryable: false,
        ..Default::default()
    };
    let manager_error = manager_error_from_tracker(error);
    assert!(matches!(
        manager_error.detail,
        Some(pb::manager_error::Detail::OwnershipIntegrityDiagnosis(diagnosis))
            if diagnosis.diagnosis_class == "ACTIVE_LEASE"
    ));
}

#[test]
fn manager_error_preserves_inconsistent_lease_classification() {
    let error = v1::IssueTrackerError {
        code: v1::ErrorCode::InvalidLease as i32,
        message: "lease attributes are inconsistent".into(),
        retryable: false,
        detail: Some(v1::issue_tracker_error::Detail::LeasePrecondition(
            v1::LeasePrecondition {
                classification: "INCONSISTENT".into(),
                invalid_attributes: vec!["lb.lease_expires_at".into()],
                observed_revision: 7,
                ..Default::default()
            },
        )),
    };
    let manager_error = manager_error_from_tracker(error);
    assert!(matches!(
        manager_error.detail,
        Some(pb::manager_error::Detail::OwnershipIntegrityDiagnosis(diagnosis))
            if diagnosis.diagnosis_class == "LEASE_INCONSISTENT"
                && diagnosis.lease_classification.as_deref() == Some("INCONSISTENT")
                && diagnosis.observed_revision == Some(7)
    ));
}

#[test]
fn manager_error_preserves_revision_conflict_detail() {
    let error = v1::IssueTrackerError {
        code: v1::ErrorCode::RevisionConflict as i32,
        message: "revision conflict".into(),
        retryable: false,
        detail: Some(v1::issue_tracker_error::Detail::RevisionConflict(
            v1::RevisionConflict {
                expected_revision: 4,
                current_revision: 7,
                ..Default::default()
            },
        )),
    };
    let manager_error = manager_error_from_tracker(error);
    assert_eq!(
        manager_error.code,
        pb::ManagerErrorCode::RevisionConflict as i32
    );
    assert!(matches!(
        manager_error.detail,
        Some(pb::manager_error::Detail::RevisionConflict(conflict))
            if conflict.expected_revision == 4 && conflict.current_revision == 7
    ));
}

#[test]
fn manager_error_preserves_lease_expiry_and_blocker_ids() {
    let lease_error = v1::IssueTrackerError {
        code: v1::ErrorCode::LeaseExpired as i32,
        message: "lease expired".into(),
        detail: Some(v1::issue_tracker_error::Detail::LeasePrecondition(
            v1::LeasePrecondition {
                classification: "EXPIRED".into(),
                lease_expires_at: Some(v1::UtcInstant {
                    unix_epoch_millis: 123_000,
                }),
                observed_revision: 9,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let manager_error = manager_error_from_tracker(lease_error);
    assert!(matches!(
        manager_error.detail,
        Some(pb::manager_error::Detail::OwnershipIntegrityDiagnosis(diagnosis))
            if diagnosis.observed_revision == Some(9)
                && diagnosis.lease_expires_at.as_ref().map(|v| v.unix_epoch_millis)
                    == Some(123_000)
    ));

    let blocker_error = v1::IssueTrackerError {
        code: v1::ErrorCode::BlockedByOpenIssue as i32,
        message: "blocked".into(),
        detail: Some(v1::issue_tracker_error::Detail::BlockerConflict(
            v1::BlockerConflict {
                open_blocker_issue_ids: vec!["issue-b".into(), "issue-a".into()],
            },
        )),
        ..Default::default()
    };
    let manager_error = manager_error_from_tracker(blocker_error);
    assert!(matches!(
        manager_error.detail,
        Some(pb::manager_error::Detail::OwnershipIntegrityDiagnosis(diagnosis))
            if diagnosis.open_blocker_issue_ids == ["issue-a", "issue-b"]
    ));
}

#[test]
fn administrative_dispatch_can_close_or_retry_after_lease_expiry() {
    let mut operation = operation();
    operation.method = "manager_apply_issue_admin_action".into();
    let mut entry = outbox::new_entry(
        "admin-outbox",
        &operation,
        "taskchampion",
        "request",
        "digest",
        0,
    )
    .expect("admin outbox entry");
    let lease = outbox::lease(&mut entry, 0, 10, Some("lease".into())).expect("lease entry");
    outbox::mark_dispatched(&mut entry, &lease.lease_id, 1).expect("mark dispatched");
    acknowledge_after_dispatch(&mut entry, &lease.lease_id, 20, true)
        .expect("expired admin lease remains safely acknowledged");
    assert_eq!(entry.status, pb::OutboxStatus::Acknowledged as i32);

    let mut retry_entry = outbox::new_entry(
        "admin-outbox-retry",
        &operation,
        "taskchampion",
        "request",
        "digest",
        0,
    )
    .expect("admin retry entry");
    let retry_lease = outbox::lease(&mut retry_entry, 0, 10, Some("retry-lease".into()))
        .expect("retry lease entry");
    outbox::mark_dispatched(&mut retry_entry, &retry_lease.lease_id, 1)
        .expect("mark retry dispatched");
    retry_after_dispatch(
        &mut retry_entry,
        &retry_lease.lease_id,
        30,
        pb::ManagerErrorCode::BackendUnavailable,
        20,
        true,
    )
    .expect("expired admin lease remains safely retryable");
    assert_eq!(retry_entry.status, pb::OutboxStatus::Ready as i32);
    assert_eq!(
        retry_entry
            .next_attempt_at
            .as_ref()
            .map(|value| value.unix_epoch_millis),
        Some(30)
    );
}

fn entry(operation: &pb::DurableOperation) -> pb::OutboxEntry {
    outbox::new_entry(
        "outbox-1",
        operation,
        "issue.update",
        "snapshot-ref",
        crate::manager::operations::digest_bytes(&[1, 2, 3]),
        100,
    )
    .unwrap()
}

fn retry_budget(max_attempts: u32) -> pb::RetryBudget {
    pb::RetryBudget {
        max_attempts,
        base_backoff: Some(pb::TimeSpan { millis: 1_000 }),
        max_backoff: Some(pb::TimeSpan { millis: 1_000 }),
    }
}

#[derive(Debug)]
struct FakeSouthbound {
    result: Result<SouthboundResponse, SouthboundError>,
}

#[async_trait::async_trait]
impl TaskChampionV1Dispatcher for FakeSouthbound {
    async fn dispatch(
        &self,
        _request: &BackendDispatchRequest,
    ) -> Result<SouthboundResponse, SouthboundError> {
        self.result.clone()
    }
}

#[tokio::test]
async fn lease_dispatch_and_exact_marker_acknowledge_and_confirm() {
    let operation = operation();
    let expected = expected();
    let mut entry = entry(&operation);
    outbox::lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
    let southbound = FakeSouthbound {
        result: Ok(SouthboundResponse::committed(marker(&expected), vec![])),
    };
    let result = dispatch_leased(
        &southbound,
        DispatchLeaseInput {
            entry: &mut entry,
            operation: &operation,
            request: BackendDispatchRequest {
                method: "manager_update_issue".into(),
                payload: vec![1, 2, 3],
                downstream_operation_id: None,
            },
            expected,
            lease_id: "lease-1",
            retry_budget: &retry_budget(2),
            now_millis: 110,
        },
    )
    .await
    .expect("dispatch should confirm");

    assert_eq!(result.state, pb::DurableOperationState::Committed);
    assert_eq!(entry.status, pb::OutboxStatus::Acknowledged as i32);
    assert!(entry.lease_id.is_none());
    assert!(result.ephemeral_claim_handle.is_none());
}

#[tokio::test]
async fn delivery_unknown_acknowledges_outbox_but_marks_operation_unconfirmed() {
    let operation = operation();
    let expected = expected();
    let mut entry = entry(&operation);
    outbox::lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
    let southbound = FakeSouthbound {
        result: Err(SouthboundError::delivery_unknown("connection closed")),
    };

    let result = dispatch_leased(
        &southbound,
        DispatchLeaseInput {
            entry: &mut entry,
            operation: &operation,
            request: BackendDispatchRequest {
                method: "manager_update_issue".into(),
                payload: vec![1, 2, 3],
                downstream_operation_id: None,
            },
            expected,
            lease_id: "lease-1",
            retry_budget: &retry_budget(2),
            now_millis: 110,
        },
    )
    .await
    .expect("unknown delivery is represented as a safe result");

    assert_eq!(result.state, pb::DurableOperationState::Unconfirmed);
    assert_eq!(entry.status, pb::OutboxStatus::Acknowledged as i32);
}

#[tokio::test]
async fn marker_conflict_acknowledges_outbox_and_requires_manual_resolution() {
    let operation = operation();
    let expected = expected();
    let mut wrong = marker(&expected);
    wrong.payload_fingerprint = "wrong".into();
    let mut entry = entry(&operation);
    outbox::lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
    let southbound = FakeSouthbound {
        result: Ok(SouthboundResponse::committed(wrong, vec![])),
    };

    let result = dispatch_leased(
        &southbound,
        DispatchLeaseInput {
            entry: &mut entry,
            operation: &operation,
            request: BackendDispatchRequest {
                method: "manager_update_issue".into(),
                payload: vec![1, 2, 3],
                downstream_operation_id: None,
            },
            expected,
            lease_id: "lease-1",
            retry_budget: &retry_budget(2),
            now_millis: 110,
        },
    )
    .await
    .expect("marker conflict is represented as a safe result");

    assert_eq!(result.state, pb::DurableOperationState::ManualResolution);
    assert!(result.reason.contains("marker"));
    assert_eq!(entry.status, pb::OutboxStatus::Acknowledged as i32);
}

#[tokio::test]
async fn not_sent_dispatch_retries_and_does_not_mutate_operation() {
    let operation = operation();
    let expected = expected();
    let mut entry = entry(&operation);
    outbox::lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
    let southbound = FakeSouthbound {
        result: Err(SouthboundError::not_sent("backend unavailable")),
    };

    let result = dispatch_leased(
        &southbound,
        DispatchLeaseInput {
            entry: &mut entry,
            operation: &operation,
            request: BackendDispatchRequest {
                method: "manager_update_issue".into(),
                payload: vec![1, 2, 3],
                downstream_operation_id: None,
            },
            expected,
            lease_id: "lease-1",
            retry_budget: &retry_budget(2),
            now_millis: 110,
        },
    )
    .await
    .expect("not-sent failure should be retryable");

    assert_eq!(result.state, pb::DurableOperationState::Received);
    assert_eq!(entry.status, pb::OutboxStatus::Ready as i32);
    assert!(entry.lease_id.is_none());
}

#[tokio::test]
async fn typed_tracker_error_is_terminal_and_never_retried() {
    let operation = operation();
    let expected = expected();
    let mut entry = entry(&operation);
    outbox::lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
    let southbound = FakeSouthbound {
        result: Err(SouthboundError::typed_business(v1::IssueTrackerError {
            code: v1::ErrorCode::RevisionConflict as i32,
            message: "revision changed".into(),
            retryable: true,
            ..Default::default()
        })),
    };

    let result = dispatch_leased(
        &southbound,
        DispatchLeaseInput {
            entry: &mut entry,
            operation: &operation,
            request: BackendDispatchRequest {
                method: "manager_update_issue".into(),
                payload: vec![1, 2, 3],
                downstream_operation_id: None,
            },
            expected,
            lease_id: "lease-1",
            retry_budget: &retry_budget(2),
            now_millis: 110,
        },
    )
    .await
    .expect("typed tracker errors are terminal results");

    assert_eq!(result.state, pb::DurableOperationState::Failed);
    assert_eq!(entry.status, pb::OutboxStatus::DeadLetter as i32);
    assert_eq!(
        entry.error_code,
        pb::ManagerErrorCode::RevisionConflict as i32
    );
    let Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot)) =
        result.operation.terminal_snapshot
    else {
        panic!("terminal typed error snapshot is required");
    };
    let error = pb::ManagerError::decode(snapshot.payload.as_slice()).unwrap();
    assert_eq!(error.code, pb::ManagerErrorCode::RevisionConflict as i32);
    assert!(!error.retryable);
}

#[tokio::test]
async fn transport_not_sent_stops_after_the_configured_retry_budget() {
    let operation = operation();
    let expected = expected();
    let mut entry = entry(&operation);
    outbox::lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
    let southbound = FakeSouthbound {
        result: Err(SouthboundError::not_sent("connection refused")),
    };

    let result = dispatch_leased(
        &southbound,
        DispatchLeaseInput {
            entry: &mut entry,
            operation: &operation,
            request: BackendDispatchRequest {
                method: "manager_update_issue".into(),
                payload: vec![1, 2, 3],
                downstream_operation_id: None,
            },
            expected,
            lease_id: "lease-1",
            retry_budget: &retry_budget(1),
            now_millis: 110,
        },
    )
    .await
    .expect("exhausted transport retry is a terminal result");

    assert_eq!(result.state, pb::DurableOperationState::Failed);
    assert_eq!(entry.status, pb::OutboxStatus::DeadLetter as i32);
    assert_eq!(
        entry.error_code,
        pb::ManagerErrorCode::RetryExhausted as i32
    );
}

#[tokio::test]
async fn stale_or_wrong_outbox_lease_cannot_dispatch() {
    let operation = operation();
    let expected = expected();
    let mut entry = entry(&operation);
    outbox::lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
    let southbound = FakeSouthbound {
        result: Ok(SouthboundResponse::committed(marker(&expected), vec![])),
    };

    let error = dispatch_leased(
        &southbound,
        DispatchLeaseInput {
            entry: &mut entry,
            operation: &operation,
            request: BackendDispatchRequest {
                method: "manager_update_issue".into(),
                payload: vec![1, 2, 3],
                downstream_operation_id: None,
            },
            expected,
            lease_id: "stale-lease",
            retry_budget: &retry_budget(2),
            now_millis: 110,
        },
    )
    .await
    .expect_err("a stale lease must not reach TaskChampion");

    assert!(matches!(
        error,
        BackendDispatchError::Outbox(OutboxError::LeaseMismatch)
    ));
    assert_eq!(entry.status, pb::OutboxStatus::Leased as i32);
}

#[test]
fn claim_handle_is_returned_only_as_ephemeral_data() {
    let response = SouthboundResponse::committed(v1::OperationRecord::default(), vec![9, 8, 7]);
    assert!(response.ephemeral_claim_handle.is_none());
    let claim =
        SouthboundResponse::committed_claim(v1::OperationRecord::default(), vec![], vec![9, 8, 7]);
    assert_eq!(claim.ephemeral_claim_handle, Some(vec![9, 8, 7]));
}

#[test]
fn confirmed_taskchampion_create_exposes_revision_zero_in_mapping() {
    let mut operation = operation();
    operation.method = "manager_create_issue".into();
    let mut expected = expected();
    expected.method_key = "create_issue".into();
    expected.entity_revisions[0].resulting_revision = 0;

    let result = dispatch_result(
        operation,
        ReconcileResult {
            state: pb::DurableOperationState::Committed,
            reason: ReconcileReason::MarkerConfirmed,
        },
        Vec::new(),
        None,
        &expected,
    );

    assert_eq!(result.issue_mappings[0].revision, 0);
}

fn manager_update_payload(issue_id: &str) -> Vec<u8> {
    pb::UpdateIssueArgs {
        context: Some(pb::ManagerWriteContext {
            method: "manager_update_issue".into(),
            target: format!("issue:{issue_id}"),
            schema_fingerprint: "manager-schema".into(),
            idempotency_key: "manager-key".into(),
            actor_audit: Some(pb::ActorAudit {
                actor_id: "lookback:test".into(),
                actor_kind: v1::ActorKind::InternalAgent as i32,
                display_name: Some("test".into()),
                agent_session_id: Some("session-1".into()),
                reason: Some("integration test".into()),
            }),
            correlation_id: "correlation-1".into(),
            data_root_id: "manager-root".into(),
            contract_fingerprint: "manager-contract".into(),
        }),
        issue_id: issue_id.into(),
        expected_revision: Some(0),
        update_mask: vec![pb::ManagerIssueField::Title as i32],
        patch: Some(pb::ManagerIssuePatch {
            title: Some("updated by manager outbox".into()),
            ..Default::default()
        }),
        target_state: None,
        terminal_report: None,
    }
    .encode_to_vec()
}

fn manager_context(method: &str, target: &str) -> pb::ManagerWriteContext {
    pb::ManagerWriteContext {
        method: method.into(),
        target: target.into(),
        schema_fingerprint: "manager-schema".into(),
        idempotency_key: "manager-key".into(),
        actor_audit: Some(pb::ActorAudit {
            actor_id: "lookback:test".into(),
            actor_kind: v1::ActorKind::InternalAgent as i32,
            display_name: Some("test".into()),
            agent_session_id: Some("session-1".into()),
            reason: Some("integration test".into()),
        }),
        correlation_id: "correlation-1".into(),
        data_root_id: "manager-root".into(),
        contract_fingerprint: "manager-contract".into(),
    }
}

#[test]
fn durable_envelope_carries_only_restart_safe_issue_intents() {
    let cases = [
        (
            "manager_claim_issue",
            pb::ClaimIssueArgs {
                context: Some(manager_context("manager_claim_issue", "issue:claim")),
                issue_id: "claim".into(),
                expected_revision: Some(4),
                requested_lease: Some(pb::TimeSpan { millis: 5_000 }),
                gateway_eligibility_generation: None,
            }
            .encode_to_vec(),
        ),
        (
            "manager_transition_issue",
            pb::TransitionIssueArgs {
                context: Some(manager_context(
                    "manager_transition_issue",
                    "issue:transition",
                )),
                issue_id: "transition".into(),
                expected_revision: Some(2),
                target_state: v1::IssueState::InProgress as i32,
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        (
            "manager_reconcile_expired_claim",
            pb::ReconcileExpiredClaimArgs {
                context: Some(manager_context(
                    "manager_reconcile_expired_claim",
                    "issue:reconcile",
                )),
                issue_id: "reconcile".into(),
                expected_revision: Some(6),
                claim_revision: Some(6),
            }
            .encode_to_vec(),
        ),
        (
            "manager_reconcile_expired_claim",
            pb::ReconcileExpiredClaimArgs {
                context: Some(manager_context(
                    "manager_reconcile_expired_claim",
                    "issue:reconcile-omitted-claim",
                )),
                issue_id: "reconcile-omitted-claim".into(),
                expected_revision: Some(7),
                claim_revision: None,
            }
            .encode_to_vec(),
        ),
        (
            "manager_add_issue_relation",
            pb::AddIssueRelationArgs {
                context: Some(manager_context(
                    "manager_add_issue_relation",
                    "issue:source",
                )),
                relation_id: "relation-1".into(),
                kind: v1::RelationKind::Blocks as i32,
                source_issue_id: "source".into(),
                source_expected_revision: Some(1),
                target_issue_id: "target".into(),
                target_expected_revision: Some(3),
            }
            .encode_to_vec(),
        ),
        (
            "manager_remove_issue_relation",
            pb::RemoveIssueRelationArgs {
                context: Some(manager_context(
                    "manager_remove_issue_relation",
                    "issue:source",
                )),
                relation_id: "relation-1".into(),
                source_issue_id: "source".into(),
                source_expected_revision: Some(4),
                target_issue_id: "target".into(),
                target_expected_revision: Some(5),
            }
            .encode_to_vec(),
        ),
        (
            "manager_add_issue_comment",
            pb::AddIssueCommentArgs {
                context: Some(manager_context(
                    "manager_add_issue_comment",
                    "issue:comment",
                )),
                issue_id: "comment".into(),
                expected_revision: Some(8),
                comment_id: "comment-1".into(),
                body: "safe body".into(),
            }
            .encode_to_vec(),
        ),
    ];

    for (method, payload) in cases {
        let durable = durable_request_from_manager(method, &payload)
            .expect("restart-safe operation should build a durable envelope");
        let envelope = DurableBackendRequest::decode(durable.as_slice())
            .expect("durable envelope should decode");
        assert_eq!(envelope.method, method);
        validate_durable_request_snapshot(method, &durable)
            .expect("durable envelope should pass snapshot validation");
        let durable_text = String::from_utf8_lossy(&durable);
        assert!(!durable_text.contains("lease_handle"));
        assert!(!durable_text.contains("Issue"));
    }
}

#[test]
fn durable_admission_rejects_create_and_lease_capabilities() {
    let create = pb::CreateIssueArgs {
        context: Some(manager_context("manager_create_issue", "issue:create")),
        issue_id: "create".into(),
        issue: Some(v1::IssueDraft::default()),
        ..Default::default()
    }
    .encode_to_vec();
    assert!(matches!(
        durable_request_from_manager("manager_create_issue", &create),
        Err(SouthboundError::NotSent(reason)) if reason.contains("structurally invalid")
    ));

    let heartbeat = pb::HeartbeatIssueClaimArgs {
        context: Some(manager_context(
            "manager_heartbeat_issue_claim",
            "issue:claim",
        )),
        issue_id: "claim".into(),
        expected_revision: Some(1),
        claim_revision: Some(1),
        lease_handle: b"never-persist".to_vec(),
        ..Default::default()
    }
    .encode_to_vec();
    assert!(matches!(
        durable_request_from_manager("manager_heartbeat_issue_claim", &heartbeat),
        Err(SouthboundError::NotSent(reason)) if reason.contains("capability")
    ));

    let release = pb::ReleaseIssueClaimArgs {
        context: Some(manager_context(
            "manager_release_issue_claim",
            "issue:claim",
        )),
        issue_id: "claim".into(),
        expected_revision: Some(1),
        claim_revision: Some(1),
        lease_handle: b"never-persist".to_vec(),
    }
    .encode_to_vec();
    assert!(matches!(
        durable_request_from_manager("manager_release_issue_claim", &release),
        Err(SouthboundError::NotSent(reason)) if reason.contains("capability")
    ));

    let transition_with_handle = pb::TransitionIssueArgs {
        context: Some(manager_context("manager_transition_issue", "issue:claim")),
        issue_id: "claim".into(),
        expected_revision: Some(1),
        target_state: v1::IssueState::Done as i32,
        lease_handle: Some(b"never-persist".to_vec()),
        ..Default::default()
    }
    .encode_to_vec();
    assert!(matches!(
        durable_request_from_manager("manager_transition_issue", &transition_with_handle),
        Err(SouthboundError::NotSent(reason)) if reason.contains("lease handle")
    ));

    let unspecified_state = pb::UpdateIssueArgs {
        context: Some(manager_context("manager_update_issue", "issue:target")),
        issue_id: "target".into(),
        expected_revision: Some(1),
        update_mask: vec![pb::ManagerIssueField::Title as i32],
        patch: Some(pb::ManagerIssuePatch {
            title: Some("updated".into()),
            ..Default::default()
        }),
        target_state: Some(v1::IssueState::Unspecified as i32),
        terminal_report: None,
    }
    .encode_to_vec();
    assert!(matches!(
        validate_backend_admission("manager_update_issue", &unspecified_state),
        Err(reason) if reason.contains("target_state")
    ));
}

#[test]
fn administrative_action_has_a_typed_restart_safe_envelope() {
    let args = pb::ApplyIssueAdminActionArgs {
        context: Some(manager_context(
            "manager_apply_issue_admin_action",
            "issue:admin-target",
        )),
        issue_id: "admin-target".into(),
        expected_revision: Some(7),
        action: pb::IssueAdminAction::Recover as i32,
        ..Default::default()
    }
    .encode_to_vec();

    let durable = durable_request_from_manager("manager_apply_issue_admin_action", &args)
        .expect("administrative action should have a durable envelope");
    validate_durable_request_snapshot("manager_apply_issue_admin_action", &durable)
        .expect("administrative envelope should validate");
    let envelope = DurableBackendRequest::decode(durable.as_slice()).unwrap();
    assert_eq!(
        envelope
            .admin_action
            .and_then(|request| request.args)
            .unwrap(),
        pb::ApplyIssueAdminActionArgs::decode(args.as_slice()).unwrap()
    );

    let invalid = pb::ApplyIssueAdminActionArgs {
        context: Some(manager_context(
            "manager_apply_issue_admin_action",
            "issue:admin-target",
        )),
        issue_id: "admin-target".into(),
        expected_revision: Some(7),
        ..Default::default()
    }
    .encode_to_vec();
    let durable = durable_request_from_manager("manager_apply_issue_admin_action", &invalid)
        .expect("structural validation happens for persisted envelopes");
    assert!(matches!(
        validate_durable_request_snapshot("manager_apply_issue_admin_action", &durable),
        Err(SouthboundError::NotSent(reason)) if reason.contains("administrative request")
    ));
}

#[test]
fn administrative_receipt_requires_a_typed_marker_snapshot() {
    let args = pb::ApplyIssueAdminActionArgs {
        issue_id: "issue-admin".into(),
        expected_revision: Some(9),
        action: pb::IssueAdminAction::Recover as i32,
        ..Default::default()
    };
    let marker = v1::OperationRecord {
        downstream_operation_id: "admin-marker-9".into(),
        committed_at: Some(v1::UtcInstant {
            unix_epoch_millis: 1234,
        }),
        result_snapshot: Some(v1::OperationResultSnapshot {
            value: Some(v1::operation_result_snapshot::Value::IssueMutation(
                v1::IssueMutationSnapshot {
                    current: Some(v1::IssueDetails {
                        issue: Some(v1::Issue {
                            issue_id: args.issue_id.clone(),
                            revision: 9,
                            state: v1::IssueState::Open as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }),
        ..Default::default()
    };

    assert!(matches!(
        admin_action_result_from_marker(&marker, &args),
        Err(BackendDispatchError::InvalidRequest(
            "typed administrative receipt is missing"
        ))
    ));
}

#[test]
fn durable_create_preserves_source_evidence_without_canonical_issue() {
    let create = pb::CreateIssueArgs {
        context: Some(manager_context("manager_create_issue", "issue:create")),
        issue_id: "create".into(),
        issue: Some(v1::IssueDraft {
            title: "created issue".into(),
            kind: v1::IssueKind::Task as i32,
            priority: v1::IssuePriority::P2 as i32,
            creation_basis: v1::CreationBasis::Explicit as i32,
            confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
            source_evidence: vec![v1::SourceEvidenceRef {
                thread_id: "thread-1".into(),
                memory_id: "memory-1".into(),
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();

    let durable = durable_request_from_manager("manager_create_issue", &create)
        .expect("verified create intent should be restart safe");
    let envelope = DurableBackendRequest::decode(durable.as_slice()).expect("decode envelope");
    let request = envelope.create_issue.expect("create intent");
    assert_eq!(request.issue_id, "create");
    let issue = request.issue.expect("draft");
    assert_eq!(issue.title, "created issue");
    assert_eq!(issue.source_evidence.len(), 1);
    assert!(envelope.update_issue.is_none());
    assert!(
        !durable
            .windows(b"lease_handle".len())
            .any(|window| window == b"lease_handle")
    );
}

#[test]
fn durable_create_allows_an_unreviewed_human_explicit_issue_without_source_evidence() {
    let mut context = manager_context("manager_create_issue", "issue:create");
    context.actor_audit = Some(pb::ActorAudit {
        actor_id: "human-1".into(),
        actor_kind: v1::ActorKind::Human as i32,
        ..Default::default()
    });
    let create = pb::CreateIssueArgs {
        context: Some(context),
        issue_id: "create".into(),
        issue: Some(v1::IssueDraft {
            title: "manually recorded".into(),
            kind: v1::IssueKind::Task as i32,
            priority: v1::IssuePriority::P2 as i32,
            creation_basis: v1::CreationBasis::Explicit as i32,
            confirmation_status: v1::ConfirmationStatus::Unreviewed as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();

    let durable = durable_request_from_manager("manager_create_issue", &create)
        .expect("human explicit create should be restart safe");
    validate_durable_request_snapshot("manager_create_issue", &durable)
        .expect("durable snapshot should retain the same admission rule");
}

#[test]
fn durable_intents_reconstruct_v1_requests_without_capabilities() {
    let cases = [
        (
            "manager_claim_issue",
            pb::ClaimIssueArgs {
                context: Some(manager_context("manager_claim_issue", "issue:claim")),
                issue_id: "claim".into(),
                expected_revision: Some(4),
                requested_lease: Some(pb::TimeSpan { millis: 5_000 }),
                gateway_eligibility_generation: None,
            }
            .encode_to_vec(),
        ),
        (
            "manager_transition_issue",
            pb::TransitionIssueArgs {
                context: Some(manager_context(
                    "manager_transition_issue",
                    "issue:transition",
                )),
                issue_id: "transition".into(),
                expected_revision: Some(2),
                target_state: v1::IssueState::Open as i32,
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        (
            "manager_reconcile_expired_claim",
            pb::ReconcileExpiredClaimArgs {
                context: Some(manager_context(
                    "manager_reconcile_expired_claim",
                    "issue:reconcile",
                )),
                issue_id: "reconcile".into(),
                expected_revision: Some(6),
                claim_revision: None,
            }
            .encode_to_vec(),
        ),
        (
            "manager_add_issue_relation",
            pb::AddIssueRelationArgs {
                context: Some(manager_context(
                    "manager_add_issue_relation",
                    "issue:source",
                )),
                relation_id: "relation-1".into(),
                kind: v1::RelationKind::Blocks as i32,
                source_issue_id: "source".into(),
                source_expected_revision: Some(1),
                target_issue_id: "target".into(),
                target_expected_revision: Some(3),
            }
            .encode_to_vec(),
        ),
        (
            "manager_remove_issue_relation",
            pb::RemoveIssueRelationArgs {
                context: Some(manager_context(
                    "manager_remove_issue_relation",
                    "issue:source",
                )),
                relation_id: "relation-1".into(),
                source_issue_id: "source".into(),
                source_expected_revision: Some(4),
                target_issue_id: "target".into(),
                target_expected_revision: Some(5),
            }
            .encode_to_vec(),
        ),
        (
            "manager_add_issue_comment",
            pb::AddIssueCommentArgs {
                context: Some(manager_context(
                    "manager_add_issue_comment",
                    "issue:comment",
                )),
                issue_id: "comment".into(),
                expected_revision: Some(8),
                comment_id: "comment-1".into(),
                body: "body".into(),
            }
            .encode_to_vec(),
        ),
    ];

    for (method, payload) in cases {
        let durable = durable_request_from_manager(method, &payload).expect("durable request");
        let (v1_method, v1_payload) = translate_manager_request(&BackendDispatchRequest {
            method: method.into(),
            payload: durable,
            downstream_operation_id: Some("downstream-1".into()),
        })
        .expect("durable request should reconstruct a v1 request");
        assert_eq!(v1_method, method.strip_prefix("manager_").unwrap());
        assert!(
            !v1_payload
                .windows(b"lease_handle".len())
                .any(|window| { window == b"lease_handle" })
        );
        match method {
            "manager_claim_issue" => {
                let args = v1::ClaimIssueArgs::decode(v1_payload.as_slice()).unwrap();
                assert_eq!(args.issue_id, "claim");
                assert_eq!(args.expected_revision, Some(4));
            }
            "manager_transition_issue" => {
                let args = v1::TransitionIssueArgs::decode(v1_payload.as_slice()).unwrap();
                assert!(args.lease_handle.is_none());
                assert_eq!(args.issue_id, "transition");
            }
            "manager_reconcile_expired_claim" => {
                let args = v1::ReconcileExpiredClaimArgs::decode(v1_payload.as_slice()).unwrap();
                assert_eq!(args.claim_revision, None);
            }
            "manager_add_issue_relation" => {
                let args = v1::AddIssueRelationArgs::decode(v1_payload.as_slice()).unwrap();
                assert_eq!(args.source_issue_id, "source");
                assert_eq!(args.target_issue_id, "target");
            }
            "manager_remove_issue_relation" => {
                let args = v1::RemoveIssueRelationArgs::decode(v1_payload.as_slice()).unwrap();
                assert_eq!(args.relation_id, "relation-1");
            }
            "manager_add_issue_comment" => {
                let args = v1::AddIssueCommentArgs::decode(v1_payload.as_slice()).unwrap();
                assert_eq!(args.body, "body");
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn durable_update_preserves_terminal_report_for_tracker_audit() {
    let request = pb::UpdateIssueArgs {
        context: Some(manager_context(
            "manager_update_issue",
            "issue:issue-terminal",
        )),
        issue_id: "issue-terminal".into(),
        expected_revision: Some(3),
        update_mask: vec![pb::ManagerIssueField::ConfirmationStatus as i32],
        patch: Some(pb::ManagerIssuePatch {
            confirmation_status: Some(v1::ConfirmationStatus::Confirmed as i32),
            ..Default::default()
        }),
        target_state: Some(v1::IssueState::Done as i32),
        terminal_report: Some(pb::update_issue_args::TerminalReport::Completion(
            v1::CompletionReport {
                result: Some(v1::completion_report::Result::Unverified(
                    v1::UnverifiedCompletion {
                        reason: "human confirmation".into(),
                        residual_risk: "none".into(),
                    },
                )),
            },
        )),
    }
    .encode_to_vec();
    let durable = durable_request_from_manager("manager_update_issue", &request)
        .expect("durable update request");
    let envelope = DurableBackendRequest::decode(durable.as_slice()).expect("decode envelope");
    assert!(matches!(
        envelope
            .update_issue
            .as_ref()
            .and_then(|update| update.terminal_report.as_ref()),
        Some(durable_update_issue_request::TerminalReport::Completion(_))
    ));

    let (method, payload) = translate_manager_request(&BackendDispatchRequest {
        method: "manager_update_issue".into(),
        payload: durable,
        downstream_operation_id: Some("downstream-terminal-update".into()),
    })
    .expect("translate durable update");
    assert_eq!(method, "update_issue");
    let translated = v1::UpdateIssueArgs::decode(payload.as_slice()).expect("decode v1 update");
    assert!(matches!(
        translated.terminal_report,
        Some(v1::update_issue_args::TerminalReport::Completion(_))
    ));
}

#[tokio::test]
async fn tracker_dispatcher_writes_canonical_tracker_and_returns_verified_marker() {
    let root = tempdir().expect("temporary tracker root");
    let tracker = Tracker::open(root.path()).await.expect("open tracker");
    let issue_id = Uuid::new_v4();
    tracker
        .create_with_id(issue_id, "seed-operation", "before")
        .await
        .expect("seed issue");
    let downstream_operation_id = "downstream-update-1";
    let payload = manager_update_payload(&issue_id.to_string());
    let dispatcher = TrackerV1Dispatcher::new(
        tracker.clone(),
        RunnerConfig {
            data_root: root.path().to_path_buf(),
            source_evidence_limits: crate::tracker::config::SourceEvidenceLimits::UNBOUNDED,
            default_lease_secs: 60,
            minimum_lease_secs: 1,
            maximum_lease_secs: 300,
            maximum_heartbeat_secs: 30,
            ownership_maintenance_interval_millis: 30_000,
            ownership_recovery_batch_size: 100,
        },
    );

    let response = dispatcher
        .dispatch(&BackendDispatchRequest {
            method: "manager_update_issue".into(),
            payload,
            downstream_operation_id: Some(downstream_operation_id.into()),
        })
        .await
        .expect("tracker write should succeed");

    let marker = response.marker;
    assert_eq!(marker.downstream_operation_id, downstream_operation_id);
    assert_eq!(marker.method_key, "update_issue");
    assert_eq!(marker.scope, v1::OperationScope::Issue as i32);
    assert_eq!(marker.affected_issue_ids, vec![issue_id.to_string()]);
    assert_eq!(marker.resulting_revision, Some(1));
    assert_eq!(
        marker.payload_fingerprint,
        marker_digest(&["update_issue", downstream_operation_id])
    );
    assert!(response.ephemeral_claim_handle.is_none());
    assert_eq!(
        tracker.get(issue_id).await.expect("read issue").title,
        "updated by manager outbox"
    );
}

#[tokio::test]
async fn tracker_dispatcher_returns_claim_handle_only_ephemerally() {
    let root = tempdir().expect("temporary tracker root");
    let tracker = Tracker::open(root.path()).await.expect("open tracker");
    let issue_id = Uuid::new_v4();
    tracker
        .create_with_id(issue_id, "seed-operation", "claim me")
        .await
        .expect("seed issue");
    let manager_args = pb::ClaimIssueArgs {
        context: Some(manager_context(
            "manager_claim_issue",
            &format!("issue:{issue_id}"),
        )),
        issue_id: issue_id.to_string(),
        expected_revision: Some(0),
        requested_lease: Some(pb::TimeSpan { millis: 5_000 }),
        gateway_eligibility_generation: None,
    }
    .encode_to_vec();
    let durable = durable_request_from_manager("manager_claim_issue", &manager_args)
        .expect("claim durable request");
    let dispatcher = TrackerV1Dispatcher::new(
        tracker,
        RunnerConfig {
            data_root: root.path().to_path_buf(),
            source_evidence_limits: crate::tracker::config::SourceEvidenceLimits::UNBOUNDED,
            default_lease_secs: 60,
            minimum_lease_secs: 1,
            maximum_lease_secs: 300,
            maximum_heartbeat_secs: 30,
            ownership_maintenance_interval_millis: 30_000,
            ownership_recovery_batch_size: 100,
        },
    );
    let response = dispatcher
        .dispatch(&BackendDispatchRequest {
            method: "manager_claim_issue".into(),
            payload: durable,
            downstream_operation_id: Some("downstream-claim-1".into()),
        })
        .await
        .expect("claim dispatch");

    assert!(
        !response
            .ephemeral_claim_handle
            .as_ref()
            .expect("claim response handle")
            .is_empty()
    );
    assert!(!response.response_payload.is_empty());
}

#[tokio::test]
async fn tracker_capability_dispatch_translates_direct_claim_and_returns_handle() {
    let root = tempdir().expect("temporary tracker root");
    let tracker = Tracker::open(root.path()).await.expect("open tracker");
    let issue_id = Uuid::new_v4();
    tracker
        .create_with_id(issue_id, "seed-operation", "claim me")
        .await
        .expect("seed issue");
    let manager_args = pb::ClaimIssueArgs {
        context: Some(manager_context(
            "manager_claim_issue",
            &format!("issue:{issue_id}"),
        )),
        issue_id: issue_id.to_string(),
        expected_revision: Some(0),
        requested_lease: Some(pb::TimeSpan { millis: 5_000 }),
        gateway_eligibility_generation: None,
    }
    .encode_to_vec();
    let dispatcher = TrackerV1Dispatcher::new(
        tracker.clone(),
        RunnerConfig {
            data_root: root.path().to_path_buf(),
            source_evidence_limits: crate::tracker::config::SourceEvidenceLimits::UNBOUNDED,
            default_lease_secs: 60,
            minimum_lease_secs: 1,
            maximum_lease_secs: 300,
            maximum_heartbeat_secs: 30,
            ownership_maintenance_interval_millis: 30_000,
            ownership_recovery_batch_size: 100,
        },
    );
    let response = dispatcher
        .dispatch_capability(&BackendDispatchRequest {
            method: "manager_claim_issue".into(),
            payload: manager_args,
            downstream_operation_id: Some("downstream-capability-claim-1".into()),
        })
        .await
        .expect("capability claim dispatch");

    assert_eq!(response.marker.method_key, "claim_issue");
    assert_eq!(response.marker.entity_revisions[0].resulting_revision, 1);
    assert!(
        !response
            .ephemeral_claim_handle
            .expect("capability handle")
            .is_empty()
    );
    assert!(!response.response_payload.is_empty());
    assert_eq!(tracker.get(issue_id).await.expect("read issue").revision, 1);
}

#[tokio::test]
async fn leased_dispatch_verifies_real_tracker_marker_and_exposes_mapping() {
    let root = tempdir().expect("temporary tracker root");
    let tracker = Tracker::open(root.path()).await.expect("open tracker");
    let issue_id = Uuid::new_v4();
    tracker
        .create_with_id(issue_id, "seed-operation", "before")
        .await
        .expect("seed issue");
    let downstream_operation_id = "downstream-update-lease-1";
    let payload = manager_update_payload(&issue_id.to_string());
    let operation = NewOperation {
        operation_id: "manager-update-lease-1".into(),
        scope: Some(OperationScope::Issue(issue_id.to_string())),
        method: "manager_update_issue".into(),
        idempotency_scope: IdempotencyScope::new(
            "manager-contract",
            "manager-root",
            "manager_update_issue",
            format!("issue:{issue_id}"),
            "idempotency-lease-1",
        )
        .unwrap(),
        request_digest: digest_bytes(&payload),
        request_schema: "issue_tracker.manager.v1.UpdateIssueArgs".into(),
        downstream_operation_id: Some(downstream_operation_id.into()),
        generation: 1,
        manager_sequence: 1,
        causal_parent: None,
        affects_ready: true,
        sync_subject: Some(issue_id.to_string()),
        affected_issue_ids: vec![issue_id.to_string()],
        actor_audit: None,
        contract_fingerprint: "manager-contract".into(),
        schema_fingerprint: "manager-schema".into(),
        correlation_id: "correlation-lease-1".into(),
        now_millis: 100,
        replayable: true,
    }
    .build()
    .expect("backend operation");
    let expected = MarkerExpectation {
        operation_id: downstream_operation_id.into(),
        method_key: "update_issue".into(),
        scope: v1::OperationScope::Issue as i32,
        payload_fingerprint: marker_digest(&["update_issue", downstream_operation_id]),
        contract_fingerprint: contract_fingerprint(),
        affected_issue_ids: vec![issue_id.to_string()],
        entity_revisions: vec![EntityRevisionExpectation {
            issue_id: issue_id.to_string(),
            backend_id: issue_id.to_string(),
            resulting_revision: 1,
        }],
        result_replayable: true,
    };
    let mut entry = outbox::new_entry(
        "outbox-lease-1",
        &operation,
        "taskchampion",
        "snapshot-lease-1",
        digest_bytes(&payload),
        100,
    )
    .expect("outbox entry");
    outbox::lease(&mut entry, 100, 100, Some("lease-1".into())).expect("lease");
    let dispatcher = TrackerV1Dispatcher::new(
        tracker.clone(),
        RunnerConfig {
            data_root: root.path().to_path_buf(),
            source_evidence_limits: crate::tracker::config::SourceEvidenceLimits::UNBOUNDED,
            default_lease_secs: 60,
            minimum_lease_secs: 1,
            maximum_lease_secs: 300,
            maximum_heartbeat_secs: 30,
            ownership_maintenance_interval_millis: 30_000,
            ownership_recovery_batch_size: 100,
        },
    );

    let result = dispatch_leased(
        &dispatcher,
        DispatchLeaseInput {
            entry: &mut entry,
            operation: &operation,
            request: BackendDispatchRequest {
                method: "manager_update_issue".into(),
                payload,
                downstream_operation_id: Some(downstream_operation_id.into()),
            },
            expected,
            lease_id: "lease-1",
            retry_budget: &retry_budget(2),
            now_millis: 110,
        },
    )
    .await
    .expect("leased tracker dispatch");

    assert_eq!(result.state, pb::DurableOperationState::Committed);
    assert_eq!(entry.status, pb::OutboxStatus::Acknowledged as i32);
    assert_eq!(result.issue_mappings.len(), 1);
    assert_eq!(result.issue_mappings[0].issue_id, issue_id.to_string());
    assert_eq!(result.issue_mappings[0].backend_id, issue_id.to_string());
    assert_eq!(result.issue_mappings[0].revision, 1);
}

#[tokio::test]
async fn tracker_dispatcher_rejects_missing_downstream_operation_id_without_writing() {
    let root = tempdir().expect("temporary tracker root");
    let tracker = Tracker::open(root.path()).await.expect("open tracker");
    let issue_id = Uuid::new_v4();
    tracker
        .create_with_id(issue_id, "seed-operation", "before")
        .await
        .expect("seed issue");
    let payload = manager_update_payload(&issue_id.to_string());
    let dispatcher = TrackerV1Dispatcher::new(
        tracker.clone(),
        RunnerConfig {
            data_root: root.path().to_path_buf(),
            source_evidence_limits: crate::tracker::config::SourceEvidenceLimits::UNBOUNDED,
            default_lease_secs: 60,
            minimum_lease_secs: 1,
            maximum_lease_secs: 300,
            maximum_heartbeat_secs: 30,
            ownership_maintenance_interval_millis: 30_000,
            ownership_recovery_batch_size: 100,
        },
    );

    let error = dispatcher
        .dispatch(&BackendDispatchRequest {
            method: "manager_update_issue".into(),
            payload,
            downstream_operation_id: None,
        })
        .await
        .expect_err("missing downstream ID must fail closed");
    assert!(matches!(error, SouthboundError::NotSent(message) if message.contains("downstream")));
    assert_eq!(
        tracker.get(issue_id).await.expect("read issue").title,
        "before"
    );
}

#[test]
fn admission_derives_persistable_expectations_for_all_issue_backend_writes() {
    let cases = [
        (
            "manager_create_issue",
            pb::CreateIssueArgs {
                issue_id: "issue-create".into(),
                ..Default::default()
            }
            .encode_to_vec(),
            0,
            true,
        ),
        (
            "manager_update_issue",
            pb::UpdateIssueArgs {
                issue_id: "issue-update".into(),
                expected_revision: Some(4),
                ..Default::default()
            }
            .encode_to_vec(),
            5,
            true,
        ),
        (
            "manager_claim_issue",
            pb::ClaimIssueArgs {
                issue_id: "issue-claim".into(),
                expected_revision: Some(7),
                ..Default::default()
            }
            .encode_to_vec(),
            8,
            false,
        ),
        (
            "manager_transition_issue",
            pb::TransitionIssueArgs {
                issue_id: "issue-transition".into(),
                expected_revision: Some(2),
                ..Default::default()
            }
            .encode_to_vec(),
            3,
            true,
        ),
        (
            "manager_heartbeat_issue_claim",
            pb::HeartbeatIssueClaimArgs {
                issue_id: "issue-heartbeat".into(),
                expected_revision: Some(3),
                claim_revision: Some(3),
                ..Default::default()
            }
            .encode_to_vec(),
            4,
            true,
        ),
        (
            "manager_release_issue_claim",
            pb::ReleaseIssueClaimArgs {
                issue_id: "issue-release".into(),
                expected_revision: Some(4),
                claim_revision: Some(4),
                ..Default::default()
            }
            .encode_to_vec(),
            5,
            true,
        ),
        (
            "manager_reconcile_expired_claim",
            pb::ReconcileExpiredClaimArgs {
                issue_id: "issue-reconcile".into(),
                expected_revision: Some(5),
                claim_revision: Some(5),
                ..Default::default()
            }
            .encode_to_vec(),
            6,
            true,
        ),
        (
            "manager_add_issue_comment",
            pb::AddIssueCommentArgs {
                issue_id: "issue-comment".into(),
                expected_revision: Some(6),
                comment_id: "comment-1".into(),
                body: "body".into(),
                ..Default::default()
            }
            .encode_to_vec(),
            7,
            true,
        ),
    ];
    for (method, request, revision, replayable) in cases {
        let mut operation = operation();
        operation.method = method.into();
        operation.downstream_operation_id = format!("downstream-{method}");
        operation.result_replayability = if replayable {
            v1::OperationResultReplayability::Replayable as i32
        } else {
            v1::OperationResultReplayability::NotReplayable as i32
        };
        let expected = derive_marker_expectation(&operation, &request)
            .expect("supported request has a deterministic marker contract");
        assert_eq!(expected.entity_revisions[0].resulting_revision, revision);
        assert_eq!(expected.result_replayable, replayable);
        assert_eq!(
            expectation_from_snapshot(&expectation_snapshot(&expected)).unwrap(),
            expected
        );
    }
}

#[test]
fn admission_derives_relation_marker_expectations_from_the_relation_anchor() {
    let cases = [
        (
            "manager_add_issue_relation",
            pb::AddIssueRelationArgs {
                relation_id: "related:source:target".into(),
                kind: v1::RelationKind::Related as i32,
                source_issue_id: "source".into(),
                source_expected_revision: Some(2),
                target_issue_id: "target".into(),
                target_expected_revision: Some(9),
                ..Default::default()
            }
            .encode_to_vec(),
            "source",
            3,
        ),
        (
            "manager_remove_issue_relation",
            pb::RemoveIssueRelationArgs {
                relation_id: "related:source:target".into(),
                source_issue_id: "source".into(),
                source_expected_revision: Some(11),
                target_issue_id: "target".into(),
                target_expected_revision: Some(13),
                ..Default::default()
            }
            .encode_to_vec(),
            "source",
            12,
        ),
        (
            "manager_add_issue_relation",
            pb::AddIssueRelationArgs {
                relation_id: "blocks:source:target".into(),
                kind: v1::RelationKind::Blocks as i32,
                source_issue_id: "source".into(),
                source_expected_revision: Some(2),
                target_issue_id: "target".into(),
                target_expected_revision: Some(9),
                ..Default::default()
            }
            .encode_to_vec(),
            "target",
            10,
        ),
        (
            "manager_remove_issue_relation",
            pb::RemoveIssueRelationArgs {
                relation_id: "blocks:source:target".into(),
                source_issue_id: "source".into(),
                source_expected_revision: Some(11),
                target_issue_id: "target".into(),
                target_expected_revision: Some(13),
                ..Default::default()
            }
            .encode_to_vec(),
            "target",
            14,
        ),
    ];

    for (method, request, anchor, revision) in cases {
        let mut operation = operation();
        operation.method = method.into();
        operation.downstream_operation_id = format!("downstream-{method}");
        let expected = derive_marker_expectation(&operation, &request)
            .expect("relation requests have a deterministic marker contract");

        assert_eq!(
            expected.method_key,
            method.strip_prefix("manager_").unwrap()
        );
        assert_eq!(expected.scope, v1::OperationScope::Issue as i32);
        assert_eq!(expected.affected_issue_ids, vec![anchor]);
        assert_eq!(
            expected.entity_revisions,
            vec![EntityRevisionExpectation {
                issue_id: anchor.into(),
                backend_id: anchor.into(),
                resulting_revision: revision,
            }]
        );
        assert!(expected.result_replayable);
        assert_eq!(
            expectation_from_snapshot(&expectation_snapshot(&expected)).unwrap(),
            expected
        );
    }
}

#[test]
fn admission_rejects_structurally_invalid_backend_requests() {
    let mut operation = operation();

    let missing_revision = pb::UpdateIssueArgs {
        issue_id: "issue".into(),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        derive_marker_expectation(&operation, &missing_revision),
        Err(BackendDispatchError::InvalidRequest("expected_revision"))
    );

    let overflow = pb::UpdateIssueArgs {
        issue_id: "issue".into(),
        expected_revision: Some(u64::MAX),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        derive_marker_expectation(&operation, &overflow),
        Err(BackendDispatchError::InvalidRequest("expected_revision"))
    );

    operation.method = "manager_heartbeat_issue_claim".into();
    let mismatched_claim_revision = pb::HeartbeatIssueClaimArgs {
        issue_id: "issue".into(),
        expected_revision: Some(2),
        claim_revision: Some(1),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        derive_marker_expectation(&operation, &mismatched_claim_revision),
        Err(BackendDispatchError::InvalidRequest("claim_revision"))
    );

    operation.method = "manager_reconcile_expired_claim".into();
    let omitted_claim_revision = pb::ReconcileExpiredClaimArgs {
        issue_id: "issue".into(),
        expected_revision: Some(2),
        claim_revision: None,
        ..Default::default()
    }
    .encode_to_vec();
    let expectation = derive_marker_expectation(&operation, &omitted_claim_revision)
        .expect("reconcile may omit the observed claim revision");
    assert_eq!(expectation.entity_revisions[0].resulting_revision, 3);

    let mismatched_reconcile_claim_revision = pb::ReconcileExpiredClaimArgs {
        issue_id: "issue".into(),
        expected_revision: Some(2),
        claim_revision: Some(1),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        derive_marker_expectation(&operation, &mismatched_reconcile_claim_revision),
        Err(BackendDispatchError::InvalidRequest("claim_revision"))
    );

    operation.method = "manager_update_issue".into();
    assert_eq!(
        derive_marker_expectation(&operation, &[0xff]),
        Err(BackendDispatchError::InvalidRequest("request"))
    );

    operation.method = "manager_add_issue_relation".into();
    let unknown_relation_kind = pb::AddIssueRelationArgs {
        kind: 999,
        source_issue_id: "source".into(),
        source_expected_revision: Some(1),
        target_issue_id: "target".into(),
        target_expected_revision: Some(1),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        derive_marker_expectation(&operation, &unknown_relation_kind),
        Err(BackendDispatchError::InvalidRequest("relation_kind"))
    );
}

#[test]
fn admission_rejects_write_context_target_mismatches_including_relations() {
    let operation = operation();
    let update = pb::UpdateIssueArgs {
        context: Some(pb::ManagerWriteContext {
            target: "issue:other".into(),
            ..Default::default()
        }),
        issue_id: "issue".into(),
        expected_revision: Some(1),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(
        derive_marker_expectation(&operation, &update),
        Err(BackendDispatchError::InvalidRequest("write_context.target"))
    );

    let mut relation_operation = operation;
    relation_operation.method = "manager_remove_issue_relation".into();
    let relation = pb::RemoveIssueRelationArgs {
        context: Some(pb::ManagerWriteContext {
            target: "issue:target".into(),
            ..Default::default()
        }),
        relation_id: "related:source:target".into(),
        source_issue_id: "source".into(),
        source_expected_revision: Some(1),
        target_issue_id: "target".into(),
        target_expected_revision: Some(1),
    }
    .encode_to_vec();
    assert_eq!(
        derive_marker_expectation(&relation_operation, &relation),
        Err(BackendDispatchError::InvalidRequest("write_context.target"))
    );
}

#[test]
fn typed_infrastructure_errors_remain_ambiguous_for_reconciliation() {
    let validation = v1::IssueTrackerError {
        code: v1::ErrorCode::RevisionConflict as i32,
        ..Default::default()
    };
    assert!(typed_error_is_definitive(&validation));

    for code in [
        v1::ErrorCode::BackendUnavailable,
        v1::ErrorCode::ResultUnconfirmed,
        v1::ErrorCode::RootLocked,
        v1::ErrorCode::BackupInvalid,
    ] {
        assert!(!typed_error_is_definitive(&v1::IssueTrackerError {
            code: code as i32,
            ..Default::default()
        }));
    }
}

#[test]
fn admin_actor_identity_carries_kind_session_and_correlation() {
    let mut context = pb::ManagerWriteContext {
        correlation_id: "admin-correlation".into(),
        actor_audit: Some(pb::ActorAudit {
            actor_id: "admin".into(),
            actor_kind: v1::ActorKind::Human as i32,
            agent_session_id: Some("admin-session".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let identity = admin_actor_identity(&context, "operation-1").unwrap();
    assert_eq!(identity.actor_id(), "admin");
    assert_eq!(identity.kind(), v1::ActorKind::Human);
    assert_eq!(identity.agent_session_id(), Some("admin-session"));
    assert_eq!(identity.correlation_id(), Some("admin-correlation"));

    // An empty correlation falls back to the downstream operation identity.
    context.correlation_id.clear();
    context.actor_audit.as_mut().unwrap().agent_session_id = None;
    let identity = admin_actor_identity(&context, "operation-1").unwrap();
    assert_eq!(identity.correlation_id(), Some("operation-1"));
    assert_eq!(identity.agent_session_id(), None);
}

#[test]
fn admin_actor_identity_rejects_missing_or_unknown_actor_kind() {
    let mut context = pb::ManagerWriteContext::default();
    assert_eq!(
        admin_actor_identity(&context, "operation-1"),
        Err(AdminActorError::Missing)
    );
    context.actor_audit = Some(pb::ActorAudit {
        actor_id: "admin".into(),
        actor_kind: 9876,
        ..Default::default()
    });
    assert_eq!(
        admin_actor_identity(&context, "operation-1"),
        Err(AdminActorError::InvalidKind)
    );
    context.actor_audit.as_mut().unwrap().actor_kind = v1::ActorKind::Unspecified as i32;
    assert_eq!(
        admin_actor_identity(&context, "operation-1"),
        Err(AdminActorError::InvalidKind)
    );
    let actor = context.actor_audit.as_mut().unwrap();
    actor.actor_kind = v1::ActorKind::Human as i32;
    actor.actor_id = "  ".into();
    assert_eq!(
        admin_actor_identity(&context, "operation-1"),
        Err(AdminActorError::Missing)
    );
}

#[test]
fn admin_actor_identity_treats_blank_correlation_as_absent() {
    let context = pb::ManagerWriteContext {
        correlation_id: "   ".into(),
        actor_audit: Some(pb::ActorAudit {
            actor_id: "admin".into(),
            actor_kind: v1::ActorKind::Human as i32,
            ..Default::default()
        }),
        ..Default::default()
    };
    let identity = admin_actor_identity(&context, "operation-1").unwrap();
    assert_eq!(identity.correlation_id(), Some("operation-1"));
}
