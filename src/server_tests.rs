use super::*;
use crate::proto_types::rpc_v1::gateway_control_service_server::GatewayControlService;
use crate::proto_types::rpc_v1::issue_gateway_server::IssueGateway;
use crate::proto_types::rpc_v1::issue_service_server::IssueService;
use crate::proto_types::rpc_v1::issue_tracker_service_server::IssueTrackerService;
use crate::proto_types::rpc_v1::outbox_maintenance_service_server::OutboxMaintenanceService;
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    transport::{Channel, Endpoint},
};

#[test]
fn only_definite_tracker_rejections_can_finalize_admin_actions_as_not_applied() {
    assert!(admin_action_rejected_before_mutation(
        &TrackerError::RevisionConflict {
            expected: 3,
            actual: 4,
        }
    ));
    assert!(admin_action_rejected_before_mutation(
        &TrackerError::InvalidLease {
            lease_expires_at: None,
            observed_revision: None,
        }
    ));
    assert!(admin_action_rejected_before_mutation(
        &TrackerError::Blocked {
            open_blocker_issue_ids: Vec::new(),
        }
    ));

    // A backend failure can happen after TaskChampion has committed the
    // write, so treating it as a final no-op would make a later replay
    // unsafe.
    assert!(!admin_action_rejected_before_mutation(
        &TrackerError::Unavailable
    ));
    assert!(!admin_action_rejected_before_mutation(
        &TrackerError::ResultUnconfirmed
    ));
}

#[test]
fn generic_invalid_lease_is_reported_as_active_not_expired() {
    let detail = admin_error_detail(
        manager_v1::ManagerErrorCode::PreconditionFailed,
        "lease is invalid or expired",
    );
    assert!(matches!(
        detail,
        Some(manager_v1::manager_error::Detail::OwnershipIntegrityDiagnosis(diagnosis))
            if diagnosis.lease_classification.as_deref() == Some("ACTIVE")
    ));
}

#[test]
fn terminal_admin_rejection_uses_state_transition_code() {
    assert_eq!(
        admin_manager_error_code(&TrackerError::InvalidStateTransition(
            "issue is already terminal".into()
        )),
        manager_v1::ManagerErrorCode::InvalidStateTransition
    );
    let detail = admin_error_detail(
        manager_v1::ManagerErrorCode::InvalidStateTransition,
        "issue is already terminal",
    );
    assert!(detail.is_none());
}

#[test]
fn admin_lease_diagnosis_preserves_expiry_without_raw_lease_values() {
    let detail = admin_error_detail_with_attributes(
        manager_v1::ManagerErrorCode::PreconditionFailed,
        "lease has expired",
        &[],
        Some(7),
        lease_expiry_to_instant(42),
        &[],
    );
    assert!(matches!(
        detail,
        Some(manager_v1::manager_error::Detail::OwnershipIntegrityDiagnosis(diagnosis))
            if diagnosis.observed_revision == Some(7)
                && diagnosis.lease_expires_at.as_ref().map(|v| v.unix_epoch_millis)
                    == Some(42_000)
    ));
}

#[test]
fn admin_blocker_diagnosis_preserves_safe_issue_ids() {
    let blockers = vec!["issue-a".to_owned(), "issue-b".to_owned()];
    let detail = admin_error_detail_with_attributes(
        manager_v1::ManagerErrorCode::PreconditionFailed,
        "issue is blocked by a dependency",
        &[],
        None,
        None,
        &blockers,
    );
    assert!(matches!(
        detail,
        Some(manager_v1::manager_error::Detail::OwnershipIntegrityDiagnosis(diagnosis))
            if diagnosis.open_blocker_issue_ids == blockers
    ));
}

#[tokio::test]
async fn ambiguous_admin_action_error_remains_unconfirmed_and_blocks_readiness() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = Arc::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = app.core.tracker().create("admin", "admin").await.unwrap();
    let identity = app.manager_store_context().await.unwrap();
    let context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "ambiguous-admin-action".into(),
        correlation_id: "test".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "admin".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("test ambiguity".into()),
            ..Default::default()
        }),
    };
    let scope = IdempotencyScope::new(
        context.contract_fingerprint.clone(),
        context.data_root_id.clone(),
        context.method.clone(),
        context.target.clone(),
        context.idempotency_key.clone(),
    )
    .unwrap();
    let request = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        action: manager_v1::IssueAdminAction::Revoke as i32,
        ..Default::default()
    };
    let digest = digest_bytes(&request.encode_to_vec());
    let operation_id = "admin-ambiguous-test".to_owned();
    app.admit_admin_action(
        &context,
        &scope,
        digest.clone(),
        operation_id.clone(),
        issue.id.to_string(),
        manager_v1::IssueAdminAction::Revoke,
        issue.revision,
        request.encode_to_vec(),
    )
    .await
    .unwrap();

    app.mark_admin_action_unconfirmed(&scope, &digest, &operation_id)
        .await
        .unwrap();

    let operation = {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .repository()
            .get_operation(&operation_id)
            .unwrap()
            .unwrap()
    };
    assert_eq!(
        operation.state,
        manager_v1::DurableOperationState::Unconfirmed as i32
    );
    assert!(operation.affects_ready);
    assert!(operation.resolution_required);
}

#[tokio::test]
async fn human_arbitration_replaces_an_existing_failed_replay_result() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = app
        .core
        .tracker()
        .create("failed arbitration", "failed arbitration")
        .await
        .unwrap();
    let identity = app.manager_store_context().await.unwrap();
    let context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "failed-arbitration-source".into(),
        correlation_id: "failed-arbitration-source".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("seed an unresolved terminal operation".into()),
            ..Default::default()
        }),
    };
    let args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        action: manager_v1::IssueAdminAction::Recover as i32,
        ..Default::default()
    };
    let scope = IdempotencyScope::new(
        context.contract_fingerprint.clone(),
        context.data_root_id.clone(),
        context.method.clone(),
        context.target.clone(),
        context.idempotency_key.clone(),
    )
    .unwrap();
    let request_payload = args.encode_to_vec();
    let request_digest = digest_bytes(&request_payload);
    let source_operation_id = "failed-arbitration-source-operation";
    app.admit_admin_action(
        &context,
        &scope,
        request_digest.clone(),
        source_operation_id.into(),
        issue.id.to_string(),
        manager_v1::IssueAdminAction::Recover,
        issue.revision,
        request_payload,
    )
    .await
    .unwrap();

    // First finalize a typed failure so the source has a non-empty,
    // immutable replay result, then mark it unresolved as would happen
    // after a delivery/reconciliation interruption.
    app.fail_admin_action(
        &scope,
        &request_digest,
        source_operation_id,
        manager_v1::ManagerErrorCode::PreconditionFailed,
        "the original result is no longer provable".into(),
    )
    .await
    .unwrap();
    let mut source = app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        source.state,
        manager_v1::DurableOperationState::Failed as i32
    );
    source.resolution_required = true;
    let source_generation = source.generation;
    app.manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .update_operation(&source)
        .unwrap();
    let expected_previous_operation = app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();
    let expected_previous_replay = app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .operation_replay_snapshot(
            source_operation_id,
            &expected_previous_operation.request_digest,
        )
        .unwrap()
        .expect("failed operation must retain its original replay result");

    let arbitration_args = manager_v1::ResolveIssueOperationArgs {
        context: Some(manager_v1::ManagerWriteContext {
            method: "manager_resolve_issue_operation".into(),
            target: format!("issue:{}", issue.id),
            schema_fingerprint: identity.schema_fingerprint.clone(),
            data_root_id: identity.data_root_id.clone(),
            contract_fingerprint: identity.contract_fingerprint.clone(),
            idempotency_key: "failed-arbitration-resolution".into(),
            correlation_id: "failed-arbitration-resolution".into(),
            actor_audit: Some(manager_v1::ActorAudit {
                actor_id: "administrator".into(),
                actor_kind: v1::ActorKind::Human as i32,
                reason: Some("settle the unresolved failed operation".into()),
                ..Default::default()
            }),
        }),
        issue_id: issue.id.to_string(),
        operation_id: source_operation_id.into(),
        expected_operation_generation: Some(source_generation),
        expected_revision: Some(issue.revision),
        mode: manager_v1::IssueOperationResolutionMode::HumanArbitration as i32,
        arbitration_outcome: manager_v1::IssueOperationArbitrationOutcome::UnknownSettled as i32,
        uncertainty_acknowledged: true,
        arbitration_reason: "settle the unresolved failed operation".into(),
        ..Default::default()
    };
    let server = GrpcServer::new(app);
    let resolution =
        ManagerConsumerService::resolve_issue_operation(&server, Request::new(arbitration_args))
            .await
            .expect("failed unresolved operation must be arbitrable")
            .into_inner();
    let resolution_mutation = match resolution.outcome.as_ref() {
        Some(manager_v1::resolve_issue_operation_result::Outcome::Success(mutation)) => mutation,
        outcome => panic!("unexpected arbitration outcome: {outcome:?}"),
    };
    let resolution_receipt = match resolution_mutation.result.as_ref() {
        Some(manager_v1::manager_mutation::Result::IssueOperationResolution(receipt)) => receipt,
        result => panic!("arbitration has no typed receipt: {result:?}"),
    };
    assert!(
        resolution_receipt
            .audit_ref
            .starts_with("manager-resolution-audit:")
    );
    assert_eq!(
        resolution_receipt.evidence_status,
        manager_v1::IssueOperationResolutionEvidenceStatus::NoneSupplied as i32
    );
    assert!(!resolution_receipt.verified_facts.is_empty());
    assert!(!resolution_receipt.remaining_uncertainties.is_empty());
    let previous_operation_snapshot = resolution_receipt
        .previous_operation_snapshot
        .as_ref()
        .expect("arbitration must retain the previous operation snapshot");
    let previous_operation =
        manager_v1::DurableOperation::decode(previous_operation_snapshot.payload.as_slice())
            .expect("previous operation snapshot must decode");
    assert_eq!(
        previous_operation.state,
        manager_v1::DurableOperationState::Failed as i32
    );
    assert_eq!(previous_operation.generation, source_generation);
    assert_eq!(
        previous_operation_snapshot.payload,
        expected_previous_operation.encode_to_vec()
    );
    let previous_replay_snapshot = resolution_receipt
        .previous_replay_snapshot
        .as_ref()
        .expect("arbitration must retain the previous replay snapshot");
    assert_eq!(
        previous_replay_snapshot.schema,
        "issue_tracker.manager.v1.ApplyIssueAdminActionResult"
    );
    manager_v1::ApplyIssueAdminActionResult::decode(previous_replay_snapshot.payload.as_slice())
        .expect("previous replay snapshot must use the method-specific result schema");
    assert_eq!(
        Some(previous_replay_snapshot.payload.as_slice()),
        Some(expected_previous_replay.as_slice())
    );
    let audit = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .issue_operation_resolution_audit(&resolution_receipt.operation_id)
        .unwrap()
        .expect("resolution audit must be durable");
    assert_eq!(audit.source_operation_id, source_operation_id);
    assert_eq!(audit.source_generation, source_generation);
    assert_eq!(
        audit.source_operation_payload,
        expected_previous_operation.encode_to_vec()
    );
    assert_eq!(audit.source_replay_payload, Some(expected_previous_replay));

    let replay = ManagerConsumerService::apply_issue_admin_action(&server, Request::new(args))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        replay.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ResultUnconfirmed as i32
    ));
    let source = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        source.state,
        manager_v1::DurableOperationState::Arbitrated as i32
    );
}

#[tokio::test]
async fn admin_finalization_rolls_back_when_replay_receipt_write_fails() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = app.core.tracker().create("admin", "admin").await.unwrap();
    let identity = app.manager_store_context().await.unwrap();
    let context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "admin-finalization-rollback".into(),
        correlation_id: "test".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("exercise transactional finalization".into()),
            ..Default::default()
        }),
    };
    let args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        action: manager_v1::IssueAdminAction::Recover as i32,
        ..Default::default()
    };
    let scope = IdempotencyScope::new(
        context.contract_fingerprint.clone(),
        context.data_root_id.clone(),
        context.method.clone(),
        context.target.clone(),
        context.idempotency_key.clone(),
    )
    .unwrap();
    let digest = digest_bytes(&args.encode_to_vec());
    let operation_id = "admin-finalization-rollback".to_owned();
    app.admit_admin_action(
        &context,
        &scope,
        digest.clone(),
        operation_id.clone(),
        issue.id.to_string(),
        manager_v1::IssueAdminAction::Recover,
        issue.revision,
        args.encode_to_vec(),
    )
    .await
    .unwrap();
    {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .read(|connection| {
                connection
                    .execute_batch(
                        "CREATE TRIGGER reject_admin_replay_receipt
                               BEFORE UPDATE OF result_snapshot ON manager_idempotency
                               BEGIN
                                 SELECT RAISE(ABORT, 'injected replay receipt failure');
                               END;",
                    )
                    .map_err(crate::manager::ManagerStorageError::from)
            })
            .unwrap();
    }
    assert!(
        app.fail_admin_action(
            &scope,
            &digest,
            &operation_id,
            manager_v1::ManagerErrorCode::RevisionConflict,
            "rejected before mutation".into(),
        )
        .await
        .is_err()
    );
    let operation = app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(&operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.state,
        manager_v1::DurableOperationState::Received as i32,
        "operation finalization must roll back with its replay receipt"
    );
    assert!(operation.terminal_snapshot.is_none());
}

#[tokio::test]
async fn admin_action_receipt_survives_process_abort_after_tracker_commit() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    const CHILD_ENV: &str = "ISSUE_TRACKER_ADMIN_COMMIT_ABORT_ROOT";
    if let Ok(root) = std::env::var(CHILD_ENV) {
        let root = std::path::PathBuf::from(root);
        let mut configured = settings_for_path(&root);
        configured.tracker.manager_settings = Some(manager_settings());
        let app = IssueTrackerApplication::open(configured).await.unwrap();
        let issue = app
            .core
            .tracker()
            .create("process abort admin receipt", "process abort admin receipt")
            .await
            .unwrap();
        let claimed = app
            .core
            .tracker()
            .claim(issue.id, "worker", issue.revision, 0, "process-abort-claim")
            .await
            .unwrap();
        let identity = app.manager_store_context().await.unwrap();
        let context = manager_v1::ManagerWriteContext {
            method: "manager_apply_issue_admin_action".into(),
            target: format!("issue:{}", issue.id),
            schema_fingerprint: identity.schema_fingerprint,
            data_root_id: identity.data_root_id,
            contract_fingerprint: identity.contract_fingerprint,
            idempotency_key: "process-abort-admin-receipt".into(),
            correlation_id: "process-abort-admin-receipt".into(),
            actor_audit: Some(manager_v1::ActorAudit {
                actor_id: "administrator".into(),
                actor_kind: v1::ActorKind::Human as i32,
                reason: Some("recover ownership after a process abort".into()),
                ..Default::default()
            }),
        };
        let args = manager_v1::ApplyIssueAdminActionArgs {
            context: Some(context.clone()),
            issue_id: issue.id.to_string(),
            expected_revision: Some(claimed.revision),
            action: manager_v1::IssueAdminAction::Recover as i32,
            ..Default::default()
        };
        let scope = IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            context.method.clone(),
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .unwrap();
        let request_payload = args.encode_to_vec();
        let request_digest = digest_bytes(&request_payload);
        let operation_id = format!("admin-{}", digest_bytes(scope.canonical().as_bytes()));
        app.admit_admin_action(
            &context,
            &scope,
            request_digest,
            operation_id.clone(),
            issue.id.to_string(),
            manager_v1::IssueAdminAction::Recover,
            claimed.revision,
            request_payload,
        )
        .await
        .unwrap();
        // This is the cross-store crash boundary: Tracker has committed
        // its immutable marker and Issue revision, while Manager has not
        // yet persisted the terminal receipt.
        app.execute_saved_admin_action(&args, issue.id, &operation_id)
            .await
            .unwrap();
        std::process::abort();
    }

    let root = TempDir::new().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "server::tests::admin_action_receipt_survives_process_abort_after_tracker_commit",
            "--nocapture",
        ])
        .env(CHILD_ENV, root.path())
        .status()
        .unwrap();
    assert!(
        !child.success(),
        "the child must abort at the commit boundary"
    );

    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = Arc::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = app
        .core
        .tracker()
        .list()
        .await
        .unwrap()
        .into_iter()
        .find(|issue| issue.title == "process abort admin receipt")
        .expect("the child persisted the Issue before aborting");
    assert_eq!(issue.state, crate::tracker::IssueState::Open);
    assert_eq!(issue.revision, 2);

    let cycle = app
        .run_outbox_cycle()
        .await
        .expect("startup recovery must finalize the marker");
    assert_eq!(cycle.confirmed_operations, 0);
    let server = GrpcServer::from_app_arc(Arc::clone(&app));
    let identity = server.app.manager_store_context().await.unwrap();
    let args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(manager_v1::ManagerWriteContext {
            method: "manager_apply_issue_admin_action".into(),
            target: format!("issue:{}", issue.id),
            schema_fingerprint: identity.schema_fingerprint.clone(),
            data_root_id: identity.data_root_id.clone(),
            contract_fingerprint: identity.contract_fingerprint.clone(),
            idempotency_key: "process-abort-admin-receipt".into(),
            correlation_id: "process-abort-admin-receipt".into(),
            actor_audit: Some(manager_v1::ActorAudit {
                actor_id: "administrator".into(),
                actor_kind: v1::ActorKind::Human as i32,
                reason: Some("recover ownership after a process abort".into()),
                ..Default::default()
            }),
        }),
        issue_id: issue.id.to_string(),
        expected_revision: Some(1),
        action: manager_v1::IssueAdminAction::Recover as i32,
        ..Default::default()
    };
    let replay = ManagerConsumerService::apply_issue_admin_action(&server, Request::new(args))
        .await
        .unwrap()
        .into_inner();
    let mutation = match replay.outcome {
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(mutation)) => mutation,
        outcome => panic!("expected a recovered admin receipt: {outcome:?}"),
    };
    let Some(manager_v1::manager_mutation::Result::IssueAdminAction(receipt)) = mutation.result
    else {
        panic!("replay must contain a typed admin receipt");
    };
    assert_eq!(receipt.resulting_state, v1::IssueState::Open as i32);
    assert_eq!(receipt.resulting_revision, 2);
    assert!(receipt.changed);
    assert_eq!(receipt.previous_revision, 1);
}

#[tokio::test]
async fn pending_admin_recovery_commits_the_original_recover_receipt() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = app
        .core
        .tracker()
        .create("recover", "recover")
        .await
        .unwrap();
    let identity = app.manager_store_context().await.unwrap();
    let context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "recover-after-tracker-commit".into(),
        correlation_id: "test".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "maintenance".into(),
            actor_kind: v1::ActorKind::System as i32,
            reason: Some("unowned claim scan".into()),
            ..Default::default()
        }),
    };
    let args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        action: manager_v1::IssueAdminAction::Recover as i32,
        ..Default::default()
    };
    let scope = IdempotencyScope::new(
        context.contract_fingerprint.clone(),
        context.data_root_id.clone(),
        context.method.clone(),
        context.target.clone(),
        context.idempotency_key.clone(),
    )
    .unwrap();
    let digest = digest_bytes(&args.encode_to_vec());
    let operation_id = "admin-recover-after-tracker-commit".to_owned();
    app.admit_admin_action(
        &context,
        &scope,
        digest,
        operation_id.clone(),
        issue.id.to_string(),
        manager_v1::IssueAdminAction::Recover,
        issue.revision,
        args.encode_to_vec(),
    )
    .await
    .unwrap();
    app.execute_saved_admin_action(&args, issue.id, &operation_id)
        .await
        .unwrap();

    assert_eq!(app.recover_pending_admin_actions(1).await.unwrap(), 1);
    let operation = {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .repository()
            .get_operation(&operation_id)
            .unwrap()
            .unwrap()
    };
    assert_eq!(
        operation.state,
        manager_v1::DurableOperationState::Committed as i32
    );
    assert!(
        !operation.affects_ready,
        "a NO_CHANGE recovery receipt must not suppress issue readiness"
    );
    let Some(manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(snapshot)) =
        operation.terminal_snapshot
    else {
        panic!("recovery must persist a terminal result snapshot");
    };
    let result =
        manager_v1::ApplyIssueAdminActionResult::decode(snapshot.payload.as_slice()).unwrap();
    let Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(mutation)) =
        result.outcome
    else {
        panic!("expected a successful NO_CHANGE receipt");
    };
    let Some(manager_v1::manager_mutation::Result::IssueAdminAction(receipt)) = mutation.result
    else {
        panic!("expected a typed NO_CHANGE receipt");
    };
    assert_eq!(
        receipt.action,
        manager_v1::IssueAdminAction::Recover.as_str_name()
    );
    assert!(!receipt.changed);
    assert!(receipt.previous_state.is_none());

    // A retry allocates a fresh candidate operation ID, but the
    // idempotency reservation must replay the original operation and
    // immutable receipt rather than looking up that fresh ID.
    let retry = app
        .admit_admin_action(
            &context,
            &scope,
            digest_bytes(&args.encode_to_vec()),
            "fresh-retry-operation-id".into(),
            issue.id.to_string(),
            manager_v1::IssueAdminAction::Recover,
            issue.revision,
            args.encode_to_vec(),
        )
        .await
        .unwrap()
        .expect("same-key retry should replay the committed receipt");
    let Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(mutation)) =
        retry.outcome
    else {
        panic!("same-key retry must return the original success receipt");
    };
    assert_eq!(
        mutation
            .operation
            .as_ref()
            .map(|operation| operation.operation_id.as_str()),
        Some(operation_id.as_str())
    );
}

#[tokio::test]
async fn legacy_saved_human_cancel_recovery_preserves_actor_history_after_restart() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = app
        .core
        .tracker()
        .create("legacy saved cancel", "cancel after restart")
        .await
        .unwrap();
    let identity = app.manager_store_context().await.unwrap();
    let context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "legacy-human-cancel-recovery".into(),
        correlation_id: "human-cancel-recovery-correlation".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "human-cancel-recovery-actor".into(),
            actor_kind: v1::ActorKind::Human as i32,
            agent_session_id: Some("human-cancel-recovery-session".into()),
            reason: Some("cancel after a saved-request recovery".into()),
            ..Default::default()
        }),
    };
    let args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        action: manager_v1::IssueAdminAction::Cancel as i32,
        terminal_report: Some(
            manager_v1::apply_issue_admin_action_args::TerminalReport::Cancellation(
                v1::CancellationReport {
                    classification: v1::CancellationClass::Other as i32,
                },
            ),
        ),
        ..Default::default()
    };
    let scope = IdempotencyScope::new(
        context.contract_fingerprint.clone(),
        context.data_root_id.clone(),
        context.method.clone(),
        context.target.clone(),
        context.idempotency_key.clone(),
    )
    .unwrap();
    let saved_args = args.encode_to_vec();
    let request_digest = digest_bytes(&saved_args);
    let operation_id = "legacy-saved-human-cancel-recovery".to_owned();
    app.admit_admin_action(
        &context,
        &scope,
        request_digest.clone(),
        operation_id.clone(),
        issue.id.to_string(),
        manager_v1::IssueAdminAction::Cancel,
        issue.revision,
        saved_args.clone(),
    )
    .await
    .unwrap();

    // Model an older Manager row whose saved request was the original Args
    // protobuf and whose outbox/Tracker marker had not yet been written.
    {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .write(|connection, _state| {
                connection.execute(
                    "UPDATE manager_operation
                        SET request_snapshot = ?1,
                            request_snapshot_schema = ?2,
                            request_snapshot_digest = ?3
                      WHERE operation_id = ?4",
                    rusqlite::params![
                        saved_args,
                        "issue_tracker.manager.v1.ApplyIssueAdminActionArgs",
                        digest_bytes(&args.encode_to_vec()),
                        operation_id,
                    ],
                )?;
                connection.execute(
                    "DELETE FROM manager_outbox WHERE operation_id = ?1",
                    [&operation_id],
                )?;
                Ok(())
            })
            .unwrap();
    }
    assert!(matches!(
        app.core.tracker().get_operation_record(&operation_id).await,
        Err(TrackerError::NotFound(_))
    ));
    drop(app);

    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let reopened = IssueTrackerApplication::open(configured).await.unwrap();
    assert_eq!(reopened.recover_pending_admin_actions(1).await.unwrap(), 1);

    let history = reopened
        .core
        .tracker()
        .list_history(issue.id)
        .await
        .unwrap();
    let entry = history
        .iter()
        .find(|entry| entry.downstream_operation_id == operation_id)
        .expect("recovery must append canonical history for the saved request");
    let actor = entry.actor.as_ref().unwrap();
    let actor_ref = actor.actor.as_ref().unwrap();
    assert_eq!(actor_ref.actor_id, "human-cancel-recovery-actor");
    assert_eq!(actor_ref.kind, v1::ActorKind::Human as i32);
    assert_eq!(
        actor.agent_session_id.as_deref(),
        Some("human-cancel-recovery-session")
    );
    assert_eq!(entry.correlation_id, "human-cancel-recovery-correlation");
    assert_eq!(
        entry
            .state_change
            .as_ref()
            .and_then(|change| change.cancellation.as_ref()),
        Some(&v1::CancellationReport {
            classification: v1::CancellationClass::Other as i32,
        })
    );
    let operation = {
        let mut manager = reopened.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .repository()
            .get_operation(&operation_id)
            .unwrap()
            .unwrap()
    };
    assert_eq!(operation.request_digest, request_digest);
}

#[tokio::test]
async fn pending_admin_recovery_does_not_replace_a_revoke_receipt_with_a_later_claim() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = app.core.tracker().create("revoke", "revoke").await.unwrap();
    let claimed = app
        .core
        .tracker()
        .claim(issue.id, "worker", issue.revision, 30, "initial-claim")
        .await
        .unwrap();
    let identity = app.manager_store_context().await.unwrap();
    let context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "revoke-after-tracker-commit".into(),
        correlation_id: "test".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("worker was replaced".into()),
            ..Default::default()
        }),
    };
    let args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(claimed.revision),
        action: manager_v1::IssueAdminAction::Revoke as i32,
        ..Default::default()
    };
    let scope = IdempotencyScope::new(
        context.contract_fingerprint.clone(),
        context.data_root_id.clone(),
        context.method.clone(),
        context.target.clone(),
        context.idempotency_key.clone(),
    )
    .unwrap();
    let digest = digest_bytes(&args.encode_to_vec());
    let operation_id = "admin-revoke-after-tracker-commit".to_owned();
    app.admit_admin_action(
        &context,
        &scope,
        digest,
        operation_id.clone(),
        issue.id.to_string(),
        manager_v1::IssueAdminAction::Revoke,
        claimed.revision,
        args.encode_to_vec(),
    )
    .await
    .unwrap();
    let revoked = app
        .execute_saved_admin_action(&args, issue.id, &operation_id)
        .await
        .unwrap();
    let later_claim = app
        .core
        .tracker()
        .claim(issue.id, "replacement", revoked.revision, 30, "later-claim")
        .await
        .unwrap();

    assert_eq!(app.recover_pending_admin_actions(1).await.unwrap(), 1);
    let operation = {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .repository()
            .get_operation(&operation_id)
            .unwrap()
            .unwrap()
    };
    assert!(
        operation.affects_ready,
        "a changing revoke remains an in-flight ownership mutation until synchronization"
    );
    let Some(manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(snapshot)) =
        operation.terminal_snapshot
    else {
        panic!("recovery must preserve the revoke result");
    };
    let result =
        manager_v1::ApplyIssueAdminActionResult::decode(snapshot.payload.as_slice()).unwrap();
    let Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(mutation)) =
        result.outcome
    else {
        panic!("recovery must return a success receipt");
    };
    let Some(manager_v1::manager_mutation::Result::IssueAdminAction(receipt)) = mutation.result
    else {
        panic!("receipt must contain the administrative result");
    };
    assert_eq!(receipt.resulting_state, v1::IssueState::Open as i32);
    assert_eq!(receipt.resulting_revision, revoked.revision);
    assert_ne!(receipt.resulting_revision, later_claim.revision);
}

#[test]
fn issue_merge_source_evidence_is_target_first_and_deduplicated() {
    let target = v1::SourceEvidenceRef {
        thread_id: "thread-1".into(),
        memory_id: "target".into(),
    };
    let source = v1::SourceEvidenceRef {
        thread_id: "thread-2".into(),
        memory_id: "source".into(),
    };
    assert_eq!(
        union_source_evidence_refs(
            std::slice::from_ref(&target),
            &[target.clone(), source.clone()],
            manager::SourceEvidenceLimits {
                max_string_bytes: 128,
                max_repeated_item_count: 8,
            },
        )
        .expect("valid references"),
        vec![target, source]
    );
}

#[test]
fn confirm_source_evidence_uses_the_patch_or_existing_issue_before_candidate() {
    let existing = v1::SourceEvidenceRef {
        thread_id: "thread-existing".into(),
        memory_id: "memory-existing".into(),
    };
    let patch = v1::SourceEvidenceRef {
        thread_id: "thread-patch".into(),
        memory_id: "memory-patch".into(),
    };
    let candidate = v1::SourceEvidenceRef {
        thread_id: "thread-candidate".into(),
        memory_id: "memory-candidate".into(),
    };
    let limits = manager::SourceEvidenceLimits {
        max_string_bytes: 128,
        max_repeated_item_count: 8,
    };
    let without_patch = manager_v1::ConfirmAssociatedIssueResolution::default();
    assert_eq!(
        resolved_confirmation_source_evidence(
            &without_patch,
            std::slice::from_ref(&existing),
            std::slice::from_ref(&candidate),
            limits,
        )
        .expect("valid references"),
        vec![existing.clone(), candidate.clone()]
    );
    let with_patch = manager_v1::ConfirmAssociatedIssueResolution {
        update_mask: vec![manager_v1::ManagerIssueField::SourceEvidence as i32],
        patch: Some(manager_v1::ManagerIssuePatch {
            source_evidence: vec![patch.clone()],
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        resolved_confirmation_source_evidence(
            &with_patch,
            std::slice::from_ref(&existing),
            std::slice::from_ref(&candidate),
            limits,
        )
        .expect("valid references"),
        vec![patch, candidate]
    );
}

fn settings(root: &TempDir) -> ServerSettings {
    settings_for_path(root.path())
}

fn settings_for_path(root: &Path) -> ServerSettings {
    ServerSettings {
        server: ListenerSettings::default(),
        gateway_endpoints: vec![],
        tracker: v1::IssueTrackerRunnerSettings {
            data_root: root.to_string_lossy().into_owned(),
            expected_contract_fingerprint: contract_fingerprint(),
            expected_storage_schema_fingerprint: Tracker::schema_fingerprint(),
            lease_policy: Some(v1::LeasePolicy {
                default_lease: Some(v1::TimeSpan { millis: 30_000 }),
                minimum_lease: Some(v1::TimeSpan { millis: 10_000 }),
                maximum_lease: Some(v1::TimeSpan { millis: 60_000 }),
                maximum_heartbeat_interval: Some(v1::TimeSpan { millis: 1_000 }),
            }),
            pagination_policy: Some(v1::PaginationPolicy {
                default_page_size: 50,
                maximum_page_size: 100,
            }),
            secret_filter_mode: v1::SecretFilterMode::FailClosed as i32,
            resource_limits: Some(v1::ResourceLimits {
                max_string_bytes: 4096,
                max_body_bytes: 65_536,
                max_uri_bytes: 4096,
                max_token_bytes: 4096,
                max_handle_bytes: 4096,
                max_repeated_item_count: 100,
                max_export_bytes: 1_048_576,
                max_export_item_count: 10_000,
                max_artifact_bytes: 1_048_576,
                max_artifact_count: 100,
                max_total_count_scan_items: 10_000,
            }),
            manager_settings: None,
            ownership_maintenance_interval: None,
            ownership_recovery_batch_size: 0,
        },
        maintenance: MaintenanceSettings::default(),
    }
}

#[tokio::test]
async fn ownership_maintenance_recovers_an_expired_in_progress_issue() {
    let root = TempDir::new().unwrap();
    let app = IssueTrackerApplication::open(settings(&root))
        .await
        .unwrap();
    let issue = app
        .core
        .tracker()
        .create("expired", "expired")
        .await
        .unwrap();
    let claimed = app
        .core
        .tracker()
        .claim(issue.id, "worker", issue.revision, 0, "expired-claim")
        .await
        .unwrap();

    assert_eq!(app.run_ownership_maintenance().await.unwrap(), 1);
    let recovered = app.core.tracker().get(issue.id).await.unwrap();
    assert_eq!(recovered.state, crate::tracker::IssueState::Open);
    assert_eq!(recovered.revision, claimed.revision + 1);
    assert!(recovered.lease_expires_at.is_none());
}

#[tokio::test]
async fn ownership_recovery_index_survives_manager_reopen() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured.clone())
        .await
        .unwrap();
    let issue = app
        .core
        .tracker()
        .create("persistent-recovery", "persistent-recovery")
        .await
        .unwrap();
    app.core
        .tracker()
        .claim(issue.id, "worker", issue.revision, 0, "persistent-claim")
        .await
        .unwrap();

    assert_eq!(app.run_ownership_maintenance().await.unwrap(), 1);
    let state = app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .ownership_recovery_state(&issue.id.to_string())
        .unwrap()
        .expect("ownership recovery state");
    assert_eq!(state.readmission_number, 0);
    assert!(!state.in_flight);
    assert!(state.current_operation_id.is_none());
    drop(app);

    let reopened = IssueTrackerApplication::open(configured).await.unwrap();
    let restored = reopened
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .ownership_recovery_state(&issue.id.to_string())
        .unwrap()
        .expect("restored ownership recovery state");
    assert_eq!(restored, state);
}

#[tokio::test]
async fn ownership_maintenance_checkpoints_after_inconsistent_callback_failure() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = app
        .core
        .tracker()
        .create("inconsistent callback", "inconsistent callback")
        .await
        .unwrap();
    app.core
        .tracker()
        .set_user_defined_attribute_for_test(issue.id, "lb.lease_owner_hash", "not-a-lease-hash")
        .await
        .unwrap();
    let state = OwnershipRecoveryState {
        issue_id: issue.id.to_string(),
        expected_revision: issue.revision,
        readmission_number: 0,
        current_operation_id: Some("inconsistent-callback-operation".into()),
        in_flight: true,
        consecutive_failures: 0,
        next_attempt_at_millis: None,
        last_failure_class: None,
        stopped: false,
        stop_basis: None,
        repair_condition: None,
        cursor_diagnostic: None,
    };
    app.manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&state)
        .unwrap();

    let manager = app.manager.as_ref().unwrap().clone();
    let tracker = app.core.tracker().clone();
    let stats = tokio::time::timeout(
        Duration::from_secs(1),
        crate::tracker::ownership_maintenance::run_with_recovery_stats(
            &tracker,
            Some(&manager),
            1,
            &|_issue_id, _revision, _operation_id, _state, _reason| async {
                Err(TrackerError::LeaseInconsistent {
                    invalid_attributes: vec!["owner".into()],
                    lease_expires_at: None,
                    observed_revision: Some(issue.revision),
                })
            },
        ),
    )
    .await
    .expect("inconsistent recovery must not deadlock while checkpointing")
    .unwrap();
    assert!(stats.failed);
    assert_eq!(stats.diagnoses, 1);
    let cursor = manager
        .lock()
        .await
        .storage_mut()
        .repository()
        .ownership_maintenance_cursor()
        .unwrap();
    assert_eq!(cursor, Some(issue.id.to_string()));
}

#[tokio::test]
async fn ownership_maintenance_retries_a_pre_send_admission_after_restart() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = app
        .core
        .tracker()
        .create("pre-send recovery", "pre-send recovery")
        .await
        .unwrap();
    let claimed = app
        .core
        .tracker()
        .claim(issue.id, "worker", issue.revision, 0, "pre-send-claim")
        .await
        .unwrap();
    let state = OwnershipRecoveryState {
        issue_id: issue.id.to_string(),
        expected_revision: claimed.revision,
        readmission_number: 0,
        current_operation_id: Some("pre-send-operation".into()),
        in_flight: true,
        consecutive_failures: 0,
        next_attempt_at_millis: None,
        last_failure_class: None,
        stopped: false,
        stop_basis: None,
        repair_condition: None,
        cursor_diagnostic: None,
    };
    app.manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&state)
        .unwrap();

    assert_eq!(app.run_ownership_maintenance().await.unwrap(), 1);
    assert_eq!(
        app.core.tracker().get(issue.id).await.unwrap().state,
        crate::tracker::IssueState::Open
    );
    let state = app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .ownership_recovery_state(&issue.id.to_string())
        .unwrap()
        .unwrap();
    assert!(!state.in_flight);
    assert!(state.current_operation_id.is_none());
}

#[tokio::test]
async fn capability_send_interruption_becomes_resolution_required_after_recovery() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let context = app.manager_store_context().await.unwrap();
    let args = manager_v1::HeartbeatIssueClaimArgs {
        context: Some(manager_v1::ManagerWriteContext {
            method: "manager_heartbeat_issue_claim".into(),
            target: "issue:capability-recovery".into(),
            schema_fingerprint: context.schema_fingerprint,
            data_root_id: context.data_root_id,
            contract_fingerprint: context.contract_fingerprint,
            idempotency_key: "capability-recovery-interrupted".into(),
            correlation_id: "capability-recovery-interrupted".into(),
            actor_audit: Some(manager_v1::ActorAudit {
                actor_id: "worker".into(),
                actor_kind: v1::ActorKind::ExternalAgent as i32,
                ..Default::default()
            }),
        }),
        issue_id: "capability-recovery".into(),
        expected_revision: Some(0),
        claim_revision: Some(0),
        lease_handle: b"ephemeral-recovery-handle".to_vec(),
        requested_extension: Some(manager_v1::TimeSpan { millis: 1_000 }),
    };
    let prepared = {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        match manager
            .prepare_lease_capability("manager_heartbeat_issue_claim", &args.encode_to_vec(), 100)
            .unwrap()
        {
            manager::LeaseCapabilityPreparation::Dispatch(prepared) => prepared,
            manager::LeaseCapabilityPreparation::Immediate(_) => {
                panic!("capability admission unexpectedly rejected")
            }
        }
    };
    let operation_id = prepared.operation.operation_id.clone();
    {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .repository()
            .mark_capability_attempt_sending(&operation_id, prepared.operation.generation, 101)
            .unwrap();
    }
    drop(prepared);

    let recovered = app
        .driver
        .as_ref()
        .unwrap()
        .recover_inflight_capability_dispatches(100)
        .await
        .unwrap();
    assert_eq!(recovered, 1);
    let operation = app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(&operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.state,
        manager_v1::DurableOperationState::Unconfirmed as i32
    );
    assert!(operation.resolution_required);
    assert!(operation.terminal_snapshot.is_none());
}

#[tokio::test]
async fn ownership_maintenance_cursor_reaches_valid_issue_after_full_invalid_page() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    configured.tracker.ownership_recovery_batch_size = 100;
    let app = IssueTrackerApplication::open(configured).await.unwrap();

    // Use ordered UUIDs so the first physical page is deliberately filled
    // with malformed rows and the recoverable Issue is just beyond it.
    for value in 1..=101_u128 {
        app.core
            .tracker()
            .create_with_id(
                uuid::Uuid::from_u128(value),
                &format!("maintenance-seed-{value}"),
                &format!("maintenance seed {value}"),
            )
            .await
            .unwrap();
    }
    for value in 1..=100_u128 {
        app.core
            .tracker()
            .set_user_defined_attribute_for_test(
                uuid::Uuid::from_u128(value),
                "lb.state",
                "not-a-state",
            )
            .await
            .unwrap();
    }
    let target = app
        .core
        .tracker()
        .get(uuid::Uuid::from_u128(101))
        .await
        .unwrap();
    let claimed = app
        .core
        .tracker()
        .claim(target.id, "worker", target.revision, 0, "cursor-target")
        .await
        .unwrap();

    let first = app.run_ownership_maintenance_stats().await.unwrap();
    assert_eq!(first.scanned, 100);
    assert_eq!(
        first.diagnoses, 100,
        "unexpected first maintenance page: {first:?}"
    );
    assert_eq!(first.recovered, 0);
    let still_pending = app.core.tracker().get(target.id).await.unwrap();
    assert_eq!(still_pending.revision, claimed.revision);
    assert_eq!(still_pending.state, crate::tracker::IssueState::InProgress);

    let second = app.run_ownership_maintenance_stats().await.unwrap();
    assert!(second.scanned >= 1);
    assert_eq!(second.recovered, 1);
    let recovered = app.core.tracker().get(target.id).await.unwrap();
    assert_eq!(recovered.state, crate::tracker::IssueState::Open);
    assert_eq!(recovered.revision, claimed.revision + 1);
}

#[tokio::test]
async fn ownership_maintenance_health_waits_for_the_initial_full_scan() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    configured.tracker.ownership_recovery_batch_size = 2;
    let app = Arc::new(IssueTrackerApplication::open(configured).await.unwrap());
    for value in 1..=4_u128 {
        app.core
            .tracker()
            .create_with_id(
                uuid::Uuid::from_u128(value),
                &format!("scan-health-{value}"),
                "scan health",
            )
            .await
            .unwrap();
    }
    app.run_outbox_cycle().await.unwrap();
    {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        let repository = manager.storage_mut().repository();
        assert!(!repository.ownership_maintenance_scan_complete().unwrap());
        assert!(!repository.ownership_maintenance_healthy().unwrap());
    }

    app.run_outbox_cycle().await.unwrap();
    {
        let mut manager = app.manager.as_ref().unwrap().lock().await;
        let repository = manager.storage_mut().repository();
        assert!(!repository.ownership_maintenance_scan_complete().unwrap());
        assert!(!repository.ownership_maintenance_healthy().unwrap());
    }

    app.run_outbox_cycle().await.unwrap();
    let mut manager = app.manager.as_ref().unwrap().lock().await;
    let repository = manager.storage_mut().repository();
    assert!(repository.ownership_maintenance_scan_complete().unwrap());
    assert!(repository.ownership_maintenance_healthy().unwrap());
}

fn manager_settings() -> manager_v1::IssueTrackerManagerSettings {
    manager_v1::IssueTrackerManagerSettings {
        default_page_size: 10,
        maximum_page_size: 20,
        maximum_payload_bytes: 65_536,
        maximum_repeated_item_count: 100,
        operation_lease: Some(manager_v1::TimeSpan { millis: 10_000 }),
        outbox_lease: Some(manager_v1::TimeSpan { millis: 10_000 }),
        resource_limits: Some(manager_v1::ManagerResourceLimits {
            max_string_bytes: 4096,
            max_body_bytes: 65_536,
            max_uri_bytes: 4096,
            max_repeated_item_count: 100,
            max_page_size: 100,
        }),
        retry_budget: Some(manager_v1::RetryBudget {
            max_attempts: 3,
            base_backoff: Some(manager_v1::TimeSpan { millis: 100 }),
            max_backoff: Some(manager_v1::TimeSpan { millis: 1_000 }),
        }),
        secret_filter_mode: manager_v1::SecretFilterMode::FailClosed as i32,
        ..Default::default()
    }
}

#[test]
fn configured_non_loopback_listener_is_accepted() {
    let settings = ServerSettings {
        server: ListenerSettings {
            listen: "0.0.0.0:50051".into(),
        },
        gateway_endpoints: vec![],
        tracker: v1::IssueTrackerRunnerSettings::default(),
        maintenance: MaintenanceSettings::default(),
    };

    assert_eq!(settings.listen_addr().unwrap().to_string(), "0.0.0.0:50051");
}

#[test]
fn ownership_maintenance_batch_size_is_bounded_to_one_hundred() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.ownership_recovery_batch_size = 101;
    let error = match validate_runner_settings(&configured.tracker) {
        Ok(_) => panic!("an ownership recovery batch over the limit must be rejected"),
        Err(error) => error,
    };
    assert!(error.contains("ownership recovery batch size"));

    configured.tracker.ownership_recovery_batch_size = 100;
    assert!(validate_runner_settings(&configured.tracker).is_ok());

    configured.tracker.ownership_maintenance_interval = Some(v1::TimeSpan { millis: 0 });
    let runner = validate_runner_settings(&configured.tracker).unwrap();
    assert_eq!(runner.ownership_maintenance_interval_millis, 30_000);
    let infra_runner =
        crate::tracker::config::RunnerConfig::from_settings(&configured.tracker).unwrap();
    assert_eq!(infra_runner.ownership_maintenance_interval_millis, 30_000);
}

#[test]
fn runner_maintenance_interval_preserves_millisecond_precision() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.ownership_maintenance_interval = Some(v1::TimeSpan { millis: 1_500 });

    let runner = validate_runner_settings(&configured.tracker).unwrap();
    let infra_runner =
        crate::tracker::config::RunnerConfig::from_settings(&configured.tracker).unwrap();

    assert_eq!(runner.ownership_maintenance_interval_millis, 1_500);
    assert_eq!(infra_runner.ownership_maintenance_interval_millis, 1_500);
}

#[tokio::test]
async fn grpc_server_uses_tracker_maintenance_settings() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.ownership_maintenance_interval = Some(v1::TimeSpan { millis: 1_500 });
    configured.tracker.ownership_recovery_batch_size = 7;

    let app = IssueTrackerApplication::open(configured).await.unwrap();

    assert_eq!(app.ownership_maintenance_interval_millis, 1_500);
    assert_eq!(app.ownership_recovery_batch_size, 7);
}

#[test]
fn gateway_endpoint_settings_accept_distinct_gateway_ids_and_addresses() {
    let mut settings = ServerSettings {
        server: ListenerSettings {
            listen: "127.0.0.1:50051".into(),
        },
        tracker: v1::IssueTrackerRunnerSettings::default(),
        maintenance: MaintenanceSettings::default(),
        gateway_endpoints: vec![
            GatewayEndpointSettings {
                gateway_id: "gateway-a".into(),
                control_listen: "127.0.0.1:50052".into(),
                worker_listen: "127.0.0.1:50053".into(),
            },
            GatewayEndpointSettings {
                gateway_id: "gateway-b".into(),
                control_listen: "127.0.0.1:50054".into(),
                worker_listen: "127.0.0.1:50055".into(),
            },
        ],
    };

    assert!(settings.validate().is_ok());
    assert_eq!(
        settings
            .gateway_control_addr(&settings.gateway_endpoints[0])
            .unwrap(),
        "127.0.0.1:50052".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(
        settings
            .gateway_worker_addr(&settings.gateway_endpoints[1])
            .unwrap(),
        "127.0.0.1:50055".parse::<SocketAddr>().unwrap()
    );
    settings.gateway_endpoints.clear();
    assert!(settings.validate().is_ok());
}

#[test]
fn resolved_gateway_endpoints_use_the_validated_listener_addresses() {
    let settings = ServerSettings {
        server: ListenerSettings {
            listen: "127.0.0.1:50051".into(),
        },
        tracker: v1::IssueTrackerRunnerSettings::default(),
        maintenance: MaintenanceSettings::default(),
        gateway_endpoints: vec![GatewayEndpointSettings {
            gateway_id: "gateway-a".into(),
            control_listen: "127.0.0.1:50052".into(),
            worker_listen: "127.0.0.1:50053".into(),
        }],
    };

    assert_eq!(
        settings.resolved_gateway_endpoints().unwrap(),
        vec![ResolvedGatewayEndpoint {
            gateway_id: "gateway-a".into(),
            control_address: "127.0.0.1:50052".parse().unwrap(),
            worker_address: "127.0.0.1:50053".parse().unwrap(),
        }]
    );
}

#[test]
fn gateway_endpoint_settings_reject_empty_or_duplicate_gateway_id() {
    let mut settings = ServerSettings {
        server: ListenerSettings::default(),
        tracker: v1::IssueTrackerRunnerSettings::default(),
        maintenance: MaintenanceSettings::default(),
        gateway_endpoints: vec![GatewayEndpointSettings {
            gateway_id: " ".into(),
            control_listen: "127.0.0.1:50052".into(),
            worker_listen: "127.0.0.1:50053".into(),
        }],
    };
    assert!(settings.validate().unwrap_err().contains("gateway_id"));

    settings.gateway_endpoints[0].gateway_id = "gateway-a".into();
    settings.gateway_endpoints.push(GatewayEndpointSettings {
        gateway_id: "gateway-a".into(),
        control_listen: "127.0.0.1:50054".into(),
        worker_listen: "127.0.0.1:50055".into(),
    });
    assert!(
        settings
            .validate()
            .unwrap_err()
            .contains("duplicate gateway_id")
    );
}

#[test]
fn gateway_endpoint_settings_reject_invalid_or_reused_listener_address() {
    let mut settings = ServerSettings {
        server: ListenerSettings {
            listen: "127.0.0.1:50051".into(),
        },
        tracker: v1::IssueTrackerRunnerSettings::default(),
        maintenance: MaintenanceSettings::default(),
        gateway_endpoints: vec![GatewayEndpointSettings {
            gateway_id: "gateway-a".into(),
            control_listen: "not-an-address".into(),
            worker_listen: "127.0.0.1:50053".into(),
        }],
    };
    assert!(settings.validate().unwrap_err().contains("control_listen"));

    settings.gateway_endpoints[0].control_listen = "127.0.0.1:50052".into();
    settings.gateway_endpoints[0].worker_listen = "also-not-an-address".into();
    assert!(settings.validate().unwrap_err().contains("worker_listen"));

    settings.gateway_endpoints[0].worker_listen = "127.0.0.1:50052".into();
    assert!(
        settings
            .validate()
            .unwrap_err()
            .contains("duplicate listener address")
    );

    settings.gateway_endpoints[0].worker_listen = "127.0.0.1:50053".into();
    settings.gateway_endpoints.push(GatewayEndpointSettings {
        gateway_id: "gateway-b".into(),
        control_listen: "127.0.0.1:50053".into(),
        worker_listen: "127.0.0.1:50054".into(),
    });
    assert!(
        settings
            .validate()
            .unwrap_err()
            .contains("duplicate listener address")
    );

    settings.gateway_endpoints.pop();
    settings.gateway_endpoints[0].worker_listen = "127.0.0.1:50051".into();
    assert!(
        settings
            .validate()
            .unwrap_err()
            .contains("duplicate listener address")
    );
}

#[tokio::test]
async fn server_resident_maintenance_joins_before_reopening_the_root() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    configured.tracker.ownership_maintenance_interval = Some(v1::TimeSpan { millis: 1_000 });
    let app = IssueTrackerApplication::open(configured.clone())
        .await
        .unwrap();
    let server = GrpcServer::new(app);
    let (stop, task) = spawn_server_resident_maintenance(server.clone());

    tokio::time::sleep(Duration::from_millis(20)).await;
    stop.send(()).unwrap();
    task.await.unwrap();
    drop(server);

    let reopened = IssueTrackerApplication::open(configured).await.unwrap();
    drop(reopened);
}

#[tokio::test]
async fn server_resident_maintenance_panic_is_reported_as_a_join_failure() {
    let task = tokio::spawn(async {
        panic!("maintenance panic");
    });
    let error = maintenance_task_join_result(task.await)
        .expect_err("a panicked maintenance task must be reported");
    assert!(error.contains("maintenance panic"));
}

#[tokio::test]
async fn resident_maintenance_failure_marks_manager_health_degraded() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();

    app.mark_maintenance_unhealthy().await;

    let manager = app.manager.as_ref().expect("manager configuration");
    let mut manager = manager.lock().await;
    assert!(
        !manager
            .storage_mut()
            .repository()
            .ownership_maintenance_healthy()
            .unwrap()
    );
}

#[tokio::test]
async fn grpc_v1_contract_info_uses_the_existing_typed_handler() {
    let root = TempDir::new().unwrap();
    let server = GrpcServer::new(
        IssueTrackerApplication::open(settings(&root))
            .await
            .unwrap(),
    );

    let response = IssueTrackerService::get_contract_info(
        &server,
        Request::new(v1::GetContractInfoArgs::default()),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(matches!(
        response.outcome,
        Some(v1::get_contract_info_result::Outcome::Success(_))
    ));
}

#[tokio::test]
async fn outbox_maintenance_reports_an_empty_cycle_with_its_contract() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let application = IssueTrackerApplication::open(configured).await.unwrap();
    let server = GrpcServer::new(application);

    let response = OutboxMaintenanceService::run_outbox_cycle(
        &server,
        Request::new(rpc_v1::RunOutboxCycleArgs::default()),
    )
    .await
    .unwrap()
    .into_inner();

    let outcome = match response.outcome {
        Some(rpc_v1::run_outbox_cycle_result::Outcome::Success(outcome)) => outcome,
        outcome => panic!("outbox maintenance must return success: {outcome:?}"),
    };
    assert_eq!(outcome.recovered_operations, 0);
    assert_eq!(outcome.delivery_attempts, 0);
    assert_eq!(outcome.confirmed_operations, 0);
    assert!(!outcome.delivery_limit_reached);
    assert!(outcome.contract.is_some());
}

#[tokio::test]
async fn gateway_server_binds_its_identity_outside_worker_requests() {
    let root = TempDir::new().unwrap();
    let server = GrpcServer::new(
        IssueTrackerApplication::open(settings(&root))
            .await
            .unwrap(),
    );

    let gateway = server.for_gateway(Arc::<str>::from("gateway-a"));

    assert_eq!(server.gateway_id(), None);
    assert_eq!(gateway.gateway_id(), Some("gateway-a"));
}

#[tokio::test]
async fn gateway_control_rejects_an_unbound_server() {
    let root = TempDir::new().unwrap();
    let server = GrpcServer::new(
        IssueTrackerApplication::open(settings(&root))
            .await
            .unwrap(),
    );

    let error = GatewayControlService::get_gateway_registration(
        &server,
        Request::new(manager_v1::GetGatewayRegistrationArgs::default()),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn gateway_control_routes_a_bound_endpoint_to_the_manager() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let application = IssueTrackerApplication::open(configured).await.unwrap();
    let server = GrpcServer::new(application);
    let gateway = server.for_gateway(Arc::<str>::from("gateway-a"));
    let context = gateway.app.manager_store_context().await.unwrap();

    let response = GatewayControlService::get_gateway_registration(
        &gateway,
        Request::new(manager_v1::GetGatewayRegistrationArgs {
            context: Some(context),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.outcome.is_some());
}

#[test]
fn gateway_ready_page_paginates_the_policy_selected_issues() {
    let issues = ["issue-a", "issue-b", "issue-c"]
        .into_iter()
        .map(|issue_id| v1::Issue {
            issue_id: issue_id.into(),
            ..Default::default()
        })
        .collect();
    let selection = manager::repository::GatewayReadySelection {
        registration: manager_v1::GatewayRegistration {
            gateway_id: "gateway-a".into(),
            policy_fingerprint: "policy-a".into(),
            registration_generation: 3,
            ..Default::default()
        },
        eligibility_evaluated_at_millis: 100,
        issues,
    };
    let args = manager_v1::ListIssuesWithSyncArgs {
        page: Some(manager_v1::PageRequest {
            page_size: 2,
            ..Default::default()
        }),
        ..Default::default()
    };

    let first = manager_v1::ListIssuesWithSyncResult::decode(
        gateway_ready_page(
            &args,
            selection.clone(),
            "secret".into(),
            "gateway-a",
            GatewayReadyPageBinding::new(7, 11, &selection),
        )
        .as_slice(),
    )
    .unwrap();
    let page = match first.outcome.unwrap() {
        manager_v1::list_issues_with_sync_result::Outcome::Success(page) => page,
        manager_v1::list_issues_with_sync_result::Outcome::Error(error) => panic!("{error:?}"),
    };
    assert_eq!(page.issues.len(), 2);
    assert_eq!(page.page.as_ref().and_then(|page| page.total_size), Some(3));
    let token = page
        .page
        .and_then(|page| page.next_page_token)
        .expect("continuation token");

    let second = manager_v1::ListIssuesWithSyncResult::decode(
        gateway_ready_page(
            &manager_v1::ListIssuesWithSyncArgs {
                page: Some(manager_v1::PageRequest {
                    page_size: 2,
                    page_token: Some(token),
                }),
                ..Default::default()
            },
            manager::repository::GatewayReadySelection {
                registration: manager_v1::GatewayRegistration {
                    gateway_id: "gateway-a".into(),
                    policy_fingerprint: "policy-a".into(),
                    registration_generation: 3,
                    ..Default::default()
                },
                eligibility_evaluated_at_millis: 100,
                issues: ["issue-a", "issue-b", "issue-c"]
                    .into_iter()
                    .map(|issue_id| v1::Issue {
                        issue_id: issue_id.into(),
                        ..Default::default()
                    })
                    .collect(),
            },
            "secret".into(),
            "gateway-a",
            GatewayReadyPageBinding::new(
                7,
                11,
                &manager::repository::GatewayReadySelection {
                    registration: manager_v1::GatewayRegistration {
                        gateway_id: "gateway-a".into(),
                        policy_fingerprint: "policy-a".into(),
                        registration_generation: 3,
                        ..Default::default()
                    },
                    eligibility_evaluated_at_millis: 100,
                    issues: Vec::new(),
                },
            ),
        )
        .as_slice(),
    )
    .unwrap();
    let page = match second.outcome.unwrap() {
        manager_v1::list_issues_with_sync_result::Outcome::Success(page) => page,
        manager_v1::list_issues_with_sync_result::Outcome::Error(error) => panic!("{error:?}"),
    };
    assert_eq!(page.issues.len(), 1);
    assert!(page.page.and_then(|page| page.next_page_token).is_none());
}

#[test]
fn gateway_ready_page_uses_the_default_and_enforces_the_maximum() {
    let selection = manager::repository::GatewayReadySelection {
        registration: manager_v1::GatewayRegistration {
            gateway_id: "gateway-a".into(),
            policy_fingerprint: "policy-a".into(),
            registration_generation: 3,
            ..Default::default()
        },
        eligibility_evaluated_at_millis: 100,
        issues: ["issue-a", "issue-b", "issue-c"]
            .into_iter()
            .map(|issue_id| v1::Issue {
                issue_id: issue_id.into(),
                ..Default::default()
            })
            .collect(),
    };
    let limits = manager::pagination::PageSizeLimits::new(1, 2);
    let page = |page_size| {
        let result = manager_v1::ListIssuesWithSyncResult::decode(
            gateway_ready_page_with_limits(
                &manager_v1::ListIssuesWithSyncArgs {
                    page: Some(manager_v1::PageRequest {
                        page_size,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                selection.clone(),
                "secret".into(),
                "gateway-a",
                GatewayReadyPageBinding::new(7, 11, &selection),
                limits,
            )
            .as_slice(),
        )
        .expect("decode response");
        match result.outcome.expect("response outcome") {
            manager_v1::list_issues_with_sync_result::Outcome::Success(page) => page,
            manager_v1::list_issues_with_sync_result::Outcome::Error(error) => {
                panic!("unexpected error: {error:?}")
            }
        }
    };

    let default_page = page(0);
    assert_eq!(default_page.issues.len(), 1);
    assert!(
        default_page
            .page
            .as_ref()
            .and_then(|page| page.next_page_token.as_ref())
            .is_some()
    );
    let capped_page = page(u32::MAX);
    assert_eq!(capped_page.issues.len(), 2);
    assert!(
        capped_page
            .page
            .as_ref()
            .and_then(|page| page.next_page_token.as_ref())
            .is_some()
    );
}

#[tokio::test]
async fn gateway_ready_dispatch_uses_configured_page_size_limits() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    let mut manager_settings = manager_settings();
    manager_settings.default_page_size = 1;
    manager_settings.maximum_page_size = 2;
    configured.tracker.manager_settings = Some(manager_settings);
    let application = IssueTrackerApplication::open(configured).await.unwrap();

    for index in 0..3 {
        let idempotency_key = format!("gateway-ready-page-{index}");
        application
            .core
            .tracker()
            .create_with_draft(
                uuid::Uuid::new_v4(),
                &idempotency_key,
                &v1::IssueDraft {
                    title: format!("ready issue {index}"),
                    kind: v1::IssueKind::Task as i32,
                    priority: v1::IssuePriority::P2 as i32,
                    creation_basis: v1::CreationBasis::Explicit as i32,
                    confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                    ..Default::default()
                },
                "test",
                &idempotency_key,
            )
            .await
            .unwrap();
    }
    application
        .manager
        .as_ref()
        .expect("manager is configured")
        .lock()
        .await
        .storage_mut()
        .repository()
        .register_gateway_policy(
            "gateway-a",
            "gateway-a",
            "gateway-ready-page-registration",
            &manager_v1::WorkEligibilityPolicy {
                mode: manager_v1::WorkEligibilityMode::ReviewedOnly as i32,
            },
        )
        .unwrap();

    for (page_size, expected_len) in [(0, 1), (u32::MAX, 2)] {
        let result: manager_v1::ListIssuesWithSyncResult = application
            .dispatch_gateway_ready_list(
                "gateway-a",
                manager_v1::ListIssuesWithSyncArgs {
                    page: Some(manager_v1::PageRequest {
                        page_size,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let page = match result.outcome.expect("response outcome") {
            manager_v1::list_issues_with_sync_result::Outcome::Success(page) => page,
            manager_v1::list_issues_with_sync_result::Outcome::Error(error) => {
                panic!("unexpected error: {error:?}")
            }
        };
        assert_eq!(page.issues.len(), expected_len);
        assert!(
            page.page
                .as_ref()
                .and_then(|page| page.next_page_token.as_ref())
                .is_some()
        );
    }
}

#[test]
fn gateway_ready_snapshot_preserves_the_existing_wire_format() {
    let snapshot = GatewayReadySnapshot {
        manager_generation: 7,
        tracker_generation: 11,
        eligibility_evaluated_at_millis: 100,
    };

    assert_eq!(snapshot.encode(), "gateway-ready-7-11-100");
    assert!(matches!(
        GatewayReadySnapshot::decode(&snapshot.encode()),
        Ok(decoded) if decoded == snapshot
    ));
    assert!(matches!(
        GatewayReadySnapshot::decode("gateway-ready-7-11-not-a-time"),
        Err(TrackerError::InvalidPageToken)
    ));
    assert!(matches!(
        GatewayReadySnapshot::decode("gateway-ready-7-11-100-extra"),
        Err(TrackerError::InvalidPageToken)
    ));
}

#[test]
fn gateway_ready_page_expires_when_policy_or_evidence_age_changes() {
    let selection = manager::repository::GatewayReadySelection {
        registration: manager_v1::GatewayRegistration {
            gateway_id: "gateway-a".into(),
            policy_fingerprint: "policy-a".into(),
            registration_generation: 3,
            ..Default::default()
        },
        eligibility_evaluated_at_millis: 100,
        issues: ["issue-a", "issue-b"]
            .into_iter()
            .map(|issue_id| v1::Issue {
                issue_id: issue_id.into(),
                ..Default::default()
            })
            .collect(),
    };
    let first_args = manager_v1::ListIssuesWithSyncArgs {
        page: Some(manager_v1::PageRequest {
            page_size: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let first = manager_v1::ListIssuesWithSyncResult::decode(
        gateway_ready_page(
            &first_args,
            selection.clone(),
            "secret".into(),
            "gateway-a",
            GatewayReadyPageBinding::new(7, 11, &selection),
        )
        .as_slice(),
    )
    .unwrap();
    let token = match first.outcome.unwrap() {
        manager_v1::list_issues_with_sync_result::Outcome::Success(page) => page
            .page
            .and_then(|page| page.next_page_token)
            .expect("continuation token"),
        manager_v1::list_issues_with_sync_result::Outcome::Error(error) => panic!("{error:?}"),
    };
    let continuation = manager_v1::ListIssuesWithSyncArgs {
        page: Some(manager_v1::PageRequest {
            page_size: 1,
            page_token: Some(token.clone()),
        }),
        ..Default::default()
    };
    let binding_result = manager_v1::ListIssuesWithSyncResult::decode(
        gateway_ready_page(
            &continuation,
            selection.clone(),
            "secret".into(),
            "gateway-b",
            GatewayReadyPageBinding::new(7, 11, &selection),
        )
        .as_slice(),
    )
    .unwrap();
    assert!(matches!(
        binding_result.outcome,
        Some(manager_v1::list_issues_with_sync_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::PageTokenExpired as i32
    ));

    let mut changed_policy = selection.clone();
    changed_policy.registration.policy_fingerprint = "policy-b".into();
    let policy_result = manager_v1::ListIssuesWithSyncResult::decode(
        gateway_ready_page(
            &continuation,
            changed_policy.clone(),
            "secret".into(),
            "gateway-a",
            GatewayReadyPageBinding::new(7, 11, &changed_policy),
        )
        .as_slice(),
    )
    .unwrap();
    let policy_error = match policy_result.outcome {
        Some(manager_v1::list_issues_with_sync_result::Outcome::Error(error)) => error,
        outcome => panic!("expected page-token error, got {outcome:?}"),
    };
    assert_eq!(
        policy_error.code,
        manager_v1::ManagerErrorCode::PageTokenExpired as i32
    );

    let mut changed_registration = selection.clone();
    changed_registration.registration.registration_generation = 4;
    let registration_result = manager_v1::ListIssuesWithSyncResult::decode(
        gateway_ready_page(
            &continuation,
            changed_registration.clone(),
            "secret".into(),
            "gateway-a",
            GatewayReadyPageBinding::new(7, 11, &changed_registration),
        )
        .as_slice(),
    )
    .unwrap();
    assert!(matches!(
        registration_result.outcome,
        Some(manager_v1::list_issues_with_sync_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::PageTokenExpired as i32
    ));

    let age_result = manager_v1::ListIssuesWithSyncResult::decode(
        gateway_ready_page(
            &continuation,
            selection.clone(),
            "secret".into(),
            "gateway-a",
            GatewayReadyPageBinding::new(7, 11, &selection),
        )
        .as_slice(),
    )
    .unwrap();
    assert!(matches!(
        age_result.outcome,
        Some(manager_v1::list_issues_with_sync_result::Outcome::Success(
            _
        ))
    ));

    let mut changed_evaluation_time = selection.clone();
    changed_evaluation_time.eligibility_evaluated_at_millis = 101;
    let time_result = manager_v1::ListIssuesWithSyncResult::decode(
        gateway_ready_page(
            &continuation,
            changed_evaluation_time.clone(),
            "secret".into(),
            "gateway-a",
            GatewayReadyPageBinding::new(7, 11, &changed_evaluation_time),
        )
        .as_slice(),
    )
    .unwrap();
    assert!(matches!(
        time_result.outcome,
        Some(manager_v1::list_issues_with_sync_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::PageTokenExpired as i32
    ));
}

#[tokio::test]
async fn gateway_claim_rechecks_policy_before_canonical_claim() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let application = IssueTrackerApplication::open(configured).await.unwrap();
    let issue_id = uuid::Uuid::new_v4();
    let created = application
        .core
        .tracker()
        .create_with_draft(
            issue_id,
            "create-gateway-policy-test",
            &v1::IssueDraft {
                title: "policy test".into(),
                kind: v1::IssueKind::Task as i32,
                priority: v1::IssuePriority::P2 as i32,
                creation_basis: v1::CreationBasis::Explicit as i32,
                confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                ..Default::default()
            },
            "test",
            "gateway-policy-test",
        )
        .await
        .unwrap();

    let result: manager_v1::ClaimIssueResult = application
        .dispatch_manager_for_gateway(
            "unregistered-gateway",
            "manager_gateway_claim_issue",
            manager_v1::ClaimIssueArgs {
                issue_id: issue_id.to_string(),
                expected_revision: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(matches!(
        result.outcome,
        Some(manager_v1::claim_issue_result::Outcome::Error(error))
            if error.message.contains("not registered")
    ));

    application
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .register_gateway_policy(
            "unregistered-gateway",
            "unregistered-gateway",
            "test-register-unregistered-gateway",
            &manager_v1::WorkEligibilityPolicy {
                mode: manager_v1::WorkEligibilityMode::ReviewedOnly as i32,
            },
        )
        .unwrap();
    let read_context = application.manager_store_context().await.unwrap();
    let claimed: manager_v1::ClaimIssueResult = application
        .dispatch_manager_for_gateway(
            "unregistered-gateway",
            "manager_gateway_claim_issue",
            manager_v1::ClaimIssueArgs {
                context: Some(manager_v1::ManagerWriteContext {
                    method: "manager_gateway_claim_issue".into(),
                    target: format!("issue:{issue_id}"),
                    schema_fingerprint: read_context.schema_fingerprint,
                    data_root_id: read_context.data_root_id,
                    contract_fingerprint: read_context.contract_fingerprint,
                    idempotency_key: "gateway-policy-claim".into(),
                    correlation_id: "gateway-policy-claim".into(),
                    actor_audit: Some(manager_v1::ActorAudit {
                        actor_id: "gateway-worker".into(),
                        actor_kind: v1::ActorKind::ExternalAgent as i32,
                        display_name: None,
                        agent_session_id: None,
                        reason: None,
                    }),
                }),
                issue_id: issue_id.to_string(),
                expected_revision: Some(created.revision),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claim = match claimed.outcome {
        Some(manager_v1::claim_issue_result::Outcome::Success(mutation)) => match mutation.result {
            Some(manager_v1::manager_mutation::Result::ClaimIssue(claim)) => claim,
            outcome => panic!("gateway claim must return a lease capability: {outcome:?}"),
        },
        outcome => panic!("gateway claim must reach canonical claim: {outcome:?}"),
    };
    let current = claim.current.expect("claimed issue");
    let heartbeat_context = application.manager_store_context().await.unwrap();
    let heartbeat: manager_v1::HeartbeatIssueClaimResult = application
        .dispatch_manager_for_gateway(
            "unregistered-gateway",
            "manager_heartbeat_issue_claim",
            manager_v1::HeartbeatIssueClaimArgs {
                context: Some(manager_v1::ManagerWriteContext {
                    method: "manager_heartbeat_issue_claim".into(),
                    target: format!("issue:{issue_id}"),
                    schema_fingerprint: heartbeat_context.schema_fingerprint,
                    data_root_id: heartbeat_context.data_root_id,
                    contract_fingerprint: heartbeat_context.contract_fingerprint,
                    idempotency_key: "gateway-policy-heartbeat".into(),
                    correlation_id: "gateway-policy-heartbeat".into(),
                    actor_audit: Some(manager_v1::ActorAudit {
                        actor_id: "gateway-worker".into(),
                        actor_kind: v1::ActorKind::ExternalAgent as i32,
                        display_name: None,
                        agent_session_id: None,
                        reason: None,
                    }),
                }),
                issue_id: issue_id.to_string(),
                expected_revision: Some(current.revision),
                claim_revision: Some(current.revision),
                lease_handle: claim.lease_handle,
                requested_extension: Some(manager_v1::TimeSpan { millis: 10_000 }),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        heartbeat.outcome,
        Some(manager_v1::heartbeat_issue_claim_result::Outcome::Success(
            _
        ))
    ));
}

#[tokio::test]
async fn gateway_rpc_claim_then_heartbeat_uses_the_capability_dispatcher() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue_id = uuid::Uuid::new_v4();
    let created = server
        .app
        .core
        .tracker()
        .create_with_draft(
            issue_id,
            "create-gateway-rpc-capability-test",
            &v1::IssueDraft {
                title: "gateway capability test".into(),
                kind: v1::IssueKind::Task as i32,
                priority: v1::IssuePriority::P2 as i32,
                creation_basis: v1::CreationBasis::Explicit as i32,
                confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                ..Default::default()
            },
            "test",
            "gateway-rpc-capability-test",
        )
        .await
        .unwrap();
    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .register_gateway_policy(
            "gateway-rpc",
            "gateway-rpc",
            "register-gateway-rpc",
            &manager_v1::WorkEligibilityPolicy {
                mode: manager_v1::WorkEligibilityMode::ReviewedOnly as i32,
            },
        )
        .unwrap();
    let worker = server.for_gateway_worker("gateway-rpc").await.unwrap();

    let claim = IssueGateway::claim(
        &worker,
        Request::new(rpc_v1::IssueGatewayClaimRequest {
            issue_id: issue_id.to_string(),
            expected_revision: Some(created.revision),
            idempotency_key: "gateway-rpc-claim".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let claim = match claim.outcome {
        Some(rpc_v1::issue_gateway_claim_response::Outcome::Success(claim)) => claim,
        outcome => panic!("gateway claim must succeed: {outcome:?}"),
    };

    let heartbeat = IssueGateway::heartbeat(
        &worker,
        Request::new(rpc_v1::IssueGatewayHeartbeatRequest {
            issue_id: issue_id.to_string(),
            capability_id: claim.capability_id.clone(),
            expected_revision: Some(claim.revision),
            idempotency_key: "gateway-rpc-heartbeat".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    match heartbeat.outcome {
        Some(rpc_v1::issue_gateway_mutation_response::Outcome::Success(_)) => {}
        outcome => panic!("gateway heartbeat must succeed: {outcome:?}"),
    }

    let current = server.app.core.tracker().get(issue_id).await.unwrap();
    let blocked = server
        .app
        .core
        .tracker()
        .transition(
            issue_id,
            crate::tracker::IssueState::Blocked,
            current.revision,
            "block-gateway-claimed-issue",
            None,
        )
        .await
        .unwrap();
    let released = IssueGateway::release(
        &worker,
        Request::new(rpc_v1::IssueGatewayReleaseRequest {
            issue_id: issue_id.to_string(),
            capability_id: claim.capability_id,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let released = match released.outcome {
        Some(rpc_v1::issue_gateway_mutation_response::Outcome::Success(value)) => value,
        outcome => panic!("gateway release must succeed: {outcome:?}"),
    };
    assert_eq!(released.revision, blocked.revision + 1);
    assert_eq!(
        server.app.core.tracker().get(issue_id).await.unwrap().state,
        crate::tracker::IssueState::Blocked
    );
}

#[tokio::test]
async fn gateway_completes_to_awaiting_review_and_replays_only_the_same_run_after_restart() {
    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(
        IssueTrackerApplication::open(configured.clone())
            .await
            .unwrap(),
    );
    let issue_id = uuid::Uuid::new_v4();
    let created = server
        .app
        .core
        .tracker()
        .create_with_draft(
            issue_id,
            "create-awaiting-gateway-test",
            &v1::IssueDraft {
                title: "awaiting review gateway test".into(),
                kind: v1::IssueKind::Task as i32,
                priority: v1::IssuePriority::P2 as i32,
                creation_basis: v1::CreationBasis::Explicit as i32,
                confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                ..Default::default()
            },
            "test",
            "awaiting-gateway-test",
        )
        .await
        .unwrap();
    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .register_gateway_policy(
            "gateway-awaiting",
            "gateway-awaiting",
            "register-gateway-awaiting",
            &manager_v1::WorkEligibilityPolicy {
                mode: manager_v1::WorkEligibilityMode::ReviewedOnly as i32,
            },
        )
        .unwrap();
    let worker = server.for_gateway_worker("gateway-awaiting").await.unwrap();
    let claimed = IssueGateway::claim(
        &worker,
        Request::new(rpc_v1::IssueGatewayClaimRequest {
            issue_id: issue_id.to_string(),
            expected_revision: Some(created.revision),
            idempotency_key: "gateway-awaiting-claim".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let claim = match claimed.outcome {
        Some(rpc_v1::issue_gateway_claim_response::Outcome::Success(value)) => value,
        outcome => panic!("gateway claim must succeed: {outcome:?}"),
    };
    let heartbeat = IssueGateway::heartbeat(
        &worker,
        Request::new(rpc_v1::IssueGatewayHeartbeatRequest {
            issue_id: issue_id.to_string(),
            capability_id: claim.capability_id.clone(),
            expected_revision: Some(claim.revision),
            idempotency_key: "gateway-awaiting-heartbeat".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let latest_revision = match heartbeat.outcome {
        Some(rpc_v1::issue_gateway_mutation_response::Outcome::Success(value)) => value.revision,
        outcome => panic!("gateway heartbeat must succeed: {outcome:?}"),
    };
    let request = rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest {
        issue_id: issue_id.to_string(),
        capability_id: claim.capability_id.clone(),
        expected_revision: Some(latest_revision),
        idempotency_key: "gateway-awaiting-finish-run-17".into(),
        state_contract_version: "awaiting_review/v1".into(),
        completion: Some(v1::AwaitingReviewCompletion {
            run_id: "run-17".into(),
            full_commit_id: "b".repeat(40),
            evidence_references: vec!["artifact://run-17/summary.json".into()],
            report: Some(v1::CompletionReport {
                result: Some(v1::completion_report::Result::Verified(
                    v1::VerifiedCompletion {
                        evidence: vec![v1::CompletionEvidence {
                            kind: v1::CompletionEvidenceKind::TestResult as i32,
                            summary: "required checks passed".into(),
                            uri: Some("artifact://run-17/tests.json".into()),
                        }],
                    },
                )),
            }),
        }),
    };

    let incompatible = IssueGateway::complete_execution_to_awaiting_review(
        &worker,
        Request::new(
            rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest {
                state_contract_version: "legacy-v7-client".into(),
                ..request.clone()
            },
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        awaiting_review_error_code(&incompatible),
        Some("precondition_failed")
    );
    assert_eq!(
        server.app.core.tracker().get(issue_id).await.unwrap().state,
        crate::tracker::IssueState::InProgress
    );

    let completed =
        IssueGateway::complete_execution_to_awaiting_review(&worker, Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner();
    let result = match completed.outcome {
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Success(
                value,
            ),
        ) => value,
        outcome => panic!("dedicated awaiting-review mutation must succeed: {outcome:?}"),
    };
    assert_eq!(result.issue_id, issue_id.to_string());
    assert_eq!(result.revision, latest_revision + 1);
    assert_eq!(result.run_id, "run-17");
    assert_eq!(result.full_commit_id, "b".repeat(40));
    assert_eq!(
        result.evidence_references,
        vec!["artifact://run-17/summary.json"]
    );
    assert!(result.lease_closed);

    let current = server.app.core.tracker().get(issue_id).await.unwrap();
    assert_eq!(current.state, crate::tracker::IssueState::AwaitingReview);
    assert_eq!(current.lease_expires_at, None);
    assert_eq!(current.lease_handle, None);
    let ready = IssueGateway::list_ready(
        &worker,
        Request::new(rpc_v1::IssueGatewayListReadyRequest {
            page_size: 20,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    match ready.outcome {
        Some(rpc_v1::issue_gateway_list_ready_response::Outcome::Success(page)) => {
            assert!(!page.issues.iter().any(|item| {
                item.issue
                    .as_ref()
                    .is_some_and(|issue| issue.issue_id == issue_id.to_string())
            }));
        }
        outcome => panic!("awaiting-review issue must be excluded from ready: {outcome:?}"),
    }

    // Generic worker state mutation is not a way around the dedicated,
    // versioned path, even when the client still knows the consumed cap id.
    let generic = IssueGateway::transition(
        &worker,
        Request::new(rpc_v1::IssueGatewayTransitionRequest {
            issue_id: issue_id.to_string(),
            target_state: v1::IssueState::AwaitingReview as i32,
            capability_id: claim.capability_id.clone(),
            expected_revision: Some(result.revision),
            idempotency_key: "generic-awaiting-bypass".into(),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        generic.outcome,
        Some(rpc_v1::issue_gateway_mutation_response::Outcome::Error(_))
    ));
    let old_heartbeat = IssueGateway::heartbeat(
        &worker,
        Request::new(rpc_v1::IssueGatewayHeartbeatRequest {
            issue_id: issue_id.to_string(),
            capability_id: claim.capability_id.clone(),
            expected_revision: Some(result.revision),
            idempotency_key: "old-cap-heartbeat-after-awaiting".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        old_heartbeat.outcome,
        Some(rpc_v1::issue_gateway_mutation_response::Outcome::Error(_))
    ));

    drop(worker);
    drop(server);
    let restarted = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let restarted_worker = restarted
        .for_gateway_worker("gateway-awaiting")
        .await
        .unwrap();
    let mut replay_request = request;
    replay_request.capability_id.clear();
    let replay = IssueGateway::complete_execution_to_awaiting_review(
        &restarted_worker,
        Request::new(replay_request.clone()),
    )
    .await
    .unwrap()
    .into_inner();
    match replay.outcome {
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Success(
                replayed,
            ),
        ) => assert_eq!(replayed, result),
        outcome => panic!("same run idempotency key must replay after restart: {outcome:?}"),
    }

    let mut actor_session_conflict = replay_request.clone();
    actor_session_conflict.completion.as_mut().unwrap().run_id = "run-18".into();
    let actor_conflict = IssueGateway::complete_execution_to_awaiting_review(
        &restarted_worker,
        Request::new(actor_session_conflict),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        actor_conflict.outcome,
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Error(
                rpc_v1::IssueGatewayError { code, .. }
            )
        ) if code == "idempotency_conflict"
    ));

    let mut report_conflict_request = replay_request.clone();
    report_conflict_request
        .completion
        .as_mut()
        .unwrap()
        .report
        .as_mut()
        .unwrap()
        .result = Some(v1::completion_report::Result::Unverified(
        v1::UnverifiedCompletion {
            reason: "checks were inconclusive".into(),
            residual_risk: "one required check did not run".into(),
        },
    ));
    let report_conflict = IssueGateway::complete_execution_to_awaiting_review(
        &restarted_worker,
        Request::new(report_conflict_request),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        report_conflict.outcome,
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Error(
                rpc_v1::IssueGatewayError { code, .. }
            )
        ) if code == "idempotency_conflict"
    ));

    let mut conflicting_replay = replay_request.clone();
    conflicting_replay
        .completion
        .as_mut()
        .unwrap()
        .full_commit_id = "c".repeat(40);
    let conflict = IssueGateway::complete_execution_to_awaiting_review(
        &restarted_worker,
        Request::new(conflicting_replay),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        conflict.outcome,
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Error(
                rpc_v1::IssueGatewayError { code, .. }
            )
        ) if code == "idempotency_conflict"
    ));

    restarted
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .register_gateway_policy(
            "gateway-awaiting",
            "gateway-awaiting",
            "register-gateway-awaiting-policy-replacement",
            &manager_v1::WorkEligibilityPolicy {
                mode: manager_v1::WorkEligibilityMode::IncludeAutoRegistered as i32,
            },
        )
        .unwrap();
    drop(restarted_worker);
    let policy_restarted_worker = restarted
        .for_gateway_worker("gateway-awaiting")
        .await
        .unwrap();
    let replaced_policy_replay = IssueGateway::complete_execution_to_awaiting_review(
        &policy_restarted_worker,
        Request::new(replay_request.clone()),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        replaced_policy_replay.outcome,
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Error(
                rpc_v1::IssueGatewayError { code, .. }
            )
        ) if code == "idempotency_conflict"
    ));

    let stale_heartbeat = IssueGateway::heartbeat(
        &policy_restarted_worker,
        Request::new(rpc_v1::IssueGatewayHeartbeatRequest {
            issue_id: issue_id.to_string(),
            capability_id: claim.capability_id,
            expected_revision: Some(result.revision),
            idempotency_key: "old-cap-heartbeat-after-restart".into(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        stale_heartbeat.outcome,
        Some(rpc_v1::issue_gateway_mutation_response::Outcome::Error(_))
    ));
    assert_eq!(
        restarted
            .app
            .core
            .tracker()
            .get(issue_id)
            .await
            .unwrap()
            .revision,
        result.revision
    );
}

async fn create_awaiting_review_issue(
    tracker: &crate::tracker::Tracker,
    operation_key: &str,
    run_id: &str,
) -> (crate::model::Issue, v1::AwaitingReviewCompletion) {
    let issue = tracker
        .create(operation_key, operation_key)
        .await
        .expect("create issue for review");
    let claim_operation = format!("{operation_key}-claim");
    let claimed = tracker
        .claim(
            issue.id,
            "execution-agent",
            issue.revision,
            60,
            &claim_operation,
        )
        .await
        .expect("claim issue for execution");
    let completion = crate::tracker::tests::awaiting_review_completion(run_id);
    let lease_handle = claimed
        .lease_handle
        .as_deref()
        .expect("claim returns its one-time handle");
    let finish_operation = format!("{operation_key}-awaiting-review");
    tracker
        .complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            &finish_operation,
            Some(lease_handle),
            Some(claimed.revision),
            "execution-agent",
            Some(run_id),
            &finish_operation,
            &completion,
        )
        .await
        .expect("finish execution into review");
    (
        tracker.get(issue.id).await.expect("read awaiting issue"),
        completion,
    )
}

#[tokio::test]
async fn manager_transition_finishes_review_or_requests_rework_with_revision_fencing() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let tracker = server.app.core.tracker();
    let identity = server.app.manager_store_context().await.unwrap();
    let write_context = |issue_id: uuid::Uuid,
                         idempotency_key: &str,
                         actor_id: &str,
                         actor_kind: v1::ActorKind,
                         reason: &str| manager_v1::ManagerWriteContext {
        method: "manager_transition_issue".into(),
        target: format!("issue:{issue_id}"),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: idempotency_key.into(),
        correlation_id: idempotency_key.into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: actor_id.into(),
            actor_kind: actor_kind as i32,
            reason: Some(reason.into()),
            ..Default::default()
        }),
    };

    let (issue, run) =
        create_awaiting_review_issue(tracker, "manager-review-approved", "review-run-approved")
            .await;
    let report = run.report.clone().expect("run has a completion report");
    let approve_request = manager_v1::TransitionIssueArgs {
        context: Some(write_context(
            issue.id,
            "manager-review-approve",
            "agent-app",
            v1::ActorKind::InternalAgent,
            "agent run completed successfully",
        )),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        target_state: v1::IssueState::Done as i32,
        terminal_report: Some(
            manager_v1::transition_issue_args::TerminalReport::Completion(report.clone()),
        ),
        ..Default::default()
    };
    let approved =
        ManagerConsumerService::transition_issue(&server, Request::new(approve_request.clone()))
            .await
            .expect("Manager TransitionIssue succeeds")
            .into_inner();
    let approved_mutation = match approved.outcome {
        Some(manager_v1::transition_issue_result::Outcome::Success(value)) => value,
        outcome => panic!("agent-app completion failed: {outcome:?}"),
    };
    let approved_issue = tracker.get(issue.id).await.unwrap();
    assert_eq!(approved_issue.state, crate::model::IssueState::Done);
    assert_eq!(approved_issue.revision, issue.revision + 1);
    assert!(approved_issue.completed_at.is_some());
    assert!(approved_issue.lease_owner.is_none());
    assert!(approved_issue.lease_expires_at.is_none());
    assert!(
        tracker
            .list_history(issue.id)
            .await
            .unwrap()
            .iter()
            .any(|entry| {
                entry.state_change.as_ref().is_some_and(|change| {
                    change.old_state == Some(v1::IssueState::AwaitingReview as i32)
                        && change.new_state == v1::IssueState::Done as i32
                        && change.completion.as_ref() == Some(&report)
                })
            })
    );
    assert!(
        approved_mutation
            .operation
            .as_ref()
            .is_some_and(|operation| {
                operation.state == manager_v1::DurableOperationState::Committed as i32
            })
    );

    let history_count = tracker.list_history(issue.id).await.unwrap().len();
    let replay = ManagerConsumerService::transition_issue(&server, Request::new(approve_request))
        .await
        .expect("identical Manager transition replays")
        .into_inner();
    assert!(matches!(
        replay.outcome,
        Some(manager_v1::transition_issue_result::Outcome::Success(_))
    ));
    assert_eq!(
        tracker.get(issue.id).await.unwrap().revision,
        approved_issue.revision
    );
    assert_eq!(
        tracker.list_history(issue.id).await.unwrap().len(),
        history_count
    );

    let (issue, run) = create_awaiting_review_issue(
        tracker,
        "manager-review-human-approved",
        "review-run-human-approved",
    )
    .await;
    let human_complete_key = "manager-review-human-complete";
    let human_complete = ManagerConsumerService::apply_issue_admin_action(
        &server,
        Request::new(manager_v1::ApplyIssueAdminActionArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_apply_issue_admin_action".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint.clone(),
                data_root_id: identity.data_root_id.clone(),
                contract_fingerprint: identity.contract_fingerprint.clone(),
                idempotency_key: human_complete_key.into(),
                correlation_id: human_complete_key.into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "reviewer".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    reason: Some("reviewed run evidence and accepted the result".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            action: manager_v1::IssueAdminAction::Complete as i32,
            terminal_report: Some(
                manager_v1::apply_issue_admin_action_args::TerminalReport::Completion(
                    run.report.expect("run has a completion report"),
                ),
            ),
            ..Default::default()
        }),
    )
    .await
    .expect("human review completion succeeds")
    .into_inner();
    let human_mutation = match human_complete.outcome {
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(value)) => value,
        outcome => panic!("human review completion failed: {outcome:?}"),
    };
    let human_receipt = match human_mutation.result {
        Some(manager_v1::manager_mutation::Result::IssueAdminAction(value)) => value,
        result => panic!("human completion has no administrative receipt: {result:?}"),
    };
    assert_eq!(human_receipt.resulting_state, v1::IssueState::Done as i32);
    assert_eq!(human_receipt.resulting_revision, issue.revision + 1);
    let human_completed_issue = tracker.get(issue.id).await.unwrap();
    assert_eq!(human_completed_issue.state, crate::model::IssueState::Done);
    assert_eq!(
        human_completed_issue.revision,
        human_receipt.resulting_revision
    );

    let (issue, run) =
        create_awaiting_review_issue(tracker, "manager-review-rework", "review-run-rework").await;
    tracker
        .update_title(
            issue.id,
            "updated while under review",
            issue.revision,
            "manager-review-intervening-update",
        )
        .await
        .expect("an intervening revision may update review metadata");
    let stale_rework = ManagerConsumerService::transition_issue(
        &server,
        Request::new(manager_v1::TransitionIssueArgs {
            context: Some(write_context(
                issue.id,
                "manager-review-stale-rework",
                "reviewer",
                v1::ActorKind::Human,
                "request changes",
            )),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            target_state: v1::IssueState::Open as i32,
            ..Default::default()
        }),
    )
    .await
    .expect("stale review decision returns a typed result")
    .into_inner();
    assert!(matches!(
        stale_rework.outcome,
        Some(manager_v1::transition_issue_result::Outcome::Error(_))
    ));
    let still_reviewing = tracker.get(issue.id).await.unwrap();
    assert_eq!(
        still_reviewing.state,
        crate::model::IssueState::AwaitingReview
    );
    assert_eq!(still_reviewing.revision, issue.revision + 1);

    let rework_revision = still_reviewing.revision;
    let rework_key = "manager-review-request-rework";
    let rework = ManagerConsumerService::transition_issue(
        &server,
        Request::new(manager_v1::TransitionIssueArgs {
            context: Some(write_context(
                issue.id,
                rework_key,
                "reviewer",
                v1::ActorKind::Human,
                "please address the failing integration test",
            )),
            issue_id: issue.id.to_string(),
            expected_revision: Some(rework_revision),
            target_state: v1::IssueState::Open as i32,
            ..Default::default()
        }),
    )
    .await
    .expect("reviewer can request rework through Manager TransitionIssue")
    .into_inner();
    assert!(matches!(
        rework.outcome,
        Some(manager_v1::transition_issue_result::Outcome::Success(_))
    ));
    let reopened = tracker.get(issue.id).await.unwrap();
    assert_eq!(reopened.state, crate::model::IssueState::Open);
    assert_eq!(reopened.revision, rework_revision + 1);
    assert!(reopened.completed_at.is_none());
    assert!(reopened.lease_owner.is_none());
    assert!(reopened.lease_expires_at.is_none());
    let history = tracker.list_history(issue.id).await.unwrap();
    assert!(history.iter().any(|entry| {
        entry
            .state_change
            .as_ref()
            .is_some_and(|change| change.awaiting_review.as_ref() == Some(&run))
    }));
    assert!(history.iter().any(|entry| {
        entry.reason == "please address the failing integration test"
            && entry.state_change.as_ref().is_some_and(|change| {
                change.old_state == Some(v1::IssueState::AwaitingReview as i32)
                    && change.new_state == v1::IssueState::Open as i32
            })
    }));

    let claim = tracker
        .claim(
            issue.id,
            "next-execution-agent",
            reopened.revision,
            60,
            "manager-review-next-run-claim",
        )
        .await
        .expect("reopened issue can begin a new run");
    let late_approval = ManagerConsumerService::transition_issue(
        &server,
        Request::new(manager_v1::TransitionIssueArgs {
            context: Some(write_context(
                issue.id,
                "manager-review-late-approval",
                "agent-app",
                v1::ActorKind::InternalAgent,
                "late result from the previous run",
            )),
            issue_id: issue.id.to_string(),
            expected_revision: Some(rework_revision),
            target_state: v1::IssueState::Done as i32,
            terminal_report: Some(
                manager_v1::transition_issue_args::TerminalReport::Completion(run.report.unwrap()),
            ),
            ..Default::default()
        }),
    )
    .await
    .expect("old run approval returns a typed result")
    .into_inner();
    assert!(matches!(
        late_approval.outcome,
        Some(manager_v1::transition_issue_result::Outcome::Error(_))
    ));
    let current = tracker.get(issue.id).await.unwrap();
    assert_eq!(current.state, crate::model::IssueState::InProgress);
    assert_eq!(current.revision, claim.revision);
    assert!(current.lease_expires_at.is_some());
    assert!(claim.lease_handle.is_some());
}

struct ClaimedAwaitingReviewGateway {
    configured: ServerSettings,
    server: GrpcServer,
    worker: GrpcServer,
    issue_id: uuid::Uuid,
    request: rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest,
}

/// Opens a Manager-backed server, claims one issue through a Gateway worker,
/// and builds the matching awaiting-review completion request.
async fn claimed_awaiting_review_gateway(
    root: &TempDir,
    gateway_id: &str,
    run_id: &str,
) -> ClaimedAwaitingReviewGateway {
    let mut configured = settings(root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(
        IssueTrackerApplication::open(configured.clone())
            .await
            .unwrap(),
    );
    let issue_id = uuid::Uuid::new_v4();
    let created = server
        .app
        .core
        .tracker()
        .create_with_draft(
            issue_id,
            &format!("create-{gateway_id}"),
            &v1::IssueDraft {
                title: gateway_id.into(),
                kind: v1::IssueKind::Task as i32,
                priority: v1::IssuePriority::P2 as i32,
                creation_basis: v1::CreationBasis::Explicit as i32,
                confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                ..Default::default()
            },
            "test",
            gateway_id,
        )
        .await
        .unwrap();
    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .register_gateway_policy(
            gateway_id,
            gateway_id,
            &format!("register-{gateway_id}"),
            &manager_v1::WorkEligibilityPolicy {
                mode: manager_v1::WorkEligibilityMode::ReviewedOnly as i32,
            },
        )
        .unwrap();
    let worker = server.for_gateway_worker(gateway_id).await.unwrap();
    let claim_response = IssueGateway::claim(
        &worker,
        Request::new(rpc_v1::IssueGatewayClaimRequest {
            issue_id: issue_id.to_string(),
            expected_revision: Some(created.revision),
            idempotency_key: format!("{gateway_id}-claim"),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let claim = match claim_response.outcome {
        Some(rpc_v1::issue_gateway_claim_response::Outcome::Success(claim)) => claim,
        outcome => panic!("claim failed before awaiting-review test: {outcome:?}"),
    };
    let request = rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest {
        issue_id: issue_id.to_string(),
        capability_id: claim.capability_id,
        expected_revision: Some(claim.revision),
        idempotency_key: format!("{gateway_id}-{run_id}"),
        state_contract_version: "awaiting_review/v1".into(),
        completion: Some(crate::tracker::tests::awaiting_review_completion(run_id)),
    };
    ClaimedAwaitingReviewGateway {
        configured,
        server,
        worker,
        issue_id,
        request,
    }
}

fn awaiting_review_error_code(
    response: &rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewResponse,
) -> Option<&str> {
    match response.outcome.as_ref() {
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Error(
                error,
            ),
        ) => Some(error.code.as_str()),
        _ => None,
    }
}

#[tokio::test]
async fn generic_gateway_transition_cannot_enter_awaiting_review_with_a_live_capability() {
    let root = TempDir::new().unwrap();
    let ClaimedAwaitingReviewGateway {
        server,
        worker,
        issue_id,
        request,
        ..
    } = claimed_awaiting_review_gateway(&root, "gateway-awaiting-bypass", "run-bypass").await;

    let generic = IssueGateway::transition(
        &worker,
        Request::new(rpc_v1::IssueGatewayTransitionRequest {
            issue_id: issue_id.to_string(),
            target_state: v1::IssueState::AwaitingReview as i32,
            capability_id: request.capability_id.clone(),
            expected_revision: request.expected_revision,
            idempotency_key: "generic-awaiting-bypass-live".into(),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(
        matches!(
            generic.outcome,
            Some(rpc_v1::issue_gateway_mutation_response::Outcome::Error(ref error))
                if error.code == "precondition_failed"
        ),
        "{generic:?}"
    );
    assert_eq!(
        server.app.core.tracker().get(issue_id).await.unwrap().state,
        crate::tracker::IssueState::InProgress
    );

    // The rejected bypass must not consume the capability for the
    // dedicated, versioned path.
    let completed =
        IssueGateway::complete_execution_to_awaiting_review(&worker, Request::new(request))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(
        awaiting_review_error_code(&completed),
        None,
        "{completed:?}"
    );
    assert_eq!(
        server.app.core.tracker().get(issue_id).await.unwrap().state,
        crate::tracker::IssueState::AwaitingReview
    );
}

#[tokio::test]
async fn awaiting_review_unsent_failure_keeps_capability_and_allows_same_key_retry() {
    let root = TempDir::new().unwrap();
    let ClaimedAwaitingReviewGateway {
        server,
        worker,
        issue_id,
        request,
        ..
    } = claimed_awaiting_review_gateway(&root, "gateway-awaiting-unsent", "run-unsent").await;

    worker.fail_next_gateway_store_context();
    let failed =
        IssueGateway::complete_execution_to_awaiting_review(&worker, Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(awaiting_review_error_code(&failed), Some("unavailable"));
    assert_eq!(
        server.app.core.tracker().get(issue_id).await.unwrap().state,
        crate::tracker::IssueState::InProgress
    );

    // Nothing reached the Manager, so the one-time capability and the
    // idempotency key must both remain usable.
    let retried =
        IssueGateway::complete_execution_to_awaiting_review(&worker, Request::new(request))
            .await
            .unwrap()
            .into_inner();
    assert!(
        matches!(
            retried.outcome,
            Some(
                rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Success(_)
            )
        ),
        "{retried:?}"
    );
    assert_eq!(
        server.app.core.tracker().get(issue_id).await.unwrap().state,
        crate::tracker::IssueState::AwaitingReview
    );
}

#[tokio::test]
async fn awaiting_review_restart_recovers_receipt_when_gateway_persistence_fails_after_tracker_commit()
 {
    let root = TempDir::new().unwrap();
    let ClaimedAwaitingReviewGateway {
        configured,
        server,
        worker,
        issue_id,
        request,
    } = claimed_awaiting_review_gateway(&root, "gateway-awaiting-recovery", "run-recovery-window")
        .await;

    worker.fail_next_gateway_replay_persistence();
    let failed_response =
        IssueGateway::complete_execution_to_awaiting_review(&worker, Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner();
    assert!(matches!(
        failed_response.outcome,
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Error(
                rpc_v1::IssueGatewayError { code, .. }
            )
        ) if code == "unconfirmed"
    ));

    let committed = server.app.core.tracker().get(issue_id).await.unwrap();
    assert_eq!(committed.state, crate::tracker::IssueState::AwaitingReview);
    let committed_revision = committed.revision;
    let committed_history = server
        .app
        .core
        .tracker()
        .list_history(issue_id)
        .await
        .unwrap();
    assert_eq!(
        committed_history
            .iter()
            .filter(|entry| {
                entry.state_change.as_ref().is_some_and(|change| {
                    change
                        .awaiting_review
                        .as_ref()
                        .is_some_and(|completion| completion.run_id == "run-recovery-window")
                })
            })
            .count(),
        1
    );
    drop(worker);
    drop(server);

    let restarted = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let restarted_worker = restarted
        .for_gateway_worker("gateway-awaiting-recovery")
        .await
        .unwrap();
    let mut exact_retry = request;
    exact_retry.capability_id.clear();
    let recovered = IssueGateway::complete_execution_to_awaiting_review(
        &restarted_worker,
        Request::new(exact_retry),
    )
    .await
    .unwrap()
    .into_inner();
    let receipt = match recovered.outcome {
        Some(
            rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome::Success(
                receipt,
            ),
        ) => receipt,
        outcome => panic!("canonical awaiting-review result was not recovered: {outcome:?}"),
    };
    assert_eq!(receipt.revision, committed_revision);
    assert_eq!(receipt.run_id, "run-recovery-window");
    assert_eq!(receipt.full_commit_id, "a".repeat(40));
    assert!(receipt.lease_closed);
    let after_retry = restarted.app.core.tracker().get(issue_id).await.unwrap();
    assert_eq!(after_retry.revision, committed_revision);
    assert_eq!(
        restarted
            .app
            .core
            .tracker()
            .list_history(issue_id)
            .await
            .unwrap()
            .len(),
        committed_history.len()
    );
}

#[tokio::test]
async fn entity_specific_issue_service_uses_the_same_application_module() {
    let root = TempDir::new().unwrap();
    let server = GrpcServer::new(
        IssueTrackerApplication::open(settings(&root))
            .await
            .unwrap(),
    );

    let response = IssueService::list_issues(&server, Request::new(v1::ListIssuesArgs::default()))
        .await
        .unwrap()
        .into_inner();

    assert!(matches!(
        response.outcome,
        Some(v1::list_issues_result::Outcome::Success(_))
    ));
}

#[tokio::test]
async fn manager_consumer_exposes_candidate_read_without_raw_claim() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());

    let response = ManagerConsumerService::get_candidate(
        &server,
        Request::new(manager_v1::GetCandidateArgs {
            candidate_id: "missing".into(),
            context: Some(server.app.manager_store_context().await.unwrap()),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::get_candidate_result::Outcome::Error(_))
    ));
}

#[tokio::test]
async fn manager_consumer_updates_issue_with_revision_and_mask_validation() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("manager-update", "before update")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let update_args =
        |idempotency_key: &str, expected_revision: u64, update_mask: Vec<i32>, title: &str| {
            manager_v1::UpdateIssueArgs {
                context: Some(manager_v1::ManagerWriteContext {
                    method: "manager_update_issue".into(),
                    target: format!("issue:{}", issue.id),
                    schema_fingerprint: identity.schema_fingerprint.clone(),
                    idempotency_key: idempotency_key.into(),
                    actor_audit: Some(manager_v1::ActorAudit {
                        actor_id: "agent-app".into(),
                        actor_kind: v1::ActorKind::InternalAgent as i32,
                        reason: Some("test issue update".into()),
                        ..Default::default()
                    }),
                    correlation_id: "manager-update-test".into(),
                    data_root_id: identity.data_root_id.clone(),
                    contract_fingerprint: identity.contract_fingerprint.clone(),
                }),
                issue_id: issue.id.to_string(),
                expected_revision: Some(expected_revision),
                update_mask,
                patch: Some(manager_v1::ManagerIssuePatch {
                    title: Some(title.into()),
                    ..Default::default()
                }),
                ..Default::default()
            }
        };

    let response = ManagerConsumerService::update_issue(
        &server,
        Request::new(update_args(
            "manager-update-success",
            issue.revision,
            vec![manager_v1::ManagerIssueField::Title as i32],
            "after update",
        )),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::update_issue_result::Outcome::Success(_))
    ));
    let updated = server.app.core.tracker().get(issue.id).await.unwrap();
    assert_eq!(updated.title, "after update");
    assert!(updated.revision > issue.revision);

    let conflict = ManagerConsumerService::update_issue(
        &server,
        Request::new(update_args(
            "manager-update-stale-revision",
            issue.revision,
            vec![manager_v1::ManagerIssueField::Title as i32],
            "must not replace title",
        )),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        conflict.outcome,
        Some(manager_v1::update_issue_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::RevisionConflict as i32
    ));

    let invalid_mask = ManagerConsumerService::update_issue(
        &server,
        Request::new(update_args(
            "manager-update-invalid-mask",
            updated.revision,
            vec![99],
            "invalid mask must not replace title",
        )),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        invalid_mask.outcome,
        Some(manager_v1::update_issue_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ManagerInvalidArgument as i32
    ));

    let unchanged = server.app.core.tracker().get(issue.id).await.unwrap();
    assert_eq!(unchanged.title, "after update");
    assert_eq!(unchanged.revision, updated.revision);
}

#[tokio::test]
async fn primary_consumer_applies_administrative_cancellation() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("admin", "admin")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let response = ManagerConsumerService::apply_issue_admin_action(
        &server,
        Request::new(manager_v1::ApplyIssueAdminActionArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_apply_issue_admin_action".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint.clone(),
                data_root_id: identity.data_root_id.clone(),
                contract_fingerprint: identity.contract_fingerprint.clone(),
                idempotency_key: "admin-cancel".into(),
                correlation_id: "admin-test".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "admin".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    reason: Some("cancelled by administrator".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            action: manager_v1::IssueAdminAction::Cancel as i32,
            terminal_report: Some(
                manager_v1::apply_issue_admin_action_args::TerminalReport::Cancellation(
                    v1::CancellationReport {
                        classification: v1::CancellationClass::Other as i32,
                    },
                ),
            ),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let mutation = match response.outcome {
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(value)) => value,
        outcome => panic!("administrative cancellation failed: {outcome:?}"),
    };
    let operation = mutation
        .operation
        .clone()
        .expect("durable administrative operation");
    assert_eq!(
        operation.state,
        manager_v1::DurableOperationState::Committed as i32
    );
    assert_eq!(
        server
            .app
            .manager
            .as_ref()
            .unwrap()
            .lock()
            .await
            .storage_mut()
            .repository()
            .get_operation(&operation.operation_id)
            .unwrap()
            .expect("persisted administrative operation")
            .request_digest,
        operation.request_digest
    );
    assert!(matches!(
        server
            .app
            .manager
            .as_ref()
            .unwrap()
            .lock()
            .await
            .storage_mut()
            .repository()
            .get_operation(&operation.operation_id)
            .unwrap()
            .expect("persisted administrative operation")
            .terminal_snapshot,
        Some(manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(
            manager_v1::Snapshot { schema, .. }
        )) if schema == "issue_tracker.manager.v1.ApplyIssueAdminActionResult"
    ));
    assert!(matches!(
        mutation.result,
        Some(manager_v1::manager_mutation::Result::IssueAdminAction(
            manager_v1::IssueAdminActionResult {
                resulting_state,
                resulting_revision,
                changed: true,
                ..
            }
        )) if resulting_state == v1::IssueState::Canceled as i32
            && resulting_revision == issue.revision + 1
    ));
    assert_eq!(
        server.app.core.tracker().get(issue.id).await.unwrap().state,
        crate::tracker::IssueState::Canceled
    );
    let replay = ManagerConsumerService::apply_issue_admin_action(
        &server,
        Request::new(manager_v1::ApplyIssueAdminActionArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_apply_issue_admin_action".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint,
                data_root_id: identity.data_root_id,
                contract_fingerprint: identity.contract_fingerprint,
                idempotency_key: "admin-cancel".into(),
                correlation_id: "admin-test".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "admin".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    reason: Some("cancelled by administrator".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            action: manager_v1::IssueAdminAction::Cancel as i32,
            terminal_report: Some(
                manager_v1::apply_issue_admin_action_args::TerminalReport::Cancellation(
                    v1::CancellationReport {
                        classification: v1::CancellationClass::Other as i32,
                    },
                ),
            ),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let replay = match replay.outcome {
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(value)) => value,
        outcome => panic!("administrative cancellation replay failed: {outcome:?}"),
    };
    assert_eq!(
        replay
            .operation
            .expect("durable replay operation")
            .operation_id,
        operation.operation_id
    );
}

#[tokio::test]
async fn failed_admin_replay_does_not_resolve_ownership_diagnosis() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let blocker = server
        .app
        .core
        .tracker()
        .create("blocker", "blocker")
        .await
        .unwrap();
    let issue = server
        .app
        .core
        .tracker()
        .create("blocked", "blocked")
        .await
        .unwrap();
    server
        .app
        .core
        .tracker()
        .relate(
            blocker.id,
            crate::tracker::RelationKind::Blocks,
            issue.id,
            0,
            0,
            "diag-rel",
        )
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let diagnosis = {
        let mut manager = server.app.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .repository()
            .record_ownership_integrity_diagnosis(
                "issue",
                &issue.id.to_string(),
                "LEASE_INCONSISTENT",
                Some(issue.revision + 1),
                Some("INCONSISTENT"),
                "explicit repair required",
                &["lb.lease_expires_at".into()],
                None,
                &[],
            )
            .unwrap();
        manager
            .storage_mut()
            .repository()
            .list_ownership_integrity_diagnoses(Some("issue"), Some(&issue.id.to_string()), 10, 0)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    };
    let context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "failed-admin-diagnosis-replay".into(),
        correlation_id: "failed-admin-diagnosis-replay".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("attempt completion".into()),
            ..Default::default()
        }),
    };
    let args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(context),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision + 1),
        action: manager_v1::IssueAdminAction::Complete as i32,
        terminal_report: Some(
            manager_v1::apply_issue_admin_action_args::TerminalReport::Completion(
                v1::CompletionReport {
                    result: Some(v1::completion_report::Result::Verified(
                        v1::VerifiedCompletion {
                            evidence: vec![v1::CompletionEvidence {
                                kind: v1::CompletionEvidenceKind::OtherEvidence as i32,
                                summary: "blocked completion".into(),
                                uri: None,
                            }],
                        },
                    )),
                },
            ),
        ),
        ownership_diagnosis: Some(manager_v1::OwnershipIntegrityDiagnosisReference {
            target_kind: diagnosis.target_kind.clone(),
            target_id: diagnosis.target_id.clone(),
            diagnosis_class: diagnosis.diagnosis_class.clone(),
            evidence_digest: diagnosis.evidence_digest.clone(),
            diagnosis_generation: diagnosis.diagnosis_generation,
            observation_token: diagnosis.observation_token,
            repair_epoch: diagnosis.repair_epoch,
        }),
        ..Default::default()
    };
    let first =
        ManagerConsumerService::apply_issue_admin_action(&server, Request::new(args.clone()))
            .await
            .unwrap()
            .into_inner();
    assert!(matches!(
        first.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(
            _
        ))
    ));
    let replay = ManagerConsumerService::apply_issue_admin_action(&server, Request::new(args))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        replay.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(
            _
        ))
    ));
    let remaining = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .list_ownership_integrity_diagnoses(Some("issue"), Some(&issue.id.to_string()), 10, 0)
        .unwrap();
    assert_eq!(remaining[0].state, "OPEN");
    assert!(remaining[0].resolved_at.is_none());
}

#[tokio::test]
async fn primary_consumer_rejects_unproven_not_applied_resolution_without_mutation() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("resolution", "resolution")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(manager_v1::ResolveIssueOperationArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_resolve_issue_operation".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint,
                data_root_id: identity.data_root_id,
                contract_fingerprint: identity.contract_fingerprint,
                idempotency_key: "unproven-not-applied".into(),
                correlation_id: "resolution-test".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "administrator".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    display_name: None,
                    agent_session_id: None,
                    reason: Some("requires server-owned evidence".into()),
                }),
            }),
            issue_id: issue.id.to_string(),
            operation_id: "missing-operation".into(),
            expected_operation_generation: Some(1),
            expected_revision: Some(issue.revision),
            decision: manager_v1::IssueOperationResolutionDecision::ConfirmNotApplied as i32,
            evidence_refs: vec!["untrusted-reference".into()],
            mode: manager_v1::IssueOperationResolutionMode::EvidenceConfirmation as i32,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(
            _
        ))
    ));
    let current = server.app.core.tracker().get(issue.id).await.unwrap();
    assert_eq!(current.revision, issue.revision);
    assert_eq!(current.state, crate::tracker::IssueState::Open);
}

#[tokio::test]
async fn resolution_requires_presence_for_both_compare_and_swap_values() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("resolution-cas-presence", "resolution-cas-presence")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let context = || manager_v1::ManagerWriteContext {
        method: "manager_resolve_issue_operation".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "cas-presence".into(),
        correlation_id: "cas-presence".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("validate mandatory CAS presence".into()),
            ..Default::default()
        }),
    };
    let missing_generation = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(manager_v1::ResolveIssueOperationArgs {
            context: Some(context()),
            issue_id: issue.id.to_string(),
            operation_id: "missing-operation".into(),
            expected_operation_generation: None,
            expected_revision: Some(issue.revision),
            mode: manager_v1::IssueOperationResolutionMode::HumanArbitration as i32,
            arbitration_outcome: manager_v1::IssueOperationArbitrationOutcome::UnknownSettled
                as i32,
            uncertainty_acknowledged: true,
            arbitration_reason: "validate mandatory CAS presence".into(),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        missing_generation.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ManagerInvalidArgument as i32
    ));

    let missing_revision = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(manager_v1::ResolveIssueOperationArgs {
            context: Some({
                let mut value = context();
                value.idempotency_key = "cas-presence-revision".into();
                value.correlation_id = "cas-presence-revision".into();
                value
            }),
            issue_id: issue.id.to_string(),
            operation_id: "missing-operation".into(),
            expected_operation_generation: Some(0),
            expected_revision: None,
            mode: manager_v1::IssueOperationResolutionMode::HumanArbitration as i32,
            arbitration_outcome: manager_v1::IssueOperationArbitrationOutcome::UnknownSettled
                as i32,
            uncertainty_acknowledged: true,
            arbitration_reason: "validate mandatory CAS presence".into(),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        missing_revision.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ManagerInvalidArgument as i32
    ));
}

#[tokio::test]
async fn primary_consumer_rejects_unproven_not_applied_without_changing_the_issue() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("failed", "failed")
        .await
        .unwrap();
    let operation = manager_v1::DurableOperation {
        operation_id: "failed-not-applied".into(),
        scope: Some(manager_v1::durable_operation::Scope::IssueId(
            issue.id.to_string(),
        )),
        method: "manager_reconcile_expired_claim".into(),
        state: manager_v1::DurableOperationState::Failed as i32,
        failure_class: manager_v1::FailureClass::Terminal as i32,
        generation: 1,
        affects_ready: false,
        terminal_snapshot: Some(
            manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(manager_v1::Snapshot {
                payload: manager_v1::ManagerError {
                    code: manager_v1::ManagerErrorCode::PreconditionFailed as i32,
                    message: "rejected before dispatch".into(),
                    retryable: false,
                    detail: None,
                }
                .encode_to_vec(),
                schema: "issue_tracker.manager.v1.ManagerError".into(),
            }),
        ),
        ..Default::default()
    };
    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .commit_mutation(
            &manager_v1::ManagerMutation {
                operation: Some(operation.clone()),
                ..Default::default()
            },
            &operation,
            None,
        )
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(manager_v1::ResolveIssueOperationArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_resolve_issue_operation".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint,
                data_root_id: identity.data_root_id,
                contract_fingerprint: identity.contract_fingerprint,
                idempotency_key: "confirm-not-applied".into(),
                correlation_id: "resolution-test".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "administrator".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    reason: Some("server evidence confirms no write".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            operation_id: operation.operation_id.clone(),
            expected_operation_generation: Some(operation.generation),
            expected_revision: Some(issue.revision),
            decision: manager_v1::IssueOperationResolutionDecision::ConfirmNotApplied as i32,
            evidence_refs: vec![format!(
                "manager-operation:{}:error-snapshot",
                operation.operation_id
            )],
            mode: manager_v1::IssueOperationResolutionMode::EvidenceConfirmation as i32,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(ref error))
            if error.code == manager_v1::ManagerErrorCode::ResolutionRequired as i32
    ));
    assert_eq!(
        server
            .app
            .manager
            .as_ref()
            .unwrap()
            .lock()
            .await
            .storage_mut()
            .repository()
            .get_operation(&operation.operation_id)
            .unwrap()
            .expect("source operation remains present")
            .terminal_snapshot,
        operation.terminal_snapshot
    );
    let current = server.app.core.tracker().get(issue.id).await.unwrap();
    assert_eq!(current.revision, issue.revision);
    assert_eq!(current.state, crate::tracker::IssueState::Open);
}

#[tokio::test]
async fn resolution_rejects_unsupported_method_before_admission() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("unsupported-resolution", "unsupported-resolution")
        .await
        .unwrap();
    let source = manager_v1::DurableOperation {
        operation_id: "unsupported-resolution-source".into(),
        scope: Some(manager_v1::durable_operation::Scope::IssueId(
            issue.id.to_string(),
        )),
        method: "manager_update_issue".into(),
        state: manager_v1::DurableOperationState::Unconfirmed as i32,
        resolution_required: true,
        generation: 1,
        ..Default::default()
    };
    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .commit_mutation(
            &manager_v1::ManagerMutation {
                operation: Some(source.clone()),
                ..Default::default()
            },
            &source,
            None,
        )
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(manager_v1::ResolveIssueOperationArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_resolve_issue_operation".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint,
                data_root_id: identity.data_root_id,
                contract_fingerprint: identity.contract_fingerprint,
                idempotency_key: "unsupported-resolution-request".into(),
                correlation_id: "unsupported-resolution-test".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "administrator".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    reason: Some("unsupported method must be rejected".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            operation_id: source.operation_id,
            expected_operation_generation: Some(source.generation),
            expected_revision: Some(issue.revision),
            decision: manager_v1::IssueOperationResolutionDecision::ConfirmApplied as i32,
            evidence_refs: vec!["unsupported-resolution-source".into()],
            mode: manager_v1::IssueOperationResolutionMode::EvidenceConfirmation as i32,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));
    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage()
        .read(|connection| {
            let resolutions: i64 = connection.query_row(
                "SELECT COUNT(*) FROM manager_resolution_request",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(resolutions, 0);
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn resolution_rejects_unspecified_mode_before_admission() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("invalid mode", "invalid mode")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(manager_v1::ResolveIssueOperationArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_resolve_issue_operation".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint,
                data_root_id: identity.data_root_id,
                contract_fingerprint: identity.contract_fingerprint,
                idempotency_key: "invalid-mode-request".into(),
                correlation_id: "invalid-mode-correlation".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "administrator".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    reason: Some("invalid mode must be rejected".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            operation_id: "invalid-mode-source".into(),
            expected_operation_generation: Some(1),
            expected_revision: Some(issue.revision),
            decision: manager_v1::IssueOperationResolutionDecision::ConfirmApplied as i32,
            mode: manager_v1::IssueOperationResolutionMode::Unspecified as i32,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ManagerInvalidArgument as i32
    ));
}

#[tokio::test]
async fn admin_and_resolution_reject_empty_correlation_before_reservation() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("missing correlation", "missing correlation")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let actor = Some(manager_v1::ActorAudit {
        actor_id: "administrator".into(),
        actor_kind: v1::ActorKind::Human as i32,
        reason: Some("input validation".into()),
        ..Default::default()
    });
    let admin_response = ManagerConsumerService::apply_issue_admin_action(
        &server,
        Request::new(manager_v1::ApplyIssueAdminActionArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_apply_issue_admin_action".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint.clone(),
                data_root_id: identity.data_root_id.clone(),
                contract_fingerprint: identity.contract_fingerprint.clone(),
                idempotency_key: "empty-correlation-admin".into(),
                correlation_id: " ".into(),
                actor_audit: actor.clone(),
            }),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            action: manager_v1::IssueAdminAction::Recover as i32,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        admin_response.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ManagerInvalidArgument as i32
    ));

    let resolution_response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(manager_v1::ResolveIssueOperationArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_resolve_issue_operation".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint.clone(),
                data_root_id: identity.data_root_id.clone(),
                contract_fingerprint: identity.contract_fingerprint,
                idempotency_key: "empty-correlation-resolution".into(),
                correlation_id: String::new(),
                actor_audit: actor,
            }),
            issue_id: issue.id.to_string(),
            operation_id: "missing-operation".into(),
            expected_operation_generation: Some(1),
            expected_revision: Some(issue.revision),
            mode: manager_v1::IssueOperationResolutionMode::HumanArbitration as i32,
            arbitration_outcome: manager_v1::IssueOperationArbitrationOutcome::UnknownSettled
                as i32,
            uncertainty_acknowledged: true,
            arbitration_reason: "input validation".into(),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        resolution_response.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ManagerInvalidArgument as i32
    ));

    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage()
        .read(|connection| {
            let reservations: i64 =
                connection.query_row("SELECT COUNT(*) FROM manager_idempotency", [], |row| {
                    row.get(0)
                })?;
            assert_eq!(reservations, 0);
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn primary_consumer_confirm_applied_restores_admin_receipt_and_replay() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("applied", "applied")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let admin_context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "confirm-applied-admin".into(),
        correlation_id: "resolution-test".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("recover confirmed by marker".into()),
            ..Default::default()
        }),
    };
    let args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(admin_context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        action: manager_v1::IssueAdminAction::Recover as i32,
        ..Default::default()
    };
    let scope = IdempotencyScope::new(
        admin_context.contract_fingerprint.clone(),
        admin_context.data_root_id.clone(),
        admin_context.method.clone(),
        admin_context.target.clone(),
        admin_context.idempotency_key.clone(),
    )
    .unwrap();
    let digest = digest_bytes(&args.encode_to_vec());
    let operation_id = "admin-confirm-applied".to_owned();
    server
        .app
        .admit_admin_action(
            &admin_context,
            &scope,
            digest.clone(),
            operation_id.clone(),
            issue.id.to_string(),
            manager_v1::IssueAdminAction::Recover,
            issue.revision,
            args.encode_to_vec(),
        )
        .await
        .unwrap();
    server
        .app
        .execute_saved_admin_action(&args, issue.id, &operation_id)
        .await
        .unwrap();
    server
        .app
        .mark_admin_action_unconfirmed(&scope, &digest, &operation_id)
        .await
        .unwrap();
    let source = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(&operation_id)
        .unwrap()
        .unwrap();

    let resolution_args = manager_v1::ResolveIssueOperationArgs {
        context: Some(manager_v1::ManagerWriteContext {
            method: "manager_resolve_issue_operation".into(),
            target: format!("issue:{}", issue.id),
            schema_fingerprint: identity.schema_fingerprint,
            data_root_id: identity.data_root_id,
            contract_fingerprint: identity.contract_fingerprint,
            idempotency_key: "confirm-applied-resolution".into(),
            correlation_id: "resolution-test".into(),
            actor_audit: Some(manager_v1::ActorAudit {
                actor_id: "administrator".into(),
                actor_kind: v1::ActorKind::Human as i32,
                reason: Some("marker proves administration applied".into()),
                ..Default::default()
            }),
        }),
        issue_id: issue.id.to_string(),
        operation_id: operation_id.clone(),
        expected_operation_generation: Some(source.generation),
        expected_revision: Some(issue.revision),
        decision: manager_v1::IssueOperationResolutionDecision::ConfirmApplied as i32,
        evidence_refs: vec![operation_id.clone()],
        mode: manager_v1::IssueOperationResolutionMode::EvidenceConfirmation as i32,
        ..Default::default()
    };
    let response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(resolution_args.clone()),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Success(_))
    ));
    let resolution_replay =
        ManagerConsumerService::resolve_issue_operation(&server, Request::new(resolution_args))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(
        response.encode_to_vec(),
        resolution_replay.encode_to_vec(),
        "a lost resolution response must replay its original receipt"
    );
    let source = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(&operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        source.state,
        manager_v1::DurableOperationState::Committed as i32
    );
    let Some(manager_v1::durable_operation::TerminalSnapshot::ResultSnapshot(snapshot)) =
        source.terminal_snapshot
    else {
        panic!("applied resolution must restore the administrative receipt");
    };
    let restored =
        manager_v1::ApplyIssueAdminActionResult::decode(snapshot.payload.as_slice()).unwrap();
    assert!(matches!(
        restored.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(_))
    ));
    let replay = server
        .app
        .admit_admin_action(
            &admin_context,
            &scope,
            digest,
            operation_id,
            issue.id.to_string(),
            manager_v1::IssueAdminAction::Recover,
            issue.revision,
            args.encode_to_vec(),
        )
        .await
        .unwrap();
    assert!(matches!(
        replay,
        Some(manager_v1::ApplyIssueAdminActionResult {
            outcome: Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(_))
        })
    ));
}

#[tokio::test]
async fn resolution_marker_absence_is_a_durable_failure_and_replays() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let app = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = app
        .core
        .tracker()
        .create("resolution-missing", "resolution-missing")
        .await
        .unwrap();
    let identity = app.manager_store_context().await.unwrap();
    let admin_context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "missing-marker-admin".into(),
        correlation_id: "resolution-missing-test".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("prepare an unresolved operation".into()),
            ..Default::default()
        }),
    };
    let admin_args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(admin_context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        action: manager_v1::IssueAdminAction::Recover as i32,
        ..Default::default()
    };
    let admin_scope = IdempotencyScope::new(
        admin_context.contract_fingerprint.clone(),
        admin_context.data_root_id.clone(),
        admin_context.method.clone(),
        admin_context.target.clone(),
        admin_context.idempotency_key.clone(),
    )
    .unwrap();
    let admin_digest = digest_bytes(&admin_args.encode_to_vec());
    let source_operation_id = "missing-marker-source";
    app.admit_admin_action(
        &admin_context,
        &admin_scope,
        admin_digest.clone(),
        source_operation_id.into(),
        issue.id.to_string(),
        manager_v1::IssueAdminAction::Recover,
        issue.revision,
        admin_args.encode_to_vec(),
    )
    .await
    .unwrap();
    // Close the private outbox without touching Tracker. This is the
    // durable state left when a southbound result is unknown and gives
    // Applied resolution a valid source operation but no marker.
    app.mark_admin_action_unconfirmed(&admin_scope, &admin_digest, source_operation_id)
        .await
        .unwrap();
    let source = app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();

    let resolution_context = manager_v1::ManagerWriteContext {
        method: "manager_resolve_issue_operation".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "missing-marker-resolution".into(),
        correlation_id: "resolution-missing-test".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("marker was not found".into()),
            ..Default::default()
        }),
    };
    let resolution_args = manager_v1::ResolveIssueOperationArgs {
        context: Some(resolution_context),
        issue_id: issue.id.to_string(),
        operation_id: source_operation_id.into(),
        expected_operation_generation: Some(source.generation),
        expected_revision: Some(issue.revision),
        decision: manager_v1::IssueOperationResolutionDecision::ConfirmApplied as i32,
        evidence_refs: vec![source_operation_id.into()],
        mode: manager_v1::IssueOperationResolutionMode::EvidenceConfirmation as i32,
        ..Default::default()
    };
    let server = GrpcServer::new(app);
    let response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(resolution_args.clone()),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(ref error))
            if error.code == manager_v1::ManagerErrorCode::ResolutionRequired as i32
    ));
    let replay =
        ManagerConsumerService::resolve_issue_operation(&server, Request::new(resolution_args))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(response.encode_to_vec(), replay.encode_to_vec());
    // Evidence confirmation is intentionally rejected above, but an
    // authorized human may still settle the same uncertain operation by
    // explicitly acknowledging that its outcome cannot be proven.
    let arbitration_context = manager_v1::ManagerWriteContext {
        method: "manager_resolve_issue_operation".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "missing-marker-arbitration".into(),
        correlation_id: "resolution-arbitration-test".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("operator accepts unresolved outcome".into()),
            ..Default::default()
        }),
    };
    let arbitration_args = manager_v1::ResolveIssueOperationArgs {
        context: Some(arbitration_context),
        issue_id: issue.id.to_string(),
        operation_id: source_operation_id.into(),
        expected_operation_generation: Some(source.generation),
        expected_revision: Some(issue.revision),
        mode: manager_v1::IssueOperationResolutionMode::HumanArbitration as i32,
        arbitration_outcome: manager_v1::IssueOperationArbitrationOutcome::UnknownSettled as i32,
        uncertainty_acknowledged: true,
        arbitration_reason: "operator accepts unresolved outcome".into(),
        ..Default::default()
    };
    let mut invalid_arbitration = arbitration_args.clone();
    invalid_arbitration
        .context
        .as_mut()
        .expect("arbitration context")
        .idempotency_key = "missing-marker-arbitration-evidence".into();
    invalid_arbitration.evidence_refs = vec![source_operation_id.into()];
    invalid_arbitration.arbitration_outcome =
        manager_v1::IssueOperationArbitrationOutcome::Applied as i32;
    let rejected =
        ManagerConsumerService::resolve_issue_operation(&server, Request::new(invalid_arbitration))
            .await
            .unwrap()
            .into_inner();
    assert!(matches!(
        rejected.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ResolutionRequired as i32
    ));
    let arbitration = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(arbitration_args.clone()),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        arbitration.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Success(_))
    ));
    let arbitration_replay =
        ManagerConsumerService::resolve_issue_operation(&server, Request::new(arbitration_args))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(
        arbitration.encode_to_vec(),
        arbitration_replay.encode_to_vec()
    );
    let source = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        source.state,
        manager_v1::DurableOperationState::Arbitrated as i32,
        "human arbitration must settle the source operation"
    );
    let resolution_id = match arbitration.outcome.as_ref() {
        Some(manager_v1::resolve_issue_operation_result::Outcome::Success(mutation)) => {
            match mutation.result.as_ref() {
                Some(manager_v1::manager_mutation::Result::IssueOperationResolution(receipt)) => {
                    assert_eq!(
                        receipt.decision,
                        manager_v1::IssueOperationResolutionDecision::Unspecified as i32,
                        "arbitration must not masquerade as evidence confirmation"
                    );
                    assert_eq!(
                        receipt.arbitration_outcome,
                        manager_v1::IssueOperationArbitrationOutcome::UnknownSettled as i32
                    );
                    receipt.operation_id.as_str()
                }
                _ => panic!("arbitration response has no typed receipt"),
            }
        }
        _ => panic!("arbitration response is not successful"),
    };
    assert_eq!(
        source
            .diagnosis
            .as_ref()
            .and_then(|diagnosis| diagnosis.resolution_operation_id.as_deref()),
        Some(resolution_id),
        "source operation must expose the resolution operation reference"
    );
    assert!(
        server
            .app
            .manager
            .as_ref()
            .unwrap()
            .lock()
            .await
            .storage_mut()
            .repository()
            .pending_resolution_requests(10)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn human_arbitration_can_settle_a_received_operation() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("received arbitration", "received arbitration")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let admin_context = manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "received-arbitration-admin".into(),
        correlation_id: "received-arbitration".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("settle an interrupted admission".into()),
            ..Default::default()
        }),
    };
    let admin_args = manager_v1::ApplyIssueAdminActionArgs {
        context: Some(admin_context.clone()),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        action: manager_v1::IssueAdminAction::Recover as i32,
        ..Default::default()
    };
    let admin_scope = IdempotencyScope::new(
        admin_context.contract_fingerprint.clone(),
        admin_context.data_root_id.clone(),
        admin_context.method.clone(),
        admin_context.target.clone(),
        admin_context.idempotency_key.clone(),
    )
    .unwrap();
    let admin_digest = digest_bytes(&admin_args.encode_to_vec());
    let source_operation_id = "received-arbitration-source";
    server
        .app
        .admit_admin_action(
            &admin_context,
            &admin_scope,
            admin_digest,
            source_operation_id.into(),
            issue.id.to_string(),
            manager_v1::IssueAdminAction::Recover,
            issue.revision,
            admin_args.encode_to_vec(),
        )
        .await
        .unwrap();
    let source = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        source.state,
        manager_v1::DurableOperationState::Received as i32
    );
    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&OwnershipRecoveryState {
            issue_id: issue.id.to_string(),
            expected_revision: issue.revision,
            readmission_number: 0,
            current_operation_id: Some(source_operation_id.into()),
            in_flight: true,
            consecutive_failures: 0,
            next_attempt_at_millis: None,
            last_failure_class: Some("UNCONFIRMED_OPERATION".into()),
            stopped: true,
            stop_basis: Some("operation outcome cannot be reconstructed".into()),
            repair_condition: Some("human arbitration".into()),
            cursor_diagnostic: None,
        })
        .unwrap();

    let arbitration_args = manager_v1::ResolveIssueOperationArgs {
        context: Some(manager_v1::ManagerWriteContext {
            method: "manager_resolve_issue_operation".into(),
            target: format!("issue:{}", issue.id),
            schema_fingerprint: identity.schema_fingerprint.clone(),
            data_root_id: identity.data_root_id.clone(),
            contract_fingerprint: identity.contract_fingerprint.clone(),
            idempotency_key: "received-arbitration-resolution".into(),
            correlation_id: "received-arbitration-resolution".into(),
            actor_audit: Some(manager_v1::ActorAudit {
                actor_id: "administrator".into(),
                actor_kind: v1::ActorKind::Human as i32,
                reason: Some("operation outcome cannot be reconstructed".into()),
                ..Default::default()
            }),
        }),
        issue_id: issue.id.to_string(),
        operation_id: source_operation_id.into(),
        expected_operation_generation: Some(source.generation),
        expected_revision: Some(issue.revision),
        mode: manager_v1::IssueOperationResolutionMode::HumanArbitration as i32,
        arbitration_outcome: manager_v1::IssueOperationArbitrationOutcome::UnknownSettled as i32,
        uncertainty_acknowledged: true,
        arbitration_reason: "operation outcome cannot be reconstructed".into(),
        ..Default::default()
    };
    let resolution = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(arbitration_args.clone()),
    )
    .await
    .unwrap()
    .into_inner();
    let resolution_mutation = match resolution.outcome.as_ref() {
        Some(manager_v1::resolve_issue_operation_result::Outcome::Success(mutation)) => mutation,
        outcome => panic!("unexpected arbitration outcome: {outcome:?}"),
    };
    let resolution_receipt = match resolution_mutation.result.as_ref() {
        Some(manager_v1::manager_mutation::Result::IssueOperationResolution(receipt)) => receipt,
        result => panic!("arbitration has no typed receipt: {result:?}"),
    };
    assert!(
        resolution_receipt
            .remaining_uncertainties
            .iter()
            .any(|uncertainty| uncertainty == "ORIGINAL_RESULT_UNAVAILABLE")
    );
    assert_eq!(
        resolution_receipt.previous_replay_snapshot, None,
        "a received operation has no historical replay payload"
    );

    let replay =
        ManagerConsumerService::apply_issue_admin_action(&server, Request::new(admin_args))
            .await
            .unwrap()
            .into_inner();
    assert!(matches!(
        replay.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(
            _
        ))
    ));
    let source = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        source.state,
        manager_v1::DurableOperationState::Arbitrated as i32
    );
    let recovery_state = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .ownership_recovery_state(&issue.id.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(recovery_state.readmission_number, 1);
    assert!(!recovery_state.in_flight);
    assert!(recovery_state.current_operation_id.is_none());
    assert!(!recovery_state.stopped);
}

#[tokio::test]
async fn human_arbitration_can_settle_a_pending_resolution_operation() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create(
            "pending resolution arbitration",
            "pending resolution arbitration",
        )
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let source_context = manager_v1::ManagerWriteContext {
        method: "manager_resolve_issue_operation".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        data_root_id: identity.data_root_id.clone(),
        contract_fingerprint: identity.contract_fingerprint.clone(),
        idempotency_key: "pending-resolution-source-key".into(),
        correlation_id: "pending-resolution-source-correlation".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "administrator".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("admit an interrupted resolution request".into()),
            ..Default::default()
        }),
    };
    let source_scope = IdempotencyScope::new(
        source_context.contract_fingerprint.clone(),
        source_context.data_root_id.clone(),
        source_context.method.clone(),
        source_context.target.clone(),
        source_context.idempotency_key.clone(),
    )
    .unwrap();
    let source_request = manager_v1::ResolveIssueOperationArgs {
        context: Some(source_context.clone()),
        issue_id: issue.id.to_string(),
        operation_id: "missing-original-operation".into(),
        expected_operation_generation: Some(1),
        expected_revision: Some(issue.revision),
        decision: manager_v1::IssueOperationResolutionDecision::ConfirmApplied as i32,
        evidence_refs: vec!["missing-original-operation".into()],
        mode: manager_v1::IssueOperationResolutionMode::EvidenceConfirmation as i32,
        ..Default::default()
    };
    let source_request_bytes = source_request.encode_to_vec();
    let source_digest = digest_bytes(&source_request_bytes);
    let source_operation_id = "pending-resolution-source-operation";
    let source_operation = {
        let manager = server.app.manager.as_ref().unwrap().lock().await;
        let config = manager.config().clone();
        let generation = manager.storage().generation().checked_add(1).unwrap();
        let sequence = manager.storage().manager_sequence();
        NewOperation {
            operation_id: source_operation_id.into(),
            scope: Some(OperationScope::Issue(issue.id.to_string())),
            method: "manager_resolve_issue_operation".into(),
            idempotency_scope: source_scope.clone(),
            request_digest: source_digest.clone(),
            request_schema: "issue_tracker.manager.v1.ResolveIssueOperationArgs".into(),
            downstream_operation_id: None,
            generation,
            manager_sequence: sequence,
            causal_parent: Some("missing-original-operation".into()),
            affects_ready: false,
            sync_subject: Some(issue.id.to_string()),
            affected_issue_ids: vec![issue.id.to_string()],
            actor_audit: source_context.actor_audit.clone(),
            contract_fingerprint: config.manager_identity().contract_fingerprint.clone(),
            schema_fingerprint: config.manager_identity().schema_fingerprint.clone(),
            correlation_id: source_context.correlation_id.clone(),
            now_millis: unix_now_millis(),
            replayable: true,
        }
        .build()
        .unwrap()
    };
    {
        let mut manager = server.app.manager.as_ref().unwrap().lock().await;
        manager
            .storage_mut()
            .repository()
            .commit_resolution_admission(
                &source_operation,
                "missing-original-operation",
                &issue.id.to_string(),
                1,
                issue.revision,
                manager_v1::IssueOperationResolutionDecision::ConfirmApplied as i32,
                &source_request_bytes,
            )
            .unwrap();
    }

    let source = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        source.state,
        manager_v1::DurableOperationState::Received as i32
    );

    let arbitration_args = manager_v1::ResolveIssueOperationArgs {
        context: Some(manager_v1::ManagerWriteContext {
            method: "manager_resolve_issue_operation".into(),
            target: format!("issue:{}", issue.id),
            schema_fingerprint: identity.schema_fingerprint.clone(),
            data_root_id: identity.data_root_id.clone(),
            contract_fingerprint: identity.contract_fingerprint.clone(),
            idempotency_key: "pending-resolution-arbitration-key".into(),
            correlation_id: "pending-resolution-arbitration-correlation".into(),
            actor_audit: Some(manager_v1::ActorAudit {
                actor_id: "administrator".into(),
                actor_kind: v1::ActorKind::Human as i32,
                reason: Some("settle the interrupted resolution request".into()),
                ..Default::default()
            }),
        }),
        issue_id: issue.id.to_string(),
        operation_id: source_operation_id.into(),
        expected_operation_generation: Some(source.generation),
        expected_revision: Some(issue.revision),
        mode: manager_v1::IssueOperationResolutionMode::HumanArbitration as i32,
        arbitration_outcome: manager_v1::IssueOperationArbitrationOutcome::UnknownSettled as i32,
        uncertainty_acknowledged: true,
        arbitration_reason: "the resolution request itself cannot be reconstructed".into(),
        ..Default::default()
    };
    let response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(arbitration_args.clone()),
    )
    .await
    .unwrap()
    .into_inner();
    let receipt = match response.outcome.as_ref() {
        Some(manager_v1::resolve_issue_operation_result::Outcome::Success(mutation)) => {
            match mutation.result.as_ref() {
                Some(manager_v1::manager_mutation::Result::IssueOperationResolution(receipt)) => {
                    receipt
                }
                result => panic!("arbitration has no typed receipt: {result:?}"),
            }
        }
        outcome => panic!("unexpected arbitration outcome: {outcome:?}"),
    };
    assert_eq!(
        receipt.source_operation_id, source_operation_id,
        "the resolution request itself must be the arbitrated source"
    );
    assert_eq!(
        receipt.arbitration_outcome,
        manager_v1::IssueOperationArbitrationOutcome::UnknownSettled as i32
    );

    let replay =
        ManagerConsumerService::resolve_issue_operation(&server, Request::new(arbitration_args))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(response.encode_to_vec(), replay.encode_to_vec());

    let source = server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .get_operation(source_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        source.state,
        manager_v1::DurableOperationState::Arbitrated as i32
    );
    assert!(!source.resolution_required);
}

#[tokio::test]
async fn administrative_cancellation_rejects_a_non_human_actor_without_mutation() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("admin", "admin")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let response = ManagerConsumerService::apply_issue_admin_action(
        &server,
        Request::new(manager_v1::ApplyIssueAdminActionArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_apply_issue_admin_action".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint,
                data_root_id: identity.data_root_id,
                contract_fingerprint: identity.contract_fingerprint,
                idempotency_key: "external-cancel".into(),
                correlation_id: "admin-test".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "untrusted".into(),
                    actor_kind: v1::ActorKind::ExternalAgent as i32,
                    reason: Some("attempted cancellation".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            action: manager_v1::IssueAdminAction::Cancel as i32,
            terminal_report: Some(
                manager_v1::apply_issue_admin_action_args::TerminalReport::Cancellation(
                    v1::CancellationReport {
                        classification: v1::CancellationClass::Other as i32,
                    },
                ),
            ),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        response.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(
            _
        ))
    ));
    let system_identity = server.app.manager_store_context().await.unwrap();
    let system_response = ManagerConsumerService::apply_issue_admin_action(
        &server,
        Request::new(manager_v1::ApplyIssueAdminActionArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_apply_issue_admin_action".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: system_identity.schema_fingerprint.clone(),
                data_root_id: system_identity.data_root_id.clone(),
                contract_fingerprint: system_identity.contract_fingerprint.clone(),
                idempotency_key: "external-system-recover".into(),
                correlation_id: "admin-system-recover".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "system-maintenance".into(),
                    actor_kind: v1::ActorKind::System as i32,
                    reason: Some("self-asserted maintenance".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            action: manager_v1::IssueAdminAction::Recover as i32,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        system_response.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ManagerInvalidArgument as i32
    ));
    let system_resolution_response = ManagerConsumerService::resolve_issue_operation(
        &server,
        Request::new(manager_v1::ResolveIssueOperationArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_resolve_issue_operation".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: system_identity.schema_fingerprint.clone(),
                data_root_id: system_identity.data_root_id.clone(),
                contract_fingerprint: system_identity.contract_fingerprint,
                idempotency_key: "external-system-resolution".into(),
                correlation_id: "admin-system-resolution".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "system-maintenance".into(),
                    actor_kind: v1::ActorKind::System as i32,
                    reason: Some("self-asserted arbitration".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            operation_id: "missing-operation".into(),
            expected_operation_generation: Some(1),
            expected_revision: Some(issue.revision),
            mode: manager_v1::IssueOperationResolutionMode::HumanArbitration as i32,
            arbitration_outcome: manager_v1::IssueOperationArbitrationOutcome::UnknownSettled
                as i32,
            uncertainty_acknowledged: true,
            arbitration_reason: "self-asserted arbitration".into(),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(matches!(
        system_resolution_response.outcome,
        Some(manager_v1::resolve_issue_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));
    let current = server.app.core.tracker().get(issue.id).await.unwrap();
    assert_eq!(current.state, crate::tracker::IssueState::Open);
    assert_eq!(current.revision, issue.revision);
}

#[tokio::test]
async fn manager_consumer_registers_every_consumer_boundary_method() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());

    macro_rules! registered {
        ($method:ident, $args:ty) => {
            let _ =
                ManagerConsumerService::$method(&server, Request::new(<$args>::default())).await;
        };
    }

    registered!(resolve_candidate, manager_v1::ResolveCandidateArgs);
    registered!(get_operation, manager_v1::GetOperationArgs);
    registered!(list_operation_history, manager_v1::ListOperationHistoryArgs);
    registered!(get_issue_with_sync, manager_v1::GetIssueWithSyncArgs);
    registered!(get_issue_relations, manager_v1::GetIssueRelationsArgs);
    registered!(list_issues_with_sync, manager_v1::ListIssuesWithSyncArgs);
    registered!(get_candidate, manager_v1::GetCandidateArgs);
    registered!(list_candidates, manager_v1::ListCandidatesArgs);
    registered!(list_review_candidates, manager_v1::ListReviewCandidatesArgs);
    registered!(get_extraction_run, manager_v1::GetExtractionRunArgs);
    registered!(list_extraction_runs, manager_v1::ListExtractionRunsArgs);
    registered!(create_issue, manager_v1::CreateIssueArgs);
    registered!(transition_issue, manager_v1::TransitionIssueArgs);
    registered!(update_candidate, manager_v1::UpdateCandidateArgs);
    registered!(add_issue_relation, manager_v1::AddIssueRelationArgs);
    registered!(
        apply_issue_admin_action,
        manager_v1::ApplyIssueAdminActionArgs
    );
    registered!(
        resolve_issue_operation,
        manager_v1::ResolveIssueOperationArgs
    );
}

#[test]
fn public_rpc_services_expose_exactly_the_allowed_methods() {
    let descriptor =
        prost_types::FileDescriptorSet::decode(issue_tracker_proto::FILE_DESCRIPTOR_SET)
            .expect("generated protobuf descriptor must decode");

    assert_service_methods(
        &descriptor,
        "issue_tracker.rpc.v1.ManagerConsumerService",
        [
            "ResolveCandidate",
            "GetOperation",
            "ListOperationHistory",
            "GetIssueWithSync",
            "GetIssueRelations",
            "ListIssuesWithSync",
            "GetCandidate",
            "ListCandidates",
            "ListReviewCandidates",
            "GetExtractionRun",
            "ListExtractionRuns",
            "CreateIssue",
            "UpdateIssue",
            "TransitionIssue",
            "UpdateCandidate",
            "AddIssueRelation",
            "ApplyIssueAdminAction",
            "ResolveIssueOperation",
            "ListOwnershipDiagnostics",
        ],
    );
    assert_service_methods(
        &descriptor,
        "issue_tracker.rpc.v1.IssueExtract",
        ["ListExtractionHints", "ApplyExtraction"],
    );
    assert_service_methods(
        &descriptor,
        "issue_tracker.rpc.v1.ManagerInfoService",
        ["GetContractInfo"],
    );
    assert_service_methods(
        &descriptor,
        "issue_tracker.rpc.v1.OutboxMaintenanceService",
        ["RunOutboxCycle"],
    );
    assert_service_methods(
        &descriptor,
        "issue_tracker.rpc.v1.GatewayControlService",
        ["RegisterGateway", "GetGatewayRegistration"],
    );
    assert_service_methods(
        &descriptor,
        "issue_tracker.rpc.v1.IssueGateway",
        [
            "ListReady",
            "Claim",
            "Heartbeat",
            "Release",
            "Transition",
            "CompleteExecutionToAwaitingReview",
        ],
    );
}

fn assert_service_methods<const N: usize>(
    descriptor: &prost_types::FileDescriptorSet,
    service_name: &str,
    expected: [&str; N],
) {
    let (package, name) = service_name
        .rsplit_once('.')
        .expect("fully-qualified service name");
    let service = descriptor
        .file
        .iter()
        .filter(|file| file.package.as_deref() == Some(package))
        .flat_map(|file| file.service.iter())
        .find(|service| service.name.as_deref() == Some(name))
        .expect("public service descriptor must exist");
    let actual = service
        .method
        .iter()
        .filter_map(|method| method.name.as_deref())
        .collect::<HashSet<_>>();
    assert_eq!(actual, expected.into_iter().collect());
}

async fn start_test_listener(
    routes: tonic::service::Routes,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener
        .local_addr()
        .expect("read primary listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_routes(routes)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    (address, shutdown_tx, task)
}

#[tokio::test]
async fn worker_listener_exposes_only_v1_gateway_reflection() {
    use tonic_reflection::pb::v1::{
        ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
    };

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let (address, shutdown, task) =
        start_test_listener(gateway_worker_routes(server).expect("worker reflection routes")).await;
    let mut reflection = ServerReflectionClient::new(test_channel(address).await);
    let response = reflection
        .server_reflection_info(Request::new(tokio_stream::iter(vec![
            ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::ListServices(String::new())),
            },
        ])))
        .await
        .expect("v1 reflection route must be available");
    let response = response
        .into_inner()
        .message()
        .await
        .expect("reflection stream must succeed")
        .expect("reflection response must be present");
    let MessageResponse::ListServicesResponse(services) =
        response.message_response.expect("list-services response")
    else {
        panic!("reflection must return services");
    };
    let names = services
        .service
        .iter()
        .map(|service| service.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert!(names.contains("issue_tracker.rpc.v1.IssueGateway"));
    assert!(!names.contains("issue_tracker.rpc.v1.ManagerConsumerService"));

    let response = reflection
        .server_reflection_info(Request::new(tokio_stream::iter(vec![
            ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::FileContainingSymbol(
                    "issue_tracker.rpc.v1.IssueGateway".into(),
                )),
            },
        ])))
        .await
        .expect("gateway descriptor lookup must be available")
        .into_inner()
        .message()
        .await
        .expect("gateway descriptor stream must succeed")
        .expect("gateway descriptor response must be present");
    let MessageResponse::FileDescriptorResponse(descriptors) = response
        .message_response
        .expect("gateway descriptor response")
    else {
        panic!("gateway symbol lookup must return descriptors");
    };
    let descriptor = descriptors
        .file_descriptor_proto
        .iter()
        .map(|encoded| prost_types::FileDescriptorProto::decode(encoded.as_ref()))
        .collect::<Result<Vec<_>, _>>()
        .expect("gateway descriptor payload must decode");
    let gateway = descriptor
        .iter()
        .flat_map(|file| file.service.iter())
        .find(|service| service.name.as_deref() == Some("IssueGateway"))
        .expect("worker reflection must return IssueGateway descriptor");
    assert_eq!(
        gateway
            .method
            .iter()
            .filter_map(|method| method.name.as_deref())
            .collect::<HashSet<_>>(),
        HashSet::from([
            "ListReady",
            "Claim",
            "Heartbeat",
            "Release",
            "Transition",
            "CompleteExecutionToAwaitingReview",
        ])
    );
    assert!(
        descriptor
            .iter()
            .flat_map(|file| file.service.iter())
            .all(|service| { service.name.as_deref() != Some("ManagerConsumerService") })
    );

    let mut legacy_reflection =
        tonic_reflection::pb::v1alpha::server_reflection_client::ServerReflectionClient::new(
            test_channel(address).await,
        );
    let error = legacy_reflection
            .server_reflection_info(Request::new(tokio_stream::iter(vec![
                tonic_reflection::pb::v1alpha::ServerReflectionRequest {
                    host: String::new(),
                    message_request: Some(
                        tonic_reflection::pb::v1alpha::server_reflection_request::MessageRequest::ListServices(String::new()),
                    ),
                },
            ])))
            .await
            .expect_err("worker listener must not publish legacy v1alpha reflection");
    assert_eq!(error.code(), Code::Unimplemented);
    shutdown_test_listener(shutdown, task).await;
}

#[tokio::test]
async fn primary_publishes_scoped_v1_reflection_only() {
    use tonic_reflection::pb::v1::{
        ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
    };

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let primary = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let control = primary.clone();
    let (primary_address, primary_shutdown, primary_task) =
        start_test_listener(primary_routes(primary)).await;
    let (control_address, control_shutdown, control_task) =
        start_test_listener(gateway_control_routes(control)).await;

    let mut reflection = ServerReflectionClient::new(test_channel(primary_address).await);
    let mut services = reflection
        .server_reflection_info(Request::new(tokio_stream::iter(vec![
            ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::ListServices(String::new())),
            },
        ])))
        .await
        .expect("primary listener must publish v1 reflection")
        .into_inner();
    let response = services
        .message()
        .await
        .expect("v1 reflection list response")
        .expect("v1 reflection must return a list response");
    let names = match response.message_response {
        Some(MessageResponse::ListServicesResponse(services)) => services
            .service
            .into_iter()
            .map(|service| service.name)
            .collect::<HashSet<_>>(),
        other => panic!("unexpected v1 reflection response: {other:?}"),
    };
    assert!(names.contains("issue_tracker.rpc.v1.IssueExtract"));
    assert!(names.contains("issue_tracker.rpc.v1.ManagerConsumerService"));
    assert!(names.contains("issue_tracker.rpc.v1.ManagerInfoService"));
    assert!(names.contains("issue_tracker.rpc.v1.OutboxMaintenanceService"));
    assert!(names.contains("issue_tracker.rpc.v1.ManagerTrustedReadService"));
    assert!(!names.contains("issue_tracker.rpc.v1.IssueGateway"));
    assert!(!names.contains("issue_tracker.rpc.v1.GatewayControlService"));
    assert_eq!(
        names,
        HashSet::from([
            "grpc.reflection.v1.ServerReflection".to_owned(),
            "issue_tracker.rpc.v1.IssueExtract".to_owned(),
            "issue_tracker.rpc.v1.ManagerConsumerService".to_owned(),
            "issue_tracker.rpc.v1.ManagerTrustedReadService".to_owned(),
            "issue_tracker.rpc.v1.ManagerInfoService".to_owned(),
            "issue_tracker.rpc.v1.OutboxMaintenanceService".to_owned(),
        ])
    );

    let mut reflection = ServerReflectionClient::new(test_channel(primary_address).await);
    let mut descriptor = reflection
        .server_reflection_info(Request::new(tokio_stream::iter(vec![
            ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::FileContainingSymbol(
                    "issue_tracker.rpc.v1.IssueExtract".into(),
                )),
            },
        ])))
        .await
        .expect("primary v1 reflection must resolve IssueExtract")
        .into_inner();
    let response = descriptor
        .message()
        .await
        .expect("v1 descriptor response")
        .expect("v1 reflection must return a descriptor response");
    let files = match response.message_response {
        Some(MessageResponse::FileDescriptorResponse(response)) => response
            .file_descriptor_proto
            .iter()
            .map(|encoded| prost_types::FileDescriptorProto::decode(encoded.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .expect("v1 descriptor payload must decode"),
        other => panic!("unexpected v1 descriptor response: {other:?}"),
    };
    let services = files
        .iter()
        .flat_map(|file| file.service.iter())
        .collect::<Vec<_>>();
    assert_eq!(
        services
            .iter()
            .filter_map(|service| service.name.as_deref())
            .collect::<HashSet<_>>(),
        HashSet::from(["IssueExtract"])
    );
    assert_eq!(
        services[0]
            .method
            .iter()
            .filter_map(|method| method.name.as_deref())
            .collect::<HashSet<_>>(),
        HashSet::from(["ListExtractionHints", "ApplyExtraction"])
    );

    let mut reflection = ServerReflectionClient::new(test_channel(primary_address).await);
    let mut descriptor = reflection
        .server_reflection_info(Request::new(tokio_stream::iter(vec![
            ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::FileContainingSymbol(
                    "issue_tracker.rpc.v1.ManagerConsumerService".into(),
                )),
            },
        ])))
        .await
        .expect("primary v1 reflection must resolve ManagerConsumerService")
        .into_inner();
    let response = descriptor
        .message()
        .await
        .expect("manager consumer descriptor response")
        .expect("v1 reflection must return a manager consumer descriptor");
    let files = match response.message_response {
        Some(MessageResponse::FileDescriptorResponse(response)) => response
            .file_descriptor_proto
            .iter()
            .map(|encoded| prost_types::FileDescriptorProto::decode(encoded.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .expect("manager consumer descriptor payload must decode"),
        other => panic!("unexpected manager consumer descriptor response: {other:?}"),
    };
    assert!(
        files
            .iter()
            .flat_map(|file| file.service.iter())
            .any(|service| {
                service.name.as_deref() == Some("ManagerConsumerService")
                    && service
                        .method
                        .iter()
                        .any(|method| method.name.as_deref() == Some("UpdateIssue"))
            })
    );

    let mut reflection = ServerReflectionClient::new(test_channel(primary_address).await);
    let mut descriptor = reflection
        .server_reflection_info(Request::new(tokio_stream::iter(vec![
            ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::FileContainingSymbol(
                    "issue_tracker.rpc.v1.ManagerTrustedReadService".into(),
                )),
            },
        ])))
        .await
        .expect("primary reflection must resolve ManagerTrustedReadService")
        .into_inner();
    let response = descriptor
        .message()
        .await
        .expect("trusted-read descriptor response")
        .expect("reflection must return the trusted-read descriptor");
    let files = match response.message_response {
        Some(MessageResponse::FileDescriptorResponse(response)) => response
            .file_descriptor_proto
            .iter()
            .map(|encoded| prost_types::FileDescriptorProto::decode(encoded.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .expect("trusted-read descriptor payload must decode"),
        other => panic!("unexpected trusted-read descriptor response: {other:?}"),
    };
    let trusted_read_service = files
        .iter()
        .flat_map(|file| file.service.iter())
        .find(|service| service.name.as_deref() == Some("ManagerTrustedReadService"))
        .expect("primary reflection must expose the trusted-read service");
    assert_eq!(
        trusted_read_service
            .method
            .iter()
            .filter_map(|method| method.name.as_deref())
            .collect::<HashSet<_>>(),
        HashSet::from(["ListIssueHistory", "GetDownstreamOperation"])
    );

    for (listener, address) in [("control", control_address)] {
        let mut consumer = crate::proto_types::rpc_v1::manager_consumer_service_client::ManagerConsumerServiceClient::new(
            test_channel(address).await,
        );
        assert_eq!(
            consumer
                .update_issue(manager_v1::UpdateIssueArgs::default())
                .await
                .expect_err("gateway control listener must not expose ManagerConsumerService")
                .code(),
            Code::Unimplemented
        );

        let mut reflection = ServerReflectionClient::new(test_channel(address).await);
        let error = reflection
            .server_reflection_info(Request::new(tokio_stream::iter(vec![
                ServerReflectionRequest {
                    host: String::new(),
                    message_request: Some(MessageRequest::ListServices(String::new())),
                },
            ])))
            .await
            .expect_err(&format!(
                "{listener} listener must not publish v1 reflection"
            ));
        assert_eq!(error.code(), Code::Unimplemented);
    }

    let mut legacy_reflection =
        tonic_reflection::pb::v1alpha::server_reflection_client::ServerReflectionClient::new(
            test_channel(primary_address).await,
        );
    let error = legacy_reflection
            .server_reflection_info(Request::new(tokio_stream::iter(vec![
                tonic_reflection::pb::v1alpha::ServerReflectionRequest {
                    host: String::new(),
                    message_request: Some(
                        tonic_reflection::pb::v1alpha::server_reflection_request::MessageRequest::ListServices(String::new()),
                    ),
                },
            ])))
            .await
            .expect_err("primary listener must not publish legacy v1alpha reflection");
    assert_eq!(error.code(), Code::Unimplemented);

    shutdown_test_listener(primary_shutdown, primary_task).await;
    shutdown_test_listener(control_shutdown, control_task).await;
}

#[tokio::test]
async fn primary_trusted_reads_bind_native_history_and_operation_to_manager_identity() {
    use crate::proto_types::rpc_v1::{
        activity_service_client::ActivityServiceClient,
        manager_consumer_service_client::ManagerConsumerServiceClient,
        manager_info_service_client::ManagerInfoServiceClient,
        manager_trusted_read_service_client::ManagerTrustedReadServiceClient,
    };

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let application = IssueTrackerApplication::open(configured).await.unwrap();
    let issue_id = uuid::Uuid::new_v4();
    let created_issue = application
        .core
        .tracker()
        .create_with_draft(
            issue_id,
            "primary-trusted-read-fixture",
            &v1::IssueDraft {
                title: "trusted read fixture".into(),
                kind: v1::IssueKind::Task as i32,
                priority: v1::IssuePriority::P2 as i32,
                creation_basis: v1::CreationBasis::Explicit as i32,
                confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                ..Default::default()
            },
            "test",
            "primary-trusted-read-fixture",
        )
        .await
        .expect("create canonical issue for trusted reads");
    let server = GrpcServer::new(application);
    let direct_read_server = server.clone();
    let (address, shutdown, task) = start_test_listener(primary_routes(server)).await;
    let channel = test_channel(address).await;

    let contract = match ManagerInfoServiceClient::new(channel.clone())
        .get_contract_info(manager_v1::GetContractInfoArgs::default())
        .await
        .expect("primary contract lookup must be available")
        .into_inner()
        .outcome
    {
        Some(manager_v1::get_contract_info_result::Outcome::Success(contract)) => contract,
        outcome => panic!("primary contract lookup failed: {outcome:?}"),
    };
    let data_root_id = contract
        .store_identities
        .iter()
        .find(|identity| identity.role == manager_v1::StoreRole::Manager as i32)
        .expect("contract must include the Manager identity")
        .data_root_id
        .clone();
    let context = manager_v1::ManagerReadContext {
        correlation_id: "primary-trusted-read-test".into(),
        schema_fingerprint: contract.schema_fingerprint.clone(),
        data_root_id: data_root_id.clone(),
        contract_fingerprint: contract.contract_fingerprint.clone(),
    };

    let mut trusted_reads = ManagerTrustedReadServiceClient::new(channel.clone());
    let pre_transition_history = trusted_reads
        .list_issue_history(manager_v1::ListIssueHistoryArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            page: Some(manager_v1::PageRequest {
                page_size: 100,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("history must be readable before a Manager operation exists")
        .into_inner();
    let pre_transition_history = match pre_transition_history.outcome {
        Some(manager_v1::list_issue_history_result::Outcome::Success(history)) => history,
        outcome => panic!("pre-transition Issue history read failed: {outcome:?}"),
    };
    assert!(
        !pre_transition_history.entries.is_empty(),
        "the pre-transition Issue history must be returned"
    );

    let mut consumer = ManagerConsumerServiceClient::new(channel.clone());
    let transition = consumer
        .transition_issue(manager_v1::TransitionIssueArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_transition_issue".into(),
                target: format!("issue:{issue_id}"),
                schema_fingerprint: contract.schema_fingerprint.clone(),
                idempotency_key: "primary-trusted-read-transition".into(),
                correlation_id: "primary-trusted-read-test".into(),
                data_root_id: data_root_id.clone(),
                contract_fingerprint: contract.contract_fingerprint.clone(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "test-client".into(),
                    actor_kind: v1::ActorKind::InternalAgent as i32,
                    ..Default::default()
                }),
            }),
            issue_id: issue_id.to_string(),
            expected_revision: Some(created_issue.revision),
            target_state: v1::IssueState::Blocked as i32,
            ..Default::default()
        })
        .await
        .expect("normal Manager transition must complete")
        .into_inner();
    let operation = match transition.outcome {
        Some(manager_v1::transition_issue_result::Outcome::Success(mutation)) => mutation
            .operation
            .expect("committed transition must return its durable Manager operation"),
        outcome => panic!("Manager transition failed: {outcome:?}"),
    };
    assert_eq!(
        manager_v1::DurableOperationState::try_from(operation.state),
        Ok(manager_v1::DurableOperationState::Committed)
    );
    assert_eq!(operation.manager_sequence, 0);
    assert!(operation.commit_snapshot.is_none());
    let downstream_operation_id = operation.downstream_operation_id.clone();
    assert!(!downstream_operation_id.is_empty());

    let bound_history = trusted_reads
        .list_issue_history(manager_v1::ListIssueHistoryArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            page: Some(manager_v1::PageRequest {
                page_size: 100,
                ..Default::default()
            }),
            manager_operation_id: Some(operation.operation_id.clone()),
            downstream_operation_id: Some(downstream_operation_id.clone()),
        })
        .await
        .expect("bound history read must succeed")
        .into_inner();
    let bound_history = match bound_history.outcome {
        Some(manager_v1::list_issue_history_result::Outcome::Success(history)) => history,
        outcome => panic!("bound Issue history read failed: {outcome:?}"),
    };
    assert!(
        bound_history
            .entries
            .iter()
            .any(|entry| { entry.downstream_operation_id == downstream_operation_id })
    );
    assert!(
        bound_history
            .entries
            .iter()
            .any(|entry| entry.downstream_operation_id != downstream_operation_id)
    );

    let downstream = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("bound downstream-operation read must succeed")
        .into_inner();
    let downstream = match downstream.outcome {
        Some(manager_v1::get_downstream_operation_result::Outcome::Success(value)) => value,
        outcome => panic!("downstream-operation read failed: {outcome:?}"),
    };
    let record = downstream
        .operation
        .expect("successful read must return the native operation record");
    assert_eq!(record.downstream_operation_id, downstream_operation_id);
    assert!(record.affected_issue_ids.contains(&issue_id.to_string()));
    let binding = downstream
        .binding
        .expect("successful read must return the Manager binding");
    assert_eq!(binding.manager_operation_id, operation.operation_id);
    assert_eq!(binding.issue_id, issue_id.to_string());
    assert_eq!(binding.downstream_operation_id, downstream_operation_id);
    assert_eq!(binding.manager_request_digest, operation.request_digest);
    assert_eq!(binding.manager_request_schema, operation.request_schema);

    let bad_context = manager_v1::ManagerReadContext {
        data_root_id: "different-root".into(),
        ..context.clone()
    };
    let bad_context_result = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(bad_context),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("identity mismatch is a typed read result")
        .into_inner();
    assert!(matches!(
        bad_context_result.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));

    for (issue_id, downstream_operation_id) in [
        (
            uuid::Uuid::new_v4().to_string(),
            downstream_operation_id.clone(),
        ),
        (
            issue_id.to_string(),
            "different-downstream-operation".into(),
        ),
    ] {
        let mismatch = trusted_reads
            .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
                context: Some(context.clone()),
                issue_id,
                manager_operation_id: operation.operation_id.clone(),
                downstream_operation_id,
            })
            .await
            .expect("binding mismatch is a typed read result")
            .into_inner();
        assert!(matches!(
            mismatch.outcome,
            Some(manager_v1::get_downstream_operation_result::Outcome::Error(
                error
            )) if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
                && error.message == "operation binding does not match the requested Issue"
        ));
    }

    for page_size in [0, 101, u32::MAX] {
        let invalid_page = trusted_reads
            .list_issue_history(manager_v1::ListIssueHistoryArgs {
                context: Some(context.clone()),
                issue_id: issue_id.to_string(),
                page: Some(manager_v1::PageRequest {
                    page_size,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .expect("invalid page size is a typed read result")
            .into_inner();
        assert!(matches!(
            invalid_page.outcome,
            Some(manager_v1::list_issue_history_result::Outcome::Error(error))
                if error.code == manager_v1::ManagerErrorCode::ResourceLimitExceeded as i32
                    && error.message == "history page size must be between 1 and 100"
        ));
    }

    let oversized_request =
        crate::proto_types::rpc_v1::manager_trusted_read_service_server::ManagerTrustedReadService::list_issue_history(
            &direct_read_server,
            Request::new(manager_v1::ListIssueHistoryArgs {
                context: Some(manager_v1::ManagerReadContext {
                    correlation_id: "x".repeat(4 * 1024 * 1024),
                    ..context.clone()
                }),
                issue_id: issue_id.to_string(),
                page: Some(manager_v1::PageRequest {
                    page_size: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .await
        .expect("oversized request must return a typed error")
        .into_inner();
    assert!(matches!(
        oversized_request.outcome,
        Some(manager_v1::list_issue_history_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::ResourceLimitExceeded as i32
                && error.message == "trusted read request exceeds the 4 MiB limit"
    ));

    direct_read_server
        .app
        .core
        .tracker()
        .set_user_defined_attribute_for_test(
            issue_id,
            &format!("lb.history.{downstream_operation_id}"),
            "{}",
        )
        .await
        .expect("remove the native history entry while preserving its operation marker");
    let fallback = direct_read_server
        .app
        .dispatch_v1::<_, v1::GetOperationResult>(
            "get_operation",
            v1::GetOperationArgs {
                context: Some(v1::RequestContext {
                    correlation_id: "trusted-read-legacy-fallback-test".into(),
                    ..Default::default()
                }),
                downstream_operation_id: downstream_operation_id.clone(),
            },
        )
        .await
        .expect("the native legacy fallback remains readable");
    assert!(matches!(
        fallback.outcome,
        Some(v1::get_operation_result::Outcome::Success(record))
            if record.method_key == "unknown"
    ));
    let rejected_fallback = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("unknown native method is a typed result")
        .into_inner();
    assert!(matches!(
        rejected_fallback.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));

    direct_read_server
        .app
        .core
        .tracker()
        .set_user_defined_attribute_for_test(
            issue_id,
            &format!("lb.operation.{downstream_operation_id}"),
            "{",
        )
        .await
        .expect("make a native operation read fail closed for ordering verification");
    let broken_native_read = direct_read_server
        .app
        .dispatch_v1::<_, v1::GetOperationResult>(
            "get_operation",
            v1::GetOperationArgs {
                context: Some(v1::RequestContext::default()),
                downstream_operation_id: downstream_operation_id.clone(),
            },
        )
        .await;
    assert!(!matches!(
        broken_native_read,
        Ok(v1::GetOperationResult {
            outcome: Some(v1::get_operation_result::Outcome::Success(_))
        })
    ));

    let original_backend_request = {
        let mut manager = direct_read_server
            .app
            .manager
            .as_ref()
            .expect("Manager must be configured")
            .lock()
            .await;
        manager
            .storage_mut()
            .repository()
            .backend_request(&operation.operation_id)
            .expect("read the durable Manager request")
            .expect("transition must have a durable request")
    };
    let replace_backend_request = |payload: &[u8]| {
        let digest = crate::manager::operations::digest_bytes(payload);
        let connection =
            rusqlite::Connection::open(root.path().join("issue-tracker-manager.sqlite3"))
                .expect("open temporary Manager database");
        connection
            .execute(
                "UPDATE manager_operation
                    SET request_snapshot = ?1, request_snapshot_digest = ?2
                  WHERE operation_id = ?3",
                rusqlite::params![payload, digest, operation.operation_id],
            )
            .expect("write the semantically mutated snapshot and its valid digest");
        connection
            .execute(
                "UPDATE manager_outbox SET payload_digest = ?1 WHERE operation_id = ?2",
                rusqlite::params![digest, operation.operation_id],
            )
            .expect("keep the outbox digest consistent with the mutated snapshot");
    };

    let mut wrong_target_state = crate::manager::backend_dispatch::DurableBackendRequest::decode(
        original_backend_request.as_slice(),
    )
    .expect("decode the persisted transition request");
    wrong_target_state
        .transition_issue
        .as_mut()
        .expect("durable transition request")
        .target_state = v1::IssueState::Open as i32;
    replace_backend_request(&wrong_target_state.encode_to_vec());
    let wrong_target_state_result = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("Manager request digest mismatch is a typed result")
        .into_inner();
    assert!(matches!(
        wrong_target_state_result.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));
    replace_backend_request(&original_backend_request);

    let mut wrong_target = crate::manager::backend_dispatch::DurableBackendRequest::decode(
        original_backend_request.as_slice(),
    )
    .expect("decode the persisted transition request");
    wrong_target
        .context
        .as_mut()
        .expect("durable write context")
        .target = format!("issue:{}", uuid::Uuid::new_v4());
    replace_backend_request(&wrong_target.encode_to_vec());

    let wrong_target_operation = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("malformed request binding is a typed result")
        .into_inner();
    assert!(matches!(
        wrong_target_operation.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));
    let wrong_target_history = trusted_reads
        .list_issue_history(manager_v1::ListIssueHistoryArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            page: Some(manager_v1::PageRequest {
                page_size: 100,
                ..Default::default()
            }),
            manager_operation_id: Some(operation.operation_id.clone()),
            downstream_operation_id: Some(downstream_operation_id.clone()),
        })
        .await
        .expect("malformed request binding is a typed result")
        .into_inner();
    assert!(matches!(
        wrong_target_history.outcome,
        Some(manager_v1::list_issue_history_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));

    let mut wrong_issue = crate::manager::backend_dispatch::DurableBackendRequest::decode(
        original_backend_request.as_slice(),
    )
    .expect("decode the persisted transition request");
    wrong_issue
        .transition_issue
        .as_mut()
        .expect("durable transition request")
        .issue_id = uuid::Uuid::new_v4().to_string();
    replace_backend_request(&wrong_issue.encode_to_vec());
    let wrong_issue_result = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("malformed request body is a typed result")
        .into_inner();
    assert!(matches!(
        wrong_issue_result.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));

    let mut wrong_revision = crate::manager::backend_dispatch::DurableBackendRequest::decode(
        original_backend_request.as_slice(),
    )
    .expect("decode the persisted transition request");
    wrong_revision
        .transition_issue
        .as_mut()
        .expect("durable transition request")
        .expected_revision = Some(created_issue.revision.saturating_add(1));
    replace_backend_request(&wrong_revision.encode_to_vec());
    let wrong_revision_result = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("request fingerprint mismatch is a typed result")
        .into_inner();
    assert!(matches!(
        wrong_revision_result.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));

    let mut wrong_terminal_report =
        crate::manager::backend_dispatch::DurableBackendRequest::decode(
            original_backend_request.as_slice(),
        )
        .expect("decode the persisted transition request");
    wrong_terminal_report
        .transition_issue
        .as_mut()
        .expect("durable transition request")
        .terminal_report = Some(
        crate::manager::backend_dispatch::durable_transition_issue_request::TerminalReport::Completion(
            v1::CompletionReport::default(),
        ),
    );
    replace_backend_request(&wrong_terminal_report.encode_to_vec());
    let wrong_terminal_report_result = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("terminal report mismatch is a typed result")
        .into_inner();
    assert!(matches!(
        wrong_terminal_report_result.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));

    let mut wrong_audit_context = crate::manager::backend_dispatch::DurableBackendRequest::decode(
        original_backend_request.as_slice(),
    )
    .expect("decode the persisted transition request");
    wrong_audit_context
        .context
        .as_mut()
        .expect("durable write context")
        .correlation_id = "different-original-context".into();
    replace_backend_request(&wrong_audit_context.encode_to_vec());
    let wrong_audit_context_result = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("request/audit context mismatch is a typed result")
        .into_inner();
    assert!(matches!(
        wrong_audit_context_result.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));

    let mut unsupported_method = crate::manager::backend_dispatch::DurableBackendRequest::decode(
        original_backend_request.as_slice(),
    )
    .expect("decode the persisted transition request");
    unsupported_method.method = "manager_unrecognized_issue_write".into();
    replace_backend_request(&unsupported_method.encode_to_vec());
    let unsupported_method_result = trusted_reads
        .get_downstream_operation(manager_v1::GetDownstreamOperationArgs {
            context: Some(context.clone()),
            issue_id: issue_id.to_string(),
            manager_operation_id: operation.operation_id.clone(),
            downstream_operation_id: downstream_operation_id.clone(),
        })
        .await
        .expect("unsupported persisted method is a typed result")
        .into_inner();
    assert!(matches!(
        unsupported_method_result.outcome,
        Some(manager_v1::get_downstream_operation_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));

    replace_backend_request(&original_backend_request);
    let mut native_activity = ActivityServiceClient::new(channel);
    assert_eq!(
        native_activity
            .add_issue_comment(v1::AddIssueCommentArgs::default())
            .await
            .expect_err("primary trusted reads must not publish native writes")
            .code(),
        Code::Unimplemented
    );
    shutdown_test_listener(shutdown, task).await;
}

async fn test_channel(address: SocketAddr) -> Channel {
    Endpoint::from_shared(format!("http://{address}"))
        .expect("create test endpoint")
        .connect()
        .await
        .expect("connect test client")
}

fn assert_route_is_available<T>(result: Result<Response<T>, Status>) {
    if let Err(status) = result {
        assert_ne!(
            status.code(),
            Code::Unimplemented,
            "registered route must not be rejected as unimplemented"
        );
    }
}

async fn shutdown_test_listener(
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let _ = shutdown.send(());
    task.await
        .expect("listener task must not panic")
        .expect("listener must stop cleanly");
}

#[tokio::test]
async fn primary_and_worker_listeners_enforce_their_rpc_allowlists() {
    use crate::proto_types::rpc_v1::{
        candidate_service_client::CandidateServiceClient,
        canonical_issue_service_client::CanonicalIssueServiceClient,
        issue_extract_client::IssueExtractClient, issue_gateway_client::IssueGatewayClient,
        manager_consumer_service_client::ManagerConsumerServiceClient,
        manager_info_service_client::ManagerInfoServiceClient,
        outbox_maintenance_service_client::OutboxMaintenanceServiceClient,
    };

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let application = IssueTrackerApplication::open(configured).await.unwrap();
    let issue_id = uuid::Uuid::new_v4();
    let created_issue = application
        .core
        .tracker()
        .create_with_draft(
            issue_id,
            "primary-listener-write-context",
            &v1::IssueDraft {
                title: "primary write context".into(),
                kind: v1::IssueKind::Task as i32,
                priority: v1::IssuePriority::P2 as i32,
                creation_basis: v1::CreationBasis::Explicit as i32,
                confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                ..Default::default()
            },
            "test",
            "primary-listener-write-context",
        )
        .await
        .expect("create issue for primary write-context test");
    let server = GrpcServer::new(application);
    let worker = server
        .for_gateway_worker(Arc::<str>::from("allowlist-test"))
        .await
        .expect("configure worker server");
    let (primary_address, primary_shutdown, primary_task) =
        start_test_listener(primary_routes(server)).await;
    let (worker_address, worker_shutdown, worker_task) =
        start_test_listener(gateway_worker_routes(worker).expect("worker reflection routes")).await;

    let primary_channel = test_channel(primary_address).await;
    let mut manager_info = ManagerInfoServiceClient::new(primary_channel.clone());
    let contract = manager_info
        .get_contract_info(manager_v1::GetContractInfoArgs::default())
        .await
        .expect("primary must serve ManagerInfoService")
        .into_inner();
    let contract = match contract.outcome {
        Some(manager_v1::get_contract_info_result::Outcome::Success(contract)) => contract,
        outcome => panic!("empty-context contract lookup must succeed: {outcome:?}"),
    };
    let mut outbox_maintenance = OutboxMaintenanceServiceClient::new(primary_channel.clone());
    let outbox_cycle = outbox_maintenance
        .run_outbox_cycle(rpc_v1::RunOutboxCycleArgs::default())
        .await
        .expect("primary must serve OutboxMaintenanceService")
        .into_inner();
    let outbox_contract = match outbox_cycle.outcome {
        Some(rpc_v1::run_outbox_cycle_result::Outcome::Success(success)) => success
            .contract
            .expect("outbox cycle success must include the manager contract"),
        outcome => panic!("primary outbox cycle must succeed: {outcome:?}"),
    };
    assert_eq!(
        outbox_contract.contract_fingerprint,
        contract.contract_fingerprint
    );
    let mut consumer = ManagerConsumerServiceClient::new(primary_channel.clone());
    let manager_context = manager_v1::ManagerReadContext {
        correlation_id: "primary-listener-startup-sequence".into(),
        schema_fingerprint: contract.schema_fingerprint.clone(),
        data_root_id: contract
            .store_identities
            .first()
            .expect("manager contract must identify its stores")
            .data_root_id
            .clone(),
        contract_fingerprint: contract.contract_fingerprint.clone(),
    };
    let candidates = consumer
        .list_candidates(manager_v1::ListCandidatesArgs {
            context: Some(manager_context),
            ..Default::default()
        })
        .await
        .expect("consumer read with returned contract identity must reach primary")
        .into_inner();
    assert!(matches!(
        candidates.outcome,
        Some(manager_v1::list_candidates_result::Outcome::Success(_))
    ));
    let transitioned = consumer
        .transition_issue(manager_v1::TransitionIssueArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_transition_issue".into(),
                target: format!("issue:{issue_id}"),
                schema_fingerprint: contract.schema_fingerprint.clone(),
                data_root_id: contract
                    .store_identities
                    .first()
                    .expect("manager contract must identify its stores")
                    .data_root_id
                    .clone(),
                contract_fingerprint: contract.contract_fingerprint.clone(),
                idempotency_key: "primary-listener-transition".into(),
                correlation_id: "primary-listener-startup-sequence".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "agent-app".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    ..Default::default()
                }),
            }),
            issue_id: issue_id.to_string(),
            expected_revision: Some(created_issue.revision),
            target_state: v1::IssueState::Blocked as i32,
            ..Default::default()
        })
        .await
        .expect("consumer write with returned contract identity must reach primary")
        .into_inner();
    assert!(matches!(
        transitioned.outcome,
        Some(manager_v1::transition_issue_result::Outcome::Success(_))
    ));
    assert_route_is_available(
        consumer
            .list_candidates(manager_v1::ListCandidatesArgs::default())
            .await,
    );
    assert_route_is_available(
        consumer
            .get_extraction_run(manager_v1::GetExtractionRunArgs::default())
            .await,
    );
    assert_route_is_available(
        consumer
            .list_extraction_runs(manager_v1::ListExtractionRunsArgs::default())
            .await,
    );
    assert_route_is_available(
        consumer
            .create_issue(manager_v1::CreateIssueArgs::default())
            .await,
    );
    assert_route_is_available(
        consumer
            .update_issue(manager_v1::UpdateIssueArgs::default())
            .await,
    );
    assert_route_is_available(
        consumer
            .transition_issue(manager_v1::TransitionIssueArgs::default())
            .await,
    );
    assert_route_is_available(
        consumer
            .update_candidate(manager_v1::UpdateCandidateArgs::default())
            .await,
    );
    assert_route_is_available(
        consumer
            .add_issue_relation(manager_v1::AddIssueRelationArgs::default())
            .await,
    );

    let mut extraction = IssueExtractClient::new(primary_channel.clone());
    assert_route_is_available(
        extraction
            .list_extraction_hints(rpc_v1::ListExtractionHintsRequest::default())
            .await,
    );
    assert_route_is_available(
        extraction
            .apply_extraction(rpc_v1::ApplyExtractionRequest::default())
            .await,
    );

    let mut candidate_service = CandidateServiceClient::new(primary_channel.clone());
    assert_eq!(
        candidate_service
            .list_candidates(manager_v1::ListCandidatesArgs::default())
            .await
            .expect_err("primary must not expose CandidateService")
            .code(),
        Code::Unimplemented
    );
    let mut canonical_issue_service = CanonicalIssueServiceClient::new(primary_channel);
    assert_eq!(
        canonical_issue_service
            .create_issue(manager_v1::CreateIssueArgs::default())
            .await
            .expect_err("primary must not expose CanonicalIssueService")
            .code(),
        Code::Unimplemented
    );
    assert_eq!(
        canonical_issue_service
            .update_issue(manager_v1::UpdateIssueArgs::default())
            .await
            .expect_err("primary must not expose CanonicalIssueService UpdateIssue")
            .code(),
        Code::Unimplemented
    );

    let worker_channel = test_channel(worker_address).await;
    let mut gateway = IssueGatewayClient::new(worker_channel.clone());
    assert_route_is_available(
        gateway
            .list_ready(rpc_v1::IssueGatewayListReadyRequest::default())
            .await,
    );
    assert_route_is_available(
        gateway
            .claim(rpc_v1::IssueGatewayClaimRequest::default())
            .await,
    );
    assert_route_is_available(
        gateway
            .heartbeat(rpc_v1::IssueGatewayHeartbeatRequest::default())
            .await,
    );
    assert_route_is_available(
        gateway
            .release(rpc_v1::IssueGatewayReleaseRequest::default())
            .await,
    );
    assert_route_is_available(
        gateway
            .transition(rpc_v1::IssueGatewayTransitionRequest::default())
            .await,
    );

    let mut worker_consumer = ManagerConsumerServiceClient::new(worker_channel.clone());
    assert_eq!(
        worker_consumer
            .list_candidates(manager_v1::ListCandidatesArgs::default())
            .await
            .expect_err("worker must not expose ManagerConsumerService")
            .code(),
        Code::Unimplemented
    );
    assert_eq!(
        worker_consumer
            .update_issue(manager_v1::UpdateIssueArgs::default())
            .await
            .expect_err("worker must not expose ManagerConsumerService UpdateIssue")
            .code(),
        Code::Unimplemented
    );
    let mut worker_manager_info = ManagerInfoServiceClient::new(worker_channel.clone());
    assert_eq!(
        worker_manager_info
            .get_contract_info(manager_v1::GetContractInfoArgs::default())
            .await
            .expect_err("worker must not expose ManagerInfoService")
            .code(),
        Code::Unimplemented
    );
    let mut worker_outbox_maintenance = OutboxMaintenanceServiceClient::new(worker_channel.clone());
    assert_eq!(
        worker_outbox_maintenance
            .run_outbox_cycle(rpc_v1::RunOutboxCycleArgs::default())
            .await
            .expect_err("worker must not expose OutboxMaintenanceService")
            .code(),
        Code::Unimplemented
    );
    let mut worker_extraction = IssueExtractClient::new(worker_channel);
    assert_eq!(
        worker_extraction
            .list_extraction_hints(rpc_v1::ListExtractionHintsRequest::default())
            .await
            .expect_err("worker must not expose IssueExtract")
            .code(),
        Code::Unimplemented
    );

    shutdown_test_listener(primary_shutdown, primary_task).await;
    shutdown_test_listener(worker_shutdown, worker_task).await;
}

#[tokio::test]
async fn primary_consumer_reopens_a_canceled_issue_through_durable_outbox() {
    use crate::proto_types::rpc_v1::{
        manager_consumer_service_client::ManagerConsumerServiceClient,
        manager_info_service_client::ManagerInfoServiceClient,
        outbox_maintenance_service_client::OutboxMaintenanceServiceClient,
    };

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let application = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = application
        .core
        .tracker()
        .create("durable-reopen", "durable reopen")
        .await
        .unwrap();
    let server = GrpcServer::new(application);
    let (address, shutdown, task) = start_test_listener(primary_routes(server)).await;
    let channel = test_channel(address).await;
    let contract = ManagerInfoServiceClient::new(channel.clone())
        .get_contract_info(manager_v1::GetContractInfoArgs::default())
        .await
        .unwrap()
        .into_inner();
    let contract = match contract.outcome {
        Some(manager_v1::get_contract_info_result::Outcome::Success(contract)) => contract,
        outcome => panic!("primary contract response: {outcome:?}"),
    };
    let read_context = manager_v1::ManagerReadContext {
        correlation_id: "durable-reopen".into(),
        schema_fingerprint: contract.schema_fingerprint.clone(),
        data_root_id: contract.store_identities[0].data_root_id.clone(),
        contract_fingerprint: contract.contract_fingerprint.clone(),
    };
    let write_context = |key: &str| manager_v1::ManagerWriteContext {
        method: "manager_transition_issue".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: contract.schema_fingerprint.clone(),
        data_root_id: contract.store_identities[0].data_root_id.clone(),
        contract_fingerprint: contract.contract_fingerprint.clone(),
        idempotency_key: key.into(),
        correlation_id: "durable-reopen".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "agent-app".into(),
            actor_kind: v1::ActorKind::Human as i32,
            ..Default::default()
        }),
    };
    let cancel = manager_v1::TransitionIssueArgs {
        context: Some(write_context("cancel-durable-reopen")),
        issue_id: issue.id.to_string(),
        expected_revision: Some(issue.revision),
        target_state: v1::IssueState::Canceled as i32,
        terminal_report: Some(
            manager_v1::transition_issue_args::TerminalReport::Cancellation(
                v1::CancellationReport {
                    classification: v1::CancellationClass::Other as i32,
                },
            ),
        ),
        ..Default::default()
    };
    let mut consumer = ManagerConsumerServiceClient::new(channel.clone());
    let cancel_response = consumer
        .transition_issue(cancel.clone())
        .await
        .unwrap()
        .into_inner();
    let cancel_operation_id = match cancel_response.outcome {
        Some(manager_v1::transition_issue_result::Outcome::Success(mutation)) => {
            mutation
                .operation
                .expect("admission must return an operation")
                .operation_id
        }
        outcome => panic!("cancel admission response: {outcome:?}"),
    };
    let replay = consumer
        .transition_issue(cancel)
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        replay.outcome,
        Some(manager_v1::transition_issue_result::Outcome::Success(mutation))
            if mutation.operation.as_ref().is_some_and(|operation| operation.operation_id == cancel_operation_id)
    ));

    let mut outbox = OutboxMaintenanceServiceClient::new(channel.clone());
    let cycle = outbox
        .run_outbox_cycle(rpc_v1::RunOutboxCycleArgs::default())
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        cycle.outcome,
        Some(rpc_v1::run_outbox_cycle_result::Outcome::Success(_))
    ));
    let canceled = consumer
        .get_issue_with_sync(manager_v1::GetIssueWithSyncArgs {
            context: Some(read_context.clone()),
            issue_id: issue.id.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let canceled = match canceled.outcome {
        Some(manager_v1::get_issue_with_sync_result::Outcome::Success(issue)) => issue,
        outcome => panic!("canceled issue read: {outcome:?}"),
    };
    let canceled_issue = canceled.issue.expect("canonical issue");
    assert_eq!(canceled_issue.state, v1::IssueState::Canceled as i32);
    assert!(canceled_issue.completed_at.is_some());

    let reopen = manager_v1::TransitionIssueArgs {
        context: Some(write_context("reopen-durable-reopen")),
        issue_id: issue.id.to_string(),
        expected_revision: Some(canceled_issue.revision),
        target_state: v1::IssueState::Open as i32,
        ..Default::default()
    };
    assert!(matches!(
        consumer
            .transition_issue(reopen)
            .await
            .unwrap()
            .into_inner()
            .outcome,
        Some(manager_v1::transition_issue_result::Outcome::Success(_))
    ));
    assert!(matches!(
        outbox
            .run_outbox_cycle(rpc_v1::RunOutboxCycleArgs::default())
            .await
            .unwrap()
            .into_inner()
            .outcome,
        Some(rpc_v1::run_outbox_cycle_result::Outcome::Success(ref result))
            if result.confirmed_operations == 0
    ));

    let reopened = consumer
        .get_issue_with_sync(manager_v1::GetIssueWithSyncArgs {
            context: Some(read_context.clone()),
            issue_id: issue.id.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let reopened = match reopened.outcome {
        Some(manager_v1::get_issue_with_sync_result::Outcome::Success(issue)) => issue,
        outcome => panic!("reopened issue read: {outcome:?}"),
    };
    assert!(matches!(
        reopened.issue,
        Some(issue) if issue.state == v1::IssueState::Open as i32 && issue.completed_at.is_none()
    ));
    let listed = consumer
        .list_issues_with_sync(manager_v1::ListIssuesWithSyncArgs {
            context: Some(read_context.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        listed.outcome,
        Some(manager_v1::list_issues_with_sync_result::Outcome::Success(page))
            if page.issues.iter().any(|entry| entry.issue.as_ref().is_some_and(|current| {
                current.issue_id == issue.id.to_string() && current.completed_at.is_none()
            }))
    ));
    let operation = consumer
        .get_operation(manager_v1::GetOperationArgs {
            context: Some(read_context),
            operation_id: cancel_operation_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        operation.outcome,
        Some(manager_v1::get_operation_result::Outcome::Success(operation))
            if operation.state == manager_v1::DurableOperationState::Committed as i32
    ));
    shutdown_test_listener(shutdown, task).await;
}

#[tokio::test]
async fn gateway_capability_is_fenced_after_trusted_cancellation_and_reopen() {
    use crate::proto_types::rpc_v1::{
        issue_gateway_client::IssueGatewayClient,
        manager_consumer_service_client::ManagerConsumerServiceClient,
        manager_info_service_client::ManagerInfoServiceClient,
    };

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let application = IssueTrackerApplication::open(configured).await.unwrap();
    let issue = application
        .core
        .tracker()
        .create("gateway-capability-fence", "gateway capability fence")
        .await
        .unwrap();
    let server = GrpcServer::new(application);
    server
        .app
        .manager
        .as_ref()
        .unwrap()
        .lock()
        .await
        .storage_mut()
        .repository()
        .register_gateway_policy(
            "gateway-capability-fence",
            "gateway-capability-fence",
            "register-gateway-capability-fence",
            &manager_v1::WorkEligibilityPolicy {
                mode: manager_v1::WorkEligibilityMode::ReviewedOnly as i32,
            },
        )
        .unwrap();
    let worker = server
        .for_gateway_worker("gateway-capability-fence")
        .await
        .unwrap();
    let (primary_address, primary_shutdown, primary_task) =
        start_test_listener(primary_routes(server)).await;
    let (worker_address, worker_shutdown, worker_task) =
        start_test_listener(gateway_worker_routes(worker).unwrap()).await;
    let primary_channel = test_channel(primary_address).await;
    let worker_channel = test_channel(worker_address).await;

    let contract = ManagerInfoServiceClient::new(primary_channel.clone())
        .get_contract_info(manager_v1::GetContractInfoArgs::default())
        .await
        .unwrap()
        .into_inner();
    let contract = match contract.outcome {
        Some(manager_v1::get_contract_info_result::Outcome::Success(contract)) => contract,
        outcome => panic!("primary contract response: {outcome:?}"),
    };
    let read_context = manager_v1::ManagerReadContext {
        correlation_id: "gateway-capability-fence".into(),
        schema_fingerprint: contract.schema_fingerprint.clone(),
        data_root_id: contract.store_identities[0].data_root_id.clone(),
        contract_fingerprint: contract.contract_fingerprint.clone(),
    };
    let write_context = |key: &str| manager_v1::ManagerWriteContext {
        method: "manager_apply_issue_admin_action".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: contract.schema_fingerprint.clone(),
        data_root_id: contract.store_identities[0].data_root_id.clone(),
        contract_fingerprint: contract.contract_fingerprint.clone(),
        idempotency_key: key.into(),
        correlation_id: "gateway-capability-fence".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "agent-app".into(),
            actor_kind: v1::ActorKind::Human as i32,
            reason: Some("cancel gateway-owned work".into()),
            ..Default::default()
        }),
    };

    let mut worker = IssueGatewayClient::new(worker_channel);
    let old_claim = worker
        .claim(rpc_v1::IssueGatewayClaimRequest {
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            idempotency_key: "claim-before-cancel".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let old_claim = match old_claim.outcome {
        Some(rpc_v1::issue_gateway_claim_response::Outcome::Success(value)) => value,
        outcome => panic!("gateway claim response: {outcome:?}"),
    };

    let mut consumer = ManagerConsumerServiceClient::new(primary_channel);
    let cancel = consumer
        .apply_issue_admin_action(manager_v1::ApplyIssueAdminActionArgs {
            context: Some(write_context("cancel-gateway-claimed-issue")),
            issue_id: issue.id.to_string(),
            expected_revision: Some(old_claim.revision),
            action: manager_v1::IssueAdminAction::Cancel as i32,
            terminal_report: Some(
                manager_v1::apply_issue_admin_action_args::TerminalReport::Cancellation(
                    v1::CancellationReport {
                        classification: v1::CancellationClass::Other as i32,
                    },
                ),
            ),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        cancel.outcome,
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(_))
    ));
    let canceled = consumer
        .get_issue_with_sync(manager_v1::GetIssueWithSyncArgs {
            context: Some(read_context.clone()),
            issue_id: issue.id.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let canceled = match canceled.outcome {
        Some(manager_v1::get_issue_with_sync_result::Outcome::Success(value)) => {
            value.issue.expect("canceled issue")
        }
        outcome => panic!("canceled issue response: {outcome:?}"),
    };
    assert_eq!(canceled.state, v1::IssueState::Canceled as i32);
    assert!(canceled.claim.is_none());

    let transition_context = |key: &str| manager_v1::ManagerWriteContext {
        method: "manager_transition_issue".into(),
        target: format!("issue:{}", issue.id),
        schema_fingerprint: contract.schema_fingerprint.clone(),
        data_root_id: contract.store_identities[0].data_root_id.clone(),
        contract_fingerprint: contract.contract_fingerprint.clone(),
        idempotency_key: key.into(),
        correlation_id: "gateway-capability-fence".into(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "agent-app".into(),
            actor_kind: v1::ActorKind::Human as i32,
            ..Default::default()
        }),
    };
    let reopen = consumer
        .transition_issue(manager_v1::TransitionIssueArgs {
            context: Some(transition_context("reopen-gateway-claimed-issue")),
            issue_id: issue.id.to_string(),
            expected_revision: Some(canceled.revision),
            target_state: v1::IssueState::Open as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        reopen.outcome,
        Some(manager_v1::transition_issue_result::Outcome::Success(_))
    ));
    let reopened = consumer
        .get_issue_with_sync(manager_v1::GetIssueWithSyncArgs {
            context: Some(read_context),
            issue_id: issue.id.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let reopened = match reopened.outcome {
        Some(manager_v1::get_issue_with_sync_result::Outcome::Success(value)) => {
            value.issue.expect("reopened issue")
        }
        outcome => panic!("reopened issue response: {outcome:?}"),
    };
    assert_eq!(reopened.state, v1::IssueState::Open as i32);
    assert!(reopened.claim.is_none());

    let stale_transition = worker
        .transition(rpc_v1::IssueGatewayTransitionRequest {
            issue_id: issue.id.to_string(),
            capability_id: old_claim.capability_id,
            expected_revision: Some(reopened.revision),
            target_state: v1::IssueState::InProgress as i32,
            idempotency_key: "transition-with-revoked-capability".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        stale_transition.outcome,
        Some(rpc_v1::issue_gateway_mutation_response::Outcome::Error(error))
            if error.code == "invalid_argument"
    ));

    let new_claim = worker
        .claim(rpc_v1::IssueGatewayClaimRequest {
            issue_id: issue.id.to_string(),
            expected_revision: Some(reopened.revision),
            idempotency_key: "claim-after-reopen".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let new_claim = match new_claim.outcome {
        Some(rpc_v1::issue_gateway_claim_response::Outcome::Success(value)) => value,
        outcome => panic!("new gateway claim response: {outcome:?}"),
    };
    assert_eq!(new_claim.revision, reopened.revision + 1);

    shutdown_test_listener(primary_shutdown, primary_task).await;
    shutdown_test_listener(worker_shutdown, worker_task).await;
}

#[tokio::test]
async fn primary_get_issue_relations_paginates_and_validates_context_and_tokens() {
    use crate::proto_types::rpc_v1::{
        manager_consumer_service_client::ManagerConsumerServiceClient,
        manager_info_service_client::ManagerInfoServiceClient,
    };

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let application = IssueTrackerApplication::open(configured).await.unwrap();
    let tracker = application.core.tracker().clone();
    let source = tracker
        .create_with_draft(
            uuid::Uuid::new_v4(),
            "relations-source",
            &v1::IssueDraft {
                title: "relations source".into(),
                kind: v1::IssueKind::Task as i32,
                priority: v1::IssuePriority::P2 as i32,
                creation_basis: v1::CreationBasis::Explicit as i32,
                confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                ..Default::default()
            },
            "test",
            "relations-source",
        )
        .await
        .unwrap();
    let target_one = tracker.create("relations-target-one", "one").await.unwrap();
    let target_two = tracker.create("relations-target-two", "two").await.unwrap();
    let inbound_source = tracker
        .create("relations-inbound", "inbound")
        .await
        .unwrap();
    tracker
        .relate_with_context(
            source.id,
            crate::tracker::RelationKind::Related,
            target_one.id,
            0,
            0,
            "relations-one",
            "test",
            None,
            "relations-one",
        )
        .await
        .unwrap();
    tracker
        .relate_with_context(
            source.id,
            crate::tracker::RelationKind::Related,
            target_two.id,
            1,
            0,
            "relations-two",
            "test",
            None,
            "relations-two",
        )
        .await
        .unwrap();
    tracker
        .relate_with_context(
            inbound_source.id,
            crate::tracker::RelationKind::Related,
            source.id,
            0,
            2,
            "relations-inbound",
            "test",
            None,
            "relations-inbound",
        )
        .await
        .unwrap();

    let server = GrpcServer::new(application);
    let (address, shutdown, task) = start_test_listener(primary_routes(server)).await;
    let channel = test_channel(address).await;
    let contract = ManagerInfoServiceClient::new(channel.clone())
        .get_contract_info(manager_v1::GetContractInfoArgs::default())
        .await
        .unwrap()
        .into_inner();
    let contract = match contract.outcome {
        Some(manager_v1::get_contract_info_result::Outcome::Success(contract)) => contract,
        other => panic!("primary contract response: {other:?}"),
    };
    let context = manager_v1::ManagerReadContext {
        correlation_id: "relations-read".into(),
        schema_fingerprint: contract.schema_fingerprint.clone(),
        data_root_id: contract.store_identities[0].data_root_id.clone(),
        contract_fingerprint: contract.contract_fingerprint.clone(),
    };
    let mut consumer = ManagerConsumerServiceClient::new(channel);
    let first = consumer
        .get_issue_relations(manager_v1::GetIssueRelationsArgs {
            context: Some(context.clone()),
            issue_id: source.id.to_string(),
            page: Some(manager_v1::PageRequest {
                page_size: 2,
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    let first = match first.outcome {
        Some(manager_v1::get_issue_relations_result::Outcome::Success(page)) => page,
        other => panic!("first relation page: {other:?}"),
    };
    assert_eq!(first.relations.len(), 2);
    assert_eq!(
        first.page.as_ref().and_then(|page| page.total_size),
        Some(3)
    );
    let token = first
        .page
        .as_ref()
        .and_then(|page| page.next_page_token.clone())
        .expect("first relation page must continue");
    assert!(first.relations.iter().all(|relation| {
        relation.source_issue_id == source.id.to_string()
            || relation.target_issue_id == source.id.to_string()
    }));

    let second = consumer
        .get_issue_relations(manager_v1::GetIssueRelationsArgs {
            context: Some(context.clone()),
            issue_id: source.id.to_string(),
            page: Some(manager_v1::PageRequest {
                page_size: 2,
                page_token: Some(token.clone()),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    let second = match second.outcome {
        Some(manager_v1::get_issue_relations_result::Outcome::Success(page)) => page,
        other => panic!("second relation page: {other:?}"),
    };
    assert_eq!(second.relations.len(), 1);
    assert!(
        first
            .relations
            .iter()
            .chain(second.relations.iter())
            .any(|relation| relation.target_issue_id == source.id.to_string())
    );

    let target_three = tracker
        .create("relations-target-three", "three")
        .await
        .unwrap();
    tracker
        .relate_with_context(
            source.id,
            crate::tracker::RelationKind::Related,
            target_three.id,
            3,
            0,
            "relations-three",
            "test",
            None,
            "relations-three",
        )
        .await
        .unwrap();
    let expired = consumer
        .get_issue_relations(manager_v1::GetIssueRelationsArgs {
            context: Some(context.clone()),
            issue_id: source.id.to_string(),
            page: Some(manager_v1::PageRequest {
                page_size: 2,
                page_token: Some(token),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        expired.outcome,
        Some(manager_v1::get_issue_relations_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::PageTokenExpired as i32
    ));

    let invalid_token = consumer
        .get_issue_relations(manager_v1::GetIssueRelationsArgs {
            context: Some(context.clone()),
            issue_id: source.id.to_string(),
            page: Some(manager_v1::PageRequest {
                page_token: Some("invalid".into()),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        invalid_token.outcome,
        Some(manager_v1::get_issue_relations_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::InvalidPageToken as i32
    ));

    let mismatch = consumer
        .get_issue_relations(manager_v1::GetIssueRelationsArgs {
            context: Some(manager_v1::ManagerReadContext {
                contract_fingerprint: "wrong".into(),
                ..context
            }),
            issue_id: source.id.to_string(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        mismatch.outcome,
        Some(manager_v1::get_issue_relations_result::Outcome::Error(error))
            if error.code == manager_v1::ManagerErrorCode::MethodTargetMismatch as i32
    ));
    shutdown_test_listener(shutdown, task).await;
}

#[tokio::test]
async fn primary_listener_does_not_publish_manager_services_without_manager_settings() {
    use crate::proto_types::rpc_v1::{
        issue_extract_client::IssueExtractClient,
        manager_consumer_service_client::ManagerConsumerServiceClient,
        manager_info_service_client::ManagerInfoServiceClient,
        manager_trusted_read_service_client::ManagerTrustedReadServiceClient,
        outbox_maintenance_service_client::OutboxMaintenanceServiceClient,
    };

    let root = TempDir::new().unwrap();
    let application = IssueTrackerApplication::open(settings(&root))
        .await
        .unwrap();
    let server = GrpcServer::new(application);
    let (address, shutdown, task) = start_test_listener(primary_routes(server)).await;
    let channel = test_channel(address).await;

    let mut manager_info = ManagerInfoServiceClient::new(channel.clone());
    assert_eq!(
        manager_info
            .get_contract_info(manager_v1::GetContractInfoArgs::default())
            .await
            .expect_err("primary must not expose ManagerInfoService without manager settings")
            .code(),
        Code::Unimplemented
    );
    let mut outbox_maintenance = OutboxMaintenanceServiceClient::new(channel.clone());
    assert_eq!(
            outbox_maintenance
                .run_outbox_cycle(rpc_v1::RunOutboxCycleArgs::default())
                .await
                .expect_err(
                    "primary must not expose OutboxMaintenanceService without manager settings",
                )
                .code(),
            Code::Unimplemented
        );
    let mut consumer = ManagerConsumerServiceClient::new(channel.clone());
    assert_eq!(
        consumer
            .list_candidates(manager_v1::ListCandidatesArgs::default())
            .await
            .expect_err("primary must not expose ManagerConsumerService without manager settings")
            .code(),
        Code::Unimplemented
    );
    let mut trusted_reads = ManagerTrustedReadServiceClient::new(channel.clone());
    assert_eq!(
        trusted_reads
            .list_issue_history(manager_v1::ListIssueHistoryArgs::default())
            .await
            .expect_err("primary must not expose trusted reads without manager settings")
            .code(),
        Code::Unimplemented
    );
    let mut extraction = IssueExtractClient::new(channel);
    assert_eq!(
        extraction
            .list_extraction_hints(rpc_v1::ListExtractionHintsRequest::default())
            .await
            .expect_err("primary must not expose IssueExtract without manager settings")
            .code(),
        Code::Unimplemented
    );

    shutdown_test_listener(shutdown, task).await;
}

#[tokio::test]
async fn opens_a_new_data_root_when_manager_is_configured() {
    let parent = TempDir::new().unwrap();
    let root = parent.path().join("new-tracker-root");
    let mut settings = settings(&parent);
    settings.tracker.data_root = root.to_string_lossy().into_owned();
    settings.tracker.manager_settings = Some(manager_v1::IssueTrackerManagerSettings {
        default_page_size: 10,
        maximum_page_size: 20,
        maximum_payload_bytes: 65_536,
        maximum_repeated_item_count: 100,
        operation_lease: Some(manager_v1::TimeSpan { millis: 10_000 }),
        outbox_lease: Some(manager_v1::TimeSpan { millis: 10_000 }),
        resource_limits: Some(manager_v1::ManagerResourceLimits {
            max_string_bytes: 4096,
            max_body_bytes: 65_536,
            max_uri_bytes: 4096,
            max_repeated_item_count: 100,
            max_page_size: 100,
        }),
        retry_budget: Some(manager_v1::RetryBudget {
            max_attempts: 3,
            base_backoff: Some(manager_v1::TimeSpan { millis: 100 }),
            max_backoff: Some(manager_v1::TimeSpan { millis: 1_000 }),
        }),
        secret_filter_mode: manager_v1::SecretFilterMode::FailClosed as i32,
        ..Default::default()
    });

    let application = IssueTrackerApplication::open(settings).await;

    assert!(application.is_ok());
    assert!(root.is_dir());
}

#[tokio::test]
async fn manager_configuration_keeps_legacy_tracker_defaults() {
    let parent = TempDir::new().unwrap();
    let root = parent.path().join("legacy-defaults-root");
    let settings = ServerSettings {
        server: ListenerSettings::default(),
        gateway_endpoints: vec![],
        tracker: v1::IssueTrackerRunnerSettings {
            data_root: root.to_string_lossy().into_owned(),
            manager_settings: Some(manager_v1::IssueTrackerManagerSettings {
                default_page_size: 10,
                maximum_page_size: 20,
                maximum_payload_bytes: 65_536,
                maximum_repeated_item_count: 100,
                operation_lease: Some(manager_v1::TimeSpan { millis: 10_000 }),
                outbox_lease: Some(manager_v1::TimeSpan { millis: 10_000 }),
                resource_limits: Some(manager_v1::ManagerResourceLimits {
                    max_string_bytes: 4096,
                    max_body_bytes: 65_536,
                    max_uri_bytes: 4096,
                    max_repeated_item_count: 100,
                    max_page_size: 100,
                }),
                retry_budget: Some(manager_v1::RetryBudget {
                    max_attempts: 3,
                    base_backoff: Some(manager_v1::TimeSpan { millis: 100 }),
                    max_backoff: Some(manager_v1::TimeSpan { millis: 1_000 }),
                }),
                secret_filter_mode: manager_v1::SecretFilterMode::FailClosed as i32,
                ..Default::default()
            }),
            ..Default::default()
        },
        maintenance: MaintenanceSettings::default(),
    };

    assert!(IssueTrackerApplication::open(settings).await.is_ok());
}

#[tokio::test]
async fn toml_allows_server_owned_fingerprints_to_be_omitted() {
    let root = TempDir::new().unwrap();
    let root = root.path().join("tracker");
    let source = format!(
        r#"
[tracker]
data_root = "{}"
secret_filter_mode = 1

[tracker.lease_policy]
default_lease = {{ millis = 30000 }}
minimum_lease = {{ millis = 10000 }}
maximum_lease = {{ millis = 60000 }}
maximum_heartbeat_interval = {{ millis = 1000 }}

[tracker.pagination_policy]
default_page_size = 50
maximum_page_size = 100

[tracker.resource_limits]
max_string_bytes = 4096
max_body_bytes = 65536
max_uri_bytes = 4096
max_token_bytes = 4096
max_handle_bytes = 4096
max_repeated_item_count = 100
max_export_bytes = 1048576
max_export_item_count = 10000
max_artifact_bytes = 1048576
max_artifact_count = 100
max_total_count_scan_items = 10000

[tracker.manager_settings]
data_root = "{}"
store_identities = []
default_page_size = 50
maximum_page_size = 100
maximum_payload_bytes = 65536
maximum_repeated_item_count = 100
secret_filter_mode = 2

[tracker.manager_settings.operation_lease]
millis = 30000
[tracker.manager_settings.outbox_lease]
millis = 30000
[tracker.manager_settings.resource_limits]
max_string_bytes = 4096
max_body_bytes = 65536
max_uri_bytes = 4096
max_repeated_item_count = 100
max_page_size = 100
[tracker.manager_settings.retry_budget]
max_attempts = 3
[tracker.manager_settings.retry_budget.base_backoff]
millis = 100
[tracker.manager_settings.retry_budget.max_backoff]
millis = 1000
"#,
        root.display(),
        root.display(),
    );

    let settings = ServerSettings::from_toml(&source).expect("decode omitted fingerprints");
    assert!(settings.tracker.expected_contract_fingerprint.is_empty());
    assert!(
        settings
            .tracker
            .manager_settings
            .as_ref()
            .expect("manager settings")
            .expected_contract_fingerprint
            .is_empty()
    );
    assert!(IssueTrackerApplication::open(settings).await.is_ok());

    let mismatched = source.replacen(
        "[tracker]\n",
        "[tracker]\nexpected_contract_fingerprint = \"not-the-server-contract\"\n",
        1,
    );
    let settings =
        ServerSettings::from_toml(&mismatched).expect("decode explicitly mismatched fingerprint");
    assert!(IssueTrackerApplication::open(settings).await.is_err());
}

#[tokio::test]
async fn primary_admin_action_records_actor_session_and_correlation_in_history() {
    use crate::proto_types::rpc_v1::manager_consumer_service_server::ManagerConsumerService;

    let root = TempDir::new().unwrap();
    let mut configured = settings(&root);
    configured.tracker.manager_settings = Some(manager_settings());
    let server = GrpcServer::new(IssueTrackerApplication::open(configured).await.unwrap());
    let issue = server
        .app
        .core
        .tracker()
        .create("admin audit", "admin audit")
        .await
        .unwrap();
    let identity = server.app.manager_store_context().await.unwrap();
    let response = ManagerConsumerService::apply_issue_admin_action(
        &server,
        Request::new(manager_v1::ApplyIssueAdminActionArgs {
            context: Some(manager_v1::ManagerWriteContext {
                method: "manager_apply_issue_admin_action".into(),
                target: format!("issue:{}", issue.id),
                schema_fingerprint: identity.schema_fingerprint,
                data_root_id: identity.data_root_id,
                contract_fingerprint: identity.contract_fingerprint,
                idempotency_key: "admin-audit-cancel".into(),
                correlation_id: "admin-audit-correlation".into(),
                actor_audit: Some(manager_v1::ActorAudit {
                    actor_id: "admin-audit-actor".into(),
                    actor_kind: v1::ActorKind::Human as i32,
                    agent_session_id: Some("admin-audit-session".into()),
                    reason: Some("cancelled by administrator".into()),
                    ..Default::default()
                }),
            }),
            issue_id: issue.id.to_string(),
            expected_revision: Some(issue.revision),
            action: manager_v1::IssueAdminAction::Cancel as i32,
            terminal_report: Some(
                manager_v1::apply_issue_admin_action_args::TerminalReport::Cancellation(
                    v1::CancellationReport {
                        classification: v1::CancellationClass::Other as i32,
                    },
                ),
            ),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let operation = match response.outcome {
        Some(manager_v1::apply_issue_admin_action_result::Outcome::Success(value)) => {
            value.operation.expect("durable administrative operation")
        }
        outcome => panic!("administrative cancellation failed: {outcome:?}"),
    };

    let history = server
        .app
        .core
        .tracker()
        .list_history(issue.id)
        .await
        .unwrap();
    let entry = history
        .iter()
        .find(|entry| entry.downstream_operation_id == operation.downstream_operation_id)
        .expect("administrative action must append canonical history");
    let actor = entry.actor.as_ref().unwrap();
    let actor_ref = actor.actor.as_ref().unwrap();
    assert_eq!(actor_ref.actor_id, "admin-audit-actor");
    assert_eq!(actor_ref.kind, v1::ActorKind::Human as i32);
    // The live path must persist the same audit context as restart recovery.
    assert_eq!(
        actor.agent_session_id.as_deref(),
        Some("admin-audit-session")
    );
    assert_eq!(entry.correlation_id, "admin-audit-correlation");
}
