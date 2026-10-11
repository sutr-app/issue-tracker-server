use super::*;
use crate::manager::{ManagerConfig, ManagerStorage, config::tests::settings};
use tempfile::tempdir;

fn candidate(revision: u64) -> pb::Candidate {
    pb::Candidate {
        candidate_id: "candidate-1".into(),
        revision,
        status: pb::CandidateStatus::Proposed as i32,
        ..Default::default()
    }
}

fn write_context(method: &str, target: &str) -> pb::ManagerWriteContext {
    pb::ManagerWriteContext {
        method: method.into(),
        target: target.into(),
        schema_fingerprint: "schema".into(),
        idempotency_key: "request-1".into(),
        data_root_id: "root".into(),
        contract_fingerprint: "contract".into(),
        ..Default::default()
    }
}

fn assert_resolve_candidate_success_snapshot(
    operation: &pb::DurableOperation,
    receipt: &pb::ResolutionReceipt,
) {
    assert_eq!(operation.state, pb::DurableOperationState::Committed as i32);
    assert_eq!(
        operation.result_replayability,
        v1::OperationResultReplayability::Replayable as i32
    );
    let pb::durable_operation::TerminalSnapshot::ResultSnapshot(snapshot) = operation
        .terminal_snapshot
        .as_ref()
        .expect("successful resolution has a result snapshot")
    else {
        panic!("successful resolution must not have an error snapshot");
    };
    assert_eq!(
        snapshot.schema,
        "issue_tracker.manager.v1.ResolveCandidateResult"
    );
    assert!(!snapshot.payload.is_empty());
    let result = pb::ResolveCandidateResult::decode(snapshot.payload.as_slice())
        .expect("decode resolution result snapshot");
    let Some(pb::resolve_candidate_result::Outcome::Success(snapshot_receipt)) = result.outcome
    else {
        panic!("resolution result snapshot must be successful");
    };
    assert_eq!(&snapshot_receipt, receipt);
}

fn history_operation(operation_id: &str, created_at: i64) -> pb::DurableOperation {
    NewOperation {
        operation_id: operation_id.into(),
        scope: Some(OperationScope::Candidate("candidate-1".into())),
        method: "manager_create_candidate".into(),
        idempotency_scope: IdempotencyScope::new(
            "contract",
            "root",
            "manager_create_candidate",
            "candidate:candidate-1",
            format!("key-{operation_id}"),
        )
        .expect("valid idempotency scope"),
        request_digest: format!("digest-{operation_id}"),
        request_schema: "issue_tracker.manager.v1.CreateCandidateArgs".into(),
        downstream_operation_id: None,
        generation: 1,
        manager_sequence: 0,
        causal_parent: None,
        affects_ready: false,
        sync_subject: None,
        affected_issue_ids: Vec::new(),
        actor_audit: None,
        contract_fingerprint: "contract".into(),
        schema_fingerprint: "schema".into(),
        correlation_id: "history-read".into(),
        now_millis: created_at,
        replayable: true,
    }
    .build()
    .expect("valid durable operation")
}

fn seed_history_operation(
    repository: &mut ManagerRepository<'_>,
    operation_id: &str,
    created_at: i64,
) {
    let operation = history_operation(operation_id, created_at);
    repository
        .commit_mutation(
            &pb::ManagerMutation {
                operation: Some(operation.clone()),
                ..Default::default()
            },
            &operation,
            None,
        )
        .expect("seed operation history");
}

fn history_args(page: pb::PageRequest) -> pb::ListOperationHistoryArgs {
    pb::ListOperationHistoryArgs {
        context: Some(pb::ManagerReadContext {
            correlation_id: "history-read".into(),
            schema_fingerprint: "schema".into(),
            data_root_id: "root".into(),
            contract_fingerprint: "contract".into(),
        }),
        page: Some(page),
        filter: Some(pb::OperationHistoryFilter {
            direct_target: Some(pb::operation_history_filter::DirectTarget::CandidateId(
                "candidate-1".into(),
            )),
            ..Default::default()
        }),
        include_total_size: None,
    }
}

fn list_read_context() -> pb::ManagerReadContext {
    pb::ManagerReadContext {
        correlation_id: "list-read".into(),
        schema_fingerprint: "schema".into(),
        data_root_id: "root".into(),
        contract_fingerprint: "contract".into(),
    }
}

#[path = "repository_tests/domain.rs"]
mod domain;
#[path = "repository_tests/gateway.rs"]
mod gateway;
#[path = "repository_tests/operation_history.rs"]
mod operation_history;
#[path = "repository_tests/operations.rs"]
mod operations;
#[path = "repository_tests/resolution.rs"]
mod resolution;

fn recovery_operation(
    operation_id: &str,
    state: pb::DurableOperationState,
    failure_class: pb::FailureClass,
) -> pb::DurableOperation {
    pb::DurableOperation {
        operation_id: operation_id.into(),
        method: "manager_create_issue".into(),
        state: state as i32,
        failure_class: failure_class as i32,
        generation: 7,
        ..Default::default()
    }
}

fn auto_registration_test_operation(operation_id: &str) -> pb::DurableOperation {
    pb::DurableOperation {
        operation_id: operation_id.into(),
        scope: Some(pb::durable_operation::Scope::IssueId(
            "issue-auto-registration".into(),
        )),
        method: "manager_create_issue".into(),
        idempotency_scope: format!("scope-{operation_id}"),
        request_digest: format!("digest-{operation_id}"),
        request_schema: "issue_tracker.manager.v1.DurableBackendRequest".into(),
        downstream_operation_id: format!("backend-{operation_id}"),
        state: pb::DurableOperationState::Pending as i32,
        generation: 1,
        created_at: Some(utc_instant(1)),
        updated_at: Some(utc_instant(1)),
        ..Default::default()
    }
}

fn seed_auto_registration_entities(storage: &mut ManagerStorage, operation: &pb::DurableOperation) {
    let candidate = pb::Candidate {
        candidate_id: "candidate-auto-registration".into(),
        extraction_run_id: "run-auto-registration".into(),
        status: pb::CandidateStatus::Proposed as i32,
        strong_detection_proof: Some(pb::StrongDetectionProof::default()),
        ..Default::default()
    };
    let run = pb::ExtractionRun {
        run_id: "run-auto-registration".into(),
        state: pb::ExtractionRunState::Running as i32,
        source: "thread-summary".into(),
        request_digest: "sha256:request".into(),
        request_schema: "issue-extract/v2".into(),
        ..Default::default()
    };
    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            let candidate_input = entity_input(
                EntityKind::Candidate,
                &candidate.candidate_id,
                candidate.revision,
                "issue_tracker.manager.v1.Candidate",
                candidate.encode_to_vec(),
            )?;
            write_entity_in_transaction(transaction, &candidate_input, None, stamp)?;
            let run_input = entity_input(
                EntityKind::ExtractionRun,
                &run.run_id,
                run.revision,
                "issue_tracker.manager.v1.ExtractionRun",
                run.encode_to_vec(),
            )?;
            write_entity_in_transaction(transaction, &run_input, None, stamp)?;
            insert_operation(transaction, operation, &[], &digest_bytes(&[]), stamp)
        })
        .expect("seed auto-registration state");
}

fn recovery_outbox(operation: &pb::DurableOperation, status: pb::OutboxStatus) -> pb::OutboxEntry {
    pb::OutboxEntry {
        outbox_id: format!("outbox-{}", operation.operation_id),
        operation_id: operation.operation_id.clone(),
        topic: "taskchampion".into(),
        status: status as i32,
        payload_snapshot_ref: operation.operation_id.clone(),
        payload_digest: "sha256:request".into(),
        generation: operation.generation,
        ..Default::default()
    }
}

fn seed_recovery_operation(
    repository: &mut ManagerRepository<'_>,
    operation: &pb::DurableOperation,
    outbox: &pb::OutboxEntry,
) {
    repository
        .commit_mutation(
            &pb::ManagerMutation {
                operation: Some(operation.clone()),
                ..Default::default()
            },
            operation,
            Some(outbox),
        )
        .expect("seed recovery operation");
}
