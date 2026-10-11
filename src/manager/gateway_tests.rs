use super::gateway::{
    IdempotencyDecision, ManagerGateway, ManagerGatewayConfig, ManagerRepository,
};
use crate::proto_types::{manager_v1 as pb, v1};
use prost::Message;

#[derive(Default)]
struct FakeRepository {
    calls: Vec<String>,
    idempotency_reservations: usize,
    replay: Option<Vec<u8>>,
    pending: Option<(pb::DurableOperation, pb::Snapshot)>,
    snapshot: Option<pb::Snapshot>,
    stored_operation_ids: Vec<String>,
}

impl ManagerRepository for FakeRepository {
    fn reserve_idempotency(
        &mut self,
        _scope: &str,
        _request_digest: &str,
        _operation_id: &str,
    ) -> Result<IdempotencyDecision, pb::ManagerError> {
        self.idempotency_reservations += 1;
        if let Some((operation, mutation_snapshot)) = self.pending.take() {
            return Ok(IdempotencyDecision::Pending {
                operation: Box::new(operation),
                mutation_snapshot: Box::new(mutation_snapshot),
            });
        }
        Ok(self
            .replay
            .take()
            .map_or(IdempotencyDecision::New, IdempotencyDecision::Replay))
    }

    fn complete_idempotency(
        &mut self,
        _scope: &str,
        _method: &str,
        _request_digest: &str,
        operation_id: &str,
        snapshot: &pb::Snapshot,
    ) -> Result<pb::DurableOperation, pb::ManagerError> {
        self.snapshot = Some(snapshot.clone());
        Ok(pb::DurableOperation {
            operation_id: operation_id.into(),
            state: pb::DurableOperationState::Received as i32,
            ..Default::default()
        })
    }

    fn store_idempotent_result(
        &mut self,
        _scope: &str,
        _request_digest: &str,
        operation_id: &str,
        _result: &[u8],
    ) -> Result<(), pb::ManagerError> {
        self.stored_operation_ids.push(operation_id.into());
        Ok(())
    }

    fn create_candidate(
        &mut self,
        _args: &pb::CreateCandidateArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        self.calls.push("create_candidate".into());
        Ok(pb::ManagerMutation {
            result: Some(pb::manager_mutation::Result::IssueCreation(
                pb::IssueCreationResult {
                    issue: Some(v1::Issue::default()),
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
    }

    fn resolve_candidate(
        &mut self,
        args: &pb::ResolveCandidateArgs,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        self.calls.push("resolve_candidate".into());
        Ok(pb::ResolutionReceipt {
            operation_id: "resolution-operation-1".into(),
            state: pb::DurableOperationState::Committed as i32,
            candidate: Some(pb::CandidateResolutionReference {
                candidate_id: args.candidate_id.clone(),
                state: pb::ResolutionReferenceState::Existing as i32,
                revision: args.expected_revision,
            }),
            ..Default::default()
        })
    }

    fn get_candidate(
        &self,
        _args: &pb::GetCandidateArgs,
    ) -> Result<pb::Candidate, pb::ManagerError> {
        Ok(pb::Candidate {
            candidate_id: "candidate-1".into(),
            title: "title".into(),
            ..Default::default()
        })
    }

    fn retry_operation(
        &mut self,
        _args: &pb::RetryOperationArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        self.calls.push("retry_operation".into());
        Ok(pb::ManagerMutation::default())
    }

    fn reconcile_operation(
        &mut self,
        _args: &pb::ReconcileOperationArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        self.calls.push("reconcile_operation".into());
        Ok(pb::ManagerMutation::default())
    }
}

fn gateway() -> ManagerGateway<FakeRepository> {
    ManagerGateway::new(
        FakeRepository::default(),
        ManagerGatewayConfig {
            contract_fingerprint: "contract".into(),
            schema_fingerprint: "schema".into(),
            data_root_id: "root".into(),
            maximum_payload_bytes: 64 * 1024,
            root_operation_history_max_window_millis: 90 * 24 * 60 * 60 * 1_000,
            operation_history_v1_compat_enabled: true,
            page_size_limits: super::pagination::PageSizeLimits::new(100, 100),
            write_admission: true,
        },
    )
}

fn write_context(method: &str, target: &str) -> pb::ManagerWriteContext {
    pb::ManagerWriteContext {
        method: method.into(),
        target: target.into(),
        schema_fingerprint: "schema".into(),
        idempotency_key: "request-1".into(),
        data_root_id: "root".into(),
        contract_fingerprint: "contract".into(),
        correlation_id: "correlation-1".into(),
        actor_audit: Some(pb::ActorAudit {
            actor_id: "user-1".into(),
            actor_kind: v1::ActorKind::Human as i32,
            display_name: Some("User One".into()),
            agent_session_id: Some("session-1".into()),
            reason: Some("test".into()),
        }),
    }
}

fn read_context() -> pb::ManagerReadContext {
    pb::ManagerReadContext {
        correlation_id: "correlation-1".into(),
        schema_fingerprint: "schema".into(),
        data_root_id: "root".into(),
        contract_fingerprint: "contract".into(),
    }
}

fn candidate() -> pb::CandidateDraft {
    pb::CandidateDraft {
        candidate_id: "candidate-1".into(),
        title: "title".into(),
        kind: pb::CandidateKind::Task as i32,
        suggested_priority: pb::CandidatePriority::P2 as i32,
        suggested_state: pb::CandidateSuggestedState::Open as i32,
        detection_basis: pb::DetectionBasis::Explicit as i32,
        ..Default::default()
    }
}

#[test]
fn secret_filter_runs_before_repository_admission() {
    let mut gateway = gateway();
    let args = pb::CreateCandidateArgs {
        context: Some(write_context("manager_create_candidate", "candidate/1")),
        candidate: Some(pb::CandidateDraft {
            description: "password=must-not-pass".into(),
            ..candidate()
        }),
    };
    let result = gateway
        .dispatch("manager_create_candidate", &args.encode_to_vec())
        .expect("typed result");
    let result = pb::CreateCandidateResult::decode(result.as_slice()).expect("decode");
    assert!(matches!(
        result.outcome,
        Some(pb::create_candidate_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::ManagerSecretDetected as i32
    ));
    assert!(gateway.repository().calls.is_empty());
}

#[test]
fn token_assignment_is_rejected_before_repository_admission() {
    let mut gateway = gateway();
    let args = pb::CreateCandidateArgs {
        context: Some(write_context("manager_create_candidate", "candidate/1")),
        candidate: Some(pb::CandidateDraft {
            description: "token=must-not-pass".into(),
            ..candidate()
        }),
    };
    let result = gateway
        .dispatch("manager_create_candidate", &args.encode_to_vec())
        .expect("typed result");
    let result = pb::CreateCandidateResult::decode(result.as_slice()).expect("decode");
    assert!(matches!(
        result.outcome,
        Some(pb::create_candidate_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::ManagerSecretDetected as i32
    ));
    assert_eq!(gateway.repository().idempotency_reservations, 0);
}

#[test]
fn manager_only_write_replays_the_typed_result_without_second_handler_call() {
    let mut gateway = gateway();
    let args = pb::CreateCandidateArgs {
        context: Some(write_context("manager_create_candidate", "candidate/1")),
        candidate: Some(candidate()),
    };
    let first = gateway
        .dispatch("manager_create_candidate", &args.encode_to_vec())
        .expect("first result");
    assert_eq!(gateway.repository().calls, ["create_candidate"]);
    let stored = gateway.repository().snapshot.as_ref().expect("snapshot");
    let stored = pb::ManagerMutation::decode(stored.payload.as_slice()).expect("snapshot decodes");
    assert!(matches!(
        stored.result,
        Some(pb::manager_mutation::Result::IssueCreation(result)) if result.issue.is_none()
    ));
    gateway.repository_mut().replay = Some(first.clone());
    let second = gateway
        .dispatch("manager_create_candidate", &args.encode_to_vec())
        .expect("replay result");
    assert_eq!(first, second);
    assert_eq!(gateway.repository().calls, ["create_candidate"]);
}

#[test]
fn pending_idempotency_rebuilds_and_finalizes_the_original_typed_result() {
    let mut gateway = gateway();
    let mutation = pb::ManagerMutation {
        entity: Some(pb::manager_mutation::Entity::Candidate(pb::Candidate {
            candidate_id: "candidate-1".into(),
            revision: 1,
            ..Default::default()
        })),
        ..Default::default()
    };
    gateway.repository_mut().pending = Some((
        pb::DurableOperation {
            operation_id: "persisted-operation".into(),
            ..Default::default()
        },
        pb::Snapshot {
            payload: mutation.encode_to_vec(),
            schema: "issue_tracker.manager.v1.ManagerMutation".into(),
        },
    ));
    let args = pb::CreateCandidateArgs {
        context: Some(write_context("manager_create_candidate", "candidate/1")),
        candidate: Some(candidate()),
    };

    let result = gateway
        .dispatch("manager_create_candidate", &args.encode_to_vec())
        .expect("rebuild pending response");
    let result = pb::CreateCandidateResult::decode(result.as_slice()).expect("decode result");
    let mutation = match result.outcome.expect("outcome") {
        pb::create_candidate_result::Outcome::Success(value) => value,
        pb::create_candidate_result::Outcome::Error(error) => panic!("unexpected error: {error:?}"),
    };
    assert_eq!(
        mutation.operation.expect("operation").operation_id,
        "persisted-operation"
    );
    assert!(gateway.repository().calls.is_empty());
    assert_eq!(
        gateway.repository().stored_operation_ids,
        ["persisted-operation"]
    );
}

#[test]
fn resolve_candidate_routes_a_typed_receipt_without_legacy_candidate_judgments() {
    let mut gateway = gateway();
    let args = pb::ResolveCandidateArgs {
        context: Some(write_context("manager_resolve_candidate", "candidate/1")),
        candidate_id: "candidate-1".into(),
        expected_revision: Some(0),
        causal_parent: None,
        decision: Some(pb::resolve_candidate_args::Decision::Reject(
            pb::RejectResolution {
                reason_category: pb::ResolutionReasonCategory::NotActionable as i32,
                ..Default::default()
            },
        )),
    };
    let result = gateway
        .dispatch("manager_resolve_candidate", &args.encode_to_vec())
        .expect("typed result");
    let result = pb::ResolveCandidateResult::decode(result.as_slice()).expect("decode");
    assert!(matches!(
        result.outcome,
        Some(pb::resolve_candidate_result::Outcome::Success(receipt))
            if receipt.operation_id == "resolution-operation-1"
    ));
    assert_eq!(gateway.repository().calls, ["resolve_candidate"]);
}

#[test]
fn create_issue_is_unavailable_before_admission_even_without_evidence() {
    let mut gateway = gateway();
    let args = pb::CreateIssueArgs {
        context: Some(write_context("manager_create_issue", "issue/1")),
        issue_id: "issue-1".into(),
        issue: Some(v1::IssueDraft::default()),
        ..Default::default()
    };
    let result = gateway
        .dispatch("manager_create_issue", &args.encode_to_vec())
        .expect("typed result");
    let result = pb::CreateIssueResult::decode(result.as_slice()).expect("decode");
    assert!(matches!(
        result.outcome,
        Some(pb::create_issue_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::BackendUnavailable as i32 && !error.retryable
    ));
    assert_eq!(gateway.repository().idempotency_reservations, 0);
}

#[test]
fn sqlite_gateway_persists_restart_safe_claim_intent_without_a_lease_handle() {
    let (_config, mut manager) = real_manager();
    let args = pb::ClaimIssueArgs {
        context: Some(write_context("manager_claim_issue", "issue:issue-1")),
        issue_id: "issue-1".into(),
        expected_revision: Some(0),
        requested_lease: Some(pb::TimeSpan { millis: 5_000 }),
        gateway_eligibility_generation: None,
    };
    let result = manager
        .dispatch_manager("manager_claim_issue", &args.encode_to_vec())
        .expect("claim admission result");
    let mutation = match pb::ClaimIssueResult::decode(result.as_slice())
        .expect("decode claim result")
        .outcome
        .expect("claim outcome")
    {
        pb::claim_issue_result::Outcome::Success(mutation) => mutation,
        pb::claim_issue_result::Outcome::Error(error) => {
            panic!("unexpected claim error: {error:?}")
        }
    };
    let operation_id = mutation.operation.expect("claim operation").operation_id;
    let request = manager
        .storage_mut()
        .repository()
        .backend_request(&operation_id)
        .expect("read claim request")
        .expect("claim request exists");
    let envelope = super::backend_dispatch::DurableBackendRequest::decode(request.as_slice())
        .expect("decode durable claim request");
    assert_eq!(envelope.method, "manager_claim_issue");
    assert!(envelope.claim_issue.is_some());
    assert!(envelope.transition_issue.is_none());
    assert!(envelope.create_issue.is_none());
    assert!(
        !request
            .windows(b"lease_handle".len())
            .any(|window| { window == b"lease_handle" })
    );
}

#[test]
fn stale_gateway_eligibility_generation_is_rejected_before_claim_admission() {
    let (_config, mut manager) = real_manager();
    let stale_generation = manager.storage_mut().generation().saturating_add(1);
    let args = pb::ClaimIssueArgs {
        context: Some(write_context("manager_claim_issue", "issue:issue-1")),
        issue_id: "issue-1".into(),
        expected_revision: Some(0),
        requested_lease: Some(pb::TimeSpan { millis: 5_000 }),
        gateway_eligibility_generation: Some(stale_generation),
    };

    let result = manager
        .dispatch_manager("manager_claim_issue", &args.encode_to_vec())
        .expect("typed claim rejection");
    let result = pb::ClaimIssueResult::decode(result.as_slice()).expect("decode claim result");

    assert!(matches!(
        result.outcome,
        Some(pb::claim_issue_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::PreconditionFailed as i32
    ));
}

#[test]
fn lease_capability_writes_are_fail_closed_before_idempotency_admission() {
    let cases = [
        (
            "manager_heartbeat_issue_claim",
            pb::HeartbeatIssueClaimArgs {
                context: Some(write_context("manager_heartbeat_issue_claim", "issue/1")),
                issue_id: "issue-1".into(),
                expected_revision: Some(1),
                claim_revision: Some(1),
                lease_handle: b"never-persist".to_vec(),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        (
            "manager_release_issue_claim",
            pb::ReleaseIssueClaimArgs {
                context: Some(write_context("manager_release_issue_claim", "issue/1")),
                issue_id: "issue-1".into(),
                expected_revision: Some(1),
                claim_revision: Some(1),
                lease_handle: b"never-persist".to_vec(),
            }
            .encode_to_vec(),
        ),
    ];

    for (method, payload) in cases {
        let mut gateway = gateway();
        let result = gateway.dispatch(method, &payload).expect("typed result");
        let result = match method {
            "manager_heartbeat_issue_claim" => {
                pb::HeartbeatIssueClaimResult::decode(result.as_slice())
                    .expect("decode heartbeat result")
                    .outcome
                    .map(|outcome| match outcome {
                        pb::heartbeat_issue_claim_result::Outcome::Error(error) => error,
                        pb::heartbeat_issue_claim_result::Outcome::Success(_) => {
                            panic!("lease capability must not be admitted")
                        }
                    })
            }
            "manager_release_issue_claim" => pb::ReleaseIssueClaimResult::decode(result.as_slice())
                .expect("decode release result")
                .outcome
                .map(|outcome| match outcome {
                    pb::release_issue_claim_result::Outcome::Error(error) => error,
                    pb::release_issue_claim_result::Outcome::Success(_) => {
                        panic!("lease capability must not be admitted")
                    }
                }),
            _ => unreachable!(),
        }
        .expect("typed error outcome");
        assert_eq!(result.code, pb::ManagerErrorCode::BackendUnavailable as i32);
        assert!(!result.retryable);
        assert_eq!(gateway.repository().idempotency_reservations, 0);
    }
}

#[test]
fn missing_or_unspecified_actor_audit_is_rejected_before_admission() {
    for actor_audit in [None, Some(pb::ActorAudit::default())] {
        let mut gateway = gateway();
        let mut context = write_context("manager_create_candidate", "candidate/1");
        context.actor_audit = actor_audit;
        let args = pb::CreateCandidateArgs {
            context: Some(context),
            candidate: Some(candidate()),
        };
        let result = gateway
            .dispatch("manager_create_candidate", &args.encode_to_vec())
            .expect("typed result");
        let result = pb::CreateCandidateResult::decode(result.as_slice()).expect("decode");
        assert!(matches!(
            result.outcome,
            Some(pb::create_candidate_result::Outcome::Error(_))
        ));
        assert_eq!(gateway.repository().idempotency_reservations, 0);
    }
}

#[test]
fn extension_methods_decode_before_returning_typed_unavailable_outcomes() {
    let mut gateway = gateway();
    let malformed = gateway
        .dispatch("manager_get_issue_with_sync", &[0xff, 0xff])
        .expect("typed sync result");
    let malformed =
        pb::GetIssueWithSyncResult::decode(malformed.as_slice()).expect("decode sync result");
    assert!(matches!(
        malformed.outcome,
        Some(pb::get_issue_with_sync_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::ManagerInvalidArgument as i32
    ));
}

#[test]
fn candidate_read_is_typed_and_unknown_supported_method_is_not_generic() {
    let mut gateway = gateway();
    let args = pb::GetCandidateArgs {
        context: Some(read_context()),
        candidate_id: "candidate-1".into(),
    };
    let result = gateway
        .dispatch("manager_get_candidate", &args.encode_to_vec())
        .expect("typed result");
    let result = pb::GetCandidateResult::decode(result.as_slice()).expect("decode");
    assert!(matches!(
        result.outcome,
        Some(pb::get_candidate_result::Outcome::Success(candidate))
            if candidate.candidate_id == "candidate-1"
    ));

    let result = gateway
        .dispatch(
            "manager_get_issue_with_sync",
            &pb::GetIssueWithSyncArgs {
                context: Some(read_context()),
                issue_id: "issue-1".into(),
            }
            .encode_to_vec(),
        )
        .expect("typed result");
    let result = pb::GetIssueWithSyncResult::decode(result.as_slice()).expect("decode");
    assert!(matches!(
        result.outcome,
        Some(pb::get_issue_with_sync_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::BackendUnavailable as i32
    ));
}

#[test]
fn retry_and_reconcile_routes_reach_the_repository() {
    let mut gateway = gateway();
    let retry = pb::RetryOperationArgs {
        context: Some(write_context("manager_retry_operation", "operation/1")),
        operation_id: "operation-1".into(),
        ..Default::default()
    };
    let result = gateway
        .dispatch("manager_retry_operation", &retry.encode_to_vec())
        .expect("retry result");
    let result = pb::RetryOperationResult::decode(result.as_slice()).expect("decode retry result");
    assert!(matches!(
        result.outcome,
        Some(pb::retry_operation_result::Outcome::Success(_))
    ));

    let reconcile = pb::ReconcileOperationArgs {
        context: Some(write_context("manager_reconcile_operation", "operation/1")),
        operation_id: "operation-1".into(),
        ..Default::default()
    };
    let result = gateway
        .dispatch("manager_reconcile_operation", &reconcile.encode_to_vec())
        .expect("reconcile result");
    let result =
        pb::ReconcileOperationResult::decode(result.as_slice()).expect("decode reconcile result");
    assert!(matches!(
        result.outcome,
        Some(pb::reconcile_operation_result::Outcome::Success(_))
    ));
    assert_eq!(
        gateway.repository().calls,
        ["retry_operation", "reconcile_operation"]
    );
}

#[test]
fn context_mismatch_and_decode_failure_are_typed_errors() {
    let mut gateway = gateway();
    let args = pb::CreateCandidateArgs {
        context: Some(write_context("wrong_method", "candidate/1")),
        candidate: Some(candidate()),
    };
    let result = gateway
        .dispatch("manager_create_candidate", &args.encode_to_vec())
        .expect("typed result");
    let result = pb::CreateCandidateResult::decode(result.as_slice()).expect("decode");
    assert!(matches!(
        result.outcome,
        Some(pb::create_candidate_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::MethodTargetMismatch as i32
    ));
    let result = gateway
        .dispatch("manager_create_candidate", &[0xff, 0xff])
        .expect("typed result");
    let result = pb::CreateCandidateResult::decode(result.as_slice()).expect("decode");
    assert!(matches!(
        result.outcome,
        Some(pb::create_candidate_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::ManagerInvalidArgument as i32
    ));
}

fn real_manager() -> (super::ManagerConfig, super::IssueTrackerManager) {
    let mut config = super::config::tests::settings();
    config.maximum_payload_bytes = 64 * 1024;
    let root = tempfile::tempdir().expect("temporary manager root");
    config.data_root = root.keep().to_string_lossy().into_owned();
    let config = super::ManagerConfig::from_proto(&config).expect("valid settings");
    let manager = super::IssueTrackerManager::open(config.clone()).expect("open manager");
    (config, manager)
}

#[test]
fn sqlite_gateway_persists_typed_entities_and_lists_them() {
    let (_config, mut manager) = real_manager();
    let candidate_args = pb::CreateCandidateArgs {
        context: Some(write_context("manager_create_candidate", "candidate/1")),
        candidate: Some(candidate()),
    };
    let candidate_result = manager
        .dispatch_manager("manager_create_candidate", &candidate_args.encode_to_vec())
        .expect("candidate result");
    let candidate_result = pb::CreateCandidateResult::decode(candidate_result.as_slice())
        .expect("decode candidate result");
    assert!(
        matches!(
            candidate_result.outcome,
            Some(pb::create_candidate_result::Outcome::Success(ref mutation))
                if matches!(mutation.entity, Some(pb::manager_mutation::Entity::Candidate(_)))
        ),
        "unexpected candidate outcome: {:?}",
        candidate_result.outcome
    );


    let extraction_args = pb::CreateExtractionRunArgs {
        context: Some(write_context("manager_create_extraction_run", "run/1")),
        run: Some(pb::ExtractionRunDraft {
            source: "memories".into(),
            request_digest: "sha256:request".into(),
            request_schema: "thread-summary.v1".into(),
            ..Default::default()
        }),
    };
    let extraction_result = manager
        .dispatch_manager(
            "manager_create_extraction_run",
            &extraction_args.encode_to_vec(),
        )
        .expect("extraction result");
    assert!(matches!(
        pb::CreateExtractionRunResult::decode(extraction_result.as_slice())
            .expect("decode extraction result")
            .outcome,
        Some(pb::create_extraction_run_result::Outcome::Success(mutation))
            if matches!(mutation.entity, Some(pb::manager_mutation::Entity::ExtractionRun(_)))
    ));

    let candidate = manager
        .dispatch_manager(
            "manager_get_candidate",
            &pb::GetCandidateArgs {
                context: Some(read_context()),
                candidate_id: "candidate-1".into(),
            }
            .encode_to_vec(),
        )
        .expect("candidate get");
    assert!(matches!(
        pb::GetCandidateResult::decode(candidate.as_slice())
            .expect("decode candidate get")
            .outcome,
        Some(pb::get_candidate_result::Outcome::Success(value))
            if value.candidate_id == "candidate-1"
    ));
    let candidates = manager
        .dispatch_manager(
            "manager_list_candidates",
            &pb::ListCandidatesArgs {
                context: Some(read_context()),
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .expect("candidate list");
    assert!(matches!(
        pb::ListCandidatesResult::decode(candidates.as_slice())
            .expect("decode candidate list")
            .outcome,
        Some(pb::list_candidates_result::Outcome::Success(page))
            if page.candidates.len() == 1
    ));


    let extraction = manager
        .dispatch_manager(
            "manager_get_extraction_run",
            &pb::GetExtractionRunArgs {
                context: Some(read_context()),
                run_id: "run-1".into(),
            }
            .encode_to_vec(),
        )
        .expect("extraction get");
    assert!(matches!(
        pb::GetExtractionRunResult::decode(extraction.as_slice())
            .expect("decode extraction get")
            .outcome,
        Some(pb::get_extraction_run_result::Outcome::Success(value))
            if value.run_id == "run-1"
    ));
    let extraction_list = manager
        .dispatch_manager(
            "manager_list_extraction_runs",
            &pb::ListExtractionRunsArgs {
                context: Some(read_context()),
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .expect("extraction list");
    assert!(matches!(
        pb::ListExtractionRunsResult::decode(extraction_list.as_slice())
            .expect("decode extraction list")
            .outcome,
        Some(pb::list_extraction_runs_result::Outcome::Success(page))
            if page.runs.len() == 1
    ));
}

#[test]
fn sqlite_gateway_replays_idempotency_and_rejects_a_digest_conflict() {
    let (config, mut manager) = real_manager();
    let args = pb::CreateCandidateArgs {
        context: Some(write_context("manager_create_candidate", "candidate/1")),
        candidate: Some(candidate()),
    };
    let first = manager
        .dispatch_manager("manager_create_candidate", &args.encode_to_vec())
        .expect("first result");
    let first_result = pb::CreateCandidateResult::decode(first.as_slice()).expect("decode first");
    let operation = match first_result.outcome {
        Some(pb::create_candidate_result::Outcome::Success(mutation)) => mutation
            .operation
            .expect("successful mutation has operation"),
        _ => panic!("expected successful mutation"),
    };
    assert_eq!(
        pb::DurableOperationState::try_from(operation.state).expect("known operation state"),
        pb::DurableOperationState::Committed
    );
    let operation_result = manager
        .dispatch_manager(
            "manager_get_operation",
            &pb::GetOperationArgs {
                context: Some(read_context()),
                operation_id: operation.operation_id.clone(),
            }
            .encode_to_vec(),
        )
        .expect("get persisted operation");
    let persisted = match pb::GetOperationResult::decode(operation_result.as_slice())
        .expect("decode persisted operation")
        .outcome
    {
        Some(pb::get_operation_result::Outcome::Success(value)) => value,
        _ => panic!("expected persisted operation"),
    };
    assert_eq!(operation, persisted);
    let replay = manager
        .dispatch_manager("manager_create_candidate", &args.encode_to_vec())
        .expect("replayed result");
    assert_eq!(first, replay);

    let mut changed = args.clone();
    changed.candidate.as_mut().expect("candidate").title = "changed".into();
    let conflict = manager
        .dispatch_manager("manager_create_candidate", &changed.encode_to_vec())
        .expect("typed conflict result");
    let conflict = pb::CreateCandidateResult::decode(conflict.as_slice()).expect("decode conflict");
    assert!(matches!(
        conflict.outcome,
        Some(pb::create_candidate_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::IdempotencyConflict as i32
    ));

    drop(manager);
    let mut reopened = super::IssueTrackerManager::open(config).expect("reopen manager");
    let replay_after_reopen = reopened
        .dispatch_manager("manager_create_candidate", &args.encode_to_vec())
        .expect("replayed after reopen");
    assert_eq!(first, replay_after_reopen);
    let operation_after_reopen = reopened
        .dispatch_manager(
            "manager_get_operation",
            &pb::GetOperationArgs {
                context: Some(read_context()),
                operation_id: operation.operation_id,
            }
            .encode_to_vec(),
        )
        .expect("get persisted operation after reopen");
    let operation_after_reopen =
        match pb::GetOperationResult::decode(operation_after_reopen.as_slice())
            .expect("decode persisted operation after reopen")
            .outcome
        {
            Some(pb::get_operation_result::Outcome::Success(value)) => value,
            _ => panic!("expected persisted operation after reopen"),
        };
    assert_eq!(persisted, operation_after_reopen);
    let candidate = reopened
        .dispatch_manager(
            "manager_get_candidate",
            &pb::GetCandidateArgs {
                context: Some(read_context()),
                candidate_id: "candidate-1".into(),
            }
            .encode_to_vec(),
        )
        .expect("get after reopen");
    assert!(matches!(
        pb::GetCandidateResult::decode(candidate.as_slice())
            .expect("decode candidate after reopen")
            .outcome,
        Some(pb::get_candidate_result::Outcome::Success(value))
            if value.title == "title"
    ));
}
