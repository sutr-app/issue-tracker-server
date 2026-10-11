use super::*;

#[test]
fn resolve_reject_commits_candidate_and_returns_a_durable_receipt() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-1",
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate(0),
        )
        .expect("seed candidate");

    let args = pb::ResolveCandidateArgs {
        context: Some(write_context(
            "manager_resolve_candidate",
            "candidate:candidate-1",
        )),
        candidate_id: "candidate-1".into(),
        expected_revision: Some(0),
        decision: Some(pb::resolve_candidate_args::Decision::Reject(
            pb::RejectResolution {
                reason_category: pb::ResolutionReasonCategory::NotActionable as i32,
                rationale: Some("  no longer actionable  ".into()),
            },
        )),
        ..Default::default()
    };
    let receipt =
        GatewayRepository::resolve_candidate(&mut repository, &args).expect("resolve rejection");

    assert_eq!(receipt.state, pb::DurableOperationState::Committed as i32);
    assert_eq!(
        receipt
            .candidate
            .as_ref()
            .expect("candidate reference")
            .revision,
        Some(1)
    );
    let stored = repository
        .get_candidate(&pb::GetCandidateArgs {
            candidate_id: "candidate-1".into(),
            ..Default::default()
        })
        .expect("read candidate");
    assert_eq!(stored.status, pb::CandidateStatus::Rejected as i32);
    assert_eq!(stored.revision, 1);
    let operation = repository
        .get_operation(&receipt.operation_id)
        .expect("read operation")
        .expect("operation exists");
    assert_resolve_candidate_success_snapshot(&operation, &receipt);
    let replay = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("replay committed rejection");
    assert_eq!(replay, receipt);
    let history = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest::default()),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("read operation history");
    let entry = history
        .entries
        .iter()
        .find(|entry| entry.operation_id == receipt.operation_id)
        .expect("resolution history entry");
    assert_eq!(
        entry.resolution_reason_category,
        Some(pb::ResolutionReasonCategory::NotActionable as i32)
    );
    assert_eq!(
        entry.resolution_rationale.as_deref(),
        Some("no longer actionable")
    );
    assert_eq!(
        entry.resolution_decision_kind,
        Some(pb::ResolutionDecisionKind::Reject as i32)
    );
    assert_eq!(entry.candidate_revision_at_decision, Some(0));
}

#[test]
fn resolve_candidate_merge_commits_a_replayable_result_snapshot() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let source = candidate(0);
    let mut target = candidate(0);
    target.candidate_id = "candidate-2".into();
    repository
        .put_proto(
            EntityKind::Candidate,
            &source.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            &source,
        )
        .expect("seed source candidate");
    repository
        .put_proto(
            EntityKind::Candidate,
            &target.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            &target,
        )
        .expect("seed target candidate");
    let mut context = write_context("manager_resolve_candidate", "candidate:candidate-1");
    context.actor_audit = Some(pb::ActorAudit {
        actor_id: "reviewer".into(),
        actor_kind: v1::ActorKind::Human as i32,
        reason: Some("reviewed duplicate candidates".into()),
        ..Default::default()
    });
    let args = pb::ResolveCandidateArgs {
        context: Some(context),
        candidate_id: source.candidate_id.clone(),
        expected_revision: Some(0),
        decision: Some(pb::resolve_candidate_args::Decision::Merge(
            pb::MergeResolution {
                target: Some(pb::merge_resolution::Target::Candidate(
                    pb::CandidateMergeTarget {
                        candidate_id: target.candidate_id.clone(),
                        expected_revision: Some(0),
                    },
                )),
                basis: pb::ResolutionBasis::HumanReview as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    };

    let receipt = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("resolve candidate merge");
    let operation = repository
        .get_operation(&receipt.operation_id)
        .expect("read operation")
        .expect("operation exists");
    assert_resolve_candidate_success_snapshot(&operation, &receipt);
    let replay = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("replay committed candidate merge");
    assert_eq!(replay, receipt);
}

#[test]
fn resolve_reject_requires_a_reason_category_before_mutating() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-1",
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate(0),
        )
        .expect("seed candidate");

    let error = GatewayRepository::resolve_candidate(
        &mut repository,
        &pb::ResolveCandidateArgs {
            context: Some(write_context(
                "manager_resolve_candidate",
                "candidate:candidate-1",
            )),
            candidate_id: "candidate-1".into(),
            expected_revision: Some(0),
            decision: Some(pb::resolve_candidate_args::Decision::Reject(
                pb::RejectResolution::default(),
            )),
            ..Default::default()
        },
    )
    .expect_err("missing reason category must fail");

    assert_eq!(
        error.code,
        pb::ManagerErrorCode::ManagerInvalidArgument as i32
    );
    assert_eq!(
        repository
            .get(EntityKind::Candidate, "candidate-1")
            .expect("read candidate")
            .expect("candidate exists")
            .revision,
        0
    );
    assert_eq!(
        repository
            .storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_idempotency")
            .expect("count idempotency records"),
        0
    );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_operation")
            .expect("count operations"),
        0
    );
}

#[test]
fn resolve_reject_rolls_back_when_storing_its_idempotency_result_fails() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-1",
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate(0),
        )
        .expect("seed candidate");
    let sequence_before = repository.storage.manager_sequence();
    let generation_before = repository.storage.generation();
    repository
        .storage
        .read(|connection| {
            connection
                .execute_batch(
                    "CREATE TRIGGER reject_idempotency_result
                           BEFORE UPDATE OF result_snapshot ON manager_idempotency
                           BEGIN
                             SELECT RAISE(ABORT, 'injected idempotency result failure');
                           END;",
                )
                .map_err(ManagerStorageError::from)
        })
        .expect("install failure trigger");

    GatewayRepository::resolve_candidate(
        &mut repository,
        &pb::ResolveCandidateArgs {
            context: Some(write_context(
                "manager_resolve_candidate",
                "candidate:candidate-1",
            )),
            candidate_id: "candidate-1".into(),
            expected_revision: Some(0),
            decision: Some(pb::resolve_candidate_args::Decision::Reject(
                pb::RejectResolution {
                    reason_category: pb::ResolutionReasonCategory::NotActionable as i32,
                    ..Default::default()
                },
            )),
            ..Default::default()
        },
    )
    .expect_err("idempotency result failure aborts the rejection");

    assert_eq!(
        repository
            .get(EntityKind::Candidate, "candidate-1")
            .expect("read candidate")
            .expect("candidate exists")
            .revision,
        0
    );
    assert_eq!(
        repository
            .storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_operation")
            .expect("count operations"),
        0
    );
    assert_eq!(
        repository
            .storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_idempotency")
            .expect("count idempotency records"),
        0
    );
    assert_eq!(repository.storage.manager_sequence(), sequence_before);
    assert_eq!(repository.storage.generation(), generation_before);
}

#[test]
fn resolve_confirm_admits_a_durable_candidate_saga_before_backend_dispatch() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-1",
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate(0),
        )
        .expect("seed candidate");
    let mut context = write_context("manager_resolve_candidate", "candidate:candidate-1");
    context.correlation_id = "confirm-saga-test".into();
    context.actor_audit = Some(pb::ActorAudit {
        actor_id: "reviewer".into(),
        actor_kind: v1::ActorKind::Human as i32,
        reason: Some("reviewed associated issue".into()),
        ..Default::default()
    });
    let args = pb::ResolveCandidateArgs {
        context: Some(context),
        candidate_id: "candidate-1".into(),
        expected_revision: Some(0),
        decision: Some(
            pb::resolve_candidate_args::Decision::ConfirmAssociatedIssue(
                pb::ConfirmAssociatedIssueResolution {
                    issue_id: "issue-1".into(),
                    expected_revision: Some(4),
                    basis: pb::ResolutionBasis::HumanReview as i32,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };

    let receipt = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("admit confirm resolution");
    assert_eq!(receipt.state, pb::DurableOperationState::Pending as i32);
    assert_eq!(receipt.mappings.len(), 1);
    let replay = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("pending confirm resolution replays its receipt");
    assert_eq!(replay.operation_id, receipt.operation_id);
    assert_eq!(replay.issues, receipt.issues);
    assert_eq!(replay.mappings, receipt.mappings);
    assert_eq!(
        repository
            .storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_resolution_saga")
            .expect("count saga rows"),
        1
    );
    let operation = repository
        .get_operation(&receipt.operation_id)
        .expect("operation read")
        .expect("operation exists");
    assert!(matches!(
        operation.scope,
        Some(pb::durable_operation::Scope::CandidateId(ref id)) if id == "candidate-1"
    ));
    assert_eq!(operation.method, "manager_resolve_candidate");
    let mut committed = operation;
    committed.state = pb::DurableOperationState::Committed as i32;
    let outbox = repository
        .get_outbox(&format!("outbox-{}", committed.operation_id))
        .expect("outbox read")
        .expect("outbox exists");
    repository
        .finalize_backend_dispatch(
            &committed,
            &outbox,
            &[pb::IssueMapping {
                issue_id: "issue-1".into(),
                backend_id: "backend-issue-1".into(),
                revision: 5,
                ..Default::default()
            }],
        )
        .expect("finalize confirm saga");
    assert_eq!(
        repository
            .get_candidate(&pb::GetCandidateArgs {
                candidate_id: "candidate-1".into(),
                ..Default::default()
            })
            .expect("candidate read")
            .status,
        pb::CandidateStatus::Accepted as i32
    );
    let mapping: pb::IssueMapping = decode_stored(
        repository
            .get(EntityKind::IssueMapping, "issue-1")
            .expect("mapping read")
            .expect("mapping exists"),
        "issue_tracker.manager.v1.IssueMapping",
    )
    .expect("decode mapping");
    assert_eq!(mapping.candidate_id.as_deref(), Some("candidate-1"));
    let operation = repository
        .get_operation(&receipt.operation_id)
        .expect("read finalized operation")
        .expect("finalized operation exists");
    let replay = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("replay finalized confirmation");
    assert_resolve_candidate_success_snapshot(&operation, &replay);
}

#[test]
fn resolve_cancel_admits_a_durable_saga_without_rejecting_before_dispatch() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let mut source = candidate(0);
    source.status = pb::CandidateStatus::Accepted as i32;
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-1",
            0,
            "issue_tracker.manager.v1.Candidate",
            &source,
        )
        .expect("seed accepted candidate");
    repository
        .put_proto(
            EntityKind::IssueMapping,
            "issue-1",
            4,
            "issue_tracker.manager.v1.IssueMapping",
            &pb::IssueMapping {
                issue_id: "issue-1".into(),
                candidate_id: Some("candidate-1".into()),
                revision: 4,
                ..Default::default()
            },
        )
        .expect("seed mapping");
    let mut context = write_context("manager_resolve_candidate", "candidate:candidate-1");
    context.correlation_id = "cancel-saga-test".into();
    context.actor_audit = Some(pb::ActorAudit {
        actor_id: "reviewer".into(),
        actor_kind: v1::ActorKind::Human as i32,
        ..Default::default()
    });
    let args = pb::ResolveCandidateArgs {
        context: Some(context),
        candidate_id: "candidate-1".into(),
        expected_revision: Some(0),
        decision: Some(pb::resolve_candidate_args::Decision::CancelAssociatedIssue(
            pb::CancelAssociatedIssueResolution {
                issue_id: "issue-1".into(),
                expected_issue_revision: Some(4),
                expected_mapping_candidate_id: "candidate-1".into(),
                expected_mapping_issue_id: "issue-1".into(),
                cancellation_report: Some(v1::CancellationReport {
                    classification: v1::CancellationClass::FalsePositive as i32,
                }),
                reason_category: pb::ResolutionReasonCategory::NotActionable as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    };

    let receipt = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("admit cancellation resolution");
    assert_eq!(receipt.state, pb::DurableOperationState::Pending as i32);
    assert!(receipt.mappings.is_empty());
    let history = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest::default()),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("read pending cancellation history");
    let entry = history
        .entries
        .iter()
        .find(|entry| entry.operation_id == receipt.operation_id)
        .expect("cancellation history entry");
    assert_eq!(
        entry.resolution_reason_category,
        Some(pb::ResolutionReasonCategory::NotActionable as i32)
    );
    assert_eq!(entry.resolution_rationale, None);
    assert_eq!(
        entry.resolution_decision_kind,
        Some(pb::ResolutionDecisionKind::CancelAssociatedIssue as i32)
    );
    assert_eq!(entry.candidate_revision_at_decision, Some(0));
    let mut decision_history_args = history_args(pb::PageRequest::default());
    decision_history_args
        .filter
        .as_mut()
        .expect("history filter")
        .decision_kinds = vec![pb::ResolutionDecisionKind::CancelAssociatedIssue as i32];
    let decision_history = GatewayRepository::list_operation_history(
        &repository,
        &decision_history_args,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("filter cancellation decision history");
    assert_eq!(decision_history.entries.len(), 1);
    assert_eq!(
        decision_history.entries[0].operation_id,
        receipt.operation_id
    );
    assert_eq!(
        repository
            .storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_resolution_saga")
            .expect("count saga rows"),
        1
    );
    assert_eq!(
        repository
            .get_candidate(&pb::GetCandidateArgs {
                candidate_id: "candidate-1".into(),
                ..Default::default()
            })
            .expect("candidate read")
            .status,
        pb::CandidateStatus::Accepted as i32
    );
    let mut committed = repository
        .get_operation(&receipt.operation_id)
        .expect("operation read")
        .expect("operation exists");
    committed.state = pb::DurableOperationState::Committed as i32;
    let outbox = repository
        .get_outbox(&format!("outbox-{}", committed.operation_id))
        .expect("outbox read")
        .expect("outbox exists");
    repository
        .finalize_backend_dispatch(
            &committed,
            &outbox,
            &[pb::IssueMapping {
                issue_id: "issue-1".into(),
                backend_id: "backend-issue-1".into(),
                revision: 5,
                ..Default::default()
            }],
        )
        .expect("finalize cancellation saga");
    assert_eq!(
        repository
            .get_candidate(&pb::GetCandidateArgs {
                candidate_id: "candidate-1".into(),
                ..Default::default()
            })
            .expect("candidate read")
            .status,
        pb::CandidateStatus::Rejected as i32
    );
    assert!(
        repository
            .get(EntityKind::IssueMapping, "issue-1")
            .expect("mapping read")
            .is_none()
    );
    let history = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest::default()),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("read completed cancellation history");
    let entry = history
        .entries
        .iter()
        .find(|entry| entry.operation_id == receipt.operation_id)
        .expect("completed cancellation history entry");
    assert_eq!(
        entry.resolution_reason_category,
        Some(pb::ResolutionReasonCategory::NotActionable as i32)
    );
    assert_eq!(entry.resolution_rationale, None);
}

#[test]
fn confirm_saga_post_commit_candidate_conflict_is_terminally_traceable() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-1",
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate(0),
        )
        .expect("seed candidate");
    let mut context = write_context("manager_resolve_candidate", "candidate:candidate-1");
    context.correlation_id = "confirm-post-commit-conflict".into();
    context.actor_audit = Some(pb::ActorAudit {
        actor_id: "reviewer".into(),
        actor_kind: v1::ActorKind::Human as i32,
        reason: Some("reviewed associated issue".into()),
        ..Default::default()
    });
    let args = pb::ResolveCandidateArgs {
        context: Some(context),
        candidate_id: "candidate-1".into(),
        expected_revision: Some(0),
        decision: Some(
            pb::resolve_candidate_args::Decision::ConfirmAssociatedIssue(
                pb::ConfirmAssociatedIssueResolution {
                    issue_id: "issue-1".into(),
                    expected_revision: Some(4),
                    basis: pb::ResolutionBasis::HumanReview as i32,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let receipt = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("admit confirm resolution");

    let mut changed_candidate = candidate(1);
    changed_candidate.decision_note = Some("concurrent human change".into());
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-1",
            1,
            "issue_tracker.manager.v1.Candidate",
            &changed_candidate,
        )
        .expect("simulate post-admission candidate change");
    let mut committed = repository
        .get_operation(&receipt.operation_id)
        .expect("operation read")
        .expect("operation exists");
    committed.state = pb::DurableOperationState::Committed as i32;
    let outbox = repository
        .get_outbox(&format!("outbox-{}", committed.operation_id))
        .expect("outbox read")
        .expect("outbox exists");

    repository
        .finalize_backend_dispatch(
            &committed,
            &outbox,
            &[pb::IssueMapping {
                issue_id: "issue-1".into(),
                backend_id: "backend-issue-1".into(),
                revision: 5,
                ..Default::default()
            }],
        )
        .expect("post-commit conflict must be made terminal");

    let operation = repository
        .get_operation(&receipt.operation_id)
        .expect("operation read")
        .expect("operation exists");
    assert_eq!(
        operation.state,
        pb::DurableOperationState::ManualResolution as i32
    );
    assert!(operation.resolution_required);
    let retained_candidate = repository
        .get_candidate(&pb::GetCandidateArgs {
            candidate_id: "candidate-1".into(),
            ..Default::default()
        })
        .expect("candidate read");
    assert_eq!(retained_candidate.revision, 1);
    assert_eq!(
        retained_candidate.status,
        pb::CandidateStatus::Proposed as i32
    );
    assert!(
        repository
            .get(EntityKind::IssueMapping, "issue-1")
            .expect("mapping read")
            .is_none()
    );
    assert_eq!(
        repository
            .get_outbox(&format!("outbox-{}", receipt.operation_id))
            .expect("outbox read")
            .expect("outbox exists")
            .status,
        pb::OutboxStatus::DeadLetter as i32
    );
    assert_eq!(
        repository
            .storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_resolution_saga")
            .expect("count saga rows"),
        0
    );
    let replay = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("idempotency replay retains the durable operation reference");
    assert_eq!(replay.operation_id, receipt.operation_id);
    assert_eq!(
        replay.state,
        pb::DurableOperationState::ManualResolution as i32
    );
}

#[test]
fn cancel_saga_post_commit_mapping_conflict_is_terminally_traceable() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let mut source = candidate(0);
    source.status = pb::CandidateStatus::Accepted as i32;
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-1",
            0,
            "issue_tracker.manager.v1.Candidate",
            &source,
        )
        .expect("seed accepted candidate");
    repository
        .put_proto(
            EntityKind::IssueMapping,
            "issue-1",
            4,
            "issue_tracker.manager.v1.IssueMapping",
            &pb::IssueMapping {
                issue_id: "issue-1".into(),
                candidate_id: Some("candidate-1".into()),
                revision: 4,
                ..Default::default()
            },
        )
        .expect("seed mapping");
    let mut context = write_context("manager_resolve_candidate", "candidate:candidate-1");
    context.correlation_id = "cancel-post-commit-conflict".into();
    context.actor_audit = Some(pb::ActorAudit {
        actor_id: "reviewer".into(),
        actor_kind: v1::ActorKind::Human as i32,
        ..Default::default()
    });
    let args = pb::ResolveCandidateArgs {
        context: Some(context),
        candidate_id: "candidate-1".into(),
        expected_revision: Some(0),
        decision: Some(pb::resolve_candidate_args::Decision::CancelAssociatedIssue(
            pb::CancelAssociatedIssueResolution {
                issue_id: "issue-1".into(),
                expected_issue_revision: Some(4),
                expected_mapping_candidate_id: "candidate-1".into(),
                expected_mapping_issue_id: "issue-1".into(),
                cancellation_report: Some(v1::CancellationReport {
                    classification: v1::CancellationClass::FalsePositive as i32,
                }),
                reason_category: pb::ResolutionReasonCategory::NotActionable as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let receipt = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("admit cancellation resolution");

    repository
        .put_proto(
            EntityKind::IssueMapping,
            "issue-1",
            5,
            "issue_tracker.manager.v1.IssueMapping",
            &pb::IssueMapping {
                issue_id: "issue-1".into(),
                candidate_id: Some("candidate-other".into()),
                revision: 5,
                ..Default::default()
            },
        )
        .expect("simulate post-admission mapping change");
    let mut committed = repository
        .get_operation(&receipt.operation_id)
        .expect("operation read")
        .expect("operation exists");
    committed.state = pb::DurableOperationState::Committed as i32;
    let outbox = repository
        .get_outbox(&format!("outbox-{}", committed.operation_id))
        .expect("outbox read")
        .expect("outbox exists");

    repository
        .finalize_backend_dispatch(
            &committed,
            &outbox,
            &[pb::IssueMapping {
                issue_id: "issue-1".into(),
                backend_id: "backend-issue-1".into(),
                revision: 6,
                ..Default::default()
            }],
        )
        .expect("post-commit conflict must be made terminal");

    let operation = repository
        .get_operation(&receipt.operation_id)
        .expect("operation read")
        .expect("operation exists");
    assert_eq!(
        operation.state,
        pb::DurableOperationState::ManualResolution as i32
    );
    assert!(operation.resolution_required);
    assert_eq!(
        repository
            .get_candidate(&pb::GetCandidateArgs {
                candidate_id: "candidate-1".into(),
                ..Default::default()
            })
            .expect("candidate read")
            .status,
        pb::CandidateStatus::Accepted as i32
    );
    let retained_mapping: pb::IssueMapping = decode_stored(
        repository
            .get(EntityKind::IssueMapping, "issue-1")
            .expect("mapping read")
            .expect("mapping exists"),
        "issue_tracker.manager.v1.IssueMapping",
    )
    .expect("decode mapping");
    assert_eq!(
        retained_mapping.candidate_id.as_deref(),
        Some("candidate-other")
    );
    assert_eq!(retained_mapping.revision, 5);
    assert_eq!(
        repository
            .get_outbox(&format!("outbox-{}", receipt.operation_id))
            .expect("outbox read")
            .expect("outbox exists")
            .status,
        pb::OutboxStatus::DeadLetter as i32
    );
    assert_eq!(
        repository
            .storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_resolution_saga")
            .expect("count saga rows"),
        0
    );
    let replay = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("idempotency replay retains the durable operation reference");
    assert_eq!(replay.operation_id, receipt.operation_id);
    assert_eq!(
        replay.state,
        pb::DurableOperationState::ManualResolution as i32
    );
}
