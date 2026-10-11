use super::*;

#[test]
fn human_review_requires_an_audit_reason() {
    let context = write_context("manager_resolve_candidate", "candidate:candidate-1");

    let error = require_human_review(&context, &candidate(0), None)
        .expect_err("human review without an audit reason must fail");

    assert_eq!(
        error.code,
        pb::ManagerErrorCode::ManagerInvalidArgument as i32
    );
}

#[test]
fn gateway_policy_round_trip_is_endpoint_bound() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let policy = pb::WorkEligibilityPolicy {
        mode: pb::WorkEligibilityMode::IncludeAutoRegistered as i32,
    };

    let registered = repository
        .register_gateway_policy("worker-listener-a", "gateway-a", "register-1", &policy)
        .expect("register gateway policy");
    let loaded = repository
        .load_gateway_policy("worker-listener-a")
        .expect("load gateway policy")
        .expect("registration exists");

    assert_eq!(loaded.gateway_id, "gateway-a");
    assert_eq!(loaded.policy, Some(policy));
    assert_eq!(loaded.policy_fingerprint, registered.policy_fingerprint);
    assert!(
        repository
            .load_gateway_policy("worker-listener-b")
            .expect("load unregistered gateway")
            .is_none()
    );
}

#[test]
fn gateway_registration_replays_only_the_same_policy_for_an_idempotency_key() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let reviewed = pb::WorkEligibilityPolicy {
        mode: pb::WorkEligibilityMode::ReviewedOnly as i32,
    };

    let first = repository
        .register_gateway_policy("worker-a", "gateway-a", "register-a", &reviewed)
        .expect("initial registration");
    let generation_after_first = repository.storage.generation();
    let replay = repository
        .register_gateway_policy("worker-a", "gateway-a", "register-a", &reviewed)
        .expect("same request replay");

    assert_eq!(replay, first);
    assert_eq!(repository.storage.generation(), generation_after_first);
    let conflict = repository
        .register_gateway_policy(
            "worker-a",
            "gateway-a",
            "register-a",
            &pb::WorkEligibilityPolicy {
                mode: pb::WorkEligibilityMode::IncludeAutoRegistered as i32,
            },
        )
        .expect_err("a reused key with another policy must conflict");
    assert!(matches!(conflict, RepositoryError::IdempotencyConflict));
}

#[test]
fn gateway_policy_rejects_unspecified_mode() {
    let error = validate_gateway_registration_input(
        "worker-listener-a",
        "gateway-a",
        &pb::WorkEligibilityPolicy::default(),
    )
    .expect_err("unspecified policy must not be persisted");

    assert!(matches!(error, RepositoryError::RequestSnapshot(_)));
}

#[test]
fn gateway_replay_is_limited_to_the_current_capability_epoch() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);

    let epoch_one = repository
        .start_gateway_capability_epoch("worker-a")
        .expect("start first epoch");
    assert!(matches!(
        repository
            .reserve_gateway_replay("worker-a", &epoch_one, "key-1", "Claim", "digest-a")
            .expect("reserve replay"),
        GatewayReplayDecision::New
    ));
    repository
        .complete_gateway_replay(
            "worker-a",
            &epoch_one,
            "key-1",
            "Claim",
            "digest-a",
            "issue_tracker.rpc.v1.IssueGatewayClaimed",
            b"successful response",
        )
        .expect("complete replay");
    assert!(matches!(
        repository
            .inspect_gateway_replay("worker-a", &epoch_one, "key-1", "Claim", "digest-a")
            .expect("inspect current epoch"),
        GatewayReplayDecision::Replay { response_snapshot, .. }
            if response_snapshot == b"successful response"
    ));

    let epoch_two = repository
        .start_gateway_capability_epoch("worker-a")
        .expect("restart creates a new epoch");
    assert_ne!(epoch_one, epoch_two);
    assert!(matches!(
        repository
            .inspect_gateway_replay("worker-a", &epoch_two, "key-1", "Claim", "digest-a")
            .expect("old success is not replayed"),
        GatewayReplayDecision::Unconfirmed
    ));
    assert!(matches!(
        repository
            .inspect_gateway_replay("worker-a", &epoch_two, "key-1", "Claim", "digest-b")
            .expect("old key with a different request conflicts"),
        GatewayReplayDecision::Conflict
    ));

    let completion_method = "complete_execution_to_awaiting_review";
    let completion_schema = "issue_tracker.rpc.v1.IssueGatewayAwaitingReviewMutation";
    assert!(matches!(
        repository
            .reserve_gateway_replay(
                "worker-a",
                &epoch_two,
                "completion-key",
                completion_method,
                "completion-digest",
            )
            .expect("reserve safe completion replay"),
        GatewayReplayDecision::New
    ));
    repository
        .complete_gateway_replay(
            "worker-a",
            &epoch_two,
            "completion-key",
            completion_method,
            "completion-digest",
            completion_schema,
            b"safe immutable receipt",
        )
        .expect("persist safe completion receipt");
    let epoch_three = repository
        .start_gateway_capability_epoch("worker-a")
        .expect("next restart creates another epoch");
    assert!(matches!(
        repository
            .inspect_gateway_replay(
                "worker-a",
                &epoch_three,
                "completion-key",
                completion_method,
                "completion-digest",
            )
            .expect("safe completed receipt replays across restart"),
        GatewayReplayDecision::Replay { response_snapshot, response_schema: actual_schema }
            if response_snapshot == b"safe immutable receipt" && actual_schema == completion_schema
    ));
    assert!(matches!(
        repository
            .inspect_gateway_replay(
                "worker-a",
                &epoch_three,
                "completion-key",
                completion_method,
                "different-digest",
            )
            .expect("changed completion request conflicts across restart"),
        GatewayReplayDecision::Conflict
    ));
}

#[test]
fn gateway_replay_reservation_keeps_incomplete_results_unconfirmed() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let epoch = repository
        .start_gateway_capability_epoch("worker-a")
        .expect("start epoch");

    assert!(matches!(
        repository
            .reserve_gateway_replay("worker-a", &epoch, "key-1", "Transition", "digest-a")
            .expect("reserve replay"),
        GatewayReplayDecision::New
    ));
    assert!(matches!(
        repository
            .inspect_gateway_replay("worker-a", &epoch, "key-1", "Transition", "digest-a")
            .expect("incomplete replay"),
        GatewayReplayDecision::Unconfirmed
    ));
    assert!(matches!(
        repository
            .inspect_gateway_replay("worker-a", &epoch, "key-1", "Heartbeat", "digest-a")
            .expect("method reuse conflicts"),
        GatewayReplayDecision::Conflict
    ));
}

#[test]
fn awaiting_review_pending_replay_can_only_be_completed_from_canonical_receipt() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let endpoint_id = "worker-awaiting-recovery";
    let method = "complete_execution_to_awaiting_review";
    let schema = "issue_tracker.rpc.v1.IssueGatewayAwaitingReviewMutation";
    let digest = "sha256:request-and-registration-snapshot";
    let first_epoch = repository
        .start_gateway_capability_epoch(endpoint_id)
        .expect("start original epoch");

    assert_eq!(
        repository
            .reserve_gateway_replay(endpoint_id, &first_epoch, "run-key", method, digest)
            .expect("reserve completion"),
        GatewayReplayDecision::New
    );
    let retry_epoch = repository
        .start_gateway_capability_epoch(endpoint_id)
        .expect("restart capability epoch");
    assert_eq!(
        repository
            .reserve_gateway_replay(endpoint_id, &retry_epoch, "run-key", method, digest)
            .expect("identify recoverable canonical completion"),
        GatewayReplayDecision::AwaitingReviewRecovery
    );

    repository
        .complete_gateway_replay_from_canonical(
            endpoint_id,
            "run-key",
            method,
            digest,
            schema,
            b"canonical immutable receipt",
        )
        .expect("persist canonical recovery receipt");
    assert!(matches!(
        repository
            .inspect_gateway_replay(endpoint_id, &retry_epoch, "run-key", method, digest)
            .expect("inspect recovered receipt"),
        GatewayReplayDecision::Replay { response_snapshot, response_schema }
            if response_snapshot == b"canonical immutable receipt" && response_schema == schema
    ));
    assert!(matches!(
        repository
            .inspect_gateway_replay(
                endpoint_id,
                &retry_epoch,
                "run-key",
                method,
                "sha256:changed-registration-or-request",
            )
            .expect("changed identity is not recovered"),
        GatewayReplayDecision::Conflict
    ));
}

#[test]
fn legacy_awaiting_review_receipt_without_registration_identity_is_not_adopted() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let endpoint_id = "worker-legacy-awaiting";
    let method = "complete_execution_to_awaiting_review";
    let old_epoch = repository
        .start_gateway_capability_epoch(endpoint_id)
        .expect("start old epoch");
    repository
        .reserve_gateway_replay(
            endpoint_id,
            &old_epoch,
            "legacy-key",
            method,
            "legacy-request-only-digest",
        )
        .expect("reserve legacy request");
    repository
        .complete_gateway_replay(
            endpoint_id,
            &old_epoch,
            "legacy-key",
            method,
            "legacy-request-only-digest",
            "issue_tracker.rpc.v1.IssueGatewayAwaitingReviewMutation",
            b"legacy receipt without registration identity",
        )
        .expect("persist legacy receipt");

    let new_epoch = repository
        .start_gateway_capability_epoch(endpoint_id)
        .expect("start new epoch");
    assert_eq!(
        repository
            .inspect_gateway_replay(
                endpoint_id,
                &new_epoch,
                "legacy-key",
                method,
                "registration-bound-request-digest",
            )
            .expect("legacy receipt must fail closed"),
        GatewayReplayDecision::Conflict
    );
}

#[test]
fn awaiting_review_capability_redaction_preserves_report_but_never_persists_the_handle() {
    let completion = v1::AwaitingReviewCompletion {
        run_id: "run-redaction".into(),
        full_commit_id: "d".repeat(40),
        evidence_references: vec!["artifact://run-redaction/report.json".into()],
        report: Some(v1::CompletionReport {
            result: Some(v1::completion_report::Result::Unverified(
                v1::UnverifiedCompletion {
                    reason: "review remains required".into(),
                    residual_risk: "not all checks ran".into(),
                },
            )),
        }),
    };
    let request = pb::TransitionIssueArgs {
        context: Some(write_context(
            "manager_transition_issue",
            "issue:issue-redaction",
        )),
        issue_id: "issue-redaction".into(),
        expected_revision: Some(12),
        target_state: v1::IssueState::AwaitingReview as i32,
        lease_handle: Some(b"plaintext-one-time-lease-handle".to_vec()),
        claim_revision: Some(12),
        awaiting_review: Some(completion.clone()),
        ..Default::default()
    };
    let encoded = request.encode_to_vec();
    let redacted = crate::manager::backend_dispatch::redacted_capability_request(
        "manager_transition_issue",
        &encoded,
    )
    .expect("dedicated capability request is valid");
    let decoded = pb::TransitionIssueArgs::decode(redacted.as_slice()).unwrap();
    assert_eq!(decoded.lease_handle, None);
    assert_eq!(decoded.awaiting_review, Some(completion));
    assert!(
        !redacted
            .windows(b"plaintext-one-time-lease-handle".len())
            .any(|window| window == b"plaintext-one-time-lease-handle")
    );

    let replay_without_capability = pb::TransitionIssueArgs {
        lease_handle: None,
        ..request
    }
    .encode_to_vec();
    let replay_redacted = crate::manager::backend_dispatch::redacted_capability_request(
        "manager_transition_issue",
        &replay_without_capability,
    )
    .expect("same idempotent request can be identified without old capability");
    assert_eq!(redacted, replay_redacted);
    assert!(crate::manager::requires_capability_dispatch(
        "manager_transition_issue",
        &replay_without_capability
    ));
}

#[test]
fn manager_preserves_awaiting_review_and_fails_closed_on_unknown_canonical_enum() {
    assert_eq!(
        crate::manager::canonical_service::canonical_state(v1::IssueState::AwaitingReview as i32),
        Some(crate::manager::sync::CanonicalIssueState::AwaitingReview)
    );
    assert!(crate::manager::canonical_service::canonical_state(9876).is_none());
    let facts = crate::manager::sync::ReadyFacts {
        state: crate::manager::sync::CanonicalIssueState::AwaitingReview,
        sync_state: crate::manager::sync::SyncState::Confirmed,
        has_open_blocker: false,
        has_active_claim: false,
        claim_result_unconfirmed: false,
        creation_basis: crate::manager::sync::CreationBasis::Explicit,
        confirmation_status: crate::manager::sync::ConfirmationStatus::Confirmed,
    };
    assert_eq!(
        crate::manager::sync::ready_exclusion(&facts),
        Some(crate::manager::sync::ReadyExclusion::StateNotOpen)
    );
}

#[test]
fn gateway_replay_discards_only_the_matching_current_pending_reservation() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let epoch = repository
        .start_gateway_capability_epoch("worker-a")
        .expect("start epoch");
    repository
        .reserve_gateway_replay("worker-a", &epoch, "key-1", "Claim", "digest-a")
        .expect("reserve pending replay");

    assert!(matches!(
        repository
            .discard_gateway_replay_reservation("worker-a", &epoch, "key-1", "Claim", "digest-b")
            .expect_err("different digest must not delete pending record"),
        RepositoryError::IdempotencyConflict
    ));
    repository
        .discard_gateway_replay_reservation("worker-a", &epoch, "key-1", "Claim", "digest-a")
        .expect("discard matching pending replay");
    assert!(matches!(
        repository
            .inspect_gateway_replay("worker-a", &epoch, "key-1", "Claim", "digest-a")
            .expect("discarded key is a new request"),
        GatewayReplayDecision::New
    ));

    repository
        .reserve_gateway_replay("worker-a", &epoch, "key-1", "Claim", "digest-a")
        .expect("reserve replacement replay");
    repository
        .complete_gateway_replay(
            "worker-a",
            &epoch,
            "key-1",
            "Claim",
            "digest-a",
            "issue_tracker.rpc.v1.IssueGatewayClaimed",
            b"successful response",
        )
        .expect("complete replay");
    assert!(matches!(
        repository
            .discard_gateway_replay_reservation("worker-a", &epoch, "key-1", "Claim", "digest-a")
            .expect_err("successful snapshot must never be discarded"),
        RepositoryError::OperationConflict
    ));
}

#[test]
fn gateway_reviewed_policy_requires_confirmed_open_unclaimed_issue() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let repository = ManagerRepository::new(&mut storage);
    let policy = pb::WorkEligibilityPolicy {
        mode: pb::WorkEligibilityMode::ReviewedOnly as i32,
    };
    let mut issue = v1::Issue {
        issue_id: "issue-1".into(),
        state: v1::IssueState::Open as i32,
        confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
        ..Default::default()
    };

    assert!(
        repository
            .gateway_policy_allows_issue(&issue, &policy, 100)
            .expect("evaluate confirmed issue")
    );
    issue.confirmation_status = v1::ConfirmationStatus::Unreviewed as i32;
    assert!(
        !repository
            .gateway_policy_allows_issue(&issue, &policy, 100)
            .expect("evaluate unreviewed issue")
    );
    issue.confirmation_status = v1::ConfirmationStatus::Confirmed as i32;
    issue.state = v1::IssueState::Blocked as i32;
    assert!(
        !repository
            .gateway_policy_allows_issue(&issue, &policy, 100)
            .expect("evaluate blocked issue")
    );
}

#[test]
fn gateway_ready_selection_requires_registration_and_rechecks_claim() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let issue = v1::Issue {
        issue_id: "issue-1".into(),
        state: v1::IssueState::Open as i32,
        confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
        ..Default::default()
    };

    assert!(matches!(
        repository.list_gateway_ready_issues("worker-a", [issue.clone()], 100),
        Err(GatewayReadinessError::Unregistered)
    ));
    repository
        .register_gateway_policy(
            "worker-a",
            "gateway-a",
            "register-worker-a",
            &pb::WorkEligibilityPolicy {
                mode: pb::WorkEligibilityMode::ReviewedOnly as i32,
            },
        )
        .expect("register policy");

    let selection = repository
        .list_gateway_ready_issues("worker-a", [issue.clone()], 100)
        .expect("select ready issues");
    assert_eq!(selection.issues, vec![issue.clone()]);
    let mut blocked = issue;
    blocked.state = v1::IssueState::Blocked as i32;
    assert!(matches!(
        repository.claim_gateway_ready_issue("worker-a", &blocked, 101),
        Err(GatewayReadinessError::Manager(_))
    ));
}

#[test]
fn relation_writes_use_the_marker_anchor_as_the_durable_operation_scope() {
    let cases = [
        (
            "manager_add_issue_relation",
            pb::AddIssueRelationArgs {
                context: Some(write_context("manager_add_issue_relation", "issue:source")),
                kind: v1::RelationKind::Related as i32,
                source_issue_id: "source".into(),
                target_issue_id: "target".into(),
                ..Default::default()
            }
            .encode_to_vec(),
            "source",
        ),
        (
            "manager_remove_issue_relation",
            pb::RemoveIssueRelationArgs {
                context: Some(write_context(
                    "manager_remove_issue_relation",
                    "issue:source",
                )),
                source_issue_id: "source".into(),
                target_issue_id: "target".into(),
                ..Default::default()
            }
            .encode_to_vec(),
            "source",
        ),
        (
            "manager_add_issue_relation",
            pb::AddIssueRelationArgs {
                context: Some(write_context("manager_add_issue_relation", "issue:target")),
                relation_id: "blocks:source:target".into(),
                kind: v1::RelationKind::Blocks as i32,
                source_issue_id: "source".into(),
                target_issue_id: "target".into(),
                ..Default::default()
            }
            .encode_to_vec(),
            "target",
        ),
        (
            "manager_remove_issue_relation",
            pb::RemoveIssueRelationArgs {
                context: Some(write_context(
                    "manager_remove_issue_relation",
                    "issue:target",
                )),
                relation_id: "blocks:source:target".into(),
                source_issue_id: "source".into(),
                target_issue_id: "target".into(),
                ..Default::default()
            }
            .encode_to_vec(),
            "target",
        ),
    ];

    for (method, args, anchor) in cases {
        let write = backend_write(method, &args).expect("relation write context");
        assert_eq!(write.context.method, method);
        assert_eq!(write.context.target, format!("issue:{anchor}"));
        assert_eq!(write.issue_id, anchor);
    }
}

#[test]
fn relation_writes_reject_unknown_add_relation_kinds() {
    let args = pb::AddIssueRelationArgs {
        context: Some(write_context("manager_add_issue_relation", "issue:source")),
        kind: 999,
        source_issue_id: "source".into(),
        target_issue_id: "target".into(),
        ..Default::default()
    }
    .encode_to_vec();

    assert!(backend_write("manager_add_issue_relation", &args).is_err());
}

#[test]
fn canonical_awaiting_review_completion_rejects_foreign_or_conflicting_receipts() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let endpoint_id = "worker-canonical-rejections";
    let method = "complete_execution_to_awaiting_review";
    let schema = "issue_tracker.rpc.v1.IssueGatewayAwaitingReviewMutation";
    let digest = "sha256:canonical-rejections";
    let epoch = repository
        .start_gateway_capability_epoch(endpoint_id)
        .expect("start epoch");

    // Canonical recovery cannot be used to complete other Gateway methods,
    // other response schemas, or an empty receipt.
    for (method, schema, snapshot) in [
        ("claim", schema, b"receipt".as_slice()),
        (
            method,
            "issue_tracker.rpc.v1.IssueGatewayClaimResponse",
            b"receipt",
        ),
        (method, schema, b""),
    ] {
        assert!(matches!(
            repository.complete_gateway_replay_from_canonical(
                endpoint_id,
                "run-key",
                method,
                digest,
                schema,
                snapshot,
            ),
            Err(RepositoryError::RequestSnapshot(_))
        ));
    }
    // Without a reservation there is nothing the canonical proof can bind.
    assert!(matches!(
        repository.complete_gateway_replay_from_canonical(
            endpoint_id,
            "run-key",
            method,
            digest,
            schema,
            b"receipt",
        ),
        Err(RepositoryError::OperationConflict)
    ));

    repository
        .reserve_gateway_replay(endpoint_id, &epoch, "run-key", method, digest)
        .expect("reserve completion");
    repository
        .complete_gateway_replay_from_canonical(
            endpoint_id,
            "run-key",
            method,
            digest,
            schema,
            b"receipt",
        )
        .expect("complete from canonical receipt");
    repository
        .complete_gateway_replay_from_canonical(
            endpoint_id,
            "run-key",
            method,
            digest,
            schema,
            b"receipt",
        )
        .expect("the identical receipt is idempotent");
    assert!(matches!(
        repository.complete_gateway_replay_from_canonical(
            endpoint_id,
            "run-key",
            method,
            digest,
            schema,
            b"different receipt",
        ),
        Err(RepositoryError::OperationConflict)
    ));
}
