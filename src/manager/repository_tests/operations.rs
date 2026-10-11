use super::*;

#[test]
fn retry_operation_returns_recoverable_operations_to_ready() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);

    let operation = recovery_operation(
        "retryable-failure",
        pb::DurableOperationState::Failed,
        pb::FailureClass::Retryable,
    );
    let outbox = recovery_outbox(&operation, pb::OutboxStatus::Dispatched);
    seed_recovery_operation(&mut repository, &operation, &outbox);

    let mutation = GatewayRepository::retry_operation(
        &mut repository,
        &pb::RetryOperationArgs {
            operation_id: operation.operation_id.clone(),
            expected_generation: Some(operation.generation),
            ..Default::default()
        },
    )
    .expect("retry recoverable operation");
    let operation = mutation.operation.expect("retried operation");
    assert_eq!(operation.state, pb::DurableOperationState::Received as i32);
    assert_eq!(
        repository
            .get_outbox("outbox-retryable-failure")
            .expect("read outbox")
            .expect("outbox exists")
            .status,
        pb::OutboxStatus::Ready as i32
    );
}

#[test]
fn pending_resolution_fence_blocks_outbox_leasing_until_terminal() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let operation = recovery_operation(
        "fenced-outbox",
        pb::DurableOperationState::Received,
        pb::FailureClass::Unspecified,
    );
    let outbox = recovery_outbox(&operation, pb::OutboxStatus::Ready);
    {
        let mut repository = ManagerRepository::new(&mut storage);
        seed_recovery_operation(&mut repository, &operation, &outbox);
    }
    storage
        .write_with::<_, RepositoryError, _>(|transaction, _| {
            let contract: String = transaction.query_row(
                "SELECT value FROM manager_metadata WHERE key='contract_fingerprint'",
                [],
                |row| row.get(0),
            )?;
            let root: String = transaction.query_row(
                "SELECT value FROM manager_metadata WHERE key='data_root_id'",
                [],
                |row| row.get(0),
            )?;
            transaction.execute(
                "INSERT INTO resolution_fence
                        (contract_fingerprint, data_root_id, source_operation_id,
                         resolution_operation_id, fence_token, source_generation,
                         expected_issue_revision, decision, request_payload,
                         request_digest, state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)",
                params![
                    contract,
                    root,
                    operation.operation_id,
                    "resolution-fence",
                    "fence-token",
                    operation.generation as i64,
                    0_i64,
                    pb::IssueOperationResolutionDecision::ConfirmApplied as i32,
                    vec![1_u8],
                    "request-digest",
                    pb::DurableOperationState::Received as i32,
                    now_millis(),
                ],
            )?;
            Ok(())
        })
        .expect("persist pending resolution fence");

    let mut repository = ManagerRepository::new(&mut storage);
    assert!(
        repository
            .lease_outbox_for_operation(&operation.operation_id, 10, 10)
            .expect("fenced outbox lookup")
            .is_none()
    );

    storage
        .write_with::<_, RepositoryError, _>(|transaction, _| {
            transaction.execute(
                "UPDATE resolution_fence SET state=?1",
                params![pb::DurableOperationState::Committed as i32],
            )?;
            Ok(())
        })
        .expect("terminalize resolution fence");
    assert!(
        ManagerRepository::new(&mut storage)
            .lease_outbox_for_operation(&operation.operation_id, 10, 10)
            .expect("terminal fence lookup")
            .is_some()
    );
}

#[test]
fn retry_operation_rejects_manual_resolution_dead_letter_and_delivery_unknown() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);

    let manual = recovery_operation(
        "manual-resolution",
        pb::DurableOperationState::ManualResolution,
        pb::FailureClass::Terminal,
    );
    let manual_outbox = recovery_outbox(&manual, pb::OutboxStatus::Ready);
    seed_recovery_operation(&mut repository, &manual, &manual_outbox);

    let dead_letter = recovery_operation(
        "dead-letter",
        pb::DurableOperationState::Unconfirmed,
        pb::FailureClass::Unspecified,
    );
    let dead_letter_outbox = recovery_outbox(&dead_letter, pb::OutboxStatus::DeadLetter);
    seed_recovery_operation(&mut repository, &dead_letter, &dead_letter_outbox);

    let unconfirmed = recovery_operation(
        "delivery-unknown",
        pb::DurableOperationState::Unconfirmed,
        pb::FailureClass::Unspecified,
    );
    let unconfirmed_outbox = recovery_outbox(&unconfirmed, pb::OutboxStatus::Acknowledged);
    seed_recovery_operation(&mut repository, &unconfirmed, &unconfirmed_outbox);

    for operation_id in ["manual-resolution", "dead-letter", "delivery-unknown"] {
        let error = GatewayRepository::retry_operation(
            &mut repository,
            &pb::RetryOperationArgs {
                operation_id: operation_id.into(),
                ..Default::default()
            },
        )
        .expect_err("manual, dead-letter, and unknown delivery require reconciliation");
        assert_eq!(error.code, pb::ManagerErrorCode::ResolutionRequired as i32);
    }
}

#[test]
fn retry_operation_rejects_a_stale_generation_without_requeueing() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let operation = recovery_operation(
        "stale-generation",
        pb::DurableOperationState::Unconfirmed,
        pb::FailureClass::Unspecified,
    );
    let outbox = recovery_outbox(&operation, pb::OutboxStatus::Dispatched);
    seed_recovery_operation(&mut repository, &operation, &outbox);

    let error = GatewayRepository::retry_operation(
        &mut repository,
        &pb::RetryOperationArgs {
            operation_id: operation.operation_id.clone(),
            expected_generation: Some(operation.generation + 1),
            ..Default::default()
        },
    )
    .expect_err("stale generation must not requeue the outbox");

    assert_eq!(error.code, pb::ManagerErrorCode::GenerationConflict as i32);
    assert_eq!(
        repository
            .get_outbox(&outbox.outbox_id)
            .expect("read outbox")
            .expect("outbox exists")
            .status,
        pb::OutboxStatus::Dispatched as i32
    );
}

#[test]
fn acknowledged_unconfirmed_operations_are_exposed_for_startup_marker_reconciliation() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let operation = recovery_operation(
        "startup-unconfirmed",
        pb::DurableOperationState::Unconfirmed,
        pb::FailureClass::Unspecified,
    );
    let outbox = recovery_outbox(&operation, pb::OutboxStatus::Acknowledged);
    seed_recovery_operation(&mut repository, &operation, &outbox);

    let operations = repository
        .unconfirmed_acknowledged_operations()
        .expect("list acknowledged unconfirmed operations");
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].operation_id, operation.operation_id);
    assert_eq!(
        operations[0].state,
        pb::DurableOperationState::Unconfirmed as i32
    );
}

#[test]
fn acknowledged_unconfirmed_operation_pages_wrap_without_starvation() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    for operation_id in ["startup-page-a", "startup-page-b", "startup-page-c"] {
        let operation = recovery_operation(
            operation_id,
            pb::DurableOperationState::Unconfirmed,
            pb::FailureClass::Unspecified,
        );
        let outbox = recovery_outbox(&operation, pb::OutboxStatus::Acknowledged);
        seed_recovery_operation(&mut repository, &operation, &outbox);
    }

    let mut cursor = None;
    let mut seen = Vec::new();
    for _ in 0..4 {
        let page = repository
            .unconfirmed_acknowledged_operations_page(cursor.as_deref(), 1)
            .expect("read one recovery page");
        assert_eq!(page.len(), 1);
        let (operation_id, created_at) = page[0].clone();
        repository
            .set_unconfirmed_recovery_cursor(created_at, &operation_id)
            .expect("checkpoint recovery cursor");
        cursor = Some(format!("{created_at}:{operation_id}"));
        seen.push(operation_id);
    }
    let distinct = seen.iter().take(3).collect::<BTreeSet<_>>();
    assert_eq!(distinct.len(), 3);
    assert_eq!(seen[3], seen[0]);
}

#[test]
fn unacknowledged_unconfirmed_operations_are_not_startup_reconciled() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let operation = recovery_operation(
        "unacknowledged-unconfirmed",
        pb::DurableOperationState::Unconfirmed,
        pb::FailureClass::Unspecified,
    );
    let outbox = recovery_outbox(&operation, pb::OutboxStatus::Ready);
    seed_recovery_operation(&mut repository, &operation, &outbox);

    assert!(
        repository
            .unconfirmed_acknowledged_operations()
            .expect("list acknowledged unconfirmed operations")
            .is_empty()
    );
}

#[test]
fn reconcile_operation_moves_unconfirmed_to_manual_resolution_atomically() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let operation = recovery_operation(
        "reconcile-unconfirmed",
        pb::DurableOperationState::Unconfirmed,
        pb::FailureClass::Unspecified,
    );
    let outbox = recovery_outbox(&operation, pb::OutboxStatus::Acknowledged);
    seed_recovery_operation(&mut repository, &operation, &outbox);

    let mutation = GatewayRepository::reconcile_operation(
        &mut repository,
        &pb::ReconcileOperationArgs {
            operation_id: operation.operation_id.clone(),
            expected_generation: Some(operation.generation),
            ..Default::default()
        },
    )
    .expect("reconcile operation");
    assert_eq!(
        mutation.operation.expect("operation").state,
        pb::DurableOperationState::ManualResolution as i32
    );
    let stored = repository
        .get_operation(&operation.operation_id)
        .expect("read reconciled operation")
        .expect("operation exists");
    assert_eq!(
        stored.state,
        pb::DurableOperationState::ManualResolution as i32
    );
    assert!(stored.resolution_required);
}

#[test]
fn reconcile_operation_rejects_a_stale_generation_without_mutating_state() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let operation = recovery_operation(
        "reconcile-stale",
        pb::DurableOperationState::Unconfirmed,
        pb::FailureClass::Unspecified,
    );
    let outbox = recovery_outbox(&operation, pb::OutboxStatus::Acknowledged);
    seed_recovery_operation(&mut repository, &operation, &outbox);

    let error = GatewayRepository::reconcile_operation(
        &mut repository,
        &pb::ReconcileOperationArgs {
            operation_id: operation.operation_id.clone(),
            expected_generation: Some(operation.generation + 1),
            ..Default::default()
        },
    )
    .expect_err("stale generation must be rejected");
    assert_eq!(error.code, pb::ManagerErrorCode::GenerationConflict as i32);
    assert_eq!(
        repository
            .get_operation(&operation.operation_id)
            .expect("read operation")
            .expect("operation exists")
            .state,
        pb::DurableOperationState::Unconfirmed as i32
    );
}

#[test]
fn pending_idempotency_recovers_the_durable_operation_and_mutation_snapshot() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let mutation = manager_mutation(pb::manager_mutation::Entity::Candidate(candidate(0)));
    let snapshot = pb::Snapshot {
        payload: mutation.encode_to_vec(),
        schema: MANAGER_MUTATION_SNAPSHOT_SCHEMA.into(),
    };

    let committed = repository
        .complete_idempotency(
            "pending-scope",
            "manager_create_candidate",
            "pending-digest",
            "op-pending",
            &snapshot,
        )
        .expect("commit domain mutation without a response snapshot");

    let pending = repository
        .reserve_idempotency("pending-scope", "pending-digest", "retry-operation")
        .expect("recover pending idempotency record");
    match pending {
        IdempotencyDecision::Pending {
            operation,
            mutation_snapshot,
        } => {
            assert_eq!(*operation, committed);
            assert_eq!(*mutation_snapshot, snapshot);
        }
        other => panic!("expected pending recovery, got {other:?}"),
    }
}

#[test]
fn skipped_extraction_apply_replays_by_key_or_canonical_digest() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);

    assert_eq!(
        repository
            .record_skipped_extraction_apply("key-1", "digest-1", 1, 1)
            .expect("store skipped apply"),
        ExtractionApplyReplayDecision::New
    );
    assert_eq!(
        repository
            .extraction_apply_replay("key-1", "digest-1")
            .expect("replay by same key"),
        ExtractionApplyReplayDecision::Replay {
            outcome: 1,
            run_id: None,
            skip_reason: Some(1),
        }
    );
    assert_eq!(
        repository
            .record_skipped_extraction_apply("key-2", "digest-1", 1, 1)
            .expect("replay by canonical digest and bind its key"),
        ExtractionApplyReplayDecision::Replay {
            outcome: 1,
            run_id: None,
            skip_reason: Some(1),
        }
    );
    assert_eq!(
        repository
            .extraction_apply_replay("key-2", "digest-2")
            .expect("replayed key conflicts with a different request"),
        ExtractionApplyReplayDecision::Conflict
    );
}

#[test]
fn auto_registration_link_completes_candidate_and_run_once() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let operation = auto_registration_test_operation("auto-registration-commit");
    seed_auto_registration_entities(&mut storage, &operation);

    {
        let mut repository = ManagerRepository::new(&mut storage);
        repository
            .register_extraction_auto_registration(
                &operation.operation_id,
                "run-auto-registration",
                "candidate-auto-registration",
                "issue-auto-registration",
                "test-auto-registration-key",
            )
            .expect("register auto-registration link");
        repository
            .register_extraction_auto_registration(
                &operation.operation_id,
                "run-auto-registration",
                "candidate-auto-registration",
                "issue-auto-registration",
                "test-auto-registration-key",
            )
            .expect("same link is idempotent");
        assert!(
            repository
                .register_extraction_auto_registration(
                    &operation.operation_id,
                    "run-auto-registration",
                    "candidate-auto-registration",
                    "other-issue",
                    "test-auto-registration-key",
                )
                .is_err()
        );
    }

    let mut committed = operation.clone();
    committed.state = pb::DurableOperationState::Committed as i32;
    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            let link =
                extraction_auto_registration_in_transaction(transaction, &committed.operation_id)?
                    .expect("auto-registration link");
            finalize_extraction_auto_registration_in_transaction(
                transaction,
                stamp,
                &committed,
                &link,
            )
        })
        .expect("complete auto-registration");

    let repository = ManagerRepository::new(&mut storage);
    let candidate = repository
        .candidates()
        .expect("list candidates")
        .into_iter()
        .next()
        .expect("candidate");
    assert_eq!(candidate.status, pb::CandidateStatus::AutoRegistered as i32);
    assert!(matches!(
        candidate.superseded_by,
        Some(pb::candidate::SupersededBy::SupersedingIssueId(ref issue_id))
            if issue_id == "issue-auto-registration"
    ));
    let run = repository
        .extraction_runs()
        .expect("list runs")
        .into_iter()
        .next()
        .expect("run");
    assert_eq!(run.state, pb::ExtractionRunState::Completed as i32);
}

#[test]
fn auto_registration_terminal_failure_marks_run_failed() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let operation = auto_registration_test_operation("auto-registration-failure");
    seed_auto_registration_entities(&mut storage, &operation);
    ManagerRepository::new(&mut storage)
        .register_extraction_auto_registration(
            &operation.operation_id,
            "run-auto-registration",
            "candidate-auto-registration",
            "issue-auto-registration",
            "test-auto-registration-key",
        )
        .expect("register auto-registration link");
    let outbox = outbox::new_entry(
        format!("outbox-{}", operation.operation_id),
        &operation,
        "test",
        operation.operation_id.clone(),
        "sha256:test",
        1,
    )
    .expect("build outbox");
    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            insert_outbox(transaction, &outbox, stamp)
        })
        .expect("seed outbox");
    let second = pb::Candidate {
        candidate_id: "candidate-auto-registration-2".into(),
        extraction_run_id: "run-auto-registration".into(),
        status: pb::CandidateStatus::Proposed as i32,
        strong_detection_proof: Some(pb::StrongDetectionProof::default()),
        ..Default::default()
    };
    ManagerRepository::new(&mut storage)
        .put_proto(
            EntityKind::Candidate,
            &second.candidate_id,
            second.revision,
            "issue_tracker.manager.v1.Candidate",
            &second,
        )
        .expect("seed second candidate");
    ManagerRepository::new(&mut storage)
        .reserve_extraction_auto_registrations(
            "run-auto-registration",
            &[(
                second.candidate_id.clone(),
                "issue-auto-registration-2".into(),
            )],
        )
        .expect("reserve an unadmitted candidate");

    let mut failed = operation.clone();
    failed.state = pb::DurableOperationState::Failed as i32;
    failed.terminal_snapshot = Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(
        pb::Snapshot {
            schema: "issue_tracker.manager.v1.ManagerError".into(),
            payload: pb::ManagerError {
                message: "backend rejected auto-registration".into(),
                ..Default::default()
            }
            .encode_to_vec(),
        },
    ));
    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            let link =
                extraction_auto_registration_in_transaction(transaction, &failed.operation_id)?
                    .expect("auto-registration link");
            finalize_extraction_auto_registration_in_transaction(transaction, stamp, &failed, &link)
        })
        .expect("fail auto-registration");

    let repository = ManagerRepository::new(&mut storage);
    let candidate = repository
        .candidates()
        .expect("list candidates")
        .into_iter()
        .next()
        .expect("candidate");
    assert_eq!(candidate.status, pb::CandidateStatus::Proposed as i32);
    let run = repository
        .extraction_runs()
        .expect("list runs")
        .into_iter()
        .next()
        .expect("run");
    assert_eq!(run.state, pb::ExtractionRunState::Failed as i32);
    assert_eq!(
        run.error.as_deref(),
        Some("backend rejected auto-registration")
    );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_extraction_auto_registration_reservation",)
            .expect("count released reservations"),
        0
    );
    assert!(
        ManagerRepository::new(&mut storage)
            .lease_outbox_for_operation(&operation.operation_id, 10, 10)
            .expect("released outbox can be leased")
            .is_some()
    );
}

#[test]
fn invalidating_candidate_proof_terminally_stops_unleased_auto_registration() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let operation = auto_registration_test_operation("invalidate-auto-registration");
    seed_auto_registration_entities(&mut storage, &operation);
    let outbox = outbox::new_entry(
        format!("outbox-{}", operation.operation_id),
        &operation,
        "test",
        operation.operation_id.clone(),
        "sha256:test",
        1,
    )
    .expect("build outbox");
    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            insert_outbox(transaction, &outbox, stamp)
        })
        .expect("seed outbox");
    let mut repository = ManagerRepository::new(&mut storage);
    repository
        .reserve_extraction_auto_registrations(
            "run-auto-registration",
            &[(
                "candidate-auto-registration".into(),
                "issue-auto-registration".into(),
            )],
        )
        .expect("reserve auto-registration");
    repository
        .register_extraction_auto_registration(
            &operation.operation_id,
            "run-auto-registration",
            "candidate-auto-registration",
            "issue-auto-registration",
            "test-auto-registration-key",
        )
        .expect("register auto-registration link");

    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            invalidate_candidate_auto_registrations_in_transaction(
                transaction,
                stamp,
                "candidate-auto-registration",
            )
        })
        .expect("invalidate auto-registration");

    let operation = ManagerRepository::new(&mut storage)
        .get_operation(&operation.operation_id)
        .expect("read operation")
        .expect("operation exists");
    assert_eq!(operation.state, pb::DurableOperationState::Failed as i32);
    assert_eq!(
            storage
                .raw_query_i64(
                    "SELECT status FROM manager_outbox WHERE operation_id='invalidate-auto-registration'",
                )
                .expect("read outbox status"),
            i64::from(pb::OutboxStatus::DeadLetter as i32)
        );
    assert_eq!(
            storage
                .raw_query_i64(
                    "SELECT state FROM manager_extraction_auto_registration WHERE operation_id='invalidate-auto-registration'",
                )
                .expect("read auto-registration link"),
            i64::from(EXTRACTION_AUTO_REGISTRATION_FAILED)
        );
    assert_eq!(
            storage
                .raw_query_i64(
                    "SELECT COUNT(*) FROM manager_extraction_auto_registration_reservation WHERE candidate_id='candidate-auto-registration'",
                )
                .expect("reservation was removed"),
            0
        );
}

#[test]
fn pending_reservation_prevents_early_run_completion() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let operation = auto_registration_test_operation("auto-registration-reservation");
    seed_auto_registration_entities(&mut storage, &operation);
    let outbox = outbox::new_entry(
        format!("outbox-{}", operation.operation_id),
        &operation,
        "test",
        operation.operation_id.clone(),
        "sha256:test",
        1,
    )
    .expect("build outbox");
    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            insert_outbox(transaction, &outbox, stamp)
        })
        .expect("seed outbox");
    let second = pb::Candidate {
        candidate_id: "candidate-auto-registration-2".into(),
        extraction_run_id: "run-auto-registration".into(),
        status: pb::CandidateStatus::Proposed as i32,
        strong_detection_proof: Some(pb::StrongDetectionProof::default()),
        ..Default::default()
    };
    ManagerRepository::new(&mut storage)
        .put_proto(
            EntityKind::Candidate,
            &second.candidate_id,
            second.revision,
            "issue_tracker.manager.v1.Candidate",
            &second,
        )
        .expect("seed second candidate");
    let reservation_keys = ManagerRepository::new(&mut storage)
        .reserve_extraction_auto_registrations(
            "run-auto-registration",
            &[
                (
                    "candidate-auto-registration".into(),
                    "issue-auto-registration".into(),
                ),
                (
                    "candidate-auto-registration-2".into(),
                    "issue-auto-registration-2".into(),
                ),
            ],
        )
        .expect("reserve every auto-registration before delivery");
    let first_reservation_key = reservation_keys
        .iter()
        .find(|entry| entry.candidate_id == "candidate-auto-registration")
        .expect("first reservation key")
        .idempotency_key
        .clone();
    assert!(!first_reservation_key.is_empty());
    storage
        .write_with::<_, RepositoryError, _>(|transaction, _| {
            consume_extraction_auto_registration_reservation_in_transaction(
                transaction,
                "ordinary-create-operation",
                "issue-auto-registration",
                "ordinary-consumer-key",
            )
        })
        .expect("ordinary create does not consume the auto-registration reservation");
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_extraction_auto_registration_reservation",)
            .expect("count reservations after ordinary create"),
        2
    );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_extraction_auto_registration")
            .expect("count links after ordinary create"),
        0
    );

    let mut committed = operation.clone();
    committed.state = pb::DurableOperationState::Committed as i32;
    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            consume_extraction_auto_registration_reservation_in_transaction(
                transaction,
                &committed.operation_id,
                "issue-auto-registration",
                &first_reservation_key,
            )?;
            let link =
                extraction_auto_registration_in_transaction(transaction, &committed.operation_id)?
                    .expect("consumed durable link");
            finalize_extraction_auto_registration_in_transaction(
                transaction,
                stamp,
                &committed,
                &link,
            )
        })
        .expect("complete first linked operation");

    let replayed_reservation_keys = ManagerRepository::new(&mut storage)
        .reserve_extraction_auto_registrations(
            "run-auto-registration",
            &[
                (
                    "candidate-auto-registration".into(),
                    "issue-auto-registration".into(),
                ),
                (
                    "candidate-auto-registration-2".into(),
                    "issue-auto-registration-2".into(),
                ),
            ],
        )
        .expect("replay does not recreate a consumed reservation");
    assert_eq!(
        replayed_reservation_keys
            .iter()
            .find(|entry| entry.candidate_id == "candidate-auto-registration")
            .expect("replayed first reservation key")
            .idempotency_key,
        first_reservation_key
    );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_extraction_auto_registration_reservation",)
            .expect("count remaining reservations"),
        1
    );
    assert!(
        ManagerRepository::new(&mut storage)
            .lease_outbox_for_operation(&operation.operation_id, 10, 10)
            .expect("check blocked outbox")
            .is_none()
    );

    let run = ManagerRepository::new(&mut storage)
        .extraction_runs()
        .expect("list runs")
        .into_iter()
        .next()
        .expect("run");
    assert_eq!(run.state, pb::ExtractionRunState::Running as i32);

    ManagerRepository::new(&mut storage)
        .fail_extraction_auto_registration_run(
            "run-auto-registration",
            "second candidate admission failed",
        )
        .expect("fail run and release unadmitted reservations");
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_extraction_auto_registration_reservation",)
            .expect("count released reservations"),
        0
    );
    let run = ManagerRepository::new(&mut storage)
        .extraction_runs()
        .expect("list failed run")
        .into_iter()
        .next()
        .expect("failed run");
    assert_eq!(run.state, pb::ExtractionRunState::Failed as i32);
    assert!(
        ManagerRepository::new(&mut storage)
            .lease_outbox_for_operation(&operation.operation_id, 10, 10)
            .expect("released outbox can be leased")
            .is_some()
    );
    ManagerRepository::new(&mut storage)
        .fail_extraction_auto_registration_run("run-auto-registration", "repeat failure cleanup")
        .expect("failed run cleanup is idempotent");
}
