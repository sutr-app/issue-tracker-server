use super::support::*;

#[test]
fn preflight_rejects_mismatched_root_and_store_identity_before_writable_open() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("open manager storage");
    let database_path = storage.database_path().to_path_buf();
    drop(storage);

    let connection = Connection::open(&database_path).expect("open test database");
    connection
        .execute(
            "UPDATE manager_metadata SET value = 'other-root' WHERE key = 'data_root_id'",
            [],
        )
        .expect("tamper root identity");
    drop(connection);
    assert!(matches!(
        ManagerStorage::preflight(&config),
        Err(ManagerStorageError::StoreIdentityMismatch(message))
            if message.contains("metadata data_root_id")
    ));

    let connection = Connection::open(&database_path).expect("reopen test database");
    connection
        .execute(
            "UPDATE manager_metadata SET value = 'root' WHERE key = 'data_root_id'",
            [],
        )
        .expect("restore root identity");
    connection
        .execute(
            "UPDATE manager_store_identity
                    SET store_pair_id = 'wrong-pair'
                  WHERE role = 1",
            [],
        )
        .expect("tamper TaskChampion pair identity");
    drop(connection);
    assert!(matches!(
        ManagerStorage::preflight(&config),
        Err(ManagerStorageError::StoreIdentityMismatch(message))
            if message.contains("TaskChampion store identity")
    ));
}

#[test]
fn rolls_back_counter_when_operation_fails() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let result: Result<((), ManagerCommit), _> =
        storage.write(|_, _| Err(ManagerStorageError::Transaction("expected failure".into())));
    assert!(result.is_err());
    assert_eq!(storage.manager_sequence(), 0);
    assert_eq!(storage.generation(), 0);
}

#[test]
fn rejects_a_second_writer_until_the_first_is_dropped() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("first writer");
    assert!(matches!(
        ManagerStorage::open(&config),
        Err(ManagerStorageError::RootLocked)
    ));
    drop(storage);
    ManagerStorage::open(&config).expect("lock is released after drop");
}

#[test]
fn schema_contains_manager_data_but_no_canonical_issue_columns() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("open manager storage");
    let columns: Vec<String> = storage
        .connection
        .prepare("PRAGMA table_info(manager_entity_revision)")
        .unwrap()
        .query_map([], |row| row.get(1))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(columns, vec!["entity_kind", "entity_id", "revision"]);
}

#[test]
fn repository_round_trips_protobuf_and_enforces_compare_and_swap() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let candidate = crate::proto_types::manager_v1::Candidate {
        candidate_id: "candidate-1".into(),
        title: "title".into(),
        revision: 1,
        ..Default::default()
    };

    storage
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::Candidate,
            &candidate.candidate_id,
            candidate.revision,
            "issue_tracker.manager.v1.Candidate",
            &candidate,
        )
        .expect("persist candidate");
    let loaded = storage
        .repository()
        .get(
            crate::manager::repository::EntityKind::Candidate,
            "candidate-1",
        )
        .expect("read candidate")
        .expect("candidate exists");
    let decoded = crate::proto_types::manager_v1::Candidate::decode(loaded.payload.as_slice())
        .expect("decode candidate");
    assert_eq!(decoded, candidate);
    assert_eq!(loaded.revision, 1);

    let newer = crate::proto_types::manager_v1::Candidate {
        revision: 2,
        ..candidate.clone()
    };
    assert!(matches!(
        storage.repository().put_proto_cas(
            crate::manager::repository::EntityKind::Candidate,
            &newer.candidate_id,
            1,
            2,
            "issue_tracker.manager.v1.Candidate",
            &newer,
        ),
        Ok(_)
    ));
    assert!(matches!(
        storage.repository().put_proto_cas(
            crate::manager::repository::EntityKind::Candidate,
            &newer.candidate_id,
            1,
            3,
            "issue_tracker.manager.v1.Candidate",
            &newer,
        ),
        Err(crate::manager::repository::RepositoryError::RevisionConflict { .. })
    ));
}

#[test]
fn manager_only_commit_persists_entity_and_operation_without_outbox() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let candidate = crate::proto_types::manager_v1::Candidate {
        candidate_id: "candidate-commit".into(),
        title: "title".into(),
        revision: 1,
        ..Default::default()
    };
    let operation = crate::proto_types::manager_v1::DurableOperation {
        operation_id: "op-manager-only".into(),
        method: "manager_create_candidate".into(),
        idempotency_scope: "scope-1".into(),
        request_digest: "digest-1".into(),
        state: crate::proto_types::manager_v1::DurableOperationState::Received as i32,
        ..Default::default()
    };
    let mutation = crate::proto_types::manager_v1::ManagerMutation {
        entity: Some(
            crate::proto_types::manager_v1::manager_mutation::Entity::Candidate(candidate.clone()),
        ),
        operation: Some(operation.clone()),
        ..Default::default()
    };
    storage
        .repository()
        .commit_mutation(&mutation, &operation, None)
        .expect("commit manager-only mutation");
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_entity")
            .unwrap(),
        1
    );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_operation")
            .unwrap(),
        1
    );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_outbox")
            .unwrap(),
        0
    );
    let snapshot: Vec<u8> = storage
        .connection
        .query_row(
            "SELECT snapshot FROM manager_operation WHERE operation_id = 'op-manager-only'",
            [],
            |row| row.get(0),
        )
        .expect("operation snapshot");
    let decoded = crate::proto_types::manager_v1::ManagerMutation::decode(snapshot.as_slice())
        .expect("decode mutation snapshot");
    assert!(decoded.entity.is_some());
}

#[test]
fn backend_commit_persists_operation_and_outbox_atomically() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let operation = crate::proto_types::manager_v1::DurableOperation {
        operation_id: "op-backend".into(),
        method: "manager_create_issue".into(),
        idempotency_scope: "scope-2".into(),
        request_digest: "digest-2".into(),
        state: crate::proto_types::manager_v1::DurableOperationState::Pending as i32,
        ..Default::default()
    };
    let mutation = crate::proto_types::manager_v1::ManagerMutation {
        operation: Some(operation.clone()),
        ..Default::default()
    };
    let outbox = crate::proto_types::manager_v1::OutboxEntry {
        outbox_id: "outbox-1".into(),
        operation_id: operation.operation_id.clone(),
        topic: "taskchampion".into(),
        status: crate::proto_types::manager_v1::OutboxStatus::Ready as i32,
        payload_snapshot_ref: "snapshot-1".into(),
        payload_digest: "digest-2".into(),
        generation: 1,
        ..Default::default()
    };
    storage
        .repository()
        .commit_mutation(&mutation, &operation, Some(&outbox))
        .expect("commit backend mutation");
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_operation")
            .unwrap(),
        1
    );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_outbox")
            .unwrap(),
        1
    );
}

#[test]
fn backend_admission_requires_reconciliation_before_idempotency_can_replay() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let admission_request = pb::UpdateIssueArgs {
        context: Some(pb::ManagerWriteContext {
            target: "issue:issue-pending".into(),
            schema_fingerprint: "schema".into(),
            actor_audit: Some(pb::ActorAudit {
                actor_id: "test".into(),
                actor_kind: crate::v1::ActorKind::InternalAgent as i32,
                ..Default::default()
            }),
            correlation_id: "correlation-pending".into(),
            ..Default::default()
        }),
        issue_id: "issue-pending".into(),
        // A pending update must carry the same optimistic-concurrency
        // precondition used by its persisted marker expectation.
        expected_revision: Some(0),
        update_mask: vec![pb::ManagerIssueField::Title as i32],
        patch: Some(pb::ManagerIssuePatch {
            title: Some("pending update".into()),
            ..Default::default()
        }),
        target_state: None,
        terminal_report: None,
    }
    .encode_to_vec();
    let request = durable_request_from_manager("manager_update_issue", &admission_request)
        .expect("durable request");
    let operation = pb::DurableOperation {
        operation_id: "op-pending".into(),
        method: "manager_update_issue".into(),
        idempotency_scope: "scope-pending".into(),
        request_digest: digest_bytes(&admission_request),
        state: pb::DurableOperationState::Pending as i32,
        generation: 1,
        downstream_operation_id: "backend-op-pending".into(),
        audit: Some(pb::ManagerOperationAudit {
            contract_fingerprint: "contract".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mutation = pb::ManagerMutation {
        operation: Some(operation.clone()),
        ..Default::default()
    };
    let outbox = pb::OutboxEntry {
        outbox_id: "outbox-pending".into(),
        operation_id: operation.operation_id.clone(),
        topic: "taskchampion".into(),
        status: pb::OutboxStatus::Ready as i32,
        payload_snapshot_ref: operation.operation_id.clone(),
        payload_digest: digest_bytes(&request),
        generation: 1,
        ..Default::default()
    };
    let expectation = derive_marker_expectation(&operation, &admission_request)
        .expect("derive marker expectation");
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    storage
        .repository()
        .commit_backend_admission(
            &mutation,
            &operation,
            &outbox,
            &request,
            "issue_tracker.manager.v1.DurableBackendRequest",
            &expectation,
        )
        .expect("commit backend admission");
    let stored_request: Vec<u8> = storage
        .connection
        .query_row(
            "SELECT request_snapshot FROM manager_operation WHERE operation_id = 'op-pending'",
            [],
            |row| row.get(0),
        )
        .expect("stored durable request");
    let envelope =
        crate::manager::backend_dispatch::DurableBackendRequest::decode(stored_request.as_slice())
            .expect("decode durable request");
    assert!(envelope.create_issue.is_none());
    let update = envelope.update_issue.expect("update-only durable request");
    assert_eq!(update.issue_id, "issue-pending");
    assert!(
        !stored_request
            .windows(b"lease_handle".len())
            .any(|window| window == b"lease_handle")
    );
    assert!(
            storage
                .connection
                .query_row(
                    "SELECT result_snapshot IS NULL FROM manager_idempotency WHERE idempotency_scope = 'scope-pending'",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .expect("read pending idempotency row")
        );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_operation")
            .expect("count operations"),
        1
    );
    assert_eq!(
        storage
            .raw_query_i64("SELECT COUNT(*) FROM manager_outbox")
            .expect("count outbox rows"),
        1
    );
    drop(storage);

    let mut reopened = ManagerStorage::open(&config).expect("reopen manager storage");
    let mut repository = reopened.repository();
    let replay = GatewayRepository::reserve_idempotency(
        &mut repository,
        "scope-pending",
        &operation.request_digest,
        "new-operation-id",
    );
    assert!(matches!(replay, Ok(IdempotencyDecision::Pending { .. })));

    let mut confirmed = repository
        .get_operation(&operation.operation_id)
        .expect("read pending operation")
        .expect("pending operation exists");
    confirmed.state = pb::DurableOperationState::Committed as i32;
    let mut outbox = repository
        .get_outbox("outbox-pending")
        .expect("read outbox")
        .expect("outbox exists");
    outbox::lease(&mut outbox, 10, 10, Some("lease-pending".into())).expect("lease outbox");
    outbox::mark_dispatched(&mut outbox, "lease-pending", 10).expect("mark outbox dispatched");
    outbox::acknowledge(&mut outbox, "lease-pending", 11).expect("acknowledge outbox");
    repository
        .finalize_backend_dispatch(&confirmed, &outbox, &[])
        .expect("atomically confirm backend result");
    let stored = repository
        .get_operation(&operation.operation_id)
        .expect("read confirmed operation")
        .expect("confirmed operation exists");
    let terminal = stored
        .terminal_snapshot
        .as_ref()
        .expect("confirmed operation has a terminal snapshot");
    let snapshot = match terminal {
        pb::durable_operation::TerminalSnapshot::ResultSnapshot(value) => value,
        other => panic!("expected result snapshot, got {other:?}"),
    };
    assert_eq!(
        snapshot.schema,
        crate::manager::operations::MANAGER_MUTATION_SNAPSHOT_SCHEMA
    );
    let mutation = pb::ManagerMutation::decode(snapshot.payload.as_slice())
        .expect("terminal snapshot is a ManagerMutation");
    if let Some(pb::manager_mutation::Result::IssueCreation(result)) = mutation.result.as_ref() {
        assert!(result.issue.is_none());
    }
    let replay = GatewayRepository::reserve_idempotency(
        &mut repository,
        "scope-pending",
        &operation.request_digest,
        "another-operation-id",
    )
    .expect("confirmed result replays");
    let result = match replay {
        IdempotencyDecision::Replay(result) => result,
        other => panic!("expected confirmed replay, got {other:?}"),
    };
    let result = pb::UpdateIssueResult::decode(result.as_slice()).expect("decode result");
    assert!(matches!(
        result.outcome,
        Some(pb::update_issue_result::Outcome::Success(mutation))
            if mutation.operation.as_ref().is_some_and(|value|
                value.state == pb::DurableOperationState::Committed as i32)
    ));
}

#[test]
fn repository_rejects_canonical_issue_payload_and_drops_claim_lease() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    assert!(matches!(
        storage.repository().put_entity(
            crate::manager::repository::EntityKind::Candidate,
            "candidate-issue-payload",
            1,
            "issue_tracker.v1.Issue",
            &[1, 2, 3],
        ),
        Err(crate::manager::repository::RepositoryError::ForbiddenPayload(_))
    ));
    let operation = crate::proto_types::manager_v1::DurableOperation {
        operation_id: "op-claim".into(),
        method: "manager_claim_issue".into(),
        idempotency_scope: "scope-claim".into(),
        request_digest: "digest-claim".into(),
        state: crate::proto_types::manager_v1::DurableOperationState::Pending as i32,
        ..Default::default()
    };
    let mutation = crate::proto_types::manager_v1::ManagerMutation {
        operation: Some(operation.clone()),
        result: Some(
            crate::proto_types::manager_v1::manager_mutation::Result::ClaimIssue(
                crate::proto_types::manager_v1::ManagerClaimIssueSuccess {
                    lease_handle: vec![9, 8, 7],
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    storage
        .repository()
        .commit_mutation(&mutation, &operation, None)
        .expect("commit claim mutation");
    let snapshot: Vec<u8> = storage
        .connection
        .query_row(
            "SELECT snapshot FROM manager_operation WHERE operation_id = 'op-claim'",
            [],
            |row| row.get(0),
        )
        .expect("operation snapshot");
    let decoded = crate::proto_types::manager_v1::ManagerMutation::decode(snapshot.as_slice())
        .expect("decode mutation snapshot");
    match decoded.result {
        Some(crate::proto_types::manager_v1::manager_mutation::Result::ClaimIssue(claim)) => {
            assert!(claim.lease_handle.is_empty())
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn operation_payload_redacts_nested_canonical_issue_and_lease() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let nested = crate::proto_types::manager_v1::ManagerMutation {
        result: Some(
            crate::proto_types::manager_v1::manager_mutation::Result::ClaimIssue(
                crate::proto_types::manager_v1::ManagerClaimIssueSuccess {
                    current: Some(crate::v1::Issue::default()),
                    lease_handle: vec![1, 2, 3],
                },
            ),
        ),
        ..Default::default()
    };
    let mut operation = crate::proto_types::manager_v1::DurableOperation {
        operation_id: "op-nested".into(),
        method: "manager_claim_issue".into(),
        idempotency_scope: "scope-nested".into(),
        request_digest: "digest-nested".into(),
        state: crate::proto_types::manager_v1::DurableOperationState::Pending as i32,
        ..Default::default()
    };
    operation.commit_snapshot = Some(crate::proto_types::manager_v1::ManagerCommitSnapshot {
        snapshot: Some(crate::proto_types::manager_v1::Snapshot {
            payload: nested.encode_to_vec(),
            schema: crate::manager::operations::MANAGER_MUTATION_SNAPSHOT_SCHEMA.into(),
        }),
        ..Default::default()
    });
    let mutation = crate::proto_types::manager_v1::ManagerMutation {
        operation: Some(operation.clone()),
        ..Default::default()
    };
    storage
        .repository()
        .commit_mutation(&mutation, &operation, None)
        .expect("commit operation");
    let restored = storage
        .repository()
        .get_operation("op-nested")
        .expect("read operation")
        .expect("operation exists");
    let snapshot = restored
        .commit_snapshot
        .and_then(|value| value.snapshot)
        .expect("commit snapshot");
    let decoded =
        crate::proto_types::manager_v1::ManagerMutation::decode(snapshot.payload.as_slice())
            .expect("decode nested snapshot");
    match decoded.result {
        Some(crate::proto_types::manager_v1::manager_mutation::Result::ClaimIssue(claim)) => {
            assert!(claim.current.is_none());
            assert!(claim.lease_handle.is_empty());
        }
        other => panic!("unexpected nested result: {other:?}"),
    }
}

#[test]
fn migration_backup_hook_runs_before_first_schema_migration() {
    use std::cell::Cell;
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let called = Cell::new(false);
    let storage = ManagerStorage::open_with_migration_backup(&config, |database| {
        called.set(true);
        assert!(database.ends_with("issue-tracker-manager.sqlite3"));
        Ok(())
    })
    .expect("open manager storage");
    assert!(called.get());
    drop(storage);
}

#[test]
fn releases_root_lock_when_migration_backup_hook_fails() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();

    assert!(matches!(
        ManagerStorage::open_with_migration_backup(&config, |_| {
            Err(ManagerStorageError::Transaction(
                "backup failed deliberately".into(),
            ))
        }),
        Err(ManagerStorageError::Transaction(message)) if message == "backup failed deliberately"
    ));
    assert!(!config.data_root.join(LOCK_FILE).exists());
    ManagerStorage::open(&config).expect("lock is released after backup failure");
}

#[test]
fn initializes_a_new_v2_root_with_completion_projections() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();

    let storage = ManagerStorage::open(&config).expect("initialize v14 root");
    assert_eq!(
        storage
            .raw_query_i64("PRAGMA user_version")
            .expect("schema version"),
        SCHEMA_VERSION
    );
    for table in [
        "manager_extraction_candidate_identity",
        "manager_extraction_suppression_audit",
        "manager_extraction_suppression_rejected_candidate",
        "manager_operation_history_decision",
    ] {
        assert_eq!(
            storage
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get::<_, i64>(0),
                )
                .expect("inspect v16 table"),
            1,
            "missing {table}"
        );
    }
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                   WHERE type='index'
                     AND name='manager_operation_history_projection_direct_state_order'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("inspect history paging index"),
        1
    );
}

#[test]
fn migrates_a_v30_root_and_adds_the_history_keyset_index() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let operation = pb::DurableOperation {
        operation_id: "migration-history-operation".into(),
        method: "manager_migration_test".into(),
        idempotency_scope: "migration-history-scope".into(),
        request_digest: "migration-history-digest".into(),
        // Simulate a terminal operation whose old v30 projection was never
        // refreshed after backend dispatch completed.
        state: pb::DurableOperationState::Committed as i32,
        generation: 1,
        created_at: Some(pb::UtcInstant {
            unix_epoch_millis: 123,
        }),
        updated_at: Some(pb::UtcInstant {
            unix_epoch_millis: 123,
        }),
        scope: Some(pb::durable_operation::Scope::IssueId(
            "migration-issue".into(),
        )),
        ..Default::default()
    };
    storage
        .repository()
        .commit_mutation(
            &pb::ManagerMutation {
                operation: Some(operation.clone()),
                ..Default::default()
            },
            &operation,
            None,
        )
        .expect("seed operation history");
    storage
        .connection
        .execute_batch(
            "DROP INDEX manager_operation_history_projection_direct_state_order;
             UPDATE manager_operation
                SET created_at = 999, updated_at = 998
              WHERE operation_id='migration-history-operation';
             UPDATE manager_operation_history_projection
                SET state = 1, created_at = 999, updated_at = 997,
                    resolution_required = 1
              WHERE operation_id='migration-history-operation';
             PRAGMA user_version = 30;
             UPDATE manager_metadata
                SET value = 'issue-tracker-manager/v14/sqlite-schema-v30'
              WHERE key = 'schema_fingerprint';
             UPDATE manager_store_identity
                SET schema_fingerprint = 'issue-tracker-manager/v14/sqlite-schema-v30'
              WHERE role = 2;",
        )
        .expect("mark root as v30");
    let generation_before_migration = storage.generation();
    drop(storage);

    let migrated = ManagerStorage::open(&config).expect("migrate v30 root");
    assert_eq!(migrated.generation(), generation_before_migration + 1);
    assert_eq!(migrated.raw_query_i64("PRAGMA user_version").unwrap(), 31);
    assert_eq!(
        migrated
            .raw_query_i64(
                "SELECT COUNT(*) FROM sqlite_master
                   WHERE type='index'
                     AND name='manager_operation_history_projection_direct_state_order'",
            )
            .unwrap(),
        1
    );
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT created_at, updated_at, resolution_required
                   FROM manager_operation_history_projection
                  WHERE operation_id='migration-history-operation'",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .unwrap(),
        (123, 123, 0)
    );
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT created_at, updated_at FROM manager_operation
                  WHERE operation_id='migration-history-operation'",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap(),
        (123, 123)
    );
    assert_eq!(
        migrated
            .connection
            .query_row(
                "SELECT state FROM manager_operation_history_projection
                  WHERE operation_id='migration-history-operation'",
                [],
                |row| row.get::<_, i32>(0),
            )
            .unwrap(),
        pb::DurableOperationState::Committed as i32
    );
}

#[test]
fn aborts_v30_migration_when_history_projection_is_inconsistent() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let operation = pb::DurableOperation {
        operation_id: "migration-corrupt-history-operation".into(),
        method: "manager_migration_test".into(),
        idempotency_scope: "migration-corrupt-history-scope".into(),
        request_digest: "migration-corrupt-history-digest".into(),
        state: pb::DurableOperationState::Received as i32,
        generation: 1,
        created_at: Some(pb::UtcInstant {
            unix_epoch_millis: 456,
        }),
        updated_at: Some(pb::UtcInstant {
            unix_epoch_millis: 456,
        }),
        scope: Some(pb::durable_operation::Scope::IssueId(
            "migration-issue".into(),
        )),
        ..Default::default()
    };
    storage
        .repository()
        .commit_mutation(
            &pb::ManagerMutation {
                operation: Some(operation.clone()),
                ..Default::default()
            },
            &operation,
            None,
        )
        .expect("seed operation history");
    storage
        .connection
        .execute_batch(
            "DROP INDEX manager_operation_history_projection_direct_state_order;
             UPDATE manager_operation_history_projection
                SET created_at = created_at + 1
              WHERE operation_id='migration-corrupt-history-operation';
             PRAGMA user_version = 30;
             UPDATE manager_metadata
                SET value = 'issue-tracker-manager/v14/sqlite-schema-v30'
              WHERE key = 'schema_fingerprint';
             UPDATE manager_store_identity
                SET schema_fingerprint = 'issue-tracker-manager/v14/sqlite-schema-v30'
              WHERE role = 2;",
        )
        .expect("corrupt and mark root as v30");
    let database_path = storage.database_path().to_path_buf();
    drop(storage);

    assert!(matches!(
        ManagerStorage::open(&config),
        Err(ManagerStorageError::Transaction(message))
            if message.contains("source and projection timestamps disagree")
    ));
    let connection = Connection::open(database_path).expect("inspect rolled-back root");
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        30
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM manager_metadata WHERE key='schema_fingerprint'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "issue-tracker-manager/v14/sqlite-schema-v30"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                   WHERE type='index'
                     AND name='manager_operation_history_projection_direct_state_order'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn aborts_v29_migration_when_history_issue_relation_is_inconsistent() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let operation = pb::DurableOperation {
        operation_id: "migration-missing-history-issue".into(),
        method: "manager_migration_test".into(),
        idempotency_scope: "migration-missing-history-issue-scope".into(),
        request_digest: "migration-missing-history-issue-digest".into(),
        state: pb::DurableOperationState::Received as i32,
        generation: 1,
        created_at: Some(pb::UtcInstant {
            unix_epoch_millis: 321,
        }),
        updated_at: Some(pb::UtcInstant {
            unix_epoch_millis: 321,
        }),
        affected_issue_ids: vec!["migration-related-issue".into()],
        scope: Some(pb::durable_operation::Scope::CandidateId(
            "migration-candidate".into(),
        )),
        ..Default::default()
    };
    storage
        .repository()
        .commit_mutation(
            &pb::ManagerMutation {
                operation: Some(operation.clone()),
                ..Default::default()
            },
            &operation,
            None,
        )
        .expect("seed operation history");
    storage
        .connection
        .execute(
            "DELETE FROM manager_operation_history_issue
              WHERE operation_id='migration-missing-history-issue'",
            [],
        )
        .expect("remove history relation");
    storage
        .connection
        .execute_batch(
            "DROP INDEX manager_operation_history_projection_direct_state_order;
             UPDATE manager_metadata
                SET value = 'issue-tracker-manager/v14/sqlite-schema-v29'
              WHERE key = 'schema_fingerprint';
             UPDATE manager_store_identity
                SET schema_fingerprint = 'issue-tracker-manager/v14/sqlite-schema-v29'
              WHERE role = 2;
             PRAGMA user_version = 29;",
        )
        .expect("mark root as v29");
    let database_path = storage.database_path().to_path_buf();
    drop(storage);

    assert!(matches!(
        ManagerStorage::open(&config),
        Err(ManagerStorageError::Transaction(message))
            if message.contains("Issue relation projection does not match")
    ));
    let connection = Connection::open(database_path).expect("inspect rolled-back root");
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        29
    );
}

#[test]
fn rejects_legacy_roots_without_running_backup_or_mutating_them() {
    use std::cell::Cell;

    for version in [14_i64, 15, 16, 18, 19, SCHEMA_VERSION - 1, SCHEMA_VERSION] {
        let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
        let root = tempdir().expect("temporary root");
        config.data_root = root.path().to_path_buf();
        let storage = ManagerStorage::open(&config).expect("initialize root");
        storage
            .connection
            .execute_batch(&format!("PRAGMA user_version = {version};"))
            .expect("make legacy root");
        storage
                .connection
                .execute(
                    "UPDATE manager_metadata SET value = 'issue-tracker-manager/v1' WHERE key = 'contract_fingerprint'",
                    [],
                )
                .expect("mark v1 contract");
        storage
            .connection
            .execute(
                "INSERT INTO manager_entity
                     (entity_kind, entity_id, revision, payload, payload_schema,
                      payload_digest, created_at, updated_at)
                     VALUES ('candidate', 'legacy-candidate', 1, X'00', 'test', 'digest', 1, 1)",
                [],
            )
            .expect("seed legacy entity");
        drop(storage);

        let callback_called = Cell::new(false);
        assert!(matches!(
            ManagerStorage::open_with_migration_backup(&config, |_| {
                callback_called.set(true);
                Ok(())
            }),
            Err(ManagerStorageError::StoreIdentityMismatch(_))
        ));
        assert!(!callback_called.get());

        let database =
            Connection::open(config.data_root.join(DATABASE_FILE)).expect("inspect rejected root");
        assert_eq!(
            database
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .expect("unchanged user version"),
            version
        );
        assert_eq!(
            database
                .query_row(
                    "SELECT value FROM manager_metadata WHERE key = 'contract_fingerprint'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("unchanged fingerprint"),
            "issue-tracker-manager/v1"
        );
        assert_eq!(
            database
                .query_row(
                    "SELECT COUNT(*) FROM manager_entity WHERE entity_id = 'legacy-candidate'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("unchanged entity"),
            1
        );
        assert!(!migration_backup_path(&config.data_root.join(DATABASE_FILE), version).exists());
    }
}

#[test]
#[ignore = "v1 roots are intentionally rejected instead of migrated"]
fn migrates_v5_store_with_verified_backup_and_phase_five_indexes() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();

    let storage = ManagerStorage::open(&config).expect("initialize current schema");
    storage
        .connection
        .execute_batch(
            "DROP TABLE manager_gateway_replay_record;
                 DROP TABLE manager_gateway_epoch;
                 DROP TABLE manager_gateway_registration;
                 DROP TABLE manager_operation_history_issue;
                 DROP TABLE manager_operation_history_projection;
                 DROP TABLE manager_issue_mapping_index;
                 PRAGMA user_version = 5;",
        )
        .expect("restore v5 schema shape");
    let mapping = pb::IssueMapping {
        issue_id: "issue-existing".into(),
        candidate_id: Some("candidate-existing".into()),
        backend_id: "backend-existing".into(),
        ..Default::default()
    };
    storage
        .connection
        .execute(
            "INSERT INTO manager_entity
                    (entity_kind, entity_id, revision, payload, payload_schema,
                     payload_digest, created_at, updated_at)
                 VALUES ('issue_mapping', ?1, 0, ?2,
                         'issue_tracker.manager.v1.IssueMapping', 'legacy', 10, 11)",
            params![mapping.issue_id.clone(), mapping.encode_to_vec()],
        )
        .expect("seed v5 mapping");
    let operation = pb::DurableOperation {
        operation_id: "operation-existing".into(),
        scope: Some(pb::durable_operation::Scope::CandidateId(
            "candidate-existing".into(),
        )),
        method: "manager_resolve_candidate".into(),
        state: pb::DurableOperationState::ManualResolution as i32,
        affected_issue_ids: vec!["issue-existing".into()],
        resolution_required: true,
        ..Default::default()
    };
    let operation_payload = operation.encode_to_vec();
    let operation_digest = digest_bytes(&operation_payload);
    storage
        .connection
        .execute(
            "INSERT INTO manager_operation
                    (operation_id, method, idempotency_scope, request_digest, state, attempt,
                     manager_sequence, generation, affects_ready, created_at, updated_at,
                     operation_payload, operation_payload_schema, operation_payload_digest)
                 VALUES (?1, ?2, '', '', ?3, 0, 0, 0, 0, 12, 13, ?4,
                         'issue_tracker.manager.v1.DurableOperation', ?5)",
            params![
                operation.operation_id,
                operation.method,
                operation.state,
                operation_payload,
                operation_digest,
            ],
        )
        .expect("seed v5 operation");
    storage
        .connection
        .execute(
            "INSERT INTO manager_operation_scope (operation_id, candidate_id)
                 VALUES ('operation-existing', 'candidate-existing')",
            [],
        )
        .expect("seed v5 operation scope");
    drop(storage);

    let backup = migration_backup_path(&config.data_root.join(DATABASE_FILE), 5);
    assert!(!backup.exists());
    let storage = ManagerStorage::open(&config).expect("migrate v5 store");

    assert!(backup.is_file());
    let backup_connection = Connection::open(&backup).expect("open verified backup");
    assert_eq!(
        backup_connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .expect("read backup schema version"),
        5
    );
    assert_eq!(
            backup_connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'manager_gateway_replay_record'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("inspect backup schema"),
            0
        );
    for table in [
        "manager_issue_mapping_index",
        "manager_operation_history_projection",
        "manager_operation_history_issue",
        "manager_gateway_registration",
        "manager_gateway_epoch",
        "manager_gateway_replay_record",
    ] {
        assert_eq!(
            storage
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get::<_, i64>(0),
                )
                .expect("inspect migrated schema"),
            1,
            "missing {table}"
        );
    }
    assert_eq!(
        storage
            .raw_query_i64("PRAGMA user_version")
            .expect("read schema version"),
        SCHEMA_VERSION
    );
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_issue_mapping_index
                      WHERE candidate_id = 'candidate-existing'
                        AND issue_id = 'issue-existing'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("read backfilled mapping"),
        1
    );
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_operation_history_projection
                      WHERE operation_id = 'operation-existing'
                        AND direct_target_kind = 'candidate'
                        AND direct_target_id = 'candidate-existing'
                        AND resolution_required = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("read backfilled history projection"),
        1
    );
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_operation_history_issue
                      WHERE operation_id = 'operation-existing'
                        AND issue_id = 'issue-existing'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("read backfilled history Issue"),
        1
    );
}

#[test]
#[ignore = "v1 roots are intentionally rejected instead of migrated"]
fn migrates_v5_mapping_conflicts_to_manual_resolution_without_selecting_a_mapping() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("initialize current schema");
    storage
        .connection
        .execute_batch(
            "DROP TABLE manager_gateway_replay_record;
                 DROP TABLE manager_gateway_epoch;
                 DROP TABLE manager_gateway_registration;
                 DROP TABLE manager_operation_history_issue;
                 DROP TABLE manager_operation_history_projection;
                 DROP TABLE manager_issue_mapping_index;
                 PRAGMA user_version = 5;",
        )
        .expect("restore v5 schema shape");
    for issue_id in ["issue-first", "issue-second"] {
        let mapping = pb::IssueMapping {
            issue_id: issue_id.into(),
            candidate_id: Some("candidate-conflicted".into()),
            backend_id: format!("backend-{issue_id}"),
            ..Default::default()
        };
        storage
            .connection
            .execute(
                "INSERT INTO manager_entity
                        (entity_kind, entity_id, revision, payload, payload_schema,
                         payload_digest, created_at, updated_at)
                     VALUES ('issue_mapping', ?1, 0, ?2,
                             'issue_tracker.manager.v1.IssueMapping', 'legacy', 10, 11)",
                params![issue_id, mapping.encode_to_vec()],
            )
            .expect("seed conflicting v5 mapping");
    }
    drop(storage);

    let storage = ManagerStorage::open(&config).expect("conflict migration completes");
    let backup = migration_backup_path(&config.data_root.join(DATABASE_FILE), 5);
    assert!(backup.is_file());
    assert_eq!(
        Connection::open(&backup)
            .expect("open preserved migration backup")
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .expect("read backup schema version"),
        5
    );
    assert_eq!(
        storage
            .raw_query_i64("PRAGMA user_version")
            .expect("read schema version"),
        SCHEMA_VERSION
    );
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_entity
                      WHERE entity_kind = 'issue_mapping'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("read preserved mappings"),
        2
    );
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_issue_mapping_index",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("conflicting mappings are not indexed"),
        0
    );
    let (state, payload): (i32, Vec<u8>) = storage
        .connection
        .query_row(
            "SELECT state, operation_payload
                   FROM manager_operation
                  WHERE method = 'manager_migrate_legacy_issue_mapping'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("manual-resolution audit operation");
    assert_eq!(state, pb::DurableOperationState::ManualResolution as i32);
    let operation = pb::DurableOperation::decode(payload.as_slice())
        .expect("decode manual-resolution audit operation");
    assert!(operation.resolution_required);
    assert_eq!(
        operation.scope,
        Some(pb::durable_operation::Scope::CandidateId(
            "candidate-conflicted".into()
        ))
    );
    let operation_count: i64 = storage
        .connection
        .query_row(
            "SELECT COUNT(*) FROM manager_operation
                  WHERE method = 'manager_migrate_legacy_issue_mapping'",
            [],
            |row| row.get(0),
        )
        .expect("count migration audits");
    drop(storage);
    let storage = ManagerStorage::open(&config).expect("reopen completed migration");
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_operation
                      WHERE method = 'manager_migrate_legacy_issue_mapping'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("migration audit is not duplicated on reopen"),
        operation_count,
    );
}

#[test]
#[ignore = "v1 roots are intentionally rejected instead of migrated"]
fn migrates_issue_conflicts_and_corrupt_payloads_to_manual_resolution() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("initialize current schema");
    storage
        .connection
        .execute_batch(
            "DROP TABLE manager_gateway_replay_record;
                 DROP TABLE manager_gateway_epoch;
                 DROP TABLE manager_gateway_registration;
                 DROP TABLE manager_operation_history_issue;
                 DROP TABLE manager_operation_history_projection;
                 DROP TABLE manager_issue_mapping_index;
                 PRAGMA user_version = 5;",
        )
        .expect("restore v5 schema shape");
    for (entity_id, candidate_id) in [
        ("legacy-first", "candidate-first"),
        ("legacy-second", "candidate-second"),
    ] {
        let mapping = pb::IssueMapping {
            issue_id: "issue-conflicted".into(),
            candidate_id: Some(candidate_id.into()),
            backend_id: format!("backend-{candidate_id}"),
            ..Default::default()
        };
        storage
            .connection
            .execute(
                "INSERT INTO manager_entity
                        (entity_kind, entity_id, revision, payload, payload_schema,
                         payload_digest, created_at, updated_at)
                     VALUES ('issue_mapping', ?1, 0, ?2,
                             'issue_tracker.manager.v1.IssueMapping', 'legacy', 10, 11)",
                params![entity_id, mapping.encode_to_vec()],
            )
            .expect("seed conflicting Issue mapping");
    }
    storage
        .connection
        .execute(
            "INSERT INTO manager_entity
                    (entity_kind, entity_id, revision, payload, payload_schema,
                     payload_digest, created_at, updated_at)
                 VALUES ('issue_mapping', 'corrupt-mapping', 0, X'FF',
                         'issue_tracker.manager.v1.IssueMapping', 'legacy', 10, 11)",
            [],
        )
        .expect("seed corrupt mapping");
    drop(storage);

    let storage = ManagerStorage::open(&config).expect("corrupt migration completes");
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_issue_mapping_index",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("conflicting mappings are not indexed"),
        0
    );
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_operation
                      WHERE method = 'manager_migrate_legacy_issue_mapping'
                        AND state = ?1",
                [pb::DurableOperationState::ManualResolution as i32],
                |row| row.get::<_, i64>(0),
            )
            .expect("read manual-resolution operations"),
        2,
    );
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM manager_operation_history_projection
                      WHERE method = 'manager_migrate_legacy_issue_mapping'
                        AND resolution_required = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("history projection contains migration audits"),
        2,
    );
}

#[test]
fn mapping_index_enforces_one_candidate_and_one_issue() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("open manager storage");

    storage
        .connection
        .execute(
            "INSERT INTO manager_issue_mapping_index
                 (candidate_id, issue_id, mapping_state, operation_id, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params!["candidate-1", "issue-1", 1_i64, "op-1", 10_i64, 10_i64],
        )
        .expect("insert first mapping");
    assert!(
        storage
            .connection
            .execute(
                "INSERT INTO manager_issue_mapping_index
                 (candidate_id, issue_id, mapping_state, operation_id, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params!["candidate-1", "issue-2", 1_i64, "op-2", 11_i64, 11_i64],
            )
            .is_err()
    );
    assert!(
        storage
            .connection
            .execute(
                "INSERT INTO manager_issue_mapping_index
                 (candidate_id, issue_id, mapping_state, operation_id, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params!["candidate-2", "issue-1", 1_i64, "op-3", 12_i64, 12_i64],
            )
            .is_err()
    );
}

#[test]
#[ignore = "v1 roots are intentionally rejected instead of migrated"]
fn reuses_a_verified_backup_when_retrying_an_unfinished_v7_migration() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();

    let storage = ManagerStorage::open(&config).expect("initialize current schema");
    storage
        .connection
        .execute_batch(
            "ALTER TABLE manager_operation_history_projection DROP COLUMN resolution_kind;
                 ALTER TABLE manager_operation_history_projection DROP COLUMN reason_category;
                 ALTER TABLE manager_operation_history_projection DROP COLUMN rationale;
                 DELETE FROM manager_metadata
                  WHERE key = 'list_issues_with_sync_page_token_secret';
                 PRAGMA user_version = 6;",
        )
        .expect("simulate interrupted v7 migration");
    drop(storage);

    let backup = migration_backup_path(&config.data_root.join(DATABASE_FILE), 6);
    let source =
        Connection::open(config.data_root.join(DATABASE_FILE)).expect("open v6 source database");
    source
        .execute_batch(&format!(
            "VACUUM INTO '{}';",
            backup.to_string_lossy().replace('\'', "''")
        ))
        .expect("create verified retry backup");
    drop(source);

    let storage = ManagerStorage::open(&config).expect("retry v7 migration");
    assert_eq!(
        storage
            .raw_query_i64("PRAGMA user_version")
            .expect("read migrated version"),
        SCHEMA_VERSION
    );
    assert!(backup.is_file());
}

#[test]
#[ignore = "v1 roots are intentionally rejected instead of migrated"]
fn migrates_v6_store_with_a_persistent_page_token_secret_and_safe_history_columns() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("initialize current schema");
    storage
        .connection
        .execute_batch(
            "ALTER TABLE manager_operation_history_projection DROP COLUMN resolution_kind;
                 ALTER TABLE manager_operation_history_projection DROP COLUMN reason_category;
                 ALTER TABLE manager_operation_history_projection DROP COLUMN rationale;
                 DELETE FROM manager_metadata
                  WHERE key = 'list_issues_with_sync_page_token_secret';
                 PRAGMA user_version = 6;",
        )
        .expect("restore v6 schema shape");
    drop(storage);

    let storage = ManagerStorage::open(&config).expect("migrate v6 store");
    let secret = storage.list_issues_with_sync_page_token_secret();
    assert_eq!(secret.len(), 64);
    assert!(secret.bytes().all(|byte| byte.is_ascii_hexdigit()));
    let columns: Vec<String> = storage
        .connection
        .prepare("PRAGMA table_info(manager_operation_history_projection)")
        .expect("prepare table info")
        .query_map([], |row| row.get(1))
        .expect("read table info")
        .collect::<Result<_, _>>()
        .expect("collect columns");
    assert!(columns.contains(&"resolution_kind".to_owned()));
    assert!(columns.contains(&"reason_category".to_owned()));
    assert!(columns.contains(&"rationale".to_owned()));
    let secret = secret.to_owned();
    drop(storage);

    let reopened = ManagerStorage::open(&config).expect("reopen migrated store");
    assert_eq!(
        reopened.list_issues_with_sync_page_token_secret(),
        secret.as_str()
    );
}

#[test]
fn page_token_secrets_are_unique_per_manager_data_root() {
    let mut first = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let mut second = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let roots = tempdir().expect("temporary roots");
    first.data_root = roots.path().join("first");
    second.data_root = roots.path().join("second");

    let first = ManagerStorage::open(&first).expect("open first manager storage");
    let second = ManagerStorage::open(&second).expect("open second manager storage");
    assert_ne!(
        first.list_issues_with_sync_page_token_secret(),
        second.list_issues_with_sync_page_token_secret()
    );
}

#[test]
fn rejects_a_corrupted_persisted_page_token_secret() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("initialize current schema");
    storage
        .connection
        .execute(
            "UPDATE manager_metadata SET value = 'not-a-secret' WHERE key = ?1",
            [LIST_ISSUES_WITH_SYNC_PAGE_TOKEN_SECRET_KEY],
        )
        .expect("corrupt persisted secret");
    drop(storage);

    assert!(matches!(
        ManagerStorage::open(&config),
        Err(ManagerStorageError::SchemaMismatch(message))
            if message.contains("page token secret")
    ));
}

#[test]
#[ignore = "v1 roots are intentionally rejected instead of migrated"]
fn migrates_v10_store_with_extraction_apply_replay_indexes() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let storage = ManagerStorage::open(&config).expect("initialize current schema");
    storage
        .connection
        .execute_batch(
            "DROP TABLE manager_extraction_auto_registration_reservation;
                 DROP TABLE manager_extraction_auto_registration;
                 DROP TABLE manager_extraction_apply_key;
                 DROP TABLE manager_extraction_apply_outcome;
                 PRAGMA user_version = 10;",
        )
        .expect("restore v10 schema version");
    drop(storage);

    let storage = ManagerStorage::open(&config).expect("migrate v10 store");
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = 'manager_extraction_apply_outcome'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("inspect replay table"),
        1
    );
    storage
        .connection
        .execute_batch(
            "INSERT INTO manager_extraction_apply_outcome
                   (request_digest, outcome, run_id, skip_reason, created_at, updated_at)
                 VALUES ('digest-1', 1, NULL, 1, 1, 1);
                 INSERT INTO manager_extraction_apply_key
                   (idempotency_key, request_digest, created_at)
                 VALUES ('key-1', 'digest-1', 1)",
        )
        .expect("insert replay record");
    assert!(
        storage
            .connection
            .execute(
                "INSERT INTO manager_extraction_apply_outcome
                   (request_digest, outcome, run_id, skip_reason, created_at, updated_at)
                 VALUES ('digest-1', 1, NULL, 1, 1, 1)",
                [],
            )
            .is_err()
    );
    assert!(
        storage
            .connection
            .execute(
                "INSERT INTO manager_extraction_apply_key
                   (idempotency_key, request_digest, created_at)
                 VALUES ('key-1', 'digest-2', 1)",
                [],
            )
            .is_err()
    );
    storage
        .connection
        .execute(
            "INSERT INTO manager_extraction_apply_key
                   (idempotency_key, request_digest, created_at)
                 VALUES ('key-2', 'digest-1', 1)",
            [],
        )
        .expect("a replayed digest may bind a new key");
}
