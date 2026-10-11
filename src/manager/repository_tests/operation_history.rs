use super::*;
use crate::manager::{
    canonical_service,
    pagination::{PageCursor, PageTokenCodec},
};
use sha2::{Digest, Sha256};

#[test]
fn manager_entity_lists_filter_and_paginate_at_a_generation_bound() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);

    for (id, title, status, run_id) in [
        (
            "candidate-1",
            "alpha",
            pb::CandidateStatus::Proposed,
            "run-1",
        ),
        (
            "candidate-2",
            "bravo",
            pb::CandidateStatus::AutoRegistered,
            "run-2",
        ),
        (
            "candidate-3",
            "charlie",
            pb::CandidateStatus::Proposed,
            "run-3",
        ),
    ] {
        repository
            .put_proto(
                EntityKind::Candidate,
                id,
                0,
                "issue_tracker.manager.v1.Candidate",
                &pb::Candidate {
                    candidate_id: id.into(),
                    title: title.into(),
                    status: status as i32,
                    extraction_run_id: run_id.into(),
                    source_evidence: if id == "candidate-2" {
                        vec![v1::SourceEvidenceRef {
                            thread_id: "thread-bravo".into(),
                            memory_id: "memory-bravo".into(),
                        }]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                },
            )
            .expect("seed candidate");
    }
    let args = pb::ListCandidatesArgs {
        context: Some(list_read_context()),
        page: Some(pb::PageRequest {
            page_size: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let first = GatewayRepository::list_candidates(&repository, &args, PageSizeLimits::new(2, 2))
        .expect("first page");
    assert_eq!(first.candidates[0].candidate_id, "candidate-1");
    let token = first
        .page
        .as_ref()
        .and_then(|page| page.next_page_token.clone())
        .expect("continuation token");
    let second = GatewayRepository::list_candidates(
        &repository,
        &pb::ListCandidatesArgs {
            context: Some(list_read_context()),
            page: Some(pb::PageRequest {
                page_size: u32::MAX,
                page_token: Some(token.clone()),
            }),
            ..Default::default()
        },
        PageSizeLimits::new(2, 2),
    )
    .expect("second bounded page");
    assert_eq!(
        second
            .candidates
            .iter()
            .map(|candidate| candidate.candidate_id.as_str())
            .collect::<Vec<_>>(),
        vec!["candidate-2", "candidate-3"]
    );
    assert_eq!(second.page.expect("page info").total_size, Some(3));

    let invalid_scope = GatewayRepository::list_candidates(
        &repository,
        &pb::ListCandidatesArgs {
            context: Some(pb::ManagerReadContext {
                correlation_id: "different-reader".into(),
                ..list_read_context()
            }),
            page: Some(pb::PageRequest {
                page_size: 1,
                page_token: Some(token.clone()),
            }),
            ..Default::default()
        },
        PageSizeLimits::new(2, 2),
    )
    .expect_err("continuation must not cross a read context");
    assert_eq!(
        invalid_scope.code,
        pb::ManagerErrorCode::PageTokenExpired as i32
    );
    repository
        .put_proto(
            EntityKind::Candidate,
            "candidate-4",
            0,
            "issue_tracker.manager.v1.Candidate",
            &pb::Candidate {
                candidate_id: "candidate-4".into(),
                ..Default::default()
            },
        )
        .expect("advance manager generation");
    let expired = GatewayRepository::list_candidates(
        &repository,
        &pb::ListCandidatesArgs {
            context: Some(list_read_context()),
            page: Some(pb::PageRequest {
                page_size: 1,
                page_token: Some(token),
            }),
            ..Default::default()
        },
        PageSizeLimits::new(2, 2),
    )
    .expect_err("continuation must expire after a manager write");
    assert_eq!(expired.code, pb::ManagerErrorCode::PageTokenExpired as i32);

    let filtered = GatewayRepository::list_candidates(
        &repository,
        &pb::ListCandidatesArgs {
            context: Some(list_read_context()),
            filter: Some(pb::CandidateFilter {
                statuses: vec![pb::CandidateStatus::AutoRegistered as i32],
                source_evidence: Some(v1::SourceEvidenceRef {
                    thread_id: "thread-bravo".into(),
                    memory_id: "memory-bravo".into(),
                }),
                text_query: Some("RAV".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        PageSizeLimits::new(2, 2),
    )
    .expect("filtered candidates");
    assert_eq!(filtered.candidates.len(), 1);
    assert_eq!(filtered.candidates[0].candidate_id, "candidate-2");
}

#[test]
fn operation_history_paginates_with_a_stable_bound_continuation() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    seed_history_operation(&mut repository, "op-1", 1);
    seed_history_operation(&mut repository, "op-2", 2);
    seed_history_operation(&mut repository, "op-3", 3);

    let first = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest {
            page_size: 1,
            ..Default::default()
        }),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("first history page");
    assert_eq!(first.entries[0].operation_id, "op-3");
    let token = first
        .page
        .as_ref()
        .and_then(|page| page.next_page_token.clone())
        .expect("continuation token");

    let second = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest {
            page_size: 1,
            page_token: Some(token),
        }),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("second history page");
    assert_eq!(second.entries[0].operation_id, "op-2");
    assert_eq!(second.page.expect("page info").total_size, Some(3));
}

#[test]
fn operation_history_uses_the_configured_default_and_maximum_page_sizes() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    seed_history_operation(&mut repository, "op-1", 1);
    seed_history_operation(&mut repository, "op-2", 2);
    seed_history_operation(&mut repository, "op-3", 3);
    let limits = PageSizeLimits::new(2, 2);

    let default_page = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest::default()),
        90 * 24 * 60 * 60 * 1_000,
        limits,
    )
    .expect("default history page");
    assert_eq!(default_page.entries.len(), 2);
    assert!(
        default_page
            .page
            .as_ref()
            .and_then(|page| page.next_page_token.as_ref())
            .is_some()
    );

    let maximum_page = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest {
            page_size: u32::MAX,
            ..Default::default()
        }),
        90 * 24 * 60 * 60 * 1_000,
        limits,
    )
    .expect("bounded history page");
    assert_eq!(maximum_page.entries.len(), 2);
}

#[test]
fn operation_history_uses_keyset_tie_breaking_and_can_omit_total_count() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    seed_history_operation(&mut repository, "op-a", 10);
    seed_history_operation(&mut repository, "op-b", 10);
    seed_history_operation(&mut repository, "op-c", 9);

    let mut first_args = history_args(pb::PageRequest {
        page_size: 1,
        ..Default::default()
    });
    first_args.include_total_size = Some(false);
    let first = GatewayRepository::list_operation_history(
        &repository,
        &first_args,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("first keyset page");
    assert_eq!(first.entries[0].operation_id, "op-b");
    assert_eq!(first.page.as_ref().and_then(|page| page.total_size), None);

    let second = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest {
            page_size: 1,
            page_token: first
                .page
                .as_ref()
                .and_then(|page| page.next_page_token.clone()),
        }),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("second keyset page");
    assert_eq!(second.entries[0].operation_id, "op-a");
}

#[test]
fn operation_history_finds_an_affected_issue_through_its_projection() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let mut operation = history_operation("op-affected-issue", 1);
    operation.affected_issue_ids = vec!["issue-related".into()];
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
    let mut args = history_args(pb::PageRequest::default());
    args.filter.as_mut().expect("filter").direct_target = Some(
        pb::operation_history_filter::DirectTarget::IssueId("issue-related".into()),
    );

    let page = GatewayRepository::list_operation_history(
        &repository,
        &args,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("affected issue history page");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].operation_id, operation.operation_id);

    args.filter.as_mut().expect("filter").direct_scope_only = true;
    let direct_only = GatewayRepository::list_operation_history(
        &repository,
        &args,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("direct-only issue history page");
    assert!(direct_only.entries.is_empty());
}

#[test]
fn operation_history_hydrates_only_selected_ids_and_fails_for_selected_corruption() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    {
        let mut repository = ManagerRepository::new(&mut storage);
        seed_history_operation(&mut repository, "op-selected", 2);
        seed_history_operation(&mut repository, "op-unselected", 1);
    }
    storage
        .write(|transaction, _| {
            transaction.execute(
                "UPDATE manager_operation_history_projection
                SET method='other-method'
              WHERE operation_id='op-unselected'",
                [],
            )?;
            Ok(())
        })
        .expect("hide unselected operation");
    storage
        .write(|transaction, _| {
            transaction.execute(
                "UPDATE manager_operation
                SET operation_payload = X'00', operation_payload_digest = 'broken'
              WHERE operation_id='op-unselected'",
                [],
            )?;
            Ok(())
        })
        .expect("corrupt unselected payload");
    let mut args = history_args(pb::PageRequest::default());
    args.filter.as_mut().expect("history filter").method = Some("manager_create_candidate".into());
    let page = {
        let repository = ManagerRepository::new(&mut storage);
        GatewayRepository::list_operation_history(
            &repository,
            &args,
            90 * 24 * 60 * 60 * 1_000,
            PageSizeLimits::new(100, 100),
        )
        .expect("unselected corruption must not be read")
    };
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].operation_id, "op-selected");

    storage
        .write(|transaction, _| {
            transaction.execute(
                "UPDATE manager_operation
                SET operation_payload = X'00', operation_payload_digest = 'broken'
              WHERE operation_id='op-selected'",
                [],
            )?;
            Ok(())
        })
        .expect("corrupt selected payload");
    let repository = ManagerRepository::new(&mut storage);
    assert!(
        GatewayRepository::list_operation_history(
            &repository,
            &args,
            90 * 24 * 60 * 60 * 1_000,
            PageSizeLimits::new(100, 100),
        )
        .is_err()
    );
}

#[test]
fn operation_insert_derives_missing_payload_timestamps_for_history_integrity() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let operation = pb::DurableOperation {
        operation_id: "op-derived-timestamps".into(),
        scope: Some(pb::durable_operation::Scope::CandidateId(
            "candidate-1".into(),
        )),
        method: "manager_create_candidate".into(),
        idempotency_scope: "scope-derived-timestamps".into(),
        request_digest: "digest-derived-timestamps".into(),
        state: pb::DurableOperationState::Received as i32,
        ..Default::default()
    };
    let mut repository = ManagerRepository::new(&mut storage);
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
    let stored = repository
        .get_operation(&operation.operation_id)
        .expect("read operation")
        .expect("operation exists");
    assert!(stored.created_at.is_some());
    assert!(stored.updated_at.is_some());
}

#[test]
fn arbitration_updates_superseded_pending_operation_timestamp_and_projection() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");

    let source = history_operation("arbitration-source", 100);
    let mut pending = history_operation("arbitration-pending", 200);
    pending.method = "manager_resolve_issue_operation".into();
    pending.request_schema = "issue_tracker.manager.v1.ResolveIssueOperationArgs".into();
    let pending_request = b"pending-resolution-request".to_vec();
    pending.request_digest = digest_bytes(&pending_request);
    {
        let mut repository = ManagerRepository::new(&mut storage);
        repository
            .commit_mutation(
                &pb::ManagerMutation {
                    operation: Some(source.clone()),
                    ..Default::default()
                },
                &source,
                None,
            )
            .expect("seed source operation");
        repository
            .commit_mutation(
                &pb::ManagerMutation {
                    operation: Some(pending.clone()),
                    ..Default::default()
                },
                &pending,
                None,
            )
            .expect("seed pending resolution operation");
    }

    let resolution_request = b"arbitration-resolution-request".to_vec();
    let mut resolution = history_operation("arbitration-resolution", 300);
    resolution.method = "manager_resolve_issue_operation".into();
    resolution.request_schema = "issue_tracker.manager.v1.ResolveIssueOperationArgs".into();
    resolution.idempotency_scope =
        "contract/manager_resolve_issue_operation/candidate:candidate-1/arbitration".into();
    resolution.request_digest = digest_bytes(&resolution_request);
    {
        let mut repository = ManagerRepository::new(&mut storage);
        repository
            .commit_mutation(
                &pb::ManagerMutation {
                    operation: Some(resolution.clone()),
                    ..Default::default()
                },
                &resolution,
                None,
            )
            .expect("seed arbitration operation");
    }
    storage
        .write(|transaction, _| {
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
            let now = now_millis();
            for (operation, request) in [
                (&pending, &pending_request),
                (&resolution, &resolution_request),
            ] {
                transaction.execute(
                    "INSERT INTO manager_resolution_request
                        (contract_fingerprint, data_root_id, resolution_operation_id,
                         source_operation_id, issue_id, request_payload, request_digest,
                         state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'PENDING', ?8, ?8)",
                    rusqlite::params![
                        contract,
                        root,
                        operation.operation_id,
                        source.operation_id,
                        "issue-1",
                        request,
                        operation.request_digest,
                        now,
                    ],
                )?;
            }
            Ok(())
        })
        .expect("seed pending resolution requests");

    let mut arbitrated_source = source.clone();
    arbitrated_source.state = pb::DurableOperationState::Arbitrated as i32;
    arbitrated_source.generation += 1;
    let mut committed_resolution = resolution.clone();
    committed_resolution.state = pb::DurableOperationState::Committed as i32;
    committed_resolution.generation += 1;
    let arbitration_started_at = now_millis();
    {
        let mut repository = ManagerRepository::new(&mut storage);
        repository
            .commit_issue_operation_resolution(
                &arbitrated_source,
                &committed_resolution,
                Some(b"source-result"),
                b"resolution-result",
                &resolution_request,
                pb::IssueOperationResolutionDecision::ConfirmNotApplied as i32,
                pb::IssueOperationResolutionMode::HumanArbitration as i32,
                pb::IssueOperationArbitrationOutcome::UnknownSettled as i32,
                1,
            )
            .expect("commit arbitration");
    }

    let pending_after = {
        let repository = ManagerRepository::new(&mut storage);
        repository
            .get_operation(&pending.operation_id)
            .expect("read superseded operation")
            .expect("superseded operation exists")
    };
    let pending_updated_at = pending_after
        .updated_at
        .expect("superseded operation updated_at")
        .unix_epoch_millis;
    assert_eq!(
        pending_after.state,
        pb::DurableOperationState::Arbitrated as i32
    );
    assert!(pending_updated_at >= arbitration_started_at);
    let projection_updated_at: i64 = storage
        .read(|connection| {
            let updated_at = connection.query_row(
                "SELECT updated_at FROM manager_operation_history_projection
                  WHERE operation_id=?1",
                [&pending.operation_id],
                |row| row.get(0),
            )?;
            Ok(updated_at)
        })
        .expect("read superseded projection timestamp");
    assert_eq!(projection_updated_at, pending_updated_at);
}

#[test]
fn operation_history_rejects_tampered_or_scope_changed_page_tokens() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    seed_history_operation(&mut repository, "op-1", 1);
    seed_history_operation(&mut repository, "op-2", 2);
    let first = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest {
            page_size: 1,
            ..Default::default()
        }),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("first history page");
    let token = first
        .page
        .as_ref()
        .and_then(|page| page.next_page_token.clone())
        .expect("continuation token");

    let error = GatewayRepository::list_candidates(
        &repository,
        &pb::ListCandidatesArgs {
            context: Some(list_read_context()),
            page: Some(pb::PageRequest {
                page_size: 1,
                page_token: Some(token.clone()),
            }),
            ..Default::default()
        },
        PageSizeLimits::new(100, 100),
    )
    .expect_err("history v2 token must not cross into offset APIs");
    assert_eq!(error.code, pb::ManagerErrorCode::InvalidPageToken as i32);

    let mut tampered = history_args(pb::PageRequest {
        page_size: 1,
        page_token: Some(format!("{token}x")),
    });
    let error = GatewayRepository::list_operation_history(
        &repository,
        &tampered,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect_err("tampered token must fail");
    assert_eq!(error.code, pb::ManagerErrorCode::InvalidPageToken as i32);

    tampered.page.as_mut().expect("page").page_token = Some(token);
    tampered.context.as_mut().expect("context").data_root_id = "other-root".into();
    let error = GatewayRepository::list_operation_history(
        &repository,
        &tampered,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect_err("cross-root token must fail");
    assert_eq!(error.code, pb::ManagerErrorCode::PageTokenExpired as i32);

    tampered.context.as_mut().expect("context").data_root_id = "root".into();
    tampered.context.as_mut().expect("context").correlation_id = "other-read".into();
    let error = GatewayRepository::list_operation_history(
        &repository,
        &tampered,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect_err("cross-read-scope token must fail");
    assert_eq!(error.code, pb::ManagerErrorCode::PageTokenExpired as i32);

    tampered.context.as_mut().expect("context").correlation_id = "history-read".into();
    tampered.filter.as_mut().expect("filter").method = Some("other-method".into());
    let error = GatewayRepository::list_operation_history(
        &repository,
        &tampered,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect_err("cross-filter token must fail");
    assert_eq!(error.code, pb::ManagerErrorCode::PageTokenExpired as i32);
}

#[test]
fn operation_history_expires_page_tokens_after_a_manager_write() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    seed_history_operation(&mut repository, "op-1", 1);
    seed_history_operation(&mut repository, "op-2", 2);
    let first = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest {
            page_size: 1,
            ..Default::default()
        }),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect("first history page");
    let token = first
        .page
        .as_ref()
        .and_then(|page| page.next_page_token.clone())
        .expect("continuation token");
    seed_history_operation(&mut repository, "op-3", 3);

    let error = GatewayRepository::list_operation_history(
        &repository,
        &history_args(pb::PageRequest {
            page_size: 1,
            page_token: Some(token),
        }),
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
    )
    .expect_err("stale token must expire");
    assert_eq!(error.code, pb::ManagerErrorCode::PageTokenExpired as i32);
}

#[test]
fn operation_history_accepts_v1_only_during_compatibility_and_reissues_v2() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    {
        let mut repository = ManagerRepository::new(&mut storage);
        seed_history_operation(&mut repository, "op-1", 1);
        seed_history_operation(&mut repository, "op-2", 2);
        seed_history_operation(&mut repository, "op-3", 3);
    }

    let initial = history_args(pb::PageRequest::default());
    let filter = initial.filter.as_ref().expect("history filter");
    let context = initial.context.as_ref().expect("history context");
    let generation = storage.generation();
    let secret = canonical_service::list_page_token_secret(&storage);
    let query_fingerprint = format!(
        "{:x}",
        Sha256::digest(
            [
                b"list-operation-history\0".as_slice(),
                filter.encode_to_vec().as_slice(),
                b"\0",
                context.encode_to_vec().as_slice(),
            ]
            .concat(),
        )
    );
    let legacy = PageTokenCodec::new(secret).encode(&PageCursor {
        offset: 1,
        generation,
        snapshot_id: format!("manager-{generation}"),
        order_fingerprint: "created-at-desc-operation-id-desc".into(),
        query_fingerprint,
        expires_at_unix_epoch_millis: i64::MAX,
    });
    let repository = ManagerRepository::new(&mut storage);
    let mut args = initial.clone();
    args.page = Some(pb::PageRequest {
        page_size: 1,
        page_token: Some(legacy.clone()),
    });
    let page = GatewayRepository::list_operation_history_with_compat(
        &repository,
        &args,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
        true,
    )
    .expect("legacy token accepted during migration");
    assert_eq!(page.entries[0].operation_id, "op-2");
    assert!(
        page.page
            .and_then(|info| info.next_page_token)
            .is_some_and(|token| token.starts_with("operation-history-page-v2|"))
    );

    let error = GatewayRepository::list_operation_history_with_compat(
        &repository,
        &args,
        90 * 24 * 60 * 60 * 1_000,
        PageSizeLimits::new(100, 100),
        false,
    )
    .expect_err("legacy token rejected after compatibility window");
    assert_eq!(error.code, pb::ManagerErrorCode::InvalidPageToken as i32);
}
