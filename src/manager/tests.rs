use super::*;
use async_trait::async_trait;
use prost::Message;
use rusqlite::params;
use std::sync::{
    Arc,
    atomic::AtomicBool,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tempfile::tempdir;

struct NeverCalledDispatcher {
    calls: Arc<AtomicUsize>,
}

#[allow(dead_code)]
struct AcceptanceMarkerDispatcher;

struct CapabilityMarkerDispatcher;

struct ClaimCapabilityDispatcher {
    calls: Arc<AtomicUsize>,
}

struct UnavailableCapabilityDispatcher;

#[test]
fn tracker_service_error_preserves_revision_conflict_detail() {
    let error = tracker_service_error(crate::TrackerError::RevisionConflict {
        expected: 4,
        actual: 7,
    });
    assert_eq!(
        error.code,
        crate::proto_types::manager_v1::ManagerErrorCode::RevisionConflict as i32
    );
    assert!(matches!(
        error.detail,
        Some(crate::proto_types::manager_v1::manager_error::Detail::RevisionConflict(
            conflict
        )) if conflict.expected_revision == 4 && conflict.current_revision == 7
    ));
}

#[async_trait]
impl backend_dispatch::TaskChampionV1Dispatcher for UnavailableCapabilityDispatcher {
    async fn dispatch(
        &self,
        _request: &backend_dispatch::BackendDispatchRequest,
    ) -> Result<backend_dispatch::SouthboundResponse, backend_dispatch::SouthboundError> {
        Err(backend_dispatch::SouthboundError::delivery_unknown(
            "transport result lost",
        ))
    }
}

#[async_trait]
impl backend_dispatch::TaskChampionV1Dispatcher for CapabilityMarkerDispatcher {
    async fn dispatch(
        &self,
        request: &backend_dispatch::BackendDispatchRequest,
    ) -> Result<backend_dispatch::SouthboundResponse, backend_dispatch::SouthboundError> {
        let args = crate::proto_types::manager_v1::HeartbeatIssueClaimArgs::decode(
            request.payload.as_slice(),
        )
        .map_err(|error| backend_dispatch::SouthboundError::not_sent(error.to_string()))?;
        let downstream = request
            .downstream_operation_id
            .clone()
            .ok_or_else(|| backend_dispatch::SouthboundError::not_sent("operation missing"))?;
        let method_key = "heartbeat_issue_claim".to_owned();
        let payload_fingerprint = crate::tracker::operation_log::marker_digest(&[
            method_key.as_str(),
            downstream.as_str(),
        ]);
        let revision = args
            .expected_revision
            .ok_or_else(|| backend_dispatch::SouthboundError::not_sent("revision missing"))?
            + 1;
        Ok(backend_dispatch::SouthboundResponse::committed(
            crate::v1::OperationRecord {
                downstream_operation_id: downstream.clone(),
                affected_issue_ids: vec![args.issue_id.clone()],
                payload_fingerprint: payload_fingerprint.clone(),
                contract_fingerprint: "task-contract".into(),
                scope: crate::v1::OperationScope::Issue as i32,
                method_key: method_key.clone(),
                entity_revisions: vec![crate::v1::OperationEntityRevision {
                    issue_id: args.issue_id.clone(),
                    backend_id: args.issue_id,
                    resulting_revision: revision,
                }],
                commit_durability: crate::v1::OperationCommitDurability::Durable as i32,
                result_replayability: crate::v1::OperationResultReplayability::NotReplayable as i32,
                result_snapshot: None,
                fingerprint_input: Some(crate::v1::OperationFingerprintInput {
                    downstream_operation_id: downstream,
                    method_key,
                    scope: crate::v1::OperationScope::Issue as i32,
                    payload_fingerprint,
                }),
                ..Default::default()
            },
            Vec::new(),
        ))
    }
}

#[async_trait]
impl backend_dispatch::TaskChampionV1Dispatcher for ClaimCapabilityDispatcher {
    async fn dispatch(
        &self,
        _request: &backend_dispatch::BackendDispatchRequest,
    ) -> Result<backend_dispatch::SouthboundResponse, backend_dispatch::SouthboundError> {
        Err(backend_dispatch::SouthboundError::not_sent(
            "claim must use capability dispatch",
        ))
    }

    async fn dispatch_capability(
        &self,
        request: &backend_dispatch::BackendDispatchRequest,
    ) -> Result<backend_dispatch::SouthboundResponse, backend_dispatch::SouthboundError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let args =
            crate::proto_types::manager_v1::ClaimIssueArgs::decode(request.payload.as_slice())
                .map_err(|error| backend_dispatch::SouthboundError::not_sent(error.to_string()))?;
        let downstream = request
            .downstream_operation_id
            .clone()
            .ok_or_else(|| backend_dispatch::SouthboundError::not_sent("operation missing"))?;
        let payload_fingerprint =
            crate::tracker::operation_log::marker_digest(&["claim_issue", downstream.as_str()]);
        let resulting_revision = args
            .expected_revision
            .ok_or_else(|| backend_dispatch::SouthboundError::not_sent("revision missing"))?
            + 1;
        let marker = crate::v1::OperationRecord {
            downstream_operation_id: downstream.clone(),
            affected_issue_ids: vec![args.issue_id.clone()],
            payload_fingerprint: payload_fingerprint.clone(),
            contract_fingerprint: "task-contract".into(),
            scope: crate::v1::OperationScope::Issue as i32,
            method_key: "claim_issue".into(),
            entity_revisions: vec![crate::v1::OperationEntityRevision {
                issue_id: args.issue_id.clone(),
                backend_id: args.issue_id.clone(),
                resulting_revision,
            }],
            commit_durability: crate::v1::OperationCommitDurability::Durable as i32,
            result_replayability: crate::v1::OperationResultReplayability::NotReplayable as i32,
            fingerprint_input: Some(crate::v1::OperationFingerprintInput {
                downstream_operation_id: downstream,
                method_key: "claim_issue".into(),
                scope: crate::v1::OperationScope::Issue as i32,
                payload_fingerprint,
            }),
            ..Default::default()
        };
        let current = crate::v1::Issue {
            issue_id: args.issue_id.clone(),
            backend_id: args.issue_id.clone(),
            revision: resulting_revision,
            title: "claimed issue".into(),
            ..Default::default()
        };
        let response = crate::v1::ClaimIssueResult {
            outcome: Some(crate::v1::claim_issue_result::Outcome::Success(
                crate::v1::ClaimIssueSuccess {
                    mutation: Some(crate::v1::IssueMutation {
                        current: Some(crate::v1::IssueDetails {
                            issue: Some(current),
                            ..Default::default()
                        }),
                        operation: Some(marker.clone()),
                        ..Default::default()
                    }),
                    lease_handle: b"ephemeral-claim-handle".to_vec(),
                },
            )),
        };
        Ok(backend_dispatch::SouthboundResponse::committed_claim(
            marker,
            response.encode_to_vec(),
            b"ephemeral-claim-handle".to_vec(),
        ))
    }
}

#[async_trait]
impl backend_dispatch::TaskChampionV1Dispatcher for AcceptanceMarkerDispatcher {
    async fn dispatch(
        &self,
        request: &backend_dispatch::BackendDispatchRequest,
    ) -> Result<backend_dispatch::SouthboundResponse, backend_dispatch::SouthboundError> {
        let envelope = backend_dispatch::DurableBackendRequest::decode(request.payload.as_slice())
            .map_err(|error| backend_dispatch::SouthboundError::not_sent(error.to_string()))?;
        let create = envelope
            .create_issue
            .ok_or_else(|| backend_dispatch::SouthboundError::not_sent("create missing"))?;
        let issue_id = create.issue_id;
        let downstream = request
            .downstream_operation_id
            .clone()
            .ok_or_else(|| backend_dispatch::SouthboundError::not_sent("operation missing"))?;
        let method_key = "create_issue".to_owned();
        let payload_fingerprint = crate::tracker::operation_log::marker_digest(&[
            method_key.as_str(),
            downstream.as_str(),
        ]);
        let marker = crate::v1::OperationRecord {
            downstream_operation_id: downstream.clone(),
            affected_issue_ids: vec![issue_id.clone()],
            payload_fingerprint: payload_fingerprint.clone(),
            contract_fingerprint: "task-contract".into(),
            scope: crate::v1::OperationScope::Issue as i32,
            method_key: method_key.clone(),
            entity_revisions: vec![crate::v1::OperationEntityRevision {
                issue_id: issue_id.clone(),
                backend_id: issue_id,
                resulting_revision: 0,
            }],
            commit_durability: crate::v1::OperationCommitDurability::Durable as i32,
            result_replayability: crate::v1::OperationResultReplayability::Replayable as i32,
            result_snapshot: Some(crate::v1::OperationResultSnapshot::default()),
            fingerprint_input: Some(crate::v1::OperationFingerprintInput {
                downstream_operation_id: downstream,
                method_key,
                scope: crate::v1::OperationScope::Issue as i32,
                payload_fingerprint,
            }),
            ..Default::default()
        };
        Ok(backend_dispatch::SouthboundResponse::committed(
            marker,
            vec![1, 2, 3],
        ))
    }
}

#[async_trait]
impl backend_dispatch::TaskChampionV1Dispatcher for NeverCalledDispatcher {
    async fn dispatch(
        &self,
        _request: &backend_dispatch::BackendDispatchRequest,
    ) -> Result<backend_dispatch::SouthboundResponse, backend_dispatch::SouthboundError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(backend_dispatch::SouthboundError::not_sent(
            "legacy request must not be dispatched",
        ))
    }
}

fn manager_for_test() -> (ManagerConfig, IssueTrackerManager) {
    let root = tempdir().expect("temporary manager root");
    let mut settings = config::tests::settings();
    let root_path = root.keep();
    settings.data_root = root_path.to_string_lossy().into_owned();
    let config = ManagerConfig::from_proto(&settings).expect("valid settings");
    let manager = IssueTrackerManager::open(config.clone()).expect("open manager");
    (config, manager)
}

fn review_context(
    config: &ManagerConfig,
    correlation_id: &str,
) -> crate::proto_types::manager_v1::ManagerReadContext {
    crate::proto_types::manager_v1::ManagerReadContext {
        correlation_id: correlation_id.into(),
        schema_fingerprint: config.expected_schema_fingerprint.clone(),
        data_root_id: config.manager_identity().data_root_id.clone(),
        contract_fingerprint: config.expected_contract_fingerprint.clone(),
    }
}

async fn dispatch_review_page(
    manager: &Arc<tokio::sync::Mutex<IssueTrackerManager>>,
    tracker: &crate::tracker::Tracker,
    args: &crate::proto_types::manager_v1::ListReviewCandidatesArgs,
) -> crate::proto_types::manager_v1::ListReviewCandidatesResult {
    let prepared = manager
        .lock()
        .await
        .prepare_review_candidate_read(&args.encode_to_vec())
        .expect("prepare review candidate page");
    crate::proto_types::manager_v1::ListReviewCandidatesResult::decode(
        dispatch_review_candidate_read(tracker, prepared)
            .await
            .as_slice(),
    )
    .expect("decode review candidate page")
}

/// Follows continuation tokens; test managers may cap pages at one row.
async fn collect_review_rows(
    manager: &Arc<tokio::sync::Mutex<IssueTrackerManager>>,
    tracker: &crate::tracker::Tracker,
    context: crate::proto_types::manager_v1::ManagerReadContext,
) -> Vec<crate::proto_types::manager_v1::ReviewCandidateRow> {
    use crate::proto_types::manager_v1::list_review_candidates_result::Outcome;
    let mut rows = Vec::new();
    let mut page_token = None;
    loop {
        let args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
            context: Some(context.clone()),
            page: Some(crate::proto_types::manager_v1::PageRequest {
                page_token: page_token.take(),
                ..Default::default()
            }),
            ..Default::default()
        };
        match dispatch_review_page(manager, tracker, &args)
            .await
            .outcome
            .expect("review candidate outcome")
        {
            Outcome::Success(page) => {
                rows.extend(page.rows);
                page_token = page.page.and_then(|page| page.next_page_token);
            }
            Outcome::Error(error) => panic!("unexpected review candidate read error: {error:?}"),
        }
        if page_token.is_none() {
            return rows;
        }
    }
}

fn store_candidate(
    manager: &mut IssueTrackerManager,
    candidate: &crate::proto_types::manager_v1::Candidate,
) {
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::Candidate,
            &candidate.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            candidate,
        )
        .expect("store candidate");
}

fn store_auto_registered_candidate_with_mapping(
    manager: &mut IssueTrackerManager,
    config: &ManagerConfig,
    candidate_id: &str,
    issue_id: uuid::Uuid,
    revision: u64,
) {
    let candidate = crate::proto_types::manager_v1::Candidate {
        candidate_id: candidate_id.into(),
        status: crate::proto_types::manager_v1::CandidateStatus::AutoRegistered as i32,
        ..Default::default()
    };
    store_candidate(manager, &candidate);
    let mapping = crate::proto_types::manager_v1::IssueMapping {
        issue_id: issue_id.to_string(),
        candidate_id: Some(candidate.candidate_id),
        backend_id: issue_id.to_string(),
        revision,
        backend_contract_fingerprint: config.taskchampion_identity().contract_fingerprint.clone(),
        manager_contract_fingerprint: config.manager_identity().contract_fingerprint.clone(),
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            &mapping.issue_id,
            0,
            "issue_tracker.manager.v1.IssueMapping",
            &mapping,
        )
        .expect("store mapping");
}

fn ownership_recovery_state(
    issue_id: &str,
    readmission_number: u64,
    operation_id: Option<&str>,
    in_flight: bool,
) -> repository::OwnershipRecoveryState {
    repository::OwnershipRecoveryState {
        issue_id: issue_id.into(),
        expected_revision: 0,
        readmission_number,
        current_operation_id: operation_id.map(str::to_owned),
        in_flight,
        consecutive_failures: 3,
        next_attempt_at_millis: Some(123),
        last_failure_class: Some("BACKEND_UNAVAILABLE".into()),
        stopped: true,
        stop_basis: Some("test stop basis".into()),
        repair_condition: Some("test repair condition".into()),
        cursor_diagnostic: Some("test cursor diagnostic".into()),
    }
}

fn seed_ownership_recovery_projection(
    manager: &mut IssueTrackerManager,
    issue_id: &str,
    readmission_number: u64,
    operation_id: &str,
    operation_generation: u64,
    source_generation: u64,
    terminal_state: i32,
) {
    let contract = manager.config().expected_contract_fingerprint.clone();
    let root = manager.config().manager_identity().data_root_id.clone();
    let issue_id = issue_id.to_owned();
    let operation_id = operation_id.to_owned();
    manager
        .storage_mut()
        .write_with::<_, repository::RepositoryError, _>(|transaction, _| {
            transaction.execute(
                "INSERT INTO manager_operation
                    (operation_id, method, idempotency_scope, request_digest, state,
                     attempt, manager_sequence, generation, causal_parent, sync_subject,
                     affects_ready, snapshot, error_snapshot, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    operation_id.as_str(),
                    "manager_apply_issue_admin_action",
                    format!("test-scope-{operation_id}"),
                    format!("test-digest-{operation_id}"),
                    terminal_state,
                    0_i64,
                    0_i64,
                    i64::try_from(operation_generation).expect("operation generation fits SQLite"),
                    Option::<String>::None,
                    Option::<String>::None,
                    0_i64,
                    Option::<Vec<u8>>::None,
                    Option::<Vec<u8>>::None,
                    0_i64,
                    0_i64,
                ],
            )?;
            transaction.execute(
                "INSERT INTO manager_ownership_recovery_projection
                    (contract_fingerprint, data_root_id, issue_id, readmission_number,
                     operation_id, source_generation, terminal_state, projected_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    contract.as_str(),
                    root.as_str(),
                    issue_id.as_str(),
                    i64::try_from(readmission_number).expect("readmission number fits SQLite"),
                    operation_id.as_str(),
                    i64::try_from(source_generation).expect("source generation fits SQLite"),
                    terminal_state,
                    0_i64,
                ],
            )?;
            Ok(())
        })
        .expect("seed ownership recovery projection");
}

#[test]
fn issue_mapping_builder_preserves_backend_revision_and_lifecycle_timestamps() {
    let (_config, manager) = manager_for_test();
    let created_at = Some(crate::proto_types::manager_v1::UtcInstant {
        unix_epoch_millis: 101,
    });
    let confirmed_at = crate::proto_types::manager_v1::UtcInstant {
        unix_epoch_millis: 202,
    };

    let mappings = manager
        .issue_mappings_from_seeds(
            [
                IssueMappingSeed {
                    issue_id: "new-issue".into(),
                    backend_id: "new-issue".into(),
                    revision: 0,
                },
                IssueMappingSeed {
                    issue_id: "blocks-target".into(),
                    backend_id: "blocks-target".into(),
                    revision: 7,
                },
            ],
            3,
            created_at.clone(),
            confirmed_at.clone(),
        )
        .expect("build mappings");

    assert_eq!(mappings.len(), 2);
    assert_eq!(mappings[0].revision, 0);
    assert_eq!(mappings[1].issue_id, "blocks-target");
    assert_eq!(mappings[1].revision, 7);
    assert_eq!(mappings[0].generation, 3);
    assert_eq!(mappings[0].created_at, created_at);
    assert_eq!(mappings[0].updated_at, Some(confirmed_at));
    assert_eq!(
        mappings[0].backend_contract_fingerprint,
        manager
            .config()
            .taskchampion_identity()
            .contract_fingerprint
    );
    assert_eq!(
        mappings[0].manager_contract_fingerprint,
        manager.config().manager_identity().contract_fingerprint
    );
}

#[test]
fn issue_mapping_builder_inherits_candidate_from_existing_index() {
    let (_config, mut manager) = manager_for_test();
    let mapping = crate::proto_types::manager_v1::IssueMapping {
        issue_id: "refresh-issue".into(),
        candidate_id: Some("refresh-candidate".into()),
        backend_id: "refresh-backend".into(),
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            &mapping.issue_id,
            0,
            "issue_tracker.manager.v1.IssueMapping",
            &mapping,
        )
        .expect("seed existing mapping");
    let mappings = manager
        .issue_mappings_from_seeds(
            [IssueMappingSeed {
                issue_id: mapping.issue_id.clone(),
                backend_id: mapping.backend_id.clone(),
                revision: 1,
            }],
            2,
            None,
            crate::proto_types::manager_v1::UtcInstant {
                unix_epoch_millis: 200,
            },
        )
        .expect("build refreshed mapping");
    assert_eq!(
        mappings[0].candidate_id.as_deref(),
        Some("refresh-candidate")
    );
}

#[test]
fn internal_service_advertises_wired_ownership_capabilities() {
    let (config, manager) = manager_for_test();

    let info = manager.get_contract_info();

    assert_eq!(info.contract_version, MANAGER_CONTRACT_VERSION);
    assert_eq!(
        info.root_operation_history_max_window
            .as_ref()
            .map(|value| value.millis),
        Some(config.root_operation_history_max_window.as_millis() as u64)
    );
    assert_eq!(
        info.health,
        crate::proto_types::manager_v1::HealthStatus::Degraded as i32
    );
    assert_eq!(
        info.activation,
        crate::proto_types::manager_v1::ActivationStatus::Blocked as i32
    );
    assert_eq!(
        info.features,
        vec![
            crate::proto_types::manager_v1::ManagerFeature::Candidates as i32,
            crate::proto_types::manager_v1::ManagerFeature::GatewayWorkerReflection as i32,
            crate::proto_types::manager_v1::ManagerFeature::OwnershipRecovery as i32,
            crate::proto_types::manager_v1::ManagerFeature::IssueAdminActions as i32,
            crate::proto_types::manager_v1::ManagerFeature::IssueOperationResolution as i32,
            crate::proto_types::manager_v1::ManagerFeature::OwnershipDiagnostics as i32,
        ]
    );
    assert_eq!(info.store_identities.len(), 2);
    assert!(
        info.method_keys
            .contains(&"manager_get_issue_with_sync".to_owned())
    );
    assert_eq!(
        info.method_keys.len(),
        crate::proto_schema::MANAGER_METHOD_SPECS.len()
    );
}

#[test]
fn maintenance_failure_keeps_contract_health_degraded_after_delivery_activation() {
    let (_, mut manager) = manager_for_test();
    manager.activate_outbox_driver();
    assert_eq!(
        manager.get_contract_info().health,
        crate::proto_types::manager_v1::HealthStatus::Healthy as i32
    );

    manager.set_ownership_maintenance_health(false).unwrap();
    let degraded = manager.get_contract_info();
    assert_eq!(
        degraded.health,
        crate::proto_types::manager_v1::HealthStatus::Degraded as i32
    );
    assert_eq!(
        degraded.activation,
        crate::proto_types::manager_v1::ActivationStatus::Blocked as i32
    );

    manager.set_ownership_maintenance_health(true).unwrap();
    assert_eq!(
        manager.get_contract_info().health,
        crate::proto_types::manager_v1::HealthStatus::Healthy as i32
    );
}

#[test]
fn maintenance_health_persists_initial_value_and_only_advances_on_value_transitions() {
    let (_, mut manager) = manager_for_test();
    let initial_generation = manager.storage().generation();
    let initial_sequence = manager.storage().manager_sequence();

    manager
        .set_ownership_maintenance_health(true)
        .expect("persist initial healthy state");
    assert_eq!(manager.storage().generation(), initial_generation + 1);
    assert_eq!(manager.storage().manager_sequence(), initial_sequence + 1);
    let persisted_after_initial = manager
        .storage()
        .read(|connection| {
            Ok(connection.query_row(
                "SELECT value FROM manager_metadata WHERE key='ownership_maintenance_healthy'",
                [],
                |row| row.get::<_, String>(0),
            )?)
        })
        .expect("read initially persisted maintenance health");
    assert_eq!(persisted_after_initial, "true");
    let updated_at_after_initial = manager
        .storage()
        .read(|connection| {
            Ok(connection.query_row(
                "SELECT value FROM manager_metadata WHERE key='ownership_maintenance_health_updated_at'",
                [],
                |row| row.get::<_, String>(0),
            )?)
        })
        .expect("read initial maintenance health update time");

    manager
        .set_ownership_maintenance_health(true)
        .expect("persist unchanged healthy state again");
    assert_eq!(manager.storage().generation(), initial_generation + 1);
    assert_eq!(manager.storage().manager_sequence(), initial_sequence + 1);
    let updated_at_after_noop = manager
        .storage()
        .read(|connection| {
            Ok(connection.query_row(
                "SELECT value FROM manager_metadata WHERE key='ownership_maintenance_health_updated_at'",
                [],
                |row| row.get::<_, String>(0),
            )?)
        })
        .expect("read unchanged maintenance health update time");
    assert_eq!(updated_at_after_noop, updated_at_after_initial);

    manager
        .set_ownership_maintenance_health(false)
        .expect("persist degraded state");
    assert_eq!(manager.storage().generation(), initial_generation + 2);
    assert_eq!(manager.storage().manager_sequence(), initial_sequence + 2);
    let persisted_after_transition = manager
        .storage()
        .read(|connection| {
            Ok(connection.query_row(
                "SELECT value FROM manager_metadata WHERE key='ownership_maintenance_healthy'",
                [],
                |row| row.get::<_, String>(0),
            )?)
        })
        .expect("read persisted degraded maintenance health");
    assert_eq!(persisted_after_transition, "false");

    manager
        .set_ownership_maintenance_health(false)
        .expect("persist unchanged degraded state");
    assert_eq!(manager.storage().generation(), initial_generation + 2);
    assert_eq!(manager.storage().manager_sequence(), initial_sequence + 2);
}

#[test]
fn ownership_maintenance_failure_blocks_issue_backend_admission() {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    let (_, mut manager) = manager_for_test_with_payload_limit();
    manager.activate_outbox_driver();
    manager
        .set_ownership_maintenance_health(false)
        .expect("persist maintenance failure");
    let identity = manager.config().manager_identity();

    let args = pb::UpdateIssueArgs {
        context: Some(pb::ManagerWriteContext {
            method: "manager_update_issue".into(),
            target: "issue:blocked-by-maintenance".into(),
            schema_fingerprint: identity.schema_fingerprint.clone(),
            idempotency_key: "blocked-write".into(),
            data_root_id: identity.data_root_id.clone(),
            contract_fingerprint: identity.contract_fingerprint.clone(),
            correlation_id: "blocked-write-correlation".into(),
            actor_audit: Some(pb::ActorAudit {
                actor_id: "worker".into(),
                actor_kind: crate::v1::ActorKind::InternalAgent as i32,
                ..Default::default()
            }),
        }),
        issue_id: "blocked-by-maintenance".into(),
        expected_revision: Some(0),
        update_mask: vec![pb::ManagerIssueField::Title as i32],
        patch: Some(pb::ManagerIssuePatch {
            title: Some("blocked write".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let response = manager
        .dispatch_manager("manager_update_issue", &args.encode_to_vec())
        .expect("typed backend admission response");
    let response = pb::UpdateIssueResult::decode(response.as_slice())
        .expect("decode backend admission response");
    assert!(matches!(
        response.outcome,
        Some(pb::update_issue_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::BackendUnavailable as i32
                && error.retryable
    ));

    manager
        .storage()
        .read(|connection| {
            let operations: i64 =
                connection.query_row("SELECT COUNT(*) FROM manager_operation", [], |row| {
                    row.get(0)
                })?;
            assert_eq!(
                operations, 0,
                "blocked admission must not reserve an operation"
            );
            Ok(())
        })
        .expect("inspect manager storage");
}

#[test]
fn ownership_maintenance_failure_blocks_lease_capability_admission() {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    let (_, mut manager) = manager_for_test();
    manager
        .set_ownership_maintenance_health(false)
        .expect("persist maintenance failure");
    let identity = manager.config().manager_identity();
    let args = pb::HeartbeatIssueClaimArgs {
        context: Some(pb::ManagerWriteContext {
            method: "manager_heartbeat_issue_claim".into(),
            target: "issue:blocked-capability".into(),
            schema_fingerprint: identity.schema_fingerprint.clone(),
            idempotency_key: "blocked-capability".into(),
            data_root_id: identity.data_root_id.clone(),
            contract_fingerprint: identity.contract_fingerprint.clone(),
            actor_audit: Some(pb::ActorAudit {
                actor_id: "worker".into(),
                actor_kind: crate::v1::ActorKind::InternalAgent as i32,
                ..Default::default()
            }),
            ..Default::default()
        }),
        issue_id: "blocked-capability".into(),
        expected_revision: Some(0),
        claim_revision: Some(0),
        lease_handle: b"ephemeral-handle".to_vec(),
        requested_extension: Some(pb::TimeSpan { millis: 1_000 }),
    };
    let preparation = manager
        .prepare_lease_capability("manager_heartbeat_issue_claim", &args.encode_to_vec(), 100)
        .expect("typed capability admission response");
    let response = match preparation {
        LeaseCapabilityPreparation::Immediate(response) => response,
        LeaseCapabilityPreparation::Dispatch(_) => {
            panic!("capability admission must be blocked")
        }
    };
    let response = pb::HeartbeatIssueClaimResult::decode(response.as_slice())
        .expect("decode capability admission response");
    assert!(matches!(
        response.outcome,
        Some(pb::heartbeat_issue_claim_result::Outcome::Error(error))
            if error.code == pb::ManagerErrorCode::BackendUnavailable as i32
                && error.retryable
    ));
}

#[test]
fn maintenance_health_is_restored_after_manager_reopen() {
    let (config, mut manager) = manager_for_test();
    manager.set_ownership_maintenance_health(false).unwrap();
    drop(manager);

    let mut reopened = IssueTrackerManager::open(config).unwrap();
    reopened.activate_outbox_driver();
    assert_eq!(
        reopened.get_contract_info().health,
        crate::proto_types::manager_v1::HealthStatus::Degraded as i32,
        "a restart must not clear a persisted maintenance failure"
    );
    reopened.set_ownership_maintenance_health(true).unwrap();
    assert_eq!(
        reopened.get_contract_info().health,
        crate::proto_types::manager_v1::HealthStatus::Healthy as i32
    );
}

#[test]
fn ownership_diagnosis_reference_is_cas_resolved_and_stale_repair_is_ignored() {
    let (_, mut manager) = manager_for_test();
    let target_id = "00000000-0000-0000-0000-000000000001";
    manager
        .storage_mut()
        .repository()
        .record_ownership_integrity_diagnosis(
            "issue",
            target_id,
            "INCONSISTENT",
            Some(4),
            Some("INCONSISTENT"),
            "clear the inconsistent lease through an administrative action",
            &["lb.lease_expires_at".into()],
            Some(123),
            &["blocker-b".into(), "blocker-a".into()],
        )
        .expect("record diagnosis");
    let diagnosis = manager
        .storage_mut()
        .repository()
        .list_ownership_integrity_diagnoses(Some("issue"), Some(target_id), 10, 0)
        .expect("list diagnosis")
        .into_iter()
        .next()
        .expect("diagnosis exists");
    assert_eq!(diagnosis.invalid_attributes, ["lb.lease_expires_at"]);
    assert_eq!(
        diagnosis
            .lease_expires_at
            .as_ref()
            .map(|value| value.unix_epoch_millis),
        Some(123_000)
    );
    assert_eq!(diagnosis.open_blocker_issue_ids, ["blocker-a", "blocker-b"]);
    let reference = crate::proto_types::manager_v1::OwnershipIntegrityDiagnosisReference {
        target_kind: diagnosis.target_kind.clone(),
        target_id: diagnosis.target_id.clone(),
        diagnosis_class: diagnosis.diagnosis_class.clone(),
        evidence_digest: diagnosis.evidence_digest.clone(),
        diagnosis_generation: diagnosis.diagnosis_generation,
        observation_token: diagnosis.observation_token,
        repair_epoch: diagnosis.repair_epoch,
    };
    assert!(
        manager
            .storage_mut()
            .repository()
            .ownership_integrity_diagnosis_matches(&reference)
            .expect("diagnosis matches")
    );
    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .resolve_ownership_integrity_diagnosis(&reference, "admin-repair-1")
            .expect("resolve diagnosis"),
        repository::OwnershipDiagnosisResolution::Resolved
    );
    assert!(
        !manager
            .storage_mut()
            .repository()
            .ownership_integrity_diagnosis_matches(&reference)
            .expect("resolved diagnosis is no longer open")
    );
    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .resolve_ownership_integrity_diagnosis(&reference, "old-repair")
            .expect("stale resolution is classified"),
        repository::OwnershipDiagnosisResolution::Stale
    );
}

#[test]
fn repaired_lease_stales_open_diagnosis_with_recovery_state_atomically() {
    let (_, mut manager) = manager_for_test();
    let target_id = "00000000-0000-0000-0000-000000000002";
    manager
        .storage_mut()
        .repository()
        .record_ownership_integrity_diagnosis(
            "issue",
            target_id,
            "LEASE_INCONSISTENT",
            Some(4),
            Some("INCONSISTENT"),
            "clear the inconsistent lease through an administrative action",
            &[],
            None,
            &[],
        )
        .expect("record diagnosis");
    let state = repository::OwnershipRecoveryState {
        issue_id: target_id.into(),
        expected_revision: 5,
        readmission_number: 1,
        current_operation_id: None,
        in_flight: false,
        consecutive_failures: 0,
        next_attempt_at_millis: None,
        last_failure_class: Some("LEASE_INCONSISTENT".into()),
        stopped: false,
        stop_basis: None,
        repair_condition: None,
        cursor_diagnostic: None,
    };
    manager
        .storage_mut()
        .repository()
        .stale_ownership_integrity_diagnosis_and_recovery_state(&state, "issue", target_id, 5)
        .expect("stale diagnosis and persist recovery state");
    let diagnosis = manager
        .storage_mut()
        .repository()
        .list_ownership_integrity_diagnoses(Some("issue"), Some(target_id), 10, 0)
        .expect("list diagnosis")
        .into_iter()
        .next()
        .expect("diagnosis exists");
    assert_eq!(diagnosis.state, "STALE");
    assert_eq!(diagnosis.observed_revision, Some(5));
    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .ownership_recovery_state(target_id)
            .expect("read recovery state")
            .expect("recovery state exists"),
        state
    );
}

#[test]
fn ownership_recovery_replays_committed_projection_and_clears_index() {
    let (_, mut manager) = manager_for_test();
    let issue_id = "ownership-committed";
    let operation_id = "ownership-committed-operation";
    let state = ownership_recovery_state(issue_id, 1, Some(operation_id), true);
    manager
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&state)
        .expect("set committed recovery state");
    seed_ownership_recovery_projection(
        &mut manager,
        issue_id,
        1,
        operation_id,
        4,
        4,
        crate::proto_types::manager_v1::DurableOperationState::Committed as i32,
    );

    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
            .expect("recover committed projection"),
        1
    );
    let recovered = manager
        .storage_mut()
        .repository()
        .ownership_recovery_state(issue_id)
        .expect("read committed recovery state")
        .expect("committed recovery state exists");
    assert!(!recovered.in_flight);
    assert_eq!(recovered.current_operation_id, None);
    assert_eq!(recovered.consecutive_failures, 0);
    assert_eq!(recovered.next_attempt_at_millis, None);
    assert!(!recovered.stopped);
    assert_eq!(recovered.last_failure_class, None);
    assert_eq!(recovered.stop_basis, None);
    assert_eq!(recovered.repair_condition, None);

    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
            .expect("replay committed projection"),
        0
    );
}

#[test]
fn ownership_recovery_replays_arbitrated_projection_and_clears_index() {
    let (_, mut manager) = manager_for_test();
    let issue_id = "ownership-arbitrated";
    let operation_id = "ownership-arbitrated-operation";
    let state = ownership_recovery_state(issue_id, 1, Some(operation_id), true);
    manager
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&state)
        .expect("set arbitrated recovery state");
    seed_ownership_recovery_projection(
        &mut manager,
        issue_id,
        1,
        operation_id,
        5,
        5,
        crate::proto_types::manager_v1::DurableOperationState::Arbitrated as i32,
    );

    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
            .expect("recover arbitrated projection"),
        1
    );
    let recovered = manager
        .storage_mut()
        .repository()
        .ownership_recovery_state(issue_id)
        .expect("read arbitrated recovery state")
        .expect("arbitrated recovery state exists");
    assert!(!recovered.in_flight);
    assert_eq!(recovered.current_operation_id, None);
    assert_eq!(recovered.consecutive_failures, 0);
    assert_eq!(recovered.next_attempt_at_millis, None);
    assert!(!recovered.stopped);
    assert_eq!(recovered.last_failure_class, None);
    assert_eq!(recovered.stop_basis, None);
    assert_eq!(recovered.repair_condition, None);

    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
            .expect("replay arbitrated projection"),
        0
    );
}

#[test]
fn ownership_recovery_missing_index_rolls_back_prior_projection_updates() {
    let (_, mut manager) = manager_for_test();
    let valid_issue_id = "001-valid-ownership";
    let valid_operation_id = "valid-ownership-operation";
    let valid_state = ownership_recovery_state(valid_issue_id, 1, Some(valid_operation_id), true);
    manager
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&valid_state)
        .expect("set valid recovery state");
    seed_ownership_recovery_projection(
        &mut manager,
        "002-missing-ownership",
        1,
        "missing-ownership-operation",
        6,
        6,
        crate::proto_types::manager_v1::DurableOperationState::Committed as i32,
    );
    seed_ownership_recovery_projection(
        &mut manager,
        valid_issue_id,
        1,
        valid_operation_id,
        7,
        7,
        crate::proto_types::manager_v1::DurableOperationState::Committed as i32,
    );

    let error = manager
        .storage_mut()
        .repository()
        .recover_ownership_recovery_projections()
        .expect_err("missing recovery index must fail recovery");
    assert!(matches!(
        error,
        repository::RepositoryError::OwnershipProjectionNotApplied {
            issue_id,
            operation_id,
            reason,
        } if issue_id == "002-missing-ownership"
            && operation_id == "missing-ownership-operation"
            && reason == "recovery index row is missing"
    ));

    let unchanged = manager
        .storage_mut()
        .repository()
        .ownership_recovery_state(valid_issue_id)
        .expect("read rolled back valid recovery state")
        .expect("valid recovery state exists");
    assert_eq!(unchanged, valid_state);
}

#[test]
fn ownership_recovery_skips_stale_readmission_projection() {
    let (_, mut manager) = manager_for_test();
    let issue_id = "ownership-stale-readmission";
    let operation_id = "ownership-stale-readmission-operation";
    let state = ownership_recovery_state(issue_id, 2, Some(operation_id), true);
    manager
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&state)
        .expect("set stale readmission recovery state");
    seed_ownership_recovery_projection(
        &mut manager,
        issue_id,
        1,
        operation_id,
        8,
        8,
        crate::proto_types::manager_v1::DurableOperationState::Committed as i32,
    );

    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
            .expect("skip stale readmission projection"),
        0
    );
    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .ownership_recovery_state(issue_id)
            .expect("read stale readmission recovery state")
            .expect("stale readmission recovery state exists"),
        state
    );
}

#[test]
fn ownership_recovery_skips_projection_with_newer_operation_generation() {
    let (_, mut manager) = manager_for_test();
    let issue_id = "ownership-newer-generation";
    let operation_id = "ownership-newer-generation-operation";
    let state = ownership_recovery_state(issue_id, 1, Some(operation_id), true);
    manager
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&state)
        .expect("set newer generation recovery state");
    seed_ownership_recovery_projection(
        &mut manager,
        issue_id,
        1,
        operation_id,
        10,
        9,
        crate::proto_types::manager_v1::DurableOperationState::Committed as i32,
    );

    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
            .expect("skip newer operation generation"),
        0
    );
    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .ownership_recovery_state(issue_id)
            .expect("read newer generation recovery state")
            .expect("newer generation recovery state exists"),
        state
    );
}

#[test]
fn ownership_recovery_skips_non_terminal_projection() {
    let (_, mut manager) = manager_for_test();
    let issue_id = "ownership-non-terminal";
    let operation_id = "ownership-non-terminal-operation";
    let state = ownership_recovery_state(issue_id, 1, Some(operation_id), true);
    manager
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&state)
        .expect("set non-terminal recovery state");
    seed_ownership_recovery_projection(
        &mut manager,
        issue_id,
        1,
        operation_id,
        11,
        11,
        crate::proto_types::manager_v1::DurableOperationState::Pending as i32,
    );

    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
            .expect("skip non-terminal projection"),
        0
    );
    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .ownership_recovery_state(issue_id)
            .expect("read non-terminal recovery state")
            .expect("non-terminal recovery state exists"),
        state
    );
}

#[test]
fn ownership_recovery_skips_already_resolved_index() {
    let (_, mut manager) = manager_for_test();
    let issue_id = "ownership-already-resolved";
    let operation_id = "ownership-already-resolved-operation";
    let state = ownership_recovery_state(issue_id, 1, Some(operation_id), false);
    manager
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(&state)
        .expect("set already resolved recovery state");
    seed_ownership_recovery_projection(
        &mut manager,
        issue_id,
        1,
        operation_id,
        12,
        12,
        crate::proto_types::manager_v1::DurableOperationState::Committed as i32,
    );

    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
            .expect("skip already resolved index"),
        0
    );
    assert_eq!(
        manager
            .storage_mut()
            .repository()
            .ownership_recovery_state(issue_id)
            .expect("read already resolved recovery state")
            .expect("already resolved recovery state exists"),
        state
    );
}

#[test]
fn phase_health_projection_round_trips_counters_and_epoch() {
    let (_, mut manager) = manager_for_test();
    let initial_generation = manager.storage().generation();
    let initial_sequence = manager.storage().manager_sequence();
    manager
        .storage_mut()
        .repository()
        .record_phase_health(
            "ownership_scan",
            crate::proto_types::manager_v1::HealthStatus::Degraded as i32,
            false,
            2,
            1,
            &["diag:one".into()],
        )
        .expect("record phase health");
    manager
        .storage_mut()
        .repository()
        .record_phase_health(
            "ownership_scan",
            crate::proto_types::manager_v1::HealthStatus::Healthy as i32,
            true,
            0,
            0,
            &[],
        )
        .expect("record recovery health");
    manager
        .storage_mut()
        .repository()
        .record_phase_health_with_counts(
            "ownership_scan",
            crate::proto_types::manager_v1::HealthStatus::Degraded as i32,
            false,
            1,
            1,
            &[],
            false,
            3,
            2,
            1,
            4,
            2,
            1,
        )
        .expect("record independent maintenance counters");
    let (epoch, phases) = manager
        .storage()
        .phase_health_snapshots()
        .expect("read phase health");
    assert_eq!(manager.storage().generation(), initial_generation);
    assert_eq!(manager.storage().manager_sequence(), initial_sequence);
    assert_eq!(epoch, 3);
    assert_eq!(phases.len(), 1);
    let phase = &phases[0];
    assert_eq!(phase.phase, "ownership_scan");
    assert_eq!(phase.attempt_count, 5);
    assert_eq!(phase.success_count, 3);
    assert_eq!(phase.skipped_count, 4);
    assert_eq!(phase.conflict_count, 2);
    assert_eq!(phase.diagnosis_count, 2);
    assert_eq!(phase.unconfirmed_count, 1);
    assert_eq!(phase.pending_gauge, 1);
    assert_eq!(phase.diagnosis_refs, Vec::<String>::new());
    assert!(phase.last_failure_at.is_some());
    assert!(phase.last_recovered_at.is_some());

    let contract_info = manager.get_contract_info();
    assert_eq!(contract_info.health_epoch, epoch);
    assert_eq!(contract_info.maintenance_phases.len(), 1);
    assert_eq!(
        contract_info.maintenance_phases[0].health,
        crate::proto_types::manager_v1::HealthStatus::Degraded as i32
    );
    assert_eq!(contract_info.maintenance_phases[0].attempt_count, 5);
    assert_eq!(contract_info.maintenance_phases[0].success_count, 3);
    assert_eq!(contract_info.maintenance_phases[0].skipped_count, 4);
}

#[test]
fn retry_and_reconcile_expired_claim_errors_keep_method_specific_envelopes() {
    let error = gateway_error(
        crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed,
        "backend delivery is unconfirmed",
        true,
    );

    let retry = manager_error_result("manager_retry_operation", error.clone());
    let retry = crate::proto_types::manager_v1::RetryOperationResult::decode(retry.as_slice())
        .expect("decode retry error");
    assert!(matches!(
        retry.outcome,
        Some(crate::proto_types::manager_v1::retry_operation_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed as i32
    ));

    let reconcile = manager_error_result("manager_reconcile_expired_claim", error);
    let reconcile =
        crate::proto_types::manager_v1::ReconcileExpiredClaimResult::decode(reconcile.as_slice())
            .expect("decode reconcile error");
    assert!(matches!(
        reconcile.outcome,
        Some(crate::proto_types::manager_v1::reconcile_expired_claim_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed as i32
    ));
}

#[test]
fn manager_gateway_routes_contract_info_to_the_active_manager_service() {
    let (config, mut manager) = manager_for_test();
    let args = crate::proto_types::manager_v1::GetContractInfoArgs {
        context: Some(crate::proto_types::manager_v1::ManagerReadContext {
            schema_fingerprint: config.expected_schema_fingerprint.clone(),
            data_root_id: config.manager_identity().data_root_id.clone(),
            contract_fingerprint: config.expected_contract_fingerprint.clone(),
            ..Default::default()
        }),
    };

    let bytes = manager
        .dispatch_manager("manager_get_contract_info", &args.encode_to_vec())
        .expect("contract info response");
    let result = crate::proto_types::manager_v1::GetContractInfoResult::decode(bytes.as_slice())
        .expect("decode contract info response");
    let info = match result.outcome.expect("contract info outcome") {
        crate::proto_types::manager_v1::get_contract_info_result::Outcome::Success(info) => info,
        crate::proto_types::manager_v1::get_contract_info_result::Outcome::Error(error) => {
            panic!("unexpected contract info error: {error:?}")
        }
    };
    assert_eq!(info.contract_version, MANAGER_CONTRACT_VERSION);
    assert_eq!(
        info.activation,
        crate::proto_types::manager_v1::ActivationStatus::Blocked as i32
    );
}

#[test]
fn gateway_control_uses_listener_identity_and_keeps_failures_typed() {
    let (config, mut manager) = manager_for_test();
    let args = crate::proto_types::manager_v1::RegisterGatewayArgs {
        context: Some(crate::proto_types::manager_v1::ManagerWriteContext {
            method: "manager_register_gateway".into(),
            target: "gateway-control".into(),
            schema_fingerprint: config.expected_schema_fingerprint.clone(),
            idempotency_key: "register-gateway".into(),
            data_root_id: config.manager_identity().data_root_id.clone(),
            contract_fingerprint: config.expected_contract_fingerprint.clone(),
            actor_audit: Some(crate::proto_types::manager_v1::ActorAudit {
                actor_id: "gateway-control".into(),
                actor_kind: crate::v1::ActorKind::InternalAgent as i32,
                ..Default::default()
            }),
            ..Default::default()
        }),
        policy: Some(crate::proto_types::manager_v1::WorkEligibilityPolicy {
            mode: crate::proto_types::manager_v1::WorkEligibilityMode::ReviewedOnly as i32,
        }),
    };

    let bytes = manager
        .dispatch_manager_for_gateway(
            "configured-gateway",
            "manager_register_gateway",
            &args.encode_to_vec(),
        )
        .expect("typed registration response");
    let result = crate::proto_types::manager_v1::RegisterGatewayResult::decode(bytes.as_slice())
        .expect("decode registration response");
    assert!(matches!(
        result.outcome,
        Some(crate::proto_types::manager_v1::register_gateway_result::Outcome::Success(registration))
            if registration.gateway_id == "configured-gateway"
    ));
    let replay = manager
        .dispatch_manager_for_gateway(
            "configured-gateway",
            "manager_register_gateway",
            &args.encode_to_vec(),
        )
        .expect("typed replay response");
    let replay = crate::proto_types::manager_v1::RegisterGatewayResult::decode(replay.as_slice())
        .expect("decode registration replay");
    assert!(matches!(
        replay.outcome,
        Some(crate::proto_types::manager_v1::register_gateway_result::Outcome::Success(registration))
            if registration.registration_generation == 1
    ));

    let mut conflicting = args;
    conflicting.policy = Some(crate::proto_types::manager_v1::WorkEligibilityPolicy {
        mode: crate::proto_types::manager_v1::WorkEligibilityMode::IncludeAutoRegistered as i32,
    });
    let conflict = manager
        .dispatch_manager_for_gateway(
            "configured-gateway",
            "manager_register_gateway",
            &conflicting.encode_to_vec(),
        )
        .expect("typed conflict response");
    let conflict =
        crate::proto_types::manager_v1::RegisterGatewayResult::decode(conflict.as_slice())
            .expect("decode registration conflict");
    assert!(matches!(
        conflict.outcome,
        Some(crate::proto_types::manager_v1::register_gateway_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::IdempotencyConflict as i32
    ));
}

#[tokio::test]
async fn manager_gateway_routes_canonical_reads_through_async_composition() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let issue = tracker
        .create("gateway-canonical-read", "gateway canonical issue")
        .await
        .expect("create canonical issue");
    let (config, mut manager) = manager_for_test();
    let args = crate::proto_types::manager_v1::GetIssueWithSyncArgs {
        context: Some(crate::proto_types::manager_v1::ManagerReadContext {
            schema_fingerprint: config.expected_schema_fingerprint.clone(),
            data_root_id: config.manager_identity().data_root_id.clone(),
            contract_fingerprint: config.expected_contract_fingerprint.clone(),
            ..Default::default()
        }),
        issue_id: issue.id.to_string(),
    };

    let bytes = manager
        .dispatch_manager_with_tracker(
            &tracker,
            "manager_get_issue_with_sync",
            &args.encode_to_vec(),
        )
        .await
        .expect("canonical response");
    let result = crate::proto_types::manager_v1::GetIssueWithSyncResult::decode(bytes.as_slice())
        .expect("decode canonical response");
    match result.outcome.expect("canonical outcome") {
        crate::proto_types::manager_v1::get_issue_with_sync_result::Outcome::Success(view) => {
            assert_eq!(view.issue.expect("issue").title, "gateway canonical issue");
        }
        crate::proto_types::manager_v1::get_issue_with_sync_result::Outcome::Error(error) => {
            panic!("unexpected canonical read error: {error:?}")
        }
    }

    let list_args = crate::proto_types::manager_v1::ListIssuesWithSyncArgs {
        context: Some(crate::proto_types::manager_v1::ManagerReadContext {
            schema_fingerprint: config.expected_schema_fingerprint.clone(),
            data_root_id: config.manager_identity().data_root_id.clone(),
            contract_fingerprint: config.expected_contract_fingerprint.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let bytes = manager
        .dispatch_manager_with_tracker(
            &tracker,
            "manager_list_issues_with_sync",
            &list_args.encode_to_vec(),
        )
        .await
        .expect("ready list response");
    let result = crate::proto_types::manager_v1::ListIssuesWithSyncResult::decode(bytes.as_slice())
        .expect("decode ready list response");
    match result.outcome.expect("ready list outcome") {
        crate::proto_types::manager_v1::list_issues_with_sync_result::Outcome::Success(page) => {
            assert_eq!(page.issues.len(), 1)
        }
        crate::proto_types::manager_v1::list_issues_with_sync_result::Outcome::Error(error) => {
            panic!("unexpected ready list error: {error:?}")
        }
    }
}

#[tokio::test]
async fn review_candidate_read_model_lists_only_actionable_candidate_statuses() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let (config, mut manager) = manager_for_test_with_payload_limit();
    let candidate = crate::proto_types::manager_v1::Candidate {
        candidate_id: "candidate-review-proposed".into(),
        title: "review me".into(),
        status: crate::proto_types::manager_v1::CandidateStatus::Proposed as i32,
        kind: crate::proto_types::manager_v1::CandidateKind::Bug as i32,
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::Candidate,
            &candidate.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate,
        )
        .expect("store candidate");
    let args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: Some(crate::proto_types::manager_v1::ManagerReadContext {
            schema_fingerprint: config.expected_schema_fingerprint.clone(),
            data_root_id: config.manager_identity().data_root_id.clone(),
            contract_fingerprint: config.expected_contract_fingerprint.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let bytes = manager
        .dispatch_manager_with_tracker(
            &tracker,
            "manager_list_review_candidates",
            &args.encode_to_vec(),
        )
        .await
        .expect("review candidate response");
    let result =
        crate::proto_types::manager_v1::ListReviewCandidatesResult::decode(bytes.as_slice())
            .expect("decode review candidate response");
    match result.outcome.expect("review candidate outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            assert_eq!(page.rows.len(), 1);
            assert_eq!(
                page.rows[0]
                    .candidate
                    .as_ref()
                    .expect("candidate")
                    .candidate_id,
                "candidate-review-proposed"
            );
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected review candidate read error: {error:?}")
        }
    }
}

#[tokio::test]
async fn review_candidate_first_page_keeps_the_read_snapshot_after_a_manager_write() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let (config, mut manager) = manager_for_test_with_payload_limit();
    let candidate = crate::proto_types::manager_v1::Candidate {
        candidate_id: "candidate-review-boundary".into(),
        title: "review me".into(),
        status: crate::proto_types::manager_v1::CandidateStatus::Proposed as i32,
        kind: crate::proto_types::manager_v1::CandidateKind::Bug as i32,
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::Candidate,
            &candidate.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate,
        )
        .expect("store candidate");
    let args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: Some(crate::proto_types::manager_v1::ManagerReadContext {
            schema_fingerprint: config.expected_schema_fingerprint.clone(),
            data_root_id: config.manager_identity().data_root_id.clone(),
            contract_fingerprint: config.expected_contract_fingerprint.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let prepared = manager
        .prepare_review_candidate_read(&args.encode_to_vec())
        .expect("prepare review candidate read");
    let manager_generation = prepared.snapshot.generation;
    manager
        .set_ownership_maintenance_health(false)
        .expect("advance manager generation");
    let backend_generation = tracker.current_read_generation();
    let response = dispatch_review_candidate_read(&tracker, prepared).await;
    let result =
        crate::proto_types::manager_v1::ListReviewCandidatesResult::decode(response.as_slice())
            .expect("decode review candidate response");
    let page = match result.outcome.expect("review candidate outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            page
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected review candidate read error: {error:?}")
        }
    };
    assert_eq!(page.rows.len(), 1);
    let composite_snapshot = page
        .composite_snapshot
        .as_ref()
        .expect("composite snapshot");
    assert_eq!(
        composite_snapshot
            .manager_snapshot
            .as_ref()
            .map(|snapshot| snapshot.read_generation),
        Some(manager_generation)
    );
    assert_eq!(
        composite_snapshot
            .backend_snapshot
            .as_ref()
            .map(|snapshot| snapshot.read_generation),
        Some(backend_generation)
    );
}

#[tokio::test]
async fn review_candidate_continuation_survives_a_manager_boundary_change() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let (config, mut manager) = manager_for_test_with_payload_limit();
    for candidate_id in ["candidate-review-page-a", "candidate-review-page-b"] {
        let candidate = crate::proto_types::manager_v1::Candidate {
            candidate_id: candidate_id.into(),
            status: crate::proto_types::manager_v1::CandidateStatus::Proposed as i32,
            kind: crate::proto_types::manager_v1::CandidateKind::Bug as i32,
            ..Default::default()
        };
        manager
            .storage_mut()
            .repository()
            .put_proto(
                crate::manager::repository::EntityKind::Candidate,
                &candidate.candidate_id,
                0,
                "issue_tracker.manager.v1.Candidate",
                &candidate,
            )
            .expect("store candidate");
    }
    let first_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: Some(crate::proto_types::manager_v1::ManagerReadContext {
            schema_fingerprint: config.expected_schema_fingerprint.clone(),
            data_root_id: config.manager_identity().data_root_id.clone(),
            contract_fingerprint: config.expected_contract_fingerprint.clone(),
            ..Default::default()
        }),
        page: Some(crate::proto_types::manager_v1::PageRequest {
            page_size: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let first_prepared = manager
        .prepare_review_candidate_read(&first_args.encode_to_vec())
        .expect("prepare first review candidate page");
    let first = crate::proto_types::manager_v1::ListReviewCandidatesResult::decode(
        dispatch_review_candidate_read(&tracker, first_prepared)
            .await
            .as_slice(),
    )
    .expect("decode first review candidate page");
    let token = match first.outcome.expect("first page outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            page.page
                .and_then(|page| page.next_page_token)
                .expect("continuation token")
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected first page error: {error:?}")
        }
    };

    let changed_query_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: first_args.context.clone(),
        filter: Some(crate::proto_types::manager_v1::ReviewCandidateFilter {
            text_query: Some("different-query".into()),
            ..Default::default()
        }),
        page: Some(crate::proto_types::manager_v1::PageRequest {
            page_size: 1,
            page_token: Some(token.clone()),
        }),
    };
    let changed_query_prepared = manager
        .prepare_review_candidate_read(&changed_query_args.encode_to_vec())
        .expect("prepare changed-query review candidate page");
    let changed_query = crate::proto_types::manager_v1::ListReviewCandidatesResult::decode(
        dispatch_review_candidate_read(&tracker, changed_query_prepared)
            .await
            .as_slice(),
    )
    .expect("decode changed-query review candidate page");
    assert!(matches!(
        changed_query.outcome,
        Some(
            crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error)
        ) if error.code
            == crate::proto_types::manager_v1::ManagerErrorCode::PageTokenExpired as i32
    ));

    manager
        .set_ownership_maintenance_health(false)
        .expect("advance manager generation");
    let second_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: first_args.context,
        page: Some(crate::proto_types::manager_v1::PageRequest {
            page_size: 1,
            page_token: Some(token),
        }),
        ..Default::default()
    };
    let second_prepared = manager
        .prepare_review_candidate_read(&second_args.encode_to_vec())
        .expect("prepare second review candidate page");
    let second = crate::proto_types::manager_v1::ListReviewCandidatesResult::decode(
        dispatch_review_candidate_read(&tracker, second_prepared)
            .await
            .as_slice(),
    )
    .expect("decode second review candidate page");
    match second.outcome.expect("second page outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            assert_eq!(page.rows.len(), 1);
            assert!(page.rows.iter().any(|row| {
                row.candidate
                    .as_ref()
                    .is_some_and(|candidate| candidate.candidate_id == "candidate-review-page-b")
            }));
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected second page error: {error:?}")
        }
    }
}

#[tokio::test]
async fn review_candidate_continuation_uses_the_pinned_associated_issue_snapshot() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let (config, mut manager) = manager_for_test_with_payload_limit();
    let proposed = crate::proto_types::manager_v1::Candidate {
        candidate_id: "candidate-review-content-a".into(),
        status: crate::proto_types::manager_v1::CandidateStatus::Proposed as i32,
        kind: crate::proto_types::manager_v1::CandidateKind::Bug as i32,
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::Candidate,
            &proposed.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            &proposed,
        )
        .expect("store proposed candidate");
    let issue = tracker
        .create("pinned issue", "initial description")
        .await
        .expect("create associated issue");
    let auto_registered = crate::proto_types::manager_v1::Candidate {
        candidate_id: "candidate-review-content-b".into(),
        status: crate::proto_types::manager_v1::CandidateStatus::AutoRegistered as i32,
        kind: crate::proto_types::manager_v1::CandidateKind::Bug as i32,
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::Candidate,
            &auto_registered.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            &auto_registered,
        )
        .expect("store auto-registered candidate");
    let mapping = crate::proto_types::manager_v1::IssueMapping {
        issue_id: issue.id.to_string(),
        candidate_id: Some(auto_registered.candidate_id.clone()),
        backend_id: issue.id.to_string(),
        revision: issue.revision,
        backend_contract_fingerprint: config.taskchampion_identity().contract_fingerprint.clone(),
        manager_contract_fingerprint: config.manager_identity().contract_fingerprint.clone(),
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            &mapping.issue_id,
            0,
            "issue_tracker.manager.v1.IssueMapping",
            &mapping,
        )
        .expect("store associated issue mapping");

    let manager = Arc::new(tokio::sync::Mutex::new(manager));
    let first_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: Some(review_context(&config, "review-content")),
        page: Some(crate::proto_types::manager_v1::PageRequest {
            page_size: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let first = dispatch_review_page(&manager, &tracker, &first_args).await;
    let token = match first.outcome.expect("first page outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            page.page
                .and_then(|page| page.next_page_token)
                .expect("continuation token")
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected first page error: {error:?}")
        }
    };
    tracker
        .update_title(
            issue.id,
            "changed after page one",
            issue.revision,
            "content-update",
        )
        .await
        .expect("update associated issue");

    let second = dispatch_review_page(
        &manager,
        &tracker,
        &crate::proto_types::manager_v1::ListReviewCandidatesArgs {
            context: first_args.context,
            page: Some(crate::proto_types::manager_v1::PageRequest {
                page_size: 1,
                page_token: Some(token),
            }),
            ..Default::default()
        },
    )
    .await;
    match second.outcome.expect("continuation outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            assert_eq!(
                page.rows[0]
                    .associated_issue
                    .as_ref()
                    .expect("associated issue summary")
                    .revision,
                issue.revision
            );
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected continuation error: {error:?}")
        }
    }
}

#[tokio::test]
async fn review_candidate_pagination_survives_manager_and_tracker_churn() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let (config, mut manager) = manager_for_test_with_payload_limit();
    let mut seeded_ids = Vec::new();
    for index in 0..8 {
        let candidate_id = format!("candidate-review-churn-{index:02}");
        let auto_registered = index % 2 == 1;
        let candidate = crate::proto_types::manager_v1::Candidate {
            candidate_id: candidate_id.clone(),
            title: format!("review candidate {index}"),
            status: if auto_registered {
                crate::proto_types::manager_v1::CandidateStatus::AutoRegistered as i32
            } else {
                crate::proto_types::manager_v1::CandidateStatus::Proposed as i32
            },
            kind: crate::proto_types::manager_v1::CandidateKind::Bug as i32,
            ..Default::default()
        };
        manager
            .storage_mut()
            .repository()
            .put_proto(
                crate::manager::repository::EntityKind::Candidate,
                &candidate_id,
                0,
                "issue_tracker.manager.v1.Candidate",
                &candidate,
            )
            .expect("store candidate");
        if auto_registered {
            let issue = tracker
                .create(&format!("review issue {index}"), "mapped review issue")
                .await
                .expect("create mapped issue");
            let mapping = crate::proto_types::manager_v1::IssueMapping {
                issue_id: issue.id.to_string(),
                candidate_id: Some(candidate_id.clone()),
                backend_id: issue.id.to_string(),
                revision: issue.revision,
                backend_contract_fingerprint: config
                    .taskchampion_identity()
                    .contract_fingerprint
                    .clone(),
                manager_contract_fingerprint: config
                    .manager_identity()
                    .contract_fingerprint
                    .clone(),
                ..Default::default()
            };
            manager
                .storage_mut()
                .repository()
                .put_proto(
                    crate::manager::repository::EntityKind::IssueMapping,
                    &mapping.issue_id,
                    0,
                    "issue_tracker.manager.v1.IssueMapping",
                    &mapping,
                )
                .expect("store candidate mapping");
        }
        seeded_ids.push(candidate_id);
    }

    let manager = Arc::new(tokio::sync::Mutex::new(manager));
    let first_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: Some(review_context(&config, "review-churn")),
        page: Some(crate::proto_types::manager_v1::PageRequest {
            page_size: 2,
            ..Default::default()
        }),
        ..Default::default()
    };
    let first = dispatch_review_page(&manager, &tracker, &first_args).await;
    let mut collected_ids = Vec::new();
    let mut next_token = match first.outcome.expect("first page outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            collected_ids.extend(page.rows.iter().filter_map(|row| {
                row.candidate
                    .as_ref()
                    .map(|candidate| candidate.candidate_id.clone())
            }));
            page.page.and_then(|page| page.next_page_token)
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected first page error: {error:?}")
        }
    };

    let stop = Arc::new(AtomicBool::new(false));
    let churn_stop = Arc::clone(&stop);
    let churn_manager = Arc::clone(&manager);
    let churn_tracker = tracker.clone();
    let churn = tokio::spawn(async move {
        let mut healthy = true;
        while !churn_stop.load(Ordering::Acquire) {
            churn_manager
                .lock()
                .await
                .set_ownership_maintenance_health(!healthy)
                .expect("tracked maintenance write");
            churn_tracker.advance_read_generation();
            healthy = !healthy;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    let mut final_page_had_no_token = false;
    while let Some(token) = next_token {
        tokio::time::sleep(Duration::from_millis(8)).await;
        let args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
            context: first_args.context.clone(),
            page: Some(crate::proto_types::manager_v1::PageRequest {
                page_size: 2,
                page_token: Some(token),
            }),
            ..Default::default()
        };
        let result = dispatch_review_page(&manager, &tracker, &args).await;
        match result.outcome.expect("continuation outcome") {
            crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(
                page,
            ) => {
                collected_ids.extend(page.rows.iter().filter_map(|row| {
                    row.candidate
                        .as_ref()
                        .map(|candidate| candidate.candidate_id.clone())
                }));
                next_token = page.page.and_then(|page| page.next_page_token);
                final_page_had_no_token = next_token.is_none();
            }
            crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(
                error,
            ) => {
                panic!("unexpected churn continuation error: {error:?}")
            }
        }
    }
    stop.store(true, Ordering::Release);
    churn.await.expect("join churn task");

    assert!(
        final_page_had_no_token,
        "last page must not have a continuation"
    );
    assert_eq!(collected_ids, seeded_ids);
}

#[tokio::test]
async fn review_candidate_pin_miss_returns_expired_and_page_one_retry_recovers() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let (config, mut manager) = manager_for_test_with_payload_limit();
    for candidate_id in ["candidate-review-pin-a", "candidate-review-pin-b"] {
        let candidate = crate::proto_types::manager_v1::Candidate {
            candidate_id: candidate_id.into(),
            status: crate::proto_types::manager_v1::CandidateStatus::Proposed as i32,
            kind: crate::proto_types::manager_v1::CandidateKind::Bug as i32,
            ..Default::default()
        };
        manager
            .storage_mut()
            .repository()
            .put_proto(
                crate::manager::repository::EntityKind::Candidate,
                &candidate.candidate_id,
                0,
                "issue_tracker.manager.v1.Candidate",
                &candidate,
            )
            .expect("store candidate");
    }
    let manager = Arc::new(tokio::sync::Mutex::new(manager));
    let first_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: Some(review_context(&config, "pin-miss-original")),
        page: Some(crate::proto_types::manager_v1::PageRequest {
            page_size: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let first = dispatch_review_page(&manager, &tracker, &first_args).await;
    let original_token = match first.outcome.expect("first page outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            page.page
                .and_then(|page| page.next_page_token)
                .expect("original continuation token")
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected first page error: {error:?}")
        }
    };

    for index in 0..read_session::MAX_PINNED_READ_SESSIONS {
        let args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
            context: Some(review_context(
                &config,
                &format!("pin-miss-eviction-{index}"),
            )),
            page: Some(crate::proto_types::manager_v1::PageRequest {
                page_size: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let result = dispatch_review_page(&manager, &tracker, &args).await;
        assert!(matches!(
            result.outcome,
            Some(
                crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(_)
            )
        ));
    }

    let continuation_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: first_args.context.clone(),
        page: Some(crate::proto_types::manager_v1::PageRequest {
            page_size: 1,
            page_token: Some(original_token),
        }),
        ..Default::default()
    };
    let expired = dispatch_review_page(&manager, &tracker, &continuation_args).await;
    match expired.outcome.expect("pin miss outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            assert_eq!(
                error.code,
                crate::proto_types::manager_v1::ManagerErrorCode::PageTokenExpired as i32
            );
            assert!(!error.retryable);
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            panic!("unexpected pin-miss success: {page:?}")
        }
    }

    let retry_first = dispatch_review_page(&manager, &tracker, &first_args).await;
    let retry_token = match retry_first.outcome.expect("retry first page outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            page.page.and_then(|page| page.next_page_token)
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected retry first page error: {error:?}")
        }
    };
    let retry_second = dispatch_review_page(
        &manager,
        &tracker,
        &crate::proto_types::manager_v1::ListReviewCandidatesArgs {
            context: first_args.context,
            page: Some(crate::proto_types::manager_v1::PageRequest {
                page_size: 1,
                page_token: retry_token,
            }),
            ..Default::default()
        },
    )
    .await;
    match retry_second.outcome.expect("retry continuation outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            assert!(
                page.page
                    .expect("retry page info")
                    .next_page_token
                    .is_none()
            );
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected retry continuation error: {error:?}")
        }
    }
}

#[tokio::test]
async fn review_candidate_read_rejects_payloads_over_manager_limit_before_decode() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let (_config, mut manager) = manager_for_test_with_payload_limit();
    let oversized_payload = vec![0_u8; 64 * 1024 + 1];
    let bytes = manager
        .dispatch_manager_with_tracker(
            &tracker,
            "manager_list_review_candidates",
            &oversized_payload,
        )
        .await
        .expect("review candidate limit response");
    let result =
        crate::proto_types::manager_v1::ListReviewCandidatesResult::decode(bytes.as_slice())
            .expect("decode review candidate limit response");
    match result.outcome.expect("review candidate outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            assert_eq!(
                error.code,
                crate::proto_types::manager_v1::ManagerErrorCode::ResourceLimitExceeded as i32
            );
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            panic!("unexpected successful review candidate response: {page:?}")
        }
    }
}

#[tokio::test]
async fn review_candidate_read_model_excludes_terminal_auto_registered_issue() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let issue = tracker
        .create("terminal review issue", "already done")
        .await
        .expect("create issue");
    let report = crate::proto_types::v1::transition_issue_args::TerminalReport::Completion(
        crate::proto_types::v1::CompletionReport {
            result: Some(crate::proto_types::v1::completion_report::Result::Verified(
                crate::proto_types::v1::VerifiedCompletion {
                    evidence: vec![crate::proto_types::v1::CompletionEvidence {
                        kind: crate::proto_types::v1::CompletionEvidenceKind::OtherEvidence as i32,
                        summary: "test completion".into(),
                        uri: None,
                    }],
                },
            )),
        },
    );
    tracker
        .admin_terminate(
            issue.id,
            crate::model::IssueState::Done,
            issue.revision,
            "review-terminal",
            "test",
            "test completion",
            &report,
            false,
            false,
        )
        .await
        .expect("complete issue");
    let (config, mut manager) = manager_for_test_with_payload_limit();
    let candidate = crate::proto_types::manager_v1::Candidate {
        candidate_id: "candidate-review-auto".into(),
        status: crate::proto_types::manager_v1::CandidateStatus::AutoRegistered as i32,
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::Candidate,
            &candidate.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate,
        )
        .expect("store candidate");
    let mapping = crate::proto_types::manager_v1::IssueMapping {
        issue_id: issue.id.to_string(),
        candidate_id: Some(candidate.candidate_id.clone()),
        backend_id: issue.id.to_string(),
        backend_contract_fingerprint: config.taskchampion_identity().contract_fingerprint.clone(),
        manager_contract_fingerprint: config.expected_contract_fingerprint.clone(),
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            &mapping.issue_id,
            0,
            "issue_tracker.manager.v1.IssueMapping",
            &mapping,
        )
        .expect("store mapping");
    let pending_operation = crate::proto_types::manager_v1::DurableOperation {
        operation_id: "review-terminal-pending-resolution".into(),
        method: "manager_resolve_candidate".into(),
        idempotency_scope: "review-terminal-pending-resolution".into(),
        request_digest: "review-terminal-pending-resolution".into(),
        state: crate::proto_types::manager_v1::DurableOperationState::Pending as i32,
        scope: Some(
            crate::proto_types::manager_v1::durable_operation::Scope::CandidateId(
                candidate.candidate_id.clone(),
            ),
        ),
        ..Default::default()
    };
    let pending_payload = pending_operation.encode_to_vec();
    let pending_digest = crate::manager::operations::digest_bytes(&pending_payload);
    manager
        .storage_mut()
        .write(|transaction, stamp| {
            transaction.execute(
                "INSERT INTO manager_operation
                    (operation_id, method, idempotency_scope, request_digest,
                     state, attempt, manager_sequence, generation, affects_ready,
                     created_at, updated_at, operation_payload,
                     operation_payload_schema, operation_payload_digest)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, 0, 1, 1, ?8, ?9, ?10)",
                params![
                    pending_operation.operation_id,
                    pending_operation.method,
                    pending_operation.idempotency_scope,
                    pending_operation.request_digest,
                    pending_operation.state,
                    i64::try_from(stamp.manager_sequence).expect("sequence fits SQLite"),
                    i64::try_from(stamp.generation).expect("generation fits SQLite"),
                    pending_payload,
                    "issue_tracker.manager.v1.DurableOperation",
                    pending_digest,
                ],
            )?;
            transaction.execute(
                "INSERT INTO manager_operation_scope (operation_id, candidate_id)
                 VALUES (?1, ?2)",
                params!["review-terminal-pending-resolution", candidate.candidate_id],
            )?;
            Ok(())
        })
        .expect("store pending resolution operation");
    let args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: Some(crate::proto_types::manager_v1::ManagerReadContext {
            schema_fingerprint: config.expected_schema_fingerprint.clone(),
            data_root_id: config.manager_identity().data_root_id.clone(),
            contract_fingerprint: config.expected_contract_fingerprint.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let bytes = manager
        .dispatch_manager_with_tracker(
            &tracker,
            "manager_list_review_candidates",
            &args.encode_to_vec(),
        )
        .await
        .expect("review candidate response");
    let result =
        crate::proto_types::manager_v1::ListReviewCandidatesResult::decode(bytes.as_slice())
            .expect("decode review candidate response");
    match result.outcome.expect("review candidate outcome") {
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Success(page) => {
            assert!(page.rows.is_empty(), "unexpected rows: {:?}", page.rows);
        }
        crate::proto_types::manager_v1::list_review_candidates_result::Outcome::Error(error) => {
            panic!("unexpected review candidate read error: {error:?}")
        }
    }
}

#[tokio::test]
async fn review_candidate_list_classifies_present_and_missing_associated_issues() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let present = tracker
        .create("review-associated-present", "present issue")
        .await
        .expect("create issue");
    let missing_issue_id = uuid::Uuid::new_v4();
    let (config, mut manager) = manager_for_test_with_payload_limit();
    store_auto_registered_candidate_with_mapping(
        &mut manager,
        &config,
        "candidate-review-present",
        present.id,
        present.revision,
    );
    store_auto_registered_candidate_with_mapping(
        &mut manager,
        &config,
        "candidate-review-missing",
        missing_issue_id,
        0,
    );
    let manager = Arc::new(tokio::sync::Mutex::new(manager));
    let rows = collect_review_rows(
        &manager,
        &tracker,
        review_context(&config, "review-associated"),
    )
    .await;
    let row = |candidate_id: &str| {
        rows.iter()
            .find(|row| {
                row.candidate
                    .as_ref()
                    .is_some_and(|candidate| candidate.candidate_id == candidate_id)
            })
            .unwrap_or_else(|| panic!("missing row for {candidate_id}: {rows:?}"))
    };
    let present_row = row("candidate-review-present");
    assert_eq!(
        present_row.integrity,
        crate::proto_types::manager_v1::ReviewCandidateIntegrity::Normal as i32
    );
    assert_eq!(
        present_row
            .associated_issue
            .as_ref()
            .expect("associated issue")
            .issue_id,
        present.id.to_string()
    );
    let missing_row = row("candidate-review-missing");
    assert_eq!(
        missing_row.integrity,
        crate::proto_types::manager_v1::ReviewCandidateIntegrity::IssueNotFound as i32
    );
    assert!(missing_row.associated_issue.is_none());
}

#[tokio::test]
async fn review_candidate_continuation_is_served_from_the_pin_without_rereading_candidates() {
    use crate::proto_types::manager_v1::list_review_candidates_result::Outcome;
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let (config, mut manager) = manager_for_test_with_payload_limit();
    for candidate_id in ["candidate-pin-only-a", "candidate-pin-only-b"] {
        store_candidate(
            &mut manager,
            &crate::proto_types::manager_v1::Candidate {
                candidate_id: candidate_id.into(),
                status: crate::proto_types::manager_v1::CandidateStatus::Proposed as i32,
                ..Default::default()
            },
        );
    }
    let manager = Arc::new(tokio::sync::Mutex::new(manager));
    let first_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: Some(review_context(&config, "review-pin-only")),
        ..Default::default()
    };
    let token = match dispatch_review_page(&manager, &tracker, &first_args)
        .await
        .outcome
        .expect("first page outcome")
    {
        Outcome::Success(page) => page
            .page
            .and_then(|page| page.next_page_token)
            .expect("continuation token"),
        Outcome::Error(error) => panic!("unexpected first page error: {error:?}"),
    };
    // A corrupt stored candidate would fail a fresh snapshot; the pinned
    // continuation must not depend on re-reading candidate rows.
    manager
        .lock()
        .await
        .storage_mut()
        .write(|transaction, _stamp| {
            transaction.execute(
                "UPDATE manager_entity SET payload_digest = 'corrupt' WHERE entity_kind = 'candidate'",
                [],
            )?;
            Ok(())
        })
        .expect("corrupt candidate digests");

    let next_args = crate::proto_types::manager_v1::ListReviewCandidatesArgs {
        context: first_args.context.clone(),
        page: Some(crate::proto_types::manager_v1::PageRequest {
            page_token: Some(token),
            ..Default::default()
        }),
        ..Default::default()
    };
    match dispatch_review_page(&manager, &tracker, &next_args)
        .await
        .outcome
        .expect("continuation outcome")
    {
        Outcome::Success(page) => {
            let ids = page
                .rows
                .iter()
                .filter_map(|row| row.candidate.as_ref())
                .map(|candidate| candidate.candidate_id.as_str())
                .collect::<Vec<_>>();
            assert_eq!(ids, vec!["candidate-pin-only-b"]);
        }
        Outcome::Error(error) => panic!("unexpected continuation error: {error:?}"),
    }
}

#[tokio::test]
async fn internal_canonical_read_composes_tracker_issue_without_a_shadow_copy() {
    let tracker_root = tempdir().expect("tracker root");
    let tracker = crate::tracker::Tracker::open(tracker_root.path())
        .await
        .expect("open tracker");
    let issue = tracker
        .create("canonical-service-read", "canonical tracker issue")
        .await
        .expect("create canonical issue");
    let (_config, manager) = manager_for_test();

    let result = manager
        .get_issue_with_sync(&tracker, &issue.id.to_string())
        .await
        .expect("canonical read");
    let ready = manager
        .list_issues_with_sync(
            &tracker,
            &crate::proto_types::manager_v1::ListIssuesWithSyncArgs::default(),
        )
        .await
        .expect("canonical ready list");

    assert_eq!(
        result.issue.expect("issue").title,
        "canonical tracker issue"
    );
    assert!(result.lookback_ready);
    assert_eq!(ready.issues.len(), 1);
    manager
        .storage()
        .read(|connection| {
            let issue_entities: i64 = connection.query_row(
                "SELECT COUNT(*) FROM manager_entity WHERE entity_kind = 'issue'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(issue_entities, 0);
            Ok(())
        })
        .expect("manager store has no issue shadow");
}

// Candidate resolution replaced the legacy acceptance operation. The
// following test is retained only until the repository-level resolution
// saga tests take over its historical migration coverage.
#[test]
fn resolve_reject_uses_a_durable_receipt_and_enforces_cas_and_reason_category() {
    let (_config, mut manager) = manager_for_test_with_payload_limit();
    let candidate = crate::proto_types::manager_v1::Candidate {
        candidate_id: "candidate-resolve-reject".into(),
        status: crate::proto_types::manager_v1::CandidateStatus::Proposed as i32,
        revision: 0,
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .put_proto(
            repository::EntityKind::Candidate,
            &candidate.candidate_id,
            candidate.revision,
            "issue_tracker.manager.v1.Candidate",
            &candidate,
        )
        .expect("seed candidate");

    let request = |expected_revision, reason_category, idempotency_key: &str| {
        crate::proto_types::manager_v1::ResolveCandidateArgs {
            context: Some(crate::proto_types::manager_v1::ManagerWriteContext {
                method: "manager_resolve_candidate".into(),
                target: format!("candidate:{}", candidate.candidate_id),
                schema_fingerprint: "schema".into(),
                idempotency_key: idempotency_key.into(),
                data_root_id: "root".into(),
                contract_fingerprint: "contract".into(),
                actor_audit: Some(crate::proto_types::manager_v1::ActorAudit {
                    actor_id: "human-reviewer".into(),
                    actor_kind: crate::v1::ActorKind::Human as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            candidate_id: candidate.candidate_id.clone(),
            expected_revision,
            decision: Some(
                crate::proto_types::manager_v1::resolve_candidate_args::Decision::Reject(
                    crate::proto_types::manager_v1::RejectResolution {
                        reason_category,
                        ..Default::default()
                    },
                ),
            ),
            ..Default::default()
        }
    };

    let success = manager
        .dispatch_manager(
            "manager_resolve_candidate",
            &request(
                Some(0),
                crate::proto_types::manager_v1::ResolutionReasonCategory::NotActionable as i32,
                "resolve-reject-success",
            )
            .encode_to_vec(),
        )
        .expect("resolve response");
    let success =
        crate::proto_types::manager_v1::ResolveCandidateResult::decode(success.as_slice())
            .expect("decode resolve response");
    assert!(matches!(
        success.outcome,
        Some(crate::proto_types::manager_v1::resolve_candidate_result::Outcome::Success(receipt))
            if receipt.state == crate::proto_types::manager_v1::DurableOperationState::Committed as i32
    ));

    let stale = manager
        .dispatch_manager(
            "manager_resolve_candidate",
            &request(
                Some(0),
                crate::proto_types::manager_v1::ResolutionReasonCategory::NotActionable as i32,
                "resolve-reject-stale",
            )
            .encode_to_vec(),
        )
        .expect("stale resolve response");
    let stale = crate::proto_types::manager_v1::ResolveCandidateResult::decode(stale.as_slice())
        .expect("decode stale resolve response");
    assert!(matches!(
        stale.outcome,
        Some(crate::proto_types::manager_v1::resolve_candidate_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::RevisionConflict as i32
    ));

    let missing_reason = manager
        .dispatch_manager(
            "manager_resolve_candidate",
            &request(
                Some(1),
                crate::proto_types::manager_v1::ResolutionReasonCategory::Unspecified as i32,
                "resolve-reject-no-reason",
            )
            .encode_to_vec(),
        )
        .expect("missing reason response");
    let missing_reason =
        crate::proto_types::manager_v1::ResolveCandidateResult::decode(missing_reason.as_slice())
            .expect("decode missing reason response");
    assert!(matches!(
        missing_reason.outcome,
        Some(crate::proto_types::manager_v1::resolve_candidate_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::ManagerInvalidArgument as i32
    ));
}
#[tokio::test]
async fn composed_manager_dispatch_returns_claim_handle_once_and_never_replays_it() {
    let (_config, mut manager) = manager_for_test_with_payload_limit();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = ClaimCapabilityDispatcher {
        calls: Arc::clone(&calls),
    };
    let eligibility_generation = manager.storage_mut().generation();
    let args = crate::proto_types::manager_v1::ClaimIssueArgs {
        context: Some(crate::proto_types::manager_v1::ManagerWriteContext {
            method: "manager_claim_issue".into(),
            target: "issue:issue-claim".into(),
            schema_fingerprint: "schema".into(),
            idempotency_key: "claim-once".into(),
            data_root_id: "root".into(),
            contract_fingerprint: "contract".into(),
            correlation_id: "claim-correlation".into(),
            actor_audit: Some(crate::proto_types::manager_v1::ActorAudit {
                actor_id: "agent-1".into(),
                actor_kind: crate::v1::ActorKind::InternalAgent as i32,
                ..Default::default()
            }),
        }),
        issue_id: "issue-claim".into(),
        expected_revision: Some(0),
        requested_lease: Some(crate::proto_types::manager_v1::TimeSpan { millis: 5_000 }),
        gateway_eligibility_generation: Some(eligibility_generation),
    };
    let first = manager
        .dispatch_manager_with_dispatcher(
            "manager_claim_issue",
            &args.encode_to_vec(),
            &dispatcher,
            100,
        )
        .await
        .expect("claim dispatch");
    let first = crate::proto_types::manager_v1::ClaimIssueResult::decode(first.as_slice())
        .expect("decode claim result");
    let operation_id = match first.outcome.expect("claim outcome") {
        crate::proto_types::manager_v1::claim_issue_result::Outcome::Success(mutation) => {
            let operation = mutation.operation.expect("operation");
            let claim = match mutation.result.expect("claim result") {
                crate::proto_types::manager_v1::manager_mutation::Result::ClaimIssue(claim) => {
                    claim
                }
                other => panic!("unexpected result: {other:?}"),
            };
            assert_eq!(claim.lease_handle, b"ephemeral-claim-handle");
            assert_eq!(claim.current.expect("current").issue_id, "issue-claim");
            assert_eq!(
                operation.state,
                crate::proto_types::manager_v1::DurableOperationState::Committed as i32
            );
            operation.operation_id
        }
        crate::proto_types::manager_v1::claim_issue_result::Outcome::Error(error) => {
            panic!("unexpected claim error: {error:?}")
        }
    };
    let replay = manager
        .dispatch_manager_with_dispatcher(
            "manager_claim_issue",
            &args.encode_to_vec(),
            &dispatcher,
            101,
        )
        .await
        .expect("claim replay");
    let replay = crate::proto_types::manager_v1::ClaimIssueResult::decode(replay.as_slice())
        .expect("decode replay result");
    assert!(matches!(
        replay.outcome,
        Some(crate::proto_types::manager_v1::claim_issue_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::PreconditionFailed as i32
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let operation = manager
        .storage_mut()
        .repository()
        .get_operation(&operation_id)
        .expect("operation read")
        .expect("operation exists");
    assert_eq!(
        operation.state,
        crate::proto_types::manager_v1::DurableOperationState::Committed as i32
    );
    manager
        .storage()
        .read(|connection| {
            let outbox: i64 =
                connection
                    .query_row("SELECT COUNT(*) FROM manager_outbox", [], |row| row.get(0))?;
            assert_eq!(outbox, 0);
            Ok(())
        })
        .expect("inspect manager storage");
}

#[tokio::test]
async fn capability_preparation_releases_manager_lock_before_tracker_dispatch() {
    let (_config, manager) = manager_for_test_with_payload_limit();
    let manager = Arc::new(tokio::sync::Mutex::new(manager));
    let args = crate::proto_types::manager_v1::HeartbeatIssueClaimArgs {
        context: Some(crate::proto_types::manager_v1::ManagerWriteContext {
            method: "manager_heartbeat_issue_claim".into(),
            target: "issue:issue-capability-lock".into(),
            schema_fingerprint: "schema".into(),
            idempotency_key: "capability-lock-boundary".into(),
            data_root_id: "root".into(),
            contract_fingerprint: "contract".into(),
            correlation_id: "capability-lock-boundary".into(),
            actor_audit: Some(crate::proto_types::manager_v1::ActorAudit {
                actor_id: "worker".into(),
                actor_kind: crate::v1::ActorKind::InternalAgent as i32,
                ..Default::default()
            }),
        }),
        issue_id: "issue-capability-lock".into(),
        expected_revision: Some(0),
        claim_revision: Some(0),
        lease_handle: b"ephemeral-only".to_vec(),
        requested_extension: Some(crate::proto_types::manager_v1::TimeSpan { millis: 1_000 }),
    };
    let preparation = {
        let mut manager = manager.lock().await;
        manager
            .prepare_lease_capability("manager_heartbeat_issue_claim", &args.encode_to_vec(), 100)
            .expect("capability admission")
    };
    let operation_id = match &preparation {
        LeaseCapabilityPreparation::Dispatch(prepared) => prepared.operation.operation_id.clone(),
        LeaseCapabilityPreparation::Immediate(_) => {
            panic!("capability admission unexpectedly rejected")
        }
    };
    let expectation = manager
        .lock()
        .await
        .storage_mut()
        .repository()
        .backend_marker_expectation(&operation_id)
        .expect("read capability marker expectation");
    assert!(expectation.is_some(), "capability marker must be durable");
    let acquired = tokio::time::timeout(std::time::Duration::from_millis(100), manager.lock())
        .await
        .expect("manager lock must not be held during Tracker dispatch");
    drop(acquired);

    // The prepared operation is intentionally left un-dispatched here;
    // the test only exercises the lock boundary and the admission record.
}

#[tokio::test]
async fn capability_claim_rejects_a_stale_gateway_eligibility_generation() {
    let (_config, mut manager) = manager_for_test_with_payload_limit();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = ClaimCapabilityDispatcher {
        calls: Arc::clone(&calls),
    };
    let stale_generation = manager.storage_mut().generation().saturating_add(1);
    let args = crate::proto_types::manager_v1::ClaimIssueArgs {
        context: Some(crate::proto_types::manager_v1::ManagerWriteContext {
            method: "manager_claim_issue".into(),
            target: "issue:issue-claim".into(),
            schema_fingerprint: "schema".into(),
            idempotency_key: "claim-stale-generation".into(),
            data_root_id: "root".into(),
            contract_fingerprint: "contract".into(),
            correlation_id: "claim-stale-generation".into(),
            actor_audit: Some(crate::proto_types::manager_v1::ActorAudit {
                actor_id: "agent-1".into(),
                actor_kind: crate::v1::ActorKind::InternalAgent as i32,
                ..Default::default()
            }),
        }),
        issue_id: "issue-claim".into(),
        expected_revision: Some(0),
        requested_lease: Some(crate::proto_types::manager_v1::TimeSpan { millis: 5_000 }),
        gateway_eligibility_generation: Some(stale_generation),
    };

    let result = manager
        .dispatch_manager_with_dispatcher(
            "manager_claim_issue",
            &args.encode_to_vec(),
            &dispatcher,
            100,
        )
        .await
        .expect("typed claim rejection");
    let result = crate::proto_types::manager_v1::ClaimIssueResult::decode(result.as_slice())
        .expect("decode claim result");

    assert!(matches!(
        result.outcome,
        Some(crate::proto_types::manager_v1::claim_issue_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::PreconditionFailed as i32
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn lease_capability_is_synchronous_not_replayable_and_never_persisted() {
    let (_config, mut manager) = manager_for_test_with_payload_limit();
    let args = crate::proto_types::manager_v1::HeartbeatIssueClaimArgs {
        context: Some(crate::proto_types::manager_v1::ManagerWriteContext {
            method: "manager_heartbeat_issue_claim".into(),
            target: "issue:issue-capability".into(),
            schema_fingerprint: "schema".into(),
            idempotency_key: "capability-request".into(),
            data_root_id: "root".into(),
            contract_fingerprint: "contract".into(),
            correlation_id: "capability-correlation".into(),
            actor_audit: Some(crate::proto_types::manager_v1::ActorAudit {
                actor_id: "human-1".into(),
                actor_kind: crate::v1::ActorKind::Human as i32,
                ..Default::default()
            }),
        }),
        issue_id: "issue-capability".into(),
        expected_revision: Some(4),
        claim_revision: Some(4),
        lease_handle: b"lease-capability-must-not-persist".to_vec(),
        requested_extension: Some(crate::proto_types::manager_v1::TimeSpan { millis: 1_000 }),
    };
    let first = manager
        .dispatch_lease_capability_with_lock(
            "manager_heartbeat_issue_claim",
            &args.encode_to_vec(),
            &CapabilityMarkerDispatcher,
            100,
        )
        .await
        .expect("capability dispatch");
    let first = crate::proto_types::manager_v1::HeartbeatIssueClaimResult::decode(first.as_slice())
        .expect("typed result");
    match first.outcome.expect("outcome") {
        crate::proto_types::manager_v1::heartbeat_issue_claim_result::Outcome::Success(
            mutation,
        ) => {
            assert_eq!(
                mutation.operation.expect("operation").state,
                crate::proto_types::manager_v1::DurableOperationState::Committed as i32
            );
        }
        crate::proto_types::manager_v1::heartbeat_issue_claim_result::Outcome::Error(error) => {
            panic!("unexpected capability error: {error:?}");
        }
    }
    let replay = manager
        .dispatch_lease_capability_with_lock(
            "manager_heartbeat_issue_claim",
            &args.encode_to_vec(),
            &CapabilityMarkerDispatcher,
            101,
        )
        .await
        .expect("capability replay");
    let replay =
        crate::proto_types::manager_v1::HeartbeatIssueClaimResult::decode(replay.as_slice())
            .expect("typed replay result");
    assert!(matches!(
        replay.outcome,
        Some(crate::proto_types::manager_v1::heartbeat_issue_claim_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed as i32
    ));
    manager
        .storage()
        .read(|connection| {
            let outbox: i64 =
                connection
                    .query_row("SELECT COUNT(*) FROM manager_outbox", [], |row| row.get(0))?;
            assert_eq!(outbox, 0);
            let attempt: (String, String) = connection.query_row(
                "SELECT state, evidence_classification
                       FROM manager_capability_attempt
                      ORDER BY created_at DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert_eq!(attempt.0, repository::CAPABILITY_ATTEMPT_RESOLVED);
            assert_eq!(attempt.1, "MARKER_CONFIRMED");
            for entry in std::fs::read_dir(manager.storage().root()).expect("list manager root") {
                let entry = entry.expect("directory entry");
                if !entry.file_type().expect("file type").is_file() {
                    continue;
                }
                let bytes = std::fs::read(entry.path()).expect("read manager file");
                assert!(
                    !bytes
                        .windows(b"lease-capability-must-not-persist".len())
                        .any(|window| window == b"lease-capability-must-not-persist")
                );
            }
            Ok(())
        })
        .expect("inspect manager database");
}

#[tokio::test]
async fn lost_lease_capability_delivery_is_unconfirmed_without_outbox_retry() {
    let (_config, mut manager) = manager_for_test_with_payload_limit();
    let args = crate::proto_types::manager_v1::ReleaseIssueClaimArgs {
        context: Some(crate::proto_types::manager_v1::ManagerWriteContext {
            method: "manager_release_issue_claim".into(),
            target: "issue:issue-release".into(),
            schema_fingerprint: "schema".into(),
            idempotency_key: "release-capability-request".into(),
            data_root_id: "root".into(),
            contract_fingerprint: "contract".into(),
            correlation_id: "release-capability-correlation".into(),
            actor_audit: Some(crate::proto_types::manager_v1::ActorAudit {
                actor_id: "human-1".into(),
                actor_kind: crate::v1::ActorKind::Human as i32,
                ..Default::default()
            }),
        }),
        issue_id: "issue-release".into(),
        expected_revision: Some(4),
        claim_revision: Some(4),
        lease_handle: b"lost-capability-handle".to_vec(),
    };
    let result = manager
        .dispatch_lease_capability_with_lock(
            "manager_release_issue_claim",
            &args.encode_to_vec(),
            &UnavailableCapabilityDispatcher,
            100,
        )
        .await
        .expect("capability dispatch");
    let result = crate::proto_types::manager_v1::ReleaseIssueClaimResult::decode(result.as_slice())
        .expect("typed result");
    assert!(matches!(
        result.outcome,
        Some(crate::proto_types::manager_v1::release_issue_claim_result::Outcome::Error(error))
            if error.code == crate::proto_types::manager_v1::ManagerErrorCode::ResultUnconfirmed as i32
    ));
    manager
        .storage()
        .read(|connection| {
            let outbox: i64 =
                connection
                    .query_row("SELECT COUNT(*) FROM manager_outbox", [], |row| row.get(0))?;
            assert_eq!(outbox, 0);
            let state: i32 = connection.query_row(
                "SELECT state FROM manager_operation ORDER BY created_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(
                state,
                crate::proto_types::manager_v1::DurableOperationState::Unconfirmed as i32
            );
            let evidence: (String, String) = connection.query_row(
                "SELECT state, evidence_classification
                       FROM manager_capability_attempt
                      ORDER BY created_at DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert_eq!(evidence.0, repository::CAPABILITY_ATTEMPT_SENT_UNKNOWN);
            assert_eq!(evidence.1, "DELIVERY_UNKNOWN");
            Ok(())
        })
        .expect("inspect manager state");
}

fn manager_for_test_with_payload_limit() -> (ManagerConfig, IssueTrackerManager) {
    let root = tempdir().expect("temporary manager root");
    let mut settings = config::tests::settings();
    settings.maximum_payload_bytes = 64 * 1024;
    settings.data_root = root.keep().to_string_lossy().into_owned();
    let config = ManagerConfig::from_proto(&settings).expect("valid settings");
    let manager = IssueTrackerManager::open(config.clone()).expect("open manager");
    (config, manager)
}

fn install_legacy_pending_outbox(manager: &mut IssueTrackerManager) {
    let request = crate::proto_types::manager_v1::UpdateIssueArgs {
        issue_id: "issue-legacy".into(),
        expected_revision: Some(0),
        ..Default::default()
    }
    .encode_to_vec();
    let operation = crate::proto_types::manager_v1::DurableOperation {
        operation_id: "operation-legacy".into(),
        scope: Some(
            crate::proto_types::manager_v1::durable_operation::Scope::IssueId(
                "issue-legacy".into(),
            ),
        ),
        method: "manager_update_issue".into(),
        idempotency_scope: "legacy-scope".into(),
        request_digest: operations::digest_bytes(&request),
        request_schema: "manager_update_issue.request".into(),
        downstream_operation_id: "downstream-legacy".into(),
        state: crate::proto_types::manager_v1::DurableOperationState::Pending as i32,
        generation: 1,
        ..Default::default()
    };
    let mutation = crate::proto_types::manager_v1::ManagerMutation {
        operation: Some(operation.clone()),
        ..Default::default()
    };
    let outbox = crate::proto_types::manager_v1::OutboxEntry {
        outbox_id: "outbox-legacy".into(),
        operation_id: operation.operation_id.clone(),
        topic: "taskchampion".into(),
        status: crate::proto_types::manager_v1::OutboxStatus::Ready as i32,
        payload_snapshot_ref: operation.operation_id.clone(),
        payload_digest: operations::digest_bytes(&request),
        generation: 1,
        ..Default::default()
    };
    manager
        .storage_mut()
        .repository()
        .commit_mutation(&mutation, &operation, Some(&outbox))
        .expect("install legacy outbox");
    let marker = Vec::new();
    manager
        .storage_mut()
        .write(|connection, _state| {
            Ok(connection.execute(
                "UPDATE manager_operation
                    SET request_snapshot = ?1,
                        request_snapshot_schema = ?2,
                        request_snapshot_digest = ?3,
                        marker_expectation = ?4,
                        marker_expectation_schema = ?5
                  WHERE operation_id = ?6",
                params![
                    request,
                    "manager_update_issue.request",
                    operations::digest_bytes(&request),
                    marker,
                    "issue_tracker.v1.OperationRecord",
                    operation.operation_id,
                ],
            )?)
        })
        .expect("install legacy raw request snapshot");
}

fn replace_with_durable_request_and_missing_marker(manager: &mut IssueTrackerManager) {
    let update = crate::proto_types::manager_v1::UpdateIssueArgs {
        issue_id: "issue-legacy".into(),
        expected_revision: Some(0),
        ..Default::default()
    }
    .encode_to_vec();
    let request = backend_dispatch::durable_request_from_manager("manager_update_issue", &update)
        .expect("build durable request");
    manager
            .storage_mut()
            .write(|connection, _state| {
                connection.execute(
                    "UPDATE manager_operation
                        SET request_snapshot = ?1,
                            request_snapshot_schema = 'issue_tracker.manager.v1.DurableBackendRequest',
                            request_snapshot_digest = ?2,
                            marker_expectation = NULL,
                            marker_expectation_schema = NULL
                      WHERE operation_id = 'operation-legacy'",
                    params![request, operations::digest_bytes(&request)],
                )?;
                Ok(connection.execute(
                    "UPDATE manager_outbox SET payload_digest = ?1 WHERE outbox_id = 'outbox-legacy'",
                    params![operations::digest_bytes(&request)],
                )?)
            })
            .expect("install durable request with missing marker");
}

#[tokio::test]
async fn legacy_raw_request_snapshot_is_quarantined_without_dispatch_and_stays_terminal_after_restart()
 {
    let (config, mut manager) = manager_for_test();
    install_legacy_pending_outbox(&mut manager);
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = NeverCalledDispatcher {
        calls: Arc::clone(&calls),
    };

    let result = manager
        .process_one_outbox(&dispatcher, 1_000)
        .await
        .expect("legacy outbox should converge safely")
        .expect("legacy outbox should produce a typed result");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        result.state,
        crate::proto_types::manager_v1::DurableOperationState::ManualResolution
    );
    assert!(result.operation.resolution_required);
    let stored_operation = manager
        .storage_mut()
        .repository()
        .get_operation("operation-legacy")
        .expect("read quarantined operation")
        .expect("quarantined operation exists");
    assert_eq!(stored_operation.state, result.operation.state);
    assert!(matches!(
        stored_operation.terminal_snapshot,
        Some(crate::proto_types::manager_v1::durable_operation::TerminalSnapshot::ErrorSnapshot(
            snapshot
        )) if snapshot.schema == "issue_tracker.manager.v1.ManagerError"
    ));
    let stored_outbox = manager
        .storage_mut()
        .repository()
        .get_outbox("outbox-legacy")
        .expect("read quarantined outbox")
        .expect("quarantined outbox exists");
    assert_eq!(
        stored_outbox.status,
        crate::proto_types::manager_v1::OutboxStatus::DeadLetter as i32
    );
    assert_eq!(
        stored_outbox.error_code,
        crate::proto_types::manager_v1::ManagerErrorCode::ResolutionRequired as i32
    );
    assert!(stored_outbox.lease_id.is_none());

    drop(manager);
    let mut reopened = IssueTrackerManager::open(config).expect("reopen manager");
    assert!(
        reopened
            .process_one_outbox(&dispatcher, 2_000)
            .await
            .expect("dead-lettered legacy outbox must not be selected")
            .is_none()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_durable_snapshot_is_quarantined_without_retrying_dispatch() {
    let (_config, mut manager) = manager_for_test();
    install_legacy_pending_outbox(&mut manager);
    manager
            .storage_mut()
            .write(|connection, _state| {
                Ok(connection.execute(
                    "UPDATE manager_operation
                        SET request_snapshot = ?1,
                            request_snapshot_schema = 'issue_tracker.manager.v1.DurableBackendRequest'
                      WHERE operation_id = 'operation-legacy'",
                    params![vec![0xff_u8]],
                )?)
            })
            .expect("install malformed durable request");
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = NeverCalledDispatcher {
        calls: Arc::clone(&calls),
    };

    let result = manager
        .process_one_outbox(&dispatcher, 1_000)
        .await
        .expect("malformed durable request must converge")
        .expect("malformed durable request produces a result");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        result.state,
        crate::proto_types::manager_v1::DurableOperationState::ManualResolution
    );
    let outbox = manager
        .storage_mut()
        .repository()
        .get_outbox("outbox-legacy")
        .expect("read quarantined outbox")
        .expect("outbox exists");
    assert_eq!(
        outbox.status,
        crate::proto_types::manager_v1::OutboxStatus::DeadLetter as i32
    );
}

#[tokio::test]
async fn missing_marker_expectation_is_quarantined_without_retrying_dispatch() {
    let (_config, mut manager) = manager_for_test();
    install_legacy_pending_outbox(&mut manager);
    replace_with_durable_request_and_missing_marker(&mut manager);
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = NeverCalledDispatcher {
        calls: Arc::clone(&calls),
    };

    let result = manager
        .process_one_outbox(&dispatcher, 1_000)
        .await
        .expect("missing marker must converge")
        .expect("missing marker produces a terminal result");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        result.state,
        crate::proto_types::manager_v1::DurableOperationState::ManualResolution
    );
    assert!(
        manager
            .process_one_outbox(&dispatcher, 2_000)
            .await
            .expect("dead-lettered outbox is not retried")
            .is_none()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn corrupt_operation_payload_is_terminally_isolated_without_dispatch() {
    let (_config, mut manager) = manager_for_test();
    install_legacy_pending_outbox(&mut manager);
    manager
        .storage_mut()
        .write(|connection, _state| {
            Ok(connection.execute(
                "UPDATE manager_operation
                        SET operation_payload = ?1,
                            operation_payload_schema = 'issue_tracker.manager.v1.DurableOperation',
                            operation_payload_digest = ?2
                      WHERE operation_id = 'operation-legacy'",
                params![vec![0xff_u8], operations::digest_bytes(&[0xff_u8])],
            )?)
        })
        .expect("corrupt operation payload");
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = NeverCalledDispatcher {
        calls: Arc::clone(&calls),
    };

    assert!(
        manager
            .process_one_outbox(&dispatcher, 1_000)
            .await
            .expect("corrupt operation is isolated")
            .is_none()
    );
    assert!(
        manager
            .process_one_outbox(&dispatcher, 2_000)
            .await
            .expect("dead-lettered corrupt operation is not retried")
            .is_none()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let outbox = manager
        .storage_mut()
        .repository()
        .get_outbox("outbox-legacy")
        .expect("read outbox")
        .expect("outbox exists");
    assert_eq!(
        outbox.status,
        crate::proto_types::manager_v1::OutboxStatus::DeadLetter as i32
    );
    let diagnosis = manager
        .storage_mut()
        .repository()
        .operation_diagnosis("operation-legacy")
        .expect("diagnosis read")
        .expect("corrupt operation diagnosis");
    assert_eq!(diagnosis.classification, "CORRUPT_OPERATION");
    assert_eq!(diagnosis.resolution_state, "MANUAL_RESOLUTION");
}
