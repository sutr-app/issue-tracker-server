use super::*;

#[test]
fn suppression_identity_index_preserves_the_first_matching_identity() {
    let identities = vec![
        ExtractionCandidateIdentity {
            candidate_id: "candidate-first".into(),
            thread_id: "thread-1".into(),
            snapshot_digest: "sha256:snapshot-1".into(),
            fingerprint: "fingerprint-1".into(),
        },
        ExtractionCandidateIdentity {
            candidate_id: "candidate-later".into(),
            thread_id: "thread-2".into(),
            snapshot_digest: "sha256:snapshot-2".into(),
            fingerprint: "fingerprint-2".into(),
        },
    ];
    let suppression = ExtractionSuppression {
        input_index: 0,
        // The former linear scan was ordered by identities, not by this
        // request list. Preserve that observable audit projection.
        rejected_candidate_ids: vec!["candidate-later".into(), "candidate-first".into()],
    };

    let index = suppression_identity_index(&identities);
    let selected = suppression_audit_identity(&identities, &index, &suppression)
        .expect("a listed rejected candidate has an immutable identity");

    assert_eq!(selected.candidate_id, "candidate-first");
}

#[test]
fn auto_registration_in_flight_query_is_usable_from_read_and_write_connections() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let operation = auto_registration_test_operation("in-flight-query");
    seed_auto_registration_entities(&mut storage, &operation);
    let mut outbox = outbox::new_entry(
        format!("outbox-{}", operation.operation_id),
        &operation,
        "test",
        operation.operation_id.clone(),
        "sha256:test",
        1,
    )
    .expect("build outbox");
    outbox::lease(&mut outbox, 10, 10, None).expect("lease outbox");
    storage
        .write_with::<_, RepositoryError, _>(|transaction, stamp| {
            insert_outbox(transaction, &outbox, stamp)
        })
        .expect("seed leased outbox");
    ManagerRepository::new(&mut storage)
        .register_extraction_auto_registration(
            &operation.operation_id,
            "run-auto-registration",
            "candidate-auto-registration",
            "issue-auto-registration",
            "test-auto-registration-key",
        )
        .expect("register auto-registration link");

    let readable = storage
        .read(|connection| {
            candidate_auto_registration_is_in_flight(connection, "candidate-auto-registration")
                .map_err(ManagerStorageError::from)
        })
        .expect("read query");
    let (writable, _) = storage
        .write_with::<_, RepositoryError, _>(|transaction, _| {
            Ok(candidate_auto_registration_is_in_flight(
                transaction,
                "candidate-auto-registration",
            )?)
        })
        .expect("write query");

    assert!(readable);
    assert!(writable);
}

#[test]
fn gateway_claim_admission_rejects_a_stale_eligibility_generation() {
    let error = validate_gateway_eligibility_generation(Some(7), 8)
        .expect_err("a changed manager snapshot must be re-evaluated");

    assert_eq!(error.code, pb::ManagerErrorCode::PreconditionFailed as i32);
    assert!(!error.retryable);
}

#[test]
fn gateway_claim_admission_accepts_current_or_unset_eligibility_generation() {
    assert!(validate_gateway_eligibility_generation(Some(7), 7).is_ok());
    assert!(validate_gateway_eligibility_generation(None, 7).is_ok());
}

#[test]
fn acceptance_maps_candidate_fields_to_a_confirmed_issue_draft() {
    let candidate = pb::Candidate {
        candidate_id: "candidate-42".into(),
        title: "Do the work".into(),
        description: "Bounded description".into(),
        kind: pb::CandidateKind::Bug as i32,
        suggested_priority: pb::CandidatePriority::P1 as i32,
        suggested_state: pb::CandidateSuggestedState::Deferred as i32,
        detection_basis: pb::DetectionBasis::Explicit as i32,
        extraction_run_id: "run-42".into(),
        metadata: vec![pb::MetadataEntry {
            key: "source".into(),
            value: "thread:1".into(),
        }],
        ..Default::default()
    };

    let issue = candidate_issue_draft(&candidate).expect("issue draft");

    assert_eq!(issue.kind, v1::IssueKind::Bug as i32);
    assert_eq!(issue.priority, v1::IssuePriority::P1 as i32);
    assert_eq!(issue.initial_state, v1::IssueState::Deferred as i32);
    assert_eq!(
        issue.confirmation_status,
        v1::ConfirmationStatus::Confirmed as i32
    );
    assert!(
        issue
            .provenance_metadata
            .iter()
            .any(|entry| { entry.key == "candidate_id" && entry.value == "candidate-42" })
    );
    assert!(
        issue
            .provenance_metadata
            .iter()
            .any(|entry| { entry.key == "extraction_run_id" && entry.value == "run-42" })
    );
}

#[test]
fn acceptance_rejects_client_owned_candidate_provenance_keys() {
    let candidate = pb::Candidate {
        candidate_id: "candidate-42".into(),
        extraction_run_id: "run-42".into(),
        title: "Candidate title".into(),
        description: "Candidate description".into(),
        kind: pb::CandidateKind::Task as i32,
        suggested_priority: pb::CandidatePriority::P2 as i32,
        suggested_state: pb::CandidateSuggestedState::Open as i32,
        detection_basis: pb::DetectionBasis::Explicit as i32,
        metadata: vec![pb::MetadataEntry {
            key: "extraction_run_id".into(),
            value: "forged-run".into(),
        }],
        ..Default::default()
    };

    assert!(candidate_issue_draft(&candidate).is_err());
}

#[test]
fn acceptance_normalizes_in_progress_suggestion_and_records_why() {
    let candidate = pb::Candidate {
        candidate_id: "candidate-in-progress".into(),
        title: "Observed in progress".into(),
        description: "The candidate is only an observation".into(),
        kind: pb::CandidateKind::Task as i32,
        suggested_priority: pb::CandidatePriority::P2 as i32,
        suggested_state: pb::CandidateSuggestedState::InProgress as i32,
        detection_basis: pb::DetectionBasis::Explicit as i32,
        ..Default::default()
    };

    let issue = candidate_issue_draft(&candidate).expect("issue draft");

    assert_eq!(issue.initial_state, v1::IssueState::Open as i32);
    assert!(
        issue.provenance_metadata.iter().any(|entry| {
            entry.key == "ownership_normalized_from" && entry.value == "IN_PROGRESS"
        })
    );
    assert!(issue.provenance_metadata.iter().any(|entry| {
        entry.key == "ownership_normalization_reason"
            && entry.value == "candidate_suggested_state_is_not_a_claim"
    }));
}

#[test]
fn candidate_adoption_state_validation_matches_and_normalizes_the_issue_draft() {
    let candidate = pb::Candidate {
        candidate_id: "candidate-in-progress-adoption".into(),
        title: "Observed in progress".into(),
        description: "The candidate is only an observation".into(),
        kind: pb::CandidateKind::Task as i32,
        suggested_priority: pb::CandidatePriority::P2 as i32,
        suggested_state: pb::CandidateSuggestedState::InProgress as i32,
        detection_basis: pb::DetectionBasis::Explicit as i32,
        ..Default::default()
    };
    let mut issue = v1::IssueDraft {
        initial_state: v1::IssueState::InProgress as i32,
        ..Default::default()
    };

    validate_candidate_issue_state(&issue, &candidate).expect("IN_PROGRESS is normalized");
    append_candidate_provenance(&mut issue.provenance_metadata, &candidate)
        .expect("server owns candidate provenance");
    normalize_candidate_issue_state(&mut issue, &candidate)
        .expect("candidate state must normalize");

    assert_eq!(issue.initial_state, v1::IssueState::Open as i32);
    assert!(
        issue.provenance_metadata.iter().any(|entry| {
            entry.key == "ownership_normalized_from" && entry.value == "IN_PROGRESS"
        })
    );
}

#[test]
fn candidate_adoption_rejects_state_mismatch_and_deferred_without_review_at() {
    let blocked = pb::Candidate {
        candidate_id: "candidate-blocked-adoption".into(),
        suggested_state: pb::CandidateSuggestedState::Blocked as i32,
        ..Default::default()
    };
    let open_issue = v1::IssueDraft {
        initial_state: v1::IssueState::Open as i32,
        ..Default::default()
    };
    assert!(validate_candidate_issue_state(&open_issue, &blocked).is_err());

    let deferred = pb::Candidate {
        candidate_id: "candidate-deferred-adoption".into(),
        suggested_state: pb::CandidateSuggestedState::Deferred as i32,
        ..Default::default()
    };
    let deferred_issue = v1::IssueDraft {
        initial_state: v1::IssueState::Deferred as i32,
        ..Default::default()
    };
    assert!(validate_candidate_issue_state(&deferred_issue, &deferred).is_err());
}

#[test]
fn resolve_create_issue_persists_normalized_candidate_state_in_the_backend_request() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    config.maximum_payload_bytes = 1 << 20;
    config.maximum_repeated_item_count = 64;
    config.resource_limits = pb::ManagerResourceLimits {
        max_string_bytes: 4096,
        max_body_bytes: 1 << 20,
        max_uri_bytes: 4096,
        max_repeated_item_count: 64,
        max_page_size: 256,
    };
    let mut storage = ManagerStorage::open(&config).expect("open storage");
    let mut repository = ManagerRepository::new(&mut storage);
    let candidate = pb::Candidate {
        candidate_id: "candidate-create-normalized".into(),
        revision: 0,
        status: pb::CandidateStatus::Proposed as i32,
        title: "candidate title".into(),
        description: "candidate description".into(),
        kind: pb::CandidateKind::Task as i32,
        suggested_priority: pb::CandidatePriority::P2 as i32,
        suggested_state: pb::CandidateSuggestedState::InProgress as i32,
        detection_basis: pb::DetectionBasis::Explicit as i32,
        extraction_run_id: "run-create-normalized".into(),
        source_evidence: Vec::new(),
        ..Default::default()
    };
    repository
        .put_proto(
            EntityKind::Candidate,
            &candidate.candidate_id,
            0,
            "issue_tracker.manager.v1.Candidate",
            &candidate,
        )
        .expect("seed candidate");

    let mut context = write_context(
        "manager_resolve_candidate",
        "candidate:candidate-create-normalized",
    );
    context.idempotency_key = "create-normalized-request".into();
    context.correlation_id = "create-normalized-request".into();
    context.actor_audit = Some(pb::ActorAudit {
        actor_id: "reviewer".into(),
        actor_kind: v1::ActorKind::Human as i32,
        reason: Some("accepted candidate after review".into()),
        ..Default::default()
    });
    let args = pb::ResolveCandidateArgs {
        context: Some(context),
        candidate_id: candidate.candidate_id.clone(),
        expected_revision: Some(0),
        decision: Some(pb::resolve_candidate_args::Decision::CreateIssue(
            pb::CreateIssueResolution {
                issue: Some(v1::IssueDraft {
                    title: candidate.title.clone(),
                    description: candidate.description.clone(),
                    kind: v1::IssueKind::Task as i32,
                    priority: v1::IssuePriority::P2 as i32,
                    creation_basis: v1::CreationBasis::Explicit as i32,
                    confirmation_status: v1::ConfirmationStatus::Confirmed as i32,
                    initial_state: v1::IssueState::InProgress as i32,
                    source_evidence: Vec::new(),
                    ..Default::default()
                }),
                basis: pb::ResolutionBasis::HumanReview as i32,
            },
        )),
        ..Default::default()
    };

    let receipt = GatewayRepository::resolve_candidate(&mut repository, &args)
        .expect("admit create resolution");
    assert_eq!(receipt.state, pb::DurableOperationState::Pending as i32);
    let context = args.context.as_ref().expect("resolution context");
    let scope = IdempotencyScope::new(
        context.contract_fingerprint.clone(),
        context.data_root_id.clone(),
        "manager_resolve_candidate",
        context.target.clone(),
        context.idempotency_key.clone(),
    )
    .expect("canonical resolution scope")
    .canonical();
    let payload = repository
        .backend_request_for_idempotency_scope(&scope)
        .expect("read backend request")
        .expect("backend request is durable");
    let envelope = DurableBackendRequest::decode(payload.as_slice()).expect("decode request");
    let issue = envelope
        .create_issue
        .expect("create issue request")
        .issue
        .expect("issue draft");
    assert_eq!(issue.initial_state, v1::IssueState::Open as i32);
    assert!(
        issue
            .provenance_metadata
            .iter()
            .any(|entry| { entry.key == "candidate_id" && entry.value == candidate.candidate_id })
    );
    assert!(issue.provenance_metadata.iter().any(|entry| {
        entry.key == "extraction_run_id" && entry.value == candidate.extraction_run_id
    }));
    assert!(
        issue.provenance_metadata.iter().any(|entry| {
            entry.key == "ownership_normalized_from" && entry.value == "IN_PROGRESS"
        })
    );
}
