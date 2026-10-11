use super::super::*;
use super::support::assert_primary_message;
use crate::proto_types::{manager_v1, v1 as issue_tracker_v1};
use prost::Message;

#[test]
fn manager_method_catalog_excludes_withdrawn_evidence_apis() {
    let methods = resolved_manager_method_protos();
    assert_eq!(methods.len(), MANAGER_METHOD_SPECS.len());
    for spec in MANAGER_METHOD_SPECS {
        let (args, result) = methods.get(spec.key).expect("catalogued manager method");
        assert_primary_message(args, spec.args);
        assert_primary_message(result, spec.result);
    }
    for withdrawn in [
        "manager_create_evidence",
        "manager_update_evidence",
        "manager_bind_evidence",
        "manager_unbind_evidence",
        "manager_get_evidence",
        "manager_list_evidence",
        "manager_get_evidence_binding",
        "manager_list_evidence_bindings",
    ] {
        assert!(
            !MANAGER_METHOD_SPECS
                .iter()
                .any(|spec| spec.key == withdrawn)
        );
    }
}

#[test]
fn manager_contract_exposes_direct_source_evidence_without_binding_types() {
    for message in ["CandidateDraft", "Candidate", "ManagerIssuePatch"] {
        assert!(MANAGER_COMMON_PROTO.contains(&format!("message {message}")));
    }
    for withdrawn in [
        "Evidence",
        "EvidenceBinding",
        "EvidenceManifest",
        "EvidenceBindingDraft",
    ] {
        assert!(!MANAGER_COMMON_PROTO.contains(&format!("message {withdrawn}")));
    }
    let candidate = manager_v1::CandidateDraft {
        candidate_id: "candidate-1".into(),
        source_evidence: vec![issue_tracker_v1::SourceEvidenceRef {
            thread_id: "thread-1".into(),
            memory_id: "memory-1".into(),
        }],
        ..Default::default()
    };
    let decoded = manager_v1::CandidateDraft::decode(candidate.encode_to_vec().as_slice())
        .expect("candidate direct references round-trip");
    assert_eq!(decoded.source_evidence[0].memory_id, "memory-1");
}

#[test]
fn resolution_basis_distinguishes_human_and_automatic_review() {
    assert_eq!(manager_v1::ResolutionBasis::HumanReview as i32, 2);
    assert_eq!(manager_v1::ResolutionBasis::AutoReview as i32, 3);
    assert!(!MANAGER_COMMON_PROTO.contains("ACTOR_REVIEW"));
}

#[test]
fn issue_merge_target_carries_server_resolved_source_references() {
    let target = manager_v1::IssueMergeTarget {
        issue_id: "issue-1".into(),
        expected_revision: Some(4),
        resolved_source_evidence: vec![issue_tracker_v1::SourceEvidenceRef {
            thread_id: "thread-1".into(),
            memory_id: "memory-1".into(),
        }],
        ..Default::default()
    };
    let decoded = manager_v1::IssueMergeTarget::decode(target.encode_to_vec().as_slice())
        .expect("issue merge target round-trips");
    assert_eq!(decoded.resolved_source_evidence.len(), 1);
}

#[test]
fn merge_and_candidate_filter_use_direct_source_evidence_contracts() {
    assert!(MANAGER_METHODS_PROTO.contains("ResolutionBasis basis = 4;"));
    assert!(MANAGER_METHODS_PROTO.contains("resolved_source_evidence = 9;"));
    assert!(!MANAGER_COMMON_PROTO.contains("source_uri"));

    let source_evidence = issue_tracker_v1::SourceEvidenceRef {
        thread_id: "thread-1".into(),
        memory_id: "memory-1".into(),
    };
    let filter = manager_v1::CandidateFilter {
        source_evidence: Some(source_evidence.clone()),
        ..Default::default()
    };
    let merge = manager_v1::MergeResolution {
        target: Some(manager_v1::merge_resolution::Target::Candidate(
            manager_v1::CandidateMergeTarget {
                candidate_id: "candidate-2".into(),
                expected_revision: Some(3),
            },
        )),
        basis: manager_v1::ResolutionBasis::SourceEvidence as i32,
        ..Default::default()
    };
    let confirm = manager_v1::ConfirmAssociatedIssueResolution {
        issue_id: "issue-1".into(),
        expected_revision: Some(4),
        resolved_source_evidence: vec![source_evidence.clone()],
        ..Default::default()
    };
    assert_eq!(
        manager_v1::CandidateFilter::decode(filter.encode_to_vec().as_slice())
            .expect("candidate filter round-trips")
            .source_evidence,
        Some(source_evidence.clone())
    );
    assert_eq!(
        manager_v1::MergeResolution::decode(merge.encode_to_vec().as_slice())
            .expect("merge resolution round-trips")
            .basis,
        manager_v1::ResolutionBasis::SourceEvidence as i32
    );
    assert_eq!(
        manager_v1::ConfirmAssociatedIssueResolution::decode(confirm.encode_to_vec().as_slice())
            .expect("confirm resolution round-trips")
            .resolved_source_evidence,
        vec![source_evidence]
    );
}

#[test]
fn review_candidate_contract_round_trips_and_exposes_resolution_pending() {
    let row = manager_v1::ReviewCandidateRow {
        candidate: Some(manager_v1::Candidate {
            candidate_id: "candidate-1".into(),
            status: manager_v1::CandidateStatus::AutoRegistered as i32,
            ..Default::default()
        }),
        integrity: manager_v1::ReviewCandidateIntegrity::Normal as i32,
        resolution_state: manager_v1::ReviewCandidateResolutionState::ResolutionPending as i32,
        ..Default::default()
    };
    let page = manager_v1::ReviewCandidatePage {
        rows: vec![row],
        composite_snapshot: Some(manager_v1::CompositePageSnapshot {
            backend_snapshot: Some(issue_tracker_v1::PageSnapshot::default()),
            manager_snapshot: Some(manager_v1::PageSnapshot::default()),
        }),
        ..Default::default()
    };
    let decoded = manager_v1::ReviewCandidatePage::decode(page.encode_to_vec().as_slice())
        .expect("review page round-trips");
    assert_eq!(decoded.rows.len(), 1);
    assert_eq!(
        manager_v1::ReviewCandidateResolutionState::try_from(decoded.rows[0].resolution_state)
            .expect("known resolution state"),
        manager_v1::ReviewCandidateResolutionState::ResolutionPending
    );
}
