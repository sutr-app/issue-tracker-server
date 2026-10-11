use super::*;
use crate::{
    manager::backend_adapter::{EntityRevisionExpectation, MarkerExpectation},
    v1,
};

fn expected() -> MarkerExpectation {
    MarkerExpectation {
        operation_id: "downstream-op-1".into(),
        method_key: "update_issue".into(),
        scope: v1::OperationScope::Issue as i32,
        payload_fingerprint: "payload-digest".into(),
        contract_fingerprint: "task-contract".into(),
        affected_issue_ids: vec!["issue-1".into()],
        entity_revisions: vec![EntityRevisionExpectation {
            issue_id: "issue-1".into(),
            backend_id: "issue-1".into(),
            resulting_revision: 2,
        }],
        result_replayable: true,
    }
}

fn marker(expected: &MarkerExpectation) -> v1::OperationRecord {
    v1::OperationRecord {
        downstream_operation_id: expected.operation_id.clone(),
        method_key: expected.method_key.clone(),
        scope: expected.scope,
        payload_fingerprint: expected.payload_fingerprint.clone(),
        contract_fingerprint: expected.contract_fingerprint.clone(),
        affected_issue_ids: expected.affected_issue_ids.clone(),
        entity_revisions: expected
            .entity_revisions
            .iter()
            .map(|revision| v1::OperationEntityRevision {
                issue_id: revision.issue_id.clone(),
                backend_id: revision.backend_id.clone(),
                resulting_revision: revision.resulting_revision,
            })
            .collect(),
        commit_durability: v1::OperationCommitDurability::Durable as i32,
        result_replayability: v1::OperationResultReplayability::Replayable as i32,
        result_snapshot: Some(v1::OperationResultSnapshot::default()),
        fingerprint_input: Some(v1::OperationFingerprintInput {
            downstream_operation_id: expected.operation_id.clone(),
            method_key: expected.method_key.clone(),
            scope: expected.scope,
            payload_fingerprint: expected.payload_fingerprint.clone(),
        }),
        ..Default::default()
    }
}

#[test]
fn recovery_decision_confirms_an_exact_marker() {
    let expected = expected();

    assert_eq!(
        recovery_decision(Some(&marker(&expected)), &expected),
        RecoveryDecision::Confirmed
    );
}

#[test]
fn recovery_decision_keeps_missing_marker_unconfirmed() {
    let expected = expected();

    assert_eq!(
        recovery_decision(None, &expected),
        RecoveryDecision::Unconfirmed
    );
}

#[test]
fn recovery_decision_manual_resolves_a_mismatched_marker() {
    let expected = expected();
    let mut marker = marker(&expected);
    marker.payload_fingerprint = "different".into();

    assert_eq!(
        recovery_decision(Some(&marker), &expected),
        RecoveryDecision::ManualResolution
    );
}

#[test]
fn recovery_error_classification_does_not_retry_permanent_input() {
    let error = crate::proto_types::manager_v1::ManagerError {
        code: crate::proto_types::manager_v1::ManagerErrorCode::SchemaMismatch as i32,
        message: "request schema is incompatible".into(),
        retryable: false,
        detail: None,
    };
    assert!(matches!(
        classify_ownership_recovery_manager_error(Some(&error), 3, "fallback"),
        TrackerError::OwnershipRecoveryPermanent { ref classification, .. }
            if classification == "CONTRACT_MISMATCH"
    ));
}

#[test]
fn recovery_error_classification_rejects_retryable_backend_without_proof() {
    let error = crate::proto_types::manager_v1::ManagerError {
        code: crate::proto_types::manager_v1::ManagerErrorCode::BackendUnavailable as i32,
        message: "canonical store unavailable".into(),
        retryable: true,
        detail: None,
    };
    assert!(matches!(
        classify_ownership_recovery_manager_error(Some(&error), 3, "fallback"),
        TrackerError::OwnershipRecoveryPermanent { .. }
    ));
}

#[test]
fn recovery_error_classification_preserves_revision_conflict_detail() {
    let error = crate::proto_types::manager_v1::ManagerError {
        code: crate::proto_types::manager_v1::ManagerErrorCode::RevisionConflict as i32,
        message: "revision changed".into(),
        detail: Some(
            crate::proto_types::manager_v1::manager_error::Detail::RevisionConflict(
                crate::proto_types::manager_v1::RevisionConflict {
                    expected_revision: 3,
                    current_revision: 8,
                },
            ),
        ),
        ..Default::default()
    };
    assert!(matches!(
        classify_ownership_recovery_manager_error(Some(&error), 3, "fallback"),
        TrackerError::RevisionConflict {
            expected: 3,
            actual: 8
        }
    ));
}

#[test]
fn recovery_error_classification_allows_only_proven_not_sent_exhaustion() {
    let error = crate::proto_types::manager_v1::ManagerError {
        code: crate::proto_types::manager_v1::ManagerErrorCode::RetryExhausted as i32,
        message: "all attempts were not sent".into(),
        retryable: true,
        detail: Some(
            crate::proto_types::manager_v1::manager_error::Detail::OperationDiagnosis(
                crate::proto_types::manager_v1::OperationDiagnosis {
                    classification: "NOT_SENT_RETRY_EXHAUSTED".into(),
                    resolution_state: "ALL_ATTEMPTS_NOT_SENT".into(),
                    replayability: "NEW_READMISSION_ONLY".into(),
                    ..Default::default()
                },
            ),
        ),
    };
    assert!(matches!(
        classify_ownership_recovery_manager_error(Some(&error), 3, "fallback"),
        TrackerError::OwnershipRecoveryRetryable(_)
    ));
}

#[test]
fn recovery_runtime_errors_remain_unconfirmed() {
    assert!(matches!(
        classify_ownership_recovery_runtime_error("sqlite timeout".into()),
        TrackerError::ResultUnconfirmed
    ));
}

#[test]
fn ownership_maintenance_errors_keep_the_runner_degraded() {
    assert!(ownership_maintenance_failed(&Err("scan failed".into())));
    assert!(ownership_maintenance_failed(&Ok(
        super::ownership_maintenance::MaintenanceStats {
            failed: true,
            ..Default::default()
        }
    )));
    assert!(!ownership_maintenance_failed(&Ok(Default::default())));
}
