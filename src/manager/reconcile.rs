//! Backend operation reconciliation at the manager/backend boundary.
//!
//! This module owns only the pure state transition after a southbound
//! dispatcher has attempted a write.  It never stores canonical Issue data or
//! an ephemeral claim lease handle.

use super::{
    backend_adapter::{MarkerExpectation, MarkerMismatch, verify_operation_marker},
    operations::{OperationError, transition_operation},
};
use crate::{proto_types::manager_v1 as pb, v1};
use thiserror::Error;

/// Why the manager selected the resulting durable operation state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReconcileReason {
    MarkerConfirmed,
    DeliveryUnknown,
    MarkerMismatch(MarkerMismatch),
    MarkerExpectationMissing,
}

/// Result of reconciling one backend delivery attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReconcileResult {
    pub state: pb::DurableOperationState,
    pub reason: ReconcileReason,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ReconcileError {
    #[error("operation is not in a reconcilable state: {0}")]
    TerminalState(String),
    #[error("operation has an invalid state: {0}")]
    InvalidState(i32),
    #[error("operation transition failed: {0}")]
    Transition(#[from] OperationError),
}

/// Reconcile the marker returned by the canonical backend.
///
/// A missing marker never implies success.  A marker mismatch is stronger
/// than an absent marker: it means that a durable backend record exists but
/// does not describe this manager operation, so automatic retry is unsafe and
/// the operation is moved to `MANUAL_RESOLUTION`.
pub(crate) fn reconcile_marker(
    operation: &mut pb::DurableOperation,
    marker: Option<&v1::OperationRecord>,
    expected: &MarkerExpectation,
    now_millis: i64,
) -> Result<ReconcileResult, ReconcileError> {
    ensure_reconcilable(operation)?;
    match marker {
        None => {
            if current_state(operation)? == pb::DurableOperationState::Pending {
                transition_operation(
                    operation,
                    pb::DurableOperationState::Unconfirmed,
                    None,
                    now_millis,
                )?;
            }
            Ok(ReconcileResult {
                state: pb::DurableOperationState::Unconfirmed,
                reason: ReconcileReason::DeliveryUnknown,
            })
        }
        Some(marker) => match verify_operation_marker(marker, expected) {
            Ok(()) => {
                transition_operation(
                    operation,
                    pb::DurableOperationState::Committed,
                    None,
                    now_millis,
                )?;
                Ok(ReconcileResult {
                    state: pb::DurableOperationState::Committed,
                    reason: ReconcileReason::MarkerConfirmed,
                })
            }
            Err(mismatch) => {
                if current_state(operation)? == pb::DurableOperationState::Pending {
                    transition_operation(
                        operation,
                        pb::DurableOperationState::Unconfirmed,
                        None,
                        now_millis,
                    )?;
                }
                transition_operation(
                    operation,
                    pb::DurableOperationState::ManualResolution,
                    None,
                    now_millis,
                )?;
                Ok(ReconcileResult {
                    state: pb::DurableOperationState::ManualResolution,
                    reason: ReconcileReason::MarkerMismatch(mismatch),
                })
            }
        },
    }
}

/// Reconcile an operation found in `UNCONFIRMED` during startup recovery.
///
/// A missing backend marker is intentionally left unconfirmed: the manager
/// has no proof that the write committed, but the acknowledged outbox entry
/// must never be sent again.  Missing marker expectations are durable
/// metadata corruption and therefore require manual resolution.
pub(crate) fn reconcile_recovered_marker(
    operation: &mut pb::DurableOperation,
    marker: Option<&v1::OperationRecord>,
    expected: Option<&MarkerExpectation>,
    now_millis: i64,
) -> Result<ReconcileResult, ReconcileError> {
    ensure_reconcilable(operation)?;
    let Some(expected) = expected else {
        transition_operation(
            operation,
            pb::DurableOperationState::ManualResolution,
            None,
            now_millis,
        )?;
        return Ok(ReconcileResult {
            state: pb::DurableOperationState::ManualResolution,
            reason: ReconcileReason::MarkerExpectationMissing,
        });
    };
    reconcile_marker(operation, marker, expected, now_millis)
}

fn ensure_reconcilable(operation: &pb::DurableOperation) -> Result<(), ReconcileError> {
    match current_state(operation)? {
        pb::DurableOperationState::Pending | pb::DurableOperationState::Unconfirmed => Ok(()),
        state => Err(ReconcileError::TerminalState(
            state.as_str_name().to_owned(),
        )),
    }
}

fn current_state(
    operation: &pb::DurableOperation,
) -> Result<pb::DurableOperationState, ReconcileError> {
    pb::DurableOperationState::try_from(operation.state)
        .map_err(|_| ReconcileError::InvalidState(operation.state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        manager::{
            backend_adapter::*,
            operations::{NewOperation, transition_operation},
        },
        proto_types::manager_v1 as pb,
        v1,
    };

    fn operation(replayable: bool) -> pb::DurableOperation {
        NewOperation {
            operation_id: "manager-op-1".into(),
            scope: Some(crate::manager::operations::OperationScope::Issue(
                "issue-1".into(),
            )),
            method: "manager_update_issue".into(),
            idempotency_scope: crate::manager::operations::IdempotencyScope::new(
                "backend-contract",
                "root",
                "manager_update_issue",
                "issue:issue-1",
                "key",
            )
            .unwrap(),
            request_digest: "request-digest".into(),
            request_schema: "UpdateIssueArgs".into(),
            downstream_operation_id: Some("downstream-op-1".into()),
            generation: 1,
            manager_sequence: 1,
            causal_parent: None,
            affects_ready: true,
            sync_subject: Some("issue-1".into()),
            affected_issue_ids: vec!["issue-1".into()],
            actor_audit: None,
            contract_fingerprint: "backend-contract".into(),
            schema_fingerprint: "backend-schema".into(),
            correlation_id: "correlation-1".into(),
            now_millis: 100,
            replayable,
        }
        .build()
        .unwrap()
    }

    fn expectation(replayable: bool) -> MarkerExpectation {
        MarkerExpectation {
            operation_id: "downstream-op-1".into(),
            method_key: "manager_update_issue".into(),
            scope: v1::OperationScope::Issue as i32,
            payload_fingerprint: "payload-digest".into(),
            contract_fingerprint: "backend-contract".into(),
            affected_issue_ids: vec!["issue-1".into()],
            entity_revisions: vec![EntityRevisionExpectation {
                issue_id: "issue-1".into(),
                backend_id: "backend-issue-1".into(),
                resulting_revision: 2,
            }],
            result_replayable: replayable,
        }
    }

    fn marker(expected: &MarkerExpectation) -> v1::OperationRecord {
        v1::OperationRecord {
            downstream_operation_id: expected.operation_id.clone(),
            affected_issue_ids: expected.affected_issue_ids.clone(),
            payload_fingerprint: expected.payload_fingerprint.clone(),
            contract_fingerprint: expected.contract_fingerprint.clone(),
            scope: expected.scope,
            method_key: expected.method_key.clone(),
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
            result_replayability: if expected.result_replayable {
                v1::OperationResultReplayability::Replayable as i32
            } else {
                v1::OperationResultReplayability::NotReplayable as i32
            },
            result_snapshot: expected
                .result_replayable
                .then_some(v1::OperationResultSnapshot::default()),
            fingerprint_input: Some(v1::OperationFingerprintInput {
                downstream_operation_id: expected.operation_id.clone(),
                method_key: expected.method_key.clone(),
                scope: expected.scope,
                payload_fingerprint: expected.payload_fingerprint.clone(),
            }),
            ..Default::default()
        }
    }

    fn pending(mut operation: pb::DurableOperation) -> pb::DurableOperation {
        transition_operation(
            &mut operation,
            pb::DurableOperationState::Pending,
            None,
            110,
        )
        .unwrap();
        operation
    }

    #[test]
    fn exact_marker_confirms_pending_operation() {
        let expected = expectation(true);
        let mut operation = pending(operation(true));

        let result = reconcile_marker(&mut operation, Some(&marker(&expected)), &expected, 120)
            .expect("marker should confirm");

        assert_eq!(result.state, pb::DurableOperationState::Committed);
        assert_eq!(operation.state, pb::DurableOperationState::Committed as i32);
        assert!(!operation.resolution_required);
    }

    #[test]
    fn missing_marker_is_unconfirmed_without_guessing_success() {
        let expected = expectation(true);
        let mut operation = pending(operation(true));

        let result = reconcile_marker(&mut operation, None, &expected, 120)
            .expect("missing marker is a safe state");

        assert_eq!(result.state, pb::DurableOperationState::Unconfirmed);
        assert_eq!(
            operation.state,
            pb::DurableOperationState::Unconfirmed as i32
        );
        assert!(!operation.resolution_required);
    }

    #[test]
    fn mismatched_marker_requires_manual_resolution() {
        let expected = expectation(true);
        let mut operation = pending(operation(true));
        let mut marker = marker(&expected);
        marker.payload_fingerprint = "different".into();

        let result = reconcile_marker(&mut operation, Some(&marker), &expected, 120)
            .expect("mismatch should become a durable manual-resolution state");

        assert_eq!(result.state, pb::DurableOperationState::ManualResolution);
        assert_eq!(
            operation.state,
            pb::DurableOperationState::ManualResolution as i32
        );
        assert!(operation.resolution_required);
        assert!(matches!(result.reason, ReconcileReason::MarkerMismatch(_)));
    }

    #[test]
    fn unconfirmed_operation_can_be_confirmed_by_a_later_exact_marker() {
        // Non-replayable operations are limited to claim_issue. This test
        // exercises late marker confirmation for a normal replayable write.
        let expected = expectation(true);
        let mut operation = pending(operation(true));
        reconcile_marker(&mut operation, None, &expected, 120).unwrap();

        let result = reconcile_marker(&mut operation, Some(&marker(&expected)), &expected, 130)
            .expect("later marker should confirm");

        assert_eq!(result.state, pb::DurableOperationState::Committed);
        assert_eq!(operation.state, pb::DurableOperationState::Committed as i32);
    }

    #[test]
    fn manual_resolution_cannot_be_confirmed_by_automatic_reconcile() {
        let expected = expectation(true);
        let mut operation = pending(operation(true));
        let mut mismatched = marker(&expected);
        mismatched.method_key = "other-method".into();
        reconcile_marker(&mut operation, Some(&mismatched), &expected, 120).unwrap();

        let error = reconcile_marker(&mut operation, Some(&marker(&expected)), &expected, 130)
            .expect_err("human resolution is required after a marker conflict");
        assert!(matches!(error, ReconcileError::TerminalState(_)));
    }
}
