use super::{conversion::instant, *};

pub(crate) fn tracker_error(error: TrackerError) -> v1::IssueTrackerError {
    let (code, retryable, detail) = match error {
        TrackerError::NotFound(_) => (v1::ErrorCode::IssueNotFound, false, None),
        TrackerError::RevisionConflict { expected, actual } => (
            v1::ErrorCode::RevisionConflict,
            true,
            Some(v1::issue_tracker_error::Detail::RevisionConflict(
                v1::RevisionConflict {
                    expected_revision: expected,
                    current_revision: actual,
                    refetch_allowed: true,
                    ..Default::default()
                },
            )),
        ),
        TrackerError::InvalidLease {
            lease_expires_at,
            observed_revision,
        } => (
            v1::ErrorCode::InvalidLease,
            false,
            Some(v1::issue_tracker_error::Detail::LeasePrecondition(
                v1::LeasePrecondition {
                    classification: "ACTIVE".into(),
                    lease_expires_at: lease_expires_at.map(instant),
                    observed_revision: observed_revision.unwrap_or_default(),
                    ..Default::default()
                },
            )),
        ),
        TrackerError::LeaseExpired {
            lease_expires_at,
            observed_revision,
        } => (
            v1::ErrorCode::LeaseExpired,
            false,
            Some(v1::issue_tracker_error::Detail::LeasePrecondition(
                v1::LeasePrecondition {
                    classification: "EXPIRED".into(),
                    lease_expires_at: lease_expires_at.map(instant),
                    observed_revision: observed_revision.unwrap_or_default(),
                    ..Default::default()
                },
            )),
        ),
        TrackerError::LeaseInconsistent {
            ref invalid_attributes,
            lease_expires_at,
            observed_revision,
        } => (
            v1::ErrorCode::InvalidLease,
            false,
            Some(v1::issue_tracker_error::Detail::LeasePrecondition(
                v1::LeasePrecondition {
                    classification: "INCONSISTENT".into(),
                    invalid_attributes: invalid_attributes.clone(),
                    lease_expires_at: lease_expires_at.map(instant),
                    observed_revision: observed_revision.unwrap_or_default(),
                },
            )),
        ),
        TrackerError::Blocked {
            ref open_blocker_issue_ids,
        } => (
            v1::ErrorCode::BlockedByOpenIssue,
            false,
            Some(v1::issue_tracker_error::Detail::BlockerConflict(
                v1::BlockerConflict {
                    open_blocker_issue_ids: open_blocker_issue_ids
                        .iter()
                        .copied()
                        .map(|id| id.to_string())
                        .collect(),
                },
            )),
        ),
        TrackerError::IdempotencyConflict => (v1::ErrorCode::IdempotencyConflict, false, None),
        TrackerError::OperationFenced => (v1::ErrorCode::InvalidStateTransition, false, None),
        TrackerError::ResultUnconfirmed => (v1::ErrorCode::ResultUnconfirmed, true, None),
        TrackerError::InvalidPageToken => (v1::ErrorCode::InvalidPageToken, false, None),
        TrackerError::PageTokenExpired {
            token_generation,
            current_generation,
        } => (
            v1::ErrorCode::PageTokenExpired,
            true,
            Some(v1::issue_tracker_error::Detail::PageTokenExpired(
                v1::PageTokenExpired {
                    snapshot_id: format!("read-{token_generation}"),
                    token_read_generation: token_generation,
                    current_read_generation: current_generation,
                    ..Default::default()
                },
            )),
        ),
        TrackerError::SecretRejected => (v1::ErrorCode::SecretDetected, false, None),
        TrackerError::RootLocked => (v1::ErrorCode::RootLocked, true, None),
        TrackerError::Unavailable => (v1::ErrorCode::BackendUnavailable, true, None),
        TrackerError::InvalidRelation(_) => (v1::ErrorCode::RelationCycle, false, None),
        TrackerError::InvalidState(_) => (v1::ErrorCode::InvalidArgument, false, None),
        TrackerError::InvalidStateTransition(_) => {
            (v1::ErrorCode::InvalidStateTransition, false, None)
        }
        TrackerError::Backup(_) => (v1::ErrorCode::BackupInvalid, false, None),
        TrackerError::Backend(_) => (v1::ErrorCode::BackendUnavailable, true, None),
        TrackerError::OwnershipRecoveryRetryable(_) => {
            (v1::ErrorCode::BackendUnavailable, true, None)
        }
        TrackerError::OwnershipRecoveryPermanent { .. } => {
            (v1::ErrorCode::InvalidStateTransition, false, None)
        }
    };
    v1::IssueTrackerError {
        code: code as i32,
        message: error.to_string(),
        retryable,
        detail,
    }
}

pub(crate) fn invalid_error(message: impl Into<String>) -> v1::IssueTrackerError {
    v1::IssueTrackerError {
        code: v1::ErrorCode::InvalidArgument as i32,
        message: message.into(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_precondition_exposes_safe_observation() {
        let error = tracker_error(TrackerError::LeaseExpired {
            lease_expires_at: Some(42),
            observed_revision: Some(7),
        });
        let detail = error.detail.expect("lease detail");
        let v1::issue_tracker_error::Detail::LeasePrecondition(precondition) = detail else {
            panic!("expected lease precondition detail");
        };
        assert_eq!(precondition.classification, "EXPIRED");
        assert_eq!(precondition.observed_revision, 7);
        assert_eq!(
            precondition
                .lease_expires_at
                .expect("expiry")
                .unix_epoch_millis,
            42_000
        );
    }
}
