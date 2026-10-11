//! Pure outbox state machine for backend-changing manager operations.
//!
//! The outbox stores only a reference and digest for a manager snapshot.  The
//! snapshot payload itself belongs to the manager store, and claim lease
//! handles from the canonical issue backend are never accepted here.

use thiserror::Error;
use uuid::Uuid;

use crate::proto_types::manager_v1 as pb;

use super::operations::{DispatchKind, classify_method};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OutboxError {
    #[error("outbox field {0} must not be empty")]
    EmptyField(&'static str),
    #[error("outbox entry has invalid status {0}")]
    InvalidStatus(i32),
    #[error("outbox transition is not allowed: {from} -> {to}")]
    InvalidTransition { from: String, to: String },
    #[error("outbox lease is missing or does not match")]
    LeaseMismatch,
    #[error("outbox lease has expired")]
    LeaseExpired,
    #[error("outbox lease duration overflows timestamp")]
    LeaseOverflow,
    #[error("outbox lease duration must be positive")]
    InvalidLeaseDuration,
    #[error("manager-only operation must not create an outbox entry")]
    ManagerOnlyOperation,
}

/// A leased entry returned to the dispatcher.  The lease ID is an outbox
/// coordination identifier, not a canonical Issue claim handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxLease {
    pub entry: pb::OutboxEntry,
    pub lease_id: String,
}

/// Build an outbox record for a backend operation.  Only a snapshot reference
/// and digest are accepted, so raw request/result bytes cannot enter this
/// queue accidentally.
pub fn new_entry(
    outbox_id: impl Into<String>,
    operation: &pb::DurableOperation,
    topic: impl Into<String>,
    payload_snapshot_ref: impl Into<String>,
    payload_digest: impl Into<String>,
    now_millis: i64,
) -> Result<pb::OutboxEntry, OutboxError> {
    let outbox_id = outbox_id.into();
    let topic = topic.into();
    let payload_snapshot_ref = payload_snapshot_ref.into();
    let payload_digest = payload_digest.into();
    for (field, value) in [
        ("outbox_id", &outbox_id),
        ("operation_id", &operation.operation_id),
        ("topic", &topic),
        ("payload_snapshot_ref", &payload_snapshot_ref),
        ("payload_digest", &payload_digest),
    ] {
        if value.is_empty() {
            return Err(OutboxError::EmptyField(field));
        }
    }
    // Administrative actions are admitted only through the trusted primary
    // consumer, but their Tracker write still needs the same durable outbox
    // lifecycle as ordinary backend mutations.  Keep the method classified
    // as ManagerOnly for the public manager gateway and allow it here solely
    // because the caller already crossed that private admission boundary.
    let is_private_admin_action = operation.method == "manager_apply_issue_admin_action";
    if classify_method(&operation.method).ok() != Some(DispatchKind::Backend)
        && !is_private_admin_action
    {
        return Err(OutboxError::ManagerOnlyOperation);
    }
    if operation.generation == 0 {
        return Err(OutboxError::EmptyField("generation"));
    }
    Ok(pb::OutboxEntry {
        outbox_id,
        operation_id: operation.operation_id.clone(),
        topic,
        status: pb::OutboxStatus::Ready as i32,
        payload_snapshot_ref,
        payload_digest,
        generation: operation.generation,
        attempt: 0,
        lease_id: None,
        lease_expires_at: None,
        next_attempt_at: None,
        dispatched_at: None,
        acknowledged_at: None,
        error_code: pb::ManagerErrorCode::ManagerErrorUnspecified as i32,
        created_at: Some(pb::UtcInstant {
            unix_epoch_millis: now_millis,
        }),
    })
}

/// Lease the next ready entry, reclaiming an expired lease first.
pub fn lease(
    entry: &mut pb::OutboxEntry,
    now_millis: i64,
    lease_millis: u64,
    lease_id: Option<String>,
) -> Result<OutboxLease, OutboxError> {
    if lease_millis == 0 {
        return Err(OutboxError::InvalidLeaseDuration);
    }
    let status = status(entry)?;
    if status == pb::OutboxStatus::Leased {
        let expires = entry
            .lease_expires_at
            .as_ref()
            .ok_or(OutboxError::LeaseMismatch)?
            .unix_epoch_millis;
        if expires > now_millis {
            return Err(OutboxError::InvalidTransition {
                from: status.as_str_name().to_owned(),
                to: "LEASED".into(),
            });
        }
        // An expired lease is safe to reclaim because the old token is
        // discarded before a new dispatch attempt is issued.
        clear_lease(entry);
    } else if status != pb::OutboxStatus::Ready {
        return Err(OutboxError::InvalidTransition {
            from: status.as_str_name().to_owned(),
            to: pb::OutboxStatus::Leased.as_str_name().to_owned(),
        });
    }
    if let Some(next) = &entry.next_attempt_at {
        if next.unix_epoch_millis > now_millis {
            return Err(OutboxError::InvalidTransition {
                from: pb::OutboxStatus::Ready.as_str_name().to_owned(),
                to: "NOT_DUE".into(),
            });
        }
    }
    let lease_id = lease_id.unwrap_or_else(|| format!("outbox-lease-{}", Uuid::new_v4()));
    if lease_id.is_empty() {
        return Err(OutboxError::EmptyField("lease_id"));
    }
    let duration = i64::try_from(lease_millis).map_err(|_| OutboxError::LeaseOverflow)?;
    let expires = now_millis
        .checked_add(duration)
        .ok_or(OutboxError::LeaseOverflow)?;
    entry.status = pb::OutboxStatus::Leased as i32;
    entry.lease_id = Some(lease_id.clone());
    entry.lease_expires_at = Some(pb::UtcInstant {
        unix_epoch_millis: expires,
    });
    entry.attempt = entry.attempt.saturating_add(1);
    Ok(OutboxLease {
        entry: entry.clone(),
        lease_id,
    })
}

pub fn mark_dispatched(
    entry: &mut pb::OutboxEntry,
    lease_id: &str,
    now_millis: i64,
) -> Result<(), OutboxError> {
    verify_active_lease(entry, lease_id, now_millis)?;
    if status(entry)? != pb::OutboxStatus::Leased {
        return Err(OutboxError::InvalidTransition {
            from: status(entry)?.as_str_name().to_owned(),
            to: pb::OutboxStatus::Dispatched.as_str_name().to_owned(),
        });
    }
    entry.status = pb::OutboxStatus::Dispatched as i32;
    entry.dispatched_at = Some(pb::UtcInstant {
        unix_epoch_millis: now_millis,
    });
    Ok(())
}

pub fn acknowledge(
    entry: &mut pb::OutboxEntry,
    lease_id: &str,
    now_millis: i64,
) -> Result<(), OutboxError> {
    verify_active_lease(entry, lease_id, now_millis)?;
    if status(entry)? != pb::OutboxStatus::Dispatched {
        return Err(OutboxError::InvalidTransition {
            from: status(entry)?.as_str_name().to_owned(),
            to: pb::OutboxStatus::Acknowledged.as_str_name().to_owned(),
        });
    }
    entry.status = pb::OutboxStatus::Acknowledged as i32;
    entry.acknowledged_at = Some(pb::UtcInstant {
        unix_epoch_millis: now_millis,
    });
    clear_lease(entry);
    Ok(())
}

pub fn retry(
    entry: &mut pb::OutboxEntry,
    lease_id: &str,
    next_attempt_at_millis: i64,
    error_code: pb::ManagerErrorCode,
    now_millis: i64,
) -> Result<(), OutboxError> {
    verify_active_lease(entry, lease_id, now_millis)?;
    let current = status(entry)?;
    if !matches!(
        current,
        pb::OutboxStatus::Leased | pb::OutboxStatus::Dispatched
    ) {
        return Err(OutboxError::InvalidTransition {
            from: current.as_str_name().to_owned(),
            to: pb::OutboxStatus::Ready.as_str_name().to_owned(),
        });
    }
    entry.status = pb::OutboxStatus::Ready as i32;
    entry.next_attempt_at = Some(pb::UtcInstant {
        unix_epoch_millis: next_attempt_at_millis,
    });
    entry.error_code = error_code as i32;
    clear_lease(entry);
    Ok(())
}

pub fn dead_letter(
    entry: &mut pb::OutboxEntry,
    lease_id: &str,
    error_code: pb::ManagerErrorCode,
    now_millis: i64,
) -> Result<(), OutboxError> {
    verify_active_lease(entry, lease_id, now_millis)?;
    let current = status(entry)?;
    if !matches!(
        current,
        pb::OutboxStatus::Leased | pb::OutboxStatus::Dispatched
    ) {
        return Err(OutboxError::InvalidTransition {
            from: current.as_str_name().to_owned(),
            to: pb::OutboxStatus::DeadLetter.as_str_name().to_owned(),
        });
    }
    entry.status = pb::OutboxStatus::DeadLetter as i32;
    entry.error_code = error_code as i32;
    clear_lease(entry);
    Ok(())
}

fn status(entry: &pb::OutboxEntry) -> Result<pb::OutboxStatus, OutboxError> {
    pb::OutboxStatus::try_from(entry.status).map_err(|_| OutboxError::InvalidStatus(entry.status))
}

fn clear_lease(entry: &mut pb::OutboxEntry) {
    entry.lease_id = None;
    entry.lease_expires_at = None;
}

fn verify_active_lease(
    entry: &pb::OutboxEntry,
    lease_id: &str,
    now_millis: i64,
) -> Result<(), OutboxError> {
    let expected = entry
        .lease_id
        .as_deref()
        .ok_or(OutboxError::LeaseMismatch)?;
    if expected != lease_id {
        return Err(OutboxError::LeaseMismatch);
    }
    let expires = entry
        .lease_expires_at
        .as_ref()
        .ok_or(OutboxError::LeaseMismatch)?
        .unix_epoch_millis;
    if expires <= now_millis {
        return Err(OutboxError::LeaseExpired);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::operations::{IdempotencyScope, NewOperation, OperationScope};

    fn operation() -> pb::DurableOperation {
        NewOperation {
            operation_id: "op-1".into(),
            scope: Some(OperationScope::Issue("issue-1".into())),
            method: "manager_create_issue".into(),
            idempotency_scope: IdempotencyScope::new(
                "contract",
                "root",
                "manager_create_issue",
                "issue:issue-1",
                "key",
            )
            .unwrap(),
            request_digest: "request-digest".into(),
            request_schema: "CreateIssueArgs".into(),
            downstream_operation_id: Some("downstream-1".into()),
            generation: 1,
            manager_sequence: 1,
            causal_parent: None,
            affects_ready: true,
            sync_subject: Some("issue-1".into()),
            affected_issue_ids: vec!["issue-1".into()],
            actor_audit: None,
            contract_fingerprint: "contract".into(),
            schema_fingerprint: "schema".into(),
            correlation_id: "corr".into(),
            now_millis: 100,
            replayable: true,
        }
        .build()
        .unwrap()
    }

    #[test]
    fn outbox_lease_dispatch_acknowledge_has_monotonic_state_and_no_payload() {
        let operation = operation();
        let mut entry = new_entry(
            "outbox-1",
            &operation,
            "issue.create",
            "snapshot-1",
            "digest-1",
            100,
        )
        .unwrap();
        let leased = lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
        assert_eq!(leased.lease_id, "lease-1");
        assert!(entry.payload_snapshot_ref.contains("snapshot"));
        assert!(!entry.payload_digest.is_empty());
        assert!(mark_dispatched(&mut entry, "lease-1", 150).is_ok());
        assert!(acknowledge(&mut entry, "lease-1", 160).is_ok());
        assert_eq!(entry.status, pb::OutboxStatus::Acknowledged as i32);
        assert!(entry.lease_id.is_none());
    }

    #[test]
    fn manager_only_operations_are_not_admitted_to_backend_outbox() {
        let mut operation = operation();
        operation.method = "manager_create_candidate".into();
        assert_eq!(
            new_entry(
                "outbox-1",
                &operation,
                "candidate.create",
                "snapshot",
                "digest",
                100
            ),
            Err(OutboxError::ManagerOnlyOperation)
        );
    }

    #[test]
    fn expired_or_wrong_lease_cannot_acknowledge_and_expired_lease_can_reclaim() {
        let operation = operation();
        let mut entry = new_entry(
            "outbox-1",
            &operation,
            "issue.create",
            "snapshot-1",
            "digest-1",
            100,
        )
        .unwrap();
        lease(&mut entry, 100, 10, Some("lease-1".into())).unwrap();
        assert_eq!(
            mark_dispatched(&mut entry, "wrong", 105),
            Err(OutboxError::LeaseMismatch)
        );
        let reclaimed = lease(&mut entry, 110, 10, Some("lease-2".into())).unwrap();
        assert_eq!(reclaimed.lease_id, "lease-2");
        assert_eq!(entry.attempt, 2);
        assert_eq!(
            acknowledge(&mut entry, "lease-1", 111),
            Err(OutboxError::LeaseMismatch)
        );
    }

    #[test]
    fn retry_and_dead_letter_clear_lease_and_preserve_only_error_metadata() {
        let operation = operation();
        let mut entry = new_entry(
            "outbox-1",
            &operation,
            "issue.create",
            "snapshot-1",
            "digest-1",
            100,
        )
        .unwrap();
        lease(&mut entry, 100, 100, Some("lease-1".into())).unwrap();
        retry(
            &mut entry,
            "lease-1",
            250,
            pb::ManagerErrorCode::BackendUnavailable,
            150,
        )
        .unwrap();
        assert_eq!(entry.status, pb::OutboxStatus::Ready as i32);
        assert!(entry.lease_id.is_none());
        assert_eq!(
            entry.error_code,
            pb::ManagerErrorCode::BackendUnavailable as i32
        );
        lease(&mut entry, 250, 100, Some("lease-2".into())).unwrap();
        dead_letter(
            &mut entry,
            "lease-2",
            pb::ManagerErrorCode::RetryExhausted,
            260,
        )
        .unwrap();
        assert_eq!(entry.status, pb::OutboxStatus::DeadLetter as i32);
        assert!(entry.lease_id.is_none());
    }
}
