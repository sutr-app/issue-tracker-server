//! Error types returned by tracker operations.

use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum TrackerError {
    #[error("TaskChampion error: {0}")]
    Backend(#[from] taskchampion::Error),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(String),
    #[error("invalid relation: {0}")]
    InvalidRelation(String),
    #[error("issue not found: {0}")]
    NotFound(Uuid),
    #[error("revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("lease is invalid or expired")]
    InvalidLease {
        lease_expires_at: Option<u64>,
        observed_revision: Option<u64>,
    },
    #[error("lease has expired")]
    LeaseExpired {
        lease_expires_at: Option<u64>,
        observed_revision: Option<u64>,
    },
    #[error("lease attributes are inconsistent")]
    LeaseInconsistent {
        invalid_attributes: Vec<String>,
        lease_expires_at: Option<u64>,
        observed_revision: Option<u64>,
    },
    #[error("operation payload contains a secret-like value")]
    SecretRejected,
    #[error("issue is blocked by an unfinished dependency")]
    Blocked { open_blocker_issue_ids: Vec<Uuid> },
    #[error("data root is already locked")]
    RootLocked,
    #[error("backup error: {0}")]
    Backup(String),
    #[error("idempotency payload conflict")]
    IdempotencyConflict,
    #[error("operation has been permanently fenced by a manual resolution")]
    OperationFenced,
    #[error("operation committed but its result cannot be replayed safely")]
    ResultUnconfirmed,
    #[error("page token is invalid")]
    InvalidPageToken,
    #[error(
        "page token expired: token generation {token_generation}, current generation {current_generation}"
    )]
    PageTokenExpired {
        token_generation: u64,
        current_generation: u64,
    },
    #[error("tracker is unavailable after restore recovery failure")]
    Unavailable,
    /// A maintenance attempt did not reach the Tracker commit boundary and
    /// may be retried with the same durable operation.  This is deliberately
    /// distinct from ordinary backend errors so the resident scanner cannot
    /// accidentally retry a permanent contract or data-integrity failure.
    #[error("ownership recovery can be retried: {0}")]
    OwnershipRecoveryRetryable(String),
    /// A maintenance attempt reached a definitive, non-retryable outcome.
    /// The classification is persisted by the maintenance projection and
    /// requires an operator or a new readmission rather than blind backoff.
    #[error("ownership recovery is permanently blocked ({classification}): {message}")]
    OwnershipRecoveryPermanent {
        classification: String,
        message: String,
    },
}
