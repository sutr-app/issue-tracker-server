//! Conversion of Tracker error codes to the Manager error contract.
//!
//! Error detail extraction remains in the dispatch layer because it depends
//! on southbound precondition evidence.  The code mapping itself is shared by
//! synchronous capability responses and durable outbox responses.

use crate::{proto_types::manager_v1 as manager, v1};

pub(crate) fn manager_error_code(error: &v1::IssueTrackerError) -> manager::ManagerErrorCode {
    match v1::ErrorCode::try_from(error.code) {
        Ok(v1::ErrorCode::IssueNotFound) => manager::ManagerErrorCode::IssueNotFound,
        Ok(v1::ErrorCode::RevisionConflict) => manager::ManagerErrorCode::RevisionConflict,
        Ok(v1::ErrorCode::IdempotencyConflict) => manager::ManagerErrorCode::IdempotencyConflict,
        Ok(v1::ErrorCode::ResultUnconfirmed) => manager::ManagerErrorCode::ResultUnconfirmed,
        Ok(v1::ErrorCode::BackendUnavailable) => manager::ManagerErrorCode::BackendUnavailable,
        Ok(v1::ErrorCode::SecretDetected) => manager::ManagerErrorCode::ManagerSecretDetected,
        Ok(v1::ErrorCode::InvalidArgument) => manager::ManagerErrorCode::ManagerInvalidArgument,
        Ok(v1::ErrorCode::InvalidStateTransition) => {
            manager::ManagerErrorCode::InvalidStateTransition
        }
        _ => manager::ManagerErrorCode::PreconditionFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_tracker_codes_without_message_parsing() {
        let error = v1::IssueTrackerError {
            code: v1::ErrorCode::InvalidArgument as i32,
            message: "invalid".into(),
            ..Default::default()
        };
        assert_eq!(
            manager_error_code(&error),
            manager::ManagerErrorCode::ManagerInvalidArgument
        );
    }
}
