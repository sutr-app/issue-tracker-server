#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ManagerValidationError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("{field} exceeds the maximum length")]
    TooLong { field: &'static str },
    #[error("{field} contains a control character")]
    ControlCharacter { field: &'static str },
}

#[cfg(test)]
pub(crate) fn validate_identifier(
    field: &'static str,
    value: &str,
) -> Result<(), ManagerValidationError> {
    if value.is_empty() {
        return Err(ManagerValidationError::Empty { field });
    }
    if value.len() > 4096 {
        return Err(ManagerValidationError::TooLong { field });
    }
    if value.chars().any(char::is_control) {
        return Err(ManagerValidationError::ControlCharacter { field });
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn validate_idempotency_key(value: &str) -> Result<(), ManagerValidationError> {
    validate_identifier("idempotency_key", value)
}

#[cfg(test)]
pub(crate) fn validate_canonical_target(value: &str) -> Result<(), ManagerValidationError> {
    validate_identifier("canonical_target", value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_normal_manager_identifiers() {
        validate_idempotency_key("chat:thread-1:create").expect("valid key");
        validate_canonical_target("candidate/c-1").expect("valid target");
    }

    #[test]
    fn rejects_empty_control_and_oversized_identifiers() {
        assert_eq!(
            validate_idempotency_key(""),
            Err(ManagerValidationError::Empty {
                field: "idempotency_key"
            })
        );
        assert_eq!(
            validate_canonical_target("bad\nid"),
            Err(ManagerValidationError::ControlCharacter {
                field: "canonical_target"
            })
        );
        assert_eq!(
            validate_idempotency_key(&"x".repeat(4097)),
            Err(ManagerValidationError::TooLong {
                field: "idempotency_key"
            })
        );
    }
}
