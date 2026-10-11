use crate::{model::IssueState, proto_types::v1};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TerminalState {
    Done,
    Canceled,
    Other,
}

impl From<v1::IssueState> for TerminalState {
    fn from(state: v1::IssueState) -> Self {
        match state {
            v1::IssueState::Done => Self::Done,
            v1::IssueState::Canceled => Self::Canceled,
            _ => Self::Other,
        }
    }
}

impl From<IssueState> for TerminalState {
    fn from(state: IssueState) -> Self {
        match state {
            IssueState::Done => Self::Done,
            IssueState::Canceled => Self::Canceled,
            _ => Self::Other,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TerminalReportKind {
    None,
    Completion { has_result: bool },
    Cancellation { has_classification: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TerminalReportValidationError {
    MissingMatchingReport,
    UnexpectedReport,
}

impl TerminalReportValidationError {
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::MissingMatchingReport => "terminal state requires its matching terminal report",
            Self::UnexpectedReport => "terminal report is only valid for done or canceled",
        }
    }
}

pub(crate) fn validate(
    state: Option<TerminalState>,
    report: TerminalReportKind,
) -> Result<(), TerminalReportValidationError> {
    match (state, report) {
        (Some(TerminalState::Done), TerminalReportKind::Completion { has_result: true })
        | (
            Some(TerminalState::Canceled),
            TerminalReportKind::Cancellation {
                has_classification: true,
            },
        ) => Ok(()),
        (Some(TerminalState::Done | TerminalState::Canceled), _) => {
            Err(TerminalReportValidationError::MissingMatchingReport)
        }
        (_, TerminalReportKind::None) => Ok(()),
        (Some(TerminalState::Other) | None, _) => {
            Err(TerminalReportValidationError::UnexpectedReport)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_only_matching_terminal_reports() {
        assert_eq!(
            validate(
                Some(TerminalState::Done),
                TerminalReportKind::Completion { has_result: true },
            ),
            Ok(())
        );
        assert_eq!(
            validate(
                Some(TerminalState::Canceled),
                TerminalReportKind::Cancellation {
                    has_classification: true,
                },
            ),
            Ok(())
        );
        assert_eq!(
            validate(Some(TerminalState::Other), TerminalReportKind::None),
            Ok(())
        );
        assert_eq!(
            validate(Some(TerminalState::Done), TerminalReportKind::None),
            Err(TerminalReportValidationError::MissingMatchingReport)
        );
        assert_eq!(
            validate(
                Some(TerminalState::Done),
                TerminalReportKind::Cancellation {
                    has_classification: true,
                },
            ),
            Err(TerminalReportValidationError::MissingMatchingReport)
        );
        assert_eq!(
            validate(
                Some(TerminalState::Other),
                TerminalReportKind::Completion { has_result: true },
            ),
            Err(TerminalReportValidationError::UnexpectedReport)
        );
    }
}
