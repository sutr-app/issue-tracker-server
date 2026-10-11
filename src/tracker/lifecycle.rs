use crate::{error::TrackerError, model::IssueState};

pub(crate) fn validate_transition(
    current: IssueState,
    target: IssueState,
) -> Result<(), TrackerError> {
    if matches!((current, target), (IssueState::Done, IssueState::Open)) {
        return Err(TrackerError::InvalidStateTransition(
            "done issues cannot be reopened".into(),
        ));
    }
    // A completed run awaits a review decision: approve (Done), send back
    // for rework (Open), or cancel. Parking or resuming execution would
    // bypass that decision.
    if current == IssueState::AwaitingReview
        && !matches!(
            target,
            IssueState::Open | IssueState::Done | IssueState::Canceled
        )
    {
        return Err(TrackerError::InvalidStateTransition(
            "awaiting_review can only move to open, done, or canceled".into(),
        ));
    }
    Ok(())
}

pub(crate) const fn is_terminal(state: IssueState) -> bool {
    matches!(state, IssueState::Done | IssueState::Canceled)
}

pub(crate) const fn is_dedicated_transition(state: IssueState) -> bool {
    matches!(state, IssueState::AwaitingReview)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn awaiting_review_exits_only_to_review_outcomes() {
        for target in [IssueState::Open, IssueState::Done, IssueState::Canceled] {
            assert!(
                validate_transition(IssueState::AwaitingReview, target).is_ok(),
                "{target:?}"
            );
        }
        for target in [
            IssueState::InProgress,
            IssueState::Blocked,
            IssueState::Deferred,
            IssueState::AwaitingReview,
        ] {
            assert!(
                matches!(
                    validate_transition(IssueState::AwaitingReview, target),
                    Err(TrackerError::InvalidStateTransition(_))
                ),
                "{target:?}"
            );
        }
    }

    #[test]
    fn permits_canceled_to_open_but_rejects_done_to_open() {
        assert!(validate_transition(IssueState::Canceled, IssueState::Open).is_ok());
        assert!(matches!(
            validate_transition(IssueState::Done, IssueState::Open),
            Err(TrackerError::InvalidStateTransition(_))
        ));
    }
}
