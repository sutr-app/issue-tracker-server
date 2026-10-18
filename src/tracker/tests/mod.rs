mod tracker_behavior;
mod tracker_recovery;

use crate::v1;

pub(crate) fn verified_completion_report(
    summary: &str,
) -> v1::transition_issue_args::TerminalReport {
    v1::transition_issue_args::TerminalReport::Completion(v1::CompletionReport {
        result: Some(v1::completion_report::Result::Verified(
            v1::VerifiedCompletion {
                evidence: vec![v1::CompletionEvidence {
                    kind: v1::CompletionEvidenceKind::OtherEvidence as i32,
                    summary: summary.into(),
                    uri: None,
                }],
            },
        )),
    })
}

pub(crate) fn awaiting_review_completion(run_id: &str) -> v1::AwaitingReviewCompletion {
    v1::AwaitingReviewCompletion {
        run_id: run_id.into(),
        full_commit_id: "a".repeat(40),
        evidence_references: vec![format!("artifact://runs/{run_id}/report.json")],
        report: Some(v1::CompletionReport {
            result: Some(v1::completion_report::Result::Verified(
                v1::VerifiedCompletion {
                    evidence: vec![v1::CompletionEvidence {
                        kind: v1::CompletionEvidenceKind::TestResult as i32,
                        summary: "required checks passed".into(),
                        uri: Some(format!("artifact://runs/{run_id}/tests.json")),
                    }],
                },
            )),
        }),
    }
}

/// Claims a fresh issue and completes it into awaiting review, returning the
/// issue id, the resulting revision, and the recorded completion.
pub(crate) async fn create_awaiting_review_fixture(
    tracker: &crate::tracker::Tracker,
    operation_id: &str,
    run_id: &str,
) -> (uuid::Uuid, u64, v1::AwaitingReviewCompletion) {
    let issue = tracker
        .create(operation_id, "awaiting-review fixture")
        .await
        .unwrap();
    let claimed = tracker
        .claim(
            issue.id,
            "fixture-worker",
            issue.revision,
            60,
            &format!("{operation_id}-claim"),
        )
        .await
        .unwrap();
    let completion = awaiting_review_completion(run_id);
    tracker
        .complete_execution_to_awaiting_review(
            issue.id,
            claimed.revision,
            operation_id,
            claimed.lease_handle.as_deref(),
            Some(claimed.revision),
            "issue-tracker:external-agent",
            Some(run_id),
            "fixture-correlation",
            &completion,
        )
        .await
        .unwrap();
    (issue.id, claimed.revision + 1, completion)
}
