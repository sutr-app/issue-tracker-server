use super::{Tracker, config::RunnerConfig, handler};

/// Routes a declared v1 method key to its responsibility-specific handler.
pub(crate) async fn dispatch_v1_method(
    method: &str,
    args: &[u8],
    tracker: &Tracker,
    config: &RunnerConfig,
) -> Result<Vec<u8>, String> {
    match method {
        "get_contract_info"
        | "get_issue"
        | "list_issues"
        | "list_ready_issues"
        | "list_issue_comments"
        | "list_issue_history"
        | "get_operation" => handler::read::dispatch(method, args, tracker).await,
        "create_issue" | "update_issue" | "transition_issue" => {
            handler::writes::dispatch(method, args, tracker).await
        }
        "claim_issue"
        | "heartbeat_issue_claim"
        | "release_issue_claim"
        | "reconcile_expired_claim" => {
            handler::claims::dispatch(method, args, tracker, config).await
        }
        "add_issue_relation" | "remove_issue_relation" | "add_issue_comment" => {
            handler::relations::dispatch(method, args, tracker).await
        }
        "get_artifact" | "list_artifacts" | "create_backup" | "export_issues" => {
            handler::artifacts::dispatch(method, args, tracker).await
        }
        _ => Err(format!("unknown IssueTracker v1 method: {method}")),
    }
}
