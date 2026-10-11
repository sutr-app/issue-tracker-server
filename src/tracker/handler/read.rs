use super::super::{
    config::contract_fingerprint,
    conversion::*,
    error_response::tracker_error,
    operation_log::marker_digest,
    pagination::{filter_issues, page_issues, page_snapshot, page_window},
    *,
};
use std::collections::HashSet;

pub(crate) async fn dispatch(
    method: &str,
    args: &[u8],
    tracker: &Tracker,
) -> Result<Vec<u8>, String> {
    match method {
        "get_contract_info" => {
            let _: v1::GetContractInfoArgs = decode_v1(method, args)?;
            let mut seen_method_keys = HashSet::new();
            let method_keys = proto_schema::METHOD_SPECS
                .iter()
                .chain(proto_schema::MANAGER_METHOD_SPECS)
                .filter_map(|spec| seen_method_keys.insert(spec.key).then_some(spec.key.into()))
                .collect();
            Ok(v1::GetContractInfoResult {
                outcome: Some(v1::get_contract_info_result::Outcome::Success(
                    v1::ContractInfo {
                        contract_version: CONTRACT_VERSION.into(),
                        contract_fingerprint: contract_fingerprint(),
                        storage_schema_version: MAPPING_SCHEMA_VERSION.into(),
                        storage_schema_fingerprint: Tracker::schema_fingerprint(),
                        taskchampion_version: "3.1.0".into(),
                        method_keys,
                        health: v1::HealthStatus::Ready as i32,
                    },
                )),
            }
            .encode_to_vec())
        }
        "get_issue" => {
            let request: v1::GetIssueArgs = decode_v1(method, args)?;
            let result = match parse_issue_id(&request.issue_id) {
                Ok(id) => tracker.get(id).await,
                Err(error) => Err(error),
            };
            Ok(v1::GetIssueResult {
                outcome: Some(match result {
                    Ok(issue) => v1::get_issue_result::Outcome::Success(issue_details(&issue)),
                    Err(error) => v1::get_issue_result::Outcome::Error(tracker_error(error)),
                }),
            }
            .encode_to_vec())
        }
        "list_issues" => {
            let request: v1::ListIssuesArgs = decode_v1(method, args)?;
            let read_generation = tracker.current_read_generation();
            let result = tracker.list().await.and_then(|issues| {
                tracker.validate_read_generation(read_generation)?;
                let filter_digest = request
                    .filter
                    .as_ref()
                    .map(|filter| format!("{:x}", Sha256::digest(filter.encode_to_vec())))
                    .unwrap_or_default();
                let query_fingerprint = marker_digest(&[
                    "list_issues",
                    request.sort.to_string().as_str(),
                    &filter_digest,
                ]);
                page_issues(
                    filter_issues(issues, request.filter.as_ref(), request.sort),
                    request.page.as_ref(),
                    read_generation,
                    &query_fingerprint,
                    tracker.page_token_secret.as_str(),
                )
            });
            Ok(v1::ListIssuesResult {
                outcome: Some(match result {
                    Ok(page) => v1::list_issues_result::Outcome::Success(page),
                    Err(error) => v1::list_issues_result::Outcome::Error(tracker_error(error)),
                }),
            }
            .encode_to_vec())
        }
        "list_ready_issues" => {
            let request: v1::ListReadyIssuesArgs = decode_v1(method, args)?;
            let read_generation = tracker.current_read_generation();
            let result = tracker.ready(now()).await.and_then(|issues| {
                tracker.validate_read_generation(read_generation)?;
                let filter_digest = request
                    .filter
                    .as_ref()
                    .map(|filter| format!("{:x}", Sha256::digest(filter.encode_to_vec())))
                    .unwrap_or_default();
                let query_fingerprint = marker_digest(&["list_ready_issues", &filter_digest]);
                page_issues(
                    filter_issues(
                        issues,
                        request.filter.as_ref(),
                        v1::IssueSort::CanonicalReady as i32,
                    ),
                    request.page.as_ref(),
                    read_generation,
                    &query_fingerprint,
                    tracker.page_token_secret.as_str(),
                )
            });
            Ok(v1::ListReadyIssuesResult {
                outcome: Some(match result {
                    Ok(page) => v1::list_ready_issues_result::Outcome::Success(page),
                    Err(error) => {
                        v1::list_ready_issues_result::Outcome::Error(tracker_error(error))
                    }
                }),
            }
            .encode_to_vec())
        }
        "list_issue_comments" => {
            let request: v1::ListIssueCommentsArgs = decode_v1(method, args)?;
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::ListIssueCommentsResult {
                        outcome: Some(v1::list_issue_comments_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let read_generation = tracker.current_read_generation();
            let outcome = match tracker.list_comments(id).await {
                Ok(comments) => {
                    let page_result =
                        tracker
                            .validate_read_generation(read_generation)
                            .and_then(|()| {
                                page_window(
                                    comments.len(),
                                    request.page.as_ref(),
                                    read_generation,
                                    &marker_digest(&["list_issue_comments", &id.to_string()]),
                                    tracker.page_token_secret.as_str(),
                                )
                            });
                    match page_result {
                        Ok((start, end, next)) => {
                            v1::list_issue_comments_result::Outcome::Success(v1::CommentPage {
                                page: Some(v1::PageInfo {
                                    next_page_token: next,
                                    total_size: Some(comments.len() as u64),
                                    snapshot: Some(page_snapshot(
                                        read_generation,
                                        "comment-timestamp-operation",
                                    )),
                                }),
                                comments: comments[start..end].to_vec(),
                            })
                        }
                        Err(error) => {
                            v1::list_issue_comments_result::Outcome::Error(tracker_error(error))
                        }
                    }
                }
                Err(error) => v1::list_issue_comments_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::ListIssueCommentsResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        "list_issue_history" => {
            let request: v1::ListIssueHistoryArgs = decode_v1(method, args)?;
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::ListIssueHistoryResult {
                        outcome: Some(v1::list_issue_history_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let read_generation = tracker.current_read_generation();
            let outcome = match tracker.list_history(id).await {
                Ok(entries) => {
                    let page_result =
                        tracker
                            .validate_read_generation(read_generation)
                            .and_then(|()| {
                                page_window(
                                    entries.len(),
                                    request.page.as_ref(),
                                    read_generation,
                                    &marker_digest(&["list_issue_history", &id.to_string()]),
                                    tracker.page_token_secret.as_str(),
                                )
                            });
                    match page_result {
                        Ok((start, end, next)) => {
                            v1::list_issue_history_result::Outcome::Success(v1::HistoryPage {
                                page: Some(v1::PageInfo {
                                    next_page_token: next,
                                    total_size: Some(entries.len() as u64),
                                    snapshot: Some(page_snapshot(read_generation, "history-id")),
                                }),
                                entries: entries[start..end].to_vec(),
                            })
                        }
                        Err(error) => {
                            v1::list_issue_history_result::Outcome::Error(tracker_error(error))
                        }
                    }
                }
                Err(error) => v1::list_issue_history_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::ListIssueHistoryResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        "get_operation" => {
            let request: v1::GetOperationArgs = decode_v1(method, args)?;
            let outcome = match tracker
                .get_operation_record(&request.downstream_operation_id)
                .await
            {
                Ok(record) => v1::get_operation_result::Outcome::Success(record),
                Err(error) => v1::get_operation_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::GetOperationResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        _ => unreachable!("method key is routed by the v1 dispatcher"),
    }
}
