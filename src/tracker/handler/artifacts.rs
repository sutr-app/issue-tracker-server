use super::super::{
    conversion::*,
    error_response::tracker_error,
    operation_log::marker_digest,
    operation_response::operation,
    pagination::{page_snapshot, page_window},
    *,
};

pub(crate) async fn dispatch(
    method: &str,
    args: &[u8],
    tracker: &Tracker,
) -> Result<Vec<u8>, String> {
    match method {
        "get_artifact" => {
            let request: v1::GetArtifactArgs = decode_v1(method, args)?;
            let outcome = match tracker.list_artifact_metadata() {
                Ok(artifacts) => artifacts
                    .into_iter()
                    .find(|artifact| artifact.artifact_id == request.artifact_id)
                    .map(v1::get_artifact_result::Outcome::Success)
                    .unwrap_or_else(|| {
                        v1::get_artifact_result::Outcome::Error(v1::IssueTrackerError {
                            code: v1::ErrorCode::ArtifactNotFound as i32,
                            message: "artifact not found".into(),
                            ..Default::default()
                        })
                    }),
                Err(error) => v1::get_artifact_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::GetArtifactResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        "list_artifacts" => {
            let request: v1::ListArtifactsArgs = decode_v1(method, args)?;
            let read_generation = tracker.current_read_generation();
            let outcome = match tracker.list_artifact_metadata() {
                Ok(artifacts) => {
                    let page_result =
                        tracker
                            .validate_read_generation(read_generation)
                            .and_then(|()| {
                                page_window(
                                    artifacts.len(),
                                    request.page.as_ref(),
                                    read_generation,
                                    &marker_digest(&["list_artifacts"]),
                                    tracker.page_token_secret.as_str(),
                                )
                            });
                    match page_result {
                        Ok((start, end, next)) => {
                            v1::list_artifacts_result::Outcome::Success(v1::ArtifactPage {
                                page: Some(v1::PageInfo {
                                    next_page_token: next,
                                    total_size: Some(artifacts.len() as u64),
                                    snapshot: Some(page_snapshot(read_generation, "artifact-id")),
                                }),
                                artifacts: artifacts[start..end].to_vec(),
                            })
                        }
                        Err(error) => {
                            v1::list_artifacts_result::Outcome::Error(tracker_error(error))
                        }
                    }
                }
                Err(error) => v1::list_artifacts_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::ListArtifactsResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        "create_backup" => {
            let request: v1::CreateBackupArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::CreateBackupResult {
                        outcome: Some(v1::create_backup_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let outcome = match tracker.create_backup_artifact(&context.operation_id).await {
                Ok(artifact) => v1::create_backup_result::Outcome::Success(v1::ArtifactMutation {
                    artifact: Some(artifact),
                    operation: Some(operation(method, &context, None)),
                }),
                Err(error) => v1::create_backup_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::CreateBackupResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        "export_issues" => {
            let request: v1::ExportIssuesArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::ExportIssuesResult {
                        outcome: Some(v1::export_issues_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let outcome = match tracker.create_export_artifact(&context.operation_id).await {
                Ok(artifact) => v1::export_issues_result::Outcome::Success(v1::ArtifactMutation {
                    artifact: Some(artifact),
                    operation: Some(operation(method, &context, None)),
                }),
                Err(error) => v1::export_issues_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::ExportIssuesResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        _ => unreachable!("method key is routed by the v1 dispatcher"),
    }
}
