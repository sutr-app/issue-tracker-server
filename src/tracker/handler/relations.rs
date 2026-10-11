use super::super::{
    conversion::*,
    error_response::{invalid_error, tracker_error},
    operation_response::operation,
    task_mapping::relation_id,
    *,
};

pub(crate) async fn dispatch(
    method: &str,
    args: &[u8],
    tracker: &Tracker,
) -> Result<Vec<u8>, String> {
    match method {
        "add_issue_relation" => {
            let request: v1::AddIssueRelationArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::AddIssueRelationResult {
                        outcome: Some(v1::add_issue_relation_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let source = match parse_issue_id(&request.source_issue_id) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(v1::AddIssueRelationResult {
                        outcome: Some(v1::add_issue_relation_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let target = match parse_issue_id(&request.target_issue_id) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(v1::AddIssueRelationResult {
                        outcome: Some(v1::add_issue_relation_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let kind = match relation_kind_value(request.kind) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(v1::AddIssueRelationResult {
                        outcome: Some(v1::add_issue_relation_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let expected_source = match request.source_expected_revision {
                Some(value) => value,
                None => {
                    return Ok(v1::AddIssueRelationResult {
                        outcome: Some(v1::add_issue_relation_result::Outcome::Error(
                            invalid_error("source_expected_revision is required"),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let expected_target = match request.target_expected_revision {
                Some(value) => value,
                None => {
                    return Ok(v1::AddIssueRelationResult {
                        outcome: Some(v1::add_issue_relation_result::Outcome::Error(
                            invalid_error("target_expected_revision is required"),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let expected_relation_id = relation_id(kind, source, target);
            if !request.relation_id.is_empty() && request.relation_id != expected_relation_id {
                return Ok(v1::AddIssueRelationResult {
                    outcome: Some(v1::add_issue_relation_result::Outcome::Error(
                        invalid_error("relation_id does not match source, target, and kind"),
                    )),
                }
                .encode_to_vec());
            }
            let result = tracker
                .relate_with_context(
                    source,
                    kind,
                    target,
                    expected_source,
                    expected_target,
                    &context.operation_id,
                    &context.actor,
                    context.session_id.as_deref(),
                    &context.correlation_id,
                )
                .await;
            let outcome = match result {
                Ok(()) => {
                    let source_issue = tracker.get(source).await.map_err(|e| e.to_string())?;
                    let target_issue = tracker.get(target).await.map_err(|e| e.to_string())?;
                    let relation = source_issue
                        .relations
                        .iter()
                        .find(|value| value.relation_id == expected_relation_id)
                        .cloned()
                        .unwrap_or(IssueRelation {
                            relation_id: expected_relation_id.clone(),
                            kind,
                            source,
                            target,
                            created_at: now(),
                            created_by: None,
                        });
                    let marker_anchor = if matches!(kind, RelationKind::Blocks) {
                        &target_issue
                    } else {
                        &source_issue
                    };
                    v1::add_issue_relation_result::Outcome::Success(v1::RelationMutation {
                        relation: Some(relation_proto(&relation)),
                        source_revision: source_issue.revision,
                        target_revision: target_issue.revision,
                        operation: Some(operation(method, &context, Some(marker_anchor))),
                        removed: false,
                    })
                }
                Err(error) => v1::add_issue_relation_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::AddIssueRelationResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        "remove_issue_relation" => {
            let request: v1::RemoveIssueRelationArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::RemoveIssueRelationResult {
                        outcome: Some(v1::remove_issue_relation_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let source = match parse_issue_id(&request.source_issue_id) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(v1::RemoveIssueRelationResult {
                        outcome: Some(v1::remove_issue_relation_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let target = match parse_issue_id(&request.target_issue_id) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(v1::RemoveIssueRelationResult {
                        outcome: Some(v1::remove_issue_relation_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let kind = match relation_kind_from_id(&request.relation_id, source, target) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(v1::RemoveIssueRelationResult {
                        outcome: Some(v1::remove_issue_relation_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let expected_source = match request.source_expected_revision {
                Some(value) => value,
                None => {
                    return Ok(v1::RemoveIssueRelationResult {
                        outcome: Some(v1::remove_issue_relation_result::Outcome::Error(
                            invalid_error("source_expected_revision is required"),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let expected_target = match request.target_expected_revision {
                Some(value) => value,
                None => {
                    return Ok(v1::RemoveIssueRelationResult {
                        outcome: Some(v1::remove_issue_relation_result::Outcome::Error(
                            invalid_error("target_expected_revision is required"),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let result = tracker
                .unrelate_with_context(
                    source,
                    kind,
                    target,
                    expected_source,
                    expected_target,
                    &context.operation_id,
                    &context.actor,
                    context.session_id.as_deref(),
                    &context.correlation_id,
                )
                .await;
            let outcome = match result {
                Ok(()) => {
                    let source_issue = tracker.get(source).await.map_err(|e| e.to_string())?;
                    let target_issue = tracker.get(target).await.map_err(|e| e.to_string())?;
                    let marker_anchor = if matches!(kind, RelationKind::Blocks) {
                        &target_issue
                    } else {
                        &source_issue
                    };
                    v1::remove_issue_relation_result::Outcome::Success(v1::RelationMutation {
                        relation: Some(v1::IssueRelation {
                            relation_id: request.relation_id,
                            kind: relation_kind(kind),
                            source_issue_id: source.to_string(),
                            target_issue_id: target.to_string(),
                            ..Default::default()
                        }),
                        source_revision: source_issue.revision,
                        target_revision: target_issue.revision,
                        operation: Some(operation(method, &context, Some(marker_anchor))),
                        removed: true,
                    })
                }
                Err(error) => {
                    v1::remove_issue_relation_result::Outcome::Error(tracker_error(error))
                }
            };
            Ok(v1::RemoveIssueRelationResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        "add_issue_comment" => {
            let request: v1::AddIssueCommentArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::AddIssueCommentResult {
                        outcome: Some(v1::add_issue_comment_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let id = match parse_issue_id(&request.issue_id) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(v1::AddIssueCommentResult {
                        outcome: Some(v1::add_issue_comment_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let expected = match request.expected_revision {
                Some(value) => value,
                None => {
                    return Ok(v1::AddIssueCommentResult {
                        outcome: Some(v1::add_issue_comment_result::Outcome::Error(invalid_error(
                            "expected_revision is required",
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            if request.comment_id.is_empty() || request.body.is_empty() {
                return Ok(v1::AddIssueCommentResult {
                    outcome: Some(v1::add_issue_comment_result::Outcome::Error(invalid_error(
                        "comment_id and body are required",
                    ))),
                }
                .encode_to_vec());
            }
            let result = tracker
                .comment_with_id_session_context(
                    id,
                    &request.comment_id,
                    &request.body,
                    expected,
                    &context.actor,
                    context.session_id.as_deref(),
                    &context.correlation_id,
                    &context.operation_id,
                )
                .await;
            let outcome = match result {
                Ok(issue) => v1::add_issue_comment_result::Outcome::Success(v1::CommentMutation {
                    comment: tracker
                        .list_comments(id)
                        .await
                        .map_err(|e| e.to_string())?
                        .into_iter()
                        .find(|comment| comment.comment_id == request.comment_id),
                    issue_revision: issue.revision,
                    operation: Some(operation(method, &context, Some(&issue))),
                }),
                Err(error) => v1::add_issue_comment_result::Outcome::Error(tracker_error(error)),
            };
            Ok(v1::AddIssueCommentResult {
                outcome: Some(outcome),
            }
            .encode_to_vec())
        }
        _ => unreachable!("method key is routed by the v1 dispatcher"),
    }
}
