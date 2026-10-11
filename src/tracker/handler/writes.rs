use super::super::{
    conversion::*,
    error_response::{invalid_error, tracker_error},
    operation_response::{non_replayable_operation, operation},
    *,
};

pub(crate) async fn dispatch(
    method: &str,
    args: &[u8],
    tracker: &Tracker,
) -> Result<Vec<u8>, String> {
    match method {
        "create_issue" => {
            let request: v1::CreateIssueArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::CreateIssueResult {
                        outcome: Some(v1::create_issue_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::CreateIssueResult {
                        outcome: Some(v1::create_issue_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let draft = match request.issue {
                Some(draft) if !draft.title.is_empty() => draft,
                _ => {
                    return Ok(v1::CreateIssueResult {
                        outcome: Some(v1::create_issue_result::Outcome::Error(invalid_error(
                            "issue title is required",
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let result = tracker
                .create_with_draft_context(
                    id,
                    &context.operation_id,
                    &draft,
                    &context.actor,
                    context.session_id.as_deref(),
                    &context.correlation_id,
                )
                .await;
            Ok(v1::CreateIssueResult {
                outcome: Some(match result {
                    Ok(issue) => v1::create_issue_result::Outcome::Success(v1::IssueMutation {
                        current: Some(issue_details(&issue)),
                        changed_fields: vec!["title".into(), "description".into(), "state".into()],
                        operation: Some(operation(method, &context, Some(&issue))),
                        ..Default::default()
                    }),
                    Err(error) => v1::create_issue_result::Outcome::Error(tracker_error(error)),
                }),
            }
            .encode_to_vec())
        }
        "update_issue" => {
            let request: v1::UpdateIssueArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::UpdateIssueResult {
                        outcome: Some(v1::update_issue_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::UpdateIssueResult {
                        outcome: Some(v1::update_issue_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let expected = match request.expected_revision {
                Some(value) => value,
                None => {
                    return Ok(v1::UpdateIssueResult {
                        outcome: Some(v1::update_issue_result::Outcome::Error(invalid_error(
                            "expected_revision is required",
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let patch = match request.patch {
                Some(patch) => patch,
                None => {
                    return Ok(v1::UpdateIssueResult {
                        outcome: Some(v1::update_issue_result::Outcome::Error(invalid_error(
                            "patch is required",
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let result = tracker
                .update_issue_with_context(
                    id,
                    &patch,
                    &request.update_mask,
                    expected,
                    &context.operation_id,
                    &context.actor,
                    context.session_id.as_deref(),
                    &context.correlation_id,
                    request.target_state,
                    request.terminal_report.as_ref(),
                )
                .await;
            let mut changed_fields = request
                .update_mask
                .iter()
                .filter_map(|field| v1::IssueField::try_from(*field).ok())
                .map(|field| format!("{field:?}"))
                .collect::<Vec<_>>();
            if request.target_state.is_some() {
                changed_fields.push("state".into());
            }
            Ok(v1::UpdateIssueResult {
                outcome: Some(match result {
                    Ok(issue) => v1::update_issue_result::Outcome::Success(v1::IssueMutation {
                        current: Some(issue_details(&issue)),
                        changed_fields,
                        operation: Some(operation(method, &context, Some(&issue))),
                        ..Default::default()
                    }),
                    Err(error) => v1::update_issue_result::Outcome::Error(tracker_error(error)),
                }),
            }
            .encode_to_vec())
        }
        "transition_issue" => {
            let request: v1::TransitionIssueArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::TransitionIssueResult {
                        outcome: Some(v1::transition_issue_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::TransitionIssueResult {
                        outcome: Some(v1::transition_issue_result::Outcome::Error(tracker_error(
                            error,
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let state = match v1::IssueState::try_from(request.target_state) {
                Ok(v1::IssueState::Open) => IssueState::Open,
                Ok(v1::IssueState::InProgress) => IssueState::InProgress,
                Ok(v1::IssueState::Blocked) => IssueState::Blocked,
                Ok(v1::IssueState::Deferred) => IssueState::Deferred,
                Ok(v1::IssueState::Done) => IssueState::Done,
                Ok(v1::IssueState::Canceled) => IssueState::Canceled,
                _ => {
                    return Ok(v1::TransitionIssueResult {
                        outcome: Some(v1::transition_issue_result::Outcome::Error(invalid_error(
                            "target_state is invalid",
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let expected = match request.expected_revision {
                Some(value) => value,
                None => {
                    return Ok(v1::TransitionIssueResult {
                        outcome: Some(v1::transition_issue_result::Outcome::Error(invalid_error(
                            "expected_revision is required",
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let report_kind = match request.terminal_report.as_ref() {
                Some(v1::transition_issue_args::TerminalReport::Completion(report)) => {
                    terminal_report::TerminalReportKind::Completion {
                        has_result: report.result.is_some(),
                    }
                }
                Some(v1::transition_issue_args::TerminalReport::Cancellation(report)) => {
                    terminal_report::TerminalReportKind::Cancellation {
                        has_classification: report.classification
                            != v1::CancellationClass::Unspecified as i32,
                    }
                }
                None => terminal_report::TerminalReportKind::None,
            };
            if let Err(error) = terminal_report::validate(Some(state.into()), report_kind) {
                return Ok(v1::TransitionIssueResult {
                    outcome: Some(v1::transition_issue_result::Outcome::Error(invalid_error(
                        error.message(),
                    ))),
                }
                .encode_to_vec());
            }
            let handle = request
                .lease_handle
                .as_deref()
                .map(|value| String::from_utf8_lossy(value).into_owned());
            let has_lease_capability = handle.as_ref().is_some_and(|value| !value.is_empty());
            let result = tracker
                .transition_with_context(
                    id,
                    state,
                    expected,
                    &context.operation_id,
                    handle.as_deref(),
                    request.claim_revision,
                    &context.actor,
                    context.session_id.as_deref(),
                    &context.correlation_id,
                    (!context.reason.is_empty()).then_some(context.reason.as_str()),
                    request.terminal_report.as_ref(),
                )
                .await;
            Ok(v1::TransitionIssueResult {
                outcome: Some(match result {
                    Ok(issue) => v1::transition_issue_result::Outcome::Success(v1::IssueMutation {
                        current: Some(issue_details(&issue)),
                        previous_state: None,
                        changed_fields: vec!["state".into()],
                        operation: Some(if has_lease_capability {
                            non_replayable_operation(operation(method, &context, Some(&issue)))
                        } else {
                            operation(method, &context, Some(&issue))
                        }),
                    }),
                    Err(error) => v1::transition_issue_result::Outcome::Error(tracker_error(error)),
                }),
            }
            .encode_to_vec())
        }
        _ => unreachable!("method key is routed by the v1 dispatcher"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use tempfile::TempDir;

    fn context(operation_id: &str) -> v1::WriteContext {
        v1::WriteContext {
            request: Some(v1::RequestContext {
                caller: Some(v1::ActorContext {
                    actor: Some(v1::ActorRef {
                        actor_id: "test-actor".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                correlation_id: operation_id.into(),
            }),
            downstream_operation_id: operation_id.into(),
            reason: "test".into(),
        }
    }

    async fn update(tracker: &Tracker, request: v1::UpdateIssueArgs) -> v1::IssueMutation {
        let result = dispatch("update_issue", &request.encode_to_vec(), tracker)
            .await
            .expect("update dispatch");
        match v1::UpdateIssueResult::decode(result.as_slice())
            .expect("update result")
            .outcome
            .expect("update outcome")
        {
            v1::update_issue_result::Outcome::Success(mutation) => mutation,
            v1::update_issue_result::Outcome::Error(error) => {
                panic!("update must succeed: {error:?}")
            }
        }
    }

    #[tokio::test]
    async fn update_reports_state_when_target_state_changes_it() {
        let root = TempDir::new().expect("temporary tracker root");
        let tracker = Tracker::open(root.path()).await.expect("open tracker");
        let issue = tracker
            .create("update state", "test")
            .await
            .expect("create issue");

        let normal = update(
            &tracker,
            v1::UpdateIssueArgs {
                context: Some(context("normal-update")),
                issue_id: issue.id.to_string(),
                expected_revision: Some(0),
                update_mask: vec![v1::IssueField::Title as i32],
                patch: Some(v1::IssuePatch {
                    title: Some("renamed".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(normal.changed_fields, ["Title"]);

        let state = update(
            &tracker,
            v1::UpdateIssueArgs {
                context: Some(context("state-update")),
                issue_id: issue.id.to_string(),
                expected_revision: Some(1),
                update_mask: vec![v1::IssueField::Title as i32],
                patch: Some(v1::IssuePatch {
                    title: Some("blocked".into()),
                    ..Default::default()
                }),
                target_state: Some(v1::IssueState::Blocked as i32),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(state.changed_fields, ["Title", "state"]);

        let terminal = update(
            &tracker,
            v1::UpdateIssueArgs {
                context: Some(context("terminal-update")),
                issue_id: issue.id.to_string(),
                expected_revision: Some(2),
                update_mask: vec![v1::IssueField::Title as i32],
                patch: Some(v1::IssuePatch {
                    title: Some("done".into()),
                    ..Default::default()
                }),
                target_state: Some(v1::IssueState::Done as i32),
                terminal_report: Some(v1::update_issue_args::TerminalReport::Completion(
                    v1::CompletionReport {
                        result: Some(v1::completion_report::Result::Verified(
                            v1::VerifiedCompletion {
                                evidence: vec![v1::CompletionEvidence {
                                    kind: v1::CompletionEvidenceKind::OtherEvidence as i32,
                                    summary: "completed".into(),
                                    uri: None,
                                }],
                            },
                        )),
                    },
                )),
            },
        )
        .await;
        assert_eq!(terminal.changed_fields, ["Title", "state"]);
    }
}
