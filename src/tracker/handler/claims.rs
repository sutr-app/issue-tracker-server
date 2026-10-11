use super::super::{
    config::RunnerConfig,
    conversion::*,
    error_response::{invalid_error, tracker_error},
    operation_response::{non_replayable_operation, operation},
    *,
};

pub(crate) async fn dispatch(
    method: &str,
    args: &[u8],
    tracker: &Tracker,
    config: &RunnerConfig,
) -> Result<Vec<u8>, String> {
    match method {
        "claim_issue" => {
            let request: v1::ClaimIssueArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::ClaimIssueResult {
                        outcome: Some(v1::claim_issue_result::Outcome::Error(tracker_error(error))),
                    }
                    .encode_to_vec());
                }
            };
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::ClaimIssueResult {
                        outcome: Some(v1::claim_issue_result::Outcome::Error(tracker_error(error))),
                    }
                    .encode_to_vec());
                }
            };
            let expected = match request.expected_revision {
                Some(value) => value,
                None => {
                    return Ok(v1::ClaimIssueResult {
                        outcome: Some(v1::claim_issue_result::Outcome::Error(invalid_error(
                            "expected_revision is required",
                        ))),
                    }
                    .encode_to_vec());
                }
            };
            let ttl = request
                .requested_lease
                .map(|value| value.millis / 1000)
                .unwrap_or(config.default_lease_secs);
            let ttl = ttl.clamp(config.minimum_lease_secs, config.maximum_lease_secs);
            let result = tracker
                .claim_with_context(
                    id,
                    &context.actor,
                    context.session_id.as_deref(),
                    expected,
                    ttl,
                    &context.operation_id,
                    &context.correlation_id,
                )
                .await;
            Ok(v1::ClaimIssueResult {
                outcome: Some(match result {
                    Ok(issue) => v1::claim_issue_result::Outcome::Success(v1::ClaimIssueSuccess {
                        mutation: Some(v1::IssueMutation {
                            current: Some(issue_details(&issue)),
                            changed_fields: vec!["state".into(), "claim".into()],
                            operation: Some(non_replayable_operation(operation(
                                method,
                                &context,
                                Some(&issue),
                            ))),
                            ..Default::default()
                        }),
                        lease_handle: issue.lease_handle.unwrap_or_default().into_bytes(),
                    }),
                    Err(error) => v1::claim_issue_result::Outcome::Error(tracker_error(error)),
                }),
            }
            .encode_to_vec())
        }
        "heartbeat_issue_claim" => {
            let request: v1::HeartbeatIssueClaimArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::HeartbeatIssueClaimResult {
                        outcome: Some(v1::heartbeat_issue_claim_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::HeartbeatIssueClaimResult {
                        outcome: Some(v1::heartbeat_issue_claim_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let (expected, claim_revision) =
                match (request.expected_revision, request.claim_revision) {
                    (Some(expected), Some(claim_revision)) => (expected, claim_revision),
                    _ => {
                        return Ok(v1::HeartbeatIssueClaimResult {
                            outcome: Some(v1::heartbeat_issue_claim_result::Outcome::Error(
                                invalid_error("expected_revision and claim_revision are required"),
                            )),
                        }
                        .encode_to_vec());
                    }
                };
            if claim_revision != expected {
                return Ok(v1::HeartbeatIssueClaimResult {
                    outcome: Some(v1::heartbeat_issue_claim_result::Outcome::Error(
                        invalid_error("claim_revision does not match expected_revision"),
                    )),
                }
                .encode_to_vec());
            }
            let handle = String::from_utf8_lossy(&request.lease_handle).into_owned();
            let ttl = request
                .requested_extension
                .map(|value| value.millis / 1000)
                .unwrap_or(config.default_lease_secs)
                .min(config.maximum_heartbeat_secs.max(config.minimum_lease_secs));
            let result = tracker
                .heartbeat_with_context(
                    id,
                    &context.actor,
                    context.session_id.as_deref(),
                    &handle,
                    expected,
                    ttl,
                    &context.operation_id,
                    &context.correlation_id,
                )
                .await;
            Ok(v1::HeartbeatIssueClaimResult {
                outcome: Some(match result {
                    Ok(issue) => {
                        v1::heartbeat_issue_claim_result::Outcome::Success(v1::ClaimMutation {
                            current: Some(issue_proto(&issue)),
                            operation: Some(non_replayable_operation(operation(
                                method,
                                &context,
                                Some(&issue),
                            ))),
                        })
                    }
                    Err(error) => {
                        v1::heartbeat_issue_claim_result::Outcome::Error(tracker_error(error))
                    }
                }),
            }
            .encode_to_vec())
        }
        "release_issue_claim" => {
            let request: v1::ReleaseIssueClaimArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::ReleaseIssueClaimResult {
                        outcome: Some(v1::release_issue_claim_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::ReleaseIssueClaimResult {
                        outcome: Some(v1::release_issue_claim_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let expected = match request.expected_revision {
                Some(expected) => expected,
                None => {
                    return Ok(v1::ReleaseIssueClaimResult {
                        outcome: Some(v1::release_issue_claim_result::Outcome::Error(
                            invalid_error("expected_revision is required"),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let handle = String::from_utf8_lossy(&request.lease_handle).into_owned();
            let result = tracker
                .release_with_context(
                    id,
                    &context.actor,
                    context.session_id.as_deref(),
                    &handle,
                    expected,
                    &context.operation_id,
                    &context.correlation_id,
                )
                .await;
            Ok(v1::ReleaseIssueClaimResult {
                outcome: Some(match result {
                    Ok(issue) => {
                        v1::release_issue_claim_result::Outcome::Success(v1::IssueMutation {
                            current: Some(issue_details(&issue)),
                            changed_fields: vec!["claim".into()],
                            operation: Some(non_replayable_operation(operation(
                                method,
                                &context,
                                Some(&issue),
                            ))),
                            ..Default::default()
                        })
                    }
                    Err(error) => {
                        v1::release_issue_claim_result::Outcome::Error(tracker_error(error))
                    }
                }),
            }
            .encode_to_vec())
        }
        "reconcile_expired_claim" => {
            let request: v1::ReconcileExpiredClaimArgs = decode_v1(method, args)?;
            let context = match call_context(request.context.as_ref()) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(v1::ReconcileExpiredClaimResult {
                        outcome: Some(v1::reconcile_expired_claim_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            if context.actor.trim().is_empty() || context.actor == "unknown" {
                return Ok(v1::ReconcileExpiredClaimResult {
                    outcome: Some(v1::reconcile_expired_claim_result::Outcome::Error(
                        invalid_error("actor identity is required"),
                    )),
                }
                .encode_to_vec());
            }
            if context.reason.trim().is_empty() {
                return Ok(v1::ReconcileExpiredClaimResult {
                    outcome: Some(v1::reconcile_expired_claim_result::Outcome::Error(
                        invalid_error("reconcile reason is required"),
                    )),
                }
                .encode_to_vec());
            }
            let id = match parse_issue_id(&request.issue_id) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(v1::ReconcileExpiredClaimResult {
                        outcome: Some(v1::reconcile_expired_claim_result::Outcome::Error(
                            tracker_error(error),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            let expected = match request.expected_revision {
                Some(expected) => expected,
                None => {
                    return Ok(v1::ReconcileExpiredClaimResult {
                        outcome: Some(v1::reconcile_expired_claim_result::Outcome::Error(
                            invalid_error("expected_revision is required"),
                        )),
                    }
                    .encode_to_vec());
                }
            };
            if request
                .claim_revision
                .is_some_and(|claim_revision| claim_revision != expected)
            {
                return Ok(v1::ReconcileExpiredClaimResult {
                    outcome: Some(v1::reconcile_expired_claim_result::Outcome::Error(
                        invalid_error("claim_revision does not match expected_revision"),
                    )),
                }
                .encode_to_vec());
            }
            let result = tracker
                .reconcile_expired_claim_with_context(
                    id,
                    expected,
                    &context.operation_id,
                    &context.actor,
                    &context.reason,
                )
                .await;
            Ok(v1::ReconcileExpiredClaimResult {
                outcome: Some(match result {
                    Ok(issue) => {
                        v1::reconcile_expired_claim_result::Outcome::Success(v1::IssueMutation {
                            current: Some(issue_details(&issue)),
                            changed_fields: vec!["state".into(), "claim".into()],
                            operation: Some(operation(method, &context, Some(&issue))),
                            ..Default::default()
                        })
                    }
                    Err(error) => {
                        v1::reconcile_expired_claim_result::Outcome::Error(tracker_error(error))
                    }
                }),
            }
            .encode_to_vec())
        }
        _ => unreachable!("method key is routed by the v1 dispatcher"),
    }
}
