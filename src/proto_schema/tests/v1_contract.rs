use super::super::*;
use super::support::{assert_dense_proto_numbering, assert_primary_message, descriptor};
use crate::proto_types::v1;
use prost::Message;
use sha2::{Digest, Sha256};

fn operation_fingerprint(input: &v1::OperationFingerprintInput) -> String {
    format!("sha256:{:x}", Sha256::digest(input.encode_to_vec()))
}

fn validate_update_mask(fields: &[i32]) -> Result<(), &'static str> {
    let mut seen = std::collections::HashSet::with_capacity(fields.len());
    for field in fields {
        if *field == v1::IssueField::Unspecified as i32 {
            return Err("update mask cannot contain ISSUE_FIELD_UNSPECIFIED");
        }
        if v1::IssueField::try_from(*field).is_err() {
            return Err("update mask contains an unknown issue field");
        }
        if !seen.insert(*field) {
            return Err("update mask cannot contain duplicate fields");
        }
    }
    Ok(())
}

#[test]
fn v1_runner_permission_denied_is_deprecated_compatibility_only() {
    assert!(COMMON_PROTO.contains("ERROR_CODE_PERMISSION_DENIED = 18 [deprecated = true];"));
    assert!(COMMON_PROTO.contains("Compatibility-only value. The runner MUST NOT emit this value"));
}

#[test]
fn pre_release_proto_numbering_is_dense_without_reservations() {
    for (name, proto) in [
        ("v1 common", COMMON_PROTO),
        ("v1 settings", SETTINGS_PROTO),
        ("v1 methods", METHODS_PROTO),
        ("manager common", MANAGER_COMMON_PROTO),
        ("manager settings", MANAGER_SETTINGS_PROTO),
        ("manager methods", MANAGER_METHODS_PROTO),
        ("embedded manager settings", SETTINGS_MANAGER_TYPES),
    ] {
        assert_dense_proto_numbering(name, proto);
    }
}

#[test]
fn v1_settings_schema_is_self_contained_and_primary_message_is_first() {
    let schema = resolved_v1_settings_proto();
    assert_primary_message(schema, "IssueTrackerRunnerSettings");
    assert!(
        !schema
            .lines()
            .any(|line| line.trim_start().starts_with("import "))
    );
}

#[test]
fn v1_settings_require_resource_limits_for_bounded_inputs_and_outputs() {
    let limits = v1::ResourceLimits {
        max_string_bytes: 1,
        max_body_bytes: 2,
        max_uri_bytes: 3,
        max_token_bytes: 4,
        max_handle_bytes: 5,
        max_repeated_item_count: 6,
        max_export_bytes: 7,
        max_export_item_count: 8,
        max_artifact_bytes: 9,
        max_artifact_count: 10,
        max_total_count_scan_items: 11,
    };
    let settings = v1::IssueTrackerRunnerSettings {
        resource_limits: Some(limits),
        ..Default::default()
    };

    assert_eq!(settings.resource_limits, Some(limits));
}

#[test]
fn every_v1_method_schema_is_self_contained_and_primary_message_is_first() {
    let schemas = resolved_v1_method_protos();
    assert_eq!(schemas.len(), METHOD_SPECS.len());
    for spec in METHOD_SPECS {
        let (args, result) = schemas.get(spec.key).expect("method schema must exist");
        assert_primary_message(args, spec.args);
        assert_primary_message(result, spec.result);
    }
}

#[test]
fn method_catalog_keys_match_resolved_schema_keys() {
    let v1_keys = METHOD_SPECS
        .iter()
        .map(|spec| spec.key)
        .collect::<std::collections::HashSet<_>>();
    let manager_keys = MANAGER_METHOD_SPECS
        .iter()
        .map(|spec| spec.key)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        v1_keys,
        resolved_v1_method_protos().keys().copied().collect()
    );
    assert_eq!(
        manager_keys,
        resolved_manager_method_protos().keys().copied().collect()
    );
}

#[test]
fn method_specs_are_explicit_plugin_method_keys() {
    assert!(!METHODS_PROTO.contains("service "));
    let keys = METHOD_SPECS
        .iter()
        .map(|spec| spec.key)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(keys.len(), METHOD_SPECS.len());
    assert!(keys.iter().all(|key| !key.is_empty() && !key.contains(' ')));
}

#[test]
fn method_specs_have_llm_actionable_descriptions() {
    for spec in METHOD_SPECS {
        let description = spec.description;
        assert!(
            description.len() >= 30,
            "{} description is too short for MCP tool guidance",
            spec.key
        );
        assert!(
            [
                "Read",
                "List",
                "Create",
                "Update",
                "Change",
                "Claim",
                "Extend",
                "Release",
                "Reconcile",
                "Add",
                "Remove",
                "Append",
                "Export",
            ]
            .iter()
            .any(|verb| description.contains(verb)),
            "{} description must explain the action",
            spec.key
        );
        assert!(!description.contains("Issue tracker method"));
        assert!(
            description.contains("issue")
                || description.contains("artifact")
                || description.contains("operation")
                || description.contains("contract"),
            "{} description must identify its target",
            spec.key
        );
    }
}

#[test]
fn method_description_lookup_matches_the_method_catalog() {
    for spec in METHOD_SPECS.iter().chain(MANAGER_METHOD_SPECS) {
        assert_eq!(
            method_description(spec.key),
            spec.description,
            "{}",
            spec.key
        );
    }
    assert_eq!(
        method_description("unknown_method"),
        "Unknown IssueTrackerRunner method."
    );
}

#[test]
fn release_claim_description_returns_an_active_issue_to_open() {
    for key in ["release_issue_claim", "manager_release_issue_claim"] {
        let description = method_description(key);
        assert!(
            description.contains("returns to OPEN"),
            "{key} must document the OPEN recovery: {description}"
        );
        assert!(
            !description.contains("without changing") && !description.contains("does not change"),
            "{key} must not promise state preservation: {description}"
        );
    }
}

#[test]
fn primary_message_scanner_ignores_comments_and_prefix_names() {
    let source = r#"syntax = "proto3";
package issue_tracker.v1;
import "v1/issue_tracker_common.proto";
// message Wanted { string decoy = 1; }
message WantedSuffix { string value = 1; }
/* message Wanted { string decoy = 1; } */
message Wanted
{
  string value = 1;
}
"#;
    let resolved = resolver::resolve_primary_message(
        source,
        "Wanted",
        &[(COMMON_IMPORT_PATH, COMMON_PROTO)],
        COMMON_IMPORT_PATH,
    )
    .unwrap();
    assert_eq!(descriptor(&resolved).get_messages()[0].name(), "Wanted");
}

#[test]
fn v1_cas_preconditions_preserve_zero_value_presence() {
    macro_rules! assert_optional_zero {
        ($args:ty, $field:ident) => {{
            let mut args = <$args>::default();
            assert_eq!(args.$field, None, stringify!($args));
            args.$field = Some(0);
            assert_eq!(args.$field, Some(0), stringify!($args));
        }};
    }

    assert_optional_zero!(v1::UpdateIssueArgs, expected_revision);
    assert_optional_zero!(v1::TransitionIssueArgs, expected_revision);
    assert_optional_zero!(v1::TransitionIssueArgs, claim_revision);
    assert_optional_zero!(v1::ClaimIssueArgs, expected_revision);
    assert_optional_zero!(v1::HeartbeatIssueClaimArgs, expected_revision);
    assert_optional_zero!(v1::HeartbeatIssueClaimArgs, claim_revision);
    assert_optional_zero!(v1::ReleaseIssueClaimArgs, expected_revision);
    assert_optional_zero!(v1::ReleaseIssueClaimArgs, claim_revision);
    assert_optional_zero!(v1::ReconcileExpiredClaimArgs, expected_revision);
    assert_optional_zero!(v1::ReconcileExpiredClaimArgs, claim_revision);
    assert_optional_zero!(v1::AddIssueRelationArgs, source_expected_revision);
    assert_optional_zero!(v1::AddIssueRelationArgs, target_expected_revision);
    assert_optional_zero!(v1::RemoveIssueRelationArgs, source_expected_revision);
    assert_optional_zero!(v1::RemoveIssueRelationArgs, target_expected_revision);
    assert_optional_zero!(v1::AddIssueCommentArgs, expected_revision);
}

#[test]
fn v1_temporal_patch_change_is_a_single_choice() {
    let patch = v1::IssuePatch {
        due_at_change: Some(v1::issue_patch::DueAtChange::ClearDueAt(v1::ClearField {})),
        review_at_change: Some(v1::issue_patch::ReviewAtChange::ReviewAt(v1::UtcInstant {
            unix_epoch_millis: 42,
        })),
        ..Default::default()
    };
    assert!(matches!(
        patch.due_at_change,
        Some(v1::issue_patch::DueAtChange::ClearDueAt(_))
    ));
    assert!(matches!(
        patch.review_at_change,
        Some(v1::issue_patch::ReviewAtChange::ReviewAt(_))
    ));
}

#[test]
fn v1_transition_terminal_report_is_a_single_choice() {
    let completion = v1::CompletionReport {
        result: Some(v1::completion_report::Result::Unverified(
            v1::UnverifiedCompletion {
                reason: "reason".into(),
                residual_risk: "risk".into(),
            },
        )),
    };
    let transition = v1::TransitionIssueArgs {
        terminal_report: Some(v1::transition_issue_args::TerminalReport::Completion(
            completion,
        )),
        ..Default::default()
    };

    assert!(matches!(
        transition.terminal_report,
        Some(v1::transition_issue_args::TerminalReport::Completion(_))
    ));
}

#[test]
fn v1_completion_and_cancellation_are_typed_history_data() {
    let completion = v1::CompletionReport {
        result: Some(v1::completion_report::Result::Verified(
            v1::VerifiedCompletion {
                evidence: vec![v1::CompletionEvidence {
                    kind: v1::CompletionEvidenceKind::TestResult.into(),
                    summary: "tests passed".into(),
                    uri: None,
                }],
            },
        )),
    };
    let state_change = v1::StateHistoryDetail {
        old_state: Some(v1::IssueState::InProgress.into()),
        new_state: v1::IssueState::Done.into(),
        completion: Some(completion),
        cancellation: None,
        awaiting_review: None,
    };
    let history = v1::IssueHistoryEntry {
        state_change: Some(state_change),
        ..Default::default()
    };
    assert!(history.state_change.is_some());
}

#[test]
fn awaiting_review_appends_the_state_enum_and_uses_an_explicit_gateway_contract() {
    assert_eq!(v1::IssueState::Open as i32, 1);
    assert_eq!(v1::IssueState::InProgress as i32, 2);
    assert_eq!(v1::IssueState::Blocked as i32, 3);
    assert_eq!(v1::IssueState::Deferred as i32, 4);
    assert_eq!(v1::IssueState::Done as i32, 5);
    assert_eq!(v1::IssueState::Canceled as i32, 6);
    assert_eq!(v1::IssueState::AwaitingReview as i32, 7);
    assert!(v1::IssueState::try_from(9876).is_err());
    assert!(COMMON_PROTO.contains("ISSUE_STATE_AWAITING_REVIEW = 7;"));

    // Additive state support does not force an in-place tracker-root or
    // manager-store migration. New writers opt in using the dedicated Gateway
    // request rather than exposing this state to the generic Transition RPC.
    assert_eq!(crate::Tracker::contract_version(), "lookback-issue/v7");
    assert_eq!(
        crate::Tracker::schema_fingerprint(),
        format!(
            "sha256:{:x}",
            Sha256::digest(b"lookback-issue-v7-taskchampion-3.1.0")
        )
    );

    let completion = v1::AwaitingReviewCompletion {
        run_id: "run-contract-test".into(),
        full_commit_id: "c".repeat(40),
        evidence_references: vec!["artifact://run-contract-test/report.json".into()],
        report: Some(v1::CompletionReport {
            result: Some(v1::completion_report::Result::Unverified(
                v1::UnverifiedCompletion {
                    reason: "required checks were not run".into(),
                    residual_risk: "review is required".into(),
                },
            )),
        }),
    };
    let request =
        crate::proto_types::rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest {
            issue_id: "issue-1".into(),
            capability_id: "ephemeral-capability".into(),
            expected_revision: Some(0),
            idempotency_key: "run-contract-test-completion".into(),
            state_contract_version: "awaiting_review/v1".into(),
            completion: Some(completion),
        };
    let decoded =
        crate::proto_types::rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest::decode(
            request.encode_to_vec().as_slice(),
        )
        .unwrap();
    assert_eq!(decoded, request);
}

#[test]
fn v1_audit_actor_preserves_agent_session() {
    let actor = v1::ActorContext {
        actor: Some(v1::ActorRef {
            actor_id: "agent".into(),
            kind: v1::ActorKind::InternalAgent.into(),
            display_name: None,
        }),
        agent_session_id: Some("session".into()),
    };
    let history = v1::IssueHistoryEntry {
        actor: Some(actor.clone()),
        ..Default::default()
    };
    let issue = v1::Issue {
        created_by: Some(actor.clone()),
        ..Default::default()
    };
    let comment = v1::IssueComment {
        actor: Some(actor.clone()),
        ..Default::default()
    };
    let relation = v1::IssueRelation {
        created_by: Some(actor.clone()),
        ..Default::default()
    };
    let claim_history = v1::ClaimHistoryDetail {
        owner: Some(actor.clone()),
        ..Default::default()
    };
    let ownership_conflict = v1::OwnershipConflict {
        current_owner: Some(actor.clone()),
        ..Default::default()
    };
    let claim = v1::ClaimView {
        owner: Some(actor.clone()),
        ..Default::default()
    };
    let decoded = v1::ActorContext::decode(actor.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        issue.created_by.unwrap().agent_session_id.as_deref(),
        Some("session")
    );
    assert_eq!(
        history.actor.unwrap().agent_session_id.as_deref(),
        Some("session")
    );
    assert_eq!(
        comment.actor.unwrap().agent_session_id.as_deref(),
        Some("session")
    );
    assert_eq!(
        relation.created_by.unwrap().agent_session_id.as_deref(),
        Some("session")
    );
    assert_eq!(
        claim.owner.unwrap().agent_session_id.as_deref(),
        Some("session")
    );
    assert_eq!(
        claim_history.owner.unwrap().agent_session_id.as_deref(),
        Some("session")
    );
    assert_eq!(
        ownership_conflict
            .current_owner
            .unwrap()
            .agent_session_id
            .as_deref(),
        Some("session")
    );
    assert_eq!(decoded.agent_session_id.as_deref(), Some("session"));
}

#[test]
fn v1_operation_record_represents_both_relation_revisions_and_result_references() {
    let record = v1::OperationRecord {
        method_key: "add_issue_relation".into(),
        entity_revisions: vec![
            v1::OperationEntityRevision {
                issue_id: "source".into(),
                backend_id: "backend-source".into(),
                resulting_revision: 3,
            },
            v1::OperationEntityRevision {
                issue_id: "target".into(),
                backend_id: "backend-target".into(),
                resulting_revision: 5,
            },
        ],
        result_references: vec![v1::OperationResultReference {
            subject: Some(v1::operation_result_reference::Subject::RelationId(
                "relation".into(),
            )),
        }],
        result_snapshot: Some(v1::OperationResultSnapshot {
            value: Some(v1::operation_result_snapshot::Value::RelationMutation(
                v1::RelationMutationSnapshot {
                    relation: Some(v1::IssueRelation {
                        relation_id: "relation".into(),
                        source_issue_id: "source".into(),
                        target_issue_id: "target".into(),
                        ..Default::default()
                    }),
                    source_revision: 3,
                    target_revision: 5,
                    removed: true,
                },
            )),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(record.entity_revisions.len(), 2);
    assert_eq!(record.entity_revisions[1].resulting_revision, 5);
    assert!(matches!(
        record.result_references[0].subject,
        Some(v1::operation_result_reference::Subject::RelationId(_))
    ));
    assert!(matches!(
        record.result_snapshot.and_then(|snapshot| snapshot.value),
        Some(v1::operation_result_snapshot::Value::RelationMutation(
            v1::RelationMutationSnapshot { removed: true, .. }
        ))
    ));
}

#[test]
fn v1_artifact_writes_have_write_context_and_replay_snapshot() {
    let backup = v1::CreateBackupArgs {
        context: Some(v1::WriteContext::default()),
        ..Default::default()
    };
    let export = v1::ExportIssuesArgs {
        context: Some(v1::WriteContext::default()),
        ..Default::default()
    };
    let artifact = v1::OperationResultSnapshot {
        value: Some(v1::operation_result_snapshot::Value::Artifact(
            v1::Artifact {
                artifact_id: "artifact".into(),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    assert!(backup.context.is_some());
    assert!(export.context.is_some());
    assert!(matches!(
        artifact.value,
        Some(v1::operation_result_snapshot::Value::Artifact(
            v1::Artifact { .. }
        ))
    ));
}

#[test]
fn v1_artifact_reads_expose_metadata_only_and_are_paginated() {
    let get = v1::GetArtifactArgs {
        context: Some(v1::RequestContext::default()),
        artifact_id: "artifact".into(),
    };
    let list = v1::ListArtifactsArgs {
        context: Some(v1::RequestContext::default()),
        page: Some(v1::PageRequest::default()),
    };
    let result = v1::ListArtifactsResult {
        outcome: Some(v1::list_artifacts_result::Outcome::Success(
            v1::ArtifactPage {
                artifacts: vec![v1::Artifact {
                    artifact_id: "artifact".into(),
                    ..Default::default()
                }],
                page: Some(v1::PageInfo::default()),
            },
        )),
    };

    assert_eq!(get.artifact_id, "artifact");
    assert!(list.page.is_some());
    assert!(matches!(
        result.outcome,
        Some(v1::list_artifacts_result::Outcome::Success(
            v1::ArtifactPage { .. }
        ))
    ));
}

#[test]
fn v1_does_not_publish_consumer_policy_operations() {
    assert!(!METHODS_PROTO.contains("rpc PurgeIssue("));
    assert!(!METHODS_PROTO.contains("rpc RestoreCurrentRoot("));
    assert!(!METHODS_PROTO.contains("HUMAN_OVERRIDE"));
    assert!(
        !METHOD_SPECS
            .iter()
            .any(|spec| matches!(spec.key, "purge_issue" | "restore_current_root"))
    );
}

#[test]
fn v1_operation_contract_separates_commit_durability_from_result_replayability() {
    let operation = v1::OperationRecord {
        commit_durability: v1::OperationCommitDurability::Durable as i32,
        result_replayability: v1::OperationResultReplayability::NotReplayable as i32,
        method_key: "claim_issue".into(),
        scope: v1::OperationScope::Issue as i32,
        ..Default::default()
    };

    // A lost claim response is represented by a durable commit with no replayable result.
    assert_eq!(
        operation.commit_durability,
        v1::OperationCommitDurability::Durable as i32
    );
    assert_eq!(
        operation.result_replayability,
        v1::OperationResultReplayability::NotReplayable as i32
    );
    assert!(operation.result_snapshot.is_none());
    let decoded = v1::OperationRecord::decode(operation.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.commit_durability, operation.commit_durability);
    assert_eq!(decoded.result_replayability, operation.result_replayability);
}

#[test]
fn v1_pagination_binds_tokens_to_a_watermark_snapshot_and_stable_traversal() {
    let initial = v1::PageRequest {
        page_size: 25,
        ..Default::default()
    };
    let request = v1::PageRequest {
        page_size: 25,
        page_token: Some("opaque-token".into()),
    };
    let snapshot = v1::PageSnapshot {
        read_revision_watermark: 42,
        snapshot_id: "snapshot-42".into(),
        stable_traversal: Some(v1::StableTraversal {
            order_fingerprint: "sha256:order".into(),
        }),
        read_generation: 7,
    };
    let page_info = v1::PageInfo {
        next_page_token: Some("next-token".into()),
        snapshot: Some(snapshot.clone()),
        ..Default::default()
    };
    let error = v1::IssueTrackerError {
        code: v1::ErrorCode::PageTokenExpired as i32,
        detail: Some(v1::issue_tracker_error::Detail::PageTokenExpired(
            v1::PageTokenExpired {
                snapshot_id: snapshot.snapshot_id.clone(),
                token_read_generation: snapshot.read_generation,
                current_read_generation: snapshot.read_generation + 1,
                ..Default::default()
            },
        )),
        ..Default::default()
    };

    assert!(initial.page_token.is_none());
    assert_eq!(initial.page_size, 25);
    assert_eq!(
        initial,
        v1::PageRequest {
            page_size: 25,
            ..Default::default()
        }
    );
    assert_eq!(request.page_token.as_deref(), Some("opaque-token"));
    assert_eq!(
        page_info.snapshot.as_ref().unwrap().read_revision_watermark,
        42
    );
    assert_eq!(
        page_info.snapshot.as_ref().unwrap().snapshot_id,
        "snapshot-42"
    );
    assert_eq!(
        page_info
            .snapshot
            .as_ref()
            .unwrap()
            .stable_traversal
            .as_ref()
            .map(|value| value.order_fingerprint.as_str()),
        Some("sha256:order")
    );
    assert_eq!(page_info.snapshot.as_ref().unwrap().read_generation, 7);
    assert!(matches!(
        error.detail,
        Some(v1::issue_tracker_error::Detail::PageTokenExpired(
            v1::PageTokenExpired {
                token_read_generation: 7,
                current_read_generation: 8,
                ..
            }
        ))
    ));
}

#[test]
fn v1_artifacts_identify_kind_format_media_version_and_validation() {
    let artifact = v1::Artifact {
        artifact_kind: v1::ArtifactKind::Backup as i32,
        format: "sqlite".into(),
        media_type: "application/vnd.sqlite3".into(),
        format_version: "3".into(),
        validation_status: v1::ArtifactValidationStatus::Validated as i32,
        ..Default::default()
    };

    assert_eq!(artifact.artifact_kind, v1::ArtifactKind::Backup as i32);
    assert_eq!(artifact.format, "sqlite");
    assert_eq!(artifact.media_type, "application/vnd.sqlite3");
    assert_eq!(artifact.format_version, "3");
    assert_eq!(
        artifact.validation_status,
        v1::ArtifactValidationStatus::Validated as i32
    );
    let decoded = v1::Artifact::decode(artifact.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.artifact_kind, artifact.artifact_kind);
    assert_eq!(decoded.validation_status, artifact.validation_status);
}

#[test]
fn operation_fingerprint_does_not_collide_across_method_or_scope() {
    let base = v1::OperationFingerprintInput {
        downstream_operation_id: "root-operation-1".into(),
        method_key: "update_issue".into(),
        scope: v1::OperationScope::Issue as i32,
        payload_fingerprint: "sha256:payload".into(),
    };
    let different_method = v1::OperationFingerprintInput {
        method_key: "transition_issue".into(),
        ..base.clone()
    };
    let different_scope = v1::OperationFingerprintInput {
        scope: v1::OperationScope::Tracker as i32,
        ..base.clone()
    };
    let different_root_operation = v1::OperationFingerprintInput {
        downstream_operation_id: "root-operation-2".into(),
        ..base.clone()
    };

    assert_ne!(
        operation_fingerprint(&base),
        operation_fingerprint(&different_method)
    );
    assert_ne!(
        operation_fingerprint(&base),
        operation_fingerprint(&different_scope)
    );
    assert_ne!(
        operation_fingerprint(&base),
        operation_fingerprint(&different_root_operation)
    );
}

#[test]
fn v1_update_mask_is_closed_and_repeated_fields_are_replacements() {
    let patch = v1::IssuePatch {
        labels: vec!["canonical".into()],
        acceptance_criteria: vec!["criterion".into()],
        contexts: vec![v1::ContextRef {
            value: "repo".into(),
            ..Default::default()
        }],
        provenance_metadata: vec![v1::MetadataEntry {
            key: "source".into(),
            value: "test".into(),
        }],
        ..Default::default()
    };
    let args = v1::UpdateIssueArgs {
        update_mask: vec![
            v1::IssueField::Labels as i32,
            v1::IssueField::AcceptanceCriteria as i32,
            v1::IssueField::Contexts as i32,
            v1::IssueField::ProvenanceMetadata as i32,
        ],
        patch: Some(patch),
        ..Default::default()
    };

    assert_eq!(args.update_mask.len(), 4);
    assert_eq!(args.patch.unwrap().labels, vec!["canonical"]);
    assert!(validate_update_mask(&args.update_mask).is_ok());
    assert!(validate_update_mask(&[v1::IssueField::Unspecified as i32]).is_err());
    assert!(validate_update_mask(&[99]).is_err());
    assert!(
        validate_update_mask(&[v1::IssueField::Labels as i32, v1::IssueField::Labels as i32,])
            .is_err()
    );
}
