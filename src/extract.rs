use crate::manager::candidates::metadata_value;
use crate::manager::extract_apply::{
    ApplyOutcome, ApplyRequest, ExtractCandidateInput, kind_name, request_digest,
};
use crate::manager::extract_persist::{PersistApplyError, persist_apply};
use crate::manager::source_labels::{
    OWNERSHIP_NORMALIZATION_REASON_KEY, OWNERSHIP_NORMALIZED_FROM_KEY, append_candidate_provenance,
    copy_candidate_snapshots_to_issue,
};
use crate::proto_types::{manager_v1, rpc_v1, v1};
use crate::server::GrpcServer;
use prost::Message;
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

impl GrpcServer {
    async fn admit_auto_issue(
        &self,
        candidate_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<(String, Vec<u8>)>, rpc_v1::ExtractError> {
        let store = self
            .app
            .manager_store_context()
            .await
            .map_err(|error| extract_error("unavailable", error))?;
        let Some(manager) = &self.app.manager else {
            return Err(extract_error("unavailable", "manager is not configured"));
        };
        let mut locked = manager.lock().await;
        let repo = crate::manager::repository::ManagerRepository::new(locked.storage_mut());
        let candidate = repo
            .candidates()
            .map_err(|error| extract_error("manager", error.message))?
            .into_iter()
            .find(|candidate| candidate.candidate_id == candidate_id)
            .ok_or_else(|| extract_error("not_found", "candidate not found"))?;
        if candidate.source_evidence.is_empty() {
            return Err(extract_error(
                "failed_precondition",
                "candidate is missing source evidence",
            ));
        }
        let issue_id = crate::manager::repository::candidate_issue_id(candidate_id);
        let already_mapped = repo
            .get(
                crate::manager::repository::EntityKind::IssueMapping,
                &issue_id,
            )
            .map_err(|error| extract_error("manager", error.to_string()))?
            .is_some();
        drop(locked);
        if already_mapped {
            return Ok(None);
        }
        let Some(proof) = candidate.strong_detection_proof.clone() else {
            return Err(extract_error("failed_precondition", "missing strong proof"));
        };
        if candidate.extraction_run_id.trim().is_empty() {
            return Err(extract_error(
                "failed_precondition",
                "candidate extraction run ID is required",
            ));
        }
        let thread_id = metadata_value(&candidate, "thread_id")
            .unwrap_or_default()
            .to_string();
        let mut provenance_metadata = vec![v1::MetadataEntry {
            key: "thread_id".into(),
            value: thread_id,
        }];
        copy_candidate_snapshots_to_issue(&candidate.metadata, &mut provenance_metadata)
            .map_err(|error| extract_error("invalid_argument", error))?;
        append_candidate_provenance(&mut provenance_metadata, &candidate)
            .map_err(|error| extract_error("invalid_argument", error))?;
        let initial_state = if candidate.suggested_state == v1::IssueState::InProgress as i32 {
            provenance_metadata.extend([
                v1::MetadataEntry {
                    key: OWNERSHIP_NORMALIZED_FROM_KEY.into(),
                    value: "IN_PROGRESS".into(),
                },
                v1::MetadataEntry {
                    key: OWNERSHIP_NORMALIZATION_REASON_KEY.into(),
                    value: "extraction_suggested_state_is_not_a_claim".into(),
                },
            ]);
            v1::IssueState::Open as i32
        } else {
            candidate.suggested_state
        };
        let admitted_payload = self
            .app
            .admit_manager_backend(
                "manager_create_issue",
                manager_v1::CreateIssueArgs {
                    context: Some(extract_write_context(
                        &store,
                        "manager_create_issue",
                        &format!("issue:{issue_id}"),
                        idempotency_key,
                    )),
                    issue_id: issue_id.clone(),
                    issue: Some(v1::IssueDraft {
                        title: candidate.title.clone(),
                        description: candidate.description.clone(),
                        kind: candidate.kind,
                        priority: candidate.suggested_priority,
                        acceptance_criteria: candidate.acceptance_criteria.clone(),
                        creation_basis: v1::CreationBasis::AutoDetected as i32,
                        confirmation_status: v1::ConfirmationStatus::Unreviewed as i32,
                        initial_state,
                        provenance_metadata,
                        source_evidence: candidate.source_evidence.clone(),
                        ..Default::default()
                    }),
                    strong_detection_proof: Some(proof),
                    provenance_sources: Vec::new(),
                },
            )
            .await
            .map_err(|error| extract_error("manager", error))?;
        let admitted = manager_v1::CreateIssueResult::decode(admitted_payload.as_slice())
            .map_err(|error| extract_error("manager", error.to_string()))?;
        match admitted.outcome {
            Some(manager_v1::create_issue_result::Outcome::Success(_)) => {}
            Some(manager_v1::create_issue_result::Outcome::Error(error)) => {
                return Err(extract_error("manager", error.message));
            }
            None => {
                return Err(extract_error(
                    "unconfirmed",
                    "create admission has no outcome",
                ));
            }
        }

        Ok(Some((issue_id, admitted_payload)))
    }

    async fn complete_auto_issue(
        &self,
        issue_id: String,
        admitted_payload: Vec<u8>,
    ) -> Result<Option<String>, rpc_v1::ExtractError> {
        let completed = self
            .app
            .complete_manager_backend::<manager_v1::CreateIssueResult>(
                "manager_create_issue",
                admitted_payload,
            )
            .await
            .map_err(|error| extract_error("manager", error))?;
        match completed.outcome {
            Some(manager_v1::create_issue_result::Outcome::Success(_)) => Ok(Some(issue_id)),
            // The operation and its completion plan are durable.  A pending,
            // retryable, or terminal backend result is reflected by the
            // outbox finalizer rather than losing the extracted Candidate.
            Some(manager_v1::create_issue_result::Outcome::Error(error)) => {
                Err(extract_error("manager", error.message))
            }
            None => Ok(None),
        }
    }

    async fn extraction_run_state(&self, run_id: &str) -> Option<i32> {
        let manager = self.app.manager.as_ref()?;
        let mut manager = manager.lock().await;
        let repository = crate::manager::repository::ManagerRepository::new(manager.storage_mut());
        repository
            .extraction_runs()
            .ok()?
            .into_iter()
            .find(|run| run.run_id == run_id)
            .map(|run| run.state)
    }

    async fn fail_extraction_auto_registration_run(&self, run_id: &str, reason: &str) {
        let Some(manager) = &self.app.manager else {
            return;
        };
        let mut manager = manager.lock().await;
        let mut repository =
            crate::manager::repository::ManagerRepository::new(manager.storage_mut());
        let _ = repository.fail_extraction_auto_registration_run(run_id, reason);
    }
}

fn extract_write_context(
    store: &manager_v1::ManagerReadContext,
    method: &str,
    target: &str,
    idempotency_key: &str,
) -> manager_v1::ManagerWriteContext {
    manager_v1::ManagerWriteContext {
        method: method.into(),
        target: target.into(),
        idempotency_key: idempotency_key.into(),
        correlation_id: idempotency_key.into(),
        schema_fingerprint: store.schema_fingerprint.clone(),
        data_root_id: store.data_root_id.clone(),
        contract_fingerprint: store.contract_fingerprint.clone(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: "issue-tracker:issue-extract".into(),
            actor_kind: v1::ActorKind::System as i32,
            agent_session_id: Some(idempotency_key.into()),
            ..Default::default()
        }),
    }
}

#[tonic::async_trait]
impl rpc_v1::issue_extract_server::IssueExtract for GrpcServer {
    async fn list_extraction_hints(
        &self,
        request: Request<rpc_v1::ListExtractionHintsRequest>,
    ) -> Result<Response<rpc_v1::ListExtractionHintsResponse>, Status> {
        let thread_id = request.into_inner().thread_id;
        let Some(manager) = &self.app.manager else {
            return Ok(error_hints("unavailable", "manager is not configured"));
        };
        let mut manager = manager.lock().await;
        let repo = crate::manager::repository::ManagerRepository::new(manager.storage_mut());
        let candidates = match repo.candidates() {
            Ok(value) => value,
            Err(error) => return Ok(error_hints("manager", error.message)),
        };
        let mut same = Vec::new();
        let mut rest = Vec::new();
        for candidate in candidates {
            let hint = rpc_v1::ExtractionHint {
                id: candidate.candidate_id.clone(),
                kind: kind_name(candidate.kind),
                title: candidate.title.clone(),
                completion: candidate
                    .acceptance_criteria
                    .first()
                    .cloned()
                    .unwrap_or_default(),
            };
            let thread = metadata_value(&candidate, "thread_id");
            if thread == Some(thread_id.as_str()) {
                same.push(hint);
            } else {
                rest.push((candidate.updated_at, hint));
            }
        }
        rest.sort_by_key(|(updated, _)| {
            std::cmp::Reverse(
                updated
                    .as_ref()
                    .map(|value| value.unix_epoch_millis)
                    .unwrap_or(0),
            )
        });
        same.extend(rest.into_iter().map(|(_, hint)| hint));
        same.truncate(8);
        Ok(Response::new(rpc_v1::ListExtractionHintsResponse {
            outcome: Some(rpc_v1::list_extraction_hints_response::Outcome::Success(
                rpc_v1::ExtractionHintPage { hints: same },
            )),
        }))
    }

    async fn apply_extraction(
        &self,
        request: Request<rpc_v1::ApplyExtractionRequest>,
    ) -> Result<Response<rpc_v1::ApplyExtractionResponse>, Status> {
        let request = request.into_inner();
        let Some(manager) = &self.app.manager else {
            return Ok(error_apply("unavailable", "manager is not configured"));
        };
        let mut manager = manager.lock().await;
        let apply_request = ApplyRequest {
            thread_id: request.thread_id,
            summary_ref: request.summary_ref,
            snapshot_digest: request.snapshot_digest,
            extract_schema: request.extract_schema,
            policy: request.policy,
            model_id: request.model_id,
            prompt_version: request.prompt_version,
            allowed_messages: match request
                .allowed_messages
                .into_iter()
                .map(|message| {
                    let calculated = (!message.content.is_empty()).then(|| {
                        format!("sha256:{:x}", Sha256::digest(message.content.as_bytes()))
                    });
                    match (calculated, message.content_digest) {
                        (Some(calculated), supplied)
                            if !supplied.is_empty() && supplied != calculated =>
                        {
                            Err("content_digest does not match content".to_string())
                        }
                        (Some(calculated), _) => Ok((message.memory_id, calculated)),
                        (None, supplied) if supplied.is_empty() => {
                            Err("content_digest is required when content is absent".to_string())
                        }
                        (None, supplied) => Ok((message.memory_id, supplied)),
                    }
                })
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(messages) => messages,
                Err(error) => return Ok(error_apply("invalid_argument", error)),
            },
            input_message_ids: request.input_message_ids,
            candidates: request
                .candidates
                .into_iter()
                .map(|candidate| ExtractCandidateInput {
                    title: candidate.title,
                    description: candidate.description,
                    kind: candidate.kind,
                    suggested_priority: candidate.suggested_priority,
                    suggested_state: candidate.suggested_state,
                    unfinished_reason: candidate.unfinished_reason,
                    completion: candidate.completion,
                    detection: candidate.detection,
                    confidence: candidate.confidence,
                    locator_memory_ids: candidate.locator_memory_ids,
                })
                .collect(),
            mixed: request.mixed,
            failure_reason: request.failure_reason,
            idempotency_key: request.idempotency_key,
            source_labels: request.source_labels,
            rejected_candidate_ids_to_unsuppress: request.rejected_candidate_ids_to_unsuppress,
            rejected_candidate_identities: Vec::new(),
        };
        if let Err(error) = request_digest(&apply_request) {
            return Ok(error_apply("invalid_argument", error));
        }
        let persist = match persist_apply(manager.storage_mut(), apply_request) {
            Ok(result) => result,
            Err(PersistApplyError::IdempotencyConflict) => {
                return Ok(error_apply("idempotency_conflict", "idempotency_conflict"));
            }
            Err(PersistApplyError::InvalidArgument(error)) => {
                return Ok(error_apply(
                    "invalid_argument",
                    format!("invalid_argument:{error}"),
                ));
            }
            Err(PersistApplyError::Failed(error)) => return Ok(error_apply("apply_failed", error)),
        };
        if persist.outcome == ApplyOutcome::SkippedExtractionDisabled {
            return Ok(Response::new(rpc_v1::ApplyExtractionResponse {
                outcome: Some(rpc_v1::apply_extraction_response::Outcome::Skipped(
                    rpc_v1::SkippedExtraction {
                        reason: rpc_v1::ExtractionSkipReason::ExtractionDisabled as i32,
                    },
                )),
            }));
        }
        drop(manager);
        if !persist.registerable_candidate_ids.is_empty() {
            let registrations = persist
                .registerable_candidate_ids
                .iter()
                .map(|candidate_id| {
                    (
                        candidate_id.clone(),
                        crate::manager::repository::candidate_issue_id(candidate_id),
                    )
                })
                .collect::<Vec<_>>();
            let Some(manager) = &self.app.manager else {
                return Ok(error_apply("unavailable", "manager is not configured"));
            };
            let mut manager = manager.lock().await;
            let mut repository =
                crate::manager::repository::ManagerRepository::new(manager.storage_mut());
            let registrations = match repository
                .reserve_extraction_auto_registrations(&persist.run_id, &registrations)
            {
                Ok(registrations) => registrations,
                Err(error) => return Ok(error_apply("apply_failed", error.to_string())),
            };
            drop(manager);
            let mut registration_keys = std::collections::HashMap::new();
            for registration in registrations {
                registration_keys.insert(registration.candidate_id, registration.idempotency_key);
            }
            let mut issue_ids = persist.issue_ids;
            let mut admissions = Vec::new();
            for candidate_id in &persist.registerable_candidate_ids {
                let Some(idempotency_key) = registration_keys.get(candidate_id) else {
                    return Ok(error_apply(
                        "apply_failed",
                        "auto-registration reservation key is missing",
                    ));
                };
                match self.admit_auto_issue(candidate_id, idempotency_key).await {
                    Ok(Some(admission)) => admissions.push(admission),
                    Ok(None) => {}
                    Err(error) => {
                        self.fail_extraction_auto_registration_run(&persist.run_id, &error.message)
                            .await;
                        return Ok(error_apply("register_failed", error.message));
                    }
                }
            }
            for (issue_id, admitted_payload) in admissions {
                match self.complete_auto_issue(issue_id, admitted_payload).await {
                    Ok(Some(issue_id)) => issue_ids.push(issue_id),
                    Ok(None) => {}
                    Err(error) => return Ok(error_apply("register_failed", error.message)),
                }
            }
            let run_state = self
                .extraction_run_state(&persist.run_id)
                .await
                .unwrap_or(persist.run_state);
            return Ok(Response::new(rpc_v1::ApplyExtractionResponse {
                outcome: Some(rpc_v1::apply_extraction_response::Outcome::Applied(
                    rpc_v1::AppliedExtraction {
                        run_id: persist.run_id,
                        run_state: run_state.to_string(),
                        candidate_ids: persist.candidate_ids,
                        issue_ids,
                    },
                )),
            }));
        }
        Ok(Response::new(rpc_v1::ApplyExtractionResponse {
            outcome: Some(rpc_v1::apply_extraction_response::Outcome::Applied(
                rpc_v1::AppliedExtraction {
                    run_id: persist.run_id,
                    run_state: persist.run_state.to_string(),
                    candidate_ids: persist.candidate_ids,
                    issue_ids: persist.issue_ids,
                },
            )),
        }))
    }
}

fn extract_error(code: impl Into<String>, message: impl Into<String>) -> rpc_v1::ExtractError {
    rpc_v1::ExtractError {
        code: code.into(),
        message: message.into(),
    }
}

fn error_hints(
    code: impl Into<String>,
    message: impl Into<String>,
) -> Response<rpc_v1::ListExtractionHintsResponse> {
    Response::new(rpc_v1::ListExtractionHintsResponse {
        outcome: Some(rpc_v1::list_extraction_hints_response::Outcome::Error(
            extract_error(code, message),
        )),
    })
}

fn error_apply(
    code: impl Into<String>,
    message: impl Into<String>,
) -> Response<rpc_v1::ApplyExtractionResponse> {
    Response::new(rpc_v1::ApplyExtractionResponse {
        outcome: Some(rpc_v1::apply_extraction_response::Outcome::Error(
            extract_error(code, message),
        )),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto_types::rpc_v1::issue_extract_server::IssueExtract;
    use crate::{
        server::{IssueTrackerApplication, ListenerSettings, MaintenanceSettings, ServerSettings},
        tracker::Tracker,
    };
    use tempfile::TempDir;

    async fn test_server() -> GrpcServer {
        let root = TempDir::new().unwrap();
        let root_path = root.keep();
        let settings = ServerSettings {
            server: ListenerSettings::default(),
            gateway_endpoints: Vec::new(),
            tracker: v1::IssueTrackerRunnerSettings {
                data_root: root_path.to_string_lossy().into_owned(),
                expected_contract_fingerprint: String::new(),
                expected_storage_schema_fingerprint: Tracker::schema_fingerprint(),
                lease_policy: None,
                pagination_policy: None,
                secret_filter_mode: v1::SecretFilterMode::Unspecified as i32,
                resource_limits: None,
                manager_settings: Some(manager_v1::IssueTrackerManagerSettings {
                    default_page_size: 10,
                    maximum_page_size: 20,
                    maximum_payload_bytes: 65_536,
                    maximum_repeated_item_count: 100,
                    operation_lease: Some(manager_v1::TimeSpan { millis: 10_000 }),
                    outbox_lease: Some(manager_v1::TimeSpan { millis: 10_000 }),
                    resource_limits: Some(manager_v1::ManagerResourceLimits {
                        max_string_bytes: 4096,
                        max_body_bytes: 65_536,
                        max_uri_bytes: 4096,
                        max_repeated_item_count: 100,
                        max_page_size: 100,
                    }),
                    retry_budget: Some(manager_v1::RetryBudget {
                        max_attempts: 3,
                        base_backoff: Some(manager_v1::TimeSpan { millis: 100 }),
                        max_backoff: Some(manager_v1::TimeSpan { millis: 1_000 }),
                    }),
                    secret_filter_mode: manager_v1::SecretFilterMode::FailClosed as i32,
                    ..Default::default()
                }),
                ownership_maintenance_interval: None,
                ownership_recovery_batch_size: 0,
            },
            maintenance: MaintenanceSettings::default(),
        };
        GrpcServer::new(IssueTrackerApplication::open(settings).await.unwrap())
    }

    fn apply_request() -> rpc_v1::ApplyExtractionRequest {
        rpc_v1::ApplyExtractionRequest {
            thread_id: "thread-1".into(),
            summary_ref: "summary:thread-1".into(),
            snapshot_digest: "workflow".into(),
            extract_schema: "lookback-issue-extract.v1".into(),
            model_id: "test-model".into(),
            prompt_version: "test-prompt-v1".into(),
            idempotency_key: "extract-rpc-test".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn disabled_extraction_returns_skipped_outcome() {
        let server = test_server().await;
        let mut request = apply_request();
        request.policy = Some(manager_v1::ExtractionPolicy {
            version: 1,
            extraction_enabled: false,
            extract_cap: 1,
            ..Default::default()
        });

        let response = IssueExtract::apply_extraction(&server, Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            response.outcome,
            Some(rpc_v1::apply_extraction_response::Outcome::Skipped(
                rpc_v1::SkippedExtraction { reason }
            )) if reason == rpc_v1::ExtractionSkipReason::ExtractionDisabled as i32
        ));
    }

    #[tokio::test]
    async fn missing_policy_returns_invalid_argument_outcome() {
        let server = test_server().await;
        let response = IssueExtract::apply_extraction(&server, Request::new(apply_request()))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            response.outcome,
            Some(rpc_v1::apply_extraction_response::Outcome::Error(error))
                if error.code == "invalid_argument"
        ));
    }

    #[tokio::test]
    async fn two_strong_candidates_are_registered_before_the_run_completes() {
        let server = test_server().await;
        let mut request = apply_request();
        request.allowed_messages = vec![
            rpc_v1::MessageDigest {
                memory_id: "message-1".into(),
                content: "first verified extraction source".into(),
                ..Default::default()
            },
            rpc_v1::MessageDigest {
                memory_id: "message-2".into(),
                content: "second verified extraction source".into(),
                ..Default::default()
            },
        ];
        request.input_message_ids = vec!["message-1".into(), "message-2".into()];
        request.policy = Some(manager_v1::ExtractionPolicy {
            version: 1,
            extraction_enabled: true,
            auto_register_min_confidence: Some(0.8),
            auto_register_priority_range: Some(manager_v1::ExtractionPriorityRange {
                lower: manager_v1::CandidatePriority::P0 as i32,
                upper: manager_v1::CandidatePriority::P2 as i32,
            }),
            extract_cap: 2,
            ..Default::default()
        });
        request.candidates = vec![
            strong_candidate("first extracted issue", "message-1"),
            rpc_v1::ExtractCandidateInput {
                suggested_state: "IN_PROGRESS".into(),
                ..strong_candidate("second extracted issue", "message-2")
            },
        ];

        let response = IssueExtract::apply_extraction(&server, Request::new(request))
            .await
            .unwrap()
            .into_inner();
        let applied = match response.outcome {
            Some(rpc_v1::apply_extraction_response::Outcome::Applied(applied)) => applied,
            outcome => panic!("expected applied extraction: {outcome:?}"),
        };
        assert_eq!(applied.candidate_ids.len(), 2, "{applied:?}");
        assert_eq!(applied.issue_ids.len(), 2, "{applied:?}");
        assert_eq!(
            applied.run_state,
            (manager_v1::ExtractionRunState::Completed as i32).to_string()
        );

        let mut manager = server.app.manager.as_ref().unwrap().lock().await;
        let repository = crate::manager::repository::ManagerRepository::new(manager.storage_mut());
        let candidates = repository.candidates().unwrap();
        for candidate_id in &applied.candidate_ids {
            let candidate = candidates
                .iter()
                .find(|candidate| candidate.candidate_id == *candidate_id)
                .expect("created candidate");
            assert_eq!(
                candidate.status,
                manager_v1::CandidateStatus::AutoRegistered as i32
            );
            let expected_memory_ids = match candidate_id.as_str() {
                "run-1-0" => vec!["message-1"],
                "run-1-1" => vec!["message-2"],
                _ => panic!("unexpected candidate ID: {candidate_id}"),
            };
            assert_eq!(
                candidate
                    .source_evidence
                    .iter()
                    .map(|reference| reference.memory_id.as_str())
                    .collect::<Vec<_>>(),
                expected_memory_ids
            );
            let mapping = repository
                .get(
                    crate::manager::repository::EntityKind::IssueMapping,
                    &crate::manager::repository::candidate_issue_id(candidate_id),
                )
                .unwrap()
                .expect("auto-registration must persist an issue mapping");
            let mapping = manager_v1::IssueMapping::decode(mapping.payload.as_slice()).unwrap();
            assert_eq!(mapping.candidate_id.as_deref(), Some(candidate_id.as_str()));
        }
        drop(manager);

        for candidate_id in &applied.candidate_ids {
            let issue_id = crate::manager::repository::candidate_issue_id(candidate_id);
            let result: v1::GetIssueResult = server
                .app
                .dispatch_v1(
                    "get_issue",
                    v1::GetIssueArgs {
                        issue_id,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let issue = match result.outcome {
                Some(v1::get_issue_result::Outcome::Success(details)) => {
                    details.issue.expect("generated Issue")
                }
                outcome => panic!("expected generated Issue: {outcome:?}"),
            };
            assert_eq!(issue.state, v1::IssueState::Open as i32);
            assert!(
                issue
                    .provenance_metadata
                    .iter()
                    .any(|entry| { entry.key == "candidate_id" && entry.value == *candidate_id })
            );
            assert!(
                issue
                    .provenance_metadata
                    .iter()
                    .any(|entry| { entry.key == "extraction_run_id" && entry.value == "run-1" })
            );
        }
    }

    fn strong_candidate(title: &str, locator_memory_id: &str) -> rpc_v1::ExtractCandidateInput {
        rpc_v1::ExtractCandidateInput {
            title: title.into(),
            description: "A detected unresolved task".into(),
            kind: "TASK".into(),
            suggested_priority: "P1".into(),
            suggested_state: "OPEN".into(),
            unfinished_reason: "The task remains unfinished".into(),
            completion: "The task is completed".into(),
            detection: "EXPLICIT".into(),
            confidence: 0.95,
            locator_memory_ids: vec![locator_memory_id.into()],
        }
    }
}
