//! Candidate resolution admission and completion.

use super::*;

impl<'a> ManagerRepository<'a> {
    pub(super) fn operation_id_for_idempotency_scope(
        &self,
        scope: &str,
    ) -> Result<Option<String>, pb::ManagerError> {
        self.storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT operation_id FROM manager_idempotency WHERE idempotency_scope = ?1",
                        params![scope],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(|error| map_repository_error(error.into()))
    }

    /// Return the durable operation identified by an idempotency scope.
    ///
    /// Callers which receive a replay or pending reservation must use the
    /// operation ID stored in the reservation rather than the freshly
    /// generated candidate ID from the retried request.  Keeping this lookup
    /// at the repository boundary prevents a retry from accidentally creating
    /// a second operation or reporting a non-existent one.
    pub(crate) fn durable_operation_for_idempotency_scope(
        &self,
        scope: &str,
    ) -> Result<Option<pb::DurableOperation>, pb::ManagerError> {
        let Some(operation_id) = self.operation_id_for_idempotency_scope(scope)? else {
            return Ok(None);
        };
        self.get_operation(&operation_id)
            .map_err(map_repository_error)
    }

    pub(crate) fn backend_request_for_idempotency_scope(
        &self,
        scope: &str,
    ) -> Result<Option<Vec<u8>>, pb::ManagerError> {
        let operation_id = self.operation_id_for_idempotency_scope(scope)?;
        operation_id
            .map(|operation_id| {
                self.backend_request(&operation_id)
                    .map_err(map_repository_error)
            })
            .transpose()
            .map(|request| request.flatten())
    }

    pub(crate) fn resolution_saga_for_idempotency_scope(
        &self,
        scope: &str,
    ) -> Result<Option<pb::ResolveCandidateArgs>, pb::ManagerError> {
        let operation_id = self.operation_id_for_idempotency_scope(scope)?;
        operation_id
            .map(|operation_id| {
                self.resolution_saga(&operation_id)
                    .map(|saga| saga.map(|saga| saga.args))
                    .map_err(map_repository_error)
            })
            .transpose()
            .map(|args| args.flatten())
    }

    pub(super) fn resolve_candidate_replay(
        &mut self,
        scope: &str,
        request_digest: &str,
    ) -> Result<Option<pb::ResolutionReceipt>, pb::ManagerError> {
        match <Self as GatewayRepository>::reserve_idempotency(self, scope, request_digest, "")? {
            IdempotencyDecision::New => Ok(None),
            IdempotencyDecision::Replay(payload) => {
                let result = pb::ResolveCandidateResult::decode(payload.as_slice())
                    .map_err(|error| domain_error(error.to_string()))?;
                match result.outcome {
                    Some(pb::resolve_candidate_result::Outcome::Success(receipt)) => {
                        Ok(Some(receipt))
                    }
                    _ => Err(gateway_error(
                        pb::ManagerErrorCode::ResolutionRequired,
                        "resolution idempotency replay has no receipt",
                    )),
                }
            }
            IdempotencyDecision::Pending { operation, .. } => {
                let candidate_id = match operation.scope.as_ref() {
                    Some(pb::durable_operation::Scope::CandidateId(candidate_id)) => candidate_id,
                    _ => {
                        return Err(gateway_error(
                            pb::ManagerErrorCode::ResolutionRequired,
                            "resolution operation has no candidate scope",
                        ));
                    }
                };
                if let Some(saga) = self
                    .resolution_saga(&operation.operation_id)
                    .map_err(map_repository_error)?
                {
                    let mappings =
                        if saga.decision == ResolutionSagaDecision::ConfirmAssociatedIssue {
                            vec![planned_mapping_reference(candidate_id, &saga.issue_id)]
                        } else {
                            Vec::new()
                        };
                    return Ok(Some(pb::ResolutionReceipt {
                        operation_id: operation.operation_id.clone(),
                        state: operation.state,
                        candidate: Some(pb::CandidateResolutionReference {
                            candidate_id: candidate_id.clone(),
                            state: pb::ResolutionReferenceState::Existing as i32,
                            revision: None,
                        }),
                        issues: vec![pb::IssueResolutionReference {
                            issue_id: saga.issue_id,
                            state: pb::ResolutionReferenceState::Existing as i32,
                            revision: None,
                        }],
                        mappings,
                    }));
                }
                let issue_id = self
                    .backend_request(&operation.operation_id)
                    .map_err(map_repository_error)?
                    .and_then(|request| DurableBackendRequest::decode(request.as_slice()).ok())
                    .and_then(|request| request.create_issue.map(|create| create.issue_id));
                Ok(Some(pb::ResolutionReceipt {
                    operation_id: operation.operation_id.clone(),
                    state: operation.state,
                    candidate: Some(pb::CandidateResolutionReference {
                        candidate_id: candidate_id.clone(),
                        state: pb::ResolutionReferenceState::Existing as i32,
                        revision: None,
                    }),
                    issues: issue_id
                        .as_deref()
                        .map(|issue_id| pb::IssueResolutionReference {
                            issue_id: issue_id.into(),
                            state: pb::ResolutionReferenceState::Reserved as i32,
                            revision: None,
                        })
                        .into_iter()
                        .collect(),
                    mappings: issue_id
                        .as_deref()
                        .map(|issue_id| planned_mapping_reference(candidate_id, issue_id))
                        .into_iter()
                        .collect(),
                }))
            }
        }
    }

    pub(super) fn validate_resolution_causal_parent(
        &self,
        args: &pb::ResolveCandidateArgs,
    ) -> Result<(), pb::ManagerError> {
        let Some(parent_id) = args.causal_parent.as_deref() else {
            return Ok(());
        };
        if parent_id.trim().is_empty() {
            return Err(domain_error("causal_parent must not be empty"));
        }
        let parent = self
            .get_operation(parent_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| domain_error("causal_parent operation was not found"))?;
        if parent.state != pb::DurableOperationState::ManualResolution as i32 {
            return Err(domain_error(
                "causal_parent must reference a manual-resolution operation",
            ));
        }
        match parent.scope {
            Some(pb::durable_operation::Scope::CandidateId(candidate_id))
                if candidate_id == args.candidate_id =>
            {
                Ok(())
            }
            _ => Err(domain_error(
                "causal_parent must have the same candidate scope",
            )),
        }
    }

    pub(super) fn validate_resolution_admission(
        &self,
        args: &pb::ResolveCandidateArgs,
        context: &pb::ManagerWriteContext,
        candidate: &pb::Candidate,
    ) -> Result<(), pb::ManagerError> {
        match args.decision.as_ref() {
            Some(pb::resolve_candidate_args::Decision::CreateIssue(create)) => {
                if candidate.status != pb::CandidateStatus::Proposed as i32 {
                    return Err(domain_error("only proposed candidates can create an issue"));
                }
                let issue = create
                    .issue
                    .as_ref()
                    .ok_or_else(|| domain_error("resolution issue draft is required"))?;
                validate_candidate_acceptance_issue(issue)?;
                validate_candidate_issue_state(issue, candidate)?;
                validate_resolution_basis(
                    context,
                    create.basis,
                    candidate,
                    Some(&issue.source_evidence),
                )?;
                self.ensure_unmapped_pair(
                    &candidate.candidate_id,
                    &candidate_issue_id(&candidate.candidate_id),
                )
            }
            Some(pb::resolve_candidate_args::Decision::ConfirmAssociatedIssue(confirm)) => {
                if !matches!(
                    pb::CandidateStatus::try_from(candidate.status),
                    Ok(pb::CandidateStatus::Proposed | pb::CandidateStatus::AutoRegistered)
                ) {
                    return Err(domain_error(
                        "only proposed or auto-registered candidates can confirm an associated issue",
                    ));
                }
                validate_confirm_basis(
                    context,
                    confirm.basis,
                    candidate,
                    &confirm.resolved_source_evidence,
                    self,
                )?;
                if candidate.status == pb::CandidateStatus::AutoRegistered as i32 {
                    self.ensure_mapping(&candidate.candidate_id, &confirm.issue_id)
                } else {
                    self.ensure_unmapped_pair(&candidate.candidate_id, &confirm.issue_id)
                }
            }
            Some(pb::resolve_candidate_args::Decision::Reject(reject)) => {
                if candidate.status != pb::CandidateStatus::Proposed as i32 {
                    return Err(domain_error("only proposed candidates can be rejected"));
                }
                if reject.reason_category == pb::ResolutionReasonCategory::Unspecified as i32 {
                    return Err(domain_error("resolution reason category is required"));
                }
                Ok(())
            }
            Some(pb::resolve_candidate_args::Decision::CancelAssociatedIssue(cancel)) => {
                if !matches!(
                    pb::CandidateStatus::try_from(candidate.status),
                    Ok(pb::CandidateStatus::AutoRegistered | pb::CandidateStatus::Accepted)
                ) {
                    return Err(domain_error(
                        "only auto-registered or accepted candidates can cancel an associated issue",
                    ));
                }
                if cancel.reason_category == pb::ResolutionReasonCategory::Unspecified as i32 {
                    return Err(domain_error("resolution reason category is required"));
                }
                self.ensure_mapping(&candidate.candidate_id, &cancel.issue_id)?;
                if cancel.expected_mapping_candidate_id != candidate.candidate_id
                    || cancel.expected_mapping_issue_id != cancel.issue_id
                {
                    return Err(domain_error("associated issue mapping precondition failed"));
                }
                Ok(())
            }
            Some(pb::resolve_candidate_args::Decision::Merge(merge)) => {
                validate_merge_resolution_shape(candidate, merge)
            }
            None => Err(domain_error("resolution decision is required")),
        }
    }

    pub(super) fn resolve_create_issue(
        &mut self,
        args: &pb::ResolveCandidateArgs,
        context: &pb::ManagerWriteContext,
        candidate: pb::Candidate,
        create: &pb::CreateIssueResolution,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        let mut issue = create
            .issue
            .clone()
            .ok_or_else(|| domain_error("resolution issue draft is required"))?;
        issue.source_evidence = super::super::candidates::union_source_evidence(
            &issue.source_evidence,
            &candidate.source_evidence,
            self.storage.source_evidence_limits(),
        )
        .map_err(|error| domain_error(format!("source evidence is invalid: {error:?}")))?;
        validate_candidate_acceptance_issue(&issue)?;
        validate_candidate_issue_state(&issue, &candidate)?;
        copy_candidate_snapshots_to_issue(&candidate.metadata, &mut issue.provenance_metadata)
            .map_err(domain_error)?;
        append_candidate_provenance(&mut issue.provenance_metadata, &candidate)
            .map_err(domain_error)?;
        normalize_candidate_issue_state(&mut issue, &candidate)?;
        validate_resolution_basis(
            context,
            create.basis,
            &candidate,
            Some(&issue.source_evidence),
        )?;
        let issue_id = candidate_issue_id(&candidate.candidate_id);
        self.ensure_unmapped_pair(&candidate.candidate_id, &issue_id)?;
        let mut backend_context = context.clone();
        backend_context.method = "manager_create_issue".into();
        backend_context.target = format!("issue:{issue_id}");
        let create_args = pb::CreateIssueArgs {
            context: Some(backend_context),
            issue_id: issue_id.clone(),
            issue: Some(issue),
            strong_detection_proof: candidate_detection_proof(&candidate)?,
            provenance_sources: Vec::new(),
        };
        self.admit_create_resolution(args, context, &candidate, &issue_id, create_args)
    }

    /// Admit the complete create-resolution saga before any canonical write is
    /// dispatched.  The candidate stays PROPOSED until the backend marker and
    /// the immutable mapping have both been committed by the finalizer.
    pub(super) fn admit_create_resolution(
        &mut self,
        args: &pb::ResolveCandidateArgs,
        context: &pb::ManagerWriteContext,
        candidate: &pb::Candidate,
        issue_id: &str,
        create_args: pb::CreateIssueArgs,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        let durable_request = super::super::backend_dispatch::durable_request_from_manager(
            "manager_create_issue",
            &create_args.encode_to_vec(),
        )
        .map_err(|error| domain_error(error.to_string()))?;
        let envelope = DurableBackendRequest::decode(durable_request.as_slice())
            .map_err(|error| domain_error(error.to_string()))?;
        let create = envelope
            .create_issue
            .as_ref()
            .ok_or_else(|| domain_error("durable create request is missing"))?;
        validate_issue_creation_request(
            create,
            envelope
                .context
                .as_ref()
                .and_then(|context| context.actor_audit.as_ref()),
        )?;

        let scope = IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            "manager_resolve_candidate",
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .map_err(|error| domain_error(error.to_string()))?;
        let request_digest = digest_bytes(&args.encode_to_vec());
        let operation_id = super::super::operations::new_operation_id();
        let now = now_millis();
        let mut operation = NewOperation {
            operation_id: operation_id.clone(),
            scope: Some(OperationScope::Candidate(candidate.candidate_id.clone())),
            method: "manager_resolve_candidate".into(),
            idempotency_scope: scope,
            request_digest,
            request_schema: "issue_tracker.manager.v1.ResolveCandidateArgs".into(),
            downstream_operation_id: Some(format!("backend-{operation_id}")),
            generation: self
                .storage
                .generation()
                .checked_add(1)
                .ok_or_else(|| domain_error("manager generation overflow"))?,
            manager_sequence: self.storage.manager_sequence(),
            causal_parent: args.causal_parent.clone(),
            affects_ready: true,
            sync_subject: Some(issue_id.into()),
            affected_issue_ids: vec![issue_id.into()],
            actor_audit: context.actor_audit.clone(),
            contract_fingerprint: context.contract_fingerprint.clone(),
            schema_fingerprint: context.schema_fingerprint.clone(),
            correlation_id: context.correlation_id.clone(),
            now_millis: now,
            replayable: true,
        }
        .build()
        .map_err(|error| domain_error(error.to_string()))?;
        operation.state = pb::DurableOperationState::Pending as i32;
        let outbox = outbox::new_entry(
            format!("outbox-{}", operation.operation_id),
            &operation,
            "taskchampion",
            operation.operation_id.clone(),
            digest_bytes(&durable_request),
            now,
        )
        .map_err(|error| domain_error(error.to_string()))?;
        let snapshot_mutation = pb::ManagerMutation {
            operation: Some(operation.clone()),
            result: Some(pb::manager_mutation::Result::CandidateAcceptance(
                pb::CandidateAcceptanceResult::default(),
            )),
            ..Default::default()
        };
        let mut backend_operation = operation.clone();
        backend_operation.method = "manager_create_issue".into();
        let mut expectation = super::super::backend_dispatch::derive_marker_expectation(
            &backend_operation,
            &create_args.encode_to_vec(),
        )
        .map_err(|error| domain_error(error.to_string()))?;
        expectation.contract_fingerprint = self.storage.backend_contract_fingerprint().into();
        self.admit_pending_acceptance(PendingAcceptance {
            operation: &operation,
            mutation: &snapshot_mutation,
            outbox: &outbox,
            request: &durable_request,
            expectation: &expectation,
            candidate_id: &candidate.candidate_id,
            candidate_revision: candidate.revision,
            history_decision: Some(ResolutionHistoryDecision {
                kind: pb::ResolutionDecisionKind::CreateIssue,
                candidate_id: &candidate.candidate_id,
                candidate_revision: candidate.revision,
            }),
        })
        .map_err(map_repository_error)?;
        Ok(pending_create_resolution_receipt(
            &operation,
            &candidate.candidate_id,
            issue_id,
        ))
    }

    pub(super) fn resolve_confirm_associated_issue(
        &mut self,
        args: &pb::ResolveCandidateArgs,
        context: &pb::ManagerWriteContext,
        candidate: pb::Candidate,
        confirm: &pb::ConfirmAssociatedIssueResolution,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        validate_confirm_basis(
            context,
            confirm.basis,
            &candidate,
            &confirm.resolved_source_evidence,
            self,
        )?;
        if candidate.status == pb::CandidateStatus::AutoRegistered as i32 {
            self.ensure_mapping(&candidate.candidate_id, &confirm.issue_id)?;
        } else {
            self.ensure_unmapped_pair(&candidate.candidate_id, &confirm.issue_id)?;
        }
        let mut backend_context = context.clone();
        backend_context.method = "manager_update_issue".into();
        backend_context.target = format!("issue:{}", confirm.issue_id);
        let update = pb::UpdateIssueArgs {
            context: Some(backend_context),
            issue_id: confirm.issue_id.clone(),
            expected_revision: confirm.expected_revision,
            update_mask: {
                let mut mask = confirm.update_mask.clone();
                if !mask.contains(&(pb::ManagerIssueField::SourceEvidence as i32)) {
                    mask.push(pb::ManagerIssueField::SourceEvidence as i32);
                }
                if !mask.contains(&(pb::ManagerIssueField::ConfirmationStatus as i32)) {
                    mask.push(pb::ManagerIssueField::ConfirmationStatus as i32);
                }
                mask
            },
            patch: Some(pb::ManagerIssuePatch {
                confirmation_status: Some(v1::ConfirmationStatus::Confirmed as i32),
                source_evidence: confirm.resolved_source_evidence.clone(),
                ..confirm.patch.clone().unwrap_or_default()
            }),
            target_state: confirm.target_state,
            terminal_report: confirm.terminal_report.clone().map(|report| match report {
                pb::confirm_associated_issue_resolution::TerminalReport::Completion(value) => {
                    pb::update_issue_args::TerminalReport::Completion(value)
                }
                pb::confirm_associated_issue_resolution::TerminalReport::Cancellation(value) => {
                    pb::update_issue_args::TerminalReport::Cancellation(value)
                }
            }),
        };
        self.admit_resolution_saga_backend(ResolutionSagaAdmission {
            args,
            context,
            candidate: &candidate,
            decision: ResolutionSagaDecision::ConfirmAssociatedIssue,
            backend_method: "manager_update_issue",
            backend_args: update.encode_to_vec(),
            issue_id: &confirm.issue_id,
        })
    }

    pub(super) fn resolve_cancel_associated_issue(
        &mut self,
        args: &pb::ResolveCandidateArgs,
        context: &pb::ManagerWriteContext,
        candidate: pb::Candidate,
        cancel: &pb::CancelAssociatedIssueResolution,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        if cancel.reason_category == pb::ResolutionReasonCategory::Unspecified as i32 {
            return Err(domain_error("resolution reason category is required"));
        }
        self.ensure_mapping(&candidate.candidate_id, &cancel.issue_id)?;
        if cancel.expected_mapping_candidate_id != candidate.candidate_id
            || cancel.expected_mapping_issue_id != cancel.issue_id
        {
            return Err(domain_error("associated issue mapping precondition failed"));
        }
        let mut backend_context = context.clone();
        backend_context.method = "manager_transition_issue".into();
        backend_context.target = format!("issue:{}", cancel.issue_id);
        let transition = pb::TransitionIssueArgs {
            context: Some(backend_context),
            issue_id: cancel.issue_id.clone(),
            expected_revision: cancel.expected_issue_revision,
            target_state: v1::IssueState::Canceled as i32,
            terminal_report: Some(pb::transition_issue_args::TerminalReport::Cancellation(
                cancel.cancellation_report.clone().unwrap_or_default(),
            )),
            ..Default::default()
        };
        self.admit_resolution_saga_backend(ResolutionSagaAdmission {
            args,
            context,
            candidate: &candidate,
            decision: ResolutionSagaDecision::CancelAssociatedIssue,
            backend_method: "manager_transition_issue",
            backend_args: transition.encode_to_vec(),
            issue_id: &cancel.issue_id,
        })
    }

    pub(super) fn admit_resolution_saga_backend(
        &mut self,
        admission: ResolutionSagaAdmission<'_>,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        let ResolutionSagaAdmission {
            args,
            context,
            candidate,
            decision,
            backend_method,
            backend_args,
            issue_id,
        } = admission;
        let durable_request = super::super::backend_dispatch::durable_request_from_manager(
            backend_method,
            &backend_args,
        )
        .map_err(|error| domain_error(error.to_string()))?;
        validate_durable_request_snapshot(backend_method, &durable_request)
            .map_err(|error| domain_error(error.to_string()))?;
        let scope = IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            "manager_resolve_candidate",
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .map_err(|error| domain_error(error.to_string()))?;
        let operation_id = super::super::operations::new_operation_id();
        let now = now_millis();
        let mut operation = NewOperation {
            operation_id: operation_id.clone(),
            scope: Some(OperationScope::Candidate(candidate.candidate_id.clone())),
            method: "manager_resolve_candidate".into(),
            idempotency_scope: scope,
            request_digest: digest_bytes(&args.encode_to_vec()),
            request_schema: "issue_tracker.manager.v1.ResolveCandidateArgs".into(),
            downstream_operation_id: Some(format!("backend-{operation_id}")),
            generation: self
                .storage
                .generation()
                .checked_add(1)
                .ok_or_else(|| domain_error("manager generation overflow"))?,
            manager_sequence: self.storage.manager_sequence(),
            causal_parent: args.causal_parent.clone(),
            affects_ready: true,
            sync_subject: Some(issue_id.into()),
            affected_issue_ids: vec![issue_id.into()],
            actor_audit: context.actor_audit.clone(),
            contract_fingerprint: context.contract_fingerprint.clone(),
            schema_fingerprint: context.schema_fingerprint.clone(),
            correlation_id: context.correlation_id.clone(),
            now_millis: now,
            replayable: true,
        }
        .build()
        .map_err(|error| domain_error(error.to_string()))?;
        operation.state = pb::DurableOperationState::Pending as i32;
        let outbox = outbox::new_entry(
            format!("outbox-{}", operation.operation_id),
            &operation,
            "taskchampion",
            operation.operation_id.clone(),
            digest_bytes(&durable_request),
            now,
        )
        .map_err(|error| domain_error(error.to_string()))?;
        let snapshot_mutation = pb::ManagerMutation {
            operation: Some(operation.clone()),
            ..Default::default()
        };
        let mut backend_operation = operation.clone();
        backend_operation.method = backend_method.into();
        let mut expectation = super::super::backend_dispatch::derive_marker_expectation(
            &backend_operation,
            &backend_args,
        )
        .map_err(|error| domain_error(error.to_string()))?;
        expectation.contract_fingerprint = self.storage.backend_contract_fingerprint().into();
        self.commit_resolution_saga_admission(
            &snapshot_mutation,
            &operation,
            &outbox,
            &durable_request,
            &expectation,
            ResolutionSaga {
                candidate_id: candidate.candidate_id.clone(),
                candidate_revision: candidate.revision,
                decision,
                issue_id: issue_id.into(),
                args: args.clone(),
            },
        )
        .map_err(map_repository_error)?;
        Ok(pending_resolution_saga_receipt(
            &operation, candidate, issue_id, decision,
        ))
    }

    #[allow(dead_code)]
    pub(super) fn admit_resolution_backend(
        &mut self,
        args: &pb::ResolveCandidateArgs,
        method: &str,
        payload: Vec<u8>,
        mut receipt: pb::ResolutionReceipt,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        let admission = super::super::gateway::Admission {
            scope: String::new(),
            request_digest: digest_bytes(&args.encode_to_vec()),
            operation_id: super::super::operations::new_operation_id(),
            decision: IdempotencyDecision::New,
        };
        let mutation = GatewayRepository::dispatch_backend(self, method, &payload, &admission)?;
        let operation = mutation.operation.ok_or_else(|| {
            domain_error("resolution backend admission did not return an operation")
        })?;
        receipt.operation_id = operation.operation_id;
        receipt.state = operation.state;
        Ok(receipt)
    }

    pub(super) fn ensure_unmapped_pair(
        &self,
        candidate_id: &str,
        issue_id: &str,
    ) -> Result<(), pb::ManagerError> {
        let mappings = self
            .list_entities(EntityKind::IssueMapping)
            .map_err(map_repository_error)?;
        for stored in mappings {
            let mapping: pb::IssueMapping =
                decode_stored(stored, "issue_tracker.manager.v1.IssueMapping")?;
            if mapping.issue_id == issue_id || mapping.candidate_id.as_deref() == Some(candidate_id)
            {
                return Err(domain_error("candidate or issue is already associated"));
            }
        }
        Ok(())
    }

    pub(super) fn ensure_mapping(
        &self,
        candidate_id: &str,
        issue_id: &str,
    ) -> Result<(), pb::ManagerError> {
        let stored = self
            .get(EntityKind::IssueMapping, issue_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| domain_error("associated issue mapping is missing"))?;
        let mapping: pb::IssueMapping =
            decode_stored(stored, "issue_tracker.manager.v1.IssueMapping")?;
        if mapping.candidate_id.as_deref() != Some(candidate_id) {
            return Err(domain_error(
                "associated issue mapping does not match candidate",
            ));
        }
        Ok(())
    }
}

pub(super) enum SupersedingTarget<'a> {
    Candidate(&'a str),
    Issue(&'a str),
}

pub(super) fn supersede_candidate(
    candidate: &pb::Candidate,
    target: SupersedingTarget<'_>,
    updated_at: pb::UtcInstant,
) -> Result<pb::Candidate, &'static str> {
    let mut superseded = candidate.clone();
    superseded.revision = superseded
        .revision
        .checked_add(1)
        .ok_or("candidate revision overflow")?;
    superseded.status = pb::CandidateStatus::Superseded as i32;
    superseded.decision_reason = pb::CandidateDecisionReason::Superseded as i32;
    superseded.superseded_by = Some(match target {
        SupersedingTarget::Candidate(candidate_id) => {
            pb::candidate::SupersededBy::SupersedingCandidateId(candidate_id.into())
        }
        SupersedingTarget::Issue(issue_id) => {
            pb::candidate::SupersededBy::SupersedingIssueId(issue_id.into())
        }
    });
    superseded.updated_at = Some(updated_at);
    Ok(superseded)
}

#[allow(dead_code)]
pub(super) fn planned_issue_reference(issue_id: &str) -> pb::IssueResolutionReference {
    pb::IssueResolutionReference {
        issue_id: issue_id.into(),
        state: pb::ResolutionReferenceState::Planned as i32,
        revision: None,
    }
}

pub(super) fn planned_mapping_reference(
    candidate_id: &str,
    issue_id: &str,
) -> pb::IssueMappingResolutionReference {
    pb::IssueMappingResolutionReference {
        candidate_id: candidate_id.into(),
        issue_id: issue_id.into(),
        state: pb::ResolutionReferenceState::Planned as i32,
        issue_revision: None,
    }
}

pub(super) fn pending_create_resolution_receipt(
    operation: &pb::DurableOperation,
    candidate_id: &str,
    issue_id: &str,
) -> pb::ResolutionReceipt {
    pb::ResolutionReceipt {
        operation_id: operation.operation_id.clone(),
        state: operation.state,
        candidate: Some(pb::CandidateResolutionReference {
            candidate_id: candidate_id.into(),
            state: pb::ResolutionReferenceState::Existing as i32,
            revision: None,
        }),
        issues: vec![pb::IssueResolutionReference {
            issue_id: issue_id.into(),
            state: pb::ResolutionReferenceState::Reserved as i32,
            revision: None,
        }],
        mappings: vec![planned_mapping_reference(candidate_id, issue_id)],
    }
}

pub(super) fn committed_create_resolution_receipt(
    operation: &pb::DurableOperation,
    candidate: &pb::Candidate,
    mapping: &pb::IssueMapping,
) -> pb::ResolutionReceipt {
    pb::ResolutionReceipt {
        operation_id: operation.operation_id.clone(),
        state: pb::DurableOperationState::Committed as i32,
        candidate: Some(pb::CandidateResolutionReference {
            candidate_id: candidate.candidate_id.clone(),
            state: pb::ResolutionReferenceState::Existing as i32,
            revision: Some(candidate.revision),
        }),
        issues: vec![pb::IssueResolutionReference {
            issue_id: mapping.issue_id.clone(),
            state: pb::ResolutionReferenceState::Existing as i32,
            revision: Some(mapping.revision),
        }],
        mappings: vec![pb::IssueMappingResolutionReference {
            candidate_id: candidate.candidate_id.clone(),
            issue_id: mapping.issue_id.clone(),
            state: pb::ResolutionReferenceState::Existing as i32,
            issue_revision: Some(mapping.revision),
        }],
    }
}

pub(super) fn pending_resolution_saga_receipt(
    operation: &pb::DurableOperation,
    candidate: &pb::Candidate,
    issue_id: &str,
    decision: ResolutionSagaDecision,
) -> pb::ResolutionReceipt {
    let mapping = if decision == ResolutionSagaDecision::ConfirmAssociatedIssue {
        vec![planned_mapping_reference(&candidate.candidate_id, issue_id)]
    } else {
        Vec::new()
    };
    pb::ResolutionReceipt {
        operation_id: operation.operation_id.clone(),
        state: operation.state,
        candidate: Some(pb::CandidateResolutionReference {
            candidate_id: candidate.candidate_id.clone(),
            state: pb::ResolutionReferenceState::Existing as i32,
            revision: None,
        }),
        issues: vec![pb::IssueResolutionReference {
            issue_id: issue_id.into(),
            state: pb::ResolutionReferenceState::Existing as i32,
            revision: None,
        }],
        mappings: mapping,
    }
}

pub(super) fn committed_resolution_saga_receipt(
    operation: &pb::DurableOperation,
    candidate: &pb::Candidate,
    mapping: Option<&pb::IssueMapping>,
    issue_id: &str,
    issue_revision: Option<u64>,
) -> pb::ResolutionReceipt {
    let (issues, mappings) = mapping.map_or_else(
        || {
            (
                vec![pb::IssueResolutionReference {
                    issue_id: issue_id.into(),
                    state: pb::ResolutionReferenceState::Existing as i32,
                    revision: issue_revision,
                }],
                Vec::new(),
            )
        },
        |mapping| {
            (
                vec![pb::IssueResolutionReference {
                    issue_id: mapping.issue_id.clone(),
                    state: pb::ResolutionReferenceState::Existing as i32,
                    revision: Some(mapping.revision),
                }],
                vec![pb::IssueMappingResolutionReference {
                    candidate_id: candidate.candidate_id.clone(),
                    issue_id: mapping.issue_id.clone(),
                    state: pb::ResolutionReferenceState::Existing as i32,
                    issue_revision: Some(mapping.revision),
                }],
            )
        },
    );
    pb::ResolutionReceipt {
        operation_id: operation.operation_id.clone(),
        state: pb::DurableOperationState::Committed as i32,
        candidate: Some(pb::CandidateResolutionReference {
            candidate_id: candidate.candidate_id.clone(),
            state: pb::ResolutionReferenceState::Existing as i32,
            revision: Some(candidate.revision),
        }),
        issues,
        mappings,
    }
}

pub(super) fn require_human_review(
    context: &pb::ManagerWriteContext,
    _candidate: &pb::Candidate,
    _issue_refs: Option<&[v1::SourceEvidenceRef]>,
) -> Result<(), pb::ManagerError> {
    let audit = context
        .actor_audit
        .as_ref()
        .ok_or_else(|| domain_error("human-review resolution requires actor audit"))?;
    if audit.actor_kind != v1::ActorKind::Human as i32
        || audit
            .reason
            .as_ref()
            .is_none_or(|reason| reason.trim().is_empty())
    {
        return Err(domain_error(
            "human-review requires a human actor and audit reason",
        ));
    }
    Ok(())
}

pub(super) fn require_auto_review(
    context: &pb::ManagerWriteContext,
    candidate: &pb::Candidate,
    issue_refs: Option<&[v1::SourceEvidenceRef]>,
) -> Result<(), pb::ManagerError> {
    let audit = context
        .actor_audit
        .as_ref()
        .ok_or_else(|| domain_error("auto-review resolution requires actor audit"))?;
    let has_refs =
        !candidate.source_evidence.is_empty() || issue_refs.is_some_and(|refs| !refs.is_empty());
    if audit.actor_kind != v1::ActorKind::InternalAgent as i32 || !has_refs {
        return Err(domain_error(
            "auto-review requires an internal agent and source evidence",
        ));
    }
    Ok(())
}

pub(super) fn validate_confirm_basis(
    context: &pb::ManagerWriteContext,
    basis: i32,
    candidate: &pb::Candidate,
    issue_refs: &[v1::SourceEvidenceRef],
    _repository: &ManagerRepository<'_>,
) -> Result<(), pb::ManagerError> {
    validate_resolution_basis(context, basis, candidate, Some(issue_refs))
}

pub(super) fn validate_resolution_basis(
    context: &pb::ManagerWriteContext,
    basis: i32,
    candidate: &pb::Candidate,
    issue_refs: Option<&[v1::SourceEvidenceRef]>,
) -> Result<(), pb::ManagerError> {
    match pb::ResolutionBasis::try_from(basis) {
        Ok(pb::ResolutionBasis::SourceEvidence) if !candidate.source_evidence.is_empty() => Ok(()),
        Ok(pb::ResolutionBasis::SourceEvidence) => Err(domain_error(
            "source-evidence resolution requires candidate source evidence",
        )),
        Ok(pb::ResolutionBasis::HumanReview) => {
            require_human_review(context, candidate, issue_refs)
        }
        Ok(pb::ResolutionBasis::AutoReview) => require_auto_review(context, candidate, issue_refs),
        _ => Err(domain_error("resolution basis is required")),
    }
}

pub(super) fn validate_merge_resolution_shape(
    source: &pb::Candidate,
    merge: &pb::MergeResolution,
) -> Result<(), pb::ManagerError> {
    if !matches!(
        pb::ResolutionBasis::try_from(merge.basis),
        Ok(pb::ResolutionBasis::SourceEvidence
            | pb::ResolutionBasis::HumanReview
            | pb::ResolutionBasis::AutoReview)
    ) {
        return Err(domain_error("resolution basis is required"));
    }
    if !matches!(
        pb::CandidateStatus::try_from(source.status),
        Ok(pb::CandidateStatus::Proposed
            | pb::CandidateStatus::AutoRegistered
            | pb::CandidateStatus::Accepted)
    ) {
        return Err(domain_error(
            "only proposed, auto-registered, or accepted candidates can merge",
        ));
    }
    match merge.target.as_ref() {
        Some(pb::merge_resolution::Target::Candidate(target)) => {
            if target.candidate_id.is_empty() || target.expected_revision.is_none() {
                return Err(domain_error(
                    "target candidate ID and expected_revision are required",
                ));
            }
            if target.candidate_id == source.candidate_id {
                return Err(domain_error("merge target cannot be the source candidate"));
            }
        }
        Some(pb::merge_resolution::Target::Issue(target)) => {
            if target.issue_id.is_empty() || target.expected_revision.is_none() {
                return Err(domain_error(
                    "target issue ID and expected_revision are required",
                ));
            }
        }
        None => return Err(domain_error("merge target is required")),
    }
    Ok(())
}

#[cfg(test)]
mod source_evidence_tests {
    use super::super::gateway_adapter::candidate_matches;
    use super::*;

    fn reference(thread_id: &str, memory_id: &str) -> v1::SourceEvidenceRef {
        v1::SourceEvidenceRef {
            thread_id: thread_id.into(),
            memory_id: memory_id.into(),
        }
    }

    fn candidate(references: Vec<v1::SourceEvidenceRef>) -> pb::Candidate {
        pb::Candidate {
            candidate_id: "candidate-1".into(),
            status: pb::CandidateStatus::Proposed as i32,
            source_evidence: references,
            ..Default::default()
        }
    }

    fn merge(
        target: pb::merge_resolution::Target,
        basis: pb::ResolutionBasis,
    ) -> pb::MergeResolution {
        pb::MergeResolution {
            target: Some(target),
            basis: basis as i32,
            ..Default::default()
        }
    }

    #[test]
    fn merge_requires_a_recognized_resolution_basis_for_every_target_type() {
        let source = candidate(vec![reference("thread-1", "memory-1")]);
        for target in [
            pb::merge_resolution::Target::Candidate(pb::CandidateMergeTarget {
                candidate_id: "candidate-2".into(),
                expected_revision: Some(0),
            }),
            pb::merge_resolution::Target::Issue(pb::IssueMergeTarget {
                issue_id: "issue-1".into(),
                expected_revision: Some(0),
                ..Default::default()
            }),
        ] {
            let error = validate_merge_resolution_shape(
                &source,
                &merge(target, pb::ResolutionBasis::Unspecified),
            )
            .expect_err("merge basis must be explicit");
            assert!(error.message.contains("resolution basis"));
        }
    }

    #[test]
    fn merge_basis_enforces_source_human_and_automatic_review_requirements() {
        let source = candidate(Vec::new());
        let target = merge(
            pb::merge_resolution::Target::Candidate(pb::CandidateMergeTarget {
                candidate_id: "candidate-2".into(),
                expected_revision: Some(0),
            }),
            pb::ResolutionBasis::SourceEvidence,
        );
        let empty_context = pb::ManagerWriteContext::default();
        assert!(validate_resolution_basis(&empty_context, target.basis, &source, None).is_err());

        let human_context = pb::ManagerWriteContext {
            actor_audit: Some(pb::ActorAudit {
                actor_id: "human-1".into(),
                actor_kind: v1::ActorKind::Human as i32,
                reason: Some("reviewed merge".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let human_merge = merge(
            pb::merge_resolution::Target::Issue(pb::IssueMergeTarget {
                issue_id: "issue-1".into(),
                expected_revision: Some(0),
                ..Default::default()
            }),
            pb::ResolutionBasis::HumanReview,
        );
        assert!(
            validate_resolution_basis(&human_context, human_merge.basis, &source, None).is_ok()
        );

        let automatic_context = pb::ManagerWriteContext {
            actor_audit: Some(pb::ActorAudit {
                actor_id: "agent-1".into(),
                actor_kind: v1::ActorKind::InternalAgent as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        let automatic_merge = merge(
            pb::merge_resolution::Target::Issue(pb::IssueMergeTarget {
                issue_id: "issue-1".into(),
                expected_revision: Some(0),
                ..Default::default()
            }),
            pb::ResolutionBasis::AutoReview,
        );
        assert!(
            validate_resolution_basis(
                &automatic_context,
                automatic_merge.basis,
                &source,
                Some(&[reference("thread-1", "memory-1")]),
            )
            .is_ok()
        );

        let sourced_candidate = candidate(vec![reference("thread-1", "memory-1")]);
        assert!(
            validate_resolution_basis(&empty_context, target.basis, &sourced_candidate, None,)
                .is_ok()
        );
    }

    #[test]
    fn supersede_transition_sets_the_target_specific_reference() {
        let source = candidate(vec![reference("thread-1", "memory-1")]);
        let updated_at = pb::UtcInstant {
            unix_epoch_millis: 42,
        };
        let candidate_target = supersede_candidate(
            &source,
            SupersedingTarget::Candidate("candidate-2"),
            updated_at.clone(),
        )
        .expect("candidate target supersedes source");
        assert_eq!(candidate_target.revision, source.revision + 1);
        assert_eq!(
            candidate_target.status,
            pb::CandidateStatus::Superseded as i32
        );
        assert_eq!(
            candidate_target.decision_reason,
            pb::CandidateDecisionReason::Superseded as i32
        );
        assert!(matches!(
            candidate_target.superseded_by,
            Some(pb::candidate::SupersededBy::SupersedingCandidateId(ref id)) if id == "candidate-2"
        ));
        assert_eq!(candidate_target.updated_at, Some(updated_at));

        let issue_target = supersede_candidate(
            &source,
            SupersedingTarget::Issue("issue-1"),
            pb::UtcInstant {
                unix_epoch_millis: 43,
            },
        )
        .expect("issue target supersedes source");
        assert!(matches!(
            issue_target.superseded_by,
            Some(pb::candidate::SupersededBy::SupersedingIssueId(ref id)) if id == "issue-1"
        ));
    }

    #[test]
    fn candidate_filter_matches_the_exact_source_reference() {
        let candidate = candidate(vec![
            reference("thread-1", "memory-1"),
            reference("thread-2", "memory-2"),
        ]);
        let matching = pb::CandidateFilter {
            source_evidence: Some(reference("thread-2", "memory-2")),
            ..Default::default()
        };
        let different_memory = pb::CandidateFilter {
            source_evidence: Some(reference("thread-2", "memory-other")),
            ..Default::default()
        };
        let blank = pb::CandidateFilter {
            source_evidence: Some(reference(" ", "memory-2")),
            ..Default::default()
        };

        assert!(candidate_matches(&candidate, Some(&matching)));
        assert!(!candidate_matches(&candidate, Some(&different_memory)));
        assert!(!candidate_matches(&candidate, Some(&blank)));
    }

    #[test]
    fn candidate_acceptance_requires_an_explicit_confirmed_issue() {
        let mut issue = v1::IssueDraft {
            creation_basis: v1::CreationBasis::AutoDetected as i32,
            confirmation_status: v1::ConfirmationStatus::Unreviewed as i32,
            ..Default::default()
        };
        assert!(validate_candidate_acceptance_issue(&issue).is_err());
        issue.creation_basis = v1::CreationBasis::Explicit as i32;
        issue.confirmation_status = v1::ConfirmationStatus::Confirmed as i32;
        assert!(validate_candidate_acceptance_issue(&issue).is_ok());
    }

    #[test]
    fn direct_issue_create_rejects_source_evidence_above_manager_limit() {
        let mut request = pb::CreateIssueArgs {
            issue_id: "issue-direct".into(),
            issue: Some(v1::IssueDraft {
                title: "direct issue".into(),
                source_evidence: vec![
                    reference("thread", "memory"),
                    reference("thread", "memory"),
                    reference("thread", "memory"),
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            normalize_direct_issue_create_source_evidence(
                &mut request,
                super::super::super::config::SourceEvidenceLimits {
                    max_string_bytes: 64,
                    max_repeated_item_count: 2,
                },
            )
            .is_err()
        );
        request
            .issue
            .as_mut()
            .expect("draft remains available")
            .source_evidence
            .truncate(2);
        normalize_direct_issue_create_source_evidence(
            &mut request,
            super::super::super::config::SourceEvidenceLimits {
                max_string_bytes: 64,
                max_repeated_item_count: 2,
            },
        )
        .expect("bounded direct create source evidence");
        assert_eq!(
            request
                .issue
                .expect("draft remains available")
                .source_evidence
                .len(),
            1
        );
    }

    #[test]
    fn direct_issue_update_rejects_source_evidence_above_manager_limit() {
        let mut request = pb::UpdateIssueArgs {
            update_mask: vec![pb::ManagerIssueField::SourceEvidence as i32],
            patch: Some(pb::ManagerIssuePatch {
                source_evidence: vec![
                    reference("thread", "memory"),
                    reference("thread", "memory"),
                    reference("thread", "memory"),
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        let limits = super::super::super::config::SourceEvidenceLimits {
            max_string_bytes: 64,
            max_repeated_item_count: 2,
        };
        assert!(normalize_direct_issue_update_source_evidence(&mut request, limits).is_err());

        request
            .patch
            .as_mut()
            .expect("patch remains available")
            .source_evidence
            .truncate(2);
        normalize_direct_issue_update_source_evidence(&mut request, limits)
            .expect("bounded source evidence");
        assert_eq!(
            request
                .patch
                .expect("patch remains available")
                .source_evidence
                .len(),
            1
        );
    }
}
