//! Gateway repository trait implementation and resolution validation.

use super::*;

fn resolve_candidate_success_result(receipt: &pb::ResolutionReceipt) -> (Vec<u8>, pb::Snapshot) {
    let payload = pb::ResolveCandidateResult {
        outcome: Some(pb::resolve_candidate_result::Outcome::Success(
            receipt.clone(),
        )),
    }
    .encode_to_vec();
    let snapshot = pb::Snapshot {
        payload: payload.clone(),
        schema: "issue_tracker.manager.v1.ResolveCandidateResult".into(),
    };
    (payload, snapshot)
}

impl<'a> GatewayRepository for ManagerRepository<'a> {
    fn reserve_idempotency(
        &mut self,
        scope: &str,
        request_digest: &str,
        _operation_id: &str,
    ) -> Result<IdempotencyDecision, pb::ManagerError> {
        let record = self
            .storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT idempotency.request_digest,
                                idempotency.result_snapshot,
                                operation.state,
                                operation.operation_payload,
                                operation.snapshot,
                                operation.snapshot_schema,
                                operation.snapshot_digest,
                                operation.method
                           FROM manager_idempotency AS idempotency
                      LEFT JOIN manager_operation AS operation
                             ON operation.operation_id = idempotency.operation_id
                          WHERE idempotency.idempotency_scope = ?1",
                        params![scope],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, Option<Vec<u8>>>(1)?,
                                row.get::<_, Option<i32>>(2)?,
                                row.get::<_, Option<Vec<u8>>>(3)?,
                                row.get::<_, Option<Vec<u8>>>(4)?,
                                row.get::<_, Option<String>>(5)?,
                                row.get::<_, Option<String>>(6)?,
                                row.get::<_, Option<String>>(7)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(|error| map_repository_error(error.into()))?;
        match record {
            None => Ok(IdempotencyDecision::New),
            Some((digest, Some(result), Some(state), ..))
                if digest == request_digest
                    && matches!(
                        pb::DurableOperationState::try_from(state),
                        Ok(pb::DurableOperationState::Committed
                            | pb::DurableOperationState::Failed
                            | pb::DurableOperationState::ManualResolution
                            | pb::DurableOperationState::Arbitrated)
                    ) =>
            {
                Ok(IdempotencyDecision::Replay(result))
            }
            Some((
                digest,
                None,
                Some(state),
                operation_payload,
                snapshot,
                snapshot_schema,
                snapshot_digest,
                operation_method,
            )) if digest == request_digest => {
                if operation_method.as_deref().is_some_and(|method| {
                    matches!(
                        method,
                        "manager_apply_issue_admin_action" | "manager_resolve_issue_operation"
                    )
                }) && snapshot.is_none()
                    && matches!(
                        pb::DurableOperationState::try_from(state),
                        Ok(pb::DurableOperationState::Committed
                            | pb::DurableOperationState::Failed
                            | pb::DurableOperationState::Arbitrated)
                    )
                {
                    // A terminal operation without a replay snapshot is
                    // corrupt/incomplete, not a fresh Pending reservation.
                    // Never let a retry mutate it (or a manual-resolution
                    // request rewrite the terminal state); expose the
                    // durable inconsistency for reconciliation instead.
                    return Err(gateway_error(
                        pb::ManagerErrorCode::ResultUnconfirmed,
                        "terminal idempotency operation has no replayable result",
                    ));
                }
                let operation_payload = operation_payload.ok_or_else(|| {
                    gateway_error(
                        pb::ManagerErrorCode::ResolutionRequired,
                        "pending idempotency record has no durable operation",
                    )
                })?;
                let snapshot = snapshot.ok_or_else(|| {
                    gateway_error(
                        pb::ManagerErrorCode::ResolutionRequired,
                        "pending idempotency record has no mutation snapshot",
                    )
                })?;
                let snapshot_schema = snapshot_schema.ok_or_else(|| {
                    gateway_error(
                        pb::ManagerErrorCode::ResolutionRequired,
                        "pending idempotency record has no mutation snapshot schema",
                    )
                })?;
                let snapshot_digest = snapshot_digest.ok_or_else(|| {
                    gateway_error(
                        pb::ManagerErrorCode::ResolutionRequired,
                        "pending idempotency record has no mutation snapshot digest",
                    )
                })?;
                if snapshot_schema != MANAGER_MUTATION_SNAPSHOT_SCHEMA
                    || digest_bytes(&snapshot) != snapshot_digest
                {
                    return Err(gateway_error(
                        pb::ManagerErrorCode::ResolutionRequired,
                        "pending idempotency mutation snapshot is invalid",
                    ));
                }
                let operation = pb::DurableOperation::decode(operation_payload.as_slice())
                    .map_err(|_| {
                        gateway_error(
                            pb::ManagerErrorCode::ResolutionRequired,
                            "pending idempotency durable operation cannot be decoded",
                        )
                    })?;
                if operation.idempotency_scope != scope
                    || operation.request_digest != request_digest
                    || operation.operation_id.is_empty()
                {
                    return Err(gateway_error(
                        pb::ManagerErrorCode::ResolutionRequired,
                        "pending idempotency durable operation does not match its reservation",
                    ));
                }
                if matches!(
                    pb::DurableOperationState::try_from(operation.state),
                    Ok(pb::DurableOperationState::Committed
                        | pb::DurableOperationState::Failed
                        | pb::DurableOperationState::Arbitrated)
                ) && operation.result_replayability
                    == v1::OperationResultReplayability::NotReplayable as i32
                {
                    // Non-replayable capability operations intentionally have
                    // no idempotency result. A terminal row must not be
                    // exposed as a fresh Pending reservation that could
                    // reissue a lease.
                    return Err(gateway_error(
                        pb::ManagerErrorCode::ResultUnconfirmed,
                        "idempotency operation is terminal without a replayable result",
                    ));
                }
                pb::ManagerMutation::decode(snapshot.as_slice()).map_err(|_| {
                    gateway_error(
                        pb::ManagerErrorCode::ResolutionRequired,
                        "pending idempotency mutation cannot be decoded",
                    )
                })?;
                Ok(IdempotencyDecision::Pending {
                    operation: Box::new(operation),
                    mutation_snapshot: Box::new(pb::Snapshot {
                        payload: snapshot,
                        schema: snapshot_schema,
                    }),
                })
            }
            Some((digest, ..)) if digest == request_digest => Err(gateway_error(
                pb::ManagerErrorCode::ResolutionRequired,
                "idempotency operation is not confirmed with a terminal result",
            )),
            Some(_) => Err(gateway_error(
                pb::ManagerErrorCode::IdempotencyConflict,
                "idempotency scope conflicts with a different request digest",
            )),
        }
    }

    fn complete_idempotency(
        &mut self,
        scope: &str,
        method: &str,
        request_digest: &str,
        operation_id: &str,
        snapshot: &pb::Snapshot,
    ) -> Result<pb::DurableOperation, pb::ManagerError> {
        if let Some(operation) = self
            .get_operation(operation_id)
            .map_err(map_repository_error)?
        {
            return Ok(operation);
        }
        let mutation = pb::ManagerMutation::decode(snapshot.payload.as_slice())
            .map_err(|error| domain_error(error.to_string()))?;
        let entity_scope = mutation.entity.as_ref().and_then(|entity| match entity {
            pb::manager_mutation::Entity::Candidate(value) => Some(
                pb::durable_operation::Scope::CandidateId(value.candidate_id.clone()),
            ),
            pb::manager_mutation::Entity::ExtractionRun(value) => Some(
                pb::durable_operation::Scope::ExtractionRunId(value.run_id.clone()),
            ),
            _ => None,
        });
        let mut operation =
            manager_only_operation(method, entity_scope, scope, request_digest, operation_id);
        operation.state = pb::DurableOperationState::Committed as i32;
        let mutation_to_commit = mutation.clone();
        if method == "manager_update_candidate" {
            self.commit_candidate_update_mutation(&mutation_to_commit, &operation)
        } else {
            self.commit_mutation_with_removal(
                &mutation_to_commit,
                &operation,
                None,
                None,
                None,
                None,
                None,
            )
        }
        .map_err(map_repository_error)?;
        self.get_operation(operation_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                gateway_error(
                    pb::ManagerErrorCode::OperationNotFound,
                    "operation not found",
                )
            })
    }

    fn store_idempotent_result(
        &mut self,
        scope: &str,
        request_digest: &str,
        operation_id: &str,
        result: &[u8],
    ) -> Result<(), pb::ManagerError> {
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                let changed = transaction.execute(
                    "UPDATE manager_idempotency
                        SET result_snapshot = ?2, updated_at = ?3
                      WHERE idempotency_scope = ?1
                        AND request_digest = ?4
                        AND operation_id = ?5
                        AND result_snapshot IS NULL",
                    params![scope, result, now_millis(), request_digest, operation_id,],
                )?;
                if changed != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                Ok(())
            })
            .map_err(map_repository_error)?;
        Ok(())
    }

    fn abort_idempotency_reservation(
        &mut self,
        scope: &str,
        request_digest: &str,
        operation_id: &str,
    ) -> Result<(), pb::ManagerError> {
        ManagerRepository::abort_idempotency_reservation(self, scope, request_digest, operation_id)
            .map_err(map_repository_error)
    }

    fn dispatch_backend(
        &mut self,
        method: &str,
        args: &[u8],
        admission: &super::super::gateway::Admission,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        match method {
            "manager_create_issue" => {
                let mut create = pb::CreateIssueArgs::decode(args).map_err(|error| {
                    domain_error(format!("create issue decode failed: {error}"))
                })?;
                normalize_direct_issue_create_source_evidence(
                    &mut create,
                    self.storage.source_evidence_limits(),
                )?;
            }
            "manager_update_issue" => {
                let mut update = pb::UpdateIssueArgs::decode(args).map_err(|error| {
                    domain_error(format!("update issue decode failed: {error}"))
                })?;
                normalize_direct_issue_update_source_evidence(
                    &mut update,
                    self.storage.source_evidence_limits(),
                )?;
            }
            _ => {}
        }
        if method == "manager_update_issue" {
            let args = pb::UpdateIssueArgs::decode(args)
                .map_err(|error| domain_error(format!("update issue decode failed: {error}")))?;
            if args
                .update_mask
                .contains(&(pb::ManagerIssueField::ProvenanceMetadata as i32))
            {
                return Err(domain_error(
                    "source label provenance metadata is immutable outside server-owned flows",
                ));
            }
        }
        if method == "manager_claim_issue" {
            let claim = pb::ClaimIssueArgs::decode(args)
                .map_err(|error| domain_error(format!("claim request decode failed: {error}")))?;
            validate_gateway_eligibility_generation(
                claim.gateway_eligibility_generation,
                self.storage.generation(),
            )?;
        }
        let backend_write = backend_write(method, args)?;
        let durable_request =
            super::super::backend_dispatch::durable_request_from_manager(method, args)
                .map_err(|error| domain_error(error.to_string()))?;
        if method == "manager_create_issue" {
            let envelope = DurableBackendRequest::decode(durable_request.as_slice())
                .map_err(|error| domain_error(format!("create request decode failed: {error}")))?;
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
        }
        let context = backend_write.context;
        let issue_id = backend_write.issue_id;
        let now = now_millis();
        let scope = IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            method,
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .map_err(|error| domain_error(error.to_string()))?;
        let mut operation = NewOperation {
            operation_id: admission.operation_id.clone(),
            scope: Some(OperationScope::Issue(issue_id.clone())),
            method: method.into(),
            idempotency_scope: scope,
            request_digest: admission.request_digest.clone(),
            request_schema: "issue_tracker.manager.v1.DurableBackendRequest".into(),
            downstream_operation_id: Some(format!("backend-{}", admission.operation_id)),
            // The enclosing SQLite commit advances the generation once. The
            // outbox schema rejects generation zero, so build the operation
            // with the generation that this admission will publish.
            generation: self
                .storage
                .generation()
                .checked_add(1)
                .ok_or_else(|| domain_error("manager generation overflow"))?,
            manager_sequence: self.storage.manager_sequence(),
            causal_parent: None,
            affects_ready: true,
            sync_subject: Some(issue_id.clone()),
            affected_issue_ids: vec![issue_id.clone()],
            actor_audit: context.actor_audit.clone(),
            contract_fingerprint: context.contract_fingerprint.clone(),
            schema_fingerprint: context.schema_fingerprint.clone(),
            correlation_id: context.correlation_id.clone(),
            now_millis: now,
            replayable: method != "manager_claim_issue",
        }
        .build()
        .map_err(|error| domain_error(error.to_string()))?;
        // Backend work becomes dispatchable only after this operation and its
        // outbox record are made durable by the enclosing transaction.
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
        let mutation = pb::ManagerMutation {
            operation: Some(operation.clone()),
            ..Default::default()
        };
        let mut expectation =
            super::super::backend_dispatch::derive_marker_expectation(&operation, args)
                .map_err(|error| domain_error(error.to_string()))?;
        expectation.contract_fingerprint = self.storage.backend_contract_fingerprint().into();
        self.commit_backend_admission_with_extraction_reservation(BackendAdmission {
            mutation: &mutation,
            operation: &operation,
            outbox: &outbox,
            request: &durable_request,
            request_schema: "issue_tracker.manager.v1.DurableBackendRequest",
            expectation: &expectation,
            extraction_reservation: (method == "manager_create_issue").then_some(
                ExtractionReservationIdentity {
                    issue_id: issue_id.as_str(),
                    idempotency_key: context.idempotency_key.as_str(),
                },
            ),
        })
        .map_err(map_repository_error)?;
        Ok(mutation)
    }

    fn create_candidate(
        &mut self,
        args: &pb::CreateCandidateArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        let draft = args
            .candidate
            .as_ref()
            .ok_or_else(|| domain_error("candidate draft is required"))?;
        if draft
            .metadata
            .iter()
            .any(|entry| entry.key == super::super::source_labels::SOURCE_LABEL_SNAPSHOTS_KEY)
        {
            return Err(domain_error(
                "source label provenance metadata is server-owned",
            ));
        }
        if self
            .get(EntityKind::Candidate, &draft.candidate_id)
            .map_err(map_repository_error)?
            .is_some()
        {
            return Err(gateway_error(
                pb::ManagerErrorCode::OperationConflict,
                "candidate already exists",
            ));
        }
        let candidate = CandidateStore::with_limits(self.storage.source_evidence_limits())
            .create(draft, utc_instant(now_millis()))
            .map_err(|error| domain_error(format!("candidate validation failed: {error:?}")))?;
        Ok(manager_mutation(pb::manager_mutation::Entity::Candidate(
            candidate,
        )))
    }

    fn update_candidate(
        &mut self,
        args: &pb::UpdateCandidateArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        let expected = args
            .expected_revision
            .ok_or_else(|| domain_error("expected_revision is required"))?;
        let patch = args
            .patch
            .as_ref()
            .ok_or_else(|| domain_error("candidate patch is required"))?;
        let current = self
            .get(EntityKind::Candidate, &args.candidate_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                gateway_error(
                    pb::ManagerErrorCode::CandidateNotFound,
                    "candidate not found",
                )
            })?;
        let current: pb::Candidate = decode_stored(current, "issue_tracker.manager.v1.Candidate")?;
        let mut store = CandidateStore::from_candidates_with_limits(
            self.candidates()?,
            self.storage.source_evidence_limits(),
        );
        let candidate = store
            .update(
                &args.candidate_id,
                expected,
                &args.update_mask,
                patch,
                utc_instant(now_millis()),
            )
            .map_err(|error| domain_error(format!("candidate update failed: {error:?}")))?;
        if current.strong_detection_proof.is_some() && candidate.strong_detection_proof.is_none() {
            self.ensure_candidate_auto_registration_is_not_in_flight(&candidate.candidate_id)
                .map_err(map_repository_error)?;
        }
        Ok(manager_mutation(pb::manager_mutation::Entity::Candidate(
            candidate,
        )))
    }

    fn resolve_candidate(
        &mut self,
        args: &pb::ResolveCandidateArgs,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
        let expected_revision = args
            .expected_revision
            .ok_or_else(|| domain_error("expected_revision is required"))?;
        let context = args
            .context
            .as_ref()
            .ok_or_else(|| domain_error("write context is required"))?;
        let scope = IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            "manager_resolve_candidate",
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .map_err(|error| domain_error(error.to_string()))?
        .canonical();
        let request_digest = digest_bytes(&args.encode_to_vec());

        // A completed retry must remain replayable even though its original
        // Candidate revision is no longer current. This lookup does not write
        // an idempotency record; a new operation is admitted only below after
        // all resolution preconditions have been validated.
        if let Some(receipt) = self.resolve_candidate_replay(&scope, &request_digest)? {
            return Ok(receipt);
        }
        let candidate = self
            .get(EntityKind::Candidate, &args.candidate_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                gateway_error(
                    pb::ManagerErrorCode::CandidateNotFound,
                    "candidate not found",
                )
            })?;
        let mut candidate: pb::Candidate =
            decode_stored(candidate, "issue_tracker.manager.v1.Candidate")?;
        if candidate.revision != expected_revision {
            return Err(gateway_error(
                pb::ManagerErrorCode::RevisionConflict,
                "candidate revision conflict",
            ));
        }
        self.validate_resolution_causal_parent(args)?;
        self.validate_resolution_admission(args, context, &candidate)?;

        // The repository currently implements reservation as a pure lookup;
        // keep this check at the admission boundary so implementations with a
        // durable reservation cannot observe an invalid resolution request.
        if let Some(receipt) = self.resolve_candidate_replay(&scope, &request_digest)? {
            return Ok(receipt);
        }

        match args.decision.as_ref() {
            Some(pb::resolve_candidate_args::Decision::CreateIssue(create)) => {
                if candidate.status != pb::CandidateStatus::Proposed as i32 {
                    return Err(domain_error("only proposed candidates can create an issue"));
                }
                self.resolve_create_issue(args, context, candidate, create)
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
                self.resolve_confirm_associated_issue(args, context, candidate, confirm)
            }
            Some(pb::resolve_candidate_args::Decision::Reject(reject)) => {
                if candidate.status != pb::CandidateStatus::Proposed as i32 {
                    return Err(domain_error("only proposed candidates can be rejected"));
                }
                if reject.reason_category == pb::ResolutionReasonCategory::Unspecified as i32 {
                    return Err(domain_error("resolution reason category is required"));
                }
                candidate.revision = candidate
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| domain_error("candidate revision overflow"))?;
                candidate.status = pb::CandidateStatus::Rejected as i32;
                candidate.decision_reason = pb::CandidateDecisionReason::Rejected as i32;
                candidate.decision_note = reject.rationale.clone();
                candidate.updated_at = Some(utc_instant(now_millis()));

                let mut operation = manager_only_operation(
                    "manager_resolve_candidate",
                    Some(pb::durable_operation::Scope::CandidateId(
                        candidate.candidate_id.clone(),
                    )),
                    &scope,
                    &request_digest,
                    &super::super::operations::new_operation_id(),
                );
                operation.state = pb::DurableOperationState::Committed as i32;
                operation.causal_parent = args.causal_parent.clone();
                let receipt = pb::ResolutionReceipt {
                    operation_id: operation.operation_id.clone(),
                    state: operation.state,
                    candidate: Some(pb::CandidateResolutionReference {
                        candidate_id: candidate.candidate_id.clone(),
                        state: pb::ResolutionReferenceState::Existing as i32,
                        revision: Some(candidate.revision),
                    }),
                    ..Default::default()
                };
                let (typed_result, result_snapshot) = resolve_candidate_success_result(&receipt);
                operation.terminal_snapshot = Some(
                    pb::durable_operation::TerminalSnapshot::ResultSnapshot(result_snapshot),
                );
                let mutation = pb::ManagerMutation {
                    entity: Some(pb::manager_mutation::Entity::Candidate(candidate.clone())),
                    operation: Some(operation.clone()),
                    ..Default::default()
                };
                let history_audit = ResolutionHistoryAudit {
                    resolution_kind: "reject",
                    reason_category: resolution_reason_category(reject.reason_category)
                        .map_err(map_repository_error)?,
                    rationale: normalized_resolution_rationale(reject.rationale.as_deref())
                        .map_err(map_repository_error)?,
                };
                self.commit_mutation_with_history_resolution(
                    &mutation,
                    &operation,
                    &typed_result,
                    &history_audit,
                    ResolutionHistoryDecision {
                        kind: pb::ResolutionDecisionKind::Reject,
                        candidate_id: &candidate.candidate_id,
                        candidate_revision: candidate
                            .revision
                            .checked_sub(1)
                            .ok_or_else(|| domain_error("candidate revision overflow"))?,
                    },
                )
                .map_err(map_repository_error)?;
                Ok(receipt)
            }
            Some(pb::resolve_candidate_args::Decision::Merge(merge)) => {
                self.resolve_merge(args, &scope, &request_digest, candidate, merge)
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
                self.resolve_cancel_associated_issue(args, context, candidate, cancel)
            }
            _ => Err(gateway_error(
                pb::ManagerErrorCode::BackendUnavailable,
                "candidate resolution decision is not implemented",
            )),
        }
    }
    fn resolve_candidate_replay(
        &mut self,
        scope: &str,
        request_digest: &str,
    ) -> Result<Option<pb::ResolutionReceipt>, pb::ManagerError> {
        ManagerRepository::resolve_candidate_replay(self, scope, request_digest)
    }

    fn validate_resolution_causal_parent(
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

    fn validate_resolution_admission(
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

    fn get_candidate(
        &self,
        args: &pb::GetCandidateArgs,
    ) -> Result<pb::Candidate, pb::ManagerError> {
        let stored = self
            .get(EntityKind::Candidate, &args.candidate_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                gateway_error(
                    pb::ManagerErrorCode::CandidateNotFound,
                    "candidate not found",
                )
            })?;
        decode_stored(stored, "issue_tracker.manager.v1.Candidate")
    }

    fn list_candidates(
        &self,
        args: &pb::ListCandidatesArgs,
        page_size_limits: PageSizeLimits,
    ) -> Result<pb::CandidatePage, pb::ManagerError> {
        let candidates = self
            .candidates()?
            .into_iter()
            .filter(|candidate| candidate_matches(candidate, args.filter.as_ref()))
            .collect::<Vec<_>>();
        let page = manager_entity_page(
            self.storage,
            ManagerEntityPageRequest {
                request: args.page.as_ref(),
                filter: args.filter.as_ref(),
                context: args.context.as_ref(),
                order_fingerprint: "list-candidates-candidate-id-asc",
                method: "manager_list_candidates",
            },
            candidates,
            page_size_limits,
        )?;
        Ok(pb::CandidatePage {
            candidates: page.items,
            page: Some(page.info),
        })
    }

    fn create_extraction_run(
        &mut self,
        args: &pb::CreateExtractionRunArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        let draft = args
            .run
            .as_ref()
            .ok_or_else(|| domain_error("extraction run draft is required"))?;
        let existing = self.extraction_runs()?;
        let mut store = ExtractionRunStore::from_runs(existing);
        let run = store
            .create(draft, utc_instant(now_millis()))
            .map_err(|error| {
                domain_error(format!("extraction run validation failed: {error:?}"))
            })?;
        if self
            .get(EntityKind::ExtractionRun, &run.run_id)
            .map_err(map_repository_error)?
            .is_some()
        {
            return Err(gateway_error(
                pb::ManagerErrorCode::OperationConflict,
                "extraction run ID already exists",
            ));
        }
        Ok(manager_mutation(
            pb::manager_mutation::Entity::ExtractionRun(run),
        ))
    }

    fn advance_extraction_run(
        &mut self,
        args: &pb::AdvanceExtractionRunArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        let expected = args
            .expected_revision
            .ok_or_else(|| domain_error("expected_revision is required"))?;
        let advance = args
            .advance
            .as_ref()
            .ok_or_else(|| domain_error("extraction advance is required"))?;
        let mut store = ExtractionRunStore::from_runs(self.extraction_runs()?);
        let run = store
            .advance(&args.run_id, expected, advance, utc_instant(now_millis()))
            .map_err(|error| domain_error(format!("extraction advance failed: {error:?}")))?;
        Ok(manager_mutation(
            pb::manager_mutation::Entity::ExtractionRun(run),
        ))
    }

    fn resume_extraction_run(
        &mut self,
        args: &pb::ResumeExtractionRunArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        let expected = args
            .expected_revision
            .ok_or_else(|| domain_error("expected_revision is required"))?;
        let mut store = ExtractionRunStore::from_runs(self.extraction_runs()?);
        let run = store
            .advance(
                &args.run_id,
                expected,
                &pb::ExtractionRunAdvance {
                    target_state: pb::ExtractionRunState::Running as i32,
                    ..Default::default()
                },
                utc_instant(now_millis()),
            )
            .map_err(|error| domain_error(format!("extraction resume failed: {error:?}")))?;
        Ok(manager_mutation(
            pb::manager_mutation::Entity::ExtractionRun(run),
        ))
    }

    fn get_extraction_run(
        &self,
        args: &pb::GetExtractionRunArgs,
    ) -> Result<pb::ExtractionRun, pb::ManagerError> {
        let stored = self
            .get(EntityKind::ExtractionRun, &args.run_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                gateway_error(
                    pb::ManagerErrorCode::ExtractionRunNotFound,
                    "extraction run not found",
                )
            })?;
        decode_stored(stored, "issue_tracker.manager.v1.ExtractionRun")
    }

    fn list_extraction_runs(
        &self,
        args: &pb::ListExtractionRunsArgs,
        page_size_limits: PageSizeLimits,
    ) -> Result<pb::ExtractionRunPage, pb::ManagerError> {
        let runs = self
            .extraction_runs()?
            .into_iter()
            .filter(|run| extraction_run_matches(run, args.filter.as_ref()))
            .collect::<Vec<_>>();
        let page = manager_entity_page(
            self.storage,
            ManagerEntityPageRequest {
                request: args.page.as_ref(),
                filter: args.filter.as_ref(),
                context: args.context.as_ref(),
                order_fingerprint: "list-extraction-runs-run-id-asc",
                method: "manager_list_extraction_runs",
            },
            runs,
            page_size_limits,
        )?;
        Ok(pb::ExtractionRunPage {
            runs: page.items,
            page: Some(page.info),
        })
    }

    fn get_operation(
        &self,
        args: &pb::GetOperationArgs,
    ) -> Result<pb::DurableOperation, pb::ManagerError> {
        match self.get_operation(&args.operation_id) {
            Ok(Some(operation)) => Ok(operation),
            Ok(None) => Err(gateway_error(
                pb::ManagerErrorCode::OperationNotFound,
                "operation not found",
            )),
            Err(error) => {
                // A quarantined operation may retain an undecodable payload.
                // Return its independently stored, non-secret diagnosis so a
                // trusted consumer can obtain repair conditions without
                // pretending the corrupt DurableOperation is readable.
                if let Some(diagnosis) = self
                    .operation_diagnosis(&args.operation_id)
                    .map_err(map_repository_error)?
                {
                    return Err(pb::ManagerError {
                        code: pb::ManagerErrorCode::ResolutionRequired as i32,
                        message: diagnosis.repair_condition.clone(),
                        retryable: false,
                        detail: Some(pb::manager_error::Detail::OperationDiagnosis(diagnosis)),
                    });
                }
                Err(map_repository_error(error))
            }
        }
    }

    fn list_operation_history(
        &self,
        args: &pb::ListOperationHistoryArgs,
        root_operation_history_max_window_millis: i64,
        page_size_limits: PageSizeLimits,
    ) -> Result<pb::OperationHistoryPage, pb::ManagerError> {
        self.list_operation_history_with_compat(
            args,
            root_operation_history_max_window_millis,
            page_size_limits,
            true,
        )
    }

    fn list_operation_history_with_compat(
        &self,
        args: &pb::ListOperationHistoryArgs,
        root_operation_history_max_window_millis: i64,
        page_size_limits: PageSizeLimits,
        operation_history_v1_compat_enabled: bool,
    ) -> Result<pb::OperationHistoryPage, pb::ManagerError> {
        const OPERATION_HISTORY_ORDER_FINGERPRINT: &str = "created-at-desc-operation-id-desc";
        let filter = args
            .filter
            .as_ref()
            .ok_or_else(|| domain_error("operation history filter is required"))?;
        super::super::operations::validate_operation_history_filter(
            filter,
            root_operation_history_max_window_millis,
        )
        .map_err(|error| domain_error(error.to_string()))?;
        let page = args.page.as_ref().cloned().unwrap_or_default();
        let generation = self.storage.generation();
        let snapshot_id = format!("manager-{generation}");
        let query_fingerprint = operation_history_query_fingerprint(filter, args.context.as_ref());
        let secret = super::super::canonical_service::list_page_token_secret(self.storage);
        let cursor = page
            .page_token
            .as_deref()
            .map(|token| {
                decode_operation_history_cursor(
                    token,
                    &secret,
                    HistoryCursorBoundary {
                        generation,
                        snapshot_id: &snapshot_id,
                        order_fingerprint: OPERATION_HISTORY_ORDER_FINGERPRINT,
                        query_fingerprint: &query_fingerprint,
                    },
                    operation_history_v1_compat_enabled,
                    now_millis(),
                )
            })
            .transpose()
            .map_err(operation_history_page_token_error)?;
        let page_size = page_size_limits.resolve(page.page_size);
        let keyset = match cursor.as_ref() {
            Some(HistoryCursor::Keyset(cursor)) => Some(cursor),
            _ => None,
        };
        let offset = match cursor.as_ref() {
            Some(HistoryCursor::Offset(offset)) => Some(*offset),
            _ => None,
        };
        let (operation_ids, has_more, total_size) = operation_history_operation_ids_page(
            self.storage,
            filter,
            page_size,
            keyset,
            offset,
            args.include_total_size.unwrap_or(true),
        )
        .map_err(|_| domain_error("operation history read failed"))?;
        let hydrated = load_operation_history_page(self.storage, &operation_ids)
            .map_err(|_| domain_error("operation history read failed"))?;
        let entries = super::super::operations::build_operation_history_entries_with_decisions(
            &hydrated.operations,
            filter,
            root_operation_history_max_window_millis,
            &hydrated.resolutions,
            &hydrated.decisions,
        )
        .map_err(|_| domain_error("operation history read failed"))?;
        let codec = OperationHistoryPageTokenCodec::new(secret);
        let next_page_token = has_more.then(|| {
            let last = entries.last().expect("a non-empty page has a continuation");
            codec.encode(&OperationHistoryPageCursor {
                created_at_unix_epoch_millis: last
                    .created_at
                    .as_ref()
                    .map_or(0, |instant| instant.unix_epoch_millis),
                operation_id: last.operation_id.clone(),
                generation,
                snapshot_id: snapshot_id.clone(),
                order_fingerprint: OPERATION_HISTORY_ORDER_FINGERPRINT.into(),
                query_fingerprint: query_fingerprint.clone(),
                version: 2,
                expires_at_unix_epoch_millis: i64::MAX,
            })
        });
        Ok(pb::OperationHistoryPage {
            entries,
            page: Some(pb::PageInfo {
                next_page_token,
                total_size,
                read_revision: generation,
                snapshot: Some(pb::PageSnapshot {
                    snapshot_id,
                    stable_traversal: Some(pb::StableTraversal {
                        order_fingerprint: OPERATION_HISTORY_ORDER_FINGERPRINT.into(),
                    }),
                    read_generation: generation,
                    read_revision_watermark: 0,
                }),
            }),
        })
    }

    fn retry_operation(
        &mut self,
        args: &pb::RetryOperationArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        let mut operation = self
            .get_operation(&args.operation_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                gateway_error(
                    pb::ManagerErrorCode::OperationNotFound,
                    "operation not found",
                )
            })?;
        if args
            .expected_generation
            .is_some_and(|expected| expected != operation.generation)
        {
            return Err(gateway_error(
                pb::ManagerErrorCode::GenerationConflict,
                "operation generation does not match the requested retry",
            ));
        }

        let state = pb::DurableOperationState::try_from(operation.state).map_err(|_| {
            gateway_error(
                pb::ManagerErrorCode::InvalidStateTransition,
                "operation has an invalid state",
            )
        })?;
        if state == pb::DurableOperationState::ManualResolution {
            return Err(gateway_error(
                pb::ManagerErrorCode::ResolutionRequired,
                "manual-resolution operations cannot be retried automatically",
            ));
        }
        if state == pb::DurableOperationState::Unconfirmed {
            return Err(gateway_error(
                pb::ManagerErrorCode::ResolutionRequired,
                "delivery-unknown operations require marker reconciliation",
            ));
        }
        if !matches!(
            state,
            pb::DurableOperationState::Failed | pb::DurableOperationState::Pending
        ) {
            return Err(gateway_error(
                pb::ManagerErrorCode::InvalidStateTransition,
                "operation is not recoverable",
            ));
        }

        let expected_operation_generation = operation.generation;
        if state == pb::DurableOperationState::Failed
            && pb::FailureClass::try_from(operation.failure_class).ok()
                != Some(pb::FailureClass::Retryable)
        {
            return Err(gateway_error(
                pb::ManagerErrorCode::ResolutionRequired,
                "terminal failures require manual resolution",
            ));
        }

        let outbox_id = self
            .storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT outbox_id FROM manager_outbox WHERE operation_id = ?1",
                        params![&args.operation_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(|error| map_repository_error(error.into()))?
            .ok_or_else(|| {
                gateway_error(
                    pb::ManagerErrorCode::OutboxNotFound,
                    "operation outbox not found",
                )
            })?;
        let mut outbox = self
            .get_outbox(&outbox_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                gateway_error(
                    pb::ManagerErrorCode::OutboxNotFound,
                    "operation outbox not found",
                )
            })?;
        let outbox_status = pb::OutboxStatus::try_from(outbox.status).map_err(|_| {
            gateway_error(
                pb::ManagerErrorCode::ResolutionRequired,
                "operation outbox has an invalid status",
            )
        })?;
        if outbox_status == pb::OutboxStatus::DeadLetter {
            return Err(gateway_error(
                pb::ManagerErrorCode::ResolutionRequired,
                "dead-letter outboxes cannot be retried automatically",
            ));
        }
        if !matches!(
            outbox_status,
            pb::OutboxStatus::Ready | pb::OutboxStatus::Leased | pb::OutboxStatus::Dispatched
        ) {
            return Err(gateway_error(
                pb::ManagerErrorCode::InvalidStateTransition,
                "operation outbox is not recoverable",
            ));
        }

        if state == pb::DurableOperationState::Failed {
            transition_operation(
                &mut operation,
                pb::DurableOperationState::Received,
                Some(pb::FailureClass::Retryable),
                now_millis(),
            )
            .map_err(|error| {
                gateway_error(
                    pb::ManagerErrorCode::InvalidStateTransition,
                    error.to_string(),
                )
            })?;
            operation.terminal_snapshot = None;
        }
        outbox.status = pb::OutboxStatus::Ready as i32;
        outbox.lease_id = None;
        outbox.lease_expires_at = None;
        outbox.next_attempt_at = Some(utc_instant(now_millis()));
        outbox.error_code = pb::ManagerErrorCode::ManagerErrorUnspecified as i32;

        let mut committed = operation.clone();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                // Re-read the row version in the write transaction. The
                // generation check above validates the caller's receipt; this
                // value is the SQL CAS guard that prevents a stale retry from
                // re-queuing an operation after another lifecycle transition.
                let (current_row_generation, current_payload): (i64, Vec<u8>) = transaction.query_row(
                    "SELECT generation, operation_payload FROM manager_operation WHERE operation_id = ?1",
                    params![committed.operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let current_operation = pb::DurableOperation::decode(current_payload.as_slice())
                    .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                if current_operation.generation != expected_operation_generation
                    || current_operation.state != state as i32
                {
                    return Err(RepositoryError::GenerationConflict {
                        expected: expected_operation_generation,
                        actual: current_operation.generation,
                    });
                }
                committed.manager_sequence = stamp.manager_sequence;
                committed.generation = stamp.generation;
                let payload = committed.encode_to_vec();
                let updated = transaction.execute(
                    "UPDATE manager_operation
                        SET state = ?2, attempt = ?3, manager_sequence = ?4, generation = ?5,
                            result_snapshot = NULL, error_snapshot = NULL, updated_at = ?6,
                            result_snapshot_schema = NULL, result_snapshot_digest = NULL,
                            error_snapshot_schema = NULL, error_snapshot_digest = NULL,
                            operation_payload = ?7,
                            operation_payload_schema = 'issue_tracker.manager.v1.DurableOperation',
                            operation_payload_digest = ?8
                      WHERE operation_id = ?1
                        AND generation = ?9
                        AND state = ?10",
                    params![
                        committed.operation_id,
                        committed.state,
                        i64::from(committed.attempt),
                        i64::try_from(committed.manager_sequence)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(committed.generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        operation_updated_at_millis(&committed, now_millis()),
                        payload,
                        digest_bytes(&committed.encode_to_vec()),
                        current_row_generation,
                        state as i32,
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::GenerationConflict {
                        expected: operation.generation,
                        actual: u64::try_from(current_row_generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                    });
                }
                super::domain::upsert_operation_history_projection(
                    transaction,
                    &committed,
                    now_millis(),
                )?;
                update_outbox_in_transaction(transaction, &outbox)?;
                transaction.execute(
                    "UPDATE manager_idempotency
                        SET state = ?2, error_snapshot = NULL, updated_at = ?3
                      WHERE operation_id = ?1",
                    params![committed.operation_id, committed.state, now_millis()],
                )?;
                Ok(())
            })
            .map_err(map_repository_error)?;
        Ok(pb::ManagerMutation {
            operation: Some(committed),
            ..Default::default()
        })
    }

    fn reconcile_operation(
        &mut self,
        args: &pb::ReconcileOperationArgs,
    ) -> Result<pb::ManagerMutation, pb::ManagerError> {
        let operation_id = args.operation_id.clone();
        let expected_generation = args.expected_generation;
        if operation_id.is_empty() {
            return Err(gateway_error(
                pb::ManagerErrorCode::OperationNotFound,
                "operation not found",
            ));
        }

        let mut committed = None;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                // Re-read the durable payload and row generation while holding
                // the write transaction. The check outside this transaction is
                // only an admission hint and cannot protect a concurrent retry.
                let (row_generation, payload, schema, digest): (i64, Vec<u8>, String, String) =
                    transaction.query_row(
                        "SELECT generation, operation_payload, operation_payload_schema,
                                operation_payload_digest
                           FROM manager_operation
                          WHERE operation_id = ?1",
                        params![operation_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )?;
                let row_generation =
                    u64::try_from(row_generation).map_err(|_| RepositoryError::RevisionOverflow)?;
                if schema != "issue_tracker.manager.v1.DurableOperation"
                    || digest_bytes(&payload) != digest
                {
                    return Err(RepositoryError::RequestSnapshot(
                        "operation payload integrity validation failed".into(),
                    ));
                }
                let current = pb::DurableOperation::decode(payload.as_slice())
                    .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                if let Some(expected) = expected_generation
                    && expected != current.generation
                {
                    return Err(RepositoryError::GenerationConflict {
                        expected,
                        actual: current.generation,
                    });
                }
                if pb::DurableOperationState::try_from(current.state).ok()
                    != Some(pb::DurableOperationState::Unconfirmed)
                {
                    return Err(RepositoryError::RequestSnapshot(
                        "only unconfirmed operations can be reconciled".into(),
                    ));
                }

                let mut operation = current;
                transition_operation(
                    &mut operation,
                    pb::DurableOperationState::ManualResolution,
                    None,
                    now_millis(),
                )
                .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
                operation.failure_class = pb::FailureClass::Terminal as i32;
                operation.terminal_snapshot = Some(
                    pb::durable_operation::TerminalSnapshot::ErrorSnapshot(pb::Snapshot {
                        payload: pb::ManagerError {
                            code: pb::ManagerErrorCode::ResolutionRequired as i32,
                            message: "backend marker requires explicit resolution".into(),
                            retryable: false,
                            detail: None,
                        }
                        .encode_to_vec(),
                        schema: "issue_tracker.manager.v1.ManagerError".into(),
                    }),
                );
                let operation = sanitize_operation(&operation)?;
                let operation_payload = operation.encode_to_vec();
                let error_snapshot = operation
                    .terminal_snapshot
                    .as_ref()
                    .and_then(|snapshot| match snapshot {
                        pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value) => {
                            Some(value)
                        }
                        pb::durable_operation::TerminalSnapshot::ResultSnapshot(_) => None,
                    })
                    .ok_or_else(|| {
                        RepositoryError::RequestSnapshot("error snapshot is missing".into())
                    })?;
                let changed = transaction.execute(
                    "UPDATE manager_operation
                        SET state = ?2, attempt = ?3, manager_sequence = ?4,
                            generation = ?5, updated_at = ?6,
                            result_snapshot = NULL, result_snapshot_schema = NULL,
                            result_snapshot_digest = NULL,
                            error_snapshot = ?7, error_snapshot_schema = ?8,
                            error_snapshot_digest = ?9, operation_payload = ?10,
                            operation_payload_schema = ?11,
                            operation_payload_digest = ?12
                      WHERE operation_id = ?1
                        AND generation = ?13
                        AND state = ?14",
                    params![
                        operation.operation_id,
                        operation.state,
                        i64::from(operation.attempt),
                        i64::try_from(stamp.manager_sequence)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(stamp.generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        operation_updated_at_millis(&operation, now_millis()),
                        error_snapshot.payload.as_slice(),
                        error_snapshot.schema,
                        digest_bytes(&error_snapshot.payload),
                        operation_payload.as_slice(),
                        "issue_tracker.manager.v1.DurableOperation",
                        digest_bytes(&operation_payload),
                        row_generation,
                        pb::DurableOperationState::Unconfirmed as i32,
                    ],
                )?;
                if changed != 1 {
                    return Err(RepositoryError::GenerationConflict {
                        expected: operation.generation,
                        actual: row_generation,
                    });
                }
                super::domain::upsert_operation_history_projection(
                    transaction,
                    &operation,
                    now_millis(),
                )?;
                // A manually resolved operation must not remain dispatchable,
                // even when a crash left its outbox leased or ready.
                transaction.execute(
                    "UPDATE manager_outbox
                        SET status = ?2, lease_id = NULL, lease_expires_at = NULL,
                            next_attempt_at = NULL, error_code = ?3
                      WHERE operation_id = ?1
                        AND status NOT IN (?4, ?5)",
                    params![
                        operation.operation_id,
                        pb::OutboxStatus::DeadLetter as i32,
                        pb::ManagerErrorCode::ResolutionRequired as i32,
                        pb::OutboxStatus::Acknowledged as i32,
                        pb::OutboxStatus::DeadLetter as i32,
                    ],
                )?;
                transaction.execute(
                    "UPDATE manager_idempotency
                        SET state = ?2, result_snapshot = NULL,
                            error_snapshot = ?3, updated_at = ?4
                      WHERE operation_id = ?1",
                    params![
                        operation.operation_id,
                        operation.state,
                        error_snapshot.payload.as_slice(),
                        now_millis(),
                    ],
                )?;
                committed = Some(operation);
                Ok(())
            })
            .map_err(map_repository_error)?;
        Ok(pb::ManagerMutation {
            operation: committed,
            ..Default::default()
        })
    }
}

pub(super) fn operation_history_query_fingerprint(
    filter: &pb::OperationHistoryFilter,
    context: Option<&pb::ManagerReadContext>,
) -> String {
    let context = context.map(Message::encode_to_vec).unwrap_or_default();
    format!(
        "{:x}",
        Sha256::digest(
            [
                b"list-operation-history\0".as_slice(),
                filter.encode_to_vec().as_slice(),
                b"\0",
                context.as_slice(),
            ]
            .concat()
        )
    )
}

pub(super) struct ManagerEntityPage<T> {
    items: Vec<T>,
    info: pb::PageInfo,
}

pub(super) struct ManagerEntityPageRequest<'a, F> {
    request: Option<&'a pb::PageRequest>,
    filter: Option<&'a F>,
    context: Option<&'a pb::ManagerReadContext>,
    order_fingerprint: &'a str,
    method: &'a str,
}

pub(super) fn manager_entity_page<T, F>(
    storage: &ManagerStorage,
    query: ManagerEntityPageRequest<'_, F>,
    values: Vec<T>,
    page_size_limits: PageSizeLimits,
) -> Result<ManagerEntityPage<T>, pb::ManagerError>
where
    F: Message,
{
    let page = query.request.cloned().unwrap_or_default();
    let generation = storage.generation();
    let snapshot_id = format!("manager-{generation}");
    let query_fingerprint =
        manager_entity_query_fingerprint(query.method, query.filter, query.context);
    let codec = PageTokenCodec::new(super::super::canonical_service::list_page_token_secret(
        storage,
    ));
    let cursor = page
        .page_token
        .as_deref()
        .map(|token| {
            decode_manager_entity_page_token(
                &codec,
                token,
                generation,
                &snapshot_id,
                query.order_fingerprint,
                &query_fingerprint,
            )
        })
        .transpose()
        .map_err(manager_entity_page_token_error)?;
    let total_size = values.len() as u64;
    let (start, end, has_more) = paginate(
        &values,
        page_size_limits.resolve(page.page_size),
        cursor.as_ref(),
    );
    let next_page_token = has_more.then(|| {
        codec.encode(&PageCursor {
            offset: end,
            generation,
            snapshot_id: snapshot_id.clone(),
            order_fingerprint: query.order_fingerprint.into(),
            query_fingerprint: query_fingerprint.clone(),
            expires_at_unix_epoch_millis: i64::MAX,
        })
    });
    Ok(ManagerEntityPage {
        items: values.into_iter().skip(start).take(end - start).collect(),
        info: pb::PageInfo {
            next_page_token,
            total_size: Some(total_size),
            read_revision: generation,
            snapshot: Some(pb::PageSnapshot {
                snapshot_id,
                stable_traversal: Some(pb::StableTraversal {
                    order_fingerprint: query.order_fingerprint.into(),
                }),
                read_generation: generation,
                read_revision_watermark: 0,
            }),
        },
    })
}

pub(super) fn manager_entity_query_fingerprint<F: Message>(
    method: &str,
    filter: Option<&F>,
    context: Option<&pb::ManagerReadContext>,
) -> String {
    let filter = filter.map(Message::encode_to_vec).unwrap_or_default();
    let context = context.map(Message::encode_to_vec).unwrap_or_default();
    format!(
        "{:x}",
        Sha256::digest(
            [
                method.as_bytes(),
                b"\0",
                filter.as_slice(),
                b"\0",
                context.as_slice(),
            ]
            .concat()
        )
    )
}

pub(super) fn decode_manager_entity_page_token(
    codec: &PageTokenCodec,
    token: &str,
    expected_generation: u64,
    expected_snapshot_id: &str,
    expected_order_fingerprint: &str,
    expected_query_fingerprint: &str,
) -> Result<PageCursor, crate::error::TrackerError> {
    decode_bound_page_token(
        codec,
        token,
        expected_generation,
        expected_snapshot_id,
        expected_order_fingerprint,
        expected_query_fingerprint,
        now_millis(),
    )
}

pub(super) fn manager_entity_page_token_error(
    error: crate::error::TrackerError,
) -> pb::ManagerError {
    match error {
        crate::error::TrackerError::InvalidPageToken => gateway_error(
            pb::ManagerErrorCode::InvalidPageToken,
            "invalid manager entity page token",
        ),
        crate::error::TrackerError::PageTokenExpired { .. } => gateway_error(
            pb::ManagerErrorCode::PageTokenExpired,
            "manager entity page token has expired",
        ),
        error => domain_error(error.to_string()),
    }
}

pub(super) fn candidate_matches(
    candidate: &pb::Candidate,
    filter: Option<&pb::CandidateFilter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    (filter.statuses.is_empty() || filter.statuses.contains(&candidate.status))
        && super::super::candidates::candidate_matches_common(
            candidate,
            &filter.kinds,
            &filter.priorities,
            filter.extraction_run_id.as_deref(),
            filter.text_query.as_deref(),
            filter.source_evidence.as_ref(),
        )
}

pub(super) fn extraction_run_matches(
    run: &pb::ExtractionRun,
    filter: Option<&pb::ExtractionRunFilter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    (filter.states.is_empty() || filter.states.contains(&run.state))
        && filter
            .source
            .as_ref()
            .is_none_or(|source| run.source == *source)
        && filter
            .request_schema
            .as_ref()
            .is_none_or(|schema| run.request_schema == *schema)
}

pub(super) fn operation_history_page_token_error(
    error: crate::error::TrackerError,
) -> pb::ManagerError {
    match error {
        crate::error::TrackerError::InvalidPageToken => gateway_error(
            pb::ManagerErrorCode::InvalidPageToken,
            "invalid operation history page token",
        ),
        crate::error::TrackerError::PageTokenExpired { .. } => gateway_error(
            pb::ManagerErrorCode::PageTokenExpired,
            "operation history page token has expired",
        ),
        error => domain_error(error.to_string()),
    }
}

impl<'a> ManagerRepository<'a> {
    fn resolve_merge(
        &mut self,
        args: &pb::ResolveCandidateArgs,
        scope: &str,
        request_digest: &str,
        source: pb::Candidate,
        merge: &pb::MergeResolution,
    ) -> Result<pb::ResolutionReceipt, pb::ManagerError> {
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
        let (target_candidate_id, target_issue_id, mut target_candidate) =
            match merge.target.as_ref() {
                Some(pb::merge_resolution::Target::Candidate(target)) => {
                    let expected = target.expected_revision.ok_or_else(|| {
                        domain_error("target candidate expected_revision is required")
                    })?;
                    let stored = self
                        .get(EntityKind::Candidate, &target.candidate_id)
                        .map_err(map_repository_error)?
                        .ok_or_else(|| domain_error("merge target candidate not found"))?;
                    let candidate: pb::Candidate =
                        decode_stored(stored, "issue_tracker.manager.v1.Candidate")?;
                    if candidate.revision != expected
                        || candidate.status != pb::CandidateStatus::Proposed as i32
                    {
                        return Err(domain_error(
                            "merge target candidate is stale or not proposed",
                        ));
                    }
                    (Some(target.candidate_id.clone()), None, Some(candidate))
                }
                Some(pb::merge_resolution::Target::Issue(target)) => {
                    target.expected_revision.ok_or_else(|| {
                        domain_error("target issue expected_revision is required")
                    })?;
                    let stored = self
                        .get(EntityKind::IssueMapping, &target.issue_id)
                        .map_err(map_repository_error)?
                        .ok_or_else(|| domain_error("merge target issue is not mapped"))?;
                    let _mapping: pb::IssueMapping =
                        decode_stored(stored, "issue_tracker.manager.v1.IssueMapping")?;
                    (None, Some(target.issue_id.clone()), None)
                }
                None => return Err(domain_error("merge target is required")),
            };
        let context = args
            .context
            .as_ref()
            .ok_or_else(|| domain_error("manager write context is required"))?;
        let target_source_evidence = match merge.target.as_ref() {
            Some(pb::merge_resolution::Target::Candidate(_)) => target_candidate
                .as_ref()
                .map(|candidate| candidate.source_evidence.as_slice()),
            Some(pb::merge_resolution::Target::Issue(target)) => {
                Some(target.resolved_source_evidence.as_slice())
            }
            None => None,
        };
        validate_resolution_basis(context, merge.basis, &source, target_source_evidence)?;
        // Updating an Issue is a canonical backend write. Admit it as a
        // saga so the source Candidate is finalized
        // only after the Issue CAS update has committed.
        if let Some(issue_id) = target_issue_id.as_deref() {
            let Some(pb::merge_resolution::Target::Issue(target)) = merge.target.as_ref() else {
                return Err(domain_error("merge target is invalid"));
            };
            if target.resolved_provenance_metadata.is_empty() {
                return Err(domain_error(
                    "issue merge provenance was not admitted by the server",
                ));
            }
            let mut backend_context = args.context.clone().unwrap_or_default();
            backend_context.method = "manager_update_issue".into();
            backend_context.target = format!("issue:{issue_id}");
            let update = pb::UpdateIssueArgs {
                context: Some(backend_context),
                issue_id: issue_id.into(),
                expected_revision: target.expected_revision,
                update_mask: vec![
                    pb::ManagerIssueField::ProvenanceMetadata as i32,
                    pb::ManagerIssueField::SourceEvidence as i32,
                ],
                patch: Some(pb::ManagerIssuePatch {
                    provenance_metadata: target.resolved_provenance_metadata.clone(),
                    source_evidence: target.resolved_source_evidence.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            return self.admit_resolution_saga_backend(ResolutionSagaAdmission {
                args,
                context: args
                    .context
                    .as_ref()
                    .ok_or_else(|| domain_error("manager write context is required"))?,
                candidate: &source,
                decision: ResolutionSagaDecision::MergeIssue,
                backend_method: "manager_update_issue",
                backend_args: update.encode_to_vec(),
                issue_id,
            });
        }
        let target_candidate_id = target_candidate_id
            .as_deref()
            .ok_or_else(|| domain_error("merge target candidate is required"))?;
        let superseded = supersede_candidate(
            &source,
            SupersedingTarget::Candidate(target_candidate_id),
            utc_instant(now_millis()),
        )
        .map_err(domain_error)?;
        let target_metadata_changed = if let Some(candidate) = target_candidate.as_mut() {
            let original_metadata = candidate.metadata.clone();
            let mut groups = Vec::new();
            if let Some(snapshots) =
                snapshots_from_manager_metadata(&source.metadata).map_err(domain_error)?
            {
                groups.push(snapshots);
            }
            if let Some(snapshots) =
                snapshots_from_manager_metadata(&candidate.metadata).map_err(domain_error)?
            {
                groups.push(snapshots);
            }
            let snapshots = union_snapshots(groups).map_err(domain_error)?;
            if !snapshots.is_empty() {
                replace_manager_metadata(&mut candidate.metadata, &snapshots)
                    .map_err(domain_error)?;
            }
            let original_source_evidence = candidate.source_evidence.clone();
            candidate.source_evidence = super::super::candidates::union_source_evidence(
                &candidate.source_evidence,
                &source.source_evidence,
                self.storage.source_evidence_limits(),
            )
            .map_err(|error| domain_error(format!("source evidence is invalid: {error:?}")))?;
            candidate.metadata != original_metadata
                || candidate.source_evidence != original_source_evidence
        } else {
            false
        };
        if target_metadata_changed {
            let candidate = target_candidate
                .as_mut()
                .expect("candidate target exists when its metadata changed");
            if candidate.strong_detection_proof.is_some() {
                self.ensure_candidate_auto_registration_is_not_in_flight(&candidate.candidate_id)
                    .map_err(map_repository_error)?;
                candidate.strong_detection_proof = None;
            }
            candidate.revision = candidate
                .revision
                .checked_add(1)
                .ok_or_else(|| domain_error("target candidate revision overflow"))?;
            candidate.updated_at = Some(utc_instant(now_millis()));
        } else {
            // A merge without additional canonical provenance must not cause
            // an unrelated target Candidate write or revision conflict.
            target_candidate = None;
        }
        let mut operation = manager_only_operation(
            "manager_resolve_candidate",
            Some(pb::durable_operation::Scope::CandidateId(
                source.candidate_id.clone(),
            )),
            scope,
            request_digest,
            &super::super::operations::new_operation_id(),
        );
        operation.state = pb::DurableOperationState::Committed as i32;
        operation.causal_parent = args.causal_parent.clone();
        let receipt = pb::ResolutionReceipt {
            operation_id: operation.operation_id.clone(),
            state: operation.state,
            candidate: Some(pb::CandidateResolutionReference {
                candidate_id: source.candidate_id,
                state: pb::ResolutionReferenceState::Existing as i32,
                revision: Some(superseded.revision),
            }),
            issues: target_issue_id
                .iter()
                .map(|issue_id| pb::IssueResolutionReference {
                    issue_id: issue_id.clone(),
                    state: pb::ResolutionReferenceState::Existing as i32,
                    revision: None,
                })
                .collect(),
            ..Default::default()
        };
        let (typed, result_snapshot) = resolve_candidate_success_result(&receipt);
        operation.terminal_snapshot = Some(
            pb::durable_operation::TerminalSnapshot::ResultSnapshot(result_snapshot),
        );
        let mutation = pb::ManagerMutation {
            entity: Some(pb::manager_mutation::Entity::Candidate(superseded.clone())),
            operation: Some(operation.clone()),
            ..Default::default()
        };
        self.commit_supersede_batch(
            &mutation,
            &operation,
            &superseded,
            target_candidate.as_ref(),
            &typed,
            ResolutionHistoryDecision {
                kind: pb::ResolutionDecisionKind::Merge,
                candidate_id: &superseded.candidate_id,
                candidate_revision: superseded
                    .revision
                    .checked_sub(1)
                    .ok_or_else(|| domain_error("candidate revision overflow"))?,
            },
        )
        .map_err(map_repository_error)?;
        Ok(receipt)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OperationHistoryProjection {
    operation_id: String,
    method: String,
    state: i32,
    direct_target_kind: Option<String>,
    direct_target_id: Option<String>,
    created_at: i64,
    updated_at: i64,
    resolution_required: bool,
    affected_issue_ids: BTreeSet<String>,
}

/// SQL predicate and bind values shared by operation-history page and count
/// queries. The query text contains only fixed SQL fragments and generated
/// placeholder counts; all caller-provided values remain bound parameters.
#[derive(Clone, Debug, Default)]
struct OperationHistoryQuery {
    conditions: Vec<String>,
    values: Vec<Value>,
}

impl OperationHistoryQuery {
    fn new(filter: &pb::OperationHistoryFilter) -> Self {
        let mut query = Self::default();
        query.append_filter_conditions(filter);
        query
    }

    fn append_filter_conditions(&mut self, filter: &pb::OperationHistoryFilter) {
        let conditions = &mut self.conditions;
        let values = &mut self.values;
        match filter.direct_target.as_ref() {
            Some(pb::operation_history_filter::DirectTarget::IssueId(issue_id)) => {
                if filter.direct_scope_only {
                    conditions.push(
                        "projection.direct_target_kind = ? AND projection.direct_target_id = ?"
                            .into(),
                    );
                    values.extend([Value::Text("issue".into()), Value::Text(issue_id.clone())]);
                } else {
                    conditions.push(
                        "(projection.direct_target_kind = ? AND projection.direct_target_id = ?
                      OR EXISTS (SELECT 1 FROM manager_operation_history_issue AS issue
                                   WHERE issue.operation_id = projection.operation_id
                                     AND issue.issue_id = ?))"
                            .into(),
                    );
                    values.extend([
                        Value::Text("issue".into()),
                        Value::Text(issue_id.clone()),
                        Value::Text(issue_id.clone()),
                    ]);
                }
            }
            Some(target) => {
                let (kind, target_id) = match target {
                    pb::operation_history_filter::DirectTarget::CandidateId(value) => {
                        ("candidate", value)
                    }
                    pb::operation_history_filter::DirectTarget::EvidenceId(value) => {
                        ("evidence", value)
                    }
                    pb::operation_history_filter::DirectTarget::ExtractionRunId(value) => {
                        ("extraction_run", value)
                    }
                    pb::operation_history_filter::DirectTarget::IssueId(_) => unreachable!(),
                };
                conditions.push(
                    "projection.direct_target_kind = ? AND projection.direct_target_id = ?".into(),
                );
                values.extend([Value::Text(kind.into()), Value::Text(target_id.clone())]);
            }
            None => {}
        }
        if let Some(from) = filter.created_at_from.as_ref() {
            conditions.push("projection.created_at >= ?".into());
            values.push(Value::Integer(from.unix_epoch_millis));
        }
        if let Some(to) = filter.created_at_to.as_ref() {
            conditions.push("projection.created_at <= ?".into());
            values.push(Value::Integer(to.unix_epoch_millis));
        }
        if let Some(method) = filter.method.as_deref() {
            conditions.push("projection.method = ?".into());
            values.push(Value::Text(method.to_owned()));
        }
        if !filter.states.is_empty() {
            let placeholders = std::iter::repeat_n("?", filter.states.len())
                .collect::<Vec<_>>()
                .join(", ");
            conditions.push(format!("projection.state IN ({placeholders})"));
            values.extend(
                filter
                    .states
                    .iter()
                    .map(|state| Value::Integer(i64::from(*state))),
            );
        }
        if !filter.decision_kinds.is_empty() {
            let placeholders = std::iter::repeat_n("?", filter.decision_kinds.len())
                .collect::<Vec<_>>()
                .join(", ");
            conditions.push(format!(
                "EXISTS (SELECT 1 FROM manager_operation_history_decision AS decision
                      WHERE decision.operation_id = projection.operation_id
                        AND decision.decision_kind IN ({placeholders}))"
            ));
            values.extend(
                filter
                    .decision_kinds
                    .iter()
                    .map(|kind| Value::Integer(i64::from(*kind))),
            );
        }
    }

    fn page_statement(
        &self,
        page_size: usize,
        keyset: Option<&OperationHistoryPageCursor>,
        offset: Option<usize>,
    ) -> (String, Vec<Value>) {
        let mut statement = String::from(
            "SELECT projection.operation_id
               FROM manager_operation_history_projection AS projection",
        );
        let mut conditions = self.conditions.clone();
        let mut values = self.values.clone();
        if let Some(cursor) = keyset {
            conditions.push(
                "(projection.created_at < ? OR
                  (projection.created_at = ? AND projection.operation_id < ?))"
                    .into(),
            );
            values.extend([
                Value::Integer(cursor.created_at_unix_epoch_millis),
                Value::Integer(cursor.created_at_unix_epoch_millis),
                Value::Text(cursor.operation_id.clone()),
            ]);
        }
        if !conditions.is_empty() {
            statement.push_str(" WHERE ");
            statement.push_str(&conditions.join(" AND "));
        }
        statement.push_str(" ORDER BY projection.created_at DESC, projection.operation_id DESC");
        statement.push_str(" LIMIT ?");
        values.push(Value::Integer(
            i64::try_from(page_size.max(1).saturating_add(1)).unwrap_or(i64::MAX),
        ));
        if let Some(offset) = offset {
            statement.push_str(" OFFSET ?");
            values.push(Value::Integer(i64::try_from(offset).unwrap_or(i64::MAX)));
        }
        (statement, values)
    }

    fn count_statement(&self) -> (String, Vec<Value>) {
        let mut statement =
            String::from("SELECT COUNT(*) FROM manager_operation_history_projection AS projection");
        if !self.conditions.is_empty() {
            statement.push_str(" WHERE ");
            statement.push_str(&self.conditions.join(" AND "));
        }
        (statement, self.values.clone())
    }
}

fn operation_history_operation_ids_page(
    storage: &ManagerStorage,
    filter: &pb::OperationHistoryFilter,
    page_size: usize,
    keyset: Option<&OperationHistoryPageCursor>,
    offset: Option<usize>,
    include_total_size: bool,
) -> Result<(Vec<String>, bool, Option<u64>), RepositoryError> {
    let query = OperationHistoryQuery::new(filter);
    let (statement, values) = query.page_statement(page_size, keyset, offset);
    let ids = storage
        .read(|connection| {
            let mut query = connection.prepare(&statement)?;
            query
                .query_map(params_from_iter(values), |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(ManagerStorageError::from)
        })
        .map_err(RepositoryError::from)?;
    let page_size = page_size.max(1);
    let has_more = ids.len() > page_size;
    let ids = ids.into_iter().take(page_size).collect::<Vec<_>>();
    let total_size = if include_total_size {
        let (count_statement, count_values) = query.count_statement();
        let count = storage
            .read(|connection| {
                connection
                    .query_row(&count_statement, params_from_iter(count_values), |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(ManagerStorageError::from)
            })
            .map_err(RepositoryError::from)?;
        Some(u64::try_from(count).map_err(|_| RepositoryError::RevisionOverflow)?)
    } else {
        None
    };
    Ok((ids, has_more, total_size))
}

/// Hydrate only the IDs selected by the bounded history query. Projection,
/// durable payload, and Issue relations are loaded in bulk and then restored
/// to the SQL ordering before integrity validation.
struct OperationHistoryHydratedPage {
    operations: Vec<pb::DurableOperation>,
    resolutions: Vec<OperationHistoryResolution>,
    decisions: Vec<OperationHistoryDecision>,
}

// Keep each history hydration statement below SQLite's variable limit. The
// bundled SQLite build permits more variables, but this conservative bound
// also works with distributions compiled with the older 999-variable limit.
const OPERATION_HISTORY_BIND_CHUNK_SIZE: usize = 500;

fn operation_history_id_chunks<T>(operation_ids: &[T]) -> impl Iterator<Item = &[T]> {
    operation_ids.chunks(OPERATION_HISTORY_BIND_CHUNK_SIZE)
}

fn load_operation_history_page(
    storage: &ManagerStorage,
    operation_ids: &[String],
) -> Result<OperationHistoryHydratedPage, RepositoryError> {
    if operation_ids.is_empty() {
        return Ok(OperationHistoryHydratedPage {
            operations: Vec::new(),
            resolutions: Vec::new(),
            decisions: Vec::new(),
        });
    }
    let rows = storage.read(|connection| {
        let mut rows = Vec::with_capacity(operation_ids.len());
        for operation_id_chunk in operation_history_id_chunks(operation_ids) {
            let placeholders = history_placeholders(operation_id_chunk.len());
            let mut statement = connection.prepare(&format!(
                "SELECT projection.operation_id, projection.method, projection.state,
                        projection.direct_target_kind, projection.direct_target_id,
                        projection.created_at, projection.updated_at,
                        projection.resolution_required,
                        operation.operation_payload, operation.operation_payload_schema,
                        operation.operation_payload_digest
                   FROM manager_operation_history_projection AS projection
                   LEFT JOIN manager_operation AS operation
                     ON operation.operation_id = projection.operation_id
                  WHERE projection.operation_id IN ({placeholders})"
            ))?;
            rows.extend(
                statement
                    .query_map(params_from_iter(operation_id_chunk.iter()), |row| {
                        Ok((
                            OperationHistoryProjection {
                                operation_id: row.get(0)?,
                                method: row.get(1)?,
                                state: row.get(2)?,
                                direct_target_kind: row.get(3)?,
                                direct_target_id: row.get(4)?,
                                created_at: row.get(5)?,
                                updated_at: row.get(6)?,
                                resolution_required: row.get::<_, i64>(7)? != 0,
                                affected_issue_ids: BTreeSet::new(),
                            },
                            row.get::<_, Option<Vec<u8>>>(8)?,
                            row.get::<_, Option<String>>(9)?,
                            row.get::<_, Option<String>>(10)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(rows)
    })?;
    let mut rows_by_id = HashMap::with_capacity(rows.len());
    for (projection, payload, schema, digest) in rows {
        if rows_by_id
            .insert(
                projection.operation_id.clone(),
                (projection, payload, schema, digest),
            )
            .is_some()
        {
            return Err(RepositoryError::RequestSnapshot(
                "operation history read returned duplicate projection rows".into(),
            ));
        }
    }

    let relations = storage.read(|connection| {
        let mut relations = HashMap::<String, BTreeSet<String>>::new();
        for operation_id_chunk in operation_history_id_chunks(operation_ids) {
            let placeholders = history_placeholders(operation_id_chunk.len());
            let mut statement = connection.prepare(&format!(
                "SELECT operation_id, issue_id
                   FROM manager_operation_history_issue
                  WHERE operation_id IN ({placeholders})
                  ORDER BY operation_id, issue_id"
            ))?;
            for row in statement.query_map(params_from_iter(operation_id_chunk.iter()), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })? {
                let (operation_id, issue_id) = row?;
                relations.entry(operation_id).or_default().insert(issue_id);
            }
        }
        Ok(relations)
    })?;

    let operations = operation_ids
        .iter()
        .map(|operation_id| {
            let (mut projection, payload, schema, digest) =
                rows_by_id.remove(operation_id).ok_or_else(|| {
                    RepositoryError::RequestSnapshot(
                        "operation history projection is missing".into(),
                    )
                })?;
            projection.affected_issue_ids =
                relations.get(operation_id).cloned().unwrap_or_default();
            let (Some(payload), Some(schema), Some(digest)) = (payload, schema, digest) else {
                return Err(RepositoryError::RequestSnapshot(
                    "operation history read failed".into(),
                ));
            };
            if schema != "issue_tracker.manager.v1.DurableOperation"
                || digest_bytes(&payload) != digest
            {
                return Err(RepositoryError::RequestSnapshot(
                    "operation history read failed".into(),
                ));
            }
            let operation = pb::DurableOperation::decode(payload.as_slice()).map_err(|_| {
                RepositoryError::RequestSnapshot("operation history read failed".into())
            })?;
            validate_operation_history_projection(&operation, &projection)?;
            Ok(operation)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let resolutions = operation_history_resolutions(storage, operation_ids)?;
    let decisions = operation_history_decisions(storage, operation_ids)?;
    Ok(OperationHistoryHydratedPage {
        operations,
        resolutions,
        decisions,
    })
}

fn history_placeholders(count: usize) -> String {
    (1..=count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_operation_history_projection(
    operation: &pb::DurableOperation,
    projection: &OperationHistoryProjection,
) -> Result<(), RepositoryError> {
    let created_at = operation
        .created_at
        .as_ref()
        .map(|instant| instant.unix_epoch_millis);
    let updated_at = operation
        .updated_at
        .as_ref()
        .map(|instant| instant.unix_epoch_millis);
    let (direct_target_kind, direct_target_id) = match operation.scope.as_ref() {
        Some(pb::durable_operation::Scope::CandidateId(id)) => (Some("candidate"), Some(id)),
        Some(pb::durable_operation::Scope::EvidenceId(id)) => (Some("evidence"), Some(id)),
        Some(pb::durable_operation::Scope::ExtractionRunId(id)) => {
            (Some("extraction_run"), Some(id))
        }
        Some(pb::durable_operation::Scope::IssueId(id)) => (Some("issue"), Some(id)),
        None => (None, None),
    };
    let affected = operation
        .affected_issue_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if operation
        .affected_issue_ids
        .iter()
        .any(|issue_id| issue_id.trim().is_empty())
        || affected.len() != operation.affected_issue_ids.len()
    {
        return Err(RepositoryError::RequestSnapshot(
            "operation history projection integrity validation failed".into(),
        ));
    }
    if operation.operation_id != projection.operation_id
        || operation.method != projection.method
        || operation.state != projection.state
        || created_at != Some(projection.created_at)
        || updated_at != Some(projection.updated_at)
        || operation.resolution_required != projection.resolution_required
        || projection.direct_target_kind.as_deref() != direct_target_kind
        || projection.direct_target_id.as_deref() != direct_target_id.map(String::as_str)
        || affected != projection.affected_issue_ids
    {
        return Err(RepositoryError::RequestSnapshot(
            "operation history projection integrity validation failed".into(),
        ));
    }
    Ok(())
}

pub(super) fn operation_history_resolutions(
    storage: &ManagerStorage,
    operation_ids: &[String],
) -> Result<Vec<OperationHistoryResolution>, RepositoryError> {
    if operation_ids.is_empty() {
        return Ok(Vec::new());
    }
    let resolutions = storage.read(|connection| {
        let mut resolutions = Vec::new();
        for operation_id_chunk in operation_history_id_chunks(operation_ids) {
            let placeholders = history_placeholders(operation_id_chunk.len());
            let mut statement = connection.prepare(&format!(
                "SELECT operation_id, resolution_kind, reason_category, rationale
                   FROM manager_operation_history_resolution
                  WHERE operation_id IN ({placeholders})
                  ORDER BY operation_id"
            ))?;
            resolutions.extend(
                statement
                    .query_map(params_from_iter(operation_id_chunk.iter()), |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i32>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(resolutions)
    })?;
    resolutions
        .into_iter()
        .map(
            |(operation_id, resolution_kind, reason_category, rationale)| {
                if !matches!(
                    resolution_kind.as_str(),
                    "reject" | "cancel_associated_issue"
                ) {
                    return Err(RepositoryError::RequestSnapshot(format!(
                        "operation history resolution {operation_id} has an invalid kind"
                    )));
                }
                Ok(OperationHistoryResolution::RejectOrCancel {
                    operation_id,
                    reason_category: resolution_reason_category(reason_category)?,
                    rationale,
                })
            },
        )
        .collect()
}

pub(super) fn operation_history_decisions(
    storage: &ManagerStorage,
    operation_ids: &[String],
) -> Result<Vec<OperationHistoryDecision>, RepositoryError> {
    if operation_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = storage.read(|connection| {
        let mut rows = Vec::new();
        for operation_id_chunk in operation_history_id_chunks(operation_ids) {
            let placeholders = history_placeholders(operation_id_chunk.len());
            let mut statement = connection.prepare(&format!(
                "SELECT operation_id, decision_kind, candidate_id, candidate_revision
                   FROM manager_operation_history_decision
                  WHERE operation_id IN ({placeholders})
                  ORDER BY operation_id"
            ))?;
            rows.extend(
                statement
                    .query_map(params_from_iter(operation_id_chunk.iter()), |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i32>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(rows)
    })?;
    rows.into_iter()
        .map(
            |(operation_id, decision_kind, candidate_id, candidate_revision)| {
                let kind = pb::ResolutionDecisionKind::try_from(decision_kind).map_err(|_| {
                    RepositoryError::RequestSnapshot(format!(
                        "operation history decision {operation_id} has an invalid kind"
                    ))
                })?;
                if kind == pb::ResolutionDecisionKind::Unspecified || candidate_id.is_empty() {
                    return Err(RepositoryError::RequestSnapshot(format!(
                        "operation history decision {operation_id} is invalid"
                    )));
                }
                Ok(OperationHistoryDecision {
                    operation_id,
                    kind,
                    candidate_id,
                    candidate_revision: u64::try_from(candidate_revision)
                        .map_err(|_| RepositoryError::RevisionOverflow)?,
                })
            },
        )
        .collect()
}

#[cfg(test)]
mod operation_history_query_tests {
    use crate::manager::{ManagerConfig, ManagerStorage};

    use super::*;

    #[test]
    fn page_and_count_queries_share_filter_predicates_and_bind_values() {
        let filter = pb::OperationHistoryFilter {
            direct_target: Some(pb::operation_history_filter::DirectTarget::IssueId(
                "issue-1".into(),
            )),
            method: Some("manager_resolve_candidate".into()),
            states: vec![
                pb::DurableOperationState::Received as i32,
                pb::DurableOperationState::Committed as i32,
            ],
            created_at_from: Some(pb::UtcInstant {
                unix_epoch_millis: 10,
            }),
            created_at_to: Some(pb::UtcInstant {
                unix_epoch_millis: 20,
            }),
            decision_kinds: vec![pb::ResolutionDecisionKind::Merge as i32],
            ..Default::default()
        };
        let query = OperationHistoryQuery::new(&filter);
        let (page_sql, page_values) = query.page_statement(
            5,
            Some(&OperationHistoryPageCursor {
                created_at_unix_epoch_millis: 15,
                operation_id: "op-1".into(),
                generation: 1,
                snapshot_id: "manager-1".into(),
                order_fingerprint: "order".into(),
                query_fingerprint: "query".into(),
                version: 2,
                expires_at_unix_epoch_millis: i64::MAX,
            }),
            None,
        );
        let (count_sql, count_values) = query.count_statement();
        assert!(page_sql.contains("projection.created_at < ?"));
        assert!(page_sql.contains("projection.state IN (?, ?)"));
        assert!(page_sql.contains("manager_operation_history_decision"));
        assert!(!page_sql.contains("OFFSET"));
        assert!(count_sql.contains("projection.created_at >= ?"));
        assert!(!count_sql.contains("projection.created_at < ?"));
        assert_eq!(&page_values[..count_values.len()], &count_values[..]);
        assert_eq!(page_values.len(), count_values.len() + 3 + 1);
    }

    #[test]
    fn direct_scope_only_uses_only_direct_target_predicate() {
        let filter = pb::OperationHistoryFilter {
            direct_target: Some(pb::operation_history_filter::DirectTarget::IssueId(
                "issue-1".into(),
            )),
            direct_scope_only: true,
            ..Default::default()
        };
        let query = OperationHistoryQuery::new(&filter);
        let (sql, values) = query.page_statement(1, None, Some(2));
        assert!(!sql.contains("manager_operation_history_issue"));
        assert!(sql.contains("projection.direct_target_kind = ?"));
        assert_eq!(values.len(), 4); // kind, id, limit, offset
    }

    #[test]
    fn large_history_id_sets_are_split_below_sqlite_bind_limit() {
        let operation_ids = (0..(OPERATION_HISTORY_BIND_CHUNK_SIZE * 2 + 1)).collect::<Vec<_>>();
        let chunks = operation_history_id_chunks(&operation_ids).collect::<Vec<_>>();

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), OPERATION_HISTORY_BIND_CHUNK_SIZE);
        assert_eq!(chunks[1].len(), OPERATION_HISTORY_BIND_CHUNK_SIZE);
        assert_eq!(chunks[2].len(), 1);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.len() <= OPERATION_HISTORY_BIND_CHUNK_SIZE)
        );
    }

    #[test]
    fn history_hydration_handles_more_ids_than_sqlite_variable_limit() {
        let config = ManagerConfig::from_proto(&crate::manager::config::tests::settings()).unwrap();
        let storage = ManagerStorage::open(&config).unwrap();
        let operation_ids = (0..=32_766)
            .map(|index| format!("missing-operation-{index}"))
            .collect::<Vec<_>>();

        assert!(
            operation_history_resolutions(&storage, &operation_ids)
                .unwrap()
                .is_empty()
        );
        assert!(
            operation_history_decisions(&storage, &operation_ids)
                .unwrap()
                .is_empty()
        );
        let error = load_operation_history_page(&storage, &operation_ids)
            .err()
            .expect("missing projections should be reported after chunked queries");
        assert!(matches!(
            error,
            RepositoryError::RequestSnapshot(message)
                if message == "operation history projection is missing"
        ));
    }
}
