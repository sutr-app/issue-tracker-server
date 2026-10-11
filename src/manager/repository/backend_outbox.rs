use super::durable::operation_diagnosis_class;
use super::durable_helpers::*;
use super::*;

impl<'a> ManagerRepository<'a> {
    #[cfg(test)]
    pub(crate) fn commit_backend_admission(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        request: &[u8],
        request_schema: &str,
        expectation: &super::super::backend_adapter::MarkerExpectation,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.commit_backend_admission_with_extraction_reservation(BackendAdmission {
            mutation,
            operation,
            outbox,
            request,
            request_schema,
            expectation,
            extraction_reservation: None,
        })
    }

    pub(super) fn commit_backend_admission_with_extraction_reservation(
        &mut self,
        admission: BackendAdmission<'_>,
    ) -> Result<ManagerCommit, RepositoryError> {
        let BackendAdmission {
            mutation,
            operation,
            outbox,
            request,
            request_schema,
            expectation,
            extraction_reservation,
        } = admission;
        if request.is_empty() || request_schema != "issue_tracker.manager.v1.DurableBackendRequest"
        {
            return Err(RepositoryError::RequestSnapshot(
                "durable request and its schema are required".into(),
            ));
        }
        DurableBackendRequest::decode(request).map_err(|error| {
            RepositoryError::RequestSnapshot(format!("durable request decode: {error}"))
        })?;
        validate_durable_request_snapshot(&durable_backend_method(request)?, request)
            .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        if digest_bytes(request) != outbox.payload_digest
            || operation.operation_id != outbox.operation_id
        {
            return Err(RepositoryError::RequestSnapshot(
                "request digest or operation ID mismatch".into(),
            ));
        }
        let sanitized = sanitize_mutation(mutation);
        let snapshot = sanitized.encode_to_vec();
        let snapshot_digest = digest_bytes(&snapshot);
        let request = request.to_vec();
        let request_schema = request_schema.to_owned();
        let operation = operation.clone();
        let outbox = outbox.clone();
        let expectation = expectation_snapshot(expectation).encode_to_vec();
        let (_, commit) = self.storage.write_with::<_, RepositoryError, _>(|transaction, stamp| {
                insert_operation(transaction, &operation, &snapshot, &snapshot_digest, stamp)?;
                transaction.execute(
                    "UPDATE manager_operation SET request_snapshot = ?2, request_snapshot_schema = ?3, request_snapshot_digest = ?4, marker_expectation = ?5, marker_expectation_schema = ?6 WHERE operation_id = ?1",
                    params![operation.operation_id, request, request_schema, digest_bytes(&request), expectation, "issue_tracker.v1.OperationRecord"],
                )?;
                insert_backend_request_index(
                    transaction,
                    &operation,
                    &durable_backend_method(&request)?,
                    &request_schema,
                    &request,
                )?;
                insert_idempotency(transaction, &operation)?;
                if let Some(identity) = extraction_reservation {
                    consume_extraction_auto_registration_reservation_in_transaction(
                        transaction,
                        &operation.operation_id,
                        identity.issue_id,
                        identity.idempotency_key,
                    )?;
                }
                insert_outbox(transaction, &outbox, stamp)?;
                Ok(())
            })?;
        Ok(commit)
    }

    /// Persists a candidate-resolution backend write and its manager-side
    /// completion plan in one transaction.  A crash can therefore never leave
    /// a dispatchable canonical write without the facts required to finish the
    /// Candidate and mapping decision.
    pub(super) fn commit_resolution_saga_admission(
        &mut self,
        mutation: &pb::ManagerMutation,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        request: &[u8],
        expectation: &super::super::backend_adapter::MarkerExpectation,
        saga: ResolutionSaga,
    ) -> Result<ManagerCommit, RepositoryError> {
        if request.is_empty() || operation.operation_id != outbox.operation_id {
            return Err(RepositoryError::RequestSnapshot(
                "resolution saga admission is invalid".into(),
            ));
        }
        validate_durable_request_snapshot(&durable_backend_method(request)?, request)
            .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        if digest_bytes(request) != outbox.payload_digest {
            return Err(RepositoryError::RequestSnapshot(
                "resolution saga request digest does not match outbox".into(),
            ));
        }
        let snapshot = sanitize_mutation(mutation).encode_to_vec();
        let snapshot_digest = digest_bytes(&snapshot);
        let request = request.to_vec();
        let expectation = expectation_snapshot(expectation).encode_to_vec();
        let operation = operation.clone();
        let outbox = outbox.clone();
        let args_snapshot = saga.args.encode_to_vec();
        let args_digest = digest_bytes(&args_snapshot);
        let history_audit = resolution_history_audit(&saga)?;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                insert_operation(transaction, &operation, &snapshot, &snapshot_digest, stamp)?;
                transaction.execute(
                    "UPDATE manager_operation
                    SET request_snapshot = ?2, request_snapshot_schema = ?3,
                        request_snapshot_digest = ?4, marker_expectation = ?5,
                        marker_expectation_schema = ?6
                  WHERE operation_id = ?1",
                    params![
                        operation.operation_id,
                        request,
                        "issue_tracker.manager.v1.DurableBackendRequest",
                        digest_bytes(&request),
                        expectation,
                        "issue_tracker.v1.OperationRecord",
                    ],
                )?;
                insert_idempotency(transaction, &operation)?;
                insert_outbox(transaction, &outbox, stamp)?;
                transaction.execute(
                    "INSERT INTO manager_resolution_saga
                    (operation_id, candidate_id, candidate_revision, decision, issue_id,
                     args_snapshot, args_digest, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        operation.operation_id,
                        saga.candidate_id,
                        i64::try_from(saga.candidate_revision)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        saga.decision as i32,
                        saga.issue_id,
                        args_snapshot,
                        args_digest,
                        now_millis(),
                    ],
                )?;
                if let Some(history_audit) = &history_audit {
                    insert_operation_history_resolution(
                        transaction,
                        &operation.operation_id,
                        history_audit,
                    )?;
                }
                insert_operation_history_decision(
                    transaction,
                    &operation.operation_id,
                    resolution_history_decision(&saga)?,
                )?;
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    pub(super) fn resolution_saga(
        &self,
        operation_id: &str,
    ) -> Result<Option<ResolutionSaga>, RepositoryError> {
        self.storage.read(|connection| {
            connection.query_row(
                "SELECT candidate_id, candidate_revision, decision, issue_id, args_snapshot, args_digest
                   FROM manager_resolution_saga WHERE operation_id = ?1",
                params![operation_id],
                |row| Ok((
                    row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i32>(2)?,
                    row.get::<_, String>(3)?, row.get::<_, Vec<u8>>(4)?, row.get::<_, String>(5)?,
                )),
            ).optional().map_err(ManagerStorageError::from)
        })?.map_or(Ok(None), |(candidate_id, revision, decision, issue_id, args, digest)| {
            if digest_bytes(&args) != digest {
                return Err(RepositoryError::RequestSnapshot("resolution saga arguments are corrupt".into()));
            }
            Ok(Some(ResolutionSaga {
                candidate_id,
                candidate_revision: u64::try_from(revision).map_err(|_| RepositoryError::RevisionOverflow)?,
                decision: ResolutionSagaDecision::from_i32(decision)?,
                issue_id,
                args: pb::ResolveCandidateArgs::decode(args.as_slice())
                    .map_err(|error| RepositoryError::Serialization(error.to_string()))?,
            }))
        })
    }

    pub(crate) fn backend_request(
        &self,
        operation_id: &str,
    ) -> Result<Option<Vec<u8>>, RepositoryError> {
        let record = self.storage
                .read(|connection| {
                        connection
                        .query_row(
                            "SELECT request_snapshot, request_snapshot_schema, request_snapshot_digest FROM manager_operation
                              WHERE operation_id = ?1",
                            params![operation_id],
                            |row| Ok((row.get::<_, Option<Vec<u8>>>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, Option<String>>(2)?)),
                        )
                        .optional()
                        .map_err(ManagerStorageError::from)
                })?;
        match record {
            None | Some((None, _, _)) => Ok(None),
            Some((Some(payload), Some(schema), Some(digest)))
                if schema == "issue_tracker.manager.v1.DurableBackendRequest"
                    && digest_bytes(&payload) == digest =>
            {
                Ok(Some(payload))
            }
            Some(_) => Err(RepositoryError::RequestSnapshot(
                "backend request integrity validation failed".into(),
            )),
        }
    }

    /// Loads the original administrative request only when its dedicated
    /// snapshot schema and digest still match.  Manual resolution must use
    /// this admitted intent, not rebuild a request from the current Issue.
    pub(crate) fn admin_action_request(
        &self,
        operation_id: &str,
    ) -> Result<Option<pb::ApplyIssueAdminActionArgs>, RepositoryError> {
        let record = self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT request_snapshot, request_snapshot_schema, request_snapshot_digest
                       FROM manager_operation WHERE operation_id=?1",
                    params![operation_id],
                    |row| {
                        Ok((
                            row.get::<_, Option<Vec<u8>>>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(ManagerStorageError::from)
        })?;
        match record {
            None | Some((None, _, _)) => Ok(None),
            Some((Some(payload), Some(schema), Some(digest)))
                if digest_bytes(&payload) == digest
                    && schema == "issue_tracker.manager.v1.ApplyIssueAdminActionArgs" =>
            {
                pb::ApplyIssueAdminActionArgs::decode(payload.as_slice())
                    .map(Some)
                    .map_err(|error| RepositoryError::Serialization(error.to_string()))
            }
            Some((Some(payload), Some(schema), Some(digest)))
                if digest_bytes(&payload) == digest
                    && schema == "issue_tracker.manager.v1.DurableBackendRequest" =>
            {
                let envelope = DurableBackendRequest::decode(payload.as_slice())
                    .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                let args = envelope
                    .admin_action
                    .and_then(|request| request.args)
                    .ok_or_else(|| {
                        RepositoryError::RequestSnapshot(
                            "administrative durable request is missing arguments".into(),
                        )
                    })?;
                Ok(Some(args))
            }
            Some(_) => Err(RepositoryError::RequestSnapshot(
                "administrative request integrity validation failed".into(),
            )),
        }
    }

    pub(crate) fn has_legacy_backend_request_snapshot(
        &self,
        operation_id: &str,
    ) -> Result<bool, RepositoryError> {
        Ok(self.storage.read(|connection| {
                connection
                    .query_row(
                        "SELECT request_snapshot IS NOT NULL
                       FROM manager_operation
                      WHERE operation_id = ?1
                        AND (request_snapshot_schema IS NULL
                             OR request_snapshot_schema != 'issue_tracker.manager.v1.DurableBackendRequest')",
                        params![operation_id],
                        |row| row.get::<_, bool>(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })?
            .unwrap_or(false))
    }

    pub(crate) fn backend_marker_expectation(
        &self,
        operation_id: &str,
    ) -> Result<Option<super::super::backend_adapter::MarkerExpectation>, RepositoryError> {
        let payload =
                self.storage
                    .read(|connection| {
                        connection.query_row(
                "SELECT marker_expectation, marker_expectation_schema FROM manager_operation WHERE operation_id = ?1",
                params![operation_id], |row| Ok((row.get::<_, Option<Vec<u8>>>(0)?, row.get::<_, Option<String>>(1)?)),
            ).optional().map_err(ManagerStorageError::from)
                    })?;
        let Some((Some(bytes), Some(schema))) = payload else {
            return Ok(None);
        };
        if schema != "issue_tracker.v1.OperationRecord" {
            return Err(RepositoryError::RequestSnapshot(
                "marker expectation schema is invalid".into(),
            ));
        }
        let snapshot = v1::OperationRecord::decode(bytes.as_slice())
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        expectation_from_snapshot(&snapshot)
            .map(Some)
            .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))
    }

    pub(crate) fn has_acceptance_guard(&self, operation_id: &str) -> Result<bool, RepositoryError> {
        Ok(self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT 1 FROM manager_acceptance_guard WHERE operation_id=?1",
                    params![operation_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map(|value| value.is_some())
                .map_err(ManagerStorageError::from)
        })?)
    }

    /// Revalidate the manager-side proof immediately before an acceptance is
    /// sent to TaskChampion.  Admission is intentionally not enough: the
    /// candidate, evidence, or candidate bindings may have changed while the
    /// outbox entry was waiting for a worker.  A failed check is resolved as a
    /// terminal manual-resolution outcome, so the backend is never called
    /// with stale proof and the candidate remains PROPOSED.
    pub(crate) fn revalidate_acceptance_before_dispatch(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
    ) -> Result<Option<pb::DurableOperation>, RepositoryError> {
        if !self.has_acceptance_guard(&operation.operation_id)? {
            return Ok(None);
        }
        let reason = match self.validate_acceptance_dispatch(operation) {
            Ok(()) => return Ok(None),
            Err(error) => format!("acceptance pre-dispatch revalidation failed: {error}"),
        };
        self.resolve_acceptance_manual_resolution(operation, outbox, reason)?;
        let resolved = self
            .get_operation(&operation.operation_id)?
            .ok_or(RepositoryError::OperationConflict)?;
        Ok(Some(resolved))
    }

    pub(super) fn validate_acceptance_dispatch(
        &self,
        operation: &pb::DurableOperation,
    ) -> Result<(), RepositoryError> {
        let (candidate_id, candidate_revision): (String, i64) =
            self.storage.read(|connection| {
                connection
                    .query_row(
                        "SELECT candidate_id, candidate_revision
                           FROM manager_acceptance_guard
                          WHERE operation_id=?1",
                        params![operation.operation_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(ManagerStorageError::from)
            })?;
        let stored = self
            .get(EntityKind::Candidate, &candidate_id)?
            .ok_or_else(|| {
                RepositoryError::RequestSnapshot("acceptance candidate is missing".into())
            })?;
        let candidate: pb::Candidate = decode_stored(stored, "issue_tracker.manager.v1.Candidate")
            .map_err(|error| RepositoryError::RequestSnapshot(error.message))?;
        let expected_revision = u64::try_from(candidate_revision).map_err(|_| {
            RepositoryError::RequestSnapshot("candidate revision is invalid".into())
        })?;
        if candidate.revision != expected_revision
            || candidate.status != pb::CandidateStatus::Proposed as i32
        {
            return Err(RepositoryError::RequestSnapshot(
                "candidate revision or status is stale".into(),
            ));
        }
        let saga = self
            .resolution_saga(&operation.operation_id)?
            .ok_or_else(|| {
                RepositoryError::RequestSnapshot("acceptance resolution saga is missing".into())
            })?;
        let Some(pb::resolve_candidate_args::Decision::CreateIssue(create)) = saga.args.decision
        else {
            return Err(RepositoryError::RequestSnapshot(
                "acceptance resolution decision is invalid".into(),
            ));
        };
        let context = saga.args.context.as_ref().ok_or_else(|| {
            RepositoryError::RequestSnapshot("acceptance write context is missing".into())
        })?;
        let issue = create.issue.as_ref().ok_or_else(|| {
            RepositoryError::RequestSnapshot("acceptance issue draft is missing".into())
        })?;
        validate_resolution_basis(
            context,
            create.basis,
            &candidate,
            Some(&issue.source_evidence),
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.message))?;
        Ok(())
    }

    /// Atomically stores the durable result of one leased backend delivery.
    pub(crate) fn finalize_backend_dispatch(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        mappings: &[pb::IssueMapping],
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.state == pb::DurableOperationState::Committed as i32
            && self.has_acceptance_guard(&operation.operation_id)?
        {
            return self.finalize_acceptance_dispatch(operation, outbox, mappings);
        }
        if self.resolution_saga(&operation.operation_id)?.is_some() {
            return self.finalize_resolution_saga_dispatch(operation, outbox, mappings);
        }
        self.finalize_backend_dispatch_without_resolution_saga(operation, outbox, mappings, false)
    }

    pub(super) fn finalize_backend_dispatch_without_resolution_saga(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        mappings: &[pb::IssueMapping],
        remove_resolution_saga: bool,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.operation_id != outbox.operation_id {
            return Err(RepositoryError::OutboxOperationMismatch);
        }
        let mut operation = sanitize_operation(operation)?;
        let payload = operation.encode_to_vec();
        let outbox = outbox.clone();
        let mut mappings = mappings.to_vec();
        let materialize_create = operation.method == "manager_create_issue"
            && operation.state == pb::DurableOperationState::Committed as i32;
        let terminal_error = if operation.state == pb::DurableOperationState::Failed as i32 {
            operation
                .terminal_snapshot
                .as_ref()
                .and_then(|snapshot| match snapshot {
                    pb::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot)
                        if snapshot.schema == "issue_tracker.manager.v1.ManagerError" =>
                    {
                        pb::ManagerError::decode(snapshot.payload.as_slice()).ok()
                    }
                    _ => None,
                })
        } else {
            None
        };
        let terminal_error_payload = terminal_error.as_ref().map(|error| error.encode_to_vec());
        let terminal_error_schema = terminal_error_payload
            .as_ref()
            .map(|_| "issue_tracker.manager.v1.ManagerError");
        let terminal_error_result = terminal_error
            .clone()
            .map(|error| super::super::manager_error_result(&operation.method, error));
        let (_, commit) =
            self.storage
                .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                    let current_payload: Vec<u8> = transaction.query_row(
                        "SELECT operation_payload FROM manager_operation
                           WHERE operation_id=?1",
                        params![operation.operation_id],
                        |row| row.get(0),
                    )?;
                    let current = pb::DurableOperation::decode(current_payload.as_slice())
                        .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                    if current.operation_id != operation.operation_id
                        || current.generation != operation.generation
                        || !matches!(
                            pb::DurableOperationState::try_from(current.state),
                            Ok(pb::DurableOperationState::Received
                                | pb::DurableOperationState::Pending
                                | pb::DurableOperationState::Unconfirmed
                                | pb::DurableOperationState::ManualResolution)
                        )
                    {
                        return Err(RepositoryError::OperationConflict);
                    }
                    let current_payload_digest = digest_bytes(&current_payload);
                    let extraction_auto_registration = extraction_auto_registration_in_transaction(
                        transaction,
                        &operation.operation_id,
                    )?;
                    if let Some(link) = extraction_auto_registration.as_ref()
                        && materialize_create
                    {
                        if mappings.len() != 1 || mappings[0].issue_id != link.issue_id {
                            return Err(RepositoryError::RequestSnapshot(
                                "auto-registration create mapping is missing or mismatched".into(),
                            ));
                        }
                        mappings[0].candidate_id = Some(link.candidate_id.clone());
                    }
                    let changed = transaction.execute(
                        "UPDATE manager_operation SET state=?2, attempt=?3,
                         manager_sequence=?4, generation=?5, updated_at=?6,
                         result_snapshot=NULL, result_snapshot_schema=NULL,
                         result_snapshot_digest=NULL, error_snapshot=?7,
                         error_snapshot_schema=?8, error_snapshot_digest=?9,
                         operation_payload=?10, operation_payload_digest=?11
                       WHERE operation_id=?1 AND operation_payload_digest=?12",
                        params![
                            operation.operation_id,
                            operation.state,
                            i64::from(operation.attempt),
                            i64::try_from(stamp.manager_sequence)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                            i64::try_from(stamp.generation)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                            operation_updated_at_millis(&operation, now_millis()),
                            terminal_error_payload.as_deref(),
                            terminal_error_schema,
                            terminal_error_payload
                                .as_ref()
                                .map(|payload| digest_bytes(payload)),
                            payload,
                            digest_bytes(&payload),
                            current_payload_digest,
                        ],
                    )?;
                    if changed != 1 {
                        return Err(RepositoryError::OperationConflict);
                    }
                    update_outbox_in_transaction(transaction, &outbox)?;
                    for mapping in &mappings {
                        let input = EntityInput {
                            kind: EntityKind::IssueMapping,
                            entity_id: mapping.issue_id.clone(),
                            revision: mapping.revision,
                            payload_schema: "issue_tracker.manager.v1.IssueMapping".into(),
                            payload: mapping.encode_to_vec(),
                        };
                        if materialize_create {
                            write_entity_idempotently_in_transaction(transaction, &input)?;
                            upsert_issue_mapping_index_in_transaction(
                                transaction,
                                mapping,
                                &operation.operation_id,
                                now_millis(),
                            )?;
                        } else {
                            write_entity_in_transaction_with_operation(
                                transaction,
                                &input,
                                None,
                                stamp,
                                &operation.operation_id,
                            )?;
                        }
                    }
                    if let Some(link) = extraction_auto_registration.as_ref() {
                        finalize_extraction_auto_registration_in_transaction(
                            transaction,
                            stamp,
                            &operation,
                            link,
                        )?;
                    }
                    let typed_wrapper = if operation.state
                        == pb::DurableOperationState::Committed as i32
                    {
                        let mutation = Self::confirmed_backend_mutation(transaction, &operation)?;
                        operation.terminal_snapshot = Some(
                            pb::durable_operation::TerminalSnapshot::ResultSnapshot(pb::Snapshot {
                                schema: MANAGER_MUTATION_SNAPSHOT_SCHEMA.into(),
                                payload: sanitize_mutation(&mutation).encode_to_vec(),
                            }),
                        );
                        let operation = sanitize_operation(&operation)?;
                        let payload = operation.encode_to_vec();
                        let terminal = match operation.terminal_snapshot.as_ref() {
                            Some(pb::durable_operation::TerminalSnapshot::ResultSnapshot(
                                value,
                            )) => value,
                            _ => {
                                return Err(RepositoryError::RequestSnapshot(
                                    "confirmed backend result snapshot is missing".into(),
                                ));
                            }
                        };
                        let changed = transaction.execute(
                            "UPDATE manager_operation
                        SET operation_payload=?2, operation_payload_digest=?3,
                            result_snapshot=?4, result_snapshot_schema=?5,
                            result_snapshot_digest=?6, updated_at=?7
                      WHERE operation_id=?1",
                            params![
                                operation.operation_id,
                                payload,
                                digest_bytes(&payload),
                                terminal.payload.as_slice(),
                                terminal.schema.as_str(),
                                digest_bytes(&terminal.payload),
                                operation_updated_at_millis(&operation, now_millis())
                            ],
                        )?;
                        if changed != 1 {
                            return Err(RepositoryError::OperationConflict);
                        }
                        let method = operation.method.clone();
                        let mut wrapped = mutation;
                        wrapped.operation = Some(operation);
                        Some(super::super::gateway::mutation_wrapper_for_method(&method)
                            .ok_or_else(|| {
                                RepositoryError::RequestSnapshot(format!(
                                    "unknown operation method: {method}"
                                ))
                            })?(Ok(wrapped)))
                    } else {
                        None
                    };
                    let changed = transaction.execute(
                        "UPDATE manager_idempotency
                    SET state = ?2,
                        result_snapshot = COALESCE(?3, result_snapshot),
                        error_snapshot = COALESCE(?4, error_snapshot),
                        updated_at = ?5
                  WHERE operation_id = ?1",
                        params![
                            operation.operation_id,
                            operation.state,
                            typed_wrapper.or(terminal_error_result),
                            terminal_error_payload.as_deref(),
                            now_millis(),
                        ],
                    )?;
                    if changed != 1 {
                        return Err(RepositoryError::OperationConflict);
                    }
                    // The terminal snapshot is materialized after the first
                    // operation update; refresh the projection last so its
                    // state mirrors the final durable payload.
                    upsert_operation_history_projection(transaction, &operation, now_millis())?;
                    if remove_resolution_saga
                        && transaction.execute(
                            "DELETE FROM manager_resolution_saga WHERE operation_id=?1",
                            params![operation.operation_id],
                        )? != 1
                    {
                        return Err(RepositoryError::OperationConflict);
                    }
                    Ok(())
                })?;
        Ok(commit)
    }

    pub(super) fn finalize_acceptance_dispatch(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        mappings: &[pb::IssueMapping],
    ) -> Result<ManagerCommit, RepositoryError> {
        let candidate_id: String = self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT candidate_id FROM manager_acceptance_guard WHERE operation_id=?1",
                    params![operation.operation_id],
                    |row| row.get(0),
                )
                .map_err(ManagerStorageError::from)
        })?;
        let prepared = (|| -> Result<_, RepositoryError> {
            let mapping = mappings
                .first()
                .filter(|_| mappings.len() == 1)
                .ok_or_else(|| {
                    RepositoryError::RequestSnapshot(
                        "acceptance backend mapping is missing or ambiguous".into(),
                    )
                })?;
            let candidate: pb::Candidate = decode_stored(
                self.get(EntityKind::Candidate, &candidate_id)?
                    .ok_or_else(|| {
                        RepositoryError::RequestSnapshot("acceptance candidate is missing".into())
                    })?,
                "issue_tracker.manager.v1.Candidate",
            )
            .map_err(|error| RepositoryError::RequestSnapshot(error.message))?;
            let candidate_revision = candidate.revision;
            let accepted = CandidateStore::from_candidates(vec![candidate])
                .accept(&candidate_id, candidate_revision, utc_instant(now_millis()))
                .map_err(|error| {
                    RepositoryError::RequestSnapshot(format!(
                        "acceptance transition failed: {error:?}"
                    ))
                })?;
            let mut mapping = mapping.clone();
            mapping.candidate_id = Some(candidate_id.clone());
            Ok((accepted, mapping, operation.clone()))
        })();
        let (accepted, mapping, operation) = match prepared {
            Ok(value) => value,
            Err(error) => {
                return self.resolve_acceptance_manual_resolution(
                    operation,
                    outbox,
                    error.to_string(),
                );
            }
        };
        let receipt = committed_create_resolution_receipt(&operation, &accepted, &mapping);
        let typed_result = pb::ResolveCandidateResult {
            outcome: Some(pb::resolve_candidate_result::Outcome::Success(receipt)),
        }
        .encode_to_vec();
        self.finalize_confirmed_acceptance(ConfirmedAcceptance {
            operation: &operation,
            outbox,
            candidate: &accepted,
            mapping: &mapping,
            typed_result: &typed_result,
        })
    }

    pub(super) fn finalize_merge_issue_saga(
        &mut self,
        saga: &ResolutionSaga,
        candidate: &pb::Candidate,
        operation: &pb::DurableOperation,
    ) -> Result<(EntityInput, Vec<EntityInput>, pb::ResolutionReceipt), String> {
        let Some(pb::resolve_candidate_args::Decision::Merge(merge)) = saga.args.decision.as_ref()
        else {
            return Err("merge resolution saga has an invalid decision payload".into());
        };
        let Some(pb::merge_resolution::Target::Issue(target)) = merge.target.as_ref() else {
            return Err("merge resolution saga has an invalid target".into());
        };
        if target.issue_id != saga.issue_id {
            return Err("merge resolution saga target changed".into());
        }
        if !matches!(
            pb::CandidateStatus::try_from(candidate.status),
            Ok(pb::CandidateStatus::Proposed
                | pb::CandidateStatus::AutoRegistered
                | pb::CandidateStatus::Accepted)
        ) {
            return Err("merge source candidate is no longer resolvable".into());
        }
        let superseded = supersede_candidate(
            candidate,
            SupersedingTarget::Issue(&target.issue_id),
            utc_instant(now_millis()),
        )?;
        let candidate_input = entity_input(
            EntityKind::Candidate,
            &superseded.candidate_id,
            superseded.revision,
            "issue_tracker.manager.v1.Candidate",
            superseded.encode_to_vec(),
        )
        .map_err(|error| error.to_string())?;
        let receipt = committed_resolution_saga_receipt(
            operation,
            &superseded,
            None,
            &saga.issue_id,
            target
                .expected_revision
                .and_then(|value| value.checked_add(1)),
        );
        Ok((candidate_input, Vec::new(), receipt))
    }

    pub(super) fn finalize_resolution_saga_dispatch(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        mappings: &[pb::IssueMapping],
    ) -> Result<ManagerCommit, RepositoryError> {
        let saga = self
            .resolution_saga(&operation.operation_id)?
            .ok_or(RepositoryError::OperationConflict)?;
        if operation.state != pb::DurableOperationState::Committed as i32 {
            // A failed backend write must not decide the candidate. Finalize
            // the terminal operation and discard the plan in one transaction.
            return self.finalize_backend_dispatch_without_resolution_saga(
                operation, outbox, mappings, true,
            );
        }
        if operation.operation_id != outbox.operation_id {
            return Err(RepositoryError::OutboxOperationMismatch);
        }
        let stored = match self.get(EntityKind::Candidate, &saga.candidate_id)? {
            Some(stored) => stored,
            None => {
                return self.resolve_resolution_saga_manual_resolution(
                    operation,
                    outbox,
                    "resolution candidate is missing".into(),
                );
            }
        };
        let mut candidate: pb::Candidate =
            match decode_stored(stored, "issue_tracker.manager.v1.Candidate") {
                Ok(candidate) => candidate,
                Err(error) => {
                    return self.resolve_resolution_saga_manual_resolution(
                        operation,
                        outbox,
                        format!("resolution candidate cannot be decoded: {}", error.message),
                    );
                }
            };
        if candidate.revision != saga.candidate_revision {
            return self.resolve_resolution_saga_manual_resolution(
                operation,
                outbox,
                format!(
                    "resolution candidate revision changed after backend commit: expected {}, got {}",
                    saga.candidate_revision, candidate.revision
                ),
            );
        }
        let existing_mapping = self
            .get(EntityKind::IssueMapping, &saga.issue_id)?
            .map(|stored| {
                decode_stored::<pb::IssueMapping>(stored, "issue_tracker.manager.v1.IssueMapping")
                    .map_err(|error| RepositoryError::RequestSnapshot(error.message))
            })
            .transpose();
        let existing_mapping = match existing_mapping {
            Ok(mapping) => mapping,
            Err(error) => {
                return self.resolve_resolution_saga_manual_resolution(
                    operation,
                    outbox,
                    format!("resolution mapping cannot be decoded: {error}"),
                );
            }
        };
        let backend_mapping = mappings
            .iter()
            .find(|mapping| mapping.issue_id == saga.issue_id)
            .cloned();
        let (candidate_input, mapping_input, removal, binding_inputs, receipt) = match saga.decision
        {
            ResolutionSagaDecision::ConfirmAssociatedIssue => {
                if !matches!(
                    pb::CandidateStatus::try_from(candidate.status),
                    Ok(pb::CandidateStatus::Proposed | pb::CandidateStatus::AutoRegistered)
                ) {
                    return self.resolve_resolution_saga_manual_resolution(
                        operation,
                        outbox,
                        "resolution candidate is no longer confirmable".into(),
                    );
                }
                let Some(mut mapping) = backend_mapping.clone().or(existing_mapping.clone()) else {
                    return self.resolve_resolution_saga_manual_resolution(
                        operation,
                        outbox,
                        "confirmed issue marker has no mapping".into(),
                    );
                };
                if let Some(existing) = existing_mapping {
                    if existing.candidate_id.as_deref() != Some(saga.candidate_id.as_str()) {
                        return self.resolve_resolution_saga_manual_resolution(
                            operation,
                            outbox,
                            "associated issue mapping changed after backend commit".into(),
                        );
                    }
                    mapping.candidate_id = existing.candidate_id;
                    if backend_mapping.is_none() {
                        mapping.revision = existing.revision;
                    }
                } else {
                    mapping.candidate_id = Some(saga.candidate_id.clone());
                }
                candidate.revision = candidate
                    .revision
                    .checked_add(1)
                    .ok_or(RepositoryError::RevisionOverflow)?;
                candidate.status = pb::CandidateStatus::Accepted as i32;
                candidate.decision_reason = pb::CandidateDecisionReason::Accepted as i32;
                candidate.updated_at = Some(utc_instant(now_millis()));
                let candidate_input = entity_input(
                    EntityKind::Candidate,
                    &candidate.candidate_id,
                    candidate.revision,
                    "issue_tracker.manager.v1.Candidate",
                    candidate.encode_to_vec(),
                )?;
                let mapping_input = entity_input(
                    EntityKind::IssueMapping,
                    &mapping.issue_id,
                    mapping.revision,
                    "issue_tracker.manager.v1.IssueMapping",
                    mapping.encode_to_vec(),
                )?;
                let receipt = committed_resolution_saga_receipt(
                    operation,
                    &candidate,
                    Some(&mapping),
                    &saga.issue_id,
                    Some(mapping.revision),
                );
                (
                    candidate_input,
                    Some(mapping_input),
                    None,
                    Vec::new(),
                    receipt,
                )
            }
            ResolutionSagaDecision::CancelAssociatedIssue => {
                let Some(mapping) = existing_mapping else {
                    return self.resolve_resolution_saga_manual_resolution(
                        operation,
                        outbox,
                        "associated issue mapping is missing after backend commit".into(),
                    );
                };
                if mapping.candidate_id.as_deref() != Some(saga.candidate_id.as_str()) {
                    return self.resolve_resolution_saga_manual_resolution(
                        operation,
                        outbox,
                        "associated issue mapping changed after backend commit".into(),
                    );
                }
                if !matches!(
                    pb::CandidateStatus::try_from(candidate.status),
                    Ok(pb::CandidateStatus::AutoRegistered | pb::CandidateStatus::Accepted)
                ) {
                    return self.resolve_resolution_saga_manual_resolution(
                        operation,
                        outbox,
                        "resolution candidate is no longer cancellable".into(),
                    );
                }
                candidate.revision = candidate
                    .revision
                    .checked_add(1)
                    .ok_or(RepositoryError::RevisionOverflow)?;
                candidate.status = pb::CandidateStatus::Rejected as i32;
                candidate.decision_reason = pb::CandidateDecisionReason::Rejected as i32;
                candidate.decision_note = match saga.args.decision.as_ref() {
                    Some(pb::resolve_candidate_args::Decision::CancelAssociatedIssue(value)) => {
                        value.rationale.clone()
                    }
                    _ => None,
                };
                candidate.updated_at = Some(utc_instant(now_millis()));
                let candidate_input = entity_input(
                    EntityKind::Candidate,
                    &candidate.candidate_id,
                    candidate.revision,
                    "issue_tracker.manager.v1.Candidate",
                    candidate.encode_to_vec(),
                )?;
                let removal = EntityRemoval {
                    kind: EntityKind::IssueMapping,
                    entity_id: mapping.issue_id,
                    expected_revision: mapping.revision,
                };
                let receipt = committed_resolution_saga_receipt(
                    operation,
                    &candidate,
                    None,
                    &saga.issue_id,
                    backend_mapping.as_ref().map(|mapping| mapping.revision),
                );
                (candidate_input, None, Some(removal), Vec::new(), receipt)
            }
            ResolutionSagaDecision::MergeIssue => match self
                .finalize_merge_issue_saga(&saga, &candidate, operation)
            {
                Ok((candidate, bindings, receipt)) => (candidate, None, None, bindings, receipt),
                Err(reason) => {
                    return self
                        .resolve_resolution_saga_manual_resolution(operation, outbox, reason);
                }
            },
        };
        let result = pb::ResolveCandidateResult {
            outcome: Some(pb::resolve_candidate_result::Outcome::Success(receipt)),
        }
        .encode_to_vec();
        let mut committed = operation.clone();
        committed.terminal_snapshot = Some(
            pb::durable_operation::TerminalSnapshot::ResultSnapshot(pb::Snapshot {
                payload: result.clone(),
                schema: "issue_tracker.manager.v1.ResolveCandidateResult".into(),
            }),
        );
        let committed = sanitize_operation(&committed)?;
        let operation_payload = committed.encode_to_vec();
        let outbox = outbox.clone();
        self.storage.write_with::<_, RepositoryError, _>(|transaction, stamp| {
            for binding in &binding_inputs {
                write_entity_idempotently_in_transaction(transaction, binding)?;
            }
            write_entity_in_transaction(transaction, &candidate_input, Some(saga.candidate_revision), stamp)?;
            if let Some(removal) = &removal {
                remove_entity_in_transaction(transaction, removal)?;
            } else if let Some(mapping_input) = &mapping_input {
                write_entity_in_transaction_with_operation(
                    transaction,
                    mapping_input,
                    None,
                    stamp,
                    &operation.operation_id,
                )?;
            } else if saga.decision != ResolutionSagaDecision::MergeIssue {
                return Err(RepositoryError::OperationConflict);
            }
            let changed = transaction.execute(
                "UPDATE manager_operation SET state=?2, manager_sequence=?3, generation=?4,
                    result_snapshot=?5, result_snapshot_schema=?6, result_snapshot_digest=?7,
                    operation_payload=?8, operation_payload_digest=?9, updated_at=?10
                  WHERE operation_id=?1",
                params![committed.operation_id, committed.state,
                    i64::try_from(stamp.manager_sequence).map_err(|_| RepositoryError::RevisionOverflow)?,
                    i64::try_from(stamp.generation).map_err(|_| RepositoryError::RevisionOverflow)?,
                    result, "issue_tracker.manager.v1.ResolveCandidateResult", digest_bytes(&result),
                    operation_payload,
                    digest_bytes(&operation_payload),
                    operation_updated_at_millis(&committed, now_millis()),
                ],
            )?;
            if changed != 1 { return Err(RepositoryError::OperationConflict); }
            update_outbox_in_transaction(transaction, &outbox)?;
            transaction.execute(
                "UPDATE manager_idempotency SET state=?2, result_snapshot=?3, updated_at=?4 WHERE operation_id=?1",
                params![committed.operation_id, committed.state, result, now_millis()],
            )?;
            upsert_operation_history_projection(transaction, &committed, now_millis())?;
            Ok(())
        }).map(|(_, commit)| commit)
    }

    /// Evaluate the worker-admission predicate from the currently persisted
    /// policy and manager-owned evidence; callers cannot weaken it.
    pub(crate) fn gateway_policy_allows_issue(
        &self,
        issue: &v1::Issue,
        policy: &pb::WorkEligibilityPolicy,
        evaluated_at_millis: i64,
    ) -> Result<bool, pb::ManagerError> {
        if issue.state != v1::IssueState::Open as i32
            || issue.claim.is_some()
            || issue.state == v1::IssueState::Blocked as i32
        {
            return Ok(false);
        }
        match pb::WorkEligibilityMode::try_from(policy.mode) {
            Ok(pb::WorkEligibilityMode::ReviewedOnly) => {
                Ok(issue.confirmation_status == v1::ConfirmationStatus::Confirmed as i32)
            }
            Ok(pb::WorkEligibilityMode::IncludeAutoRegistered) => {
                if issue.confirmation_status == v1::ConfirmationStatus::Confirmed as i32 {
                    return Ok(true);
                }
                self.auto_registered_issue_has_fresh_evidence(issue, evaluated_at_millis)
            }
            Ok(pb::WorkEligibilityMode::Unspecified) | Err(_) => {
                Err(domain_error("gateway work eligibility policy is invalid"))
            }
        }
    }

    pub(super) fn auto_registered_issue_has_fresh_evidence(
        &self,
        issue: &v1::Issue,
        _evaluated_at_millis: i64,
    ) -> Result<bool, pb::ManagerError> {
        if issue.confirmation_status != v1::ConfirmationStatus::Unreviewed as i32 {
            return Ok(false);
        }
        let Some(stored) = self
            .get(EntityKind::IssueMapping, &issue.issue_id)
            .map_err(map_repository_error)?
        else {
            return Ok(false);
        };
        let mapping: pb::IssueMapping =
            decode_stored(stored, "issue_tracker.manager.v1.IssueMapping")?;
        let Some(candidate_id) = mapping.candidate_id else {
            return Ok(false);
        };
        let Some(stored_candidate) = self
            .get(EntityKind::Candidate, &candidate_id)
            .map_err(map_repository_error)?
        else {
            return Ok(false);
        };
        let candidate: pb::Candidate =
            decode_stored(stored_candidate, "issue_tracker.manager.v1.Candidate")?;
        if candidate.status != pb::CandidateStatus::AutoRegistered as i32 {
            return Ok(false);
        }
        Ok(!issue.source_evidence.is_empty())
    }

    /// A marker proves the backend write, but it cannot prove that the
    /// manager-side acceptance evidence stayed current while the write was in
    /// flight.  Resolve that ambiguity atomically and never redeliver it.
    pub(super) fn resolve_acceptance_manual_resolution(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        reason: String,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.resolve_post_dispatch_manual_resolution(
            operation,
            outbox,
            reason,
            PostDispatchPlan::Acceptance,
        )
    }

    pub(super) fn resolve_resolution_saga_manual_resolution(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        reason: String,
    ) -> Result<ManagerCommit, RepositoryError> {
        self.resolve_post_dispatch_manual_resolution(
            operation,
            outbox,
            reason,
            PostDispatchPlan::ResolutionSaga,
        )
    }

    /// A backend marker only proves its canonical write. If the manager-side
    /// completion plan is no longer safe, atomically retain a terminal,
    /// auditable operation instead of leaving a retryable outbox behind.
    pub(super) fn resolve_post_dispatch_manual_resolution(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        reason: String,
        plan: PostDispatchPlan,
    ) -> Result<ManagerCommit, RepositoryError> {
        if operation.operation_id != outbox.operation_id {
            return Err(RepositoryError::OutboxOperationMismatch);
        }
        let diagnosis_class = operation_diagnosis_class(&reason);
        let evidence_digest = digest_bytes(reason.as_bytes());
        let error_snapshot = pb::ManagerError {
            code: pb::ManagerErrorCode::ResolutionRequired as i32,
            message: reason,
            retryable: false,
            detail: None,
        };
        let error_payload = error_snapshot.encode_to_vec();
        let mut operation = operation.clone();
        operation.state = pb::DurableOperationState::ManualResolution as i32;
        operation.failure_class = pb::FailureClass::Terminal as i32;
        operation.resolution_required = true;
        operation.updated_at = Some(utc_instant(now_millis()));
        operation.terminal_snapshot = Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(
            pb::Snapshot {
                payload: error_payload.clone(),
                schema: "issue_tracker.manager.v1.ManagerError".into(),
            },
        ));
        operation.diagnosis = Some(pb::OperationDiagnosis {
            operation_id: operation.operation_id.clone(),
            classification: diagnosis_class,
            source_generation: Some(operation.generation),
            attempt_generation: Some(operation.generation),
            evidence_ref: format!("manager-operation-diagnosis:{}", operation.operation_id),
            evidence_digest,
            replayability: "NOT_REPLAYABLE".into(),
            resolution_state: "MANUAL_RESOLUTION".into(),
            resolution_operation_id: None,
            repair_condition: error_snapshot.message.clone(),
            recorded_at: Some(utc_instant(now_millis())),
        });
        let operation = sanitize_operation(&operation)?;
        let operation_payload = operation.encode_to_vec();
        let mut outbox = outbox.clone();
        outbox.status = pb::OutboxStatus::DeadLetter as i32;
        outbox.error_code = pb::ManagerErrorCode::ResolutionRequired as i32;
        outbox.lease_id = None;
        outbox.lease_expires_at = None;
        outbox.next_attempt_at = None;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let updated = transaction.execute(
                    "UPDATE manager_operation
                        SET state=?2, attempt=?3, manager_sequence=?4, generation=?5,
                            updated_at=?6, result_snapshot=NULL, result_snapshot_schema=NULL,
                            result_snapshot_digest=NULL, error_snapshot=?7,
                            error_snapshot_schema=?8, error_snapshot_digest=?9,
                            operation_payload=?10, operation_payload_schema=?11,
                            operation_payload_digest=?12, diagnosis=?13
                      WHERE operation_id=?1",
                    params![
                        operation.operation_id,
                        operation.state,
                        i64::from(operation.attempt),
                        i64::try_from(stamp.manager_sequence)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(stamp.generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        operation_updated_at_millis(&operation, now_millis()),
                        error_payload,
                        "issue_tracker.manager.v1.ManagerError",
                        digest_bytes(&error_payload),
                        operation_payload,
                        "issue_tracker.manager.v1.DurableOperation",
                        digest_bytes(&operation_payload),
                        operation
                            .diagnosis
                            .as_ref()
                            .map(prost::Message::encode_to_vec),
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                // Keep the query-only history row in lockstep with the
                // terminal manual-resolution durable payload.
                super::domain::upsert_operation_history_projection(
                    transaction,
                    &operation,
                    operation_updated_at_millis(&operation, now_millis()),
                )?;
                let (contract, root) = manager_root_identity(transaction)?;
                if let Some(diagnosis) = operation.diagnosis.as_ref() {
                    let generation: i64 = transaction.query_row(
                        "SELECT COALESCE(MAX(diagnosis_generation), 0) + 1
                           FROM manager_operation_diagnosis
                          WHERE contract_fingerprint=?1 AND data_root_id=?2
                            AND operation_id=?3 AND classification=?4
                            AND evidence_digest=?5",
                        params![
                            &contract,
                            &root,
                            &diagnosis.operation_id,
                            &diagnosis.classification,
                            &diagnosis.evidence_digest,
                        ],
                        |row| row.get(0),
                    )?;
                    transaction.execute(
                        "INSERT INTO manager_operation_diagnosis
                            (contract_fingerprint, data_root_id, operation_id,
                             diagnosis_generation, classification, source_generation,
                             attempt_generation, evidence_ref, evidence_digest,
                             replayability, resolution_state, resolution_operation_id,
                             repair_condition, recorded_at, updated_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                                 NULL, ?12, ?13, ?13)
                         ON CONFLICT(contract_fingerprint, data_root_id, operation_id,
                                     classification, evidence_digest)
                         DO UPDATE SET resolution_state=excluded.resolution_state,
                             repair_condition=excluded.repair_condition,
                             updated_at=excluded.updated_at",
                        params![
                            &contract,
                            &root,
                            &diagnosis.operation_id,
                            generation,
                            &diagnosis.classification,
                            diagnosis
                                .source_generation
                                .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                            diagnosis
                                .attempt_generation
                                .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                            &diagnosis.evidence_ref,
                            &diagnosis.evidence_digest,
                            &diagnosis.replayability,
                            &diagnosis.resolution_state,
                            &diagnosis.repair_condition,
                            diagnosis
                                .recorded_at
                                .as_ref()
                                .map(|value| value.unix_epoch_millis),
                        ],
                    )?;
                }
                update_outbox_in_transaction(transaction, &outbox)?;
                let updated = transaction.execute(
                    "UPDATE manager_idempotency
                        SET state=?2, result_snapshot=NULL, error_snapshot=?3, updated_at=?4
                      WHERE operation_id=?1",
                    params![
                        operation.operation_id,
                        operation.state,
                        error_payload,
                        now_millis(),
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                let plan_deleted = match plan {
                    PostDispatchPlan::Acceptance => transaction.execute(
                        "DELETE FROM manager_acceptance_guard WHERE operation_id=?1",
                        params![operation.operation_id],
                    )?,
                    PostDispatchPlan::ResolutionSaga => transaction.execute(
                        "DELETE FROM manager_resolution_saga WHERE operation_id=?1",
                        params![operation.operation_id],
                    )?,
                };
                if plan_deleted != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                Ok(())
            })
            .map(|(_, commit)| commit)
    }

    /// Rebuild the confirmed mutation after marker check.  The admission
    /// snapshot contains no canonical Issue; create-issue injects mapping
    /// facts only.  Callers persist a sanitized ManagerMutation as the
    /// terminal snapshot and wrap a copy for idempotency replay.
    pub(super) fn confirmed_backend_mutation(
        transaction: &Transaction<'_>,
        operation: &pb::DurableOperation,
    ) -> Result<pb::ManagerMutation, RepositoryError> {
        let (snapshot, schema, digest): (Vec<u8>, String, String) = transaction.query_row(
            "SELECT snapshot, snapshot_schema, snapshot_digest
               FROM manager_operation
              WHERE operation_id = ?1",
            params![operation.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if schema != MANAGER_MUTATION_SNAPSHOT_SCHEMA || digest_bytes(&snapshot) != digest {
            return Err(RepositoryError::RequestSnapshot(
                "backend mutation snapshot integrity validation failed".into(),
            ));
        }
        let mut mutation = pb::ManagerMutation::decode(snapshot.as_slice())
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        if operation.method == "manager_create_issue"
            && operation.state == pb::DurableOperationState::Committed as i32
        {
            mutation.result = Some(pb::manager_mutation::Result::IssueCreation(
                Self::issue_creation_result_in_transaction(transaction, operation)?,
            ));
        }
        mutation.operation = None;
        Ok(mutation)
    }

    pub(super) fn issue_creation_result_in_transaction(
        transaction: &Transaction<'_>,
        operation: &pb::DurableOperation,
    ) -> Result<pb::IssueCreationResult, RepositoryError> {
        let (request, schema): (Vec<u8>, String) = transaction.query_row(
            "SELECT request_snapshot, request_snapshot_schema
               FROM manager_operation
              WHERE operation_id = ?1",
            params![operation.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if schema != "issue_tracker.manager.v1.DurableBackendRequest" {
            return Err(RepositoryError::RequestSnapshot(
                "create request snapshot schema is invalid".into(),
            ));
        }
        let envelope = DurableBackendRequest::decode(request.as_slice())
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        let create = envelope.create_issue.ok_or_else(|| {
            RepositoryError::RequestSnapshot("create request intent is missing".into())
        })?;
        let mapping_payload: Vec<u8> = transaction.query_row(
            "SELECT payload FROM manager_entity
                  WHERE entity_kind = 'issue_mapping' AND entity_id = ?1",
            params![create.issue_id],
            |row| row.get(0),
        )?;
        let mapping = pb::IssueMapping::decode(mapping_payload.as_slice())
            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
        Ok(pb::IssueCreationResult {
            mapping: Some(mapping),
            ..Default::default()
        })
    }

    /// Move a leased outbox whose request uses the removed raw-snapshot schema
    /// to terminal manual resolution without invoking the backend. The
    /// operation and outbox rows are updated in one manager transaction, so a
    /// restart cannot turn the legacy payload into a new delivery attempt.
    pub(crate) fn quarantine_legacy_outbox(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        now_millis: i64,
    ) -> Result<(pb::DurableOperation, pb::OutboxEntry), RepositoryError> {
        self.quarantine_outbox(
            operation,
            outbox,
            now_millis,
            "legacy raw backend request snapshot requires manual resolution",
        )
    }

    /// Terminally isolate a leased backend operation whose durable metadata is
    /// corrupt or no longer matches the outbox entry. No backend call is made.
    pub(crate) fn quarantine_outbox(
        &mut self,
        operation: &pb::DurableOperation,
        outbox: &pb::OutboxEntry,
        now_millis: i64,
        reason: &str,
    ) -> Result<(pb::DurableOperation, pb::OutboxEntry), RepositoryError> {
        if operation.operation_id != outbox.operation_id {
            return Err(RepositoryError::OutboxOperationMismatch);
        }
        let mut operation = operation.clone();
        match pb::DurableOperationState::try_from(operation.state)
            .map_err(|_| RepositoryError::RequestSnapshot("invalid operation state".into()))?
        {
            pb::DurableOperationState::Received => {
                transition_operation(
                    &mut operation,
                    pb::DurableOperationState::Pending,
                    None,
                    now_millis,
                )
                .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
                transition_operation(
                    &mut operation,
                    pb::DurableOperationState::Unconfirmed,
                    None,
                    now_millis,
                )
                .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
            }
            pb::DurableOperationState::Pending => {
                transition_operation(
                    &mut operation,
                    pb::DurableOperationState::Unconfirmed,
                    None,
                    now_millis,
                )
                .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
            }
            pb::DurableOperationState::Unconfirmed => {}
            state => {
                return Err(RepositoryError::RequestSnapshot(format!(
                    "legacy outbox operation is not pending: {}",
                    state.as_str_name()
                )));
            }
        }
        transition_operation(
            &mut operation,
            pb::DurableOperationState::ManualResolution,
            None,
            now_millis,
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        operation.failure_class = pb::FailureClass::Terminal as i32;
        let error_snapshot = pb::ManagerError {
            code: pb::ManagerErrorCode::ResolutionRequired as i32,
            message: reason.into(),
            retryable: false,
            detail: None,
        };
        operation.terminal_snapshot = Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(
            pb::Snapshot {
                payload: error_snapshot.encode_to_vec(),
                schema: "issue_tracker.manager.v1.ManagerError".into(),
            },
        ));
        // Preserve a safe, typed diagnosis alongside the terminal fence. The
        // original request payload and any lease/capability data stay in the
        // private request tables; only a stable classification and digest are
        // exposed for operator repair.
        let diagnosis_class = operation_diagnosis_class(reason);
        let evidence_digest = digest_bytes(reason.as_bytes());
        operation.diagnosis = Some(pb::OperationDiagnosis {
            operation_id: operation.operation_id.clone(),
            classification: diagnosis_class.clone(),
            source_generation: None,
            attempt_generation: Some(operation.generation),
            evidence_ref: format!("manager-operation-diagnosis:{}", operation.operation_id),
            evidence_digest: evidence_digest.clone(),
            replayability: "NOT_REPLAYABLE".into(),
            resolution_state: "MANUAL_RESOLUTION".into(),
            resolution_operation_id: None,
            repair_condition: reason.to_owned(),
            recorded_at: Some(pb::UtcInstant {
                unix_epoch_millis: now_millis,
            }),
        });
        let operation = sanitize_operation(&operation)?;
        let operation_payload = operation.encode_to_vec();
        let mut outbox = outbox.clone();
        let lease_id = outbox.lease_id.clone().ok_or_else(|| {
            RepositoryError::RequestSnapshot("legacy outbox is not leased".into())
        })?;
        super::super::outbox::dead_letter(
            &mut outbox,
            &lease_id,
            pb::ManagerErrorCode::ResolutionRequired,
            now_millis,
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        let error_payload = operation
            .terminal_snapshot
            .as_ref()
            .and_then(|snapshot| match snapshot {
                pb::durable_operation::TerminalSnapshot::ErrorSnapshot(value) => {
                    Some(value.payload.as_slice())
                }
                pb::durable_operation::TerminalSnapshot::ResultSnapshot(_) => None,
            })
            .ok_or_else(|| RepositoryError::RequestSnapshot("error snapshot is missing".into()))?;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let updated = transaction.execute(
                    "UPDATE manager_operation
                        SET state = ?2,
                            attempt = ?3, manager_sequence = ?4, generation = ?5,
                            updated_at = ?6, result_snapshot = NULL,
                            result_snapshot_schema = NULL, result_snapshot_digest = NULL,
                            error_snapshot = ?7, error_snapshot_schema = ?8,
                            error_snapshot_digest = ?9, operation_payload = ?10,
                            operation_payload_digest = ?11, diagnosis = ?12
                      WHERE operation_id = ?1",
                    params![
                        operation.operation_id,
                        operation.state,
                        i64::from(operation.attempt),
                        i64::try_from(stamp.manager_sequence)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(stamp.generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        operation_updated_at_millis(&operation, now_millis),
                        error_payload,
                        "issue_tracker.manager.v1.ManagerError",
                        digest_bytes(error_payload),
                        operation_payload,
                        digest_bytes(&operation_payload),
                        operation
                            .diagnosis
                            .as_ref()
                            .map(prost::Message::encode_to_vec),
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                super::domain::upsert_operation_history_projection(
                    transaction,
                    &operation,
                    now_millis,
                )?;
                // Close the northbound idempotency row with the same typed
                // rejection. Without this update a quarantined operation
                // remains PENDING forever and a retry cannot discover the
                // durable diagnosis or resolution requirement.
                let response_payload =
                    super::super::manager_error_result(&operation.method, error_snapshot.clone());
                if !response_payload.is_empty() {
                    transaction.execute(
                        "UPDATE manager_idempotency
                            SET state=?2, result_snapshot=?3, error_snapshot=?4,
                                updated_at=?5
                          WHERE operation_id=?1 AND result_snapshot IS NULL",
                        params![
                            operation.operation_id,
                            operation.state,
                            response_payload,
                            error_payload,
                            now_millis,
                        ],
                    )?;
                }
                let (contract, root) = manager_root_identity(transaction)?;
                if let Some(diagnosis) = operation.diagnosis.as_ref() {
                    let generation: i64 = transaction.query_row(
                        "SELECT COALESCE(MAX(diagnosis_generation), 0) + 1
                               FROM manager_operation_diagnosis
                              WHERE contract_fingerprint=?1 AND data_root_id=?2
                                AND operation_id=?3 AND classification=?4
                                AND evidence_digest=?5",
                        params![
                            &contract,
                            &root,
                            &diagnosis.operation_id,
                            &diagnosis.classification,
                            &diagnosis.evidence_digest,
                        ],
                        |row| row.get(0),
                    )?;
                    let existing: Option<i64> = transaction
                        .query_row(
                            "SELECT diagnosis_generation
                               FROM manager_operation_diagnosis
                              WHERE contract_fingerprint=?1 AND data_root_id=?2
                                AND operation_id=?3 AND classification=?4
                                AND evidence_digest=?5
                                AND resolution_state NOT IN ('RESOLVED','STALE','SUPERSEDED')
                              ORDER BY diagnosis_generation DESC LIMIT 1",
                            params![
                                &contract,
                                &root,
                                &diagnosis.operation_id,
                                &diagnosis.classification,
                                &diagnosis.evidence_digest,
                            ],
                            |row| row.get(0),
                        )
                        .optional()?;
                    let generation = existing.unwrap_or(generation);
                    transaction.execute(
                        "INSERT INTO manager_operation_diagnosis
                            (contract_fingerprint, data_root_id, operation_id,
                             diagnosis_generation, classification, source_generation,
                             attempt_generation, evidence_ref, evidence_digest,
                             replayability, resolution_state, resolution_operation_id,
                             repair_condition, recorded_at, updated_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                                 NULL, ?12, ?13, ?13)
                         ON CONFLICT(contract_fingerprint, data_root_id, operation_id,
                                     classification, evidence_digest)
                         DO UPDATE SET attempt_generation=excluded.attempt_generation,
                             evidence_ref=excluded.evidence_ref,
                             replayability=excluded.replayability,
                             resolution_state=excluded.resolution_state,
                             repair_condition=excluded.repair_condition,
                             updated_at=excluded.updated_at",
                        params![
                            &contract,
                            &root,
                            &diagnosis.operation_id,
                            generation,
                            &diagnosis.classification,
                            diagnosis
                                .source_generation
                                .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                            diagnosis
                                .attempt_generation
                                .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                            &diagnosis.evidence_ref,
                            &diagnosis.evidence_digest,
                            &diagnosis.replayability,
                            &diagnosis.resolution_state,
                            &diagnosis.repair_condition,
                            diagnosis
                                .recorded_at
                                .as_ref()
                                .map(|value| value.unix_epoch_millis),
                        ],
                    )?;
                }
                update_outbox_in_transaction(transaction, &outbox)?;
                if let Some(link) = extraction_auto_registration_in_transaction(
                    transaction,
                    &operation.operation_id,
                )? {
                    finalize_extraction_auto_registration_in_transaction(
                        transaction,
                        stamp,
                        &operation,
                        &link,
                    )?;
                }
                Ok(())
            })?;
        Ok((operation, outbox))
    }

    /// Terminally isolates a leased orphan outbox row. There is no operation
    /// row to reconcile, so the outbox itself is the sole durable evidence.
    pub(crate) fn quarantine_orphan_outbox(
        &mut self,
        outbox: &pb::OutboxEntry,
        now_millis: i64,
    ) -> Result<pb::OutboxEntry, RepositoryError> {
        let mut outbox = outbox.clone();
        let lease_id = outbox.lease_id.clone().ok_or_else(|| {
            RepositoryError::RequestSnapshot("orphan outbox is not leased".into())
        })?;
        super::super::outbox::dead_letter(
            &mut outbox,
            &lease_id,
            pb::ManagerErrorCode::ResolutionRequired,
            now_millis,
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                update_outbox_in_transaction(transaction, &outbox)
            })?;
        Ok(outbox)
    }

    /// Terminally isolates an outbox whose operation row exists but cannot be
    /// decoded or validated. The opaque operation payload is retained only for
    /// forensic recovery; it is never dispatched again.
    pub(crate) fn quarantine_corrupt_operation_outbox(
        &mut self,
        outbox: &pb::OutboxEntry,
        now_millis: i64,
    ) -> Result<pb::OutboxEntry, RepositoryError> {
        // Read only the stable metadata columns.  The operation payload is
        // deliberately not decoded here: this function is called precisely
        // when that payload is malformed or its digest/schema is invalid.
        let metadata = self
            .storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT method, idempotency_scope, request_digest, state,
                                generation, attempt, affects_ready
                           FROM manager_operation
                          WHERE operation_id=?1",
                        params![outbox.operation_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, i32>(3)?,
                                row.get::<_, i64>(4)?,
                                row.get::<_, i64>(5)?,
                                row.get::<_, i64>(6)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })?
            .ok_or(RepositoryError::OperationConflict)?;
        let (
            method,
            _idempotency_scope,
            request_digest,
            _state,
            generation,
            _attempt,
            _affects_ready,
        ) = metadata;
        let generation_u64 = u64::try_from(generation).unwrap_or(0);
        let classification = "CORRUPT_OPERATION".to_owned();
        let evidence_digest = digest_bytes(
            format!(
                "operation-integrity\0{}\0{}\0{}",
                outbox.operation_id, method, request_digest
            )
            .as_bytes(),
        );
        let diagnosis = pb::OperationDiagnosis {
            operation_id: outbox.operation_id.clone(),
            classification: classification.clone(),
            source_generation: None,
            attempt_generation: Some(generation_u64),
            evidence_ref: format!("manager-operation-diagnosis:{}", outbox.operation_id),
            evidence_digest: evidence_digest.clone(),
            replayability: "NOT_REPLAYABLE".into(),
            resolution_state: "MANUAL_RESOLUTION".into(),
            resolution_operation_id: None,
            repair_condition: "restore the durable operation payload from a trusted backup".into(),
            recorded_at: Some(pb::UtcInstant {
                unix_epoch_millis: now_millis,
            }),
        };
        let error = pb::ManagerError {
            code: pb::ManagerErrorCode::ResolutionRequired as i32,
            message: diagnosis.repair_condition.clone(),
            retryable: false,
            detail: Some(pb::manager_error::Detail::OperationDiagnosis(
                diagnosis.clone(),
            )),
        };
        let response_payload = crate::manager::manager_error_result(&method, error.clone());
        let error_payload = error.encode_to_vec();
        let diagnosis_payload = diagnosis.encode_to_vec();
        let mut outbox = outbox.clone();
        let lease_id = outbox.lease_id.clone().ok_or_else(|| {
            RepositoryError::RequestSnapshot("corrupt outbox is not leased".into())
        })?;
        super::super::outbox::dead_letter(
            &mut outbox,
            &lease_id,
            pb::ManagerErrorCode::ResolutionRequired,
            now_millis,
        )
        .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, stamp| {
                let updated = transaction.execute(
                    "UPDATE manager_operation
                        SET state=?2, affects_ready=1,
                            manager_sequence=?3, generation=?4, updated_at=?5,
                            error_snapshot=?6, error_snapshot_schema=?7,
                            error_snapshot_digest=?8, diagnosis=?9
                      WHERE operation_id=?1",
                    params![
                        outbox.operation_id,
                        pb::DurableOperationState::ManualResolution as i32,
                        i64::try_from(stamp.manager_sequence)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        i64::try_from(generation_u64.saturating_add(1))
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        now_millis,
                        error_payload,
                        "issue_tracker.manager.v1.ManagerError",
                        digest_bytes(&error.encode_to_vec()),
                        diagnosis_payload,
                    ],
                )?;
                if updated != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                // The payload is intentionally not decoded in this branch,
                // so the normal projection upsert cannot be used.  Keep the
                // indexed lifecycle state visible for state-filtered audit
                // reads; hydration will still reject the corrupt payload as
                // a page-wide history read error.
                transaction.execute(
                    "UPDATE manager_operation_history_projection
                        SET state=?2, updated_at=?3, resolution_required=1
                      WHERE operation_id=?1",
                    params![
                        outbox.operation_id,
                        pb::DurableOperationState::ManualResolution as i32,
                        now_millis,
                    ],
                )?;
                let (contract, root) = manager_root_identity(transaction)?;
                transaction.execute(
                    "INSERT INTO manager_operation_diagnosis
                        (contract_fingerprint, data_root_id, operation_id,
                         diagnosis_generation, classification, source_generation,
                         attempt_generation, evidence_ref, evidence_digest,
                         replayability, resolution_state, resolution_operation_id,
                         repair_condition, recorded_at, updated_at)
                     VALUES (?1, ?2, ?3, 1, ?4, NULL, ?5, ?6, ?7, ?8, ?9,
                             NULL, ?10, ?11, ?11)
                     ON CONFLICT(contract_fingerprint, data_root_id, operation_id,
                                 classification, evidence_digest)
                     DO UPDATE SET resolution_state=excluded.resolution_state,
                         repair_condition=excluded.repair_condition,
                         updated_at=excluded.updated_at",
                    params![
                        contract,
                        root,
                        outbox.operation_id,
                        classification,
                        i64::try_from(generation_u64).unwrap_or(i64::MAX),
                        diagnosis.evidence_ref,
                        diagnosis.evidence_digest,
                        diagnosis.replayability,
                        diagnosis.resolution_state,
                        diagnosis.repair_condition,
                        now_millis,
                    ],
                )?;
                update_outbox_in_transaction(transaction, &outbox)?;
                // A legacy/corrupt row may have lost its idempotency index as
                // well.  The outbox and diagnosis must still be quarantined;
                // requiring this optional index would make one damaged row
                // abort the whole maintenance cycle.  When present, make it
                // terminal and replayable in the same transaction.
                transaction.execute(
                    "UPDATE manager_idempotency
                        SET state=?2, result_snapshot=?3, error_snapshot=?4,
                            updated_at=?5
                      WHERE operation_id=?1",
                    params![
                        outbox.operation_id,
                        pb::DurableOperationState::ManualResolution as i32,
                        response_payload,
                        error.encode_to_vec(),
                        now_millis,
                    ],
                )?;
                transaction.execute(
                    "UPDATE admin_dispatch_attempt
                        SET state=?2, resolved_at=?3, lease_id=NULL,
                            lease_expires_at=NULL, evidence_classification=?4
                      WHERE operation_id=?1 AND attempt_generation <= ?5",
                    params![
                        outbox.operation_id,
                        pb::OutboxStatus::DeadLetter as i32,
                        now_millis,
                        "CORRUPT_OPERATION",
                        i64::try_from(generation_u64).unwrap_or(i64::MAX),
                    ],
                )?;
                Ok(())
            })?;
        Ok(outbox)
    }

    pub(crate) fn lease_due_outbox(
        &mut self,
        now: i64,
        lease_millis: u64,
    ) -> Result<Option<pb::OutboxEntry>, RepositoryError> {
        let id = self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT outbox_id FROM manager_outbox AS outbox
                  WHERE ((status = ?1 AND (next_attempt_at IS NULL OR next_attempt_at <= ?2))
                      OR (status = ?3 AND lease_expires_at <= ?2))
                    AND NOT EXISTS (
                        SELECT 1
                          FROM manager_extraction_auto_registration AS link
                          JOIN manager_extraction_auto_registration_reservation AS reservation
                            ON reservation.run_id = link.run_id
                         WHERE link.operation_id = outbox.operation_id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM resolution_fence AS fence
                         WHERE fence.source_operation_id = outbox.operation_id
                           AND fence.state NOT IN (?4, ?5, ?6)
                    )
                  ORDER BY created_at LIMIT 1",
                    params![
                        pb::OutboxStatus::Ready as i32,
                        now,
                        pb::OutboxStatus::Leased as i32,
                        pb::DurableOperationState::Committed as i32,
                        pb::DurableOperationState::Failed as i32,
                        pb::DurableOperationState::Arbitrated as i32,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(ManagerStorageError::from)
        })?;
        let Some(id) = id else {
            return Ok(None);
        };
        match self.lease_outbox_by_id(&id, now, lease_millis) {
            Err(RepositoryError::OutboxResolutionFenced) => Ok(None),
            result => result.map(Some),
        }
    }

    pub(crate) fn lease_outbox_for_operation(
        &mut self,
        operation_id: &str,
        now: i64,
        lease_millis: u64,
    ) -> Result<Option<pb::OutboxEntry>, RepositoryError> {
        let id = self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT outbox_id FROM manager_outbox AS outbox
                  WHERE operation_id = ?1
                    AND ((status = ?2 AND (next_attempt_at IS NULL OR next_attempt_at <= ?3))
                      OR (status = ?4 AND lease_expires_at <= ?3))
                    AND NOT EXISTS (
                        SELECT 1
                          FROM manager_extraction_auto_registration AS link
                          JOIN manager_extraction_auto_registration_reservation AS reservation
                            ON reservation.run_id = link.run_id
                         WHERE link.operation_id = outbox.operation_id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM resolution_fence AS fence
                         WHERE fence.source_operation_id = outbox.operation_id
                           AND fence.state NOT IN (?5, ?6, ?7)
                    )",
                    params![
                        operation_id,
                        pb::OutboxStatus::Ready as i32,
                        now,
                        pb::OutboxStatus::Leased as i32,
                        pb::DurableOperationState::Committed as i32,
                        pb::DurableOperationState::Failed as i32,
                        pb::DurableOperationState::Arbitrated as i32,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(ManagerStorageError::from)
        })?;
        let Some(id) = id else {
            return Ok(None);
        };
        match self.lease_outbox_by_id(&id, now, lease_millis) {
            Err(RepositoryError::OutboxResolutionFenced) => Ok(None),
            result => result.map(Some),
        }
    }

    pub(super) fn lease_outbox_by_id(
        &mut self,
        outbox_id: &str,
        now: i64,
        lease_millis: u64,
    ) -> Result<pb::OutboxEntry, RepositoryError> {
        let mut entry = self
            .get_outbox(outbox_id)?
            .ok_or(RepositoryError::OperationConflict)?;
        super::super::outbox::lease(&mut entry, now, lease_millis, None)
            .map_err(|error| RepositoryError::RequestSnapshot(error.to_string()))?;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _| {
                if has_pending_resolution_fence(transaction, &entry.operation_id)? {
                    return Err(RepositoryError::OutboxResolutionFenced);
                }
                update_outbox_in_transaction(transaction, &entry)
            })?;
        Ok(entry)
    }
}
