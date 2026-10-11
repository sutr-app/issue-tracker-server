//! Gateway registration, policy, and replay persistence.

use super::*;

impl<'a> ManagerRepository<'a> {
    pub(crate) fn list_gateway_ready_issues(
        &self,
        endpoint_id: &str,
        issues: impl IntoIterator<Item = v1::Issue>,
        eligibility_evaluated_at_millis: i64,
    ) -> Result<GatewayReadySelection, GatewayReadinessError> {
        let registration = self
            .load_gateway_policy(endpoint_id)?
            .ok_or(GatewayReadinessError::Unregistered)?;
        let policy = registration
            .policy
            .as_ref()
            .ok_or_else(|| GatewayReadinessError::Manager("gateway policy is missing".into()))?;
        let mut eligible = issues
            .into_iter()
            .map(|issue| {
                self.gateway_policy_allows_issue(&issue, policy, eligibility_evaluated_at_millis)
                    .map_err(|error| GatewayReadinessError::Manager(error.message))
                    .map(|eligible| eligible.then_some(issue))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        eligible.sort_unstable_by(|left, right| left.issue_id.cmp(&right.issue_id));
        Ok(GatewayReadySelection {
            registration,
            eligibility_evaluated_at_millis,
            issues: eligible,
        })
    }

    /// Re-evaluates the same readiness predicate immediately before claim
    /// admission. A list result is only a hint and cannot authorize a stale
    /// worker claim.
    pub(crate) fn claim_gateway_ready_issue(
        &self,
        endpoint_id: &str,
        issue: &v1::Issue,
        eligibility_evaluated_at_millis: i64,
    ) -> Result<pb::GatewayRegistration, GatewayReadinessError> {
        let registration = self
            .load_gateway_policy(endpoint_id)?
            .ok_or(GatewayReadinessError::Unregistered)?;
        let policy = registration
            .policy
            .as_ref()
            .ok_or_else(|| GatewayReadinessError::Manager("gateway policy is missing".into()))?;
        if !self
            .gateway_policy_allows_issue(issue, policy, eligibility_evaluated_at_millis)
            .map_err(|error| GatewayReadinessError::Manager(error.message))?
        {
            return Err(GatewayReadinessError::Manager(
                "issue is outside the gateway policy".into(),
            ));
        }
        Ok(registration)
    }

    pub(crate) fn register_gateway_policy(
        &mut self,
        endpoint_id: &str,
        gateway_id: &str,
        idempotency_key: &str,
        policy: &pb::WorkEligibilityPolicy,
    ) -> Result<pb::GatewayRegistration, RepositoryError> {
        validate_gateway_registration_input(endpoint_id, gateway_id, policy)?;
        validate_gateway_replay_component(idempotency_key, "idempotency key")?;
        let policy_snapshot = policy.encode_to_vec();
        let policy_fingerprint = digest_bytes(&policy_snapshot);
        let request_digest = policy_fingerprint.clone();
        if let Some((stored_digest, snapshot)) = self.storage.read(|connection| {
            connection
                .query_row(
                    "SELECT request_digest, registration_snapshot
                       FROM manager_gateway_registration_idempotency
                      WHERE endpoint_id = ?1 AND idempotency_key = ?2",
                    params![endpoint_id, idempotency_key],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
                )
                .optional()
                .map_err(ManagerStorageError::from)
        })? {
            if stored_digest != request_digest {
                return Err(RepositoryError::IdempotencyConflict);
            }
            return pb::GatewayRegistration::decode(snapshot.as_slice())
                .map_err(|error| RepositoryError::Serialization(error.to_string()));
        }
        let registration_generation =
            self.storage
                .generation()
                .checked_add(1)
                .ok_or(RepositoryError::RequestSnapshot(
                    "gateway registration generation overflow".into(),
                ))?;
        let registration = pb::GatewayRegistration {
            gateway_id: gateway_id.into(),
            policy: Some(policy.clone()),
            policy_fingerprint,
            registration_generation,
        };
        let registration_snapshot = registration.encode_to_vec();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                transaction.execute(
                    "INSERT INTO manager_gateway_registration
                    (endpoint_id, gateway_id, policy_snapshot, policy_fingerprint,
                     registration_generation, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(endpoint_id) DO UPDATE SET
                    gateway_id = excluded.gateway_id,
                    policy_snapshot = excluded.policy_snapshot,
                    policy_fingerprint = excluded.policy_fingerprint,
                    registration_generation = excluded.registration_generation,
                    updated_at = excluded.updated_at",
                    params![
                        endpoint_id,
                        gateway_id,
                        policy_snapshot,
                        registration.policy_fingerprint.clone(),
                        i64::try_from(registration_generation)
                            .map_err(|_| RepositoryError::RevisionOverflow)?,
                        now_millis(),
                    ],
                )?;
                transaction.execute(
                    "INSERT INTO manager_gateway_registration_idempotency
                     (endpoint_id, idempotency_key, request_digest, registration_snapshot, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        endpoint_id,
                        idempotency_key,
                        request_digest,
                        registration_snapshot,
                        now_millis(),
                    ],
                )?;
                Ok(())
            })?;
        Ok(registration)
    }

    /// Creates a process-local capability epoch for one worker endpoint.
    ///
    /// Replacing the epoch never deletes historical records: they are needed
    /// to distinguish an old retry from a new request using the same key.
    pub(crate) fn start_gateway_capability_epoch(
        &mut self,
        endpoint_id: &str,
    ) -> Result<String, RepositoryError> {
        validate_gateway_replay_component(endpoint_id, "endpoint ID")?;
        let epoch = Uuid::new_v4().to_string();
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                transaction.execute(
                    "INSERT INTO manager_gateway_epoch (endpoint_id, capability_epoch, started_at)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(endpoint_id) DO UPDATE SET
                         capability_epoch = excluded.capability_epoch,
                         started_at = excluded.started_at",
                    params![endpoint_id, epoch, now_millis()],
                )?;
                Ok(())
            })?;
        Ok(epoch)
    }

    /// Reserves a Gateway idempotency key in the active epoch.
    ///
    /// The reservation is intentionally durable before the caller executes
    /// the write. If execution completes but the response cannot be recorded,
    /// future retries are unconfirmed rather than accidentally replaying a
    /// stale capability after restart.
    pub(crate) fn reserve_gateway_replay(
        &mut self,
        endpoint_id: &str,
        capability_epoch: &str,
        idempotency_key: &str,
        rpc_method: &str,
        request_digest: &str,
    ) -> Result<GatewayReplayDecision, RepositoryError> {
        validate_gateway_replay_input(
            endpoint_id,
            capability_epoch,
            idempotency_key,
            rpc_method,
            request_digest,
        )?;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                let active_epoch = query_active_gateway_epoch(transaction, endpoint_id)?;
                if active_epoch.as_deref() != Some(capability_epoch) {
                    return Ok(GatewayReplayDecision::Unconfirmed);
                }
                let current = query_gateway_replay(
                    transaction,
                    endpoint_id,
                    capability_epoch,
                    idempotency_key,
                )?;
                if let Some(record) = current {
                    return Ok(gateway_replay_decision_for_record(
                        &record,
                        rpc_method,
                        request_digest,
                    ));
                }
                if let Some(decision) = gateway_replay_old_epoch_decision(
                    transaction,
                    endpoint_id,
                    capability_epoch,
                    idempotency_key,
                    rpc_method,
                    request_digest,
                )? {
                    return Ok(decision);
                }
                let now = now_millis();
                transaction.execute(
                    "INSERT INTO manager_gateway_replay_record
                      (endpoint_id, capability_epoch, idempotency_key, rpc_method,
                       request_digest, response_snapshot, response_schema, operation_state,
                       created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, ?6, ?7, ?7)",
                    params![
                        endpoint_id,
                        capability_epoch,
                        idempotency_key,
                        rpc_method,
                        request_digest,
                        pb::DurableOperationState::Unspecified as i32,
                        now,
                    ],
                )?;
                Ok(GatewayReplayDecision::New)
            })
            .map(|(decision, _)| decision)
    }

    /// Returns the current replay state without reserving a new key.
    #[allow(dead_code)]
    pub(crate) fn inspect_gateway_replay(
        &self,
        endpoint_id: &str,
        capability_epoch: &str,
        idempotency_key: &str,
        rpc_method: &str,
        request_digest: &str,
    ) -> Result<GatewayReplayDecision, RepositoryError> {
        validate_gateway_replay_input(
            endpoint_id,
            capability_epoch,
            idempotency_key,
            rpc_method,
            request_digest,
        )?;
        Ok(self.storage.read(|connection| {
            if query_active_gateway_epoch(connection, endpoint_id)
                .map_err(ManagerStorageError::from)?
                .as_deref()
                != Some(capability_epoch)
            {
                return Ok(GatewayReplayDecision::Unconfirmed);
            }
            if let Some(record) =
                query_gateway_replay(connection, endpoint_id, capability_epoch, idempotency_key)
                    .map_err(ManagerStorageError::from)?
            {
                return Ok(gateway_replay_decision_for_record(
                    &record,
                    rpc_method,
                    request_digest,
                ));
            }
            if let Some(decision) = gateway_replay_old_epoch_decision(
                connection,
                endpoint_id,
                capability_epoch,
                idempotency_key,
                rpc_method,
                request_digest,
            )
            .map_err(ManagerStorageError::from)?
            {
                return Ok(decision);
            }
            Ok(GatewayReplayDecision::New)
        })?)
    }

    /// Stores the immutable successful response for an already-reserved key.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn complete_gateway_replay(
        &mut self,
        endpoint_id: &str,
        capability_epoch: &str,
        idempotency_key: &str,
        rpc_method: &str,
        request_digest: &str,
        response_schema: &str,
        response_snapshot: &[u8],
    ) -> Result<(), RepositoryError> {
        validate_gateway_replay_input(
            endpoint_id,
            capability_epoch,
            idempotency_key,
            rpc_method,
            request_digest,
        )?;
        validate_gateway_replay_component(response_schema, "response schema")?;
        if response_snapshot.is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "gateway replay response snapshot must not be empty".into(),
            ));
        }
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                let active_epoch = query_active_gateway_epoch(transaction, endpoint_id)?;
                if active_epoch.as_deref() != Some(capability_epoch) {
                    return Err(RepositoryError::RequestSnapshot(
                        "gateway capability epoch is no longer active".into(),
                    ));
                }
                let updated = transaction.execute(
                    "UPDATE manager_gateway_replay_record
                        SET response_snapshot = ?6,
                            response_schema = ?7,
                            operation_state = ?8,
                            updated_at = ?9
                      WHERE endpoint_id = ?1
                        AND capability_epoch = ?2
                        AND idempotency_key = ?3
                        AND rpc_method = ?4
                        AND request_digest = ?5
                        AND response_snapshot IS NULL",
                    params![
                        endpoint_id,
                        capability_epoch,
                        idempotency_key,
                        rpc_method,
                        request_digest,
                        response_snapshot,
                        response_schema,
                        pb::DurableOperationState::Committed as i32,
                        now_millis(),
                    ],
                )?;
                if updated == 1 {
                    return Ok(());
                }
                match query_gateway_replay(
                    transaction,
                    endpoint_id,
                    capability_epoch,
                    idempotency_key,
                )? {
                    Some(record)
                        if record.rpc_method == rpc_method
                            && record.request_digest == request_digest
                            && record.response_snapshot.is_some() =>
                    {
                        Ok(())
                    }
                    _ => Err(RepositoryError::IdempotencyConflict),
                }
            })?;
        Ok(())
    }

    /// Completes an awaiting-review reservation after the caller has verified
    /// the exact immutable TaskChampion marker and history. Unlike normal
    /// completion this may update a reservation from an inactive capability
    /// epoch; it cannot authorize a new backend write.
    pub(crate) fn complete_gateway_replay_from_canonical(
        &mut self,
        endpoint_id: &str,
        idempotency_key: &str,
        rpc_method: &str,
        request_digest: &str,
        response_schema: &str,
        response_snapshot: &[u8],
    ) -> Result<(), RepositoryError> {
        validate_gateway_replay_component(endpoint_id, "endpoint ID")?;
        validate_gateway_replay_component(idempotency_key, "idempotency key")?;
        validate_gateway_replay_component(request_digest, "request digest")?;
        if rpc_method != AWAITING_REVIEW_REPLAY_METHOD
            || response_schema != AWAITING_REVIEW_REPLAY_SCHEMA
        {
            return Err(RepositoryError::RequestSnapshot(
                "canonical gateway recovery is limited to awaiting-review completion".into(),
            ));
        }
        if response_snapshot.is_empty() {
            return Err(RepositoryError::RequestSnapshot(
                "gateway replay response snapshot must not be empty".into(),
            ));
        }
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                let matching_count: i64 = transaction.query_row(
                    "SELECT COUNT(*)
                       FROM manager_gateway_replay_record
                      WHERE endpoint_id = ?1
                        AND idempotency_key = ?2
                        AND rpc_method = ?3
                        AND request_digest = ?4",
                    params![endpoint_id, idempotency_key, rpc_method, request_digest],
                    |row| row.get(0),
                )?;
                if matching_count != 1 {
                    return Err(RepositoryError::OperationConflict);
                }
                let updated = transaction.execute(
                    "UPDATE manager_gateway_replay_record
                        SET response_snapshot = ?5,
                            response_schema = ?6,
                            operation_state = ?7,
                            updated_at = ?8
                      WHERE endpoint_id = ?1
                        AND idempotency_key = ?2
                        AND rpc_method = ?3
                        AND request_digest = ?4
                        AND response_snapshot IS NULL
                        AND operation_state = ?9",
                    params![
                        endpoint_id,
                        idempotency_key,
                        rpc_method,
                        request_digest,
                        response_snapshot,
                        response_schema,
                        pb::DurableOperationState::Committed as i32,
                        now_millis(),
                        pb::DurableOperationState::Unspecified as i32,
                    ],
                )?;
                if updated == 1 {
                    return Ok(());
                }
                let (stored_schema, stored_snapshot, stored_state) = transaction.query_row(
                    "SELECT response_schema, response_snapshot, operation_state
                       FROM manager_gateway_replay_record
                      WHERE endpoint_id = ?1
                        AND idempotency_key = ?2
                        AND rpc_method = ?3
                        AND request_digest = ?4",
                    params![endpoint_id, idempotency_key, rpc_method, request_digest],
                    |row| {
                        Ok((
                            row.get::<_, Option<String>>(0)?,
                            row.get::<_, Option<Vec<u8>>>(1)?,
                            row.get::<_, i32>(2)?,
                        ))
                    },
                )?;
                if stored_schema.as_deref() == Some(response_schema)
                    && stored_snapshot.as_deref() == Some(response_snapshot)
                    && stored_state == pb::DurableOperationState::Committed as i32
                {
                    Ok(())
                } else {
                    Err(RepositoryError::OperationConflict)
                }
            })?;
        Ok(())
    }

    /// Removes a reservation only after a deterministic rejection.
    ///
    /// A completed snapshot and an old epoch are evidence of a possibly
    /// successful operation, so neither can be discarded by this path.
    pub(crate) fn discard_gateway_replay_reservation(
        &mut self,
        endpoint_id: &str,
        capability_epoch: &str,
        idempotency_key: &str,
        rpc_method: &str,
        request_digest: &str,
    ) -> Result<(), RepositoryError> {
        validate_gateway_replay_input(
            endpoint_id,
            capability_epoch,
            idempotency_key,
            rpc_method,
            request_digest,
        )?;
        self.storage
            .write_with::<_, RepositoryError, _>(|transaction, _stamp| {
                if query_active_gateway_epoch(transaction, endpoint_id)?.as_deref()
                    != Some(capability_epoch)
                {
                    return Err(RepositoryError::OperationConflict);
                }
                let Some(record) = query_gateway_replay(
                    transaction,
                    endpoint_id,
                    capability_epoch,
                    idempotency_key,
                )?
                else {
                    return Err(RepositoryError::OperationConflict);
                };
                if record.rpc_method != rpc_method || record.request_digest != request_digest {
                    return Err(RepositoryError::IdempotencyConflict);
                }
                if record.response_snapshot.is_some() {
                    return Err(RepositoryError::OperationConflict);
                }
                let deleted = transaction.execute(
                    "DELETE FROM manager_gateway_replay_record
                      WHERE endpoint_id = ?1
                        AND capability_epoch = ?2
                        AND idempotency_key = ?3
                        AND rpc_method = ?4
                        AND request_digest = ?5
                        AND response_snapshot IS NULL",
                    params![
                        endpoint_id,
                        capability_epoch,
                        idempotency_key,
                        rpc_method,
                        request_digest,
                    ],
                )?;
                if deleted == 1 {
                    Ok(())
                } else {
                    Err(RepositoryError::OperationConflict)
                }
            })?;
        Ok(())
    }

    pub(crate) fn load_gateway_policy(
        &self,
        endpoint_id: &str,
    ) -> Result<Option<pb::GatewayRegistration>, RepositoryError> {
        if endpoint_id.trim().is_empty() {
            return Err(RepositoryError::EmptyEntityId);
        }
        self.storage
                .read(|connection| {
                    connection
                    .query_row(
                        "SELECT gateway_id, policy_snapshot, policy_fingerprint, registration_generation
                       FROM manager_gateway_registration
                      WHERE endpoint_id = ?1",
                        params![endpoint_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, Vec<u8>>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, i64>(3)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
                })?
                .map_or(
                    Ok(None),
                    |(gateway_id, snapshot, fingerprint, generation)| {
                        let policy = pb::WorkEligibilityPolicy::decode(snapshot.as_slice())
                            .map_err(|error| RepositoryError::Serialization(error.to_string()))?;
                        validate_gateway_registration_input(endpoint_id, &gateway_id, &policy)?;
                        if digest_bytes(&snapshot) != fingerprint {
                            return Err(RepositoryError::RequestSnapshot(
                                "gateway policy fingerprint is invalid".into(),
                            ));
                        }
                        Ok(Some(pb::GatewayRegistration {
                            gateway_id,
                            policy: Some(policy),
                            policy_fingerprint: fingerprint,
                            registration_generation: u64::try_from(generation)
                                .map_err(|_| RepositoryError::RevisionOverflow)?,
                        }))
                    },
                )
    }
}
