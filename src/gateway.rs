//! Worker-only Issue Gateway facade.

use crate::{
    manager::repository::{
        AWAITING_REVIEW_REPLAY_METHOD, AWAITING_REVIEW_REPLAY_SCHEMA, GatewayReplayDecision,
    },
    proto_types::{manager_v1, rpc_v1, v1},
    server::GrpcServer,
    tracker::issue_write::awaiting_review_gateway_correlation_id,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

const EXTERNAL_ACTOR_ID: &str = "issue-tracker:external-agent";
const CLAIM_REPLAY_SCHEMA: &str = "issue_tracker.rpc.v1.IssueGatewayClaimed";
const MUTATION_REPLAY_SCHEMA: &str = "issue_tracker.rpc.v1.IssueGatewayMutation";
const AWAITING_REVIEW_STATE_CONTRACT: &str = "awaiting_review/v1";

fn now_millis() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn lease_expiry(issue: &v1::Issue) -> Result<i64, rpc_v1::IssueGatewayError> {
    issue
        .claim
        .as_ref()
        .and_then(|claim| claim.lease_expires_at.as_ref())
        .map(|expiry| expiry.unix_epoch_millis)
        .filter(|expiry| *expiry > now_millis())
        .ok_or_else(|| gateway_error("unconfirmed", "claim did not return an active lease expiry"))
}

#[derive(Clone, Debug)]
struct LeaseCapability {
    handle: Vec<u8>,
    issue_id: String,
    claim_revision: u64,
    expires_at_millis: i64,
}

struct LeaseCapabilityState {
    capabilities: HashMap<String, LeaseCapability>,
}

#[derive(Clone)]
pub(crate) struct LeaseCapabilities {
    epoch: Arc<str>,
    inner: Arc<Mutex<LeaseCapabilityState>>,
}

impl LeaseCapabilities {
    pub(crate) fn for_epoch(epoch: impl Into<Arc<str>>) -> Self {
        Self {
            epoch: epoch.into(),
            inner: Arc::new(Mutex::new(LeaseCapabilityState {
                capabilities: HashMap::new(),
            })),
        }
    }

    pub(crate) fn epoch(&self) -> &str {
        &self.epoch
    }

    fn with_live_state<T>(&self, operation: impl FnOnce(&mut LeaseCapabilityState) -> T) -> T {
        let mut state = self.inner.lock().expect("lease capability map");
        Self::prune_expired(&mut state);
        operation(&mut state)
    }

    fn insert(&self, id: String, value: LeaseCapability) {
        self.with_live_state(|state| {
            state.capabilities.insert(id, value);
        });
    }
    fn get(&self, id: &str) -> Option<LeaseCapability> {
        self.with_live_state(|state| state.capabilities.get(id).cloned())
    }

    fn prune_expired(state: &mut LeaseCapabilityState) {
        let now = now_millis();
        state
            .capabilities
            .retain(|_, capability| capability.expires_at_millis > now);
    }
    fn remove(&self, id: &str) {
        self.with_live_state(|state| {
            state.capabilities.remove(id);
        });
    }

    fn update_lease(&self, id: &str, revision: u64, expires_at_millis: i64) {
        self.with_live_state(|state| {
            if let Some(capability) = state.capabilities.get_mut(id) {
                capability.claim_revision = revision;
                capability.expires_at_millis = expires_at_millis;
            }
        });
    }

    fn complete_transition(&self, id: &str, target_state: v1::IssueState, revision: Option<u64>) {
        if matches!(
            target_state,
            v1::IssueState::Done | v1::IssueState::Canceled | v1::IssueState::AwaitingReview
        ) {
            self.remove(id);
        } else if let Some(revision) = revision {
            self.with_live_state(|state| {
                if let Some(capability) = state.capabilities.get_mut(id) {
                    capability.claim_revision = revision;
                }
            });
        }
    }
}

/// One-shot fault injection shared by a server and its Gateway workers.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct GatewayTestFaults {
    pub(crate) replay_persistence: std::sync::atomic::AtomicBool,
    pub(crate) store_context: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl GatewayTestFaults {
    fn take_replay_persistence(&self) -> bool {
        self.replay_persistence
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    fn take_store_context(&self) -> bool {
        self.store_context
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    }
}

impl Default for LeaseCapabilities {
    fn default() -> Self {
        Self::for_epoch(Arc::<str>::from(""))
    }
}

impl GrpcServer {
    fn capabilities(&self) -> &LeaseCapabilities {
        &self.capabilities
    }
    fn bound_gateway_id(&self) -> Result<&str, rpc_v1::IssueGatewayError> {
        self.gateway_id()
            .ok_or_else(|| gateway_error("unavailable", "gateway endpoint is not bound"))
    }
    async fn store_context(
        &self,
    ) -> Result<manager_v1::ManagerReadContext, rpc_v1::IssueGatewayError> {
        #[cfg(test)]
        if self.gateway_test_faults.take_store_context() {
            return Err(gateway_error(
                "unavailable",
                "injected gateway store context failure",
            ));
        }
        self.app
            .manager_store_context()
            .await
            .map_err(|error| gateway_error("unavailable", error))
    }
    async fn current_issue(
        &self,
        store: &manager_v1::ManagerReadContext,
        issue_id: &str,
    ) -> Result<v1::Issue, rpc_v1::IssueGatewayError> {
        let result = self
            .dispatch_manager_for_bound_gateway::<_, manager_v1::GetIssueWithSyncResult>(
                "manager_get_issue_with_sync",
                manager_v1::GetIssueWithSyncArgs {
                    context: Some(read_context(store)),
                    issue_id: issue_id.into(),
                },
            )
            .await
            .map_err(|_| gateway_error("unavailable", "gateway operation is unavailable"))?;
        match result.outcome {
            Some(manager_v1::get_issue_with_sync_result::Outcome::Success(value)) => value
                .issue
                .ok_or_else(|| gateway_error("not_found", "issue has no canonical snapshot")),
            Some(manager_v1::get_issue_with_sync_result::Outcome::Error(error)) => {
                Err(manager_gateway_error(error))
            }
            None => Err(gateway_error(
                "unconfirmed",
                "issue read returned no outcome",
            )),
        }
    }

    async fn current_issue_revision(
        &self,
        store: &manager_v1::ManagerReadContext,
        issue_id: &str,
    ) -> Result<u64, rpc_v1::IssueGatewayError> {
        Ok(self.current_issue(store, issue_id).await?.revision)
    }

    async fn reserve_gateway_replay<T>(
        &self,
        replay: &GatewayReplayContext,
    ) -> Result<Option<T>, rpc_v1::IssueGatewayError>
    where
        T: Message + Default,
    {
        let decision = self.gateway_replay_decision(replay).await?;
        decode_gateway_replay(decision, replay.response_schema)
    }

    async fn gateway_replay_decision(
        &self,
        replay: &GatewayReplayContext,
    ) -> Result<GatewayReplayDecision, rpc_v1::IssueGatewayError> {
        self.app
            .reserve_gateway_replay(
                self.bound_gateway_id()?,
                self.capabilities().epoch(),
                &replay.idempotency_key,
                replay.method,
                &replay.request_digest,
            )
            .await
            .map_err(|_| gateway_error("unavailable", "gateway replay store is unavailable"))
    }

    async fn complete_gateway_replay<T: Message>(
        &self,
        replay: &GatewayReplayContext,
        response: &T,
    ) -> Result<(), rpc_v1::IssueGatewayError> {
        #[cfg(test)]
        if self.gateway_test_faults.take_replay_persistence() {
            return Err(gateway_error(
                "unconfirmed",
                "injected gateway replay persistence failure",
            ));
        }
        self.app
            .complete_gateway_replay(
                self.bound_gateway_id()?,
                self.capabilities().epoch(),
                &replay.idempotency_key,
                replay.method,
                &replay.request_digest,
                replay.response_schema,
                &response.encode_to_vec(),
            )
            .await
            .map_err(|_| gateway_error("unconfirmed", "gateway replay outcome is not confirmed"))
    }

    async fn registered_gateway_policy(
        &self,
        endpoint_id: &str,
    ) -> Result<manager_v1::GatewayRegistration, rpc_v1::IssueGatewayError> {
        self.app
            .load_gateway_policy(endpoint_id)
            .await
            .map_err(|_| gateway_error("unavailable", "gateway policy is unavailable"))?
            .ok_or_else(|| gateway_error("unavailable", "gateway has no registered policy"))
    }

    async fn complete_gateway_replay_from_canonical<T: Message>(
        &self,
        replay: &GatewayReplayContext,
        response: &T,
    ) -> Result<(), rpc_v1::IssueGatewayError> {
        self.app
            .complete_gateway_replay_from_canonical(
                self.bound_gateway_id()?,
                &replay.idempotency_key,
                replay.method,
                &replay.request_digest,
                replay.response_schema,
                &response.encode_to_vec(),
            )
            .await
            .map_err(|_| gateway_error("unconfirmed", "gateway replay outcome is not confirmed"))
    }

    async fn recover_awaiting_review_replay(
        &self,
        replay: &GatewayReplayContext,
        request: &rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest,
        expected_revision: u64,
        completion: &v1::AwaitingReviewCompletion,
    ) -> Result<rpc_v1::IssueGatewayAwaitingReviewMutation, rpc_v1::IssueGatewayError> {
        let issue_id = Uuid::parse_str(&request.issue_id)
            .map_err(|_| gateway_error("unconfirmed", "canonical completion proof is missing"))?;
        let canonical_commit = self
            .app
            .verify_awaiting_review_commit(
                issue_id,
                expected_revision,
                EXTERNAL_ACTOR_ID,
                &replay.request_digest,
                completion,
            )
            .await
            .map_err(|_| gateway_error("unconfirmed", "canonical completion proof is missing"))?;
        if !canonical_commit {
            return Err(gateway_error(
                "unconfirmed",
                "canonical completion proof does not match the request",
            ));
        }
        let revision = expected_revision.checked_add(1).ok_or_else(|| {
            gateway_error("unconfirmed", "canonical completion revision is invalid")
        })?;
        let receipt = awaiting_review_receipt(&request.issue_id, revision, completion);
        self.complete_gateway_replay_from_canonical(replay, &receipt)
            .await?;
        Ok(receipt)
    }

    async fn awaiting_review_outcome(
        &self,
        mut request: rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest,
    ) -> Result<rpc_v1::IssueGatewayAwaitingReviewMutation, rpc_v1::IssueGatewayError> {
        require_non_empty(&request.issue_id, "issue_id")?;
        require_non_empty(&request.idempotency_key, "idempotency_key")?;
        if request.state_contract_version != AWAITING_REVIEW_STATE_CONTRACT {
            return Err(gateway_error(
                "precondition_failed",
                "consumer must opt in to awaiting_review/v1",
            ));
        }
        let expected_revision = require_revision(request.expected_revision)?;
        let missing_completion = || gateway_error("invalid_argument", "completion is required");
        let run_id = &request
            .completion
            .as_ref()
            .ok_or_else(missing_completion)?
            .run_id;
        require_non_empty(run_id, "completion.run_id")?;

        let endpoint_id = self.bound_gateway_id()?;
        let registration = self.registered_gateway_policy(endpoint_id).await?;
        let replay = GatewayReplayContext {
            method: AWAITING_REVIEW_REPLAY_METHOD,
            request_digest: awaiting_review_request_digest(
                &mut request,
                endpoint_id,
                &registration,
            ),
            idempotency_key: request.idempotency_key.clone(),
            response_schema: AWAITING_REVIEW_REPLAY_SCHEMA,
        };
        let request = request;
        let completion = request.completion.as_ref().ok_or_else(missing_completion)?;
        let replay_decision = self.gateway_replay_decision(&replay).await?;
        if replay_decision == GatewayReplayDecision::AwaitingReviewRecovery {
            return self
                .recover_awaiting_review_replay(&replay, &request, expected_revision, completion)
                .await;
        }
        if let Some(stored) = decode_gateway_replay::<rpc_v1::IssueGatewayAwaitingReviewMutation>(
            replay_decision,
            replay.response_schema,
        )? {
            let expected_receipt = expected_revision
                .checked_add(1)
                .map(|revision| awaiting_review_receipt(&request.issue_id, revision, completion));
            return if expected_receipt.as_ref() == Some(&stored) {
                Ok(stored)
            } else {
                Err(gateway_error(
                    "unconfirmed",
                    "stored awaiting-review replay does not match the request",
                ))
            };
        }

        let (capability, store) = match self.prepare_awaiting_review_dispatch(&request).await {
            Ok(prepared) => prepared,
            Err(error) => {
                // Nothing has reached the Manager yet. Release the
                // reservation and keep the capability so the identical
                // request can be retried instead of entering recovery.
                return Err(
                    match self.discard_gateway_replay_reservation(&replay).await {
                        Ok(()) => error,
                        Err(discard_error) => discard_error,
                    },
                );
            }
        };
        match self
            .dispatch_awaiting_review(
                &request,
                &replay,
                expected_revision,
                completion,
                capability,
                &store,
            )
            .await
        {
            Ok(value) => {
                self.capabilities().remove(&request.capability_id);
                self.complete_gateway_replay(&replay, &value).await?;
                Ok(value)
            }
            Err(error) => {
                if matches!(error.code.as_str(), "unavailable" | "unconfirmed") {
                    // Delivery may have committed before the response was
                    // lost. Do not let this one-time in-memory capability
                    // authorize another mutation against an unknown result.
                    self.capabilities().remove(&request.capability_id);
                }
                Err(self.gateway_replay_rejection(&replay, error).await)
            }
        }
    }

    async fn prepare_awaiting_review_dispatch(
        &self,
        request: &rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest,
    ) -> Result<(LeaseCapability, manager_v1::ManagerReadContext), rpc_v1::IssueGatewayError> {
        let capability = capability_for(
            self.capabilities(),
            &request.capability_id,
            &request.issue_id,
        )?;
        let store = self.store_context().await?;
        Ok((capability, store))
    }

    async fn dispatch_awaiting_review(
        &self,
        request: &rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest,
        replay: &GatewayReplayContext,
        expected_revision: u64,
        completion: &v1::AwaitingReviewCompletion,
        capability: LeaseCapability,
        store: &manager_v1::ManagerReadContext,
    ) -> Result<rpc_v1::IssueGatewayAwaitingReviewMutation, rpc_v1::IssueGatewayError> {
        let mut context = gateway_issue_write_context(
            store,
            "manager_transition_issue",
            &request.issue_id,
            &request.idempotency_key,
        );
        context.correlation_id = awaiting_review_gateway_correlation_id(&replay.request_digest);
        if let Some(actor) = context.actor_audit.as_mut() {
            actor.agent_session_id = Some(completion.run_id.clone());
        }
        let manager_result = self
            .dispatch_manager_for_bound_gateway::<_, manager_v1::TransitionIssueResult>(
                "manager_transition_issue",
                manager_v1::TransitionIssueArgs {
                    context: Some(context),
                    issue_id: request.issue_id.clone(),
                    expected_revision: Some(expected_revision),
                    target_state: v1::IssueState::AwaitingReview as i32,
                    lease_handle: Some(capability.handle),
                    claim_revision: Some(capability.claim_revision),
                    awaiting_review: Some(completion.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| gateway_error("unavailable", "gateway operation is unavailable"))?;
        match manager_result.outcome {
            Some(manager_v1::transition_issue_result::Outcome::Success(mutation)) => {
                let revision = issue_revision(&mutation, &request.issue_id).ok_or_else(|| {
                    gateway_error("unconfirmed", "mutation did not return an issue revision")
                })?;
                Ok(awaiting_review_receipt(
                    &request.issue_id,
                    revision,
                    completion,
                ))
            }
            Some(manager_v1::transition_issue_result::Outcome::Error(error)) => {
                Err(manager_gateway_error(error))
            }
            None => Err(gateway_error(
                "unconfirmed",
                "awaiting-review completion returned no outcome",
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_gateway_replay_persistence(&self) {
        self.gateway_test_faults
            .replay_persistence
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_gateway_store_context(&self) {
        self.gateway_test_faults
            .store_context
            .store(true, std::sync::atomic::Ordering::Release);
    }

    async fn discard_gateway_replay_reservation(
        &self,
        replay: &GatewayReplayContext,
    ) -> Result<(), rpc_v1::IssueGatewayError> {
        self.app
            .discard_gateway_replay_reservation(
                self.bound_gateway_id()?,
                self.capabilities().epoch(),
                &replay.idempotency_key,
                replay.method,
                &replay.request_digest,
            )
            .await
            .map_err(|_| gateway_error("unconfirmed", "gateway replay outcome is not confirmed"))
    }

    async fn gateway_replay_rejection(
        &self,
        replay: &GatewayReplayContext,
        error: rpc_v1::IssueGatewayError,
    ) -> rpc_v1::IssueGatewayError {
        if !should_discard_gateway_replay_reservation(&error) {
            return error;
        }
        match self.discard_gateway_replay_reservation(replay).await {
            Ok(()) => error,
            Err(discard_error) => discard_error,
        }
    }
}

trait GatewayReplayRequest {
    fn idempotency_key(&self) -> &str;
}

#[derive(Clone, Debug)]
struct GatewayReplayContext {
    method: &'static str,
    request_digest: String,
    idempotency_key: String,
    response_schema: &'static str,
}

impl GatewayReplayContext {
    fn from_request<T: Message + GatewayReplayRequest>(
        method: &'static str,
        request: &T,
        response_schema: &'static str,
    ) -> Self {
        Self {
            method,
            request_digest: gateway_request_digest(method, request),
            idempotency_key: request.idempotency_key().into(),
            response_schema,
        }
    }
}

impl GatewayReplayRequest for rpc_v1::IssueGatewayClaimRequest {
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

impl GatewayReplayRequest for rpc_v1::IssueGatewayHeartbeatRequest {
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

impl GatewayReplayRequest for rpc_v1::IssueGatewayTransitionRequest {
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

fn awaiting_review_request_digest(
    request: &mut rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest,
    endpoint_id: &str,
    registration: &manager_v1::GatewayRegistration,
) -> String {
    // The capability is an in-memory authorization handle, not part of the
    // immutable operation identity. Omitting it permits a confirmed replay
    // after restart without persisting or resubmitting the old capability.
    // It is detached temporarily instead of cloning the request, which can
    // carry megabytes of evidence.
    let capability_id = std::mem::take(&mut request.capability_id);
    let request_digest = gateway_request_digest(AWAITING_REVIEW_REPLAY_METHOD, request);
    request.capability_id = capability_id;
    let registration_snapshot = registration.encode_to_vec();
    let actor_kind = (v1::ActorKind::ExternalAgent as i32).to_be_bytes();
    let mut digest = Sha256::new();
    digest.update(b"issue-gateway/awaiting-review-registration/v1\0");
    for value in [
        request_digest.as_bytes(),
        endpoint_id.as_bytes(),
        registration_snapshot.as_slice(),
        EXTERNAL_ACTOR_ID.as_bytes(),
        actor_kind.as_slice(),
    ] {
        digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(value);
    }
    format!("sha256:{:x}", digest.finalize())
}

fn awaiting_review_receipt(
    issue_id: &str,
    revision: u64,
    completion: &v1::AwaitingReviewCompletion,
) -> rpc_v1::IssueGatewayAwaitingReviewMutation {
    rpc_v1::IssueGatewayAwaitingReviewMutation {
        issue_id: issue_id.to_owned(),
        revision,
        run_id: completion.run_id.clone(),
        full_commit_id: completion.full_commit_id.clone(),
        evidence_references: completion.evidence_references.clone(),
        lease_closed: true,
    }
}

fn decode_gateway_replay<T: Message + Default>(
    decision: GatewayReplayDecision,
    response_schema: &str,
) -> Result<Option<T>, rpc_v1::IssueGatewayError> {
    match decision {
        GatewayReplayDecision::New => Ok(None),
        GatewayReplayDecision::Replay {
            response_schema: actual_schema,
            response_snapshot,
        } if actual_schema == response_schema => T::decode(response_snapshot.as_slice())
            .map(Some)
            .map_err(|_| gateway_error("unconfirmed", "stored gateway replay is invalid")),
        GatewayReplayDecision::Replay { .. } | GatewayReplayDecision::Unconfirmed => Err(
            gateway_error("unconfirmed", "gateway replay outcome is not confirmed"),
        ),
        GatewayReplayDecision::AwaitingReviewRecovery => Err(gateway_error(
            "unconfirmed",
            "canonical awaiting-review recovery is required",
        )),
        GatewayReplayDecision::Conflict => Err(gateway_error(
            "idempotency_conflict",
            "idempotency key is already bound to a different request",
        )),
    }
}

fn should_discard_gateway_replay_reservation(error: &rpc_v1::IssueGatewayError) -> bool {
    !matches!(error.code.as_str(), "unavailable" | "unconfirmed")
}

fn gateway_issue_write_context(
    store: &manager_v1::ManagerReadContext,
    method: &str,
    issue_id: &str,
    key: &str,
) -> manager_v1::ManagerWriteContext {
    manager_v1::ManagerWriteContext {
        method: method.into(),
        target: format!("issue:{issue_id}"),
        idempotency_key: key.into(),
        correlation_id: store.correlation_id.clone(),
        schema_fingerprint: store.schema_fingerprint.clone(),
        data_root_id: store.data_root_id.clone(),
        contract_fingerprint: store.contract_fingerprint.clone(),
        actor_audit: Some(manager_v1::ActorAudit {
            actor_id: EXTERNAL_ACTOR_ID.into(),
            actor_kind: v1::ActorKind::ExternalAgent as i32,
            agent_session_id: Some(Uuid::new_v4().to_string()),
            ..Default::default()
        }),
    }
}
fn read_context(store: &manager_v1::ManagerReadContext) -> manager_v1::ManagerReadContext {
    manager_v1::ManagerReadContext {
        correlation_id: Uuid::new_v4().to_string(),
        schema_fingerprint: store.schema_fingerprint.clone(),
        data_root_id: store.data_root_id.clone(),
        contract_fingerprint: store.contract_fingerprint.clone(),
    }
}
fn gateway_error(code: &str, message: impl Into<String>) -> rpc_v1::IssueGatewayError {
    rpc_v1::IssueGatewayError {
        code: code.into(),
        message: message.into(),
        ..Default::default()
    }
}

fn manager_gateway_error(error: manager_v1::ManagerError) -> rpc_v1::IssueGatewayError {
    use manager_v1::ManagerErrorCode as ManagerCode;

    let code = ManagerCode::try_from(error.code).unwrap_or(ManagerCode::BackendUnavailable);
    match code {
        ManagerCode::ResultUnconfirmed => {
            gateway_error("unconfirmed", "operation outcome is not confirmed")
        }
        ManagerCode::PreconditionFailed => gateway_error(
            "precondition_failed",
            "operation precondition is not satisfied",
        ),
        ManagerCode::RevisionConflict => {
            let detail = match error.detail {
                Some(manager_v1::manager_error::Detail::RevisionConflict(detail)) => {
                    rpc_v1::GatewayRevisionConflictDetail {
                        expected_revision: detail.expected_revision,
                        current_revision: detail.current_revision,
                    }
                }
                _ => rpc_v1::GatewayRevisionConflictDetail::default(),
            };
            rpc_v1::IssueGatewayError {
                code: "revision_conflict".into(),
                message: "issue revision does not match the current revision".into(),
                detail: Some(rpc_v1::issue_gateway_error::Detail::RevisionConflict(
                    detail,
                )),
            }
        }
        ManagerCode::IdempotencyConflict => gateway_error(
            "idempotency_conflict",
            "idempotency key is already bound to a different request",
        ),
        ManagerCode::IssueNotFound => gateway_error("not_found", "issue was not found"),
        ManagerCode::ManagerInvalidArgument => {
            gateway_error("invalid_argument", "request is invalid")
        }
        _ => gateway_error(
            "unavailable",
            format!("gateway operation is unavailable: {}", error.message),
        ),
    }
}

fn issue_blocked_error() -> rpc_v1::IssueGatewayError {
    rpc_v1::IssueGatewayError {
        code: "precondition_failed".into(),
        message: "issue is blocked and requires an authorized internal resume".into(),
        detail: Some(rpc_v1::issue_gateway_error::Detail::Precondition(
            rpc_v1::GatewayPreconditionDetail {
                precondition_kind:
                    rpc_v1::gateway_precondition_detail::PreconditionKind::IssueBlocked as i32,
            },
        )),
    }
}

fn require_worker_transition_allowed(
    current_state: v1::IssueState,
    target_state: v1::IssueState,
) -> Result<(), rpc_v1::IssueGatewayError> {
    if target_state == v1::IssueState::AwaitingReview {
        return Err(gateway_error(
            "precondition_failed",
            "awaiting_review requires the dedicated execution completion path",
        ));
    }
    if current_state == v1::IssueState::Blocked {
        return Err(issue_blocked_error());
    }
    if matches!(
        current_state,
        v1::IssueState::Done | v1::IssueState::Canceled
    ) {
        return Err(gateway_error(
            "precondition_failed",
            "terminal issues cannot be transitioned by a gateway worker",
        ));
    }
    Ok(())
}

fn gateway_request_digest<T: Message>(method: &str, request: &T) -> String {
    let mut digest = Sha256::new();
    digest.update(b"issue-gateway/v1\0");
    digest.update(method.as_bytes());
    digest.update(b"\0");
    digest.update(request.encode_to_vec());
    format!("sha256:{:x}", digest.finalize())
}
fn require_non_empty(value: &str, name: &str) -> Result<(), rpc_v1::IssueGatewayError> {
    if value.trim().is_empty() {
        Err(gateway_error(
            "invalid_argument",
            format!("{name} is required"),
        ))
    } else {
        Ok(())
    }
}
fn require_revision(value: Option<u64>) -> Result<u64, rpc_v1::IssueGatewayError> {
    value.ok_or_else(|| gateway_error("invalid_argument", "expected_revision is required"))
}
fn page_request(page_size: u32, page_token: Option<String>) -> manager_v1::PageRequest {
    manager_v1::PageRequest {
        page_size,
        page_token,
    }
}
fn mutation_response(
    issue_id: String,
    mutation: manager_v1::ManagerMutation,
) -> rpc_v1::IssueGatewayMutationResponse {
    let outcome = mutation
        .entity_revisions
        .into_iter()
        .find(|item| item.entity_id == issue_id)
        .map(|item| {
            rpc_v1::issue_gateway_mutation_response::Outcome::Success(
                rpc_v1::IssueGatewayMutation {
                    issue_id,
                    revision: item.revision,
                },
            )
        })
        .unwrap_or_else(|| {
            rpc_v1::issue_gateway_mutation_response::Outcome::Error(gateway_error(
                "unconfirmed",
                "mutation did not return an issue revision",
            ))
        });
    rpc_v1::IssueGatewayMutationResponse {
        outcome: Some(outcome),
    }
}

fn issue_revision(mutation: &manager_v1::ManagerMutation, issue_id: &str) -> Option<u64> {
    mutation
        .entity_revisions
        .iter()
        .find(|item| item.entity_id == issue_id)
        .map(|item| item.revision)
}
fn error_response(error: rpc_v1::IssueGatewayError) -> rpc_v1::IssueGatewayMutationResponse {
    rpc_v1::IssueGatewayMutationResponse {
        outcome: Some(rpc_v1::issue_gateway_mutation_response::Outcome::Error(
            error,
        )),
    }
}
fn capability_for(
    capabilities: &LeaseCapabilities,
    capability_id: &str,
    issue_id: &str,
) -> Result<LeaseCapability, rpc_v1::IssueGatewayError> {
    require_non_empty(capability_id, "capability_id")?;
    let capability = capabilities
        .get(capability_id)
        .ok_or_else(|| gateway_error("forbidden", "capability id is unknown or expired"))?;
    if capability.issue_id != issue_id {
        return Err(gateway_error(
            "forbidden",
            "capability does not belong to issue_id",
        ));
    }
    Ok(capability)
}

#[tonic::async_trait]
impl rpc_v1::issue_gateway_server::IssueGateway for GrpcServer {
    async fn list_ready(
        &self,
        request: Request<rpc_v1::IssueGatewayListReadyRequest>,
    ) -> Result<Response<rpc_v1::IssueGatewayListReadyResponse>, Status> {
        let request = request.into_inner();
        let store = match self.store_context().await {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(rpc_v1::IssueGatewayListReadyResponse {
                    outcome: Some(rpc_v1::issue_gateway_list_ready_response::Outcome::Error(
                        error,
                    )),
                }));
            }
        };
        let result = self
            .dispatch_manager_for_bound_gateway::<_, manager_v1::ListIssuesWithSyncResult>(
                "manager_gateway_list_ready",
                manager_v1::ListIssuesWithSyncArgs {
                    context: Some(read_context(&store)),
                    page: Some(page_request(request.page_size, request.page_token)),
                    ..Default::default()
                },
            )
            .await
            .map_err(Status::internal)?;
        let outcome = match result.outcome {
            Some(manager_v1::list_issues_with_sync_result::Outcome::Success(value)) => {
                rpc_v1::issue_gateway_list_ready_response::Outcome::Success(value)
            }
            Some(manager_v1::list_issues_with_sync_result::Outcome::Error(error)) => {
                rpc_v1::issue_gateway_list_ready_response::Outcome::Error(manager_gateway_error(
                    error,
                ))
            }
            None => rpc_v1::issue_gateway_list_ready_response::Outcome::Error(gateway_error(
                "unconfirmed",
                "ready list returned no outcome",
            )),
        };
        Ok(Response::new(rpc_v1::IssueGatewayListReadyResponse {
            outcome: Some(outcome),
        }))
    }

    async fn claim(
        &self,
        request: Request<rpc_v1::IssueGatewayClaimRequest>,
    ) -> Result<Response<rpc_v1::IssueGatewayClaimResponse>, Status> {
        let request = request.into_inner();
        if let Err(error) = require_non_empty(&request.idempotency_key, "idempotency_key") {
            return Ok(Response::new(rpc_v1::IssueGatewayClaimResponse {
                outcome: Some(rpc_v1::issue_gateway_claim_response::Outcome::Error(error)),
            }));
        }
        if let Err(error) = require_non_empty(&request.issue_id, "issue_id") {
            return Ok(Response::new(rpc_v1::IssueGatewayClaimResponse {
                outcome: Some(rpc_v1::issue_gateway_claim_response::Outcome::Error(error)),
            }));
        }
        if let Err(error) = require_revision(request.expected_revision) {
            return Ok(Response::new(rpc_v1::IssueGatewayClaimResponse {
                outcome: Some(rpc_v1::issue_gateway_claim_response::Outcome::Error(error)),
            }));
        }
        let replay = GatewayReplayContext::from_request("claim", &request, CLAIM_REPLAY_SCHEMA);
        match self
            .reserve_gateway_replay::<rpc_v1::IssueGatewayClaimed>(&replay)
            .await
        {
            Ok(Some(value)) => {
                return Ok(Response::new(rpc_v1::IssueGatewayClaimResponse {
                    outcome: Some(rpc_v1::issue_gateway_claim_response::Outcome::Success(
                        value,
                    )),
                }));
            }
            Ok(None) => {}
            Err(error) => {
                return Ok(Response::new(rpc_v1::IssueGatewayClaimResponse {
                    outcome: Some(rpc_v1::issue_gateway_claim_response::Outcome::Error(error)),
                }));
            }
        }
        let outcome = async {
            let expected_revision = require_revision(request.expected_revision)?;
            let store = self.store_context().await?;
            let result = self
                .dispatch_manager_for_bound_gateway::<_, manager_v1::ClaimIssueResult>(
                    "manager_gateway_claim_issue",
                    manager_v1::ClaimIssueArgs {
                        context: Some(gateway_issue_write_context(
                            &store,
                            "manager_gateway_claim_issue",
                            &request.issue_id,
                            &request.idempotency_key,
                        )),
                        issue_id: request.issue_id.clone(),
                        expected_revision: Some(expected_revision),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| gateway_error("unavailable", "gateway operation is unavailable"))?;
            match result.outcome {
                Some(manager_v1::claim_issue_result::Outcome::Success(mutation)) => {
                    match mutation.result {
                        Some(manager_v1::manager_mutation::Result::ClaimIssue(claim))
                            if !claim.lease_handle.is_empty() =>
                        {
                            let current = claim.current.as_ref().ok_or_else(|| {
                                gateway_error(
                                    "unconfirmed",
                                    "claim did not return the canonical issue",
                                )
                            })?;
                            let revision = current.revision;
                            let expires_at_millis = lease_expiry(current)?;
                            let capability_id = Uuid::new_v4().to_string();
                            self.capabilities().insert(
                                capability_id.clone(),
                                LeaseCapability {
                                    handle: claim.lease_handle,
                                    issue_id: request.issue_id.clone(),
                                    claim_revision: revision,
                                    expires_at_millis,
                                },
                            );
                            Ok(rpc_v1::IssueGatewayClaimed {
                                issue_id: request.issue_id,
                                capability_id,
                                revision,
                            })
                        }
                        _ => Err(gateway_error(
                            "unconfirmed",
                            "claim did not return a lease handle",
                        )),
                    }
                }
                Some(manager_v1::claim_issue_result::Outcome::Error(error)) => {
                    Err(manager_gateway_error(error))
                }
                None => Err(gateway_error("unconfirmed", "claim returned no outcome")),
            }
        }
        .await;
        let outcome = match outcome {
            Ok(value) => match self.complete_gateway_replay(&replay, &value).await {
                Ok(()) => rpc_v1::issue_gateway_claim_response::Outcome::Success(value),
                Err(error) => rpc_v1::issue_gateway_claim_response::Outcome::Error(error),
            },
            Err(error) => rpc_v1::issue_gateway_claim_response::Outcome::Error(
                self.gateway_replay_rejection(&replay, error).await,
            ),
        };
        Ok(Response::new(rpc_v1::IssueGatewayClaimResponse {
            outcome: Some(outcome),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<rpc_v1::IssueGatewayHeartbeatRequest>,
    ) -> Result<Response<rpc_v1::IssueGatewayMutationResponse>, Status> {
        let request = request.into_inner();
        if let Err(error) = require_non_empty(&request.idempotency_key, "idempotency_key") {
            return Ok(Response::new(error_response(error)));
        }
        if let Err(error) = require_non_empty(&request.issue_id, "issue_id") {
            return Ok(Response::new(error_response(error)));
        }
        if let Err(error) = require_revision(request.expected_revision) {
            return Ok(Response::new(error_response(error)));
        }
        let replay =
            GatewayReplayContext::from_request("heartbeat", &request, MUTATION_REPLAY_SCHEMA);
        match self
            .reserve_gateway_replay::<rpc_v1::IssueGatewayMutation>(&replay)
            .await
        {
            Ok(Some(value)) => {
                return Ok(Response::new(rpc_v1::IssueGatewayMutationResponse {
                    outcome: Some(rpc_v1::issue_gateway_mutation_response::Outcome::Success(
                        value,
                    )),
                }));
            }
            Ok(None) => {}
            Err(error) => return Ok(Response::new(error_response(error))),
        }
        let result = async {
            let expected_revision = require_revision(request.expected_revision)?;
            let capability = capability_for(
                self.capabilities(),
                &request.capability_id,
                &request.issue_id,
            )?;
            let store = self.store_context().await?;
            let result = self
                .dispatch_manager_for_bound_gateway::<_, manager_v1::HeartbeatIssueClaimResult>(
                    "manager_heartbeat_issue_claim",
                    manager_v1::HeartbeatIssueClaimArgs {
                        context: Some(gateway_issue_write_context(
                            &store,
                            "manager_heartbeat_issue_claim",
                            &request.issue_id,
                            &request.idempotency_key,
                        )),
                        issue_id: request.issue_id.clone(),
                        expected_revision: Some(expected_revision),
                        claim_revision: Some(capability.claim_revision),
                        lease_handle: capability.handle,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| gateway_error("unavailable", "gateway operation is unavailable"))?;
            match result.outcome {
                Some(manager_v1::heartbeat_issue_claim_result::Outcome::Success(mutation)) => {
                    Ok((mutation, store))
                }
                Some(manager_v1::heartbeat_issue_claim_result::Outcome::Error(error)) => {
                    Err(manager_gateway_error(error))
                }
                None => Err(gateway_error(
                    "unconfirmed",
                    "heartbeat returned no outcome",
                )),
            }
        }
        .await;
        Ok(Response::new(match result {
            Ok((mutation, store)) => {
                let expiry = match self.current_issue(&store, &request.issue_id).await {
                    Ok(issue) => match lease_expiry(&issue) {
                        Ok(expiry) => expiry,
                        Err(error) => return Ok(Response::new(error_response(error))),
                    },
                    Err(error) => return Ok(Response::new(error_response(error))),
                };
                let response = mutation_response(request.issue_id, mutation);
                if let Some(rpc_v1::issue_gateway_mutation_response::Outcome::Success(value)) =
                    response.outcome.as_ref()
                {
                    self.capabilities().update_lease(
                        &request.capability_id,
                        value.revision,
                        expiry,
                    );
                    if let Err(error) = self.complete_gateway_replay(&replay, value).await {
                        return Ok(Response::new(error_response(error)));
                    }
                }
                response
            }
            Err(error) => error_response(self.gateway_replay_rejection(&replay, error).await),
        }))
    }

    async fn release(
        &self,
        request: Request<rpc_v1::IssueGatewayReleaseRequest>,
    ) -> Result<Response<rpc_v1::IssueGatewayMutationResponse>, Status> {
        let request = request.into_inner();
        let result = async {
            require_non_empty(&request.issue_id, "issue_id")?;
            let capability = capability_for(
                self.capabilities(),
                &request.capability_id,
                &request.issue_id,
            )?;
            let store = self.store_context().await?;
            let revision = self
                .current_issue_revision(&store, &request.issue_id)
                .await?;
            let result = self
                .dispatch_manager_for_bound_gateway::<_, manager_v1::ReleaseIssueClaimResult>(
                    "manager_release_issue_claim",
                    manager_v1::ReleaseIssueClaimArgs {
                        context: Some(gateway_issue_write_context(
                            &store,
                            "manager_release_issue_claim",
                            &request.issue_id,
                            &Uuid::new_v4().to_string(),
                        )),
                        issue_id: request.issue_id.clone(),
                        expected_revision: Some(revision),
                        claim_revision: Some(capability.claim_revision),
                        lease_handle: capability.handle,
                    },
                )
                .await
                .map_err(|_| gateway_error("unavailable", "gateway operation is unavailable"))?;
            match result.outcome {
                Some(manager_v1::release_issue_claim_result::Outcome::Success(mutation)) => {
                    Ok(mutation)
                }
                Some(manager_v1::release_issue_claim_result::Outcome::Error(error)) => {
                    Err(manager_gateway_error(error))
                }
                None => Err(gateway_error("unconfirmed", "release returned no outcome")),
            }
        }
        .await;
        if result.is_ok() {
            self.capabilities().remove(&request.capability_id);
        }
        Ok(Response::new(match result {
            Ok(mutation) => mutation_response(request.issue_id, mutation),
            Err(error) => error_response(error),
        }))
    }

    async fn transition(
        &self,
        request: Request<rpc_v1::IssueGatewayTransitionRequest>,
    ) -> Result<Response<rpc_v1::IssueGatewayMutationResponse>, Status> {
        let request = request.into_inner();
        if let Err(error) = require_non_empty(&request.idempotency_key, "idempotency_key") {
            return Ok(Response::new(error_response(error)));
        }
        if let Err(error) = require_non_empty(&request.issue_id, "issue_id") {
            return Ok(Response::new(error_response(error)));
        }
        if let Err(error) = require_revision(request.expected_revision) {
            return Ok(Response::new(error_response(error)));
        }
        if let Err(error) = v1::IssueState::try_from(request.target_state)
            .map_err(|_| gateway_error("invalid_argument", "target_state is invalid"))
        {
            return Ok(Response::new(error_response(error)));
        }
        let replay =
            GatewayReplayContext::from_request("transition", &request, MUTATION_REPLAY_SCHEMA);
        match self
            .reserve_gateway_replay::<rpc_v1::IssueGatewayMutation>(&replay)
            .await
        {
            Ok(Some(value)) => {
                return Ok(Response::new(rpc_v1::IssueGatewayMutationResponse {
                    outcome: Some(rpc_v1::issue_gateway_mutation_response::Outcome::Success(
                        value,
                    )),
                }));
            }
            Ok(None) => {}
            Err(error) => return Ok(Response::new(error_response(error))),
        }
        let result = async {
            let expected_revision = require_revision(request.expected_revision)?;
            let capability = capability_for(
                self.capabilities(),
                &request.capability_id,
                &request.issue_id,
            )?;
            let target_state = v1::IssueState::try_from(request.target_state)
                .map_err(|_| gateway_error("invalid_argument", "target_state is invalid"))?;
            let store = self.store_context().await?;
            let current_issue = self.current_issue(&store, &request.issue_id).await?;
            let current_state = v1::IssueState::try_from(current_issue.state)
                .map_err(|_| gateway_error("precondition_failed", "issue state is invalid"))?;
            require_worker_transition_allowed(current_state, target_state)?;
            let terminal_report = match request.terminal_report {
                Some(rpc_v1::issue_gateway_transition_request::TerminalReport::Completion(
                    value,
                )) => Some(manager_v1::transition_issue_args::TerminalReport::Completion(value)),
                Some(rpc_v1::issue_gateway_transition_request::TerminalReport::Cancellation(
                    value,
                )) => Some(manager_v1::transition_issue_args::TerminalReport::Cancellation(value)),
                None => None,
            };
            let result = self
                .dispatch_manager_for_bound_gateway::<_, manager_v1::TransitionIssueResult>(
                    "manager_transition_issue",
                    manager_v1::TransitionIssueArgs {
                        context: Some(gateway_issue_write_context(
                            &store,
                            "manager_transition_issue",
                            &request.issue_id,
                            &request.idempotency_key,
                        )),
                        issue_id: request.issue_id.clone(),
                        expected_revision: Some(expected_revision),
                        target_state: target_state as i32,
                        lease_handle: Some(capability.handle),
                        claim_revision: Some(capability.claim_revision),
                        terminal_report,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| gateway_error("unavailable", "gateway operation is unavailable"))?;
            match result.outcome {
                Some(manager_v1::transition_issue_result::Outcome::Success(mutation)) => {
                    Ok(mutation)
                }
                Some(manager_v1::transition_issue_result::Outcome::Error(error)) => {
                    Err(manager_gateway_error(error))
                }
                None => Err(gateway_error(
                    "unconfirmed",
                    "transition returned no outcome",
                )),
            }
        }
        .await;
        Ok(Response::new(match result {
            Ok(mutation) => {
                if let Ok(target_state) = v1::IssueState::try_from(request.target_state) {
                    self.capabilities().complete_transition(
                        &request.capability_id,
                        target_state,
                        issue_revision(&mutation, &request.issue_id),
                    );
                }
                let response = mutation_response(request.issue_id, mutation);
                let completion_error =
                    if let Some(rpc_v1::issue_gateway_mutation_response::Outcome::Success(value)) =
                        response.outcome.as_ref()
                    {
                        self.complete_gateway_replay(&replay, value).await.err()
                    } else {
                        None
                    };
                if let Some(error) = completion_error {
                    return Ok(Response::new(error_response(error)));
                }
                response
            }
            Err(error) => error_response(self.gateway_replay_rejection(&replay, error).await),
        }))
    }

    async fn complete_execution_to_awaiting_review(
        &self,
        request: Request<rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest>,
    ) -> Result<Response<rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewResponse>, Status>
    {
        use rpc_v1::issue_gateway_complete_execution_to_awaiting_review_response::Outcome;
        let outcome = match self.awaiting_review_outcome(request.into_inner()).await {
            Ok(value) => Outcome::Success(value),
            Err(error) => Outcome::Error(error),
        };
        Ok(Response::new(
            rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewResponse {
                outcome: Some(outcome),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_revision_conflict_is_exposed_with_safe_cas_detail() {
        let error = manager_gateway_error(manager_v1::ManagerError {
            code: manager_v1::ManagerErrorCode::RevisionConflict as i32,
            message: "internal backend detail".into(),
            retryable: false,
            detail: Some(manager_v1::manager_error::Detail::RevisionConflict(
                manager_v1::RevisionConflict {
                    expected_revision: 4,
                    current_revision: 7,
                },
            )),
        });

        assert_eq!(error.code, "revision_conflict");
        assert!(matches!(
            error.detail,
            Some(rpc_v1::issue_gateway_error::Detail::RevisionConflict(detail))
                if detail.expected_revision == 4 && detail.current_revision == 7
        ));
        assert_ne!(error.message, "internal backend detail");
    }

    #[test]
    fn list_ready_reports_unavailable_when_gateway_has_no_registration() {
        let error = manager_gateway_error(manager_v1::ManagerError {
            code: manager_v1::ManagerErrorCode::BackendUnavailable as i32,
            message: "gateway registration storage is unavailable".into(),
            retryable: true,
            detail: None,
        });

        assert_eq!(error.code, "unavailable");
        assert_eq!(
            error.message,
            "gateway operation is unavailable: gateway registration storage is unavailable"
        );
    }

    #[test]
    fn claim_reports_policy_rejection_as_precondition_failed() {
        let error = manager_gateway_error(manager_v1::ManagerError {
            code: manager_v1::ManagerErrorCode::PreconditionFailed as i32,
            message: "issue is outside the gateway policy".into(),
            retryable: false,
            detail: None,
        });

        assert_eq!(error.code, "precondition_failed");
        assert_eq!(error.message, "operation precondition is not satisfied");
    }

    #[test]
    fn blocked_error_has_precondition_detail() {
        let error = issue_blocked_error();

        assert_eq!(error.code, "precondition_failed");
        assert!(matches!(
            error.detail,
            Some(rpc_v1::issue_gateway_error::Detail::Precondition(detail))
                if detail.precondition_kind
                    == rpc_v1::gateway_precondition_detail::PreconditionKind::IssueBlocked as i32
        ));
    }

    #[test]
    fn worker_transition_rejects_blocked_before_any_target_is_considered() {
        for target in [
            v1::IssueState::Open,
            v1::IssueState::InProgress,
            v1::IssueState::Done,
            v1::IssueState::Canceled,
            v1::IssueState::Blocked,
        ] {
            let error = require_worker_transition_allowed(v1::IssueState::Blocked, target)
                .expect_err("BLOCKED must reject every new worker transition");
            assert_eq!(error.code, "precondition_failed");
        }
        assert!(
            require_worker_transition_allowed(v1::IssueState::InProgress, v1::IssueState::Blocked)
                .is_ok()
        );
        assert_eq!(
            require_worker_transition_allowed(
                v1::IssueState::InProgress,
                v1::IssueState::AwaitingReview,
            )
            .unwrap_err()
            .code,
            "precondition_failed"
        );
        for current in [v1::IssueState::Done, v1::IssueState::Canceled] {
            let error = require_worker_transition_allowed(current, v1::IssueState::Open)
                .expect_err("worker must not reopen a terminal issue");
            assert_eq!(error.code, "precondition_failed");
        }
    }

    #[test]
    fn persisted_replay_returns_the_stored_response_for_matching_schema() {
        let result = rpc_v1::IssueGatewayClaimed {
            issue_id: "issue-1".into(),
            capability_id: "capability-1".into(),
            revision: 3,
        };

        assert_eq!(
            decode_gateway_replay::<rpc_v1::IssueGatewayClaimed>(
                GatewayReplayDecision::Replay {
                    response_schema: CLAIM_REPLAY_SCHEMA.into(),
                    response_snapshot: result.encode_to_vec(),
                },
                CLAIM_REPLAY_SCHEMA,
            )
            .expect("matching stored response must decode"),
            Some(result),
        );
    }

    #[test]
    fn replay_from_a_previous_epoch_is_unconfirmed() {
        let error = decode_gateway_replay::<rpc_v1::IssueGatewayMutation>(
            GatewayReplayDecision::Unconfirmed,
            MUTATION_REPLAY_SCHEMA,
        )
        .expect_err("a replay reservation outside the active epoch is not confirmed");

        assert_eq!(error.code, "unconfirmed");
    }

    #[test]
    fn replay_with_a_different_request_is_an_idempotency_conflict() {
        let error = decode_gateway_replay::<rpc_v1::IssueGatewayMutation>(
            GatewayReplayDecision::Conflict,
            MUTATION_REPLAY_SCHEMA,
        )
        .expect_err("a key cannot be reused for a different request");

        assert_eq!(error.code, "idempotency_conflict");
    }

    #[test]
    fn request_digest_distinguishes_optional_field_presence() {
        let absent = rpc_v1::IssueGatewayClaimRequest {
            issue_id: "issue-1".into(),
            expected_revision: None,
            idempotency_key: "key-1".into(),
        };
        let present = rpc_v1::IssueGatewayClaimRequest {
            expected_revision: Some(0),
            ..absent.clone()
        };

        assert_ne!(
            gateway_request_digest("claim", &absent),
            gateway_request_digest("claim", &present)
        );
    }

    #[test]
    fn awaiting_review_replay_identity_excludes_only_the_ephemeral_capability() {
        let mut request = rpc_v1::IssueGatewayCompleteExecutionToAwaitingReviewRequest {
            issue_id: "issue-1".into(),
            capability_id: "capability-one-time".into(),
            expected_revision: Some(0),
            idempotency_key: "run-1-complete".into(),
            state_contract_version: AWAITING_REVIEW_STATE_CONTRACT.into(),
            completion: Some(v1::AwaitingReviewCompletion {
                run_id: "run-1".into(),
                full_commit_id: "a".repeat(40),
                evidence_references: vec!["artifact://run-1/report.json".into()],
                ..Default::default()
            }),
        };
        let mut replay = request.clone();
        replay.capability_id.clear();
        let registration = manager_v1::GatewayRegistration {
            gateway_id: "gateway-a".into(),
            policy: Some(manager_v1::WorkEligibilityPolicy {
                mode: manager_v1::WorkEligibilityMode::ReviewedOnly as i32,
            }),
            policy_fingerprint: "policy-a".into(),
            registration_generation: 1,
        };

        assert_eq!(
            awaiting_review_request_digest(&mut request, "endpoint-a", &registration),
            awaiting_review_request_digest(&mut replay, "endpoint-a", &registration)
        );
        replay.completion.as_mut().unwrap().run_id = "run-2".into();
        assert_ne!(
            awaiting_review_request_digest(&mut request, "endpoint-a", &registration),
            awaiting_review_request_digest(&mut replay, "endpoint-a", &registration)
        );
        let mut changed_registration = registration.clone();
        changed_registration.registration_generation += 1;
        changed_registration.policy_fingerprint = "policy-b".into();
        assert_ne!(
            awaiting_review_request_digest(&mut request, "endpoint-a", &registration),
            awaiting_review_request_digest(&mut request, "endpoint-a", &changed_registration)
        );
        assert_ne!(
            awaiting_review_request_digest(&mut request, "endpoint-a", &registration),
            awaiting_review_request_digest(&mut request, "endpoint-b", &registration)
        );
    }

    #[test]
    fn replay_context_keeps_the_request_identity_together() {
        let request = rpc_v1::IssueGatewayClaimRequest {
            issue_id: "issue-1".into(),
            expected_revision: Some(3),
            idempotency_key: "key-1".into(),
        };

        let context = GatewayReplayContext::from_request("claim", &request, CLAIM_REPLAY_SCHEMA);

        assert_eq!(context.method, "claim");
        assert_eq!(context.idempotency_key, "key-1");
        assert_eq!(context.response_schema, CLAIM_REPLAY_SCHEMA);
        assert_eq!(
            context.request_digest,
            gateway_request_digest("claim", &request)
        );
    }

    #[test]
    fn deterministic_gateway_rejections_discard_only_pending_replay_reservations() {
        for code in [
            "invalid_argument",
            "forbidden",
            "precondition_failed",
            "revision_conflict",
            "not_found",
            "idempotency_conflict",
        ] {
            assert!(should_discard_gateway_replay_reservation(&gateway_error(
                code, "rejected"
            )));
        }
        for code in ["unavailable", "unconfirmed"] {
            assert!(!should_discard_gateway_replay_reservation(&gateway_error(
                code,
                "uncertain"
            )));
        }
    }

    #[test]
    fn gateway_issue_write_context_binds_the_issue_target_and_audit_actor() {
        let store = manager_v1::ManagerReadContext {
            correlation_id: "correlation-1".into(),
            schema_fingerprint: "schema-1".into(),
            data_root_id: "root-1".into(),
            contract_fingerprint: "contract-1".into(),
        };

        let context =
            gateway_issue_write_context(&store, "manager_gateway_claim_issue", "issue-1", "key-1");

        assert_eq!(context.method, "manager_gateway_claim_issue");
        assert_eq!(context.target, "issue:issue-1");
        assert_eq!(context.idempotency_key, "key-1");
        assert_eq!(context.correlation_id, store.correlation_id);
        assert!(matches!(
            context.actor_audit,
            Some(manager_v1::ActorAudit { actor_id, actor_kind, .. })
                if actor_id == EXTERNAL_ACTOR_ID
                    && actor_kind == v1::ActorKind::ExternalAgent as i32
        ));
    }

    #[test]
    fn write_operations_require_present_revision_and_non_empty_key() {
        assert_eq!(require_revision(None).unwrap_err().code, "invalid_argument");
        assert_eq!(
            require_non_empty("  ", "idempotency_key").unwrap_err().code,
            "invalid_argument"
        );
        assert_eq!(require_revision(Some(0)).unwrap(), 0);
    }
    #[test]
    fn transition_capability_must_exist_and_belong_to_issue() {
        let capabilities = LeaseCapabilities::default();
        assert_eq!(
            capability_for(&capabilities, "", "issue-1")
                .unwrap_err()
                .code,
            "invalid_argument"
        );
        capabilities.insert(
            "capability-1".into(),
            LeaseCapability {
                handle: vec![1],
                issue_id: "issue-1".into(),
                claim_revision: 2,
                expires_at_millis: now_millis() + 1_000,
            },
        );
        assert_eq!(
            capability_for(&capabilities, "capability-1", "issue-2")
                .unwrap_err()
                .code,
            "forbidden"
        );
    }

    #[test]
    fn terminal_transition_revokes_the_lease_capability() {
        let capabilities = LeaseCapabilities::default();
        let capability = LeaseCapability {
            handle: vec![1],
            issue_id: "issue-1".into(),
            claim_revision: 2,
            expires_at_millis: now_millis() + 1_000,
        };
        capabilities.insert("done".into(), capability.clone());
        capabilities.complete_transition("done", v1::IssueState::Done, Some(3));
        assert!(capabilities.get("done").is_none());
        assert_eq!(
            capability_for(&capabilities, "done", "issue-1")
                .unwrap_err()
                .code,
            "forbidden"
        );

        capabilities.insert("canceled".into(), capability.clone());
        capabilities.complete_transition("canceled", v1::IssueState::Canceled, Some(3));
        assert!(capabilities.get("canceled").is_none());
        assert_eq!(
            capability_for(&capabilities, "canceled", "issue-1")
                .unwrap_err()
                .code,
            "forbidden"
        );

        capabilities.insert("blocked".into(), capability);
        capabilities.complete_transition("blocked", v1::IssueState::Blocked, Some(3));
        assert_eq!(capabilities.get("blocked").unwrap().claim_revision, 3);
    }

    #[test]
    fn expired_capability_is_removed_before_it_can_authorize_a_write() {
        let capabilities = LeaseCapabilities::default();
        capabilities.insert(
            "expired".into(),
            LeaseCapability {
                handle: vec![1],
                issue_id: "issue-1".into(),
                claim_revision: 2,
                expires_at_millis: now_millis() - 1,
            },
        );

        assert_eq!(
            capability_for(&capabilities, "expired", "issue-1")
                .unwrap_err()
                .code,
            "forbidden"
        );
        assert!(capabilities.get("expired").is_none());
    }

    #[test]
    fn accessing_an_active_capability_prunes_unrelated_expired_capabilities() {
        let capabilities = LeaseCapabilities::default();
        capabilities.insert(
            "active".into(),
            LeaseCapability {
                handle: vec![2],
                issue_id: "issue-active".into(),
                claim_revision: 2,
                expires_at_millis: now_millis() + 1_000,
            },
        );
        capabilities.insert(
            "expired".into(),
            LeaseCapability {
                handle: vec![1],
                issue_id: "issue-expired".into(),
                claim_revision: 1,
                expires_at_millis: now_millis() - 1,
            },
        );

        assert!(capabilities.get("active").is_some());
        assert!(capabilities.get("expired").is_none());
    }

    #[test]
    fn updating_a_live_capability_prunes_unrelated_expired_capabilities() {
        let capabilities = LeaseCapabilities::default();
        capabilities.insert(
            "active".into(),
            LeaseCapability {
                handle: vec![2],
                issue_id: "issue-active".into(),
                claim_revision: 2,
                expires_at_millis: now_millis() + 1_000,
            },
        );
        capabilities.insert(
            "expired".into(),
            LeaseCapability {
                handle: vec![1],
                issue_id: "issue-expired".into(),
                claim_revision: 1,
                expires_at_millis: now_millis() - 1,
            },
        );

        capabilities.update_lease("active", 3, now_millis() + 1_000);

        assert_eq!(capabilities.get("active").unwrap().claim_revision, 3);
        assert!(capabilities.get("expired").is_none());
    }
}
