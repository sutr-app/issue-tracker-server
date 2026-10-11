//! Local-only tonic server composition and transport-independent application service.

use crate::manager::gateway::ManagerRepository as _;
#[cfg(test)]
use crate::manager::repository::OwnershipRecoveryState;
use crate::{
    app::AppModule,
    error::TrackerError,
    manager::{
        self, IssueTrackerManager,
        backend_adapter::{EntityRevisionExpectation, MarkerExpectation, verify_operation_marker},
        operations::{IdempotencyScope, NewOperation, OperationScope, digest_bytes},
        repository::GatewayReplayDecision,
    },
    proto_types::manager_v1,
    tracker::{
        Tracker,
        config::{RunnerConfig, contract_fingerprint},
        outbox_driver::OutboxDriver,
    },
    v1,
};
use prost::Message;
use serde::{Deserialize, Deserializer, de::Error as _};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs,
    future::Future,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, oneshot};
use tokio::time::Duration;
use tonic::{Request, Response, Status};

use crate::proto_types::rpc_v1;

const DEFAULT_RECOVERY_BATCH_SIZE: usize = 32;
const DEFAULT_DELIVERY_BATCH_SIZE: usize = 32;
const GATEWAY_READY_ORDER_FINGERPRINT: &str = "issue-id";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GatewayReadySnapshot {
    manager_generation: u64,
    tracker_generation: u64,
    eligibility_evaluated_at_millis: i64,
}

impl GatewayReadySnapshot {
    fn encode(self) -> String {
        format!(
            "gateway-ready-{}-{}-{}",
            self.manager_generation, self.tracker_generation, self.eligibility_evaluated_at_millis
        )
    }

    fn decode(snapshot_id: &str) -> Result<Self, TrackerError> {
        let mut parts = snapshot_id.split('-');
        let parsed = match (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) {
            (
                Some("gateway"),
                Some("ready"),
                Some(manager),
                Some(tracker),
                Some(evaluated_at),
                None,
            ) => (manager, tracker, evaluated_at),
            _ => return Err(TrackerError::InvalidPageToken),
        };
        Ok(Self {
            manager_generation: parsed
                .0
                .parse()
                .map_err(|_| TrackerError::InvalidPageToken)?,
            tracker_generation: parsed
                .1
                .parse()
                .map_err(|_| TrackerError::InvalidPageToken)?,
            eligibility_evaluated_at_millis: parsed
                .2
                .parse()
                .map_err(|_| TrackerError::InvalidPageToken)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GatewayReadyPageBinding {
    generation: u64,
    snapshot_id: String,
    query_fingerprint: String,
}

impl GatewayReadyPageBinding {
    fn new(
        generation: u64,
        tracker_generation: u64,
        selection: &manager::repository::GatewayReadySelection,
    ) -> Self {
        let registration = &selection.registration;
        let snapshot_id = GatewayReadySnapshot {
            manager_generation: generation,
            tracker_generation,
            eligibility_evaluated_at_millis: selection.eligibility_evaluated_at_millis,
        }
        .encode();
        let query_fingerprint = gateway_ready_query_fingerprint(
            &registration.gateway_id,
            &registration.policy_fingerprint,
            registration.registration_generation,
        );
        Self {
            generation,
            snapshot_id,
            query_fingerprint,
        }
    }
}

fn gateway_ready_query_fingerprint(
    gateway_id: &str,
    policy_fingerprint: &str,
    registration_generation: u64,
) -> String {
    let mut digest = Sha256::new();
    let registration_generation = registration_generation.to_string();
    for value in [
        gateway_id,
        policy_fingerprint,
        registration_generation.as_str(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    format!("gateway-ready:{:x}", digest.finalize())
}

fn decode_gateway_ready_page_token(
    codec: &manager::pagination::PageTokenCodec,
    token: &str,
) -> Result<manager::pagination::PageCursor, TrackerError> {
    let parts = token.split('|').collect::<Vec<_>>();
    if parts.len() != 8 {
        return Err(TrackerError::InvalidPageToken);
    }
    let token_generation = parts[2]
        .parse::<u64>()
        .map_err(|_| TrackerError::InvalidPageToken)?;
    // Authenticate the immutable cursor before using its evaluation time.
    codec.decode(
        token,
        token_generation,
        parts[3],
        GATEWAY_READY_ORDER_FINGERPRINT,
        parts[5],
        i64::MAX,
    )
}

fn gateway_ready_page_token_error(error: TrackerError) -> Vec<u8> {
    let (code, message) = match error {
        TrackerError::PageTokenExpired { .. } => (
            manager_v1::ManagerErrorCode::PageTokenExpired,
            "gateway ready page token has expired",
        ),
        _ => (
            manager_v1::ManagerErrorCode::InvalidPageToken,
            "gateway ready page token is invalid",
        ),
    };
    manager_v1::ListIssuesWithSyncResult {
        outcome: Some(manager_v1::list_issues_with_sync_result::Outcome::Error(
            manager_v1::ManagerError {
                code: code as i32,
                message: message.into(),
                retryable: false,
                detail: None,
            },
        )),
    }
    .encode_to_vec()
}

fn decode_gateway_ready_error<R>(error: TrackerError) -> Result<R, String>
where
    R: Message + Default,
{
    R::decode(gateway_ready_page_token_error(error).as_slice()).map_err(|error| error.to_string())
}

#[cfg(test)]
fn gateway_ready_page(
    args: &manager_v1::ListIssuesWithSyncArgs,
    selection: manager::repository::GatewayReadySelection,
    page_token_secret: String,
    gateway_id: &str,
    binding: GatewayReadyPageBinding,
) -> Vec<u8> {
    gateway_ready_page_with_limits(
        args,
        selection,
        page_token_secret,
        gateway_id,
        binding,
        manager::pagination::PageSizeLimits::new(50, 100),
    )
}

fn gateway_ready_page_with_limits(
    args: &manager_v1::ListIssuesWithSyncArgs,
    selection: manager::repository::GatewayReadySelection,
    page_token_secret: String,
    gateway_id: &str,
    binding: GatewayReadyPageBinding,
    page_size_limits: manager::pagination::PageSizeLimits,
) -> Vec<u8> {
    let codec = manager::pagination::PageTokenCodec::new(page_token_secret);
    let cursor = args
        .page
        .as_ref()
        .and_then(|page| page.page_token.as_deref())
        .map(|token| decode_gateway_ready_page_token(&codec, token))
        .transpose();
    let cursor = match cursor {
        Ok(cursor) => cursor,
        Err(error) => return gateway_ready_page_token_error(error),
    };
    if selection.registration.gateway_id != gateway_id {
        return gateway_ready_page_token_error(TrackerError::PageTokenExpired {
            token_generation: cursor
                .as_ref()
                .map_or(binding.generation, |cursor| cursor.generation),
            current_generation: binding.generation,
        });
    }
    if let Some(cursor) = cursor.as_ref()
        && (cursor.generation != binding.generation
            || cursor.snapshot_id != binding.snapshot_id
            || cursor.order_fingerprint != GATEWAY_READY_ORDER_FINGERPRINT
            || cursor.query_fingerprint != binding.query_fingerprint)
    {
        return gateway_ready_page_token_error(TrackerError::PageTokenExpired {
            token_generation: cursor.generation,
            current_generation: binding.generation,
        });
    }
    let requested_page_size = args.page.as_ref().map_or(0, |page| page.page_size);
    let page_size = page_size_limits.resolve(requested_page_size);
    let (start, end, has_more) =
        manager::pagination::paginate(&selection.issues, page_size, cursor.as_ref());
    let next_page_token = has_more.then(|| {
        codec.encode(&manager::pagination::PageCursor {
            offset: end,
            generation: binding.generation,
            snapshot_id: binding.snapshot_id.clone(),
            order_fingerprint: GATEWAY_READY_ORDER_FINGERPRINT.into(),
            query_fingerprint: binding.query_fingerprint.clone(),
            expires_at_unix_epoch_millis: i64::MAX,
        })
    });
    manager_v1::ListIssuesWithSyncResult {
        outcome: Some(manager_v1::list_issues_with_sync_result::Outcome::Success(
            manager_v1::IssueWithSyncPage {
                issues: selection.issues[start..end]
                    .iter()
                    .cloned()
                    .map(|issue| manager_v1::IssueWithSync {
                        issue: Some(issue),
                        lookback_ready: true,
                        ..Default::default()
                    })
                    .collect(),
                page: Some(manager_v1::PageInfo {
                    next_page_token,
                    total_size: Some(selection.issues.len() as u64),
                    read_revision: binding.generation,
                    snapshot: Some(manager_v1::PageSnapshot {
                        snapshot_id: binding.snapshot_id,
                        read_generation: binding.generation,
                        stable_traversal: Some(manager_v1::StableTraversal {
                            order_fingerprint: GATEWAY_READY_ORDER_FINGERPRINT.into(),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            },
        )),
    }
    .encode_to_vec()
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerSettings {
    #[serde(default)]
    pub server: ListenerSettings,
    #[serde(default)]
    pub gateway_endpoints: Vec<GatewayEndpointSettings>,
    #[serde(deserialize_with = "deserialize_tracker_settings")]
    pub tracker: v1::IssueTrackerRunnerSettings,
    #[serde(default)]
    pub maintenance: MaintenanceSettings,
}

/// The launch-file contract permits server-owned fingerprints to be omitted.
/// Protobuf's derived TOML decoder treats scalar fields as required, so add
/// the omitted values before it decodes the nested runner settings. The normal
/// validation path still rejects an explicitly supplied mismatched value.
fn deserialize_tracker_settings<'de, D>(
    deserializer: D,
) -> Result<v1::IssueTrackerRunnerSettings, D::Error>
where
    D: Deserializer<'de>,
{
    let mut value = toml::Value::deserialize(deserializer)?;
    let tracker = value
        .as_table_mut()
        .ok_or_else(|| D::Error::custom("tracker settings must be a TOML table"))?;
    tracker
        .entry("expected_contract_fingerprint")
        .or_insert_with(|| toml::Value::String(String::new()));
    tracker
        .entry("expected_storage_schema_fingerprint")
        .or_insert_with(|| toml::Value::String(String::new()));
    if let Some(manager) = tracker
        .get_mut("manager_settings")
        .and_then(toml::Value::as_table_mut)
    {
        manager
            .entry("expected_contract_fingerprint")
            .or_insert_with(|| toml::Value::String(String::new()));
        manager
            .entry("expected_schema_fingerprint")
            .or_insert_with(|| toml::Value::String(String::new()));
    }
    v1::IssueTrackerRunnerSettings::deserialize(value).map_err(D::Error::custom)
}

#[derive(Clone, Debug, Deserialize)]
pub struct GatewayEndpointSettings {
    pub gateway_id: String,
    pub control_listen: String,
    pub worker_listen: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedGatewayEndpoint {
    gateway_id: String,
    control_address: SocketAddr,
    worker_address: SocketAddr,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ListenerSettings {
    #[serde(default = "default_listen")]
    pub listen: String,
}

impl Default for ListenerSettings {
    fn default() -> Self {
        Self {
            listen: default_listen(),
        }
    }
}

fn default_listen() -> String {
    "127.0.0.1:50051".into()
}

#[derive(Clone, Debug, Deserialize)]
pub struct MaintenanceSettings {
    #[serde(default = "default_recovery_batch_size")]
    pub recovery_batch_size: usize,
    #[serde(default = "default_delivery_batch_size")]
    pub delivery_batch_size: usize,
}

impl Default for MaintenanceSettings {
    fn default() -> Self {
        Self {
            recovery_batch_size: DEFAULT_RECOVERY_BATCH_SIZE,
            delivery_batch_size: DEFAULT_DELIVERY_BATCH_SIZE,
        }
    }
}

fn default_recovery_batch_size() -> usize {
    DEFAULT_RECOVERY_BATCH_SIZE
}
fn default_delivery_batch_size() -> usize {
    DEFAULT_DELIVERY_BATCH_SIZE
}

impl ServerSettings {
    pub fn from_toml(source: &str) -> Result<Self, String> {
        let settings: Self = toml::from_str(source).map_err(|error| error.to_string())?;
        settings.validate()?;
        Ok(settings)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let source = fs::read_to_string(path).map_err(|error| error.to_string())?;
        Self::from_toml(&source)
    }

    pub fn listen_addr(&self) -> Result<SocketAddr, String> {
        parse_listener_addr("server.listen", &self.server.listen)
    }

    pub fn gateway_control_addr(
        &self,
        endpoint: &GatewayEndpointSettings,
    ) -> Result<SocketAddr, String> {
        parse_listener_addr("gateway control_listen", &endpoint.control_listen)
    }

    pub fn gateway_worker_addr(
        &self,
        endpoint: &GatewayEndpointSettings,
    ) -> Result<SocketAddr, String> {
        parse_listener_addr("gateway worker_listen", &endpoint.worker_listen)
    }

    fn resolved_gateway_endpoints(&self) -> Result<Vec<ResolvedGatewayEndpoint>, String> {
        self.gateway_endpoints
            .iter()
            .map(|endpoint| {
                Ok(ResolvedGatewayEndpoint {
                    gateway_id: endpoint.gateway_id.clone(),
                    control_address: self.gateway_control_addr(endpoint)?,
                    worker_address: self.gateway_worker_addr(endpoint)?,
                })
            })
            .collect()
    }

    fn validate(&self) -> Result<(), String> {
        let mut gateway_ids = HashSet::new();
        let mut listener_addresses = HashSet::from([self.listen_addr()?]);
        for endpoint in &self.gateway_endpoints {
            if endpoint.gateway_id.trim().is_empty() {
                return Err("gateway_id must not be empty".into());
            }
            if !gateway_ids.insert(endpoint.gateway_id.as_str()) {
                return Err(format!("duplicate gateway_id: {}", endpoint.gateway_id));
            }
            for address in [
                self.gateway_control_addr(endpoint)?,
                self.gateway_worker_addr(endpoint)?,
            ] {
                if !listener_addresses.insert(address) {
                    return Err(format!("duplicate listener address: {address}"));
                }
            }
        }
        if self.maintenance.recovery_batch_size == 0 || self.maintenance.delivery_batch_size == 0 {
            return Err("maintenance batch sizes must be positive".into());
        }
        Ok(())
    }
}

fn parse_listener_addr(field: &str, value: &str) -> Result<SocketAddr, String> {
    value
        .parse::<SocketAddr>()
        .map_err(|error| format!("{field} is not a valid socket address: {error}"))
}

pub(crate) struct IssueTrackerApplication {
    core: AppModule,
    pub(crate) manager: Option<Arc<Mutex<IssueTrackerManager>>>,
    driver: Option<Arc<OutboxDriver>>,
    maintenance: MaintenanceSettings,
    ownership_maintenance_interval_millis: u64,
    ownership_recovery_batch_size: usize,
}

pub(crate) struct OutboxCycle {
    pub recovered_operations: u32,
    pub delivery_attempts: u32,
    pub confirmed_operations: u32,
    pub delivery_limit_reached: bool,
    pub contract: manager_v1::ContractInfo,
}

/// Installable recovery entry point used by the static infrastructure layer.
/// The scheduler itself lives in `tracker::outbox_driver`; this thin public
/// façade keeps the trusted-consumer implementation in the application layer
/// without making the driver depend on this module directly.
pub async fn recover_pending_resolutions_from_resources(
    tracker: Tracker,
    config: crate::tracker::config::RunnerConfig,
    manager: Arc<Mutex<IssueTrackerManager>>,
    limit: usize,
) -> Result<u32, String> {
    IssueTrackerApplication::recover_pending_resolutions_from_resources(
        tracker, config, manager, limit,
    )
    .await
}

#[path = "server_application.rs"]
mod application_impl;
/// Only errors proved by Tracker validation before its commit boundary can
/// safely become a terminal "not applied" operation. All other errors may
/// have followed a durable Tracker mutation and require reconciliation.
fn admin_action_rejected_before_mutation(error: &TrackerError) -> bool {
    matches!(
        error,
        TrackerError::InvalidState(_)
            | TrackerError::InvalidStateTransition(_)
            | TrackerError::InvalidRelation(_)
            | TrackerError::NotFound(_)
            | TrackerError::RevisionConflict { .. }
            | TrackerError::InvalidLease { .. }
            | TrackerError::LeaseExpired { .. }
            | TrackerError::LeaseInconsistent { .. }
            | TrackerError::SecretRejected
            | TrackerError::Blocked { .. }
            | TrackerError::IdempotencyConflict
    )
}

fn admin_manager_error_code(error: &TrackerError) -> manager_v1::ManagerErrorCode {
    match error {
        TrackerError::NotFound(_) => manager_v1::ManagerErrorCode::IssueNotFound,
        TrackerError::RevisionConflict { .. } => manager_v1::ManagerErrorCode::RevisionConflict,
        TrackerError::IdempotencyConflict => manager_v1::ManagerErrorCode::IdempotencyConflict,
        TrackerError::InvalidStateTransition(_) => {
            manager_v1::ManagerErrorCode::InvalidStateTransition
        }
        TrackerError::InvalidState(_)
        | TrackerError::InvalidRelation(_)
        | TrackerError::InvalidLease { .. }
        | TrackerError::LeaseExpired { .. }
        | TrackerError::LeaseInconsistent { .. }
        | TrackerError::SecretRejected
        | TrackerError::Blocked { .. } => manager_v1::ManagerErrorCode::PreconditionFailed,
        _ => manager_v1::ManagerErrorCode::PreconditionFailed,
    }
}

fn admin_error_detail(
    code: manager_v1::ManagerErrorCode,
    message: &str,
) -> Option<manager_v1::manager_error::Detail> {
    admin_error_detail_with_attributes(code, message, &[], None, None, &[])
}

fn admin_error_detail_with_attributes(
    code: manager_v1::ManagerErrorCode,
    message: &str,
    invalid_attributes: &[String],
    observed_revision: Option<u64>,
    lease_expires_at: Option<manager_v1::UtcInstant>,
    blocker_issue_ids: &[String],
) -> Option<manager_v1::manager_error::Detail> {
    let lower = message.to_ascii_lowercase();
    let (diagnosis_class, lease_classification, repair_condition) = match code {
        manager_v1::ManagerErrorCode::PreconditionFailed if lower.contains("lease") => (
            if lower.contains("inconsistent") {
                "LEASE_INCONSISTENT"
            } else {
                "ACTIVE_LEASE"
            },
            Some(if lower.contains("inconsistent") {
                "INCONSISTENT"
            } else if lower.contains("lease has expired") || lower.contains("expired claim") {
                "EXPIRED"
            } else {
                "ACTIVE"
            }),
            "renew, release, or explicitly recover the ownership lease",
        ),
        manager_v1::ManagerErrorCode::PreconditionFailed
            if lower.contains("depend") || lower.contains("block") =>
        {
            (
                "DEPENDENCY_BLOCKED",
                None,
                "resolve the unfinished blocking dependency before terminating the issue",
            )
        }
        _ => return None,
    };
    let state = if diagnosis_class == "DEPENDENCY_BLOCKED" {
        "BLOCKED"
    } else {
        "OPEN"
    };
    Some(
        manager_v1::manager_error::Detail::OwnershipIntegrityDiagnosis(
            manager_v1::OwnershipIntegrityDiagnosis {
                diagnosis_class: diagnosis_class.into(),
                lease_classification: lease_classification.map(str::to_owned),
                repair_condition: repair_condition.into(),
                state: state.into(),
                invalid_attributes: invalid_attributes.to_vec(),
                observed_revision,
                lease_expires_at,
                open_blocker_issue_ids: blocker_issue_ids.to_vec(),
                ..Default::default()
            },
        ),
    )
}

fn lease_expiry_to_instant(seconds: u64) -> Option<manager_v1::UtcInstant> {
    let millis = seconds.saturating_mul(1_000);
    i64::try_from(millis)
        .ok()
        .map(|unix_epoch_millis| manager_v1::UtcInstant { unix_epoch_millis })
}

fn admin_admission_error_code(message: &str) -> manager_v1::ManagerErrorCode {
    let lower = message.to_ascii_lowercase();
    if lower.contains("idempotency") && lower.contains("conflict") {
        manager_v1::ManagerErrorCode::IdempotencyConflict
    } else if lower.contains("target") || lower.contains("scope") {
        manager_v1::ManagerErrorCode::MethodTargetMismatch
    } else if lower.contains("not found") {
        manager_v1::ManagerErrorCode::IssueNotFound
    } else {
        manager_v1::ManagerErrorCode::ResultUnconfirmed
    }
}

fn unix_now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

/// Build the non-secret source receipt used when a worker ownership
/// operation is confirmed by its Tracker marker during manual resolution.
/// The one-time Claim handle is deliberately absent; the marker is the only
/// source of the historical state and is preferred over the mutable Issue.
fn ownership_result_response_from_marker(
    operation: &manager_v1::DurableOperation,
    marker: &v1::OperationRecord,
) -> Result<Vec<u8>, String> {
    let marker_issue = marker
        .result_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.value.as_ref())
        .and_then(|value| match value {
            v1::operation_result_snapshot::Value::IssueMutation(mutation) => mutation
                .current
                .as_ref()
                .and_then(|details| details.issue.as_ref()),
            _ => None,
        });
    let issue = marker_issue
        .cloned()
        .ok_or_else(|| "ownership marker has no immutable Issue snapshot".to_owned())?;
    let result = manager_v1::IssueOwnershipResult {
        method: operation.method.clone(),
        issue_id: issue.issue_id.clone(),
        resulting_state: issue.state,
        resulting_revision: issue.revision,
        // The canonical Issue is a redacted, non-secret marker snapshot. It
        // is safe to retain and lets a manual-resolution replay return the
        // same ownership result without rereading mutable Tracker state.
        current: Some(issue.clone()),
        audit_operation_id: marker.downstream_operation_id.clone(),
    };
    let mut source_operation = operation.clone();
    // ConfirmApplied advances the source operation generation and settles its
    // resolution fence.  The response must describe that committed source,
    // rather than the pre-resolution UNCONFIRMED/MANUAL_RESOLUTION view that
    // was used while validating the marker.
    source_operation.state = manager_v1::DurableOperationState::Committed as i32;
    source_operation.failure_class = manager_v1::FailureClass::Unspecified as i32;
    source_operation.resolution_required = false;
    source_operation.generation = source_operation
        .generation
        .checked_add(1)
        .ok_or_else(|| "source operation generation overflow".to_owned())?;
    // The result wrapper is stored separately from the operation row. Avoid
    // recursively embedding a terminal snapshot in its own payload.
    source_operation.terminal_snapshot = None;
    let mutation = manager_v1::ManagerMutation {
        operation: Some(source_operation),
        result: Some(manager_v1::manager_mutation::Result::IssueOwnership(result)),
        entity_revisions: vec![manager_v1::ManagerEntityRevision {
            entity_kind: "issue".into(),
            entity_id: issue.issue_id.clone(),
            revision: issue.revision,
        }],
        ..Default::default()
    };
    manager::gateway::mutation_wrapper_for_method(&operation.method)
        .ok_or_else(|| {
            format!(
                "unsupported ownership operation method: {}",
                operation.method
            )
        })
        .map(|wrapper| wrapper(Ok(mutation)))
}

fn ownership_result_schema(method: &str) -> &'static str {
    crate::manager::methods::spec(method)
        .filter(|value| {
            value.category == crate::manager::methods::MethodCategory::OwnershipCapability
        })
        .map(|value| value.result_schema)
        .unwrap_or("issue_tracker.manager.v1.ManagerMutation")
}

fn is_ownership_operation_method(method: &str) -> bool {
    crate::manager::methods::is_ownership(method)
}

/// Only operations whose result can be reconstructed from a server-owned
/// Tracker marker (or from the explicit administrative request) are eligible
/// for evidence-confirmation resolution. Human arbitration deliberately uses
/// a separate mode and may settle any Issue-scoped operation without treating
/// a missing marker as proof of non-application.
fn is_resolvable_issue_operation_method(method: &str) -> bool {
    crate::manager::methods::spec(method).is_some_and(|value| {
        matches!(
            value.category,
            crate::manager::methods::MethodCategory::Administrative
                | crate::manager::methods::MethodCategory::OwnershipCapability
        )
    })
}

#[derive(Clone)]
pub struct GrpcServer {
    pub(crate) app: Arc<IssueTrackerApplication>,
    pub(crate) capabilities: crate::gateway::LeaseCapabilities,
    gateway_id: Option<Arc<str>>,
    #[cfg(test)]
    pub(crate) gateway_test_faults: Arc<crate::gateway::GatewayTestFaults>,
}

#[path = "server_grpc.rs"]
mod grpc_impl;
macro_rules! v1_rpc {
    ($name:ident, $key:literal, $args:ty, $result:ty) => {
        fn $name<'life0, 'async_trait>(
            &'life0 self,
            request: Request<$args>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<$result>, Status>> + Send + 'async_trait>>
        where
            Self: 'async_trait,
            'life0: 'async_trait,
        {
            Box::pin(async move { self.v1($key, request).await })
        }
    };
}

macro_rules! manager_rpc {
    ($name:ident, $key:literal, $args:ty, $result:ty) => {
        fn $name<'life0, 'async_trait>(
            &'life0 self,
            request: Request<$args>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<$result>, Status>> + Send + 'async_trait>>
        where
            Self: 'async_trait,
            'life0: 'async_trait,
        {
            Box::pin(async move { self.manager($key, request).await })
        }
    };
}

macro_rules! admin_rpc {
    () => {
        fn apply_issue_admin_action<'life0, 'async_trait>(
            &'life0 self,
            request: Request<manager_v1::ApplyIssueAdminActionArgs>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<Response<manager_v1::ApplyIssueAdminActionResult>, Status>,
                    > + Send
                    + 'async_trait,
            >,
        >
        where
            Self: 'async_trait,
            'life0: 'async_trait,
        {
            Box::pin(async move { self.apply_issue_admin_action(request).await })
        }
    };
}

macro_rules! issue_operation_resolution_rpc {
    () => {
        fn resolve_issue_operation<'life0, 'async_trait>(
            &'life0 self,
            request: Request<manager_v1::ResolveIssueOperationArgs>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<Response<manager_v1::ResolveIssueOperationResult>, Status>,
                    > + Send
                    + 'async_trait,
            >,
        >
        where
            Self: 'async_trait,
            'life0: 'async_trait,
        {
            Box::pin(async move { self.resolve_issue_operation(request).await })
        }
    };
}

macro_rules! impl_v1_service {
    ($($body:tt)*) => {
        impl rpc_v1::issue_tracker_service_server::IssueTrackerService for GrpcServer { $($body)* }
    };
}

impl_v1_service! {
    v1_rpc!(get_contract_info, "get_contract_info", v1::GetContractInfoArgs, v1::GetContractInfoResult);
    v1_rpc!(get_issue, "get_issue", v1::GetIssueArgs, v1::GetIssueResult);
    v1_rpc!(list_issues, "list_issues", v1::ListIssuesArgs, v1::ListIssuesResult);
    v1_rpc!(list_ready_issues, "list_ready_issues", v1::ListReadyIssuesArgs, v1::ListReadyIssuesResult);
    v1_rpc!(list_issue_comments, "list_issue_comments", v1::ListIssueCommentsArgs, v1::ListIssueCommentsResult);
    v1_rpc!(list_issue_history, "list_issue_history", v1::ListIssueHistoryArgs, v1::ListIssueHistoryResult);
    v1_rpc!(get_artifact, "get_artifact", v1::GetArtifactArgs, v1::GetArtifactResult);
    v1_rpc!(list_artifacts, "list_artifacts", v1::ListArtifactsArgs, v1::ListArtifactsResult);
    v1_rpc!(get_operation, "get_operation", v1::GetOperationArgs, v1::GetOperationResult);
    v1_rpc!(create_issue, "create_issue", v1::CreateIssueArgs, v1::CreateIssueResult);
    v1_rpc!(update_issue, "update_issue", v1::UpdateIssueArgs, v1::UpdateIssueResult);
    v1_rpc!(transition_issue, "transition_issue", v1::TransitionIssueArgs, v1::TransitionIssueResult);
    v1_rpc!(claim_issue, "claim_issue", v1::ClaimIssueArgs, v1::ClaimIssueResult);
    v1_rpc!(heartbeat_issue_claim, "heartbeat_issue_claim", v1::HeartbeatIssueClaimArgs, v1::HeartbeatIssueClaimResult);
    v1_rpc!(release_issue_claim, "release_issue_claim", v1::ReleaseIssueClaimArgs, v1::ReleaseIssueClaimResult);
    v1_rpc!(reconcile_expired_claim, "reconcile_expired_claim", v1::ReconcileExpiredClaimArgs, v1::ReconcileExpiredClaimResult);
    v1_rpc!(add_issue_relation, "add_issue_relation", v1::AddIssueRelationArgs, v1::AddIssueRelationResult);
    v1_rpc!(remove_issue_relation, "remove_issue_relation", v1::RemoveIssueRelationArgs, v1::RemoveIssueRelationResult);
    v1_rpc!(add_issue_comment, "add_issue_comment", v1::AddIssueCommentArgs, v1::AddIssueCommentResult);
    v1_rpc!(create_backup, "create_backup", v1::CreateBackupArgs, v1::CreateBackupResult);
    v1_rpc!(export_issues, "export_issues", v1::ExportIssuesArgs, v1::ExportIssuesResult);
}

macro_rules! impl_split_service {
    ($trait:path { $($body:tt)* }) => {
        impl $trait for GrpcServer { $($body)* }
    };
}

impl_split_service!(rpc_v1::tracker_info_service_server::TrackerInfoService {
    v1_rpc!(get_contract_info, "get_contract_info", v1::GetContractInfoArgs, v1::GetContractInfoResult);
});
impl_split_service!(rpc_v1::issue_service_server::IssueService {
    v1_rpc!(get_issue, "get_issue", v1::GetIssueArgs, v1::GetIssueResult);
    v1_rpc!(list_issues, "list_issues", v1::ListIssuesArgs, v1::ListIssuesResult);
    v1_rpc!(list_ready_issues, "list_ready_issues", v1::ListReadyIssuesArgs, v1::ListReadyIssuesResult);
    v1_rpc!(create_issue, "create_issue", v1::CreateIssueArgs, v1::CreateIssueResult);
    v1_rpc!(update_issue, "update_issue", v1::UpdateIssueArgs, v1::UpdateIssueResult);
    v1_rpc!(transition_issue, "transition_issue", v1::TransitionIssueArgs, v1::TransitionIssueResult);
});
impl_split_service!(rpc_v1::claim_service_server::ClaimService {
    v1_rpc!(claim_issue, "claim_issue", v1::ClaimIssueArgs, v1::ClaimIssueResult);
    v1_rpc!(heartbeat_issue_claim, "heartbeat_issue_claim", v1::HeartbeatIssueClaimArgs, v1::HeartbeatIssueClaimResult);
    v1_rpc!(release_issue_claim, "release_issue_claim", v1::ReleaseIssueClaimArgs, v1::ReleaseIssueClaimResult);
    v1_rpc!(reconcile_expired_claim, "reconcile_expired_claim", v1::ReconcileExpiredClaimArgs, v1::ReconcileExpiredClaimResult);
});
impl_split_service!(rpc_v1::relation_service_server::RelationService {
    v1_rpc!(add_issue_relation, "add_issue_relation", v1::AddIssueRelationArgs, v1::AddIssueRelationResult);
    v1_rpc!(remove_issue_relation, "remove_issue_relation", v1::RemoveIssueRelationArgs, v1::RemoveIssueRelationResult);
});
impl_split_service!(rpc_v1::activity_service_server::ActivityService {
    v1_rpc!(list_issue_comments, "list_issue_comments", v1::ListIssueCommentsArgs, v1::ListIssueCommentsResult);
    v1_rpc!(list_issue_history, "list_issue_history", v1::ListIssueHistoryArgs, v1::ListIssueHistoryResult);
    v1_rpc!(add_issue_comment, "add_issue_comment", v1::AddIssueCommentArgs, v1::AddIssueCommentResult);
});
impl_split_service!(rpc_v1::artifact_service_server::ArtifactService {
    v1_rpc!(get_artifact, "get_artifact", v1::GetArtifactArgs, v1::GetArtifactResult);
    v1_rpc!(list_artifacts, "list_artifacts", v1::ListArtifactsArgs, v1::ListArtifactsResult);
    v1_rpc!(create_backup, "create_backup", v1::CreateBackupArgs, v1::CreateBackupResult);
    v1_rpc!(export_issues, "export_issues", v1::ExportIssuesArgs, v1::ExportIssuesResult);
});
impl_split_service!(rpc_v1::operation_service_server::OperationService {
    v1_rpc!(get_operation, "get_operation", v1::GetOperationArgs, v1::GetOperationResult);
});

impl_split_service!(rpc_v1::manager_info_service_server::ManagerInfoService {
    manager_rpc!(get_contract_info, "manager_get_contract_info", manager_v1::GetContractInfoArgs, manager_v1::GetContractInfoResult);
});
impl_split_service!(rpc_v1::candidate_service_server::CandidateService {
    manager_rpc!(create_candidate, "manager_create_candidate", manager_v1::CreateCandidateArgs, manager_v1::CreateCandidateResult);
    manager_rpc!(get_candidate, "manager_get_candidate", manager_v1::GetCandidateArgs, manager_v1::GetCandidateResult);
    manager_rpc!(list_candidates, "manager_list_candidates", manager_v1::ListCandidatesArgs, manager_v1::ListCandidatesResult);
    manager_rpc!(update_candidate, "manager_update_candidate", manager_v1::UpdateCandidateArgs, manager_v1::UpdateCandidateResult);
    manager_rpc!(resolve_candidate, "manager_resolve_candidate", manager_v1::ResolveCandidateArgs, manager_v1::ResolveCandidateResult);
});
impl_split_service!(rpc_v1::extraction_run_service_server::ExtractionRunService {
    manager_rpc!(create_extraction_run, "manager_create_extraction_run", manager_v1::CreateExtractionRunArgs, manager_v1::CreateExtractionRunResult);
    manager_rpc!(get_extraction_run, "manager_get_extraction_run", manager_v1::GetExtractionRunArgs, manager_v1::GetExtractionRunResult);
    manager_rpc!(list_extraction_runs, "manager_list_extraction_runs", manager_v1::ListExtractionRunsArgs, manager_v1::ListExtractionRunsResult);
    manager_rpc!(advance_extraction_run, "manager_advance_extraction_run", manager_v1::AdvanceExtractionRunArgs, manager_v1::AdvanceExtractionRunResult);
    manager_rpc!(resume_extraction_run, "manager_resume_extraction_run", manager_v1::ResumeExtractionRunArgs, manager_v1::ResumeExtractionRunResult);
});
impl_split_service!(rpc_v1::manager_operation_service_server::ManagerOperationService {
    manager_rpc!(get_operation, "manager_get_operation", manager_v1::GetOperationArgs, manager_v1::GetOperationResult);
    manager_rpc!(list_operation_history, "manager_list_operation_history", manager_v1::ListOperationHistoryArgs, manager_v1::ListOperationHistoryResult);
    manager_rpc!(retry_operation, "manager_retry_operation", manager_v1::RetryOperationArgs, manager_v1::RetryOperationResult);
    manager_rpc!(reconcile_operation, "manager_reconcile_operation", manager_v1::ReconcileOperationArgs, manager_v1::ReconcileOperationResult);
});
impl_split_service!(rpc_v1::manager_consumer_service_server::ManagerConsumerService {
    manager_rpc!(resolve_candidate, "manager_resolve_candidate", manager_v1::ResolveCandidateArgs, manager_v1::ResolveCandidateResult);
    manager_rpc!(get_operation, "manager_get_operation", manager_v1::GetOperationArgs, manager_v1::GetOperationResult);
    manager_rpc!(list_operation_history, "manager_list_operation_history", manager_v1::ListOperationHistoryArgs, manager_v1::ListOperationHistoryResult);
    manager_rpc!(list_ownership_diagnostics, "manager_list_ownership_diagnostics", manager_v1::ListOwnershipDiagnosticsArgs, manager_v1::ListOwnershipDiagnosticsResult);
    manager_rpc!(get_issue_with_sync, "manager_get_issue_with_sync", manager_v1::GetIssueWithSyncArgs, manager_v1::GetIssueWithSyncResult);
    manager_rpc!(get_issue_relations, "manager_get_issue_relations", manager_v1::GetIssueRelationsArgs, manager_v1::GetIssueRelationsResult);
    manager_rpc!(list_issues_with_sync, "manager_list_issues_with_sync", manager_v1::ListIssuesWithSyncArgs, manager_v1::ListIssuesWithSyncResult);
    manager_rpc!(get_candidate, "manager_get_candidate", manager_v1::GetCandidateArgs, manager_v1::GetCandidateResult);
    manager_rpc!(list_candidates, "manager_list_candidates", manager_v1::ListCandidatesArgs, manager_v1::ListCandidatesResult);
    manager_rpc!(list_review_candidates, "manager_list_review_candidates", manager_v1::ListReviewCandidatesArgs, manager_v1::ListReviewCandidatesResult);
    manager_rpc!(get_extraction_run, "manager_get_extraction_run", manager_v1::GetExtractionRunArgs, manager_v1::GetExtractionRunResult);
    manager_rpc!(list_extraction_runs, "manager_list_extraction_runs", manager_v1::ListExtractionRunsArgs, manager_v1::ListExtractionRunsResult);
    manager_rpc!(create_issue, "manager_create_issue", manager_v1::CreateIssueArgs, manager_v1::CreateIssueResult);
    manager_rpc!(update_issue, "manager_update_issue", manager_v1::UpdateIssueArgs, manager_v1::UpdateIssueResult);
    manager_rpc!(transition_issue, "manager_transition_issue", manager_v1::TransitionIssueArgs, manager_v1::TransitionIssueResult);
    manager_rpc!(update_candidate, "manager_update_candidate", manager_v1::UpdateCandidateArgs, manager_v1::UpdateCandidateResult);
    manager_rpc!(add_issue_relation, "manager_add_issue_relation", manager_v1::AddIssueRelationArgs, manager_v1::AddIssueRelationResult);
    admin_rpc!();
    issue_operation_resolution_rpc!();
});
#[async_trait::async_trait]
impl rpc_v1::gateway_control_service_server::GatewayControlService for GrpcServer {
    async fn register_gateway(
        &self,
        request: Request<manager_v1::RegisterGatewayArgs>,
    ) -> Result<Response<manager_v1::RegisterGatewayResult>, Status> {
        self.gateway_control("manager_register_gateway", request)
            .await
    }

    async fn get_gateway_registration(
        &self,
        request: Request<manager_v1::GetGatewayRegistrationArgs>,
    ) -> Result<Response<manager_v1::GetGatewayRegistrationResult>, Status> {
        self.gateway_control("manager_get_gateway_registration", request)
            .await
    }
}
impl_split_service!(rpc_v1::canonical_issue_service_server::CanonicalIssueService {
    manager_rpc!(get_issue_with_sync, "manager_get_issue_with_sync", manager_v1::GetIssueWithSyncArgs, manager_v1::GetIssueWithSyncResult);
    manager_rpc!(list_issues_with_sync, "manager_list_issues_with_sync", manager_v1::ListIssuesWithSyncArgs, manager_v1::ListIssuesWithSyncResult);
    manager_rpc!(create_issue, "manager_create_issue", manager_v1::CreateIssueArgs, manager_v1::CreateIssueResult);
    manager_rpc!(update_issue, "manager_update_issue", manager_v1::UpdateIssueArgs, manager_v1::UpdateIssueResult);
    manager_rpc!(transition_issue, "manager_transition_issue", manager_v1::TransitionIssueArgs, manager_v1::TransitionIssueResult);
    manager_rpc!(claim_issue, "manager_claim_issue", manager_v1::ClaimIssueArgs, manager_v1::ClaimIssueResult);
    manager_rpc!(heartbeat_issue_claim, "manager_heartbeat_issue_claim", manager_v1::HeartbeatIssueClaimArgs, manager_v1::HeartbeatIssueClaimResult);
    manager_rpc!(release_issue_claim, "manager_release_issue_claim", manager_v1::ReleaseIssueClaimArgs, manager_v1::ReleaseIssueClaimResult);
    manager_rpc!(reconcile_expired_claim, "manager_reconcile_expired_claim", manager_v1::ReconcileExpiredClaimArgs, manager_v1::ReconcileExpiredClaimResult);
    manager_rpc!(add_issue_relation, "manager_add_issue_relation", manager_v1::AddIssueRelationArgs, manager_v1::AddIssueRelationResult);
    manager_rpc!(remove_issue_relation, "manager_remove_issue_relation", manager_v1::RemoveIssueRelationArgs, manager_v1::RemoveIssueRelationResult);
    manager_rpc!(add_issue_comment, "manager_add_issue_comment", manager_v1::AddIssueCommentArgs, manager_v1::AddIssueCommentResult);
});

#[async_trait::async_trait]
impl rpc_v1::issue_tracker_maintenance_service_server::IssueTrackerMaintenanceService
    for GrpcServer
{
    async fn run_outbox_cycle(
        &self,
        _request: Request<rpc_v1::RunOutboxCycleArgs>,
    ) -> Result<Response<rpc_v1::RunOutboxCycleResult>, Status> {
        let outcome = match self.app.run_outbox_cycle().await {
            Ok(cycle) => {
                rpc_v1::run_outbox_cycle_result::Outcome::Success(rpc_v1::RunOutboxCycleSuccess {
                    recovered_operations: cycle.recovered_operations,
                    delivery_attempts: cycle.delivery_attempts,
                    confirmed_operations: cycle.confirmed_operations,
                    delivery_limit_reached: cycle.delivery_limit_reached,
                    maintenance_phases: cycle.contract.maintenance_phases.clone(),
                    contract: Some(cycle.contract),
                })
            }
            Err(message) => {
                rpc_v1::run_outbox_cycle_result::Outcome::Error(manager_v1::ManagerError {
                    code: manager_v1::ManagerErrorCode::BackendUnavailable as i32,
                    message,
                    retryable: true,
                    detail: None,
                })
            }
        };
        Ok(Response::new(rpc_v1::RunOutboxCycleResult {
            outcome: Some(outcome),
        }))
    }
}

#[async_trait::async_trait]
impl rpc_v1::outbox_maintenance_service_server::OutboxMaintenanceService for GrpcServer {
    async fn run_outbox_cycle(
        &self,
        request: Request<rpc_v1::RunOutboxCycleArgs>,
    ) -> Result<Response<rpc_v1::RunOutboxCycleResult>, Status> {
        <Self as rpc_v1::issue_tracker_maintenance_service_server::IssueTrackerMaintenanceService>::run_outbox_cycle(self, request).await
    }
}

pub async fn serve(settings: ServerSettings) -> Result<(), String> {
    settings.validate()?;
    let address = settings.listen_addr()?;
    let gateway_endpoints = settings.resolved_gateway_endpoints()?;
    let server = GrpcServer::new(IssueTrackerApplication::open(settings).await?);
    let mut gateway_tasks = tokio::task::JoinSet::new();
    for endpoint in gateway_endpoints {
        let ResolvedGatewayEndpoint {
            gateway_id,
            control_address,
            worker_address,
        } = endpoint;
        let control_server = server.for_gateway(Arc::<str>::from(gateway_id.clone()));
        let worker_server = server
            .for_gateway_worker(Arc::<str>::from(gateway_id))
            .await?;
        gateway_tasks
            .spawn(async move { serve_gateway_control(control_address, control_server).await });
        gateway_tasks
            .spawn(async move { serve_gateway_worker(worker_address, worker_server).await });
    }

    // Keep ownership maintenance resident independently from request
    // handling.  Its task is joined on every server exit so no Tracker or
    // Manager root lock can outlive the server lifecycle.
    let (maintenance_stop, mut maintenance_task) =
        spawn_server_resident_maintenance(server.clone());
    let primary = serve_primary(address, server.clone());
    tokio::pin!(primary);
    let mut maintenance_join_result: Option<Result<(), String>> = None;
    let result = tokio::select! {
        result = &mut primary => result,
        result = gateway_tasks.join_next(), if !gateway_tasks.is_empty() => match result {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(format!("gateway endpoint task failed: {error}")),
            None => Err("gateway endpoint tasks ended unexpectedly".into()),
        },
        result = &mut maintenance_task => {
            maintenance_join_result = Some(maintenance_task_join_result(result));
            Err("resident maintenance task stopped unexpectedly".into())
        },
    };
    let _ = maintenance_stop.send(());
    if maintenance_join_result.is_none() {
        maintenance_join_result = Some(maintenance_task_join_result(maintenance_task.await));
    }
    if let Some(Err(error)) = maintenance_join_result {
        server.app.mark_maintenance_unhealthy().await;
        return Err(format!("resident maintenance task failed: {error}"));
    }
    result
}

fn maintenance_task_join_result(result: Result<(), tokio::task::JoinError>) -> Result<(), String> {
    result.map_err(|error| error.to_string())
}

async fn serve_gateway_control(address: SocketAddr, server: GrpcServer) -> Result<(), String> {
    serve_routes(address, gateway_control_routes(server)).await
}

async fn serve_gateway_worker(address: SocketAddr, server: GrpcServer) -> Result<(), String> {
    serve_routes(address, gateway_worker_routes(server)?).await
}

fn spawn_server_resident_maintenance(
    server: GrpcServer,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let maintenance_interval =
        Duration::from_millis(server.app.ownership_maintenance_interval_millis);
    let (maintenance_stop, mut maintenance_stop_rx) = oneshot::channel();
    let maintenance_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(maintenance_interval);
        loop {
            tokio::select! {
                _ = &mut maintenance_stop_rx => break,
                _ = interval.tick() => {
                    let _ = server.app.run_outbox_cycle().await;
                }
            }
        }
    });
    (maintenance_stop, maintenance_task)
}

async fn serve_primary(address: SocketAddr, server: GrpcServer) -> Result<(), String> {
    serve_routes(address, primary_routes(server)).await
}

async fn serve_routes(address: SocketAddr, routes: tonic::service::Routes) -> Result<(), String> {
    tonic::transport::Server::builder()
        .add_routes(routes)
        .serve_with_shutdown(address, shutdown_signal())
        .await
        .map_err(|error| error.to_string())
}

fn gateway_control_routes(server: GrpcServer) -> tonic::service::Routes {
    let mut routes = tonic::service::Routes::builder();
    routes.add_service(
        rpc_v1::gateway_control_service_server::GatewayControlServiceServer::new(server),
    );
    routes.routes()
}

fn primary_routes(server: GrpcServer) -> tonic::service::Routes {
    let reflection_v1 = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            issue_tracker_proto::PRIMARY_TRUSTED_CONSUMER_FILE_DESCRIPTOR_SET,
        )
        .build_v1()
        .expect("primary trusted-consumer descriptor must be valid");
    let mut routes = tonic::service::Routes::builder();
    if server.app.manager.is_some() {
        routes
            .add_service(
                rpc_v1::manager_consumer_service_server::ManagerConsumerServiceServer::new(
                    server.clone(),
                ),
            )
            .add_service(
                rpc_v1::manager_trusted_read_service_server::ManagerTrustedReadServiceServer::new(
                    server.clone(),
                ),
            )
            .add_service(rpc_v1::issue_extract_server::IssueExtractServer::new(
                server.clone(),
            ))
            .add_service(
                rpc_v1::manager_info_service_server::ManagerInfoServiceServer::new(server.clone()),
            )
            .add_service(
                rpc_v1::outbox_maintenance_service_server::OutboxMaintenanceServiceServer::new(
                    server,
                ),
            )
            .add_service(reflection_v1);
    }
    routes.routes()
}

fn gateway_worker_service(
    server: GrpcServer,
) -> rpc_v1::issue_gateway_server::IssueGatewayServer<GrpcServer> {
    rpc_v1::issue_gateway_server::IssueGatewayServer::new(server)
}

/// Worker endpoints intentionally publish only their own descriptor closure.
/// Reflection uses the current v1 protocol and must not expose primary-consumer services.
fn gateway_worker_routes(server: GrpcServer) -> Result<tonic::service::Routes, String> {
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            issue_tracker_proto::GATEWAY_WORKER_FILE_DESCRIPTOR_SET,
        )
        .build_v1()
        .map_err(|error| format!("build gateway worker reflection service: {error}"))?;
    let mut routes = tonic::service::Routes::builder();
    routes
        .add_service(gateway_worker_service(server))
        .add_service(reflection);
    Ok(routes.routes())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
pub(crate) fn validate_runner_settings(
    settings: &v1::IssueTrackerRunnerSettings,
) -> Result<RunnerConfig, String> {
    // Keep the server's strict entry-point contract while delegating all
    // structural validation and runtime construction to RunnerConfig.
    if settings.data_root.is_empty() {
        return Err("missing tracker.data_root".into());
    }
    if settings.expected_contract_fingerprint != contract_fingerprint() {
        return Err("contract fingerprint mismatch".into());
    }
    if settings.expected_storage_schema_fingerprint != Tracker::schema_fingerprint() {
        return Err("storage schema fingerprint mismatch".into());
    }
    RunnerConfig::from_settings(settings)
}

fn normalize_runner_settings(settings: &mut v1::IssueTrackerRunnerSettings) -> Result<(), String> {
    if settings.expected_contract_fingerprint.is_empty() {
        settings.expected_contract_fingerprint = contract_fingerprint();
    }
    if settings.expected_storage_schema_fingerprint.is_empty() {
        settings.expected_storage_schema_fingerprint = Tracker::schema_fingerprint();
    }
    // The plugin accepted manager-enabled configurations that relied on these
    // runner defaults. Keep that migration path while standalone tracker
    // settings remain fully validated below.
    if settings.manager_settings.is_some() {
        if settings.lease_policy.is_none() {
            settings.lease_policy = Some(v1::LeasePolicy {
                default_lease: Some(v1::TimeSpan { millis: 30_000 }),
                minimum_lease: Some(v1::TimeSpan { millis: 10_000 }),
                maximum_lease: Some(v1::TimeSpan { millis: 60_000 }),
                maximum_heartbeat_interval: Some(v1::TimeSpan { millis: 1_000 }),
            });
        }
        if settings.pagination_policy.is_none() {
            settings.pagination_policy = Some(v1::PaginationPolicy {
                default_page_size: 50,
                maximum_page_size: 100,
            });
        }
        if settings.secret_filter_mode == v1::SecretFilterMode::Unspecified as i32 {
            settings.secret_filter_mode = v1::SecretFilterMode::FailClosed as i32;
        }
        if settings.resource_limits.is_none() {
            settings.resource_limits = Some(v1::ResourceLimits {
                max_string_bytes: 4096,
                max_body_bytes: 65_536,
                max_uri_bytes: 4096,
                max_token_bytes: 4096,
                max_handle_bytes: 4096,
                max_repeated_item_count: 100,
                max_export_bytes: 1_048_576,
                max_export_item_count: 10_000,
                max_artifact_bytes: 1_048_576,
                max_artifact_count: 100,
                max_total_count_scan_items: 10_000,
            });
        }
    }
    Ok(())
}

fn union_source_evidence_refs(
    primary: &[v1::SourceEvidenceRef],
    secondary: &[v1::SourceEvidenceRef],
    limits: manager::SourceEvidenceLimits,
) -> Result<Vec<v1::SourceEvidenceRef>, String> {
    manager::candidates::union_source_evidence(primary, secondary, limits)
        .map_err(|error| format!("source evidence is invalid: {error:?}"))
}

fn resolved_confirmation_source_evidence(
    confirm: &manager_v1::ConfirmAssociatedIssueResolution,
    existing: &[v1::SourceEvidenceRef],
    candidate: &[v1::SourceEvidenceRef],
    limits: manager::SourceEvidenceLimits,
) -> Result<Vec<v1::SourceEvidenceRef>, String> {
    let primary = if confirm
        .update_mask
        .contains(&(manager_v1::ManagerIssueField::SourceEvidence as i32))
    {
        confirm
            .patch
            .as_ref()
            .map_or(&[][..], |patch| patch.source_evidence.as_slice())
    } else {
        existing
    };
    union_source_evidence_refs(primary, candidate, limits)
}
