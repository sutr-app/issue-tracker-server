//! Manager-only domain operations.
//!
//! The primary service owns lifecycle and transport publication, so these
//! independently tested building blocks remain internal to this crate.
#![allow(
    clippy::clone_on_copy,
    clippy::collapsible_if,
    clippy::needless_late_init
)]

use prost::Message;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs,
    path::Path,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
};

pub(crate) mod backend_adapter;
pub(crate) mod backend_dispatch;
pub(crate) mod candidates;
pub(crate) mod canonical_service;
mod config;
pub(crate) mod error_mapping;
#[allow(dead_code)]
pub(crate) mod extract_apply;
#[allow(dead_code)]
pub(crate) mod extract_eval;
#[allow(dead_code)]
pub(crate) mod extract_persist;
pub(crate) mod extraction;
pub(crate) mod gateway;
pub(crate) mod issue_mapping;
pub(crate) mod methods;
mod model;
#[allow(dead_code)]
pub(crate) mod operations;
pub(crate) mod outbox;
pub(crate) mod pagination;
pub(crate) mod read_session;
pub(crate) mod reconcile;
pub(crate) mod repository;
pub(crate) mod review_candidates;
pub(crate) mod source_labels;
mod storage;
pub(crate) mod sync;
pub(crate) mod trusted_read;
pub(crate) mod validation;

pub(crate) use config::SourceEvidenceLimits;
pub use config::{ManagerConfig, ManagerConfigError};
pub use storage::MANAGER_STORAGE_SCHEMA_FINGERPRINT;
pub(crate) use storage::{ManagerStorage, ManagerStorageError};

// v14 adds immutable pre-resolution audit snapshots and typed evidence facts
// to the human-arbitration result contract. It intentionally requires a fresh
// Manager root rather than decoding older snapshots under the expanded public
// result contract.
pub const MANAGER_CONTRACT_VERSION: &str = "issue-tracker-manager/v14";

pub fn contract_fingerprint() -> String {
    use sha2::{Digest, Sha256};

    format!(
        "sha256:{:x}",
        Sha256::digest(MANAGER_CONTRACT_VERSION.as_bytes())
    )
}

/// Derive the stable identity pair used by the Manager and its paired
/// TaskChampion store. Callers must pass the canonical application root so
/// that every bootstrap path (the primary server and the static runner)
/// computes the same identity before opening either store.
pub fn derive_store_identity_ids(root: &Path) -> (String, String) {
    let root_id = format!(
        "sha256:{:x}",
        Sha256::digest(format!("issue-tracker-manager/root/v1:{}", root.display()).as_bytes())
    );
    let pair_id = format!(
        "sha256:{:x}",
        Sha256::digest(format!("issue-tracker-manager/pair/v1:{root_id}").as_bytes())
    );
    (root_id, pair_id)
}

/// Resolve the Manager settings that are paired with one canonical Tracker
/// root.  Both the primary server and the static runner use this boundary so
/// identity defaults and validation cannot drift between bootstrap paths.
pub fn manager_config_for_tracker(
    settings: &issue_tracker_proto::manager_v1::IssueTrackerManagerSettings,
    tracker_root: &Path,
) -> Result<ManagerConfig, String> {
    let mut normalized = settings.clone();
    let root = if tracker_root.exists() {
        fs::canonicalize(tracker_root).map_err(|error| error.to_string())?
    } else {
        tracker_root.to_path_buf()
    };
    if normalized.data_root.is_empty() {
        normalized.data_root = root.to_string_lossy().into_owned();
    }
    if Path::new(&normalized.data_root) != root {
        return Err("manager data_root must match tracker.data_root".into());
    }

    let manager_contract = contract_fingerprint();
    let manager_schema = MANAGER_STORAGE_SCHEMA_FINGERPRINT.to_owned();
    if normalized.expected_contract_fingerprint.is_empty() {
        normalized.expected_contract_fingerprint = manager_contract.clone();
    }
    if normalized.expected_schema_fingerprint.is_empty() {
        normalized.expected_schema_fingerprint = manager_schema.clone();
    }
    if normalized.expected_contract_fingerprint != manager_contract
        || normalized.expected_schema_fingerprint != manager_schema
    {
        return Err("manager fingerprints do not match server".into());
    }

    let (root_id, pair_id) = derive_store_identity_ids(&root);
    let expected = vec![
        issue_tracker_proto::manager_v1::StoreIdentity {
            role: issue_tracker_proto::manager_v1::StoreRole::Manager as i32,
            data_root_id: root_id.clone(),
            store_id: "manager".into(),
            schema_fingerprint: manager_schema,
            store_pair_id: pair_id.clone(),
            contract_fingerprint: manager_contract,
            ..Default::default()
        },
        issue_tracker_proto::manager_v1::StoreIdentity {
            role: issue_tracker_proto::manager_v1::StoreRole::Taskchampion as i32,
            data_root_id: root_id,
            store_id: "taskchampion".into(),
            schema_fingerprint: crate::Tracker::schema_fingerprint(),
            store_pair_id: pair_id,
            contract_fingerprint: crate::Tracker::contract_fingerprint(),
            ..Default::default()
        },
    ];
    if normalized.store_identities.is_empty() {
        normalized.store_identities = expected;
    } else if normalized.store_identities != expected {
        return Err("manager store identities do not match the server-derived pair".into());
    }

    ManagerConfig::from_proto(&normalized).map_err(|error| error.to_string())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ManagerServiceError {
    #[error(transparent)]
    Canonical(#[from] canonical_service::CanonicalServiceError),
    #[error(transparent)]
    Repository(#[from] repository::RepositoryError),
}

pub(crate) enum PreparedCanonicalRead {
    Get {
        issue_id: String,
        snapshot: canonical_service::CanonicalReadSnapshot,
    },
    List(Box<PreparedCanonicalList>),
    Relations {
        issue_id: String,
        snapshot: canonical_service::CanonicalReadSnapshot,
        page: crate::proto_types::manager_v1::PageRequest,
        page_token_secret: String,
        page_size_limits: pagination::PageSizeLimits,
    },
}

pub(crate) struct PreparedCanonicalList {
    snapshot: canonical_service::CanonicalReadSnapshot,
    args: crate::proto_types::manager_v1::ListIssuesWithSyncArgs,
    page_token_secret: String,
    page_size_limits: pagination::PageSizeLimits,
}

pub(crate) struct PreparedReviewCandidateRead {
    pub(crate) snapshot: Arc<review_candidates::ReviewCandidateReadSnapshot>,
    pub(crate) args: crate::proto_types::manager_v1::ListReviewCandidatesArgs,
    pub(crate) pinned_read_registry: Arc<StdMutex<read_session::PinnedReadRegistry>>,
}

/// Manager-owned state prepared before a capability-bearing Tracker call.
/// The request bytes and marker expectation are immutable for the duration of
/// the external call; the Manager mutex can therefore be released safely.
pub(crate) struct PreparedLeaseCapability {
    pub(crate) method: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) operation: crate::proto_types::manager_v1::DurableOperation,
    pub(crate) expected: backend_adapter::MarkerExpectation,
    pub(crate) now_millis: i64,
    // The guard is intentionally retained for Drop-based fencing.  It is not
    // otherwise read after preparation.
    #[allow(dead_code)]
    active_guard: Option<CapabilityDispatchGuard>,
}

/// A process-local owner for the interval between capability admission and
/// finalization. Embedding the guard in the prepared request makes task
/// cancellation release the protection automatically.
struct CapabilityDispatchGuard {
    active: Arc<StdMutex<HashSet<String>>>,
    operation_id: String,
}

impl Drop for CapabilityDispatchGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.operation_id);
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum LeaseCapabilityPreparation {
    Immediate(Vec<u8>),
    Dispatch(PreparedLeaseCapability),
}

#[derive(Debug, thiserror::Error)]
pub enum ManagerError {
    #[error(transparent)]
    Configuration(#[from] ManagerConfigError),
    #[error(transparent)]
    Storage(#[from] ManagerStorageError),
    #[error(transparent)]
    Validation(#[from] validation::ManagerValidationError),
}

fn gateway_error(
    code: crate::proto_types::manager_v1::ManagerErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> crate::proto_types::manager_v1::ManagerError {
    crate::proto_types::manager_v1::ManagerError {
        code: code as i32,
        message: message.into(),
        retryable,
        detail: None,
    }
}

fn manager_service_error(
    error: ManagerServiceError,
) -> crate::proto_types::manager_v1::ManagerError {
    use crate::proto_types::manager_v1 as pb;

    match error {
        ManagerServiceError::Canonical(error) => match error {
            canonical_service::CanonicalServiceError::InvalidIssueId => gateway_error(
                pb::ManagerErrorCode::ManagerInvalidArgument,
                "issue ID is invalid",
                false,
            ),
            canonical_service::CanonicalServiceError::Tracker(error) => {
                tracker_service_error(error)
            }
            canonical_service::CanonicalServiceError::Storage(error) => gateway_error(
                pb::ManagerErrorCode::BackendUnavailable,
                error.to_string(),
                true,
            ),
            error => gateway_error(
                pb::ManagerErrorCode::PreconditionFailed,
                error.to_string(),
                false,
            ),
        },
        ManagerServiceError::Repository(error) => match error {
            repository::RepositoryError::IdempotencyConflict => gateway_error(
                pb::ManagerErrorCode::IdempotencyConflict,
                error.to_string(),
                false,
            ),
            error => gateway_error(
                pb::ManagerErrorCode::BackendUnavailable,
                error.to_string(),
                true,
            ),
        },
    }
}

fn review_candidate_service_error(
    error: review_candidates::ReviewCandidateReadError,
) -> crate::proto_types::manager_v1::ManagerError {
    use crate::proto_types::manager_v1 as pb;
    match error {
        review_candidates::ReviewCandidateReadError::Tracker(error) => tracker_service_error(error),
        review_candidates::ReviewCandidateReadError::Storage(error) => gateway_error(
            pb::ManagerErrorCode::BackendUnavailable,
            error.to_string(),
            true,
        ),
        review_candidates::ReviewCandidateReadError::Integrity(error) => gateway_error(
            pb::ManagerErrorCode::BackendUnavailable,
            error.to_string(),
            true,
        ),
    }
}

fn tracker_service_error(
    error: crate::TrackerError,
) -> crate::proto_types::manager_v1::ManagerError {
    use crate::proto_types::manager_v1 as pb;

    let tracker_error = crate::tracker::error_response::tracker_error(error);
    let detail = backend_dispatch::tracker_precondition_detail(&tracker_error);
    let message = tracker_error.message.clone();
    let (code, retryable) = match crate::v1::ErrorCode::try_from(tracker_error.code) {
        Ok(crate::v1::ErrorCode::IssueNotFound) => (pb::ManagerErrorCode::IssueNotFound, false),
        Ok(crate::v1::ErrorCode::RevisionConflict) => {
            (pb::ManagerErrorCode::RevisionConflict, false)
        }
        Ok(crate::v1::ErrorCode::IdempotencyConflict) => {
            (pb::ManagerErrorCode::IdempotencyConflict, false)
        }
        Ok(crate::v1::ErrorCode::ResultUnconfirmed) => {
            (pb::ManagerErrorCode::ResultUnconfirmed, false)
        }
        Ok(crate::v1::ErrorCode::SecretDetected) => {
            (pb::ManagerErrorCode::ManagerSecretDetected, false)
        }
        Ok(crate::v1::ErrorCode::InvalidPageToken) => {
            (pb::ManagerErrorCode::InvalidPageToken, false)
        }
        Ok(crate::v1::ErrorCode::PageTokenExpired) => {
            (pb::ManagerErrorCode::PageTokenExpired, false)
        }
        Ok(crate::v1::ErrorCode::BackendUnavailable) => {
            (pb::ManagerErrorCode::BackendUnavailable, true)
        }
        Ok(crate::v1::ErrorCode::InvalidStateTransition) => {
            (pb::ManagerErrorCode::InvalidStateTransition, false)
        }
        _ => (pb::ManagerErrorCode::PreconditionFailed, false),
    };
    pb::ManagerError {
        code: code as i32,
        message,
        retryable,
        detail,
    }
}

pub(crate) fn quarantined_dispatch_result(
    operation: crate::proto_types::manager_v1::DurableOperation,
    reason: &str,
) -> backend_dispatch::DispatchResult {
    let diagnosis = operation.diagnosis.clone();
    let error = crate::proto_types::manager_v1::ManagerError {
        code: crate::proto_types::manager_v1::ManagerErrorCode::ResolutionRequired as i32,
        message: reason.into(),
        retryable: false,
        detail: diagnosis
            .map(crate::proto_types::manager_v1::manager_error::Detail::OperationDiagnosis),
    };
    backend_dispatch::DispatchResult {
        state: crate::proto_types::manager_v1::DurableOperationState::ManualResolution,
        reason: reason.into(),
        response_payload: manager_error_result(&operation.method, error),
        ephemeral_claim_handle: None,
        issue_mappings: Vec::new(),
        operation,
    }
}

/// Validated manager settings and the durable manager SQLite boundary.
pub struct IssueTrackerManager {
    config: ManagerConfig,
    storage: ManagerStorage,
    outbox_driver_active: Arc<AtomicBool>,
    ownership_maintenance_healthy: Arc<AtomicBool>,
    active_capability_dispatches: Arc<StdMutex<HashSet<String>>>,
    pinned_read_registry: Arc<StdMutex<read_session::PinnedReadRegistry>>,
}

/// The verified backend identity needed to persist an IssueMapping.
///
/// Both immediate dispatch and crash recovery derive these values from a
/// verified marker. Keeping the conversion here prevents the two paths from
/// drifting on revision or store identity semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IssueMappingSeed {
    pub(crate) issue_id: String,
    pub(crate) backend_id: String,
    pub(crate) revision: u64,
}

/// Material prepared while the Manager mutex is held.  The Tracker call is
/// performed only after this value has been moved out of the Manager lock.
pub(crate) struct PreparedOutboxDispatch {
    pub(crate) entry: crate::proto_types::manager_v1::OutboxEntry,
    pub(crate) operation: crate::proto_types::manager_v1::DurableOperation,
    pub(crate) request: backend_dispatch::BackendDispatchRequest,
    pub(crate) expected: backend_adapter::MarkerExpectation,
    pub(crate) retry_budget: crate::proto_types::manager_v1::RetryBudget,
    pub(crate) now_millis: i64,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum OutboxPreparation {
    Dispatch(Box<PreparedOutboxDispatch>),
    Immediate(backend_dispatch::DispatchResult),
}

impl IssueTrackerManager {
    pub fn preflight(config: &ManagerConfig) -> Result<(), ManagerError> {
        ManagerStorage::preflight(config)?;
        Ok(())
    }

    pub fn open(config: ManagerConfig) -> Result<Self, ManagerError> {
        let mut storage = ManagerStorage::open(&config)?;
        let maintenance_healthy = storage
            .repository()
            .ownership_maintenance_healthy()
            .map_err(|error| ManagerStorageError::Transaction(error.to_string()))?;
        Ok(Self {
            config,
            storage,
            outbox_driver_active: Arc::new(AtomicBool::new(false)),
            ownership_maintenance_healthy: Arc::new(AtomicBool::new(maintenance_healthy)),
            active_capability_dispatches: Arc::new(StdMutex::new(HashSet::new())),
            pinned_read_registry: Arc::new(StdMutex::new(read_session::PinnedReadRegistry::new())),
        })
    }

    /// Snapshot capability calls that are still owned by a live task. The
    /// caller must hold the Manager mutex while taking this snapshot so it is
    /// ordered with durable attempt transitions.
    pub(crate) fn active_capability_dispatch_ids(&self) -> Result<Vec<String>, String> {
        self.active_capability_dispatches
            .lock()
            .map(|active| active.iter().cloned().collect())
            .map_err(|_| "active capability dispatch registry is poisoned".to_owned())
    }

    pub(crate) fn config(&self) -> &ManagerConfig {
        &self.config
    }

    pub(crate) fn storage(&self) -> &ManagerStorage {
        &self.storage
    }

    pub(crate) fn storage_mut(&mut self) -> &mut ManagerStorage {
        &mut self.storage
    }

    pub(crate) fn issue_mappings_from_seeds<I>(
        &self,
        seeds: I,
        generation: u64,
        created_at: Option<crate::proto_types::manager_v1::UtcInstant>,
        confirmed_at: crate::proto_types::manager_v1::UtcInstant,
    ) -> Result<Vec<crate::proto_types::manager_v1::IssueMapping>, ManagerStorageError>
    where
        I: IntoIterator<Item = IssueMappingSeed>,
    {
        let mut mappings = Vec::new();
        for seed in seeds {
            let candidate_id = self.storage.read(|connection| {
                connection
                    .query_row(
                        "SELECT candidate_id FROM manager_issue_mapping_index WHERE issue_id=?1",
                        params![seed.issue_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })?;
            mappings.push(crate::proto_types::manager_v1::IssueMapping {
                issue_id: seed.issue_id,
                candidate_id,
                backend_id: seed.backend_id,
                revision: seed.revision,
                // A mapping must retain the identity of each authoritative store independently.
                backend_contract_fingerprint: self
                    .config
                    .taskchampion_identity()
                    .contract_fingerprint
                    .clone(),
                manager_contract_fingerprint: self
                    .config
                    .manager_identity()
                    .contract_fingerprint
                    .clone(),
                generation,
                created_at: created_at.clone(),
                updated_at: Some(confirmed_at.clone()),
            });
        }
        Ok(mappings)
    }

    /// Load the outbox row associated with a recovery operation without
    /// leasing it. Startup reconciliation must be able to finalize an already
    /// acknowledged row while preserving the no-redelivery invariant.
    pub(crate) fn recovery_outbox_entry(
        &mut self,
        operation_id: &str,
    ) -> Result<Option<crate::proto_types::manager_v1::OutboxEntry>, String> {
        let outbox_id = self
            .storage
            .read(|connection| {
                connection
                    .query_row(
                        "SELECT outbox_id FROM manager_outbox WHERE operation_id = ?1",
                        params![operation_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(ManagerStorageError::from)
            })
            .map_err(|error| error.to_string())?;
        self.storage
            .repository()
            .get_outbox(outbox_id.as_deref().unwrap_or_default())
            .map_err(|error| error.to_string())
    }

    /// Marks the runner-owned driver as available after composition succeeds.
    pub(crate) fn activate_outbox_driver(&mut self) {
        self.outbox_driver_active.store(true, Ordering::SeqCst);
    }

    /// Marks the runner-owned driver as unavailable after a worker failure.
    pub(crate) fn deactivate_outbox_driver(&mut self) {
        self.outbox_driver_active.store(false, Ordering::SeqCst);
    }

    /// Ownership maintenance is independently observable from delivery. A
    /// healthy outbox must not hide a failed raw scan or cursor checkpoint.
    pub(crate) fn set_ownership_maintenance_health(&mut self, healthy: bool) -> Result<(), String> {
        if let Err(error) = self
            .storage
            .repository()
            .set_ownership_maintenance_health(healthy)
        {
            self.ownership_maintenance_healthy
                .store(false, Ordering::SeqCst);
            return Err(error.to_string());
        }
        self.ownership_maintenance_healthy
            .store(healthy, Ordering::SeqCst);
        Ok(())
    }

    /// Returns whether a new external Manager mutation may be admitted.
    /// Administrative recovery and read paths intentionally do not use this
    /// gate so an operator can repair a degraded ownership store.
    pub(crate) fn write_admission_available(&self) -> bool {
        self.ownership_maintenance_healthy.load(Ordering::SeqCst)
    }

    pub(crate) fn runner_read_context(
        &self,
        correlation_id: String,
    ) -> crate::proto_types::manager_v1::ManagerReadContext {
        crate::proto_types::manager_v1::ManagerReadContext {
            correlation_id,
            schema_fingerprint: self.config.expected_schema_fingerprint.clone(),
            data_root_id: self.config.manager_identity().data_root_id.clone(),
            contract_fingerprint: self.config.expected_contract_fingerprint.clone(),
        }
    }

    /// Returns manager health derived from the validated store pair held by
    /// this process. This is intentionally an internal service API; the
    /// primary service owns the external lifecycle and transport registration.
    pub(crate) fn get_contract_info(&self) -> crate::proto_types::manager_v1::ContractInfo {
        use crate::proto_types::manager_v1 as pb;

        let (health_epoch, phase_snapshots) =
            self.storage.phase_health_snapshots().unwrap_or_default();
        let maintenance_phases = phase_snapshots
            .into_iter()
            .map(|snapshot| pb::MaintenancePhaseHealth {
                phase: snapshot.phase,
                health: snapshot.health,
                last_started_at: snapshot
                    .last_started_at
                    .map(|unix_epoch_millis| pb::UtcInstant { unix_epoch_millis }),
                last_finished_at: snapshot
                    .last_finished_at
                    .map(|unix_epoch_millis| pb::UtcInstant { unix_epoch_millis }),
                last_failure_at: snapshot
                    .last_failure_at
                    .map(|unix_epoch_millis| pb::UtcInstant { unix_epoch_millis }),
                last_recovered_at: snapshot
                    .last_recovered_at
                    .map(|unix_epoch_millis| pb::UtcInstant { unix_epoch_millis }),
                attempt_count: snapshot.attempt_count,
                success_count: snapshot.success_count,
                skipped_count: snapshot.skipped_count,
                conflict_count: snapshot.conflict_count,
                diagnosis_count: snapshot.diagnosis_count,
                unconfirmed_count: snapshot.unconfirmed_count,
                pending_gauge: snapshot.pending_gauge,
                inconsistent_gauge: snapshot.inconsistent_gauge,
                permit_enabled: snapshot.permit_enabled,
                gauge_stale: snapshot.gauge_stale,
                diagnosis_refs: snapshot.diagnosis_refs,
            })
            .collect();

        pb::ContractInfo {
            contract_version: MANAGER_CONTRACT_VERSION.into(),
            contract_fingerprint: self.config.expected_contract_fingerprint.clone(),
            schema_fingerprint: self.config.expected_schema_fingerprint.clone(),
            method_keys: crate::proto_schema::MANAGER_METHOD_SPECS
                .iter()
                .map(|spec| spec.key.to_owned())
                .collect(),
            store_identities: self
                .config
                .store_identities
                .iter()
                .map(store_identity_proto)
                .collect(),
            features: {
                let mut features = vec![
                    pb::ManagerFeature::Candidates as i32,
                    pb::ManagerFeature::GatewayWorkerReflection as i32,
                    pb::ManagerFeature::OwnershipRecovery as i32,
                    pb::ManagerFeature::IssueAdminActions as i32,
                    pb::ManagerFeature::IssueOperationResolution as i32,
                    pb::ManagerFeature::OwnershipDiagnostics as i32,
                ];
                if self.outbox_driver_active.load(Ordering::SeqCst) {
                    features.push(pb::ManagerFeature::DurableOperations as i32);
                }
                features
            },
            // Until the lifecycle driver is active, advertising durable
            // operations would let callers treat unprocessed work as ready.
            health: if self.outbox_driver_active.load(Ordering::SeqCst)
                && self.ownership_maintenance_healthy.load(Ordering::SeqCst)
            {
                pb::HealthStatus::Healthy as i32
            } else {
                pb::HealthStatus::Degraded as i32
            },
            activation: if self.outbox_driver_active.load(Ordering::SeqCst)
                && self.ownership_maintenance_healthy.load(Ordering::SeqCst)
            {
                pb::ActivationStatus::Ready as i32
            } else {
                pb::ActivationStatus::Blocked as i32
            },
            root_operation_history_max_window: Some(pb::TimeSpan {
                millis: self.config.root_operation_history_max_window.as_millis() as u64,
            }),
            maintenance_phases,
            health_epoch,
            ..Default::default()
        }
    }

    pub(crate) fn prepare_canonical_read(
        &self,
        method: &str,
        payload: &[u8],
    ) -> Result<PreparedCanonicalRead, Vec<u8>> {
        use crate::proto_types::manager_v1 as pb;
        use prost::Message;

        match method {
            "manager_get_issue_relations" => {
                let args = match pb::GetIssueRelationsArgs::decode(payload) {
                    Ok(args) => args,
                    Err(error) => {
                        return Err(gateway::read_result::<pb::GetIssueRelationsResult>(
                            gateway_error(
                                pb::ManagerErrorCode::ManagerInvalidArgument,
                                error.to_string(),
                                false,
                            ),
                            0,
                        ));
                    }
                };
                if let Err(error) =
                    gateway::validate_read_context(&self.gateway_config(), args.context.as_ref())
                {
                    return Err(gateway::read_result::<pb::GetIssueRelationsResult>(
                        error, 0,
                    ));
                }
                let snapshot = canonical_service::CanonicalReadSnapshot::from_storage(
                    &self.storage,
                )
                .map_err(|error| {
                    gateway::read_result::<pb::GetIssueRelationsResult>(
                        manager_service_error(error.into()),
                        0,
                    )
                })?;
                Ok(PreparedCanonicalRead::Relations {
                    issue_id: args.issue_id,
                    snapshot,
                    page: args.page.unwrap_or_default(),
                    page_token_secret: canonical_service::list_page_token_secret(&self.storage),
                    page_size_limits: self.config.page_size_limits(),
                })
            }
            "manager_get_issue_with_sync" => {
                let args = match pb::GetIssueWithSyncArgs::decode(payload) {
                    Ok(args) => args,
                    Err(error) => {
                        return Err(gateway::read_result::<pb::GetIssueWithSyncResult>(
                            gateway_error(
                                pb::ManagerErrorCode::ManagerInvalidArgument,
                                error.to_string(),
                                false,
                            ),
                            0,
                        ));
                    }
                };
                if let Err(error) =
                    gateway::validate_read_context(&self.gateway_config(), args.context.as_ref())
                {
                    return Err(gateway::read_result::<pb::GetIssueWithSyncResult>(error, 0));
                }
                let snapshot =
                    match canonical_service::CanonicalReadSnapshot::from_storage(&self.storage) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            return Err(gateway::read_result::<pb::GetIssueWithSyncResult>(
                                manager_service_error(error.into()),
                                0,
                            ));
                        }
                    };
                Ok(PreparedCanonicalRead::Get {
                    issue_id: args.issue_id,
                    snapshot,
                })
            }
            "manager_list_issues_with_sync" => {
                let args = match pb::ListIssuesWithSyncArgs::decode(payload) {
                    Ok(args) => args,
                    Err(error) => {
                        return Err(gateway::read_result::<pb::ListIssuesWithSyncResult>(
                            gateway_error(
                                pb::ManagerErrorCode::ManagerInvalidArgument,
                                error.to_string(),
                                false,
                            ),
                            0,
                        ));
                    }
                };
                if let Err(error) =
                    gateway::validate_read_context(&self.gateway_config(), args.context.as_ref())
                {
                    return Err(gateway::read_result::<pb::ListIssuesWithSyncResult>(
                        error, 0,
                    ));
                }
                let snapshot =
                    match canonical_service::CanonicalReadSnapshot::from_storage(&self.storage) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            return Err(gateway::read_result::<pb::ListIssuesWithSyncResult>(
                                manager_service_error(error.into()),
                                0,
                            ));
                        }
                    };
                Ok(PreparedCanonicalRead::List(Box::new(
                    PreparedCanonicalList {
                        snapshot,
                        page_token_secret: canonical_service::list_page_token_secret(&self.storage),
                        args,
                        page_size_limits: self.config.page_size_limits(),
                    },
                )))
            }
            _ => unreachable!("canonical read preparation called for unsupported method"),
        }
    }

    pub(crate) fn prepare_review_candidate_read(
        &self,
        payload: &[u8],
    ) -> Result<PreparedReviewCandidateRead, Vec<u8>> {
        use crate::proto_types::manager_v1 as pb;
        use prost::Message;

        if payload.len() as u64 > self.config.maximum_payload_bytes {
            return Err(gateway::read_result::<pb::ListReviewCandidatesResult>(
                gateway_error(
                    pb::ManagerErrorCode::ResourceLimitExceeded,
                    "manager request exceeds the configured payload limit",
                    false,
                ),
                0,
            ));
        }
        let args = match pb::ListReviewCandidatesArgs::decode(payload) {
            Ok(args) => args,
            Err(error) => {
                return Err(gateway::read_result::<pb::ListReviewCandidatesResult>(
                    gateway_error(
                        pb::ManagerErrorCode::ManagerInvalidArgument,
                        error.to_string(),
                        false,
                    ),
                    0,
                ));
            }
        };
        if let Err(error) =
            gateway::validate_read_context(&self.gateway_config(), args.context.as_ref())
        {
            return Err(gateway::read_result::<pb::ListReviewCandidatesResult>(
                error, 0,
            ));
        }
        let snapshot = match review_candidates::snapshot_from_storage(
            &self.storage,
            &args,
            self.config.page_size_limits(),
            &self.config.manager_identity().contract_fingerprint,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(gateway::read_result::<pb::ListReviewCandidatesResult>(
                    review_candidate_service_error(error),
                    0,
                ));
            }
        };
        Ok(PreparedReviewCandidateRead {
            snapshot: Arc::new(snapshot),
            args,
            pinned_read_registry: Arc::clone(&self.pinned_read_registry),
        })
    }

    #[cfg(test)]
    pub(crate) async fn get_issue_with_sync(
        &self,
        tracker: &crate::tracker::Tracker,
        issue_id: &str,
    ) -> Result<crate::proto_types::manager_v1::IssueWithSync, ManagerServiceError> {
        Ok(canonical_service::CanonicalReadService::new(
            tracker,
            &self.storage,
            self.config.page_size_limits(),
        )
        .get_issue_with_sync(issue_id)
        .await?)
    }

    #[cfg(test)]
    pub(crate) async fn list_issues_with_sync(
        &self,
        tracker: &crate::tracker::Tracker,
        args: &crate::proto_types::manager_v1::ListIssuesWithSyncArgs,
    ) -> Result<crate::proto_types::manager_v1::IssueWithSyncPage, ManagerServiceError> {
        Ok(canonical_service::CanonicalReadService::new(
            tracker,
            &self.storage,
            self.config.page_size_limits(),
        )
        .list_issues_with_sync(args)
        .await?)
    }

    /// Dispatch one internal manager method through the typed gateway.
    ///
    /// This entry point intentionally remains private to this crate; manager
    /// methods are not exposed through a plugin runner.
    pub(crate) fn dispatch_manager(
        &mut self,
        method: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, String> {
        let gateway_config = self.gateway_config();
        if method == "manager_get_contract_info" {
            return self.dispatch_contract_info(payload);
        }
        let repository = self.storage.repository();
        let mut gateway = gateway::ManagerGateway::new(repository, gateway_config);
        gateway.dispatch(method, payload)
    }

    /// Dispatch a Gateway-control request after the listener selected its
    /// configured identity. The identity is deliberately not read from the
    /// public protobuf request: a remote caller must not choose a Gateway.
    pub(crate) fn dispatch_manager_for_gateway(
        &mut self,
        gateway_id: &str,
        method: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, String> {
        use crate::proto_types::manager_v1 as pb;
        use prost::Message;

        if gateway_id.trim().is_empty() {
            return Ok(gateway_control_error_result(
                method,
                gateway_error(
                    pb::ManagerErrorCode::ManagerInvalidArgument,
                    "gateway identity is required",
                    false,
                ),
            ));
        }

        match method {
            "manager_register_gateway" => {
                let args = match pb::RegisterGatewayArgs::decode(payload) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(gateway_control_error_result(
                            method,
                            gateway_error(
                                pb::ManagerErrorCode::ManagerInvalidArgument,
                                error.to_string(),
                                false,
                            ),
                        ));
                    }
                };
                let error = match args.context.as_ref() {
                    Some(context) => {
                        gateway::validate_write_context(&self.gateway_config(), method, context)
                            .err()
                    }
                    None => Some(gateway_error(
                        pb::ManagerErrorCode::ManagerInvalidArgument,
                        "write context is required",
                        false,
                    )),
                };
                if let Some(error) = error {
                    return Ok(gateway_control_error_result(method, error));
                }
                let Some(policy) = args.policy.as_ref() else {
                    return Ok(gateway_control_error_result(
                        method,
                        gateway_error(
                            pb::ManagerErrorCode::ManagerInvalidArgument,
                            "work eligibility policy is required",
                            false,
                        ),
                    ));
                };
                let result = self
                    .storage
                    .repository()
                    .register_gateway_policy(
                        gateway_id,
                        gateway_id,
                        &args
                            .context
                            .as_ref()
                            .expect("validated context")
                            .idempotency_key,
                        policy,
                    )
                    .map_err(|error| manager_service_error(error.into()));
                Ok(match result {
                    Ok(registration) => pb::RegisterGatewayResult {
                        outcome: Some(pb::register_gateway_result::Outcome::Success(registration)),
                    }
                    .encode_to_vec(),
                    Err(error) => gateway_control_error_result(method, error),
                })
            }
            "manager_get_gateway_registration" => {
                let args = match pb::GetGatewayRegistrationArgs::decode(payload) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(gateway_control_error_result(
                            method,
                            gateway_error(
                                pb::ManagerErrorCode::ManagerInvalidArgument,
                                error.to_string(),
                                false,
                            ),
                        ));
                    }
                };
                if let Err(error) =
                    gateway::validate_read_context(&self.gateway_config(), args.context.as_ref())
                {
                    return Ok(gateway_control_error_result(method, error));
                }
                let result = self
                    .storage
                    .repository()
                    .load_gateway_policy(gateway_id)
                    .map_err(|error| manager_service_error(error.into()));
                Ok(match result {
                    Ok(Some(registration)) => pb::GetGatewayRegistrationResult {
                        outcome: Some(pb::get_gateway_registration_result::Outcome::Success(
                            registration,
                        )),
                    }
                    .encode_to_vec(),
                    Ok(None) => gateway_control_error_result(
                        method,
                        gateway_error(
                            pb::ManagerErrorCode::BackendUnavailable,
                            "gateway has no registered policy",
                            true,
                        ),
                    ),
                    Err(error) => gateway_control_error_result(method, error),
                })
            }
            _ => self.dispatch_manager(method, payload),
        }
    }

    /// Dispatch a manager request with the southbound adapter composed by the
    /// caller. Lease-bearing operations are deliberately routed to the
    /// synchronous in-memory capability path; all other methods retain the
    /// ordinary manager gateway semantics.
    #[cfg(test)]
    pub(crate) async fn dispatch_manager_with_dispatcher<D>(
        &mut self,
        method: &str,
        payload: &[u8],
        dispatcher: &D,
        now_millis: i64,
    ) -> Result<Vec<u8>, String>
    where
        D: backend_dispatch::TaskChampionV1Dispatcher,
    {
        if requires_capability_dispatch(method, payload) {
            self.dispatch_lease_capability_with_lock(method, payload, dispatcher, now_millis)
                .await
        } else {
            self.dispatch_manager(method, payload)
        }
    }

    /// Dispatch methods whose implementation needs the active manager
    /// composition root. The synchronous gateway remains useful for manager
    /// entities, but it cannot safely manufacture a TaskChampion async read.
    #[cfg(test)]
    pub(crate) async fn dispatch_manager_with_tracker(
        &mut self,
        tracker: &crate::tracker::Tracker,
        method: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, String> {
        use crate::proto_types::manager_v1 as pb;
        use prost::Message;

        match method {
            "manager_get_issue_with_sync" => {
                let args = match pb::GetIssueWithSyncArgs::decode(payload) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(gateway::read_result::<pb::GetIssueWithSyncResult>(
                            gateway_error(
                                pb::ManagerErrorCode::ManagerInvalidArgument,
                                error.to_string(),
                                false,
                            ),
                            0,
                        ));
                    }
                };
                if let Err(error) =
                    gateway::validate_read_context(&self.gateway_config(), args.context.as_ref())
                {
                    return Ok(gateway::read_result::<pb::GetIssueWithSyncResult>(error, 0));
                }
                let result = self
                    .get_issue_with_sync(tracker, &args.issue_id)
                    .await
                    .map_err(manager_service_error);
                Ok(match result {
                    Ok(issue) => pb::GetIssueWithSyncResult {
                        outcome: Some(pb::get_issue_with_sync_result::Outcome::Success(issue)),
                    }
                    .encode_to_vec(),
                    Err(error) => gateway::read_result::<pb::GetIssueWithSyncResult>(error, 0),
                })
            }
            "manager_list_issues_with_sync" => {
                let args = match pb::ListIssuesWithSyncArgs::decode(payload) {
                    Ok(args) => args,
                    Err(error) => {
                        return Ok(gateway::read_result::<pb::ListIssuesWithSyncResult>(
                            gateway_error(
                                pb::ManagerErrorCode::ManagerInvalidArgument,
                                error.to_string(),
                                false,
                            ),
                            0,
                        ));
                    }
                };
                if let Err(error) =
                    gateway::validate_read_context(&self.gateway_config(), args.context.as_ref())
                {
                    return Ok(gateway::read_result::<pb::ListIssuesWithSyncResult>(
                        error, 0,
                    ));
                }
                let result = self
                    .list_issues_with_sync(tracker, &args)
                    .await
                    .map_err(manager_service_error);
                Ok(match result {
                    Ok(page) => pb::ListIssuesWithSyncResult {
                        outcome: Some(pb::list_issues_with_sync_result::Outcome::Success(page)),
                    }
                    .encode_to_vec(),
                    Err(error) => gateway::read_result::<pb::ListIssuesWithSyncResult>(error, 0),
                })
            }
            "manager_list_review_candidates" => {
                let prepared = self.prepare_review_candidate_read(payload);
                match prepared {
                    Ok(prepared) => Ok(dispatch_review_candidate_read(tracker, prepared).await),
                    Err(result) => Ok(result),
                }
            }
            _ => self.dispatch_manager(method, payload),
        }
    }

    fn gateway_config(&self) -> gateway::ManagerGatewayConfig {
        gateway::ManagerGatewayConfig {
            contract_fingerprint: self.config.expected_contract_fingerprint.clone(),
            schema_fingerprint: self.config.expected_schema_fingerprint.clone(),
            data_root_id: self.config.manager_identity().data_root_id.clone(),
            maximum_payload_bytes: self.config.maximum_payload_bytes,
            root_operation_history_max_window_millis: self
                .config
                .root_operation_history_max_window
                .as_millis() as i64,
            operation_history_v1_compat_enabled: self.config.operation_history_v1_compat_enabled,
            page_size_limits: self.config.page_size_limits(),
            write_admission: self.ownership_maintenance_healthy.load(Ordering::SeqCst),
        }
    }

    fn dispatch_contract_info(&self, payload: &[u8]) -> Result<Vec<u8>, String> {
        use crate::proto_types::manager_v1 as pb;
        use prost::Message;

        let args = match pb::GetContractInfoArgs::decode(payload) {
            Ok(args) => args,
            Err(error) => {
                return Ok(gateway::read_result::<pb::GetContractInfoResult>(
                    gateway_error(
                        pb::ManagerErrorCode::ManagerInvalidArgument,
                        error.to_string(),
                        false,
                    ),
                    0,
                ));
            }
        };
        if let Err(error) =
            gateway::validate_read_context(&self.gateway_config(), args.context.as_ref())
        {
            return Ok(gateway::read_result::<pb::GetContractInfoResult>(error, 0));
        }
        Ok(pb::GetContractInfoResult {
            outcome: Some(pb::get_contract_info_result::Outcome::Success(
                self.get_contract_info(),
            )),
        }
        .encode_to_vec())
    }

    /// Admit a capability write and persist its pre-send operation. The
    /// returned preparation contains no mutable Manager reference, allowing
    /// the resident driver to release the Manager mutex before contacting
    /// Tracker.
    pub(crate) fn prepare_lease_capability(
        &mut self,
        method: &str,
        payload: &[u8],
        now_millis: i64,
    ) -> Result<LeaseCapabilityPreparation, String> {
        use crate::proto_types::manager_v1 as pb;
        use gateway::ManagerRepository as _;
        use operations::{IdempotencyScope, NewOperation, digest_bytes, new_operation_id};
        use prost::Message;

        let redacted = backend_dispatch::redacted_capability_request(method, payload)
            .map_err(|error| error.to_string())?;
        let context = capability_context(method, payload)?;
        let gateway_config = gateway::ManagerGatewayConfig {
            contract_fingerprint: self.config.expected_contract_fingerprint.clone(),
            schema_fingerprint: self.config.expected_schema_fingerprint.clone(),
            data_root_id: self.config.manager_identity().data_root_id.clone(),
            maximum_payload_bytes: self.config.maximum_payload_bytes,
            root_operation_history_max_window_millis: self
                .config
                .root_operation_history_max_window
                .as_millis() as i64,
            operation_history_v1_compat_enabled: self.config.operation_history_v1_compat_enabled,
            page_size_limits: self.config.page_size_limits(),
            write_admission: self.ownership_maintenance_healthy.load(Ordering::SeqCst),
        };
        if !gateway_config.write_admission {
            return Ok(LeaseCapabilityPreparation::Immediate(
                capability_error_result(
                    method,
                    capability_error(
                        pb::ManagerErrorCode::BackendUnavailable,
                        "manager write admission is blocked while ownership maintenance is unhealthy",
                        true,
                    ),
                ),
            ));
        }
        if payload.len() as u64 > gateway_config.maximum_payload_bytes {
            return Ok(LeaseCapabilityPreparation::Immediate(
                capability_error_result(
                    method,
                    capability_error(
                        pb::ManagerErrorCode::ResourceLimitExceeded,
                        "manager request exceeds the configured payload limit",
                        false,
                    ),
                ),
            ));
        }
        if let Err(error) = gateway::validate_write_context(&gateway_config, method, &context) {
            return Ok(LeaseCapabilityPreparation::Immediate(
                capability_error_result(method, error),
            ));
        }
        if gateway::contains_secret(&redacted) {
            return Ok(LeaseCapabilityPreparation::Immediate(
                capability_error_result(
                    method,
                    capability_error(
                        pb::ManagerErrorCode::ManagerSecretDetected,
                        "secret-like value detected before manager admission",
                        false,
                    ),
                ),
            ));
        }
        if method == "manager_claim_issue" {
            let claim = match pb::ClaimIssueArgs::decode(payload) {
                Ok(claim) => claim,
                Err(error) => {
                    return Ok(LeaseCapabilityPreparation::Immediate(
                        capability_error_result(
                            method,
                            capability_error(
                                pb::ManagerErrorCode::ManagerInvalidArgument,
                                error.to_string(),
                                false,
                            ),
                        ),
                    ));
                }
            };
            if claim.gateway_eligibility_generation != Some(self.storage.generation()) {
                return Ok(LeaseCapabilityPreparation::Immediate(
                    capability_error_result(
                        method,
                        capability_error(
                            pb::ManagerErrorCode::PreconditionFailed,
                            "gateway work eligibility changed; re-evaluate the claim",
                            false,
                        ),
                    ),
                ));
            }
        }
        let scope = match IdempotencyScope::new(
            gateway_config.contract_fingerprint.clone(),
            gateway_config.data_root_id.clone(),
            method,
            context.target.clone(),
            context.idempotency_key.clone(),
        ) {
            Ok(scope) => scope,
            Err(error) => {
                return Ok(LeaseCapabilityPreparation::Immediate(
                    capability_error_result(
                        method,
                        capability_error(
                            pb::ManagerErrorCode::ManagerInvalidArgument,
                            error.to_string(),
                            false,
                        ),
                    ),
                ));
            }
        };
        let request_digest = digest_bytes(&redacted);
        let operation_id = new_operation_id();
        let reservation_scope = scope.canonical();
        {
            let mut repository = self.storage.repository();
            match repository.reserve_idempotency(&reservation_scope, &request_digest, &operation_id)
            {
                Err(error) => {
                    return Ok(LeaseCapabilityPreparation::Immediate(
                        capability_error_result(method, error),
                    ));
                }
                Ok(gateway::IdempotencyDecision::New) => {}
                Ok(gateway::IdempotencyDecision::Replay(_))
                | Ok(gateway::IdempotencyDecision::Pending { .. }) => {
                    return Ok(LeaseCapabilityPreparation::Immediate(
                        capability_error_result(
                            method,
                            capability_error(
                                pb::ManagerErrorCode::ResultUnconfirmed,
                                "ephemeral lease capability results are never replayed",
                                false,
                            ),
                        ),
                    ));
                }
            }
        }
        let issue_id = match capability_issue_id(method, payload) {
            Ok(issue_id) => issue_id,
            Err(error) => {
                let _ = self.storage.repository().abort_idempotency_reservation(
                    &reservation_scope,
                    &request_digest,
                    &operation_id,
                );
                return Err(error.to_string());
            }
        };
        let operation = NewOperation {
            operation_id: operation_id.clone(),
            scope: Some(operations::OperationScope::Issue(issue_id.clone())),
            method: method.into(),
            idempotency_scope: scope,
            request_digest: request_digest.clone(),
            request_schema: "issue_tracker.manager.v1.EphemeralLeaseCapability".into(),
            downstream_operation_id: Some(format!("backend-capability-{}", new_operation_id())),
            generation: self.storage.generation().saturating_add(1),
            manager_sequence: self.storage.manager_sequence(),
            causal_parent: None,
            affects_ready: true,
            sync_subject: Some(issue_id.clone()),
            affected_issue_ids: vec![issue_id.clone()],
            actor_audit: context.actor_audit.clone(),
            contract_fingerprint: gateway_config.contract_fingerprint,
            schema_fingerprint: gateway_config.schema_fingerprint,
            correlation_id: context.correlation_id.clone(),
            now_millis,
            replayable: false,
        };
        let mut operation = match operation.build() {
            Ok(operation) => operation,
            Err(error) => {
                let _ = self.storage.repository().abort_idempotency_reservation(
                    &reservation_scope,
                    &request_digest,
                    &operation_id,
                );
                return Err(error.to_string());
            }
        };
        operation.state = pb::DurableOperationState::Pending as i32;
        let mutation = pb::ManagerMutation {
            operation: Some(operation.clone()),
            ..Default::default()
        };
        let capability_fingerprint = operations::digest_bytes(&redacted);
        let mut expected = match backend_dispatch::derive_marker_expectation(&operation, payload) {
            Ok(expected) => expected,
            Err(error) => {
                let _ = self.storage.repository().abort_idempotency_reservation(
                    &reservation_scope,
                    &request_digest,
                    &operation_id,
                );
                return Err(error.to_string());
            }
        };
        if method == "manager_transition_issue"
            && pb::TransitionIssueArgs::decode(payload)
                .ok()
                .is_some_and(|args| args.awaiting_review.is_some())
        {
            // This dedicated transition's marker contains only immutable
            // completion evidence and a redacted Issue snapshot; unlike other
            // ownership operations it is safe for Gateway replay.
            expected.result_replayable = true;
        }
        expected.contract_fingerprint = self
            .config
            .taskchampion_identity()
            .contract_fingerprint
            .clone();
        if let Err(error) = self.storage.repository().commit_capability_admission(
            &mutation,
            &operation,
            &redacted,
            method,
            &issue_id,
            &capability_fingerprint,
            &expected,
        ) {
            let _ = self.storage.repository().abort_idempotency_reservation(
                &reservation_scope,
                &request_digest,
                &operation_id,
            );
            return Err(error.to_string());
        }
        let active_guard = {
            let mut active = self
                .active_capability_dispatches
                .lock()
                .map_err(|_| "active capability dispatch registry is poisoned".to_owned())?;
            active.insert(operation.operation_id.clone());
            CapabilityDispatchGuard {
                active: Arc::clone(&self.active_capability_dispatches),
                operation_id: operation.operation_id.clone(),
            }
        };
        Ok(LeaseCapabilityPreparation::Dispatch(
            PreparedLeaseCapability {
                method: method.into(),
                payload: payload.to_vec(),
                operation,
                expected,
                now_millis,
                active_guard: Some(active_guard),
            },
        ))
    }

    /// Finalize a prepared capability request after the external Tracker call
    /// has completed. The lease handle, if any, is used only to build the
    /// transient response and is never written to Manager storage.
    pub(crate) fn finalize_lease_capability(
        &mut self,
        mut prepared: PreparedLeaseCapability,
        response: Result<backend_dispatch::SouthboundResponse, backend_dispatch::SouthboundError>,
    ) -> Result<Vec<u8>, String> {
        use crate::proto_types::manager_v1 as pb;
        use crate::v1;
        use operations::transition_operation;
        use prost::Message;

        match response {
            Ok(response) => {
                let reconciliation = reconcile::reconcile_marker(
                    &mut prepared.operation,
                    Some(&response.marker),
                    &prepared.expected,
                    prepared.now_millis,
                )
                .map_err(|error| error.to_string())?;
                if reconciliation.state != pb::DurableOperationState::Committed {
                    let result = capability_error_result(
                        &prepared.method,
                        capability_error(
                            pb::ManagerErrorCode::ResultUnconfirmed,
                            "ephemeral lease capability could not be confirmed",
                            false,
                        ),
                    );
                    self.storage
                        .repository()
                        .finalize_capability_operation(
                            &prepared.operation,
                            Some(&result),
                            repository::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
                            "MARKER_UNCONFIRMED",
                            prepared.now_millis,
                        )
                        .map_err(|error| error.to_string())?;
                    return Ok(result);
                }
                if prepared.method == "manager_claim_issue" {
                    let lease_handle = match response.ephemeral_claim_handle {
                        Some(value) if !value.is_empty() => value,
                        _ => {
                            let result = capability_error_result(
                                &prepared.method,
                                capability_error(
                                    pb::ManagerErrorCode::ResultUnconfirmed,
                                    "claim was committed but the lease capability was not returned",
                                    false,
                                ),
                            );
                            self.storage
                                .repository()
                                .finalize_capability_operation(
                                    &prepared.operation,
                                    Some(&result),
                                    repository::CAPABILITY_ATTEMPT_RESOLVED,
                                    "MARKER_CONFIRMED_CAPABILITY_MISSING",
                                    prepared.now_millis,
                                )
                                .map_err(|error| error.to_string())?;
                            return Ok(result);
                        }
                    };
                    let current =
                        match v1::ClaimIssueResult::decode(response.response_payload.as_slice()) {
                            Ok(result) => match result.outcome {
                                Some(v1::claim_issue_result::Outcome::Success(value)) => value
                                    .mutation
                                    .and_then(|mutation| mutation.current)
                                    .and_then(|details| details.issue),
                                _ => None,
                            },
                            Err(_) => None,
                        };
                    let Some(current) = current else {
                        let result = capability_error_result(
                            &prepared.method,
                            capability_error(
                                pb::ManagerErrorCode::ResultUnconfirmed,
                                "claim response did not contain the canonical issue",
                                false,
                            ),
                        );
                        self.storage
                            .repository()
                            .finalize_capability_operation(
                                &prepared.operation,
                                Some(&result),
                                repository::CAPABILITY_ATTEMPT_RESOLVED,
                                "MARKER_CONFIRMED_RESPONSE_INVALID",
                                prepared.now_millis,
                            )
                            .map_err(|error| error.to_string())?;
                        return Ok(result);
                    };
                    let response_payload = capability_claim_success_result(
                        prepared.operation.clone(),
                        current,
                        lease_handle,
                    );
                    let durable_response =
                        redacted_capability_response(&prepared.method, &response_payload)?;
                    prepared.operation.terminal_snapshot = Some(
                        pb::durable_operation::TerminalSnapshot::ResultSnapshot(pb::Snapshot {
                            payload: durable_response.clone(),
                            schema: capability_result_schema(&prepared.method).into(),
                        }),
                    );
                    self.storage
                        .repository()
                        .finalize_capability_operation(
                            &prepared.operation,
                            Some(&durable_response),
                            repository::CAPABILITY_ATTEMPT_RESOLVED,
                            "MARKER_CONFIRMED",
                            prepared.now_millis,
                        )
                        .map_err(|error| error.to_string())?;
                    Ok(response_payload)
                } else {
                    let response_payload = capability_success_result_from_marker(
                        &prepared.method,
                        prepared.operation.clone(),
                        &response.marker,
                    )?;
                    let durable_response =
                        redacted_capability_response(&prepared.method, &response_payload)?;
                    prepared.operation.terminal_snapshot = Some(
                        pb::durable_operation::TerminalSnapshot::ResultSnapshot(pb::Snapshot {
                            payload: durable_response.clone(),
                            schema: capability_result_schema(&prepared.method).into(),
                        }),
                    );
                    self.storage
                        .repository()
                        .finalize_capability_operation(
                            &prepared.operation,
                            Some(&durable_response),
                            repository::CAPABILITY_ATTEMPT_RESOLVED,
                            "MARKER_CONFIRMED",
                            prepared.now_millis,
                        )
                        .map_err(|error| error.to_string())?;
                    Ok(response_payload)
                }
            }
            Err(backend_dispatch::SouthboundError::TypedBusiness(error))
                if !backend_dispatch::typed_error_is_definitive(&error) =>
            {
                transition_operation(
                    &mut prepared.operation,
                    pb::DurableOperationState::Unconfirmed,
                    None,
                    prepared.now_millis,
                )
                .map_err(|error| error.to_string())?;
                let result = capability_error_result(
                    &prepared.method,
                    capability_error(
                        pb::ManagerErrorCode::ResultUnconfirmed,
                        "ephemeral lease capability delivery is unconfirmed",
                        false,
                    ),
                );
                self.storage
                    .repository()
                    .finalize_capability_operation(
                        &prepared.operation,
                        Some(&result),
                        repository::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
                        "DELIVERY_UNKNOWN_TYPED_ERROR",
                        prepared.now_millis,
                    )
                    .map_err(|error| error.to_string())?;
                Ok(result)
            }
            Err(backend_dispatch::SouthboundError::TypedBusiness(error)) => {
                transition_operation(
                    &mut prepared.operation,
                    pb::DurableOperationState::Failed,
                    Some(pb::FailureClass::Terminal),
                    prepared.now_millis,
                )
                .map_err(|error| error.to_string())?;
                prepared.operation.affects_ready = false;
                let result = capability_error_result(&prepared.method, tracker_error(*error));
                prepared.operation.terminal_snapshot = Some(
                    pb::durable_operation::TerminalSnapshot::ErrorSnapshot(pb::Snapshot {
                        payload: result.clone(),
                        schema: capability_result_schema(&prepared.method).into(),
                    }),
                );
                self.storage
                    .repository()
                    .finalize_capability_operation(
                        &prepared.operation,
                        Some(&result),
                        repository::CAPABILITY_ATTEMPT_RESOLVED,
                        "REJECTED_BEFORE_MUTATION",
                        prepared.now_millis,
                    )
                    .map_err(|error| error.to_string())?;
                Ok(result)
            }
            Err(_) => {
                transition_operation(
                    &mut prepared.operation,
                    pb::DurableOperationState::Unconfirmed,
                    None,
                    prepared.now_millis,
                )
                .map_err(|error| error.to_string())?;
                let result = capability_error_result(
                    &prepared.method,
                    capability_error(
                        pb::ManagerErrorCode::ResultUnconfirmed,
                        "ephemeral lease capability delivery is unconfirmed",
                        false,
                    ),
                );
                self.storage
                    .repository()
                    .finalize_capability_operation(
                        &prepared.operation,
                        Some(&result),
                        repository::CAPABILITY_ATTEMPT_SENT_UNKNOWN,
                        "DELIVERY_UNKNOWN",
                        prepared.now_millis,
                    )
                    .map_err(|error| error.to_string())?;
                Ok(result)
            }
        }
    }

    /// Execute a capability request using the same staged lock boundary as
    /// the resident outbox driver. The Manager lock is held only while
    /// admitting and finalizing; Tracker is always awaited without it.
    #[cfg(test)]
    pub(crate) async fn dispatch_lease_capability_with_lock<D>(
        &mut self,
        method: &str,
        payload: &[u8],
        dispatcher: &D,
        now_millis: i64,
    ) -> Result<Vec<u8>, String>
    where
        D: backend_dispatch::TaskChampionV1Dispatcher,
    {
        let preparation = self.prepare_lease_capability(method, payload, now_millis)?;
        let prepared = match preparation {
            LeaseCapabilityPreparation::Immediate(result) => return Ok(result),
            LeaseCapabilityPreparation::Dispatch(prepared) => prepared,
        };
        self.storage
            .repository()
            .mark_capability_attempt_sending(
                &prepared.operation.operation_id,
                prepared.operation.generation,
                now_millis,
            )
            .map_err(|error| error.to_string())?;
        let response = dispatcher
            .dispatch_capability(&backend_dispatch::BackendDispatchRequest {
                method: prepared.method.clone(),
                payload: prepared.payload.clone(),
                downstream_operation_id: Some(prepared.operation.downstream_operation_id.clone()),
            })
            .await;
        self.finalize_lease_capability(prepared, response)
    }

    /// Prepare one outbox delivery while holding the Manager lock, but do not
    /// await the external Tracker.  This is the lock boundary used by the
    /// resident driver; all malformed rows are quarantined here and returned
    /// as an immediate result.
    pub(crate) fn prepare_outbox(
        &mut self,
        operation_id: Option<&str>,
        now_millis: i64,
    ) -> Result<Option<OutboxPreparation>, String> {
        let lease_millis = u64::try_from(self.config.outbox_lease.as_millis())
            .map_err(|_| "outbox lease duration overflows milliseconds".to_owned())?;
        let mut repository = self.storage.repository();
        let entry = match operation_id {
            Some(operation_id) => {
                repository.lease_outbox_for_operation(operation_id, now_millis, lease_millis)
            }
            None => repository.lease_due_outbox(now_millis, lease_millis),
        }
        .map_err(|error| error.to_string())?;
        let Some(entry) = entry else {
            return Ok(None);
        };
        let operation = match repository.get_operation(&entry.operation_id) {
            Ok(Some(operation)) => operation,
            Ok(None) => {
                repository
                    .quarantine_orphan_outbox(&entry, now_millis)
                    .map_err(|error| error.to_string())?;
                return Ok(None);
            }
            Err(_) => {
                repository
                    .quarantine_corrupt_operation_outbox(&entry, now_millis)
                    .map_err(|error| error.to_string())?;
                return Ok(None);
            }
        };
        if repository
            .has_legacy_backend_request_snapshot(&entry.operation_id)
            .map_err(|error| error.to_string())?
        {
            let (operation, _) = repository
                .quarantine_legacy_outbox(&operation, &entry, now_millis)
                .map_err(|error| error.to_string())?;
            return Ok(Some(OutboxPreparation::Immediate(
                quarantined_dispatch_result(
                    operation,
                    "legacy raw backend request snapshot requires manual resolution",
                ),
            )));
        }
        let request = match repository.backend_request(&entry.operation_id) {
            Ok(Some(request)) if !request.is_empty() => request,
            Ok(_) => {
                let (operation, _) = repository
                    .quarantine_outbox(
                        &operation,
                        &entry,
                        now_millis,
                        "backend request snapshot is missing",
                    )
                    .map_err(|error| error.to_string())?;
                return Ok(Some(OutboxPreparation::Immediate(
                    quarantined_dispatch_result(operation, "backend request snapshot is missing"),
                )));
            }
            Err(_) => {
                let (operation, _) = repository
                    .quarantine_outbox(
                        &operation,
                        &entry,
                        now_millis,
                        "backend request snapshot integrity validation failed",
                    )
                    .map_err(|error| error.to_string())?;
                return Ok(Some(OutboxPreparation::Immediate(
                    quarantined_dispatch_result(
                        operation,
                        "backend request snapshot integrity validation failed",
                    ),
                )));
            }
        };
        let backend_method =
            match backend_dispatch::DurableBackendRequest::decode(request.as_slice()) {
                Ok(request) if !request.method.trim().is_empty() => request.method,
                _ => {
                    let (operation, _) = repository
                        .quarantine_outbox(
                            &operation,
                            &entry,
                            now_millis,
                            "invalid durable backend request requires manual resolution",
                        )
                        .map_err(|error| error.to_string())?;
                    return Ok(Some(OutboxPreparation::Immediate(
                        quarantined_dispatch_result(
                            operation,
                            "invalid durable backend request requires manual resolution",
                        ),
                    )));
                }
            };
        if backend_dispatch::validate_durable_request_snapshot(&backend_method, &request).is_err() {
            let (operation, _) = repository
                .quarantine_outbox(
                    &operation,
                    &entry,
                    now_millis,
                    "invalid durable backend request requires manual resolution",
                )
                .map_err(|error| error.to_string())?;
            return Ok(Some(OutboxPreparation::Immediate(
                quarantined_dispatch_result(
                    operation,
                    "invalid durable backend request requires manual resolution",
                ),
            )));
        }
        let expected = match repository.backend_marker_expectation(&entry.operation_id) {
            Ok(Some(expected)) => expected,
            Ok(None) | Err(_) => {
                let (operation, _) = repository
                    .quarantine_outbox(
                        &operation,
                        &entry,
                        now_millis,
                        "backend marker expectation is missing or invalid",
                    )
                    .map_err(|error| error.to_string())?;
                return Ok(Some(OutboxPreparation::Immediate(
                    quarantined_dispatch_result(
                        operation,
                        "backend marker expectation is missing or invalid",
                    ),
                )));
            }
        };
        if repository
            .has_acceptance_guard(&entry.operation_id)
            .map_err(|error| error.to_string())?
        {
            if let Some(operation) = repository
                .revalidate_acceptance_before_dispatch(&operation, &entry)
                .map_err(|error| error.to_string())?
            {
                return Ok(Some(OutboxPreparation::Immediate(
                    quarantined_dispatch_result(
                        operation,
                        "acceptance pre-dispatch revalidation failed",
                    ),
                )));
            }
        }
        let downstream_operation_id = operation.downstream_operation_id.clone();
        Ok(Some(OutboxPreparation::Dispatch(Box::new(
            PreparedOutboxDispatch {
                entry,
                operation,
                request: backend_dispatch::BackendDispatchRequest {
                    method: backend_method,
                    payload: request,
                    downstream_operation_id: Some(downstream_operation_id),
                },
                expected,
                retry_budget: self.config.retry_budget.clone(),
                now_millis,
            },
        ))))
    }

    /// Persist the result of a prepared delivery after the external await has
    /// completed.  No Tracker call occurs while this method borrows `self`.
    pub(crate) fn finalize_prepared_outbox(
        &mut self,
        prepared: PreparedOutboxDispatch,
        result: backend_dispatch::DispatchResult,
    ) -> Result<(), String> {
        let mappings = self
            .issue_mappings_from_seeds(
                result
                    .issue_mappings
                    .iter()
                    .map(|mapping| IssueMappingSeed {
                        issue_id: mapping.issue_id.clone(),
                        backend_id: mapping.backend_id.clone(),
                        revision: mapping.revision,
                    }),
                result.operation.generation,
                result.operation.created_at.clone().or(Some(
                    crate::proto_types::manager_v1::UtcInstant {
                        unix_epoch_millis: prepared.now_millis,
                    },
                )),
                crate::proto_types::manager_v1::UtcInstant {
                    unix_epoch_millis: prepared.now_millis,
                },
            )
            .map_err(|error| error.to_string())?;
        if result.operation.method == "manager_apply_issue_admin_action" {
            self.storage
                .repository()
                .finalize_admin_backend_dispatch(
                    &result.operation,
                    &prepared.entry,
                    &result.response_payload,
                )
                .map_err(|error| error.to_string())?;
        } else {
            self.storage
                .repository()
                .finalize_backend_dispatch(&result.operation, &prepared.entry, &mappings)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    /// Deliver one due backend operation through the staged preparation/finalization path.
    /// The compatibility entry point remains only for existing in-crate tests; all
    /// production delivery uses OutboxDriver::process_one.
    #[cfg(test)]
    pub(crate) async fn process_one_outbox<D>(
        &mut self,
        dispatcher: &D,
        now_millis: i64,
    ) -> Result<Option<backend_dispatch::DispatchResult>, String>
    where
        D: backend_dispatch::TaskChampionV1Dispatcher,
    {
        let preparation = self.prepare_outbox(None, now_millis)?;
        let Some(preparation) = preparation else {
            return Ok(None);
        };
        let mut prepared = match preparation {
            OutboxPreparation::Immediate(result) => return Ok(Some(result)),
            OutboxPreparation::Dispatch(prepared) => *prepared,
        };
        let lease_id = prepared
            .entry
            .lease_id
            .clone()
            .ok_or_else(|| "leased outbox has no lease ID".to_owned())?;
        let result = match backend_dispatch::dispatch_leased(
            dispatcher,
            backend_dispatch::DispatchLeaseInput {
                entry: &mut prepared.entry,
                operation: &prepared.operation,
                request: prepared.request.clone(),
                expected: prepared.expected.clone(),
                lease_id: &lease_id,
                retry_budget: &prepared.retry_budget,
                now_millis: prepared.now_millis,
            },
        )
        .await
        {
            Ok(result) => result,
            Err(backend_dispatch::BackendDispatchError::InvalidRequest(_)) => {
                let (operation, _) = self
                    .storage
                    .repository()
                    .quarantine_outbox(
                        &prepared.operation,
                        &prepared.entry,
                        now_millis,
                        "backend outbox integrity validation failed",
                    )
                    .map_err(|error| error.to_string())?;
                return Ok(Some(quarantined_dispatch_result(
                    operation,
                    "backend outbox integrity validation failed",
                )));
            }
            Err(error) => return Err(error.to_string()),
        };
        self.finalize_prepared_outbox(prepared, result.clone())?;
        Ok(Some(result))
    }
}

pub(crate) async fn dispatch_prepared_canonical_read(
    tracker: &crate::tracker::Tracker,
    prepared: PreparedCanonicalRead,
) -> Vec<u8> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    match prepared {
        PreparedCanonicalRead::Relations {
            issue_id,
            snapshot,
            page,
            page_token_secret,
            page_size_limits,
        } => {
            let backend_generation = tracker.current_read_generation();
            let read = canonical_service::CanonicalReadService::get_issue_and_sync_from_snapshot(
                tracker, &snapshot, &issue_id,
            )
            .await;
            let generation = snapshot.composite_generation(backend_generation);
            let snapshot_id = format!(
                "issue-relations-{issue_id}-{}",
                snapshot.composite_snapshot_id(backend_generation)
            );
            let query_fingerprint = format!("issue-relations-{issue_id}");
            let codec = pagination::PageTokenCodec::new(page_token_secret);
            let cursor = page
                .page_token
                .as_deref()
                .map(|token| {
                    pagination::decode_bound_page_token(
                        &codec,
                        token,
                        generation,
                        &snapshot_id,
                        "relation-id",
                        &query_fingerprint,
                        i64::MAX,
                    )
                })
                .transpose();
            let boundary = tracker.validate_read_generation(backend_generation);
            match (read, cursor, boundary) {
                (Ok((issue, issue_with_sync)), Ok(cursor), Ok(())) => {
                    let mut relations = issue.relations;
                    relations.sort_by(|left, right| left.relation_id.cmp(&right.relation_id));
                    let (start, end, more) = pagination::paginate(
                        &relations,
                        page_size_limits.resolve(page.page_size),
                        cursor.as_ref(),
                    );
                    let next_page_token = more.then(|| {
                        codec.encode(&pagination::PageCursor {
                            offset: end,
                            generation,
                            snapshot_id: snapshot_id.clone(),
                            order_fingerprint: "relation-id".into(),
                            query_fingerprint: query_fingerprint.clone(),
                            expires_at_unix_epoch_millis: i64::MAX,
                        })
                    });
                    pb::GetIssueRelationsResult {
                        outcome: Some(pb::get_issue_relations_result::Outcome::Success(
                            pb::IssueRelationsWithSync {
                                issue: Some(issue_with_sync),
                                relations: relations[start..end]
                                    .iter()
                                    .map(crate::tracker::conversion::relation_proto)
                                    .collect(),
                                page: Some(pb::PageInfo {
                                    next_page_token,
                                    total_size: Some(relations.len() as u64),
                                    ..Default::default()
                                }),
                            },
                        )),
                    }
                    .encode_to_vec()
                }
                (Err(error), _, _) => gateway::read_result::<pb::GetIssueRelationsResult>(
                    manager_service_error(error.into()),
                    0,
                ),
                (_, Err(error), _) | (_, _, Err(error)) => {
                    let manager_error = match error {
                        error @ crate::error::TrackerError::InvalidPageToken => gateway_error(
                            pb::ManagerErrorCode::InvalidPageToken,
                            error.to_string(),
                            false,
                        ),
                        error @ crate::error::TrackerError::PageTokenExpired { .. } => {
                            gateway_error(
                                pb::ManagerErrorCode::PageTokenExpired,
                                error.to_string(),
                                false,
                            )
                        }
                        error => tracker_service_error(error),
                    };
                    gateway::read_result::<pb::GetIssueRelationsResult>(manager_error, 0)
                }
            }
        }
        PreparedCanonicalRead::Get { issue_id, snapshot } => {
            match canonical_service::CanonicalReadService::get_issue_with_sync_from_snapshot(
                tracker, &snapshot, &issue_id,
            )
            .await
            {
                Ok(issue) => pb::GetIssueWithSyncResult {
                    outcome: Some(pb::get_issue_with_sync_result::Outcome::Success(issue)),
                }
                .encode_to_vec(),
                Err(error) => gateway::read_result::<pb::GetIssueWithSyncResult>(
                    manager_service_error(error.into()),
                    0,
                ),
            }
        }
        PreparedCanonicalRead::List(list) => {
            let PreparedCanonicalList {
                snapshot,
                args,
                page_token_secret,
                page_size_limits,
            } = *list;
            match canonical_service::CanonicalReadService::list_issues_with_sync_from_snapshot(
                tracker,
                &page_token_secret,
                &snapshot,
                &args,
                page_size_limits,
            )
            .await
            {
                Ok(page) => pb::ListIssuesWithSyncResult {
                    outcome: Some(pb::list_issues_with_sync_result::Outcome::Success(page)),
                }
                .encode_to_vec(),
                Err(error) => gateway::read_result::<pb::ListIssuesWithSyncResult>(
                    manager_service_error(error.into()),
                    0,
                ),
            }
        }
    }
}

pub(crate) async fn dispatch_review_candidate_read(
    tracker: &crate::tracker::Tracker,
    prepared: PreparedReviewCandidateRead,
) -> Vec<u8> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    match review_candidates::list_from_snapshot(
        tracker,
        prepared.snapshot,
        &prepared.args,
        &prepared.pinned_read_registry,
    )
    .await
    {
        Ok(page) => pb::ListReviewCandidatesResult {
            outcome: Some(pb::list_review_candidates_result::Outcome::Success(page)),
        }
        .encode_to_vec(),
        Err(error) => gateway::read_result::<pb::ListReviewCandidatesResult>(
            review_candidate_service_error(error),
            0,
        ),
    }
}

fn store_identity_proto(
    identity: &model::StoreIdentity,
) -> crate::proto_types::manager_v1::StoreIdentity {
    crate::proto_types::manager_v1::StoreIdentity {
        role: identity.role.as_i32(),
        data_root_id: identity.data_root_id.clone(),
        store_id: identity.store_id.clone(),
        schema_fingerprint: identity.schema_fingerprint.clone(),
        generation: identity.generation,
        store_pair_id: identity.store_pair_id.clone(),
        fencing_epoch: identity.fencing_epoch,
        contract_fingerprint: identity.contract_fingerprint.clone(),
    }
}

fn capability_context(
    method: &str,
    payload: &[u8],
) -> Result<crate::proto_types::manager_v1::ManagerWriteContext, String> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    let context = match method {
        "manager_claim_issue" => {
            pb::ClaimIssueArgs::decode(payload)
                .map_err(|error| error.to_string())?
                .context
        }
        "manager_heartbeat_issue_claim" => {
            pb::HeartbeatIssueClaimArgs::decode(payload)
                .map_err(|error| error.to_string())?
                .context
        }
        "manager_release_issue_claim" => {
            pb::ReleaseIssueClaimArgs::decode(payload)
                .map_err(|error| error.to_string())?
                .context
        }
        "manager_transition_issue" => {
            pb::TransitionIssueArgs::decode(payload)
                .map_err(|error| error.to_string())?
                .context
        }
        _ => return Err("method is not an ephemeral lease capability".into()),
    };
    context.ok_or_else(|| "write context is required".into())
}

pub(crate) fn requires_capability_dispatch(method: &str, payload: &[u8]) -> bool {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    match method {
        "manager_transition_issue" => {
            pb::TransitionIssueArgs::decode(payload)
                .ok()
                .is_some_and(|args| {
                    args.awaiting_review.is_some()
                        || args.lease_handle.is_some_and(|handle| !handle.is_empty())
                })
        }
        _ => methods::is_capability(method),
    }
}

fn capability_issue_id(method: &str, payload: &[u8]) -> Result<String, String> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    let issue_id = match method {
        "manager_claim_issue" => {
            pb::ClaimIssueArgs::decode(payload)
                .map_err(|error| error.to_string())?
                .issue_id
        }
        "manager_heartbeat_issue_claim" => {
            pb::HeartbeatIssueClaimArgs::decode(payload)
                .map_err(|error| error.to_string())?
                .issue_id
        }
        "manager_release_issue_claim" => {
            pb::ReleaseIssueClaimArgs::decode(payload)
                .map_err(|error| error.to_string())?
                .issue_id
        }
        "manager_transition_issue" => {
            pb::TransitionIssueArgs::decode(payload)
                .map_err(|error| error.to_string())?
                .issue_id
        }
        _ => return Err("method is not an ephemeral lease capability".into()),
    };
    if issue_id.trim().is_empty() {
        Err("backend issue ID is required".into())
    } else {
        Ok(issue_id)
    }
}

fn capability_error(
    code: crate::proto_types::manager_v1::ManagerErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> crate::proto_types::manager_v1::ManagerError {
    crate::proto_types::manager_v1::ManagerError {
        code: code as i32,
        message: message.into(),
        retryable,
        detail: None,
    }
}

fn tracker_error(
    error: crate::v1::IssueTrackerError,
) -> crate::proto_types::manager_v1::ManagerError {
    let code = error_mapping::manager_error_code(&error);
    let detail = backend_dispatch::tracker_precondition_detail(&error);
    crate::proto_types::manager_v1::ManagerError {
        code: code as i32,
        message: error.message,
        retryable: false,
        detail,
    }
}

pub(crate) fn capability_success_result(
    method: &str,
    operation: crate::proto_types::manager_v1::DurableOperation,
    entity_revisions: Vec<crate::proto_types::manager_v1::ManagerEntityRevision>,
) -> Vec<u8> {
    use crate::proto_types::manager_v1 as pb;

    let mutation = pb::ManagerMutation {
        operation: Some(operation),
        entity_revisions,
        ..Default::default()
    };
    match gateway::mutation_wrapper_for_method(method) {
        Some(wrapper) => wrapper(Ok(mutation)),
        None => Vec::new(),
    }
}

/// Build an ownership-capability response from the immutable Tracker marker.
/// Heartbeat, release, and transition used to return only an entity revision,
/// which made a durable replay lose the resulting state (and, for some
/// callers, the full canonical Issue).  The marker already contains the
/// redacted Issue mutation snapshot, so derive the typed non-secret receipt
/// from that snapshot instead of rereading mutable Tracker state.
pub(crate) fn capability_success_result_from_marker(
    method: &str,
    operation: crate::proto_types::manager_v1::DurableOperation,
    marker: &crate::v1::OperationRecord,
) -> Result<Vec<u8>, String> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    let current = marker
        .result_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.value.as_ref())
        .and_then(|value| match value {
            crate::v1::operation_result_snapshot::Value::IssueMutation(snapshot) => snapshot
                .current
                .as_ref()
                .and_then(|details| details.issue.clone()),
            crate::v1::operation_result_snapshot::Value::ClaimMutation(snapshot) => {
                snapshot.mutation.as_ref().and_then(|mutation| {
                    mutation
                        .current
                        .as_ref()
                        .and_then(|details| details.issue.clone())
                })
            }
            _ => None,
        });
    let Some(current) = current else {
        // Older Tracker markers may contain only entity revisions.  Preserve
        // their replayable shape rather than turning a successful capability
        // into an unavailable error; newly written markers take the typed
        // ownership path below.
        return Ok(capability_success_result(
            method,
            operation,
            capability_entity_revisions(marker),
        ));
    };
    let result = pb::IssueOwnershipResult {
        method: method.to_owned(),
        issue_id: current.issue_id.clone(),
        resulting_state: current.state,
        resulting_revision: current.revision,
        // Durable manager snapshots contain only the non-secret ownership
        // receipt. The live Tracker response may include the Issue, but that
        // canonical payload must never be copied into the Manager store.
        current: None,
        audit_operation_id: marker.downstream_operation_id.clone(),
    };
    let mutation = pb::ManagerMutation {
        operation: Some(operation),
        entity_revisions: vec![pb::ManagerEntityRevision {
            entity_kind: "issue".into(),
            entity_id: result.issue_id.clone(),
            revision: result.resulting_revision,
        }],
        result: Some(pb::manager_mutation::Result::IssueOwnership(result)),
        ..Default::default()
    };
    let mutation = match method {
        "manager_heartbeat_issue_claim" => pb::HeartbeatIssueClaimResult {
            outcome: Some(pb::heartbeat_issue_claim_result::Outcome::Success(mutation)),
        }
        .encode_to_vec(),
        "manager_release_issue_claim" => pb::ReleaseIssueClaimResult {
            outcome: Some(pb::release_issue_claim_result::Outcome::Success(mutation)),
        }
        .encode_to_vec(),
        "manager_transition_issue" => pb::TransitionIssueResult {
            outcome: Some(pb::transition_issue_result::Outcome::Success(mutation)),
        }
        .encode_to_vec(),
        _ => return Err(format!("unsupported ownership capability method: {method}")),
    };
    Ok(mutation)
}

pub(crate) fn capability_entity_revisions(
    marker: &crate::v1::OperationRecord,
) -> Vec<crate::proto_types::manager_v1::ManagerEntityRevision> {
    marker
        .entity_revisions
        .iter()
        .map(
            |revision| crate::proto_types::manager_v1::ManagerEntityRevision {
                entity_kind: "issue".into(),
                entity_id: revision.issue_id.clone(),
                revision: revision.resulting_revision,
            },
        )
        .collect()
}

fn capability_claim_success_result(
    operation: crate::proto_types::manager_v1::DurableOperation,
    current: crate::v1::Issue,
    lease_handle: Vec<u8>,
) -> Vec<u8> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    let mutation = pb::ManagerMutation {
        operation: Some(operation),
        result: Some(pb::manager_mutation::Result::ClaimIssue(
            pb::ManagerClaimIssueSuccess {
                current: Some(current),
                lease_handle,
            },
        )),
        ..Default::default()
    };
    pb::ClaimIssueResult {
        outcome: Some(pb::claim_issue_result::Outcome::Success(mutation)),
    }
    .encode_to_vec()
}

pub(crate) fn manager_error_result(
    method: &str,
    error: crate::proto_types::manager_v1::ManagerError,
) -> Vec<u8> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    // Mutation wrappers are the single source of truth for method-specific
    // result envelopes. Keep only the non-mutation manager operations here.
    if let Some(wrapper) = gateway::mutation_wrapper_for_method(method) {
        return wrapper(Err(error));
    }
    match method {
        "manager_resolve_candidate" => pb::ResolveCandidateResult {
            outcome: Some(pb::resolve_candidate_result::Outcome::Error(error)),
        }
        .encode_to_vec(),
        "manager_retry_operation" => pb::RetryOperationResult {
            outcome: Some(pb::retry_operation_result::Outcome::Error(error)),
        }
        .encode_to_vec(),
        _ => Vec::new(),
    }
}

fn gateway_control_error_result(
    method: &str,
    error: crate::proto_types::manager_v1::ManagerError,
) -> Vec<u8> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    match method {
        "manager_register_gateway" => pb::RegisterGatewayResult {
            outcome: Some(pb::register_gateway_result::Outcome::Error(error)),
        }
        .encode_to_vec(),
        "manager_get_gateway_registration" => pb::GetGatewayRegistrationResult {
            outcome: Some(pb::get_gateway_registration_result::Outcome::Error(error)),
        }
        .encode_to_vec(),
        _ => manager_error_result(method, error),
    }
}

pub(crate) fn capability_error_result(
    method: &str,
    error: crate::proto_types::manager_v1::ManagerError,
) -> Vec<u8> {
    manager_error_result(method, error)
}

pub(crate) fn capability_result_schema(method: &str) -> &'static str {
    methods::spec(method)
        .filter(|value| value.supports_capability_dispatch)
        .map(|value| value.result_schema)
        .unwrap_or("issue_tracker.manager.v1.ManagerError")
}

/// Strip the nested operation and every ephemeral capability from a
/// method-specific response before it is placed in a terminal snapshot.
/// The direct northbound response is built separately and may contain the
/// one-time Claim handle.
pub(crate) fn redacted_capability_response(
    method: &str,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    use crate::proto_types::manager_v1 as pb;
    use prost::Message;

    match method {
        "manager_claim_issue" => {
            let mut result = pb::ClaimIssueResult::decode(payload)
                .map_err(|error| format!("claim capability result decode: {error}"))?;
            if let Some(pb::claim_issue_result::Outcome::Success(mut mutation)) = result.outcome {
                mutation.operation = None;
                if let Some(pb::manager_mutation::Result::ClaimIssue(value)) =
                    mutation.result.as_mut()
                {
                    value.current = None;
                    value.lease_handle.clear();
                }
                result.outcome = Some(pb::claim_issue_result::Outcome::Success(mutation));
            }
            Ok(result.encode_to_vec())
        }
        "manager_heartbeat_issue_claim" => {
            let mut result = pb::HeartbeatIssueClaimResult::decode(payload)
                .map_err(|error| format!("heartbeat capability result decode: {error}"))?;
            if let Some(pb::heartbeat_issue_claim_result::Outcome::Success(mut mutation)) =
                result.outcome
            {
                mutation.operation = None;
                if let Some(pb::manager_mutation::Result::IssueOwnership(value)) =
                    mutation.result.as_mut()
                {
                    value.current = None;
                }
                result.outcome = Some(pb::heartbeat_issue_claim_result::Outcome::Success(mutation));
            }
            Ok(result.encode_to_vec())
        }
        "manager_release_issue_claim" => {
            let mut result = pb::ReleaseIssueClaimResult::decode(payload)
                .map_err(|error| format!("release capability result decode: {error}"))?;
            if let Some(pb::release_issue_claim_result::Outcome::Success(mut mutation)) =
                result.outcome
            {
                mutation.operation = None;
                if let Some(pb::manager_mutation::Result::IssueOwnership(value)) =
                    mutation.result.as_mut()
                {
                    value.current = None;
                }
                result.outcome = Some(pb::release_issue_claim_result::Outcome::Success(mutation));
            }
            Ok(result.encode_to_vec())
        }
        "manager_transition_issue" => {
            let mut result = pb::TransitionIssueResult::decode(payload)
                .map_err(|error| format!("transition capability result decode: {error}"))?;
            if let Some(pb::transition_issue_result::Outcome::Success(mut mutation)) =
                result.outcome
            {
                mutation.operation = None;
                if let Some(pb::manager_mutation::Result::IssueOwnership(value)) =
                    mutation.result.as_mut()
                {
                    value.current = None;
                }
                result.outcome = Some(pb::transition_issue_result::Outcome::Success(mutation));
            }
            Ok(result.encode_to_vec())
        }
        _ => Err("method is not an ephemeral lease capability".into()),
    }
}

#[cfg(test)]
mod tests;
