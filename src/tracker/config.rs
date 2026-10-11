use crate::{Tracker, tracker::actor::ActorIdentity, v1};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub(crate) const CONTRACT_VERSION: &str = "lookback-issue/v7";
const DEFAULT_OWNERSHIP_MAINTENANCE_INTERVAL_SECS: u64 = 30;
const DEFAULT_OWNERSHIP_RECOVERY_BATCH_SIZE: usize = 100;

#[derive(Clone)]
pub struct RunnerConfig {
    pub(crate) data_root: PathBuf,
    pub(crate) source_evidence_limits: SourceEvidenceLimits,
    pub(crate) default_lease_secs: u64,
    pub(crate) minimum_lease_secs: u64,
    pub(crate) maximum_lease_secs: u64,
    pub(crate) maximum_heartbeat_secs: u64,
    pub(crate) ownership_maintenance_interval_millis: u64,
    pub(crate) ownership_recovery_batch_size: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceEvidenceLimits {
    pub(crate) max_string_bytes: u64,
    pub(crate) max_repeated_item_count: u64,
}

impl SourceEvidenceLimits {
    pub(crate) const UNBOUNDED: Self = Self {
        max_string_bytes: u64::MAX,
        max_repeated_item_count: u64::MAX,
    };
}

impl RunnerConfig {
    /// Build the runtime configuration after validating the public settings contract.
    pub fn from_settings(settings: &v1::IssueTrackerRunnerSettings) -> Result<Self, String> {
        if settings.data_root.is_empty() {
            return Err("missing tracker.data_root".into());
        }
        if !settings.expected_contract_fingerprint.is_empty()
            && settings.expected_contract_fingerprint != contract_fingerprint()
        {
            return Err("contract fingerprint mismatch".into());
        }
        if !settings.expected_storage_schema_fingerprint.is_empty()
            && settings.expected_storage_schema_fingerprint != Tracker::schema_fingerprint()
        {
            return Err("storage schema fingerprint mismatch".into());
        }
        let root = PathBuf::from(&settings.data_root);
        if !root.is_absolute() {
            return Err("tracker.data_root must be absolute".into());
        }
        if let Ok(metadata) = fs::symlink_metadata(&root) {
            if metadata.file_type().is_symlink() {
                return Err("tracker.data_root must not be a symlink".into());
            }
        } else if root.exists() {
            return Err("tracker.data_root metadata is unavailable".into());
        }
        let lease = settings
            .lease_policy
            .as_ref()
            .ok_or("tracker.lease_policy is required")?;
        let duration = |value: Option<&v1::TimeSpan>| value.map(|value| value.millis).unwrap_or(0);
        let (default, minimum, maximum, heartbeat) = (
            duration(lease.default_lease.as_ref()),
            duration(lease.minimum_lease.as_ref()),
            duration(lease.maximum_lease.as_ref()),
            duration(lease.maximum_heartbeat_interval.as_ref()),
        );
        if minimum == 0
            || default == 0
            || maximum == 0
            || heartbeat == 0
            || minimum > default
            || default > maximum
            || heartbeat.saturating_mul(3) > minimum
        {
            return Err("invalid tracker.lease_policy".into());
        }
        let pagination = settings
            .pagination_policy
            .as_ref()
            .ok_or("tracker.pagination_policy is required")?;
        if pagination.default_page_size == 0
            || pagination.maximum_page_size == 0
            || pagination.default_page_size > pagination.maximum_page_size
        {
            return Err("invalid tracker.pagination_policy".into());
        }
        if v1::SecretFilterMode::try_from(settings.secret_filter_mode)
            != Ok(v1::SecretFilterMode::FailClosed)
        {
            return Err("tracker.secret_filter_mode must be FAIL_CLOSED".into());
        }
        let limits = settings
            .resource_limits
            .as_ref()
            .ok_or("tracker.resource_limits is required")?;
        if [
            limits.max_string_bytes,
            limits.max_body_bytes,
            limits.max_uri_bytes,
            limits.max_token_bytes,
            limits.max_handle_bytes,
            limits.max_repeated_item_count,
            limits.max_export_bytes,
            limits.max_export_item_count,
            limits.max_artifact_bytes,
            limits.max_artifact_count,
            limits.max_total_count_scan_items,
        ]
        .into_iter()
        .any(|value| value == 0)
        {
            return Err("tracker.resource_limits must be positive".into());
        }
        let ownership_interval_millis = settings
            .ownership_maintenance_interval
            .as_ref()
            .map(|value| value.millis)
            .filter(|millis| *millis != 0)
            .unwrap_or(DEFAULT_OWNERSHIP_MAINTENANCE_INTERVAL_SECS.saturating_mul(1_000));
        if ownership_interval_millis < 1_000 {
            return Err("ownership maintenance interval must be at least one second".into());
        }
        let ownership_batch_size = if settings.ownership_recovery_batch_size == 0 {
            DEFAULT_OWNERSHIP_RECOVERY_BATCH_SIZE
        } else {
            usize::try_from(settings.ownership_recovery_batch_size)
                .map_err(|_| "ownership recovery batch size is too large")?
        };
        if ownership_batch_size > DEFAULT_OWNERSHIP_RECOVERY_BATCH_SIZE {
            return Err("ownership recovery batch size must not exceed 100".into());
        }
        Ok(Self {
            data_root: root,
            source_evidence_limits: SourceEvidenceLimits {
                max_string_bytes: limits.max_string_bytes,
                max_repeated_item_count: limits.max_repeated_item_count,
            },
            default_lease_secs: default / 1000,
            minimum_lease_secs: minimum / 1000,
            maximum_lease_secs: maximum / 1000,
            maximum_heartbeat_secs: heartbeat / 1000,
            ownership_maintenance_interval_millis: ownership_interval_millis,
            ownership_recovery_batch_size: ownership_batch_size,
        })
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }
}

pub(crate) fn contract_fingerprint() -> String {
    format!("sha256:{:x}", Sha256::digest(CONTRACT_VERSION.as_bytes()))
}

#[derive(Clone, Default)]
pub(crate) struct CallContext {
    pub(crate) operation_id: String,
    pub(crate) actor: ActorIdentity,
    pub(crate) session_id: Option<String>,
    pub(crate) correlation_id: String,
    pub(crate) reason: String,
}
