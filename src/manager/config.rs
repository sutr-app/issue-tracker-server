use super::{
    model::{StoreIdentity, StoreRole},
    pagination::PageSizeLimits,
};
use crate::proto_types::manager_v1;
use std::{path::PathBuf, time::Duration};

const REQUIRED_RESOURCE_LIMITS: &[&str] = &[
    "max_string_bytes",
    "max_body_bytes",
    "max_uri_bytes",
    "max_repeated_item_count",
    "max_page_size",
];
const DEFAULT_ROOT_OPERATION_HISTORY_MAX_WINDOW: Duration = Duration::from_secs(90 * 24 * 60 * 60);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ManagerConfigError {
    #[error("manager setting is invalid: {0}")]
    Invalid(String),
}

/// Validated settings used to open the manager store.
#[derive(Clone, Debug)]
pub struct ManagerConfig {
    pub data_root: PathBuf,
    pub expected_contract_fingerprint: String,
    pub expected_schema_fingerprint: String,
    pub default_page_size: u32,
    pub maximum_page_size: u32,
    pub maximum_payload_bytes: u64,
    pub maximum_repeated_item_count: u64,
    pub operation_lease: Duration,
    pub outbox_lease: Duration,
    pub store_identities: Vec<StoreIdentity>,
    pub resource_limits: manager_v1::ManagerResourceLimits,
    pub retry_budget: manager_v1::RetryBudget,
    pub root_operation_history_max_window: Duration,
    pub operation_history_v1_compat_enabled: bool,
}

/// Bounds applied to manager-owned source references.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SourceEvidenceLimits {
    pub(crate) max_string_bytes: u64,
    pub(crate) max_repeated_item_count: u64,
}

impl ManagerConfig {
    pub(crate) fn source_evidence_limits(&self) -> SourceEvidenceLimits {
        SourceEvidenceLimits {
            max_string_bytes: self.resource_limits.max_string_bytes,
            max_repeated_item_count: self
                .maximum_repeated_item_count
                .min(self.resource_limits.max_repeated_item_count),
        }
    }

    pub(crate) fn page_size_limits(&self) -> PageSizeLimits {
        let resource_maximum =
            u32::try_from(self.resource_limits.max_page_size).unwrap_or(u32::MAX);
        let maximum_page_size = self.maximum_page_size.min(resource_maximum);
        PageSizeLimits::new(
            self.default_page_size.min(maximum_page_size),
            maximum_page_size,
        )
    }

    pub fn from_proto(
        settings: &manager_v1::IssueTrackerManagerSettings,
    ) -> Result<Self, ManagerConfigError> {
        let secret_filter = manager_v1::SecretFilterMode::try_from(settings.secret_filter_mode)
            .map_err(|_| ManagerConfigError::Invalid("secret_filter_mode is unknown".into()))?;
        if secret_filter != manager_v1::SecretFilterMode::FailClosed {
            return Err(ManagerConfigError::Invalid(
                "secret_filter_mode must be FAIL_CLOSED".into(),
            ));
        }
        let data_root = PathBuf::from(&settings.data_root);
        if settings.data_root.is_empty() || !data_root.is_absolute() {
            return Err(ManagerConfigError::Invalid(
                "data_root must be an absolute path".into(),
            ));
        }
        if settings.expected_contract_fingerprint.is_empty()
            || settings.expected_schema_fingerprint.is_empty()
        {
            return Err(ManagerConfigError::Invalid(
                "contract and schema fingerprints are required".into(),
            ));
        }
        if settings.default_page_size == 0
            || settings.maximum_page_size == 0
            || settings.default_page_size > settings.maximum_page_size
        {
            return Err(ManagerConfigError::Invalid(
                "page size limits are invalid".into(),
            ));
        }
        if settings.maximum_payload_bytes == 0 || settings.maximum_repeated_item_count == 0 {
            return Err(ManagerConfigError::Invalid(
                "payload and repeated-item limits must be positive".into(),
            ));
        }
        let operation_lease =
            duration_from_proto(settings.operation_lease.as_ref(), "operation_lease")?;
        let outbox_lease = duration_from_proto(settings.outbox_lease.as_ref(), "outbox_lease")?;
        let resource_limits = settings
            .resource_limits
            .clone()
            .ok_or_else(|| ManagerConfigError::Invalid("resource_limits are required".into()))?;
        for name in REQUIRED_RESOURCE_LIMITS {
            if resource_limit(&resource_limits, name) == 0 {
                return Err(ManagerConfigError::Invalid(format!(
                    "resource limit {name} must be positive"
                )));
            }
        }
        let retry_budget = settings
            .retry_budget
            .clone()
            .ok_or_else(|| ManagerConfigError::Invalid("retry_budget is required".into()))?;
        if retry_budget.max_attempts == 0 {
            return Err(ManagerConfigError::Invalid(
                "retry_budget.max_attempts must be positive".into(),
            ));
        }
        let base_backoff = duration_from_proto(retry_budget.base_backoff.as_ref(), "base_backoff")?;
        let max_backoff = duration_from_proto(retry_budget.max_backoff.as_ref(), "max_backoff")?;
        if base_backoff > max_backoff {
            return Err(ManagerConfigError::Invalid(
                "retry backoff bounds are invalid".into(),
            ));
        }
        let root_operation_history_max_window = optional_duration_from_proto(
            settings.root_operation_history_max_window.as_ref(),
            "root_operation_history_max_window",
            DEFAULT_ROOT_OPERATION_HISTORY_MAX_WINDOW,
        )?;
        let operation_history_v1_compat_enabled =
            settings.operation_history_v1_compat_enabled.unwrap_or(true);

        let store_identities = settings
            .store_identities
            .iter()
            .map(StoreIdentity::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        validate_store_identities(
            &store_identities,
            &settings.expected_contract_fingerprint,
            &settings.expected_schema_fingerprint,
        )?;

        Ok(Self {
            data_root,
            expected_contract_fingerprint: settings.expected_contract_fingerprint.clone(),
            expected_schema_fingerprint: settings.expected_schema_fingerprint.clone(),
            default_page_size: settings.default_page_size,
            maximum_page_size: settings.maximum_page_size,
            maximum_payload_bytes: settings.maximum_payload_bytes,
            maximum_repeated_item_count: settings.maximum_repeated_item_count,
            operation_lease,
            outbox_lease,
            store_identities,
            resource_limits,
            retry_budget,
            root_operation_history_max_window,
            operation_history_v1_compat_enabled,
        })
    }

    pub(crate) fn manager_identity(&self) -> &StoreIdentity {
        self.store_identities
            .iter()
            .find(|identity| identity.role == StoreRole::Manager)
            .expect("validated settings always contain manager identity")
    }

    pub(crate) fn taskchampion_identity(&self) -> &StoreIdentity {
        self.store_identities
            .iter()
            .find(|identity| identity.role == StoreRole::TaskChampion)
            .expect("validated settings always contain TaskChampion identity")
    }
}

fn duration_from_proto(
    value: Option<&manager_v1::TimeSpan>,
    name: &str,
) -> Result<Duration, ManagerConfigError> {
    let millis = value
        .map(|span| span.millis)
        .filter(|millis| *millis > 0)
        .ok_or_else(|| ManagerConfigError::Invalid(format!("{name} must be positive")))?;
    Ok(Duration::from_millis(millis))
}

fn optional_duration_from_proto(
    value: Option<&manager_v1::TimeSpan>,
    name: &str,
    default: Duration,
) -> Result<Duration, ManagerConfigError> {
    match value {
        Some(value) => duration_from_proto(Some(value), name),
        None => Ok(default),
    }
}

fn resource_limit(resources: &manager_v1::ManagerResourceLimits, name: &str) -> u64 {
    match name {
        "max_string_bytes" => resources.max_string_bytes,
        "max_body_bytes" => resources.max_body_bytes,
        "max_uri_bytes" => resources.max_uri_bytes,
        "max_repeated_item_count" => resources.max_repeated_item_count,
        "max_page_size" => resources.max_page_size,
        _ => 0,
    }
}

fn validate_store_identities(
    identities: &[StoreIdentity],
    expected_contract: &str,
    expected_schema: &str,
) -> Result<(), ManagerConfigError> {
    if identities.len() != 2 {
        return Err(ManagerConfigError::Invalid(
            "exactly one manager and one TaskChampion identity are required".into(),
        ));
    }
    let manager = identities
        .iter()
        .find(|item| item.role == StoreRole::Manager);
    let taskchampion = identities
        .iter()
        .find(|item| item.role == StoreRole::TaskChampion);
    let (Some(manager), Some(taskchampion)) = (manager, taskchampion) else {
        return Err(ManagerConfigError::Invalid(
            "manager and TaskChampion store identities are required".into(),
        ));
    };
    if identities.iter().any(|item| {
        item.data_root_id.is_empty()
            || item.store_id.is_empty()
            || item.store_pair_id.is_empty()
            || item.schema_fingerprint.is_empty()
            || item.contract_fingerprint.is_empty()
    }) {
        return Err(ManagerConfigError::Invalid(
            "store identity fields are required".into(),
        ));
    }
    if manager.data_root_id != taskchampion.data_root_id
        || manager.store_pair_id != taskchampion.store_pair_id
    {
        return Err(ManagerConfigError::Invalid(
            "store identities must share data_root_id and store_pair_id".into(),
        ));
    }
    if manager.contract_fingerprint != expected_contract
        || manager.schema_fingerprint != expected_schema
    {
        return Err(ManagerConfigError::Invalid(
            "manager store identity does not match configured fingerprints".into(),
        ));
    }
    Ok(())
}

impl TryFrom<&manager_v1::StoreIdentity> for StoreIdentity {
    type Error = ManagerConfigError;

    fn try_from(value: &manager_v1::StoreIdentity) -> Result<Self, Self::Error> {
        let role = StoreRole::from_i32(value.role)
            .ok_or_else(|| ManagerConfigError::Invalid("store identity role is unknown".into()))?;
        Ok(Self {
            role,
            data_root_id: value.data_root_id.clone(),
            store_id: value.store_id.clone(),
            schema_fingerprint: value.schema_fingerprint.clone(),
            generation: value.generation,
            store_pair_id: value.store_pair_id.clone(),
            fencing_epoch: value.fencing_epoch,
            contract_fingerprint: value.contract_fingerprint.clone(),
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tempfile::tempdir;

    pub(crate) fn settings() -> manager_v1::IssueTrackerManagerSettings {
        let root = tempdir().expect("temporary root").keep();
        let limits = manager_v1::ManagerResourceLimits {
            max_string_bytes: 1,
            max_body_bytes: 1,
            max_uri_bytes: 1,
            max_repeated_item_count: 1,
            max_page_size: 1,
        };
        let manager = manager_v1::StoreIdentity {
            role: manager_v1::StoreRole::Manager as i32,
            data_root_id: "root".into(),
            store_id: "manager".into(),
            schema_fingerprint: "schema".into(),
            store_pair_id: "pair".into(),
            contract_fingerprint: "contract".into(),
            ..Default::default()
        };
        let taskchampion = manager_v1::StoreIdentity {
            role: manager_v1::StoreRole::Taskchampion as i32,
            data_root_id: "root".into(),
            store_id: "taskchampion".into(),
            schema_fingerprint: "task-schema".into(),
            store_pair_id: "pair".into(),
            contract_fingerprint: "task-contract".into(),
            ..Default::default()
        };
        manager_v1::IssueTrackerManagerSettings {
            data_root: root.to_string_lossy().into_owned(),
            expected_contract_fingerprint: "contract".into(),
            expected_schema_fingerprint: "schema".into(),
            default_page_size: 10,
            maximum_page_size: 20,
            maximum_payload_bytes: 1,
            maximum_repeated_item_count: 1,
            operation_lease: Some(manager_v1::TimeSpan { millis: 1 }),
            outbox_lease: Some(manager_v1::TimeSpan { millis: 1 }),
            resource_limits: Some(limits),
            retry_budget: Some(manager_v1::RetryBudget {
                max_attempts: 1,
                base_backoff: Some(manager_v1::TimeSpan { millis: 1 }),
                max_backoff: Some(manager_v1::TimeSpan { millis: 1 }),
            }),
            store_identities: vec![manager, taskchampion],
            secret_filter_mode: manager_v1::SecretFilterMode::FailClosed as i32,
            root_operation_history_max_window: None,
            operation_history_v1_compat_enabled: None,
        }
    }

    #[test]
    fn validates_fail_closed_settings_and_store_pair() {
        let config = ManagerConfig::from_proto(&settings()).expect("valid manager settings");
        assert_eq!(config.store_identities.len(), 2);
        assert_eq!(config.manager_identity().store_id, "manager");
        assert_eq!(
            config.root_operation_history_max_window,
            DEFAULT_ROOT_OPERATION_HISTORY_MAX_WINDOW
        );
        assert_eq!(config.page_size_limits().resolve(0), 1);
        assert_eq!(config.page_size_limits().resolve(u32::MAX), 1);
    }

    #[test]
    fn uses_configured_history_window() {
        let mut value = settings();
        value.root_operation_history_max_window = Some(manager_v1::TimeSpan { millis: 456 });

        let config = ManagerConfig::from_proto(&value).expect("valid configured windows");

        assert_eq!(
            config.root_operation_history_max_window,
            Duration::from_millis(456)
        );
    }

    #[test]
    fn operation_history_legacy_token_compatibility_defaults_on_and_can_be_disabled() {
        let mut value = settings();
        assert!(
            ManagerConfig::from_proto(&value)
                .expect("default compatibility")
                .operation_history_v1_compat_enabled
        );
        value.operation_history_v1_compat_enabled = Some(false);
        assert!(
            !ManagerConfig::from_proto(&value)
                .expect("disabled compatibility")
                .operation_history_v1_compat_enabled
        );
    }

    #[test]
    fn rejects_non_fail_closed_secret_filter_before_opening_root() {
        let mut value = settings();
        value.secret_filter_mode = manager_v1::SecretFilterMode::Disabled as i32;
        assert!(matches!(
            ManagerConfig::from_proto(&value),
            Err(ManagerConfigError::Invalid(message)) if message.contains("FAIL_CLOSED")
        ));
    }

    #[test]
    fn rejects_mismatched_store_pair_and_zero_resource_limit() {
        let mut value = settings();
        value.store_identities[1].store_pair_id = "other".into();
        assert!(ManagerConfig::from_proto(&value).is_err());

        let mut value = settings();
        value.resource_limits.as_mut().unwrap().max_body_bytes = 0;
        assert!(ManagerConfig::from_proto(&value).is_err());
    }
}
