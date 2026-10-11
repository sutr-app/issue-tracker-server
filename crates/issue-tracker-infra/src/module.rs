use issue_tracker_app::{
    AppResources, IssueTrackerManager, OutboxDriver, RunnerConfig, Tracker, manager, v1,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

/// Validated filesystem boundary used to construct persistent adapters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryModule {
    data_root: PathBuf,
}

impl RepositoryModule {
    /// Validate a root before any adapter creates external state beneath it.
    pub fn new(data_root: impl AsRef<Path>) -> Result<Self, String> {
        let data_root = data_root.as_ref();
        if !data_root.is_absolute() {
            return Err("data root must be absolute".into());
        }
        if let Ok(metadata) = std::fs::symlink_metadata(data_root)
            && metadata.file_type().is_symlink()
        {
            return Err("data root must not be a symlink".into());
        }
        Ok(Self {
            data_root: data_root.to_path_buf(),
        })
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    /// Open all durable collaborators used by one application instance.
    pub async fn open(
        &self,
        settings: &v1::IssueTrackerRunnerSettings,
    ) -> Result<AppResources, String> {
        let config = RunnerConfig::from_settings(settings)?;
        if config.data_root() != self.data_root() {
            return Err("repository root must match tracker.data_root".into());
        }
        let manager_config = settings
            .manager_settings
            .as_ref()
            .map(|settings| manager::manager_config_for_tracker(settings, config.data_root()))
            .transpose()?;
        if let Some(manager_config) = manager_config.as_ref() {
            IssueTrackerManager::preflight(manager_config).map_err(|error| error.to_string())?;
        }
        let tracker = Tracker::open(config.data_root())
            .await
            .map_err(|error| error.to_string())?;
        let manager = manager_config
            .map(IssueTrackerManager::open)
            .transpose()
            .map_err(|error| error.to_string())?
            .map(|manager| Arc::new(Mutex::new(manager)));
        let outbox_driver = match manager.as_ref() {
            Some(manager) => {
                let driver = Arc::new(
                    OutboxDriver::new(Arc::clone(manager), tracker.clone(), config.clone()).await,
                );
                let recovery_tracker = tracker.clone();
                let recovery_config = config.clone();
                let recovery_manager = Arc::clone(manager);
                driver.set_resolution_recovery_handler(move |limit| {
                    let tracker = recovery_tracker.clone();
                    let config = recovery_config.clone();
                    let manager = Arc::clone(&recovery_manager);
                    async move {
                        issue_tracker_app::server::recover_pending_resolutions_from_resources(
                            tracker, config, manager, limit,
                        )
                        .await
                    }
                })?;
                Some(driver)
            }
            None => None,
        };
        let mut resources = AppResources::new(tracker, config, manager, outbox_driver)?;
        resources.start_resident_maintenance()?;
        Ok(resources)
    }
}

#[cfg(test)]
mod tests {
    use super::RepositoryModule;
    use issue_tracker_app::{Tracker, manager, v1};
    use tempfile::TempDir;

    #[test]
    fn rejects_relative_data_roots_before_opening_adapters() {
        assert!(RepositoryModule::new("relative-root").is_err());
    }

    #[tokio::test]
    async fn opens_tracker_resources_for_application_dependency_injection() {
        let root = TempDir::new().expect("temporary data root");
        let repositories = RepositoryModule::new(root.path()).expect("validated root");

        let resources = repositories
            .open(&runner_settings(&root))
            .await
            .expect("persistent resources");

        assert!(resources.manager().is_none());
        assert!(resources.outbox_driver().is_none());
        assert_eq!(Tracker::schema_fingerprint(), Tracker::schema_fingerprint());
    }

    #[tokio::test]
    async fn joins_resident_maintenance_before_reopening_the_root() {
        let root = TempDir::new().expect("temporary data root");
        let repositories = RepositoryModule::new(root.path()).expect("validated root");
        let mut settings = runner_settings(&root);
        settings.manager_settings = Some(manager_settings(&root, Tracker::contract_fingerprint()));
        let resources = repositories.open(&settings).await.expect("open resources");
        assert!(resources.outbox_driver().is_some());
        resources.shutdown().await.expect("join maintenance");

        // A joined task has released all references to the old root lock.
        let reopened = repositories.open(&settings).await.expect("reopen root");
        reopened
            .shutdown()
            .await
            .expect("join reopened maintenance");
    }

    #[tokio::test]
    async fn resident_maintenance_recovers_an_expired_claim_before_shutdown() {
        use tokio::time::{Duration, Instant, sleep};

        let root = TempDir::new().expect("temporary data root");
        let repositories = RepositoryModule::new(root.path()).expect("validated root");
        let mut settings = runner_settings(&root);
        settings.lease_policy = Some(v1::LeasePolicy {
            default_lease: Some(v1::TimeSpan { millis: 1_000 }),
            minimum_lease: Some(v1::TimeSpan { millis: 1_000 }),
            maximum_lease: Some(v1::TimeSpan { millis: 2_000 }),
            maximum_heartbeat_interval: Some(v1::TimeSpan { millis: 300 }),
        });
        settings.ownership_maintenance_interval = Some(v1::TimeSpan { millis: 1_000 });
        settings.manager_settings = Some(manager_settings(&root, Tracker::contract_fingerprint()));
        let resources = repositories.open(&settings).await.expect("open resources");
        let issue = resources
            .tracker()
            .create("resident recovery", "resident recovery")
            .await
            .expect("create issue");
        resources
            .tracker()
            .claim(
                issue.id,
                "resident-owner",
                issue.revision,
                1,
                "resident-claim",
            )
            .await
            .expect("claim issue");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let current = resources.tracker().get(issue.id).await.expect("read issue");
            if current.state == issue_tracker_app::IssueState::Open {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "resident maintenance did not recover the claim"
            );
            sleep(Duration::from_millis(100)).await;
        }

        resources.shutdown().await.expect("join maintenance");
        let reopened = repositories
            .open(&settings)
            .await
            .expect("reopen after joined maintenance");
        let current = reopened
            .tracker()
            .get(issue.id)
            .await
            .expect("read recovered issue");
        assert_eq!(current.state, issue_tracker_app::IssueState::Open);
        assert!(current.lease_handle.is_none());
        reopened
            .shutdown()
            .await
            .expect("join reopened maintenance");
    }

    #[tokio::test]
    async fn dropping_resources_does_not_keep_the_resident_driver_alive() {
        use tokio::time::{Duration, timeout};

        let root = TempDir::new().expect("temporary data root");
        let repositories = RepositoryModule::new(root.path()).expect("validated root");
        let mut settings = runner_settings(&root);
        settings.manager_settings = Some(manager_settings(&root, Tracker::contract_fingerprint()));
        let resources = repositories.open(&settings).await.expect("open resources");
        drop(resources);

        let reopened = timeout(Duration::from_secs(2), repositories.open(&settings))
            .await
            .expect("resident task must not retain the dropped root")
            .expect("reopen root");
        reopened.shutdown().await.expect("join maintenance");
    }

    #[tokio::test]
    async fn rejects_mismatched_taskchampion_identity_before_tracker_open() {
        let root = TempDir::new().expect("temporary data root");
        let repositories = RepositoryModule::new(root.path()).expect("validated root");
        let mut settings = runner_settings(&root);
        settings.manager_settings = Some(manager_settings(&root, "wrong-task-contract"));

        let error = match repositories.open(&settings).await {
            Ok(_) => panic!("a mismatched paired identity must be rejected"),
            Err(error) => error,
        };
        assert!(error.contains("server-derived pair"));
        assert!(
            !root.path().join("taskchampion.sqlite3").exists(),
            "Tracker must not be opened after identity validation fails"
        );
    }

    #[tokio::test]
    async fn derives_pair_identity_when_manager_settings_omit_store_identities() {
        let root = TempDir::new().expect("temporary data root");
        let repositories = RepositoryModule::new(root.path()).expect("validated root");
        let mut settings = runner_settings(&root);
        let mut manager = manager_settings(&root, Tracker::contract_fingerprint());
        manager.store_identities.clear();
        settings.manager_settings = Some(manager);

        let resources = repositories
            .open(&settings)
            .await
            .expect("server-derived identities should be accepted");
        assert!(resources.manager().is_some());
        resources.shutdown().await.expect("join maintenance");
    }

    fn manager_settings(
        root: &TempDir,
        task_contract_fingerprint: impl Into<String>,
    ) -> issue_tracker_proto::manager_v1::IssueTrackerManagerSettings {
        let (root_id, pair_id) = manager::derive_store_identity_ids(root.path());
        issue_tracker_proto::manager_v1::IssueTrackerManagerSettings {
            data_root: root.path().to_string_lossy().into_owned(),
            default_page_size: 10,
            maximum_page_size: 20,
            maximum_payload_bytes: 65_536,
            maximum_repeated_item_count: 100,
            operation_lease: Some(issue_tracker_proto::manager_v1::TimeSpan { millis: 10_000 }),
            outbox_lease: Some(issue_tracker_proto::manager_v1::TimeSpan { millis: 10_000 }),
            resource_limits: Some(issue_tracker_proto::manager_v1::ManagerResourceLimits {
                max_string_bytes: 4096,
                max_body_bytes: 65_536,
                max_uri_bytes: 4096,
                max_repeated_item_count: 100,
                max_page_size: 100,
            }),
            retry_budget: Some(issue_tracker_proto::manager_v1::RetryBudget {
                max_attempts: 3,
                base_backoff: Some(issue_tracker_proto::manager_v1::TimeSpan { millis: 100 }),
                max_backoff: Some(issue_tracker_proto::manager_v1::TimeSpan { millis: 1_000 }),
            }),
            secret_filter_mode: issue_tracker_proto::manager_v1::SecretFilterMode::FailClosed
                as i32,
            store_identities: vec![
                issue_tracker_proto::manager_v1::StoreIdentity {
                    role: issue_tracker_proto::manager_v1::StoreRole::Manager as i32,
                    data_root_id: root_id.clone(),
                    store_id: "manager".into(),
                    schema_fingerprint:
                        issue_tracker_app::manager::MANAGER_STORAGE_SCHEMA_FINGERPRINT.into(),
                    store_pair_id: pair_id.clone(),
                    contract_fingerprint: manager::contract_fingerprint(),
                    ..Default::default()
                },
                issue_tracker_proto::manager_v1::StoreIdentity {
                    role: issue_tracker_proto::manager_v1::StoreRole::Taskchampion as i32,
                    data_root_id: root_id,
                    store_id: "taskchampion".into(),
                    schema_fingerprint: Tracker::schema_fingerprint(),
                    store_pair_id: pair_id,
                    contract_fingerprint: task_contract_fingerprint.into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    fn runner_settings(root: &TempDir) -> v1::IssueTrackerRunnerSettings {
        v1::IssueTrackerRunnerSettings {
            data_root: root.path().to_string_lossy().into_owned(),
            expected_contract_fingerprint: String::new(),
            expected_storage_schema_fingerprint: String::new(),
            lease_policy: Some(v1::LeasePolicy {
                default_lease: Some(v1::TimeSpan { millis: 30_000 }),
                minimum_lease: Some(v1::TimeSpan { millis: 10_000 }),
                maximum_lease: Some(v1::TimeSpan { millis: 60_000 }),
                maximum_heartbeat_interval: Some(v1::TimeSpan { millis: 1_000 }),
            }),
            pagination_policy: Some(v1::PaginationPolicy {
                default_page_size: 50,
                maximum_page_size: 100,
            }),
            secret_filter_mode: v1::SecretFilterMode::FailClosed as i32,
            resource_limits: Some(v1::ResourceLimits {
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
            }),
            manager_settings: None,
            ownership_maintenance_interval: None,
            ownership_recovery_batch_size: 0,
        }
    }
}
