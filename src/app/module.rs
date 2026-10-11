use crate::{
    manager::IssueTrackerManager,
    tracker::{
        Tracker,
        config::RunnerConfig,
        dispatch::dispatch_v1_method,
        outbox_driver::{MaintenanceHandle, OutboxDriver},
    },
};
use prost::Message;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;

/// Storage-backed collaborators created by the infrastructure layer.
pub struct AppResources {
    tracker: Tracker,
    config: RunnerConfig,
    manager: Option<Arc<Mutex<IssueTrackerManager>>>,
    outbox_driver: Option<Arc<OutboxDriver>>,
    maintenance: Option<MaintenanceHandle>,
}

impl AppResources {
    /// Combine the already-open persistent resources for one application instance.
    pub fn new(
        tracker: Tracker,
        config: RunnerConfig,
        manager: Option<Arc<Mutex<IssueTrackerManager>>>,
        outbox_driver: Option<Arc<OutboxDriver>>,
    ) -> Result<Self, String> {
        if manager.is_some() != outbox_driver.is_some() {
            return Err("manager and outbox driver must be configured together".into());
        }
        Ok(Self {
            tracker,
            config,
            manager,
            outbox_driver,
            maintenance: None,
        })
    }

    /// Start the resident maintenance loop for a static runner. The loop is
    /// intentionally explicit so infrastructure can finish composition
    /// before any background write begins.
    pub fn start_resident_maintenance(&mut self) -> Result<(), String> {
        if self.maintenance.is_some() {
            return Ok(());
        }
        let Some(driver) = self.outbox_driver.as_ref() else {
            return Ok(());
        };
        self.maintenance = Some(driver.spawn_resident_maintenance(
            Duration::from_millis(self.config.ownership_maintenance_interval_millis),
            self.config.ownership_recovery_batch_size,
        )?);
        Ok(())
    }

    /// Join resident maintenance before dropping the tracker/manager roots.
    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(handle) = self.maintenance.take() {
            handle.shutdown().await?;
        }
        Ok(())
    }

    pub fn tracker(&self) -> &Tracker {
        &self.tracker
    }
    pub fn manager(&self) -> Option<&Arc<Mutex<IssueTrackerManager>>> {
        self.manager.as_ref()
    }
    pub fn outbox_driver(&self) -> Option<&Arc<OutboxDriver>> {
        self.outbox_driver.as_ref()
    }
}

/// Application-layer entry point for the canonical issue tracker use cases.
///
/// The module receives storage-backed dependencies in its constructor so the
/// front end cannot instantiate or access TaskChampion directly.
pub struct AppModule {
    tracker: Tracker,
    config: RunnerConfig,
    manager: Option<Arc<Mutex<IssueTrackerManager>>>,
    outbox_driver: Option<Arc<OutboxDriver>>,
    maintenance: Option<MaintenanceHandle>,
}

impl AppModule {
    pub fn new(tracker: Tracker, config: RunnerConfig) -> Self {
        Self {
            tracker,
            config,
            manager: None,
            outbox_driver: None,
            maintenance: None,
        }
    }

    /// Construct the application from resources opened by infrastructure.
    pub fn from_resources(resources: AppResources) -> Self {
        let AppResources {
            tracker,
            config,
            manager,
            outbox_driver,
            maintenance,
        } = resources;
        Self {
            tracker,
            config,
            manager,
            outbox_driver,
            maintenance,
        }
    }

    pub fn tracker(&self) -> &Tracker {
        &self.tracker
    }

    pub fn manager(&self) -> Option<&Arc<Mutex<IssueTrackerManager>>> {
        self.manager.as_ref()
    }

    pub fn outbox_driver(&self) -> Option<&Arc<OutboxDriver>> {
        self.outbox_driver.as_ref()
    }

    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(handle) = self.maintenance.take() {
            handle.shutdown().await?;
        }
        Ok(())
    }

    pub async fn dispatch_v1<A, R>(&self, method: &str, request: A) -> Result<R, String>
    where
        A: Message,
        R: Message + Default,
    {
        let response = dispatch_v1_method(
            method,
            &request.encode_to_vec(),
            &self.tracker,
            &self.config,
        )
        .await?;
        R::decode(response.as_slice()).map_err(|error| error.to_string())
    }
}
