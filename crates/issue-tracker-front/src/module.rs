use issue_tracker_app::server::{ServerSettings, serve};
use issue_tracker_infra::module::RepositoryModule;

/// Composition root for the local tonic process.
pub struct FrontModule {
    settings: ServerSettings,
    _repositories: RepositoryModule,
}

impl FrontModule {
    pub fn new(settings: ServerSettings) -> Result<Self, String> {
        let repositories = RepositoryModule::new(&settings.tracker.data_root)?;
        Ok(Self {
            settings,
            _repositories: repositories,
        })
    }

    pub async fn serve(self) -> Result<(), String> {
        serve(self.settings).await
    }
}
