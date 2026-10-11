use dotenvy::dotenv;
use issue_tracker_app::server::ServerSettings;
use issue_tracker_front::FrontModule;

#[tokio::main]
async fn main() -> Result<(), String> {
    dotenv().ok();
    command_utils::util::tracing::init_from_env_and_filename("issue-tracker-server", "log")
        .await
        .map_err(|error| error.to_string())?;

    let result = async {
        let mut arguments = std::env::args().skip(1);
        let Some(flag) = arguments.next() else {
            return Err("usage: issue-tracker-server --config <path>".into());
        };
        if flag != "--config" {
            return Err("usage: issue-tracker-server --config <path>".into());
        }
        let Some(path) = arguments.next() else {
            return Err("usage: issue-tracker-server --config <path>".into());
        };
        if arguments.next().is_some() {
            return Err("usage: issue-tracker-server --config <path>".into());
        }
        FrontModule::new(ServerSettings::load(path)?)?.serve().await
    }
    .await;

    command_utils::util::tracing::shutdown_tracer_provider();
    result
}
