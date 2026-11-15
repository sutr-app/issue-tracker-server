use dotenvy::dotenv;
use issue_tracker_app::server::ServerSettings;
use issue_tracker_front::FrontModule;

const USAGE: &str = "usage: issue-tracker-server --config <path>";

enum Invocation {
    Version,
    Serve { config: String },
    Invalid,
}

impl Invocation {
    fn parse(arguments: impl Iterator<Item = String>) -> Self {
        let arguments = arguments.collect::<Vec<_>>();
        match arguments.as_slice() {
            [flag] if flag == "--version" => Self::Version,
            [flag, config] if flag == "--config" => Self::Serve {
                config: config.clone(),
            },
            _ => Self::Invalid,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let invocation = Invocation::parse(std::env::args().skip(1));
    // Answered before .env and tracing so that checking the version of a
    // downloaded binary never creates log files in the working directory.
    if let Invocation::Version = invocation {
        println!("issue-tracker-server {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    // Usage errors are reported only after logging is configured, so they
    // reach the configured log outputs too.
    dotenv().ok();
    command_utils::util::tracing::init_from_env_and_filename("issue-tracker-server", "log")
        .await
        .map_err(|error| error.to_string())?;

    let result = match invocation {
        Invocation::Serve { config } => serve(config).await,
        Invocation::Version | Invocation::Invalid => Err(USAGE.into()),
    };

    command_utils::util::tracing::shutdown_tracer_provider();
    result
}

async fn serve(config: String) -> Result<(), String> {
    FrontModule::new(ServerSettings::load(config)?)?
        .serve()
        .await
}
