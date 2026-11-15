//! Helpers shared by the tests that start the server binary.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

pub const USAGE: &str = "usage: issue-tracker-server --config <path>";

/// Runs the server binary in `workspace` with the host's logging and OTLP
/// settings removed, so only the workspace `.env` configures it.
pub fn run_server(workspace: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_issue-tracker-server"))
        .args(arguments)
        .current_dir(workspace)
        .env_remove("LOG_FILE_DIR")
        .env_remove("LOG_USE_JSON")
        .env_remove("LOG_USE_STDOUT")
        .env_remove("OTLP_ADDR")
        .env_remove("OTLP_HTTP_ADDR")
        .env_remove("OTLP_AUTH_TOKEN")
        .env_remove("OTLP_LOG_PROTOCOL")
        .env_remove("OTLP_LOG_ENDPOINT")
        .output()
        .expect("start issue tracker server")
}

/// A workspace whose `.env` points logging at the returned directory, so a
/// test can tell whether the server initialized logging.
pub fn workspace_with_dotenv_logging() -> (tempfile::TempDir, PathBuf) {
    let workspace = tempfile::tempdir().expect("create temporary workspace");
    let log_directory = workspace.path().join("logs");
    fs::write(
        workspace.path().join(".env"),
        format!(
            "LOG_FILE_DIR={}\nLOG_USE_JSON=false\nLOG_USE_STDOUT=false\n",
            log_directory.display()
        ),
    )
    .expect("write dotenv file");
    (workspace, log_directory)
}
