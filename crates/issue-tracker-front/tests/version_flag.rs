mod support;

use support::{USAGE, run_server, workspace_with_dotenv_logging};

#[test]
fn version_flag_prints_the_package_version() {
    let (workspace, _) = workspace_with_dotenv_logging();

    let output = run_server(workspace.path(), &["--version"]);

    assert!(output.status.success(), "--version must succeed");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("issue-tracker-server {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn version_flag_does_not_initialize_logging() {
    let (workspace, log_directory) = workspace_with_dotenv_logging();

    let output = run_server(workspace.path(), &["--version"]);

    assert!(output.status.success(), "--version must succeed");
    assert!(
        !log_directory.exists(),
        "--version must exit before loading .env and creating log files"
    );
}

#[test]
fn version_flag_with_extra_arguments_is_a_usage_error() {
    let (workspace, _) = workspace_with_dotenv_logging();

    let output = run_server(workspace.path(), &["--version", "extra"]);

    assert!(!output.status.success(), "extra arguments must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(USAGE),
        "extra arguments must report usage"
    );
}

#[test]
fn config_flag_without_path_is_still_a_usage_error() {
    let (workspace, _) = workspace_with_dotenv_logging();

    let output = run_server(workspace.path(), &["--config"]);

    assert!(!output.status.success(), "missing config path must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(USAGE),
        "missing config path must report usage"
    );
}
