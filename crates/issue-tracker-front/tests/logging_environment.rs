mod support;

use std::fs;

use support::{USAGE, run_server, workspace_with_dotenv_logging};

#[test]
fn loads_log_file_configuration_from_dotenv_before_argument_validation() {
    let (workspace, log_directory) = workspace_with_dotenv_logging();

    let output = run_server(workspace.path(), &[]);

    assert!(!output.status.success(), "missing arguments must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(USAGE),
        "missing arguments must report usage"
    );
    let files = fs::read_dir(&log_directory)
        .expect("dotenv log directory must be created")
        .collect::<Result<Vec<_>, _>>()
        .expect("read log directory");
    assert!(
        files.iter().any(|entry| entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "log")),
        "dotenv configuration must create a log file before argument validation"
    );
}
