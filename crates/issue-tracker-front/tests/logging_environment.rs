use std::{fs, process::Command};

#[test]
fn loads_log_file_configuration_from_dotenv_before_argument_validation() {
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

    let output = Command::new(env!("CARGO_BIN_EXE_issue-tracker-server"))
        .current_dir(workspace.path())
        .env_remove("LOG_FILE_DIR")
        .env_remove("LOG_USE_JSON")
        .env_remove("LOG_USE_STDOUT")
        .env_remove("OTLP_ADDR")
        .env_remove("OTLP_HTTP_ADDR")
        .env_remove("OTLP_AUTH_TOKEN")
        .env_remove("OTLP_LOG_PROTOCOL")
        .env_remove("OTLP_LOG_ENDPOINT")
        .output()
        .expect("start issue tracker server");

    assert!(!output.status.success(), "missing arguments must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("usage: issue-tracker-server --config <path>"),
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
