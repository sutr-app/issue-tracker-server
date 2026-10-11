//! Filesystem operations and validation for backup and export artifacts.

use super::{
    ENTITY_KEY, ID_KEY, IssueState, MAPPING_SCHEMA_VERSION, METADATA_ENTITY, SCHEMA_VERSION_KEY,
    STATE_KEY, Tracker, TrackerError, conversion::instant, now, revision, validate_issue_task,
};
use crate::{tracker::config::contract_fingerprint, v1};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};
use taskchampion::{Replica, SqliteStorage, storage::AccessMode};

pub(crate) fn file_digest(path: &Path) -> Result<String, String> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(fs::read(path).map_err(|error| error.to_string())?)
    ))
}
pub(crate) fn checkpoint_sqlite(path: &Path) -> Result<(), TrackerError> {
    let connection = rusqlite::Connection::open(path)
        .map_err(|error| TrackerError::Backup(format!("open backup connection: {error}")))?;
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|error| TrackerError::Backup(format!("checkpoint SQLite: {error}")))
}
pub(crate) async fn verify_storage_root(root: &Path) -> Result<(), TrackerError> {
    let storage = SqliteStorage::new(root, AccessMode::ReadWrite, true)
        .await
        .map_err(TrackerError::from)?;
    let mut replica = Replica::new(storage);
    let tasks = replica.all_tasks().await?;
    let metadata = tasks
        .values()
        .find(|task| task.get_user_defined_attribute(ENTITY_KEY) == Some(METADATA_ENTITY));
    if metadata.is_none()
        || metadata.and_then(|task| task.get_user_defined_attribute(SCHEMA_VERSION_KEY))
            != Some(MAPPING_SCHEMA_VERSION)
    {
        return Err(TrackerError::Backup(
            "restored metadata schema mismatch".into(),
        ));
    }
    for task in tasks
        .values()
        .filter(|task| task.get_user_defined_attribute(ID_KEY).is_some())
    {
        validate_issue_task(task)?;
        IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        revision(task)?;
    }
    Ok(())
}
pub(crate) fn move_artifacts(
    source: &Path,
    destination: &Path,
    remove_destination: bool,
) -> Result<(), TrackerError> {
    fs::create_dir_all(destination).map_err(|error| TrackerError::Backup(error.to_string()))?;
    for suffix in ["", "-wal", "-shm"] {
        let filename = format!("taskchampion.sqlite3{suffix}");
        let source_file = source.join(&filename);
        let destination_file = destination.join(&filename);
        if remove_destination && destination_file.exists() {
            fs::remove_file(&destination_file)
                .map_err(|error| TrackerError::Backup(error.to_string()))?;
        }
        if source_file.exists() {
            if destination_file.exists() {
                fs::remove_file(&destination_file)
                    .map_err(|error| TrackerError::Backup(error.to_string()))?;
            }
            fs::rename(&source_file, &destination_file)
                .map_err(|error| TrackerError::Backup(error.to_string()))?;
        } else if remove_destination && destination_file.exists() {
            fs::remove_file(destination_file)
                .map_err(|error| TrackerError::Backup(error.to_string()))?;
        }
    }
    Ok(())
}
pub(crate) fn scan_secret_artifact(path: &Path) -> Result<(), TrackerError> {
    let bytes = fs::read(path).map_err(|error| TrackerError::Backup(error.to_string()))?;
    scan_secret_bytes(&bytes)
}
pub(crate) fn scan_secret_bytes(bytes: &[u8]) -> Result<(), TrackerError> {
    let lower = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    let markers = [
        "sk-",
        "bearer ",
        "api_key",
        "secret=",
        "password=",
        "ghp_",
        "github_pat_",
        "xoxb-",
        "-----begin ",
    ];
    if markers.iter().any(|marker| lower.contains(marker)) {
        return Err(TrackerError::Backup(
            "secret-like value detected in backup artifact".into(),
        ));
    }
    Ok(())
}
pub(crate) fn reject_secret<'a>(
    values: impl IntoIterator<Item = &'a str>,
) -> Result<(), TrackerError> {
    for value in values {
        let lower = value.to_ascii_lowercase();
        if lower.contains("sk-")
            || lower.contains("bearer ")
            || lower.contains("api_key")
            || lower.contains("secret=")
            || lower.contains("token=")
            || lower.contains("password=")
            || lower.contains("ghp_")
            || lower.contains("github_pat_")
            || lower.contains("xoxb-")
            || value.starts_with("AKIA")
            || lower.contains("-----begin ")
        {
            return Err(TrackerError::SecretRejected);
        }
    }
    Ok(())
}
pub(crate) fn artifact_from_directory(
    root: &Path,
    path: &Path,
    artifact_id: &str,
    kind: v1::ArtifactKind,
) -> Result<v1::Artifact, TrackerError> {
    let mut digest_input = Vec::new();
    let mut size = 0u64;
    for entry in fs::read_dir(path).map_err(|error| TrackerError::Backup(error.to_string()))? {
        let entry = entry.map_err(|error| TrackerError::Backup(error.to_string()))?;
        if entry.path().is_file() {
            let bytes =
                fs::read(entry.path()).map_err(|error| TrackerError::Backup(error.to_string()))?;
            scan_secret_bytes(&bytes)?;
            size = size.saturating_add(bytes.len() as u64);
            digest_input.extend_from_slice(&bytes);
        }
    }
    let relative_path = path
        .strip_prefix(root)
        .map_err(|_| TrackerError::Backup("artifact path escaped data root".into()))?
        .to_string_lossy()
        .replace('\\', "/");
    Ok(v1::Artifact {
        artifact_id: artifact_id.to_owned(),
        relative_path,
        digest: format!("sha256:{:x}", Sha256::digest(digest_input)),
        size_bytes: size,
        created_at: Some(instant(now())),
        contract_fingerprint: contract_fingerprint(),
        storage_schema_fingerprint: Tracker::schema_fingerprint(),
        revision_watermark: 0,
        artifact_kind: kind as i32,
        format: if matches!(kind, v1::ArtifactKind::Backup) {
            "sqlite-directory"
        } else {
            "json"
        }
        .into(),
        media_type: if matches!(kind, v1::ArtifactKind::Backup) {
            "application/vnd.lookback.issue-backup"
        } else {
            "application/json"
        }
        .into(),
        format_version: "1".into(),
        validation_status: v1::ArtifactValidationStatus::Validated as i32,
    })
}
#[cfg(test)]
mod tests {
    use super::scan_secret_bytes;
    #[test]
    fn secret_scan_is_case_insensitive() {
        assert!(scan_secret_bytes(b"Authorization: BEARER value").is_err());
    }
}
