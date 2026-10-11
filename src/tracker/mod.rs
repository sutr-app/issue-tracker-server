//! TaskChampion-backed generic Issue tracker operations.

pub mod actor;
pub(crate) mod artifact_fs;
pub(crate) mod artifacts;
pub(crate) mod claims;
pub mod config;
pub(crate) mod conversion;
pub(crate) mod dispatch;
pub(crate) mod error_response;
pub(crate) mod handler;
pub(crate) mod issue_read;
pub(crate) mod issue_write;
pub(crate) mod lifecycle;
pub(crate) mod operation_log;
pub(crate) mod operation_response;
pub mod outbox_driver;
pub(crate) mod ownership_maintenance;
pub(crate) mod pagination;
pub(crate) mod relations;
pub(crate) mod storage;
pub(crate) mod task_mapping;
pub(crate) mod terminal_report;
#[cfg(test)]
pub(crate) mod tests;

pub(crate) use crate::model::IssueMetadata;
pub use crate::{
    error::TrackerError,
    model::{ExportBundle, Issue, IssueRelation, IssueState, RelationKind},
};
use crate::{proto_schema, v1};

use fs2::FileExt;
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use taskchampion::{Operations, Replica, SqliteStorage, Status, storage::AccessMode};
use tokio::sync::{Mutex, OwnedRwLockWriteGuard, RwLock};
use uuid::Uuid;

use config::{CONTRACT_VERSION, SourceEvidenceLimits};
const STATE_KEY: &str = "lb.state";
const REVISION_KEY: &str = "lb.revision";
const LEASE_OWNER_KEY: &str = "lb.lease_owner_hash";
const LEASE_OWNER_CONTEXT_KEY: &str = "lb.lease_owner_context";
const LEASE_EXPIRES_KEY: &str = "lb.lease_expires_at";
const LEASE_HANDLE_KEY: &str = "lb.lease_handle_hash";
const ID_KEY: &str = "lb.issue_id";
const ENTITY_KEY: &str = "lb.entity";
const SCHEMA_VERSION_KEY: &str = "lb.schema_version";
const ISSUE_METADATA_KEY: &str = "lb.issue.metadata";
const RELATION_METADATA_PREFIX: &str = "lb.relation.meta.";
const METADATA_ENTITY: &str = "tracker_metadata";
const ISSUE_ENTITY: &str = "issue";
// Marker result snapshots and structured precondition details are part of the
// persistent Tracker contract. A v4 root has a different mapping identity and
// must be rejected rather than interpreted as a v7 root after an in-place
// upgrade.
const MAPPING_SCHEMA_VERSION: &str = "7";
const SCHEMA_FINGERPRINT: &str = "lookback-issue-v7-taskchampion-3.1.0";
const PAGE_TOKEN_VERSION: &str = "v1";

#[derive(Clone)]
pub struct Tracker {
    inner: Arc<Mutex<Replica<SqliteStorage>>>,
    admission: Arc<RwLock<()>>,
    failed_closed: Arc<AtomicBool>,
    read_generation: Arc<AtomicU64>,
    page_token_secret: Arc<String>,
    root: Arc<PathBuf>,
    source_evidence_limits: SourceEvidenceLimits,
    _lock: Arc<RootLock>,
}

struct RootLock {
    path: PathBuf,
    _file: File,
}

impl Drop for RootLock {
    fn drop(&mut self) {
        let _ = self._file.unlock();
        let _ = fs::remove_file(&self.path);
    }
}

fn create_operation_id(idempotency_key: &str) -> String {
    format!("create:{idempotency_key}")
}
fn revision(task: &taskchampion::Task) -> Result<u64, TrackerError> {
    task.get_user_defined_attribute(REVISION_KEY)
        .unwrap_or("0")
        .parse()
        .map_err(|_| TrackerError::InvalidState("invalid revision".into()))
}
fn metadata_id() -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, b"lookback:issue-tracker:metadata:v1")
}
fn validate_issue_task(task: &taskchampion::Task) -> Result<(), TrackerError> {
    if task.get_user_defined_attribute(ENTITY_KEY) != Some(ISSUE_ENTITY)
        || task.get_user_defined_attribute(SCHEMA_VERSION_KEY) != Some(MAPPING_SCHEMA_VERSION)
    {
        return Err(TrackerError::InvalidState(
            "issue task schema mismatch".into(),
        ));
    }
    Ok(())
}

fn reaches(depmap: &taskchampion::DependencyMap, start: Uuid, target: Uuid) -> bool {
    let mut pending = vec![start];
    let mut visited = std::collections::HashSet::new();
    while let Some(current) = pending.pop() {
        if current == target {
            return true;
        }
        if visited.insert(current) {
            pending.extend(depmap.dependencies(current));
        }
    }
    false
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generation_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(1)
}
fn hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

/// Test-only escape hatch for constructing malformed persisted ownership
/// tuples. Production callers must use the validated write APIs; this helper
/// exists so maintenance diagnosis and recovery boundaries can be exercised
/// without exposing raw TaskChampion state in the public API.
#[cfg(test)]
impl Tracker {
    pub(crate) async fn set_user_defined_attribute_for_test(
        &self,
        id: Uuid,
        key: &str,
        value: &str,
    ) -> Result<(), TrackerError> {
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let mut operations = Operations::new();
        task.set_user_defined_attribute(key, value, &mut operations)?;
        replica.commit_operations(operations).await?;
        self.advance_read_generation();
        Ok(())
    }
}
