#![allow(clippy::redundant_pattern_matching)]

use super::{
    config::ManagerConfig,
    config::SourceEvidenceLimits,
    model::{ManagerCommit, ManagerState, StoreRole},
    operations::digest_bytes,
    repository::ManagerRepository,
};
use crate::proto_types::manager_v1 as pb;
use fs2::FileExt;
use prost::Message;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

// Candidate protobuf tags were deliberately reassigned when independent
// Evidence records were removed. Existing payloads must never be decoded by
// this storage format.
const SCHEMA_VERSION: i64 = 31;
const PREVIOUS_SCHEMA_VERSION: i64 = 30;
const PREVIOUS_SCHEMA_FINGERPRINT: &str = "issue-tracker-manager/v14/sqlite-schema-v30";
const LEGACY_SCHEMA_FINGERPRINT: &str = "issue-tracker-manager/v14/sqlite-schema-v29";
pub const MANAGER_STORAGE_SCHEMA_FINGERPRINT: &str = "issue-tracker-manager/v14/sqlite-schema-v31";
const LOCK_FILE: &str = "issue-tracker-manager.lock";
const DATABASE_FILE: &str = "issue-tracker-manager.sqlite3";
const LIST_ISSUES_WITH_SYNC_PAGE_TOKEN_SECRET_KEY: &str = "list_issues_with_sync_page_token_secret";
const LEGACY_MAPPING_BACKFILL_OPERATION_ID: &str = "migration-v6-legacy-mapping";
const MAPPING_INDEX_REBUILD_OPERATION_ID: &str = "migration-v30-rebuild-mapping-index";

#[derive(Debug, thiserror::Error)]
pub enum ManagerStorageError {
    #[error("manager root is invalid: {0}")]
    InvalidRoot(String),
    #[error("manager root is already locked")]
    RootLocked,
    #[error("manager database schema is incompatible: {0}")]
    SchemaMismatch(String),
    #[error("manager store identity is incompatible: {0}")]
    StoreIdentityMismatch(String),
    #[error("manager database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("manager filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("manager transaction failed: {0}")]
    Transaction(String),
}

struct RootLock {
    path: PathBuf,
    file: File,
}

impl Drop for RootLock {
    fn drop(&mut self) {
        release_root_lock(&self.path, &self.file);
    }
}

/// The manager's single-writer SQLite boundary.
pub struct ManagerStorage {
    #[allow(dead_code)]
    root: PathBuf,
    #[allow(dead_code)]
    database_path: PathBuf,
    backend_contract_fingerprint: String,
    source_evidence_limits: SourceEvidenceLimits,
    connection: Connection,
    _lock: RootLock,
    state: ManagerState,
    list_issues_with_sync_page_token_secret: String,
}

impl ManagerStorage {
    /// Validate a manager root without creating a directory, lock file, WAL,
    /// or migration backup. Application bootstrap calls this before opening
    /// the paired Tracker root so an incompatible Manager store cannot leave
    /// the other store partially initialized.
    pub(crate) fn preflight(config: &ManagerConfig) -> Result<(), ManagerStorageError> {
        if !config.data_root.exists() {
            return Ok(());
        }
        let metadata = fs::symlink_metadata(&config.data_root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ManagerStorageError::InvalidRoot(
                "manager root must be a regular directory".into(),
            ));
        }
        let database_path = config.data_root.join(DATABASE_FILE);
        if database_path.exists() {
            preflight_existing_database(&database_path, config)?;
        }
        Ok(())
    }

    pub(crate) fn open(config: &ManagerConfig) -> Result<Self, ManagerStorageError> {
        Self::open_with_migration_backup(config, |_| Ok(()))
    }

    /// Open the manager store after giving the caller a chance to create a
    /// verified backup of the existing database before migrations run.
    ///
    /// The callback is deliberately an API hook only: backup creation and its
    /// coordinated TaskChampion drain remain outside this storage layer.
    pub(crate) fn open_with_migration_backup<F>(
        config: &ManagerConfig,
        before_migration_backup: F,
    ) -> Result<Self, ManagerStorageError>
    where
        F: FnOnce(&Path) -> Result<(), ManagerStorageError>,
    {
        ensure_root(&config.data_root)?;
        let lock_path = config.data_root.join(LOCK_FILE);
        let lock_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| ManagerStorageError::RootLocked)?;

        let database_path = config.data_root.join(DATABASE_FILE);
        if database_path.exists() {
            if let Err(error) = preflight_existing_database(&database_path, config) {
                release_root_lock(&lock_path, &lock_file);
                return Err(error);
            }
        }
        let mut connection = match Connection::open(&database_path) {
            Ok(connection) => connection,
            Err(error) => {
                release_root_lock(&lock_path, &lock_file);
                return Err(error.into());
            }
        };
        let migrate_result = configure_connection(&mut connection).and_then(|()| {
            let current: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            if current == 0 {
                before_migration_backup(&database_path)?;
            }
            migrate(&mut connection, &config.expected_schema_fingerprint)
        });
        if let Err(error) = migrate_result {
            release_root_lock(&lock_path, &lock_file);
            return Err(error);
        }
        let state = match load_or_initialize_state(&mut connection, config) {
            Ok(state) => state,
            Err(error) => {
                release_root_lock(&lock_path, &lock_file);
                return Err(error);
            }
        };
        let list_issues_with_sync_page_token_secret =
            match read_list_issues_with_sync_page_token_secret(&connection) {
                Ok(secret) => secret,
                Err(error) => {
                    release_root_lock(&lock_path, &lock_file);
                    return Err(error);
                }
            };

        Ok(Self {
            root: config.data_root.clone(),
            database_path,
            backend_contract_fingerprint: config
                .taskchampion_identity()
                .contract_fingerprint
                .clone(),
            source_evidence_limits: config.source_evidence_limits(),
            connection,
            _lock: RootLock {
                path: lock_path,
                file: lock_file,
            },
            state,
            list_issues_with_sync_page_token_secret,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    #[allow(dead_code)]
    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) fn backend_contract_fingerprint(&self) -> &str {
        &self.backend_contract_fingerprint
    }

    pub(crate) fn source_evidence_limits(&self) -> SourceEvidenceLimits {
        self.source_evidence_limits
    }

    #[allow(dead_code)]
    pub(crate) fn state(&self) -> &ManagerState {
        &self.state
    }

    pub(crate) fn manager_sequence(&self) -> u64 {
        self.state.manager_sequence
    }

    pub(crate) fn generation(&self) -> u64 {
        self.state.generation
    }

    /// Returns the persisted key material used only to authenticate list page tokens.
    pub(crate) fn list_issues_with_sync_page_token_secret(&self) -> &str {
        &self.list_issues_with_sync_page_token_secret
    }

    pub(crate) fn repository(&mut self) -> ManagerRepository<'_> {
        ManagerRepository::new(self)
    }

    /// Read phase-health projections without taking the mutable repository
    /// facade. ContractInfo is available through an `&self` service handler,
    /// while all updates still go through ManagerRepository transactions.
    pub(crate) fn phase_health_snapshots(
        &self,
    ) -> Result<(u64, Vec<super::repository::PhaseHealthSnapshot>), ManagerStorageError> {
        self.read(|connection| {
            let epoch = connection
                .query_row(
                    "SELECT value FROM manager_metadata WHERE key='maintenance_health_epoch'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .and_then(|value| value.parse().ok())
                .unwrap_or(0_u64);
            let (contract, root): (String, String) = connection.query_row(
                "SELECT
                    (SELECT value FROM manager_metadata WHERE key='contract_fingerprint'),
                    (SELECT value FROM manager_metadata WHERE key='data_root_id')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let mut statement = connection.prepare(
                "SELECT phase, health, last_started_at, last_finished_at,
                        last_failure_at, last_recovered_at, attempt_count,
                        success_count, skipped_count, conflict_count,
                        diagnosis_count, unconfirmed_count, pending_gauge,
                        inconsistent_gauge, permit_enabled, gauge_stale,
                        snapshot_payload
                   FROM manager_phase_health
                  WHERE contract_fingerprint=?1 AND data_root_id=?2
                  ORDER BY phase",
            )?;
            let rows = statement.query_map(params![contract, root], |row| {
                let snapshot: Option<Vec<u8>> = row.get(16)?;
                let refs = snapshot
                    .as_deref()
                    .and_then(|value| std::str::from_utf8(value).ok())
                    .filter(|value| !value.is_empty())
                    .map(|value| value.split('\n').map(ToOwned::to_owned).collect())
                    .unwrap_or_default();
                Ok(super::repository::PhaseHealthSnapshot {
                    phase: row.get(0)?,
                    health: row.get(1)?,
                    last_started_at: row.get(2)?,
                    last_finished_at: row.get(3)?,
                    last_failure_at: row.get(4)?,
                    last_recovered_at: row.get(5)?,
                    attempt_count: row.get::<_, i64>(6)?.try_into().unwrap_or(0),
                    success_count: row.get::<_, i64>(7)?.try_into().unwrap_or(0),
                    skipped_count: row.get::<_, i64>(8)?.try_into().unwrap_or(0),
                    conflict_count: row.get::<_, i64>(9)?.try_into().unwrap_or(0),
                    diagnosis_count: row.get::<_, i64>(10)?.try_into().unwrap_or(0),
                    unconfirmed_count: row.get::<_, i64>(11)?.try_into().unwrap_or(0),
                    pending_gauge: row.get::<_, i64>(12)?.try_into().unwrap_or(0),
                    inconsistent_gauge: row.get::<_, i64>(13)?.try_into().unwrap_or(0),
                    permit_enabled: row.get::<_, i64>(14)? != 0,
                    gauge_stale: row.get::<_, i64>(15)? != 0,
                    diagnosis_refs: refs,
                })
            })?;
            Ok((epoch, rows.collect::<Result<Vec<_>, _>>()?))
        })
    }

    #[allow(dead_code)]
    pub(crate) fn fencing_epoch(&self) -> u64 {
        self.state.fencing_epoch
    }

    /// Execute one manager write and advance both durable counters atomically.
    #[allow(dead_code)]
    pub(crate) fn write<T, F>(
        &mut self,
        operation: F,
    ) -> Result<(T, ManagerCommit), ManagerStorageError>
    where
        F: FnOnce(&Transaction<'_>, ManagerCommit) -> Result<T, ManagerStorageError>,
    {
        self.write_with(operation)
    }

    pub(crate) fn write_with<T, E, F>(&mut self, operation: F) -> Result<(T, ManagerCommit), E>
    where
        E: From<ManagerStorageError>,
        F: FnOnce(&Transaction<'_>, ManagerCommit) -> Result<T, E>,
    {
        let next = ManagerCommit {
            manager_sequence: self.state.manager_sequence.checked_add(1).ok_or_else(|| {
                E::from(ManagerStorageError::Transaction(
                    "manager sequence overflow".into(),
                ))
            })?,
            generation: self.state.generation.checked_add(1).ok_or_else(|| {
                E::from(ManagerStorageError::Transaction(
                    "manager generation overflow".into(),
                ))
            })?,
        };
        let transaction = self
            .connection
            .transaction()
            .map_err(ManagerStorageError::from)
            .map_err(E::from)?;
        update_counter(&transaction, "manager_sequence", next.manager_sequence).map_err(E::from)?;
        update_counter(&transaction, "generation", next.generation).map_err(E::from)?;
        let result = operation(&transaction, next)?;
        transaction
            .commit()
            .map_err(ManagerStorageError::from)
            .map_err(E::from)?;
        self.state.manager_sequence = next.manager_sequence;
        self.state.generation = next.generation;
        Ok((result, next))
    }

    /// Execute a manager-local projection write without advancing the manager
    /// mutation boundary, so telemetry cannot invalidate data-read continuations.
    /// Do not call this from a `write_with` transaction closure because rusqlite rejects nested transactions.
    pub(crate) fn write_untracked<T, E, F>(&mut self, operation: F) -> Result<T, E>
    where
        E: From<ManagerStorageError>,
        F: FnOnce(&Transaction<'_>) -> Result<T, E>,
    {
        let transaction = self
            .connection
            .transaction()
            .map_err(ManagerStorageError::from)
            .map_err(E::from)?;
        let result = operation(&transaction)?;
        transaction
            .commit()
            .map_err(ManagerStorageError::from)
            .map_err(E::from)?;
        Ok(result)
    }

    pub(crate) fn read<T, F>(&self, reader: F) -> Result<T, ManagerStorageError>
    where
        F: FnOnce(&Connection) -> Result<T, ManagerStorageError>,
    {
        reader(&self.connection)
    }

    #[cfg(test)]
    pub(crate) fn raw_query_i64(&self, sql: &str) -> Result<i64, ManagerStorageError> {
        Ok(self.connection.query_row(sql, [], |row| row.get(0))?)
    }
}

fn release_root_lock(lock_path: &Path, lock_file: &File) {
    let _ = lock_file.unlock();
    let _ = fs::remove_file(lock_path);
}

fn ensure_root(root: &Path) -> Result<(), ManagerStorageError> {
    if !root.is_absolute() {
        return Err(ManagerStorageError::InvalidRoot(
            "manager root must be absolute".into(),
        ));
    }
    if root.exists() && fs::symlink_metadata(root)?.file_type().is_symlink() {
        return Err(ManagerStorageError::InvalidRoot(
            "manager root must not be a symlink".into(),
        ));
    }
    fs::create_dir_all(root)?;
    if !root.is_dir() {
        return Err(ManagerStorageError::InvalidRoot(
            "manager root must be a directory".into(),
        ));
    }
    Ok(())
}

fn configure_connection(connection: &mut Connection) -> Result<(), ManagerStorageError> {
    connection.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;
         PRAGMA busy_timeout = 5000;",
    )?;
    Ok(())
}

/// Reject incompatible persisted roots before a writable connection can run a
/// migration or update journal state. The immediately preceding v29 root is
/// accepted only so the v30 mapping-index rebuild and v31 history projection
/// migration can run; older contract roots remain incompatible with this
/// server.
fn preflight_existing_database(
    database_path: &Path,
    config: &ManagerConfig,
) -> Result<(), ManagerStorageError> {
    if !database_path.is_file() {
        return Err(ManagerStorageError::InvalidRoot(
            "manager database path must be a regular file".into(),
        ));
    }
    let connection = Connection::open_with_flags(database_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let current: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if current == 0 {
        let table_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_master
              WHERE type IN ('table', 'view', 'trigger', 'index')
                AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if table_count == 0 {
            return Ok(());
        }
        return Err(ManagerStorageError::StoreIdentityMismatch(
            "existing manager database is not an empty v14 root".into(),
        ));
    }
    if current != SCHEMA_VERSION && current != PREVIOUS_SCHEMA_VERSION && current != 29 {
        return Err(ManagerStorageError::StoreIdentityMismatch(format!(
            "existing manager database schema v{current} is not contract v14 schema v{SCHEMA_VERSION}"
        )));
    }
    let migrating_previous_schema = current < SCHEMA_VERSION;
    let source_schema_fingerprint = match current {
        29 => LEGACY_SCHEMA_FINGERPRINT,
        30 => PREVIOUS_SCHEMA_FINGERPRINT,
        _ => config.expected_schema_fingerprint.as_str(),
    };
    let metadata_exists: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master
              WHERE type = 'table' AND name = 'manager_metadata'
         )",
        [],
        |row| row.get(0),
    )?;
    if !metadata_exists {
        return Err(ManagerStorageError::StoreIdentityMismatch(
            "existing v14 manager database is missing metadata".into(),
        ));
    }
    for (key, expected) in [
        (
            "data_root_id",
            config.manager_identity().data_root_id.as_str(),
        ),
        (
            "manager_store_id",
            config.manager_identity().store_id.as_str(),
        ),
        (
            "taskchampion_store_id",
            config.taskchampion_identity().store_id.as_str(),
        ),
        (
            "store_pair_id",
            config.manager_identity().store_pair_id.as_str(),
        ),
        (
            "contract_fingerprint",
            config.expected_contract_fingerprint.as_str(),
        ),
        ("schema_fingerprint", source_schema_fingerprint),
    ] {
        let actual: Option<String> = connection
            .query_row(
                "SELECT value FROM manager_metadata WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;
        if actual.as_deref() != Some(expected) {
            return Err(ManagerStorageError::StoreIdentityMismatch(format!(
                "existing manager database metadata {key} is incompatible"
            )));
        }
    }

    let identity_table_exists: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master
              WHERE type = 'table' AND name = 'manager_store_identity'
         )",
        [],
        |row| row.get(0),
    )?;
    if !identity_table_exists {
        return Err(ManagerStorageError::StoreIdentityMismatch(
            "existing v14 manager database is missing store identities".into(),
        ));
    }
    for identity in [config.manager_identity(), config.taskchampion_identity()] {
        let actual: Option<(String, String, String, i64, String, i64, String)> = connection
            .query_row(
                "SELECT data_root_id, store_id, schema_fingerprint, generation,
                        store_pair_id, fencing_epoch, contract_fingerprint
                   FROM manager_store_identity
                  WHERE role = ?1",
                params![identity.role.as_i32()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            data_root_id,
            store_id,
            schema_fingerprint,
            generation,
            store_pair_id,
            fencing_epoch,
            contract_fingerprint,
        )) = actual
        else {
            return Err(ManagerStorageError::StoreIdentityMismatch(format!(
                "existing manager database is missing {:?} store identity",
                identity.role
            )));
        };
        let expected_generation = i64::try_from(identity.generation).map_err(|_| {
            ManagerStorageError::StoreIdentityMismatch(
                "configured store generation is too large".into(),
            )
        })?;
        let expected_fencing_epoch = i64::try_from(identity.fencing_epoch).map_err(|_| {
            ManagerStorageError::StoreIdentityMismatch(
                "configured fencing epoch is too large".into(),
            )
        })?;
        let expected_identity_schema =
            if migrating_previous_schema && identity.role == StoreRole::Manager {
                source_schema_fingerprint
            } else {
                identity.schema_fingerprint.as_str()
            };
        if data_root_id != identity.data_root_id
            || store_id != identity.store_id
            || schema_fingerprint != expected_identity_schema
            || generation != expected_generation
            || store_pair_id != identity.store_pair_id
            || fencing_epoch != expected_fencing_epoch
            || contract_fingerprint != identity.contract_fingerprint
        {
            return Err(ManagerStorageError::StoreIdentityMismatch(format!(
                "existing {:?} store identity does not match configured identity",
                identity.role
            )));
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn migration_backup_path(database_path: &Path, source_version: i64) -> PathBuf {
    database_path.with_file_name(format!(
        "{DATABASE_FILE}.pre-migration-v{source_version}.bak"
    ))
}

/// Create a standalone SQLite snapshot before an irreversible schema change.
/// `VACUUM INTO` takes a consistent snapshot even when the source uses WAL.
fn migrate(
    connection: &mut Connection,
    expected_schema_fingerprint: &str,
) -> Result<(), ManagerStorageError> {
    let current: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if current > SCHEMA_VERSION {
        return Err(ManagerStorageError::SchemaMismatch(format!(
            "database version {current} is newer than supported version {SCHEMA_VERSION}"
        )));
    }
    if current < 1 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_metadata (
             key TEXT PRIMARY KEY NOT NULL,
             value TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS manager_store_identity (
             role INTEGER PRIMARY KEY NOT NULL,
             data_root_id TEXT NOT NULL,
             store_id TEXT NOT NULL,
             schema_fingerprint TEXT NOT NULL,
             generation INTEGER NOT NULL,
             store_pair_id TEXT NOT NULL,
             fencing_epoch INTEGER NOT NULL,
             contract_fingerprint TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS manager_entity_revision (
             entity_kind TEXT NOT NULL,
             entity_id TEXT NOT NULL,
             revision INTEGER NOT NULL,
             PRIMARY KEY (entity_kind, entity_id)
         );
         CREATE TABLE IF NOT EXISTS manager_idempotency (
             idempotency_scope TEXT PRIMARY KEY NOT NULL,
             request_digest TEXT NOT NULL,
             operation_id TEXT NOT NULL UNIQUE,
             state INTEGER NOT NULL,
             result_snapshot BLOB,
             error_snapshot BLOB,
             created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS manager_operation (
             operation_id TEXT PRIMARY KEY NOT NULL,
             method TEXT NOT NULL,
             idempotency_scope TEXT NOT NULL,
             request_digest TEXT NOT NULL,
             state INTEGER NOT NULL,
             attempt INTEGER NOT NULL,
             manager_sequence INTEGER NOT NULL,
             generation INTEGER NOT NULL,
             causal_parent TEXT,
             sync_subject TEXT,
             affects_ready INTEGER NOT NULL,
             snapshot BLOB,
             error_snapshot BLOB,
             created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS manager_outbox (
             outbox_id TEXT PRIMARY KEY NOT NULL,
             operation_id TEXT NOT NULL UNIQUE,
             topic TEXT NOT NULL,
             status INTEGER NOT NULL,
             payload_snapshot_ref TEXT NOT NULL,
             payload_digest TEXT NOT NULL,
             generation INTEGER NOT NULL,
             attempt INTEGER NOT NULL,
             lease_id TEXT,
             lease_expires_at INTEGER,
             next_attempt_at INTEGER,
             created_at INTEGER NOT NULL
         );
         PRAGMA user_version = 1;",
        )?;
        transaction.commit()?;
    }
    if current < 2 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_entity (
                 entity_kind TEXT NOT NULL CHECK (entity_kind IN
                     ('candidate', 'evidence', 'binding', 'extraction_run',
                      'issue_mapping', 'backup', 'export', 'tombstone')),
                 entity_id TEXT NOT NULL,
                 revision INTEGER NOT NULL CHECK (revision >= 0),
                 payload BLOB NOT NULL,
                 payload_schema TEXT NOT NULL,
                 payload_digest TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (entity_kind, entity_id)
             );
             CREATE INDEX IF NOT EXISTS manager_entity_kind_revision
                 ON manager_entity (entity_kind, revision, entity_id);
             CREATE INDEX IF NOT EXISTS manager_entity_digest
                 ON manager_entity (payload_digest);
             CREATE VIEW IF NOT EXISTS manager_candidate AS
                 SELECT entity_id AS candidate_id, revision, payload, payload_schema,
                        payload_digest, created_at, updated_at
                   FROM manager_entity WHERE entity_kind = 'candidate';
             CREATE VIEW IF NOT EXISTS manager_evidence AS
                 SELECT entity_id AS evidence_id, revision, payload, payload_schema,
                        payload_digest, created_at, updated_at
                   FROM manager_entity WHERE entity_kind = 'evidence';
             CREATE VIEW IF NOT EXISTS manager_binding AS
                 SELECT entity_id AS binding_id, revision, payload, payload_schema,
                        payload_digest, created_at, updated_at
                   FROM manager_entity WHERE entity_kind = 'binding';
             CREATE VIEW IF NOT EXISTS manager_extraction_run AS
                 SELECT entity_id AS run_id, revision, payload, payload_schema,
                        payload_digest, created_at, updated_at
                   FROM manager_entity WHERE entity_kind = 'extraction_run';
             CREATE VIEW IF NOT EXISTS manager_issue_mapping AS
                 SELECT entity_id AS issue_id, revision, payload, payload_schema,
                        payload_digest, created_at, updated_at
                   FROM manager_entity WHERE entity_kind = 'issue_mapping';
             CREATE VIEW IF NOT EXISTS manager_backup AS
                 SELECT entity_id AS backup_id, revision, payload, payload_schema,
                        payload_digest, created_at, updated_at
                   FROM manager_entity WHERE entity_kind = 'backup';
             CREATE VIEW IF NOT EXISTS manager_export AS
                 SELECT entity_id AS export_id, revision, payload, payload_schema,
                        payload_digest, created_at, updated_at
                   FROM manager_entity WHERE entity_kind = 'export';
             CREATE TABLE IF NOT EXISTS manager_operation_scope (
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 candidate_id TEXT,
                 evidence_id TEXT,
                 extraction_run_id TEXT,
                 issue_id TEXT,
                 backup_id TEXT,
                 export_id TEXT,
                 FOREIGN KEY (operation_id) REFERENCES manager_operation(operation_id)
             );
             CREATE INDEX IF NOT EXISTS manager_operation_scope_candidate
                 ON manager_operation_scope(candidate_id);
             CREATE INDEX IF NOT EXISTS manager_operation_scope_issue
                 ON manager_operation_scope(issue_id);
             CREATE TABLE IF NOT EXISTS manager_tombstone (
                 tombstone_id TEXT PRIMARY KEY NOT NULL,
                 entity_id TEXT NOT NULL UNIQUE,
                 revision INTEGER NOT NULL,
                 payload BLOB NOT NULL,
                 payload_schema TEXT NOT NULL,
                 payload_digest TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             ALTER TABLE manager_operation ADD COLUMN snapshot_schema TEXT;
             ALTER TABLE manager_operation ADD COLUMN snapshot_digest TEXT;
             ALTER TABLE manager_operation ADD COLUMN result_snapshot BLOB;
             ALTER TABLE manager_operation ADD COLUMN result_snapshot_schema TEXT;
             ALTER TABLE manager_operation ADD COLUMN result_snapshot_digest TEXT;
             ALTER TABLE manager_operation ADD COLUMN error_snapshot_schema TEXT;
             ALTER TABLE manager_operation ADD COLUMN error_snapshot_digest TEXT;
             ALTER TABLE manager_operation ADD COLUMN operation_payload BLOB;
             ALTER TABLE manager_operation ADD COLUMN operation_payload_schema TEXT;
             ALTER TABLE manager_operation ADD COLUMN operation_payload_digest TEXT;
             ALTER TABLE manager_outbox ADD COLUMN dispatched_at INTEGER;
             ALTER TABLE manager_outbox ADD COLUMN acknowledged_at INTEGER;
             ALTER TABLE manager_outbox ADD COLUMN error_code INTEGER NOT NULL DEFAULT 0;
             PRAGMA user_version = 2;",
        )?;
        transaction.commit()?;
    }
    if current < 3 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "ALTER TABLE manager_operation ADD COLUMN request_snapshot BLOB;
             ALTER TABLE manager_operation ADD COLUMN request_snapshot_schema TEXT;
             ALTER TABLE manager_operation ADD COLUMN request_snapshot_digest TEXT;
             CREATE INDEX IF NOT EXISTS manager_outbox_due
                 ON manager_outbox (status, next_attempt_at, created_at);
             PRAGMA user_version = 3;",
        )?;
        transaction.commit()?;
    }
    if current < 4 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "ALTER TABLE manager_operation ADD COLUMN marker_expectation BLOB;
             ALTER TABLE manager_operation ADD COLUMN marker_expectation_schema TEXT;
             PRAGMA user_version = 4;",
        )?;
        transaction.commit()?;
    }
    if current < 5 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_acceptance_guard (
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 candidate_id TEXT NOT NULL,
                 candidate_revision INTEGER NOT NULL,
                 manifest_digest TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS manager_acceptance_guard_candidate
                 ON manager_acceptance_guard (candidate_id);
             PRAGMA user_version = 5;",
        )?;
        transaction.commit()?;
    }
    if current < 6 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_issue_mapping_index (
                 candidate_id TEXT PRIMARY KEY NOT NULL,
                 issue_id TEXT NOT NULL UNIQUE,
                 mapping_state INTEGER NOT NULL,
                 operation_id TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS manager_issue_mapping_index_issue
                 ON manager_issue_mapping_index (issue_id);
             CREATE TABLE IF NOT EXISTS manager_operation_history_projection (
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 method TEXT NOT NULL,
                 state INTEGER NOT NULL,
                 direct_target_kind TEXT,
                 direct_target_id TEXT,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 resolution_required INTEGER NOT NULL DEFAULT 0,
                 FOREIGN KEY (operation_id) REFERENCES manager_operation(operation_id)
             );
             CREATE INDEX IF NOT EXISTS manager_operation_history_projection_target
                 ON manager_operation_history_projection
                    (direct_target_kind, direct_target_id, created_at DESC, operation_id DESC);
             CREATE INDEX IF NOT EXISTS manager_operation_history_projection_query
                 ON manager_operation_history_projection
                    (method, state, created_at DESC, operation_id DESC);
             CREATE INDEX IF NOT EXISTS manager_operation_history_projection_direct_state_order
                 ON manager_operation_history_projection
                    (direct_target_kind, direct_target_id, state,
                     created_at DESC, operation_id DESC);
             CREATE TABLE IF NOT EXISTS manager_operation_history_issue (
                 operation_id TEXT NOT NULL,
                 issue_id TEXT NOT NULL,
                 PRIMARY KEY (operation_id, issue_id),
                 FOREIGN KEY (operation_id) REFERENCES manager_operation(operation_id)
             );
             CREATE INDEX IF NOT EXISTS manager_operation_history_issue_lookup
                 ON manager_operation_history_issue (issue_id, operation_id);
             CREATE TABLE IF NOT EXISTS manager_gateway_registration (
                 endpoint_id TEXT PRIMARY KEY NOT NULL,
                 gateway_id TEXT NOT NULL UNIQUE,
                 policy_snapshot BLOB NOT NULL,
                 policy_fingerprint TEXT NOT NULL,
                 registration_generation INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS manager_gateway_epoch (
                 endpoint_id TEXT PRIMARY KEY NOT NULL,
                 capability_epoch TEXT NOT NULL,
                 started_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS manager_gateway_replay_record (
                 endpoint_id TEXT NOT NULL,
                 capability_epoch TEXT NOT NULL,
                 idempotency_key TEXT NOT NULL,
                 rpc_method TEXT NOT NULL,
                 request_digest TEXT NOT NULL,
                 response_snapshot BLOB,
                 response_schema TEXT,
                 operation_state INTEGER NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (endpoint_id, capability_epoch, idempotency_key)
             );
             CREATE INDEX IF NOT EXISTS manager_gateway_replay_record_lookup
                 ON manager_gateway_replay_record
                    (endpoint_id, idempotency_key, capability_epoch);",
        )?;
        backfill_issue_mapping_index(&transaction)?;
        backfill_operation_history_projection(&transaction)?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute_batch("PRAGMA user_version = 6;")?;
        transaction.commit()?;
    }
    if current < 7 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "ALTER TABLE manager_operation_history_projection
                 ADD COLUMN resolution_kind TEXT;
             ALTER TABLE manager_operation_history_projection
                 ADD COLUMN reason_category TEXT;
             ALTER TABLE manager_operation_history_projection
                 ADD COLUMN rationale TEXT;
             PRAGMA user_version = 7;",
        )?;
        transaction.execute(
            "INSERT INTO manager_metadata (key, value)
             SELECT ?1, ?2
              WHERE EXISTS (
                  SELECT 1 FROM manager_metadata WHERE key = 'data_root_id'
              )
             ON CONFLICT(key) DO NOTHING",
            params![
                LIST_ISSUES_WITH_SYNC_PAGE_TOKEN_SECRET_KEY,
                generate_list_issues_with_sync_page_token_secret(),
            ],
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 8 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_resolution_saga (
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 candidate_id TEXT NOT NULL,
                 candidate_revision INTEGER NOT NULL,
                 decision INTEGER NOT NULL,
                 issue_id TEXT NOT NULL,
                 args_snapshot BLOB NOT NULL,
                 args_digest TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 FOREIGN KEY (operation_id) REFERENCES manager_operation(operation_id)
             );
             CREATE INDEX IF NOT EXISTS manager_resolution_saga_candidate
                 ON manager_resolution_saga (candidate_id);
             PRAGMA user_version = 8;",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 9 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_gateway_registration_idempotency (
                 endpoint_id TEXT NOT NULL,
                 idempotency_key TEXT NOT NULL,
                 request_digest TEXT NOT NULL,
                 registration_snapshot BLOB NOT NULL,
                 created_at INTEGER NOT NULL,
                 PRIMARY KEY (endpoint_id, idempotency_key)
             );
             PRAGMA user_version = 9;",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 10 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_operation_history_resolution (
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 resolution_kind TEXT NOT NULL,
                 reason_category INTEGER NOT NULL,
                 rationale TEXT,
                 FOREIGN KEY (operation_id) REFERENCES manager_operation(operation_id)
             );
             PRAGMA user_version = 10;",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 11 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_extraction_apply_outcome (
                 request_digest TEXT PRIMARY KEY NOT NULL,
                 outcome INTEGER NOT NULL,
                 run_id TEXT,
                 skip_reason INTEGER,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS manager_extraction_apply_outcome_run
                 ON manager_extraction_apply_outcome (run_id);
             CREATE TABLE IF NOT EXISTS manager_extraction_apply_key (
                 idempotency_key TEXT PRIMARY KEY NOT NULL,
                 request_digest TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 FOREIGN KEY (request_digest)
                     REFERENCES manager_extraction_apply_outcome(request_digest)
             );
             CREATE INDEX IF NOT EXISTS manager_extraction_apply_key_digest
                 ON manager_extraction_apply_key (request_digest);
             PRAGMA user_version = 11;",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 12 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_extraction_auto_registration (
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 run_id TEXT NOT NULL,
                 candidate_id TEXT NOT NULL UNIQUE,
                 issue_id TEXT NOT NULL UNIQUE,
                 state INTEGER NOT NULL,
                 failure_reason TEXT,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 FOREIGN KEY (operation_id) REFERENCES manager_operation(operation_id)
             );
             CREATE INDEX IF NOT EXISTS manager_extraction_auto_registration_run
                 ON manager_extraction_auto_registration (run_id, state);
             PRAGMA user_version = 12;",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 13 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_extraction_auto_registration_reservation (
                 issue_id TEXT PRIMARY KEY NOT NULL,
                 run_id TEXT NOT NULL,
                 candidate_id TEXT NOT NULL UNIQUE,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS manager_extraction_auto_registration_reservation_run
                 ON manager_extraction_auto_registration_reservation (run_id);
             PRAGMA user_version = 13;",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 14 {
        let transaction = connection.transaction()?;
        if !table_has_column(
            &transaction,
            "manager_extraction_auto_registration_reservation",
            "expected_idempotency_key",
        )? {
            transaction.execute_batch(
                "ALTER TABLE manager_extraction_auto_registration_reservation
                     ADD COLUMN expected_idempotency_key TEXT NOT NULL DEFAULT '';",
            )?;
        }
        transaction.execute_batch("PRAGMA user_version = 14;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 15 {
        let transaction = connection.transaction()?;
        if !table_has_column(
            &transaction,
            "manager_extraction_auto_registration",
            "expected_idempotency_key",
        )? {
            transaction.execute_batch(
                "ALTER TABLE manager_extraction_auto_registration
                     ADD COLUMN expected_idempotency_key TEXT NOT NULL DEFAULT '';",
            )?;
        }
        transaction.execute_batch("PRAGMA user_version = 15;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 16 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_extraction_candidate_identity (
                 candidate_id TEXT PRIMARY KEY NOT NULL,
                 thread_id TEXT NOT NULL,
                 snapshot_digest TEXT NOT NULL,
                 fingerprint TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS manager_extraction_candidate_identity_group
                 ON manager_extraction_candidate_identity
                    (thread_id, snapshot_digest, fingerprint, candidate_id);
             CREATE TABLE IF NOT EXISTS manager_extraction_suppression_audit (
                 run_id TEXT NOT NULL,
                 input_index INTEGER NOT NULL CHECK (input_index >= 0),
                 reason INTEGER NOT NULL,
                 thread_id TEXT NOT NULL,
                 snapshot_digest TEXT NOT NULL,
                 fingerprint TEXT NOT NULL,
                 PRIMARY KEY (run_id, input_index, reason)
             );
             CREATE INDEX IF NOT EXISTS manager_extraction_suppression_audit_run
                 ON manager_extraction_suppression_audit (run_id, input_index);
             CREATE TABLE IF NOT EXISTS manager_extraction_suppression_rejected_candidate (
                 run_id TEXT NOT NULL,
                 input_index INTEGER NOT NULL CHECK (input_index >= 0),
                 reason INTEGER NOT NULL,
                 candidate_id TEXT NOT NULL,
                 PRIMARY KEY (run_id, input_index, reason, candidate_id),
                 FOREIGN KEY (run_id, input_index, reason)
                     REFERENCES manager_extraction_suppression_audit
                         (run_id, input_index, reason)
             );
             CREATE INDEX IF NOT EXISTS manager_extraction_suppression_rejected_candidate_lookup
                 ON manager_extraction_suppression_rejected_candidate (candidate_id, run_id);
             CREATE TABLE IF NOT EXISTS manager_operation_history_decision (
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 decision_kind INTEGER NOT NULL,
                 candidate_id TEXT NOT NULL,
                 candidate_revision INTEGER NOT NULL CHECK (candidate_revision >= 0),
                 FOREIGN KEY (operation_id) REFERENCES manager_operation(operation_id)
             );
             CREATE INDEX IF NOT EXISTS manager_operation_history_decision_kind
                 ON manager_operation_history_decision
                    (decision_kind, operation_id);",
        )?;
        // This branch is only reached while initializing an empty root.  An
        // existing root is preflighted before migration and must already be
        // v17, because its protobuf payloads are not compatible with this
        // contract.
        transaction.execute_batch("PRAGMA user_version = 17;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 18 {
        let transaction = connection.transaction()?;
        // v4 intentionally required a fresh root. This version boundary only
        // records the new identity while initializing that root.
        transaction.execute_batch("PRAGMA user_version = 18;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 19 {
        let transaction = connection.transaction()?;
        // v5 changes durable administrative result snapshots. This branch is
        // reached only while creating a new empty root; preflight rejects a
        // populated v18 root before migration.
        transaction.execute_batch("PRAGMA user_version = 19;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 20 {
        let transaction = connection.transaction()?;
        // v6 changes the persisted administrative result wrapper. It is only
        // reached while initializing a fresh root; populated v19 roots are
        // rejected by preflight before this migration runs.
        transaction.execute_batch("PRAGMA user_version = 20;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 21 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_backend_request (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 operation_id TEXT NOT NULL,
                 method TEXT NOT NULL,
                 request_schema TEXT NOT NULL,
                 request_payload BLOB NOT NULL,
                 request_digest TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, operation_id)
             );
             CREATE TABLE IF NOT EXISTS admin_marker_expectation (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 downstream_operation_id TEXT NOT NULL,
                 operation_id TEXT NOT NULL,
                 issue_id TEXT NOT NULL,
                 action INTEGER NOT NULL,
                 expected_revision INTEGER NOT NULL,
                 request_digest TEXT NOT NULL,
                 tracker_contract_fingerprint TEXT NOT NULL,
                 tracker_schema_fingerprint TEXT NOT NULL,
                 method_key TEXT NOT NULL,
                 expectation_payload BLOB NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, downstream_operation_id)
             );
             CREATE TABLE IF NOT EXISTS admin_dispatch_attempt (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 operation_id TEXT NOT NULL,
                 attempt_generation INTEGER NOT NULL,
                 outbox_id TEXT NOT NULL,
                 state INTEGER NOT NULL,
                 lease_id TEXT,
                 lease_expires_at INTEGER,
                 task_id TEXT,
                 fence_token TEXT,
                 permitted_at INTEGER,
                 resolved_at INTEGER,
                 evidence_classification TEXT,
                 PRIMARY KEY (contract_fingerprint, data_root_id, operation_id, attempt_generation)
             );
             CREATE TABLE IF NOT EXISTS manager_capability_attempt (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 operation_id TEXT NOT NULL,
                 attempt_generation INTEGER NOT NULL,
                 method TEXT NOT NULL,
                 issue_id TEXT NOT NULL,
                 request_digest TEXT NOT NULL,
                 capability_fingerprint TEXT NOT NULL,
                 state TEXT NOT NULL,
                 permitted_at INTEGER,
                 sent_at INTEGER,
                 resolved_at INTEGER,
                 evidence_classification TEXT,
                 evidence_snapshot BLOB,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, operation_id, attempt_generation)
             );
             CREATE TABLE IF NOT EXISTS manager_resolution_request (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 resolution_operation_id TEXT PRIMARY KEY NOT NULL,
                 source_operation_id TEXT NOT NULL,
                 issue_id TEXT NOT NULL,
                 request_payload BLOB NOT NULL,
                 request_digest TEXT NOT NULL,
                 state TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS resolution_fence (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 source_operation_id TEXT NOT NULL,
                 resolution_operation_id TEXT NOT NULL,
                 fence_token TEXT NOT NULL,
                 source_generation INTEGER NOT NULL,
                 expected_issue_revision INTEGER NOT NULL,
                 decision INTEGER NOT NULL,
                 request_payload BLOB NOT NULL,
                 request_digest TEXT NOT NULL,
                 state INTEGER NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, source_operation_id)
             );
             CREATE TABLE IF NOT EXISTS manager_ownership_recovery (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 issue_id TEXT NOT NULL,
                 readmission_number INTEGER NOT NULL,
                 current_operation_id TEXT,
                 expected_revision INTEGER NOT NULL,
                 in_flight INTEGER NOT NULL,
                 consecutive_failures INTEGER NOT NULL,
                 stopped INTEGER NOT NULL,
                 stop_classification TEXT,
                 stop_basis TEXT,
                 repair_condition TEXT,
                 next_attempt_at INTEGER,
                 cursor_diagnostic TEXT,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, issue_id)
             );
             CREATE TABLE IF NOT EXISTS manager_ownership_recovery_projection (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 issue_id TEXT NOT NULL,
                 readmission_number INTEGER NOT NULL,
                 operation_id TEXT NOT NULL,
                 source_generation INTEGER NOT NULL,
                 terminal_state INTEGER NOT NULL,
                 projected_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, issue_id, readmission_number, operation_id)
             );
             PRAGMA user_version = 21;",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 22 {
        let transaction = connection.transaction()?;
        // v8 fixes the durable ownership/result contract.  The table layout
        // introduced by v7 is intentionally retained, but the version bump
        // makes the protobuf and replay semantics an explicit root identity.
        // Populated roots are rejected by preflight before reaching this
        // branch; only an empty/new root is allowed to acquire the v8 identity
        // before the v9 diagnostic tables are created below.
        transaction.execute_batch("PRAGMA user_version = 22;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 23 {
        let transaction = connection.transaction()?;
        // v9 introduces independent diagnosis records and phase-health
        // snapshots. They never overwrite operation or ownership rows, so a
        // malformed payload/Issue can be quarantined and queried safely.
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_operation_diagnosis (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 operation_id TEXT NOT NULL,
                 diagnosis_generation INTEGER NOT NULL,
                 classification TEXT NOT NULL,
                 source_generation INTEGER,
                 attempt_generation INTEGER,
                 evidence_ref TEXT NOT NULL,
                 evidence_digest TEXT NOT NULL,
                 replayability INTEGER NOT NULL,
                 resolution_state TEXT NOT NULL,
                 resolution_operation_id TEXT,
                 repair_condition TEXT NOT NULL,
                 recorded_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, operation_id,
                              diagnosis_generation),
                 UNIQUE (contract_fingerprint, data_root_id, operation_id,
                         classification, evidence_digest)
             );
             CREATE INDEX IF NOT EXISTS manager_operation_diagnosis_operation
                 ON manager_operation_diagnosis(contract_fingerprint, data_root_id,
                                                 operation_id, diagnosis_generation);
             CREATE TABLE IF NOT EXISTS manager_ownership_integrity_diagnosis (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 target_kind TEXT NOT NULL,
                 target_id TEXT NOT NULL,
                 diagnosis_class TEXT NOT NULL,
                 evidence_digest TEXT NOT NULL,
                 diagnosis_generation INTEGER NOT NULL,
                 observation_token INTEGER NOT NULL,
                 repair_epoch INTEGER NOT NULL,
                 observed_revision INTEGER,
                 lease_classification TEXT,
                 repair_condition TEXT,
                 state TEXT NOT NULL,
                 resolution_operation_id TEXT,
                 first_detected_at INTEGER NOT NULL,
                 last_confirmed_at INTEGER NOT NULL,
                 resolved_at INTEGER,
                 invalid_attributes TEXT NOT NULL DEFAULT '[]',
                 lease_expires_at INTEGER,
                 open_blocker_issue_ids TEXT NOT NULL DEFAULT '[]',
                 PRIMARY KEY (contract_fingerprint, data_root_id, target_kind,
                              target_id, diagnosis_class, evidence_digest,
                              diagnosis_generation)
             );
             CREATE INDEX IF NOT EXISTS manager_ownership_integrity_target
                 ON manager_ownership_integrity_diagnosis(contract_fingerprint,
                                                          data_root_id,
                                                          target_kind, target_id,
                                                          diagnosis_generation);
             CREATE TABLE IF NOT EXISTS manager_diagnosis_epoch (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 target_kind TEXT NOT NULL,
                 target_id TEXT NOT NULL,
                 diagnosis_epoch INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, target_kind,
                              target_id)
             );
             CREATE TABLE IF NOT EXISTS manager_phase_health (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 phase TEXT NOT NULL,
                 health INTEGER NOT NULL,
                 last_started_at INTEGER,
                 last_finished_at INTEGER,
                 last_failure_at INTEGER,
                 last_recovered_at INTEGER,
                 attempt_count INTEGER NOT NULL,
                 success_count INTEGER NOT NULL,
                 skipped_count INTEGER NOT NULL,
                 conflict_count INTEGER NOT NULL,
                 diagnosis_count INTEGER NOT NULL,
                 unconfirmed_count INTEGER NOT NULL,
                 pending_gauge INTEGER NOT NULL,
                 inconsistent_gauge INTEGER NOT NULL,
                 permit_enabled INTEGER NOT NULL,
                 gauge_stale INTEGER NOT NULL,
                 snapshot_payload BLOB,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id, phase)
             );
             ALTER TABLE manager_operation ADD COLUMN diagnosis BLOB;
             PRAGMA user_version = 23;",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 24 {
        let transaction = connection.transaction()?;
        // The v11 predecessor extended the administrative request/result contract with
        // diagnosis-bound repair references and phase-health projections.
        // Existing populated roots are rejected by preflight; this branch is
        // therefore only reached while constructing a fresh compatible root.
        transaction.execute_batch("PRAGMA user_version = 24;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 25 {
        let transaction = connection.transaction()?;
        if !table_has_column(&transaction, "manager_phase_health", "unconfirmed_count")? {
            transaction.execute(
                "ALTER TABLE manager_phase_health ADD COLUMN unconfirmed_count INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        transaction.execute_batch("PRAGMA user_version = 25;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 26 {
        let transaction = connection.transaction()?;
        // The v11 predecessor added structured lease-expiry and dependency-blocker diagnosis
        // fields to the public Manager error contract. Existing populated
        // roots are rejected by preflight; this branch is only used while
        // constructing a fresh compatible root.
        if !table_has_column(
            &transaction,
            "manager_ownership_integrity_diagnosis",
            "invalid_attributes",
        )? {
            transaction.execute(
                "ALTER TABLE manager_ownership_integrity_diagnosis
                    ADD COLUMN invalid_attributes TEXT NOT NULL DEFAULT '[]'",
                [],
            )?;
        }
        if !table_has_column(
            &transaction,
            "manager_ownership_integrity_diagnosis",
            "lease_expires_at",
        )? {
            transaction.execute(
                "ALTER TABLE manager_ownership_integrity_diagnosis
                    ADD COLUMN lease_expires_at INTEGER",
                [],
            )?;
        }
        if !table_has_column(
            &transaction,
            "manager_ownership_integrity_diagnosis",
            "open_blocker_issue_ids",
        )? {
            transaction.execute(
                "ALTER TABLE manager_ownership_integrity_diagnosis
                    ADD COLUMN open_blocker_issue_ids TEXT NOT NULL DEFAULT '[]'",
                [],
            )?;
        }
        transaction.execute_batch("PRAGMA user_version = 26;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 27 {
        let transaction = connection.transaction()?;
        // v12 extends the administrative receipt with immutable dependency
        // observations. No relational columns are needed; the schema bump
        // fences roots whose durable operations predate that wire contract.
        transaction.execute_batch("PRAGMA user_version = 27;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 28 {
        let transaction = connection.transaction()?;
        // v13 adds explicit human-arbitration fields to the durable
        // operation contract. No relational columns are required; the schema
        // bump fences roots whose payloads predate the new terminal state.
        transaction.execute_batch("PRAGMA user_version = 28;")?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.commit()?;
    }
    if current < 29 {
        let transaction = connection.transaction()?;
        // v14 adds an append-only audit boundary for Issue-operation
        // resolution. The source operation and replay payload are retained
        // before an uncertain result is replaced, so later GetOperation reads
        // cannot erase the evidence used by a human decision.
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_issue_operation_resolution_audit (
                 contract_fingerprint TEXT NOT NULL,
                 data_root_id TEXT NOT NULL,
                 resolution_operation_id TEXT NOT NULL,
                 source_operation_id TEXT NOT NULL,
                 source_generation INTEGER NOT NULL,
                 source_state INTEGER NOT NULL,
                 source_operation_payload BLOB NOT NULL,
                 source_operation_payload_schema TEXT NOT NULL,
                 source_operation_payload_digest TEXT NOT NULL,
                 source_replay_payload BLOB,
                 source_replay_schema TEXT,
                 source_replay_digest TEXT,
                 resolution_result_payload BLOB NOT NULL,
                 resolution_result_schema TEXT NOT NULL,
                 resolution_result_digest TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 PRIMARY KEY (contract_fingerprint, data_root_id,
                              resolution_operation_id)
             );
             CREATE INDEX IF NOT EXISTS manager_issue_operation_resolution_audit_source
                 ON manager_issue_operation_resolution_audit
                    (contract_fingerprint, data_root_id, source_operation_id,
                     source_generation, resolution_operation_id);",
        )?;
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute_batch("PRAGMA user_version = 29;")?;
        transaction.commit()?;
    }
    if current < 30 {
        let transaction = connection.transaction()?;
        // A populated v29 root must be left in a self-consistent v30 state
        // when this step commits.  In particular, a crash between the v30
        // and v31 transactions must pass the next startup preflight instead
        // of carrying the final v31 fingerprint on a v30 schema.
        let migration_schema_fingerprint = if current == 29 {
            PREVIOUS_SCHEMA_FINGERPRINT
        } else {
            expected_schema_fingerprint
        };
        // Schema v30 rebuilds the review read-model join from the durable IssueMapping
        // entities. Before v30, ordinary IssueMapping writes did not maintain
        // this index, so an upgraded v29 root may have no rows even though the
        // source entities are present.
        // Validate the history projection before committing the v30 step as
        // well. This keeps a v29 -> v31 upgrade atomic when the later v31
        // index migration discovers a corrupt history row.
        let history_projection_repaired =
            validate_operation_history_projection_integrity(&transaction, true)?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS manager_issue_mapping_index (
                 candidate_id TEXT PRIMARY KEY NOT NULL,
                 issue_id TEXT NOT NULL UNIQUE,
                 mapping_state INTEGER NOT NULL,
                 operation_id TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS manager_issue_mapping_index_issue
                 ON manager_issue_mapping_index (issue_id);",
        )?;
        rebuild_issue_mapping_index(&transaction)?;
        if history_projection_repaired {
            migration_commit_stamp(&transaction)?;
        }
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![migration_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1
              WHERE role = ?2",
            params![migration_schema_fingerprint, StoreRole::Manager.as_i32()],
        )?;
        transaction.execute_batch("PRAGMA user_version = 30;")?;
        transaction.commit()?;
    }
    if current < 31 {
        let transaction = connection.transaction()?;
        // Schema v31 adds the composite lookup used by keyset-paged history
        // reads. Normal writes keep the projection's created_at and scope
        // immutable; migration may first normalize the known legacy timestamp
        // relationship before introducing this read-optimized index.
        let history_projection_repaired =
            validate_operation_history_projection_integrity(&transaction, true)?;
        transaction.execute_batch(
            "CREATE INDEX IF NOT EXISTS manager_operation_history_projection_direct_state_order
                 ON manager_operation_history_projection
                    (direct_target_kind, direct_target_id, state,
                     created_at DESC, operation_id DESC);",
        )?;
        if history_projection_repaired {
            migration_commit_stamp(&transaction)?;
        }
        transaction.execute(
            "UPDATE manager_metadata SET value = ?1 WHERE key = 'schema_fingerprint'",
            params![expected_schema_fingerprint],
        )?;
        transaction.execute(
            "UPDATE manager_store_identity SET schema_fingerprint = ?1
              WHERE role = ?2",
            params![expected_schema_fingerprint, StoreRole::Manager.as_i32()],
        )?;
        transaction.execute_batch("PRAGMA user_version = 31;")?;
        transaction.commit()?;
    }
    Ok(())
}

/// Verify and, for known legacy drift, repair every existing history projection
/// before v31 makes it queryable by the keyset path. Legacy v30 writes used the
/// commit-time value for both the source and projection columns while the
/// durable payload retained its admission-time `created_at`. They also failed
/// to refresh lifecycle state in the projection. Those repairs are accepted
/// only when the durable source and payload agree and the projection's
/// immutable identity fields remain unchanged.
#[derive(Debug)]
struct HistoryProjectionIntegrityRow {
    operation_id: String,
    projection_method: String,
    projection_state: i32,
    direct_target_kind: Option<String>,
    direct_target_id: Option<String>,
    projection_created_at: i64,
    projection_updated_at: i64,
    projection_resolution_required: i64,
    source_method: Option<String>,
    source_state: Option<i32>,
    source_created_at: Option<i64>,
    source_updated_at: Option<i64>,
    payload: Option<Vec<u8>>,
    payload_schema: Option<String>,
    payload_digest: Option<String>,
}

fn load_history_projection_integrity_rows(
    transaction: &Transaction<'_>,
) -> Result<Vec<HistoryProjectionIntegrityRow>, ManagerStorageError> {
    let mut statement = transaction.prepare(
        "SELECT projection.operation_id, projection.method, projection.state,
                projection.direct_target_kind, projection.direct_target_id,
                projection.created_at, projection.updated_at,
                projection.resolution_required, operation.method, operation.state,
                operation.created_at, operation.updated_at,
                operation.operation_payload,
                operation.operation_payload_schema, operation.operation_payload_digest
           FROM manager_operation_history_projection AS projection
           LEFT JOIN manager_operation AS operation
                  ON operation.operation_id = projection.operation_id
          ORDER BY projection.operation_id",
    )?;
    statement
        .query_map([], |row| {
            Ok(HistoryProjectionIntegrityRow {
                operation_id: row.get(0)?,
                projection_method: row.get(1)?,
                projection_state: row.get(2)?,
                direct_target_kind: row.get(3)?,
                direct_target_id: row.get(4)?,
                projection_created_at: row.get(5)?,
                projection_updated_at: row.get(6)?,
                projection_resolution_required: row.get(7)?,
                source_method: row.get(8)?,
                source_state: row.get(9)?,
                source_created_at: row.get(10)?,
                source_updated_at: row.get(11)?,
                payload: row.get(12)?,
                payload_schema: row.get(13)?,
                payload_digest: row.get(14)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(ManagerStorageError::from)
}

fn decode_history_projection_payload(
    row: &HistoryProjectionIntegrityRow,
) -> Result<pb::DurableOperation, ManagerStorageError> {
    let (Some(payload), Some(schema), Some(digest)) =
        (&row.payload, &row.payload_schema, &row.payload_digest)
    else {
        return Err(ManagerStorageError::Transaction(
            "operation history projection has no durable payload".into(),
        ));
    };
    if schema != "issue_tracker.manager.v1.DurableOperation"
        || digest_bytes(payload) != digest.as_str()
    {
        return Err(ManagerStorageError::Transaction(
            "operation history durable payload integrity validation failed".into(),
        ));
    }
    pb::DurableOperation::decode(payload.as_slice()).map_err(|_| {
        ManagerStorageError::Transaction(
            "operation history durable payload cannot be decoded".into(),
        )
    })
}

fn operation_scope_identity(operation: &pb::DurableOperation) -> (Option<&str>, Option<&str>) {
    match operation.scope.as_ref() {
        Some(pb::durable_operation::Scope::CandidateId(id)) => (Some("candidate"), Some(id)),
        Some(pb::durable_operation::Scope::EvidenceId(id)) => (Some("evidence"), Some(id)),
        Some(pb::durable_operation::Scope::ExtractionRunId(id)) => {
            (Some("extraction_run"), Some(id))
        }
        Some(pb::durable_operation::Scope::IssueId(id)) => (Some("issue"), Some(id)),
        None => (None, None),
    }
}

fn validate_history_projection_identity(
    row: &HistoryProjectionIntegrityRow,
    operation: &pb::DurableOperation,
) -> Result<(), ManagerStorageError> {
    let (expected_kind, expected_id) = operation_scope_identity(operation);
    if operation.operation_id != row.operation_id
        || operation.method != row.projection_method
        || row.source_method.as_deref() != Some(operation.method.as_str())
        || row.source_state != Some(operation.state)
        || row.direct_target_kind.as_deref() != expected_kind
        || row.direct_target_id.as_deref() != expected_id
    {
        return Err(ManagerStorageError::Transaction(
            "operation history projection does not match durable payload".into(),
        ));
    }
    if row.source_created_at != Some(row.projection_created_at) {
        return Err(ManagerStorageError::Transaction(
            "operation history source and projection timestamps disagree".into(),
        ));
    }
    Ok(())
}

fn history_payload_timestamps(
    operation: &pb::DurableOperation,
) -> Result<(i64, i64), ManagerStorageError> {
    let created_at = operation
        .created_at
        .as_ref()
        .map(|instant| instant.unix_epoch_millis)
        .ok_or_else(|| {
            ManagerStorageError::Transaction(
                "operation history durable payload has no created_at".into(),
            )
        })?;
    let updated_at = operation
        .updated_at
        .as_ref()
        .map(|instant| instant.unix_epoch_millis)
        .ok_or_else(|| {
            ManagerStorageError::Transaction(
                "operation history durable payload has no updated_at".into(),
            )
        })?;
    Ok((created_at, updated_at))
}

fn repair_legacy_history_projection(
    transaction: &Transaction<'_>,
    row: &HistoryProjectionIntegrityRow,
    operation: &pb::DurableOperation,
    created_at: i64,
    updated_at: i64,
) -> Result<(), ManagerStorageError> {
    transaction.execute(
        "UPDATE manager_operation
            SET created_at = ?1, updated_at = ?2
          WHERE operation_id = ?3",
        rusqlite::params![created_at, updated_at, row.operation_id],
    )?;
    transaction.execute(
        "UPDATE manager_operation_history_projection
            SET state = ?1, created_at = ?2, updated_at = ?3,
                resolution_required = ?4
          WHERE operation_id = ?5",
        rusqlite::params![
            operation.state,
            created_at,
            updated_at,
            i64::from(operation.resolution_required),
            row.operation_id,
        ],
    )?;
    Ok(())
}

fn validate_history_issue_relations(
    transaction: &Transaction<'_>,
    operation: &pb::DurableOperation,
) -> Result<(), ManagerStorageError> {
    let mut statement = transaction.prepare(
        "SELECT issue_id FROM manager_operation_history_issue
          WHERE operation_id = ?1 ORDER BY issue_id",
    )?;
    let projected = statement
        .query_map(rusqlite::params![operation.operation_id], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<BTreeSet<_>, _>>()?;
    let durable = operation
        .affected_issue_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if operation
        .affected_issue_ids
        .iter()
        .any(|issue_id| issue_id.trim().is_empty())
        || durable.len() != operation.affected_issue_ids.len()
        || projected != durable
    {
        return Err(ManagerStorageError::Transaction(
            "operation history Issue relation projection does not match durable payload".into(),
        ));
    }
    Ok(())
}

fn validate_operation_history_projection_integrity(
    transaction: &Transaction<'_>,
    repair_legacy_projection: bool,
) -> Result<bool, ManagerStorageError> {
    let missing_projection: Option<String> = transaction
        .query_row(
            "SELECT operation.operation_id
               FROM manager_operation AS operation
              LEFT JOIN manager_operation_history_projection AS projection
                     ON projection.operation_id = operation.operation_id
              WHERE projection.operation_id IS NULL
              ORDER BY operation.operation_id
              LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if missing_projection.is_some() {
        return Err(ManagerStorageError::Transaction(
            "durable operation has no operation history projection".into(),
        ));
    }
    let rows = load_history_projection_integrity_rows(transaction)?;
    let mut repaired = false;
    for row in &rows {
        let operation = decode_history_projection_payload(row)?;
        validate_history_projection_identity(row, &operation)?;
        if row.source_updated_at.is_none() {
            return Err(ManagerStorageError::Transaction(
                "operation history durable source has no updated_at".into(),
            ));
        }
        let (operation_created_at, operation_updated_at) = history_payload_timestamps(&operation)?;
        let timestamp_mismatch = operation_created_at != row.projection_created_at
            || row.projection_state != operation.state
            || row.projection_updated_at != operation_updated_at
            || row.source_updated_at != Some(operation_updated_at)
            || row.projection_resolution_required != i64::from(operation.resolution_required);
        if timestamp_mismatch {
            if !repair_legacy_projection {
                return Err(ManagerStorageError::Transaction(
                    "operation history projection does not match durable payload".into(),
                ));
            }
            repair_legacy_history_projection(
                transaction,
                row,
                &operation,
                operation_created_at,
                operation_updated_at,
            )?;
            repaired = true;
        }
        validate_history_issue_relations(transaction, &operation)?;
    }
    Ok(repaired)
}

fn table_has_column(
    transaction: &Transaction<'_>,
    table_name: &str,
    column_name: &str,
) -> Result<bool, rusqlite::Error> {
    let mut statement = transaction.prepare(&format!("PRAGMA table_info({table_name})"))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        if row.get::<_, String>(1)? == column_name {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Backfill only unambiguous legacy mappings.
///
/// Schema v5 has no durable operation reference for a mapping. Selecting one
/// member of a conflicting group would invent causality, so every invalid or
/// non-bijective group is instead represented by a terminal durable operation
/// requiring human resolution. The migration itself remains atomic: either
/// both the usable indexes and every required audit operation are committed,
/// or neither is visible.
fn backfill_issue_mapping_index(transaction: &Transaction<'_>) -> Result<(), ManagerStorageError> {
    let mappings = {
        let mut statement = transaction.prepare(
            "SELECT entity_id, payload, created_at, updated_at
               FROM manager_entity
              WHERE entity_kind = 'issue_mapping'
              ORDER BY entity_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut candidate_to_issues = BTreeMap::<String, BTreeSet<String>>::new();
    let mut issue_to_candidates = BTreeMap::<String, BTreeSet<String>>::new();
    let mut decoded_associations = Vec::new();
    let mut resolutions = Vec::new();
    for (entity_id, payload, created_at, updated_at) in mappings {
        let mapping = match pb::IssueMapping::decode(payload.as_slice()) {
            Ok(mapping) => mapping,
            Err(error) => {
                resolutions.push(LegacyMappingResolution::invalid(
                    entity_id,
                    format!("legacy IssueMapping payload cannot be decoded: {error}"),
                ));
                continue;
            }
        };
        if mapping.issue_id.trim().is_empty() {
            resolutions.push(LegacyMappingResolution::invalid(
                entity_id,
                "legacy IssueMapping Issue identity is empty".into(),
            ));
            continue;
        }
        let Some(candidate_id) = mapping.candidate_id else {
            continue;
        };
        if candidate_id.trim().is_empty() {
            resolutions.push(LegacyMappingResolution::invalid(
                entity_id,
                "legacy IssueMapping Candidate identity is empty".into(),
            ));
            continue;
        }
        candidate_to_issues
            .entry(candidate_id.clone())
            .or_default()
            .insert(mapping.issue_id.clone());
        issue_to_candidates
            .entry(mapping.issue_id.clone())
            .or_default()
            .insert(candidate_id.clone());
        decoded_associations.push((
            entity_id,
            candidate_id,
            mapping.issue_id,
            created_at,
            updated_at,
        ));
    }

    let conflicting_candidates = candidate_to_issues
        .iter()
        .filter(|(_, issues)| issues.len() > 1)
        .map(|(candidate_id, issues)| (candidate_id.clone(), issues.clone()))
        .collect::<BTreeMap<_, _>>();
    let conflicting_issues = issue_to_candidates
        .iter()
        .filter(|(_, candidates)| candidates.len() > 1)
        .map(|(issue_id, candidates)| (issue_id.clone(), candidates.clone()))
        .collect::<BTreeMap<_, _>>();

    for (candidate_id, issue_ids) in &conflicting_candidates {
        resolutions.push(LegacyMappingResolution::candidate_conflict(
            candidate_id.clone(),
            issue_ids.clone(),
        ));
    }
    for (issue_id, candidate_ids) in &conflicting_issues {
        resolutions.push(LegacyMappingResolution::issue_conflict(
            issue_id.clone(),
            candidate_ids.clone(),
        ));
    }

    for (entity_id, candidate_id, issue_id, created_at, updated_at) in decoded_associations {
        if conflicting_candidates.contains_key(&candidate_id)
            || conflicting_issues.contains_key(&issue_id)
        {
            continue;
        }
        if entity_id != issue_id {
            resolutions.push(LegacyMappingResolution::invalid(
                entity_id,
                "legacy IssueMapping Issue identity differs from its entity ID".into(),
            ));
            continue;
        }
        transaction.execute(
            "INSERT INTO manager_issue_mapping_index
                (candidate_id, issue_id, mapping_state, operation_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                candidate_id,
                issue_id,
                2_i64,
                LEGACY_MAPPING_BACKFILL_OPERATION_ID,
                created_at,
                updated_at,
            ],
        )?;
    }
    insert_legacy_mapping_resolution_operations(transaction, &resolutions)?;
    Ok(())
}

/// Rebuild the v30 review join from the durable IssueMapping entities.
///
/// The index is a derived read model, so the entity payload is authoritative.
/// Invalid payloads and non-bijective associations are deliberately skipped;
/// the review query will expose them as integrity anomalies instead of
/// inventing an association during migration. A pre-existing index entry is
/// used only when an old refresh omitted `candidate_id`, preserving the
/// relation that was already durably observed before the rebuild. When that
/// fallback is selected, the entity payload and digest are repaired together
/// with the rebuilt index row.
fn rebuild_issue_mapping_index(transaction: &Transaction<'_>) -> Result<(), ManagerStorageError> {
    let existing_candidate_by_issue = {
        let mut statement = transaction.prepare(
            "SELECT issue_id, candidate_id
               FROM manager_issue_mapping_index
              ORDER BY issue_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<BTreeMap<_, _>, _>>()?
    };
    let mappings = {
        let mut statement = transaction.prepare(
            "SELECT entity_id, payload, payload_schema, payload_digest,
                    created_at, updated_at
               FROM manager_entity
              WHERE entity_kind = 'issue_mapping'
              ORDER BY entity_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };

    let mut candidate_to_issues = BTreeMap::<String, BTreeSet<String>>::new();
    let mut issue_to_candidates = BTreeMap::<String, BTreeSet<String>>::new();
    let mut associations = Vec::new();
    for (entity_id, payload, payload_schema, payload_digest, created_at, updated_at) in mappings {
        if payload_schema != "issue_tracker.manager.v1.IssueMapping"
            || payload_digest != digest_bytes(&payload)
        {
            continue;
        }
        let Ok(mapping) = pb::IssueMapping::decode(payload.as_slice()) else {
            continue;
        };
        if mapping.issue_id != entity_id {
            continue;
        }
        let candidate_id = mapping
            .candidate_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| existing_candidate_by_issue.get(&entity_id).cloned());
        let Some(candidate_id) = candidate_id else {
            continue;
        };
        candidate_to_issues
            .entry(candidate_id.clone())
            .or_default()
            .insert(entity_id.clone());
        issue_to_candidates
            .entry(entity_id.clone())
            .or_default()
            .insert(candidate_id.clone());
        let inherited_candidate_id = mapping
            .candidate_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty());
        associations.push((
            candidate_id,
            entity_id,
            created_at,
            updated_at,
            mapping,
            inherited_candidate_id,
        ));
    }

    transaction.execute("DELETE FROM manager_issue_mapping_index", [])?;
    for (candidate_id, issue_id, created_at, updated_at, mut mapping, inherited_candidate_id) in
        associations
    {
        if candidate_to_issues
            .get(&candidate_id)
            .is_none_or(|issues| issues.len() != 1)
            || issue_to_candidates
                .get(&issue_id)
                .is_none_or(|candidates| candidates.len() != 1)
        {
            continue;
        }
        if inherited_candidate_id {
            mapping.candidate_id = Some(candidate_id.clone());
            let payload = mapping.encode_to_vec();
            transaction.execute(
                "UPDATE manager_entity
                    SET payload=?2, payload_digest=?3
                  WHERE entity_kind='issue_mapping' AND entity_id=?1",
                params![issue_id, payload, digest_bytes(&payload)],
            )?;
        }
        transaction.execute(
            "INSERT INTO manager_issue_mapping_index
                (candidate_id, issue_id, mapping_state, operation_id, created_at, updated_at)
             VALUES (?1, ?2, 2, ?3, ?4, ?5)",
            params![
                candidate_id,
                issue_id,
                MAPPING_INDEX_REBUILD_OPERATION_ID,
                created_at,
                updated_at,
            ],
        )?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
enum LegacyMappingResolutionTarget {
    Candidate(String),
    Issue(String),
    None,
}

#[derive(Clone, Debug)]
struct LegacyMappingResolution {
    target: LegacyMappingResolutionTarget,
    affected_issue_ids: Vec<String>,
    reason: String,
}

impl LegacyMappingResolution {
    fn invalid(entity_id: String, reason: String) -> Self {
        let target = if entity_id.trim().is_empty() {
            LegacyMappingResolutionTarget::None
        } else {
            LegacyMappingResolutionTarget::Issue(entity_id)
        };
        Self {
            target,
            affected_issue_ids: Vec::new(),
            reason,
        }
    }

    fn candidate_conflict(candidate_id: String, issue_ids: BTreeSet<String>) -> Self {
        let affected_issue_ids = issue_ids.into_iter().collect::<Vec<_>>();
        Self {
            target: LegacyMappingResolutionTarget::Candidate(candidate_id.clone()),
            reason: format!(
                "legacy Candidate {candidate_id} maps to multiple Issues: {}",
                affected_issue_ids.join(", ")
            ),
            affected_issue_ids,
        }
    }

    fn issue_conflict(issue_id: String, candidate_ids: BTreeSet<String>) -> Self {
        Self {
            target: LegacyMappingResolutionTarget::Issue(issue_id.clone()),
            affected_issue_ids: vec![issue_id.clone()],
            reason: format!(
                "legacy Issue {issue_id} maps to multiple Candidates: {}",
                candidate_ids.into_iter().collect::<Vec<_>>().join(", ")
            ),
        }
    }

    fn stable_identity(&self) -> String {
        let target = match &self.target {
            LegacyMappingResolutionTarget::Candidate(id) => format!("candidate:{id}"),
            LegacyMappingResolutionTarget::Issue(id) => format!("issue:{id}"),
            LegacyMappingResolutionTarget::None => "none".into(),
        };
        format!(
            "{target}|{}|{}",
            self.affected_issue_ids.join(","),
            self.reason
        )
    }
}

fn insert_legacy_mapping_resolution_operations(
    transaction: &Transaction<'_>,
    resolutions: &[LegacyMappingResolution],
) -> Result<(), ManagerStorageError> {
    if resolutions.is_empty() {
        return Ok(());
    }
    let stamp = migration_commit_stamp(transaction)?;
    let now = now_millis();
    for resolution in resolutions {
        let identity = resolution.stable_identity();
        let operation_id = format!(
            "migration-v6-legacy-mapping-manual-{}",
            &digest_bytes(identity.as_bytes())[..24]
        );
        let (scope, target) = match &resolution.target {
            LegacyMappingResolutionTarget::Candidate(candidate_id) => (
                Some(pb::durable_operation::Scope::CandidateId(
                    candidate_id.clone(),
                )),
                format!("candidate:{candidate_id}"),
            ),
            LegacyMappingResolutionTarget::Issue(issue_id) => (
                Some(pb::durable_operation::Scope::IssueId(issue_id.clone())),
                format!("issue:{issue_id}"),
            ),
            LegacyMappingResolutionTarget::None => (None, "legacy_mapping".into()),
        };
        let error = pb::ManagerError {
            code: pb::ManagerErrorCode::ResolutionRequired as i32,
            message: resolution.reason.clone(),
            retryable: false,
            detail: None,
        };
        let error_snapshot = pb::Snapshot {
            payload: error.encode_to_vec(),
            schema: "issue_tracker.manager.v1.ManagerError".into(),
        };
        let operation = pb::DurableOperation {
            operation_id: operation_id.clone(),
            scope,
            method: "manager_migrate_legacy_issue_mapping".into(),
            idempotency_scope: format!("migration/v6/legacy-mapping/{operation_id}"),
            request_digest: digest_bytes(identity.as_bytes()),
            request_schema: "issue_tracker.manager.v1.LegacyIssueMappingMigration".into(),
            state: pb::DurableOperationState::ManualResolution as i32,
            failure_class: pb::FailureClass::Terminal as i32,
            manager_sequence: stamp.0,
            generation: stamp.1,
            affects_ready: false,
            affected_issue_ids: resolution.affected_issue_ids.clone(),
            terminal_snapshot: Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(
                error_snapshot,
            )),
            resolution_required: true,
            created_at: Some(pb::UtcInstant {
                unix_epoch_millis: now,
            }),
            updated_at: Some(pb::UtcInstant {
                unix_epoch_millis: now,
            }),
            audit: Some(pb::ManagerOperationAudit {
                method: "manager_migrate_legacy_issue_mapping".into(),
                target,
                data_root_id: metadata_value(transaction, "data_root_id")?,
                contract_fingerprint: metadata_value(transaction, "contract_fingerprint")?,
                schema_fingerprint: metadata_value(transaction, "schema_fingerprint")?,
                idempotency_key: operation_id.clone(),
                actor_audit: None,
                correlation_id: operation_id.clone(),
                recorded_at: Some(pb::UtcInstant {
                    unix_epoch_millis: now,
                }),
            }),
            commit_durability: crate::v1::OperationCommitDurability::Durable as i32,
            result_replayability: crate::v1::OperationResultReplayability::NotReplayable as i32,
            ..Default::default()
        };
        let operation_payload = operation.encode_to_vec();
        let operation_payload_digest = digest_bytes(&operation_payload);
        let error_payload = match operation.terminal_snapshot.as_ref() {
            Some(pb::durable_operation::TerminalSnapshot::ErrorSnapshot(snapshot)) => {
                snapshot.payload.clone()
            }
            _ => unreachable!("migration operation always has an error snapshot"),
        };
        let error_payload_digest = digest_bytes(&error_payload);
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO manager_operation
                (operation_id, method, idempotency_scope, request_digest, state, attempt,
                 manager_sequence, generation, causal_parent, sync_subject, affects_ready,
                 error_snapshot, created_at, updated_at, error_snapshot_schema,
                 error_snapshot_digest, operation_payload, operation_payload_schema,
                 operation_payload_digest)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, NULL, NULL, 0, ?8, ?9, ?9,
                     ?10, ?11, ?12, ?13, ?14)",
            params![
                operation.operation_id,
                operation.method,
                operation.idempotency_scope,
                operation.request_digest,
                operation.state,
                i64::try_from(stamp.0).map_err(|_| ManagerStorageError::Transaction(
                    "migration manager sequence exceeds SQLite range".into()
                ))?,
                i64::try_from(stamp.1).map_err(|_| ManagerStorageError::Transaction(
                    "migration generation exceeds SQLite range".into()
                ))?,
                error_payload,
                now,
                "issue_tracker.manager.v1.ManagerError",
                error_payload_digest,
                operation_payload,
                "issue_tracker.manager.v1.DurableOperation",
                operation_payload_digest,
            ],
        )?;
        if inserted == 0 {
            let existing: Vec<u8> = transaction.query_row(
                "SELECT operation_payload FROM manager_operation WHERE operation_id = ?1",
                params![operation_id],
                |row| row.get(0),
            )?;
            if existing != operation.encode_to_vec() {
                return Err(ManagerStorageError::Transaction(
                    "legacy mapping migration operation ID conflicts with different content".into(),
                ));
            }
            continue;
        }
        let (candidate_id, issue_id) = match operation.scope.as_ref() {
            Some(pb::durable_operation::Scope::CandidateId(candidate_id)) => {
                (Some(candidate_id.clone()), None)
            }
            Some(pb::durable_operation::Scope::IssueId(issue_id)) => (None, Some(issue_id.clone())),
            _ => (None, None),
        };
        transaction.execute(
            "INSERT INTO manager_operation_scope (operation_id, candidate_id, issue_id)
             VALUES (?1, ?2, ?3)",
            params![operation_id, candidate_id, issue_id],
        )?;
    }
    Ok(())
}

fn migration_commit_stamp(
    transaction: &Transaction<'_>,
) -> Result<(u64, u64), ManagerStorageError> {
    let sequence = metadata_value(transaction, "manager_sequence")?
        .parse::<u64>()
        .map_err(|_| ManagerStorageError::SchemaMismatch("manager_sequence is not numeric".into()))?
        .checked_add(1)
        .ok_or_else(|| ManagerStorageError::Transaction("manager sequence overflow".into()))?;
    let generation = metadata_value(transaction, "generation")?
        .parse::<u64>()
        .map_err(|_| ManagerStorageError::SchemaMismatch("generation is not numeric".into()))?
        .checked_add(1)
        .ok_or_else(|| ManagerStorageError::Transaction("manager generation overflow".into()))?;
    transaction.execute(
        "UPDATE manager_metadata SET value = ?1 WHERE key = 'manager_sequence'",
        params![sequence.to_string()],
    )?;
    transaction.execute(
        "UPDATE manager_metadata SET value = ?1 WHERE key = 'generation'",
        params![generation.to_string()],
    )?;
    Ok((sequence, generation))
}

fn metadata_value(transaction: &Transaction<'_>, key: &str) -> Result<String, ManagerStorageError> {
    transaction
        .query_row(
            "SELECT value FROM manager_metadata WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .map_err(ManagerStorageError::from)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

/// Rebuild the query-only history projection from its durable-operation
/// source of truth; no new audit event is created during migration.
fn backfill_operation_history_projection(
    transaction: &Transaction<'_>,
) -> Result<(), ManagerStorageError> {
    let operations = {
        let mut statement = transaction.prepare(
            "SELECT operation.operation_id, operation.method, operation.state,
                    operation.created_at, operation.updated_at,
                    operation.operation_payload, operation.operation_payload_schema,
                    operation.operation_payload_digest,
                    scope.candidate_id, scope.evidence_id, scope.extraction_run_id, scope.issue_id
               FROM manager_operation AS operation
          LEFT JOIN manager_operation_scope AS scope
                 ON scope.operation_id = operation.operation_id
              ORDER BY operation.operation_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (
        operation_id,
        method,
        state,
        created_at,
        updated_at,
        payload,
        schema,
        digest,
        candidate_id,
        evidence_id,
        extraction_run_id,
        issue_id,
    ) in operations
    {
        let (Some(payload), Some(schema), Some(digest)) = (payload, schema, digest) else {
            return Err(ManagerStorageError::Transaction(format!(
                "durable operation {operation_id} has no verifiable payload for history backfill"
            )));
        };
        if schema != "issue_tracker.manager.v1.DurableOperation" || digest_bytes(&payload) != digest
        {
            return Err(ManagerStorageError::Transaction(format!(
                "durable operation {operation_id} payload integrity validation failed during history backfill"
            )));
        }
        let operation = pb::DurableOperation::decode(payload.as_slice()).map_err(|error| {
            ManagerStorageError::Transaction(format!(
                "durable operation {operation_id} cannot be decoded for history backfill: {error}"
            ))
        })?;
        if operation.operation_id != operation_id
            || operation.method != method
            || i64::from(operation.state) != state
            || operation
                .created_at
                .as_ref()
                .map(|instant| instant.unix_epoch_millis)
                != Some(created_at)
        {
            return Err(ManagerStorageError::Transaction(format!(
                "durable operation {operation_id} does not match its history source columns"
            )));
        }
        let targets = [
            ("candidate", candidate_id),
            ("evidence", evidence_id),
            ("extraction_run", extraction_run_id),
            ("issue", issue_id),
        ]
        .into_iter()
        .filter_map(|(kind, id)| id.map(|id| (kind, id)))
        .collect::<Vec<_>>();
        if targets.len() > 1 {
            return Err(ManagerStorageError::Transaction(format!(
                "durable operation {operation_id} has multiple direct targets"
            )));
        }
        let (target_kind, target_id) = targets
            .into_iter()
            .next()
            .map_or((None, None), |(kind, id)| (Some(kind), Some(id)));
        transaction.execute(
            "INSERT INTO manager_operation_history_projection
                (operation_id, method, state, direct_target_kind, direct_target_id,
                 created_at, updated_at, resolution_required)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                operation_id,
                method,
                state,
                target_kind,
                target_id,
                created_at,
                updated_at,
                i64::from(operation.resolution_required),
            ],
        )?;
        let affected_issue_ids = operation
            .affected_issue_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        for issue_id in affected_issue_ids {
            if issue_id.trim().is_empty() {
                return Err(ManagerStorageError::Transaction(format!(
                    "durable operation {operation_id} has an empty affected Issue ID"
                )));
            }
            transaction.execute(
                "INSERT INTO manager_operation_history_issue (operation_id, issue_id)
                 VALUES (?1, ?2)",
                params![operation_id, issue_id],
            )?;
        }
    }
    Ok(())
}

fn load_or_initialize_state(
    connection: &mut Connection,
    config: &ManagerConfig,
) -> Result<ManagerState, ManagerStorageError> {
    let transaction = connection.transaction()?;
    let existing: Option<String> = transaction
        .query_row(
            "SELECT value FROM manager_metadata WHERE key = 'data_root_id'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(data_root_id) = existing {
        verify_metadata(&transaction, config, &data_root_id)?;
    } else {
        initialize_metadata(&transaction, config)?;
    }
    let state = read_state(&transaction, config)?;
    transaction.commit()?;
    Ok(state)
}

fn initialize_metadata(
    transaction: &Transaction<'_>,
    config: &ManagerConfig,
) -> Result<(), ManagerStorageError> {
    let manager = config.manager_identity();
    let taskchampion = config.taskchampion_identity();
    for (key, value) in [
        ("data_root_id", manager.data_root_id.clone()),
        ("manager_store_id", manager.store_id.clone()),
        ("taskchampion_store_id", taskchampion.store_id.clone()),
        ("store_pair_id", manager.store_pair_id.clone()),
        (
            "contract_fingerprint",
            config.expected_contract_fingerprint.clone(),
        ),
        (
            "schema_fingerprint",
            config.expected_schema_fingerprint.clone(),
        ),
        ("manager_sequence", "0".into()),
        ("generation", "0".into()),
        ("fencing_epoch", manager.fencing_epoch.to_string()),
        (
            LIST_ISSUES_WITH_SYNC_PAGE_TOKEN_SECRET_KEY,
            generate_list_issues_with_sync_page_token_secret(),
        ),
    ] {
        transaction.execute(
            "INSERT INTO manager_metadata (key, value) VALUES (?1, ?2)",
            params![key, value],
        )?;
    }
    for identity in &config.store_identities {
        transaction.execute(
            "INSERT INTO manager_store_identity
             (role, data_root_id, store_id, schema_fingerprint, generation, store_pair_id, fencing_epoch, contract_fingerprint)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                identity.role.as_i32(),
                identity.data_root_id,
                identity.store_id,
                identity.schema_fingerprint,
                identity.generation,
                identity.store_pair_id,
                identity.fencing_epoch,
                identity.contract_fingerprint,
            ],
        )?;
    }
    Ok(())
}

fn generate_list_issues_with_sync_page_token_secret() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn read_list_issues_with_sync_page_token_secret(
    connection: &Connection,
) -> Result<String, ManagerStorageError> {
    let secret: Option<String> = connection
        .query_row(
            "SELECT value FROM manager_metadata WHERE key = ?1",
            params![LIST_ISSUES_WITH_SYNC_PAGE_TOKEN_SECRET_KEY],
            |row| row.get(0),
        )
        .optional()?;
    match secret
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        Some(secret) => Ok(secret),
        None => Err(ManagerStorageError::SchemaMismatch(
            "list page token secret is missing or invalid".into(),
        )),
    }
}

fn verify_metadata(
    transaction: &Transaction<'_>,
    config: &ManagerConfig,
    existing_root_id: &str,
) -> Result<(), ManagerStorageError> {
    let expected = [
        (
            "data_root_id",
            config.manager_identity().data_root_id.as_str(),
        ),
        (
            "manager_store_id",
            config.manager_identity().store_id.as_str(),
        ),
        (
            "taskchampion_store_id",
            config.taskchampion_identity().store_id.as_str(),
        ),
        (
            "store_pair_id",
            config.manager_identity().store_pair_id.as_str(),
        ),
        (
            "contract_fingerprint",
            config.expected_contract_fingerprint.as_str(),
        ),
        (
            "schema_fingerprint",
            config.expected_schema_fingerprint.as_str(),
        ),
    ];
    for (key, value) in expected {
        let actual: String = transaction.query_row(
            "SELECT value FROM manager_metadata WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )?;
        if actual != value {
            return Err(ManagerStorageError::StoreIdentityMismatch(format!(
                "metadata {key} does not match configured identity"
            )));
        }
    }
    if existing_root_id != config.manager_identity().data_root_id {
        return Err(ManagerStorageError::StoreIdentityMismatch(
            "data_root_id does not match configured identity".into(),
        ));
    }
    Ok(())
}

fn read_state(
    transaction: &Transaction<'_>,
    config: &ManagerConfig,
) -> Result<ManagerState, ManagerStorageError> {
    let value = |key: &str| -> Result<String, ManagerStorageError> {
        Ok(transaction.query_row(
            "SELECT value FROM manager_metadata WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )?)
    };
    Ok(ManagerState {
        data_root_id: value("data_root_id")?,
        manager_store_id: value("manager_store_id")?,
        taskchampion_store_id: value("taskchampion_store_id")?,
        store_pair_id: value("store_pair_id")?,
        contract_fingerprint: value("contract_fingerprint")?,
        schema_fingerprint: value("schema_fingerprint")?,
        manager_sequence: value("manager_sequence")?.parse().map_err(|_| {
            ManagerStorageError::SchemaMismatch("manager_sequence is not numeric".into())
        })?,
        generation: value("generation")?
            .parse()
            .map_err(|_| ManagerStorageError::SchemaMismatch("generation is not numeric".into()))?,
        fencing_epoch: value("fencing_epoch")?.parse().map_err(|_| {
            ManagerStorageError::SchemaMismatch("fencing_epoch is not numeric".into())
        })?,
        root: config.data_root.clone(),
    })
}

fn update_counter(
    transaction: &Transaction<'_>,
    key: &str,
    value: u64,
) -> Result<(), ManagerStorageError> {
    let updated = transaction.execute(
        "UPDATE manager_metadata SET value = ?1 WHERE key = ?2",
        params![value.to_string(), key],
    )?;
    if updated != 1 {
        return Err(ManagerStorageError::SchemaMismatch(format!(
            "metadata key {key} is missing"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
