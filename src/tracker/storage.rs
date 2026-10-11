use super::{
    artifacts::set,
    operation_log::{OPERATION_FENCE_PREFIX, marker_digest},
    *,
};

/// Validate an existing TaskChampion root without opening it read-write.
///
/// The v7 tracker contract is intentionally a fresh-root boundary.  A
/// populated root from an earlier contract must be rejected before
/// `SqliteStorage::new(..., ReadWrite, ...)` can upgrade journal/schema state.
/// An on-disk database with no tasks is equivalent to a newly-created root and
/// may be initialized by `ensure_metadata` below.
async fn preflight_existing_database(root: &Path) -> Result<(), TrackerError> {
    let database_path = root.join("taskchampion.sqlite3");
    if !database_path.is_file() {
        return Err(TrackerError::InvalidState(
            "tracker database path must be a regular file".into(),
        ));
    }
    let storage = SqliteStorage::new(root, AccessMode::ReadOnly, false).await?;
    let mut replica = Replica::new(storage);
    if let Some(metadata) = replica.get_task_data(metadata_id()).await? {
        let expected_fingerprint = Tracker::schema_fingerprint();
        let expected_contract = crate::tracker::config::contract_fingerprint();
        let compatible = metadata.get(ENTITY_KEY) == Some(METADATA_ENTITY)
            && metadata.get(SCHEMA_VERSION_KEY) == Some(MAPPING_SCHEMA_VERSION)
            && metadata.get("lb.schema_fingerprint") == Some(expected_fingerprint.as_str())
            && metadata.get("lb.contract_fingerprint") == Some(expected_contract.as_str());
        if !compatible {
            return Err(TrackerError::InvalidState(
                "existing tracker root has an incompatible contract/schema identity".into(),
            ));
        }
        return Ok(());
    }

    // A database containing any task but no metadata is not a safe empty root:
    // interpreting those tasks under v7 could silently mutate user data.
    if !replica.all_task_uuids().await?.is_empty() {
        return Err(TrackerError::InvalidState(
            "existing tracker root is populated but missing metadata".into(),
        ));
    }
    Ok(())
}

impl Tracker {
    /// Hold the Tracker admission fence while a bounded application-level
    /// resolution transaction validates and commits an Issue observation.
    pub(crate) async fn acquire_admission_write(&self) -> OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.admission).write_owned().await
    }

    /// Persist a fence for one downstream operation while the caller holds
    /// the admission write guard.  The fence is stored on the Issue task but
    /// does not alter the canonical state or revision; all subsequent write
    /// paths consult it through `operation_replayed` before mutating.
    pub(crate) async fn persist_operation_fence(
        &self,
        issue_id: Uuid,
        operation_id: &str,
        resolution_operation_id: &str,
        request_digest: &str,
    ) -> Result<(), TrackerError> {
        if operation_id.trim().is_empty()
            || resolution_operation_id.trim().is_empty()
            || request_digest.trim().is_empty()
        {
            return Err(TrackerError::InvalidState(
                "operation fence identifiers are required".into(),
            ));
        }
        let mut replica = self.inner.lock().await;
        let Some(mut task) = replica.get_task(issue_id).await? else {
            return Err(TrackerError::NotFound(issue_id));
        };
        let key = format!("{OPERATION_FENCE_PREFIX}{operation_id}");
        if let Some(existing) = task.get_user_defined_attribute(&key) {
            let parsed: serde_json::Value = serde_json::from_str(existing)
                .map_err(|_| TrackerError::InvalidState("operation fence is corrupt".into()))?;
            if parsed.get("status").and_then(serde_json::Value::as_str) == Some("fenced") {
                // The first validated resolver establishes the permanent
                // operation fence. Later resolver retries may use a new
                // idempotency key, but must observe (not replace) the same
                // source-operation fence.
                return Ok(());
            }
            return Err(TrackerError::IdempotencyConflict);
        }
        let value = serde_json::json!({
            "status": "fenced",
            "operation_id": operation_id,
            "resolution_operation_id": resolution_operation_id,
            "request_digest": request_digest,
            "contract": crate::tracker::config::contract_fingerprint(),
            "schema_fingerprint": Self::schema_fingerprint(),
            "recorded_at": crate::tracker::now(),
        });
        let mut ops = taskchampion::Operations::new();
        set(&mut task, &key, &value.to_string(), &mut ops)?;
        replica.commit_operations(ops).await?;
        Ok(())
    }

    /// Read an Issue while the caller owns the admission write guard.  The
    /// normal public read path acquires a read guard and would deadlock when
    /// used during a resolution fence transaction.
    pub(crate) async fn get_while_admission_held(
        &self,
        issue_id: Uuid,
    ) -> Result<Issue, TrackerError> {
        let mut replica = self.inner.lock().await;
        let Some(task) = replica.get_task(issue_id).await? else {
            return Err(TrackerError::NotFound(issue_id));
        };
        self.issue_from_task(&task)
    }

    pub async fn open(root: impl AsRef<Path>) -> Result<Self, TrackerError> {
        Self::open_with_source_evidence_limits(root, SourceEvidenceLimits::UNBOUNDED).await
    }

    pub(crate) async fn open_with_source_evidence_limits(
        root: impl AsRef<Path>,
        source_evidence_limits: SourceEvidenceLimits,
    ) -> Result<Self, TrackerError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|e| TrackerError::Backup(e.to_string()))?;
        let lock_path = root.join("lookback-issue.lock");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|_| TrackerError::RootLocked)?;
        file.try_lock_exclusive()
            .map_err(|_| TrackerError::RootLocked)?;
        if root.join("taskchampion.sqlite3").exists()
            && let Err(error) = preflight_existing_database(&root).await
        {
            let _ = fs::remove_file(&lock_path);
            return Err(error);
        }
        let storage = match SqliteStorage::new(&root, AccessMode::ReadWrite, true).await {
            Ok(storage) => storage,
            Err(error) => {
                let _ = fs::remove_file(&lock_path);
                return Err(error.into());
            }
        };
        let tracker = Self {
            inner: Arc::new(Mutex::new(Replica::new(storage))),
            admission: Arc::new(RwLock::new(())),
            failed_closed: Arc::new(AtomicBool::new(false)),
            read_generation: Arc::new(AtomicU64::new(generation_seed())),
            page_token_secret: Arc::new(Uuid::new_v4().to_string()),
            root: Arc::new(root),
            source_evidence_limits,
            _lock: Arc::new(RootLock {
                path: lock_path,
                _file: file,
            }),
        };
        tracker.ensure_metadata().await?;
        Ok(tracker)
    }

    pub(crate) fn current_read_generation(&self) -> u64 {
        self.read_generation.load(Ordering::Acquire)
    }

    pub(crate) fn advance_read_generation(&self) {
        self.read_generation.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn validate_read_generation(&self, expected: u64) -> Result<(), TrackerError> {
        let current = self.current_read_generation();
        if current == expected {
            Ok(())
        } else {
            Err(TrackerError::PageTokenExpired {
                token_generation: expected,
                current_generation: current,
            })
        }
    }

    pub fn contract_version() -> &'static str {
        CONTRACT_VERSION
    }

    pub fn schema_fingerprint() -> String {
        format!("sha256:{:x}", Sha256::digest(SCHEMA_FINGERPRINT.as_bytes()))
    }

    /// Return the fingerprint of the Tracker wire/storage contract. This is
    /// public so bootstrap adapters can validate the paired store identity
    /// before opening the Tracker root.
    pub fn contract_fingerprint() -> String {
        crate::tracker::config::contract_fingerprint()
    }

    pub(crate) fn ensure_healthy(&self) -> Result<(), TrackerError> {
        if self.failed_closed.load(Ordering::Acquire) {
            Err(TrackerError::Unavailable)
        } else {
            Ok(())
        }
    }
    pub(crate) fn fail_closed(&self) {
        self.failed_closed.store(true, Ordering::Release);
    }

    pub(crate) async fn ensure_metadata(&self) -> Result<(), TrackerError> {
        let metadata_id = metadata_id();
        let _admission = self.admission.read().await;
        let mut replica = self.inner.lock().await;
        if let Some(task) = replica.get_task(metadata_id).await? {
            let expected_contract = crate::tracker::config::contract_fingerprint();
            if task.get_user_defined_attribute(ENTITY_KEY) != Some(METADATA_ENTITY)
                || task.get_user_defined_attribute(SCHEMA_VERSION_KEY)
                    != Some(MAPPING_SCHEMA_VERSION)
                || task.get_user_defined_attribute("lb.schema_fingerprint")
                    != Some(Self::schema_fingerprint().as_str())
                || task.get_user_defined_attribute("lb.contract_fingerprint")
                    != Some(expected_contract.as_str())
            {
                return Err(TrackerError::InvalidState(
                    "tracker metadata schema mismatch".into(),
                ));
            }
            return Ok(());
        }
        let mut ops = Operations::new();
        let mut task = replica.create_task(metadata_id, &mut ops).await?;
        task.set_description("Lookback internal Issue tracker metadata".into(), &mut ops)?;
        task.set_status(Status::Completed, &mut ops)?;
        set(&mut task, ENTITY_KEY, METADATA_ENTITY, &mut ops)?;
        set(
            &mut task,
            SCHEMA_VERSION_KEY,
            MAPPING_SCHEMA_VERSION,
            &mut ops,
        )?;
        set(
            &mut task,
            "lb.schema_fingerprint",
            &Self::schema_fingerprint(),
            &mut ops,
        )?;
        set(
            &mut task,
            "lb.contract_fingerprint",
            &crate::tracker::config::contract_fingerprint(),
            &mut ops,
        )?;
        set(
            &mut task,
            "lb.task_fingerprint",
            &marker_digest(&[METADATA_ENTITY, MAPPING_SCHEMA_VERSION]),
            &mut ops,
        )?;
        replica.commit_operations(ops).await?;
        Ok(())
    }
}
