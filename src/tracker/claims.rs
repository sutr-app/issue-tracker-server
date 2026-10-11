use super::{
    actor::ActorIdentity,
    artifact_fs::*,
    artifacts::set,
    issue_write::{
        LeaseState, expired_lease_error, inconsistent_lease_error, invalid_lease_error, lease_state,
    },
    operation_log::*,
    task_mapping::*,
    *,
};

impl Tracker {
    pub async fn claim(
        &self,
        id: Uuid,
        owner: &str,
        expected_revision: u64,
        ttl_secs: u64,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.claim_with_context(
            id,
            owner,
            None,
            expected_revision,
            ttl_secs,
            operation_id,
            operation_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn claim_with_context(
        &self,
        id: Uuid,
        owner: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        expected_revision: u64,
        ttl_secs: u64,
        operation_id: &str,
        correlation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let owner = owner.into();
        reject_secret([owner.actor_id(), operation_id, correlation_id])?;
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let claim_digest = marker_digest(&[
            id.to_string().as_str(),
            owner.actor_id(),
            "claim",
            session_id.unwrap_or_default(),
            correlation_id,
            expected_revision.to_string().as_str(),
            ttl_secs.to_string().as_str(),
        ]);
        if operation_replayed(&task, operation_id, &claim_digest)? {
            return Err(TrackerError::ResultUnconfirmed);
        }
        let revision = revision(&task)?;
        if revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision,
            });
        }
        let lease = lease_state(&task, now());
        if lease != LeaseState::None {
            return Err(match lease {
                LeaseState::Expired => expired_lease_error(&task),
                LeaseState::Inconsistent => inconsistent_lease_error(&task),
                _ => invalid_lease_error(&task),
            });
        }
        if task.get_user_defined_attribute(STATE_KEY) != Some(IssueState::Open.as_str()) {
            return Err(TrackerError::InvalidState(
                "only open issues can be claimed".into(),
            ));
        }
        let mut ops = Operations::new();
        set(
            &mut task,
            STATE_KEY,
            IssueState::InProgress.as_str(),
            &mut ops,
        )?;
        task.set_status(Status::Pending, &mut ops)?;
        set(
            &mut task,
            REVISION_KEY,
            &(revision + 1).to_string(),
            &mut ops,
        )?;
        set(
            &mut task,
            LEASE_OWNER_KEY,
            &hash(owner.actor_id()),
            &mut ops,
        )?;
        set(
            &mut task,
            LEASE_OWNER_CONTEXT_KEY,
            &serde_json::to_string(&actor_value_with_session(
                &owner,
                session_id,
                correlation_id,
            ))
            .map_err(|error| TrackerError::InvalidState(error.to_string()))?,
            &mut ops,
        )?;
        let lease_handle = Uuid::new_v4().to_string();
        set(&mut task, LEASE_HANDLE_KEY, &hash(&lease_handle), &mut ops)?;
        set(
            &mut task,
            LEASE_EXPIRES_KEY,
            &(now() + ttl_secs).to_string(),
            &mut ops,
        )?;
        set_history_with_context(
            &mut task,
            operation_id,
            "claim",
            revision + 1,
            &claim_digest,
            &owner,
            correlation_id,
            Some(IssueState::Open.as_str()),
            Some(IssueState::InProgress.as_str()),
            None,
            &mut ops,
        )?;
        set_history_session(&mut task, operation_id, session_id, &mut ops)?;
        set_marker_non_replayable(&mut task, operation_id, &claim_digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        let mut issue = self.issue_from_task(&task)?;
        issue.lease_handle = Some(lease_handle);
        Ok(issue)
    }

    pub async fn release(
        &self,
        id: Uuid,
        owner: &str,
        lease_handle: &str,
        expected_revision: u64,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.release_with_context(
            id,
            owner,
            None,
            lease_handle,
            expected_revision,
            operation_id,
            operation_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn release_with_context(
        &self,
        id: Uuid,
        owner: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        lease_handle: &str,
        expected_revision: u64,
        operation_id: &str,
        correlation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let owner = owner.into();
        reject_secret([
            owner.actor_id(),
            session_id.unwrap_or_default(),
            operation_id,
            correlation_id,
        ])?;
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        reject_secret([lease_handle])?;
        let release_digest = marker_digest(&[
            id.to_string().as_str(),
            owner.actor_id(),
            "release",
            lease_handle,
            session_id.unwrap_or_default(),
            correlation_id,
            expected_revision.to_string().as_str(),
        ]);
        if operation_replayed(&task, operation_id, &release_digest)? {
            return self.issue_from_task(&task);
        }
        let lease = lease_state(&task, now());
        if lease != LeaseState::Active
            || task.get_user_defined_attribute(LEASE_OWNER_KEY)
                != Some(hash(owner.actor_id()).as_str())
            || task.get_user_defined_attribute(LEASE_HANDLE_KEY)
                != Some(hash(lease_handle).as_str())
        {
            return Err(match lease {
                LeaseState::Expired => expired_lease_error(&task),
                LeaseState::Inconsistent => inconsistent_lease_error(&task),
                _ => invalid_lease_error(&task),
            });
        }
        let revision = revision(&task)?;
        if revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision,
            });
        }
        let current_state =
            IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
        let next_state = if current_state == IssueState::InProgress {
            IssueState::Open
        } else {
            current_state
        };
        let mut ops = Operations::new();
        if next_state != current_state {
            set(&mut task, STATE_KEY, next_state.as_str(), &mut ops)?;
            task.set_status(next_state.native_status(), &mut ops)?;
        }
        set(
            &mut task,
            REVISION_KEY,
            &(revision + 1).to_string(),
            &mut ops,
        )?;
        clear(&mut task, LEASE_OWNER_KEY, &mut ops)?;
        clear(&mut task, LEASE_OWNER_CONTEXT_KEY, &mut ops)?;
        clear(&mut task, LEASE_HANDLE_KEY, &mut ops)?;
        clear(&mut task, LEASE_EXPIRES_KEY, &mut ops)?;
        set_history_with_context(
            &mut task,
            operation_id,
            "release",
            revision + 1,
            &release_digest,
            &owner,
            correlation_id,
            Some(current_state.as_str()),
            Some(next_state.as_str()),
            None,
            &mut ops,
        )?;
        set_history_session(&mut task, operation_id, session_id, &mut ops)?;
        set_marker(&mut task, operation_id, &release_digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }
}
