use super::{
    actor::ActorIdentity,
    artifact_fs::*,
    artifacts::set,
    issue_write::{LeaseState, invalid_lease_error, lease_state},
    operation_log::*,
    task_mapping::*,
    *,
};

impl Tracker {
    pub async fn relate(
        &self,
        source: Uuid,
        relation: RelationKind,
        target: Uuid,
        expected_revision: u64,
        target_expected_revision: u64,
        operation_id: &str,
    ) -> Result<(), TrackerError> {
        self.relate_with_context(
            source,
            relation,
            target,
            expected_revision,
            target_expected_revision,
            operation_id,
            ActorIdentity::unknown(),
            None,
            operation_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn relate_with_context(
        &self,
        source: Uuid,
        relation: RelationKind,
        target: Uuid,
        expected_revision: u64,
        target_expected_revision: u64,
        operation_id: &str,
        actor: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        correlation_id: &str,
    ) -> Result<(), TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        reject_secret([operation_id, actor.actor_id(), correlation_id])?;
        if source == target {
            return Err(TrackerError::InvalidRelation(
                "an issue cannot relate to itself".into(),
            ));
        }
        let mut replica = self.inner.lock().await;
        let mut source_task = replica
            .get_task(source)
            .await?
            .ok_or(TrackerError::NotFound(source))?;
        let mut target_task = replica
            .get_task(target)
            .await?
            .ok_or(TrackerError::NotFound(target))?;
        if matches!(relation, RelationKind::Blocks) {
            let relation_digest = marker_digest(&[
                source.to_string().as_str(),
                relation.key(),
                target.to_string().as_str(),
                expected_revision.to_string().as_str(),
                target_expected_revision.to_string().as_str(),
                "relate",
            ]);
            if operation_replayed(&target_task, operation_id, &relation_digest)? {
                return Ok(());
            }
            let revision = revision(&target_task)?;
            if revision != target_expected_revision {
                return Err(TrackerError::RevisionConflict {
                    expected: target_expected_revision,
                    actual: revision,
                });
            }
            let depmap = replica.dependency_map(true).await?;
            if source == target || reaches(&depmap, source, target) {
                return Err(TrackerError::InvalidRelation("blocking cycle".into()));
            }
            let mut ops = Operations::new();
            target_task.add_dependency(source, &mut ops)?;
            set(
                &mut target_task,
                REVISION_KEY,
                &(revision + 1).to_string(),
                &mut ops,
            )?;
            set_history_with_context(
                &mut target_task,
                operation_id,
                "relate",
                revision + 1,
                &relation_digest,
                &actor,
                correlation_id,
                None,
                None,
                None,
                &mut ops,
            )?;
            set_history_subject_fields(
                &mut target_task,
                operation_id,
                &[
                    (
                        "relation_id",
                        Value::String(relation_id(relation, source, target)),
                    ),
                    ("relation_kind", Value::String(relation.key().into())),
                    ("source_issue_id", Value::String(source.to_string())),
                    ("target_issue_id", Value::String(target.to_string())),
                    ("marker_anchor_issue_id", Value::String(target.to_string())),
                ],
                &mut ops,
            )?;
            set_history_session(&mut target_task, operation_id, session_id, &mut ops)?;
            set(
                &mut target_task,
                &relation_metadata_key(&relation_id(relation, source, target)),
                &serde_json::json!({
                    "created_at": now(),
                    "created_by": actor_value_with_session(&actor, session_id, correlation_id),
                })
                .to_string(),
                &mut ops,
            )?;
            set(
                &mut source_task,
                &relation_metadata_key(&relation_id(relation, source, target)),
                &serde_json::json!({
                    "created_at": now(),
                    "created_by": actor_value_with_session(&actor, session_id, correlation_id),
                })
                .to_string(),
                &mut ops,
            )?;
            set_marker(&mut target_task, operation_id, &relation_digest, &mut ops)?;
            replica.commit_operations(ops).await?;
            self.advance_read_generation();
            return Ok(());
        }
        let relation_digest = marker_digest(&[
            source.to_string().as_str(),
            relation.key(),
            target.to_string().as_str(),
            expected_revision.to_string().as_str(),
            target_expected_revision.to_string().as_str(),
            "relate",
        ]);
        if operation_replayed(&source_task, operation_id, &relation_digest)? {
            return Ok(());
        }
        let source_revision = revision(&source_task)?;
        if source_revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: source_revision,
            });
        }
        let outgoing_key = format!("lb.relation.out.{}", relation.key());
        let incoming_key = format!("lb.relation.in.{}", relation.key());
        let mut values = relation_values(&source_task, &outgoing_key)?;
        if !values.contains(&target) {
            values.push(target);
        }
        let mut reverse_values = relation_values(&target_task, &incoming_key)?;
        if !reverse_values.contains(&source) {
            reverse_values.push(source);
        }
        let mut ops = Operations::new();
        set(
            &mut source_task,
            &outgoing_key,
            &serde_json::to_string(&values).unwrap(),
            &mut ops,
        )?;
        set(
            &mut source_task,
            REVISION_KEY,
            &(source_revision + 1).to_string(),
            &mut ops,
        )?;
        set(
            &mut target_task,
            &incoming_key,
            &serde_json::to_string(&reverse_values).unwrap(),
            &mut ops,
        )?;
        let target_revision = revision(&target_task)?;
        if target_revision != target_expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: target_expected_revision,
                actual: target_revision,
            });
        }
        set(
            &mut target_task,
            REVISION_KEY,
            &(target_revision + 1).to_string(),
            &mut ops,
        )?;
        set_history_with_context(
            &mut source_task,
            operation_id,
            "relate",
            source_revision + 1,
            &relation_digest,
            &actor,
            correlation_id,
            None,
            None,
            None,
            &mut ops,
        )?;
        set_history_subject_fields(
            &mut source_task,
            operation_id,
            &[
                (
                    "relation_id",
                    Value::String(relation_id(relation, source, target)),
                ),
                ("relation_kind", Value::String(relation.key().into())),
                ("source_issue_id", Value::String(source.to_string())),
                ("target_issue_id", Value::String(target.to_string())),
            ],
            &mut ops,
        )?;
        set_history_session(&mut source_task, operation_id, session_id, &mut ops)?;
        set_history_with_context(
            &mut target_task,
            operation_id,
            "relate",
            target_revision + 1,
            &relation_digest,
            &actor,
            correlation_id,
            None,
            None,
            None,
            &mut ops,
        )?;
        set_history_subject_fields(
            &mut target_task,
            operation_id,
            &[
                (
                    "relation_id",
                    Value::String(relation_id(relation, source, target)),
                ),
                ("relation_kind", Value::String(relation.key().into())),
                ("source_issue_id", Value::String(source.to_string())),
                ("target_issue_id", Value::String(target.to_string())),
            ],
            &mut ops,
        )?;
        set_history_session(&mut target_task, operation_id, session_id, &mut ops)?;
        let relation_metadata = serde_json::json!({
            "created_at": now(),
            "created_by": actor_value_with_session(&actor, session_id, correlation_id),
        })
        .to_string();
        set(
            &mut source_task,
            &relation_metadata_key(&relation_id(relation, source, target)),
            &relation_metadata,
            &mut ops,
        )?;
        set(
            &mut target_task,
            &relation_metadata_key(&relation_id(relation, source, target)),
            &relation_metadata,
            &mut ops,
        )?;
        set_marker(&mut source_task, operation_id, &relation_digest, &mut ops)?;
        set_marker(&mut target_task, operation_id, &relation_digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        Ok(())
    }

    pub async fn unrelate(
        &self,
        source: Uuid,
        relation: RelationKind,
        target: Uuid,
        expected_revision: u64,
        target_expected_revision: u64,
        operation_id: &str,
    ) -> Result<(), TrackerError> {
        self.unrelate_with_context(
            source,
            relation,
            target,
            expected_revision,
            target_expected_revision,
            operation_id,
            ActorIdentity::unknown(),
            None,
            operation_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn unrelate_with_context(
        &self,
        source: Uuid,
        relation: RelationKind,
        target: Uuid,
        expected_revision: u64,
        target_expected_revision: u64,
        operation_id: &str,
        actor: impl Into<ActorIdentity>,
        _session_id: Option<&str>,
        correlation_id: &str,
    ) -> Result<(), TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        reject_secret([operation_id, actor.actor_id(), correlation_id])?;
        if source == target {
            return Err(TrackerError::InvalidRelation(
                "an issue cannot relate to itself".into(),
            ));
        }
        let mut replica = self.inner.lock().await;
        let mut source_task = replica
            .get_task(source)
            .await?
            .ok_or(TrackerError::NotFound(source))?;
        let mut target_task = replica
            .get_task(target)
            .await?
            .ok_or(TrackerError::NotFound(target))?;
        let relation_digest = marker_digest(&[
            source.to_string().as_str(),
            relation.key(),
            target.to_string().as_str(),
            expected_revision.to_string().as_str(),
            target_expected_revision.to_string().as_str(),
            "unrelate",
        ]);
        let marker_task = if matches!(relation, RelationKind::Blocks) {
            &target_task
        } else {
            &source_task
        };
        if operation_replayed(marker_task, operation_id, &relation_digest)? {
            return Ok(());
        }
        let mut ops = Operations::new();
        if matches!(relation, RelationKind::Blocks) {
            let target_revision = revision(&target_task)?;
            if target_revision != target_expected_revision {
                return Err(TrackerError::RevisionConflict {
                    expected: target_expected_revision,
                    actual: target_revision,
                });
            }
            if target_task
                .get_dependencies()
                .any(|dependency| dependency == source)
            {
                target_task.remove_dependency(source, &mut ops)?;
                set(
                    &mut target_task,
                    REVISION_KEY,
                    &(target_revision + 1).to_string(),
                    &mut ops,
                )?;
            }
            set_history_with_context(
                &mut target_task,
                operation_id,
                "unrelate",
                target_revision + 1,
                &relation_digest,
                &actor,
                correlation_id,
                None,
                None,
                None,
                &mut ops,
            )?;
            set_history_subject_fields(
                &mut target_task,
                operation_id,
                &[
                    (
                        "relation_id",
                        Value::String(relation_id(relation, source, target)),
                    ),
                    ("relation_kind", Value::String(relation.key().into())),
                    ("source_issue_id", Value::String(source.to_string())),
                    ("target_issue_id", Value::String(target.to_string())),
                    ("marker_anchor_issue_id", Value::String(target.to_string())),
                ],
                &mut ops,
            )?;
            set_history_session(&mut target_task, operation_id, _session_id, &mut ops)?;
            clear(
                &mut target_task,
                &relation_metadata_key(&relation_id(relation, source, target)),
                &mut ops,
            )?;
            clear(
                &mut source_task,
                &relation_metadata_key(&relation_id(relation, source, target)),
                &mut ops,
            )?;
            set_marker(&mut target_task, operation_id, &relation_digest, &mut ops)?;
        } else {
            let source_revision = revision(&source_task)?;
            if source_revision != expected_revision {
                return Err(TrackerError::RevisionConflict {
                    expected: expected_revision,
                    actual: source_revision,
                });
            }
            let target_revision = revision(&target_task)?;
            if target_revision != target_expected_revision {
                return Err(TrackerError::RevisionConflict {
                    expected: target_expected_revision,
                    actual: target_revision,
                });
            }
            let outgoing_key = format!("lb.relation.out.{}", relation.key());
            let incoming_key = format!("lb.relation.in.{}", relation.key());
            let mut values = relation_values(&source_task, &outgoing_key)?;
            let mut reverse_values = relation_values(&target_task, &incoming_key)?;
            let source_changed = values
                .iter()
                .position(|value| *value == target)
                .map(|index| {
                    values.remove(index);
                    true
                })
                .unwrap_or(false);
            let target_changed = reverse_values
                .iter()
                .position(|value| *value == source)
                .map(|index| {
                    reverse_values.remove(index);
                    true
                })
                .unwrap_or(false);
            if source_changed {
                write_relation(&mut source_task, &outgoing_key, &values, &mut ops)?;
                set(
                    &mut source_task,
                    REVISION_KEY,
                    &(source_revision + 1).to_string(),
                    &mut ops,
                )?;
            }
            if target_changed {
                write_relation(&mut target_task, &incoming_key, &reverse_values, &mut ops)?;
                set(
                    &mut target_task,
                    REVISION_KEY,
                    &(target_revision + 1).to_string(),
                    &mut ops,
                )?;
            }
            set_history_with_context(
                &mut source_task,
                operation_id,
                "unrelate",
                source_revision + u64::from(source_changed),
                &relation_digest,
                &actor,
                correlation_id,
                None,
                None,
                None,
                &mut ops,
            )?;
            set_history_subject_fields(
                &mut source_task,
                operation_id,
                &[
                    (
                        "relation_id",
                        Value::String(relation_id(relation, source, target)),
                    ),
                    ("relation_kind", Value::String(relation.key().into())),
                    ("source_issue_id", Value::String(source.to_string())),
                    ("target_issue_id", Value::String(target.to_string())),
                ],
                &mut ops,
            )?;
            set_history_session(&mut source_task, operation_id, _session_id, &mut ops)?;
            if target_changed {
                set_history_with_context(
                    &mut target_task,
                    operation_id,
                    "unrelate",
                    target_revision + 1,
                    &relation_digest,
                    &actor,
                    correlation_id,
                    None,
                    None,
                    None,
                    &mut ops,
                )?;
                set_history_subject_fields(
                    &mut target_task,
                    operation_id,
                    &[
                        (
                            "relation_id",
                            Value::String(relation_id(relation, source, target)),
                        ),
                        ("relation_kind", Value::String(relation.key().into())),
                        ("source_issue_id", Value::String(source.to_string())),
                        ("target_issue_id", Value::String(target.to_string())),
                    ],
                    &mut ops,
                )?;
                set_history_session(&mut target_task, operation_id, _session_id, &mut ops)?;
            }
            clear(
                &mut source_task,
                &relation_metadata_key(&relation_id(relation, source, target)),
                &mut ops,
            )?;
            clear(
                &mut target_task,
                &relation_metadata_key(&relation_id(relation, source, target)),
                &mut ops,
            )?;
            set_marker(&mut source_task, operation_id, &relation_digest, &mut ops)?;
            set_marker(&mut target_task, operation_id, &relation_digest, &mut ops)?;
        }
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        Ok(())
    }

    pub async fn heartbeat(
        &self,
        id: Uuid,
        owner: &str,
        lease_handle: &str,
        expected_revision: u64,
        ttl_secs: u64,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.heartbeat_with_context(
            id,
            owner,
            None,
            lease_handle,
            expected_revision,
            ttl_secs,
            operation_id,
            operation_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn heartbeat_with_context(
        &self,
        id: Uuid,
        owner: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        lease_handle: &str,
        expected_revision: u64,
        ttl_secs: u64,
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
        let heartbeat_digest = marker_digest(&[
            id.to_string().as_str(),
            "heartbeat",
            owner.actor_id(),
            lease_handle,
            session_id.unwrap_or_default(),
            correlation_id,
            expected_revision.to_string().as_str(),
            ttl_secs.to_string().as_str(),
        ]);
        if operation_replayed(&task, operation_id, &heartbeat_digest)? {
            return self.issue_from_task(&task);
        }
        if lease_state(&task, now()) != LeaseState::Active
            || task.get_user_defined_attribute(LEASE_OWNER_KEY)
                != Some(hash(owner.actor_id()).as_str())
            || task.get_user_defined_attribute(LEASE_HANDLE_KEY)
                != Some(hash(lease_handle).as_str())
        {
            return Err(invalid_lease_error(&task));
        }
        let revision = revision(&task)?;
        if revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision,
            });
        }
        let mut ops = Operations::new();
        set(
            &mut task,
            REVISION_KEY,
            &(revision + 1).to_string(),
            &mut ops,
        )?;
        set(
            &mut task,
            LEASE_EXPIRES_KEY,
            &(now() + ttl_secs).to_string(),
            &mut ops,
        )?;
        set_history_with_context(
            &mut task,
            operation_id,
            "heartbeat",
            revision + 1,
            &heartbeat_digest,
            &owner,
            correlation_id,
            None,
            None,
            None,
            &mut ops,
        )?;
        set_history_session(&mut task, operation_id, session_id, &mut ops)?;
        set_marker(&mut task, operation_id, &heartbeat_digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }
    pub async fn comment(
        &self,
        id: Uuid,
        comment: &str,
        expected_revision: u64,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.comment_with_context(
            id,
            comment,
            expected_revision,
            ActorIdentity::unknown(),
            operation_id,
            operation_id,
        )
        .await
    }

    pub async fn comment_with_context(
        &self,
        id: Uuid,
        comment: &str,
        expected_revision: u64,
        actor: impl Into<ActorIdentity>,
        correlation_id: &str,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.comment_with_id_context(
            id,
            operation_id,
            comment,
            expected_revision,
            actor,
            correlation_id,
            operation_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn comment_with_id_context(
        &self,
        id: Uuid,
        comment_id: &str,
        comment: &str,
        expected_revision: u64,
        actor: impl Into<ActorIdentity>,
        correlation_id: &str,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.comment_with_id_session_context(
            id,
            comment_id,
            comment,
            expected_revision,
            actor,
            None,
            correlation_id,
            operation_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn comment_with_id_session_context(
        &self,
        id: Uuid,
        comment_id: &str,
        comment: &str,
        expected_revision: u64,
        actor: impl Into<ActorIdentity>,
        session_id: Option<&str>,
        correlation_id: &str,
        operation_id: &str,
    ) -> Result<Issue, TrackerError> {
        self.ensure_healthy()?;
        let _admission = self.admission.read().await;
        let actor = actor.into();
        reject_secret([
            comment,
            comment_id,
            actor.actor_id(),
            session_id.unwrap_or_default(),
            correlation_id,
            operation_id,
        ])?;
        let mut replica = self.inner.lock().await;
        let mut task = replica
            .get_task(id)
            .await?
            .ok_or(TrackerError::NotFound(id))?;
        let comment_digest = marker_digest(&[
            id.to_string().as_str(),
            "comment",
            comment_id,
            comment,
            actor.actor_id(),
            session_id.unwrap_or_default(),
            correlation_id,
            expected_revision.to_string().as_str(),
        ]);
        if operation_replayed(&task, operation_id, &comment_digest)? {
            return self.issue_from_task(&task);
        }
        let revision = revision(&task)?;
        if revision != expected_revision {
            return Err(TrackerError::RevisionConflict {
                expected: expected_revision,
                actual: revision,
            });
        }
        let mut ops = Operations::new();
        set(
            &mut task,
            &format!("lb.comment.{}.{}", now(), operation_id),
            &serde_json::json!({
                "comment_id": comment_id,
                "actor": actor.actor_id(),
                "actor_kind": actor.kind() as i32,
                "agent_session_id": session_id,
                "timestamp": now(),
                "correlation_id": correlation_id,
                "body": comment,
            })
            .to_string(),
            &mut ops,
        )?;
        set(
            &mut task,
            REVISION_KEY,
            &(revision + 1).to_string(),
            &mut ops,
        )?;
        set_history_with_context(
            &mut task,
            operation_id,
            "comment",
            revision + 1,
            &comment_digest,
            &actor,
            correlation_id,
            None,
            None,
            None,
            &mut ops,
        )?;
        set_history_session(&mut task, operation_id, session_id, &mut ops)?;
        set_history_subject_fields(
            &mut task,
            operation_id,
            &[("comment_id", Value::String(comment_id.to_owned()))],
            &mut ops,
        )?;
        set_marker(&mut task, operation_id, &comment_digest, &mut ops)?;
        replica.commit_operations(ops).await?;
        self.advance_read_generation();
        self.issue_from_task(&task)
    }
}
