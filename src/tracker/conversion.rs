use super::{actor::ActorIdentity, config::CallContext, task_mapping::relation_id, *};

pub(crate) fn decode_v1<T>(method: &str, args: &[u8]) -> Result<T, String>
where
    T: Message + Default,
{
    T::decode(args).map_err(|error| format!("{method} protobuf decode failed: {error}"))
}

pub(crate) fn call_context(
    context: Option<&v1::WriteContext>,
) -> Result<CallContext, TrackerError> {
    let context =
        context.ok_or_else(|| TrackerError::InvalidState("write context is required".into()))?;
    if context.downstream_operation_id.is_empty() {
        return Err(TrackerError::InvalidState(
            "downstream operation id is required".into(),
        ));
    }
    let (actor, session_id, correlation_id) = if let Some(request) = context.request.as_ref() {
        let caller = request.caller.as_ref();
        let actor = match caller.and_then(|caller| caller.actor.as_ref()) {
            Some(actor) => {
                let kind = v1::ActorKind::try_from(actor.kind).map_err(|_| {
                    TrackerError::InvalidState("write context actor kind is invalid".into())
                })?;
                ActorIdentity::new(actor.actor_id.clone(), kind)
            }
            None => ActorIdentity::unknown(),
        };
        (
            actor,
            caller.and_then(|caller| caller.agent_session_id.clone()),
            request.correlation_id.clone(),
        )
    } else {
        (
            ActorIdentity::unknown(),
            None,
            context.downstream_operation_id.clone(),
        )
    };
    Ok(CallContext {
        operation_id: context.downstream_operation_id.clone(),
        actor,
        session_id,
        correlation_id: if correlation_id.is_empty() {
            context.downstream_operation_id.clone()
        } else {
            correlation_id
        },
        reason: context.reason.clone(),
    })
}

pub(crate) fn parse_issue_id(value: &str) -> Result<Uuid, TrackerError> {
    Uuid::parse_str(value).map_err(|_| TrackerError::InvalidState("invalid issue id".into()))
}

pub(crate) fn instant(seconds: u64) -> v1::UtcInstant {
    v1::UtcInstant {
        unix_epoch_millis: seconds.saturating_mul(1000) as i64,
    }
}

pub(crate) fn issue_state(value: IssueState) -> i32 {
    match value {
        IssueState::Open => v1::IssueState::Open as i32,
        IssueState::InProgress => v1::IssueState::InProgress as i32,
        IssueState::Blocked => v1::IssueState::Blocked as i32,
        IssueState::Deferred => v1::IssueState::Deferred as i32,
        IssueState::Done => v1::IssueState::Done as i32,
        IssueState::Canceled => v1::IssueState::Canceled as i32,
        IssueState::AwaitingReview => v1::IssueState::AwaitingReview as i32,
    }
}

pub(crate) fn relation_kind(value: RelationKind) -> i32 {
    match value {
        RelationKind::ParentChild => v1::RelationKind::ParentChild as i32,
        RelationKind::Blocks => v1::RelationKind::Blocks as i32,
        RelationKind::Related => v1::RelationKind::Related as i32,
        RelationKind::DiscoveredFrom => v1::RelationKind::DiscoveredFrom as i32,
        RelationKind::Supersedes => v1::RelationKind::Supersedes as i32,
    }
}

pub(crate) fn relation_kind_value(value: i32) -> Result<RelationKind, TrackerError> {
    match v1::RelationKind::try_from(value) {
        Ok(v1::RelationKind::ParentChild) => Ok(RelationKind::ParentChild),
        Ok(v1::RelationKind::Blocks) => Ok(RelationKind::Blocks),
        Ok(v1::RelationKind::Related) => Ok(RelationKind::Related),
        Ok(v1::RelationKind::DiscoveredFrom) => Ok(RelationKind::DiscoveredFrom),
        Ok(v1::RelationKind::Supersedes) => Ok(RelationKind::Supersedes),
        _ => Err(TrackerError::InvalidRelation(
            "relation kind is required".into(),
        )),
    }
}

pub(crate) fn relation_kind_from_id(
    id: &str,
    source: Uuid,
    target: Uuid,
) -> Result<RelationKind, TrackerError> {
    for kind in [
        RelationKind::ParentChild,
        RelationKind::Blocks,
        RelationKind::Related,
        RelationKind::DiscoveredFrom,
        RelationKind::Supersedes,
    ] {
        if relation_id(kind, source, target) == id {
            return Ok(kind);
        }
    }
    Err(TrackerError::InvalidRelation(
        "relation_id is invalid".into(),
    ))
}

pub(crate) fn issue_proto(issue: &Issue) -> v1::Issue {
    v1::Issue {
        issue_id: issue.id.to_string(),
        backend_id: issue.id.to_string(),
        title: issue.title.clone(),
        description: issue.description.clone(),
        kind: issue.kind,
        priority: issue.priority,
        state: issue_state(issue.state),
        labels: issue.labels.clone(),
        acceptance_criteria: issue.acceptance_criteria.clone(),
        contexts: issue.contexts.iter().filter_map(context_ref).collect(),
        due_at: issue.due_at.map(|value| v1::UtcInstant {
            unix_epoch_millis: value,
        }),
        review_at: issue.review_at.map(|value| v1::UtcInstant {
            unix_epoch_millis: value,
        }),
        revision: issue.revision,
        created_by: issue.created_by.as_ref().and_then(actor_context_value),
        created_at: Some(v1::UtcInstant {
            unix_epoch_millis: issue.created_at.saturating_mul(1000) as i64,
        }),
        updated_at: Some(v1::UtcInstant {
            unix_epoch_millis: issue.updated_at.saturating_mul(1000) as i64,
        }),
        completed_at: issue.completed_at.map(instant),
        creation_basis: issue.creation_basis,
        confirmation_status: issue.confirmation_status,
        provenance_metadata: issue
            .provenance_metadata
            .iter()
            .filter_map(metadata_entry)
            .collect(),
        source_evidence: issue
            .source_evidence
            .iter()
            .filter_map(|value| serde_json::from_value(value.clone()).ok())
            .collect(),
        claim: issue.lease_expires_at.map(|expires| v1::ClaimView {
            owner: issue.lease_owner.as_ref().and_then(actor_context_value),
            lease_expires_at: Some(instant(expires)),
            claim_revision: issue.revision,
        }),
    }
}

pub(crate) fn relation_proto(relation: &IssueRelation) -> v1::IssueRelation {
    v1::IssueRelation {
        relation_id: relation.relation_id.clone(),
        kind: relation_kind(relation.kind),
        source_issue_id: relation.source.to_string(),
        target_issue_id: relation.target.to_string(),
        created_at: Some(instant(relation.created_at)),
        created_by: relation.created_by.as_ref().and_then(actor_context_value),
    }
}

pub(crate) fn actor_context_value(value: &Value) -> Option<v1::ActorContext> {
    let actor_id = value.get("actor_id")?.as_str()?.to_owned();
    // Actor metadata is a display projection; malformed values are
    // represented as Unspecified rather than upgraded to a trusted kind.
    let kind = super::actor::persisted_kind(value).unwrap_or(v1::ActorKind::Unspecified);
    Some(v1::ActorContext {
        actor: Some(v1::ActorRef {
            actor_id,
            kind: kind as i32,
            ..Default::default()
        }),
        agent_session_id: value
            .get("agent_session_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

pub(crate) fn metadata_entry(value: &Value) -> Option<v1::MetadataEntry> {
    Some(v1::MetadataEntry {
        key: value.get("key")?.as_str()?.to_owned(),
        value: value.get("value")?.as_str()?.to_owned(),
    })
}

pub(crate) fn context_ref(value: &Value) -> Option<v1::ContextRef> {
    Some(v1::ContextRef {
        kind: value.get("kind")?.as_i64()? as i32,
        value: value.get("value")?.as_str()?.to_owned(),
        uri: value
            .get("uri")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

pub(crate) fn issue_details(issue: &Issue) -> v1::IssueDetails {
    v1::IssueDetails {
        issue: Some(issue_proto(issue)),
        relations: issue.relations.iter().map(relation_proto).collect(),
        relation_page: Some(v1::PageInfo {
            snapshot: Some(v1::PageSnapshot {
                read_revision_watermark: issue.revision,
                snapshot_id: format!("issue-{}", issue.id),
                stable_traversal: Some(v1::StableTraversal {
                    order_fingerprint: "issue-id".into(),
                }),
                read_generation: issue.revision,
            }),
            ..Default::default()
        }),
    }
}
