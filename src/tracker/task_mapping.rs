//! TaskChampion attribute encoding for issue metadata and relations.

use super::{
    ISSUE_METADATA_KEY, IssueMetadata, RELATION_METADATA_PREFIX, RelationKind, TrackerError,
    actor::ActorIdentity,
};
use crate::tracker::artifacts::set;
use serde_json::Value;
use taskchampion::Operations;
use uuid::Uuid;

pub(crate) fn metadata_from_task(task: &taskchampion::Task) -> Result<IssueMetadata, TrackerError> {
    task.get_user_defined_attribute(ISSUE_METADATA_KEY)
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| TrackerError::InvalidState(format!("invalid issue metadata: {error}")))
        .map(|metadata| metadata.unwrap_or_default())
}
pub(crate) fn set_metadata(
    task: &mut taskchampion::Task,
    metadata: &IssueMetadata,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    let value = serde_json::to_string(metadata).map_err(|error| {
        TrackerError::InvalidState(format!("issue metadata encode failed: {error}"))
    })?;
    set(task, ISSUE_METADATA_KEY, &value, ops)
}
pub(crate) fn relation_id(kind: RelationKind, source: Uuid, target: Uuid) -> String {
    format!("{}:{}:{}", kind.key(), source, target)
}
pub(crate) fn relation_metadata_key(id: &str) -> String {
    format!("{RELATION_METADATA_PREFIX}{id}")
}
pub(crate) fn relation_metadata(task: &taskchampion::Task, id: &str) -> (u64, Option<Value>) {
    task.get_user_defined_attribute(&relation_metadata_key(id))
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .map(|value| {
            (
                value
                    .get("created_at")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                value.get("created_by").cloned(),
            )
        })
        .unwrap_or((0, None))
}
pub(crate) fn actor_value_with_session(
    actor: &ActorIdentity,
    session_id: Option<&str>,
    correlation_id: &str,
) -> Option<Value> {
    // `unknown` is an audit value, never an authorization bypass. Keeping it
    // explicit makes legacy/internal writes distinguishable from a missing
    // provenance record and prevents downstream consumers from inferring a
    // trusted actor from absent metadata.
    Some(serde_json::json!({
        "actor_id": actor.actor_id(),
        "actor_kind": actor.kind() as i32,
        "agent_session_id": session_id,
        "correlation_id": correlation_id,
    }))
}
pub(crate) fn clear(
    task: &mut taskchampion::Task,
    key: &str,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    task.remove_user_defined_attribute(key, ops)
        .map_err(TrackerError::from)
}
pub(crate) fn relation_values(
    task: &taskchampion::Task,
    key: &str,
) -> Result<Vec<Uuid>, TrackerError> {
    task.get_user_defined_attribute(key)
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| TrackerError::InvalidRelation(error.to_string()))
        .map(|values| values.unwrap_or_default())
}
pub(crate) fn write_relation(
    task: &mut taskchampion::Task,
    key: &str,
    values: &[Uuid],
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    if values.is_empty() {
        clear(task, key, ops)
    } else {
        let value = serde_json::to_string(values)
            .map_err(|error| TrackerError::InvalidRelation(error.to_string()))?;
        set(task, key, &value, ops)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn relation_id_is_directional() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        assert_ne!(
            relation_id(RelationKind::Related, a, b),
            relation_id(RelationKind::Related, b, a)
        );
    }

    #[test]
    fn unknown_actor_is_preserved_as_explicit_audit_metadata() {
        let audit = actor_value_with_session(&ActorIdentity::from("unknown"), None, "")
            .expect("unknown actor must not erase audit provenance");
        assert_eq!(audit["actor_id"], "unknown");
        assert_eq!(audit["correlation_id"], "");
        assert!(audit["agent_session_id"].is_null());
    }
}
