//! Idempotency markers, operation history, and request digests.

use super::{
    CONTRACT_VERSION, ID_KEY, IssueState, LEASE_EXPIRES_KEY, STATE_KEY, Tracker, TrackerError,
    actor::ActorIdentity,
    conversion::{instant, issue_state},
    now, revision,
    task_mapping::metadata_from_task,
};
use crate::manager::operations::digest_bytes;
use crate::proto_types::manager_v1;
use crate::tracker::artifacts::set;
use crate::v1;
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use taskchampion::Operations;
use uuid::Uuid;

const ADMIN_RECEIPT_PREFIX: &str = "lb.admin_receipt.";
pub(crate) const OPERATION_FENCE_PREFIX: &str = "lb.operation_fence.";

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).ok())
        .collect()
}

/// Attach a typed Manager receipt to the TaskChampion marker input.  The
/// attribute is only an intermediate write; `marker_result_snapshot` copies
/// its bytes into the immutable protobuf marker before the operation commits.
pub(crate) fn set_admin_receipt_payload(
    task: &mut taskchampion::Task,
    operation_id: &str,
    receipt: &manager_v1::IssueAdminActionResult,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    let payload = receipt.encode_to_vec();
    set(
        task,
        &format!("{ADMIN_RECEIPT_PREFIX}{operation_id}"),
        &encode_hex(&payload),
        ops,
    )
}

pub(crate) fn set_marker(
    task: &mut taskchampion::Task,
    operation_id: &str,
    digest: &str,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    set_marker_with_replayability(task, operation_id, digest, true, ops)
}
pub(crate) fn set_marker_non_replayable(
    task: &mut taskchampion::Task,
    operation_id: &str,
    digest: &str,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    set_marker_with_replayability(task, operation_id, digest, false, ops)
}
fn set_marker_with_replayability(
    task: &mut taskchampion::Task,
    operation_id: &str,
    digest: &str,
    replayable: bool,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    // Non-replayable operations (notably Claim) still retain a redacted,
    // method-neutral snapshot for audit and crash reconciliation.  The
    // replayability bit remains false, so callers must not use this snapshot
    // to reconstruct one-time secrets such as the lease handle.
    let result = marker_result_snapshot(task, operation_id)?;
    let value=serde_json::json!({"status":"committed","payload_digest":digest,"contract":CONTRACT_VERSION,"schema_fingerprint":Tracker::schema_fingerprint(),"revision":revision(task)?,"result_replayability":if replayable{"replayable"}else{"not_replayable"},"result":result}).to_string();
    set(task, &format!("lb.operation.{operation_id}"), &value, ops)
}

/// Store a method-neutral Issue mutation snapshot in the marker itself.  It
/// is produced before the enclosing TaskChampion operation commits, so later
/// writes cannot replace an idempotent result with the Issue's current value.
fn marker_result_snapshot(
    task: &taskchampion::Task,
    operation_id: &str,
) -> Result<Value, TrackerError> {
    let issue_id = task
        .get_user_defined_attribute(ID_KEY)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| TrackerError::InvalidState("marker issue ID is invalid".into()))?;
    let state = IssueState::parse(task.get_user_defined_attribute(STATE_KEY).unwrap_or("open"))?;
    let metadata = metadata_from_task(task)?;
    let revision = revision(task)?;
    let previous_state = task
        .get_user_defined_attribute(&format!("lb.history.{operation_id}"))
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .and_then(|history| {
            history
                .get("old_state")
                .and_then(Value::as_str)
                .and_then(|value| IssueState::parse(value).ok())
        })
        .map(issue_state);
    let repaired_ownership_attributes = task
        .get_user_defined_attribute(&format!("lb.history.{operation_id}"))
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .map(|history| {
            if !matches!(
                history.get("kind").and_then(Value::as_str),
                Some("apply_issue_admin_action" | "lease_recovered" | "lease_revoked")
            ) {
                return Vec::new();
            }
            history
                .get("ownership_cleared_attributes")
                .and_then(Value::as_array)
                .map(|attributes| {
                    attributes
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();
    let issue = v1::Issue {
        issue_id: issue_id.to_string(),
        backend_id: issue_id.to_string(),
        title: task.get_description().to_owned(),
        description: metadata.description,
        kind: metadata.kind,
        priority: metadata.priority,
        state: issue_state(state),
        labels: metadata.labels,
        acceptance_criteria: metadata.acceptance_criteria,
        contexts: metadata
            .contexts
            .iter()
            .filter_map(crate::tracker::conversion::context_ref)
            .collect(),
        due_at: metadata.due_at.map(|value| v1::UtcInstant {
            unix_epoch_millis: value,
        }),
        review_at: metadata.review_at.map(|value| v1::UtcInstant {
            unix_epoch_millis: value,
        }),
        revision,
        created_at: Some(instant(metadata.created_at)),
        updated_at: Some(instant(metadata.updated_at)),
        completed_at: metadata.completed_at.map(instant),
        creation_basis: metadata.creation_basis,
        confirmation_status: metadata.confirmation_status,
        provenance_metadata: metadata
            .provenance_metadata
            .iter()
            .filter_map(crate::tracker::conversion::metadata_entry)
            .collect(),
        source_evidence: metadata
            .source_evidence
            .iter()
            .filter_map(|value| serde_json::from_value(value.clone()).ok())
            .collect(),
        claim: task
            .get_user_defined_attribute(LEASE_EXPIRES_KEY)
            .and_then(|value| value.parse::<u64>().ok())
            .map(|expires| v1::ClaimView {
                owner: None,
                lease_expires_at: Some(instant(expires)),
                claim_revision: revision,
            }),
        ..Default::default()
    };
    let mut snapshot = v1::OperationResultSnapshot {
        value: Some(v1::operation_result_snapshot::Value::IssueMutation(
            v1::IssueMutationSnapshot {
                current: Some(v1::IssueDetails {
                    issue: Some(issue),
                    ..Default::default()
                }),
                previous_state,
                changed_fields: repaired_ownership_attributes,
            },
        )),
        ..Default::default()
    };
    if let Some(value) =
        task.get_user_defined_attribute(&format!("{ADMIN_RECEIPT_PREFIX}{operation_id}"))
        && let Some(payload) = decode_hex(value)
    {
        snapshot.manager_result_schema = "issue_tracker.manager.v1.IssueAdminActionResult".into();
        snapshot.manager_result_digest = digest_bytes(&payload);
        snapshot.manager_result_payload = payload;
    }
    serde_json::to_value(snapshot).map_err(|error| {
        TrackerError::InvalidState(format!("marker snapshot encode failed: {error}"))
    })
}
pub(crate) fn set_history(
    task: &mut taskchampion::Task,
    operation_id: &str,
    kind: &str,
    revision: u64,
    digest: &str,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    set_history_with_context(
        task,
        operation_id,
        kind,
        revision,
        digest,
        &ActorIdentity::unknown(),
        operation_id,
        None,
        None,
        None,
        ops,
    )
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn set_history_with_context(
    task: &mut taskchampion::Task,
    operation_id: &str,
    kind: &str,
    revision: u64,
    digest: &str,
    actor: &ActorIdentity,
    correlation_id: &str,
    old_state: Option<&str>,
    new_state: Option<&str>,
    reason: Option<&str>,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    let correlation_id = actor.correlation_id().unwrap_or(correlation_id);
    let mut record = serde_json::json!({"operation_id":operation_id,"kind":kind,"revision":revision,"timestamp":now(),"payload_digest":digest,"contract":CONTRACT_VERSION,"actor":actor.actor_id(),"actor_kind":actor.kind() as i32,"correlation_id":correlation_id,"old_state":old_state,"new_state":new_state,"reason":reason});
    if let Some(session_id) = actor.agent_session_id() {
        record["agent_session_id"] = Value::String(session_id.to_owned());
    }
    set(
        task,
        &format!("lb.history.{operation_id}"),
        &record.to_string(),
        ops,
    )
}
pub(crate) fn set_history_subject_fields(
    task: &mut taskchampion::Task,
    operation_id: &str,
    fields: &[(&str, Value)],
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    let key = format!("lb.history.{operation_id}");
    let mut record: Value = task
        .get_user_defined_attribute(&key)
        .and_then(|value| serde_json::from_str(value).ok())
        .ok_or_else(|| TrackerError::InvalidState("history record is missing".into()))?;
    if let Some(object) = record.as_object_mut() {
        for (name, value) in fields {
            object.insert((*name).to_owned(), value.clone());
        }
    }
    set(task, &key, &record.to_string(), ops)
}
pub(crate) fn set_history_session(
    task: &mut taskchampion::Task,
    operation_id: &str,
    session_id: Option<&str>,
    ops: &mut Operations,
) -> Result<(), TrackerError> {
    set_history_subject_fields(
        task,
        operation_id,
        &[(
            "agent_session_id",
            session_id
                .map(|value| Value::String(value.to_owned()))
                .unwrap_or(Value::Null),
        )],
        ops,
    )
}
pub(crate) fn marker_digest(parts: &[&str]) -> String {
    format!("{:x}", Sha256::digest(parts.join("\u{1f}").as_bytes()))
}
pub(crate) fn operation_replayed(
    task: &taskchampion::Task,
    operation_id: &str,
    digest: &str,
) -> Result<bool, TrackerError> {
    if let Some(fence) =
        task.get_user_defined_attribute(&format!("{OPERATION_FENCE_PREFIX}{operation_id}"))
    {
        let parsed: Value =
            serde_json::from_str(fence).map_err(|_| TrackerError::OperationFenced)?;
        if parsed.get("status").and_then(Value::as_str) == Some("fenced") {
            return Err(TrackerError::OperationFenced);
        }
    }
    let Some(value) = task.get_user_defined_attribute(&format!("lb.operation.{operation_id}"))
    else {
        return Ok(false);
    };
    let parsed: Value =
        serde_json::from_str(value).map_err(|_| TrackerError::IdempotencyConflict)?;
    if parsed.get("status").and_then(Value::as_str) == Some("committed")
        && parsed.get("contract").and_then(Value::as_str) == Some(CONTRACT_VERSION)
        && parsed.get("payload_digest").and_then(Value::as_str) == Some(digest)
    {
        Ok(true)
    } else {
        Err(TrackerError::IdempotencyConflict)
    }
}
#[cfg(test)]
mod tests {
    use super::marker_digest;
    #[test]
    fn digest_preserves_part_boundaries() {
        assert_ne!(marker_digest(&["ab", "c"]), marker_digest(&["a", "bc"]));
    }
}
