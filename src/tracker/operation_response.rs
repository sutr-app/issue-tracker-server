use super::{
    config::{CallContext, contract_fingerprint},
    conversion::{instant, issue_details},
    operation_log::marker_digest,
    *,
};

pub(crate) fn operation(
    method: &str,
    context: &CallContext,
    issue: Option<&Issue>,
) -> v1::OperationRecord {
    let (affected_issue_ids, resulting_revision, entity_revisions) = issue
        .map(|issue| {
            (
                vec![issue.id.to_string()],
                Some(issue.revision),
                vec![v1::OperationEntityRevision {
                    issue_id: issue.id.to_string(),
                    backend_id: issue.id.to_string(),
                    resulting_revision: issue.revision,
                }],
            )
        })
        .unwrap_or_default();
    let scope = if issue.is_some() {
        v1::OperationScope::Issue
    } else {
        v1::OperationScope::Tracker
    };
    let payload_fingerprint = marker_digest(&[method, &context.operation_id]);
    let result_snapshot = issue.map(|issue| v1::OperationResultSnapshot {
        value: Some(v1::operation_result_snapshot::Value::IssueMutation(
            v1::IssueMutationSnapshot {
                current: Some(issue_details(issue)),
                previous_state: None,
                changed_fields: Vec::new(),
            },
        )),
        ..Default::default()
    });
    v1::OperationRecord {
        downstream_operation_id: context.operation_id.clone(),
        affected_issue_ids,
        resulting_revision,
        payload_fingerprint: payload_fingerprint.clone(),
        contract_fingerprint: contract_fingerprint(),
        committed_at: Some(instant(now())),
        scope: scope as i32,
        method_key: method.into(),
        entity_revisions,
        commit_durability: v1::OperationCommitDurability::Durable as i32,
        result_replayability: v1::OperationResultReplayability::Replayable as i32,
        result_snapshot,
        fingerprint_input: Some(v1::OperationFingerprintInput {
            downstream_operation_id: context.operation_id.clone(),
            method_key: method.into(),
            scope: scope as i32,
            payload_fingerprint,
        }),
        ..Default::default()
    }
}

pub(crate) fn non_replayable_operation(mut record: v1::OperationRecord) -> v1::OperationRecord {
    record.result_replayability = v1::OperationResultReplayability::NotReplayable as i32;
    record.result_snapshot = None;
    record
}
