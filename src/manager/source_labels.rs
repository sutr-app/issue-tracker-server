//! Canonical immutable provenance for labels captured from an extraction source.

use crate::{proto_types::manager_v1 as manager, v1};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const SOURCE_LABEL_SNAPSHOTS_KEY: &str = "lookback.source_label_snapshots_json";
/// Server-owned provenance keys which bind a canonical Issue back to the
/// Manager candidate that produced it and, when applicable, its extraction
/// run.  Callers may not provide or override these values.
pub(crate) const CANDIDATE_ID_KEY: &str = "candidate_id";
pub(crate) const EXTRACTION_RUN_ID_KEY: &str = "extraction_run_id";
pub(crate) const OWNERSHIP_NORMALIZED_FROM_KEY: &str = "ownership_normalized_from";
pub(crate) const OWNERSHIP_NORMALIZATION_REASON_KEY: &str = "ownership_normalization_reason";

pub(crate) fn is_server_owned_candidate_provenance_key(key: &str) -> bool {
    matches!(
        key,
        CANDIDATE_ID_KEY
            | EXTRACTION_RUN_ID_KEY
            | OWNERSHIP_NORMALIZED_FROM_KEY
            | OWNERSHIP_NORMALIZATION_REASON_KEY
    )
}

/// Append the immutable Candidate/ExtractionRun identity to a generated Issue
/// draft.  The values are always sourced from the persisted Candidate; a
/// caller-supplied metadata entry with one of the server-owned keys is
/// rejected instead of being overwritten.
pub(crate) fn append_candidate_provenance(
    issue_metadata: &mut Vec<v1::MetadataEntry>,
    candidate: &manager::Candidate,
) -> Result<(), String> {
    if issue_metadata
        .iter()
        .any(|entry| is_server_owned_candidate_provenance_key(&entry.key))
    {
        return Err("candidate provenance metadata is server-owned".into());
    }
    issue_metadata.push(v1::MetadataEntry {
        key: CANDIDATE_ID_KEY.into(),
        value: candidate.candidate_id.clone(),
    });
    if !candidate.extraction_run_id.trim().is_empty() {
        issue_metadata.push(v1::MetadataEntry {
            key: EXTRACTION_RUN_ID_KEY.into(),
            value: candidate.extraction_run_id.clone(),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceLabelSnapshot {
    pub thread_id: String,
    pub extraction_run_id: String,
    pub labels: Vec<String>,
}

pub(crate) fn canonical_labels(values: &[String]) -> Result<Vec<String>, String> {
    let mut seen = std::collections::HashSet::new();
    let mut labels = Vec::with_capacity(values.len());
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            return Err("source label must not be empty".into());
        }
        if value.len() > 512 {
            return Err("source label exceeds 512 UTF-8 bytes".into());
        }
        if seen.insert(value.to_owned()) {
            labels.push(value.to_owned());
        }
    }
    labels.sort_unstable();
    Ok(labels)
}

pub(crate) fn canonical_snapshots(
    snapshots: impl IntoIterator<Item = SourceLabelSnapshot>,
) -> Result<Vec<SourceLabelSnapshot>, String> {
    let mut by_identity = BTreeMap::new();
    for mut snapshot in snapshots {
        if snapshot.thread_id.is_empty() || snapshot.extraction_run_id.is_empty() {
            return Err("source label snapshot identity must not be empty".into());
        }
        snapshot.labels = canonical_labels(&snapshot.labels)?;
        let identity = (
            snapshot.thread_id.clone(),
            snapshot.extraction_run_id.clone(),
        );
        if let Some(existing) = by_identity.insert(identity, snapshot.clone()) {
            if existing.labels != snapshot.labels {
                return Err("source label provenance conflict".into());
            }
        }
    }
    Ok(by_identity.into_values().collect())
}

pub(crate) fn snapshot_json(
    snapshots: impl IntoIterator<Item = SourceLabelSnapshot>,
) -> Result<String, String> {
    serde_json::to_string(&canonical_snapshots(snapshots)?).map_err(|error| error.to_string())
}

pub(crate) fn union_snapshots(
    groups: impl IntoIterator<Item = Vec<SourceLabelSnapshot>>,
) -> Result<Vec<SourceLabelSnapshot>, String> {
    canonical_snapshots(groups.into_iter().flatten())
}

pub(crate) fn parse_snapshot_json(value: &str) -> Result<Vec<SourceLabelSnapshot>, String> {
    let snapshots: Vec<SourceLabelSnapshot> = serde_json::from_str(value)
        .map_err(|error| format!("source label snapshots must be strict JSON: {error}"))?;
    canonical_snapshots(snapshots)
}

pub(crate) fn snapshots_from_manager_metadata(
    metadata: &[manager::MetadataEntry],
) -> Result<Option<Vec<SourceLabelSnapshot>>, String> {
    let values = metadata
        .iter()
        .filter(|entry| entry.key == SOURCE_LABEL_SNAPSHOTS_KEY)
        .map(|entry| entry.value.as_str())
        .collect::<Vec<_>>();
    match values.as_slice() {
        [] => Ok(None),
        [value] => parse_snapshot_json(value).map(Some),
        _ => Err("source label snapshots metadata key must occur at most once".into()),
    }
}

pub(crate) fn snapshots_from_issue_metadata(
    metadata: &[v1::MetadataEntry],
) -> Result<Option<Vec<SourceLabelSnapshot>>, String> {
    let values = metadata
        .iter()
        .filter(|entry| entry.key == SOURCE_LABEL_SNAPSHOTS_KEY)
        .map(|entry| entry.value.as_str())
        .collect::<Vec<_>>();
    match values.as_slice() {
        [] => Ok(None),
        [value] => parse_snapshot_json(value).map(Some),
        _ => Err("source label snapshots metadata key must occur at most once".into()),
    }
}

pub(crate) fn replace_manager_metadata(
    metadata: &mut Vec<manager::MetadataEntry>,
    snapshots: &[SourceLabelSnapshot],
) -> Result<(), String> {
    metadata.retain(|entry| entry.key != SOURCE_LABEL_SNAPSHOTS_KEY);
    metadata.push(manager::MetadataEntry {
        key: SOURCE_LABEL_SNAPSHOTS_KEY.into(),
        value: snapshot_json(snapshots.iter().cloned())?,
    });
    Ok(())
}

pub(crate) fn replace_issue_metadata(
    metadata: &mut Vec<v1::MetadataEntry>,
    snapshots: &[SourceLabelSnapshot],
) -> Result<(), String> {
    metadata.retain(|entry| entry.key != SOURCE_LABEL_SNAPSHOTS_KEY);
    metadata.push(v1::MetadataEntry {
        key: SOURCE_LABEL_SNAPSHOTS_KEY.into(),
        value: snapshot_json(snapshots.iter().cloned())?,
    });
    Ok(())
}

pub(crate) fn has_source_label_metadata(metadata: &[v1::MetadataEntry]) -> bool {
    metadata
        .iter()
        .any(|entry| entry.key == SOURCE_LABEL_SNAPSHOTS_KEY)
}

pub(crate) fn copy_candidate_snapshots_to_issue(
    candidate_metadata: &[manager::MetadataEntry],
    issue_metadata: &mut Vec<v1::MetadataEntry>,
) -> Result<(), String> {
    if has_source_label_metadata(issue_metadata) {
        return Err("source label snapshots are server-owned for candidate issue creation".into());
    }
    if let Some(snapshots) = snapshots_from_manager_metadata(candidate_metadata)? {
        replace_issue_metadata(issue_metadata, &snapshots)?;
    }
    Ok(())
}

pub(crate) fn snapshot_for_extraction(
    thread_id: String,
    extraction_run_id: String,
    labels: &[String],
) -> Result<SourceLabelSnapshot, String> {
    Ok(SourceLabelSnapshot {
        thread_id,
        extraction_run_id,
        labels: canonical_labels(labels)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_trim_deduplicate_and_sort() {
        assert_eq!(
            canonical_labels(&[" team:api ".into(), "dir:x".into(), "team:api".into()]).unwrap(),
            ["dir:x", "team:api"]
        );
    }

    #[test]
    fn labels_reject_empty_and_byte_overflow() {
        assert!(canonical_labels(&[" \u{2003} ".into()]).is_err());
        assert!(canonical_labels(&["x".repeat(513)]).is_err());
    }

    #[test]
    fn snapshot_conflict_is_rejected() {
        let left = SourceLabelSnapshot {
            thread_id: "t".into(),
            extraction_run_id: "r".into(),
            labels: vec!["a".into()],
        };
        let right = SourceLabelSnapshot {
            labels: vec!["b".into()],
            ..left.clone()
        };
        assert!(canonical_snapshots([left, right]).is_err());
    }

    #[test]
    fn snapshot_json_rejects_unknown_fields_and_canonicalizes_order() {
        assert!(
            parse_snapshot_json(
                r#"[{"thread_id":"t","extraction_run_id":"r","labels":[],"extra":true}]"#
            )
            .is_err()
        );
        let encoded = snapshot_json([
            SourceLabelSnapshot {
                thread_id: "z".into(),
                extraction_run_id: "r".into(),
                labels: vec![],
            },
            SourceLabelSnapshot {
                thread_id: "a".into(),
                extraction_run_id: "r".into(),
                labels: vec!["b".into(), "a".into()],
            },
        ])
        .unwrap();
        assert_eq!(
            encoded,
            r#"[{"thread_id":"a","extraction_run_id":"r","labels":["a","b"]},{"thread_id":"z","extraction_run_id":"r","labels":[]}]"#
        );
    }

    #[test]
    fn replacing_snapshots_preserves_unrelated_manager_metadata() {
        let mut metadata = vec![manager::MetadataEntry {
            key: "thread_id".into(),
            value: "thread-1".into(),
        }];
        replace_manager_metadata(
            &mut metadata,
            &[SourceLabelSnapshot {
                thread_id: "thread-1".into(),
                extraction_run_id: "run-1".into(),
                labels: vec![],
            }],
        )
        .unwrap();
        assert_eq!(metadata[0].key, "thread_id");
        assert_eq!(metadata.len(), 2);
        assert!(
            snapshots_from_manager_metadata(&metadata)
                .unwrap()
                .is_some()
        );
    }
}
