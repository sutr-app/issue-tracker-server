//! Mapping between a manager Issue identity and the canonical backend Issue.
//!
//! The mapping intentionally contains identifiers, revision and contract
//! fingerprints only.  It is not a manager-side Issue snapshot; all Issue
//! attributes and state remain owned by the backend.

use crate::{proto_types::manager_v1, v1};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IssueMappingRecord {
    pub issue_id: String,
    pub candidate_id: Option<String>,
    pub backend_id: String,
    pub revision: u64,
    pub backend_contract_fingerprint: String,
    pub manager_contract_fingerprint: String,
    pub generation: u64,
    pub created_at_millis: i64,
    pub updated_at_millis: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MappingError {
    MissingIssueId,
    MissingBackendId,
    EmptyCandidateId,
    MissingBackendContractFingerprint,
    MissingManagerContractFingerprint,
    RevisionRegression,
    IdentityMismatch,
    FingerprintMismatch,
}

impl std::fmt::Display for MappingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MissingIssueId => "backend issue ID is required",
            Self::MissingBackendId => "backend ID is required",
            Self::EmptyCandidateId => "candidate ID must not be empty",
            Self::MissingBackendContractFingerprint => "backend contract fingerprint is required",
            Self::MissingManagerContractFingerprint => "manager contract fingerprint is required",
            Self::RevisionRegression => "backend issue revision regressed",
            Self::IdentityMismatch => "backend issue identity does not match mapping",
            Self::FingerprintMismatch => "backend contract fingerprint does not match mapping",
        })
    }
}

impl std::error::Error for MappingError {}

#[allow(dead_code)]
impl IssueMappingRecord {
    /// Creates a mapping from the backend's current Issue read.
    pub(crate) fn from_backend_issue(
        issue: &v1::Issue,
        candidate_id: Option<String>,
        backend_contract_fingerprint: impl Into<String>,
        manager_contract_fingerprint: impl Into<String>,
        generation: u64,
        now_millis: i64,
    ) -> Result<Self, MappingError> {
        let backend_contract_fingerprint = backend_contract_fingerprint.into();
        let manager_contract_fingerprint = manager_contract_fingerprint.into();
        validate_issue_identity(issue, &backend_contract_fingerprint)?;
        if let Some(candidate_id) = candidate_id.as_deref()
            && candidate_id.trim().is_empty()
        {
            return Err(MappingError::EmptyCandidateId);
        }
        if manager_contract_fingerprint.trim().is_empty() {
            return Err(MappingError::MissingManagerContractFingerprint);
        }
        Ok(Self {
            issue_id: issue.issue_id.clone(),
            candidate_id,
            backend_id: issue.backend_id.clone(),
            revision: issue.revision,
            backend_contract_fingerprint,
            manager_contract_fingerprint,
            generation,
            created_at_millis: now_millis,
            updated_at_millis: now_millis,
        })
    }

    /// Refreshes only the mapping facts after a confirmed backend mutation.
    pub(crate) fn refresh_from_backend_issue(
        &self,
        issue: &v1::Issue,
        backend_contract_fingerprint: &str,
        generation: u64,
        now_millis: i64,
    ) -> Result<Self, MappingError> {
        validate_issue_identity(issue, backend_contract_fingerprint)?;
        if issue.issue_id != self.issue_id || issue.backend_id != self.backend_id {
            return Err(MappingError::IdentityMismatch);
        }
        if backend_contract_fingerprint != self.backend_contract_fingerprint {
            return Err(MappingError::FingerprintMismatch);
        }
        if issue.revision < self.revision {
            return Err(MappingError::RevisionRegression);
        }
        let mut refreshed = self.clone();
        refreshed.revision = issue.revision;
        refreshed.generation = generation;
        refreshed.updated_at_millis = now_millis;
        Ok(refreshed)
    }

    /// Confirms that a backend read still describes this mapping.
    pub(crate) fn matches_backend_issue(
        &self,
        issue: &v1::Issue,
        backend_contract_fingerprint: &str,
    ) -> bool {
        issue.issue_id == self.issue_id
            && issue.backend_id == self.backend_id
            && issue.revision == self.revision
            && backend_contract_fingerprint == self.backend_contract_fingerprint
    }

    pub(crate) fn to_proto(&self) -> manager_v1::IssueMapping {
        manager_v1::IssueMapping {
            issue_id: self.issue_id.clone(),
            candidate_id: self.candidate_id.clone(),
            backend_id: self.backend_id.clone(),
            revision: self.revision,
            backend_contract_fingerprint: self.backend_contract_fingerprint.clone(),
            manager_contract_fingerprint: self.manager_contract_fingerprint.clone(),
            generation: self.generation,
            created_at: Some(manager_v1::UtcInstant {
                unix_epoch_millis: self.created_at_millis,
            }),
            updated_at: Some(manager_v1::UtcInstant {
                unix_epoch_millis: self.updated_at_millis,
            }),
        }
    }

    pub(crate) fn from_proto(mapping: &manager_v1::IssueMapping) -> Result<Self, MappingError> {
        if mapping.issue_id.trim().is_empty() {
            return Err(MappingError::MissingIssueId);
        }
        if mapping.backend_id.trim().is_empty() {
            return Err(MappingError::MissingBackendId);
        }
        if mapping.backend_contract_fingerprint.trim().is_empty() {
            return Err(MappingError::MissingBackendContractFingerprint);
        }
        if mapping.manager_contract_fingerprint.trim().is_empty() {
            return Err(MappingError::MissingManagerContractFingerprint);
        }
        if mapping.candidate_id.as_deref().is_some_and(str::is_empty) {
            return Err(MappingError::EmptyCandidateId);
        }
        Ok(Self {
            issue_id: mapping.issue_id.clone(),
            candidate_id: mapping.candidate_id.clone(),
            backend_id: mapping.backend_id.clone(),
            revision: mapping.revision,
            backend_contract_fingerprint: mapping.backend_contract_fingerprint.clone(),
            manager_contract_fingerprint: mapping.manager_contract_fingerprint.clone(),
            generation: mapping.generation,
            created_at_millis: mapping
                .created_at
                .as_ref()
                .map_or(0, |instant| instant.unix_epoch_millis),
            updated_at_millis: mapping
                .updated_at
                .as_ref()
                .map_or(0, |instant| instant.unix_epoch_millis),
        })
    }
}

fn validate_issue_identity(
    issue: &v1::Issue,
    backend_contract_fingerprint: &str,
) -> Result<(), MappingError> {
    if issue.issue_id.trim().is_empty() {
        return Err(MappingError::MissingIssueId);
    }
    if issue.backend_id.trim().is_empty() {
        return Err(MappingError::MissingBackendId);
    }
    if backend_contract_fingerprint.trim().is_empty() {
        return Err(MappingError::MissingBackendContractFingerprint);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue() -> v1::Issue {
        v1::Issue {
            issue_id: "issue-1".into(),
            backend_id: "task-1".into(),
            revision: 3,
            title: "canonical title".into(),
            description: "canonical description".into(),
            ..Default::default()
        }
    }

    #[test]
    fn mapping_round_trips_identity_without_issue_snapshot() {
        let mapping = IssueMappingRecord::from_backend_issue(
            &issue(),
            Some("candidate-1".into()),
            "backend-v1",
            "manager-v1",
            7,
            100,
        )
        .expect("mapping");
        let proto = mapping.to_proto();
        let restored = IssueMappingRecord::from_proto(&proto).expect("mapping proto");
        assert_eq!(restored, mapping);
        assert_eq!(proto.issue_id, "issue-1");
        assert_eq!(proto.backend_id, "task-1");
        assert_eq!(proto.revision, 3);
    }

    #[test]
    fn proto_mapping_accepts_taskchampion_creation_revision_zero() {
        let mapping = manager_v1::IssueMapping {
            issue_id: "issue-1".into(),
            backend_id: "task-1".into(),
            revision: 0,
            backend_contract_fingerprint: "backend-v1".into(),
            manager_contract_fingerprint: "manager-v1".into(),
            ..Default::default()
        };

        let restored = IssueMappingRecord::from_proto(&mapping).expect("mapping proto");

        assert_eq!(restored.revision, 0);
    }

    #[test]
    fn backend_issue_mapping_accepts_taskchampion_creation_revision_zero() {
        let mut creation = issue();
        creation.revision = 0;

        let mapping = IssueMappingRecord::from_backend_issue(
            &creation,
            None,
            "backend-v1",
            "manager-v1",
            1,
            100,
        )
        .expect("creation mapping");

        assert_eq!(mapping.revision, 0);
    }

    #[test]
    fn refresh_rejects_revision_regression_and_identity_change() {
        let mapping = IssueMappingRecord::from_backend_issue(
            &issue(),
            None,
            "backend-v1",
            "manager-v1",
            1,
            100,
        )
        .expect("mapping");
        let mut older = issue();
        older.revision = 2;
        assert_eq!(
            mapping.refresh_from_backend_issue(&older, "backend-v1", 2, 200),
            Err(MappingError::RevisionRegression)
        );
        let mut changed = issue();
        changed.backend_id = "task-2".into();
        assert_eq!(
            mapping.refresh_from_backend_issue(&changed, "backend-v1", 2, 200),
            Err(MappingError::IdentityMismatch)
        );
    }

    #[test]
    fn mapping_does_not_confirm_a_different_backend_read() {
        let mapping = IssueMappingRecord::from_backend_issue(
            &issue(),
            None,
            "backend-v1",
            "manager-v1",
            1,
            100,
        )
        .expect("mapping");
        let mut changed = issue();
        changed.title = "changed canonical data".into();
        assert!(mapping.matches_backend_issue(&changed, "backend-v1"));
        changed.revision = 4;
        assert!(!mapping.matches_backend_issue(&changed, "backend-v1"));
    }
}
