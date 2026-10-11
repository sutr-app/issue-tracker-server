//! Public issue-tracking domain model.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use taskchampion::Status;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IssueState {
    Open,
    InProgress,
    Blocked,
    Deferred,
    Done,
    Canceled,
    AwaitingReview,
}

impl IssueState {
    pub(crate) fn parse(value: &str) -> Result<Self, crate::error::TrackerError> {
        match value {
            "open" => Ok(Self::Open),
            "in_progress" => Ok(Self::InProgress),
            "blocked" => Ok(Self::Blocked),
            "deferred" => Ok(Self::Deferred),
            "done" => Ok(Self::Done),
            "canceled" => Ok(Self::Canceled),
            "awaiting_review" => Ok(Self::AwaitingReview),
            "candidate" => Err(crate::error::TrackerError::InvalidState(
                "candidate is not canonical".into(),
            )),
            other => Err(crate::error::TrackerError::InvalidState(format!(
                "unknown state: {other}"
            ))),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::InProgress => "in_progress",
            Self::Blocked => "blocked",
            Self::Deferred => "deferred",
            Self::Done => "done",
            Self::Canceled => "canceled",
            Self::AwaitingReview => "awaiting_review",
        }
    }

    pub(crate) fn native_status(self) -> Status {
        match self {
            Self::Done | Self::Canceled => Status::Completed,
            Self::Open
            | Self::InProgress
            | Self::Blocked
            | Self::Deferred
            | Self::AwaitingReview => Status::Pending,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RelationKind {
    ParentChild,
    Blocks,
    Related,
    DiscoveredFrom,
    Supersedes,
}

impl RelationKind {
    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::ParentChild => "parent-child",
            Self::Blocks => "blocks",
            Self::Related => "related",
            Self::DiscoveredFrom => "discovered-from",
            Self::Supersedes => "supersedes",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IssueRelation {
    pub relation_id: String,
    pub kind: RelationKind,
    pub source: Uuid,
    pub target: Uuid,
    pub created_at: u64,
    pub created_by: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Issue {
    pub id: Uuid,
    pub title: String,
    pub description: String,
    pub kind: i32,
    pub priority: i32,
    pub labels: Vec<String>,
    pub acceptance_criteria: Vec<String>,
    pub contexts: Vec<Value>,
    pub due_at: Option<i64>,
    pub review_at: Option<i64>,
    pub state: IssueState,
    pub revision: u64,
    pub relations: Vec<IssueRelation>,
    pub created_by: Option<Value>,
    pub created_at: u64,
    pub updated_at: u64,
    pub completed_at: Option<u64>,
    pub creation_basis: i32,
    pub confirmation_status: i32,
    pub provenance_metadata: Vec<Value>,
    pub source_evidence: Vec<Value>,
    pub lease_owner: Option<Value>,
    pub lease_expires_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_handle: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct IssueMetadata {
    #[serde(default)]
    pub(crate) description: String,
    #[serde(default)]
    pub(crate) kind: i32,
    #[serde(default)]
    pub(crate) priority: i32,
    #[serde(default)]
    pub(crate) labels: Vec<String>,
    #[serde(default)]
    pub(crate) acceptance_criteria: Vec<String>,
    #[serde(default)]
    pub(crate) contexts: Vec<Value>,
    #[serde(default)]
    pub(crate) due_at: Option<i64>,
    #[serde(default)]
    pub(crate) review_at: Option<i64>,
    #[serde(default)]
    pub(crate) created_by: Option<Value>,
    #[serde(default)]
    pub(crate) created_at: u64,
    #[serde(default)]
    pub(crate) updated_at: u64,
    #[serde(default)]
    pub(crate) completed_at: Option<u64>,
    #[serde(default)]
    pub(crate) creation_basis: i32,
    #[serde(default)]
    pub(crate) confirmation_status: i32,
    #[serde(default)]
    pub(crate) provenance_metadata: Vec<Value>,
    #[serde(default)]
    pub(crate) source_evidence: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportBundle {
    pub contract: String,
    pub schema_fingerprint: String,
    pub issues: Vec<Issue>,
    pub history: Vec<Value>,
    pub uri: Option<String>,
    pub provenance_fingerprint: Option<String>,
}
