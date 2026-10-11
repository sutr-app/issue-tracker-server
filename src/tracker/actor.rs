use crate::{TrackerError, v1};
use serde_json::Value;
use std::ops::Deref;

/// Audit identity used by Tracker writes. Actor kinds are descriptive audit
/// data only; they do not establish authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorIdentity {
    actor_id: String,
    kind: v1::ActorKind,
    agent_session_id: Option<String>,
    correlation_id: Option<String>,
}

impl ActorIdentity {
    pub fn new(actor_id: impl Into<String>, kind: v1::ActorKind) -> Self {
        Self {
            actor_id: actor_id.into(),
            kind,
            agent_session_id: None,
            correlation_id: None,
        }
    }

    /// Adds optional request provenance without changing the actor identity.
    pub fn with_audit_context(
        mut self,
        agent_session_id: Option<String>,
        correlation_id: Option<String>,
    ) -> Self {
        self.agent_session_id = agent_session_id;
        self.correlation_id = correlation_id;
        self
    }

    /// Placeholder for writes whose context carries no caller identity.
    pub fn unknown() -> Self {
        Self::new("unknown", v1::ActorKind::Unspecified)
    }

    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    pub fn kind(&self) -> v1::ActorKind {
        self.kind
    }

    pub fn agent_session_id(&self) -> Option<&str> {
        self.agent_session_id.as_deref()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }
}

impl Default for ActorIdentity {
    fn default() -> Self {
        Self::new(String::new(), v1::ActorKind::Unspecified)
    }
}

impl Deref for ActorIdentity {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.actor_id()
    }
}

impl PartialEq<&str> for ActorIdentity {
    fn eq(&self, other: &&str) -> bool {
        self.actor_id == *other
    }
}

impl From<&str> for ActorIdentity {
    fn from(actor_id: &str) -> Self {
        Self::new(actor_id, v1::ActorKind::InternalAgent)
    }
}

impl From<String> for ActorIdentity {
    fn from(actor_id: String) -> Self {
        Self::new(actor_id, v1::ActorKind::InternalAgent)
    }
}

impl From<&String> for ActorIdentity {
    fn from(actor_id: &String) -> Self {
        Self::from(actor_id.as_str())
    }
}

impl From<&ActorIdentity> for ActorIdentity {
    fn from(actor: &ActorIdentity) -> Self {
        actor.clone()
    }
}

/// Hydrates the optional numeric `actor_kind` field in TaskChampion JSON.
pub(crate) fn persisted_kind(record: &Value) -> Result<v1::ActorKind, TrackerError> {
    persisted_kind_value(record.get("actor_kind"))
}

/// Missing kinds predate typed actor persistence and retain the old
/// InternalAgent interpretation; malformed or unknown values never become
/// privileged kinds.
pub(crate) fn persisted_kind_value(value: Option<&Value>) -> Result<v1::ActorKind, TrackerError> {
    let Some(value) = value else {
        return Ok(v1::ActorKind::InternalAgent);
    };
    let numeric = value
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| TrackerError::InvalidState("invalid persisted actor kind".into()))?;
    v1::ActorKind::try_from(numeric)
        .map_err(|_| TrackerError::InvalidState("invalid persisted actor kind".into()))
}
