//! Shared metadata for Manager methods.
//!
//! The wire method names are part of the internal durable-operation contract.
//! Keeping their result schema and lifecycle category in one table prevents
//! replay and capability code from drifting when a method is added.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MethodCategory {
    Mutation,
    OwnershipCapability,
    Administrative,
    Resolution,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MethodSpec {
    pub(crate) result_schema: &'static str,
    pub(crate) category: MethodCategory,
    pub(crate) supports_capability_dispatch: bool,
}

/// Complete catalog of method names whose durable mutation payloads can be
/// replayed.  Keeping the list next to [`spec`] gives tests and new call sites
/// one place to enumerate the internal wire contract.
#[cfg(test)]
pub(crate) const KNOWN_METHODS: &[&str] = &[
    "manager_create_issue",
    "manager_update_issue",
    "manager_transition_issue",
    "manager_claim_issue",
    "manager_heartbeat_issue_claim",
    "manager_release_issue_claim",
    "manager_reconcile_expired_claim",
    "manager_add_issue_relation",
    "manager_remove_issue_relation",
    "manager_add_issue_comment",
    "manager_apply_issue_admin_action",
    "manager_resolve_issue_operation",
];

pub(crate) fn spec(method: &str) -> Option<MethodSpec> {
    let (result_schema, category, supports_capability_dispatch) = match method {
        "manager_create_issue" => (
            "issue_tracker.manager.v1.CreateIssueResult",
            MethodCategory::Mutation,
            false,
        ),
        "manager_update_issue" => (
            "issue_tracker.manager.v1.UpdateIssueResult",
            MethodCategory::Mutation,
            false,
        ),
        "manager_transition_issue" => (
            "issue_tracker.manager.v1.TransitionIssueResult",
            MethodCategory::OwnershipCapability,
            true,
        ),
        "manager_claim_issue" => (
            "issue_tracker.manager.v1.ClaimIssueResult",
            MethodCategory::OwnershipCapability,
            true,
        ),
        "manager_heartbeat_issue_claim" => (
            "issue_tracker.manager.v1.HeartbeatIssueClaimResult",
            MethodCategory::OwnershipCapability,
            true,
        ),
        "manager_release_issue_claim" => (
            "issue_tracker.manager.v1.ReleaseIssueClaimResult",
            MethodCategory::OwnershipCapability,
            true,
        ),
        "manager_reconcile_expired_claim" => (
            "issue_tracker.manager.v1.ReconcileExpiredClaimResult",
            MethodCategory::OwnershipCapability,
            false,
        ),
        "manager_add_issue_relation" => (
            "issue_tracker.manager.v1.AddIssueRelationResult",
            MethodCategory::Mutation,
            false,
        ),
        "manager_remove_issue_relation" => (
            "issue_tracker.manager.v1.RemoveIssueRelationResult",
            MethodCategory::Mutation,
            false,
        ),
        "manager_add_issue_comment" => (
            "issue_tracker.manager.v1.AddIssueCommentResult",
            MethodCategory::Mutation,
            false,
        ),
        "manager_apply_issue_admin_action" => (
            "issue_tracker.manager.v1.ApplyIssueAdminActionResult",
            MethodCategory::Administrative,
            false,
        ),
        "manager_resolve_issue_operation" => (
            "issue_tracker.manager.v1.ResolveIssueOperationResult",
            MethodCategory::Resolution,
            false,
        ),
        _ => return None,
    };
    Some(MethodSpec {
        result_schema,
        category,
        supports_capability_dispatch,
    })
}

pub(crate) fn is_ownership(method: &str) -> bool {
    spec(method).is_some_and(|value| value.category == MethodCategory::OwnershipCapability)
}

pub(crate) fn is_capability(method: &str) -> bool {
    spec(method).is_some_and(|value| value.supports_capability_dispatch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_methods_have_stable_schema_and_category() {
        assert_eq!(
            spec("manager_claim_issue"),
            Some(MethodSpec {
                result_schema: "issue_tracker.manager.v1.ClaimIssueResult",
                category: MethodCategory::OwnershipCapability,
                supports_capability_dispatch: true,
            })
        );
        assert_eq!(
            spec("manager_apply_issue_admin_action").map(|value| value.category),
            Some(MethodCategory::Administrative)
        );
        assert!(spec("manager_unknown").is_none());
    }

    #[test]
    fn every_catalog_method_has_a_replay_wrapper() {
        for method in KNOWN_METHODS {
            assert!(
                spec(method).is_some(),
                "catalog method missing metadata: {method}"
            );
            assert!(
                crate::manager::gateway::mutation_wrapper_for_method(method).is_some(),
                "catalog method missing replay wrapper: {method}"
            );
        }
    }
}
