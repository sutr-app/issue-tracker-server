#[derive(Clone, Copy)]
pub(crate) struct MethodSpec {
    pub(crate) key: &'static str,
    pub(crate) args: &'static str,
    pub(crate) result: &'static str,
    pub(crate) description: &'static str,
}

macro_rules! method_specs {
    ($(($key:literal, $args:literal, $result:literal, $description:literal)),+ $(,)?) => {
        &[
            $(MethodSpec { key: $key, args: $args, result: $result, description: $description }),+
        ]
    };
}

pub(crate) const METHOD_SPECS: &[MethodSpec] = method_specs![
    (
        "get_contract_info",
        "GetContractInfoArgs",
        "GetContractInfoResult",
        "Read the tracker contract, storage schema fingerprint, supported method keys, and health status. Use this before writes to verify compatibility."
    ),
    (
        "get_issue",
        "GetIssueArgs",
        "GetIssueResult",
        "Read one issue by ID, including its canonical state, revision, claim, and direction-preserving relations. This method is read-only."
    ),
    (
        "list_issues",
        "ListIssuesArgs",
        "ListIssuesResult",
        "List issues with optional state, kind, priority, label, and context filters using a stable page token. Use this for broad read-only searches."
    ),
    (
        "list_ready_issues",
        "ListReadyIssuesArgs",
        "ListReadyIssuesResult",
        "List issues that are open, confirmed, unblocked, and have no active or unresolved claim. Use this as the work-start queue; listing never reconciles expired claims."
    ),
    (
        "list_issue_comments",
        "ListIssueCommentsArgs",
        "ListIssueCommentsResult",
        "Read the append-only comments for one issue with pagination. Use this to inspect discussion without changing the issue."
    ),
    (
        "list_issue_history",
        "ListIssueHistoryArgs",
        "ListIssueHistoryResult",
        "Read immutable state, claim, relation, comment, and operation history for one issue with pagination. Use this for audit and troubleshooting."
    ),
    (
        "get_artifact",
        "GetArtifactArgs",
        "GetArtifactResult",
        "Read metadata for one validated backup or export artifact by ID. Artifact content and arbitrary filesystem paths are never returned."
    ),
    (
        "list_artifacts",
        "ListArtifactsArgs",
        "ListArtifactsResult",
        "List validated backup and export artifact metadata with pagination. Use this to discover artifacts; artifact content is not returned."
    ),
    (
        "get_operation",
        "GetOperationArgs",
        "GetOperationResult",
        "Read a root-global downstream operation record to inspect idempotency, commit durability, result replayability, affected entities, and result references."
    ),
    (
        "create_issue",
        "CreateIssueArgs",
        "CreateIssueResult",
        "Create a canonical issue with a caller-supplied ID and write context. Auto-detected issues retain their supplied priority; provide explicit creation and confirmation fields."
    ),
    (
        "update_issue",
        "UpdateIssueArgs",
        "UpdateIssueResult",
        "Update issue fields with an expected revision and closed update mask. Include only fields named by the mask; repeated fields replace their complete value. This is a write."
    ),
    (
        "transition_issue",
        "TransitionIssueArgs",
        "TransitionIssueResult",
        "Change an issue state with an expected revision. DONE requires no unfinished blockers, a reason, and verified or explicitly unverified completion evidence; active claims require the matching lease handle and claim revision."
    ),
    (
        "claim_issue",
        "ClaimIssueArgs",
        "ClaimIssueResult",
        "Claim an open issue atomically with a lease. The result contains a one-time plaintext lease handle; store it securely and use it for heartbeat, release, or completion. Active claims cannot be claimed again."
    ),
    (
        "heartbeat_issue_claim",
        "HeartbeatIssueClaimArgs",
        "HeartbeatIssueClaimResult",
        "Extend an existing claim before expiry using the issue ID, expected revision, claim revision, owner, and lease handle. Expired or mismatched leases are rejected."
    ),
    (
        "release_issue_claim",
        "ReleaseIssueClaimArgs",
        "ReleaseIssueClaimResult",
        "Release the active issue claim using the expected revision, claim revision, owner, and lease handle. An in-progress Issue returns to OPEN atomically. Expired or different leases are rejected."
    ),
    (
        "reconcile_expired_claim",
        "ReconcileExpiredClaimArgs",
        "ReconcileExpiredClaimResult",
        "Reconcile one expired in-progress issue claim into open using the expected revision. This is the only expiry recovery write; ready listing never mutates state."
    ),
    (
        "add_issue_relation",
        "AddIssueRelationArgs",
        "AddIssueRelationResult",
        "Add a directed relation from the source issue to the target issue; for blocks, the source blocks the target. Supply both endpoint revisions. Self-links and blocking cycles are rejected."
    ),
    (
        "remove_issue_relation",
        "RemoveIssueRelationArgs",
        "RemoveIssueRelationResult",
        "Remove a directed relation between the source and target issues using both endpoint expected revisions. Unrelated relations are not changed."
    ),
    (
        "add_issue_comment",
        "AddIssueCommentArgs",
        "AddIssueCommentResult",
        "Append an immutable comment to an issue using the expected revision and write context. Create a new comment instead of editing an existing one."
    ),
    (
        "create_backup",
        "CreateBackupArgs",
        "CreateBackupResult",
        "Create a validated backup artifact under the runner-reserved root using write context. The operation is idempotent; arbitrary destination paths and live-root restore are not allowed."
    ),
    (
        "export_issues",
        "ExportIssuesArgs",
        "ExportIssuesResult",
        "Export canonical issues, directed relations, and immutable history as a backend-neutral artifact using write context. Metadata tasks and secret-like values are excluded or rejected."
    ),
];

pub(crate) const MANAGER_METHOD_SPECS: &[MethodSpec] = method_specs![
    (
        "manager_get_contract_info",
        "GetContractInfoArgs",
        "GetContractInfoResult",
        "Read the manager contract, supported method keys, and health status before issuing manager writes."
    ),
    (
        "manager_create_candidate",
        "CreateCandidateArgs",
        "CreateCandidateResult",
        "Create an issue candidate with write context, extraction provenance, and evidence references."
    ),
    (
        "manager_get_candidate",
        "GetCandidateArgs",
        "GetCandidateResult",
        "Read one manager candidate and its canonical lifecycle state without changing it."
    ),
    (
        "manager_list_candidates",
        "ListCandidatesArgs",
        "ListCandidatesResult",
        "List manager candidates with filters and stable pagination for read-only review."
    ),
    (
        "manager_list_review_candidates",
        "ListReviewCandidatesArgs",
        "ListReviewCandidatesResult",
        "List review candidates joined with manager mappings and backend Issue lifecycle state using stable trusted-consumer pagination."
    ),
    (
        "manager_update_candidate",
        "UpdateCandidateArgs",
        "UpdateCandidateResult",
        "Update a PROPOSED manager candidate using a typed CandidatePatch, its expected revision, and a closed field mask."
    ),
    (
        "manager_resolve_candidate",
        "ResolveCandidateArgs",
        "ResolveCandidateResult",
        "Resolve a candidate through one explicit create, confirm, reject, cancel, or merge decision."
    ),
    (
        "manager_create_extraction_run",
        "CreateExtractionRunArgs",
        "CreateExtractionRunResult",
        "Create an extraction run that records candidate discovery inputs and provenance."
    ),
    (
        "manager_get_extraction_run",
        "GetExtractionRunArgs",
        "GetExtractionRunResult",
        "Read one extraction run and its state without changing the manager record."
    ),
    (
        "manager_list_extraction_runs",
        "ListExtractionRunsArgs",
        "ListExtractionRunsResult",
        "List extraction runs with stable pagination for manager auditing."
    ),
    (
        "manager_advance_extraction_run",
        "AdvanceExtractionRunArgs",
        "AdvanceExtractionRunResult",
        "Atomically advance extraction state, progress, and a successful checkpoint watermark using the run's expected revision. Failed or unconfirmed runs never advance the checkpoint."
    ),
    (
        "manager_resume_extraction_run",
        "ResumeExtractionRunArgs",
        "ResumeExtractionRunResult",
        "Resume a paused or recoverable extraction run with write context and expected revision."
    ),
    (
        "manager_get_operation",
        "GetOperationArgs",
        "GetOperationResult",
        "Read a durable manager operation to inspect downstream execution and resolution state."
    ),
    (
        "manager_retry_operation",
        "RetryOperationArgs",
        "RetryOperationResult",
        "Retry an eligible durable manager operation while preserving idempotency and audit state."
    ),
    (
        "manager_reconcile_operation",
        "ReconcileOperationArgs",
        "ReconcileOperationResult",
        "Reconcile a durable manager operation when downstream execution requires explicit resolution."
    ),
    (
        "manager_get_issue_with_sync",
        "GetIssueWithSyncArgs",
        "GetIssueWithSyncResult",
        "Read an issue together with its manager synchronization state without changing either record."
    ),
    (
        "manager_get_issue_relations",
        "GetIssueRelationsArgs",
        "GetIssueRelationsResult",
        "Read one canonical issue together with its inbound and outbound relations."
    ),
    (
        "manager_list_issues_with_sync",
        "ListIssuesWithSyncArgs",
        "ListIssuesWithSyncResult",
        "List canonical issues with manager synchronization data using conjunctive canonical and synchronization filters."
    ),
    (
        "manager_list_operation_history",
        "ListOperationHistoryArgs",
        "ListOperationHistoryResult",
        "List durable operation history for audit display with filters and stable pagination."
    ),
    (
        "manager_create_issue",
        "CreateIssueArgs",
        "CreateIssueResult",
        "Create a canonical issue through the manager with verified evidence. Auto-detected creation also requires a strong detection proof and remains unreviewed."
    ),
    (
        "manager_update_issue",
        "UpdateIssueArgs",
        "UpdateIssueResult",
        "Update a canonical issue through the manager with a typed semantic patch, closed field mask, and revision precondition."
    ),
    (
        "manager_transition_issue",
        "TransitionIssueArgs",
        "TransitionIssueResult",
        "Transition a canonical issue through the manager with a typed target state, revision precondition, lease proof when required, and terminal report."
    ),
    (
        "manager_apply_issue_admin_action",
        "ApplyIssueAdminActionArgs",
        "ApplyIssueAdminActionResult",
        "Apply a trusted-consumer administrative completion, cancellation, recovery, or revocation with an issue revision precondition and audit reason."
    ),
    (
        "manager_resolve_issue_operation",
        "ResolveIssueOperationArgs",
        "ResolveIssueOperationResult",
        "Resolve a delivery-unknown Issue operation using a server-owned downstream marker and operation/revision preconditions."
    ),
    (
        "manager_list_ownership_diagnostics",
        "ListOwnershipDiagnosticsArgs",
        "ListOwnershipDiagnosticsResult",
        "List safe ownership-integrity diagnoses for trusted-consumer recovery without exposing raw TaskData, handles, or request payloads."
    ),
    (
        "manager_claim_issue",
        "ClaimIssueArgs",
        "ClaimIssueResult",
        "Claim a canonical issue through the manager. The lease handle is returned only on the first successful response and is never stored or replayed."
    ),
    (
        "manager_heartbeat_issue_claim",
        "HeartbeatIssueClaimArgs",
        "HeartbeatIssueClaimResult",
        "Extend a canonical issue claim through the manager using its lease handle, claim revision, issue revision, and requested extension."
    ),
    (
        "manager_release_issue_claim",
        "ReleaseIssueClaimArgs",
        "ReleaseIssueClaimResult",
        "Release a canonical issue claim through the manager; an in-progress Issue returns to OPEN atomically, using its lease handle, claim revision, and issue revision."
    ),
    (
        "manager_reconcile_expired_claim",
        "ReconcileExpiredClaimArgs",
        "ReconcileExpiredClaimResult",
        "Reconcile an expired canonical issue claim through the manager using claim and issue revision preconditions; this operation does not require the expired handle."
    ),
    (
        "manager_add_issue_relation",
        "AddIssueRelationArgs",
        "AddIssueRelationResult",
        "Add a directed canonical issue relation through the manager with a stable relation ID and revision preconditions for both endpoints."
    ),
    (
        "manager_remove_issue_relation",
        "RemoveIssueRelationArgs",
        "RemoveIssueRelationResult",
        "Remove a directed canonical issue relation through the manager with its stable relation ID and revision preconditions for both endpoints."
    ),
    (
        "manager_add_issue_comment",
        "AddIssueCommentArgs",
        "AddIssueCommentResult",
        "Append a canonical issue comment through the manager with a stable comment ID and issue revision precondition."
    ),
];
