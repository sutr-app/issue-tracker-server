use super::*;

impl IssueTrackerApplication {
    pub(super) fn idempotency_scope(
        context: Option<&manager_v1::ManagerWriteContext>,
        method: &str,
    ) -> Option<String> {
        let context = context?;
        IdempotencyScope::new(
            context.contract_fingerprint.clone(),
            context.data_root_id.clone(),
            method,
            context.target.clone(),
            context.idempotency_key.clone(),
        )
        .ok()
        .map(|scope| scope.canonical())
    }

    pub(super) async fn stored_resolution_saga(
        &self,
        scope: &str,
    ) -> Result<Option<manager_v1::ResolveCandidateArgs>, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .resolution_saga_for_idempotency_scope(scope)
            .map_err(|error| error.message)
    }

    pub(super) async fn stored_backend_request(
        &self,
        scope: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        let manager = self.manager.as_ref().ok_or("manager is not configured")?;
        manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .backend_request_for_idempotency_scope(scope)
            .map_err(|error| error.message)
    }

    #[allow(clippy::result_large_err)]
    pub(super) async fn prepare_issue_merge_provenance(
        &self,
        mut args: manager_v1::ResolveCandidateArgs,
    ) -> Result<manager_v1::ResolveCandidateArgs, manager_v1::ManagerError> {
        use crate::manager::source_labels::{
            replace_manager_metadata, snapshots_from_issue_metadata,
            snapshots_from_manager_metadata, union_snapshots,
        };
        use manager_v1::{
            ManagerError, ManagerErrorCode, merge_resolution::Target,
            resolve_candidate_args::Decision,
        };
        let invalid = |message: String| ManagerError {
            code: ManagerErrorCode::ManagerInvalidArgument as i32,
            message,
            retryable: false,
            detail: None,
        };
        let precondition = |message: String| ManagerError {
            code: ManagerErrorCode::PreconditionFailed as i32,
            message,
            retryable: false,
            detail: None,
        };
        let Some(Decision::Merge(merge)) = args.decision.as_ref() else {
            return Ok(args);
        };
        let Some(Target::Issue(target)) = merge.target.as_ref() else {
            return Ok(args);
        };
        if !target.resolved_provenance_metadata.is_empty()
            || !target.resolved_source_evidence.is_empty()
        {
            return Err(invalid(
                "issue merge resolution fields are server-owned".into(),
            ));
        }
        let stored =
            match Self::idempotency_scope(args.context.as_ref(), "manager_resolve_candidate") {
                Some(scope) => self
                    .stored_resolution_saga(&scope)
                    .await
                    .map_err(precondition)?,
                None => None,
            };
        if let Some(stored) = stored {
            let mut external = stored.clone();
            if let Some(Decision::Merge(merge)) = external.decision.as_mut()
                && let Some(Target::Issue(target)) = merge.target.as_mut()
            {
                target.resolved_provenance_metadata.clear();
                target.resolved_source_evidence.clear();
            }
            if external == args {
                return Ok(stored);
            }
        }
        target
            .expected_revision
            .ok_or_else(|| invalid("target issue expected_revision is required".into()))?;
        let target_issue_id = target.issue_id.clone();
        let candidate_id = args.candidate_id.clone();
        let (candidates, source_evidence_limits) = {
            let manager = self
                .manager
                .as_ref()
                .ok_or_else(|| precondition("manager is not configured".into()))?;
            let mut manager = manager.lock().await;
            let source_evidence_limits = manager.config().source_evidence_limits();
            let candidates = manager
                .storage_mut()
                .repository()
                .candidates()
                .map_err(|error| precondition(error.message))?;
            (candidates, source_evidence_limits)
        };
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.candidate_id == candidate_id)
            .ok_or_else(|| precondition("merge source candidate was not found".into()))?;
        let issue = self
            .core
            .tracker()
            .list()
            .await
            .map_err(|error| precondition(error.to_string()))?
            .into_iter()
            .map(|issue| crate::tracker::conversion::issue_proto(&issue))
            .find(|issue| issue.issue_id == target_issue_id)
            .ok_or_else(|| precondition("merge target issue was not found".into()))?;
        // This admission runs before the manager driver's idempotency lookup.
        // Do not reject a completed retry based on the Issue's current
        // revision: rebuilding the canonical provenance lets the repository
        // replay its stored resolution result. A genuinely new request still
        // carries its expected revision into the durable Issue update, where
        // the CAS precondition is enforced.
        let mut groups = Vec::new();
        if let Some(snapshots) =
            snapshots_from_manager_metadata(&candidate.metadata).map_err(invalid)?
        {
            groups.push(snapshots);
        }
        if let Some(snapshots) =
            snapshots_from_issue_metadata(&issue.provenance_metadata).map_err(invalid)?
        {
            groups.push(snapshots);
        }
        let snapshots = union_snapshots(groups).map_err(invalid)?;
        let Some(Decision::Merge(merge)) = args.decision.as_mut() else {
            return Err(invalid("merge decision changed during admission".into()));
        };
        let Some(Target::Issue(target)) = merge.target.as_mut() else {
            return Err(invalid("merge target changed during admission".into()));
        };
        target.resolved_provenance_metadata = issue
            .provenance_metadata
            .iter()
            .map(|entry| manager_v1::MetadataEntry {
                key: entry.key.clone(),
                value: entry.value.clone(),
            })
            .collect();
        replace_manager_metadata(&mut target.resolved_provenance_metadata, &snapshots)
            .map_err(invalid)?;
        target.resolved_source_evidence = union_source_evidence_refs(
            &issue.source_evidence,
            &candidate.source_evidence,
            source_evidence_limits,
        )
        .map_err(invalid)?;
        Ok(args)
    }

    #[allow(clippy::result_large_err)]
    pub(super) async fn prepare_confirm_source_evidence(
        &self,
        mut args: manager_v1::ResolveCandidateArgs,
    ) -> Result<manager_v1::ResolveCandidateArgs, manager_v1::ManagerError> {
        use manager_v1::{ManagerError, ManagerErrorCode, resolve_candidate_args::Decision};
        let invalid = |message: String| ManagerError {
            code: ManagerErrorCode::ManagerInvalidArgument as i32,
            message,
            retryable: false,
            detail: None,
        };
        let precondition = |message: String| ManagerError {
            code: ManagerErrorCode::PreconditionFailed as i32,
            message,
            retryable: false,
            detail: None,
        };
        let Some(Decision::ConfirmAssociatedIssue(confirm)) = args.decision.as_ref() else {
            return Ok(args);
        };
        if !confirm.resolved_source_evidence.is_empty() {
            return Err(invalid(
                "confirm associated issue source evidence is server-owned".into(),
            ));
        }
        let stored =
            match Self::idempotency_scope(args.context.as_ref(), "manager_resolve_candidate") {
                Some(scope) => self
                    .stored_resolution_saga(&scope)
                    .await
                    .map_err(precondition)?,
                None => None,
            };
        if let Some(stored) = stored {
            let mut external = stored.clone();
            if let Some(Decision::ConfirmAssociatedIssue(confirm)) = external.decision.as_mut() {
                confirm.resolved_source_evidence.clear();
            }
            if external == args {
                return Ok(stored);
            }
        }
        confirm
            .expected_revision
            .ok_or_else(|| invalid("target issue expected_revision is required".into()))?;
        let issue_id = confirm.issue_id.clone();
        let candidate_id = args.candidate_id.clone();
        let (candidate, source_evidence_limits) = {
            let manager = self
                .manager
                .as_ref()
                .ok_or_else(|| precondition("manager is not configured".into()))?;
            let mut manager = manager.lock().await;
            let source_evidence_limits = manager.config().source_evidence_limits();
            let candidate = manager
                .storage_mut()
                .repository()
                .candidates()
                .map_err(|error| precondition(error.message))?
                .into_iter()
                .find(|candidate| candidate.candidate_id == candidate_id)
                .ok_or_else(|| precondition("confirm source candidate was not found".into()))?;
            (candidate, source_evidence_limits)
        };
        let issue = self
            .core
            .tracker()
            .list()
            .await
            .map_err(|error| precondition(error.to_string()))?
            .into_iter()
            .map(|issue| crate::tracker::conversion::issue_proto(&issue))
            .find(|issue| issue.issue_id == issue_id)
            .ok_or_else(|| precondition("confirm target issue was not found".into()))?;
        let Some(Decision::ConfirmAssociatedIssue(confirm)) = args.decision.as_mut() else {
            return Err(invalid("confirm decision changed during admission".into()));
        };
        confirm.resolved_source_evidence = resolved_confirmation_source_evidence(
            confirm,
            &issue.source_evidence,
            &candidate.source_evidence,
            source_evidence_limits,
        )
        .map_err(invalid)?;
        Ok(args)
    }

    #[allow(clippy::result_large_err)]
    pub(super) async fn prepare_create_issue_provenance(
        &self,
        mut args: manager_v1::CreateIssueArgs,
    ) -> Result<manager_v1::CreateIssueArgs, manager_v1::ManagerError> {
        use crate::manager::source_labels::{
            has_source_label_metadata, replace_issue_metadata, snapshots_from_issue_metadata,
            snapshots_from_manager_metadata, union_snapshots,
        };
        use manager_v1::{ManagerError, ManagerErrorCode, provenance_source_ref::Source};

        let invalid = |message: String| ManagerError {
            code: ManagerErrorCode::ManagerInvalidArgument as i32,
            message,
            retryable: false,
            detail: None,
        };
        let precondition = |message: String| ManagerError {
            code: ManagerErrorCode::PreconditionFailed as i32,
            message,
            retryable: false,
            detail: None,
        };
        let draft = args
            .issue
            .as_ref()
            .ok_or_else(|| invalid("issue draft is required".into()))?;
        if has_source_label_metadata(&draft.provenance_metadata) {
            return Err(invalid(
                "source label snapshots must be derived by the server".into(),
            ));
        }
        if args.provenance_sources.is_empty() {
            return Ok(args);
        }

        let request = match Self::idempotency_scope(args.context.as_ref(), "manager_create_issue") {
            Some(scope) => self
                .stored_backend_request(&scope)
                .await
                .map_err(precondition)?,
            None => None,
        };
        if let Some(request) = request
            && let Ok(envelope) =
                crate::manager::backend_dispatch::DurableBackendRequest::decode(request.as_slice())
            && let Some(create) = envelope.create_issue
            && let Ok(stored) =
                manager_v1::CreateIssueArgs::decode(create.admitted_manager_args.as_slice())
        {
            let mut external = stored.clone();
            if let Some(issue) = external.issue.as_mut() {
                issue.provenance_metadata.retain(|entry| {
                    entry.key != crate::manager::source_labels::SOURCE_LABEL_SNAPSHOTS_KEY
                });
            }
            if external == args {
                return Ok(stored);
            }
        }

        let candidates = {
            let manager = self
                .manager
                .as_ref()
                .ok_or_else(|| precondition("manager is not configured".into()))?;
            let mut manager = manager.lock().await;
            manager
                .storage_mut()
                .repository()
                .candidates()
                .map_err(|error| precondition(error.message))?
        };
        let issues = self
            .core
            .tracker()
            .list()
            .await
            .map_err(|error| precondition(error.to_string()))?
            .into_iter()
            .map(|issue| crate::tracker::conversion::issue_proto(&issue))
            .collect::<Vec<_>>();
        let mut groups = Vec::new();
        let mut stale_source = false;
        for reference in &args.provenance_sources {
            let expected = reference
                .expected_revision
                .ok_or_else(|| invalid("provenance source expected_revision is required".into()))?;
            match reference.source.as_ref() {
                Some(Source::CandidateId(id)) if !id.is_empty() => {
                    let candidate = candidates
                        .iter()
                        .find(|candidate| candidate.candidate_id == *id)
                        .ok_or_else(|| {
                            precondition("provenance source candidate was not found".into())
                        })?;
                    stale_source |= candidate.revision != expected;
                    if let Some(snapshots) =
                        snapshots_from_manager_metadata(&candidate.metadata).map_err(invalid)?
                    {
                        groups.push(snapshots);
                    }
                }
                Some(Source::IssueId(id)) if !id.is_empty() => {
                    let issue = issues
                        .iter()
                        .find(|issue| issue.issue_id == *id)
                        .ok_or_else(|| {
                            precondition("provenance source issue was not found".into())
                        })?;
                    stale_source |= issue.revision != expected;
                    if let Some(snapshots) =
                        snapshots_from_issue_metadata(&issue.provenance_metadata)
                            .map_err(invalid)?
                    {
                        groups.push(snapshots);
                    }
                }
                _ => return Err(invalid("provenance source ID is required".into())),
            }
        }
        if stale_source {
            let scope = Self::idempotency_scope(args.context.as_ref(), "manager_create_issue")
                .ok_or_else(|| {
                    invalid("manager write context is required for provenance sources".into())
                })?;
            if self
                .stored_backend_request(&scope)
                .await
                .map_err(precondition)?
                .is_none()
            {
                return Err(precondition("provenance source revision conflict".into()));
            }
        }
        let snapshots = union_snapshots(groups).map_err(invalid)?;
        if !snapshots.is_empty() {
            replace_issue_metadata(
                &mut args
                    .issue
                    .as_mut()
                    .expect("draft was checked")
                    .provenance_metadata,
                &snapshots,
            )
            .map_err(invalid)?;
        }
        Ok(args)
    }
}
