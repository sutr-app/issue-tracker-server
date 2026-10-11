//! Shared resident ownership maintenance for the gRPC server and static runner.

use super::{
    Tracker, TrackerError, issue_read::LeaseMaintenanceScanEntry, issue_write::LeaseState,
};
use crate::manager::{IssueTrackerManager, repository::OwnershipRecoveryState};
use std::{
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

/// Reconcile one bounded, cursor-rotated page of expired or unowned leases.
/// The Manager lock is never held while TaskChampion is awaited. Every
/// malformed entry and per-Issue write error is isolated so delivery and later
/// candidates continue; a non-empty error is returned only for health state.
#[allow(dead_code)]
pub(crate) async fn run(
    tracker: &Tracker,
    manager: Option<&Arc<Mutex<IssueTrackerManager>>>,
    batch_size: usize,
) -> Result<u32, String> {
    run_with_recovery(
        tracker,
        manager,
        batch_size,
        &|issue_id, revision, operation_id, _state, reason| async move {
            tracker
                .recover_claim(
                    issue_id,
                    revision,
                    false,
                    &operation_id,
                    "system-maintenance",
                    reason.as_str(),
                )
                .await
                .map(|_| ())
        },
    )
    .await
}

/// Stable audit reason attached to a server-generated maintenance request.
/// The inconsistent variant is only used for the rejected diagnostic attempt;
/// it never authorizes an automatic ownership repair.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnershipRecoveryReason {
    ExpiredClaim,
    UnownedInProgress,
    InconsistentLease,
}

impl OwnershipRecoveryReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ExpiredClaim => "expired_claim",
            Self::UnownedInProgress => "unowned_in_progress",
            Self::InconsistentLease => "lease_inconsistent",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaintenanceStats {
    pub scanned: u32,
    pub recovered: u32,
    pub skipped: u32,
    pub conflicts: u32,
    pub diagnoses: u32,
    pub unconfirmed: u32,
    pub failed: bool,
    pub error: Option<String>,
}

/// Same scan as [`run`], but lets the owner supply the mutation boundary.
/// Production runners pass an outbox-backed administrative admission here;
/// the small wrapper above remains for Tracker-only users that intentionally
/// have no Manager root.
#[allow(dead_code)]
pub(crate) async fn run_with_recovery<F, Fut>(
    tracker: &Tracker,
    manager: Option<&Arc<Mutex<IssueTrackerManager>>>,
    batch_size: usize,
    recover: &F,
) -> Result<u32, String>
where
    F: Fn(uuid::Uuid, u64, String, OwnershipRecoveryState, OwnershipRecoveryReason) -> Fut,
    Fut: Future<Output = Result<(), TrackerError>>,
{
    let stats = run_with_recovery_stats(tracker, manager, batch_size, recover).await?;
    if stats.failed {
        Err(stats
            .error
            .unwrap_or_else(|| "ownership maintenance failed".into()))
    } else {
        Ok(stats.recovered)
    }
}

pub(crate) async fn run_with_recovery_stats<F, Fut>(
    tracker: &Tracker,
    manager: Option<&Arc<Mutex<IssueTrackerManager>>>,
    batch_size: usize,
    recover: &F,
) -> Result<MaintenanceStats, String>
where
    F: Fn(uuid::Uuid, u64, String, OwnershipRecoveryState, OwnershipRecoveryReason) -> Fut,
    Fut: Future<Output = Result<(), TrackerError>>,
{
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    let now_millis = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis(),
    )
    .map_err(|_| "maintenance clock exceeds i64 milliseconds".to_owned())?;
    let (cursor, scan_complete) = if let Some(manager) = manager {
        let mut manager = manager.lock().await;
        let repository = manager.storage_mut().repository();
        (
            repository
                .ownership_maintenance_cursor()
                .map_err(|error| error.to_string())?,
            repository
                .ownership_maintenance_scan_complete()
                .map_err(|error| error.to_string())?,
        )
    } else {
        (None, true)
    };
    let page = tracker
        .lease_maintenance_candidates_page_with_status(now, cursor.as_deref(), batch_size)
        .await
        .map_err(|error| error.to_string())?;
    let page_reached_end = page.reached_end;
    let entries = page.entries;

    let mut stats = MaintenanceStats::default();
    let mut maintenance_error: Option<String> = None;
    if let Some(manager) = manager
        && let Err(error) = manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .recover_ownership_recovery_projections()
    {
        maintenance_error = Some(error.to_string());
    }

    for entry in entries.into_iter().take(batch_size) {
        stats.scanned = stats.scanned.saturating_add(1);
        let evaluated_issue_id = entry.cursor_id().to_string();
        let issue = match entry {
            LeaseMaintenanceScanEntry::Valid(issue) => issue,
            LeaseMaintenanceScanEntry::Skipped { .. } => {
                stats.skipped = stats.skipped.saturating_add(1);
                checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
                continue;
            }
            LeaseMaintenanceScanEntry::Invalid {
                storage_id,
                classification,
                ..
            } => {
                stats.diagnoses = stats.diagnoses.saturating_add(1);
                // Invalid raw rows require explicit repair; cursor advancement
                // keeps one broken row from starving all later candidates.
                if let Some(manager) = manager {
                    let mut manager_guard = manager.lock().await;
                    if let Err(error) = manager_guard
                        .storage_mut()
                        .repository()
                        .record_ownership_integrity_diagnosis(
                            "physical_task_uuid",
                            &storage_id.to_string(),
                            classification,
                            None,
                            Some("INCONSISTENT"),
                            "repair the Tracker mapping before automatic ownership recovery",
                            &[],
                            None,
                            &[],
                        )
                    {
                        maintenance_error.get_or_insert(error.to_string());
                    }
                }
                maintenance_error.get_or_insert_with(|| {
                    "ownership maintenance found an invalid raw Issue entry".to_owned()
                });
                checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
                continue;
            }
        };

        let mut state = if let Some(manager) = manager {
            match manager
                .lock()
                .await
                .storage_mut()
                .repository()
                .ownership_recovery_state(&issue.id.to_string())
            {
                Ok(Some(state)) => state,
                Ok(None) => empty_state(issue.id.to_string(), issue.revision),
                Err(error) => {
                    maintenance_error.get_or_insert_with(|| error.to_string());
                    checkpoint_cursor(Some(manager), &evaluated_issue_id, &mut maintenance_error)
                        .await;
                    continue;
                }
            }
        } else {
            empty_state(issue.id.to_string(), issue.revision)
        };
        if state.in_flight && state.current_operation_id.is_none() {
            state.stopped = true;
            state.last_failure_class = Some("IN_FLIGHT_OPERATION_MISSING".into());
            state.stop_basis = Some("in-flight ownership recovery has no operation ID".into());
            state.repair_condition = Some("repair the ownership recovery index".into());
            maintenance_error.get_or_insert_with(|| {
                format!("ownership recovery index is inconsistent for {}", issue.id)
            });
            if let Err(error) = persist(manager, &state).await {
                maintenance_error.get_or_insert(error);
            }
            checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
            continue;
        }
        // An in-flight operation is reconciled through the owner-supplied
        // recovery boundary below.  Do not inspect a Tracker marker here and
        // clear the Manager projection in a separate write: doing so could
        // release the fence while Manager finalization is still pending.
        // If the Issue changed since admission, retain the fence and require
        // an explicit operator decision rather than reusing the old request.
        if state.in_flight
            && (state.current_operation_id.is_none()
                || state.expected_revision != issue.revision
                || !is_recovery_candidate(&issue))
        {
            state.stopped = true;
            state.last_failure_class = Some("IN_FLIGHT_ISSUE_CHANGED".into());
            state.stop_basis = Some("ownership recovery request no longer matches Issue".into());
            state.repair_condition =
                Some("verify the historical operation and Issue revision".into());
            maintenance_error.get_or_insert_with(|| {
                format!(
                    "ownership recovery requires manual reconciliation for {}",
                    issue.id
                )
            });
            if let Err(error) = persist(manager, &state).await {
                maintenance_error.get_or_insert(error);
            }
            checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
            continue;
        }

        if state.stopped {
            // An ACTIVE_OR_INCONSISTENT_LEASE or LEASE_INCONSISTENT stop is
            // recoverable after the lease expires or a human administrative
            // action (typically REVOKE) clears the tuple. Do not apply this
            // automatic reset to marker corruption or an unknown in-flight
            // operation: those still require explicit evidence repair.
            let lease_repaired = matches!(
                state.last_failure_class.as_deref(),
                Some("ACTIVE_OR_INCONSISTENT_LEASE") | Some("LEASE_INCONSISTENT")
            ) && !matches!(
                issue.lease_state,
                LeaseState::Active | LeaseState::Inconsistent
            );
            if lease_repaired {
                state.stopped = false;
                state.stop_basis = None;
                state.repair_condition = None;
                state.expected_revision = issue.revision;
                state.readmission_number = match state.readmission_number.checked_add(1) {
                    Some(value) => value,
                    None => {
                        maintenance_error.get_or_insert_with(|| {
                            format!(
                                "ownership recovery readmission number overflow for {}",
                                issue.id
                            )
                        });
                        checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error)
                            .await;
                        continue;
                    }
                };
                state.current_operation_id = None;
                state.in_flight = false;
                state.consecutive_failures = 0;
                state.next_attempt_at_millis = None;
                state.last_failure_class = None;
                if let Some(manager) = manager {
                    let stale_error = {
                        let mut manager_guard = manager.lock().await;
                        manager_guard
                            .storage_mut()
                            .repository()
                            .stale_ownership_integrity_diagnosis_and_recovery_state(
                                &state,
                                "issue",
                                &issue.id.to_string(),
                                issue.revision,
                            )
                            .err()
                    };
                    if let Some(error) = stale_error {
                        maintenance_error.get_or_insert(error.to_string());
                        checkpoint_cursor(
                            Some(manager),
                            &evaluated_issue_id,
                            &mut maintenance_error,
                        )
                        .await;
                        continue;
                    }
                }
            } else {
                // A stopped or backoff entry is unresolved operational state,
                // not a successful no-op. Keep health degraded until its
                // recorded repair condition is met.
                maintenance_error.get_or_insert_with(|| {
                    format!("ownership recovery is pending for {}", issue.id)
                });
                checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
                continue;
            }
        }
        if state
            .next_attempt_at_millis
            .is_some_and(|next| next > now_millis)
        {
            // A future retry is unresolved operational state, not a
            // successful no-op. Keep health degraded until it is attempted.
            maintenance_error.get_or_insert_with(|| {
                format!("ownership recovery is in backoff for {}", issue.id)
            });
            checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
            continue;
        }
        if !is_recovery_candidate(&issue) {
            checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
            continue;
        }
        // An inconsistent ownership tuple is a diagnosis-only condition. Do
        // not allocate an operation ID or invoke the recovery callback for a
        // fresh candidate: RECOVER is intentionally restricted to EXPIRED
        // and NONE+IN_PROGRESS. An operation that was already admitted by an
        // older process is still allowed through the in-flight path below so
        // its durable result can be reconciled safely.
        if issue.lease_state == LeaseState::Inconsistent && !state.in_flight {
            stats.diagnoses = stats.diagnoses.saturating_add(1);
            state.expected_revision = issue.revision;
            state.current_operation_id = None;
            state.in_flight = false;
            state.stopped = true;
            state.next_attempt_at_millis = None;
            state.last_failure_class = Some("LEASE_INCONSISTENT".into());
            state.stop_basis = Some("ownership lease tuple is inconsistent".into());
            state.repair_condition =
                Some("clear the inconsistent lease through an administrative action".into());
            maintenance_error.get_or_insert_with(|| {
                format!("ownership recovery requires lease repair for {}", issue.id)
            });
            if let Some(manager) = manager {
                let mut manager_guard = manager.lock().await;
                if let Err(error) = manager_guard
                    .storage_mut()
                    .repository()
                    .record_ownership_integrity_diagnosis_and_recovery_state(
                        &state,
                        "issue",
                        &issue.id.to_string(),
                        "LEASE_INCONSISTENT",
                        Some(issue.revision),
                        Some("INCONSISTENT"),
                        "clear the inconsistent lease through an administrative action",
                        &[],
                        issue.expires_at,
                        &[],
                    )
                {
                    maintenance_error.get_or_insert(error.to_string());
                }
            }
            checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
            continue;
        }
        if state.expected_revision != issue.revision {
            state.expected_revision = issue.revision;
            let Some(readmission_number) = state.readmission_number.checked_add(1) else {
                maintenance_error.get_or_insert_with(|| {
                    format!(
                        "ownership recovery readmission number overflow for {}",
                        issue.id
                    )
                });
                checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
                continue;
            };
            state.readmission_number = readmission_number;
            state.current_operation_id = None;
            state.in_flight = false;
            state.next_attempt_at_millis = None;
            state.last_failure_class = None;
            state.stop_basis = None;
            state.repair_condition = None;
            if let Err(error) = persist(manager, &state).await {
                maintenance_error.get_or_insert(error);
                checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
                continue;
            }
        }

        let operation_id = state.current_operation_id.clone().unwrap_or_else(|| {
            format!(
                "ownership-maintenance-{}-{}-{}",
                issue.id, issue.revision, state.readmission_number
            )
        });
        state.current_operation_id = Some(operation_id.clone());
        state.in_flight = true;
        state.next_attempt_at_millis = None;

        // Manager-backed callbacks persist this in-flight state together
        // with the durable admission.  Avoiding a separate pre-admission
        // write prevents a readmission index from getting ahead of its
        // operation if the process stops at this boundary.  Tracker-only
        // callers have no Manager state and therefore retain the same
        // behaviour as before.
        match recover(
            issue.id,
            issue.revision,
            operation_id.clone(),
            state.clone(),
            recovery_reason(&issue),
        )
        .await
        {
            Ok(_) => {
                state.in_flight = false;
                state.current_operation_id = None;
                state.consecutive_failures = 0;
                state.next_attempt_at_millis = None;
                state.last_failure_class = None;
                stats.recovered = stats.recovered.saturating_add(1);
            }
            Err(TrackerError::RevisionConflict { .. }) => {
                stats.conflicts = stats.conflicts.saturating_add(1);
                state.in_flight = false;
                state.current_operation_id = None;
                let Some(readmission_number) = state.readmission_number.checked_add(1) else {
                    maintenance_error.get_or_insert_with(|| {
                        format!(
                            "ownership recovery readmission number overflow for {}",
                            issue.id
                        )
                    });
                    checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
                    continue;
                };
                state.readmission_number = readmission_number;
                state.last_failure_class = Some("REVISION_CONFLICT".into());
            }
            Err(
                error @ (TrackerError::InvalidLease { .. }
                | TrackerError::LeaseExpired { .. }
                | TrackerError::LeaseInconsistent { .. }),
            ) => {
                stats.diagnoses = stats.diagnoses.saturating_add(1);
                state.in_flight = false;
                state.current_operation_id = None;
                if matches!(issue.lease_state, LeaseState::Inconsistent) {
                    state.last_failure_class = Some("LEASE_INCONSISTENT".into());
                    state.stopped = true;
                    state.stop_basis = Some("ownership lease tuple is inconsistent".into());
                    state.repair_condition = Some(
                        "clear the inconsistent lease through an administrative action".into(),
                    );
                    maintenance_error.get_or_insert_with(|| {
                        format!("ownership recovery requires lease repair for {}", issue.id)
                    });
                    if let Some(manager) = manager {
                        let invalid_attributes = match &error {
                            TrackerError::LeaseInconsistent {
                                invalid_attributes, ..
                            } => invalid_attributes.clone(),
                            _ => Vec::new(),
                        };
                        let record_error = {
                            let mut manager_guard = manager.lock().await;
                            manager_guard
                                .storage_mut()
                                .repository()
                                .record_ownership_integrity_diagnosis_and_recovery_state(
                                    &state,
                                    "issue",
                                    &issue.id.to_string(),
                                    "LEASE_INCONSISTENT",
                                    Some(issue.revision),
                                    Some("INCONSISTENT"),
                                    "clear the inconsistent lease through an administrative action",
                                    &invalid_attributes,
                                    issue.expires_at,
                                    &[],
                                )
                                .err()
                        };
                        if let Some(record_error) = record_error {
                            maintenance_error.get_or_insert(record_error.to_string());
                        }
                        // The combined write owns the recovery-state
                        // persistence boundary. Do not let the common
                        // persist below write a stopped state when diagnosis
                        // admission failed; the next cursor cycle must retry
                        // both records together.
                        checkpoint_cursor(
                            Some(manager),
                            &evaluated_issue_id,
                            &mut maintenance_error,
                        )
                        .await;
                        continue;
                    }
                    maintenance_error.get_or_insert_with(|| {
                        format!("ownership recovery requires lease repair for {}", issue.id)
                    });
                } else if matches!(error, TrackerError::InvalidLease { .. }) {
                    // The lease became active after the scan. Reserve a new
                    // readmission number now so the immutable failed
                    // operation is never reused when the same lease later
                    // expires.
                    let Some(readmission_number) = state.readmission_number.checked_add(1) else {
                        maintenance_error.get_or_insert_with(|| {
                            format!(
                                "ownership recovery readmission number overflow for {}",
                                issue.id
                            )
                        });
                        checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error)
                            .await;
                        continue;
                    };
                    state.readmission_number = readmission_number;
                }
            }
            Err(TrackerError::ResultUnconfirmed) => {
                stats.unconfirmed = stats.unconfirmed.saturating_add(1);
                // Keep the admission fence and operation identity while the
                // result is unknown.  The durable projection written by a
                // later marker reconciliation is keyed by this pair; clearing
                // it here would make a successful manual resolution unable
                // to release the maintenance stop state.
                state.in_flight = true;
                state.stopped = true;
                state.last_failure_class = Some("UNCONFIRMED_OPERATION".into());
                state.stop_basis =
                    Some("ownership recovery operation has no confirmed Tracker marker".into());
                state.repair_condition = Some(
                    "resolve the ownership recovery operation with a server-owned marker".into(),
                );
                maintenance_error.get_or_insert_with(|| {
                    format!(
                        "ownership recovery requires manual resolution for {}",
                        issue.id
                    )
                });
            }
            Err(TrackerError::OwnershipRecoveryPermanent {
                classification,
                message,
            }) => {
                // Permanent input/contract/integrity failures must not be
                // converted into an endless exponential retry.  Preserve a
                // stopped projection with an explicit repair condition and
                // continue evaluating the rest of this page.
                state.in_flight = false;
                state.current_operation_id = None;
                state.stopped = true;
                state.next_attempt_at_millis = None;
                state.last_failure_class = Some(classification);
                state.stop_basis = Some(message.clone());
                state.repair_condition =
                    Some("repair the recorded ownership recovery diagnosis before retrying".into());
                maintenance_error.get_or_insert(message);
            }
            Err(TrackerError::OwnershipRecoveryRetryable(message)) => {
                state.in_flight = false;
                state.current_operation_id = None;
                // A terminal all-NotSent exhaustion belongs to the previous
                // operation. Allocate the next maintenance operation ID only
                // after the persisted backoff, keeping the original failure
                // immutable for idempotent replay.
                let Some(readmission_number) = state.readmission_number.checked_add(1) else {
                    maintenance_error.get_or_insert_with(|| {
                        format!(
                            "ownership recovery readmission number overflow for {}",
                            issue.id
                        )
                    });
                    checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
                    continue;
                };
                state.readmission_number = readmission_number;
                state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                let shift = state.consecutive_failures.saturating_sub(1).min(6);
                let backoff_secs = 30_u64.saturating_mul(1_u64 << shift).min(30 * 60);
                state.next_attempt_at_millis = Some(
                    now_millis.saturating_add(
                        i64::try_from(backoff_secs)
                            .unwrap_or(i64::MAX)
                            .saturating_mul(1_000),
                    ),
                );
                state.last_failure_class = Some("BACKEND_UNAVAILABLE".into());
                maintenance_error.get_or_insert(message);
            }
            Err(TrackerError::Backend(error)) => {
                state.in_flight = false;
                state.current_operation_id = None;
                state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                let shift = state.consecutive_failures.saturating_sub(1).min(6);
                let backoff_secs = 30_u64.saturating_mul(1_u64 << shift).min(30 * 60);
                state.next_attempt_at_millis = Some(
                    now_millis.saturating_add(
                        i64::try_from(backoff_secs)
                            .unwrap_or(i64::MAX)
                            .saturating_mul(1_000),
                    ),
                );
                state.last_failure_class = Some("BACKEND_UNAVAILABLE".into());
                maintenance_error.get_or_insert_with(|| {
                    format!(
                        "ownership recovery backend failure for {}: {error}",
                        issue.id
                    )
                });
            }
            Err(TrackerError::Unavailable) => {
                state.in_flight = false;
                state.current_operation_id = None;
                state.consecutive_failures = state.consecutive_failures.saturating_add(1);
                let shift = state.consecutive_failures.saturating_sub(1).min(6);
                let backoff_secs = 30_u64.saturating_mul(1_u64 << shift).min(30 * 60);
                state.next_attempt_at_millis = Some(
                    now_millis.saturating_add(
                        i64::try_from(backoff_secs)
                            .unwrap_or(i64::MAX)
                            .saturating_mul(1_000),
                    ),
                );
                state.last_failure_class = Some("BACKEND_UNAVAILABLE".into());
                maintenance_error.get_or_insert_with(|| {
                    format!("ownership recovery backend is unavailable for {}", issue.id)
                });
            }
            Err(error) => {
                // InvalidState and other unexpected outcomes are treated as
                // permanent by default.  Retrying an unknown outcome could
                // repeatedly re-submit a mutation whose safety cannot be
                // established.
                state.in_flight = false;
                state.current_operation_id = None;
                state.stopped = true;
                state.next_attempt_at_millis = None;
                state.last_failure_class = Some("PERMANENT_RECOVERY_FAILURE".into());
                state.stop_basis = Some(error.to_string());
                state.repair_condition =
                    Some("inspect and repair the ownership recovery record before retrying".into());
                maintenance_error.get_or_insert_with(|| {
                    format!(
                        "ownership recovery permanently failed for {}: {error}",
                        issue.id
                    )
                });
            }
        }
        if let Err(error) = persist(manager, &state).await {
            maintenance_error.get_or_insert(error);
        }
        checkpoint_cursor(manager, &evaluated_issue_id, &mut maintenance_error).await;
    }

    // Readiness is withheld until a complete physical-UUID sweep has been
    // evaluated.  `reached_end` also covers an exact batch-size final page;
    // relying only on `entries.len() < batch_size` would never complete such
    // a sweep.  Do not persist completion when any checkpoint or diagnosis
    // write failed, because the traversal cannot then be proven durable.
    if !scan_complete
        && page_reached_end
        && maintenance_error.is_none()
        && let Some(manager) = manager
        && let Err(error) = manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .set_ownership_maintenance_scan_complete(true)
    {
        maintenance_error = Some(error.to_string());
    }

    stats.failed = maintenance_error.is_some();
    stats.error = maintenance_error;
    Ok(stats)
}

/// Advance the cursor only after the candidate has been evaluated.  A
/// process stop before this point replays the candidate on the next cycle,
/// while a permanently malformed row still consumes one page slot once it
/// has been diagnosed.
async fn checkpoint_cursor(
    manager: Option<&Arc<Mutex<IssueTrackerManager>>>,
    issue_id: &str,
    maintenance_error: &mut Option<String>,
) {
    if let Some(manager) = manager
        && let Err(error) = manager
            .lock()
            .await
            .storage_mut()
            .repository()
            .set_ownership_maintenance_cursor(issue_id)
    {
        maintenance_error.get_or_insert(error.to_string());
    }
}

fn empty_state(issue_id: String, revision: u64) -> OwnershipRecoveryState {
    OwnershipRecoveryState {
        issue_id,
        expected_revision: revision,
        readmission_number: 0,
        current_operation_id: None,
        in_flight: false,
        consecutive_failures: 0,
        next_attempt_at_millis: None,
        last_failure_class: None,
        stopped: false,
        stop_basis: None,
        repair_condition: None,
        cursor_diagnostic: None,
    }
}

fn is_recovery_candidate(issue: &super::issue_read::LeaseMaintenanceCandidate) -> bool {
    matches!(
        issue.lease_state,
        LeaseState::Expired | LeaseState::Inconsistent
    ) || (matches!(issue.lease_state, LeaseState::None)
        && issue.state == super::IssueState::InProgress)
}

fn recovery_reason(
    issue: &super::issue_read::LeaseMaintenanceCandidate,
) -> OwnershipRecoveryReason {
    match issue.lease_state {
        LeaseState::Expired => OwnershipRecoveryReason::ExpiredClaim,
        LeaseState::None if issue.state == super::IssueState::InProgress => {
            OwnershipRecoveryReason::UnownedInProgress
        }
        LeaseState::Inconsistent => OwnershipRecoveryReason::InconsistentLease,
        // The caller has already applied is_recovery_candidate; this branch is
        // defensive for future candidate additions and keeps the audit reason
        // deterministic rather than deriving it from a mutable wall clock.
        _ => OwnershipRecoveryReason::ExpiredClaim,
    }
}

async fn persist(
    manager: Option<&Arc<Mutex<IssueTrackerManager>>>,
    state: &OwnershipRecoveryState,
) -> Result<(), String> {
    let Some(manager) = manager else {
        return Ok(());
    };
    manager
        .lock()
        .await
        .storage_mut()
        .repository()
        .set_ownership_recovery_state(state)
        .map(|_| ())
        .map_err(|error| error.to_string())
}
