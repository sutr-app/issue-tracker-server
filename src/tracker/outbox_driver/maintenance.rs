use super::*;

impl OutboxDriver {
    /// Start the static-runner resident task. The task owns no root lock of
    /// its own; it only uses the already-open Tracker/Manager pair and exits
    /// before the caller releases those resources.
    pub fn spawn_resident_maintenance(
        self: &Arc<Self>,
        interval: Duration,
        batch_size: usize,
    ) -> Result<MaintenanceHandle, String> {
        if interval.is_zero() || batch_size == 0 || batch_size > 100 {
            return Err("invalid resident ownership maintenance settings".into());
        }
        // Resolution recovery is an application-layer responsibility injected
        // by the composition root.  Fail before spawning a resident task if
        // the static runner was assembled without that handler; otherwise a
        // pending resolution would only be discovered asynchronously on the
        // first maintenance pass.
        let handler_configured = self
            .resolution_recovery_handler
            .lock()
            .map_err(|_| "resolution recovery handler registry is poisoned".to_owned())?
            .is_some();
        if !handler_configured {
            return Err("resolution recovery handler is not configured".into());
        }
        let (stop, mut stop_rx) = oneshot::channel();
        // Do not let a detached task keep the persistent roots alive after a
        // runner drops its resources without awaiting shutdown. Each cycle
        // upgrades this weak reference only for the duration of its work.
        let driver_weak = Arc::downgrade(self);
        let join = tokio::spawn(async move {
            let Some(driver) = driver_weak.upgrade() else {
                return;
            };
            // A previous process may have persisted an administrative send
            // permit but stopped before the Tracker result was finalized.
            // Close that permit as SENT_UNKNOWN before any new delivery is
            // considered; otherwise an expired lease could be reclaimed and
            // the mutation sent twice.
            let admin_recovery_failed = driver.recover_inflight_admin_dispatches().await.is_err();
            driver
                .record_phase_health(
                    "admin_dispatch",
                    !admin_recovery_failed,
                    !admin_recovery_failed,
                    u64::from(admin_recovery_failed),
                    0,
                )
                .await;
            let capability_recovery_failed = driver
                .recover_inflight_capability_dispatches(batch_size)
                .await
                .is_err();
            driver
                .record_phase_health(
                    "ownership_recovery",
                    !capability_recovery_failed,
                    !capability_recovery_failed,
                    u64::from(capability_recovery_failed),
                    0,
                )
                .await;
            let resolution_recovery_failed = driver
                .recover_pending_resolutions(batch_size)
                .await
                .is_err();
            driver
                .record_phase_health(
                    "pending_resolution",
                    !resolution_recovery_failed,
                    !resolution_recovery_failed,
                    u64::from(resolution_recovery_failed),
                    0,
                )
                .await;
            if admin_recovery_failed || capability_recovery_failed || resolution_recovery_failed {
                driver.mark_unhealthy().await;
            }
            drop(driver);
            let mut ticker = time::interval(interval);
            // Do not make an immediate tick a special case: the first tick
            // performs startup recovery, then the configured cadence applies.
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = ticker.tick() => {
                        let Some(driver) = driver_weak.upgrade() else {
                            break;
                        };
                        // Recovery and delivery run before the ownership scan
                        // so a malformed lease cannot starve unrelated
                        // durable operations. Each phase is isolated and the
                        // next tick retries only the failed phase.
                        let resolution_failed =
                            driver.recover_pending_resolutions(batch_size).await.is_err();
                        driver
                            .record_phase_health(
                                "pending_resolution",
                                !resolution_failed,
                                !resolution_failed,
                                u64::from(resolution_failed),
                                0,
                            )
                            .await;
                        if resolution_failed {
                            driver.mark_unhealthy().await;
                        }
                        let admin_dispatch_recovery_failed =
                            driver.recover_inflight_admin_dispatches().await.is_err();
                        if admin_dispatch_recovery_failed {
                            driver.mark_unhealthy().await;
                        }
                        driver
                            .record_phase_health(
                                "admin_dispatch",
                                !admin_dispatch_recovery_failed,
                                !admin_dispatch_recovery_failed,
                                u64::from(admin_dispatch_recovery_failed),
                                0,
                            )
                            .await;
                        let cycle = driver
                            .run_maintenance_cycle(batch_size, batch_size, batch_size)
                            .await;
                        let recovery_failed = cycle.recovery_error.is_some();
                        let delivery_failed = cycle.delivery_error.is_some();
                        driver
                            .record_phase_health(
                                "ownership_recovery",
                                !recovery_failed,
                                !recovery_failed,
                                u64::from(recovery_failed),
                                0,
                            )
                            .await;
                        driver
                            .record_phase_health(
                                "delivery",
                                !delivery_failed,
                                !delivery_failed,
                                u64::from(delivery_failed),
                                0,
                            )
                            .await;
                        let maintenance = cycle.ownership;
                        driver
                            .record_phase_health_with_counts(
                                "ownership_scan",
                                maintenance.as_ref().is_ok_and(|stats| !stats.failed),
                                maintenance.as_ref().is_ok_and(|stats| !stats.failed),
                                u64::from(
                                    maintenance.as_ref().is_ok_and(|stats| stats.failed),
                                ),
                                0,
                                maintenance
                                    .as_ref()
                                    .map(|stats| stats.skipped as u64)
                                    .unwrap_or_default(),
                                maintenance
                                    .as_ref()
                                    .map(|stats| stats.conflicts as u64)
                                    .unwrap_or_default(),
                                maintenance
                                    .as_ref()
                                    .map(|stats| stats.scanned as u64)
                                    .unwrap_or_default(),
                                maintenance
                                    .as_ref()
                                    .map(|stats| stats.recovered as u64)
                                    .unwrap_or_default(),
                                maintenance
                                    .as_ref()
                                    .map(|stats| stats.diagnoses as u64)
                                    .unwrap_or_default(),
                                maintenance
                                    .as_ref()
                                    .map(|stats| stats.unconfirmed as u64)
                                    .unwrap_or_default(),
                            )
                            .await;
                        let maintenance_failed = ownership_maintenance_failed(&maintenance);
                        let healthy = if maintenance_failed
                            || resolution_failed
                            || admin_dispatch_recovery_failed
                        {
                            false
                        } else {
                            let mut manager_guard = driver
                                .manager
                                .lock()
                                .await;
                            let repository = manager_guard.storage_mut().repository();
                            let scan_complete = repository
                                .ownership_maintenance_scan_complete()
                                .unwrap_or(false);
                            repository
                                .ownership_maintenance_has_unresolved()
                                .and_then(|unresolved| {
                                    repository
                                        .has_pending_admin_actions()
                                        .and_then(|admin_pending| {
                                            repository
                                                .has_pending_resolution_requests()
                                                .map(|pending| {
                                                    scan_complete
                                                        && !unresolved
                                                        && !admin_pending
                                                        && !pending
                                                })
                                        })
                                })
                                .unwrap_or(false)
                        };
                        let _ = driver.manager.lock().await.set_ownership_maintenance_health(healthy);
                        if healthy
                            && !resolution_failed
                            && !recovery_failed
                            && !delivery_failed
                            && !admin_dispatch_recovery_failed
                        {
                            driver.mark_healthy().await;
                        } else {
                            driver.mark_unhealthy().await;
                        }
                        drop(driver);
                    }
                }
            }
        });
        Ok(MaintenanceHandle {
            stop: Some(stop),
            join: Some(join),
        })
    }

    pub(crate) async fn recover_inflight_admin_dispatches(&self) -> Result<u32, String> {
        // Take the Manager lock before sampling the process-local registry.
        // Dispatchers register their owner before acquiring this same lock to
        // mark the attempt SENDING.  This ordering makes the registry sample
        // and the durable state transition one exclusion boundary: a live
        // send can no longer be mistaken for an abandoned send between the
        // two awaits.
        let mut manager = self.manager.lock().await;
        let protected = self
            .active_admin_dispatches
            .lock()
            .map_err(|_| "active admin dispatch registry is poisoned".to_owned())?
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        manager
            .storage_mut()
            .repository()
            .recover_inflight_admin_dispatches(Self::now_millis(), &protected)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default()
    }

    /// Reconcile unconfirmed markers, then process a bounded number of due
    /// entries before the runner advertises delivery availability. An
    /// acknowledged outbox entry is never due and therefore is never sent
    /// again during recovery.
    pub(crate) async fn recover(&self, budget: usize) -> Result<usize, String> {
        #[cfg(test)]
        if self.fail_initial_recovery.swap(false, Ordering::SeqCst) {
            return Err("test recovery failure".into());
        }
        let mut first_error = None;
        let mut recovered = 0usize;
        match self.recover_inflight_capability_dispatches(budget).await {
            Ok(count) => recovered = recovered.saturating_add(count as usize),
            Err(error) => first_error = Some(error),
        }
        match self.recover_unconfirmed(budget).await {
            Ok(count) => recovered = recovered.saturating_add(count),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(recovered)
        }
    }

    /// Run the common durable phases after caller-specific recovery has
    /// completed.  Recovery, delivery, and ownership scanning are deliberately
    /// independent: an error in one phase is reported in the typed result but
    /// does not prevent later phases from running in the same cycle.
    pub(crate) async fn run_maintenance_cycle(
        &self,
        recovery_budget: usize,
        delivery_budget: usize,
        ownership_budget: usize,
    ) -> MaintenanceCycleReport {
        let recovery = self.recover(recovery_budget).await;
        let (recovered_operations, recovery_error) = match recovery {
            Ok(count) => (count, None),
            Err(error) => (0, Some(error)),
        };

        let mut delivery_attempts = 0_u32;
        let mut confirmed_operations = 0_u32;
        let mut delivery_error = None;
        for _ in 0..delivery_budget {
            match self.process_one().await {
                Ok(Some(result)) => {
                    delivery_attempts = delivery_attempts.saturating_add(1);
                    if result.state
                        == crate::proto_types::manager_v1::DurableOperationState::Committed
                    {
                        confirmed_operations = confirmed_operations.saturating_add(1);
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    delivery_error = Some(error);
                    break;
                }
            }
        }

        let ownership = super::ownership_maintenance::run_with_recovery_stats(
            &self.tracker,
            Some(&self.manager),
            ownership_budget,
            &|issue_id, revision, operation_id, state, reason| {
                self.recover_ownership_via_outbox(issue_id, revision, operation_id, state, reason)
            },
        )
        .await;

        MaintenanceCycleReport {
            recovered_operations,
            delivery_attempts,
            confirmed_operations,
            delivery_limit_reached: delivery_attempts == delivery_budget as u32,
            recovery_error,
            delivery_error,
            ownership,
        }
    }
}
