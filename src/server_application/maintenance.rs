use super::*;

impl IssueTrackerApplication {
    pub(crate) async fn run_outbox_cycle(self: &Arc<Self>) -> Result<OutboxCycle, String> {
        let driver = self.driver.as_ref().ok_or("manager is not configured")?;
        let cycle = async {
            let admin_dispatch_recovery_error =
                driver.recover_inflight_admin_dispatches().await.err();
            driver
                .record_phase_health(
                    "admin_dispatch",
                    admin_dispatch_recovery_error.is_none(),
                    admin_dispatch_recovery_error.is_none(),
                    u64::from(admin_dispatch_recovery_error.is_some()),
                    0,
                )
                .await;
            let resolution_recovery = self
                .recover_pending_resolutions(self.maintenance.recovery_batch_size)
                .await;
            driver
                .record_phase_health(
                    "pending_resolution",
                    resolution_recovery.is_ok(),
                    resolution_recovery.is_ok(),
                    u64::from(resolution_recovery.is_err()),
                    0,
                )
                .await;
            let admin_action_recovery = self
                .recover_pending_admin_actions(self.maintenance.recovery_batch_size)
                .await;
            driver
                .record_phase_health(
                    "admin_recovery",
                    admin_action_recovery.is_ok(),
                    admin_action_recovery.is_ok(),
                    u64::from(admin_action_recovery.is_err()),
                    0,
                )
                .await;
            // Recovery, delivery, and ownership scanning share the same
            // orchestration in the static runner and the gRPC server. Keep
            // the server-specific admission phases above, then consume the
            // driver's typed report for the common work.
            let common_cycle = driver
                .run_maintenance_cycle(
                    self.maintenance.recovery_batch_size,
                    self.maintenance.delivery_batch_size,
                    self.ownership_recovery_batch_size,
                )
                .await;
            driver
                .record_phase_health(
                    "ownership_recovery",
                    common_cycle.recovery_error.is_none(),
                    common_cycle.recovery_error.is_none(),
                    u64::from(common_cycle.recovery_error.is_some()),
                    0,
                )
                .await;
            let recovery_error = admin_dispatch_recovery_error
                .or_else(|| resolution_recovery.as_ref().err().cloned())
                .or_else(|| admin_action_recovery.as_ref().err().cloned())
                .or(common_cycle.recovery_error.clone());
            driver
                .record_phase_health(
                    "delivery",
                    common_cycle.delivery_error.is_none(),
                    common_cycle.delivery_error.is_none(),
                    u64::from(common_cycle.delivery_error.is_some()),
                    0,
                )
                .await;
            // Ownership maintenance is best-effort for delivery: a failed
            // scan/checkpoint must not starve the recovery and delivery work
            // above. It is nevertheless a health failure and remains visible
            // until a subsequent complete maintenance cycle succeeds.
            let maintenance_result = common_cycle.ownership;
            driver
                .record_phase_health_with_counts(
                    "ownership_scan",
                    maintenance_result.as_ref().is_ok_and(|stats| !stats.failed),
                    maintenance_result.as_ref().is_ok_and(|stats| !stats.failed),
                    u64::from(maintenance_result.as_ref().is_ok_and(|stats| stats.failed)),
                    0,
                    maintenance_result
                        .as_ref()
                        .map(|stats| stats.skipped as u64)
                        .unwrap_or_default(),
                    maintenance_result
                        .as_ref()
                        .map(|stats| stats.conflicts as u64)
                        .unwrap_or_default(),
                    maintenance_result
                        .as_ref()
                        .map(|stats| stats.scanned as u64)
                        .unwrap_or_default(),
                    maintenance_result
                        .as_ref()
                        .map(|stats| stats.recovered as u64)
                        .unwrap_or_default(),
                    maintenance_result
                        .as_ref()
                        .map(|stats| stats.diagnoses as u64)
                        .unwrap_or_default(),
                    maintenance_result
                        .as_ref()
                        .map(|stats| stats.unconfirmed as u64)
                        .unwrap_or_default(),
                )
                .await;
            let maintenance_healthy = if maintenance_result.as_ref().is_ok_and(|stats| stats.failed)
                || resolution_recovery.is_err()
            {
                false
            } else if let Some(manager) = &self.manager {
                let mut manager_guard = manager.lock().await;
                let repository = manager_guard.storage_mut().repository();
                let scan_complete = repository
                    .ownership_maintenance_scan_complete()
                    .unwrap_or(false);
                !repository
                    .ownership_maintenance_has_unresolved()
                    .unwrap_or(true)
                    && !repository.has_pending_admin_actions().unwrap_or(true)
                    && !repository.has_pending_resolution_requests().unwrap_or(true)
                    && scan_complete
            } else {
                true
            };
            if let Some(manager) = &self.manager {
                let _ = manager
                    .lock()
                    .await
                    .set_ownership_maintenance_health(maintenance_healthy);
            }
            let contract = self
                .manager
                .as_ref()
                .expect("driver requires manager")
                .lock()
                .await
                .get_contract_info();
            let maintenance_error = maintenance_result
                .as_ref()
                .ok()
                .and_then(|stats| stats.error.clone())
                .or_else(|| maintenance_result.as_ref().err().cloned());
            if let Some(error) = recovery_error
                .or(common_cycle.delivery_error)
                .or(maintenance_error)
            {
                return Err(error);
            }
            Ok(OutboxCycle {
                recovered_operations: common_cycle.recovered_operations as u32,
                delivery_attempts: common_cycle.delivery_attempts,
                confirmed_operations: common_cycle.confirmed_operations,
                delivery_limit_reached: common_cycle.delivery_limit_reached,
                contract,
            })
        }
        .await;
        match cycle {
            Ok(cycle) => {
                driver.mark_healthy().await;
                Ok(cycle)
            }
            Err(error) => {
                driver.mark_unhealthy().await;
                Err(error)
            }
        }
    }
}
