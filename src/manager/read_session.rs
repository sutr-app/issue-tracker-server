use super::review_candidates::ReviewCandidateReadSnapshot;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

pub(crate) const MAX_PINNED_READ_SESSIONS: usize = 32;
pub(crate) const PINNED_READ_SESSION_TTL: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub(crate) struct PinnedRead {
    pub(crate) snapshot: Arc<ReviewCandidateReadSnapshot>,
    pub(crate) backend_generation: u64,
    pub(crate) composite_generation: u64,
    pub(crate) pinned_at: Instant,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PinKey {
    composite_snapshot_id: String,
    query_fingerprint: String,
}

pub(crate) struct PinnedReadRegistry {
    entries: HashMap<PinKey, PinnedRead>,
    lru: VecDeque<PinKey>,
}

impl PinnedReadRegistry {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    pub(crate) fn pin(
        &mut self,
        composite_snapshot_id: String,
        query_fingerprint: String,
        snapshot: Arc<ReviewCandidateReadSnapshot>,
        backend_generation: u64,
        composite_generation: u64,
    ) {
        self.pin_at(
            composite_snapshot_id,
            query_fingerprint,
            snapshot,
            backend_generation,
            composite_generation,
            Instant::now(),
        );
    }

    pub(crate) fn lookup(
        &mut self,
        composite_snapshot_id: &str,
        query_fingerprint: &str,
    ) -> Option<PinnedRead> {
        self.lookup_at(composite_snapshot_id, query_fingerprint, Instant::now())
    }

    fn pin_at(
        &mut self,
        composite_snapshot_id: String,
        query_fingerprint: String,
        snapshot: Arc<ReviewCandidateReadSnapshot>,
        backend_generation: u64,
        composite_generation: u64,
        pinned_at: Instant,
    ) {
        self.purge_expired(pinned_at);
        let key = PinKey {
            composite_snapshot_id,
            query_fingerprint,
        };
        self.lru.retain(|existing| existing != &key);
        self.entries.insert(
            key.clone(),
            PinnedRead {
                snapshot,
                backend_generation,
                composite_generation,
                pinned_at,
            },
        );
        self.lru.push_back(key);
        while self.entries.len() > MAX_PINNED_READ_SESSIONS {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
    }

    fn lookup_at(
        &mut self,
        composite_snapshot_id: &str,
        query_fingerprint: &str,
        now: Instant,
    ) -> Option<PinnedRead> {
        let key = PinKey {
            composite_snapshot_id: composite_snapshot_id.into(),
            query_fingerprint: query_fingerprint.into(),
        };
        let expired = self
            .entries
            .get(&key)
            .is_some_and(|pin| is_expired(pin.pinned_at, now));
        if expired {
            self.entries.remove(&key);
            self.lru.retain(|existing| existing != &key);
            return None;
        }
        let pin = self.entries.get_mut(&key)?;
        pin.pinned_at = now;
        self.lru.retain(|existing| existing != &key);
        self.lru.push_back(key);
        Some(pin.clone())
    }

    fn purge_expired(&mut self, now: Instant) {
        self.entries
            .retain(|_, pin| !is_expired(pin.pinned_at, now));
        self.lru.retain(|key| self.entries.contains_key(key));
    }
}

impl Default for PinnedReadRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn is_expired(pinned_at: Instant, now: Instant) -> bool {
    now.checked_duration_since(pinned_at)
        .is_some_and(|age| age >= PINNED_READ_SESSION_TTL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::review_candidates::ReviewCandidateReadSnapshot;
    use std::time::{Duration, Instant};

    fn snapshot(generation: u64) -> Arc<ReviewCandidateReadSnapshot> {
        Arc::new(ReviewCandidateReadSnapshot::empty_for_test(generation))
    }

    #[test]
    fn pin_survives_backend_and_manager_generation_advances() {
        let mut registry = PinnedReadRegistry::new();
        registry.pin("composite-1-1".into(), "query".into(), snapshot(1), 11, 101);

        let pin = registry
            .lookup("composite-1-1", "query")
            .expect("pin survives generation changes");
        assert_eq!(pin.backend_generation, 11);
        assert_eq!(pin.composite_generation, 101);
        assert_eq!(pin.snapshot.generation, 1);
    }

    #[test]
    fn expired_pin_is_a_miss() {
        let mut registry = PinnedReadRegistry::new();
        let pinned_at = Instant::now() - PINNED_READ_SESSION_TTL - Duration::from_secs(1);
        registry.pin_at(
            "composite-expired".into(),
            "query".into(),
            snapshot(1),
            11,
            101,
            pinned_at,
        );

        assert!(
            registry
                .lookup_at("composite-expired", "query", Instant::now())
                .is_none()
        );
    }

    #[test]
    fn unknown_snapshot_id_is_a_miss() {
        let mut registry = PinnedReadRegistry::new();
        registry.pin(
            "composite-known".into(),
            "query".into(),
            snapshot(1),
            11,
            101,
        );

        assert!(registry.lookup("composite-unknown", "query").is_none());
    }

    #[test]
    fn lru_evicts_only_the_oldest_entry_after_thirty_two_plus_one_pins() {
        let mut registry = PinnedReadRegistry::new();
        for index in 0..MAX_PINNED_READ_SESSIONS {
            registry.pin(
                format!("composite-{index}"),
                "query".into(),
                snapshot(index as u64),
                index as u64,
                index as u64,
            );
        }
        registry.pin("composite-new".into(), "query".into(), snapshot(99), 99, 99);

        assert!(registry.lookup("composite-0", "query").is_none());
        for index in 1..MAX_PINNED_READ_SESSIONS {
            assert!(
                registry
                    .lookup(&format!("composite-{index}"), "query")
                    .is_some()
            );
        }
        assert!(registry.lookup("composite-new", "query").is_some());
    }

    #[test]
    fn same_snapshot_id_with_a_different_query_fingerprint_is_a_miss() {
        let mut registry = PinnedReadRegistry::new();
        registry.pin("composite-1".into(), "query-a".into(), snapshot(1), 11, 101);

        assert!(registry.lookup("composite-1", "query-b").is_none());
    }

    #[test]
    fn pin_hit_refreshes_the_sliding_ttl() {
        let mut registry = PinnedReadRegistry::new();
        let pinned_at = Instant::now();
        registry.pin_at(
            "composite-sliding".into(),
            "query".into(),
            snapshot(1),
            11,
            101,
            pinned_at,
        );

        let just_before_expiry = pinned_at + PINNED_READ_SESSION_TTL - Duration::from_secs(1);
        assert!(
            registry
                .lookup_at("composite-sliding", "query", just_before_expiry)
                .is_some()
        );
        assert!(
            registry
                .lookup_at(
                    "composite-sliding",
                    "query",
                    just_before_expiry + PINNED_READ_SESSION_TTL - Duration::from_secs(1),
                )
                .is_some()
        );
        assert!(
            registry
                .lookup_at(
                    "composite-sliding",
                    "query",
                    just_before_expiry
                        + PINNED_READ_SESSION_TTL
                        + PINNED_READ_SESSION_TTL
                        + Duration::from_secs(1),
                )
                .is_none()
        );
    }
}
