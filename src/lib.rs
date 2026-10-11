//! TaskChampion-backed generic Issue tracker runner.
//!
//! This crate root is intentionally a thin public facade. Domain types, errors,
//! storage operations, generated protocol types, and server wiring live in their
//! dedicated modules.

pub mod app;
pub mod error;
pub mod extract;
pub mod gateway;
pub mod manager;
pub mod model;
pub mod proto_schema;
mod proto_types;
pub mod server;
pub mod tracker;
pub mod v1;

pub use app::{AppModule, AppResources};
pub use error::TrackerError;
pub use manager::{IssueTrackerManager, ManagerConfig, ManagerConfigError};
pub use model::{ExportBundle, Issue, IssueRelation, IssueState, RelationKind};
pub use tracker::{Tracker, config::RunnerConfig, outbox_driver::OutboxDriver};

#[cfg(test)]
mod public_api_compatibility_tests {
    use super::{
        IssueState, RelationKind, Tracker,
        error::TrackerError as ErrorModuleTrackerError,
        model::{
            ExportBundle as ModelExportBundle, Issue as ModelIssue,
            IssueRelation as ModelIssueRelation, IssueState as ModelIssueState,
            RelationKind as ModelRelationKind,
        },
        tracker::{
            ExportBundle as TrackerExportBundle, Issue as TrackerIssue,
            IssueRelation as TrackerIssueRelation, IssueState as TrackerIssueState,
            RelationKind as TrackerRelationKind, TrackerError as TrackerModuleTrackerError,
        },
        v1,
    };

    #[test]
    fn facade_keeps_domain_types_and_tracker_entry_points_public() {
        assert_eq!(IssueState::Open, IssueState::Open);
        assert_eq!(RelationKind::Related, RelationKind::Related);
        let _: fn() -> &'static str = Tracker::contract_version;
        let _: fn() -> String = Tracker::contract_fingerprint;
        let _: fn() -> String = Tracker::schema_fingerprint;
        let _: v1::Issue = Default::default();
    }

    #[test]
    fn domain_types_and_errors_are_available_through_new_and_legacy_modules() {
        let _: Option<fn(ModelIssueState)> = None::<fn(TrackerIssueState)>;
        let _: Option<fn(ModelRelationKind)> = None::<fn(TrackerRelationKind)>;
        let _: Option<fn(ErrorModuleTrackerError)> = None::<fn(TrackerModuleTrackerError)>;
        let _: Option<fn(ModelIssue)> = None::<fn(TrackerIssue)>;
        let _: Option<fn(ModelIssueRelation)> = None::<fn(TrackerIssueRelation)>;
        let _: Option<fn(ModelExportBundle)> = None::<fn(TrackerExportBundle)>;
    }
}
