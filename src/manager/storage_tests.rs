mod support {
    pub(super) use super::super::*;
    pub(super) use crate::{
        manager::{
            ManagerConfig, ManagerStorage,
            backend_dispatch::{derive_marker_expectation, durable_request_from_manager},
            config::tests::settings,
            gateway::{IdempotencyDecision, ManagerRepository as GatewayRepository},
            operations::digest_bytes,
            outbox,
        },
        proto_types::manager_v1 as pb,
    };
    pub(super) use prost::Message;
    pub(super) use tempfile::tempdir;
}

#[path = "storage_tests/core.rs"]
mod core;
#[path = "storage_tests/mapping.rs"]
mod mapping;
