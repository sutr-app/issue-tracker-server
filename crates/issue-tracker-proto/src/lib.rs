//! Canonical protobuf types and tonic bindings for issue-tracker services.

mod generated {
    pub mod manager {
        pub mod v1 {
            pub use super::super::manager_v1::*;
        }
    }

    #[allow(dead_code, clippy::enum_variant_names, clippy::large_enum_variant)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/issue_tracker.v1.rs"));
    }

    #[allow(dead_code, clippy::enum_variant_names, clippy::large_enum_variant)]
    pub mod manager_v1 {
        include!(concat!(env!("OUT_DIR"), "/issue_tracker.manager.v1.rs"));
    }
}

pub use generated::{manager_v1, v1};

/// Compatibility hierarchy used by generated imports between packages.
pub mod issue_tracker {
    pub mod v1 {
        pub use crate::v1::*;
    }
    pub mod manager {
        pub mod v1 {
            pub use crate::manager_v1::*;
        }
    }
    pub mod rpc {
        #[allow(clippy::large_enum_variant)]
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/issue_tracker.rpc.v1.rs"));
        }
    }
}

pub use issue_tracker::rpc::v1 as rpc_v1;

pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));

/// Descriptor set restricted to `IssueGateway` and the protobuf files it imports.
pub const GATEWAY_WORKER_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/gateway-worker-descriptor.bin"));

/// Descriptor set restricted to primary trusted-consumer services and their protobuf imports.
pub const PRIMARY_TRUSTED_CONSUMER_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(
    env!("OUT_DIR"),
    "/primary-trusted-consumer-descriptor.bin"
));

pub mod manager {
    pub mod v1 {
        pub use crate::manager_v1::*;
    }
}

#[cfg(test)]
mod tests {
    use super::{GATEWAY_WORKER_FILE_DESCRIPTOR_SET, PRIMARY_TRUSTED_CONSUMER_FILE_DESCRIPTOR_SET};
    use prost::Message;

    #[test]
    fn worker_descriptor_is_limited_to_issue_gateway_and_its_imports() {
        let descriptor = prost_types::FileDescriptorSet::decode(GATEWAY_WORKER_FILE_DESCRIPTOR_SET)
            .expect("worker descriptor must decode");
        let names = descriptor
            .file
            .iter()
            .filter_map(|file| file.name.as_deref())
            .collect::<std::collections::HashSet<_>>();

        assert!(names.contains("rpc/v1/issue_gateway.proto"));
        assert!(names.contains("manager/v1/issue_tracker_manager_common.proto"));
        assert!(names.contains("v1/issue_tracker_common.proto"));
        assert!(!names.contains("rpc/v1/manager_consumer_service.proto"));
    }

    #[test]
    fn primary_descriptor_contains_only_registered_trusted_consumer_services() {
        let descriptor =
            prost_types::FileDescriptorSet::decode(PRIMARY_TRUSTED_CONSUMER_FILE_DESCRIPTOR_SET)
                .expect("primary descriptor must decode");
        let services = descriptor
            .file
            .iter()
            .flat_map(|file| file.service.iter())
            .filter_map(|service| service.name.as_deref())
            .collect::<std::collections::HashSet<_>>();

        assert_eq!(
            services,
            std::collections::HashSet::from([
                "ManagerConsumerService",
                "IssueExtract",
                "ManagerTrustedReadService",
                "ManagerInfoService",
                "OutboxMaintenanceService",
            ])
        );
    }
}
