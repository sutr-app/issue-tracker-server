use prost::Message;

fn configured_prost_build() -> prost_build::Config {
    let mut config = prost_build::Config::new();
    config.type_attribute(
        ".",
        "#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]",
    );
    // Settings are loaded from TOML as well as protobuf.  Newly added
    // scalar settings must remain backwards compatible with older files that
    // omit the field; prost's generated serde derives otherwise treat the
    // field as required during deserialization.
    config.field_attribute(
        "issue_tracker.v1.IssueTrackerRunnerSettings.ownership_recovery_batch_size",
        "#[serde(default)]",
    );
    config
}

fn descriptor_closure(
    full: &prost_types::FileDescriptorSet,
    roots: &[&str],
) -> prost_types::FileDescriptorSet {
    use std::collections::{BTreeSet, HashMap};

    let files = full
        .file
        .iter()
        .cloned()
        .map(|file| {
            let name = file
                .name
                .clone()
                .expect("generated descriptor files must have names");
            (name, file)
        })
        .collect::<HashMap<_, _>>();
    let mut pending = roots
        .iter()
        .map(|root| (*root).to_owned())
        .collect::<Vec<_>>();
    let mut selected = BTreeSet::new();

    while let Some(name) = pending.pop() {
        if !selected.insert(name.clone()) {
            continue;
        }
        let file = files
            .get(&name)
            .unwrap_or_else(|| panic!("descriptor root closure is missing {name}"));
        pending.extend(file.dependency.iter().cloned());
    }

    prost_types::FileDescriptorSet {
        file: selected
            .into_iter()
            .map(|name| files.get(&name).expect("selected descriptor file").clone())
            .collect(),
    }
}

fn main() {
    let v1_protos = [
        "../../protobuf/v1/issue_tracker_common.proto",
        "../../protobuf/v1/issue_tracker_settings.proto",
        "../../protobuf/v1/issue_tracker_methods.proto",
    ];
    let manager_protos = [
        "../../protobuf/manager/v1/issue_tracker_manager_common.proto",
        "../../protobuf/manager/v1/issue_tracker_manager_settings.proto",
        "../../protobuf/manager/v1/issue_tracker_manager_methods.proto",
    ];
    let rpc_protos = [
        "../../protobuf/rpc/v1/issue_tracker_service.proto",
        "../../protobuf/rpc/v1/tracker_info_service.proto",
        "../../protobuf/rpc/v1/issue_service.proto",
        "../../protobuf/rpc/v1/claim_service.proto",
        "../../protobuf/rpc/v1/relation_service.proto",
        "../../protobuf/rpc/v1/activity_service.proto",
        "../../protobuf/rpc/v1/artifact_service.proto",
        "../../protobuf/rpc/v1/operation_service.proto",
        "../../protobuf/rpc/v1/manager_info_service.proto",
        "../../protobuf/rpc/v1/candidate_service.proto",
        "../../protobuf/rpc/v1/extraction_run_service.proto",
        "../../protobuf/rpc/v1/manager_operation_service.proto",
        "../../protobuf/rpc/v1/manager_consumer_service.proto",
        "../../protobuf/rpc/v1/manager_trusted_read_service.proto",
        "../../protobuf/rpc/v1/canonical_issue_service.proto",
        "../../protobuf/rpc/v1/outbox_maintenance_service.proto",
        "../../protobuf/rpc/v1/gateway_control_service.proto",
        "../../protobuf/rpc/v1/issue_gateway.proto",
        "../../protobuf/rpc/v1/issue_extract.proto",
    ];
    let protos = [
        v1_protos.as_slice(),
        manager_protos.as_slice(),
        rpc_protos.as_slice(),
    ]
    .concat();
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    let descriptor_path =
        std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("descriptor.bin");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(&descriptor_path)
        .compile_with_config(configured_prost_build(), &protos, &["../../protobuf"])
        .expect("issue tracker protobuf schemas must compile");

    let worker_descriptor_path = descriptor_path.with_file_name("gateway-worker-descriptor.bin");
    let full = prost_types::FileDescriptorSet::decode(
        std::fs::read(&descriptor_path)
            .expect("generated descriptor must be readable")
            .as_slice(),
    )
    .expect("generated descriptor must decode");
    let worker = descriptor_closure(&full, &["rpc/v1/issue_gateway.proto"]);
    std::fs::write(
        &worker_descriptor_path,
        prost::Message::encode_to_vec(&worker),
    )
    .expect("worker descriptor must be writable");
    let primary_trusted_consumer_descriptor_path =
        descriptor_path.with_file_name("primary-trusted-consumer-descriptor.bin");
    let primary_trusted_consumer = descriptor_closure(
        &full,
        &[
            "rpc/v1/manager_consumer_service.proto",
            "rpc/v1/manager_trusted_read_service.proto",
            "rpc/v1/issue_extract.proto",
            "rpc/v1/manager_info_service.proto",
            "rpc/v1/outbox_maintenance_service.proto",
        ],
    );
    std::fs::write(
        primary_trusted_consumer_descriptor_path,
        prost::Message::encode_to_vec(&primary_trusted_consumer),
    )
    .expect("primary trusted-consumer descriptor must be writable");
}
