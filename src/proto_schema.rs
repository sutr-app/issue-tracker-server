use command_utils::protobuf::resolve::resolve_proto_imports;
use std::collections::HashMap;
use std::sync::LazyLock;

mod catalog;
mod resolver;

#[allow(dead_code, unused_imports)]
pub(crate) use catalog::{MANAGER_METHOD_SPECS, METHOD_SPECS, MethodSpec};

#[allow(dead_code)]
pub(crate) fn method_description(key: &str) -> &'static str {
    METHOD_SPECS
        .iter()
        .chain(MANAGER_METHOD_SPECS)
        .find(|spec| spec.key == key)
        .map_or("Unknown IssueTrackerRunner method.", |spec| {
            spec.description
        })
}

const COMMON_PROTO: &str = include_str!("../protobuf/v1/issue_tracker_common.proto");
const SETTINGS_PROTO: &str = include_str!("../protobuf/v1/issue_tracker_settings.proto");
const METHODS_PROTO: &str = include_str!("../protobuf/v1/issue_tracker_methods.proto");
const MANAGER_COMMON_PROTO: &str =
    include_str!("../protobuf/manager/v1/issue_tracker_manager_common.proto");
const MANAGER_SETTINGS_PROTO: &str =
    include_str!("../protobuf/manager/v1/issue_tracker_manager_settings.proto");
const MANAGER_METHODS_PROTO: &str =
    include_str!("../protobuf/manager/v1/issue_tracker_manager_methods.proto");
const COMMON_IMPORT_PATH: &str = "v1/issue_tracker_common.proto";
const MANAGER_COMMON_IMPORT_PATH: &str = "manager/v1/issue_tracker_manager_common.proto";

static RESOLVED_SETTINGS_PROTO: LazyLock<String> =
    LazyLock::new(|| resolve_settings_proto().expect("v1 settings proto imports must resolve"));

static RESOLVED_METHOD_PROTOS: LazyLock<HashMap<&'static str, (String, String)>> =
    LazyLock::new(|| {
        METHOD_SPECS
            .iter()
            .map(|spec| {
                let args = resolver::resolve_primary_message(
                    METHODS_PROTO,
                    spec.args,
                    &[(COMMON_IMPORT_PATH, COMMON_PROTO)],
                    COMMON_IMPORT_PATH,
                )
                .unwrap_or_else(|error| panic!("{} args schema: {error}", spec.key));
                let result = resolver::resolve_primary_message(
                    METHODS_PROTO,
                    spec.result,
                    &[(COMMON_IMPORT_PATH, COMMON_PROTO)],
                    COMMON_IMPORT_PATH,
                )
                .unwrap_or_else(|error| panic!("{} result schema: {error}", spec.key));
                (spec.key, (args, result))
            })
            .collect()
    });

static RESOLVED_MANAGER_SETTINGS_PROTO: LazyLock<String> = LazyLock::new(|| {
    resolve_manager_imports(MANAGER_SETTINGS_PROTO)
        .expect("manager settings proto imports must resolve")
});

static RESOLVED_MANAGER_METHOD_PROTOS: LazyLock<HashMap<&'static str, (String, String)>> =
    LazyLock::new(|| {
        MANAGER_METHOD_SPECS
            .iter()
            .map(|spec| {
                let args = resolve_manager_primary_message(MANAGER_METHODS_PROTO, spec.args)
                    .unwrap_or_else(|error| panic!("{} args schema: {error}", spec.key));
                let result = resolve_manager_primary_message(MANAGER_METHODS_PROTO, spec.result)
                    .unwrap_or_else(|error| panic!("{} result schema: {error}", spec.key));
                (spec.key, (args, result))
            })
            .collect()
    });

fn resolve_with_common(proto: &str) -> Result<String, String> {
    resolve_proto_imports(proto, &[(COMMON_IMPORT_PATH, COMMON_PROTO)])
}

fn resolve_settings_proto() -> Result<String, String> {
    let settings = SETTINGS_PROTO
        .replace(
            "import \"manager/v1/issue_tracker_manager_settings.proto\";",
            "",
        )
        .replace(
            "issue_tracker.manager.v1.IssueTrackerManagerSettings",
            "IssueTrackerManagerSettings",
        );
    let settings = format!("{settings}\n{SETTINGS_MANAGER_TYPES}");
    resolve_with_common(&settings)
}

// Only the manager settings graph is embedded in the runner settings schema.
// It deliberately reuses v1 TimeSpan and has manager-prefixed enum names so
// that the published single-file proto has no package or enum collisions.
const SETTINGS_MANAGER_TYPES: &str = r#"
message IssueTrackerManagerSettings {
  string data_root = 1;
  string expected_contract_fingerprint = 2;
  string expected_schema_fingerprint = 3;
  uint32 default_page_size = 4;
  uint32 maximum_page_size = 5;
  uint64 maximum_payload_bytes = 6;
  uint64 maximum_repeated_item_count = 7;
  TimeSpan operation_lease = 8;
  TimeSpan outbox_lease = 9;
  ManagerResourceLimits resource_limits = 10;
  RetryBudget retry_budget = 11;
  repeated StoreIdentity store_identities = 12;
  ManagerSecretFilterMode secret_filter_mode = 13;
  TimeSpan root_operation_history_max_window = 14;
  optional bool operation_history_v1_compat_enabled = 15;
}
enum ManagerSecretFilterMode {
  MANAGER_SECRET_FILTER_MODE_UNSPECIFIED = 0;
  MANAGER_SECRET_FILTER_MODE_DISABLED = 1;
  MANAGER_SECRET_FILTER_MODE_FAIL_CLOSED = 2;
}
message ManagerResourceLimits {
  uint64 max_string_bytes = 1;
  uint64 max_body_bytes = 2;
  uint64 max_uri_bytes = 3;
  uint64 max_repeated_item_count = 4;
  uint64 max_page_size = 5;
}
message RetryBudget {
  uint32 max_attempts = 1;
  TimeSpan base_backoff = 2;
  TimeSpan max_backoff = 3;
}
enum StoreRole {
  STORE_ROLE_UNSPECIFIED = 0;
  STORE_ROLE_TASKCHAMPION = 1;
  STORE_ROLE_MANAGER = 2;
}
message StoreIdentity {
  StoreRole role = 1;
  string data_root_id = 2;
  string store_id = 3;
  string schema_fingerprint = 4;
  uint64 generation = 5;
  string store_pair_id = 6;
  uint64 fencing_epoch = 7;
  string contract_fingerprint = 8;
}
"#;

fn resolve_manager_primary_message(proto: &str, message_name: &str) -> Result<String, String> {
    let common = manager_common_import_proto();
    resolver::resolve_primary_message(
        proto,
        message_name,
        &[
            (MANAGER_COMMON_IMPORT_PATH, MANAGER_COMMON_PROTO),
            (COMMON_IMPORT_PATH, &common),
        ],
        MANAGER_COMMON_IMPORT_PATH,
    )
}

fn resolve_manager_imports(proto: &str) -> Result<String, String> {
    let common = manager_common_import_proto();
    resolve_proto_imports(
        proto,
        &[
            (MANAGER_COMMON_IMPORT_PATH, MANAGER_COMMON_PROTO),
            (COMMON_IMPORT_PATH, &common),
        ],
    )
}

fn manager_common_import_proto() -> String {
    let mut common = COMMON_PROTO.to_owned();
    for message_name in [
        "UtcInstant",
        "TimeSpan",
        "MetadataEntry",
        "StableTraversal",
        "PageSnapshot",
        "PageRequest",
        "PageInfo",
        "PageTokenExpired",
        "RevisionConflict",
    ] {
        if let Some((start, open)) = resolver::find_top_level_message(&common, message_name)
            && let Some(end) = resolver::find_block_end(&common, open)
        {
            common.replace_range(start..end, "");
        }
    }
    if let Some((start, open)) = resolver::find_top_level_enum(&common, "HealthStatus")
        && let Some(end) = resolver::find_block_end(&common, open)
    {
        common.replace_range(start..end, "");
    }
    common
}

pub fn resolved_v1_settings_proto() -> &'static str {
    RESOLVED_SETTINGS_PROTO.as_str()
}

pub fn resolved_v1_method_protos() -> &'static HashMap<&'static str, (String, String)> {
    &RESOLVED_METHOD_PROTOS
}

pub fn resolved_manager_settings_proto() -> &'static str {
    RESOLVED_MANAGER_SETTINGS_PROTO.as_str()
}

pub fn resolved_manager_method_protos() -> &'static HashMap<&'static str, (String, String)> {
    &RESOLVED_MANAGER_METHOD_PROTOS
}

#[cfg(test)]
mod tests;
