use std::path::PathBuf;

/// The role of a store participating in a coordinated manager operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreRole {
    TaskChampion,
    Manager,
}

impl StoreRole {
    pub(crate) fn as_i32(self) -> i32 {
        match self {
            Self::TaskChampion => 1,
            Self::Manager => 2,
        }
    }

    pub(crate) fn from_i32(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::TaskChampion),
            2 => Some(Self::Manager),
            _ => None,
        }
    }
}

/// A stable identity for one of the two stores owned by the manager boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreIdentity {
    pub role: StoreRole,
    pub data_root_id: String,
    pub store_id: String,
    pub schema_fingerprint: String,
    pub generation: u64,
    pub store_pair_id: String,
    pub fencing_epoch: u64,
    pub contract_fingerprint: String,
}

/// The manager-side stamp assigned to a committed write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagerCommit {
    pub manager_sequence: u64,
    pub generation: u64,
}

/// Persistent manager state loaded from the metadata table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManagerState {
    pub data_root_id: String,
    pub manager_store_id: String,
    pub taskchampion_store_id: String,
    pub store_pair_id: String,
    pub contract_fingerprint: String,
    pub schema_fingerprint: String,
    pub manager_sequence: u64,
    pub generation: u64,
    pub fencing_epoch: u64,
    pub root: PathBuf,
}
