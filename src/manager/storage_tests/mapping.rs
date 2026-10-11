use super::support::*;

#[test]
fn initializes_metadata_and_persists_write_counters() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    assert_eq!(storage.manager_sequence(), 0);
    assert_eq!(storage.generation(), 0);
    let (_, stamp) = storage
        .write(|_, stamp| Ok(((), stamp)))
        .expect("write metadata counters");
    assert_eq!(stamp.manager_sequence, 1);
    assert_eq!(storage.generation(), 1);
    drop(storage);
    let storage = ManagerStorage::open(&config).expect("reopen manager storage");
    assert_eq!(storage.manager_sequence(), 1);
    assert_eq!(storage.generation(), 1);
    assert_eq!(
        storage.raw_query_i64("PRAGMA user_version").unwrap(),
        SCHEMA_VERSION
    );
}

#[test]
fn issue_mapping_entity_writes_keep_candidate_index_in_sync() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let mapping = pb::IssueMapping {
        issue_id: "issue-index-test".into(),
        candidate_id: Some("candidate-index-test".into()),
        backend_id: "backend-index-test".into(),
        ..Default::default()
    };
    storage
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            &mapping.issue_id,
            0,
            "issue_tracker.manager.v1.IssueMapping",
            &mapping,
        )
        .expect("store mapping");
    assert_eq!(
        storage
            .raw_query_i64(
                "SELECT COUNT(*) FROM manager_issue_mapping_index
                     WHERE candidate_id='candidate-index-test'
                       AND issue_id='issue-index-test'"
            )
            .expect("read mapping index"),
        1
    );
}

#[test]
fn issue_mapping_refresh_without_candidate_preserves_existing_review_join() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let issue_id = "issue-refresh-test";
    let first = pb::IssueMapping {
        issue_id: issue_id.into(),
        candidate_id: Some("candidate-refresh-test".into()),
        backend_id: "backend-refresh-test".into(),
        ..Default::default()
    };
    storage
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            issue_id,
            0,
            "issue_tracker.manager.v1.IssueMapping",
            &first,
        )
        .expect("store initial mapping");
    let refresh = pb::IssueMapping {
        issue_id: issue_id.into(),
        backend_id: "backend-refresh-test".into(),
        revision: 1,
        ..Default::default()
    };
    storage
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            issue_id,
            1,
            "issue_tracker.manager.v1.IssueMapping",
            &refresh,
        )
        .expect("store refreshed mapping");
    let payload: Vec<u8> = storage
        .connection
        .query_row(
            "SELECT payload FROM manager_entity WHERE entity_kind='issue_mapping' AND entity_id=?1",
            params![issue_id],
            |row| row.get(0),
        )
        .expect("read refreshed mapping");
    let restored = pb::IssueMapping::decode(payload.as_slice()).expect("decode mapping");
    assert_eq!(
        restored.candidate_id.as_deref(),
        Some("candidate-refresh-test")
    );
    assert_eq!(
        storage
            .raw_query_i64(
                "SELECT COUNT(*) FROM manager_issue_mapping_index
                     WHERE candidate_id='candidate-refresh-test'
                       AND issue_id='issue-refresh-test'"
            )
            .expect("read preserved mapping index"),
        1
    );
}

#[test]
fn rebuilds_issue_mapping_index_when_upgrading_a_v29_root() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let mapping = pb::IssueMapping {
        issue_id: "issue-v29-migration".into(),
        candidate_id: Some("candidate-v29-migration".into()),
        backend_id: "backend-v29-migration".into(),
        ..Default::default()
    };
    storage
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            &mapping.issue_id,
            0,
            "issue_tracker.manager.v1.IssueMapping",
            &mapping,
        )
        .expect("store mapping entity");
    storage
        .connection
        .execute_batch(
            "DELETE FROM manager_issue_mapping_index;
                 UPDATE manager_metadata
                    SET value = 'issue-tracker-manager/v14/sqlite-schema-v29'
                  WHERE key = 'schema_fingerprint';
                 UPDATE manager_store_identity
                    SET schema_fingerprint = 'issue-tracker-manager/v14/sqlite-schema-v29'
                  WHERE role = 2;
                 PRAGMA user_version = 29;",
        )
        .expect("simulate a v29 root with an unbuilt index");
    drop(storage);

    let storage = ManagerStorage::open(&config).expect("upgrade v29 root");
    assert_eq!(
        storage
            .raw_query_i64("PRAGMA user_version")
            .expect("read schema version"),
        SCHEMA_VERSION
    );
    assert_eq!(
        storage
            .connection
            .query_row(
                "SELECT value FROM manager_metadata WHERE key='schema_fingerprint'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("read upgraded schema fingerprint"),
        config.expected_schema_fingerprint
    );
    assert_eq!(
        storage
            .raw_query_i64(
                "SELECT COUNT(*) FROM manager_issue_mapping_index
                     WHERE candidate_id='candidate-v29-migration'
                       AND issue_id='issue-v29-migration'"
            )
            .expect("read rebuilt mapping index"),
        1
    );
}

#[test]
fn migration_persists_inherited_candidate_id_in_mapping_payload() {
    let mut config = ManagerConfig::from_proto(&settings()).expect("valid settings");
    let root = tempdir().expect("temporary root");
    config.data_root = root.path().to_path_buf();
    let mut storage = ManagerStorage::open(&config).expect("open manager storage");
    let issue_id = "issue-v29-inherited-candidate";
    let candidate_id = "candidate-v29-inherited-candidate";
    let mapping = pb::IssueMapping {
        issue_id: issue_id.into(),
        candidate_id: Some(candidate_id.into()),
        backend_id: "backend-v29-inherited-candidate".into(),
        ..Default::default()
    };
    storage
        .repository()
        .put_proto(
            crate::manager::repository::EntityKind::IssueMapping,
            issue_id,
            0,
            "issue_tracker.manager.v1.IssueMapping",
            &mapping,
        )
        .expect("store initial mapping");

    // Simulate a v29 refresh that omitted candidate_id while retaining
    // the pre-existing derived index row.
    let refresh = pb::IssueMapping {
        issue_id: issue_id.into(),
        backend_id: mapping.backend_id.clone(),
        revision: 1,
        ..Default::default()
    };
    let refresh_payload = refresh.encode_to_vec();
    let refresh_digest = digest_bytes(&refresh_payload);
    storage
        .connection
        .execute(
            "UPDATE manager_entity
                    SET revision=1, payload=?2, payload_digest=?3
                  WHERE entity_kind='issue_mapping' AND entity_id=?1",
            params![issue_id, refresh_payload, refresh_digest],
        )
        .expect("simulate v29 refresh without Candidate ID");
    storage
        .connection
        .execute_batch(
            "UPDATE manager_metadata
                    SET value = 'issue-tracker-manager/v14/sqlite-schema-v29'
                  WHERE key = 'schema_fingerprint';
                 UPDATE manager_store_identity
                    SET schema_fingerprint = 'issue-tracker-manager/v14/sqlite-schema-v29'
                  WHERE role = 2;
                 PRAGMA user_version = 29;",
        )
        .expect("simulate a v29 root");
    drop(storage);

    let storage = ManagerStorage::open(&config).expect("upgrade v29 root");
    let (payload, digest): (Vec<u8>, String) = storage
        .connection
        .query_row(
            "SELECT payload, payload_digest
                   FROM manager_entity
                  WHERE entity_kind='issue_mapping' AND entity_id=?1",
            params![issue_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read migrated mapping");
    let restored = pb::IssueMapping::decode(payload.as_slice()).expect("decode mapping");
    assert_eq!(restored.candidate_id.as_deref(), Some(candidate_id));
    assert_eq!(digest, digest_bytes(&payload));
    assert_eq!(
        storage
            .raw_query_i64(
                "SELECT COUNT(*) FROM manager_issue_mapping_index
                     WHERE candidate_id='candidate-v29-inherited-candidate'
                       AND issue_id='issue-v29-inherited-candidate'"
            )
            .expect("read rebuilt mapping index"),
        1
    );
}
