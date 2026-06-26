#![allow(clippy::panic)]
// These tests use panic in impossible match arms to preserve precise failure messages.

//! Behavior tests for the checkpoint lifecycle: creation, publication
//! races, retention, fork materialization, and corruption rejection.

use super::build::{
    build_manifest_tables, build_manifest_tables_from_rows, MetadataTableSegmentation,
};
use super::cache::{MetadataTableCache, MetadataTableCacheConfig};
use super::create::{
    build_namespace_manifest_from_metadata_state, checkpoint_record_by_id, create_checkpoint,
    create_checkpoint_with_policy, load_checkpoint_projection_metadata_state,
    ManifestMetadataSource,
};
use super::error::ManifestLoadError;
use super::load::{
    head_from_manifest, load_manifest_materialization_for_inspection,
    load_manifest_metadata_state_for_inspection_from_manifest,
};
use super::publish::{
    publish_current_manifest_id, write_namespace_manifest, ManifestPublicationOutcome,
};
use super::retention::advance_retention_floor;
use super::row::{manifest_rows_for_family, metadata_states_equivalent};
use super::runs::{
    flatten_manifest_tables, runs_from_metadata_files, MetadataLsmPolicy, MetadataRunManifest,
    CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL, CHECKPOINT_TABLE_FAMILIES,
    DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT, MAX_CHECKPOINT_L0_RUNS,
};
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::path::write::ops::{move_path, put_file_bytes, write_file_bytes};
use crate::MutationContext;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::manifest::{
    encode_metadata_sst_envelope_zstd, encode_namespace_manifest_json, MetadataFileRef,
    MetadataPage, MetadataRow, MetadataSegmentKey, MetadataSstEnvelope, MetadataSstPayload,
    MetadataTableFamily as ApiMetadataTableFamily, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loonfs_api::{
    validate_checkpoint_id, ChangeSeq, CommitId, InodeId, ManifestId, NamespaceId, PutBehavior,
};
use loonfs_objectstore::fs::LocalFsStore;
use loonfs_objectstore::keys::{metadata_sst, namespace_head, namespace_manifest};
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use tempfile::tempdir;

#[derive(Debug)]
struct CurrentProjection {
    head: HeadState,
    metadata_state: MetadataState,
}

async fn load_current_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<CurrentProjection, CoreError> {
    let (head, metadata_state) =
        load_checkpoint_projection_metadata_state(store, namespace_id).await?;
    Ok(CurrentProjection {
        head,
        metadata_state,
    })
}

#[test]
fn fork_and_retention_do_not_use_inspection_materialization() {
    let fork_source = include_str!("../namespace/fork.rs");
    let retention_source = include_str!("retention.rs");

    for source in [fork_source, retention_source] {
        assert!(
            !source.contains("load_manifest_materialization_for_inspection"),
            "fork/retention must use verified manifest tables, not full inspection materialization"
        );
        assert!(
            !source.contains("ManifestMaterializationForInspection"),
            "fork/retention must not depend on full inspection materialization"
        );
        assert!(
            !source.contains("load_manifest_metadata_state_for_inspection"),
            "fork/retention must not construct MetadataState from manifest rows"
        );
    }
}

#[tokio::test]
async fn manifest_round_trip_uses_manifest_materialization_for_mixed_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
    .await
    .expect("write second");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello again\n",
        PutBehavior::Replace,
        &context,
        None,
    )
    .await
    .expect("replace");

    let before = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization before");
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization after");

    assert_eq!(after.head.current_manifest_id, Some(checkpoint.manifest_id));
    assert_eq!(before.head.seq, after.head.seq);
    assert!(metadata_states_equivalent(
        &before.metadata_state,
        &after.metadata_state
    ));
}

#[tokio::test]
async fn manifest_round_trip_preserves_direntry_unbind_rows() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    move_path(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        "/docs/moved.txt",
        &context,
        None,
    )
    .await
    .expect("move hello");

    let before = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization before");
    assert_eq!(before.metadata_state.direntry_unbinds().len(), 1);
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization after");

    assert_eq!(
        after.metadata_state.direntry_unbinds(),
        before.metadata_state.direntry_unbinds()
    );
    assert!(metadata_states_equivalent(
        &before.metadata_state,
        &after.metadata_state
    ));
}

#[tokio::test]
async fn manifest_round_trip_supports_empty_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(
        materialization.head.current_manifest_id,
        Some(ManifestId(1))
    );
    assert!(materialization.head.latest_checkpoint_id.is_some());
    let bootstrap_manifest =
        load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(0))
            .await
            .expect("load bootstrap manifest");
    assert!(bootstrap_manifest.manifest.payload.checkpoints.is_empty());
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(1))
            .await
            .expect("load namespace manifest");
    assert_eq!(materialized.manifest.payload.checkpoints.len(), 1);
    let checkpoint = &materialized.manifest.payload.checkpoints[0];
    assert!(validate_checkpoint_id(&checkpoint.checkpoint_id).is_ok());
    assert_eq!(checkpoint.head_seq, ChangeSeq(0));
    assert_eq!(checkpoint.manifest_id, ManifestId(1));
}

#[tokio::test]
async fn strict_manifest_consumption_fails_when_manifest_is_corrupted() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let manifest_key = namespace_manifest(namespace_id.as_str(), ManifestId(1));
    store
        .put_overwrite(&manifest_key, Bytes::from_static(br#"{"bad":"json"}"#))
        .await
        .expect("corrupt manifest");

    match load_current_projection(&store, &namespace_id).await {
        Err(CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ManifestCodec { .. },
        ))) => {}
        other => panic!("expected manifest codec manifest load error, got {other:?}"),
    }
}

#[tokio::test]
async fn create_checkpoint_surfaces_conflicting_invalid_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let manifest_key = namespace_manifest(namespace_id.as_str(), ManifestId(1));
    let store = ConflictOnManifestCreateStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        manifest_key,
        br#"{"bad":"json"}"#.to_vec(),
    );
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    match create_checkpoint(&store, &namespace_id, &context).await {
        Err(CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ManifestCodec { .. },
        ))) => {}
        other => panic!("expected manifest codec manifest load error, got {other:?}"),
    }

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(
        materialization.head.current_manifest_id,
        Some(ManifestId(0))
    );
}

#[tokio::test]
async fn retention_advancement_uses_published_manifest_and_updates_floor_only() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        MetadataSstGetCountingStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    store.reset_metadata_sst_gets();
    let unchanged = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("initial manifest already covers floor zero");
    assert_eq!(unchanged.retention_floor_seq, ChangeSeq(0));
    assert_eq!(
        store.metadata_sst_gets(),
        0,
        "retention should validate manifest descriptors without loading metadata SST payloads"
    );

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    store.reset_metadata_sst_gets();
    let advanced = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention");
    assert_eq!(advanced.retention_floor_seq, ChangeSeq(1));
    assert_eq!(
        store.metadata_sst_gets(),
        0,
        "retention should advance from manifest descriptors without materializing rows"
    );

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization.head.retention_floor_seq, ChangeSeq(1));
    assert_eq!(
        store
            .list_prefix(&format!("namespaces/{}/wal/", namespace_id.as_str()))
            .await
            .expect("list wal")
            .len(),
        1
    );
    assert!(store
        .head(&namespace_manifest(namespace_id.as_str(), ManifestId(1)))
        .await
        .expect("manifest head")
        .is_some());
}

#[tokio::test]
async fn manifest_materialization_uses_written_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(1))
            .await
            .expect("load materialized manifest");
    let current = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest_head = head_from_manifest(&current.head, &materialized.manifest);
    assert_eq!(manifest_head.seq, ChangeSeq(1));
    assert!(metadata_states_equivalent(
        &materialized.metadata_state,
        &current.metadata_state
    ));

    let segment_key = base_segment_object_keys_for_family(
        &materialized.manifest,
        ApiMetadataTableFamily::Revisions,
    )
    .into_iter()
    .next()
    .expect("revision segment");
    assert!(store
        .head(&segment_key)
        .await
        .expect("head segment")
        .is_some());
}

#[tokio::test]
async fn manifest_l0_run_materialization_matches_checkpoint_projection() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    let first = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("first checkpoint");
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first.manifest_id)
            .await
            .expect("load first manifest");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
    .await
    .expect("write second");
    let second = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("second checkpoint");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let second_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, second.manifest_id)
            .await
            .expect("load second manifest");

    assert_eq!(
        second_materialized.manifest.payload.base_seq,
        first.checkpoint_seq
    );
    assert_eq!(
        base_run(&second_materialized.manifest).tables,
        base_run(&first_materialized.manifest).tables
    );
    let l0_runs = l0_runs(&second_materialized.manifest);
    assert_eq!(l0_runs.len(), 1);
    assert_eq!(l0_runs[0].run_seq, second.checkpoint_seq);
    assert_eq!(l0_runs[0].level, CHECKPOINT_L0_RUN_LEVEL);
    assert_eq!(second_materialized.manifest.payload.checkpoints.len(), 2);
    assert_eq!(
        second_materialized.manifest.payload.checkpoints[0].checkpoint_id,
        first.checkpoint_id
    );
    assert_eq!(
        second_materialized.manifest.payload.checkpoints[1].checkpoint_id,
        second.checkpoint_id
    );
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &second_materialized.metadata_state
    ));
}

#[tokio::test]
async fn manifest_l0_run_missing_table_fails_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("first checkpoint");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
    .await
    .expect("write second");
    let second = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("second checkpoint");
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, second.manifest_id)
            .await
            .expect("load materialized manifest");
    let deleted_key = l0_runs(&materialized.manifest)[0]
        .tables
        .iter()
        .flat_map(|table| table.segments.iter())
        .next()
        .expect("l0 run segment")
        .object_key
        .clone();
    store.delete(&deleted_key).await.expect("delete l0 segment");

    match load_manifest_materialization_for_inspection(&store, &namespace_id, second.manifest_id)
        .await
    {
        Err(ManifestLoadError::MissingSegment { object_key }) => {
            assert_eq!(object_key, deleted_key);
        }
        other => panic!("expected missing l0 segment, got {other:?}"),
    }
}

#[tokio::test]
async fn manifest_materialization_rejects_off_pattern_table_keys() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let first = write_file_and_checkpoint(&store, &namespace_id, &context, 1).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id(first))
            .await
            .expect("load first manifest");
    let manifest_key = namespace_manifest(namespace_id.as_str(), manifest_id(first));
    let mut bad_base_manifest = first_materialized.manifest.clone();
    let expected_base_key = {
        let base_descriptor = bad_base_manifest
            .payload
            .metadata_files
            .iter_mut()
            .find(|metadata_file| {
                metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                    && metadata_file.family == ApiMetadataTableFamily::Inodes
            })
            .expect("base metadata file");
        let expected = metadata_sst(
            base_descriptor.owner_namespace_id.as_str(),
            base_descriptor.table_id.as_str(),
        );
        base_descriptor.object_key = format!("{}-wrong", base_descriptor.object_key);
        expected
    };

    match load_manifest_metadata_state_for_inspection_from_manifest(
        &store,
        &namespace_id,
        &manifest_key,
        &bad_base_manifest,
    )
    .await
    {
        Err(ManifestLoadError::SegmentObjectKeyMismatch { expected, .. }) => {
            assert_eq!(expected, expected_base_key);
        }
        other => panic!("expected base table key mismatch, got {other:?}"),
    }

    let second = write_file_and_checkpoint(&store, &namespace_id, &context, 2).await;
    let second_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id(second))
            .await
            .expect("load second manifest");
    let manifest_key = namespace_manifest(namespace_id.as_str(), manifest_id(second));
    let mut bad_l0_manifest = second_materialized.manifest.clone();
    let expected_l0_key = {
        let l0_descriptor = bad_l0_manifest
            .payload
            .metadata_files
            .iter_mut()
            .find(|metadata_file| {
                metadata_file.level == CHECKPOINT_L0_RUN_LEVEL
                    && metadata_file.family == ApiMetadataTableFamily::Inodes
            })
            .expect("l0 metadata file");
        let expected = metadata_sst(
            l0_descriptor.owner_namespace_id.as_str(),
            l0_descriptor.table_id.as_str(),
        );
        l0_descriptor.object_key = format!("{}-wrong", l0_descriptor.object_key);
        expected
    };

    match load_manifest_metadata_state_for_inspection_from_manifest(
        &store,
        &namespace_id,
        &manifest_key,
        &bad_l0_manifest,
    )
    .await
    {
        Err(ManifestLoadError::SegmentObjectKeyMismatch { expected, .. }) => {
            assert_eq!(expected, expected_l0_key);
        }
        other => panic!("expected l0 table key mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn manifest_run_rejects_rows_after_run_seq() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let first = write_file_and_checkpoint(&store, &namespace_id, &context, 1).await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/file-2.txt",
        b"file 2\n",
        &context,
        None,
    )
    .await
    .expect("write second file");
    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let malformed_run_tables = build_manifest_tables_from_rows(
        &store,
        &namespace_id,
        first,
        CHECKPOINT_BASE_RUN_LEVEL,
        &context.writer_version,
        |family| manifest_rows_for_family(&materialization.metadata_state, family),
        MetadataTableSegmentation::Full,
    )
    .await
    .expect("write malformed run tables");
    let metadata_ssts = build_manifest_tables_from_rows(
        &store,
        &namespace_id,
        materialization.head.seq,
        CHECKPOINT_L0_RUN_LEVEL,
        &context.writer_version,
        |family| {
            super::row::manifest_rows_for_family_after_seq(
                &materialization.metadata_state,
                family,
                first,
            )
        },
        MetadataTableSegmentation::Full,
    )
    .await
    .expect("write empty metadata run tables");
    let mut metadata_files = flatten_manifest_tables(malformed_run_tables);
    metadata_files.extend(flatten_manifest_tables(metadata_ssts));
    let manifest = NamespaceManifestEnvelope::from_payload(
        &context.writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id: manifest_id(materialization.head.seq),
            head_seq: materialization.head.seq,
            head_commit_id: materialization.head.head_commit_id.clone(),
            base_seq: first,
            active_fence_token: materialization.head.active_fence_token,
            next_inode_id: materialization.head.next_inode_id,
            name_policy: materialization.head.name_policy,
            retention_floor_seq: materialization.head.retention_floor_seq,
            initialized: true,
            verified: true,
            fork: None,
            checkpoints: Vec::new(),
            features: BTreeMap::new(),
            metadata_files,
        },
    )
    .expect("build malformed manifest");

    match load_manifest_metadata_state_for_inspection_from_manifest(
        &store,
        &namespace_id,
        &namespace_manifest(namespace_id.as_str(), manifest_id(materialization.head.seq)),
        &manifest,
    )
    .await
    {
        Err(ManifestLoadError::SegmentDescriptorMismatch { message, .. }) => {
            assert!(message.contains("after expected max"));
        }
        other => panic!("expected row range mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn manifest_l0_runs_chain_across_successive_manifests() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    for index in 1..=4 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(4))
            .await
            .expect("load chained manifest");
    assert_eq!(materialized.manifest.payload.base_seq, ChangeSeq(1));
    let l0_runs = l0_runs(&materialized.manifest);
    assert_eq!(l0_runs.len(), 3);
    for (offset, run) in l0_runs.iter().enumerate() {
        let seq = ChangeSeq(offset as u64 + 2);
        assert_eq!(run.run_seq, seq);
        assert_eq!(run.level, CHECKPOINT_L0_RUN_LEVEL);
    }
}

#[tokio::test]
async fn metadata_lsm_policy_default_matches_l0_run_cap() {
    assert_eq!(
        MetadataLsmPolicy::default().max_l0_runs,
        MAX_CHECKPOINT_L0_RUNS
    );
    assert_eq!(
        MetadataLsmPolicy::default().max_rows_per_segment,
        DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT
    );
}

#[tokio::test]
async fn manifest_policy_compacts_when_l0_runs_exceed_threshold() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataLsmPolicy {
        max_l0_runs: 2,
        ..MetadataLsmPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    for index in 1..=4 {
        write_file_and_checkpoint_with_policy(&store, &namespace_id, &context, index, policy).await;
    }

    let capped = load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(3))
        .await
        .expect("load capped manifest");
    assert_eq!(capped.manifest.payload.base_seq, ChangeSeq(1));
    assert_eq!(l0_runs(&capped.manifest).len(), 2);

    let compacted =
        load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(4))
            .await
            .expect("load compacted manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(compacted.manifest.payload.base_seq, ChangeSeq(4));
    assert!(l0_runs(&compacted.manifest).is_empty());
    assert_eq!(
        runs_from_metadata_files(&compacted.manifest.payload).len(),
        1
    );
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &compacted.metadata_state
    ));
}

#[tokio::test]
async fn manifest_rejects_segment_descriptor_payload_key_mismatch() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let manifest = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest.manifest_id)
            .await
            .expect("load manifest");
    let mut manifest = materialized.manifest;
    let descriptor = manifest
        .payload
        .metadata_files
        .iter_mut()
        .find(|metadata_file| {
            metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                && metadata_file.family == ApiMetadataTableFamily::Revisions
        })
        .expect("revision metadata file");
    descriptor.segment_key = MetadataSegmentKey::RowKeyRange { shard: u32::MAX };

    let manifest_key = namespace_manifest(namespace_id.as_str(), manifest.payload.manifest_id);
    let writer_version = manifest.writer_version.clone();
    let manifest_id = manifest.payload.manifest_id;
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(writer_version, manifest.payload)
            .expect("updated manifest");
    let manifest_bytes =
        encode_namespace_manifest_json(&updated_manifest).expect("encode manifest");
    store
        .put_overwrite(&manifest_key, Bytes::from(manifest_bytes))
        .await
        .expect("overwrite manifest");

    match load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id).await {
        Err(ManifestLoadError::SegmentKeyMismatch { .. }) => {}
        other => panic!("expected segment key mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn manifest_base_run_tables_have_sorted_segment_coverage() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..6 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    let policy = MetadataLsmPolicy {
        max_rows_per_segment: 2,
        ..MetadataLsmPolicy::default()
    };
    let materialization_before = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization before checkpoint");

    let manifest = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("manifest");
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest.manifest_id)
            .await
            .expect("load manifest");

    let base = base_run(&materialized.manifest);
    let revisions = base
        .tables
        .iter()
        .find(|table| table.family == ApiMetadataTableFamily::Revisions)
        .expect("revision table");
    assert!(revisions.segments.len() >= 3);
    assert!(revisions.segments.iter().all(|descriptor| {
        matches!(
            descriptor.segment_key,
            MetadataSegmentKey::RowKeyRange { .. }
        )
    }));

    let direntries = base
        .tables
        .iter()
        .find(|table| table.family == ApiMetadataTableFamily::DirentryBinds)
        .expect("direntry table");
    assert!(
        direntries.segments.len() >= 3,
        "hot directory direntry rows should be range-split"
    );
    assert!(direntries.segments.iter().all(|descriptor| {
        matches!(
            descriptor.segment_key,
            MetadataSegmentKey::RowKeyRange { .. }
        )
    }));

    for table in &base.tables {
        let mut previous_max_key: Option<&str> = None;
        for descriptor in &table.segments {
            assert!(descriptor.min_key.as_str() <= descriptor.max_key.as_str());
            if let Some(previous) = previous_max_key {
                assert!(previous < descriptor.min_key.as_str());
            }
            previous_max_key = Some(descriptor.max_key.as_str());
        }
    }
    assert!(metadata_states_equivalent(
        &materialization_before.metadata_state,
        &materialized.metadata_state
    ));
}

#[tokio::test]
async fn large_table_scan_does_not_insert_metadata_cache_blocks() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    let policy = MetadataLsmPolicy {
        max_rows_per_segment: 1,
        ..MetadataLsmPolicy::default()
    };
    let manifest = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("manifest");
    let cache = super::MetadataTableCache::new(Default::default());
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        manifest.manifest_id,
    )
    .await
    .expect("load tables");

    let before = cache.stats();
    let revisions = tables
        .scan_prefix(ApiMetadataTableFamily::Revisions, "revision-")
        .await
        .expect("scan revisions");
    let after = cache.stats();

    assert!(revisions.len() >= 8);
    assert_eq!(after.inserts, before.inserts);
    assert!(after.misses > before.misses);
}

#[tokio::test]
async fn table_range_page_merges_base_and_l0_in_row_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/docs/a.txt", b"a\n", &context, None)
        .await
        .expect("write a");
    write_file_bytes(&store, &namespace_id, "/docs/c.txt", b"c\n", &context, None)
        .await
        .expect("write c");

    let policy = MetadataLsmPolicy {
        max_rows_per_segment: 1,
        ..MetadataLsmPolicy::default()
    };
    create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("first checkpoint");
    write_file_bytes(&store, &namespace_id, "/docs/b.txt", b"b\n", &context, None)
        .await
        .expect("write b");
    let manifest = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("second checkpoint");
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        None,
        &namespace_id,
        manifest.manifest_id,
    )
    .await
    .expect("load tables");

    let docs_inode_id = InodeId(2);
    let lower_bound = format!("direntry-{:020}-", docs_inode_id.0);
    let upper_bound = super::string_prefix_upper_bound(&lower_bound);
    let page = tables
        .scan_range_page(
            ApiMetadataTableFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            2,
        )
        .await
        .expect("scan range page");
    let display_names = page
        .into_iter()
        .filter_map(|row| match row {
            MetadataRow::DirentryBind { display_name, .. } => Some(display_name),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(display_names, vec!["a.txt", "b.txt"]);
}

#[tokio::test]
async fn maintenance_materialization_does_not_populate_metadata_table_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    let policy = MetadataLsmPolicy {
        max_rows_per_segment: 1,
        ..MetadataLsmPolicy::default()
    };
    let manifest = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("manifest");
    let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
    let before = cache.stats();

    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest.manifest_id)
            .await
            .expect("load materialized manifest");
    let after = cache.stats();

    assert!(flatten_manifest_tables(base_run(&materialized.manifest).tables).len() > 1);
    assert_eq!(after, before);
}

#[tokio::test]
async fn metadata_cache_budget_counts_decoded_blocks() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/file.txt",
        b"file\n",
        &context,
        None,
    )
    .await
    .expect("write file");
    let manifest = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let cache = MetadataTableCache::new(MetadataTableCacheConfig {
        enabled: true,
        max_blocks: 256,
        max_decoded_bytes: Some(1),
    });
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        manifest.manifest_id,
    )
    .await
    .expect("load tables");

    let key = "inode-00000000000000000001";
    assert!(tables
        .get(ApiMetadataTableFamily::Inodes, key)
        .await
        .expect("get inode")
        .is_some());

    let stats = cache.stats();
    assert!(stats.inserts > 0);
    assert!(stats.evictions > 0);
}

#[tokio::test]
async fn whole_run_compaction_rewrites_base_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataLsmPolicy {
        max_l0_runs: 1,
        max_rows_per_segment: 2,
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/hot/original.txt",
        b"hot\n",
        &context,
        None,
    )
    .await
    .expect("write hot");
    write_file_bytes(
        &store,
        &namespace_id,
        "/cold/original.txt",
        b"cold\n",
        &context,
        None,
    )
    .await
    .expect("write cold");

    let first = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("first checkpoint");
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first.manifest_id)
            .await
            .expect("load first manifest");
    let first_run_keys = run_segment_object_keys(&first_materialized.manifest);

    write_file_bytes(
        &store,
        &namespace_id,
        "/hot/after-l0.txt",
        b"hot 2\n",
        &context,
        None,
    )
    .await
    .expect("write hot l0");
    create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("l0 checkpoint");
    write_file_bytes(
        &store,
        &namespace_id,
        "/hot/after-compact.txt",
        b"hot 3\n",
        &context,
        None,
    )
    .await
    .expect("write hot compact");
    let compacted = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("compacted checkpoint");
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted.manifest_id)
            .await
            .expect("load compacted manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let compacted_run_keys = run_segment_object_keys(&compacted_materialized.manifest);
    let compacted_run_prefix = format!("namespaces/{}/tables/metadata/tbl_", namespace_id.as_str());

    assert_eq!(
        compacted_materialized.manifest.payload.base_seq,
        compacted.checkpoint_seq
    );
    assert!(l0_runs(&compacted_materialized.manifest).is_empty());
    assert_eq!(
        runs_from_metadata_files(&compacted_materialized.manifest.payload).len(),
        1
    );
    assert!(!compacted_run_keys.is_empty());
    assert!(compacted_run_keys
        .iter()
        .all(|key| key.starts_with(&compacted_run_prefix)));
    assert!(compacted_run_keys
        .iter()
        .all(|key| !first_run_keys.contains(key)));
    assert_manifest_rows_have_unique_keys(&compacted_materialized.metadata_state);
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &compacted_materialized.metadata_state
    ));
}

#[tokio::test]
async fn whole_run_compaction_resegments_row_key_range_families_with_l0_runs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataLsmPolicy {
        max_l0_runs: 1,
        max_rows_per_segment: 2,
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..6 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"initial\n", &context, None)
            .await
            .expect("write initial file");
    }

    let first = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("first checkpoint");
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first.manifest_id)
            .await
            .expect("load first manifest");
    let revision_keys_before = base_segment_object_keys_for_family(
        &first_materialized.manifest,
        ApiMetadataTableFamily::Revisions,
    );
    assert!(revision_keys_before.len() >= 3);

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/file-0.txt",
        b"l0\n",
        &context,
        None,
    )
    .await
    .expect("write l0 revision");
    create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("l0 checkpoint");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/file-0.txt",
        b"compact\n",
        &context,
        None,
    )
    .await
    .expect("write compaction revision");
    let compacted = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("compacted checkpoint");
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted.manifest_id)
            .await
            .expect("load compacted manifest");
    let revision_keys_after = base_segment_object_keys_for_family(
        &compacted_materialized.manifest,
        ApiMetadataTableFamily::Revisions,
    );
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");

    assert!(l0_runs(&compacted_materialized.manifest).is_empty());
    assert_eq!(
        runs_from_metadata_files(&compacted_materialized.manifest.payload).len(),
        1
    );
    assert!(revision_keys_after
        .iter()
        .all(|key| !revision_keys_before.contains(key)));
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &compacted_materialized.metadata_state
    ));
    assert_manifest_rows_have_unique_keys(&compacted_materialized.metadata_state);
}

#[tokio::test]
async fn manifest_writes_and_validates_direntry_child_bind_index() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let manifest = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest.manifest_id)
            .await
            .expect("load manifest");
    let base = base_run(&materialized.manifest);
    let child_table = base
        .tables
        .iter()
        .find(|table| {
            table.family == loonfs_api::wire::manifest::MetadataTableFamily::DirentryChildBinds
        })
        .expect("child bind table");
    let child_segment = child_table.segments.first().expect("child bind segment");
    assert!(child_segment
        .min_key
        .starts_with("direntry-child-000000000000000000"));

    let deleted_key = child_segment.object_key.clone();
    store
        .delete(&deleted_key)
        .await
        .expect("delete child index");
    match load_manifest_materialization_for_inspection(&store, &namespace_id, manifest.manifest_id)
        .await
    {
        Err(ManifestLoadError::MissingSegment { object_key }) => {
            assert_eq!(object_key, deleted_key);
        }
        other => panic!("expected missing child-bind segment, got {other:?}"),
    }
}

#[tokio::test]
async fn manifest_rejects_child_bind_index_that_diverges_from_canonical_binds() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/other.txt",
        b"other\n",
        &context,
        None,
    )
    .await
    .expect("write other");

    let manifest = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest.manifest_id)
            .await
            .expect("load manifest before corruption");
    let mut manifest = materialized.manifest;
    let mut child_index_rows = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataTableFamily::DirentryChildBinds,
    );
    assert!(child_index_rows.len() >= 2);
    child_index_rows[0] = child_index_rows[1].clone();
    child_index_rows
        .sort_by_key(|row| row.row_key_for_family(ApiMetadataTableFamily::DirentryChildBinds));

    let child_descriptor = manifest
        .payload
        .metadata_files
        .iter_mut()
        .find(|metadata_file| {
            metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                && metadata_file.family == ApiMetadataTableFamily::DirentryChildBinds
        })
        .expect("child bind metadata file");
    rewrite_manifest_segment(
        &store,
        &namespace_id,
        manifest.payload.head_seq,
        ApiMetadataTableFamily::DirentryChildBinds,
        child_descriptor,
        child_index_rows,
        &context.writer_version,
    )
    .await;

    let manifest_key = namespace_manifest(namespace_id.as_str(), manifest.payload.manifest_id);
    let writer_version = manifest.writer_version.clone();
    let manifest_id = manifest.payload.manifest_id;
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(writer_version, manifest.payload)
            .expect("updated manifest");
    let manifest_bytes =
        encode_namespace_manifest_json(&updated_manifest).expect("encode updated manifest");
    store
        .put_overwrite(&manifest_key, Bytes::from(manifest_bytes))
        .await
        .expect("overwrite manifest");

    assert_child_index_mismatch(
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id).await,
    );
}

async fn rewrite_manifest_segment(
    store: &LocalFsStore,
    _namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    family: ApiMetadataTableFamily,
    descriptor: &mut MetadataFileRef,
    rows: Vec<MetadataRow>,
    writer_version: &str,
) {
    let row_keys = rows
        .iter()
        .map(|row| row.row_key_for_family(family))
        .collect::<Vec<_>>();
    let min_key = row_keys.first().cloned().unwrap_or_default();
    let max_key = row_keys.last().cloned().unwrap_or_default();
    let page = MetadataPage {
        page_index: 0,
        min_key: min_key.clone(),
        max_key: max_key.clone(),
        row_keys,
        rows,
    };
    let payload = MetadataSstPayload {
        namespace_id: descriptor.owner_namespace_id.clone(),
        table_id: descriptor.table_id.clone(),
        run_seq,
        level: descriptor.level,
        family,
        segment_index: descriptor.segment_index,
        segment_key: descriptor.segment_key.clone(),
        row_count: page.rows.len() as u64,
        min_key,
        max_key,
        pages: vec![page],
    };
    let envelope =
        MetadataSstEnvelope::from_payload(writer_version, payload).expect("rewritten segment");
    let encoded = encode_metadata_sst_envelope_zstd(&envelope).expect("encode rewritten segment");
    store
        .put_overwrite(&descriptor.object_key, Bytes::from(encoded))
        .await
        .expect("overwrite segment");

    descriptor.row_count = envelope.payload.row_count;
    descriptor.min_key = envelope.payload.min_key.clone();
    descriptor.max_key = envelope.payload.max_key.clone();
    descriptor.payload_checksum = envelope.payload_checksum.clone();
}

fn assert_child_index_mismatch<T>(result: Result<T, ManifestLoadError>) {
    match result {
        Err(ManifestLoadError::SegmentDescriptorMismatch { message, .. }) => {
            assert!(message.contains("direntry-child-binds index"));
        }
        Err(other) => panic!("expected child index mismatch, got {other:?}"),
        Ok(_) => panic!("expected child index mismatch"),
    }
}

#[tokio::test]
async fn manifest_rejects_missing_revision_desc_index() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let manifest = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest.manifest_id)
            .await
            .expect("load manifest before corruption");
    let mut manifest = materialized.manifest;
    let manifest_id = manifest.payload.manifest_id;
    manifest.payload.metadata_files.retain(|metadata_file| {
        metadata_file.family != ApiMetadataTableFamily::RevisionsByInodeDesc
    });
    overwrite_manifest(&store, &namespace_id, manifest).await;

    assert_revision_index_mismatch(
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id).await,
    );
}

#[tokio::test]
async fn manifest_rejects_revision_desc_index_missing_row() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/other.txt",
        b"other\n",
        &context,
        None,
    )
    .await
    .expect("write other");

    let (manifest_id, mut manifest, mut revision_index_rows) =
        revision_index_test_materialization(&store, &namespace_id, &context).await;
    revision_index_rows.pop().expect("revision index row");
    rewrite_revision_index_segment(
        &store,
        &namespace_id,
        &mut manifest,
        revision_index_rows,
        &context.writer_version,
    )
    .await;
    overwrite_manifest(&store, &namespace_id, manifest).await;

    assert_revision_index_mismatch(
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id).await,
    );
}

#[tokio::test]
async fn manifest_rejects_revision_desc_index_extra_row() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let (manifest_id, mut manifest, mut revision_index_rows) =
        revision_index_test_materialization(&store, &namespace_id, &context).await;
    let extra_row = revision_index_rows
        .first()
        .expect("revision index row")
        .clone();
    revision_index_rows.push(match extra_row {
        MetadataRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            revision_delta_index,
            content_ref,
        } => MetadataRow::Revision {
            inode_id,
            revision_no: loonfs_api::RevisionNo(revision_no.0 + 100),
            committed_seq,
            revision_delta_index,
            content_ref,
        },
        other => other,
    });
    rewrite_revision_index_segment(
        &store,
        &namespace_id,
        &mut manifest,
        revision_index_rows,
        &context.writer_version,
    )
    .await;
    overwrite_manifest(&store, &namespace_id, manifest).await;

    assert_revision_index_mismatch(
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id).await,
    );
}

#[tokio::test]
async fn manifest_rejects_revision_desc_index_changed_content_ref() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let (manifest_id, mut manifest, mut revision_index_rows) =
        revision_index_test_materialization(&store, &namespace_id, &context).await;
    let first = revision_index_rows.first_mut().expect("revision index row");
    if let MetadataRow::Revision { content_ref, .. } = first {
        content_ref.digest =
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned();
    }
    rewrite_revision_index_segment(
        &store,
        &namespace_id,
        &mut manifest,
        revision_index_rows,
        &context.writer_version,
    )
    .await;
    overwrite_manifest(&store, &namespace_id, manifest).await;

    assert_revision_index_mismatch(
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id).await,
    );
}

#[tokio::test]
async fn manifest_rejects_revision_desc_index_duplicate_rows() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let (manifest_id, mut manifest, mut revision_index_rows) =
        revision_index_test_materialization(&store, &namespace_id, &context).await;
    revision_index_rows.push(
        revision_index_rows
            .first()
            .expect("revision index row")
            .clone(),
    );
    rewrite_revision_index_segment(
        &store,
        &namespace_id,
        &mut manifest,
        revision_index_rows,
        &context.writer_version,
    )
    .await;
    overwrite_manifest(&store, &namespace_id, manifest).await;

    match load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id).await {
        Err(ManifestLoadError::DuplicateRevisionRow { family, .. }) => {
            assert_eq!(family, ApiMetadataTableFamily::RevisionsByInodeDesc);
        }
        other => panic!("expected duplicate revision row, got {other:?}"),
    }
}

async fn revision_index_test_materialization(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> (ManifestId, NamespaceManifestEnvelope, Vec<MetadataRow>) {
    let manifest = create_checkpoint(store, namespace_id, context)
        .await
        .expect("checkpoint");
    let materialized =
        load_manifest_materialization_for_inspection(store, namespace_id, manifest.manifest_id)
            .await
            .expect("load manifest before corruption");
    let revision_index_rows = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataTableFamily::RevisionsByInodeDesc,
    );
    assert!(!revision_index_rows.is_empty());
    (
        materialized.manifest.payload.manifest_id,
        materialized.manifest,
        revision_index_rows,
    )
}

async fn rewrite_revision_index_segment(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    manifest: &mut NamespaceManifestEnvelope,
    mut rows: Vec<MetadataRow>,
    writer_version: &str,
) {
    rows.sort_by_key(|row| row.row_key_for_family(ApiMetadataTableFamily::RevisionsByInodeDesc));
    let descriptor = manifest
        .payload
        .metadata_files
        .iter_mut()
        .find(|metadata_file| {
            metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                && metadata_file.family == ApiMetadataTableFamily::RevisionsByInodeDesc
        })
        .expect("revision index metadata file");
    rewrite_manifest_segment(
        store,
        namespace_id,
        manifest.payload.head_seq,
        ApiMetadataTableFamily::RevisionsByInodeDesc,
        descriptor,
        rows,
        writer_version,
    )
    .await;
}

async fn overwrite_manifest(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    manifest: NamespaceManifestEnvelope,
) {
    let manifest_key = namespace_manifest(namespace_id.as_str(), manifest.payload.manifest_id);
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(manifest.writer_version, manifest.payload)
            .expect("updated manifest");
    let manifest_bytes =
        encode_namespace_manifest_json(&updated_manifest).expect("encode updated manifest");
    store
        .put_overwrite(&manifest_key, Bytes::from(manifest_bytes))
        .await
        .expect("overwrite manifest");
}

fn assert_revision_index_mismatch<T>(result: Result<T, ManifestLoadError>) {
    match result {
        Err(ManifestLoadError::RevisionIndexMismatch { .. }) => {}
        Err(other) => panic!("expected revision index mismatch, got {other:?}"),
        Ok(_) => panic!("expected revision index mismatch"),
    }
}

fn base_run(manifest: &NamespaceManifestEnvelope) -> MetadataRunManifest {
    runs_from_metadata_files(&manifest.payload)
        .into_iter()
        .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
        .expect("base run")
}

fn l0_runs(manifest: &NamespaceManifestEnvelope) -> Vec<MetadataRunManifest> {
    runs_from_metadata_files(&manifest.payload)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        .collect()
}

fn run_segment_object_keys(manifest: &NamespaceManifestEnvelope) -> Vec<String> {
    runs_from_metadata_files(&manifest.payload)
        .into_iter()
        .flat_map(|run| {
            run.tables.into_iter().flat_map(|table| {
                table
                    .segments
                    .into_iter()
                    .map(|descriptor| descriptor.object_key.clone())
            })
        })
        .collect()
}

fn base_segment_object_keys_for_family(
    manifest: &NamespaceManifestEnvelope,
    family: ApiMetadataTableFamily,
) -> Vec<String> {
    base_run(manifest)
        .tables
        .iter()
        .find(|table| table.family == family)
        .expect("table")
        .segments
        .iter()
        .map(|descriptor| descriptor.object_key.clone())
        .collect()
}

fn assert_manifest_rows_have_unique_keys(metadata_state: &MetadataState) {
    for family in CHECKPOINT_TABLE_FAMILIES {
        let rows = manifest_rows_for_family(metadata_state, family);
        let mut seen = BTreeSet::new();
        for row in rows {
            let row_key = row.row_key_for_family(family);
            assert!(
                seen.insert(row_key.clone()),
                "duplicate metadata row key `{row_key}` in {family:?}"
            );
        }
    }
}

#[tokio::test]
async fn manifest_l0_run_cap_collapses_back_to_base_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    for index in 1..=10 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    let capped = load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(9))
        .await
        .expect("load capped manifest");
    assert_eq!(capped.manifest.payload.base_seq, ChangeSeq(1));
    assert_eq!(l0_runs(&capped.manifest).len(), MAX_CHECKPOINT_L0_RUNS);

    let collapsed =
        load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(10))
            .await
            .expect("load collapsed manifest");
    assert_eq!(collapsed.manifest.payload.base_seq, ChangeSeq(10));
    assert!(l0_runs(&collapsed.manifest).is_empty());
    assert_eq!(
        runs_from_metadata_files(&collapsed.manifest.payload).len(),
        1
    );
}

#[tokio::test]
async fn unreferenced_manifest_run_is_ignored_by_current_projection_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    let first = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("first checkpoint");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
    .await
    .expect("write second");

    let materialization_before = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let orphan_manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization_before.head,
            metadata_state: &materialization_before.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(2),
        None,
    )
    .await
    .expect("build orphan manifest");
    write_namespace_manifest(&store, &orphan_manifest)
        .await
        .expect("write orphan manifest");

    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(
        materialization_after.head.current_manifest_id,
        Some(first.manifest_id)
    );
    assert_eq!(
        materialization_after.head.seq,
        orphan_manifest.payload.head_seq
    );
    assert!(metadata_states_equivalent(
        &materialization_before.metadata_state,
        &materialization_after.metadata_state
    ));
}

#[tokio::test]
async fn write_namespace_manifest_conflict_same_payload_is_idempotent() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
        None,
    )
    .await
    .expect("build manifest");

    write_namespace_manifest(&store, &manifest)
        .await
        .expect("first manifest write");
    write_namespace_manifest(&store, &manifest)
        .await
        .expect("same manifest write is idempotent");
}

#[tokio::test]
async fn write_namespace_manifest_conflict_different_payload_is_error() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
        None,
    )
    .await
    .expect("build manifest");
    let mut conflicting_payload = manifest.payload.clone();
    conflicting_payload.next_inode_id = InodeId(conflicting_payload.next_inode_id.0 + 1);
    let conflicting_manifest =
        NamespaceManifestEnvelope::from_payload(&context.writer_version, conflicting_payload)
            .expect("build conflicting manifest");

    write_namespace_manifest(&store, &manifest)
        .await
        .expect("first manifest write");
    let error = write_namespace_manifest(&store, &conflicting_manifest)
        .await
        .expect_err("different same-id manifest must conflict");

    match error {
        MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ManifestConflict {
            manifest_id,
            expected_payload_checksum,
            actual_payload_checksum,
            ..
        }) => {
            assert_eq!(manifest_id, ManifestId(1));
            assert_eq!(
                expected_payload_checksum,
                conflicting_manifest.payload_checksum
            );
            assert_eq!(actual_payload_checksum, manifest.payload_checksum);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn create_checkpoint_retries_same_id_different_payload_allocation_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let raw_store = LocalFsStore::new(temp_dir.path()).expect("raw store");
    bootstrap_namespace(&raw_store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &raw_store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let materialization = load_current_projection(&raw_store, &namespace_id)
        .await
        .expect("materialization");
    let conflicting = build_namespace_manifest_from_metadata_state(
        &raw_store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
        None,
    )
    .await
    .expect("build conflicting manifest");
    let mut conflicting_payload = conflicting.payload;
    conflicting_payload.next_inode_id = InodeId(conflicting_payload.next_inode_id.0 + 1);
    let conflicting =
        NamespaceManifestEnvelope::from_payload(&context.writer_version, conflicting_payload)
            .expect("rewrap conflicting manifest");
    let conflicting_bytes =
        encode_namespace_manifest_json(&conflicting).expect("encode conflicting manifest");
    let store = ConflictOnManifestCreateStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        namespace_manifest(namespace_id.as_str(), ManifestId(1)),
        conflicting_bytes,
    );

    let checkpoint = create_checkpoint_with_policy(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await
    .expect("create checkpoint should retry allocation");

    assert_eq!(checkpoint.manifest_id, ManifestId(2));
    let retried =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_id)
            .await
            .expect("load retried manifest");
    assert!(checkpoint_record_by_id(&retried.manifest, &checkpoint.checkpoint_id).is_some());
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(
        materialization_after.head.current_manifest_id,
        Some(ManifestId(2))
    );
}

#[tokio::test]
async fn create_checkpoint_adds_record_when_current_manifest_exists_without_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest_without_checkpoint = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
        None,
    )
    .await
    .expect("build manifest");
    let original_files = manifest_without_checkpoint.payload.metadata_files.clone();
    write_namespace_manifest(&store, &manifest_without_checkpoint)
        .await
        .expect("write manifest");
    publish_current_manifest_id(
        &store,
        &namespace_id,
        ManifestId(1),
        "chk_00000000000000000000000000000099",
        &context.writer_version,
    )
    .await
    .expect("publish manifest without checkpoint");

    let checkpoint = create_checkpoint_with_policy(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await
    .expect("create checkpoint");

    assert_eq!(checkpoint.manifest_id, ManifestId(2));
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_id)
            .await
            .expect("load new manifest");
    assert_eq!(materialized.manifest.payload.metadata_files, original_files);
    assert_eq!(materialized.manifest.payload.checkpoints.len(), 1);
    assert_eq!(
        materialized.manifest.payload.checkpoints[0].checkpoint_id,
        checkpoint.checkpoint_id
    );
    assert_eq!(
        materialized.manifest.payload.checkpoints[0].manifest_id,
        checkpoint.manifest_id
    );
}

#[tokio::test]
async fn checkpoint_l0_update_does_not_read_existing_metadata_ssts() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        MetadataSstGetCountingStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/first.txt",
        b"first\n",
        &context,
        None,
    )
    .await
    .expect("write first");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create first checkpoint");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
    .await
    .expect("write second");
    store.reset_metadata_sst_gets();

    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create L0 checkpoint");

    assert_eq!(
        store.metadata_sst_gets(),
        0,
        "L0 checkpoint update should use the WAL tail and copy existing metadata file refs"
    );
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_id)
            .await
            .expect("load checkpoint manifest");
    assert_eq!(l0_runs(&materialized.manifest).len(), 1);
}

#[tokio::test]
async fn manifest_without_checkpoint_record_reconstructs_manifest_head_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
        None,
    )
    .await
    .expect("build manifest without checkpoint");
    assert!(manifest.payload.checkpoints.is_empty());
    let mut newer_live_head = materialization.head.clone();
    newer_live_head.head_commit_id =
        CommitId::parse("c_00000000000000000000000000000099").expect("commit id");

    let reconstructed = head_from_manifest(&newer_live_head, &manifest);

    assert_eq!(
        reconstructed.head_commit_id,
        manifest.payload.head_commit_id
    );
    assert_ne!(reconstructed.head_commit_id, newer_live_head.head_commit_id);
}

#[tokio::test]
async fn current_manifest_advance_without_checkpoint_record_is_not_success() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let materialization_before = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let tables = build_manifest_tables(
        &store,
        &namespace_id,
        materialization_before.head.seq,
        CHECKPOINT_BASE_RUN_LEVEL,
        &materialization_before.metadata_state,
        &context.writer_version,
        MetadataLsmPolicy::default().max_rows_per_segment,
    )
    .await
    .expect("build metadata tables");
    let manifest = NamespaceManifestEnvelope::from_payload(
        &context.writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id: ManifestId(materialization_before.head.seq.0),
            head_seq: materialization_before.head.seq,
            head_commit_id: materialization_before.head.head_commit_id.clone(),
            base_seq: materialization_before.head.seq,
            active_fence_token: materialization_before.head.active_fence_token,
            next_inode_id: materialization_before.head.next_inode_id,
            name_policy: materialization_before.head.name_policy,
            retention_floor_seq: materialization_before.head.retention_floor_seq,
            initialized: true,
            verified: true,
            fork: None,
            checkpoints: Vec::new(),
            features: BTreeMap::new(),
            metadata_files: flatten_manifest_tables(tables),
        },
    )
    .expect("build manifest");
    write_namespace_manifest(&store, &manifest)
        .await
        .expect("write manifest");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
    .await
    .expect("write second");
    let later_checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("later checkpoint");
    assert!(later_checkpoint.manifest_id > ManifestId(materialization_before.head.seq.0));

    let checkpoint_id = "chk_00000000000000000000000000000099";
    let outcome = publish_current_manifest_id(
        &store,
        &namespace_id,
        ManifestId(materialization_before.head.seq.0),
        checkpoint_id,
        &context.writer_version,
    )
    .await
    .expect("manifest publication check should classify current manifest");

    assert_eq!(
        outcome,
        ManifestPublicationOutcome::CurrentManifestMissingCheckpoint {
            current_manifest_id: later_checkpoint.manifest_id
        }
    );
}

#[tokio::test]
async fn current_manifest_cas_retry_exhaustion_reports_head_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = HeadCasFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        namespace_head(namespace_id.as_str()),
    );
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    store.fail_head_cas();
    let outcome = publish_current_manifest_id(
        &store,
        &namespace_id,
        ManifestId(1),
        "chk_00000000000000000000000000000000",
        &context.writer_version,
    )
    .await
    .expect("current manifest publication should report CAS race");

    assert_eq!(outcome, ManifestPublicationOutcome::HeadCasRaceLost);
}

fn test_context() -> MutationContext {
    MutationContext {
        writer_id: "test-writer".to_owned(),
        writer_version: "test-writer/0.1.0".to_owned(),
        now_ms: 1_000,
        lease_duration_ms: 60_000,
    }
}

fn manifest_id(seq: ChangeSeq) -> ManifestId {
    ManifestId(seq.0)
}

async fn write_file_and_checkpoint(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    index: u64,
) -> ChangeSeq {
    let path = format!("/docs/file-{index}.txt");
    let bytes = format!("file {index}\n");
    write_file_bytes(store, namespace_id, &path, bytes.as_bytes(), context, None)
        .await
        .expect("write file");
    create_checkpoint(store, namespace_id, context)
        .await
        .expect("create checkpoint")
        .checkpoint_seq
}

async fn write_file_and_checkpoint_with_policy(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    index: u64,
    policy: MetadataLsmPolicy,
) -> ChangeSeq {
    let path = format!("/docs/file-{index}.txt");
    let bytes = format!("file {index}\n");
    write_file_bytes(store, namespace_id, &path, bytes.as_bytes(), context, None)
        .await
        .expect("write file");
    create_checkpoint_with_policy(store, namespace_id, context, policy)
        .await
        .expect("create checkpoint")
        .checkpoint_seq
}

#[derive(Debug)]
struct HeadCasFailureStore {
    inner: LocalFsStore,
    head_key: String,
    fail_head_cas: Mutex<bool>,
}

impl HeadCasFailureStore {
    fn new(inner: LocalFsStore, head_key: String) -> Self {
        Self {
            inner,
            head_key,
            fail_head_cas: Mutex::new(false),
        }
    }

    fn fail_head_cas(&self) {
        *self
            .fail_head_cas
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    }
}

#[async_trait]
impl ObjectStore for HeadCasFailureStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key
            && matches!(&mode, PutMode::CompareAndSwap { .. })
            && *self
                .fail_head_cas
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            return Err(ObjectStoreError::PreconditionFailed);
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct MetadataSstGetCountingStore {
    inner: LocalFsStore,
    metadata_sst_gets: Mutex<usize>,
}

impl MetadataSstGetCountingStore {
    fn new(inner: LocalFsStore) -> Self {
        Self {
            inner,
            metadata_sst_gets: Mutex::new(0),
        }
    }

    fn metadata_sst_gets(&self) -> usize {
        *self
            .metadata_sst_gets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn reset_metadata_sst_gets(&self) {
        *self
            .metadata_sst_gets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = 0;
    }

    fn record_if_metadata_sst(&self, key: &str) {
        if key.contains("/tables/metadata/") && key.ends_with(".sst.zst") {
            *self
                .metadata_sst_gets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
        }
    }
}

#[async_trait]
impl ObjectStore for MetadataSstGetCountingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.record_if_metadata_sst(key);
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.record_if_metadata_sst(key);
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct ConflictOnManifestCreateStore {
    inner: LocalFsStore,
    manifest_key: String,
    replacement_bytes: Vec<u8>,
    injected: Mutex<bool>,
}

impl ConflictOnManifestCreateStore {
    fn new(inner: LocalFsStore, manifest_key: String, replacement_bytes: Vec<u8>) -> Self {
        Self {
            inner,
            manifest_key,
            replacement_bytes,
            injected: Mutex::new(false),
        }
    }
}

#[async_trait]
impl ObjectStore for ConflictOnManifestCreateStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.manifest_key && matches!(&mode, PutMode::CreateIfAbsent) {
            let should_inject = {
                let mut injected = self
                    .injected
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let should_inject = !*injected;
                if should_inject {
                    *injected = true;
                }
                should_inject
            };
            if should_inject {
                self.inner
                    .put_overwrite(key, Bytes::copy_from_slice(&self.replacement_bytes))
                    .await?;
                return Err(ObjectStoreError::Conflict);
            }
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}
