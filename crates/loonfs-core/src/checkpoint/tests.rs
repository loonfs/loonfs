#![allow(clippy::panic)]
// These tests use panic in impossible match arms to preserve precise failure messages.

//! Behavior tests for the checkpoint lifecycle: creation, publication
//! races, retention, fork materialization, and corruption rejection.

use super::build::{
    build_manifest_tables, build_manifest_tables_from_rows, MetadataTableSegmentation,
};
use super::cache::{MetadataTableCache, MetadataTableCacheConfig};
use super::create::load_checkpoint_projection_metadata_state;
use super::error::ManifestLoadError;
use super::index_build::{
    build_grams_index_step, disable_grams_index, enable_grams_index, fold_grams_index_step,
    GramIndexBuildOutcome, GramIndexBuildPolicy, GramIndexDisableOutcome, GramIndexEnableOutcome,
    GramIndexFoldOutcome,
};
use super::load::{
    head_from_manifest, load_manifest_materialization_for_inspection,
    load_manifest_metadata_state_for_inspection_from_manifest, load_verified_manifest_tables,
};
use super::publish::{publish_metadata_root, write_namespace_manifest, ManifestPublicationOutcome};
use super::record::read_checkpoint_record;
use super::retention::advance_retention_floor;
use super::row::{manifest_rows_for_family, metadata_states_equivalent};
use super::runs::{
    flatten_manifest_tables, runs_from_metadata_files, runs_in_scan_order, MetadataLsmPolicy,
    MetadataRunManifest, CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL,
    CHECKPOINT_TABLE_FAMILIES, DEFAULT_MAX_CHECKPOINT_L0_RUNS,
};
use crate::error::{CoreError, ErrorCode, MetadataProjectionLoadError};
use crate::metadata::MetadataState;
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::namespace::control::{
    read_head_object, read_metadata_root_object, read_wal_floor_object,
};
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::path::write::ops::{
    delete_path, move_path, put_file_bytes, restore_file_revision, write_file_bytes,
};
use crate::protocol::list_changes_after;
use crate::MutationContext;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::wire::control::{HeadState, MetadataRootState};
use loonfs_api::wire::index_grams::{
    Gram, GramPosting, IndexGramsFeature, IndexRow, INDEX_GRAMS_FEATURE_KEY,
};
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, encode_namespace_manifest_json, lookup_keys, MetadataFileRef,
    MetadataRow, MetadataTableFamily as ApiMetadataTableFamily, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_index_block, BlockHandle, SegmentBlocksBuilder,
};
use loonfs_api::{
    ChangeSeq, CheckpointId, CommitId, EffectiveLimit, InodeId, ManifestId, ManifestObjectId,
    NameKey, NamespaceId, PutBehavior, RevisionNo,
};
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_manifest_prefix, metadata_table, wal_head, wal_segment,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

/// Every lifecycle test in this file pins as one user owner; owner-specific
/// behavior (fork owners, distinct-owner records) is exercised explicitly
/// where it matters.
async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> crate::error::Result<loonfs_api::CreateCheckpointResponse> {
    super::create::create_checkpoint(
        store,
        namespace_id,
        loonfs_api::wire::control::CheckpointOwner::User {
            name: "test-pin".to_owned(),
        },
        None,
        context,
    )
    .await
}

#[derive(Debug)]
struct CurrentProjection {
    head: HeadState,
    root: MetadataRootState,
    metadata_state: MetadataState,
}

async fn read_floor_seq<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    read_wal_floor_object(store, namespace_id)
        .await
        .expect("read wal floor")
        .envelope
        .state
        .floor_seq
}

/// Checkpoints, then folds every L0 run into the base through
/// reorganization units, returning the resulting current manifest id. The
/// old synchronous rebuild produced this shape in one checkpoint call;
/// tests that need a compacted base with a specific segmentation policy use
/// this instead.
async fn checkpoint_then_reorganize<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> ManifestId {
    create_checkpoint(store, namespace_id, context)
        .await
        .expect("create checkpoint");
    drain_reorganization(store, namespace_id, context, policy).await
}

/// Runs reorganization units until nothing is left to fold, with the
/// trigger forced so even one L0 run folds.
async fn drain_reorganization<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> ManifestId {
    let fold_policy = MetadataLsmPolicy {
        max_l0_runs: 1,
        ..policy
    };
    loop {
        let report = super::reorganize_metadata_step(store, namespace_id, context, fold_policy)
            .await
            .expect("reorganization step");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { .. }
            | super::MetadataReorganizeOutcome::Superseded => continue,
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
        }
    }
    read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state
        .manifest_id
}

async fn current_manifest_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ManifestId {
    read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state
        .manifest_id
}

async fn current_manifest_object_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ManifestObjectId {
    read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state
        .manifest_object_id
}

async fn current_manifest_key<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> String {
    metadata_manifest_object(
        namespace_id.as_str(),
        &current_manifest_object_id(store, namespace_id).await,
    )
}

fn manifest_object_id(manifest_id: ManifestId) -> ManifestObjectId {
    ManifestObjectId::parse(format!("{:020}-0123456789abcdef", manifest_id.0))
        .expect("valid manifest object id")
}

async fn load_current_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<CurrentProjection, CoreError> {
    let (head, metadata_state) =
        load_checkpoint_projection_metadata_state(store, namespace_id).await?;
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .envelope
        .state;
    Ok(CurrentProjection {
        head,
        root,
        metadata_state,
    })
}

async fn build_manifest_from_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &CurrentProjection,
    context: &MutationContext,
    manifest_id: ManifestId,
) -> NamespaceManifestEnvelope {
    build_namespace_manifest_from_metadata_state(
        store,
        namespace_id,
        ManifestMetadataSource {
            head: &projection.head,
            basis_manifest_id: Some(projection.root.manifest_id),
            retention_floor_seq: read_floor_seq(store, namespace_id).await,
            metadata_state: &projection.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        manifest_id,
    )
    .await
    .expect("build manifest")
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

    assert_eq!(after.root.manifest_id, checkpoint.manifest_id);
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

    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    // The bootstrap manifest already covers the head: pinning writes a
    // record against it instead of materializing a new manifest.
    assert_eq!(checkpoint.manifest_id, ManifestId(0));
    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization.root.manifest_id, ManifestId(0));
    let record = read_checkpoint_record(&store, &namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert!(CheckpointId::parse(record.checkpoint_id.as_str()).is_ok());
    assert_eq!(record.manifest_head_seq, ChangeSeq(0));
    assert_eq!(record.manifest_id, ManifestId(0));
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

    let manifest_key = current_manifest_key(&store, &namespace_id).await;
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
    let manifest_key = format!(
        "{}{:020}-",
        metadata_manifest_prefix(namespace_id.as_str()),
        1
    );
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
    assert_eq!(materialization.root.manifest_id, ManifestId(0));
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

    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(1));
    assert_eq!(
        store
            .list_prefix(&format!(
                "namespaces/{}/wal/segments/",
                namespace_id.as_str()
            ))
            .await
            .expect("list wal")
            .len(),
        1
    );
    assert!(store
        .head(&current_manifest_key(&store, &namespace_id).await)
        .await
        .expect("manifest head")
        .is_some());
}

#[tokio::test]
async fn checkpoint_publication_preserves_writer_identity() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    acquire_writer_epoch(&store, &namespace_id, &context)
        .await
        .expect("acquire writer");
    let before = load_current_projection(&store, &namespace_id)
        .await
        .expect("load before checkpoint")
        .head;

    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let after = load_current_projection(&store, &namespace_id)
        .await
        .expect("load after checkpoint")
        .head;

    assert_eq!(after.writer_epoch, before.writer_epoch);
    assert_eq!(after.writer, before.writer);
}

#[tokio::test]
async fn retention_floor_advancement_preserves_writer_identity() {
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
    let before = load_current_projection(&store, &namespace_id)
        .await
        .expect("load before retention")
        .head;

    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention");
    let after = load_current_projection(&store, &namespace_id)
        .await
        .expect("load after retention")
        .head;

    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(1));
    // Floor advancement no longer touches the head at all.
    assert_eq!(after, before);
}

#[tokio::test]
async fn maintenance_does_not_make_orphan_wal_visible() {
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
        "/docs/first.txt",
        b"first\n",
        &context,
        None,
    )
    .await
    .expect("write first");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
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

    let orphan_key = wal_segment(
        namespace_id.as_str(),
        "00000000000000000002-deadbeefdeadbeef",
    );
    store
        .put_overwrite(&orphan_key, Bytes::from_static(b"not a wal envelope"))
        .await
        .expect("write orphan wal");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention");

    let head_after = read_head_object(&store, &namespace_id)
        .await
        .expect("read head")
        .envelope
        .state;
    assert_eq!(head_after.seq, ChangeSeq(2));

    let changes = list_changes_after(
        &store,
        &namespace_id,
        ChangeSeq(1),
        EffectiveLimit::new(NonZeroU32::new(10).expect("nonzero")),
    )
    .await
    .expect("list changes");

    assert_eq!(changes.through_seq, ChangeSeq(2));
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].seq, ChangeSeq(2));
}

#[tokio::test]
async fn retention_floor_does_not_advance_past_missing_metadata_segment() {
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
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_id)
            .await
            .expect("load manifest");
    let missing_key = materialized
        .manifest
        .payload
        .metadata_files
        .first()
        .expect("metadata file")
        .object_key
        .clone();
    store.delete(&missing_key).await.expect("delete segment");

    let error = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect_err("floor must not advance past missing segment");
    assert!(
        matches!(error, CoreError::CheckpointUnavailable(_)),
        "unexpected error: {error:?}"
    );
    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(0));
}

#[tokio::test]
async fn create_checkpoint_revives_a_dead_record_for_a_verified_basis() {
    // A record left dead by a failed verification is revived, not
    // duplicated, when the same basis verifies later.
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
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");
    let first = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    crate::checkpoint::record::set_checkpoint_record_state(
        &store,
        &namespace_id,
        &first.checkpoint_id,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Released,
        &context.writer_version,
    )
    .await
    .expect("mark dead");

    let revived = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("recreate checkpoint");
    assert_eq!(revived.checkpoint_id, first.checkpoint_id);
    let record = read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(
        record.state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Active
    );
}

#[tokio::test]
async fn checkpoint_verification_rejects_a_basis_below_the_floor() {
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
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance floor");

    // The bootstrap basis (seq 0) now sits below the floor (seq 1): a
    // record pinned to it must fail post-write verification.
    let stale = loonfs_api::wire::control::CheckpointRecordState {
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        namespace_id: namespace_id.clone(),
        manifest_id: ManifestId(0),
        manifest_object_id: manifest_object_id(ManifestId(0)),
        manifest_head_seq: ChangeSeq(0),
        manifest_payload_checksum: "sha256:stale".to_owned(),
        head_commit_id: CommitId::parse("c_00000000000000000000000000000000").expect("commit id"),
        created_at_ms: context.now_ms,
        expires_at_ms: None,
        owner: loonfs_api::wire::control::CheckpointOwner::User {
            name: "test-pin".to_owned(),
        },
        state: loonfs_api::wire::control::CheckpointRecordLifecycle::Active,
    };
    let verified = super::record::verify_checkpoint_basis(&store, &stale)
        .await
        .expect("verification runs");
    assert!(!verified, "sub-floor basis must not verify");
}

#[tokio::test]
async fn checkpoint_records_are_standalone_files_deduplicated_per_basis() {
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
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");
    let first = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    // Re-creating for the same pinned basis returns the existing record
    // instead of stacking a duplicate file.
    let repeated = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("repeat checkpoint");
    assert_eq!(repeated, first);

    let record = read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(record.manifest_id, first.manifest_id);
    assert_eq!(record.manifest_head_seq, first.checkpoint_seq);
    assert_eq!(
        record.state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Active
    );

    // A new basis mints a new record; both files exist side by side.
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/file-2.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write 2");
    let second = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("second checkpoint");
    assert_ne!(second.checkpoint_id, first.checkpoint_id);
    assert!(
        read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read first record")
            .is_some()
    );
}

#[tokio::test]
async fn base_rebuild_drops_commit_receipts_below_retention_floor() {
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
        "/docs/one.txt",
        b"one\n",
        &context,
        None,
    )
    .await
    .expect("write one");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/two.txt",
        b"two\n",
        &context,
        None,
    )
    .await
    .expect("write two");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let advanced = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance floor");
    assert_eq!(advanced.retention_floor_seq, ChangeSeq(2));

    let mut last_manifest_id = None;
    for round in 0..9u32 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{round}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write");
        let checkpoint = create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
        last_manifest_id = Some(checkpoint.manifest_id);
    }
    // Checkpoints only append; dropping happens when reorganization folds
    // the runs against the advanced floor.
    let _ = last_manifest_id.expect("manifest id");
    let reorganized_manifest_id = drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        reorganized_manifest_id,
    )
    .await
    .expect("load manifest");
    let receipts = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataTableFamily::CommitReceipts,
    );
    assert!(!receipts.is_empty());
    for row in &receipts {
        if let MetadataRow::CommitReceipt { committed_seq, .. } = row {
            assert!(
                *committed_seq >= ChangeSeq(2),
                "receipt below floor survived: {committed_seq:?}"
            );
        }
    }
    assert!(receipts.iter().any(|row| matches!(
        row,
        MetadataRow::CommitReceipt { committed_seq, .. } if *committed_seq == ChangeSeq(2)
    )));
}

#[test]
fn drop_pass_keeps_the_floor_visible_binding_across_a_later_rename() {
    use std::collections::BTreeMap;
    let bind = |seq: u64, delta: u32| MetadataRow::DirentryBind {
        parent_inode_id: InodeId(1),
        name_key: NameKey::parse("docs").expect("valid name key"),
        display_name: "docs".to_owned(),
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(seq),
        bind_delta_index: delta,
    };
    let unbind = |bind_seq: u64, delta: u32, unbind_seq: u64| MetadataRow::DirentryUnbind {
        parent_inode_id: InodeId(1),
        name_key: NameKey::parse("docs").expect("valid name key"),
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(bind_seq),
        bind_delta_index: delta,
        unbind_seq: ChangeSeq(unbind_seq),
        unbind_delta_index: 0,
    };
    let mut rows = BTreeMap::new();
    // bind at seq 1 is visible at the floor (1); the rename that supersedes
    // it happens above the floor, so bind, unbind, and replacement all stay.
    rows.insert(
        ApiMetadataTableFamily::DirentryBinds,
        vec![bind(1, 0), bind(2, 1)],
    );
    rows.insert(
        ApiMetadataTableFamily::DirentryChildBinds,
        vec![bind(1, 0), bind(2, 1)],
    );
    rows.insert(
        ApiMetadataTableFamily::DirentryUnbinds,
        vec![unbind(1, 0, 2)],
    );

    super::reorganize::drop_rows_below_retention_floor(&mut rows, ChangeSeq(1)).expect("drop");

    assert_eq!(rows[&ApiMetadataTableFamily::DirentryBinds].len(), 2);
    assert_eq!(rows[&ApiMetadataTableFamily::DirentryChildBinds].len(), 2);
    assert_eq!(rows[&ApiMetadataTableFamily::DirentryUnbinds].len(), 1);
}

#[test]
fn drop_pass_resolves_same_seq_rebinds_by_delta_index() {
    use std::collections::BTreeMap;
    let bind = |delta: u32| MetadataRow::DirentryBind {
        parent_inode_id: InodeId(1),
        name_key: NameKey::parse("docs").expect("valid name key"),
        display_name: "docs".to_owned(),
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(1),
        bind_delta_index: delta,
    };
    let unbind = MetadataRow::DirentryUnbind {
        parent_inode_id: InodeId(1),
        name_key: NameKey::parse("docs").expect("valid name key"),
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(1),
        bind_delta_index: 0,
        unbind_seq: ChangeSeq(1),
        unbind_delta_index: 1,
    };
    let mut rows = BTreeMap::new();
    rows.insert(
        ApiMetadataTableFamily::DirentryBinds,
        vec![bind(0), bind(2)],
    );
    rows.insert(
        ApiMetadataTableFamily::DirentryChildBinds,
        vec![bind(0), bind(2)],
    );
    rows.insert(ApiMetadataTableFamily::DirentryUnbinds, vec![unbind]);

    super::reorganize::drop_rows_below_retention_floor(&mut rows, ChangeSeq(1)).expect("drop");

    // Only the delta-2 rebind (the slot's latest) survives; the superseded
    // delta-0 bind and its spent unbind marker are gone from both families.
    for family in [
        ApiMetadataTableFamily::DirentryBinds,
        ApiMetadataTableFamily::DirentryChildBinds,
    ] {
        let kept = &rows[&family];
        assert_eq!(kept.len(), 1);
        assert!(matches!(
            kept[0],
            MetadataRow::DirentryBind {
                bind_delta_index: 2,
                ..
            }
        ));
    }
    assert!(rows[&ApiMetadataTableFamily::DirentryUnbinds].is_empty());
}

#[test]
fn drop_pass_refuses_superseded_bind_without_unbind() {
    use std::collections::BTreeMap;
    let bind = |delta: u32| MetadataRow::DirentryBind {
        parent_inode_id: InodeId(1),
        name_key: NameKey::parse("docs").expect("valid name key"),
        display_name: "docs".to_owned(),
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(1),
        bind_delta_index: delta,
    };
    let mut rows = BTreeMap::new();
    rows.insert(
        ApiMetadataTableFamily::DirentryBinds,
        vec![bind(0), bind(1)],
    );

    let error = super::reorganize::drop_rows_below_retention_floor(&mut rows, ChangeSeq(1))
        .expect_err("superseded live bind must refuse the drop");
    assert!(matches!(error, CoreError::NamespaceCorrupt(_)));
}

#[tokio::test]
async fn restore_below_the_floor_fails_with_revision_not_found() {
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
        "/docs/a.txt",
        b"one\n",
        &context,
        None,
    )
    .await
    .expect("write one");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"two\n",
        &context,
        None,
    )
    .await
    .expect("write two");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance floor");
    for round in 0..9u32 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{round}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write");
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
    }
    // The superseded revision is reclaimed when reorganization folds the
    // runs against the advanced floor, not at checkpoint time.
    drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;

    let error = restore_file_revision(
        &store,
        &namespace_id,
        "/docs/a.txt",
        RevisionNo(1),
        &context,
        None,
    )
    .await
    .expect_err("restoring a reclaimed revision must fail cleanly");
    assert_eq!(error.code(), crate::error::ErrorCode::RevisionNotFound);
}

#[tokio::test]
async fn base_rebuild_drops_revisions_superseded_below_floor() {
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
        "/docs/a.txt",
        b"one\n",
        &context,
        None,
    )
    .await
    .expect("write one");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"two\n",
        &context,
        None,
    )
    .await
    .expect("write two");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance floor");

    let mut last_manifest_id = None;
    for round in 0..9u32 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{round}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write");
        let checkpoint = create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
        last_manifest_id = Some(checkpoint.manifest_id);
    }
    // Checkpoints only append; dropping happens when reorganization folds
    // the runs against the advanced floor.
    let _ = last_manifest_id.expect("manifest id");
    let reorganized_manifest_id = drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        reorganized_manifest_id,
    )
    .await
    .expect("load manifest");
    let revisions = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataTableFamily::Revisions,
    );
    let digest_one = loonfs_api::sha256_digest(b"one\n");
    let digest_two = loonfs_api::sha256_digest(b"two\n");
    assert!(!revisions.iter().any(|row| matches!(
        row,
        MetadataRow::Revision { content_ref, .. } if content_ref.digest == digest_one
    )));
    assert!(revisions.iter().any(|row| matches!(
        row,
        MetadataRow::Revision { content_ref, .. } if content_ref.digest == digest_two
    )));
}

#[tokio::test]
async fn base_rebuild_drops_bindings_unbound_below_floor() {
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
        "/docs/tmp.txt",
        b"scratch\n",
        &context,
        None,
    )
    .await
    .expect("write tmp");
    delete_path(&store, &namespace_id, "/docs/tmp.txt", &context, None)
        .await
        .expect("delete tmp");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance floor");

    let mut last_manifest_id = None;
    for round in 0..9u32 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{round}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write");
        let checkpoint = create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
        last_manifest_id = Some(checkpoint.manifest_id);
    }
    // Checkpoints only append; dropping happens when reorganization folds
    // the runs against the advanced floor.
    let _ = last_manifest_id.expect("manifest id");
    let reorganized_manifest_id = drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        reorganized_manifest_id,
    )
    .await
    .expect("load manifest");
    let binds = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataTableFamily::DirentryBinds,
    );
    assert!(!binds.iter().any(|row| matches!(
        row,
        MetadataRow::DirentryBind { display_name, .. } if display_name == "tmp.txt"
    )));
    let unbinds = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataTableFamily::DirentryUnbinds,
    );
    assert!(
        unbinds.is_empty(),
        "spent unbind markers survived: {unbinds:?}"
    );
}

#[tokio::test]
async fn publish_backpressure_rejects_when_wal_tail_outruns_maintenance() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let limit = u32::try_from(crate::commit_engine::WAL_TAIL_BACKPRESSURE_SEGMENTS).expect("limit");
    for round in 0..=limit {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{round}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write within backpressure window");
    }

    let error = write_file_bytes(
        &store,
        &namespace_id,
        "/docs/one-too-many.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect_err("publish past the backpressure limit must be rejected");
    assert_eq!(error.code(), ErrorCode::MaintenanceRequired);

    // Reads never gate: the change feed still serves the whole tail.
    let changes = list_changes_after(
        &store,
        &namespace_id,
        ChangeSeq(0),
        EffectiveLimit::new(NonZeroU32::new(200).expect("nonzero")),
    )
    .await
    .expect("list changes");
    assert_eq!(changes.through_seq, ChangeSeq(129));
}

#[tokio::test]
async fn base_rebuild_rejects_divergent_revision_index() {
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

    // Count-neutral divergence: same row count, different content, so only
    // row-level index equality can catch it.
    let (_, mut manifest, mut revision_index_rows) =
        revision_index_test_materialization(&store, &namespace_id, &context).await;
    let first = revision_index_rows.first_mut().expect("revision index row");
    if let MetadataRow::Revision { content_ref, .. } = first {
        content_ref.digest =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111".to_owned();
    }
    rewrite_revision_index_segment(&store, &namespace_id, &mut manifest, revision_index_rows).await;
    overwrite_manifest(&store, &namespace_id, manifest).await;

    // L0 appends never re-read the base run; the reorganization merge that
    // folds every run back together is the production point that must
    // reject it.
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/another.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("l0 checkpoint");
    let fold_policy = MetadataLsmPolicy {
        max_l0_runs: 1,
        ..MetadataLsmPolicy::default()
    };
    let mut rebuild_error = None;
    for _unit in 0..8 {
        match super::reorganize_metadata_step(&store, &namespace_id, &context, fold_policy).await {
            Ok(report) => match report.outcome {
                super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
                _ => continue,
            },
            Err(error) => {
                rebuild_error = Some(error);
                break;
            }
        }
    }
    match rebuild_error.expect("reorganization should reject the divergent index") {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::RevisionIndexMismatch { .. },
        )) => {}
        other => panic!("expected revision index mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn manifest_load_rejects_unequal_index_descriptor_counts() {
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
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    // Tamper only the descriptor's row_count for the revision index family;
    // the per-run count-equality check must reject the manifest at load.
    let mut manifest =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_id)
            .await
            .expect("load manifest")
            .manifest;
    let descriptor = manifest
        .payload
        .metadata_files
        .iter_mut()
        .find(|file| file.family == ApiMetadataTableFamily::RevisionsByInodeDesc)
        .expect("revision index descriptor");
    descriptor.row_count += 1;
    let manifest_object_id = manifest.payload.manifest_object_id.clone();
    overwrite_manifest(&store, &namespace_id, manifest).await;

    match load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id).await {
        Err(ManifestLoadError::RunManifestMismatch { .. }) => {}
        Err(other) => panic!("expected run manifest mismatch, got {other:?}"),
        Ok(_) => panic!("tampered descriptor counts must not load"),
    }
}

#[tokio::test]
async fn retention_advance_aborts_when_current_manifest_changes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let plain = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&plain, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &plain,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&plain, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    // Lose the floor CAS once while a competing compactor swaps the root to
    // a same-seq replacement mid-flight. Root updates are monotonic, so the
    // replacement covers at least the derived floor and the retried publish
    // is safe to complete.
    let store = ManifestSwapOnCasConflictStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("store"),
        namespace_id: namespace_id.clone(),
        remaining_conflicts: std::sync::atomic::AtomicUsize::new(1),
    };
    let response = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("floor advance survives a monotonic root swap");
    assert_eq!(response.retention_floor_seq, ChangeSeq(1));
    assert_eq!(
        read_floor_seq(&store.inner, &namespace_id).await,
        ChangeSeq(1)
    );
}

#[derive(Debug)]
struct ManifestSwapOnCasConflictStore {
    inner: LocalFsStore,
    namespace_id: NamespaceId,
    remaining_conflicts: std::sync::atomic::AtomicUsize,
}

impl ManifestSwapOnCasConflictStore {
    async fn install_competing_manifest_id(&self) {
        let loaded = read_metadata_root_object(&self.inner, &self.namespace_id)
            .await
            .expect("read root for swap");
        let mut root = loaded.envelope.state;
        // Same-seq replacement referencing a different manifest: the shape a
        // pure compaction publishes.
        root.manifest_id = ManifestId(root.manifest_id.0 + 1);
        let envelope = loonfs_api::wire::control::MetadataRootEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::MetadataRoot,
            "test-writer/0.1.0",
            root,
        )
        .expect("root envelope");
        let bytes =
            loonfs_api::wire::control::encode_control_object(&envelope).expect("root bytes");
        self.inner
            .put(
                &loonfs_objectstore::keys::metadata_root(self.namespace_id.as_str()),
                Bytes::from(bytes),
                PutMode::Overwrite,
            )
            .await
            .expect("install competing head");
    }
}

#[async_trait]
impl ObjectStore for ManifestSwapOnCasConflictStore {
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
        self.inner.put(key, bytes, mode).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        use std::sync::atomic::Ordering;
        if self.remaining_conflicts.load(Ordering::SeqCst) > 0 {
            self.remaining_conflicts.fetch_sub(1, Ordering::SeqCst);
            self.install_competing_manifest_id().await;
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        self.inner.compare_and_swap(key, expected_etag, bytes).await
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
    let reorganized_manifest_id = drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        reorganized_manifest_id,
    )
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

    // Checkpoints only append: the base run stays the bootstrap seed's and
    // each checkpoint contributes one L0 run.
    assert_eq!(
        second_materialized.manifest.payload.base_seq,
        first_materialized.manifest.payload.base_seq
    );
    assert_eq!(
        base_run(&second_materialized.manifest).tables,
        base_run(&first_materialized.manifest).tables
    );
    let l0_runs = l0_runs(&second_materialized.manifest);
    assert_eq!(l0_runs.len(), 2);
    assert_eq!(l0_runs[0].run_seq, first.checkpoint_seq);
    assert_eq!(l0_runs[1].run_seq, second.checkpoint_seq);
    assert!(l0_runs
        .iter()
        .all(|run| run.level == CHECKPOINT_L0_RUN_LEVEL));
    for response in [&first, &second] {
        let record = read_checkpoint_record(&store, &namespace_id, &response.checkpoint_id)
            .await
            .expect("read checkpoint record")
            .expect("record exists")
            .state;
        assert_eq!(record.manifest_id, response.manifest_id);
    }
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
    let mut bad_base_manifest = first_materialized.manifest.clone();
    let manifest_key = metadata_manifest_object(
        namespace_id.as_str(),
        &bad_base_manifest.payload.manifest_object_id,
    );
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
        let expected = metadata_table(
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
    let mut bad_l0_manifest = second_materialized.manifest.clone();
    let manifest_key = metadata_manifest_object(
        namespace_id.as_str(),
        &bad_l0_manifest.payload.manifest_object_id,
    );
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
        let expected = metadata_table(
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
            manifest_object_id: manifest_object_id(manifest_id(materialization.head.seq)),
            head_seq: materialization.head.seq,
            head_commit_id: materialization.head.head_commit_id.clone(),
            base_seq: first,
            writer_epoch: materialization.head.writer_epoch,
            next_inode_id: materialization.head.next_inode_id,
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            initialized: true,
            verified: true,
            fork: None,
            features: BTreeMap::new(),
            metadata_files,
            index_files: Vec::new(),
        },
    )
    .expect("build malformed manifest");

    match load_manifest_metadata_state_for_inspection_from_manifest(
        &store,
        &namespace_id,
        &metadata_manifest_object(namespace_id.as_str(), &manifest.payload.manifest_object_id),
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
    // Always-append checkpoints keep the seed base (seq 0) and chain one
    // L0 run per checkpoint, the first included.
    assert_eq!(materialized.manifest.payload.base_seq, ChangeSeq(0));
    let l0_runs = l0_runs(&materialized.manifest);
    assert_eq!(l0_runs.len(), 4);
    for (offset, run) in l0_runs.iter().enumerate() {
        let seq = ChangeSeq(offset as u64 + 1);
        assert_eq!(run.run_seq, seq);
        assert_eq!(run.level, CHECKPOINT_L0_RUN_LEVEL);
    }
}

#[tokio::test]
async fn checkpoints_append_past_the_threshold_and_reorganization_drains() {
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
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    // Checkpoints never compact: well past the policy threshold every
    // delta run is still chained and the base is still the seed's.
    let appended = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_id(&store, &namespace_id).await,
    )
    .await
    .expect("load appended manifest");
    assert_eq!(l0_runs(&appended.manifest).len(), 4);
    assert_eq!(appended.manifest.payload.base_seq, ChangeSeq(0));

    // Reorganization folds one family group per unit, each publishing its
    // own manifest — the manifest chain is the progress record.
    let mut units = 0usize;
    let mut last_manifest_id = None;
    loop {
        let report = super::reorganize_metadata_step(&store, &namespace_id, &context, policy)
            .await
            .expect("reorganization step");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { manifest_id, .. } => {
                units += 1;
                if let Some(previous) = last_manifest_id {
                    assert!(manifest_id > previous, "units must advance the manifest");
                }
                last_manifest_id = Some(manifest_id);
            }
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
        }
    }
    assert!(units >= 2, "several family groups should fold, got {units}");

    let drained = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_id(&store, &namespace_id).await,
    )
    .await
    .expect("load drained manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(l0_runs(&drained.manifest).is_empty());
    assert_eq!(drained.manifest.payload.base_seq, ChangeSeq(4));
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &drained.metadata_state
    ));
}

#[tokio::test]
async fn manifest_rejects_segment_whose_index_fails_its_descriptor_checksum() {
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

    let reorganized_manifest_id = checkpoint_then_reorganize(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        reorganized_manifest_id,
    )
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
    // The descriptor is the only description of a segment; its index CRC
    // is what binds the manifest to the object's exact bytes.
    descriptor.index_block.crc32c ^= 0xffff_ffff;

    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &manifest.payload.manifest_object_id);
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
        Err(ManifestLoadError::SegmentCodec { message, .. }) => {
            assert!(
                message.contains("checksum"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected index checksum rejection, got {other:?}"),
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

    let manifest_id = checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id)
            .await
            .expect("load manifest");

    let base = base_run(&materialized.manifest);
    let revisions = base
        .tables
        .iter()
        .find(|table| table.family == ApiMetadataTableFamily::Revisions)
        .expect("revision table");
    assert!(revisions.segments.len() >= 3);

    let direntries = base
        .tables
        .iter()
        .find(|table| table.family == ApiMetadataTableFamily::DirentryBinds)
        .expect("direntry table");
    assert!(
        direntries.segments.len() >= 3,
        "hot directory direntry rows should be range-split"
    );

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
async fn byte_budgeted_cache_admits_large_table_scans() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        MetadataSstGetCountingStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
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
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    // The default cache config carries a decoded-byte budget, so a scan wider
    // than the small-scan limit populates the cache instead of reading through.
    let cache = super::MetadataTableCache::new(Default::default());
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    let revisions = tables
        .scan_prefix(ApiMetadataTableFamily::Revisions, "revision-")
        .await
        .expect("scan revisions");
    let after_first = cache.stats();
    assert!(revisions.len() >= 8);
    assert!(
        after_first.inserts >= 8,
        "a wide scan against a byte-budgeted cache should admit every segment"
    );

    // A fresh view has no per-view segment memo; only the shared cache can
    // answer, so the repeated scan must issue no segment fetches.
    let fresh_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load fresh tables");
    store.reset_metadata_sst_gets();
    let repeated = fresh_tables
        .scan_prefix(ApiMetadataTableFamily::Revisions, "revision-")
        .await
        .expect("repeated scan");
    let after_repeat = cache.stats();

    assert_eq!(repeated, revisions);
    assert_eq!(
        store.metadata_sst_gets(),
        0,
        "a warm wide scan should be served entirely from the cache"
    );
    assert!(after_repeat.hits > after_first.hits);
}

#[tokio::test]
async fn concurrent_scans_share_one_fetch_per_segment() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        MetadataSstGetCountingStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
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
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    // Concurrent scans over one shared cache must not multiply fetches:
    // single-flight covers blocks racing before the first insert lands, and
    // population covers everything after.
    let cache = super::MetadataTableCache::new(MetadataTableCacheConfig::default());
    // A solo pass over its own cold cache measures the true per-scan
    // fetch count.
    let solo_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load solo tables");
    store.reset_metadata_sst_gets();
    let solo = solo_tables
        .scan_prefix(ApiMetadataTableFamily::Revisions, "revision-")
        .await
        .expect("solo scan");
    let solo_fetches = store.metadata_sst_gets();
    assert!(solo.len() >= 8);
    assert!(solo_fetches >= 8, "solo scan should fetch every segment");

    // Concurrent requests race over a second cold cache, each with its own
    // tables view; single-flight is what keeps the pair at the solo count.
    let paired_cache = super::MetadataTableCache::new(MetadataTableCacheConfig::default());
    let first_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&paired_cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load first tables");
    let second_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&paired_cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load second tables");
    store.reset_metadata_sst_gets();
    let (first, second) = tokio::join!(
        first_tables.scan_prefix(ApiMetadataTableFamily::Revisions, "revision-"),
        second_tables.scan_prefix(ApiMetadataTableFamily::Revisions, "revision-"),
    );
    let paired_fetches = store.metadata_sst_gets();
    assert_eq!(first.expect("first scan"), second.expect("second scan"));
    assert_eq!(
        paired_fetches, solo_fetches,
        "concurrent scans over one shared cache should share one fetch per segment"
    );
}

#[tokio::test]
async fn cached_manifest_carries_its_scan_order_runs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..4 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create first checkpoint");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/tail.txt",
        b"tail\n",
        &context,
        None,
    )
    .await
    .expect("write tail file");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create second checkpoint");
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;

    let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
    let first = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load first tables");
    let second = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load second tables");

    assert!(
        first.scan_runs.len() >= 2,
        "two checkpoints should leave more than one run to order"
    );
    assert_eq!(
        *first.scan_runs,
        runs_in_scan_order(&first.manifest().payload),
        "the cached run list must equal the manifest's scan-order grouping"
    );
    assert!(
        Arc::ptr_eq(&first.scan_runs, &second.scan_runs),
        "views over one cached manifest should share one derived run list"
    );
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
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    write_file_bytes(&store, &namespace_id, "/docs/b.txt", b"b\n", &context, None)
        .await
        .expect("write b");
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        None,
        &namespace_id,
        &manifest_object_id,
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
async fn byte_budgeted_cache_admits_large_range_scans() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        MetadataSstGetCountingStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
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
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let cache = super::MetadataTableCache::new(Default::default());
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    // The list preload path: one page-shaped range scan over a directory
    // whose bind rows span more segments than the small-scan limit.
    let docs_inode_id = InodeId(2);
    let lower_bound = format!("direntry-{:020}-", docs_inode_id.0);
    let upper_bound = super::string_prefix_upper_bound(&lower_bound);
    let page = tables
        .scan_range_page(
            ApiMetadataTableFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            8,
        )
        .await
        .expect("scan range page");
    let after_first = cache.stats();
    assert_eq!(page.len(), 8);
    assert!(
        after_first.inserts > 4,
        "a wide range scan should admit its segments to the cache"
    );

    let fresh_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load fresh tables");
    store.reset_metadata_sst_gets();
    let repeated = fresh_tables
        .scan_range_page(
            ApiMetadataTableFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            8,
        )
        .await
        .expect("repeated scan range page");

    assert_eq!(repeated, page);
    assert_eq!(
        store.metadata_sst_gets(),
        0,
        "a warm range scan should be served entirely from the cache"
    );
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
    let manifest_id = checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
    let before = cache.stats();

    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id)
            .await
            .expect("load materialized manifest");
    let after = cache.stats();

    assert!(flatten_manifest_tables(base_run(&materialized.manifest).tables).len() > 1);
    assert_eq!(after, before);
}

#[tokio::test]
async fn lookup_skips_segments_whose_filter_rules_the_name_out() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // "beta" lands in the base run; a second checkpoint puts "alpha" and
    // "gamma" in an L0 run. The L0 bind segment's key range then straddles
    // "beta", so min/max pruning cannot exclude it — only its bloom filter
    // can prove the name absent.
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/beta.txt",
        b"beta\n",
        &context,
        None,
    )
    .await
    .expect("write beta");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("first checkpoint");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha\n",
        &context,
        None,
    )
    .await
    .expect("write alpha");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/gamma.txt",
        b"gamma\n",
        &context,
        None,
    )
    .await
    .expect("write gamma");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("second checkpoint");

    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    // Resolve /docs, then look up beta's bind under it through the filtered
    // scan the visibility adapter uses.
    let docs_binds = tables
        .scan_prefix(
            ApiMetadataTableFamily::DirentryBinds,
            "direntry-00000000000000000001-",
        )
        .await
        .expect("scan root binds");
    let docs_inode = docs_binds
        .iter()
        .find_map(|row| match row {
            MetadataRow::DirentryBind { child_inode_id, .. } => Some(*child_inode_id),
            _ => None,
        })
        .expect("docs directory bind");
    let encoded_name = loonfs_api::wire::manifest::hex_encode_row_key_component("beta.txt");
    let filter_probe = format!("direntry-{:020}-{encoded_name}", docs_inode.0);
    let prefix = format!("{filter_probe}-");
    let rows = tables
        .scan_prefix_for_lookup(
            ApiMetadataTableFamily::DirentryBinds,
            &prefix,
            &filter_probe,
            false,
        )
        .await
        .expect("filtered lookup");

    assert_eq!(rows.len(), 1, "beta's bind should still be found");
    let stats = cache.stats();
    assert!(
        stats.filter_skips >= 1,
        "the L0 bind segment should be skipped by its filter, stats: {stats:?}"
    );
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
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let cache = MetadataTableCache::new(MetadataTableCacheConfig {
        max_decoded_bytes: 1,
    });
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    let key = "inode-00000000000000000001";
    assert!(tables
        .get_for_lookup(ApiMetadataTableFamily::Inodes, key, key)
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

    let first_manifest_id =
        checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first_manifest_id)
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
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
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
    let compacted = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("compacted checkpoint");
    let compacted_manifest_id = drain_reorganization(&store, &namespace_id, &context, policy).await;
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted_manifest_id)
            .await
            .expect("load compacted manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let compacted_run_keys = run_segment_object_keys(&compacted_materialized.manifest);
    let compacted_run_prefix = format!("namespaces/{}/metadata/tables/tbl_", namespace_id.as_str());

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

    let first_manifest_id =
        checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first_manifest_id)
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
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
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
    let _compacted = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("compacted checkpoint");
    let compacted_manifest_id = drain_reorganization(&store, &namespace_id, &context, policy).await;
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted_manifest_id)
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
    // Groups untouched since the first fold keep their older base run, so
    // several base runs may coexist; what matters is that no delta remains.
    assert!(
        runs_from_metadata_files(&compacted_materialized.manifest.payload)
            .iter()
            .all(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
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

    let manifest_id = checkpoint_then_reorganize(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id)
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
    match load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id).await {
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

    let manifest_id = checkpoint_then_reorganize(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id)
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
    )
    .await;

    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &manifest.payload.manifest_object_id);
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
    _run_seq: ChangeSeq,
    family: ApiMetadataTableFamily,
    descriptor: &mut MetadataFileRef,
    rows: Vec<MetadataRow>,
) {
    let mut builder = SegmentBlocksBuilder::default();
    for row in &rows {
        let row_key = row.row_key_for_family(family);
        let filter_key = row.filter_key_for_family(family);
        builder
            .push(&row_key, &filter_key, row)
            .expect("rewritten rows should encode");
    }
    let built = builder.finish().expect("rewritten segment");
    store
        .put_overwrite(&descriptor.object_key, Bytes::from(built.bytes.clone()))
        .await
        .expect("overwrite segment");

    descriptor.row_count = built.row_count;
    descriptor.min_key = built.min_key;
    descriptor.max_key = built.max_key;
    descriptor.index_block = built.index;
    descriptor.filter_block = built.filter;
    descriptor.payload_checksum = loonfs_api::sha256_digest(&built.bytes);
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
    rewrite_revision_index_segment(&store, &namespace_id, &mut manifest, revision_index_rows).await;
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
    rewrite_revision_index_segment(&store, &namespace_id, &mut manifest, revision_index_rows).await;
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
    rewrite_revision_index_segment(&store, &namespace_id, &mut manifest, revision_index_rows).await;
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
    rewrite_revision_index_segment(&store, &namespace_id, &mut manifest, revision_index_rows).await;
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
    // The corruptions below target base-run segments, which only exist
    // once reorganization has folded the checkpoint's L0 runs.
    let manifest_id =
        checkpoint_then_reorganize(store, namespace_id, context, MetadataLsmPolicy::default())
            .await;
    let materialized =
        load_manifest_materialization_for_inspection(store, namespace_id, manifest_id)
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
    )
    .await;
}

async fn overwrite_manifest(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    manifest: NamespaceManifestEnvelope,
) {
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &manifest.payload.manifest_object_id);
    let manifest_id = manifest.payload.manifest_id;
    let manifest_object_id = manifest.payload.manifest_object_id.clone();
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(manifest.writer_version, manifest.payload)
            .expect("updated manifest");
    let manifest_bytes =
        encode_namespace_manifest_json(&updated_manifest).expect("encode updated manifest");
    store
        .put_overwrite(&manifest_key, Bytes::from(manifest_bytes))
        .await
        .expect("overwrite manifest");
    // Keep the tampered manifest consistent with the root's checksum pin, as
    // a well-formed-but-divergent publisher would: the point of these tests
    // is the deeper row-level guards, not the checksum pin.
    let loaded_root = read_metadata_root_object(store, namespace_id)
        .await
        .expect("read root");
    if loaded_root.envelope.state.manifest_id == manifest_id {
        let mut root = loaded_root.envelope.state;
        root.manifest_object_id = manifest_object_id;
        root.manifest_payload_checksum = updated_manifest.payload_checksum.clone();
        let envelope = loonfs_api::wire::control::MetadataRootEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::MetadataRoot,
            "test-writer/0.1.0",
            root,
        )
        .expect("root envelope");
        let bytes =
            loonfs_api::wire::control::encode_control_object(&envelope).expect("root bytes");
        store
            .put_overwrite(
                &loonfs_objectstore::keys::metadata_root(namespace_id.as_str()),
                Bytes::from(bytes),
            )
            .await
            .expect("overwrite root");
    }
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
async fn reorganization_resumes_from_the_manifest_after_interruption() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataLsmPolicy {
        max_l0_runs: 1,
        ..MetadataLsmPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 1..=3 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    // One unit runs, then the process "crashes": nothing is carried over
    // but the published manifest.
    let first_report = super::reorganize_metadata_step(&store, &namespace_id, &context, policy)
        .await
        .expect("first unit");
    let super::MetadataReorganizeOutcome::UnitPublished {
        families: first_families,
        ..
    } = first_report.outcome
    else {
        panic!("expected a published unit, got {:?}", first_report.outcome);
    };

    // A checkpoint lands in between, adding a fresh delta run.
    write_file_and_checkpoint(&store, &namespace_id, &context, 9).await;

    // A fresh sequence of steps reads the live manifest and finishes the
    // job, including the delta the interleaved checkpoint added.
    let mut folded_groups = vec![first_families];
    loop {
        let report = super::reorganize_metadata_step(&store, &namespace_id, &context, policy)
            .await
            .expect("resumed unit");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { families, .. } => {
                folded_groups.push(families);
            }
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
        }
    }
    assert!(folded_groups.len() >= 2);

    let drained = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_id(&store, &namespace_id).await,
    )
    .await
    .expect("load drained manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(l0_runs(&drained.manifest).is_empty());
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &drained.metadata_state
    ));
}

#[tokio::test]
async fn checkpoints_chain_l0_runs_past_the_default_cap() {
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

    // The default cap is a reorganization trigger, never a checkpoint
    // behavior: publication keeps appending regardless.
    let appended = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_id(&store, &namespace_id).await,
    )
    .await
    .expect("load appended manifest");
    assert!(l0_runs(&appended.manifest).len() > DEFAULT_MAX_CHECKPOINT_L0_RUNS);
    assert_eq!(appended.manifest.payload.base_seq, ChangeSeq(0));
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
            basis_manifest_id: Some(materialization_before.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization_before.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(2),
    )
    .await
    .expect("build orphan manifest");
    write_namespace_manifest(&store, &orphan_manifest)
        .await
        .expect("write orphan manifest");

    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization_after.root.manifest_id, first.manifest_id);
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
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
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
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
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

    let store = ConflictOnManifestCreateStore::mutate_next_inode(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        format!(
            "{}{:020}-",
            metadata_manifest_prefix(namespace_id.as_str()),
            1
        ),
    );

    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint should retry allocation");

    assert_eq!(checkpoint.manifest_id, ManifestId(1));
    let record = read_checkpoint_record(&store, &namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(record.manifest_id, ManifestId(1));
    let manifest_objects = store
        .list_prefix(&metadata_manifest_prefix(namespace_id.as_str()))
        .await
        .expect("list manifest objects");
    assert_eq!(
        manifest_objects.len(),
        3,
        "bootstrap, injected conflict, and retried checkpoint manifests should all be immutable"
    );
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization_after.root.manifest_id, ManifestId(1));
}

#[tokio::test]
async fn same_root_checkpoint_builders_write_distinct_manifest_objects_and_loser_is_superseded() {
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
    let first_manifest = build_manifest_from_projection(
        &store,
        &namespace_id,
        &materialization,
        &context,
        ManifestId(1),
    )
    .await;
    let second_manifest = build_manifest_from_projection(
        &store,
        &namespace_id,
        &materialization,
        &context,
        ManifestId(1),
    )
    .await;
    assert_eq!(
        first_manifest.payload.manifest_id,
        second_manifest.payload.manifest_id
    );
    assert_ne!(
        first_manifest.payload.manifest_object_id,
        second_manifest.payload.manifest_object_id
    );

    write_namespace_manifest(&store, &first_manifest)
        .await
        .expect("write first manifest");
    write_namespace_manifest(&store, &second_manifest)
        .await
        .expect("write second manifest");

    let first_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &first_manifest,
        &materialization.root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("first publication succeeds");
    match first_outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(
                root.manifest_object_id,
                first_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected first publication to win, got {other:?}"),
    }

    let second_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &second_manifest,
        &materialization.root.manifest_object_id,
        context.now_ms + 1,
        &context.writer_version,
    )
    .await
    .expect("second publication should yield to the published root");
    match second_outcome {
        ManifestPublicationOutcome::Superseded(root) => {
            assert_eq!(
                root.manifest_object_id,
                first_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected second publication to be superseded, got {other:?}"),
    }

    let manifest_objects = store
        .list_prefix(&metadata_manifest_prefix(namespace_id.as_str()))
        .await
        .expect("list manifest objects");
    assert_eq!(
        manifest_objects.len(),
        3,
        "bootstrap and both race manifests should remain distinct immutable objects"
    );
}

#[tokio::test]
async fn create_checkpoint_pins_a_current_basis_without_building_a_new_manifest() {
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
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
    )
    .await
    .expect("build manifest");
    write_namespace_manifest(&store, &manifest_without_checkpoint)
        .await
        .expect("write manifest");
    publish_metadata_root(
        &store,
        &namespace_id,
        &manifest_without_checkpoint,
        &materialization.root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("publish manifest");

    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    // With standalone records, pinning a basis that already covers the head
    // writes a checkpoint file against it instead of building a new
    // manifest.
    assert_eq!(checkpoint.manifest_id, ManifestId(1));
    let record = read_checkpoint_record(&store, &namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(record.manifest_id, ManifestId(1));
    assert_eq!(
        record.manifest_payload_checksum,
        manifest_without_checkpoint.payload_checksum
    );
}

#[tokio::test]
async fn a_view_reuses_decoded_blocks_without_a_shared_cache() {
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
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    let manifest_object_id = {
        checkpoint_then_reorganize(
            &store,
            &namespace_id,
            &context,
            MetadataLsmPolicy::default(),
        )
        .await;
        current_manifest_object_id(&store, &namespace_id).await
    };

    // The cold-boot shape: a view with no shared cache attached. Repeating
    // a lookup must not re-fetch — the per-view memo is the only reuse this
    // configuration has, and without it a cold list degrades from one fetch
    // per block to one fetch per lookup.
    let tables = super::load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load tables");
    store.reset_metadata_sst_gets();
    let key = "inode-00000000000000000001";
    assert!(tables
        .get_for_lookup(ApiMetadataTableFamily::Inodes, key, key)
        .await
        .expect("first lookup")
        .is_some());
    let first_lookup_gets = store.metadata_sst_gets();
    assert!(first_lookup_gets > 0, "a cold lookup fetches blocks");

    assert!(tables
        .get_for_lookup(ApiMetadataTableFamily::Inodes, key, key)
        .await
        .expect("repeated lookup")
        .is_some());
    let other = "inode-00000000000000000002";
    assert!(tables
        .get_for_lookup(ApiMetadataTableFamily::Inodes, other, other)
        .await
        .expect("second-key lookup")
        .is_some());
    assert_eq!(
        store.metadata_sst_gets(),
        first_lookup_gets,
        "later lookups through the same view should reuse decoded blocks"
    );
}

#[tokio::test]
async fn point_lookups_skip_inline_filtered_runs_without_fetches() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        MetadataSstGetCountingStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    // Three checkpoints append three L0 runs whose direntry key ranges
    // straddle one another (each run binds names from both ends of the
    // alphabet), so range pruning alone cannot narrow a name lookup below
    // several candidate segments — the shape a bulk-loaded directory's
    // unfolded backlog takes.
    for names in [["a.txt", "z.txt"], ["b.txt", "y.txt"], ["c.txt", "x.txt"]] {
        for name in names {
            write_file_bytes(
                &store,
                &namespace_id,
                &format!("/docs/{name}"),
                b"content\n",
                &context,
                None,
            )
            .await
            .expect("write file");
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
    }
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let tables = load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load tables");
    let direntry_descriptors: Vec<_> = tables
        .manifest()
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| descriptor.family == ApiMetadataTableFamily::DirentryBinds)
        .collect();
    assert!(direntry_descriptors.len() >= 3);
    assert!(
        direntry_descriptors
            .iter()
            .all(|descriptor| descriptor.filter_inline.is_some()),
        "small delta-run segments should inline their filters in the manifest"
    );

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        tables.manifest().payload.manifest_id,
    )
    .await
    .expect("materialize manifest");
    let binding = materialized
        .metadata_state
        .direntry_binds()
        .iter()
        .find(|binding| binding.name_key == "x.txt")
        .expect("binding for x.txt")
        .clone();
    let prefix = lookup_keys::direntry_bind_prefix(binding.parent_inode_id, &binding.name_key);
    let probe = lookup_keys::direntry_bind_probe(binding.parent_inode_id, &binding.name_key);

    store.reset_metadata_sst_gets();
    let rows = tables
        .scan_prefix_for_lookup(ApiMetadataTableFamily::DirentryBinds, &prefix, &probe, true)
        .await
        .expect("point lookup");
    assert_eq!(rows.len(), 1, "exactly one bind row for the probed name");
    assert_eq!(
        store.metadata_sst_gets(),
        1,
        "inline filters reject the other runs without fetches, and the one \
         admitted small segment loads whole with a single ranged GET"
    );

    // The same lookup against the same manifest with the inline copies
    // stripped must return the same rows through fetched filter blocks —
    // the inline copy is an accelerator, never an answer of its own.
    let mut stripped_payload = tables.manifest().payload.clone();
    stripped_payload.manifest_id = ManifestId(stripped_payload.manifest_id.0 + 1);
    stripped_payload.manifest_object_id = ManifestObjectId::generate(stripped_payload.manifest_id);
    for descriptor in &mut stripped_payload.metadata_files {
        descriptor.filter_inline = None;
    }
    let stripped_object_id = stripped_payload.manifest_object_id.clone();
    let stripped = NamespaceManifestEnvelope::from_payload("test-writer", stripped_payload)
        .expect("stripped manifest envelope");
    write_namespace_manifest(&store, &stripped)
        .await
        .expect("write stripped manifest");
    let stripped_tables = load_verified_manifest_tables(&store, &namespace_id, &stripped_object_id)
        .await
        .expect("load stripped tables");
    store.reset_metadata_sst_gets();
    let stripped_rows = stripped_tables
        .scan_prefix_for_lookup(ApiMetadataTableFamily::DirentryBinds, &prefix, &probe, true)
        .await
        .expect("point lookup without inline filters");
    assert_eq!(stripped_rows, rows);
    assert!(
        store.metadata_sst_gets() > 1,
        "without inline copies the ruled-out runs pay filter fetches"
    );
}

#[tokio::test]
async fn corrupt_inline_filter_fails_the_lookup() {
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
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let tables = load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load tables");
    let descriptor = tables
        .manifest()
        .payload
        .metadata_files
        .iter()
        .find(|descriptor| {
            descriptor.family == ApiMetadataTableFamily::DirentryBinds
                && descriptor.filter_inline.is_some()
        })
        .expect("inline-filtered direntry segment")
        .clone();

    // Flip one nibble of the inline copy: the handle's CRC no longer
    // matches, so the read must fail instead of consulting corrupt bits.
    let mut tampered = descriptor.clone();
    let mut inline = tampered.filter_inline.take().expect("inline filter");
    let flipped = if inline.ends_with('0') { '1' } else { '0' };
    inline.pop();
    inline.push(flipped);
    tampered.filter_inline = Some(inline);

    let memo = super::load::SessionBlockMemo::default();
    super::load::load_segment_filter(&store, None, &memo, &descriptor)
        .await
        .expect("intact inline filter decodes");
    let error = super::load::load_segment_filter(
        &store,
        None,
        &super::load::SessionBlockMemo::default(),
        &tampered,
    )
    .await
    .expect_err("tampered inline filter must fail");
    assert!(
        matches!(error, ManifestLoadError::SegmentCodec { .. }),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn manifest_load_rejects_descriptors_off_the_frozen_segment_layout() {
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
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let tables = load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load tables");
    let payload = tables.manifest().payload.clone();

    // The read path assumes the filter block directly precedes the index
    // block and that an inline copy matches its handle's length; loading a
    // manifest that breaks either must fail instead of degrading.
    type Perturbation = fn(&mut MetadataFileRef);
    fn misalign_filter(descriptor: &mut MetadataFileRef) {
        descriptor.filter_block.offset -= 1;
    }
    fn truncate_inline(descriptor: &mut MetadataFileRef) {
        let inline = descriptor.filter_inline.as_mut().expect("inline filter");
        inline.truncate(inline.len() - 2);
    }
    let perturbations: [(&str, Perturbation); 2] = [
        ("filter not adjacent to index", misalign_filter),
        ("inline length disagrees with handle", truncate_inline),
    ];
    for (index, (label, perturb)) in perturbations.iter().enumerate() {
        let mut perturbed = payload.clone();
        perturbed.manifest_id = ManifestId(perturbed.manifest_id.0 + 1 + index as u64);
        perturbed.manifest_object_id = ManifestObjectId::generate(perturbed.manifest_id);
        let descriptor = perturbed
            .metadata_files
            .iter_mut()
            .find(|descriptor| descriptor.filter_inline.is_some())
            .expect("an inline-filtered descriptor");
        perturb(descriptor);
        let perturbed_object_id = perturbed.manifest_object_id.clone();
        let envelope = NamespaceManifestEnvelope::from_payload("test-writer", perturbed)
            .expect("perturbed manifest envelope");
        write_namespace_manifest(&store, &envelope)
            .await
            .expect("write perturbed manifest");
        let error = match load_verified_manifest_tables(&store, &namespace_id, &perturbed_object_id)
            .await
        {
            Ok(_) => panic!("{label}: perturbed manifest must not load"),
            Err(error) => error,
        };
        assert!(
            matches!(error, ManifestLoadError::SegmentDescriptorMismatch { .. }),
            "{label}: unexpected error {error:?}"
        );
    }
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
    // One L0 run per checkpoint, the first included.
    assert_eq!(l0_runs(&materialized.manifest).len(), 2);
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
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
    )
    .await
    .expect("build manifest without checkpoint");
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
async fn lower_seq_root_publication_yields_to_the_newer_root() {
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
        MetadataLsmPolicy::default().max_rows_per_segment,
    )
    .await
    .expect("build metadata tables");
    let manifest = NamespaceManifestEnvelope::from_payload(
        &context.writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id: ManifestId(materialization_before.head.seq.0),
            manifest_object_id: manifest_object_id(ManifestId(materialization_before.head.seq.0)),
            head_seq: materialization_before.head.seq,
            head_commit_id: materialization_before.head.head_commit_id.clone(),
            base_seq: materialization_before.head.seq,
            writer_epoch: materialization_before.head.writer_epoch,
            next_inode_id: materialization_before.head.next_inode_id,
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            initialized: true,
            verified: true,
            fork: None,
            features: BTreeMap::new(),
            metadata_files: flatten_manifest_tables(tables),
            index_files: Vec::new(),
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
    assert!(later_checkpoint.checkpoint_seq > materialization_before.head.seq);

    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &manifest,
        &materialization_before.root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("manifest publication should classify the newer root");

    match outcome {
        ManifestPublicationOutcome::Superseded(current) => {
            assert_eq!(current.manifest_id, later_checkpoint.manifest_id);
        }
        other => panic!("expected superseded outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn current_manifest_cas_retry_exhaustion_reports_head_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = HeadCasFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_objectstore::keys::metadata_root(namespace_id.as_str()),
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
    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
    )
    .await
    .expect("build manifest");
    write_namespace_manifest(&store, &manifest)
        .await
        .expect("write manifest");

    store.fail_head_cas();
    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &manifest,
        &materialization.root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("root publication should report CAS race");

    assert_eq!(outcome, ManifestPublicationOutcome::RootCasRaceLost);
}

#[tokio::test]
async fn root_cas_transport_error_recovers_when_candidate_was_published() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = RootCasTransportAfterApplyStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_objectstore::keys::metadata_root(namespace_id.as_str()),
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
    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(1),
    )
    .await
    .expect("build manifest");
    write_namespace_manifest(&store, &manifest)
        .await
        .expect("write manifest");

    store.fail_next_root_cas_after_apply();
    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &manifest,
        &materialization.root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("root publication should recover after ambiguous CAS");

    match outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(root.manifest_id, manifest.payload.manifest_id);
            assert_eq!(root.manifest_object_id, manifest.payload.manifest_object_id);
        }
        other => panic!("expected recovered published root, got {other:?}"),
    }
}

#[tokio::test]
async fn root_cas_transport_error_recovers_when_newer_root_was_published() {
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
    let candidate_manifest = build_manifest_from_projection(
        &raw_store,
        &namespace_id,
        &materialization,
        &context,
        ManifestId(1),
    )
    .await;
    let competing_manifest = build_manifest_from_projection(
        &raw_store,
        &namespace_id,
        &materialization,
        &context,
        ManifestId(2),
    )
    .await;
    write_namespace_manifest(&raw_store, &candidate_manifest)
        .await
        .expect("write candidate manifest");
    write_namespace_manifest(&raw_store, &competing_manifest)
        .await
        .expect("write competing manifest");

    let competing_root = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest_id: competing_manifest.payload.manifest_id,
        manifest_object_id: competing_manifest.payload.manifest_object_id.clone(),
        manifest_head_seq: competing_manifest.payload.head_seq,
        manifest_payload_checksum: competing_manifest.payload_checksum.clone(),
        updated_at_ms: context.now_ms + 1,
    };
    let store = RootCasTransportAfterCompetingRootStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_objectstore::keys::metadata_root(namespace_id.as_str()),
        competing_root,
        &context.writer_version,
    );

    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &candidate_manifest,
        &materialization.root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("root publication should recover by observing the competing root");

    match outcome {
        ManifestPublicationOutcome::Superseded(root) => {
            assert_eq!(root.manifest_id, competing_manifest.payload.manifest_id);
            assert_eq!(
                root.manifest_object_id,
                competing_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected superseded root after ambiguous CAS, got {other:?}"),
    }
}

#[tokio::test]
async fn floor_cas_never_regresses_under_a_competing_advancement() {
    // A competing GC actor lands a higher floor between our verification and
    // CAS. The retry observes it and yields: floors are monotonic.
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &inner,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let store = FloorRaiseOnCasConflictStore {
        inner,
        namespace_id: namespace_id.clone(),
        remaining_conflicts: std::sync::atomic::AtomicUsize::new(1),
    };
    let response = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance yields to the competing higher floor");
    // The competitor installed seq 5; our target of seq 1 must not clobber it.
    assert_eq!(response.retention_floor_seq, ChangeSeq(5));
    assert_eq!(
        read_floor_seq(&store.inner, &namespace_id).await,
        ChangeSeq(5)
    );
}

#[derive(Debug)]
struct FloorRaiseOnCasConflictStore {
    inner: LocalFsStore,
    namespace_id: NamespaceId,
    remaining_conflicts: std::sync::atomic::AtomicUsize,
}

impl FloorRaiseOnCasConflictStore {
    async fn install_higher_floor(&self) {
        let loaded = read_wal_floor_object(&self.inner, &self.namespace_id)
            .await
            .expect("read floor for raise");
        let mut floor = loaded.envelope.state;
        floor.floor_seq = ChangeSeq(5);
        let envelope = loonfs_api::wire::control::WalFloorEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::WalFloor,
            "test-writer/0.1.0",
            floor,
        )
        .expect("floor envelope");
        let bytes =
            loonfs_api::wire::control::encode_control_object(&envelope).expect("floor bytes");
        self.inner
            .put(
                &loonfs_objectstore::keys::wal_floor(self.namespace_id.as_str()),
                Bytes::from(bytes),
                PutMode::Overwrite,
            )
            .await
            .expect("write raised floor");
    }
}

#[async_trait]
impl ObjectStore for FloorRaiseOnCasConflictStore {
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
        self.inner.put(key, bytes, mode).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        use std::sync::atomic::Ordering;
        if key == loonfs_objectstore::keys::wal_floor(self.namespace_id.as_str())
            && self.remaining_conflicts.load(Ordering::SeqCst) > 0
        {
            self.remaining_conflicts.fetch_sub(1, Ordering::SeqCst);
            self.install_higher_floor().await;
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        self.inner.compare_and_swap(key, expected_etag, bytes).await
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

#[tokio::test]
async fn a_missing_floor_reads_as_retain_everything() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    store
        .delete(&loonfs_objectstore::keys::wal_floor(namespace_id.as_str()))
        .await
        .expect("delete floor");

    let floor = crate::namespace::control::read_wal_floor_seq_or_zero(&store, &namespace_id)
        .await
        .expect("missing floor defaults");
    assert_eq!(floor, ChangeSeq(0));
}

#[tokio::test]
async fn same_seq_root_replacement_publishes_a_compacted_manifest() {
    // A pure compaction publishes a different manifest for the same logical
    // seq. The monotonic root accepts it: seq must not decrease, the
    // manifest may change.
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
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization.root.manifest_id, checkpoint.manifest_id);
    let compacted = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        &context.writer_version,
        MetadataLsmPolicy::default(),
        ManifestId(checkpoint.manifest_id.0 + 1),
    )
    .await
    .expect("build compacted manifest");
    assert_eq!(
        compacted.payload.head_seq,
        materialization.root.manifest_head_seq
    );
    write_namespace_manifest(&store, &compacted)
        .await
        .expect("write compacted manifest");

    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &compacted,
        &materialization.root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("same-seq replacement publishes");
    match outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(root.manifest_id, compacted.payload.manifest_id);
            assert_eq!(
                root.manifest_head_seq,
                materialization.root.manifest_head_seq
            );
        }
        other => panic!("expected published compacted root, got {other:?}"),
    }
    // Reads keep working against the replaced root.
    let after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization after compaction");
    assert_eq!(after.root.manifest_id, compacted.payload.manifest_id);
    assert!(metadata_states_equivalent(
        &materialization.metadata_state,
        &after.metadata_state
    ));
}

#[tokio::test]
async fn read_anchor_reloads_the_head_when_the_root_is_ahead() {
    // A reader that loads a stale head next to a fresher root must reload
    // the head instead of treating the pair as corruption.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stale_head = inner
        .get_with_metadata(&wal_head(namespace_id.as_str()))
        .await
        .expect("read bootstrap head")
        .expect("bootstrap head exists");

    write_file_bytes(
        &inner,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let store = StaleHeadOnceStore {
        inner,
        head_key: wal_head(namespace_id.as_str()),
        stale_head: std::sync::Mutex::new(Some(stale_head)),
    };
    let projection = load_current_projection(&store, &namespace_id)
        .await
        .expect("read anchor resolves the stale-head race by reloading");
    assert_eq!(projection.head.seq, ChangeSeq(1));
    assert_eq!(projection.root.manifest_head_seq, ChangeSeq(1));
}

#[derive(Debug)]
struct StaleHeadOnceStore {
    inner: LocalFsStore,
    head_key: String,
    stale_head: std::sync::Mutex<Option<ObjectBody>>,
}

#[async_trait]
impl ObjectStore for StaleHeadOnceStore {
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
        if key == self.head_key {
            if let Some(stale) = self.stale_head.lock().expect("stale head lock").take() {
                return Ok(Some(stale));
            }
        }
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

fn test_context() -> MutationContext {
    MutationContext {
        writer_id: "test-writer".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_version: "test-writer/0.1.0".to_owned(),
        now_ms: 1_000,
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
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
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
struct RootCasTransportAfterApplyStore {
    inner: LocalFsStore,
    root_key: String,
    fail_next_root_cas: Mutex<bool>,
}

impl RootCasTransportAfterApplyStore {
    fn new(inner: LocalFsStore, root_key: String) -> Self {
        Self {
            inner,
            root_key,
            fail_next_root_cas: Mutex::new(false),
        }
    }

    fn fail_next_root_cas_after_apply(&self) {
        *self
            .fail_next_root_cas
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    }
}

#[async_trait]
impl ObjectStore for RootCasTransportAfterApplyStore {
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
        if key == self.root_key && matches!(&mode, PutMode::CompareAndSwap { .. }) {
            let should_fail = {
                let mut fail_next = self
                    .fail_next_root_cas
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let should_fail = *fail_next;
                if should_fail {
                    *fail_next = false;
                }
                should_fail
            };
            if should_fail {
                self.inner.put(key, bytes, mode).await?;
                return Err(ObjectStoreError::transport(
                    key,
                    "simulated ambiguous root CAS transport error",
                ));
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

#[derive(Debug)]
struct RootCasTransportAfterCompetingRootStore {
    inner: LocalFsStore,
    root_key: String,
    competing_root: Mutex<Option<MetadataRootState>>,
    writer_version: String,
}

impl RootCasTransportAfterCompetingRootStore {
    fn new(
        inner: LocalFsStore,
        root_key: String,
        competing_root: MetadataRootState,
        writer_version: &str,
    ) -> Self {
        Self {
            inner,
            root_key,
            competing_root: Mutex::new(Some(competing_root)),
            writer_version: writer_version.to_owned(),
        }
    }

    async fn install_competing_root(&self, root: MetadataRootState) {
        let envelope = loonfs_api::wire::control::MetadataRootEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::MetadataRoot,
            &self.writer_version,
            root,
        )
        .expect("metadata root envelope");
        let bytes = Bytes::from(
            loonfs_api::wire::control::encode_control_object(&envelope)
                .expect("encode metadata root"),
        );
        self.inner
            .put(&self.root_key, bytes, PutMode::Overwrite)
            .await
            .expect("install competing root");
    }
}

#[async_trait]
impl ObjectStore for RootCasTransportAfterCompetingRootStore {
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
        if key == self.root_key && matches!(&mode, PutMode::CompareAndSwap { .. }) {
            let competing_root = {
                self.competing_root
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
            };
            if let Some(root) = competing_root {
                self.install_competing_root(root).await;
                return Err(ObjectStoreError::transport(
                    key,
                    "simulated ambiguous root CAS transport error",
                ));
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

#[derive(Debug)]
pub(crate) struct MetadataSstGetCountingStore {
    inner: LocalFsStore,
    metadata_sst_gets: Mutex<usize>,
}

impl MetadataSstGetCountingStore {
    pub(crate) fn new(inner: LocalFsStore) -> Self {
        Self {
            inner,
            metadata_sst_gets: Mutex::new(0),
        }
    }

    pub(crate) fn metadata_sst_gets(&self) -> usize {
        *self
            .metadata_sst_gets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn reset_metadata_sst_gets(&self) {
        *self
            .metadata_sst_gets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = 0;
    }

    fn record_if_metadata_sst(&self, key: &str) {
        if key.contains("/metadata/tables/") && key.ends_with(".sst.zst") {
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
enum ManifestConflictReplacement {
    Fixed(Vec<u8>),
    MutateCandidateNextInode,
}

#[derive(Debug)]
struct ConflictOnManifestCreateStore {
    inner: LocalFsStore,
    manifest_key: String,
    replacement: ManifestConflictReplacement,
    injected: Mutex<bool>,
}

impl ConflictOnManifestCreateStore {
    fn new(inner: LocalFsStore, manifest_key: String, replacement_bytes: Vec<u8>) -> Self {
        Self {
            inner,
            manifest_key,
            replacement: ManifestConflictReplacement::Fixed(replacement_bytes),
            injected: Mutex::new(false),
        }
    }

    fn mutate_next_inode(inner: LocalFsStore, manifest_key: String) -> Self {
        Self {
            inner,
            manifest_key,
            replacement: ManifestConflictReplacement::MutateCandidateNextInode,
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
        let manifest_matches = key == self.manifest_key
            || ((self.manifest_key.ends_with('/') || self.manifest_key.ends_with('-'))
                && key.starts_with(&self.manifest_key));
        if manifest_matches && matches!(&mode, PutMode::CreateIfAbsent) {
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
                let replacement_bytes = match &self.replacement {
                    ManifestConflictReplacement::Fixed(bytes) => Bytes::copy_from_slice(bytes),
                    ManifestConflictReplacement::MutateCandidateNextInode => {
                        let candidate = decode_namespace_manifest_json(&bytes)
                            .map_err(|error| ObjectStoreError::transport(key, error.to_string()))?;
                        let mut payload = candidate.payload;
                        payload.next_inode_id = InodeId(payload.next_inode_id.0 + 1);
                        let mutated = NamespaceManifestEnvelope::from_payload(
                            candidate.writer_version,
                            payload,
                        )
                        .map_err(|error| ObjectStoreError::transport(key, error.to_string()))?;
                        Bytes::from(
                            encode_namespace_manifest_json(&mutated).map_err(|error| {
                                ObjectStoreError::transport(key, error.to_string())
                            })?,
                        )
                    }
                };
                self.inner.put_overwrite(key, replacement_bytes).await?;
                return Err(ObjectStoreError::Conflict {
                    object_key: key.to_owned(),
                });
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

use super::build::{
    build_manifest_l0_run_tables, debug_assert_manifest_table_segments_do_not_overlap,
};
use super::runs::l0_run_count;

// Test support: a manifest built directly from a MetadataState, used to
// author arbitrary layouts without driving the full checkpoint pipeline.
#[cfg(test)]
pub(crate) struct ManifestMetadataSource<'a> {
    pub(super) head: &'a HeadState,
    pub(super) basis_manifest_id: Option<ManifestId>,
    pub(super) retention_floor_seq: ChangeSeq,
    pub(super) metadata_state: &'a MetadataState,
}

#[cfg(test)]
pub(crate) async fn build_namespace_manifest_from_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    source: ManifestMetadataSource<'_>,
    writer_version: &str,
    policy: MetadataLsmPolicy,
    manifest_id: ManifestId,
) -> crate::error::Result<NamespaceManifestEnvelope> {
    let manifest_object_id = ManifestObjectId::generate(manifest_id);
    let head = source.head;
    let metadata_state = source.metadata_state;
    let head_seq = head.seq;
    let previous_manifest = match source.basis_manifest_id {
        Some(previous_id) => Some(
            load_manifest_materialization_for_inspection(store, namespace_id, previous_id)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
                })?,
        ),
        _ => None,
    };

    let (base_seq, metadata_files) = match previous_manifest {
        Some(previous) if is_bootstrap_seed_manifest(&previous.manifest.payload) => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
            (head_seq, flatten_manifest_tables(run_tables))
        }
        Some(previous) if l0_run_count(&previous.manifest.payload) < policy.max_l0_runs => {
            let mut metadata_files = previous.manifest.payload.metadata_files.clone();
            if previous.manifest.payload.head_seq < head_seq {
                metadata_files.extend(flatten_manifest_tables(
                    build_manifest_l0_run_tables(
                        store,
                        namespace_id,
                        head_seq,
                        previous.manifest.payload.head_seq,
                        metadata_state,
                    )
                    .await?,
                ));
            }
            (previous.manifest.payload.base_seq, metadata_files)
        }
        Some(_) => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
            (head_seq, flatten_manifest_tables(run_tables))
        }
        _ => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                policy.max_rows_per_segment,
            )
            .await?;
            (head_seq, flatten_manifest_tables(run_tables))
        }
    };

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id,
            manifest_object_id,
            head_seq,
            head_commit_id: head.head_commit_id.clone(),
            base_seq,
            writer_epoch: head.writer_epoch,
            next_inode_id: head.next_inode_id,
            retention_floor_seq: source.retention_floor_seq,
            initialized: true,
            verified: true,
            fork: None,
            features: BTreeMap::new(),
            metadata_files,
            index_files: Vec::new(),
        },
    )
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace manifest envelope: {err}"
        ))
    })
}

#[cfg(test)]
fn is_bootstrap_seed_manifest(payload: &NamespaceManifestPayload) -> bool {
    payload.head_seq == ChangeSeq(0) && payload.base_seq == ChangeSeq(0) && payload.fork.is_none()
}

/// Deterministic timer advancing a fixed step per reading, so publication
/// budgets are consumed by observations instead of wall time.
#[derive(Debug)]
struct SteppingTimer {
    now_ms: std::sync::atomic::AtomicU64,
    step_ms: u64,
}

impl SteppingTimer {
    fn new(step_ms: u64) -> Self {
        Self {
            now_ms: std::sync::atomic::AtomicU64::new(0),
            step_ms,
        }
    }
}

impl crate::timing::MonotonicTimer for SteppingTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.now_ms
            .fetch_add(self.step_ms, std::sync::atomic::Ordering::SeqCst)
    }
}

/// An over-budget WAL flush aborts before the root compare-and-swap:
/// the root keeps its previous basis, the orphan outputs are harmless GC
/// candidates, and an in-budget retry succeeds.
#[tokio::test]
async fn over_budget_wal_flush_aborts_without_publishing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = MutationContext {
        writer_id: "budget-test".to_owned(),
        writer_session_id: "wrs_budget_test".to_owned(),
        writer_version: "budget-test/0.1.0".to_owned(),
        now_ms: 1_000,
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/budget.txt",
        b"body",
        PutBehavior::NoReplace,
        &context,
        None,
    )
    .await
    .expect("seed file");
    let root_before = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
        .state;

    // Every reading advances 20 minutes against the 15-minute budget: the
    // pre-CAS check observes the publication as over budget.
    let overrun = SteppingTimer::new(20 * 60 * 1000);
    let error = super::flush::flush_wal_with_timer(&store, &namespace_id, &context, &overrun)
        .await
        .expect_err("over-budget publication must abort");
    assert!(
        matches!(error, CoreError::MetadataPublicationBudgetExceeded { .. }),
        "expected budget error, got {error:?}"
    );

    let root_after = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
        .state;
    assert_eq!(
        root_after, root_before,
        "an aborted publication must not move the root"
    );

    // The in-budget retry publishes normally over fresh outputs.
    let advanced = super::flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("in-budget retry succeeds");
    assert_eq!(advanced.outcome, loonfs_api::FlushWalOutcome::Published);
    assert!(advanced.manifest_id > root_before.manifest_id);
}

/// An over-budget reorganization unit aborts the same way: no root motion,
/// orphan tables left for garbage collection.
#[tokio::test]
async fn over_budget_reorganization_aborts_without_publishing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = MutationContext {
        writer_id: "budget-test".to_owned(),
        writer_session_id: "wrs_budget_test".to_owned(),
        writer_version: "budget-test/0.1.0".to_owned(),
        now_ms: 1_000,
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/fold.txt",
        b"body",
        PutBehavior::NoReplace,
        &context,
        None,
    )
    .await
    .expect("seed file");
    super::flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("publish an L0 run to fold");
    let root_before = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
        .state;

    let fold_everything = MetadataLsmPolicy {
        max_l0_runs: 1,
        ..Default::default()
    };
    let overrun = SteppingTimer::new(20 * 60 * 1000);
    let error = super::reorganize::reorganize_metadata_step_with_timer(
        &store,
        &namespace_id,
        &context,
        fold_everything,
        &overrun,
    )
    .await
    .expect_err("over-budget reorganization must abort");
    assert!(
        matches!(error, CoreError::MetadataPublicationBudgetExceeded { .. }),
        "expected budget error, got {error:?}"
    );

    let root_after = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
        .state;
    assert_eq!(root_after, root_before);

    let report = super::reorganize::reorganize_metadata_step(
        &store,
        &namespace_id,
        &context,
        fold_everything,
    )
    .await
    .expect("in-budget retry folds the unit");
    assert!(matches!(
        report.outcome,
        super::MetadataReorganizeOutcome::UnitPublished { .. }
    ));
}

#[tokio::test]
async fn stale_basis_publication_cannot_clobber_a_sibling_root() {
    // The review's flush race: a publisher builds from root A while a
    // sibling publishes B from the same basis, then tries to win on a
    // higher head. Its carried-forward state predates B, so head ordering
    // must not decide the winner — the predecessor gate must.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/one.txt", b"one\n", &context, None)
        .await
        .expect("write one");
    let sibling_basis = load_current_projection(&store, &namespace_id)
        .await
        .expect("sibling basis");

    // The stale publisher observes a newer head against the same root.
    write_file_bytes(&store, &namespace_id, "/two.txt", b"two\n", &context, None)
        .await
        .expect("write two");
    let stale_basis = load_current_projection(&store, &namespace_id)
        .await
        .expect("stale basis");
    assert_eq!(
        sibling_basis.root.manifest_object_id,
        stale_basis.root.manifest_object_id
    );
    assert!(stale_basis.head.seq > sibling_basis.head.seq);

    let sibling = build_manifest_from_projection(
        &store,
        &namespace_id,
        &sibling_basis,
        &context,
        ManifestId(sibling_basis.root.manifest_id.0 + 1),
    )
    .await;
    let stale_higher_head = build_manifest_from_projection(
        &store,
        &namespace_id,
        &stale_basis,
        &context,
        ManifestId(stale_basis.root.manifest_id.0 + 1),
    )
    .await;
    assert!(stale_higher_head.payload.head_seq > sibling.payload.head_seq);
    write_namespace_manifest(&store, &sibling)
        .await
        .expect("write sibling manifest");
    write_namespace_manifest(&store, &stale_higher_head)
        .await
        .expect("write stale manifest");

    let sibling_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &sibling,
        &sibling_basis.root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("sibling publication");
    assert!(matches!(
        sibling_outcome,
        ManifestPublicationOutcome::Published(_)
    ));

    let stale_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &stale_higher_head,
        &stale_basis.root.manifest_object_id,
        context.now_ms + 1,
        &context.writer_version,
    )
    .await
    .expect("stale publication");
    match stale_outcome {
        ManifestPublicationOutcome::Superseded(root) => {
            assert_eq!(
                root.manifest_object_id, sibling.payload.manifest_object_id,
                "the sibling's acknowledged publication must survive"
            );
        }
        other => panic!("a stale-basis candidate must be superseded, got {other:?}"),
    }

    let root_after = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
        .state;
    assert_eq!(
        root_after.manifest_object_id,
        sibling.payload.manifest_object_id
    );
}
// ---------------------------------------------------------------------------
// Gram index build
// ---------------------------------------------------------------------------

async fn grams_feature(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> Option<IndexGramsFeature> {
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state;
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .expect("load manifest tables");
    tables
        .manifest()
        .payload
        .features
        .get(INDEX_GRAMS_FEATURE_KEY)
        .map(|value| IndexGramsFeature::from_value(value).expect("decode feature value"))
}

async fn live_index_files(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> Vec<loonfs_api::wire::manifest::IndexFileRef> {
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state;
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .expect("load manifest tables");
    tables.manifest().payload.index_files.clone()
}

/// Brute reader over every referenced index segment: the union of postings
/// stored for `gram`, in ascending order.
async fn stored_gram_postings(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    gram: Gram,
) -> Vec<GramPosting> {
    fn section<'a>(bytes: &'a [u8], handle: &BlockHandle) -> &'a [u8] {
        &bytes[handle.offset as usize..handle.offset as usize + handle.stored_len as usize]
    }
    let mut postings = Vec::new();
    for descriptor in live_index_files(store, namespace_id).await {
        let bytes = store
            .get(&descriptor.object_key, None)
            .await
            .expect("read index segment")
            .expect("index segment exists");
        let index = decode_index_block(
            section(&bytes, &descriptor.index_block),
            &descriptor.index_block,
        )
        .expect("decode index block");
        for entry in &index {
            let block =
                decode_data_block_rows::<IndexRow>(section(&bytes, &entry.block), &entry.block)
                    .expect("decode index data block");
            for row in &block.rows {
                let IndexRow::GramPostings { gram: row_gram, .. } = row;
                if *row_gram == gram {
                    postings.extend(row.postings().expect("decode postings"));
                }
            }
        }
    }
    postings.sort_unstable();
    postings
}

/// Runs build steps until the index reports up to date, returning how many
/// steps published work.
async fn drain_grams_index(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: GramIndexBuildPolicy,
) -> u64 {
    let mut published = 0u64;
    loop {
        let report = build_grams_index_step(store, namespace_id, context, policy)
            .await
            .expect("build step");
        match report.outcome {
            GramIndexBuildOutcome::Published { .. } => published += 1,
            GramIndexBuildOutcome::UpToDate { .. } => return published,
            other => panic!("unexpected build outcome: {other:?}"),
        }
    }
}

fn posting(inode: u64, revision: u64) -> GramPosting {
    GramPosting {
        inode_id: InodeId(inode),
        revision_no: RevisionNo(revision),
    }
}

#[tokio::test]
async fn grams_index_backfill_covers_existing_text_and_skips_binary() {
    let temp_dir = tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = NamespaceId::parse("grams-backfill").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // Inode allocation is sequential from the root: alpha 2, bravo 3, bin 4.
    write_file_bytes(
        &store,
        &namespace_id,
        "/alpha.txt",
        b"alpha alpha\n",
        &context,
        None,
    )
    .await
    .expect("write alpha");
    write_file_bytes(
        &store,
        &namespace_id,
        "/bravo.txt",
        b"bravo file\n",
        &context,
        None,
    )
    .await
    .expect("write bravo");
    write_file_bytes(
        &store,
        &namespace_id,
        "/blob.bin",
        b"zz\0binary",
        &context,
        None,
    )
    .await
    .expect("write binary");
    let checkpoint_seq = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint")
        .checkpoint_seq;

    let enabled = enable_grams_index(&store, &namespace_id, &context)
        .await
        .expect("enable");
    assert_eq!(
        enabled,
        GramIndexEnableOutcome::Enabled {
            built_through_seq: checkpoint_seq
        }
    );
    let feature = grams_feature(&store, &namespace_id).await.expect("feature");
    assert!(!feature.is_materialized(), "backfill starts unmaterialized");

    // A two-file page budget forces the backfill across several steps.
    let policy = GramIndexBuildPolicy {
        max_files_per_step: 2,
        ..GramIndexBuildPolicy::default()
    };
    let published = drain_grams_index(&store, &namespace_id, &context, policy).await;
    assert!(
        published >= 2,
        "expected a multi-step backfill, got {published}"
    );

    let feature = grams_feature(&store, &namespace_id).await.expect("feature");
    assert!(feature.is_materialized());
    assert_eq!(feature.built_through_seq, checkpoint_seq);

    assert_eq!(
        stored_gram_postings(&store, &namespace_id, Gram(*b"alp")).await,
        vec![posting(2, 1)]
    );
    assert_eq!(
        stored_gram_postings(&store, &namespace_id, Gram(*b"rav")).await,
        vec![posting(3, 1)]
    );
    // The binary file contributed nothing.
    assert_eq!(
        stored_gram_postings(&store, &namespace_id, Gram(*b"nar")).await,
        Vec::new()
    );

    assert_eq!(
        drain_grams_index(&store, &namespace_id, &context, policy).await,
        0,
        "an up-to-date index publishes nothing"
    );
}

#[tokio::test]
async fn grams_index_steady_state_follows_the_wal_and_survives_flush() {
    let temp_dir = tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = NamespaceId::parse("grams-steady").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/alpha.txt",
        b"alpha one\n",
        &context,
        None,
    )
    .await
    .expect("write alpha");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    enable_grams_index(&store, &namespace_id, &context)
        .await
        .expect("enable");
    let policy = GramIndexBuildPolicy::default();
    drain_grams_index(&store, &namespace_id, &context, policy).await;

    // New commits after materialization arrive through WAL replay, without
    // any checkpoint in between.
    let charlie = write_file_bytes(
        &store,
        &namespace_id,
        "/charlie.txt",
        b"charlie code\n",
        &context,
        None,
    )
    .await
    .expect("write charlie");
    write_file_bytes(
        &store,
        &namespace_id,
        "/alpha.txt",
        b"alpha two\n",
        &context,
        None,
    )
    .await
    .expect("overwrite alpha");

    let published = drain_grams_index(&store, &namespace_id, &context, policy).await;
    assert!(published >= 1);
    let feature = grams_feature(&store, &namespace_id).await.expect("feature");
    assert!(feature.built_through_seq > charlie.committed_seq);

    assert_eq!(
        stored_gram_postings(&store, &namespace_id, Gram(*b"har")).await,
        vec![posting(3, 1)]
    );
    // Both revisions of alpha are retained history; both stay indexed.
    assert_eq!(
        stored_gram_postings(&store, &namespace_id, Gram(*b"alp")).await,
        vec![posting(2, 1), posting(2, 2)]
    );

    // A checkpoint flush publishes a successor manifest; the feature entry
    // and the index segment references must carry forward verbatim.
    write_file_bytes(
        &store,
        &namespace_id,
        "/delta.txt",
        b"delta\n",
        &context,
        None,
    )
    .await
    .expect("write delta");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint after index");
    let feature = grams_feature(&store, &namespace_id)
        .await
        .expect("feature survives flush");
    assert!(feature.is_materialized());
    assert!(
        !live_index_files(&store, &namespace_id).await.is_empty(),
        "index segments survive flush"
    );
}

#[tokio::test]
async fn retention_floor_clamps_to_the_grams_watermark() {
    let temp_dir = tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = NamespaceId::parse("grams-retention").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/one.txt", b"one\n", &context, None)
        .await
        .expect("write one");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    enable_grams_index(&store, &namespace_id, &context)
        .await
        .expect("enable");
    let policy = GramIndexBuildPolicy::default();
    drain_grams_index(&store, &namespace_id, &context, policy).await;
    let watermark = grams_feature(&store, &namespace_id)
        .await
        .expect("feature")
        .built_through_seq;

    // Move the manifest head past the watermark without building the index.
    write_file_bytes(&store, &namespace_id, "/two.txt", b"two\n", &context, None)
        .await
        .expect("write two");
    let head_seq = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint")
        .checkpoint_seq;
    assert!(head_seq > watermark);

    let clamped = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention");
    assert_eq!(
        clamped.retention_floor_seq, watermark,
        "the floor must not pass the index watermark"
    );

    // Catch the index up; the floor may then advance to the head.
    drain_grams_index(&store, &namespace_id, &context, policy).await;
    let advanced = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention after catch-up");
    assert_eq!(advanced.retention_floor_seq, head_seq);
}

#[tokio::test]
async fn grams_index_disable_drops_the_feature_and_references() {
    let temp_dir = tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = NamespaceId::parse("grams-disable").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/one.txt",
        b"searchable\n",
        &context,
        None,
    )
    .await
    .expect("write one");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    enable_grams_index(&store, &namespace_id, &context)
        .await
        .expect("enable");
    drain_grams_index(
        &store,
        &namespace_id,
        &context,
        GramIndexBuildPolicy::default(),
    )
    .await;
    assert!(!live_index_files(&store, &namespace_id).await.is_empty());

    let disabled = disable_grams_index(&store, &namespace_id, &context)
        .await
        .expect("disable");
    assert_eq!(disabled, GramIndexDisableOutcome::Disabled);
    assert!(grams_feature(&store, &namespace_id).await.is_none());
    assert!(live_index_files(&store, &namespace_id).await.is_empty());

    let report = build_grams_index_step(
        &store,
        &namespace_id,
        &context,
        GramIndexBuildPolicy::default(),
    )
    .await
    .expect("build step after disable");
    assert_eq!(report.outcome, GramIndexBuildOutcome::NotEnabled);

    let enabled_again = enable_grams_index(&store, &namespace_id, &context)
        .await
        .expect("re-enable");
    assert!(matches!(
        enabled_again,
        GramIndexEnableOutcome::Enabled { .. }
    ));
}

#[tokio::test]
async fn forking_a_lagging_index_restarts_backfill_instead_of_inheriting_a_gap() {
    let temp_dir = tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let source = NamespaceId::parse("grams-fork-source").expect("namespace id");
    let target = NamespaceId::parse("grams-fork-target").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &source, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &source, "/one.txt", b"needle one\n", &context, None)
        .await
        .expect("write one");
    create_checkpoint(&store, &source, &context)
        .await
        .expect("checkpoint");
    enable_grams_index(&store, &source, &context)
        .await
        .expect("enable");
    drain_grams_index(&store, &source, &context, GramIndexBuildPolicy::default()).await;
    let watermark = grams_feature(&store, &source)
        .await
        .expect("source feature")
        .built_through_seq;

    // Move the fork point past the watermark without building the index:
    // the fork target will never hold the WAL that covers this gap.
    write_file_bytes(
        &store,
        &source,
        "/gap.txt",
        b"needle in the gap\n",
        &context,
        None,
    )
    .await
    .expect("write gap");
    crate::namespace::fork::fork_namespace(&store, &source, &target, &context)
        .await
        .expect("fork");

    let feature = grams_feature(&store, &target)
        .await
        .expect("target feature");
    assert!(
        feature.built_through_seq > watermark,
        "fork point is past the watermark"
    );
    assert!(
        !feature.is_materialized(),
        "a lagging inherited index must restart backfill"
    );

    // Backfill over the copied metadata tables rebuilds the gap.
    drain_grams_index(&store, &target, &context, GramIndexBuildPolicy::default()).await;
    let feature = grams_feature(&store, &target)
        .await
        .expect("target feature after drain");
    assert!(feature.is_materialized());
    let gap_postings = stored_gram_postings(&store, &target, Gram(*b"gap")).await;
    assert!(
        !gap_postings.is_empty(),
        "the fork gap must be indexed on the target"
    );
    let old_postings = stored_gram_postings(&store, &target, Gram(*b"one")).await;
    assert!(!old_postings.is_empty());
}

#[tokio::test]
async fn grams_index_fold_merges_delta_segments_into_a_base() {
    let temp_dir = tempdir().expect("temp dir");
    let store = LocalFsStore::new(temp_dir.path()).expect("local store");
    let namespace_id = NamespaceId::parse("grams-fold").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // Every file shares the word "shared" so the fold must merge one gram's
    // batches across segments; a one-file page budget forces one delta
    // segment per backfill step.
    write_file_bytes(
        &store,
        &namespace_id,
        "/alpha.txt",
        b"alpha shared\n",
        &context,
        None,
    )
    .await
    .expect("write alpha");
    write_file_bytes(
        &store,
        &namespace_id,
        "/bravo.txt",
        b"bravo shared\n",
        &context,
        None,
    )
    .await
    .expect("write bravo");
    write_file_bytes(
        &store,
        &namespace_id,
        "/charlie.txt",
        b"charlie shared\n",
        &context,
        None,
    )
    .await
    .expect("write charlie");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    enable_grams_index(&store, &namespace_id, &context)
        .await
        .expect("enable");
    let policy = GramIndexBuildPolicy {
        max_files_per_step: 1,
        max_l0_segments: 2,
        ..GramIndexBuildPolicy::default()
    };
    drain_grams_index(&store, &namespace_id, &context, policy).await;

    let delta_segments = live_index_files(&store, &namespace_id).await;
    assert!(
        delta_segments.len() >= 3,
        "expected one delta segment per backfill step, got {}",
        delta_segments.len()
    );
    let shared_before = stored_gram_postings(&store, &namespace_id, Gram(*b"sha")).await;
    assert_eq!(
        shared_before,
        vec![posting(2, 1), posting(3, 1), posting(4, 1)]
    );

    let report = fold_grams_index_step(&store, &namespace_id, &context, policy)
        .await
        .expect("fold step");
    match report.outcome {
        GramIndexFoldOutcome::UnitPublished {
            merged_segments,
            segments_written,
        } => {
            assert_eq!(merged_segments, delta_segments.len());
            assert_eq!(segments_written, 1, "small corpora fold to one base");
        }
        other => panic!("expected a published fold unit, got {other:?}"),
    }

    let base_segments = live_index_files(&store, &namespace_id).await;
    assert_eq!(base_segments.len(), 1);
    assert!(base_segments
        .iter()
        .all(|descriptor| descriptor.level == CHECKPOINT_BASE_RUN_LEVEL));
    assert_eq!(
        stored_gram_postings(&store, &namespace_id, Gram(*b"sha")).await,
        shared_before,
        "folding must not change what the index answers"
    );
    assert_eq!(
        stored_gram_postings(&store, &namespace_id, Gram(*b"alp")).await,
        vec![posting(2, 1)]
    );

    let report = fold_grams_index_step(&store, &namespace_id, &context, policy)
        .await
        .expect("second fold step");
    assert!(
        matches!(
            report.outcome,
            GramIndexFoldOutcome::NotNeeded { l0_segments: 0 }
        ),
        "a folded family has no delta segments left"
    );
}
