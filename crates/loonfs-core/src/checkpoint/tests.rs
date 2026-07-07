#![allow(clippy::panic)]
// These tests use panic in impossible match arms to preserve precise failure messages.

//! Behavior tests for the checkpoint lifecycle: creation, publication
//! races, retention, fork materialization, and corruption rejection.

use super::build::{
    build_manifest_tables, build_manifest_tables_from_rows, MetadataTableSegmentation,
};
use super::cache::{MetadataTableCache, MetadataTableCacheConfig};
use super::create::{
    build_namespace_manifest_from_metadata_state, create_checkpoint, create_checkpoint_with_policy,
    drop_rows_below_retention_floor, load_checkpoint_projection_metadata_state,
    ManifestMetadataSource,
};
use super::error::ManifestLoadError;
use super::load::{
    head_from_manifest, load_manifest_materialization_for_inspection,
    load_manifest_metadata_state_for_inspection_from_manifest, load_verified_manifest_tables,
};
use super::publish::{publish_metadata_root, write_namespace_manifest, ManifestPublicationOutcome};
use super::record::read_checkpoint_record;
use super::retention::advance_retention_floor;
use super::row::{manifest_rows_for_family, metadata_states_equivalent};
use super::runs::{
    flatten_manifest_tables, runs_from_metadata_files, MetadataLsmPolicy, MetadataRunManifest,
    CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL, CHECKPOINT_TABLE_FAMILIES,
    DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT, MAX_CHECKPOINT_L0_RUNS,
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
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, encode_metadata_sst_envelope_zstd,
    encode_namespace_manifest_json, MetadataFileRef, MetadataPage, MetadataRow, MetadataSegmentKey,
    MetadataSstEnvelope, MetadataSstPayload, MetadataTableFamily as ApiMetadataTableFamily,
    NamespaceManifestEnvelope, NamespaceManifestPayload,
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
use std::sync::Mutex;
use tempfile::tempdir;

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
        loonfs_api::wire::control::CheckpointRecordLifecycle::Dead,
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
        owner: None,
        name: None,
        state: loonfs_api::wire::control::CheckpointRecordLifecycle::Active,
    };
    let verified = crate::checkpoint::verify_checkpoint_basis(&store, &stale)
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

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        last_manifest_id.expect("manifest id"),
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

    drop_rows_below_retention_floor(&mut rows, ChangeSeq(1)).expect("drop");

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

    drop_rows_below_retention_floor(&mut rows, ChangeSeq(1)).expect("drop");

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

    let error = drop_rows_below_retention_floor(&mut rows, ChangeSeq(1))
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

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        last_manifest_id.expect("manifest id"),
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

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        last_manifest_id.expect("manifest id"),
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
    rewrite_revision_index_segment(
        &store,
        &namespace_id,
        &mut manifest,
        revision_index_rows,
        &context.writer_version,
    )
    .await;
    overwrite_manifest(&store, &namespace_id, manifest).await;

    // L0 appends never re-read the base run; the base rebuild that folds
    // every run back together is the production point that must reject it.
    let mut rebuild_error = None;
    for round in 0..12u32 {
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
        match create_checkpoint(&store, &namespace_id, &context).await {
            Ok(_) => {}
            Err(error) => {
                rebuild_error = Some(error);
                break;
            }
        }
    }
    match rebuild_error.expect("base rebuild should reject the divergent index") {
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
            manifest_object_id: manifest_object_id(manifest_id(materialization.head.seq)),
            prev_manifest_id: Some(materialization.root.manifest_id),
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
    create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("manifest");
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
    create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
        .await
        .expect("second checkpoint");
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
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let cache = MetadataTableCache::new(MetadataTableCacheConfig {
        enabled: true,
        max_blocks: 256,
        max_decoded_bytes: Some(1),
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

    let checkpoint = create_checkpoint_with_policy(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
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
        context.now_ms,
        &context.writer_version,
    )
    .await
    .expect("publish manifest");

    let checkpoint = create_checkpoint_with_policy(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
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
            manifest_object_id: manifest_object_id(ManifestId(materialization_before.head.seq.0)),
            prev_manifest_id: Some(materialization_before.root.manifest_id),
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
    assert_eq!(
        compacted.payload.prev_manifest_id,
        Some(checkpoint.manifest_id)
    );

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
