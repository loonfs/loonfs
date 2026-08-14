//! Checkpoint secondary-index parity and manifest descriptor validation.

use super::*;
use loonfs_api::{Checksum, ContentId};

pub(super) async fn rewrite_manifest_segment(
    store: &LocalFsStore,
    _namespace_id: &NamespaceId,
    _run_seq: ChangeSeq,
    family: ApiMetadataTableFamily,
    descriptor: &mut MetadataFileRef,
    rows: Vec<MetadataRow>,
    target_block_bytes: NonZeroUsize,
) {
    let mut builder = SegmentBlocksBuilder::new(target_block_bytes);
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
    // A rewritten segment's filter changes size, and a descriptor that inlines
    // a filter must inline the one it actually has: manifest load compares the
    // two. Derived exactly as `write_manifest_segment` derives it.
    descriptor.filter_inline = (built.filter.stored_len
        <= super::super::build::INLINE_SEGMENT_FILTER_MAX_BYTES)
        .then(|| {
            let start = built.filter.offset as usize;
            loonfs_api::wire::hex::hex_encode_bytes(
                &built.bytes[start..start + built.filter.stored_len as usize],
            )
        });
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
        default_target_block_bytes(),
    )
    .await;
}

fn default_target_block_bytes() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_TARGET_BLOCK_BYTES).expect("the default target is positive")
}

pub(super) async fn overwrite_manifest(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    manifest: NamespaceManifestEnvelope,
) {
    let manifest_key = metadata_manifest_object(namespace_id, &manifest.payload.manifest_object_id);
    let manifest_id = manifest.payload.manifest_id;
    let manifest_object_id = manifest.payload.manifest_object_id.clone();
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(manifest.payload).expect("updated manifest");
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
    if loaded_root.state.manifest_id == manifest_id {
        let mut root = loaded_root.state;
        root.manifest_object_id = manifest_object_id;
        root.manifest_payload_checksum = updated_manifest.payload_checksum.clone();
        let envelope = loonfs_api::wire::control::MetadataRootEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::MetadataRoot,
            root,
        )
        .expect("root envelope");
        let bytes =
            loonfs_api::wire::control::encode_control_object(&envelope).expect("root bytes");
        store
            .put_overwrite(
                &loonfs_objectstore::keys::metadata_root(namespace_id),
                Bytes::from(bytes),
            )
            .await
            .expect("overwrite root");
    }
}

/// Republishes a payload under a fresh manifest id and loads it back, so a
/// caller can assert on what validation makes of an edited manifest.
async fn load_perturbed_manifest(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    mut payload: NamespaceManifestPayload,
    id_offset: u64,
) -> Result<(), ManifestLoadError> {
    payload.manifest_id = ManifestId(payload.manifest_id.0 + id_offset);
    payload.manifest_object_id = ManifestObjectId::generate(payload.manifest_id);
    let manifest_object_id = payload.manifest_object_id.clone();
    let envelope =
        NamespaceManifestEnvelope::from_payload(payload).expect("perturbed manifest envelope");
    write_namespace_manifest(store, &envelope)
        .await
        .expect("write perturbed manifest");
    load_verified_manifest_tables(store, namespace_id, &manifest_object_id)
        .await
        .map(|_| ())
}

/// A second descriptor modelled on one the manifest already holds: a fresh
/// table id and object key, stamped with whatever run identity the caller
/// wants to test. The rows it points at are the modelled segment's, which is
/// what a stray or duplicated descriptor looks like.
fn segment_modelled_on(
    modelled_on: &MetadataFileRef,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    level: u32,
) -> MetadataFileRef {
    let table_id = loonfs_api::MetadataTableId::generate();
    MetadataFileRef {
        object_key: metadata_table(namespace_id, &table_id),
        table_id,
        run_seq,
        level,
        segment_index: 0,
        ..modelled_on.clone()
    }
}

/// One base-tier segment of `family`, for a test that builds a second
/// descriptor out of it.
fn base_segment_of_family(
    manifest: &NamespaceManifestEnvelope,
    family: ApiMetadataTableFamily,
) -> MetadataFileRef {
    manifest
        .payload
        .metadata_files
        .iter()
        .find(|descriptor| {
            descriptor.family == family && descriptor.level == CHECKPOINT_BASE_RUN_LEVEL
        })
        .expect("a folded base segment of this family")
        .clone()
}

/// A namespace with one folded base run and one delta run above it, which is
/// the smallest shape a second base-tier run can be added to.
async fn seed_folded_base_with_a_delta_run(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        store,
        namespace_id,
        "/docs/first.txt",
        b"first\n",
        &context,
        None,
    )
    .await
    .expect("write the seed file");
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the seed");
    drain_reorganization(store, namespace_id, &context, MetadataLsmPolicy::default()).await;
    write_file_bytes(
        store,
        namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
    .await
    .expect("write a file above the base");
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint a delta run above the base");
}

/// The current manifest, loaded and detached from the tables that hold it.
async fn current_manifest(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> NamespaceManifestEnvelope {
    let manifest_object_id = current_manifest_object_id(store, namespace_id).await;
    let tables = load_verified_manifest_tables(store, namespace_id, &manifest_object_id)
        .await
        .expect("load the current manifest's tables");
    tables.manifest().clone()
}

fn assert_revision_index_mismatch<T>(result: Result<T, ManifestLoadError>) {
    match result {
        Err(ManifestLoadError::RevisionIndexMismatch { .. }) => {}
        Err(other) => panic!("expected revision index mismatch, got {other:?}"),
        Ok(_) => panic!("expected revision index mismatch"),
    }
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
        display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(seq),
        bind_delta_index: delta,
    };
    let unbind = |bind_seq: u64, delta: u32, unbind_seq: u64| MetadataRow::DirentryUnbind {
        parent_inode_id: InodeId(1),
        name_key: NameKey::parse("docs").expect("valid name key"),
        display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
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

    fold_rows_with_retention(MetadataFamilyGroup::Bindings, &mut rows, ChangeSeq(1)).expect("drop");

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
        display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(1),
        bind_delta_index: delta,
    };
    let unbind = MetadataRow::DirentryUnbind {
        parent_inode_id: InodeId(1),
        name_key: NameKey::parse("docs").expect("valid name key"),
        display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
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

    fold_rows_with_retention(MetadataFamilyGroup::Bindings, &mut rows, ChangeSeq(1)).expect("drop");

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
        display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(1),
        bind_delta_index: delta,
    };
    let mut rows = BTreeMap::new();
    rows.insert(
        ApiMetadataTableFamily::DirentryBinds,
        vec![bind(0), bind(1)],
    );

    let error = fold_rows_with_retention(MetadataFamilyGroup::Bindings, &mut rows, ChangeSeq(1))
        .expect_err("superseded live bind must refuse the drop");
    assert!(matches!(error, CoreError::NamespaceCorrupt(_)));
}

#[tokio::test]
async fn restore_below_the_floor_succeeds_after_reorganization() {
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
    // Revision history is retained independently of the replay floor:
    // folding the runs against the advanced floor must leave every
    // revision readable.
    drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;

    let restored = restore_file_revision(
        &store,
        &namespace_id,
        "/docs/a.txt",
        RevisionNo(1),
        &context,
        None,
    )
    .await
    .expect("restoring a revision below the floor succeeds");
    assert!(restored.committed_seq > ChangeSeq(0));
}

#[tokio::test]
async fn base_rebuild_retains_revisions_superseded_below_floor() {
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
    // Checkpoints only append, and the base rebuild folds the runs against
    // the advanced floor — but revision rows are never dropped: file
    // history is durable data, not replay state.
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
    let checksum_one = Checksum::sha256(b"one\n");
    let checksum_two = Checksum::sha256(b"two\n");
    assert!(revisions.iter().any(|row| matches!(
        row,
        MetadataRow::Revision { content_ref, .. } if content_ref.checksum == checksum_one
    )));
    assert!(revisions.iter().any(|row| matches!(
        row,
        MetadataRow::Revision { content_ref, .. } if content_ref.checksum == checksum_two
    )));
    let index_rows = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataTableFamily::RevisionsByInodeDesc,
    );
    assert_eq!(index_rows.len(), revisions.len());
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
        MetadataRow::DirentryBind { display_name, .. } if display_name.as_str() == "tmp.txt"
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
async fn bounded_subset_rebuild_rejects_divergent_revision_index() {
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
        content_ref.content_id = ContentId::generate();
    }
    rewrite_revision_index_segment(&store, &namespace_id, &mut manifest, revision_index_rows).await;
    overwrite_manifest(&store, &namespace_id, manifest).await;

    // L0 appends never re-read the base run; the reorganization merge that
    // folds every run back together is the production point that must
    // reject it.
    for index in 0..3 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/another-{index}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write");
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("l0 checkpoint");
    }
    let fold_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
        ..MetadataLsmPolicy::default()
    };
    let mut rebuild_error = None;
    for _unit in 0..8 {
        match super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            fold_policy,
            MetadataCompactionView::default(),
        )
        .await
        {
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
    // The merge digests the rows it writes into each family of the pair and
    // compares the two, which is the one index-parity check either
    // reorganization path makes.
    match rebuild_error.expect("reorganization should reject the divergent index") {
        CoreError::NamespaceCorrupt(message) => {
            assert!(
                message.contains("RevisionsByInodeDesc")
                    && message.contains("must hold the same rows"),
                "expected a revision index mismatch, got: {message}"
            );
        }
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
        metadata_manifest_object(&namespace_id, &manifest.payload.manifest_object_id);
    let manifest_id = manifest.payload.manifest_id;
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(manifest.payload).expect("updated manifest");
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
        default_target_block_bytes(),
    )
    .await;

    let manifest_key =
        metadata_manifest_object(&namespace_id, &manifest.payload.manifest_object_id);
    let manifest_id = manifest.payload.manifest_id;
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(manifest.payload).expect("updated manifest");
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
            committed_at_ms,
            actor,
            revision_delta_index,
            content_ref,
        } => MetadataRow::Revision {
            inode_id,
            revision_no: loonfs_api::RevisionNo(revision_no.0 + 100),
            committed_seq,
            committed_at_ms,
            actor,
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
        content_ref.content_id = ContentId::generate();
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

/// A data block closes from inside the builder's `push` as soon as it
/// reaches its target size, so a segment's last row can be the row that
/// closes its block. The descriptor's max key must still describe the rows
/// the segment holds: a keyed scan prunes a segment whose max key sorts
/// below the scan's lower bound, so an empty max key hides every row from
/// every point and prefix lookup while reorganization, which scans from the
/// empty bound, still reads them.
#[tokio::test]
async fn lookups_find_rows_in_a_segment_whose_last_row_closed_a_block() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 1..=6 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{index}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write file");
    }
    // Fold the L0 runs so every inode row lands in one base segment.
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
            .expect("load manifest before the rewrite");
    let mut manifest = materialized.manifest;
    let mut inode_rows =
        manifest_rows_for_family(&materialized.metadata_state, ApiMetadataTableFamily::Inodes);
    inode_rows.sort_by_key(|row| row.row_key_for_family(ApiMetadataTableFamily::Inodes));
    assert!(
        inode_rows.len() > 1,
        "the namespace should hold several inodes"
    );
    let last_inode_key = inode_rows
        .last()
        .expect("inode rows")
        .row_key_for_family(ApiMetadataTableFamily::Inodes);

    let base_inode_segments = manifest
        .payload
        .metadata_files
        .iter()
        .filter(|metadata_file| {
            metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                && metadata_file.family == ApiMetadataTableFamily::Inodes
        })
        .count();
    assert_eq!(
        base_inode_segments, 1,
        "the folded base should hold one inode segment"
    );
    let descriptor = manifest
        .payload
        .metadata_files
        .iter_mut()
        .find(|metadata_file| {
            metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                && metadata_file.family == ApiMetadataTableFamily::Inodes
        })
        .expect("base inode segment");
    // A one-byte target closes a block on every push, the last row
    // included. That is the shape a final row produces whenever it lands on
    // the target crossing, which happens at any block size.
    rewrite_manifest_segment(
        &store,
        &namespace_id,
        manifest.payload.head_seq,
        ApiMetadataTableFamily::Inodes,
        descriptor,
        inode_rows.clone(),
        NonZeroUsize::MIN,
    )
    .await;
    assert_eq!(
        descriptor.max_key, last_inode_key,
        "the descriptor must carry the segment's last row key"
    );
    overwrite_manifest(&store, &namespace_id, manifest).await;

    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let tables = load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("the rewritten manifest should load");
    for row in &inode_rows {
        let key = row.row_key_for_family(ApiMetadataTableFamily::Inodes);
        let found = tables
            .get_for_lookup(ApiMetadataTableFamily::Inodes, &key, &key)
            .await
            .expect("inode lookup");
        assert!(
            found.is_some(),
            "the keyed lookup lost inode row `{key}` in the rewritten segment"
        );
    }
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
    // block, that an inline copy matches its handle's length, and that a
    // segment holding rows reports the key range those rows span; loading a
    // manifest that breaks any of them must fail instead of degrading.
    type Perturbation = fn(&mut MetadataFileRef);
    fn misalign_filter(descriptor: &mut MetadataFileRef) {
        descriptor.filter_block.offset -= 1;
    }
    fn truncate_inline(descriptor: &mut MetadataFileRef) {
        let inline = descriptor.filter_inline.as_mut().expect("inline filter");
        inline.truncate(inline.len() - 2);
    }
    // An empty max key sorts below every scan bound, so the segment would
    // answer no keyed lookup while still holding its rows.
    fn clear_max_key(descriptor: &mut MetadataFileRef) {
        assert!(descriptor.row_count > 0, "the segment should hold rows");
        descriptor.max_key.clear();
    }
    fn invert_key_range(descriptor: &mut MetadataFileRef) {
        assert!(descriptor.row_count > 0, "the segment should hold rows");
        std::mem::swap(&mut descriptor.min_key, &mut descriptor.max_key);
    }
    let perturbations: [(&str, Perturbation); 4] = [
        ("filter not adjacent to index", misalign_filter),
        ("inline length disagrees with handle", truncate_inline),
        ("segment with rows has no max key", clear_max_key),
        ("segment key range descends", invert_key_range),
    ];
    for (index, (label, perturb)) in perturbations.iter().enumerate() {
        let mut perturbed = payload.clone();
        let descriptor = perturbed
            .metadata_files
            .iter_mut()
            // A segment spanning several keys, so swapping its bounds
            // actually descends.
            .find(|descriptor| {
                descriptor.filter_inline.is_some() && descriptor.min_key != descriptor.max_key
            })
            .expect("an inline-filtered descriptor spanning several keys");
        perturb(descriptor);
        let Err(error) =
            load_perturbed_manifest(&store, &namespace_id, perturbed, 1 + index as u64).await
        else {
            panic!("{label}: perturbed manifest must not load")
        };
        assert!(
            matches!(error, ManifestLoadError::SegmentDescriptorMismatch { .. }),
            "{label}: unexpected error {error:?}"
        );
    }
}

/// A group with two base-tier runs does not load.
///
/// This is the shape a merge above the base used to write: its output went to
/// the base tier stamped at the manifest head, so it became a second base run
/// for the group rather than a bigger delta run. Every fragment stayed under
/// the per-step budget on its own, so nothing ever reported the group's oldest
/// run as over budget, and the group's rows below the retention floor could
/// never be dropped — dropping needs a fold whose window starts at the group's
/// oldest run.
///
/// Refusing the shape at load is what says it cannot come back: no builder
/// writes it now, and a manifest carrying it is corruption rather than a
/// state to carry on from.
#[tokio::test]
async fn a_manifest_whose_group_base_fragmented_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let manifest = current_manifest(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::Inodes);
    assert_eq!(
        group_base_runs(&manifest, group).len(),
        1,
        "the seed must leave the group in one base run"
    );

    let mut fragmented = manifest.payload.clone();
    fragmented.metadata_files.push(segment_modelled_on(
        &base_segment_of_family(&manifest, ApiMetadataTableFamily::Inodes),
        &namespace_id,
        manifest.payload.head_seq,
        CHECKPOINT_BASE_RUN_LEVEL,
    ));

    let error = load_perturbed_manifest(&store, &namespace_id, fragmented, 1)
        .await
        .expect_err("a group with two base runs must not load");
    let ManifestLoadError::RunManifestMismatch { message, .. } = &error else {
        panic!("expected a run mismatch, got {error:?}")
    };
    assert!(
        message.contains("Inodes") && message.contains("base"),
        "the rejection must name the group and what it holds too many of, got `{message}`"
    );
}

/// Two segments of one family carrying the same index inside one run do not
/// load.
///
/// A family in a run has one producer, and every producer numbers from zero,
/// so the numbers are a dense sequence. Two sets of them at one identity is
/// what a merge above the base used to leave behind when the group's base
/// already sat at the identity the merge stamped: both sets started at zero.
#[tokio::test]
async fn a_manifest_that_numbers_one_family_twice_in_one_run_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let manifest = current_manifest(&store, &namespace_id).await;
    let existing = base_segment_of_family(&manifest, ApiMetadataTableFamily::Inodes);
    assert_eq!(existing.segment_index, 0);

    let mut repeated = manifest.payload.clone();
    repeated.metadata_files.push(segment_modelled_on(
        &existing,
        &namespace_id,
        existing.run_seq,
        existing.level,
    ));

    let error = load_perturbed_manifest(&store, &namespace_id, repeated, 2)
        .await
        .expect_err("two segments of one family at one index must not load");
    let ManifestLoadError::SegmentDescriptorMismatch { message, .. } = &error else {
        panic!("expected a segment descriptor mismatch, got {error:?}")
    };
    assert!(
        message.contains("Inodes") && message.contains("numbered from zero"),
        "the rejection must name the family and the numbering rule, got `{message}`"
    );
}

/// Two segments of one family whose key ranges touch inside one run do not
/// load.
///
/// One producer writes a family's segments in ascending key order, so segment
/// one starts strictly above where segment zero ended. A descriptor can keep
/// the numbering dense and still not belong — stamped with another run's
/// identity, say — and then its key range is what disagrees with its
/// neighbours'. Here the second segment repeats the first one's whole range,
/// which is the shape a duplicated descriptor takes.
#[tokio::test]
async fn a_manifest_whose_run_segments_overlap_in_key_range_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let manifest = current_manifest(&store, &namespace_id).await;
    let existing = base_segment_of_family(&manifest, ApiMetadataTableFamily::Inodes);
    assert_eq!(existing.segment_index, 0);

    let mut overlapping = manifest.payload.clone();
    let mut second =
        segment_modelled_on(&existing, &namespace_id, existing.run_seq, existing.level);
    // Index one keeps the numbering dense, so only the key order can object.
    second.segment_index = 1;
    overlapping.metadata_files.push(second);

    let error = load_perturbed_manifest(&store, &namespace_id, overlapping, 3)
        .await
        .expect_err("overlapping segment ranges within one run must not load");
    let ManifestLoadError::SegmentDescriptorMismatch { message, .. } = &error else {
        panic!("expected a segment descriptor mismatch, got {error:?}")
    };
    assert!(
        message.contains("Inodes") && message.contains("ascending key order"),
        "the rejection must name the family and the ordering rule, got `{message}`"
    );
}
