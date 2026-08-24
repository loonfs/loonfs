//! Checkpoint secondary-index parity and manifest descriptor validation.

use super::*;
use loonfs_api::{Checksum, ContentId};

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PreCommitIdMetadataRow {
    Inode {
        inode_id: InodeId,
        inode_kind: loonfs_api::InodeKind,
        created_seq: ChangeSeq,
        created_by: loonfs_api::ActorRef,
        created_at_ms: u64,
    },
}

pub(super) async fn rewrite_manifest_segment(
    store: &LocalFsStore,
    _namespace_id: &NamespaceId,
    _run_seq: ChangeSeq,
    family: ApiMetadataRowFamily,
    descriptor: &mut MetadataSegmentRef,
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
        .put_overwrite(
            &metadata_segment_object_key(descriptor),
            Bytes::from(built.bytes.clone()),
        )
        .await
        .expect("overwrite segment");

    descriptor.row_count = built.row_count;
    descriptor.min_row_key = built.min_row_key;
    descriptor.max_row_key = built.max_row_key;
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
    descriptor.object_checksum = loonfs_api::sha256_digest(&built.bytes);
}

fn assert_child_index_mismatch<T>(result: Result<T, ManifestLoadError>) {
    match result {
        Err(ManifestLoadError::SegmentDescriptorMismatch { message, .. }) => {
            assert!(message.contains("direntry_child_binds index"));
        }
        Err(other) => panic!("expected child index mismatch, got {other:?}"),
        Ok(_) => panic!("expected child index mismatch"),
    }
}

async fn revision_index_test_materialization(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> (ManifestNo, NamespaceManifestEnvelope, Vec<MetadataRow>) {
    // The corruptions below target base-run segments, which only exist
    // once reorganization has folded the checkpoint's delta runs.
    let manifest_no =
        checkpoint_then_reorganize(store, namespace_id, context, MetadataLsmPolicy::default())
            .await;
    let materialized =
        load_manifest_materialization_for_inspection(store, namespace_id, manifest_no)
            .await
            .expect("load manifest before corruption");
    let revision_index_rows = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataRowFamily::RevisionsByInodeDesc,
    );
    assert!(!revision_index_rows.is_empty());
    (
        materialized.manifest.payload.manifest_no,
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
    rows.sort_by_key(|row| row.row_key_for_family(ApiMetadataRowFamily::RevisionsByInodeDesc));
    let descriptor = manifest
        .payload
        .segments
        .iter_mut()
        .find(|descriptor| {
            descriptor.level == CHECKPOINT_BASE_RUN_LEVEL
                && descriptor.family == ApiMetadataRowFamily::RevisionsByInodeDesc
        })
        .expect("revision index metadata file");
    rewrite_manifest_segment(
        store,
        namespace_id,
        manifest.payload.head_seq,
        ApiMetadataRowFamily::RevisionsByInodeDesc,
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
    let manifest_no = manifest.payload.manifest_no;
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
    let loaded_root = load_metadata_root_object(store, namespace_id)
        .await
        .expect("read root");
    if loaded_root.state.manifest.manifest_no == manifest_no {
        let mut root = loaded_root.state;
        root.manifest.manifest_object_id = manifest_object_id;
        root.manifest.manifest_payload_checksum = updated_manifest.payload_checksum.clone();
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

/// Republishes a payload under a fresh manifest number and loads it back, so a
/// caller can assert on what validation makes of an edited manifest.
async fn load_perturbed_manifest(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    mut payload: NamespaceManifestPayload,
    id_offset: u64,
) -> Result<(), ManifestLoadError> {
    payload.manifest_no = ManifestNo(payload.manifest_no.0 + id_offset);
    payload.manifest_object_id = ManifestObjectId::generate(payload.manifest_no);
    let manifest_object_id = payload.manifest_object_id.clone();
    let envelope =
        NamespaceManifestEnvelope::from_payload(payload).expect("perturbed manifest envelope");
    write_namespace_manifest(store, &envelope)
        .await
        .expect("write perturbed manifest");
    load_verified_manifest_segments(store, namespace_id, &manifest_object_id)
        .await
        .map(|_| ())
}

/// Copies a descriptor with a new segment id and the requested run identity.
/// The copied row metadata models a stray or duplicate descriptor.
fn segment_modelled_on(
    modelled_on: &MetadataSegmentRef,
    run_no: RunNo,
    run_seq: ChangeSeq,
    level: u32,
) -> MetadataSegmentRef {
    MetadataSegmentRef {
        segment_id: loonfs_api::MetadataSegmentId::generate(),
        run_no,
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
    family: ApiMetadataRowFamily,
) -> MetadataSegmentRef {
    manifest
        .payload
        .segments
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

/// Loads a copy of the current manifest payload.
async fn current_manifest(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> NamespaceManifestEnvelope {
    let manifest_object_id = current_manifest_object_id(store, namespace_id).await;
    let segments = load_verified_manifest_segments(store, namespace_id, &manifest_object_id)
        .await
        .expect("load the current manifest's segments");
    segments.manifest().clone()
}

fn assert_revision_index_mismatch<T>(result: Result<T, ManifestLoadError>) {
    match result {
        Err(ManifestLoadError::RevisionIndexMismatch { .. }) => {}
        Err(other) => panic!("expected revision index mismatch, got {other:?}"),
        Ok(_) => panic!("expected revision index mismatch"),
    }
}

#[tokio::test]
async fn a_base_rebuild_drops_what_the_floor_covers_and_keeps_what_it_does_not() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for (path, body) in [
        ("/docs/one.txt", &b"one\n"[..]),
        ("/docs/two.txt", &b"two\n"[..]),
        ("/docs/a.txt", &b"alpha one\n"[..]),
        ("/docs/a.txt", &b"alpha two\n"[..]),
        ("/docs/tmp.txt", &b"scratch\n"[..]),
    ] {
        write_file_bytes(&store, &namespace_id, path, body, &context, None)
            .await
            .expect("write");
    }
    delete_path(&store, &namespace_id, "/docs/tmp.txt", &context, None)
        .await
        .expect("delete tmp");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let advanced = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance floor");
    let floor = advanced.retention_floor_seq;
    assert_eq!(
        floor,
        ChangeSeq(6),
        "every seeded commit sits at or below the floor"
    );

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
    // Checkpoints only append; dropping happens when reorganization folds
    // the runs against the advanced floor.
    let reorganized_manifest_no = drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        reorganized_manifest_no,
    )
    .await
    .expect("load manifest");

    let receipts = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataRowFamily::CommitReceipts,
    );
    assert!(!receipts.is_empty());
    for row in &receipts {
        if let MetadataRow::CommitReceipt { committed_seq, .. } = row {
            assert!(
                *committed_seq >= floor,
                "receipt below floor survived: {committed_seq:?}"
            );
        }
    }
    assert!(receipts.iter().any(|row| matches!(
        row,
        MetadataRow::CommitReceipt { committed_seq, .. } if *committed_seq == floor
    )));

    let revisions = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataRowFamily::Revisions,
    );
    let checksum_one = Checksum::sha256(b"alpha one\n");
    let checksum_two = Checksum::sha256(b"alpha two\n");
    assert!(revisions.iter().any(|row| matches!(
        row,
        MetadataRow::FileRevision { content_ref, .. } if content_ref.checksum == checksum_one
    )));
    assert!(revisions.iter().any(|row| matches!(
        row,
        MetadataRow::FileRevision { content_ref, .. } if content_ref.checksum == checksum_two
    )));
    let index_rows = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataRowFamily::RevisionsByInodeDesc,
    );
    assert_eq!(index_rows.len(), revisions.len());

    let binds = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataRowFamily::DirentryBinds,
    );
    assert!(!binds.iter().any(|row| matches!(
        row,
        MetadataRow::DirentryBind { display_name, .. } if display_name.as_str() == "tmp.txt"
    )));
    let unbinds = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataRowFamily::DirentryUnbinds,
    );
    assert!(
        unbinds.is_empty(),
        "spent unbind markers survived: {unbinds:?}"
    );

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
        ApiMetadataRowFamily::DirentryBinds,
        vec![bind(1, 0), bind(2, 1)],
    );
    rows.insert(
        ApiMetadataRowFamily::DirentryChildBinds,
        vec![bind(1, 0), bind(2, 1)],
    );
    rows.insert(ApiMetadataRowFamily::DirentryUnbinds, vec![unbind(1, 0, 2)]);

    fold_rows_with_retention(MetadataFamilyGroup::Bindings, &mut rows, ChangeSeq(1)).expect("drop");

    assert_eq!(rows[&ApiMetadataRowFamily::DirentryBinds].len(), 2);
    assert_eq!(rows[&ApiMetadataRowFamily::DirentryChildBinds].len(), 2);
    assert_eq!(rows[&ApiMetadataRowFamily::DirentryUnbinds].len(), 1);
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
    rows.insert(ApiMetadataRowFamily::DirentryBinds, vec![bind(0), bind(2)]);
    rows.insert(
        ApiMetadataRowFamily::DirentryChildBinds,
        vec![bind(0), bind(2)],
    );
    rows.insert(ApiMetadataRowFamily::DirentryUnbinds, vec![unbind]);

    fold_rows_with_retention(MetadataFamilyGroup::Bindings, &mut rows, ChangeSeq(1)).expect("drop");

    // Only the delta-2 rebind (the slot's latest) survives; the superseded
    // delta-0 bind and its spent unbind marker are gone from both families.
    for family in [
        ApiMetadataRowFamily::DirentryBinds,
        ApiMetadataRowFamily::DirentryChildBinds,
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
    assert!(rows[&ApiMetadataRowFamily::DirentryUnbinds].is_empty());
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
    rows.insert(ApiMetadataRowFamily::DirentryBinds, vec![bind(0), bind(1)]);

    let error = fold_rows_with_retention(MetadataFamilyGroup::Bindings, &mut rows, ChangeSeq(1))
        .expect_err("superseded live bind must refuse the drop");
    assert!(matches!(error, CoreError::NamespaceCorrupt(_)));
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
    if let MetadataRow::FileRevision { content_ref, .. } = first {
        content_ref.content_id = ContentId::generate();
    }
    rewrite_revision_index_segment(&store, &namespace_id, &mut manifest, revision_index_rows).await;
    overwrite_manifest(&store, &namespace_id, manifest).await;

    // Delta appends never re-read the base run; the reorganization merge that
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
            .expect("delta checkpoint");
    }
    let fold_policy = MetadataLsmPolicy {
        max_delta_runs: NonZeroUsize::MIN,
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
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_no)
            .await
            .expect("load manifest")
            .manifest;
    let descriptor = manifest
        .payload
        .segments
        .iter_mut()
        .find(|descriptor| descriptor.family == ApiMetadataRowFamily::RevisionsByInodeDesc)
        .expect("revision index descriptor");
    descriptor.row_count += 1;
    let manifest_object_id = manifest.payload.manifest_object_id.clone();
    overwrite_manifest(&store, &namespace_id, manifest).await;

    match load_verified_manifest_segments(&store, &namespace_id, &manifest_object_id).await {
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

    let reorganized_manifest_no = checkpoint_then_reorganize(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        reorganized_manifest_no,
    )
    .await
    .expect("load manifest");
    let mut manifest = materialized.manifest;
    let descriptor = manifest
        .payload
        .segments
        .iter_mut()
        .find(|descriptor| {
            descriptor.level == CHECKPOINT_BASE_RUN_LEVEL
                && descriptor.family == ApiMetadataRowFamily::Revisions
        })
        .expect("revision metadata file");
    // The descriptor is the only description of a segment; its index CRC
    // is what binds the manifest to the object's exact bytes.
    descriptor.index_block.crc32c ^= 0xffff_ffff;

    let manifest_key =
        metadata_manifest_object(&namespace_id, &manifest.payload.manifest_object_id);
    let manifest_no = manifest.payload.manifest_no;
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(manifest.payload).expect("updated manifest");
    let manifest_bytes =
        encode_namespace_manifest_json(&updated_manifest).expect("encode manifest");
    store
        .put_overwrite(&manifest_key, Bytes::from(manifest_bytes))
        .await
        .expect("overwrite manifest");

    match load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no).await {
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
async fn manifest_load_names_the_segment_codec_for_a_pre_commit_id_row() {
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
        .expect("checkpoint");
    let mut materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_no)
            .await
            .expect("load manifest before replacing a row");

    let row_key = loonfs_api::wire::manifest::lookup_keys::inode_key(loonfs_api::ROOT_INODE_ID);
    let mut builder = SegmentBlocksBuilder::default();
    builder
        .push(
            &row_key,
            &row_key,
            &PreCommitIdMetadataRow::Inode {
                inode_id: loonfs_api::ROOT_INODE_ID,
                inode_kind: loonfs_api::InodeKind::Directory,
                created_seq: ChangeSeq(0),
                created_by: loonfs_api::ActorRef::loonfs_system(),
                created_at_ms: context.now_ms,
            },
        )
        .expect("encode pre-change row");
    let built = builder.finish().expect("finish pre-change segment");
    let descriptor = materialized
        .manifest
        .payload
        .segments
        .iter_mut()
        .find(|descriptor| descriptor.family == ApiMetadataRowFamily::Inodes)
        .expect("inode segment");
    store
        .put_overwrite(
            &metadata_segment_object_key(descriptor),
            Bytes::from(built.bytes.clone()),
        )
        .await
        .expect("replace inode segment");
    descriptor.row_count = built.row_count;
    descriptor.min_row_key = built.min_row_key;
    descriptor.max_row_key = built.max_row_key;
    descriptor.index_block = built.index;
    descriptor.filter_inline = (built.filter.stored_len
        <= super::super::build::INLINE_SEGMENT_FILTER_MAX_BYTES)
        .then(|| {
            let start = built.filter.offset as usize;
            loonfs_api::wire::hex::hex_encode_bytes(
                &built.bytes[start..start + built.filter.stored_len as usize],
            )
        });
    descriptor.filter_block = built.filter;
    descriptor.object_checksum = loonfs_api::sha256_digest(&built.bytes);
    let segment_key = metadata_segment_object_key(descriptor);
    let manifest_no = materialized.manifest.payload.manifest_no;
    overwrite_manifest(&store, &namespace_id, materialized.manifest).await;

    match load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no).await {
        Err(ManifestLoadError::SegmentCodec {
            object_key,
            message,
        }) => {
            assert_eq!(object_key, segment_key);
            assert!(
                message.contains("missing field `commit_id`"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected named segment codec rejection, got {other:?}"),
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

    let manifest_no = checkpoint_then_reorganize(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no)
            .await
            .expect("load manifest");
    let base = base_tier(&materialized.manifest);
    let child_binds = base
        .iter()
        .find(|family_segments| {
            family_segments.family
                == loonfs_api::wire::manifest::MetadataRowFamily::DirentryChildBinds
        })
        .expect("child bind segments");
    let child_segment = child_binds.segments.first().expect("child bind segment");
    assert!(child_segment
        .min_row_key
        .starts_with("direntry-child-bind-000000000000000000"));

    let deleted_key = metadata_segment_object_key(child_segment);
    store
        .delete(&deleted_key)
        .await
        .expect("delete child index");
    match load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no).await {
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

    let manifest_no = checkpoint_then_reorganize(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no)
            .await
            .expect("load manifest before corruption");
    let mut manifest = materialized.manifest;
    let mut child_index_rows = manifest_rows_for_family(
        &materialized.metadata_state,
        ApiMetadataRowFamily::DirentryChildBinds,
    );
    assert!(child_index_rows.len() >= 2);
    child_index_rows[0] = child_index_rows[1].clone();
    child_index_rows
        .sort_by_key(|row| row.row_key_for_family(ApiMetadataRowFamily::DirentryChildBinds));

    let child_descriptor = manifest
        .payload
        .segments
        .iter_mut()
        .find(|descriptor| {
            descriptor.level == CHECKPOINT_BASE_RUN_LEVEL
                && descriptor.family == ApiMetadataRowFamily::DirentryChildBinds
        })
        .expect("child bind metadata file");
    rewrite_manifest_segment(
        &store,
        &namespace_id,
        manifest.payload.head_seq,
        ApiMetadataRowFamily::DirentryChildBinds,
        child_descriptor,
        child_index_rows,
        default_target_block_bytes(),
    )
    .await;

    let manifest_key =
        metadata_manifest_object(&namespace_id, &manifest.payload.manifest_object_id);
    let manifest_no = manifest.payload.manifest_no;
    let updated_manifest =
        NamespaceManifestEnvelope::from_payload(manifest.payload).expect("updated manifest");
    let manifest_bytes =
        encode_namespace_manifest_json(&updated_manifest).expect("encode updated manifest");
    store
        .put_overwrite(&manifest_key, Bytes::from(manifest_bytes))
        .await
        .expect("overwrite manifest");

    assert_child_index_mismatch(
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no).await,
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
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest.manifest_no)
            .await
            .expect("load manifest before corruption");
    let mut manifest = materialized.manifest;
    let manifest_no = manifest.payload.manifest_no;
    manifest
        .payload
        .segments
        .retain(|descriptor| descriptor.family != ApiMetadataRowFamily::RevisionsByInodeDesc);
    overwrite_manifest(&store, &namespace_id, manifest).await;

    assert_revision_index_mismatch(
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no).await,
    );
}

#[tokio::test]
async fn manifest_rejects_a_revision_desc_index_that_disagrees_with_its_family() {
    enum Rejection {
        IndexMismatch,
        DuplicateRow,
    }

    type IndexCorruption = (&'static str, fn(&mut Vec<MetadataRow>), Rejection);

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

    let (manifest_no, pristine_manifest, pristine_rows) =
        revision_index_test_materialization(&store, &namespace_id, &context).await;
    let index_key = metadata_segment_object_key(
        pristine_manifest
            .payload
            .segments
            .iter()
            .find(|descriptor| {
                descriptor.level == CHECKPOINT_BASE_RUN_LEVEL
                    && descriptor.family == ApiMetadataRowFamily::RevisionsByInodeDesc
            })
            .expect("revision index metadata file"),
    );
    let pristine_index_bytes = store
        .get(&index_key, None)
        .await
        .expect("read revision index segment")
        .expect("revision index segment exists");

    let cases: [IndexCorruption; 4] = [
        (
            "missing row",
            |rows| {
                rows.pop().expect("revision index row");
            },
            Rejection::IndexMismatch,
        ),
        (
            "extra row",
            |rows| {
                let extra = rows.first().expect("revision index row").clone();
                rows.push(match extra {
                    MetadataRow::FileRevision {
                        inode_id,
                        revision_no,
                        committed_seq,
                        commit_id,
                        committed_at_ms,
                        committed_by,
                        delta_index,
                        content_ref,
                    } => MetadataRow::FileRevision {
                        inode_id,
                        revision_no: loonfs_api::RevisionNo(revision_no.0 + 100),
                        committed_seq,
                        commit_id,
                        committed_at_ms,
                        committed_by,
                        delta_index,
                        content_ref,
                    },
                    other => other,
                });
            },
            Rejection::IndexMismatch,
        ),
        (
            "changed content ref",
            |rows| {
                let first = rows.first_mut().expect("revision index row");
                if let MetadataRow::FileRevision { content_ref, .. } = first {
                    content_ref.content_id = ContentId::generate();
                }
            },
            Rejection::IndexMismatch,
        ),
        (
            "duplicate row",
            |rows| {
                rows.push(rows.first().expect("revision index row").clone());
            },
            Rejection::DuplicateRow,
        ),
    ];

    for (label, corrupt, expected) in cases {
        store
            .put_overwrite(&index_key, pristine_index_bytes.clone())
            .await
            .expect("restore the revision index segment");
        let mut manifest = pristine_manifest.clone();
        let mut rows = pristine_rows.clone();
        corrupt(&mut rows);
        rewrite_revision_index_segment(&store, &namespace_id, &mut manifest, rows).await;
        overwrite_manifest(&store, &namespace_id, manifest).await;

        let result =
            load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no).await;
        match expected {
            Rejection::IndexMismatch => match result {
                Err(ManifestLoadError::RevisionIndexMismatch { .. }) => {}
                Err(other) => {
                    panic!("expected revision index mismatch for `{label}`, got {other:?}")
                }
                Ok(_) => panic!("expected revision index mismatch for `{label}`"),
            },
            Rejection::DuplicateRow => match result {
                Err(ManifestLoadError::DuplicateRevisionRow { family, .. }) => {
                    assert_eq!(
                        family,
                        ApiMetadataRowFamily::RevisionsByInodeDesc,
                        "for `{label}`"
                    );
                }
                other => panic!("expected duplicate revision row for `{label}`, got {other:?}"),
            },
        }
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
            basis_manifest_no: Some(materialization_before.root.manifest.manifest_no),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization_before.metadata_state,
        },
        MetadataLsmPolicy::default(),
        ManifestNo(2),
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
        materialization_after.root.manifest.manifest_no,
        first.manifest_no
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
    // Fold the delta runs so every inode row lands in one base segment.
    let manifest_no = checkpoint_then_reorganize(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no)
            .await
            .expect("load manifest before the rewrite");
    let mut manifest = materialized.manifest;
    let mut inode_rows =
        manifest_rows_for_family(&materialized.metadata_state, ApiMetadataRowFamily::Inodes);
    inode_rows.sort_by_key(|row| row.row_key_for_family(ApiMetadataRowFamily::Inodes));
    assert!(
        inode_rows.len() > 1,
        "the namespace should hold several inodes"
    );
    let last_inode_key = inode_rows
        .last()
        .expect("inode rows")
        .row_key_for_family(ApiMetadataRowFamily::Inodes);

    let base_inode_segments = manifest
        .payload
        .segments
        .iter()
        .filter(|descriptor| {
            descriptor.level == CHECKPOINT_BASE_RUN_LEVEL
                && descriptor.family == ApiMetadataRowFamily::Inodes
        })
        .count();
    assert_eq!(
        base_inode_segments, 1,
        "the folded base should hold one inode segment"
    );
    let descriptor = manifest
        .payload
        .segments
        .iter_mut()
        .find(|descriptor| {
            descriptor.level == CHECKPOINT_BASE_RUN_LEVEL
                && descriptor.family == ApiMetadataRowFamily::Inodes
        })
        .expect("base inode segment");
    // A one-byte target closes a block on every push, the last row
    // included. That is the shape a final row produces whenever it lands on
    // the target crossing, which happens at any block size.
    rewrite_manifest_segment(
        &store,
        &namespace_id,
        manifest.payload.head_seq,
        ApiMetadataRowFamily::Inodes,
        descriptor,
        inode_rows.clone(),
        NonZeroUsize::MIN,
    )
    .await;
    assert_eq!(
        descriptor.max_row_key, last_inode_key,
        "the descriptor must carry the segment's last row key"
    );
    overwrite_manifest(&store, &namespace_id, manifest).await;

    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let segments = load_verified_manifest_segments(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("the rewritten manifest should load");
    for row in &inode_rows {
        let key = row.row_key_for_family(ApiMetadataRowFamily::Inodes);
        let found = segments
            .get_for_lookup(ApiMetadataRowFamily::Inodes, &key, &key)
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
    let segments = load_verified_manifest_segments(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load segments");
    let payload = segments.manifest().payload.clone();

    // The read path assumes the filter block directly precedes the index
    // block, that an inline copy matches its handle's length, and that a
    // segment holding rows reports the key range those rows span; loading a
    // manifest that breaks any of them must fail instead of degrading.
    type Perturbation = fn(&mut MetadataSegmentRef);
    fn misalign_filter(descriptor: &mut MetadataSegmentRef) {
        descriptor.filter_block.offset -= 1;
    }
    fn truncate_inline(descriptor: &mut MetadataSegmentRef) {
        let inline = descriptor.filter_inline.as_mut().expect("inline filter");
        inline.truncate(inline.len() - 2);
    }
    // An empty max key sorts below every scan bound, so the segment would
    // answer no keyed lookup while still holding its rows.
    fn clear_max_key(descriptor: &mut MetadataSegmentRef) {
        assert!(descriptor.row_count > 0, "the segment should hold rows");
        descriptor.max_row_key.clear();
    }
    fn invert_key_range(descriptor: &mut MetadataSegmentRef) {
        assert!(descriptor.row_count > 0, "the segment should hold rows");
        std::mem::swap(&mut descriptor.min_row_key, &mut descriptor.max_row_key);
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
            .segments
            .iter_mut()
            // A segment spanning several keys, so swapping its bounds
            // actually descends.
            .find(|descriptor| {
                descriptor.filter_inline.is_some()
                    && descriptor.min_row_key != descriptor.max_row_key
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

#[tokio::test]
async fn a_manifest_whose_group_base_fragmented_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let manifest = current_manifest(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataRowFamily::Inodes);
    assert_eq!(
        group_base_runs(&manifest, group).len(),
        1,
        "the seed must leave the group in one base run"
    );

    let mut fragmented = manifest.payload.clone();
    // A second base run, not a second segment of the one already there, so
    // the copy takes a run number of its own out of the allocator.
    let second_base_run_no = fragmented.next_run_no;
    fragmented.next_run_no = RunNo(second_base_run_no.0 + 1);
    fragmented.segments.push(segment_modelled_on(
        &base_segment_of_family(&manifest, ApiMetadataRowFamily::Inodes),
        second_base_run_no,
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

#[tokio::test]
async fn a_manifest_that_numbers_one_family_twice_in_one_run_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let manifest = current_manifest(&store, &namespace_id).await;
    let existing = base_segment_of_family(&manifest, ApiMetadataRowFamily::Inodes);
    assert_eq!(existing.segment_index, 0);

    let mut repeated = manifest.payload.clone();
    repeated.segments.push(segment_modelled_on(
        &existing,
        existing.run_no,
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

#[tokio::test]
async fn a_manifest_whose_run_segments_overlap_in_key_range_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let manifest = current_manifest(&store, &namespace_id).await;
    let existing = base_segment_of_family(&manifest, ApiMetadataRowFamily::Inodes);
    assert_eq!(existing.segment_index, 0);

    let mut overlapping = manifest.payload.clone();
    let mut second =
        segment_modelled_on(&existing, existing.run_no, existing.run_seq, existing.level);
    // Index one keeps the numbering valid, leaving the overlapping range as
    // the only error.
    second.segment_index = 1;
    overlapping.segments.push(second);

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
