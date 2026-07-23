//! Checkpoint secondary-index parity and manifest descriptor validation.

use super::*;

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
        display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
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
        max_l0_runs: NonZeroUsize::MIN,
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
            revision_delta_index,
            content_ref,
        } => MetadataRow::Revision {
            inode_id,
            revision_no: loonfs_api::RevisionNo(revision_no.0 + 100),
            committed_seq,
            committed_at_ms,
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
