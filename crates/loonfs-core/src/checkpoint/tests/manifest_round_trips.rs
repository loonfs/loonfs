//! Checkpoint manifest construction, round trips, and strict consumption.

use super::*;

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
        DestinationBehavior::Replace,
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
    // The published manifest already covers the head: pinning writes a
    // record against it instead of materializing a new manifest.
    assert_eq!(checkpoint.manifest_id, ManifestId(1));
    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization.root.manifest_id, ManifestId(1));
    let record = load_checkpoint_record(&store, &namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert!(CheckpointId::parse(record.checkpoint_id.as_str()).is_ok());
    assert_eq!(record.manifest_head_seq, ChangeSeq(0));
    assert_eq!(record.manifest_id, ManifestId(1));
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
    let manifest_key = format!("{}{:020}-", metadata_manifest_prefix(&namespace_id), 2);
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
        Err(CoreError::NamespaceCorrupt(message))
            if message.contains("already exists with a different payload") => {}
        other => panic!("expected conflicting manifest corruption error, got {other:?}"),
    }

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization.root.manifest_id, ManifestId(1));
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
async fn maintenance_and_status_do_not_make_orphan_wal_visible() {
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
        &namespace_id,
        &loonfs_api::WalSegmentId::parse("00000000000000000002-deadbeefdeadbeef")
            .expect("valid WAL segment id"),
    );
    store
        .put_overwrite(&orphan_key, Bytes::from_static(b"not a wal envelope"))
        .await
        .expect("write orphan wal");
    let status = load_namespace_diagnostics(&store, &namespace_id)
        .await
        .expect("load namespace status");
    // Status reports the documented visible-chain length, not race-loser
    // objects that await garbage collection.
    assert_eq!(status.wal_tail_segments, 1);

    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention");

    let head_after = load_head_object(&store, &namespace_id)
        .await
        .expect("read head")
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
    assert_eq!(changes.changes[0].committed_seq, ChangeSeq(2));
}

#[tokio::test]
async fn checkpoint_records_are_standalone_files_one_per_pin() {
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
    // Pinning the same basis again is a second pin, not a second name for
    // the first one: its own record file under its own id, with the same
    // basis facts inside.
    let repeated = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("repeat checkpoint");
    assert_ne!(repeated.checkpoint_id, first.checkpoint_id);
    assert_eq!(repeated.manifest_id, first.manifest_id);
    assert_eq!(repeated.checkpoint_seq, first.checkpoint_seq);

    let record = load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(record.manifest_id, first.manifest_id);
    assert_eq!(record.manifest_head_seq, first.checkpoint_seq);
    assert_eq!(
        record.state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Active {}
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
        load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read first record")
            .is_some()
    );
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
        let record = load_checkpoint_record(&store, &namespace_id, &response.checkpoint_id)
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
    let manifest_key =
        metadata_manifest_object(&namespace_id, &bad_base_manifest.payload.manifest_object_id);
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
            &base_descriptor.owner_namespace_id,
            &base_descriptor.table_id,
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
    let manifest_key =
        metadata_manifest_object(&namespace_id, &bad_l0_manifest.payload.manifest_object_id);
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
        let expected = metadata_table(&l0_descriptor.owner_namespace_id, &l0_descriptor.table_id);
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
    let manifest = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_id: manifest_id(materialization.head.seq),
        manifest_object_id: manifest_object_id(manifest_id(materialization.head.seq)),
        head_seq: materialization.head.seq,
        head_commit_id: materialization.head.head_commit_id.clone(),
        base_seq: first,
        writer_epoch: materialization.head.writer_epoch,
        next_inode_id: materialization.head.next_inode_id,
        retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
        metadata_files,
    })
    .expect("build malformed manifest");

    match load_manifest_metadata_state_for_inspection_from_manifest(
        &store,
        &namespace_id,
        &metadata_manifest_object(&namespace_id, &manifest.payload.manifest_object_id),
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
        load_manifest_materialization_for_inspection(&store, &namespace_id, ManifestId(5))
            .await
            .expect("load chained manifest");
    // Always-append checkpoints keep the first published base (seq 0) and
    // chain one L0 run per checkpoint, the first included.
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
        max_rows_per_segment: NonZeroUsize::new(2)
            .expect("test segment row budget should be nonzero"),
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
        MetadataLsmPolicy::default(),
        ManifestId(1),
    )
    .await
    .expect("build manifest");
    let mut conflicting_payload = manifest.payload.clone();
    conflicting_payload.next_inode_id = InodeId(conflicting_payload.next_inode_id.0 + 1);
    let conflicting_manifest = NamespaceManifestEnvelope::from_payload(conflicting_payload)
        .expect("build conflicting manifest");

    write_namespace_manifest(&store, &manifest)
        .await
        .expect("first manifest write");
    let error = write_namespace_manifest(&store, &conflicting_manifest)
        .await
        .expect_err("different same-id manifest must conflict");

    assert_eq!(
        CoreError::MetadataProjection(error.clone()).code(),
        ErrorCode::NamespaceCorrupt
    );
    match error {
        MetadataProjectionLoadError::ManifestLoad(ManifestLoadError::ManifestObjectConflict {
            manifest_id,
            ..
        }) => {
            assert_eq!(manifest_id, ManifestId(1));
        }
        other => panic!("unexpected error: {other:?}"),
    }
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
        Some(materialization.root.manifest_object_id.clone()),
        context.now_ms,
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
    let record = load_checkpoint_record(&store, &namespace_id, &checkpoint.checkpoint_id)
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
