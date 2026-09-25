//! Multi-call namespace read snapshots.

use crate::common::{assert_core_error_kind, open_runtime_async, store};
use loonfs::{
    CreateNamespaceOptions, CreateSnapshotOptions, DestinationBehavior, ErrorCode, NamespaceId,
    PageRequest, PaginationPolicy, PutFileOptions,
};
use tempfile::tempdir;

async fn read_during_compaction_and_collection(
    durable: bool,
    pin_in_memory: bool,
) -> loonfs::Result<loonfs::PathEntry> {
    use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStore};
    use loonfs_test_support::stores::{
        BlockingStore, KeyPredicate, MetadataMapStore, OperationClass,
    };
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    let directory = tempdir().expect("tempdir");
    let old_segments = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let selected = old_segments.clone();
    let store = Arc::new(BlockingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::new(move |key| selected.lock().expect("old segments").contains(key)),
        ),
        KeyPredicate::metadata_segment(),
        OperationClass::Get,
    ));
    let runtime = open_runtime_async(store.clone(), "reader-gc-probe").await;
    let namespace = NamespaceId::parse("reader-gc-probe").expect("namespace");
    runtime
        .create_namespace(
            &namespace,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    for name in ["a", "b"] {
        runtime
            .put_file_bytes(
                &namespace,
                &format!("/{name}"),
                name.as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("file");
        runtime
            .maintenance
            .flush_wal(&namespace)
            .await
            .expect("flush");
    }
    let segment_keys = store
        .list_prefix(&loonfs_objectstore::keys::metadata_segment_prefix(
            &namespace,
        ))
        .await
        .expect("list metadata");
    old_segments
        .lock()
        .expect("old segments")
        .extend(segment_keys.into_iter().filter(|key| {
            loonfs_objectstore::layout::parse_object_key(key).is_some_and(|parsed| {
                parsed.family() == loonfs_objectstore::layout::DurableObjectFamily::MetadataSegment
            })
        }));
    assert!(!old_segments.lock().expect("old segments").is_empty());
    let snapshot = if durable {
        Some(
            runtime
                .writer
                .create_snapshot(
                    &namespace,
                    CreateSnapshotOptions {
                        name: "durable".to_owned(),
                        expires_at_ms: loonfs::current_time_ms().expect("time") + 60_000,
                    },
                )
                .await
                .expect("snapshot"),
        )
    } else {
        None
    };
    let reader = loonfs::FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("cold reader");
    let pinned = if let Some(snapshot) = &snapshot {
        Some(
            reader
                .pin_namespace_at_snapshot(&namespace, &snapshot.checkpoint_id)
                .await
                .expect("durable view"),
        )
    } else if pin_in_memory {
        Some(reader.pin_namespace(&namespace).await.expect("memory view"))
    } else {
        None
    };
    let captured = runtime
        .reader
        .get_path_entry(&namespace, "/a", Default::default())
        .await
        .expect("captured entry");
    store.block_next();
    let read = async {
        match &pinned {
            Some(pinned) => pinned.get_path_entry("/a", Default::default()).await,
            None => {
                reader
                    .get_path_entry(&namespace, "/a", Default::default())
                    .await
            }
        }
    };
    let maintenance = async {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            store.wait_until_blocked(),
        )
        .await
        .expect("read reached an uncached segment");
        runtime
            .put_file_bytes(
                &namespace,
                "/a",
                b"current",
                PutFileOptions {
                    behavior: DestinationBehavior::Replace,
                    ..PutFileOptions::new(loonfs_test_support::test_actor())
                },
            )
            .await
            .expect("replace captured file");
        runtime
            .maintenance
            .flush_wal(&namespace)
            .await
            .expect("flush replacement");
        let mut published = false;
        let mut converged = false;
        for _ in 0..16 {
            let outcome = runtime
                .maintenance
                .compact_metadata(&namespace)
                .await
                .expect("compact");
            if matches!(
                outcome.compaction,
                loonfs::MetadataCompactionOutcome::NotNeeded
            ) {
                converged = true;
                break;
            }
            assert!(
                matches!(
                    outcome.compaction,
                    loonfs::MetadataCompactionOutcome::BoundedMergePublished
                        | loonfs::MetadataCompactionOutcome::Published { .. }
                ),
                "unexpected compaction: {:?}",
                outcome.compaction
            );
            published = true;
        }
        assert!(published && converged);
        let gc = runtime
            .maintenance
            .gc_namespace(&namespace, &Default::default())
            .await
            .expect("GC");
        assert!(
            gc.deleted.metadata_segments > 0,
            "the pass really collected old segments"
        );
        store.release();
    };
    let (result, ()) = tokio::join!(read, maintenance);
    let fresh = loonfs::FsReader::builder_with_store(store)
        .build()
        .await
        .expect("fresh reader");
    let current = fresh
        .get_file_bytes(&namespace, "/a")
        .await
        .expect("current data remains readable");
    assert_eq!(current.bytes, b"current");
    if let Ok(entry) = &result {
        let current_entry = fresh
            .get_path_entry(&namespace, "/a", Default::default())
            .await
            .expect("current entry");
        assert_eq!(entry, if durable { &captured } else { &current_entry });
    }
    runtime.writer.shutdown().await.expect("shutdown");
    result
}

#[tokio::test]
async fn memory_pinned_read_returns_stale_head_after_compaction_and_collection() {
    let error = read_during_compaction_and_collection(false, true)
        .await
        .expect_err("captured segments were collected");
    assert_eq!(error.code(), ErrorCode::StaleHead);
}

#[tokio::test]
async fn durable_pinned_read_survives_compaction_and_collection() {
    read_during_compaction_and_collection(true, true)
        .await
        .expect("durable snapshot remains readable");
}

#[tokio::test]
async fn ordinary_read_returns_current_data_after_compaction_and_collection() {
    read_during_compaction_and_collection(false, false)
        .await
        .expect("one current read remains readable");
}

#[tokio::test]
async fn durable_pinned_reads_keep_missing_segments_corrupt_after_manifest_advance() {
    use loonfs_api::wire::manifest::MetadataRowFamily;
    use loonfs_core::control::load_namespace_current_manifest;
    use loonfs_objectstore::{keys, local_fs_store::LocalFsStore, ObjectStore};
    use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
    use std::sync::Arc;

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let namespace_id = NamespaceId::parse("missing-pinned-segment").expect("namespace");
    let runtime = open_runtime_async(store.clone(), "missing-pinned-segment").await;
    runtime
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/file",
            b"content",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("file");
    let snapshot = runtime
        .writer
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "durable".to_owned(),
                expires_at_ms: loonfs::current_time_ms().expect("time") + 60_000,
            },
        )
        .await
        .expect("snapshot");
    let checkpoint = runtime
        .create_checkpoint(&namespace_id)
        .await
        .expect("checkpoint");
    let reader = loonfs::FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("cold reader");
    let pinned_snapshot = reader
        .pin_namespace_at_snapshot(&namespace_id, &snapshot.checkpoint_id)
        .await
        .expect("snapshot view");
    let pinned_checkpoint = reader
        .pin_namespace_at_checkpoint(&namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("checkpoint view");
    let captured = load_namespace_current_manifest(store.as_ref(), &namespace_id)
        .await
        .expect("captured manifest");
    let segment = captured
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .find(|segment| segment.family == MetadataRowFamily::Inodes)
        .expect("inode segment");
    store
        .delete(&keys::metadata_segment_object_key(segment))
        .await
        .expect("delete pinned segment");
    loonfs_core::NamespaceEngine::writer(
        store.clone(),
        namespace_id.clone(),
        loonfs_api::WriterId::parse("manifest-advance").expect("writer id"),
    )
    .claim_compactor()
    .await
    .expect("advance current manifest");
    let current = load_namespace_current_manifest(store.as_ref(), &namespace_id)
        .await
        .expect("current manifest");
    assert!(current.state.manifest().manifest_no > captured.state.manifest().manifest_no);

    for pinned in [pinned_snapshot, pinned_checkpoint] {
        store.reset();
        assert_core_error_kind(
            pinned.get_path_entry("/file", Default::default()).await,
            ErrorCode::NamespaceCorrupt,
        );
        assert_eq!(store.count(OperationClass::Put), 0);
        assert_eq!(store.count(OperationClass::Delete), 0);
    }
    runtime.writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn snapshot_directory_cursor_resumes_only_at_its_snapshot() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-cursor-test").await;
    let namespace_id = NamespaceId::parse("snapshot-cursor").expect("namespace id");
    runtime
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    for name in ["a", "c", "e", "g"] {
        runtime
            .put_file_bytes(
                &namespace_id,
                &format!("/{name}.txt"),
                name.as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("seed file");
    }

    let now_ms = loonfs::current_time_ms().expect("current time");
    let first_snapshot = runtime
        .writer
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "first".to_owned(),
                expires_at_ms: now_ms + 60_000,
            },
        )
        .await
        .expect("create first snapshot");
    let first_view = runtime
        .reader
        .pin_namespace_at_snapshot(&namespace_id, &first_snapshot.checkpoint_id)
        .await
        .expect("pin first snapshot");
    let limit = PaginationPolicy::default()
        .resolve_limit(Some(2))
        .expect("page limit");
    let first_page = first_view
        .list_path_entries_page(
            "/",
            PageRequest {
                limit,
                cursor: None,
            },
            Default::default(),
        )
        .await
        .expect("list first snapshot page");
    let cursor = loonfs_api::decode_cursor::<loonfs::DirectoryPageCursor>(
        first_page.next_cursor.as_deref().expect("next cursor"),
    )
    .expect("decode cursor");
    assert_eq!(
        cursor.snapshot_id.as_ref(),
        Some(&first_snapshot.checkpoint_id)
    );

    runtime
        .put_file_bytes(
            &namespace_id,
            "/d.txt",
            b"d",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("change current directory");
    let second_snapshot = runtime
        .writer
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "second".to_owned(),
                expires_at_ms: now_ms + 60_000,
            },
        )
        .await
        .expect("create second snapshot");
    let second_view = runtime
        .reader
        .pin_namespace_at_snapshot(&namespace_id, &second_snapshot.checkpoint_id)
        .await
        .expect("pin second snapshot");

    assert_core_error_kind(
        second_view
            .list_path_entries_page(
                "/",
                PageRequest {
                    limit,
                    cursor: Some(cursor.clone()),
                },
                Default::default(),
            )
            .await,
        ErrorCode::InvalidRequest,
    );
    let mut unbound_cursor = cursor.clone();
    unbound_cursor.snapshot_id = None;
    assert_core_error_kind(
        second_view
            .list_path_entries_page(
                "/",
                PageRequest {
                    limit,
                    cursor: Some(unbound_cursor.clone()),
                },
                Default::default(),
            )
            .await,
        ErrorCode::InvalidRequest,
    );
    assert_core_error_kind(
        runtime
            .reader
            .list_path_entries_page(
                &namespace_id,
                "/",
                PageRequest {
                    limit,
                    cursor: Some(cursor.clone()),
                },
                Default::default(),
            )
            .await,
        ErrorCode::InvalidRequest,
    );
    let root_inode_id = first_page.entries[0]
        .parent_inode_id
        .expect("listed entry has root parent");
    assert_core_error_kind(
        runtime
            .reader
            .list_inode_children_page(
                &namespace_id,
                root_inode_id,
                PageRequest {
                    limit,
                    cursor: Some(cursor.clone()),
                },
                Default::default(),
            )
            .await,
        ErrorCode::InvalidRequest,
    );

    assert_core_error_kind(
        first_view
            .list_path_entries_page(
                "/",
                PageRequest {
                    limit,
                    cursor: Some(unbound_cursor),
                },
                Default::default(),
            )
            .await,
        ErrorCode::InvalidRequest,
    );

    let second_page = first_view
        .list_path_entries_page(
            "/",
            PageRequest {
                limit,
                cursor: Some(cursor),
            },
            Default::default(),
        )
        .await
        .expect("resume the original snapshot");
    assert_eq!(
        second_page
            .entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        ["/e.txt", "/g.txt"]
    );
}

#[tokio::test]
async fn pinned_namespace_reads_keep_one_head_across_later_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-reader-test").await;
    let namespace_id = NamespaceId::parse("snapshot-reads").expect("namespace id");
    runtime
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    let created = runtime
        .put_file_bytes(
            &namespace_id,
            "/before.txt",
            b"before",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create initial file");

    let snapshot = runtime
        .reader
        .pin_namespace(&namespace_id)
        .await
        .expect("pin namespace");
    assert_eq!(snapshot.head_seq(), created.committed_seq);
    let before = snapshot
        .get_path_entry("/before.txt", Default::default())
        .await
        .expect("resolve initial file");

    runtime
        .put_file_bytes(
            &namespace_id,
            "/before.txt",
            b"after",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace file after pin");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/later.txt",
            b"later",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create file after pin");

    let pinned_before = snapshot
        .get_path_entry("/before.txt", Default::default())
        .await
        .expect("resolve pinned file");
    assert_eq!(pinned_before.revision_no(), before.revision_no());
    assert_eq!(
        snapshot
            .read_content_ref(
                pinned_before.content_ref().expect("file content reference"),
                64,
            )
            .await
            .expect("read pinned content"),
        b"before"
    );
    let later_error = snapshot
        .get_path_entry("/later.txt", Default::default())
        .await
        .expect_err("later file is absent from the pinned view");
    assert_eq!(later_error.code(), ErrorCode::PathNotFound);

    let page = snapshot
        .list_path_entries_page(
            "/",
            PageRequest {
                limit: PaginationPolicy::default()
                    .resolve_limit(None)
                    .expect("default page limit"),
                cursor: None,
            },
            Default::default(),
        )
        .await
        .expect("list pinned root");
    assert_eq!(page.head_seq, snapshot.head_seq());
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        ["/before.txt"]
    );
    let states = snapshot
        .resolve_current_files(&[before.inode_id])
        .await
        .expect("resolve pinned inode");
    assert_eq!(states[0].current_revision_no, before.revision_no());

    let latest = runtime
        .reader
        .get_file_bytes(&namespace_id, "/before.txt")
        .await
        .expect("read latest replacement");
    assert_eq!(latest.bytes, b"after");
    runtime
        .reader
        .get_path_entry(&namespace_id, "/later.txt", Default::default())
        .await
        .expect("latest view sees later file");
}

#[tokio::test]
async fn pinned_checkpoint_reads_answer_the_state_the_checkpoint_captured() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-checkpoint-test").await;
    let namespace_id = NamespaceId::parse("snapshot-checkpoint-reads").expect("namespace id");
    runtime
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"pinned",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create initial file");
    let checkpoint = runtime
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"replaced",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace file after the checkpoint");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/later.txt",
            b"later",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create file after the checkpoint");

    let snapshot = runtime
        .reader
        .pin_namespace_at_checkpoint(&namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("pin namespace at checkpoint");
    let pinned = snapshot
        .get_path_entry("/pinned.txt", Default::default())
        .await
        .expect("resolve pinned file");
    assert_eq!(
        snapshot
            .read_content_ref(pinned.content_ref().expect("file content reference"), 64)
            .await
            .expect("read pinned content"),
        b"pinned"
    );
    let later_error = snapshot
        .get_path_entry("/later.txt", Default::default())
        .await
        .expect_err("later file is absent from the checkpointed view");
    assert_eq!(later_error.code(), ErrorCode::PathNotFound);

    let latest = runtime
        .reader
        .get_file_bytes(&namespace_id, "/pinned.txt")
        .await
        .expect("read latest replacement");
    assert_eq!(latest.bytes, b"replaced");
    runtime
        .reader
        .get_path_entry(&namespace_id, "/later.txt", Default::default())
        .await
        .expect("latest view sees later file");
}

#[tokio::test]
async fn a_deleted_checkpoint_refuses_a_pin_instead_of_reading_current_state() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime =
        open_runtime_async(store(temp_dir.path()), "snapshot-checkpoint-release-test").await;
    let namespace_id = NamespaceId::parse("snapshot-deleted-checkpoint").expect("namespace id");
    runtime
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"pinned",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create initial file");
    let checkpoint = runtime
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");
    runtime
        .maintenance
        .delete_checkpoint(&namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("release checkpoint");

    assert_core_error_kind(
        runtime
            .reader
            .pin_namespace_at_checkpoint(&namespace_id, &checkpoint.checkpoint_id)
            .await,
        ErrorCode::CheckpointUnavailable,
    );
}

#[tokio::test]
async fn snapshot_pins_serve_captured_state_and_enforce_release() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-lease-read-test").await;
    let namespace_id = NamespaceId::parse("snapshot-lease-reads").expect("namespace id");
    runtime
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"captured",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create captured file");
    let captured = runtime
        .reader
        .get_path_entry(&namespace_id, "/pinned.txt", Default::default())
        .await
        .expect("resolve captured file");
    let now_ms = loonfs::current_time_ms().expect("current time");
    let snapshot = runtime
        .writer
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "reader".to_owned(),
                expires_at_ms: now_ms + 60_000,
            },
        )
        .await
        .expect("create snapshot");
    let snapshot_options = loonfs::StatPathOptions {
        snapshot_id: Some(snapshot.checkpoint_id.clone()),
        ..Default::default()
    };
    assert_eq!(
        runtime
            .reader
            .get_path_entry(&namespace_id, "/pinned.txt", snapshot_options.clone())
            .await
            .expect("read with snapshot options"),
        captured,
    );

    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"current",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace captured file");
    let pinned = runtime
        .reader
        .pin_namespace_at_snapshot(&namespace_id, &snapshot.checkpoint_id)
        .await
        .expect("pin live snapshot");
    assert_eq!(
        pinned
            .get_path_entry("/pinned.txt", snapshot_options)
            .await
            .expect("read pinned entry with snapshot options"),
        captured,
    );
    assert_eq!(pinned.head_seq(), snapshot.captured_seq);
    assert_eq!(
        pinned
            .get_file_bytes("/pinned.txt")
            .await
            .expect("read captured bytes")
            .bytes,
        b"captured"
    );
    let download = pinned
        .create_download("/pinned.txt")
        .await
        .expect("resolve captured download");
    assert_eq!(
        download.revision_no,
        captured.revision_no().expect("file revision")
    );
    assert_eq!(
        &download.content_ref,
        captured.content_ref().expect("content reference")
    );

    runtime
        .writer
        .delete_snapshot(&namespace_id, &snapshot.checkpoint_id)
        .await
        .expect("release snapshot");
    assert_core_error_kind(
        runtime
            .reader
            .pin_namespace_at_snapshot(&namespace_id, &snapshot.checkpoint_id)
            .await,
        ErrorCode::SnapshotNotFound,
    );
}

#[tokio::test]
async fn a_pinned_reader_rejects_options_naming_another_snapshot() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-mismatch-read-test").await;
    let namespace_id = NamespaceId::parse("snapshot-mismatch-reads").expect("namespace id");
    runtime
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"captured",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create captured file");
    let now_ms = loonfs::current_time_ms().expect("current time");
    let mut snapshots = Vec::new();
    for name in ["first", "second"] {
        snapshots.push(
            runtime
                .writer
                .create_snapshot(
                    &namespace_id,
                    CreateSnapshotOptions {
                        name: name.to_owned(),
                        expires_at_ms: now_ms + 60_000,
                    },
                )
                .await
                .expect("create snapshot"),
        );
    }
    let pinned = runtime
        .reader
        .pin_namespace_at_snapshot(&namespace_id, &snapshots[0].checkpoint_id)
        .await
        .expect("pin the first snapshot");
    let other = snapshots[1].checkpoint_id.clone();
    let limit = PaginationPolicy::default()
        .resolve_limit(None)
        .expect("limit");
    let root = pinned
        .get_path_entry("/", Default::default())
        .await
        .expect("root entry")
        .inode_id;

    let assert_rejected = |result: loonfs::Result<()>| {
        let error = result
            .expect_err("naming another snapshot is rejected")
            .to_api_error();
        assert_eq!(error.code, "invalid_request");
        assert_eq!(error.param.as_deref(), Some("snapshot_id"));
    };
    assert_rejected(
        pinned
            .get_path_entry(
                "/pinned.txt",
                loonfs::StatPathOptions {
                    snapshot_id: Some(other.clone()),
                    ..Default::default()
                },
            )
            .await
            .map(drop),
    );
    assert_rejected(
        pinned
            .get_inode(
                root,
                loonfs::StatPathOptions {
                    snapshot_id: Some(other.clone()),
                    ..Default::default()
                },
            )
            .await
            .map(drop),
    );
    assert_rejected(
        pinned
            .list_path_entries_page(
                "/",
                PageRequest {
                    limit,
                    cursor: None,
                },
                loonfs::ListPathEntriesOptions {
                    snapshot_id: Some(other.clone()),
                    ..Default::default()
                },
            )
            .await
            .map(drop),
    );
    assert_rejected(
        pinned
            .list_inode_children_page(
                root,
                PageRequest {
                    limit,
                    cursor: None,
                },
                loonfs::ListInodeChildrenOptions {
                    snapshot_id: Some(other),
                    ..Default::default()
                },
            )
            .await
            .map(drop),
    );
}

#[tokio::test]
async fn a_missing_current_segment_stays_corrupt_and_manifest_read_failures_propagate() {
    use loonfs_objectstore::{keys, local_fs_store::LocalFsStore, ObjectStore};
    use loonfs_test_support::stores::{
        FailStore, InjectedError, KeyPredicate, OperationClass, RecordingStore,
    };
    use std::sync::Arc;

    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("missing-current-segment").expect("namespace");
    let failures = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationClass::Get,
        InjectedError::Transport("manifest read failed".to_owned()),
    ));
    let store = Arc::new(RecordingStore::new(failures.clone(), KeyPredicate::any()));
    let runtime = open_runtime_async(store.clone(), "missing-current-segment").await;
    runtime
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/file",
            b"content",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("file");
    runtime
        .maintenance
        .flush_wal(&namespace_id)
        .await
        .expect("flush");
    let reader = loonfs::FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("reader");
    let pinned = reader
        .pin_namespace(&namespace_id)
        .await
        .expect("captured view");
    for key in store
        .list_prefix(&keys::metadata_segment_prefix(&namespace_id))
        .await
        .expect("segments")
    {
        store.delete(&key).await.expect("delete segment");
    }
    store.reset();
    assert_core_error_kind(
        reader
            .get_path_entry(&namespace_id, "/file", Default::default())
            .await,
        ErrorCode::NamespaceCorrupt,
    );
    assert_eq!(
        store
            .snapshot()
            .iter()
            .filter(|operation| operation.key() == keys::hint(&namespace_id))
            .count(),
        2
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    assert_eq!(store.count(OperationClass::Delete), 0);

    failures.fail_all();
    store.reset();
    assert_core_error_kind(
        pinned.get_path_entry("/file", Default::default()).await,
        ErrorCode::ServerError,
    );
    assert_eq!(failures.attempts(), 1);
    assert_eq!(store.count(OperationClass::Put), 0);
    assert_eq!(store.count(OperationClass::Delete), 0);
    runtime.writer.shutdown().await.expect("shutdown");
}
