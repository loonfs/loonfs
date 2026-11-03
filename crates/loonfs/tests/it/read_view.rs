//! Read views that keep related reads on one captured namespace state.

use crate::common::{assert_core_error_kind, metadata_options, open_runtime_async, store};
use loonfs::{
    ChangeSeq, CreateSnapshotOptions, CurrentFile, DestinationBehavior, ErrorCode, LoonFs,
    NamespaceId, PutFileOptions, RevisionNo,
};
use std::assert_matches;
use tempfile::tempdir;

/// Reads `/a` while a compaction and a collection remove the segments the
/// read started from, and hands back the cold reader that read it.
async fn read_during_compaction_and_collection(
    durable: bool,
    head_view: bool,
) -> (loonfs::Result<loonfs::PathEntry>, LoonFs<loonfs::ReadOnly>) {
    use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStoreExtension};
    use loonfs_test_support::stores::{
        BlockingStore, KeyPredicate, MetadataMapStore, OperationSelector,
    };
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("reader-gc-probe").expect("namespace");
    let old_segments = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let selected = old_segments.clone();
    // The old segments and every manifest report an ancient age. A superseded
    // manifest roots its segments while its successor is younger than the
    // grace, and this probe needs the collection to really remove them.
    let manifest_prefix = loonfs_objectstore::keys::metadata_manifest_prefix(&namespace);
    let store = Arc::new(BlockingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::new(move |key| {
                key.starts_with(&manifest_prefix)
                    || selected.lock().expect("old segments").contains(key)
            }),
        ),
        KeyPredicate::metadata_segment(),
        OperationSelector::Get,
    ));
    let runtime = open_runtime_async(store.clone(), "reader-gc-probe").await;
    let runtime_namespace = runtime.reader.namespace(&namespace);
    runtime
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace)
        .expect("open namespace");
    for name in ["a", "b"] {
        runtime
            .put_file(
                &namespace,
                &format!("/{name}"),
                name.as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("file");
        runtime
            .maintenance
            .maintain_metadata_with_options(
                &namespace,
                &loonfs::MaintainMetadataOptions {
                    compaction_policy: loonfs::MetadataCompactionPolicy::CompactImmediately,
                    ..metadata_options(1)
                },
            )
            .await
            .expect("fold and compact");
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
            namespace_writer
                .create_snapshot_with_options(
                    "durable",
                    &CreateSnapshotOptions {
                        ttl_ms: std::num::NonZeroU64::new(60_000),
                    },
                )
                .await
                .expect("snapshot"),
        )
    } else {
        None
    };
    let reader = loonfs::LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("cold reader");
    let cold_namespace = reader.namespace(&namespace);
    let view = if let Some(snapshot) = &snapshot {
        Some(
            cold_namespace
                .read_view_at_snapshot(&snapshot.snapshot_id)
                .await
                .expect("snapshot view"),
        )
    } else if head_view {
        Some(cold_namespace.read_view().await.expect("head view"))
    } else {
        None
    };
    let captured = runtime_namespace.stat("/a").await.expect("captured entry");
    store.block_next();
    let read = async {
        match &view {
            Some(view) => view.stat("/a").await,
            None => cold_namespace.stat("/a").await,
        }
    };
    let maintenance = async {
        loonfs_test_support::clock::bounded(
            std::time::Duration::from_secs(10),
            store.wait_until_blocked(),
        )
        .await
        .expect("read reached an uncached segment");
        runtime
            .put_file_with_options(
                &namespace,
                "/a",
                b"current",
                &loonfs_test_support::test_actor(),
                &PutFileOptions {
                    behavior: DestinationBehavior::Replace,
                    ..Default::default()
                },
            )
            .await
            .expect("replace captured file");
        runtime
            .maintenance
            .fold_wal(&namespace)
            .await
            .expect("fold replacement");
        let mut published = false;
        let mut converged = false;
        for _ in 0..16 {
            let outcome = runtime
                .maintenance
                .compact_metadata(&namespace, &loonfs::MaintenanceCancellation::new())
                .await
                .expect("compact");
            if matches!(
                outcome.compaction,
                loonfs::MetadataCompactionOutcome::NotNeeded
            ) {
                converged = true;
                break;
            }
            assert_matches!(
                outcome.compaction,
                loonfs::MetadataCompactionOutcome::Published { .. },
                "unexpected compaction: {:?}",
                outcome.compaction
            );
            published = true;
        }
        assert!(published && converged);
        let gc = runtime.maintenance.gc(&namespace).await.expect("GC");
        assert!(
            gc.deleted.metadata_segments > 0,
            "the pass really collected old segments"
        );
        store.release();
    };
    let (result, ()) = tokio::join!(read, maintenance);
    let fresh = loonfs::LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let fresh_namespace = fresh.namespace(&namespace);
    let current = fresh_namespace
        .read_file("/a")
        .await
        .expect("current data remains readable");
    assert_eq!(current.bytes, b"current");
    if let Ok(entry) = &result {
        let current_entry = fresh_namespace.stat("/a").await.expect("current entry");
        assert_eq!(entry, if durable { &captured } else { &current_entry });
    }
    runtime.writer.shutdown().await.expect("shutdown");
    (result, reader)
}

#[tokio::test]
async fn head_read_view_returns_stale_head_after_compaction_and_collection() {
    let error = read_during_compaction_and_collection(false, true)
        .await
        .0
        .expect_err("captured segments were collected");
    assert_eq!(error.code(), ErrorCode::StaleHead);
}

#[tokio::test]
async fn snapshot_read_view_survives_compaction_and_collection() {
    read_during_compaction_and_collection(true, true)
        .await
        .0
        .expect("durable snapshot remains readable");
}

#[tokio::test]
async fn ordinary_read_returns_current_data_after_compaction_and_collection() {
    let (result, reader) = read_during_compaction_and_collection(false, false).await;
    let entry = result.expect("one current read remains readable");

    let before_read = reader.metadata_cache().stats();
    let again = reader
        .namespace(&entry.namespace_id)
        .stat("/a")
        .await
        .expect("the read after the stale-head retry");
    assert_eq!(again, entry);
    let after_read = reader.metadata_cache().stats();
    assert_eq!(after_read.wal_tails.misses, before_read.wal_tails.misses);
    assert_eq!(
        after_read.wal_tails.hits,
        before_read.wal_tails.hits + 1,
        "the next read reuses the tail the retry loaded"
    );
}

#[tokio::test]
async fn snapshot_views_with_and_without_an_expiry_keep_missing_segments_corrupt_after_manifest_advance(
) {
    use loonfs_objectstore::{keys, local_fs_store::LocalFsStore, ObjectStore};
    use loonfs_test_support::stores::{KeyPredicate, OperationSelector, RecordingStore};
    use loonfs_types::format::manifest::MetadataRowFamily;
    use std::sync::Arc;

    let directory = tempdir().expect("tempdir");
    // The writer raises the hint in the background after each publication,
    // so the recording leaves the hint out of the write counts.
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any().except_hints(),
    ));
    let namespace_id = NamespaceId::parse("missing-pinned-segment").expect("namespace");
    let runtime = open_runtime_async(store.clone(), "missing-pinned-segment").await;
    runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    runtime
        .put_file(
            &namespace_id,
            "/file",
            b"content",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("file");
    let snapshot = namespace_writer
        .create_snapshot_with_options(
            "durable",
            &CreateSnapshotOptions {
                ttl_ms: std::num::NonZeroU64::new(60_000),
            },
        )
        .await
        .expect("snapshot");
    let unexpiring = runtime
        .create_snapshot(&namespace_id)
        .await
        .expect("snapshot without an expiry");
    let reader = loonfs::LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("cold reader");
    let namespace = reader.namespace(&namespace_id);
    let snapshot_view = namespace
        .read_view_at_snapshot(&snapshot.snapshot_id)
        .await
        .expect("snapshot view");
    let unexpiring_view = namespace
        .read_view_at_snapshot(&unexpiring.snapshot_id)
        .await
        .expect("view of the snapshot without an expiry");
    let captured = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .load_current_manifest()
        .await
        .expect("captured manifest");
    let segment = captured
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .find(|segment| segment.family == MetadataRowFamily::DirentryBinds)
        .expect("direntry segment");
    store
        .delete(&keys::metadata_segment_object_key(segment))
        .await
        .expect("delete pinned segment");
    loonfs_core::NamespaceEngine::writer(
        loonfs_core::test_support::namespace_store(store.clone(), &namespace_id),
        loonfs_types::WriterId::parse("manifest-advance").expect("writer id"),
    )
    .claim_compactor()
    .await
    .expect("advance current manifest");
    let current = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert!(current.manifest().manifest_no > captured.manifest().manifest_no);

    for view in [snapshot_view, unexpiring_view] {
        store.reset();
        assert_core_error_kind(view.stat("/file").await, ErrorCode::NamespaceCorrupt);
        assert_eq!(store.count(OperationSelector::Put), 0);
        assert_eq!(store.count(OperationSelector::Delete), 0);
    }
    runtime.writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn snapshot_views_with_and_without_an_expiry_report_their_deleted_pin_when_a_segment_is_missing(
) {
    use loonfs_objectstore::{keys, ObjectStore};
    use loonfs_types::format::manifest::MetadataRowFamily;

    let directory = tempdir().expect("tempdir");
    let store = store(directory.path());
    let namespace_id = NamespaceId::parse("deleted-pin-segment").expect("namespace");
    let runtime = open_runtime_async(store.clone(), "deleted-pin-segment").await;
    runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    runtime
        .put_file(
            &namespace_id,
            "/file",
            b"content",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("file");
    let snapshot = namespace_writer
        .create_snapshot_with_options(
            "deleted",
            &CreateSnapshotOptions {
                ttl_ms: std::num::NonZeroU64::new(60_000),
            },
        )
        .await
        .expect("snapshot");
    let unexpiring = runtime
        .create_snapshot(&namespace_id)
        .await
        .expect("snapshot without an expiry");
    let reader = loonfs::LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("cold reader");
    let namespace = reader.namespace(&namespace_id);
    let snapshot_view = namespace
        .read_view_at_snapshot(&snapshot.snapshot_id)
        .await
        .expect("snapshot view");
    let unexpiring_view = namespace
        .read_view_at_snapshot(&unexpiring.snapshot_id)
        .await
        .expect("view of the snapshot without an expiry");
    namespace_writer
        .delete_snapshot(&snapshot.snapshot_id)
        .await
        .expect("delete snapshot");
    namespace_writer
        .delete_snapshot(&unexpiring.snapshot_id)
        .await
        .expect("delete the snapshot without an expiry");
    let manifest = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let segment = manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .find(|segment| segment.family == MetadataRowFamily::DirentryBinds)
        .expect("direntry segment");
    store
        .delete(&keys::metadata_segment_object_key(segment))
        .await
        .expect("delete pinned segment");

    assert_core_error_kind(
        snapshot_view.stat("/file").await,
        ErrorCode::SnapshotNotFound,
    );
    assert_core_error_kind(
        unexpiring_view.stat("/file").await,
        ErrorCode::SnapshotNotFound,
    );
    runtime.writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn snapshot_directory_cursor_resumes_only_at_its_snapshot() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-cursor-test").await;
    let namespace_id = NamespaceId::parse("snapshot-cursor").expect("namespace id");
    let namespace = runtime.reader.namespace(&namespace_id);
    runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for name in ["a", "c", "e", "g"] {
        runtime
            .put_file(
                &namespace_id,
                &format!("/{name}.txt"),
                name.as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("seed file");
    }

    let first_snapshot = namespace_writer
        .create_snapshot_with_options(
            "first",
            &CreateSnapshotOptions {
                ttl_ms: std::num::NonZeroU64::new(60_000),
            },
        )
        .await
        .expect("create first snapshot");
    let first_view = namespace
        .read_view_at_snapshot(&first_snapshot.snapshot_id)
        .await
        .expect("view first snapshot");
    let limit = loonfs_types::EffectiveLimit::from_requested(Some(2)).expect("page limit");
    let first_page = first_view
        .list("/")
        .page_limit(limit)
        .page()
        .await
        .expect("list first snapshot page");
    let cursor = loonfs_types::decode_cursor::<loonfs::DirectoryPageCursor>(
        first_page.next_cursor.as_deref().expect("next cursor"),
    )
    .expect("decode cursor");
    assert_eq!(cursor.pin_id.as_ref(), Some(&first_snapshot.snapshot_id));
    let mut unbound_cursor = cursor.clone();
    unbound_cursor.pin_id = None;
    let unbound_cursor = loonfs_types::encode_cursor(&unbound_cursor).expect("encode cursor");
    let cursor = first_page.next_cursor.clone().expect("next cursor");

    runtime
        .put_file(
            &namespace_id,
            "/d.txt",
            b"d",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("change current directory");
    let second_snapshot = namespace_writer
        .create_snapshot_with_options(
            "second",
            &CreateSnapshotOptions {
                ttl_ms: std::num::NonZeroU64::new(60_000),
            },
        )
        .await
        .expect("create second snapshot");
    let second_view = namespace
        .read_view_at_snapshot(&second_snapshot.snapshot_id)
        .await
        .expect("view second snapshot");

    assert_core_error_kind(
        second_view
            .list("/")
            .page_limit(limit)
            .cursor(Some(cursor.clone()))
            .page()
            .await,
        ErrorCode::InvalidRequest,
    );
    assert_core_error_kind(
        second_view
            .list("/")
            .page_limit(limit)
            .cursor(Some(unbound_cursor.clone()))
            .page()
            .await,
        ErrorCode::InvalidRequest,
    );
    assert_core_error_kind(
        namespace
            .list("/")
            .page_limit(limit)
            .cursor(Some(cursor.clone()))
            .page()
            .await,
        ErrorCode::InvalidRequest,
    );
    let root_inode_id = first_page.entries[0]
        .parent_inode_id
        .expect("listed entry has root parent");
    assert_core_error_kind(
        namespace
            .list_by_inode(root_inode_id)
            .page_limit(limit)
            .cursor(Some(cursor.clone()))
            .page()
            .await,
        ErrorCode::InvalidRequest,
    );

    assert_core_error_kind(
        first_view
            .list("/")
            .page_limit(limit)
            .cursor(Some(unbound_cursor))
            .page()
            .await,
        ErrorCode::InvalidRequest,
    );

    let second_page = first_view
        .list("/")
        .page_limit(limit)
        .cursor(Some(cursor))
        .page()
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
async fn a_read_view_keeps_one_head_across_later_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-reader-test").await;
    let namespace_id = NamespaceId::parse("snapshot-reads").expect("namespace id");
    let namespace = runtime.reader.namespace(&namespace_id);
    runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let created = runtime
        .put_file(
            &namespace_id,
            "/before.txt",
            b"before",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("create initial file");

    let view = namespace.read_view().await.expect("read view");
    assert_eq!(view.head_seq(), created.committed_seq);
    let before = view
        .stat("/before.txt")
        .await
        .expect("resolve initial file");

    runtime
        .put_file_with_options(
            &namespace_id,
            "/before.txt",
            b"after",
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("replace file after the view");
    runtime
        .put_file(
            &namespace_id,
            "/later.txt",
            b"later",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("create file after the view");

    let after_commits = view
        .stat("/before.txt")
        .await
        .expect("resolve file through the view");
    assert_eq!(after_commits.revision_no(), before.revision_no());
    assert_eq!(
        view.read_content(
            after_commits.content_ref().expect("file content reference"),
            64,
        )
        .await
        .expect("read content through the view"),
        b"before"
    );
    let later_error = view
        .stat("/later.txt")
        .await
        .expect_err("later file is absent from the view");
    assert_eq!(later_error.code(), ErrorCode::PathNotFound);

    let page = view
        .list("/")
        .page()
        .await
        .expect("list root through the view");
    assert_eq!(page.head_seq, view.head_seq());
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        ["/before.txt"]
    );
    let states = view
        .stat_current_files_by_inode(&[before.inode_id])
        .await
        .expect("resolve inode through the view");
    assert_eq!(
        states[0].file,
        CurrentFile::Visible {
            path: before.path.clone(),
            revision_no: before.revision_no(),
        }
    );

    let latest = namespace
        .read_file("/before.txt")
        .await
        .expect("read latest replacement");
    assert_eq!(latest.bytes, b"after");
    namespace
        .stat("/later.txt")
        .await
        .expect("latest view sees later file");
}

/// Checks that a view at an earlier sequence reads the namespace as it was at
/// that sequence, refuses a sequence above its head, and returns
/// `rebootstrap_required` once the retention floor passes the sequence.
#[tokio::test]
async fn a_view_at_an_earlier_sequence_reads_that_sequence_until_the_floor_passes_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let runtime = open_runtime_async(store.clone(), "earlier-view-test").await;
    let namespace_id = NamespaceId::parse("earlier-views").expect("namespace id");
    let actor = loonfs_test_support::test_actor();
    runtime
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    let first = runtime
        .put_file(&namespace_id, "/a.txt", b"one", &actor)
        .await
        .expect("create a");
    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..Default::default()
    };
    runtime
        .put_file_with_options(&namespace_id, "/a.txt", b"two", &actor, &replace)
        .await
        .expect("replace a");
    runtime
        .put_file(&namespace_id, "/b.txt", b"b", &actor)
        .await
        .expect("create b");

    let view = runtime
        .reader
        .namespace(&namespace_id)
        .read_view()
        .await
        .expect("read view");
    let earlier = view
        .at_seq(first.committed_seq)
        .await
        .expect("view at the first commit");
    assert_eq!(earlier.head_seq(), first.committed_seq);
    let a = earlier.stat("/a.txt").await.expect("stat a");
    assert_eq!(a.revision_no(), Some(RevisionNo(1)));
    assert_eq!(a.head_seq, first.committed_seq);
    assert_eq!(
        earlier
            .read_file_revision_by_inode(a.inode_id, RevisionNo(1), 64)
            .await
            .expect("read a's first revision"),
        b"one"
    );
    assert_eq!(
        earlier
            .stat_current_files_by_inode(&[a.inode_id])
            .await
            .expect("resolve a")[0]
            .file,
        CurrentFile::Visible {
            path: a.path.clone(),
            revision_no: Some(RevisionNo(1)),
        }
    );
    let later = earlier.stat("/b.txt").await.expect_err("b is later");
    assert_eq!(later.code(), ErrorCode::PathNotFound);
    let Err(ahead) = view.at_seq(ChangeSeq(view.head_seq().0 + 1)).await else {
        panic!("a sequence above the head should be refused");
    };
    assert_eq!(ahead.code(), ErrorCode::InvalidRequest);

    runtime
        .maintenance
        .maintain_metadata_with_options(&namespace_id, &metadata_options(1))
        .await
        .expect("fold the WAL");
    loonfs::test_support::maintenance_past_the_grace_window(store.clone())
        .await
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the retention floor");
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let view = reader
        .namespace(&namespace_id)
        .read_view()
        .await
        .expect("read view after the floor moved");
    let Err(error) = view.at_seq(first.committed_seq).await else {
        panic!("the floor passed the first commit");
    };
    assert_eq!(error.code(), ErrorCode::RebootstrapRequired);
}

/// Checks that a view lists every retained file revision, including the
/// revisions in its WAL tail and the revisions of a deleted file, by inode
/// and newest first, across pages.
#[tokio::test]
async fn a_view_lists_every_retained_revision_with_its_wal_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "retained-revisions-test").await;
    let namespace_id = NamespaceId::parse("retained-revisions").expect("namespace id");
    let actor = loonfs_test_support::test_actor();
    runtime
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    runtime
        .put_file(&namespace_id, "/a.txt", b"one", &actor)
        .await
        .expect("create a");
    runtime
        .maintenance
        .maintain_metadata_with_options(&namespace_id, &metadata_options(1))
        .await
        .expect("fold the first revision into the manifest");
    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..Default::default()
    };
    runtime
        .put_file_with_options(&namespace_id, "/a.txt", b"two", &actor, &replace)
        .await
        .expect("replace a");
    runtime
        .put_file(&namespace_id, "/b.txt", b"b", &actor)
        .await
        .expect("create b");
    runtime
        .namespace_writer(&namespace_id)
        .expect("open namespace")
        .delete_path("/b.txt", &actor)
        .await
        .expect("delete b");

    let view = runtime
        .reader
        .namespace(&namespace_id)
        .read_view()
        .await
        .expect("read view");
    let mut listed = Vec::new();
    let mut cursor = None;
    loop {
        let page = view
            .list_retained_revisions()
            .page_limit(loonfs::EffectiveLimit::from_requested(Some(1)).expect("limit"))
            .cursor(cursor)
            .page()
            .await
            .expect("list retained revisions");
        assert!(page.revisions.len() <= 1);
        listed.extend(page.revisions);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    let a = view.stat("/a.txt").await.expect("stat a").inode_id;
    assert_eq!(
        listed
            .iter()
            .map(|revision| (revision.inode_id == a, revision.revision_no))
            .collect::<Vec<_>>(),
        [
            (true, RevisionNo(2)),
            (true, RevisionNo(1)),
            (false, RevisionNo(1))
        ]
    );
    assert!(listed[0].inode_id < listed[2].inode_id);
}

/// Checks that a view below its manifest's head lists a full page of
/// retained revisions when the manifest's first rows are above the view's
/// sequence, and returns no cursor after its last revision.
#[tokio::test]
async fn a_view_below_the_manifest_head_lists_full_pages_of_retained_revisions() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "earlier-revisions-test").await;
    let namespace_id = NamespaceId::parse("earlier-revisions").expect("namespace id");
    let actor = loonfs_test_support::test_actor();
    runtime
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    runtime
        .put_file(&namespace_id, "/a.txt", b"one", &actor)
        .await
        .expect("create a");
    let b = runtime
        .put_file(&namespace_id, "/b.txt", b"b", &actor)
        .await
        .expect("create b");
    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..Default::default()
    };
    for content in [b"two", b"six"] {
        runtime
            .put_file_with_options(&namespace_id, "/a.txt", content, &actor, &replace)
            .await
            .expect("replace a");
    }
    runtime
        .maintenance
        .maintain_metadata_with_options(&namespace_id, &metadata_options(1))
        .await
        .expect("fold every revision into the manifest");

    let earlier = runtime
        .reader
        .namespace(&namespace_id)
        .read_view()
        .await
        .expect("read view")
        .at_seq(b.committed_seq)
        .await
        .expect("view at b's creation");
    // The revisions of `a` sort newest first, so the manifest's first two
    // rows are the two replacements, which the view doesn't see.
    let page = earlier
        .list_retained_revisions()
        .page_limit(loonfs::EffectiveLimit::from_requested(Some(2)).expect("limit"))
        .page()
        .await
        .expect("list retained revisions");
    let a = earlier.stat("/a.txt").await.expect("stat a").inode_id;
    assert_eq!(
        page.revisions
            .iter()
            .map(|revision| (revision.inode_id == a, revision.revision_no))
            .collect::<Vec<_>>(),
        [(true, RevisionNo(1)), (false, RevisionNo(1))]
    );
    assert_eq!(page.next_cursor, None);
}

#[tokio::test]
async fn snapshot_read_views_serve_captured_state_and_enforce_release() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-lease-read-test").await;
    let namespace_id = NamespaceId::parse("snapshot-lease-reads").expect("namespace id");
    let namespace = runtime.reader.namespace(&namespace_id);
    runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    runtime
        .put_file(
            &namespace_id,
            "/pinned.txt",
            b"captured",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("create captured file");
    let captured = namespace
        .stat("/pinned.txt")
        .await
        .expect("resolve captured file");
    let snapshot = namespace_writer
        .create_snapshot_with_options(
            "reader",
            &CreateSnapshotOptions {
                ttl_ms: std::num::NonZeroU64::new(60_000),
            },
        )
        .await
        .expect("create snapshot");
    let snapshot_options = loonfs::StatOptions {
        at: loonfs::ReadAt::Snapshot(snapshot.snapshot_id.clone()),
        ..Default::default()
    };
    assert_eq!(
        namespace
            .stat_with_options("/pinned.txt", &snapshot_options)
            .await
            .expect("read with snapshot options"),
        captured,
    );

    runtime
        .put_file_with_options(
            &namespace_id,
            "/pinned.txt",
            b"current",
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("replace captured file");
    let view = namespace
        .read_view_at_snapshot(&snapshot.snapshot_id)
        .await
        .expect("view live snapshot");
    assert_eq!(
        view.stat_with_options("/pinned.txt", &snapshot_options)
            .await
            .expect("read view entry with snapshot options"),
        captured,
    );
    assert_eq!(view.head_seq(), snapshot.captured_seq);
    assert_eq!(
        view.read_file("/pinned.txt")
            .await
            .expect("read captured bytes")
            .bytes,
        b"captured"
    );
    let download = view
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
    assert_eq!(
        view.read_file_by_inode(captured.inode_id)
            .await
            .expect("read captured bytes by inode")
            .bytes,
        b"captured"
    );
    let mut stream = view
        .read_file_stream_by_inode(captured.inode_id)
        .await
        .expect("stream captured bytes by inode");
    let mut streamed = Vec::new();
    while let Some(chunk) = stream.next_chunk().await.expect("captured chunk") {
        streamed.extend_from_slice(&chunk);
    }
    assert_eq!(streamed, b"captured");
    let inode_download = view
        .create_download_by_inode(captured.inode_id)
        .await
        .expect("resolve captured download by inode");
    assert_eq!(inode_download.revision_no, download.revision_no);
    assert_eq!(inode_download.content_ref, download.content_ref);

    namespace_writer
        .delete_snapshot(&snapshot.snapshot_id)
        .await
        .expect("release snapshot");
    assert_core_error_kind(
        namespace.read_view_at_snapshot(&snapshot.snapshot_id).await,
        ErrorCode::SnapshotNotFound,
    );
}

#[tokio::test]
async fn a_reader_judges_snapshot_expiry_on_its_own_wall_clock() {
    use loonfs_test_support::clock::ManualClock;
    use std::sync::Arc;

    const EXPIRES_AT_MS: u64 = 1_750_000_060_000;
    let temp_dir = tempdir().expect("tempdir");
    let clock = Arc::new(ManualClock::new(1_750_000_000_000));
    let writer = LoonFs::builder_with_store(store(temp_dir.path()))
        .writer_id(loonfs_test_support::ids::writer_id("snapshot-clock-writer"))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("build writer");
    let reader = LoonFs::builder_with_store(store(temp_dir.path()))
        .read_only()
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("build reader");
    let namespace_id = NamespaceId::parse("snapshot-clock").expect("namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let snapshot = namespace_writer
        .create_snapshot_with_options(
            "clock",
            &CreateSnapshotOptions {
                ttl_ms: std::num::NonZeroU64::new(EXPIRES_AT_MS - 1_750_000_000_000),
            },
        )
        .await
        .expect("create snapshot");
    assert_eq!(snapshot.expires_at_ms, Some(EXPIRES_AT_MS));

    clock.set_ms(EXPIRES_AT_MS - 1);
    for reader in [&reader, &writer.read_only()] {
        let namespace = reader.namespace(&namespace_id);
        let view = namespace
            .read_view_at_snapshot(&snapshot.snapshot_id)
            .await
            .expect("the snapshot is live one millisecond before its expiry");
        assert_eq!(view.head_seq(), snapshot.captured_seq);
    }
    clock.set_ms(EXPIRES_AT_MS);
    for reader in [&reader, &writer.read_only()] {
        let namespace = reader.namespace(&namespace_id);
        assert_core_error_kind(
            namespace.read_view_at_snapshot(&snapshot.snapshot_id).await,
            ErrorCode::SnapshotNotFound,
        );
    }
}

#[tokio::test]
async fn a_snapshot_read_view_rejects_options_naming_another_snapshot() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-mismatch-read-test").await;
    let namespace_id = NamespaceId::parse("snapshot-mismatch-reads").expect("namespace id");
    let namespace = runtime.reader.namespace(&namespace_id);
    runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    runtime
        .put_file(
            &namespace_id,
            "/pinned.txt",
            b"captured",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("create captured file");
    let mut snapshots = Vec::new();
    for name in ["first", "second"] {
        snapshots.push(
            namespace_writer
                .create_snapshot(name)
                .await
                .expect("create snapshot"),
        );
    }
    let view = namespace
        .read_view_at_snapshot(&snapshots[0].snapshot_id)
        .await
        .expect("view the first snapshot");
    let other = snapshots[1].snapshot_id.clone();
    let limit = loonfs_types::EffectiveLimit::DEFAULT;
    let root = view.stat("/").await.expect("root entry").inode_id;

    let assert_rejected = |result: loonfs::Result<()>| {
        let error = result
            .expect_err("naming another snapshot is rejected")
            .to_error_response();
        assert_eq!(error.code, "invalid_request");
        assert_eq!(error.param.as_deref(), Some("snapshot_id"));
    };
    assert_rejected(
        view.stat_with_options(
            "/pinned.txt",
            &loonfs::StatOptions {
                at: loonfs::ReadAt::Snapshot(other.clone()),
                ..Default::default()
            },
        )
        .await
        .map(drop),
    );
    assert_rejected(
        view.stat_by_inode_with_options(
            root,
            &loonfs::StatOptions {
                at: loonfs::ReadAt::Snapshot(other.clone()),
                ..Default::default()
            },
        )
        .await
        .map(drop),
    );
    assert_rejected(
        view.list_with_options(
            "/",
            &loonfs::ListOptions {
                at: loonfs::ReadAt::Snapshot(other.clone()),
                ..Default::default()
            },
        )
        .page_limit(limit)
        .page()
        .await
        .map(drop),
    );
    assert_rejected(
        view.list_by_inode_with_options(
            root,
            &loonfs::ListOptions {
                at: loonfs::ReadAt::Snapshot(other),
                ..Default::default()
            },
        )
        .page_limit(limit)
        .page()
        .await
        .map(drop),
    );
}

#[tokio::test]
async fn a_missing_current_segment_stays_corrupt_and_manifest_read_failures_propagate() {
    use loonfs_objectstore::{
        keys, local_fs_store::LocalFsStore, ObjectStore, ObjectStoreExtension,
    };
    use loonfs_test_support::stores::{
        FailStore, InjectedError, KeyPredicate, OperationSelector, RecordingStore,
    };
    use std::sync::Arc;

    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("missing-current-segment").expect("namespace");
    let failures = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::Get,
        InjectedError::Transport("manifest read failed".to_owned()),
    ));
    let store = Arc::new(RecordingStore::new(failures.clone(), KeyPredicate::any()));
    let runtime = open_runtime_async(store.clone(), "missing-current-segment").await;
    runtime
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    runtime
        .put_file(
            &namespace_id,
            "/file",
            b"content",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("file");
    runtime
        .writer
        .maintenance(loonfs_test_support::ids::writer_id(
            "missing-current-segment",
        ))
        .fold_wal(&namespace_id)
        .await
        .expect("fold");
    // A runtime skips a hint raise less than a second after its last raise
    // of the same hint, so the test raises the hint to the fold's manifest
    // before it counts the reader's hint reads.
    runtime.writer.drain().await.expect("drain");
    loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let reader = loonfs::LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("reader");
    let namespace = reader.namespace(&namespace_id);
    let view = namespace.read_view().await.expect("captured view");
    for key in store
        .list_prefix(&keys::metadata_segment_prefix(&namespace_id))
        .await
        .expect("segments")
    {
        store.delete(&key).await.expect("delete segment");
    }
    store.reset();
    assert_core_error_kind(namespace.stat("/file").await, ErrorCode::NamespaceCorrupt);
    assert_eq!(
        store
            .snapshot()
            .iter()
            .filter(|operation| operation.key() == keys::hint(&namespace_id))
            .count(),
        2
    );
    assert_eq!(store.count(OperationSelector::Put), 0);
    assert_eq!(store.count(OperationSelector::Delete), 0);

    failures.fail_all();
    store.reset();
    assert_core_error_kind(view.stat("/file").await, ErrorCode::ServerError);
    assert_eq!(failures.attempts(), 1);
    assert_eq!(store.count(OperationSelector::Put), 0);
    assert_eq!(store.count(OperationSelector::Delete), 0);
    runtime.writer.shutdown().await.expect("shutdown");
}
