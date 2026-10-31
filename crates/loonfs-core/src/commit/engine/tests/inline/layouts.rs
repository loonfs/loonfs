//! Immutable extent writes, reads, downloads, and collection.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::storage::content::DurableContentValidationError;
use crate::storage::tail_content::{assemble_tail_content, write_tail_content, ContentWrites};
use crate::test_support::WriterAt;
use crate::GrantedRange;
use loonfs_objectstore::keys::{content_blob, content_span};
use loonfs_test_support::stores::MetadataMapStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{Checksum, ExtentObject};
use std::assert_matches;

async fn layout(
    store: &RecordingStore<LocalFsStore>,
    reference: &ContentRef,
) -> ContentLayoutRecord {
    crate::test_support::namespace_store(store, &reference.owner_namespace_id)
        .load_current_metadata_view()
        .await
        .expect("view")
        .projected_metadata_view()
        .content_layout(&reference.content_id)
        .await
        .expect("layout lookup")
        .expect("layout")
}

fn content_operations(store: &RecordingStore<LocalFsStore>) -> Vec<RecordedOperation> {
    store
        .snapshot()
        .into_iter()
        .filter(|operation| loonfs_objectstore::layout::content_id_of(operation.key()).is_some())
        .collect()
}

#[tokio::test]
async fn upload_evidence_carries_a_layout_on_every_reference() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let stored = crate::test_support::namespace_store(&store, &namespace_id)
        .store_bytes_as_content(b"uploaded")
        .await
        .expect("upload");
    for (commit_id, paths) in [("first", vec!["/one", "/two"]), ("again", vec!["/three"])] {
        let request = CommitIntent {
            commit_id: CommitId::parse(commit_id).expect("commit"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            preconditions: Vec::new(),
            operations: paths
                .into_iter()
                .map(|path| put(path, stored.content_ref()))
                .collect(),
        };
        publish(
            &mut engine,
            &context,
            CommitCandidate::prepared(
                request,
                vec![PreparedContent::for_durable_content_write(
                    stored.content_ref().clone(),
                )],
            ),
        )
        .await
        .expect("commit");
    }
    let anchor = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("anchor");
    let tail = crate::namespace::read_anchor::project_anchor_tail(
        &store,
        &crate::cache::MetadataSegmentCache::unshared(0),
        &anchor,
    )
    .expect("tail");
    assert_eq!(tail.revisions().count(), 3);
    let rows: Vec<_> = tail.content_layouts().collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].layout.extents[0].object, ExtentObject::Whole);
    assert_eq!(rows[0].size_bytes, 8);
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold upload layout");
}

#[tokio::test]
async fn folds_write_whole_then_spans_and_merge_small_tail_extents() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let mut bytes = vec![b'a'; 100];
    let (inode_id, original) = folded_file(&store, &mut engine, &context, &bytes).await;
    let first = layout(&store, &original).await;
    assert_eq!(first.layout.extents.len(), 1);
    assert_eq!(first.layout.extents[0].object, ExtentObject::Whole);
    for (revision, added, expected_lengths) in [
        (2_u8, 10, vec![100, 10]),
        (3, 3, vec![100, 10, 3]),
        (4, 2, vec![100, 15]),
    ] {
        let start = bytes.len();
        bytes.resize(start + added, b'b' + revision);
        let reference = commit_piece(
            &store,
            &namespace_id,
            (inode_id, RevisionNo(u64::from(revision))),
            &original.content_id,
            &bytes,
            start,
            None,
        )
        .await;
        store.reset();
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold");
        let operations = content_operations(&store);
        assert_eq!(
            operations
                .iter()
                .filter(|operation| matches!(operation, RecordedOperation::Assemble { .. }))
                .count(),
            1
        );
        let row = layout(&store, &reference).await;
        assert_eq!(
            row.layout
                .extents
                .iter()
                .map(|extent| extent.length)
                .collect::<Vec<_>>(),
            expected_lengths
        );
        assert_eq!(row.size_bytes, bytes.len() as u64);
        assert!(
            matches!(
                row.layout.extents.last().expect("tail").object,
                ExtentObject::Span {
                    start: 100,
                    end: 115
                }
            ) == (revision == 4)
        );
        assert_eq!(
            read_file(&store, &namespace_id, RevisionNo(u64::from(revision))).await,
            bytes
        );
    }
    assert_eq!(
        store
            .get(&content_blob(&namespace_id, &original.content_id), None)
            .await
            .expect("get")
            .expect("whole"),
        vec![b'a'; 100]
    );
}

#[tokio::test]
async fn three_extents_read_three_ranges_and_older_references_take_a_prefix() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let mut bytes = vec![b'a'; 100];
    let (inode_id, original) = folded_file(&store, &mut engine, &context, &bytes).await;
    let mut reference = original.clone();
    for (revision, added) in [(2, 10), (3, 3)] {
        let offset = bytes.len();
        bytes.resize(offset + added, b'b');
        reference = commit_piece(
            &store,
            &namespace_id,
            (inode_id, RevisionNo(revision)),
            &original.content_id,
            &bytes,
            offset,
            None,
        )
        .await;
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold");
    }
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let location = view
        .resolve_content_location(&reference)
        .await
        .expect("location");
    store.reset();
    assert_eq!(
        location
            .read_bytes(&store, None, &reference)
            .await
            .expect("read"),
        bytes
    );
    assert_eq!(store.counts().gets, 3);
    assert_eq!(store.counts().heads, 0);
    let prefix = ContentRef::blob_v1(
        namespace_id.clone(),
        original.content_id.clone(),
        &bytes[..105],
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let older = view
        .resolve_content_location(&prefix)
        .await
        .expect("prefix location");
    store.reset();
    assert_eq!(
        older
            .read_bytes(&store, None, &prefix)
            .await
            .expect("prefix"),
        bytes[..105]
    );
    assert_eq!(store.counts().gets, 2);
    let missing = content_span(&namespace_id, &original.content_id, 100, 110);
    store.delete(&missing).await.expect("delete span");
    assert_matches!(
        location.read_bytes(&store, None, &reference).await,
        Err(DurableContentValidationError::MissingContentObject { object_key })
            if object_key == missing
    );
}

#[tokio::test]
async fn a_merged_span_must_match_its_objects_stored_checksum() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let mut bytes = vec![b'a'; 100];
    let (inode_id, original) = folded_file(&store, &mut engine, &context, &bytes).await;
    bytes.resize(103, b'b');
    commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_id,
        &bytes,
        100,
        None,
    )
    .await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("span fold");
    let key = content_span(&namespace_id, &original.content_id, 100, 103);
    let incorrect = MetadataMapStore::new(
        store.clone(),
        KeyPredicate::exact(key.clone()),
        |mut metadata| {
            metadata.checksum = Some(Checksum::crc64nvme(b"bad"));
            metadata
        },
    );
    bytes.resize(105, b'c');
    commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(3)),
        &original.content_id,
        &bytes,
        103,
        None,
    )
    .await;
    store.reset();
    let error = crate::test_support::namespace_store(&incorrect, &namespace_id)
        .fold_wal()
        .await
        .expect_err("stored checksum mismatch");
    assert_matches!(error, CoreError::NamespaceCorrupt(message) if message.contains(&key));
    assert_no_writes(&store);
}

#[tokio::test]
async fn a_retry_accepts_a_span_with_a_matching_stored_checksum_without_changing_it() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let mut bytes = vec![b'a'; 100];
    let (inode_id, original) = folded_file(&store, &mut engine, &context, &bytes).await;
    bytes.resize(110, b'b');
    let reference = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_id,
        &bytes,
        100,
        None,
    )
    .await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let anchor = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("anchor");
    let tail = crate::namespace::read_anchor::project_anchor_tail(
        &store,
        &crate::cache::MetadataSegmentCache::unshared(0),
        &anchor,
    )
    .expect("tail");
    let content = tail.content(&reference).expect("pieces");
    let first = write_tail_content(
        ContentWrites::new(&store, &crate::test_support::content_writes()),
        &view.projected_metadata_view(),
        &content,
        assemble_tail_content(&content).expect("pieces"),
    )
    .await
    .expect("first write");
    let key = content_span(&namespace_id, &original.content_id, 100, 110);
    let before = store.head(&key).await.expect("head").expect("span");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("retry publishes");
    assert_eq!(layout(&store, &reference).await, first);
    assert_eq!(store.head(&key).await.expect("head").expect("span"), before);
}

#[tokio::test]
async fn a_retry_after_an_append_keeps_the_first_whole_object_immutable() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let original = inline(&namespace_id, Bytes::from_static(b"first"));
    publish(
        &mut engine,
        &context,
        candidate("file", vec![original.clone()]),
    )
    .await
    .expect("first commit");
    let anchor = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("anchor");
    let tail = crate::namespace::read_anchor::project_anchor_tail(
        &store,
        &crate::cache::MetadataSegmentCache::unshared(0),
        &anchor,
    )
    .expect("tail");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let content = tail.content(original.content_ref()).expect("content");
    write_tail_content(
        ContentWrites::new(&store, &crate::test_support::content_writes()),
        &view.projected_metadata_view(),
        &content,
        assemble_tail_content(&content).expect("pieces"),
    )
    .await
    .expect("whole before manifest");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    assert_eq!(
        view.direct_download_target(
            "/file-0",
            None,
            0,
            &access,
            &crate::test_support::content_writes(),
        )
        .await
        .expect("equal whole download")
        .ranges[0]
            .object_key,
        content_blob(&namespace_id, &original.content_ref().content_id)
    );
    let inode_id = tail.revisions().next().expect("revision").inode_id;
    let reference = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_ref().content_id,
        b"first+next",
        5,
        None,
    )
    .await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("view after append");
    for (revision, expected) in [
        (RevisionNo(1), b"first".as_slice()),
        (RevisionNo(2), b"first+next".as_slice()),
    ] {
        let target = view
            .direct_download_target(
                "/file-0",
                Some(revision),
                0,
                &access,
                &crate::test_support::content_writes(),
            )
            .await
            .expect("download revision");
        let bytes = store
            .get(
                &target.ranges[0].object_key,
                Some(loonfs_objectstore::ByteRange {
                    start_inclusive: 0,
                    end_exclusive: target.content_ref.size_bytes,
                }),
            )
            .await
            .expect("get")
            .expect("object");
        assert_eq!(bytes, expected);
    }
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("retry after append");
    let row = layout(&store, &reference).await;
    assert_eq!(row.layout.extents.len(), 1);
    assert_eq!(
        row.layout.extents[0].object,
        ExtentObject::Span { start: 0, end: 10 }
    );
    assert_eq!(
        store
            .get(&content_blob(&namespace_id, &reference.content_id), None)
            .await
            .expect("get whole")
            .expect("whole"),
        b"first".as_slice()
    );
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(2)).await,
        b"first+next"
    );
}

#[tokio::test]
async fn a_fresh_chain_shares_its_bases_extents_without_copying() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let mut bytes = vec![b'a'; 100];
    let (inode_id, original) = folded_file(&store, &mut engine, &context, &bytes).await;
    for (revision_no, size, offset) in [(2, 105, 100), (3, 110, 105)] {
        bytes.resize(size, b'b');
        commit_piece(
            &store,
            &namespace_id,
            (inode_id, RevisionNo(revision_no)),
            &original.content_id,
            &bytes,
            offset,
            None,
        )
        .await;
    }
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold base span");
    engine.invalidate_projection();
    for (commit_id, operation) in [
        (
            "restore-prefix",
            FilesystemOperation::RestoreRevision {
                path: AbsolutePath::parse("/file-0").expect("path"),
                source_revision_no: RevisionNo(2),
            },
        ),
        (
            "append-prefix",
            FilesystemOperation::AppendFile {
                path: AbsolutePath::parse("/file-0").expect("path"),
                inline_content: vec![b'!'],
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
    ] {
        publish(
            &mut engine,
            &context,
            CommitCandidate::new(CommitIntent::single(
                CommitId::parse(commit_id).expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                operation,
            )),
        )
        .await
        .expect("publish");
    }
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let revision = view
        .projected_metadata_view()
        .latest_revision_head(inode_id)
        .await
        .expect("revision")
        .expect("file");
    let fresh = revision.content_ref;
    assert_ne!(fresh.content_id, original.content_id);
    let carried = layout(&store, &fresh).await;
    assert_eq!(carried.size_bytes, 105);
    assert_eq!(
        carried
            .layout
            .extents
            .iter()
            .map(|extent| extent.length)
            .collect::<Vec<_>>(),
        [100, 5]
    );
    bytes.truncate(105);
    bytes.push(b'!');
    store.reset();
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold shared base");
    let span = content_span(&namespace_id, &fresh.content_id, 105, 106);
    assert_matches!(
        content_operations(&store).as_slice(),
        [RecordedOperation::Assemble { key, bytes: 1, .. }] if key == &span
    );
    let row = layout(&store, &fresh).await;
    row.layout.validate(106).expect("shared prefix layout");
    let base = layout(&store, &original).await;
    assert_eq!(row.layout.extents[0], base.layout.extents[0]);
    let mut prefix = base.layout.extents[1].clone();
    prefix.length = 5;
    assert_eq!(row.layout.extents[1], prefix);
    assert_eq!(read_file(&store, &namespace_id, RevisionNo(5)).await, bytes);
}

#[tokio::test]
async fn downloads_cover_three_extents_from_inside_the_second_and_create_empty_objects() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let mut bytes = vec![b'a'; 100];
    let (inode_id, original) = folded_file(&store, &mut engine, &context, &bytes).await;
    for (revision, added) in [(2_u8, 10), (3, 3)] {
        let offset = bytes.len();
        bytes.resize(offset + added, b'b' + revision);
        commit_piece(
            &store,
            &namespace_id,
            (inode_id, RevisionNo(u64::from(revision))),
            &original.content_id,
            &bytes,
            offset,
            None,
        )
        .await;
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold extent");
    }
    let access = ReadAccess::live(Authorizer::Unrestricted);
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let content_writes = crate::test_support::content_writes();
    store.reset();
    let target = view
        .direct_download_target("/file-0", None, 105, &access, &content_writes)
        .await
        .expect("resumed ranges");
    assert_eq!(
        target.ranges,
        vec![
            GrantedRange {
                object_key: content_span(&namespace_id, &original.content_id, 100, 110),
                object_start: 5,
                length: 5,
            },
            GrantedRange {
                object_key: content_span(&namespace_id, &original.content_id, 110, 113),
                object_start: 0,
                length: 3,
            },
        ]
    );
    assert!(content_operations(&store).is_empty());
    let previous = view
        .direct_download_target(
            "/file-0",
            Some(RevisionNo(2)),
            105,
            &access,
            &content_writes,
        )
        .await
        .expect("previous revision");
    assert_eq!(previous.ranges, target.ranges[..1]);
    let empty = inline(&namespace_id, Bytes::new());
    engine.invalidate_projection();
    publish(
        &mut engine,
        &context,
        candidate("empty", vec![empty.clone()]),
    )
    .await
    .expect("empty commit");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let target = view
        .direct_download_target(
            "/empty-0",
            None,
            0,
            &access,
            &crate::test_support::content_writes(),
        )
        .await
        .expect("empty download");
    assert_eq!(target.ranges.len(), 1);
    assert_eq!(target.ranges[0].length, 0);
    assert_eq!(target.ranges[0].object_start, 0);
    assert_eq!(
        target.ranges[0].object_key,
        content_blob(&namespace_id, &empty.content_ref().content_id)
    );
    assert_eq!(
        store
            .head(&target.ranges[0].object_key)
            .await
            .expect("head")
            .expect("empty object")
            .size_bytes,
        0
    );
}

#[tokio::test]
async fn collection_sweeps_id_shards_and_keeps_a_shared_base_in_another_shard() {
    use crate::gc::GcOptions;
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let value = InlineContent::new(
        namespace_id.clone(),
        ContentId::parse("con_10000000000000000000000000").expect("base id"),
        Bytes::from_static(b"hello"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    publish(
        &mut engine,
        &context,
        candidate("file", vec![value.clone()]),
    )
    .await
    .expect("base file");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold base");
    let inode_id = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_metadata_view()
        .await
        .expect("view")
        .resolve_path(
            "/file-0",
            AttributeInclusion::Omit,
            &ReadAccess::live(Authorizer::Unrestricted),
        )
        .await
        .expect("base entry")
        .inode_id;
    let original = value.content_ref().clone();
    let fresh_id = ContentId::parse("con_00000000000000000000000000").expect("fresh id");
    let fresh = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &fresh_id,
        b"hello!",
        5,
        Some(layout(&store, &original).await.layout),
    )
    .await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let other = crate::storage::content_staging::stage_bytes_under_content_id(
        &store,
        namespace_id.clone(),
        ContentId::parse("con_40000000000000000000000000").expect("other id"),
        b"other",
    )
    .await
    .expect("other shard content");
    let candidate = CommitCandidate::prepared(
        CommitIntent::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            put("/other", other.content_ref()),
        ),
        vec![PreparedContent::for_durable_content_write(
            other.content_ref().clone(),
        )],
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .publish_candidates(vec![candidate])
        .await
        .pop()
        .expect("one outcome")
        .expect("other file");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold other shard");
    prune_revisions(&store, &namespace_id).await;
    let rooted_span = content_span(&namespace_id, &fresh.content_id, 5, 6);
    let superseded_span = content_span(&namespace_id, &fresh.content_id, 1, 2);
    store
        .put_immutable_verified(&superseded_span, Bytes::from_static(b"x"))
        .await
        .expect("old span");
    let unrooted = ContentId::parse("con_70000000000000000000000000").expect("unrooted id");
    let unrooted_keys = [
        content_blob(&namespace_id, &unrooted),
        content_span(&namespace_id, &unrooted, 1, 2),
    ];
    for key in &unrooted_keys {
        store
            .put_immutable_verified(key, Bytes::from_static(b"x"))
            .await
            .expect("unrooted object");
    }
    let aged = MetadataMapStore::aged(store.clone(), KeyPredicate::any());
    let options = GcOptions {
        content_shard_rows: 1,
        ..Default::default()
    };
    store.reset();
    let report = crate::test_support::namespace_store(&aged, &namespace_id)
        .gc(
            None,
            &options,
            &WriterAt {
                now_ms: options.grace_window_ms + 1,
                ..context.clone()
            }
            .clock(),
        )
        .await
        .expect("collect");
    assert_eq!(report.deleted.content_objects, 3);
    let prefix = loonfs_objectstore::keys::content_prefix(&namespace_id);
    let listed: Vec<_> = store
        .snapshot()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::List { prefix: listed } if listed.starts_with(&prefix) => {
                Some(listed)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        listed,
        (0..8)
            .map(|shard| format!("{prefix}con_{shard}"))
            .collect::<Vec<_>>()
    );
    assert!(store
        .head(&superseded_span)
        .await
        .expect("superseded span")
        .is_none());
    let shared_keys = [
        content_blob(&namespace_id, &original.content_id),
        rooted_span,
    ];
    for key in &shared_keys {
        assert!(store.head(key).await.expect("head").is_some(), "{key}");
    }
    for key in unrooted_keys {
        assert!(store.head(&key).await.expect("head").is_none(), "{key}");
    }
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(2)).await,
        b"hello!"
    );
    commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(3)),
        &ContentId::parse("con_20000000000000000000000000").expect("replacement id"),
        b"replacement",
        0,
        None,
    )
    .await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold replacement");
    prune_revisions(&store, &namespace_id).await;
    let report = crate::test_support::namespace_store(&aged, &namespace_id)
        .gc(
            None,
            &options,
            &WriterAt {
                now_ms: options.grace_window_ms + 1,
                ..context
            }
            .clock(),
        )
        .await
        .expect("collect pruned chains");
    assert_eq!(report.deleted.content_objects, shared_keys.len() as u64);
    for key in shared_keys {
        assert!(store.head(&key).await.expect("head").is_none(), "{key}");
    }
}

async fn prune_revisions(store: &RecordingStore<LocalFsStore>, namespace_id: &NamespaceId) {
    use crate::compaction::MetadataCompactionPolicy;
    use crate::manifest::RetentionTarget;
    crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(1_000),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("floor");
    for _ in 0..16 {
        if matches!(
            crate::test_support::NamespaceFixture::new(
                store,
                namespace_id,
                &crate::test_support::writer_at("test-writer", 0)
            )
            .engine()
            .compact_metadata_unit(
                None,
                MetadataCompactionPolicy::CompactImmediately,
                crate::compaction::CompactionAdmission::AnySize,
                loonfs_types::CompactorEpoch(0),
                &crate::compaction::MetadataCompactionCancellation::default()
            )
            .await
            .expect("compact"),
            crate::compaction::CompactionUnitOutcome::NotNeeded
        ) {
            break;
        }
    }
}

#[tokio::test]
async fn collection_keeps_shared_objects_named_only_by_an_unfolded_restore() {
    use crate::gc::GcOptions;
    use crate::manifest::metadata_basis_from_manifest;
    use loonfs_types::format::manifest::{MetadataRow, MetadataRowFamily};

    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    engine.invalidate_projection();
    publish(
        &mut engine,
        &context,
        CommitCandidate::new(CommitIntent::single(
            CommitId::parse("copy").expect("commit"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CopyPath {
                source_path: AbsolutePath::parse("/file-0").expect("path"),
                destination_path: AbsolutePath::parse("/b").expect("path"),
                precondition: Default::default(),
            },
        )),
    )
    .await
    .expect("copy the first chain");
    let shared = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &ContentId::generate(),
        b"hello!",
        5,
        Some(layout(&store, &original).await.layout),
    )
    .await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold shared chain");
    let shared_layout = layout(&store, &shared).await;
    commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(3)),
        &ContentId::generate(),
        b"replacement",
        0,
        None,
    )
    .await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/b", b"replacement-b", None)
        .await
        .expect("replace the other reference");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/b", None)
        .await
        .expect("delete the other file");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold replacements");
    let restored = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .restore_file_revision("/file-0", RevisionNo(2), None)
        .await
        .expect("unfolded restore");
    prune_revisions(&store, &namespace_id).await;
    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    assert!(manifest.payload().head_seq < restored.committed_seq);
    let basis = metadata_basis_from_manifest(&store, None, &manifest);
    let rows = basis
        .segments
        .scan_range_page_with_keys(
            MetadataRowFamily::Revisions,
            MetadataRowFamily::Revisions.row_key_prefix(),
            None,
            32,
        )
        .await
        .expect("manifest revisions");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, row)| matches!(
        row,
        MetadataRow::FileRevision(row) if row.content_ref.content_id != original.content_id && row.content_ref.content_id != shared.content_id
    )));
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("restored view");
    assert_eq!(
        view.projected_metadata_view()
            .content_layout(&shared.content_id)
            .await
            .expect("layout")
            .expect("carried layout")
            .layout,
        shared_layout.layout
    );
    let aged = MetadataMapStore::aged(store.clone(), KeyPredicate::any());
    let options = GcOptions::default();
    let report = crate::test_support::namespace_store(&aged, &namespace_id)
        .gc(
            None,
            &options,
            &WriterAt {
                now_ms: options.grace_window_ms + 1,
                ..context
            }
            .clock(),
        )
        .await
        .expect("collect after grace");
    assert_eq!(report.deleted.content_objects, 0);
    for extent in &shared_layout.layout.extents {
        let key = crate::storage::content_location::extent_object_key(extent);
        assert!(store.head(&key).await.expect("head").is_some(), "{key}");
    }
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(4)).await,
        b"hello!"
    );
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold restore");
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(4)).await,
        b"hello!"
    );
}

#[tokio::test]
async fn tail_revision_downloads_use_materialized_objects_in_either_order() {
    for revisions in [
        [RevisionNo(2), RevisionNo(1)],
        [RevisionNo(1), RevisionNo(2)],
    ] {
        let (_directory, store, mut engine, context) = setup().await;
        let namespace_id = engine.namespace.namespace_id.clone();
        let original = inline(&namespace_id, Bytes::from_static(b"first"));
        publish(
            &mut engine,
            &context,
            candidate("file", vec![original.clone()]),
        )
        .await
        .expect("put");
        publish(
            &mut engine,
            &context,
            CommitCandidate::new(CommitIntent::single(
                CommitId::parse("append").expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::AppendFile {
                    path: AbsolutePath::parse("/file-0").expect("path"),
                    inline_content: b"+next".to_vec(),
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            )),
        )
        .await
        .expect("append");
        let blocked = loonfs_test_support::stores::BlockingStore::new(
            store.clone(),
            KeyPredicate::content_blob(),
            loonfs_test_support::stores::OperationSelector::Put,
        );
        let namespace = crate::test_support::namespace_store(&blocked, &namespace_id);
        let view = namespace.load_current_metadata_view().await.expect("view");
        let access = ReadAccess::live(Authorizer::Unrestricted);
        for revision in revisions {
            let key = content_span(&namespace_id, &original.content_ref().content_id, 0, 10);
            let pool = crate::PermitPool::new(std::num::NonZeroUsize::MIN, None);
            let absent = store.head(&key).await.expect("head").is_none();
            if absent {
                blocked.block_next();
            }
            let mut download = std::pin::pin!(view.direct_download_target(
                "/file-0",
                Some(revision),
                0,
                &access,
                &pool,
            ));
            if absent {
                tokio::select! {
                    () = blocked.wait_until_blocked() => {}
                    result = &mut download => panic!("download finished before writing: {result:?}"),
                }
                assert_eq!(pool.counts().running, 1);
                blocked.release();
            }
            let target = download.await.expect("download revision");
            assert_eq!(pool.counts(), crate::PoolCounts::default());
            assert_eq!(target.ranges[0].object_key, key);
            assert_eq!(
                target.content_ref.content_id,
                original.content_ref().content_id
            );
            let expected = if revision == RevisionNo(1) {
                b"first".as_slice()
            } else {
                b"first+next".as_slice()
            };
            let bytes = store
                .get(
                    &key,
                    Some(loonfs_objectstore::ByteRange {
                        start_inclusive: 0,
                        end_exclusive: target.content_ref.size_bytes,
                    }),
                )
                .await
                .expect("read range")
                .expect("object");
            assert_eq!(bytes, expected);
        }
        let key = content_span(&namespace_id, &original.content_ref().content_id, 0, 10);
        let before = store.head(&key).await.expect("head").expect("span");
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold after downloads");
        assert_eq!(store.head(&key).await.expect("head").expect("span"), before);
    }
}

#[tokio::test]
async fn two_chains_in_one_fold_share_one_write_permit() {
    use loonfs_test_support::stores::{BlockingStore, ConcurrencyWatchStore, OperationSelector};
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let values = vec![
        inline(&namespace_id, Bytes::from_static(b"first")),
        inline(&namespace_id, Bytes::from_static(b"second")),
    ];
    publish(&mut engine, &context, candidate("files", values))
        .await
        .expect("publish");
    let blocked = ConcurrencyWatchStore::new(
        BlockingStore::new(
            store.clone(),
            KeyPredicate::content_blob(),
            OperationSelector::Put,
        ),
        KeyPredicate::content_blob(),
    );
    blocked.inner().block_next();
    store.reset();
    let pool = Arc::new(crate::PermitPool::new(std::num::NonZeroUsize::MIN, None));
    let deadline = OperationClock::start(0, Arc::new(StdMonotonicTimer::default()));
    let namespace = crate::NamespaceStore {
        content_writes: Arc::clone(&pool),
        ..crate::test_support::namespace_store(&blocked, &namespace_id)
    };
    let mut fold = std::pin::pin!(namespace.fold_wal_tail(None, &deadline));
    tokio::select! {
        () = blocked.inner().wait_until_blocked() => {}
        _ = &mut fold => panic!("fold finished before the content write"),
    }
    assert!(futures::poll!(&mut fold).is_pending());
    assert_eq!(pool.counts().running, 1);
    assert_eq!(blocked.puts().total, 1);
    blocked.inner().release();
    fold.await.expect("both chains finish");
    assert_eq!(blocked.puts().peak_in_flight, 1);
    assert_eq!(blocked.puts().total, 2);
    assert_eq!(pool.counts(), crate::PoolCounts::default());
    assert_eq!(
        content_operations(&store)
            .iter()
            .filter(|operation| matches!(operation, RecordedOperation::Assemble { .. }))
            .count(),
        2
    );
}

#[tokio::test]
async fn a_copy_carries_its_layout_without_shadowing_a_longer_fold() {
    use crate::path::write::PublishPlanningSession;

    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    engine.invalidate_projection();
    let target = inline(&namespace_id, Bytes::from_static(b"target"));
    publish(&mut engine, &context, candidate("target", vec![target]))
        .await
        .expect("target");
    let stale_layout = layout(&store, &original).await.layout;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let candidate = CommitCandidate::new(CommitIntent::single(
        CommitId::parse("copy").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/file-0").expect("path"),
            destination_path: AbsolutePath::parse("/target-0").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: loonfs_types::DestinationBehavior::Replace,
                ..Default::default()
            },
        },
    ));
    let content_writes = crate::test_support::content_writes();
    let session = PublishPlanningSession::new(
        view.head(),
        view.wal_tail(),
        view.projected_metadata_view(),
        ContentWrites::new(&namespace.store, &content_writes),
        context.now_ms,
    );
    let plan = session
        .prepare_commit(
            &candidate,
            crate::authorize::Authorizer::Unrestricted,
            candidate.semantic_identity(&namespace_id),
        )
        .await
        .expect("copy plan");
    assert!(plan.deltas.iter().any(|delta| matches!(&delta.delta,
        WalDelta::AppendFileRevision { layout: Some(layout), .. } if *layout == stale_layout)));
    drop(view);
    let extended = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_id,
        b"hello!",
        5,
        None,
    )
    .await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold append");
    let longer = layout(&store, &extended).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .append_wal_commit(
            plan.deltas.into_iter().map(|delta| delta.delta).collect(),
            Vec::new(),
        )
        .await
        .expect("stale copy");
    for _ in 0..2 {
        assert_eq!(layout(&store, &original).await, longer);
        assert_eq!(
            read_file(&store, &namespace_id, RevisionNo(2)).await,
            b"hello!"
        );
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace.load_current_metadata_view().await.expect("view");
        let read = view
            .read_file_revision_bytes(
                "/target-0",
                RevisionNo(2),
                None,
                &ReadAccess::live(Authorizer::Unrestricted),
            )
            .await
            .expect("copy bytes");
        assert_eq!(read.bytes, b"hello");
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold copy");
    }
}

#[tokio::test]
async fn downloads_report_unwritable_pieces_without_changing_content() {
    use loonfs_test_support::stores::{FailStore, InjectedError, OperationKind};

    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    publish(
        &mut engine,
        &context,
        candidate(
            "file",
            vec![
                inline(&namespace_id, Bytes::from_static(b"pieces")),
                inline(&namespace_id, Bytes::new()),
            ],
        ),
    )
    .await
    .expect("publish pieces");
    let denied = FailStore::matching(
        store,
        |operation| {
            matches!(
                operation.kind(),
                OperationKind::Put { .. } | OperationKind::Assemble { .. }
            ) && loonfs_objectstore::layout::content_id_of(operation.key()).is_some()
        },
        InjectedError::PermissionDenied("read-only store".to_owned()),
    );
    denied.fail_all();
    let namespace = crate::test_support::namespace_store(&denied, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    let content_writes = crate::test_support::content_writes();
    let store = denied.inner();
    store.reset();
    for path in ["/file-0", "/file-1"] {
        assert_matches!(
            view.direct_download_target(path, None, 10, &access, &content_writes)
                .await,
            Err(CoreError::ResumeOffsetOutOfRange { .. })
        );
    }
    assert_eq!(denied.attempts(), 0);
    for path in ["/file-0", "/file-1"] {
        assert_matches!(
            view.direct_download_target(path, None, 0, &access, &content_writes)
                .await,
            Err(CoreError::ContentNotMaterialized { .. })
        );
    }
    assert_eq!(denied.attempts(), 2);
    assert_no_writes(store);
}

#[tokio::test]
async fn references_to_pending_pieces_survive_layout_pruning_and_collection() {
    use crate::gc::GcOptions;
    use crate::manifest::metadata_basis_from_manifest;
    use crate::path::write::PublishPlanningSession;
    use loonfs_types::format::manifest::MetadataRowFamily;

    for restore in [true, false] {
        let (_directory, store, mut engine, context) = setup().await;
        let namespace_id = engine.namespace.namespace_id.clone();
        publish(
            &mut engine,
            &context,
            candidate(
                "target",
                vec![InlineContent::new(
                    namespace_id.clone(),
                    ContentId::parse("con_00000000000000000000000001").expect("content"),
                    Bytes::from_static(b"target"),
                    loonfs_types::ChecksumAlgorithm::Crc64nvme,
                )],
            ),
        )
        .await
        .expect("target");
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold target");
        engine.invalidate_projection();
        let original = InlineContent::new(
            namespace_id.clone(),
            ContentId::parse("con_00000000000000000000000004").expect("content"),
            Bytes::from_static(b"hello"),
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        publish(&mut engine, &context, candidate("file", vec![original]))
            .await
            .expect("file");
        publish(
            &mut engine,
            &context,
            CommitCandidate::new(CommitIntent::single(
                CommitId::parse("append").expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::AppendFile {
                    path: AbsolutePath::parse("/file-0").expect("path"),
                    inline_content: b"!".to_vec(),
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            )),
        )
        .await
        .expect("append");
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace.load_current_metadata_view().await.expect("view");
        let candidate = CommitCandidate::new(CommitIntent::single(
            CommitId::parse("reference").expect("commit"),
            loonfs_test_support::test_actor(),
            None,
            if restore {
                FilesystemOperation::RestoreRevision {
                    path: AbsolutePath::parse("/file-0").expect("path"),
                    source_revision_no: RevisionNo(2),
                }
            } else {
                FilesystemOperation::CopyPath {
                    source_path: AbsolutePath::parse("/file-0").expect("path"),
                    destination_path: AbsolutePath::parse("/target-0").expect("path"),
                    precondition: loonfs_types::DestinationPrecondition {
                        behavior: loonfs_types::DestinationBehavior::Replace,
                        ..Default::default()
                    },
                }
            },
        ));
        let content_writes = crate::test_support::content_writes();
        let session = PublishPlanningSession::new(
            view.head(),
            view.wal_tail(),
            view.projected_metadata_view(),
            ContentWrites::new(&namespace.store, &content_writes),
            context.now_ms,
        );
        let mut plan = session
            .prepare_commit(
                &candidate,
                crate::authorize::Authorizer::Unrestricted,
                candidate.semantic_identity(&namespace_id),
            )
            .await
            .expect("reference plan");
        let (content_ref, carried) = plan
            .deltas
            .iter()
            .find_map(|delta| match &delta.delta {
                WalDelta::AppendFileRevision {
                    content_ref,
                    layout: Some(layout),
                    ..
                } => Some((content_ref.clone(), layout.clone())),
                _ => None,
            })
            .expect("reference carries a layout");
        assert_eq!(carried.size_bytes(), 6);
        assert!(plan.appended.is_empty());
        drop(view);
        let replacement = InlineContent::new(
            namespace_id.clone(),
            ContentId::parse("con_00000000000000000000000003").expect("content"),
            Bytes::from_static(b"replacement"),
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let replacement = CommitCandidate::with_inline_content(
            CommitIntent::single(
                CommitId::parse("replace").expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::PutFile {
                    path: AbsolutePath::parse("/file-0").expect("path"),
                    content: loonfs_types::FileContent::Ref(replacement.content_ref().clone()),
                    behavior: loonfs_types::DestinationBehavior::Replace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            ),
            Vec::new(),
            vec![replacement],
        );
        publish(&mut engine, &context, replacement)
            .await
            .expect("replace source");
        let fold = engine.writer_position().expect("fold input");
        if restore {
            for delta in &mut plan.deltas {
                if let WalDelta::AppendFileRevision { revision_no, .. } = &mut delta.delta {
                    *revision_no = RevisionNo(4);
                }
            }
        }
        crate::test_support::namespace_store(&store, &namespace_id)
            .append_wal_commit(
                plan.deltas.into_iter().map(|delta| delta.delta).collect(),
                Vec::new(),
            )
            .await
            .expect("commit reference");
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal_tail(
                Some(fold),
                &OperationClock::start(0, Arc::new(StdMonotonicTimer::default())),
            )
            .await
            .expect("fold captured source");
        prune_revisions(&store, &namespace_id).await;
        let manifest = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("manifest");
        let basis = metadata_basis_from_manifest(&store, None, &manifest);
        let rows = basis
            .segments
            .scan_range_page_with_keys(
                MetadataRowFamily::ContentLayouts,
                &format!("content-layout-{}-", content_ref.content_id.body()),
                Some(&format!(
                    "content-layout-{}.",
                    content_ref.content_id.body()
                )),
                32,
            )
            .await
            .expect("layouts");
        assert!(rows.is_empty(), "the basis no longer names this chain");
        let options = GcOptions::default();
        let aged = MetadataMapStore::aged(store.clone(), KeyPredicate::any());
        crate::test_support::namespace_store(&aged, &namespace_id)
            .gc(
                None,
                &options,
                &WriterAt {
                    now_ms: options.grace_window_ms + 1,
                    ..context
                }
                .clock(),
            )
            .await
            .expect("collect");
        for extent in &carried.extents {
            let key = crate::storage::content_location::extent_object_key(extent);
            assert!(store.head(&key).await.expect("head").is_some(), "{key}");
        }
        for folded in [false, true] {
            if folded {
                crate::test_support::namespace_store(&store, &namespace_id)
                    .fold_wal()
                    .await
                    .expect("fold reference");
            }
            let namespace = crate::test_support::namespace_store(&store, &namespace_id);
            let view = namespace.load_current_metadata_view().await.expect("view");
            let bytes = view
                .read_file_revision_bytes(
                    if restore { "/file-0" } else { "/target-0" },
                    RevisionNo(if restore { 4 } else { 2 }),
                    None,
                    &ReadAccess::live(Authorizer::Unrestricted),
                )
                .await
                .expect("read reference");
            assert_eq!(bytes.bytes, b"hello!");
        }
    }
}
