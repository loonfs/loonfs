//! Immutable extent writes, reads, downloads, and collection.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::storage::content::DurableContentValidationError;
use crate::storage::tail_content::{assemble_tail_content, write_tail_content};
use crate::GrantedRange;
use loonfs_objectstore::keys::{content_blob, content_span};
use loonfs_test_support::stores::MetadataMapStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{Checksum, ExtentObject};
use tokio::sync::Semaphore;

async fn layout(
    store: &RecordingStore<LocalFsStore>,
    reference: &ContentRef,
) -> ContentLayoutRecord {
    load_current_metadata_view(store, &reference.owner_namespace_id)
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
    let namespace_id = engine.namespace_id.clone();
    let stored = store_bytes_as_content(&store, &namespace_id, b"uploaded")
        .await
        .expect("upload");
    for (commit_id, paths) in [("first", vec!["/one", "/two"]), ("again", vec!["/three"])] {
        let request = CommitRequest {
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
            &store,
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
    let anchor = crate::namespace::read_anchor::load_read_anchor(&store, &namespace_id)
        .await
        .expect("anchor");
    let tail = crate::namespace::read_anchor::project_anchor_tail(&store, None, &anchor)
        .await
        .expect("tail");
    assert_eq!(tail.rows.revisions().len(), 3);
    let rows = tail.rows.content_layouts();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].layout.extents[0].object, ExtentObject::Whole);
    assert_eq!(rows[0].size_bytes, 8);
    fold_wal(&store, &namespace_id)
        .await
        .expect("fold upload layout");
}

#[tokio::test]
async fn folds_write_whole_then_spans_and_merge_small_tail_extents() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let mut bytes = vec![b'a'; 100];
    let (inode_id, original) = folded_file(&store, &mut engine, &context, &bytes).await;
    let first = layout(&store, &original).await;
    assert_eq!(first.layout.extents.len(), 1);
    assert_eq!(first.layout.extents[0].object, ExtentObject::Whole);
    for (revision, added, expected_lengths) in [
        (2, 10, vec![100, 10]),
        (3, 3, vec![100, 10, 3]),
        (4, 2, vec![100, 15]),
    ] {
        let start = bytes.len();
        bytes.resize(start + added, b'b' + revision as u8);
        let reference = commit_piece(
            &store,
            &namespace_id,
            (inode_id, RevisionNo(revision)),
            &original.content_id,
            &bytes,
            start,
            None,
        )
        .await;
        store.reset();
        fold_wal(&store, &namespace_id).await.expect("fold");
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
            read_file(&store, &namespace_id, RevisionNo(revision)).await,
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
    let namespace_id = engine.namespace_id.clone();
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
        fold_wal(&store, &namespace_id).await.expect("fold");
    }
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let location = view
        .resolve_content_location(&reference)
        .await
        .expect("location");
    store.reset();
    assert_eq!(
        location.get_bytes(&store, &reference).await.expect("read"),
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
        older.get_bytes(&store, &prefix).await.expect("prefix"),
        bytes[..105]
    );
    assert_eq!(store.counts().gets, 2);
    let missing = content_span(&namespace_id, &original.content_id, 100, 110);
    store.delete(&missing).await.expect("delete span");
    assert!(
        matches!(location.get_bytes(&store, &reference).await, Err(DurableContentValidationError::MissingContentObject { object_key }) if object_key == missing)
    );
}

#[tokio::test]
async fn a_merged_span_must_match_its_objects_stored_checksum() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
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
    fold_wal(&store, &namespace_id).await.expect("span fold");
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
    let error = fold_wal(&incorrect, &namespace_id)
        .await
        .expect_err("stored checksum mismatch");
    assert!(matches!(error, CoreError::NamespaceCorrupt(message) if message.contains(&key)));
    assert_no_writes(&store);
}

#[tokio::test]
async fn a_retry_accepts_a_span_with_a_matching_stored_checksum_without_changing_it() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
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
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let anchor = crate::namespace::read_anchor::load_read_anchor(&store, &namespace_id)
        .await
        .expect("anchor");
    let tail = crate::namespace::read_anchor::project_anchor_tail(&store, None, &anchor)
        .await
        .expect("tail");
    let content = tail.content(&reference).expect("pieces");
    let first = write_tail_content(
        &store,
        &view.projected_metadata_view(),
        &tail,
        content,
        assemble_tail_content(content).expect("pieces"),
        &Semaphore::new(32),
    )
    .await
    .expect("first write");
    let key = content_span(&namespace_id, &original.content_id, 100, 110);
    let before = store.head(&key).await.expect("head").expect("span");
    fold_wal(&store, &namespace_id)
        .await
        .expect("retry publishes");
    assert_eq!(layout(&store, &reference).await, first);
    assert_eq!(store.head(&key).await.expect("head").expect("span"), before);
}

#[tokio::test]
async fn a_retry_after_an_append_keeps_the_first_whole_object_immutable() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let original = inline(&namespace_id, Bytes::from_static(b"first"));
    publish(
        &mut engine,
        &store,
        &context,
        candidate("file", vec![original.clone()]),
    )
    .await
    .expect("first commit");
    let anchor = crate::namespace::read_anchor::load_read_anchor(&store, &namespace_id)
        .await
        .expect("anchor");
    let tail = crate::namespace::read_anchor::project_anchor_tail(&store, None, &anchor)
        .await
        .expect("tail");
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let content = tail.content(original.content_ref()).expect("content");
    write_tail_content(
        &store,
        &view.projected_metadata_view(),
        &tail,
        content,
        assemble_tail_content(content).expect("pieces"),
        &Semaphore::new(32),
    )
    .await
    .expect("whole before manifest");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    assert_eq!(
        view.direct_download_target(
            &store,
            "/file-0",
            None,
            0,
            &access,
            &tokio::sync::Semaphore::new(32),
        )
        .await
        .expect("equal whole download")
        .ranges[0]
            .object_key,
        content_blob(&namespace_id, &original.content_ref().content_id)
    );
    let inode_id = tail.rows.revisions()[0].inode_id;
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
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view after append");
    for (revision, expected) in [
        (RevisionNo(1), b"first".as_slice()),
        (RevisionNo(2), b"first+next".as_slice()),
    ] {
        let target = view
            .direct_download_target(
                &store,
                "/file-0",
                Some(revision),
                0,
                &access,
                &tokio::sync::Semaphore::new(32),
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
    fold_wal(&store, &namespace_id)
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
    let namespace_id = engine.namespace_id.clone();
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
    fold_wal(&store, &namespace_id)
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
            &store,
            &context,
            CommitCandidate::new(CommitRequest::single(
                CommitId::parse(commit_id).expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                operation,
            )),
        )
        .await
        .expect("publish");
    }
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
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
    fold_wal(&store, &namespace_id)
        .await
        .expect("fold shared base");
    let span = content_span(&namespace_id, &fresh.content_id, 105, 106);
    assert!(
        matches!(content_operations(&store).as_slice(), [RecordedOperation::Assemble { key, bytes: 1, .. }] if key == &span)
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
    let namespace_id = engine.namespace_id.clone();
    let mut bytes = vec![b'a'; 100];
    let (inode_id, original) = folded_file(&store, &mut engine, &context, &bytes).await;
    for (revision, added) in [(2, 10), (3, 3)] {
        let offset = bytes.len();
        bytes.resize(offset + added, b'b' + revision as u8);
        commit_piece(
            &store,
            &namespace_id,
            (inode_id, RevisionNo(revision)),
            &original.content_id,
            &bytes,
            offset,
            None,
        )
        .await;
        fold_wal(&store, &namespace_id).await.expect("fold extent");
    }
    let access = ReadAccess::live(Authorizer::Unrestricted);
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let content_writes = Semaphore::new(32);
    store.reset();
    let target = view
        .direct_download_target(&store, "/file-0", None, 105, &access, &content_writes)
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
            &store,
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
        &store,
        &context,
        candidate("empty", vec![empty.clone()]),
    )
    .await
    .expect("empty commit");
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let target = view
        .direct_download_target(
            &store,
            "/empty-0",
            None,
            0,
            &access,
            &tokio::sync::Semaphore::new(32),
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
    use crate::gc::{gc_namespace, GcOptions};
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let value = InlineContent::new(
        namespace_id.clone(),
        ContentId::parse("con_10000000000000000000000000000000").expect("base id"),
        Bytes::from_static(b"hello"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    publish(
        &mut engine,
        &store,
        &context,
        candidate("file", vec![value.clone()]),
    )
    .await
    .expect("base file");
    fold_wal(&store, &namespace_id).await.expect("fold base");
    let inode_id = load_current_metadata_view(&store, &namespace_id)
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
    let fresh_id = ContentId::parse("con_00000000000000000000000000000000").expect("fresh id");
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
    fold_wal(&store, &namespace_id).await.expect("fold");
    let other = crate::storage::content::stage_bytes_under_content_id(
        &store,
        namespace_id.clone(),
        ContentId::parse("con_80000000000000000000000000000000").expect("other id"),
        b"other",
    )
    .await
    .expect("other shard content");
    let candidate = CommitCandidate::prepared(
        CommitRequest::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            put("/other", other.content_ref()),
        ),
        vec![PreparedContent::for_durable_content_write(
            other.content_ref().clone(),
        )],
    );
    crate::commit_engine::publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![candidate],
        &context,
        std::sync::Arc::new(tokio::sync::Semaphore::new(32)),
    )
    .await
    .pop()
    .expect("one outcome")
    .expect("other file");
    fold_wal(&store, &namespace_id)
        .await
        .expect("fold other shard");
    prune_revisions(&store, &namespace_id).await;
    let rooted_span = content_span(&namespace_id, &fresh.content_id, 5, 6);
    let superseded_span = content_span(&namespace_id, &fresh.content_id, 1, 2);
    store
        .put_immutable_verified(&superseded_span, Bytes::from_static(b"x"))
        .await
        .expect("old span");
    let unrooted = ContentId::parse("con_f0000000000000000000000000000000").expect("unrooted id");
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
    let report = gc_namespace(
        &aged,
        None,
        &namespace_id,
        &options,
        &MutationContext {
            now_ms: options.grace_window_ms + 1,
            ..context.clone()
        },
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
        (0..16)
            .map(|shard| format!("{prefix}con_{shard:x}"))
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
        &ContentId::parse("con_20000000000000000000000000000000").expect("replacement id"),
        b"replacement",
        0,
        None,
    )
    .await;
    fold_wal(&store, &namespace_id)
        .await
        .expect("fold replacement");
    prune_revisions(&store, &namespace_id).await;
    let report = gc_namespace(
        &aged,
        None,
        &namespace_id,
        &options,
        &MutationContext {
            now_ms: options.grace_window_ms + 1,
            ..context
        },
    )
    .await
    .expect("collect pruned chains");
    assert_eq!(report.deleted.content_objects, shared_keys.len() as u64);
    for key in shared_keys {
        assert!(store.head(&key).await.expect("head").is_none(), "{key}");
    }
}

async fn prune_revisions(store: &RecordingStore<LocalFsStore>, namespace_id: &NamespaceId) {
    use crate::manifest::{
        advance_retention_floor, compaction_step, MetadataCompactionPolicy, RetentionTarget,
    };
    advance_retention_floor(store, None, namespace_id, RetentionTarget::Head)
        .await
        .expect("floor");
    for _ in 0..16 {
        if matches!(
            compaction_step(
                store,
                namespace_id,
                loonfs_types::CompactorEpoch(0),
                Default::default(),
                MetadataCompactionPolicy::CompactImmediately,
                Arc::default()
            )
            .await
            .expect("compact"),
            crate::manifest::CompactionStepOutcome::NotNeeded { .. }
        ) {
            break;
        }
    }
}

#[tokio::test]
async fn collection_keeps_shared_objects_named_only_by_an_unfolded_restore() {
    use crate::gc::{gc_namespace, GcOptions};
    use crate::manifest::metadata_basis_from_manifest;
    use crate::namespace::control::load_current_manifest;
    use loonfs_types::format::manifest::{MetadataRow, MetadataRowFamily};

    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    engine.invalidate_projection();
    publish(
        &mut engine,
        &store,
        &context,
        CommitCandidate::new(CommitRequest::single(
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
    fold_wal(&store, &namespace_id)
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
    crate::test_support::ops::write_file_bytes(
        &store,
        &namespace_id,
        "/b",
        b"replacement-b",
        &context,
        None,
    )
    .await
    .expect("replace the other reference");
    crate::test_support::ops::delete_path(&store, &namespace_id, "/b", &context, None)
        .await
        .expect("delete the other file");
    fold_wal(&store, &namespace_id)
        .await
        .expect("fold replacements");
    let restored = crate::test_support::ops::restore_file_revision(
        &store,
        &namespace_id,
        "/file-0",
        RevisionNo(2),
        &context,
        None,
    )
    .await
    .expect("unfolded restore");
    prune_revisions(&store, &namespace_id).await;
    let manifest = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert!(manifest.state.envelope.payload().head_seq < restored.committed_seq);
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
    let view = load_current_metadata_view(&store, &namespace_id)
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
    let report = gc_namespace(
        &aged,
        None,
        &namespace_id,
        &options,
        &MutationContext {
            now_ms: options.grace_window_ms + 1,
            ..context
        },
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
    fold_wal(&store, &namespace_id).await.expect("fold restore");
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
        let namespace_id = engine.namespace_id.clone();
        let original = inline(&namespace_id, Bytes::from_static(b"first"));
        publish(
            &mut engine,
            &store,
            &context,
            candidate("file", vec![original.clone()]),
        )
        .await
        .expect("put");
        publish(
            &mut engine,
            &store,
            &context,
            CommitCandidate::new(CommitRequest::single(
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
            loonfs_test_support::stores::OperationClass::Put,
        );
        let view = load_current_metadata_view(&blocked, &namespace_id)
            .await
            .expect("view");
        let access = ReadAccess::live(Authorizer::Unrestricted);
        for revision in revisions {
            let key = content_span(&namespace_id, &original.content_ref().content_id, 0, 10);
            let pool = Semaphore::new(1);
            let absent = store.head(&key).await.expect("head").is_none();
            if absent {
                blocked.block_next();
            }
            let mut download = std::pin::pin!(view.direct_download_target(
                &blocked,
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
                assert_eq!(pool.available_permits(), 0);
                blocked.release();
            }
            let target = download.await.expect("download revision");
            assert_eq!(pool.available_permits(), 1);
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
        fold_wal(&store, &namespace_id)
            .await
            .expect("fold after downloads");
        assert_eq!(store.head(&key).await.expect("head").expect("span"), before);
    }
}

#[tokio::test]
async fn two_chains_in_one_fold_share_one_write_permit() {
    use loonfs_test_support::stores::{BlockingStore, ConcurrencyWatchStore, OperationClass};
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let values = vec![
        inline(&namespace_id, Bytes::from_static(b"first")),
        inline(&namespace_id, Bytes::from_static(b"second")),
    ];
    publish(&mut engine, &store, &context, candidate("files", values))
        .await
        .expect("publish");
    let blocked = ConcurrencyWatchStore::new(
        BlockingStore::new(
            store.clone(),
            KeyPredicate::content_blob(),
            OperationClass::Put,
        ),
        KeyPredicate::content_blob(),
    );
    blocked.inner().block_next();
    store.reset();
    let pool = Semaphore::new(1);
    let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
    let mut fold = std::pin::pin!(fold_wal_tail(
        &blocked,
        None,
        &namespace_id,
        None,
        &deadline,
        &pool,
    ));
    tokio::select! {
        () = blocked.inner().wait_until_blocked() => {}
        _ = &mut fold => panic!("fold finished before the content write"),
    }
    assert!(futures::poll!(&mut fold).is_pending());
    assert_eq!(pool.available_permits(), 0);
    assert_eq!(blocked.puts().total, 1);
    blocked.inner().release();
    fold.await.expect("both chains finish");
    assert_eq!(blocked.puts().peak_in_flight, 1);
    assert_eq!(blocked.puts().total, 2);
    assert_eq!(pool.available_permits(), 1);
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
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    engine.invalidate_projection();
    let target = inline(&namespace_id, Bytes::from_static(b"target"));
    publish(
        &mut engine,
        &store,
        &context,
        candidate("target", vec![target]),
    )
    .await
    .expect("target");
    let stale_layout = layout(&store, &original).await.layout;
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let candidate = CommitCandidate::new(CommitRequest::single(
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
    let session = PublishPlanningSession::new(view.head(), view.wal_tail());
    let plan = session
        .prepare_commit(
            &store,
            &tokio::sync::Semaphore::new(32),
            &candidate,
            candidate
                .semantic_identity(&namespace_id)
                .expect("identity"),
            view.projected_metadata_view(),
            context.now_ms,
            &mut session.begin_candidate(),
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
    fold_wal(&store, &namespace_id).await.expect("fold append");
    let longer = layout(&store, &extended).await;
    crate::test_support::ops::append_wal_commit(
        &store,
        &namespace_id,
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
        let view = load_current_metadata_view(&store, &namespace_id)
            .await
            .expect("view");
        let read = view
            .get_file_revision_bytes(
                &store,
                "/target-0",
                RevisionNo(2),
                None,
                &ReadAccess::live(Authorizer::Unrestricted),
            )
            .await
            .expect("copy bytes");
        assert_eq!(read.bytes, b"hello");
        fold_wal(&store, &namespace_id).await.expect("fold copy");
    }
}

#[tokio::test]
async fn downloads_report_unwritable_pieces_without_changing_content() {
    use loonfs_test_support::stores::{FailStore, InjectedError, OperationKind};

    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    publish(
        &mut engine,
        &store,
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
    let view = load_current_metadata_view(&denied, &namespace_id)
        .await
        .expect("view");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    let content_writes = Semaphore::new(32);
    let store = denied.inner();
    store.reset();
    for path in ["/file-0", "/file-1"] {
        assert!(matches!(
            view.direct_download_target(&denied, path, None, 10, &access, &content_writes)
                .await,
            Err(CoreError::ResumeOffsetOutOfRange { .. })
        ));
    }
    assert_eq!(denied.attempts(), 0);
    for path in ["/file-0", "/file-1"] {
        assert!(matches!(
            view.direct_download_target(&denied, path, None, 0, &access, &content_writes)
                .await,
            Err(CoreError::ContentNotMaterialized { .. })
        ));
    }
    assert_eq!(denied.attempts(), 2);
    assert_no_writes(store);
}

#[tokio::test]
async fn references_to_pending_pieces_survive_layout_pruning_and_collection() {
    use crate::gc::{gc_namespace, GcOptions};
    use crate::manifest::{fold_wal_tail, metadata_basis_from_manifest};
    use crate::namespace::control::load_current_manifest;
    use crate::path::write::PublishPlanningSession;
    use loonfs_types::format::manifest::MetadataRowFamily;

    for restore in [true, false] {
        let (_directory, store, mut engine, context) = setup().await;
        let namespace_id = engine.namespace_id.clone();
        publish(
            &mut engine,
            &store,
            &context,
            candidate(
                "target",
                vec![InlineContent::new(
                    namespace_id.clone(),
                    ContentId::parse("con_00000000000000000000000000000001").expect("content"),
                    Bytes::from_static(b"target"),
                    loonfs_types::ChecksumAlgorithm::Crc64nvme,
                )],
            ),
        )
        .await
        .expect("target");
        fold_wal(&store, &namespace_id).await.expect("fold target");
        engine.invalidate_projection();
        let original = InlineContent::new(
            namespace_id.clone(),
            ContentId::parse("con_00000000000000000000000000000004").expect("content"),
            Bytes::from_static(b"hello"),
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        publish(
            &mut engine,
            &store,
            &context,
            candidate("file", vec![original]),
        )
        .await
        .expect("file");
        publish(
            &mut engine,
            &store,
            &context,
            CommitCandidate::new(CommitRequest::single(
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
        let view = load_current_metadata_view(&store, &namespace_id)
            .await
            .expect("view");
        let candidate = CommitCandidate::new(CommitRequest::single(
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
        let session = PublishPlanningSession::new(view.head(), view.wal_tail());
        let content_writes = tokio::sync::Semaphore::new(32);
        let mut plan = session
            .prepare_commit(
                &store,
                &content_writes,
                &candidate,
                candidate
                    .semantic_identity(&namespace_id)
                    .expect("identity"),
                view.projected_metadata_view(),
                context.now_ms,
                &mut session.begin_candidate(),
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
            ContentId::parse("con_00000000000000000000000000000003").expect("content"),
            Bytes::from_static(b"replacement"),
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let replacement = CommitCandidate::with_inline_content(
            CommitRequest::single(
                CommitId::parse("replace").expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::PutFile {
                    path: AbsolutePath::parse("/file-0").expect("path"),
                    content_ref: Some(replacement.content_ref().clone()),
                    inline_content: None,
                    behavior: loonfs_types::DestinationBehavior::Replace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            ),
            Vec::new(),
            vec![replacement],
        );
        publish(&mut engine, &store, &context, replacement)
            .await
            .expect("replace source");
        let fold = engine.wal_fold_input().expect("fold input");
        if restore {
            for delta in &mut plan.deltas {
                if let WalDelta::AppendFileRevision { revision_no, .. } = &mut delta.delta {
                    *revision_no = RevisionNo(4);
                }
            }
        }
        crate::test_support::ops::append_wal_commit(
            &store,
            &namespace_id,
            plan.deltas.into_iter().map(|delta| delta.delta).collect(),
            Vec::new(),
        )
        .await
        .expect("commit reference");
        fold_wal_tail(
            &store,
            None,
            &namespace_id,
            Some(fold),
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
            &content_writes,
        )
        .await
        .expect("fold captured source");
        prune_revisions(&store, &namespace_id).await;
        let manifest = load_current_manifest(&store, &namespace_id)
            .await
            .expect("manifest");
        let basis = metadata_basis_from_manifest(&store, None, &manifest);
        let rows = basis
            .segments
            .scan_range_page_with_keys(
                MetadataRowFamily::ContentLayouts,
                &format!("content-layout-{}-", content_ref.content_id),
                Some(&format!("content-layout-{}.", content_ref.content_id)),
                32,
            )
            .await
            .expect("layouts");
        assert!(rows.is_empty(), "the basis no longer names this chain");
        let options = GcOptions::default();
        let aged = MetadataMapStore::aged(store.clone(), KeyPredicate::any());
        gc_namespace(
            &aged,
            None,
            &namespace_id,
            &options,
            &MutationContext {
                now_ms: options.grace_window_ms + 1,
                ..context
            },
        )
        .await
        .expect("collect");
        for extent in &carried.extents {
            let key = crate::storage::content_location::extent_object_key(extent);
            assert!(store.head(&key).await.expect("head").is_some(), "{key}");
        }
        for folded in [false, true] {
            if folded {
                fold_wal(&store, &namespace_id)
                    .await
                    .expect("fold reference");
            }
            let view = load_current_metadata_view(&store, &namespace_id)
                .await
                .expect("view");
            let bytes = view
                .get_file_revision_bytes(
                    &store,
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
