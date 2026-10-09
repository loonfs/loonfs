//! Immutable extent writes, reads, downloads, and collection.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::storage::content::DurableContentValidationError;
use crate::storage::tail_content::{assemble_tail_content, write_tail_content};
use loonfs_objectstore::keys::{content_blob, content_span};
use loonfs_test_support::stores::MetadataMapStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::{Checksum, ExtentObject};

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
async fn upload_evidence_publishes_a_layout_only_for_a_new_chain() {
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
    assert_eq!(rows.len(), 1);
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
        assert!(operations.iter().all(|operation| !matches!(
            operation,
            RecordedOperation::Extend { .. } | RecordedOperation::PutImmutableExtended { .. }
        )));
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
async fn a_merged_span_must_match_its_objects_attestation() {
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
            metadata.sha256 = Some(Checksum::sha256(b"bad"));
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
        .expect_err("attestation mismatch");
    assert!(matches!(error, CoreError::NamespaceCorrupt(message) if message.contains(&key)));
    assert_no_writes(&store);
}

#[tokio::test]
async fn a_retry_accepts_an_attested_span_without_changing_it() {
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
    )
    .await
    .expect("whole before manifest");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    assert_eq!(
        view.direct_download_target(&store, "/file-0", None, 0, &access)
            .await
            .expect("equal whole download")
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
    for revision in [RevisionNo(1), RevisionNo(2)] {
        assert!(matches!(
            view.direct_download_target(&store, "/file-0", Some(revision), 0, &access)
                .await,
            Err(CoreError::ContentNotMaterialized { .. })
        ));
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
    bytes.resize(110, b'b');
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
    fold_wal(&store, &namespace_id)
        .await
        .expect("fold base span");
    bytes.truncate(105);
    bytes.push(b'!');
    let fresh = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(3)),
        &ContentId::generate(),
        &bytes,
        105,
        Some(ContentBase {
            owner_namespace_id: namespace_id.clone(),
            content_id: original.content_id.clone(),
        }),
    )
    .await;
    store.reset();
    fold_wal(&store, &namespace_id)
        .await
        .expect("fold shared base");
    let span = content_span(&namespace_id, &fresh.content_id, 105, 106);
    assert!(
        matches!(content_operations(&store).as_slice(), [RecordedOperation::Put { key, bytes: 1, .. }] if key == &span)
    );
    let row = layout(&store, &fresh).await;
    row.layout.validate(106).expect("shared prefix layout");
    let base = layout(&store, &original).await;
    assert_eq!(row.layout.extents[0], base.layout.extents[0]);
    let mut prefix = base.layout.extents[1].clone();
    prefix.length = 5;
    assert_eq!(row.layout.extents[1], prefix);
    assert_eq!(read_file(&store, &namespace_id, RevisionNo(3)).await, bytes);
}

#[tokio::test]
async fn downloads_require_one_object_and_create_empty_whole_objects() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    let access = ReadAccess::live(Authorizer::Unrestricted);
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let target = view
        .direct_download_target(&store, "/file-0", None, 0, &access)
        .await
        .expect("one object");
    assert_eq!(
        target.object_key,
        content_blob(&namespace_id, &original.content_id)
    );
    commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_id,
        b"hello!",
        5,
        None,
    )
    .await;
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view with pieces");
    store.reset();
    assert!(matches!(
        view.direct_download_target(&store, "/file-0", None, 0, &access)
            .await,
        Err(CoreError::ContentNotMaterialized { .. })
    ));
    assert!(content_operations(&store).is_empty());
    fold_wal(&store, &namespace_id).await.expect("fold");
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    assert!(matches!(
        view.direct_download_target(&store, "/file-0", None, 0, &access)
            .await,
        Err(CoreError::ContentNotMaterialized { .. })
    ));
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
        .direct_download_target(&store, "/empty-0", None, 0, &access)
        .await
        .expect("empty download");
    assert_eq!(
        target.object_key,
        content_blob(&namespace_id, &empty.content_ref().content_id)
    );
    assert_eq!(
        store
            .head(&target.object_key)
            .await
            .expect("head")
            .expect("empty object")
            .size_bytes,
        0
    );
}

#[tokio::test]
async fn collection_keeps_shared_and_rooted_spans_and_deletes_an_unrooted_chain() {
    use crate::gc::{gc_namespace, GcOptions};
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    let fresh = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &ContentId::generate(),
        b"hello!",
        5,
        Some(ContentBase {
            owner_namespace_id: namespace_id.clone(),
            content_id: original.content_id.clone(),
        }),
    )
    .await;
    fold_wal(&store, &namespace_id).await.expect("fold");
    prune_revisions(&store, &namespace_id).await;
    let rooted_span = content_span(&namespace_id, &fresh.content_id, 5, 6);
    let superseded_span = content_span(&namespace_id, &fresh.content_id, 1, 2);
    store
        .put_immutable_verified(&superseded_span, Bytes::from_static(b"x"))
        .await
        .expect("old span");
    let unrooted = ContentId::generate();
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
    let options = GcOptions::default();
    gc_namespace(
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
    let shared_keys = [
        content_blob(&namespace_id, &original.content_id),
        rooted_span,
        superseded_span,
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
        &ContentId::generate(),
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
