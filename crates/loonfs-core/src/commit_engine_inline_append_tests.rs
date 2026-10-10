//! Appends: which content object takes the bytes, guards, rejections, and
//! retries.

#![allow(clippy::panic)]

use super::*;
use crate::error::ErrorCode;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::PutMode;
use loonfs_types::{Checksum, ContentRefKind};

fn append(path: &str, bytes: &[u8]) -> FilesystemOperation {
    FilesystemOperation::AppendFile {
        path: AbsolutePath::parse(path).expect("path"),
        inline_content: bytes.to_vec(),
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

fn request(commit_id: &str, operations: Vec<FilesystemOperation>) -> CommitCandidate {
    CommitCandidate::new(CommitRequest {
        commit_id: CommitId::parse(commit_id).expect("commit"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations,
    })
}

/// The pieces the newest WAL object carries.
async fn newest_pieces(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
) -> Vec<WalInlineContent> {
    let key = store
        .list_prefix(&wal_prefix(namespace_id))
        .await
        .expect("list WAL")
        .pop()
        .expect("a WAL object");
    let bytes = store.get(&key, None).await.expect("get").expect("WAL");
    let wal = decode_wal_object_envelope_zstd(&bytes).expect("decode WAL");
    wal.payload()
        .records
        .iter()
        .flat_map(|record| record.inline_content.clone())
        .collect()
}

async fn revision_bytes(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
    path: &str,
    revision_no: u64,
) -> Vec<u8> {
    load_current_metadata_view(store, namespace_id)
        .await
        .expect("view")
        .get_file_revision_bytes(
            store,
            path,
            RevisionNo(revision_no),
            None,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("read")
        .bytes
}

async fn current_ref(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
    path: &str,
) -> ContentRef {
    let entry = load_current_metadata_view(store, namespace_id)
        .await
        .expect("view")
        .resolve_path(
            path,
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("entry");
    match entry.kind {
        PathEntryKind::File { content_ref, .. } => content_ref,
        other => panic!("expected a file, got {other:?}"),
    }
}

#[tokio::test]
async fn an_append_continues_its_chain_and_preserves_earlier_revisions() {
    let (_directory, store, mut engine, context) = setup().await;
    let (_, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    engine.invalidate_projection();
    publish(
        &mut engine,
        &store,
        &context,
        request("append", vec![append("/file-0", b" world")]),
    )
    .await
    .expect("append");

    let namespace_id = engine.namespace_id.clone();
    assert_eq!(
        newest_pieces(&store, &namespace_id).await,
        vec![WalInlineContent {
            content_id: original.content_id.clone(),
            offset: 5,
            bytes: b" world".to_vec(),
        }]
    );
    assert_eq!(
        current_ref(&store, &namespace_id, "/file-0").await,
        ContentRef::blob_v1(
            namespace_id.clone(),
            original.content_id.clone(),
            b"hello world"
        )
    );
    for _ in ["before the fold", "after the fold"] {
        assert_eq!(
            read_file(&store, &namespace_id, RevisionNo(2)).await,
            b"hello world"
        );
        assert_eq!(
            read_file(&store, &namespace_id, RevisionNo(1)).await,
            b"hello"
        );
        fold_wal(&store, &namespace_id).await.expect("fold");
    }
    assert_eq!(
        store
            .get(&content_blob(&namespace_id, &original.content_id), None)
            .await
            .expect("get")
            .expect("object"),
        b"hello".as_slice()
    );
}

#[tokio::test]
async fn appends_in_one_commit_chain_in_operation_order_after_a_put() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let value = inline(&namespace_id, Bytes::from_static(b"ab"));
    let mut chain = request(
        "chain",
        vec![
            put("/log", value.content_ref()),
            append("/log", b"cd"),
            append("/log", b"ef"),
            append("/log", b"g"),
        ],
    );
    chain.inline_content.push(value.clone());
    publish(&mut engine, &store, &context, chain)
        .await
        .expect("chain");

    let content_id = &value.content_ref().content_id;
    let pieces = newest_pieces(&store, &namespace_id).await;
    assert_eq!(
        pieces
            .iter()
            .map(|piece| (&piece.content_id, piece.offset, piece.bytes.as_slice()))
            .collect::<Vec<_>>(),
        [(0, "ab"), (2, "cd"), (4, "ef"), (6, "g")].map(|(offset, bytes)| (
            content_id,
            offset,
            bytes.as_bytes()
        ))
    );
    for (revision_no, expected) in [(1, "ab"), (2, "abcd"), (3, "abcdef"), (4, "abcdefg")] {
        assert_eq!(
            revision_bytes(&store, &namespace_id, "/log", revision_no).await,
            expected.as_bytes()
        );
    }
}

#[tokio::test]
async fn an_append_to_a_restored_revision_starts_a_new_chain_from_it() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (_, original) = folded_file(&store, &mut engine, &context, b"one").await;
    engine.invalidate_projection();
    for (commit_id, operation) in [
        ("two", append("/file-0", b"two")),
        (
            "restore",
            FilesystemOperation::RestoreRevision {
                path: AbsolutePath::parse("/file-0").expect("path"),
                source_revision_no: RevisionNo(1),
            },
        ),
        ("three", append("/file-0", b"three")),
    ] {
        publish(
            &mut engine,
            &store,
            &context,
            request(commit_id, vec![operation]),
        )
        .await
        .expect(commit_id);
    }

    let pieces = newest_pieces(&store, &namespace_id).await;
    let [piece] = pieces.as_slice() else {
        panic!("expected one piece, got {pieces:?}");
    };
    assert_ne!(piece.content_id, original.content_id);
    assert_eq!(piece.offset, 3);
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let carried = view
        .projected_metadata_view()
        .content_layout(&piece.content_id)
        .await
        .expect("layout")
        .expect("carried layout");
    assert_eq!(carried.size_bytes, 3);
    assert_eq!(carried.layout.extents[0].content_id, original.content_id);
    for _ in ["before the fold", "after the fold"] {
        assert_eq!(
            read_file(&store, &namespace_id, RevisionNo(4)).await,
            b"onethree"
        );
        assert_eq!(
            read_file(&store, &namespace_id, RevisionNo(2)).await,
            b"onetwo"
        );
        fold_wal(&store, &namespace_id).await.expect("fold");
    }
}

#[tokio::test]
async fn append_guards_name_the_inode_and_revision_they_require() {
    let (_directory, store, mut engine, context) = setup().await;
    let (inode_id, _) = folded_file(&store, &mut engine, &context, b"log").await;
    engine.invalidate_projection();
    let guarded = |expected_inode_id, expected_revision_no| FilesystemOperation::AppendFile {
        path: AbsolutePath::parse("/file-0").expect("path"),
        inline_content: b"!".to_vec(),
        expected_inode_id,
        expected_revision_no,
    };
    let by_inode = |expected_revision_no| FilesystemOperation::AppendFileByInode {
        inode_id,
        inline_content: b"?".to_vec(),
        expected_revision_no,
    };
    for (name, operation, code) in [
        (
            "stale",
            guarded(Some(inode_id), Some(RevisionNo(2))),
            ErrorCode::StaleRevision,
        ),
        (
            "other-inode",
            guarded(Some(InodeId(99)), None),
            ErrorCode::PathConflict,
        ),
        (
            "revision-alone",
            guarded(None, Some(RevisionNo(1))),
            ErrorCode::InvalidRequest,
        ),
        (
            "stale-by-inode",
            by_inode(Some(RevisionNo(2))),
            ErrorCode::StaleRevision,
        ),
    ] {
        store.reset();
        let error = publish(
            &mut engine,
            &store,
            &context,
            request(name, vec![operation]),
        )
        .await
        .expect_err(name);
        assert_eq!(error.code(), code, "{name}: {error}");
        assert_no_writes(&store);
    }
    for (name, operation) in [
        ("current", guarded(Some(inode_id), Some(RevisionNo(1)))),
        ("current-by-inode", by_inode(Some(RevisionNo(2)))),
    ] {
        publish(
            &mut engine,
            &store,
            &context,
            request(name, vec![operation]),
        )
        .await
        .expect(name);
    }
    assert_eq!(
        read_file(&store, &engine.namespace_id, RevisionNo(3)).await,
        b"log!?"
    );
}

#[tokio::test]
async fn appends_name_an_existing_file() {
    let (_directory, store, mut engine, context) = setup().await;
    folded_file(&store, &mut engine, &context, b"file").await;
    engine.invalidate_projection();
    publish(
        &mut engine,
        &store,
        &context,
        request(
            "directory",
            vec![FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/directory").expect("path"),
                parents: false,
            }],
        ),
    )
    .await
    .expect("directory");
    let by_inode = |inode_id| FilesystemOperation::AppendFileByInode {
        inode_id,
        inline_content: b"bytes".to_vec(),
        expected_revision_no: None,
    };
    for (name, operation, code) in [
        (
            "to-directory",
            append("/directory", b"bytes"),
            ErrorCode::PathConflict,
        ),
        (
            "missing",
            append("/missing", b"bytes"),
            ErrorCode::PathNotFound,
        ),
        (
            "root",
            by_inode(loonfs_types::ROOT_INODE_ID),
            ErrorCode::PathConflict,
        ),
        (
            "missing-inode",
            by_inode(InodeId(99)),
            ErrorCode::InodeNotFound,
        ),
    ] {
        store.reset();
        let error = publish(
            &mut engine,
            &store,
            &context,
            request(name, vec![operation]),
        )
        .await
        .expect_err(name);
        assert_eq!(error.code(), code, "{name}: {error}");
        assert_no_writes(&store);
    }
}

#[tokio::test]
async fn empty_and_oversized_appends_are_refused_before_planning() {
    let (_directory, store, mut engine, context) = setup().await;
    let empty = request("empty", vec![append("/missing", b"")]);
    let oversized = request(
        "oversized",
        vec![
            append("/missing", b"x"),
            append("/missing", &vec![0; MAX_WAL_INLINE_CONTENT_BYTES + 1]),
        ],
    );
    store.reset();
    match publish(&mut engine, &store, &context, empty).await {
        Err(CoreError::FailedOperation {
            operation_index: 0,
            source,
        }) if matches!(
            *source,
            CoreError::InvalidCommitField {
                field: "inline_content",
                ..
            }
        ) => {}
        other => panic!("expected an empty append to be refused, got {other:?}"),
    }
    match publish(&mut engine, &store, &context, oversized).await {
        Err(
            error @ CoreError::FailedOperation {
                operation_index: 1, ..
            },
        ) => assert_eq!(error.code(), ErrorCode::ContentTooLarge),
        other => panic!("expected an oversized append to be refused, got {other:?}"),
    }
    assert_no_writes(&store);
}

#[tokio::test]
async fn an_append_retry_replays_and_changed_bytes_conflict() {
    let (_directory, store, mut engine, context) = setup().await;
    folded_file(&store, &mut engine, &context, b"log").await;
    engine.invalidate_projection();
    let committed = publish(
        &mut engine,
        &store,
        &context,
        request("retry", vec![append("/file-0", b"+")]),
    )
    .await
    .expect("append");
    store.reset();
    assert_eq!(
        publish(
            &mut engine,
            &store,
            &context,
            request("retry", vec![append("/file-0", b"+")]),
        )
        .await
        .expect("replay"),
        committed
    );
    assert!(matches!(
        publish(
            &mut engine,
            &store,
            &context,
            request("retry", vec![append("/file-0", b"-")]),
        )
        .await,
        Err(CoreError::CommitIdReuseConflict { .. })
    ));
    assert_no_writes(&store);
    assert_eq!(
        read_file(&store, &engine.namespace_id, RevisionNo(2)).await,
        b"log+"
    );
}

/// A direct upload records only what its provider reports: a CRC-64/NVME
/// on S3, a CRC-32C on GCS.
#[tokio::test]
async fn appends_continue_the_digest_their_base_recorded() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, _) = folded_file(&store, &mut engine, &context, b"seed").await;
    for (revision_no, checksum) in [
        (2, Checksum::crc64nvme(b"direct")),
        (4, Checksum::crc32c(b"direct")),
    ] {
        let content_id = ContentId::generate();
        store
            .put(
                &content_blob(&namespace_id, &content_id),
                Bytes::from_static(b"direct"),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("direct upload");
        crate::test_support::ops::append_wal_commit(
            &store,
            &namespace_id,
            vec![WalDelta::AppendFileRevision {
                delta_index: 0,
                inode_id,
                revision_no: RevisionNo(revision_no),
                content_ref: ContentRef {
                    kind: ContentRefKind::BlobV1,
                    owner_namespace_id: namespace_id.clone(),
                    content_id: content_id.clone(),
                    size_bytes: 6,
                    checksum: checksum.clone(),
                },
                hash_state: None,
                crc64nvme: (checksum.algorithm == loonfs_types::ChecksumAlgorithm::Crc64nvme)
                    .then(|| checksum.clone()),
                layout: Some(loonfs_types::ContentLayout {
                    extents: vec![loonfs_types::ContentExtent {
                        owner_namespace_id: namespace_id.clone(),
                        content_id,
                        object: loonfs_types::ExtentObject::Whole,
                        offset: 0,
                        length: 6,
                    }],
                }),
            }],
            Vec::new(),
        )
        .await
        .expect("commit the upload");
        engine.invalidate_projection();
        let appended = publish(
            &mut engine,
            &store,
            &context,
            request(
                &format!("append-{revision_no}"),
                vec![append("/file-0", b"+tail")],
            ),
        )
        .await;
        if revision_no == 2 {
            appended.expect("append to a CRC-64/NVME");
            assert_eq!(
                current_ref(&store, &namespace_id, "/file-0").await.checksum,
                Checksum::crc64nvme(b"direct+tail")
            );
            for _ in ["before the fold", "after the fold"] {
                assert_eq!(
                    read_file(&store, &namespace_id, RevisionNo(3)).await,
                    b"direct+tail"
                );
                fold_wal(&store, &namespace_id).await.expect("fold");
            }
        } else {
            let error = appended.expect_err("nothing to continue");
            assert_eq!(error.code(), ErrorCode::NotSupported);
            assert!(error.to_string().contains("`/file-0`"), "{error}");
        }
    }
}

#[tokio::test]
async fn a_copy_appends_from_its_revision_digests_under_a_fresh_chain_id() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (_, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    engine.invalidate_projection();
    publish(
        &mut engine,
        &store,
        &context,
        request(
            "copy",
            vec![FilesystemOperation::CopyPath {
                source_path: AbsolutePath::parse("/file-0").expect("path"),
                destination_path: AbsolutePath::parse("/copy").expect("path"),
                precondition: Default::default(),
            }],
        ),
    )
    .await
    .expect("copy");
    publish(
        &mut engine,
        &store,
        &context,
        request("original", vec![append("/file-0", b"!")]),
    )
    .await
    .expect("append original");
    fold_wal(&store, &namespace_id).await.expect("fold");
    engine.invalidate_projection();
    store.reset();
    publish(
        &mut engine,
        &store,
        &context,
        request("copied", vec![append("/copy", b"?")]),
    )
    .await
    .expect("append copy");
    assert!(store
        .snapshot()
        .iter()
        .all(|operation| loonfs_objectstore::layout::content_id_of(operation.key()).is_none()));
    let reference = current_ref(&store, &namespace_id, "/copy").await;
    assert_ne!(reference.content_id, original.content_id);
    assert_eq!(reference.checksum, Checksum::sha256(b"hello?"));
}

fn copy(source: &str, destination: &str) -> FilesystemOperation {
    FilesystemOperation::CopyPath {
        source_path: AbsolutePath::parse(source).expect("source"),
        destination_path: AbsolutePath::parse(destination).expect("destination"),
        precondition: Default::default(),
    }
}

#[tokio::test]
async fn a_fresh_chain_shares_folded_extents_and_copies_resident_bytes() {
    for folded in [false, true] {
        for scope in ["commit", "batch", "tail"] {
            let (_directory, store, mut engine, context) = setup().await;
            let namespace_id = engine.namespace_id.clone();
            let prefix = vec![b'a'; MAX_WAL_INLINE_CONTENT_BYTES / 2];
            let resident = vec![b'b'; MAX_WAL_INLINE_CONTENT_BYTES / 2 + 10];
            let suffix = vec![b'?'; MAX_WAL_INLINE_CONTENT_BYTES];
            let value = inline(&namespace_id, Bytes::copy_from_slice(&prefix));
            if folded {
                publish(
                    &mut engine,
                    &store,
                    &context,
                    candidate("file", vec![value.clone()]),
                )
                .await
                .expect("file");
                fold_wal(&store, &namespace_id).await.expect("fold prefix");
                engine.invalidate_projection();
            }
            let mut operations = Vec::new();
            if !folded {
                operations.push(put("/file-0", value.content_ref()));
            }
            operations.extend([
                append("/file-0", &resident),
                copy("/file-0", "/copy"),
                append("/copy", b"!"),
                append("/file-0", &suffix),
            ]);
            store.reset();
            let mut candidates = if scope == "commit" {
                vec![request("branches", operations)]
            } else {
                operations
                    .into_iter()
                    .enumerate()
                    .map(|(index, operation)| request(&format!("branch-{index}"), vec![operation]))
                    .collect::<Vec<_>>()
            };
            if !folded {
                candidates[0].inline_content.push(value.clone());
            }
            if scope == "tail" {
                for candidate in candidates {
                    publish(&mut engine, &store, &context, candidate)
                        .await
                        .expect("branch");
                }
            } else {
                let results = engine
                    .publish_batch(
                        &store,
                        candidates,
                        &context,
                        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
                    )
                    .await
                    .results;
                for result in results {
                    result.expect("branch");
                }
            }
            assert!(store.snapshot().iter().all(|operation| {
                loonfs_objectstore::layout::content_id_of(operation.key()).is_none()
            }));
            let fresh = current_ref(&store, &namespace_id, "/file-0").await;
            let copied = current_ref(&store, &namespace_id, "/copy").await;
            assert_ne!(fresh.content_id, value.content_ref().content_id);
            assert_eq!(copied.content_id, value.content_ref().content_id);
            let offset = if folded { prefix.len() as u64 } else { 0 };
            let expected_pieces = if folded {
                [resident.as_slice(), suffix.as_slice()].concat()
            } else {
                [prefix.as_slice(), resident.as_slice(), suffix.as_slice()].concat()
            };
            let pieces = newest_pieces(&store, &namespace_id)
                .await
                .into_iter()
                .filter(|piece| piece.content_id == fresh.content_id)
                .collect::<Vec<_>>();
            assert_eq!(
                pieces.len(),
                expected_pieces.len().div_ceil(MAX_WAL_INLINE_CONTENT_BYTES)
            );
            for (index, piece) in pieces.iter().enumerate() {
                assert_eq!(
                    piece.offset,
                    offset + (index * MAX_WAL_INLINE_CONTENT_BYTES) as u64
                );
                assert!(piece.bytes.len() <= MAX_WAL_INLINE_CONTENT_BYTES);
            }
            assert_eq!(
                pieces
                    .iter()
                    .flat_map(|piece| piece.bytes.iter().copied())
                    .collect::<Vec<_>>(),
                expected_pieces
            );
            let expected_prefix = loonfs_types::ContentExtent {
                owner_namespace_id: namespace_id.clone(),
                content_id: value.content_ref().content_id.clone(),
                object: loonfs_types::ExtentObject::Whole,
                offset: 0,
                length: prefix.len() as u64,
            };
            let view = load_current_metadata_view(&store, &namespace_id)
                .await
                .expect("view");
            let carried = view
                .projected_metadata_view()
                .content_layout(&fresh.content_id)
                .await
                .expect("layout");
            if folded {
                assert_eq!(
                    carried.expect("prefix").layout.extents,
                    std::slice::from_ref(&expected_prefix)
                );
            } else {
                assert!(carried.is_none());
            }
            let original_bytes =
                [prefix.as_slice(), resident.as_slice(), suffix.as_slice()].concat();
            let copied_bytes = [prefix.as_slice(), resident.as_slice(), b"!"].concat();
            for after_fold in [false, true] {
                if after_fold {
                    fold_wal(&store, &namespace_id)
                        .await
                        .expect("fold both chains");
                }
                assert_eq!(
                    revision_bytes(&store, &namespace_id, "/file-0", 3).await,
                    original_bytes
                );
                assert_eq!(
                    revision_bytes(&store, &namespace_id, "/copy", 2).await,
                    copied_bytes
                );
            }
            let view = load_current_metadata_view(&store, &namespace_id)
                .await
                .expect("folded view");
            let row = view
                .projected_metadata_view()
                .content_layout(&fresh.content_id)
                .await
                .expect("layout")
                .expect("folded layout");
            if folded {
                assert_eq!(row.layout.extents[0], expected_prefix);
            }
            let extent = row.layout.extents.last().expect("new extent");
            assert_eq!(extent.content_id, fresh.content_id);
            let key = crate::storage::content_location::extent_object_key(extent);
            assert_eq!(
                store.get(&key, None).await.expect("get").expect("object"),
                expected_pieces
            );
        }
    }
}

#[tokio::test]
async fn copied_pieces_count_toward_the_commit_and_batch_wal_limit() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let bytes = vec![b'x'; MAX_WAL_INLINE_CONTENT_BYTES];
    let value = inline(&namespace_id, Bytes::copy_from_slice(&bytes));
    let mut base = candidate("base", vec![value]);
    base.request
        .operations
        .extend((0..7).map(|_| append("/base-0", &bytes)));
    publish(&mut engine, &store, &context, base)
        .await
        .expect("base");
    publish(
        &mut engine,
        &store,
        &context,
        request(
            "copies",
            vec![
                copy("/base-0", "/a"),
                copy("/base-0", "/b"),
                append("/base-0", b"!"),
            ],
        ),
    )
    .await
    .expect("copies");
    let original = current_ref(&store, &namespace_id, "/a").await;
    store.reset();
    let error = publish(
        &mut engine,
        &store,
        &context,
        request("too-large", vec![append("/a", b"a"), append("/b", b"b")]),
    )
    .await
    .expect_err("copied bytes exceed the WAL limit");
    assert!(matches!(error, CoreError::FailedOperation { source, .. }
        if matches!(*source, CoreError::CommitTooLarge { estimated_bytes, max_bytes }
            if estimated_bytes == MAX_WAL_OBJECT_INLINE_CONTENT_BYTES + 2
                && max_bytes == MAX_WAL_OBJECT_INLINE_CONTENT_BYTES)));
    assert_no_writes(&store);
    let result = engine
        .publish_batch(
            &store,
            [
                request("fits", vec![append("/a", b"a")]),
                request("does-not-fit", vec![append("/b", b"b")]),
            ],
            &context,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await;
    assert!(result.results[0].is_ok());
    assert!(matches!(&result.results[1], Err(CoreError::CommitTooLarge {
        estimated_bytes, max_bytes,
    }) if *estimated_bytes == MAX_WAL_OBJECT_INLINE_CONTENT_BYTES + 2
        && *max_bytes == MAX_WAL_OBJECT_INLINE_CONTENT_BYTES));
    assert_eq!(current_ref(&store, &namespace_id, "/b").await, original);
    assert_eq!(
        revision_bytes(&store, &namespace_id, "/a", 2).await,
        [vec![b'x'; 8 * bytes.len()], vec![b'a']].concat()
    );
    assert_eq!(result.wal_tail_inline_bytes, 16 * bytes.len() + 2);
}

#[tokio::test]
async fn a_fresh_chain_with_missing_resident_base_bytes_writes_nothing() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let value = inline(&namespace_id, Bytes::from_static(b"abc"));
    publish(
        &mut engine,
        &store,
        &context,
        candidate("file", vec![value.clone()]),
    )
    .await
    .expect("file");
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    let inode_id = view
        .resolve_path(
            "/file-0",
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("file")
        .inode_id;
    for (revision_no, whole, offset) in [
        (2, b"abcdefghi".as_slice(), 6),
        (3, b"abcdefghij".as_slice(), 9),
    ] {
        commit_piece(
            &store,
            &namespace_id,
            (inode_id, RevisionNo(revision_no)),
            &value.content_ref().content_id,
            whole,
            offset,
            None,
        )
        .await;
    }
    engine.invalidate_projection();
    publish(
        &mut engine,
        &store,
        &context,
        request(
            "restore",
            vec![FilesystemOperation::RestoreRevision {
                path: AbsolutePath::parse("/file-0").expect("path"),
                source_revision_no: RevisionNo(2),
            }],
        ),
    )
    .await
    .expect("restore");
    store.reset();
    let error = publish(
        &mut engine,
        &store,
        &context,
        request("missing", vec![append("/file-0", b"!")]),
    )
    .await
    .expect_err("missing base bytes");
    assert!(matches!(error, CoreError::FailedOperation { source, .. }
        if matches!(*source, CoreError::NamespaceCorrupt(ref message)
            if message.contains(value.content_ref().content_id.as_str()) && message.contains("offset 3"))));
    assert_no_writes(&store);
}
