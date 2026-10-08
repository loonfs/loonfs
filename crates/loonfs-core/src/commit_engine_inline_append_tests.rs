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
    wal.payload().records[0].inline_content.clone()
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
async fn an_append_extends_its_object_and_earlier_revisions_keep_their_prefix() {
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
            base: None,
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
        b"hello world".as_slice()
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
            .map(|piece| (
                &piece.content_id,
                piece.offset,
                piece.bytes.as_slice(),
                &piece.base
            ))
            .collect::<Vec<_>>(),
        [(0, "ab"), (2, "cd"), (4, "ef"), (6, "g")].map(|(offset, bytes)| (
            content_id,
            offset,
            bytes.as_bytes(),
            &None
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
    assert_eq!(
        (piece.offset, &piece.base),
        (
            3,
            &Some(ContentBase {
                owner_namespace_id: namespace_id.clone(),
                content_id: original.content_id.clone(),
            })
        )
    );
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
                    content_id,
                    size_bytes: 6,
                    checksum: checksum.clone(),
                },
                hash_state: None,
                crc64nvme: (checksum.algorithm == loonfs_types::ChecksumAlgorithm::Crc64nvme)
                    .then(|| checksum.clone()),
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
