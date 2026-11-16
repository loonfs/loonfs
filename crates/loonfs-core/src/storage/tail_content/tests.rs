//! Contracts for provider assembly and extent checksums.

use super::*;
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, FakeMultipartStore, KeyPredicate, MetadataMapStore, OperationSelector,
    RecordedOperation, RecordingStore,
};
use loonfs_types::format::wal::{WalCommitDelta, WalCommitPayload, WalInlineContent};
use loonfs_types::{CommitId, CommitSeq, ContentId, ContentRef, InodeId, NamespaceId, RevisionNo};
use std::assert_matches;

/// The head of the namespace whose tail these tests write, a namespace that
/// was created without a source.
static HEAD: std::sync::LazyLock<NamespaceReadState> = std::sync::LazyLock::new(|| {
    NamespaceReadState::initial(
        NamespaceId::parse("demo").expect("namespace"),
        0,
        loonfs_test_support::test_actor(),
    )
});

struct Layout(ContentLayoutRecord);

/// Returns the chain offsets that `object` stores, or `None` for a whole
/// object.
fn span_bounds(object: &ExtentObject) -> Option<(u64, u64)> {
    match object {
        ExtentObject::Span { start, end, .. } => Some((*start, *end)),
        ExtentObject::Whole => None,
    }
}

#[async_trait::async_trait]
impl LayoutLookup for Layout {
    async fn content_layout(&self, _: &ContentId) -> Result<Option<ContentLayoutRecord>> {
        Ok(Some(self.0.clone()))
    }
}

fn piece_commit(reference: &ContentRef, offset: u64, bytes: Vec<u8>, seq: u64) -> WalCommitPayload {
    WalCommitPayload {
        committed_seq: CommitSeq(seq),
        commit_id: CommitId::parse(format!("c_{seq:032x}")).expect("commit id"),
        committed_by: loonfs_test_support::test_actor(),
        semantic_commit_fingerprint: loonfs_test_support::ids::commit_fingerprint("test"),
        committed_at_ms: 0,
        message: None,
        merge: None,
        deltas: vec![WalCommitDelta {
            semantic_operation_index: 0,
            delta: crate::test_support::file_revision_delta(
                0,
                InodeId(2),
                RevisionNo(seq),
                reference.clone(),
                None,
            ),
        }],
        inline_content: vec![WalInlineContent {
            content_id: reference.content_id.clone(),
            offset,
            bytes,
        }],
    }
}

async fn fixture<S: ObjectStore>(
    store: &S,
    lengths: &[usize],
    shared: bool,
    tail_length: usize,
) -> (Layout, ProjectedWalTail, ContentRef) {
    let content_id = ContentId::generate();
    let mut extents = Vec::new();
    let mut offset = 0;
    let mut crc = Checksum::crc64nvme(&[]);
    for (index, &length) in (0_u8..).zip(lengths) {
        let bytes = Bytes::from(vec![b'a' + index; length]);
        let extent = ContentExtent {
            content_id: if shared && index == 0 {
                ContentId::generate()
            } else {
                content_id.clone()
            },
            object: ExtentObject::Span {
                object_id: SpanObjectId::generate(),
                start: offset,
                end: offset + length as u64,
            },
            object_checksum: Checksum::crc64nvme(&bytes),
            offset: 0,
            length: length as u64,
        };
        store
            .put_immutable_verified(&extent_object_key(&HEAD.pool_id, &extent), bytes.clone())
            .await
            .expect("source");
        crc = crc
            .crc_combine(&Checksum::crc64nvme(&bytes), length as u64)
            .expect("combine");
        offset += length as u64;
        extents.push(extent);
    }
    let layout = Layout(ContentLayoutRecord {
        content_id: content_id.clone(),
        committed_seq: CommitSeq(1),
        size_bytes: offset,
        layout: ContentLayout {
            creator_namespace_id: HEAD.namespace_id.clone(),
            extents,
        },
    });
    let bytes = vec![b'z'; tail_length];
    let reference = ContentRef {
        kind: loonfs_types::ContentRefKind::BlobV1,
        content_id: content_id.clone(),
        size_bytes: offset + tail_length as u64,
        checksum: crc
            .crc_combine(&Checksum::crc64nvme(&bytes), tail_length as u64)
            .expect("tail checksum"),
    };
    let mut tail = ProjectedWalTail::default();
    tail.push_object(
        &HEAD.namespace_id,
        &[piece_commit(&reference, offset, bytes, 2)],
        0,
    )
    .expect("tail");
    (layout, tail, reference)
}

/// Checks that a large write sends only the sources' HEAD requests before its
/// assembly, and that a second write of the same pieces assembles a second
/// object under a new key with the same bytes.
#[tokio::test]
async fn large_extents_assemble_with_only_source_heads_and_write_again_under_a_new_key() {
    let directory = tempfile::tempdir().expect("directory");
    let local = std::sync::Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let store = RecordingStore::new(FakeMultipartStore::new(local.clone()), KeyPredicate::any());
    let mib = 1024 * 1024;
    let (layout, tail, reference) = fixture(&store, &[20 * mib, 20 * mib], false, 12 * mib).await;
    let content = tail.content(&reference).expect("content");
    store.reset();
    let permits = Permits::new(std::num::NonZeroUsize::MIN, None);
    let written = write_tail_content(
        ContentWrites::new(&store, &permits, &HEAD),
        &layout,
        &content,
        assemble_tail_content(&HEAD.pool_id, &content).expect("pieces"),
    )
    .await
    .expect("assembly");
    assert_eq!(written.layout.extents.len(), 1);
    let extent = &written.layout.extents[0];
    assert_eq!(span_bounds(&extent.object), Some((0, reference.size_bytes)));
    let operations = store.snapshot();
    assert_eq!(operations.len(), 3);
    assert!(operations[..2]
        .iter()
        .all(|operation| matches!(operation, RecordedOperation::Head { .. })));
    assert_matches!(
        &operations[2],
        RecordedOperation::Assemble { sources, expected, .. }
        if sources.len() == 2 && expected == &reference.checksum
    );
    if let RecordedOperation::Assemble { sources, .. } = &operations[2] {
        for source in sources {
            assert_eq!(
                store.head(&source.key).await.expect("head"),
                Some(source.metadata.clone()),
                "each source includes the metadata returned by the caller's HEAD request"
            );
        }
    }
    let key = extent_object_key(&HEAD.pool_id, extent);
    let metadata = store.head(&key).await.expect("head").expect("span");
    assert_eq!(metadata.checksum, Some(reference.checksum.clone()));
    assert_eq!(extent.object_checksum, reference.checksum);
    let again = write_tail_content(
        ContentWrites::new(&store, &permits, &HEAD),
        &layout,
        &content,
        assemble_tail_content(&HEAD.pool_id, &content).expect("pieces"),
    )
    .await
    .expect("second write");
    let again_extent = &again.layout.extents[0];
    assert_eq!(
        span_bounds(&again_extent.object),
        span_bounds(&extent.object)
    );
    assert_ne!(again_extent.object, extent.object);
    let again_key = extent_object_key(&HEAD.pool_id, again_extent);
    let again_metadata = store.head(&again_key).await.expect("head").expect("span");
    assert_eq!(again_metadata.checksum, metadata.checksum);
    assert_eq!(again_metadata.size_bytes, metadata.size_bytes);
    assert_eq!(again_extent.object_checksum, extent.object_checksum);
}

#[tokio::test]
async fn extent_bound_assembles_behind_the_largest_own_extent_and_keeps_shared_extents() {
    let directory = tempfile::tempdir().expect("directory");
    let store = RecordingStore::new(
        FakeMultipartStore::new(LocalFsStore::new(directory.path()).expect("store")),
        KeyPredicate::any(),
    );
    let mut lengths = vec![200, 100];
    lengths.extend(vec![3; MAX_LAYOUT_EXTENTS - 1]);
    let (layout, tail, reference) = fixture(&store, &lengths, true, 1).await;
    let content = tail.content(&reference).expect("content");
    store.reset();
    let written = write_tail_content(
        ContentWrites::new(
            &store,
            &Permits::new(std::num::NonZeroUsize::MIN, None),
            &HEAD,
        ),
        &layout,
        &content,
        assemble_tail_content(&HEAD.pool_id, &content).expect("pieces"),
    )
    .await
    .expect("bounded layout");
    assert_eq!(written.layout.extents.len(), 3);
    assert_eq!(&written.layout.extents[..2], &layout.0.layout.extents[..2]);
    assert_eq!(
        written.layout.extents[2].length,
        3 * (MAX_LAYOUT_EXTENTS as u64 - 1) + 1
    );
    let operations = store.snapshot();
    assert_matches!(
        operations.last(),
        Some(RecordedOperation::Assemble { sources, .. }) if sources.len() == MAX_LAYOUT_EXTENTS - 1
    );
    let new_object = store
        .head(&extent_object_key(
            &HEAD.pool_id,
            &written.layout.extents[2],
        ))
        .await
        .expect("head")
        .expect("the new object");
    assert_eq!(
        Some(written.layout.extents[2].object_checksum.clone()),
        new_object.checksum,
        "the new extent records the checksum that the store keeps for the assembled object"
    );
    assert_eq!(store.counts().gets, 0);
}

#[tokio::test]
async fn assembly_sources_require_whole_extents_and_stored_checksums_in_the_store_algorithm() {
    let directory = tempfile::tempdir().expect("directory");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let (layout, _, _) = fixture(&local, &[12], false, 0).await;
    let extent = layout.0.layout.extents[0].clone();
    for algorithm in [ChecksumAlgorithm::Crc64nvme, ChecksumAlgorithm::Crc32c] {
        for checksum in [Some(Checksum::compute(algorithm, &[b'a'; 12])), None] {
            let valid = checksum.is_some() && algorithm == local.checksum_algorithm();
            let store = MetadataMapStore::new(&local, KeyPredicate::any(), move |mut metadata| {
                metadata.checksum = checksum.clone();
                metadata
            });
            for offset in [0, 1] {
                let mut extent = extent.clone();
                extent.offset = offset;
                extent.length -= offset;
                let located = LocatedExtent {
                    object_key: extent_object_key(&HEAD.pool_id, &extent),
                    extent,
                };
                let result = assembly_sources(&store, "destination", &[located], &[]).await;
                if valid && offset == 0 {
                    assert_eq!(result.expect("whole source").0.len(), 1);
                } else {
                    assert_matches!(result, Err(CoreError::NamespaceCorrupt(_)));
                }
            }
        }
    }
}

#[tokio::test]
async fn local_large_tail_assembles_without_sources() {
    let directory = tempfile::tempdir().expect("directory");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let store = RecordingStore::new(local, KeyPredicate::any());
    let (layout, _, mut reference) = fixture(&store, &[], false, 0).await;
    let piece = vec![b'z'; 4 * 1024 * 1024];
    let mut records = Vec::new();
    for index in 0..13 {
        let offset = index * piece.len() as u64;
        reference.size_bytes += piece.len() as u64;
        reference.checksum = reference
            .checksum
            .crc_combine(&Checksum::crc64nvme(&piece), piece.len() as u64)
            .expect("combine");
        records.push(piece_commit(&reference, offset, piece.clone(), index + 2));
    }
    let mut tail = ProjectedWalTail::default();
    tail.push_object(&HEAD.namespace_id, &records, 0)
        .expect("tail");
    let content = tail.content(&reference).expect("content");
    let pieces = assemble_tail_content(&HEAD.pool_id, &content).expect("pieces");
    assert_eq!(pieces.len(), 13);
    for (actual, resident) in pieces.iter().zip(&content.pieces) {
        assert_eq!(actual.as_ptr(), resident.bytes.as_ptr());
    }
    let permits = Permits::new(std::num::NonZeroUsize::MIN, None);
    let written = write_tail_content(
        ContentWrites::new(&store, &permits, &HEAD),
        &layout,
        &content,
        pieces,
    )
    .await
    .expect("streamed tail");
    assert_matches!(
        store.snapshot().as_slice(),
        [RecordedOperation::Assemble { sources, .. }] if sources.is_empty()
    );
    let stored = store
        .head(&extent_object_key(
            &HEAD.pool_id,
            &written.layout.extents[0],
        ))
        .await
        .expect("checksum")
        .expect("object");
    assert_eq!(stored.size_bytes, reference.size_bytes);
    assert_eq!(
        written.layout.extents[0].object_checksum,
        reference.checksum
    );
    assert_eq!(stored.checksum, Some(reference.checksum));
}

/// Checks that a read of a whole span object is checked against the object
/// checksum that the extent records, that a read of part of the object isn't,
/// and that a wrong recorded checksum is corruption that identifies the
/// object.
#[tokio::test]
async fn a_read_of_a_whole_span_object_checks_the_extents_object_checksum() {
    let directory = tempfile::tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (mut layout, _, reference) = fixture(&store, &[6, 4], false, 0).await;
    let location = ContentLocation::resolve(&layout, &HEAD.pool_id, None, &reference)
        .await
        .expect("location");
    assert_eq!(
        location
            .read_range(&store, None, 0, 10)
            .await
            .expect("read"),
        Bytes::from_static(b"aaaaaabbbb")
    );
    layout.0.layout.extents[1].object_checksum = Checksum::crc64nvme(b"cccc");
    let object_key = extent_object_key(&HEAD.pool_id, &layout.0.layout.extents[1]);
    let location = ContentLocation::resolve(&layout, &HEAD.pool_id, None, &reference)
        .await
        .expect("location");
    for (start, end) in [(0, 10), (6, 10)] {
        let error = location
            .read_range(&store, None, start, end)
            .await
            .expect_err("a whole object that doesn't match its recorded checksum");
        assert_matches!(
            &error,
            DurableContentValidationError::ContentChecksumMismatch { object_key: key, .. }
                if *key == object_key
        );
        assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
    }
    for (start, end) in [(0, 6), (0, 9), (7, 10)] {
        location
            .read_range(&store, None, start, end)
            .await
            .expect("a read of part of an object isn't checked against its checksum");
    }
}

#[tokio::test]
async fn materialized_tail_prefixes_keep_own_extents_whole() {
    for (size, layout_size) in [(8, 8), (10, 10), (10, 8)] {
        let directory = tempfile::tempdir().expect("directory");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let (mut layout, tail, reference) = fixture(&store, &[6], false, 4).await;
        let extent = &mut layout.0.layout.extents[0];
        extent.object = ExtentObject::Span {
            object_id: SpanObjectId::generate(),
            start: 0,
            end: size,
        };
        extent.length = layout_size;
        let key = extent_object_key(&HEAD.pool_id, extent);
        let bytes = Bytes::from(
            [
                b"aaaaaa".as_slice(),
                &b"zzzz"[..loonfs_types::usize_from_u64(size) - 6],
            ]
            .concat(),
        );
        store
            .put_immutable_verified(&key, bytes)
            .await
            .expect("materialized prefix");
        layout.0.size_bytes = layout_size;
        store.reset();
        let content = tail.content(&reference).expect("tail");
        let written = write_tail_content(
            ContentWrites::new(
                &store,
                &Permits::new(std::num::NonZeroUsize::MIN, None),
                &HEAD,
            ),
            &layout,
            &content,
            assemble_tail_content(&HEAD.pool_id, &content).expect("pieces"),
        )
        .await
        .expect("write");
        assert_eq!(written.layout.size_bytes(), reference.size_bytes);
        assert_eq!(written.layout.extents[0].length, size);
        let operations = store.snapshot();
        assert_matches!(
            operations.last(),
            Some(RecordedOperation::Assemble { sources, bytes, .. })
                if *bytes == 10 - loonfs_types::usize_from_u64(size)
                    && sources.len() == usize::from(size == 10)
        );
    }
}

#[tokio::test]
async fn validation_materialization_holds_a_write_permit() {
    let directory = tempfile::tempdir().expect("directory");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
        OperationSelector::Head,
    );
    let (layout, tail, reference) = fixture(&store, &[6], false, 4).await;
    let permits = Permits::new(std::num::NonZeroUsize::MIN, None);
    store.arm();
    let content = tail.content(&reference);
    let write = materialize_content_layout(
        ContentWrites::new(&store, &permits, &HEAD),
        &layout,
        content.as_ref(),
        &reference,
    );
    let check = async {
        store.wait_until_blocked().await;
        assert_eq!(permits.counts().running, 1);
        store.release();
    };
    let (written, ()) = tokio::join!(write, check);
    assert_eq!(written.expect("combined").extents[0].length, 10);
    assert_eq!(permits.counts(), crate::PermitCounts::default());
}

/// Checks that a namespace that writes the pieces of a chain that another
/// member created records that member as the creator, and writes the pieces
/// as one extent that isn't combined with the chain's extents, although the
/// doubling rule combines them when the creator writes. The fold's write and
/// validation's write share the plan, so both behave the same way.
#[tokio::test]
async fn another_members_chain_keeps_its_creator_and_its_extents() {
    let main = NamespaceId::parse("main").expect("namespace");
    for writer in [&HEAD.namespace_id, &main] {
        for validation in [false, true] {
            let directory = tempfile::tempdir().expect("directory");
            let store = RecordingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::any(),
            );
            // The chain's 6-byte extent is at most twice the 4 tail bytes.
            let (layout, tail, reference) = fixture(&store, &[6], false, 4).await;
            let content = tail.content(&reference).expect("content");
            let permits = Permits::new(std::num::NonZeroUsize::MIN, None);
            let content_writes = ContentWrites {
                store: &store,
                permits: &permits,
                pool_id: &HEAD.pool_id,
                namespace_id: writer,
            };
            store.reset();
            let written = if validation {
                materialize_content_layout(content_writes, &layout, Some(&content), &reference)
                    .await
                    .expect("validation's write")
            } else {
                let pieces = assemble_tail_content(&HEAD.pool_id, &content).expect("pieces");
                write_tail_content(content_writes, &layout, &content, pieces)
                    .await
                    .expect("fold's write")
                    .layout
            };
            assert_eq!(written.creator_namespace_id, HEAD.namespace_id);
            let operations = store.snapshot();
            let Some(RecordedOperation::Assemble { sources, bytes, .. }) = operations.last() else {
                panic!("the write ends with an assembly: {operations:?}");
            };
            assert_eq!(*bytes, 4);
            if writer == &HEAD.namespace_id {
                assert_eq!(sources.len(), 1);
                assert_eq!(written.extents.len(), 1);
                assert_eq!(written.extents[0].length, 10);
            } else {
                assert!(sources.is_empty());
                assert_eq!(written.extents.len(), 2);
                assert_eq!(written.extents[0], layout.0.layout.extents[0]);
                assert_eq!(span_bounds(&written.extents[1].object), Some((6, 10)));
                assert_eq!(written.extents[1].length, 4);
            }
        }
    }
}

/// Checks that a chain without a layout row takes its creator from the WAL
/// object that started it, not from the namespace that writes its pieces.
#[tokio::test]
async fn a_chain_that_started_in_another_members_tail_records_that_member_as_creator() {
    let directory = tempfile::tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (layout, _, reference) = fixture(&store, &[], false, 4).await;
    let source = NamespaceId::parse("source").expect("namespace");
    let mut tail = ProjectedWalTail::default();
    tail.push_object(&source, &[piece_commit(&reference, 0, vec![b'z'; 4], 2)], 0)
        .expect("tail");
    let content = tail.content(&reference).expect("content");
    let written = write_tail_content(
        ContentWrites::new(
            &store,
            &Permits::new(std::num::NonZeroUsize::MIN, None),
            &HEAD,
        ),
        &layout,
        &content,
        assemble_tail_content(&HEAD.pool_id, &content).expect("pieces"),
    )
    .await
    .expect("write");
    assert_eq!(written.layout.creator_namespace_id, source);
    assert_eq!(span_bounds(&written.layout.extents[0].object), Some((0, 4)));
}
