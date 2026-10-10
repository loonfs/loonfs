//! Contracts for provider assembly and extent checksums.

use super::*;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{
    BlockingStore, BufferWatchStore, FakeMultipartStore, KeyPredicate, MetadataMapStore,
    OperationClass, RecordedOperation, RecordingStore,
};
use loonfs_types::format::wal::WalInlineContent;
use loonfs_types::{ChangeSeq, ContentId, ContentRef, NamespaceId};

struct Layout(ContentLayoutRecord);

#[async_trait::async_trait]
impl LayoutLookup for Layout {
    async fn content_layout(&self, _: &ContentId) -> Result<Option<ContentLayoutRecord>> {
        Ok(Some(self.0.clone()))
    }
}

async fn fixture<S: ObjectStore>(
    store: &S,
    lengths: &[usize],
    shared: bool,
    tail_length: usize,
) -> (Layout, ProjectedWalTail, ContentRef) {
    let owner = NamespaceId::parse("demo").expect("owner");
    let content_id = ContentId::generate();
    let mut extents = Vec::new();
    let mut offset = 0;
    let mut crc = Checksum::crc64nvme(&[]);
    for (index, &length) in lengths.iter().enumerate() {
        let bytes = Bytes::from(vec![b'a' + index as u8; length]);
        let extent = ContentExtent {
            owner_namespace_id: owner.clone(),
            content_id: if shared && index == 0 {
                ContentId::generate()
            } else {
                content_id.clone()
            },
            object: ExtentObject::Span {
                start: offset,
                end: offset + length as u64,
            },
            offset: 0,
            length: length as u64,
        };
        store
            .put_immutable_verified(&extent_object_key(&extent), bytes.clone())
            .await
            .expect("source");
        crc = crc
            .crc_combine(&Checksum::crc64nvme(&bytes), length as u64)
            .expect("combine");
        offset += length as u64;
        extents.push(extent);
    }
    let layout = Layout(ContentLayoutRecord {
        owner_namespace_id: owner.clone(),
        content_id: content_id.clone(),
        committed_seq: ChangeSeq(1),
        size_bytes: offset,
        layout: ContentLayout { extents },
    });
    let bytes = vec![b'z'; tail_length];
    let reference = ContentRef {
        kind: loonfs_types::ContentRefKind::BlobV1,
        owner_namespace_id: owner,
        content_id: content_id.clone(),
        size_bytes: offset + tail_length as u64,
        checksum: crc
            .crc_combine(&Checksum::crc64nvme(&bytes), tail_length as u64)
            .expect("tail checksum"),
    };
    let mut tail = ProjectedWalTail::default();
    tail.insert_piece(
        &reference,
        &None,
        &WalInlineContent {
            content_id,
            offset,
            bytes,
        },
        ChangeSeq(2),
    );
    (layout, tail, reference)
}

#[tokio::test]
async fn large_extents_assemble_with_only_source_heads_and_retry_without_writes() {
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
    let pool = Semaphore::new(0);
    let written = write_tail_content(
        &store,
        &layout,
        &tail,
        content,
        assemble_tail_content(content).expect("pieces"),
        &pool,
    )
    .await
    .expect("assembly");
    assert_eq!(written.layout.extents.len(), 1);
    let extent = &written.layout.extents[0];
    assert_eq!(
        extent.object,
        ExtentObject::Span {
            start: 0,
            end: reference.size_bytes
        }
    );
    let operations = store.snapshot();
    assert_eq!(operations.len(), 3);
    assert!(operations[..2]
        .iter()
        .all(|operation| matches!(operation, RecordedOperation::Head { .. })));
    assert!(
        matches!(&operations[2], RecordedOperation::Assemble { sources, expected, .. }
        if sources.len() == 2 && expected == &reference.checksum)
    );
    let key = extent_object_key(extent);
    let metadata = store.head(&key).await.expect("head").expect("span");
    assert_eq!(metadata.checksum, Some(reference.checksum.clone()));
    local.reset();
    let retried = write_tail_content(
        &store,
        &layout,
        &tail,
        content,
        assemble_tail_content(content).expect("pieces"),
        &pool,
    )
    .await
    .expect("retry");
    assert_eq!(retried, written);
    assert_eq!(local.counts().puts, 0);
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
        &store,
        &layout,
        &tail,
        content,
        assemble_tail_content(content).expect("pieces"),
        &Semaphore::new(0),
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
    assert!(
        matches!(operations.last(), Some(RecordedOperation::Assemble { sources, .. }) if sources.len() == MAX_LAYOUT_EXTENTS - 1)
    );
    assert_eq!(store.counts().gets, 0);
}

#[tokio::test]
async fn whole_extent_reads_require_the_store_checksum_and_ranges_stay_unverified() {
    for algorithm in [ChecksumAlgorithm::Crc64nvme, ChecksumAlgorithm::Crc32c] {
        let directory = tempfile::tempdir().expect("directory");
        let local = LocalFsStore::new(directory.path()).expect("store");
        let extent = ContentExtent {
            owner_namespace_id: NamespaceId::parse("demo").expect("owner"),
            content_id: ContentId::generate(),
            object: ExtentObject::Whole,
            offset: 0,
            length: 12,
        };
        let mut located = LocatedExtent {
            object_key: extent_object_key(&extent),
            extent,
        };
        local
            .put(
                &located.object_key,
                Bytes::from_static(b"whole object"),
                PutMode::Overwrite,
            )
            .await
            .expect("put");
        for expected in [
            Some(Checksum::compute(algorithm, b"whole object")),
            Some(Checksum::compute(algorithm, b"wrong object")),
            Some(Checksum::sha256(b"whole object")),
            None,
        ] {
            let correct = expected == Some(Checksum::compute(algorithm, b"whole object"));
            let missing = expected.is_none();
            let store = MetadataMapStore::new(&local, KeyPredicate::any(), move |mut metadata| {
                metadata.checksum = expected.clone();
                metadata
            })
            .algorithm(algorithm);
            let mut bytes = Vec::new();
            let result = read_extent(&store, &located, &mut bytes).await;
            if correct {
                result.expect("correct checksum");
                assert_eq!(bytes, b"whole object");
            } else {
                assert!(matches!(result, Err(CoreError::NamespaceCorrupt(_))));
            }
            located.extent.offset = 1;
            located.extent.length = 3;
            let mut bytes = Vec::new();
            let result = read_extent(&store, &located, &mut bytes).await;
            if missing {
                assert!(matches!(result, Err(CoreError::NamespaceCorrupt(_))));
            } else {
                result.expect("range");
                assert_eq!(bytes, b"hol");
            }
            located.extent.offset = 0;
            located.extent.length = 12;
        }
    }
}

#[tokio::test]
async fn local_large_tail_streams_pieces_without_merge_permits() {
    let directory = tempfile::tempdir().expect("directory");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let watched = std::sync::Arc::new(BufferWatchStore::new(local, KeyPredicate::any()));
    let store = RecordingStore::new(watched.clone(), KeyPredicate::any());
    let (layout, _, mut reference) = fixture(&store, &[], false, 0).await;
    let piece = vec![b'z'; 4 * 1024 * 1024];
    let mut tail = ProjectedWalTail::default();
    for index in 0..13 {
        let offset = index * piece.len() as u64;
        reference.size_bytes += piece.len() as u64;
        reference.checksum = reference
            .checksum
            .crc_combine(&Checksum::crc64nvme(&piece), piece.len() as u64)
            .expect("combine");
        tail.insert_piece(
            &reference,
            &None,
            &WalInlineContent {
                content_id: reference.content_id.clone(),
                offset,
                bytes: piece.clone(),
            },
            ChangeSeq(index + 2),
        );
    }
    let content = tail.content(&reference).expect("content");
    let pieces = assemble_tail_content(content).expect("pieces");
    assert_eq!(pieces.len(), 13);
    for (actual, resident) in pieces.iter().zip(&content.pieces) {
        assert_eq!(actual.as_ptr(), resident.bytes.as_ptr());
    }
    let pool = Semaphore::new(0);
    let written = write_tail_content(&store, &layout, &tail, content, pieces, &pool)
        .await
        .expect("streamed tail");
    let peaks = watched.peaks();
    assert_eq!(peaks.total_bytes, 52 * 1024 * 1024);
    assert_eq!(peaks.largest_buffer_bytes, piece.len() as u64);
    assert!(peaks.peak_live_bytes <= CONTENT_READ_CHUNK_BYTES);
    assert!(matches!(
        store.snapshot().as_slice(),
        [RecordedOperation::PutImmutableStream { .. }]
    ));
    let stored = store
        .head(&extent_object_key(&written.layout.extents[0]))
        .await
        .expect("checksum")
        .expect("object");
    assert_eq!(stored.size_bytes, reference.size_bytes);
    assert_eq!(stored.checksum, Some(reference.checksum));
}

#[tokio::test]
async fn in_memory_merge_reserves_extents_and_tail_before_reading() {
    let directory = tempfile::tempdir().expect("directory");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
        OperationClass::Head,
    );
    let (layout, tail, reference) = fixture(&store, &[6], false, 4).await;
    let content = tail.content(&reference).expect("content");
    let pool = Semaphore::new(10);
    store.arm();
    let write = write_tail_content(
        &store,
        &layout,
        &tail,
        content,
        assemble_tail_content(content).expect("pieces"),
        &pool,
    );
    let check = async {
        store.wait_until_blocked().await;
        assert_eq!(pool.available_permits(), 0);
        store.release();
    };
    let (written, ()) = tokio::join!(write, check);
    assert_eq!(written.expect("merged").layout.extents[0].length, 10);
    assert_eq!(pool.available_permits(), 10);
}
