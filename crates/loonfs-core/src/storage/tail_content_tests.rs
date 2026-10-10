//! Contracts for provider assembly and extent checksums.

use super::*;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, FakeMultipartStore, KeyPredicate, MetadataMapStore, OperationClass,
    RecordedOperation, RecordingStore,
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
    let pool = Semaphore::new(1);
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
        &Semaphore::new(1),
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
                    object_key: extent_object_key(&extent),
                    extent,
                };
                let result = assembly_sources(&store, "destination", &[located], &[]).await;
                if valid && offset == 0 {
                    assert_eq!(result.expect("whole source").0.len(), 1);
                } else {
                    assert!(matches!(result, Err(CoreError::NamespaceCorrupt(_))));
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
    let pool = Semaphore::new(1);
    let written = write_tail_content(&store, &layout, &tail, content, pieces, &pool)
        .await
        .expect("streamed tail");
    assert!(matches!(
        store.snapshot().as_slice(),
        [RecordedOperation::Assemble { sources, .. }] if sources.is_empty()
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
            start: 0,
            end: size,
        };
        extent.length = layout_size;
        let key = extent_object_key(extent);
        let bytes = Bytes::from([b"aaaaaa".as_slice(), &b"zzzz"[..size as usize - 6]].concat());
        store
            .put_immutable_verified(&key, bytes)
            .await
            .expect("materialized prefix");
        layout.0.size_bytes = layout_size;
        store.reset();
        let content = tail.content(&reference).expect("tail");
        let written = write_tail_content(
            &store,
            &layout,
            &tail,
            content,
            assemble_tail_content(content).expect("pieces"),
            &Semaphore::new(1),
        )
        .await
        .expect("write");
        assert_eq!(written.layout.size_bytes(), reference.size_bytes);
        assert_eq!(written.layout.extents[0].length, size);
        let operations = store.snapshot();
        assert!(
            matches!(operations.last(), Some(RecordedOperation::Assemble { sources, bytes, .. })
            if *bytes == 10 - size as usize && sources.len() == usize::from(size == 10))
        );
    }
}

#[tokio::test]
async fn validation_materialization_holds_a_write_permit() {
    let directory = tempfile::tempdir().expect("directory");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
        OperationClass::Head,
    );
    let (layout, tail, reference) = fixture(&store, &[6], false, 4).await;
    let pool = Semaphore::new(1);
    store.arm();
    let write = materialize_content_layout(&store, &layout, &tail, &reference, &pool);
    let check = async {
        store.wait_until_blocked().await;
        assert_eq!(pool.available_permits(), 0);
        store.release();
    };
    let (written, ()) = tokio::join!(write, check);
    assert_eq!(written.expect("merged").extents[0].length, 10);
    assert_eq!(pool.available_permits(), 1);
}
