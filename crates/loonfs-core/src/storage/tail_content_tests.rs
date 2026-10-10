//! Contracts for provider assembly and extent attestations.

use super::*;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{
    FakeMultipartStore, KeyPredicate, MetadataMapStore, RecordedOperation, RecordingStore,
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
    assert_eq!(metadata.attestation, Some(reference.checksum.clone()));
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
async fn reading_an_extent_checks_its_crc_attestation() {
    let directory = tempfile::tempdir().expect("directory");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let expected = Checksum::crc32c(b"whole object");
    let store = MetadataMapStore::new(local, KeyPredicate::any(), move |mut metadata| {
        metadata.attestation = Some(expected.clone());
        metadata
    });
    let extent = ContentExtent {
        owner_namespace_id: NamespaceId::parse("demo").expect("owner"),
        content_id: ContentId::generate(),
        object: ExtentObject::Whole,
        offset: 0,
        length: 12,
    };
    let located = LocatedExtent {
        object_key: extent_object_key(&extent),
        extent,
    };
    store
        .put(
            &located.object_key,
            Bytes::from_static(b"whole object"),
            PutMode::Overwrite,
        )
        .await
        .expect("put");
    let mut bytes = Vec::new();
    read_extent(&store, &located, &mut bytes)
        .await
        .expect("verified CRC");
    assert_eq!(bytes, b"whole object");
    store
        .put(
            &located.object_key,
            Bytes::from_static(b"wrong object"),
            PutMode::Overwrite,
        )
        .await
        .expect("corrupt");
    assert!(matches!(
        read_extent(&store, &located, &mut Vec::new()).await,
        Err(CoreError::NamespaceCorrupt(_))
    ));
}
