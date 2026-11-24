//! Contracts for provider assembly and extent checksums.

use super::*;
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, FakeMultipartStore, KeyPredicate, MetadataMapStore, OperationSelector,
    RecordedOperation, RecordingStore,
};
use loonfs_types::format::wal::{
    WalCommitDelta, WalCommitPayload, WalCommits, WalInlineContent, WalObjectBody,
};
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

/// Returns a data WAL object's body with `records`.
fn commits(records: Vec<WalCommitPayload>) -> WalObjectBody {
    WalObjectBody::Commits(WalCommits::new(records).expect("commits"))
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

/// Which chain's object an extent of a [`fixture_of`] layout reads, and how
/// much of the object it takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// A span object of the chain itself.
    Own,
    /// A span object of a base chain, taken whole.
    Shared,
    /// A span object of a base chain that stores `extra` more bytes after the
    /// bytes that the extent takes.
    SharedSlice { extra: usize },
    /// The whole object of a base chain's upload.
    SharedUpload,
}

async fn fixture<S: ObjectStore>(
    store: &S,
    lengths: &[usize],
    shared: bool,
    tail_length: usize,
) -> (Layout, ProjectedWalTail, ContentRef) {
    let extents: Vec<_> = (0..)
        .zip(lengths)
        .map(|(index, &length)| {
            let source = if shared && index == 0 {
                Source::Shared
            } else {
                Source::Own
            };
            (length, source)
        })
        .collect();
    fixture_of(store, &extents, tail_length).await
}

/// Returns a layout of a chain whose extents have the lengths and sources of
/// `extents`, with their objects in `store`, a WAL tail with one piece of
/// `tail_length` bytes after them, and the reference that the piece ends.
async fn fixture_of<S: ObjectStore>(
    store: &S,
    extents: &[(usize, Source)],
    tail_length: usize,
) -> (Layout, ProjectedWalTail, ContentRef) {
    let content_id = ContentId::generate();
    let base_id = ContentId::generate();
    let mut layout_extents = Vec::new();
    let mut offset = 0;
    let mut crc = Checksum::crc64nvme(&[]);
    for (index, &(length, source)) in (0_u8..).zip(extents) {
        let taken = vec![b'a' + index % 26; length];
        let extra = match source {
            Source::SharedSlice { extra } => extra,
            _ => 0,
        };
        let bytes = Bytes::from([taken.as_slice(), &vec![b'#'; extra]].concat());
        let span = ExtentObject::Span {
            object_id: SpanObjectId::generate(),
            start: offset,
            end: offset + bytes.len() as u64,
        };
        let (content_id, object) = match source {
            Source::Own => (content_id.clone(), span),
            Source::Shared | Source::SharedSlice { .. } => (base_id.clone(), span),
            Source::SharedUpload => (ContentId::generate(), ExtentObject::Whole),
        };
        let extent = ContentExtent {
            content_id,
            object,
            object_checksum: Checksum::crc64nvme(&bytes),
            offset: 0,
            length: length as u64,
        };
        store
            .put_immutable_verified(&extent_object_key(&HEAD.pool_id, &extent), bytes)
            .await
            .expect("source");
        crc = crc
            .crc_combine(&Checksum::crc64nvme(&taken), length as u64)
            .expect("combine");
        offset += length as u64;
        layout_extents.push(extent);
    }
    let layout = Layout(ContentLayoutRecord {
        content_id: content_id.clone(),
        committed_seq: CommitSeq(1),
        size_bytes: offset,
        layout: ContentLayout {
            creator_namespace_id: HEAD.namespace_id.clone(),
            extents: layout_extents,
        },
    });
    let mut reference = ContentRef {
        kind: loonfs_types::ContentRefKind::BlobV1,
        content_id,
        size_bytes: offset,
        checksum: crc,
    };
    let tail = append_tail(&mut reference, tail_length, 2);
    (layout, tail, reference)
}

/// Extends `reference` by `length` bytes and returns a WAL tail whose one
/// piece, committed at `seq`, stores those bytes.
fn append_tail(reference: &mut ContentRef, length: usize, seq: u64) -> ProjectedWalTail {
    let bytes = vec![b'z'; length];
    let offset = reference.size_bytes;
    reference.size_bytes += length as u64;
    reference.checksum = reference
        .checksum
        .crc_combine(&Checksum::crc64nvme(&bytes), length as u64)
        .expect("tail checksum");
    let mut tail = ProjectedWalTail::default();
    tail.push_object(
        &HEAD.namespace_id,
        &commits(vec![piece_commit(reference, offset, bytes, seq)]),
        0,
    )
    .expect("tail");
    tail
}

/// Writes the tail's pieces of `reference` as the fold does, with `policy`.
async fn fold<S: ObjectStore>(
    store: &S,
    layout: &Layout,
    tail: &ProjectedWalTail,
    reference: &ContentRef,
    policy: CombinePolicy,
) -> ContentLayoutRecord {
    let content = tail.content(reference).expect("content");
    write_tail_content(
        ContentWrites::new(
            store,
            &Permits::new(std::num::NonZeroUsize::MIN, None),
            &HEAD,
        ),
        layout,
        &content,
        assemble_tail_content(&HEAD.pool_id, &content).expect("pieces"),
        policy,
    )
    .await
    .expect("fold's write")
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
        CombinePolicy::OwnExtents,
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
        CombinePolicy::OwnExtents,
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

/// Checks that the extent cap keeps the longest prefix in which each extent
/// is at least as long as every extent after it and the candidate together:
/// with extents of 200 (shared), 100, and 31 of 3 bytes and a 1-byte tail, the
/// write keeps 200 and 100 and assembles one 94-byte extent, whether or not
/// it may combine the shared extent.
#[tokio::test]
async fn extent_cap_keeps_the_prefix_in_which_each_extent_covers_the_rest() {
    for policy in [
        CombinePolicy::OwnExtents,
        CombinePolicy::OwnAndSharedBaseExtents,
    ] {
        let directory = tempfile::tempdir().expect("directory");
        let store = RecordingStore::new(
            FakeMultipartStore::new(LocalFsStore::new(directory.path()).expect("store")),
            KeyPredicate::any(),
        );
        let mut lengths = vec![200, 100];
        lengths.extend(vec![3; MAX_LAYOUT_EXTENTS - 1]);
        let (layout, tail, reference) = fixture(&store, &lengths, true, 1).await;
        store.reset();
        let written = fold(&store, &layout, &tail, &reference, policy).await;
        let lengths: Vec<_> = written
            .layout
            .extents
            .iter()
            .map(|extent| extent.length)
            .collect();
        assert_eq!(lengths, [200, 100, 94], "{policy:?}");
        assert_eq!(&written.layout.extents[..2], &layout.0.layout.extents[..2]);
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
}

/// Checks that a fold in the chain's creator merges a shared base extent that
/// takes its whole object, a span object or an upload's whole object, under
/// the doubling rule, with one HEAD request for the source before the
/// assembly, and that the new extent records the checksum that the store
/// keeps. Validation and a direct download write only the pieces.
#[tokio::test]
async fn a_creators_fold_combines_whole_shared_span_extents_and_other_writes_keep_them() {
    for source in [Source::Shared, Source::SharedUpload] {
        for write in ["fold", "validation", "download"] {
            // A fold merges a shared span that takes its whole object, and
            // keeps a shared whole-object extent because the layout doesn't
            // record the object's length.
            let combines = write == "fold" && source == Source::Shared;
            let directory = tempfile::tempdir().expect("directory");
            let store = RecordingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::any(),
            );
            // The 6-byte extent is at most twice the 4 tail bytes.
            let (layout, tail, reference) = fixture_of(&store, &[(6, source)], 4).await;
            let shared = layout.0.layout.extents[0].clone();
            store.reset();
            let written = match write {
                "fold" => {
                    let policy = CombinePolicy::OwnAndSharedBaseExtents;
                    fold(&store, &layout, &tail, &reference, policy)
                        .await
                        .layout
                }
                "validation" => materialize_content_layout(
                    ContentWrites::new(
                        &store,
                        &Permits::new(std::num::NonZeroUsize::MIN, None),
                        &HEAD,
                    ),
                    &layout,
                    tail.content(&reference).as_ref(),
                    &reference,
                )
                .await
                .expect("validation's write"),
                _ => {
                    let policy = CombinePolicy::OwnExtents;
                    fold(&store, &layout, &tail, &reference, policy)
                        .await
                        .layout
                }
            };
            let operations = store.snapshot();
            let context = format!("{source:?} {write}");
            if combines {
                assert_eq!(written.extents.len(), 1, "{context}");
                let extent = &written.extents[0];
                assert_eq!(extent.content_id, reference.content_id, "{context}");
                assert_eq!(span_bounds(&extent.object), Some((0, 10)), "{context}");
                assert_eq!(extent.object_checksum, reference.checksum, "{context}");
                let key = extent_object_key(&HEAD.pool_id, extent);
                let stored = store.head(&key).await.expect("head").expect("span");
                assert_eq!(stored.checksum.as_ref(), Some(&reference.checksum));
                assert_matches!(
                    operations.as_slice(),
                    [
                        RecordedOperation::Head { key },
                        RecordedOperation::Assemble { sources, expected, .. },
                    ] if *key == extent_object_key(&HEAD.pool_id, &shared)
                        && sources.len() == 1
                        && *expected == reference.checksum,
                    "{context}"
                );
            } else {
                assert_eq!(written.extents.len(), 2, "{context}");
                assert_eq!(written.extents[0], shared, "{context}");
                assert_eq!(written.extents[1].length, 4, "{context}");
                assert_matches!(
                    operations.as_slice(),
                    [RecordedOperation::Assemble { sources, .. }] if sources.is_empty(),
                    "{context}"
                );
            }
        }
    }
}

/// Checks that a shared base extent that takes part of its object stops both
/// rules: a fold keeps it and the 31 extents before it, which the doubling
/// rule would otherwise merge, and combines only the extents after it. The
/// cap counts only the extents after it, so twelve 1-byte folds leave the
/// doubling rule's extents of 8, 3, and 1 bytes after it.
#[tokio::test]
async fn a_shared_base_extent_that_takes_part_of_its_object_stops_both_rules() {
    let directory = tempfile::tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let mut extents = vec![(1, Source::Shared); MAX_LAYOUT_EXTENTS - 1];
    extents.push((2, Source::SharedSlice { extra: 2 }));
    let (mut layout, mut tail, mut reference) = fixture_of(&store, &extents, 1).await;
    let frozen = layout.0.layout.extents.clone();
    let frozen_keys: Vec<_> = frozen
        .iter()
        .map(|extent| extent_object_key(&HEAD.pool_id, extent))
        .collect();
    for seq in 3..15 {
        store.reset();
        let policy = CombinePolicy::OwnAndSharedBaseExtents;
        let written = fold(&store, &layout, &tail, &reference, policy).await;
        assert_eq!(&written.layout.extents[..frozen.len()], frozen.as_slice());
        let operations = store.snapshot();
        let Some(RecordedOperation::Assemble { sources, .. }) = operations.last() else {
            panic!("the fold ends with an assembly: {operations:?}");
        };
        assert!(sources
            .iter()
            .all(|source| !frozen_keys.contains(&source.key)));
        layout.0 = written;
        tail = append_tail(&mut reference, 1, seq);
    }
    let after: Vec<_> = layout.0.layout.extents[frozen.len()..]
        .iter()
        .map(|extent| extent.length)
        .collect();
    assert_eq!(after, [8, 3, 1]);
}

/// Checks that the cap counts the shared base extents that a fold can combine:
/// 31 shared extents and two own extents with a 1-byte tail exceed the cap, so
/// the fold assembles the whole chain into one extent, while a write that may
/// combine only own extents keeps all 34.
#[tokio::test]
async fn the_cap_counts_whole_shared_base_extents() {
    let directory = tempfile::tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let mut extents = vec![(1000, Source::Shared); MAX_LAYOUT_EXTENTS - 1];
    extents.extend([(100, Source::Own), (10, Source::Own)]);
    let (layout, tail, reference) = fixture_of(&store, &extents, 1).await;
    let policy = CombinePolicy::OwnAndSharedBaseExtents;
    let folded = fold(&store, &layout, &tail, &reference, policy).await;
    assert_eq!(folded.layout.extents.len(), 1);
    assert_eq!(
        span_bounds(&folded.layout.extents[0].object),
        Some((0, reference.size_bytes))
    );
    assert_eq!(folded.layout.extents[0].object_checksum, reference.checksum);
    let policy = CombinePolicy::OwnExtents;
    let kept = fold(&store, &layout, &tail, &reference, policy).await;
    assert_eq!(kept.layout.extents.len(), MAX_LAYOUT_EXTENTS + 2);
    assert_eq!(
        &kept.layout.extents[..MAX_LAYOUT_EXTENTS + 1],
        layout.0.layout.extents.as_slice()
    );
}

/// Returns a located extent of `length` bytes of the chain `content_id`, in a
/// span object that it takes whole.
fn located(content_id: &ContentId, length: u64) -> LocatedExtent {
    let extent = ContentExtent {
        content_id: content_id.clone(),
        object: ExtentObject::Span {
            object_id: SpanObjectId::generate(),
            start: 0,
            end: length,
        },
        object_checksum: Checksum::crc64nvme(&[]),
        offset: 0,
        length,
    };
    LocatedExtent {
        object_key: extent_object_key(&HEAD.pool_id, &extent),
        extent,
    }
}

/// Checks the cap through repeated fork, append, and merge cycles. A 1 MB
/// uploaded base, which the fold never merges, is followed in each cycle by
/// a fork's 1,000-byte extent, which a merge writes without combining, and
/// `main`'s fold of 10 appended bytes, which the doubling rule can't merge
/// into the fork's extent. The cap counts only the extents after the base,
/// so it fires at cycle 17 and copies the appended history once (17,170
/// bytes). At cycle 33 it keeps that span, which is longer than everything
/// after it, and at cycle 49 the extents after it have passed its length, so
/// it copies the span again with the rest.
#[test]
fn the_cap_copies_each_span_again_only_after_the_extents_behind_it_reach_its_length() {
    let chain = ContentId::generate();
    let fork = ContentId::generate();
    let own = |extent: &ContentExtent| extent.content_id == chain;
    let mut base = located(&ContentId::generate(), 1_000_000);
    base.extent.object = ExtentObject::Whole;
    let mut layout = vec![base];
    let mut firings = Vec::new();
    for cycle in 1..=49 {
        layout.push(located(&fork, 1_000));
        let CombinePlan {
            retained,
            candidate_length,
        } = combine_plan(&layout, 10, own, |extent| {
            own(extent) || takes_whole_span(extent)
        });
        assert!(retained <= MAX_LAYOUT_EXTENTS, "cycle {cycle}");
        if retained < layout.len() - 1 {
            firings.push((cycle, retained, candidate_length));
        }
        layout.truncate(retained);
        layout.push(located(&chain, candidate_length));
    }
    assert_eq!(firings, [(17, 1, 17_170), (33, 2, 16_160), (49, 1, 49_490)]);
    let lengths: Vec<_> = layout.iter().map(|located| located.extent.length).collect();
    assert_eq!(lengths, [1_000_000, 49_490]);
}

/// Checks the cap's cost when 31 extents each cover everything after them.
/// The prefix then fills every slot but one, so the extent after it absorbs
/// each fold's pieces and is copied again on every fold: a hundred 1-byte
/// folds behind a 256-byte extent copy that extent's span every time, and
/// grow it to 356 bytes, while the 31 extents before it stay as they are.
#[test]
fn a_full_prefix_copies_the_span_after_it_on_every_fold() {
    let chain = ContentId::generate();
    let own = |extent: &ContentExtent| extent.content_id == chain;
    let mut layout: Vec<_> = (0..31)
        .rev()
        .map(|exponent| located(&chain, 512 << exponent))
        .collect();
    let prefix: Vec<_> = layout
        .iter()
        .map(|located| located.object_key.clone())
        .collect();
    layout.push(located(&chain, 256));
    for fold in 1..=100 {
        let CombinePlan {
            retained,
            candidate_length,
        } = combine_plan(&layout, 1, own, own);
        assert_eq!(
            (retained, candidate_length),
            (31, 256 + fold),
            "fold {fold}"
        );
        layout.truncate(retained);
        layout.push(located(&chain, candidate_length));
    }
    let kept: Vec<_> = layout[..31]
        .iter()
        .map(|located| located.object_key.clone())
        .collect();
    assert_eq!(kept, prefix);
    assert_eq!(layout[31].extent.length, 356);
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

/// Checks that an assembly rejects a source object whose stored checksum
/// differs from the checksum that its extent records, with a corruption error
/// that identifies the source object's key.
#[tokio::test]
async fn an_assembly_source_is_stored_with_the_checksum_that_its_extent_records() {
    let directory = tempfile::tempdir().expect("directory");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let (layout, _, _) = fixture(&local, &[12], false, 0).await;
    let extent = layout.0.layout.extents[0].clone();
    assert_eq!(extent.object_checksum, Checksum::crc64nvme(&[b'a'; 12]));
    let algorithm = local.checksum_algorithm();
    let store = MetadataMapStore::new(&local, KeyPredicate::any(), move |mut metadata| {
        metadata.checksum = Some(Checksum::compute(algorithm, &[b'b'; 12]));
        metadata
    });
    let located = LocatedExtent {
        object_key: extent_object_key(&HEAD.pool_id, &extent),
        extent,
    };
    let source_key = located.object_key.clone();
    let error = assembly_sources(&store, "destination", &[located], &[])
        .await
        .expect_err("a source stored with another checksum");
    assert_matches!(
        &error,
        CoreError::NamespaceCorrupt(message) if message.contains(&source_key)
    );
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
    tail.push_object(&HEAD.namespace_id, &commits(records), 0)
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
        CombinePolicy::OwnExtents,
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
        let bytes = Bytes::from(
            [
                b"aaaaaa".as_slice(),
                &b"zzzz"[..loonfs_types::usize_from_u64(size) - 6],
            ]
            .concat(),
        );
        extent.object_checksum = Checksum::crc64nvme(&bytes);
        let key = extent_object_key(&HEAD.pool_id, extent);
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
            CombinePolicy::OwnExtents,
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
                write_tail_content(
                    content_writes,
                    &layout,
                    &content,
                    pieces,
                    CombinePolicy::OwnAndSharedBaseExtents,
                )
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

/// Checks that a fold onto a chain whose source object now stores other bytes
/// of the same length fails with a corruption error after the source's HEAD
/// request, without writing a span object from those bytes.
#[tokio::test]
async fn a_fold_onto_a_source_object_with_other_bytes_writes_no_span_object() {
    let directory = tempfile::tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    // The chain's 6-byte extent is at most twice the 4 tail bytes, so the
    // fold assembles the extent's object into the new span object.
    let (layout, tail, reference) = fixture(&store, &[6], false, 4).await;
    let source_key = extent_object_key(&HEAD.pool_id, &layout.0.layout.extents[0]);
    let etag = store
        .head(&source_key)
        .await
        .expect("head")
        .and_then(|metadata| metadata.etag)
        .expect("the source's etag");
    store
        .put(
            &source_key,
            Bytes::from_static(b"cccccc"),
            loonfs_objectstore::PutMode::CompareAndSwap {
                expected_etag: etag,
            },
        )
        .await
        .expect("other bytes under the source's key");
    let content = tail.content(&reference).expect("content");
    store.reset();
    let error = write_tail_content(
        ContentWrites::new(
            &store,
            &Permits::new(std::num::NonZeroUsize::MIN, None),
            &HEAD,
        ),
        &layout,
        &content,
        assemble_tail_content(&HEAD.pool_id, &content).expect("pieces"),
        CombinePolicy::OwnExtents,
    )
    .await
    .expect_err("a fold onto a source object with other bytes");
    assert_matches!(
        &error,
        CoreError::NamespaceCorrupt(message) if message.contains(&source_key)
    );
    assert_eq!(
        store.snapshot(),
        vec![RecordedOperation::Head { key: source_key }]
    );
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
    tail.push_object(
        &source,
        &commits(vec![piece_commit(&reference, 0, vec![b'z'; 4], 2)]),
        0,
    )
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
        CombinePolicy::OwnExtents,
    )
    .await
    .expect("write");
    assert_eq!(written.layout.creator_namespace_id, source);
    assert_eq!(span_bounds(&written.layout.extents[0].object), Some((0, 4)));
}
