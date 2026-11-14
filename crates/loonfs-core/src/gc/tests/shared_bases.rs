use super::*;
use crate::manifest::tests::{
    build_namespace_manifest_from_metadata_state, write_namespace_manifest, ManifestMetadataSource,
};
use crate::metadata::MetadataStateBuilder;
use crate::namespace::state::NamespaceReadState;
use crate::segment::MetadataRunPolicy;
use crate::storage::content_location::extent_object_key;
use loonfs_objectstore::keys::{content_blob, content_prefix, metadata_segment_object_key};
use loonfs_objectstore::ByteRange;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::manifest::{
    ContentLayoutRecord, MetadataRowFamily, NamespaceManifestPayload,
};
use loonfs_types::format::segment_blocks::decode_filter_block;
use loonfs_types::{CommitSeq, ContentExtent, ContentId, ContentLayout, ExtentObject};

/// Returns a layout whose chain reads one byte of another chain's object (a
/// shared base) before one byte of its own object.
fn shared_layout(namespace_id: &NamespaceId, index: usize) -> ContentLayoutRecord {
    let content_id = ContentId::parse(format!("con_1{index:025}")).expect("chain id");
    let base_id = ContentId::parse(format!("con_2{index:025}")).expect("base id");
    ContentLayoutRecord {
        content_id: content_id.clone(),
        size_bytes: 2,
        committed_seq: CommitSeq(1),
        layout: ContentLayout {
            creator_namespace_id: namespace_id.clone(),
            extents: [base_id, content_id]
                .into_iter()
                .map(|content_id| ContentExtent {
                    content_id,
                    object: ExtentObject::Whole,
                    offset: 0,
                    length: 1,
                })
                .collect(),
        },
    }
}

async fn shared_layout_manifest(
    store: &impl ObjectStore,
    namespace_id: &NamespaceId,
    rows: usize,
) -> (NamespaceManifestPayload, BTreeSet<String>) {
    layout_manifest(
        store,
        namespace_id,
        (0..rows).map(|index| shared_layout(namespace_id, index)),
    )
    .await
}

async fn layout_manifest(
    store: &impl ObjectStore,
    namespace_id: &NamespaceId,
    rows: impl IntoIterator<Item = ContentLayoutRecord>,
) -> (NamespaceManifestPayload, BTreeSet<String>) {
    let mut head =
        NamespaceReadState::initial(namespace_id.clone(), 0, loonfs_test_support::test_actor());
    head.seq = CommitSeq(1);
    let mut state = MetadataStateBuilder::default();
    let mut keys = BTreeSet::new();
    for row in rows {
        for extent in &row.layout.extents {
            let key = extent_object_key(&head.pool_id, extent);
            store
                .put_if_absent(&key, Bytes::from_static(b"x"))
                .await
                .expect("content");
            keys.insert(key);
        }
        state.push_content_layout(row);
    }
    let manifest = build_namespace_manifest_from_metadata_state(
        store,
        namespace_id,
        ManifestMetadataSource {
            head: &head,
            basis_manifest_no: None,
            retention_floor_seq: head.seq,
            metadata_state: &state.build(),
        },
        MetadataRunPolicy {
            max_rows_per_segment: NonZeroUsize::new(8).expect("segment rows"),
            ..Default::default()
        },
        ManifestNo(1),
    )
    .await
    .expect("manifest");
    (manifest.payload().clone(), keys)
}

/// Publishes `manifest` as a namespace's first manifest, with the
/// namespace's member entry in the pool that the manifest records.
async fn publish_layout_manifest(store: &impl ObjectStore, manifest: NamespaceManifestPayload) {
    crate::test_support::write_member_entry(store, &manifest.pool_id, &manifest.namespace_id).await;
    write_namespace_manifest(store, manifest)
        .await
        .expect("write manifest");
}

/// Returns the object key and byte range of the extent filter of each layout
/// segment that `manifest` lists.
fn extent_filter_reads(manifest: &NamespaceManifestPayload) -> BTreeSet<(String, u64, u64)> {
    manifest
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
        .map(|segment| {
            let handle = segment
                .extent_filter_block
                .expect("a layouts segment has an extent filter");
            (
                metadata_segment_object_key(segment),
                handle.offset,
                handle.offset + u64::from(handle.stored_bytes),
            )
        })
        .collect()
}

/// Returns the reads in `operations` of a range that `reads` lists.
fn extent_filter_gets(
    operations: &[RecordedOperation],
    reads: &BTreeSet<(String, u64, u64)>,
) -> Vec<(String, u64, u64)> {
    operations
        .iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Get {
                key,
                range:
                    Some(ByteRange {
                        start_inclusive,
                        end_exclusive,
                    }),
                ..
            } => Some((key.clone(), *start_inclusive, *end_exclusive)),
            _ => None,
        })
        .filter(|read| reads.contains(read))
        .collect()
}

/// Checks that a collection reads the extent filter of each distinct layout segment
/// once, lists the content prefix once, and deletes only the orphans, and that
/// a second collection through the same segment cache reads no extent filter.
#[tokio::test]
async fn a_collection_reads_each_extent_filter_once_and_lists_content_once() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("many-shared-bases").expect("namespace");
    let (manifest, protected) = shared_layout_manifest(&store, &namespace_id, 2_048).await;
    let reads = extent_filter_reads(&manifest);
    assert_eq!(reads.len(), 256, "eight rows in each layout segment");
    let pool_id = manifest.pool_id.clone();
    publish_layout_manifest(&store, manifest).await;
    let mut orphans = BTreeSet::new();
    for index in 0..8 {
        let content_id = ContentId::parse(format!("con_7{index:025}")).expect("orphan id");
        let key = content_blob(&pool_id, &content_id);
        store
            .put_if_absent(&key, Bytes::from_static(b"orphan"))
            .await
            .expect("orphan");
        orphans.insert(key);
    }
    let cache = Arc::new(crate::cache::MetadataSegmentCache::new(
        Arc::new(crate::cache::SharedSegmentBlocks::new(
            usize::MAX,
            usize::MAX,
        )),
        crate::cache::ObjectStoreScope::new(0),
        Arc::new(crate::cache::ReadWorkingMemory::new(64 * 1024 * 1024, None)),
    ));
    let prefix = content_prefix(&pool_id);
    for (round, deleted, filter_gets) in [(1, &orphans, reads.len()), (2, &BTreeSet::new(), 0)] {
        store.reset();
        let report = crate::PoolStore {
            segment_cache: Arc::clone(&cache),
            ..crate::test_support::pool_store(&store, &pool_id)
        }
        .gc(None, &context(GC_GRACE_WINDOW_MS + 1).clock())
        .await
        .expect("collection");
        assert_eq!(report.deleted.content_objects, deleted.len() as u64);
        let operations = store.snapshot();
        let gets = extent_filter_gets(&operations, &reads);
        assert_eq!(gets.len(), filter_gets, "collection {round}");
        assert_eq!(gets.into_iter().collect::<BTreeSet<_>>().len(), filter_gets);
        let listed: Vec<_> = operations
            .iter()
            .filter_map(|operation| match operation {
                RecordedOperation::List { prefix: listed } if listed.starts_with(&prefix) => {
                    Some(listed.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(listed, vec![prefix.clone()], "collection {round}");
        let deletes: BTreeSet<_> = operations
            .iter()
            .filter_map(|operation| match operation {
                RecordedOperation::Delete { key } => Some(key.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(&deletes, deleted, "collection {round}");
    }
    assert_eq!(
        store
            .list_prefix(&prefix)
            .await
            .expect("survivors")
            .into_iter()
            .collect::<BTreeSet<_>>(),
        protected
    );
}

/// Checks that an object that an extent filter matches by chance stays while
/// a rooted manifest lists the filter's segment, and is collected once none
/// does.
#[tokio::test]
async fn a_false_match_of_an_extent_filter_stays_until_no_root_lists_its_segment() {
    let directory = tempdir().expect("directory");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("extent-false-match").expect("namespace");
    // One layout segment of eight chains, each of 25 whole objects, so the
    // segment's extent filter lists 200 names.
    let rows = (0..8_u64).map(|chain| ContentLayoutRecord {
        content_id: ContentId::parse(format!("con_1{chain:025}")).expect("chain id"),
        size_bytes: 25,
        committed_seq: CommitSeq(1),
        layout: ContentLayout {
            creator_namespace_id: namespace_id.clone(),
            extents: (0..25)
                .map(|object| ContentExtent {
                    content_id: ContentId::parse(format!("con_3{:025}", chain * 25 + object))
                        .expect("object id"),
                    object: ExtentObject::Whole,
                    offset: object,
                    length: 1,
                })
                .collect(),
        },
    });
    let (manifest, protected) = layout_manifest(&store, &namespace_id, rows).await;
    let segment = manifest
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .find(|segment| segment.family == MetadataRowFamily::ContentLayouts)
        .expect("a layouts segment")
        .clone();
    let handle = segment.extent_filter_block.expect("an extent filter");
    let stored = store
        .get(
            &metadata_segment_object_key(&segment),
            Some(ByteRange {
                start_inclusive: handle.offset,
                end_exclusive: handle.offset + u64::from(handle.stored_bytes),
            }),
        )
        .await
        .expect("read the extent filter")
        .expect("the segment");
    let filter = decode_filter_block(&stored, &handle).expect("decode the extent filter");
    // The filter has the minimum 4,096 bits for its 200 names, so about one
    // name in 5,800 matches it by chance, and 200,000 candidates contain no
    // match about once in 10^15.
    let orphan = (0..200_000)
        .map(|index| ContentId::parse(format!("con_7{index:025}")).expect("orphan id"))
        .find(|content_id| filter.may_contain(content_id.as_str()))
        .expect("a name that the filter matches by chance");
    let orphan = content_blob(&manifest.pool_id, &orphan);
    store
        .put_if_absent(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    let pool_id = manifest.pool_id.clone();
    publish_layout_manifest(&store, manifest).await;
    let report = collect_pool(&store, &pool_id, &context(GC_GRACE_WINDOW_MS + 1)).await;
    assert_eq!(report.deleted.content_objects, 0);

    // The next manifest lists only a layout segment of another chain.
    let (mut unlisted, _) =
        layout_manifest(&store, &namespace_id, [shared_layout(&namespace_id, 1)]).await;
    unlisted.manifest_no = ManifestNo(2);
    write_namespace_manifest(&store, unlisted)
        .await
        .expect("a manifest that doesn't list the first layout segment");
    let report = collect_pool(&store, &pool_id, &context(GC_GRACE_WINDOW_MS + 1)).await;
    assert_eq!(report.deleted.content_objects, protected.len() as u64 + 1);
    assert!(store.head(&orphan).await.expect("head").is_none());
}

/// Checks that an extent that only a superseded layout row lists stays
/// protected while the current manifest lists that row's segment, and is
/// collected once a compacted manifest is current and no rooted manifest
/// lists the segment.
#[tokio::test]
async fn an_extent_of_a_superseded_layout_row_stays_until_no_root_lists_its_segment() {
    use loonfs_types::format::manifest::{MetadataRunRef, RunTier};
    use loonfs_types::RunNo;

    let directory = tempdir().expect("directory");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("superseded-layout").expect("namespace");
    let chain = numbered_content_id(1);
    let layout = |object, size_bytes, committed_seq| ContentLayoutRecord {
        content_id: chain.clone(),
        size_bytes,
        committed_seq: CommitSeq(committed_seq),
        layout: ContentLayout {
            creator_namespace_id: namespace_id.clone(),
            extents: vec![ContentExtent {
                content_id: chain.clone(),
                object,
                offset: 0,
                length: size_bytes,
            }],
        },
    };
    // The longer layout, from a later run, supersedes the first one and lists
    // a combined object instead of the first object.
    let (mut first, superseded) =
        layout_manifest(&store, &namespace_id, [layout(ExtentObject::Whole, 1, 1)]).await;
    let (longer, kept) = layout_manifest(
        &store,
        &namespace_id,
        [layout(ExtentObject::Span { start: 0, end: 2 }, 2, 2)],
    )
    .await;
    let longer_run = |tier| MetadataRunRef {
        run_no: RunNo(1),
        run_seq: CommitSeq(2),
        tier,
        segments: longer.runs[0].segments.clone(),
    };
    first.head_seq = CommitSeq(2);
    first.next_run_no = RunNo(2);
    let mut compacted = first.clone();
    first.runs.push(longer_run(RunTier::Delta));
    compacted.manifest_no = ManifestNo(2);
    compacted.runs = vec![longer_run(RunTier::Base)];
    let pool_id = first.pool_id.clone();
    publish_layout_manifest(&store, first).await;

    let now_ms = GC_GRACE_WINDOW_MS + 1;
    let collect = async |deleted, survivors: BTreeSet<String>| {
        let report = collect_pool(&store, &pool_id, &context(now_ms)).await;
        assert_eq!(report.deleted.content_objects, deleted);
        assert_eq!(
            store
                .list_prefix(&content_prefix(&pool_id))
                .await
                .expect("survivors")
                .into_iter()
                .collect::<BTreeSet<_>>(),
            survivors
        );
    };
    // Every object reports an ancient age, and the current manifest still
    // lists the superseded row's segment.
    collect(0, superseded.union(&kept).cloned().collect()).await;
    write_namespace_manifest(&store, compacted)
        .await
        .expect("compacted manifest");
    collect(1, kept).await;
}
