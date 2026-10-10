use super::*;
use crate::bloom_filter::BloomFilter;
use crate::gc::content::ContentSweep;
use crate::gc::live_set::LiveSet;
use crate::limits::MAX_BLOOM_FILTER_BYTES;
use crate::manifest::tests::{
    build_namespace_manifest_from_metadata_state, write_namespace_manifest, ManifestMetadataSource,
};
use crate::manifest::MetadataLsmPolicy;
use crate::metadata::MetadataStateBuilder;
use crate::namespace::state::NamespaceReadState;
use crate::storage::content_location::extent_object_key;
use loonfs_objectstore::keys::{content_blob, content_prefix, temporary_object};
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::control::{encode_control_state, HintPayload};
use loonfs_types::format::manifest::{ContentLayoutRecord, NamespaceManifestPayload};
use loonfs_types::{ChangeSeq, ContentExtent, ContentId, ContentLayout, ExtentObject};

fn shared_layout(namespace_id: &NamespaceId, index: usize) -> ContentLayoutRecord {
    let shard = index % 16;
    let content_id = ContentId::parse(format!("con_{shard:x}a{index:030x}")).expect("chain id");
    let base_id =
        ContentId::parse(format!("con_{:x}b{index:030x}", (shard + 1) % 16)).expect("base id");
    ContentLayoutRecord {
        owner_namespace_id: namespace_id.clone(),
        content_id: content_id.clone(),
        size_bytes: 2,
        committed_seq: ChangeSeq(1),
        layout: ContentLayout {
            extents: [base_id, content_id]
                .into_iter()
                .map(|content_id| ContentExtent {
                    owner_namespace_id: namespace_id.clone(),
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
    let mut state = MetadataStateBuilder::default();
    let mut keys = BTreeSet::new();
    for row in rows {
        for extent in &row.layout.extents {
            let key = extent_object_key(extent);
            store
                .put_if_absent(&key, Bytes::from_static(b"x"))
                .await
                .expect("content");
            keys.insert(key);
        }
        state.push_content_layout(row);
    }
    let mut head =
        NamespaceReadState::initial(namespace_id.clone(), 0, loonfs_test_support::test_actor());
    head.seq = ChangeSeq(1);
    let manifest = build_namespace_manifest_from_metadata_state(
        store,
        namespace_id,
        ManifestMetadataSource {
            head: &head,
            basis_manifest_no: None,
            retention_floor_seq: head.seq,
            metadata_state: &state.finish(),
        },
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::new(8).expect("segment rows"),
            ..Default::default()
        },
        ManifestNo(1),
    )
    .await
    .expect("manifest");
    (manifest.payload().clone(), keys)
}

async fn publish_layout_manifest(store: &impl ObjectStore, manifest: NamespaceManifestPayload) {
    let hint_key = hint(&manifest.namespace_id);
    let hint_bytes = encode_control_state(
        ControlObjectKind::Hint,
        &HintPayload {
            namespace_id: manifest.namespace_id.clone(),
            manifest_no: manifest.manifest_no,
        },
    )
    .expect("hint");
    write_namespace_manifest(store, manifest)
        .await
        .expect("write manifest");
    store
        .put_if_absent(&hint_key, Bytes::from(hint_bytes))
        .await
        .expect("write hint");
}

#[tokio::test]
async fn distinct_shared_bases_fit_a_small_read_budget_and_orphans_in_every_shard_are_deleted() {
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
    publish_layout_manifest(&store, manifest).await;
    let mut orphans = BTreeSet::new();
    for shard in 0..16 {
        let content_id = ContentId::parse(format!("con_{shard:x}f{:030x}", 0)).expect("orphan id");
        let key = content_blob(&namespace_id, &content_id);
        store
            .put_if_absent(&key, Bytes::from_static(b"orphan"))
            .await
            .expect("orphan");
        orphans.insert(key);
    }
    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(32 * 1024, None));
    let cache = root_scan_cache(Arc::clone(&memory));
    let options = GcOptions {
        content_shard_rows: 128,
        ..options()
    };
    store.reset();
    let report = gc_namespace(
        &store,
        Some(&cache),
        &namespace_id,
        &options,
        &context(GRACE_MS + 1),
    )
    .await
    .expect("pass fits the read budget");
    assert_eq!(report.deleted.content_objects, 16);
    assert_eq!(memory.in_use(), 0);
    let operations = store.snapshot();
    let deleted: BTreeSet<_> = operations
        .iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Delete { key } => Some(key.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(deleted, orphans);
    let prefix = content_prefix(&namespace_id);
    let listed: Vec<_> = operations
        .iter()
        .filter_map(|operation| match operation {
            RecordedOperation::List { prefix: listed } if listed.starts_with(&prefix) => {
                Some(listed.clone())
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

#[tokio::test]
async fn a_shared_base_false_positive_is_collected_with_a_later_pass_seed() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("shared-false-positive").expect("namespace");
    let (manifest, protected) = shared_layout_manifest(&store, &namespace_id, 1).await;
    publish_layout_manifest(&store, manifest).await;
    let base_key = extent_object_key(&shared_layout(&namespace_id, 0).layout.extents[0]);
    let mut first = BloomFilter::new(1, GRACE_MS + 1).expect("filter");
    let mut second = BloomFilter::new(1, GRACE_MS + 2).expect("filter");
    first.insert(base_key.as_bytes()).expect("insert");
    second.insert(base_key.as_bytes()).expect("insert");
    let orphan = (0..100_000)
        .map(|index| {
            content_blob(
                &namespace_id,
                &ContentId::parse(format!("con_f{index:031x}")).expect("orphan id"),
            )
        })
        .find(|key| first.may_contain(key.as_bytes()) && !second.may_contain(key.as_bytes()))
        .expect("false positive only for the first seed");
    store
        .put_if_absent(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    for (now_ms, expected) in [(GRACE_MS + 1, 0), (GRACE_MS + 2, 1)] {
        store.reset();
        let report = gc_namespace(&store, None, &namespace_id, &options(), &context(now_ms))
            .await
            .expect("pass");
        assert_eq!(report.deleted.content_objects, expected);
        let deleted: Vec<_> = store
            .snapshot()
            .into_iter()
            .filter_map(|operation| match operation {
                RecordedOperation::Delete { key } => Some(key),
                _ => None,
            })
            .collect();
        assert_eq!(
            deleted,
            if expected == 0 {
                vec![]
            } else {
                vec![orphan.clone()]
            }
        );
        let mut survivors = protected.clone();
        if expected == 0 {
            survivors.insert(orphan.clone());
        }
        assert_eq!(
            store
                .list_prefix(&content_prefix(&namespace_id))
                .await
                .expect("survivors")
                .into_iter()
                .collect::<BTreeSet<_>>(),
            survivors
        );
    }
}

#[tokio::test]
async fn a_shared_filter_over_its_cap_skips_content_and_still_sweeps_other_families() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("shared-filter-cap").expect("namespace");
    let (mut manifest, _) = shared_layout_manifest(&store, &namespace_id, 1).await;
    manifest.runs[0].segments[0].row_count = MAX_BLOOM_FILTER_BYTES as u64 * 8 / 10 + 1;
    publish_layout_manifest(&store, manifest).await;
    let orphan = content_blob(&namespace_id, &ContentId::generate());
    let temporary = temporary_object(&namespace_id);
    for key in [&orphan, &temporary] {
        store
            .put_if_absent(key, Bytes::from_static(b"orphan"))
            .await
            .expect("orphan");
    }
    store.reset();
    let report = gc_namespace(
        &store,
        None,
        &namespace_id,
        &options(),
        &context(GRACE_MS + 1),
    )
    .await
    .expect("pass");
    assert_eq!(report.deleted.content_objects, 0);
    assert_eq!(report.deleted.temporary_objects, 1);
    let prefix = content_prefix(&namespace_id);
    let segments = metadata_segment_prefix(&namespace_id);
    for operation in store.snapshot() {
        assert!(!operation.key().starts_with(&prefix), "{operation:?}");
        assert!(
            !matches!(
                &operation,
                RecordedOperation::Get { .. } | RecordedOperation::GetWithMetadata { .. }
            ) || !operation.key().starts_with(&segments),
            "{operation:?}"
        );
        if let RecordedOperation::Delete { key } = operation {
            assert_eq!(key, temporary);
        }
    }
}

#[tokio::test]
async fn shared_filter_growth_over_the_cap_skips_content_before_any_content_requests() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("shared-filter-growth-cap").expect("namespace");
    let mut row = shared_layout(&namespace_id, 0);
    row.layout
        .extents
        .extend((1..16).map(|index| shared_layout(&namespace_id, index).layout.extents[0].clone()));
    row.size_bytes = row.layout.extents.len() as u64;
    let (manifest, _) = layout_manifest(&store, &namespace_id, [row]).await;
    publish_layout_manifest(&store, manifest).await;
    let context = context(GRACE_MS + 1);
    let live = LiveSet::load(&store, None, &namespace_id, GRACE_MS, &context)
        .await
        .expect("roots");
    for (byte_limit, expected_sweep) in [(40, true), (18, false)] {
        store.reset();
        let content = ContentSweep::load_with_byte_limit(
            &store,
            None,
            &namespace_id,
            &live,
            128,
            context.now_ms,
            byte_limit,
        )
        .await
        .expect("load content roots");
        assert_eq!(content.is_some(), expected_sweep);
        assert!(store.counts().gets > 0);
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
        let prefix = content_prefix(&namespace_id);
        assert!(store
            .snapshot()
            .iter()
            .all(|operation| !operation.key().starts_with(&prefix)));
    }
}
