//! Listing ages and store requests made by a complete pass.

use super::*;
use loonfs_objectstore::keys::{content_blob, metadata_segment_object_key};
use loonfs_types::format::manifest::MetadataRowFamily;

#[tokio::test]
async fn a_pass_heads_only_the_discovery_successor() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("no-heads").expect("namespace");
    folded_namespace_with_two_files(&store, &namespace_id).await;
    let manifest = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    let successor = manifest
        .state
        .manifest()
        .manifest_no
        .successor()
        .expect("successor");
    let orphan = content_blob(&namespace_id, &loonfs_types::ContentId::generate());
    store
        .put_if_absent(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    let recorded = RecordingStore::new(
        MetadataMapStore::aged(store, KeyPredicate::any()),
        KeyPredicate::any(),
    );
    let report = gc_namespace(
        &recorded,
        None,
        &namespace_id,
        &options(),
        &context(GRACE_MS + 1),
    )
    .await
    .expect("pass");
    assert_eq!(report.deleted.content_objects, 1);
    let heads: Vec<_> = recorded
        .snapshot()
        .into_iter()
        .filter_map(|operation| match operation {
            loonfs_test_support::stores::RecordedOperation::Head { key } => Some(key),
            _ => None,
        })
        .collect();
    assert_eq!(heads, [metadata_manifest_object(&namespace_id, &successor)]);
    let target = NamespaceId::parse("retired-no-heads").expect("target");
    let merge_memory = Arc::new(tokio::sync::Semaphore::new(32 * 1024 * 1024));
    fork_namespace(
        &recorded,
        &namespace_id,
        &target,
        &loonfs_test_support::test_actor(),
        None,
        &context(1_000),
        Arc::new(StdMonotonicTimer::default()),
        Default::default(),
        None,
        &merge_memory,
    )
    .await
    .expect("fork");
    delete_namespace(
        &recorded,
        &target,
        DeleteNamespaceOptions::default(),
        &context(1_000),
        merge_memory,
    )
    .await
    .expect("delete target");
    recorded.reset();
    let retired = gc_namespace(
        &recorded,
        None,
        &target,
        &options(),
        &context(1_000 + retirement_ms()),
    )
    .await
    .expect("retire fork");
    assert_eq!(retired.deleted_checkpoints_by_owner.fork, 1);
    assert_eq!(recorded.counts().heads, 0);
}

#[tokio::test]
async fn listed_times_retain_undated_content_and_delete_aged_content() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("listed-ages").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("namespace");
    let undated_key = content_blob(&namespace_id, &loonfs_types::ContentId::generate());
    let aged_key = content_blob(&namespace_id, &loonfs_types::ContentId::generate());
    for key in [&undated_key, &aged_key] {
        store
            .put_if_absent(key, Bytes::from_static(b"orphan"))
            .await
            .expect("orphan");
    }
    let aged = MetadataMapStore::aged(store, KeyPredicate::any());
    let undated = MetadataMapStore::without_last_modified(aged, KeyPredicate::exact(&undated_key));
    let recorded = RecordingStore::new(undated, KeyPredicate::any());
    let report = gc_namespace(
        &recorded,
        None,
        &namespace_id,
        &options(),
        &context(GRACE_MS + 1),
    )
    .await
    .expect("pass");
    assert_eq!(report.retained.no_provider_timestamp, 1);
    assert_eq!(report.deleted.content_objects, 1);
    let deleted: Vec<_> = recorded
        .snapshot()
        .into_iter()
        .filter_map(|op| match op {
            loonfs_test_support::stores::RecordedOperation::Delete { key } => Some(key),
            _ => None,
        })
        .collect();
    assert_eq!(deleted, [aged_key]);
    assert!(recorded
        .inner()
        .head(&undated_key)
        .await
        .expect("head retained")
        .is_some());
}

fn layout_segments(manifest: &crate::namespace::control::LoadedManifest) -> BTreeSet<String> {
    manifest
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
        .map(metadata_segment_object_key)
        .collect()
}

#[tokio::test]
async fn superseded_views_add_segment_reads_only_for_uncovered_layout_segments() {
    use crate::manifest::{compaction_step, fold_wal, CompactionStepOutcome};
    use crate::namespace::control::load_current_manifest;
    use crate::namespace::writer_epoch::acquire_writer_epoch;

    for own_runs in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("layout-views").expect("namespace");
        let store = Arc::new(MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        create(&store, &namespace_id, &context(1_000))
            .await
            .expect("namespace");
        for name in ["one", "two"] {
            write_test_file(
                &store,
                &namespace_id,
                &format!("/docs/{name}"),
                name,
                &context(1_000),
            )
            .await;
            fold_wal(&store, &namespace_id).await.expect("fold");
        }
        let previous = load_current_manifest(&store, &namespace_id)
            .await
            .expect("previous");
        if own_runs {
            for _ in 0..16 {
                let outcome = compaction_step(
                    &store,
                    &namespace_id,
                    loonfs_types::CompactorEpoch(0),
                    Default::default(),
                    MetadataCompactionPolicy::CompactImmediately,
                    Arc::default(),
                )
                .await
                .expect("compact");
                if matches!(
                    outcome,
                    CompactionStepOutcome::UnitPublished {
                        group: loonfs_types::MetadataFamilyGroup::ContentLayouts,
                        ..
                    }
                ) {
                    break;
                }
            }
        } else {
            acquire_writer_epoch(&store, &namespace_id, &context(1_000))
                .await
                .expect("supersede");
        }
        let current = load_current_manifest(&store, &namespace_id)
            .await
            .expect("current");
        assert_eq!(
            layout_segments(&previous) != layout_segments(&current),
            own_runs
        );
        let now_ms = GRACE_MS + 1;
        let recent = MetadataMapStore::new(
            Arc::clone(&store),
            KeyPredicate::exact(&current.object_key),
            move |mut meta| {
                meta.last_modified_ms = Some(now_ms);
                meta
            },
        );
        let rooted = RecordingStore::metadata_segments(recent);
        gc_namespace(&rooted, None, &namespace_id, &options(), &context(now_ms))
            .await
            .expect("root predecessor");
        // Reads within one pass have no fixed order, so both lists are
        // compared sorted.
        let mut rooted_reads = rooted.take_get_keys();
        rooted_reads.sort();
        let current_only = RecordingStore::metadata_segments(Arc::clone(&store));
        gc_namespace(
            &current_only,
            None,
            &namespace_id,
            &options(),
            &context(now_ms),
        )
        .await
        .expect("current only");
        let mut current_reads = current_only.take_get_keys();
        current_reads.sort();
        if own_runs {
            assert!(rooted_reads.len() > current_reads.len());
            assert!(layout_segments(&previous)
                .iter()
                .all(|key| rooted_reads.contains(key) && !current_reads.contains(key)));
        } else {
            assert_eq!(rooted_reads, current_reads);
        }
        assert!(current_reads
            .iter()
            .all(|key| layout_segments(&current).contains(key)));
    }
}
