//! Pins the cold first-stat request shape: a fresh handle resolving a path
//! over a manifest that carries several unfolded delta runs must not pay a
//! per-run filter fetch (the manifest's inline filter copies answer those),
//! and every metadata-segment object it does touch is small enough to load
//! whole with a single ranged GET.
//!
//! This is the regression guard for the cold-stat latency cliff: the run
//! count multiplying into filter-block round-trips on `DirentryBinds`
//! lookups was the scale term, and dependent per-section GETs were the
//! wave count.

use loonfs::{
    CreateNamespaceOptions, FsAdmin, FsReader, FsWriter, MaintenancePlan,
    MetadataMaintenanceOptions, NamespaceId, PutFileOptions,
};
use loonfs_api::wire::manifest::{decode_namespace_manifest_json, MetadataRowFamily};
use loonfs_api::AbsolutePath;
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_segment_object_key};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{KeyPredicate, RecordedGet, RecordingStore};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

/// Publishes already-classified candidates through the publication
/// service. Every candidate is admitted before the publisher's worker can
/// take any of them, so they coalesce into one publication.
async fn publish_candidates(
    writer: &FsWriter,
    namespace_id: &NamespaceId,
    candidates: Vec<loonfs::publish::CommitCandidate>,
) {
    let publisher = writer.publisher();
    let submissions = candidates
        .into_iter()
        .map(|candidate| publisher.submit_candidate(namespace_id.clone(), candidate));
    for outcome in futures::future::join_all(submissions).await {
        outcome.expect("publish batch member");
    }
}

#[tokio::test]
async fn cold_stat_pays_no_per_run_filter_fetches() {
    let temp_dir = tempdir().expect("tempdir");
    let log = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let store: loonfs::SharedObjectStore = log.clone();
    let namespace_id = NamespaceId::parse("coldstat").expect("valid namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("coldstat-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("coldstat-admin")
        .build()
        .await
        .expect("build admin");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    // Publish the namespace's first manifest up front, so each maintenance
    // step below adds exactly one delta run to it. A flush needs a tail to
    // flush, so one seed commit comes first; its run holds a single name
    // that sorts past every name looked up below, so range pruning rules it
    // out of those lookups.
    writer
        .put_file_bytes(
            &namespace_id,
            "/tree/dir-000000/seed.txt",
            b"seed",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("seed the first manifest");
    admin
        .flush_wal(&namespace_id)
        .await
        .expect("publish first manifest");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");

    // Three commit batches, checkpointed one delta run each, with names
    // interleaved by stride so every run's direntry key range straddles the
    // whole directory span: range pruning alone cannot rule any run out of
    // a name lookup, which is exactly the shape a bulk-loaded backlog takes.
    const FILES: usize = 180;
    const RUNS: usize = 3;
    for run in 0..RUNS {
        let mut candidates = Vec::new();
        for index in (0..FILES).filter(|index| index % RUNS == run) {
            let bytes = format!("body {index}");
            let stored = loonfs_core::content::store_bytes_as_content(
                &store,
                &namespace_id,
                bytes.as_bytes(),
            )
            .await
            .expect("stage content");
            let prepared = loonfs_core::content::prepare_existing_content_ref(
                &store,
                &catalog,
                stored.into_content_ref(),
            )
            .await
            .expect("prepare existing content");
            let content_ref = prepared.content_ref().clone();
            candidates.push(loonfs::publish::CommitCandidate::prepared(
                loonfs::publish::CommitRequest::single(
                    loonfs::CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    loonfs::publish::FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!("/tree/dir-000000/file-{index:09}.txt"))
                            .expect("path"),
                        content_ref: content_ref.clone(),
                        behavior: loonfs::DestinationBehavior::NoReplace,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared],
            ));
        }
        publish_candidates(&writer, &namespace_id, candidates).await;
        admin
            .maintenance_step_namespace(
                &namespace_id,
                MaintenancePlan {
                    metadata: Some(MetadataMaintenanceOptions {
                        max_wal_tail_segments: std::num::NonZeroU64::MIN,
                    }),
                    ..MaintenancePlan::default()
                },
            )
            .await
            .expect("step");
    }

    // Confirm the manifest really carries several straddling delta direntry
    // runs with inline filters — the premise of the assertions below.
    let root = loonfs_core::control::load_namespace_metadata_root_control(&store, &namespace_id)
        .await
        .expect("load metadata root");
    let manifest_key =
        metadata_manifest_object(&namespace_id, &root.state.manifest.manifest_object_id);
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    let direntry_delta_runs: BTreeSet<_> = manifest
        .payload
        .segments
        .iter()
        .filter(|descriptor| {
            descriptor.level == 0 && descriptor.family == MetadataRowFamily::DirentryBinds
        })
        .map(|descriptor| descriptor.run_seq)
        .collect();
    assert!(
        direntry_delta_runs.len() >= RUNS,
        "expected at least {RUNS} unfolded delta direntry runs, found {}",
        direntry_delta_runs.len()
    );
    assert!(
        manifest
            .payload
            .segments
            .iter()
            .filter(|descriptor| descriptor.level == 0)
            .all(|descriptor| descriptor.filter_inline.is_some()),
        "every delta segment should carry an inline filter"
    );
    let filter_offsets: BTreeSet<(String, u64)> = manifest
        .payload
        .segments
        .iter()
        .map(|descriptor| {
            (
                metadata_segment_object_key(descriptor),
                descriptor.filter_block.offset,
            )
        })
        .collect();
    let segment_keys: BTreeSet<String> = manifest
        .payload
        .segments
        .iter()
        .map(metadata_segment_object_key)
        .collect();

    // The measured operation: first stat on a fresh handle, nothing warm.
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let _ = log.take_gets();
    let entry = reader
        .stat_path(
            &namespace_id,
            "/tree/dir-000000/file-000000042.txt",
            Default::default(),
        )
        .await
        .expect("cold stat");
    assert_eq!(
        entry
            .display_name
            .as_ref()
            .expect("file entry should carry a display name")
            .as_str(),
        "file-000000042.txt"
    );
    assert!(
        entry.revision_no().is_some(),
        "file stat carries a revision"
    );

    let gets = log.take_gets();
    let segment_gets: Vec<&RecordedGet> = gets
        .iter()
        .filter(|(key, _)| segment_keys.contains(key))
        .collect();
    assert!(
        !segment_gets.is_empty(),
        "a cold stat reads metadata segments"
    );
    for (key, range) in &segment_gets {
        let start = range.map(|(start, _)| start);
        assert!(
            !filter_offsets.contains(&(key.clone(), start.unwrap_or(0))),
            "cold stat fetched a filter block from `{key}`: inline copies \
             and whole-object reads should answer every filter consultation"
        );
        assert_eq!(
            start,
            Some(0),
            "small segments should load whole with one ranged GET, got a \
             partial read of `{key}` at {start:?}"
        );
    }
    // The wave budget: a handful of control-plane reads plus one
    // whole-object read per touched segment. The exact count is pinned
    // loosely so genuine wave regressions (per-run filter sweeps, split
    // filter/index/data fetches) trip it while block-size tuning does not.
    assert!(
        gets.len() <= 16,
        "cold stat issued {} GETs; expected a bounded, near-flat set: {:?}",
        gets.len(),
        gets
    );
}
