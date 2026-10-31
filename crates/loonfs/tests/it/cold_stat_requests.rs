//! Pins the cold first-stat request shape: a fresh handle resolving a path
//! over a manifest that carries several unmerged delta runs must not pay a
//! per-run filter fetch (the manifest's inline filter copies answer those),
//! every metadata-segment object it does touch is small enough to load
//! whole with a single ranged GET, and a file written in every run costs
//! the segments of its newest runs, not of all of them.
//!
//! This is the regression guard for the cold-stat latency cliff: the run
//! count multiplying into filter-block round-trips on `DirentryBinds`
//! lookups was the scale term, and dependent per-section GETs were the
//! wave count.

use loonfs::{LoonFs, Namespace, NamespaceId, Writable};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_segment_object_key};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{KeyPredicate, RecordedGet, RecordingStore};
use loonfs_types::format::hex::hex_decode_bytes;
use loonfs_types::format::manifest::{
    decode_namespace_manifest_json, lookup_keys, MetadataRowFamily,
};
use loonfs_types::format::segment_blocks::{decode_filter_block, key_range_may_intersect};
use loonfs_types::{AbsolutePath, DisplayName, NameKey};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

/// Publishes already-classified candidates through the publication
/// service. Every candidate is admitted before the publisher's worker can
/// take any of them, so they coalesce into one publication.
async fn publish_candidates(
    namespace: &Namespace<Writable>,
    candidates: Vec<loonfs::publish::CommitCandidate>,
) {
    let submissions = candidates
        .into_iter()
        .map(|candidate| namespace.commit_candidate(candidate));
    for outcome in futures::future::join_all(submissions).await {
        outcome.expect("publish batch member");
    }
}

/// How many segments of `family` in `runs` have a key range that meets
/// `prefix` and an inline filter that admits `probe`. A newest-first lookup
/// that reaches such a segment before the run with its row pays one
/// whole-segment GET for it.
fn segments_admitting(
    runs: &[&loonfs_types::format::manifest::MetadataRunRef],
    family: MetadataRowFamily,
    prefix: &str,
    probe: &str,
) -> usize {
    // Every prefix ends with `-`, and `.` is the next byte.
    let upper_bound = format!("{}.", &prefix[..prefix.len() - 1]);
    runs.iter()
        .flat_map(|run| &run.segments)
        .filter(|descriptor| descriptor.family == family)
        .filter(|descriptor| {
            key_range_may_intersect(
                &descriptor.min_row_key,
                &descriptor.max_row_key,
                descriptor.row_count,
                prefix,
                Some(&upper_bound),
            )
        })
        .filter(|descriptor| {
            let inline = descriptor
                .filter_inline
                .as_deref()
                .expect("every delta segment has an inline filter");
            let bytes = hex_decode_bytes(inline).expect("inline filter hex");
            decode_filter_block(&bytes, &descriptor.filter_block)
                .expect("decode the inline filter")
                .may_contain(probe)
        })
        .count()
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

    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("coldstat-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("build writer");
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("coldstat-maintenance"))
        .build()
        .await
        .expect("build maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("coldstat-maintenance"));
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    // Publish the namespace's first manifest up front, so each maintenance
    // step below adds exactly one delta run to it. A fold needs a tail to
    // fold, so one seed commit comes first; its run holds a single name
    // that sorts past every name looked up below, so range pruning rules it
    // out of those lookups.
    namespace_writer
        .put_file(
            "/tree/dir-000000/seed.txt",
            b"seed",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("seed the first manifest");
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("publish first manifest");
    let catalog = loonfs_core::test_support::namespace_store(&store, &namespace_id)
        .load_catalog_entry()
        .await
        .expect("load namespace catalog");

    // Six commit batches, checkpointed one delta run each, with names
    // interleaved by stride so every run's direntry key range straddles the
    // whole directory span: range pruning alone cannot rule any run out of
    // a name lookup, which is exactly the shape a bulk-loaded backlog takes.
    // Each batch also writes `hot.txt` again, so every run lists one of its
    // revisions.
    const FILES: usize = 180;
    const RUNS: usize = 6;
    for run in 0..RUNS {
        let mut candidates = Vec::new();
        for index in (0..FILES).filter(|index| index % RUNS == run) {
            let bytes = format!("body {index}");
            let stored = loonfs_core::test_support::namespace_store(&store, &namespace_id)
                .store_bytes_as_content(bytes.as_bytes())
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
                loonfs::publish::CommitIntent::single(
                    loonfs::CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    loonfs::publish::FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!("/tree/dir-000000/file-{index:09}.txt"))
                            .expect("path"),
                        content: loonfs_types::FileContent::Ref(content_ref.clone()),
                        behavior: loonfs::DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared],
            ));
        }
        publish_candidates(&namespace_writer, candidates).await;
        namespace_writer
            .put_file_with_options(
                "/tree/dir-000000/hot.txt",
                format!("hot {run}").as_bytes(),
                &loonfs_test_support::test_actor(),
                &loonfs::PutFileOptions {
                    behavior: loonfs::DestinationBehavior::Replace,
                    ..Default::default()
                },
            )
            .await
            .expect("write the file that every run lists");
        maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold the batch into one delta run");
    }

    // Confirm the manifest really carries several straddling delta direntry
    // runs with inline filters — the premise of the assertions below.
    let root = loonfs_core::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load metadata root");
    let manifest_key = metadata_manifest_object(&namespace_id, &root.manifest().manifest_no);
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    let direntry_delta_runs: BTreeSet<_> = manifest
        .payload()
        .runs
        .iter()
        .filter(|run| {
            run.tier == loonfs_types::format::manifest::RunTier::Delta
                && run
                    .segments
                    .iter()
                    .any(|descriptor| descriptor.family == MetadataRowFamily::DirentryBinds)
        })
        .map(|run| run.run_seq)
        .collect();
    assert!(
        direntry_delta_runs.len() >= RUNS,
        "expected at least {RUNS} unmerged delta direntry runs, found {}",
        direntry_delta_runs.len()
    );
    assert!(
        manifest
            .payload()
            .runs
            .iter()
            .filter(|run| run.tier == loonfs_types::format::manifest::RunTier::Delta)
            .flat_map(|run| &run.segments)
            .all(|descriptor| descriptor.filter_inline.is_some()),
        "every delta segment should carry an inline filter"
    );
    let filter_offsets: BTreeSet<(String, u64)> = manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(|descriptor| {
            (
                metadata_segment_object_key(descriptor),
                descriptor.filter_block.offset,
            )
        })
        .collect();
    let segment_keys: BTreeSet<String> = manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect();

    // The measured operation: first stat on a fresh handle, nothing warm,
    // of a file that one run lists and of a file that every run lists.
    let mut segment_gets_per_stat = Vec::new();
    let mut gets_per_stat = Vec::new();
    let mut inode_ids = Vec::new();
    for (name, revision_no) in [("file-000000042.txt", 1), ("hot.txt", RUNS as u64)] {
        let reader = LoonFs::builder_with_store(store.clone())
            .read_only()
            .build()
            .await
            .expect("build reader");
        let namespace = reader.namespace(&namespace_id);
        let _ = log.take_gets();
        let entry = namespace
            .stat(&format!("/tree/dir-000000/{name}"))
            .await
            .expect("cold stat");
        assert_eq!(
            entry
                .display_name
                .as_ref()
                .expect("file entry should carry a display name")
                .as_str(),
            name
        );
        assert_eq!(
            entry.revision_no(),
            Some(loonfs_types::RevisionNo(revision_no)),
            "file stat carries its newest revision"
        );

        let gets = log.take_gets();
        assert_eq!(
            gets.iter().filter(|(key, _)| key.contains("/wal/")).count(),
            1
        );
        let segment_gets: Vec<&RecordedGet> = gets
            .iter()
            .filter(|(key, _)| segment_keys.contains(key))
            .collect();
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
        segment_gets_per_stat.push((name, segment_gets.len()));
        gets_per_stat.push(gets.len());
        inode_ids.push(entry.inode_id);
    }

    // Each stat reads three segments: the first run's bindings for the parent
    // directories, the run with the file's binding, and the run with its
    // newest revision. The file's keys contain its inode ID, whose low bits
    // are random, so a newer run's inline filter can admit a key by a false
    // positive, and the newest-first lookup then reads that run's segment too.
    // The batch runs are the newest `RUNS` delta runs, oldest first.
    let directory = LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("build reader")
        .namespace(&namespace_id)
        .stat("/tree/dir-000000")
        .await
        .expect("stat the directory")
        .inode_id;
    let mut batches: Vec<_> = manifest
        .payload()
        .runs
        .iter()
        .filter(|run| run.tier == loonfs_types::format::manifest::RunTier::Delta)
        .collect();
    batches.sort_by_key(|run| run.run_seq);
    let batches = &batches[batches.len() - RUNS..];
    let false_positive_gets = |name: &str, inode_id, binding_run: usize, revision_run: usize| {
        let name_key = NameKey::for_display_name(&DisplayName::parse(name).expect("name"));
        segments_admitting(
            &batches[binding_run + 1..],
            MetadataRowFamily::DirentryBinds,
            &lookup_keys::direntry_bind_prefix(directory, &name_key),
            &lookup_keys::direntry_bind_probe(directory, &name_key),
        ) + segments_admitting(
            &batches[revision_run + 1..],
            MetadataRowFamily::Revisions,
            &lookup_keys::revision_prefix(inode_id),
            &lookup_keys::revision_probe(inode_id),
        )
    };
    assert_eq!(
        segment_gets_per_stat,
        [
            (
                "file-000000042.txt",
                3 + false_positive_gets("file-000000042.txt", inode_ids[0], 42 % RUNS, 42 % RUNS)
            ),
            (
                "hot.txt",
                3 + false_positive_gets("hot.txt", inode_ids[1], 0, RUNS - 1)
            ),
        ],
        "segment GETs of each cold stat, of {gets_per_stat:?} GETs in all"
    );
}
