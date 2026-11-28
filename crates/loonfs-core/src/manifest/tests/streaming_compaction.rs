//! Rebuilding a family group in one compaction unit: the plan, the oracle
//! that says a unit and its split pieces reach the same place, restart
//! equivalence, and the resource bounds that make a compaction independent of the
//! size of what it compacts.

use super::*;
use crate::compaction::group_compaction::{run_metadata_compaction, GroupCompactionReport};
use crate::compaction::plan::MetadataCompactionSpec;
use crate::compaction::plan::{group_run_descriptors, plan_group};
use crate::compaction::publication::input_segment_keys;
use crate::compaction::step::MaintenanceCancellation;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::segment::row::manifest_row_commit_seq;
use crate::segment::runs::MetadataFamilyGroup;
use crate::segment::scan::VerifiedMetadataSegments;
use crate::test_support::kept_inodes::{compact_with_seed, whole_group_spec};
use crate::time::{OperationClock, StdMonotonicTimer};
use loonfs_objectstore::keys::metadata_segment_prefix;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    ConcurrencyWatchStore, KeyPredicate, OperationSelector, RecordedOperation, RecordingStore,
};
use std::assert_matches;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn every_family_belongs_to_exactly_one_compaction_group() {
    for family in ApiMetadataRowFamily::ALL {
        let groups: Vec<_> = MetadataFamilyGroup::ALL
            .into_iter()
            .filter(|group| group.families().contains(&family))
            .collect();
        assert_eq!(
            groups.len(),
            1,
            "`{family:?}` belongs to {} compaction groups",
            groups.len()
        );
    }
    let listed: usize = MetadataFamilyGroup::ALL
        .iter()
        .map(|group| group.families().len())
        .sum();
    assert_eq!(
        listed,
        ApiMetadataRowFamily::ALL.len(),
        "the groups must list every family once and nothing else"
    );
}

#[test]
fn retention_clusters_partition_their_group_families() {
    for group in MetadataFamilyGroup::ALL {
        let clusters = retention_clusters(group);
        for family in group.families() {
            let holding = clusters
                .iter()
                .filter(|cluster| cluster.families.contains(family))
                .count();
            assert_eq!(
                holding,
                1,
                "`{family:?}` is in {holding} retention clusters of group `{}`",
                group.as_str()
            );
        }
        for family in clusters.iter().flat_map(|cluster| cluster.families) {
            assert!(
                group.families().contains(family),
                "a cluster of group `{}` lists `{family:?}`, which the group does not hold",
                group.as_str()
            );
        }
    }
}

/// A namespace whose bindings group contains many parent directories, deletions
/// and moves below the retention floor, and more of both above it (enough
/// for a job to cross many slots and for the drop rules to have something to
/// do).
async fn seed_bindings_workload(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context)
        .await
        .expect("bootstrap");

    // Below the floor: files created, some deleted, some moved. The deletions
    // and moves leave unbinds the job may drop.
    for directory in 0..6u64 {
        for file in 0..3u64 {
            crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
                .write_file_bytes(
                    &format!("/d{directory}/f{file}.txt"),
                    format!("body {directory}/{file}\n").as_bytes(),
                    None,
                )
                .await
                .expect("write file");
        }
        create_snapshot(store, namespace_id, &context)
            .await
            .expect("snapshot");
    }
    for directory in 0..3u64 {
        crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
            .delete_path(&format!("/d{directory}/f0.txt"), None)
            .await
            .expect("delete file");
        crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
            .move_path(
                &format!("/d{directory}/f1.txt"),
                &format!("/d{directory}/moved-f1.txt"),
                None,
            )
            .await
            .expect("move file");
    }
    // One deleted directory, so a tombstone and a trash row travel
    // with the bindings the job drops.
    crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
        .delete_path("/d5", None)
        .await
        .expect("delete directory");
    create_snapshot(store, namespace_id, &context)
        .await
        .expect("snapshot the deletions");

    // Compact everything into one base run, so the input has a base under its
    // delta runs the way a real over-budget group does. The base is cut into
    // small segments on purpose: an iterator walks whole segments, so small
    // segments make it open and close several of them.
    drain_compaction(
        store,
        namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy {
                max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
                ..MetadataRunPolicy::default()
            }
        },
    )
    .await;
    crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance the floor past the deletions");

    // Above the floor: fresh directories and one more deletion, published as
    // delta runs the job compacts with the base.
    for directory in 6..9u64 {
        for file in 0..2u64 {
            crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
                .write_file_bytes(
                    &format!("/d{directory}/f{file}.txt"),
                    format!("late body {directory}/{file}\n").as_bytes(),
                    None,
                )
                .await
                .expect("write late file");
        }
        create_snapshot(store, namespace_id, &context)
            .await
            .expect("snapshot late writes");
    }
    crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
        .delete_path("/d6/f0.txt", None)
        .await
        .expect("delete a late file");
    create_snapshot(store, namespace_id, &context)
        .await
        .expect("snapshot the late deletion");
}

/// A namespace whose bindings group is dominated by one directory, so one
/// parent holds most of the group's rows.
///
/// Renames are what make it lopsided. Every rename writes a bind and an
/// unbind under the same parent, and no inode row and no revision row, so the
/// parent grows while the rest of the namespace stays where it was.
async fn seed_one_wide_directory(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context)
        .await
        .expect("bootstrap");
    for file in 0..6u64 {
        crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
            .write_file_bytes(
                &format!("/wide/f{file}.txt"),
                format!("body {file}\n").as_bytes(),
                None,
            )
            .await
            .expect("write file");
    }
    create_snapshot(store, namespace_id, &context)
        .await
        .expect("snapshot the writes");
    for round in 0..10u64 {
        for file in 0..6u64 {
            let from = match round {
                0 => format!("/wide/f{file}.txt"),
                previous => format!("/wide/f{file}-r{}.txt", previous - 1),
            };
            crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
                .move_path(&from, &format!("/wide/f{file}-r{round}.txt"), None)
                .await
                .expect("rename file");
        }
        create_snapshot(store, namespace_id, &context)
            .await
            .expect("snapshot the renames");
    }
    drain_compaction(
        store,
        namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy {
                max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
                ..MetadataRunPolicy::default()
            }
        },
    )
    .await;
    crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance the floor past the renames");

    crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
        .write_file_bytes("/wide/late.txt", b"late body\n", None)
        .await
        .expect("write a late file");
    create_snapshot(store, namespace_id, &context)
        .await
        .expect("snapshot the late write");
}

/// A budget that admits the whole group in one unit.
fn compact_everything_policy() -> MetadataRunPolicy {
    MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_decoded_input_bytes_per_step: NonZeroUsize::new(1 << 30).expect("nonzero"),
        max_input_runs_per_step: NonZeroUsize::new(64).expect("nonzero"),
        ..MetadataRunPolicy::default()
    }
}

/// A budget one byte wide, which admits no run whole, so the planner has no
/// window that makes progress and answers with a compaction.
fn starving_policy() -> MetadataRunPolicy {
    MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    }
}

/// A budget that rolls many small output segments, so the job's writers are
/// exercised rather than producing one segment per family.
fn small_segment_policy() -> MetadataRunPolicy {
    MetadataRunPolicy {
        max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
        ..compact_everything_policy()
    }
}

/// Loads the segments referenced by the current manifest.
async fn load_current_manifest_segments<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> VerifiedMetadataSegments<'a, S> {
    let manifest_number = current_manifest_number(store, namespace_id).await;
    load_manifest_segments_for_inspection(store, None, namespace_id, &manifest_number)
        .await
        .expect("load the current manifest's segments")
}

/// The runs a job reads: every run of the manifest that holds rows of the
/// group, which is what makes the input bottom-anchored and its drops legal.
fn input_runs_for_group(
    manifest: &NamespaceManifestEnvelope,
    group: MetadataFamilyGroup,
) -> Vec<MetadataRunManifest> {
    runs_newest_first(manifest.payload())
        .into_iter()
        .filter(|run| {
            run.segments.iter().any(|family_segments| {
                group.families().contains(&family_segments.family)
                    && !family_segments.segments.is_empty()
            })
        })
        .collect()
}

/// The spec of a base compaction of `group` against the current manifest:
/// the group's oldest window of at most eight runs, the output at the
/// manifest head, and the live retention floor.
async fn compaction_spec_for_group<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) -> MetadataCompactionSpec {
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    whole_group_spec(&current, group)
}

/// The claim a job holds while it runs, opened the way the job driver opens
/// it. These tests split the rebuild from the driver, so they create the
/// lease themselves. Every later write compare-and-swaps against the etag
/// this one returns, so a lease that was never created cannot be refreshed.
#[allow(
    clippy::result_large_err,
    reason = "returns the outcome that stopped the rebuild as its error, as the compaction does"
)]
async fn run_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    policy: MetadataRunPolicy,
    cancellation: &MaintenanceCancellation,
) -> std::result::Result<GroupCompactionReport, CompactionUnitOutcome> {
    let segments = load_current_manifest_segments(store, namespace_id).await;
    run_metadata_compaction(
        &segments,
        &crate::cache::MetadataSegmentCache::unshared(0),
        namespace_id,
        spec,
        policy,
        cancellation,
    )
    .await
    .expect("run the streaming compaction")
}

/// The segments the group's input runs hold right now, which is what
/// finalization compares the manifest against.
async fn input_keys_now<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
) -> BTreeSet<String> {
    let segments = load_current_manifest_segments(store, namespace_id).await;
    input_segment_keys(&segments.scan_runs, spec).expect("the input must be present")
}

/// The production finalizer, called the way the job driver calls it.
///
/// The driver runs the rebuild and this together. These tests split the two so
/// they can assert on what the rebuild produced before it is published, and on
/// what happens when the manifest moves in between.
async fn finalize_streaming_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    input_keys: &BTreeSet<String>,
    result: &GroupCompactionReport,
) -> CompactionUnitOutcome {
    finalize_streaming_compaction_under(
        store,
        namespace_id,
        spec,
        input_keys,
        result,
        &MaintenanceCancellation::default(),
    )
    .await
}

/// The same, with the token the tests that cancel a finalization set.
async fn finalize_streaming_compaction_under<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    input_keys: &BTreeSet<String>,
    result: &GroupCompactionReport,
    cancellation: &MaintenanceCancellation,
) -> CompactionUnitOutcome {
    try_finalize_streaming_compaction(store, namespace_id, spec, input_keys, result, cancellation)
        .await
        .expect("finalize the streaming compaction")
}

/// The same, returning the finalization's error.
async fn try_finalize_streaming_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    input_keys: &BTreeSet<String>,
    result: &GroupCompactionReport,
    cancellation: &MaintenanceCancellation,
) -> crate::Result<CompactionUnitOutcome> {
    publish_compaction_over_current(
        store,
        namespace_id,
        CompactionWrites {
            spec: spec.clone(),
            input_keys: input_keys.clone(),
            report: result.clone(),
            compaction: OperationClock::start(0, Arc::new(StdMonotonicTimer::default())),
        },
        cancellation,
    )
    .await
}

/// The same, for the tests where anything but a publication is a failure.
async fn publish_streaming_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    input_keys: &BTreeSet<String>,
    result: &GroupCompactionReport,
) -> ManifestNo {
    match finalize_streaming_compaction(store, namespace_id, spec, input_keys, result).await {
        CompactionUnitOutcome::Published { manifest, .. } => {
            manifest.manifest().manifest().manifest_no
        }
        other => panic!("no concurrent publisher exists in this test, got {other:?}"),
    }
}

/// Reads and orders the current rows for one family group.
async fn group_rows_from_manifest<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    group: MetadataFamilyGroup,
) -> BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> {
    let mut rows_by_family = BTreeMap::new();
    for family in group.families() {
        let mut rows = segments
            .scan_range(
                *family,
                "",
                None,
                usize::MAX,
                None,
                scan::BlocksPerRefill::One,
            )
            .await
            .expect("scan the group")
            .into_iter()
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.row_key_for_family(*family));
        rows_by_family.insert(*family, rows);
    }
    rows_by_family
}

async fn group_rows_of_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) -> BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> {
    let segments = load_current_manifest_segments(store, namespace_id).await;
    group_rows_from_manifest(&segments, group).await
}

async fn current_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> MetadataState {
    load_manifest_state_for_inspection(
        store,
        namespace_id,
        crate::test_support::namespace_store(store, namespace_id)
            .load_current_manifest()
            .await
            .expect("read manifest")
            .manifest()
            .manifest_no,
    )
    .await
    .expect("materialize the manifest")
    .metadata_state
}

/// Copies a local store's whole object tree, so two jobs can start from
/// byte-identical durable state. Content ids and segment ids are generated, so
/// building the same namespace twice would not produce the same rows.
fn copy_store_tree(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).expect("read the store directory") {
        let entry = entry.expect("directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("entry file type").is_dir() {
            std::fs::create_dir_all(&target).expect("create the copied directory");
            copy_store_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy the object");
        }
    }
}

async fn orphan_segment_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    use futures::StreamExt;
    let referenced = referenced_segment_keys(store, namespace_id).await;
    store
        .list_prefix_stream(&metadata_segment_prefix(namespace_id))
        .map(|key| key.expect("list segments"))
        .filter(|key| std::future::ready(!referenced.contains(key)))
        .collect()
        .await
}

/// Compacts the group's oldest window of at most eight runs into its base
/// run, through the same run, finalization, and publication steps as a unit.
async fn compact_group_whole<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) {
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let seed = current.payload().next_run_no;
    compact_with_seed(
        store,
        namespace_id,
        &whole_group_spec(&current, group),
        seed,
    )
    .await;
}

/// Checks that a planned unit judges rows against the manifest's retention
/// floor.
#[tokio::test]
async fn the_planner_judges_rows_against_the_manifests_retention_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    // A one-byte buffer changes nothing about the plan.
    for policy in [compact_everything_policy(), starving_policy()] {
        let spec = plan_group(
            segments.manifest().payload(),
            &segments.scan_runs,
            group,
            policy,
        )
        .expect("plan the group");
        assert_eq!(spec.group(), group);
        assert_eq!(spec.frozen_floor_seq(), floor_seq);
    }
}

/// Checks that a whole unit and the plan, rewrite, and finalization run apart
/// reach the same rows, drop only rows the floor covers, and leave the group
/// in one base run.
#[tokio::test]
async fn a_unit_and_its_split_pieces_reach_the_same_rows() {
    let job_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(job_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let frozen_floor_seq = read_floor_seq(&store, &namespace_id).await;

    // The same durable bytes, compacted the ordinary way.
    let whole_dir = tempdir().expect("tempdir");
    copy_store_tree(job_dir.path(), whole_dir.path());
    let whole_store = LocalFsStore::new(whole_dir.path()).expect("store");
    compact_group_whole(&whole_store, &namespace_id, group).await;
    let whole = group_rows_of_current_manifest(&whole_store, &namespace_id, group).await;
    let whole_state = current_metadata_state(&whole_store, &namespace_id).await;

    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let input_keys = input_keys_now(&store, &namespace_id, &spec).await;
    let outcome = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MaintenanceCancellation::default(),
    )
    .await;
    let Ok(result) = outcome else {
        panic!("nothing cancelled this job");
    };
    assert!(
        result.output_segments.len() > 1,
        "the segment budget must roll several output segments, got {}",
        result.output_segments.len()
    );
    assert!(
        result.output_segments.iter().all(|descriptor| {
            metadata_segment_object_key(descriptor)
                .starts_with(&metadata_segment_prefix(&namespace_id))
        }),
        "every output segment is written to the segment directory"
    );
    publish_streaming_compaction(&store, &namespace_id, &spec, &input_keys, &result).await;
    let compacted = group_rows_of_current_manifest(&store, &namespace_id, group).await;

    assert_eq!(
        compacted, whole,
        "a unit and its split pieces must keep the same rows"
    );
    assert!(
        metadata_states_equivalent(
            &current_metadata_state(&store, &namespace_id).await,
            &whole_state
        ),
        "and must materialize the same namespace"
    );
    // The format gives every binding event exactly one reverse row, and the
    // slot index's summary rows have none. The two families are decided in
    // different passes here, so this is what says they stayed in lockstep.
    let binding_events = compacted[&ApiMetadataRowFamily::DirentryBinds]
        .iter()
        .filter(|row| {
            matches!(row, MetadataRow::DirentryBinding(binding) if binding.is_binding_event())
        })
        .count();
    assert_eq!(
        binding_events,
        compacted[&ApiMetadataRowFamily::DirentryChildBinds].len(),
    );
    assert_eq!(
        result.rows_written,
        compacted.values().map(Vec::len).sum::<usize>() as u64,
        "the job's row count must be what the manifest now holds"
    );
    // The loader's invariants must accept the result, and the group must be
    // left in one base run.
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let runs = input_runs_for_group(segments.manifest(), group);
    assert_eq!(runs.len(), 1, "the job must leave the group in one run");
    assert_eq!(runs[0].tier, RunTier::Base);

    // Teeth: the rebuilds must actually have dropped something, or the
    // comparison above is comparing two copies of the input.
    let rows_of = |rows: &BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>>| {
        rows.values().map(Vec::len).sum::<usize>()
    };
    assert!(
        rows_of(&compacted) < rows_of(&before),
        "the rebuild must drop rows for this comparison to mean anything: {} before, {} after",
        rows_of(&before),
        rows_of(&compacted)
    );
    // What a rebuild may drop is bounded from both sides: it never invents a
    // row, and it never touches one the retention floor still covers.
    for (family, rows) in &compacted {
        let input: BTreeSet<String> = before[family]
            .iter()
            .map(|row| row.row_key_for_family(*family))
            .collect();
        for row in rows {
            assert!(
                input.contains(&row.row_key_for_family(*family)),
                "the job wrote a {family:?} row its input did not hold"
            );
        }
    }
    for (family, rows) in &before {
        let survivors: BTreeSet<String> = compacted[family]
            .iter()
            .map(|row| row.row_key_for_family(*family))
            .collect();
        for row in rows {
            if manifest_row_commit_seq(row) > frozen_floor_seq {
                assert!(
                    survivors.contains(&row.row_key_for_family(*family)),
                    "the job dropped a {family:?} row above the retention floor"
                );
            }
        }
    }
}

/// Checks that a base compaction prunes only at the floor of the manifest
/// that it planned from, when a retention advance raises the floor before
/// the compaction publishes: the rows that it publishes are the rows that
/// the same compaction publishes without the advance, the manifest keeps the
/// advance's floor, and a compaction planned at that floor drops more rows.
#[tokio::test]
async fn a_base_compaction_prunes_only_at_its_manifests_floor() {
    let job_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(job_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let planned_floor = read_floor_seq(&store, &namespace_id).await;
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    assert_eq!(spec.frozen_floor_seq(), planned_floor);
    let input_keys = input_keys_now(&store, &namespace_id, &spec).await;
    let Ok(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        compact_everything_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };

    // The same durable bytes and output, published without the advance.
    let without_dir = tempdir().expect("tempdir");
    copy_store_tree(job_dir.path(), without_dir.path());
    let without = LocalFsStore::new(without_dir.path()).expect("store");
    publish_streaming_compaction(&without, &namespace_id, &spec, &input_keys, &result).await;
    let pruned_at_the_plan = group_rows_of_current_manifest(&without, &namespace_id, group).await;

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(test_context().now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance the floor past the late deletion");
    let raised_floor = read_floor_seq(&store, &namespace_id).await;
    assert!(raised_floor > planned_floor, "the advance raises the floor");
    publish_streaming_compaction(&store, &namespace_id, &spec, &input_keys, &result).await;
    assert_eq!(read_floor_seq(&store, &namespace_id).await, raised_floor);
    let published = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    assert_eq!(
        published, pruned_at_the_plan,
        "the compaction prunes at the floor of the manifest it planned from"
    );

    // Teeth: a compaction planned at the raised floor drops more rows, so the
    // comparison above would see a prune at that floor.
    compact_group_whole(&store, &namespace_id, group).await;
    let pruned_at_the_raised_floor =
        group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let rows_of = |rows: &BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>>| {
        rows.values().map(Vec::len).sum::<usize>()
    };
    assert!(
        rows_of(&pruned_at_the_raised_floor) < rows_of(&published),
        "{} rows at the raised floor, {} at the planned floor",
        rows_of(&pruned_at_the_raised_floor),
        rows_of(&published)
    );
}

#[tokio::test]
async fn compaction_keeps_retained_revisions_and_their_layouts() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    // Below the floor: three revisions of a file that stays and of one that is
    // deleted.
    for body in ["one", "two", "three"] {
        for path in ["/live.txt", "/deleted.txt"] {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                .write_file_bytes(path, body.as_bytes(), None)
                .await
                .expect("write a revision");
        }
        create_snapshot(&store, &namespace_id, &context)
            .await
            .expect("snapshot the revisions");
    }
    let live = crate::test_support::visible_inode_id(&store, &namespace_id, "/live.txt").await;
    let deleted =
        crate::test_support::visible_inode_id(&store, &namespace_id, "/deleted.txt").await;
    let deletion_seq = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/deleted.txt", None)
        .await
        .expect("delete a file")
        .committed_seq;
    create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("snapshot the deletion");
    // The floor stops just below the deletion, so the deletion stays
    // in the trash.
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Seq(CommitSeq(deletion_seq.0 - 1)))
    .await
    .expect("advance the floor to the last revision");
    // Above the floor: two more revisions of the file that stays.
    for body in ["four", "five"] {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes("/live.txt", body.as_bytes(), None)
            .await
            .expect("write a revision above the floor");
        create_snapshot(&store, &namespace_id, &context)
            .await
            .expect("snapshot the revision");
    }

    for group in [
        MetadataFamilyGroup::Revisions,
        MetadataFamilyGroup::ContentLayouts,
    ] {
        let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
        let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
        let input_keys = input_keys_now(&store, &namespace_id, &spec).await;
        let Ok(result) = run_compaction(
            &store,
            &namespace_id,
            &spec,
            small_segment_policy(),
            &MaintenanceCancellation::default(),
        )
        .await
        else {
            panic!("nothing cancelled this job");
        };
        publish_streaming_compaction(&store, &namespace_id, &spec, &input_keys, &result).await;
        let after = group_rows_of_current_manifest(&store, &namespace_id, group).await;

        if group == MetadataFamilyGroup::ContentLayouts {
            let revisions = group_rows_of_current_manifest(
                &store,
                &namespace_id,
                MetadataFamilyGroup::Revisions,
            )
            .await;
            for revision in &revisions[&ApiMetadataRowFamily::Revisions] {
                let MetadataRow::FileRevision(revision) = revision else {
                    panic!("revision")
                };
                assert!(after[&ApiMetadataRowFamily::ContentLayouts].iter().any(|row| matches!(
                    row, MetadataRow::ContentLayout(layout) if layout.content_id == revision.content_ref.content_id
                )));
            }
            assert!(result.rows_written <= result.rows_read);
            continue;
        }
        let kept: Vec<(InodeId, u64)> = after[&ApiMetadataRowFamily::Revisions]
            .iter()
            .map(|row| match row {
                MetadataRow::FileRevision(revision) => (revision.inode_id, revision.revision_no.0),
                other => panic!("the revisions family holds a foreign row: {other:?}"),
            })
            .collect();
        // Row keys order an inode's revisions newest first, and the inodes by
        // ID.
        let mut expected = vec![(live, 5), (live, 4), (live, 3), (deleted, 3)];
        expected
            .sort_by_key(|(inode_id, revision_no)| (*inode_id, std::cmp::Reverse(*revision_no)));
        assert_eq!(
            kept, expected,
            "each file keeps the revisions above the floor and its newest at or below it, \
             and the file in the trash keeps its newest"
        );
        assert!(
            after[&ApiMetadataRowFamily::Revisions]
                .iter()
                .all(|row| before[&ApiMetadataRowFamily::Revisions].contains(row)),
            "kept rows are input rows, unchanged"
        );
        assert_eq!((result.rows_read, result.rows_written), (8, 4));
    }
}

/// The object keys the current manifest holds for `group`.
async fn group_segment_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) -> BTreeSet<String> {
    let segments = load_current_manifest_segments(store, namespace_id).await;
    input_runs_for_group(segments.manifest(), group)
        .iter()
        .flat_map(|run| group_run_descriptors(run, group))
        .map(metadata_segment_object_key)
        .collect()
}

/// Rewrites the group's base-run segment for `family` with `rows`, updating
/// the descriptor in `manifest` to match.
async fn rewrite_base_segment(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    manifest: &mut NamespaceManifestEnvelope,
    family: ApiMetadataRowFamily,
    mut rows: Vec<MetadataRow>,
) {
    let mut payload = manifest.payload().clone();
    rows.sort_by_key(|row| row.row_key_for_family(family));
    let head_seq = payload.head_seq;
    let descriptor = payload
        .runs
        .iter_mut()
        .filter(|run| run.tier == RunTier::Base)
        .flat_map(|run| &mut run.segments)
        .find(|descriptor| descriptor.family == family)
        .expect("the compacted base run holds this family");
    super::index_parity::rewrite_manifest_segment(
        store,
        namespace_id,
        head_seq,
        family,
        descriptor,
        rows,
        DEFAULT_TARGET_BLOCK_BYTES,
    )
    .await;
    *manifest = encode_namespace_manifest_json(payload)
        .expect("encode changed manifest")
        .into_envelope();
}

#[tokio::test]
async fn a_repeated_revision_row_key_is_refused() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Revisions;

    // Compact everything into one base run, then repeat one revision.
    drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..compact_everything_policy()
        },
    )
    .await;
    let mut manifest = load_current_manifest_segments(&store, &namespace_id)
        .await
        .manifest()
        .clone();
    let rows = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let repeated = rows[&ApiMetadataRowFamily::Revisions]
        .first()
        .expect("the namespace has revisions")
        .clone();
    let duplicated_key = repeated.row_key_for_family(ApiMetadataRowFamily::Revisions);
    let family = ApiMetadataRowFamily::Revisions;
    let mut family_rows = rows[&family].clone();
    family_rows.push(repeated);
    rewrite_base_segment(&store, &namespace_id, &mut manifest, family, family_rows).await;
    super::index_parity::overwrite_manifest(&store, &namespace_id, manifest).await;
    // One delta run above the doctored base, so a bottom-anchored window makes
    // progress and the compaction reads the base.
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/late.txt", b"late\n", None)
        .await
        .expect("write a late file");
    create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("snapshot the late write");

    // Loading descriptors does not read the duplicate rows. The compaction must
    // detect them before publishing its output.
    let keys_before = group_segment_keys(&store, &namespace_id, group).await;
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let job_error = run_metadata_compaction(
        &segments,
        &crate::cache::MetadataSegmentCache::unshared(0),
        &namespace_id,
        &spec,
        compact_everything_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    .expect_err("a compaction must refuse the duplicate");
    assert_duplicate_row_key(&job_error, &duplicated_key);
    drop(segments);

    assert_eq!(
        group_segment_keys(&store, &namespace_id, group).await,
        keys_before,
        "no run built from a duplicated key may be published"
    );
}

fn assert_duplicate_row_key(error: &CoreError, duplicated_key: &str) {
    let CoreError::NamespaceCorrupt(message) = error else {
        panic!("a duplicate row key is namespace corruption, got {error:?}");
    };
    assert!(
        message.contains("Revisions") && message.contains(duplicated_key),
        "the error must name the family and the duplicated key, got: {message}"
    );
}

/// What one compaction cost the store, by section.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ReadProfile {
    filter_reads: usize,
    index_reads: usize,
    data_reads: usize,
    other_reads: usize,
    stored_bytes: u64,
}

impl ReadProfile {
    fn requests(&self) -> usize {
        self.filter_reads + self.index_reads + self.data_reads + self.other_reads
    }
}

/// Classifies a read log against the segment descriptors it touched. A read
/// at a segment's filter offset is a filter read, one at its index offset is
/// an index read, and anything else is data.
fn classify_reads(
    operations: &[RecordedOperation],
    descriptors: &[MetadataSegmentRef],
) -> ReadProfile {
    let mut profile = ReadProfile::default();
    let reads = operations.iter().filter_map(|operation| match operation {
        RecordedOperation::Get {
            key,
            range,
            result_bytes,
        } if *result_bytes > 0 => Some((
            key,
            range.as_ref().map_or(0, |range| range.start_inclusive),
            *result_bytes as u64,
        )),
        RecordedOperation::GetWithMetadata { key, result_bytes } if *result_bytes > 0 => {
            Some((key, 0, *result_bytes as u64))
        }
        _ => None,
    });
    for (key, start, bytes) in reads {
        profile.stored_bytes += bytes;
        let Some(descriptor) = descriptors
            .iter()
            .find(|descriptor| metadata_segment_object_key(descriptor) == *key)
        else {
            profile.other_reads += 1;
            continue;
        };
        if start == descriptor.filter_block.offset {
            profile.filter_reads += 1;
        } else if start == descriptor.index_block.offset {
            profile.index_reads += 1;
        } else {
            profile.data_reads += 1;
        }
    }
    profile
}

async fn seed_binding_history(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    parents: u64,
    rounds: u64,
) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context)
        .await
        .expect("bootstrap");
    for round in 0..rounds {
        for parent in 0..parents {
            crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
                .write_file_bytes(&format!("/d{parent}/f{round}.txt"), b"body\n", None)
                .await
                .expect("write file");
        }
        // The WAL tail is capped, so each round is captured in a snapshot as it is made.
        create_snapshot(store, namespace_id, &context)
            .await
            .expect("snapshot the writes");
    }
    for round in 0..rounds {
        for parent in 0..parents {
            crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
                .delete_path(&format!("/d{parent}/f{round}.txt"), None)
                .await
                .expect("delete file");
        }
        create_snapshot(store, namespace_id, &context)
            .await
            .expect("snapshot the deletions");
    }
    drain_compaction(
        store,
        namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy {
                max_rows_per_segment: NonZeroUsize::new(16).expect("nonzero"),
                ..compact_everything_policy()
            }
        },
    )
    .await;
    crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance the floor past the deletions");
    crate::test_support::NamespaceFixture::new(store, namespace_id, &context)
        .write_file_bytes("/late.txt", b"late\n", None)
        .await
        .expect("write a late file");
    create_snapshot(store, namespace_id, &context)
        .await
        .expect("snapshot the late write");
}

async fn install_synthetic_bindings_base(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    count: u64,
    rows_per_segment: NonZeroUsize,
) {
    assert!(
        count.is_power_of_two(),
        "the scrambling multiplier is only a permutation over a power of two"
    );
    let mut binds = Vec::with_capacity(loonfs_types::usize_from_u64(count));
    let mut unbinds = Vec::with_capacity(loonfs_types::usize_from_u64(count));
    for index in 0..count {
        let parent = InodeId(1_000);
        // Odd multiplier, power-of-two modulus: a bijection, so every name is
        // distinct and the name order is scattered against the child order.
        let scrambled = index.wrapping_mul(2_654_435_761) % count;
        let name = format!("g{scrambled:012}");
        let delta = u32::try_from(index).expect("test row counts fit in u32");
        binds.push(MetadataRow::DirentryBinding(
            crate::metadata::DirentryBindingRecord {
                parent_inode_id: parent,
                name_key: NameKey::parse(&name).expect("name key"),
                state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                    display_name: loonfs_types::DisplayName::parse(&name).expect("display name"),
                    revision: Some(crate::test_support::revision_summary()),
                },
                child_inode_id: InodeId(u128::from(100_000 + index)),
                child_kind: loonfs_types::InodeKind::File,
                child_created_by: loonfs_types::ActorId::loonfs(),
                child_created_at_ms: 4_200,
                committed_seq: CommitSeq(1),
                delta_index: delta,
                commit_id: crate::test_support::binding_commit_id(),
                committed_by: loonfs_types::ActorId::loonfs(),
                committed_at_ms: 0,
                bind_position: loonfs_types::format::manifest::DeltaPosition {
                    seq: CommitSeq(1),
                    delta_index: delta,
                },
            },
        ));
        unbinds.push(MetadataRow::DirentryBinding(
            crate::metadata::DirentryBindingRecord {
                parent_inode_id: parent,
                name_key: NameKey::parse(&name).expect("name key"),
                child_inode_id: InodeId(u128::from(100_000 + index)),
                child_kind: loonfs_types::InodeKind::File,
                child_created_by: loonfs_types::ActorId::loonfs(),
                child_created_at_ms: 4_200,
                committed_seq: CommitSeq(2),
                delta_index: delta,
                commit_id: crate::test_support::binding_commit_id(),
                committed_by: loonfs_types::ActorId::loonfs(),
                committed_at_ms: 0,
                bind_position: loonfs_types::format::manifest::DeltaPosition {
                    seq: CommitSeq(2),
                    delta_index: delta,
                },
                state: loonfs_types::format::manifest::DirentryBindingState::Unbound,
            },
        ));
    }

    let manifest = load_current_manifest_segments(store, namespace_id)
        .await
        .manifest()
        .clone();
    // The synthetic segments replace this run's bindings, so they join the
    // run that already holds the group's other families.
    let base_run = manifest
        .payload()
        .runs
        .iter()
        .find(|run| {
            run.tier == RunTier::Base
                && run.segments.iter().any(|descriptor| {
                    MetadataFamilyGroup::Bindings
                        .families()
                        .contains(&descriptor.family)
                })
        })
        .expect("the namespace has been compacted into a base run");
    let base_run_no = base_run.run_no;
    let mut rows_by_family = BTreeMap::from([
        (
            ApiMetadataRowFamily::DirentryBinds,
            [binds.clone(), unbinds.clone()].concat(),
        ),
        (
            ApiMetadataRowFamily::DirentryChildBinds,
            [binds, unbinds].concat(),
        ),
    ]);
    let run_segments = crate::NamespaceStore {
        run_policy: MetadataRunPolicy {
            max_rows_per_segment: rows_per_segment,
            ..MetadataRunPolicy::default()
        },
        ..crate::test_support::namespace_store(store, namespace_id)
    }
    .build_manifest_segments_from_rows(|family| {
        let mut rows = rows_by_family.remove(&family).unwrap_or_default();
        rows.sort_by_key(|row| row.row_key_for_family(family));
        rows
    })
    .await
    .expect("build the synthetic bindings run");

    // Only the base tier is replaced: the delta run above it is what gives a
    // bottom-anchored window something to compact.
    let mut payload = manifest.payload().clone();
    let base_run = payload
        .runs
        .iter_mut()
        .find(|run| run.run_no == base_run_no)
        .expect("the base run should still exist");
    base_run.segments.retain(|descriptor| {
        !MetadataFamilyGroup::Bindings
            .families()
            .contains(&descriptor.family)
    });
    base_run
        .segments
        .extend(flatten_manifest_segments(run_segments));
    super::index_parity::overwrite_manifest(
        store,
        namespace_id,
        encode_namespace_manifest_json(payload)
            .expect("encode changed manifest")
            .into_envelope(),
    )
    .await;
}

/// Checks that a compaction reads each input segment's index once, and that its
/// requests follow the window's segments, not its rows.
#[tokio::test]
async fn a_compaction_reads_its_window_once() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_binding_history(&inner, &namespace_id, 4, 2).await;
    let slots = 4_096;
    install_synthetic_bindings_base(
        &inner,
        &namespace_id,
        slots,
        NonZeroUsize::new(512).expect("nonzero"),
    )
    .await;

    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let group = MetadataFamilyGroup::Bindings;
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let descriptors: Vec<MetadataSegmentRef> = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| run.segments.iter().cloned())
        .collect();
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let window_segments = segments
        .scan_runs
        .iter()
        .filter(|run| spec.inputs().contains(&run.run_no))
        .flat_map(|run| group_run_descriptors(run, group))
        .count();
    drop(segments);
    store.reset();

    let Ok(compacted) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        compact_everything_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this compaction");
    };
    let profile = classify_reads(&store.take(), &descriptors);

    // One index read and one span of data reads per segment. Data blocks are
    // fetched two at a time, so a segment costs at most its block count. The
    // bound that matters is that neither follows the row count.
    assert!(
        profile.index_reads <= window_segments,
        "the compaction read {} indexes for {window_segments} segments",
        profile.index_reads
    );
    assert!(
        (profile.requests() as u64) < compacted.rows_read / 8,
        "total work must follow the window's segments, not its rows: {} requests for {} rows",
        profile.requests(),
        compacted.rows_read
    );
}

/// Cancels the job by setting a token once the store has served a given
/// number of reads, so a cancellation happens in the middle of a compaction rather
/// than at a boundary the test picked.
#[derive(Debug)]
struct CancelAfterReadsStore {
    inner: LocalFsStore,
    cancellation: MaintenanceCancellation,
    reads_before_cancel: usize,
    reads: AtomicUsize,
}

#[async_trait]
impl ObjectStore for CancelAfterReadsStore {
    loonfs_test_support::delegate_object_store!(self => self.inner; except get);

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        if self.reads.fetch_add(1, Ordering::SeqCst) + 1 >= self.reads_before_cancel {
            self.cancellation.cancel();
        }
        self.inner.get(key, range).await
    }
}

#[tokio::test]
async fn a_cancelled_compaction_leaves_orphans_and_the_rerun_ends_where_it_would_have() {
    let straight_dir = tempdir().expect("tempdir");
    let straight_store = LocalFsStore::new(straight_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&straight_store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;

    let interrupted_dir = tempdir().expect("tempdir");
    copy_store_tree(straight_dir.path(), interrupted_dir.path());

    // The uninterrupted run, for the comparison.
    let spec = compaction_spec_for_group(&straight_store, &namespace_id, group).await;
    let input_keys = input_keys_now(&straight_store, &namespace_id, &spec).await;
    let Ok(straight_result) = run_compaction(
        &straight_store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };
    publish_streaming_compaction(
        &straight_store,
        &namespace_id,
        &spec,
        &input_keys,
        &straight_result,
    )
    .await;
    let straight_rows = group_rows_of_current_manifest(&straight_store, &namespace_id, group).await;

    let reader_answers_before = group_rows_of_current_manifest(
        &LocalFsStore::new(interrupted_dir.path()).expect("store"),
        &namespace_id,
        group,
    )
    .await;
    let manifest_before = current_manifest_number(
        &LocalFsStore::new(interrupted_dir.path()).expect("store"),
        &namespace_id,
    )
    .await;
    let store = LocalFsStore::new(interrupted_dir.path()).expect("store");
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;

    let mut orphans = BTreeSet::new();
    let mut cancelled_attempts = 0;
    for reads_before_cancel in [4usize, 12, 40] {
        let cancellation = MaintenanceCancellation::default();
        let store = CancelAfterReadsStore {
            inner: LocalFsStore::new(interrupted_dir.path()).expect("store"),
            cancellation: cancellation.clone(),
            reads_before_cancel,
            reads: AtomicUsize::new(0),
        };
        let outcome = run_compaction(
            &store,
            &namespace_id,
            &spec,
            small_segment_policy(),
            &cancellation,
        )
        .await;
        if matches!(outcome, Err(CompactionUnitOutcome::Cancelled)) {
            cancelled_attempts += 1;
        }
        assert_eq!(
            current_manifest_number(&store, &namespace_id).await,
            manifest_before,
            "a cancelled attempt publishes nothing"
        );
        assert_eq!(
            group_rows_of_current_manifest(&store, &namespace_id, group).await,
            reader_answers_before,
            "and what a reader sees never moves"
        );
        orphans.extend(orphan_segment_keys(&store, &namespace_id).await);
    }
    assert!(
        cancelled_attempts > 0,
        "at least one attempt must have been cancelled mid-job"
    );
    assert!(
        !orphans.is_empty(),
        "a cancelled attempt leaves the segments it had written staged"
    );

    // The re-run from the same spec, against the same durable state.
    let store = LocalFsStore::new(interrupted_dir.path()).expect("store");
    let input_keys = input_keys_now(&store, &namespace_id, &spec).await;
    let Ok(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled the re-run");
    };
    assert_eq!(
        (result.rows_read, result.rows_written),
        (straight_result.rows_read, straight_result.rows_written)
    );
    let referenced: BTreeSet<String> = result
        .output_segments
        .iter()
        .map(metadata_segment_object_key)
        .collect();
    publish_streaming_compaction(&store, &namespace_id, &spec, &input_keys, &result).await;

    assert_eq!(
        group_rows_of_current_manifest(&store, &namespace_id, group).await,
        straight_rows,
        "a re-run must keep the rows an uninterrupted job kept"
    );
    // The cancelled attempts' segments are still there and nothing names
    // them: they are orphans for the collector, not state anything reads.
    let staged_now = orphan_segment_keys(&store, &namespace_id).await;
    for orphan in &orphans {
        assert!(staged_now.contains(orphan));
        assert!(
            !referenced.contains(orphan),
            "a cancelled attempt's segment must not be referenced by the published run"
        );
    }
    let manifest_keys: BTreeSet<String> = load_current_manifest_segments(&store, &namespace_id)
        .await
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect();
    assert!(
        orphans.iter().all(|orphan| !manifest_keys.contains(orphan)),
        "no manifest names a cancelled attempt's segment"
    );
}

/// Builds `run_count` runs of `rows_per_run` attribute rows each, with a
/// 4 KiB value of random characters in every row, and returns the runs with
/// every row in row-key order.
async fn attribute_compaction_inputs(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    run_count: u64,
    rows_per_run: u64,
) -> (Vec<MetadataRunManifest>, Vec<MetadataRow>) {
    use loonfs_types::format::manifest::AttributesRevisionRecord;
    use loonfs_types::{AttributeKey, AttributeValue, Attributes, AttributesRevisionNo};

    let mut runs = Vec::new();
    let mut sequential = Vec::new();
    let mut value_state = 1u64;
    for run in 0..run_count {
        let mut rows = Vec::new();
        for index in 0..rows_per_run {
            let value: String = (0..4_096)
                .map(|_| {
                    value_state ^= value_state << 13;
                    value_state ^= value_state >> 7;
                    value_state ^= value_state << 17;
                    char::from(b'!' + (value_state % 90) as u8)
                })
                .collect();
            rows.push(MetadataRow::AttributesRevision(AttributesRevisionRecord {
                inode_id: InodeId(u128::from(index * 3 + run + 1)),
                attributes_revision_no: AttributesRevisionNo(1),
                committed_seq: CommitSeq(run + 1),
                commit_id: CommitId::parse(format!("c_{run}")).expect("commit id"),
                delta_index: 0,
                committed_by: loonfs_types::ActorId::loonfs(),
                committed_at_ms: 0,
                attributes: Attributes::new(BTreeMap::from([(
                    AttributeKey::parse("value").expect("key"),
                    AttributeValue::parse(value).expect("value"),
                )]))
                .expect("attributes"),
            }));
        }
        sequential.extend(rows.iter().cloned());
        let mut rows = Some(rows);
        let segments = crate::NamespaceStore {
            run_policy: MetadataRunPolicy {
                max_rows_per_segment: NonZeroUsize::new(1_200).expect("nonzero"),
                ..MetadataRunPolicy::default()
            },
            ..crate::test_support::namespace_store(store, namespace_id)
        }
        .build_manifest_segments_from_rows(|family| {
            if family == ApiMetadataRowFamily::Attributes {
                rows.take().expect("one attributes family")
            } else {
                Vec::new()
            }
        })
        .await
        .expect("input segments");
        runs.push(MetadataRunManifest {
            run_no: RunNo(run + 1),
            run_seq: CommitSeq(run + 1),
            tier: if run == 0 {
                RunTier::Base
            } else {
                RunTier::Delta
            },
            segments,
        });
    }
    sequential.sort_by_key(|row| row.row_key_for_family(ApiMetadataRowFamily::Attributes));
    (runs, sequential)
}

#[tokio::test]
async fn a_compaction_reads_byte_sized_spans_concurrently_within_its_configured_decoded_input() {
    use crate::compaction::run_iterator::{
        refill_iterators, MetadataSegmentBlockLoader, MetadataSegmentRowIterator,
        ITERATOR_FETCH_TARGET_BYTES,
    };

    let directory = tempdir().expect("directory");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("byte-compaction").expect("namespace");
    let group = MetadataFamilyGroup::Attributes;
    let (runs, sequential) = attribute_compaction_inputs(&local, &namespace_id, 3, 2_400).await;
    let mut indexes = BTreeMap::new();
    let mut expected_gets = 0;
    let mut old_gets = 0;
    for descriptor in runs
        .iter()
        .flat_map(|run| group_run_descriptors(run, group))
    {
        let (index, _) = index_block_load::load_segment_index_for_compaction(&local, descriptor)
            .await
            .expect("index");
        // Every refill of the segment asks for blocks until their stored
        // bytes reach the fetch target, and fetches them in one GET.
        let mut refill_bytes = 0;
        for entry in index.iter() {
            refill_bytes += entry.block.stored_bytes as usize;
            if refill_bytes >= ITERATOR_FETCH_TARGET_BYTES {
                expected_gets += 1;
                refill_bytes = 0;
            }
        }
        expected_gets += usize::from(refill_bytes > 0);
        old_gets += index.len().div_ceil(2);
        indexes.insert(metadata_segment_object_key(descriptor), index);
    }
    let store = ConcurrencyWatchStore::new(
        RecordingStore::metadata_segments(local),
        KeyPredicate::metadata_segment(),
    );
    let cache = MetadataSegmentCache::unshared(8 * 1024 * 1024);
    let segments = VerifiedMetadataSegments::from_runs(&store, &cache, runs.clone());
    let spec = MetadataCompactionSpec::new(
        group,
        runs.iter().map(|run| run.run_no).collect(),
        runs.iter()
            .flat_map(|run| group_run_descriptors(run, group))
            .map(|descriptor| descriptor.row_count)
            .sum(),
        // A delta compaction: these attribute rows have no inodes or bindings,
        // so a base compaction's kept inode filter would remove every one.
        plan::CompactionPlacement::Delta {
            output_seq: CommitSeq(3),
        },
        CommitSeq(0),
    );
    let Ok(result) = run_metadata_compaction(
        &segments,
        &crate::cache::MetadataSegmentCache::unshared(0),
        &namespace_id,
        &spec,
        MetadataRunPolicy::default(),
        &MaintenanceCancellation::default(),
    )
    .await
    .expect("compact") else {
        panic!("nothing cancelled this compaction");
    };
    let gets = store.inner().take_gets();
    let data_gets = gets
        .iter()
        .filter(|(key, range)| {
            range.is_some_and(|(start, _)| {
                indexes[key].iter().any(|entry| entry.block.offset == start)
            })
        })
        .count();
    assert_eq!(data_gets, expected_gets);
    assert!(data_gets * 8 < old_gets);
    assert!((2..=STORE_READS_IN_FLIGHT).contains(&store.reads().peak_in_flight));

    let local = store.inner().inner();
    let mut actual = Vec::new();
    for descriptor in &result.output_segments {
        let (index, _) = index_block_load::load_segment_index_for_compaction(local, descriptor)
            .await
            .expect("output index");
        let blocks =
            data_block_load::load_segment_data_block_span(local, None, None, descriptor, &index)
                .await
                .expect("output blocks");
        for block in blocks {
            actual.extend(block.rows.iter().cloned());
        }
    }
    assert_eq!(actual, sequential);

    let descriptor = group_run_descriptors(&runs[0], group)
        .next()
        .expect("input")
        .clone();
    let mut iterators: Vec<_> = (0..64)
        .map(|_| {
            MetadataSegmentRowIterator::metadata(
                ApiMetadataRowFamily::Attributes,
                CommitSeq(3),
                vec![descriptor.clone()],
            )
        })
        .collect();
    let max_decoded_input_bytes = 32 * 1024 * 1024;
    refill_iterators(
        &MetadataSegmentBlockLoader::new(&segments),
        &mut iterators,
        max_decoded_input_bytes,
    )
    .await
    .expect("wide refill");
    let mut decoded_bytes = 0;
    let mut data_gets = 0;
    for (key, range) in store.inner().take_gets() {
        if let Some((start, end)) = range {
            let entries: Vec<_> = indexes[&key]
                .iter()
                .filter(|entry| entry.block.offset >= start && entry.block.offset < end)
                .collect();
            if !entries.is_empty() {
                data_gets += 1;
                decoded_bytes += entries
                    .iter()
                    .map(|entry| entry.block.decoded_bytes as usize)
                    .sum::<usize>();
            }
        }
    }
    assert_eq!(data_gets, iterators.len());
    assert!(decoded_bytes <= max_decoded_input_bytes);
    assert!(decoded_bytes > max_decoded_input_bytes / 2);
    assert!(store.reads().peak_in_flight <= STORE_READS_IN_FLIGHT);
}

/// Checks that a compaction reads each input segment of at most 256 KiB in
/// one GET of the whole object, from which it takes both the index and the
/// data blocks.
#[tokio::test]
async fn a_compaction_reads_each_small_input_segment_whole_in_one_get() {
    let directory = tempdir().expect("directory");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("small-segments").expect("namespace");
    let group = MetadataFamilyGroup::Attributes;
    let (runs, sequential) = attribute_compaction_inputs(&local, &namespace_id, 3, 20).await;
    let object_bytes: BTreeMap<String, u64> = runs
        .iter()
        .flat_map(|run| group_run_descriptors(run, group))
        .map(|descriptor| {
            (
                metadata_segment_object_key(descriptor),
                index_block_load::segment_object_bytes(descriptor),
            )
        })
        .collect();
    assert_eq!(object_bytes.len(), runs.len());
    assert!(object_bytes.values().all(|&bytes| bytes <= 256 * 1024));
    let store = RecordingStore::metadata_segments(local);
    let cache = MetadataSegmentCache::unshared(8 * 1024 * 1024);
    let segments = VerifiedMetadataSegments::from_runs(&store, &cache, runs.clone());
    let spec = MetadataCompactionSpec::new(
        group,
        runs.iter().map(|run| run.run_no).collect(),
        sequential.len() as u64,
        plan::CompactionPlacement::Delta {
            output_seq: CommitSeq(3),
        },
        CommitSeq(0),
    );
    let Ok(result) = run_metadata_compaction(
        &segments,
        &crate::cache::MetadataSegmentCache::unshared(0),
        &namespace_id,
        &spec,
        MetadataRunPolicy::default(),
        &MaintenanceCancellation::default(),
    )
    .await
    .expect("compact") else {
        panic!("nothing cancelled this compaction");
    };
    assert_eq!(result.rows_read, sequential.len() as u64);

    let gets = store.take_gets();
    assert_eq!(gets.len(), object_bytes.len(), "one GET per input segment");
    for (key, range) in &gets {
        assert_eq!(
            *range,
            Some((0, object_bytes[key])),
            "`{key}` is read whole"
        );
    }
}

/// Checks that a compaction reads an input segment larger than 256 KiB in
/// two GETs: the index section, then the data blocks.
#[tokio::test]
async fn a_compaction_reads_a_large_input_segment_index_then_data() {
    let directory = tempdir().expect("directory");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("large-segment").expect("namespace");
    let group = MetadataFamilyGroup::Attributes;
    let (runs, sequential) = attribute_compaction_inputs(&local, &namespace_id, 1, 100).await;
    let descriptors: Vec<_> = group_run_descriptors(&runs[0], group).cloned().collect();
    let [descriptor] = descriptors.as_slice() else {
        panic!("one input segment, not {}", descriptors.len());
    };
    let object_bytes = index_block_load::segment_object_bytes(descriptor);
    assert!(object_bytes > 256 * 1024);
    let (index, object) = index_block_load::load_segment_index_for_compaction(&local, descriptor)
        .await
        .expect("index");
    assert!(object.is_none());
    let store = RecordingStore::metadata_segments(local);
    let cache = MetadataSegmentCache::unshared(8 * 1024 * 1024);
    let segments = VerifiedMetadataSegments::from_runs(&store, &cache, runs.clone());
    let spec = MetadataCompactionSpec::new(
        group,
        vec![runs[0].run_no],
        descriptor.row_count,
        plan::CompactionPlacement::Delta {
            output_seq: CommitSeq(2),
        },
        CommitSeq(0),
    );
    let Ok(result) = run_metadata_compaction(
        &segments,
        &crate::cache::MetadataSegmentCache::unshared(0),
        &namespace_id,
        &spec,
        MetadataRunPolicy::default(),
        &MaintenanceCancellation::default(),
    )
    .await
    .expect("compact") else {
        panic!("nothing cancelled this compaction");
    };
    assert_eq!(result.rows_read, sequential.len() as u64);

    let key = metadata_segment_object_key(descriptor);
    let gets: Vec<_> = store
        .take_gets()
        .into_iter()
        .filter(|(read, _)| *read == key)
        .map(|(_, range)| range)
        .collect();
    let last = &index.last().expect("a data block").block;
    assert_eq!(
        gets,
        [
            Some((descriptor.index_block.offset, object_bytes)),
            Some((
                index[0].block.offset,
                last.offset + u64::from(last.stored_bytes)
            )),
        ]
    );
}

#[tokio::test]
async fn a_compaction_keeps_its_reads_and_its_decoded_blocks_bounded() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = ConcurrencyWatchStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
    );
    seed_bindings_workload(store.inner(), &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let rows_in_group = group_rows_of_current_manifest(&store, &namespace_id, group)
        .await
        .values()
        .map(Vec::len)
        .sum::<usize>();

    let store = ConcurrencyWatchStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
    );
    let Ok(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };

    let reads = store.reads();
    assert!(
        reads.peak_in_flight <= STORE_READS_IN_FLIGHT,
        "the job overlapped {} reads at once",
        reads.peak_in_flight
    );
    assert!(reads.total > 0, "the job must have read the input");
    // Two blocks per iterator, and a bindings cluster opens one iterator per
    // run per forward family. The bound is a property of the job, not of the
    // group: the group here holds far more rows than the blocks ever hold.
    let iterators = spec.inputs().len() * 2;
    assert!(
        result.peak_resident_blocks <= iterators * 2,
        "the job held {} decoded blocks at once against a bound of {}",
        result.peak_resident_blocks,
        iterators * 2
    );
    assert!(
        result.rows_read > (rows_in_group / 2) as u64,
        "this assertion means nothing unless the job read most of the group"
    );
}

#[tokio::test]
async fn one_wide_directory_streams_a_row_at_a_time() {
    let job_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(job_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_one_wide_directory(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;

    let whole_dir = tempdir().expect("tempdir");
    copy_store_tree(job_dir.path(), whole_dir.path());
    let whole_store = LocalFsStore::new(whole_dir.path()).expect("store");
    compact_group_whole(&whole_store, &namespace_id, group).await;
    let whole = group_rows_of_current_manifest(&whole_store, &namespace_id, group).await;
    let whole_state = current_metadata_state(&whole_store, &namespace_id).await;

    // How many rows the widest directory holds, so the test covers a
    // directory that spans several small segments.
    let mut rows_per_parent = BTreeMap::<InodeId, usize>::new();
    for row in &before[&ApiMetadataRowFamily::DirentryBinds] {
        if let MetadataRow::DirentryBinding(binding) = row {
            *rows_per_parent.entry(binding.parent_inode_id).or_default() += 1;
        }
    }
    let widest = rows_per_parent
        .into_values()
        .max()
        .expect("the namespace holds bindings");
    assert!(
        widest > 32,
        "the seed must put many rows under one parent, got {widest}"
    );

    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let input_keys = input_keys_now(&store, &namespace_id, &spec).await;
    let Ok(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };

    publish_streaming_compaction(&store, &namespace_id, &spec, &input_keys, &result).await;
    assert_eq!(
        group_rows_of_current_manifest(&store, &namespace_id, group).await,
        whole,
        "a job that never held the directory must still reach the whole-group compaction's rows"
    );
    assert!(
        metadata_states_equivalent(
            &current_metadata_state(&store, &namespace_id).await,
            &whole_state
        ),
        "and must answer reads the way a whole-group compaction answers them"
    );
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    assert_eq!(
        input_runs_for_group(segments.manifest(), group).len(),
        1,
        "the job must leave the group in one run"
    );
}

/// Every object key the current manifest references.
async fn referenced_segment_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    load_current_manifest_segments(store, namespace_id)
        .await
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect()
}

#[tokio::test]
async fn a_cancellation_after_the_last_row_publishes_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let input_keys = input_keys_now(&store, &namespace_id, &spec).await;
    let cancellation = MaintenanceCancellation::default();
    let Ok(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &cancellation,
    )
    .await
    else {
        panic!("nothing cancelled this job while it read");
    };
    let manifest_before = current_manifest_number(&store, &namespace_id).await;
    let visible_before = visible_namespace(&store, &namespace_id).await;

    cancellation.cancel();
    let finalization = finalize_streaming_compaction_under(
        &store,
        &namespace_id,
        &spec,
        &input_keys,
        &result,
        &cancellation,
    )
    .await;

    assert_matches!(
        finalization,
        CompactionUnitOutcome::Cancelled,
        "a cancelled finalization must publish nothing, got {finalization:?}"
    );
    assert_eq!(
        current_manifest_number(&store, &namespace_id).await,
        manifest_before,
        "the manifest must be where the job found it"
    );
    assert_eq!(
        visible_namespace(&store, &namespace_id).await,
        visible_before,
        "and a read must answer exactly what it answered before"
    );
    let staged = orphan_segment_keys(&store, &namespace_id).await;
    let referenced = referenced_segment_keys(&store, &namespace_id).await;
    assert!(
        !staged.is_empty(),
        "the job wrote segments before it was cancelled"
    );
    assert!(
        staged.is_disjoint(&referenced),
        "and nothing may reference them"
    );
}

#[tokio::test]
async fn a_backlogged_job_limits_input_and_preserves_unselected_runs() {
    let dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(dir.path()).expect("store");
    let namespace = NamespaceId::parse("backlog").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace, &context)
        .await
        .expect("bootstrap");
    for index in 0..20 {
        crate::test_support::NamespaceFixture::new(&store, &namespace, &context)
            .write_file_bytes(&format!("/file-{index}.txt"), b"data", None)
            .await
            .expect("write");
        create_snapshot(&store, &namespace, &context)
            .await
            .expect("fold");
    }
    let before = current_metadata_state(&store, &namespace).await;
    let spec = compaction_spec_for_group(&store, &namespace, MetadataFamilyGroup::Bindings).await;
    assert_eq!(
        spec.input_runs(),
        MetadataRunPolicy::default().max_input_runs_per_step.get()
    );
    let input = input_keys_now(&store, &namespace, &spec).await;
    let untouched = referenced_segment_keys(&store, &namespace)
        .await
        .difference(&input)
        .cloned()
        .collect::<BTreeSet<_>>();
    assert!(!untouched.is_empty());
    let result = run_compaction(
        &store,
        &namespace,
        &spec,
        small_segment_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    .expect("compact");
    assert!(
        result.peak_resident_blocks <= 2 * 2 * spec.input_runs(),
        "two blocks per iterator, at most two families per cluster"
    );
    publish_streaming_compaction(&store, &namespace, &spec, &input, &result).await;
    let referenced = referenced_segment_keys(&store, &namespace).await;
    assert!(untouched.is_subset(&referenced));
    let after = current_metadata_state(&store, &namespace).await;
    assert!(metadata_states_equivalent(&before, &after));
}

#[tokio::test]
async fn an_expired_job_prevents_publication() {
    use crate::limits::METADATA_COMPACTION_BUDGET_MS;
    use loonfs_test_support::stores::RecordingStore;

    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace = NamespaceId::parse("abandoned-publication").expect("namespace");
    seed_bindings_workload(&store, &namespace).await;
    let spec = compaction_spec_for_group(&store, &namespace, MetadataFamilyGroup::Bindings).await;
    let input = input_keys_now(&store, &namespace, &spec).await;
    let cancellation = MaintenanceCancellation::default();
    let result = run_compaction(
        &store,
        &namespace,
        &spec,
        small_segment_policy(),
        &cancellation,
    )
    .await
    .expect("compact");
    let store = RecordingStore::new(store, KeyPredicate::any());
    let timer = Arc::new(ManualClock::stepping(1_000));
    let writes = || CompactionWrites {
        spec: spec.clone(),
        input_keys: input.clone(),
        report: result.clone(),
        compaction: OperationClock::start(0, timer.clone()),
    };
    let expired = writes();
    timer.set_ms(METADATA_COMPACTION_BUDGET_MS + 1);
    let outcome = publish_compaction_over_current(&store, &namespace, expired, &cancellation)
        .await
        .expect("elapsed bound");
    assert_eq!(outcome, CompactionUnitOutcome::Abandoned);
    assert_eq!(store.counts().puts, 0);
}

/// Checks that a finalization from the current manifest, which a discovery
/// loads with the manifest boundary, a listing of the manifests, the newest
/// manifest, and the boundary again, sends its put and reads the boundary
/// once after it, and nothing more.
#[tokio::test]
async fn a_finalization_attempt_loads_the_current_manifest_once() {
    use loonfs_test_support::stores::RecordingStore;

    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("finalization-requests").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    seed_bindings_workload(&store, &namespace).await;
    let spec = compaction_spec_for_group(&store, &namespace, MetadataFamilyGroup::Bindings).await;
    let input = input_keys_now(&store, &namespace, &spec).await;
    let result = run_compaction(
        &store,
        &namespace,
        &spec,
        small_segment_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    .expect("compact");
    let current = current_manifest_number(&store, &namespace).await;
    let next = current.successor().expect("next manifest number");
    let store = RecordingStore::new(store, KeyPredicate::any());
    let manifest_no =
        publish_streaming_compaction(&store, &namespace, &spec, &input, &result).await;
    assert_eq!(manifest_no, next);
    let operations = store.take();
    let mut expected = discovery_requests(&namespace, current);
    expected.extend(publication_requests(&namespace, next));
    assert_eq!(control_requests(&namespace, &operations), expected);
    assert_eq!(operations.len(), expected.len(), "{operations:?}");
}

/// Returns the requests of a search for the newest manifest, which is
/// `current`: the manifest boundary, a listing of the manifests, the
/// manifest, and the boundary again.
fn discovery_requests(namespace: &NamespaceId, current: ManifestNo) -> Vec<(&'static str, String)> {
    let boundary = boundary(namespace);
    vec![
        ("get", boundary.clone()),
        ("list", metadata_manifest_prefix(namespace)),
        ("get", metadata_manifest_object(namespace, &current)),
        ("get", boundary),
    ]
}

/// Returns the requests of a publication of manifest `next`: its put and one
/// read of the manifest boundary.
fn publication_requests(namespace: &NamespaceId, next: ManifestNo) -> Vec<(&'static str, String)> {
    vec![
        ("put", metadata_manifest_object(namespace, &next)),
        ("get", boundary(namespace)),
    ]
}

/// Checks that a compaction unit loads nothing to plan, publishes on its
/// session's newest published manifest in its turn with its put and no
/// request before it, and reads the manifest boundary after the put.
///
/// The test installs a segment cache in the fixture's namespace, as a
/// runtime has one, so the unit takes its position's manifest from the cache
/// that the session's load filled. (A fixture's namespace has no segment
/// cache by default.)
#[tokio::test]
async fn a_unit_loads_no_manifest_to_plan() {
    use loonfs_test_support::stores::RecordingStore;

    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("unit-requests").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    seed_bindings_workload(&store, &namespace).await;
    let store = RecordingStore::new(store, KeyPredicate::any());
    let engine = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = compact_everything_policy();
        fixture.namespace.segment_cache =
            std::sync::Arc::new(crate::segment::MetadataSegmentCache::unshared(64 << 20));
        fixture
    }
    .engine();
    // The session's claim and its load of the position come before the unit.
    let mut session = engine.commit_engine();
    let position = session
        .load_position_alone()
        .await
        .expect("the session's position");
    let next = position
        .read_basis()
        .basis
        .manifest()
        .manifest_no
        .successor()
        .expect("next manifest number");
    store.reset();
    let cancellation = MaintenanceCancellation::default();
    let crate::CompactionWriteOutcome::Written(writes) = engine
        .write_metadata_compaction_unit(&position, &cancellation)
        .await
        .expect("write a compaction unit")
    else {
        panic!("a compaction unit is due");
    };
    let lane = session.publication_lane();
    let outcome = engine
        .namespace
        .publish_compaction_in_turn(lane.lock().await.acquired_turn(), writes, &cancellation)
        .await
        .expect("publish a compaction unit");
    assert_matches!(
        outcome,
        CompactionUnitOutcome::Published { manifest, .. } if manifest.manifest().manifest().manifest_no == next
    );
    assert_eq!(
        control_requests(&namespace, &store.take()),
        publication_requests(&namespace, next)
    );
}

#[tokio::test]
async fn direct_output_is_published_by_number_and_failed_output_ages_out() {
    use crate::limits::GC_GRACE_WINDOW_MS;
    use loonfs_test_support::stores::{FailStore, InjectedError, MetadataMapStore, RecordingStore};

    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("direct-output").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    seed_bindings_workload(&store, &namespace).await;
    let before = current_manifest_number(&store, &namespace).await;
    let store = RecordingStore::new(store, KeyPredicate::any());
    let outcome = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = small_segment_policy();
        fixture
    }
    .engine()
    .compact_from_current()
    .await
    .expect("publish");
    assert_matches!(
        outcome,
        CompactionUnitOutcome::Published { manifest, .. }
            if manifest.manifest().manifest().manifest_no == before.successor().expect("next number")
    );
    let puts = store
        .take()
        .into_iter()
        .filter(|operation| {
            matches!(
                operation,
                loonfs_test_support::stores::RecordedOperation::Put { .. }
                    | loonfs_test_support::stores::RecordedOperation::PutStreamed { .. }
            )
        })
        .map(|operation| operation.key().to_owned())
        .collect::<Vec<_>>();
    assert!(puts
        .iter()
        .any(|key| key.starts_with(&metadata_segment_prefix(&namespace))));
    assert!(puts
        .iter()
        .all(|key| key.starts_with(&metadata_segment_prefix(&namespace))
            || key.starts_with(&loonfs_objectstore::keys::metadata_manifest_prefix(
                &namespace
            ))));

    // The first job left the group with one base run, which the planner does
    // not select; one more binding gives it a delta run to select.
    crate::test_support::NamespaceFixture::new(&store, &namespace, &test_context())
        .write_file_bytes("/late/f.txt", b"late\n", None)
        .await
        .expect("write a later file");
    create_snapshot(&store, &namespace, &test_context())
        .await
        .expect("fold the later write");
    let current_keys = referenced_segment_keys(&store, &namespace).await;
    let before = current_manifest_number(&store, &namespace).await;
    let existing_orphans = orphan_segment_keys(&store, &namespace).await;
    let failing = FailStore::new(
        store,
        KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
            &namespace,
        )),
        OperationSelector::Put,
        InjectedError::PermissionDenied("publication stopped".to_owned()),
    );
    failing.fail_next(1);
    {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &failing,
            &namespace,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = small_segment_policy();
        fixture
    }
    .engine()
    .compact_from_current()
    .await
    .expect_err("a stopped publication should fail the compaction unit");
    assert_eq!(current_manifest_number(&failing, &namespace).await, before);
    let orphans = orphan_segment_keys(&failing, &namespace)
        .await
        .difference(&existing_orphans)
        .cloned()
        .collect::<BTreeSet<_>>();
    assert!(!orphans.is_empty());
    failing.clear();
    let store = MetadataMapStore::aged(failing, KeyPredicate::any());
    for age_ms in [GC_GRACE_WINDOW_MS - 1, GC_GRACE_WINDOW_MS] {
        crate::test_support::namespace_store(&store, &namespace)
            .gc(&writer_at("collector", age_ms).clock())
            .await
            .expect("collect");
        for key in &orphans {
            assert_eq!(
                store.head(key).await.expect("head").is_some(),
                age_ms < GC_GRACE_WINDOW_MS
            );
        }
        for key in &current_keys {
            assert!(store.head(key).await.expect("head").is_some());
        }
    }
}

#[tokio::test]
async fn layout_compactions_prune_absent_chains_only_at_the_base_even_past_the_filter_cap() {
    use crate::metadata::RevisionRecord;
    use loonfs_types::format::manifest::ContentLayoutRecord;
    use loonfs_types::{ContentExtent, ContentId, ContentLayout, ContentRef, ExtentObject};

    let directory = tempdir().expect("directory");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = NamespaceId::parse("layouts").expect("namespace");
    let reference = ContentRef::blob_v1(
        ContentId::parse("con_00000000000000000000000000").expect("content"),
        b"four",
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let absent = ContentId::parse("con_00000000000000000000000001").expect("content");
    let mut state = MetadataStateBuilder::default();
    state.push_revision(RevisionRecord {
        inode_id: InodeId(2),
        revision_no: RevisionNo(1),
        committed_seq: CommitSeq(1),
        commit_id: CommitId::parse("first").expect("commit"),
        committed_at_ms: 0,
        committed_by: loonfs_types::ActorId::loonfs(),
        delta_index: 0,
        content_ref: reference.clone(),
    });
    for (content_id, size_bytes, committed_seq) in [
        (reference.content_id.clone(), 8, CommitSeq(1)),
        (reference.content_id.clone(), 4, CommitSeq(2)),
        (absent.clone(), 4, CommitSeq(1)),
    ] {
        state.push_content_layout(ContentLayoutRecord {
            content_id: content_id.clone(),
            size_bytes,
            committed_seq,
            layout: ContentLayout {
                creator_namespace_id: namespace_id.clone(),
                extents: vec![ContentExtent {
                    content_id,
                    object: ExtentObject::Whole,
                    object_bytes: size_bytes,
                    object_checksum: loonfs_types::Checksum::crc64nvme(b""),
                    offset: 0,
                    length: size_bytes,
                }],
            },
        });
    }
    let state = state.build();
    let mut head =
        NamespaceReadState::initial(namespace_id.clone(), 0, loonfs_types::ActorId::loonfs());
    head.seq = CommitSeq(2);
    let policy = MetadataRunPolicy::default();
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &head,
            basis_manifest_no: None,
            retention_floor_seq: head.seq,
            metadata_state: &state,
        },
        policy,
        ManifestNo(1),
    )
    .await
    .expect("manifest");
    let cache = MetadataSegmentCache::unshared(8 * 1024 * 1024);
    let runs = runs_newest_first(manifest.payload());
    let mut basis = VerifiedMetadataSegments::from_runs(&store, &cache, runs.clone());
    basis.manifest = Arc::new(manifest);
    let revision_key = metadata_segment_object_key(
        group_run_descriptors(&runs[0], MetadataFamilyGroup::Revisions)
            .next()
            .expect("revision segment"),
    );
    let group = MetadataFamilyGroup::ContentLayouts;
    for (bottom, capped, expected) in [(true, false, 1), (false, false, 2), (true, true, 1)] {
        if capped {
            let mut counted_runs = runs.clone();
            for run in &mut counted_runs {
                for family in &mut run.segments {
                    // 200 million IDs take the whole 64 MiB at about
                    // 2.7 bits per ID.
                    if family.family == ApiMetadataRowFamily::Revisions {
                        family.segments[0].row_count = 200_000_000;
                    }
                }
            }
            basis.scan_runs = Arc::new(counted_runs);
        }
        let placement = if bottom {
            plan::CompactionPlacement::Base {
                output_seq: head.seq,
            }
        } else {
            plan::CompactionPlacement::Delta {
                output_seq: head.seq,
            }
        };
        store.reset();
        let spec = MetadataCompactionSpec::new(group, vec![RunNo(0)], 3, placement, head.seq);
        let result = run_metadata_compaction(
            &basis,
            &crate::cache::MetadataSegmentCache::unshared(0),
            &namespace_id,
            &spec,
            policy,
            &MaintenanceCancellation::default(),
        )
        .await
        .expect("compact")
        .expect("not cancelled");
        assert_eq!(
            store
                .take_gets()
                .iter()
                .filter(|read| read.0 == revision_key)
                .count(),
            if bottom { 1 } else { 0 }
        );
        {
            assert_eq!(result.rows_written, expected);
            let mut layouts = Vec::new();
            for descriptor in &result.output_segments {
                let (index, _) =
                    index_block_load::load_segment_index_for_compaction(&store, descriptor)
                        .await
                        .expect("index");
                let blocks = data_block_load::load_segment_data_block_span(
                    &store, None, None, descriptor, &index,
                )
                .await
                .expect("blocks");
                for block in blocks {
                    layouts.extend(block.rows.iter().cloned());
                }
            }
            assert!(layouts
                .iter()
                .any(|row| matches!(row, MetadataRow::ContentLayout(layout)
                if layout.content_id == reference.content_id && layout.size_bytes == 8)));
            assert_eq!(
                layouts
                    .iter()
                    .any(|row| matches!(row, MetadataRow::ContentLayout(layout)
                if layout.content_id == absent)),
                !bottom
            );
        }
    }
}

/// Checks that a unit whose output has no segments, and whose manifest put
/// is written but loses its acknowledgment, reports `Published`. The output
/// adds no segment to the manifest, so the unit can recognize the manifest
/// it reads back as its own only by the publication ID.
#[tokio::test]
async fn an_empty_output_whose_put_loses_its_acknowledgment_is_published() {
    use loonfs_test_support::stores::{FailStore, InjectedError};

    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    seed_bindings_workload(&store, &namespace).await;
    let next = current_manifest_number(&store, &namespace)
        .await
        .successor()
        .expect("next number");
    let spec = compaction_spec_for_group(&store, &namespace, MetadataFamilyGroup::Bindings).await;
    let input = input_keys_now(&store, &namespace, &spec).await;
    let mut result = run_compaction(
        &store,
        &namespace,
        &spec,
        small_segment_policy(),
        &MaintenanceCancellation::default(),
    )
    .await
    .expect("compaction");
    // Clear the segments to simulate an output in which every row was dropped.
    result.output_segments.clear();
    let store = FailStore::new(
        store,
        KeyPredicate::manifest(&namespace),
        OperationSelector::Put,
        InjectedError::Transport("lost acknowledgment".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    let outcome = finalize_streaming_compaction(&store, &namespace, &spec, &input, &result).await;
    assert_matches!(
        outcome,
        CompactionUnitOutcome::Published { manifest, output_segments: 0, .. }
            if manifest.manifest().manifest().manifest_no == next
    );
    assert_eq!(current_manifest_number(&store, &namespace).await, next);
}
