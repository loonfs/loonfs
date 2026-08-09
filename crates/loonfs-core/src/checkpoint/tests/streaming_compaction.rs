//! Rebuilding a family group in one streaming job: the plan split, the
//! oracle that says a streaming job and a whole-group fold reach the same
//! place, restart equivalence, and the resource bounds that make the job
//! independent of the size of what it rebuilds.

use super::super::reorganize::{select_reorganization_input, ReorganizationPlan};
use super::super::row::manifest_row_commit_seq;
use super::super::runs::MetadataFamilyGroup;
use super::super::scan::VerifiedMetadataTables;
use super::super::streaming_compaction::{
    finalize_metadata_compaction, run_metadata_compaction, snapshot_segment_keys, Finalization,
    MetadataCompactionCancellation, MetadataCompactionOutcome, MetadataCompactionResult,
    MetadataCompactionSpec,
};
use super::*;
use crate::timing::StdMonotonicTimer;
use loonfs_objectstore::keys::metadata_compaction_staging_prefix;
use loonfs_test_support::stores::ConcurrencyWatchStore;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

// -------------------------------------------------------------------------
// The family-group table
// -------------------------------------------------------------------------

/// Every family compacts in exactly one group. A family in none would never
/// be folded; a family in two would be rewritten twice at one identity.
#[test]
fn every_family_belongs_to_exactly_one_reorganization_group() {
    for family in CHECKPOINT_TABLE_FAMILIES {
        let groups: Vec<_> = REORGANIZE_FAMILY_GROUPS
            .into_iter()
            .filter(|group| group.families().contains(&family))
            .collect();
        assert_eq!(
            groups.len(),
            1,
            "`{family:?}` belongs to {} reorganization groups",
            groups.len()
        );
    }
    let listed: usize = REORGANIZE_FAMILY_GROUPS
        .iter()
        .map(|group| group.families().len())
        .sum();
    assert_eq!(
        listed,
        CHECKPOINT_TABLE_FAMILIES.len(),
        "the groups must list every family once and nothing else"
    );
}

// -------------------------------------------------------------------------
// Workloads
// -------------------------------------------------------------------------

/// A namespace whose bindings group holds many parent directories, deletions
/// and moves below the retention floor, and more of both above it — enough
/// for a job to cross many slots and for the drop rules to have something to
/// do.
async fn seed_bindings_workload(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // Below the floor: files created, some deleted, some moved. The deletions
    // and moves leave unbinds the job may drop.
    for directory in 0..6u64 {
        for file in 0..3u64 {
            write_file_bytes(
                store,
                namespace_id,
                &format!("/d{directory}/f{file}.txt"),
                format!("body {directory}/{file}\n").as_bytes(),
                &context,
                None,
            )
            .await
            .expect("write file");
        }
        create_checkpoint(store, namespace_id, &context)
            .await
            .expect("checkpoint");
    }
    for directory in 0..3u64 {
        delete_path(
            store,
            namespace_id,
            &format!("/d{directory}/f0.txt"),
            &context,
            None,
        )
        .await
        .expect("delete file");
        move_path(
            store,
            namespace_id,
            &format!("/d{directory}/f1.txt"),
            &format!("/d{directory}/moved-f1.txt"),
            &context,
            None,
        )
        .await
        .expect("move file");
    }
    // One deleted directory, so a tombstone and an active deletion travel
    // with the bindings the job drops.
    delete_path(store, namespace_id, "/d5", &context, None)
        .await
        .expect("delete directory");
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the deletions");

    // Fold everything into one base run, so the snapshot has a base under its
    // delta runs the way a real over-budget group does. The base is cut into
    // small segments on purpose: an iterator walks whole segments, so small
    // segments make it open and close several of them.
    drain_reorganization(
        store,
        namespace_id,
        &context,
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
            ..MetadataLsmPolicy::default()
        },
    )
    .await;
    advance_retention_floor(store, namespace_id, &context)
        .await
        .expect("advance the floor past the deletions");

    // Above the floor: fresh directories and one more deletion, published as
    // delta runs the job merges with the base.
    for directory in 6..9u64 {
        for file in 0..2u64 {
            write_file_bytes(
                store,
                namespace_id,
                &format!("/d{directory}/f{file}.txt"),
                format!("late body {directory}/{file}\n").as_bytes(),
                &context,
                None,
            )
            .await
            .expect("write late file");
        }
        create_checkpoint(store, namespace_id, &context)
            .await
            .expect("checkpoint late writes");
    }
    delete_path(store, namespace_id, "/d6/f0.txt", &context, None)
        .await
        .expect("delete a late file");
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the late deletion");
}

/// A namespace whose bindings group is dominated by one directory, so one
/// parent holds most of the group's rows.
///
/// Renames are what make it lopsided. Every rename writes a bind and an
/// unbind under the same parent, and no inode row and no revision row, so the
/// parent grows while the rest of the namespace stays where it was. That is
/// the shape the old per-step row budget had no answer for.
async fn seed_one_wide_directory(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for file in 0..6u64 {
        write_file_bytes(
            store,
            namespace_id,
            &format!("/wide/f{file}.txt"),
            format!("body {file}\n").as_bytes(),
            &context,
            None,
        )
        .await
        .expect("write file");
    }
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the writes");
    for round in 0..10u64 {
        for file in 0..6u64 {
            let from = match round {
                0 => format!("/wide/f{file}.txt"),
                previous => format!("/wide/f{file}-r{}.txt", previous - 1),
            };
            move_path(
                store,
                namespace_id,
                &from,
                &format!("/wide/f{file}-r{round}.txt"),
                &context,
                None,
            )
            .await
            .expect("rename file");
        }
        create_checkpoint(store, namespace_id, &context)
            .await
            .expect("checkpoint the renames");
    }
    drain_reorganization(
        store,
        namespace_id,
        &context,
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
            ..MetadataLsmPolicy::default()
        },
    )
    .await;
    advance_retention_floor(store, namespace_id, &context)
        .await
        .expect("advance the floor past the renames");

    write_file_bytes(
        store,
        namespace_id,
        "/wide/late.txt",
        b"late body\n",
        &context,
        None,
    )
    .await
    .expect("write a late file");
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the late write");
}

// -------------------------------------------------------------------------
// Driving a job
// -------------------------------------------------------------------------

/// A budget that admits the whole group in one step, so the ordinary fold can
/// rebuild in one unit whatever the streaming job did incrementally.
fn fold_everything_policy() -> MetadataLsmPolicy {
    MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(4_000_000).expect("nonzero"),
        max_decoded_input_bytes_per_step: NonZeroUsize::new(1 << 30).expect("nonzero"),
        max_input_runs_per_step: NonZeroUsize::new(64).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    }
}

/// A budget one byte wide, which admits no run whole, so the planner has no
/// window that makes progress and answers with a compaction.
fn starving_policy() -> MetadataLsmPolicy {
    MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    }
}

/// A per-step row budget one row short of what `group`'s base run holds.
///
/// That is the production condition: no window over this group makes progress,
/// so a step hands it to a job, while every other group still folds normally
/// on the steps around it.
async fn policy_that_starves_the_group<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) -> MetadataLsmPolicy {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    let base_rows: u64 = runs_in_scan_order(&tables.manifest().payload)
        .iter()
        .filter(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
        .flat_map(|run| run.tables.iter())
        .filter(|table| group.contains(table.family))
        .flat_map(|table| &table.segments)
        .map(|descriptor| descriptor.row_count)
        .sum();
    assert!(
        base_rows > 1,
        "the seed must leave this group a base run to starve"
    );
    MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(
            usize::try_from(base_rows).expect("test row counts are small") - 1,
        )
        .expect("nonzero"),
        max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    }
}

/// A budget that rolls many small output segments, so the job's writers are
/// exercised rather than producing one segment per family.
fn small_segment_policy() -> MetadataLsmPolicy {
    MetadataLsmPolicy {
        max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
        ..fold_everything_policy()
    }
}

/// The tables of whatever manifest the namespace's root names right now.
async fn load_current_manifest_tables<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> VerifiedMetadataTables<'a, S> {
    let manifest_object_id = current_manifest_object_id(store, namespace_id).await;
    load_verified_manifest_tables(store, namespace_id, &manifest_object_id)
        .await
        .expect("load the current manifest's tables")
}

/// The runs a job snapshots: every run of the manifest that holds rows of the
/// group, which is what makes the input bottom-anchored and its drops legal.
fn snapshot_runs_for_group(
    manifest: &NamespaceManifestEnvelope,
    group: MetadataFamilyGroup,
) -> Vec<MetadataRunManifest> {
    runs_in_scan_order(&manifest.payload)
        .into_iter()
        .filter(|run| {
            run.tables
                .iter()
                .any(|table| group.contains(table.family) && !table.segments.is_empty())
        })
        .collect()
}

/// The spec a planner would produce for `group` against the current manifest:
/// every run the group holds, the output at the manifest head, and the live
/// retention floor.
async fn compaction_spec_for_group<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) -> MetadataCompactionSpec {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    let frozen_floor_seq = read_floor_seq(store, namespace_id).await;
    // One byte admits no run whole, so the planner has no window that makes
    // progress and answers with the compaction plan for this group.
    let selection = select_reorganization_input(
        &tables,
        group,
        MetadataLsmPolicy {
            max_l0_runs: NonZeroUsize::MIN,
            max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
            ..MetadataLsmPolicy::default()
        },
        frozen_floor_seq,
    )
    .await
    .expect("plan the group");
    match selection.plan {
        Some(ReorganizationPlan::FullCompaction(spec)) => spec,
        _ => panic!("a group no budget admits must plan as a streaming compaction"),
    }
}

async fn run_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    policy: MetadataLsmPolicy,
    cancellation: &MetadataCompactionCancellation,
) -> MetadataCompactionOutcome {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    run_metadata_compaction(&tables, namespace_id, spec, policy, cancellation)
        .await
        .expect("run the streaming compaction")
}

/// The segments the group's snapshot runs hold right now, which is what
/// finalization compares the manifest against.
async fn snapshot_keys_now<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
) -> BTreeSet<String> {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    snapshot_segment_keys(&tables, spec).expect("the snapshot must be present")
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
    snapshot_keys: &BTreeSet<String>,
    result: &MetadataCompactionResult,
) -> Finalization {
    finalize_metadata_compaction(
        store,
        namespace_id,
        &test_context(),
        spec,
        snapshot_keys,
        result.clone(),
        &StdMonotonicTimer::default(),
    )
    .await
    .expect("finalize the streaming compaction")
}

/// The same, for the tests where anything but a publication is a failure.
async fn publish_streaming_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    snapshot_keys: &BTreeSet<String>,
    result: &MetadataCompactionResult,
) -> ManifestId {
    match finalize_streaming_compaction(store, namespace_id, spec, snapshot_keys, result).await {
        Finalization::Published(manifest_id) => manifest_id,
        other => panic!("no concurrent publisher exists in this test, got {other:?}"),
    }
}

/// The group's rows a reader sees right now: read from `metadata_files`,
/// which is exactly what a scan concatenates.
async fn group_rows_from_manifest<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    group: MetadataFamilyGroup,
) -> BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>> {
    let mut rows_by_family = BTreeMap::new();
    for family in group.families() {
        let mut rows = tables
            .scan_prefix(*family, "")
            .await
            .expect("scan the group");
        rows.sort_by_key(|row| row.row_key_for_family(*family));
        rows_by_family.insert(*family, rows);
    }
    rows_by_family
}

async fn group_rows_of_current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) -> BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>> {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    group_rows_from_manifest(&tables, group).await
}

async fn current_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> MetadataState {
    load_manifest_materialization_for_inspection(
        store,
        namespace_id,
        read_metadata_root_object(store, namespace_id)
            .await
            .expect("read root")
            .envelope
            .state
            .manifest_id,
    )
    .await
    .expect("materialize the manifest")
    .metadata_state
}

/// Copies a local store's whole object tree, so two jobs can start from
/// byte-identical durable state. Content ids and table ids are generated, so
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

async fn staged_object_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    use futures::StreamExt;
    store
        .list_prefix_stream(&metadata_compaction_staging_prefix(namespace_id.as_str()))
        .map(|key| key.expect("list staged objects"))
        .collect()
        .await
}

/// Runs the whole-group fold with the budgets raised, which is the other way
/// of doing what the streaming job does.
async fn fold_group_whole<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) {
    let report = super::super::reorganize_metadata_step(
        store,
        namespace_id,
        &test_context(),
        fold_everything_policy(),
        None,
    )
    .await
    .expect("fold the group whole");
    let MetadataReorganizeOutcome::UnitPublished { families, .. } = report.outcome else {
        panic!("the whole-group fold must publish a unit");
    };
    assert_eq!(
        families,
        group.families(),
        "both rebuilds must work on the same family group"
    );
}

// -------------------------------------------------------------------------
// The plan split
// -------------------------------------------------------------------------

/// A group whose bottom-anchored window fits the budgets is a bounded merge;
/// the same group under budgets no window can satisfy is a streaming
/// compaction over every run it holds.
#[tokio::test]
async fn the_planner_answers_with_a_merge_or_with_a_compaction() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    let tables = load_current_manifest_tables(&store, &namespace_id).await;

    let generous = select_reorganization_input(&tables, group, fold_everything_policy(), floor_seq)
        .await
        .expect("plan under generous budgets");
    assert!(
        matches!(generous.plan, Some(ReorganizationPlan::BoundedMerge(_))),
        "a window that fits is a bounded merge"
    );
    assert!(generous.group_bottom_over_budget.is_none());

    let starved = select_reorganization_input(
        &tables,
        group,
        MetadataLsmPolicy {
            max_l0_runs: NonZeroUsize::MIN,
            max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
            ..MetadataLsmPolicy::default()
        },
        floor_seq,
    )
    .await
    .expect("plan under budgets nothing fits");
    let Some(ReorganizationPlan::FullCompaction(spec)) = starved.plan else {
        panic!("a group no window fits must plan as a streaming compaction");
    };
    assert!(
        starved.group_bottom_over_budget.is_some(),
        "the operator still hears that the group's oldest run is over budget"
    );
    assert_eq!(spec.group(), group);
    assert_eq!(spec.frozen_floor_seq(), floor_seq);
    let snapshot = snapshot_runs_for_group(tables.manifest(), group);
    assert_eq!(
        spec.inputs().iter().copied().collect::<BTreeSet<_>>(),
        snapshot
            .iter()
            .map(|run| (run.run_seq, run.level))
            .collect::<BTreeSet<_>>(),
        "a compaction takes every run the group holds, which is what anchors it at the bottom"
    );
    assert!(
        snapshot.len() > 1,
        "this workload must leave a base under at least one delta run"
    );
}

/// A step that plans a compaction publishes nothing and stages nothing: it
/// hands the plan back, and the runtime starts the job.
#[tokio::test]
async fn a_step_that_plans_a_compaction_publishes_nothing_itself() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let before = current_manifest_object_id(&store, &namespace_id).await;

    let report = super::super::reorganize_metadata_step(
        &store,
        &namespace_id,
        &test_context(),
        starving_policy(),
        None,
    )
    .await
    .expect("budgeted step");
    let MetadataReorganizeOutcome::CompactionPlanned { families, spec } = report.outcome else {
        panic!("a group no window fits must plan a streaming compaction");
    };
    assert_eq!(families, MetadataFamilyGroup::Bindings.families());
    assert!(spec.input_rows() > 0, "the plan must report what it reads");
    assert_eq!(
        current_manifest_object_id(&store, &namespace_id).await,
        before,
        "a step that plans a compaction publishes nothing"
    );
    assert!(
        staged_object_keys(&store, &namespace_id).await.is_empty(),
        "and stages nothing, because the job has not started"
    );
}

/// While a job rebuilds one group, steps leave that group alone and fold the
/// others. Without this a step would keep re-planning the running job: the
/// group's L0 rows are frozen in the job's snapshot, so its count never falls
/// while every other group's does.
#[tokio::test]
async fn a_step_leaves_the_group_a_running_job_is_rebuilding_alone() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };

    // Same durable state, twice. With no job in flight this group is what a
    // step takes: it holds the most L0 rows.
    let fold_dir = tempdir().expect("tempdir");
    copy_store_tree(temp_dir.path(), fold_dir.path());
    let fold_store = LocalFsStore::new(fold_dir.path()).expect("store");
    let without = super::super::reorganize_metadata_step(
        &fold_store,
        &namespace_id,
        &test_context(),
        policy,
        None,
    )
    .await
    .expect("step with no job running");
    let MetadataReorganizeOutcome::UnitPublished { families, .. } = without.outcome else {
        panic!("expected a merge, got {:?}", without.outcome);
    };
    assert_eq!(
        families,
        group.families(),
        "the seed must leave this group the one a step would take"
    );

    // With the job's plan in hand, every step folds another group and none
    // touches this one, so the job's input is exactly what it was.
    let mut other_groups_folded = 0usize;
    for _ in 0..16 {
        let report = super::super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &test_context(),
            policy,
            Some(&spec),
        )
        .await
        .expect("step with the job running");
        match report.outcome {
            MetadataReorganizeOutcome::UnitPublished { families, .. } => {
                assert_ne!(
                    families,
                    group.families(),
                    "a step must not merge the group a job is rebuilding"
                );
                other_groups_folded += 1;
            }
            MetadataReorganizeOutcome::NotNeeded { .. } => break,
            other => panic!("unexpected outcome {other:?}"),
        }
    }
    assert!(
        other_groups_folded > 0,
        "ordinary maintenance must keep going while a job runs"
    );
    assert_eq!(
        snapshot_keys_now(&store, &namespace_id, &spec).await,
        snapshot_keys,
        "the job's input must be exactly what it was when the job was planned"
    );
}

// -------------------------------------------------------------------------
// The oracle
// -------------------------------------------------------------------------

/// A streaming compaction and a whole-group fold with the budgets raised are
/// two ways of doing the same thing, so they must land in the same place: the
/// same surviving rows in every family of the group, the same materialized
/// namespace, and the same rows dropped.
#[tokio::test]
async fn a_streaming_compaction_and_a_whole_group_fold_reach_the_same_rows() {
    let job_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(job_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let frozen_floor_seq = read_floor_seq(&store, &namespace_id).await;

    // The same durable bytes, folded the ordinary way.
    let fold_dir = tempdir().expect("tempdir");
    copy_store_tree(job_dir.path(), fold_dir.path());
    let fold_store = LocalFsStore::new(fold_dir.path()).expect("store");
    fold_group_whole(&fold_store, &namespace_id, group).await;
    let folded = group_rows_of_current_manifest(&fold_store, &namespace_id, group).await;
    let folded_state = current_metadata_state(&fold_store, &namespace_id).await;

    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let outcome = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await;
    let MetadataCompactionOutcome::Completed(result) = outcome else {
        panic!("nothing cancelled this job");
    };
    assert!(
        result.output_segments.len() > 1,
        "the segment budget must roll several output segments, got {}",
        result.output_segments.len()
    );
    assert!(
        result.output_segments.iter().all(|descriptor| descriptor
            .object_key
            .starts_with(&metadata_compaction_staging_prefix(namespace_id.as_str()))),
        "every output segment is written to the staging directory"
    );
    // Every reverse row the floor covers costs one point read, and no other
    // row costs one. That is what bounds the reads a job makes.
    let reverse_rows_at_or_below_floor = before[&ApiMetadataTableFamily::DirentryChildBinds]
        .iter()
        .filter(|row| {
            matches!(row, MetadataRow::DirentryBind { bind_seq, .. } if *bind_seq <= frozen_floor_seq)
        })
        .count() as u64;
    assert!(
        reverse_rows_at_or_below_floor > 0,
        "the seed must leave reverse rows the floor covers"
    );
    assert_eq!(result.unbind_probes, reverse_rows_at_or_below_floor);

    publish_streaming_compaction(&store, &namespace_id, &spec, &snapshot_keys, &result).await;
    let compacted = group_rows_of_current_manifest(&store, &namespace_id, group).await;

    assert_eq!(
        compacted, folded,
        "a streaming compaction and a whole-group fold must keep the same rows"
    );
    assert!(
        metadata_states_equivalent(
            &current_metadata_state(&store, &namespace_id).await,
            &folded_state
        ),
        "and must materialize the same namespace"
    );
    // The format gives every bind row exactly one reverse row, and manifest
    // load rejects a run whose two counts disagree. The two families are
    // decided in different passes here, so this is what says they stayed in
    // lockstep.
    assert_eq!(
        compacted[&ApiMetadataTableFamily::DirentryBinds].len(),
        compacted[&ApiMetadataTableFamily::DirentryChildBinds].len(),
    );
    assert_eq!(
        result.rows_written,
        compacted.values().map(Vec::len).sum::<usize>() as u64,
        "the job's row count must be what the manifest now holds"
    );
    // The loader's invariants must accept the result, and the group must be
    // left in one base run.
    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let runs = snapshot_runs_for_group(tables.manifest(), group);
    assert_eq!(runs.len(), 1, "the job must leave the group in one run");
    assert_eq!(runs[0].level, CHECKPOINT_BASE_RUN_LEVEL);

    // Teeth: the rebuilds must actually have dropped something, or the
    // comparison above is comparing two copies of the input.
    let rows_of = |rows: &BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>>| {
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

/// The oracle has teeth only if the retention operators are what make the two
/// rebuilds agree. A floor of zero is above nothing, so every rule that reads
/// the floor keeps every row, and the job must then disagree with the fold.
#[tokio::test]
async fn a_compaction_that_drops_nothing_fails_the_oracle() {
    let job_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(job_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;

    let fold_dir = tempdir().expect("tempdir");
    copy_store_tree(job_dir.path(), fold_dir.path());
    let fold_store = LocalFsStore::new(fold_dir.path()).expect("store");
    fold_group_whole(&fold_store, &namespace_id, group).await;
    let folded = group_rows_of_current_manifest(&fold_store, &namespace_id, group).await;

    let spec = compaction_spec_for_group(&store, &namespace_id, group)
        .await
        .with_frozen_floor_seq(ChangeSeq(0));
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let MetadataCompactionOutcome::Completed(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };
    assert_eq!(result.unbind_probes, 0, "a floor of zero covers no row");
    publish_streaming_compaction(&store, &namespace_id, &spec, &snapshot_keys, &result).await;

    let compacted = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    assert_ne!(
        compacted, folded,
        "with the floor rules keeping everything, the job must not reach the fold's rows"
    );
    assert!(
        compacted.values().map(Vec::len).sum::<usize>()
            > folded.values().map(Vec::len).sum::<usize>(),
        "and must keep strictly more rows than the fold kept"
    );
}

/// A group with no drop rule at all is a straight rewrite: revisions are
/// durable history, so the job's output must hold every row it read.
#[tokio::test]
async fn a_compaction_of_the_revisions_group_rewrites_every_row_it_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Revisions;
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;

    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let MetadataCompactionOutcome::Completed(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };
    assert_eq!(
        result.rows_read, result.rows_written,
        "a revisions rebuild drops nothing"
    );
    assert_eq!(
        result.unbind_probes, 0,
        "the revisions group has no bind rule, so it reads no unbind"
    );
    publish_streaming_compaction(&store, &namespace_id, &spec, &snapshot_keys, &result).await;

    assert_eq!(
        group_rows_of_current_manifest(&store, &namespace_id, group).await,
        before,
        "revision rows are durable history and are never dropped"
    );
}

// -------------------------------------------------------------------------
// Restart equivalence
// -------------------------------------------------------------------------

/// Cancels the job by setting a token once the store has served a given
/// number of reads, so a cancellation lands in the middle of a merge rather
/// than at a boundary the test picked.
#[derive(Debug)]
struct CancelAfterReadsStore {
    inner: LocalFsStore,
    cancellation: MetadataCompactionCancellation,
    reads_before_cancel: usize,
    reads: AtomicUsize,
}

#[async_trait]
impl ObjectStore for CancelAfterReadsStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

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

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

/// A cancelled job costs its work and nothing else. The manifest never moved,
/// so a reader answers exactly as before; the segments the attempt wrote are
/// staged and unreferenced; and running the same spec again lands where an
/// uninterrupted job would have landed.
#[tokio::test]
async fn a_cancelled_compaction_leaves_orphans_and_the_rerun_lands_where_it_would_have() {
    let straight_dir = tempdir().expect("tempdir");
    let straight_store = LocalFsStore::new(straight_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&straight_store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;

    let interrupted_dir = tempdir().expect("tempdir");
    copy_store_tree(straight_dir.path(), interrupted_dir.path());

    // The uninterrupted run, for the comparison.
    let spec = compaction_spec_for_group(&straight_store, &namespace_id, group).await;
    let snapshot_keys = snapshot_keys_now(&straight_store, &namespace_id, &spec).await;
    let MetadataCompactionOutcome::Completed(straight_result) = run_compaction(
        &straight_store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };
    publish_streaming_compaction(
        &straight_store,
        &namespace_id,
        &spec,
        &snapshot_keys,
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
    let manifest_before = current_manifest_object_id(
        &LocalFsStore::new(interrupted_dir.path()).expect("store"),
        &namespace_id,
    )
    .await;

    let mut orphans = BTreeSet::new();
    let mut cancelled_attempts = 0;
    for reads_before_cancel in [4usize, 12, 40] {
        let cancellation = MetadataCompactionCancellation::default();
        let store = CancelAfterReadsStore {
            inner: LocalFsStore::new(interrupted_dir.path()).expect("store"),
            cancellation: cancellation.clone(),
            reads_before_cancel,
            reads: AtomicUsize::new(0),
        };
        let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
        let outcome = run_compaction(
            &store,
            &namespace_id,
            &spec,
            small_segment_policy(),
            &cancellation,
        )
        .await;
        if matches!(outcome, MetadataCompactionOutcome::Cancelled) {
            cancelled_attempts += 1;
        }
        assert_eq!(
            current_manifest_object_id(&store, &namespace_id).await,
            manifest_before,
            "a cancelled attempt publishes nothing"
        );
        assert_eq!(
            group_rows_of_current_manifest(&store, &namespace_id, group).await,
            reader_answers_before,
            "and what a reader sees never moves"
        );
        orphans.extend(staged_object_keys(&store, &namespace_id).await);
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
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let MetadataCompactionOutcome::Completed(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled the re-run");
    };
    assert_eq!(
        (result.rows_read, result.rows_written, result.unbind_probes),
        (
            straight_result.rows_read,
            straight_result.rows_written,
            straight_result.unbind_probes
        ),
        "a re-run must do the same work an uninterrupted job did"
    );
    let referenced: BTreeSet<String> = result
        .output_segments
        .iter()
        .map(|descriptor| descriptor.object_key.clone())
        .collect();
    publish_streaming_compaction(&store, &namespace_id, &spec, &snapshot_keys, &result).await;

    assert_eq!(
        group_rows_of_current_manifest(&store, &namespace_id, group).await,
        straight_rows,
        "a re-run must keep the rows an uninterrupted job kept"
    );
    // The cancelled attempts' segments are still there and nothing names
    // them: they are orphans for the collector, not state anything reads.
    let staged_now = staged_object_keys(&store, &namespace_id).await;
    for orphan in &orphans {
        assert!(staged_now.contains(orphan));
        assert!(
            !referenced.contains(orphan),
            "a cancelled attempt's segment must not be referenced by the published run"
        );
    }
    let manifest_keys: BTreeSet<String> = load_current_manifest_tables(&store, &namespace_id)
        .await
        .manifest()
        .payload
        .metadata_files
        .iter()
        .map(|descriptor| descriptor.object_key.clone())
        .collect();
    assert!(
        orphans.iter().all(|orphan| !manifest_keys.contains(orphan)),
        "no manifest names a cancelled attempt's segment"
    );
}

// -------------------------------------------------------------------------
// Resource discipline
// -------------------------------------------------------------------------

/// The job's reads never overlap wider than its fetch bound, and the decoded
/// blocks it holds never follow the size of what it is rebuilding.
#[tokio::test]
async fn a_compaction_keeps_its_reads_and_its_decoded_blocks_bounded() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = ConcurrencyWatchStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_table(),
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
        KeyPredicate::metadata_table(),
    );
    let MetadataCompactionOutcome::Completed(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };

    // Eight iterators refill at once at most, and each refill is one span
    // fetch, so eight reads is the widest the job ever goes.
    let reads = store.reads();
    assert!(
        reads.peak_in_flight <= 8,
        "the job overlapped {} reads at once",
        reads.peak_in_flight
    );
    assert!(reads.total > 0, "the job must have read the snapshot");
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
    // No locality group is a family: the rules read one name slot, and a slot
    // holds the generations of one binding.
    assert!(
        result.peak_locality_rows <= 8,
        "one locality group held {} rows",
        result.peak_locality_rows
    );
}

/// One directory far past the old per-step row budget streams through with
/// the same bounded state. The case that needed a durable offset inside a
/// partition needs nothing now: the rules read one name slot, so the peak
/// tracks a slot's rows and not the directory's.
#[tokio::test]
async fn one_directory_far_past_the_row_budget_streams_a_slot_at_a_time() {
    let job_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(job_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_one_wide_directory(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;

    let fold_dir = tempdir().expect("tempdir");
    copy_store_tree(job_dir.path(), fold_dir.path());
    let fold_store = LocalFsStore::new(fold_dir.path()).expect("store");
    fold_group_whole(&fold_store, &namespace_id, group).await;
    let folded = group_rows_of_current_manifest(&fold_store, &namespace_id, group).await;
    let folded_state = current_metadata_state(&fold_store, &namespace_id).await;

    // How many rows the widest directory holds, which is what the peak must
    // not follow.
    let mut rows_per_parent = BTreeMap::<InodeId, usize>::new();
    for family in [
        ApiMetadataTableFamily::DirentryBinds,
        ApiMetadataTableFamily::DirentryUnbinds,
    ] {
        for row in &before[&family] {
            match row {
                MetadataRow::DirentryBind {
                    parent_inode_id, ..
                }
                | MetadataRow::DirentryUnbind {
                    parent_inode_id, ..
                } => *rows_per_parent.entry(*parent_inode_id).or_default() += 1,
                _ => {}
            }
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
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let MetadataCompactionOutcome::Completed(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };
    assert!(
        result.peak_locality_rows * 4 < widest,
        "the peak locality held {} rows against a directory of {widest}",
        result.peak_locality_rows
    );

    publish_streaming_compaction(&store, &namespace_id, &spec, &snapshot_keys, &result).await;
    assert_eq!(
        group_rows_of_current_manifest(&store, &namespace_id, group).await,
        folded,
        "a job that never held the directory must still reach the fold's rows"
    );
    assert!(
        metadata_states_equivalent(
            &current_metadata_state(&store, &namespace_id).await,
            &folded_state
        ),
        "and must answer reads the way a whole-group fold answers them"
    );
    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    assert_eq!(
        snapshot_runs_for_group(tables.manifest(), group).len(),
        1,
        "the job must leave the group in one run"
    );
}

// -------------------------------------------------------------------------
// The whole arc, through the step the maintenance runner calls
// -------------------------------------------------------------------------

/// The unbind rows a floor covers, which is the churn a rebuild reclaims.
fn unbinds_at_or_below(
    rows: &BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>>,
    floor_seq: ChangeSeq,
) -> usize {
    rows[&ApiMetadataTableFamily::DirentryUnbinds]
        .iter()
        .filter(|row| {
            matches!(row, MetadataRow::DirentryUnbind { unbind_seq, .. } if *unbind_seq <= floor_seq)
        })
        .count()
}

/// The group's segments the manifest holds outside a spec's input runs: the
/// runs that arrived after the job was planned.
async fn group_segments_outside_the_job<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    group: MetadataFamilyGroup,
) -> BTreeSet<String> {
    let inputs: BTreeSet<(ChangeSeq, u32)> = spec.inputs().iter().copied().collect();
    load_current_manifest_tables(store, namespace_id)
        .await
        .manifest()
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            group.contains(descriptor.family)
                && !inputs.contains(&(descriptor.run_seq, descriptor.level))
        })
        .map(|descriptor| descriptor.object_key.clone())
        .collect()
}

/// Every object key the current manifest references.
async fn referenced_segment_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    load_current_manifest_tables(store, namespace_id)
        .await
        .manifest()
        .payload
        .metadata_files
        .iter()
        .map(|descriptor| descriptor.object_key.clone())
        .collect()
}

/// The whole arc, driven through the entry point the maintenance runner
/// calls.
///
/// A group past the budgets with churn below the floor is handed to a job.
/// The runner starts that job and goes on stepping; here the steps and the job
/// run in one task, which is the same interleaving with the timing taken out.
/// While the job is in flight the other groups keep folding, no step touches
/// the job's group, a run arrives above the job's snapshot and survives its
/// publication, the loader accepts the manifest every step leaves, and a read
/// answers the same thing throughout. The job then publishes, the group ends
/// in one base run, and the churn the floor covered is gone.
#[tokio::test]
async fn an_over_budget_group_is_rebuilt_by_a_job_while_maintenance_carries_on() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let policy = policy_that_starves_the_group(&store, &namespace_id, group).await;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    let rows_before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    assert!(
        unbinds_at_or_below(&rows_before, floor_seq) > 0,
        "the seed must leave churn the floor covers, or a rebuild reclaims nothing"
    );
    let mut visible = visible_namespace(&store, &namespace_id).await;
    assert!(
        visible.iter().any(|state| state.visible),
        "the seed must leave something to read"
    );

    let mut active: Option<MetadataCompactionSpec> = None;
    let mut steps_with_a_job_running = 0usize;
    let mut other_groups_folded = 0usize;
    let mut arrived_keys = BTreeSet::new();
    let mut published_jobs = 0usize;
    let mut settled = false;

    for _step in 0..64 {
        let report = super::super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            active.as_ref(),
        )
        .await
        .expect("maintenance step");
        match report.outcome {
            MetadataReorganizeOutcome::UnitPublished { ref families, .. } => {
                if active.is_some() {
                    assert_ne!(
                        families.as_slice(),
                        group.families(),
                        "no step may merge the group a job is rebuilding"
                    );
                    other_groups_folded += 1;
                }
            }
            MetadataReorganizeOutcome::CompactionPlanned {
                ref families,
                ref spec,
            } => {
                // One job at a time per namespace: a plan that arrives while
                // one runs is reported and skipped, which is what the runner
                // does with it.
                if active.is_none() {
                    assert_eq!(
                        families.as_slice(),
                        group.families(),
                        "this budget starves this group first"
                    );
                    active = Some(spec.clone());
                    if arrived_keys.is_empty() {
                        // A run arrives while the first job runs. It is
                        // outside that job's snapshot, so the job never reads
                        // it and the publication must land underneath it.
                        write_file_bytes(
                            &store,
                            &namespace_id,
                            "/arrived-mid-job.txt",
                            b"arrived while the job ran\n",
                            &context,
                            None,
                        )
                        .await
                        .expect("write a file while the job runs");
                        create_checkpoint(&store, &namespace_id, &context)
                            .await
                            .expect("checkpoint it");
                        arrived_keys =
                            group_segments_outside_the_job(&store, &namespace_id, spec, group)
                                .await;
                        assert!(
                            !arrived_keys.is_empty(),
                            "the arriving run must sit outside the job's input"
                        );
                        visible = visible_namespace(&store, &namespace_id).await;
                    }
                }
            }
            MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
            MetadataReorganizeOutcome::NotNeeded { .. } if active.is_none() => {
                settled = true;
                break;
            }
            MetadataReorganizeOutcome::NotNeeded { .. } => {}
        }

        // Every step leaves a manifest the loader accepts and a read
        // answering exactly what it answered before.
        assert_eq!(
            visible_namespace(&store, &namespace_id).await,
            visible,
            "a step changed what a read answers"
        );

        // The runner has the job running in another task. Waiting a few steps
        // and then running it here is the same interleaving with the timing
        // taken out: the steps in between did ordinary work against a
        // manifest that holds the job's whole input.
        if let Some(spec) = active.clone() {
            steps_with_a_job_running += 1;
            if steps_with_a_job_running % 3 == 0 {
                publish_planned_compaction(&store, &namespace_id, &context, policy, &spec).await;
                published_jobs += 1;
                active = None;
                if published_jobs == 1 {
                    let referenced = referenced_segment_keys(&store, &namespace_id).await;
                    assert!(
                        arrived_keys.is_subset(&referenced),
                        "the run that arrived while the job ran must survive its publication"
                    );
                }
                assert_eq!(
                    visible_namespace(&store, &namespace_id).await,
                    visible,
                    "the job's publication changed what a read answers"
                );
            }
        }
    }

    assert!(published_jobs > 0, "the group must be rebuilt by a job");
    assert!(
        other_groups_folded > 0,
        "ordinary maintenance must keep folding while a job runs"
    );
    assert!(settled, "maintenance must settle with nothing left to fold");

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let base_runs = snapshot_runs_for_group(tables.manifest(), group)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
        .count();
    drop(tables);
    assert_eq!(base_runs, 1, "the group must end in one base run");

    let rows_after = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    assert_eq!(
        unbinds_at_or_below(&rows_after, floor_seq),
        0,
        "every unbind the floor covered must be gone"
    );
    assert!(
        rows_after.values().map(Vec::len).sum::<usize>()
            < rows_before.values().map(Vec::len).sum::<usize>(),
        "the rebuild must reclaim rows"
    );
}

/// Runs steps until one hands a group to a job, and answers with that plan.
///
/// A step folds the group with the most L0 rows, so the starved group is
/// reached after the groups that still fit have folded.
async fn step_until_a_compaction_is_planned<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> MetadataCompactionSpec {
    for _step in 0..64 {
        let report =
            super::super::reorganize_metadata_step(store, namespace_id, context, policy, None)
                .await
                .expect("maintenance step");
        match report.outcome {
            MetadataReorganizeOutcome::CompactionPlanned { spec, .. } => return spec,
            MetadataReorganizeOutcome::UnitPublished { .. } => {}
            other => panic!("expected a plan or a merge, got {other:?}"),
        }
    }
    panic!("no step handed a group to a job")
}

/// The crash variant of the same arc: the job dies mid-run, and the next step
/// plans it again.
///
/// Nothing durable records that a job was running, so a process that dies —
/// or a shutdown that cancels — costs the work and nothing else. What a read
/// answers has not moved, the manifest has not moved, the segments the attempt
/// wrote are staged and named by nothing, and the step after it plans the
/// group again and the second attempt finishes.
#[tokio::test]
async fn a_job_that_dies_mid_run_leaves_orphans_and_the_next_step_plans_it_again() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(
        &LocalFsStore::new(temp_dir.path()).expect("store"),
        &namespace_id,
    )
    .await;
    let group = MetadataFamilyGroup::Bindings;
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let policy = policy_that_starves_the_group(&store, &namespace_id, group).await;

    let spec = step_until_a_compaction_is_planned(&store, &namespace_id, &context, policy).await;
    let visible = visible_namespace(&store, &namespace_id).await;
    let manifest_before = current_manifest_object_id(&store, &namespace_id).await;

    // The attempt dies partway through, which is what a cancellation and a
    // kill leave behind alike: staged segments and nothing else. The
    // cancellation lands after a number of reads rather than at a boundary
    // the test picked, so several thresholds are tried until one lands after
    // the job has written something.
    let mut orphans = BTreeSet::new();
    for reads_before_cancel in [8usize, 16, 24, 32] {
        let cancellation = MetadataCompactionCancellation::default();
        let dying_store = CancelAfterReadsStore {
            inner: LocalFsStore::new(temp_dir.path()).expect("store"),
            cancellation: cancellation.clone(),
            reads_before_cancel,
            reads: AtomicUsize::new(0),
        };
        let outcome = run_metadata_compaction_job(
            &dying_store,
            &namespace_id,
            &context,
            policy,
            &spec,
            &cancellation,
        )
        .await
        .expect("run the job");
        assert_eq!(
            outcome,
            MetadataCompactionJobOutcome::Cancelled,
            "the cancellation must land before the job finishes"
        );
        assert_eq!(
            current_manifest_object_id(&store, &namespace_id).await,
            manifest_before,
            "a job that died must publish nothing"
        );
        assert_eq!(
            visible_namespace(&store, &namespace_id).await,
            visible,
            "and must leave what a read answers exactly where it was"
        );
        orphans.extend(staged_object_keys(&store, &namespace_id).await);
        if !orphans.is_empty() {
            break;
        }
    }
    assert!(
        !orphans.is_empty(),
        "an attempt must leave the segments it had written staged"
    );
    let referenced = referenced_segment_keys(&store, &namespace_id).await;
    assert!(
        orphans.is_disjoint(&referenced),
        "and nothing may reference them"
    );

    // The next step plans the group again, and the second attempt finishes.
    let spec = step_until_a_compaction_is_planned(&store, &namespace_id, &context, policy).await;
    publish_planned_compaction(&store, &namespace_id, &context, policy, &spec).await;

    assert_eq!(
        visible_namespace(&store, &namespace_id).await,
        visible,
        "the rebuild must leave what a read answers where it was"
    );
    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let runs = snapshot_runs_for_group(tables.manifest(), group);
    drop(tables);
    assert_eq!(runs.len(), 1, "the group must end in one run");
    assert_eq!(runs[0].level, CHECKPOINT_BASE_RUN_LEVEL);
    // The first attempt's segments are still staged and still named by
    // nothing: orphans for the collector, not state anything reads.
    let referenced = referenced_segment_keys(&store, &namespace_id).await;
    let staged_now = staged_object_keys(&store, &namespace_id).await;
    assert!(orphans.is_subset(&staged_now));
    assert!(orphans.is_disjoint(&referenced));
}

/// Publishes a competing manifest the first time the finalizer writes its
/// replacement manifest object.
///
/// That is the window the retry is for: the finalizer has reloaded the root
/// and decided its swap, and a flush lands before its compare-and-swap does.
#[derive(Debug)]
struct FlushDuringFinalizationStore {
    inner: LocalFsStore,
    namespace_id: NamespaceId,
    flushed: AtomicUsize,
}

#[async_trait]
impl ObjectStore for FlushDuringFinalizationStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key.ends_with(".manifest.json") && self.flushed.fetch_add(1, Ordering::SeqCst) == 0 {
            super::super::flush::flush_wal(&self.inner, &self.namespace_id, &test_context())
                .await
                .expect("the competing flush must publish");
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

/// A flush that lands while a job is finalizing does not cost the job.
///
/// The finalizer reloads, checks that its input is still exactly what it read,
/// and publishes on top of the flush. The flush's own run survives, because it
/// arrived above the job's snapshot and the swap replaces only what the
/// snapshot held.
#[tokio::test]
async fn a_flush_landing_during_finalization_is_retried_over() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(
        &LocalFsStore::new(temp_dir.path()).expect("store"),
        &namespace_id,
    )
    .await;
    let group = MetadataFamilyGroup::Bindings;
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let MetadataCompactionOutcome::Completed(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };

    // A write with no checkpoint behind it leaves a WAL tail, which is what
    // the competing flush publishes.
    let visible_before = visible_namespace(&store, &namespace_id).await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/raced-the-finalizer.txt",
        b"raced the finalizer\n",
        &context,
        None,
    )
    .await
    .expect("write a file the flush will publish");

    let racing_store = FlushDuringFinalizationStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("store"),
        namespace_id: namespace_id.clone(),
        flushed: AtomicUsize::new(0),
    };
    let manifest_id = match finalize_streaming_compaction(
        &racing_store,
        &namespace_id,
        &spec,
        &snapshot_keys,
        &result,
    )
    .await
    {
        Finalization::Published(manifest_id) => manifest_id,
        other => panic!("the retry must publish, got {other:?}"),
    };
    assert!(
        racing_store.flushed.load(Ordering::SeqCst) > 1,
        "the finalizer must have written a replacement manifest more than once"
    );

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    assert_eq!(tables.manifest().payload.manifest_id, manifest_id);
    let runs = snapshot_runs_for_group(tables.manifest(), group);
    drop(tables);
    // Two runs: the base run the job built, and the delta run the flush
    // published above the job's snapshot. The flush's run survives because
    // the swap replaces only what the snapshot held.
    assert_eq!(
        runs.iter()
            .filter(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
            .count(),
        1,
        "the group must end in one base run"
    );
    assert_eq!(
        runs.iter()
            .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
            .count(),
        1,
        "the flush's run must survive the swap"
    );
    // Nothing the job rebuilt moved, and the file the flush published is
    // there: the swap replaced its own snapshot and preserved the rest.
    let visible_after = visible_namespace(&store, &namespace_id).await;
    assert_eq!(
        visible_after.len(),
        visible_before.len() + 1,
        "the swap must leave the flush's file and nothing else new"
    );
    assert_eq!(
        &visible_after[..visible_before.len()],
        visible_before.as_slice(),
        "the swap must not move anything a read already answered"
    );
    assert!(
        visible_after.last().expect("the flush's file").visible,
        "the file the flush published must be visible after the swap"
    );
}
