//! Rebuilding a family group in one streaming job: the plan split, the
//! oracle that says a streaming job and a whole-group fold reach the same
//! place, restart equivalence, and the resource bounds that make the job
//! independent of the size of what it rebuilds.

use super::super::reorganize::{
    select_reorganization_input, write_reorganized_manifest, ReorganizationPlan,
};
use super::super::row::manifest_row_commit_seq;
use super::super::runs::MetadataFamilyGroup;
use super::super::scan::VerifiedMetadataTables;
use super::super::streaming_compaction::{
    run_metadata_compaction, MetadataCompactionCancellation, MetadataCompactionOutcome,
    MetadataCompactionResult, MetadataCompactionSpec,
};
use super::*;
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

/// The swap a finished job's caller performs, as much of it as this change
/// owns: reload the current manifest, verify every snapshot run is still
/// present and unchanged, replace exactly those descriptors with the output
/// run, and publish through the ordinary manifest path.
///
/// The runner productionizes this — the race handling, the reporting, and the
/// retry are its business. This is the minimum the oracle needs to look at
/// what the job built through a manifest a reader can load.
async fn finalize_streaming_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    snapshot_at_start: &[MetadataRunManifest],
    result: &MetadataCompactionResult,
) -> ManifestId {
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .expect("read root")
        .envelope
        .state;
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .expect("load the current manifest");
    assert_eq!(
        snapshot_runs_for_group(tables.manifest(), spec.group()),
        snapshot_at_start,
        "the snapshot must still be present and unchanged at finalization"
    );

    let previous = tables.manifest();
    let inputs: BTreeSet<(ChangeSeq, u32)> = spec.inputs().iter().copied().collect();
    let mut metadata_files: Vec<MetadataFileRef> = previous
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            !spec.group().contains(descriptor.family)
                || !inputs.contains(&(descriptor.run_seq, descriptor.level))
        })
        .cloned()
        .collect();
    metadata_files.extend(result.output_segments.iter().cloned());
    let base_seq = metadata_files
        .iter()
        .map(|descriptor| descriptor.run_seq)
        .min()
        .unwrap_or(previous.payload.base_seq);
    let retention_floor_seq = previous
        .payload
        .retention_floor_seq
        .max(spec.frozen_floor_seq());

    let manifest = write_reorganized_manifest(
        store,
        namespace_id,
        previous,
        metadata_files,
        base_seq,
        retention_floor_seq,
    )
    .await
    .expect("write the replacement manifest");
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        Some(root.manifest_object_id.clone()),
        test_context().now_ms,
    )
    .await
    .expect("publish the replacement manifest")
    {
        ManifestPublicationOutcome::Published(_) => manifest.payload.manifest_id,
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

/// The step reports the same blocked outcome it reported before the compactor
/// existed. Scheduling the job is the runner's, and lands with it.
#[tokio::test]
async fn a_step_that_plans_a_compaction_still_reports_the_group_blocked() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let before = current_manifest_object_id(&store, &namespace_id).await;

    let report = super::super::reorganize_metadata_step(
        &store,
        &namespace_id,
        &test_context(),
        MetadataLsmPolicy {
            max_l0_runs: NonZeroUsize::MIN,
            max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
            ..MetadataLsmPolicy::default()
        },
    )
    .await
    .expect("budgeted step");
    assert!(matches!(
        report.outcome,
        MetadataReorganizeOutcome::BudgetExhausted { .. }
    ));
    assert_eq!(
        current_manifest_object_id(&store, &namespace_id).await,
        before,
        "a blocked step publishes nothing"
    );
    assert!(
        staged_object_keys(&store, &namespace_id).await.is_empty(),
        "and stages nothing, because no job runs yet"
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
    let snapshot = snapshot_runs_for_group(
        load_current_manifest_tables(&store, &namespace_id)
            .await
            .manifest(),
        group,
    );
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

    finalize_streaming_compaction(&store, &namespace_id, &spec, &snapshot, &result).await;
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
    let snapshot = snapshot_runs_for_group(
        load_current_manifest_tables(&store, &namespace_id)
            .await
            .manifest(),
        group,
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
    assert_eq!(result.unbind_probes, 0, "a floor of zero covers no row");
    finalize_streaming_compaction(&store, &namespace_id, &spec, &snapshot, &result).await;

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
    let snapshot = snapshot_runs_for_group(
        load_current_manifest_tables(&store, &namespace_id)
            .await
            .manifest(),
        group,
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
    assert_eq!(
        result.rows_read, result.rows_written,
        "a revisions rebuild drops nothing"
    );
    assert_eq!(
        result.unbind_probes, 0,
        "the revisions group has no bind rule, so it reads no unbind"
    );
    finalize_streaming_compaction(&store, &namespace_id, &spec, &snapshot, &result).await;

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
    let snapshot = snapshot_runs_for_group(
        load_current_manifest_tables(&straight_store, &namespace_id)
            .await
            .manifest(),
        group,
    );
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
    finalize_streaming_compaction(
        &straight_store,
        &namespace_id,
        &spec,
        &snapshot,
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
    let snapshot = snapshot_runs_for_group(
        load_current_manifest_tables(&store, &namespace_id)
            .await
            .manifest(),
        group,
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
    finalize_streaming_compaction(&store, &namespace_id, &spec, &snapshot, &result).await;

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
    let snapshot = snapshot_runs_for_group(
        load_current_manifest_tables(&store, &namespace_id)
            .await
            .manifest(),
        group,
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
    assert!(
        result.peak_locality_rows * 4 < widest,
        "the peak locality held {} rows against a directory of {widest}",
        result.peak_locality_rows
    );

    finalize_streaming_compaction(&store, &namespace_id, &spec, &snapshot, &result).await;
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
