//! Rebuilding a family group in one streaming job: the plan split, the
//! oracle that says a streaming job and a whole-group fold reach the same
//! place, restart equivalence, and the resource bounds that make the job
//! independent of the size of what it rebuilds.

use super::super::compaction_lease::CompactionLease;
use super::super::reorganize::{
    drop_rows_below_retention_floor, select_reorganization_input, ReorganizationPlan,
};
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
use loonfs_api::wire::manifest::ActiveDeletionRowAction;
use loonfs_objectstore::keys::{metadata_compaction_lease, metadata_compaction_prefix};
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

/// A namespace whose churn all lands on one locality of each kind: one
/// inode's attribute history, one parent-and-name slot's binding
/// generations, and one path deleted and undeleted over and over.
///
/// This is the shape the wide-directory seed does not produce. Many distinct
/// names prove that distinct localities do not accumulate together; only one
/// hot locality proves that the rules inside one do not accumulate either.
async fn seed_one_hot_locality_of_each_kind(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        store,
        namespace_id,
        "/hot/annotated.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write the annotated file");
    // One inode, many attribute revisions. Every one supersedes the last, so
    // the whole history below the floor collapses to one row. The tail is
    // flushed along the way because a namespace may only carry so many
    // unpublished WAL segments.
    for revision in 0..48u64 {
        update_attributes(
            store,
            namespace_id,
            "/hot/annotated.txt",
            "loon.round",
            &revision.to_string(),
            &context,
        )
        .await;
        if revision % 16 == 15 {
            flush::flush_wal(store, namespace_id, &context)
                .await
                .expect("flush the tail");
        }
    }
    // One parent-and-name slot, many binding generations: the same path
    // created and deleted over and over.
    for round in 0..48u64 {
        write_file_bytes(
            store,
            namespace_id,
            "/hot/recreated.txt",
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("recreate the file");
        delete_path(store, namespace_id, "/hot/recreated.txt", &context, None)
            .await
            .expect("delete the file");
        if round % 8 == 7 {
            flush::flush_wal(store, namespace_id, &context)
                .await
                .expect("flush the tail");
        }
    }
    // One deletion generation cancelled by an undelete, which is the pair the
    // active-deletion rule drops together.
    write_file_bytes(
        store,
        namespace_id,
        "/hot/undeleted.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write the undeleted file");
    delete_path(store, namespace_id, "/hot/undeleted.txt", &context, None)
        .await
        .expect("delete the file to undelete");
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the deletion so its listed row is readable");
    undelete_newest_deletion(store, namespace_id, &context).await;
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the churn");
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
        .expect("advance the floor past the churn");

    // A little above the floor, so the job has rows it may not drop.
    write_file_bytes(
        store,
        namespace_id,
        "/hot/late.txt",
        b"late body\n",
        &context,
        None,
    )
    .await
    .expect("write a late file");
    update_attributes(
        store,
        namespace_id,
        "/hot/annotated.txt",
        "loon.round",
        "late",
        &context,
    )
    .await;
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the late writes");
}

/// Publishes one attribute update, which is one more revision for the
/// inode the path resolves to.
async fn update_attributes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    key: &str,
    value: &str,
    context: &MutationContext,
) {
    publish_one_operation(
        store,
        namespace_id,
        FilesystemOperation::UpdateAttributes {
            path: AbsolutePath::parse(path).expect("path"),
            set: [(
                loonfs_api::AttributeKey::parse(key).expect("attribute key"),
                loonfs_api::AttributeValue::String {
                    value: value.to_owned(),
                },
            )]
            .into_iter()
            .collect(),
            remove: Vec::new(),
            expected_inode_id: None,
            expected_attributes_revision_no: None,
        },
        context,
    )
    .await;
}

/// Cancels the namespace's newest recoverable deletion, which writes the
/// removal marker the active-deletion rule pairs with the listed row.
///
/// The deletion is read back out of the manifest rather than remembered from
/// the commit response, because the row is what names the generation the
/// undelete has to cancel.
async fn undelete_newest_deletion<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    let rows =
        group_rows_of_current_manifest(store, namespace_id, MetadataFamilyGroup::ActiveDeletions)
            .await;
    let (root_inode_id, deleted_at_seq) = rows[&ApiMetadataTableFamily::ActiveDeletions]
        .iter()
        .filter_map(|row| match row {
            MetadataRow::ActiveDeletion {
                root_inode_id,
                deleted_at_seq,
                action: ActiveDeletionRowAction::Listed { .. },
            } => Some((*root_inode_id, *deleted_at_seq)),
            _ => None,
        })
        .max_by_key(|(_, deleted_at_seq)| *deleted_at_seq)
        .expect("the namespace holds a recoverable deletion");
    publish_one_operation(
        store,
        namespace_id,
        FilesystemOperation::Undelete {
            inode_id: root_inode_id,
            deleted_at_seq,
            path: None,
        },
        context,
    )
    .await;
}

async fn publish_one_operation<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    operation: FilesystemOperation,
    context: &MutationContext,
) {
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![CommitCandidate::prepared(
                CommitRequest::single(CommitId::generate(), None, operation),
                Vec::new(),
            )],
            context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("publish the operation");
}

// -------------------------------------------------------------------------
// Driving a job
// -------------------------------------------------------------------------

/// The view a step sees while `spec`'s job is rebuilding its group.
fn rebuilding(spec: &MetadataCompactionSpec) -> MetadataCompactionView<'_> {
    MetadataCompactionView {
        active: Some(spec),
        blocked_engagements: &[],
    }
}

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
        false,
    )
    .await
    .expect("plan the group");
    match selection.plan {
        Some(ReorganizationPlan::FullCompaction(spec)) => spec,
        _ => panic!("a group no budget admits must plan as a streaming compaction"),
    }
}

/// The claim a job holds while it runs, built the way the job driver builds
/// it. These tests split the rebuild from the driver, so they open the claim
/// themselves.
fn test_lease<'a>(
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    timer: &'a StdMonotonicTimer,
) -> CompactionLease<'a> {
    let context = test_context();
    CompactionLease::new(
        namespace_id,
        spec.job_id(),
        &context.writer_id,
        context.now_ms,
        timer,
    )
}

async fn run_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    policy: MetadataLsmPolicy,
    cancellation: &MetadataCompactionCancellation,
) -> MetadataCompactionOutcome {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    let timer = StdMonotonicTimer::default();
    let mut lease = test_lease(namespace_id, spec, &timer);
    run_metadata_compaction(
        &tables,
        namespace_id,
        spec,
        policy,
        cancellation,
        &mut lease,
    )
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
    finalize_streaming_compaction_under(
        store,
        namespace_id,
        spec,
        snapshot_keys,
        result,
        &MetadataCompactionCancellation::default(),
    )
    .await
}

/// The same, with the token the tests that cancel a finalization set.
async fn finalize_streaming_compaction_under<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    snapshot_keys: &BTreeSet<String>,
    result: &MetadataCompactionResult,
    cancellation: &MetadataCompactionCancellation,
) -> Finalization {
    let timer = StdMonotonicTimer::default();
    let mut lease = test_lease(namespace_id, spec, &timer);
    finalize_metadata_compaction(
        store,
        namespace_id,
        spec,
        snapshot_keys,
        result.clone(),
        cancellation,
        &mut lease,
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
        .list_prefix_stream(&metadata_compaction_prefix(namespace_id.as_str()))
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
        MetadataCompactionView::default(),
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

    let generous =
        select_reorganization_input(&tables, group, fold_everything_policy(), floor_seq, false)
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
        false,
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
        MetadataCompactionView::default(),
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
        MetadataCompactionView::default(),
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
            rebuilding(&spec),
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

/// A group whose bottom-anchored window is blocked still merges the delta
/// runs above the frozen base — and under sustained writes there is always
/// another pair of them, so a planner reading one step's view alone would take
/// that merge forever and never start the job.
///
/// The runtime counts those engagements, and after
/// `DELTA_MERGES_OVER_A_FROZEN_BASE` of them the planner starts the job
/// regardless of what delta window is available. Here every step publishes a
/// fresh delta run before the next one plans, which is the write pattern that
/// used to postpone the job indefinitely.
#[tokio::test]
async fn sustained_delta_runs_cannot_postpone_a_blocked_groups_job() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_one_wide_directory(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let policy = policy_that_starves_the_group(&store, &namespace_id, group).await;

    let mut engagements = 0u32;
    let mut delta_merges_over_the_frozen_base = 0usize;
    let mut planned: Option<MetadataCompactionSpec> = None;
    for round in 0..16u64 {
        // A new delta run before every step, so the planner always has two
        // runs above the base to merge.
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/wide/arrival-{round}.txt"),
            b"arrival\n",
            &context,
            None,
        )
        .await
        .expect("write a file");
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the write");

        let blocked = [(group.families(), engagements)];
        let report = super::super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            MetadataCompactionView {
                active: None,
                blocked_engagements: &blocked,
            },
        )
        .await
        .expect("maintenance step");
        match report.outcome {
            MetadataReorganizeOutcome::CompactionPlanned { ref families, .. } => {
                assert_eq!(families.as_slice(), group.families());
                planned = match report.outcome {
                    MetadataReorganizeOutcome::CompactionPlanned { spec, .. } => Some(spec),
                    _ => unreachable!(),
                };
                break;
            }
            MetadataReorganizeOutcome::UnitPublished {
                ref families,
                bottom_anchored_merge_blocked,
                ..
            } if families.as_slice() == group.families() => {
                assert!(
                    bottom_anchored_merge_blocked,
                    "this budget starves the group's base, so every merge of it runs above one"
                );
                engagements += 1;
                delta_merges_over_the_frozen_base += 1;
            }
            _ => {}
        }
    }

    assert!(
        planned.is_some(),
        "the job must start while delta runs keep arriving"
    );
    assert_eq!(
        delta_merges_over_the_frozen_base,
        super::super::reorganize::DELTA_MERGES_OVER_A_FROZEN_BASE as usize,
        "the group merges deltas over its frozen base exactly that many times before the job"
    );
}

/// Between jobs, the ordinary delta merge is what consolidates new runs.
///
/// Starting a job for every batch of delta runs would reread the whole group
/// to fold in a sliver, which is the amortization the counter exists to keep.
/// A group whose base is not frozen never counts an engagement at all, and a
/// group whose base is frozen spends its budget on merges first.
#[tokio::test]
async fn small_delta_batches_are_consolidated_by_merges_rather_than_by_jobs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };

    // Budgets this group fits: every merge starts at the group's oldest run,
    // so nothing is ever blocked and no engagement is ever counted.
    let mut merges = 0usize;
    for _ in 0..16 {
        let report = super::super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            MetadataCompactionView::default(),
        )
        .await
        .expect("maintenance step");
        match report.outcome {
            MetadataReorganizeOutcome::UnitPublished {
                ref families,
                bottom_anchored_merge_blocked,
                ..
            } => {
                if families.as_slice() == group.families() {
                    assert!(
                        !bottom_anchored_merge_blocked,
                        "a group whose window fits is never merging over a frozen base"
                    );
                    merges += 1;
                }
            }
            MetadataReorganizeOutcome::NotNeeded { .. } => break,
            other => panic!("unexpected outcome {other:?}"),
        }
    }
    assert!(merges > 0, "the ordinary merge must do this group's work");

    // A fresh delta batch arrives. It folds through the ordinary merge, and
    // the planner never reaches for a job.
    for round in 0..3u64 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/steady-{round}.txt"),
            b"steady\n",
            &context,
            None,
        )
        .await
        .expect("write a file");
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the write");
    }
    for _ in 0..16 {
        let report = super::super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            MetadataCompactionView::default(),
        )
        .await
        .expect("maintenance step");
        match report.outcome {
            MetadataReorganizeOutcome::UnitPublished { .. } => {}
            MetadataReorganizeOutcome::NotNeeded { .. } => break,
            other => panic!("a group whose window fits must never plan a job, got {other:?}"),
        }
    }
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
            .starts_with(&metadata_compaction_prefix(namespace_id.as_str()))),
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
// The job's claim on its own output
// -------------------------------------------------------------------------

/// A job writes its lease before its first output object and deletes it after
/// publishing, and the segments it published stay where they are.
///
/// The lease is what tells a collector that an unreferenced staged object
/// belongs to a job that is still running. Once the manifest names those
/// objects they are live wherever they sit, so the claim has nothing left to
/// protect.
#[tokio::test]
async fn a_job_claims_its_prefix_while_it_runs_and_drops_the_claim_when_it_publishes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let policy = policy_that_starves_the_group(&store, &namespace_id, group).await;
    let spec = step_until_a_compaction_is_planned(&store, &namespace_id, &context, policy).await;
    let lease_key = metadata_compaction_lease(namespace_id.as_str(), spec.job_id().as_str());

    let outcome = run_metadata_compaction_job(
        &store,
        &namespace_id,
        &context,
        policy,
        &spec,
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("run the job");
    assert!(matches!(
        outcome,
        MetadataCompactionJobOutcome::Published { .. }
    ));

    // Every object the job wrote sits under its own prefix, and the manifest
    // now names the segments.
    let staged = staged_object_keys(&store, &namespace_id).await;
    let referenced = referenced_segment_keys(&store, &namespace_id).await;
    let job_prefix = format!(
        "{}{}/",
        metadata_compaction_prefix(namespace_id.as_str()),
        spec.job_id().as_str()
    );
    assert!(
        staged.iter().all(|key| key.starts_with(&job_prefix)),
        "every object a job writes belongs to that job's prefix: {staged:?}"
    );
    assert!(
        !staged.is_empty() && staged.iter().all(|key| referenced.contains(key)),
        "the manifest must name the segments the job published"
    );
    assert!(
        !staged.contains(&lease_key),
        "a published job deletes its own lease"
    );
    assert!(store
        .head(&lease_key)
        .await
        .expect("head the lease")
        .is_none());
}

/// A cancelled job leaves its lease behind, which is what keeps a collector
/// off the segments it had written until the lease expires.
#[tokio::test]
async fn a_cancelled_job_leaves_its_claim_standing() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(
        &LocalFsStore::new(temp_dir.path()).expect("store"),
        &namespace_id,
    )
    .await;
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let group = MetadataFamilyGroup::Bindings;
    let policy = policy_that_starves_the_group(&store, &namespace_id, group).await;
    let spec = step_until_a_compaction_is_planned(&store, &namespace_id, &context, policy).await;
    let lease_key = metadata_compaction_lease(namespace_id.as_str(), spec.job_id().as_str());

    let cancellation = MetadataCompactionCancellation::default();
    let dying_store = CancelAfterReadsStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("store"),
        cancellation: cancellation.clone(),
        reads_before_cancel: 8,
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
    assert_eq!(outcome, MetadataCompactionJobOutcome::Cancelled);
    assert!(
        store
            .head(&lease_key)
            .await
            .expect("head the lease")
            .is_some(),
        "a cancelled job's claim stands until it expires"
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
    // The retention operators hold answers, not rows. The bindings operator
    // holds one bind row until its generation's unbinds have arrived, and no
    // other operator holds a row at all.
    assert!(
        result.peak_operator_rows <= 1,
        "a retention operator held {} rows",
        result.peak_operator_rows
    );
}

/// One directory far past the old per-step row budget streams through with
/// the same bounded state. The case that needed a durable offset inside a
/// partition needs nothing now: the operators hold one row at most, whatever
/// the directory holds.
#[tokio::test]
async fn one_directory_far_past_the_row_budget_streams_a_row_at_a_time() {
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
        result.peak_operator_rows <= 1,
        "a retention operator held {} rows against a directory of {widest}",
        result.peak_operator_rows
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

/// Every group of a namespace whose churn all lands on one hot locality
/// rebuilds to the fold's rows while its operators hold at most one row.
///
/// The wide-directory case above proves that distinct localities do not
/// accumulate together. This proves the other half: one inode's attribute
/// history, one name slot's binding generations, and one deletion's markers
/// are each decided by fixed state rather than by holding the locality.
#[tokio::test]
async fn one_hot_locality_of_each_kind_rebuilds_with_fixed_operator_state() {
    let job_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(job_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_one_hot_locality_of_each_kind(&store, &namespace_id).await;
    let frozen_floor_seq = read_floor_seq(&store, &namespace_id).await;

    let fold_dir = tempdir().expect("tempdir");
    copy_store_tree(job_dir.path(), fold_dir.path());
    let fold_store = LocalFsStore::new(fold_dir.path()).expect("store");

    for group in REORGANIZE_FAMILY_GROUPS {
        let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
        if before.values().all(Vec::is_empty) {
            continue;
        }
        // The same durable bytes, folded the ordinary way.
        let folded_before = group_rows_of_current_manifest(&fold_store, &namespace_id, group).await;
        assert_eq!(
            folded_before, before,
            "both rebuilds must start from the same rows"
        );
        let mut folded = folded_before.clone();
        for family in group.families() {
            folded.entry(*family).or_default();
        }
        drop_rows_below_retention_floor(&mut folded, frozen_floor_seq)
            .expect("fold the group whole");
        for (family, rows) in &mut folded {
            rows.sort_by_key(|row| row.row_key_for_family(*family));
        }

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
            result.peak_operator_rows <= 1,
            "a {group:?} retention operator held {} rows",
            result.peak_operator_rows
        );
        publish_streaming_compaction(&store, &namespace_id, &spec, &snapshot_keys, &result).await;

        let compacted = group_rows_of_current_manifest(&store, &namespace_id, group).await;
        assert_eq!(
            compacted, folded,
            "a streaming rebuild of {group:?} must keep the rows a whole-group fold keeps"
        );
        // Nothing above the floor may go, whatever the rules dropped below it.
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

    // The hot localities really did collapse: the group with an attribute
    // history and the group with a name slot both hold fewer rows than they
    // did, and the two bind families stayed in lockstep.
    let attributes =
        group_rows_of_current_manifest(&store, &namespace_id, MetadataFamilyGroup::Attributes)
            .await;
    assert_eq!(
        attributes[&ApiMetadataTableFamily::Attributes].len(),
        2,
        "one inode's history must collapse to the newest row at the floor plus the late one"
    );
    let bindings =
        group_rows_of_current_manifest(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    assert_eq!(
        bindings[&ApiMetadataTableFamily::DirentryBinds].len(),
        bindings[&ApiMetadataTableFamily::DirentryChildBinds].len(),
    );
    assert!(
        bindings[&ApiMetadataTableFamily::DirentryUnbinds].is_empty(),
        "every unbind the floor covered must be gone"
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
            active.as_ref().map_or_else(
                MetadataCompactionView::default,
                |spec: &MetadataCompactionSpec| rebuilding(spec),
            ),
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
        let report = super::super::reorganize_metadata_step(
            store,
            namespace_id,
            context,
            policy,
            MetadataCompactionView::default(),
        )
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

/// When the namespace's root was last published.
async fn root_updated_at_ms<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) -> u64 {
    read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state
        .updated_at_ms
}

/// The root's timestamp says when the publication landed, not when the job
/// that produced it started.
///
/// A job holds its writer identity for as long as it runs, and a long one
/// finishes hours after the context it was planned under was built. Stamping
/// the root with that context would make a job that rebases over a newer
/// flush move the namespace's `updated_at_ms` backwards.
#[tokio::test]
async fn a_rebased_publication_never_moves_the_root_timestamp_backward() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
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

    // A flush lands while the job runs, stamping the root a minute past the
    // context the job carries.
    write_file_bytes(
        &store,
        &namespace_id,
        "/landed-while-the-job-ran.txt",
        b"landed while the job ran\n",
        &context,
        None,
    )
    .await
    .expect("write a file the flush will publish");
    let later = mutation_context(&context.writer_id, context.now_ms + 60_000);
    flush::flush_wal(&store, &namespace_id, &later)
        .await
        .expect("the flush must publish");
    let flushed_at_ms = root_updated_at_ms(&store, &namespace_id).await;
    assert!(
        flushed_at_ms >= later.now_ms,
        "the flush must stamp the root with its own time"
    );

    publish_streaming_compaction(&store, &namespace_id, &spec, &snapshot_keys, &result).await;

    let published_at_ms = root_updated_at_ms(&store, &namespace_id).await;
    assert!(
        published_at_ms > context.now_ms,
        "the root must carry publication time, not the time the job was planned at"
    );
    assert!(
        published_at_ms >= flushed_at_ms,
        "a job rebased over a newer flush must not move the root's timestamp backwards"
    );
}

/// A token set after the last row costs the job its publication.
///
/// The executor finished, so there is a whole rebuilt group in memory and
/// staged output on the store. None of it lands: the manifest stays where it
/// was, and the staged segments stay orphans a later pass reclaims.
#[tokio::test]
async fn a_cancellation_after_the_last_row_publishes_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let cancellation = MetadataCompactionCancellation::default();
    let MetadataCompactionOutcome::Completed(result) = run_compaction(
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
    let manifest_before = current_manifest_object_id(&store, &namespace_id).await;
    let visible_before = visible_namespace(&store, &namespace_id).await;

    cancellation.cancel();
    let finalization = finalize_streaming_compaction_under(
        &store,
        &namespace_id,
        &spec,
        &snapshot_keys,
        &result,
        &cancellation,
    )
    .await;

    assert!(
        matches!(finalization, Finalization::Cancelled),
        "a cancelled finalization must publish nothing, got {finalization:?}"
    );
    assert_eq!(
        current_manifest_object_id(&store, &namespace_id).await,
        manifest_before,
        "the manifest must be where the job found it"
    );
    assert_eq!(
        visible_namespace(&store, &namespace_id).await,
        visible_before,
        "and a read must answer exactly what it answered before"
    );
    let staged = staged_object_keys(&store, &namespace_id).await;
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

/// Fails every root compare-and-swap, and cancels the job the first time it
/// tries one.
///
/// That is the shape a shutdown takes during finalization: attempts are still
/// available, the races are still there to take, and the job must stop taking
/// them.
#[derive(Debug)]
struct CancelAtTheFirstPublicationStore {
    inner: LocalFsStore,
    cancellation: MetadataCompactionCancellation,
    manifests: AtomicUsize,
}

#[async_trait]
impl ObjectStore for CancelAtTheFirstPublicationStore {
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
        if key.ends_with(".manifest.json") {
            self.manifests.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(mode, PutMode::CompareAndSwap { .. }) {
            self.cancellation.cancel();
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
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

/// A shutdown during finalization does not wait out the attempts it has left.
///
/// The first attempt loses its race and the token is set while it does. The
/// attempts after it are exactly the wait a shutdown must not sit through, so
/// the job stops at the top of the next one rather than building three more
/// manifests to lose three more races with.
#[tokio::test]
async fn a_cancelled_finalization_does_not_take_the_races_it_has_left() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let cancellation = MetadataCompactionCancellation::default();
    let MetadataCompactionOutcome::Completed(result) = run_compaction(
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
    let manifest_before = current_manifest_object_id(&store, &namespace_id).await;

    let cancelling_store = CancelAtTheFirstPublicationStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("store"),
        cancellation: cancellation.clone(),
        manifests: AtomicUsize::new(0),
    };
    let finalization = finalize_streaming_compaction_under(
        &cancelling_store,
        &namespace_id,
        &spec,
        &snapshot_keys,
        &result,
        &cancellation,
    )
    .await;

    assert!(
        matches!(finalization, Finalization::Cancelled),
        "the cancelled attempt must not be retried, got {finalization:?}"
    );
    assert_eq!(
        cancelling_store.manifests.load(Ordering::SeqCst),
        1,
        "only the attempt that was running when the token was set may build a manifest"
    );
    assert_eq!(
        current_manifest_object_id(&store, &namespace_id).await,
        manifest_before,
        "and nothing may be published"
    );
}
