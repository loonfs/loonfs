//! Rebuilding a family group in one background job: the plan split, the
//! oracle that says the job's orchestration and a maintenance pass's reach the
//! same place, restart equivalence, and the resource bounds that make a merge
//! independent of the size of what it merges.

use super::super::compaction_lease::{
    claim_group_lease, load_group_lease, CompactionLease, CompactionPrefixOwner, LeaseHold,
};
use super::super::reorganize::{
    group_run_descriptors, select_reorganization_input, FrozenBasePolicy, ReorganizationPlan,
};
use super::super::row::manifest_row_commit_seq;
use super::super::runs::MetadataFamilyGroup;
use super::super::scan::VerifiedMetadataSegments;
use super::super::streaming_compaction::{
    finalize_metadata_compaction, merge_group_in_step, run_metadata_compaction,
    snapshot_segment_keys, MetadataCompactionCancellation, MetadataCompactionJobOutcome,
    MetadataCompactionSpec, MetadataMergeResult,
};
use super::*;
use crate::limits::METADATA_COMPACTION_LEASE_EXPIRY_MS;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::ActiveDeletionRowAction;
use loonfs_objectstore::keys::{metadata_compaction_lease, metadata_compaction_prefix};
use loonfs_test_support::stores::{
    BlockingStore, ConcurrencyWatchStore, KeyPredicate, OperationClass,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// -------------------------------------------------------------------------
// The family-group mapping
// -------------------------------------------------------------------------

#[test]
fn every_family_belongs_to_exactly_one_reorganization_group() {
    for family in CHECKPOINT_ROW_FAMILIES {
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
        CHECKPOINT_ROW_FAMILIES.len(),
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
                loonfs_api::AttributeValue::parse(value).expect("attribute value"),
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
    let (root_inode_id, deletion_seq) = rows[&ApiMetadataRowFamily::ActiveDeletions]
        .iter()
        .filter_map(|row| match row {
            MetadataRow::ActiveDeletion(crate::metadata::ActiveDeletionRecord {
                root_inode_id,
                deletion_seq,
                action: ActiveDeletionRowAction::Listed { .. },
            }) => Some((*root_inode_id, *deletion_seq)),
            _ => None,
        })
        .max_by_key(|(_, deletion_seq)| *deletion_seq)
        .expect("the namespace holds a recoverable deletion");
    publish_one_operation(
        store,
        namespace_id,
        FilesystemOperation::Undelete {
            inode_id: root_inode_id,
            deletion_seq,
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
                CommitRequest::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    operation,
                ),
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

/// A budget that admits the whole group in one step, so the ordinary fold can
/// rebuild in one unit whatever the streaming job did incrementally.
fn fold_everything_policy() -> MetadataLsmPolicy {
    MetadataLsmPolicy {
        max_delta_runs: NonZeroUsize::MIN,
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
        max_delta_runs: NonZeroUsize::MIN,
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
    let segments = load_current_manifest_segments(store, namespace_id).await;
    let base_rows: u64 = runs_in_reorganization_order(&segments.manifest().payload)
        .iter()
        .filter(|run| run.tier == RunTier::Base)
        .flat_map(|run| run.segments.iter())
        .filter(|family_segments| group.families().contains(&family_segments.family))
        .flat_map(|family_segments| &family_segments.segments)
        .map(|descriptor| descriptor.row_count)
        .sum();
    assert!(
        base_rows > 1,
        "the seed must leave this group a base run to starve"
    );
    MetadataLsmPolicy {
        max_delta_runs: NonZeroUsize::MIN,
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

/// Loads the segments referenced by the current root manifest.
async fn load_current_manifest_segments<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> VerifiedMetadataSegments<'a, S> {
    let manifest_object_id = current_manifest_object_id(store, namespace_id).await;
    load_manifest_segments_for_inspection(store, None, namespace_id, &manifest_object_id)
        .await
        .expect("load the current manifest's segments")
}

/// The runs a job snapshots: every run of the manifest that holds rows of the
/// group, which is what makes the input bottom-anchored and its drops legal.
fn snapshot_runs_for_group(
    manifest: &NamespaceManifestEnvelope,
    group: MetadataFamilyGroup,
) -> Vec<MetadataRunManifest> {
    runs_in_reorganization_order(&manifest.payload)
        .into_iter()
        .filter(|run| {
            run.segments.iter().any(|family_segments| {
                group.families().contains(&family_segments.family)
                    && !family_segments.segments.is_empty()
            })
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
    let segments = load_current_manifest_segments(store, namespace_id).await;
    let frozen_floor_seq = read_floor_seq(store, namespace_id).await;
    // One byte admits no run whole, so the planner has no window that makes
    // progress and answers with the compaction plan for this group.
    let selection = select_reorganization_input(
        &segments,
        group,
        MetadataLsmPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
            ..MetadataLsmPolicy::default()
        },
        frozen_floor_seq,
        &FrozenBasePolicy::default(),
    )
    .await
    .expect("plan the group");
    match selection.plan {
        ReorganizationPlan::FullCompaction(spec) => spec,
        _ => panic!("a group no budget admits must plan as a streaming compaction"),
    }
}

/// The claim a job holds while it runs, opened the way the job driver opens
/// it. These tests split the rebuild from the driver, so they create the
/// lease themselves — every later write compare-and-swaps against the etag
/// this one returns, so a lease that was never created cannot be refreshed.
async fn test_lease<'a, S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    timer: &'a dyn MonotonicTimer,
) -> CompactionLease<'a> {
    let context = test_context();
    CompactionLease::open_for_test(
        store,
        namespace_id,
        spec,
        &context.writer_id,
        context.now_ms,
        timer,
    )
    .await
    .expect("open the job's lease")
}

async fn write_active_lease<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    expires_at_ms: u64,
) {
    write_lease_in_state(
        store,
        namespace_id,
        spec,
        expires_at_ms,
        loonfs_api::wire::control::CompactionLeaseStatus::Active {},
    )
    .await;
}

async fn write_lease_in_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    expires_at_ms: u64,
    status: loonfs_api::wire::control::CompactionLeaseStatus,
) {
    let state = loonfs_api::wire::control::MetadataCompactionLeaseState {
        job_id: spec.job_id().clone(),
        namespace_id: namespace_id.clone(),
        group: spec.group(),
        writer_id: loonfs_api::WriterId::parse("test-writer").expect("writer id"),
        status,
        started_at_ms: 1_000,
        expires_at_ms,
    };
    let bytes = loonfs_api::wire::control::encode_control_state(
        loonfs_api::wire::control::ControlObjectKind::CompactionLease,
        &state,
    )
    .expect("encode lease");
    store
        .put_overwrite(
            &metadata_compaction_lease(namespace_id, spec.group()),
            Bytes::from(bytes),
        )
        .await
        .expect("write lease");
}

async fn run_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    policy: MetadataLsmPolicy,
    cancellation: &MetadataCompactionCancellation,
) -> std::result::Result<MetadataMergeResult, MetadataCompactionJobOutcome> {
    let segments = load_current_manifest_segments(store, namespace_id).await;
    let timer = StdMonotonicTimer::default();
    let mut lease = test_lease(store, namespace_id, spec, &timer).await;
    run_metadata_compaction(
        &segments,
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
    let segments = load_current_manifest_segments(store, namespace_id).await;
    snapshot_segment_keys(&segments, spec).expect("the snapshot must be present")
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
    result: &MetadataMergeResult,
) -> MetadataCompactionJobOutcome {
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
    result: &MetadataMergeResult,
    cancellation: &MetadataCompactionCancellation,
) -> MetadataCompactionJobOutcome {
    let timer = StdMonotonicTimer::default();
    let mut lease = test_lease(store, namespace_id, spec, &timer).await;
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
    result: &MetadataMergeResult,
) -> ManifestNo {
    match finalize_streaming_compaction(store, namespace_id, spec, snapshot_keys, result).await {
        MetadataCompactionJobOutcome::Published { manifest_no, .. } => manifest_no,
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
) -> BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> {
    let segments = load_current_manifest_segments(store, namespace_id).await;
    group_rows_from_manifest(&segments, group).await
}

async fn current_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> MetadataState {
    load_manifest_materialization_for_inspection(
        store,
        namespace_id,
        load_metadata_root_object(store, namespace_id)
            .await
            .expect("read root")
            .state
            .manifest
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

async fn staged_object_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    use futures::StreamExt;
    store
        .list_prefix_stream(&metadata_compaction_prefix(namespace_id))
        .map(|key| key.expect("list staged objects"))
        .collect()
        .await
}

/// Merges the group inside one maintenance pass with the budgets raised, which
/// is the other way of running the same engine.
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
        FrozenBasePolicy::default(),
    )
    .await
    .expect("fold the group whole");
    let MetadataReorganizeOutcome::UnitPublished { group: folded, .. } = report.outcome else {
        panic!("the whole-group fold must publish a unit");
    };
    assert_eq!(
        folded, group,
        "both rebuilds must work on the same family group"
    );
}

// -------------------------------------------------------------------------
// The plan split
// -------------------------------------------------------------------------

#[tokio::test]
async fn the_planner_answers_with_a_merge_or_with_a_compaction() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    let segments = load_current_manifest_segments(&store, &namespace_id).await;

    let generous = select_reorganization_input(
        &segments,
        group,
        fold_everything_policy(),
        floor_seq,
        &FrozenBasePolicy::default(),
    )
    .await
    .expect("plan under generous budgets");
    assert!(
        matches!(generous.plan, ReorganizationPlan::BoundedMerge(_)),
        "a window that fits is a bounded merge"
    );
    assert!(generous.group_bottom_over_budget.is_none());

    let starved = select_reorganization_input(
        &segments,
        group,
        MetadataLsmPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
            ..MetadataLsmPolicy::default()
        },
        floor_seq,
        &FrozenBasePolicy::default(),
    )
    .await
    .expect("plan under budgets nothing fits");
    let ReorganizationPlan::FullCompaction(spec) = starved.plan else {
        panic!("a group no window fits must plan as a streaming compaction");
    };
    assert!(
        starved.group_bottom_over_budget.is_some(),
        "the operator still hears that the group's oldest run is over budget"
    );
    assert_eq!(spec.group(), group);
    assert_eq!(spec.frozen_floor_seq(), floor_seq);
    let snapshot = snapshot_runs_for_group(segments.manifest(), group);
    assert_eq!(
        spec.inputs().iter().copied().collect::<BTreeSet<_>>(),
        snapshot
            .iter()
            .map(|run| run.run_no)
            .collect::<BTreeSet<_>>(),
        "a compaction takes every run the group holds, which is what anchors it at the bottom"
    );
    assert!(
        snapshot.len() > 1,
        "this workload must leave a base under at least one delta run"
    );
}

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
        FrozenBasePolicy::default(),
    )
    .await
    .expect("budgeted step");
    let MetadataReorganizeOutcome::CompactionPlanned { group, spec } = report.outcome else {
        panic!("a group no window fits must plan a streaming compaction");
    };
    assert_eq!(group, MetadataFamilyGroup::Bindings);
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

#[tokio::test]
async fn held_group_leases_are_skipped_and_an_expired_lease_is_available() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let policy = MetadataLsmPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    let context = mutation_context(
        "test-writer",
        METADATA_COMPACTION_LEASE_EXPIRY_MS.saturating_add(10_000),
    );
    let active_dir = tempdir().expect("tempdir");
    let reaping_dir = tempdir().expect("tempdir");
    let expired_dir = tempdir().expect("tempdir");
    copy_store_tree(temp_dir.path(), active_dir.path());
    copy_store_tree(temp_dir.path(), reaping_dir.path());
    copy_store_tree(temp_dir.path(), expired_dir.path());
    let active_store = LocalFsStore::new(active_dir.path()).expect("store");
    let reaping_store = LocalFsStore::new(reaping_dir.path()).expect("store");
    let expired_store = LocalFsStore::new(expired_dir.path()).expect("store");

    write_active_lease(
        &active_store,
        &namespace_id,
        &spec,
        context
            .now_ms
            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS),
    )
    .await;
    let active = super::super::reorganize_metadata_step(
        &active_store,
        &namespace_id,
        &context,
        policy,
        FrozenBasePolicy::default(),
    )
    .await
    .expect("step with active lease");
    let MetadataReorganizeOutcome::UnitPublished {
        group: active_group,
        ..
    } = active.outcome
    else {
        panic!("expected another group to merge, got {:?}", active.outcome);
    };
    assert_ne!(
        active_group, group,
        "the active lease excludes its family group"
    );

    write_lease_in_state(
        &reaping_store,
        &namespace_id,
        &spec,
        0,
        loonfs_api::wire::control::CompactionLeaseStatus::Reaping {},
    )
    .await;
    let reaping = super::super::reorganize_metadata_step(
        &reaping_store,
        &namespace_id,
        &context,
        policy,
        FrozenBasePolicy::default(),
    )
    .await
    .expect("step with reaping lease");
    let MetadataReorganizeOutcome::UnitPublished {
        group: reaping_group,
        ..
    } = reaping.outcome
    else {
        panic!("expected another group to merge, got {:?}", reaping.outcome);
    };
    assert_ne!(
        reaping_group, group,
        "the reaping lease excludes its family group"
    );

    write_active_lease(&expired_store, &namespace_id, &spec, 0).await;
    let expired = super::super::reorganize_metadata_step(
        &expired_store,
        &namespace_id,
        &context,
        policy,
        FrozenBasePolicy::default(),
    )
    .await
    .expect("step with expired lease");
    let MetadataReorganizeOutcome::UnitPublished {
        group: expired_group,
        ..
    } = expired.outcome
    else {
        panic!(
            "expected the leased group to merge, got {:?}",
            expired.outcome
        );
    };
    assert_eq!(
        expired_group, group,
        "an expired lease does not exclude its family group"
    );
}

#[tokio::test]
async fn sustained_delta_runs_cannot_postpone_a_blocked_groups_job() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_one_wide_directory(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let policy = policy_that_starves_the_group(&store, &namespace_id, group).await;

    let mut delta_merges_over_the_frozen_base = 0u32;
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

        let report = super::super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            FrozenBasePolicy::Amortized,
        )
        .await
        .expect("maintenance pass");
        match report.outcome {
            MetadataReorganizeOutcome::CompactionPlanned {
                group: planned_group,
                spec,
            } => {
                assert_eq!(planned_group, group);
                planned = Some(spec);
                break;
            }
            MetadataReorganizeOutcome::UnitPublished {
                group: folded,
                bottom_anchored_merge_blocked,
                ..
            } if folded == group => {
                assert!(
                    bottom_anchored_merge_blocked,
                    "this budget starves the group's base, so every merge of it runs above one"
                );
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
        super::super::reorganize::DELTA_MERGES_OVER_A_FROZEN_BASE,
        "the group merges deltas over its frozen base exactly that many times before the job"
    );
}

#[tokio::test]
async fn small_delta_batches_are_consolidated_by_merges_rather_than_by_jobs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let policy = MetadataLsmPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };

    // Budgets this group fits: every merge starts at the group's oldest run,
    // so nothing is ever blocked and no merge over a frozen base is counted.
    let mut merges = 0usize;
    for _ in 0..16 {
        let report = super::super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            FrozenBasePolicy::default(),
        )
        .await
        .expect("maintenance pass");
        match report.outcome {
            MetadataReorganizeOutcome::UnitPublished {
                group: folded,
                bottom_anchored_merge_blocked,
                ..
            } => {
                if folded == group {
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

    // A new delta batch can be handled by a normal merge without planning a
    // streaming compaction job.
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
            FrozenBasePolicy::default(),
        )
        .await
        .expect("maintenance pass");
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

#[tokio::test]
async fn a_background_compaction_and_a_step_contained_merge_reach_the_same_rows() {
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
                .starts_with(&metadata_compaction_prefix(&namespace_id))
        }),
        "every output segment is written to the staging directory"
    );
    // Every reverse row the floor covers costs one point read, and no other
    // row costs one. That is what bounds the reads a job makes.
    let reverse_rows_at_or_below_floor = before[&ApiMetadataRowFamily::DirentryChildBinds]
        .iter()
        .filter(|row| {
            matches!(row, MetadataRow::DirentryBind (crate::metadata::DirentryBindRecord { bind_seq, .. }) if *bind_seq <= frozen_floor_seq)
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
        "a background compaction and a step-contained merge must keep the same rows"
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
        compacted[&ApiMetadataRowFamily::DirentryBinds].len(),
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
    let runs = snapshot_runs_for_group(segments.manifest(), group);
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
    let Ok(result) = run_compaction(
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
        "with the floor rules keeping everything, the job must not reach the step's rows"
    );
    assert!(
        compacted.values().map(Vec::len).sum::<usize>()
            > folded.values().map(Vec::len).sum::<usize>(),
        "and must keep strictly more rows than the step kept"
    );
}

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
    let Ok(result) = run_compaction(
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
// Duplicate row keys
// -------------------------------------------------------------------------

/// The object keys the current manifest holds for `group`.
async fn group_segment_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) -> BTreeSet<String> {
    let segments = load_current_manifest_segments(store, namespace_id).await;
    snapshot_runs_for_group(segments.manifest(), group)
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
    rows.sort_by_key(|row| row.row_key_for_family(family));
    let head_seq = manifest.payload.head_seq;
    let descriptor = manifest
        .payload
        .runs
        .iter_mut()
        .filter(|run| run.tier == RunTier::Base)
        .flat_map(|run| &mut run.segments)
        .find(|descriptor| descriptor.family == family)
        .expect("the folded base run holds this family");
    super::index_parity::rewrite_manifest_segment(
        store,
        namespace_id,
        head_seq,
        family,
        descriptor,
        rows,
        NonZeroUsize::new(DEFAULT_TARGET_BLOCK_BYTES).expect("the default target is positive"),
    )
    .await;
}

#[tokio::test]
async fn a_row_key_repeated_in_both_families_of_an_index_pair_is_refused() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Revisions;

    // Fold everything into one base run, then repeat one revision inside both
    // families of the pair.
    drain_reorganization(&store, &namespace_id, &context, fold_everything_policy()).await;
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
    for family in [
        ApiMetadataRowFamily::Revisions,
        ApiMetadataRowFamily::RevisionsByInodeDesc,
    ] {
        let mut family_rows = rows[&family].clone();
        family_rows.push(repeated.clone());
        rewrite_base_segment(&store, &namespace_id, &mut manifest, family, family_rows).await;
    }
    super::index_parity::overwrite_manifest(&store, &namespace_id, manifest).await;
    // One delta run above the doctored base, so a bottom-anchored window makes
    // progress and the merge reads the base.
    write_file_bytes(
        &store,
        &namespace_id,
        "/late.txt",
        b"late\n",
        &context,
        None,
    )
    .await
    .expect("write a late file");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint the late write");

    // A doctored run still loads: the two families hold the same rows as each
    // other, so descriptor counts and the digests agree.
    let keys_before = group_segment_keys(&store, &namespace_id, group).await;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let ReorganizationPlan::BoundedMerge(input) = select_reorganization_input(
        &segments,
        group,
        fold_everything_policy(),
        floor_seq,
        &FrozenBasePolicy::default(),
    )
    .await
    .expect("plan the group")
    .plan
    else {
        panic!("raised budgets must admit a bounded merge");
    };
    drop(segments);

    let step_error = merge_group_in_step(
        &store,
        &namespace_id,
        group,
        &input.runs,
        input.placement,
        floor_seq,
        fold_everything_policy(),
    )
    .await
    .expect_err("a step-contained merge must refuse the duplicate");
    assert_duplicate_row_key(&step_error, &duplicated_key);

    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let timer = StdMonotonicTimer::default();
    let mut lease = test_lease(&store, &namespace_id, &spec, &timer).await;
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let job_error = run_metadata_compaction(
        &segments,
        &namespace_id,
        &spec,
        fold_everything_policy(),
        &MetadataCompactionCancellation::default(),
        &mut lease,
    )
    .await
    .expect_err("a background compaction must refuse the duplicate");
    assert_duplicate_row_key(&job_error, &duplicated_key);
    drop(segments);

    assert_eq!(
        group_segment_keys(&store, &namespace_id, group).await,
        keys_before,
        "neither path may publish a run built from a duplicated key"
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

// -------------------------------------------------------------------------
// Reverse-bind resolution cost
// -------------------------------------------------------------------------

/// Records every ranged read so a test can classify it against the manifest's
/// descriptors: a read at a segment's filter offset is a filter read, one at
/// its index offset is an index read, and anything else is data.
#[derive(Debug)]
struct ReadRecorderStore {
    inner: LocalFsStore,
    reads: Mutex<Vec<(String, u64, u64)>>,
}

impl ReadRecorderStore {
    fn new(inner: LocalFsStore) -> Self {
        Self {
            inner,
            reads: Mutex::new(Vec::new()),
        }
    }

    fn take_reads(&self) -> Vec<(String, u64, u64)> {
        std::mem::take(&mut self.reads.lock().expect("read log"))
    }
}

#[async_trait]
impl ObjectStore for ReadRecorderStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let result = self.inner.get(key, range.clone()).await;
        if let Ok(Some(bytes)) = &result {
            let start = range.as_ref().map_or(0, |range| range.start_inclusive);
            self.reads
                .lock()
                .expect("read log")
                .push((key.to_owned(), start, bytes.len() as u64));
        }
        result
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let result = self.inner.get_with_metadata(key).await;
        if let Ok(Some(body)) = &result {
            self.reads
                .lock()
                .expect("read log")
                .push((key.to_owned(), 0, body.bytes.len() as u64));
        }
        result
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

/// What one merge cost the store, by section.
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

/// Classifies a read log against the segment descriptors it touched.
fn classify_reads(reads: &[(String, u64, u64)], descriptors: &[MetadataSegmentRef]) -> ReadProfile {
    let mut profile = ReadProfile::default();
    for (key, start, bytes) in reads {
        profile.stored_bytes += bytes;
        let Some(descriptor) = descriptors
            .iter()
            .find(|descriptor| metadata_segment_object_key(descriptor) == *key)
        else {
            profile.other_reads += 1;
            continue;
        };
        if *start == descriptor.filter_block.offset {
            profile.filter_reads += 1;
        } else if *start == descriptor.index_block.offset {
            profile.index_reads += 1;
        } else {
            profile.data_reads += 1;
        }
    }
    profile
}

/// A namespace whose reverse-bind index walks the unbind keyspace out of
/// order.
///
/// Files are created round-robin across many parents, so child inode ids
/// ascend across parents rather than within one. The reverse index is keyed by
/// child and the unbind family by parent, so reading the reverse index in its
/// own key order jumps between parents on every row. Every file is then
/// deleted and the floor advanced past the deletions, which is what makes each
/// reverse row cost a probe.
async fn seed_reverse_binds_that_jump_the_unbind_keyspace(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    parents: u64,
    rounds: u64,
) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for round in 0..rounds {
        for parent in 0..parents {
            write_file_bytes(
                store,
                namespace_id,
                &format!("/d{parent}/f{round}.txt"),
                b"body\n",
                &context,
                None,
            )
            .await
            .expect("write file");
        }
        // The WAL tail is capped, so each round is checkpointed as it is made.
        create_checkpoint(store, namespace_id, &context)
            .await
            .expect("checkpoint the writes");
    }
    for round in 0..rounds {
        for parent in 0..parents {
            delete_path(
                store,
                namespace_id,
                &format!("/d{parent}/f{round}.txt"),
                &context,
                None,
            )
            .await
            .expect("delete file");
        }
        create_checkpoint(store, namespace_id, &context)
            .await
            .expect("checkpoint the deletions");
    }
    // Small segments, so the unbind family spans many segments and a probe
    // that jumps parents lands in a different one.
    drain_reorganization(
        store,
        namespace_id,
        &context,
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::new(16).expect("nonzero"),
            ..fold_everything_policy()
        },
    )
    .await;
    advance_retention_floor(store, namespace_id, &context)
        .await
        .expect("advance the floor past the deletions");
    write_file_bytes(store, namespace_id, "/late.txt", b"late\n", &context, None)
        .await
        .expect("write a late file");
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the late write");
}

/// Replaces the bindings families of the namespace's base run with `count`
/// synthetic generations, each bound and unbound below the retention floor.
///
/// The point is the ordering. The reverse index is keyed by child and the
/// unbind family by parent and name, so a generation's name is scrambled
/// against its child id: reading the reverse index in its own order walks the
/// unbind keyspace in a pseudo-random permutation, which is the worst case for
/// a cache that holds a bounded slice of it. `count` must be a power of two,
/// which makes the odd multiplier below a bijection.
///
/// Going through the write path for a working set this size would take hours,
/// and the merge only ever sees durable segments, so they are built directly.
async fn install_synthetic_bindings_base(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    count: u64,
    rows_per_segment: NonZeroUsize,
) -> u64 {
    assert!(
        count.is_power_of_two(),
        "the scrambling multiplier is only a permutation over a power of two"
    );
    let mut binds = Vec::with_capacity(count as usize);
    let mut unbinds = Vec::with_capacity(count as usize);
    for index in 0..count {
        let parent = InodeId(1_000);
        // Odd multiplier, power-of-two modulus: a bijection, so every name is
        // distinct and the name order is scattered against the child order.
        let scrambled = index.wrapping_mul(2_654_435_761) % count;
        let name = format!("g{scrambled:012}");
        let delta = u32::try_from(index).expect("test generation counts are small");
        binds.push(MetadataRow::DirentryBind(
            crate::metadata::DirentryBindRecord {
                parent_inode_id: parent,
                name_key: NameKey::parse(&name).expect("name key"),
                display_name: loonfs_api::DisplayName::parse(&name).expect("display name"),
                child_inode_id: InodeId(100_000 + index),
                bind_seq: ChangeSeq(1),
                bind_delta_index: delta,
            },
        ));
        unbinds.push(MetadataRow::DirentryUnbind(
            crate::metadata::DirentryUnbindRecord {
                parent_inode_id: parent,
                name_key: NameKey::parse(&name).expect("name key"),
                display_name: loonfs_api::DisplayName::parse(&name).expect("display name"),
                child_inode_id: InodeId(100_000 + index),
                bind_seq: ChangeSeq(1),
                bind_delta_index: delta,
                unbind_seq: ChangeSeq(2),
                unbind_delta_index: delta,
            },
        ));
    }

    let mut manifest = load_current_manifest_segments(store, namespace_id)
        .await
        .manifest()
        .clone();
    // The synthetic segments replace this run's bindings, so they join the
    // run that already holds the group's other families.
    let base_run = manifest
        .payload
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
        .expect("the namespace has been folded into a base run");
    let base_run_no = base_run.run_no;
    let mut rows_by_family = BTreeMap::from([
        (ApiMetadataRowFamily::DirentryBinds, binds.clone()),
        (ApiMetadataRowFamily::DirentryChildBinds, binds),
        (ApiMetadataRowFamily::DirentryUnbinds, unbinds),
    ]);
    let run_segments = build_manifest_segments_from_rows(
        store,
        namespace_id,
        |family| {
            let mut rows = rows_by_family.remove(&family).unwrap_or_default();
            rows.sort_by_key(|row| row.row_key_for_family(family));
            rows
        },
        rows_per_segment,
    )
    .await
    .expect("build the synthetic bindings run");

    // Only the base tier is replaced: the delta run above it is what gives a
    // bottom-anchored window something to fold.
    let base_run = manifest
        .payload
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
    // What the probe cache would have to hold to serve every probe without
    // refetching: the decoded data blocks of the unbind family.
    let mut unbind_decoded_bytes = 0u64;
    for descriptor in manifest
        .payload
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|descriptor| descriptor.family == ApiMetadataRowFamily::DirentryUnbinds)
    {
        let index = block_fetch::load_segment_index_for_reorganization(
            store,
            None,
            &Default::default(),
            descriptor,
        )
        .await
        .expect("load a synthetic segment index");
        unbind_decoded_bytes += index
            .iter()
            .map(|entry| u64::from(entry.block.decoded_len))
            .sum::<u64>();
    }
    super::index_parity::overwrite_manifest(store, namespace_id, manifest).await;
    unbind_decoded_bytes
}

#[tokio::test]
async fn a_step_contained_merge_reads_its_window_once() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_reverse_binds_that_jump_the_unbind_keyspace(&inner, &namespace_id, 4, 2).await;
    let generations = 4_096;
    install_synthetic_bindings_base(
        &inner,
        &namespace_id,
        generations,
        NonZeroUsize::new(512).expect("nonzero"),
    )
    .await;

    let store = ReadRecorderStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let group = MetadataFamilyGroup::Bindings;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let descriptors: Vec<MetadataSegmentRef> = segments
        .manifest()
        .payload
        .runs
        .iter()
        .flat_map(|run| run.segments.iter().cloned())
        .collect();
    let ReorganizationPlan::BoundedMerge(input) = select_reorganization_input(
        &segments,
        group,
        fold_everything_policy(),
        floor_seq,
        &FrozenBasePolicy::default(),
    )
    .await
    .expect("plan the group")
    .plan
    else {
        panic!("raised budgets must admit a bounded merge");
    };
    drop(segments);
    let window_segments = input
        .runs
        .iter()
        .flat_map(|run| group_run_descriptors(run, group))
        .count();
    let unbinds_below_floor = descriptors
        .iter()
        .filter(|descriptor| descriptor.family == ApiMetadataRowFamily::DirentryUnbinds)
        .map(|descriptor| descriptor.row_count)
        .sum::<u64>();
    let _ = store.take_reads();

    let merged = merge_group_in_step(
        &store,
        &namespace_id,
        group,
        &input.runs,
        input.placement,
        floor_seq,
        fold_everything_policy(),
    )
    .await
    .expect("merge the window");
    let profile = classify_reads(&store.take_reads(), &descriptors);

    assert_eq!(
        merged.unbind_probes, 0,
        "a step-contained merge resolves the reverse index from what it streamed"
    );
    assert_eq!(
        merged.collected_unbind_generations as u64, unbinds_below_floor,
        "the collected set holds one identity per below-floor unbind in the window, and no more"
    );
    // One index read and one span of data reads per segment. Data blocks are
    // fetched two at a time, so a segment costs at most its block count; the
    // bound that matters is that neither follows the row count.
    assert!(
        profile.index_reads <= window_segments,
        "the merge read {} indexes for {window_segments} segments",
        profile.index_reads
    );
    assert!(
        profile.requests() < merged.rows_read as usize / 8,
        "total work must follow the window's segments, not its rows: {} requests for {} rows",
        profile.requests(),
        merged.rows_read
    );

    // The background job keeps the probe, because it has no budget capping
    // what it would otherwise collect.
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
    let Ok(job) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        fold_everything_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job");
    };
    assert_eq!(
        job.unbind_probes, unbinds_below_floor,
        "a background compaction probes once per reverse row the floor covers"
    );
    assert_eq!(
        job.collected_unbind_generations, 0,
        "and collects nothing, so its memory does not follow the group"
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

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
    let Ok(straight_result) = run_compaction(
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
    let store = LocalFsStore::new(interrupted_dir.path()).expect("store");
    let spec = compaction_spec_for_group(&store, &namespace_id, group).await;

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
        let outcome = run_compaction(
            &store,
            &namespace_id,
            &spec,
            small_segment_policy(),
            &cancellation,
        )
        .await;
        if matches!(outcome, Err(MetadataCompactionJobOutcome::Cancelled)) {
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
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let Ok(result) = run_compaction(
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
        .map(metadata_segment_object_key)
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
    let manifest_keys: BTreeSet<String> = load_current_manifest_segments(&store, &namespace_id)
        .await
        .manifest()
        .payload
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

// -------------------------------------------------------------------------
// The job's claim on its own output
// -------------------------------------------------------------------------

#[tokio::test]
async fn a_job_claims_its_prefix_while_it_runs_and_leaves_the_claim_standing_when_it_publishes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(&store, &namespace_id).await;
    let group = MetadataFamilyGroup::Bindings;
    let policy = policy_that_starves_the_group(&store, &namespace_id, group).await;
    let spec = step_until_a_compaction_is_planned(&store, &namespace_id, &context, policy).await;
    let lease_key = metadata_compaction_lease(&namespace_id, spec.group());

    let outcome = run_metadata_compaction_job(
        &store,
        &namespace_id,
        &context,
        &spec,
        policy,
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("run the job");
    assert!(matches!(
        outcome,
        MetadataCompactionJobOutcome::Published { .. }
    ));

    // The job writes every segment under its own prefix, and the manifest now
    // references them.
    let staged = staged_object_keys(&store, &namespace_id).await;
    let referenced = referenced_segment_keys(&store, &namespace_id).await;
    let job_prefix = format!(
        "{}{}/",
        metadata_compaction_prefix(&namespace_id),
        spec.job_id().as_str()
    );
    assert!(
        staged.iter().all(|key| key.starts_with(&job_prefix)),
        "every object a job writes belongs to that job's prefix: {staged:?}"
    );
    assert!(
        !staged.is_empty() && staged.iter().all(|key| referenced.contains(key)),
        "the manifest must name every segment the job published"
    );
    assert!(
        store
            .head(&lease_key)
            .await
            .expect("head the group lease")
            .is_some(),
        "a published job leaves its final lease standing for the pass that has not seen the new \
         root yet"
    );
}

#[tokio::test]
async fn group_lease_acquisition_supersedes_a_second_job_and_fences_an_expired_owner() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let policy = small_segment_policy();
    let first_spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let second_spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let timer = StdMonotonicTimer::default();
    let mut first_lease = test_lease(&store, &namespace_id, &first_spec, &timer).await;

    let staged_before = staged_object_keys(&store, &namespace_id).await;
    let held = run_metadata_compaction_job(
        &store,
        &namespace_id,
        &test_context(),
        &second_spec,
        policy,
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("try the held group");
    assert_eq!(held, MetadataCompactionJobOutcome::Superseded);
    assert_eq!(
        staged_object_keys(&store, &namespace_id).await,
        staged_before,
        "a superseded job writes no staged object"
    );

    let takeover_context = mutation_context(
        "takeover-writer",
        test_context()
            .now_ms
            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS)
            .saturating_add(1),
    );
    let previous = load_group_lease(&store, &namespace_id, first_spec.group())
        .await
        .expect("load the first lease")
        .expect("the first lease exists");
    assert!(
        previous.state.expires_at_ms < takeover_context.now_ms,
        "takeover requires the stored expiry to be in the past"
    );
    let taken_over = run_metadata_compaction_job(
        &store,
        &namespace_id,
        &takeover_context,
        &second_spec,
        policy,
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("take over the expired lease");
    assert!(matches!(
        taken_over,
        MetadataCompactionJobOutcome::Published { .. }
    ));
    assert_eq!(
        first_lease
            .refresh(&store)
            .await
            .expect("refresh the old lease"),
        LeaseHold::Fenced
    );
}

#[tokio::test]
async fn a_worker_whose_expired_lease_was_claimed_is_fenced_and_publishes_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let Ok(result) = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    else {
        panic!("nothing cancelled this job while it read");
    };
    let manifest_before = current_manifest_object_id(&store, &namespace_id).await;

    // The claim the worker holds, taken before the collector's. That is the
    // whole shape of the race: the worker's etag is the one it last wrote,
    // and the collector is about to replace it.
    let timer = StdMonotonicTimer::default();
    let mut lease = test_lease(&store, &namespace_id, &spec, &timer).await;

    // Well past the expiry rather than one millisecond past it: the job
    // stamped its lease off a real clock, so the pass's clock has to clear
    // whatever it spent running.
    let expired_ms = test_context().now_ms + METADATA_COMPACTION_LEASE_EXPIRY_MS * 2;
    let owner = claim_group_lease(
        &store,
        &namespace_id,
        spec.group(),
        spec.job_id(),
        expired_ms,
    )
    .await
    .expect("claim the expired prefix");
    assert_eq!(
        owner,
        CompactionPrefixOwner::ThisCollector,
        "an expired lease is claimed, and only the winner may reap the prefix"
    );

    assert_eq!(
        lease.refresh(&store).await.expect("refresh the lease"),
        LeaseHold::Fenced,
        "the worker's next refresh must find the claim and stop"
    );
    let finalization = finalize_metadata_compaction(
        &store,
        &namespace_id,
        &spec,
        &snapshot_keys,
        result,
        &MetadataCompactionCancellation::default(),
        &mut lease,
    )
    .await
    .expect("finalize the fenced job");
    assert!(
        matches!(finalization, MetadataCompactionJobOutcome::Fenced),
        "a fenced job reports it rather than publishing, got {finalization:?}"
    );
    assert_eq!(
        current_manifest_object_id(&store, &namespace_id).await,
        manifest_before,
        "and the root must not have moved"
    );
}

/// A monotonic clock that advances a second per reading.
#[derive(Debug, Default)]
struct SteppingTimer(AtomicU64);

impl MonotonicTimer for SteppingTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0.fetch_add(1_000, Ordering::SeqCst)
    }
}

#[tokio::test]
async fn lease_creation_and_refresh_write_absolute_expiry() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let context = test_context();
    let timer = SteppingTimer::default();
    let mut lease = test_lease(&store, &namespace_id, &spec, &timer).await;

    let created = load_group_lease(&store, &namespace_id, spec.group())
        .await
        .expect("load the created lease")
        .expect("created lease");
    assert_eq!(
        created.state.expires_at_ms,
        context
            .now_ms
            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS),
        "creation writes the expiry policy as an absolute instant"
    );

    assert_eq!(
        lease.refresh(&store).await.expect("refresh the lease"),
        LeaseHold::Held
    );
    let refreshed = load_group_lease(&store, &namespace_id, spec.group())
        .await
        .expect("load the refreshed lease")
        .expect("refreshed lease");
    assert_eq!(
        refreshed.state.expires_at_ms,
        context
            .now_ms
            .saturating_add(1_000)
            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS),
        "refresh writes its current time plus the expiry policy"
    );
}

#[tokio::test]
async fn exactly_one_of_a_refresh_and_a_reaping_claim_wins() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let lease_key = metadata_compaction_lease(&namespace_id, spec.group());
    // The stepping clock below moves each refresh's expiry forward a few
    // seconds, so the pass's clock is set well past the expiry rather than one
    // millisecond past it.
    let expired_ms = test_context().now_ms + METADATA_COMPACTION_LEASE_EXPIRY_MS * 2;
    // The local store's etags are content hashes, so two lease writes inside
    // one millisecond would carry identical bytes under an identical etag and
    // the race under test would not be one. A stepping clock makes every
    // refresh a distinct document, which is what a provider etag gives for
    // free.
    let timer = SteppingTimer::default();
    let mut lease = test_lease(&store, &namespace_id, &spec, &timer).await;

    let gated = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(lease_key),
        OperationClass::CompareAndSwap,
    );
    gated.block_next();
    let (claimed, refresh) = tokio::join!(
        claim_group_lease(
            &gated,
            &namespace_id,
            spec.group(),
            spec.job_id(),
            expired_ms,
        ),
        async {
            gated.wait_until_blocked().await;
            let refresh = lease.refresh(&store).await.expect("refresh the lease");
            gated.release();
            refresh
        }
    );
    assert_eq!(
        refresh,
        LeaseHold::Held,
        "the worker got there first, so it keeps its prefix"
    );
    assert_eq!(
        claimed.expect("claim the prefix"),
        CompactionPrefixOwner::LiveJob,
        "and the claim behind it is refused, so exactly one of the two won"
    );

    // The other order, on the same lease: the claim lands, and the refresh
    // behind it finds an etag it does not hold.
    assert_eq!(
        claim_group_lease(
            &store,
            &namespace_id,
            spec.group(),
            spec.job_id(),
            expired_ms,
        )
        .await
        .expect("claim the prefix"),
        CompactionPrefixOwner::ThisCollector
    );
    assert_eq!(
        lease.refresh(&store).await.expect("refresh the lease"),
        LeaseHold::Fenced,
        "exactly one of the two compare-and-swaps may win"
    );
}

// -------------------------------------------------------------------------
// Resource discipline
// -------------------------------------------------------------------------

#[tokio::test]
async fn a_merge_keeps_its_reads_and_its_decoded_blocks_bounded() {
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

    // The same bounds hold for the same engine run inside a maintenance pass.
    // The step's input budgets cap what a window may hold, but they say nothing
    // about what merging it costs, and the old path read every row of the
    // window into vectors.
    let store = ConcurrencyWatchStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
    );
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let ReorganizationPlan::BoundedMerge(input) = select_reorganization_input(
        &segments,
        group,
        small_segment_policy(),
        floor_seq,
        &FrozenBasePolicy::default(),
    )
    .await
    .expect("plan the group")
    .plan
    else {
        panic!("raised budgets must admit a bounded merge");
    };
    drop(segments);
    let merged = merge_group_in_step(
        &store,
        &namespace_id,
        group,
        &input.runs,
        input.placement,
        floor_seq,
        small_segment_policy(),
    )
    .await
    .expect("merge the window inside a step");
    assert!(
        store.reads().peak_in_flight <= 8,
        "the step's merge overlapped {} reads at once",
        store.reads().peak_in_flight
    );
    assert!(
        merged.peak_resident_blocks <= input.runs.len() * 2 * 2,
        "the step's merge held {} decoded blocks at once",
        merged.peak_resident_blocks
    );
    assert!(
        merged.peak_operator_rows <= 1,
        "a retention operator held {} rows in the step's merge",
        merged.peak_operator_rows
    );
    assert_eq!(
        merged.rows_read, result.rows_read,
        "both paths read the same window"
    );
}

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
        ApiMetadataRowFamily::DirentryBinds,
        ApiMetadataRowFamily::DirentryUnbinds,
    ] {
        for row in &before[&family] {
            match row {
                MetadataRow::DirentryBind(crate::metadata::DirentryBindRecord {
                    parent_inode_id,
                    ..
                })
                | MetadataRow::DirentryUnbind(crate::metadata::DirentryUnbindRecord {
                    parent_inode_id,
                    ..
                }) => *rows_per_parent.entry(*parent_inode_id).or_default() += 1,
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
    let Ok(result) = run_compaction(
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
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    assert_eq!(
        snapshot_runs_for_group(segments.manifest(), group).len(),
        1,
        "the job must leave the group in one run"
    );
}

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

    // What both stores hold before either of them rebuilds anything. The trees
    // are byte-identical here, so one read stands for both.
    let mut rows_before = BTreeMap::new();
    for group in REORGANIZE_FAMILY_GROUPS {
        rows_before.insert(
            group,
            group_rows_of_current_manifest(&store, &namespace_id, group).await,
        );
    }
    // The same durable bytes, rebuilt the other way: synchronous merges inside
    // maintenance passes, with the budgets raised so each group folds whole.
    drain_reorganization(
        &fold_store,
        &namespace_id,
        &test_context(),
        fold_everything_policy(),
    )
    .await;

    for group in REORGANIZE_FAMILY_GROUPS {
        let before = rows_before[&group].clone();
        if before.values().all(Vec::is_empty) {
            continue;
        }
        let folded = group_rows_of_current_manifest(&fold_store, &namespace_id, group).await;

        let spec = compaction_spec_for_group(&store, &namespace_id, group).await;
        let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
        let Ok(result) = run_compaction(
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
            "a background rebuild of {group:?} must keep the rows a step-contained merge keeps"
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
        attributes[&ApiMetadataRowFamily::Attributes].len(),
        2,
        "one inode's history must collapse to the newest row at the floor plus the late one"
    );
    let bindings =
        group_rows_of_current_manifest(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    assert_eq!(
        bindings[&ApiMetadataRowFamily::DirentryBinds].len(),
        bindings[&ApiMetadataRowFamily::DirentryChildBinds].len(),
    );
    assert!(
        bindings[&ApiMetadataRowFamily::DirentryUnbinds].is_empty(),
        "every unbind the floor covered must be gone"
    );
}

// -------------------------------------------------------------------------
// The whole arc, through the step the maintenance runner calls
// -------------------------------------------------------------------------

/// The unbind rows a floor covers, which is the churn a rebuild reclaims.
fn unbinds_at_or_below(
    rows: &BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>>,
    floor_seq: ChangeSeq,
) -> usize {
    rows[&ApiMetadataRowFamily::DirentryUnbinds]
        .iter()
        .filter(|row| {
            matches!(row, MetadataRow::DirentryUnbind (crate::metadata::DirentryUnbindRecord { unbind_seq, .. }) if *unbind_seq <= floor_seq)
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
    let inputs: BTreeSet<RunNo> = spec.inputs().iter().copied().collect();
    load_current_manifest_segments(store, namespace_id)
        .await
        .manifest()
        .payload
        .runs
        .iter()
        .filter(|run| !inputs.contains(&run.run_no))
        .flat_map(|run| &run.segments)
        .filter(|descriptor| group.families().contains(&descriptor.family))
        .map(metadata_segment_object_key)
        .collect()
}

/// Every object key the current manifest references.
async fn referenced_segment_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    load_current_manifest_segments(store, namespace_id)
        .await
        .manifest()
        .payload
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect()
}

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
            FrozenBasePolicy::default(),
        )
        .await
        .expect("maintenance pass");
        match report.outcome {
            MetadataReorganizeOutcome::UnitPublished { group: folded, .. } => {
                if active.is_some() {
                    assert_ne!(
                        folded, group,
                        "no step may merge the group a job is rebuilding"
                    );
                    other_groups_folded += 1;
                }
            }
            MetadataReorganizeOutcome::CompactionPlanned {
                group: planned_group,
                ref spec,
            } => {
                // One job at a time per namespace: a plan that arrives while
                // one runs is reported and skipped, which is what the runner
                // does with it.
                if active.is_none() {
                    assert_eq!(planned_group, group, "this budget starves this group first");
                    active = Some(spec.clone());
                    write_active_lease(
                        &store,
                        &namespace_id,
                        spec,
                        context
                            .now_ms
                            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS),
                    )
                    .await;
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
                store
                    .delete(&metadata_compaction_lease(&namespace_id, spec.group()))
                    .await
                    .expect("remove the test lease before the job creates it");
                publish_planned_compaction(&store, &namespace_id, &context, policy, &spec).await;
                store
                    .delete(&metadata_compaction_lease(&namespace_id, spec.group()))
                    .await
                    .expect("expire the completed test job's lease");
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

    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let base_runs = snapshot_runs_for_group(segments.manifest(), group)
        .into_iter()
        .filter(|run| run.tier == RunTier::Base)
        .count();
    drop(segments);
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
/// A step folds the group with the most delta rows, so the starved group is
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
            FrozenBasePolicy::default(),
        )
        .await
        .expect("maintenance pass");
        match report.outcome {
            MetadataReorganizeOutcome::CompactionPlanned { spec, .. } => return spec,
            MetadataReorganizeOutcome::UnitPublished { .. } => {}
            other => panic!("expected a plan or a merge, got {other:?}"),
        }
    }
    panic!("no step handed a group to a job")
}

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
    for (attempt, reads_before_cancel) in [8usize, 16, 24, 32].into_iter().enumerate() {
        let cancellation = MetadataCompactionCancellation::default();
        let dying_store = CancelAfterReadsStore {
            inner: LocalFsStore::new(temp_dir.path()).expect("store"),
            cancellation: cancellation.clone(),
            reads_before_cancel,
            reads: AtomicUsize::new(0),
        };
        let attempt_context = mutation_context(
            "test-writer",
            context.now_ms.saturating_add(
                u64::try_from(attempt)
                    .expect("four attempts should fit in u64")
                    .saturating_mul(METADATA_COMPACTION_LEASE_EXPIRY_MS.saturating_mul(2)),
            ),
        );
        let outcome = run_metadata_compaction_job(
            &dying_store,
            &namespace_id,
            &attempt_context,
            &spec,
            policy,
            &cancellation,
        )
        .await
        .expect("run the job");
        assert_eq!(
            outcome,
            MetadataCompactionJobOutcome::Cancelled,
            "the cancellation must land before the job finishes"
        );
        assert!(
            store
                .head(&metadata_compaction_lease(&namespace_id, spec.group()))
                .await
                .expect("head the lease")
                .is_some(),
            "a cancelled job's claim stands until it expires"
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

    // Once the abandoned lease expires, the next step plans the group again,
    // and the second attempt finishes.
    let expired_context = mutation_context(
        "test-writer",
        context
            .now_ms
            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS.saturating_mul(10)),
    );
    let spec =
        step_until_a_compaction_is_planned(&store, &namespace_id, &expired_context, policy).await;
    publish_planned_compaction(&store, &namespace_id, &expired_context, policy, &spec).await;

    assert_eq!(
        visible_namespace(&store, &namespace_id).await,
        visible,
        "the rebuild must leave what a read answers where it was"
    );
    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    let runs = snapshot_runs_for_group(segments.manifest(), group);
    drop(segments);
    assert_eq!(runs.len(), 1, "the group must end in one run");
    assert_eq!(runs[0].tier, RunTier::Base);
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

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
    let Ok(result) = run_compaction(
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
    let manifest_no = match finalize_streaming_compaction(
        &racing_store,
        &namespace_id,
        &spec,
        &snapshot_keys,
        &result,
    )
    .await
    {
        MetadataCompactionJobOutcome::Published { manifest_no, .. } => manifest_no,
        other => panic!("the retry must publish, got {other:?}"),
    };
    assert!(
        racing_store.flushed.load(Ordering::SeqCst) > 1,
        "the finalizer must have written a replacement manifest more than once"
    );

    let segments = load_current_manifest_segments(&store, &namespace_id).await;
    assert_eq!(segments.manifest().payload.manifest_no, manifest_no);
    let runs = snapshot_runs_for_group(segments.manifest(), group);
    drop(segments);
    // Two runs: the base run the job built, and the delta run the flush
    // published above the job's snapshot. The flush's run survives because
    // the swap replaces only what the snapshot held.
    assert_eq!(
        runs.iter().filter(|run| run.tier == RunTier::Base).count(),
        1,
        "the group must end in one base run"
    );
    assert_eq!(
        runs.iter().filter(|run| run.tier == RunTier::Delta).count(),
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
    load_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .state
        .updated_at_ms
}

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
    let Ok(result) = run_compaction(
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
    let later = mutation_context(context.writer_id.as_str(), context.now_ms + 60_000);
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
        matches!(finalization, MetadataCompactionJobOutcome::Cancelled),
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

/// Cancels the job when its first root compare-and-swap fails. Lease updates
/// still succeed so cancellation, rather than fencing, stops the job.
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
        if key.ends_with("/metadata/root.json") && matches!(mode, PutMode::CompareAndSwap { .. }) {
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

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
        matches!(finalization, MetadataCompactionJobOutcome::Cancelled),
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

fn mismatched_references(reference: &ManifestRef) -> Vec<(&'static str, ManifestRef)> {
    let mut number = reference.clone();
    number.manifest_no = ManifestNo(reference.manifest_no.0 + 1);
    let mut sequence = reference.clone();
    sequence.manifest_head_seq = ChangeSeq(reference.manifest_head_seq.0 - 1);
    let mut checksum = reference.clone();
    checksum.manifest_payload_checksum = format!("sha256:{}", "0".repeat(64));
    vec![
        ("manifest_no", number),
        ("manifest_head_seq", sequence),
        ("manifest_payload_checksum", checksum),
    ]
}

async fn replace_root_reference(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    reference: ManifestRef,
) -> Bytes {
    use loonfs_api::wire::control::{encode_control_state, ControlObjectKind};
    let mut root = load_metadata_root_object(store, namespace_id)
        .await
        .expect("load root")
        .state;
    root.manifest = reference;
    let bytes = Bytes::from(
        encode_control_state(ControlObjectKind::MetadataRoot, &root).expect("encode root"),
    );
    store
        .put_overwrite(&metadata_root(namespace_id), bytes.clone())
        .await
        .expect("replace root reference");
    bytes
}

fn assert_reference_corrupt(error: CoreError, field: &str) {
    assert!(
        matches!(&error, CoreError::NamespaceCorrupt(message) if message.contains(field)),
        "expected corrupt {field}, got {error:?}"
    );
}

#[tokio::test]
async fn compaction_rejects_mismatched_manifest_references_before_writing() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let root = load_metadata_root_object(&store, &namespace_id)
        .await
        .expect("root")
        .state;
    let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
    super::super::load::load_manifest_segments(&store, Some(&cache), &root.manifest)
        .await
        .expect("warm manifest cache");

    for (field, reference) in mismatched_references(&root.manifest) {
        let error = super::super::load::load_manifest_segments(&store, Some(&cache), &reference)
            .await
            .err()
            .expect("cache hits must verify the reference too");
        assert_reference_corrupt(error, field);
        let corrupted = replace_root_reference(&store, &namespace_id, reference).await;
        let recording = RecordingStore::new(store.clone(), KeyPredicate::any());
        let error = load_current_projection(&recording, &namespace_id)
            .await
            .expect_err("ordinary reads must reject the reference");
        assert_reference_corrupt(error, field);
        for max_bytes in [usize::MAX, 1] {
            let error = reorganize_metadata_step(
                &recording,
                &namespace_id,
                &test_context(),
                MetadataLsmPolicy {
                    max_delta_runs: NonZeroUsize::MIN,
                    max_decoded_input_bytes_per_step: NonZeroUsize::new(max_bytes)
                        .expect("nonzero budget"),
                    ..MetadataLsmPolicy::default()
                },
                FrozenBasePolicy::default(),
            )
            .await
            .expect_err("bounded merges and full-job planning must reject the reference");
            assert_reference_corrupt(error, field);
        }
        let error = run_metadata_compaction_job(
            &recording,
            &namespace_id,
            &test_context(),
            &spec,
            MetadataLsmPolicy::default(),
            &MetadataCompactionCancellation::default(),
        )
        .await
        .expect_err("job startup must reject the reference");
        assert_reference_corrupt(error, field);
        assert_eq!(
            recording.counts().puts,
            0,
            "no output or lease may be written"
        );
        assert_eq!(recording.counts().compare_and_swaps, 0);
        assert_eq!(
            store
                .get(&metadata_root(&namespace_id), None)
                .await
                .expect("read root"),
            Some(corrupted),
        );
    }
}

#[tokio::test]
async fn compaction_finalization_rechecks_the_complete_manifest_reference() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    seed_bindings_workload(&store, &namespace_id).await;
    let spec =
        compaction_spec_for_group(&store, &namespace_id, MetadataFamilyGroup::Bindings).await;
    let snapshot_keys = snapshot_keys_now(&store, &namespace_id, &spec).await;
    let result = run_compaction(
        &store,
        &namespace_id,
        &spec,
        small_segment_policy(),
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("build staged output");
    let timer = StdMonotonicTimer::default();
    let mut lease = test_lease(&store, &namespace_id, &spec, &timer).await;
    let root = load_metadata_root_object(&store, &namespace_id)
        .await
        .expect("root")
        .state;
    for (field, reference) in mismatched_references(&root.manifest) {
        let corrupted = replace_root_reference(&store, &namespace_id, reference).await;
        let lease_key = metadata_compaction_lease(&namespace_id, spec.group());
        let recording = RecordingStore::new(
            store.clone(),
            KeyPredicate::new(move |key| key != lease_key),
        );
        let error = finalize_metadata_compaction(
            &recording,
            &namespace_id,
            &spec,
            &snapshot_keys,
            result.clone(),
            &MetadataCompactionCancellation::default(),
            &mut lease,
        )
        .await
        .expect_err("finalization must reject an inconsistent reference");
        assert_reference_corrupt(error, field);
        assert_eq!(
            recording.counts().puts,
            0,
            "no replacement manifest may be written"
        );
        assert_eq!(
            store
                .get(&metadata_root(&namespace_id), None)
                .await
                .expect("read root"),
            Some(corrupted),
            "finalization must not replace the inconsistent root",
        );
    }
}
