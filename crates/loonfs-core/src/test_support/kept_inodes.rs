//! Base compactions whose kept inode filter is seeded by the test, so that an
//! assertion about removed rows doesn't depend on a filter false positive.

use crate::compaction::group_compaction::{run_metadata_compaction, GroupCompactionReport};
use crate::compaction::kept_inodes::{load_kept_inode_filter, KeptInodeFilter};
use crate::compaction::plan::{group_run_descriptors, CompactionPlacement, MetadataCompactionSpec};
use crate::compaction::publication::input_segment_keys;
use crate::compaction::{CompactionUnitOutcome, CompactionWrites, MaintenanceCancellation};
use crate::manifest::metadata_basis_from_manifest;
use crate::manifest::publish::encode_manifest;
use crate::metadata::InodeRecord;
use crate::namespace::control::LoadedManifest;
use crate::segment::runs::{runs_newest_first, MetadataRunManifest, MetadataRunPolicy};
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, NamespaceManifestEnvelope, RunTier};
use loonfs_types::{
    CommitId, CommitSeq, InodeId, InodeKind, MetadataFamilyGroup, NamespaceId, RunNo,
};
use std::assert_matches;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Compacts every run of each of `groups` into one base run, with each compaction's
/// kept inode filter seeded so that it keeps no row that `hidden` inodes own.
///
/// The low bits of an inode ID are random, so a filter seeded by the next run
/// number could keep a hidden inode's row by a false positive. Choosing the
/// seed makes every compaction remove every hidden row, so an assertion after it
/// doesn't depend on the IDs that a run of the test drew.
pub async fn compact_groups_without_false_positives<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    groups: &[MetadataFamilyGroup],
    hidden: &[InodeId],
) {
    // Each compaction takes at most the default policy's fan-in, so a group
    // with more runs needs more than one compaction.
    let max_runs = MetadataRunPolicy::default().max_input_runs_per_step.get();
    for &group in groups {
        let mut compacted_every_run = false;
        for _ in 0..8 {
            let current = super::namespace_store(store, namespace_id)
                .load_current_manifest()
                .await
                .expect("load the current manifest");
            let payload = current.payload();
            let runs = group_runs_base_first(&current, group).len();
            compacted_every_run = runs <= max_runs;
            if runs == 0 {
                break;
            }
            let spec = whole_group_spec(&current, group);
            let seed = seed_keeping_none_of(store, namespace_id, payload.next_run_no, hidden).await;
            compact_with_seed(store, namespace_id, &spec, seed).await;
            if compacted_every_run {
                break;
            }
        }
        assert!(
            compacted_every_run,
            "{group:?} is compacted into one base run"
        );
    }
}

/// The first run number from `first` that seeds a kept inode filter over the
/// current manifest that keeps no row that `hidden` inodes own.
pub(crate) async fn seed_keeping_none_of<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    first: RunNo,
    hidden: &[InodeId],
) -> RunNo {
    let mut seed = None;
    for offset in 0..64 {
        let candidate = RunNo(first.0 + offset);
        let filter = seeded_kept_inode_filter(store, namespace_id, candidate).await;
        if hidden
            .iter()
            .all(|inode_id| !filter.keeps(&inode_probe(*inode_id)))
        {
            seed = Some(candidate);
            break;
        }
    }
    seed.expect("a seed whose kept inode filter keeps no hidden row")
}

/// The plan of a base compaction of `group`'s oldest runs in `current`, up
/// to the default run policy's `max_input_runs_per_step`, base first.
///
/// The output is the group's base run at the manifest's head, and the
/// compaction judges rows against the manifest's retention floor. A group
/// with no more runs than that bound is compacted whole.
pub(crate) fn whole_group_spec(
    current: &LoadedManifest,
    group: MetadataFamilyGroup,
) -> MetadataCompactionSpec {
    let payload = current.payload();
    let mut runs = group_runs_base_first(current, group);
    runs.truncate(MetadataRunPolicy::default().max_input_runs_per_step.get());
    MetadataCompactionSpec::new(
        group,
        runs.iter().map(|run| run.run_no).collect(),
        runs.iter()
            .flat_map(|run| group_run_descriptors(run, group))
            .map(|descriptor| descriptor.row_count)
            .sum(),
        CompactionPlacement::Base {
            output_seq: payload.head_seq,
        },
        payload.retention_floor_seq,
    )
}

/// The runs of `current` that list rows of `group`, the base first and then
/// the delta runs from oldest to newest.
fn group_runs_base_first(
    current: &LoadedManifest,
    group: MetadataFamilyGroup,
) -> Vec<MetadataRunManifest> {
    let mut runs = runs_newest_first(current.payload())
        .into_iter()
        .filter(|run| group_run_descriptors(run, group).next().is_some())
        .collect::<Vec<_>>();
    runs.sort_by_key(|run| (run.tier != RunTier::Base, run.run_seq));
    runs
}

/// An inode row for `inode_id`, which a kept inode filter keeps exactly when
/// it keeps every row that the inode owns.
fn inode_probe(inode_id: InodeId) -> MetadataRow {
    MetadataRow::Inode(InodeRecord {
        inode_id,
        inode_kind: InodeKind::File,
        committed_seq: CommitSeq(1),
        commit_id: CommitId::parse("c_probe").expect("commit id"),
        committed_by: loonfs_types::ActorId::loonfs(),
        committed_at_ms: 1_000,
    })
}

/// The current manifest with `next_run_no` set to `seed`, which seeds the
/// kept inode filter of a compaction over it.
pub(crate) fn seeded_manifest(
    current: &LoadedManifest,
    seed: RunNo,
) -> Arc<NamespaceManifestEnvelope> {
    let mut payload = current.payload().clone();
    payload.next_run_no = seed;
    Arc::new(encode_manifest(payload).into_envelope())
}

/// Compacts `spec`'s window over the current manifest with its kept inode filter seeded
/// by `seed`, and publishes the output.
pub(crate) async fn compact_with_seed<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    seed: RunNo,
) {
    let rewritten = rewrite_with_seed(store, namespace_id, spec, seed).await;
    assert_matches!(
        publish_rewritten(store, namespace_id, spec, rewritten).await,
        CompactionUnitOutcome::Published { .. }
    );
}

/// A compaction unit's output, written but not yet published, with the keys of
/// the input segments it replaces.
pub(crate) struct RewrittenUnit {
    input_keys: BTreeSet<String>,
    compacted: GroupCompactionReport,
}

/// Rewrites `spec`'s window over the current manifest, with its kept inode
/// filter seeded by `seed`, and publishes nothing.
pub(crate) async fn rewrite_with_seed<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    seed: RunNo,
) -> RewrittenUnit {
    let current = super::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let mut basis = metadata_basis_from_manifest(store, None, &current).segments;
    basis.manifest = seeded_manifest(&current, seed);
    let input_keys = input_segment_keys(&basis.scan_runs, spec).expect("the spec's runs");
    let cancellation = MaintenanceCancellation::default();
    let compacted = run_metadata_compaction(
        &basis,
        &crate::cache::MetadataSegmentCache::unshared(0),
        namespace_id,
        spec,
        MetadataRunPolicy::default(),
        &cancellation,
    )
    .await
    .expect("compact")
    .expect("not cancelled");
    RewrittenUnit {
        input_keys,
        compacted,
    }
}

/// Publishes `rewritten`, the output of `spec`'s window, over the current
/// manifest.
pub(crate) async fn publish_rewritten<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    rewritten: RewrittenUnit,
) -> CompactionUnitOutcome {
    let namespace = super::namespace_store(store, namespace_id);
    let basis = namespace
        .load_current_basis(super::observed_now())
        .await
        .expect("load the current manifest");
    let lane = super::lane_at_manifest(basis);
    let guard = lane.lock().await;
    namespace
        .publish_compaction_in_turn(
            guard.acquired_turn(),
            CompactionWrites {
                namespace_id: namespace_id.clone(),
                spec: spec.clone(),
                input_keys: rewritten.input_keys,
                report: rewritten.compacted,
                compaction: OperationClock::start(0, super::std_timer()),
            },
            &MaintenanceCancellation::default(),
        )
        .await
        .expect("publish the compaction")
}

/// The kept inode filter of a base compaction of the inodes group over the
/// current manifest, seeded by `seed`.
pub(crate) async fn seeded_kept_inode_filter<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    seed: RunNo,
) -> KeptInodeFilter {
    let current = super::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let mut basis = metadata_basis_from_manifest(store, None, &current).segments;
    basis.manifest = seeded_manifest(&current, seed);
    load_kept_inode_filter(
        &basis,
        namespace_id,
        &MetadataCompactionSpec::new(
            MetadataFamilyGroup::Inodes,
            vec![RunNo(0)],
            0,
            CompactionPlacement::Base {
                output_seq: current.payload().head_seq,
            },
            CommitSeq(0),
        ),
        &MaintenanceCancellation::default(),
    )
    .await
    .expect("build the filter")
    .expect("not cancelled")
}
