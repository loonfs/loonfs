//! Base compactions whose kept inode filter is seeded by the test, so that an
//! assertion about removed rows doesn't depend on a filter false positive.

use crate::compaction::group_merge::run_metadata_compaction;
use crate::compaction::kept_inodes::{load_kept_inode_filter, KeptInodeFilter};
use crate::compaction::plan::{group_run_descriptors, MergePlacement, MetadataCompactionSpec};
use crate::compaction::publication::{input_segment_keys, CompactionPublication};
use crate::compaction::{CompactionUnitOutcome, MetadataCompactionCancellation};
use crate::manifest::metadata_basis_from_manifest;
use crate::manifest::publish::encode_manifest;
use crate::metadata::InodeRecord;
use crate::namespace::control::LoadedManifest;
use crate::segment::runs::{runs_newest_first, MetadataRunPolicy};
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, NamespaceManifestEnvelope, RunTier};
use loonfs_types::{
    ChangeSeq, CommitId, InodeId, InodeKind, MetadataFamilyGroup, NamespaceId, RunNo,
};
use std::assert_matches;
use std::sync::Arc;

/// Merges every run of each of `groups` into one base run, with each merge's
/// kept inode filter seeded so that it keeps no row that `hidden` inodes own.
///
/// The low bits of an inode ID are random, so a filter seeded by the next run
/// number could keep a hidden inode's row by a false positive. Choosing the
/// seed makes every merge remove every hidden row, so an assertion after it
/// doesn't depend on the IDs that a run of the test drew.
pub async fn compact_groups_without_false_positives<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    groups: &[MetadataFamilyGroup],
    hidden: &[InodeId],
) {
    for &group in groups {
        // Each merge takes at most `MAX_COMPACTION_INPUT_RUNS` runs, so a
        // group with more runs needs more than one merge.
        let mut merged_every_run = false;
        for _ in 0..8 {
            let current = super::namespace_store(store, namespace_id)
                .load_current_manifest()
                .await
                .expect("load the current manifest");
            let payload = current.payload();
            let mut runs = runs_newest_first(payload)
                .into_iter()
                .filter(|run| group_run_descriptors(run, group).next().is_some())
                .collect::<Vec<_>>();
            runs.sort_by_key(|run| (run.tier != RunTier::Base, run.run_seq));
            merged_every_run = runs.len() <= crate::limits::MAX_COMPACTION_INPUT_RUNS;
            if runs.is_empty() {
                break;
            }
            runs.truncate(crate::limits::MAX_COMPACTION_INPUT_RUNS);
            let spec = MetadataCompactionSpec::new(
                group,
                runs.iter().map(|run| run.run_no).collect(),
                runs.iter()
                    .flat_map(|run| group_run_descriptors(run, group))
                    .map(|descriptor| descriptor.row_count)
                    .sum(),
                MergePlacement::Base {
                    output_seq: payload.head_seq,
                },
                payload.retention_floor_seq,
            );
            let mut seed = None;
            for offset in 0..64 {
                let candidate = RunNo(payload.next_run_no.0 + offset);
                let filter = seeded_kept_inode_filter(store, namespace_id, candidate).await;
                if hidden
                    .iter()
                    .all(|inode_id| !filter.keeps(&inode_probe(*inode_id)))
                {
                    seed = Some(candidate);
                    break;
                }
            }
            let seed = seed.expect("a seed whose kept inode filter keeps no hidden row");
            compact_with_seed(store, namespace_id, &spec, seed).await;
            if merged_every_run {
                break;
            }
        }
        assert!(merged_every_run, "{group:?} merges into one base run");
    }
}

/// An inode row for `inode_id`, which a kept inode filter keeps exactly when
/// it keeps every row that the inode owns.
fn inode_probe(inode_id: InodeId) -> MetadataRow {
    MetadataRow::Inode(InodeRecord {
        inode_id,
        inode_kind: InodeKind::File,
        committed_seq: ChangeSeq(1),
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

/// Merges `spec` over the current manifest with its kept inode filter seeded
/// by `seed`, and publishes the output.
pub(crate) async fn compact_with_seed<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    seed: RunNo,
) {
    let current = super::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let mut basis = metadata_basis_from_manifest(store, None, &current).segments;
    basis.manifest = seeded_manifest(&current, seed);
    let input_keys = input_segment_keys(&basis.scan_runs, spec).expect("the spec's runs");
    let cancellation = MetadataCompactionCancellation::default();
    let merged = run_metadata_compaction(
        &basis,
        &crate::cache::MetadataSegmentCache::unshared(0),
        namespace_id,
        spec,
        MetadataRunPolicy::default(),
        &cancellation,
    )
    .await
    .expect("merge")
    .expect("not cancelled");
    drop(basis);
    let timer = super::std_timer();
    let outcome = super::namespace_store(store, namespace_id)
        .finalize_metadata_compaction(
            None,
            spec,
            &input_keys,
            merged,
            &cancellation,
            &CompactionPublication {
                compactor_epoch: loonfs_types::CompactorEpoch(0),
                compaction: OperationClock::start(0, Arc::clone(&timer)),
                publication: OperationClock::start(0, timer),
            },
        )
        .await
        .expect("publish the merge");
    assert_matches!(outcome, CompactionUnitOutcome::Published { .. });
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
            MergePlacement::Base {
                output_seq: current.payload().head_seq,
            },
            ChangeSeq(0),
        ),
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("build the filter")
    .expect("not cancelled")
}
