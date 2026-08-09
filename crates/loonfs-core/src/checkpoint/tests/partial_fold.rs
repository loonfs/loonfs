//! Folding a family group one slice at a time: the partition grammar, the
//! walk executor, the oracle that says a walk and a whole-group fold reach
//! the same place, and the whole arc through the maintenance step.

use super::super::partial_fold::{
    self, MetadataFoldSliceDrops, MetadataFoldSliceReport, MetadataFoldWalk,
    MetadataFoldWalkOutcome,
};
use super::super::partition::{GroupPartitioning, PartitionCursor, PartitionKey};
use super::super::row::manifest_row_commit_seq;
use super::super::scan::VerifiedMetadataTables;
use super::index_parity::{load_perturbed_manifest, reorganize_output_segment};
use super::*;
use crate::path::read::{
    load_metadata_view, resolve_current_files, CurrentFileState, ReadLoadContext,
};
use crate::timing::StdMonotonicTimer;
use loonfs_api::wire::manifest::{
    ActiveDeletionRowAction, TombstoneGeneration, TombstoneRowAction,
};
use loonfs_api::{AttributeRevisionNo, Attributes, DisplayName, InodeKind};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

// -------------------------------------------------------------------------
// The partition grammar
// -------------------------------------------------------------------------

/// Every reorganization family group must have a partition grammar, and
/// every family in a group must read the same partition space, or one cursor
/// could not bound them all.
#[test]
fn every_family_group_has_a_partition_grammar() {
    for group in REORGANIZE_FAMILY_GROUPS {
        let partitioning = GroupPartitioning::for_group(group)
            .unwrap_or_else(|| panic!("family group {group:?} has no partition grammar"));
        let first = partitioning.first_cursor();
        let spelled = partitioning.spell_cursor(&first);
        assert_eq!(
            partitioning.parse_cursor(&spelled).expect("parse"),
            first,
            "{group:?}: the first cursor must survive a round trip"
        );
        let end = partitioning.spell_cursor(&PartitionCursor::End);
        assert_eq!(
            partitioning.parse_cursor(&end).expect("parse"),
            PartitionCursor::End,
            "{group:?}: the end cursor must survive a round trip"
        );
        // Every family's bound must sit at or below its own rows and, at the
        // end, above all of them.
        for family in group {
            assert!(
                partitioning
                    .family_lower_bound(*family, &first)
                    .starts_with(family.row_key_prefix()),
                "{group:?}: the bound for {family:?} must be a key of that family"
            );
            assert!(
                partitioning.family_lower_bound(*family, &PartitionCursor::End)
                    > partitioning.family_lower_bound(*family, &first),
                "{group:?}: the end bound for {family:?} must sort above the first"
            );
        }
    }
}

/// The invariant every drop rule rests on: two rows where one cancels the
/// other land in the same partition, so a slice that holds one holds both.
#[test]
fn an_unbind_shares_a_partition_with_the_bind_it_cancels() {
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let partitioning = GroupPartitioning::for_group(group).expect("grammar");
    let parent = InodeId(7);
    let name_key = NameKey::parse("report.txt").expect("name key");
    let display_name = DisplayName::parse("report.txt").expect("display name");
    let bind = MetadataRow::DirentryBind {
        parent_inode_id: parent,
        name_key: name_key.clone(),
        display_name: display_name.clone(),
        child_inode_id: InodeId(42),
        bind_seq: ChangeSeq(11),
        bind_delta_index: 0,
    };
    let unbind = MetadataRow::DirentryUnbind {
        parent_inode_id: parent,
        name_key,
        display_name,
        child_inode_id: InodeId(42),
        bind_seq: ChangeSeq(11),
        bind_delta_index: 0,
        unbind_seq: ChangeSeq(19),
        unbind_delta_index: 0,
    };

    assert_eq!(
        partitioning.partition_of_row(ApiMetadataTableFamily::DirentryBinds, &bind),
        partitioning.partition_of_row(ApiMetadataTableFamily::DirentryUnbinds, &unbind),
        "an unbind and the bind it cancels must fold in one slice"
    );
    // The reverse index is the documented exception: it keys by child, so it
    // shares the group's partition space without sharing a partition with
    // the forward row. Its drops are resolved against the frozen unbind set
    // instead of against the slice.
    assert_ne!(
        partitioning.partition_of_row(ApiMetadataTableFamily::DirentryChildBinds, &bind),
        partitioning.partition_of_row(ApiMetadataTableFamily::DirentryBinds, &bind),
        "this test only means something while the reverse index is keyed by child"
    );
}

#[test]
fn a_removal_marker_shares_a_partition_with_the_deletion_it_cancels() {
    let group = group_containing(ApiMetadataTableFamily::ActiveDeletions);
    let partitioning = GroupPartitioning::for_group(group).expect("grammar");
    let listed = MetadataRow::ActiveDeletion {
        root_inode_id: InodeId(42),
        deleted_at_seq: ChangeSeq(11),
        action: ActiveDeletionRowAction::Listed {
            deleted_at_ms: 1_000,
            deleted_direntry: None,
        },
    };
    let removed = MetadataRow::ActiveDeletion {
        root_inode_id: InodeId(42),
        deleted_at_seq: ChangeSeq(11),
        action: ActiveDeletionRowAction::Removed {
            revoked_at_seq: ChangeSeq(19),
        },
    };

    assert_eq!(
        partitioning.partition_of_row(ApiMetadataTableFamily::ActiveDeletions, &listed),
        partitioning.partition_of_row(ApiMetadataTableFamily::ActiveDeletions, &removed),
        "a removal marker and the listed row it cancels must fold in one slice"
    );
    // The pair sorts together because the marker repeats its target's
    // sequence, which is the family's leading key component. The root inode
    // sits second and does not partition the family.
    assert_eq!(
        partitioning.partition_of_row(ApiMetadataTableFamily::ActiveDeletions, &listed),
        Some(PartitionKey::Number(11)),
    );
}

#[test]
fn an_attribute_revision_shares_a_partition_with_the_revisions_it_supersedes() {
    let group = group_containing(ApiMetadataTableFamily::Attributes);
    let partitioning = GroupPartitioning::for_group(group).expect("grammar");
    let revision_of = |revision: u64, seq: u64| MetadataRow::AttributesRevision {
        inode_id: InodeId(42),
        attributes_revision_no: AttributeRevisionNo(revision),
        committed_seq: ChangeSeq(seq),
        delta_index: 0,
        attributes: Attributes::default(),
    };

    assert_eq!(
        partitioning.partition_of_row(ApiMetadataTableFamily::Attributes, &revision_of(1, 5)),
        partitioning.partition_of_row(ApiMetadataTableFamily::Attributes, &revision_of(2, 9)),
        "an attribute revision and the revisions it supersedes must fold in one slice"
    );
}

/// The two-family groups partition on the identity their index shares with
/// its canonical family, so a slice always holds both sides of a row.
#[test]
fn a_secondary_index_row_shares_a_partition_with_the_row_it_indexes() {
    let group = group_containing(ApiMetadataTableFamily::Revisions);
    let partitioning = GroupPartitioning::for_group(group).expect("grammar");
    let revision = MetadataRow::Revision {
        inode_id: InodeId(42),
        revision_no: RevisionNo(3),
        committed_seq: ChangeSeq(11),
        committed_at_ms: 11_000,
        revision_delta_index: 0,
        content_ref: loonfs_api::ContentRef::blob_v1(
            loonfs_api::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                .expect("content id"),
            b"partition sample",
        ),
    };

    assert_eq!(
        partitioning.partition_of_row(ApiMetadataTableFamily::Revisions, &revision),
        partitioning.partition_of_row(ApiMetadataTableFamily::RevisionsByInodeDesc, &revision),
    );
    assert_eq!(
        partitioning.partition_of_row(ApiMetadataTableFamily::Revisions, &revision),
        Some(PartitionKey::Number(42)),
    );
}

/// The single-family groups partition on their own leading key component,
/// which is the inode for inodes and attributes, the root inode for
/// tombstones, and the receipt id for receipts.
#[test]
fn single_family_groups_partition_on_their_leading_key_component() {
    let inode_group = group_containing(ApiMetadataTableFamily::Inodes);
    let inodes = GroupPartitioning::for_group(inode_group).expect("grammar");
    assert_eq!(
        inodes.partition_of_row(
            ApiMetadataTableFamily::Inodes,
            &MetadataRow::Inode {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(3),
            }
        ),
        Some(PartitionKey::Number(42)),
    );

    let tombstone_group = group_containing(ApiMetadataTableFamily::Tombstones);
    let tombstones = GroupPartitioning::for_group(tombstone_group).expect("grammar");
    assert_eq!(
        tombstones.partition_of_row(
            ApiMetadataTableFamily::Tombstones,
            &MetadataRow::Tombstone {
                root_inode_id: InodeId(42),
                generation: TombstoneGeneration {
                    seq: ChangeSeq(11),
                    delta_index: 0,
                },
                action: TombstoneRowAction::Set {
                    deleted_direntry: None,
                },
                deleted_at_ms: 11_000,
            }
        ),
        Some(PartitionKey::Number(42)),
    );

    let receipt_group = group_containing(ApiMetadataTableFamily::CommitReceipts);
    let receipts = GroupPartitioning::for_group(receipt_group).expect("grammar");
    let commit_id = CommitId::parse("c_00000000000000000000000000000001").expect("commit id");
    let receipt_partition = receipts
        .partition_of_row(
            ApiMetadataTableFamily::CommitReceipts,
            &MetadataRow::CommitReceipt {
                commit_id: commit_id.clone(),
                semantic_commit_fingerprint: "sha256:unused".to_owned(),
                committed_seq: ChangeSeq(11),
                committed_at_ms: 11_000,
                message: None,
            },
        )
        .expect("a receipt row has a partition");
    assert_eq!(
        receipt_partition,
        PartitionKey::ReceiptId(loonfs_api::wire::manifest::hex_encode_row_key_component(
            commit_id.as_str()
        )),
    );
    // Receipt ids are variable length, so the boundary after one is the
    // shortest hex string above it: nothing sorts between the two.
    let after = receipts.cursor_after(&receipt_partition);
    assert!(
        receipts.spell_cursor(&after)
            > MetadataRow::CommitReceipt {
                commit_id,
                semantic_commit_fingerprint: "sha256:unused".to_owned(),
                committed_seq: ChangeSeq(u64::MAX),
                committed_at_ms: 0,
                message: None,
            }
            .row_key_for_family(ApiMetadataTableFamily::CommitReceipts),
        "the boundary after a receipt must sort above every row of that receipt"
    );
}

/// The cursor is a boundary between partitions, so a spelling with anything
/// else in it is not a position a fold may resume from.
#[test]
fn a_cursor_that_is_not_a_partition_boundary_does_not_parse() {
    let group = group_containing(ApiMetadataTableFamily::Revisions);
    let partitioning = GroupPartitioning::for_group(group).expect("grammar");
    for spelled in [
        "",
        "inode-00000000000000000009",
        "revision-",
        "revision-9",
        "revision-000000000000000000009",
        "revision-0000000000000000000x",
        "revision-00000000000000000009-00000000000000000001-0000000000",
    ] {
        assert!(
            partitioning.parse_cursor(spelled).is_err(),
            "`{spelled}` must not parse as a revisions-group cursor"
        );
    }
    assert_eq!(
        partitioning
            .parse_cursor("revision-00000000000000000009")
            .expect("a padded inode id is a boundary"),
        PartitionCursor::At(PartitionKey::Number(9)),
    );
}

fn group_containing(family: ApiMetadataTableFamily) -> &'static [ApiMetadataTableFamily] {
    REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .find(|group| group.contains(&family))
        .expect("every family belongs to a reorganization group")
}

// -------------------------------------------------------------------------
// The walk
// -------------------------------------------------------------------------

/// A namespace whose bindings group holds many parent directories, deletions
/// and moves below the retention floor, and more of both above it — enough
/// for a walk to cross many partitions and for the drop rules to have
/// something to do.
async fn seed_bindings_workload(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // Below the floor: files created, some deleted, some moved. The
    // deletions and moves leave unbinds the fold may drop.
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
    // with the bindings the fold drops.
    delete_path(store, namespace_id, "/d5", &context, None)
        .await
        .expect("delete directory");
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the deletions");

    // Fold everything into one base run, so the walk's snapshot has a base
    // under its delta runs the way a real over-budget group does. The base is
    // cut into small segments on purpose: a plan prices whole data blocks, so
    // small segments give it the granularity a small test budget needs.
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
    // delta runs the walk merges with the base.
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

/// The runs a walk snapshots: every run of the manifest that holds rows of
/// the group, which is what makes the input bottom-anchored and its drops
/// legal.
fn snapshot_runs_for_group(
    manifest: &NamespaceManifestEnvelope,
    group: &[ApiMetadataTableFamily],
) -> Vec<MetadataRunManifest> {
    runs_in_scan_order(&manifest.payload)
        .into_iter()
        .filter(|run| {
            run.tables
                .iter()
                .any(|table| group.contains(&table.family) && !table.segments.is_empty())
        })
        .collect()
}

/// How a test drives the walk between steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkDriver {
    /// One executor for the whole walk.
    KeepExecutor,
    /// The executor is dropped at every step boundary and rebuilt from the
    /// manifest that survived, which is what a crashed process does.
    ResumeEveryStep,
}

struct WalkRun {
    slices: Vec<MetadataFoldSliceReport>,
    /// The group's rows, per family, read from `metadata_files` after every
    /// step — the rows a reader would see at that moment.
    visible_rows_per_step: Vec<BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>>>,
}

/// Runs a partial fold of `group` to completion against the namespace's
/// current manifest.
async fn run_partial_fold(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    group: &'static [ApiMetadataTableFamily],
    policy: MetadataLsmPolicy,
    driver: WalkDriver,
) -> WalkRun {
    let context = test_context();
    let timer = StdMonotonicTimer::default();
    let frozen_floor_seq = read_floor_seq(store, namespace_id).await;
    let mut run = WalkRun {
        slices: Vec::new(),
        visible_rows_per_step: Vec::new(),
    };

    let mut tables = load_current_manifest_tables(store, namespace_id).await;
    let snapshot = snapshot_runs_for_group(tables.manifest(), group);
    assert!(
        snapshot.len() > 1,
        "this test needs a base run under at least one delta run"
    );
    let mut walk = MetadataFoldWalk::start(&tables, group, snapshot, frozen_floor_seq)
        .expect("start a partial fold");

    for _step in 0..256 {
        let outcome = walk
            .advance(store, namespace_id, &tables, policy, &context, &timer)
            .await
            .expect("advance the partial fold");
        tables = load_current_manifest_tables(store, namespace_id).await;
        match outcome {
            MetadataFoldWalkOutcome::SlicePublished(report) => {
                run.slices.push(report);
                run.visible_rows_per_step
                    .push(group_rows_from_manifest(&tables, group).await);
                if driver == WalkDriver::ResumeEveryStep {
                    drop(walk);
                    walk = MetadataFoldWalk::resume_from_manifest(&tables)
                        .expect("resume a partial fold")
                        .expect("the manifest must still carry the fold");
                }
            }
            MetadataFoldWalkOutcome::Completed {
                output_segments,
                output_rows,
                ..
            } => {
                assert!(
                    tables.manifest().payload.reorganize.is_none(),
                    "the completing publication must clear the fold's state"
                );
                assert_eq!(
                    output_rows,
                    run.slices
                        .iter()
                        .map(|slice| slice.output_rows)
                        .sum::<u64>(),
                    "the finished run must hold what every slice wrote and nothing else"
                );
                let finished: Vec<_> = tables
                    .manifest()
                    .payload
                    .metadata_files
                    .iter()
                    .filter(|descriptor| group.contains(&descriptor.family))
                    .collect();
                assert_eq!(finished.len(), output_segments);
                assert_eq!(
                    finished
                        .iter()
                        .map(|descriptor| descriptor.row_count)
                        .sum::<u64>(),
                    output_rows
                );
                return run;
            }
            MetadataFoldWalkOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
        }
    }
    panic!("a partial fold must finish in a bounded number of steps");
}

/// The group's rows a reader sees right now: read from `metadata_files`,
/// which is exactly what a scan concatenates.
async fn group_rows_from_manifest<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    group: &[ApiMetadataTableFamily],
) -> BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>> {
    let mut rows_by_family = BTreeMap::new();
    for family in group {
        let mut rows = tables
            .scan_prefix(*family, "")
            .await
            .expect("scan the group");
        rows.sort_by_key(|row| row.row_key_for_family(*family));
        rows_by_family.insert(*family, rows);
    }
    rows_by_family
}

async fn group_rows_of_current_manifest(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    group: &[ApiMetadataTableFamily],
) -> BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>> {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    group_rows_from_manifest(&tables, group).await
}

/// The tables of whatever manifest the namespace's root names right now.
async fn load_current_manifest_tables<'a>(
    store: &'a LocalFsStore,
    namespace_id: &NamespaceId,
) -> VerifiedMetadataTables<'a, LocalFsStore> {
    let manifest_object_id = current_manifest_object_id(store, namespace_id).await;
    load_verified_manifest_tables(store, namespace_id, &manifest_object_id)
        .await
        .expect("load the current manifest's tables")
}

async fn current_metadata_state(store: &LocalFsStore, namespace_id: &NamespaceId) -> MetadataState {
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

/// Copies a local store's whole object tree, so two folds can start from
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

/// A budget that admits the whole group in one step, so the ordinary fold
/// can rebuild in one unit whatever the walk needed many steps for.
fn fold_everything_policy() -> MetadataLsmPolicy {
    MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(4_000_000).expect("nonzero"),
        max_decoded_input_bytes_per_step: NonZeroUsize::new(1 << 30).expect("nonzero"),
        max_input_runs_per_step: NonZeroUsize::new(64).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    }
}

/// A budget that forces the walk to take many small slices while leaving
/// every partition of these workloads foldable whole.
fn small_slice_policy() -> MetadataLsmPolicy {
    MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(32).expect("nonzero"),
        max_decoded_input_bytes_per_step: NonZeroUsize::new(1 << 20).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    }
}

/// The oracle. A walk in many bounded steps and a whole-group fold with the
/// budgets raised are two ways of doing the same thing, so they must land in
/// the same place: the same surviving rows in every family of the group, the
/// same materialized namespace, and the same rows dropped.
#[tokio::test]
async fn a_walk_and_a_whole_group_fold_reach_the_same_rows() {
    let walk_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(walk_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;

    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let frozen_floor_seq = read_floor_seq(&store, &namespace_id).await;

    // The same durable bytes, folded the ordinary way.
    let fold_dir = tempdir().expect("tempdir");
    copy_store_tree(walk_dir.path(), fold_dir.path());
    let fold_store = LocalFsStore::new(fold_dir.path()).expect("store");
    let report = super::super::reorganize_metadata_step(
        &fold_store,
        &namespace_id,
        &test_context(),
        fold_everything_policy(),
    )
    .await
    .expect("fold the group whole");
    let folded_families = match report.outcome {
        MetadataReorganizeOutcome::UnitPublished { families, .. } => families,
        other => panic!("the whole-group fold must publish a unit, got {other:?}"),
    };
    assert_eq!(
        folded_families, group,
        "both folds must work on the same family group"
    );
    let folded = group_rows_of_current_manifest(&fold_store, &namespace_id, group).await;
    let folded_state = current_metadata_state(&fold_store, &namespace_id).await;

    let run = run_partial_fold(
        &store,
        &namespace_id,
        group,
        small_slice_policy(),
        WalkDriver::KeepExecutor,
    )
    .await;
    assert!(
        run.slices.len() > 3,
        "the budget must force many slices, got {}",
        run.slices.len()
    );
    assert!(
        run.slices
            .iter()
            .all(|slice| slice.drops == MetadataFoldSliceDrops::Applied),
        "every partition of this workload fits one step, so every slice runs every rule"
    );
    // The reverse index is what the walk reads the snapshot for, and it reads
    // it exactly once per reverse row at or below the frozen floor. A row
    // above the floor survives whatever retired it later and costs nothing,
    // which is what bounds the reads a slice makes.
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
    assert_eq!(
        run.slices
            .iter()
            .map(|slice| slice.unbind_probes)
            .sum::<u64>(),
        reverse_rows_at_or_below_floor,
        "one point read per reverse row at or below the floor, and none for the rest"
    );
    let walked = group_rows_of_current_manifest(&store, &namespace_id, group).await;

    assert_eq!(
        walked, folded,
        "a walk and a whole-group fold must keep the same rows"
    );
    assert!(
        metadata_states_equivalent(
            &current_metadata_state(&store, &namespace_id).await,
            &folded_state
        ),
        "a walk and a whole-group fold must materialize the same namespace"
    );
    // The format gives every bind row exactly one reverse row, and manifest
    // load rejects a run whose two counts disagree. A walk drops the two
    // families in lockstep, and this is what says so: dropping one side
    // alone would read correctly and still fail to publish.
    assert_eq!(
        walked[&ApiMetadataTableFamily::DirentryBinds].len(),
        walked[&ApiMetadataTableFamily::DirentryChildBinds].len(),
        "a walk must never leave a reverse row without its forward row"
    );

    // Teeth: the folds must actually have dropped something, or the
    // comparison above is comparing two copies of the input.
    let rows_of = |rows: &BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>>| {
        rows.values().map(Vec::len).sum::<usize>()
    };
    assert!(
        rows_of(&walked) < rows_of(&before),
        "the fold must drop rows for this comparison to mean anything: {} before, {} after",
        rows_of(&before),
        rows_of(&walked)
    );
    // What a fold may drop is bounded from both sides: it never invents a
    // row, and it never touches one the retention floor still covers.
    for (family, rows) in &walked {
        let input: BTreeSet<String> = before[family]
            .iter()
            .map(|row| row.row_key_for_family(*family))
            .collect();
        for row in rows {
            assert!(
                input.contains(&row.row_key_for_family(*family)),
                "the fold wrote a {family:?} row its input did not hold"
            );
        }
    }
    for (family, rows) in &before {
        let survivors: BTreeSet<String> = walked[family]
            .iter()
            .map(|row| row.row_key_for_family(*family))
            .collect();
        for row in rows {
            if manifest_row_commit_seq(row) > frozen_floor_seq {
                assert!(
                    survivors.contains(&row.row_key_for_family(*family)),
                    "the fold dropped a {family:?} row above the retention floor"
                );
            }
        }
    }
}

/// A walk interrupted at a step boundary resumes from the manifest and
/// nothing else. Rebuilding the executor at every boundary must reach the
/// same place an uninterrupted walk reaches.
#[tokio::test]
async fn a_walk_resumed_at_every_step_boundary_lands_where_it_would_have() {
    let uninterrupted_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(uninterrupted_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);

    let resumed_dir = tempdir().expect("tempdir");
    copy_store_tree(uninterrupted_dir.path(), resumed_dir.path());
    let resumed_store = LocalFsStore::new(resumed_dir.path()).expect("store");

    let straight = run_partial_fold(
        &store,
        &namespace_id,
        group,
        small_slice_policy(),
        WalkDriver::KeepExecutor,
    )
    .await;
    let resumed = run_partial_fold(
        &resumed_store,
        &namespace_id,
        group,
        small_slice_policy(),
        WalkDriver::ResumeEveryStep,
    )
    .await;

    assert_eq!(
        straight.slices.len(),
        resumed.slices.len(),
        "a resumed walk must take the same slices"
    );
    for (straight_slice, resumed_slice) in straight.slices.iter().zip(&resumed.slices) {
        assert_eq!(
            (
                straight_slice.partitions,
                straight_slice.decoded_input_rows,
                straight_slice.output_rows,
                straight_slice.drops,
            ),
            (
                resumed_slice.partitions,
                resumed_slice.decoded_input_rows,
                resumed_slice.output_rows,
                resumed_slice.drops,
            ),
            "every step must decide the same way after a resume"
        );
    }
    assert_eq!(
        group_rows_of_current_manifest(&store, &namespace_id, group).await,
        group_rows_of_current_manifest(&resumed_store, &namespace_id, group).await,
        "a resumed walk must keep the same rows"
    );
}

/// Rows appear exactly once. The outputs a walk accumulates are invisible
/// until the swap, so every manifest it publishes on the way answers a scan
/// of the group with exactly the rows the manifest before the walk answered.
#[tokio::test]
async fn a_walk_in_flight_never_changes_what_a_scan_returns() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;

    let run = run_partial_fold(
        &store,
        &namespace_id,
        group,
        small_slice_policy(),
        WalkDriver::KeepExecutor,
    )
    .await;

    assert!(!run.visible_rows_per_step.is_empty());
    for (index, visible) in run.visible_rows_per_step.iter().enumerate() {
        assert_eq!(
            *visible, before,
            "step {index} changed what a scan of the group returns"
        );
    }
}

/// A budget so small that every partition has to be folded in pieces still
/// reaches the rows a whole-group fold reaches.
///
/// This is the sharp version of the lockstep claim. A piece holds one
/// family's rows and nothing else, so both bind families are decided by
/// point reads into the snapshot rather than from the slice, and the forward
/// row and the reverse row that indexes it are decided in different steps.
/// They must still agree row for row, because the format gives every bind
/// row exactly one reverse row and a run whose two counts disagree does not
/// load.
#[tokio::test]
async fn a_walk_that_folds_every_partition_in_pieces_reaches_the_same_rows() {
    let walk_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(walk_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);

    let fold_dir = tempdir().expect("tempdir");
    copy_store_tree(walk_dir.path(), fold_dir.path());
    let fold_store = LocalFsStore::new(fold_dir.path()).expect("store");
    super::super::reorganize_metadata_step(
        &fold_store,
        &namespace_id,
        &test_context(),
        fold_everything_policy(),
    )
    .await
    .expect("fold the group whole");
    let folded = group_rows_of_current_manifest(&fold_store, &namespace_id, group).await;

    // A byte budget no data block can fit in, so no partition is ever folded
    // whole.
    let piecemeal_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(64).expect("nonzero"),
        max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    let run = run_partial_fold(
        &store,
        &namespace_id,
        group,
        piecemeal_policy,
        WalkDriver::ResumeEveryStep,
    )
    .await;
    assert!(
        run.slices
            .iter()
            .any(|slice| slice.drops == MetadataFoldSliceDrops::PartitionPiece),
        "a budget this small must force pieces"
    );
    assert!(
        run.slices.iter().any(|slice| slice.unbind_probes > 0),
        "a piece of the bindings group decides its bind rows by point read"
    );

    let walked = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    assert_eq!(
        walked, folded,
        "a walk that never saw a whole partition must still reach the fold's rows"
    );
    assert_eq!(
        walked[&ApiMetadataTableFamily::DirentryBinds].len(),
        walked[&ApiMetadataTableFamily::DirentryChildBinds].len(),
        "the two bind families must drop in lockstep however the walk is sliced"
    );
    // Folding in pieces still does the thing a frozen base could not: it
    // rebuilds the group into one run.
    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    assert_eq!(
        snapshot_runs_for_group(tables.manifest(), group).len(),
        1,
        "the walk must leave the group in one run"
    );
}

/// A walk over a group with no unbind rule needs no unbind set at all; it
/// is a straight rewrite in partitions. Revisions are never dropped, so the
/// walk's output must hold every input row.
#[tokio::test]
async fn a_walk_over_the_revisions_group_rewrites_every_row_it_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::Revisions);
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;

    let run = run_partial_fold(
        &store,
        &namespace_id,
        group,
        small_slice_policy(),
        WalkDriver::ResumeEveryStep,
    )
    .await;
    assert!(run.slices.len() > 1);
    assert!(
        run.slices.iter().all(|slice| slice.unbind_probes == 0),
        "the revisions group has no bind rule, so no slice of it reads an unbind"
    );

    assert_eq!(
        group_rows_of_current_manifest(&store, &namespace_id, group).await,
        before,
        "revision rows are durable history and are never dropped"
    );
}

/// The state a walk publishes is the state the loader validates and the
/// state a resume reads back: same group, same snapshot, same output
/// identity, same frozen floor, and a cursor that advances every step.
#[tokio::test]
async fn a_walk_publishes_the_state_a_resume_reads_back() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let policy = small_slice_policy();
    let context = test_context();
    let timer = StdMonotonicTimer::default();
    let frozen_floor_seq = read_floor_seq(&store, &namespace_id).await;

    let mut tables = load_current_manifest_tables(&store, &namespace_id).await;
    let head_seq = tables.manifest().payload.head_seq;
    let snapshot = snapshot_runs_for_group(tables.manifest(), group);
    let snapshot_ids: BTreeSet<_> = snapshot
        .iter()
        .map(|run| MetadataRunId {
            run_seq: run.run_seq,
            level: run.level,
        })
        .collect();
    let mut walk = MetadataFoldWalk::start(&tables, group, snapshot, frozen_floor_seq)
        .expect("start a partial fold");
    assert_eq!(walk.progress().cursor, "direntry-00000000000000000000");

    let mut positions = Vec::new();
    for _step in 0..64 {
        let outcome = walk
            .advance(&store, &namespace_id, &tables, policy, &context, &timer)
            .await
            .expect("advance");
        tables = load_current_manifest_tables(&store, &namespace_id).await;
        if matches!(outcome, MetadataFoldWalkOutcome::Completed { .. }) {
            break;
        }
        let progress = tables
            .manifest()
            .payload
            .reorganize
            .clone()
            .expect("a step in flight must publish its state");
        assert_eq!(progress.families, group);
        assert_eq!(
            progress.input_runs.iter().copied().collect::<BTreeSet<_>>(),
            snapshot_ids
        );
        assert_eq!(progress.output_run_seq, head_seq);
        assert_eq!(progress.output_level, CHECKPOINT_BASE_RUN_LEVEL);
        assert_eq!(progress.frozen_floor_seq, frozen_floor_seq);
        assert!(
            progress
                .output_segments
                .iter()
                .all(|segment| segment.run_seq == head_seq
                    && segment.level == CHECKPOINT_BASE_RUN_LEVEL),
            "every output segment carries the identity fixed at the start"
        );
        // The position is the cursor and, when the fold stands inside one
        // oversized partition, how far into it: that pair is what must
        // advance, and a fold working through a partition republishes the
        // same cursor with the offset moved on.
        positions.push((progress.cursor.clone(), progress.partition_offset.clone()));

        // The resumed walk must agree with the published state exactly.
        let resumed = MetadataFoldWalk::resume_from_manifest(&tables)
            .expect("resume")
            .expect("the manifest carries a fold");
        assert_eq!(*resumed.progress(), progress);
        assert_eq!(resumed.group(), group);
    }

    assert!(
        positions.len() > 2,
        "the walk must publish several positions"
    );
    let mut ascending = positions.clone();
    ascending.sort();
    ascending.dedup();
    assert_eq!(
        positions, ascending,
        "the position must advance strictly, so nothing is folded twice"
    );
    assert!(
        MetadataFoldWalk::resume_from_manifest(&tables)
            .expect("resume the finished manifest")
            .is_none(),
        "a finished walk leaves no state to resume"
    );
}

/// The completing publication is the swap: the snapshot's group segments
/// leave `metadata_files` in the same step the finished run enters it, and
/// runs that arrived during the walk survive untouched.
#[tokio::test]
async fn the_completing_step_swaps_the_snapshot_for_the_run_it_built() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let before = tables.manifest().payload.clone();
    let snapshot_keys: BTreeSet<String> = before
        .metadata_files
        .iter()
        .filter(|descriptor| group.contains(&descriptor.family))
        .map(|descriptor| descriptor.object_key.clone())
        .collect();
    let untouched_keys: BTreeSet<String> = before
        .metadata_files
        .iter()
        .filter(|descriptor| !group.contains(&descriptor.family))
        .map(|descriptor| descriptor.object_key.clone())
        .collect();
    assert!(!snapshot_keys.is_empty() && !untouched_keys.is_empty());
    drop(tables);

    run_partial_fold(
        &store,
        &namespace_id,
        group,
        small_slice_policy(),
        WalkDriver::KeepExecutor,
    )
    .await;

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let after = &tables.manifest().payload;
    let after_keys: BTreeSet<String> = after
        .metadata_files
        .iter()
        .map(|descriptor| descriptor.object_key.clone())
        .collect();
    assert!(
        snapshot_keys.is_disjoint(&after_keys),
        "the snapshot's segments must leave the file set"
    );
    assert!(
        untouched_keys.is_subset(&after_keys),
        "segments outside the group must survive the swap untouched"
    );
    assert!(after.reorganize.is_none());
    let group_runs: BTreeSet<(ChangeSeq, u32)> = after
        .metadata_files
        .iter()
        .filter(|descriptor| group.contains(&descriptor.family))
        .map(|descriptor| (descriptor.run_seq, descriptor.level))
        .collect();
    assert_eq!(
        group_runs,
        BTreeSet::from([(before.head_seq, CHECKPOINT_BASE_RUN_LEVEL)]),
        "the group must be left in the one run the walk built, at the identity it fixed"
    );
    assert_eq!(
        after.base_seq,
        after
            .metadata_files
            .iter()
            .map(|descriptor| descriptor.run_seq)
            .min()
            .expect("the manifest names files"),
        "the swap must restate the manifest's oldest run"
    );
}

// -------------------------------------------------------------------------
// Through the maintenance step
// -------------------------------------------------------------------------

async fn write_seed_file(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    directory: u64,
    file: u64,
) {
    write_file_bytes(
        store,
        namespace_id,
        &format!("/d{directory}/f{file}.txt"),
        format!("body {directory}/{file}\n").as_bytes(),
        &test_context(),
        None,
    )
    .await
    .expect("write file");
}

/// A namespace whose bindings group cannot be folded in one step.
///
/// Three things have to line up for the whole arc to be visible. The base
/// run must be larger than the budget, or nothing starts a partial fold.
/// Delta runs must sit above it, or there is nothing left to merge normally
/// afterwards. And most of the group's rows must be churn below the
/// retention floor, so that folding the group shrinks it back under the
/// budget and the condition that started the fold stops holding.
///
/// The base is cut into small segments on purpose: a slice stops at a
/// partition boundary the planner can see in a segment index, so many small
/// segments give it many places to stop.
async fn seed_over_budget_bindings_group(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> MetadataLsmPolicy {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context, false)
        .await
        .expect("bootstrap");

    for directory in 0..7u64 {
        for file in 0..4u64 {
            write_seed_file(store, namespace_id, directory, file).await;
        }
        create_checkpoint(store, namespace_id, &context)
            .await
            .expect("checkpoint");
    }
    let seed_policy = MetadataLsmPolicy {
        max_rows_per_segment: NonZeroUsize::new(8).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    };
    drain_reorganization(store, namespace_id, &context, seed_policy).await;

    for directory in 7..10u64 {
        for file in 0..4u64 {
            write_seed_file(store, namespace_id, directory, file).await;
        }
        create_checkpoint(store, namespace_id, &context)
            .await
            .expect("checkpoint");
    }
    for directory in 0..10u64 {
        for file in 0..3u64 {
            delete_path(
                store,
                namespace_id,
                &format!("/d{directory}/f{file}.txt"),
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
    advance_retention_floor(store, namespace_id, &context)
        .await
        .expect("advance the floor past the deletions");

    // One last delta run, written above the floor, so the fold has rows it
    // must carry through untouched as well as rows it may drop.
    for file in 0..2u64 {
        write_seed_file(store, namespace_id, 10, file).await;
    }
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the late writes");
    seed_policy
}

/// A budget one row short of what the group's base run needs, which is the
/// condition that starts a partial fold.
async fn policy_that_cannot_fold_the_group_base(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    group: &[ApiMetadataTableFamily],
    seed_policy: MetadataLsmPolicy,
) -> MetadataLsmPolicy {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    let base_rows: u64 = tables
        .manifest()
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            descriptor.level == CHECKPOINT_BASE_RUN_LEVEL && group.contains(&descriptor.family)
        })
        .map(|descriptor| descriptor.row_count)
        .sum();
    assert!(base_rows > 1, "the base run must hold rows in this group");
    MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(
            usize::try_from(base_rows).expect("test row counts are small") - 1,
        )
        .expect("nonzero"),
        ..seed_policy
    }
}

/// Whether the step would warn about this group's oldest run.
///
/// The step logs one line when the selector reports an over-budget bottom,
/// and the selector's record is what that line reads from, so asserting on
/// the record is how these tests pin the warning.
async fn group_bottom_is_over_budget(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    group: &[ApiMetadataTableFamily],
    policy: MetadataLsmPolicy,
) -> bool {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    reorganize::select_reorganization_input(&tables, group, policy)
        .await
        .expect("select a reorganization input")
        .group_bottom_over_budget
        .is_some()
}

/// The rows a group's base-tier runs hold right now.
async fn group_base_rows(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    group: &[ApiMetadataTableFamily],
) -> u64 {
    load_current_manifest_tables(store, namespace_id)
        .await
        .manifest()
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            descriptor.level == CHECKPOINT_BASE_RUN_LEVEL && group.contains(&descriptor.family)
        })
        .map(|descriptor| descriptor.row_count)
        .sum()
}

/// The family group a manifest's partial fold names, or `None`.
async fn folding_group(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> Option<Vec<ApiMetadataTableFamily>> {
    load_current_manifest_tables(store, namespace_id)
        .await
        .manifest()
        .payload
        .reorganize
        .as_ref()
        .map(|progress| progress.families.clone())
}

/// Two groups over budget at once, folded one after the other.
///
/// A manifest carries one partial fold at a time. Before this was enforced,
/// the step that selected the second over-budget group started a fold for it
/// and overwrote the first group's state, so two such groups took turns
/// discarding each other's work and neither ever finished. The second group
/// now waits: it goes on merging its delta runs the way it did before partial
/// folds existed, its oldest run keeps failing the budget so the warning
/// keeps firing for it, and its own fold starts once the slot is free.
#[tokio::test]
async fn a_second_over_budget_group_waits_for_the_fold_in_flight() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_bindings_workload(&store, &namespace_id).await;
    let bindings = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let revisions = group_containing(ApiMetadataTableFamily::Revisions);

    // One row under the smaller of the two base runs, so neither group's
    // oldest run fits one step and both want a fold.
    let smallest_base = group_base_rows(&store, &namespace_id, bindings)
        .await
        .min(group_base_rows(&store, &namespace_id, revisions).await);
    assert!(smallest_base > 1, "both groups must hold base rows");
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(
            usize::try_from(smallest_base).expect("test row counts are small") - 1,
        )
        .expect("nonzero"),
        ..MetadataLsmPolicy::default()
    };
    for group in [bindings, revisions] {
        assert!(
            group_bottom_is_over_budget(&store, &namespace_id, group, policy).await,
            "{group:?} must not fit one step, or only one group wants a fold"
        );
    }
    let mut visible = visible_namespace(&store, &namespace_id).await;

    let mut completions: Vec<Vec<ApiMetadataTableFamily>> = Vec::new();
    let mut units_while_waiting: BTreeMap<Vec<ApiMetadataTableFamily>, usize> = BTreeMap::new();
    let mut warned_while_waiting: BTreeSet<Vec<ApiMetadataTableFamily>> = BTreeSet::new();
    let mut nudges = 0;
    let both_folded = |completions: &[Vec<ApiMetadataTableFamily>]| {
        [bindings, revisions]
            .into_iter()
            .all(|group| completions.iter().any(|folded| folded == group))
    };
    for _step in 0..512 {
        let in_flight = folding_group(&store, &namespace_id).await;
        let report = reorganize_metadata_step(&store, &namespace_id, &context, policy)
            .await
            .expect("reorganization step");
        match report.outcome {
            MetadataReorganizeOutcome::PartialFoldAdvanced { ref families, .. } => {
                assert!(
                    in_flight.is_none() || in_flight.as_deref() == Some(families.as_slice()),
                    "a step advanced {families:?} while {in_flight:?} was in flight"
                );
            }
            MetadataReorganizeOutcome::PartialFoldCompleted { ref families, .. } => {
                completions.push(families.clone());
            }
            MetadataReorganizeOutcome::UnitPublished { ref families, .. } => {
                // A group over budget that merged a unit anyway is a group
                // waiting its turn, which is exactly the behaviour it had
                // before partial folds existed.
                if let Some(in_flight) = &in_flight {
                    assert_ne!(
                        families, in_flight,
                        "a group with a fold in flight folds no other way"
                    );
                    if group_bottom_is_over_budget(&store, &namespace_id, families, policy).await {
                        *units_while_waiting.entry(families.clone()).or_default() += 1;
                        warned_while_waiting.insert(families.clone());
                    }
                }
            }
            MetadataReorganizeOutcome::NotNeeded { .. } => {
                if both_folded(&completions) {
                    break;
                }
                // A group is only ever selected when it has delta runs
                // waiting, fold or no fold, so a group whose turn came after
                // it had merged its own delta runs away needs fresh pressure
                // before its fold can start. A running namespace gets that
                // from the next checkpoint; this is that checkpoint. It
                // overwrites a file that already exists, so the pressure
                // lands on the revisions group and not on the bindings
                // group, whose fold has just finished.
                assert!(nudges < 8, "the second group's fold never started");
                nudges += 1;
                write_file_bytes(
                    &store,
                    &namespace_id,
                    "/d0/f2.txt",
                    format!("nudge {nudges}\n").as_bytes(),
                    &context,
                    None,
                )
                .await
                .expect("overwrite a file");
                create_checkpoint(&store, &namespace_id, &context)
                    .await
                    .expect("checkpoint it");
                visible = visible_namespace(&store, &namespace_id).await;
            }
            other => panic!("unexpected reorganization outcome {other:?}"),
        }
        assert_eq!(
            visible_namespace(&store, &namespace_id).await,
            visible,
            "a step changed what a read answers"
        );
    }

    assert!(
        both_folded(&completions),
        "both groups must finish a fold, got {completions:?}"
    );
    // Whichever group waited was not idle and was not silent: it kept merging
    // its delta runs, and its oldest run kept failing the budget, which is
    // the line that says it is still waiting.
    assert!(
        !units_while_waiting.is_empty(),
        "a group waiting its turn must keep merging its delta runs"
    );
    assert!(
        !warned_while_waiting.is_empty(),
        "a group waiting its turn must keep failing the budget out loud"
    );

    assert!(folding_group(&store, &namespace_id).await.is_none());
    for group in [bindings, revisions] {
        let tables = load_current_manifest_tables(&store, &namespace_id).await;
        let runs: BTreeSet<(ChangeSeq, u32)> = tables
            .manifest()
            .payload
            .metadata_files
            .iter()
            .filter(|descriptor| group.contains(&descriptor.family))
            .map(|descriptor| (descriptor.run_seq, descriptor.level))
            .collect();
        assert_eq!(runs.len(), 1, "{group:?} must end in one run, got {runs:?}");
    }
}

/// Starting a walk over a manifest that already carries one is refused, not
/// allowed to replace it.
///
/// The reorganization step waits rather than reaching this, so the refusal is
/// only ever hit by a caller that got the rule wrong. It is here so that
/// caller finds out rather than stranding the segments the fold in flight had
/// written.
#[tokio::test]
async fn a_walk_refuses_to_start_over_a_fold_already_in_flight() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let timer = StdMonotonicTimer::default();
    seed_bindings_workload(&store, &namespace_id).await;
    let bindings = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let revisions = group_containing(ApiMetadataTableFamily::Revisions);
    let policy = small_slice_policy();
    let frozen_floor_seq = read_floor_seq(&store, &namespace_id).await;

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let snapshot = snapshot_runs_for_group(tables.manifest(), bindings);
    let mut walk = MetadataFoldWalk::start(&tables, bindings, snapshot, frozen_floor_seq)
        .expect("start a partial fold");
    walk.advance(&store, &namespace_id, &tables, policy, &context, &timer)
        .await
        .expect("advance one slice");
    drop(tables);

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let before = tables.manifest().payload.clone();
    assert!(before.reorganize.is_some(), "a fold must be in flight");
    let snapshot = snapshot_runs_for_group(tables.manifest(), revisions);
    let error = match MetadataFoldWalk::start(&tables, revisions, snapshot, frozen_floor_seq) {
        Ok(_) => panic!("a second fold must be refused"),
        Err(error) => error,
    };
    let CoreError::Internal(message) = &error else {
        panic!("expected an internal error, got {error:?}")
    };
    assert!(
        message.contains("Revisions") && message.contains("DirentryBinds"),
        "the refusal must name both groups, got `{message}`"
    );

    // The refusal writes nothing: the manifest the namespace stands on, and
    // the fold it carries, are exactly what they were.
    drop(tables);
    let after = load_current_manifest_tables(&store, &namespace_id)
        .await
        .manifest()
        .payload
        .clone();
    assert_eq!(
        after, before,
        "a refused start must leave the manifest alone"
    );
}

/// The group's segment keys that a fold in flight is not merging: the runs
/// that arrived above its snapshot after it started.
async fn group_segment_keys_outside_the_fold(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    group: &[ApiMetadataTableFamily],
) -> BTreeSet<String> {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    let payload = &tables.manifest().payload;
    let progress = payload
        .reorganize
        .clone()
        .expect("a fold must be in flight to have anything outside it");
    payload
        .metadata_files
        .iter()
        .filter(|descriptor| group.contains(&descriptor.family))
        .filter(|descriptor| {
            !progress
                .input_runs
                .iter()
                .any(|run| run.run_seq == descriptor.run_seq && run.level == descriptor.level)
        })
        .map(|descriptor| descriptor.object_key.clone())
        .collect()
}

async fn manifest_segment_keys(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> BTreeSet<String> {
    let tables = load_current_manifest_tables(store, namespace_id).await;
    tables
        .manifest()
        .payload
        .metadata_files
        .iter()
        .map(|descriptor| descriptor.object_key.clone())
        .collect()
}

/// What a reader sees right now: every inode the namespace knows, resolved
/// the way a read resolves it — visible or not, at what path, at what
/// revision.
///
/// This is the answer that must not move while a fold runs. Comparing rows
/// would say the opposite of what is wanted: a fold drops rows precisely
/// because no read can observe them, so the row set is meant to change and
/// this is not.
async fn visible_namespace(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> Vec<CurrentFileState> {
    let inode_ids: Vec<InodeId> = current_metadata_state(store, namespace_id)
        .await
        .inodes()
        .iter()
        .map(|inode| inode.inode_id)
        .collect();
    let view = load_metadata_view(store, namespace_id, ReadLoadContext::latest())
        .await
        .expect("load the read view");
    resolve_current_files(&view, &inode_ids)
        .await
        .expect("resolve every inode the namespace knows")
}

/// The check every reorganization step in these tests must pass: nothing a
/// reader can see moved.
async fn continue_after_step(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    visible: &[CurrentFileState],
) {
    assert_eq!(
        visible_namespace(store, namespace_id).await,
        visible,
        "a step changed what a read answers"
    );
}

/// The binding one row names: what an unbind retires, and what a bind is.
fn binding_generation(row: &MetadataRow) -> Option<(InodeId, NameKey, ChangeSeq, u32)> {
    match row {
        MetadataRow::DirentryUnbind {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            ..
        }
        | MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            ..
        } => Some((
            *parent_inode_id,
            name_key.clone(),
            *bind_seq,
            *bind_delta_index,
        )),
        _ => None,
    }
}

/// The whole arc, driven through the entry point the maintenance runner
/// calls rather than through the executor.
///
/// A group whose oldest run no longer fits one step is warned about once,
/// folded a slice at a time over the steps that follow, and left in one run
/// that the delta runs arriving meanwhile then merge with normally. Nothing
/// a reader can see moves anywhere along the way, the other groups keep
/// folding while it runs, and the warning stops once the group fits again.
#[tokio::test]
async fn an_over_budget_group_folds_through_repeated_maintenance_steps() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let seed_policy = seed_over_budget_bindings_group(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let policy =
        policy_that_cannot_fold_the_group_base(&store, &namespace_id, group, seed_policy).await;

    assert!(
        group_bottom_is_over_budget(&store, &namespace_id, group, policy).await,
        "the warning must fire before anything folds this group"
    );
    let rows_before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let mut visible = visible_namespace(&store, &namespace_id).await;
    assert!(
        visible.iter().any(|state| state.visible),
        "the seed must leave something to read"
    );

    let mut slices = 0usize;
    let mut written_rows = 0u64;
    let mut cursors = Vec::new();
    let mut completed = false;
    let mut other_groups_folded_during_the_fold = 0usize;
    let mut group_merged_after_the_fold = 0usize;
    let mut arrived_segment_keys = BTreeSet::new();

    for _step in 0..256 {
        let report = reorganize_metadata_step(&store, &namespace_id, &context, policy)
            .await
            .expect("reorganization step");
        match report.outcome {
            MetadataReorganizeOutcome::PartialFoldAdvanced {
                ref families,
                partitions,
                decoded_input_rows,
                output_rows,
                ref cursor,
                drops,
                ..
            } => {
                // A slice that covers no partition is one that stepped over
                // a stretch of the keyspace holding no row at all, and it
                // must have read nothing for it.
                assert_eq!(
                    partitions == 0,
                    decoded_input_rows == 0,
                    "a slice must read the rows of the partitions it covers"
                );
                assert_eq!(
                    drops,
                    MetadataFoldSliceDrops::Applied,
                    "every partition of this workload fits one step"
                );
                // This budget is one row under this group's base run, and
                // another group's base can be larger still, so more than one
                // group may fold in slices here. The arc below is this
                // group's, so only its slices are counted.
                if families != group {
                    continue_after_step(&store, &namespace_id, &visible).await;
                    continue;
                }
                // A slice may keep nothing: every row in its partitions can
                // be churn the frozen floor lets go.
                written_rows += output_rows;
                cursors.push(cursor.clone());
                slices += 1;
                if slices == 1 {
                    // A delta run arrives while the fold is running, under
                    // the root inode, which is a partition the fold has
                    // already passed. It is not in the fold's snapshot, so
                    // the fold never reads it and the swap must land
                    // underneath it.
                    write_file_bytes(
                        &store,
                        &namespace_id,
                        "/arrived-mid-fold.txt",
                        b"arrived mid-fold\n",
                        &context,
                        None,
                    )
                    .await
                    .expect("write a file while the fold runs");
                    create_checkpoint(&store, &namespace_id, &context)
                        .await
                        .expect("checkpoint it");
                    arrived_segment_keys =
                        group_segment_keys_outside_the_fold(&store, &namespace_id, group).await;
                    assert!(
                        !arrived_segment_keys.is_empty(),
                        "the arriving run must sit outside the fold's input"
                    );
                    visible = visible_namespace(&store, &namespace_id).await;
                }
            }
            MetadataReorganizeOutcome::PartialFoldCompleted {
                ref families,
                output_segments,
                output_rows,
                ..
            } => {
                if families != group {
                    continue_after_step(&store, &namespace_id, &visible).await;
                    continue;
                }
                assert!(!completed, "the fold must finish exactly once");
                assert!(output_segments > 0);
                assert_eq!(
                    output_rows, written_rows,
                    "the finished run must hold what every slice wrote and nothing else"
                );
                completed = true;
                let referenced = manifest_segment_keys(&store, &namespace_id).await;
                assert!(
                    arrived_segment_keys.is_subset(&referenced),
                    "runs that arrived while the fold ran must survive the swap"
                );
            }
            MetadataReorganizeOutcome::UnitPublished { ref families, .. } => {
                if families == group {
                    assert!(
                        completed,
                        "this group folds no other way while its fold is in flight"
                    );
                    group_merged_after_the_fold += 1;
                } else if !completed {
                    other_groups_folded_during_the_fold += 1;
                }
            }
            MetadataReorganizeOutcome::NotNeeded { .. } => break,
            MetadataReorganizeOutcome::BudgetExhausted { .. } => {
                panic!("the group parked instead of folding a slice at a time")
            }
            MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
        }
        continue_after_step(&store, &namespace_id, &visible).await;
    }

    assert!(
        slices > 1,
        "the fold must take several slices, got {slices}"
    );
    assert!(completed, "the fold must finish");
    // Non-decreasing rather than strictly increasing: a fold working through
    // one partition in pieces republishes the same cursor with its offset
    // moved on, and the step outcome carries the cursor only.
    let mut ascending = cursors.clone();
    ascending.sort();
    assert_eq!(
        cursors, ascending,
        "no slice may leave the fold further back than it found it"
    );
    assert!(
        other_groups_folded_during_the_fold > 0,
        "the other groups must keep folding while one group folds in slices"
    );
    assert!(
        group_merged_after_the_fold > 0,
        "the runs that arrived during the fold must merge normally once it finishes"
    );

    // The base is folded: the group is left in the one run the fold built.
    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let after_payload = tables.manifest().payload.clone();
    drop(tables);
    assert!(after_payload.reorganize.is_none());
    let group_runs: BTreeSet<(ChangeSeq, u32)> = after_payload
        .metadata_files
        .iter()
        .filter(|descriptor| group.contains(&descriptor.family))
        .map(|descriptor| (descriptor.run_seq, descriptor.level))
        .collect();
    assert_eq!(
        group_runs.len(),
        1,
        "the group must end in one run, got {group_runs:?}"
    );

    // Retention reclaimed the churn: every spent unbind marker went, and so
    // did the binding each one retired.
    let rows_after = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let retired: BTreeSet<_> = rows_before[&ApiMetadataTableFamily::DirentryUnbinds]
        .iter()
        .filter_map(binding_generation)
        .collect();
    assert!(
        !retired.is_empty(),
        "the seed must delete files for this to mean anything"
    );
    assert!(
        rows_after[&ApiMetadataTableFamily::DirentryUnbinds].is_empty(),
        "every unbind marker sat at or below the floor and must have gone"
    );
    for family in [
        ApiMetadataTableFamily::DirentryBinds,
        ApiMetadataTableFamily::DirentryChildBinds,
    ] {
        for row in &rows_after[&family] {
            assert!(
                binding_generation(row).is_none_or(|generation| !retired.contains(&generation)),
                "a retired binding survived the fold in {family:?}"
            );
        }
    }

    // And the condition that started the fold no longer holds: the group
    // fits one step again, so nothing warns about it.
    write_seed_file(&store, &namespace_id, 11, 0).await;
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint one more delta run");
    assert!(
        !group_bottom_is_over_budget(&store, &namespace_id, group, policy).await,
        "the folded group fits one step again, so the warning must stop"
    );
}

/// One step's tally, and whether the namespace has gone quiet.
#[derive(Debug, Default, PartialEq, Eq)]
struct DriveTally {
    slices: usize,
    completions: usize,
    units: usize,
}

impl DriveTally {
    fn record(&mut self, outcome: &MetadataReorganizeOutcome) -> bool {
        match outcome {
            MetadataReorganizeOutcome::PartialFoldAdvanced { .. } => self.slices += 1,
            MetadataReorganizeOutcome::PartialFoldCompleted { .. } => self.completions += 1,
            MetadataReorganizeOutcome::UnitPublished { .. } => self.units += 1,
            MetadataReorganizeOutcome::NotNeeded { .. } => return true,
            other => panic!("unexpected reorganization outcome {other:?}"),
        }
        false
    }
}

async fn drive_reorganization_to_quiescence(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> DriveTally {
    let mut tally = DriveTally::default();
    for _step in 0..256 {
        let report = reorganize_metadata_step(store, namespace_id, context, policy)
            .await
            .expect("reorganization step");
        if tally.record(&report.outcome) {
            return tally;
        }
    }
    panic!("reorganization did not go quiet in a bounded number of steps")
}

/// The same arc with nothing carried between steps but the store.
///
/// A step already rebuilds everything it needs from durable state, so the
/// harsher version of a crash is to throw the store handle away as well and
/// build a new one over the same directory before every step. The fold must
/// land where an uninterrupted one lands.
#[tokio::test]
async fn a_fold_through_maintenance_steps_resumes_from_the_store_alone() {
    let straight_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(straight_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let seed_policy = seed_over_budget_bindings_group(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let policy =
        policy_that_cannot_fold_the_group_base(&store, &namespace_id, group, seed_policy).await;

    let resumed_dir = tempdir().expect("tempdir");
    copy_store_tree(straight_dir.path(), resumed_dir.path());

    let straight =
        drive_reorganization_to_quiescence(&store, &namespace_id, &context, policy).await;
    let mut resumed = DriveTally::default();
    let mut quiet = false;
    for _step in 0..256 {
        // Everything a step could have remembered dies here: the handle,
        // its caches, and the fold it was running.
        let rebuilt = LocalFsStore::new(resumed_dir.path()).expect("rebuild the store handle");
        let report = reorganize_metadata_step(&rebuilt, &namespace_id, &context, policy)
            .await
            .expect("reorganization step");
        if resumed.record(&report.outcome) {
            quiet = true;
            break;
        }
    }
    assert!(quiet, "the rebuilt run must go quiet too");

    assert!(straight.slices > 1);
    assert_eq!(
        straight, resumed,
        "a fold rebuilt at every step must take the same steps"
    );
    let resumed_store = LocalFsStore::new(resumed_dir.path()).expect("store");
    assert_eq!(
        group_rows_of_current_manifest(&store, &namespace_id, group).await,
        group_rows_of_current_manifest(&resumed_store, &namespace_id, group).await,
        "a fold rebuilt at every step must keep the same rows"
    );
    assert_eq!(
        visible_namespace(&store, &namespace_id).await,
        visible_namespace(&resumed_store, &namespace_id).await,
        "and must answer reads the same way"
    );
}

/// The whole arc for a group one of whose partitions is larger than a step,
/// driven through the entry point the maintenance runner calls.
///
/// The trigger fires on the group's oldest run as before, the fold reports
/// the pieces it takes so an operator can tell a step that ran every rule the
/// frozen floor defines from one that could only run the rules a row answers
/// on its own, and the group still ends up in one run with nothing a reader
/// can see having moved.
#[tokio::test]
async fn an_oversized_partition_folds_through_repeated_maintenance_steps() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    seed_one_wide_directory(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);

    // Under the wide directory's own partition and well under the group's
    // oldest run, so the group folds in slices and that one partition folds
    // in pieces. Above every other group's oldest run too: a rename writes
    // two binding rows and one receipt, so three quarters of the wide
    // partition leaves the receipts group foldable whole and no other group
    // starts a fold of its own.
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let wide_partition_rows = wide_directory_partition_rows(&before);
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(wide_partition_rows * 3 / 4)
            .expect("nonzero"),
        max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    };
    assert!(
        group_bottom_is_over_budget(&store, &namespace_id, group, policy).await,
        "the group's oldest run must not fit one step, or nothing folds in slices"
    );
    let visible = visible_namespace(&store, &namespace_id).await;

    let mut drops_reported = Vec::new();
    let mut completed = false;
    for _step in 0..256 {
        let report = reorganize_metadata_step(&store, &namespace_id, &context, policy)
            .await
            .expect("reorganization step");
        match report.outcome {
            MetadataReorganizeOutcome::PartialFoldAdvanced {
                ref families,
                drops,
                ..
            } => {
                assert_eq!(families, group, "only this group folds in slices here");
                drops_reported.push(drops);
            }
            MetadataReorganizeOutcome::PartialFoldCompleted { ref families, .. } => {
                assert_eq!(families, group);
                completed = true;
            }
            MetadataReorganizeOutcome::UnitPublished { .. } => {}
            MetadataReorganizeOutcome::NotNeeded { .. } => break,
            other => panic!("unexpected reorganization outcome {other:?}"),
        }
        assert_eq!(
            visible_namespace(&store, &namespace_id).await,
            visible,
            "a step changed what a read answers"
        );
    }

    assert!(completed, "the fold must finish");
    assert!(
        drops_reported.contains(&MetadataFoldSliceDrops::PartitionPiece),
        "a step folding a piece of a partition must say so, got {drops_reported:?}"
    );
    assert!(
        drops_reported.contains(&MetadataFoldSliceDrops::Applied),
        "the partitions that do fit must still have every rule run over them"
    );
    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let group_runs: BTreeSet<(ChangeSeq, u32)> = tables
        .manifest()
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| group.contains(&descriptor.family))
        .map(|descriptor| (descriptor.run_seq, descriptor.level))
        .collect();
    assert_eq!(
        group_runs.len(),
        1,
        "the group must end in one run, got {group_runs:?}"
    );
}

/// One partition several times larger than a step's row budget still folds,
/// in bounded pieces, and a resume at any of those piece boundaries lands
/// where an uninterrupted fold lands.
///
/// This is the case a fold that advanced only in whole partitions had no
/// answer for: a directory has no size limit, so accepting the first
/// partition whatever it cost meant unbounded step memory and a fold that
/// retried the same partition forever.
#[tokio::test]
async fn one_partition_larger_than_a_step_folds_in_bounded_pieces() {
    let walk_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(walk_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    seed_one_wide_directory(&store, &namespace_id).await;

    let fold_dir = tempdir().expect("tempdir");
    copy_store_tree(walk_dir.path(), fold_dir.path());
    let fold_store = LocalFsStore::new(fold_dir.path()).expect("store");
    super::super::reorganize_metadata_step(
        &fold_store,
        &namespace_id,
        &test_context(),
        fold_everything_policy(),
    )
    .await
    .expect("fold the group whole");
    let folded = group_rows_of_current_manifest(&fold_store, &namespace_id, group).await;
    let folded_visible = visible_namespace(&fold_store, &namespace_id).await;

    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    let wide_partition_rows = wide_directory_partition_rows(&before);
    // A row budget several times under one partition, so that partition
    // cannot be folded whole however the walk is sliced.
    let row_budget = wide_partition_rows / 4;
    assert!(
        row_budget > 1,
        "the seed must put many rows under one parent, got {wide_partition_rows}"
    );
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(row_budget).expect("nonzero"),
        max_rows_per_segment: NonZeroUsize::new(8).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    };

    let run = run_partial_fold(
        &store,
        &namespace_id,
        group,
        policy,
        WalkDriver::ResumeEveryStep,
    )
    .await;
    let pieces = run
        .slices
        .iter()
        .filter(|slice| slice.drops == MetadataFoldSliceDrops::PartitionPiece)
        .count();
    assert!(
        pieces > 1,
        "the oversized partition must take several pieces, got {pieces} of {} slices",
        run.slices.len()
    );
    // Every step stays inside the budget it was given, plus the one block a
    // piece always takes however much it costs. Blocks here hold a handful
    // of rows, so the slack is a handful too.
    let block_slack = 32u64;
    for (index, slice) in run.slices.iter().enumerate() {
        assert!(
            slice.decoded_input_rows <= row_budget as u64 + block_slack,
            "step {index} decoded {} rows against a budget of {row_budget}",
            slice.decoded_input_rows
        );
    }
    // Nothing a reader can see moved at any point along the way.
    for (index, visible) in run.visible_rows_per_step.iter().enumerate() {
        assert_eq!(*visible, before, "step {index} changed what a scan returns");
    }

    let walked = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    assert_eq!(
        walked, folded,
        "a fold that had to cut one partition into pieces must reach the fold's rows"
    );
    assert_eq!(
        visible_namespace(&store, &namespace_id).await,
        folded_visible,
        "and must answer reads the way a whole-group fold answers them"
    );
}

/// How many rows the wide directory's own partition holds: the bindings and
/// the unbinds under the parent that carries the most of them. The reverse
/// index is keyed by child, so its rows sit in other partitions.
fn wide_directory_partition_rows(
    rows: &BTreeMap<ApiMetadataTableFamily, Vec<MetadataRow>>,
) -> usize {
    let mut per_parent = BTreeMap::<InodeId, usize>::new();
    for family in [
        ApiMetadataTableFamily::DirentryBinds,
        ApiMetadataTableFamily::DirentryUnbinds,
    ] {
        for row in &rows[&family] {
            match row {
                MetadataRow::DirentryBind {
                    parent_inode_id, ..
                }
                | MetadataRow::DirentryUnbind {
                    parent_inode_id, ..
                } => *per_parent.entry(*parent_inode_id).or_default() += 1,
                _ => {}
            }
        }
    }
    per_parent
        .into_values()
        .max()
        .expect("the namespace holds bindings")
}

/// A namespace whose bindings group is dominated by one directory, so one
/// partition of that group holds most of the group's rows.
///
/// Renames are what make it lopsided. Every rename writes a bind and an
/// unbind under the same parent, and no inode row and no revision row, so the
/// parent's partition grows while the rest of the namespace stays where it
/// was. That is the shape a fold advancing only in whole partitions has no
/// answer for.
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
    // Small base segments so a plan, which prices whole data blocks, has
    // somewhere to stop inside the wide directory.
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

/// A run whose secondary index does not hold the rows its canonical family
/// holds must not be swapped in.
///
/// No slice ever holds a bind row and the reverse row that indexes it, so
/// there is nothing to compare directly; the two digests the fold keeps are
/// what stands in. Folding one corrupted reverse row into the index digest is
/// exactly what a step that wrote the index a row the canonical family does
/// not hold would leave behind.
#[tokio::test]
async fn a_walk_refuses_to_swap_in_a_run_whose_reverse_index_disagrees() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let timer = StdMonotonicTimer::default();
    seed_bindings_workload(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let policy = small_slice_policy();
    let frozen_floor_seq = read_floor_seq(&store, &namespace_id).await;

    let mut tables = load_current_manifest_tables(&store, &namespace_id).await;
    let snapshot = snapshot_runs_for_group(tables.manifest(), group);
    let mut walk = MetadataFoldWalk::start(&tables, group, snapshot, frozen_floor_seq)
        .expect("start a partial fold");

    // One reverse row written with the wrong child inode: the digest sees a
    // row the canonical family never held in place of one it did.
    let honest = MetadataRow::DirentryBind {
        parent_inode_id: InodeId(7),
        name_key: NameKey::parse("report.txt").expect("name key"),
        display_name: DisplayName::parse("report.txt").expect("display name"),
        child_inode_id: InodeId(42),
        bind_seq: ChangeSeq(11),
        bind_delta_index: 0,
    };
    let MetadataRow::DirentryBind { .. } = &honest else {
        unreachable!()
    };
    let corrupted = match honest.clone() {
        MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            display_name,
            bind_seq,
            bind_delta_index,
            ..
        } => MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id: InodeId(43),
            bind_seq,
            bind_delta_index,
        },
        other => other,
    };

    let mut error = None;
    for _step in 0..64 {
        // Applied to the state the walk will publish next, so the corruption
        // survives every remaining step exactly as a bad write would.
        let progress = walk.progress_mut();
        let digest = partial_fold::parse_row_digest(&progress.index_rows_digest)
            .expect("the walk spells its own digests")
            .wrapping_sub(partial_fold::row_digest(&honest).expect("digest"))
            .wrapping_add(partial_fold::row_digest(&corrupted).expect("digest"));
        progress.index_rows_digest = partial_fold::spell_row_digest(digest);

        match walk
            .advance(&store, &namespace_id, &tables, policy, &context, &timer)
            .await
        {
            Ok(MetadataFoldWalkOutcome::SlicePublished(_)) => {
                tables = load_current_manifest_tables(&store, &namespace_id).await;
            }
            Ok(other) => panic!("the swap must be refused, got {other:?}"),
            Err(failure) => {
                error = Some(failure);
                break;
            }
        }
    }
    let error = error.expect("the completing step must refuse the swap");
    let CoreError::NamespaceCorrupt(message) = &error else {
        panic!("expected a corruption error, got {error:?}")
    };
    assert!(
        message.contains("DirentryBinds"),
        "the refusal must name the group, got `{message}`"
    );
    assert!(
        load_current_manifest_tables(&store, &namespace_id)
            .await
            .manifest()
            .payload
            .reorganize
            .is_some(),
        "a refused swap leaves the fold in flight rather than half applied"
    );
}

// -------------------------------------------------------------------------
// The fragmented base
// -------------------------------------------------------------------------

/// A namespace with one folded base run and one delta run above it, which is
/// the smallest shape a second base-tier run can be added to.
async fn seed_folded_base_with_a_delta_run(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_seed_file(store, namespace_id, 0, 0).await;
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint the seed");
    drain_reorganization(store, namespace_id, &context, MetadataLsmPolicy::default()).await;
    write_seed_file(store, namespace_id, 0, 1).await;
    create_checkpoint(store, namespace_id, &context)
        .await
        .expect("checkpoint a delta run above the base");
}

/// One base-tier segment of `family`, for a test that builds a second run out
/// of it.
fn base_segment_of_family(
    manifest: &NamespaceManifestEnvelope,
    family: ApiMetadataTableFamily,
) -> MetadataFileRef {
    manifest
        .payload
        .metadata_files
        .iter()
        .find(|descriptor| {
            descriptor.family == family && descriptor.level == CHECKPOINT_BASE_RUN_LEVEL
        })
        .expect("a folded base segment of this family")
        .clone()
}

/// A group with two base-tier runs does not load.
///
/// This is the shape a merge above the base used to write: its output went to
/// the base tier stamped at the manifest head, so it became a second base run
/// for the group rather than a bigger delta run. Every fragment stayed under
/// the per-step budget on its own, so nothing ever reported the group's oldest
/// run as over budget, the fold in slices never started, and the group's rows
/// below the retention floor could never be dropped — dropping needs a fold
/// whose window starts at the group's oldest run.
///
/// Refusing the shape at load is what says it cannot come back: no builder
/// writes it now, and a manifest carrying it is corruption rather than a
/// state to carry on from.
#[tokio::test]
async fn a_manifest_whose_group_base_fragmented_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let manifest = tables.manifest().clone();
    drop(tables);
    let group = group_containing(ApiMetadataTableFamily::Inodes);
    assert_eq!(
        group_base_runs(&manifest, group).len(),
        1,
        "the seed must leave the group in one base run"
    );

    let mut fragmented = manifest.payload.clone();
    fragmented.metadata_files.push(reorganize_output_segment(
        &base_segment_of_family(&manifest, ApiMetadataTableFamily::Inodes),
        &namespace_id,
        manifest.payload.head_seq,
        CHECKPOINT_BASE_RUN_LEVEL,
    ));

    let error = load_perturbed_manifest(&store, &namespace_id, fragmented, 1)
        .await
        .expect_err("a group with two base runs must not load");
    let ManifestLoadError::RunManifestMismatch { message, .. } = &error else {
        panic!("expected a run mismatch, got {error:?}")
    };
    assert!(
        message.contains("Inodes") && message.contains("base"),
        "the rejection must name the group and what it holds too many of, got `{message}`"
    );
}

/// Two segments of one family carrying the same index inside one run do not
/// load.
///
/// A family in a run has one producer, and every producer numbers from zero,
/// so the numbers are a dense sequence. Two sets of them at one identity is
/// what a merge above the base used to leave behind when the group's base
/// already sat at the identity the merge stamped: both sets started at zero.
/// The hardening pass could only check this for a fold's own outputs, because
/// the file set genuinely held repeats; with the level rule it holds for every
/// run, and the check moved there.
#[tokio::test]
async fn a_manifest_that_numbers_one_family_twice_in_one_run_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let manifest = tables.manifest().clone();
    drop(tables);
    let existing = base_segment_of_family(&manifest, ApiMetadataTableFamily::Inodes);
    assert_eq!(existing.segment_index, 0);

    let mut repeated = manifest.payload.clone();
    repeated.metadata_files.push(reorganize_output_segment(
        &existing,
        &namespace_id,
        existing.run_seq,
        existing.level,
    ));

    let error = load_perturbed_manifest(&store, &namespace_id, repeated, 2)
        .await
        .expect_err("two segments of one family at one index must not load");
    let ManifestLoadError::SegmentDescriptorMismatch { message, .. } = &error else {
        panic!("expected a segment descriptor mismatch, got {error:?}")
    };
    assert!(
        message.contains("Inodes") && message.contains("numbered from zero"),
        "the rejection must name the family and the numbering rule, got `{message}`"
    );
}

/// Two segments of one family whose key ranges touch inside one run do not
/// load.
///
/// One producer writes a family's segments in ascending key order, so
/// segment one starts strictly above where segment zero ended. A descriptor
/// can keep the numbering dense and still not belong — stamped with another
/// run's identity, say — and then its key range is what disagrees with its
/// neighbours'. Here the second segment repeats the first one's whole range,
/// which is the shape a duplicated descriptor takes.
#[tokio::test]
async fn a_manifest_whose_run_segments_overlap_in_key_range_does_not_load() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_folded_base_with_a_delta_run(&store, &namespace_id).await;

    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    let manifest = tables.manifest().clone();
    drop(tables);
    let existing = base_segment_of_family(&manifest, ApiMetadataTableFamily::Inodes);
    assert_eq!(existing.segment_index, 0);

    let mut overlapping = manifest.payload.clone();
    let mut second = reorganize_output_segment(
        &existing,
        &namespace_id,
        existing.run_seq,
        existing.level,
    );
    // Index one keeps the numbering dense, so only the key order can object.
    second.segment_index = 1;
    overlapping.metadata_files.push(second);

    let error = load_perturbed_manifest(&store, &namespace_id, overlapping, 3)
        .await
        .expect_err("overlapping segment ranges within one run must not load");
    let ManifestLoadError::SegmentDescriptorMismatch { message, .. } = &error else {
        panic!("expected a segment descriptor mismatch, got {error:?}")
    };
    assert!(
        message.contains("Inodes") && message.contains("ascending key order"),
        "the rejection must name the family and the ordering rule, got `{message}`"
    );
}

/// The end state the soak never reached.
///
/// Four cycles of writes, deletions, and a retention floor that moves past
/// them, folded under budgets too small to take a group whole. The old level
/// rule turned every merge above the base into another base-tier run, so the
/// bases fragmented and the fold in slices never started: a live soak of that
/// code ended with six base-tier runs — direntry binds split across three of
/// them, revisions across four — with no fold in flight and every unbind
/// below the floor still there.
///
/// What is pinned here is what that run should have reached: one base run per
/// group throughout, a fold in slices that starts once a group stops fitting
/// one step, and the churn below the floor gone.
#[tokio::test]
async fn repeated_churn_under_small_budgets_leaves_one_base_run_per_group() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    // Small segments so a fold in slices has places to stop, and a row budget
    // the groups outgrow within a couple of cycles.
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(24).expect("nonzero"),
        max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    };

    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let mut folds_in_slices = 0usize;
    for cycle in 0..4u64 {
        for file in 0..4u64 {
            write_seed_file(&store, &namespace_id, cycle, file).await;
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the writes");
        for file in 0..3u64 {
            delete_path(
                &store,
                &namespace_id,
                &format!("/d{cycle}/f{file}.txt"),
                &context,
                None,
            )
            .await
            .expect("delete a file");
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the deletions");
        advance_retention_floor(&store, &namespace_id, &context)
            .await
            .expect("advance the floor past the deletions");
        let visible = visible_namespace(&store, &namespace_id).await;

        let mut quiet = false;
        for _step in 0..512 {
            let report = reorganize_metadata_step(&store, &namespace_id, &context, policy)
                .await
                .expect("reorganization step");
            let tables = load_current_manifest_tables(&store, &namespace_id).await;
            for (folded, base_runs) in base_runs_per_family_group(tables.manifest()) {
                assert!(
                    base_runs.len() <= 1,
                    "cycle {cycle}: {folded:?} holds base runs {base_runs:?}"
                );
            }
            drop(tables);
            continue_after_step(&store, &namespace_id, &visible).await;
            match report.outcome {
                MetadataReorganizeOutcome::PartialFoldCompleted { .. } => folds_in_slices += 1,
                MetadataReorganizeOutcome::NotNeeded { .. } => {
                    quiet = true;
                    break;
                }
                MetadataReorganizeOutcome::BudgetExhausted { ref families, .. } => {
                    panic!("cycle {cycle}: {families:?} parked instead of folding")
                }
                MetadataReorganizeOutcome::Superseded => {
                    panic!("no concurrent publisher exists in this test")
                }
                MetadataReorganizeOutcome::PartialFoldAdvanced { .. }
                | MetadataReorganizeOutcome::UnitPublished { .. } => {}
            }
        }
        assert!(quiet, "cycle {cycle}: reorganization did not go quiet");
    }

    assert!(
        folds_in_slices > 0,
        "a group must have outgrown one step and been folded in slices"
    );
    // The bindings group ends in one run, and the churn the floor covers is
    // gone from it: every deletion below the floor left an unbind, and an
    // unbind is only ever dropped by a fold that starts at the group's oldest
    // run.
    let tables = load_current_manifest_tables(&store, &namespace_id).await;
    assert_eq!(group_base_runs(tables.manifest(), group).len(), 1);
    drop(tables);
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    let rows = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    assert!(
        rows[&ApiMetadataTableFamily::DirentryUnbinds]
            .iter()
            .all(|row| !matches!(row, MetadataRow::DirentryUnbind { unbind_seq, .. } if *unbind_seq <= floor_seq)),
        "the unbind markers the floor covers must have been dropped"
    );
    assert_eq!(
        rows[&ApiMetadataTableFamily::DirentryBinds].len(),
        rows[&ApiMetadataTableFamily::DirentryChildBinds].len(),
        "the two bind families must drop in lockstep"
    );
}
