//! Folding a family group one slice at a time: the partition grammar, the
//! walk executor, and the oracle that says a walk and a whole-group fold
//! reach the same place.

use super::super::partial_fold::{
    MetadataFoldSliceDrops, MetadataFoldSliceReport, MetadataFoldWalk, MetadataFoldWalkOutcome,
};
use super::super::partition::{GroupPartitioning, PartitionCursor, PartitionKey};
use super::super::row::manifest_row_commit_seq;
use super::super::scan::VerifiedMetadataTables;
use super::*;
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
    // under its delta runs the way a real over-budget group does.
    drain_reorganization(store, namespace_id, &context, MetadataLsmPolicy::default()).await;
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
    let mut walk = MetadataFoldWalk::start(&tables, group, snapshot, frozen_floor_seq, policy)
        .await
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
                    walk = MetadataFoldWalk::resume_from_manifest(&tables, policy)
                        .await
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
/// room for the unbind set.
fn small_slice_policy() -> MetadataLsmPolicy {
    MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(64).expect("nonzero"),
        max_decoded_input_bytes_per_step: NonZeroUsize::new(4_096).expect("nonzero"),
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
        "this budget must leave room for the unbind set"
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

/// The unbind set has a bound, and a walk over it keeps going as a pure
/// rewrite rather than stopping. That is also the check that the equivalence
/// oracle has teeth: with the drops off, the walk must NOT reach the
/// whole-group fold's rows.
#[tokio::test]
async fn a_walk_that_cannot_hold_the_unbind_set_rewrites_without_dropping() {
    let walk_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(walk_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    seed_bindings_workload(&store, &namespace_id).await;
    let group = group_containing(ApiMetadataTableFamily::DirentryUnbinds);
    let before = group_rows_of_current_manifest(&store, &namespace_id, group).await;

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

    // A byte budget too small for even one unbind entry.
    let no_drop_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(64).expect("nonzero"),
        max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    let run = run_partial_fold(
        &store,
        &namespace_id,
        group,
        no_drop_policy,
        WalkDriver::KeepExecutor,
    )
    .await;
    assert!(
        run.slices
            .iter()
            .all(|slice| slice.drops == MetadataFoldSliceDrops::UnbindSetOverBound),
        "an unbind set over its bound must stop the whole walk dropping"
    );

    let walked = group_rows_of_current_manifest(&store, &namespace_id, group).await;
    assert_eq!(
        walked, before,
        "a no-drop walk is a pure rewrite: every input row must survive"
    );
    assert_ne!(
        walked, folded,
        "the oracle has no teeth unless a walk without the drop rules misses the fold's rows"
    );
    // A pure rewrite still does the thing a frozen base could not: it
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
        run.slices
            .iter()
            .all(|slice| slice.drops == MetadataFoldSliceDrops::Applied),
        "the revisions group has no reverse bind index and no unbind set"
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
    let mut walk = MetadataFoldWalk::start(&tables, group, snapshot, frozen_floor_seq, policy)
        .await
        .expect("start a partial fold");
    assert_eq!(walk.progress().cursor, "direntry-00000000000000000000");

    let mut cursors = Vec::new();
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
        cursors.push(progress.cursor.clone());

        // The resumed walk must agree with the published state exactly.
        let resumed = MetadataFoldWalk::resume_from_manifest(&tables, policy)
            .await
            .expect("resume")
            .expect("the manifest carries a fold");
        assert_eq!(*resumed.progress(), progress);
        assert_eq!(resumed.group(), group);
    }

    assert!(cursors.len() > 2, "the walk must publish several cursors");
    let mut ascending = cursors.clone();
    ascending.sort();
    ascending.dedup();
    assert_eq!(
        cursors, ascending,
        "the cursor must advance strictly, so no partition is folded twice"
    );
    assert!(
        MetadataFoldWalk::resume_from_manifest(&tables, policy)
            .await
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
