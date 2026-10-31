//! Applies retention rules while rows are streamed in key order.

use super::merge_iterator::LocalityGrouping;
use crate::error::{CoreError, Result};
use crate::metadata::DirentryBindingRecord;
use loonfs_types::format::manifest::{ActiveDeletionRowAction, MetadataRow, MetadataRowFamily};
use loonfs_types::MetadataFamilyGroup;
use loonfs_types::{ChangeSeq, InodeId, NameKey};

/// One row a retention operator kept, and the family it belongs to.
pub(super) type KeptRow = (MetadataRowFamily, MetadataRow);

/// Retention rule assigned to a row cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetentionRule {
    /// Retain every row in the group.
    KeepEveryRow,
    /// Keeps the first row in each group; layout keys sort the longest layout first,
    /// then the newest.
    FirstPerGroup,
    /// A commit row or its receipt is judged by its own commit sequence against the floor.
    CommitHistory,
    /// Every revision above the floor, plus the newest at or below it, per
    /// inode.
    WholeState,
    /// Remove cancelled deletion pairs and the listed rows of expired
    /// deletions.
    ActiveDeletions,
    /// Per edge and per kind of row, retains the values above the floor and
    /// the newest value at or below it, unless the edge is unbound at the
    /// floor.
    Bindings,
}

impl RetentionRule {
    /// The operator that applies the rule in a merge whose basis manifest
    /// has `acknowledged_floor_seq` as its acknowledged floor.
    pub(crate) fn operator(self, acknowledged_floor_seq: ChangeSeq) -> RetentionOperator {
        match self {
            Self::KeepEveryRow => RetentionOperator::KeepEveryRow,
            Self::FirstPerGroup => RetentionOperator::FirstPerGroup(false),
            Self::CommitHistory => RetentionOperator::CommitHistory,
            Self::WholeState => RetentionOperator::WholeState(WholeStateRetention::default()),
            Self::ActiveDeletions => RetentionOperator::ActiveDeletions(ActiveDeletionRetention {
                acknowledged_floor_seq,
                revoked: false,
            }),
            Self::Bindings => RetentionOperator::Bindings(BindingRetention::default()),
        }
    }
}

/// The state one cluster's rule keeps while a merge streams through it.
#[derive(Debug)]
pub(crate) enum RetentionOperator {
    KeepEveryRow,
    FirstPerGroup(bool),
    CommitHistory,
    WholeState(WholeStateRetention),
    ActiveDeletions(ActiveDeletionRetention),
    Bindings(BindingRetention),
}

impl RetentionOperator {
    /// Returns the retained row, or `None` when the row is dropped.
    pub(crate) fn push(
        &mut self,
        family: MetadataRowFamily,
        row: MetadataRow,
        floor_seq: ChangeSeq,
    ) -> Result<Option<KeptRow>> {
        let kept = match self {
            Self::KeepEveryRow => Some(row),
            Self::FirstPerGroup(kept) => (!std::mem::replace(kept, true)).then_some(row),
            Self::CommitHistory => keep_commit_history_row(row, floor_seq),
            Self::WholeState(state) => state.push(family, row, floor_seq)?,
            Self::ActiveDeletions(state) => state.push(row),
            Self::Bindings(state) => state.push(family, row, floor_seq)?,
        };
        Ok(kept.map(|row| (family, row)))
    }

    /// Finishes the current key group. Every rule decides each row as it
    /// arrives, so no row waits for the end of its group.
    pub(crate) fn close_group(&mut self) {
        match self {
            Self::KeepEveryRow | Self::CommitHistory => {}
            Self::FirstPerGroup(kept) => *kept = false,
            Self::WholeState(state) => state.close_group(),
            Self::ActiveDeletions(state) => state.close_group(),
            Self::Bindings(state) => state.close_group(),
        }
    }
}

fn keep_commit_history_row(row: MetadataRow, floor_seq: ChangeSeq) -> Option<MetadataRow> {
    match &row {
        MetadataRow::CommitReceipt(crate::metadata::CommitReceiptRecord {
            committed_seq, ..
        }) if *committed_seq < floor_seq => None,
        MetadataRow::Commit(record) if record.committed_seq < floor_seq => None,
        _ => Some(row),
    }
}

/// Retains every file, attribute, and access revision above the floor and
/// the newest revision at or below the floor for each inode.
///
/// Whole-state row keys sort each inode's revisions newest first. The first row
/// at or below the floor is the inode's state at the floor, including a cleared
/// state, and the floor keeps no older history. The operator keeps a
/// recoverably deleted inode's rows like any other inode's, so an undelete
/// restores the prior state, and a base merge removes the rows of an inode
/// that an expired deletion hides before they reach the operator. The
/// operator processes this order without retaining the complete history.
#[derive(Debug, Default)]
pub(crate) struct WholeStateRetention {
    /// The revision of the previous row at or below the floor. The inode has
    /// kept its newest row at or below the floor once this is set. It also
    /// catches two rows sharing one revision number: descending revision
    /// order puts any such pair next to each other, so one field sees them.
    previous_at_floor: Option<u64>,
}

impl WholeStateRetention {
    fn push(
        &mut self,
        family: MetadataRowFamily,
        row: MetadataRow,
        floor_seq: ChangeSeq,
    ) -> Result<Option<MetadataRow>> {
        let Some((inode_id, revision, committed_seq)) = whole_state_revision(&row) else {
            return Ok(Some(row));
        };
        if committed_seq > floor_seq {
            return Ok(Some(row));
        }
        // Writer invariant: one inode's whole-state rows are numbered
        // without gaps or repeats, so "the newest at or below the floor"
        // names exactly one row. Two rows sharing a number would make the
        // choice arbitrary and the drop unsafe; refuse to compact state that
        // violates it.
        if self.previous_at_floor == Some(revision) {
            let family = family.as_str();
            return Err(CoreError::NamespaceCorrupt(format!(
                "inode `{inode_id}` has two {family} rows at revision \
                 `{revision}` at or below the retention floor; refusing to drop rows"
            )));
        }
        if self.previous_at_floor.replace(revision).is_some() {
            return Ok(None);
        }
        Ok(Some(row))
    }

    fn close_group(&mut self) {
        self.previous_at_floor = None;
    }
}

fn whole_state_revision(row: &MetadataRow) -> Option<(InodeId, u64, ChangeSeq)> {
    match row {
        MetadataRow::FileRevision(record) => {
            Some((record.inode_id, record.revision_no.0, record.committed_seq))
        }
        MetadataRow::AttributesRevision(record) => Some((
            record.inode_id,
            record.attributes_revision_no.0,
            record.committed_seq,
        )),
        MetadataRow::AccessRevision(record) => Some((
            record.inode_id,
            record.access_revision_no.0,
            record.committed_seq,
        )),
        _ => None,
    }
}

/// Processes active-deletion rows one deletion identity at a time.
///
/// The operator removes a listed row paired with its removal marker, and the
/// listed row of a deletion at or below the acknowledged floor, which has
/// expired. A listed row above the acknowledged floor stays, even at or
/// below the retention floor, because an undelete validated against an
/// older floor can still revoke it
/// ([format section 10.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#103-row-retention-during-a-base-compaction)).
/// Row-key order places the marker first, allowing one flag to determine
/// whether the listed row is retained.
#[derive(Debug)]
pub(crate) struct ActiveDeletionRetention {
    /// The basis manifest's acknowledged floor.
    acknowledged_floor_seq: ChangeSeq,
    /// Whether this deletion's removal marker has already arrived.
    revoked: bool,
}

impl ActiveDeletionRetention {
    fn push(&mut self, row: MetadataRow) -> Option<MetadataRow> {
        let MetadataRow::ActiveDeletion(crate::metadata::ActiveDeletionRecord {
            action,
            deletion_seq,
            ..
        }) = &row
        else {
            return Some(row);
        };
        match action {
            // Every removal marker goes; what this code determines is whether
            // the listed row goes with it.
            ActiveDeletionRowAction::Removed { .. } => {
                self.revoked = true;
                None
            }
            ActiveDeletionRowAction::Listed { .. }
                if self.revoked || *deletion_seq <= self.acknowledged_floor_seq =>
            {
                None
            }
            ActiveDeletionRowAction::Listed { .. } => Some(row),
        }
    }

    fn close_group(&mut self) {
        self.revoked = false;
    }
}

/// Applies the binding rule to one binding index, whose keys sort each
/// edge's rows newest first.
///
/// An edge's rows in the slot index are of two kinds: binding events, which
/// both indexes store, and the summary rows of a file's edge or the subtree
/// rows of a directory's edge, which the slot index alone stores.
/// For each edge and each kind, the operator keeps every row above the floor
/// and the kind's first row at or below it, which is the newest at or below
/// the floor, and drops the kind's later rows. When the edge's first binding
/// event at or below the floor is unbound, the edge was unbound at the
/// floor, so the operator drops the edge's rows of both kinds at or below
/// it. The child index stores binding events alone, so the rule reads the
/// same there with one kind. Both indexes keep or drop each binding event
/// the same way
/// ([format section 10.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#103-row-retention-during-a-base-compaction)).
///
/// A summary or subtree row exists only while its edge is bound, so such a
/// row that arrives before the edge's first binding event at or below the
/// floor belongs to a binding that is still in place at the floor.
#[derive(Debug, Default)]
pub(crate) struct BindingRetention {
    /// The state of the edge at the floor, once the edge's first binding
    /// event at or below the floor has arrived.
    floor_event: Option<FloorEvent>,
    /// Whether the edge's first summary or subtree row at or below the floor
    /// has arrived.
    floor_summary_seen: bool,
    /// The parent, name, and child of the last edge bound at the floor in
    /// this index. The slot index keeps one slot's edges adjacent and the
    /// child index keeps one child's edges adjacent, so a second edge bound
    /// in the same slot, or for the same child, is always compared with this
    /// one.
    last_bound_edge: Option<(InodeId, NameKey, InodeId)>,
}

/// An edge's binding event at the floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FloorEvent {
    Bound,
    Unbound,
}

impl BindingRetention {
    fn push(
        &mut self,
        family: MetadataRowFamily,
        row: MetadataRow,
        floor_seq: ChangeSeq,
    ) -> Result<Option<MetadataRow>> {
        let MetadataRow::DirentryBinding(binding) = &row else {
            return Ok(Some(row));
        };
        if binding.committed_seq > floor_seq {
            return Ok(Some(row));
        }
        if !binding.is_binding_event() {
            let keep = self.floor_event != Some(FloorEvent::Unbound) && !self.floor_summary_seen;
            self.floor_summary_seen = true;
            return Ok(keep.then_some(row));
        }
        if self.floor_event.is_some() {
            return Ok(None);
        }
        if !binding.is_bound() {
            self.floor_event = Some(FloorEvent::Unbound);
            return Ok(None);
        }
        self.floor_event = Some(FloorEvent::Bound);
        self.refuse_a_second_bound_edge(family, binding)?;
        Ok(Some(row))
    }

    /// Refuses a second edge bound at the floor in the slot of the last
    /// bound edge, in the slot index, or for its child, in the child index.
    /// Commit validation allows one child per slot and one parent per child.
    fn refuse_a_second_bound_edge(
        &mut self,
        family: MetadataRowFamily,
        binding: &DirentryBindingRecord,
    ) -> Result<()> {
        if let Some((parent_inode_id, name_key, child_inode_id)) = &self.last_bound_edge {
            let same_slot =
                *parent_inode_id == binding.parent_inode_id && *name_key == binding.name_key;
            let same_child = *child_inode_id == binding.child_inode_id;
            if family == MetadataRowFamily::DirentryBinds && same_slot {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "parent `{parent_inode_id}` binds name `{name_key}` to children \
                     `{child_inode_id}` and `{}` at or below the retention floor; refusing to \
                     drop rows",
                    binding.child_inode_id
                )));
            }
            if family == MetadataRowFamily::DirentryChildBinds && same_child {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "child `{child_inode_id}` is bound as `{name_key}` under parent \
                     `{parent_inode_id}` and as `{}` under parent `{}` at or below the retention \
                     floor; refusing to drop rows",
                    binding.name_key, binding.parent_inode_id
                )));
            }
        }
        self.last_bound_edge = Some((
            binding.parent_inode_id,
            binding.name_key.clone(),
            binding.child_inode_id,
        ));
        Ok(())
    }

    fn close_group(&mut self) {
        self.floor_event = None;
        self.floor_summary_seen = false;
    }
}

/// One set of families the engine merges and judges together, and how it
/// groups their rows while doing it.
pub(crate) struct RetentionCluster {
    pub(crate) families: &'static [MetadataRowFamily],
    pub(crate) locality: LocalityGrouping,
    pub(crate) rule: RetentionRule,
}

// Both indexes put an edge's parent, name, and child ahead of its position, so
// positions run newest first within one edge.
const BINDINGS_CLUSTERS: [RetentionCluster; 2] = [
    RetentionCluster {
        families: &[MetadataRowFamily::DirentryBinds],
        locality: LocalityGrouping::LeadingKeyComponents(3),
        rule: RetentionRule::Bindings,
    },
    RetentionCluster {
        families: &[MetadataRowFamily::DirentryChildBinds],
        locality: LocalityGrouping::LeadingKeyComponents(3),
        rule: RetentionRule::Bindings,
    },
];

/// A family no rule ever drops a row from, rewritten in key order.
const fn row_cluster(families: &'static [MetadataRowFamily]) -> RetentionCluster {
    RetentionCluster {
        families,
        locality: LocalityGrouping::Row,
        rule: RetentionRule::KeepEveryRow,
    }
}

const LAYOUT_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::ContentLayouts],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::FirstPerGroup,
}];
const INODE_CLUSTERS: [RetentionCluster; 1] = [row_cluster(&[MetadataRowFamily::Inodes])];
const TOMBSTONE_CLUSTERS: [RetentionCluster; 1] = [row_cluster(&[MetadataRowFamily::Tombstones])];
const COMMIT_CLUSTERS: [RetentionCluster; 2] = [
    RetentionCluster {
        families: &[MetadataRowFamily::Commits],
        locality: LocalityGrouping::Row,
        rule: RetentionRule::CommitHistory,
    },
    RetentionCluster {
        families: &[MetadataRowFamily::CommitReceipts],
        locality: LocalityGrouping::Row,
        rule: RetentionRule::CommitHistory,
    },
];
const ACTIVE_DELETION_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::ActiveDeletions],
    locality: LocalityGrouping::LeadingKeyComponents(2),
    rule: RetentionRule::ActiveDeletions,
}];
const REVISION_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::Revisions],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::WholeState,
}];
const ATTRIBUTE_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::Attributes],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::WholeState,
}];
const ACCESS_CLUSTERS: [RetentionCluster; 1] = [RetentionCluster {
    families: &[MetadataRowFamily::Access],
    locality: LocalityGrouping::LeadingKeyComponents(1),
    rule: RetentionRule::WholeState,
}];

pub(crate) fn retention_clusters(group: MetadataFamilyGroup) -> &'static [RetentionCluster] {
    match group {
        MetadataFamilyGroup::Bindings => &BINDINGS_CLUSTERS,
        MetadataFamilyGroup::Revisions => &REVISION_CLUSTERS,
        MetadataFamilyGroup::Inodes => &INODE_CLUSTERS,
        MetadataFamilyGroup::Tombstones => &TOMBSTONE_CLUSTERS,
        MetadataFamilyGroup::ActiveDeletions => &ACTIVE_DELETION_CLUSTERS,
        MetadataFamilyGroup::Commits => &COMMIT_CLUSTERS,
        MetadataFamilyGroup::ContentLayouts => &LAYOUT_CLUSTERS,
        MetadataFamilyGroup::Attributes => &ATTRIBUTE_CLUSTERS,
        MetadataFamilyGroup::Access => &ACCESS_CLUSTERS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_types::ROOT_INODE_ID;
    use loonfs_types::{
        AccessRevisionNo, AttributesRevisionNo, DisplayName, InodeId, NameKey, RevisionNo,
    };
    use std::assert_matches;

    #[test]
    fn layout_compaction_keeps_the_first_row_per_content_id() {
        use super::super::merge_iterator::locality_of;
        use super::retention_clusters;
        use loonfs_types::format::manifest::{ContentLayoutRecord, MetadataFamilyGroup};
        use loonfs_types::{ContentExtent, ContentId, ContentLayout, ExtentObject, NamespaceId};
        let owner = NamespaceId::parse("owner").expect("namespace");
        let first = ContentId::generate();
        let second = ContentId::generate();
        let row = |content_id: ContentId, committed_seq| {
            MetadataRow::ContentLayout(ContentLayoutRecord {
                owner_namespace_id: owner.clone(),
                content_id: content_id.clone(),
                committed_seq: ChangeSeq(committed_seq),
                size_bytes: 1,
                layout: ContentLayout {
                    extents: vec![ContentExtent {
                        owner_namespace_id: owner.clone(),
                        content_id,
                        object: ExtentObject::Whole,
                        offset: 0,
                        length: 1,
                    }],
                },
            })
        };
        let mut rows = vec![
            row(first.clone(), 1),
            row(second.clone(), 2),
            row(first.clone(), 3),
        ];
        rows.sort_by_key(MetadataRow::row_key);
        let cluster = &retention_clusters(MetadataFamilyGroup::ContentLayouts)[0];
        let mut operator = cluster.rule.operator(ChangeSeq(0));
        let family = MetadataRowFamily::ContentLayouts;
        let mut previous = String::new();
        let mut kept = Vec::new();
        for row in rows {
            let key = row.row_key();
            let group = locality_of(family, &key, cluster.locality);
            if previous != group {
                operator.close_group();
                previous = group.to_owned();
            }
            if let Some((_, row)) = operator.push(family, row, ChangeSeq(0)).expect("push") {
                kept.push(row);
            }
        }
        let mut expected = vec![row(first, 3), row(second, 2)];
        expected.sort_by_key(MetadataRow::row_key);
        assert_eq!(kept, expected);
    }

    const WHOLE_STATE_FAMILIES: [MetadataRowFamily; 3] = [
        MetadataRowFamily::Revisions,
        MetadataRowFamily::Attributes,
        MetadataRowFamily::Access,
    ];

    fn floor() -> ChangeSeq {
        ChangeSeq(100)
    }

    fn whole_state_row(
        family: MetadataRowFamily,
        inode: u64,
        revision: u64,
        committed_seq: u64,
    ) -> MetadataRow {
        let commit_id =
            loonfs_types::CommitId::parse(format!("c_row_{committed_seq}")).expect("commit id");
        match family {
            MetadataRowFamily::Access => {
                MetadataRow::AccessRevision(crate::metadata::AccessRevisionRecord {
                    inode_id: InodeId(u128::from(inode)),
                    access_revision_no: AccessRevisionNo(revision),
                    committed_seq: ChangeSeq(committed_seq),
                    commit_id,
                    delta_index: 0,
                    committed_by: loonfs_types::ActorId::loonfs(),
                    committed_at_ms: 1_000 + committed_seq,
                    boundary: false,
                    grants: Default::default(),
                })
            }
            MetadataRowFamily::Revisions => {
                MetadataRow::FileRevision(crate::metadata::RevisionRecord {
                    inode_id: InodeId(u128::from(inode)),
                    revision_no: RevisionNo(revision),
                    committed_seq: ChangeSeq(committed_seq),
                    commit_id,
                    committed_by: loonfs_types::ActorId::loonfs(),
                    committed_at_ms: 1_000 + committed_seq,
                    delta_index: 0,
                    content_ref: loonfs_types::ContentRef::blob_v1(
                        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
                        loonfs_types::ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff")
                            .expect("content id"),
                        b"body",
                        loonfs_types::ChecksumAlgorithm::Crc64nvme,
                    ),
                })
            }
            _ => MetadataRow::AttributesRevision(crate::metadata::AttributesRevisionRecord {
                inode_id: InodeId(u128::from(inode)),
                attributes_revision_no: AttributesRevisionNo(revision),
                committed_seq: ChangeSeq(committed_seq),
                commit_id,
                delta_index: 0,
                committed_by: loonfs_types::ActorId::loonfs(),
                committed_at_ms: 1_000 + committed_seq,
                attributes: Default::default(),
            }),
        }
    }

    /// A binding row of the file `child` named `name` under `parent`, at
    /// `seq`, of the binding that the bind at `bind_seq` established.
    fn binding_row(
        parent: u64,
        name: &str,
        child: u64,
        seq: u64,
        bind_seq: u64,
        bound: bool,
    ) -> MetadataRow {
        let mut record = crate::test_support::binding_row(parent, name, child, seq);
        record.bind_position.seq = ChangeSeq(bind_seq);
        if !bound {
            record.state = loonfs_types::format::manifest::DirentryBindingState::Unbound;
        }
        MetadataRow::DirentryBinding(record)
    }

    fn bind_row(parent: u64, name: &str, child: u64, bind_seq: u64) -> MetadataRow {
        binding_row(parent, name, child, bind_seq, bind_seq, true)
    }

    fn summary_row(child: u64, seq: u64, bind_seq: u64) -> MetadataRow {
        binding_row(7, "a.txt", child, seq, bind_seq, true)
    }

    fn unbind_row(child: u64, seq: u64, bind_seq: u64) -> MetadataRow {
        binding_row(7, "a.txt", child, seq, bind_seq, false)
    }

    /// A bound row of the directory `child` named `docs` under 7, at `seq`,
    /// of the binding that the bind at `bind_seq` established: the bind
    /// itself when `seq` is `bind_seq`, and a subtree row otherwise.
    fn directory_row(child: u64, seq: u64, bind_seq: u64) -> MetadataRow {
        let mut record = crate::test_support::binding_row(7, "docs", child, seq);
        record.bind_position.seq = ChangeSeq(bind_seq);
        record.child_kind = loonfs_types::InodeKind::Directory;
        record.state = loonfs_types::format::manifest::DirentryBindingState::Bound {
            display_name: DisplayName::parse("docs").expect("display name"),
            revision: None,
            subtree_seq: Some(ChangeSeq(seq)),
        };
        MetadataRow::DirentryBinding(record)
    }

    /// Runs `rows` through the retention rule of `family`'s cluster in key
    /// order, as a base merge does, and returns the rows it keeps.
    fn retained(
        group: MetadataFamilyGroup,
        family: MetadataRowFamily,
        mut rows: Vec<MetadataRow>,
    ) -> Vec<MetadataRow> {
        use super::super::merge_iterator::locality_of;
        if family == MetadataRowFamily::DirentryChildBinds {
            rows.retain(|row| {
                matches!(row, MetadataRow::DirentryBinding(binding) if binding.is_binding_event())
            });
        }
        rows.sort_by_key(|row| row.row_key_for_family(family));
        let cluster = retention_clusters(group)
            .iter()
            .find(|cluster| cluster.families.contains(&family))
            .expect("a cluster merges the family");
        let mut operator = cluster.rule.operator(ChangeSeq(0));
        let mut locality: Option<String> = None;
        let mut kept = Vec::new();
        for row in rows {
            let key = row.row_key_for_family(family);
            let row_locality = locality_of(family, &key, cluster.locality);
            if locality.as_deref() != Some(row_locality) {
                operator.close_group();
                locality = Some(row_locality.to_owned());
            }
            if let Some((_, row)) = operator.push(family, row, floor()).expect("push") {
                kept.push(row);
            }
        }
        kept
    }

    #[test]
    fn one_inode_keeps_the_newest_row_at_the_floor() {
        for family in WHOLE_STATE_FAMILIES {
            let mut operator = RetentionRule::WholeState.operator(ChangeSeq(0));
            let mut kept = Vec::new();
            // Newest first: two above the floor, then a hundred thousand below.
            for (revision, committed_seq) in [(100_002u64, 102u64), (100_001, 101)] {
                if let Some((_, row)) = operator
                    .push(
                        family,
                        whole_state_row(family, 7, revision, committed_seq),
                        floor(),
                    )
                    .expect("push")
                {
                    kept.push(row);
                }
            }
            for revision in (1..=100_000u64).rev() {
                let pushed = operator
                    .push(family, whole_state_row(family, 7, revision, 50), floor())
                    .expect("push");
                if let Some((_, row)) = pushed {
                    kept.push(row);
                }
            }
            assert_eq!(kept.len(), 3, "two above the floor and the newest below it");
            assert_eq!(kept[2], whole_state_row(family, 7, 100_000, 50));
        }
    }

    #[test]
    fn two_whole_state_rows_at_one_revision_below_the_floor_are_refused() {
        for family in WHOLE_STATE_FAMILIES {
            let mut operator = RetentionRule::WholeState.operator(ChangeSeq(0));
            operator
                .push(family, whole_state_row(family, 7, 5, 50), floor())
                .expect("the first row at the floor is kept");
            let error = operator
                .push(family, whole_state_row(family, 7, 5, 49), floor())
                .expect_err("a repeated revision at the floor is refused");
            assert!(error
                .to_string()
                .contains(&format!("two {} rows at revision", family.as_str())));
        }
    }

    fn active_deletion_row(deletion_seq: u64, action: ActiveDeletionRowAction) -> MetadataRow {
        MetadataRow::ActiveDeletion(crate::metadata::ActiveDeletionRecord {
            root_inode_id: InodeId(9),
            deletion_seq: ChangeSeq(deletion_seq),
            action,
        })
    }

    fn listed(deletion_seq: u64) -> MetadataRow {
        active_deletion_row(
            deletion_seq,
            ActiveDeletionRowAction::Listed {
                inode_kind: loonfs_types::InodeKind::Directory,
                deleted_at_ms: 1_000,
                deleted_by: loonfs_types::ActorId::loonfs(),
                deleted_binding: loonfs_types::format::manifest::DeletedBinding {
                    parent_inode_id: ROOT_INODE_ID,
                    name_key: loonfs_types::NameKey::parse("deleted").expect("valid name key"),
                    display_name: loonfs_types::DisplayName::parse("deleted")
                        .expect("valid display name"),
                },
            },
        )
    }

    #[test]
    fn a_cancelled_deletion_leaves_and_a_live_one_stays() {
        let mut operator = RetentionRule::ActiveDeletions.operator(ChangeSeq(0));
        let removed = active_deletion_row(
            3,
            ActiveDeletionRowAction::Removed {
                removed_seq: ChangeSeq(4),
            },
        );
        assert!(operator
            .push(MetadataRowFamily::ActiveDeletions, removed, floor())
            .expect("push")
            .is_none());
        assert!(operator
            .push(MetadataRowFamily::ActiveDeletions, listed(3), floor())
            .expect("push")
            .is_none());

        operator.close_group();
        assert!(
            operator
                .push(MetadataRowFamily::ActiveDeletions, listed(3), floor())
                .expect("push")
                .is_some(),
            "the next deletion is decided on its own markers, not the previous one's"
        );
    }

    /// Checks that a listed row at or below the acknowledged floor leaves,
    /// and that one above it stays, even at or below the retention floor.
    #[test]
    fn a_listed_row_leaves_at_or_below_the_acknowledged_floor() {
        let mut operator = RetentionRule::ActiveDeletions.operator(ChangeSeq(40));
        let mut kept = Vec::new();
        for deletion_seq in [39, 40, 41, 99] {
            operator.close_group();
            if let Some((_, row)) = operator
                .push(
                    MetadataRowFamily::ActiveDeletions,
                    listed(deletion_seq),
                    floor(),
                )
                .expect("push")
            {
                kept.push(row);
            }
        }
        assert_eq!(kept, vec![listed(41), listed(99)]);
    }

    /// Checks that both binding indexes keep an edge's rows above the floor
    /// and its newest bound row at or below it, reading the edge newest
    /// first, and that the tombstone family keeps every event.
    #[test]
    fn binding_and_tombstone_histories_keep_their_floor_rows_newest_first() {
        // An edge bound at 50, unbound at 60, bound again at 70 and 120.
        let edge = vec![
            bind_row(7, "a.txt", 42, 50),
            unbind_row(42, 60, 50),
            bind_row(7, "a.txt", 42, 70),
            unbind_row(42, 110, 70),
            bind_row(7, "a.txt", 42, 120),
        ];
        for family in [
            MetadataRowFamily::DirentryBinds,
            MetadataRowFamily::DirentryChildBinds,
        ] {
            assert_eq!(
                retained(MetadataFamilyGroup::Bindings, family, edge.clone()),
                vec![
                    bind_row(7, "a.txt", 42, 120),
                    unbind_row(42, 110, 70),
                    bind_row(7, "a.txt", 42, 70),
                ],
                "{family:?}"
            );
        }
        let tombstone = |seq: u64, set: bool| {
            MetadataRow::Tombstone(crate::metadata::SubtreeTombstoneRecord {
                root_inode_id: InodeId(42),
                committed_seq: ChangeSeq(seq),
                delta_index: 0,
                commit_id: crate::test_support::binding_commit_id(),
                action: if set {
                    crate::metadata::TombstoneRowAction::Set {
                        root_kind: loonfs_types::InodeKind::File,
                        deleted_binding: loonfs_types::format::manifest::DeletedBinding {
                            parent_inode_id: InodeId(7),
                            name_key: NameKey::parse("a.txt").expect("name key"),
                            display_name: DisplayName::parse("a.txt").expect("display name"),
                        },
                    }
                } else {
                    crate::metadata::TombstoneRowAction::Revoke {
                        target: loonfs_types::format::manifest::DeltaPosition {
                            seq: ChangeSeq(seq - 1),
                            delta_index: 0,
                        },
                    }
                },
                committed_by: loonfs_types::ActorId::loonfs(),
                committed_at_ms: 0,
            })
        };
        let events = vec![
            tombstone(40, true),
            tombstone(41, false),
            tombstone(90, true),
            tombstone(91, false),
            tombstone(130, true),
        ];
        let mut newest_first = events.clone();
        newest_first.reverse();
        assert_eq!(
            retained(
                MetadataFamilyGroup::Tombstones,
                MetadataRowFamily::Tombstones,
                events
            ),
            newest_first
        );
    }

    /// Checks that an edge with summary rows above, at, and below the floor
    /// keeps its rows above the floor and the floor row of each kind, so the
    /// child index and the slot index keep the same binding events.
    #[test]
    fn an_edge_keeps_the_floor_row_of_each_kind() {
        // Bound at 20, written at 30, 40, 90, and 130, with the floor at 100.
        let edge = vec![
            bind_row(7, "a.txt", 42, 20),
            summary_row(42, 30, 20),
            summary_row(42, 40, 20),
            summary_row(42, 90, 20),
            summary_row(42, 130, 20),
        ];
        assert_eq!(
            retained(
                MetadataFamilyGroup::Bindings,
                MetadataRowFamily::DirentryBinds,
                edge.clone()
            ),
            vec![
                summary_row(42, 130, 20),
                summary_row(42, 90, 20),
                bind_row(7, "a.txt", 42, 20),
            ]
        );
        assert_eq!(
            retained(
                MetadataFamilyGroup::Bindings,
                MetadataRowFamily::DirentryChildBinds,
                edge
            ),
            vec![bind_row(7, "a.txt", 42, 20)]
        );
    }

    /// Checks that a directory's edge keeps every subtree row above the floor
    /// and exactly one at or below it, the newest, next to its bind, and that
    /// the child index keeps the bind alone.
    #[test]
    fn a_directory_edge_keeps_one_subtree_row_at_the_floor() {
        // Bound at 20 and advanced at 30, 40, 90, and 130, with the floor at
        // 100.
        let edge = vec![
            directory_row(42, 20, 20),
            directory_row(42, 30, 20),
            directory_row(42, 40, 20),
            directory_row(42, 90, 20),
            directory_row(42, 130, 20),
        ];
        assert_eq!(
            retained(
                MetadataFamilyGroup::Bindings,
                MetadataRowFamily::DirentryBinds,
                edge.clone()
            ),
            vec![
                directory_row(42, 130, 20),
                directory_row(42, 90, 20),
                directory_row(42, 20, 20),
            ]
        );
        assert_eq!(
            retained(
                MetadataFamilyGroup::Bindings,
                MetadataRowFamily::DirentryChildBinds,
                edge
            ),
            vec![directory_row(42, 20, 20)]
        );
    }

    /// Checks that an edge whose binding event at the floor is unbound loses
    /// its rows of both kinds at or below the floor, and keeps the rows above
    /// it.
    #[test]
    fn an_unbound_floor_row_drops_the_edges_history() {
        let edge = vec![
            bind_row(7, "a.txt", 42, 20),
            summary_row(42, 30, 20),
            unbind_row(42, 50, 20),
            bind_row(7, "a.txt", 42, 110),
            summary_row(42, 120, 110),
        ];
        for family in [
            MetadataRowFamily::DirentryBinds,
            MetadataRowFamily::DirentryChildBinds,
        ] {
            let mut expected = vec![summary_row(42, 120, 110), bind_row(7, "a.txt", 42, 110)];
            if family == MetadataRowFamily::DirentryChildBinds {
                expected.remove(0);
            }
            assert_eq!(
                retained(MetadataFamilyGroup::Bindings, family, edge.clone()),
                expected,
                "{family:?}"
            );
        }
    }

    #[test]
    fn two_children_bound_in_one_slot_at_the_floor_are_refused() {
        let mut operator = RetentionRule::Bindings.operator(ChangeSeq(0));
        assert!(operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "a.txt", 2, 20),
                floor(),
            )
            .expect("push")
            .is_some());
        operator.close_group();
        let error = operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "a.txt", 3, 10),
                floor(),
            )
            .expect_err("a second child bound in the slot at the floor is refused");
        assert_matches!(error, CoreError::NamespaceCorrupt(_));
        assert!(
            error
                .to_string()
                .contains(&format!("children `{}` and `{}`", InodeId(2), InodeId(3))),
            "the error must name both children, got: {error}"
        );
    }

    #[test]
    fn two_edges_bound_for_one_child_at_the_floor_are_refused() {
        let mut operator = RetentionRule::Bindings.operator(ChangeSeq(0));
        for (parent, name, child) in [(9, "z.txt", 41), (7, "a.txt", 42)] {
            assert!(
                operator
                    .push(
                        MetadataRowFamily::DirentryChildBinds,
                        bind_row(parent, name, child, 20),
                        floor(),
                    )
                    .expect("push")
                    .is_some(),
                "one bound edge per child is kept"
            );
            operator.close_group();
        }
        let error = operator
            .push(
                MetadataRowFamily::DirentryChildBinds,
                bind_row(8, "b.txt", 42, 10),
                floor(),
            )
            .expect_err("a second edge bound for the child at the floor is refused");
        assert_matches!(error, CoreError::NamespaceCorrupt(_));
        assert!(
            error.to_string().contains(&format!(
                "as `a.txt` under parent `{}` and as `b.txt` under parent `{}`",
                InodeId(7),
                InodeId(8)
            )),
            "the error must name both edges, got: {error}"
        );
    }
}
