//! Applies retention rules while rows are streamed in key order.

use crate::error::{CoreError, Result};
use loonfs_types::format::manifest::{ActiveDeletionRowAction, MetadataRow, MetadataRowFamily};
use loonfs_types::{ChangeSeq, InodeId, NameKey};

/// One row a retention operator kept, and the family it belongs to.
pub(super) type KeptRow = (MetadataRowFamily, MetadataRow);

/// Retention rule assigned to a row cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RetentionRule {
    /// Retain every row in the group.
    KeepEveryRow,
    /// Keeps the first row in each group, whose keys sort newest first.
    NewestPerGroup,
    /// A commit row or its receipt is decided by its own commit sequence against the floor.
    CommitHistory,
    /// Every revision above the floor, plus the newest at or below it, per
    /// inode.
    WholeState,
    /// Retain active deletions and remove completed deletion pairs.
    ActiveDeletions,
    /// Per edge, retains the values above the floor and the bound value at
    /// the floor.
    Bindings,
}

impl RetentionRule {
    pub(super) fn operator(self) -> RetentionOperator {
        match self {
            Self::KeepEveryRow => RetentionOperator::KeepEveryRow,
            Self::NewestPerGroup => RetentionOperator::NewestPerGroup(false),
            Self::CommitHistory => RetentionOperator::CommitHistory,
            Self::WholeState => RetentionOperator::WholeState(WholeStateRetention::default()),
            Self::ActiveDeletions => {
                RetentionOperator::ActiveDeletions(ActiveDeletionRetention::default())
            }
            Self::Bindings => RetentionOperator::Bindings(Box::default()),
        }
    }
}

/// The state one cluster's rule holds while a merge streams through it.
#[derive(Debug)]
pub(super) enum RetentionOperator {
    KeepEveryRow,
    NewestPerGroup(bool),
    CommitHistory,
    WholeState(WholeStateRetention),
    ActiveDeletions(ActiveDeletionRetention),
    Bindings(Box<BindingRetention>),
}

impl RetentionOperator {
    /// Returns the retained row, or `None` when the row is dropped or held
    /// until [`Self::close_group`].
    pub(super) fn push(
        &mut self,
        family: MetadataRowFamily,
        row: MetadataRow,
        floor_seq: ChangeSeq,
    ) -> Result<Option<KeptRow>> {
        let kept = match self {
            Self::KeepEveryRow => Some(row),
            Self::NewestPerGroup(kept) => (!std::mem::replace(kept, true)).then_some(row),
            Self::CommitHistory => keep_commit_history_row(row, floor_seq),
            Self::WholeState(state) => state.push(family, row, floor_seq)?,
            Self::ActiveDeletions(state) => state.push(row),
            Self::Bindings(state) => state.push(family, row, floor_seq)?,
        };
        Ok(kept.map(|row| (family, row)))
    }

    pub(super) fn take_floor_value_before(
        &mut self,
        row: &MetadataRow,
        floor_seq: ChangeSeq,
    ) -> Result<Option<KeptRow>> {
        match (self, row) {
            (Self::Bindings(state), MetadataRow::DirentryBinding(binding))
                if binding.committed_seq > floor_seq =>
            {
                state.close_group()
            }
            _ => Ok(None),
        }
    }

    /// Finishes the current key group and returns any retained row.
    pub(super) fn close_group(&mut self, _floor_seq: ChangeSeq) -> Result<Option<KeptRow>> {
        match self {
            Self::KeepEveryRow | Self::CommitHistory => Ok(None),
            Self::NewestPerGroup(kept) => {
                *kept = false;
                Ok(None)
            }
            Self::WholeState(state) => {
                state.close_group();
                Ok(None)
            }
            Self::ActiveDeletions(state) => {
                state.close_group();
                Ok(None)
            }
            Self::Bindings(state) => state.close_group(),
        }
    }

    /// Number of rows currently held by this operator.
    pub(super) fn held_rows(&self) -> usize {
        match self {
            Self::KeepEveryRow
            | Self::NewestPerGroup(_)
            | Self::CommitHistory
            | Self::WholeState(_)
            | Self::ActiveDeletions(_) => 0,
            Self::Bindings(state) => usize::from(state.at_floor.is_some()),
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
/// state, and the floor keeps no older history. Deleted inodes keep their
/// whole-state rows so an undelete restores the prior state. The operator
/// processes this order without retaining the complete history.
#[derive(Debug, Default)]
pub(super) struct WholeStateRetention {
    /// Whether this inode has already kept its newest row at or below the
    /// floor.
    kept_at_floor: bool,
    /// The revision of the previous row at or below the floor, which is what
    /// catches two rows sharing one revision number. Descending revision
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
        self.previous_at_floor = Some(revision);
        if self.kept_at_floor {
            return Ok(None);
        }
        self.kept_at_floor = true;
        Ok(Some(row))
    }

    fn close_group(&mut self) {
        self.kept_at_floor = false;
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
/// Active deletions represent current recoverable state, so the retention
/// floor does not remove a listed deletion. The operator removes only a
/// listed row paired with its removal marker. Row-key order places the marker
/// first, allowing one flag to determine whether the listed row is retained.
#[derive(Debug, Default)]
pub(super) struct ActiveDeletionRetention {
    /// Whether this deletion's removal marker has already arrived.
    revoked: bool,
}

impl ActiveDeletionRetention {
    fn push(&mut self, row: MetadataRow) -> Option<MetadataRow> {
        let MetadataRow::ActiveDeletion(crate::metadata::ActiveDeletionRecord { action, .. }) =
            &row
        else {
            return Some(row);
        };
        match action {
            // Every removal marker goes; what it is here to decide is whether
            // the listed row goes with it.
            ActiveDeletionRowAction::Removed { .. } => {
                self.revoked = true;
                None
            }
            ActiveDeletionRowAction::Listed { .. } if self.revoked => None,
            ActiveDeletionRowAction::Listed { .. } => Some(row),
        }
    }

    fn close_group(&mut self) {
        self.revoked = false;
    }
}

#[derive(Debug, Default)]
pub(super) struct BindingRetention {
    at_floor: Option<KeptRow>,
    /// The parent, name, and child of the last bound floor value this index
    /// kept. The slot index keeps one slot's edges adjacent and the child
    /// index keeps one child's edges adjacent, so a second edge bound in the
    /// same slot, or for the same child, is always compared with this one.
    last_bound_edge: Option<(InodeId, NameKey, InodeId)>,
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
        if binding.is_bound() {
            if let Some((_, MetadataRow::DirentryBinding(previous))) = &self.at_floor {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "binding at seq `{}` delta `{}` is superseded at or below the retention floor without an unbound version",
                    previous.committed_seq, previous.delta_index
                )));
            }
        }
        self.at_floor = binding.is_bound().then_some((family, row));
        Ok(None)
    }

    fn close_group(&mut self) -> Result<Option<KeptRow>> {
        let Some(kept) = self.at_floor.take() else {
            return Ok(None);
        };
        let (family, MetadataRow::DirentryBinding(binding)) = &kept else {
            return Ok(Some(kept));
        };
        if let Some((parent_inode_id, name_key, child_inode_id)) = &self.last_bound_edge {
            let same_slot =
                *parent_inode_id == binding.parent_inode_id && *name_key == binding.name_key;
            let same_child = *child_inode_id == binding.child_inode_id;
            if *family == MetadataRowFamily::DirentryBinds && same_slot {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "parent `{parent_inode_id}` binds name `{name_key}` to children \
                     `{child_inode_id}` and `{}` at or below the retention floor; refusing to \
                     drop rows",
                    binding.child_inode_id
                )));
            }
            if *family == MetadataRowFamily::DirentryChildBinds && same_child {
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
        Ok(Some(kept))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_types::{
        AccessRevisionNo, AttributesRevisionNo, DisplayName, InodeId, NameKey, RevisionNo,
    };

    #[test]
    fn layout_compaction_keeps_the_newest_row_per_content_id() {
        use super::super::compaction_merge::locality_of;
        use super::super::streaming_compaction::retention_clusters;
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
        let mut operator = cluster.rule.operator();
        let family = MetadataRowFamily::ContentLayouts;
        let mut previous = String::new();
        let mut kept = Vec::new();
        for row in rows {
            let key = row.row_key();
            let group = locality_of(family, &key, cluster.locality);
            if previous != group {
                operator.close_group(ChangeSeq(0)).expect("close");
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
                    inode_id: InodeId(inode),
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
                    inode_id: InodeId(inode),
                    revision_no: RevisionNo(revision),
                    committed_seq: ChangeSeq(committed_seq),
                    commit_id,
                    committed_by: loonfs_types::ActorId::loonfs(),
                    committed_at_ms: 1_000 + committed_seq,
                    delta_index: 0,
                    content_ref: loonfs_types::ContentRef::blob_v1(
                        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
                        loonfs_types::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                            .expect("content id"),
                        b"body",
                    ),
                    hash_state: None,
                    crc64nvme: None,
                })
            }
            _ => MetadataRow::AttributesRevision(crate::metadata::AttributesRevisionRecord {
                inode_id: InodeId(inode),
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

    fn bind_row(parent: u64, name: &str, child: u64, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                display_name: DisplayName::parse(name).expect("display name"),
            },
            child_inode_id: InodeId(child),
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(bind_seq),
            delta_index: 0,
        })
    }

    fn unbind_row(parent: u64, name: &str, unbind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            child_inode_id: InodeId(42),
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(unbind_seq),
            delta_index: 0,
            state: loonfs_types::format::manifest::DirentryBindingState::Unbound,
        })
    }

    #[test]
    fn one_inode_keeps_the_newest_row_at_the_floor_and_holds_nothing() {
        for family in WHOLE_STATE_FAMILIES {
            let mut operator = RetentionRule::WholeState.operator();
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
                assert_eq!(operator.held_rows(), 0);
            }
            assert_eq!(kept.len(), 3, "two above the floor and the newest below it");
            assert_eq!(kept[2], whole_state_row(family, 7, 100_000, 50));
        }
    }

    #[test]
    fn two_whole_state_rows_at_one_revision_below_the_floor_are_refused() {
        for family in WHOLE_STATE_FAMILIES {
            let mut operator = RetentionRule::WholeState.operator();
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

    #[test]
    fn a_cancelled_deletion_leaves_and_a_live_one_stays() {
        let mut operator = RetentionRule::ActiveDeletions.operator();
        let removed = MetadataRow::ActiveDeletion(crate::metadata::ActiveDeletionRecord {
            root_inode_id: InodeId(9),
            deletion_seq: ChangeSeq(3),
            action: ActiveDeletionRowAction::Removed {
                revocation_seq: ChangeSeq(4),
            },
        });
        let listed = MetadataRow::ActiveDeletion(crate::metadata::ActiveDeletionRecord {
            root_inode_id: InodeId(9),
            deletion_seq: ChangeSeq(3),
            action: ActiveDeletionRowAction::Listed {
                inode_kind: loonfs_types::InodeKind::Directory,
                deleted_at_ms: 1_000,
                deleted_by: loonfs_types::ActorId::loonfs(),
                deleted_binding: loonfs_types::format::manifest::DeletedBinding {
                    parent_inode_id: InodeId(1),
                    name_key: loonfs_types::NameKey::parse("deleted").expect("valid name key"),
                    display_name: loonfs_types::DisplayName::parse("deleted")
                        .expect("valid display name"),
                },
            },
        });
        assert!(operator
            .push(MetadataRowFamily::ActiveDeletions, removed.clone(), floor())
            .expect("push")
            .is_none());
        assert!(operator
            .push(MetadataRowFamily::ActiveDeletions, listed.clone(), floor())
            .expect("push")
            .is_none());
        assert_eq!(operator.held_rows(), 0);

        operator.close_group(floor()).expect("close");
        assert!(
            operator
                .push(MetadataRowFamily::ActiveDeletions, listed, floor())
                .expect("push")
                .is_some(),
            "the next deletion is decided on its own markers, not the previous one's"
        );
    }

    #[test]
    fn one_slot_of_many_versions_holds_at_most_one_row() {
        let mut operator = RetentionRule::Bindings.operator();
        let mut kept = Vec::new();
        let mut peak = 0usize;
        for position in 1..=100_000u64 {
            operator
                .push(
                    MetadataRowFamily::DirentryBinds,
                    bind_row(7, "hot.txt", 42, position * 2 - 1),
                    ChangeSeq(200_000),
                )
                .expect("push");
            peak = peak.max(operator.held_rows());
            operator
                .push(
                    MetadataRowFamily::DirentryBinds,
                    unbind_row(7, "hot.txt", position * 2),
                    ChangeSeq(200_000),
                )
                .expect("push");
            peak = peak.max(operator.held_rows());
            if let Some((_, row)) = operator.close_group(ChangeSeq(200_000)).expect("close") {
                kept.push(row);
            }
        }
        assert_eq!(peak, 1, "the operator holds one bind row and no more");
        assert!(kept.is_empty(), "every floor value is unbound");
    }

    #[test]
    fn a_second_unretired_bind_in_one_slot_is_refused() {
        let mut operator = RetentionRule::Bindings.operator();
        operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "a.txt", 42, 10),
                floor(),
            )
            .expect("push");
        assert!(operator
            .close_group(floor())
            .expect("close")
            .is_some_and(|(family, _)| family == MetadataRowFamily::DirentryBinds));

        // The next slot is unaffected by the one before it: the bind left
        // standing in `a.txt` is that slot's latest, which is what the
        // invariant allows.
        operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "b.txt", 42, 11),
                floor(),
            )
            .expect("push");

        let error = operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "b.txt", 42, 12),
                floor(),
            )
            .expect_err("a superseded bind with no unbind is refused");
        assert!(
            error.to_string().contains("seq `11`"),
            "the error must name the bind that was superseded, got: {error}"
        );
        assert!(error.to_string().contains("superseded at or below"));
    }

    #[test]
    fn two_children_bound_in_one_slot_at_the_floor_are_refused() {
        let mut operator = RetentionRule::Bindings.operator();
        operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "a.txt", 2, 20),
                floor(),
            )
            .expect("push");
        assert!(operator.close_group(floor()).expect("close").is_some());
        operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "a.txt", 3, 10),
                floor(),
            )
            .expect("push");

        let error = operator
            .close_group(floor())
            .expect_err("a second child bound in the slot at the floor is refused");
        assert!(matches!(error, CoreError::NamespaceCorrupt(_)), "{error:?}");
        assert!(
            error.to_string().contains("children `2` and `3`"),
            "the error must name both children, got: {error}"
        );
    }

    #[test]
    fn two_edges_bound_for_one_child_at_the_floor_are_refused() {
        let mut operator = RetentionRule::Bindings.operator();
        for (parent, name, child) in [(9, "z.txt", 41), (7, "a.txt", 42)] {
            operator
                .push(
                    MetadataRowFamily::DirentryChildBinds,
                    bind_row(parent, name, child, 20),
                    floor(),
                )
                .expect("push");
            assert!(
                operator.close_group(floor()).expect("close").is_some(),
                "one bound edge per child is kept"
            );
        }
        operator
            .push(
                MetadataRowFamily::DirentryChildBinds,
                bind_row(8, "b.txt", 42, 10),
                floor(),
            )
            .expect("push");

        let error = operator
            .close_group(floor())
            .expect_err("a second edge bound for the child at the floor is refused");
        assert!(matches!(error, CoreError::NamespaceCorrupt(_)), "{error:?}");
        assert!(
            error
                .to_string()
                .contains("as `a.txt` under parent `7` and as `b.txt` under parent `8`"),
            "the error must name both edges, got: {error}"
        );
    }
}
