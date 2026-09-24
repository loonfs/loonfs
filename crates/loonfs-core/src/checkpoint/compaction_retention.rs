//! Applies retention rules while rows are streamed in key order.

use crate::error::{CoreError, Result};
use loonfs_api::wire::manifest::{ActiveDeletionRowAction, MetadataRow, MetadataRowFamily};
use loonfs_api::{ChangeSeq, InodeId};

/// One row a retention operator kept, and the family it belongs to.
pub(super) type KeptRow = (MetadataRowFamily, MetadataRow);

/// Retention rule assigned to a row cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RetentionRule {
    /// Retain every row in the group.
    KeepEveryRow,
    /// A commit row or its receipt is decided by its own commit sequence against the floor.
    CommitHistory,
    /// Every revision above the floor, plus the newest at or below it, per
    /// inode.
    WholeState,
    /// Retain active deletions and remove completed deletion pairs.
    ActiveDeletions,
    /// Retains slot or child values above the floor and its bound value at the floor.
    Bindings,
}

impl RetentionRule {
    pub(super) fn operator(self) -> RetentionOperator {
        match self {
            Self::KeepEveryRow => RetentionOperator::KeepEveryRow,
            Self::CommitHistory => RetentionOperator::CommitHistory,
            Self::WholeState => RetentionOperator::WholeState(WholeStateRetention::default()),
            Self::ActiveDeletions => {
                RetentionOperator::ActiveDeletions(ActiveDeletionRetention::default())
            }
            Self::Bindings => RetentionOperator::Bindings(BindingRetention::default()),
        }
    }
}

/// The state one cluster's rule holds while a merge streams through it.
#[derive(Debug)]
pub(super) enum RetentionOperator {
    KeepEveryRow,
    CommitHistory,
    WholeState(WholeStateRetention),
    ActiveDeletions(ActiveDeletionRetention),
    Bindings(BindingRetention),
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
    ) -> Option<KeptRow> {
        match (self, row) {
            (Self::Bindings(state), MetadataRow::DirentryBinding(binding))
                if binding.committed_seq > floor_seq =>
            {
                state.close_group()
            }
            _ => None,
        }
    }

    /// Finishes the current key group and returns any retained row.
    pub(super) fn close_group(&mut self, _floor_seq: ChangeSeq) -> Result<Option<KeptRow>> {
        match self {
            Self::KeepEveryRow | Self::CommitHistory => Ok(None),
            Self::WholeState(state) => {
                state.close_group();
                Ok(None)
            }
            Self::ActiveDeletions(state) => {
                state.close_group();
                Ok(None)
            }
            Self::Bindings(state) => Ok(state.close_group()),
        }
    }

    /// Number of rows currently held by this operator.
    pub(super) fn held_rows(&self) -> usize {
        match self {
            Self::KeepEveryRow
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
        MetadataRow::Commit(record) if record.seq < floor_seq => None,
        _ => Some(row),
    }
}

/// Retains all whole-state rows above the floor and the newest revision at
/// or below the floor for each inode.
///
/// Whole-state row keys sort each inode's revisions newest first. The first row
/// at or below the floor represents current state, including a cleared state.
/// Older revisions cannot be observed and may be removed. Deleted inodes keep
/// their whole-state rows so an undelete restores the prior state. The operator
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

    fn close_group(&mut self) -> Option<KeptRow> {
        self.at_floor.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::{AccessRevisionNo, AttributesRevisionNo, DisplayName, InodeId, NameKey};

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
            loonfs_api::CommitId::parse(format!("c_row_{committed_seq}")).expect("commit id");
        if family == MetadataRowFamily::Access {
            MetadataRow::AccessRevision(crate::metadata::AccessRevisionRecord {
                inode_id: InodeId(inode),
                access_revision_no: AccessRevisionNo(revision),
                committed_seq: ChangeSeq(committed_seq),
                commit_id,
                delta_index: 0,
                committed_by: loonfs_api::ActorId::loonfs(),
                committed_at_ms: 1_000 + committed_seq,
                boundary: false,
                grants: Default::default(),
            })
        } else {
            MetadataRow::AttributesRevision(crate::metadata::AttributesRevisionRecord {
                inode_id: InodeId(inode),
                attributes_revision_no: AttributesRevisionNo(revision),
                committed_seq: ChangeSeq(committed_seq),
                commit_id,
                delta_index: 0,
                committed_by: loonfs_api::ActorId::loonfs(),
                committed_at_ms: 1_000 + committed_seq,
                attributes: Default::default(),
            })
        }
    }

    fn bind_row(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            state: loonfs_api::wire::manifest::DirentryBindingState::Bound {
                display_name: DisplayName::parse(name).expect("display name"),
            },
            child_inode_id: InodeId(42),
            committed_seq: ChangeSeq(bind_seq),
            delta_index: 0,
        })
    }

    fn unbind_row(parent: u64, name: &str, unbind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            child_inode_id: InodeId(42),
            committed_seq: ChangeSeq(unbind_seq),
            delta_index: 0,
            state: loonfs_api::wire::manifest::DirentryBindingState::Unbound,
        })
    }

    #[test]
    fn one_inode_keeps_the_newest_row_at_the_floor_and_holds_nothing() {
        for family in [MetadataRowFamily::Attributes, MetadataRowFamily::Access] {
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
        for family in [MetadataRowFamily::Attributes, MetadataRowFamily::Access] {
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
                inode_kind: loonfs_api::InodeKind::Directory,
                deleted_at_ms: 1_000,
                deleted_by: loonfs_api::ActorId::loonfs(),
                deleted_binding: loonfs_api::wire::manifest::DeletedBinding {
                    parent_inode_id: InodeId(1),
                    name_key: loonfs_api::NameKey::parse("deleted").expect("valid name key"),
                    display_name: loonfs_api::DisplayName::parse("deleted")
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
                    bind_row(7, "hot.txt", position * 2 - 1),
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
                bind_row(7, "a.txt", 10),
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
                bind_row(7, "b.txt", 11),
                floor(),
            )
            .expect("push");

        let error = operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "b.txt", 12),
                floor(),
            )
            .expect_err("a superseded bind with no unbind is refused");
        assert!(
            error.to_string().contains("seq `11`"),
            "the error must name the bind that was superseded, got: {error}"
        );
        assert!(error.to_string().contains("superseded at or below"));
    }
}
