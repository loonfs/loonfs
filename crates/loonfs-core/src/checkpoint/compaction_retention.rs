//! Applies retention rules while rows are streamed in key order.
//!
//! Each operator keeps bounded state for its current key group. Reverse bind
//! rows require point reads because binds and unbinds use different key
//! prefixes; [`super::streaming_compaction`] handles those reads.

use super::frozen_floor::{bind_survives_frozen_floor, BindingGeneration};
use crate::error::{CoreError, Result};
use loonfs_api::wire::manifest::{ActiveDeletionRowAction, MetadataRow, MetadataRowFamily};
use loonfs_api::{AttributeRevisionNo, ChangeSeq};
use std::collections::BTreeSet;

/// One row a retention operator kept, and the family it belongs to.
pub(super) type KeptRow = (MetadataRowFamily, MetadataRow);

/// Retention rule assigned to a row cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RetentionRule {
    /// Retain every row in the group.
    KeepEveryRow,
    /// A receipt is decided by its own sequence against the floor.
    Receipts,
    /// Every revision above the floor, plus the newest at or below it, per
    /// inode.
    Attributes,
    /// Retain active deletions and remove completed deletion pairs.
    ActiveDeletions,
    /// A bind and the unbinds of its own generation, decided together.
    ForwardBindings,
    /// Reverse bind rows, each decided by a point read into the snapshot.
    /// The operator holds nothing; the merge does the reading.
    ReverseBindProbe,
}

impl RetentionRule {
    pub(super) fn operator(self) -> RetentionOperator {
        match self {
            Self::KeepEveryRow => RetentionOperator::KeepEveryRow,
            Self::Receipts => RetentionOperator::Receipts,
            Self::Attributes => RetentionOperator::Attributes(AttributeRetention::default()),
            Self::ActiveDeletions => {
                RetentionOperator::ActiveDeletions(ActiveDeletionRetention::default())
            }
            Self::ForwardBindings => RetentionOperator::ForwardBindings(Box::default()),
            // The merge checks reverse bind rows against its snapshot, so
            // this operator has no state to maintain.
            Self::ReverseBindProbe => RetentionOperator::KeepEveryRow,
        }
    }
}

/// The state one cluster's rule holds while a merge streams through it.
#[derive(Debug)]
pub(super) enum RetentionOperator {
    KeepEveryRow,
    Receipts,
    Attributes(AttributeRetention),
    ActiveDeletions(ActiveDeletionRetention),
    ForwardBindings(Box<BindingRetention>),
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
            Self::Receipts => keep_receipt(row, floor_seq),
            Self::Attributes(state) => state.push(row, floor_seq)?,
            Self::ActiveDeletions(state) => state.push(row),
            Self::ForwardBindings(state) => state.push(row, floor_seq),
        };
        Ok(kept.map(|row| (family, row)))
    }

    /// Finishes the current key group and returns any retained row.
    pub(super) fn close_group(&mut self, floor_seq: ChangeSeq) -> Result<Option<KeptRow>> {
        match self {
            Self::KeepEveryRow | Self::Receipts => Ok(None),
            Self::Attributes(state) => {
                state.close_group();
                Ok(None)
            }
            Self::ActiveDeletions(state) => {
                state.close_group();
                Ok(None)
            }
            Self::ForwardBindings(state) => Ok(state
                .close_group(floor_seq)?
                .map(|row| (MetadataRowFamily::DirentryBinds, row))),
        }
    }

    /// Number of rows currently held by this operator.
    pub(super) fn held_rows(&self) -> usize {
        match self {
            Self::KeepEveryRow
            | Self::Receipts
            | Self::Attributes(_)
            | Self::ActiveDeletions(_) => 0,
            Self::ForwardBindings(state) => usize::from(state.held_bind.is_some()),
        }
    }
}

/// Keeps commit receipts at or above the retention floor.
///
/// A commit ID is idempotent only while its receipt is retained. After a
/// receipt below the floor is removed, retrying that commit ID creates a new
/// mutation (format spec, section 3.3).
fn keep_receipt(row: MetadataRow, floor_seq: ChangeSeq) -> Option<MetadataRow> {
    match &row {
        MetadataRow::CommitReceipt(crate::metadata::CommitReceiptRecord {
            committed_seq, ..
        }) if *committed_seq < floor_seq => None,
        _ => Some(row),
    }
}

/// Retains all attribute revisions above the floor and the newest revision at
/// or below the floor for each inode.
///
/// Attribute row keys sort each inode's revisions newest first. The first row
/// at or below the floor represents current state, including an empty map that
/// records cleared attributes. Older revisions cannot be observed and may be
/// removed. Deleted inodes keep their attribute rows so an undelete restores
/// the prior attributes. The operator processes this order without retaining
/// the complete history.
#[derive(Debug, Default)]
pub(super) struct AttributeRetention {
    /// Whether this inode has already kept its newest row at or below the
    /// floor.
    kept_at_floor: bool,
    /// The revision of the previous row at or below the floor, which is what
    /// catches two rows sharing one revision number. Descending revision
    /// order puts any such pair next to each other, so one field sees them.
    previous_at_floor: Option<AttributeRevisionNo>,
}

impl AttributeRetention {
    fn push(&mut self, row: MetadataRow, floor_seq: ChangeSeq) -> Result<Option<MetadataRow>> {
        let MetadataRow::AttributesRevision(crate::metadata::AttributesRevisionRecord {
            inode_id,
            attributes_revision_no,
            committed_seq,
            ..
        }) = &row
        else {
            return Ok(Some(row));
        };
        if *committed_seq > floor_seq {
            return Ok(Some(row));
        }
        // Writer invariant: one inode's attribute revisions are numbered
        // without gaps or repeats, so "the newest at or below the floor"
        // names exactly one row. Two rows sharing a number would make the
        // choice arbitrary and the drop unsafe; refuse to compact state that
        // violates it.
        if self.previous_at_floor == Some(*attributes_revision_no) {
            return Err(CoreError::NamespaceCorrupt(format!(
                "inode `{inode_id}` has two attribute rows at revision \
                 `{attributes_revision_no}` at or below the retention floor; refusing to drop rows"
            )));
        }
        self.previous_at_floor = Some(*attributes_revision_no);
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

/// Processes one binding generation at a time. Each locality group contains
/// one bind and the unbind rows that can retire it. The bind sorts first and
/// is retained or removed when the group ends.
///
/// The one thing that outlives a generation is the slot invariant below: at
/// or below the floor only the latest bind in a slot may stand un-retired, and
/// that is a property of the whole parent-and-name slot rather than of one
/// generation. It costs one generation identity, not one row per generation.
#[derive(Debug, Default)]
pub(super) struct BindingRetention {
    /// The bind row of the generation being read, held until its unbinds have
    /// arrived. One row: a generation has one bind.
    held_bind: Option<MetadataRow>,
    /// The generations this group's unbinds retired at or below the floor.
    /// One entry at most — every unbind in the group names the same
    /// generation — and it is the set both bind families are judged against, so
    /// bind families reach `bind_survives_frozen_floor` by the same route.
    unbound_at_floor: BTreeSet<BindingGeneration>,
    /// The one bind at or below the floor in this slot that nothing retired,
    /// or `None` while the slot has none. A second one means the first was
    /// superseded without an unbind.
    unretired_at_floor: Option<BindingGeneration>,
}

impl BindingRetention {
    fn push(&mut self, row: MetadataRow, floor_seq: ChangeSeq) -> Option<MetadataRow> {
        match &row {
            MetadataRow::DirentryUnbind(crate::metadata::DirentryUnbindRecord {
                parent_inode_id,
                name_key,
                bind_seq,
                bind_delta_index,
                unbind_seq,
                ..
            }) => {
                if *unbind_seq > floor_seq {
                    return Some(row);
                }
                // A spent marker: the floor covers it, so nothing can observe
                // the binding it retired, and it retires that binding for the
                // bind row held above.
                self.unbound_at_floor.insert((
                    *parent_inode_id,
                    name_key.clone(),
                    *bind_seq,
                    *bind_delta_index,
                ));
                None
            }
            // A bind above the floor survives whatever retired it later, so
            // it needs no verdict and is written straight through.
            MetadataRow::DirentryBind(crate::metadata::DirentryBindRecord { bind_seq, .. })
                if *bind_seq > floor_seq =>
            {
                Some(row)
            }
            MetadataRow::DirentryBind(crate::metadata::DirentryBindRecord { .. }) => {
                self.held_bind = Some(row);
                None
            }
            _ => Some(row),
        }
    }

    fn close_group(&mut self, floor_seq: ChangeSeq) -> Result<Option<MetadataRow>> {
        let unbound_at_floor = std::mem::take(&mut self.unbound_at_floor);
        let Some(row) = self.held_bind.take() else {
            return Ok(None);
        };
        let MetadataRow::DirentryBind(crate::metadata::DirentryBindRecord {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            ..
        }) = &row
        else {
            return Ok(Some(row));
        };
        let generation = (
            *parent_inode_id,
            name_key.clone(),
            *bind_seq,
            *bind_delta_index,
        );
        self.check_the_slot_invariant(&generation)?;
        let survives = bind_survives_frozen_floor(&row, floor_seq, &unbound_at_floor);
        self.unretired_at_floor = survives.then_some(generation);
        Ok(survives.then_some(row))
    }

    /// Verifies the writer invariant required for binding cleanup.
    ///
    /// Superseding a bind also writes its unbind. Therefore every non-current
    /// bind at or below the retention floor must have a matching unbind.
    /// Generations arrive in ascending order, so a second unretired bind in
    /// the same slot proves that the earlier bind was superseded without an
    /// unbind. Cleanup is rejected when that occurs.
    fn check_the_slot_invariant(&mut self, generation: &BindingGeneration) -> Result<()> {
        let Some(previous) = self.unretired_at_floor.take() else {
            return Ok(());
        };
        if (&previous.0, &previous.1) != (&generation.0, &generation.1) {
            // A different parent-and-name slot: the previous slot's latest
            // bind was its latest, which is exactly what the invariant
            // allows.
            return Ok(());
        }
        Err(CoreError::NamespaceCorrupt(format!(
            "bind at seq `{}` delta {} for parent `{}` is superseded at or below the retention \
             floor without an unbind; refusing to drop rows",
            previous.2, previous.3, previous.0
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::{DisplayName, InodeId, NameKey};

    fn floor() -> ChangeSeq {
        ChangeSeq(100)
    }

    fn attribute_row(inode: u64, revision: u64, committed_seq: u64) -> MetadataRow {
        MetadataRow::AttributesRevision(crate::metadata::AttributesRevisionRecord {
            inode_id: InodeId(inode),
            attributes_revision_no: AttributeRevisionNo(revision),
            committed_seq: ChangeSeq(committed_seq),
            commit_id: loonfs_api::CommitId::parse(format!("c_attribute_{committed_seq}"))
                .expect("commit id"),
            delta_index: 0,
            updated_by: loonfs_api::ActorRef::loonfs_system(),
            updated_at_ms: 1_000 + committed_seq,
            attributes: Default::default(),
        })
    }

    fn bind_row(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBind(crate::metadata::DirentryBindRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            display_name: DisplayName::parse(name).expect("display name"),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(bind_seq),
            bind_delta_index: 0,
        })
    }

    fn unbind_row(parent: u64, name: &str, bind_seq: u64, unbind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryUnbind(crate::metadata::DirentryUnbindRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            display_name: DisplayName::parse(name).expect("display name"),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(bind_seq),
            bind_delta_index: 0,
            unbind_seq: ChangeSeq(unbind_seq),
            unbind_delta_index: 0,
        })
    }

    #[test]
    fn one_inode_keeps_the_newest_row_at_the_floor_and_holds_nothing() {
        let mut operator = RetentionRule::Attributes.operator();
        let mut kept = Vec::new();
        // Newest first: two above the floor, then a hundred thousand below.
        for (revision, committed_seq) in [(100_002u64, 102u64), (100_001, 101)] {
            if let Some((_, row)) = operator
                .push(
                    MetadataRowFamily::Attributes,
                    attribute_row(7, revision, committed_seq),
                    floor(),
                )
                .expect("push")
            {
                kept.push(row);
            }
        }
        for revision in (1..=100_000u64).rev() {
            let pushed = operator
                .push(
                    MetadataRowFamily::Attributes,
                    attribute_row(7, revision, 50),
                    floor(),
                )
                .expect("push");
            if let Some((_, row)) = pushed {
                kept.push(row);
            }
            assert_eq!(operator.held_rows(), 0);
        }
        assert_eq!(kept.len(), 3, "two above the floor and the newest below it");
        assert_eq!(kept[2], attribute_row(7, 100_000, 50));
    }

    #[test]
    fn two_attribute_rows_at_one_revision_below_the_floor_are_refused() {
        let mut operator = RetentionRule::Attributes.operator();
        operator
            .push(
                MetadataRowFamily::Attributes,
                attribute_row(7, 5, 50),
                floor(),
            )
            .expect("the first row at the floor is kept");
        let error = operator
            .push(
                MetadataRowFamily::Attributes,
                attribute_row(7, 5, 49),
                floor(),
            )
            .expect_err("a repeated revision at the floor is refused");
        assert!(error.to_string().contains("two attribute rows at revision"));
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
                deleted_at_ms: 1_000,
                deleted_by: loonfs_api::ActorRef::loonfs_system(),
                deleted_direntry: loonfs_api::wire::manifest::DeletedDirentry {
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
    fn one_name_slot_of_many_generations_holds_at_most_one_row() {
        let mut operator = RetentionRule::ForwardBindings.operator();
        let mut kept = Vec::new();
        let mut peak = 0usize;
        for generation in 1..=100_000u64 {
            // Every generation is bound and then unbound below the floor,
            // which is the shape a repeatedly recreated name leaves.
            operator
                .push(
                    MetadataRowFamily::DirentryBinds,
                    bind_row(7, "hot.txt", generation),
                    ChangeSeq(200_000),
                )
                .expect("push");
            peak = peak.max(operator.held_rows());
            operator
                .push(
                    MetadataRowFamily::DirentryUnbinds,
                    unbind_row(7, "hot.txt", generation, generation + 1),
                    ChangeSeq(200_000),
                )
                .expect("push");
            peak = peak.max(operator.held_rows());
            if let Some((_, row)) = operator.close_group(ChangeSeq(200_000)).expect("close") {
                kept.push(row);
            }
        }
        assert_eq!(peak, 1, "the operator holds one bind row and no more");
        assert!(
            kept.is_empty(),
            "every generation was unbound below the floor, so every bind goes"
        );
    }

    #[test]
    fn a_second_unretired_bind_in_one_slot_is_refused() {
        let mut operator = RetentionRule::ForwardBindings.operator();
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
        operator.close_group(floor()).expect("close");

        // A second generation in that same slot with nothing retiring the
        // first is the state the drop rule may not act on.
        operator
            .push(
                MetadataRowFamily::DirentryBinds,
                bind_row(7, "b.txt", 12),
                floor(),
            )
            .expect("push");
        let error = operator
            .close_group(floor())
            .expect_err("a superseded bind with no unbind is refused");
        assert!(
            error.to_string().contains("seq `11`"),
            "the error must name the bind that was superseded, got: {error}"
        );
        assert!(error.to_string().contains("superseded at or below"));
    }
}
