//! The partition grammar a partial fold walks a family group by.
//!
//! A whole-group fold reads every row of the group at once. A group too
//! large for that is folded a slice at a time, and the slice boundary must
//! be a place where the drop rules stay local: two rows where one cancels
//! the other have to land on the same side of it. Each family group
//! therefore names one **partition key** that every family in the group
//! prefixes its row keys by, and the fold advances in whole partitions.
//!
//! The partition key per group is read off the row-key grammar in
//! `MetadataRow::row_key_for_family`, family by family:
//!
//! | group | partition | per-family key prefix |
//! | --- | --- | --- |
//! | binds, child binds, unbinds | inode id | `direntry-{parent}`, `direntry-child-{child}`, `direntry-unbind-{parent}` |
//! | revisions, revisions by inode | inode id | `revision-{inode}`, `revision-by-inode-desc-{inode}` |
//! | inodes | inode id | `inode-{inode}` |
//! | tombstones | root inode id | `tombstone-{root}` |
//! | active deletions | deletion sequence | `active-deletion-{deleted_at_seq}` |
//! | commit receipts | receipt id | `commit-receipt-{commit_id_hex}` |
//! | attributes | inode id | `attributes-{inode}` |
//!
//! Two entries need their own sentence. The bindings group partitions by
//! **parent** inode for the two forward families, because an unbind cancels
//! the bind under the same parent and name; the reverse index is keyed by
//! **child**, so it shares the group's partition space without sharing a
//! partition with the rows it indexes, and the fold decides its drops with a
//! point read into the snapshot's unbind family rather than from the slice
//! (see [`super::partial_fold`]). Active deletions partition
//! by **deletion sequence**, the family's leading key component — not by
//! root inode, which sits second — and a removal marker repeats its target's
//! sequence, so the cancelling pair still shares a partition.
//!
//! One partition can still be larger than one step may read: a directory has
//! no size limit, and neither has an inode's revision history. Such a
//! partition is folded in pieces, and [`PartitionOffset`] is the position
//! inside it.

use loonfs_api::wire::manifest::{MetadataRow, MetadataTableFamily};

/// How wide a numeric partition component is written in a row key. Every
/// numeric component in the grammar is a `u64` zero-padded to this width, so
/// row-key order is value order and a boundary is one padded number.
const NUMERIC_PARTITION_WIDTH: usize = 20;

/// The cursor component that sorts above every partition of a group.
/// Decimal digits and lowercase hex digits all sort below it, so a bound
/// built from it is above every row of every family in the group.
const PAST_LAST_PARTITION: &str = "~";

/// One partition of a family group's keyspace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum PartitionKey {
    /// A `u64` component: an inode id, a parent inode id, a child inode id,
    /// a root inode id, or a deletion sequence, depending on the group and
    /// the family.
    Number(u64),
    /// A receipt id in the hex spelling its row key carries. Commit ids are
    /// variable length, so this component is too.
    ReceiptId(String),
}

/// How far a partial fold has got: the next partition it has not processed,
/// or the end of the group's keyspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PartitionCursor {
    /// Every partition from this one upward is unprocessed.
    At(PartitionKey),
    /// Every partition is processed.
    End,
}

/// How far into one partition a fold has got, when that partition alone is
/// larger than one step may read.
///
/// A fold normally advances in whole partitions. One partition too large for
/// that is folded in pieces instead, one family at a time in the group's
/// declared order, and this is the position inside it: the family the fold is
/// working through and the last row key of that family it has written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PartitionOffset {
    pub(super) family: MetadataTableFamily,
    /// The last row key written. The next piece resumes strictly after it,
    /// which is why callers read it through [`Self::resume_bound`].
    pub(super) written_through: String,
}

impl PartitionOffset {
    /// The least row key the next piece may read: row keys are globally
    /// unique, so resuming strictly past the last one written skips exactly
    /// that row.
    pub(super) fn resume_bound(&self) -> String {
        format!("{}\0", self.written_through)
    }
}

/// What a group's partition keys are made of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionSpace {
    /// Partitions are `u64` values written zero-padded to twenty digits.
    Number,
    /// Partitions are hex-encoded receipt ids of any length.
    ReceiptId,
}

/// The partition grammar of one family group.
#[derive(Debug, Clone, Copy)]
pub(super) struct GroupPartitioning {
    /// The family whose row-key prefix spells a cursor. Any family in the
    /// group would do; the first one is chosen so the spelling is stable.
    cursor_family: MetadataTableFamily,
    space: PartitionSpace,
}

impl GroupPartitioning {
    /// The partition grammar of `group`, or `None` when the group's families
    /// do not share one partition space and no single cursor could bound
    /// them all.
    pub(super) fn for_group(group: &[MetadataTableFamily]) -> Option<Self> {
        let cursor_family = *group.first()?;
        let space = partition_space_of(cursor_family);
        if group
            .iter()
            .any(|family| partition_space_of(*family) != space)
        {
            return None;
        }
        Some(Self {
            cursor_family,
            space,
        })
    }

    /// The cursor a fold starts at: the group's first possible partition.
    pub(super) fn first_cursor(self) -> PartitionCursor {
        match self.space {
            PartitionSpace::Number => PartitionCursor::At(PartitionKey::Number(0)),
            // The empty hex string sorts below every receipt id.
            PartitionSpace::ReceiptId => {
                PartitionCursor::At(PartitionKey::ReceiptId(String::new()))
            }
        }
    }

    /// The cursor a fold stands at once it has processed `partition`.
    ///
    /// The result is the smallest boundary above `partition`: no partition
    /// of the group sorts between the two, so nothing is skipped.
    pub(super) fn cursor_after(self, partition: &PartitionKey) -> PartitionCursor {
        match partition {
            PartitionKey::Number(value) => match value.checked_add(1) {
                Some(next) => PartitionCursor::At(PartitionKey::Number(next)),
                // Nothing sorts above the last representable partition.
                None => PartitionCursor::End,
            },
            // Every hex digit sorts at or above `0`, so appending one lands
            // at or below every receipt id that extends this one, and below
            // every receipt id that diverges from it upward.
            PartitionKey::ReceiptId(hex) => {
                PartitionCursor::At(PartitionKey::ReceiptId(format!("{hex}0")))
            }
        }
    }

    /// The durable spelling of `cursor`: the cursor family's row-key prefix
    /// followed by the partition component.
    pub(super) fn spell_cursor(self, cursor: &PartitionCursor) -> String {
        self.bound_for(self.cursor_family, cursor)
    }

    /// Reads back a cursor spelled by [`Self::spell_cursor`].
    ///
    /// The loader runs this over every manifest that carries a partial fold:
    /// a cursor that does not parse is a fold with no position, and a fold
    /// resumed from one would rewrite the wrong slice.
    pub(super) fn parse_cursor(self, spelled: &str) -> Result<PartitionCursor, String> {
        let prefix = self.cursor_family.row_key_prefix();
        let component = spelled.strip_prefix(prefix).ok_or_else(|| {
            format!("`{spelled}` does not start with this group's cursor prefix `{prefix}`")
        })?;
        if component == PAST_LAST_PARTITION {
            return Ok(PartitionCursor::End);
        }
        exact_partition_component(self.space, component)
            .map(PartitionCursor::At)
            .ok_or_else(|| format!("`{component}` is not a partition key of this group"))
    }

    /// Reads back an offset published beside a cursor.
    ///
    /// The loader runs this over every manifest that carries a partial fold
    /// standing inside a partition: the offset says which family the fold is
    /// working through and how far, so an offset that names no family of the
    /// group, or a row key of some other partition, is a position the fold
    /// cannot resume from.
    pub(super) fn parse_partition_offset(
        self,
        group: &[MetadataTableFamily],
        cursor: &PartitionCursor,
        spelled: &str,
    ) -> Result<PartitionOffset, String> {
        let PartitionCursor::At(partition) = cursor else {
            return Err(
                "a fold that has passed every partition cannot stand inside one".to_owned(),
            );
        };
        let family = self
            .family_of_row_key(group, spelled)
            .ok_or_else(|| format!("`{spelled}` is not a row key of any family in this group"))?;
        let offset_partition = self
            .partition_of_row_key(family, spelled)
            .ok_or_else(|| format!("`{spelled}` carries no partition key"))?;
        if offset_partition != *partition {
            return Err(format!(
                "`{spelled}` lies in partition {offset_partition:?}, but the cursor stands at \
                 {partition:?}"
            ));
        }
        Ok(PartitionOffset {
            family,
            written_through: spelled.to_owned(),
        })
    }

    /// The family of `group` whose row keys `row_key` is spelled in.
    ///
    /// The longest matching prefix wins: within the bindings group
    /// `direntry-child-` and `direntry-unbind-` both extend `direntry-`, so a
    /// shorter match would claim rows of the wrong family.
    pub(super) fn family_of_row_key(
        self,
        group: &[MetadataTableFamily],
        row_key: &str,
    ) -> Option<MetadataTableFamily> {
        group
            .iter()
            .filter(|family| row_key.starts_with(family.row_key_prefix()))
            .max_by_key(|family| family.row_key_prefix().len())
            .copied()
    }

    /// The least row key of `family` that `cursor` admits: every row of the
    /// family in the cursor's partition or above sorts at or above this, and
    /// every row below it sorts below.
    pub(super) fn family_lower_bound(
        self,
        family: MetadataTableFamily,
        cursor: &PartitionCursor,
    ) -> String {
        self.bound_for(family, cursor)
    }

    /// The bound one past every row of `family` in `partition`: the lower
    /// bound of the next partition.
    pub(super) fn family_partition_end(
        self,
        family: MetadataTableFamily,
        partition: &PartitionKey,
    ) -> String {
        self.bound_for(family, &self.cursor_after(partition))
    }

    fn bound_for(self, family: MetadataTableFamily, cursor: &PartitionCursor) -> String {
        let prefix = family.row_key_prefix();
        match cursor {
            PartitionCursor::At(PartitionKey::Number(value)) => {
                format!("{prefix}{value:0width$}", width = NUMERIC_PARTITION_WIDTH)
            }
            PartitionCursor::At(PartitionKey::ReceiptId(hex)) => format!("{prefix}{hex}"),
            PartitionCursor::End => format!("{prefix}{PAST_LAST_PARTITION}"),
        }
    }

    /// The partition a row of `family` belongs to, or `None` when the row
    /// does not belong to `family` at all.
    pub(super) fn partition_of_row(
        self,
        family: MetadataTableFamily,
        row: &MetadataRow,
    ) -> Option<PartitionKey> {
        self.partition_of_row_key(family, &row.row_key_for_family(family))
    }

    /// The partition a stored row key of `family` belongs to. Slice planning
    /// reads this off the last key of each index entry, which is the only
    /// key-order information a segment publishes without being decoded.
    pub(super) fn partition_of_row_key(
        self,
        family: MetadataTableFamily,
        row_key: &str,
    ) -> Option<PartitionKey> {
        let component = row_key.strip_prefix(family.row_key_prefix())?;
        leading_partition_component(self.space, component)
    }
}

fn partition_space_of(family: MetadataTableFamily) -> PartitionSpace {
    match family {
        MetadataTableFamily::CommitReceipts => PartitionSpace::ReceiptId,
        MetadataTableFamily::Inodes
        | MetadataTableFamily::DirentryBinds
        | MetadataTableFamily::DirentryChildBinds
        | MetadataTableFamily::DirentryUnbinds
        | MetadataTableFamily::Revisions
        | MetadataTableFamily::RevisionsByInodeDesc
        | MetadataTableFamily::Tombstones
        | MetadataTableFamily::ActiveDeletions
        | MetadataTableFamily::Attributes => PartitionSpace::Number,
    }
}

/// Reads the partition component that opens a row key's variable part,
/// ignoring the rest of the row key behind it.
fn leading_partition_component(space: PartitionSpace, component: &str) -> Option<PartitionKey> {
    match space {
        PartitionSpace::Number => number_partition(component.get(..NUMERIC_PARTITION_WIDTH)?),
        // A receipt row key is `{hex}-{committed_seq}`, and hex carries no
        // `-`, so the first one ends the partition component.
        PartitionSpace::ReceiptId => {
            receipt_partition(component.split('-').next().unwrap_or(component))
        }
    }
}

/// Reads a partition component that is the whole of `component`, which is
/// what a cursor carries: a cursor with anything trailing is not a boundary.
fn exact_partition_component(space: PartitionSpace, component: &str) -> Option<PartitionKey> {
    match space {
        PartitionSpace::Number => {
            if component.len() != NUMERIC_PARTITION_WIDTH {
                return None;
            }
            number_partition(component)
        }
        PartitionSpace::ReceiptId => receipt_partition(component),
    }
}

fn number_partition(digits: &str) -> Option<PartitionKey> {
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u64>().ok().map(PartitionKey::Number)
}

fn receipt_partition(hex: &str) -> Option<PartitionKey> {
    if !hex
        .bytes()
        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    Some(PartitionKey::ReceiptId(hex.to_owned()))
}
