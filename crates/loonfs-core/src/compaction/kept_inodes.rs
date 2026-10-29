//! The kept inode filter, which tells a base compaction which inodes keep
//! their rows.
//!
//! A deletion has expired in a compaction's basis when its latest tombstone
//! event is a `set` and its deletion sequence is at or below the basis
//! manifest's acknowledged floor. Its tombstone hides its subtree at every
//! sequence that a compaction preserves, and no undelete of it can be
//! committed, so a base compaction of a group that stores rows by inode
//! removes every row of the inodes that the deletion hides
//! ([format section 10.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#103-row-retention-during-a-base-compaction)).
//! The filter contains every inode that's visible at the basis's head or
//! under a recoverable deletion. A walk of the basis's directory tree finds
//! them.

use super::plan::{group_run_descriptors, MetadataCompactionSpec};
use super::step::{CompactionUnitOutcome, MetadataCompactionCancellation};
use crate::bloom_filter::BloomFilter;
use crate::error::{CoreError, Result};
use crate::limits::{DIRECTORY_PAGE_RAW_SCAN_LIMIT, MAX_KEPT_INODE_WALK_DEPTH};
use crate::metadata::row_decode::{
    active_deletion_from_manifest_row, direntry_binding_from_manifest_row,
};
use crate::metadata::{ActiveDeletionRecord, DirentryBindingRecord};
use crate::segment::runs::MetadataFamilyGroup;
use crate::segment::scan::VerifiedMetadataSegments;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{
    lookup_keys, ActiveDeletionRowAction, MetadataRow, MetadataRowFamily,
};
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, ROOT_INODE_ID};
use std::collections::VecDeque;
use std::ops::ControlFlow;

/// Rows one scan of the walk reads, from a directory's slot index or from
/// `active_deletions`.
const KEPT_INODE_SCAN_ROWS: usize = DIRECTORY_PAGE_RAW_SCAN_LIMIT;

/// The inodes whose rows a base merge keeps.
pub(crate) enum KeptInodeFilter {
    /// The merge keeps every row, because it isn't a base merge of a group
    /// that stores rows by inode, because the basis manifest's acknowledged
    /// floor is 0, or because the walk would exceed its depth limit.
    EveryInode,
    /// The inodes that the walk reached. The filter can also match an inode
    /// that the walk didn't reach, which keeps that inode's rows for one
    /// merge.
    Reached(BloomFilter),
}

impl KeptInodeFilter {
    /// Whether the merge keeps `row` and judges it under its family's
    /// retention rule, rather than removing it because an expired deletion
    /// hides its inode.
    pub(crate) fn keeps(&self, row: &MetadataRow) -> bool {
        let Self::Reached(filter) = self else {
            return true;
        };
        let inode_id = match row {
            MetadataRow::Inode(record) => record.inode_id,
            MetadataRow::FileRevision(record) => record.inode_id,
            MetadataRow::AttributesRevision(record) => record.inode_id,
            MetadataRow::AccessRevision(record) => record.inode_id,
            // A hidden directory's edges are hidden with it. Both binding
            // indexes test an edge by its parent, so they keep or remove each
            // binding event together.
            MetadataRow::DirentryBinding(record) => record.parent_inode_id,
            MetadataRow::Tombstone(_)
            | MetadataRow::ActiveDeletion(_)
            | MetadataRow::CommitReceipt(_)
            | MetadataRow::Commit(_)
            | MetadataRow::ContentLayout(_) => return true,
        };
        filter.may_contain(&filter_key(inode_id))
    }
}

fn filter_key(inode_id: InodeId) -> [u8; 8] {
    inode_id.0.to_le_bytes()
}

/// Whether a base merge of `group` removes the rows of hidden inodes.
fn removes_hidden_rows(group: MetadataFamilyGroup) -> bool {
    match group {
        MetadataFamilyGroup::Inodes
        | MetadataFamilyGroup::Revisions
        | MetadataFamilyGroup::Attributes
        | MetadataFamilyGroup::Access
        | MetadataFamilyGroup::Bindings => true,
        MetadataFamilyGroup::Tombstones
        | MetadataFamilyGroup::ActiveDeletions
        | MetadataFamilyGroup::Commits
        | MetadataFamilyGroup::ContentLayouts => false,
    }
}

/// Builds the kept inode filter for the merge that `spec` plans over `basis`,
/// or returns the outcome that stopped the walk.
///
/// A basis whose manifest has an acknowledged floor of 0 keeps every row
/// without building the filter. Every deletion's sequence is at least 1, so
/// no deletion in that basis is at or below the acknowledged floor, and the
/// acknowledged floor never decreases, so no earlier compaction treated a
/// deletion in it as expired either. Any other basis builds the filter,
/// because a base compaction of `active_deletions` removes an expired
/// deletion's `listed` row on its own schedule, so the other groups can't
/// tell from that family whether they still store rows that the deletion
/// hides.
///
/// The filter uses ten bits per expected inode, capped at
/// `MAX_BLOOM_FILTER_BYTES`. Its seed is the output run number. A walk
/// deeper than `MAX_KEPT_INODE_WALK_DEPTH` keeps every row.
pub(crate) async fn load_kept_inode_filter<S: ObjectStore + ?Sized>(
    basis: &VerifiedMetadataSegments<'_, S>,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    cancellation: &MetadataCompactionCancellation,
) -> Result<std::result::Result<KeptInodeFilter, CompactionUnitOutcome>> {
    let group = spec.group();
    if !spec.placement().may_drop_rows_below_the_retention_floor() || !removes_hidden_rows(group) {
        return Ok(Ok(KeptInodeFilter::EveryInode));
    }
    let manifest = basis.manifest().payload();
    let acknowledged_floor_seq = manifest.acknowledged_floor_seq;
    if acknowledged_floor_seq == ChangeSeq(0) {
        return Ok(Ok(KeptInodeFilter::EveryInode));
    }
    let expected_inodes = basis
        .scan_runs
        .iter()
        .flat_map(|run| group_run_descriptors(run, MetadataFamilyGroup::Inodes))
        .fold(0_u64, |count, segment| {
            count.saturating_add(segment.row_count)
        });
    let filter = BloomFilter::new(expected_inodes, manifest.next_run_no.0);
    let mut walk = KeptInodeWalk {
        segments: basis.with_empty_block_memo(),
        filter,
        acknowledged_floor_seq,
        cancellation,
    };
    match walk.reach_kept_inodes().await? {
        ControlFlow::Continue(()) => Ok(Ok(KeptInodeFilter::Reached(walk.filter))),
        ControlFlow::Break(WalkStop::Cancelled) => Ok(Err(CompactionUnitOutcome::Cancelled)),
        ControlFlow::Break(WalkStop::TooDeep) => {
            tracing::info!(namespace_id = %namespace_id, max_depth = MAX_KEPT_INODE_WALK_DEPTH,
                "kept inode walk exceeded its depth limit; kept every row");
            Ok(Ok(KeptInodeFilter::EveryInode))
        }
    }
}

/// Why a walk stopped before it reached every inode it keeps.
enum WalkStop {
    Cancelled,
    /// The walk would keep more than `MAX_KEPT_INODE_WALK_DEPTH` directories.
    TooDeep,
}

/// One directory on the walk's stack.
struct WalkFrame {
    directory: InodeId,
    /// Where the next scan of the directory's slot index starts, or `None`
    /// once a scan reached the directory's last row.
    resume_at: Option<String>,
}

impl WalkFrame {
    fn new(directory: InodeId) -> Self {
        Self {
            directory,
            resume_at: Some(lookup_keys::direntry_parent_prefix(directory)),
        }
    }
}

/// Reads the deletions in a range of the basis's `active_deletions` that
/// have a `listed` row and no `removed` row, a page at a time.
struct ListedDeletions {
    lower_bound: String,
    /// `None` reads to the end of the family.
    upper_bound: Option<String>,
    /// The rows of the last page that the reader hasn't returned yet.
    unread: VecDeque<ActiveDeletionRecord>,
    /// Whether the last page reached the end of the range.
    scanned_to_end: bool,
    /// The last removal read. A removal sorts just before the listed row it
    /// cancels, so a listed row can only match the last removal read.
    removed: Option<(ChangeSeq, InodeId)>,
}

impl ListedDeletions {
    /// The deletions above `after_seq`.
    fn above(after_seq: ChangeSeq) -> Self {
        Self {
            lower_bound: lookup_keys::active_deletion_key_after_seq(after_seq),
            upper_bound: string_prefix_upper_bound(lookup_keys::ACTIVE_DELETION_ROW_PREFIX),
            unread: VecDeque::new(),
            scanned_to_end: false,
            removed: None,
        }
    }

    /// Returns the next deletion's root and kind, or `None` at the end of the
    /// range. Checks `cancellation` before it reads each page.
    async fn next<S: ObjectStore + ?Sized>(
        &mut self,
        segments: &VerifiedMetadataSegments<'_, S>,
        cancellation: &MetadataCompactionCancellation,
    ) -> Result<ControlFlow<WalkStop, Option<(InodeId, InodeKind)>>> {
        loop {
            while let Some(deletion) = self.unread.pop_front() {
                let identity = (deletion.deletion_seq, deletion.root_inode_id);
                match deletion.action {
                    ActiveDeletionRowAction::Removed { .. } => self.removed = Some(identity),
                    ActiveDeletionRowAction::Listed { inode_kind, .. }
                        if self.removed != Some(identity) =>
                    {
                        return Ok(ControlFlow::Continue(Some((
                            deletion.root_inode_id,
                            inode_kind,
                        ))));
                    }
                    ActiveDeletionRowAction::Listed { .. } => {}
                }
            }
            if self.scanned_to_end {
                return Ok(ControlFlow::Continue(None));
            }
            if cancellation.is_cancelled() {
                return Ok(ControlFlow::Break(WalkStop::Cancelled));
            }
            let rows = segments
                .scan_range_page_with_keys(
                    MetadataRowFamily::ActiveDeletions,
                    &self.lower_bound,
                    self.upper_bound.as_deref(),
                    KEPT_INODE_SCAN_ROWS,
                )
                .await?;
            self.scanned_to_end = rows.len() < KEPT_INODE_SCAN_ROWS;
            if let Some((row_key, _)) = rows.last() {
                self.lower_bound = lookup_keys::after_row_key(row_key);
            }
            for (_, row) in rows {
                self.unread
                    .push_back(active_deletion_from_manifest_row(row)?);
            }
        }
    }
}

struct KeptInodeWalk<'a, 'c, S: ObjectStore + ?Sized> {
    /// The basis, with a block memo that the walk drops before the merge
    /// reads its input.
    segments: VerifiedMetadataSegments<'a, S>,
    filter: BloomFilter,
    /// The basis manifest's acknowledged floor. A deletion at or below it
    /// that no undelete cancelled has expired.
    acknowledged_floor_seq: ChangeSeq,
    cancellation: &'c MetadataCompactionCancellation,
}

impl<S: ObjectStore + ?Sized> KeptInodeWalk<'_, '_, S> {
    /// Inserts every inode that's visible at the basis's head, and then every
    /// inode under a recoverable deletion.
    ///
    /// Deleting an item unbinds it, so the walk from the root inode doesn't
    /// reach a deleted subtree. An expired deletion isn't a starting point,
    /// and its root is unbound, so no walk reaches the inodes it hides. An
    /// inner deletion is older than the deletion around it, so no recoverable
    /// deletion is inside an expired one, and the two walks reach every
    /// ancestor through which the rights on an item under a recoverable
    /// deletion are evaluated.
    async fn reach_kept_inodes(&mut self) -> Result<ControlFlow<WalkStop>> {
        if let ControlFlow::Break(stop) = self.reach(ROOT_INODE_ID, InodeKind::Directory).await? {
            return Ok(ControlFlow::Break(stop));
        }
        self.reach_recoverable_deletions().await
    }

    /// Reaches every deletion above the acknowledged floor that has a
    /// `listed` row and no `removed` row in the basis's `active_deletions`.
    async fn reach_recoverable_deletions(&mut self) -> Result<ControlFlow<WalkStop>> {
        let mut recoverable = ListedDeletions::above(self.acknowledged_floor_seq);
        loop {
            let (root_inode_id, inode_kind) =
                match recoverable.next(&self.segments, self.cancellation).await? {
                    ControlFlow::Break(stop) => return Ok(ControlFlow::Break(stop)),
                    ControlFlow::Continue(None) => return Ok(ControlFlow::Continue(())),
                    ControlFlow::Continue(Some(deletion)) => deletion,
                };
            if let ControlFlow::Break(stop) = self.reach(root_inode_id, inode_kind).await? {
                return Ok(ControlFlow::Break(stop));
            }
        }
    }

    /// Inserts `inode_id` unless the filter already matches it, because
    /// inserting a key that the filter matches only grows the filter.
    ///
    /// The filter is sized for the basis's `inodes` rows, and every inode the
    /// walk reaches has such a row, so a filter that refuses the insert has
    /// found a basis with more reachable inodes than inode rows, which is
    /// corruption.
    fn insert(&mut self, inode_id: InodeId) -> Result<()> {
        let key = filter_key(inode_id);
        if self.filter.may_contain(&key) || self.filter.insert(&key).is_some() {
            return Ok(());
        }
        Err(CoreError::NamespaceCorrupt(format!(
            "the kept inode walk reached inode {inode_id} beyond the basis's `inodes` row count"
        )))
    }

    /// Inserts `inode_id`, and every inode below it when it's a directory.
    async fn reach(
        &mut self,
        inode_id: InodeId,
        inode_kind: InodeKind,
    ) -> Result<ControlFlow<WalkStop>> {
        self.insert(inode_id)?;
        if inode_kind != InodeKind::Directory {
            return Ok(ControlFlow::Continue(()));
        }
        self.reach_descendants(inode_id).await
    }

    /// Inserts every inode below `directory`, depth first, with one frame for
    /// each directory from `directory` down to the one the walk is reading,
    /// the way a recursive listing walks a tree.
    ///
    /// The walk reads a directory's slot index in key order, one edge (a name
    /// and a child) at a time. An edge's rows run oldest first, so its last
    /// row is its state at the basis's head, and the walk reaches the child of
    /// an edge whose last row is bound. Before the walk descends into a child
    /// directory, it drops the rows it scanned past that child's edge and
    /// scans them again when it returns, so only the directory it's reading
    /// keeps a page of rows. A directory whose last edge leads to the child
    /// leaves the stack before the child joins it.
    async fn reach_descendants(&mut self, directory: InodeId) -> Result<ControlFlow<WalkStop>> {
        let mut stack = vec![WalkFrame::new(directory)];
        // The scanned rows of the directory on top of the stack that the walk
        // hasn't read yet.
        let mut unread: VecDeque<(String, DirentryBindingRecord)> = VecDeque::new();
        // The last row read of the edge the walk is reading, with its row key.
        let mut edge: Option<(String, DirentryBindingRecord)> = None;
        loop {
            if self.cancellation.is_cancelled() {
                return Ok(ControlFlow::Break(WalkStop::Cancelled));
            }
            let Some(frame) = stack.last_mut() else {
                return Ok(ControlFlow::Continue(()));
            };
            if unread.is_empty() {
                if let Some(lower_bound) = frame.resume_at.take() {
                    let (rows, resume_at) =
                        self.scan_slot_index(frame.directory, &lower_bound).await?;
                    frame.resume_at = resume_at;
                    unread = rows;
                    continue;
                }
            }
            let next = unread.pop_front();
            if let (Some((_, last)), Some((_, row))) = (&edge, &next) {
                if last.name_key == row.name_key && last.child_inode_id == row.child_inode_id {
                    edge = next;
                    continue;
                }
            }
            let ended = std::mem::replace(&mut edge, next);
            let directory_read = edge.is_none();
            if directory_read {
                stack.pop();
            }
            let Some((row_key, last)) = ended else {
                continue;
            };
            if !last.is_bound() {
                continue;
            }
            self.insert(last.child_inode_id)?;
            if last.child_kind != InodeKind::Directory {
                continue;
            }
            if !directory_read {
                let parent = stack
                    .last_mut()
                    .expect("a directory with rows left should be on the stack");
                parent.resume_at = Some(lookup_keys::after_row_key(&row_key));
                unread.clear();
                edge = None;
            }
            if stack.len() == MAX_KEPT_INODE_WALK_DEPTH {
                return Ok(ControlFlow::Break(WalkStop::TooDeep));
            }
            stack.push(WalkFrame::new(last.child_inode_id));
        }
    }

    /// Scans up to a page of `directory`'s slot-index rows from
    /// `lower_bound`, and returns them with the bound that the next scan
    /// starts at, which is `None` when the page reached the directory's last
    /// row.
    async fn scan_slot_index(
        &self,
        directory: InodeId,
        lower_bound: &str,
    ) -> Result<(VecDeque<(String, DirentryBindingRecord)>, Option<String>)> {
        let upper_bound =
            string_prefix_upper_bound(&lookup_keys::direntry_parent_prefix(directory));
        let rows = self
            .segments
            .scan_range_page_with_keys(
                MetadataRowFamily::DirentryBinds,
                lower_bound,
                upper_bound.as_deref(),
                KEPT_INODE_SCAN_ROWS,
            )
            .await?;
        let resume_at = rows
            .last()
            .filter(|_| rows.len() == KEPT_INODE_SCAN_ROWS)
            .map(|(row_key, _)| lookup_keys::after_row_key(row_key));
        let rows = rows
            .into_iter()
            .map(|(row_key, row)| Ok((row_key, direntry_binding_from_manifest_row(row)?)))
            .collect::<Result<_>>()?;
        Ok((rows, resume_at))
    }
}
