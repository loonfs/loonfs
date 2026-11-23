//! The fork's net change over a range of its commits: the inodes that the
//! range's commits touched, what the fork changed in each of them, and the
//! layout of each content reference in the change, after the merge writes
//! the reference's pieces from the fork's WAL tail.

use super::memory::PlanningMemory;
use super::state::{InodeState, StateReader};
use crate::commit::wal_size::{MAX_MERGED_INODES, MIN_MERGE_OPERATION_BYTES};
use crate::error::{CoreError, MergeBound, Result};
use crate::heap_bytes::HeapBytes;
use crate::in_flight::STORE_WRITES_IN_FLIGHT;
use crate::metadata::MetadataView;
use crate::namespace::state::NamespaceReadState;
use crate::storage::content_location::ContentLocation;
use crate::storage::tail_content::{cut_layout, materialize_content_layout, ContentWrites};
use crate::wal::ProjectedWalTail;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{ForkRange, MergeRecord, MAX_WAL_OBJECT_BYTES};
use loonfs_types::{CommitSeq, ContentId, ContentLayout, ContentRef, InodeId, NamespaceId, PoolId};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem::size_of;

/// The most of the fork's commits that one read of a merge's range returns.
/// A page includes each commit's deltas, so it stays at tens of commits.
const MERGE_COMMITS_PAGE: usize = 64;

/// The most heap bytes of the fork's commits that one read of a merge's
/// range returns.
///
/// A read ends with the commit that takes the page to this bound or past
/// it, so a page is at most this bound plus one commit.
pub const MERGE_COMMITS_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// The estimated bytes of one inode in the set of inodes that a merge's
/// range touched.
///
/// The estimate is the inode ID, and as much again for the B-tree's node
/// headers and unfilled slots.
const TOUCHED_INODE_BYTES: usize = 2 * size_of::<InodeId>();

/// Which components of an inode's state the fork changed in the range.
pub(super) struct Changed {
    pub(super) binding: bool,
    pub(super) content: bool,
    pub(super) attributes: bool,
    pub(super) access: bool,
}

impl Changed {
    /// Compares the inode's state at the range's start, or `None` when the
    /// fork created it in the range, with its state at the end.
    fn between(before: Option<&InodeState>, after: &InodeState) -> Option<Self> {
        let changed = match before {
            None => Self {
                binding: true,
                content: true,
                attributes: true,
                access: true,
            },
            Some(before) => Self {
                binding: !before.same_binding(after),
                content: !before.same_content(after),
                attributes: !before.same_attributes(after),
                access: !before.same_access(after),
            },
        };
        (changed.binding || changed.content || changed.attributes || changed.access)
            .then_some(changed)
    }
}

/// One inode's change over the range.
pub(super) struct InodeChange {
    /// The inode's state at the range's end.
    pub(super) ours: InodeState,
    /// The inode's state at the range's start, or `None` when the fork
    /// created it in the range.
    pub(super) before: Option<InodeState>,
    pub(super) changed: Changed,
}

/// Reads `inode_id`'s state at the range's end from `ours` and at its start
/// from `before`, and returns the inode's change, or `None` when the fork
/// changed none of its components. `fork` is the fork, whose commits in the
/// range touched the inode.
pub(super) async fn inode_change<S: ObjectStore + ?Sized>(
    ours: &mut StateReader<'_, '_, S>,
    before: &mut StateReader<'_, '_, S>,
    fork: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<InodeChange>> {
    let state = ours.state(inode_id).await?.ok_or_else(|| {
        CoreError::NamespaceCorrupt(format!(
            "a commit of fork `{fork}` touched inode `{inode_id}`, which the fork doesn't have"
        ))
    })?;
    let start = before.state(inode_id).await?;
    Ok(
        Changed::between(start.as_ref(), &state).map(|changed| InodeChange {
            ours: state,
            before: start,
            changed,
        }),
    )
}

/// What a merge reads from the fork's commits in its range: the inodes that
/// the commits touched, and the ranges of other forks that the replay
/// commits among them cover.
pub(super) struct RangeCommits {
    pub(super) touched: BTreeSet<InodeId>,
    /// One range for each fork, from the smallest `source_after_seq` to the
    /// largest `source_through_seq` of the replay commits' ranges of that
    /// fork, in the order in which the commits first refer to each fork.
    pub(super) covered_ranges: Vec<ForkRange>,
}

/// Lists the inodes that the fork's commits after `source_after_seq` touched,
/// through the sequence that `view` reads at, and the ranges that the replay
/// commits among them cover. The fork is the namespace whose head is
/// `fork_head`, and a replay commit is a commit whose merge record's source
/// is the fork that a rebase made the fork from, so a fork that no rebase
/// made covers no range.
///
/// [`MAX_MERGED_INODES`] bounds the set. A range that touched more inodes
/// than one WAL object could contain operations for fails with
/// `merge_too_large`, even if some of those inodes drop out of the net
/// change. The error's `split_at_seq` is the last commit before the one that
/// crossed the bound, so the caller can merge the range through that commit,
/// unless the first commit of the range crossed it.
///
/// After each page of commits, the function reports the set and the page to
/// `memory`, which checks the merge allowance. When the set and the page
/// exceed the allowance, the error's `split_at_seq` is the last commit
/// before the page, unless the first page crossed the allowance.
pub(super) async fn range_commits<S: ObjectStore + ?Sized>(
    view: MetadataView<'_, '_, S>,
    fork_head: &NamespaceReadState,
    source_after_seq: CommitSeq,
    memory: &PlanningMemory,
) -> Result<RangeCommits> {
    let rebased_from = fork_head
        .fork_basis
        .as_ref()
        .and_then(|basis| basis.rebased_from.as_ref())
        .map(|rebased_from| &rebased_from.source_namespace_id);
    let mut commits = RangeCommits {
        touched: BTreeSet::new(),
        covered_ranges: Vec::new(),
    };
    let mut after_seq = source_after_seq;
    // Each pass reads the next page of the range's commits, and the view
    // ends at the range's end, so the loop ends when a page is empty.
    loop {
        let page = view
            .commits_after(after_seq, MERGE_COMMITS_PAGE, MERGE_COMMITS_PAGE_BYTES)
            .await?;
        if page.is_empty() {
            memory.release_page();
            return Ok(commits);
        }
        let page_after_seq = after_seq;
        for commit in &page {
            let replayed = commit
                .merge
                .as_ref()
                .filter(|merge| Some(&merge.source_namespace_id) == rebased_from);
            for range in replayed.into_iter().flat_map(MergeRecord::ranges) {
                let covered = &mut commits.covered_ranges;
                match covered
                    .iter_mut()
                    .find(|covered| covered.source_namespace_id == range.source_namespace_id)
                {
                    Some(covered) => {
                        covered.source_after_seq =
                            covered.source_after_seq.min(range.source_after_seq);
                        covered.source_through_seq =
                            covered.source_through_seq.max(range.source_through_seq);
                    }
                    None => covered.push(range),
                }
            }
            let touched = &mut commits.touched;
            touched.extend(crate::diff::touched_inodes(&commit.deltas));
            if touched.len() > MAX_MERGED_INODES {
                return Err(CoreError::MergeTooLarge {
                    bound: MergeBound::TouchedInodes,
                    estimated_bytes: touched.len().saturating_mul(MIN_MERGE_OPERATION_BYTES),
                    max_bytes: MAX_WAL_OBJECT_BYTES,
                    split_at_seq: (after_seq > source_after_seq).then_some(after_seq),
                });
            }
            after_seq = commit.committed_seq;
        }
        let touched_bytes = commits.touched.len().saturating_mul(TOUCHED_INODE_BYTES);
        match memory.check_commits(touched_bytes, page.heap_bytes()) {
            Err(CoreError::MergeTooLarge {
                bound: MergeBound::MergeAllowance,
                estimated_bytes,
                max_bytes,
                ..
            }) => {
                return Err(CoreError::MergeTooLarge {
                    bound: MergeBound::MergeAllowance,
                    estimated_bytes,
                    max_bytes,
                    split_at_seq: (page_after_seq > source_after_seq).then_some(page_after_seq),
                });
            }
            result => result?,
        }
    }
}

/// Returns the number of extents in the layout that [`fork_layouts`] returns
/// for `content_ref` in `view`, a view of the fork whose WAL tail is `tail`,
/// without writing anything.
///
/// The layout lists the extents that the chain's layout row records for the
/// bytes before the tail's pieces, and then one extent for the pieces, or for
/// an empty chain in the tail. The namespace that the merge commits into
/// didn't create the chain, so it never combines that extent with the others
/// (format spec, "Publishing a manifest"). `pool_id` is the pool of both
/// namespaces.
pub(super) async fn fork_layout_extents<S: ObjectStore + ?Sized>(
    view: MetadataView<'_, '_, S>,
    tail: &ProjectedWalTail,
    pool_id: &PoolId,
    content_ref: &ContentRef,
) -> Result<usize> {
    let content = tail.content(content_ref);
    let location = ContentLocation::resolve(&view, pool_id, content.as_ref(), content_ref).await?;
    let pending = location.has_pieces() || (content_ref.size_bytes == 0 && location.is_resident());
    Ok(location.extents.len() + usize::from(pending))
}

/// Returns the layout of each of `content_refs` in `view`, a view of the fork
/// whose WAL tail is `tail`, after writing the pieces in `tail` that each
/// reference's bytes need to content objects through `content_writes`, which
/// writes under the pool's prefix with the content-write permits of the
/// namespace that the merge commits into (format spec, "Cross-namespace
/// copies and moves").
///
/// The pieces of each chain are written once, as the chain's next extent, for
/// the chain's longest reference, and a shorter reference to the same chain
/// gets that layout cut to its size. Each layout records the chain's creator
/// as the fork's rows record it, which is the fork for every chain that has
/// pieces in the tail. The writes run [`STORE_WRITES_IN_FLIGHT`] at a time.
pub(super) async fn fork_layouts<S: ObjectStore + ?Sized>(
    content_writes: ContentWrites<'_, S>,
    view: MetadataView<'_, '_, S>,
    tail: &ProjectedWalTail,
    content_refs: &HashSet<&ContentRef>,
) -> Result<Vec<(ContentRef, ContentLayout)>> {
    let mut longest: HashMap<&ContentId, &ContentRef> = HashMap::new();
    for content_ref in content_refs {
        let chain = longest
            .entry(&content_ref.content_id)
            .or_insert(content_ref);
        if content_ref.size_bytes > chain.size_bytes {
            *chain = content_ref;
        }
    }
    let longest: Vec<ContentRef> = longest.into_values().cloned().collect();
    let chains: HashMap<ContentId, ContentLayout> = stream::iter(longest)
        .map(|content_ref| async move {
            let content = tail.content(&content_ref);
            let layout =
                materialize_content_layout(content_writes, &view, content.as_ref(), &content_ref)
                    .await?;
            Ok::<_, CoreError>((content_ref.content_id, layout))
        })
        .buffer_unordered(STORE_WRITES_IN_FLIGHT)
        .try_collect()
        .await?;
    Ok(content_refs
        .iter()
        .map(|content_ref| {
            let mut layout = chains[&content_ref.content_id].clone();
            cut_layout(&mut layout, content_ref.size_bytes);
            ((*content_ref).clone(), layout)
        })
        .collect())
}
