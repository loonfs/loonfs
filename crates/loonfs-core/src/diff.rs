//! The diff: the inodes whose visible state differs between two retained
//! sequences, found from the commits between them
//! ([API section 6.12](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#612-get-diff)).

use crate::changes::{ensure_event_index_allowed, CommitScan};
use crate::error::{CoreError, Result};
use crate::heap_bytes::HeapBytes;
use crate::limits::{CHANGE_FEED_PAGE_BYTES, DIFF_PAGE_TOUCHES};
use crate::metadata::{
    MetadataView, MetadataViewSession, MetadataVisibilityReads, ResolvedVisiblePath,
};
use crate::path::read::{resolve_visible_inode, LoadedMetadataView};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{
    semantic_operation_groups, WalCommitDelta, WalCommitPayload, WalDelta,
};
use loonfs_types::options::{CommitSplitting, DiffOptions};
use loonfs_types::{
    u64_from_usize, AbsolutePath, AccessRevisionNo, AttributesRevisionNo, ChangeFeedPosition,
    ChangeSeq, DiffEntry, DiffEntryKind, DiffResponse, InodeId, InodeKind, NameKey, NamespaceId,
    RevisionNo,
};
use std::collections::{BTreeSet, HashMap};

/// One page of a diff, with counts of the work that built it, which the
/// runtime reports as metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffPage {
    /// The page.
    pub response: DiffResponse,
    /// The commits that the page read, including a commit that it left for
    /// the next page.
    pub commits_scanned: u64,
    /// The inodes that those commits touched, counted once for each commit.
    pub touches_examined: u64,
    /// The paths that the page resolved, at most one for each inode it
    /// classified at each end of the diff.
    pub path_resolutions: u64,
}

/// Reads one page of the diff from `from_seq` to `options.to_seq` under
/// `options.path`, starting after `options.after_seq`.
///
/// `to_seq` defaults to the head of `view`, and `after_seq` to `from_seq`.
/// The page checks `from_seq` against the retention floor of `view` before
/// it reads anything, so a page whose basis has a floor above `from_seq`
/// returns `rebootstrap_required`.
pub(crate) async fn diff_page<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
    from_seq: ChangeSeq,
    options: &DiffOptions,
) -> Result<DiffPage> {
    diff_page_within(view, from_seq, options, DIFF_PAGE_TOUCHES).await
}

/// Reads one page as [`diff_page`] does, and ends the page after the commit
/// that brings the touches it has examined to `max_touches`. With
/// [`CommitSplitting::AtPageBudget`], a commit that alone touches more than
/// `max_touches` inodes is read across pages instead.
async fn diff_page_within<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
    from_seq: ChangeSeq,
    options: &DiffOptions,
    max_touches: usize,
) -> Result<DiffPage> {
    let DiffOptions {
        path,
        to_seq,
        after_seq,
        event_index,
        limit,
        commit_splitting,
        consistency: _,
    } = options;
    let (to_seq, after_seq, event_index, limit, splitting) =
        (*to_seq, *after_seq, *event_index, *limit, *commit_splitting);
    let head = view.head();
    crate::namespace::control::ensure_namespace_live(&head.namespace_id, head.status)?;
    let retention_floor_seq = view.retention_floor_seq();
    if from_seq < retention_floor_seq {
        return Err(CoreError::DiffRebootstrapRequired {
            from_seq,
            retention_floor_seq,
        });
    }
    let to_seq = to_seq.unwrap_or(head.seq);
    if to_seq > head.seq {
        return Err(out_of_range(
            "to_seq",
            format!("`{to_seq}` is ahead of namespace head `{}`", head.seq),
        ));
    }
    if from_seq > to_seq {
        return Err(out_of_range(
            "from_seq",
            format!("`{from_seq}` is after `to_seq` `{to_seq}`"),
        ));
    }
    let after_seq = after_seq.unwrap_or(from_seq);
    if !(from_seq..=to_seq).contains(&after_seq) {
        return Err(out_of_range(
            "after_seq",
            format!("`{after_seq}` is outside `from_seq` `{from_seq}` through `to_seq` `{to_seq}`"),
        ));
    }
    ensure_event_index_allowed(event_index, splitting)?;
    if event_index > 0 && after_seq == from_seq {
        return Err(CoreError::InvalidEventIndex(format!(
            "a position inside a commit resumes the commit at `after_seq`, which has to be after \
             `from_seq` `{from_seq}`"
        )));
    }

    let to_view = view.metadata_view_at(to_seq);
    let mut diff = Diff {
        namespace_id: &head.namespace_id,
        subtree: name_keys(path),
        from: End::new(view.metadata_view_at(from_seq)),
        to: End::new(to_view),
        touched: HashMap::new(),
        commits_scanned: 0,
        touches_examined: 0,
    };
    let mut scan = CommitScan::new(
        to_view,
        ChangeFeedPosition {
            after_seq,
            event_index,
        },
        limit,
        splitting,
        CHANGE_FEED_PAGE_BYTES,
    );
    let mut entries = Vec::new();
    while let Some(scanned) = scan.next().await? {
        let record = &scanned.record;
        let split = splitting == CommitSplitting::AtPageBudget
            && (scanned.first_event_index > 0
                || touched_inodes(&record.deltas).len() > max_touches);
        if split {
            // A commit that alone exceeds the touch bound starts a page of
            // its own, and the page reads its events until the bound.
            if !scan.is_empty() {
                scan.end_page();
                continue;
            }
            let part = diff
                .commit_part_entries(record, scanned.first_event_index, max_touches)
                .await?;
            let heap_bytes = part.entries.heap_bytes();
            match part.next_event_index {
                Some(next_event_index) => {
                    scan.admit_part(record.committed_seq, heap_bytes, next_event_index);
                }
                None => {
                    scan.admit(record.committed_seq, heap_bytes);
                }
            }
            entries.extend(part.entries);
        } else {
            let commit_entries = diff.commit_entries(record).await?;
            if !scan.admit(record.committed_seq, commit_entries.heap_bytes()) {
                continue;
            }
            entries.extend(commit_entries);
        }
        if diff.touches_examined >= u64_from_usize(max_touches) {
            scan.end_page();
        }
    }
    Ok(DiffPage {
        response: DiffResponse {
            namespace_id: head.namespace_id.clone(),
            from_seq,
            to_seq,
            after_seq,
            next_after_seq: scan.next_after_seq(),
            next_event_index: scan.next_event_index(),
            retention_floor_seq,
            entries,
        },
        commits_scanned: diff.commits_scanned,
        touches_examined: diff.touches_examined,
        path_resolutions: diff.from.path_resolutions + diff.to.path_resolutions,
    })
}

fn out_of_range(param: &'static str, message: String) -> CoreError {
    CoreError::InvalidDiffRange { param, message }
}

/// The work of one diff page: the two ends it compares, and what it read
/// about each inode that its commits touched.
struct Diff<'n, 'a, 'store, S: ObjectStore + ?Sized> {
    namespace_id: &'n NamespaceId,
    /// The name keys of the requested path's components, root first.
    subtree: Vec<NameKey>,
    from: End<'a, 'store, S>,
    to: End<'a, 'store, S>,
    /// What the page read about each touched inode at `to_seq`, on the
    /// inode's first touch in the page. Like the caches of the two sessions,
    /// it grows with the touches that the page examines, which
    /// [`DIFF_PAGE_TOUCHES`] bounds.
    touched: HashMap<InodeId, TouchedInode>,
    commits_scanned: u64,
    touches_examined: u64,
}

impl<S: ObjectStore + ?Sized> Diff<'_, '_, '_, S> {
    /// Returns the entries of the inodes whose last touch in the range is
    /// `record`'s commit.
    async fn commit_entries(&mut self, record: &WalCommitPayload) -> Result<Vec<DiffEntry>> {
        self.commits_scanned += 1;
        let mut entries = Vec::new();
        for inode_id in touched_inodes(&record.deltas) {
            self.touches_examined += 1;
            let touched = self.touched_inode(inode_id).await?;
            if touched
                .last_touch_seq
                .is_some_and(|seq| seq > record.committed_seq)
            {
                continue;
            }
            entries.extend(self.entry(inode_id, touched).await?);
        }
        Ok(entries)
    }

    /// Returns the entries of the inodes that `record`'s events touch, from
    /// the event at `first_event_index`, for the inodes whose last touch in
    /// the range is the commit. The part ends after the event that brings
    /// the inodes it has examined to `max_touches`, and the next page resumes
    /// the commit at the event after it.
    ///
    /// An inode that the part's events touch more than once is examined once,
    /// and an inode that events on two pages touch is reported on both.
    async fn commit_part_entries(
        &mut self,
        record: &WalCommitPayload,
        first_event_index: u32,
        max_touches: usize,
    ) -> Result<CommitPartEntries> {
        self.commits_scanned += 1;
        let groups: Vec<_> = semantic_operation_groups(&record.deltas).collect();
        let first = usize::try_from(first_event_index).expect("a u32 index should fit in usize");
        if first >= groups.len() {
            return Err(CoreError::InvalidEventIndex(format!(
                "commit `{}` has no event at index {first_event_index}",
                record.committed_seq
            )));
        }
        let mut examined = BTreeSet::new();
        let mut entries = Vec::new();
        for (index, group) in groups.iter().enumerate().skip(first) {
            for inode_id in touched_inodes(group) {
                if !examined.insert(inode_id) {
                    continue;
                }
                self.touches_examined += 1;
                let touched = self.touched_inode(inode_id).await?;
                if touched
                    .last_touch_seq
                    .is_some_and(|seq| seq > record.committed_seq)
                {
                    continue;
                }
                entries.extend(self.entry(inode_id, touched).await?);
            }
            if examined.len() >= max_touches && index + 1 < groups.len() {
                let next_event_index =
                    u32::try_from(index + 1).expect("a commit's events should fit u32 indexes");
                return Ok(CommitPartEntries {
                    entries,
                    next_event_index: Some(next_event_index),
                });
            }
        }
        Ok(CommitPartEntries {
            entries,
            next_event_index: None,
        })
    }

    /// Returns what the page read about `inode_id` at `to_seq`, and reads it
    /// on the inode's first touch in the page.
    ///
    /// The last touch comes from the inode's own rows alone. An ancestor's
    /// later move, deletion, or access change doesn't touch the inode, and
    /// the ancestor's own entry doesn't report the inode's change.
    async fn touched_inode(&mut self, inode_id: InodeId) -> Result<TouchedInode> {
        if let Some(touched) = self.touched.get(&inode_id) {
            return Ok(*touched);
        }
        let to = &mut self.to;
        let binding = to
            .session
            .find_latest_parent_binding_for_child(inode_id)
            .await?;
        let tombstone = to.view.newest_subtree_tombstone(inode_id).await?;
        let (revisions, revisions_seq) = match to.session.inode_at_seq(inode_id).await? {
            Some(inode) => {
                let (revisions, newest_seq) = to.revisions(inode_id, inode.inode_kind).await?;
                (Some(revisions), newest_seq)
            }
            None => (None, None),
        };
        let touched = TouchedInode {
            last_touch_seq: [
                binding.map(|binding| binding.committed_seq),
                tombstone.map(|tombstone| tombstone.committed_seq),
                revisions_seq,
            ]
            .into_iter()
            .flatten()
            .max(),
            revisions,
        };
        self.touched.insert(inode_id, touched);
        Ok(touched)
    }

    /// Classifies `inode_id` by its states at the two ends, and returns its
    /// entry when the states differ and the entry is under the requested
    /// path.
    ///
    /// An entry is under the requested path when the inode's path at either
    /// end is the requested path or below it. An entry is also under the
    /// requested path when the inode is a `deleted`, `moved`, or `undeleted`
    /// directory whose path at either end is above the requested path,
    /// because the kind applies to everything below the directory, and the
    /// diff doesn't list those descendants. A directory above the requested
    /// path is classified by its states and edges, and its revisions at
    /// `from_seq` are read only for a `moved` entry, so classifying a directory
    /// above the path that kept its parent and name reads no more than
    /// classifying an inode outside the path.
    async fn entry(
        &mut self,
        inode_id: InodeId,
        touched: TouchedInode,
    ) -> Result<Option<DiffEntry>> {
        let from = self.from.state(inode_id).await?;
        let to = self.to.state(inode_id).await?;
        let visible = || [&from, &to].into_iter().filter_map(State::visible);
        let at_or_below =
            visible().any(|resolved| is_at_or_below(&resolved.absolute_path, &self.subtree));
        if !at_or_below
            && !visible().any(|resolved| {
                resolved.inode_kind == InodeKind::Directory
                    && is_above(&resolved.absolute_path, &self.subtree)
            })
        {
            return Ok(None);
        }
        let (kind, from_revisions) = match (&from, &to) {
            (State::Absent, State::Visible(_)) if at_or_below => (DiffEntryKind::Created, None),
            (State::Hidden, State::Visible(_)) => (DiffEntryKind::Undeleted, None),
            (State::Visible(_), State::Absent | State::Hidden) => (DiffEntryKind::Deleted, None),
            (State::Visible(from_path), State::Visible(to_path))
                if at_or_below || edge(from_path) != edge(to_path) =>
            {
                let (from_revisions, _) =
                    self.from.revisions(inode_id, from_path.inode_kind).await?;
                let kind = if edge(from_path) != edge(to_path) {
                    DiffEntryKind::Moved
                } else if Some(from_revisions) != touched.revisions {
                    DiffEntryKind::Changed
                } else {
                    return Ok(None);
                };
                (kind, Some(from_revisions))
            }
            // A directory above the requested path that was created, or that
            // kept its parent and name, isn't an entry. Each inode that a
            // created directory contains at `to_seq` and that's under the
            // requested path was created, moved in, or undeleted in the range,
            // so it's an entry of its own. A change to the directory's own
            // revisions doesn't change the state of an inode below the
            // directory.
            (State::Absent | State::Visible(_), State::Visible(_)) => return Ok(None),
            (State::Absent | State::Hidden, State::Absent | State::Hidden) => return Ok(None),
        };
        let (from, to) = (from.into_visible(), to.into_visible());

        // A created or undeleted file reports its revision. A moved or
        // changed inode reports each revision that differs between the ends.
        let (revision_no, attributes_revision_no, access_revision_no) =
            match (from_revisions, touched.revisions) {
                (None, Some(to_revisions)) if kind != DiffEntryKind::Deleted => {
                    (to_revisions.revision_no, None, None)
                }
                (Some(from_revisions), Some(to_revisions)) => (
                    to_revisions
                        .revision_no
                        .filter(|_| from_revisions.revision_no != to_revisions.revision_no),
                    (from_revisions.attributes_revision_no != to_revisions.attributes_revision_no)
                        .then_some(to_revisions.attributes_revision_no),
                    (from_revisions.access_revision_no != to_revisions.access_revision_no)
                        .then_some(to_revisions.access_revision_no),
                ),
                _ => (None, None, None),
            };
        let inode_kind = to
            .as_ref()
            .or(from.as_ref())
            .expect("an entry's inode should be visible at one end")
            .inode_kind;
        Ok(Some(DiffEntry {
            inode_id,
            inode_kind: inode_kind.into(),
            kind,
            binding_version: to
                .as_ref()
                .and_then(|resolved| resolved.binding_version)
                .map(|position| crate::binding_version::encode(position, self.namespace_id)),
            path: to.map(|resolved| resolved.absolute_path),
            previous_path: from
                .filter(|_| matches!(kind, DiffEntryKind::Moved | DiffEntryKind::Deleted))
                .map(|resolved| resolved.absolute_path),
            revision_no,
            attributes_revision_no,
            access_revision_no,
        }))
    }
}

/// The entries that one page reads from part of a commit.
struct CommitPartEntries {
    entries: Vec<DiffEntry>,
    /// The event that the next page resumes the commit at, or `None` when the
    /// part reaches the end of the commit.
    next_event_index: Option<u32>,
}

/// One end of a diff: a session over the view at the end's sequence, and the
/// paths resolved there.
struct End<'a, 'store, S: ObjectStore + ?Sized> {
    view: MetadataView<'a, 'store, S>,
    session: MetadataViewSession<'a, 'store, S>,
    /// The paths resolved at this end, which their descendants' paths start
    /// from.
    paths: HashMap<InodeId, AbsolutePath>,
    path_resolutions: u64,
}

impl<'a, 'store, S: ObjectStore + ?Sized> End<'a, 'store, S> {
    fn new(view: MetadataView<'a, 'store, S>) -> Self {
        Self {
            view,
            session: view.session(),
            paths: HashMap::new(),
            path_resolutions: 0,
        }
    }

    /// Returns the inode's visible state at this end's sequence
    /// ([format section 4.4](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#44-paths-listings-and-revision-reads)).
    async fn state(&mut self, inode_id: InodeId) -> Result<State> {
        if self.session.inode_at_seq(inode_id).await?.is_none() {
            return Ok(State::Absent);
        }
        let resolved = resolve_visible_inode(&mut self.session, &mut self.paths, inode_id).await?;
        Ok(match resolved {
            Some(resolved) => {
                self.path_resolutions += 1;
                State::Visible(resolved)
            }
            None => State::Hidden,
        })
    }

    /// Reads the inode's newest revision, attribute, and access rows at this
    /// end's sequence, and returns their numbers and the newest row's
    /// sequence.
    async fn revisions(
        &self,
        inode_id: InodeId,
        inode_kind: InodeKind,
    ) -> Result<(Revisions, Option<ChangeSeq>)> {
        let revision = match inode_kind {
            InodeKind::File => self.view.latest_revision_record(inode_id).await?,
            InodeKind::Directory => None,
        };
        let attributes = self.view.latest_attributes_revision(inode_id).await?;
        let access = self.view.latest_access_revision(inode_id).await?;
        let newest_seq = [
            revision.as_ref().map(|row| row.committed_seq),
            attributes.as_ref().map(|row| row.committed_seq),
            access.as_ref().map(|row| row.committed_seq),
        ]
        .into_iter()
        .flatten()
        .max();
        let revisions = Revisions {
            revision_no: revision.map(|row| row.revision_no),
            attributes_revision_no: attributes
                .map_or(AttributesRevisionNo(0), |row| row.attributes_revision_no),
            access_revision_no: access.map_or(AccessRevisionNo(0), |row| row.access_revision_no),
        };
        Ok((revisions, newest_seq))
    }
}

/// What a diff page read about one touched inode at `to_seq`.
#[derive(Debug, Clone, Copy)]
struct TouchedInode {
    /// The sequence of the inode's newest own row (a binding event, a
    /// tombstone event, or a revision, attribute, or access row), which is
    /// the last commit in the range that touched the inode.
    last_touch_seq: Option<ChangeSeq>,
    /// The inode's revision numbers, or `None` when it doesn't exist at
    /// `to_seq`.
    revisions: Option<Revisions>,
}

/// The revision numbers of one inode at one sequence, which a `changed` or
/// `moved` entry compares. An inode with no attribute or access row is at
/// revision 0 of each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Revisions {
    /// The file's newest revision, which a directory doesn't have.
    revision_no: Option<RevisionNo>,
    attributes_revision_no: AttributesRevisionNo,
    access_revision_no: AccessRevisionNo,
}

/// An inode's visible state at one end of a diff.
enum State {
    /// The inode didn't exist yet.
    Absent,
    /// A deletion hides the inode.
    Hidden,
    /// The inode is visible, with this path and binding.
    Visible(ResolvedVisiblePath),
}

impl State {
    fn visible(&self) -> Option<&ResolvedVisiblePath> {
        match self {
            Self::Visible(resolved) => Some(resolved),
            Self::Absent | Self::Hidden => None,
        }
    }

    fn into_visible(self) -> Option<ResolvedVisiblePath> {
        match self {
            Self::Visible(resolved) => Some(resolved),
            Self::Absent | Self::Hidden => None,
        }
    }
}

/// Returns the inodes that `deltas` touch, each once: the child of a bind or
/// unbind, the inode of a revision, attribute, access, or creation delta,
/// and the root of a deletion or undelete. A bind's parent isn't touched,
/// because a child's binding doesn't change the parent's own state.
fn touched_inodes(deltas: &[WalCommitDelta]) -> BTreeSet<InodeId> {
    deltas
        .iter()
        .map(|delta| match &delta.delta {
            WalDelta::CreateInode { inode_id, .. }
            | WalDelta::AppendFileRevision { inode_id, .. }
            | WalDelta::AppendAttributesRevision { inode_id, .. }
            | WalDelta::AppendAccessRevision { inode_id, .. } => *inode_id,
            WalDelta::BindDirentry { child_inode_id, .. }
            | WalDelta::UnbindDirentry { child_inode_id, .. } => *child_inode_id,
            WalDelta::TombstoneSubtree { root_inode_id, .. }
            | WalDelta::RevokeSubtreeTombstone { root_inode_id, .. } => *root_inode_id,
        })
        .collect()
}

/// Returns the edge of a visible inode: its parent and its name there. A
/// rename that changes only the name's case changes the edge.
fn edge(resolved: &ResolvedVisiblePath) -> (Option<InodeId>, &str) {
    (resolved.parent_inode_id, &resolved.display_name)
}

fn name_keys(path: &AbsolutePath) -> Vec<NameKey> {
    path.components()
        .iter()
        .map(|component| NameKey::for_display_name(&component.to_display_name()))
        .collect()
}

/// Returns whether `path` is the path whose name keys are `subtree`, or is
/// below it. Names compare by name key, as path lookups compare them, so a
/// request can spell the path in another case.
fn is_at_or_below(path: &AbsolutePath, subtree: &[NameKey]) -> bool {
    path.components().len() >= subtree.len() && name_keys(path).starts_with(subtree)
}

/// Returns whether `path` is above the path whose name keys are `subtree`,
/// comparing names as [`is_at_or_below`] does.
fn is_above(path: &AbsolutePath, subtree: &[NameKey]) -> bool {
    path.components().len() < subtree.len() && subtree.starts_with(&name_keys(path))
}

#[cfg(test)]
mod tests;
