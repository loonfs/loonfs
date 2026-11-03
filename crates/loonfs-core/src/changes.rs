//! The change feed: committed changes after a sequence number, with each
//! commit's durable WAL deltas mapped to semantic filesystem events. The
//! commit scan that pages the feed also pages the diff.

use crate::error::{CoreError, Result};
use crate::heap_bytes::HeapBytes;
use crate::limits::CHANGE_FEED_PAGE_BYTES;
use crate::metadata::MetadataView;
use crate::path::read::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::format::wal::{WalCommitDelta, WalCommitPayload, WalDelta};
use loonfs_types::options::CommitSplitting;
use loonfs_types::{ChangeFeedPosition, ChangeSeq, EffectiveLimit, NamespaceId};
use loonfs_types::{Commit, FilesystemChange, ListChangesResponse, Merge};

pub(crate) async fn list_changes_after<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
    position: ChangeFeedPosition,
    limit: EffectiveLimit,
    splitting: CommitSplitting,
) -> Result<ListChangesResponse> {
    list_changes_within(view, position, limit, splitting, CHANGE_FEED_PAGE_BYTES).await
}

/// Reads one page of the change feed as [`list_changes_after`] does, within a
/// budget of `max_page_bytes` of events.
async fn list_changes_within<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
    position: ChangeFeedPosition,
    limit: EffectiveLimit,
    splitting: CommitSplitting,
    max_page_bytes: usize,
) -> Result<ListChangesResponse> {
    let head = view.head();
    let head_seq = view.visible_seq();
    let retention_floor_seq = view.retention_floor_seq();
    let namespace_id = &head.namespace_id;
    crate::namespace::control::ensure_namespace_live(&head.namespace_id, head.status)?;
    let ChangeFeedPosition {
        after_seq,
        event_index,
    } = position;

    // A position inside a commit starts with that commit, so the commit has
    // to be above the floor, where the feed returns commits.
    if after_seq < retention_floor_seq || (event_index > 0 && after_seq == retention_floor_seq) {
        return Err(CoreError::RebootstrapRequired {
            after_seq,
            retention_floor_seq,
        });
    }
    if after_seq > head_seq {
        return Err(CoreError::InvalidCursor(format!(
            "change feed sequence `{after_seq}` is ahead of namespace head `{head_seq}`"
        )));
    }
    ensure_event_index_allowed(event_index, splitting)?;
    if after_seq == head_seq && event_index == 0 {
        return Ok(ListChangesResponse {
            namespace_id: namespace_id.clone(),
            after_seq,
            head_seq,
            next_after_seq: None,
            next_event_index: None,
            changes: Vec::new(),
        });
    }

    let mut scan = CommitScan::new(
        view.metadata_view(),
        position,
        limit,
        splitting,
        max_page_bytes,
    );
    let mut changes = Vec::new();
    while let Some(scanned) = scan.next().await? {
        let record = &scanned.record;
        let built = events_from_wal_deltas_within(
            namespace_id,
            record.committed_seq,
            &record.deltas,
            scanned.first_event_index,
            scan.event_budget(),
        )?;
        let change = committed_change_with_events(namespace_id, record, built.events);
        let heap_bytes = change.events.heap_bytes();
        match built.next_event_index {
            None => {
                if scan.admit(change.committed_seq, heap_bytes) {
                    changes.push(change);
                }
            }
            Some(next_event_index) if scan.is_empty() => {
                scan.admit_part(change.committed_seq, heap_bytes, next_event_index);
                changes.push(change);
            }
            // The commit doesn't fit with the commits before it, so it
            // starts the next page.
            Some(_) => scan.end_page(),
        }
    }

    Ok(ListChangesResponse {
        namespace_id: namespace_id.clone(),
        after_seq,
        head_seq,
        next_after_seq: scan.next_after_seq(),
        next_event_index: scan.next_event_index(),
        changes,
    })
}

/// Refuses a position inside a commit unless the request lets a page end
/// inside a commit, because only such a page returns one.
pub(crate) fn ensure_event_index_allowed(
    event_index: u32,
    splitting: CommitSplitting,
) -> Result<()> {
    if event_index > 0 && splitting == CommitSplitting::Whole {
        return Err(CoreError::InvalidEventIndex(
            "a position inside a commit requires `split_commits`".to_owned(),
        ));
    }
    Ok(())
}

/// One commit that a [`CommitScan`] offers to its page, and the event that
/// the page starts the commit at, which is 0 except for a commit that the
/// page resumes.
pub(crate) struct ScannedCommit {
    pub(crate) record: WalCommitPayload,
    pub(crate) first_event_index: u32,
}

/// The page loop that the change feed and the diff share. It reads the
/// commits after one sequence in order, through the sequence of its view.
///
/// A caller takes each commit from [`Self::next`], builds what its page
/// reports for that commit, and offers the commit to [`Self::admit`] with
/// the heap bytes of what it built. The page ends after `limit` commits, at
/// the view's sequence, or at the page's byte budget. A commit that would
/// take a page with other commits past the budget is left for the next page,
/// so a commit that fits isn't split across pages. A commit larger than
/// the budget is a page of its own, or, with
/// [`CommitSplitting::AtPageBudget`], is returned across pages through
/// [`Self::admit_part`], and the next page resumes it at an event index.
pub(crate) struct CommitScan<'a, 'store, S: ObjectStore + ?Sized> {
    view: MetadataView<'a, 'store, S>,
    limit: EffectiveLimit,
    splitting: CommitSplitting,
    max_page_bytes: usize,
    /// The last commit on the page, or the page's start before the first.
    through_seq: ChangeSeq,
    /// The event at which the page resumes the commit at `through_seq`,
    /// until [`Self::next`] returns that commit.
    resume_event_index: Option<u32>,
    /// The first event of the commit at `through_seq` that the page didn't
    /// return, when the page ended inside that commit.
    next_event_index: Option<u32>,
    commits: usize,
    page_bytes: usize,
    /// Whether the byte budget or the caller ended the page.
    full: bool,
}

impl<'a, 'store, S: ObjectStore + ?Sized> CommitScan<'a, 'store, S> {
    /// Starts a page at `position` in the commits that `view` contains.
    pub(crate) fn new(
        view: MetadataView<'a, 'store, S>,
        position: ChangeFeedPosition,
        limit: EffectiveLimit,
        splitting: CommitSplitting,
        max_page_bytes: usize,
    ) -> Self {
        Self {
            view,
            limit,
            splitting,
            max_page_bytes,
            through_seq: position.after_seq,
            resume_event_index: (position.event_index > 0).then_some(position.event_index),
            next_event_index: None,
            commits: 0,
            page_bytes: 0,
            full: false,
        }
    }

    /// Returns the next commit for the page, or `None` once the page has
    /// ended. The caller offers each commit to [`Self::admit`] or
    /// [`Self::admit_part`] before it asks for the next one.
    pub(crate) async fn next(&mut self) -> Result<Option<ScannedCommit>> {
        if let Some(first_event_index) = self.resume_event_index.take() {
            let record = self
                .view
                .commit_at_seq(self.through_seq)
                .await?
                .ok_or_else(|| {
                    CoreError::InvalidEventIndex(format!(
                        "no commit at sequence `{}` to resume",
                        self.through_seq
                    ))
                })?;
            return Ok(Some(ScannedCommit {
                record,
                first_event_index,
            }));
        }
        let end_seq = self.view.visible_seq();
        if self.full || self.commits >= self.limit.as_usize() || self.through_seq >= end_seq {
            return Ok(None);
        }
        let record = self.view.commits_after(self.through_seq, 1).await?.pop();
        if record.is_none() {
            self.through_seq = end_seq;
        }
        Ok(record.map(|record| ScannedCommit {
            record,
            first_event_index: 0,
        }))
    }

    /// Returns whether the page has no commit yet.
    pub(crate) fn is_empty(&self) -> bool {
        self.commits == 0
    }

    /// Returns how many bytes of events the next commit can bring to the
    /// page: the rest of the budget, or no bound for a commit that starts an
    /// empty page and is returned whole.
    pub(crate) fn event_budget(&self) -> Option<usize> {
        match (self.is_empty(), self.splitting) {
            (true, CommitSplitting::Whole) => None,
            _ => Some(self.max_page_bytes.saturating_sub(self.page_bytes)),
        }
    }

    /// Puts the commit at `committed_seq` on the page if `heap_bytes` more
    /// fit, and returns whether they did. A commit that doesn't fit ends the
    /// page, and the next page starts with it.
    pub(crate) fn admit(&mut self, committed_seq: ChangeSeq, heap_bytes: usize) -> bool {
        let page_bytes = self.page_bytes.saturating_add(heap_bytes);
        if self.commits > 0 && page_bytes > self.max_page_bytes {
            self.full = true;
            return false;
        }
        self.page_bytes = page_bytes;
        self.through_seq = committed_seq;
        self.commits += 1;
        self.full = page_bytes >= self.max_page_bytes;
        true
    }

    /// Puts part of the commit at `committed_seq`, `heap_bytes` of its
    /// events, on the page, and ends the page there, so the next page
    /// resumes the commit at `next_event_index`. Only a commit that alone
    /// exceeds the budget is returned in parts.
    pub(crate) fn admit_part(
        &mut self,
        committed_seq: ChangeSeq,
        heap_bytes: usize,
        next_event_index: u32,
    ) {
        assert!(
            self.is_empty() && self.splitting == CommitSplitting::AtPageBudget,
            "only a page that splits commits returns part of a commit, and only by itself"
        );
        self.page_bytes = self.page_bytes.saturating_add(heap_bytes);
        self.through_seq = committed_seq;
        self.next_event_index = Some(next_event_index);
        self.commits += 1;
        self.full = true;
    }

    /// Ends the page after the commit that [`Self::admit`] took last, so
    /// [`Self::next`] returns `None`, and the next page starts after that
    /// commit.
    pub(crate) fn end_page(&mut self) {
        self.full = true;
    }

    /// Returns the sequence that the next page starts after, or resumes,
    /// or `None` when this page reached the view's sequence.
    pub(crate) fn next_after_seq(&self) -> Option<ChangeSeq> {
        (self.next_event_index.is_some() || self.through_seq < self.view.visible_seq())
            .then_some(self.through_seq)
    }

    /// Returns the event at which the next page resumes the commit at
    /// [`Self::next_after_seq`], when this page ended inside it.
    pub(crate) fn next_event_index(&self) -> Option<u32> {
        self.next_event_index
    }
}

/// Converts one WAL commit record into the shared API change shape.
pub(super) fn committed_change_from_wal_record(
    namespace_id: &NamespaceId,
    record: &WalCommitPayload,
) -> Result<Commit> {
    let events = events_from_wal_deltas(namespace_id, record.committed_seq, &record.deltas)?;
    Ok(committed_change_with_events(namespace_id, record, events))
}

/// Builds the API change of `record` with `events`, which are the commit's
/// events or, on a page that splits the commit, the page's part of them.
fn committed_change_with_events(
    namespace_id: &NamespaceId,
    record: &WalCommitPayload,
    events: Vec<FilesystemChange>,
) -> Commit {
    Commit {
        namespace_id: namespace_id.clone(),
        committed_seq: record.committed_seq,
        commit_id: record.commit_id.clone(),
        committed_by: record.committed_by.clone(),
        committed_at_ms: record.committed_at_ms,
        message: record.message.clone(),
        merge: record.merge.as_ref().map(|merge| Merge {
            source_namespace_id: merge.source_namespace_id.clone(),
            source_after_seq: merge.source_after_seq,
            source_through_seq: merge.source_through_seq,
        }),
        events,
    }
}

/// Maps one commit's ordered WAL deltas to semantic filesystem events, one
/// per internal operation.
///
/// One request operation can compile into several internal operations —
/// creating missing parent directories, replacing a file by moving over it,
/// copying attributes onto a new inode — and each of those gets its own
/// event. The deltas include the request-operation index they came from, so the
/// events stay in request order whatever their count.
///
/// Validation emits one fixed delta pattern per internal operation, so a
/// stored commit with an unmatched pattern returns `namespace_corrupt`.
///
/// `committed_seq` is also the bind sequence for bindings created by this
/// commit.
pub(crate) fn events_from_wal_deltas(
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
    deltas: &[WalCommitDelta],
) -> Result<Vec<FilesystemChange>> {
    Ok(events_from_wal_deltas_within(namespace_id, committed_seq, deltas, 0, None)?.events)
}

/// The events of one commit that one page returns.
pub(crate) struct CommitEvents {
    pub(crate) events: Vec<FilesystemChange>,
    /// The index of the first event that the page didn't build, or `None`
    /// when the events reach the end of the commit.
    pub(crate) next_event_index: Option<u32>,
}

/// Maps a commit's deltas to its events as [`events_from_wal_deltas`] does,
/// from the event at `first_event_index`, and stops before the event that
/// would take the events past `max_heap_bytes`, after at least one event.
///
/// Each internal operation's deltas are contiguous and produce one event, so
/// the event at an index is the operation group at that index, and the
/// groups before it are skipped without building their events.
pub(crate) fn events_from_wal_deltas_within(
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
    deltas: &[WalCommitDelta],
    first_event_index: u32,
    max_heap_bytes: Option<usize>,
) -> Result<CommitEvents> {
    let no_event = || {
        CoreError::InvalidEventIndex(format!(
            "commit `{committed_seq}` has no event at index {first_event_index}"
        ))
    };
    let mut groups = loonfs_types::format::wal::semantic_operation_groups(deltas).peekable();
    // The index comes from a request, so the skip stops at the commit's last
    // group instead of running to an index as large as `u32::MAX`.
    for _ in 0..first_event_index {
        groups.next().ok_or_else(no_event)?;
    }
    if first_event_index > 0 && groups.peek().is_none() {
        return Err(no_event());
    }
    let mut events = Vec::new();
    let mut heap_bytes = 0_usize;
    let mut group = Vec::new();
    let mut event_index = first_event_index;
    for deltas in groups {
        group.clear();
        group.extend(deltas.iter().map(|delta| &delta.delta));
        let event = event_from_op_deltas(namespace_id, committed_seq, &group)?;
        let event_bytes = std::mem::size_of::<FilesystemChange>() + event.heap_bytes();
        if max_heap_bytes
            .is_some_and(|max| !events.is_empty() && heap_bytes.saturating_add(event_bytes) > max)
        {
            return Ok(CommitEvents {
                events,
                next_event_index: Some(event_index),
            });
        }
        heap_bytes = heap_bytes.saturating_add(event_bytes);
        events.push(event);
        event_index = event_index
            .checked_add(1)
            .expect("a commit should have fewer events than its 32-bit delta positions allow");
    }
    Ok(CommitEvents {
        events,
        next_event_index: None,
    })
}

fn event_from_op_deltas(
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
    deltas: &[&WalDelta],
) -> Result<FilesystemChange> {
    Ok(match deltas {
        [WalDelta::CreateInode {
            inode_id,
            inode_kind: loonfs_types::InodeKind::Directory,
            ..
        }, WalDelta::BindDirentry {
            delta_index,
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }] if child_inode_id == inode_id => FilesystemChange::DirectoryCreated {
            inode_id: *inode_id,
            parent_inode_id: *parent_inode_id,
            display_name: display_name.clone(),
            binding_version: binding_version(namespace_id, committed_seq, *delta_index),
        },
        // CreateFile (and copy-file): allocate + bind + first revision.
        [WalDelta::CreateInode {
            inode_id,
            inode_kind: loonfs_types::InodeKind::File,
            ..
        }, WalDelta::BindDirentry {
            delta_index,
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }, WalDelta::AppendFileRevision {
            inode_id: revision_inode_id,
            revision_no,
            content_ref,
            ..
        }] if child_inode_id == inode_id && revision_inode_id == inode_id => {
            FilesystemChange::FileCreated {
                inode_id: *inode_id,
                parent_inode_id: *parent_inode_id,
                display_name: display_name.clone(),
                binding_version: binding_version(namespace_id, committed_seq, *delta_index),
                revision_no: *revision_no,
                content_ref: content_ref.clone(),
            }
        }
        // ReplaceFile (a put, a copy, or an append) and RestoreRevision share
        // one delta kind. Only an append's delta records the offset where its
        // bytes begin.
        [WalDelta::AppendFileRevision {
            inode_id,
            revision_no,
            content_ref,
            append_offset,
            ..
        }] => FilesystemChange::ContentChanged {
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref: content_ref.clone(),
            append_offset: *append_offset,
        },
        // Rename: retire the old binding, publish the new one.
        [WalDelta::UnbindDirentry {
            parent_inode_id: source_parent_inode_id,
            display_name: from_name,
            child_inode_id,
            ..
        }, WalDelta::BindDirentry {
            delta_index,
            parent_inode_id: destination_parent_inode_id,
            display_name: to_name,
            child_inode_id: bound_inode_id,
            ..
        }] if child_inode_id == bound_inode_id => FilesystemChange::Moved {
            inode_id: *child_inode_id,
            source_parent_inode_id: *source_parent_inode_id,
            source_display_name: from_name.clone(),
            destination_parent_inode_id: *destination_parent_inode_id,
            destination_display_name: to_name.clone(),
            binding_version: binding_version(namespace_id, committed_seq, *delta_index),
        },
        // DeleteFile / DeleteSubtree: retire the binding, hide the subtree.
        [WalDelta::UnbindDirentry { child_inode_id, .. }, WalDelta::TombstoneSubtree {
            root_inode_id,
            deleted_binding,
            ..
        }] if child_inode_id == root_inode_id => FilesystemChange::Deleted {
            inode_id: *root_inode_id,
            deleted_binding: loonfs_types::DirectoryBinding {
                parent_inode_id: deleted_binding.parent_inode_id,
                name_key: deleted_binding.name_key.clone(),
                display_name: deleted_binding.display_name.clone(),
            },
        },
        // Undelete: revoke the exact deletion position, re-bind the root.
        [WalDelta::RevokeSubtreeTombstone { root_inode_id, .. }, WalDelta::BindDirentry {
            delta_index,
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }] if root_inode_id == child_inode_id => FilesystemChange::Undeleted {
            inode_id: *root_inode_id,
            parent_inode_id: *parent_inode_id,
            display_name: display_name.clone(),
            binding_version: binding_version(namespace_id, committed_seq, *delta_index),
        },
        // UpdateAttributes, including the copy that copies a source's
        // attributes onto the inode it just created. The delta already contains
        // the complete resulting map, so the event does too.
        [WalDelta::AppendAttributesRevision {
            inode_id,
            attributes_revision_no,
            attributes,
            ..
        }] => FilesystemChange::AttributesChanged {
            inode_id: *inode_id,
            attributes_revision_no: *attributes_revision_no,
            attributes: attributes.clone(),
        },
        [WalDelta::AppendAccessRevision {
            inode_id,
            access_revision_no,
            boundary,
            grants,
            ..
        }] => FilesystemChange::AccessChanged {
            inode_id: *inode_id,
            access_revision_no: *access_revision_no,
            boundary: *boundary,
            grants: grants.clone(),
        },
        other => {
            return Err(CoreError::NamespaceCorrupt(format!(
                "commit {committed_seq} has an operation whose {} deltas form no pattern \
                 the change feed maps",
                other.len()
            )))
        }
    })
}

fn binding_version(
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
    delta_index: u32,
) -> loonfs_types::BindingVersion {
    crate::binding_version::encode(
        DeltaPosition {
            seq: committed_seq,
            delta_index,
        },
        namespace_id,
    )
}

#[cfg(test)]
mod split_tests;

#[cfg(test)]
mod tests {
    use super::event_from_op_deltas;
    use crate::error::CoreError;

    use crate::test_support::WriterAt;
    use crate::{NamespaceEngine, RuntimeReadContext};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_types::format::wal::WalDelta;
    use loonfs_types::FilesystemChange;
    use loonfs_types::{
        AttributeKey, AttributeValue, Attributes, AttributesRevisionNo, ChangeSeq, EffectiveLimit,
        InodeId, NamespaceId,
    };
    use std::assert_matches;
    use std::num::NonZeroU32;

    use tempfile::tempdir;

    fn namespace_id() -> NamespaceId {
        loonfs_test_support::ids::namespace_id("demo")
    }

    fn attributes() -> Attributes {
        Attributes::new(std::collections::BTreeMap::from([(
            AttributeKey::parse("owner").expect("valid attribute key"),
            AttributeValue::parse("ada").expect("valid attribute value"),
        )]))
        .expect("valid attribute map")
    }

    fn append_attributes(delta_index: u32) -> WalDelta {
        WalDelta::AppendAttributesRevision {
            delta_index,
            inode_id: InodeId(7),
            attributes_revision_no: AttributesRevisionNo(3),
            attributes: attributes(),
        }
    }

    #[tokio::test]
    async fn change_feed_accepts_the_head_but_rejects_a_future_cursor() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = namespace_id();
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &WriterAt {
                writer_id: loonfs_types::WriterId::parse("writer").expect("writer id"),
                now_ms: 1,
            },
        )
        .create()
        .await
        .expect("bootstrap");
        let limit = EffectiveLimit::new(NonZeroU32::MIN);
        let loaded = crate::test_support::namespace_store(&store, &namespace_id)
            .load_read_anchor(crate::test_support::observed_now())
            .await
            .expect("load read basis");
        let context = RuntimeReadContext {
            read_basis: crate::control::ReadBasis {
                basis: loaded.basis(),
                head: loaded.read_state,
            },
            tail: None,
            visible_seq: None,
        };
        let engine =
            NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id));

        let caught_up = engine
            .list_changes_after(
                loonfs_types::ChangeFeedPosition::after(ChangeSeq(0)),
                limit,
                loonfs_types::options::CommitSplitting::Whole,
                &context,
            )
            .await
            .expect("the head is a valid cursor");
        assert!(caught_up.changes.is_empty());
        assert_eq!(caught_up.head_seq, ChangeSeq(0));

        let error = engine
            .list_changes_after(
                loonfs_types::ChangeFeedPosition::after(ChangeSeq(1)),
                limit,
                loonfs_types::options::CommitSplitting::Whole,
                &context,
            )
            .await
            .expect_err("an unpublished sequence cannot be a valid cursor");
        assert_matches!(error, CoreError::InvalidCursor(_));
    }

    #[tokio::test]
    async fn large_commits_page_by_event_bytes_without_splitting_or_skipping() {
        use crate::commit::engine::CommitCandidate;
        use crate::heap_bytes::HeapBytes;
        use crate::limits::{CHANGE_FEED_PAGE_BYTES, MAX_COMMIT_OPERATIONS};
        use crate::publish::{CommitIntent, FilesystemOperation};
        use loonfs_types::{AbsolutePath, CommitId};

        let directory = tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = namespace_id();
        let context = WriterAt {
            writer_id: loonfs_types::WriterId::parse("writer").expect("writer id"),
            now_ms: 1,
        };
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        let mut expected = Vec::new();
        for number in 1..=5 {
            let depth = if number == 4 { 4 } else { 2 };
            let request = CommitIntent {
                preconditions: Vec::new(),
                commit_id: CommitId::parse(format!("commit-{number}")).expect("commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations: (0..MAX_COMMIT_OPERATIONS)
                    .map(|operation| FilesystemOperation::CreateDirectory {
                        path: AbsolutePath::parse(format!(
                            "/directory-{number}-{operation}{}",
                            "/child".repeat(depth - 1)
                        ))
                        .expect("path"),
                        parents: true,
                    })
                    .collect(),
            };
            let commit =
                crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                    .publish_candidates(vec![CommitCandidate::new(request)])
                    .await
                    .pop()
                    .expect("one result")
                    .expect("publish commit");
            assert_eq!(commit.events.len(), MAX_COMMIT_OPERATIONS * depth);
            assert_eq!(
                commit.events.heap_bytes() > CHANGE_FEED_PAGE_BYTES,
                number == 4
            );
            expected.push((commit.committed_seq, commit.commit_id, commit.events.len()));
            if number == 2 || number == 4 {
                crate::test_support::namespace_store(&store, &namespace_id)
                    .fold_wal()
                    .await
                    .expect("fold commits");
            }
        }

        for fully_folded in [false, true] {
            if fully_folded {
                crate::test_support::namespace_store(&store, &namespace_id)
                    .fold_wal()
                    .await
                    .expect("fold tail");
            }
            let namespace = crate::test_support::namespace_store(&store, &namespace_id);
            let view = namespace
                .load_current_metadata_view()
                .await
                .expect("read view");
            for commit_limit in [1000, 1] {
                let limit = EffectiveLimit::new(NonZeroU32::new(commit_limit).expect("limit"));
                let mut after_seq = ChangeSeq(0);
                let mut actual = Vec::new();
                loop {
                    let page = super::list_changes_after(
                        &view,
                        loonfs_types::ChangeFeedPosition::after(after_seq),
                        limit,
                        loonfs_types::options::CommitSplitting::Whole,
                    )
                    .await
                    .expect("page");
                    assert!(!page.changes.is_empty());
                    assert!(page.changes.len() <= limit.as_usize());
                    let bytes: usize = page
                        .changes
                        .iter()
                        .map(|commit| commit.events.heap_bytes())
                        .sum();
                    if bytes > CHANGE_FEED_PAGE_BYTES {
                        assert_eq!(page.changes.len(), 1);
                        assert_eq!(page.changes[0].committed_seq, ChangeSeq(4));
                    }
                    if after_seq == ChangeSeq(0) && commit_limit == 1000 {
                        assert!(page.changes.len() < limit.as_usize());
                        assert!(bytes <= CHANGE_FEED_PAGE_BYTES);
                        assert!(page.next_after_seq.is_some());
                    }
                    let last = page.changes.last().expect("nonempty page").committed_seq;
                    assert_eq!(page.head_seq, expected.last().expect("published commits").0);
                    actual.extend(page.changes.into_iter().map(|commit| {
                        (commit.committed_seq, commit.commit_id, commit.events.len())
                    }));
                    let Some(next) = page.next_after_seq else {
                        break;
                    };
                    assert_eq!(next, last);
                    assert!(next > after_seq);
                    after_seq = next;
                    assert!(actual.len() < expected.len());
                }
                assert_eq!(actual, expected);
            }
        }
        let manifest = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("manifest");
        let basis = crate::manifest::metadata_basis_from_manifest(&store, None, &manifest);
        let rows = basis
            .segments
            .scan_range_page(
                loonfs_types::format::manifest::MetadataRowFamily::Commits,
                &loonfs_types::format::manifest::lookup_keys::commit_row_key(ChangeSeq(2)),
                None,
                2,
            )
            .await
            .expect("commit row page");
        assert_eq!(rows.len(), 2);
        assert_eq!(basis.segments.peak_page_rows(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| {
                    row.row_key_for_family(
                        loonfs_types::format::manifest::MetadataRowFamily::Commits,
                    )
                })
                .collect::<Vec<_>>(),
            [2, 3].map(|seq| {
                loonfs_types::format::manifest::lookup_keys::commit_row_key(ChangeSeq(seq))
            })
        );
    }

    #[test]
    fn one_attribute_delta_maps_to_one_event_carrying_the_whole_map() {
        let delta = append_attributes(0);

        assert_eq!(
            event_from_op_deltas(&namespace_id(), ChangeSeq(7), &[&delta])
                .expect("map the operation"),
            FilesystemChange::AttributesChanged {
                inode_id: InodeId(7),
                attributes_revision_no: AttributesRevisionNo(3),
                attributes: attributes(),
            }
        );
    }

    #[test]
    fn a_delta_pattern_the_reducer_never_produces_is_rejected() {
        let first = append_attributes(0);
        let second = append_attributes(1);

        let error = event_from_op_deltas(&namespace_id(), ChangeSeq(7), &[&first, &second])
            .expect_err("two attribute deltas are not one operation");
        assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
    }
}
