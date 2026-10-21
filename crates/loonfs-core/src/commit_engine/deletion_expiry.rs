//! A writer session's expiry of the deletions that the retention floor
//! reaches, one bounded step at a time
//! ([Retention window](https://github.com/loonfs/loonfs/blob/main/docs/design/retention-window.md#expiring-a-deletion)).

use super::{
    failed_publication, LoadedPublishView, NamespaceCommitEngine,
    NamespaceCommitEnginePublishResult, WriterSessionStatus,
};
use crate::context::MutationContext;
use crate::error::Result;
use crate::limits::MAX_EXPIRED_DELETIONS_PER_COMMIT;
use crate::metadata::ActiveDeletionRange;
use crate::namespace::state::NamespaceReadState;
use crate::time::Deadline;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{ChangeSeq, CommitId, ExpiredDeletion};

/// How far a writer session has expired the deletions at or below the
/// retention floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DeletionExpiryState {
    /// The last floor whose deletions the session finished expiring. A new
    /// session starts at sequence 0, where no deletion can be.
    finished_floor_seq: ChangeSeq,
    /// The last deletion that the session expired above
    /// `finished_floor_seq`. The next scan starts after it, or above
    /// `finished_floor_seq` when there's none.
    last_expired: Option<ExpiredDeletion>,
    /// The fork point, when the session opened a fork. The session expires
    /// every deletion at or below it before its first request.
    fork_seq: Option<ChangeSeq>,
}

impl DeletionExpiryState {
    pub(super) fn new() -> Self {
        Self {
            finished_floor_seq: ChangeSeq(0),
            last_expired: None,
            fork_seq: None,
        }
    }

    /// Records the fork point of the namespace that the session's writer
    /// epoch was acquired on, which every manifest of a fork records in its
    /// fork basis.
    pub(super) fn open_session(&mut self, head: &NamespaceReadState) {
        self.fork_seq = head
            .fork_basis
            .as_ref()
            .map(|basis| basis.manifest.head_seq);
    }

    /// The fork point, while a deletion at or below it can still be
    /// recoverable.
    fn inherited_through_seq(&self) -> Option<ChangeSeq> {
        self.fork_seq
            .filter(|fork_seq| *fork_seq > self.finished_floor_seq)
    }

    /// The deletions that the next scan reads, ending at `through_seq`.
    fn next_range(&self, through_seq: ChangeSeq) -> ActiveDeletionRange {
        match &self.last_expired {
            Some(last) => ActiveDeletionRange::after(Some((last.deletion_seq, last.inode_id))),
            None => ActiveDeletionRange::after(None).above_seq(self.finished_floor_seq),
        }
        .through(through_seq)
    }

    /// Records that no listed deletion at or below `floor_seq` remains.
    fn finish(&mut self, floor_seq: ChangeSeq) {
        self.finished_floor_seq = floor_seq;
        self.last_expired = None;
    }
}

impl NamespaceCommitEngine {
    /// Whether the position's retention floor is above the last floor whose
    /// deletions the session finished expiring, so that
    /// [`Self::publish_expiry_step`] has deletions to look for.
    ///
    /// The position takes a manifest's floor when it loads the manifest or
    /// moves onto it (see [`Self::record_manifest_published`]), so this turns
    /// true at the first position move after a retention advance.
    pub fn expiry_due(&self) -> bool {
        !matches!(*self.lock_session(), WriterSessionStatus::Fenced(_))
            && self.publish_tail.as_ref().is_some_and(|position| {
                position.retention_floor_seq() > self.deletion_expiry.finished_floor_seq
            })
    }

    /// Runs one step of the session's expiry under the retention floor: scans
    /// the session's view of `active_deletions` for the next
    /// [`MAX_EXPIRED_DELETIONS_PER_COMMIT`] `listed` deletions at or below
    /// the floor, and commits an expire event for each of them in one commit
    /// attributed to `loonfs`.
    ///
    /// The scan and the commit use one view, and the engine plans no other
    /// commit between them, so every expire event passes validation. The
    /// next step starts after the last deletion that this step expired.
    /// Returns `None`, and records that the session finished expiring under
    /// the floor, once the scan finds no deletion.
    pub async fn publish_expiry_step<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        context: &MutationContext,
        batch: &Deadline,
    ) -> Option<NamespaceCommitEnginePublishResult> {
        let segment_cache = self.segment_cache.clone();
        let loaded = match self
            .load_publish_view(store, segment_cache.as_deref(), context, batch)
            .await
        {
            Ok(loaded) => loaded,
            Err(error) => return Some(failed_publication(1, error)),
        };
        let floor_seq = loaded.view.retention_floor_seq();
        if floor_seq > self.deletion_expiry.finished_floor_seq {
            match self.scan_expirable_deletions(&loaded, floor_seq).await {
                Ok(deletions) if !deletions.is_empty() => {
                    return Some(
                        self.publish_scanned_expiry(store, loaded, deletions, context, batch)
                            .await,
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    self.keep_loaded_position(loaded);
                    return Some(failed_publication(1, error));
                }
            }
        }
        self.keep_loaded_position(loaded);
        None
    }

    /// Runs one step of the expiry of a fork's inherited deletions against
    /// `loaded`, when the session opened a fork and hasn't finished expiring
    /// them, so that the session's first request can't restore an inherited
    /// deletion, whatever the timing.
    ///
    /// Returns the view once no inherited deletion is recoverable, and the
    /// batch plans against it. Returns `None` after a step that expired
    /// inherited deletions, and the batch loads the view again.
    pub(super) async fn expire_inherited_deletions<'a, S: ObjectStore + ?Sized>(
        &mut self,
        store: &'a S,
        loaded: LoadedPublishView<'a, S>,
        context: &MutationContext,
        batch: &Deadline,
    ) -> Result<Option<LoadedPublishView<'a, S>>> {
        // The fork's floor starts at the fork point and never goes below it,
        // so the scan can end at the fork point.
        let Some(fork_seq) = self.deletion_expiry.inherited_through_seq() else {
            return Ok(Some(loaded));
        };
        let deletions = match self.scan_expirable_deletions(&loaded, fork_seq).await {
            Ok(deletions) => deletions,
            Err(error) => {
                self.keep_loaded_position(loaded);
                return Err(error);
            }
        };
        if deletions.is_empty() {
            return Ok(Some(loaded));
        }
        let published = self
            .publish_scanned_expiry(store, loaded, deletions, context, batch)
            .await;
        match published.results.into_iter().find_map(Result::err) {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    /// Scans `loaded` for the next `listed` deletions at or below
    /// `through_seq`, and records that the session finished expiring through
    /// `through_seq` when the scan finds none.
    async fn scan_expirable_deletions<S: ObjectStore + ?Sized>(
        &mut self,
        loaded: &LoadedPublishView<'_, S>,
        through_seq: ChangeSeq,
    ) -> Result<Vec<ExpiredDeletion>> {
        let range = self.deletion_expiry.next_range(through_seq);
        let found = loaded
            .view
            .metadata_view()
            .session()
            .active_deletions_page(&range, MAX_EXPIRED_DELETIONS_PER_COMMIT)
            .await?;
        if found.is_empty() {
            self.deletion_expiry.finish(through_seq);
        }
        Ok(found
            .into_iter()
            .map(|deletion| ExpiredDeletion {
                inode_id: deletion.root_inode_id,
                deletion_seq: deletion.deletion_seq,
            })
            .collect())
    }

    /// Commits the expiry of `deletions`, which a scan of `loaded` found,
    /// against the same view, so the next scan starts after the last of
    /// them.
    async fn publish_scanned_expiry<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        loaded: LoadedPublishView<'_, S>,
        deletions: Vec<ExpiredDeletion>,
        context: &MutationContext,
        batch: &Deadline,
    ) -> NamespaceCommitEnginePublishResult {
        let last = deletions.last().copied();
        let published = self
            .publish_expiry_against(
                store,
                loaded,
                CommitId::generate(),
                deletions,
                context,
                batch,
            )
            .await;
        if last.is_some() && published.results.iter().all(Result::is_ok) {
            self.deletion_expiry.last_expired = last;
        }
        published
    }
}
