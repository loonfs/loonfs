//! Deciding one streaming compaction's objects: the lease says who owns them,
//! and claiming it is what makes the answer safe to act on.
//!
//! A job writes its output under `metadata/compactions/{job_id}/` and refreshes
//! its lease beside it by compare-and-swap while it runs (format spec,
//! "Garbage collection", rule 12). A pass sweeping that prefix therefore asks
//! one question per job: does the job that owns these keys still hold its
//! lease? A fresh lease retains the whole prefix however old the objects under
//! it are. An expired one is not an answer on its own — the job may be
//! resuming from a long stall — so the pass claims it, moving it from `Active`
//! to `Reaping` by compare-and-swap. Winning that claim fences the job: its
//! next heartbeat fails, it publishes nothing, and only then are the objects
//! under the prefix ordinary orphans.
//!
//! The claimed lease is deleted last, after the segments it covers. The lease
//! of a job sorts before that job's `tables/` directory, so the pass reads it
//! first and holds the deletion until the walk leaves the prefix. A pass that
//! stops in between leaves a `Reaping` lease behind, which is exactly what a
//! later pass needs to finish the reap without deciding anything again.
//!
//! Because a job's objects are contiguous, remembering the answer for the job
//! being walked turns one lease read per object into one lease read per job.
//! The memo holds only answers a compare-and-swap settled — a fresh `Active`
//! read or a won claim — never a merely expired read.

use crate::checkpoint::{claim_compaction_prefix, CompactionPrefixOwner};
use crate::error::{CoreError, Result};
use loonfs_api::{MetadataCompactionId, NamespaceId};
use loonfs_objectstore::keys::{metadata_compaction_job_id_from_key, metadata_compaction_lease};
use loonfs_objectstore::ObjectStore;

/// What one pass has learned about the compaction job it is walking, and the
/// claimed lease it still owes a deletion.
#[derive(Debug, Default)]
pub(super) struct CompactionLeases {
    /// The job this pass last read a lease for, and who owns its prefix.
    last_read: Option<(String, CompactionPrefixOwner)>,
    /// The lease key of a claimed prefix, held until the walk leaves that
    /// prefix so the segments under it go first.
    claimed_lease: Option<String>,
}

/// What the sweep should do with one key under the compaction prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StagedObject {
    /// A live job owns it. Nothing about its age matters.
    OwnedByALiveJob,
    /// No job owns it: it belongs to a job that finished, died, or was
    /// fenced. It ages out as an ordinary unreferenced orphan.
    Orphaned,
    /// The lease of a prefix this pass claimed. It is deleted when the walk
    /// leaves the prefix, after the orphans under it.
    ClaimedLease,
    /// The key is under the prefix but is neither a job's lease nor one of
    /// its staged segments, so this pass cannot tell what wrote it and
    /// retains it.
    UnrecognizedKey,
}

impl CompactionLeases {
    /// Reads `key`'s owning job, and the lease of that job unless this pass
    /// has already settled it.
    pub(super) async fn owner_of<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        key: &str,
        now_ms: u64,
    ) -> Result<StagedObject> {
        let Some(metadata_compaction_id) = metadata_compaction_job_id_from_key(key)
            .and_then(|job_id| MetadataCompactionId::parse(job_id).ok())
        else {
            return Ok(StagedObject::UnrecognizedKey);
        };
        let owner = match &self.last_read {
            Some((read_job_id, owner)) if read_job_id == metadata_compaction_id.as_str() => *owner,
            _ => {
                // The walk has left whatever job it was on, so the lease that
                // job's reap was holding may go now.
                self.delete_claimed_lease(store).await?;
                let owner =
                    claim_compaction_prefix(store, namespace_id, &metadata_compaction_id, now_ms)
                        .await?;
                self.last_read = Some((metadata_compaction_id.to_string(), owner));
                if owner == CompactionPrefixOwner::ThisCollector {
                    self.claimed_lease = Some(metadata_compaction_lease(
                        namespace_id,
                        &metadata_compaction_id,
                    ));
                }
                owner
            }
        };
        Ok(match owner {
            CompactionPrefixOwner::ALiveJob => StagedObject::OwnedByALiveJob,
            CompactionPrefixOwner::ThisCollector if self.claimed_lease.as_deref() == Some(key) => {
                StagedObject::ClaimedLease
            }
            CompactionPrefixOwner::ThisCollector | CompactionPrefixOwner::NoOne => {
                StagedObject::Orphaned
            }
        })
    }

    /// Deletes the lease of the prefix this pass claimed, now that the walk
    /// has left it.
    ///
    /// Called when the walk reaches another job and once more when it ends, so
    /// a pass never leaves behind a lease whose prefix it finished reaping. It
    /// is not aged: the claim is a compare-and-swap this pass won, which is a
    /// stronger statement about ownership than any timestamp, and the swap
    /// itself would have reset the object's write time anyway.
    pub(super) async fn finish<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        self.delete_claimed_lease(store).await
    }

    async fn delete_claimed_lease<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        let Some(object_key) = self.claimed_lease.take() else {
            return Ok(());
        };
        store
            .delete(&object_key)
            .await
            .map_err(|error| CoreError::store(&object_key, &error))
    }
}
