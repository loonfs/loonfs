//! Determines whether staged output from a streaming compaction can be collected.
//!
//! A job writes its output under `metadata/compactions/{job_id}/` and refreshes
//! its lease with compare-and-swap while it runs (format spec, "Garbage
//! collection", rule 12). A current lease means every object under the job's
//! prefix must be retained. An expired lease is not sufficient evidence for
//! collection because the job may resume. The collector must first change the
//! lease from `Active` to `Reaping` with compare-and-swap. After that succeeds,
//! the job's next heartbeat fails and it can no longer publish the staged
//! output, so those objects can be treated as unreferenced.
//!
//! A job's lease sorts before its `segments/` directory. The collector reads the
//! lease first and deletes it only after processing the staged segments. If the
//! pass stops midway through the prefix, the `Reaping` lease remains so a
//! later pass can safely continue.
//!
//! Objects for one job are contiguous in key order. The pass caches the
//! confirmed ownership result for the current job, reducing lease reads from
//! one per object to one per job. It caches only a current `Active` lease or a
//! successful claim, never an expired lease that was only observed.

use crate::checkpoint::{claim_compaction_prefix, CompactionPrefixOwner};
use crate::error::{CoreError, Result};
use loonfs_api::{MetadataCompactionId, NamespaceId};
use loonfs_objectstore::keys::{metadata_compaction_job_id_from_key, metadata_compaction_lease};
use loonfs_objectstore::ObjectStore;

/// Tracks the confirmed owner of the current job prefix and any claimed lease
/// that must be deleted after that prefix is processed.
#[derive(Debug, Default)]
pub(super) struct CompactionLeases {
    /// The most recently checked job and the confirmed owner of its prefix.
    last_read: Option<(String, CompactionPrefixOwner)>,
    /// A successfully claimed lease, retained until all earlier staged objects
    /// in the same job prefix have been processed.
    claimed_lease: Option<String>,
}

/// What the sweep should do with one key under the compaction prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StagedObject {
    /// The job has a current lease, so the object must be retained.
    OwnedByALiveJob,
    /// No active job can publish the object. Apply the normal grace period for
    /// unreferenced objects.
    Orphaned,
    /// The collector claimed this lease. Delete it after processing the
    /// remaining objects in the job prefix.
    ClaimedLease,
    /// The key is neither a recognized lease nor a staged segment, so the
    /// collector retains it.
    UnrecognizedKey,
}

impl CompactionLeases {
    /// Returns the collection state for `key`, reusing the current job's
    /// confirmed lease state when possible.
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
                // A different job prefix has started, so the previously
                // claimed lease can now be deleted.
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

    /// Deletes a claimed lease after its job prefix has been processed.
    ///
    /// This runs when the pass reaches another job and again when the pass
    /// ends. The lease does not need an age check because this collector
    /// already claimed it with compare-and-swap.
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
