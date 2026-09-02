//! Determines whether streaming-compaction output may be collected.
//!
//! A current lease protects every object under a compaction job's prefix. To
//! collect an expired job, the collector first changes its lease from `Active`
//! to `Reaping` with compare-and-swap. This fences the job from publishing and
//! lets later passes safely resume collection.

use crate::checkpoint::{claim_compaction_prefix, CompactionPrefixOwner};
use crate::error::{CoreError, Result};
use loonfs_api::{MetadataCompactionId, NamespaceId};
use loonfs_objectstore::keys::{metadata_compaction_job_id_from_key, metadata_compaction_lease};
use loonfs_objectstore::ObjectStore;

/// Tracks the confirmed owner of the current job prefix and any claimed lease
/// that must be deleted after that prefix is processed.
#[derive(Debug, Default)]
pub(super) struct CompactionLeases {
    last_read: Option<LastPrefixRead>,
    /// A successfully claimed lease, retained until all earlier staged objects
    /// in the same job prefix have been processed.
    claimed_lease: Option<String>,
}

#[derive(Debug)]
struct LastPrefixRead {
    job_id: MetadataCompactionId,
    owner: CompactionPrefixOwner,
}

impl CompactionLeases {
    /// Returns the confirmed owner of `key`'s job prefix, or `None` when the
    /// key names no job. One claim per job prefix is reused for every key
    /// under it.
    pub(super) async fn owner_of<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        key: &str,
        now_ms: u64,
    ) -> Result<Option<CompactionPrefixOwner>> {
        let Some(metadata_compaction_id) = metadata_compaction_job_id_from_key(key)
            .and_then(|job_id| MetadataCompactionId::parse(job_id).ok())
        else {
            return Ok(None);
        };
        let owner = match &self.last_read {
            Some(read) if read.job_id == metadata_compaction_id => read.owner,
            _ => {
                // A different job prefix has started, so the previously
                // claimed lease can now be deleted.
                self.delete_claimed_lease(store).await?;
                let owner =
                    claim_compaction_prefix(store, namespace_id, &metadata_compaction_id, now_ms)
                        .await?;
                if owner == CompactionPrefixOwner::ThisCollector {
                    self.claimed_lease = Some(metadata_compaction_lease(
                        namespace_id,
                        &metadata_compaction_id,
                    ));
                }
                self.last_read = Some(LastPrefixRead {
                    job_id: metadata_compaction_id,
                    owner,
                });
                owner
            }
        };
        Ok(Some(owner))
    }

    /// Whether `key` is the lease this collector claimed for the current job.
    pub(super) fn is_claimed_lease(&self, key: &str) -> bool {
        self.claimed_lease.as_deref() == Some(key)
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
