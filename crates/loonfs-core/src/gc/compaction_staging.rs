//! Determines whether streaming-compaction output may be collected.
//!
//! A current group lease protects only the staged objects written by its
//! named job. Garbage collection claims expired leases before reclaiming that
//! job's output, which fences the job from publishing.

use crate::checkpoint::{
    claim_loaded_group_lease, load_group_lease, CompactionPrefixOwner, LoadedCompactionLease,
};
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::CompactionLeaseStatus;
use loonfs_api::{MetadataCompactionId, MetadataFamilyGroup, NamespaceId};
use loonfs_objectstore::keys::metadata_compaction_job_id_from_key;
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeMap;

/// What the loaded-lease sweep should do with one lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum GroupLeaseSweep {
    Retain,
    Delete { object_key: String },
}

/// Tracks the seven group leases and any claim awaiting prefix completion.
#[derive(Debug, Default)]
pub(super) struct CompactionLeases {
    by_job_id: BTreeMap<MetadataCompactionId, LoadedCompactionLease>,
    last_read: Option<LastPrefixRead>,
    claimed_lease: Option<String>,
    staging_complete: bool,
    loaded: bool,
}

#[derive(Debug)]
struct LastPrefixRead {
    job_id: MetadataCompactionId,
    owner: CompactionPrefixOwner,
}

impl CompactionLeases {
    /// Reads every deterministic group lease once for this pass.
    pub(super) async fn load_once<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
    ) -> Result<()> {
        if self.loaded {
            return Ok(());
        }
        let mut by_job_id = BTreeMap::new();
        for group in MetadataFamilyGroup::ALL {
            let Some(loaded) = load_group_lease(store, namespace_id, group).await? else {
                continue;
            };
            if by_job_id
                .insert(loaded.state.job_id.clone(), loaded)
                .is_some()
            {
                return Err(CoreError::NamespaceCorrupt(
                    "two metadata family-group leases name the same compaction job".to_owned(),
                ));
            }
        }
        self.by_job_id = by_job_id;
        self.loaded = true;
        Ok(())
    }

    /// Returns the confirmed owner of `key`'s job prefix.
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
                self.delete_claimed_lease(store).await?;
                let owner = match self.by_job_id.get(&metadata_compaction_id).cloned() {
                    Some(loaded) => {
                        let lease_key = loaded.object_key.clone();
                        let owner = claim_loaded_group_lease(
                            store,
                            namespace_id,
                            &metadata_compaction_id,
                            loaded,
                            now_ms,
                        )
                        .await?;
                        if owner == CompactionPrefixOwner::ThisCollector {
                            self.claimed_lease = Some(lease_key);
                        }
                        owner
                    }
                    None => CompactionPrefixOwner::NoOne,
                };
                self.last_read = Some(LastPrefixRead {
                    job_id: metadata_compaction_id,
                    owner,
                });
                owner
            }
        };
        Ok(Some(owner))
    }

    /// Records that the complete staging-prefix listing finished.
    pub(super) fn staging_finished(&mut self) {
        self.staging_complete = true;
    }

    pub(super) fn contains_group(&self, group: MetadataFamilyGroup) -> bool {
        self.by_job_id
            .values()
            .any(|loaded| loaded.state.group == group)
    }

    /// Decides one loaded lease after the staging prefix has been walked.
    pub(super) async fn sweep_group_lease<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        group: MetadataFamilyGroup,
        now_ms: u64,
    ) -> Result<Option<GroupLeaseSweep>> {
        let Some(loaded) = self
            .by_job_id
            .values()
            .find(|loaded| loaded.state.group == group)
            .cloned()
        else {
            return Ok(None);
        };
        if !self.staging_complete
            || (loaded.state.status == (CompactionLeaseStatus::Active {})
                && now_ms
                    <= loaded
                        .state
                        .heartbeat_at_ms
                        .saturating_add(crate::limits::METADATA_COMPACTION_LEASE_EXPIRY_MS))
        {
            return Ok(Some(GroupLeaseSweep::Retain));
        }
        let job_id = loaded.state.job_id.clone();
        let object_key = loaded.object_key.clone();
        let owner = claim_loaded_group_lease(store, namespace_id, &job_id, loaded, now_ms).await?;
        Ok(Some(match owner {
            CompactionPrefixOwner::ThisCollector => GroupLeaseSweep::Delete { object_key },
            CompactionPrefixOwner::LiveJob | CompactionPrefixOwner::NoOne => {
                GroupLeaseSweep::Retain
            }
        }))
    }

    /// Deletes a claimed lease after its job prefix has been processed.
    pub(super) async fn finish<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        self.delete_claimed_lease(store).await
    }

    async fn delete_claimed_lease<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        let Some(object_key) = self.claimed_lease.take() else {
            return Ok(());
        };
        self.by_job_id
            .retain(|_, loaded| loaded.object_key != object_key);
        store
            .delete(&object_key)
            .await
            .map_err(|error| CoreError::store(&object_key, &error))
    }
}
