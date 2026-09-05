//! Determines whether streaming-compaction output may be collected.
//!
//! A current group lease protects only the staged objects written by its
//! named job. Garbage collection claims expired leases before reclaiming that
//! job's output, which fences the job from publishing.

use crate::checkpoint::{
    claim_loaded_group_lease, load_group_lease, load_output_protection, CompactionPrefixOwner,
    LoadedCompactionLease,
};
use crate::error::{CoreError, Result};
use loonfs_api::{MetadataCompactionId, MetadataFamilyGroup, NamespaceId};
use loonfs_objectstore::keys::metadata_compaction_job_id_from_key;
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeMap;

/// Tracks the seven group leases and the last inspected job prefix.
#[derive(Debug, Default)]
pub(super) struct CompactionLeases {
    by_job_id: BTreeMap<MetadataCompactionId, LoadedCompactionLease>,
    last_read: Option<LastPrefixRead>,
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
                let protection =
                    load_output_protection(store, namespace_id, &metadata_compaction_id).await?;
                let owner = if protection.is_some_and(|loaded| now_ms <= loaded.state.expires_at_ms)
                {
                    CompactionPrefixOwner::Protected
                } else if let Some(loaded) = self.by_job_id.get(&metadata_compaction_id).cloned() {
                    claim_loaded_group_lease(
                        store,
                        namespace_id,
                        &metadata_compaction_id,
                        loaded,
                        now_ms,
                    )
                    .await?
                } else {
                    CompactionPrefixOwner::Unclaimed
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
}
