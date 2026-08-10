//! Deciding one streaming compaction's objects: the lease says whether the
//! job that wrote them still owns them.
//!
//! A job writes its output under `metadata/compactions/{job_id}/` and rewrites
//! its lease beside it while it runs (format spec, "Garbage collection", rule
//! 12). A pass sweeping that prefix therefore asks one question per object:
//! does the job that owns this key still hold its lease? A fresh lease retains
//! the whole prefix however old the objects under it are, and a stale or
//! missing one leaves them to ordinary orphan collection.
//!
//! The lease of a job sorts before that job's `tables/` directory and a job's
//! objects are contiguous, so remembering the answer for the job being walked
//! turns one lease read per object into one lease read per job. The memo is
//! not authority: a resumed pass simply reads the lease again.

use crate::checkpoint::{read_compaction_lease_state, CompactionLeaseState};
use crate::error::Result;
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::metadata_compaction_job_id_from_key;
use loonfs_objectstore::ObjectStore;

/// What one pass has learned about the compaction job it is walking.
#[derive(Debug, Default)]
pub(super) struct CompactionLeases {
    /// The job this pass last read a lease for, and what that lease said.
    last_read: Option<(String, CompactionLeaseState)>,
}

/// What the sweep should do with one key under the compaction prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StagedObject {
    /// A live job owns it. Nothing about its age matters.
    OwnedByALiveJob,
    /// No job owns it: it belongs to a job that finished, died, or never
    /// wrote a lease. It ages out as an ordinary unreferenced orphan.
    Orphaned,
    /// The key is under the prefix but is neither a job's lease nor one of
    /// its staged segments, so this pass cannot tell what wrote it and
    /// retains it.
    UnrecognizedKey,
}

impl CompactionLeases {
    /// Reads `key`'s owning job, and the lease of that job unless this pass
    /// has already read it.
    pub(super) async fn owner_of<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        key: &str,
        now_ms: u64,
    ) -> Result<StagedObject> {
        let Some(job_id) = metadata_compaction_job_id_from_key(key) else {
            return Ok(StagedObject::UnrecognizedKey);
        };
        let state = match &self.last_read {
            Some((read_job_id, state)) if read_job_id == job_id => *state,
            _ => {
                let state =
                    read_compaction_lease_state(store, namespace_id, job_id, now_ms).await?;
                self.last_read = Some((job_id.to_owned(), state));
                state
            }
        };
        Ok(match state {
            CompactionLeaseState::Held => StagedObject::OwnedByALiveJob,
            CompactionLeaseState::Gone => StagedObject::Orphaned,
        })
    }
}
