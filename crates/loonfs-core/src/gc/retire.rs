//! Content and source-pin cleanup for an eligible tombstone.

use super::live_set::{LiveSet, RetirementStatus};
use super::sweep::Decision;
use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use futures::TryStreamExt;
use loonfs_objectstore::keys::content_prefix;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::GcResponse;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Deletes the content prefix of a family root's pool, which stores the
    /// content of its whole fork family, and the source pin of a fork's basis,
    /// once the namespace is eligible for retirement. Returns whether the
    /// collection listed the content prefix.
    ///
    /// A family root is retired after every other member of its family, so a
    /// fork's retirement deletes no content.
    pub(super) async fn retire_namespace(
        &self,
        live: &LiveSet,
        report: &mut GcResponse,
    ) -> Result<bool> {
        if live.retirement_status() != RetirementStatus::Eligible {
            return Ok(false);
        }
        let tombstone = live
            .current_tombstone
            .as_ref()
            .expect("eligible namespace should have a tombstone");
        let content_listed = tombstone.fork_basis.is_none();
        if content_listed {
            self.sweep_content(tombstone, report).await?;
        }
        if let Some(basis) = &tombstone.fork_basis {
            if self.delete_source_pin(basis).await? {
                report.deleted_checkpoints_by_owner.fork += 1;
            }
        }
        Ok(content_listed)
    }

    async fn sweep_content(
        &self,
        tombstone: &NamespaceManifestPayload,
        report: &mut GcResponse,
    ) -> Result<()> {
        let prefix = content_prefix(&tombstone.pool_id);
        let mut listed_bytes = 0;
        let decisions = self
            .store
            .list_entries_from_stream(&prefix, None)
            .map_err(|error| CoreError::store(&prefix, &error))
            .map_ok(|entry| {
                listed_bytes += entry.size_bytes;
                Decision::Delete {
                    key: entry.key,
                    counter: |report| &mut report.deleted.retired_content_objects,
                }
            });
        self.apply_decisions(decisions, report).await?;
        report.listed_bytes += listed_bytes;
        Ok(())
    }
}
