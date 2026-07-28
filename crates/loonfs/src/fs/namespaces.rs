//! Namespace lifecycle: create, fork, and delete.

use super::core::{should_invalidate_after_result, FsCore};
use crate::{
    CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse, NamespaceId,
    NamespaceSummary,
};
use crate::{Result, RuntimeError};

impl FsCore {
    /// Creates a namespace, bootstrapping its durable state.
    ///
    /// With `options.allow_existing`, an already-existing namespace is
    /// treated as success.
    pub(crate) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(namespace_id)
            .bootstrap_namespace(loonfs_core::BootstrapOptions {
                allow_existing: options.allow_existing,
            })
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Forks `source` into `target` at the source's current head.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    pub(crate) async fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(source)
            .fork_namespace(target)
            .await
            .map_err(RuntimeError::from);
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace_cache(source);
        }
        if result.is_ok() {
            self.invalidate_namespace_cache(target);
        }
        result
    }

    /// Deletes a namespace: a fenced, terminal head transition (format
    /// spec, "Tombstones and deletion"). Commits acknowledged before the
    /// swap stay committed; reads, writes, forks, and re-creation of the id
    /// fail with `namespace_deleted` afterward. Deletion does not reclaim
    /// storage; reclamation is explicit garbage collection.
    ///
    /// Sequenced as a barrier through the publication service: mutations
    /// admitted before the delete publish first, and mutations admitted
    /// after it fail once it succeeds.
    pub(crate) async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.inner
            .publisher
            .submit_delete(namespace_id.clone(), options)
            .await
            .map_err(RuntimeError::Core)
    }

    /// The delete itself, run by the publication service once the barrier
    /// admits it, through the publisher's own commit engine: the session
    /// epoch and fencing that govern this namespace's publications govern
    /// its tombstone swap too. Only the service calls this; everything else
    /// must go through [`Self::delete_namespace`] so the barrier holds.
    pub(crate) async fn delete_namespace_with_engine(
        &self,
        namespace_id: &NamespaceId,
        engine: &mut loonfs_core::publish::NamespaceCommitEngine,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        let result = engine
            .delete_namespace(self.store(), options, &self.mutation_context()?)
            .await
            .map_err(RuntimeError::from);
        if result.is_ok() {
            // Only a namespace that is actually gone drops its cached state:
            // a failed delete (a fenced deleter, say) leaves the namespace
            // live, and its cached reads valid.
            self.invalidate_namespace_cache_for_delete(namespace_id);
        }
        result
    }
}
