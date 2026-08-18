//! Namespace lifecycle: create, fork, and delete.

use super::core::{should_invalidate_after_result, ReadCore, WriterIdentity};
use crate::FsWriter;
use crate::{
    CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse, NamespaceId,
    NamespaceStatusResponse,
};
use crate::{Result, RuntimeError};

impl FsWriter {
    /// Creates a namespace, bootstrapping its durable state.
    ///
    /// With `options.allow_existing`, an already-existing namespace is
    /// treated as success. Returns the namespace's post-operation status.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.create_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "create_namespace",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceStatusResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .bootstrap_namespace(loonfs_core::BootstrapOptions {
                allow_existing: options.allow_existing,
            })
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Forks `source` into `target` at the source's current head.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history,
    /// and the response reports the target at the fork point.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.fork_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "fork_namespace",
            namespace_id = %source,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceStatusResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(source)
            .fork_namespace(target)
            .await
            .map_err(RuntimeError::from);
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace(source);
        }
        if result.is_ok() {
            self.invalidate_namespace(target);
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
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.delete_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "delete_namespace",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.publisher
            .submit_delete(namespace_id.clone(), options)
            .await
    }
}

/// The delete itself, run by the publication service once the barrier
/// admits it, through the publisher's own commit engine: the session
/// epoch and fencing that govern this namespace's publications govern
/// its tombstone swap too. Only the service calls this; everything else
/// must go through [`FsWriter::delete_namespace`] so the barrier holds.
pub(crate) async fn delete_namespace_with_engine(
    core: &ReadCore,
    actor: &WriterIdentity,
    namespace_id: &NamespaceId,
    engine: &mut loonfs_core::publish::NamespaceCommitEngine,
    options: DeleteNamespaceOptions,
) -> Result<DeleteNamespaceResponse> {
    let result = engine
        .delete_namespace(core.store(), options, &actor.mutation_context()?)
        .await
        .map_err(RuntimeError::from);
    if result.is_ok() {
        // Only a namespace that is actually gone drops its cached state:
        // a failed delete (a fenced deleter, say) leaves the namespace
        // live, and its cached reads valid.
        core.invalidate_namespace_cache_for_delete(namespace_id);
    }
    result
}
