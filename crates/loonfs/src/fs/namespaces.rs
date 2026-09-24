//! Namespace lifecycle: create, fork, and delete.

use super::core::{should_invalidate_after_result, ReadCore, WriterBits};
use crate::maintenance::namespace_reclaim_at_ms;
use crate::{
    CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse, ForkNamespaceOptions,
    Namespace, NamespaceId,
};
use crate::{FsWriter, MaintenanceHint, MaintenanceJobId};
use crate::{Result, RuntimeError};

impl FsWriter {
    /// Fork, delete, and snapshot management belong to the token holder and
    /// to administrators of an ACL namespace.
    pub(super) async fn require_administrator(&self, namespace_id: &NamespaceId) -> Result<()> {
        if self.core.subject.is_none() {
            return Ok(());
        }
        let (engine, context) = self.core.pinned_metadata_read(namespace_id).await?;
        Ok(engine.require_administrator(&context).await?)
    }

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
    ) -> Result<Namespace> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .bootstrap_namespace(loonfs_core::BootstrapOptions {
                actor_id: options.actor_id,
                access: options.access,
                allow_existing: options.allow_existing,
            })
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Forks `source_namespace_id` into `new_namespace_id` at the selected current head or live snapshot.
    pub async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        options: ForkNamespaceOptions,
    ) -> Result<Namespace> {
        self.fork_namespace_with(source_namespace_id, new_namespace_id, options)
            .await
    }

    /// Forks `source_namespace_id` into `new_namespace_id` at the selected current head or live snapshot.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.fork_namespace",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "fork_namespace",
            namespace_id = %source_namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn fork_namespace_with(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        options: ForkNamespaceOptions,
    ) -> Result<Namespace> {
        self.require_administrator(source_namespace_id).await?;
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(source_namespace_id)
            .fork_namespace(
                new_namespace_id,
                &options.actor_id,
                options.snapshot_id.as_ref(),
            )
            .await
            .map_err(RuntimeError::from);
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace(source_namespace_id);
        }
        if result.is_ok() {
            self.invalidate_namespace(new_namespace_id);
        }
        result
    }

    /// Deletes a namespace: a fenced, terminal head transition (format
    /// spec, "Tombstones and deletion"). Commits acknowledged before the
    /// swap stay committed; reads, writes, forks, and re-creation of the id
    /// fail with `namespace_deleted` afterward. Repeated garbage collection
    /// reclaims the namespace's own content after retirement and its grace
    /// period. See the API spec, "Deleting, retaining, and reclaiming", for
    /// blockers and limits.
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
        self.require_administrator(namespace_id).await?;
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
    writer: &WriterBits,
    namespace_id: &NamespaceId,
    engine: &mut loonfs_core::publish::NamespaceCommitEngine,
    options: DeleteNamespaceOptions,
) -> Result<DeleteNamespaceResponse> {
    let context = writer.identity.mutation_context()?;
    let result = engine
        .delete_namespace(core.store(), options, &context)
        .await
        .map_err(RuntimeError::from);
    if result.is_ok() {
        // Only a namespace that is actually gone drops its cached state:
        // a failed delete (a fenced deleter, say) leaves the namespace
        // live, and its cached reads valid.
        core.invalidate_namespace_read_cache(namespace_id);
        writer.send_maintenance_hint(
            namespace_id,
            MaintenanceHint::DueAt {
                namespace_id: namespace_id.clone(),
                job: MaintenanceJobId::GC,
                not_before_ms: namespace_reclaim_at_ms(context.now_ms),
            },
        );
    }
    result
}
