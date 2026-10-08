//! Namespace lifecycle: create, fork, and delete.

use super::core::{should_invalidate_after_result, RuntimeCore, WriterBits};
use crate::{
    ActorId, CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse,
    ForkNamespaceOptions, NamespaceId,
};
use crate::{Error, Result};
use crate::{ErrorCode, LoonFs, Namespace, Writable};

impl LoonFs<Writable> {
    /// Fork, delete, and snapshot management belong to the token holder and
    /// to administrators of an ACL namespace.
    pub(super) async fn require_administrator(&self, namespace_id: &NamespaceId) -> Result<()> {
        if self.core.subject.is_none() {
            return Ok(());
        }
        let (engine, context) = self.core.pinned_metadata_read(namespace_id).await?;
        Ok(engine.require_administrator(&context).await?)
    }

    /// Creates an unrestricted, case-insensitive namespace, bootstrapping its
    /// durable state. An existing namespace is an error.
    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<crate::NamespaceMetadata> {
        self.create_namespace_with_options(namespace_id, actor, &CreateNamespaceOptions::default())
            .await
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
    pub async fn create_namespace_with_options(
        &self,
        namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &CreateNamespaceOptions,
    ) -> Result<crate::NamespaceMetadata> {
        self.core.record_trace_context(&tracing::Span::current());
        let result = self
            .engine(namespace_id)
            .bootstrap_namespace(actor, options)
            .await
            .map_err(Error::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Forks `source_namespace_id` into `new_namespace_id` at its current head.
    pub async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<crate::NamespaceMetadata> {
        self.fork_namespace_with_options(
            source_namespace_id,
            new_namespace_id,
            actor,
            &ForkNamespaceOptions::default(),
        )
        .await
    }

    /// Forks `source_namespace_id` into `new_namespace_id` at the selected current head or live snapshot.
    ///
    /// A fork of the current head folds the source's WAL tail first when it
    /// is not folded, and waits for a fold permit from the runtime's
    /// execution budget either way. A fork of a snapshot never folds and
    /// takes no fold permit.
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
    pub async fn fork_namespace_with_options(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &ForkNamespaceOptions,
    ) -> Result<crate::NamespaceMetadata> {
        self.require_administrator(source_namespace_id).await?;
        self.core.record_trace_context(&tracing::Span::current());
        let result = {
            let _permit = match options.snapshot_id {
                Some(_) => None,
                None => Some(self.mode.bits.execution_budget.fold_permit().await),
            };
            self.engine(source_namespace_id)
                .fork_namespace(new_namespace_id, actor, options.snapshot_id.as_ref())
                .await
        }
        .map_err(Error::from);
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace(source_namespace_id);
        }
        if result.is_ok() {
            self.invalidate_namespace(new_namespace_id);
        }
        result
    }
}

impl Namespace<Writable> {
    /// Delete and snapshot management belong to the token holder and to
    /// administrators of an ACL namespace.
    pub(super) async fn require_administrator(&self) -> Result<()> {
        if self.core.subject.is_none() {
            return Ok(());
        }
        let (engine, context) = self.core.pinned_metadata_read(&self.namespace_id).await?;
        Ok(engine.require_administrator(&context).await?)
    }

    /// Ends the namespace after folding its final WAL tail, whatever its
    /// head.
    pub async fn delete(&self) -> Result<DeleteNamespaceResponse> {
        self.delete_with_options(&DeleteNamespaceOptions::default())
            .await
    }

    /// Ends the namespace after folding its final WAL tail.
    /// See [namespace deletion](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#94-deleting-a-namespace).
    ///
    /// Sequenced as a barrier through the publication service: mutations
    /// admitted before the delete publish first, and mutations admitted
    /// after it fail once it succeeds.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.delete",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "delete",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn delete_with_options(
        &self,
        options: &DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.require_administrator().await?;
        self.core.record_trace_context(&tracing::Span::current());
        self.session().submit_delete(*options).await
    }
}

/// The delete itself, run by the publication service once the barrier
/// admits it, through the publisher's own commit engine: the session
/// epoch and fencing that govern this namespace's publications govern
/// its tombstone swap too. Only the service calls this; everything else
/// must go through [`Namespace::delete`] so the barrier holds.
pub(crate) async fn delete_namespace_with_engine(
    core: &RuntimeCore,
    writer: &WriterBits,
    namespace_id: &NamespaceId,
    engine: &mut loonfs_core::publish::NamespaceCommitEngine,
    options: DeleteNamespaceOptions,
) -> Result<DeleteNamespaceResponse> {
    let context = core.mutation_context(&writer.identity)?;
    let result = engine
        .delete_namespace(core.store(), options, &context)
        .await
        .map_err(Error::from);
    // What follows a deletion depends on the namespace being deleted, not on
    // which call deleted it.
    if result
        .as_ref()
        .err()
        .is_none_or(|error| error.code() == ErrorCode::NamespaceDeleted)
    {
        core.invalidate_namespace_read_cache(namespace_id);
    }
    result
}
