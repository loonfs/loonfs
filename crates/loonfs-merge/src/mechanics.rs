//! The server operations that the merge loop uses, and their implementation
//! by the HTTP client.

use loonfs_client::{CheckpointsPager, Client, Result};
use loonfs_types::options::MergeForkOptions;
use loonfs_types::{
    ActorId, Commit, DeleteNamespaceResponse, NamespaceId, NamespaceMetadata,
    NextMergeRangeResponse, RebaseForkResponse,
};
use std::future::Future;

/// The server operations that a [`MergeSession`](crate::MergeSession) sends,
/// with one method for each operation.
///
/// Each method has the signature of the [`Client`] method with the same name,
/// and [`Client`] implements the trait by calling that method. A test
/// implements it with answers that it scripts.
pub trait Mechanics: Send + Sync {
    /// Merges the fork `source_namespace_id`'s commits into `namespace_id`
    /// (see [`Client::merge_fork_with_options`]).
    fn merge_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> impl Future<Output = Result<Commit>> + Send;

    /// Replays the fork `source_namespace_id`'s unmerged commits into
    /// `new_namespace_id` (see [`Client::rebase_fork_with_options`]).
    fn rebase_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> impl Future<Output = Result<RebaseForkResponse>> + Send;

    /// Returns where the fork `source_namespace_id`'s next merge into
    /// `namespace_id` starts (see [`Client::next_merge_range`]).
    fn next_merge_range(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
    ) -> impl Future<Output = Result<NextMergeRangeResponse>> + Send;

    /// Deletes the namespace (see [`Client::delete_namespace`]).
    fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> impl Future<Output = Result<DeleteNamespaceResponse>> + Send;

    /// Returns the namespace's current state, whose fork basis identifies
    /// the fork that a rebase replayed into it (see [`Client::get_namespace`]).
    fn get_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> impl Future<Output = Result<NamespaceMetadata>> + Send;

    /// Lists the namespace's pins, whose fork pins identify the namespace's
    /// forks (see [`Client::list_checkpoints`]).
    fn list_checkpoints(&self, namespace_id: &NamespaceId) -> CheckpointsPager;
}

impl Mechanics for Client {
    async fn merge_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<Commit> {
        Client::merge_fork_with_options(self, namespace_id, source_namespace_id, actor, options)
            .await
    }

    async fn rebase_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<RebaseForkResponse> {
        Client::rebase_fork_with_options(
            self,
            namespace_id,
            source_namespace_id,
            new_namespace_id,
            actor,
            options,
        )
        .await
    }

    async fn next_merge_range(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
    ) -> Result<NextMergeRangeResponse> {
        Client::next_merge_range(self, namespace_id, source_namespace_id).await
    }

    async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DeleteNamespaceResponse> {
        Client::delete_namespace(self, namespace_id).await
    }

    async fn get_namespace(&self, namespace_id: &NamespaceId) -> Result<NamespaceMetadata> {
        Client::get_namespace(self, namespace_id).await
    }

    fn list_checkpoints(&self, namespace_id: &NamespaceId) -> CheckpointsPager {
        Client::list_checkpoints(self, namespace_id)
    }
}
