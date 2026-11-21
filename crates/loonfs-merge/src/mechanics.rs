//! The server operations that the merge loop uses, and their implementation
//! by the HTTP client.

use loonfs_client::{Client, Result, SnapshotRecordsPager};
use loonfs_types::options::MergeForkOptions;
use loonfs_types::{
    ActorId, Commit, DeleteNamespaceResponse, NamespaceId, NamespaceMetadata,
    NextMergeRangeResponse, ReplayForkResponse,
};
use std::future::Future;

/// The server operations that a [`MergeSession`](crate::MergeSession) sends,
/// with one method for each operation.
///
/// Each method has the signature of the [`Client`] method with the same name,
/// with two differences: the three methods that send `Loonfs-Actor` take the
/// actor that the session's state records, and the rebase's new fork ID is
/// required, because the session chooses it. [`Client`] implements the trait
/// by calling that method on a clone with that actor (see
/// [`Client::with_actor`]). A test implements the trait with answers that it
/// scripts.
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

    /// Creates `new_namespace_id` as a rebase's new fork of `namespace_id`
    /// for the fork `source_namespace_id` (see
    /// [`Client::fork_namespace_for_rebase`]).
    fn fork_namespace_for_rebase(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> impl Future<Output = Result<NamespaceMetadata>> + Send;

    /// Replays the fork `source_namespace_id`'s unmerged commits into
    /// `namespace_id`, a rebase's new fork (see
    /// [`Client::replay_fork_with_options`]).
    fn replay_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> impl Future<Output = Result<ReplayForkResponse>> + Send;

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

    /// Lists the namespace's snapshots, whose fork snapshots identify the namespace's
    /// forks (see [`Client::list_snapshot_records`]).
    fn list_snapshot_records(&self, namespace_id: &NamespaceId) -> SnapshotRecordsPager;
}

impl Mechanics for Client {
    async fn merge_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<Commit> {
        self.with_actor(actor.clone())
            .merge_fork_with_options(namespace_id, source_namespace_id, options)
            .await
    }

    async fn fork_namespace_for_rebase(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<NamespaceMetadata> {
        self.with_actor(actor.clone())
            .fork_namespace_for_rebase(namespace_id, source_namespace_id, Some(new_namespace_id))
            .await
    }

    async fn replay_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<ReplayForkResponse> {
        self.with_actor(actor.clone())
            .replay_fork_with_options(namespace_id, source_namespace_id, options)
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

    fn list_snapshot_records(&self, namespace_id: &NamespaceId) -> SnapshotRecordsPager {
        Client::list_snapshot_records(self, namespace_id)
    }
}
