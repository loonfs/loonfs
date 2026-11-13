//! Namespace lifecycle: creating, reading, deleting, forking, merging, and
//! rebasing namespaces.

use crate::operations::{self};
use crate::transport::QueryBuilder;
use crate::{Client, Result};
use loonfs_types::options::{
    CreateNamespaceOptions, DeleteNamespaceOptions, ForkNamespaceOptions, MergeForkOptions,
};
use loonfs_types::{
    headers, ActorId, Commit, CommitId, CreateNamespaceRequest, DeleteNamespaceResponse, ErrorCode,
    ForkNamespaceForRebaseRequest, ForkNamespaceRequest, MergeForkRequest, NamespaceId,
    NamespaceMetadata, NextMergeRangeResponse, ReplayForkRequest, ReplayForkResponse,
};

impl Client {
    /// Creates an empty unrestricted namespace with the given ID and returns
    /// its genesis state.
    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<NamespaceMetadata> {
        self.create_namespace_with_options(namespace_id, actor, &CreateNamespaceOptions::default())
            .await
    }

    /// Creates an empty namespace with the given ID and options and returns
    /// its genesis state.
    ///
    /// With `options.allow_existing`, a namespace that already exists isn't
    /// an error, and the call returns that namespace's current state.
    pub async fn create_namespace_with_options(
        &self,
        namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &CreateNamespaceOptions,
    ) -> Result<NamespaceMetadata> {
        let url = format!("{}/v0/namespaces", self.base_url);
        // Namespace creation has no durable request identity to reconcile an ambiguous success.
        let created = self
            .send_json(
                self.request(operations::CREATE_NAMESPACE, &url)
                    .header(headers::ACTOR, actor.as_str())
                    .json(&CreateNamespaceRequest {
                        access: options.access.clone(),
                        namespace_id: Some(namespace_id.clone()),
                        retention_window_ms: options.retention_window_ms,
                    })?,
            )
            .await;
        match created {
            Err(error)
                if options.allow_existing && error.code() == Some(ErrorCode::NamespaceExists) =>
            {
                self.get_namespace(namespace_id).await
            }
            created => created,
        }
    }

    /// Returns the namespace's current state.
    pub async fn get_namespace(&self, namespace_id: &NamespaceId) -> Result<NamespaceMetadata> {
        // Validated namespace ids are URL-safe by construction, like the
        // other parsed id segments interpolated into paths here and below.
        let url = self.namespace_url(namespace_id, "");
        self.send_json(self.request(operations::GET_NAMESPACE, &url))
            .await
    }

    /// Deletes the namespace.
    ///
    /// See [`Self::delete_namespace_with_options`].
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DeleteNamespaceResponse> {
        self.delete_namespace_with_options(namespace_id, &DeleteNamespaceOptions::default())
            .await
    }

    /// Deletes the namespace.
    ///
    /// Set `options.expected_head_seq` to delete only if the namespace is
    /// still where you last observed it (`stale_head` on mismatch). Deleting
    /// an already-deleted namespace fails with `namespace_deleted`.
    pub async fn delete_namespace_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        let mut query = QueryBuilder::new(self.namespace_url(namespace_id, ""));
        if let Some(expected) = options.expected_head_seq {
            query.push("expected_head_seq", expected.0);
        }
        // The expected head is a precondition, not an idempotency key for an ambiguous delete.
        self.send_json(self.request(operations::DELETE_NAMESPACE, &query.build()))
            .await
    }

    /// Creates a new namespace from the source's current head and returns the
    /// target's state at the fork point.
    pub async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<NamespaceMetadata> {
        self.fork_namespace_with_options(
            source_namespace_id,
            new_namespace_id,
            actor,
            &ForkNamespaceOptions::default(),
        )
        .await
    }

    /// Creates a new namespace from the selected current head or unexpired snapshot and
    /// returns the target's state at the fork point.
    pub async fn fork_namespace_with_options(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &ForkNamespaceOptions,
    ) -> Result<NamespaceMetadata> {
        let url = self.namespace_url(source_namespace_id, "/forks");
        // Namespace forks have no durable request identity to replay after an ambiguous success.
        self.send_json(
            self.request(operations::FORK_NAMESPACE, &url)
                .header(headers::ACTOR, actor.as_str())
                .json(&ForkNamespaceRequest {
                    new_namespace_id: Some(new_namespace_id.clone()),
                    snapshot_id: options.snapshot_id.clone(),
                })?,
        )
        .await
    }

    /// Merges the fork `source_namespace_id`'s commits into `namespace_id` as
    /// one commit, through the fork's head.
    pub async fn merge_fork(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.merge_fork_with_options(
            namespace_id,
            source_namespace_id,
            actor,
            &MergeForkOptions::default(),
        )
        .await
    }

    /// Merges the fork `source_namespace_id`'s commits into `namespace_id` as
    /// one commit, through `options.through_seq` or the fork's head, under
    /// `options.commit_id` or a generated commit ID.
    pub async fn merge_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<Commit> {
        let url = self.namespace_url(namespace_id, "/merges");
        // The request's commit ID resolves an ambiguous resend through the
        // merge's durable receipt.
        self.send_json(
            self.request(operations::MERGE_FORK, &url)
                .header(headers::ACTOR, actor.as_str())
                .json(&MergeForkRequest {
                    source_namespace_id: source_namespace_id.clone(),
                    through_seq: options.through_seq,
                    commit_id: options.commit_id.clone().unwrap_or_else(CommitId::generate),
                    message: options.message.clone(),
                })?,
        )
        .await
    }

    /// Returns where the fork `source_namespace_id`'s next merge into
    /// `namespace_id` starts, without planning the merge. When a retention
    /// floor would refuse the merge with `below_retention_floor`, the
    /// response's outcome says which floor.
    pub async fn next_merge_range(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
    ) -> Result<NextMergeRangeResponse> {
        let mut query = QueryBuilder::new(self.namespace_url(namespace_id, "/merges"));
        query.push("source", source_namespace_id.as_str());
        self.send_json(self.request(operations::GET_NEXT_MERGE_RANGE, &query.build()))
            .await
    }

    /// Creates `new_namespace_id` as a rebase's new fork of `namespace_id`'s
    /// head for the fork `source_namespace_id`, and returns the new fork's
    /// state at its fork point. A `new_namespace_id` that already exists
    /// returns `namespace_exists`.
    pub async fn fork_namespace_for_rebase(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<NamespaceMetadata> {
        let url = self.namespace_url(namespace_id, "/rebases");
        // Namespace forks have no durable request identity to replay after an ambiguous success.
        self.send_json(
            self.request(operations::FORK_NAMESPACE_FOR_REBASE, &url)
                .header(headers::ACTOR, actor.as_str())
                .json(&ForkNamespaceForRebaseRequest {
                    source_namespace_id: source_namespace_id.clone(),
                    new_namespace_id: Some(new_namespace_id.clone()),
                })?,
        )
        .await
    }

    /// Replays the fork `source_namespace_id`'s unmerged commits into
    /// `namespace_id`, a rebase's new fork for the fork, as one replay
    /// commit, through the fork's head.
    pub async fn replay_fork(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<ReplayForkResponse> {
        self.replay_fork_with_options(
            namespace_id,
            source_namespace_id,
            actor,
            &MergeForkOptions::default(),
        )
        .await
    }

    /// Replays the fork `source_namespace_id`'s unmerged commits into
    /// `namespace_id`, a rebase's new fork for the fork, as one replay
    /// commit, through `options.through_seq` or the fork's head, under
    /// `options.commit_id` or a generated commit ID. The response's conflict
    /// report is absent when the commit ID identifies a replay commit that an
    /// earlier request committed.
    pub async fn replay_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<ReplayForkResponse> {
        let url = self.namespace_url(namespace_id, "/replays");
        // The request's commit ID resolves an ambiguous resend through the
        // replay commit's durable receipt.
        self.send_json(
            self.request(operations::REPLAY_FORK, &url)
                .header(headers::ACTOR, actor.as_str())
                .json(&ReplayForkRequest {
                    source_namespace_id: source_namespace_id.clone(),
                    through_seq: options.through_seq,
                    commit_id: options.commit_id.clone().unwrap_or_else(CommitId::generate),
                    message: options.message.clone(),
                })?,
        )
        .await
    }
}
