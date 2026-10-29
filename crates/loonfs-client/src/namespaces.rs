//! Namespace lifecycle: creating, reading, deleting, and forking namespaces.

use crate::operations::{self};
use crate::transport::QueryBuilder;
use crate::{Client, Result};
use loonfs_types::options::{CreateNamespaceOptions, DeleteNamespaceOptions, ForkNamespaceOptions};
use loonfs_types::{
    headers, ActorId, CreateNamespaceRequest, DeleteNamespaceResponse, ErrorCode,
    ForkNamespaceRequest, NamespaceId, NamespaceMetadata,
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
                        namespace_id: namespace_id.clone(),
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
                    new_namespace_id: new_namespace_id.clone(),
                    snapshot_id: options.snapshot_id.clone(),
                })?,
        )
        .await
    }
}
