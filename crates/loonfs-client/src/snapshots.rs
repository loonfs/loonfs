//! Snapshots: creating, listing, reading, extending, and deleting them.

use crate::operations::{self};
use crate::transport::QueryBuilder;
use crate::{Client, ClientError, Result};
use loonfs_types::options::CreateSnapshotOptions;
use loonfs_types::{
    CreateSnapshotRequest, DeleteSnapshotResponse, ExtendSnapshotRequest, ListSnapshotsResponse,
    Snapshot,
};
use loonfs_types::{NamespaceId, PageRequest, Pager, PinId};
use std::num::NonZeroU64;

/// A pager over snapshots.
pub type SnapshotsPager = Pager<ListSnapshotsResponse, ClientError>;

impl Client {
    /// Saves the namespace's current state as a snapshot that doesn't expire.
    ///
    /// See [`Self::create_snapshot_with_options`].
    pub async fn create_snapshot(
        &self,
        namespace_id: &NamespaceId,
        name: &str,
    ) -> Result<Snapshot> {
        self.create_snapshot_with_options(namespace_id, name, &CreateSnapshotOptions::default())
            .await
    }

    /// Saves the namespace's current state as a snapshot. With
    /// `options.ttl_ms`, the snapshot expires that many milliseconds from the
    /// server's current time, and without it, the snapshot doesn't expire.
    ///
    /// Retrying this request starts a distinct attempt.
    pub async fn create_snapshot_with_options(
        &self,
        namespace_id: &NamespaceId,
        name: &str,
        options: &CreateSnapshotOptions,
    ) -> Result<Snapshot> {
        let url = self.namespace_url(namespace_id, "/snapshots");
        self.send_json(self.request(operations::CREATE_SNAPSHOT, &url).json(
            &CreateSnapshotRequest {
                name: name.to_owned(),
                ttl_ms: options.ttl_ms,
            },
        )?)
        .await
    }

    /// Lists snapshots, including expired snapshots that garbage collection
    /// hasn't deleted.
    pub fn list_snapshots(&self, namespace_id: &NamespaceId) -> SnapshotsPager {
        Pager::with_state(
            (self.clone(), namespace_id.clone()),
            |(client, namespace_id), request| async move {
                client.snapshots_page(&namespace_id, request).await
            },
        )
    }

    async fn snapshots_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<String>,
    ) -> Result<ListSnapshotsResponse> {
        let mut query = QueryBuilder::new(self.namespace_url(namespace_id, "/snapshots"));
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        self.send_json(self.request(operations::LIST_SNAPSHOTS, &query.build()))
            .await
    }

    /// Returns one snapshot, as the listing returns it.
    ///
    /// A missing id returns `snapshot_not_found`.
    pub async fn get_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
    ) -> Result<Snapshot> {
        let url = self.namespace_url(namespace_id, &format!("/snapshots/{snapshot_id}"));
        self.send_json(self.request(operations::GET_SNAPSHOT, &url))
            .await
    }

    /// Extends an unexpired snapshot's lifetime to `ttl_ms` from the
    /// server's current time. The expiry never moves earlier.
    ///
    /// Repeating the same request is safe.
    pub async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
        ttl_ms: NonZeroU64,
    ) -> Result<Snapshot> {
        let url = self.namespace_url(namespace_id, &format!("/snapshots/{snapshot_id}/extend"));
        self.send_json(
            self.request(operations::EXTEND_SNAPSHOT, &url)
                .json(&ExtendSnapshotRequest { ttl_ms })?,
        )
        .await
    }

    /// Deletes the snapshot record.
    ///
    /// Deleting a missing snapshot succeeds, so repeating the request is
    /// safe.
    pub async fn delete_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
    ) -> Result<DeleteSnapshotResponse> {
        let url = self.namespace_url(namespace_id, &format!("/snapshots/{snapshot_id}"));
        self.send_json(self.request(operations::DELETE_SNAPSHOT, &url))
            .await
    }
}
