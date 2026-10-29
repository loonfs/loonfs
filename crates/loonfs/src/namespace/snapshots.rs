//! Snapshot reads and mutations.

use crate::pagers::SnapshotsPager;
use crate::publisher::NamespaceChange;
use crate::trace::{self, operation_span};
use crate::{
    CreateSnapshotOptions, DeleteSnapshotResponse, Error, ListSnapshotsResponse, Namespace, Result,
    Snapshot, Writable,
};
use loonfs_core::SnapshotPageCursor;
use loonfs_types::PageRequest;
use loonfs_types::PinId;
use std::num::NonZeroU64;

/// Returns the expiry of a snapshot whose lifetime of `ttl_ms` starts at
/// `now_ms`.
fn expires_at_ms(now_ms: u64, ttl_ms: NonZeroU64) -> u64 {
    now_ms.saturating_add(ttl_ms.get())
}

impl<M: crate::Mode> Namespace<M> {
    /// Returns one snapshot, as the listing returns it, including an expired
    /// snapshot that garbage collection hasn't deleted. A missing id returns
    /// `snapshot_not_found`.
    pub async fn get_snapshot(&self, snapshot_id: &PinId) -> Result<Snapshot> {
        trace::in_operation(
            operation_span!(self.core, "get_snapshot", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.ensure_administrator().await?;
                self.core
                    .reader_engine(&self.namespace_id)
                    .get_snapshot(snapshot_id)
                    .await
                    .map_err(Error::from)
            },
        )
        .await
    }

    /// Lists snapshots in ascending id order, including expired snapshots
    /// that garbage collection hasn't deleted.
    pub fn list_snapshots(&self) -> SnapshotsPager {
        loonfs_types::Pager::with_state(self.read_only(), |reader, request| async move {
            reader
                .snapshots_page(crate::pagers::decode_page_request(request)?)
                .await
        })
    }

    async fn snapshots_page(
        &self,
        request: PageRequest<SnapshotPageCursor>,
    ) -> Result<ListSnapshotsResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_snapshots", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.ensure_administrator().await?;
                let page = self
                    .core
                    .reader_engine(&self.namespace_id)
                    .list_snapshots_page(request)
                    .await
                    .map_err(Error::from)?;
                Ok(ListSnapshotsResponse {
                    namespace_id: self.namespace_id.clone(),
                    snapshots: page.items,
                    next_cursor: crate::pagers::encode_next_cursor(page.next_cursor.as_ref())?,
                })
            },
        )
        .await
    }
}

impl Namespace<Writable> {
    /// Creates a snapshot of the current namespace state that doesn't expire.
    ///
    /// See [`Self::create_snapshot_with_options`].
    pub async fn create_snapshot(&self, name: &str) -> Result<Snapshot> {
        self.create_snapshot_with_options(name, &CreateSnapshotOptions::default())
            .await
    }

    /// Creates a snapshot of the current namespace state. The name is a
    /// label that doesn't need to be unique, and every call creates a new
    /// snapshot with a new id.
    ///
    /// A snapshot created with `options.ttl_ms` expires that many
    /// milliseconds from now. After that it can't be read or forked, and it
    /// stays listed until garbage collection deletes it. A snapshot without a
    /// TTL doesn't expire.
    ///
    /// Folds the WAL tail first when it isn't folded, and waits for a fold
    /// permit from the runtime's execution budget either way.
    pub async fn create_snapshot_with_options(
        &self,
        name: &str,
        options: &CreateSnapshotOptions,
    ) -> Result<Snapshot> {
        trace::in_operation(
            operation_span!(self.core, "create_snapshot", self.namespace_id),
            async {
                let expires_at_ms = options
                    .ttl_ms
                    .map(|ttl_ms| expires_at_ms(self.core.now_ms(), ttl_ms));
                self.ensure_administrator().await?;
                let engine = self.engine();
                let result = {
                    let _permit = self.core.fold_permit().await;
                    engine.create_snapshot(name.to_owned(), expires_at_ms).await
                }
                .map_err(Error::from);
                self.mode
                    .publisher
                    .finish_publication(&self.namespace_id, result)
                    .await
            },
        )
        .await
    }

    /// Moves an unexpired snapshot's expiry to `ttl_ms` from now. The expiry
    /// never moves earlier, so a snapshot without an expiry is returned
    /// unchanged. An expired or missing snapshot returns
    /// `snapshot_not_found`.
    pub async fn extend_snapshot(
        &self,
        snapshot_id: &PinId,
        ttl_ms: NonZeroU64,
    ) -> Result<Snapshot> {
        trace::in_operation(operation_span!(self.core, "extend_snapshot", self.namespace_id, snapshot_id = %snapshot_id), async {
        let requested_expires_at_ms = expires_at_ms(self.core.now_ms(), ttl_ms);
        self.ensure_administrator().await?;
        let result = self.engine()
            .extend_snapshot(snapshot_id, requested_expires_at_ms)
            .await
            .map_err(Error::from);
        self.mode
            .publisher
            .finish_namespace_mutation(&self.namespace_id, result, |_| NamespaceChange::Unchanged)
            .await
    }).await
    }

    /// Deletes a snapshot. Deleting a missing snapshot succeeds, so a retry
    /// returns the same response. The id of a fork pin returns
    /// `snapshot_not_found`.
    ///
    /// Deletion works after the namespace is deleted, so a snapshot without
    /// an expiry can still be released.
    pub async fn delete_snapshot(&self, snapshot_id: &PinId) -> Result<DeleteSnapshotResponse> {
        trace::in_operation(operation_span!(self.core, "delete_snapshot", self.namespace_id, snapshot_id = %snapshot_id), async {
        self.ensure_administrator().await?;
        let result = self.engine()
            .delete_snapshot(snapshot_id)
            .await
            .map_err(Error::from);
        self.mode
            .publisher
            .finish_namespace_mutation(&self.namespace_id, result, |_| NamespaceChange::Unchanged)
            .await
    }).await
    }
}
