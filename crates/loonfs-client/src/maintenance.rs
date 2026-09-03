//! Snapshots, checkpoints, maintenance, store probes, and grep maintenance.

use super::*;
use crate::transport::{QueryBuilder, SendPolicy};

/// A pager over active checkpoints.
pub type CheckpointsPager = loonfs_api::Pager<ListCheckpointsResponse, ClientError>;
/// A pager over live snapshots.
pub type SnapshotsPager = loonfs_api::Pager<ListSnapshotsResponse, ClientError>;

impl Client {
    /// Saves the namespace's current state for a limited time.
    /// Retrying this request starts a distinct attempt.
    pub async fn create_snapshot(
        &self,
        namespace_id: &NamespaceId,
        name: &str,
        ttl_ms: u64,
    ) -> Result<SnapshotSummary> {
        let url = format!("{}/v0/namespaces/{namespace_id}/snapshots", self.base_url);
        self.request_json(
            self.post(&url),
            Some(&CreateSnapshotRequest {
                name: name.to_owned(),
                ttl_ms,
            }),
            SendPolicy::Once,
        )
        .await
    }

    /// Creates a snapshot pager beginning at `cursor`.
    pub fn list_snapshots_pager(
        &self,
        namespace_id: &NamespaceId,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> SnapshotsPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move {
                client
                    .list_snapshots_page(&namespace_id, page_size, cursor.as_deref())
                    .await
            }
        })
    }

    /// Lists one bounded page of available snapshots.
    pub async fn list_snapshots_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListSnapshotsResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/snapshots",
            self.base_url
        ));
        query.pagination(limit, cursor);
        let url = query.finish();
        self.request_json::<(), ListSnapshotsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Extends a snapshot's lifetime. Repeating the same request is safe.
    pub async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
        ttl_ms: u64,
    ) -> Result<SnapshotSummary> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
            self.base_url
        );
        self.request_json(
            self.post(&url),
            Some(&ExtendSnapshotRequest { ttl_ms }),
            SendPolicy::Retry,
        )
        .await
    }

    /// Releases a snapshot. Releasing it again succeeds.
    pub async fn release_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
    ) -> Result<ReleaseSnapshotResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/release",
            self.base_url
        );
        self.request_json::<(), ReleaseSnapshotResponse>(self.post(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns namespace state and storage details used by maintenance.
    pub async fn get_namespace_diagnostics(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceDiagnostics> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/diagnostics",
            self.base_url
        );
        self.request_json::<(), NamespaceDiagnostics>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Creates a named, user-owned checkpoint pinning the namespace's
    /// current view (maintenance plane). Every call creates a new checkpoint; the
    /// name is a label, not a key. This is a maintenance operation, not a
    /// file mutation. The record is a garbage-collection root until released
    /// or expired.
    /// Retrying this request starts a distinct attempt.
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: &CreateCheckpointRequest,
    ) -> Result<Checkpoint> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/checkpoints",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request), SendPolicy::Once)
            .await
    }

    /// Creates a checkpoint pager beginning at `cursor` (maintenance plane).
    pub fn list_checkpoints_pager(
        &self,
        namespace_id: &NamespaceId,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> CheckpointsPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move {
                client
                    .list_checkpoints_page(&namespace_id, page_size, cursor.as_deref())
                    .await
            }
        })
    }

    /// Lists one bounded page of active checkpoint records (maintenance plane).
    pub async fn list_checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListCheckpointsResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/checkpoints",
            self.base_url
        ));
        query.pagination(limit, cursor);
        let url = query.finish();
        self.request_json::<(), ListCheckpointsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Releases a user-owned checkpoint pin by id (maintenance plane). Idempotent:
    /// releasing an already-released or reaped record succeeds.
    pub async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}/release",
            self.base_url
        );
        self.request_json::<(), ReleaseCheckpointResponse>(self.post(&url), None, SendPolicy::Retry)
            .await
    }

    /// Runs one maintenance job against a namespace (maintenance plane).
    /// Retrying this request starts a distinct attempt.
    pub async fn run_maintenance(
        &self,
        namespace_id: &NamespaceId,
        request: &MaintenanceRunRequest,
    ) -> Result<MaintenanceRunResponse> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/runs",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request), SendPolicy::Once)
            .await
    }

    /// Proves the server's backing store honours the object-store contract
    /// LoonFS depends on (maintenance plane).
    ///
    /// The probe writes and deletes objects under a scratch prefix, so it
    /// runs only when asked. A store that fails a check answers with that
    /// check reported failed rather than with an error: the probe ran, and
    /// the answer is that the store is wrong.
    /// Retrying this request starts a distinct attempt.
    pub async fn probe_store(&self, request: &StoreProbeRequest) -> Result<StoreProbeResponse> {
        let url = format!("{}/v0/maintenance/store/probe", self.base_url);
        self.request_json(self.post(&url), Some(request), SendPolicy::Once)
            .await
    }

    /// Content search over the namespace's grep index (query plane).
    /// Gate on the `query.grep` capability before calling against unknown
    /// deployments; the namespace must also have a materialized active
    /// grep root or the server answers `not_supported`.
    pub async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
        limit: Option<u32>,
    ) -> Result<GrepResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/grep",
            self.base_url
        ));
        query.push("pattern", &request.pattern);
        query.push("case_insensitive", request.case_insensitive);
        if let Some(path_prefix) = &request.path_prefix {
            query.push("path_prefix", path_prefix.as_str());
        }
        query.push("allow_scan", request.allow_scan);
        query.push("allow_stale", request.allow_stale);
        query.pagination(limit, request.cursor.as_deref());
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns whether the namespace's grep index is disabled, being built,
    /// or active. This operation does not change the index.
    pub async fn get_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Enables the namespace's grep root (maintenance plane); embedded mode starts
    /// that namespace's event-driven backfill. Idempotent.
    pub async fn enable_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index/enable",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.post(&url), None, SendPolicy::Retry)
            .await
    }

    /// Disables the namespace's grep root (maintenance plane); garbage collection
    /// reclaims the segments. Idempotent.
    pub async fn disable_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index/disable",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.post(&url), None, SendPolicy::Retry)
            .await
    }

    /// Runs one explicit grep-index garbage-collection pass for a namespace.
    ///
    /// `max_objects` bounds the reads the pass spends; when keys remain the
    /// response carries a `next_cursor` to resume from.
    /// Retrying this request starts a distinct attempt.
    pub async fn gc_grep_index(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepGcRequest,
    ) -> Result<GrepGcResponse> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index/gc",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request), SendPolicy::Once)
            .await
    }
}
