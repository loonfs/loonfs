//! Checkpoints, maintenance, store probes, and grep administration.

use super::*;
use crate::transport::append_optional_pagination_query;

impl Client {
    /// Returns namespace state and storage details used by maintenance.
    pub async fn namespace_diagnostics(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceDiagnostics> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/diagnostics",
            self.base_url
        );
        self.request_json::<(), NamespaceDiagnostics>(self.get(&url), None)
            .await
    }

    /// Creates a named, user-owned checkpoint pinning the namespace's
    /// current view (admin plane). Every call creates a new checkpoint; the
    /// name is a label, not a key. This is a maintenance operation, not a
    /// file mutation. The record is a garbage-collection root until released
    /// or expired.
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: &CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Lists every active checkpoint record by following bounded pages
    /// (admin plane).
    ///
    /// A checkpoint name is a label rather than a key, so this is how a pin
    /// is found again once its creation response is gone. An expired record
    /// that no collection pass has released yet is still listed, with its
    /// expiry in the entry.
    pub async fn list_checkpoints_all(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ListCheckpointsResponse> {
        let first_page = self.list_checkpoints_page(namespace_id, None, None).await?;
        let mut response = ListCheckpointsResponse {
            namespace_id: first_page.namespace_id,
            checkpoints: first_page.checkpoints,
            next_cursor: None,
        };
        let mut next_cursor = first_page.next_cursor;
        while let Some(cursor) = next_cursor {
            let page = self
                .list_checkpoints_page(namespace_id, None, Some(&cursor))
                .await?;
            response.checkpoints.extend(page.checkpoints);
            next_cursor = page.next_cursor;
        }
        Ok(response)
    }

    /// Lists one bounded page of active checkpoint records (admin plane).
    pub async fn list_checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListCheckpointsResponse> {
        let mut url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints",
            self.base_url
        );
        let mut has_query = false;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListCheckpointsResponse>(self.get(&url), None)
            .await
    }

    /// Releases a user-owned checkpoint pin by id (admin plane). Idempotent:
    /// releasing an already-released or reaped record succeeds.
    pub async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints/{checkpoint_id}/release",
            self.base_url
        );
        self.request_json::<(), ReleaseCheckpointResponse>(self.post(&url), None)
            .await
    }

    /// Runs one bounded maintenance step against a namespace (admin plane).
    ///
    /// The request selects the actions by naming them, and a request that
    /// names none is rejected. Absent overrides inside a selected action use
    /// the server's defaults.
    pub async fn maintenance_step(
        &self,
        namespace_id: &NamespaceId,
        request: &MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/maintenance/step",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Proves the server's backing store honours the object-store contract
    /// LoonFS depends on (admin plane).
    ///
    /// The probe writes and deletes objects under a scratch prefix, so it
    /// runs only when asked. A store that fails a check answers with that
    /// check reported failed rather than with an error: the probe ran, and
    /// the answer is that the store is wrong.
    pub async fn probe_store(&self, request: &StoreProbeRequest) -> Result<StoreProbeResponse> {
        let url = format!("{}/v0/admin/store/probe", self.base_url);
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Content search over the namespace's grep index (query plane).
    /// Gate on the `query.grep` capability before calling against unknown
    /// deployments; the namespace must also have a materialized active
    /// grep root or the server answers `not_supported`.
    pub async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse> {
        let url = format!("{}/v0/namespaces/{namespace_id}/query/grep", self.base_url);
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Reads the namespace's grep-index lifecycle (admin plane): disabled,
    /// backfilling toward a captured sequence, or active at a watermark.
    /// One grep root read on the server, with no side effects.
    pub async fn grep_index_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index",
            self.base_url
        );
        self.request_json::<(), GrepIndexStatusResponse>(self.get(&url), None)
            .await
    }

    /// Enables the namespace's grep root (admin plane); embedded mode starts
    /// that namespace's event-driven backfill. Idempotent.
    pub async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/enable",
            self.base_url
        );
        self.request_json::<(), GrepIndexStatusResponse>(self.post(&url), None)
            .await
    }

    /// Disables the namespace's grep root (admin plane); garbage collection
    /// reclaims the segments. Idempotent.
    pub async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/disable",
            self.base_url
        );
        self.request_json::<(), GrepIndexStatusResponse>(self.post(&url), None)
            .await
    }

    /// Runs one explicit grep-index garbage-collection pass for a namespace.
    ///
    /// `max_objects` bounds the reads the pass spends; when keys remain the
    /// response carries a `next_cursor` to resume from.
    pub async fn gc_grep_index(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepGcRequest,
    ) -> Result<GrepGcResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/gc",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request)).await
    }
}
