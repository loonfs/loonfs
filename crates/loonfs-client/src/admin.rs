//! Checkpoints, maintenance, store probes, and grep administration.

use super::*;
use crate::transport::{append_optional_pagination_query, append_query_param};

/// Fetches active-checkpoint pages as needed.
#[must_use]
pub struct CheckpointsPager {
    client: Client,
    namespace_id: NamespaceId,
    page_size: Option<u32>,
    cursor: Option<String>,
    pending: Option<ListCheckpointsResponse>,
    exhausted: bool,
}

impl CheckpointsPager {
    /// Returns the next checkpoint page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListCheckpointsResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .client
            .list_checkpoints_page(&self.namespace_id, self.page_size, self.cursor.as_deref())
            .await;
        Some(page.inspect(|page| {
            self.cursor = page.next_cursor.clone();
            self.exhausted = self.cursor.is_none();
        }))
    }

    /// Returns at most `max_items` checkpoints.
    ///
    /// Unused checkpoints from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<Checkpoint>> {
        let mut checkpoints = Vec::new();
        while checkpoints.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - checkpoints.len()).min(page.checkpoints.len());
            if take < page.checkpoints.len() {
                let remaining = page.checkpoints.split_off(take);
                checkpoints.extend(page.checkpoints);
                page.checkpoints = remaining;
                self.pending = Some(page);
                break;
            }
            checkpoints.extend(page.checkpoints);
        }
        Ok(checkpoints)
    }
}

impl Client {
    /// Returns namespace state and storage details used by maintenance.
    pub async fn get_namespace_diagnostics(
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
    /// Retrying this request starts a distinct attempt.
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: &CreateCheckpointRequest,
    ) -> Result<Checkpoint> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints",
            self.base_url
        );
        self.request_json_once(self.post(&url), Some(request)).await
    }

    /// Creates a checkpoint pager beginning at `cursor` (admin plane).
    pub fn list_checkpoints_pager(
        &self,
        namespace_id: &NamespaceId,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> CheckpointsPager {
        CheckpointsPager {
            client: self.clone(),
            namespace_id: namespace_id.clone(),
            page_size,
            cursor,
            pending: None,
            exhausted: false,
        }
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
    /// Retrying this request starts a distinct attempt.
    pub async fn run_maintenance(
        &self,
        namespace_id: &NamespaceId,
        request: &MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/maintenance/run",
            self.base_url
        );
        self.request_json_once(self.post(&url), Some(request)).await
    }

    /// Proves the server's backing store honours the object-store contract
    /// LoonFS depends on (admin plane).
    ///
    /// The probe writes and deletes objects under a scratch prefix, so it
    /// runs only when asked. A store that fails a check answers with that
    /// check reported failed rather than with an error: the probe ran, and
    /// the answer is that the store is wrong.
    /// Retrying this request starts a distinct attempt.
    pub async fn probe_store(&self, request: &StoreProbeRequest) -> Result<StoreProbeResponse> {
        let url = format!("{}/v0/admin/store/probe", self.base_url);
        self.request_json_once(self.post(&url), Some(request)).await
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
        let mut url = format!("{}/v0/namespaces/{namespace_id}/grep", self.base_url);
        let mut has_query = false;
        append_query_param(&mut url, &mut has_query, "pattern", &request.pattern);
        append_query_param(
            &mut url,
            &mut has_query,
            "case_insensitive",
            &request.case_insensitive.to_string(),
        );
        if let Some(path_prefix) = &request.path_prefix {
            append_query_param(
                &mut url,
                &mut has_query,
                "path_prefix",
                path_prefix.as_str(),
            );
        }
        append_query_param(
            &mut url,
            &mut has_query,
            "allow_scan",
            &request.allow_scan.to_string(),
        );
        append_query_param(
            &mut url,
            &mut has_query,
            "allow_stale",
            &request.allow_stale.to_string(),
        );
        append_optional_pagination_query(
            &mut url,
            &mut has_query,
            request.limit,
            request.cursor.as_deref(),
        );
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Returns whether the namespace's grep index is disabled, being built,
    /// or active. This operation does not change the index.
    pub async fn get_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.get(&url), None)
            .await
    }

    /// Enables the namespace's grep root (admin plane); embedded mode starts
    /// that namespace's event-driven backfill. Idempotent.
    pub async fn enable_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/enable",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.post(&url), None)
            .await
    }

    /// Disables the namespace's grep root (admin plane); garbage collection
    /// reclaims the segments. Idempotent.
    pub async fn disable_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/disable",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.post(&url), None)
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
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/gc",
            self.base_url
        );
        self.request_json_once(self.post(&url), Some(request)).await
    }
}
