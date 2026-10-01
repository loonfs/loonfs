//! Checkpoints, maintenance, store probes, and grep maintenance.

use super::*;
use crate::transport::{QueryBuilder, SendPolicy};
use loonfs_types::{ActorId, PageRequest};

/// A pager over existing checkpoints.
pub type CheckpointsPager = loonfs_types::Pager<ListCheckpointsResponse, ClientError>;

impl Client {
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

    /// Creates a user-owned checkpoint pinning the namespace's current view
    /// (maintenance API group).
    ///
    /// Every call creates a new checkpoint; the name is a label, not a key.
    /// This is a maintenance operation, not a file mutation. The record keeps
    /// its manifest retained until it is deleted, either explicitly or by
    /// garbage collection after expiry plus grace. Retrying this request
    /// starts a distinct attempt.
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

    /// Lists existing checkpoints, including expired checkpoints that garbage
    /// collection has not yet deleted (maintenance API group).
    pub fn list_checkpoints(&self, namespace_id: &NamespaceId) -> CheckpointsPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_types::Pager::new(move |request| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move { client.checkpoints_page(&namespace_id, request).await }
        })
    }

    async fn checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<String>,
    ) -> Result<ListCheckpointsResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/checkpoints",
            self.base_url
        ));
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        let url = query.finish();
        self.request_json::<(), ListCheckpointsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Deletes a user-owned checkpoint pin through the maintenance API.
    /// A missing id returns `checkpoint_not_found`.
    pub async fn delete_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &PinId,
    ) -> Result<DeleteCheckpointResponse> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}",
            self.base_url
        );
        self.request_json::<(), DeleteCheckpointResponse>(self.delete(&url), None, SendPolicy::Once)
            .await
    }

    /// Runs one maintenance job against a namespace (maintenance API group).
    /// `actor_id` attributes the administrator recovery commit when supplied.
    /// Retrying this request starts a distinct attempt.
    pub async fn run_maintenance(
        &self,
        namespace_id: &NamespaceId,
        request: &RunMaintenanceRequest,
        actor_id: Option<&ActorId>,
    ) -> Result<RunMaintenanceResponse> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/runs",
            self.base_url
        );
        let mut wire_request = self.post(&url);
        if let Some(actor_id) = actor_id {
            wire_request = wire_request.header("Loonfs-Actor", actor_id.as_str());
        }
        self.request_json(wire_request, Some(request), SendPolicy::Once)
            .await
    }

    /// Proves the server's backing store honours the object-store contract
    /// LoonFS depends on (maintenance API group).
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

    /// Returns whether the namespace's grep index is disabled, being built,
    /// or active (maintenance API group). This operation does not change the
    /// index.
    pub async fn get_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Enables the namespace's grep index (maintenance API group).
    ///
    /// The server asks its maintenance runner to start the backfill; this call
    /// does not wait for it. Idempotent.
    pub async fn enable_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index/enable",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.post(&url), None, SendPolicy::Retry)
            .await
    }

    /// Disables the namespace's grep index (maintenance API group); garbage
    /// collection reclaims the segments. Idempotent.
    pub async fn disable_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index/disable",
            self.base_url
        );
        self.request_json::<(), GrepIndex>(self.post(&url), None, SendPolicy::Retry)
            .await
    }
}
