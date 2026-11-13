//! The snapshot inventory, maintenance jobs, store probes, and grep
//! maintenance.

use crate::operations;
use crate::transport::QueryBuilder;
use crate::{Client, ClientError, Result};
use loonfs_types::options::GcOptions;
use loonfs_types::{
    headers, ActorId, AdvanceRetentionRequest, AdvanceRetentionResponse, GcRequest, GcResponse,
    ListSnapshotRecordsResponse, MetadataMaintenanceRequest, MetadataMaintenanceResponse,
    NamespaceDiagnostics, NamespaceId, PageRequest, Pager, PoolGcResponse, PoolId, PrincipalId,
    RecoverAdministratorRequest, RecoverAdministratorResponse, RunMaintenanceJobRequest,
    RunMaintenanceJobResponse, RunPoolMaintenanceJobRequest, RunPoolMaintenanceJobResponse,
    SetRetentionWindowRequest, SetRetentionWindowResponse, SnapshotId, SnapshotRecord,
};
use loonfs_types::{GrepIndex, StoreProbeRequest, StoreProbeResponse};
use std::num::NonZeroU64;

/// A pager over existing snapshots.
pub type SnapshotRecordsPager = Pager<ListSnapshotRecordsResponse, ClientError>;

/// Returns the response of the job that a typed maintenance method asked
/// for, or a protocol error when the server ran a different one.
macro_rules! job_response {
    ($response:expr, $job:ident::$variant:ident) => {
        match $response {
            $job::$variant(response) => Ok(response),
            _ => Err(ClientError::Protocol(format!(
                "the server returned another job's response to a `{}` maintenance job",
                stringify!($variant)
            ))),
        }
    };
}

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
        self.send_json(self.request(operations::GET_NAMESPACE_DIAGNOSTICS, &url))
            .await
    }

    /// Lists every snapshot, including fork snapshots and expired
    /// snapshots that garbage collection has not yet deleted (maintenance API
    /// group).
    pub fn list_snapshot_records(&self, namespace_id: &NamespaceId) -> SnapshotRecordsPager {
        Pager::with_state(
            (self.clone(), namespace_id.clone()),
            |(client, namespace_id), request| async move {
                client.snapshot_records_page(&namespace_id, request).await
            },
        )
    }

    async fn snapshot_records_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<String>,
    ) -> Result<ListSnapshotRecordsResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/snapshots",
            self.base_url
        ));
        query.page(&request);
        self.send_json(self.request(operations::LIST_SNAPSHOT_RECORDS, &query.build()))
            .await
    }

    /// Returns one existing snapshot of any owner, as the snapshot inventory returns it,
    /// through the maintenance API. A missing id returns
    /// `snapshot_not_found`.
    pub async fn get_snapshot_record(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &SnapshotId,
    ) -> Result<SnapshotRecord> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/snapshots/{snapshot_id}",
            self.base_url
        );
        self.send_json(self.request(operations::GET_SNAPSHOT_RECORD, &url))
            .await
    }

    /// Runs one maintenance job against a namespace (maintenance API group).
    ///
    /// An administrator recovery needs an actor, so recover an administrator
    /// with [`Self::recover_administrator`] instead. Retrying this request
    /// starts a distinct attempt.
    pub async fn run_maintenance_job(
        &self,
        namespace_id: &NamespaceId,
        request: &RunMaintenanceJobRequest,
    ) -> Result<RunMaintenanceJobResponse> {
        self.run_maintenance_as(namespace_id, request, None).await
    }

    async fn run_maintenance_as(
        &self,
        namespace_id: &NamespaceId,
        request: &RunMaintenanceJobRequest,
        actor: Option<&ActorId>,
    ) -> Result<RunMaintenanceJobResponse> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/jobs",
            self.base_url
        );
        let mut wire_request = self.request(operations::RUN_MAINTENANCE_JOB, &url);
        if let Some(actor) = actor {
            wire_request = wire_request.header(headers::ACTOR, actor.as_str());
        }
        self.send_json(wire_request.json(request)?).await
    }

    /// Folds the WAL tail when it's due, and compacts metadata until nothing
    /// is due or the job reaches its work limit.
    ///
    /// See [`Self::maintain_metadata_while_due_with_options`].
    pub async fn maintain_metadata_while_due(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<MetadataMaintenanceResponse> {
        self.maintain_metadata_while_due_with_options(
            namespace_id,
            &MetadataMaintenanceRequest::default(),
        )
        .await
    }

    /// Folds the WAL tail when it reaches the length that `options` sets, and
    /// compacts metadata until nothing is due or the job reaches its work
    /// limit.
    ///
    /// The runtime's options for this job include a compaction policy that
    /// the wire request has no field for, so this method takes the request
    /// body. Retrying this request starts a distinct attempt.
    pub async fn maintain_metadata_while_due_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &MetadataMaintenanceRequest,
    ) -> Result<MetadataMaintenanceResponse> {
        let response = self
            .run_maintenance_job(
                namespace_id,
                &RunMaintenanceJobRequest::Metadata(options.clone()),
            )
            .await?;
        job_response!(response, RunMaintenanceJobResponse::Metadata)
    }

    /// Runs one garbage collection of a namespace with the server's
    /// default grace window.
    ///
    /// See [`Self::gc_with_options`].
    pub async fn gc(&self, namespace_id: &NamespaceId) -> Result<GcResponse> {
        let response = self
            .run_maintenance_job(
                namespace_id,
                &RunMaintenanceJobRequest::Gc(GcRequest::default()),
            )
            .await?;
        job_response!(response, RunMaintenanceJobResponse::Gc)
    }

    /// Runs one garbage collection of a namespace with the grace
    /// window that `options` sets.
    ///
    /// Retrying this request starts a distinct attempt.
    pub async fn gc_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &GcOptions,
    ) -> Result<GcResponse> {
        let response = self
            .run_maintenance_job(
                namespace_id,
                &RunMaintenanceJobRequest::Gc(GcRequest {
                    grace_window_ms: Some(options.grace_window_ms),
                }),
            )
            .await?;
        job_response!(response, RunMaintenanceJobResponse::Gc)
    }

    /// Runs one garbage collection of a pool with the server's default grace
    /// window.
    ///
    /// See [`Self::gc_pool_with_options`].
    pub async fn gc_pool(&self, pool_id: &PoolId) -> Result<PoolGcResponse> {
        let response = self
            .run_pool_maintenance_job(
                pool_id,
                &RunPoolMaintenanceJobRequest::Gc(GcRequest::default()),
            )
            .await?;
        job_response!(response, RunPoolMaintenanceJobResponse::Gc)
    }

    /// Runs one garbage collection of a pool with the grace window that
    /// `options` sets.
    ///
    /// A pool with no pool record returns `pool_not_found`. Retrying this
    /// request starts a distinct attempt.
    pub async fn gc_pool_with_options(
        &self,
        pool_id: &PoolId,
        options: &GcOptions,
    ) -> Result<PoolGcResponse> {
        let response = self
            .run_pool_maintenance_job(
                pool_id,
                &RunPoolMaintenanceJobRequest::Gc(GcRequest {
                    grace_window_ms: Some(options.grace_window_ms),
                }),
            )
            .await?;
        job_response!(response, RunPoolMaintenanceJobResponse::Gc)
    }

    async fn run_pool_maintenance_job(
        &self,
        pool_id: &PoolId,
        request: &RunPoolMaintenanceJobRequest,
    ) -> Result<RunPoolMaintenanceJobResponse> {
        let url = format!("{}/v0/maintenance/pools/{pool_id}/jobs", self.base_url);
        self.send_json(
            self.request(operations::RUN_POOL_MAINTENANCE_JOB, &url)
                .json(request)?,
        )
        .await
    }

    /// Advances the namespace's retention floor toward the folded manifest
    /// head.
    ///
    /// See [`Self::advance_retention_floor_with_options`].
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        self.advance_retention_floor_with_options(namespace_id, &AdvanceRetentionRequest::default())
            .await
    }

    /// Advances the namespace's retention floor toward the target that
    /// `options` sets.
    ///
    /// The floor doesn't pass a commit that's less than one hour old. The
    /// runtime's options for this job include targets that the wire request
    /// can't express, so this method takes the request body. Retrying this
    /// request starts a distinct attempt.
    pub async fn advance_retention_floor_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &AdvanceRetentionRequest,
    ) -> Result<AdvanceRetentionResponse> {
        let response = self
            .run_maintenance_job(
                namespace_id,
                &RunMaintenanceJobRequest::Retention(options.clone()),
            )
            .await?;
        job_response!(response, RunMaintenanceJobResponse::Retention)
    }

    /// Sets the namespace's retention window, or clears it when
    /// `retention_window_ms` is `None`, so that the server's maintenance
    /// sweep doesn't advance the namespace's floor.
    ///
    /// Retrying this request starts a distinct attempt.
    pub async fn set_retention_window(
        &self,
        namespace_id: &NamespaceId,
        retention_window_ms: Option<NonZeroU64>,
    ) -> Result<SetRetentionWindowResponse> {
        let response = self
            .run_maintenance_job(
                namespace_id,
                &RunMaintenanceJobRequest::RetentionWindow(SetRetentionWindowRequest {
                    retention_window_ms,
                }),
            )
            .await?;
        job_response!(response, RunMaintenanceJobResponse::RetentionWindow)
    }

    /// Grants `admin` on the root to `principal_id`, through a commit that
    /// `actor` makes and that doesn't check any subject's rights.
    ///
    /// Retrying this request starts a distinct attempt.
    pub async fn recover_administrator(
        &self,
        namespace_id: &NamespaceId,
        principal_id: &PrincipalId,
        actor: &ActorId,
    ) -> Result<RecoverAdministratorResponse> {
        let response = self
            .run_maintenance_as(
                namespace_id,
                &RunMaintenanceJobRequest::RecoverAdministrator(RecoverAdministratorRequest {
                    principal_id: principal_id.clone(),
                }),
                Some(actor),
            )
            .await?;
        job_response!(response, RunMaintenanceJobResponse::RecoverAdministrator)
    }

    /// Proves the server's backing store honours the object-store contract
    /// LoonFS depends on (maintenance API group).
    ///
    /// The probe writes and deletes objects under a scratch prefix, so it
    /// runs only when asked. A store that fails a check reports that
    /// check reported failed rather than with an error: the probe ran, and
    /// the result is that the store is wrong.
    /// Retrying this request starts a distinct attempt.
    pub async fn probe_store(&self, request: &StoreProbeRequest) -> Result<StoreProbeResponse> {
        let url = format!("{}/v0/maintenance/store/probe", self.base_url);
        self.send_json(self.request(operations::PROBE_STORE, &url).json(request)?)
            .await
    }

    /// Returns whether the namespace's grep index is disabled, being built, or
    /// active (maintenance API group).
    ///
    /// This operation does not change the index.
    pub async fn get_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index",
            self.base_url
        );
        self.send_json(self.request(operations::GET_GREP_INDEX, &url))
            .await
    }

    /// Enables the namespace's grep index (maintenance API group).
    ///
    /// The server's maintenance builds the index later; this call does not
    /// wait for it. Idempotent.
    pub async fn enable_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index/enable",
            self.base_url
        );
        self.send_json(self.request(operations::ENABLE_GREP_INDEX, &url))
            .await
    }

    /// Disables the namespace's grep index (maintenance API group); garbage
    /// collection reclaims the segments.
    ///
    /// Idempotent.
    pub async fn disable_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let url = format!(
            "{}/v0/maintenance/namespaces/{namespace_id}/grep/index/disable",
            self.base_url
        );
        self.send_json(self.request(operations::DISABLE_GREP_INDEX, &url))
            .await
    }
}
