//! Blocking HTTP client for a LoonFS server.
//!
//! Use this crate when your process should talk to a hosted LoonFS runtime
//! instead of embedding the runtime directly. The client keeps paths simple:
//! pass a [`NamespacePath`] for filesystem operations and use explicit commit
//! helpers when you need retry control.
//!
//! Hosts that want to stay transport-agnostic should program against the
//! [`backend::Backend`] trait instead of [`Client`] directly.

pub mod backend;
mod config;
mod error;
mod transport;

use loonfs_api::{
    v0::{
        BeginUploadRequest, BeginUploadResponse, ChangesResponse,
        CommitResponse as ApiCommitResponse, CommitSubmissionRequest, CompleteUploadRequest,
        CompleteUploadResponse, DisableGramsIndexResponse, EnableGramsIndexResponse,
        GrepGcResponse, ObjectTransferAccess, RepairNamespaceResponse, UploadContentResponse,
        UploadMode, ValidatedContentToken,
    },
    AbsolutePath, AdvanceRetentionResponse, AuthoritativePathEntry, CapabilityDocument, ChangeSeq,
    CommitId, ContentRef, CreateCheckpointRequest, CreateCheckpointResponse,
    CreateNamespaceRequest, DeleteDirectoryBehavior, DeleteNamespaceResponse, DestinationBehavior,
    ErrorCode, FilesystemOperation, FilesystemOperationRequest, FlushWalResponse,
    ForkNamespaceRequest, GcRequest, GcResponse, GrepRequest, GrepResponse, InodeId,
    ListFileRevisionsResponse, ListPathEntriesResponse, MaintenanceTickRequest,
    MaintenanceTickResponse, NamespaceId, NamespaceStatusResponse, NamespaceSummary,
    ReleaseCheckpointResponse, RestoreFileRevisionRequest, RevisionNo,
};
use std::sync::{Arc, OnceLock};

pub use config::ClientConfig;
pub use error::ClientError;
use transport::IO_INACTIVITY_TIMEOUT;

/// Synchronous HTTP client for LoonFS.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    auth_token: Option<String>,
    agent: ureq::Agent,
    /// Whether transient server errors are retried (see
    /// [`ClientConfig::disable_transient_retry`]).
    transient_retry: bool,
    /// Capability document cache, shared by clones and filled on first use.
    capabilities: Arc<OnceLock<CapabilityDocument>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StagedContent {
    content_ref: ContentRef,
    validated_content_token: Option<ValidatedContentToken>,
}

/// A path qualified by namespace.
///
/// Both parts are validated at construction — [`NamespacePath::parse`] for
/// strings, [`NamespacePath::new`] for already-typed parts — so a value of
/// this type always names a well-formed target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePath {
    namespace: NamespaceId,
    absolute_path: AbsolutePath,
}

/// Options shared by every client mutation, mirroring the runtime's
/// per-operation options structs.
#[derive(Debug, Clone, Default)]
pub struct MutationOptions {
    /// Idempotency key for the commit; retrying with the same id replays
    /// the committed mutation instead of double-committing. A fresh id is
    /// generated when absent.
    pub commit_id: Option<CommitId>,
}

impl MutationOptions {
    /// Retry with a caller-chosen idempotency key.
    pub fn with_commit_id(commit_id: CommitId) -> Self {
        Self {
            commit_id: Some(commit_id),
        }
    }

    fn resolve_commit_id(&self) -> CommitId {
        self.commit_id.clone().unwrap_or_else(CommitId::generate)
    }
}

impl Client {
    /// Creates a client, validating the config exactly as
    /// [`ClientConfig::load`] does — direct Rust construction cannot bypass
    /// validation.
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        config.validate()?;
        let mut agent = ureq::AgentBuilder::new()
            .timeout_read(IO_INACTIVITY_TIMEOUT)
            .timeout_write(IO_INACTIVITY_TIMEOUT);
        if let Some(timeout_ms) = config.request_timeout_ms {
            agent = agent.timeout(std::time::Duration::from_millis(timeout_ms));
        }
        Ok(Self {
            base_url: config.server_url.trim().trim_end_matches('/').to_owned(),
            auth_token: config.auth_token,
            agent: agent.build(),
            transient_retry: !config.disable_transient_retry,
            capabilities: Arc::new(OnceLock::new()),
        })
    }

    /// Returns the server's capability document, fetched once and cached for
    /// the life of this client and its clones (API spec, "Capability
    /// discovery").
    ///
    /// Feature keys that are not parented by an advertised profile are
    /// dropped rather than trusted, per the spec's client guidance for
    /// malformed documents.
    pub fn capabilities(&self) -> Result<CapabilityDocument, ClientError> {
        if let Some(document) = self.capabilities.get() {
            return Ok(document.clone());
        }
        let url = format!("{}/v0/config", self.base_url);
        let mut document: CapabilityDocument =
            self.request_json::<(), _>(self.agent.get(&url), None)?;
        document.retain_well_formed();
        // If a racing clone fetched first, keep its copy; both came from the
        // same server.
        let _ = self.capabilities.set(document);
        Ok(self
            .capabilities
            .get()
            .expect("capability cache was just filled")
            .clone())
    }

    pub fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, ClientError> {
        let url = format!("{}/v0/namespaces", self.base_url);
        // Namespace creation has no durable request identity to reconcile an ambiguous success.
        self.request_json_once::<_, NamespaceSummary>(
            self.agent.post(&url),
            Some(&CreateNamespaceRequest {
                namespace_id: namespace_id.clone(),
            }),
        )
    }

    pub fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse, ClientError> {
        // Validated namespace ids are URL-safe by construction, like the
        // other parsed id segments interpolated into paths here and below.
        let url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        self.request_json::<(), NamespaceStatusResponse>(self.agent.get(&url), None)
    }

    /// Deletes a namespace (feature `core.namespaces.delete`): terminal,
    /// and the id is permanently retired. Pass `expected_head_seq` to delete
    /// only if the namespace is still where you last observed it
    /// (`stale_head` on mismatch). Deleting an already-deleted namespace
    /// fails with `namespace_deleted`.
    pub fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse, ClientError> {
        let mut url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        if let Some(expected) = expected_head_seq {
            url.push_str(&format!("?expected_head_seq={}", expected.0));
        }
        // The expected head is a precondition, not an idempotency key for an ambiguous delete.
        self.request_json_once::<(), DeleteNamespaceResponse>(self.agent.delete(&url), None)
    }

    pub fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{source_namespace_id}/forks",
            self.base_url
        );
        // Namespace forks have no durable request identity to replay after an ambiguous success.
        self.request_json_once::<_, NamespaceSummary>(
            self.agent.post(&url),
            Some(&ForkNamespaceRequest {
                new_namespace_id: new_namespace_id.clone(),
            }),
        )
    }

    /// Lists a directory by aggregating every page into one response.
    ///
    /// A commit landing mid-listing retires the cursor with
    /// `rebootstrap_required` (API spec: restart the listing from a fresh
    /// first page). This helper performs that restart itself, discarding
    /// the partial aggregation, up to three times before surfacing the
    /// error. Use [`Self::list_path_page`] for page-level control and
    /// cursor errors surfaced as-is.
    pub fn list_path_all(
        &self,
        spec: &NamespacePath,
    ) -> Result<ListPathEntriesResponse, ClientError> {
        // Bounded so a write-hot namespace cannot pin this loop forever.
        const MAX_LISTING_RESTARTS: u32 = 3;
        let mut restarts = 0;
        'restart: loop {
            let mut entries = Vec::new();
            let mut envelope = None;
            let mut cursor = None;
            loop {
                let page = match self.list_path_page(spec, None, cursor.as_deref()) {
                    Ok(page) => page,
                    Err(error)
                        if cursor.is_some()
                            && restarts < MAX_LISTING_RESTARTS
                            && error.error_code() == Some(ErrorCode::RebootstrapRequired) =>
                    {
                        restarts += 1;
                        continue 'restart;
                    }
                    Err(error) => return Err(error),
                };
                let envelope_ref = envelope.get_or_insert_with(|| ListPathEntriesResponse {
                    namespace_id: page.namespace_id.clone(),
                    absolute_path: page.absolute_path.clone(),
                    head_seq: page.head_seq,
                    entries: Vec::new(),
                    next_cursor: None,
                });
                entries.extend(page.entries);
                cursor = page.next_cursor;
                if cursor.is_none() {
                    // Pages arrive in canonical name-key order; concatenation
                    // preserves it, so aggregation must not re-sort.
                    envelope_ref.entries = entries;
                    return Ok(envelope.expect("first page initializes response envelope"));
                }
            }
        }
    }

    pub fn list_path_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListPathEntriesResponse, ClientError> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/list?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        append_optional_pagination_query(&mut url, true, limit, cursor);
        self.request_json::<(), ListPathEntriesResponse>(self.agent.get(&url), None)
    }

    pub fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/stat?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        self.request_json::<(), AuthoritativePathEntry>(self.agent.get(&url), None)
    }

    pub fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        self.request_bytes(&url)
    }

    pub fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}&revision_no={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str()),
            revision_no.0
        );
        self.request_bytes(&url)
    }

    pub fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, ClientError> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/revisions?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        append_optional_pagination_query(&mut url, true, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.agent.get(&url), None)
    }

    pub fn list_file_revisions_for_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, ClientError> {
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{}/revisions",
            self.base_url, inode_id.0
        );
        append_optional_pagination_query(&mut url, false, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.agent.get(&url), None)
    }

    pub fn read_file_revision_bytes_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{}/revisions/{}/content",
            self.base_url, inode_id.0, revision_no.0
        );
        self.request_bytes(&url)
    }

    pub fn health(&self) -> Result<(), ClientError> {
        let url = format!("{}/health", self.base_url);
        let request = self.authenticated(self.agent.get(&url));
        self.call_with_transient_retry(&request, None)?;
        Ok(())
    }

    pub fn begin_upload(
        &self,
        namespace_id: &NamespaceId,
        request: &BeginUploadRequest,
    ) -> Result<BeginUploadResponse, ClientError> {
        let url = format!("{}/v0/namespaces/{namespace_id}/uploads", self.base_url);
        // Beginning an upload mints a new session id, so a resend could create a second session.
        self.request_json_once::<_, BeginUploadResponse>(self.agent.post(&url), Some(request))
    }

    pub fn begin_direct_put(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<BeginUploadResponse, ClientError> {
        self.begin_upload(
            namespace_id,
            &BeginUploadRequest {
                mode: Some(UploadMode::DirectPut),
                content_ref: Some(content_ref),
            },
        )
    }

    pub fn upload_via_presigned_url(
        &self,
        access: &ObjectTransferAccess,
        bytes: &[u8],
    ) -> Result<(), ClientError> {
        let (method, url, headers) = match access {
            ObjectTransferAccess::PresignedUrl {
                method,
                url,
                headers,
                ..
            } => (method, url, headers),
        };
        if method != "PUT" {
            return Err(ClientError::Http(format!(
                "unsupported presigned upload method `{method}`"
            )));
        }
        let mut request = ureq::put(url);
        for (name, value) in headers {
            request = request.set(name, value);
        }
        // A successful create-only PUT may replay as a provider precondition error, not success.
        self.call_once(&request, Some(bytes)).map(|_| ())
    }

    pub fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<UploadContentResponse, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
            self.base_url
        );
        let request = self
            .authenticated(self.agent.put(&url))
            .set("content-type", "application/octet-stream");
        // Proxied uploads are the request most likely to hit the server's
        // concurrency cap; staging the same bytes again is idempotent.
        let response = self.call_with_transient_retry(&request, Some(bytes))?;
        serde_json::from_reader(response.into_reader())
            .map_err(|err| ClientError::Json(err.to_string()))
    }

    pub fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
            self.base_url
        );
        // The durable completed-session record replays an identical completion without new effect.
        self.request_json::<_, CompleteUploadResponse>(self.agent.post(&url), Some(request))
    }

    pub fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: &CommitSubmissionRequest,
    ) -> Result<ApiCommitResponse, ClientError> {
        let url = format!("{}/v0/namespaces/{namespace_id}/commits", self.base_url);
        // The request's commit id resolves an ambiguous resend through a durable receipt.
        self.request_json::<_, ApiCommitResponse>(self.agent.post(&url), Some(request))
    }

    pub fn list_changes_page(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, ClientError> {
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/changes?after_seq={}",
            self.base_url, after_seq.0
        );
        if let Some(limit) = limit {
            url.push_str(&format!("&limit={limit}"));
        }
        self.request_json::<(), ChangesResponse>(self.agent.get(&url), None)
    }

    /// Creates or reuses a named, user-owned checkpoint pinning the
    /// namespace's current view (admin plane). This is a maintenance
    /// operation, not a file mutation. The record is a garbage-collection
    /// root until released or expired.
    pub fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: &CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, ClientError> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints",
            self.base_url
        );
        self.request_json(self.agent.post(&url), Some(request))
    }

    /// Releases a user-owned checkpoint pin by id (admin plane). Idempotent:
    /// releasing an already-released or reaped record succeeds.
    pub fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &str,
    ) -> Result<ReleaseCheckpointResponse, ClientError> {
        let checkpoint_id = checkpoint_id_url_segment(checkpoint_id)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints/{checkpoint_id}/release",
            self.base_url
        );
        self.request_json::<(), ReleaseCheckpointResponse>(self.agent.post(&url), None)
    }

    /// Flushes the WAL tail and advances the metadata root to a manifest
    /// covering the current head (admin plane). The latest-state maintenance operation: no checkpoint
    /// record is created. The request carries no body.
    pub fn flush_wal(&self, namespace_id: &NamespaceId) -> Result<FlushWalResponse, ClientError> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/wal/flush",
            self.base_url
        );
        self.request_json::<(), FlushWalResponse>(self.agent.post(&url), None)
    }

    /// Advances the namespace retention floor to what checkpoint state allows
    /// (admin plane). Irreversible: WAL history before the floor stops being
    /// replayable. The request carries no body.
    pub fn advance_retention(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse, ClientError> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/retention/advance",
            self.base_url
        );
        self.request_json::<(), AdvanceRetentionResponse>(self.agent.post(&url), None)
    }

    /// Runs one bounded maintenance step against a namespace (admin plane).
    /// Absent request fields use the server's defaults; garbage collection
    /// runs only when the request opts in.
    pub fn maintenance_tick(
        &self,
        namespace_id: &NamespaceId,
        request: &MaintenanceTickRequest,
    ) -> Result<MaintenanceTickResponse, ClientError> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/maintenance/tick",
            self.base_url
        );
        self.request_json(self.agent.post(&url), Some(request))
    }

    /// Runs one mark-and-sweep garbage-collection pass (admin plane).
    /// Nothing sweeps without this explicit call or a maintenance-tick
    /// opt-in.
    pub fn gc_namespace(
        &self,
        namespace_id: &NamespaceId,
        request: &GcRequest,
    ) -> Result<GcResponse, ClientError> {
        let url = format!("{}/v0/admin/namespaces/{namespace_id}/gc", self.base_url);
        self.request_json(self.agent.post(&url), Some(request))
    }

    /// Explicitly repairs one incomplete namespace installation.
    pub fn repair_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<RepairNamespaceResponse, ClientError> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/repair",
            self.base_url
        );
        // Repair can reap a namespace, so a lost success cannot be replayed
        // into the same outcome without a durable request identity.
        self.request_json_once::<(), RepairNamespaceResponse>(self.agent.post(&url), None)
    }

    /// Content search over the namespace's gram index (query plane).
    /// Gate on the `query.grep` capability before calling against unknown
    /// deployments; the namespace must also have a materialized steady-state
    /// grep root or the server answers `not_supported`.
    pub fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse, ClientError> {
        let url = format!("{}/v0/namespaces/{namespace_id}/query/grep", self.base_url);
        self.request_json(self.agent.post(&url), Some(request))
    }

    /// Enables the namespace's grep root (admin plane); embedded mode starts
    /// that namespace's event-driven backfill. Idempotent.
    pub fn enable_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGramsIndexResponse, ClientError> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/index/grams/enable",
            self.base_url
        );
        self.request_json::<(), EnableGramsIndexResponse>(self.agent.post(&url), None)
    }

    /// Disables the namespace's grep root (admin plane); garbage collection
    /// reclaims the segments. Idempotent.
    pub fn disable_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGramsIndexResponse, ClientError> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/index/grams/disable",
            self.base_url
        );
        self.request_json::<(), DisableGramsIndexResponse>(self.agent.post(&url), None)
    }

    /// Runs one explicit gram-index garbage-collection pass for a namespace.
    pub fn gc_grams_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepGcResponse, ClientError> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/index/grams/gc",
            self.base_url
        );
        self.request_json::<(), GrepGcResponse>(self.agent.post(&url), None)
    }

    fn apply_filesystem_operation(
        &self,
        namespace_id: &NamespaceId,
        request: &FilesystemOperationRequest,
    ) -> Result<ApiCommitResponse, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/filesystem/operations",
            self.base_url
        );
        // The request's commit id resolves an ambiguous resend through a durable receipt.
        self.request_json::<_, ApiCommitResponse>(self.agent.post(&url), Some(request))
    }

    fn stage_bytes_as_content_ref(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<StagedContent, ClientError> {
        let upload = self.begin_upload(namespace_id, &BeginUploadRequest::default())?;
        let staged = self.upload_content(namespace_id, upload.upload_id.as_str(), bytes)?;
        let response = self.complete_upload(
            namespace_id,
            upload.upload_id.as_str(),
            &CompleteUploadRequest {
                content_ref: staged.content_ref,
            },
        )?;
        let validated_content_token =
            response
                .validated_content_token
                .map(|token| ValidatedContentToken {
                    content_ref: response.content_ref.clone(),
                    token,
                });
        Ok(StagedContent {
            content_ref: response.content_ref,
            validated_content_token,
        })
    }

    pub fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        let commit_id = options.resolve_commit_id();
        let staged = self.stage_bytes_as_content_ref(spec.namespace(), bytes)?;
        let response = self.apply_filesystem_operation(
            spec.namespace(),
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: staged.validated_content_token.into_iter().collect(),
                operation: FilesystemOperation::PutFile {
                    path: spec.absolute_path().as_str().to_owned(),
                    content_ref: staged.content_ref,
                    behavior,
                },
            },
        )?;
        Ok(response)
    }

    pub fn write_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        self.put_file_bytes(spec, bytes, DestinationBehavior::Replace, options)
    }

    pub fn create_directory(
        &self,
        spec: &NamespacePath,
        parents: bool,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        let commit_id = options.resolve_commit_id();
        let response = self.apply_filesystem_operation(
            spec.namespace(),
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::CreateDirectory {
                    path: spec.absolute_path().as_str().to_owned(),
                    parents,
                },
            },
        )?;
        Ok(response)
    }

    pub fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        self.delete_path_with_behavior(spec, DeleteDirectoryBehavior::NonRecursive, None, options)
    }

    /// Like [`Self::delete_path`], but the delete applies only while the
    /// path still resolves to `expected_inode_id` — a raced rebinding
    /// fails instead of deleting the wrong inode.
    pub fn delete_path_expecting(
        &self,
        spec: &NamespacePath,
        expected_inode_id: InodeId,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        self.delete_path_with_behavior(
            spec,
            DeleteDirectoryBehavior::NonRecursive,
            Some(expected_inode_id),
            options,
        )
    }

    pub fn delete_path_recursive(
        &self,
        spec: &NamespacePath,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        self.delete_path_with_behavior(spec, DeleteDirectoryBehavior::Recursive, None, options)
    }

    fn delete_path_with_behavior(
        &self,
        spec: &NamespacePath,
        behavior: DeleteDirectoryBehavior,
        expected_inode_id: Option<InodeId>,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        let commit_id = options.resolve_commit_id();
        let response = self.apply_filesystem_operation(
            spec.namespace(),
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::DeletePath {
                    path: spec.absolute_path().as_str().to_owned(),
                    behavior,
                    expected_inode_id,
                },
            },
        )?;
        Ok(response)
    }

    pub fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        if from.namespace() != to.namespace() {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot move across namespaces: {} -> {}",
                from.namespace(),
                to.namespace()
            )));
        }
        let commit_id = options.resolve_commit_id();
        let response = self.apply_filesystem_operation(
            from.namespace(),
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::MovePath {
                    from_path: from.absolute_path().as_str().to_owned(),
                    to_path: to.absolute_path().as_str().to_owned(),
                    behavior,
                },
            },
        )?;
        Ok(response)
    }

    pub fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        if from.namespace() != to.namespace() {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot copy across namespaces: {} -> {}",
                from.namespace(),
                to.namespace()
            )));
        }
        let commit_id = options.resolve_commit_id();
        let response = self.apply_filesystem_operation(
            from.namespace(),
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::CopyPath {
                    from_path: from.absolute_path().as_str().to_owned(),
                    to_path: to.absolute_path().as_str().to_owned(),
                    behavior,
                },
            },
        )?;
        Ok(response)
    }

    /// Recovers a deleted file or subtree: clears the tombstone rooted at
    /// `inode_id` (the id the delete reported) and re-binds it at the spec's
    /// path.
    pub fn undelete(
        &self,
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        let commit_id = options.resolve_commit_id();
        let response = self.apply_filesystem_operation(
            spec.namespace(),
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::Undelete {
                    inode_id,
                    deleted_at_seq,
                    path: spec.absolute_path().as_str().to_owned(),
                },
            },
        )?;
        Ok(response)
    }

    pub fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &MutationOptions,
    ) -> Result<ApiCommitResponse, ClientError> {
        let commit_id = options.resolve_commit_id();
        let response = self.apply_filesystem_operation(
            spec.namespace(),
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::RestoreRevision {
                    path: spec.absolute_path().as_str().to_owned(),
                    source_revision_no,
                },
            },
        )?;
        Ok(response)
    }

    pub fn restore_file_revision_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
        commit_id: &CommitId,
    ) -> Result<ApiCommitResponse, ClientError> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{}/revisions/{}/restore",
            self.base_url, inode_id.0, source_revision_no.0
        );
        self.request_json::<_, ApiCommitResponse>(
            self.agent.post(&url),
            Some(&RestoreFileRevisionRequest {
                commit_id: commit_id.clone(),
                base_revision_no,
            }),
        )
    }
}

impl NamespacePath {
    /// Parses and validates both parts of a namespace-qualified path.
    pub fn parse(namespace: &str, absolute_path: &str) -> Result<Self, ClientError> {
        let namespace = NamespaceId::parse(namespace)
            .map_err(|error| ClientError::InvalidNamespacePath(error.to_string()))?;
        let absolute_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| ClientError::InvalidNamespacePath(error.to_string()))?;
        Ok(Self {
            namespace,
            absolute_path,
        })
    }

    /// Pairs already-validated parts without re-parsing.
    pub fn new(namespace: NamespaceId, absolute_path: AbsolutePath) -> Self {
        Self {
            namespace,
            absolute_path,
        }
    }

    /// Namespace the path is scoped to.
    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    /// Absolute path inside the namespace.
    pub fn absolute_path(&self) -> &AbsolutePath {
        &self.absolute_path
    }
}

/// Validated checkpoint ids are URL-safe by construction, like the other
/// parsed id segments interpolated into paths.
fn checkpoint_id_url_segment(checkpoint_id: &str) -> Result<&str, ClientError> {
    loonfs_api::CheckpointId::parse(checkpoint_id)
        .map(|_| checkpoint_id)
        .map_err(|error| ClientError::InvalidCheckpointId(error.to_string()))
}

fn append_optional_pagination_query(
    url: &mut String,
    has_query: bool,
    limit: Option<u32>,
    cursor: Option<&str>,
) {
    let mut has_query = has_query;
    if let Some(limit) = limit {
        append_query_param(url, &mut has_query, "limit", &limit.to_string());
    }
    if let Some(cursor) = cursor {
        append_query_param(url, &mut has_query, "cursor", cursor);
    }
}

fn append_query_param(url: &mut String, has_query: &mut bool, name: &str, value: &str) {
    url.push(if *has_query { '&' } else { '?' });
    *has_query = true;
    url.push_str(name);
    url.push('=');
    url.push_str(&urlencoding::encode(value));
}

#[cfg(test)]
mod tests {

    /// `Client::new` runs the same validation as `ClientConfig::load`, so a
    /// directly built config cannot bypass it.
    #[test]
    fn construction_validates_config_like_load_does() {
        let error = super::Client::new(super::ClientConfig {
            server_url: "ftp://example.com".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
        })
        .expect_err("ftp scheme must be rejected");
        assert!(
            matches!(
                &error,
                super::ClientError::ConfigValidation {
                    field: "server_url",
                    ..
                }
            ),
            "unexpected error: {error:?}"
        );
    }

    /// Configs are strict like everywhere else in the workspace: a typo'd
    /// key fails decode instead of silently producing an unauthenticated
    /// client.
    #[test]
    fn client_config_rejects_unknown_keys() {
        let error = toml::from_str::<ClientConfig>(
            "server_url = \"http://localhost:1\"\nauth_tokn = \"oops\"\n",
        )
        .expect_err("unknown key must fail decode");
        assert!(error.to_string().contains("auth_tokn"), "{error}");

        let config: ClientConfig =
            toml::from_str("server_url = \"http://localhost:1\"\n").expect("minimal config");
        assert!(config.auth_token.is_none());
    }
    use super::*;

    /// The retry policy in one place: network-level transport failures and
    /// the retryable-unavailability codes resend; everything else — including
    /// a served status whose body was not the error envelope — surfaces
    /// immediately.
    #[test]
    fn transient_failure_covers_transport_and_retryable_unavailability_only() {
        let api = |code: &str| ClientError::Api {
            status: 503,
            code: code.to_owned(),
            feature: None,
            message: String::new(),
            request_id: None,
            details: None,
        };
        assert!(transient_failure(
            true,
            &ClientError::Http("reset".to_owned())
        ));
        assert!(transient_failure(false, &api("server_busy")));
        assert!(transient_failure(false, &api("commit_queue_full")));
        assert!(transient_failure(false, &api("shutting_down")));
        assert!(!transient_failure(false, &api("server_error")));
        assert!(!transient_failure(false, &api("maintenance_required")));
        assert!(!transient_failure(
            false,
            &ClientError::Http("http status 502 with a non-envelope body".to_owned())
        ));
    }

    /// A network-level transport failure resends up to the attempt cap when
    /// retry is enabled; with it disabled the first failure surfaces.
    #[test]
    fn transport_failures_resend_up_to_the_attempt_cap() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let transport = crate::transport::test_transport::failures(MAX_TRANSIENT_ATTEMPTS as usize);
        let retrying = Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
        })
        .expect("valid client config");
        let error = retrying
            .namespace_status(&namespace_id)
            .expect_err("dropped connections must fail");
        assert!(matches!(error, ClientError::Http(_)), "{error:?}");
        assert_eq!(transport.attempts(), MAX_TRANSIENT_ATTEMPTS as usize);
        drop(transport);

        let transport = crate::transport::test_transport::failures(1);
        let single_shot = Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: true,
        })
        .expect("valid client config");
        single_shot
            .namespace_status(&namespace_id)
            .expect_err("dropped connection must fail without retry");
        assert_eq!(transport.attempts(), 1);
    }

    fn retry_policy_client() -> Client {
        Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
        })
        .expect("valid client config")
    }

    fn assert_transport_failure_after_one_attempt<T>(
        call: impl FnOnce(&Client) -> Result<T, ClientError>,
    ) {
        let transport = crate::transport::test_transport::failure_then_success(b"{}".to_vec());
        let client = retry_policy_client();
        let result = call(&client);
        assert!(
            matches!(result, Err(ClientError::Http(_))),
            "expected the first transport failure to surface"
        );
        assert_eq!(transport.attempts(), 1);
    }

    #[test]
    fn retry_policy_lifecycle_mutations_are_single_attempt() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        assert_transport_failure_after_one_attempt(|client| client.create_namespace(&namespace_id));
        assert_transport_failure_after_one_attempt(|client| {
            client.fork_namespace(
                &namespace_id,
                &NamespaceId::parse("fork").expect("valid id"),
            )
        });
        assert_transport_failure_after_one_attempt(|client| {
            client.delete_namespace(&namespace_id, Some(ChangeSeq(7)))
        });
        assert_transport_failure_after_one_attempt(|client| client.repair_namespace(&namespace_id));
    }

    #[test]
    fn retry_policy_commit_id_filesystem_mutation_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let commit_id =
            CommitId::parse("c_00000000000000000000000000000001").expect("valid commit id");
        let response = ApiCommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: commit_id.clone(),
            committed_seq: ChangeSeq(1),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();
        let spec = NamespacePath::parse("demo", "/docs").expect("valid namespace path");

        let actual = client
            .create_directory(&spec, false, &MutationOptions::with_commit_id(commit_id))
            .expect("commit-id mutation should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[test]
    fn retry_policy_read_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let response = NamespaceStatusResponse {
            namespace_id: namespace_id.clone(),
            head_seq: ChangeSeq(0),
            current_manifest_id: None,
            wal_tail_segments: 0,
            retention_floor_seq: ChangeSeq(0),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .namespace_status(&namespace_id)
            .expect("read should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[test]
    fn retry_policy_upload_begins_are_single_attempt() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        assert_transport_failure_after_one_attempt(|client| {
            client.begin_upload(&namespace_id, &BeginUploadRequest::default())
        });
        assert_transport_failure_after_one_attempt(|client| {
            client.begin_direct_put(&namespace_id, ContentRef::whole_file_v0(b"direct"))
        });
    }

    #[test]
    fn retry_policy_presigned_upload_is_single_attempt() {
        let transport = crate::transport::test_transport::failure_then_success(Vec::new());
        let client = retry_policy_client();
        let access = ObjectTransferAccess::PresignedUrl {
            method: "PUT".to_owned(),
            url: "http://example.invalid/upload".to_owned(),
            headers: std::collections::BTreeMap::new(),
            expires_at_ms: 1,
        };

        let result = client.upload_via_presigned_url(&access, b"direct");

        assert!(matches!(result, Err(ClientError::Http(_))), "{result:?}");
        assert_eq!(transport.attempts(), 1);
    }

    #[test]
    fn retry_policy_proxied_upload_content_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let upload_id = loonfs_api::UploadId::parse("upl_00000000000000000000000000000001")
            .expect("valid upload id");
        let response = UploadContentResponse {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            content_ref: ContentRef::whole_file_v0(b"content"),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .upload_content(&namespace_id, upload_id.as_str(), b"content")
            .expect("identical content staging should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[test]
    fn retry_policy_upload_completion_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let upload_id = loonfs_api::UploadId::parse("upl_00000000000000000000000000000001")
            .expect("valid upload id");
        let content_ref = ContentRef::whole_file_v0(b"content");
        let response = CompleteUploadResponse {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            content_ref: content_ref.clone(),
            validated_content_token: None,
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .complete_upload(
                &namespace_id,
                upload_id.as_str(),
                &CompleteUploadRequest { content_ref },
            )
            .expect("completed-session replay should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    /// An intermediary answering with a non-envelope body (a load balancer's
    /// HTML 502) must keep its status in the surfaced error — the status is
    /// the only signal the response carried.
    #[test]
    fn map_error_keeps_the_status_when_the_body_is_not_the_envelope() {
        let client = Client::new(ClientConfig {
            server_url: "http://localhost:0".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: true,
        })
        .expect("valid client config");
        let response = ureq::Response::new(502, "Bad Gateway", "<html>upstream error</html>")
            .expect("synthetic response");
        let error = client.map_error(ureq::Error::Status(502, response));
        let ClientError::Http(message) = error else {
            unreachable!("expected Http error, got {error:?}");
        };
        assert!(message.contains("502"), "{message}");
        assert!(message.contains("non-envelope body"), "{message}");
    }
    use super::{Client, ClientConfig, ClientError, ErrorCode, NamespacePath};
    use crate::transport::{transient_failure, MAX_TRANSIENT_ATTEMPTS};
    use loonfs_api::ErrorKind;
    use std::fs;
    use tempfile::tempdir;

    fn api_error(status: u16, code: &str) -> ClientError {
        ClientError::Api {
            status,
            code: code.to_owned(),
            feature: None,
            message: "test".to_owned(),
            request_id: None,
            details: None,
        }
    }

    #[test]
    fn api_errors_with_known_codes_classify_through_the_registry() {
        let error = api_error(409, "stale_revision");
        assert_eq!(error.error_code(), Some(ErrorCode::StaleRevision));
        assert_eq!(error.kind(), Some(ErrorKind::Conflict));

        let error = api_error(409, "content_not_prepared");
        assert_eq!(error.error_code(), Some(ErrorCode::ContentNotPrepared));
        assert_eq!(error.kind(), Some(ErrorKind::Conflict));

        let error = api_error(410, "namespace_deleted");
        assert_eq!(error.error_code(), Some(ErrorCode::NamespaceDeleted));
        assert_eq!(error.kind(), Some(ErrorKind::Gone));

        let error = api_error(503, "commit_outcome_unknown");
        assert_eq!(error.error_code(), Some(ErrorCode::CommitOutcomeUnknown));
        assert_eq!(error.kind(), Some(ErrorKind::OutcomeUnknown));

        let error = api_error(500, "index_corrupt");
        assert_eq!(error.error_code(), Some(ErrorCode::IndexCorrupt));
        assert_eq!(error.kind(), Some(ErrorKind::DataCorruption));
    }

    #[test]
    fn api_errors_with_unknown_codes_fall_back_to_the_status_class() {
        for (status, kind) in [
            (400, ErrorKind::InvalidRequest),
            (404, ErrorKind::InvalidRequest),
            (500, ErrorKind::Internal),
            (503, ErrorKind::Unavailable),
        ] {
            let error = api_error(status, "code_from_a_newer_server");
            assert_eq!(error.error_code(), None);
            assert_eq!(error.kind(), Some(kind), "status {status}");
        }
    }

    #[test]
    fn non_api_errors_have_no_code_or_kind() {
        let error = ClientError::Http("connection refused".to_owned());
        assert_eq!(error.error_code(), None);
        assert_eq!(error.kind(), None);
    }

    #[test]
    fn load_rejects_invalid_server_url() {
        let path = write_config(
            r#"
server_url = "ftp://example.com"
auth_token = "dev-token"
"#,
        );

        let error = ClientConfig::load(&path).expect_err("invalid server url");

        assert!(
            matches!(error, ClientError::ConfigValidation { field, .. } if field == "server_url"),
            "expected config validation error, got {error:?}"
        );
    }

    #[test]
    fn load_rejects_blank_auth_token() {
        let path = write_config(
            r#"
server_url = "http://127.0.0.1:9400"
auth_token = "   "
"#,
        );

        let error = ClientConfig::load(&path).expect_err("blank auth token");

        assert!(
            matches!(error, ClientError::ConfigValidation { field, .. } if field == "auth_token"),
            "expected config validation error, got {error:?}"
        );
    }

    #[test]
    fn load_preserves_missing_file_as_config_io() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("missing.toml");

        let error = ClientConfig::load(&path).expect_err("missing config");

        assert!(matches!(error, ClientError::ConfigIo(_)));
    }

    #[test]
    fn load_preserves_decode_error() {
        let path = write_config("server_url = [");

        let error = ClientConfig::load(&path).expect_err("decode error");

        assert!(matches!(error, ClientError::ConfigDecode(_)));
    }

    #[test]
    fn namespace_path_parse_rejects_invalid_namespace_id() {
        for namespace in ["bad/name", "Demo", "..", "demo?"] {
            assert!(
                matches!(
                    NamespacePath::parse(namespace, "/notes.txt"),
                    Err(ClientError::InvalidNamespacePath(_))
                ),
                "expected invalid namespace path for id {namespace:?}"
            );
        }
    }

    /// Construction is the only door: the fields are private, so a bad id
    /// or a bad path fails `parse` with the same error the string-shuttling
    /// client surfaced before the fields were typed.
    #[test]
    fn namespace_path_parse_rejects_invalid_paths() {
        for path in ["notes.txt", "", "/docs/../a.txt", "/docs/./a.txt"] {
            assert!(
                matches!(
                    NamespacePath::parse("demo", path),
                    Err(ClientError::InvalidNamespacePath(_))
                ),
                "expected invalid namespace path for path {path:?}"
            );
        }
    }

    fn write_config(contents: &str) -> std::path::PathBuf {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("client.toml");
        fs::write(&path, contents).expect("write config");
        let _ = temp_dir.keep();
        path
    }
}
