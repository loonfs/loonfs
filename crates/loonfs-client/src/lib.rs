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
        CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse,
        CompleteUploadRequest, CompleteUploadResponse, ObjectTransferAccess, UploadContentResponse,
        UploadMode, ValidatedContentToken,
    },
    AdvanceRetentionResponse, AuthoritativePathEntry, CapabilityDocument, ChangeSeq, CommitId,
    ContentRef, CreateCheckpointRequest, CreateCheckpointResponse, CreateNamespaceRequest,
    DeleteDirectoryBehavior, DeleteNamespaceResponse, DestinationBehavior,
    DisableGramsIndexResponse, EnableGramsIndexResponse, ErrorCode, FilesystemOperation,
    FilesystemOperationRequest, FlushWalResponse, ForkNamespaceRequest, GcRequest, GcResponse,
    GrepRequest, GrepResponse, InodeId, ListFileRevisionsResponse, ListPathEntriesResponse,
    MaintenanceTickRequest, MaintenanceTickResponse, NamespaceId, NamespaceStatusResponse,
    NamespaceSummary, ReleaseCheckpointResponse, RestoreFileRevisionRequest, RevisionNo,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePath {
    /// Namespace id as text.
    pub namespace: String,
    /// Absolute path inside the namespace.
    pub absolute_path: String,
}

/// Options shared by every client mutation, mirroring the runtime's
/// per-operation options structs.
#[derive(Debug, Clone, Default)]
pub struct MutationOptions {
    /// Idempotency key for the commit; retrying with the same id replays
    /// the committed mutation instead of double-committing. A fresh id is
    /// generated when absent.
    pub commit_id: Option<String>,
}

impl MutationOptions {
    /// Retry with a caller-chosen idempotency key.
    pub fn with_commit_id(commit_id: impl Into<String>) -> Self {
        Self {
            commit_id: Some(commit_id.into()),
        }
    }

    fn resolve_commit_id(&self) -> Result<CommitId, ClientError> {
        match &self.commit_id {
            Some(value) => parse_commit_id(value),
            None => parse_commit_id(&generated_commit_id()),
        }
    }
}

impl Client {
    /// Creates a client from validated config.
    pub fn new(config: ClientConfig) -> Self {
        let mut agent = ureq::AgentBuilder::new()
            .timeout_read(IO_INACTIVITY_TIMEOUT)
            .timeout_write(IO_INACTIVITY_TIMEOUT);
        if let Some(timeout_ms) = config.request_timeout_ms {
            agent = agent.timeout(std::time::Duration::from_millis(timeout_ms));
        }
        Self {
            base_url: config.server_url.trim().trim_end_matches('/').to_owned(),
            auth_token: config.auth_token,
            agent: agent.build(),
            transient_retry: !config.disable_transient_retry,
            capabilities: Arc::new(OnceLock::new()),
        }
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

    pub fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, ClientError> {
        let namespace_id = NamespaceId::parse(namespace_id).map_err(invalid_namespace_id_error)?;
        let url = format!("{}/v0/namespaces", self.base_url);
        self.request_json::<_, NamespaceSummary>(
            self.agent.post(&url),
            Some(&CreateNamespaceRequest {
                namespace_id: namespace_id.as_str().to_owned(),
            }),
        )
    }

    pub fn namespace_status(
        &self,
        namespace: &str,
    ) -> Result<NamespaceStatusResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!("{}/v0/namespaces/{namespace}", self.base_url);
        self.request_json::<(), NamespaceStatusResponse>(self.agent.get(&url), None)
    }

    /// Deletes a namespace (feature `core.namespaces.delete`): terminal,
    /// and the id is permanently retired. Pass `expected_head_seq` to delete
    /// only if the namespace is still where you last observed it
    /// (`stale_head` on mismatch). Deleting an already-deleted namespace
    /// fails with `namespace_deleted`.
    pub fn delete_namespace(
        &self,
        namespace: &str,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let mut url = format!("{}/v0/namespaces/{namespace}", self.base_url);
        if let Some(expected) = expected_head_seq {
            url.push_str(&format!("?expected_head_seq={}", expected.0));
        }
        self.request_json::<(), DeleteNamespaceResponse>(self.agent.delete(&url), None)
    }

    pub fn fork_namespace(
        &self,
        source_namespace: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, ClientError> {
        let source_namespace = namespace_url_segment(source_namespace)?;
        let new_namespace_id =
            NamespaceId::parse(new_namespace_id).map_err(invalid_namespace_id_error)?;
        let url = format!("{}/v0/namespaces/{source_namespace}/forks", self.base_url);
        self.request_json::<_, NamespaceSummary>(
            self.agent.post(&url),
            Some(&ForkNamespaceRequest {
                new_namespace_id: new_namespace_id.as_str().to_owned(),
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
        let namespace = namespace_url_segment(&spec.namespace)?;
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/list?path={}",
            self.base_url,
            namespace,
            urlencoding::encode(&spec.absolute_path)
        );
        append_optional_pagination_query(&mut url, true, limit, cursor);
        self.request_json::<(), ListPathEntriesResponse>(self.agent.get(&url), None)
    }

    pub fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, ClientError> {
        let namespace = namespace_url_segment(&spec.namespace)?;
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/stat?path={}",
            self.base_url,
            namespace,
            urlencoding::encode(&spec.absolute_path)
        );
        self.request_json::<(), AuthoritativePathEntry>(self.agent.get(&url), None)
    }

    pub fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, ClientError> {
        let namespace = namespace_url_segment(&spec.namespace)?;
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}",
            self.base_url,
            namespace,
            urlencoding::encode(&spec.absolute_path)
        );
        self.request_bytes(&url)
    }

    pub fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, ClientError> {
        let namespace = namespace_url_segment(&spec.namespace)?;
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}&revision_no={}",
            self.base_url,
            namespace,
            urlencoding::encode(&spec.absolute_path),
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
        let namespace = namespace_url_segment(&spec.namespace)?;
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/revisions?path={}",
            self.base_url,
            namespace,
            urlencoding::encode(&spec.absolute_path)
        );
        append_optional_pagination_query(&mut url, true, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.agent.get(&url), None)
    }

    pub fn list_file_revisions_for_inode_page(
        &self,
        namespace: &str,
        inode_id: InodeId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let mut url = format!(
            "{}/v0/namespaces/{namespace}/inodes/{}/revisions",
            self.base_url, inode_id.0
        );
        append_optional_pagination_query(&mut url, false, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.agent.get(&url), None)
    }

    pub fn read_file_revision_bytes_for_inode(
        &self,
        namespace: &str,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/namespaces/{namespace}/inodes/{}/revisions/{}/content",
            self.base_url, inode_id.0, revision_no.0
        );
        self.request_bytes(&url)
    }

    pub fn health(&self) -> Result<(), ClientError> {
        let url = format!("{}/health", self.base_url);
        let request = self.authenticated(self.agent.get(&url));
        request.call().map_err(|err| self.map_error(err))?;
        Ok(())
    }

    pub fn begin_upload(
        &self,
        namespace: &str,
        request: &BeginUploadRequest,
    ) -> Result<BeginUploadResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!("{}/v0/namespaces/{namespace}/uploads", self.base_url);
        self.request_json::<_, BeginUploadResponse>(self.agent.post(&url), Some(request))
    }

    pub fn begin_direct_put(
        &self,
        namespace: &str,
        content_ref: ContentRef,
    ) -> Result<BeginUploadResponse, ClientError> {
        self.begin_upload(
            namespace,
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
        request
            .send_bytes(bytes)
            .map(|_| ())
            .map_err(|err| self.map_error(err))
    }

    pub fn upload_content(
        &self,
        namespace: &str,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<UploadContentResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/namespaces/{namespace}/uploads/{upload_id}/content",
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
        namespace: &str,
        upload_id: &str,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/namespaces/{namespace}/uploads/{upload_id}/complete",
            self.base_url
        );
        self.request_json::<_, CompleteUploadResponse>(self.agent.post(&url), Some(request))
    }

    pub fn commit_operations(
        &self,
        namespace: &str,
        request: &ApiCommitRequest,
    ) -> Result<ApiCommitResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!("{}/v0/namespaces/{namespace}/commits", self.base_url);
        self.request_json::<_, ApiCommitResponse>(self.agent.post(&url), Some(request))
    }

    pub fn list_changes_page(
        &self,
        namespace: &str,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let mut url = format!(
            "{}/v0/namespaces/{namespace}/changes?after_seq={}",
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
        namespace: &str,
        request: &CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace}/checkpoints",
            self.base_url
        );
        self.request_json(self.agent.post(&url), Some(request))
    }

    /// Releases a user-owned checkpoint pin by id (admin plane). Idempotent:
    /// releasing an already-released or reaped record succeeds.
    pub fn release_checkpoint(
        &self,
        namespace: &str,
        checkpoint_id: &str,
    ) -> Result<ReleaseCheckpointResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let checkpoint_id = checkpoint_id_url_segment(checkpoint_id)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace}/checkpoints/{checkpoint_id}/release",
            self.base_url
        );
        self.request_json::<(), ReleaseCheckpointResponse>(self.agent.post(&url), None)
    }

    /// Flushes the WAL tail and advances the metadata root to a manifest
    /// covering the current head (admin plane). The latest-state maintenance operation: no checkpoint
    /// record is created. The request carries no body.
    pub fn flush_wal(&self, namespace: &str) -> Result<FlushWalResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace}/wal/flush",
            self.base_url
        );
        self.request_json::<(), FlushWalResponse>(self.agent.post(&url), None)
    }

    /// Advances the namespace retention floor to what checkpoint state allows
    /// (admin plane). Irreversible: WAL history before the floor stops being
    /// replayable. The request carries no body.
    pub fn advance_retention(
        &self,
        namespace: &str,
    ) -> Result<AdvanceRetentionResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace}/retention/advance",
            self.base_url
        );
        self.request_json::<(), AdvanceRetentionResponse>(self.agent.post(&url), None)
    }

    /// Runs one bounded maintenance step against a namespace (admin plane).
    /// Absent request fields use the server's defaults; garbage collection
    /// runs only when the request opts in.
    pub fn maintenance_tick(
        &self,
        namespace: &str,
        request: &MaintenanceTickRequest,
    ) -> Result<MaintenanceTickResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace}/maintenance/tick",
            self.base_url
        );
        self.request_json(self.agent.post(&url), Some(request))
    }

    /// Runs one mark-and-sweep garbage-collection pass (admin plane).
    /// Nothing sweeps without this explicit call or a maintenance-tick
    /// opt-in.
    pub fn gc_namespace(
        &self,
        namespace: &str,
        request: &GcRequest,
    ) -> Result<GcResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!("{}/v0/admin/namespaces/{namespace}/gc", self.base_url);
        self.request_json(self.agent.post(&url), Some(request))
    }

    /// Content search over the namespace's gram index (query plane).
    /// Gate on the `query.grep` capability before calling against unknown
    /// deployments; the namespace must also have `index.grams`
    /// materialized or the server answers `not_supported`.
    pub fn grep(
        &self,
        namespace: &str,
        request: &GrepRequest,
    ) -> Result<GrepResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!("{}/v0/namespaces/{namespace}/query/grep", self.base_url);
        self.request_json(self.agent.post(&url), Some(request))
    }

    /// Publishes the `index.grams` feature entry (admin plane); backfill
    /// runs through maintenance ticks. Idempotent.
    pub fn enable_grams_index(
        &self,
        namespace: &str,
    ) -> Result<EnableGramsIndexResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace}/index/grams/enable",
            self.base_url
        );
        self.request_json::<(), EnableGramsIndexResponse>(self.agent.post(&url), None)
    }

    /// Removes the `index.grams` feature entry (admin plane); garbage
    /// collection reclaims the segments. Idempotent.
    pub fn disable_grams_index(
        &self,
        namespace: &str,
    ) -> Result<DisableGramsIndexResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace}/index/grams/disable",
            self.base_url
        );
        self.request_json::<(), DisableGramsIndexResponse>(self.agent.post(&url), None)
    }

    fn apply_filesystem_operation(
        &self,
        namespace: &str,
        request: &FilesystemOperationRequest,
    ) -> Result<ApiCommitResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/namespaces/{namespace}/filesystem/operations",
            self.base_url
        );
        self.request_json::<_, ApiCommitResponse>(self.agent.post(&url), Some(request))
    }

    fn stage_bytes_as_content_ref(
        &self,
        namespace: &str,
        bytes: &[u8],
    ) -> Result<StagedContent, ClientError> {
        let upload = self.begin_upload(namespace, &BeginUploadRequest::default())?;
        let staged = self.upload_content(namespace, upload.upload_id.as_str(), bytes)?;
        let response = self.complete_upload(
            namespace,
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
        let commit_id = options.resolve_commit_id()?;
        let staged = self.stage_bytes_as_content_ref(&spec.namespace, bytes)?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: staged.validated_content_token.into_iter().collect(),
                operation: FilesystemOperation::PutFile {
                    path: spec.absolute_path.clone(),
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
        let commit_id = options.resolve_commit_id()?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::CreateDirectory {
                    path: spec.absolute_path.clone(),
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
        let commit_id = options.resolve_commit_id()?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::DeletePath {
                    path: spec.absolute_path.clone(),
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
        if from.namespace != to.namespace {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot move across namespaces: {} -> {}",
                from.namespace, to.namespace
            )));
        }
        let commit_id = options.resolve_commit_id()?;
        let response = self.apply_filesystem_operation(
            &from.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::MovePath {
                    from_path: from.absolute_path.clone(),
                    to_path: to.absolute_path.clone(),
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
        if from.namespace != to.namespace {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot copy across namespaces: {} -> {}",
                from.namespace, to.namespace
            )));
        }
        let commit_id = options.resolve_commit_id()?;
        let response = self.apply_filesystem_operation(
            &from.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::CopyPath {
                    from_path: from.absolute_path.clone(),
                    to_path: to.absolute_path.clone(),
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
        let commit_id = options.resolve_commit_id()?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::Undelete {
                    inode_id,
                    deleted_at_seq,
                    path: spec.absolute_path.clone(),
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
        let commit_id = options.resolve_commit_id()?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::RestoreRevision {
                    path: spec.absolute_path.clone(),
                    source_revision_no,
                },
            },
        )?;
        Ok(response)
    }

    pub fn restore_file_revision_for_inode(
        &self,
        namespace: &str,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
        commit_id: &str,
    ) -> Result<ApiCommitResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let commit_id = parse_commit_id(commit_id)?;
        let url = format!(
            "{}/v0/namespaces/{namespace}/inodes/{}/revisions/{}/restore",
            self.base_url, inode_id.0, source_revision_no.0
        );
        self.request_json::<_, ApiCommitResponse>(
            self.agent.post(&url),
            Some(&RestoreFileRevisionRequest {
                commit_id,
                base_revision_no,
            }),
        )
    }
}

impl NamespacePath {
    pub fn parse(value: &str) -> Result<Self, ClientError> {
        let (namespace, path) = value
            .split_once(':')
            .ok_or_else(|| ClientError::InvalidNamespacePath(value.to_owned()))?;
        NamespaceId::parse(namespace)
            .map_err(|err| ClientError::InvalidNamespacePath(err.to_string()))?;
        if !path.starts_with('/') {
            return Err(ClientError::InvalidNamespacePath(value.to_owned()));
        }
        Ok(Self {
            namespace: namespace.to_owned(),
            absolute_path: path.to_owned(),
        })
    }
}

fn namespace_url_segment(namespace: &str) -> Result<&str, ClientError> {
    NamespaceId::parse(namespace)
        .map(|_| namespace)
        .map_err(invalid_namespace_id_error)
}

/// Validated checkpoint ids are URL-safe by construction, like the other
/// parsed id segments interpolated into paths.
fn checkpoint_id_url_segment(checkpoint_id: &str) -> Result<&str, ClientError> {
    loonfs_api::CheckpointId::parse(checkpoint_id)
        .map(|_| checkpoint_id)
        .map_err(|error| ClientError::InvalidCheckpointId(error.to_string()))
}

fn invalid_namespace_id_error(error: loonfs_api::NamespaceIdValidationError) -> ClientError {
    ClientError::InvalidNamespacePath(error.to_string())
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

fn parse_commit_id(commit_id: &str) -> Result<CommitId, ClientError> {
    CommitId::parse(commit_id).map_err(|error| ClientError::InvalidCommitId(error.to_string()))
}

fn generated_commit_id() -> String {
    CommitId::generate().to_string()
}

#[cfg(test)]
mod tests {

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

    /// A connection the server drops before answering is a transport
    /// failure: with the retry enabled the client resends up to the attempt
    /// cap; with it disabled the first failure surfaces.
    #[test]
    fn transport_failures_resend_up_to_the_attempt_cap() {
        use std::io::Read;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        let total_expected = MAX_TRANSIENT_ATTEMPTS as usize + 1;
        let server = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let seen = counter.fetch_add(1, Ordering::SeqCst) + 1;
                // Read a little so the request bytes are on the wire, then
                // drop the connection without answering.
                let mut buf = [0u8; 256];
                let _ = stream.read(&mut buf);
                drop(stream);
                if seen >= total_expected {
                    break;
                }
            }
        });

        let retrying = Client::new(ClientConfig {
            server_url: format!("http://{addr}"),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
        });
        let error = retrying
            .namespace_status("demo")
            .expect_err("dropped connections must fail");
        assert!(matches!(error, ClientError::Http(_)), "{error:?}");
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            MAX_TRANSIENT_ATTEMPTS as usize
        );

        let single_shot = Client::new(ClientConfig {
            server_url: format!("http://{addr}"),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: true,
        });
        single_shot
            .namespace_status("demo")
            .expect_err("dropped connection must fail without retry");
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            MAX_TRANSIENT_ATTEMPTS as usize + 1
        );
        server.join().expect("server thread");
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
        });
        let response = ureq::Response::new(502, "Bad Gateway", "<html>upstream error</html>")
            .expect("synthetic response");
        let error = client.map_error(ureq::Error::Status(502, response));
        let ClientError::Http(message) = error else {
            unreachable!("expected Http error, got {error:?}");
        };
        assert!(message.contains("502"), "{message}");
        assert!(message.contains("non-envelope body"), "{message}");
    }
    use super::{
        BeginUploadRequest, Client, ClientConfig, ClientError, CreateCheckpointRequest, ErrorCode,
        NamespacePath,
    };
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

        let error = api_error(410, "namespace_deleted");
        assert_eq!(error.error_code(), Some(ErrorCode::NamespaceDeleted));
        assert_eq!(error.kind(), Some(ErrorKind::Gone));

        let error = api_error(503, "commit_outcome_unknown");
        assert_eq!(error.error_code(), Some(ErrorCode::CommitOutcomeUnknown));
        assert_eq!(error.kind(), Some(ErrorKind::OutcomeUnknown));
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
        for value in [
            "bad/name:/notes.txt",
            "Demo:/notes.txt",
            "..:/notes.txt",
            "demo?:/notes.txt",
        ] {
            assert!(
                matches!(
                    NamespacePath::parse(value),
                    Err(ClientError::InvalidNamespacePath(_))
                ),
                "expected invalid namespace path {value:?}"
            );
        }
    }

    #[test]
    fn client_rejects_invalid_namespace_ids_before_http_requests() {
        let client = Client::new(ClientConfig {
            server_url: "http://127.0.0.1:9".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
        });

        for result in [
            client.create_namespace("bad/name").map(|_| ()),
            client.fork_namespace("demo", "bad/name").map(|_| ()),
            client
                .begin_upload("bad/name", &BeginUploadRequest::default())
                .map(|_| ()),
            client
                .create_checkpoint(
                    "bad/name",
                    &CreateCheckpointRequest {
                        name: "nightly".to_owned(),
                        ttl_ms: None,
                    },
                )
                .map(|_| ()),
            client
                .release_checkpoint("bad/name", "chk_00000000000000000000000000000001")
                .map(|_| ()),
            client.flush_wal("bad/name").map(|_| ()),
            client.advance_retention("bad/name").map(|_| ()),
        ] {
            assert!(matches!(result, Err(ClientError::InvalidNamespacePath(_))));
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
