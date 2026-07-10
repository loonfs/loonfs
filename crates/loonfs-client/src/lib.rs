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

use http::Uri;
use loonfs_api::{
    v0::{
        BeginUploadRequest, BeginUploadResponse, ChangesResponse,
        CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse,
        CompleteUploadRequest, CompleteUploadResponse, MoveBehavior, ObjectTransferAccess,
        UploadContentResponse, UploadMode, ValidatedContentToken,
    },
    AdvanceRetentionResponse, ApiError, AuthoritativePathEntry, CapabilityDocument, ChangeSeq,
    CommitId, ContentRef, CreateCheckpointResponse, CreateNamespaceRequest,
    DeleteDirectoryBehavior, DeleteNamespaceResponse, ErrorCode, ErrorKind, FilesystemOperation,
    FilesystemOperationRequest, FilesystemOperationResponse, ForkNamespaceRequest, GcRequest,
    GcResponse, InodeId, ListFileRevisionsResponse, ListPathEntriesResponse,
    MaintenanceTickRequest, MaintenanceTickResponse, MutationResult, NamespaceId,
    NamespaceStatusResponse, NamespaceSummary, PutBehavior, RestoreFileRevisionRequest, RevisionNo,
};
use serde::Deserialize;
use std::fs;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use thiserror::Error;

/// Client configuration loaded from TOML or built by the caller.
#[derive(Debug, Clone, Deserialize)]
pub struct ClientConfig {
    /// Base URL for the LoonFS server.
    pub server_url: String,
    /// Optional bearer token.
    pub auth_token: Option<String>,
    /// Optional overall per-request deadline in milliseconds. Unset means no
    /// whole-request deadline: requests are bounded only by the built-in
    /// 60-second socket inactivity timeouts, so slow-but-progressing large
    /// transfers are not cut off while a stalled connection still fails.
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
}

/// Socket read/write inactivity timeout applied to every request. A
/// connection that makes no progress for this long fails instead of hanging
/// the caller forever.
const IO_INACTIVITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Synchronous HTTP client for LoonFS.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    auth_token: Option<String>,
    agent: ureq::Agent,
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

/// Error returned by the blocking HTTP client.
///
/// Foreign causes (io, json, ureq) are stringified for now; this crate has no
/// Clone/serde constraint, so switching to `#[source]` chains later is fine.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("failed to read config: {0}")]
    ConfigIo(String),
    #[error("failed to decode config: {0}")]
    ConfigDecode(String),
    #[error("missing `{field}`")]
    MissingConfigField { field: &'static str },
    #[error("invalid `{field}`: {reason}")]
    ConfigValidation { field: &'static str, reason: String },
    #[error("invalid namespace path `{0}`")]
    InvalidNamespacePath(String),
    #[error("invalid commit_id `{0}`")]
    InvalidCommitId(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("server returned {status} {code}: {message}")]
    Api {
        status: u16,
        code: String,
        /// Capability feature key accompanying `not_supported` errors.
        feature: Option<String>,
        message: String,
    },
    #[error("i/o error: {0}")]
    Io(String),
    #[error("json error: {0}")]
    Json(String),
}

impl ClientError {
    /// Returns the typed code for [`ClientError::Api`] errors, or `None` for
    /// non-API errors and for codes this build does not know (clients must
    /// tolerate unknown codes).
    pub fn error_code(&self) -> Option<ErrorCode> {
        match self {
            ClientError::Api { code, .. } => ErrorCode::parse(code),
            _ => None,
        }
    }

    /// Returns the caller-action category for [`ClientError::Api`] errors.
    ///
    /// Known codes classify through [`ErrorCode::kind`]. Unknown codes (a
    /// newer server) fall back to the HTTP status class, so retry decisions
    /// still work: 503 is [`ErrorKind::Unavailable`], other 5xx are
    /// [`ErrorKind::Internal`], and 4xx are [`ErrorKind::InvalidRequest`].
    pub fn kind(&self) -> Option<ErrorKind> {
        match self {
            ClientError::Api { status, code, .. } => match ErrorCode::parse(code) {
                Some(code) => Some(code.kind()),
                None => kind_for_status_class(*status),
            },
            _ => None,
        }
    }
}

/// Coarse status-class fallback for error codes this build does not know.
fn kind_for_status_class(status: u16) -> Option<ErrorKind> {
    match status {
        // 503 stays retryable even when the code is unknown.
        503 => Some(ErrorKind::Unavailable),
        400..=499 => Some(ErrorKind::InvalidRequest),
        500..=599 => Some(ErrorKind::Internal),
        _ => None,
    }
}

impl ClientConfig {
    /// Loads and validates a client config from TOML.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let bytes =
            fs::read(path.as_ref()).map_err(|err| ClientError::ConfigIo(err.to_string()))?;
        let config: Self = toml::from_str(
            std::str::from_utf8(&bytes)
                .map_err(|err| ClientError::ConfigDecode(err.to_string()))?,
        )
        .map_err(|err| ClientError::ConfigDecode(err.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ClientError> {
        validate_absolute_http_url("server_url", &self.server_url)?;
        if let Some(token) = &self.auth_token {
            if token.trim().is_empty() {
                return Err(ClientError::ConfigValidation {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                });
            }
        }
        if self.request_timeout_ms == Some(0) {
            return Err(ClientError::ConfigValidation {
                field: "request_timeout_ms",
                reason: "must be greater than zero; omit it for no deadline".to_owned(),
            });
        }
        Ok(())
    }
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

    pub fn list_path(&self, spec: &NamespacePath) -> Result<ListPathEntriesResponse, ClientError> {
        let mut entries = Vec::new();
        let mut envelope = None;
        let mut cursor = None;
        loop {
            let page = self.list_path_page(spec, None, cursor.as_deref())?;
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

    pub fn list_file_revisions(
        &self,
        spec: &NamespacePath,
    ) -> Result<ListFileRevisionsResponse, ClientError> {
        self.list_file_revisions_page(spec, None, None)
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

    pub fn list_file_revisions_for_inode(
        &self,
        namespace: &str,
        inode_id: InodeId,
    ) -> Result<ListFileRevisionsResponse, ClientError> {
        self.list_file_revisions_for_inode_page(namespace, inode_id, None, None)
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
        let response = request
            .send_bytes(bytes)
            .map_err(|err| self.map_error(err))?;
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

    pub fn list_changes(
        &self,
        namespace: &str,
        after_seq: ChangeSeq,
    ) -> Result<ChangesResponse, ClientError> {
        self.list_changes_page(namespace, after_seq, None)
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

    /// Creates or reuses a checkpoint pinning the namespace's current view
    /// (admin plane). This is a maintenance operation, not a file mutation;
    /// the request carries no body.
    pub fn create_checkpoint(
        &self,
        namespace: &str,
    ) -> Result<CreateCheckpointResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/admin/namespaces/{namespace}/checkpoint",
            self.base_url
        );
        self.request_json::<(), CreateCheckpointResponse>(self.agent.post(&url), None)
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

    fn apply_filesystem_operation(
        &self,
        namespace: &str,
        request: &FilesystemOperationRequest,
    ) -> Result<FilesystemOperationResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!(
            "{}/v0/namespaces/{namespace}/filesystem/operations",
            self.base_url
        );
        self.request_json::<_, FilesystemOperationResponse>(self.agent.post(&url), Some(request))
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
        force: bool,
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
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
                    behavior: if force {
                        PutBehavior::Replace
                    } else {
                        PutBehavior::NoReplace
                    },
                },
            },
        )?;
        Ok(response.into())
    }

    pub fn write_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
        self.put_file_bytes(spec, bytes, true, options)
    }

    pub fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
        let commit_id = options.resolve_commit_id()?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::CreateDirectory {
                    path: spec.absolute_path.clone(),
                },
            },
        )?;
        Ok(response.into())
    }

    pub fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
        self.delete_path_with_behavior(spec, DeleteDirectoryBehavior::NonRecursive, options)
    }

    pub fn delete_path_recursive(
        &self,
        spec: &NamespacePath,
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
        self.delete_path_with_behavior(spec, DeleteDirectoryBehavior::Recursive, options)
    }

    fn delete_path_with_behavior(
        &self,
        spec: &NamespacePath,
        behavior: DeleteDirectoryBehavior,
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
        let commit_id = options.resolve_commit_id()?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::DeletePath {
                    path: spec.absolute_path.clone(),
                    behavior,
                },
            },
        )?;
        Ok(response.into())
    }

    pub fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
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
                    behavior: MoveBehavior::NoReplace,
                },
            },
        )?;
        Ok(response.into())
    }

    pub fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
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
                },
            },
        )?;
        Ok(response.into())
    }

    pub fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &MutationOptions,
    ) -> Result<MutationResult, ClientError> {
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
        Ok(response.into())
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

    fn request_json<Req, Resp>(
        &self,
        request: ureq::Request,
        body: Option<&Req>,
    ) -> Result<Resp, ClientError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let request = self.authenticated(request);
        let response = match body {
            Some(body) => request.send_json(body).map_err(|err| self.map_error(err))?,
            None => request.call().map_err(|err| self.map_error(err))?,
        };
        serde_json::from_reader(response.into_reader())
            .map_err(|err| ClientError::Json(err.to_string()))
    }

    fn request_bytes(&self, url: &str) -> Result<Vec<u8>, ClientError> {
        let request = self.authenticated(self.agent.get(url));
        let response = request.call().map_err(|err| self.map_error(err))?;
        let mut reader = response.into_reader();
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut bytes)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        Ok(bytes)
    }

    fn authenticated(&self, request: ureq::Request) -> ureq::Request {
        match &self.auth_token {
            Some(token) => request.set("authorization", &format!("Bearer {token}")),
            None => request,
        }
    }

    fn map_error(&self, error: ureq::Error) -> ClientError {
        match error {
            ureq::Error::Status(status, response) => {
                let parsed = serde_json::from_reader::<_, ApiError>(response.into_reader());
                match parsed {
                    Ok(body) => ClientError::Api {
                        status,
                        code: body.code,
                        feature: body.feature,
                        message: body.message,
                    },
                    Err(err) => ClientError::Http(err.to_string()),
                }
            }
            ureq::Error::Transport(err) => ClientError::Http(err.to_string()),
        }
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

fn validate_absolute_http_url(field: &'static str, value: &str) -> Result<(), ClientError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ClientError::MissingConfigField { field });
    }

    let uri: Uri =
        trimmed
            .parse()
            .map_err(|err: http::uri::InvalidUri| ClientError::ConfigValidation {
                field,
                reason: err.to_string(),
            })?;

    match uri.scheme_str() {
        Some("http" | "https") => {}
        Some(other) => {
            return Err(ClientError::ConfigValidation {
                field,
                reason: format!("scheme must be http or https, got `{other}`"),
            });
        }
        None => {
            return Err(ClientError::ConfigValidation {
                field,
                reason: "must be an absolute http or https URL".to_owned(),
            });
        }
    }

    if uri.authority().is_none() {
        return Err(ClientError::ConfigValidation {
            field,
            reason: "must be an absolute http or https URL".to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BeginUploadRequest, Client, ClientConfig, ClientError, ErrorCode, ErrorKind, NamespacePath,
    };
    use std::fs;
    use tempfile::tempdir;

    fn api_error(status: u16, code: &str) -> ClientError {
        ClientError::Api {
            status,
            code: code.to_owned(),
            feature: None,
            message: "test".to_owned(),
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
        });

        for result in [
            client.create_namespace("bad/name").map(|_| ()),
            client.fork_namespace("demo", "bad/name").map(|_| ()),
            client
                .begin_upload("bad/name", &BeginUploadRequest::default())
                .map(|_| ()),
            client.create_checkpoint("bad/name").map(|_| ()),
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
