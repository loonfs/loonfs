//! Blocking HTTP client for a LoonFS server.
//!
//! Use this crate when your process should talk to a hosted LoonFS runtime
//! instead of embedding the runtime directly. The client keeps paths simple:
//! pass a [`NamespacePath`] for filesystem operations and use explicit commit
//! helpers when you need retry control.

use http::Uri;
use loonfs_api::{
    v0::RenameMode,
    v0::{
        BeginUploadRequest, BeginUploadResponse, ChangesResponse,
        CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse,
        CompleteUploadRequest, CompleteUploadResponse, ObjectTransferAccess, UploadContentResponse,
        UploadMode, ValidatedContentToken,
    },
    ApiError, AuthoritativePathEntry, CapabilityDocument, ChangeSeq, CommitId, ContentRef,
    CreateNamespaceRequest, DeleteNamespaceResponse, FilesystemOperation,
    FilesystemOperationRequest, FilesystemOperationResponse, FilesystemPutBehavior,
    ForkNamespaceRequest, InodeId, ListFileRevisionsResponse, ListPathEntriesResponse,
    MutationResult, NamespaceId, NamespaceStatusResponse, NamespaceSummary,
    RestoreFileRevisionRequest, RevisionNo,
};
use serde::Deserialize;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use thiserror::Error;
use walkdir::WalkDir;

/// Client configuration loaded from TOML or built by the caller.
#[derive(Debug, Clone, Deserialize)]
pub struct ClientConfig {
    /// Base URL for the LoonFS server.
    pub server_url: String,
    /// Optional bearer token.
    pub auth_token: Option<String>,
}

/// Synchronous HTTP client for LoonFS.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    auth_token: Option<String>,
    agent: ureq::Agent,
    /// Capability document cache, shared by clones and filled on first use.
    capabilities: Arc<OnceLock<CapabilityDocument>>,
}

/// Result of downloading a remote path to local storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPathResult {
    pub destination: PathBuf,
    pub bytes_written: u64,
}

/// Result of uploading a local path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutPathResult {
    pub committed_seq: ChangeSeq,
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
        Ok(())
    }
}

impl Client {
    /// Creates a client from validated config.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            base_url: config.server_url.trim().trim_end_matches('/').to_owned(),
            auth_token: config.auth_token,
            agent: ureq::AgentBuilder::new().build(),
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
                entries.sort_by(|left, right| {
                    left.display_name
                        .cmp(&right.display_name)
                        .then(left.inode_id.0.cmp(&right.inode_id.0))
                });
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

    pub fn begin_upload(&self, namespace: &str) -> Result<BeginUploadResponse, ClientError> {
        let namespace = namespace_url_segment(namespace)?;
        let url = format!("{}/v0/namespaces/{namespace}/uploads", self.base_url);
        self.request_json::<(), BeginUploadResponse>(self.agent.post(&url), None)
    }

    pub fn begin_upload_with_request(
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
        self.begin_upload_with_request(
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
        let upload = self.begin_upload(namespace)?;
        let staged = self.upload_content(namespace, &upload.upload_id, bytes)?;
        let response = self.complete_upload(
            namespace,
            &upload.upload_id,
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
    ) -> Result<MutationResult, ClientError> {
        self.put_file_bytes_with_commit_id(spec, bytes, force, &generated_commit_id())
    }

    pub fn put_file_bytes_with_commit_id(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
        commit_id: &str,
    ) -> Result<MutationResult, ClientError> {
        let commit_id = parse_commit_id(commit_id)?;
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
                        FilesystemPutBehavior::ReplaceExisting
                    } else {
                        FilesystemPutBehavior::CreateOnly
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
    ) -> Result<MutationResult, ClientError> {
        self.write_file_bytes_with_commit_id(spec, bytes, &generated_commit_id())
    }

    pub fn write_file_bytes_with_commit_id(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        commit_id: &str,
    ) -> Result<MutationResult, ClientError> {
        self.put_file_bytes_with_commit_id(spec, bytes, true, commit_id)
    }

    pub fn create_dir(&self, spec: &NamespacePath) -> Result<MutationResult, ClientError> {
        self.create_dir_with_commit_id(spec, &generated_commit_id())
    }

    pub fn create_dir_with_commit_id(
        &self,
        spec: &NamespacePath,
        commit_id: &str,
    ) -> Result<MutationResult, ClientError> {
        let commit_id = parse_commit_id(commit_id)?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::CreateDir {
                    path: spec.absolute_path.clone(),
                },
            },
        )?;
        Ok(response.into())
    }

    pub fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, ClientError> {
        self.delete_path_with_commit_id(spec, &generated_commit_id())
    }

    pub fn delete_path_with_commit_id(
        &self,
        spec: &NamespacePath,
        commit_id: &str,
    ) -> Result<MutationResult, ClientError> {
        let commit_id = parse_commit_id(commit_id)?;
        let response = self.apply_filesystem_operation(
            &spec.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::DeletePath {
                    path: spec.absolute_path.clone(),
                },
            },
        )?;
        Ok(response.into())
    }

    pub fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, ClientError> {
        self.move_path_with_commit_id(from, to, &generated_commit_id())
    }

    pub fn move_path_with_commit_id(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        commit_id: &str,
    ) -> Result<MutationResult, ClientError> {
        if from.namespace != to.namespace {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot move across namespaces: {} -> {}",
                from.namespace, to.namespace
            )));
        }
        let commit_id = parse_commit_id(commit_id)?;
        let response = self.apply_filesystem_operation(
            &from.namespace,
            &FilesystemOperationRequest {
                commit_id,
                content_tokens: Vec::new(),
                operation: FilesystemOperation::MovePath {
                    from_path: from.absolute_path.clone(),
                    to_path: to.absolute_path.clone(),
                    mode: RenameMode::NoReplace,
                },
            },
        )?;
        Ok(response.into())
    }

    pub fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, ClientError> {
        self.copy_path_with_commit_id(from, to, &generated_commit_id())
    }

    pub fn copy_path_with_commit_id(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        commit_id: &str,
    ) -> Result<MutationResult, ClientError> {
        if from.namespace != to.namespace {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot copy across namespaces: {} -> {}",
                from.namespace, to.namespace
            )));
        }
        let commit_id = parse_commit_id(commit_id)?;
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
    ) -> Result<MutationResult, ClientError> {
        self.restore_file_revision_with_commit_id(spec, source_revision_no, &generated_commit_id())
    }

    pub fn restore_file_revision_with_commit_id(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        commit_id: &str,
    ) -> Result<MutationResult, ClientError> {
        let commit_id = parse_commit_id(commit_id)?;
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

    pub fn get_to_path(
        &self,
        spec: &NamespacePath,
        destination: impl AsRef<Path>,
    ) -> Result<GetPathResult, ClientError> {
        let destination = destination.as_ref();
        let entry = self.stat_path(spec)?;
        match entry.inode_kind {
            loonfs_api::InodeKind::File => {
                let bytes = self.read_file_bytes(spec)?;
                let target = if destination.is_dir() {
                    destination.join(file_name_for_path(&spec.absolute_path)?)
                } else {
                    destination.to_path_buf()
                };
                let bytes_written = write_local_file(&target, &bytes)?;
                Ok(GetPathResult {
                    destination: target,
                    bytes_written,
                })
            }
            loonfs_api::InodeKind::Dir => {
                let bytes_written = self.get_directory(spec, destination)?;
                Ok(GetPathResult {
                    destination: destination.to_path_buf(),
                    bytes_written,
                })
            }
        }
    }

    pub fn put_from_path(
        &self,
        source: impl AsRef<Path>,
        spec: &NamespacePath,
    ) -> Result<PutPathResult, ClientError> {
        let source = source.as_ref();
        if source.is_file() {
            let bytes = fs::read(source).map_err(|err| ClientError::Io(err.to_string()))?;
            let result = self.write_file_bytes(spec, &bytes)?;
            return Ok(PutPathResult {
                committed_seq: result.committed_seq,
            });
        }
        if !source.is_dir() {
            return Err(ClientError::Io(format!(
                "local path is neither file nor directory: {}",
                source.display()
            )));
        }

        let mut last_result = None;
        for entry in WalkDir::new(source).into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(source)
                .map_err(|err| ClientError::Io(err.to_string()))?;
            let remote_path = join_remote_path(&spec.absolute_path, relative)?;
            let target = NamespacePath {
                namespace: spec.namespace.clone(),
                absolute_path: remote_path,
            };
            let bytes = fs::read(entry.path()).map_err(|err| ClientError::Io(err.to_string()))?;
            last_result = Some(self.write_file_bytes(&target, &bytes)?);
        }

        let Some(result) = last_result else {
            return Err(ClientError::Io(format!(
                "local directory does not contain any files: {}",
                source.display()
            )));
        };

        Ok(PutPathResult {
            committed_seq: result.committed_seq,
        })
    }

    fn get_directory(&self, spec: &NamespacePath, destination: &Path) -> Result<u64, ClientError> {
        fs::create_dir_all(destination).map_err(|err| ClientError::Io(err.to_string()))?;
        let mut bytes_written = 0;
        for entry in self.list_path(spec)?.entries {
            let child_spec = NamespacePath {
                namespace: spec.namespace.clone(),
                absolute_path: entry.absolute_path.clone(),
            };
            let child_dest = destination.join(if entry.display_name.is_empty() {
                file_name_for_path(&entry.absolute_path)?
            } else {
                entry.display_name.clone()
            });
            match entry.inode_kind {
                loonfs_api::InodeKind::Dir => {
                    bytes_written += self.get_directory(&child_spec, &child_dest)?;
                }
                loonfs_api::InodeKind::File => {
                    let bytes = self.read_file_bytes(&child_spec)?;
                    bytes_written += write_local_file(&child_dest, &bytes)?;
                }
            }
        }
        Ok(bytes_written)
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

fn file_name_for_path(path: &str) -> Result<String, ClientError> {
    path.rsplit('/')
        .find(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ClientError::InvalidNamespacePath(path.to_owned()))
}

fn join_remote_path(base: &str, relative: &Path) -> Result<String, ClientError> {
    let mut path = PathBuf::from(base.trim_end_matches('/'));
    path.push(relative);
    let rendered = format!("/{}", path.display().to_string().trim_start_matches('/'));
    if rendered.contains('\\') {
        return Err(ClientError::InvalidNamespacePath(rendered));
    }
    Ok(rendered)
}

fn write_local_file(path: &Path, bytes: &[u8]) -> Result<u64, ClientError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| ClientError::Io(err.to_string()))?;
    }
    let mut file = fs::File::create(path).map_err(|err| ClientError::Io(err.to_string()))?;
    file.write_all(bytes)
        .map_err(|err| ClientError::Io(err.to_string()))?;
    Ok(bytes.len() as u64)
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
    use super::{Client, ClientConfig, ClientError, NamespacePath};
    use std::fs;
    use tempfile::tempdir;

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
        });

        for result in [
            client.create_namespace("bad/name").map(|_| ()),
            client.fork_namespace("demo", "bad/name").map(|_| ()),
            client.begin_upload("bad/name").map(|_| ()),
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
