#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitOp, CommitOpResult, CommitPrecondition,
    CommitRequest, CommitResponse, CompleteUploadRequest, CompleteUploadResponse,
    UploadContentResponse,
};
pub use loon_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, CommitId,
    ContentRef, CreateCheckpointResponse, FilesystemOperationResponse, InodeId, MutationResult,
    NamespaceId, NamespaceSummary,
};
use loon_core::MutationContext;
pub use loon_core::{BootstrapNamespaceError, CoreError, CoreErrorKind, PutFileBehavior};
pub use loon_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

pub const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;

pub type SharedObjectStore = Arc<dyn ObjectStore + Send + Sync>;
pub type Result<T> = std::result::Result<T, RuntimeError>;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Bootstrap(#[from] BootstrapNamespaceError),
    #[error("invalid runtime config: {0}")]
    Config(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsConfig {
    pub writer_id: String,
    pub writer_version: String,
    pub lease_duration_ms: u64,
}

impl FsConfig {
    pub fn new(writer_id: impl Into<String>) -> Self {
        Self {
            writer_id: writer_id.into(),
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
        }
    }
}

#[derive(Clone)]
pub struct Fs {
    inner: Arc<FsInner>,
}

struct FsInner {
    store: SharedObjectStore,
    config: FsConfig,
}

pub struct FsBuilder {
    store: SharedObjectStore,
    writer_id: Option<String>,
    writer_version: String,
    lease_duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateNamespaceOptions {
    pub allow_existing: bool,
}

impl Default for CreateNamespaceOptions {
    fn default() -> Self {
        Self {
            allow_existing: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutFileOptions {
    pub behavior: PutFileBehavior,
    pub commit_id: Option<CommitId>,
}

impl Default for PutFileOptions {
    fn default() -> Self {
        Self {
            behavior: PutFileBehavior::CreateOnly,
            commit_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOptions {
    pub recursive: bool,
    pub commit_id: Option<CommitId>,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            commit_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MoveOptions {
    pub commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CopyOptions {
    pub commit_id: Option<CommitId>,
}

impl FsBuilder {
    pub fn new(store: SharedObjectStore) -> Self {
        Self {
            store,
            writer_id: None,
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
        }
    }

    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    pub fn lease_duration_ms(mut self, lease_duration_ms: u64) -> Self {
        self.lease_duration_ms = lease_duration_ms;
        self
    }

    pub fn build(self) -> Result<Fs> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        Fs::open(
            self.store,
            FsConfig {
                writer_id,
                writer_version: self.writer_version,
                lease_duration_ms: self.lease_duration_ms,
            },
        )
    }
}

impl Fs {
    pub fn open(store: SharedObjectStore, config: FsConfig) -> Result<Self> {
        validate_config(&config)?;
        Ok(Self {
            inner: Arc::new(FsInner { store, config }),
        })
    }

    pub fn builder(store: SharedObjectStore) -> FsBuilder {
        FsBuilder::new(store)
    }

    pub fn config(&self) -> &FsConfig {
        &self.inner.config
    }

    pub fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceSummary> {
        Ok(loon_core::bootstrap_namespace(
            self.store(),
            namespace_id,
            &self.mutation_context(),
            options.allow_existing,
        )?)
    }

    pub fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        Ok(loon_core::fork_namespace(
            self.store(),
            source,
            target,
            &self.mutation_context(),
        )?)
    }

    pub fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>> {
        Ok(loon_core::list_namespaces(self.store())?)
    }

    pub fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        Ok(loon_core::resolve_path(
            self.store(),
            namespace_id,
            absolute_path,
        )?)
    }

    pub fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>> {
        Ok(loon_core::list_path(
            self.store(),
            namespace_id,
            absolute_path,
        )?)
    }

    pub fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        Ok(loon_core::read_file_bytes(
            self.store(),
            namespace_id,
            absolute_path,
        )?)
    }

    pub fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        Ok(loon_core::put_file_bytes(
            self.store(),
            namespace_id,
            absolute_path,
            bytes,
            options.behavior,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )?)
    }

    pub fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        Ok(loon_core::put_file_content_ref(
            self.store(),
            namespace_id,
            absolute_path,
            content_ref,
            options.behavior,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )?)
    }

    pub fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<MutationResult> {
        let commit_id = options.commit_id.as_ref().map(CommitId::as_str);
        if options.recursive {
            Ok(loon_core::delete_path(
                self.store(),
                namespace_id,
                absolute_path,
                &self.mutation_context(),
                commit_id,
            )?)
        } else {
            Ok(loon_core::delete_path_non_recursive(
                self.store(),
                namespace_id,
                absolute_path,
                &self.mutation_context(),
                commit_id,
            )?)
        }
    }

    pub fn move_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<MutationResult> {
        Ok(loon_core::move_path(
            self.store(),
            namespace_id,
            from_path,
            to_path,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )?)
    }

    pub fn copy_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> Result<MutationResult> {
        Ok(loon_core::copy_file_path(
            self.store(),
            namespace_id,
            from_path,
            to_path,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )?)
    }

    pub fn begin_upload(&self, namespace_id: &NamespaceId) -> Result<BeginUploadResponse> {
        Ok(loon_core::begin_upload(
            self.store(),
            namespace_id,
            &self.mutation_context(),
        )?)
    }

    pub fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        Ok(loon_core::upload_content(
            self.store(),
            namespace_id,
            upload_id,
            bytes,
            &self.mutation_context(),
        )?)
    }

    pub fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        Ok(loon_core::complete_upload(
            self.store(),
            namespace_id,
            upload_id,
            request,
            &self.mutation_context(),
        )?)
    }

    pub fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        Ok(loon_core::commit_operations(
            self.store(),
            namespace_id,
            request,
            &self.mutation_context(),
        )?)
    }

    pub fn commit_operations_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<Result<CommitResponse>> {
        loon_core::commit_operations_batch(
            self.store(),
            namespace_id,
            requests,
            &self.mutation_context(),
        )
        .into_iter()
        .map(|result| result.map_err(RuntimeError::Core))
        .collect()
    }

    pub fn list_changes_after(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> Result<ChangesResponse> {
        Ok(loon_core::list_changes_after(
            self.store(),
            namespace_id,
            after_seq,
        )?)
    }

    pub fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CreateCheckpointResponse> {
        Ok(loon_core::create_checkpoint(
            self.store(),
            namespace_id,
            &self.mutation_context(),
        )?)
    }

    pub fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        Ok(loon_core::advance_retention_floor(
            self.store(),
            namespace_id,
            &self.mutation_context(),
        )?)
    }

    fn store(&self) -> &(dyn ObjectStore + Send + Sync) {
        self.inner.store.as_ref()
    }

    fn mutation_context(&self) -> MutationContext {
        MutationContext {
            writer_id: self.inner.config.writer_id.clone(),
            writer_version: self.inner.config.writer_version.clone(),
            now_ms: current_time_ms(),
            lease_duration_ms: self.inner.config.lease_duration_ms,
        }
    }
}

fn default_writer_version() -> String {
    format!("loonfs/{}", env!("CARGO_PKG_VERSION"))
}

fn validate_config(config: &FsConfig) -> Result<()> {
    if config.writer_id.trim().is_empty() {
        return Err(RuntimeError::Config(
            "writer_id must not be empty".to_owned(),
        ));
    }
    if config.writer_version.trim().is_empty() {
        return Err(RuntimeError::Config(
            "writer_version must not be empty".to_owned(),
        ));
    }
    if config.lease_duration_ms == 0 {
        return Err(RuntimeError::Config(
            "lease_duration_ms must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
