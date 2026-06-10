use crate::context::MutationContext;
use crate::error::Result as CoreResult;
use crate::namespace::basis::{load_verified_namespace_basis, VerifiedNamespaceBasis};
use crate::namespace::{bootstrap, catalog, fork, BootstrapNamespaceError};
use crate::options::{
    BootstrapOptions, CommitOptions, ForkOptions, ReadOptions, ReadSource, WriteOptions,
};
use crate::publisher::NamespaceMutationCandidate;
use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitRequest, CommitResponse, CompleteUploadRequest,
    CompleteUploadResponse, UploadContentResponse,
};
use loon_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq,
    ContentRef, CreateCheckpointResponse, InodeId, ListFileRevisionsResponse, MutationResult,
    NamespaceId, NamespaceSummary, RevisionNo,
};
use loon_objectstore::ObjectStore;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;

/// A namespace-scoped core API.
///
/// `NamespaceEngine` owns an object store handle plus the writer identity used
/// for mutations. It is the main entrypoint for direct reads, path writes,
/// explicit commits, uploads, checkpoints, and retention work.
#[derive(Debug)]
pub struct NamespaceEngine<S> {
    store: S,
    namespace_id: NamespaceId,
    writer_id: String,
    writer_version: String,
    lease_duration_ms: u64,
    read_options: ReadOptions,
    write_options: WriteOptions,
    commit_options: CommitOptions,
}

impl<S: ObjectStore> NamespaceEngine<S> {
    /// Starts an engine builder for the supplied object store.
    ///
    /// The builder requires a namespace id and writer id before it can build.
    pub fn builder(store: S) -> NamespaceEngineBuilder<S> {
        NamespaceEngineBuilder {
            store,
            namespace_id: None,
            writer_id: None,
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
            read_options: ReadOptions::default(),
            write_options: WriteOptions::default(),
            commit_options: CommitOptions::default(),
        }
    }

    /// Returns the namespace this engine is bound to.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the writer id used for leases and commit publication.
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Returns the writer version reported in mutation context.
    pub fn writer_version(&self) -> &str {
        &self.writer_version
    }

    /// Returns the lease duration used by write operations.
    pub fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_ms
    }

    /// Returns the default read options configured on the builder.
    pub fn read_options(&self) -> &ReadOptions {
        &self.read_options
    }

    /// Returns the default write options configured on the builder.
    pub fn write_options(&self) -> &WriteOptions {
        &self.write_options
    }

    /// Returns the default explicit-commit options configured on the builder.
    pub fn commit_options(&self) -> &CommitOptions {
        &self.commit_options
    }

    /// Consumes the engine and returns the underlying object store.
    pub fn into_store(self) -> S {
        self.store
    }

    /// Creates the namespace if it does not already exist.
    ///
    /// Use this before normal reads and writes for a new namespace.
    pub async fn bootstrap_namespace(
        &self,
        options: BootstrapOptions,
    ) -> Result<NamespaceSummary, BootstrapNamespaceError> {
        bootstrap::bootstrap_namespace(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
            options.allow_existing,
        )
        .await
    }

    /// Creates a new namespace at the current head of this namespace.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    pub async fn fork_namespace(
        &self,
        target: &NamespaceId,
        _options: ForkOptions,
    ) -> CoreResult<NamespaceSummary> {
        fork::fork_namespace(
            &self.store,
            &self.namespace_id,
            target,
            &self.mutation_context(),
        )
        .await
    }

    /// Lists complete namespaces visible in the object store.
    pub async fn list_namespaces(&self) -> CoreResult<Vec<NamespaceSummary>> {
        catalog::list_namespaces(&self.store).await
    }

    /// Resolves one absolute path to the authoritative entry at the current head.
    pub async fn resolve_path(
        &self,
        path: impl AsRef<str>,
        options: ReadOptions,
    ) -> CoreResult<AuthoritativePathEntry> {
        let path = path.as_ref();
        match options.source() {
            ReadSource::PreferMaterialized => {
                if let Some(entry) = crate::path::query::resolve_path_from_materialized_tables(
                    &self.store,
                    &self.namespace_id,
                    path,
                )
                .await?
                {
                    return Ok(entry);
                }
            }
            ReadSource::MaterializedTablesAtHead { head, table_cache } => {
                if let Some(entry) =
                    crate::path::query::resolve_path_from_materialized_tables_at_head_with_cache(
                        &self.store,
                        &self.namespace_id,
                        head,
                        path,
                        table_cache.as_deref(),
                    )
                    .await?
                {
                    return Ok(entry);
                }
            }
            ReadSource::FullBasis | ReadSource::VerifiedBasis(_) => {}
        }
        let basis = self.basis_for_read_options(options).await?;
        crate::path::query::resolve_path_from_basis(&basis, path)
    }

    /// Lists the children of a directory path.
    pub async fn list_path(
        &self,
        path: impl AsRef<str>,
        options: ReadOptions,
    ) -> CoreResult<Vec<AuthoritativePathEntry>> {
        let path = path.as_ref();
        match options.source() {
            ReadSource::PreferMaterialized => {
                if let Some(entries) = crate::path::query::list_path_from_materialized_tables(
                    &self.store,
                    &self.namespace_id,
                    path,
                )
                .await?
                {
                    return Ok(entries);
                }
            }
            ReadSource::MaterializedTablesAtHead { head, table_cache } => {
                if let Some(entries) =
                    crate::path::query::list_path_from_materialized_tables_at_head_with_cache(
                        &self.store,
                        &self.namespace_id,
                        head,
                        path,
                        table_cache.as_deref(),
                    )
                    .await?
                {
                    return Ok(entries);
                }
            }
            ReadSource::FullBasis | ReadSource::VerifiedBasis(_) => {}
        }
        let basis = self.basis_for_read_options(options).await?;
        crate::path::query::list_path_from_basis(&basis, path)
    }

    /// Reads the current bytes for a file path.
    ///
    /// Content bytes are validated against the file's `content_ref` before they
    /// are returned.
    pub async fn read_file(
        &self,
        path: impl AsRef<str>,
        options: ReadOptions,
    ) -> CoreResult<AuthoritativeFileBytes> {
        let basis = self.basis_for_read_options(options).await?;
        crate::path::query::read_file_bytes_from_basis(&self.store, &basis, path.as_ref()).await
    }

    /// Lists retained revisions for the file currently visible at `path`.
    pub async fn list_file_revisions(
        &self,
        path: impl AsRef<str>,
        options: ReadOptions,
    ) -> CoreResult<ListFileRevisionsResponse> {
        let basis = self.basis_for_read_options(options).await?;
        crate::path::query::list_file_revisions_from_basis(&basis, path.as_ref())
    }

    /// Lists retained revisions for a file inode, independent of its current path.
    pub async fn list_file_revisions_for_inode(
        &self,
        inode_id: InodeId,
        options: ReadOptions,
    ) -> CoreResult<ListFileRevisionsResponse> {
        let basis = self.basis_for_read_options(options).await?;
        crate::path::query::list_file_revisions_for_inode_from_basis(&basis, inode_id)
    }

    /// Reads a retained revision for the file currently visible at `path`.
    pub async fn read_file_revision(
        &self,
        path: impl AsRef<str>,
        revision_no: RevisionNo,
        options: ReadOptions,
    ) -> CoreResult<AuthoritativeFileBytes> {
        let basis = self.basis_for_read_options(options).await?;
        crate::path::query::read_file_revision_bytes_from_basis(
            &self.store,
            &basis,
            path.as_ref(),
            revision_no,
        )
        .await
    }

    /// Reads a retained revision by stable inode id.
    pub async fn read_file_revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: ReadOptions,
    ) -> CoreResult<Vec<u8>> {
        let basis = self.basis_for_read_options(options).await?;
        crate::path::query::read_file_revision_bytes_for_inode_from_basis(
            &self.store,
            &basis,
            inode_id,
            revision_no,
        )
        .await
    }

    /// Writes file bytes to a path.
    ///
    /// The bytes become durable content first. Metadata is published only after
    /// that content is safe to reference.
    pub async fn put_file(
        &self,
        path: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::put_file_bytes(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            bytes.as_ref(),
            options.put_file_behavior,
            &self.mutation_context(),
            options
                .commit_id
                .as_ref()
                .map(|commit_id| commit_id.as_str()),
        )
        .await
    }

    /// Publishes a file revision that points at an already-durable content ref.
    ///
    /// Use this when the caller staged content separately.
    pub async fn put_file_content_ref(
        &self,
        path: impl AsRef<str>,
        content_ref: ContentRef,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::put_file_content_ref(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            content_ref,
            options.put_file_behavior,
            &self.mutation_context(),
            options
                .commit_id
                .as_ref()
                .map(|commit_id| commit_id.as_str()),
        )
        .await
    }

    /// Creates a directory at an absolute path.
    pub async fn create_dir(
        &self,
        path: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::create_dir_path(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            &self.mutation_context(),
            options
                .commit_id
                .as_ref()
                .map(|commit_id| commit_id.as_str()),
        )
        .await
    }

    /// Deletes a file or directory path.
    pub async fn delete_path(
        &self,
        path: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        let commit_id = options
            .commit_id
            .as_ref()
            .map(|commit_id| commit_id.as_str());
        if options.recursive_delete {
            crate::path::write::ops::delete_path(
                &self.store,
                &self.namespace_id,
                path.as_ref(),
                &self.mutation_context(),
                commit_id,
            )
            .await
        } else {
            crate::path::write::ops::delete_path_non_recursive(
                &self.store,
                &self.namespace_id,
                path.as_ref(),
                &self.mutation_context(),
                commit_id,
            )
            .await
        }
    }

    /// Moves a path within the same namespace.
    pub async fn move_path(
        &self,
        source: impl AsRef<str>,
        dest: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::move_path(
            &self.store,
            &self.namespace_id,
            source.as_ref(),
            dest.as_ref(),
            &self.mutation_context(),
            options
                .commit_id
                .as_ref()
                .map(|commit_id| commit_id.as_str()),
        )
        .await
    }

    /// Copies a file path within the same namespace.
    pub async fn copy_path(
        &self,
        source: impl AsRef<str>,
        dest: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::copy_file_path(
            &self.store,
            &self.namespace_id,
            source.as_ref(),
            dest.as_ref(),
            &self.mutation_context(),
            options
                .commit_id
                .as_ref()
                .map(|commit_id| commit_id.as_str()),
        )
        .await
    }

    /// Restores a prior file revision by appending a new current revision.
    pub async fn restore_file_revision(
        &self,
        path: impl AsRef<str>,
        source_revision_no: RevisionNo,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::restore_file_revision(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            source_revision_no,
            &self.mutation_context(),
            options
                .commit_id
                .as_ref()
                .map(|commit_id| commit_id.as_str()),
        )
        .await
    }

    /// Submits one explicit semantic commit request.
    ///
    /// This is the lower-level surface used by clients that need their own
    /// commit ids, preconditions, and operation lists.
    pub async fn commit_operations(
        &self,
        request: CommitRequest,
        _options: CommitOptions,
    ) -> CoreResult<CommitResponse> {
        crate::protocol::commit_operations(
            &self.store,
            &self.namespace_id,
            request,
            &self.mutation_context(),
        )
        .await
    }

    /// Submits explicit semantic commit requests as one publication attempt.
    pub async fn commit_operations_batch(
        &self,
        requests: Vec<CommitRequest>,
        _options: CommitOptions,
    ) -> Vec<CoreResult<CommitResponse>> {
        crate::protocol::commit_operations_batch(
            &self.store,
            &self.namespace_id,
            requests,
            &self.mutation_context(),
        )
        .await
    }

    /// Publishes already-classified namespace mutation candidates.
    ///
    /// Server code uses this to batch path intents and explicit commits through
    /// one namespace publisher.
    pub async fn publish_namespace_mutations_batch(
        &self,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<CoreResult<CommitResponse>> {
        crate::publisher::publish_namespace_mutations_batch(
            &self.store,
            &self.namespace_id,
            candidates,
            &self.mutation_context(),
        )
        .await
    }

    /// Reads committed changes after `after_seq`.
    pub async fn list_changes_after(&self, after_seq: ChangeSeq) -> CoreResult<ChangesResponse> {
        crate::protocol::list_changes_after(&self.store, &self.namespace_id, after_seq).await
    }

    /// Starts a durable upload session for this namespace.
    pub async fn begin_upload(&self) -> CoreResult<BeginUploadResponse> {
        crate::protocol::begin_upload(&self.store, &self.namespace_id, &self.mutation_context())
            .await
    }

    /// Uploads whole-file content into an upload session.
    pub async fn upload_content(
        &self,
        upload_id: &str,
        bytes: &[u8],
    ) -> CoreResult<UploadContentResponse> {
        crate::protocol::upload_content(
            &self.store,
            &self.namespace_id,
            upload_id,
            bytes,
            &self.mutation_context(),
        )
        .await
    }

    /// Completes an upload session when the expected content ref matches.
    pub async fn complete_upload(
        &self,
        upload_id: &str,
        request: &CompleteUploadRequest,
    ) -> CoreResult<CompleteUploadResponse> {
        crate::protocol::complete_upload(
            &self.store,
            &self.namespace_id,
            upload_id,
            request,
            &self.mutation_context(),
        )
        .await
    }

    /// Creates or reuses a checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest version for retention/provenance. If the
    /// current head has no manifest yet, this first publishes one for the
    /// current durable namespace state; it is not a request to compact metadata.
    pub async fn create_checkpoint(&self) -> CoreResult<CreateCheckpointResponse> {
        crate::checkpoint::create_checkpoint(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
        )
        .await
    }

    /// Advances the retention floor when a verified checkpoint makes it safe.
    pub async fn advance_retention_floor(&self) -> CoreResult<AdvanceRetentionResponse> {
        crate::checkpoint::advance_retention_floor(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
        )
        .await
    }

    async fn basis_for_read_options(
        &self,
        options: ReadOptions,
    ) -> CoreResult<Arc<VerifiedNamespaceBasis>> {
        match options.into_source() {
            ReadSource::VerifiedBasis(basis) => Ok(basis),
            ReadSource::PreferMaterialized
            | ReadSource::FullBasis
            | ReadSource::MaterializedTablesAtHead { .. } => Ok(Arc::new(
                load_verified_namespace_basis(&self.store, &self.namespace_id).await?,
            )),
        }
    }

    fn mutation_context(&self) -> MutationContext {
        MutationContext {
            writer_id: self.writer_id.clone(),
            writer_version: self.writer_version.clone(),
            now_ms: current_time_ms(),
            lease_duration_ms: self.lease_duration_ms,
        }
    }
}

/// Builder for [`NamespaceEngine`].
///
/// The builder keeps construction explicit: choose a namespace, choose the
/// writer identity, then build the engine.
#[derive(Debug)]
pub struct NamespaceEngineBuilder<S> {
    store: S,
    namespace_id: Option<NamespaceId>,
    writer_id: Option<String>,
    writer_version: String,
    lease_duration_ms: u64,
    read_options: ReadOptions,
    write_options: WriteOptions,
    commit_options: CommitOptions,
}

impl<S: ObjectStore> NamespaceEngineBuilder<S> {
    /// Sets the namespace this engine will operate on.
    pub fn namespace(mut self, namespace_id: NamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self
    }

    /// Sets the writer identity used for leases and commits.
    pub fn writer(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Sets a human-readable writer version.
    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    /// Sets how long this writer's namespace lease should remain valid.
    pub fn lease_duration_ms(mut self, lease_duration_ms: u64) -> Self {
        self.lease_duration_ms = lease_duration_ms;
        self
    }

    /// Sets default read options stored on the engine.
    pub fn read_options(mut self, options: ReadOptions) -> Self {
        self.read_options = options;
        self
    }

    /// Sets default write options stored on the engine.
    pub fn write_options(mut self, options: WriteOptions) -> Self {
        self.write_options = options;
        self
    }

    /// Sets default explicit-commit options stored on the engine.
    pub fn commit_options(mut self, options: CommitOptions) -> Self {
        self.commit_options = options;
        self
    }

    /// Builds the engine after required fields are present.
    pub fn build(self) -> Result<NamespaceEngine<S>, NamespaceEngineBuildError> {
        let namespace_id = self
            .namespace_id
            .ok_or(NamespaceEngineBuildError::MissingNamespace)?;
        let writer_id = self
            .writer_id
            .ok_or(NamespaceEngineBuildError::MissingWriter)?;
        if writer_id.trim().is_empty() {
            return Err(NamespaceEngineBuildError::EmptyWriter);
        }
        if self.writer_version.trim().is_empty() {
            return Err(NamespaceEngineBuildError::EmptyWriterVersion);
        }

        Ok(NamespaceEngine {
            store: self.store,
            namespace_id,
            writer_id,
            writer_version: self.writer_version,
            lease_duration_ms: self.lease_duration_ms,
            read_options: self.read_options,
            write_options: self.write_options,
            commit_options: self.commit_options,
        })
    }
}

/// Error returned when a [`NamespaceEngine`] cannot be built.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum NamespaceEngineBuildError {
    /// A namespace id was not supplied.
    #[error("namespace is required")]
    MissingNamespace,
    /// A writer id was not supplied.
    #[error("writer identity is required")]
    MissingWriter,
    /// The writer id was empty or whitespace.
    #[error("writer identity must not be empty")]
    EmptyWriter,
    /// The writer version was empty or whitespace.
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
}

fn default_writer_version() -> String {
    format!("loon-core/{}", env!("CARGO_PKG_VERSION"))
}

#[allow(clippy::disallowed_methods)]
fn current_time_ms() -> u64 {
    // Engine wrappers set request timestamps at this API boundary; core replay remains deterministic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_objectstore::fs::LocalFsStore;
    use tempfile::tempdir;

    #[test]
    fn namespace_engine_builds_with_required_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let engine = NamespaceEngine::builder(store)
            .namespace(namespace_id.clone())
            .writer("writer-a")
            .build()
            .expect("engine builds");

        assert_eq!(engine.namespace_id(), &namespace_id);
        assert_eq!(engine.writer_id(), "writer-a");
        assert!(!engine.writer_version().is_empty());
        assert_eq!(engine.lease_duration_ms(), DEFAULT_LEASE_DURATION_MS);
        assert!(matches!(
            engine.read_options().source(),
            ReadSource::PreferMaterialized
        ));
        assert_eq!(engine.write_options(), &WriteOptions::default());
        assert_eq!(engine.commit_options(), &CommitOptions::default());
    }

    #[test]
    fn namespace_engine_builder_rejects_missing_required_fields() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let err = NamespaceEngine::builder(store)
            .build()
            .expect_err("missing namespace");
        assert_eq!(err, NamespaceEngineBuildError::MissingNamespace);

        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let err = NamespaceEngine::builder(store)
            .namespace(NamespaceId::parse("demo").expect("valid namespace id"))
            .build()
            .expect_err("missing writer");
        assert_eq!(err, NamespaceEngineBuildError::MissingWriter);
    }
}
