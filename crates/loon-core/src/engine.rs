use crate::context::MutationContext;
use crate::error::Result as CoreResult;
use crate::namespace::{lifecycle, BootstrapNamespaceError};
use crate::options::{BootstrapOptions, CommitOptions, ForkOptions, ReadOptions, WriteOptions};
use crate::publisher::NamespaceMutationCandidate;
use crate::{basis::VerifiedNamespaceBasis, checkpoint::MetadataTableCache};
use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitRequest, CommitResponse, CompleteUploadRequest,
    CompleteUploadResponse, UploadContentResponse,
};
use loon_api::wire::control::HeadState;
use loon_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq,
    ContentRef, CreateCheckpointResponse, InodeId, ListFileRevisionsResponse, MutationResult,
    NamespaceId, NamespaceSummary, RevisionNo,
};
use loon_objectstore::ObjectStore;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;

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

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    pub fn writer_version(&self) -> &str {
        &self.writer_version
    }

    pub fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_ms
    }

    pub fn read_options(&self) -> &ReadOptions {
        &self.read_options
    }

    pub fn write_options(&self) -> &WriteOptions {
        &self.write_options
    }

    pub fn commit_options(&self) -> &CommitOptions {
        &self.commit_options
    }

    pub fn into_store(self) -> S {
        self.store
    }

    pub fn bootstrap_namespace(
        &self,
        options: BootstrapOptions,
    ) -> Result<NamespaceSummary, BootstrapNamespaceError> {
        lifecycle::bootstrap_namespace(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
            options.allow_existing,
        )
    }

    pub fn fork_namespace(
        &self,
        target: &NamespaceId,
        _options: ForkOptions,
    ) -> CoreResult<NamespaceSummary> {
        lifecycle::fork_namespace(
            &self.store,
            &self.namespace_id,
            target,
            &self.mutation_context(),
        )
    }

    pub fn list_namespaces(&self) -> CoreResult<Vec<NamespaceSummary>> {
        lifecycle::list_namespaces(&self.store)
    }

    pub fn resolve_path(
        &self,
        path: impl AsRef<str>,
        _options: ReadOptions,
    ) -> CoreResult<AuthoritativePathEntry> {
        crate::path::query::resolve_path(&self.store, &self.namespace_id, path.as_ref())
    }

    pub fn resolve_path_from_basis(
        &self,
        basis: &VerifiedNamespaceBasis,
        path: impl AsRef<str>,
        _options: ReadOptions,
    ) -> CoreResult<AuthoritativePathEntry> {
        crate::path::query::resolve_path_from_basis(basis, path.as_ref())
    }

    pub fn resolve_path_from_materialized_tables_at_head(
        &self,
        head: &HeadState,
        path: impl AsRef<str>,
        table_cache: Option<&MetadataTableCache>,
        _options: ReadOptions,
    ) -> CoreResult<Option<AuthoritativePathEntry>> {
        crate::path::query::resolve_path_from_materialized_tables_at_head_with_cache(
            &self.store,
            &self.namespace_id,
            head,
            path.as_ref(),
            table_cache,
        )
    }

    pub fn list_path(
        &self,
        path: impl AsRef<str>,
        _options: ReadOptions,
    ) -> CoreResult<Vec<AuthoritativePathEntry>> {
        crate::path::query::list_path(&self.store, &self.namespace_id, path.as_ref())
    }

    pub fn list_path_from_basis(
        &self,
        basis: &VerifiedNamespaceBasis,
        path: impl AsRef<str>,
        _options: ReadOptions,
    ) -> CoreResult<Vec<AuthoritativePathEntry>> {
        crate::path::query::list_path_from_basis(basis, path.as_ref())
    }

    pub fn list_path_from_materialized_tables_at_head(
        &self,
        head: &HeadState,
        path: impl AsRef<str>,
        table_cache: Option<&MetadataTableCache>,
        _options: ReadOptions,
    ) -> CoreResult<Option<Vec<AuthoritativePathEntry>>> {
        crate::path::query::list_path_from_materialized_tables_at_head_with_cache(
            &self.store,
            &self.namespace_id,
            head,
            path.as_ref(),
            table_cache,
        )
    }

    pub fn read_file(
        &self,
        path: impl AsRef<str>,
        _options: ReadOptions,
    ) -> CoreResult<AuthoritativeFileBytes> {
        crate::path::query::read_file_bytes(&self.store, &self.namespace_id, path.as_ref())
    }

    pub fn read_file_from_basis(
        &self,
        basis: &VerifiedNamespaceBasis,
        path: impl AsRef<str>,
        _options: ReadOptions,
    ) -> CoreResult<AuthoritativeFileBytes> {
        crate::path::query::read_file_bytes_from_basis(&self.store, basis, path.as_ref())
    }

    pub fn list_file_revisions(
        &self,
        path: impl AsRef<str>,
        _options: ReadOptions,
    ) -> CoreResult<ListFileRevisionsResponse> {
        crate::path::query::list_file_revisions(&self.store, &self.namespace_id, path.as_ref())
    }

    pub fn list_file_revisions_from_basis(
        &self,
        basis: &VerifiedNamespaceBasis,
        path: impl AsRef<str>,
        _options: ReadOptions,
    ) -> CoreResult<ListFileRevisionsResponse> {
        crate::path::query::list_file_revisions_from_basis(basis, path.as_ref())
    }

    pub fn list_file_revisions_for_inode(
        &self,
        inode_id: InodeId,
        _options: ReadOptions,
    ) -> CoreResult<ListFileRevisionsResponse> {
        crate::path::query::list_file_revisions_for_inode(&self.store, &self.namespace_id, inode_id)
    }

    pub fn list_file_revisions_for_inode_from_basis(
        &self,
        basis: &VerifiedNamespaceBasis,
        inode_id: InodeId,
        _options: ReadOptions,
    ) -> CoreResult<ListFileRevisionsResponse> {
        crate::path::query::list_file_revisions_for_inode_from_basis(basis, inode_id)
    }

    pub fn read_file_revision(
        &self,
        path: impl AsRef<str>,
        revision_no: RevisionNo,
        _options: ReadOptions,
    ) -> CoreResult<AuthoritativeFileBytes> {
        crate::path::query::read_file_revision_bytes(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            revision_no,
        )
    }

    pub fn read_file_revision_from_basis(
        &self,
        basis: &VerifiedNamespaceBasis,
        path: impl AsRef<str>,
        revision_no: RevisionNo,
        _options: ReadOptions,
    ) -> CoreResult<AuthoritativeFileBytes> {
        crate::path::query::read_file_revision_bytes_from_basis(
            &self.store,
            basis,
            path.as_ref(),
            revision_no,
        )
    }

    pub fn read_file_revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        _options: ReadOptions,
    ) -> CoreResult<Vec<u8>> {
        crate::path::query::read_file_revision_bytes_for_inode(
            &self.store,
            &self.namespace_id,
            inode_id,
            revision_no,
        )
    }

    pub fn read_file_revision_for_inode_from_basis(
        &self,
        basis: &VerifiedNamespaceBasis,
        inode_id: InodeId,
        revision_no: RevisionNo,
        _options: ReadOptions,
    ) -> CoreResult<Vec<u8>> {
        crate::path::query::read_file_revision_bytes_for_inode_from_basis(
            &self.store,
            basis,
            inode_id,
            revision_no,
        )
    }

    pub fn put_file(
        &self,
        path: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::mutation::put_file_bytes(
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
    }

    pub fn put_file_content_ref(
        &self,
        path: impl AsRef<str>,
        content_ref: ContentRef,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::mutation::put_file_content_ref(
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
    }

    pub fn create_dir(
        &self,
        path: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::mutation::create_dir_path(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            &self.mutation_context(),
            options
                .commit_id
                .as_ref()
                .map(|commit_id| commit_id.as_str()),
        )
    }

    pub fn delete_path(
        &self,
        path: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        let commit_id = options
            .commit_id
            .as_ref()
            .map(|commit_id| commit_id.as_str());
        if options.recursive_delete {
            crate::path::mutation::delete_path(
                &self.store,
                &self.namespace_id,
                path.as_ref(),
                &self.mutation_context(),
                commit_id,
            )
        } else {
            crate::path::mutation::delete_path_non_recursive(
                &self.store,
                &self.namespace_id,
                path.as_ref(),
                &self.mutation_context(),
                commit_id,
            )
        }
    }

    pub fn move_path(
        &self,
        source: impl AsRef<str>,
        dest: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::mutation::move_path(
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
    }

    pub fn copy_path(
        &self,
        source: impl AsRef<str>,
        dest: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::mutation::copy_file_path(
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
    }

    pub fn restore_file_revision(
        &self,
        path: impl AsRef<str>,
        source_revision_no: RevisionNo,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::mutation::restore_file_revision(
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
    }

    pub fn commit_operations(
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
    }

    pub fn commit_operations_batch(
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
    }

    pub fn publish_namespace_mutations_batch(
        &self,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<CoreResult<CommitResponse>> {
        crate::publisher::publish_namespace_mutations_batch(
            &self.store,
            &self.namespace_id,
            candidates,
            &self.mutation_context(),
        )
    }

    pub fn list_changes_after(&self, after_seq: ChangeSeq) -> CoreResult<ChangesResponse> {
        crate::protocol::list_changes_after(&self.store, &self.namespace_id, after_seq)
    }

    pub fn begin_upload(&self) -> CoreResult<BeginUploadResponse> {
        crate::protocol::begin_upload(&self.store, &self.namespace_id, &self.mutation_context())
    }

    pub fn upload_content(
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
    }

    pub fn complete_upload(
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
    }

    pub fn create_checkpoint(&self) -> CoreResult<CreateCheckpointResponse> {
        crate::checkpoint::create_checkpoint(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
        )
    }

    pub fn advance_retention_floor(&self) -> CoreResult<AdvanceRetentionResponse> {
        crate::checkpoint::advance_retention_floor(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
        )
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
    pub fn namespace(mut self, namespace_id: NamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self
    }

    pub fn writer(mut self, writer_id: impl Into<String>) -> Self {
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

    pub fn read_options(mut self, options: ReadOptions) -> Self {
        self.read_options = options;
        self
    }

    pub fn write_options(mut self, options: WriteOptions) -> Self {
        self.write_options = options;
        self
    }

    pub fn commit_options(mut self, options: CommitOptions) -> Self {
        self.commit_options = options;
        self
    }

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

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum NamespaceEngineBuildError {
    #[error("namespace is required")]
    MissingNamespace,
    #[error("writer identity is required")]
    MissingWriter,
    #[error("writer identity must not be empty")]
    EmptyWriter,
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
}

fn default_writer_version() -> String {
    format!("loon-core/{}", env!("CARGO_PKG_VERSION"))
}

fn current_time_ms() -> u64 {
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
        assert_eq!(engine.read_options(), &ReadOptions::default());
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
