//! [`NamespaceEngine`]: the namespace-scoped entry point for reads, writes,
//! uploads, checkpoints, and maintenance.

use crate::cache::{MetadataTableCache, WalTailProjectionCache};
use crate::commit_engine::NamespaceMutationCandidate;
use crate::context::MutationContext;
use crate::error::Result as CoreResult;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::{bootstrap, delete, fork, BootstrapNamespaceError};
use crate::options::{BootstrapOptions, DeleteNamespaceOptions};
use crate::path::read::{load_metadata_view, LoadedMetadataView, ReadLoadContext};
use crate::storage::content_admission::PreparedContent;
use loonfs_api::generated_id;
use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitResponse,
    CompleteUploadRequest, CompleteUploadResponse, UploadContentResponse,
};
use loonfs_api::wire::control::{CheckpointOwner, HeadState};
use loonfs_api::EffectiveLimit;
use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq,
    CheckpointId, ContentRef, CreateCheckpointResponse, DirectoryPageCursor, FileRevision,
    FileRevisionsPageCursor, FlushWalResponse, InodeId, ManifestId, ManifestObjectId, NamespaceId,
    NamespaceSummary, Page, PageRequest, ReleaseCheckpointResponse, RevisionNo, UploadId,
};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// The pinned inputs the runtime resolves once per read: the head anchored
/// to its manifest, the shared caches, and (when the runtime has it) the
/// namespace's immutable catalog pair.
///
/// This is the runtime seam: the `loonfs` crate pins one context per
/// request and fans every read of that request through it, so the whole
/// request observes a single snapshot and shares the caches. It is a
/// sanctioned public hook (STYLE, "Harness hooks are sanctioned"), not an
/// application API — embedded applications use the `loonfs` handles, which
/// drive this seam internally.
#[derive(Debug, Clone)]
pub struct RuntimeReadContext {
    pub head: HeadState,
    pub head_etag: String,
    pub manifest_id: ManifestId,
    pub manifest_object_id: ManifestObjectId,
    pub table_cache: Arc<MetadataTableCache>,
    pub tail_cache: Arc<WalTailProjectionCache>,
    pub catalog: Option<VerifiedNamespaceCatalogEntry>,
}

fn runtime_read_load_context(options: &RuntimeReadContext) -> ReadLoadContext<'_> {
    ReadLoadContext::pinned_head(
        &options.head,
        Some(options.head_etag.as_str()),
        options.manifest_id,
        &options.manifest_object_id,
        Some(&options.table_cache),
        Some(&options.tail_cache),
        options.catalog.as_ref(),
    )
}

/// Internal target used by server integrations before they mint a presigned URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectPutUploadTarget {
    pub content_ref: ContentRef,
    pub object_key: String,
}

/// Internal response for preparing a direct_put session before URL signing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginDirectPutUploadTargetResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    pub target: DirectPutUploadTarget,
}

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
    writer_session_id: String,
    writer_version: String,
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
            writer_session_id: None,
            writer_version: default_writer_version(),
        }
    }

    /// Returns the namespace this engine is bound to.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the writer id used for epoch acquisition and commit publication.
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Returns the unique writer session id for this engine instance.
    pub fn writer_session_id(&self) -> &str {
        &self.writer_session_id
    }

    /// Returns the writer version reported in mutation context.
    pub fn writer_version(&self) -> &str {
        &self.writer_version
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
            &self.mutation_context()?,
            options.allow_existing,
        )
        .await
    }

    /// Creates a new namespace at the current head of this namespace.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    pub async fn fork_namespace(&self, target: &NamespaceId) -> CoreResult<NamespaceSummary> {
        fork::fork_namespace(
            &self.store,
            &self.namespace_id,
            target,
            &self.mutation_context()?,
        )
        .await
    }

    /// Deletes this namespace: a fenced, terminal head-state transition.
    /// Commits acknowledged before the swap stay committed; everything that
    /// observes the deleted head afterward fails with `namespace_deleted`.
    pub async fn delete_namespace(
        &self,
        options: DeleteNamespaceOptions,
    ) -> CoreResult<loonfs_api::DeleteNamespaceResponse> {
        delete::delete_namespace(
            &self.store,
            &self.namespace_id,
            options,
            &self.mutation_context()?,
        )
        .await
    }

    /// Stats one path against the pinned runtime read context.
    pub async fn resolve_path_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        options: &RuntimeReadContext,
    ) -> CoreResult<AuthoritativePathEntry> {
        self.resolve_path_with_context(path.as_ref(), runtime_read_load_context(options))
            .await
    }

    /// Lists one directory page against the pinned runtime read context.
    pub async fn list_path_page_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<DirectoryPageCursor>,
        options: &RuntimeReadContext,
    ) -> CoreResult<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        self.list_path_page_with_context(path.as_ref(), request, runtime_read_load_context(options))
            .await
    }

    /// Reads file content against the pinned runtime read context.
    pub async fn read_file_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        options: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> CoreResult<AuthoritativeFileBytes> {
        self.read_file_with_context(
            path.as_ref(),
            runtime_read_load_context(options),
            max_content_bytes,
        )
        .await
    }

    /// Loads the coherent read view passed to `loonfs-grep`.
    ///
    /// This is part of the read surface consumed by `loonfs-grep`.
    pub async fn load_grep_view_with_runtime_context<'a>(
        &'a self,
        options: &'a RuntimeReadContext,
    ) -> CoreResult<LoadedMetadataView<'a, S>> {
        self.load_read_view(runtime_read_load_context(options))
            .await
    }

    /// Lists one revision page for a path against the pinned runtime read context.
    pub async fn list_file_revisions_page_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<FileRevisionsPageCursor>,
        options: &RuntimeReadContext,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        self.list_file_revisions_page_with_context(
            path.as_ref(),
            request,
            runtime_read_load_context(options),
        )
        .await
    }

    /// Lists one revision page for an inode against the pinned runtime read context.
    pub async fn list_file_revisions_for_inode_page_with_runtime_context(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        options: &RuntimeReadContext,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        self.list_file_revisions_for_inode_page_with_context(
            inode_id,
            request,
            runtime_read_load_context(options),
        )
        .await
    }

    /// Reads one revision's content by path against the pinned runtime
    /// read context.
    pub async fn read_file_revision_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        revision_no: RevisionNo,
        options: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> CoreResult<AuthoritativeFileBytes> {
        self.read_file_revision_with_context(
            path.as_ref(),
            revision_no,
            runtime_read_load_context(options),
            max_content_bytes,
        )
        .await
    }

    /// Reads one revision's content against the pinned runtime read context.
    pub async fn read_file_revision_for_inode_with_runtime_context(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> CoreResult<Vec<u8>> {
        self.read_file_revision_for_inode_with_context(
            inode_id,
            revision_no,
            runtime_read_load_context(options),
            max_content_bytes,
        )
        .await
    }

    async fn load_read_view<'a>(
        &'a self,
        context: ReadLoadContext<'a>,
    ) -> CoreResult<LoadedMetadataView<'a, S>> {
        load_metadata_view(&self.store, &self.namespace_id, context).await
    }

    async fn resolve_path_with_context(
        &self,
        path: &str,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<AuthoritativePathEntry> {
        let view = self.load_read_view(context).await?;
        view.resolve_path(path).await
    }

    async fn list_path_page_with_context(
        &self,
        path: &str,
        request: PageRequest<DirectoryPageCursor>,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_path_page(path, request).await
    }

    async fn read_file_with_context(
        &self,
        path: &str,
        context: ReadLoadContext<'_>,
        max_content_bytes: Option<u64>,
    ) -> CoreResult<AuthoritativeFileBytes> {
        let view = self.load_read_view(context).await?;
        view.read_file_bytes(&self.store, path, max_content_bytes)
            .await
    }

    async fn list_file_revisions_page_with_context(
        &self,
        path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_page(path, request).await
    }

    async fn list_file_revisions_for_inode_page_with_context(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_for_inode_page(inode_id, request)
            .await
    }

    async fn read_file_revision_with_context(
        &self,
        path: &str,
        revision_no: RevisionNo,
        context: ReadLoadContext<'_>,
        max_content_bytes: Option<u64>,
    ) -> CoreResult<AuthoritativeFileBytes> {
        let view = self.load_read_view(context).await?;
        view.read_file_revision_bytes(&self.store, path, revision_no, max_content_bytes)
            .await
    }

    async fn read_file_revision_for_inode_with_context(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        context: ReadLoadContext<'_>,
        max_content_bytes: Option<u64>,
    ) -> CoreResult<Vec<u8>> {
        let view = self.load_read_view(context).await?;
        view.read_file_revision_bytes_for_inode(
            &self.store,
            inode_id,
            revision_no,
            max_content_bytes,
        )
        .await
    }

    /// Publishes already-classified mutation candidates as one batch: one WAL
    /// segment, one head compare-and-swap, one result per candidate in order.
    pub async fn publish_namespace_mutations_batch(
        &self,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<CoreResult<CommitResponse>> {
        let context = match self.mutation_context() {
            Ok(context) => context,
            Err(error) => return candidates.iter().map(|_| Err(error.clone())).collect(),
        };
        crate::commit_engine::publish_namespace_mutations_batch(
            &self.store,
            &self.namespace_id,
            candidates,
            &context,
        )
        .await
    }

    /// Reads up to `limit` committed changes after `after_seq`.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
    ) -> CoreResult<ChangesResponse> {
        crate::protocol::list_changes_after(&self.store, &self.namespace_id, after_seq, limit).await
    }

    /// Starts a durable upload session with explicit transport options.
    pub async fn begin_upload(
        &self,
        request: BeginUploadRequest,
    ) -> CoreResult<BeginUploadResponse> {
        crate::protocol::begin_upload(
            &self.store,
            &self.namespace_id,
            request,
            &self.mutation_context()?,
        )
        .await
    }

    /// Starts a direct_put upload session and returns the internal object key to sign.
    pub async fn begin_direct_put_upload_target(
        &self,
        content_ref: ContentRef,
    ) -> CoreResult<BeginDirectPutUploadTargetResponse> {
        crate::protocol::begin_direct_put_upload_target(
            &self.store,
            &self.namespace_id,
            content_ref,
            &self.mutation_context()?,
        )
        .await
    }

    /// Uploads whole-file content into an upload session.
    pub async fn upload_content(
        &self,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> CoreResult<UploadContentResponse> {
        crate::protocol::upload_content(
            &self.store,
            &self.namespace_id,
            upload_id,
            bytes,
            &self.mutation_context()?,
        )
        .await
    }

    /// Completes an upload session when the expected content ref matches.
    pub async fn complete_upload(
        &self,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> CoreResult<CompleteUploadResponse> {
        let (response, _) = self.complete_upload_prepared(upload_id, request).await?;
        Ok(response)
    }

    /// Completes an upload session and returns proof for later publication.
    ///
    /// Service-proxied completion performs no content-blob I/O. Direct-put
    /// completion performs one content-blob HEAD and no content-blob GET.
    pub async fn complete_upload_prepared(
        &self,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> CoreResult<(CompleteUploadResponse, PreparedContent)> {
        let catalog = crate::namespace::catalog::load_namespace_catalog_entry(
            &self.store,
            &self.namespace_id,
        )
        .await?;
        self.complete_upload_prepared_with_catalog(&catalog, upload_id, request)
            .await
    }

    /// Completes an upload with a namespace catalog binding already resolved
    /// by the runtime.
    pub async fn complete_upload_prepared_with_catalog(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> CoreResult<(CompleteUploadResponse, PreparedContent)> {
        if catalog.namespace_id() != &self.namespace_id {
            return Err(
                crate::commit_engine::ContentPreparationError::ContentNotPrepared {
                    content_ref_digest: request.content_ref.digest.clone(),
                }
                .into(),
            );
        }
        crate::protocol::complete_upload(
            &self.store,
            &self.namespace_id,
            catalog.content_store_id(),
            upload_id,
            request,
            &self.mutation_context()?,
        )
        .await
    }

    /// Creates or reuses a named checkpoint pinning the current namespace
    /// head for the calling user.
    ///
    /// A checkpoint pins a manifest version for retention/provenance. If the
    /// current head has no manifest yet, this first publishes one for the
    /// current durable namespace state; it is not a request to compact
    /// metadata. `ttl_ms` computes the record's expiry from the engine's
    /// clock; absent means the pin holds until explicitly released.
    pub async fn create_checkpoint(
        &self,
        name: String,
        ttl_ms: Option<u64>,
    ) -> CoreResult<CreateCheckpointResponse> {
        let context = self.mutation_context()?;
        let expires_at_ms = ttl_ms.map(|ttl_ms| context.now_ms.saturating_add(ttl_ms));
        crate::checkpoint::create_checkpoint(
            &self.store,
            &self.namespace_id,
            CheckpointOwner::User { name },
            expires_at_ms,
            &context,
        )
        .await
    }

    /// Releases a user-owned checkpoint by id.
    ///
    /// Idempotent: releasing an already-released or reaped record succeeds.
    /// The record is reaped by a later garbage-collection pass; its basis
    /// becomes collectable only on the pass after that.
    pub async fn release_checkpoint(
        &self,
        checkpoint_id: &CheckpointId,
    ) -> CoreResult<ReleaseCheckpointResponse> {
        crate::checkpoint::release_checkpoint(
            &self.store,
            &self.namespace_id,
            checkpoint_id,
            &self.mutation_context()?,
        )
        .await
    }

    /// Flushes the visible WAL tail and advances `metadata/root.json` to a
    /// manifest covering the current head.
    ///
    /// This is the latest-state maintenance operation: it absorbs the visible
    /// WAL tail into a published manifest and advances the root, creating no
    /// checkpoint record. Superseded manifests become garbage-collection
    /// candidates once nothing pins them.
    pub async fn flush_wal(&self) -> CoreResult<FlushWalResponse> {
        crate::checkpoint::flush_wal(&self.store, &self.namespace_id, &self.mutation_context()?)
            .await
    }

    /// Runs at most one metadata reorganization unit: folds one family
    /// group's L0 delta rows into new base segments and publishes a manifest
    /// swapping that group's references. Checkpoints only append L0 runs, so
    /// calling this from maintenance is what keeps read fan-out bounded.
    /// Repeat until the report says `NotNeeded`; every call re-reads durable
    /// state, so interrupted reorganizations resume from the live manifest.
    pub async fn reorganize_metadata(
        &self,
    ) -> CoreResult<crate::checkpoint::MetadataReorganizeReport> {
        crate::checkpoint::reorganize_metadata_step(
            &self.store,
            &self.namespace_id,
            &self.mutation_context()?,
            crate::checkpoint::MetadataLsmPolicy::default(),
        )
        .await
    }

    /// Advances the retention floor when a verified checkpoint makes it safe.
    pub async fn advance_retention_floor(&self) -> CoreResult<AdvanceRetentionResponse> {
        crate::checkpoint::advance_retention_floor(
            &self.store,
            &self.namespace_id,
            &self.mutation_context()?,
        )
        .await
    }

    fn mutation_context(&self) -> CoreResult<MutationContext> {
        Ok(MutationContext {
            writer_id: self.writer_id.clone(),
            writer_session_id: self.writer_session_id.clone(),
            writer_version: self.writer_version.clone(),
            now_ms: current_time_ms()?,
        })
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
    writer_session_id: Option<String>,
    writer_version: String,
}

impl<S: ObjectStore> NamespaceEngineBuilder<S> {
    /// Sets the namespace this engine will operate on.
    pub fn namespace_id(mut self, namespace_id: NamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self
    }

    /// Sets the writer identity used for epoch acquisition and commits.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Pins the writer session id instead of generating one — the runtime
    /// seam for hosts that manage session identity themselves.
    pub fn writer_session_id(mut self, writer_session_id: impl Into<String>) -> Self {
        self.writer_session_id = Some(writer_session_id.into());
        self
    }

    /// Sets a human-readable writer version.
    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
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
            writer_session_id: self
                .writer_session_id
                .unwrap_or_else(|| generated_id("wrs")),
            writer_version: self.writer_version,
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
    format!("loonfs-core/{}", env!("CARGO_PKG_VERSION"))
}

#[allow(clippy::disallowed_methods)]
fn current_time_ms() -> CoreResult<u64> {
    // Engine wrappers set request timestamps at this API boundary; core replay remains deterministic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|_| {
            crate::error::CoreError::Internal("system time is before unix epoch".to_owned())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    #[test]
    fn namespace_engine_builds_with_required_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let engine = NamespaceEngine::builder(store)
            .namespace_id(namespace_id.clone())
            .writer_id("writer-a")
            .build()
            .expect("engine builds");

        assert_eq!(engine.namespace_id(), &namespace_id);
        assert_eq!(engine.writer_id(), "writer-a");
        assert!(!engine.writer_version().is_empty());
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
            .namespace_id(NamespaceId::parse("demo").expect("valid namespace id"))
            .build()
            .expect_err("missing writer");
        assert_eq!(err, NamespaceEngineBuildError::MissingWriter);
    }
}
