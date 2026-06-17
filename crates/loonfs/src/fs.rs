//! The embedded runtime handle: [`Fs`], its builder, and the public
//! operation surface, each method a thin, cache-aware delegation to
//! `loonfs-core`.

use crate::cache::{
    BasisCache, CommitEngineCache, MetadataReadSource, RuntimeCacheStatsInner, RuntimeControlCache,
};
use crate::config::{default_writer_version, validate_config};
use crate::publish::{NamespaceMutationCandidate, PathMutationIntent};
use crate::time::current_time_ms;
use crate::trace::{TraceMode, TraceStoreKind};
use crate::DEFAULT_LEASE_DURATION_MS;
use crate::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry,
    BeginDirectPutUploadTargetResponse, BeginUploadRequest, BeginUploadResponse, ChangeSeq,
    ChangesResponse, CommitId, CommitOp, CommitPrecondition, CommitRequest, CommitResponse,
    CompleteUploadRequest, CompleteUploadResponse, ContentRef, CopyOptions, CoreError,
    CreateCheckpointResponse, CreateDirOptions, CreateNamespaceOptions, DeleteNamespaceOptions,
    DeleteNamespaceResponse, DeleteOptions, ErrorCode, FsConfig, InodeId,
    ListFileRevisionsResponse, ListPathEntriesResponse, MaintenanceTickOptions,
    MaintenanceTickOutcome, MaintenanceTickResult, MoveOptions, MutationResult, NamespaceId,
    NamespaceStatus, NamespaceSummary, ObjectStore, ObjectStoreMetricsRecorder, PutFileOptions,
    RenameMode, RestoreRevisionOptions, RevisionNo, RuntimeCacheConfig, RuntimeCacheStats,
    UploadContentResponse,
};
use crate::{Result, RuntimeError, SharedObjectStore};
use loonfs_api::{
    AbsolutePath, CapabilityDocument, FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE,
    FEATURE_NAMESPACES_FORK, FEATURE_NAMESPACES_LIST, FEATURE_UPLOADS_DIRECT_PUT, PROFILE_ADMIN_V0,
    PROFILE_CORE_V0, PROTOCOL_VERSION,
};
use loonfs_core::cache::{load_namespace_head_summary, MetadataTableCache};
use loonfs_core::{MutationContext, NamespaceEngine};
use loonfs_objectstore::metrics::InstrumentedObjectStore;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// Embedded filesystem runtime.
///
/// `Fs` is cheap to clone. Clones share caches and the underlying object store.
#[derive(Clone)]
pub struct Fs {
    pub(crate) inner: Arc<FsInner>,
}

pub(crate) struct FsInner {
    pub(crate) store: SharedObjectStore,
    pub(crate) config: FsConfig,
    pub(crate) basis_cache: Mutex<BasisCache>,
    pub(crate) commit_engines: Mutex<CommitEngineCache>,
    pub(crate) control_cache: Mutex<RuntimeControlCache>,
    pub(crate) metadata_table_cache: Arc<MetadataTableCache>,
    pub(crate) cache_stats: RuntimeCacheStatsInner,
}

/// Lock accessors for the runtime caches.
///
/// Poisoning is propagated as a panic: a poisoned cache means another thread
/// panicked mid-update, and serving from it could violate the consistency the
/// caches promise.
impl FsInner {
    pub(crate) fn basis_cache(&self) -> MutexGuard<'_, BasisCache> {
        self.basis_cache.lock().expect("basis cache lock poisoned")
    }

    pub(crate) fn commit_engines(&self) -> MutexGuard<'_, CommitEngineCache> {
        self.commit_engines
            .lock()
            .expect("commit engine cache lock poisoned")
    }

    pub(crate) fn control_cache(&self) -> MutexGuard<'_, RuntimeControlCache> {
        self.control_cache
            .lock()
            .expect("control cache lock poisoned")
    }
}

/// Builder for [`Fs`].
pub struct FsBuilder {
    store: SharedObjectStore,
    writer_id: Option<String>,
    writer_version: String,
    lease_duration_ms: u64,
    runtime_cache: RuntimeCacheConfig,
    trace_mode: TraceMode,
    trace_store_kind: TraceStoreKind,
    metrics_recorder: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
}

impl FsBuilder {
    /// Starts an embedded runtime builder.
    pub fn new(store: SharedObjectStore) -> Self {
        Self {
            store,
            writer_id: None,
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
            runtime_cache: RuntimeCacheConfig::default(),
            trace_mode: TraceMode::Embedded,
            trace_store_kind: TraceStoreKind::Unknown,
            metrics_recorder: None,
        }
    }

    /// Sets the writer id used by namespace mutations.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Sets the writer version used in mutation context.
    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    /// Sets the lease duration for write operations.
    pub fn lease_duration_ms(mut self, lease_duration_ms: u64) -> Self {
        self.lease_duration_ms = lease_duration_ms;
        self
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.runtime_cache = runtime_cache;
        self
    }

    /// Sets the tracing mode label.
    pub fn trace_mode(mut self, trace_mode: TraceMode) -> Self {
        self.trace_mode = trace_mode;
        self
    }

    /// Sets the object-store kind label used by tracing and metrics.
    pub fn trace_store_kind(mut self, trace_store_kind: TraceStoreKind) -> Self {
        self.trace_store_kind = trace_store_kind;
        self
    }

    /// Install object-store metrics collection for this runtime.
    ///
    /// The runtime wraps the provided object store before opening `Fs`; callers do not need to
    /// manually construct an instrumented store.
    pub fn with_metrics_recorder(mut self, recorder: Arc<dyn ObjectStoreMetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    /// Opens the runtime.
    pub fn build(self) -> Result<Fs> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        let trace_store_kind = self.trace_store_kind;
        let store = match self.metrics_recorder {
            Some(recorder) => Arc::new(
                InstrumentedObjectStore::new(self.store, recorder)
                    .with_store_kind(trace_store_kind.as_str()),
            ) as SharedObjectStore,
            None => self.store,
        };
        Fs::open(
            store,
            FsConfig {
                writer_id,
                writer_version: self.writer_version,
                lease_duration_ms: self.lease_duration_ms,
                runtime_cache: self.runtime_cache,
                trace_mode: self.trace_mode,
                trace_store_kind,
            },
        )
    }
}

impl Fs {
    /// Opens an embedded runtime from an object store and config.
    pub fn open(store: SharedObjectStore, config: FsConfig) -> Result<Self> {
        validate_config(&config)?;
        let metadata_table_cache = Arc::new(MetadataTableCache::new(
            config.runtime_cache.metadata_table_cache.clone(),
        ));
        Ok(Self {
            inner: Arc::new(FsInner {
                store,
                config,
                basis_cache: Mutex::new(BasisCache::default()),
                commit_engines: Mutex::new(CommitEngineCache::default()),
                control_cache: Mutex::new(RuntimeControlCache::default()),
                metadata_table_cache,
                cache_stats: RuntimeCacheStatsInner::default(),
            }),
        })
    }

    /// Starts a runtime builder.
    pub fn builder(store: SharedObjectStore) -> FsBuilder {
        FsBuilder::new(store)
    }

    /// Returns this runtime's config.
    pub fn config(&self) -> &FsConfig {
        &self.inner.config
    }

    fn record_trace_context(&self, span: &tracing::Span) {
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
    }

    /// Creates a namespace, bootstrapping its durable state.
    ///
    /// With `options.allow_existing`, an already-existing namespace is
    /// treated as success.
    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(namespace_id)
            .bootstrap_namespace(loonfs_core::BootstrapOptions {
                allow_existing: options.allow_existing,
            })
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Forks `source` into `target` at the source's current head.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    pub async fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(source)
            .fork_namespace(target, loonfs_core::ForkOptions::default())
            .await
            .map_err(RuntimeError::from);
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace_cache(source);
        }
        if result.is_ok() {
            self.invalidate_namespace_cache(target);
        }
        result
    }

    /// Deletes a namespace: a fenced, terminal head transition (format
    /// spec, "Tombstones and deletion"). Commits acknowledged before the
    /// swap stay committed; reads, writes, forks, and re-creation of the id
    /// fail with `namespace_deleted` afterward. Deletion does not reclaim
    /// storage; reclamation is future maintenance work.
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        // Serialize with this process's publishers so the delete takes its
        // turn behind any in-flight publication for the namespace.
        let engine = self.commit_engine(namespace_id);
        let result = {
            let _engine = engine.lock().await;
            self.namespace_engine(namespace_id)
                .delete_namespace(options)
                .await
                .map_err(RuntimeError::from)
        };
        self.invalidate_namespace_cache(namespace_id);
        result
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    ///
    /// The embedded runtime implements every v0 plane, so the answer is a
    /// constant. Callers should still gate on the document rather than on
    /// the backend kind, so the same logic works against remote deployments
    /// that implement less.
    pub fn capabilities(&self) -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            profiles: vec![PROFILE_CORE_V0.to_owned(), PROFILE_ADMIN_V0.to_owned()],
            features: BTreeMap::from([
                (FEATURE_NAMESPACES_LIST.to_owned(), true),
                (FEATURE_NAMESPACES_CREATE.to_owned(), true),
                (FEATURE_NAMESPACES_FORK.to_owned(), true),
                (FEATURE_NAMESPACES_DELETE.to_owned(), true),
                (FEATURE_UPLOADS_DIRECT_PUT.to_owned(), false),
            ]),
            limits: BTreeMap::new(),
        }
    }

    /// Lists complete namespaces visible in the object store.
    pub async fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>> {
        Ok(loonfs_core::list_namespaces(self.store()).await?)
    }

    /// Summarizes a namespace's current head: manifest, latest checkpoint,
    /// WAL tail, and retention floor.
    pub async fn namespace_status(&self, namespace_id: &NamespaceId) -> Result<NamespaceStatus> {
        let summary = load_namespace_head_summary(self.store(), namespace_id).await?;
        Ok(NamespaceStatus {
            namespace_id: summary.namespace_id,
            head_seq: summary.head_seq,
            current_manifest_id: summary.current_manifest_id,
            latest_checkpoint_id: summary.latest_checkpoint_id,
            wal_tail_segments: summary.wal_tail_segments,
            retention_floor_seq: summary.retention_floor_seq,
        })
    }

    /// Runs one bounded maintenance step against a namespace.
    ///
    /// Publishes a checkpoint once the visible WAL tail reaches
    /// `options.max_wal_tail_segments`. Losing the head race or being
    /// superseded by another checkpoint is reported as an outcome, not an
    /// error.
    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn maintenance_tick_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> Result<MaintenanceTickResult> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        if options.max_wal_tail_segments == 0 {
            return Err(RuntimeError::Config(
                "max_wal_tail_segments must be greater than zero".to_owned(),
            ));
        }

        let status_before = self.namespace_status(namespace_id).await?;
        let observed_head_seq = status_before.head_seq;
        if status_before.wal_tail_segments < options.max_wal_tail_segments {
            return Ok(MaintenanceTickResult {
                namespace_id: namespace_id.clone(),
                status_before,
                outcome: MaintenanceTickOutcome::NotNeeded,
            });
        }

        let checkpoint = match self.create_checkpoint(namespace_id).await {
            Ok(checkpoint) => checkpoint,
            Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => {
                return Ok(MaintenanceTickResult {
                    namespace_id: namespace_id.clone(),
                    status_before,
                    outcome: MaintenanceTickOutcome::CheckpointPublishRaceLost {
                        observed_head_seq,
                    },
                });
            }
            Err(error) => return Err(error),
        };

        let outcome = if checkpoint.current_manifest_id == Some(checkpoint.manifest_id) {
            MaintenanceTickOutcome::CheckpointPublished {
                checkpoint_seq: checkpoint.checkpoint_seq,
            }
        } else {
            let Some(current_manifest_id) = checkpoint.current_manifest_id else {
                return Err(RuntimeError::Core(CoreError::Store(
                    "checkpoint publication returned no current manifest id".to_owned(),
                )));
            };
            MaintenanceTickOutcome::CheckpointSuperseded {
                attempted_seq: checkpoint.checkpoint_seq,
                current_manifest_id,
            }
        };

        Ok(MaintenanceTickResult {
            namespace_id: namespace_id.clone(),
            status_before,
            outcome,
        })
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head.
    #[tracing::instrument(
        level = "info",
        name = "loon.stat",
        err,
        skip_all,
        fields(
            operation = "stat",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            cache_path = tracing::field::Empty,
        )
    )]
    pub async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let head = self.head_for_metadata_read(namespace_id).await?;
        let engine = self.namespace_engine(namespace_id);
        if head.state.current_manifest_id.is_some() {
            let entry = engine
                .resolve_path(
                    absolute_path,
                    loonfs_core::ReadOptions::materialized_tables_at_head(
                        head.state.clone(),
                        Some(Arc::clone(&self.inner.metadata_table_cache)),
                    ),
                )
                .await?;
            tracing::Span::current().record(
                "cache_path",
                crate::trace::CachePath::MaterializedTables.as_str(),
            );
            self.inner
                .cache_stats
                .record_metadata_read_source(MetadataReadSource::MaterializedTables);
            return Ok(entry);
        }

        let basis = self.basis_for_read_at_head(namespace_id, &head).await?;
        let entry = engine
            .resolve_path(
                absolute_path,
                loonfs_core::ReadOptions::verified_basis(Arc::clone(&basis)),
            )
            .await?;
        self.inner
            .cache_stats
            .record_metadata_read_source(MetadataReadSource::FullBasisFallback);
        Ok(entry)
    }

    /// Lists the children of a directory path.
    ///
    /// Entries-only convenience over [`Self::list_path_entries`].
    pub async fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>> {
        Ok(self
            .list_path_entries(namespace_id, absolute_path)
            .await?
            .entries)
    }

    /// Lists a directory together with the head the listing was read from.
    ///
    /// The envelope and every entry come from one consistent head, so an
    /// empty directory still reports which state answered the question.
    pub async fn list_path_entries(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListPathEntriesResponse> {
        let listed_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        let head = self.head_for_metadata_read(namespace_id).await?;
        let engine = self.namespace_engine(namespace_id);
        let head_seq = head.state.seq;
        let entries = if head.state.current_manifest_id.is_some() {
            let entries = engine
                .list_path(
                    absolute_path,
                    loonfs_core::ReadOptions::materialized_tables_at_head(
                        head.state.clone(),
                        Some(Arc::clone(&self.inner.metadata_table_cache)),
                    ),
                )
                .await?;
            self.inner
                .cache_stats
                .record_metadata_read_source(MetadataReadSource::MaterializedTables);
            entries
        } else {
            let basis = self.basis_for_read_at_head(namespace_id, &head).await?;
            let entries = engine
                .list_path(
                    absolute_path,
                    loonfs_core::ReadOptions::verified_basis(Arc::clone(&basis)),
                )
                .await?;
            self.inner
                .cache_stats
                .record_metadata_read_source(MetadataReadSource::FullBasisFallback);
            entries
        };
        Ok(ListPathEntriesResponse {
            namespace_id: namespace_id.clone(),
            absolute_path: listed_path.as_str().to_owned(),
            head_seq,
            entries,
        })
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub async fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        let basis = self.basis_for_read(namespace_id).await?;
        Ok(self
            .namespace_engine(namespace_id)
            .read_file(
                absolute_path,
                loonfs_core::ReadOptions::verified_basis(Arc::clone(&basis)),
            )
            .await?)
    }

    /// Lists the revision history of a file path.
    pub async fn list_file_revisions(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListFileRevisionsResponse> {
        let basis = self.basis_for_read(namespace_id).await?;
        Ok(self
            .namespace_engine(namespace_id)
            .list_file_revisions(
                absolute_path,
                loonfs_core::ReadOptions::verified_basis(Arc::clone(&basis)),
            )
            .await?)
    }

    /// Lists a file's revision history by inode id, independent of its
    /// current path.
    pub async fn list_file_revisions_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<ListFileRevisionsResponse> {
        let basis = self.basis_for_read(namespace_id).await?;
        Ok(self
            .namespace_engine(namespace_id)
            .list_file_revisions_for_inode(
                inode_id,
                loonfs_core::ReadOptions::verified_basis(Arc::clone(&basis)),
            )
            .await?)
    }

    /// Reads the content of one historical file revision by path.
    pub async fn read_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        let basis = self.basis_for_read(namespace_id).await?;
        Ok(self
            .namespace_engine(namespace_id)
            .read_file_revision(
                absolute_path,
                revision_no,
                loonfs_core::ReadOptions::verified_basis(Arc::clone(&basis)),
            )
            .await?)
    }

    /// Reads the content of one historical file revision by inode id.
    pub async fn read_file_revision_bytes_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let basis = self.basis_for_read(namespace_id).await?;
        Ok(self
            .namespace_engine(namespace_id)
            .read_file_revision_for_inode(
                inode_id,
                revision_no,
                loonfs_core::ReadOptions::verified_basis(Arc::clone(&basis)),
            )
            .await?)
    }

    /// Writes file bytes to a path.
    ///
    /// The bytes become durable content first; metadata referencing them is
    /// published only afterward. `options.behavior` selects create-only or
    /// replace semantics.
    #[tracing::instrument(
        level = "info",
        name = "loon.put",
        err,
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
        validate_runtime_mutation_path(absolute_path)?;
        let store = self.store();
        let stored = loonfs_core::content::store_bytes_as_content(&store, namespace_id, bytes)
            .await
            .map_err(RuntimeError::from);
        let content_ref = stored?.content_ref;
        let admitted_content_ref = content_ref.clone();
        self.publish_admitted_path_intent(
            namespace_id,
            PathMutationIntent::PutFile {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: absolute_path.to_owned(),
                content_ref,
                behavior: options.behavior,
            },
            vec![admitted_content_ref],
        )
        .await
    }

    /// Publishes a file revision that points at an already-durable content
    /// ref.
    ///
    /// Use this when content was staged separately, for example through the
    /// upload protocol.
    #[tracing::instrument(
        level = "info",
        name = "loon.put",
        err,
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
            ),
        );
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::PutFile {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: absolute_path.to_owned(),
                content_ref,
                behavior: options.behavior,
            },
        )
        .await
    }

    /// Creates a directory at an absolute path.
    pub async fn create_dir(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirOptions,
    ) -> Result<MutationResult> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::CreateDir {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: absolute_path.to_owned(),
            },
        )
        .await
    }

    /// Deletes a file or directory path.
    ///
    /// Deletion is tombstone-first: the commit hides the path (or, with
    /// `options.recursive`, the subtree) without erasing history. Physical
    /// reclamation is background garbage collection.
    pub async fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<MutationResult> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::DeletePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: absolute_path.to_owned(),
                recursive: options.recursive,
            },
        )
        .await
    }

    /// Moves a path within the same namespace, never replacing an existing
    /// destination.
    pub async fn move_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<MutationResult> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::MovePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                from_path: from_path.to_owned(),
                to_path: to_path.to_owned(),
                mode: RenameMode::NoReplace,
            },
        )
        .await
    }

    /// Copies a file to a new path in the same namespace. The new file
    /// reuses the source revision's content reference: no bytes are copied.
    pub async fn copy_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> Result<MutationResult> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::CopyFilePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                from_path: from_path.to_owned(),
                to_path: to_path.to_owned(),
            },
        )
        .await
    }

    /// Restores a prior file revision by appending a new current revision.
    pub async fn restore_file_revision(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<MutationResult> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::RestoreRevision {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: absolute_path.to_owned(),
                source_revision_no,
            },
        )
        .await
    }

    /// Restores a prior revision of an inode, guarded by a base-revision
    /// precondition.
    ///
    /// The commit appends a new current revision from `source_revision_no`
    /// and fails if the inode's current revision is no longer
    /// `base_revision_no`.
    pub async fn restore_file_revision_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<CommitResponse> {
        let commit_id = options.commit_id.unwrap_or_else(CommitId::generate);
        let request = CommitRequest {
            commit_id,
            preconditions: vec![CommitPrecondition::InodeRevisionIs {
                inode_id,
                revision_no: base_revision_no,
            }],
            ops: vec![CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            }],
            message: None,
            annotations: None,
        };
        self.commit_operations(namespace_id, request).await
    }

    /// Starts a durable upload session for a namespace.
    pub async fn begin_upload(&self, namespace_id: &NamespaceId) -> Result<BeginUploadResponse> {
        self.begin_upload_with_request(namespace_id, BeginUploadRequest::default())
            .await
    }

    /// Starts a durable upload session with explicit transport options.
    pub async fn begin_upload_with_request(
        &self,
        namespace_id: &NamespaceId,
        request: BeginUploadRequest,
    ) -> Result<BeginUploadResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .begin_upload_with_request(request)
            .await?)
    }

    /// Starts a direct_put upload session and returns the internal target for server-side signing.
    pub async fn begin_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .begin_direct_put_upload_target(content_ref)
            .await?)
    }

    /// Uploads whole-file content into an upload session.
    pub async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .upload_content(upload_id, bytes)
            .await?)
    }

    /// Completes an upload session when the expected content ref matches.
    pub async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .complete_upload(upload_id, request)
            .await?)
    }

    /// Submits one explicit semantic commit request.
    ///
    /// This is the lower-level surface for clients that need their own commit
    /// ids, preconditions, and operation lists.
    pub async fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.publish_namespace_mutations_batch(
            namespace_id,
            vec![NamespaceMutationCandidate::Commit(request)],
        )
        .await
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            Err(RuntimeError::Core(CoreError::Store(
                "empty commit batch".to_owned(),
            )))
        })
    }

    /// Submits explicit semantic commit requests as one publication attempt,
    /// returning one result per request in order.
    pub async fn commit_operations_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<Result<CommitResponse>> {
        self.publish_namespace_mutations_batch(
            namespace_id,
            requests
                .into_iter()
                .map(NamespaceMutationCandidate::Commit)
                .collect(),
        )
        .await
    }

    async fn publish_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
    ) -> Result<MutationResult> {
        let mut results = self
            .publish_namespace_mutations_batch(
                namespace_id,
                vec![NamespaceMutationCandidate::Path(intent)],
            )
            .await;
        let response = results.pop().unwrap_or_else(|| {
            Err(RuntimeError::Core(CoreError::Store(
                "empty path mutation batch".to_owned(),
            )))
        })?;
        Ok(MutationResult {
            namespace_id: response.namespace_id,
            committed_seq: response.committed_seq,
        })
    }

    async fn publish_admitted_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
        admitted_content_refs: Vec<ContentRef>,
    ) -> Result<MutationResult> {
        let mut results = self
            .publish_namespace_mutations_batch(
                namespace_id,
                vec![NamespaceMutationCandidate::AdmittedPath {
                    intent,
                    admitted_content_refs,
                }],
            )
            .await;
        let response = results.pop().unwrap_or_else(|| {
            Err(RuntimeError::Core(CoreError::Store(
                "empty path mutation batch".to_owned(),
            )))
        })?;
        Ok(MutationResult {
            namespace_id: response.namespace_id,
            committed_seq: response.committed_seq,
        })
    }

    /// Publishes already-classified namespace mutation candidates as one
    /// batch.
    ///
    /// Server code uses this to push path intents and explicit commits
    /// through one namespace publisher; results match candidates in order.
    pub async fn publish_namespace_mutations_batch(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
        let store = self.store();
        if self.commit_engine_cache_enabled() {
            let engine = self.commit_engine(namespace_id);
            let publish = {
                let context = self.mutation_context();
                let mut engine = engine.lock().await;
                engine.publish_batch(&store, candidates, &context).await
            };
            {
                let _span = tracing::info_span!(
                    "publisher.batch_update_cache",
                    phase = "batch_update_cache",
                    mode = self.inner.config.trace_mode.as_str(),
                    store_kind = self.inner.config.trace_store_kind.as_str(),
                    batch_size
                )
                .entered();
                self.inner.cache_stats.record_publish_result(&publish);
                if let Some(basis) = publish
                    .verified_basis_cache_update
                    .verified_basis_to_cache()
                {
                    self.cache_basis(basis);
                } else if publish.verified_basis_cache_update.is_invalidated() {
                    self.invalidate_namespace_cache(namespace_id);
                }
            }
            return publish
                .results
                .into_iter()
                .map(|result| result.map_err(RuntimeError::Core))
                .collect();
        }

        let results: Vec<_> = self
            .namespace_engine_with_store(namespace_id, store)
            .publish_namespace_mutations_batch(candidates)
            .await
            .into_iter()
            .map(|result| result.map_err(RuntimeError::Core))
            .collect();
        {
            let _span = tracing::info_span!(
                "publisher.batch_update_cache",
                phase = "batch_update_cache",
                mode = self.inner.config.trace_mode.as_str(),
                store_kind = self.inner.config.trace_store_kind.as_str(),
                batch_size
            )
            .entered();
            self.invalidate_namespace_cache_after_batch(namespace_id, &results);
        }
        results
    }

    /// Snapshots the runtime cache counters.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.inner
            .cache_stats
            .snapshot(self.inner.metadata_table_cache.stats())
    }

    /// Reads the ordered change feed after the `after_seq` cursor.
    pub async fn list_changes_after(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> Result<ChangesResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .list_changes_after(after_seq)
            .await?)
    }

    /// Creates or reuses a checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest version for retention and provenance. If
    /// the current head has no manifest yet, one is published first for the
    /// current durable namespace state; this is not a request to compact
    /// metadata.
    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CreateCheckpointResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let result = self
            .namespace_engine(namespace_id)
            .create_checkpoint()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    /// Advances the namespace retention floor when a verified checkpoint
    /// makes it safe.
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let result = self
            .namespace_engine(namespace_id)
            .advance_retention_floor()
            .await
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub(crate) fn store(&self) -> &(dyn ObjectStore + Send + Sync) {
        self.inner.store.as_ref()
    }

    pub(crate) fn namespace_engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceEngine<SharedObjectStore> {
        self.namespace_engine_with_store(namespace_id, self.inner.store.clone())
    }

    pub(crate) fn namespace_engine_with_store<S: ObjectStore>(
        &self,
        namespace_id: &NamespaceId,
        store: S,
    ) -> NamespaceEngine<S> {
        NamespaceEngine::builder(store)
            .namespace(namespace_id.clone())
            .writer(self.inner.config.writer_id.clone())
            .writer_version(self.inner.config.writer_version.clone())
            .lease_duration_ms(self.inner.config.lease_duration_ms)
            .build()
            .expect("validated runtime config should build namespace engine")
    }

    pub(crate) fn mutation_context(&self) -> MutationContext {
        MutationContext {
            writer_id: self.inner.config.writer_id.clone(),
            writer_version: self.inner.config.writer_version.clone(),
            now_ms: current_time_ms(),
            lease_duration_ms: self.inner.config.lease_duration_ms,
        }
    }
}

pub(crate) fn should_invalidate_after_result<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => true,
        Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => true,
        _ => false,
    }
}

fn validate_runtime_mutation_path(absolute_path: &str) -> Result<()> {
    let path = AbsolutePath::parse(absolute_path).map_err(|error| {
        RuntimeError::Core(CoreError::InvalidPath(
            error.invalid_path_input().to_owned(),
        ))
    })?;
    if path.is_root() {
        return Err(RuntimeError::Core(CoreError::RootMutationForbidden));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        ChangeSeq, CommitId, CommitOp, CommitPrecondition, CommitRequest, DisplayName, InodeId,
        NameKey, NamePolicy, RenameMode, RevisionNo,
    };

    #[test]
    fn explicit_commit_facade_exports_constructor_types() {
        let display_name = DisplayName::parse("Report.txt").expect("valid display name");
        let name_key = NameKey::for_display_name(NamePolicy::default(), &display_name);
        let precondition =
            CommitPrecondition::binding_is(InodeId(1), name_key, InodeId(2), ChangeSeq(3), 4);

        let request = CommitRequest {
            commit_id: CommitId::generate(),
            preconditions: vec![precondition],
            ops: vec![
                CommitOp::RestoreRevision {
                    inode_id: InodeId(2),
                    source_revision_no: RevisionNo(1),
                    base_revision_no: RevisionNo(2),
                },
                CommitOp::Rename {
                    inode_id: InodeId(2),
                    new_parent_inode: InodeId(1),
                    new_display_name: "report.txt".to_owned(),
                    mode: RenameMode::NoReplace,
                },
            ],
            message: None,
            annotations: None,
        };

        assert_eq!(request.preconditions.len(), 1);
        assert_eq!(request.ops.len(), 2);
    }
}
