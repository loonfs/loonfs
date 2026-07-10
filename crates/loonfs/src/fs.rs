//! The shared runtime core behind the purpose-specific handles: caches,
//! writer session identity, and the operation surface, each method a thin,
//! cache-aware delegation to `loonfs-core`.

use crate::background::BackgroundWork;
use crate::cache::{RuntimeCacheStatsInner, RuntimeControlCache};
use crate::commit_window::{
    commit_window_delay, pad_missing_results, CommitWindows, WindowEntry, WindowRole,
};
use crate::config::{validate_config, FsConfig};
use crate::content_tokens::ContentAdmission;
use crate::publish::{NamespaceMutationCandidate, PathMutationIntent};
use crate::time::current_time_ms;
use crate::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry,
    BeginDirectPutUploadTargetResponse, BeginUploadRequest, BeginUploadResponse, ChangeSeq,
    ChangesResponse, CommitId, CommitOp, CommitPrecondition, CommitRequest, CommitResponse,
    CompleteUploadRequest, CompleteUploadResponse, ContentRef, CopyOptions, CoreError,
    CreateCheckpointResponse, CreateDirectoryOptions, CreateNamespaceOptions,
    DeleteNamespaceOptions, DeleteNamespaceResponse, DeleteOptions, ErrorCode, InodeId,
    ListChangesOptions, ListFileRevisionsResponse, ListPathEntriesResponse, MaintenanceTickOptions,
    MaintenanceTickOutcome, MaintenanceTickResult, MoveOptions, MutationResult, NamespaceId,
    NamespaceStatusResponse, NamespaceSummary, ObjectStore, PutFileOptions, RestoreRevisionOptions,
    RevisionNo, RuntimeCacheStats, UploadContentResponse,
};
use crate::{Result, RuntimeError, SharedObjectStore};
use loonfs_api::{
    encode_directory_cursor, encode_file_revisions_cursor, generated_id, AbsolutePath,
    CapabilityDocument, DirectoryPageCursor, EffectiveLimit, FileRevision, FileRevisionsPageCursor,
    Page, PageRequest, PaginationPolicy, UploadId, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_UPLOADS_DIRECT_PUT,
    PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROTOCOL_VERSION,
};
use loonfs_core::cache::{
    load_namespace_head_summary, MetadataTableCache, WalTailProjectionCache,
    WalTailProjectionCacheConfig,
};
use loonfs_core::{MutationContext, NamespaceEngine};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// Shared runtime core. [`FsWriter`](crate::FsWriter),
/// [`FsReader`](crate::FsReader), and [`FsAdmin`](crate::FsAdmin) each wrap
/// one; handles derived from the same core share its caches and store.
///
/// `FsCore` is cheap to clone.
#[derive(Clone)]
pub(crate) struct FsCore {
    pub(crate) inner: Arc<FsInner>,
}

pub(crate) struct FsInner {
    pub(crate) store: SharedObjectStore,
    pub(crate) config: FsConfig,
    pub(crate) writer_session_id: String,
    pub(crate) control_cache: Mutex<RuntimeControlCache>,
    pub(crate) metadata_table_cache: Arc<MetadataTableCache>,
    pub(crate) wal_tail_projection_cache: Arc<WalTailProjectionCache>,
    pub(crate) cache_stats: RuntimeCacheStatsInner,
    pub(crate) background: BackgroundWork,
    pub(crate) commit_windows: CommitWindows,
}

/// Lock accessors for the runtime caches.
///
/// Poisoning is propagated as a panic: a poisoned cache means another thread
/// panicked mid-update, and serving from it could violate the consistency the
/// caches promise.
impl FsInner {
    pub(crate) fn control_cache(&self) -> MutexGuard<'_, RuntimeControlCache> {
        self.control_cache
            .lock()
            .expect("control cache lock poisoned")
    }
}

impl FsCore {
    pub(crate) fn open_with_background(
        store: SharedObjectStore,
        config: FsConfig,
        background: BackgroundWork,
    ) -> Result<Self> {
        validate_config(&config)?;
        let metadata_table_cache = Arc::new(MetadataTableCache::new(
            config.runtime_cache.metadata_table_cache.clone(),
        ));
        let wal_tail_projection_cache =
            Arc::new(WalTailProjectionCache::new(WalTailProjectionCacheConfig {
                max_entries: config.runtime_cache.max_cached_namespaces,
                max_rows: config.runtime_cache.max_cached_wal_tail_projection_rows,
                max_decoded_bytes: config
                    .runtime_cache
                    .max_cached_wal_tail_projection_decoded_bytes,
            }));
        Ok(Self {
            inner: Arc::new(FsInner {
                store,
                config,
                writer_session_id: generated_id("wrs"),
                control_cache: Mutex::new(RuntimeControlCache::default()),
                metadata_table_cache,
                wal_tail_projection_cache,
                cache_stats: RuntimeCacheStatsInner::default(),
                background,
                commit_windows: CommitWindows::default(),
            }),
        })
    }

    /// Returns this runtime's config.
    pub(crate) fn config(&self) -> &FsConfig {
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
    pub(crate) async fn create_namespace(
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
    pub(crate) async fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(source)
            .fork_namespace(target)
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
    pub(crate) async fn delete_namespace(
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
        self.invalidate_namespace_cache_for_delete(namespace_id);
        result
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    ///
    /// The embedded runtime implements every v0 plane, so the answer is a
    /// constant. Callers should still gate on the document rather than on
    /// the backend kind, so the same logic works against remote deployments
    /// that implement less.
    pub(crate) fn capabilities(&self) -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            profiles: vec![PROFILE_CORE_V0.to_owned(), PROFILE_ADMIN_V0.to_owned()],
            features: BTreeMap::from([
                (FEATURE_NAMESPACES_CREATE.to_owned(), true),
                (FEATURE_NAMESPACES_FORK.to_owned(), true),
                (FEATURE_NAMESPACES_DELETE.to_owned(), true),
                (FEATURE_UPLOADS_DIRECT_PUT.to_owned(), false),
            ]),
            limits: PaginationPolicy::default().capability_limits(),
        }
    }

    /// Summarizes a namespace's current head: manifest, latest checkpoint,
    /// WAL tail, and retention floor.
    pub(crate) async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse> {
        let summary = load_namespace_head_summary(self.store(), namespace_id).await?;
        Ok(NamespaceStatusResponse {
            namespace_id: summary.namespace_id,
            head_seq: summary.head_seq,
            current_manifest_id: summary.current_manifest_id,
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
    pub(crate) async fn maintenance_tick_namespace(
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
            self.run_tick_reorganization(namespace_id).await?;
            let gc = self.run_tick_gc(namespace_id, options.gc.as_ref()).await?;
            return Ok(MaintenanceTickResult {
                namespace_id: namespace_id.clone(),
                status_before,
                outcome: MaintenanceTickOutcome::NotNeeded,
                gc,
            });
        }

        let checkpoint = match self.create_checkpoint(namespace_id).await {
            Ok(checkpoint) => checkpoint,
            Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => {
                self.run_tick_reorganization(namespace_id).await?;
                let gc = self.run_tick_gc(namespace_id, options.gc.as_ref()).await?;
                return Ok(MaintenanceTickResult {
                    namespace_id: namespace_id.clone(),
                    status_before,
                    outcome: MaintenanceTickOutcome::CheckpointPublishRaceLost {
                        observed_head_seq,
                    },
                    gc,
                });
            }
            Err(error) => return Err(error),
        };
        self.run_tick_reorganization(namespace_id).await?;

        let outcome = if checkpoint.current_manifest_id == Some(checkpoint.manifest_id) {
            MaintenanceTickOutcome::CheckpointPublished {
                checkpoint_seq: checkpoint.checkpoint_seq,
            }
        } else {
            let Some(current_manifest_id) = checkpoint.current_manifest_id else {
                return Err(RuntimeError::Core(CoreError::Internal(
                    "checkpoint publication returned no current manifest id".to_owned(),
                )));
            };
            MaintenanceTickOutcome::CheckpointSuperseded {
                attempted_seq: checkpoint.checkpoint_seq,
                current_manifest_id,
            }
        };

        let gc = self.run_tick_gc(namespace_id, options.gc.as_ref()).await?;
        Ok(MaintenanceTickResult {
            namespace_id: namespace_id.clone(),
            status_before,
            outcome,
            gc,
        })
    }

    /// One bounded reorganization unit per tick: folds one family group of
    /// L0 delta rows into the base when enough L0 runs have piled up (see
    /// `loonfs-core`'s `reorganize_metadata`). Explicit ticks stay bounded at
    /// one unit per call; the returned outcome lets writer-scheduled
    /// background work keep folding until nothing is left.
    async fn run_tick_reorganization(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<loonfs_core::MetadataReorganizeOutcome> {
        let report = self
            .namespace_engine(namespace_id)
            .reorganize_metadata()
            .await
            .map_err(RuntimeError::Core)?;
        match &report.outcome {
            loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. } => {}
            loonfs_core::MetadataReorganizeOutcome::UnitPublished {
                families,
                folded_l0_rows,
                ..
            } => {
                self.invalidate_namespace_cache(namespace_id);
                tracing::info!(
                    families = ?families,
                    folded_l0_rows,
                    "metadata reorganization unit published"
                );
            }
            loonfs_core::MetadataReorganizeOutcome::Superseded => {
                tracing::info!("metadata reorganization unit superseded; will retry");
            }
        }
        Ok(report.outcome)
    }

    /// Folds reorganization units until the trigger reports nothing left to
    /// do. Only writer-scheduled background ticks drain like this — an
    /// explicit maintenance tick keeps its one-unit-per-call cost bound —
    /// so fold debt created by a burst of writes is settled by the burst's
    /// own background tick instead of waiting for future threshold
    /// crossings that may never come.
    async fn drain_reorganization_backlog(&self, namespace_id: &NamespaceId) -> Result<()> {
        // Four family groups exist today; the bound is a livelock guard,
        // not a scheduling policy.
        const MAX_UNITS_PER_DRAIN: usize = 16;
        for _ in 0..MAX_UNITS_PER_DRAIN {
            match self.run_tick_reorganization(namespace_id).await? {
                loonfs_core::MetadataReorganizeOutcome::UnitPublished { .. } => {}
                loonfs_core::MetadataReorganizeOutcome::NotNeeded { .. }
                | loonfs_core::MetadataReorganizeOutcome::Superseded => break,
            }
        }
        Ok(())
    }

    async fn run_tick_gc(
        &self,
        namespace_id: &NamespaceId,
        config: Option<&crate::GcConfig>,
    ) -> Result<Option<crate::GcReport>> {
        let Some(config) = config else {
            return Ok(None);
        };
        Ok(Some(self.gc_namespace(namespace_id, config).await?))
    }

    /// Runs the v1 mark-and-sweep garbage collector for one namespace.
    ///
    /// Never runs implicitly: callers opt in here or through
    /// [`MaintenanceTickOptions::gc`].
    pub(crate) async fn gc_namespace(
        &self,
        namespace_id: &NamespaceId,
        config: &crate::GcConfig,
    ) -> Result<crate::GcReport> {
        let report =
            loonfs_core::gc_namespace(self.store(), namespace_id, config, &self.mutation_context())
                .await
                .map_err(RuntimeError::Core)?;
        // Sweeping can remove objects cached views still reference; drop the
        // namespace caches rather than trusting them across a collection.
        self.invalidate_namespace_read_cache(namespace_id);
        Ok(report)
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
    pub(crate) async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let entry = engine
            .resolve_path_with_runtime_context(absolute_path, &read_context)
            .await?;
        tracing::Span::current().record(
            "cache_path",
            crate::trace::CachePath::MaterializedTables.as_str(),
        );
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(entry)
    }

    /// Lists the children of a directory path.
    ///
    /// Entries-only convenience over [`Self::list_path_entries`].
    pub(crate) async fn list_path(
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
    /// empty directory still reports which state answered the question. Entries
    /// are returned in canonical name-key order, matching paged listings.
    pub(crate) async fn list_path_entries(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListPathEntriesResponse> {
        let limit = default_page_limit();
        let mut cursor = None;
        let mut entries = Vec::new();
        let mut envelope = None;
        loop {
            let (page, next_cursor) = self
                .list_path_entries_page_typed(
                    namespace_id,
                    absolute_path,
                    PageRequest { limit, cursor },
                )
                .await?;
            let envelope_ref = envelope.get_or_insert_with(|| ListPathEntriesResponse {
                namespace_id: page.namespace_id.clone(),
                absolute_path: page.absolute_path.clone(),
                head_seq: page.head_seq,
                entries: Vec::new(),
                next_cursor: None,
            });
            entries.extend(page.entries);
            cursor = next_cursor;
            if cursor.is_none() {
                envelope_ref.entries = entries;
                return Ok(envelope.expect("first page initializes response envelope"));
            }
        }
    }

    /// Lists one page of a directory together with the head the page was read from.
    pub(crate) async fn list_path_entries_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> Result<ListPathEntriesResponse> {
        let (mut response, next_cursor) = self
            .list_path_entries_page_typed(namespace_id, absolute_path, request)
            .await?;
        response.next_cursor = next_cursor
            .as_ref()
            .map(encode_directory_cursor)
            .transpose()
            .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
        Ok(response)
    }

    async fn list_path_entries_page_typed(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> Result<(ListPathEntriesResponse, Option<DirectoryPageCursor>)> {
        let listed_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| CoreError::InvalidPath(error.to_string()))?;
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let request_head_seq = request.cursor.as_ref().map(|cursor| cursor.head_seq);
        let page = engine
            .list_path_page_with_runtime_context(listed_path.as_str(), request, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        let head_seq = page
            .items
            .first()
            .map(|entry| entry.head_seq)
            .or(request_head_seq)
            .unwrap_or(read_context.head.seq);
        let next_cursor = page.next_cursor;
        let response = ListPathEntriesResponse {
            namespace_id: namespace_id.clone(),
            absolute_path: listed_path.as_str().to_owned(),
            head_seq,
            entries: page.items,
            next_cursor: None,
        };
        Ok((response, next_cursor))
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub(crate) async fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_with_runtime_context(absolute_path, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
    }

    /// Lists the revision history of a file path.
    pub(crate) async fn list_file_revisions(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let revisions = engine
            .list_file_revisions_with_runtime_context(absolute_path, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(revisions)
    }

    /// Lists one page of a file path's revision history.
    pub(crate) async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let fallback_inode_id = request.cursor.as_ref().map(|cursor| cursor.inode_id);
        let page = engine
            .list_file_revisions_page_with_runtime_context(absolute_path, request, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            fallback_inode_id,
        )?)
    }

    /// Lists a file's revision history by inode id, independent of its
    /// current path.
    pub(crate) async fn list_file_revisions_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let revisions = engine
            .list_file_revisions_for_inode_with_runtime_context(inode_id, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(revisions)
    }

    /// Lists one page of a file inode's revision history.
    pub(crate) async fn list_file_revisions_for_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<ListFileRevisionsResponse> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let page = engine
            .list_file_revisions_for_inode_page_with_runtime_context(
                inode_id,
                request,
                &read_context,
            )
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(file_revisions_page_response(
            namespace_id.clone(),
            read_context.head.seq,
            page,
            Some(inode_id),
        )?)
    }

    /// Reads the content of one historical file revision by path.
    pub(crate) async fn read_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_revision_with_runtime_context(absolute_path, revision_no, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
    }

    /// Reads the content of one historical file revision by inode id.
    pub(crate) async fn read_file_revision_bytes_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let (engine, read_context) = self.pinned_read(namespace_id).await?;
        let read = engine
            .read_file_revision_for_inode_with_runtime_context(inode_id, revision_no, &read_context)
            .await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(read)
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
    pub(crate) async fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
        // Pre-flight the mutation-path invariant before staging content so an
        // invalid or root path cannot orphan an already-written content blob;
        // core re-checks the same invariant when the intent is planned.
        loonfs_core::path::validate_path_for_mutation(absolute_path)?;
        // The bytes become durable content before the publish is submitted,
        // so the commit window never waits on a content upload and an upload
        // failure surfaces here with nothing published. A publish that fails
        // afterward leaves only a GC-covered orphan behind an unmoved head.
        let cached_content_store_id = self
            .load_namespace_catalog_cached(namespace_id)
            .await?
            .map(|catalog| catalog.content_store_id().clone());
        let stored = match cached_content_store_id {
            Some(content_store_id) => {
                loonfs_core::content::store_bytes_as_content_with_store_id(
                    &self.inner.store,
                    content_store_id,
                    bytes,
                )
                .await?
            }
            None => {
                loonfs_core::content::store_bytes_as_content(&self.inner.store, namespace_id, bytes)
                    .await?
            }
        };
        // The admission tells batch validation not to re-probe for the blob
        // whose acked write this handle just observed.
        let content_admission = ContentAdmission::for_durable_content_write(
            namespace_id.clone(),
            stored.content_ref.clone(),
            current_time_ms(),
        );
        self.publish_candidate(
            namespace_id,
            NamespaceMutationCandidate::PathWithContentAdmission {
                intent: PathMutationIntent::PutFile {
                    commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                    absolute_path: absolute_path.to_owned(),
                    content_ref: stored.content_ref,
                    behavior: options.behavior,
                },
                admissions: vec![content_admission],
            },
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
    pub(crate) async fn put_file_content_ref(
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
    pub(crate) async fn create_directory(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
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
    /// Deletion is tombstone-first: the commit hides the path without erasing
    /// history. Physical reclamation is background garbage collection.
    pub(crate) async fn delete_path(
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
                behavior: options.behavior,
            },
        )
        .await
    }

    /// Moves a path within the same namespace.
    pub(crate) async fn move_path(
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
                behavior: options.behavior,
            },
        )
        .await
    }

    /// Copies a file to a new path in the same namespace. The new file
    /// reuses the source revision's content reference: no bytes are copied.
    pub(crate) async fn copy_path(
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
    pub(crate) async fn restore_file_revision(
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
    pub(crate) async fn restore_file_revision_for_inode(
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
        };
        self.commit_operations(namespace_id, request).await
    }

    /// Starts a durable upload session for a namespace.
    pub(crate) async fn begin_upload(
        &self,
        namespace_id: &NamespaceId,
        request: BeginUploadRequest,
    ) -> Result<BeginUploadResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .begin_upload(request)
            .await?)
    }

    /// Starts a direct_put upload session and returns the internal target for server-side signing.
    pub(crate) async fn begin_direct_put_upload_target(
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
    pub(crate) async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .upload_content(upload_id, bytes)
            .await?)
    }

    /// Completes an upload session when the expected content ref matches.
    pub(crate) async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
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
    pub(crate) async fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.publish_through_commit_window(
            namespace_id,
            vec![NamespaceMutationCandidate::Commit(request)],
        )
        .await
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            Err(RuntimeError::Core(CoreError::Internal(
                "empty commit batch".to_owned(),
            )))
        })
    }

    /// Submits explicit semantic commit requests as one publication attempt,
    /// returning one result per request in order.
    pub(crate) async fn commit_operations_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<Result<CommitResponse>> {
        self.publish_through_commit_window(
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
        self.publish_candidate(namespace_id, NamespaceMutationCandidate::Path(intent))
            .await
    }

    async fn publish_candidate(
        &self,
        namespace_id: &NamespaceId,
        candidate: NamespaceMutationCandidate,
    ) -> Result<MutationResult> {
        let mut results = self
            .publish_through_commit_window(namespace_id, vec![candidate])
            .await;
        let response = results.pop().unwrap_or_else(|| {
            Err(RuntimeError::Core(CoreError::Internal(
                "empty path mutation batch".to_owned(),
            )))
        })?;
        Ok(MutationResult {
            namespace_id: response.namespace_id,
            committed_seq: response.committed_seq,
        })
    }

    /// Publishes a direct submission through the namespace's commit window
    /// (see [`crate::commit_window`]): concurrent submissions within the
    /// window flush as one batch — one WAL segment, one head CAS — and every
    /// submitter receives its own results. A zero-length window bypasses
    /// coalescing entirely. The server's batching publisher submits through
    /// [`Self::publish_namespace_mutations_batch`] instead, which stays
    /// window-free because it already coalesces.
    async fn publish_through_commit_window(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let window_ms = self.inner.config.commit_window_ms;
        if window_ms == 0 {
            return self
                .publish_namespace_mutations_batch(namespace_id, candidates)
                .await;
        }
        let candidate_count = candidates.len();
        let role = self.inner.commit_windows.enter(namespace_id, candidates);
        let result_receiver = match role {
            WindowRole::Opener {
                guard,
                result_receiver,
            } => {
                commit_window_delay(std::time::Duration::from_millis(window_ms)).await;
                let entries = guard.take_entries();
                let mut combined = Vec::new();
                let mut routes = Vec::new();
                for entry in entries {
                    let WindowEntry {
                        candidates,
                        result_sender,
                    } = entry;
                    routes.push((candidates.len(), result_sender));
                    combined.extend(candidates);
                }
                let mut results = self
                    .publish_namespace_mutations_batch(namespace_id, combined)
                    .await
                    .into_iter();
                for (expected, sender) in routes {
                    let slice: Vec<_> = results.by_ref().take(expected).collect();
                    let _ = sender.send(pad_missing_results(slice, expected));
                }
                result_receiver
            }
            WindowRole::Joiner(result_receiver) => result_receiver,
        };
        match result_receiver.await {
            Ok(results) => results,
            // The opener was cancelled before distributing results; the
            // window closed behind it, so the submission failed rather than
            // committed and the caller may retry.
            Err(_) => (0..candidate_count)
                .map(|_| {
                    Err(RuntimeError::Core(CoreError::Internal(
                        "commit window abandoned before its flush completed".to_owned(),
                    )))
                })
                .collect(),
        }
    }

    /// Publishes already-classified namespace mutation candidates as one
    /// batch.
    ///
    /// Server code uses this to push path intents and explicit commits
    /// through one namespace publisher; results match candidates in order.
    pub(crate) async fn publish_namespace_mutations_batch(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
        let store = self.store();
        if self.commit_engine_cache_enabled() {
            // Warm the immutable catalog through the control cache so a
            // recreated engine starts seeded; a load failure here surfaces
            // as the publish view's own, properly shaped error instead.
            self.load_namespace_catalog_cached(namespace_id).await.ok();
            let engine = self.commit_engine(namespace_id);
            let mut publish = {
                let context = self.mutation_context();
                let cache_config = &self.inner.config.runtime_cache;
                // Boxing erases the engine's deeply nested publish future;
                // without it, callers awaiting a put or commit (CLI, server,
                // embedding crates) exceed rustc's type-recursion depth.
                let tail_options = loonfs_core::publish::PublishTailOptions {
                    max_tail_rows: cache_config.max_cached_wal_tail_projection_rows,
                    max_tail_decoded_bytes: cache_config
                        .max_cached_wal_tail_projection_decoded_bytes,
                };
                Box::pin(async {
                    let mut engine = engine.lock().await;
                    engine
                        .publish_batch_with_tail_options(
                            &store,
                            candidates,
                            &context,
                            &tail_options,
                        )
                        .await
                })
                .await
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
                match publish.resulting_read_state.take() {
                    // A landed publish hands the caches exactly the state a
                    // rebuild would recompute; use it instead of dropping.
                    Some(state) => self.seed_namespace_read_cache(namespace_id, state),
                    None => {
                        let runtime_results = publish
                            .results
                            .iter()
                            .map(|result| result.clone().map_err(RuntimeError::Core))
                            .collect::<Vec<_>>();
                        self.invalidate_namespace_cache_after_batch(namespace_id, &runtime_results);
                    }
                }
            }
            let wal_tail_segments = publish.wal_tail_segments;
            let results = publish
                .results
                .into_iter()
                .map(|result| result.map_err(RuntimeError::Core))
                .collect();
            self.maybe_auto_tick_after_publish(namespace_id, wal_tail_segments);
            return results;
        }

        let engine = self.namespace_engine_with_store(namespace_id, store);
        // Boxed for the same type-recursion reason as the cached-engine path.
        let results: Vec<_> = Box::pin(engine.publish_namespace_mutations_batch(candidates))
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

    /// Schedules a maintenance tick after a publish that observed the WAL
    /// tail at or past the checkpoint threshold. Ticks are spawned on the
    /// handle's owning runtime — never on a hidden LoonFS runtime — so no
    /// writer (and no server batch pipeline) waits behind a checkpoint or
    /// base rebuild. The per-namespace singleflight claim dedupes concurrent
    /// publishers and is released on every outcome, including tick panics
    /// and dropped tasks. The cache-disabled diagnostic mode never reaches
    /// this, as it tracks no tail projection to observe.
    fn maybe_auto_tick_after_publish(&self, namespace_id: &NamespaceId, wal_tail_segments: u64) {
        let options = MaintenanceTickOptions::default();
        if wal_tail_segments < options.max_wal_tail_segments {
            return;
        }
        if !self.inner.background.try_claim(namespace_id) {
            return;
        }
        let claim = BackgroundTickClaim {
            fs: self.clone(),
            namespace_id: namespace_id.clone(),
        };
        self.inner.background.spawn(async move {
            let tick = claim
                .fs
                .maintenance_tick_namespace(&claim.namespace_id, options)
                .await;
            let drained = match tick {
                Ok(_) => {
                    claim
                        .fs
                        .drain_reorganization_backlog(&claim.namespace_id)
                        .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = drained {
                tracing::info!(
                    phase = "auto_maintenance_tick",
                    result = "error",
                    error = %error,
                    "post-publish maintenance tick failed"
                );
            }
        });
    }

    /// Waits until every scheduled background maintenance tick has finished.
    ///
    /// Call this to quiesce before shutdown, or in tests that assert on
    /// post-maintenance state. Panicked ticks surface as a runtime-task
    /// error.
    pub(crate) async fn wait_for_background_maintenance(&self) -> Result<()> {
        self.inner.background.drain().await
    }

    /// Rejects any further background maintenance scheduling.
    pub(crate) fn shut_down_background(&self) {
        self.inner.background.shut_down();
    }

    /// Snapshots the runtime cache counters.
    pub(crate) fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.inner.cache_stats.snapshot(
            self.inner.metadata_table_cache.stats(),
            self.inner.wal_tail_projection_cache.stats(),
        )
    }

    /// Reads the ordered change feed after the `after_seq` cursor.
    pub(crate) async fn list_changes_after(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: ListChangesOptions,
    ) -> Result<ChangesResponse> {
        let limit = match options.limit {
            Some(limit) => limit,
            None => PaginationPolicy::default()
                .resolve_limit(None)
                .map_err(|error| RuntimeError::Config(error.to_string()))?,
        };
        Ok(self
            .namespace_engine(namespace_id)
            .list_changes_after(after_seq, limit)
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
    pub(crate) async fn create_checkpoint(
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
    pub(crate) async fn advance_retention_floor(
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

    pub(crate) fn store(&self) -> &dyn ObjectStore {
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
            .namespace_id(namespace_id.clone())
            .writer_id(self.inner.config.writer_id.clone())
            .writer_session_id(self.inner.writer_session_id.clone())
            .writer_version(self.inner.config.writer_version.clone())
            .build()
            .expect("validated runtime config should build namespace engine")
    }

    pub(crate) fn mutation_context(&self) -> MutationContext {
        MutationContext {
            writer_id: self.inner.config.writer_id.clone(),
            writer_session_id: self.inner.writer_session_id.clone(),
            writer_version: self.inner.config.writer_version.clone(),
            now_ms: current_time_ms(),
        }
    }
}

/// Holds a background singleflight claim across a spawned tick. Dropping —
/// on completion, panic, or a task discarded with its runtime — releases the
/// namespace for the next scheduling decision.
struct BackgroundTickClaim {
    fs: FsCore,
    namespace_id: NamespaceId,
}

impl Drop for BackgroundTickClaim {
    fn drop(&mut self) {
        self.fs.inner.background.release(&self.namespace_id);
    }
}

pub(crate) fn should_invalidate_after_result<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => true,
        Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => true,
        _ => false,
    }
}

fn default_page_limit() -> EffectiveLimit {
    PaginationPolicy::default()
        .resolve_limit(None)
        .expect("default pagination policy must resolve its default limit")
}

fn file_revisions_page_response(
    namespace_id: NamespaceId,
    head_seq: ChangeSeq,
    page: Page<FileRevision, FileRevisionsPageCursor>,
    fallback_inode_id: Option<InodeId>,
) -> std::result::Result<ListFileRevisionsResponse, CoreError> {
    let inode_id = page
        .items
        .first()
        .map(|revision| revision.inode_id)
        .or_else(|| page.next_cursor.as_ref().map(|cursor| cursor.inode_id))
        .or(fallback_inode_id)
        .ok_or_else(|| {
            CoreError::InvalidCursor("empty revision page lacks inode identity".into())
        })?;
    let next_cursor = page
        .next_cursor
        .as_ref()
        .map(encode_file_revisions_cursor)
        .transpose()
        .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    Ok(ListFileRevisionsResponse {
        namespace_id,
        inode_id,
        head_seq,
        revisions: page.items,
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        ChangeSeq, CommitId, CommitOp, CommitPrecondition, CommitRequest, DisplayName, InodeId,
        MoveBehavior, NameKey, NamePolicy, RevisionNo,
    };

    #[test]
    fn explicit_commit_facade_exports_constructor_types() {
        let display_name = DisplayName::parse("Report.txt").expect("valid display name");
        let name_key = NameKey::for_display_name(NamePolicy::default(), &display_name);
        let precondition = CommitPrecondition::BindingIs {
            parent_inode_id: InodeId(1),
            name_key,
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(3),
            bind_delta_index: 4,
        };

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
                    new_parent_inode_id: InodeId(1),
                    new_display_name: "report.txt".to_owned(),
                    behavior: MoveBehavior::NoReplace,
                },
            ],
            message: None,
        };

        assert_eq!(request.preconditions.len(), 1);
        assert_eq!(request.ops.len(), 2);
    }
}
