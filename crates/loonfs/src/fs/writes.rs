//! [`FsWriter`]'s path mutations, commits, and the publication pipeline.

use super::core::{BackgroundStepClaim, ReadCore, WriterBits};
use crate::publish::{CommitCandidate, CommitRequest, FilesystemOperation, PreparedContent};
use crate::publisher::PublisherRegistry;
use crate::FsWriter;
use crate::{
    ChangeSeq, CommitId, CommitResponse, ContentRef, CopyOptions, CoreError,
    CreateDirectoryOptions, DeleteOptions, InodeId, MaintenanceStepOptions, MoveOptions,
    NamespaceId, PutFileOptions, RestoreRevisionOptions, RevisionNo, UndeleteOptions,
};
use crate::{Result, RuntimeError};
use loonfs_core::NamespaceEngine;
use std::sync::Arc;

impl FsWriter {
    /// A mutating engine under this writer's identity.
    pub(crate) fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceEngine<crate::SharedObjectStore> {
        self.core.writer_engine(&self.bits.identity, namespace_id)
    }

    /// Drops everything this runtime caches for a namespace: the read
    /// caches, and the rebuildable half of its publisher's publish state.
    pub(crate) fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
        self.core.invalidate_namespace_read_cache(namespace_id);
        self.publisher.invalidate_engine(namespace_id);
    }

    pub(crate) fn finish_namespace_mutation<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<T>,
    ) -> Result<T> {
        if super::should_invalidate_after_result(&result) {
            self.invalidate_namespace(namespace_id);
        }
        result
    }

    /// Writes file bytes to a path.
    ///
    /// The bytes become durable content first; metadata referencing them is
    /// published only afterward. `options.behavior` selects create-only or
    /// replace semantics.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.put",
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
    ) -> Result<CommitResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
        let prepared_content = self.prepare_file_bytes(namespace_id, bytes).await?;
        self.put_file_prepared(namespace_id, absolute_path, prepared_content, options)
            .await
    }

    /// Stages file bytes as durable content for later publication.
    ///
    /// Preparation performs one content PUT and no content reads. A publish
    /// that fails afterward leaves only a GC-covered orphan behind an
    /// unmoved head.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.prepare",
        err,
        skip_all,
        fields(
            operation = "prepare",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn prepare_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        let stored = loonfs_core::content::store_bytes_as_content_with_store_id(
            &self.core.inner.store,
            catalog.content_store_id().clone(),
            bytes,
        )
        .await?;
        Ok(
            loonfs_core::content::prepare_stored_content(&catalog, stored)
                .map_err(CoreError::from)?,
        )
    }

    /// Publishes a file revision from already-prepared content.
    ///
    /// Submission and publication perform no content I/O. `options.behavior`
    /// selects create-only or replace semantics.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.put",
        err,
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_file_prepared(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        prepared_content: PreparedContent,
        options: PutFileOptions,
    ) -> Result<CommitResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(prepared_content.content_ref().size_bytes).unwrap_or(usize::MAX),
            ),
        );
        let content_ref = prepared_content.content_ref().clone();
        self.commit_prepared(
            namespace_id,
            CommitRequest::single(
                options.commit_id.unwrap_or_else(CommitId::generate),
                options.message.clone(),
                FilesystemOperation::PutFile {
                    absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    content_ref,
                    behavior: options.behavior,
                    expected_revision_no: options.expected_revision_no,
                },
            ),
            vec![prepared_content],
        )
        .await
    }

    /// Publishes a file revision that points at an already-durable content ref.
    ///
    /// This explicitly slow helper reads the full object to prove durability
    /// before publication. Callers that already hold proof should prefer
    /// [`Self::put_file_prepared`].
    #[tracing::instrument(
        level = "info",
        name = "loonfs.put",
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
    ) -> Result<CommitResponse> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
            ),
        );
        let prepared_content = self.prepare_content_ref(namespace_id, content_ref).await?;
        self.put_file_prepared(namespace_id, absolute_path, prepared_content, options)
            .await
    }

    /// Fully validates an existing content ref for later publication.
    ///
    /// Preparation performs one content HEAD followed by one full content
    /// GET and digest check. Later prepared publication performs no content
    /// I/O.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.prepare",
        err,
        skip_all,
        fields(
            operation = "prepare",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn prepare_content_ref(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<PreparedContent> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
            ),
        );
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        Ok(loonfs_core::content::prepare_existing_content_ref(
            &self.core.inner.store,
            &catalog,
            content_ref,
        )
        .await
        .map_err(CoreError::from)?)
    }

    /// Verifies an authorized content token against the namespace's durable
    /// content-store binding.
    pub async fn prepare_content_token(
        &self,
        namespace_id: &NamespaceId,
        secret: &str,
        token: &loonfs_api::v0::ValidatedContentToken,
        now_ms: u64,
    ) -> Result<std::result::Result<PreparedContent, loonfs_core::content::ContentTokenError>> {
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        Ok(loonfs_core::content::verify_content_token(
            secret, &catalog, token, now_ms,
        ))
    }

    /// Loads the namespace identity used to bind prepared content to its
    /// content store.
    pub(crate) async fn load_namespace_catalog_for_content_preparation(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<loonfs_core::control::VerifiedNamespaceCatalogEntry> {
        self.core.load_namespace_catalog_cached(namespace_id).await
    }

    /// Creates a directory at an absolute path.
    pub async fn create_directory(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> Result<CommitResponse> {
        self.commit(
            namespace_id,
            CommitRequest::single(
                options.commit_id.unwrap_or_else(CommitId::generate),
                options.message.clone(),
                FilesystemOperation::CreateDir {
                    absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    parents: options.parents,
                },
            ),
        )
        .await
    }

    /// Deletes a file or directory path.
    ///
    /// Deletion is tombstone-first: the commit hides the path without erasing
    /// history. Physical reclamation is explicit garbage collection: nothing
    /// sweeps unless an operator asks, through `FsAdmin::gc_namespace` or a
    /// maintenance step that opted in.
    pub async fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<CommitResponse> {
        self.commit(
            namespace_id,
            CommitRequest::single(
                options.commit_id.unwrap_or_else(CommitId::generate),
                options.message.clone(),
                FilesystemOperation::DeletePath {
                    absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    behavior: options.behavior,
                    expected_inode_id: options.expected_inode_id,
                },
            ),
        )
        .await
    }

    /// Moves a path within the same namespace.
    pub async fn move_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<CommitResponse> {
        self.commit(
            namespace_id,
            CommitRequest::single(
                options.commit_id.unwrap_or_else(CommitId::generate),
                options.message.clone(),
                FilesystemOperation::MovePath {
                    from_path: loonfs_core::path::parse_mutation_path(from_path)?,
                    to_path: loonfs_core::path::parse_mutation_path(to_path)?,
                    behavior: options.behavior,
                },
            ),
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
    ) -> Result<CommitResponse> {
        self.commit(
            namespace_id,
            CommitRequest::single(
                options.commit_id.unwrap_or_else(CommitId::generate),
                options.message.clone(),
                FilesystemOperation::CopyFilePath {
                    from_path: loonfs_core::path::parse_mutation_path(from_path)?,
                    to_path: loonfs_core::path::parse_mutation_path(to_path)?,
                    behavior: options.behavior,
                },
            ),
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
    ) -> Result<CommitResponse> {
        self.commit(
            namespace_id,
            CommitRequest::single(
                options.commit_id.unwrap_or_else(CommitId::generate),
                options.message.clone(),
                FilesystemOperation::RestoreRevision {
                    absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    source_revision_no,
                },
            ),
        )
        .await
    }

    /// Recovers a deleted file or subtree: clears the tombstone rooted at
    /// `inode_id` and binds it at `absolute_path`. The inode id is the one
    /// the delete reported (also visible in the change feed).
    pub async fn undelete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        absolute_path: &str,
        options: UndeleteOptions,
    ) -> Result<CommitResponse> {
        self.commit(
            namespace_id,
            CommitRequest::single(
                options.commit_id.unwrap_or_else(CommitId::generate),
                options.message.clone(),
                FilesystemOperation::Undelete {
                    inode_id,
                    deleted_at_seq,
                    absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                },
            ),
        )
        .await
    }

    /// Applies one commit request: its operations land together, in
    /// order, under one commit id.
    ///
    /// Each operation resolves against the namespace plus everything the
    /// operations ahead of it do, so a request can create a directory and
    /// write into it. Nothing commits unless every operation does, and the
    /// error of a request that stops names the operation that stopped it.
    /// Operations that introduce new external content require
    /// [`Self::commit_prepared`].
    pub async fn commit(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.publish_candidate(namespace_id, CommitCandidate::new(request))
            .await
    }

    /// Applies one commit request with prepared content proofs.
    ///
    /// Submission and publication perform no content I/O. One prepared value
    /// covers every operation that uses its content ref.
    pub async fn commit_prepared(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
        prepared_content: Vec<PreparedContent>,
    ) -> Result<CommitResponse> {
        self.publish_candidate(
            namespace_id,
            CommitCandidate::prepared(request, prepared_content),
        )
        .await
    }

    /// Publishes one candidate through the core's publication service (see
    /// [`crate::publisher`]): batching is adaptive, every submitter receives
    /// its own durable result, and admitted work is owned by the service's
    /// worker — a cancelled caller abandons only its result delivery, never
    /// the publication.
    async fn publish_candidate(
        &self,
        namespace_id: &NamespaceId,
        candidate: CommitCandidate,
    ) -> Result<CommitResponse> {
        self.publisher
            .submit_candidate(namespace_id.clone(), candidate)
            .await
            .map_err(RuntimeError::Core)
    }
}

/// Publishes already-classified candidates as one batch — one WAL
/// segment, one head compare-and-swap — through the namespace
/// publisher's own commit engine, and settles the runtime state the
/// batch produced: read caches, publish observer, maintenance.
///
/// Only the publication service calls this: it owns the engine, and
/// borrowing it here keeps engine construction and locking in that one
/// place. `publisher` is the registry a scheduled maintenance step
/// invalidates engines through; it is absent only for a publisher whose
/// registry is already gone. Results match candidates in order.
pub(crate) async fn publish_batch_with_engine(
    core: &ReadCore,
    writer: &Arc<WriterBits>,
    publisher: Option<&PublisherRegistry>,
    namespace_id: &NamespaceId,
    engine: &mut loonfs_core::publish::NamespaceCommitEngine,
    candidates: Vec<CommitCandidate>,
) -> Vec<Result<CommitResponse>> {
    let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    let store = core.store();
    let context = match writer.identity.mutation_context() {
        Ok(context) => context,
        Err(error) => return candidates.iter().map(|_| Err(error.clone())).collect(),
    };
    let cache_config = &core.inner.config.runtime_cache;
    let tail_options = loonfs_core::publish::PublishTailOptions {
        max_tail_rows: cache_config.max_cached_wal_tail_projection_rows,
        max_tail_decoded_bytes: cache_config.max_cached_wal_tail_projection_decoded_bytes,
    };
    // Boxing erases the engine's deeply nested publish future; without
    // it, callers awaiting a put or commit (CLI, server, embedding
    // crates) exceed rustc's type-recursion depth.
    let mut publish =
        Box::pin(engine.publish_batch(&store, candidates, &context, &tail_options)).await;
    if !core.control_cache_enabled() {
        // Diagnostic mode: the publisher's engine outlives the publish
        // even with caches off, so drop the tail projection it just
        // built. Every publish then reads what a cold engine reads.
        engine.invalidate();
    }
    {
        let _span = tracing::info_span!(
            "loonfs.phase",
            phase = "batch_update_cache",
            mode = core.trace_mode(),
            store_kind = core.trace_store_kind(),
            batch_size
        )
        .entered();
        match publish.resulting_read_state.take() {
            // A landed publish hands the caches exactly the state a
            // rebuild would recompute; use it instead of dropping.
            Some(state) => core.seed_namespace_read_cache(namespace_id, state),
            None => {
                let runtime_results = publish
                    .results
                    .iter()
                    .map(|result| result.clone().map_err(RuntimeError::Core))
                    .collect::<Vec<_>>();
                core.invalidate_read_cache_after_batch(namespace_id, &runtime_results);
            }
        }
    }
    let wal_tail_segments = publish.wal_tail_segments;
    let results = publish
        .results
        .into_iter()
        .map(|result| result.map_err(RuntimeError::Core))
        .collect::<Vec<_>>();
    notify_publish_observer(writer, namespace_id, &results);
    maybe_auto_step_after_publish(core, writer, publisher, namespace_id, wal_tail_segments);
    results
}

fn notify_publish_observer(
    writer: &WriterBits,
    namespace_id: &NamespaceId,
    results: &[Result<CommitResponse>],
) {
    let Some(observer) = &writer.publish_observer else {
        return;
    };
    if let Some(committed_seq) = results
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .map(|response| response.committed_seq)
        .max()
    {
        observer(namespace_id, committed_seq);
    }
}

/// Schedules a maintenance step after a publish that observed the WAL
/// tail at or past the checkpoint threshold. Steps are spawned on the
/// writer's owning runtime — never on a hidden LoonFS runtime — so no
/// writer (and no server batch pipeline) waits behind a checkpoint or
/// base rebuild. The per-namespace singleflight claim dedupes concurrent
/// publishers and is released on every outcome, including step panics
/// and dropped tasks.
fn maybe_auto_step_after_publish(
    core: &ReadCore,
    writer: &Arc<WriterBits>,
    publisher: Option<&PublisherRegistry>,
    namespace_id: &NamespaceId,
    wal_tail_segments: u64,
) {
    let options = MaintenanceStepOptions::default();
    if wal_tail_segments < options.max_wal_tail_segments {
        return;
    }
    // No registry means the publication service that would own the step is
    // already gone; there is nothing left to schedule maintenance for.
    let Some(publisher) = publisher else {
        return;
    };
    if !writer.background.try_claim(namespace_id) {
        return;
    }
    let claim = BackgroundStepClaim {
        core: core.clone(),
        bits: Arc::clone(writer),
        publisher: publisher.clone(),
        namespace_id: namespace_id.clone(),
    };
    spawn_claimed_auto_maintenance(claim, options);
}

fn spawn_claimed_auto_maintenance(claim: BackgroundStepClaim, options: MaintenanceStepOptions) {
    // Type erasure lets a finishing task transfer its claim and spawn the
    // next queued namespace without forming a recursive future type.
    let background = Arc::clone(&claim.bits.background);
    let future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(async move {
            let mut claim = claim;
            loop {
                if let Err(error) = run_auto_maintenance(&claim, options.clone()).await {
                    tracing::info!(
                        phase = "auto_maintenance_step",
                        result = "error",
                        error = %error,
                        "post-publish maintenance step failed"
                    );
                }
                let Some(next_namespace_id) =
                    claim.bits.background.finish_or_rerun(&claim.namespace_id)
                else {
                    break;
                };
                if next_namespace_id == claim.namespace_id {
                    // Preserve the existing own-namespace rerun in this
                    // task before handing its slot to global queued work.
                    continue;
                }
                claim.namespace_id = next_namespace_id;
                spawn_claimed_auto_maintenance(claim, options);
                return;
            }
        });
    background.spawn(future);
}

async fn run_auto_maintenance(
    claim: &BackgroundStepClaim,
    options: MaintenanceStepOptions,
) -> Result<()> {
    // Background maintenance runs the same operations an operator runs,
    // through the same handle, rather than a private copy of them — over
    // the writer's own runtime, so its invalidations reach the writer's
    // caches and publisher engines.
    let admin = crate::FsAdmin::from_writer_parts(
        claim.core.clone(),
        claim.bits.identity.clone(),
        claim.publisher.clone(),
    );
    let started = tokio::time::Instant::now();
    let step = admin
        .maintenance_step_namespace(&claim.namespace_id, options)
        .await?;
    admin
        .drain_reorganization_backlog(&claim.namespace_id)
        .await?;
    // Quiet conclusions (`NotNeeded`) emit nothing at default levels;
    // this is the only record that a background step ran at all.
    tracing::debug!(
        phase = "auto_maintenance_step",
        namespace_id = %step.namespace_id,
        wal_tail_segments_before = step.status_before.wal_tail_segments,
        wal_flush = ?step.wal_flush,
        reorganize = ?step.reorganize,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "background maintenance step concluded"
    );
    Ok(())
}
