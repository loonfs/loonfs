//! Path mutations, commits, and the publication pipeline.

use super::core::{BackgroundTickClaim, FsCore};
use crate::publish::{NamespaceMutationCandidate, PathMutationIntent, PreparedContent};
use crate::{
    ChangeSeq, CommitId, CommitOp, CommitPrecondition, CommitRequest, CommitResponse, ContentRef,
    CopyOptions, CoreError, CreateDirectoryOptions, DeleteOptions, InodeId, MaintenanceTickOptions,
    MoveOptions, NamespaceId, PutFileOptions, RestoreRevisionOptions, RevisionNo, UndeleteOptions,
};
use crate::{Result, RuntimeError};

impl FsCore {
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
    ) -> Result<CommitResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
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
        name = "loon.prepare",
        err,
        skip_all,
        fields(
            operation = "prepare",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub(crate) async fn prepare_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
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
        Ok(loonfs_core::content::prepare_stored_content(
            namespace_id.clone(),
            stored,
        ))
    }

    /// Publishes a file revision from already-prepared content.
    ///
    /// Submission and publication perform no content I/O. `options.behavior`
    /// selects create-only or replace semantics.
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
    pub(crate) async fn put_file_prepared(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        prepared_content: PreparedContent,
        options: PutFileOptions,
    ) -> Result<CommitResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(prepared_content.content_ref().size_bytes).unwrap_or(usize::MAX),
            ),
        );
        let content_ref = prepared_content.content_ref().clone();
        self.publish_candidate(
            namespace_id,
            NamespaceMutationCandidate::path_prepared(
                PathMutationIntent::PutFile {
                    commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                    absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    content_ref,
                    behavior: options.behavior,
                },
                vec![prepared_content],
            ),
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
    ) -> Result<CommitResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
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
        name = "loon.prepare",
        err,
        skip_all,
        fields(
            operation = "prepare",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub(crate) async fn prepare_content_ref(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<PreparedContent> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
            ),
        );
        let content_store_id = match self.load_namespace_catalog_cached(namespace_id).await? {
            Some(catalog) => catalog.content_store_id().clone(),
            None => {
                loonfs_core::control::load_namespace_catalog_entry(&self.inner.store, namespace_id)
                    .await
                    .map_err(CoreError::from)?
                    .content_store_id()
                    .clone()
            }
        };
        Ok(loonfs_core::content::prepare_existing_content_ref(
            &self.inner.store,
            namespace_id,
            &content_store_id,
            content_ref,
        )
        .await
        .map_err(CoreError::from)?)
    }

    /// Creates a directory at an absolute path.
    pub(crate) async fn create_directory(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::CreateDir {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                parents: options.parents,
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
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::DeletePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                behavior: options.behavior,
                expected_inode_id: options.expected_inode_id,
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
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::MovePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                from_path: loonfs_core::path::parse_mutation_path(from_path)?,
                to_path: loonfs_core::path::parse_mutation_path(to_path)?,
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
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::CopyFilePath {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                from_path: loonfs_core::path::parse_mutation_path(from_path)?,
                to_path: loonfs_core::path::parse_mutation_path(to_path)?,
                behavior: options.behavior,
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
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::RestoreRevision {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                source_revision_no,
            },
        )
        .await
    }

    /// Recovers a deleted file or subtree: clears the tombstone rooted at
    /// `inode_id` and binds it at `absolute_path`. The inode id is the one
    /// the delete reported (also visible in the change feed).
    pub(crate) async fn undelete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        absolute_path: &str,
        options: UndeleteOptions,
    ) -> Result<CommitResponse> {
        self.publish_path_intent(
            namespace_id,
            PathMutationIntent::Undelete {
                commit_id: options.commit_id.unwrap_or_else(CommitId::generate),
                inode_id,
                deleted_at_seq,
                absolute_path: loonfs_core::path::parse_mutation_path(absolute_path)?,
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

    /// Submits one explicit semantic commit request.
    ///
    /// This is the lower-level surface for clients that need their own commit
    /// ids, preconditions, and operation lists. Operations with external
    /// content refs require [`Self::commit_operations_prepared`].
    pub(crate) async fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.publish_candidate(namespace_id, NamespaceMutationCandidate::commit(request))
            .await
    }

    /// Submits one semantic commit request with prepared content proofs.
    ///
    /// Submission and publication perform no content I/O. One prepared value
    /// covers every operation that uses its content ref.
    pub(crate) async fn commit_operations_prepared(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
        prepared_content: Vec<PreparedContent>,
    ) -> Result<CommitResponse> {
        self.publish_candidate(
            namespace_id,
            NamespaceMutationCandidate::commit_prepared(request, prepared_content),
        )
        .await
    }

    /// Submits explicit semantic commit requests, returning one result per
    /// request in order. Requests admitted together usually publish
    /// together, batched by the publication service.
    pub(crate) async fn commit_operations_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<Result<CommitResponse>> {
        self.publish_through_publisher(
            namespace_id,
            requests
                .into_iter()
                .map(NamespaceMutationCandidate::commit)
                .collect(),
        )
        .await
    }

    async fn publish_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
    ) -> Result<CommitResponse> {
        self.publish_candidate(namespace_id, NamespaceMutationCandidate::path(intent))
            .await
    }

    async fn publish_candidate(
        &self,
        namespace_id: &NamespaceId,
        candidate: NamespaceMutationCandidate,
    ) -> Result<CommitResponse> {
        self.publish_through_publisher(namespace_id, vec![candidate])
            .await
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                Err(RuntimeError::Core(CoreError::Internal(
                    "empty publication batch".to_owned(),
                )))
            })
    }

    /// Publishes direct submissions through the core's publication service
    /// (see [`crate::publisher`]): batching is adaptive, every submitter
    /// receives its own durable result, and admitted work is owned by the
    /// service's tasks — a cancelled caller abandons only its result
    /// delivery, never the publication. Candidates are admitted in order,
    /// so one call's requests usually publish as one batch.
    async fn publish_through_publisher(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let submissions = candidates.into_iter().map(|candidate| {
            self.inner
                .publisher
                .submit_candidate(namespace_id.clone(), candidate)
        });
        futures::future::join_all(submissions)
            .await
            .into_iter()
            .map(|result| result.map_err(RuntimeError::Core))
            .collect()
    }

    /// Publishes already-classified namespace mutation candidates as one
    /// batch: one WAL segment, one head compare-and-swap.
    ///
    /// This is the engine-level publish the publication service's tasks
    /// drive; results match candidates in order. Everything else submits
    /// through [`Self::publish_through_publisher`].
    pub(crate) async fn publish_namespace_mutations_batch(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
        let store = self.store();
        let context = match self.mutation_context() {
            Ok(context) => context,
            Err(error) => return candidates.iter().map(|_| Err(error.clone())).collect(),
        };
        if self.commit_engine_cache_enabled() {
            // Warm the immutable catalog through the control cache so a
            // recreated engine starts seeded; a load failure here surfaces
            // as the publish view's own, properly shaped error instead.
            self.load_namespace_catalog_cached(namespace_id).await.ok();
            let engine = self.commit_engine(namespace_id);
            let mut publish = {
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

        // Cache-disabled diagnostic mode: a throwaway engine per publish,
        // but the session's epoch and fencing still come from the registry —
        // cache configuration disables neither session state nor
        // maintenance scheduling.
        let mut engine = loonfs_core::publish::NamespaceCommitEngine::new(namespace_id.clone())
            .writer_session(self.inner.writer_sessions.state(namespace_id));
        // Boxed for the same type-recursion reason as the cached-engine path.
        let publish = Box::pin(engine.publish_batch(&store, candidates, &context)).await;
        let wal_tail_segments = publish.wal_tail_segments;
        let results: Vec<_> = publish
            .results
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
        self.maybe_auto_tick_after_publish(namespace_id, wal_tail_segments);
        results
    }

    /// Schedules a maintenance tick after a publish that observed the WAL
    /// tail at or past the checkpoint threshold. Ticks are spawned on the
    /// handle's owning runtime — never on a hidden LoonFS runtime — so no
    /// writer (and no server batch pipeline) waits behind a checkpoint or
    /// base rebuild. The per-namespace singleflight claim dedupes concurrent
    /// publishers and is released on every outcome, including tick panics
    /// and dropped tasks.
    fn maybe_auto_tick_after_publish(&self, namespace_id: &NamespaceId, wal_tail_segments: u64) {
        let options = MaintenanceTickOptions::default();
        let run_full_tick = wal_tail_segments >= options.max_wal_tail_segments;
        // Below the WAL threshold, index catch-up alone is still scheduled
        // when the gram index is (or may be) enabled: index lag is measured
        // in files, not segments, and one publish can put more files in the
        // tail than a grep will scan. `None` means this process has not
        // observed the namespace yet; the drain it schedules learns the
        // answer, so the discovery cost is one cheap step per namespace per
        // process. Flush cadence is unchanged — only the full tick moves
        // WAL segments into tables.
        let index_may_lag = self.grams_hint(namespace_id) != Some(false);
        if !run_full_tick && !index_may_lag {
            return;
        }
        if !self.inner.background.try_claim(namespace_id, run_full_tick) {
            return;
        }
        let mut claim = BackgroundTickClaim {
            fs: self.clone(),
            namespace_id: namespace_id.clone(),
            releases_on_drop: true,
        };
        self.inner.background.spawn(async move {
            let mut run_full_tick = run_full_tick;
            loop {
                if let Err(error) = claim
                    .fs
                    .run_auto_maintenance(&claim.namespace_id, options, run_full_tick)
                    .await
                {
                    tracing::info!(
                        phase = "auto_maintenance_tick",
                        result = "error",
                        error = %error,
                        "post-publish maintenance tick failed"
                    );
                }
                if !claim.finish_tick() {
                    break;
                }
                run_full_tick = true;
            }
        });
    }

    async fn run_auto_maintenance(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
        run_full_tick: bool,
    ) -> Result<()> {
        if run_full_tick {
            self.maintenance_tick_namespace(namespace_id, options)
                .await?;
            self.drain_reorganization_backlog(namespace_id).await?;
        }
        self.drain_grams_index_backlog(namespace_id).await
    }
}
