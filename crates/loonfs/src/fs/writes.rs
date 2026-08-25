//! [`FsWriter`]'s path mutations, commits, and the publication pipeline.

use super::core::{ReadCore, WriterBits};
use crate::maintenance_runner::NamespacePublication;
use crate::publish::{CommitCandidate, CommitRequest, FilesystemOperation, PreparedContent};
use crate::trace::phase_span;
use crate::ByteStream;
use crate::FsWriter;
use crate::{
    ChangeSeq, CommitId, CommitOptions, CommitResponse, ContentRef, CopyOptions, CoreError,
    CreateDirectoryOptions, DeleteOptions, InodeId, MoveOptions, NamespaceId, PutFileOptions,
    RestoreRevisionOptions, RevisionNo, UndeleteOptions, UpdateAttributesOptions,
};
use crate::{Result, RuntimeError};
use loonfs_api::{
    ContentEvidence, EffectiveLimit, ErrorCode, PutRetryAttempt, PutRetryErrorClassification,
    PutRetryReceipt,
};
use loonfs_core::NamespaceWriterEngine;
use std::num::NonZeroU32;
use std::sync::Arc;

fn single_operation(commit: &CommitOptions, operation: FilesystemOperation) -> CommitRequest {
    CommitRequest::single(
        commit.commit_id.clone().unwrap_or_else(CommitId::generate),
        commit.actor.clone(),
        commit.message.clone(),
        operation,
    )
}

fn classify_put_retry_error(error: &RuntimeError) -> PutRetryErrorClassification {
    match error.code() {
        ErrorCode::CommitIdReuseConflict => {
            let receipt = error.details().and_then(|details| {
                Some(PutRetryReceipt {
                    committed_seq: details.committed_seq?,
                    committed_fingerprint: details.committed_fingerprint?,
                })
            });
            PutRetryErrorClassification::CommitIdReuseConflict(receipt)
        }
        ErrorCode::RebootstrapRequired => PutRetryErrorClassification::RebootstrapRequired,
        _ => PutRetryErrorClassification::Other,
    }
}

impl FsWriter {
    /// A mutating engine under this writer's identity.
    pub(crate) fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceWriterEngine<crate::SharedObjectStore> {
        self.core.writer_engine(&self.bits.identity, namespace_id)
    }

    /// Drops everything this runtime caches for a namespace: the read
    /// caches, and the rebuildable half of its publisher's publish state.
    pub(crate) fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
        self.core.invalidate_namespace_read_cache(namespace_id);
        self.publisher.invalidate_projection(namespace_id);
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
        level = "debug",
        name = "loonfs.put",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "put",
            method = "put_file_bytes",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    /// A retry with the same `commit_id` is reconciled against the durable
    /// receipt.
    ///
    /// Each retry stages a new content object, so its initial fingerprint differs
    /// from the committed request. On a reuse conflict, the runtime reads the
    /// committed change, rebuilds the fingerprint with the committed content
    /// reference, and verifies that the staged bytes match. The retry succeeds
    /// only when the complete request and payload are equivalent.
    ///
    /// The unused staged object remains unpublished and is reclaimed by content
    /// garbage collection after the grace period.
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
        let attempt = options.clone();
        let prepared_content = self.prepare_file_bytes_inner(namespace_id, bytes).await?;
        let published = self
            .put_file_prepared_inner(namespace_id, absolute_path, prepared_content, options)
            .await;
        self.settle_put(
            namespace_id,
            absolute_path,
            &attempt,
            published,
            ContentEvidence::Bytes(bytes),
        )
        .await
    }

    /// Writes a file from a payload read once from its source.
    ///
    /// This is [`Self::put_file_bytes`] for a caller that should not hold
    /// its payload: the stream is hashed as it is forwarded to object
    /// storage and never held whole, so a large file costs one transfer
    /// part of memory rather than its own size. Everything after the bytes
    /// land is identical — same full-object checksum, same
    /// publication, same retry reconciliation — so a caller that already
    /// holds its bytes has no reason to come here.
    ///
    /// Retrying with a `commit_id` that already committed is safe in the
    /// same way it is for the buffered call. The evidence is different only
    /// because the payload is gone: what this pass measured and hashed on
    /// the way past is what the reconciliation compares.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.put",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "put",
            method = "put_file_stream",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = "streamed",
        )
    )]
    pub async fn put_file_stream(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        body: ByteStream,
        options: PutFileOptions,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        let attempt = options.clone();
        let prepared_content = self.prepare_file_stream_inner(namespace_id, body).await?;
        // The prepared reference describes the bytes that just went past,
        // and with the payload gone it is the only description of them that
        // still exists.
        let staged = prepared_content.content_ref().clone();
        let published = self
            .put_file_prepared_inner(namespace_id, absolute_path, prepared_content, options)
            .await;
        self.settle_put(
            namespace_id,
            absolute_path,
            &attempt,
            published,
            ContentEvidence::ContentRef(&staged),
        )
        .await
    }

    /// Turns a reused-commit-id rejection into the answer the retry
    /// deserves, and passes everything else through.
    async fn settle_put(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        attempt: &PutFileOptions,
        published: Result<CommitResponse>,
        staged: ContentEvidence<'_>,
    ) -> Result<CommitResponse> {
        match (published, attempt.commit.commit_id.as_ref()) {
            (Err(error), Some(commit_id)) if error.code() == ErrorCode::CommitIdReuseConflict => {
                self.reconcile_commit_id_reuse(
                    namespace_id,
                    absolute_path,
                    commit_id,
                    attempt,
                    staged,
                    error,
                )
                .await
            }
            (published, _) => published,
        }
    }

    /// Checks whether a commit-ID conflict came from retrying the same file
    /// write.
    ///
    /// The shared helper compares the full request fingerprint and verifies
    /// that the new upload contains the same bytes as the original commit. If
    /// either check cannot be completed or does not match, this method returns
    /// the original conflict.
    async fn reconcile_commit_id_reuse(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        commit_id: &CommitId,
        attempt: &PutFileOptions,
        staged: ContentEvidence<'_>,
        conflict: RuntimeError,
    ) -> Result<CommitResponse> {
        let Ok(path) = loonfs_core::path::parse_mutation_path(absolute_path) else {
            return Err(conflict);
        };
        let engine = self.engine(namespace_id);
        loonfs_api::reconcile_put_commit_id_reuse(
            PutRetryAttempt {
                namespace_id,
                path: &path,
                commit_id,
                options: attempt,
                staged,
            },
            conflict,
            |after_seq| async move {
                engine
                    .list_changes_after(after_seq, EffectiveLimit::new(NonZeroU32::MIN))
                    .await
                    .map_err(RuntimeError::from)
            },
            classify_put_retry_error,
        )
        .await
    }

    /// Stores file bytes and returns proof that they are ready to publish.
    ///
    /// Preparation writes the upload-session record, the content object, and the
    /// completed session record, in that order. If publication later fails, the
    /// unpublished object is reclaimed after the content-reclamation grace
    /// period.
    ///
    /// The returned proof remains valid through the completed upload's receipt
    /// horizon. Publication rejects it after that deadline so garbage
    /// collection cannot reclaim the object before a later commit uses it.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.prepare",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "prepare",
            method = "prepare_file_bytes",
            namespace_id = %namespace_id,
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
        self.prepare_file_bytes_inner(namespace_id, bytes).await
    }

    async fn prepare_file_bytes_inner(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        Ok(self
            .engine(namespace_id)
            .stage_owned_bytes(&catalog, bytes)
            .await?)
    }

    /// Stages a streamed payload as durable content for later publication.
    ///
    /// The stream is hashed as it is forwarded to object storage and never
    /// held whole, so a large file costs one transfer part of memory rather
    /// than its own size. What comes back is the same [`PreparedContent`]
    /// [`Self::prepare_file_bytes`] produces — same full-object checksum,
    /// same publication path, same guarantees — so callers that
    /// already hold their bytes have no reason to come here.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.prepare",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "prepare",
            method = "prepare_file_stream",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = "streamed",
        )
    )]
    pub async fn prepare_file_stream(
        &self,
        namespace_id: &NamespaceId,
        body: ByteStream,
    ) -> Result<PreparedContent> {
        self.core.record_trace_context(&tracing::Span::current());
        self.prepare_file_stream_inner(namespace_id, body).await
    }

    async fn prepare_file_stream_inner(
        &self,
        namespace_id: &NamespaceId,
        body: ByteStream,
    ) -> Result<PreparedContent> {
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        Ok(self
            .engine(namespace_id)
            .stage_owned_stream(&catalog, body)
            .await?)
    }

    /// Publishes a file revision from already-prepared content.
    ///
    /// Submission and publication perform no content I/O. `options.behavior`
    /// selects create-only or replace semantics.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.put",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "put",
            method = "put_file_prepared",
            namespace_id = %namespace_id,
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
        self.put_file_prepared_inner(namespace_id, absolute_path, prepared_content, options)
            .await
    }

    async fn put_file_prepared_inner(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        prepared_content: PreparedContent,
        options: PutFileOptions,
    ) -> Result<CommitResponse> {
        let content_ref = prepared_content.content_ref().clone();
        self.commit_candidate_inner(
            namespace_id,
            CommitCandidate::prepared(
                single_operation(
                    &options.commit,
                    FilesystemOperation::PutFile {
                        path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                        content_ref,
                        behavior: options.behavior,
                        expected_revision_no: options.expected_revision_no,
                    },
                ),
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
        level = "debug",
        name = "loonfs.put",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "put",
            method = "put_file_content_ref",
            namespace_id = %namespace_id,
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
        let prepared_content = self
            .prepare_content_ref_inner(namespace_id, content_ref)
            .await?;
        self.put_file_prepared_inner(namespace_id, absolute_path, prepared_content, options)
            .await
    }

    /// Fully validates an existing content ref for later publication.
    ///
    /// Preparation performs one content HEAD followed by one full content
    /// GET and digest check. Later prepared publication performs no content
    /// I/O.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.prepare",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "prepare",
            method = "prepare_content_ref",
            namespace_id = %namespace_id,
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
        self.prepare_content_ref_inner(namespace_id, content_ref)
            .await
    }

    async fn prepare_content_ref_inner(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<PreparedContent> {
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
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.prepare",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "prepare",
            method = "prepare_content_token",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn prepare_content_token(
        &self,
        namespace_id: &NamespaceId,
        secret: &str,
        token: &crate::content_tokens::ContentToken,
        now_ms: u64,
    ) -> Result<std::result::Result<PreparedContent, loonfs_core::content::ContentTokenError>> {
        self.core.record_trace_context(&tracing::Span::current());
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
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "create_directory",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_directory(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(
            namespace_id,
            CommitCandidate::new(single_operation(
                &options.commit,
                FilesystemOperation::CreateDirectory {
                    path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    parents: options.parents,
                },
            )),
        )
        .await
    }

    /// Deletes a file or directory path.
    ///
    /// Deletion is tombstone-first: the commit hides the path without erasing
    /// history. Physical reclamation is explicit garbage collection: nothing
    /// sweeps unless an operator asks, through `FsAdmin::gc_namespace` or a
    /// maintenance step that opted in.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "delete_path",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(
            namespace_id,
            CommitCandidate::new(single_operation(
                &options.commit,
                FilesystemOperation::DeletePath {
                    path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    behavior: options.behavior,
                    expected_inode_id: options.expected_inode_id,
                },
            )),
        )
        .await
    }

    /// Moves a path within the same namespace.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "move_path",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn move_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(
            namespace_id,
            CommitCandidate::new(single_operation(
                &options.commit,
                FilesystemOperation::MovePath {
                    from_path: loonfs_core::path::parse_mutation_path(from_path)?,
                    to_path: loonfs_core::path::parse_mutation_path(to_path)?,
                    behavior: options.behavior,
                },
            )),
        )
        .await
    }

    /// Copies a file to a new path in the same namespace. The new file
    /// reuses the source revision's content reference: no bytes are copied.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "copy_path",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn copy_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(
            namespace_id,
            CommitCandidate::new(single_operation(
                &options.commit,
                FilesystemOperation::CopyPath {
                    from_path: loonfs_core::path::parse_mutation_path(from_path)?,
                    to_path: loonfs_core::path::parse_mutation_path(to_path)?,
                    behavior: options.behavior,
                },
            )),
        )
        .await
    }

    /// Restores a prior file revision by appending a new current revision.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "restore_file_revision",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn restore_file_revision(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(
            namespace_id,
            CommitCandidate::new(single_operation(
                &options.commit,
                FilesystemOperation::RestoreRevision {
                    path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    source_revision_no,
                },
            )),
        )
        .await
    }

    /// Writes and removes attributes on the inode a path resolves to. The
    /// target may be a file or a directory, because an attribute belongs to
    /// the resource.
    ///
    /// Naming neither a write nor a removal is rejected, as is an update that
    /// would leave the map exactly as it was.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "update_attributes",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn update_attributes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: UpdateAttributesOptions,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(
            namespace_id,
            CommitCandidate::new(single_operation(
                &options.commit,
                FilesystemOperation::UpdateAttributes {
                    path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                    set: options.set,
                    remove: options.remove,
                    expected_inode_id: options.expected_inode_id,
                    expected_attributes_revision_no: options.expected_attributes_revision_no,
                },
            )),
        )
        .await
    }

    /// Restores a deleted file or subtree, optionally at a new path.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "undelete",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn undelete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        absolute_path: Option<&str>,
        options: UndeleteOptions,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        // An absent destination restores in place: the entry re-binds under
        // the parent and name its deletion recorded.
        let path = absolute_path
            .map(loonfs_core::path::parse_mutation_path)
            .transpose()?;
        self.commit_candidate_inner(
            namespace_id,
            CommitCandidate::new(single_operation(
                &options.commit,
                FilesystemOperation::Undelete {
                    inode_id,
                    deletion_seq,
                    path,
                },
            )),
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
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "create_commit",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_commit(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(namespace_id, CommitCandidate::new(request))
            .await
    }

    /// Applies one commit request with prepared content proofs.
    ///
    /// Submission and publication perform no content I/O. One prepared value
    /// covers every operation that uses its content ref.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "commit_prepared",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn commit_prepared(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
        prepared_content: Vec<PreparedContent>,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(
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
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.apply_commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "apply_commit",
            method = "commit_candidate",
            namespace_id = %namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn commit_candidate(
        &self,
        namespace_id: &NamespaceId,
        candidate: CommitCandidate,
    ) -> Result<CommitResponse> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(namespace_id, candidate).await
    }

    async fn commit_candidate_inner(
        &self,
        namespace_id: &NamespaceId,
        candidate: CommitCandidate,
    ) -> Result<CommitResponse> {
        self.publisher
            .submit_candidate(namespace_id.clone(), candidate)
            .await
    }
}

/// Publishes already-classified candidates as one batch — one WAL
/// segment, one head compare-and-swap — through the namespace
/// publisher's own commit engine, and settles the runtime state the
/// batch produced: read caches, publish observer, maintenance.
///
/// Only the publication service calls this: it owns the engine, and
/// borrowing it here keeps engine construction and locking in that one
/// place. Results match candidates in order.
pub(crate) async fn publish_batch_with_engine(
    core: &ReadCore,
    writer: &Arc<WriterBits>,
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
    let cache_config = core.runtime_cache_config();
    // The per-projection ceiling. The publisher applies the same two knobs
    // as an aggregate over every projection it retains.
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
        engine.invalidate_projection();
    }
    {
        let _span = phase_span!(core, "batch_update_cache", namespace_id, batch_size).entered();
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
    writer.notify_after_publish(
        namespace_id,
        &NamespacePublication {
            committed_through_seq: highest_committed_seq(&results),
            wal_tail_segments,
        },
    );
    results
}

/// Returns the highest sequence committed by the batch.
fn highest_committed_seq(results: &[Result<CommitResponse>]) -> Option<ChangeSeq> {
    results
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .map(|response| response.committed_seq)
        .max()
}
