//! [`FsWriter`]'s path mutations, commits, and the publication pipeline.

use super::core::{ReadCore, WriterBits};
use crate::publish::{CommitCandidate, CommitRequest, FilesystemOperation, PreparedContent};
use crate::ByteStream;
use crate::FsWriter;
use crate::{
    ChangeSeq, CommitId, CommitResponse, ContentRef, CopyOptions, CoreError,
    CreateDirectoryOptions, DeleteOptions, InodeId, MaintenanceJobId, MaintenanceStepOptions,
    MoveOptions, NamespaceId, PutFileOptions, RestoreRevisionOptions, RevisionNo, UndeleteOptions,
};
use crate::{ChecksumAlgorithm, CommittedChange, FilesystemChange};
use crate::{Result, RuntimeError};
use loonfs_api::{EffectiveLimit, ErrorCode, StorageChecksum};
use loonfs_core::NamespaceEngine;
use std::num::NonZeroU32;
use std::sync::Arc;

/// What a retry can still say about the payload it just staged.
///
/// A buffered put can answer any question about its bytes by hashing them
/// again. A streamed put cannot: the payload went by once and is gone, and
/// what it kept instead is the reference the write path built while hashing
/// it. Both describe the same bytes; they differ only in which questions
/// they can answer, and a question neither can answer is reported as such
/// rather than guessed at.
#[derive(Debug, Clone, Copy)]
enum StagedPayload<'a> {
    /// The payload itself, which can produce any digest this build knows.
    Bytes(&'a [u8]),
    /// The reference built for a payload that was streamed past.
    Streamed(&'a ContentRef),
}

impl StagedPayload<'_> {
    /// Complete length of what was staged.
    fn size_bytes(&self) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes.len() as u64,
            Self::Streamed(content_ref) => content_ref.size_bytes,
        }
    }

    /// Whether the staged bytes produce this checksum. `None` is a refusal
    /// to answer, never agreement.
    fn matches(&self, expected: &StorageChecksum) -> Option<bool> {
        match self {
            Self::Bytes(bytes) => expected.matches(bytes),
            Self::Streamed(content_ref) => {
                let observed = if content_ref.storage_checksum.algorithm == expected.algorithm {
                    Some(content_ref.storage_checksum.value.as_str())
                } else if expected.algorithm == ChecksumAlgorithm::Sha256 {
                    content_ref.whole_file_sha256.as_deref()
                } else {
                    None
                };
                Some(observed? == expected.value)
            }
        }
    }
}

/// Where a reuse conflict says the commit id already landed.
///
/// The publisher decides the conflict against a durable receipt, and the
/// receipt holds the sequence, so the error carries it. It is absent only
/// when nothing has committed under the id yet — two conflicting requests
/// claiming it at once — and then there is nothing to read back.
fn reported_committed_seq(error: &RuntimeError) -> Option<ChangeSeq> {
    match error {
        RuntimeError::Core(CoreError::CommitIdReuseConflict { committed_seq, .. }) => {
            *committed_seq
        }
        _ => None,
    }
}

/// The content one committed change wrote, if it wrote any.
fn committed_content_ref(change: &CommittedChange) -> Option<&ContentRef> {
    change.events.iter().find_map(|event| match event {
        FilesystemChange::Created { content_ref, .. } => content_ref.as_ref(),
        FilesystemChange::ContentChanged { content_ref, .. } => Some(content_ref),
        _ => None,
    })
}

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
    /// Re-running this with a `commit_id` that already committed is safe
    /// when the bytes are the same. A commit's identity names *which*
    /// content object it wrote, and a re-run necessarily stages a fresh
    /// one, so the publisher sees a different commit and reports
    /// `commit_id_reuse_conflict`. This resolves that by reading back what
    /// the commit id actually committed and comparing its content against
    /// the bytes just staged: equal means the operation had already
    /// succeeded. Different bytes, or content this build cannot compare,
    /// surface the conflict.
    ///
    /// The freshly staged duplicate object is then referenced by nothing.
    /// That is by design, not a leak: content garbage collection reclaims an
    /// unreferenced object once its grace passes.
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
        let commit_id = options.commit_id.clone();
        let prepared_content = self.prepare_file_bytes(namespace_id, bytes).await?;
        let published = self
            .put_file_prepared(namespace_id, absolute_path, prepared_content, options)
            .await;
        self.settle_put(
            namespace_id,
            commit_id,
            published,
            StagedPayload::Bytes(bytes),
        )
        .await
    }

    /// Writes a file from a payload read once from its source.
    ///
    /// This is [`Self::put_file_bytes`] for a caller that should not hold
    /// its payload: the stream is hashed as it is forwarded to object
    /// storage and never held whole, so a large file costs one transfer
    /// part of memory rather than its own size. Everything after the bytes
    /// land is identical — same trusted whole-file SHA-256, same
    /// publication, same retry reconciliation — so a caller that already
    /// holds its bytes has no reason to come here.
    ///
    /// Retrying with a `commit_id` that already committed is safe in the
    /// same way it is for the buffered call. The evidence is different only
    /// because the payload is gone: what this pass measured and hashed on
    /// the way past is what the reconciliation compares.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.put",
        err,
        skip_all,
        fields(
            operation = "put",
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
        let commit_id = options.commit_id.clone();
        let prepared_content = self.prepare_file_stream(namespace_id, body).await?;
        // The prepared reference describes the bytes that just went past,
        // and with the payload gone it is the only description of them that
        // still exists.
        let staged = prepared_content.content_ref().clone();
        let published = self
            .put_file_prepared(namespace_id, absolute_path, prepared_content, options)
            .await;
        self.settle_put(
            namespace_id,
            commit_id,
            published,
            StagedPayload::Streamed(&staged),
        )
        .await
    }

    /// Turns a reused-commit-id rejection into the answer the retry
    /// deserves, and passes everything else through.
    async fn settle_put(
        &self,
        namespace_id: &NamespaceId,
        commit_id: Option<CommitId>,
        published: Result<CommitResponse>,
        staged: StagedPayload<'_>,
    ) -> Result<CommitResponse> {
        match (published, commit_id) {
            (Err(error), Some(commit_id)) if error.code() == ErrorCode::CommitIdReuseConflict => {
                self.reconcile_commit_id_reuse(namespace_id, &commit_id, staged, error)
                    .await
            }
            (published, _) => published,
        }
    }

    /// Decides whether a reused commit id already did this exact work.
    ///
    /// The evidence is the committed content itself, read from the change
    /// feed at the sequence the conflict reported and compared against the
    /// bytes just staged. Nothing weaker counts: a comparison this build
    /// cannot make is reported as a conflict that names why, never as
    /// agreement.
    async fn reconcile_commit_id_reuse(
        &self,
        namespace_id: &NamespaceId,
        commit_id: &CommitId,
        staged: StagedPayload<'_>,
        conflict: RuntimeError,
    ) -> Result<CommitResponse> {
        let Some(committed_seq) = reported_committed_seq(&conflict) else {
            return Err(conflict);
        };
        let Some(committed) = self
            .read_committed_change(namespace_id, commit_id, committed_seq)
            .await?
        else {
            return Err(conflict);
        };
        let Some(content_ref) = committed_content_ref(&committed) else {
            return Err(conflict);
        };
        if content_ref.size_bytes != staged.size_bytes() {
            return Err(conflict);
        }

        // The trusted whole-file digest when one exists, and otherwise the
        // reference's own storage checksum — which for a provider-assembled
        // multipart object is the only full-object evidence there is.
        let evidence = match &content_ref.whole_file_sha256 {
            Some(digest) => StorageChecksum {
                algorithm: ChecksumAlgorithm::Sha256,
                value: digest.clone(),
            },
            None => content_ref.storage_checksum.clone(),
        };
        match staged.matches(&evidence) {
            Some(true) => Ok(CommitResponse {
                namespace_id: namespace_id.clone(),
                commit_id: committed.commit_id,
                committed_seq: committed.seq,
            }),
            Some(false) => Err(conflict),
            None => Err(RuntimeError::Config(format!(
                "commit `{commit_id}` already committed content checksummed with `{}`, \
                 which this build cannot compare against what it just staged, so it \
                 cannot tell whether this is the same upload retried",
                evidence.algorithm
            ))),
        }
    }

    /// Reads the one change a reuse conflict said the commit id landed at.
    ///
    /// There is no by-commit-id read, but the conflict already named the
    /// sequence, so this is one feed page positioned on it rather than a
    /// search. `None` means the evidence is not there to compare — the
    /// sequence has fallen below the retention floor, or the row at it
    /// belongs to some other commit — and the caller turns that into the
    /// conflict rather than into a guess.
    async fn read_committed_change(
        &self,
        namespace_id: &NamespaceId,
        commit_id: &CommitId,
        committed_seq: ChangeSeq,
    ) -> Result<Option<CommittedChange>> {
        let after_seq = ChangeSeq(committed_seq.0.saturating_sub(1));
        let page = match self
            .engine(namespace_id)
            .list_changes_after(after_seq, EffectiveLimit::new(NonZeroU32::MIN))
            .await
        {
            Ok(page) => page,
            // Retention gave up the replay promise below the floor: the
            // commit landed, but what it wrote can no longer be read back.
            Err(error) if error.code() == ErrorCode::RebootstrapRequired => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(page
            .changes
            .into_iter()
            .find(|change| change.seq == committed_seq && &change.commit_id == commit_id))
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

    /// Stages a streamed payload as durable content for later publication.
    ///
    /// The stream is hashed as it is forwarded to object storage and never
    /// held whole, so a large file costs one transfer part of memory rather
    /// than its own size. What comes back is the same [`PreparedContent`]
    /// [`Self::prepare_file_bytes`] produces — same trusted whole-file
    /// SHA-256, same publication path, same guarantees — so callers that
    /// already hold their bytes have no reason to come here.
    #[tracing::instrument(
        level = "info",
        name = "loonfs.prepare",
        err,
        skip_all,
        fields(
            operation = "prepare",
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
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        let stored = loonfs_core::content::store_stream_as_content_with_store_id(
            &self.core.inner.store,
            catalog.content_store_id().clone(),
            body,
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
    maybe_auto_step_after_publish(writer, namespace_id, wal_tail_segments);
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

/// Tells the maintenance runner a namespace crossed its WAL-tail threshold.
///
/// The nudge is a hint and nothing more: it never blocks the publish, never
/// waits for a permit, and carries no claim on the step it suggests. The
/// runner decides when — and whether — to run, and the step it eventually
/// runs re-reads the tail rather than trusting what this publish saw.
fn maybe_auto_step_after_publish(
    writer: &Arc<WriterBits>,
    namespace_id: &NamespaceId,
    wal_tail_segments: u64,
) {
    if wal_tail_segments < MaintenanceStepOptions::default().max_wal_tail_segments {
        return;
    }
    writer
        .maintenance
        .handle()
        .nudge(MaintenanceJobId::METADATA, namespace_id);
}
