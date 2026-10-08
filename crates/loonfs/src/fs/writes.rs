//! The writable [`Namespace`] handle's path mutations and commits, and the
//! publication pipeline they go through.

use super::core::RuntimeCore;
use crate::publish::{CommitCandidate, CommitRequest, FilesystemOperation, PreparedContent};
use crate::trace::phase_span;
use crate::ByteStream;
use crate::Result;
use crate::{
    AccessState, ActorId, AttributeChanges, ChangeSeq, Commit, CommitId, CommitOptions, ContentRef,
    CopyOptions, CreateDirectoryOptions, DeleteOptions, InodeId, MoveOptions, NamespaceId,
    PutFileOptions, RevisionNo, UndeleteOptions, UpdateAccessOptions, UpdateAttributesOptions,
};
use crate::{LoonFs, Namespace, Writable};
use futures::StreamExt;
use loonfs_core::NamespaceWriterEngine;

fn single_operation(
    actor: &ActorId,
    commit: &CommitOptions,
    operation: FilesystemOperation,
) -> CommitRequest {
    CommitRequest::single(
        commit.commit_id.clone().unwrap_or_else(CommitId::generate),
        actor.clone(),
        commit.message.clone(),
        operation,
    )
    .preconditions(commit.preconditions.clone())
}

impl LoonFs<Writable> {
    /// A mutating engine under this runtime's writer identity.
    pub(crate) fn engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> NamespaceWriterEngine<crate::SharedObjectStore> {
        self.core
            .writer_engine(&self.mode.bits.identity, namespace_id)
    }

    /// Drops everything this runtime caches for a namespace: the read
    /// caches, and the rebuildable half of its publisher's publish state.
    pub(crate) fn invalidate_namespace(&self, namespace_id: &NamespaceId) {
        self.core.invalidate_namespace_read_cache(namespace_id);
        self.mode.publisher.invalidate_projection(namespace_id);
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
}

impl Namespace<Writable> {
    /// A mutating engine under this writer's identity.
    pub(crate) fn engine(&self) -> NamespaceWriterEngine<crate::SharedObjectStore> {
        self.core
            .writer_engine(&self.mode.bits.identity, &self.namespace_id)
    }

    /// Drops everything this runtime caches for the namespace: the read
    /// caches, and the rebuildable half of its publisher's publish state.
    pub(crate) fn invalidate_namespace(&self) {
        self.core
            .invalidate_namespace_read_cache(&self.namespace_id);
        self.mode
            .publisher
            .invalidate_projection(&self.namespace_id);
    }

    pub(crate) fn finish_namespace_mutation<T>(&self, result: Result<T>) -> Result<T> {
        if super::should_invalidate_after_result(&result) {
            self.invalidate_namespace();
        }
        result
    }

    /// Writes file bytes to a path, refusing to replace an existing file.
    pub async fn put_file(
        &self,
        absolute_path: &str,
        bytes: &[u8],
        actor: &ActorId,
    ) -> Result<Commit> {
        self.put_file_with_options(absolute_path, bytes, actor, &PutFileOptions::default())
            .await
    }

    /// Writes file bytes to a path.
    ///
    /// Content at or under the configured inline threshold is prepared inline
    /// and identified by its bytes. A rerun with the same bytes and commit ID
    /// replays. Larger content stages, so a rerun conflicts. At every size,
    /// retain [`Self::prepare_content`]'s result and retry with
    /// [`Self::put_file_prepared_with_options`], the same commit ID, and
    /// unchanged options.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.put_file",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "put_file",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_file_with_options(
        &self,
        absolute_path: &str,
        bytes: &[u8],
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
        let prepared_content = self.prepare_content_inner(bytes).await?;
        self.put_file_prepared_inner(absolute_path, prepared_content, actor, options)
            .await
    }

    /// Writes a file from a payload read once from its source, refusing to
    /// replace an existing file.
    pub async fn put_file_stream(
        &self,
        absolute_path: &str,
        body: ByteStream,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.put_file_stream_with_options(absolute_path, body, actor, &PutFileOptions::default())
            .await
    }

    /// Writes a file from a payload read once from its source.
    ///
    /// Content at or under the configured inline threshold is prepared inline
    /// and identified by its bytes. A rerun with the same bytes and commit ID
    /// replays. Larger content stages, so a rerun conflicts. At every size,
    /// retain [`Self::prepare_content_stream`]'s result and retry with
    /// [`Self::put_file_prepared_with_options`], the same commit ID, and
    /// unchanged options.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.put_file_stream",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "put_file_stream",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = "streamed",
        )
    )]
    pub async fn put_file_stream_with_options(
        &self,
        absolute_path: &str,
        body: ByteStream,
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        let prepared_content = self.prepare_content_stream_inner(body).await?;
        self.put_file_prepared_inner(absolute_path, prepared_content, actor, options)
            .await
    }

    /// Prepares content for publication.
    ///
    /// Content at or under the configured inline threshold needs no store
    /// request and has no expiry. Larger content writes an upload session and
    /// object. Its proof expires at the completed upload's receipt horizon.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.prepare_content",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "prepare_content",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn prepare_content(&self, bytes: &[u8]) -> Result<PreparedContent> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record("payload_class", crate::trace::payload_class(bytes.len()));
        self.prepare_content_inner(bytes).await
    }

    pub(super) async fn prepare_content_inner(&self, bytes: &[u8]) -> Result<PreparedContent> {
        if self
            .mode
            .bits
            .inline_content
            .inline_content_threshold_bytes
            .is_some_and(|threshold| bytes.len() <= threshold)
        {
            return Ok(PreparedContent::inline(
                self.namespace_id.clone(),
                bytes::Bytes::copy_from_slice(bytes),
            ));
        }
        let catalog = self
            .load_namespace_catalog_for_content_preparation()
            .await?;
        Ok(self
            .engine()
            .stage_owned_bytes(
                &catalog,
                self.core
                    .subject
                    .as_ref()
                    .map(|subject| &subject.subject_id),
                bytes,
            )
            .await?)
    }

    /// Prepares a stream, buffering at most the inline threshold plus one byte
    /// before choosing inline content or forwarding the stream to storage.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.prepare_content_stream",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "prepare_content_stream",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = "streamed",
        )
    )]
    pub async fn prepare_content_stream(&self, body: ByteStream) -> Result<PreparedContent> {
        self.core.record_trace_context(&tracing::Span::current());
        self.prepare_content_stream_inner(body).await
    }

    async fn prepare_content_stream_inner(&self, mut body: ByteStream) -> Result<PreparedContent> {
        if let Some(threshold) = self.mode.bits.inline_content.inline_content_threshold_bytes {
            let mut buffered = bytes::BytesMut::with_capacity(threshold + 1);
            while buffered.len() <= threshold {
                let Some(chunk) = body.next().await else {
                    return Ok(PreparedContent::inline(
                        self.namespace_id.clone(),
                        buffered.freeze(),
                    ));
                };
                let mut chunk = chunk.map_err(|error| crate::CoreError::Store {
                    object_key: "upload body".to_owned(),
                    message: error.public_message().into_owned(),
                    class: loonfs_objectstore::ObjectStoreErrorClass::of(&error),
                })?;
                let take = chunk.len().min(threshold + 1 - buffered.len());
                buffered.extend_from_slice(&chunk.split_to(take));
                if buffered.len() > threshold {
                    body = futures::stream::iter([Ok(buffered.freeze()), Ok(chunk)])
                        .chain(body)
                        .boxed();
                    break;
                }
            }
        }
        let catalog = self
            .load_namespace_catalog_for_content_preparation()
            .await?;
        Ok(self
            .engine()
            .stage_owned_stream(
                &catalog,
                self.core
                    .subject
                    .as_ref()
                    .map(|subject| &subject.subject_id),
                body,
            )
            .await?)
    }

    /// Publishes a file revision from already-prepared content, refusing to
    /// replace an existing file.
    pub async fn put_file_prepared(
        &self,
        absolute_path: &str,
        prepared_content: PreparedContent,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.put_file_prepared_with_options(
            absolute_path,
            prepared_content,
            actor,
            &PutFileOptions::default(),
        )
        .await
    }

    /// Publishes a file revision from already-prepared content.
    ///
    /// Inline content may stage before submission when a policy limit is reached.
    /// `options.behavior`
    /// selects create-only or replace semantics. Retry with a clone of the
    /// prepared content, the same explicit `options.commit.commit_id`, and
    /// unchanged options. A missing commit ID generates a new one on each call.
    /// Replay is bounded by the namespace's receipt retention horizon.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.put_file_prepared",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "put_file_prepared",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_file_prepared_with_options(
        &self,
        absolute_path: &str,
        prepared_content: PreparedContent,
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(prepared_content.content_ref().size_bytes).unwrap_or(usize::MAX),
            ),
        );
        self.put_file_prepared_inner(absolute_path, prepared_content, actor, options)
            .await
    }

    async fn put_file_prepared_inner(
        &self,
        absolute_path: &str,
        prepared_content: PreparedContent,
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        let operation = FilesystemOperation::PutFile {
            path: loonfs_core::path::parse_mutation_path(absolute_path)?,
            content_ref: Some(prepared_content.content_ref().clone()),
            inline_content: None,
            behavior: options.behavior,
            expected_inode_id: options.expected_inode_id,
            expected_revision_no: options.expected_revision_no,
        };
        self.commit_prepared_one(actor, &options.commit, operation, prepared_content)
            .await
    }

    /// Publishes a file revision by importing an already-durable content
    /// reference, refusing to replace an existing file.
    pub async fn put_file_content_ref(
        &self,
        absolute_path: &str,
        content_ref: ContentRef,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.put_file_content_ref_with_options(
            absolute_path,
            content_ref,
            actor,
            &PutFileOptions::default(),
        )
        .await
    }

    /// Publishes a file revision by importing an already-durable content reference.
    ///
    /// This explicitly slow helper reads and verifies the full source object,
    /// then stages a fresh copy owned by this namespace before publication.
    /// A subject must be an administrator of the reference's owner namespace.
    /// Callers that already hold same-namespace proof should prefer
    /// [`Self::put_file_prepared_with_options`]. The published revision points
    /// at the fresh prepared ref, not the input ref.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.put_file_content_ref",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "put_file_content_ref",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_file_content_ref_with_options(
        &self,
        absolute_path: &str,
        content_ref: ContentRef,
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
            ),
        );
        let prepared_content = self.prepare_content_ref_inner(content_ref).await?;
        self.put_file_prepared_inner(absolute_path, prepared_content, actor, options)
            .await
    }

    /// Imports an existing content reference for later publication.
    ///
    /// Preparation verifies the source bytes and stages them under a fresh
    /// identity owned by this namespace. Later publication performs no content
    /// I/O. A subject must be an administrator of the reference's owner namespace.
    /// See the API specification's content preparation contract.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.prepare_content_ref",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "prepare_content_ref",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn prepare_content_ref(&self, content_ref: ContentRef) -> Result<PreparedContent> {
        let span = tracing::Span::current();
        self.core.record_trace_context(&span);
        span.record(
            "payload_class",
            crate::trace::payload_class(
                usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
            ),
        );
        self.prepare_content_ref_inner(content_ref).await
    }

    async fn prepare_content_ref_inner(&self, content_ref: ContentRef) -> Result<PreparedContent> {
        let catalog = self
            .load_namespace_catalog_for_content_preparation()
            .await?;
        let engine = self.engine();
        let (owner, context) = self
            .core
            .pinned_metadata_read(&content_ref.owner_namespace_id)
            .await?;
        owner.require_administrator(&context).await?;
        // Forks pin manifests, so inherited content from a deleted owner is already an object.
        let owner_location = if context.head.status.is_deleted() {
            None
        } else {
            Some(
                owner
                    .resolve_content_location(&content_ref, &context)
                    .await?,
            )
        };
        match engine
            .import_content_ref(
                &catalog,
                self.core
                    .subject
                    .as_ref()
                    .map(|subject| &subject.subject_id),
                &content_ref,
                owner_location,
            )
            .await
        {
            Err(crate::CoreError::DurableContent(
                loonfs_core::content::DurableContentValidationError::MissingContentObject {
                    ..
                },
            )) if context.head.status.is_deleted() => Err(crate::CoreError::NamespaceDeleted {
                namespace_id: content_ref.owner_namespace_id,
            }
            .into()),
            result => Ok(result?),
        }
    }

    /// Verifies an authorized content token for this namespace.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.prepare_content_token",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "prepare_content_token",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn prepare_content_token(
        &self,
        secret: &str,
        token: &crate::content_tokens::ContentToken,
        now_ms: u64,
    ) -> Result<std::result::Result<PreparedContent, loonfs_core::content::ContentTokenError>> {
        self.core.record_trace_context(&tracing::Span::current());
        let catalog = self
            .load_namespace_catalog_for_content_preparation()
            .await?;
        Ok(loonfs_core::content::verify_content_token(
            secret, &catalog, token, now_ms,
        ))
    }

    pub(crate) async fn load_namespace_catalog_for_content_preparation(
        &self,
    ) -> Result<loonfs_core::control::VerifiedNamespaceCatalogEntry> {
        self.core
            .load_namespace_catalog_cached(&self.namespace_id)
            .await
    }

    /// Creates a directory at an absolute path, failing when its parent is missing.
    pub async fn create_directory(&self, absolute_path: &str, actor: &ActorId) -> Result<Commit> {
        self.create_directory_with_options(absolute_path, actor, &CreateDirectoryOptions::default())
            .await
    }

    /// Creates a directory at an absolute path.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.create_directory",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "create_directory",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_directory_with_options(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        options: &CreateDirectoryOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_one(
            actor,
            &options.commit,
            FilesystemOperation::CreateDirectory {
                path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                parents: options.parents,
            },
        )
        .await
    }

    /// Deletes a file or empty directory path.
    pub async fn delete_path(&self, absolute_path: &str, actor: &ActorId) -> Result<Commit> {
        self.delete_path_with_options(absolute_path, actor, &DeleteOptions::default())
            .await
    }

    /// Deletes a file or directory path.
    ///
    /// Deletion is tombstone-first: the commit hides the path without erasing
    /// history. Physical reclamation is explicit garbage collection: nothing
    /// sweeps unless an operator asks, through `Maintenance::gc` or a
    /// maintenance pass that opted in.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.delete_path",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "delete_path",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn delete_path_with_options(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        options: &DeleteOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_one(
            actor,
            &options.commit,
            FilesystemOperation::DeletePath {
                path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                behavior: options.behavior,
                expected_inode_id: options.expected_inode_id,
            },
        )
        .await
    }

    /// Moves a path within the same namespace, refusing to replace the
    /// destination.
    pub async fn move_path(
        &self,
        source_path: &str,
        destination_path: &str,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.move_path_with_options(
            source_path,
            destination_path,
            actor,
            &MoveOptions::default(),
        )
        .await
    }

    /// Moves a path within the same namespace.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.move_path",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "move_path",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn move_path_with_options(
        &self,
        source_path: &str,
        destination_path: &str,
        actor: &ActorId,
        options: &MoveOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_one(
            actor,
            &options.commit,
            FilesystemOperation::MovePath {
                source_path: loonfs_core::path::parse_mutation_path(source_path)?,
                destination_path: loonfs_core::path::parse_mutation_path(destination_path)?,
                precondition: loonfs_types::DestinationPrecondition {
                    behavior: options.behavior,
                    expected_inode_id: options.expected_destination_inode_id,
                    expected_revision_no: options.expected_destination_revision_no,
                },
            },
        )
        .await
    }

    /// Copies a file to a new path in the same namespace, refusing to replace
    /// the destination.
    pub async fn copy_path(
        &self,
        source_path: &str,
        destination_path: &str,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.copy_path_with_options(
            source_path,
            destination_path,
            actor,
            &CopyOptions::default(),
        )
        .await
    }

    /// Copies a file to a new path in the same namespace. The new file
    /// reuses the source revision's content reference: no bytes are copied.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.copy_path",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "copy_path",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn copy_path_with_options(
        &self,
        source_path: &str,
        destination_path: &str,
        actor: &ActorId,
        options: &CopyOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_one(
            actor,
            &options.commit,
            FilesystemOperation::CopyPath {
                source_path: loonfs_core::path::parse_mutation_path(source_path)?,
                destination_path: loonfs_core::path::parse_mutation_path(destination_path)?,
                precondition: loonfs_types::DestinationPrecondition {
                    behavior: options.behavior,
                    expected_inode_id: options.expected_destination_inode_id,
                    expected_revision_no: options.expected_destination_revision_no,
                },
            },
        )
        .await
    }

    /// Restores a prior file revision by appending a new current revision.
    pub async fn restore_revision(
        &self,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.restore_revision_with_options(
            absolute_path,
            source_revision_no,
            actor,
            &CommitOptions::default(),
        )
        .await
    }

    /// Restores a prior file revision by appending a new current revision,
    /// under the given commit settings.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.restore_revision",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "restore_revision",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn restore_revision_with_options(
        &self,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_one(
            actor,
            options,
            FilesystemOperation::RestoreRevision {
                path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                source_revision_no,
            },
        )
        .await
    }

    /// Writes and removes attributes on the inode a path resolves to.
    pub async fn update_attributes(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        changes: AttributeChanges,
    ) -> Result<Commit> {
        self.update_attributes_with_options(
            absolute_path,
            actor,
            changes,
            &UpdateAttributesOptions::default(),
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
        name = "loonfs.update_attributes",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "update_attributes",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn update_attributes_with_options(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        changes: AttributeChanges,
        options: &UpdateAttributesOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_one(
            actor,
            &options.commit,
            FilesystemOperation::UpdateAttributes {
                path: loonfs_core::path::parse_mutation_path(absolute_path)?,
                set: changes.set,
                remove: changes.remove,
                expected_inode_id: options.expected_inode_id,
                expected_attributes_revision_no: options.expected_attributes_revision_no,
            },
        )
        .await
    }

    /// Replaces a visible inode's access row, including the root.
    pub async fn update_access(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        access: AccessState,
    ) -> Result<Commit> {
        self.update_access_with_options(
            absolute_path,
            actor,
            access,
            &UpdateAccessOptions::default(),
        )
        .await
    }

    /// Replaces a visible inode's access row, including the root, under optional inode and revision preconditions.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.update_access",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "update_access",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn update_access_with_options(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        access: AccessState,
        options: &UpdateAccessOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_one(
            actor,
            &options.commit,
            FilesystemOperation::UpdateAccess {
                path: loonfs_types::AbsolutePath::parse(absolute_path)
                    .map_err(|error| loonfs_core::Error::InvalidPath(error.to_string()))?,
                boundary: access.boundary,
                grants: access.grants,
                expected_inode_id: options.expected_inode_id,
                expected_access_revision_no: options.expected_access_revision_no,
            },
        )
        .await
    }

    /// Restores a deleted file or subtree under the parent and name its
    /// deletion recorded.
    pub async fn undelete(
        &self,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.undelete_with_options(inode_id, deletion_seq, actor, &UndeleteOptions::default())
            .await
    }

    /// Restores a deleted file or subtree where `options` says.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.undelete",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "undelete",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn undelete_with_options(
        &self,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        actor: &ActorId,
        options: &UndeleteOptions,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_one(
            actor,
            &options.commit,
            FilesystemOperation::undelete(inode_id, deletion_seq, &options.destination),
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
        name = "loonfs.commit",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "commit",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn commit(&self, request: CommitRequest) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(CommitCandidate::new(request))
            .await
    }

    /// Applies one commit request with prepared content proofs.
    ///
    /// Inline content may stage before submission when a policy limit is reached.
    /// One prepared value covers every operation that uses its content ref.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.commit_prepared",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "commit_prepared",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn commit_prepared(
        &self,
        request: CommitRequest,
        prepared_content: Vec<PreparedContent>,
    ) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(CommitCandidate::prepared(request, prepared_content))
            .await
    }

    /// Publishes one candidate through the runtime's publication service:
    /// batching is adaptive, every submitter receives its own durable result,
    /// and admitted work is owned by the service's worker — a cancelled
    /// caller abandons only its result delivery, never the publication.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.commit_candidate",
        err(level = "debug"),
        skip_all,
        fields(
            operation = "commit_candidate",
            namespace_id = %self.namespace_id,
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn commit_candidate(&self, candidate: CommitCandidate) -> Result<Commit> {
        self.core.record_trace_context(&tracing::Span::current());
        self.commit_candidate_inner(candidate).await
    }

    pub(crate) async fn commit_candidate_inner(
        &self,
        candidate: CommitCandidate,
    ) -> Result<Commit> {
        let candidate = match &self.core.subject {
            Some(subject) => candidate.with_subject(subject.clone()),
            None => candidate,
        };
        self.session().submit_candidate(candidate).await
    }

    pub(super) async fn commit_one(
        &self,
        actor: &ActorId,
        commit: &CommitOptions,
        operation: FilesystemOperation,
    ) -> Result<Commit> {
        self.commit_candidate_inner(CommitCandidate::new(single_operation(
            actor, commit, operation,
        )))
        .await
    }

    pub(super) async fn commit_prepared_one(
        &self,
        actor: &ActorId,
        commit: &CommitOptions,
        operation: FilesystemOperation,
        prepared_content: PreparedContent,
    ) -> Result<Commit> {
        self.commit_candidate_inner(CommitCandidate::prepared(
            single_operation(actor, commit, operation),
            vec![prepared_content],
        ))
        .await
    }
}

pub(crate) struct EnginePublishResult {
    pub(crate) results: Vec<std::result::Result<Commit, crate::CoreError>>,
    pub(crate) wal_tail_objects: u64,
    pub(crate) wal_tail_inline_bytes: usize,
    pub(crate) wal_tail_observed: bool,
    pub(crate) wal_tail_discovered: bool,
}

/// Publishes already-classified candidates as one batch — one WAL
/// object, one numbered WAL put — through the namespace
/// publisher's own commit engine, and seeds the read cache with the
/// state the batch produced.
///
/// Only the publication service calls this: it owns the engine, and
/// borrowing it here keeps engine construction and locking in that one
/// place. Results match candidates in order.
pub(crate) async fn publish_batch_with_engine(
    core: &RuntimeCore,
    namespace_id: &NamespaceId,
    engine: &mut loonfs_core::publish::NamespaceCommitEngine,
    candidates: &[CommitCandidate],
    context: &loonfs_core::MutationContext,
    batch: &loonfs_core::time::Deadline,
) -> EnginePublishResult {
    let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    let store = core.store();
    // Boxing erases the engine's deeply nested publish future; without
    // it, callers awaiting a put or commit (CLI, server, embedding
    // crates) exceed rustc's type-recursion depth.
    let mut publish = Box::pin(engine.publish_batch(&store, candidates, context, batch)).await;
    {
        let _span = phase_span!(core, "batch_update_cache", namespace_id, batch_size).entered();
        if let Some(state) = publish.resulting_read_state.take() {
            core.seed_namespace_read_cache(namespace_id, state);
        }
    }
    EnginePublishResult {
        results: publish.results,
        wal_tail_objects: publish.wal_tail_objects,
        wal_tail_inline_bytes: publish.wal_tail_inline_bytes,
        wal_tail_observed: publish.wal_tail_observed,
        wal_tail_discovered: publish.wal_tail_discovered,
    }
}
