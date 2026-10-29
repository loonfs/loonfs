//! The writable [`Namespace`] handle's path mutations and commits, and the
//! publication pipeline they go through.

use crate::publish::{CommitCandidate, CommitIntent, FilesystemOperation, PreparedContent};
use crate::trace::{self, operation_span};
use crate::ByteStream;
use crate::Result;
use crate::{
    AccessState, ActorId, AppendFileOptions, AttributeChanges, ChangeSeq, Commit, CommitOptions,
    ContentRef, CopyOptions, CreateDirectoryOptions, DeleteOptions, InodeId, MoveOptions,
    PutFileOptions, RevisionNo, UndeleteOptions, UpdateAccessOptions, UpdateAttributesOptions,
};
use crate::{Namespace, Writable};

use loonfs_types::u64_from_usize;
use loonfs_types::FileContent;

fn single_operation(
    actor: &ActorId,
    commit: &CommitOptions,
    operation: FilesystemOperation,
) -> CommitIntent {
    CommitIntent::single(
        commit.commit_id_or_generate(),
        actor.clone(),
        commit.message.clone(),
        operation,
    )
    .preconditions(commit.preconditions.clone())
}

impl Namespace<Writable> {
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
    pub async fn put_file_with_options(
        &self,
        absolute_path: &str,
        bytes: &[u8],
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_file",
                self.namespace_id,
                payload_class = trace::payload_class(u64_from_usize(bytes.len()))
            ),
            async {
                let prepared_content = self.prepare_content_inner(bytes).await?;
                self.put_file_prepared_inner(absolute_path, prepared_content, actor, options)
                    .await
            },
        )
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
    pub async fn put_file_stream_with_options(
        &self,
        absolute_path: &str,
        body: ByteStream,
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_file_stream",
                self.namespace_id,
                payload_class = "streamed"
            ),
            async {
                let prepared_content = self.prepare_content_stream_inner(body).await?;
                self.put_file_prepared_inner(absolute_path, prepared_content, actor, options)
                    .await
            },
        )
        .await
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
    pub async fn put_file_prepared_with_options(
        &self,
        absolute_path: &str,
        prepared_content: PreparedContent,
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_file_prepared",
                self.namespace_id,
                payload_class = trace::payload_class(prepared_content.content_ref().size_bytes)
            ),
            self.put_file_prepared_inner(absolute_path, prepared_content, actor, options),
        )
        .await
    }

    async fn put_file_prepared_inner(
        &self,
        absolute_path: &str,
        prepared_content: PreparedContent,
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        let operation = FilesystemOperation::put_file(
            loonfs_core::path::parse_mutation_path(absolute_path)?,
            FileContent::Ref(prepared_content.content_ref().clone()),
            options,
        );
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
    /// Callers that already have same-namespace proof should prefer
    /// [`Self::put_file_prepared_with_options`]. The published revision points
    /// at the fresh prepared ref, not the input ref.
    pub async fn put_file_content_ref_with_options(
        &self,
        absolute_path: &str,
        content_ref: ContentRef,
        actor: &ActorId,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_file_content_ref",
                self.namespace_id,
                payload_class = trace::payload_class(content_ref.size_bytes)
            ),
            async {
                let prepared_content = self.prepare_content_ref_inner(content_ref).await?;
                self.put_file_prepared_inner(absolute_path, prepared_content, actor, options)
                    .await
            },
        )
        .await
    }

    /// Adds bytes to the end of a file as its next revision.
    pub async fn append_file(
        &self,
        absolute_path: &str,
        bytes: &[u8],
        actor: &ActorId,
    ) -> Result<Commit> {
        self.append_file_with_options(absolute_path, bytes, actor, &AppendFileOptions::default())
            .await
    }

    /// Adds bytes to the end of a file as its next revision.
    ///
    /// The path must name a visible file. The commit includes the bytes, from
    /// 1 byte to 256 KiB. Without a guard, concurrent appends apply in commit
    /// order, each to the result of the one before. A rerun with the same
    /// bytes and commit ID replays.
    pub async fn append_file_with_options(
        &self,
        absolute_path: &str,
        bytes: &[u8],
        actor: &ActorId,
        options: &AppendFileOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(
                self.core,
                "append_file",
                self.namespace_id,
                payload_class = trace::payload_class(u64_from_usize(bytes.len()))
            ),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::append_file(
                    loonfs_core::path::parse_mutation_path(absolute_path)?,
                    bytes.to_vec(),
                    options,
                ),
            ),
        )
        .await
    }

    /// Creates a directory at an absolute path, failing when its parent is missing.
    pub async fn create_directory(&self, absolute_path: &str, actor: &ActorId) -> Result<Commit> {
        self.create_directory_with_options(absolute_path, actor, &CreateDirectoryOptions::default())
            .await
    }

    /// Creates a directory at an absolute path.
    pub async fn create_directory_with_options(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        options: &CreateDirectoryOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "create_directory", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::create_directory(
                    loonfs_core::path::parse_mutation_path(absolute_path)?,
                    options,
                ),
            ),
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
    pub async fn delete_path_with_options(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        options: &DeleteOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "delete_path", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::delete_path(
                    loonfs_core::path::parse_mutation_path(absolute_path)?,
                    options,
                ),
            ),
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
    pub async fn move_path_with_options(
        &self,
        source_path: &str,
        destination_path: &str,
        actor: &ActorId,
        options: &MoveOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "move_path", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::move_path(
                    loonfs_core::path::parse_mutation_path(source_path)?,
                    loonfs_core::path::parse_mutation_path(destination_path)?,
                    options,
                ),
            ),
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
    pub async fn copy_path_with_options(
        &self,
        source_path: &str,
        destination_path: &str,
        actor: &ActorId,
        options: &CopyOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "copy_path", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::copy_path(
                    loonfs_core::path::parse_mutation_path(source_path)?,
                    loonfs_core::path::parse_mutation_path(destination_path)?,
                    options,
                ),
            ),
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
    pub async fn restore_revision_with_options(
        &self,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "restore_revision", self.namespace_id),
            self.commit_one(
                actor,
                options,
                FilesystemOperation::restore_revision(
                    loonfs_core::path::parse_mutation_path(absolute_path)?,
                    source_revision_no,
                ),
            ),
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
    pub async fn update_attributes_with_options(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        changes: AttributeChanges,
        options: &UpdateAttributesOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "update_attributes", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::update_attributes(
                    loonfs_core::path::parse_mutation_path(absolute_path)?,
                    changes,
                    options,
                ),
            ),
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
    pub async fn update_access_with_options(
        &self,
        absolute_path: &str,
        actor: &ActorId,
        access: AccessState,
        options: &UpdateAccessOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "update_access", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::update_access(
                    loonfs_types::AbsolutePath::parse(absolute_path)
                        .map_err(loonfs_core::CoreError::from)?,
                    access,
                    options,
                ),
            ),
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
    pub async fn undelete_with_options(
        &self,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        actor: &ActorId,
        options: &UndeleteOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "undelete", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::undelete(inode_id, deletion_seq, &options.destination),
            ),
        )
        .await
    }

    /// Applies one commit request: its operations are committed together, in
    /// order, under one commit id.
    ///
    /// Each operation resolves against the namespace plus everything the
    /// operations ahead of it do, so a request can create a directory and
    /// write into it. Nothing commits unless every operation does, and the
    /// error of a request that stops names the operation that stopped it.
    /// Operations that introduce new external content require
    /// [`Self::commit_prepared`].
    pub async fn commit(&self, request: CommitIntent) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "commit", self.namespace_id),
            self.commit_candidate_inner(CommitCandidate::new(request)),
        )
        .await
    }

    /// Applies one commit request with prepared content proofs.
    ///
    /// Inline content may stage before submission when a policy limit is reached.
    /// One prepared value covers every operation that uses its content ref.
    pub async fn commit_prepared(
        &self,
        request: CommitIntent,
        prepared_content: Vec<PreparedContent>,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "commit_prepared", self.namespace_id),
            self.commit_candidate_inner(CommitCandidate::prepared(request, prepared_content)),
        )
        .await
    }

    /// Publishes one candidate through the runtime's publication service:
    /// batching is adaptive, every submitter receives its own durable result,
    /// and admitted work is owned by the service's worker — a cancelled
    /// caller abandons only its result delivery, never the publication.
    pub async fn commit_candidate(&self, candidate: CommitCandidate) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "commit_candidate", self.namespace_id),
            self.commit_candidate_inner(candidate),
        )
        .await
    }

    pub(crate) async fn commit_candidate_inner(
        &self,
        candidate: CommitCandidate,
    ) -> Result<Commit> {
        let candidate = match self.core.subject() {
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
