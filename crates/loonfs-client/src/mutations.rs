//! Commit submission and path-oriented filesystem mutations.

use crate::operations;
use crate::uploads::staging::{PreparedContent, PreparedContentKind, UploadContinuity};
use crate::{
    Client, ClientError, MultipartUploadResume, NamespacePath, PayloadSource, PutFileJournal,
    Result,
};
use loonfs_types::options::{
    AccessState, AppendFileOptions, AttributeChanges, CommitOptions, CopyOptions,
    CreateDirectoryOptions, DeleteOptions, MoveOptions, PutFileOptions, RestoreDeletedOptions,
    UpdateAccessOptions, UpdateAttributesOptions,
};
use loonfs_types::{
    headers, CommitRequest, CommitSeq, ContentRef, FileContent, FilesystemOperation, InodeId,
    NamespaceId, RevisionNo,
};
use loonfs_types::{Commit, ContentToken};

impl Client {
    /// Applies a commit atomically, preserving operation order.
    ///
    /// The convenience methods below are the one-element case of this call.
    /// Operations that introduce new external content include their proofs in
    /// the request's `content_tokens`; stage the bytes with the upload
    /// methods first.
    pub async fn commit(
        &self,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
    ) -> Result<Commit> {
        let url = self.namespace_url(namespace_id, "/commits");
        // The request's commit id resolves an ambiguous resend through a durable receipt.
        self.send_json(
            self.request(operations::CREATE_COMMIT, &url)
                .header(headers::ACTOR, self.mutation_actor()?.as_str())
                .json(request)?,
        )
        .await
    }

    /// Applies a commit of one operation.
    pub(crate) async fn commit_one(
        &self,
        namespace_id: &NamespaceId,
        commit: &CommitOptions,
        operation: FilesystemOperation,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            &CommitRequest::with_operations(commit, vec![operation]),
        )
        .await
    }

    /// Writes file bytes to a path, refusing to replace an existing file.
    pub async fn put_file(&self, spec: &NamespacePath, bytes: &[u8]) -> Result<Commit> {
        self.put_file_with_options(spec, bytes, &PutFileOptions::default())
            .await
    }

    /// Writes file bytes to a path.
    ///
    /// Content at or under the advertised inline limit is identified by its bytes.
    /// A rerun with the same bytes and commit ID replays. Larger content uploads a
    /// new object, so a rerun conflicts.
    /// At every size, retain [`Self::prepare_content`]'s result and retry with
    /// [`Self::put_file_prepared_with_options`], the same commit ID, and
    /// unchanged options.
    pub async fn put_file_with_options(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<Commit> {
        let prepared = self.prepare_content(spec.namespace(), bytes).await?;
        self.put_file_prepared_with_options(spec, prepared, options)
            .await
    }

    /// Writes a file from a payload read once from its source, refusing to
    /// replace an existing file.
    pub async fn put_file_stream(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
    ) -> Result<Commit> {
        self.put_file_stream_with_options(spec, source, &PutFileOptions::default())
            .await
    }

    /// Writes a file from a payload read once from its source.
    ///
    /// Buffers up to the advertised inline limit plus one byte to choose the
    /// content kind, then forwards larger payloads in bounded chunks.
    ///
    /// Direct multipart uploads support at most 10,000 parts. Payloads larger
    /// than `part_size_bytes × 10_000` require a larger configured part size.
    ///
    /// Inline content is identified by its bytes, so a rerun with the same bytes
    /// and commit ID replays. Larger content uploads, so a rerun conflicts.
    /// At every size, retain [`Self::prepare_content_stream`]'s result and retry with
    /// [`Self::put_file_prepared_with_options`], the same commit ID, and
    /// unchanged options.
    pub async fn put_file_stream_with_options(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        self.put_file_stream_continuing(spec, source, options, UploadContinuity::default())
            .await
    }

    /// Writes a stream with optional multipart resume state.
    ///
    /// The journal records the complete commit request before submission for
    /// every transport. Multipart uploads also record session geometry and parts.
    /// Save the request and actor, and after an interruption replay the request
    /// with [`Self::commit`] on a client with that actor; the request includes the
    /// inline bytes or uploaded reference and commit ID.
    ///
    /// A resumed multipart attempt still receives the source from the
    /// beginning because the final checksum covers the complete object.
    pub async fn put_file_stream_resumable(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
        journal: &dyn PutFileJournal,
        resume: Option<&MultipartUploadResume>,
    ) -> Result<Commit> {
        self.put_file_stream_continuing(
            spec,
            source,
            options,
            UploadContinuity {
                resume,
                journal: Some(journal),
            },
        )
        .await
    }

    async fn put_file_stream_continuing(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
        continuity: UploadContinuity<'_>,
    ) -> Result<Commit> {
        let prepared_content = self
            .prepare_file_source(spec.namespace(), source, continuity)
            .await?;
        self.commit_prepared_file(spec, prepared_content, options, continuity.journal)
            .await
    }

    /// Prepares content for publication.
    ///
    /// Content at or under the advertised inline limit needs no upload and has
    /// no expiry. Larger content uploads; its proof expires at the upload's
    /// receipt horizon. Retain this value and an explicit commit ID to retry
    /// [`Self::put_file_prepared_with_options`] at every size.
    pub async fn prepare_content(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        // Prepared content is committed by this client, so a missing actor
        // fails here, before any request.
        self.mutation_actor()?;
        if self
            .inline_content_limit()
            .await?
            .is_some_and(|limit| bytes.len() <= limit)
        {
            return Ok(PreparedContent {
                kind: PreparedContentKind::Inline(bytes.to_vec()),
            });
        }
        self.stage_bytes_as_content_ref(namespace_id, bytes).await
    }

    /// Prepares a stream, buffering up to the advertised inline limit plus one byte.
    ///
    /// The result has the same retry contract as [`Self::prepare_content`].
    pub async fn prepare_content_stream(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
    ) -> Result<PreparedContent> {
        self.prepare_file_source(namespace_id, source, UploadContinuity::default())
            .await
    }

    /// Publishes already-prepared content, refusing to replace an existing
    /// file.
    pub async fn put_file_prepared(
        &self,
        spec: &NamespacePath,
        prepared_content: PreparedContent,
    ) -> Result<Commit> {
        self.put_file_prepared_with_options(spec, prepared_content, &PutFileOptions::default())
            .await
    }

    /// Publishes already-prepared content without uploading it again.
    ///
    /// Retry with a clone of the prepared content, the same explicit
    /// `options.commit.commit_id`, and unchanged options. A missing commit ID
    /// generates a new one on each call. Replay is bounded by server receipt
    /// retention. For process recovery, use [`Self::put_file_stream_resumable`]
    /// to save the full request before submission.
    pub async fn put_file_prepared_with_options(
        &self,
        spec: &NamespacePath,
        prepared_content: PreparedContent,
        options: &PutFileOptions,
    ) -> Result<Commit> {
        self.commit_prepared_file(spec, prepared_content, options, None)
            .await
    }

    /// Commits content after an upload completed but its file commit did not.
    ///
    /// Call [`Self::get_upload`] to recover the content reference and token.
    /// This method then commits the existing content without uploading it
    /// again. An optional journal saves the complete request before submission.
    pub async fn commit_completed_upload(
        &self,
        spec: &NamespacePath,
        content_ref: ContentRef,
        content_token: Option<ContentToken>,
        options: &PutFileOptions,
        journal: Option<&dyn PutFileJournal>,
    ) -> Result<Commit> {
        let staged = PreparedContent {
            kind: PreparedContentKind::Staged {
                upload_id: None,
                content_ref,
                content_token,
            },
        };
        self.commit_prepared_file(spec, staged, options, journal)
            .await
    }

    async fn commit_prepared_file(
        &self,
        spec: &NamespacePath,
        prepared_content: PreparedContent,
        options: &PutFileOptions,
        journal: Option<&dyn PutFileJournal>,
    ) -> Result<Commit> {
        self.commit_prepared_operation(
            spec.namespace(),
            prepared_content,
            &options.commit,
            journal,
            |content| FilesystemOperation::put_file(spec.absolute_path().clone(), content, options),
        )
        .await
    }

    /// Commits one operation that writes prepared content. Saves the exact
    /// request when journaled, then submits it unchanged.
    pub(crate) async fn commit_prepared_operation(
        &self,
        namespace_id: &NamespaceId,
        prepared_content: PreparedContent,
        commit: &CommitOptions,
        journal: Option<&dyn PutFileJournal>,
        operation: impl FnOnce(FileContent) -> FilesystemOperation,
    ) -> Result<Commit> {
        let (content, content_token, upload_id) = match prepared_content.kind {
            PreparedContentKind::Inline(bytes) => (FileContent::Inline(bytes), None, None),
            PreparedContentKind::Staged {
                upload_id,
                content_ref,
                content_token,
            } => (FileContent::Ref(content_ref), content_token, upload_id),
        };
        let mut request = CommitRequest::with_operations(commit, vec![operation(content)]);
        request.content_tokens = content_token.into_iter().collect();
        if let Some(journal) = journal {
            journal
                .commit_prepared(&request, self.mutation_actor()?, upload_id.as_ref())
                .map_err(|error| {
                    ClientError::Io(format!(
                        "could not record file commit `{}` before submission: {error}",
                        request.commit_id
                    ))
                })?;
        }
        self.commit(namespace_id, &request).await
    }

    /// Adds bytes to the end of a file as its next revision.
    pub async fn append_file(&self, spec: &NamespacePath, bytes: &[u8]) -> Result<Commit> {
        self.append_file_with_options(spec, bytes, &AppendFileOptions::default())
            .await
    }

    /// Adds bytes to the end of a file as its next revision.
    ///
    /// The commit includes the bytes, from 1 byte to 256 KiB. A rerun with the
    /// same bytes and commit ID replays.
    pub async fn append_file_with_options(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &AppendFileOptions,
    ) -> Result<Commit> {
        self.commit_one(
            spec.namespace(),
            &options.commit,
            FilesystemOperation::append_file(spec.absolute_path().clone(), bytes.to_vec(), options),
        )
        .await
    }

    /// Creates a directory at the requested path, failing when its parent is
    /// missing.
    pub async fn create_directory(&self, spec: &NamespacePath) -> Result<Commit> {
        self.create_directory_with_options(spec, &CreateDirectoryOptions::default())
            .await
    }

    /// Creates a directory at the requested path.
    pub async fn create_directory_with_options(
        &self,
        spec: &NamespacePath,
        options: &CreateDirectoryOptions,
    ) -> Result<Commit> {
        self.commit_one(
            spec.namespace(),
            &options.commit,
            FilesystemOperation::create_directory(spec.absolute_path().clone(), options),
        )
        .await
    }

    /// Deletes the requested file or empty directory.
    pub async fn delete_path(&self, spec: &NamespacePath) -> Result<Commit> {
        self.delete_path_with_options(spec, &DeleteOptions::default())
            .await
    }

    /// Deletes the requested path.
    pub async fn delete_path_with_options(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<Commit> {
        self.commit_one(
            spec.namespace(),
            &options.commit,
            FilesystemOperation::delete_path(spec.absolute_path().clone(), options),
        )
        .await
    }

    /// Writes and removes attributes on the inode a path resolves to.
    pub async fn update_attributes(
        &self,
        spec: &NamespacePath,
        changes: AttributeChanges,
    ) -> Result<Commit> {
        self.update_attributes_with_options(spec, changes, &UpdateAttributesOptions::default())
            .await
    }

    /// Writes and removes attributes on the inode a path resolves to.
    ///
    /// The target may be a file or a directory.
    pub async fn update_attributes_with_options(
        &self,
        spec: &NamespacePath,
        changes: AttributeChanges,
        options: &UpdateAttributesOptions,
    ) -> Result<Commit> {
        self.commit_one(
            spec.namespace(),
            &options.commit,
            FilesystemOperation::update_attributes(spec.absolute_path().clone(), changes, options),
        )
        .await
    }

    /// Replaces a visible inode's access row, including the root.
    pub async fn update_access(&self, spec: &NamespacePath, access: AccessState) -> Result<Commit> {
        self.update_access_with_options(spec, access, &UpdateAccessOptions::default())
            .await
    }

    /// Replaces a visible inode's access row, including the root, under optional inode and revision preconditions.
    pub async fn update_access_with_options(
        &self,
        spec: &NamespacePath,
        access: AccessState,
        options: &UpdateAccessOptions,
    ) -> Result<Commit> {
        self.commit_one(
            spec.namespace(),
            &options.commit,
            FilesystemOperation::update_access(spec.absolute_path().clone(), access, options),
        )
        .await
    }

    /// Moves a path within one namespace, refusing to replace the destination.
    pub async fn move_path(
        &self,
        source_path: &NamespacePath,
        destination_path: &NamespacePath,
    ) -> Result<Commit> {
        self.move_path_with_options(source_path, destination_path, &MoveOptions::default())
            .await
    }

    /// Moves a path within one namespace.
    pub async fn move_path_with_options(
        &self,
        source_path: &NamespacePath,
        destination_path: &NamespacePath,
        options: &MoveOptions,
    ) -> Result<Commit> {
        if source_path.namespace() != destination_path.namespace() {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot move across namespaces: {} -> {}",
                source_path.namespace(),
                destination_path.namespace()
            )));
        }
        self.commit_one(
            source_path.namespace(),
            &options.commit,
            FilesystemOperation::move_path(
                source_path.absolute_path().clone(),
                destination_path.absolute_path().clone(),
                options,
            ),
        )
        .await
    }

    /// Copies a path within one namespace, refusing to replace the destination.
    pub async fn copy_path(
        &self,
        source_path: &NamespacePath,
        destination_path: &NamespacePath,
    ) -> Result<Commit> {
        self.copy_path_with_options(source_path, destination_path, &CopyOptions::default())
            .await
    }

    /// Copies a path within one namespace.
    pub async fn copy_path_with_options(
        &self,
        source_path: &NamespacePath,
        destination_path: &NamespacePath,
        options: &CopyOptions,
    ) -> Result<Commit> {
        if source_path.namespace() != destination_path.namespace() {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot copy across namespaces: {} -> {}",
                source_path.namespace(),
                destination_path.namespace()
            )));
        }
        self.commit_one(
            source_path.namespace(),
            &options.commit,
            FilesystemOperation::copy_path(
                source_path.absolute_path().clone(),
                destination_path.absolute_path().clone(),
                options,
            ),
        )
        .await
    }

    /// Restores a deleted file or subtree under the parent and name its
    /// deletion recorded.
    pub async fn restore_deleted(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deletion_seq: CommitSeq,
    ) -> Result<Commit> {
        self.restore_deleted_with_options(
            namespace_id,
            inode_id,
            deletion_seq,
            &RestoreDeletedOptions::default(),
        )
        .await
    }

    /// Restores a deleted file or subtree where `options` says.
    pub async fn restore_deleted_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deletion_seq: CommitSeq,
        options: &RestoreDeletedOptions,
    ) -> Result<Commit> {
        self.commit_one(
            namespace_id,
            &options.commit,
            FilesystemOperation::restore_deleted(inode_id, deletion_seq, &options.destination),
        )
        .await
    }

    /// Makes an earlier file revision the current revision.
    pub async fn restore_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
    ) -> Result<Commit> {
        self.restore_revision_with_options(spec, source_revision_no, &CommitOptions::default())
            .await
    }

    /// Makes an earlier file revision the current revision, under the given
    /// commit settings.
    pub async fn restore_revision_with_options(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &CommitOptions,
    ) -> Result<Commit> {
        self.commit_one(
            spec.namespace(),
            options,
            FilesystemOperation::restore_revision(spec.absolute_path().clone(), source_revision_no),
        )
        .await
    }
}
