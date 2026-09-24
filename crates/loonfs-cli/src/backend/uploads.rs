//! Embedded upload preparation and replay of journaled requests.

use super::EmbeddedBackend;
use crate::backend_error::{map_namespace_scoped_runtime_error, NamespaceScoped};
use crate::error::CliError;
use crate::uploads::UploadJournal;
use loonfs::publish::{CommitRequest as RuntimeCommitRequest, PreparedContent};
use loonfs::uploads::ResolvedUploadCompletion;
use loonfs::{ByteStream, PutFileOptions};
use loonfs_api::{
    ActorId, Commit, CommitRequest, ErrorCode, FilesystemOperation, NamespaceId, UploadId,
};
use loonfs_client::NamespacePath;

impl EmbeddedBackend {
    pub(super) async fn put_file_stream_journaled(
        &self,
        spec: &NamespacePath,
        body: ByteStream,
        options: &PutFileOptions,
        journal: Option<&UploadJournal>,
    ) -> Result<Commit, CliError> {
        let Some(journal) = journal else {
            return self.put_file_stream(spec, body, options).await;
        };
        let prepared = self
            .writer
            .prepare_file_stream(spec.namespace(), body)
            .await
            .scoped(spec.namespace())?;
        self.publish_prepared_upload(spec, prepared, options, journal)
            .await
    }

    pub(super) async fn commit_completed_upload(
        &self,
        spec: &NamespacePath,
        upload_id: &UploadId,
        options: &PutFileOptions,
        journal: &UploadJournal,
    ) -> Result<Commit, CliError> {
        let completed = self
            .writer
            .complete_upload(
                spec.namespace(),
                upload_id,
                None,
                ResolvedUploadCompletion::KnownContent,
            )
            .await
            .scoped(spec.namespace())?;
        self.publish_prepared_upload(spec, completed.prepared, options, journal)
            .await
    }

    async fn publish_prepared_upload(
        &self,
        spec: &NamespacePath,
        prepared: PreparedContent,
        options: &PutFileOptions,
        journal: &UploadJournal,
    ) -> Result<Commit, CliError> {
        let inline_content = prepared
            .inline_content()
            .map(|content| content.bytes().to_vec());
        let request = CommitRequest::single(
            options
                .commit
                .commit_id
                .clone()
                .expect("journal should assign a commit id"),
            options.commit.message.clone(),
            FilesystemOperation::PutFile {
                path: spec.absolute_path().clone(),
                content_ref: inline_content
                    .is_none()
                    .then(|| prepared.content_ref().clone()),
                inline_content,
                behavior: options.behavior,
                expected_inode_id: options.expected_inode_id,
                expected_revision_no: options.expected_revision_no,
            },
        )
        .preconditions(options.commit.preconditions.clone());
        journal
            .record_prepared(&request, &options.commit.actor_id, prepared.upload_id())
            .map_err(CliError::io)?;
        let result = self
            .writer
            .put_file_prepared(
                spec.namespace(),
                spec.absolute_path().as_str(),
                prepared,
                options.clone(),
            )
            .await
            .scoped(spec.namespace());
        self.drain_runner_after(result).await
    }

    pub(super) async fn replay_file_commit(
        &self,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
        actor_id: &ActorId,
        journal: &UploadJournal,
    ) -> Result<Commit, CliError> {
        let mut content = Vec::new();
        if let Some(upload_id) = journal.prepared_upload_id() {
            match self
                .writer
                .complete_upload(
                    namespace_id,
                    &upload_id,
                    None,
                    ResolvedUploadCompletion::KnownContent,
                )
                .await
            {
                Ok(completed) => content.push(completed.prepared),
                // Retained commit receipts still replay after their upload was collected.
                Err(error) if error.code() == ErrorCode::UploadNotFound => {}
                Err(error) => return Err(map_namespace_scoped_runtime_error(namespace_id, error)),
            }
        }
        let mut operations = request.operations.clone();
        for operation in &mut operations {
            if let FilesystemOperation::PutFile {
                content_ref,
                inline_content,
                ..
            } = operation
            {
                if let Some(bytes) = inline_content.take() {
                    let prepared = PreparedContent::inline(namespace_id.clone(), bytes.into());
                    *content_ref = Some(prepared.content_ref().clone());
                    content.push(prepared);
                }
            }
        }
        let result = self
            .writer
            .commit_prepared(
                namespace_id,
                RuntimeCommitRequest {
                    commit_id: request.commit_id.clone(),
                    actor_id: actor_id.clone(),
                    subject: None,
                    message: request.message.clone(),
                    preconditions: request.preconditions.clone(),
                    operations,
                },
                content,
            )
            .await
            .scoped(namespace_id);
        self.drain_runner_after(result).await
    }
}
