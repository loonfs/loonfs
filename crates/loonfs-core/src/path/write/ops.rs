//! Test-support wrappers that publish one path mutation at a time through
//! the same pipeline production batches use.

use super::content_write::store_file_bytes_before_metadata_publish;
use super::intent::{CommitRequest, FilesystemOperation};
use crate::commit_engine::CommitCandidate;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::path::helpers::parse_mutation_path;
use crate::storage::content_admission::PreparedContent;
use loonfs_api::{
    CommitId, CommitResponse, DeleteDirectoryBehavior, DestinationBehavior, NamespaceId, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

fn normalized_commit_id(commit_id: Option<&CommitId>) -> CommitId {
    commit_id.cloned().unwrap_or_else(CommitId::generate)
}

async fn submit_operation<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    commit_id: CommitId,
    operation: FilesystemOperation,
    prepared_content: Vec<PreparedContent>,
    context: &MutationContext,
) -> Result<CommitResponse> {
    let request = CommitRequest::single(commit_id, None, operation);
    let candidate = if prepared_content.is_empty() {
        CommitCandidate::new(request)
    } else {
        CommitCandidate::prepared(request, prepared_content)
    };
    let mut results = crate::commit_engine::publish_namespace_commits_batch(
        store,
        namespace_id,
        vec![candidate],
        context,
    )
    .await;
    if results.len() != 1 {
        return Err(CoreError::Internal(format!(
            "path mutation batch returned {count} results for one candidate",
            count = results.len(),
        )));
    }
    results
        .pop()
        .expect("single-candidate batch should hold exactly one result")
}

pub(crate) async fn put_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: DestinationBehavior,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse> {
    let prepared_content =
        store_file_bytes_before_metadata_publish(store, namespace_id, absolute_path, bytes).await?;
    put_prepared_file_content(
        store,
        namespace_id,
        absolute_path,
        prepared_content,
        behavior,
        context,
        commit_id,
    )
    .await
}

pub(crate) async fn write_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse> {
    put_file_bytes(
        store,
        namespace_id,
        absolute_path,
        bytes,
        DestinationBehavior::Replace,
        context,
        commit_id,
    )
    .await
}

async fn put_prepared_file_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    prepared_content: PreparedContent,
    behavior: DestinationBehavior,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse> {
    let content_ref = prepared_content.content_ref().clone();
    submit_operation(
        store,
        namespace_id,
        normalized_commit_id(commit_id),
        FilesystemOperation::PutFile {
            absolute_path: parse_mutation_path(absolute_path)?,
            content_ref,
            behavior,
            expected_revision_no: None,
        },
        vec![prepared_content],
        context,
    )
    .await
}

pub(crate) async fn delete_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse> {
    delete_path_with_behavior(
        store,
        namespace_id,
        absolute_path,
        DeleteDirectoryBehavior::Recursive,
        context,
        commit_id,
    )
    .await
}

async fn delete_path_with_behavior<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    behavior: DeleteDirectoryBehavior,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse> {
    let commit_id = normalized_commit_id(commit_id);
    submit_operation(
        store,
        namespace_id,
        commit_id,
        FilesystemOperation::DeletePath {
            absolute_path: parse_mutation_path(absolute_path)?,
            behavior,
            expected_inode_id: None,
        },
        Vec::new(),
        context,
    )
    .await
}

pub(crate) async fn move_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse> {
    let commit_id = normalized_commit_id(commit_id);
    submit_operation(
        store,
        namespace_id,
        commit_id,
        FilesystemOperation::MovePath {
            from_path: parse_mutation_path(from_path)?,
            to_path: parse_mutation_path(to_path)?,
            behavior: DestinationBehavior::NoReplace,
        },
        Vec::new(),
        context,
    )
    .await
}

pub(crate) async fn restore_file_revision<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    source_revision_no: RevisionNo,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse> {
    let commit_id = normalized_commit_id(commit_id);
    submit_operation(
        store,
        namespace_id,
        commit_id,
        FilesystemOperation::RestoreRevision {
            absolute_path: parse_mutation_path(absolute_path)?,
            source_revision_no,
        },
        Vec::new(),
        context,
    )
    .await
}
