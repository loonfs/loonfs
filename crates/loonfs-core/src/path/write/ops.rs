//! Test-support wrappers that publish one path mutation at a time through
//! the same pipeline production batches use.

use super::content_write::store_file_bytes_before_metadata_publish;
use super::intent::PathMutationIntent;
use crate::commit_engine::NamespaceMutationCandidate;
use crate::context::MutationContext;
use crate::error::CoreError;
use loonfs_api::{
    v0::MoveBehavior, CommitId, CommitResponse, ContentRef, DeleteDirectoryBehavior, NamespaceId,
    PutBehavior, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

fn normalized_commit_id(commit_id: Option<&CommitId>) -> CommitId {
    commit_id.cloned().unwrap_or_else(CommitId::generate)
}

async fn submit_path_intent<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    intent: PathMutationIntent,
    context: &MutationContext,
) -> Result<CommitResponse, CoreError> {
    let mut results = crate::commit_engine::publish_namespace_mutations_batch(
        store,
        namespace_id,
        vec![NamespaceMutationCandidate::Path(intent)],
        context,
    )
    .await;
    results
        .pop()
        .unwrap_or_else(|| Err(CoreError::Internal("empty path mutation batch".to_owned())))
}

pub(crate) async fn put_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: PutBehavior,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse, CoreError> {
    let content_ref =
        store_file_bytes_before_metadata_publish(store, namespace_id, absolute_path, bytes).await?;
    put_file_content_ref(
        store,
        namespace_id,
        absolute_path,
        content_ref,
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
) -> Result<CommitResponse, CoreError> {
    put_file_bytes(
        store,
        namespace_id,
        absolute_path,
        bytes,
        PutBehavior::Replace,
        context,
        commit_id,
    )
    .await
}

pub(crate) async fn put_file_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    content_ref: ContentRef,
    behavior: PutBehavior,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<CommitResponse, CoreError> {
    let commit_id = normalized_commit_id(commit_id);
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::PutFile {
            commit_id,
            absolute_path: absolute_path.to_owned(),
            content_ref,
            behavior,
        },
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
) -> Result<CommitResponse, CoreError> {
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
) -> Result<CommitResponse, CoreError> {
    let commit_id = normalized_commit_id(commit_id);
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::DeletePath {
            commit_id,
            absolute_path: absolute_path.to_owned(),
            behavior,
        },
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
) -> Result<CommitResponse, CoreError> {
    let commit_id = normalized_commit_id(commit_id);
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::MovePath {
            commit_id,
            from_path: from_path.to_owned(),
            to_path: to_path.to_owned(),
            behavior: MoveBehavior::NoReplace,
        },
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
) -> Result<CommitResponse, CoreError> {
    let commit_id = normalized_commit_id(commit_id);
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::RestoreRevision {
            commit_id,
            absolute_path: absolute_path.to_owned(),
            source_revision_no,
        },
        context,
    )
    .await
}
