//! Test-support wrappers that publish one path mutation at a time through
//! the same pipeline production batches use.

use super::content_write::store_file_bytes_before_metadata_publish;
use super::intent::PathMutationIntent;
use crate::commit_engine::NamespaceMutationCandidate;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::path::helpers::parse_mutation_path;
use crate::storage::content_admission::{ContentAdmission, PreparedContent};
use loonfs_api::{
    CommitId, CommitResponse, DeleteDirectoryBehavior, DestinationBehavior, NamespaceId, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

fn normalized_commit_id(commit_id: Option<&CommitId>) -> CommitId {
    commit_id.cloned().unwrap_or_else(CommitId::generate)
}

async fn submit_path_intent<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    intent: PathMutationIntent,
    admissions: Vec<ContentAdmission>,
    context: &MutationContext,
) -> Result<CommitResponse, CoreError> {
    let candidate = if admissions.is_empty() {
        NamespaceMutationCandidate::Path(intent)
    } else {
        NamespaceMutationCandidate::PathWithContentAdmission { intent, admissions }
    };
    let mut results = crate::commit_engine::publish_namespace_mutations_batch(
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
) -> Result<CommitResponse, CoreError> {
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
) -> Result<CommitResponse, CoreError> {
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
) -> Result<CommitResponse, CoreError> {
    let content_ref = prepared_content.content_ref().clone();
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::PutFile {
            commit_id: normalized_commit_id(commit_id),
            absolute_path: parse_mutation_path(absolute_path)?,
            content_ref,
            behavior,
        },
        vec![prepared_content.into_admission()],
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
) -> Result<CommitResponse, CoreError> {
    let commit_id = normalized_commit_id(commit_id);
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::MovePath {
            commit_id,
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
) -> Result<CommitResponse, CoreError> {
    let commit_id = normalized_commit_id(commit_id);
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::RestoreRevision {
            commit_id,
            absolute_path: parse_mutation_path(absolute_path)?,
            source_revision_no,
        },
        Vec::new(),
        context,
    )
    .await
}
