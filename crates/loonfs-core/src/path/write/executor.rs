use super::intent::PathMutationIntent;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::publisher::{DirectObjectStorePublisher, PublishOptions};
use loonfs_api::{CommitId, MutationResult, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(super) fn normalized_commit_id(commit_id: Option<&CommitId>) -> CommitId {
    commit_id.cloned().unwrap_or_else(CommitId::generate)
}

pub(super) async fn submit_path_intent<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    intent: PathMutationIntent,
    context: &MutationContext,
) -> Result<MutationResult, CoreError> {
    DirectObjectStorePublisher::new(store)
        .submit_path_intent(namespace_id, intent, context, PublishOptions::default())
        .await
}
