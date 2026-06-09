use super::intent::PathMutationIntent;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::publisher::{DirectObjectStorePublisher, PublishOptions};
use loon_api::{CommitId, MutationResult, NamespaceId};
use loon_objectstore::ObjectStore;

pub(super) fn normalized_commit_id(commit_id: Option<&str>) -> Result<CommitId, CoreError> {
    commit_id
        .filter(|value| !value.trim().is_empty())
        .map(CommitId::parse)
        .transpose()
        .map(|commit_id| commit_id.unwrap_or_else(CommitId::generate))
        .map_err(CoreError::from)
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
