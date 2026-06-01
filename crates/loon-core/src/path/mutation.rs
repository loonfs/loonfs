use super::helpers::validate_path_for_mutation;
use super::intent::{PathMutationIntent, PutFileBehavior};
use crate::content::store_bytes_as_content;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::publisher::{DirectObjectStorePublisher, PublishOptions};
use loon_api::{v0::RenameMode, CommitId, ContentRef, MutationResult, NamespaceId, RevisionNo};
use loon_objectstore::ObjectStore;

fn generated_commit_id() -> CommitId {
    CommitId::generate()
}

fn normalized_commit_id(commit_id: Option<&str>) -> Result<CommitId, CoreError> {
    let commit_id = commit_id
        .filter(|value| !value.trim().is_empty())
        .map(CommitId::parse)
        .transpose()?
        .unwrap_or_else(generated_commit_id);
    Ok(commit_id)
}

pub fn put_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: PutFileBehavior,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let stored = store_bytes_as_content(store, namespace_id, bytes)?;
    put_file_content_ref(
        store,
        namespace_id,
        absolute_path,
        stored.content_ref,
        behavior,
        context,
        commit_id,
    )
}

pub fn write_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    put_file_bytes(
        store,
        namespace_id,
        absolute_path,
        bytes,
        PutFileBehavior::ReplaceExisting,
        context,
        commit_id,
    )
}

pub fn create_dir_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::CreateDir {
        commit_id,
        absolute_path: absolute_path.to_owned(),
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn put_file_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    content_ref: ContentRef,
    behavior: PutFileBehavior,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::PutFile {
        commit_id,
        absolute_path: absolute_path.to_owned(),
        content_ref,
        behavior,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn delete_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::DeletePath {
        commit_id,
        absolute_path: absolute_path.to_owned(),
        recursive: true,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn delete_path_non_recursive<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::DeletePath {
        commit_id,
        absolute_path: absolute_path.to_owned(),
        recursive: false,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn move_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::MovePath {
        commit_id,
        from_path: from_path.to_owned(),
        to_path: to_path.to_owned(),
        mode: RenameMode::NoReplace,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::CopyFilePath {
        commit_id,
        from_path: from_path.to_owned(),
        to_path: to_path.to_owned(),
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}

pub fn restore_file_revision<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    source_revision_no: RevisionNo,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    let intent = PathMutationIntent::RestoreRevision {
        commit_id,
        absolute_path: absolute_path.to_owned(),
        source_revision_no,
    };
    DirectObjectStorePublisher::new(store).submit_path_intent(
        namespace_id,
        intent,
        context,
        PublishOptions::default(),
    )
}
