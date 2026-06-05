use super::content_write::store_file_bytes_before_metadata_publish;
use super::executor::{normalized_commit_id, submit_path_intent};
use super::intent::{PathMutationIntent, PutFileBehavior};
use crate::context::MutationContext;
use crate::error::CoreError;
use loon_api::{v0::RenameMode, ContentRef, MutationResult, NamespaceId, RevisionNo};
use loon_objectstore::ObjectStore;

pub(crate) fn put_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: PutFileBehavior,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let content_ref =
        store_file_bytes_before_metadata_publish(store, namespace_id, absolute_path, bytes)?;
    put_file_content_ref(
        store,
        namespace_id,
        absolute_path,
        content_ref,
        behavior,
        context,
        commit_id,
    )
}

#[cfg(test)]
pub(crate) fn write_file_bytes<S: ObjectStore + ?Sized>(
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

pub(crate) fn create_dir_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::CreateDir {
            commit_id,
            absolute_path: absolute_path.to_owned(),
        },
        context,
    )
}

pub(crate) fn put_file_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    content_ref: ContentRef,
    behavior: PutFileBehavior,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
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
}

pub(crate) fn delete_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    delete_path_with_recursion(store, namespace_id, absolute_path, true, context, commit_id)
}

pub(crate) fn delete_path_non_recursive<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    delete_path_with_recursion(
        store,
        namespace_id,
        absolute_path,
        false,
        context,
        commit_id,
    )
}

fn delete_path_with_recursion<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    recursive: bool,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::DeletePath {
            commit_id,
            absolute_path: absolute_path.to_owned(),
            recursive,
        },
        context,
    )
}

pub(crate) fn move_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::MovePath {
            commit_id,
            from_path: from_path.to_owned(),
            to_path: to_path.to_owned(),
            mode: RenameMode::NoReplace,
        },
        context,
    )
}

pub(crate) fn copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
    submit_path_intent(
        store,
        namespace_id,
        PathMutationIntent::CopyFilePath {
            commit_id,
            from_path: from_path.to_owned(),
            to_path: to_path.to_owned(),
        },
        context,
    )
}

pub(crate) fn restore_file_revision<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    source_revision_no: RevisionNo,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<MutationResult, CoreError> {
    let commit_id = normalized_commit_id(commit_id)?;
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
}
