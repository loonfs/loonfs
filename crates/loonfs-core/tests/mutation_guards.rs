//! Commit and publication guard-flow integration tests.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

mod common;

use async_trait::async_trait;
use bytes::Bytes;
use common::{namespace_engine, read_context};
use futures::stream::BoxStream;
use loonfs_api::{
    sha256_digest,
    v0::{
        CommitDelta, CommitOp, CommitOp as ApiCommitOp, CommitPrecondition,
        CommitRequest as ApiCommitRequest, CompleteUploadRequest, UploadMode,
        ValidatedContentToken,
    },
    wire::control::{
        decode_control_object, encode_control_object, CheckpointOwner,
        ContentStoreDescriptorEnvelope, ContentStoreDescriptorState, ControlObjectKind, HeadState,
        HeadStateEnvelope, NamespaceConfigEnvelope, NamespaceConfigState, UploadSessionEnvelope,
        UploadSessionState, WriterBlock,
    },
    wire::manifest::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, MetadataTableFamily,
        NamespaceManifestEnvelope,
    },
    wire::wal::{decode_wal_segment_envelope_zstd, WalDelta},
    AbsolutePath, AuthoritativePathEntry, ChangeSeq, CommitId, ContentRef, ContentRefKind,
    ContentStoreId, DeleteDirectoryBehavior, DestinationBehavior, DirectoryPageCursor, DisplayName,
    InodeId, InodeKind, ManifestId, NameKey, NamespaceId, Page, PageRequest,
    RepairNamespaceOutcome, RevisionNo, UploadId, WriterEpoch,
};
use loonfs_core::commit::{
    build_commit_plan, core_commit_fingerprint_for_v0_request, materialize_commit, CommitOpResult,
    CommitRequest, CommitValidationContext, CommitValidationError, PreparedCommit,
};
use loonfs_core::content::{
    mint_content_token, prepare_existing_content_ref, store_bytes_as_content, verify_content_token,
};
use loonfs_core::control::load_namespace_head_control;
use loonfs_core::metadata::MetadataState;
use loonfs_core::publish::{
    NamespaceCommitEngine, NamespaceMutationCandidate, PathMutationIntent, PublishTailOptions,
};
use loonfs_core::{
    repair_namespace, BeginDirectPutUploadTargetResponse, BootstrapOptions, Error as CoreError,
    ErrorCode, MutationContext,
};
use loonfs_objectstore::keys::{
    content_blob, content_store_descriptor, metadata_manifest_object, namespace_config,
    upload_session, upload_session_prefix, wal_head,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::{
    CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass, OperationContext,
    OperationKind,
};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tempfile::tempdir;

#[derive(Debug, Clone)]
struct BindingIdentity {
    parent_inode_id: InodeId,
    name_key: NameKey,
    child_inode_id: InodeId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
}

async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    allow_existing: bool,
) -> Result<loonfs_api::NamespaceSummary, loonfs_core::BootstrapNamespaceError> {
    namespace_engine(store, namespace_id, context)
        .bootstrap_namespace(BootstrapOptions { allow_existing })
        .await
}

async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::NamespaceSummary, CoreError> {
    namespace_engine(store, source_namespace_id, context)
        .fork_namespace(new_namespace_id)
        .await
}

async fn load_namespace_descriptor_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> NamespaceConfigState {
    let descriptor_key = namespace_config(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .await
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    decode_control_object(&descriptor_bytes, ControlObjectKind::NamespaceConfig)
        .expect("decode namespace descriptor")
        .state
}

async fn commit_operations<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: ApiCommitRequest,
    context: &MutationContext,
) -> Result<loonfs_api::v0::CommitResponse, CoreError> {
    let candidate = prepared_commit_candidate(store, namespace_id, request).await?;
    publish_namespace_mutations_batch(store, namespace_id, vec![candidate], context)
        .await
        .into_iter()
        .next()
        .expect("single commit result")
}

async fn commit_operations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    requests: Vec<ApiCommitRequest>,
    context: &MutationContext,
) -> Vec<Result<loonfs_api::v0::CommitResponse, CoreError>> {
    let request_count = requests.len();
    let mut candidates = Vec::with_capacity(request_count);
    for request in requests {
        match prepared_commit_candidate(store, namespace_id, request).await {
            Ok(candidate) => candidates.push(candidate),
            Err(error) => return (0..request_count).map(|_| Err(error.clone())).collect(),
        }
    }
    publish_namespace_mutations_batch(store, namespace_id, candidates, context).await
}

async fn prepared_commit_candidate<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: ApiCommitRequest,
) -> Result<NamespaceMutationCandidate, CoreError> {
    let mut seen = HashSet::new();
    let mut prepared = Vec::new();
    let mut catalog = None;
    for content_ref in request.ops.iter().filter_map(|op| match op {
        ApiCommitOp::CreateFile { content_ref, .. }
        | ApiCommitOp::ReplaceFile { content_ref, .. } => Some(content_ref),
        _ => None,
    }) {
        if seen.insert(content_ref.clone()) {
            if catalog.is_none() {
                catalog = Some(
                    loonfs_core::control::load_namespace_catalog_entry(store, namespace_id).await?,
                );
            }
            prepared.push(
                prepare_existing_content_ref(
                    store,
                    catalog
                        .as_ref()
                        .expect("external content should load the namespace catalog"),
                    content_ref.clone(),
                )
                .await?,
            );
        }
    }
    Ok(NamespaceMutationCandidate::commit_prepared(
        request, prepared,
    ))
}

async fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<loonfs_api::v0::CommitResponse, CoreError>> {
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine
        .publish_batch(store, candidates, context, &PublishTailOptions::default())
        .await
        .results
}

async fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::v0::BeginUploadResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .begin_upload(loonfs_api::v0::BeginUploadRequest::default())
        .await
}

async fn begin_direct_put_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_ref: ContentRef,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .begin_direct_put_upload_target(content_ref)
        .await
}

async fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<loonfs_api::v0::UploadContentResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .upload_content(upload_id, bytes)
        .await
}

async fn complete_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    request: &CompleteUploadRequest,
    context: &MutationContext,
) -> Result<loonfs_api::v0::CompleteUploadResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .complete_upload(upload_id, request)
        .await
}

async fn list_changes_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
) -> Result<loonfs_api::v0::ChangesResponse, CoreError> {
    namespace_engine(store, namespace_id, &mutation_context())
        .list_changes_after(after_seq, page_limit(loonfs_api::DEFAULT_PAGE_LIMIT))
        .await
}

async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::CreateCheckpointResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
}

fn test_commit_id(commit_id: Option<&str>) -> CommitId {
    commit_id
        .map(|value| CommitId::parse(value).expect("valid test commit id"))
        .unwrap_or_else(CommitId::generate)
}

fn test_display_name(value: impl AsRef<str>) -> DisplayName {
    DisplayName::parse(value.as_ref()).expect("test display name should be valid")
}

/// Pins the current head and manifest the way the runtime does before a
/// read.
/// Publishes one path intent through the commit engine — the single publish
/// pipeline.
async fn admitted_candidate<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    intent: PathMutationIntent,
) -> NamespaceMutationCandidate {
    match &intent {
        PathMutationIntent::PutFile { content_ref, .. } => {
            let catalog = loonfs_core::control::load_namespace_catalog_entry(store, namespace_id)
                .await
                .expect("load namespace catalog");
            let prepared = prepare_existing_content_ref(store, &catalog, content_ref.clone())
                .await
                .expect("prepare existing content");
            NamespaceMutationCandidate::path_prepared(intent, vec![prepared])
        }
        _ => NamespaceMutationCandidate::path(intent),
    }
}

async fn submit_intent_async<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    intent: PathMutationIntent,
    context: &MutationContext,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    let candidate = admitted_candidate(store, namespace_id, intent).await;
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![candidate],
            context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("one publish result")
}

async fn submit_intent<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    intent: PathMutationIntent,
    context: &MutationContext,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_intent_async(store, namespace_id, intent, context).await
}

async fn put_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: DestinationBehavior,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    let content = store_bytes_as_content(store, namespace_id, bytes).await?;
    submit_intent(
        store,
        namespace_id,
        PathMutationIntent::PutFile {
            commit_id: test_commit_id(commit_id),
            absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
            content_ref: content.content_ref,
            behavior,
        },
        context,
    )
    .await
}

async fn write_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
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

async fn create_directory_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_intent(
        store,
        namespace_id,
        PathMutationIntent::CreateDir {
            commit_id: test_commit_id(commit_id),
            absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
            parents: false,
        },
        context,
    )
    .await
}

async fn delete_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_intent(
        store,
        namespace_id,
        PathMutationIntent::DeletePath {
            commit_id: test_commit_id(commit_id),
            absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
            behavior: DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        },
        context,
    )
    .await
}

async fn delete_path_non_recursive<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_intent(
        store,
        namespace_id,
        PathMutationIntent::DeletePath {
            commit_id: test_commit_id(commit_id),
            absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        context,
    )
    .await
}

async fn delete_path_non_recursive_expecting<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    expected_inode_id: Option<InodeId>,
    context: &MutationContext,
    commit_id: &str,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_intent(
        store,
        namespace_id,
        PathMutationIntent::DeletePath {
            commit_id: CommitId::parse(commit_id).expect("valid test commit id"),
            absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id,
        },
        context,
    )
    .await
}

async fn move_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_intent(
        store,
        namespace_id,
        PathMutationIntent::MovePath {
            commit_id: test_commit_id(commit_id),
            from_path: AbsolutePath::parse(from_path).expect("path"),
            to_path: AbsolutePath::parse(to_path).expect("path"),
            behavior: DestinationBehavior::NoReplace,
        },
        context,
    )
    .await
}

async fn copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_intent(
        store,
        namespace_id,
        PathMutationIntent::CopyFilePath {
            commit_id: test_commit_id(commit_id),
            from_path: AbsolutePath::parse(from_path).expect("path"),
            to_path: AbsolutePath::parse(to_path).expect("path"),
            behavior: DestinationBehavior::NoReplace,
        },
        context,
    )
    .await
}

async fn restore_file_revision<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    source_revision_no: RevisionNo,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_intent(
        store,
        namespace_id,
        PathMutationIntent::RestoreRevision {
            commit_id: test_commit_id(commit_id),
            absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
            source_revision_no,
        },
        context,
    )
    .await
}

async fn resolve_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_api::AuthoritativePathEntry, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .resolve_path_with_runtime_context(absolute_path, &context)
        .await
}

async fn list_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<loonfs_api::AuthoritativePathEntry>, CoreError> {
    let context = read_context(store, namespace_id).await;
    let engine = namespace_engine(store, namespace_id, &mutation_context());
    let mut entries = Vec::new();
    let mut cursor = None;
    loop {
        let page = engine
            .list_path_page_with_runtime_context(
                absolute_path,
                PageRequest {
                    limit: page_limit(1_000),
                    cursor,
                },
                &context,
            )
            .await?;
        entries.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(entries),
        }
    }
}

async fn list_path_page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    limit: u32,
    cursor: Option<DirectoryPageCursor>,
) -> Result<Page<AuthoritativePathEntry, DirectoryPageCursor>, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .list_path_page_with_runtime_context(
            absolute_path,
            PageRequest {
                limit: page_limit(limit),
                cursor,
            },
            &context,
        )
        .await
}

async fn read_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_api::AuthoritativeFileBytes, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .read_file_with_runtime_context(absolute_path, &context, None)
        .await
}

/// Drains the paged revision listing — the only revision-listing operation
/// the stack exposes — so guards can assert over a file's full history.
async fn list_file_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_api::ListFileRevisionsResponse, CoreError> {
    let entry = resolve_path(store, namespace_id, absolute_path).await?;
    list_file_revisions_for_inode(store, namespace_id, entry.inode_id).await
}

async fn list_file_revisions_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<loonfs_api::ListFileRevisionsResponse, CoreError> {
    let context = read_context(store, namespace_id).await;
    let engine = namespace_engine(store, namespace_id, &mutation_context());
    let mut revisions = Vec::new();
    let mut cursor = None;
    loop {
        let page = engine
            .list_file_revisions_for_inode_page_with_runtime_context(
                inode_id,
                PageRequest {
                    limit: page_limit(2),
                    cursor: cursor.clone(),
                },
                &context,
            )
            .await?;
        revisions.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(loonfs_api::ListFileRevisionsResponse {
                namespace_id: namespace_id.clone(),
                inode_id,
                head_seq: context.head.seq,
                revisions,
                next_cursor: None,
            });
        }
    }
}

async fn read_file_revision_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    revision_no: RevisionNo,
) -> Result<loonfs_api::AuthoritativeFileBytes, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .read_file_revision_with_runtime_context(absolute_path, revision_no, &context, None)
        .await
}

async fn read_file_revision_bytes_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .read_file_revision_for_inode_with_runtime_context(inode_id, revision_no, &context, None)
        .await
}

async fn latest_binding_for_child_from_change_feed<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    target_child_inode_id: InodeId,
) -> BindingIdentity {
    list_changes_after(store, namespace_id, ChangeSeq(0))
        .await
        .expect("change feed")
        .changes
        .into_iter()
        .flat_map(|change| {
            change
                .deltas
                .into_iter()
                .filter_map(move |delta| match delta {
                    CommitDelta::BindDirentry {
                        delta_index,
                        parent_inode_id,
                        name_key,
                        child_inode_id,
                        ..
                    } if child_inode_id == target_child_inode_id => Some(BindingIdentity {
                        parent_inode_id,
                        name_key,
                        child_inode_id,
                        bind_seq: change.seq,
                        bind_delta_index: delta_index,
                    }),
                    _ => None,
                })
        })
        .max_by_key(|binding| (binding.bind_seq, binding.bind_delta_index))
        .expect("binding exists in change feed")
}

async fn resolve_path_latest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_api::AuthoritativePathEntry, CoreError> {
    resolve_path(store, namespace_id, absolute_path).await
}

async fn list_path_latest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<loonfs_api::AuthoritativePathEntry>, CoreError> {
    list_path(store, namespace_id, absolute_path).await
}

fn wal_create_directory(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: String,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::Directory,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode_id,
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(
                loonfs_api::NamePolicy::default(),
                &display_name,
            ))
            .expect("derived name key"),
            display_name: test_display_name(display_name),
            child_inode_id: inode_id,
        },
    ]
}

fn wal_create_file(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: String,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::File,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode_id,
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(
                loonfs_api::NamePolicy::default(),
                &display_name,
            ))
            .expect("derived name key"),
            display_name: test_display_name(display_name),
            child_inode_id: inode_id,
        },
        WalDelta::AppendFileRevision {
            delta_index: delta_index.saturating_add(2),
            inode_id,
            revision_no: RevisionNo(1),
            content_ref,
        },
    ]
}

fn wal_append_revision(
    delta_index: u32,
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![WalDelta::AppendFileRevision {
        delta_index,
        inode_id,
        revision_no,
        content_ref,
    }]
}

fn wal_tombstone(delta_index: u32, root_inode_id: InodeId) -> Vec<WalDelta> {
    vec![WalDelta::TombstoneSubtree {
        delta_index,
        root_inode_id,
    }]
}

#[tokio::test]
async fn stale_revision_precondition_is_rejected() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("stale-revision").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::ReplaceFile {
            inode_id: InodeId(3),
            base_revision_no: RevisionNo(1),
            content_ref: content_ref("content-3"),
        }],
        preconditions: Vec::new(),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("stale revision");
    assert!(matches!(
        error,
        CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));
}

#[tokio::test]
async fn failed_multi_op_plan_uses_preview_without_mutating_base_metadata() {
    let metadata_state = metadata_state_after(&[]);
    let context = validation_context(&metadata_state, ChangeSeq(0), InodeId(2));
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("preview-rollback").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![
            CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: test_display_name("docs"),
            },
            CommitOp::CreateFile {
                parent_inode_id: InodeId(2),
                display_name: test_display_name("readme.txt"),
                content_ref: content_ref("content-1"),
            },
            CommitOp::ReplaceFile {
                inode_id: InodeId(99),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            },
        ],
        preconditions: Vec::new(),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("late op fails");
    assert!(matches!(
        error,
        CommitValidationError::ReplaceFileInodeMissing {
            inode_id: InodeId(99)
        }
    ));
    assert!(metadata_state
        .visible_child(
            InodeId(1),
            &NameKey::parse("docs").expect("valid name key"),
            ChangeSeq(1),
        )
        .is_none());
}

#[tokio::test]
async fn create_and_replace_under_ancestor_tombstone_are_rejected() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_tombstone(0, InodeId(2)),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let create_error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("create-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::CreateFile {
                parent_inode_id: InodeId(2),
                display_name: test_display_name("new.txt"),
                content_ref: content_ref("content-2"),
            }],
            preconditions: Vec::new(),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("create under tombstone");
    assert!(matches!(
        create_error,
        CommitValidationError::CreateUnderSubtreeTombstone {
            parent_inode_id: InodeId(2),
            ..
        }
    ));

    let replace_error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("replace-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            }],
            preconditions: Vec::new(),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("replace under tombstone");
    assert!(matches!(
        replace_error,
        CommitValidationError::ReplaceFileUnderSubtreeTombstone {
            inode_id: InodeId(3),
            ..
        }
    ));
}

#[tokio::test]
async fn restore_revision_validation_rejects_missing_inode() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        InodeId(1),
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-missing-inode").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(99),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }],
        preconditions: Vec::new(),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore missing inode");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionInodeMissing {
            inode_id: InodeId(99),
        }
    ));
}

#[tokio::test]
async fn restore_revision_validation_rejects_non_file_target() {
    let metadata_state = metadata_state_after(&[wal_create_directory(
        0,
        InodeId(2),
        InodeId(1),
        "docs".to_owned(),
    )]);
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-non-file").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
        }],
        preconditions: Vec::new(),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore non-file");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionInodeNotFile {
            inode_id: InodeId(2),
            actual_kind: InodeKind::Directory,
        }
    ));
}

#[tokio::test]
async fn restore_revision_validation_rejects_stale_or_missing_source_revision() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let stale_base = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("restore-stale-base").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(1),
            }],
            preconditions: Vec::new(),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("restore stale base");
    assert!(matches!(
        stale_base,
        CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id: InodeId(3),
            expected: RevisionNo(1),
            actual: Some(RevisionNo(2)),
        }
    ));

    let missing_source = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("restore-missing-source").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(99),
                base_revision_no: RevisionNo(2),
            }],
            preconditions: Vec::new(),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("restore missing source");
    assert!(matches!(
        missing_source,
        CommitValidationError::RestoreRevisionSourceRevisionMissing {
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(99),
        }
    ));
}

#[tokio::test]
async fn restore_revision_can_reference_revision_created_earlier_in_same_request() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(2), InodeId(4));

    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-same-request-source").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![
            CommitOp::ReplaceFile {
                inode_id: InodeId(3),
                base_revision_no: RevisionNo(1),
                content_ref: content_ref("content-2"),
            },
            CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(2),
                base_revision_no: RevisionNo(2),
            },
        ],
        preconditions: Vec::new(),
        message: None,
    };
    let plan = build_commit_plan(&request, 4_200, &context)
        .await
        .expect("replace then restore in same request should validate");
    let materialized = materialize_commit(
        PreparedCommit::new(request, plan).expect("prepare commit"),
        4_200,
    );
    let expected = content_ref("content-2");
    assert!(matches!(
        &materialized.results[1],
        CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
}

#[tokio::test]
async fn restore_revision_can_reference_restore_created_earlier_in_same_request() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-after-restore-same-request").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![
            CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(2),
            },
            CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(3),
                base_revision_no: RevisionNo(3),
            },
        ],
        preconditions: Vec::new(),
        message: None,
    };
    let plan = build_commit_plan(&request, 4_200, &context)
        .await
        .expect("restore then restore in same request should validate");
    let materialized = materialize_commit(
        PreparedCommit::new(request, plan).expect("prepare commit"),
        4_200,
    );
    let expected = content_ref("content-1");
    assert!(matches!(
        &materialized.results[0],
        CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
    assert!(matches!(
        &materialized.results[1],
        CommitOpResult::RestoreRevision {
            content_ref,
            ..
        } if *content_ref == expected
    ));
}

#[tokio::test]
async fn restore_revision_under_tombstoned_ancestor_is_rejected() {
    let metadata_state = metadata_state_after(&[
        wal_create_directory(0, InodeId(2), InodeId(1), "docs".to_owned()),
        wal_create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt".to_owned(),
            content_ref("content-1"),
        ),
        wal_tombstone(0, InodeId(2)),
    ]);
    let context = validation_context(&metadata_state, ChangeSeq(3), InodeId(4));

    let error = build_commit_plan(
        &CommitRequest {
            namespace_id: namespace_id("demo"),
            commit_id: CommitId::parse("restore-under-tombstone").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::RestoreRevision {
                inode_id: InodeId(3),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(1),
            }],
            preconditions: Vec::new(),
            message: None,
        },
        4_200,
        &context,
    )
    .await
    .expect_err("restore tombstone conflict");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
            inode_id: InodeId(3),
            ..
        }
    ));
}

#[tokio::test]
async fn restore_revision_overflow_is_rejected() {
    let mut deltas = wal_create_file(
        0,
        InodeId(2),
        InodeId(1),
        "overflow.txt".to_owned(),
        content_ref("content-max"),
    );
    deltas[2] = WalDelta::AppendFileRevision {
        delta_index: 2,
        inode_id: InodeId(2),
        revision_no: RevisionNo(u64::MAX),
        content_ref: content_ref("content-max"),
    };
    let metadata_state = MetadataState::default()
        .apply_committed_wal_deltas(
            ChangeSeq(0),
            4_200,
            &[WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
            }],
        )
        .metadata_state
        .apply_committed_wal_deltas(ChangeSeq(1), 4_200, &deltas)
        .metadata_state;
    let context = validation_context(&metadata_state, ChangeSeq(1), InodeId(3));
    let request = CommitRequest {
        namespace_id: namespace_id("demo"),
        commit_id: CommitId::parse("restore-overflow").expect("valid commit id"),
        writer_id: "writer-a".to_owned(),
        writer_session_id: "wrs_test".to_owned(),
        writer_epoch: WriterEpoch(1),
        ops: vec![CommitOp::RestoreRevision {
            inode_id: InodeId(2),
            source_revision_no: RevisionNo(u64::MAX),
            base_revision_no: RevisionNo(u64::MAX),
        }],
        preconditions: Vec::new(),
        message: None,
    };

    let error = build_commit_plan(&request, 4_200, &context)
        .await
        .expect_err("restore overflow");
    assert!(matches!(
        error,
        CommitValidationError::RestoreRevisionOverflow {
            inode_id: InodeId(2),
            base_revision_no: RevisionNo(u64::MAX),
        }
    ));
}

#[tokio::test]
async fn namespace_creation_writes_descriptors_and_rejects_partial_recreation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    let duplicate_error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("complete namespace id reuse should be rejected");
    assert!(matches!(
        duplicate_error,
        loonfs_core::BootstrapNamespaceError::NamespaceAlreadyExists { .. }
    ));
    let existing = bootstrap_namespace(&store, &namespace_id, &context, true)
        .await
        .expect("allow existing");
    assert_eq!(existing.namespace_id, namespace_id);

    let descriptor_key = namespace_config(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .await
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    let descriptor: NamespaceConfigEnvelope =
        decode_control_object(&descriptor_bytes, ControlObjectKind::NamespaceConfig)
            .expect("decode namespace descriptor");
    assert_eq!(descriptor.state.namespace_id, namespace_id);
    assert!(store
        .head(&content_store_descriptor(
            descriptor.state.content_store_id.as_str()
        ))
        .await
        .expect("content store descriptor head")
        .is_some());
    let manifest_root =
        loonfs_core::control::load_namespace_metadata_root_control(&store, &namespace_id)
            .await
            .expect("metadata root");
    let manifest_bytes = store
        .get(
            &metadata_manifest_object(
                namespace_id.as_str(),
                &manifest_root.state.manifest_object_id,
            ),
            None,
        )
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let manifest =
        decode_namespace_manifest_json(&manifest_bytes).expect("decode namespace manifest");
    assert!(
        manifest.payload.fork.is_none(),
        "root namespace creation must not write fork provenance"
    );

    let content_descriptor_key =
        content_store_descriptor(descriptor.state.content_store_id.as_str());
    let content_descriptor_bytes = store
        .get(&content_descriptor_key, None)
        .await
        .expect("read content-store descriptor")
        .expect("content-store descriptor exists");
    let content_descriptor: ContentStoreDescriptorEnvelope = decode_control_object(
        &content_descriptor_bytes,
        ControlObjectKind::ContentStoreDescriptor,
    )
    .expect("decode content-store descriptor");
    assert_eq!(
        content_descriptor.state.content_store_id,
        descriptor.state.content_store_id
    );

    let content_store_descriptors = store
        .list_prefix("content-stores/")
        .await
        .expect("list content stores");
    assert_eq!(
        content_store_descriptors,
        vec![content_descriptor_key],
        "new root namespace should create exactly one content store descriptor"
    );

    store
        .put_if_absent(
            &wal_head("partial"),
            Bytes::from_static(br#"{"not":"a descriptor"}"#),
        )
        .await
        .expect("write partial namespace key");

    let partial_error = bootstrap_namespace(
        &store,
        &NamespaceId::parse("partial").expect("valid namespace id"),
        &context,
        false,
    )
    .await
    .expect_err("partial namespace should be rejected");
    assert!(matches!(
        partial_error,
        loonfs_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
}

#[tokio::test]
async fn bootstrap_head_lost_ack_stays_partial_until_explicit_repair() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(wal_head(namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: Vec::new(),
        },
    );

    let error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("target head precondition should re-check partial namespace");
    // The lost ack left the head written and the descriptor absent, so the
    // shared install re-check answers partial — the same policy fork pins.
    assert!(matches!(
        &error,
        loonfs_core::BootstrapNamespaceError::Core(CoreError::NamespacePartial { .. })
    ));
    assert_eq!(error.code(), ErrorCode::NamespacePartial);
    assert!(
        store
            .list_prefix("content-stores/")
            .await
            .expect("list content stores")
            .is_empty(),
        "content-store descriptor must not be allocated before namespace head reservation"
    );
    // The injected failure wrote the head before reporting the lost ack.
    // Normal create retries are classification-only and leave it untouched.
    let retry = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("retry preserves the ack-lost partial tree");
    assert_eq!(retry.code(), ErrorCode::NamespacePartial);
    let report = repair_namespace(&store, &namespace_id, &context)
        .await
        .expect("explicit repair completes the ack-lost create");
    assert_eq!(report.outcome, RepairNamespaceOutcome::Completed);
}

#[tokio::test]
async fn bootstrap_head_conflict_rechecks_complete_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    // The injected conflict simulates a racing create that completed the
    // namespace: its head wins the reservation and its descriptor pair is
    // already published when this attempt re-checks.
    let content_store_id = ContentStoreId::generate();
    let content_store_descriptor_envelope = ContentStoreDescriptorEnvelope::from_state(
        ControlObjectKind::ContentStoreDescriptor,
        &context.writer_version,
        ContentStoreDescriptorState {
            content_store_id: content_store_id.clone(),
        },
    )
    .expect("content store descriptor envelope");
    let descriptor = NamespaceConfigEnvelope::from_state(
        ControlObjectKind::NamespaceConfig,
        &context.writer_version,
        NamespaceConfigState {
            namespace_id: namespace_id.clone(),
            content_store_id: content_store_id.clone(),
            name_policy: loonfs_api::NamePolicy::default(),
        },
    )
    .expect("descriptor envelope");
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(wal_head(namespace_id.as_str())),
        InjectedCreateFailure::Conflict {
            write_attempted_object: true,
            additional_writes: vec![
                (
                    content_store_descriptor(content_store_id.as_str()),
                    encode_control_object(&content_store_descriptor_envelope)
                        .expect("content store descriptor bytes"),
                ),
                (
                    namespace_config(namespace_id.as_str()),
                    encode_control_object(&descriptor).expect("descriptor bytes"),
                ),
            ],
        },
    );

    let error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("target head conflict should re-check complete namespace");
    assert!(matches!(
        &error,
        loonfs_core::BootstrapNamespaceError::Core(CoreError::NamespaceExists { .. })
    ));
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
}

#[tokio::test]
async fn begin_upload_rejects_missing_and_partial_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    let missing_error = begin_upload(&store, &namespace_id, &context)
        .await
        .expect_err("missing namespace");
    assert_eq!(missing_error.code(), ErrorCode::NamespaceNotFound);

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    store
        .delete(&namespace_config(namespace_id.as_str()))
        .await
        .expect("delete descriptor");

    let partial_error = begin_upload(&store, &namespace_id, &context)
        .await
        .expect_err("partial namespace");
    assert_eq!(partial_error.code(), ErrorCode::NamespacePartial);
}

#[tokio::test]
async fn begin_direct_put_rejects_unsupported_content_ref_without_session() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let content_ref = ContentRef {
        kind: ContentRefKind::Unsupported("future_kind".to_owned()),
        digest: sha256_digest(b"hello"),
        size_bytes: 5,
    };
    let error = begin_direct_put_upload_target(&store, &namespace_id, content_ref, &context)
        .await
        .expect_err("unsupported direct_put content ref");

    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        store
            .list_prefix(&upload_session_prefix(namespace_id.as_str()))
            .await
            .expect("list upload sessions"),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn begin_upload_does_not_read_manifest_or_wal_replay_objects() {
    let temp_dir = tempdir().expect("tempdir");
    let setup_store = LocalFsStore::new(temp_dir.path()).expect("setup store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&setup_store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &setup_store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("upload-guard-create"),
    )
    .await
    .expect("create file");
    create_checkpoint(&setup_store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    put_file_bytes(
        &setup_store,
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        DestinationBehavior::Replace,
        &context,
        Some("upload-guard-replace"),
    )
    .await
    .expect("replace file");

    let guarded_store = replay_read_guard_store(temp_dir.path(), namespace_id.as_str());
    let begin = begin_upload(&guarded_store, &namespace_id, &context)
        .await
        .expect("begin upload");
    assert_eq!(begin.namespace_id, namespace_id);
    assert_eq!(guarded_store.attempts(), 0);
}

#[tokio::test]
async fn complete_upload_does_not_get_content_blob_after_staging() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let begin = begin_upload(&store, &namespace_id, &context)
        .await
        .expect("begin upload");
    let uploaded = upload_content(&store, &namespace_id, &begin.upload_id, b"hello", &context)
        .await
        .expect("upload content");

    store.reset();
    let completed = complete_upload(
        &store,
        &namespace_id,
        &begin.upload_id,
        &CompleteUploadRequest {
            content_ref: uploaded.content_ref.clone(),
        },
        &context,
    )
    .await
    .expect("complete upload");
    assert_eq!(completed.content_ref, uploaded.content_ref);
    assert_eq!(store.count(OperationClass::Read), 0);

    store.reset();
    let completed_again = complete_upload(
        &store,
        &namespace_id,
        &begin.upload_id,
        &CompleteUploadRequest {
            content_ref: uploaded.content_ref,
        },
        &context,
    )
    .await
    .expect("complete upload idempotently");
    assert_eq!(completed_again.content_ref, completed.content_ref);
    assert_eq!(store.count(OperationClass::Read), 0);

    let mismatch_begin = begin_upload(&store, &namespace_id, &context)
        .await
        .expect("begin mismatch");
    let mismatch_uploaded = upload_content(
        &store,
        &namespace_id,
        &mismatch_begin.upload_id,
        b"staged",
        &context,
    )
    .await
    .expect("upload mismatch content");
    let wrong_ref = ContentRef::whole_file_v0(b"different");
    assert_ne!(wrong_ref, mismatch_uploaded.content_ref);

    store.reset();
    let mismatch = complete_upload(
        &store,
        &namespace_id,
        &mismatch_begin.upload_id,
        &CompleteUploadRequest {
            content_ref: wrong_ref,
        },
        &context,
    )
    .await
    .expect_err("mismatched content ref");
    assert_eq!(mismatch.code(), ErrorCode::InvalidRequest);
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn complete_upload_rejects_direct_put_session_without_bound_target() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stored = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("store content");

    let upload_id =
        UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id");
    let state = UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode: UploadMode::DirectPut,
        direct_put_content_ref: None,
        staged_content_ref: None,
        completed: None,
        created_at_ms: context.now_ms,
        state: loonfs_api::wire::control::UploadSessionLifecycle::Active,
    };
    let envelope =
        UploadSessionEnvelope::from_state(ControlObjectKind::UploadSession, "test", state)
            .expect("upload session envelope");
    let encoded = encode_control_object(&envelope).expect("encode upload session");
    store
        .put_if_absent(
            &upload_session(namespace_id.as_str(), upload_id.as_str()),
            Bytes::from(encoded),
        )
        .await
        .expect("write malformed upload session");

    let error = complete_upload(
        &store,
        &namespace_id,
        &upload_id,
        &CompleteUploadRequest {
            content_ref: stored.content_ref,
        },
        &context,
    )
    .await
    .expect_err("direct_put session without target should fail closed");

    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn path_put_file_without_admission_fails_without_reading_content() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("stage content");

    store.reset();
    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![NamespaceMutationCandidate::path(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-cold-content").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs/hello.txt").expect("path"),
                content_ref: content.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
        )],
        &context,
    )
    .await;

    assert_eq!(
        responses[0]
            .as_ref()
            .expect_err("unadmitted path put must fail")
            .code(),
        ErrorCode::ContentNotPrepared
    );
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn path_batch_rejects_repeated_unadmitted_content_without_reading_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"shared")
        .await
        .expect("stage content");

    store.reset();
    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![
            NamespaceMutationCandidate::path(PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-shared-a").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                content_ref: content.content_ref.clone(),
                behavior: DestinationBehavior::NoReplace,
            }),
            NamespaceMutationCandidate::path(PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-shared-b").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                content_ref: content.content_ref,
                behavior: DestinationBehavior::NoReplace,
            }),
        ],
        &context,
    )
    .await;

    assert!(responses.iter().all(|response| {
        response
            .as_ref()
            .is_err_and(|error| error.code() == ErrorCode::ContentNotPrepared)
    }));
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn valid_content_admission_skips_durable_content_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"admitted")
        .await
        .expect("stage content");
    let token = ValidatedContentToken {
        content_ref: content.content_ref.clone(),
        token: mint_content_token(
            "test-content-token-secret",
            &namespace_id,
            &content.content_ref,
            context.now_ms,
        )
        .expect("mint token"),
    };
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = verify_content_token(
        "test-content-token-secret",
        &catalog,
        &token,
        context.now_ms,
    )
    .expect("verify token");

    store.reset();
    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![NamespaceMutationCandidate::path_prepared(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("put-admitted-content").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs/admitted.txt").expect("path"),
                content_ref: content.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
            vec![prepared],
        )],
        &context,
    )
    .await;

    assert!(responses[0].is_ok());
    // A live admission is the fast path: no content read at all.
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn metadata_queries_do_not_get_content_blobs_but_file_reads_do_once() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &namespace_id, "/docs", &context, Some("mkdir-docs"))
        .await
        .expect("create docs");

    for index in 0..3 {
        let path = format!("/docs/file-{index}.txt");
        let bytes = format!("file-{index}-bytes");
        let commit_id = format!("put-file-{index}");
        put_file_bytes(
            &store,
            &namespace_id,
            &path,
            bytes.as_bytes(),
            DestinationBehavior::NoReplace,
            &context,
            Some(&commit_id),
        )
        .await
        .expect("put file");
    }

    store.reset();
    let stat = resolve_path(&store, &namespace_id, "/docs/file-1.txt")
        .await
        .expect("stat file");
    assert_eq!(stat.inode_kind, InodeKind::File);
    assert_eq!(stat.size_bytes, Some("file-1-bytes".len() as u64));
    assert!(stat.content_ref.is_some());
    assert_eq!(store.count(OperationClass::Read), 0);

    store.reset();
    let entries = list_path(&store, &namespace_id, "/docs")
        .await
        .expect("list docs");
    assert_eq!(entries.len(), 3);
    for entry in entries {
        assert_eq!(entry.inode_kind, InodeKind::File);
        assert_eq!(entry.size_bytes, Some("file-0-bytes".len() as u64));
        assert!(entry.content_ref.is_some());
    }
    assert_eq!(store.count(OperationClass::Read), 0);

    store.reset();
    let read = read_file_bytes(&store, &namespace_id, "/docs/file-1.txt")
        .await
        .expect("read file");
    assert_eq!(read.bytes, b"file-1-bytes");
    assert_eq!(store.count(OperationClass::Read), 1);
}

#[tokio::test]
async fn query_driven_reads_use_initial_manifest_with_wal_overlay() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/file.txt",
        b"file",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-file"),
    )
    .await
    .expect("put file");

    let stat = resolve_path_latest(&store, &namespace_id, "/docs/file.txt")
        .await
        .expect("stat with manifest");
    let list = list_path_latest(&store, &namespace_id, "/docs")
        .await
        .expect("list with manifest");

    assert_eq!(stat.size_bytes, Some(4));
    assert_eq!(list.len(), 1);
}

#[tokio::test]
async fn query_driven_stat_and_list_use_metadata_view_with_l0_run_and_wal_overlay() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-alpha"),
    )
    .await
    .expect("put alpha");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"bravo",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-bravo"),
    )
    .await
    .expect("put bravo");
    put_file_bytes(
        &store,
        &namespace_id,
        "/dead/leaf.txt",
        b"dead",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-dead"),
    )
    .await
    .expect("put dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("base checkpoint");

    move_path(
        &store,
        &namespace_id,
        "/docs/a.txt",
        "/docs/moved.txt",
        &context,
        Some("move-alpha"),
    )
    .await
    .expect("move alpha");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"bravo two",
        DestinationBehavior::Replace,
        &context,
        Some("replace-bravo"),
    )
    .await
    .expect("replace bravo");
    delete_path(
        &store,
        &namespace_id,
        "/dead",
        &context,
        Some("delete-dead"),
    )
    .await
    .expect("delete dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("l0 checkpoint");

    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/wal.txt",
        b"wal",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-wal"),
    )
    .await
    .expect("put wal tail");

    let expected_stat = resolve_path(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("stat");
    let expected_list = list_path(&store, &namespace_id, "/docs")
        .await
        .expect("list");
    let expected_file_list = list_path(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("file list");

    store.reset();
    let actual_stat = resolve_path_latest(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("materialized stat");
    let actual_list = list_path_latest(&store, &namespace_id, "/docs")
        .await
        .expect("materialized list");
    let actual_file_list = list_path_latest(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("materialized file list");
    let actual_casefold_stat = resolve_path_latest(&store, &namespace_id, "/DOCS/MOVED.TXT")
        .await
        .expect("materialized casefold stat");

    assert_eq!(actual_stat, expected_stat);
    assert_eq!(actual_casefold_stat, expected_stat);
    assert_eq!(actual_list, expected_list);
    assert_eq!(actual_file_list, expected_file_list);
    assert_eq!(store.count(OperationClass::Read), 0);

    assert!(resolve_path_latest(&store, &namespace_id, "/docs/a.txt")
        .await
        .is_err());
    assert!(resolve_path_latest(&store, &namespace_id, "/dead/leaf.txt")
        .await
        .is_err());
}

#[tokio::test]
async fn query_driven_stat_uses_exact_name_key_for_dash_containing_siblings() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/report",
        b"short",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-report"),
    )
    .await
    .expect("put report");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/report-2024",
        b"newer-longer",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-report-2024"),
    )
    .await
    .expect("put report-2024");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");

    let expected = resolve_path(&store, &namespace_id, "/docs/report")
        .await
        .expect("stat");
    let actual = resolve_path_latest(&store, &namespace_id, "/docs/report")
        .await
        .expect("materialized stat");

    assert_eq!(actual, expected);
    assert_eq!(actual.absolute_path, "/docs/report");
    assert_eq!(actual.size_bytes, Some(5));
}

#[tokio::test]
async fn batch_delete_then_recreate_of_a_durable_file_layers_over_cached_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/cycled.txt",
        b"durable",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-durable"),
    )
    .await
    .expect("put durable file");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint durable state");

    // One batch: delete the checkpointed file, then recreate the same name
    // with NoReplace. The recreate must observe the batch-local unbind over
    // the durable binding, and the delete must observe the durable binding
    // at all — both through the batch's cached durable layer.
    let staged = loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"recreated")
        .await
        .expect("stage recreated content");
    let results = namespace_engine(&store, &namespace_id, &context)
        .publish_namespace_mutations_batch(vec![
            NamespaceMutationCandidate::path(PathMutationIntent::DeletePath {
                commit_id: CommitId::parse("delete-cycled").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/docs/cycled.txt").expect("path"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            }),
            admitted_candidate(
                &store,
                &namespace_id,
                PathMutationIntent::PutFile {
                    commit_id: CommitId::parse("recreate-cycled").expect("valid commit id"),
                    absolute_path: AbsolutePath::parse("/docs/cycled.txt").expect("path"),
                    content_ref: staged.content_ref,
                    behavior: DestinationBehavior::NoReplace,
                },
            )
            .await,
        ])
        .await;
    results[0]
        .as_ref()
        .expect("delete of durable file succeeds");
    results[1]
        .as_ref()
        .expect("no-replace recreate sees the in-batch unbind over the durable binding");

    let recreated = read_file_bytes(&store, &namespace_id, "/docs/cycled.txt")
        .await
        .expect("read recreated file");
    assert_eq!(recreated.bytes, b"recreated");
}

#[tokio::test]
async fn wide_directory_listing_resolves_tail_unbinds_cross_directory_renames_and_rebinds() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &namespace_id, "/wide", &context, Some("mkdir-wide"))
        .await
        .expect("create wide");
    create_directory_path(
        &store,
        &namespace_id,
        "/other",
        &context,
        Some("mkdir-other"),
    )
    .await
    .expect("create other");

    // More names than one raw scan chunk, so group boundaries cross
    // manifest pages; a few names rebound several times, so groups carry
    // more than one row.
    for index in 0..70u32 {
        let path = format!("/wide/file-{index:03}.txt");
        put_file_bytes(
            &store,
            &namespace_id,
            &path,
            b"first",
            DestinationBehavior::NoReplace,
            &context,
            Some(&format!("put-{index:03}")),
        )
        .await
        .expect("put file");
    }
    for index in [3u32, 35, 68] {
        let path = format!("/wide/file-{index:03}.txt");
        put_file_bytes(
            &store,
            &namespace_id,
            &path,
            b"rebound",
            DestinationBehavior::Replace,
            &context,
            Some(&format!("rebind-{index:03}")),
        )
        .await
        .expect("rebind file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint wide dir");

    // Tail-only mutations over checkpointed state: a delete whose unbind
    // lives only in the WAL tail, and a rename into another directory whose
    // current binding lives outside the listed parent.
    delete_path(
        &store,
        &namespace_id,
        "/wide/file-010.txt",
        &context,
        Some("delete-tail-unbind"),
    )
    .await
    .expect("delete over checkpointed bind");
    move_path(
        &store,
        &namespace_id,
        "/wide/file-020.txt",
        "/other/moved-away.txt",
        &context,
        Some("move-cross-directory"),
    )
    .await
    .expect("move into other directory");
    put_file_bytes(
        &store,
        &namespace_id,
        "/wide/file-005.txt",
        b"tail-rebound",
        DestinationBehavior::Replace,
        &context,
        Some("tail-rebind-005"),
    )
    .await
    .expect("tail rebind over checkpointed bind");

    let mut listed = Vec::new();
    let mut cursor = None;
    loop {
        let page = list_path_page(&store, &namespace_id, "/wide", 16, cursor.clone())
            .await
            .expect("list page");
        listed.extend(
            page.items
                .iter()
                .map(|entry| entry.display_name.as_str().to_owned()),
        );
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    let expected: Vec<String> = (0..70u32)
        .filter(|index| *index != 10 && *index != 20)
        .map(|index| format!("file-{index:03}.txt"))
        .collect();
    assert_eq!(listed, expected);

    // The tail rebind is the visible revision, not the checkpointed one.
    let rebound = read_file_bytes(&store, &namespace_id, "/wide/file-005.txt")
        .await
        .expect("read tail-rebound file");
    assert_eq!(rebound.bytes, b"tail-rebound");
    // The moved child is visible under its new parent only.
    let moved = read_file_bytes(&store, &namespace_id, "/other/moved-away.txt")
        .await
        .expect("read moved file");
    assert_eq!(moved.bytes, b"first");
}

#[tokio::test]
async fn query_driven_directory_page_merges_manifest_and_tail_visible_children() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &namespace_id, "/docs", &context, Some("mkdir-docs"))
        .await
        .expect("create docs");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs/a-dir",
        &context,
        Some("mkdir-a-dir"),
    )
    .await
    .expect("create a-dir");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/c-file.txt",
        b"charlie",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-c-file"),
    )
    .await
    .expect("put c-file");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/stale.txt",
        b"stale",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-stale"),
    )
    .await
    .expect("put stale");
    move_path(
        &store,
        &namespace_id,
        "/docs/stale.txt",
        "/docs/b-renamed.txt",
        &context,
        Some("move-stale"),
    )
    .await
    .expect("move stale");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/dead/leaf.txt",
        b"dead",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-dead"),
    )
    .await
    .expect("put dead");
    delete_path(
        &store,
        &namespace_id,
        "/docs/dead",
        &context,
        Some("delete-dead"),
    )
    .await
    .expect("delete dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint manifest children");

    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/d-tail.txt",
        b"delta",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-d-tail"),
    )
    .await
    .expect("put d-tail");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs/e-tail-dir",
        &context,
        Some("mkdir-e-tail-dir"),
    )
    .await
    .expect("create tail dir");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/tail-dead/leaf.txt",
        b"tail-dead",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-tail-dead"),
    )
    .await
    .expect("put tail dead");
    delete_path(
        &store,
        &namespace_id,
        "/docs/tail-dead",
        &context,
        Some("delete-tail-dead"),
    )
    .await
    .expect("delete tail dead");

    let first = list_path_page(&store, &namespace_id, "/docs", 2, None)
        .await
        .expect("first page");
    assert_eq!(
        first
            .items
            .iter()
            .map(|entry| entry.display_name.as_str())
            .collect::<Vec<_>>(),
        vec!["a-dir", "b-renamed.txt"]
    );
    let second = list_path_page(&store, &namespace_id, "/docs", 2, first.next_cursor.clone())
        .await
        .expect("second page");
    assert_eq!(
        second
            .items
            .iter()
            .map(|entry| entry.display_name.as_str())
            .collect::<Vec<_>>(),
        vec!["c-file.txt", "d-tail.txt"]
    );
    let third = list_path_page(
        &store,
        &namespace_id,
        "/docs",
        2,
        second.next_cursor.clone(),
    )
    .await
    .expect("third page");
    assert!(third.next_cursor.is_none());
    assert_eq!(
        third
            .items
            .iter()
            .map(|entry| entry.display_name.as_str())
            .collect::<Vec<_>>(),
        vec!["e-tail-dir"]
    );

    let entries = first
        .items
        .into_iter()
        .chain(second.items)
        .chain(third.items)
        .collect::<Vec<_>>();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.display_name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "a-dir",
            "b-renamed.txt",
            "c-file.txt",
            "d-tail.txt",
            "e-tail-dir"
        ]
    );
    assert!(entries.iter().all(|entry| !matches!(
        entry.display_name.as_str(),
        "stale.txt" | "dead" | "tail-dead"
    )));

    for directory_name in ["a-dir", "e-tail-dir"] {
        let entry = entries
            .iter()
            .find(|entry| entry.display_name.as_str() == directory_name)
            .expect("directory entry");
        assert_eq!(entry.inode_kind, InodeKind::Directory);
        assert_eq!(entry.revision_no, None);
        assert_eq!(entry.size_bytes, None);
        assert!(entry.content_ref.is_none());
    }

    for (file_name, size) in [
        ("b-renamed.txt", b"stale".len() as u64),
        ("c-file.txt", b"charlie".len() as u64),
        ("d-tail.txt", b"delta".len() as u64),
    ] {
        let entry = entries
            .iter()
            .find(|entry| entry.display_name.as_str() == file_name)
            .expect("file entry");
        assert_eq!(entry.inode_kind, InodeKind::File);
        assert_eq!(entry.revision_no, Some(RevisionNo(1)));
        assert_eq!(entry.size_bytes, Some(size));
        assert!(entry.content_ref.is_some());
    }
}

#[tokio::test]
async fn upload_content_rejects_invalid_upload_id_before_key_construction() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let invalid_upload_id = ["upl", "123"].join("-");
    let error = UploadId::parse(&invalid_upload_id)
        .map_err(CoreError::InvalidUploadId)
        .expect_err("invalid upload_id should be rejected");

    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        store
            .list_prefix(&upload_session_prefix(namespace_id.as_str()))
            .await
            .expect("list upload sessions"),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn revision_queries_read_historical_bytes_and_path_restore_appends_revision() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        b"one",
        DestinationBehavior::NoReplace,
        &context,
        Some("rev-create"),
    )
    .await
    .expect("create file");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        b"two",
        &context,
        Some("rev-replace"),
    )
    .await
    .expect("replace file");

    let entry = resolve_path(&store, &namespace_id, "/docs/rev.txt")
        .await
        .expect("stat file");
    let inode_id = entry.inode_id;
    let path_revisions = list_file_revisions(&store, &namespace_id, "/docs/rev.txt")
        .await
        .expect("path revisions");
    assert_eq!(path_revisions.inode_id, inode_id);
    assert_eq!(
        path_revisions
            .revisions
            .iter()
            .map(|revision| revision.revision_no)
            .collect::<Vec<_>>(),
        vec![RevisionNo(2), RevisionNo(1)]
    );

    let historical =
        read_file_revision_bytes(&store, &namespace_id, "/docs/rev.txt", RevisionNo(1))
            .await
            .expect("read first revision");
    assert_eq!(historical.bytes, b"one");
    let inode_historical =
        read_file_revision_bytes_for_inode(&store, &namespace_id, inode_id, RevisionNo(2))
            .await
            .expect("read inode revision");
    assert_eq!(inode_historical, b"two");

    move_path(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        "/docs/moved.txt",
        &context,
        Some("rev-move"),
    )
    .await
    .expect("move file");
    assert_eq!(
        list_file_revisions(&store, &namespace_id, "/docs/rev.txt")
            .await
            .expect_err("old path no longer resolves")
            .code(),
        ErrorCode::PathNotFound
    );
    let inode_revisions = list_file_revisions_for_inode(&store, &namespace_id, inode_id)
        .await
        .expect("inode revisions");
    assert_eq!(inode_revisions.revisions.len(), 2);

    restore_file_revision(
        &store,
        &namespace_id,
        "/docs/moved.txt",
        RevisionNo(1),
        &context,
        Some("rev-restore"),
    )
    .await
    .expect("restore first revision");
    let restored = read_file_bytes(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("read restored");
    assert_eq!(restored.bytes, b"one");
    let restored_revisions = list_file_revisions_for_inode(&store, &namespace_id, inode_id)
        .await
        .expect("restored revisions");
    assert_eq!(
        restored_revisions
            .revisions
            .iter()
            .map(|revision| revision.revision_no)
            .collect::<Vec<_>>(),
        vec![RevisionNo(3), RevisionNo(2), RevisionNo(1)]
    );
}

#[tokio::test]
async fn batch_commit_writes_one_segment_and_expands_change_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("req-batch-a").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: test_display_name("alpha"),
                }],
                message: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("req-batch-b").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: test_display_name("beta"),
                }],
                message: None,
            },
        ],
        &context,
    )
    .await;
    let first = responses[0].as_ref().expect("first commit");
    let second = responses[1].as_ref().expect("second commit");
    assert_eq!(first.committed_seq, ChangeSeq(1));
    assert_eq!(second.committed_seq, ChangeSeq(2));

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.start_seq, ChangeSeq(1));
    assert_eq!(segment.payload.end_seq, ChangeSeq(2));
    assert_eq!(segment.payload.records.len(), 2);
    assert_eq!(segment.payload.records[0].deltas.len(), 2);
    assert_eq!(segment.payload.records[0].deltas[0].semantic_op_index, 0);
    assert_eq!(segment.payload.records[0].deltas[1].semantic_op_index, 0);
    match &segment.payload.records[0].deltas[1].delta {
        WalDelta::BindDirentry {
            name_key,
            display_name,
            ..
        } => {
            assert_eq!(name_key.as_str(), "alpha");
            assert_eq!(display_name.as_str(), "alpha");
        }
        delta => panic!("expected bind delta, got {delta:?}"),
    }
    store
        .put_if_absent(
            "namespaces/demo/wal/00000000000000000099-9999999999999999.wal.zst",
            wal_bytes,
        )
        .await
        .expect("write unreachable orphan");

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect("changes");
    assert_eq!(changes.changes.len(), 2);
    assert_eq!(
        changes.changes[0].commit_id,
        CommitId::parse("req-batch-a").expect("valid commit id")
    );
    assert_eq!(
        changes.changes[1].commit_id,
        CommitId::parse("req-batch-b").expect("valid commit id")
    );
    assert_eq!(changes.changes[0].deltas.len(), 2);
    assert!(matches!(
        &changes.changes[0].deltas[0],
        CommitDelta::CreateInode {
            semantic_op_index: 0,
            delta_index: 0,
            inode_kind: InodeKind::Directory,
            ..
        }
    ));
    assert!(matches!(
        &changes.changes[0].deltas[1],
        CommitDelta::BindDirentry {
            semantic_op_index: 0,
            delta_index: 1,
            name_key,
            display_name,
            ..
        } if name_key.as_str() == "alpha" && display_name.as_str() == "alpha"
    ));
}

#[tokio::test]
async fn wire_name_key_stays_typed_through_planning_and_fingerprint() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let request: ApiCommitRequest = serde_json::from_slice(
        br#"{
            "commit_id":"typed-name-key",
            "preconditions":[{
                "kind":"child_name_absent",
                "parent_inode_id":1,
                "name_key":"caf\u00e9"
            }],
            "ops":[{
                "kind":"create_directory",
                "parent_inode_id":1,
                "display_name":"Caf\u00e9"
            }],
            "message":"typed boundary"
        }"#,
    )
    .expect("deserialize wire commit request");
    let expected_name_key = NameKey::parse("café").expect("valid name key");
    assert!(matches!(
        request.preconditions.as_slice(),
        [CommitPrecondition::ChildNameAbsent { name_key, .. }]
            if name_key == &expected_name_key
    ));
    let expected_fingerprint = core_commit_fingerprint_for_v0_request(&namespace_id, &request)
        .expect("fingerprint wire request");

    commit_operations(&store, &namespace_id, request, &context)
        .await
        .expect("commit typed name key");

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);
    assert_eq!(
        segment.payload.records[0].semantic_commit_fingerprint,
        expected_fingerprint.as_str()
    );
    assert!(segment.payload.records[0]
        .deltas
        .iter()
        .any(|delta| matches!(
            &delta.delta,
            WalDelta::BindDirentry { name_key, .. } if name_key == &expected_name_key
        )));
}

#[tokio::test]
async fn change_feed_validates_wal_chain_before_current_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &namespace_id, "/docs", &context, None)
        .await
        .expect("create docs");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    store
        .put_overwrite(&wal_keys[0], Bytes::from_static(b"not a wal segment"))
        .await
        .expect("corrupt wal");

    resolve_path(&store, &namespace_id, "/docs")
        .await
        .expect("checkpoint-backed read should not read pre-checkpoint wal");
    let error = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect_err("corrupt wal chain");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
}

#[tokio::test]
async fn binding_is_precondition_observes_earlier_batch_candidate() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/readme.txt",
        b"hello",
        &context,
        Some("seed-child-name-is"),
    )
    .await
    .expect("seed file");
    let docs_inode = resolve_path(&store, &namespace_id, "/docs")
        .await
        .expect("resolve docs")
        .inode_id;
    let file_inode = resolve_path(&store, &namespace_id, "/docs/readme.txt")
        .await
        .expect("resolve file")
        .inode_id;
    let original_binding =
        latest_binding_for_child_from_change_feed(&store, &namespace_id, file_inode).await;

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("move-before-child-name-check")
                    .expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::Rename {
                    inode_id: file_inode,
                    new_parent_inode_id: docs_inode,
                    new_display_name: test_display_name("moved.txt"),
                }],
                message: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("delete-with-stale-binding").expect("valid commit id"),
                preconditions: vec![CommitPrecondition::BindingIs {
                    parent_inode_id: docs_inode,
                    name_key: original_binding.name_key.clone(),
                    child_inode_id: file_inode,
                    bind_seq: original_binding.bind_seq,
                    bind_delta_index: original_binding.bind_delta_index,
                }],
                ops: vec![ApiCommitOp::DeleteFile {
                    inode_id: file_inode,
                }],
                message: None,
            },
        ],
        &context,
    )
    .await;

    assert_eq!(
        responses[0].as_ref().expect("rename").committed_seq,
        ChangeSeq(2)
    );
    let error = responses[1]
        .as_ref()
        .expect_err("stale binding precondition");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn directory_empty_precondition_observes_earlier_batch_candidate() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    commit_operations(
        &store,
        &namespace_id,
        ApiCommitRequest {
            commit_id: CommitId::parse("seed-empty-dir").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: test_display_name("docs"),
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect("seed docs");
    let docs_inode = resolve_path(&store, &namespace_id, "/docs")
        .await
        .expect("resolve seeded directory")
        .inode_id;
    let content = store_bytes_as_content(&store, &namespace_id, b"child")
        .await
        .expect("stage content");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("create-child-before-empty-check")
                    .expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateFile {
                    parent_inode_id: docs_inode,
                    display_name: test_display_name("child.txt"),
                    content_ref: content.content_ref,
                }],
                message: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("delete-dir-with-stale-empty-check")
                    .expect("valid commit id"),
                preconditions: vec![CommitPrecondition::DirectoryEmpty {
                    inode_id: docs_inode,
                }],
                ops: vec![ApiCommitOp::DeleteSubtree {
                    root_inode_id: docs_inode,
                }],
                message: None,
            },
        ],
        &context,
    )
    .await;

    assert_eq!(
        responses[0].as_ref().expect("create child").committed_seq,
        ChangeSeq(2)
    );
    let error = responses[1]
        .as_ref()
        .expect_err("directory empty precondition");
    assert_eq!(error.code(), ErrorCode::DirectoryNotEmpty);
}

#[tokio::test]
async fn namespace_delete_is_terminal_for_reads_writes_creation_and_forks() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"will vanish")
        .await
        .expect("stage content");
    submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::PutFile {
            commit_id: CommitId::parse("before-delete").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/keep.txt").expect("path"),
            content_ref: content.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("commit before delete");

    // A stale precondition deletes nothing.
    let engine = namespace_engine(&store, &namespace_id, &context);
    let stale = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions {
            expected_head_seq: Some(ChangeSeq(0)),
        })
        .await
        .expect_err("stale precondition");
    assert_eq!(stale.code(), ErrorCode::StaleHead);

    let response = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    assert_eq!(response.head_seq, ChangeSeq(1));

    // Terminal: reads, commits, status, repeat deletes, re-creation, and forks
    // all observe the deleted head.
    let read = resolve_path(&store, &namespace_id, "/")
        .await
        .expect_err("read after delete");
    assert_eq!(read.code(), ErrorCode::NamespaceDeleted);
    let commit = submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::PutFile {
            commit_id: CommitId::parse("after-delete").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/late.txt").expect("path"),
            content_ref: content.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect_err("commit after delete");
    assert_eq!(commit.code(), ErrorCode::NamespaceDeleted);
    let again = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect_err("repeat delete");
    assert_eq!(again.code(), ErrorCode::NamespaceDeleted);
    let recreate = bootstrap_namespace(&store, &namespace_id, &context, false).await;
    assert!(matches!(
        recreate,
        Err(loonfs_core::BootstrapNamespaceError::NamespaceDeleted { .. })
    ));
    let fork_target = NamespaceId::parse("fork-of-deleted").expect("valid namespace id");
    let fork = fork_namespace(&store, &namespace_id, &fork_target, &context).await;
    assert_eq!(
        fork.expect_err("fork of deleted source").code(),
        ErrorCode::NamespaceDeleted
    );
}

#[tokio::test]
async fn fork_clone_survives_source_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let source = NamespaceId::parse("source").expect("valid namespace id");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&store, &source, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &source, b"shared bytes")
        .await
        .expect("stage content");
    submit_intent_async(
        &store,
        &source,
        PathMutationIntent::PutFile {
            commit_id: CommitId::parse("seed-clone").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/shared.txt").expect("path"),
            content_ref: content.content_ref,
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("seed source");
    fork_namespace(&store, &source, &clone, &context)
        .await
        .expect("fork");

    namespace_engine(&store, &source, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete source");

    // The spec promise: the clone keeps reading through the source-owned
    // immutable metadata its manifest pins.
    let clone_head = load_namespace_head_control(&store, &clone)
        .await
        .expect("clone head survives source delete");
    assert_eq!(clone_head.state.seq, ChangeSeq(1));
    resolve_path(&store, &clone, "/shared.txt")
        .await
        .expect("clone reads forked file");
}

#[tokio::test]
async fn ack_lost_head_cas_reports_unknown_outcome_and_replays_idempotently() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = ack_lost_head_cas_store(temp_dir.path(), &namespace_id);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"ack lost")
        .await
        .expect("stage content");
    let intent = || PathMutationIntent::PutFile {
        commit_id: CommitId::parse("ack-lost-put").expect("valid commit id"),
        absolute_path: AbsolutePath::parse("/ack.txt").expect("path"),
        content_ref: content.content_ref.clone(),
        behavior: DestinationBehavior::NoReplace,
    };

    // The CAS landed but its acknowledgment was lost: this must surface as
    // an unknown outcome, never as definite failure.
    let error = submit_intent_async(&store, &namespace_id, intent(), &context)
        .await
        .expect_err("ack-lost head CAS is not definite failure");
    assert_eq!(error.code(), ErrorCode::CommitOutcomeUnknown);
    assert!(store.injected_ack_loss());

    // The documented remedy: retry with the same commit id. The commit is
    // already visible, so the retry replays it instead of double-committing.
    let result = submit_intent_async(&store, &namespace_id, intent(), &context)
        .await
        .expect("same-commit-id retry replays the committed mutation");
    assert_eq!(result.committed_seq, ChangeSeq(1));

    let head = load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.state.seq, ChangeSeq(1));
}

#[tokio::test]
async fn retry_succeeds_after_wal_orphaned_by_stale_head_cas() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = StaleHeadAfterWalWriteStore::new(temp_dir.path(), &namespace_id);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"retry")
        .await
        .expect("stage content");
    let intent = PathMutationIntent::PutFile {
        commit_id: CommitId::parse("retry-after-orphan").expect("valid commit id"),
        absolute_path: AbsolutePath::parse("/retry.txt").expect("path"),
        content_ref: content.content_ref,
        behavior: DestinationBehavior::NoReplace,
    };
    let error = submit_intent_async(&store, &namespace_id, intent.clone(), &context)
        .await
        .expect_err("injected stale head surfaces to the caller");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    assert!(store.injected_stale_head());

    // The orphaned segment from the failed attempt must not block a retry
    // with the same commit id.
    let result = submit_intent_async(&store, &namespace_id, intent, &context)
        .await
        .expect("retry after the orphaned segment");
    assert_eq!(result.committed_seq, ChangeSeq(1));

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 2);

    let head = load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.state.seq, ChangeSeq(1));
    let visible_tip = head
        .state
        .visible_wal_tip
        .as_ref()
        .expect("visible wal tip");
    assert!(wal_keys.contains(&visible_tip.object_key));
    let orphan_keys = wal_keys
        .iter()
        .filter(|key| *key != &visible_tip.object_key)
        .collect::<Vec<_>>();
    assert_eq!(orphan_keys.len(), 1);

    let visible_wal = store
        .get(&visible_tip.object_key, None)
        .await
        .expect("read visible wal")
        .expect("visible wal exists");
    let visible_segment =
        decode_wal_segment_envelope_zstd(&visible_wal).expect("decode visible segment");
    assert_eq!(visible_segment.payload.start_seq, ChangeSeq(1));
    assert_eq!(visible_segment.payload.end_seq, ChangeSeq(1));
    assert_eq!(visible_segment.payload.records.len(), 1);
    assert_eq!(
        visible_segment.payload.records[0].commit_id,
        CommitId::parse("retry-after-orphan").expect("valid commit id")
    );

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(
        changes.changes[0].commit_id,
        CommitId::parse("retry-after-orphan").expect("valid commit id")
    );
}

/// A batch where the WAL write fails must report that failure for every
/// outcome that depended on the batch publishing: the accepted candidate and
/// the rejection decided against its speculative in-batch state. Before this
/// contract the speculative rejection surfaced as a definitive `PathConflict`
/// derived from a create that never became durable, so its client gave up
/// instead of retrying. Rejections decided against the durable materialization alone
/// stand regardless of the batch outcome.
#[tokio::test]
async fn failed_wal_write_fails_rejections_decided_against_in_batch_state() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix("namespaces/demo/wal/segments/".to_owned()),
        InjectedCreateFailure::Transport {
            message: "injected wal write failure",
        },
    );
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"batch bytes")
        .await
        .expect("stage content");

    let batch = || async {
        vec![
            // Rejected against the durable materialization: nothing was accepted yet.
            NamespaceMutationCandidate::path(PathMutationIntent::DeletePath {
                commit_id: CommitId::parse("reject-materialization").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/missing.txt").expect("path"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            }),
            // Accepted into the batch.
            admitted_candidate(
                &store,
                &namespace_id,
                PathMutationIntent::PutFile {
                    commit_id: CommitId::parse("accept-a").expect("valid commit id"),
                    absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    content_ref: content.content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                },
            )
            .await,
            // Rejected only because of the accepted candidate's speculative
            // in-batch create.
            admitted_candidate(
                &store,
                &namespace_id,
                PathMutationIntent::PutFile {
                    commit_id: CommitId::parse("reject-speculative").expect("valid commit id"),
                    absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    content_ref: content.content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                },
            )
            .await,
            // Alias of the materialization-decided rejection.
            NamespaceMutationCandidate::path(PathMutationIntent::DeletePath {
                commit_id: CommitId::parse("reject-materialization").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/missing.txt").expect("path"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            }),
        ]
    };

    let failed =
        publish_namespace_mutations_batch(&store, &namespace_id, batch().await, &context).await;

    let materialization_rejection = failed[0]
        .as_ref()
        .expect_err("materialization-decided rejection");
    assert_eq!(materialization_rejection.code(), ErrorCode::PathNotFound);
    let accepted = failed[1].as_ref().expect_err("accepted candidate fails");
    assert!(matches!(accepted, CoreError::WalWrite { .. }));
    let speculative = failed[2].as_ref().expect_err("speculative rejection");
    assert!(
        matches!(speculative, CoreError::WalWrite { .. }),
        "rejection decided against unpublished in-batch state must take the \
         batch error, got {speculative:?}"
    );
    let alias = failed[3].as_ref().expect_err("alias mirrors its primary");
    assert_eq!(alias.code(), ErrorCode::PathNotFound);

    // Nothing became durable, so the retry the batch error asks for derives
    // every verdict from durable state.
    let head = load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.state.seq, ChangeSeq(0));

    let retried =
        publish_namespace_mutations_batch(&store, &namespace_id, batch().await, &context).await;
    assert_eq!(
        retried[0].as_ref().expect_err("still missing").code(),
        ErrorCode::PathNotFound
    );
    let committed = retried[1].as_ref().expect("create lands on retry");
    assert_eq!(committed.committed_seq, ChangeSeq(1));
    assert_eq!(
        retried[2]
            .as_ref()
            .expect_err("conflict against durably published state")
            .code(),
        ErrorCode::PathConflict
    );
    assert_eq!(
        retried[3].as_ref().expect_err("still missing").code(),
        ErrorCode::PathNotFound
    );
}

/// Same contract when the batch dies at the head CAS instead of the WAL
/// write: the stale-head error replaces the rejection decided against the
/// accepted candidate's speculative state, while the materialization-decided rejection
/// stands.
#[tokio::test]
async fn stale_head_cas_fails_rejections_decided_against_in_batch_state() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = StaleHeadAfterWalWriteStore::new(temp_dir.path(), &namespace_id);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"batch bytes")
        .await
        .expect("stage content");

    let batch = || async {
        vec![
            NamespaceMutationCandidate::path(PathMutationIntent::DeletePath {
                commit_id: CommitId::parse("reject-materialization").expect("valid commit id"),
                absolute_path: AbsolutePath::parse("/missing.txt").expect("path"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            }),
            admitted_candidate(
                &store,
                &namespace_id,
                PathMutationIntent::PutFile {
                    commit_id: CommitId::parse("accept-a").expect("valid commit id"),
                    absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    content_ref: content.content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                },
            )
            .await,
            admitted_candidate(
                &store,
                &namespace_id,
                PathMutationIntent::PutFile {
                    commit_id: CommitId::parse("reject-speculative").expect("valid commit id"),
                    absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    content_ref: content.content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                },
            )
            .await,
        ]
    };

    let failed =
        publish_namespace_mutations_batch(&store, &namespace_id, batch().await, &context).await;
    assert!(store.injected_stale_head());

    assert_eq!(
        failed[0]
            .as_ref()
            .expect_err("materialization-decided rejection")
            .code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        failed[1]
            .as_ref()
            .expect_err("accepted candidate fails")
            .code(),
        ErrorCode::StaleHead
    );
    let speculative = failed[2].as_ref().expect_err("speculative rejection");
    assert_eq!(
        speculative.code(),
        ErrorCode::StaleHead,
        "rejection decided against unpublished in-batch state must take the \
         batch error, got {speculative:?}"
    );

    let retried =
        publish_namespace_mutations_batch(&store, &namespace_id, batch().await, &context).await;
    assert_eq!(
        retried[0].as_ref().expect_err("still missing").code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        retried[1]
            .as_ref()
            .expect("create lands on retry")
            .committed_seq,
        ChangeSeq(1)
    );
    assert_eq!(
        retried[2]
            .as_ref()
            .expect_err("conflict against durably published state")
            .code(),
        ErrorCode::PathConflict
    );
}

#[tokio::test]
async fn retry_succeeds_after_stale_head_get_during_publish_view_load() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = StaleHeadGetStore::new(temp_dir.path(), &namespace_id);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    create_directory_path(
        &store,
        &namespace_id,
        "/parent",
        &context,
        Some("mkdir-parent"),
    )
    .await
    .expect("create parent");
    put_file_bytes(
        &store,
        &namespace_id,
        "/file.txt",
        b"first contents",
        DestinationBehavior::NoReplace,
        &context,
        Some("write-first"),
    )
    .await
    .expect("write first revision");
    put_file_bytes(
        &store,
        &namespace_id,
        "/file.txt",
        b"second contents win",
        DestinationBehavior::Replace,
        &context,
        Some("write-second"),
    )
    .await
    .expect("write second revision");
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/file.txt")
            .await
            .expect("read before stale get")
            .bytes,
        b"second contents win"
    );

    store.inject_stale_head_get_after(1);
    let error = create_directory_path(
        &store,
        &namespace_id,
        "/parent/child",
        &context,
        Some("mkdir-child"),
    )
    .await
    .expect_err("injected stale head read surfaces to the caller");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    assert!(store.injected_stale_head_get());

    // The failed attempt must not block a retry with the same commit id.
    let result = create_directory_path(
        &store,
        &namespace_id,
        "/parent/child",
        &context,
        Some("mkdir-child"),
    )
    .await
    .expect("retry after the stale head read");
    assert_eq!(result.committed_seq, ChangeSeq(4));
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/file.txt")
            .await
            .expect("read after stale get")
            .bytes,
        b"second contents win"
    );
    resolve_path(&store, &namespace_id, "/parent/child")
        .await
        .expect("child directory remains visible");

    let head = load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.state.seq, ChangeSeq(4));
}

#[tokio::test]
async fn batch_commit_aliases_duplicate_commit_id_with_same_fingerprint() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let request = ApiCommitRequest {
        commit_id: CommitId::parse("req-duplicate").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![ApiCommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: test_display_name("alpha"),
        }],
        message: None,
    };

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![request.clone(), request],
        &context,
    )
    .await;
    let first = responses[0].as_ref().expect("primary commit");
    let duplicate = responses[1].as_ref().expect("duplicate commit");
    assert_eq!(first, duplicate);

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(
        changes.changes[0].commit_id,
        CommitId::parse("req-duplicate").expect("valid commit id")
    );
}

#[tokio::test]
async fn oversized_prepared_proof_candidate_replays_receipt_but_new_request_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stored = store_bytes_as_content(&store, &namespace_id, b"proof replay")
        .await
        .expect("store content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = prepare_existing_content_ref(&store, &catalog, stored.content_ref.clone())
        .await
        .expect("prepare content");
    let request = ApiCommitRequest {
        commit_id: CommitId::parse("over-proof-replay").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![ApiCommitOp::CreateFile {
            parent_inode_id: InodeId(1),
            display_name: test_display_name("proof-replay.txt"),
            content_ref: stored.content_ref,
        }],
        message: None,
    };

    let original = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![NamespaceMutationCandidate::commit_prepared(
            request.clone(),
            vec![prepared.clone()],
        )],
        &context,
    )
    .await
    .remove(0)
    .expect("land original commit");
    let oversized_proofs = vec![prepared; loonfs_core::limits::MAX_COMMIT_CONTENT_TOKENS + 1];

    let replay = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![NamespaceMutationCandidate::commit_prepared(
            request.clone(),
            oversized_proofs.clone(),
        )],
        &context,
    )
    .await
    .remove(0)
    .expect("durable receipt must win over current proof limit");
    assert_eq!(replay, original);

    let mut new_request = request;
    new_request.commit_id = CommitId::parse("over-proof-new").expect("valid commit id");
    let error = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![NamespaceMutationCandidate::commit_prepared(
            new_request,
            oversized_proofs,
        )],
        &context,
    )
    .await
    .remove(0)
    .expect_err("new over-proof request must be rejected after receipt miss");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn same_batch_over_limit_proof_duplicate_joins_its_primary() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stored = store_bytes_as_content(&store, &namespace_id, b"batch proof")
        .await
        .expect("store content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = prepare_existing_content_ref(&store, &catalog, stored.content_ref.clone())
        .await
        .expect("prepare content");
    let request = ApiCommitRequest {
        commit_id: CommitId::parse("over-proof-duplicate").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![ApiCommitOp::CreateFile {
            parent_inode_id: InodeId(1),
            display_name: test_display_name("batch-proof.txt"),
            content_ref: stored.content_ref,
        }],
        message: None,
    };

    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![
            NamespaceMutationCandidate::commit_prepared(request.clone(), vec![prepared.clone()]),
            NamespaceMutationCandidate::commit_prepared(
                request,
                vec![prepared; loonfs_core::limits::MAX_COMMIT_CONTENT_TOKENS + 1],
            ),
        ],
        &context,
    )
    .await;

    let primary = responses[0].as_ref().expect("primary commit");
    let duplicate = responses[1]
        .as_ref()
        .expect("over-limit duplicate must join primary");
    assert_eq!(duplicate, primary);
}

#[tokio::test]
async fn new_candidate_with_4097_operations_is_rejected_after_identity_computation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let candidate = NamespaceMutationCandidate::commit(ApiCommitRequest {
        commit_id: CommitId::parse("over-operation-new").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: (0..=loonfs_core::limits::MAX_COMMIT_OPERATIONS)
            .map(|index| ApiCommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: test_display_name(format!("over-operation-{index}")),
            })
            .collect(),
        message: None,
    });

    // A later release may lower the operation limit below a durable request;
    // identity must remain available so receipt resolution can win first.
    candidate
        .semantic_identity(&namespace_id)
        .expect("current request limits must not affect identity");
    let error = publish_namespace_mutations_batch(&store, &namespace_id, vec![candidate], &context)
        .await
        .remove(0)
        .expect_err("new over-operation request must be rejected");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn visible_commit_id_retry_aliases_across_writer_takeover() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer_a = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &writer_a, false)
        .await
        .expect("bootstrap");

    let request = ApiCommitRequest {
        commit_id: CommitId::parse("retry-across-writer").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![ApiCommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: test_display_name("alpha"),
        }],
        message: None,
    };

    let first = commit_operations(&store, &namespace_id, request.clone(), &writer_a)
        .await
        .expect("writer a commit");
    let writer_b = MutationContext {
        writer_id: "writer-b".to_owned(),
        writer_session_id: "wrs-writer-b".to_owned(),
        writer_version: "writer-b/0.1.0".to_owned(),
        now_ms: writer_a.now_ms.saturating_add(1),
    };

    let retry = commit_operations(&store, &namespace_id, request, &writer_b)
        .await
        .expect("writer b retry");

    assert_eq!(first, retry);
}

#[tokio::test]
async fn batch_commit_rejects_duplicate_commit_id_with_different_fingerprint() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let responses = commit_operations_batch(
        &store,
        &namespace_id,
        vec![
            ApiCommitRequest {
                commit_id: CommitId::parse("req-conflict").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: test_display_name("alpha"),
                }],
                message: None,
            },
            ApiCommitRequest {
                commit_id: CommitId::parse("req-conflict").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![ApiCommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: test_display_name("beta"),
                }],
                message: None,
            },
        ],
        &context,
    )
    .await;

    responses[0].as_ref().expect("primary commit");
    let error = responses[1].as_ref().expect_err("duplicate conflict");
    assert!(matches!(
        error,
        CoreError::CommitIdReuseConflict(commit_id) if commit_id == "req-conflict"
    ));

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect("changes");
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(
        changes.changes[0].commit_id,
        CommitId::parse("req-conflict").expect("valid commit id")
    );
}

#[tokio::test]
async fn explicit_commit_rejects_invalid_display_names() {
    assert!(DisplayName::parse("a/b").is_err());
    assert!(DisplayName::parse(".").is_err());
}

#[tokio::test]
async fn path_intents_cover_basic_mutations() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let content = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("stage content");
    let put = submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::PutFile {
            commit_id: CommitId::parse("put-path").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            content_ref: content.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("put path");
    assert_eq!(put.committed_seq, ChangeSeq(1));

    let moved = submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::MovePath {
            commit_id: CommitId::parse("move-path").expect("valid commit id"),
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("move path");
    assert_eq!(moved.committed_seq, ChangeSeq(2));

    let copied = submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::CopyFilePath {
            commit_id: CommitId::parse("copy-path").expect("valid commit id"),
            from_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/c.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("copy path");
    assert_eq!(copied.committed_seq, ChangeSeq(3));

    let deleted = submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::DeletePath {
            commit_id: CommitId::parse("delete-path").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        &context,
    )
    .await
    .expect("delete path");
    assert_eq!(deleted.committed_seq, ChangeSeq(4));

    let copied_bytes = read_file_bytes(&store, &namespace_id, "/docs/c.txt")
        .await
        .expect("read copied file");
    assert_eq!(copied_bytes.bytes, b"hello");
}

#[tokio::test]
async fn path_publishes_use_durable_path_commit_receipt_index() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("stage content");

    let intent = PathMutationIntent::PutFile {
        commit_id: CommitId::parse("same-path-request").expect("valid commit id"),
        absolute_path: AbsolutePath::parse("/same//path.txt").expect("path"),
        content_ref: content.content_ref.clone(),
        behavior: DestinationBehavior::NoReplace,
    };
    let first = submit_intent_async(&store, &namespace_id, intent.clone(), &context)
        .await
        .expect("first publish");
    let retry = submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::PutFile {
            commit_id: CommitId::parse("same-path-request").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/same/path.txt").expect("path"),
            content_ref: content.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect("idempotent retry");
    assert_eq!(retry.committed_seq, first.committed_seq);

    let conflict = submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::DeletePath {
            commit_id: CommitId::parse("same-path-request").expect("valid commit id"),
            absolute_path: AbsolutePath::parse("/same/path.txt").expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        &context,
    )
    .await
    .expect_err("conflicting retry");
    assert!(matches!(
        conflict,
        CoreError::CommitIdReuseConflict(commit_id) if commit_id == "same-path-request"
    ));

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
}

#[tokio::test]
async fn delete_path_commit_id_reuse_includes_expected_inode_id() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let seeded_paths = [
        ("/same-guard.txt", "seed-same-guard"),
        ("/changed-guard.txt", "seed-changed-guard"),
        ("/removed-guard.txt", "seed-removed-guard"),
        ("/added-guard.txt", "seed-added-guard"),
    ];
    for (path, commit_id) in seeded_paths {
        write_file_bytes(
            &store,
            &namespace_id,
            path,
            b"hello",
            &context,
            Some(commit_id),
        )
        .await
        .expect("seed guarded delete target");
    }

    let same_inode = resolve_path(&store, &namespace_id, "/same-guard.txt")
        .await
        .expect("resolve same-guard target")
        .inode_id;
    let first = delete_path_non_recursive_expecting(
        &store,
        &namespace_id,
        "/same-guard.txt",
        Some(same_inode),
        &context,
        "delete-same-guard",
    )
    .await
    .expect("first guarded delete");
    let retry = delete_path_non_recursive_expecting(
        &store,
        &namespace_id,
        "/same-guard.txt",
        Some(same_inode),
        &context,
        "delete-same-guard",
    )
    .await
    .expect("identical guarded delete retry");
    assert_eq!(retry, first);

    let changed_inode = resolve_path(&store, &namespace_id, "/changed-guard.txt")
        .await
        .expect("resolve changed-guard target")
        .inode_id;
    let removed_inode = resolve_path(&store, &namespace_id, "/removed-guard.txt")
        .await
        .expect("resolve removed-guard target")
        .inode_id;
    let added_inode = resolve_path(&store, &namespace_id, "/added-guard.txt")
        .await
        .expect("resolve added-guard target")
        .inode_id;
    let conflicts = [
        (
            "delete-changed-guard",
            "/changed-guard.txt",
            Some(changed_inode),
            Some(InodeId(1)),
        ),
        (
            "delete-removed-guard",
            "/removed-guard.txt",
            Some(removed_inode),
            None,
        ),
        (
            "delete-added-guard",
            "/added-guard.txt",
            None,
            Some(added_inode),
        ),
    ];
    for (commit_id, path, first_guard, retry_guard) in conflicts {
        delete_path_non_recursive_expecting(
            &store,
            &namespace_id,
            path,
            first_guard,
            &context,
            commit_id,
        )
        .await
        .expect("first delete");

        let error = delete_path_non_recursive_expecting(
            &store,
            &namespace_id,
            path,
            retry_guard,
            &context,
            commit_id,
        )
        .await
        .expect_err("changed guard must conflict");
        assert!(matches!(
            error,
            CoreError::CommitIdReuseConflict(reused) if reused == commit_id
        ));
    }

    let head = load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.state.seq, ChangeSeq(8));
}

#[tokio::test]
async fn fresh_delete_path_expected_inode_guard_still_matches_or_rejects() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/matching-guard.txt",
        b"matching",
        &context,
        Some("seed-matching-guard"),
    )
    .await
    .expect("seed matching target");
    write_file_bytes(
        &store,
        &namespace_id,
        "/mismatching-guard.txt",
        b"mismatching",
        &context,
        Some("seed-mismatching-guard"),
    )
    .await
    .expect("seed mismatching target");
    let matching_inode = resolve_path(&store, &namespace_id, "/matching-guard.txt")
        .await
        .expect("resolve matching target")
        .inode_id;
    let mismatching_inode = resolve_path(&store, &namespace_id, "/mismatching-guard.txt")
        .await
        .expect("resolve mismatching target")
        .inode_id;

    delete_path_non_recursive_expecting(
        &store,
        &namespace_id,
        "/matching-guard.txt",
        Some(matching_inode),
        &context,
        "delete-matching-guard",
    )
    .await
    .expect("matching guard deletes");
    let missing = resolve_path(&store, &namespace_id, "/matching-guard.txt")
        .await
        .expect_err("matching target is deleted");
    assert_eq!(missing.code(), ErrorCode::PathNotFound);

    let error = delete_path_non_recursive_expecting(
        &store,
        &namespace_id,
        "/mismatching-guard.txt",
        Some(InodeId(1)),
        &context,
        "delete-mismatching-guard",
    )
    .await
    .expect_err("mismatching guard must fail planning");
    assert!(matches!(
        error,
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_child_inode_id: InodeId(1),
            actual_child_inode_id: actual,
            ..
        }) if actual == mismatching_inode
    ));
    assert_eq!(
        resolve_path(&store, &namespace_id, "/mismatching-guard.txt")
            .await
            .expect("mismatching target remains")
            .inode_id,
        mismatching_inode
    );
}

#[tokio::test]
async fn path_intents_in_one_batch_see_tentative_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("stage content");

    let responses = publish_namespace_mutations_batch(
        &store,
        &namespace_id,
        vec![
            admitted_candidate(
                &store,
                &namespace_id,
                PathMutationIntent::PutFile {
                    commit_id: CommitId::parse("put-batched-path").expect("valid commit id"),
                    absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    content_ref: content.content_ref,
                    behavior: DestinationBehavior::NoReplace,
                },
            )
            .await,
            NamespaceMutationCandidate::path(PathMutationIntent::MovePath {
                commit_id: CommitId::parse("move-batched-path").expect("valid commit id"),
                from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                behavior: DestinationBehavior::NoReplace,
            }),
        ],
        &context,
    )
    .await;

    assert_eq!(
        responses[0].as_ref().expect("put").committed_seq,
        ChangeSeq(1)
    );
    assert_eq!(
        responses[1].as_ref().expect("move").committed_seq,
        ChangeSeq(2)
    );
    let moved_bytes = read_file_bytes(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("read moved file");
    assert_eq!(moved_bytes.bytes, b"hello");

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 2);
}

#[tokio::test]
async fn namespace_descriptor_checksum_is_validated() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let descriptor_key = namespace_config(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .await
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    let descriptor: NamespaceConfigEnvelope =
        decode_control_object(&descriptor_bytes, ControlObjectKind::NamespaceConfig)
            .expect("decode namespace descriptor");
    // Corrupt the durable document at the byte level: swap the stored
    // checksum for a syntactically valid but wrong digest.
    let corrupted = String::from_utf8(descriptor_bytes.to_vec())
        .expect("descriptor is utf8")
        .replace(
            &descriptor.payload_checksum,
            &sha256_digest(b"not-the-payload"),
        );
    store
        .put_overwrite(&descriptor_key, Bytes::from(corrupted))
        .await
        .expect("overwrite descriptor");

    let error = resolve_path(&store, &namespace_id, "/")
        .await
        .expect_err("descriptor checksum");
    assert!(
        error.to_string().contains("checksum mismatch"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn fork_namespace_reuses_content_store_and_isolates_metadata() {
    let temp_dir = tempdir().expect("tempdir");
    let store = metadata_table_counting_store(temp_dir.path());
    let context = mutation_context();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    bootstrap_namespace(&store, &source_namespace_id, &context, false)
        .await
        .expect("bootstrap source namespace");
    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"base",
        &context,
        Some("seed-shared"),
    )
    .await
    .expect("seed shared file");
    namespace_engine(&store, &source_namespace_id, &context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint before fork");

    let source_head = load_namespace_head_control(&store, &source_namespace_id)
        .await
        .expect("source head");
    assert_eq!(source_head.state.seq, ChangeSeq(1));
    let content_store_id = load_namespace_descriptor_state(&store, &source_namespace_id)
        .await
        .content_store_id;
    let blobs_before = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .await
        .expect("list blobs before fork");

    store.reset();
    fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect("fork namespace");
    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "fork should validate manifest descriptors without loading metadata SST payloads"
    );

    let blobs_after = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .await
        .expect("list blobs after fork");
    assert_eq!(blobs_after, blobs_before, "fork must not copy content");

    let clone_descriptor = load_namespace_descriptor_state(&store, &clone_namespace_id).await;
    assert_eq!(clone_descriptor.content_store_id, content_store_id);
    let clone_head = load_namespace_head_control(&store, &clone_namespace_id)
        .await
        .expect("clone head");
    assert_eq!(clone_head.state.seq, ChangeSeq(1));
    let clone_root =
        loonfs_core::control::load_namespace_metadata_root_control(&store, &clone_namespace_id)
            .await
            .expect("clone metadata root");
    assert_eq!(clone_root.state.manifest_id, ManifestId(1));
    let clone_floor =
        loonfs_core::control::load_namespace_wal_floor_control(&store, &clone_namespace_id)
            .await
            .expect("clone wal floor");
    assert_eq!(clone_floor.state.floor_seq, ChangeSeq(1));

    let target_manifest_key = metadata_manifest_object(
        clone_namespace_id.as_str(),
        &clone_root.state.manifest_object_id,
    );
    let target_manifest_bytes = store
        .get(&target_manifest_key, None)
        .await
        .expect("read target manifest")
        .expect("target manifest exists");
    let target_manifest =
        decode_namespace_manifest_json(&target_manifest_bytes).expect("decode target manifest");
    let fork_provenance = target_manifest
        .payload
        .fork
        .as_ref()
        .expect("fork provenance lives in target manifest");
    assert_eq!(fork_provenance.source_namespace_id, source_namespace_id);
    assert_eq!(fork_provenance.fork_seq, ChangeSeq(1));
    assert!(fork_provenance
        .source_checkpoint_id
        .as_str()
        .starts_with("chk_"));
    assert_eq!(fork_provenance.source_manifest_id, ManifestId(1));
    assert_eq!(fork_provenance.source_head_seq, ChangeSeq(1));
    let source_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source_namespace_id,
        &fork_provenance.source_checkpoint_id,
    )
    .await
    .expect("read source checkpoint record")
    .expect("source checkpoint record exists");
    assert_eq!(source_record.manifest_head_seq, ChangeSeq(1));
    assert_eq!(source_record.manifest_id, ManifestId(1));
    assert!(
        target_manifest
            .payload
            .metadata_files
            .iter()
            .all(|metadata_file| metadata_file.owner_namespace_id == source_namespace_id),
        "fork target manifest should reference source-owned metadata SSTs"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/metadata/tables/",
                clone_namespace_id.as_str()
            ))
            .await
            .expect("list target metadata SSTs")
            .is_empty(),
        "COW fork should not copy metadata SSTs into the target namespace"
    );

    assert!(
        matches!(
            &source_record.owner,
            CheckpointOwner::Fork { target_namespace_id }
                if *target_namespace_id == clone_namespace_id
        ),
        "fork record is owned by its target namespace"
    );
    let referenced_metadata_files = target_manifest
        .payload
        .metadata_files
        .iter()
        .map(|metadata_file| metadata_file.object_key.clone())
        .collect::<Vec<_>>();

    let duplicate_error =
        fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
            .await
            .expect_err("duplicate fork target");
    assert_eq!(duplicate_error.code(), ErrorCode::NamespaceExists);

    let source_entry = resolve_path(&store, &source_namespace_id, "/docs/shared.txt")
        .await
        .expect("source stat");
    let clone_entry = resolve_path(&store, &clone_namespace_id, "/docs/shared.txt")
        .await
        .expect("clone stat");
    assert_eq!(source_entry.content_ref, clone_entry.content_ref);
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone")
            .bytes,
        b"base"
    );
    let stale_clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(0))
        .await
        .expect_err("old cursor");
    assert_eq!(stale_clone_changes.code(), ErrorCode::RebootstrapRequired);
    let empty_clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(1))
        .await
        .expect("empty changes");
    assert!(empty_clone_changes.changes.is_empty());

    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"source-after-fork",
        &context,
        Some("source-after-fork"),
    )
    .await
    .expect("source replace");
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone after source write")
            .bytes,
        b"base"
    );

    let clone_write = write_file_bytes(
        &store,
        &clone_namespace_id,
        "/docs/shared.txt",
        b"clone-after-fork",
        &context,
        Some("clone-after-fork"),
    )
    .await
    .expect("clone replace");
    assert_eq!(clone_write.committed_seq, ChangeSeq(2));
    assert_eq!(
        read_file_bytes(&store, &source_namespace_id, "/docs/shared.txt")
            .await
            .expect("read source")
            .bytes,
        b"source-after-fork"
    );
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone")
            .bytes,
        b"clone-after-fork"
    );

    let clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(1))
        .await
        .expect("clone changes");
    assert_eq!(clone_changes.changes.len(), 1);
    assert_eq!(clone_changes.changes[0].seq, ChangeSeq(2));

    for prefix in [
        format!("namespaces/{}/wal/head.json", source_namespace_id.as_str()),
        format!("namespaces/{}/wal/", source_namespace_id.as_str()),
        format!(
            "namespaces/{}/metadata/manifests/",
            source_namespace_id.as_str()
        ),
    ] {
        for key in store
            .list_prefix(&prefix)
            .await
            .expect("list source mutable keys")
        {
            store.delete(&key).await.expect("delete source mutable key");
        }
    }
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("clone remains readable")
            .bytes,
        b"clone-after-fork"
    );

    let referenced_sst = referenced_metadata_files
        .first()
        .expect("fork should reference source metadata SST")
        .clone();
    store
        .delete(&referenced_sst)
        .await
        .expect("delete referenced source metadata SST");
    let corrupt_target = read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
        .await
        .expect_err("target should fail when referenced source SST is missing");
    assert_eq!(corrupt_target.code(), ErrorCode::NamespaceCorrupt);
}

#[tokio::test]
async fn fork_namespace_rejects_corrupt_source_manifest_descriptors() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    bootstrap_namespace(&store, &source_namespace_id, &context, false)
        .await
        .expect("bootstrap source namespace");
    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"base",
        &context,
        Some("seed-shared"),
    )
    .await
    .expect("seed shared file");
    let checkpoint = namespace_engine(&store, &source_namespace_id, &context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint");

    let source_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source_namespace_id,
        &checkpoint.checkpoint_id,
    )
    .await
    .expect("read source checkpoint record")
    .expect("source checkpoint record exists");
    let manifest_key = metadata_manifest_object(
        source_namespace_id.as_str(),
        &source_record.manifest_object_id,
    );
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read source manifest")
        .expect("source manifest exists");
    let mut manifest =
        decode_namespace_manifest_json(&manifest_bytes).expect("decode source manifest");
    manifest
        .payload
        .metadata_files
        .retain(|metadata_file| metadata_file.family != MetadataTableFamily::RevisionsByInodeDesc);
    let manifest =
        NamespaceManifestEnvelope::from_payload(manifest.writer_version, manifest.payload)
            .expect("rebuild manifest checksum");
    let corrupted = encode_namespace_manifest_json(&manifest).expect("encode corrupt manifest");
    store
        .put_overwrite(&manifest_key, Bytes::from(corrupted))
        .await
        .expect("overwrite source manifest");

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("corrupt source manifest should block fork");

    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head clone descriptor")
            .is_none(),
        "failed fork must not publish target descriptor"
    );
}

#[tokio::test]
async fn fork_target_head_reservation_failure_keeps_descriptor_unpublished() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(wal_head(clone_namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: Vec::new(),
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("target head precondition should re-check partial namespace");
    assert_eq!(error.code(), ErrorCode::NamespacePartial);
    assert!(
        !store
            .list_prefix(&format!(
                "namespaces/{}/metadata/manifests/",
                clone_namespace_id.as_str()
            ))
            .await
            .expect("list target manifests")
            .is_empty(),
        "target manifest should be written before target head reservation"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context).await;
    let retry = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("normal fork retry preserves the raced partial target");
    assert_eq!(retry.code(), ErrorCode::NamespacePartial);
    let report = repair_namespace(&store, &clone_namespace_id, &context)
        .await
        .expect("explicit repair completes the raced fork target");
    assert_eq!(report.outcome, RepairNamespaceOutcome::Completed);
}

#[tokio::test]
async fn fork_source_checkpoint_failure_leaves_target_namespace_absent() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix(format!(
            "namespaces/{}/checkpoints/",
            source_namespace_id.as_str()
        )),
        InjectedCreateFailure::Transport {
            message: "injected source checkpoint failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("source checkpoint failure should abort fork before target publication");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&wal_head(clone_namespace_id.as_str()))
            .await
            .expect("head target head")
            .is_none(),
        "target head must not be reserved before source retention is pinned"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head target descriptor")
            .is_none(),
        "target descriptor must remain unpublished"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/metadata/manifests/",
                clone_namespace_id.as_str()
            ))
            .await
            .expect("list target manifests")
            .is_empty(),
        "target manifest must not be written before source retention is pinned"
    );
    assert!(
        store
            .head(&namespace_config(source_namespace_id.as_str()))
            .await
            .expect("head source descriptor")
            .is_some(),
        "source descriptor should remain published"
    );
}

#[tokio::test]
async fn fork_target_manifest_failure_leaves_target_namespace_absent() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix(format!(
            "namespaces/{}/metadata/manifests/",
            clone_namespace_id.as_str()
        )),
        InjectedCreateFailure::Transport {
            message: "injected target manifest failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("target manifest write should fail");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&wal_head(clone_namespace_id.as_str()))
            .await
            .expect("head target head")
            .is_none(),
        "target head must not be reserved before target manifest exists"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/metadata/manifests/",
                clone_namespace_id.as_str()
            ))
            .await
            .expect("list target manifests")
            .is_empty(),
        "target manifest should not exist after injected manifest write failure"
    );
    assert!(
        store
            .head(&namespace_config(source_namespace_id.as_str()))
            .await
            .expect("head source descriptor")
            .is_some(),
        "source descriptor should remain published"
    );
}

#[tokio::test]
async fn fork_failure_after_target_head_before_descriptor_remains_partial() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_config(clone_namespace_id.as_str())),
        InjectedCreateFailure::Transport {
            message: "injected target descriptor failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("target descriptor write should fail");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&wal_head(clone_namespace_id.as_str()))
            .await
            .expect("head target head")
            .is_some(),
        "target head should still reserve namespace"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    let target_manifest_keys = store
        .list_prefix(&format!(
            "namespaces/{}/metadata/manifests/",
            clone_namespace_id.as_str()
        ))
        .await
        .expect("list target manifests");
    assert!(
        !target_manifest_keys.is_empty(),
        "target manifest should have been written before descriptor failure"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context).await;
}

#[tokio::test]
async fn fork_target_control_conflict_rechecks_complete_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_source_namespace_for_fork(&inner, &source_namespace_id, &context).await;
    let content_store_id = load_namespace_descriptor_state(&inner, &source_namespace_id)
        .await
        .content_store_id;
    let descriptor = NamespaceConfigEnvelope::from_state(
        ControlObjectKind::NamespaceConfig,
        &context.writer_version,
        NamespaceConfigState {
            namespace_id: clone_namespace_id.clone(),
            content_store_id,
            name_policy: loonfs_api::NamePolicy::default(),
        },
    )
    .expect("descriptor envelope");
    let store = InjectCreateFailureStore::new(
        inner,
        KeyMatcher::Exact(wal_head(clone_namespace_id.as_str())),
        InjectedCreateFailure::Conflict {
            write_attempted_object: true,
            additional_writes: vec![(
                namespace_config(clone_namespace_id.as_str()),
                loonfs_api::wire::control::encode_control_object(&descriptor)
                    .expect("descriptor bytes"),
            )],
        },
    );

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("target head conflict should re-check complete namespace");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    assert!(
        store
            .head(&namespace_config(source_namespace_id.as_str()))
            .await
            .expect("head source descriptor")
            .is_some(),
        "source descriptor should remain published"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head clone descriptor")
            .is_some(),
        "clone descriptor should be present for the simulated complete target"
    );
}

#[tokio::test]
async fn restore_revision_does_not_revalidate_retained_content_before_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    let first = store_bytes_as_content(&store, &namespace_id("demo"), b"first")
        .await
        .expect("stage first");
    commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-create").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode_id: InodeId(1),
                display_name: test_display_name("restore.txt"),
                content_ref: first.content_ref.clone(),
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect("create file");
    let inode_id = resolve_path(&store, &namespace_id("demo"), "/restore.txt")
        .await
        .expect("resolve created file")
        .inode_id;

    let second = store_bytes_as_content(&store, &namespace_id("demo"), b"second")
        .await
        .expect("stage second");
    commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-replace").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::ReplaceFile {
                inode_id,
                base_revision_no: RevisionNo(1),
                content_ref: second.content_ref,
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect("replace file");

    store
        .delete(
            &content_blob(first.content_store_id.as_str(), &first.content_ref.digest)
                .expect("first content key"),
        )
        .await
        .expect("delete first content");

    let response = commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-missing-content").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::RestoreRevision {
                inode_id,
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(2),
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect("restore trusts retained content metadata");
    assert_eq!(response.committed_seq, ChangeSeq(3));
}

#[tokio::test]
async fn metadata_only_commit_does_not_validate_content_store_refs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/delete-me.txt",
        b"hello",
        &context,
        Some("seed-metadata-only-delete"),
    )
    .await
    .expect("seed file");
    let inode_id = resolve_path(&store, &namespace_id("demo"), "/docs/delete-me.txt")
        .await
        .expect("resolve seeded file")
        .inode_id;

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 1);
    let response = commit_operations(
        &guarded_store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("metadata-only-delete").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::DeleteFile { inode_id }],
            message: None,
        },
        &context,
    )
    .await
    .expect("metadata-only delete should not perform content validation");

    assert_eq!(response.committed_seq, ChangeSeq(2));
    assert_eq!(
        guarded_store.content_store_access_count(),
        1,
        "materialization loading performs one content-store descriptor full read; metadata-only validation must not add another lookup",
    );
}

#[tokio::test]
async fn explicit_create_file_fails_unprepared_before_parent_validation_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 1);
    let missing_content = content_ref("missing-content");
    let error = publish_namespace_mutations_batch(
        &guarded_store,
        &namespace_id("demo"),
        vec![NamespaceMutationCandidate::commit(ApiCommitRequest {
            commit_id: CommitId::parse("create-missing-parent-unprepared-content")
                .expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateFile {
                parent_inode_id: InodeId(99),
                display_name: test_display_name("missing.txt"),
                content_ref: missing_content.clone(),
            }],
            message: None,
        })],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("one result")
    .expect_err("unprepared content should win before missing parent");
    assert!(matches!(
        error,
        CoreError::ContentPreparation(
            loonfs_core::publish::ContentPreparationError::ContentNotPrepared {
                content_ref_digest
            }
        ) if content_ref_digest == missing_content.digest
    ));
    assert_eq!(guarded_store.content_store_access_count(), 1);
}

#[tokio::test]
async fn explicit_replace_file_fails_unprepared_before_revision_validation_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/replace.txt",
        b"first",
        &context,
        Some("seed-replace"),
    )
    .await
    .expect("seed replace target");
    let inode_id = resolve_path(&store, &namespace_id("demo"), "/docs/replace.txt")
        .await
        .expect("resolve path")
        .inode_id;

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 1);
    let missing_content = content_ref("missing-content");
    let error = publish_namespace_mutations_batch(
        &guarded_store,
        &namespace_id("demo"),
        vec![NamespaceMutationCandidate::commit(ApiCommitRequest {
            commit_id: CommitId::parse("replace-stale-missing-content").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::ReplaceFile {
                inode_id,
                base_revision_no: RevisionNo(99),
                content_ref: missing_content.clone(),
            }],
            message: None,
        })],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("one result")
    .expect_err("unprepared content should win before stale revision");
    assert!(matches!(
        error,
        CoreError::ContentPreparation(
            loonfs_core::publish::ContentPreparationError::ContentNotPrepared {
                content_ref_digest
            }
        ) if content_ref_digest == missing_content.digest
    ));
    assert_eq!(guarded_store.content_store_access_count(), 1);
}

#[tokio::test]
async fn restore_revision_missing_source_is_revision_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/restore.txt",
        b"first",
        &context,
        Some("seed-restore"),
    )
    .await
    .expect("seed restore target");
    let inode_id = resolve_path(&store, &namespace_id("demo"), "/docs/restore.txt")
        .await
        .expect("resolve path")
        .inode_id;

    let error = commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("restore-missing-source").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::RestoreRevision {
                inode_id,
                source_revision_no: RevisionNo(99),
                base_revision_no: RevisionNo(1),
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect_err("missing restore source should fail");
    assert_eq!(error.code(), ErrorCode::RevisionNotFound);
}

#[tokio::test]
async fn idempotent_path_retry_returns_receipt_before_content_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    let content = store_bytes_as_content(&store, &namespace_id("demo"), b"idempotent")
        .await
        .expect("stage content");
    let commit_id = CommitId::parse("idempotent-put-without-token").expect("valid commit id");
    let first = publish_namespace_mutations_batch(
        &store,
        &namespace_id("demo"),
        vec![
            admitted_candidate(
                &store,
                &namespace_id("demo"),
                PathMutationIntent::PutFile {
                    commit_id: commit_id.clone(),
                    absolute_path: AbsolutePath::parse("/docs/idempotent.txt").expect("path"),
                    content_ref: content.content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                },
            )
            .await,
        ],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("single response")
    .expect("first commit");
    store
        .delete(&content.object_key)
        .await
        .expect("delete committed content blob");

    let retry = publish_namespace_mutations_batch(
        &store,
        &namespace_id("demo"),
        vec![NamespaceMutationCandidate::path(
            PathMutationIntent::PutFile {
                commit_id,
                absolute_path: AbsolutePath::parse("/docs/idempotent.txt").expect("path"),
                content_ref: content.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
        )],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("single response")
    .expect("idempotent retry should return existing receipt");

    assert_eq!(retry.committed_seq, first.committed_seq);
}

#[tokio::test]
async fn move_path_into_occupied_target_is_path_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/a.txt",
        b"alpha",
        &context,
        Some("seed-docs"),
    )
    .await
    .expect("seed docs");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/tmp/a.txt",
        b"tmp-a",
        &context,
        Some("seed-tmp"),
    )
    .await
    .expect("seed tmp");

    let error = move_path(
        &store,
        &namespace_id("demo"),
        "/tmp/a.txt",
        "/docs/a.txt",
        &context,
        Some("move-conflict"),
    )
    .await
    .expect_err("move conflict");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn move_path_directory_cycle_is_would_cycle() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/archive/leaf.txt",
        b"leaf",
        &context,
        Some("seed-cycle"),
    )
    .await
    .expect("seed cycle dirs");

    let error = move_path(
        &store,
        &namespace_id("demo"),
        "/docs",
        "/docs/archive/docs",
        &context,
        Some("cycle"),
    )
    .await
    .expect_err("cycle");
    assert_eq!(error.code(), ErrorCode::WouldCycle);
}

#[tokio::test]
async fn write_and_move_under_deleted_ancestor_start_fresh_subtrees() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/old.txt",
        b"old",
        &context,
        Some("seed-docs"),
    )
    .await
    .expect("seed docs");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/tmp/source.txt",
        b"source",
        &context,
        Some("seed-source"),
    )
    .await
    .expect("seed source");
    delete_path(
        &store,
        &namespace_id("demo"),
        "/docs",
        &context,
        Some("delete-docs"),
    )
    .await
    .expect("delete docs");

    // Deleting `/docs` unbinds the name: it is reusable immediately, and
    // writes and moves under it create a fresh subtree rather than
    // conflicting with the dead one. The old subtree stays dead — its
    // previous child is not resurrected by the recreation.
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/new.txt",
        b"new",
        &context,
        Some("put-under-recreated"),
    )
    .await
    .expect("put recreates the subtree");
    let old_child = resolve_path(&store, &namespace_id("demo"), "/docs/old.txt").await;
    assert!(old_child.is_err(), "dead subtree child must stay invisible");

    move_path(
        &store,
        &namespace_id("demo"),
        "/tmp/source.txt",
        "/docs/source.txt",
        &context,
        Some("move-under-recreated"),
    )
    .await
    .expect("move lands in the recreated subtree");
}

#[tokio::test]
async fn create_directory_path_creates_directory_without_auto_parents() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    let created = create_directory_path(
        &store,
        &namespace_id("demo"),
        "/docs",
        &context,
        Some("mkdir-docs"),
    )
    .await
    .expect("create dir");
    assert_eq!(created.committed_seq, ChangeSeq(1));
    let docs = resolve_path(&store, &namespace_id("demo"), "/docs")
        .await
        .expect("resolve docs");
    assert_eq!(docs.inode_kind, InodeKind::Directory);

    let missing_parent = create_directory_path(
        &store,
        &namespace_id("demo"),
        "/missing/nested",
        &context,
        Some("mkdir-missing-parent"),
    )
    .await
    .expect_err("mkdir does not auto-create parents");
    assert_eq!(missing_parent.code(), ErrorCode::PathNotFound);
}

#[tokio::test]
async fn path_move_writes_unbind_and_stale_binding_is_fails() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/a.txt",
        b"hello",
        &context,
        Some("seed-a"),
    )
    .await
    .expect("seed file");
    let file = resolve_path(&store, &namespace_id("demo"), "/docs/a.txt")
        .await
        .expect("resolve file");
    let old_binding =
        latest_binding_for_child_from_change_feed(&store, &namespace_id("demo"), file.inode_id)
            .await;

    move_path(
        &store,
        &namespace_id("demo"),
        "/docs/a.txt",
        "/docs/b.txt",
        &context,
        Some("move-a-to-b"),
    )
    .await
    .expect("move file");
    let unbind_count = list_changes_after(&store, &namespace_id("demo"), ChangeSeq(0))
        .await
        .expect("change feed")
        .changes
        .iter()
        .flat_map(|change| &change.deltas)
        .filter(|delta| matches!(delta, CommitDelta::UnbindDirentry { .. }))
        .count();
    assert_eq!(unbind_count, 1);
    assert!(resolve_path(&store, &namespace_id("demo"), "/docs/a.txt")
        .await
        .is_err());
    resolve_path(&store, &namespace_id("demo"), "/docs/b.txt")
        .await
        .expect("new path visible");

    let stale_binding = commit_operations(
        &store,
        &namespace_id("demo"),
        ApiCommitRequest {
            commit_id: CommitId::parse("delete-with-stale-binding").expect("valid commit id"),
            preconditions: vec![CommitPrecondition::BindingIs {
                parent_inode_id: old_binding.parent_inode_id,
                name_key: old_binding.name_key.clone(),
                child_inode_id: old_binding.child_inode_id,
                bind_seq: old_binding.bind_seq,
                bind_delta_index: old_binding.bind_delta_index,
            }],
            ops: vec![ApiCommitOp::DeleteFile {
                inode_id: file.inode_id,
            }],
            message: None,
        },
        &context,
    )
    .await
    .expect_err("stale binding should fail");
    assert_eq!(stale_binding.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn put_file_no_replace_rejects_existing_target_without_force() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/hello.txt",
        b"hello",
        &context,
        Some("seed-hello"),
    )
    .await
    .expect("seed file");

    let error = put_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/hello.txt",
        b"new-bytes",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-no-force"),
    )
    .await
    .expect_err("put without force");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn delete_path_non_recursive_rejects_non_empty_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/hello.txt",
        b"hello",
        &context,
        Some("seed-docs"),
    )
    .await
    .expect("seed file");

    let error = delete_path_non_recursive(
        &store,
        &namespace_id("demo"),
        "/docs",
        &context,
        Some("delete-docs"),
    )
    .await
    .expect_err("non-recursive delete should reject non-empty dir");
    assert!(matches!(error, CoreError::DirectoryNotEmpty(path) if path == "/docs"));
}

#[tokio::test]
async fn copy_file_path_creates_new_inode_and_reuses_content_blob() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/source.txt",
        b"hello",
        &context,
        Some("seed-source"),
    )
    .await
    .expect("seed source");

    copy_file_path(
        &store,
        &namespace_id("demo"),
        "/docs/source.txt",
        "/docs/copy.txt",
        &context,
        Some("copy-file"),
    )
    .await
    .expect("copy file");

    let source = resolve_path(&store, &namespace_id("demo"), "/docs/source.txt")
        .await
        .expect("source stat");
    let copy = resolve_path(&store, &namespace_id("demo"), "/docs/copy.txt")
        .await
        .expect("copy stat");
    assert_ne!(source.inode_id, copy.inode_id);
    assert_eq!(
        source.content_ref, copy.content_ref,
        "copy should reuse stored content ref"
    );
}

#[tokio::test]
async fn resolve_path_uses_nfc_casefold_name_policy() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let stored_path = "/Cafe\u{0301}.txt";
    let lookup_path = "/CAF\u{00c9}.TXT";
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        stored_path,
        b"hello",
        &context,
        Some("seed-unicode-name"),
    )
    .await
    .expect("seed unicode name");

    let resolved = resolve_path(&store, &namespace_id("demo"), lookup_path)
        .await
        .expect("resolve path");
    assert_eq!(resolved.absolute_path, stored_path);
    assert_eq!(resolved.display_name.as_str(), "Cafe\u{0301}.txt");
}

#[tokio::test]
async fn no_replace_put_rejects_casefold_and_normalization_equivalent_name() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/Cafe\u{0301}.txt",
        b"hello",
        &context,
        Some("seed-unicode-name"),
    )
    .await
    .expect("seed unicode name");

    let error = put_file_bytes(
        &store,
        &namespace_id("demo"),
        "/CAF\u{00c9}.TXT",
        b"new-bytes",
        DestinationBehavior::NoReplace,
        &context,
        Some("create-only-conflict"),
    )
    .await
    .expect_err("create-only conflict");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

async fn seed_source_namespace_for_fork<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    bootstrap_namespace(store, source_namespace_id, context, false)
        .await
        .expect("bootstrap source namespace");
    write_file_bytes(
        store,
        source_namespace_id,
        "/docs/shared.txt",
        b"base",
        context,
        Some("seed-shared"),
    )
    .await
    .expect("seed shared file");
}

async fn assert_namespace_partial<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    let partial_error = bootstrap_namespace(store, namespace_id, context, false)
        .await
        .expect_err("partial namespace");
    assert!(matches!(
        partial_error,
        loonfs_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
}

#[derive(Debug)]
struct InjectCreateFailureStore {
    inner: LocalFsStore,
    matcher: KeyMatcher,
    failure: InjectedCreateFailure,
    injected: Mutex<bool>,
}

impl InjectCreateFailureStore {
    fn new(inner: LocalFsStore, matcher: KeyMatcher, failure: InjectedCreateFailure) -> Self {
        Self {
            inner,
            matcher,
            failure,
            injected: Mutex::new(false),
        }
    }
}

#[derive(Debug)]
enum KeyMatcher {
    Exact(String),
    Prefix(String),
}

impl KeyMatcher {
    fn matches(&self, key: &str) -> bool {
        match self {
            Self::Exact(expected) => key == expected,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }
}

#[derive(Debug)]
enum InjectedCreateFailure {
    Transport {
        message: &'static str,
    },
    Conflict {
        write_attempted_object: bool,
        additional_writes: Vec<(String, Vec<u8>)>,
    },
    PreconditionFailed {
        write_attempted_object: bool,
        additional_writes: Vec<(String, Vec<u8>)>,
    },
}

impl InjectedCreateFailure {
    async fn apply_before_error(
        &self,
        inner: &LocalFsStore,
        attempted_key: &str,
        attempted_bytes: Bytes,
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::Transport { .. } => Ok(()),
            Self::Conflict {
                write_attempted_object,
                additional_writes,
            }
            | Self::PreconditionFailed {
                write_attempted_object,
                additional_writes,
            } => {
                if *write_attempted_object {
                    inner
                        .put_overwrite(attempted_key, attempted_bytes.clone())
                        .await?;
                }
                for (key, bytes) in additional_writes {
                    inner
                        .put_overwrite(key, Bytes::copy_from_slice(bytes))
                        .await?;
                }
                Ok(())
            }
        }
    }

    fn error(&self, attempted_key: &str) -> ObjectStoreError {
        match self {
            Self::Transport { message } => {
                ObjectStoreError::transport(attempted_key, (*message).to_owned())
            }
            Self::Conflict { .. } => ObjectStoreError::Conflict {
                object_key: attempted_key.to_owned(),
            },
            Self::PreconditionFailed { .. } => ObjectStoreError::PreconditionFailed {
                object_key: attempted_key.to_owned(),
            },
        }
    }
}

#[async_trait]
impl ObjectStore for InjectCreateFailureStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if matches!(&mode, PutMode::CreateIfAbsent) && self.matcher.matches(key) {
            let should_inject = {
                let mut injected = self
                    .injected
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *injected {
                    false
                } else {
                    *injected = true;
                    true
                }
            };
            if should_inject {
                self.failure
                    .apply_before_error(&self.inner, key, bytes.clone())
                    .await?;
                return Err(self.failure.error(key));
            }
        }

        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn metadata_state_after(sequences: &[Vec<WalDelta>]) -> MetadataState {
    let mut state = MetadataState::default()
        .apply_committed_wal_deltas(
            ChangeSeq(0),
            4_200,
            &[WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
            }],
        )
        .metadata_state;

    for (index, deltas) in sequences.iter().enumerate() {
        state = state
            .apply_committed_wal_deltas(
                ChangeSeq(u64::try_from(index + 1).expect("seq")),
                4_200,
                deltas,
            )
            .metadata_state;
    }

    state
}

fn validation_context(
    metadata_state: &MetadataState,
    seq: ChangeSeq,
    next_inode_id: InodeId,
) -> CommitValidationContext<'_> {
    let namespace_id = namespace_id("demo");
    let head = HeadState {
        namespace_id: namespace_id.clone(),
        seq,
        head_commit_id: CommitId::parse("c_00000000000000000000000000000000").expect("commit id"),
        writer_epoch: WriterEpoch(1),
        writer: Some(WriterBlock {
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            acquired_at_ms: 1_000,
        }),
        next_inode_id,
        visible_wal_tip: None,
        recent_segments: Vec::new(),
        state: Default::default(),
    };
    CommitValidationContext {
        head,
        name_policy: loonfs_api::NamePolicy::default(),
        metadata_state,
    }
}

fn mutation_context() -> MutationContext {
    common::mutation_context("writer-a", "wrs_test", "writer-a/0.1.0", 1_000)
}

fn content_ref(seed: &str) -> ContentRef {
    ContentRef {
        kind: ContentRefKind::WholeFileV0,
        digest: sha256_digest(seed.as_bytes()),
        size_bytes: seed.len() as u64,
    }
}

fn replay_read_guard_store(root: impl AsRef<Path>, namespace: &str) -> FailStore<LocalFsStore> {
    let wal_prefix = format!("namespaces/{namespace}/wal/segments/");
    let manifest_prefix = format!("namespaces/{namespace}/metadata/manifests/");
    let store = FailStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::new(move |key| {
            key.starts_with(&wal_prefix) || key.starts_with(&manifest_prefix)
        }),
        OperationClass::Read,
        InjectedError::Transport("begin_upload unexpectedly read replay object".to_owned()),
    );
    store.fail_all();
    store
}

fn content_blob_counting_store(root: impl AsRef<Path>) -> CountingStore<LocalFsStore> {
    CountingStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::content_blob(),
    )
}

fn metadata_table_counting_store(root: impl AsRef<Path>) -> CountingStore<LocalFsStore> {
    CountingStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::metadata_table(),
    )
}

#[derive(Debug)]
struct ContentStoreAccessLimitStore {
    inner: LocalFsStore,
    content_store_accesses: AtomicUsize,
    max_content_store_accesses: usize,
}

impl ContentStoreAccessLimitStore {
    fn new(root: impl AsRef<Path>, max_content_store_accesses: usize) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            content_store_accesses: AtomicUsize::new(0),
            max_content_store_accesses,
        }
    }

    fn content_store_access_count(&self) -> usize {
        self.content_store_accesses.load(Ordering::SeqCst)
    }

    fn record_content_store_access(&self, key: &str) -> Result<(), ObjectStoreError> {
        if !key.starts_with("content-stores/") {
            return Ok(());
        }

        let previous = self.content_store_accesses.fetch_add(1, Ordering::SeqCst);
        if previous >= self.max_content_store_accesses {
            return Err(ObjectStoreError::transport(
                key,
                "unexpected content-store descriptor access",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for ContentStoreAccessLimitStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct StaleHeadGetStore {
    inner: LocalFsStore,
    head_key: String,
    state: Mutex<StaleHeadGetState>,
}

#[derive(Debug)]
struct StaleHeadGetState {
    stale_head_body: Option<ObjectBody>,
    clean_head_gets_before_injection: Option<usize>,
    injected_stale_head_get: bool,
}

impl StaleHeadGetStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            head_key: wal_head(namespace_id.as_str()),
            state: Mutex::new(StaleHeadGetState {
                stale_head_body: None,
                clean_head_gets_before_injection: None,
                injected_stale_head_get: false,
            }),
        }
    }

    fn inject_stale_head_get_after(&self, clean_head_gets: usize) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.stale_head_body.is_some(),
            "stale head body should be captured before injection"
        );
        state.clean_head_gets_before_injection = Some(clean_head_gets);
        state.injected_stale_head_get = false;
    }

    fn injected_stale_head_get(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .injected_stale_head_get
    }

    fn record_head_write(&self, previous_head: Option<ObjectBody>) {
        if let Some(previous_head) = previous_head {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .stale_head_body = Some(previous_head);
        }
    }
}

#[async_trait]
impl ObjectStore for StaleHeadGetStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        if key == self.head_key {
            let stale_head = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match state.clean_head_gets_before_injection {
                    Some(0) => {
                        state.clean_head_gets_before_injection = None;
                        state.injected_stale_head_get = true;
                        state.stale_head_body.clone()
                    }
                    Some(remaining) => {
                        state.clean_head_gets_before_injection = Some(remaining - 1);
                        None
                    }
                    None => None,
                }
            };
            if let Some(stale_head) = stale_head {
                return Ok(Some(stale_head));
            }
        }

        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let previous_head = if key == self.head_key {
            self.inner.get_with_metadata(key).await?
        } else {
            None
        };
        let metadata = self.inner.put(key, bytes, mode).await?;
        if key == self.head_key {
            self.record_head_write(previous_head);
        }
        Ok(metadata)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn ack_lost_head_cas_store(
    root: impl AsRef<Path>,
    namespace_id: &NamespaceId,
) -> FailStore<LocalFsStore> {
    let head_key = wal_head(namespace_id.as_str());
    let store = FailStore::matching(
        LocalFsStore::new(root.as_ref()).expect("store"),
        move |operation: &OperationContext<'_>| {
            if operation.key() != head_key {
                return false;
            }
            let bytes = match operation.kind() {
                OperationKind::CompareAndSwap { bytes, .. }
                | OperationKind::Put {
                    bytes,
                    mode: PutMode::CompareAndSwap { .. },
                } => bytes,
                _ => return false,
            };
            decode_control_object::<HeadState>(bytes, ControlObjectKind::WalHead)
                .is_ok_and(|envelope| envelope.state.seq > ChangeSeq(0))
        },
        InjectedError::Transport("response lost after head compare-and-swap".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    store
}

trait AckLossProbe {
    fn injected_ack_loss(&self) -> bool;
}

impl AckLossProbe for FailStore<LocalFsStore> {
    fn injected_ack_loss(&self) -> bool {
        self.attempts() > 0 && self.remaining() == 0
    }
}

#[derive(Debug)]
struct StaleHeadAfterWalWriteStore {
    inner: LocalFsStore,
    head_key: String,
    injected_stale_head: Mutex<bool>,
}

impl StaleHeadAfterWalWriteStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            head_key: wal_head(namespace_id.as_str()),
            injected_stale_head: Mutex::new(false),
        }
    }

    fn injected_stale_head(&self) -> bool {
        *self
            .injected_stale_head
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[async_trait]
impl ObjectStore for StaleHeadAfterWalWriteStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key
            && matches!(mode, PutMode::CompareAndSwap { .. })
            && head_cas_advances_seq(&self.inner, key, &bytes).await?
        {
            let should_inject = {
                let mut injected = self
                    .injected_stale_head
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *injected {
                    false
                } else {
                    *injected = true;
                    true
                }
            };
            if should_inject {
                if let Some(existing) = self.inner.get(key, None).await? {
                    self.inner.put_overwrite(key, existing).await?;
                }
                return Err(ObjectStoreError::PreconditionFailed {
                    object_key: key.to_owned(),
                });
            }
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

async fn head_cas_advances_seq(
    store: &LocalFsStore,
    key: &str,
    candidate_bytes: &[u8],
) -> Result<bool, ObjectStoreError> {
    let candidate: HeadStateEnvelope =
        decode_control_object(candidate_bytes, ControlObjectKind::WalHead).map_err(|err| {
            ObjectStoreError::transport(key, format!("decode candidate head: {err}"))
        })?;
    let Some(existing_bytes) = store.get(key, None).await? else {
        return Ok(true);
    };
    let existing: HeadStateEnvelope =
        decode_control_object(&existing_bytes, ControlObjectKind::WalHead).map_err(|err| {
            ObjectStoreError::transport(key, format!("decode existing head: {err}"))
        })?;
    Ok(candidate.state.seq > existing.state.seq)
}

/// Listings decide visibility during child enumeration and then look up
/// revision heads without re-deriving it, so this pins both halves of that
/// contract: a recursively deleted subtree stays out of listings and
/// resolution entirely, while entries the enumeration does admit still
/// surface their real revision data (revision_no, size, content ref).
#[tokio::test]
async fn tombstoned_children_stay_unlisted_and_live_entries_keep_revision_data() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    create_directory_path(&store, &namespace_id("demo"), "/live", &context, None)
        .await
        .expect("create live dir");
    create_directory_path(&store, &namespace_id("demo"), "/dead", &context, None)
        .await
        .expect("create dead dir");
    put_file_bytes(
        &store,
        &namespace_id("demo"),
        "/live/kept.txt",
        b"alive",
        DestinationBehavior::NoReplace,
        &context,
        None,
    )
    .await
    .expect("put live file");
    put_file_bytes(
        &store,
        &namespace_id("demo"),
        "/dead/gone.txt",
        b"doomed",
        DestinationBehavior::NoReplace,
        &context,
        None,
    )
    .await
    .expect("put doomed file");
    delete_path(&store, &namespace_id("demo"), "/dead", &context, None)
        .await
        .expect("delete dead subtree");

    let root_entries = list_path(&store, &namespace_id("demo"), "/")
        .await
        .expect("list root");
    let root_names: Vec<&str> = root_entries
        .iter()
        .map(|entry| entry.absolute_path.as_str())
        .collect();
    assert!(
        root_names.contains(&"/live"),
        "live directory must list, got {root_names:?}"
    );
    assert!(
        !root_names.iter().any(|path| path.starts_with("/dead")),
        "tombstoned directory must not list, got {root_names:?}"
    );

    let live_entries = list_path(&store, &namespace_id("demo"), "/live")
        .await
        .expect("list live dir");
    assert_eq!(live_entries.len(), 1, "one live child");
    let kept = &live_entries[0];
    assert_eq!(kept.absolute_path, "/live/kept.txt");
    assert_eq!(
        kept.size_bytes,
        Some(b"alive".len() as u64),
        "listed entry carries its revision's size"
    );
    assert!(
        kept.revision_no.is_some(),
        "listed entry carries its revision number"
    );
    assert!(
        kept.content_ref.is_some(),
        "listed entry carries its content ref"
    );

    resolve_path(&store, &namespace_id("demo"), "/dead/gone.txt")
        .await
        .expect_err("file under tombstoned subtree must not resolve");
    resolve_path(&store, &namespace_id("demo"), "/dead")
        .await
        .expect_err("tombstoned directory must not resolve");
}

#[tokio::test]
async fn move_replace_atomically_replaces_a_file_destination() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-a"),
    )
    .await
    .expect("put a");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"beta",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-b"),
    )
    .await
    .expect("put b");

    // The default stays create-only: an occupied destination is a conflict.
    let error = submit_intent(
        &store,
        &namespace_id,
        PathMutationIntent::MovePath {
            commit_id: CommitId::parse("move-no-replace").expect("valid commit id"),
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
        },
        &context,
    )
    .await
    .expect_err("no-replace move onto an occupied name fails");
    assert!(matches!(error, CoreError::DestinationExists(_)));

    // Replace compiles to one commit: the destination file's delete and the
    // source's rebind land atomically.
    submit_intent(
        &store,
        &namespace_id,
        PathMutationIntent::MovePath {
            commit_id: CommitId::parse("move-replace").expect("valid commit id"),
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
        },
        &context,
    )
    .await
    .expect("replace move");

    let read = read_file_bytes(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("read destination");
    assert_eq!(read.bytes, b"alpha");
    read_file_bytes(&store, &namespace_id, "/docs/a.txt")
        .await
        .expect_err("source path is gone");
}

#[tokio::test]
async fn move_replace_rejects_directory_destinations_and_self_moves() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-a"),
    )
    .await
    .expect("put a");
    create_directory_path(&store, &namespace_id, "/docs/dir", &context, Some("mkdir"))
        .await
        .expect("mkdir");

    // Mirrors put: only a file destination can be replaced.
    let error = submit_intent(
        &store,
        &namespace_id,
        PathMutationIntent::MovePath {
            commit_id: CommitId::parse("move-onto-dir").expect("valid commit id"),
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/dir").expect("path"),
            behavior: DestinationBehavior::Replace,
        },
        &context,
    )
    .await
    .expect_err("replace move onto a directory fails");
    assert!(matches!(error, CoreError::ExpectedFile { .. }));

    // A path never replaces itself, force or not.
    let error = submit_intent(
        &store,
        &namespace_id,
        PathMutationIntent::MovePath {
            commit_id: CommitId::parse("move-onto-self").expect("valid commit id"),
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
        },
        &context,
    )
    .await
    .expect_err("replace move onto itself fails");
    assert!(matches!(error, CoreError::DestinationExists(_)));
}

#[tokio::test]
async fn copy_replace_appends_a_revision_to_the_destination_inode() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-a"),
    )
    .await
    .expect("put a");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"beta",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-b"),
    )
    .await
    .expect("put b");
    let before = list_file_revisions(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("destination revisions before");

    submit_intent(
        &store,
        &namespace_id,
        PathMutationIntent::CopyFilePath {
            commit_id: CommitId::parse("copy-replace").expect("valid commit id"),
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
        },
        &context,
    )
    .await
    .expect("replace copy");

    // Mirrors put onto an existing file: the destination keeps its inode and
    // revision history, gaining one revision with the source's content.
    let read = read_file_bytes(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("read destination");
    assert_eq!(read.bytes, b"alpha");
    let after = list_file_revisions(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("destination revisions after");
    assert_eq!(after.inode_id, before.inode_id);
    assert_eq!(after.revisions.len(), before.revisions.len() + 1);
    let source = read_file_bytes(&store, &namespace_id, "/docs/a.txt")
        .await
        .expect("source still read");
    assert_eq!(source.bytes, b"alpha");
}
