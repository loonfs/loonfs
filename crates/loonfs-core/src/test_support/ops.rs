//! Namespace creation and individual path mutations for tests.

use super::content_write::store_file_bytes_before_metadata_publish;
use crate::commit_engine::CommitCandidate;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::path::mutation_path::parse_mutation_path;
use crate::path::write::{CommitRequest, FilesystemOperation};
use crate::storage::content_admission::PreparedContent;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_types::format::wal::{
    encode_wal_object_envelope_zstd, WalCommitDelta, WalCommitPayload, WalDelta, WalInlineContent,
    WalObjectPayload,
};
use loonfs_types::{
    ChangeSeq, Commit, CommitId, DeleteDirectoryBehavior, DestinationBehavior, NamespaceId,
    RevisionNo,
};

pub(crate) async fn create<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_types::NamespaceMetadata> {
    crate::namespace::bootstrap::bootstrap_namespace(
        store,
        namespace_id,
        context,
        &loonfs_test_support::test_actor(),
        &crate::options::CreateNamespaceOptions::default(),
    )
    .await
}

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
) -> Result<Commit> {
    let request = CommitRequest::single(
        commit_id,
        loonfs_test_support::test_actor(),
        None,
        operation,
    );
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

pub(crate) async fn put_file<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: DestinationBehavior,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<Commit> {
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
) -> Result<Commit> {
    put_file(
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

/// Writes several files in one commit, for fixtures that need many rows but
/// not a commit per row.
pub(crate) async fn write_files_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_paths: &[String],
    bytes: &[u8],
    context: &MutationContext,
) -> Result<Commit> {
    let mut operations = Vec::with_capacity(absolute_paths.len());
    let mut prepared = Vec::with_capacity(absolute_paths.len());
    for absolute_path in absolute_paths {
        let content =
            store_file_bytes_before_metadata_publish(store, namespace_id, absolute_path, bytes)
                .await?;
        operations.push(FilesystemOperation::PutFile {
            path: parse_mutation_path(absolute_path)?,
            content_ref: Some(content.content_ref().clone()),
            inline_content: None,
            behavior: DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        });
        prepared.push(content);
    }
    let request = CommitRequest {
        commit_id: CommitId::generate(),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations,
        preconditions: Vec::new(),
    };
    let mut results = crate::commit_engine::publish_namespace_commits_batch(
        store,
        namespace_id,
        vec![CommitCandidate::prepared(request, prepared)],
        context,
    )
    .await;
    results
        .pop()
        .expect("single-candidate batch should hold exactly one result")
}

async fn put_prepared_file_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    prepared_content: PreparedContent,
    behavior: DestinationBehavior,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<Commit> {
    let content_ref = prepared_content.content_ref().clone();
    submit_operation(
        store,
        namespace_id,
        normalized_commit_id(commit_id),
        FilesystemOperation::PutFile {
            path: parse_mutation_path(absolute_path)?,
            content_ref: Some(content_ref),
            inline_content: None,
            behavior,
            expected_inode_id: None,
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
) -> Result<Commit> {
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
) -> Result<Commit> {
    submit_operation(
        store,
        namespace_id,
        normalized_commit_id(commit_id),
        FilesystemOperation::DeletePath {
            path: parse_mutation_path(absolute_path)?,
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
    source_path: &str,
    destination_path: &str,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<Commit> {
    submit_operation(
        store,
        namespace_id,
        normalized_commit_id(commit_id),
        FilesystemOperation::MovePath {
            source_path: parse_mutation_path(source_path)?,
            destination_path: parse_mutation_path(destination_path)?,
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        Vec::new(),
        context,
    )
    .await
}

/// Publishes `deltas` and `inline_content` as one commit in the next WAL
/// object, without planning, so a test controls the exact deltas and pieces.
#[allow(
    clippy::disallowed_methods,
    reason = "a test writes the next numbered WAL object directly"
)]
pub(crate) async fn append_wal_commit<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    deltas: Vec<WalDelta>,
    inline_content: Vec<WalInlineContent>,
) -> Result<ChangeSeq> {
    let head = crate::namespace::control::load_namespace_read_state(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let committed_seq = ChangeSeq(head.seq.0 + 1);
    let wal_no = loonfs_types::WalNo(head.wal_no.0 + 1);
    let payload = WalObjectPayload {
        namespace_id: namespace_id.clone(),
        wal_no,
        writer_epoch: head.writer_epoch,
        head_seq: committed_seq,
        next_inode_id: head.next_inode_id,
        records: vec![WalCommitPayload {
            committed_seq,
            commit_id: CommitId::generate(),
            committed_by: loonfs_test_support::test_actor(),
            semantic_commit_fingerprint: serde_json::from_str(&format!(
                r#""v1:sha256:{:064x}""#,
                committed_seq.0
            ))
            .expect("fingerprint"),
            committed_at_ms: 0,
            message: None,
            deltas: deltas
                .into_iter()
                .enumerate()
                .map(|(index, delta)| WalCommitDelta {
                    semantic_operation_index: index as u32,
                    delta,
                })
                .collect(),
            inline_content,
        }],
    };
    let object = encode_wal_object_envelope_zstd(payload)
        .map_err(|error| CoreError::Internal(error.to_string()))?;
    let key = loonfs_objectstore::keys::wal_object(namespace_id, &wal_no);
    store
        .put(&key, object.into_bytes().into(), PutMode::CreateIfAbsent)
        .await
        .map_err(|error| CoreError::store(&key, &error))?;
    Ok(committed_seq)
}

pub(crate) async fn restore_file_revision<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    source_revision_no: RevisionNo,
    context: &MutationContext,
    commit_id: Option<&CommitId>,
) -> Result<Commit> {
    submit_operation(
        store,
        namespace_id,
        normalized_commit_id(commit_id),
        FilesystemOperation::RestoreRevision {
            path: parse_mutation_path(absolute_path)?,
            source_revision_no,
        },
        Vec::new(),
        context,
    )
    .await
}
