//! Batch publication, replay, acknowledgement loss, limits, and fencing preconditions.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use bytes::Bytes;
use loonfs_api::{
    v0::FilesystemChange,
    wire::wal::{decode_wal_segment_envelope_zstd, WalDelta},
    AbsolutePath, ActorId, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior,
    InodeId, NamespaceId,
};
use loonfs_core::commit::CommitValidationError;
use loonfs_core::content::{prepare_existing_content_ref, store_bytes_as_content};
use loonfs_core::control::load_namespace_read_state;
use loonfs_core::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, NamespaceCommitEngine, PublishTailOptions,
};
use loonfs_core::{Error as CoreError, ErrorCode, MutationContext};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{FailStore, InjectedError, OperationContext, OperationKind};
use std::path::Path;
use tempfile::tempdir;

async fn delete_path_non_recursive_expecting<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    expected_inode_id: Option<InodeId>,
    context: &MutationContext,
    commit_id: &str,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_operation(
        store,
        namespace_id,
        CommitId::parse(commit_id).expect("valid test commit id"),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse(absolute_path).expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id,
        },
        context,
    )
    .await
}

/// Builds a single-operation request the way every fixture below wants it:
/// a fixed commit id so retries replay, and no caller annotation.
fn commit_request(commit_id: &str, operation: FilesystemOperation) -> CommitRequest {
    CommitRequest::single(
        CommitId::parse(commit_id).expect("valid test commit id"),
        loonfs_test_support::test_actor(),
        None,
        operation,
    )
}

fn ack_lost_wal_put_store(
    root: impl AsRef<Path>,
    namespace_id: &NamespaceId,
) -> FailStore<LocalFsStore> {
    let wal_prefix = loonfs_objectstore::keys::wal_segment_prefix(namespace_id);
    let store = FailStore::matching(
        LocalFsStore::new(root.as_ref()).expect("store"),
        move |operation: &OperationContext<'_>| {
            if !operation.key().starts_with(&wal_prefix) {
                return false;
            }
            let bytes = match operation.kind() {
                OperationKind::Put {
                    bytes,
                    mode: PutMode::CreateIfAbsent,
                } => bytes,
                _ => return false,
            };
            decode_wal_segment_envelope_zstd(bytes)
                .is_ok_and(|envelope| !envelope.payload().records.is_empty())
        },
        InjectedError::Transport("response lost after WAL put".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    store
}

async fn data_wal_keys<S: ObjectStore + ?Sized>(store: &S) -> Vec<String> {
    let mut data_keys = Vec::new();
    for key in store
        .list_prefix("namespaces/demo/wal/")
        .await
        .expect("list WAL")
    {
        let bytes = store
            .get(&key, None)
            .await
            .expect("read WAL")
            .expect("WAL exists");
        if !decode_wal_segment_envelope_zstd(&bytes)
            .expect("decode WAL")
            .payload()
            .records
            .is_empty()
        {
            data_keys.push(key);
        }
    }
    data_keys
}

fn failed_data_put_store(inner: LocalFsStore) -> FailStore<LocalFsStore> {
    let store = FailStore::matching(
        inner,
        |operation: &OperationContext<'_>| match operation.kind() {
            OperationKind::Put {
                bytes,
                mode: PutMode::CreateIfAbsent,
            } if operation.key().starts_with("namespaces/demo/wal/") => {
                decode_wal_segment_envelope_zstd(bytes)
                    .is_ok_and(|envelope| !envelope.payload().records.is_empty())
            }
            _ => false,
        },
        InjectedError::PermissionDenied("WAL put refused".to_owned()),
    );
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
        .publish_namespace_commits_batch(vec![
            CommitCandidate::new(commit_request(
                "delete-cycled",
                FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/docs/cycled.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                },
            )),
            prepared_candidate(
                &store,
                &namespace_id,
                commit_request(
                    "recreate-cycled",
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/cycled.txt").expect("path"),
                        content_ref: staged.into_content_ref(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
        ])
        .await
        .expect("build mutation context");
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
async fn batch_commit_writes_one_segment_and_expands_change_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let responses = submit_commits_batch(
        &store,
        &namespace_id,
        vec![
            commit_request(
                "req-batch-a",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/alpha").expect("path"),
                    parents: false,
                },
            ),
            commit_request(
                "req-batch-b",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/beta").expect("path"),
                    parents: false,
                },
            ),
        ],
        &context,
    )
    .await;
    let first = responses[0].as_ref().expect("first commit");
    let second = responses[1].as_ref().expect("second commit");
    assert_eq!(first.committed_seq, ChangeSeq(1));
    assert_eq!(second.committed_seq, ChangeSeq(2));

    let wal_keys = data_wal_keys(&store).await;
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload().start_seq, ChangeSeq(1));
    assert_eq!(segment.payload().end_seq, ChangeSeq(2));
    assert_eq!(segment.payload().records.len(), 2);
    assert_eq!(segment.payload().records[0].deltas.len(), 2);
    assert_eq!(segment.payload().records[0].deltas[0].semantic_op_index, 0);
    assert_eq!(segment.payload().records[0].deltas[1].semantic_op_index, 0);
    match &segment.payload().records[0].deltas[1].delta {
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
    assert_eq!(changes.changes[0].events.len(), 1);
    assert!(matches!(
        &changes.changes[0].events[0],
        FilesystemChange::DirectoryCreated {
            parent_inode_id: InodeId(1),
            display_name,
            ..
        } if display_name.as_str() == "alpha"
    ));
}

#[tokio::test]
async fn change_feed_validates_wal_tail_before_current_manifest() {
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

    let wal_keys = data_wal_keys(&store).await;
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
        .expect_err("corrupt WAL tail");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
}

#[tokio::test]
async fn ack_lost_wal_put_reports_unknown_outcome_and_replays_idempotently() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = ack_lost_wal_put_store(temp_dir.path(), &namespace_id);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"ack lost")
        .await
        .expect("stage content");
    let put = || {
        commit_request(
            "ack-lost-put",
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/ack.txt").expect("path"),
                content_ref: content.content_ref().clone(),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
    };

    // The WAL put landed but its acknowledgment was lost: this must surface as
    // an unknown outcome, never as definite failure.
    let error = submit_commit(&store, &namespace_id, put(), &context)
        .await
        .expect_err("ack-lost WAL put is not definite failure");
    assert_eq!(error.code(), ErrorCode::CommitOutcomeUnknown);
    assert!(store.injected_ack_loss());

    // The documented remedy: retry with the same commit id. The commit is
    // already visible, so the retry replays it instead of double-committing.
    let result = submit_commit(&store, &namespace_id, put(), &context)
        .await
        .expect("same-commit-id retry replays the committed mutation");
    assert_eq!(result.committed_seq, ChangeSeq(1));

    let head = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.seq, ChangeSeq(1));
}

#[tokio::test]
async fn failed_wal_write_fails_rejections_decided_against_in_batch_state() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = failed_data_put_store(LocalFsStore::new(temp_dir.path()).expect("store"));
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"batch bytes")
        .await
        .expect("stage content");

    let batch = || async {
        vec![
            CommitCandidate::new(commit_request(
                "reject-materialization",
                FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/missing.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                },
            )),
            prepared_candidate(
                &store,
                &namespace_id,
                commit_request(
                    "accept-a",
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content_ref: content.content_ref().clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
            prepared_candidate(
                &store,
                &namespace_id,
                commit_request(
                    "reject-speculative",
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content_ref: content.content_ref().clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
            CommitCandidate::new(commit_request(
                "reject-materialization",
                FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/missing.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                },
            )),
            prepared_candidate(
                &store,
                &namespace_id,
                commit_request(
                    "accept-a",
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content_ref: content.content_ref().clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
        ]
    };

    let failed =
        publish_namespace_commits_batch(&store, &namespace_id, batch().await, &context).await;

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
    let accepted_alias = failed[4]
        .as_ref()
        .expect_err("alias of an accepted candidate fails with it");
    assert!(matches!(accepted_alias, CoreError::WalWrite { .. }));

    let head = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.seq, ChangeSeq(0));

    let retried =
        publish_namespace_commits_batch(&store, &namespace_id, batch().await, &context).await;
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
    assert_eq!(
        retried[4].as_ref().expect("alias lands with its primary"),
        committed
    );
}

#[tokio::test]
async fn failed_batch_preserves_an_independent_commit_id_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    submit_commit(
        &store,
        &namespace_id,
        commit_request(
            "durable-receipt",
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/durable").expect("path"),
                parents: false,
            },
        ),
        &context,
    )
    .await
    .expect("publish durable receipt");

    let store = failed_data_put_store(store);
    let failed = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![
            CommitCandidate::new(commit_request(
                "accepted-before-receipt-conflict",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/accepted").expect("path"),
                    parents: false,
                },
            )),
            CommitCandidate::new(commit_request(
                "durable-receipt",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/different").expect("path"),
                    parents: false,
                },
            )),
        ],
        &context,
    )
    .await;

    assert!(matches!(failed[0], Err(CoreError::WalWrite { .. })));
    assert_eq!(
        failed[1]
            .as_ref()
            .expect_err("commit ID conflict must stand")
            .code(),
        ErrorCode::CommitIdReuseConflict
    );
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

    let duplicated = commit_request(
        "req-duplicate",
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/alpha").expect("path"),
            parents: false,
        },
    );

    let responses = submit_commits_batch(
        &store,
        &namespace_id,
        vec![duplicated.clone(), duplicated],
        &context,
    )
    .await;
    let first = responses[0].as_ref().expect("primary commit");
    let duplicate = responses[1].as_ref().expect("duplicate commit");
    assert_eq!(first, duplicate);

    let wal_keys = data_wal_keys(&store).await;
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload().records.len(), 1);

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
    let prepared = prepare_existing_content_ref(&store, &catalog, stored.content_ref().clone())
        .await
        .expect("prepare content");
    let put = commit_request(
        "over-proof-replay",
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/proof-replay.txt").expect("path"),
            content_ref: stored.into_content_ref(),
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );

    let original = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![CommitCandidate::prepared(
            put.clone(),
            vec![prepared.clone()],
        )],
        &context,
    )
    .await
    .remove(0)
    .expect("land original commit");
    let oversized_proofs = vec![prepared; loonfs_core::limits::MAX_COMMIT_CONTENT_TOKENS + 1];

    let replay = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![CommitCandidate::prepared(
            put.clone(),
            oversized_proofs.clone(),
        )],
        &context,
    )
    .await
    .remove(0)
    .expect("durable receipt must win over current proof limit");
    assert_eq!(replay, original);

    let mut new_request = put;
    new_request.commit_id = CommitId::parse("over-proof-new").expect("valid commit id");
    let error = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![CommitCandidate::prepared(new_request, oversized_proofs)],
        &context,
    )
    .await
    .remove(0)
    .expect_err("new over-proof request must be rejected after receipt miss");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn empty_request_is_rejected_before_commit_id_reuse() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    submit_commit(
        &store,
        &namespace_id,
        commit_request(
            "empty-reuse",
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/docs").expect("path"),
                parents: false,
            },
        ),
        &context,
    )
    .await
    .expect("initial commit");

    let error = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            preconditions: Vec::new(),
            commit_id: CommitId::parse("empty-reuse").expect("valid commit id"),
            actor_id: loonfs_test_support::test_actor(),
            message: None,
            operations: Vec::new(),
        },
        &context,
    )
    .await
    .expect_err("empty request must fail before commit id reuse");

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
    let prepared = prepare_existing_content_ref(&store, &catalog, stored.content_ref().clone())
        .await
        .expect("prepare content");
    let put = commit_request(
        "over-proof-duplicate",
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/batch-proof.txt").expect("path"),
            content_ref: stored.into_content_ref(),
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );

    let responses = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![
            CommitCandidate::prepared(put.clone(), vec![prepared.clone()]),
            CommitCandidate::prepared(
                put,
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
    let candidate = CommitCandidate::new(CommitRequest {
        preconditions: Vec::new(),
        commit_id: CommitId::parse("over-operation-new").expect("valid commit id"),
        actor_id: loonfs_test_support::test_actor(),
        message: None,
        operations: (0..=loonfs_core::limits::MAX_COMMIT_OPERATIONS)
            .map(|index| FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse(format!("/over-operation-{index}")).expect("path"),
                parents: false,
            })
            .collect(),
    });

    // A later release may lower the operation limit below a durable request;
    // identity must remain available so receipt resolution can win first.
    candidate
        .semantic_identity(&namespace_id)
        .expect("current request limits must not affect identity");
    let error = publish_namespace_commits_batch(&store, &namespace_id, vec![candidate], &context)
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

    let mkdir = commit_request(
        "retry-across-writer",
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/alpha").expect("path"),
            parents: false,
        },
    );

    let first = submit_commit(&store, &namespace_id, mkdir.clone(), &writer_a)
        .await
        .expect("writer a commit");
    let writer_b = MutationContext {
        writer_id: loonfs_api::WriterId::parse("writer-b").expect("writer id"),
        now_ms: writer_a.now_ms.saturating_add(1),
    };

    let retry = submit_commit(&store, &namespace_id, mkdir, &writer_b)
        .await
        .expect("writer b retry");

    assert_eq!(first, retry);
}

#[tokio::test]
async fn checkpoint_receipt_keeps_actor_identity_after_the_commit_wal_is_compacted() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let first_context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &first_context, false)
        .await
        .expect("bootstrap");

    let actor = ActorId::parse("shared-id").expect("actor id");
    let request = |actor: ActorId| {
        CommitRequest::single(
            CommitId::parse("attributed-receipt").expect("commit id"),
            actor,
            Some("receipt identity".to_owned()),
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/attributed").expect("path"),
                parents: false,
            },
        )
    };
    let first = submit_commit(
        &store,
        &namespace_id,
        request(actor.clone()),
        &first_context,
    )
    .await
    .expect("commit");
    create_checkpoint(&store, &namespace_id, &first_context)
        .await
        .expect("compact commit into receipt row");
    // Retire the commit's history, which is what leaves the receipt as the
    // only record of it.
    namespace_engine(&store, &namespace_id, &first_context)
        .advance_retention_floor()
        .await
        .expect("advance retention floor past the commit");

    // Corrupt the original WAL so the checks below can only use the commit
    // receipt stored in the checkpoint.
    let wal_keys = data_wal_keys(&store).await;
    assert_eq!(wal_keys.len(), 1);
    store
        .put_overwrite(
            &wal_keys[0],
            Bytes::from_static(b"compacted WAL must not be read"),
        )
        .await
        .expect("poison compacted WAL");

    let later_context = MutationContext {
        writer_id: loonfs_api::WriterId::parse("writer-b").expect("writer id"),
        now_ms: first_context.now_ms + 10_000,
    };
    let replay = submit_commit(
        &store,
        &namespace_id,
        request(actor.clone()),
        &later_context,
    )
    .await
    .expect("same actor and request replay from receipt");
    assert_eq!(replay.committed_seq, first.committed_seq);
    assert_eq!(replay.commit_id, first.commit_id);
    assert_eq!(replay.committed_by, first.committed_by);
    assert_eq!(
        replay.committed_at_ms, first.committed_at_ms,
        "committed_at_ms is outside identity"
    );
    assert_eq!(replay.message, first.message);
    // The retired commit's events are gone with its history, so the receipt
    // answers without them.
    assert_eq!(replay.events, None);
    assert!(first.events.is_some());

    let error = submit_commit(
        &store,
        &namespace_id,
        request(ActorId::parse("different-id").expect("actor id")),
        &later_context,
    )
    .await
    .expect_err("a different actor cannot reuse the commit id");
    assert!(matches!(
        error,
        CoreError::CommitIdReuseConflict {
            commit_id,
            committed_seq: Some(committed_seq),
            committed_fingerprint: Some(fingerprint),
        } if commit_id == "attributed-receipt"
            && committed_seq == first.committed_seq
            && fingerprint.starts_with("v4:sha256:")
    ));
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

    let responses = submit_commits_batch(
        &store,
        &namespace_id,
        vec![
            commit_request(
                "req-conflict",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/alpha").expect("path"),
                    parents: false,
                },
            ),
            commit_request(
                "req-conflict",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/beta").expect("path"),
                    parents: false,
                },
            ),
        ],
        &context,
    )
    .await;

    responses[0].as_ref().expect("primary commit");
    let error = responses[1].as_ref().expect_err("duplicate conflict");
    // Neither claim had committed when the batch admitted them, so the
    // conflict has no receipt to name: neither the sequence nor the
    // fingerprint a retry would reconcile against.
    assert!(matches!(
        error,
        CoreError::CommitIdReuseConflict {
            commit_id,
            committed_seq: None,
            committed_fingerprint: None,
        } if commit_id == "req-conflict"
    ));

    let wal_keys = data_wal_keys(&store).await;
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload().records.len(), 1);

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

    let put = commit_request(
        "same-path-request",
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/same/path.txt").expect("path"),
            content_ref: content.content_ref().clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    let first = submit_commit(&store, &namespace_id, put.clone(), &context)
        .await
        .expect("first publish");
    let retry = submit_commit(&store, &namespace_id, put, &context)
        .await
        .expect("idempotent retry");
    assert_eq!(retry.committed_seq, first.committed_seq);

    let conflict = submit_commit(
        &store,
        &namespace_id,
        commit_request(
            "same-path-request",
            FilesystemOperation::DeletePath {
                path: AbsolutePath::parse("/same/path.txt").expect("path"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            },
        ),
        &context,
    )
    .await
    .expect_err("conflicting retry");
    // The receipt decided this one, so the conflict names where the id
    // landed and what landed there — the sequence a retry reads back, and
    // the identity it proves itself against.
    assert!(matches!(
        conflict,
        CoreError::CommitIdReuseConflict {
            commit_id,
            committed_seq,
            committed_fingerprint: Some(fingerprint),
        } if commit_id == "same-path-request"
            && committed_seq == Some(first.committed_seq)
            && fingerprint.starts_with("v4:sha256:")
    ));

    let wal_keys = data_wal_keys(&store).await;
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
        ("/same-precondition.txt", "seed-same-precondition"),
        ("/changed-precondition.txt", "seed-changed-precondition"),
        ("/removed-precondition.txt", "seed-removed-precondition"),
        ("/added-precondition.txt", "seed-added-precondition"),
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
        .expect("seed delete target");
    }

    let same_inode = resolve_path(&store, &namespace_id, "/same-precondition.txt")
        .await
        .expect("resolve same-precondition target")
        .inode_id;
    let first = delete_path_non_recursive_expecting(
        &store,
        &namespace_id,
        "/same-precondition.txt",
        Some(same_inode),
        &context,
        "delete-same-precondition",
    )
    .await
    .expect("first delete with preconditions");
    let retry = delete_path_non_recursive_expecting(
        &store,
        &namespace_id,
        "/same-precondition.txt",
        Some(same_inode),
        &context,
        "delete-same-precondition",
    )
    .await
    .expect("identical delete retry with preconditions");
    assert_eq!(retry, first);

    let changed_inode = resolve_path(&store, &namespace_id, "/changed-precondition.txt")
        .await
        .expect("resolve changed-precondition target")
        .inode_id;
    let removed_inode = resolve_path(&store, &namespace_id, "/removed-precondition.txt")
        .await
        .expect("resolve removed-precondition target")
        .inode_id;
    let added_inode = resolve_path(&store, &namespace_id, "/added-precondition.txt")
        .await
        .expect("resolve added-precondition target")
        .inode_id;
    let conflicts = [
        (
            "delete-changed-precondition",
            "/changed-precondition.txt",
            Some(changed_inode),
            Some(InodeId(1)),
        ),
        (
            "delete-removed-precondition",
            "/removed-precondition.txt",
            Some(removed_inode),
            None,
        ),
        (
            "delete-added-precondition",
            "/added-precondition.txt",
            None,
            Some(added_inode),
        ),
    ];
    for (commit_id, path, first_precondition, retry_precondition) in conflicts {
        delete_path_non_recursive_expecting(
            &store,
            &namespace_id,
            path,
            first_precondition,
            &context,
            commit_id,
        )
        .await
        .expect("first delete");

        let error = delete_path_non_recursive_expecting(
            &store,
            &namespace_id,
            path,
            retry_precondition,
            &context,
            commit_id,
        )
        .await
        .expect_err("changed precondition must conflict");
        assert!(matches!(
            error,
            CoreError::CommitIdReuseConflict {
                commit_id: reused,
                committed_seq: Some(_),
                committed_fingerprint: Some(_),
            } if reused == commit_id
        ));
    }

    let head = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.seq, ChangeSeq(8));
}

#[tokio::test]
async fn fresh_delete_path_expected_inode_precondition_still_matches_or_rejects() {
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
        "/matching-precondition.txt",
        b"matching",
        &context,
        Some("seed-matching-precondition"),
    )
    .await
    .expect("seed matching target");
    write_file_bytes(
        &store,
        &namespace_id,
        "/mismatching-precondition.txt",
        b"mismatching",
        &context,
        Some("seed-mismatching-precondition"),
    )
    .await
    .expect("seed mismatching target");
    let matching_inode = resolve_path(&store, &namespace_id, "/matching-precondition.txt")
        .await
        .expect("resolve matching target")
        .inode_id;
    let mismatching_inode = resolve_path(&store, &namespace_id, "/mismatching-precondition.txt")
        .await
        .expect("resolve mismatching target")
        .inode_id;

    delete_path_non_recursive_expecting(
        &store,
        &namespace_id,
        "/matching-precondition.txt",
        Some(matching_inode),
        &context,
        "delete-matching-precondition",
    )
    .await
    .expect("matching precondition deletes");
    let missing = resolve_path(&store, &namespace_id, "/matching-precondition.txt")
        .await
        .expect_err("matching target is deleted");
    assert_eq!(missing.code(), ErrorCode::PathNotFound);

    let error = delete_path_non_recursive_expecting(
        &store,
        &namespace_id,
        "/mismatching-precondition.txt",
        Some(InodeId(1)),
        &context,
        "delete-mismatching-precondition",
    )
    .await
    .expect_err("mismatching precondition must fail planning");
    assert!(matches!(
        error,
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_inode_id: Some(InodeId(1)),
            actual_inode_id: Some(actual),
            ..
        }) if actual == mismatching_inode
    ));
    assert_eq!(
        resolve_path(&store, &namespace_id, "/mismatching-precondition.txt")
            .await
            .expect("mismatching target remains")
            .inode_id,
        mismatching_inode
    );
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
    let first = publish_namespace_commits_batch(
        &store,
        &namespace_id("demo"),
        vec![
            prepared_candidate(
                &store,
                &namespace_id("demo"),
                CommitRequest::single(
                    commit_id.clone(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/idempotent.txt").expect("path"),
                        content_ref: content.content_ref().clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
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
        .delete(content.object_key())
        .await
        .expect("delete committed content blob");

    // The retry carries no preparation proof at all, so a receipt miss would
    // fail it: returning the first commit's sequence is what proves the
    // receipt is consulted before content is validated.
    let retry = publish_namespace_commits_batch(
        &store,
        &namespace_id("demo"),
        vec![CommitCandidate::new(CommitRequest::single(
            commit_id,
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/docs/idempotent.txt").expect("path"),
                content_ref: content.into_content_ref(),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ))],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("single response")
    .expect("idempotent retry should return existing receipt");

    assert_eq!(retry.committed_seq, first.committed_seq);
}

fn directory_with_preconditions(
    commit_id: &str,
    path: &str,
    expected_head_seq: ChangeSeq,
) -> CommitRequest {
    commit_request(
        commit_id,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(path).expect("path"),
            parents: false,
        },
    )
    .preconditions(vec![loonfs_api::CommitPrecondition::NamespaceHead {
        expected_head_seq,
    }])
}

#[tokio::test]
async fn head_preconditions_use_admitted_pre_state_and_receipts_resolve_first() {
    for second_expected in [ChangeSeq(0), ChangeSeq(1)] {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = namespace_id("demo");
        let context = mutation_context();
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        let first = directory_with_preconditions("first", "/first", ChangeSeq(0));
        let second = directory_with_preconditions("second", "/second", second_expected);
        let next_seq = if second_expected == ChangeSeq(0) {
            2
        } else {
            3
        };
        let results = publish_namespace_commits_batch(
            &store,
            &namespace_id,
            vec![
                CommitCandidate::new(first.clone()),
                CommitCandidate::new(second),
                CommitCandidate::new(directory_with_preconditions(
                    "next",
                    "/next",
                    ChangeSeq(next_seq - 1),
                )),
            ],
            &context,
        )
        .await;
        let landed = results[0].as_ref().expect("first commit");
        assert_eq!(landed.committed_seq, ChangeSeq(1));
        if second_expected == ChangeSeq(0) {
            let error = results[1]
                .as_ref()
                .expect_err("second precondition is stale");
            assert_eq!(error.code(), ErrorCode::StaleHead);
            let details = error.details().expect("precondition details");
            assert_eq!(details.expected_head_seq, Some(ChangeSeq(0)));
            assert_eq!(details.actual_head_seq, Some(ChangeSeq(1)));
            assert_eq!(details.precondition_index, Some(0));
            assert_eq!(details.operation_index, None);
        } else {
            assert_eq!(
                results[1].as_ref().expect("second commit").committed_seq,
                ChangeSeq(2)
            );
        }
        assert_eq!(
            results[2].as_ref().expect("next commit").committed_seq,
            ChangeSeq(next_seq)
        );
        let head = load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("head");
        assert_eq!(head.next_inode_id, InodeId(next_seq + 2));
        let replay = submit_commit(&store, &namespace_id, first.clone(), &context)
            .await
            .expect("receipt resolves despite stale precondition");
        assert_eq!(&replay, landed);
        let changed = first.preconditions(vec![loonfs_api::CommitPrecondition::NamespaceHead {
            expected_head_seq: ChangeSeq(next_seq),
        }]);
        let conflict = submit_commit(&store, &namespace_id, changed, &context)
            .await
            .expect_err("changed precondition changes identity");
        assert_eq!(conflict.code(), ErrorCode::CommitIdReuseConflict);
    }
}

fn scoped_directory(
    commit_id: &str,
    preconditions: Vec<loonfs_api::CommitPrecondition>,
) -> CommitRequest {
    commit_request(
        commit_id,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(format!("/{commit_id}")).expect("path"),
            parents: false,
        },
    )
    .preconditions(preconditions)
}

fn precondition_details(
    result: &Result<loonfs_api::CommitResponse, CoreError>,
    code: ErrorCode,
    index: u32,
) -> loonfs_api::ErrorDetails {
    let error = result.as_ref().expect_err("precondition fails");
    assert_eq!(error.code(), code);
    let details = error.details().expect("precondition details");
    assert_eq!(details.precondition_index, Some(index));
    assert_eq!(details.operation_index, None);
    details
}

#[tokio::test]
async fn file_revision_preconditions_ignore_unrelated_commits_and_reject_rewrites_and_deletion() {
    use loonfs_api::{CommitPrecondition, RevisionNo};

    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/input",
        b"input",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed");
    let inode_id = resolve_path(&store, &namespace_id, "/input")
        .await
        .expect("input")
        .inode_id;
    let content = store_bytes_as_content(&store, &namespace_id, b"rewrite")
        .await
        .expect("content");
    let precondition = CommitPrecondition::FileRevision {
        inode_id,
        expected_revision_no: RevisionNo(1),
    };
    let put = |path: &str| FilesystemOperation::PutFile {
        path: AbsolutePath::parse(path).expect("path"),
        content_ref: content.content_ref().clone(),
        behavior: DestinationBehavior::Replace,
        expected_inode_id: None,
        expected_revision_no: None,
    };
    let results = submit_commits_batch(
        &store,
        &namespace_id,
        vec![
            commit_request("unrelated", put("/other")),
            scoped_directory("holds", vec![precondition.clone()]),
            commit_request("rewrite", put("/input")),
            scoped_directory("stale", vec![precondition.clone()]),
            commit_request(
                "delete",
                FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/input").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                },
            ),
            scoped_directory("deleted", vec![precondition]),
            scoped_directory(
                "later",
                vec![CommitPrecondition::PathAbsence {
                    path: AbsolutePath::parse("/stale").expect("path"),
                }],
            ),
        ],
        &context,
    )
    .await;
    for index in [0, 1, 2, 4, 6] {
        results[index].as_ref().expect("admitted candidate");
    }
    let stale = precondition_details(&results[3], ErrorCode::StaleRevision, 0);
    assert_eq!(stale.inode_id, Some(inode_id));
    assert_eq!(stale.expected_revision_no, Some(RevisionNo(1)));
    assert_eq!(stale.actual_revision_no, Some(RevisionNo(2)));
    let deleted = precondition_details(&results[5], ErrorCode::StaleRevision, 0);
    assert_eq!(deleted.inode_id, Some(inode_id));
    assert_eq!(deleted.expected_revision_no, Some(RevisionNo(1)));
    assert_eq!(deleted.actual_revision_no, None);
    assert_eq!(
        results[6].as_ref().expect("later").committed_seq,
        ChangeSeq(6)
    );
    let head = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("head");
    assert_eq!(head.next_inode_id, InodeId(6));
}

#[tokio::test]
async fn binding_preconditions_track_identity_absence_and_moves() {
    use loonfs_api::CommitPrecondition;

    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for (path, commit_id) in [("/input", "seed"), ("/other", "other")] {
        put_file_bytes(
            &store,
            &namespace_id,
            path,
            b"input",
            DestinationBehavior::NoReplace,
            &context,
            Some(commit_id),
        )
        .await
        .expect("seed file");
    }
    let input = resolve_path(&store, &namespace_id, "/input")
        .await
        .expect("input");
    let other = resolve_path(&store, &namespace_id, "/other")
        .await
        .expect("other");
    let binding = CommitPrecondition::PathBinding {
        path: AbsolutePath::parse("/input").expect("path"),
        expected_inode_id: input.inode_id,
        expected_binding_generation: None,
    };
    let generation = CommitPrecondition::PathBinding {
        path: AbsolutePath::parse("/input").expect("path"),
        expected_inode_id: input.inode_id,
        expected_binding_generation: input.binding_generation.clone(),
    };
    assert!(input.binding_generation.is_some());
    let absent = CommitPrecondition::PathAbsence {
        path: AbsolutePath::parse("/vacant").expect("path"),
    };
    let move_file = |from: &str, to: &str| FilesystemOperation::MovePath {
        from_path: AbsolutePath::parse(from).expect("source"),
        to_path: AbsolutePath::parse(to).expect("destination"),
        precondition: loonfs_api::DestinationPrecondition {
            behavior: DestinationBehavior::Replace,
            ..Default::default()
        },
    };
    let results = submit_commits_batch(
        &store,
        &namespace_id,
        vec![
            scoped_directory("unrelated", vec![]),
            scoped_directory(
                "holds",
                vec![
                    binding.clone(),
                    generation.clone(),
                    absent.clone(),
                    CommitPrecondition::PathAbsence {
                        path: AbsolutePath::parse("/missing/child").expect("path"),
                    },
                    CommitPrecondition::PathAbsence {
                        path: AbsolutePath::parse("/input/child").expect("path"),
                    },
                    CommitPrecondition::PathBinding {
                        path: AbsolutePath::root(),
                        expected_inode_id: InodeId(1),
                        expected_binding_generation: None,
                    },
                ],
            ),
            commit_request("away", move_file("/input", "/away")),
            commit_request("back", move_file("/away", "/input")),
            scoped_directory("moved", vec![generation]),
            scoped_directory("same-inode", vec![binding.clone()]),
            commit_request("rebind", move_file("/other", "/input")),
            scoped_directory("rebound", vec![binding]),
            commit_request("bind-vacant", move_file("/input", "/vacant")),
            scoped_directory("now-bound", vec![absent]),
            scoped_directory(
                "root-bound",
                vec![CommitPrecondition::PathAbsence {
                    path: AbsolutePath::root(),
                }],
            ),
        ],
        &context,
    )
    .await;
    for index in [0, 1, 2, 3, 5, 6, 8] {
        results[index].as_ref().expect("admitted candidate");
    }
    let moved = precondition_details(&results[4], ErrorCode::BindingGenerationMismatch, 0);
    assert_eq!(moved.inode_id, Some(input.inode_id));
    let rebound = precondition_details(&results[7], ErrorCode::PathConflict, 0);
    assert_eq!(rebound.expected_inode_id, Some(input.inode_id));
    assert_eq!(rebound.actual_inode_id, Some(other.inode_id));
    let now_bound = precondition_details(&results[9], ErrorCode::PathConflict, 0);
    assert_eq!(now_bound.expected_inode_id, None);
    assert_eq!(now_bound.actual_inode_id, Some(other.inode_id));
    let root = precondition_details(&results[10], ErrorCode::PathConflict, 0);
    assert_eq!(root.expected_inode_id, None);
    assert_eq!(root.actual_inode_id, Some(InodeId(1)));
}

#[tokio::test]
async fn attributes_preconditions_ignore_content_rewrites_and_reject_attribute_updates() {
    use loonfs_api::{AttributeKey, AttributeRevisionNo, AttributeValue, CommitPrecondition};
    use std::collections::BTreeMap;

    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/input",
        b"input",
        DestinationBehavior::NoReplace,
        &context,
        Some("seed"),
    )
    .await
    .expect("seed");
    let inode_id = resolve_path(&store, &namespace_id, "/input")
        .await
        .expect("input")
        .inode_id;
    let content = store_bytes_as_content(&store, &namespace_id, b"rewrite")
        .await
        .expect("content");
    let precondition = CommitPrecondition::AttributesRevision {
        inode_id,
        expected_attributes_revision_no: AttributeRevisionNo(0),
    };
    let results = submit_commits_batch(
        &store,
        &namespace_id,
        vec![
            commit_request(
                "rewrite",
                FilesystemOperation::PutFileRevisionByInode {
                    inode_id,
                    content_ref: content.content_ref().clone(),
                    expected_revision_no: loonfs_api::RevisionNo(1),
                },
            ),
            scoped_directory("holds", vec![precondition.clone()]),
            commit_request(
                "update",
                FilesystemOperation::UpdateAttributes {
                    path: AbsolutePath::parse("/input").expect("path"),
                    set: BTreeMap::from([(
                        AttributeKey::parse("owner").expect("key"),
                        AttributeValue::parse("hopper").expect("value"),
                    )]),
                    remove: vec![],
                    expected_inode_id: None,
                    expected_attributes_revision_no: None,
                },
            ),
            scoped_directory("stale", vec![precondition]),
        ],
        &context,
    )
    .await;
    for result in &results[..3] {
        result.as_ref().expect("admitted candidate");
    }
    let stale = precondition_details(&results[3], ErrorCode::StaleAttributes, 0);
    assert_eq!(stale.inode_id, Some(inode_id));
    assert_eq!(
        stale.expected_attributes_revision_no,
        Some(AttributeRevisionNo(0))
    );
    assert_eq!(
        stale.actual_attributes_revision_no,
        Some(AttributeRevisionNo(1))
    );
}

#[tokio::test]
async fn mixed_preconditions_report_the_first_failure_and_write_nothing() {
    use loonfs_api::{CommitPrecondition, RevisionNo};
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};

    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine
        .publish_batch(
            &store,
            vec![CommitCandidate::new(scoped_directory("seed", vec![]))],
            &context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .remove(0)
        .expect("seed");
    let head = CommitPrecondition::NamespaceHead {
        expected_head_seq: ChangeSeq(0),
    };
    let scoped = CommitPrecondition::FileRevision {
        inode_id: InodeId(99),
        expected_revision_no: RevisionNo(1),
    };
    let valid = CommitPrecondition::NamespaceHead {
        expected_head_seq: ChangeSeq(1),
    };
    store.take();
    for (index, (preconditions, code, failed_index)) in [
        (vec![head.clone(), scoped.clone()], ErrorCode::StaleHead, 0),
        (vec![scoped.clone(), head], ErrorCode::StaleRevision, 0),
        (vec![valid, scoped], ErrorCode::StaleRevision, 1),
    ]
    .into_iter()
    .enumerate()
    {
        let result = engine
            .publish_batch(
                &store,
                vec![CommitCandidate::new(scoped_directory(
                    &format!("mixed-{index}"),
                    preconditions,
                ))],
                &context,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .remove(0);
        precondition_details(&result, code, failed_index);
    }
    let counts = store.counts();
    assert_eq!(counts.puts, 0);
    assert_eq!(counts.compare_and_swaps, 0);
    assert_eq!(counts.deletes, 0);
}

#[tokio::test]
async fn precondition_limit_rejects_before_planning_and_writes_nothing() {
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};

    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine
        .publish_batch(
            &store,
            vec![CommitCandidate::new(directory_with_preconditions(
                "seed",
                "/seed",
                ChangeSeq(0),
            ))],
            &context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .remove(0)
        .expect("seed commit");
    store.take();
    let request = directory_with_preconditions("over-limit", "/missing/child", ChangeSeq(0))
        .preconditions(vec![
            loonfs_api::CommitPrecondition::NamespaceHead {
                expected_head_seq: ChangeSeq(0),
            };
            loonfs_core::limits::MAX_COMMIT_PRECONDITIONS + 1
        ]);
    let error = engine
        .publish_batch(
            &store,
            vec![CommitCandidate::new(request)],
            &context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .remove(0)
        .expect_err("precondition limit");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert!(error
        .to_string()
        .contains("1025 preconditions; maximum is 1024"));
    let counts = store.counts();
    assert_eq!(counts.puts, 0);
    assert_eq!(counts.compare_and_swaps, 0);
    assert_eq!(counts.deletes, 0);
}
