//! Batch publication, replay, acknowledgement loss, limits, and fencing guards.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::{
    v0::FilesystemChange,
    wire::control::{decode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope},
    wire::wal::{decode_wal_segment_envelope_zstd, WalDelta},
    AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId,
    InodeKind, NamespaceId,
};
use loonfs_core::commit::CommitValidationError;
use loonfs_core::content::{prepare_existing_content_ref, store_bytes_as_content};
use loonfs_core::control::load_namespace_head_control;
use loonfs_core::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, NamespaceCommitEngine, PublishTailOptions,
};
use loonfs_core::{Error as CoreError, ErrorCode, MutationContext};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{FailStore, InjectedError, OperationContext, OperationKind};
use std::path::Path;
use std::sync::Mutex;
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
            absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
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
        None,
        operation,
    )
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
                    absolute_path: AbsolutePath::parse("/docs/cycled.txt").expect("path"),
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
                        absolute_path: AbsolutePath::parse("/docs/cycled.txt").expect("path"),
                        content_ref: staged.content_ref,
                        behavior: DestinationBehavior::NoReplace,
                        expected_revision_no: None,
                    },
                ),
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
                FilesystemOperation::CreateDir {
                    absolute_path: AbsolutePath::parse("/alpha").expect("path"),
                    parents: false,
                },
            ),
            commit_request(
                "req-batch-b",
                FilesystemOperation::CreateDir {
                    absolute_path: AbsolutePath::parse("/beta").expect("path"),
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
    assert_eq!(changes.changes[0].events.len(), 1);
    assert!(matches!(
        &changes.changes[0].events[0],
        FilesystemChange::Created {
            inode_kind: InodeKind::Directory,
            parent_inode_id: InodeId(1),
            name,
            revision_no: None,
            content_ref: None,
            ..
        } if name.as_str() == "alpha"
    ));
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
    let put = || {
        commit_request(
            "ack-lost-put",
            FilesystemOperation::PutFile {
                absolute_path: AbsolutePath::parse("/ack.txt").expect("path"),
                content_ref: content.content_ref.clone(),
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            },
        )
    };

    // The CAS landed but its acknowledgment was lost: this must surface as
    // an unknown outcome, never as definite failure.
    let error = submit_commit(&store, &namespace_id, put(), &context)
        .await
        .expect_err("ack-lost head CAS is not definite failure");
    assert_eq!(error.code(), ErrorCode::CommitOutcomeUnknown);
    assert!(store.injected_ack_loss());

    // The documented remedy: retry with the same commit id. The commit is
    // already visible, so the retry replays it instead of double-committing.
    let result = submit_commit(&store, &namespace_id, put(), &context)
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
    let put = commit_request(
        "retry-after-orphan",
        FilesystemOperation::PutFile {
            absolute_path: AbsolutePath::parse("/retry.txt").expect("path"),
            content_ref: content.content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        },
    );
    let error = submit_commit(&store, &namespace_id, put.clone(), &context)
        .await
        .expect_err("injected stale head surfaces to the caller");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    assert!(store.injected_stale_head());

    // The orphaned segment from the failed attempt must not block a retry
    // with the same commit id.
    let result = submit_commit(&store, &namespace_id, put, &context)
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
            CommitCandidate::new(commit_request(
                "reject-materialization",
                FilesystemOperation::DeletePath {
                    absolute_path: AbsolutePath::parse("/missing.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                },
            )),
            // Accepted into the batch.
            prepared_candidate(
                &store,
                &namespace_id,
                commit_request(
                    "accept-a",
                    FilesystemOperation::PutFile {
                        absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content_ref: content.content_ref.clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
            // Rejected only because of the accepted candidate's speculative
            // in-batch create.
            prepared_candidate(
                &store,
                &namespace_id,
                commit_request(
                    "reject-speculative",
                    FilesystemOperation::PutFile {
                        absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content_ref: content.content_ref.clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
            // Alias of the materialization-decided rejection.
            CommitCandidate::new(commit_request(
                "reject-materialization",
                FilesystemOperation::DeletePath {
                    absolute_path: AbsolutePath::parse("/missing.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: None,
                },
            )),
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

    // Nothing became durable, so the retry the batch error asks for derives
    // every verdict from durable state.
    let head = load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("load head");
    assert_eq!(head.state.seq, ChangeSeq(0));

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
            CommitCandidate::new(commit_request(
                "reject-materialization",
                FilesystemOperation::DeletePath {
                    absolute_path: AbsolutePath::parse("/missing.txt").expect("path"),
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
                        absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content_ref: content.content_ref.clone(),
                        behavior: DestinationBehavior::NoReplace,
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
                        absolute_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content_ref: content.content_ref.clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
        ]
    };

    let failed =
        publish_namespace_commits_batch(&store, &namespace_id, batch().await, &context).await;
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
        publish_namespace_commits_batch(&store, &namespace_id, batch().await, &context).await;
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

    // One engine across the rest of the test, so the injected stale read
    // lands on the publish-view load and not on an epoch acquisition —
    // acquisition reads the head too, and a stale read there is a different
    // failure. The engine acquires once, on this warm-up publish.
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    let publish = async |engine: &mut NamespaceCommitEngine, path: &str, commit_id: &str| {
        let mkdir = commit_request(
            commit_id,
            FilesystemOperation::CreateDir {
                absolute_path: AbsolutePath::parse(path).expect("path"),
                parents: false,
            },
        );
        engine
            .publish_batch(
                &store,
                vec![CommitCandidate::new(mkdir)],
                &context,
                &PublishTailOptions::default(),
            )
            .await
            .results
            .pop()
            .expect("one publish result")
    };
    let warm = publish(&mut engine, "/parent/warm", "mkdir-warm")
        .await
        .expect("warm-up publish acquires the epoch");
    assert_eq!(warm.committed_seq, ChangeSeq(4));

    store.inject_stale_head_get_after(0);
    let error = publish(&mut engine, "/parent/child", "mkdir-child")
        .await
        .expect_err("injected stale head read surfaces to the caller");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    assert!(store.injected_stale_head_get());

    // The failed attempt must not block a retry with the same commit id.
    let result = publish(&mut engine, "/parent/child", "mkdir-child")
        .await
        .expect("retry after the stale head read");
    assert_eq!(result.committed_seq, ChangeSeq(5));
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
    assert_eq!(head.state.seq, ChangeSeq(5));
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
        FilesystemOperation::CreateDir {
            absolute_path: AbsolutePath::parse("/alpha").expect("path"),
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
    let put = commit_request(
        "over-proof-replay",
        FilesystemOperation::PutFile {
            absolute_path: AbsolutePath::parse("/proof-replay.txt").expect("path"),
            content_ref: stored.content_ref,
            behavior: DestinationBehavior::NoReplace,
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
    let put = commit_request(
        "over-proof-duplicate",
        FilesystemOperation::PutFile {
            absolute_path: AbsolutePath::parse("/batch-proof.txt").expect("path"),
            content_ref: stored.content_ref,
            behavior: DestinationBehavior::NoReplace,
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
        commit_id: CommitId::parse("over-operation-new").expect("valid commit id"),
        message: None,
        operations: (0..=loonfs_core::limits::MAX_COMMIT_OPERATIONS)
            .map(|index| FilesystemOperation::CreateDir {
                absolute_path: AbsolutePath::parse(format!("/over-operation-{index}"))
                    .expect("path"),
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
        FilesystemOperation::CreateDir {
            absolute_path: AbsolutePath::parse("/alpha").expect("path"),
            parents: false,
        },
    );

    let first = submit_commit(&store, &namespace_id, mkdir.clone(), &writer_a)
        .await
        .expect("writer a commit");
    let writer_b = MutationContext {
        writer_id: "writer-b".to_owned(),
        now_ms: writer_a.now_ms.saturating_add(1),
    };

    let retry = submit_commit(&store, &namespace_id, mkdir, &writer_b)
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

    let responses = submit_commits_batch(
        &store,
        &namespace_id,
        vec![
            commit_request(
                "req-conflict",
                FilesystemOperation::CreateDir {
                    absolute_path: AbsolutePath::parse("/alpha").expect("path"),
                    parents: false,
                },
            ),
            commit_request(
                "req-conflict",
                FilesystemOperation::CreateDir {
                    absolute_path: AbsolutePath::parse("/beta").expect("path"),
                    parents: false,
                },
            ),
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
            absolute_path: AbsolutePath::parse("/same/path.txt").expect("path"),
            content_ref: content.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
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
                absolute_path: AbsolutePath::parse("/same/path.txt").expect("path"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            },
        ),
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
                    None,
                    FilesystemOperation::PutFile {
                        absolute_path: AbsolutePath::parse("/docs/idempotent.txt").expect("path"),
                        content_ref: content.content_ref.clone(),
                        behavior: DestinationBehavior::NoReplace,
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
        .delete(&content.object_key)
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
            None,
            FilesystemOperation::PutFile {
                absolute_path: AbsolutePath::parse("/docs/idempotent.txt").expect("path"),
                content_ref: content.content_ref,
                behavior: DestinationBehavior::NoReplace,
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
