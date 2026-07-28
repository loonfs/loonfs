//! Writer epoch acquisition: the last-writer-wins fencing that keeps two
//! sessions from publishing interleaved commits.

use crate::context::MutationContext;
use crate::control_update::{update_head, ControlUpdateError, HeadUpdate};
use crate::namespace::control::ControlObjectLoadError;
use loonfs_api::wire::control::{AcquiredWriter, HeadState, NamespaceState, WriterBlock};
use loonfs_api::{NamespaceId, WriterEpoch};
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_WRITER_EPOCH_ACQUIRE_ATTEMPTS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WriterEpochAcquireError {
    // Control loads map differently from head writes, so conversion stays explicit.
    #[error(transparent)]
    LoadHead(ControlObjectLoadError),
    #[error("namespace `{namespace_id}` is deleted")]
    NamespaceDeleted { namespace_id: NamespaceId },
    #[error("empty writer id")]
    EmptyWriterId,
    #[error("empty writer session id")]
    EmptyWriterSessionId,
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error("writer epoch overflow from `{active}`")]
    WriterEpochOverflow { active: WriterEpoch },
    #[error("failed to write head object during writer epoch acquire: {0}")]
    HeadWrite(String),
    #[error("writer epoch acquire retries exhausted after {attempts} attempts")]
    RetryExhausted { attempts: usize },
}

/// Acquires the namespace writer epoch for a writer session.
///
/// Lazy by design: sessions call this on their first semantic write, not at
/// open, and cache the result. Acquisition bumps `writer_epoch` and records
/// the non-authoritative `writer` block, which fences every other session at
/// its next publish. There is no lease and no expiry: nothing arbitrates
/// between two live writers except the epoch itself, so acquisition never
/// refuses a live caller and contention resolves as deterministic
/// last-writer-wins. A session that has been fenced must not call this again
/// on its own; reacquisition is an explicit caller decision.
///
/// The one refusal is terminal state: a deleted namespace's head is an
/// immutable tombstone, so acquisition fails with `NamespaceDeleted` before
/// any CAS — no attempt may rewrite the tombstone, inflate its epoch, or
/// name a "current writer" for a dead namespace.
///
/// The only non-bumping success path is idempotent retry: when the head's
/// writer block already names this exact session, its current epoch is
/// returned without a CAS.
pub(crate) async fn acquire_writer_epoch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AcquiredWriter, WriterEpochAcquireError> {
    if context.writer_id.trim().is_empty() {
        return Err(WriterEpochAcquireError::EmptyWriterId);
    }
    if context.writer_session_id.trim().is_empty() {
        return Err(WriterEpochAcquireError::EmptyWriterSessionId);
    }

    update_head(
        store,
        namespace_id,
        &context.writer_version,
        MAX_WRITER_EPOCH_ACQUIRE_ATTEMPTS,
        |loaded_head| {
            let head = &loaded_head.envelope.state;
            // Terminal-state guard before the idempotent-retry path: even
            // the session named in the tombstone's writer block must not get
            // an epoch back for a deleted namespace.
            if head.state == NamespaceState::Deleted {
                return Err(WriterEpochAcquireError::NamespaceDeleted {
                    namespace_id: head.namespace_id.clone(),
                });
            }
            if let Some(writer) = head.writer.as_ref() {
                if writer.writer_id == context.writer_id
                    && writer.writer_session_id == context.writer_session_id
                {
                    return Ok(HeadUpdate::Noop(AcquiredWriter {
                        writer_id: context.writer_id.clone(),
                        writer_session_id: context.writer_session_id.clone(),
                        writer_epoch: head.writer_epoch,
                    }));
                }
            }

            let next_epoch = next_writer_epoch(head.writer_epoch)?;
            Ok(HeadUpdate::Replace {
                next: Box::new(head_with_writer(head, next_epoch, context)),
                outcome: AcquiredWriter {
                    writer_id: context.writer_id.clone(),
                    writer_session_id: context.writer_session_id.clone(),
                    writer_epoch: next_epoch,
                },
            })
        },
    )
    .await
}

fn next_writer_epoch(active: WriterEpoch) -> Result<WriterEpoch, WriterEpochAcquireError> {
    active
        .0
        .checked_add(1)
        .map(WriterEpoch)
        .ok_or(WriterEpochAcquireError::WriterEpochOverflow { active })
}

fn head_with_writer(
    current_head: &HeadState,
    writer_epoch: WriterEpoch,
    context: &MutationContext,
) -> HeadState {
    HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: current_head.seq,
        head_commit_id: current_head.head_commit_id.clone(),
        writer_epoch,
        writer: Some(WriterBlock {
            writer_id: context.writer_id.clone(),
            writer_session_id: context.writer_session_id.clone(),
            acquired_at_ms: context.now_ms,
        }),
        next_inode_id: current_head.next_inode_id,
        visible_wal_tip: current_head.visible_wal_tip.clone(),
        recent_segments: current_head.recent_segments.clone(),
        state: current_head.state,
    }
}

impl From<ControlUpdateError> for WriterEpochAcquireError {
    fn from(value: ControlUpdateError) -> Self {
        match value {
            ControlUpdateError::LoadHead(error) => Self::LoadHead(error),
            ControlUpdateError::MissingEtag { object_key } => Self::MissingHeadEtag { object_key },
            ControlUpdateError::Codec {
                object_key,
                message,
            }
            | ControlUpdateError::Store {
                object_key,
                message,
            } => Self::HeadWrite(format!("`{object_key}`: {message}")),
            ControlUpdateError::RetryExhausted { attempts } => Self::RetryExhausted { attempts },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_engine::delete_namespace;
    use crate::commit_engine::{NamespaceCommitEngine, NamespaceMutationCandidate};
    use crate::error::ErrorCode;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::read_head_object;
    use crate::options::DeleteNamespaceOptions;

    async fn commit_operations<S: loonfs_objectstore::ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        request: ApiCommitRequest,
        context: &crate::context::MutationContext,
    ) -> crate::error::Result<loonfs_api::v0::CommitResponse> {
        let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
        engine
            .publish_batch(
                store,
                vec![NamespaceMutationCandidate::commit(request)],
                context,
                &crate::protocol::PublishTailOptions::default(),
            )
            .await
            .results
            .pop()
            .expect("one commit result")
    }
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::v0::{CommitOp as ApiCommitOp, CommitRequest as ApiCommitRequest};
    use loonfs_api::wire::control::{
        decode_control_object, encode_control_object, ControlObjectKind, HeadStateEnvelope,
        NamespaceState,
    };
    use loonfs_api::{ChangeSeq, CommitId, InodeId, NamespaceId};
    use loonfs_objectstore::keys::wal_head;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{
        ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    const WRITER_VERSION: &str = "writer/0.1.0";

    fn context(writer_id: &str, writer_session_id: &str, now_ms: u64) -> MutationContext {
        MutationContext {
            writer_id: writer_id.to_owned(),
            writer_session_id: writer_session_id.to_owned(),
            writer_version: WRITER_VERSION.to_owned(),
            now_ms,
        }
    }

    fn head_with_session(
        namespace_id: &NamespaceId,
        writer_id: &str,
        writer_session_id: &str,
        writer_epoch: WriterEpoch,
    ) -> HeadState {
        let mut head = HeadState::initial(namespace_id.clone());
        head.writer_epoch = writer_epoch;
        head.writer = Some(WriterBlock {
            writer_id: writer_id.to_owned(),
            writer_session_id: writer_session_id.to_owned(),
            acquired_at_ms: 500,
        });
        head
    }

    async fn write_head(store: &LocalFsStore, namespace_id: &NamespaceId, head: HeadState) {
        let envelope =
            HeadStateEnvelope::from_state(ControlObjectKind::WalHead, WRITER_VERSION, head)
                .expect("head envelope");
        let bytes = encode_control_object(&envelope).expect("head bytes");
        store
            .put_if_absent(&wal_head(namespace_id.as_str()), Bytes::from(bytes))
            .await
            .expect("write head");
    }

    async fn head_etag(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
        store
            .head(&wal_head(namespace_id.as_str()))
            .await
            .expect("head metadata")
            .expect("head exists")
            .etag
            .expect("head etag")
    }

    #[tokio::test]
    async fn same_session_reuses_active_epoch_without_rewriting_head() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &store,
            &namespace_id,
            head_with_session(&namespace_id, "writer", "session-a", WriterEpoch(7)),
        )
        .await;
        let etag_before = head_etag(&store, &namespace_id).await;

        let acquired = acquire_writer_epoch(
            &store,
            &namespace_id,
            &context("writer", "session-a", 1_000),
        )
        .await
        .expect("acquire writer");

        assert_eq!(acquired.writer_epoch, WriterEpoch(7));
        assert_eq!(head_etag(&store, &namespace_id).await, etag_before);
    }

    #[tokio::test]
    async fn restarted_writer_session_bumps_epoch() {
        // Same writer id, new session: the restarted process fences its own
        // predecessor instead of silently sharing its epoch.
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &store,
            &namespace_id,
            head_with_session(&namespace_id, "writer", "session-a", WriterEpoch(7)),
        )
        .await;

        let acquired = acquire_writer_epoch(
            &store,
            &namespace_id,
            &context("writer", "session-b", 1_000),
        )
        .await
        .expect("acquire writer");

        assert_eq!(acquired.writer_epoch, WriterEpoch(8));
        assert_eq!(acquired.writer_session_id, "session-b");
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.writer_epoch, WriterEpoch(8));
        assert_eq!(
            head.writer.expect("writer block").writer_session_id,
            "session-b"
        );
    }

    #[tokio::test]
    async fn acquire_on_deleted_namespace_is_rejected_and_leaves_the_tombstone_unchanged() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        // The tombstone names the deleting session in its writer block: even
        // that exact session must be refused, so the guard precedes the
        // idempotent-retry path.
        let mut tombstone = head_with_session(&namespace_id, "writer", "session-a", WriterEpoch(7));
        tombstone.state = NamespaceState::Deleted;
        write_head(&store, &namespace_id, tombstone).await;
        let etag_before = head_etag(&store, &namespace_id).await;

        for session in ["session-a", "session-b"] {
            let error =
                acquire_writer_epoch(&store, &namespace_id, &context("writer", session, 1_000))
                    .await
                    .expect_err("acquire on a deleted namespace must be refused");
            assert!(matches!(
                &error,
                WriterEpochAcquireError::NamespaceDeleted { namespace_id: deleted_id }
                    if *deleted_id == namespace_id
            ));
            assert_eq!(
                crate::error::CoreError::from(error).code(),
                ErrorCode::NamespaceDeleted
            );
        }

        // The tombstone is byte-identical after every attempt: no epoch
        // inflation, no new writer block, no churn on a terminal object.
        assert_eq!(head_etag(&store, &namespace_id).await, etag_before);
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.state, NamespaceState::Deleted);
        assert_eq!(head.writer_epoch, WriterEpoch(7));
    }

    #[tokio::test]
    async fn deleting_an_already_deleted_namespace_still_answers_namespace_deleted() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a", "session-a", 1_000);
        bootstrap_namespace(&store, &namespace_id, &writer, false)
            .await
            .expect("bootstrap");
        delete_namespace(
            &store,
            &namespace_id,
            DeleteNamespaceOptions::default(),
            &writer,
        )
        .await
        .expect("first delete");

        // The refusal now surfaces at epoch acquire instead of inside the
        // delete loop; the public code is unchanged.
        let error = delete_namespace(
            &store,
            &namespace_id,
            DeleteNamespaceOptions::default(),
            &context("writer-a", "session-b", 2_000),
        )
        .await
        .expect_err("second delete must be refused");
        assert_eq!(error.code(), ErrorCode::NamespaceDeleted);
    }

    #[tokio::test]
    async fn new_writer_takes_over_and_records_writer_block() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &store,
            &namespace_id,
            head_with_session(&namespace_id, "writer-a", "session-a", WriterEpoch(7)),
        )
        .await;

        let acquired = acquire_writer_epoch(
            &store,
            &namespace_id,
            &context("writer-b", "session-b", 2_000),
        )
        .await
        .expect("takeover acquire");

        assert_eq!(acquired.writer_epoch, WriterEpoch(8));
        assert_eq!(acquired.writer_id, "writer-b");
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        let writer = head.writer.expect("writer block");
        assert_eq!(writer.writer_id, "writer-b");
        assert_eq!(writer.acquired_at_ms, 2_000);
    }

    fn create_dir_request(commit_id: &str, display_name: &str) -> ApiCommitRequest {
        ApiCommitRequest {
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse(display_name)
                    .expect("valid display name"),
            }],
            message: None,
        }
    }

    #[tokio::test]
    async fn one_shot_commits_reacquire_and_alternate_writers_ping_pong() {
        // Each one-shot commit is its own acquisition decision, so two
        // alternating writers fence each other back and forth instead of one
        // being locked out: deterministic last-writer-wins, every commit
        // lands exactly once.
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a", "session-a", 1_000);
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        commit_operations(
            &store,
            &namespace_id,
            create_dir_request("writer-a-first", "from-a"),
            &writer_a,
        )
        .await
        .expect("writer a first commit");

        let writer_b = context("writer-b", "session-b", 2_000);
        commit_operations(
            &store,
            &namespace_id,
            create_dir_request("writer-b-first", "from-b"),
            &writer_b,
        )
        .await
        .expect("writer b commit after takeover");

        let writer_a_again = context("writer-a", "session-a", 3_000);
        commit_operations(
            &store,
            &namespace_id,
            create_dir_request("writer-a-second", "from-a-again"),
            &writer_a_again,
        )
        .await
        .expect("writer a reacquires on its next one-shot commit");

        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.seq, ChangeSeq(3));
        assert_eq!(head.writer.expect("writer block").writer_id, "writer-a");
    }

    #[tokio::test]
    async fn stale_writer_epoch_cannot_delete_namespace_after_takeover() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a", "session-a", 1_000);
        bootstrap_namespace(
            &LocalFsStore::new(temp_dir.path()).expect("store"),
            &namespace_id,
            &writer_a,
            false,
        )
        .await
        .expect("bootstrap");

        // Rewrites the head with a writer-b takeover just before the delete
        // loop's reload (head read #2), i.e. after writer A already acquired
        // its epoch (head read #1) — the stalled-deleter interleaving.
        let store = TakeoverBetweenHeadReadsStore {
            inner: LocalFsStore::new(temp_dir.path()).expect("store"),
            head_key: wal_head(namespace_id.as_str()),
            head_reads: AtomicUsize::new(0),
        };

        // The deleting session supplies its own acquired epoch (in
        // production the commit engine's), so the interleaving is explicit:
        // acquire (head read #1), takeover, delete-loop reload (head read
        // #2).
        let delete_attempt = context("writer-a", "session-a", 2_000);
        let acquired = acquire_writer_epoch(&store, &namespace_id, &delete_attempt)
            .await
            .expect("acquire before the takeover");
        let error = crate::namespace::delete::delete_namespace(
            &store,
            &namespace_id,
            DeleteNamespaceOptions::default(),
            &delete_attempt,
            acquired,
        )
        .await
        .expect_err("stale-epoch delete must be fenced");
        assert_eq!(error.code(), ErrorCode::WriterFenced);

        let head = read_head_object(&store.inner, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.state, NamespaceState::Active);
        let writer = head.writer.expect("writer block");
        assert_eq!(writer.writer_id, "writer-b");
    }

    #[derive(Debug)]
    struct TakeoverBetweenHeadReadsStore {
        inner: LocalFsStore,
        head_key: String,
        head_reads: AtomicUsize,
    }

    impl TakeoverBetweenHeadReadsStore {
        async fn inject_takeover(&self) {
            let body = self
                .inner
                .get_with_metadata(&self.head_key)
                .await
                .expect("read head for takeover")
                .expect("head exists");
            let envelope: HeadStateEnvelope =
                decode_control_object(&body.bytes, ControlObjectKind::WalHead)
                    .expect("decode head");
            let mut head = envelope.state;
            head.writer_epoch = WriterEpoch(head.writer_epoch.0 + 1);
            head.writer = Some(WriterBlock {
                writer_id: "writer-b".to_owned(),
                writer_session_id: "session-b".to_owned(),
                acquired_at_ms: 1_500,
            });
            let next =
                HeadStateEnvelope::from_state(ControlObjectKind::WalHead, WRITER_VERSION, head)
                    .expect("head envelope");
            let bytes = encode_control_object(&next).expect("head bytes");
            self.inner
                .put(&self.head_key, Bytes::from(bytes), PutMode::Overwrite)
                .await
                .expect("write takeover head");
        }
    }

    #[async_trait]
    impl ObjectStore for TakeoverBetweenHeadReadsStore {
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

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            if key == self.head_key && self.head_reads.fetch_add(1, Ordering::SeqCst) == 1 {
                self.inject_takeover().await;
            }
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

    #[tokio::test]
    async fn losing_an_acquire_race_retries_and_takes_the_next_epoch() {
        // writer-c's first CAS loses to writer-b. With no lease there is
        // nothing to defer to: the retry observes writer-b's epoch and bumps
        // past it, so the last acquirer deterministically wins.
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        write_head(
            &inner,
            &namespace_id,
            head_with_session(&namespace_id, "writer-a", "session-a", WriterEpoch(7)),
        )
        .await;
        let store = TakeoverOnCasConflictStore {
            inner,
            namespace_id: namespace_id.clone(),
            remaining_conflicts: AtomicUsize::new(1),
        };

        let acquired = acquire_writer_epoch(
            &store,
            &namespace_id,
            &context("writer-c", "session-c", 2_000),
        )
        .await
        .expect("acquire after losing the race");

        // writer-b installed epoch 8 during the conflict; writer-c retried
        // and took 9.
        assert_eq!(acquired.writer_epoch, WriterEpoch(9));
        let head = read_head_object(&store.inner, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.writer_epoch, WriterEpoch(9));
        assert_eq!(head.writer.expect("writer block").writer_id, "writer-c");
    }

    #[derive(Debug)]
    struct TakeoverOnCasConflictStore {
        inner: LocalFsStore,
        namespace_id: NamespaceId,
        remaining_conflicts: AtomicUsize,
    }

    impl TakeoverOnCasConflictStore {
        async fn inject_winner(&self) {
            let winner =
                head_with_session(&self.namespace_id, "writer-b", "session-b", WriterEpoch(8));
            let envelope =
                HeadStateEnvelope::from_state(ControlObjectKind::WalHead, WRITER_VERSION, winner)
                    .expect("head envelope");
            let bytes = encode_control_object(&envelope).expect("head bytes");
            self.inner
                .put(
                    &wal_head(self.namespace_id.as_str()),
                    Bytes::from(bytes),
                    PutMode::Overwrite,
                )
                .await
                .expect("write winner head");
        }
    }

    #[async_trait]
    impl ObjectStore for TakeoverOnCasConflictStore {
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

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
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

        async fn compare_and_swap(
            &self,
            key: &str,
            expected_etag: &str,
            bytes: Bytes,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if self.remaining_conflicts.load(Ordering::SeqCst) > 0 {
                self.remaining_conflicts.fetch_sub(1, Ordering::SeqCst);
                self.inject_winner().await;
                return Err(ObjectStoreError::PreconditionFailed {
                    object_key: key.to_owned(),
                });
            }
            self.inner.compare_and_swap(key, expected_etag, bytes).await
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
}
