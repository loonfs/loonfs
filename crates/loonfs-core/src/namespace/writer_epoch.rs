//! Writer epoch acquisition: the last-writer-wins fencing that keeps two
//! sessions from publishing interleaved commits.

use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::control_update::{update_head, ControlUpdateError, HeadReplacement};
use crate::error::StoreFailureClass;
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
    #[error("writer epoch overflow from `{active}`")]
    WriterEpochOverflow { active: WriterEpoch },
    #[error("failed to write head object `{object_key}` during writer epoch acquire: {message}")]
    HeadWrite {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("writer epoch acquire retries exhausted after {attempts} attempts")]
    RetryExhausted { attempts: usize },
}

/// Acquires the namespace writer epoch for one writer session.
///
/// A session acquires lazily on its first write and caches the result.
/// Acquisition increments `writer_epoch`; any older session is fenced on its
/// next publish. There is no lease or expiry, so concurrent acquisitions use
/// last-writer-wins ordering.
///
/// Deleted namespaces reject acquisition before the compare-and-swap. A
/// fenced session may reacquire only when its caller explicitly starts a new
/// writer session.
///
/// Every acquisition attempt increments the epoch, even when the same writer
/// ID is already recorded. Writer IDs do not identify sessions, and the
/// commit engine normally calls this only once per session.
pub(crate) async fn acquire_writer_epoch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AcquiredWriter, WriterEpochAcquireError> {
    if context.writer_id.trim().is_empty() {
        return Err(WriterEpochAcquireError::EmptyWriterId);
    }

    update_head(
        store,
        namespace_id,
        MAX_WRITER_EPOCH_ACQUIRE_ATTEMPTS,
        |loaded_head| {
            let head = &loaded_head.state;
            // Check for deletion before incrementing the epoch. A deleted namespace
            // never grants another writer epoch, including to the writer recorded in
            // its tombstone.
            if head.state == NamespaceState::Deleted {
                return Err(WriterEpochAcquireError::NamespaceDeleted {
                    namespace_id: head.namespace_id.clone(),
                });
            }

            let next_epoch = next_writer_epoch(head.writer_epoch)?;
            Ok(HeadReplacement {
                next: Box::new(head_with_writer(head, next_epoch, context)),
                outcome: AcquiredWriter {
                    writer_id: context.writer_id.clone(),
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
        // Epoch acquisition rewrites the head, so it carries the
        // namespace's immutable identity forward like every other
        // successor.
        content_store_id: current_head.content_store_id.clone(),
        fork_basis: current_head.fork_basis.clone(),
        seq: current_head.seq,
        head_commit_id: current_head.head_commit_id.clone(),
        writer_epoch,
        writer: Some(WriterBlock {
            writer_id: context.writer_id.clone(),
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
            ControlUpdateError::Codec {
                object_key,
                message,
            } => Self::HeadWrite {
                object_key,
                message,
                class: StoreFailureClass::Other,
            },
            ControlUpdateError::Store {
                object_key,
                message,
                class,
            } => Self::HeadWrite {
                object_key,
                message,
                class,
            },
            ControlUpdateError::RetryExhausted { attempts } => Self::RetryExhausted { attempts },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_engine::delete_namespace;
    use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
    use crate::error::{CoreError, ErrorCode};
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::read_head_object;
    use crate::options::DeleteNamespaceOptions;

    async fn submit_commit<S: loonfs_objectstore::ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        request: crate::path::write::CommitRequest,
        context: &crate::context::MutationContext,
    ) -> crate::error::Result<loonfs_api::v0::CommitResponse> {
        let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
        engine
            .publish_batch(
                store,
                vec![CommitCandidate::new(request)],
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
    use loonfs_api::wire::control::{
        decode_control_object, encode_control_object, ControlObjectKind, HeadStateEnvelope,
        NamespaceState,
    };
    use loonfs_api::{ChangeSeq, CommitId, NamespaceId};
    use loonfs_objectstore::keys::wal_head;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{
        ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    };
    use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationClass};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    fn context(writer_id: &str, now_ms: u64) -> MutationContext {
        MutationContext {
            writer_id: writer_id.to_owned(),
            now_ms,
        }
    }

    fn head_owned_by(
        namespace_id: &NamespaceId,
        writer_id: &str,
        writer_epoch: WriterEpoch,
    ) -> HeadState {
        let mut head =
            HeadState::initial(namespace_id.clone(), loonfs_api::ContentStoreId::generate());
        head.writer_epoch = writer_epoch;
        head.writer = Some(WriterBlock {
            writer_id: writer_id.to_owned(),
            acquired_at_ms: 500,
        });
        head
    }

    async fn write_head(store: &LocalFsStore, namespace_id: &NamespaceId, head: HeadState) {
        let envelope =
            HeadStateEnvelope::from_state(ControlObjectKind::WalHead, head).expect("head envelope");
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
    async fn acquiring_twice_from_the_same_context_takes_a_higher_epoch_each_time() {
        // Nothing recognizes an already-owned head, so a repeat acquire is
        // an ordinary takeover of the caller's own epoch: it bumps, and the
        // head's writer block is rewritten with the new acquisition stamp.
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &store,
            &namespace_id,
            head_owned_by(&namespace_id, "writer", WriterEpoch(7)),
        )
        .await;

        let first = acquire_writer_epoch(&store, &namespace_id, &context("writer", 1_000))
            .await
            .expect("first acquire");
        let second = acquire_writer_epoch(&store, &namespace_id, &context("writer", 2_000))
            .await
            .expect("second acquire");

        assert_eq!(first.writer_epoch, WriterEpoch(8));
        assert_eq!(second.writer_epoch, WriterEpoch(9));
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state;
        assert_eq!(head.writer_epoch, WriterEpoch(9));
        let writer = head.writer.expect("writer block");
        assert_eq!(writer.writer_id, "writer");
        assert_eq!(writer.acquired_at_ms, 2_000);
    }

    #[tokio::test]
    async fn a_permission_denied_head_write_is_classified_as_permission_denied() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &inner,
            &namespace_id,
            head_owned_by(&namespace_id, "writer-a", WriterEpoch(7)),
        )
        .await;
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(wal_head(namespace_id.as_str())),
            OperationClass::CompareAndSwap,
            InjectedError::PermissionDenied("head write forbidden".to_owned()),
        );
        store.fail_all();

        let error = acquire_writer_epoch(&store, &namespace_id, &context("writer-b", 2_000))
            .await
            .expect_err("permission-denied head write must fail");
        assert!(matches!(
            &error,
            WriterEpochAcquireError::HeadWrite {
                class: StoreFailureClass::PermissionDenied,
                ..
            }
        ));
        assert_eq!(CoreError::from(error).code(), ErrorCode::PermissionDenied);
    }

    #[tokio::test]
    async fn acquire_on_deleted_namespace_is_rejected_and_leaves_the_tombstone_unchanged() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        // The tombstone names the deleting writer in its writer block: even
        // that writer must be refused, so the guard precedes the bump.
        let mut tombstone = head_owned_by(&namespace_id, "writer-a", WriterEpoch(7));
        tombstone.state = NamespaceState::Deleted;
        write_head(&store, &namespace_id, tombstone).await;
        let etag_before = head_etag(&store, &namespace_id).await;

        for writer_id in ["writer-a", "writer-b"] {
            let error = acquire_writer_epoch(&store, &namespace_id, &context(writer_id, 1_000))
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
            .state;
        assert_eq!(head.state, NamespaceState::Deleted);
        assert_eq!(head.writer_epoch, WriterEpoch(7));
    }

    #[tokio::test]
    async fn deleting_an_already_deleted_namespace_still_answers_namespace_deleted() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a", 1_000);
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
            &context("writer-a", 2_000),
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
            head_owned_by(&namespace_id, "writer-a", WriterEpoch(7)),
        )
        .await;

        let acquired = acquire_writer_epoch(&store, &namespace_id, &context("writer-b", 2_000))
            .await
            .expect("takeover acquire");

        assert_eq!(acquired.writer_epoch, WriterEpoch(8));
        assert_eq!(acquired.writer_id, "writer-b");
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .state;
        let writer = head.writer.expect("writer block");
        assert_eq!(writer.writer_id, "writer-b");
        assert_eq!(writer.acquired_at_ms, 2_000);
    }

    fn create_dir_request(
        commit_id: &str,
        display_name: &str,
    ) -> crate::path::write::CommitRequest {
        crate::path::write::CommitRequest::single(
            CommitId::parse(commit_id).expect("valid commit id"),
            None,
            crate::path::write::FilesystemOperation::CreateDirectory {
                path: loonfs_api::AbsolutePath::parse(format!("/{display_name}"))
                    .expect("valid path"),
                parents: false,
            },
        )
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
        let writer_a = context("writer-a", 1_000);
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        submit_commit(
            &store,
            &namespace_id,
            create_dir_request("writer-a-first", "from-a"),
            &writer_a,
        )
        .await
        .expect("writer a first commit");

        let writer_b = context("writer-b", 2_000);
        submit_commit(
            &store,
            &namespace_id,
            create_dir_request("writer-b-first", "from-b"),
            &writer_b,
        )
        .await
        .expect("writer b commit after takeover");

        let writer_a_again = context("writer-a", 3_000);
        submit_commit(
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
            .state;
        assert_eq!(head.seq, ChangeSeq(3));
        assert_eq!(head.writer.expect("writer block").writer_id, "writer-a");
    }

    #[tokio::test]
    async fn stale_writer_epoch_cannot_delete_namespace_after_takeover() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a", 1_000);
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
        let delete_attempt = context("writer-a", 2_000);
        let acquired = acquire_writer_epoch(&store, &namespace_id, &delete_attempt)
            .await
            .expect("acquire before the takeover");
        let error = crate::namespace::delete::delete_namespace(
            &store,
            &namespace_id,
            DeleteNamespaceOptions::default(),
            acquired,
        )
        .await
        .expect_err("stale-epoch delete must be fenced");
        assert_eq!(error.code(), ErrorCode::WriterFenced);

        let head = read_head_object(&store.inner, &namespace_id)
            .await
            .expect("read head")
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
                acquired_at_ms: 1_500,
            });
            let next = HeadStateEnvelope::from_state(ControlObjectKind::WalHead, head)
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
            head_owned_by(&namespace_id, "writer-a", WriterEpoch(7)),
        )
        .await;
        let store = TakeoverOnCasConflictStore {
            inner,
            namespace_id: namespace_id.clone(),
            remaining_conflicts: AtomicUsize::new(1),
        };

        let acquired = acquire_writer_epoch(&store, &namespace_id, &context("writer-c", 2_000))
            .await
            .expect("acquire after losing the race");

        // writer-b installed epoch 8 during the conflict; writer-c retried
        // and took 9.
        assert_eq!(acquired.writer_epoch, WriterEpoch(9));
        let head = read_head_object(&store.inner, &namespace_id)
            .await
            .expect("read head")
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
            let winner = head_owned_by(&self.namespace_id, "writer-b", WriterEpoch(8));
            let envelope = HeadStateEnvelope::from_state(ControlObjectKind::WalHead, winner)
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
