use crate::context::MutationContext;
use crate::control_update::{update_head, ControlUpdateError, HeadUpdate};
use crate::namespace::control::ControlObjectLoadError;
use loonfs_api::wire::control::{AcquiredWriter, HeadState, WriterLease};
use loonfs_api::{NamespaceId, WriterEpoch};
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_WRITER_EPOCH_ACQUIRE_ATTEMPTS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WriterEpochAcquireError {
    #[error(transparent)]
    LoadHead(ControlObjectLoadError),
    #[error("empty writer id")]
    EmptyWriterId,
    #[error("empty writer session id")]
    EmptyWriterSessionId,
    #[error("lease duration must be greater than zero")]
    ZeroLeaseDuration,
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error("writer epoch overflow from `{active:?}`")]
    WriterEpochOverflow { active: WriterEpoch },
    #[error(
        "namespace writer lease is held by another active writer `{writer_id}` until `{lease_expires_at_ms}`"
    )]
    HeldByOtherWriter {
        writer_id: String,
        lease_expires_at_ms: u64,
    },
    #[error("failed to write head object during writer epoch acquire: {0}")]
    HeadWrite(String),
    #[error("writer epoch acquire retries exhausted after {attempts} attempts")]
    RetryExhausted { attempts: usize },
}

pub(crate) async fn acquire_writer_epoch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    params: &MutationContext,
) -> Result<AcquiredWriter, WriterEpochAcquireError> {
    if params.writer_id.trim().is_empty() {
        return Err(WriterEpochAcquireError::EmptyWriterId);
    }
    if params.writer_session_id.trim().is_empty() {
        return Err(WriterEpochAcquireError::EmptyWriterSessionId);
    }
    if params.lease_duration_ms == 0 {
        return Err(WriterEpochAcquireError::ZeroLeaseDuration);
    }

    update_head(
        store,
        namespace_id,
        &params.writer_version,
        MAX_WRITER_EPOCH_ACQUIRE_ATTEMPTS,
        |loaded_head| {
            let head = &loaded_head.envelope.state;
            if let Some(active_lease) = head
                .writer_lease
                .as_ref()
                .filter(|lease| lease.is_valid_at(params.now_ms))
            {
                if active_lease.writer_id != params.writer_id {
                    return Err(WriterEpochAcquireError::HeldByOtherWriter {
                        writer_id: active_lease.writer_id.clone(),
                        lease_expires_at_ms: active_lease.lease_expires_at_ms,
                    });
                }

                if active_lease.writer_session_id == params.writer_session_id {
                    if !lease_needs_renewal(active_lease.lease_expires_at_ms, params) {
                        return Ok(HeadUpdate::Noop(acquired_writer_from_lease(
                            head.writer_epoch,
                            active_lease,
                        )));
                    }
                    return Ok(HeadUpdate::Replace {
                        next: Box::new(head_with_writer_lease(head, head.writer_epoch, params)),
                        outcome: acquired_writer(head.writer_epoch, params),
                    });
                }
            }

            let next_epoch = next_writer_epoch(head.writer_epoch)?;
            Ok(HeadUpdate::Replace {
                next: Box::new(head_with_writer_lease(head, next_epoch, params)),
                outcome: acquired_writer(next_epoch, params),
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

fn head_with_writer_lease(
    current_head: &HeadState,
    writer_epoch: WriterEpoch,
    params: &MutationContext,
) -> HeadState {
    HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: current_head.seq,
        head_commit_id: current_head.head_commit_id.clone(),
        writer_epoch,
        writer_lease: Some(WriterLease {
            writer_id: params.writer_id.clone(),
            writer_session_id: params.writer_session_id.clone(),
            lease_expires_at_ms: params.now_ms.saturating_add(params.lease_duration_ms),
        }),
        next_inode_id: current_head.next_inode_id,
        name_policy: current_head.name_policy,
        current_manifest_id: current_head.current_manifest_id,
        latest_checkpoint_id: current_head.latest_checkpoint_id.clone(),
        retention_floor_seq: current_head.retention_floor_seq,
        visible_wal_tip: current_head.visible_wal_tip.clone(),
        state: current_head.state,
    }
}

fn acquired_writer(writer_epoch: WriterEpoch, params: &MutationContext) -> AcquiredWriter {
    AcquiredWriter {
        writer_id: params.writer_id.clone(),
        writer_session_id: params.writer_session_id.clone(),
        writer_epoch,
        lease_expires_at_ms: params.now_ms.saturating_add(params.lease_duration_ms),
    }
}

fn acquired_writer_from_lease(writer_epoch: WriterEpoch, lease: &WriterLease) -> AcquiredWriter {
    AcquiredWriter {
        writer_id: lease.writer_id.clone(),
        writer_session_id: lease.writer_session_id.clone(),
        writer_epoch,
        lease_expires_at_ms: lease.lease_expires_at_ms,
    }
}

fn lease_needs_renewal(lease_expires_at_ms: u64, params: &MutationContext) -> bool {
    let renew_after_ms = params.lease_duration_ms / 2;
    lease_expires_at_ms <= params.now_ms.saturating_add(renew_after_ms)
}

impl From<ControlUpdateError> for WriterEpochAcquireError {
    fn from(value: ControlUpdateError) -> Self {
        match value {
            ControlUpdateError::LoadHead(error) => Self::LoadHead(error),
            ControlUpdateError::MissingEtag { object_key } => Self::MissingHeadEtag { object_key },
            ControlUpdateError::Codec(message) | ControlUpdateError::Store(message) => {
                Self::HeadWrite(message)
            }
            ControlUpdateError::RetryExhausted { attempts } => Self::RetryExhausted { attempts },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::read_head_object;
    use crate::namespace::delete::delete_namespace;
    use crate::options::DeleteNamespaceOptions;
    use crate::protocol::commit_operations;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::v0::{CommitOp as ApiCommitOp, CommitRequest as ApiCommitRequest};
    use loonfs_api::wire::control::{
        decode_control_object, encode_control_object, ControlObjectKind, HeadStateEnvelope,
        NamespaceState,
    };
    use loonfs_api::{ChangeSeq, CommitId, InodeId, NamespaceId};
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::keys::wal_head;
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
            lease_duration_ms: 10_000,
        }
    }

    fn leased_head(
        namespace_id: &NamespaceId,
        writer_id: &str,
        writer_session_id: &str,
        writer_epoch: WriterEpoch,
        lease_expires_at_ms: u64,
    ) -> HeadState {
        let mut head = HeadState::initial(namespace_id.clone());
        head.writer_epoch = writer_epoch;
        head.writer_lease = Some(WriterLease {
            writer_id: writer_id.to_owned(),
            writer_session_id: writer_session_id.to_owned(),
            lease_expires_at_ms,
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
            leased_head(&namespace_id, "writer", "session-a", WriterEpoch(7), 20_000),
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
        assert_eq!(acquired.lease_expires_at_ms, 20_000);
        assert_eq!(head_etag(&store, &namespace_id).await, etag_before);
    }

    #[tokio::test]
    async fn same_session_renews_near_expiry_without_bumping_epoch() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &store,
            &namespace_id,
            leased_head(&namespace_id, "writer", "session-a", WriterEpoch(7), 6_000),
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
        assert_eq!(acquired.lease_expires_at_ms, 11_000);
        assert_ne!(head_etag(&store, &namespace_id).await, etag_before);
    }

    #[tokio::test]
    async fn restarted_writer_session_bumps_epoch() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &store,
            &namespace_id,
            leased_head(&namespace_id, "writer", "session-a", WriterEpoch(7), 20_000),
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
            head.writer_lease.expect("writer lease").writer_session_id,
            "session-b"
        );
    }

    #[tokio::test]
    async fn active_other_writer_is_rejected() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &store,
            &namespace_id,
            leased_head(
                &namespace_id,
                "writer-a",
                "session-a",
                WriterEpoch(7),
                20_000,
            ),
        )
        .await;

        let error = acquire_writer_epoch(
            &store,
            &namespace_id,
            &context("writer-b", "session-b", 1_000),
        )
        .await
        .expect_err("other active writer should be rejected");

        assert!(matches!(
            error,
            WriterEpochAcquireError::HeldByOtherWriter { .. }
        ));
    }

    #[tokio::test]
    async fn expired_lease_takeover_bumps_epoch() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        write_head(
            &store,
            &namespace_id,
            leased_head(
                &namespace_id,
                "writer-a",
                "session-a",
                WriterEpoch(7),
                1_000,
            ),
        )
        .await;

        let acquired = acquire_writer_epoch(
            &store,
            &namespace_id,
            &context("writer-b", "session-b", 2_000),
        )
        .await
        .expect("expired lease takeover");

        assert_eq!(acquired.writer_epoch, WriterEpoch(8));
        assert_eq!(acquired.writer_id, "writer-b");
    }

    fn create_dir_request(commit_id: &str, display_name: &str) -> ApiCommitRequest {
        ApiCommitRequest {
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![ApiCommitOp::CreateDirectory {
                parent_inode: InodeId(1),
                display_name: display_name.to_owned(),
            }],
            message: None,
        }
    }

    #[tokio::test]
    async fn previous_writer_cannot_publish_after_writer_takeover() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a", "session-a", 1_000);
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");
        let epoch_at_bootstrap = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state
            .writer_epoch;

        // Writer A's lease has expired; writer B takes over and commits.
        let writer_b = context("writer-b", "session-b", 12_001);
        commit_operations(
            &store,
            &namespace_id,
            create_dir_request("writer-b-create", "from-b"),
            &writer_b,
        )
        .await
        .expect("writer b commit after takeover");

        let writer_a_retry = context("writer-a", "session-a", 12_002);
        let error = commit_operations(
            &store,
            &namespace_id,
            create_dir_request("writer-a-stale", "from-a"),
            &writer_a_retry,
        )
        .await
        .expect_err("previous writer should be fenced out");
        assert_eq!(error.code(), ErrorCode::LeaseConflict);

        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.seq, ChangeSeq(1));
        assert!(head.writer_epoch > epoch_at_bootstrap);
        let lease = head.writer_lease.expect("active lease");
        assert_eq!(lease.writer_id, "writer-b");
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

        let delete_attempt = context("writer-a", "session-a", 2_000);
        let error = delete_namespace(
            &store,
            &namespace_id,
            DeleteNamespaceOptions::default(),
            &delete_attempt,
        )
        .await
        .expect_err("stale-epoch delete must be fenced");
        assert_eq!(error.code(), ErrorCode::LeaseConflict);

        let head = read_head_object(&store.inner, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.state, NamespaceState::Active);
        let lease = head.writer_lease.expect("active lease");
        assert_eq!(lease.writer_id, "writer-b");
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
            head.writer_lease = Some(WriterLease {
                writer_id: "writer-b".to_owned(),
                writer_session_id: "session-b".to_owned(),
                lease_expires_at_ms: 100_000,
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
    async fn cas_conflict_then_fenced_yields_held_by_other_writer() {
        // writer-a's lease has expired and writer-c starts a takeover. Its
        // CAS loses to writer-b, whose fresh lease must fence writer-c out on
        // the conflict re-read instead of being clobbered by another bump.
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        write_head(
            &inner,
            &namespace_id,
            leased_head(
                &namespace_id,
                "writer-a",
                "session-a",
                WriterEpoch(7),
                1_000,
            ),
        )
        .await;
        let store = TakeoverOnCasConflictStore {
            inner,
            namespace_id: namespace_id.clone(),
            remaining_conflicts: AtomicUsize::new(1),
        };

        let error = acquire_writer_epoch(
            &store,
            &namespace_id,
            &context("writer-c", "session-c", 2_000),
        )
        .await
        .expect_err("conflicting takeover must observe the winner's lease");

        assert!(matches!(
            error,
            WriterEpochAcquireError::HeldByOtherWriter { ref writer_id, .. }
                if writer_id == "writer-b"
        ));
        let head = read_head_object(&store.inner, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.writer_epoch, WriterEpoch(8));
        let lease = head.writer_lease.expect("active lease");
        assert_eq!(lease.writer_id, "writer-b");
    }

    #[derive(Debug)]
    struct TakeoverOnCasConflictStore {
        inner: LocalFsStore,
        namespace_id: NamespaceId,
        remaining_conflicts: AtomicUsize,
    }

    impl TakeoverOnCasConflictStore {
        async fn inject_winner(&self) {
            let winner = leased_head(
                &self.namespace_id,
                "writer-b",
                "session-b",
                WriterEpoch(8),
                99_000,
            );
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
                return Err(ObjectStoreError::PreconditionFailed);
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
