use crate::context::MutationContext;
use crate::namespace::control::{read_head_object, read_lease_object, ControlObjectLoadError};
use crate::namespace::{next_takeover_head, HeadFenceTakeoverError};
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope, LeaseState,
    LeaseStateEnvelope,
};
use loonfs_api::{FenceToken, NamespaceId};
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_LEASE_ACQUIRE_ATTEMPTS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum LeaseAcquireError {
    #[error(transparent)]
    LoadHead(ControlObjectLoadError),
    #[error("failed to load lease object: {0}")]
    LoadLease(ControlObjectLoadError),
    #[error("empty writer id")]
    EmptyWriterId,
    #[error("lease duration must be greater than zero")]
    ZeroLeaseDuration,
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error("missing lease etag for `{object_key}`")]
    MissingLeaseEtag { object_key: String },
    #[error("failed to rotate head fence token: {0}")]
    HeadFenceTakeover(String),
    #[error(
        "namespace lease is held by another active writer `{holder_id}` until `{lease_expires_at_ms}`"
    )]
    HeldByOtherWriter {
        holder_id: String,
        lease_expires_at_ms: u64,
    },
    #[error(
        "unexpected control state during lease acquire: head_fence_token={head_fence_token:?} lease_fence_token={lease_fence_token:?} lease_expires_at_ms={lease_expires_at_ms} now_ms={now_ms}"
    )]
    UnexpectedControlState {
        head_fence_token: FenceToken,
        lease_fence_token: FenceToken,
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    #[error("failed to write head object during lease acquire: {0}")]
    HeadWrite(String),
    #[error("failed to write lease object during lease acquire: {0}")]
    LeaseWrite(String),
    #[error("lease acquire retries exhausted after {attempts} attempts")]
    RetryExhausted { attempts: usize },
}

pub(crate) async fn acquire_or_renew_namespace_lease<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    params: &MutationContext,
) -> Result<(), LeaseAcquireError> {
    if params.writer_id.trim().is_empty() {
        return Err(LeaseAcquireError::EmptyWriterId);
    }
    if params.lease_duration_ms == 0 {
        return Err(LeaseAcquireError::ZeroLeaseDuration);
    }

    for _attempt in 0..MAX_LEASE_ACQUIRE_ATTEMPTS {
        let loaded_head = read_head_object(store, namespace_id)
            .await
            .map_err(LeaseAcquireError::LoadHead)?;
        let loaded_lease = read_lease_object(store, namespace_id)
            .await
            .map_err(LeaseAcquireError::LoadLease)?;
        let head_etag = loaded_head.metadata.etag.clone().ok_or_else(|| {
            LeaseAcquireError::MissingHeadEtag {
                object_key: loaded_head.object_key.clone(),
            }
        })?;
        let lease_etag = loaded_lease.metadata.etag.clone().ok_or_else(|| {
            LeaseAcquireError::MissingLeaseEtag {
                object_key: loaded_lease.object_key.clone(),
            }
        })?;
        let head = loaded_head.envelope.state;
        let lease = loaded_lease.envelope.state;

        if lease.is_valid_at(params.now_ms) {
            if head.active_fence_token != lease.fence_token {
                return Err(LeaseAcquireError::UnexpectedControlState {
                    head_fence_token: head.active_fence_token,
                    lease_fence_token: lease.fence_token,
                    lease_expires_at_ms: lease.lease_expires_at_ms,
                    now_ms: params.now_ms,
                });
            }
            if lease.holder_id != params.writer_id {
                return Err(LeaseAcquireError::HeldByOtherWriter {
                    holder_id: lease.holder_id,
                    lease_expires_at_ms: lease.lease_expires_at_ms,
                });
            }

            let renewed = desired_lease_state(
                namespace_id,
                &params.writer_id,
                head.active_fence_token,
                params,
            );
            match compare_and_swap_lease(
                store,
                &loaded_lease.object_key,
                &lease_etag,
                &params.writer_version,
                renewed,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(CasOutcome::Retryable) => continue,
                Err(CasOutcome::Fatal(message)) => {
                    return Err(LeaseAcquireError::LeaseWrite(message));
                }
            }
        }

        if head.active_fence_token == lease.fence_token {
            let takeover_head = next_takeover_head(&head).map_err(map_head_takeover_error)?;
            match compare_and_swap_head(
                store,
                &loaded_head.object_key,
                &head_etag,
                &params.writer_version,
                takeover_head,
            )
            .await
            {
                Ok(()) => continue,
                Err(CasOutcome::Retryable) => continue,
                Err(CasOutcome::Fatal(message)) => {
                    return Err(LeaseAcquireError::HeadWrite(message));
                }
            }
        }

        if head.active_fence_token.0 == lease.fence_token.0.saturating_add(1) {
            let reacquired = desired_lease_state(
                namespace_id,
                &params.writer_id,
                head.active_fence_token,
                params,
            );
            match compare_and_swap_lease(
                store,
                &loaded_lease.object_key,
                &lease_etag,
                &params.writer_version,
                reacquired,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(CasOutcome::Retryable) => continue,
                Err(CasOutcome::Fatal(message)) => {
                    return Err(LeaseAcquireError::LeaseWrite(message));
                }
            }
        }

        return Err(LeaseAcquireError::UnexpectedControlState {
            head_fence_token: head.active_fence_token,
            lease_fence_token: lease.fence_token,
            lease_expires_at_ms: lease.lease_expires_at_ms,
            now_ms: params.now_ms,
        });
    }

    Err(LeaseAcquireError::RetryExhausted {
        attempts: MAX_LEASE_ACQUIRE_ATTEMPTS,
    })
}

fn desired_lease_state(
    namespace_id: &NamespaceId,
    holder_id: &str,
    fence_token: FenceToken,
    params: &MutationContext,
) -> LeaseState {
    LeaseState {
        namespace_id: namespace_id.clone(),
        holder_id: holder_id.to_owned(),
        fence_token,
        lease_expires_at_ms: params.now_ms.saturating_add(params.lease_duration_ms),
    }
}

async fn compare_and_swap_head<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_etag: &str,
    writer_version: &str,
    next_head: HeadState,
) -> Result<(), CasOutcome> {
    let envelope =
        HeadStateEnvelope::from_state(ControlObjectKind::NamespaceHead, writer_version, next_head)
            .map_err(|err| CasOutcome::Fatal(err.to_string()))?;
    let bytes =
        encode_control_object(&envelope).map_err(|err| CasOutcome::Fatal(err.to_string()))?;
    store
        .compare_and_swap(object_key, expected_etag, Bytes::from(bytes))
        .await
        .map(|_| ())
        .map_err(map_cas_error)
}

async fn compare_and_swap_lease<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_etag: &str,
    writer_version: &str,
    next_lease: LeaseState,
) -> Result<(), CasOutcome> {
    let envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        writer_version,
        next_lease,
    )
    .map_err(|err| CasOutcome::Fatal(err.to_string()))?;
    let bytes =
        encode_control_object(&envelope).map_err(|err| CasOutcome::Fatal(err.to_string()))?;
    store
        .compare_and_swap(object_key, expected_etag, Bytes::from(bytes))
        .await
        .map(|_| ())
        .map_err(map_cas_error)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CasOutcome {
    Retryable,
    Fatal(String),
}

fn map_cas_error(err: ObjectStoreError) -> CasOutcome {
    match err {
        ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict => CasOutcome::Retryable,
        other => CasOutcome::Fatal(other.to_string()),
    }
}

fn map_head_takeover_error(err: HeadFenceTakeoverError) -> LeaseAcquireError {
    LeaseAcquireError::HeadFenceTakeover(err.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // These tests use panic in fake-store guards to fail if forbidden calls occur.

    use super::*;
    use crate::error::ErrorCode;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::{read_head_object, read_lease_object};
    use crate::protocol::commit_operations;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::v0::{CommitOp as ApiCommitOp, CommitRequest as ApiCommitRequest};
    use loonfs_api::{ChangeSeq, CommitId, InodeId};
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::keys::namespace_lease;
    use loonfs_objectstore::{ByteRange, ObjectBody, ObjectMetadata, ObjectStoreError, PutMode};
    use tempfile::tempdir;

    #[tokio::test]
    async fn same_holder_renewal_extends_expiry_without_advancing_fence() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = namespace_id();
        let initial = context("writer-a", 1_000);
        bootstrap_namespace(&store, &namespace_id, &initial, false)
            .await
            .expect("bootstrap");
        let before = read_lease_object(&store, &namespace_id)
            .await
            .expect("read lease before");

        let renewed_context = context("writer-a", 1_500);
        acquire_or_renew_namespace_lease(&store, &namespace_id, &renewed_context)
            .await
            .expect("renew lease");

        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        let renewed = read_lease_object(&store, &namespace_id)
            .await
            .expect("read renewed lease")
            .envelope
            .state;
        assert_eq!(head.active_fence_token, FenceToken(0));
        assert_eq!(renewed.fence_token, FenceToken(0));
        assert_eq!(renewed.holder_id, "writer-a");
        assert!(renewed.lease_expires_at_ms > before.envelope.state.lease_expires_at_ms);
    }

    #[tokio::test]
    async fn expired_lease_takeover_rewrites_existing_lease_and_advances_fence() {
        let temp_dir = tempdir().expect("tempdir");
        let store = DeleteRejectingStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = namespace_id();
        let initial = context("writer-a", 1_000);
        bootstrap_namespace(&store, &namespace_id, &initial, false)
            .await
            .expect("bootstrap");
        let lease_key = namespace_lease(namespace_id.as_str());
        assert!(store
            .head(&lease_key)
            .await
            .expect("lease head before")
            .is_some());

        let takeover_context = context("writer-b", 3_001);
        acquire_or_renew_namespace_lease(&store, &namespace_id, &takeover_context)
            .await
            .expect("take over expired lease");

        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        let lease = read_lease_object(&store, &namespace_id)
            .await
            .expect("read lease")
            .envelope
            .state;
        assert!(store
            .head(&lease_key)
            .await
            .expect("lease head after")
            .is_some());
        assert_eq!(head.active_fence_token, FenceToken(1));
        assert_eq!(lease.fence_token, FenceToken(1));
        assert_eq!(lease.holder_id, "writer-b");
        assert_eq!(
            lease.lease_expires_at_ms,
            takeover_context
                .now_ms
                .saturating_add(takeover_context.lease_duration_ms)
        );
    }

    #[tokio::test]
    async fn previous_writer_cannot_publish_while_newer_lease_is_valid() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = namespace_id();
        let writer_a = context("writer-a", 1_000);
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        let writer_b = context("writer-b", 3_001);
        commit_operations(
            &store,
            &namespace_id,
            create_dir_request("writer-b-create", "from-b"),
            &writer_b,
        )
        .await
        .expect("writer b commit after takeover");

        let writer_a_retry = context("writer-a", 3_002);
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
        let lease = read_lease_object(&store, &namespace_id)
            .await
            .expect("read lease")
            .envelope
            .state;
        assert_eq!(head.seq, ChangeSeq(1));
        assert_eq!(head.active_fence_token, FenceToken(1));
        assert_eq!(lease.fence_token, FenceToken(1));
        assert_eq!(lease.holder_id, "writer-b");
    }

    fn namespace_id() -> NamespaceId {
        NamespaceId::parse("demo").expect("valid namespace id")
    }

    fn context(writer_id: &str, now_ms: u64) -> MutationContext {
        MutationContext {
            writer_id: writer_id.to_owned(),
            writer_version: format!("{writer_id}/0.1.0"),
            now_ms,
            lease_duration_ms: 1_000,
        }
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
            annotations: None,
        }
    }

    #[derive(Debug)]
    struct DeleteRejectingStore {
        inner: LocalFsStore,
    }

    impl DeleteRejectingStore {
        fn new(inner: LocalFsStore) -> Self {
            Self { inner }
        }
    }

    #[async_trait]
    impl ObjectStore for DeleteRejectingStore {
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

        async fn delete(&self, _key: &str) -> Result<(), ObjectStoreError> {
            panic!("lease acquisition must not delete live namespace objects")
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }
}
