use crate::core::namespace::{next_takeover_head, HeadFenceTakeoverError};
use crate::mutation::loading::{read_head_object, read_lease_object, ControlObjectLoadError};
use crate::mutation::ClientMutationExecutionParams;
use crate::objectstore::ObjectStore;
use crate::objectstore::ObjectStoreError;
use loon_types::{
    ControlObjectKind, FenceToken, HeadStateEnvelope, LeaseState, LeaseStateEnvelope, NamespaceId,
};
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

pub(crate) fn acquire_or_renew_namespace_lease<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    params: &ClientMutationExecutionParams,
) -> Result<(), LeaseAcquireError> {
    if params.writer_id.trim().is_empty() {
        return Err(LeaseAcquireError::EmptyWriterId);
    }
    if params.lease_duration_ms == 0 {
        return Err(LeaseAcquireError::ZeroLeaseDuration);
    }

    for _attempt in 0..MAX_LEASE_ACQUIRE_ATTEMPTS {
        let loaded_head =
            read_head_object(store, namespace_id).map_err(LeaseAcquireError::LoadHead)?;
        let loaded_lease =
            read_lease_object(store, namespace_id).map_err(LeaseAcquireError::LoadLease)?;
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
            ) {
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
            ) {
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
            ) {
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
    params: &ClientMutationExecutionParams,
) -> LeaseState {
    LeaseState {
        namespace_id: namespace_id.clone(),
        holder_id: holder_id.to_owned(),
        fence_token,
        lease_expires_at_ms: params.now_ms.saturating_add(params.lease_duration_ms),
    }
}

fn compare_and_swap_head<S: ObjectStore>(
    store: &S,
    object_key: &str,
    expected_etag: &str,
    writer_version: &str,
    next_head: loon_types::HeadState,
) -> Result<(), CasOutcome> {
    let envelope =
        HeadStateEnvelope::from_state(ControlObjectKind::NamespaceHead, writer_version, next_head)
            .map_err(|err| CasOutcome::Fatal(err.to_string()))?;
    let bytes = serde_json::to_vec(&envelope).map_err(|err| CasOutcome::Fatal(err.to_string()))?;
    store
        .compare_and_swap(object_key, expected_etag, &bytes)
        .map(|_| ())
        .map_err(map_cas_error)
}

fn compare_and_swap_lease<S: ObjectStore>(
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
    let bytes = serde_json::to_vec(&envelope).map_err(|err| CasOutcome::Fatal(err.to_string()))?;
    store
        .compare_and_swap(object_key, expected_etag, &bytes)
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
    use super::{acquire_or_renew_namespace_lease, LeaseAcquireError};
    use crate::mutation::loading::{read_head_object, read_lease_object};
    use crate::mutation::ClientMutationExecutionParams;
    use crate::objectstore::fs::LocalFsStore;
    use crate::objectstore::keys::{namespace_head, namespace_lease};
    use crate::objectstore::ObjectStore;
    use loon_testkit::tempdir::TestDir;
    use loon_types::{
        ChangeSeq, ControlObjectKind, FenceToken, HeadState, HeadStateEnvelope, InodeId,
        LeaseState, LeaseStateEnvelope, NamespaceId,
    };

    #[test]
    fn renews_active_holder_without_rotating_fence() {
        let temp_dir = TestDir::new("lease-renew-active-holder");
        let store = LocalFsStore::new(temp_dir.path()).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        seed_head_and_lease(
            &store,
            &HeadState {
                namespace_id: namespace_id.clone(),
                seq: ChangeSeq(41),
                active_fence_token: FenceToken(8),
                next_inode_id: InodeId(501),
                snapshot_hint_seq: Some(ChangeSeq(40)),
                retention_floor_seq: ChangeSeq(40),
            },
            &LeaseState {
                namespace_id: namespace_id.clone(),
                holder_id: "writer-a".to_owned(),
                fence_token: FenceToken(8),
                lease_expires_at_ms: 2_000,
            },
        );

        acquire_or_renew_namespace_lease(
            &store,
            &namespace_id,
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_500,
                lease_duration_ms: 60_000,
            },
        )
        .expect("renew active holder");

        let head = read_head_object(&store, &namespace_id)
            .expect("load head")
            .envelope
            .state;
        let lease = read_lease_object(&store, &namespace_id)
            .expect("load lease")
            .envelope
            .state;
        assert_eq!(head.active_fence_token, FenceToken(8));
        assert_eq!(lease.holder_id, "writer-a");
        assert_eq!(lease.fence_token, FenceToken(8));
        assert_eq!(lease.lease_expires_at_ms, 61_500);
    }

    #[test]
    fn expired_reacquire_rotates_fence_even_for_same_holder() {
        let temp_dir = TestDir::new("lease-reacquire-same-holder");
        let store = LocalFsStore::new(temp_dir.path()).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        seed_head_and_lease(
            &store,
            &HeadState {
                namespace_id: namespace_id.clone(),
                seq: ChangeSeq(41),
                active_fence_token: FenceToken(8),
                next_inode_id: InodeId(501),
                snapshot_hint_seq: Some(ChangeSeq(40)),
                retention_floor_seq: ChangeSeq(40),
            },
            &LeaseState {
                namespace_id: namespace_id.clone(),
                holder_id: "writer-a".to_owned(),
                fence_token: FenceToken(8),
                lease_expires_at_ms: 1_000,
            },
        );

        acquire_or_renew_namespace_lease(
            &store,
            &namespace_id,
            &ClientMutationExecutionParams {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_500,
                lease_duration_ms: 60_000,
            },
        )
        .expect("reacquire expired lease");

        let head = read_head_object(&store, &namespace_id)
            .expect("load head")
            .envelope
            .state;
        let lease = read_lease_object(&store, &namespace_id)
            .expect("load lease")
            .envelope
            .state;
        assert_eq!(head.active_fence_token, FenceToken(9));
        assert_eq!(lease.holder_id, "writer-a");
        assert_eq!(lease.fence_token, FenceToken(9));
        assert_eq!(lease.lease_expires_at_ms, 61_500);
    }

    #[test]
    fn active_foreign_holder_is_rejected() {
        let temp_dir = TestDir::new("lease-reject-active-foreign-holder");
        let store = LocalFsStore::new(temp_dir.path()).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        seed_head_and_lease(
            &store,
            &HeadState {
                namespace_id: namespace_id.clone(),
                seq: ChangeSeq(41),
                active_fence_token: FenceToken(8),
                next_inode_id: InodeId(501),
                snapshot_hint_seq: Some(ChangeSeq(40)),
                retention_floor_seq: ChangeSeq(40),
            },
            &LeaseState {
                namespace_id: namespace_id.clone(),
                holder_id: "writer-a".to_owned(),
                fence_token: FenceToken(8),
                lease_expires_at_ms: 2_000,
            },
        );

        let error = acquire_or_renew_namespace_lease(
            &store,
            &namespace_id,
            &ClientMutationExecutionParams {
                writer_id: "writer-b".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_500,
                lease_duration_ms: 60_000,
            },
        )
        .expect_err("active foreign holder should block acquire");

        assert_eq!(
            error,
            LeaseAcquireError::HeldByOtherWriter {
                holder_id: "writer-a".to_owned(),
                lease_expires_at_ms: 2_000,
            }
        );
    }

    #[test]
    fn repairs_one_step_recovery_shape_and_completes_takeover() {
        let temp_dir = TestDir::new("lease-repair-one-step-recovery");
        let store = LocalFsStore::new(temp_dir.path()).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        seed_head_and_lease(
            &store,
            &HeadState {
                namespace_id: namespace_id.clone(),
                seq: ChangeSeq(41),
                active_fence_token: FenceToken(9),
                next_inode_id: InodeId(501),
                snapshot_hint_seq: Some(ChangeSeq(40)),
                retention_floor_seq: ChangeSeq(40),
            },
            &LeaseState {
                namespace_id: namespace_id.clone(),
                holder_id: "writer-a".to_owned(),
                fence_token: FenceToken(8),
                lease_expires_at_ms: 1_000,
            },
        );

        acquire_or_renew_namespace_lease(
            &store,
            &namespace_id,
            &ClientMutationExecutionParams {
                writer_id: "writer-b".to_owned(),
                writer_version: "loon-server-test".to_owned(),
                now_ms: 1_500,
                lease_duration_ms: 60_000,
            },
        )
        .expect("repair one-step recovery shape");

        let head = read_head_object(&store, &namespace_id)
            .expect("load head")
            .envelope
            .state;
        let lease = read_lease_object(&store, &namespace_id)
            .expect("load lease")
            .envelope
            .state;
        assert_eq!(head.active_fence_token, FenceToken(9));
        assert_eq!(lease.holder_id, "writer-b");
        assert_eq!(lease.fence_token, FenceToken(9));
        assert_eq!(lease.lease_expires_at_ms, 61_500);
    }

    fn seed_head_and_lease(store: &LocalFsStore, head: &HeadState, lease: &LeaseState) {
        let head_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "loon-server-test",
            head.clone(),
        )
        .expect("encode head envelope");
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-server-test",
            lease.clone(),
        )
        .expect("encode lease envelope");

        store
            .put_if_absent(
                &namespace_head(head.namespace_id.as_str()),
                &serde_json::to_vec(&head_envelope).expect("serialize head"),
            )
            .expect("seed head");
        store
            .put_if_absent(
                &namespace_lease(lease.namespace_id.as_str()),
                &serde_json::to_vec(&lease_envelope).expect("serialize lease"),
            )
            .expect("seed lease");
    }
}
