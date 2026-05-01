use crate::context::MutationContext;
use crate::loading::{read_head_object, read_lease_object, ControlObjectLoadError};
use crate::namespace::{next_takeover_head, HeadFenceTakeoverError};
use loon_api::{
    ControlObjectKind, FenceToken, HeadStateEnvelope, LeaseState, LeaseStateEnvelope, NamespaceId,
};
use loon_objectstore::ObjectStore;
use loon_objectstore::ObjectStoreError;
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

pub fn acquire_or_renew_namespace_lease<S: ObjectStore + ?Sized>(
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
    params: &MutationContext,
) -> LeaseState {
    LeaseState {
        namespace_id: namespace_id.clone(),
        holder_id: holder_id.to_owned(),
        fence_token,
        lease_expires_at_ms: params.now_ms.saturating_add(params.lease_duration_ms),
    }
}

fn compare_and_swap_head<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_etag: &str,
    writer_version: &str,
    next_head: loon_api::HeadState,
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

fn compare_and_swap_lease<S: ObjectStore + ?Sized>(
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
