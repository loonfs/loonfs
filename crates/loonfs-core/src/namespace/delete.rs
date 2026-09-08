//! Namespace deletion: a fenced, terminal head-state transition.

use crate::commit::CommitHeadPublishError;
use crate::control_update::{retry_while_contended, CasAttempt, WriteEvidence};
use crate::error::CoreError;
use crate::namespace::control::load_head_object;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::options::DeleteNamespaceOptions;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, AcquiredWriter, ControlObjectKind, HeadState, NamespaceStatus,
};
use loonfs_api::{DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

/// Marks a namespace as deleted with a compare-and-swap on its head.
///
/// Each retry verifies the writer epoch before attempting the swap.
pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    acquired_writer: AcquiredWriter,
) -> Result<DeleteNamespaceResponse, CoreError> {
    let acquired_writer = &acquired_writer;
    retry_while_contended(
        || async move {
            let loaded = load_head_object(store, namespace_id)
                .await
                .map_err(CoreError::ControlObjectLoad)?;
            let head = loaded.state.clone();
            super::control::ensure_namespace_live(&head)?;

            ensure_writer_not_fenced(&head, acquired_writer)?;

            if let Some(expected) = options.expected_head_seq {
                if head.seq != expected {
                    return Err(CoreError::StaleHeadPrecondition {
                        assertion_index: None,
                        expected,
                        actual: head.seq,
                    });
                }
            }

            let deleted_head = HeadState {
                status: NamespaceStatus::Deleted {
                    reclaim_after_ms: None,
                },
                ..head.clone()
            };
            let encoded = encode_control_state(ControlObjectKind::WalHead, &deleted_head).map_err(
                |error| CoreError::Codec {
                    object_key: loaded.object_key.clone(),
                    message: error.to_string(),
                },
            )?;
            match store
                .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
                .await
            {
                Ok(_) => Ok(CasAttempt::Settled(DeleteNamespaceResponse {
                    namespace_id: namespace_id.clone(),
                    head_seq: head.seq,
                })),
                Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended(
                    CoreError::HeadPublish(CommitHeadPublishError::StaleHead),
                )),
                Err(error @ ObjectStoreError::Transport { .. }) => {
                    Ok(CasAttempt::Ambiguous(error, ()))
                }
                Err(error) => Err(CoreError::store(&loaded.object_key, &error)),
            }
        },
        |_, ()| async {
            let loaded = load_head_object(store, namespace_id)
                .await
                .map_err(CoreError::ControlObjectLoad)?;
            if loaded.state.status.is_deleted() {
                Ok(WriteEvidence::Landed(DeleteNamespaceResponse {
                    namespace_id: namespace_id.clone(),
                    head_seq: loaded.state.seq,
                }))
            } else {
                Ok(WriteEvidence::Lost(CoreError::HeadPublish(
                    CommitHeadPublishError::StaleHead,
                )))
            }
        },
    )
    .await?
}

pub(crate) async fn retire_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    grace_window_ms: u64,
    fresh_now_ms: u64,
) -> Result<u64, CoreError> {
    let deadline = fresh_now_ms
        .checked_add(grace_window_ms.max(crate::limits::NAMESPACE_RETIREMENT_GRACE_MS))
        .ok_or_else(|| CoreError::Internal("namespace retirement deadline overflow".to_owned()))?;
    retry_while_contended(
        || async {
            let loaded = load_head_object(store, namespace_id)
                .await
                .map_err(CoreError::ControlObjectLoad)?;
            let head = &loaded.state;
            if !head.status.is_deleted() {
                return Err(CoreError::NamespaceCorrupt(
                    "a deleted namespace became active".to_owned(),
                ));
            }
            if let Some(deadline) = head.status.reclaim_after_ms() {
                return Ok(CasAttempt::Settled(deadline));
            }
            let retired = HeadState {
                status: NamespaceStatus::Deleted {
                    reclaim_after_ms: Some(deadline),
                },
                ..head.clone()
            };
            head.ensure_successor_identity(&retired)
                .map_err(|error| CoreError::NamespaceCorrupt(error.to_string()))?;
            let encoded =
                encode_control_state(ControlObjectKind::WalHead, &retired).map_err(|error| {
                    CoreError::Codec {
                        object_key: loaded.object_key.clone(),
                        message: error.to_string(),
                    }
                })?;
            match store
                .compare_and_swap(&loaded.object_key, &loaded.etag, Bytes::from(encoded))
                .await
            {
                Ok(_) => Ok(CasAttempt::Settled(deadline)),
                Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended(
                    CoreError::HeadPublish(CommitHeadPublishError::StaleHead),
                )),
                Err(error @ ObjectStoreError::Transport { .. }) => {
                    Ok(CasAttempt::Ambiguous(error, ()))
                }
                Err(error) => Err(CoreError::store(&loaded.object_key, &error)),
            }
        },
        |_, ()| async {
            let loaded = load_head_object(store, namespace_id)
                .await
                .map_err(CoreError::ControlObjectLoad)?;
            if !loaded.state.status.is_deleted() {
                return Err(CoreError::NamespaceCorrupt(
                    "a deleted namespace became active".to_owned(),
                ));
            }
            Ok(match loaded.state.status.reclaim_after_ms() {
                Some(deadline) => WriteEvidence::Landed(deadline),
                None => WriteEvidence::Unknown,
            })
        },
    )
    .await?
}
