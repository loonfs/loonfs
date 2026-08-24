//! Namespace deletion: a fenced, terminal head-state transition.

use crate::control_update::{retry_while_contended, CasAttempt};
use crate::error::CoreError;
use crate::namespace::control::load_head_object;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::options::DeleteNamespaceOptions;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, AcquiredWriter, ControlObjectKind, HeadState, NamespaceStatus,
};
use loonfs_api::{DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError, PutMode};
use std::sync::atomic::{AtomicBool, Ordering};

/// Marks a namespace as deleted with a compare-and-swap on its head.
///
/// Each retry verifies the writer epoch before attempting the swap. A deleted
/// head is checked first because no further write is required: it indicates
/// success after an ambiguous swap, or `namespace_deleted` if this call never
/// attempted one.
pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    acquired_writer: AcquiredWriter,
) -> Result<DeleteNamespaceResponse, CoreError> {
    let attempted_swap = &AtomicBool::new(false);
    let acquired_writer = &acquired_writer;
    let deleted = retry_while_contended(|| async move {
        let loaded = load_head_object(store, namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let head = loaded.state.clone();

        // Check the terminal state before the writer fence because a deleted
        // namespace requires no further write.
        if head.status == (NamespaceStatus::Deleted {}) {
            // A deleted head resolves an earlier ambiguous swap as success.
            if attempted_swap.load(Ordering::Relaxed) {
                return Ok(CasAttempt::Settled(DeleteNamespaceResponse {
                    namespace_id: namespace_id.clone(),
                    head_seq: head.seq,
                }));
            }
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            });
        }

        ensure_writer_not_fenced(&head, acquired_writer)?;

        if let Some(expected) = options.expected_head_seq {
            if head.seq != expected {
                return Err(CoreError::StaleHeadPrecondition {
                    expected,
                    actual: head.seq,
                });
            }
        }

        let head_etag = loaded.etag;
        let deleted_head = HeadState {
            status: NamespaceStatus::Deleted {},
            ..head.clone()
        };
        let encoded =
            encode_control_state(ControlObjectKind::WalHead, &deleted_head).map_err(|error| {
                CoreError::Codec {
                    object_key: loaded.object_key.clone(),
                    message: error.to_string(),
                }
            })?;

        let swap = store
            .put(
                &loaded.object_key,
                Bytes::from(encoded),
                PutMode::CompareAndSwap {
                    expected_etag: head_etag,
                },
            )
            .await;
        match swap {
            Ok(_) => Ok(CasAttempt::Settled(DeleteNamespaceResponse {
                namespace_id: namespace_id.clone(),
                head_seq: head.seq,
            })),
            // The head moved (a commit, fence takeover, or another deleter
            // landed first). Reload and re-evaluate.
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended),
            // Reload to determine whether an unconfirmed delete succeeded.
            Err(ObjectStoreError::Transport { .. }) => {
                attempted_swap.store(true, Ordering::Relaxed);
                Ok(CasAttempt::Contended)
            }
            Err(other) => Err(CoreError::store(&loaded.object_key, &other)),
        }
    })
    .await?;

    deleted.ok_or(CoreError::HeadPublish(
        crate::commit::CommitHeadPublishError::StaleHead,
    ))
}
