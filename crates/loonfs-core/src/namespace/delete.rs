//! Namespace deletion: a fenced, terminal head-state transition.

use crate::error::CoreError;
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::control::load_head_object;
use crate::options::DeleteNamespaceOptions;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, AcquiredWriter, ControlObjectKind, HeadState, NamespaceStatus,
};
use loonfs_api::{DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError, PutMode};

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
    let mut attempted_swap = false;
    for _attempt in 0..CONTENTION_RETRY_LIMIT {
        let loaded = load_head_object(store, namespace_id)
            .await
            .map_err(|error| CoreError::MetadataProjection(error.into()))?;
        let head = loaded.state.clone();

        // Check the terminal state before the writer fence because a deleted
        // namespace requires no further write.
        if head.status == (NamespaceStatus::Deleted {}) {
            // A deleted head resolves an earlier ambiguous swap as success.
            if attempted_swap {
                return Ok(DeleteNamespaceResponse {
                    namespace_id: namespace_id.clone(),
                    head_seq: head.seq,
                });
            }
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            });
        }

        // The epoch is the fence: any takeover bumps it, so a mismatch means
        // another writer owns the namespace and this delete must not land.
        if head.writer_epoch != acquired_writer.writer_epoch {
            return Err(CoreError::WriterFenced(crate::error::WriterFence {
                fenced_epoch: acquired_writer.writer_epoch,
                active_epoch: head.writer_epoch,
                active_writer: head.writer.as_ref().map(|writer| writer.writer_id.clone()),
                active_acquired_at_ms: head.writer.as_ref().map(|writer| writer.acquired_at_ms),
            }));
        }

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
            Ok(_) => {
                return Ok(DeleteNamespaceResponse {
                    namespace_id: namespace_id.clone(),
                    head_seq: head.seq,
                });
            }
            // The head moved (a commit, fence takeover, or another deleter
            // landed first). Reload and re-evaluate.
            Err(ObjectStoreError::PreconditionFailed { .. }) => continue,
            // Outcome unobserved; the reload at the top of the loop resolves
            // it, because the target state is terminal.
            Err(ObjectStoreError::Transport { .. }) => {
                attempted_swap = true;
                continue;
            }
            Err(other) => return Err(CoreError::store(&loaded.object_key, &other)),
        }
    }

    Err(CoreError::HeadPublish(
        crate::commit::CommitHeadPublishError::StaleHead,
    ))
}
