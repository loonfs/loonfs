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

/// Deletes a namespace by compare-and-swapping its head into the terminal
/// `deleted` state (format spec, "Tombstones and deletion").
///
/// `acquired_writer` is the deleting session's writer epoch, so no stale
/// writer session can publish past the delete. The caller owns acquisition —
/// [`NamespaceCommitEngine`](crate::publish::NamespaceCommitEngine), which
/// refuses outright when the session is already fenced. Every retry that
/// still has a swap to make re-checks that epoch against the reloaded head,
/// so a writer takeover between acquisition and the swap aborts the delete
/// instead of deleting a namespace another writer now owns.
/// The delete linearizes at that swap: commits whose
/// head advance serialized before it stay committed and durable; everything
/// that observes the deleted head afterward fails with `namespace_deleted`.
///
/// A reload that finds the head already deleted answers before the fence,
/// because there is no swap left to fence. The namespace is in the terminal
/// state this call was asking for, so the answer is the same whoever owns
/// the writer epoch now: `namespace_deleted` when this call never swapped
/// (API spec, "DELETE /v0/namespaces/{ns}"), and success when it did. Every
/// acquisition bumps the epoch, so checking the fence first would report a
/// concurrent deleter's tombstone as `writer_fenced` and, worse, would fence
/// this session for good over a delete that landed.
///
/// Because `deleted` is terminal, a lost acknowledgment is self-resolving:
/// if a reload after an ambiguous swap shows the head deleted, the delete
/// succeeded (ours or a concurrent deleter's — indistinguishable and
/// equivalent). Only an unreachable store surfaces an unknown outcome.
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

        // Terminal state first, ahead of the fence and the precondition
        // below: both of those decide whether this call may swap, and there
        // is nothing left to swap. See the fence paragraph above.
        if head.status == (NamespaceStatus::Deleted {}) {
            // If we already sent a swap, the delete is done regardless of
            // whose swap landed; if we never sent one, the namespace was
            // already deleted when we arrived.
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
