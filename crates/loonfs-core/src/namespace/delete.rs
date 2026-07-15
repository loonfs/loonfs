//! Namespace deletion: a fenced, terminal head-state transition.

use crate::context::MutationContext;
use crate::error::CoreError;
use crate::namespace::control::read_head_object;
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::options::DeleteNamespaceOptions;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope, NamespaceState,
};
use loonfs_api::{DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError, PutMode};

const MAX_DELETE_CAS_ATTEMPTS: usize = 8;

/// Deletes a namespace by compare-and-swapping its head into the terminal
/// `deleted` state (format spec, "Tombstones and deletion").
///
/// The deleting writer first acquires the namespace writer epoch, so no stale
/// writer session can publish past the delete, then swaps the head. Every
/// retry re-checks the acquired epoch against the reloaded head, so a writer
/// takeover between acquisition and the swap aborts the delete instead of
/// deleting a namespace another writer now owns.
/// The delete linearizes at that swap: commits whose
/// head advance serialized before it stay committed and durable; everything
/// that observes the deleted head afterward fails with `namespace_deleted`.
///
/// Because `deleted` is terminal, a lost acknowledgment is self-resolving:
/// if a reload after an ambiguous swap shows the head deleted, the delete
/// succeeded (ours or a concurrent deleter's — indistinguishable and
/// equivalent). Only an unreachable store surfaces an unknown outcome.
pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    context: &MutationContext,
) -> Result<DeleteNamespaceResponse, CoreError> {
    let acquired_writer = acquire_writer_epoch(store, namespace_id, context).await?;

    let mut attempted_swap = false;
    for _attempt in 0..MAX_DELETE_CAS_ATTEMPTS {
        let loaded = read_head_object(store, namespace_id)
            .await
            .map_err(|error| CoreError::MetadataProjection(error.into()))?;
        let head = loaded.envelope.state.clone();

        if head.state == NamespaceState::Deleted {
            // Terminal state reached. If we already sent a swap, the delete
            // is done regardless of whose swap landed; if we never sent one,
            // the namespace was already deleted when we arrived.
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
            }));
        }

        if let Some(expected) = options.expected_head_seq {
            if head.seq != expected {
                return Err(CoreError::HeadPublish(
                    crate::commit::CommitHeadPublishError::StaleHead,
                ));
            }
        }

        let head_etag = loaded.metadata.etag.clone().ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!("missing head etag for `{}`", loaded.object_key))
        })?;
        let deleted_head = HeadState {
            state: NamespaceState::Deleted,
            ..head.clone()
        };
        let envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::WalHead,
            &context.writer_version,
            deleted_head,
        )
        .map_err(|err| CoreError::Internal(format!("failed to build head envelope: {err}")))?;
        let encoded = encode_control_object(&envelope)
            .map_err(|err| CoreError::Internal(format!("failed to encode head object: {err}")))?;

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
            Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. },
            ) => continue,
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
