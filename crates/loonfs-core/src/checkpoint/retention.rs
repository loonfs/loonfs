//! Retention floor advancement, gated on verified derived progress.

use super::load::load_verified_manifest_materialization;
use super::publish::{compare_and_swap_head, HEAD_CAS_RETRY_LIMIT};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::namespace::basis::BasisLoadError;
use crate::namespace::control::read_head_object;
use loonfs_api::wire::control::{
    decode_control_object, ControlObjectKind, HeadState, ProgressStateEnvelope,
};
use loonfs_api::{AdvanceRetentionResponse, ChangeSeq, NamespaceId};
use loonfs_objectstore::keys::{derived_progress, DerivedWorkClass};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

// V1 does not require any derived work classes to be caught up before the
// retention floor advances. This hook stays in place so future retention gates
// can add progress requirements without restructuring the flow.
pub(super) const REQUIRED_RETENTION_PROGRESS_CLASSES: &[DerivedWorkClass] = &[];

pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AdvanceRetentionResponse, CoreError> {
    for _attempt in 0..HEAD_CAS_RETRY_LIMIT {
        let loaded_head = read_head_object(store, namespace_id)
            .await
            .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
        let head = loaded_head.envelope.state;
        let Some(current_manifest_id) = head.current_manifest_id else {
            return Err(CoreError::CheckpointUnavailable(format!(
                "namespace `{}` has no published manifest",
                namespace_id.as_str()
            )));
        };
        let manifest =
            load_verified_manifest_materialization(store, namespace_id, current_manifest_id)
                .await
                .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?;
        let target_floor = manifest.manifest.payload.head_seq;
        ensure_required_retention_progress(store, namespace_id, target_floor).await?;

        if head.retention_floor_seq >= target_floor {
            return Ok(AdvanceRetentionResponse {
                namespace_id: namespace_id.clone(),
                retention_floor_seq: head.retention_floor_seq,
            });
        }

        let next_head = HeadState {
            namespace_id: head.namespace_id.clone(),
            seq: head.seq,
            head_commit_id: head.head_commit_id.clone(),
            active_fence_token: head.active_fence_token,
            next_inode_id: head.next_inode_id,
            name_policy: head.name_policy,
            current_manifest_id: head.current_manifest_id,
            latest_checkpoint_id: head.latest_checkpoint_id.clone(),
            retention_floor_seq: target_floor,
            visible_wal_tip: head.visible_wal_tip.clone(),
        };
        match compare_and_swap_head(
            store,
            &loaded_head.object_key,
            loaded_head.metadata.etag.as_deref(),
            &context.writer_version,
            &next_head,
        )
        .await
        {
            Ok(()) => {
                return Ok(AdvanceRetentionResponse {
                    namespace_id: namespace_id.clone(),
                    retention_floor_seq: target_floor,
                });
            }
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(error) => return Err(CoreError::Store(error.to_string())),
        }
    }

    Err(CoreError::Store(
        "retention floor compare-and-swap retry exhausted".to_owned(),
    ))
}

pub(super) async fn ensure_required_retention_progress<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    target_floor: ChangeSeq,
) -> Result<(), CoreError> {
    for work_class in REQUIRED_RETENTION_PROGRESS_CLASSES {
        let object_key = derived_progress(namespace_id.as_str(), *work_class);
        let work_class_name = work_class.as_str();
        let Some(bytes) = store
            .get(&object_key, None)
            .await
            .map_err(|err| CoreError::Store(err.to_string()))?
        else {
            return Err(CoreError::CheckpointUnavailable(format!(
                "required derived progress `{work_class_name}` is missing for namespace `{}`",
                namespace_id.as_str()
            )));
        };
        let progress: ProgressStateEnvelope =
            decode_control_object(&bytes, ControlObjectKind::NamespaceProgress).map_err(|err| {
                CoreError::Store(format!("invalid derived progress `{object_key}`: {err}"))
            })?;
        if progress.state.through_seq < target_floor {
            return Err(CoreError::CheckpointUnavailable(format!(
                "required derived progress `{work_class_name}` only covers {:?} for namespace `{}`",
                progress.state.through_seq,
                namespace_id.as_str()
            )));
        }
    }
    Ok(())
}
