//! Publishes the first manifest of a namespace.

use super::control::load_current_manifest_if_present;
use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::error::{CoreError, Result};
use crate::time::MonotonicTimer;
use bytes::Bytes;
use loonfs_api::wire::control::{encode_control_state, ControlObjectKind, HintPayload};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{ManifestNo, WalNo};
use loonfs_objectstore::keys::hint;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use serde::Serialize;

pub(super) async fn publish_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    start: &NamespaceManifestPayload,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<()> {
    let namespace_id = &start.namespace_id;
    if let Some(current) = load_current_manifest_if_present(store, namespace_id).await? {
        if current.envelope.payload().status.is_deleted() {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            });
        }
        return Err(CoreError::NamespaceExists {
            namespace_id: namespace_id.clone(),
        });
    }
    let first = HintPayload {
        namespace_id: namespace_id.clone(),
        manifest_no: ManifestNo(1),
        wal_no: WalNo(0),
    };
    put_control_if_absent(store, hint(namespace_id), ControlObjectKind::Hint, &first).await?;
    let manifest = encode_manifest(start.clone())?;
    match publish_manifest(store, manifest, timer, started_ms).await? {
        ManifestPublicationOutcome::Published(_) => Ok(()),
        ManifestPublicationOutcome::CoveredByCurrent(_)
        | ManifestPublicationOutcome::PredecessorChanged(_) => {
            let current = super::control::load_current_manifest(store, namespace_id).await?;
            if current.envelope.payload().status.is_deleted() {
                return Err(CoreError::NamespaceDeleted {
                    namespace_id: namespace_id.clone(),
                });
            }
            Err(CoreError::NamespaceExists {
                namespace_id: namespace_id.clone(),
            })
        }
    }
}

async fn put_control_if_absent<S: ObjectStore + ?Sized, T: Serialize>(
    store: &S,
    object_key: String,
    kind: ControlObjectKind,
    state: &T,
) -> Result<()> {
    let bytes = encode_control_state(kind, state).map_err(|error| CoreError::Codec {
        object_key: object_key.clone(),
        message: error.to_string(),
    })?;
    match store.put_if_absent(&object_key, Bytes::from(bytes)).await {
        Ok(_) | Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(()),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}
