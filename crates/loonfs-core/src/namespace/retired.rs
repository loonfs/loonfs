//! Point reads and verification for retired generation records.

use super::control::load_discovered_manifest;
use crate::checkpoint::ensure_manifest_reference_matches;
use crate::control_object::{
    expect_identity_field, expect_namespace, load_control_object, ControlObjectLoadError,
};
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::{ControlObjectKind, RetiredGenerationPayload};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{NamespaceGeneration, NamespaceId};
use loonfs_objectstore::keys::retired_generation_record;
use loonfs_objectstore::ObjectStore;

pub(crate) async fn load_retired_generation<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    generation: NamespaceGeneration,
) -> Result<Option<RetiredGenerationPayload>> {
    let loaded = load_control_object(
        store,
        retired_generation_record(namespace_id, generation),
        ControlObjectKind::RetiredGeneration,
        |state: &RetiredGenerationPayload| {
            expect_namespace(namespace_id, &state.namespace_id)?;
            expect_namespace(namespace_id, &state.tombstone.owner_namespace_id)?;
            expect_identity_field(
                "generation",
                &generation.to_string(),
                &state.generation.to_string(),
            )
        },
    )
    .await;
    match loaded {
        Ok(loaded) => Ok(Some(loaded.state)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn load_retired_tombstone<S: ObjectStore + ?Sized>(
    store: &S,
    record: &RetiredGenerationPayload,
) -> Result<Option<NamespaceManifestPayload>> {
    let Some(loaded) =
        load_discovered_manifest(store, &record.namespace_id, record.tombstone.manifest_no).await?
    else {
        return Ok(None);
    };
    ensure_manifest_reference_matches("retired generation", &record.tombstone, &loaded.envelope)?;
    let payload = loaded.envelope.payload();
    if payload.generation != record.generation || !payload.status.is_deleted() {
        return Err(CoreError::NamespaceCorrupt(format!(
            "retired generation `{}` of `{}` names a different generation or an active manifest",
            record.generation, record.namespace_id,
        )));
    }
    Ok(Some(payload.clone()))
}
