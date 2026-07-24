//! The namespace catalog: the immutable descriptor pair (namespace config
//! and content-store descriptor) loaded once and reused for a session, the
//! initialization-state probe over that pair, and the installation writes
//! namespace create and fork share.

use crate::error::{CoreError, MetadataProjectionLoadError, StoreFailureClass};
use crate::namespace::control::{
    read_content_store_descriptor_object, read_head_object, read_namespace_descriptor_object,
    ControlObjectLoadError,
};
use bytes::Bytes;
use loonfs_api::wire::control::NamespaceConfigState;
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::{ContentStoreId, NamespaceId};
use loonfs_objectstore::keys::{metadata_root, namespace_config, wal_floor, wal_head};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

// Both variants share ControlObjectLoadError; conversion stays explicit so
// `?` cannot pick a variant silently.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NamespaceCatalogLoadError {
    #[error("failed to load namespace descriptor: {0}")]
    LoadNamespaceDescriptor(ControlObjectLoadError),
    #[error("failed to load content store descriptor: {0}")]
    LoadContentStoreDescriptor(ControlObjectLoadError),
}

/// The namespace's spec-immutable identity pair: its config and the
/// content-store binding it was created with. Loaded once and reusable for
/// the life of a process; neither object can change after creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedNamespaceCatalogEntry {
    pub(crate) namespace_config: NamespaceConfigState,
    pub(crate) content_store_id: ContentStoreId,
}

impl VerifiedNamespaceCatalogEntry {
    /// Returns the namespace whose immutable catalog binding was verified.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_config.namespace_id
    }

    pub fn content_store_id(&self) -> &ContentStoreId {
        &self.content_store_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceInitializationState {
    Absent,
    /// Head and descriptor are both absent but pre-head namespace objects
    /// exist: a create or fork crashed before its gating head write, or one
    /// is in flight right now. Explicit repair uses age to decide whether
    /// reaping is safe.
    PreHeadDebris,
    /// The head exists but the descriptor does not: a create or fork
    /// crashed after its linearization point. Explicit repair can complete a
    /// descriptor whose value is derivable from the tree.
    Partial,
    Complete,
}

// The two Load* variants share ControlObjectLoadError; conversion stays
// explicit so `?` cannot pick a variant silently.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum NamespaceInitializationError {
    #[error("failed to inspect namespace descriptor object `{object_key}`: {message}")]
    InspectNamespaceDescriptor {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("failed to inspect namespace head object `{object_key}`: {message}")]
    InspectNamespaceHead {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("failed to inspect namespace control object `{object_key}`: {message}")]
    InspectNamespaceControl {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("failed to load namespace descriptor: {0}")]
    LoadNamespaceDescriptor(ControlObjectLoadError),
    #[error("failed to load namespace head: {0}")]
    LoadNamespaceHead(ControlObjectLoadError),
    #[error("failed to load content store descriptor: {0}")]
    LoadContentStoreDescriptor(ControlObjectLoadError),
}

pub(crate) async fn load_namespace_descriptor<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<NamespaceConfigState, NamespaceCatalogLoadError> {
    let loaded_descriptor = read_namespace_descriptor_object(store, expected_namespace_id)
        .await
        .map_err(NamespaceCatalogLoadError::LoadNamespaceDescriptor)?;
    Ok(loaded_descriptor.envelope.state)
}

pub async fn load_namespace_catalog_entry<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<VerifiedNamespaceCatalogEntry, NamespaceCatalogLoadError> {
    let namespace_config = load_namespace_descriptor(store, expected_namespace_id).await?;
    let content_store_id = namespace_config.content_store_id.clone();
    read_content_store_descriptor_object(store, &content_store_id)
        .await
        .map_err(NamespaceCatalogLoadError::LoadContentStoreDescriptor)?;

    Ok(VerifiedNamespaceCatalogEntry {
        namespace_config,
        content_store_id,
    })
}

pub(crate) async fn load_namespace_content_store_id<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<ContentStoreId, NamespaceCatalogLoadError> {
    Ok(load_namespace_catalog_entry(store, expected_namespace_id)
        .await?
        .content_store_id)
}

pub(crate) async fn namespace_initialization_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceInitializationState, NamespaceInitializationError> {
    let descriptor_key = namespace_config(namespace_id.as_str());
    let head_key = wal_head(namespace_id.as_str());

    let descriptor_exists = store
        .head(&descriptor_key)
        .await
        .map_err(
            |err| NamespaceInitializationError::InspectNamespaceDescriptor {
                object_key: descriptor_key.clone(),
                message: err.message(),
                class: StoreFailureClass::of(&err),
            },
        )?
        .is_some();
    let head_exists = store
        .head(&head_key)
        .await
        .map_err(|err| NamespaceInitializationError::InspectNamespaceHead {
            object_key: head_key.clone(),
            message: err.message(),
            class: StoreFailureClass::of(&err),
        })?
        .is_some();

    match (descriptor_exists, head_exists) {
        (false, false) => {
            // Nothing is visible yet, but a crashed create or fork may have
            // left pre-head root/floor debris that blocks a fresh attempt's
            // put-if-absent writes. A stray immutable manifest alone blocks
            // nothing and a successful namespace can reap it later.
            for probe_key in [
                metadata_root(namespace_id.as_str()),
                wal_floor(namespace_id.as_str()),
            ] {
                let exists = store
                    .head(&probe_key)
                    .await
                    .map_err(
                        |err| NamespaceInitializationError::InspectNamespaceControl {
                            object_key: probe_key.clone(),
                            message: err.message(),
                            class: StoreFailureClass::of(&err),
                        },
                    )?
                    .is_some();
                if exists {
                    return Ok(NamespaceInitializationState::PreHeadDebris);
                }
            }
            Ok(NamespaceInitializationState::Absent)
        }
        (true, true) => {
            let head = read_head_object(store, namespace_id)
                .await
                .map_err(NamespaceInitializationError::LoadNamespaceHead)?;
            if head.envelope.state.state == NamespaceState::Condemned {
                return Ok(NamespaceInitializationState::Partial);
            }
            let descriptor = read_namespace_descriptor_object(store, namespace_id)
                .await
                .map_err(NamespaceInitializationError::LoadNamespaceDescriptor)?;
            read_content_store_descriptor_object(
                store,
                &descriptor.envelope.state.content_store_id,
            )
            .await
            .map_err(NamespaceInitializationError::LoadContentStoreDescriptor)?;
            Ok(NamespaceInitializationState::Complete)
        }
        _ => Ok(NamespaceInitializationState::Partial),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceInstallGate {
    Landed,
    /// The store reported a conditional-write conflict, but an exact readback
    /// found this attempt's head in a still-partial namespace. Prepare the
    /// deterministic prerequisites, then preserve the existing unknown-outcome
    /// surface so explicit repair can complete the tree.
    ExistingPartial,
}

/// Installs the WAL head as the first fixed-key conditional admission gate for
/// create and fork. An exact partial readback preserves lost-ack recovery
/// without letting a different or condemned head admit any subsequent
/// fixed-key install write.
pub(crate) async fn put_namespace_install_gate<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    bytes: &[u8],
) -> Result<NamespaceInstallGate, CoreError> {
    let object_key = wal_head(namespace_id.as_str());
    match store
        .put_if_absent(&object_key, Bytes::copy_from_slice(bytes))
        .await
    {
        Ok(_) => Ok(NamespaceInstallGate::Landed),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            let exact_readback = store
                .get(&object_key, None)
                .await
                .map_err(|read_error| CoreError::store(&object_key, &read_error))?
                .is_some_and(|existing| existing.as_ref() == bytes);
            let state = namespace_initialization_state(store, namespace_id)
                .await
                .map_err(map_namespace_initialization_error_to_core)?;
            if exact_readback
                && matches!(
                    state,
                    NamespaceInitializationState::Partial
                        | NamespaceInitializationState::PreHeadDebris
                )
            {
                Ok(NamespaceInstallGate::ExistingPartial)
            } else {
                Err(namespace_install_conflict_error(
                    namespace_id,
                    &object_key,
                    state,
                ))
            }
        }
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

/// The one classification-probe failure mapping every entry point (create,
/// fork, status, uploads) shares: store failures keep their
/// [`StoreFailureClass`], so the same probe failure answers the same wire
/// code — `permission_denied` for rejected credentials — regardless of which
/// operation ran the probe.
pub(crate) fn map_namespace_initialization_error_to_core(
    error: NamespaceInitializationError,
) -> CoreError {
    match error {
        NamespaceInitializationError::LoadNamespaceDescriptor(error) => {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadNamespaceDescriptor(
                error,
            ))
        }
        NamespaceInitializationError::LoadNamespaceHead(error) => {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        }
        NamespaceInitializationError::LoadContentStoreDescriptor(error) => {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadContentStoreDescriptor(
                error,
            ))
        }
        NamespaceInitializationError::InspectNamespaceDescriptor {
            object_key,
            message,
            class,
        }
        | NamespaceInitializationError::InspectNamespaceHead {
            object_key,
            message,
            class,
        }
        | NamespaceInitializationError::InspectNamespaceControl {
            object_key,
            message,
            class,
        } => CoreError::Store {
            object_key,
            message,
            class,
        },
    }
}

/// Installs one control object of a namespace being created or forked.
///
/// Losing the put-if-absent race re-classifies the target instead of
/// guessing at the winner: a complete target answers `namespace_exists`, a
/// partial or pre-head tree answers `namespace_partial`, and an absent
/// target reports the write failure itself. Non-conflict failures keep
/// their [`StoreFailureClass`].
pub(crate) async fn put_target_namespace_control_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), CoreError> {
    match store
        .put_if_absent(object_key, Bytes::copy_from_slice(bytes))
        .await
    {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            let state = namespace_initialization_state(store, namespace_id)
                .await
                .map_err(map_namespace_initialization_error_to_core)?;
            Err(namespace_install_conflict_error(
                namespace_id,
                object_key,
                state,
            ))
        }
        Err(err) => Err(CoreError::store(object_key, &err)),
    }
}

fn namespace_install_conflict_error(
    namespace_id: &NamespaceId,
    object_key: &str,
    state: NamespaceInitializationState,
) -> CoreError {
    match state {
        NamespaceInitializationState::Complete => CoreError::NamespaceExists {
            namespace_id: namespace_id.clone(),
        },
        NamespaceInitializationState::Partial | NamespaceInitializationState::PreHeadDebris => {
            CoreError::NamespacePartial {
                namespace_id: namespace_id.clone(),
            }
        }
        NamespaceInitializationState::Absent => CoreError::Store {
            object_key: object_key.to_owned(),
            message: "control object write failed, but namespace remains absent".to_owned(),
            class: StoreFailureClass::Other,
        },
    }
}

/// Publishes the namespace descriptor — the completion marker — for a
/// post-head completion retry, tolerating the race: a conflict means another
/// completion wrote it first and the namespace is complete either way.
pub(crate) async fn put_completion_descriptor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    bytes: &[u8],
) -> Result<(), CoreError> {
    let descriptor_key = namespace_config(namespace_id.as_str());
    match store
        .put_if_absent(&descriptor_key, Bytes::copy_from_slice(bytes))
        .await
    {
        Ok(_)
        | Err(ObjectStoreError::PreconditionFailed { .. }) => {
            Ok(())
        }
        Err(err) => Err(CoreError::store(&descriptor_key, &err)),
    }
}
