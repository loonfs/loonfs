use crate::error::CoreError;
use crate::namespace::control::{
    read_content_store_descriptor_object, read_namespace_descriptor_object, ControlObjectLoadError,
};
use loon_api::wire::control::NamespaceDescriptorState;
use loon_api::{ContentStoreId, NamespaceId, NamespaceIdValidationError, NamespaceSummary};
use loon_objectstore::keys::{namespace_descriptor, namespace_head, namespace_lease};
use loon_objectstore::ObjectStore;
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum NamespaceCatalogLoadError {
    #[error("failed to load namespace descriptor: {0}")]
    LoadNamespaceDescriptor(ControlObjectLoadError),
    #[error("failed to load content store descriptor: {0}")]
    LoadContentStoreDescriptor(ControlObjectLoadError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedNamespaceCatalogEntry {
    pub(crate) namespace_descriptor: NamespaceDescriptorState,
    pub(crate) content_store_id: ContentStoreId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceInitializationState {
    Absent,
    Partial,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum NamespaceInitializationError {
    #[error(transparent)]
    InvalidNamespaceId(#[from] NamespaceIdValidationError),
    #[error("failed to inspect namespace descriptor object: {0}")]
    InspectNamespaceDescriptor(String),
    #[error("failed to inspect namespace head object: {0}")]
    InspectNamespaceHead(String),
    #[error("failed to inspect namespace lease object: {0}")]
    InspectNamespaceLease(String),
    #[error("failed to load namespace descriptor: {0}")]
    LoadNamespaceDescriptor(ControlObjectLoadError),
    #[error("failed to load content store descriptor: {0}")]
    LoadContentStoreDescriptor(ControlObjectLoadError),
}

pub(crate) async fn load_namespace_descriptor<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<NamespaceDescriptorState, NamespaceCatalogLoadError> {
    let loaded_descriptor = read_namespace_descriptor_object(store, expected_namespace)
        .await
        .map_err(NamespaceCatalogLoadError::LoadNamespaceDescriptor)?;
    Ok(loaded_descriptor.envelope.state)
}

pub(crate) async fn load_namespace_catalog_entry<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<VerifiedNamespaceCatalogEntry, NamespaceCatalogLoadError> {
    let namespace_descriptor = load_namespace_descriptor(store, expected_namespace).await?;
    let content_store_id = namespace_descriptor.content_store_id.clone();
    read_content_store_descriptor_object(store, &content_store_id)
        .await
        .map_err(NamespaceCatalogLoadError::LoadContentStoreDescriptor)?;

    Ok(VerifiedNamespaceCatalogEntry {
        namespace_descriptor,
        content_store_id,
    })
}

pub(crate) async fn load_namespace_content_store_id<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace: &NamespaceId,
) -> Result<ContentStoreId, NamespaceCatalogLoadError> {
    Ok(load_namespace_catalog_entry(store, expected_namespace)
        .await?
        .content_store_id)
}

pub(crate) async fn list_namespaces<S: ObjectStore + ?Sized>(
    store: &S,
) -> Result<Vec<NamespaceSummary>, CoreError> {
    let keys = store
        .list_prefix("namespaces/")
        .await
        .map_err(|err| CoreError::Store(err.to_string()))?;
    let mut names = BTreeSet::new();
    for key in keys {
        let Some(rest) = key.strip_prefix("namespaces/") else {
            continue;
        };
        let Some((namespace, leaf)) = rest.split_once('/') else {
            continue;
        };
        if leaf != "descriptor.json" {
            continue;
        };
        let namespace_id = NamespaceId::parse(namespace)?;
        load_namespace_descriptor(store, &namespace_id).await?;
        names.insert(namespace_id);
    }
    Ok(names
        .into_iter()
        .map(|namespace_id| NamespaceSummary { namespace_id })
        .collect())
}

pub(crate) async fn namespace_initialization_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<NamespaceInitializationState, NamespaceInitializationError> {
    NamespaceId::parse(namespace_id.as_str())?;

    let descriptor_key = namespace_descriptor(namespace_id.as_str());
    let head_key = namespace_head(namespace_id.as_str());
    let lease_key = namespace_lease(namespace_id.as_str());

    let descriptor_exists = store
        .head(&descriptor_key)
        .await
        .map_err(|err| NamespaceInitializationError::InspectNamespaceDescriptor(err.to_string()))?
        .is_some();
    let head_exists = store
        .head(&head_key)
        .await
        .map_err(|err| NamespaceInitializationError::InspectNamespaceHead(err.to_string()))?
        .is_some();
    let lease_exists = store
        .head(&lease_key)
        .await
        .map_err(|err| NamespaceInitializationError::InspectNamespaceLease(err.to_string()))?
        .is_some();

    match (descriptor_exists, head_exists, lease_exists) {
        (false, false, false) => Ok(NamespaceInitializationState::Absent),
        (true, true, true) => {
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
