//! Verified namespace identity and access mode.

use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::load_current_manifest;
use crate::namespace::state::NamespaceReadState;
use loonfs_api::{ContentStoreId, NamespaceAccess, NamespaceGeneration, NamespaceId};
use loonfs_objectstore::ObjectStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedNamespaceCatalogEntry {
    namespace_id: NamespaceId,
    generation: NamespaceGeneration,
    content_store_id: ContentStoreId,
    access: NamespaceAccess,
}

impl VerifiedNamespaceCatalogEntry {
    pub fn access(&self) -> &NamespaceAccess {
        &self.access
    }

    pub fn from_head(head: &NamespaceReadState) -> Self {
        Self {
            namespace_id: head.namespace_id.clone(),
            generation: head.generation,
            content_store_id: head.content_store_id.clone(),
            access: head.access.clone(),
        }
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn generation(&self) -> NamespaceGeneration {
        self.generation
    }

    pub fn content_store_id(&self) -> &ContentStoreId {
        &self.content_store_id
    }
}

pub async fn load_namespace_catalog_entry<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<VerifiedNamespaceCatalogEntry, ControlObjectLoadError> {
    let manifest = load_current_manifest(store, expected_namespace_id).await?;
    Ok(VerifiedNamespaceCatalogEntry::from_head(
        &NamespaceReadState::from(manifest.envelope.payload()),
    ))
}

pub(crate) async fn load_namespace_content_store_id<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<ContentStoreId, ControlObjectLoadError> {
    Ok(load_namespace_catalog_entry(store, expected_namespace_id)
        .await?
        .content_store_id)
}
