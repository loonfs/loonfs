use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::load_current_manifest;
use crate::namespace::state::NamespaceReadState;
use loonfs_api::{ContentStoreId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NamespaceCatalogLoadError {
    #[error("failed to load namespace manifest: {0}")]
    LoadManifest(#[from] ControlObjectLoadError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedNamespaceCatalogEntry {
    namespace_id: NamespaceId,
    content_store_id: ContentStoreId,
}

impl VerifiedNamespaceCatalogEntry {
    pub fn from_head(head: &NamespaceReadState) -> Self {
        Self {
            namespace_id: head.namespace_id.clone(),
            content_store_id: head.content_store_id.clone(),
        }
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn content_store_id(&self) -> &ContentStoreId {
        &self.content_store_id
    }
}

pub async fn load_namespace_catalog_entry<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<VerifiedNamespaceCatalogEntry, NamespaceCatalogLoadError> {
    let manifest = load_current_manifest(store, expected_namespace_id).await?;
    Ok(VerifiedNamespaceCatalogEntry {
        namespace_id: manifest.envelope.payload().namespace_id.clone(),
        content_store_id: manifest.envelope.payload().content_store_id.clone(),
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
