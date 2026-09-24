//! Verified namespace identity and access mode.

use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::load_current_manifest;
use crate::namespace::state::NamespaceReadState;
use loonfs_api::{NamespaceAccess, NamespaceId};
use loonfs_objectstore::ObjectStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedNamespaceCatalogEntry {
    namespace_id: NamespaceId,
    access: NamespaceAccess,
}

impl VerifiedNamespaceCatalogEntry {
    pub fn access(&self) -> &NamespaceAccess {
        &self.access
    }

    pub fn from_head(head: &NamespaceReadState) -> Self {
        Self {
            namespace_id: head.namespace_id.clone(),
            access: head.access.clone(),
        }
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }
}

pub async fn load_namespace_catalog_entry<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<VerifiedNamespaceCatalogEntry, ControlObjectLoadError> {
    let manifest = load_current_manifest(store, expected_namespace_id).await?;
    Ok(VerifiedNamespaceCatalogEntry::from_head(
        &NamespaceReadState::from(manifest.state.envelope.payload()),
    ))
}
