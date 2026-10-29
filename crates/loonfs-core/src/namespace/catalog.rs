//! Verified namespace identity and access mode.

use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{NamespaceAccess, NamespaceId};

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

impl<S: ObjectStore> NamespaceStore<S> {
    pub async fn load_catalog_entry(&self) -> crate::error::Result<VerifiedNamespaceCatalogEntry> {
        let manifest = self.load_current_manifest().await?;
        Ok(VerifiedNamespaceCatalogEntry::from_head(
            &NamespaceReadState::from(manifest.payload()),
        ))
    }
}
