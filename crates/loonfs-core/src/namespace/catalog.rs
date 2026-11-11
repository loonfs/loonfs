//! Verified namespace identity, access mode, and pool.

use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{NamespaceAccess, NamespaceId, PoolId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedNamespaceCatalogEntry {
    namespace_id: NamespaceId,
    access: NamespaceAccess,
    pool_id: PoolId,
}

impl VerifiedNamespaceCatalogEntry {
    pub fn access(&self) -> &NamespaceAccess {
        &self.access
    }

    pub fn from_head(head: &NamespaceReadState) -> Self {
        Self {
            namespace_id: head.namespace_id.clone(),
            access: head.access.clone(),
            pool_id: head.pool_id.clone(),
        }
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the pool whose prefix stores the namespace's content objects
    /// and upload records.
    pub fn pool_id(&self) -> &PoolId {
        &self.pool_id
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
