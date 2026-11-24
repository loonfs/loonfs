//! Verified namespace identity and pool.

use crate::engine::RuntimeReadContext;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{NamespaceId, PoolId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedNamespaceCatalogEntry {
    namespace_id: NamespaceId,
    pool_id: PoolId,
}

impl VerifiedNamespaceCatalogEntry {
    pub fn from_head(head: &NamespaceReadState) -> Self {
        Self {
            namespace_id: head.namespace_id.clone(),
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

/// The catalog entry of a namespace whose head was active when the entry was
/// built.
///
/// Staging content and each step of an upload session except its abort take
/// this entry, and [`Self::from_context`] is the only way to build one. A
/// read context's head is active, so a
/// caller that has only a [`VerifiedNamespaceCatalogEntry`] can't stage
/// content, start an upload session, or complete one for a namespace that is
/// initializing or deleted. LoonFS aborts the sessions of a deleted namespace
/// too, so an abort takes the plain entry, and this entry dereferences to the
/// plain entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveNamespaceCatalogEntry {
    entry: VerifiedNamespaceCatalogEntry,
}

impl ActiveNamespaceCatalogEntry {
    /// Builds the entry of the head that `context` reads.
    pub fn from_context(context: &RuntimeReadContext) -> Self {
        Self {
            entry: VerifiedNamespaceCatalogEntry::from_head(context.read_basis().head()),
        }
    }
}

impl std::ops::Deref for ActiveNamespaceCatalogEntry {
    type Target = VerifiedNamespaceCatalogEntry;

    fn deref(&self) -> &VerifiedNamespaceCatalogEntry {
        &self.entry
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
