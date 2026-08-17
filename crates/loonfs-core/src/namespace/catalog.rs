//! The namespace catalog: the namespace's immutable identity — its content
//! store and name policy — read from the head that carries them.

use crate::control_object::ControlObjectLoadError;
use crate::namespace::control::load_head_object;
use loonfs_api::wire::control::HeadState;
use loonfs_api::{ContentStoreId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NamespaceCatalogLoadError {
    #[error("failed to load namespace head: {0}")]
    LoadHead(#[from] ControlObjectLoadError),
}

/// The namespace's spec-immutable identity: the content store it publishes
/// file bytes into and the name policy its keys are computed under.
///
/// Both live in the head and are carried forward verbatim by every head the
/// namespace ever publishes, so an entry built from any head of a namespace
/// is valid for the namespace's whole life.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedNamespaceCatalogEntry {
    namespace_id: NamespaceId,
    content_store_id: ContentStoreId,
}

impl VerifiedNamespaceCatalogEntry {
    /// Reads the namespace's identity off a loaded head. No I/O: the head is
    /// the only durable home of these fields.
    pub fn from_head(head: &HeadState) -> Self {
        Self {
            namespace_id: head.namespace_id.clone(),
            content_store_id: head.content_store_id.clone(),
        }
    }

    /// Returns the namespace whose immutable identity this entry carries.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn content_store_id(&self) -> &ContentStoreId {
        &self.content_store_id
    }
}

/// Loads the namespace's immutable identity for a caller holding no head.
///
/// Callers that already loaded or pinned a head build the entry from it
/// with [`VerifiedNamespaceCatalogEntry::from_head`] instead of paying this
/// read.
pub async fn load_namespace_catalog_entry<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<VerifiedNamespaceCatalogEntry, NamespaceCatalogLoadError> {
    let head = load_head_object(store, expected_namespace_id).await?;
    Ok(VerifiedNamespaceCatalogEntry::from_head(&head.state))
}

pub(crate) async fn load_namespace_content_store_id<S: ObjectStore + ?Sized>(
    store: &S,
    expected_namespace_id: &NamespaceId,
) -> Result<ContentStoreId, NamespaceCatalogLoadError> {
    Ok(load_namespace_catalog_entry(store, expected_namespace_id)
        .await?
        .content_store_id)
}
