//! Reusable key predicates for object-store wrappers.

use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{metadata_root, wal_head};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use std::fmt;
use std::sync::Arc;

type Predicate = dyn Fn(&str) -> bool + Send + Sync;

/// A cloneable predicate selecting object keys for a test wrapper.
#[derive(Clone)]
pub struct KeyPredicate {
    predicate: Arc<Predicate>,
}

impl KeyPredicate {
    /// Selects keys accepted by `predicate`.
    pub fn new(predicate: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        Self {
            predicate: Arc::new(predicate),
        }
    }

    /// Selects every key.
    pub fn any() -> Self {
        Self::new(|_| true)
    }

    /// Selects exactly `key`.
    pub fn exact(key: impl Into<String>) -> Self {
        let key = key.into();
        Self::new(move |candidate| candidate == key)
    }

    /// Selects keys beginning with `prefix`.
    pub fn prefix(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self::new(move |key| key.starts_with(&prefix))
    }

    /// Selects keys in one durable object family.
    pub fn family(family: DurableObjectFamily) -> Self {
        Self::new(move |key| parse_object_key(key).is_some_and(|parsed| parsed.family() == family))
    }

    /// Selects content-blob keys.
    pub fn content_blob() -> Self {
        Self::family(DurableObjectFamily::ContentBlob)
    }

    /// Selects metadata-table keys.
    pub fn metadata_table() -> Self {
        Self::family(DurableObjectFamily::MetadataTable)
    }

    /// Selects the WAL head key for the given namespace.
    pub fn wal_head(namespace_id: &NamespaceId) -> Self {
        Self::exact(wal_head(namespace_id))
    }

    /// Selects the metadata root key for the given namespace.
    pub fn metadata_root(namespace_id: &NamespaceId) -> Self {
        Self::exact(metadata_root(namespace_id))
    }

    pub(crate) fn matches(&self, key: &str) -> bool {
        (self.predicate)(key)
    }
}

impl fmt::Debug for KeyPredicate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KeyPredicate(..)")
    }
}
