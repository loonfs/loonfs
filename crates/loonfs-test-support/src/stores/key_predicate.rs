//! Reusable key predicates for object-store wrappers.

use loonfs_objectstore::keys::{hint, metadata_manifest_prefix};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_types::NamespaceId;
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

    /// Selects metadata-segment keys.
    pub fn metadata_segment() -> Self {
        Self::family(DurableObjectFamily::MetadataSegment)
    }

    /// Selects the manifest discovery hint key for the given namespace.
    pub fn hint(namespace_id: &NamespaceId) -> Self {
        Self::exact(hint(namespace_id))
    }

    /// Selects every numbered manifest in the namespace.
    pub fn manifest(namespace_id: &NamespaceId) -> Self {
        Self::prefix(metadata_manifest_prefix(namespace_id))
    }

    /// Selects the keys that this predicate selects, except every
    /// namespace's hint.
    ///
    /// A runtime raises a namespace's hint in the background after each
    /// manifest publication, so a test that counts or parks requests leaves
    /// the hint out when the raise's load and swap would race its count.
    pub fn except_hints(self) -> Self {
        let predicate = self.predicate;
        Self::new(move |key| {
            predicate(key)
                && parse_object_key(key)
                    .is_none_or(|parsed| parsed.family() != DurableObjectFamily::Hint)
        })
    }

    pub fn matches(&self, key: &str) -> bool {
        (self.predicate)(key)
    }
}

impl fmt::Debug for KeyPredicate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KeyPredicate(..)")
    }
}
