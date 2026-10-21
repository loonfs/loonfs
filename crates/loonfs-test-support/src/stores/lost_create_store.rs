//! A create that loses its race: another writer's object lands at the key
//! just as this writer's first create of it arrives.

use super::KeyPredicate;
use async_trait::async_trait;
use bytes::Bytes;
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use std::sync::atomic::{AtomicBool, Ordering};

/// Writes `winner_bytes` at the first key a create-if-absent put names that
/// `keys` selects, then refuses that put with a precondition failure. Every
/// later operation is forwarded.
#[derive(Debug)]
pub struct LostCreateStore<S> {
    inner: S,
    keys: KeyPredicate,
    winner_bytes: Bytes,
    lost: AtomicBool,
}

impl<S> LostCreateStore<S> {
    /// Loses the first create of a key that `keys` selects to `winner_bytes`.
    pub fn new(inner: S, keys: KeyPredicate, winner_bytes: impl Into<Bytes>) -> Self {
        Self {
            inner,
            keys,
            winner_bytes: winner_bytes.into(),
            lost: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for LostCreateStore<S> {
    crate::delegate_object_store!(self => self.inner; except put);

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if matches!(mode, PutMode::CreateIfAbsent)
            && self.keys.matches(key)
            && !self.lost.swap(true, Ordering::SeqCst)
        {
            self.inner
                .put_if_absent(key, self.winner_bytes.clone())
                .await?;
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        self.inner.put(key, bytes, mode).await
    }
}
