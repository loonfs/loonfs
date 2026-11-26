//! A store that publishes a newer manifest of a namespace before selected
//! operations, as another process does when it publishes manifests while a
//! discovery reads the WAL tail.

use super::{
    Intercept, InterceptStore, Interceptor, KeyPredicate, OperationContext, OperationSelector,
};
use async_trait::async_trait;
use bytes::Bytes;
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_manifest_prefix};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::{ObjectStore, ObjectStoreExtension};
use loonfs_types::format::manifest::{
    decode_namespace_manifest_json, encode_namespace_manifest_json, NamespaceManifestPayload,
};
use loonfs_types::{NamespaceId, PublicationId};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Publishes the next manifest of a namespace before each selected operation
/// while armed (see [`NewerManifestStore`]).
pub struct NewerManifestInterceptor {
    /// The store without this interceptor, which the publications write to.
    rival: Arc<dyn ObjectStore>,
    namespace_id: NamespaceId,
    keys: KeyPredicate,
    operation: OperationSelector,
    armed: AtomicBool,
    /// Stays locked across each publication's requests, because a WAL read
    /// sends GETs concurrently, and two publications at once would put the
    /// same manifest number.
    publishing: tokio::sync::Mutex<()>,
    published: AtomicUsize,
}

impl fmt::Debug for NewerManifestInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewerManifestInterceptor")
            .field("namespace_id", &self.namespace_id)
            .field("operation", &self.operation)
            .field("armed", &self.armed)
            .field("published", &self.published)
            .finish_non_exhaustive()
    }
}

/// A store that publishes the next manifest of a namespace before each
/// selected operation, once a test arms it.
///
/// Each manifest is a copy of the newest one with the next manifest number
/// and a new publication ID (see [`publish_next_manifest`]), so it folds no
/// WAL object, like the manifest of a compaction unit or of a retention
/// window change. The store selects operations with a key predicate and an
/// operation selector.
pub type NewerManifestStore<S> = InterceptStore<S, NewerManifestInterceptor>;

impl<S> InterceptStore<S, NewerManifestInterceptor> {
    /// Wraps `inner`, and publishes through `rival`, a store of the same
    /// objects that this wrapper doesn't intercept.
    pub fn new(
        inner: S,
        rival: Arc<dyn ObjectStore>,
        namespace_id: NamespaceId,
        keys: KeyPredicate,
        operation: OperationSelector,
    ) -> Self {
        Self::with_interceptor(
            inner,
            NewerManifestInterceptor {
                rival,
                namespace_id,
                keys,
                operation,
                armed: AtomicBool::new(false),
                publishing: tokio::sync::Mutex::new(()),
                published: AtomicUsize::new(0),
            },
        )
    }

    /// Starts publishing before each selected operation.
    pub fn arm(&self) {
        self.interceptor().armed.store(true, Ordering::SeqCst);
    }

    /// Returns how many manifests the wrapper has published.
    pub fn published(&self) -> usize {
        self.interceptor().published.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Interceptor for NewerManifestInterceptor {
    type Guard = ();

    async fn before(&self, context: &OperationContext<'_>) -> (Intercept, ()) {
        if self.armed.load(Ordering::SeqCst)
            && self.keys.matches(context.key())
            && self.operation.matches(context.kind())
        {
            let _publishing = self.publishing.lock().await;
            publish_next_manifest(self.rival.as_ref(), &self.namespace_id, |_| {}).await;
            self.published.fetch_add(1, Ordering::SeqCst);
        }
        (Intercept::Continue, ())
    }
}

/// Publishes the manifest after the namespace's newest manifest in `store`,
/// and returns the published payload.
///
/// The published manifest is a copy of the newest one with the next manifest
/// number and a new publication ID, which `change` then edits. The call
/// stands in for another process's publication in a test, so it panics when
/// the namespace has no manifest or when the put finds the number taken.
pub async fn publish_next_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    change: impl FnOnce(&mut NamespaceManifestPayload),
) -> NamespaceManifestPayload {
    let newest = store
        .list_prefix(&metadata_manifest_prefix(namespace_id))
        .await
        .expect("list the namespace's manifests")
        .iter()
        .filter_map(|key| manifest_no_of(key))
        .max()
        .expect("the namespace has a manifest");
    let bytes = store
        .get(&metadata_manifest_object(namespace_id, &newest), None)
        .await
        .expect("get the newest manifest")
        .expect("the listed manifest exists");
    let mut payload = decode_namespace_manifest_json(&bytes)
        .expect("decode the newest manifest")
        .payload()
        .clone();
    payload.manifest_no = newest.successor().expect("a next manifest number");
    payload.publication_id = PublicationId::generate();
    change(&mut payload);
    let key = metadata_manifest_object(namespace_id, &payload.manifest_no);
    let encoded = encode_namespace_manifest_json(payload.clone())
        .expect("encode the next manifest")
        .into_bytes();
    store
        .put_if_absent(&key, Bytes::from(encoded))
        .await
        .expect("publish the next manifest");
    payload
}
