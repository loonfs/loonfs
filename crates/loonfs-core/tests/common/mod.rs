#![allow(dead_code)]

use loonfs_api::NamespaceId;
use loonfs_core::cache::{
    MetadataTableCache, MetadataTableCacheConfig, WalTailProjectionCache,
    WalTailProjectionCacheConfig, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
use loonfs_core::control::load_namespace_read_anchor;
use loonfs_core::{MutationContext, NamespaceEngine, RuntimeReadContext};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub(crate) fn mutation_context(
    writer_id: &str,
    writer_session_id: &str,
    writer_version: &str,
    now_ms: u64,
) -> MutationContext {
    MutationContext {
        writer_id: writer_id.to_owned(),
        writer_session_id: writer_session_id.to_owned(),
        writer_version: writer_version.to_owned(),
        now_ms,
    }
}

pub(crate) fn namespace_engine<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> NamespaceEngine<&'a S> {
    NamespaceEngine::builder(store)
        .namespace_id(namespace_id.clone())
        .writer_id(context.writer_id.clone())
        .writer_session_id(context.writer_session_id.clone())
        .writer_version(context.writer_version.clone())
        .build()
        .expect("build namespace engine")
}

pub(crate) async fn read_context<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> RuntimeReadContext {
    let (head, root) = load_namespace_read_anchor(store, namespace_id)
        .await
        .expect("load read anchor");
    RuntimeReadContext {
        head: head.state,
        head_etag: head.identity.etag,
        manifest_id: root.state.manifest_id,
        manifest_object_id: root.state.manifest_object_id,
        table_cache: Arc::new(MetadataTableCache::new(MetadataTableCacheConfig::default())),
        tail_cache: Arc::new(WalTailProjectionCache::new(WalTailProjectionCacheConfig {
            max_entries: 4,
            max_rows: DEFAULT_WAL_TAIL_PROJECTION_ROWS,
            max_decoded_bytes: DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
        })),
        catalog: None,
    }
}
