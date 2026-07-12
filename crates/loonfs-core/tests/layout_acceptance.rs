//! Structural acceptance tests for the namespace layout redesign (format
//! spec, "Final design summary"): live visibility comes only from the WAL
//! head, nothing correct depends on listing, and maintenance never touches
//! the head.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::v0::BeginUploadRequest;
use loonfs_api::{ChangeSeq, EffectiveLimit, NamespaceId};
use loonfs_core::cache::{
    MetadataTableCache, MetadataTableCacheConfig, WalTailProjectionCache,
    WalTailProjectionCacheConfig, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::control::load_namespace_read_anchor;
use loonfs_core::gc::{gc_namespace, GcConfig};
use loonfs_core::publish::{NamespaceCommitEngine, NamespaceMutationCandidate, PathMutationIntent};
use loonfs_core::{BootstrapOptions, MutationContext, NamespaceEngine, RuntimeReadContext};
use loonfs_objectstore::fs::LocalFsStore;
use loonfs_objectstore::keys::{wal_head, wal_segment_prefix};
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

async fn put_file<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
) {
    let content = store_bytes_as_content(store, namespace_id, bytes)
        .await
        .expect("stage content");
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![NamespaceMutationCandidate::Path(
                PathMutationIntent::PutFile {
                    commit_id: loonfs_api::CommitId::generate(),
                    absolute_path: absolute_path.to_owned(),
                    content_ref: content.content_ref,
                    behavior: loonfs_api::PutBehavior::NoReplace,
                },
            )],
            context,
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("put file");
}

async fn read_context<S: ObjectStore + ?Sized>(
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

fn page_limit() -> EffectiveLimit {
    EffectiveLimit::new(std::num::NonZeroU32::new(1024).expect("nonzero limit"))
}

fn context() -> MutationContext {
    MutationContext {
        writer_id: "acceptance".to_owned(),
        writer_session_id: "wrs_acceptance".to_owned(),
        writer_version: "acceptance/0.1.0".to_owned(),
        now_ms: 1_000,
    }
}

fn engine<'a, S: ObjectStore + ?Sized>(
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
        .expect("build engine")
}

/// Counts every LIST issued through the store.
#[derive(Debug)]
struct ListCountingStore {
    inner: LocalFsStore,
    lists: AtomicUsize,
}

impl ListCountingStore {
    fn list_count(&self) -> usize {
        self.lists.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStore for ListCountingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list_prefix_stream(prefix)
    }
}

/// Nothing correct depends on listing: reads, commits, and the change feed
/// complete without a single LIST, even with junk parked in the segment
/// collection (chain traversal never sees it).
#[tokio::test]
async fn reads_commits_and_change_feed_never_list() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ListCountingStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("store"),
        lists: AtomicUsize::new(0),
    };
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = context();
    let engine = engine(&store, &namespace_id, &context);
    engine
        .bootstrap_namespace(BootstrapOptions::default())
        .await
        .expect("bootstrap");

    // Foreign junk in the segment collection must be invisible to every
    // hot path.
    store
        .put_if_absent(
            &format!("{}zz-junk.tmp", wal_segment_prefix(namespace_id.as_str())),
            Bytes::from_static(b"junk"),
        )
        .await
        .expect("write junk");

    let baseline = store.list_count();
    let staged = engine
        .begin_upload(BeginUploadRequest::default())
        .await
        .expect("begin upload");
    let uploaded = engine
        .upload_content(&staged.upload_id, b"uploaded\n")
        .await
        .expect("upload content");
    engine
        .complete_upload(
            &staged.upload_id,
            &loonfs_api::v0::CompleteUploadRequest {
                content_ref: uploaded.content_ref,
            },
        )
        .await
        .expect("complete upload");
    put_file(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
    )
    .await;

    let ctx = read_context(&store, &namespace_id).await;
    engine
        .resolve_path_with_runtime_context("/docs/hello.txt", &ctx)
        .await
        .expect("stat");
    engine
        .list_path_page_with_runtime_context(
            "/docs",
            loonfs_api::PageRequest {
                limit: page_limit(),
                cursor: None,
            },
            &ctx,
        )
        .await
        .expect("list directory");
    let bytes = engine
        .read_file_with_runtime_context("/docs/hello.txt", &ctx)
        .await
        .expect("read");
    assert_eq!(bytes.bytes, b"hello\n");
    let changes = engine
        .list_changes_after(ChangeSeq(0), page_limit())
        .await
        .expect("change feed");
    assert!(!changes.changes.is_empty());

    assert_eq!(
        store.list_count(),
        baseline,
        "read, commit, upload, and change-feed paths must not LIST"
    );
}

/// The head changes only when commits land: checkpoint creation, root
/// publication, floor advancement, garbage collection, and the upload
/// workflow leave `wal/head.json` byte-identical.
#[tokio::test]
async fn maintenance_never_touches_the_wal_head() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = context();
    let engine = engine(&store, &namespace_id, &context);
    engine
        .bootstrap_namespace(BootstrapOptions::default())
        .await
        .expect("bootstrap");
    put_file(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
    )
    .await;

    let head_key = wal_head(namespace_id.as_str());
    let before = store
        .get_with_metadata(&head_key)
        .await
        .expect("read head")
        .expect("head exists");

    engine
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("checkpoint");
    engine
        .advance_retention_floor()
        .await
        .expect("advance floor");
    gc_namespace(&store, &namespace_id, &GcConfig::default(), &context)
        .await
        .expect("gc pass");
    let staged = engine
        .begin_upload(BeginUploadRequest::default())
        .await
        .expect("second upload");
    let uploaded = engine
        .upload_content(&staged.upload_id, b"more\n")
        .await
        .expect("second upload content");
    engine
        .complete_upload(
            &staged.upload_id,
            &loonfs_api::v0::CompleteUploadRequest {
                content_ref: uploaded.content_ref,
            },
        )
        .await
        .expect("second upload complete");

    let after = store
        .get_with_metadata(&head_key)
        .await
        .expect("read head")
        .expect("head exists");
    assert_eq!(after.bytes, before.bytes, "head bytes must be unchanged");
    assert_eq!(
        after.metadata.etag, before.metadata.etag,
        "head object identity must be unchanged"
    );
}
