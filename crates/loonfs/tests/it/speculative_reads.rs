//! Pins the buffered-read contract behind speculative small reads: while the
//! validated view is the cached one, a read resolves its path once, and a
//! replaced file is never served from the reference the cached view named.

use loonfs::{
    CreateNamespaceOptions, DestinationBehavior, FsMaintenance, FsReader, FsWriter,
    MetadataSegmentCacheConfig, NamespaceId, PutFileOptions, RuntimeCacheConfig,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{OperationClass, RecordingStore};
use std::sync::Arc;
use tempfile::tempdir;

const PATH: &str = "/docs/small.txt";

async fn writer_with_file(
    store: &loonfs::SharedObjectStore,
    namespace_id: &NamespaceId,
) -> FsWriter {
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("speculative-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(
            namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            namespace_id,
            PATH,
            b"first",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write file");
    writer
}

/// A reader whose metadata segments are never cached, so every path
/// resolution shows up as segment GETs.
async fn uncached_segment_reader(
    store: &loonfs::SharedObjectStore,
    max_cached_namespaces: usize,
) -> FsReader {
    FsReader::builder_with_store(store.clone())
        .runtime_cache(RuntimeCacheConfig {
            max_cached_namespaces,
            metadata_segment_cache: MetadataSegmentCacheConfig {
                max_decoded_bytes: 0,
            },
            ..RuntimeCacheConfig::default()
        })
        .build()
        .await
        .expect("build reader")
}

#[tokio::test]
async fn an_unchanged_view_resolves_the_path_once() {
    let temp_dir = tempdir().expect("tempdir");
    let log = Arc::new(RecordingStore::metadata_segments(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
    ));
    let store: loonfs::SharedObjectStore = log.clone();
    let namespace_id = NamespaceId::parse("speculative").expect("valid namespace id");
    let _writer = writer_with_file(&store, &namespace_id).await;
    FsMaintenance::builder_with_store(store.clone())
        .actor_id("speculative-maintenance")
        .build()
        .await
        .expect("build maintenance")
        .flush_wal(&namespace_id)
        .await
        .expect("move the file's rows into metadata segments");

    // Without a cached namespace there is no candidate: one ordinary resolution.
    let ordinary = uncached_segment_reader(&store, 0).await;
    log.reset();
    ordinary
        .get_file_bytes(&namespace_id, PATH)
        .await
        .expect("ordinary read");
    let one_resolution = log.count(OperationClass::Get);
    assert!(
        one_resolution > 0,
        "resolution should read metadata segments"
    );

    let speculative = uncached_segment_reader(&store, 8).await;
    speculative
        .get_file_bytes(&namespace_id, PATH)
        .await
        .expect("read that caches the namespace");
    log.reset();
    let read = speculative
        .get_file_bytes(&namespace_id, PATH)
        .await
        .expect("speculative read");
    assert_eq!(read.bytes, b"first");
    assert_eq!(
        log.count(OperationClass::Get),
        one_resolution,
        "an unchanged view must not resolve the path a second time"
    );
}

#[tokio::test]
async fn a_replaced_file_is_not_served_from_the_cached_reference() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let namespace_id = NamespaceId::parse("speculative").expect("valid namespace id");
    let writer = writer_with_file(&store, &namespace_id).await;
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let first = reader
        .get_file_bytes(&namespace_id, PATH)
        .await
        .expect("read that caches the namespace");
    assert_eq!(first.bytes, b"first");

    let mut replace = PutFileOptions::new(loonfs_test_support::test_actor());
    replace.behavior = DestinationBehavior::Replace;
    writer
        .put_file_bytes(&namespace_id, PATH, b"again", replace)
        .await
        .expect("replace file");

    let replaced = reader
        .get_file_bytes(&namespace_id, PATH)
        .await
        .expect("read after replacement");
    assert_eq!(replaced.bytes, b"again");
    let settled = reader
        .get_file_bytes(&namespace_id, PATH)
        .await
        .expect("read from the advanced view");
    assert_eq!(settled.bytes, b"again");
}

#[tokio::test]
async fn buffered_inline_reads_request_no_content_object_on_either_branch() {
    use bytes::Bytes;
    use loonfs_api::{AbsolutePath, CommitId, ContentId, WriterId};
    use loonfs_core::publish::{
        CommitCandidate, CommitRequest, FilesystemOperation, InlineContent, NamespaceCommitEngine,
        PublishTailOptions,
    };
    use loonfs_core::{BootstrapOptions, MutationContext, NamespaceEngine};
    use loonfs_test_support::stores::KeyPredicate;

    let directory = tempdir().expect("directory");
    let log = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: loonfs::SharedObjectStore = log.clone();
    let namespace_id = NamespaceId::parse("inline-reads").expect("namespace");
    let writer_id = WriterId::parse("inline-writer").expect("writer");
    NamespaceEngine::writer(store.clone(), namespace_id.clone(), writer_id.clone())
        .bootstrap_namespace(BootstrapOptions::new(loonfs_test_support::test_actor()))
        .await
        .expect("bootstrap");
    let values: Vec<_> = [
        Bytes::new(),
        Bytes::from_static(b"small inline file"),
        Bytes::from(vec![42; 65 * 1024]),
    ]
    .into_iter()
    .map(|bytes| InlineContent::new(namespace_id.clone(), ContentId::generate(), bytes))
    .collect();
    let candidate = CommitCandidate::with_inline_content(
        CommitRequest {
            commit_id: CommitId::parse("inline").expect("commit"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            preconditions: Vec::new(),
            operations: values
                .iter()
                .enumerate()
                .map(|(index, value)| FilesystemOperation::PutFile {
                    path: AbsolutePath::parse(format!("/file-{index}")).expect("path"),
                    content_ref: value.content_ref().clone(),
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                })
                .collect(),
        },
        Vec::new(),
        values.clone(),
    );
    let result = NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            &store,
            [candidate],
            &MutationContext {
                writer_id,
                now_ms: 1_000,
            },
            &PublishTailOptions::default(),
        )
        .await;
    assert!(result.results[0].is_ok());
    for max_cached_namespaces in [0, 8] {
        let reader = uncached_segment_reader(&store, max_cached_namespaces).await;
        for (index, value) in values.iter().enumerate() {
            for _ in 0..2 {
                log.reset();
                let read = reader
                    .get_file_bytes(&namespace_id, &format!("/file-{index}"))
                    .await
                    .expect("inline read");
                assert_eq!(read.bytes, value.bytes().as_ref());
                assert!(
                    log.snapshot().is_empty(),
                    "{max_cached_namespaces} cached namespaces requested content"
                );
            }
        }
    }
}
