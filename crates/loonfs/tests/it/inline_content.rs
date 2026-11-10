//! Downloads and appends of committed inline content.

use bytes::Bytes;
use loonfs::{AppendFileOptions, LoonFs, SharedObjectStore};
use loonfs_core::publish::{
    CommitCandidate, CommitIntent, FilesystemOperation, InlineContent, NamespaceCommitEngine,
};
use loonfs_objectstore::keys::{content_blob, content_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, ObjectStoreExtension};
use loonfs_test_support::stores::{KeyPredicate, OperationSelector, RecordingStore};
use loonfs_types::{
    AbsolutePath, CommitId, ContentId, ContentRef, DestinationBehavior, NamespaceId, RevisionNo,
};
use std::sync::Arc;

async fn publish_inline(store: &SharedObjectStore, namespace_id: &NamespaceId) -> ContentRef {
    let value = InlineContent::new(
        ContentId::generate(),
        Bytes::from_static(b"inline content"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let request = CommitIntent::single(
        CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/file").expect("path"),
            content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    let writer = loonfs_core::test_support::writer_at("inline-writer", 1_000);
    NamespaceCommitEngine::new(
        loonfs_core::test_support::namespace_store(store, namespace_id),
        writer.writer_id.clone(),
    )
    .publish_batch(
        [CommitCandidate::with_inline_content(
            request,
            Vec::new(),
            vec![value.clone()],
        )],
        &writer.clock(),
    )
    .await
    .results
    .pop()
    .expect("result")
    .expect("publish inline");
    value.content_ref().clone()
}

#[tokio::test]
async fn reader_downloads_write_tail_content_by_path_and_inode() {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("runtime-writer"))
        .metadata_cache(
            loonfs::MetadataCache::builder()
                .max_head_bytes(0)
                .max_wal_tail_bytes(0)
                .build(),
        )
        .build()
        .await
        .expect("writer");
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("reader");
    for by_inode in [false, true] {
        let namespace_id =
            NamespaceId::parse(if by_inode { "inode" } else { "path" }).expect("namespace");
        let namespace = reader.namespace(&namespace_id);
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("namespace");
        let content_ref = publish_inline(&store, &namespace_id).await;
        let inode_id = namespace.stat("/file").await.expect("entry").inode_id;
        recording.reset();
        let object_key = if by_inode {
            namespace
                .create_revision_download_by_inode(inode_id, RevisionNo(1))
                .await
                .expect("inode download")
                .ranges
                .remove(0)
                .object_key
        } else {
            namespace
                .create_download("/file")
                .await
                .expect("path download")
                .ranges
                .remove(0)
                .object_key
        };
        assert_eq!(recording.count(OperationSelector::Put), 1);

        let pool_id = loonfs_core::test_support::load_pool_id(&store, &namespace_id).await;
        assert_eq!(object_key, content_blob(&pool_id, &content_ref.content_id));
        assert_eq!(
            store
                .get(&object_key, None)
                .await
                .expect("get")
                .expect("object")
                .as_ref(),
            b"inline content"
        );
    }
}

/// Checks the creator rule: a fork's first append to an inherited file
/// starts a chain that the fork creates, the fork's next append extends that
/// chain in place, and an append by the fork's source to the fork's chain
/// starts another chain, because the source isn't its creator.
#[tokio::test]
async fn an_append_in_a_fork_starts_a_chain_the_fork_creates() {
    let directory = tempfile::tempdir().expect("directory");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("runtime-writer"))
        .build()
        .await
        .expect("writer");
    let actor = loonfs_test_support::test_actor();
    let source = NamespaceId::parse("source").expect("source");
    let fork = NamespaceId::parse("fork").expect("fork");
    writer
        .create_namespace(&source, &actor)
        .await
        .expect("namespace");
    let source_writer = writer.open_namespace(&source).expect("open source");
    source_writer
        .put_file("/log", b"first", &actor)
        .await
        .expect("put");
    let guard = AppendFileOptions {
        expected_inode_id: Some(source_writer.stat("/log").await.expect("stat").inode_id),
        expected_revision_no: Some(RevisionNo(1)),
        ..AppendFileOptions::default()
    };
    source_writer
        .append_file_with_options("/log", b" second", &actor, &guard)
        .await
        .expect("append in the source");
    writer
        .fork_namespace(&source, &fork, &actor)
        .await
        .expect("fork");
    let fork_writer = writer.open_namespace(&fork).expect("open fork");
    let inherited = fork_writer
        .stat("/log")
        .await
        .expect("inherited entry")
        .content_ref()
        .expect("inherited reference")
        .clone();

    fork_writer
        .append_file("/log", b" third", &actor)
        .await
        .expect("append in the fork");
    let mut fork_chain = None;
    for fold in [false, true] {
        if fold {
            writer
                .maintenance(loonfs_test_support::ids::writer_id("folder"))
                .fold_wal(&fork)
                .await
                .expect("fold the fork");
        }
        let file = fork_writer.read_file("/log").await.expect("read the fork");
        assert_eq!(file.bytes, b"first second third");
        let appended = file.entry.content_ref().expect("appended reference");
        assert_ne!(appended.content_id, inherited.content_id);
        fork_chain = Some(appended.clone());
    }
    let fork_chain = fork_chain.expect("the fork's chain");
    // The fork's fold wrote the new chain's extent under the prefix of the
    // source's pool.
    let pool_id = loonfs_core::test_support::load_pool_id(&store, &source).await;
    assert_eq!(
        loonfs_core::test_support::load_pool_id(&store, &fork).await,
        pool_id
    );
    assert!(store
        .list_prefix(&content_prefix(&pool_id))
        .await
        .expect("list the family's content")
        .iter()
        .any(|key| key.contains(fork_chain.content_id.as_str())));
    fork_writer
        .append_file("/log", b" fourth", &actor)
        .await
        .expect("second append in the fork");
    let extended = fork_writer
        .stat("/log")
        .await
        .expect("fork entry")
        .content_ref()
        .expect("extended reference")
        .clone();
    assert_eq!(extended.content_id, fork_chain.content_id);
    assert_eq!(
        source_writer
            .read_file("/log")
            .await
            .expect("read the source")
            .bytes,
        b"first second"
    );

    // A merge applies the fork's chain to the source without a copy, and the
    // source's append starts a chain that the source creates.
    source_writer
        .merge_fork(&fork, &actor)
        .await
        .expect("merge the fork's chain");
    assert_eq!(
        source_writer
            .stat("/log")
            .await
            .expect("merged entry")
            .content_ref(),
        Some(&extended)
    );
    source_writer
        .append_file("/log", b" fifth", &actor)
        .await
        .expect("append in the source");
    let file = source_writer
        .read_file("/log")
        .await
        .expect("read the source's file");
    assert_eq!(file.bytes, b"first second third fourth fifth");
    let root_chain = file.entry.content_ref().expect("root's chain");
    assert_ne!(root_chain.content_id, extended.content_id);
    assert_eq!(
        fork_writer
            .read_file("/log")
            .await
            .expect("read the fork")
            .bytes,
        b"first second third fourth"
    );
}
