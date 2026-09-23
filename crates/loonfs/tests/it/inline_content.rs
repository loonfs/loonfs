//! Downloads and imports of committed inline content.

use bytes::Bytes;
use loonfs::{
    CreateNamespaceOptions, DeleteNamespaceOptions, ForkNamespaceOptions, FsReader, FsWriter,
    PutFileOptions, SharedObjectStore,
};
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, CommitId, ContentId, ContentRef,
    DestinationBehavior, NamespaceAccess, NamespaceId, PrincipalId, PrincipalScope, PrincipalSet,
    RevisionNo, Subject, SubjectId, WriterId,
};
use loonfs_core::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, InlineContent, NamespaceCommitEngine,
    PublishTailOptions,
};
use loonfs_core::{MutationContext, NamespaceEngine};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{
    KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

async fn publish_inline(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    subject: Option<Subject>,
) -> ContentRef {
    let value = InlineContent::new(
        namespace_id.clone(),
        ContentId::generate(),
        Bytes::from_static(b"inline content"),
    );
    let mut request = CommitRequest::single(
        CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/file").expect("path"),
            content_ref: Some(value.content_ref().clone()),
            inline_content: None,
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    request.subject = subject;
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            [CommitCandidate::with_inline_content(
                request,
                Vec::new(),
                vec![value.clone()],
            )],
            &MutationContext {
                writer_id: WriterId::parse("inline-writer").expect("writer"),
                now_ms: 1_000,
            },
            &PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("result")
        .expect("publish inline");
    value.content_ref().clone()
}

#[tokio::test]
async fn reader_downloads_materialize_tail_content_by_path_and_inode() {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("runtime-writer")
        .runtime_cache(loonfs::RuntimeCacheConfig {
            max_cached_namespaces: 0,
            ..Default::default()
        })
        .build()
        .await
        .expect("writer");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("reader");
    for by_inode in [false, true] {
        let namespace_id =
            NamespaceId::parse(if by_inode { "inode" } else { "path" }).expect("namespace");
        writer
            .create_namespace(
                &namespace_id,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("namespace");
        let content_ref = publish_inline(&store, &namespace_id, None).await;
        let inode_id = reader
            .get_path_entry(&namespace_id, "/file", Default::default())
            .await
            .expect("entry")
            .inode_id;
        recording.reset();
        let object_key = if by_inode {
            reader
                .create_download_by_inode(&namespace_id, inode_id, RevisionNo(1))
                .await
                .expect("inode download")
                .object_key
        } else {
            reader
                .create_download(&namespace_id, "/file", None)
                .await
                .expect("path download")
                .object_key
        };
        assert_eq!(recording.count(OperationClass::Put), 1);

        assert_eq!(
            object_key,
            content_blob(&namespace_id, &content_ref.content_id)
        );
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

#[tokio::test]
async fn imports_read_the_owners_tail_before_folding_and_object_after_folding() {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("runtime-writer")
        .runtime_cache(loonfs::RuntimeCacheConfig {
            max_cached_namespaces: 0,
            ..Default::default()
        })
        .build()
        .await
        .expect("writer");
    let source = NamespaceId::parse("source").expect("source");
    let destination = NamespaceId::parse("destination").expect("destination");
    let principal = PrincipalId::parse("owner").expect("principal");
    let subject = Subject {
        principal_scope: PrincipalScope::parse("scope").expect("scope"),
        subject_id: SubjectId::parse("owner").expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([principal.clone()])).expect("principals"),
    };
    writer
        .create_namespace(
            &source,
            CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("scope").expect("scope"),
                    root_grants: AccessGrants::new(BTreeMap::from([(
                        principal,
                        AccessRights::from_iter([AccessRight::Admin]),
                    )]))
                    .expect("grants"),
                },
                ..CreateNamespaceOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("source namespace");
    writer
        .create_namespace(
            &destination,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("destination namespace");
    let content_ref = publish_inline(&store, &source, Some(subject)).await;

    let source_key = content_blob(&source, &content_ref.content_id);
    assert!(store.head(&source_key).await.expect("head").is_none());
    for folded in [false, true] {
        if folded {
            NamespaceEngine::writer(
                store.clone(),
                source.clone(),
                WriterId::parse("fold").expect("writer"),
            )
            .flush_wal()
            .await
            .expect("flush source");
        }
        recording.reset();
        let path = if folded { "/folded" } else { "/tail" };
        writer
            .put_file_content_ref(
                &destination,
                path,
                content_ref.clone(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("import without source administrator rights");
        let source_operations: Vec<_> = recording
            .snapshot()
            .into_iter()
            .filter(|operation| operation.key() == source_key)
            .collect();
        if folded {
            assert_eq!(
                source_operations
                    .iter()
                    .filter(|operation| matches!(operation, RecordedOperation::Head { .. }))
                    .count(),
                1
            );
            assert_eq!(
                source_operations
                    .iter()
                    .filter(|operation| matches!(
                        operation,
                        RecordedOperation::Get { .. } | RecordedOperation::GetWithMetadata { .. }
                    ))
                    .count(),
                1
            );
            assert!(!source_operations.iter().any(|operation| matches!(
                operation,
                RecordedOperation::Put { .. } | RecordedOperation::PutStreamed { .. }
            )));
        } else {
            assert!(source_operations.is_empty());
        }
        let file = writer
            .reader()
            .get_file_bytes(&destination, path)
            .await
            .expect("imported bytes");
        assert_eq!(file.bytes, b"inline content");
        let imported_ref = file.entry.content_ref().expect("reference");
        assert_eq!(imported_ref.owner_namespace_id, destination);
        assert_ne!(imported_ref.content_id, content_ref.content_id);
    }
}

#[tokio::test]
async fn same_namespace_imports_read_inline_bytes_without_a_content_request() {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("runtime-writer")
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse("same-namespace").expect("namespace");
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    let content_ref = publish_inline(&store, &namespace_id, None).await;
    recording.reset();
    writer
        .put_file_content_ref(
            &namespace_id,
            "/file",
            content_ref.clone(),
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("import before folding");
    assert_eq!(recording.count(OperationClass::Head), 0);
    assert_eq!(recording.count(OperationClass::Read), 0);
    let file = writer
        .reader()
        .get_file_bytes(&namespace_id, "/file")
        .await
        .expect("imported bytes");
    assert_eq!(file.bytes, b"inline content");
    assert_eq!(file.entry.revision_no(), Some(RevisionNo(2)));
    let imported_ref = file.entry.content_ref().expect("reference");
    assert_eq!(imported_ref.owner_namespace_id, namespace_id);
    assert_ne!(imported_ref.content_id, content_ref.content_id);
}

#[tokio::test]
async fn imports_of_fork_content_read_the_deleted_owners_key() {
    for inline in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let recording = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::content_blob(),
        ));
        let store: SharedObjectStore = recording.clone();
        let writer = FsWriter::builder_with_store(store.clone())
            .writer_id("runtime-writer")
            .build()
            .await
            .expect("writer");
        let source = NamespaceId::parse("source").expect("source");
        let fork = NamespaceId::parse("fork").expect("fork");
        let destination = NamespaceId::parse("destination").expect("destination");
        for namespace_id in [&source, &destination] {
            writer
                .create_namespace(
                    namespace_id,
                    CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .expect("namespace");
        }
        if inline {
            publish_inline(&store, &source, None).await;
        } else {
            writer
                .put_file_bytes(
                    &source,
                    "/file",
                    b"inline content",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
                .expect("publish object");
        }
        writer
            .fork_namespace(
                &source,
                &fork,
                ForkNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("fork");
        writer
            .delete_namespace(&source, DeleteNamespaceOptions::default())
            .await
            .expect("delete source");
        let entry = writer
            .reader()
            .get_path_entry(&fork, "/file", Default::default())
            .await
            .expect("fork entry");
        let content_ref = entry.content_ref().expect("fork reference");
        assert_eq!(content_ref.owner_namespace_id, source);

        let source_key = content_blob(&source, &content_ref.content_id);
        recording.reset();
        writer
            .put_file_content_ref(
                &destination,
                "/imported",
                content_ref.clone(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("import after owner deletion");
        assert_eq!(recording.take_get_keys(), vec![source_key]);
        let file = writer
            .reader()
            .get_file_bytes(&destination, "/imported")
            .await
            .expect("imported bytes");
        assert_eq!(file.bytes, b"inline content");
        let imported_ref = file.entry.content_ref().expect("imported reference");
        assert_eq!(imported_ref.owner_namespace_id, destination);
        assert_ne!(imported_ref.content_id, content_ref.content_id);
    }
}
