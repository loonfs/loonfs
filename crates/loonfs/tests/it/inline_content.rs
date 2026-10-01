//! Downloads and imports of committed inline content.

use bytes::Bytes;
use loonfs::{CreateNamespaceOptions, LoonFs, PutFileOptions, SharedObjectStore};
use loonfs_core::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, InlineContent, NamespaceCommitEngine,
};
use loonfs_core::time::Deadline;
use loonfs_core::{MutationContext, NamespaceEngine};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{
    KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use loonfs_types::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, CommitId, ContentId, ContentRef,
    DestinationBehavior, NamespaceAccess, NamespaceId, PrincipalId, PrincipalScope, PrincipalSet,
    RevisionNo, Subject, SubjectId, WriterId,
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
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
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
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("runtime-writer")
        .metadata_cache(
            loonfs::MetadataCache::builder()
                .max_head_state_bytes(0)
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
        let content_ref = publish_inline(&store, &namespace_id, None).await;
        let inode_id = namespace.stat("/file").await.expect("entry").inode_id;
        recording.reset();
        let object_key = if by_inode {
            namespace
                .create_download_by_inode(inode_id, RevisionNo(1))
                .await
                .expect("inode download")
                .object_key
        } else {
            namespace
                .create_download("/file", None)
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
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("runtime-writer")
        .metadata_cache(
            loonfs::MetadataCache::builder()
                .max_head_state_bytes(0)
                .build(),
        )
        .build()
        .await
        .expect("writer");
    let source = NamespaceId::parse("source").expect("source");
    let destination = NamespaceId::parse("destination").expect("destination");
    let namespace = writer.namespace(&destination);
    let principal = PrincipalId::parse("owner").expect("principal");
    let subject = Subject {
        principal_scope: PrincipalScope::parse("scope").expect("scope"),
        subject_id: SubjectId::parse("owner").expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([principal.clone()])).expect("principals"),
    };
    writer
        .create_namespace_with_options(
            &source,
            &loonfs_test_support::test_actor(),
            &CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("scope").expect("scope"),
                    root_grants: AccessGrants::new(BTreeMap::from([(
                        principal,
                        AccessRights::from_iter([AccessRight::Admin]),
                    )]))
                    .expect("grants"),
                },
                ..Default::default()
            },
        )
        .await
        .expect("source namespace");
    writer
        .create_namespace(&destination, &loonfs_test_support::test_actor())
        .await
        .expect("destination namespace");
    let namespace_writer = writer.open_namespace(&destination).expect("open namespace");
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
            .fold_wal()
            .await
            .expect("fold source");
        }
        recording.reset();
        let path = if folded { "/folded" } else { "/tail" };
        namespace_writer
            .put_file_content_ref(
                path,
                content_ref.clone(),
                &loonfs_test_support::test_actor(),
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
        let file = namespace.read_file(path).await.expect("imported bytes");
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
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("runtime-writer")
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse("same-namespace").expect("namespace");
    let namespace = writer.namespace(&namespace_id);
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let content_ref = publish_inline(&store, &namespace_id, None).await;
    recording.reset();
    namespace_writer
        .put_file_content_ref_with_options(
            "/file",
            content_ref.clone(),
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("import before folding");
    assert_eq!(recording.count(OperationClass::Head), 0);
    assert_eq!(recording.count(OperationClass::Read), 0);
    let file = namespace.read_file("/file").await.expect("imported bytes");
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
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id("runtime-writer")
            .build()
            .await
            .expect("writer");
        let source = NamespaceId::parse("source").expect("source");
        let source_writer = writer.open_namespace(&source).expect("open namespace");
        let fork = NamespaceId::parse("fork").expect("fork");
        let fork_namespace = writer.namespace(&fork);
        let destination = NamespaceId::parse("destination").expect("destination");
        let destination_namespace = writer.namespace(&destination);
        for namespace_id in [&source, &destination] {
            writer
                .create_namespace(namespace_id, &loonfs_test_support::test_actor())
                .await
                .expect("namespace");
        }
        if inline {
            publish_inline(&store, &source, None).await;
        } else {
            source_writer
                .put_file(
                    "/file",
                    b"inline content",
                    &loonfs_test_support::test_actor(),
                )
                .await
                .expect("publish object");
        }
        writer
            .fork_namespace(&source, &fork, &loonfs_test_support::test_actor())
            .await
            .expect("fork");
        let destination_writer = writer.open_namespace(&destination).expect("open namespace");
        source_writer.delete().await.expect("delete source");
        let entry = fork_namespace.stat("/file").await.expect("fork entry");
        let content_ref = entry.content_ref().expect("fork reference");
        assert_eq!(content_ref.owner_namespace_id, source);

        let source_key = content_blob(&source, &content_ref.content_id);
        recording.reset();
        destination_writer
            .put_file_content_ref(
                "/imported",
                content_ref.clone(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("import after owner deletion");
        assert_eq!(recording.take_get_keys(), vec![source_key]);
        let file = destination_namespace
            .read_file("/imported")
            .await
            .expect("imported bytes");
        assert_eq!(file.bytes, b"inline content");
        let imported_ref = file.entry.content_ref().expect("imported reference");
        assert_eq!(imported_ref.owner_namespace_id, destination);
        assert_ne!(imported_ref.content_id, content_ref.content_id);
    }
}
