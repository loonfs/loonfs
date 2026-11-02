//! Downloads, imports, and appends of committed inline content.

use bytes::Bytes;
use loonfs::{
    AppendFileOptions, CreateNamespaceOptions, LoonFs, PutFileOptions, SharedObjectStore,
};
use loonfs_core::content::DurableContentValidationError;
use loonfs_core::publish::{
    CommitCandidate, CommitIntent, FilesystemOperation, InlineContent, NamespaceCommitEngine,
};
use loonfs_core::{CoreError, NamespaceEngine};
use loonfs_objectstore::keys::{content_blob, content_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, ObjectStoreExtension};
use loonfs_test_support::stores::{
    KeyPredicate, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::{
    AbsolutePath, AccessGrants, AccessRight, Checksum, CommitId, ContentId, ContentRef,
    DestinationBehavior, ErrorCode, NamespaceAccess, NamespaceId, PrincipalId, PrincipalScope,
    PrincipalSet, RevisionNo, Subject, SubjectId, WriterId,
};
use std::assert_matches;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

async fn publish_inline(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    subject: Option<Subject>,
) -> ContentRef {
    let value = InlineContent::new(
        ContentId::generate(),
        Bytes::from_static(b"inline content"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let mut request = CommitIntent::single(
        CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/file").expect("path"),
            content: loonfs_types::FileContent::Ref {
                content_ref: value.content_ref().clone(),
                source_namespace_id: None,
            },
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    request.subject = subject;
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
        let content_ref = publish_inline(&store, &namespace_id, None).await;
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
                        AccessRight::Admin.into(),
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
                loonfs_core::test_support::namespace_store(store.clone(), &source),
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
                &source,
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
                0
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
        assert_ne!(imported_ref.content_id, content_ref.content_id);
    }
}

/// Checks that an import within a namespace keeps the reference, because a
/// namespace is in its own fork family, and reads none of the content.
#[tokio::test]
async fn a_same_namespace_import_keeps_the_reference_and_reads_no_content() {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("runtime-writer"))
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
            &namespace_id,
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("import before folding");
    assert_eq!(recording.count(OperationSelector::Head), 0);
    assert_eq!(recording.count(OperationSelector::Read), 0);
    let file = namespace.read_file("/file").await.expect("imported bytes");
    assert_eq!(file.bytes, b"inline content");
    assert_eq!(file.entry.revision_no(), Some(RevisionNo(2)));
    assert_eq!(file.entry.content_ref(), Some(&content_ref));
}

/// Checks that an import of a fork's content into another fork family reads
/// the object under the prefix of the fork's family root, after the family
/// root is deleted.
#[tokio::test]
async fn imports_of_fork_content_read_the_deleted_family_roots_key() {
    for inline in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let recording = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::content_blob(),
        ));
        let store: SharedObjectStore = recording.clone();
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id(loonfs_test_support::ids::writer_id("runtime-writer"))
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

        let source_key = content_blob(&source, &content_ref.content_id);
        recording.reset();
        destination_writer
            .put_file_content_ref(
                "/imported",
                content_ref.clone(),
                &fork,
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("import after the family root's deletion");
        assert_eq!(recording.take_get_keys(), vec![source_key]);
        let file = destination_namespace
            .read_file("/imported")
            .await
            .expect("imported bytes");
        assert_eq!(file.bytes, b"inline content");
        let imported_ref = file.entry.content_ref().expect("imported reference");
        assert_ne!(imported_ref.content_id, content_ref.content_id);
    }
}

/// Checks the creator rule: a fork's first append to an inherited file
/// starts a chain that the fork creates, the fork's next append extends that
/// chain in place, and an append by the family root to the fork's chain
/// starts another chain, because the family root isn't its creator.
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
    // The fork's fold wrote the new chain's extent under the family root's
    // prefix, and the fork's own content prefix stays empty.
    assert!(store
        .list_prefix(&content_prefix(&fork))
        .await
        .expect("list the fork's content")
        .is_empty());
    assert!(store
        .list_prefix(&content_prefix(&source))
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

    // The family root refers to the fork's chain without a copy, and its
    // append starts a chain that the family root creates.
    source_writer
        .put_file_content_ref("/copied", extended.clone(), &fork, &actor)
        .await
        .expect("import the fork's chain");
    source_writer
        .append_file("/copied", b" fifth", &actor)
        .await
        .expect("append in the family root");
    let file = source_writer
        .read_file("/copied")
        .await
        .expect("read the family root's copy");
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

/// Checks that an import from a member of the same fork family refers to the
/// same content and writes no content object, and that an import from
/// another fork family copies the bytes under the destination family root's
/// prefix.
#[tokio::test]
async fn an_import_within_a_fork_family_writes_no_content_object() {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("runtime-writer"))
        .build()
        .await
        .expect("writer");
    let actor = loonfs_test_support::test_actor();
    let source = NamespaceId::parse("source").expect("source");
    let fork = NamespaceId::parse("fork").expect("fork");
    let other = NamespaceId::parse("other").expect("other");
    for namespace_id in [&source, &other] {
        writer
            .create_namespace(namespace_id, &actor)
            .await
            .expect("namespace");
    }
    writer
        .open_namespace(&source)
        .expect("open source")
        .put_file("/file", b"family bytes", &actor)
        .await
        .expect("put");
    writer
        .fork_namespace(&source, &fork, &actor)
        .await
        .expect("fork");
    let fork_writer = writer.open_namespace(&fork).expect("open fork");
    let content_ref = fork_writer
        .stat("/file")
        .await
        .expect("inherited entry")
        .content_ref()
        .expect("inherited reference")
        .clone();
    let family_objects = store
        .list_prefix(&content_prefix(&source))
        .await
        .expect("family content");

    recording.reset();
    fork_writer
        .put_file_content_ref("/imported", content_ref.clone(), &source, &actor)
        .await
        .expect("import within the family");
    assert_eq!(recording.count(OperationSelector::Put), 0);
    let imported = fork_writer
        .read_file("/imported")
        .await
        .expect("imported bytes");
    assert_eq!(imported.bytes, b"family bytes");
    assert_eq!(imported.entry.content_ref(), Some(&content_ref));
    assert_eq!(
        store
            .list_prefix(&content_prefix(&source))
            .await
            .expect("family content"),
        family_objects
    );

    let other_writer = writer.open_namespace(&other).expect("open other");
    other_writer
        .put_file_content_ref("/imported", content_ref.clone(), &fork, &actor)
        .await
        .expect("import from another family");
    let copied = other_writer
        .read_file("/imported")
        .await
        .expect("copied bytes");
    assert_eq!(copied.bytes, b"family bytes");
    let copied_ref = copied.entry.content_ref().expect("copied reference");
    assert_ne!(copied_ref.content_id, content_ref.content_id);
    assert!(store
        .list_prefix(&content_prefix(&other))
        .await
        .expect("other content")
        .iter()
        .any(|key| key.contains(copied_ref.content_id.as_str())));
}

#[tokio::test]
async fn an_import_within_a_fork_family_verifies_the_reference_against_its_bytes() {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("runtime-writer"))
        .build()
        .await
        .expect("writer");
    let actor = loonfs_test_support::test_actor();
    let source = NamespaceId::parse("source").expect("source");
    let fork = NamespaceId::parse("fork").expect("fork");
    let other = NamespaceId::parse("other").expect("other");
    for namespace_id in [&source, &other] {
        writer
            .create_namespace(namespace_id, &actor)
            .await
            .expect("namespace");
    }
    writer
        .open_namespace(&source)
        .expect("open source")
        .put_file("/file", b"family bytes", &actor)
        .await
        .expect("put");
    writer
        .fork_namespace(&source, &fork, &actor)
        .await
        .expect("fork");
    let fork_writer = writer.open_namespace(&fork).expect("open fork");
    let content_ref = fork_writer
        .stat("/file")
        .await
        .expect("inherited entry")
        .content_ref()
        .expect("inherited reference")
        .clone();

    let algorithm = content_ref.checksum.algorithm();
    let altered = [
        ContentRef {
            checksum: Checksum::compute(algorithm, b"family BYTES"),
            ..content_ref.clone()
        },
        ContentRef {
            size_bytes: content_ref.size_bytes - 1,
            ..content_ref.clone()
        },
        ContentRef {
            size_bytes: content_ref.size_bytes + 1,
            ..content_ref.clone()
        },
    ];
    // The import into `other` copies the bytes, and the import into the fork
    // fails the same way without copying them.
    let other_writer = writer.open_namespace(&other).expect("open other");
    for altered in altered {
        let mut errors = Vec::new();
        for destination in [&fork_writer, &other_writer] {
            let error = destination
                .put_file_content_ref("/imported", altered.clone(), &source, &actor)
                .await
                .expect_err("an altered reference");
            assert_matches!(
                &error,
                loonfs::Error::Core(CoreError::DurableContent(
                    DurableContentValidationError::ContentChecksumMismatch { .. }
                        | DurableContentValidationError::ContentLengthMismatch { .. }
                )),
                "{altered:?}"
            );
            let missing = destination
                .stat("/imported")
                .await
                .expect_err("no revision of an altered reference");
            assert_eq!(missing.code(), ErrorCode::PathNotFound);
            errors.push(error.to_string());
        }
        assert_eq!(errors[0], errors[1]);
    }
    recording.reset();
    fork_writer
        .put_file_content_ref("/imported", content_ref.clone(), &source, &actor)
        .await
        .expect("import within the family");
    assert_eq!(recording.count(OperationSelector::Put), 0);
    assert_eq!(
        fork_writer
            .read_file("/imported")
            .await
            .expect("imported bytes")
            .bytes,
        b"family bytes"
    );
}
