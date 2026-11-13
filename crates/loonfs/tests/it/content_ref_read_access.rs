//! Authorization for embedded content reference reads.

use loonfs::test_support::writer;
use loonfs::{CreateNamespaceOptions, LoonFs, PutFileOptions, SharedObjectStore, Writable};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, OperationSelector, RecordingStore};
use loonfs_types::{
    AccessGrants, AccessRight, ContentRef, ErrorCode, NamespaceAccess, NamespaceId, PrincipalId,
    PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn subject(principal: &str) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(principal).expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals"),
    }
}

fn acl(administrator: &str) -> NamespaceAccess {
    NamespaceAccess::Acl {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        root_grants: administrator_grants(administrator),
    }
}

fn administrator_grants(administrator: &str) -> AccessGrants {
    AccessGrants::new(BTreeMap::from([(
        PrincipalId::parse(administrator).expect("principal"),
        AccessRight::Admin.into(),
    )]))
    .expect("grants")
}

async fn open_writer() -> (
    tempfile::TempDir,
    Arc<RecordingStore<LocalFsStore>>,
    LoonFs<Writable>,
) {
    let directory = tempfile::tempdir().expect("directory");
    // The runtime raises the hint in the background after each publication,
    // so the recording leaves the hint out.
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any().except_hints(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = writer(store, "content-ref-read-access").await;
    (directory, recording, writer)
}

async fn create_namespace(
    writer: &LoonFs<Writable>,
    namespace_id: &NamespaceId,
    access: NamespaceAccess,
) {
    writer
        .create_namespace_with_options(
            namespace_id,
            &loonfs_test_support::test_actor(),
            &CreateNamespaceOptions {
                access,
                ..Default::default()
            },
        )
        .await
        .expect("namespace");
}

async fn publish_inline(writer: &LoonFs<Writable>, namespace_id: &NamespaceId) -> ContentRef {
    let namespace = writer
        .read_only()
        .with_subject(subject("administrator"))
        .namespace(namespace_id);
    let namespace_writer = writer.open_namespace(namespace_id).expect("open namespace");
    let options = PutFileOptions::default();
    namespace_writer
        .with_subject(subject("administrator"))
        .put_file_with_options(
            "/source",
            b"private inline bytes",
            &loonfs_test_support::test_actor(),
            &options,
        )
        .await
        .expect("publish source");
    namespace
        .stat("/source")
        .await
        .expect("source entry")
        .content_ref()
        .expect("content reference")
        .clone()
}

#[tokio::test]
async fn by_reference_reads_require_publication_in_the_reading_view() {
    let (_directory, recording, writer) = open_writer().await;
    let source = namespace_id("private");
    let destination = namespace_id("unrestricted");
    create_namespace(&writer, &source, acl("administrator")).await;
    create_namespace(&writer, &destination, NamespaceAccess::unrestricted()).await;
    let private_ref = publish_inline(&writer, &source).await;
    loonfs::LoonFs::builder_with_store(recording.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            loonfs_test_support::test_actor().as_str(),
        ))
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id(
            loonfs_test_support::test_actor().as_str(),
        ))
        .fold_wal(&source)
        .await
        .expect("materialize private content");
    let reader = writer.read_only().with_subject(subject("stranger"));
    let destination_namespace = reader.namespace(&destination);
    let source_namespace = reader.namespace(&source);
    assert_eq!(
        source_namespace
            .read_file("/source")
            .await
            .expect_err("private path is unreadable")
            .code(),
        ErrorCode::PathNotFound
    );
    let view = destination_namespace
        .read_view()
        .await
        .expect("view before publication");
    let own_ref = publish_inline(&writer, &destination).await;
    assert_eq!(
        destination_namespace
            .read_content(&own_ref, u64::MAX)
            .await
            .expect("current view has published its own reference"),
        b"private inline bytes"
    );

    recording.reset();
    assert_eq!(
        destination_namespace
            .read_content(&private_ref, u64::MAX)
            .await
            .expect_err("unrelated namespace has not published private content")
            .code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        view.read_content(&own_ref, u64::MAX)
            .await
            .expect_err("read view predates its own content publication")
            .code(),
        ErrorCode::PathNotFound
    );
    assert!(recording.snapshot().iter().all(|operation| !matches!(
        loonfs_objectstore::layout::parse_object_key(operation.key()),
        Some(key) if key.family() == loonfs_objectstore::layout::DurableObjectFamily::ContentBlob
    )));
    assert_eq!(recording.count(OperationSelector::Put), 0);
    assert_eq!(recording.count(OperationSelector::CompareAndSwap), 0);
    assert_eq!(recording.count(OperationSelector::Delete), 0);
}
