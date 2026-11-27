//! Authorization for embedded content reference reads.

use loonfs::test_support::writer;
use loonfs::{LoonFs, PutFileOptions, SharedObjectStore, Writable};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, OperationSelector, RecordingStore};
use loonfs_types::{
    AccessGrants, AccessRight, ContentRef, ErrorCode, NamespaceId, PrincipalId, PrincipalSet,
    Subject,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn subject(principal: &str) -> Subject {
    Subject {
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals"),
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
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = writer(store, "content-ref-read-access").await;
    (directory, recording, writer)
}

/// Creates `namespace_id`, with `administrator` as its administrator when
/// there is one.
async fn create_namespace(
    writer: &LoonFs<Writable>,
    namespace_id: &NamespaceId,
    administrator: Option<&str>,
) {
    writer
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    if let Some(administrator) = administrator {
        loonfs::test_support::set_root_access(
            writer,
            namespace_id,
            administrator_grants(administrator),
        )
        .await;
    }
}

async fn publish_inline(writer: &LoonFs<Writable>, namespace_id: &NamespaceId) -> ContentRef {
    let namespace = writer
        .read_only()
        .with_context(subject("administrator").into())
        .namespace(namespace_id);
    let namespace_writer = writer.open_namespace(namespace_id).expect("open namespace");
    let options = PutFileOptions::default();
    namespace_writer
        .with_context(subject("administrator").into())
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
    let destination = namespace_id("destination");
    create_namespace(&writer, &source, Some("administrator")).await;
    create_namespace(&writer, &destination, Some("administrator")).await;
    let private_ref = publish_inline(&writer, &source).await;
    writer
        .open_namespace(&source)
        .expect("open the source")
        .fold_wal()
        .await
        .expect("materialize private content");
    let reader = writer.read_only().with_context(subject("stranger").into());
    let destination_namespace = writer.read_only().namespace(&destination);
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
