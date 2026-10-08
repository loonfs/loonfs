//! Authorization for embedded content reference reads and imports.

use loonfs::AccessState;
use loonfs::{CreateNamespaceOptions, Error, LoonFs, PutFileOptions, SharedObjectStore, Writable};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
use loonfs_types::{
    AccessGrants, AccessRight, AccessRights, ContentRef, ErrorCode, NamespaceAccess, NamespaceId,
    PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
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
        AccessRights::from_iter([AccessRight::Admin]),
    )]))
    .expect("grants")
}

/// Runs one collection pass on `namespace_id` at a clock far enough ahead
/// that every unreferenced segment is past its age gate.
async fn collect_aged_segments(store: &RecordingStore<LocalFsStore>, namespace_id: &NamespaceId) {
    let now_ms = loonfs::current_time_ms().expect("wall clock")
        + 2 * loonfs_core::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS
        + 2 * loonfs::GcOptions::default().grace_window_ms;
    loonfs_core::gc_namespace(
        store,
        None,
        namespace_id,
        &loonfs::GcOptions::default(),
        &loonfs_core::MutationContext {
            writer_id: loonfs_types::WriterId::parse("import-access-gc").expect("writer id"),
            now_ms,
        },
    )
    .await
    .expect("garbage collection");
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
    let writer = LoonFs::builder_with_store(store)
        .writer_id("content-ref-import-access")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
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

fn assert_forbidden_without_writes(recording: &RecordingStore<LocalFsStore>, error: Error) {
    assert_eq!(error.code(), ErrorCode::Forbidden);
    assert_eq!(recording.count(OperationClass::Put), 0);
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
        .writer_id(loonfs_test_support::test_actor().as_str())
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
    assert_eq!(recording.count(OperationClass::Put), 0);
    assert_eq!(recording.count(OperationClass::CompareAndSwap), 0);
    assert_eq!(recording.count(OperationClass::Delete), 0);
}

#[tokio::test]
async fn subject_without_source_rights_cannot_prepare_or_publish_an_inline_tail_reference() {
    let (_directory, recording, writer) = open_writer().await;
    let source = namespace_id("source");
    let destination = namespace_id("destination");
    let destination_namespace = writer.namespace(&destination);
    create_namespace(&writer, &source, acl("administrator")).await;
    create_namespace(&writer, &destination, NamespaceAccess::unrestricted()).await;
    let content_ref = publish_inline(&writer, &source).await;
    let scoped = writer.with_subject(subject("stranger"));
    let source_namespace = scoped.namespace(&source);
    let namespace = scoped.open_namespace(&destination).expect("open namespace");

    assert_eq!(
        source_namespace
            .read_file("/source")
            .await
            .expect_err("source read")
            .code(),
        ErrorCode::PathNotFound
    );

    recording.reset();
    let error = namespace
        .prepare_content_ref(content_ref.clone())
        .await
        .expect_err("prepare requires source administrator");
    assert_forbidden_without_writes(recording.as_ref(), error);

    recording.reset();
    let error = namespace
        .put_file_content_ref("/imported", content_ref, &loonfs_test_support::test_actor())
        .await
        .expect_err("put requires source administrator");
    assert_forbidden_without_writes(recording.as_ref(), error);
    assert_eq!(
        destination_namespace
            .read_file("/imported")
            .await
            .expect_err("refused put publishes nothing")
            .code(),
        ErrorCode::PathNotFound
    );
}

#[tokio::test]
async fn same_namespace_inline_tail_import_requires_its_administrator() {
    let (_directory, recording, writer) = open_writer().await;
    let namespace_id = namespace_id("same-namespace");
    create_namespace(&writer, &namespace_id, acl("administrator")).await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let content_ref = publish_inline(&writer, &namespace_id).await;

    recording.reset();
    let error = namespace
        .with_subject(subject("stranger"))
        .put_file_content_ref(
            "/imported",
            content_ref.clone(),
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect_err("same-namespace import requires administrator");
    assert_forbidden_without_writes(recording.as_ref(), error);

    let prepared = namespace
        .with_subject(subject("administrator"))
        .prepare_content_ref(content_ref.clone())
        .await
        .expect("administrator imports same-namespace reference");
    assert_eq!(prepared.content_ref().owner_namespace_id, namespace_id);
    assert_ne!(prepared.content_ref().content_id, content_ref.content_id);
}

#[tokio::test]
async fn bare_reference_import_accepts_service_administrator_and_unrestricted_authority() {
    let (_directory, _recording, writer) = open_writer().await;
    let acl_source = namespace_id("acl-source");
    let unrestricted_source = namespace_id("unrestricted-source");
    let destination = namespace_id("destination");
    create_namespace(&writer, &acl_source, acl("administrator")).await;
    create_namespace(
        &writer,
        &unrestricted_source,
        NamespaceAccess::unrestricted(),
    )
    .await;
    create_namespace(&writer, &destination, NamespaceAccess::unrestricted()).await;
    let acl_ref = publish_inline(&writer, &acl_source).await;
    let unrestricted_ref = publish_inline(&writer, &unrestricted_source).await;

    for (authority, content_ref) in [
        (writer.clone(), acl_ref.clone()),
        (
            writer.with_subject(subject("administrator")),
            acl_ref.clone(),
        ),
        (writer.with_subject(subject("stranger")), unrestricted_ref),
    ] {
        let namespace = authority
            .open_namespace(&destination)
            .expect("open namespace");
        let prepared = namespace
            .prepare_content_ref(content_ref.clone())
            .await
            .expect("authorized import");
        assert_eq!(prepared.content_ref().owner_namespace_id, destination);
        assert_ne!(prepared.content_ref().content_id, content_ref.content_id);
    }
}

#[tokio::test]
async fn reclaimed_deleted_owner_import_reports_the_owner_without_writes() {
    let (_directory, recording, writer) = open_writer().await;
    let source = namespace_id("reclaimed-source");
    let destination = namespace_id("destination");
    create_namespace(&writer, &source, acl("administrator")).await;
    let source_writer = writer.open_namespace(&source).expect("open namespace");
    create_namespace(&writer, &destination, NamespaceAccess::unrestricted()).await;
    let destination_writer = writer.open_namespace(&destination).expect("open namespace");
    let content_ref = publish_inline(&writer, &source).await;
    source_writer.delete().await.expect("delete owner");
    let report = loonfs_core::gc_namespace(
        recording.as_ref(),
        None,
        &source,
        &loonfs_core::GcOptions {
            grace_window_ms: loonfs_core::limits::GC_MIN_GRACE_WINDOW_MS,
        },
        &loonfs_core::MutationContext {
            writer_id: loonfs_types::WriterId::parse("collector").expect("writer id"),
            now_ms: loonfs::current_time_ms().expect("clock")
                + loonfs_core::limits::NAMESPACE_RETIREMENT_GRACE_MS
                + 1,
        },
    )
    .await
    .expect("collect retired content");
    assert_eq!(report.deleted.retired_content_objects, 1);

    recording.reset();
    let error = destination_writer
        .with_subject(subject("administrator"))
        .prepare_content_ref(content_ref)
        .await
        .expect_err("reclaimed owner");
    assert_eq!(error.code(), ErrorCode::NamespaceDeleted);
    assert_eq!(error.details().expect("details").namespace_id, Some(source));
    assert_eq!(recording.count(OperationClass::Put), 0);
    assert_eq!(recording.count(OperationClass::CompareAndSwap), 0);
    assert_eq!(recording.count(OperationClass::Delete), 0);
}

#[tokio::test]
async fn deleted_owner_import_uses_updated_access_state_in_the_surviving_head() {
    let (_directory, recording, writer) = open_writer().await;
    let source = namespace_id("source");
    let fork = namespace_id("fork");
    let namespace = writer
        .read_only()
        .with_subject(subject("administrator"))
        .namespace(&fork);
    let destination = namespace_id("destination");
    create_namespace(&writer, &source, acl("administrator")).await;
    create_namespace(&writer, &destination, NamespaceAccess::unrestricted()).await;
    let content_ref = publish_inline(&writer, &source).await;
    writer
        .fork_namespace(&source, &fork, &loonfs_test_support::test_actor())
        .await
        .expect("fork");
    let source_writer = writer.open_namespace(&source).expect("open namespace");
    // The replacement lands after the fork, so only the source's final runs
    // carry it; the fork keeps the administrator it inherited.
    source_writer
        .with_subject(subject("administrator"))
        .update_access(
            "/",
            &loonfs_test_support::test_actor(),
            AccessState {
                boundary: false,
                grants: administrator_grants("replacement"),
            },
        )
        .await
        .expect("replace administrator");
    source_writer.delete().await.expect("delete source");
    assert_eq!(
        namespace
            .read_view()
            .await
            .expect("view fork")
            .read_content(&content_ref, u64::MAX)
            .await
            .expect("fork published the inherited reference"),
        b"private inline bytes"
    );
    // Only the tombstone names the runs holding the replacement. A pass past
    // the age gate must leave them, and a cold runtime must find them.
    collect_aged_segments(recording.as_ref(), &source).await;
    let store: SharedObjectStore = recording.clone();
    let importer = LoonFs::builder_with_store(store)
        .writer_id("content-ref-import-access-cold")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("cold writer");
    let destination_writer = importer
        .open_namespace(&destination)
        .expect("open namespace");

    recording.reset();
    let error = destination_writer
        .with_subject(subject("administrator"))
        .prepare_content_ref(content_ref.clone())
        .await
        .expect_err("former administrator is refused");
    assert_forbidden_without_writes(recording.as_ref(), error);

    let prepared = destination_writer
        .with_subject(subject("replacement"))
        .prepare_content_ref(content_ref.clone())
        .await
        .expect("surviving access state authorizes administrator");
    assert_eq!(prepared.content_ref().owner_namespace_id, destination);
    assert_ne!(prepared.content_ref().content_id, content_ref.content_id);
}
