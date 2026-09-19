//! Source authorization for embedded bare content reference imports.

use loonfs::{
    CreateNamespaceOptions, DeleteNamespaceOptions, ForkNamespaceOptions, FsWriter, PutFileOptions,
    RuntimeError, SharedObjectStore, UpdateAccessOptions,
};
use loonfs_api::{
    AccessGrants, AccessRight, AccessRights, ContentRef, ErrorCode, NamespaceAccess, NamespaceId,
    PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
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

async fn open_writer() -> (
    tempfile::TempDir,
    Arc<RecordingStore<LocalFsStore>>,
    FsWriter,
) {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = FsWriter::builder_with_store(store)
        .writer_id("content-ref-import-access")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    (directory, recording, writer)
}

async fn create_namespace(writer: &FsWriter, namespace_id: &NamespaceId, access: NamespaceAccess) {
    writer
        .create_namespace(
            namespace_id,
            CreateNamespaceOptions {
                access,
                ..CreateNamespaceOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("namespace");
}

async fn publish_inline(writer: &FsWriter, namespace_id: &NamespaceId) -> ContentRef {
    let mut options = PutFileOptions::new(loonfs_test_support::test_actor());
    options.commit.subject = Some(subject("administrator"));
    writer
        .put_file_bytes(namespace_id, "/source", b"private inline bytes", options)
        .await
        .expect("publish source");
    writer
        .reader()
        .as_subject(subject("administrator"))
        .get_path_entry(namespace_id, "/source", Default::default())
        .await
        .expect("source entry")
        .content_ref()
        .expect("content reference")
        .clone()
}

fn assert_forbidden_without_writes(recording: &RecordingStore<LocalFsStore>, error: RuntimeError) {
    assert_eq!(error.code(), ErrorCode::Forbidden);
    assert_eq!(recording.count(OperationClass::Put), 0);
}

#[tokio::test]
async fn subject_without_source_rights_cannot_prepare_or_publish_an_inline_tail_reference() {
    let (_directory, recording, writer) = open_writer().await;
    let source = namespace_id("source");
    let destination = namespace_id("destination");
    create_namespace(&writer, &source, acl("administrator")).await;
    create_namespace(&writer, &destination, NamespaceAccess::unrestricted()).await;
    let content_ref = publish_inline(&writer, &source).await;
    let scoped = writer.as_subject(subject("stranger"));

    assert_eq!(
        scoped
            .reader()
            .get_file_bytes(&source, "/source")
            .await
            .expect_err("source read")
            .code(),
        ErrorCode::PathNotFound
    );

    recording.reset();
    let error = scoped
        .prepare_content_ref(&destination, content_ref.clone())
        .await
        .expect_err("prepare requires source administrator");
    assert_forbidden_without_writes(recording.as_ref(), error);

    recording.reset();
    let error = scoped
        .put_file_content_ref(
            &destination,
            "/imported",
            content_ref,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect_err("put requires source administrator");
    assert_forbidden_without_writes(recording.as_ref(), error);
    assert_eq!(
        writer
            .reader()
            .get_file_bytes(&destination, "/imported")
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
    let content_ref = publish_inline(&writer, &namespace_id).await;

    recording.reset();
    let error = writer
        .as_subject(subject("stranger"))
        .put_file_content_ref(
            &namespace_id,
            "/imported",
            content_ref.clone(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect_err("same-namespace import requires administrator");
    assert_forbidden_without_writes(recording.as_ref(), error);

    let prepared = writer
        .as_subject(subject("administrator"))
        .prepare_content_ref(&namespace_id, content_ref.clone())
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
        (writer.as_subject(subject("administrator")), acl_ref.clone()),
        (writer.as_subject(subject("stranger")), unrestricted_ref),
    ] {
        let prepared = authority
            .prepare_content_ref(&destination, content_ref.clone())
            .await
            .expect("authorized import");
        assert_eq!(prepared.content_ref().owner_namespace_id, destination);
        assert_ne!(prepared.content_ref().content_id, content_ref.content_id);
    }
}

#[tokio::test]
async fn deleted_owner_import_uses_updated_access_state_in_the_surviving_head() {
    let (_directory, recording, writer) = open_writer().await;
    let source = namespace_id("source");
    let fork = namespace_id("fork");
    let destination = namespace_id("destination");
    create_namespace(&writer, &source, acl("administrator")).await;
    create_namespace(&writer, &destination, NamespaceAccess::unrestricted()).await;
    let content_ref = publish_inline(&writer, &source).await;
    let mut access = UpdateAccessOptions::new(
        loonfs_test_support::test_actor(),
        administrator_grants("replacement"),
    );
    access.commit.subject = Some(subject("administrator"));
    writer
        .update_access(&source, "/", access)
        .await
        .expect("replace administrator");
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

    recording.reset();
    let error = writer
        .as_subject(subject("administrator"))
        .prepare_content_ref(&destination, content_ref.clone())
        .await
        .expect_err("former administrator is refused");
    assert_forbidden_without_writes(recording.as_ref(), error);

    let prepared = writer
        .as_subject(subject("replacement"))
        .prepare_content_ref(&destination, content_ref.clone())
        .await
        .expect("surviving access state authorizes administrator");
    assert_eq!(prepared.content_ref().owner_namespace_id, destination);
    assert_ne!(prepared.content_ref().content_id, content_ref.content_id);
}
