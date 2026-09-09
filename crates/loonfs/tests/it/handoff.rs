//! Multi-node handoff and durable reconstruction.

#![allow(clippy::panic)]

use crate::common::{collect_path_entries, directory_options, expect_code, writer};
use loonfs::{
    CreateNamespaceOptions, ErrorCode, FsMaintenance, FsReader, FsWriter, ManifestNo,
    MetadataMaintenanceOptions, NamespaceId, NamespaceSessionPolicy, NamespaceSessionState,
    PutFileOptions, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

async fn explicit_writer(store: SharedObjectStore, writer_id: &str) -> FsWriter {
    FsWriter::builder_with_store(store)
        .writer_id(writer_id)
        .min_publish_interval_ms(0)
        .namespace_sessions(NamespaceSessionPolicy::ExplicitOpen)
        .build()
        .await
        .expect("build explicit writer")
}

fn file_options() -> PutFileOptions {
    PutFileOptions::new(loonfs_test_support::test_actor())
}

async fn fresh_reader(store: SharedObjectStore) -> FsReader {
    FsReader::builder_with_store(store)
        .build()
        .await
        .expect("build fresh reader")
}

async fn assert_root_paths(
    reader: &FsReader,
    namespace_id: &NamespaceId,
    expected: &BTreeSet<String>,
) {
    let actual = collect_path_entries(reader, namespace_id, "/")
        .await
        .expect("list namespace root")
        .entries
        .into_iter()
        .map(|entry| entry.path.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(&actual, expected);
}

async fn put_file(writer: &FsWriter, namespace_id: &NamespaceId, path: &str) {
    writer
        .put_file_bytes(namespace_id, path, b"body", file_options())
        .await
        .expect("publish file");
}

#[tokio::test]
async fn a_takeover_during_a_paused_publish_fences_the_old_node() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("paused-takeover").expect("namespace id");
    let blocking = BlockingStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        crate::common::data_wal_put_for(&namespace_id),
    );
    let failing = Arc::new(FailStore::new(
        blocking,
        KeyPredicate::any(),
        OperationClass::Any,
        InjectedError::PermissionDenied("store access after fencing".to_owned()),
    ));
    let store: SharedObjectStore = failing.clone();
    let writer_a = writer(store.clone(), "paused-writer-a").await;
    let writer_b = writer(store.clone(), "takeover-writer-b").await;
    writer_a
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer_a
        .create_directory(&namespace_id, "/from-a-first", directory_options())
        .await
        .expect("writer A publishes first");

    failing.inner().block_next();
    let parked = tokio::spawn({
        let writer_a = writer_a.clone();
        let namespace_id = namespace_id.clone();
        async move {
            writer_a
                .create_directory(&namespace_id, "/from-a-parked", directory_options())
                .await
        }
    });
    failing.inner().wait_until_blocked().await;

    writer_b
        .create_directory(&namespace_id, "/from-b", directory_options())
        .await
        .expect("writer B takes over and publishes");
    failing.inner().release();
    expect_code(
        parked.await.expect("join writer A publish"),
        ErrorCode::WriterFenced,
    );

    failing.fail_all();
    expect_code(
        writer_a
            .create_directory(&namespace_id, "/from-a-after-fence", directory_options())
            .await,
        ErrorCode::WriterFenced,
    );
    assert_eq!(failing.attempts(), 0);
    assert_eq!(
        writer_a.namespace_session_state(&namespace_id),
        NamespaceSessionState::Open {
            fenced: true,
            queued_commits: 0,
        }
    );
    failing.clear();

    let expected = BTreeSet::from(["/from-a-first".to_owned(), "/from-b".to_owned()]);
    assert_root_paths(&writer_b.reader(), &namespace_id, &expected).await;
    let cold = fresh_reader(store).await;
    assert_root_paths(&cold, &namespace_id, &expected).await;
}

#[tokio::test]
async fn a_closed_session_does_not_reopen_for_a_stale_request() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let namespace_id = NamespaceId::parse("closed-session").expect("namespace id");
    let writer_a = explicit_writer(store.clone(), "session-writer-a").await;
    let writer_b = explicit_writer(store, "session-writer-b").await;
    writer_a
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer_a
        .open_namespace(&namespace_id)
        .expect("open writer A session");
    let stale_a = writer_a.clone();
    writer_a
        .create_directory(&namespace_id, "/from-a-first", directory_options())
        .await
        .expect("writer A publishes");
    writer_a
        .close_namespace(&namespace_id)
        .await
        .expect("close writer A session");

    for refused in [
        stale_a.create_directory(&namespace_id, "/from-stale-clone", directory_options()),
        writer_a.create_directory(&namespace_id, "/from-closed-a", directory_options()),
    ] {
        expect_code(refused.await, ErrorCode::WriterSessionClosed);
    }

    writer_b
        .open_namespace(&namespace_id)
        .expect("open writer B session");
    writer_b
        .create_directory(&namespace_id, "/from-b", directory_options())
        .await
        .expect("writer B publishes");

    writer_a
        .open_namespace(&namespace_id)
        .expect("explicitly reopen writer A session");
    writer_a
        .create_directory(&namespace_id, "/from-a-reopened", directory_options())
        .await
        .expect("reopened writer A publishes");
    expect_code(
        writer_b
            .create_directory(&namespace_id, "/from-fenced-b", directory_options())
            .await,
        ErrorCode::WriterFenced,
    );
}

#[tokio::test]
async fn a_cold_node_reconstructs_current_state_during_active_writes() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let namespace_id = NamespaceId::parse("cold-handoff").expect("namespace id");
    let writer = writer(store.clone(), "active-writer").await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let fold_threshold = usize::try_from(
        MetadataMaintenanceOptions::default()
            .max_wal_tail_segments
            .get(),
    )
    .expect("fold threshold fits usize");
    let mut expected = BTreeSet::new();

    for index in 0..(fold_threshold - 1) {
        let path = format!("/file-{index:03}.txt");
        put_file(&writer, &namespace_id, &path).await;
        expected.insert(path);
    }
    assert_root_paths(&fresh_reader(store.clone()).await, &namespace_id, &expected).await;

    let fold_path = format!("/file-{:03}.txt", fold_threshold - 1);
    put_file(&writer, &namespace_id, &fold_path).await;
    expected.insert(fold_path);
    writer
        .wait_for_fold(&namespace_id)
        .await
        .expect("first fold completes");
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id("cold-handoff-inspection")
        .build()
        .await
        .expect("build maintenance handle");
    let diagnostics = maintenance
        .get_namespace_diagnostics(&namespace_id)
        .await
        .expect("read diagnostics after fold");
    assert_eq!(diagnostics.current_manifest_no, Some(ManifestNo(3)));
    assert_root_paths(&fresh_reader(store.clone()).await, &namespace_id, &expected).await;

    for index in fold_threshold..(fold_threshold + 3) {
        let path = format!("/file-{index:03}.txt");
        put_file(&writer, &namespace_id, &path).await;
        expected.insert(path);
    }
    assert_root_paths(&fresh_reader(store).await, &namespace_id, &expected).await;
}
