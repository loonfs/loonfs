#![allow(clippy::panic)]
// Lifecycle assertions use panic for precise failure diagnostics.

//! Handle-level lifecycle of the gram index: enable through `FsAdmin`,
//! build through maintenance ticks, query through `FsReader`, disable.

use loonfs::{
    CreateNamespaceOptions, ErrorCode, FsAdmin, FsBackgroundWork, FsReader, FsWriter, GrepRequest,
    MaintenanceTickOptions, NamespaceId, PutFileOptions,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use std::sync::Arc;
use tempfile::tempdir;

fn grep_request(pattern: &str) -> GrepRequest {
    GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        limit: None,
        allow_stale: false,
        allow_scan: false,
    }
}

#[tokio::test]
async fn maintenance_ticks_build_the_gram_index_once_enabled() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-runtime").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-writer")
        .commit_window_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-admin")
        .build()
        .await
        .expect("build admin");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/alpha.txt",
            b"a needle in alpha\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write alpha");
    writer
        .put_file_bytes(
            &namespace_id,
            "/bravo.txt",
            b"nothing here\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write bravo");

    // Before enablement, grep names the missing data half.
    let error = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect_err("grep without the feature must be refused");
    let loonfs::Error::Core(core) = &error else {
        panic!("expected a core error, got {error:?}");
    };
    assert_eq!(core.code(), ErrorCode::NotSupported);

    let enabled = admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");
    assert!(!enabled.already_enabled);
    let again = admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("re-enable");
    assert!(again.already_enabled);

    // Explicit maintenance ticks run the backfill and keep the watermark
    // current; two ticks comfortably cover backfill plus catch-up here.
    for _ in 0..2 {
        admin
            .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
            .await
            .expect("maintenance tick");
    }

    let response = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("grep after ticks");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/alpha.txt");

    // New commits are visible immediately through the exhaustive tail, and
    // a later tick absorbs them into the index.
    writer
        .put_file_bytes(
            &namespace_id,
            "/charlie.txt",
            b"another needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write charlie");
    let response = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("grep with tail");
    assert_eq!(response.matches.len(), 2);
    admin
        .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
        .await
        .expect("tick after write");
    let response = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("grep after catch-up tick");
    assert_eq!(response.matches.len(), 2);
    assert!(response.built_through_seq.0 > 0);

    let disabled = admin
        .disable_grams_index(&namespace_id)
        .await
        .expect("disable");
    assert!(disabled.was_enabled);
    let error = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect_err("grep after disable must be refused");
    let loonfs::Error::Core(core) = &error else {
        panic!("expected a core error, got {error:?}");
    };
    assert_eq!(core.code(), ErrorCode::NotSupported);

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Index catch-up is scheduled by index lag, not by the WAL-segment
/// threshold: one small publish must still get the index built in the
/// background, with no explicit ticks and a tail far below the threshold.
#[tokio::test]
async fn a_publish_below_the_wal_threshold_still_schedules_index_catch_up() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-auto-tick").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-auto-writer")
        .commit_window_ms(0)
        .background_work(FsBackgroundWork::Enabled)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-auto-admin")
        .build()
        .await
        .expect("build admin");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");

    // The writer runtime has not observed this namespace's feature yet, so
    // this publish exercises the discovery path of the scheduling hint.
    writer
        .put_file_bytes(
            &namespace_id,
            "/delta.txt",
            b"a needle in delta\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write delta");
    writer
        .wait_for_background_work()
        .await
        .expect("background work quiesces");

    let status = admin.namespace_status(&namespace_id).await.expect("status");
    assert!(
        status.wal_tail_segments < MaintenanceTickOptions::default().max_wal_tail_segments,
        "the tail must stay below the flush threshold for this test to \
         exercise index-only scheduling: {status:?}"
    );

    // A stale grep serves from the index alone, so a match here proves the
    // background drain advanced the watermark past the publish.
    let mut stale = grep_request("needle");
    stale.allow_stale = true;
    let response = reader
        .grep(&namespace_id, &stale)
        .await
        .expect("stale grep after background catch-up");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/delta.txt");

    writer.shutdown_background().await.expect("writer shutdown");
}
