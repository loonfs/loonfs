#![allow(clippy::panic)]
// Handle integration tests use panic in helper assertions for precise diagnostics.

//! Purpose-specific handle coverage: builder contracts, the background-work
//! policy, close semantics, and cross-handle reads. Each test drives every
//! handle from one runtime fixture, matching the runtime-ownership contract
//! the handles document.

use loonfs::{
    CreateNamespaceOptions, FsAdmin, FsBackgroundWork, FsReader, FsWriter, MaintenanceTickOptions,
    ManifestId, NamespaceId, PutFileOptions, RuntimeError, StoreConfig,
};
use std::future::Future;
use std::path::Path;
use tempfile::tempdir;

fn store_config(root: &Path) -> StoreConfig {
    StoreConfig::LocalFs {
        root: root.to_string_lossy().into_owned(),
        key_prefix: None,
    }
}

fn namespace_id() -> NamespaceId {
    NamespaceId::parse("demo").expect("valid namespace id")
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

async fn writer(root: &Path, background_work: FsBackgroundWork) -> FsWriter {
    FsWriter::builder(store_config(root))
        .writer_id("handle-test-writer")
        .background_work(background_work)
        .build()
        .await
        .expect("build writer")
}

async fn fill_wal_tail_past_threshold(writer: &FsWriter, namespace_id: &NamespaceId) {
    for round in 0..33u32 {
        writer
            .put_file_bytes(
                namespace_id,
                &format!("/docs/file-{round}.txt"),
                b"body",
                PutFileOptions::default(),
            )
            .await
            .expect("put file");
    }
}

#[test]
fn writer_reader_and_admin_share_a_namespace_through_store_config() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::ManualOnly).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/hello.txt",
                b"hello",
                PutFileOptions::default(),
            )
            .await
            .expect("put file");

        // A reader derived from the writer shares its caches.
        let derived = writer.reader();
        let read = derived
            .read_file_bytes(&namespace_id, "/docs/hello.txt")
            .await
            .expect("read through derived reader");
        assert_eq!(read.bytes, b"hello");

        // A standalone reader opens its own store client from config and
        // still observes the write.
        let standalone = FsReader::builder(store_config(temp_dir.path()))
            .build()
            .await
            .expect("build standalone reader");
        let read = standalone
            .read_file_bytes(&namespace_id, "/docs/hello.txt")
            .await
            .expect("read through standalone reader");
        assert_eq!(read.bytes, b"hello");
        let entries = standalone
            .list_path(&namespace_id, "/docs")
            .await
            .expect("list through standalone reader");
        assert_eq!(entries.len(), 1);

        // Admin inspects the same namespace through its own handle.
        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .namespace_status(&namespace_id)
            .await
            .expect("namespace status");
        assert_eq!(status.namespace_id, namespace_id);
        assert_eq!(status.wal_tail_segments, 1);
        // Admin-driven work is observable through the admin handle's own
        // cache counters, like writer and reader work through theirs.
        let _ = admin.runtime_cache_stats();

        standalone.close().await.expect("close standalone reader");
        derived.close().await.expect("close derived reader");
        admin.close().await.expect("close admin");
        writer.close().await.expect("close writer");
    });
}

#[test]
fn manual_only_writer_never_schedules_maintenance() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::ManualOnly).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        fill_wal_tail_past_threshold(&writer, &namespace_id).await;
        writer
            .wait_for_background_work()
            .await
            .expect("no background work to wait for");

        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .namespace_status(&namespace_id)
            .await
            .expect("status after writes");
        assert_eq!(
            status.current_manifest_id,
            Some(ManifestId(0)),
            "manual-only writer must not publish checkpoints: {status:?}"
        );
        assert!(
            status.wal_tail_segments >= 33,
            "manual-only writer must leave the tail alone: {status:?}"
        );

        // Explicit admin maintenance bounds the tail the writer left.
        let tick = admin
            .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
            .await
            .expect("explicit maintenance tick");
        assert_ne!(
            tick.outcome,
            loonfs::MaintenanceTickOutcome::NotNeeded,
            "tick should act on the oversized tail"
        );
        let status = admin
            .namespace_status(&namespace_id)
            .await
            .expect("status after explicit tick");
        assert!(
            status.current_manifest_id > Some(ManifestId(0)),
            "explicit tick should publish a manifest: {status:?}"
        );
        assert!(
            status.wal_tail_segments < 32,
            "explicit tick should bound the tail: {status:?}"
        );
    });
}

#[test]
fn enabled_writer_schedules_maintenance_on_its_owning_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::Enabled).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        fill_wal_tail_past_threshold(&writer, &namespace_id).await;
        writer
            .wait_for_background_work()
            .await
            .expect("background maintenance quiesces");

        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .namespace_status(&namespace_id)
            .await
            .expect("status after auto tick");
        assert!(
            status.current_manifest_id > Some(ManifestId(0)),
            "auto tick should have published a manifest: {status:?}"
        );
        assert!(
            status.wal_tail_segments < 32,
            "auto tick should have bounded the tail: {status:?}"
        );
        writer.close().await.expect("close writer");
    });
}

#[test]
fn closed_writer_rejects_new_background_work_but_keeps_writing() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::Enabled).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer.close().await.expect("close writer");

        // Foreground writes still work after close; only handle-owned
        // background scheduling is rejected.
        fill_wal_tail_past_threshold(&writer, &namespace_id).await;
        writer
            .wait_for_background_work()
            .await
            .expect("nothing scheduled after close");

        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let status = admin
            .namespace_status(&namespace_id)
            .await
            .expect("status after post-close writes");
        assert_eq!(
            status.current_manifest_id,
            Some(ManifestId(0)),
            "closed writer must not schedule checkpoints: {status:?}"
        );
        assert!(
            status.wal_tail_segments >= 33,
            "closed writer must leave the tail alone: {status:?}"
        );
    });
}

#[test]
fn builders_require_identity_and_a_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    match block_on(FsWriter::builder(store_config(temp_dir.path())).build()) {
        Err(RuntimeError::Config(_)) => {}
        Err(other) => panic!("expected config error for missing writer_id, got {other:?}"),
        Ok(_) => panic!("writer_id must be required"),
    }
    match block_on(FsAdmin::builder(store_config(temp_dir.path())).build()) {
        Err(RuntimeError::Config(_)) => {}
        Err(other) => panic!("expected config error for missing actor_id, got {other:?}"),
        Ok(_) => panic!("actor_id must be required"),
    }

    // Polling build() outside a Tokio runtime is a config error, not a panic.
    let outside_runtime = futures::executor::block_on(
        FsWriter::builder(store_config(temp_dir.path()))
            .writer_id("handle-test-writer")
            .build(),
    );
    match outside_runtime {
        Err(RuntimeError::Config(_)) => {}
        Err(other) => panic!("expected config error outside a runtime, got {other:?}"),
        Ok(_) => panic!("build must require an owning runtime"),
    }
}

#[test]
fn admin_checkpoint_and_retention_are_explicit_one_shot_calls() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    block_on(async {
        let writer = writer(temp_dir.path(), FsBackgroundWork::ManualOnly).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/hello.txt",
                b"hello",
                PutFileOptions::default(),
            )
            .await
            .expect("put file");

        let admin = FsAdmin::builder(store_config(temp_dir.path()))
            .actor_id("handle-test-admin")
            .build()
            .await
            .expect("build admin");
        let checkpoint = admin
            .create_checkpoint(&namespace_id)
            .await
            .expect("create checkpoint");
        assert!(checkpoint.manifest_id > ManifestId(0));
        let retention = admin
            .advance_retention_floor(&namespace_id)
            .await
            .expect("advance retention");
        assert_eq!(retention.namespace_id, namespace_id);
    });
}
