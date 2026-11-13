//! Checks operation spans for the writer, reader, and maintenance handles.
//!
//! This is a separate binary because the tests install a subscriber with
//! `set_default`, and tracing caches per-callsite interest for the whole
//! process.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod capture;

use capture::take_log;
use loonfs::LoonFs;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{ErrorCode, PrincipalId};
use tempfile::tempdir;

fn closed_spans(log: &str, span_name: &str) -> Vec<serde_json::Value> {
    log.lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("captured JSON event"))
        // `span` is the span closing; `spans` also includes its ancestors.
        // A child's close event must not count as another parent close.
        .filter(|event| event["fields"]["message"] == "close" && event["span"]["name"] == span_name)
        .collect()
}

#[test]
fn every_handle_emits_an_operation_span_with_its_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("operation-spans");
    let (captured, _guard) = capture::capture(true);

    block_on(async {
        let writer = capture::writer(temp_dir.path(), "operation-span-writer").await;
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let recovery = writer
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .recover_administrator(
                &PrincipalId::parse("prn_root").expect("principal"),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect_err("an unrestricted namespace has no administrator to recover");
        assert_eq!(recovery.code(), ErrorCode::NamespaceUnrestricted);

        let reader = writer.read_only();
        let namespace = reader.namespace(&namespace_id);
        namespace.stat("/").await.expect("stat namespace root");
        namespace.metadata().await.expect("read namespace state");

        let maintenance = LoonFs::builder_with_store(writer.object_store())
            .writer_id(loonfs_test_support::ids::writer_id(
                "operation-span-maintenance",
            ))
            .build()
            .await
            .expect("build maintenance")
            .maintenance(loonfs_test_support::ids::writer_id(
                "operation-span-maintenance",
            ));
        maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("read namespace diagnostics");
        maintenance
            .maintain_metadata(&namespace_id)
            .await
            .expect("maintain metadata");
    });

    let log = take_log(&captured);
    for span_name in [
        "loonfs.create_namespace",
        "loonfs.recover_administrator",
        "loonfs.stat",
        "loonfs.metadata",
        "loonfs.diagnostics",
        "loonfs.maintain_metadata",
    ] {
        let operation = span_name.trim_start_matches("loonfs.");
        assert!(
            closed_spans(&log, span_name).iter().any(|event| {
                event["span"]["namespace_id"] == "operation-spans"
                    && event["span"]["operation"] == operation
            }),
            "`{span_name}` lacks a close event with its namespace and operation in:\n{log}"
        );
    }
}

#[test]
fn delegated_writer_calls_close_one_operation_span() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("operation-span-counts");
    let (captured, _guard) = capture::capture(true);

    let (create_log, put_log) = block_on(async {
        let writer = capture::writer(temp_dir.path(), "operation-span-count-writer").await;
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        let _setup_log = take_log(&captured);
        namespace
            .create_directory("/docs", &loonfs_test_support::test_actor())
            .await
            .expect("create directory");
        let create_log = take_log(&captured);
        namespace
            .put_file(
                "/docs/file.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");
        let put_log = take_log(&captured);
        (create_log, put_log)
    });

    let create_directory = closed_spans(&create_log, "loonfs.create_directory");
    assert_eq!(
        create_directory.len(),
        1,
        "create_directory closed the wrong number of create_directory spans:\n{create_log}"
    );
    assert!(
        create_directory[0]["span"]["operation"] == "create_directory",
        "create_directory span lacks its operation field:\n{}",
        create_directory[0]
    );
    assert_eq!(
        closed_spans(&put_log, "loonfs.put_file").len(),
        1,
        "put_file closed the wrong number of put_file spans:\n{put_log}"
    );
    assert_eq!(
        closed_spans(&put_log, "loonfs.prepare_content").len(),
        0,
        "put_file closed a prepare_content span:\n{put_log}"
    );
    for commit_span in [
        "loonfs.commit",
        "loonfs.commit_prepared",
        "loonfs.commit_candidate",
    ] {
        assert_eq!(
            closed_spans(&put_log, commit_span).len(),
            0,
            "put_file closed a `{commit_span}` span:\n{put_log}"
        );
    }
}
