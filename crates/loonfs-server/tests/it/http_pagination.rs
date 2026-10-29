//! HTTP directory and revision pagination behavior.

use crate::common::http_split_support::*;
use crate::common::{assert_api_code, collect_checkpoints, collect_path_entries, start_server};
use loonfs_client::{ClientError, ListOptions, MoveOptions, NamespacePath, PutFileOptions};
use loonfs_test_support::http::{raw_agent, retry_result_on_macos_teardown_einval};
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_types::{
    ChangeSeq, CommitId, DestinationBehavior, ErrorCode, ErrorResponse, FilesystemChange,
    ListCheckpointsResponse, ListPathEntriesResponse, RevisionNo, MAX_PAGE_LIMIT,
};
use std::assert_matches;
use tempfile::tempdir;

fn entry_names(response: &ListPathEntriesResponse) -> Vec<&str> {
    response
        .entries
        .iter()
        .map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("listed entry should carry a display name")
                .as_str()
        })
        .collect()
}

fn assert_invalid_request<T: std::fmt::Debug>(result: Result<T, ClientError>) {
    assert_api_code(result, 400, ErrorCode::InvalidRequest);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_paginates_checkpoint_inventory_and_rejects_invalid_requests() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-checkpoint-pagination",
    ))
    .await;
    let demo = namespace_id("demo");
    let other = namespace_id("other");
    for namespace_id in [&demo, &other] {
        harness
            .client
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
    }

    let mut expected_ids = Vec::new();
    for index in 0..5 {
        expected_ids.push(
            harness
                .client
                .create_snapshot(&demo, &format!("pin-{index}"))
                .await
                .expect("create checkpoint")
                .snapshot_id,
        );
    }
    expected_ids.sort();

    let mut actual_ids = Vec::new();
    let mut cursor = None;
    loop {
        let page = harness
            .client
            .list_checkpoints(&demo)
            .page_limit(page_limit(2))
            .cursor(cursor.clone())
            .page()
            .await
            .expect("list checkpoint page");
        actual_ids.extend(
            page.checkpoints
                .into_iter()
                .map(|checkpoint| checkpoint.checkpoint_id),
        );
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(actual_ids, expected_ids);

    let all = collect_checkpoints(&harness.client, &demo)
        .await
        .expect("aggregate checkpoint pages");
    assert_eq!(
        all.checkpoints
            .into_iter()
            .map(|checkpoint| checkpoint.checkpoint_id)
            .collect::<Vec<_>>(),
        expected_ids
    );
    assert!(all.next_cursor.is_none());

    let first = harness
        .client
        .list_checkpoints(&demo)
        .page_limit(page_limit(1))
        .page()
        .await
        .expect("first checkpoint page");
    let foreign_cursor = first.next_cursor.expect("checkpoint cursor");
    assert_invalid_request(
        harness
            .client
            .list_checkpoints(&other)
            .page_limit(page_limit(1))
            .cursor(Some(foreign_cursor.clone()))
            .page()
            .await,
    );
    assert_invalid_request(
        harness
            .client
            .list_checkpoints(&demo)
            .page_limit(page_limit(1))
            .cursor(Some("not-a-cursor".to_owned()))
            .page()
            .await,
    );
    // A page request holds only a valid limit, so a zero limit goes out raw.
    let zero_limit: Result<ListCheckpointsResponse, Box<ErrorResponse>> = get_json(
        &format!(
            "{}/v0/maintenance/namespaces/demo/checkpoints?limit=0",
            harness.server_url
        ),
        "test-token",
    );
    assert_eq!(
        zero_limit.expect_err("zero limit rejected").code,
        "invalid_request"
    );
    assert_invalid_request(
        harness
            .client
            .list_checkpoints(&demo)
            .page_limit(page_limit(MAX_PAGE_LIMIT + 1))
            .page()
            .await,
    );

    let raw: ListCheckpointsResponse = get_json(
        &format!(
            "{}/v0/maintenance/namespaces/demo/checkpoints?limit=1",
            harness.server_url
        ),
        "test-token",
    )
    .expect("raw checkpoint page");
    assert_eq!(raw.checkpoints.len(), 1);
    assert!(raw.next_cursor.is_some());

    harness
        .client
        .delete_namespace(&demo)
        .await
        .expect("delete namespace");
    let deleted_page = harness
        .client
        .list_checkpoints(&demo)
        .page_limit(page_limit(1))
        .cursor(Some(foreign_cursor.clone()))
        .page()
        .await
        .expect("resume checkpoints in deleted namespace");
    assert_eq!(deleted_page.checkpoints.len(), 1);
    harness.server.abort();
}

fn get_json<T: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
) -> Result<T, Box<ErrorResponse>> {
    retry_result_on_macos_teardown_einval(|| {
        let request = raw_agent()
            .get(url)
            .set("authorization", &format!("Bearer {auth_token}"));
        match request.call() {
            Ok(response) => serde_json::from_reader(response.into_reader()).map_err(|err| {
                Box::new(ErrorResponse {
                    code: "invalid_json".to_owned(),
                    feature: None,
                    message: err.to_string(),
                    param: None,
                    request_id: None,
                    details: None,
                })
            }),
            Err(ureq::Error::Status(_, response)) => Err(Box::new(
                serde_json::from_reader::<_, ErrorResponse>(response.into_reader()).unwrap_or_else(
                    |err| ErrorResponse {
                        code: "invalid_json".to_owned(),
                        feature: None,
                        message: err.to_string(),
                        param: None,
                        request_id: None,
                        details: None,
                    },
                ),
            )),
            Err(ureq::Error::Transport(error)) => Err(Box::new(ErrorResponse {
                code: "transport".to_owned(),
                feature: None,
                message: error.to_string(),
                param: None,
                request_id: None,
                details: None,
            })),
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_paginates_directory_listing_and_rejects_cursor_path_mismatch() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-directory-pagination",
    ))
    .await;

    harness
        .client
        .create_namespace(&namespace_id("demo"), &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let docs = NamespacePath::parse("demo", "/docs").expect("docs path");
    let other = NamespacePath::parse("demo", "/other").expect("other path");
    harness
        .client
        .create_directory(&docs, &loonfs_test_support::test_actor())
        .await
        .expect("create docs dir");
    harness
        .client
        .create_directory(&other, &loonfs_test_support::test_actor())
        .await
        .expect("create other dir");
    for name in ["a.txt", "b.txt", "c.txt"] {
        let path = NamespacePath::parse("demo", &format!("/docs/{name}")).expect("file path");
        harness
            .client
            .put_file_with_options(
                &path,
                name.as_bytes(),
                &loonfs_test_support::test_actor(),
                &replace_file_options(),
            )
            .await
            .expect("write file");
    }

    let first_page = harness
        .client
        .list(&docs)
        .page_limit(page_limit(2))
        .page()
        .await
        .expect("first directory page");
    assert_eq!(entry_names(&first_page), vec!["a.txt", "b.txt"]);
    let cursor = first_page.next_cursor.clone().expect("directory cursor");

    let second_page = harness
        .client
        .list(&docs)
        .page_limit(page_limit(2))
        .cursor(Some(cursor.clone()))
        .page()
        .await
        .expect("second directory page");
    assert_eq!(entry_names(&second_page), vec!["c.txt"]);
    assert_eq!(second_page.next_cursor, None);

    let full_listing = collect_path_entries(&harness.client, &docs, &Default::default())
        .await
        .expect("full directory list");
    assert_eq!(entry_names(&full_listing), vec!["a.txt", "b.txt", "c.txt"]);
    assert_eq!(full_listing.next_cursor, None);

    let mismatch = harness
        .client
        .list(&other)
        .page_limit(page_limit(2))
        .cursor(Some(cursor.clone()))
        .page()
        .await
        .expect_err("directory cursor must match listed path");
    match mismatch {
        ClientError::Api {
            status, response, ..
        } => {
            assert_eq!(status, 400);
            assert_eq!(response.code, "invalid_request");
        }
        other => panic!("expected cursor rejection, got {other:?}"),
    }

    let raw_first_page: ListPathEntriesResponse = get_json(
        &format!(
            "{}/v0/namespaces/demo/filesystem/entries?path=/docs&limit=1",
            harness.server_url
        ),
        "test-token",
    )
    .expect("raw first directory page");
    assert_eq!(raw_first_page.entries.len(), 1);
    assert!(raw_first_page.next_cursor.is_some());

    let nonnumeric_limit: Result<ListPathEntriesResponse, Box<ErrorResponse>> = get_json(
        &format!(
            "{}/v0/namespaces/demo/filesystem/entries?path=/docs&limit=not-a-number",
            harness.server_url
        ),
        "test-token",
    );
    let error = nonnumeric_limit.expect_err("nonnumeric limit rejected");
    assert_eq!(error.code, "invalid_request");
    assert_eq!(error.param.as_deref(), Some("limit"));

    harness.server.abort();
}

/// Returns the `param` of an `invalid_request` error.
fn error_param<T: std::fmt::Debug>(result: Result<T, ClientError>) -> Option<String> {
    match result.expect_err("request should fail") {
        ClientError::Api {
            status, response, ..
        } => {
            assert_eq!((status, response.code.as_str()), (400, "invalid_request"));
            response.param
        }
        other => panic!("expected API error, got {other:?}"),
    }
}

/// Checks that both list routes take `recursive`, that the client sends
/// the parameter and pages a recursive listing by path and by inode, and
/// that each kind of listing accepts only its own kind of cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_recursive_listing_round_trips_by_path_and_by_inode() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-recursive-listing",
    ))
    .await;
    let demo = namespace_id("demo");
    harness
        .client
        .create_namespace(&demo, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let docs = NamespacePath::parse("demo", "/docs").expect("docs path");
    for directory in ["/docs", "/docs/a"] {
        harness
            .client
            .create_directory(
                &NamespacePath::parse("demo", directory).expect("directory path"),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("create directory");
    }
    for file in ["/docs/a/x.txt", "/docs/b.txt"] {
        harness
            .client
            .put_file_with_options(
                &NamespacePath::parse("demo", file).expect("file path"),
                b"body",
                &loonfs_test_support::test_actor(),
                &replace_file_options(),
            )
            .await
            .expect("write file");
    }
    let recursive = ListOptions {
        recursive: true,
        ..Default::default()
    };
    let expected = ["/docs/a", "/docs/a/x.txt", "/docs/b.txt"];

    let mut by_path = Vec::new();
    let mut cursor = None;
    loop {
        let page = harness
            .client
            .list_with_options(&docs, &recursive)
            .page_limit(page_limit(1))
            .cursor(cursor)
            .page()
            .await
            .expect("recursive page by path");
        by_path.extend(page.entries.into_iter().map(|entry| entry.path.to_string()));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(by_path, expected);
    let docs_inode = harness
        .client
        .stat(&docs)
        .await
        .expect("stat docs")
        .inode_id;
    let by_inode = harness
        .client
        .list_by_inode_with_options(&demo, docs_inode, &recursive)
        .collect_up_to(10)
        .await
        .expect("recursive listing by inode");
    assert_eq!(
        by_inode
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        expected
    );

    for (issuing, resuming) in [
        (ListOptions::default(), recursive.clone()),
        (recursive.clone(), ListOptions::default()),
    ] {
        let cursor = harness
            .client
            .list_with_options(&docs, &issuing)
            .page_limit(page_limit(1))
            .page()
            .await
            .expect("first page")
            .next_cursor;
        let resumed = harness
            .client
            .list_with_options(&docs, &resuming)
            .page_limit(page_limit(1))
            .cursor(cursor)
            .page()
            .await;
        assert_eq!(error_param(resumed).as_deref(), Some("cursor"));
    }

    let raw: ListPathEntriesResponse = get_json(
        &format!(
            "{}/v0/namespaces/demo/filesystem/entries?path=/docs&recursive=true",
            harness.server_url
        ),
        "test-token",
    )
    .expect("raw recursive listing");
    assert_eq!(raw.entries.len(), expected.len());
    let invalid: Result<ListPathEntriesResponse, Box<ErrorResponse>> = get_json(
        &format!(
            "{}/v0/namespaces/demo/filesystem/entries?path=/docs&recursive=yes",
            harness.server_url
        ),
        "test-token",
    );
    let error = invalid.expect_err("a recursive value that isn't a boolean");
    assert_eq!(error.code, "invalid_request");
    assert_eq!(error.param.as_deref(), Some("recursive"));

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_client_listing_preserves_canonical_name_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-listing-order",
    ))
    .await;

    harness
        .client
        .create_namespace(&namespace_id("demo"), &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let docs = NamespacePath::parse("demo", "/docs").expect("docs path");
    harness
        .client
        .create_directory(&docs, &loonfs_test_support::test_actor())
        .await
        .expect("create docs dir");
    for name in ["B.txt", "a.txt", "c.txt"] {
        let path = NamespacePath::parse("demo", &format!("/docs/{name}")).expect("file path");
        harness
            .client
            .put_file_with_options(
                &path,
                name.as_bytes(),
                &loonfs_test_support::test_actor(),
                &replace_file_options(),
            )
            .await
            .expect("write file");
    }

    let listing = collect_path_entries(&harness.client, &docs, &Default::default())
        .await
        .expect("directory list");
    assert_eq!(entry_names(&listing), vec!["a.txt", "B.txt", "c.txt"]);

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_restore_revision_appends_new_head_and_reports_change() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-restore",
        "http-restore",
    ))
    .await;

    let namespace = namespace_id("demo");
    let target = NamespacePath::parse("demo", "/restore.txt").expect("target");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");

    harness
        .client
        .put_file_with_options(
            &target,
            b"first bytes\n",
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                commit: loonfs_types::options::CommitOptions {
                    commit_id: Some(
                        CommitId::parse("req-restore-create").expect("valid commit id"),
                    ),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("create file");
    let created = harness
        .client
        .stat(&target)
        .await
        .expect("stat created file");
    let inode_id = created.inode_id;
    let first_content_ref = created
        .content_ref()
        .cloned()
        .expect("created file content ref");

    let replace = harness
        .client
        .put_file_with_options(
            &target,
            b"second bytes\n",
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit: loonfs_types::options::CommitOptions {
                    commit_id: Some(
                        CommitId::parse("req-restore-replace").expect("valid commit id"),
                    ),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("replace file");
    assert_eq!(replace.committed_seq, ChangeSeq(2));

    let restore = harness
        .client
        .restore_revision_with_options(
            &target,
            RevisionNo(1),
            &loonfs_test_support::test_actor(),
            &loonfs_types::options::CommitOptions {
                preconditions: Vec::new(),
                commit_id: Some(CommitId::parse("req-restore-restore").expect("valid commit id")),
                message: Some("restore revision".to_owned()),
            },
        )
        .await
        .expect("restore revision");
    assert_eq!(restore.committed_seq, ChangeSeq(3));

    let entry = harness
        .client
        .stat(&target)
        .await
        .expect("stat restored file");
    assert_eq!(entry.inode_id, inode_id);
    assert_eq!(entry.content_ref(), Some(&first_content_ref));
    let bytes = harness
        .client
        .read_file(&target)
        .await
        .expect("read restored file");
    assert_eq!(bytes, b"first bytes\n");

    let changes = harness
        .client
        .list_changes(&namespace, ChangeSeq(0))
        .page()
        .await
        .expect("list changes");
    assert_eq!(changes.changes.len(), 3);
    assert_eq!(
        changes.changes[2].commit_id,
        CommitId::parse("req-restore-restore").expect("valid commit id")
    );
    // Restoring a revision emits the same event as a regular content update.
    assert_eq!(changes.changes[2].events.len(), 1);
    assert_matches!(
        &changes.changes[2].events[0],
        FilesystemChange::ContentChanged {
            inode_id: event_inode,
            revision_no,
            content_ref,
            append_offset: None,
        } if *event_inode == inode_id
            && *revision_no == RevisionNo(3)
            && *content_ref == first_content_ref
    );

    let first_page = harness
        .client
        .list_changes(&namespace, ChangeSeq(0))
        .page_limit(page_limit(2))
        .page()
        .await
        .expect("list first changes page");
    assert_eq!(first_page.after_seq, ChangeSeq(0));
    assert_eq!(first_page.head_seq, ChangeSeq(3));
    assert_eq!(first_page.next_after_seq, Some(ChangeSeq(2)));
    assert_eq!(first_page.changes.len(), 2);

    let second_page = harness
        .client
        .list_changes(&namespace, first_page.next_after_seq.expect("next page"))
        .page_limit(page_limit(2))
        .page()
        .await
        .expect("list second changes page");
    assert_eq!(second_page.after_seq, ChangeSeq(2));
    assert_eq!(second_page.head_seq, ChangeSeq(3));
    assert_eq!(second_page.next_after_seq, None);
    assert_eq!(
        second_page
            .changes
            .iter()
            .map(|change| change.committed_seq)
            .collect::<Vec<_>>(),
        vec![ChangeSeq(3)]
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_revision_routes_list_read_and_restore_by_path() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-revisions",
        "http-revisions",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/rev.txt").expect("target");
    harness
        .client
        .put_file(&target, b"one", &loonfs_test_support::test_actor())
        .await
        .expect("create file");
    harness
        .client
        .put_file_with_options(
            &target,
            b"two",
            &loonfs_test_support::test_actor(),
            &replace_file_options(),
        )
        .await
        .expect("replace file");

    let entry = harness.client.stat(&target).await.expect("stat file");
    let revisions = harness
        .client
        .list_file_revisions(&target)
        .page()
        .await
        .expect("path revisions");
    assert_eq!(revisions.inode_id, entry.inode_id);
    assert_eq!(revisions.revisions.len(), 2);
    assert_eq!(
        harness
            .client
            .read_file_revision(&target, RevisionNo(1))
            .await
            .expect("read path revision"),
        b"one"
    );

    let moved = NamespacePath::parse("demo", "/docs/moved.txt").expect("moved");
    harness
        .client
        .move_path_with_options(
            &target,
            &moved,
            &loonfs_test_support::test_actor(),
            &MoveOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_types::options::CommitOptions {
                    preconditions: Vec::new(),
                    commit_id: None,
                    message: None,
                },
                expected_destination_inode_id: None,
                expected_destination_revision_no: None,
            },
        )
        .await
        .expect("move path");
    assert_matches!(
        harness.client.list_file_revisions(&target).page().await,
        Err(ClientError::Api { response, .. }) if response.code == "path_not_found"
    );
    // Revision history follows the inode after a move.
    let moved_revisions = harness
        .client
        .list_file_revisions(&moved)
        .page()
        .await
        .expect("moved-path revisions");
    assert_eq!(moved_revisions.inode_id, entry.inode_id);
    assert_eq!(moved_revisions.revisions.len(), 2);

    harness
        .client
        .restore_revision(&moved, RevisionNo(1), &loonfs_test_support::test_actor())
        .await
        .expect("path restore");
    assert_eq!(
        harness
            .client
            .read_file(&moved)
            .await
            .expect("read restored file"),
        b"one"
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_restore_revision_missing_source_returns_revision_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-restore-missing-source",
        "http-restore-missing-source",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/restore.txt").expect("target");
    harness
        .client
        .put_file(
            &target,
            b"first bytes\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("create file");

    match harness
        .client
        .restore_revision_with_options(
            &target,
            RevisionNo(99),
            &loonfs_test_support::test_actor(),
            &loonfs_types::options::CommitOptions {
                preconditions: Vec::new(),
                commit_id: Some(
                    CommitId::parse("req-restore-missing-source-restore").expect("valid commit id"),
                ),
                message: None,
            },
        )
        .await
    {
        Err(ClientError::Api {
            status, response, ..
        }) => {
            assert_eq!(status, 404);
            assert_eq!(response.code, "revision_not_found");
        }
        other => panic!("expected revision_not_found, got {other:?}"),
    }

    harness.server.abort();
}
