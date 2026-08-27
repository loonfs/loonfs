//! HTTP directory and revision pagination behavior.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::{collect_checkpoints, collect_path_entries, start_server};
use loonfs_api::{
    v0::FilesystemChange, ApiError, ChangeSeq, CommitId, CreateCheckpointRequest,
    DestinationBehavior, ListCheckpointsResponse, ListPathEntriesResponse, RevisionNo,
    DEFAULT_MAX_PAGE_LIMIT,
};
use loonfs_client::{
    ClientError, CreateDirectoryOptions, MoveOptions, NamespacePath, PutFileOptions,
    RestoreRevisionOptions,
};
use loonfs_test_support::http::{raw_agent, retry_result_on_macos_teardown_einval};
use loonfs_test_support::ids::namespace_id;
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
    match result.expect_err("request should be rejected") {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 400);
            assert_eq!(code, "invalid_request");
        }
        other => panic!("expected invalid_request API error, got {other:?}"),
    }
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
            .create_namespace(namespace_id)
            .await
            .expect("create namespace");
    }

    let mut expected_ids = Vec::new();
    for index in 0..5 {
        expected_ids.push(
            harness
                .client
                .create_checkpoint(
                    &demo,
                    &CreateCheckpointRequest {
                        name: format!("pin-{index}"),
                        ttl_ms: None,
                    },
                )
                .await
                .expect("create checkpoint")
                .checkpoint_id,
        );
    }
    expected_ids.sort();

    let mut actual_ids = Vec::new();
    let mut cursor = None;
    loop {
        let page = harness
            .client
            .list_checkpoints_page(&demo, Some(2), cursor.as_deref())
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
        .list_checkpoints_page(&demo, Some(1), None)
        .await
        .expect("first checkpoint page");
    let foreign_cursor = first.next_cursor.expect("checkpoint cursor");
    assert_invalid_request(
        harness
            .client
            .list_checkpoints_page(&other, Some(1), Some(&foreign_cursor))
            .await,
    );
    assert_invalid_request(
        harness
            .client
            .list_checkpoints_page(&demo, Some(1), Some("not-a-cursor"))
            .await,
    );
    assert_invalid_request(
        harness
            .client
            .list_checkpoints_page(&demo, Some(0), None)
            .await,
    );
    assert_invalid_request(
        harness
            .client
            .list_checkpoints_page(&demo, Some(DEFAULT_MAX_PAGE_LIMIT + 1), None)
            .await,
    );

    let raw: ListCheckpointsResponse = get_json(
        &format!(
            "{}/v0/admin/namespaces/demo/checkpoints?limit=1",
            harness.server_url
        ),
        "test-token",
    )
    .expect("raw checkpoint page");
    assert_eq!(raw.checkpoints.len(), 1);
    assert!(raw.next_cursor.is_some());

    harness.server.abort();
}

fn get_json<T: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
) -> Result<T, Box<ApiError>> {
    retry_result_on_macos_teardown_einval(|| {
        let request = raw_agent()
            .get(url)
            .set("authorization", &format!("Bearer {auth_token}"));
        match request.call() {
            Ok(response) => serde_json::from_reader(response.into_reader()).map_err(|err| {
                Box::new(ApiError {
                    code: "invalid_json".to_owned(),
                    feature: None,
                    message: err.to_string(),
                    param: None,
                    request_id: None,
                    details: None,
                })
            }),
            Err(ureq::Error::Status(_, response)) => Err(Box::new(
                serde_json::from_reader::<_, ApiError>(response.into_reader()).unwrap_or_else(
                    |err| ApiError {
                        code: "invalid_json".to_owned(),
                        feature: None,
                        message: err.to_string(),
                        param: None,
                        request_id: None,
                        details: None,
                    },
                ),
            )),
            Err(ureq::Error::Transport(error)) => Err(Box::new(ApiError {
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
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let docs = NamespacePath::parse("demo", "/docs").expect("docs path");
    let other = NamespacePath::parse("demo", "/other").expect("other path");
    harness
        .client
        .create_directory(
            &docs,
            &CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create docs dir");
    harness
        .client
        .create_directory(
            &other,
            &CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create other dir");
    for name in ["a.txt", "b.txt", "c.txt"] {
        let path = NamespacePath::parse("demo", &format!("/docs/{name}")).expect("file path");
        harness
            .client
            .put_file_bytes(&path, name.as_bytes(), &replace_file_options())
            .await
            .expect("write file");
    }

    let first_page = harness
        .client
        .list_path_entries_page(&docs, Some(2), None, &Default::default())
        .await
        .expect("first directory page");
    assert_eq!(entry_names(&first_page), vec!["a.txt", "b.txt"]);
    let cursor = first_page.next_cursor.clone().expect("directory cursor");

    let second_page = harness
        .client
        .list_path_entries_page(&docs, Some(2), Some(&cursor), &Default::default())
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
        .list_path_entries_page(&other, Some(2), Some(&cursor), &Default::default())
        .await
        .expect_err("directory cursor must match listed path");
    match mismatch {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 400);
            assert_eq!(code, "invalid_request");
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

    let nonnumeric_limit: Result<ListPathEntriesResponse, Box<ApiError>> = get_json(
        &format!(
            "{}/v0/namespaces/demo/filesystem/entries?path=/docs&limit=not-a-number",
            harness.server_url
        ),
        "test-token",
    );
    let error = nonnumeric_limit.expect_err("nonnumeric limit rejected");
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("invalid limit"));

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
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let docs = NamespacePath::parse("demo", "/docs").expect("docs path");
    harness
        .client
        .create_directory(
            &docs,
            &CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create docs dir");
    for name in ["B.txt", "a.txt", "c.txt"] {
        let path = NamespacePath::parse("demo", &format!("/docs/{name}")).expect("file path");
        harness
            .client
            .put_file_bytes(&path, name.as_bytes(), &replace_file_options())
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
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    harness
        .client
        .put_file_bytes(
            &target,
            b"first bytes\n",
            &PutFileOptions {
                commit: loonfs_api::options::CommitOptions {
                    commit_id: Some(
                        CommitId::parse("req-restore-create").expect("valid commit id"),
                    ),
                    ..loonfs_api::options::CommitOptions::new(loonfs_test_support::test_actor())
                },
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("create file");
    let created = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat created file");
    let inode_id = created.inode_id;
    let first_content_ref = created
        .content_ref()
        .cloned()
        .expect("created file content ref");

    let replace = harness
        .client
        .put_file_bytes(
            &target,
            b"second bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit: loonfs_api::options::CommitOptions {
                    commit_id: Some(
                        CommitId::parse("req-restore-replace").expect("valid commit id"),
                    ),
                    ..loonfs_api::options::CommitOptions::new(loonfs_test_support::test_actor())
                },
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace file");
    assert_eq!(replace.committed_seq, ChangeSeq(2));

    let restore = harness
        .client
        .restore_file_revision(
            &target,
            RevisionNo(1),
            &RestoreRevisionOptions {
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(
                        CommitId::parse("req-restore-restore").expect("valid commit id"),
                    ),
                    message: Some("restore revision".to_owned()),
                },
            },
        )
        .await
        .expect("restore revision");
    assert_eq!(restore.committed_seq, ChangeSeq(3));

    let entry = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat restored file");
    assert_eq!(entry.inode_id, inode_id);
    assert_eq!(entry.content_ref(), Some(&first_content_ref));
    let bytes = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read restored file");
    assert_eq!(bytes, b"first bytes\n");

    let changes = harness
        .client
        .list_changes(&namespace, ChangeSeq(0), None)
        .await
        .expect("list changes");
    assert_eq!(changes.changes.len(), 3);
    assert_eq!(
        changes.changes[2].commit_id,
        CommitId::parse("req-restore-restore").expect("valid commit id")
    );
    // Restoring a revision emits the same event as a regular content update.
    assert_eq!(changes.changes[2].events.len(), 1);
    assert!(matches!(
        &changes.changes[2].events[0],
        FilesystemChange::ContentChanged {
            inode_id: event_inode,
            revision_no,
            content_ref,
        } if *event_inode == inode_id
            && *revision_no == RevisionNo(3)
            && *content_ref == first_content_ref
    ));

    let first_page = harness
        .client
        .list_changes(&namespace, ChangeSeq(0), Some(2))
        .await
        .expect("list first changes page");
    assert_eq!(first_page.after_seq, ChangeSeq(0));
    assert_eq!(first_page.through_seq, ChangeSeq(2));
    assert_eq!(first_page.next_after_seq, Some(ChangeSeq(2)));
    assert_eq!(first_page.changes.len(), 2);

    let second_page = harness
        .client
        .list_changes(
            &namespace,
            first_page.next_after_seq.expect("next page"),
            Some(2),
        )
        .await
        .expect("list second changes page");
    assert_eq!(second_page.after_seq, ChangeSeq(2));
    assert_eq!(second_page.through_seq, ChangeSeq(3));
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
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/rev.txt").expect("target");
    harness
        .client
        .put_file_bytes(
            &target,
            b"one",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create file");
    harness
        .client
        .put_file_bytes(&target, b"two", &replace_file_options())
        .await
        .expect("replace file");

    let entry = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat file");
    let revisions = harness
        .client
        .list_file_revisions_page(&target, None, None)
        .await
        .expect("path revisions");
    assert_eq!(revisions.inode_id, entry.inode_id);
    assert_eq!(revisions.revisions.len(), 2);
    assert_eq!(
        harness
            .client
            .get_file_revision_bytes(&target, RevisionNo(1))
            .await
            .expect("read path revision"),
        b"one"
    );

    let moved = NamespacePath::parse("demo", "/docs/moved.txt").expect("moved");
    harness
        .client
        .move_path(
            &target,
            &moved,
            &MoveOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: None,
                    message: None,
                },
            },
        )
        .await
        .expect("move path");
    assert!(matches!(
        harness.client.list_file_revisions_page(&target, None, None).await,
        Err(ClientError::Api { code, .. }) if code == "path_not_found"
    ));
    // Revision history follows the inode after a move.
    let moved_revisions = harness
        .client
        .list_file_revisions_page(&moved, None, None)
        .await
        .expect("moved-path revisions");
    assert_eq!(moved_revisions.inode_id, entry.inode_id);
    assert_eq!(moved_revisions.revisions.len(), 2);

    harness
        .client
        .restore_file_revision(
            &moved,
            RevisionNo(1),
            &RestoreRevisionOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("path restore");
    assert_eq!(
        harness
            .client
            .get_file_bytes(&moved)
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
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/restore.txt").expect("target");
    harness
        .client
        .put_file_bytes(
            &target,
            b"first bytes\n",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create file");

    match harness
        .client
        .restore_file_revision(
            &target,
            RevisionNo(99),
            &RestoreRevisionOptions {
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(
                        CommitId::parse("req-restore-missing-source-restore")
                            .expect("valid commit id"),
                    ),
                    message: None,
                },
            },
        )
        .await
    {
        Err(ClientError::Api { status, code, .. }) => {
            assert_eq!(status, 404);
            assert_eq!(code, "revision_not_found");
        }
        other => panic!("expected revision_not_found, got {other:?}"),
    }

    harness.server.abort();
}
