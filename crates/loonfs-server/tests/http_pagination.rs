//! HTTP directory and revision pagination behavior.

mod common;

use common::http_split_support::*;
use common::start_server;
use loonfs_api::{
    v0::{CommitDelta, CommitOp, CommitRequest as ApiCommitRequest, CommitSubmissionRequest},
    ApiError, ChangeSeq, CommitId, DestinationBehavior, InodeId, ListPathEntriesResponse,
    RevisionNo,
};
use loonfs_client::{
    ClientError, CreateDirectoryOptions, MutationOptions, NamespacePath, PutFileOptions,
};
use loonfs_test_support::http::raw_agent;
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

fn get_json<T: serde::de::DeserializeOwned>(url: &str, auth_token: &str) -> Result<T, ApiError> {
    let request = raw_agent()
        .get(url)
        .set("authorization", &format!("Bearer {auth_token}"));
    match request.call() {
        Ok(response) => serde_json::from_reader(response.into_reader()).map_err(|err| ApiError {
            code: "invalid_json".to_owned(),
            feature: None,
            message: err.to_string(),
            request_id: None,
            details: None,
        }),
        Err(ureq::Error::Status(_, response)) => Err(serde_json::from_reader::<_, ApiError>(
            response.into_reader(),
        )
        .unwrap_or_else(|err| ApiError {
            code: "invalid_json".to_owned(),
            feature: None,
            message: err.to_string(),
            request_id: None,
            details: None,
        })),
        Err(ureq::Error::Transport(error)) => Err(ApiError {
            code: "transport".to_owned(),
            feature: None,
            message: error.to_string(),
            request_id: None,
            details: None,
        }),
    }
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
        .create_directory(&docs, &CreateDirectoryOptions::default())
        .await
        .expect("create docs dir");
    harness
        .client
        .create_directory(&other, &CreateDirectoryOptions::default())
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
        .list_path_entries_page(&docs, Some(2), None)
        .await
        .expect("first directory page");
    assert_eq!(entry_names(&first_page), vec!["a.txt", "b.txt"]);
    let cursor = first_page.next_cursor.clone().expect("directory cursor");

    let second_page = harness
        .client
        .list_path_entries_page(&docs, Some(2), Some(&cursor))
        .await
        .expect("second directory page");
    assert_eq!(entry_names(&second_page), vec!["c.txt"]);
    assert_eq!(second_page.next_cursor, None);

    let full_listing = harness
        .client
        .list_path_entries_all(&docs)
        .await
        .expect("full directory list");
    assert_eq!(entry_names(&full_listing), vec!["a.txt", "b.txt", "c.txt"]);
    assert_eq!(full_listing.next_cursor, None);

    let mismatch = harness
        .client
        .list_path_entries_page(&other, Some(2), Some(&cursor))
        .await
        .expect_err("directory cursor must match listed path");
    match mismatch {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 400);
            assert_eq!(code, "invalid_request");
        }
        other => unreachable!("expected cursor rejection, got {other:?}"),
    }

    let raw_first_page: ListPathEntriesResponse = get_json(
        &format!(
            "{}/v0/namespaces/demo/filesystem/list?path=/docs&limit=1",
            harness.server_url
        ),
        "test-token",
    )
    .expect("raw first directory page");
    assert_eq!(raw_first_page.entries.len(), 1);
    assert!(raw_first_page.next_cursor.is_some());

    let nonnumeric_limit: Result<ListPathEntriesResponse, ApiError> = get_json(
        &format!(
            "{}/v0/namespaces/demo/filesystem/list?path=/docs&limit=not-a-number",
            harness.server_url
        ),
        "test-token",
    );
    let error = nonnumeric_limit.expect_err("nonnumeric limit rejected");
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("invalid limit"));

    harness.server.abort();
}

/// The client aggregates pages without re-ordering: mixed-case names must come
/// back in the server's canonical casefolded name-key order, not in raw
/// display-name byte order (`B.txt` sorts after `a.txt`, not before).
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
        .create_directory(&docs, &CreateDirectoryOptions::default())
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

    let listing = harness
        .client
        .list_path_entries_all(&docs)
        .await
        .expect("directory list");
    assert_eq!(entry_names(&listing), vec!["a.txt", "B.txt", "c.txt"]);

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_restore_revision_appends_new_head_and_reports_change() {
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

    let first = stage_uploaded_content(&harness.client, &namespace, b"first bytes\n").await;
    let first_content_ref = first.content_ref.clone();
    harness
        .client
        .commit_operations(
            &namespace,
            &CommitSubmissionRequest {
                commit: ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-create").expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("restore.txt")
                            .expect("valid display name"),
                        content_ref: first_content_ref.clone(),
                    }],
                    message: None,
                },
                content_tokens: vec![validated_content_token(&first)],
            },
        )
        .await
        .expect("create file");
    let inode_id = harness
        .client
        .stat_path(&target)
        .await
        .expect("stat created file")
        .inode_id;

    let second = stage_uploaded_content(&harness.client, &namespace, b"second bytes\n").await;
    let second_content_ref = second.content_ref.clone();
    let replace = harness
        .client
        .commit_operations(
            &namespace,
            &CommitSubmissionRequest {
                commit: ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-replace").expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::ReplaceFile {
                        inode_id,
                        base_revision_no: RevisionNo(1),
                        content_ref: second_content_ref.clone(),
                    }],
                    message: None,
                },
                content_tokens: vec![validated_content_token(&second)],
            },
        )
        .await
        .expect("replace file");
    assert_eq!(replace.committed_seq, ChangeSeq(2));

    let restore = harness
        .client
        .commit_operations(
            &namespace,
            &CommitSubmissionRequest {
                commit: ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-restore").expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::RestoreRevision {
                        inode_id,
                        source_revision_no: RevisionNo(1),
                        base_revision_no: RevisionNo(2),
                    }],
                    message: Some("restore revision".to_owned()),
                },
                content_tokens: Vec::new(),
            },
        )
        .await
        .expect("restore revision");
    assert_eq!(restore.committed_seq, ChangeSeq(3));

    let entry = harness
        .client
        .stat_path(&target)
        .await
        .expect("stat restored file");
    assert_eq!(entry.inode_id, inode_id);
    assert_eq!(entry.content_ref.as_ref(), Some(&first_content_ref));
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
    assert!(matches!(
        &changes.changes[2].deltas[0],
        CommitDelta::AppendFileRevision {
            semantic_op_index: 0,
            inode_id: delta_inode,
            revision_no,
            content_ref,
            ..
        } if *delta_inode == inode_id
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
            .map(|change| change.seq)
            .collect::<Vec<_>>(),
        vec![ChangeSeq(3)]
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_revision_routes_list_read_and_restore_by_path_and_inode() {
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
        .put_file_bytes(&target, b"one", &PutFileOptions::default())
        .await
        .expect("create file");
    harness
        .client
        .put_file_bytes(&target, b"two", &replace_file_options())
        .await
        .expect("replace file");

    let entry = harness.client.stat_path(&target).await.expect("stat file");
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
            DestinationBehavior::NoReplace,
            &MutationOptions::default(),
        )
        .await
        .expect("move path");
    assert!(matches!(
        harness.client.list_file_revisions_page(&target, None, None).await,
        Err(ClientError::Api { code, .. }) if code == "path_not_found"
    ));
    let inode_revisions = harness
        .client
        .list_file_revisions_by_inode_page(&namespace, entry.inode_id, None, None)
        .await
        .expect("inode revisions");
    assert_eq!(inode_revisions.revisions.len(), 2);
    assert_eq!(
        harness
            .client
            .get_file_revision_bytes_by_inode(&namespace, entry.inode_id, RevisionNo(2))
            .await
            .expect("read inode revision"),
        b"two"
    );

    harness
        .client
        .restore_file_revision(&moved, RevisionNo(1), &MutationOptions::default())
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
    harness
        .client
        .restore_file_revision_by_inode(
            &namespace,
            entry.inode_id,
            RevisionNo(2),
            RevisionNo(3),
            &CommitId::parse("c_restore_file_revision_0001").expect("valid commit id"),
        )
        .await
        .expect("inode restore");
    assert_eq!(
        harness
            .client
            .get_file_bytes(&moved)
            .await
            .expect("read inode-restored file"),
        b"two"
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_restore_revision_missing_source_returns_revision_not_found() {
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

    let first = stage_uploaded_content(&harness.client, &namespace, b"first bytes\n").await;
    harness
        .client
        .commit_operations(
            &namespace,
            &CommitSubmissionRequest {
                commit: ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-missing-source-create")
                        .expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("restore.txt")
                            .expect("valid display name"),
                        content_ref: first.content_ref.clone(),
                    }],
                    message: None,
                },
                content_tokens: vec![validated_content_token(&first)],
            },
        )
        .await
        .expect("create file");
    let inode_id = harness
        .client
        .stat_path(&target)
        .await
        .expect("stat created file")
        .inode_id;

    match harness
        .client
        .commit_operations(
            &namespace,
            &CommitSubmissionRequest {
                commit: ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-missing-source-restore")
                        .expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::RestoreRevision {
                        inode_id,
                        source_revision_no: RevisionNo(99),
                        base_revision_no: RevisionNo(1),
                    }],
                    message: None,
                },
                content_tokens: Vec::new(),
            },
        )
        .await
    {
        Err(ClientError::Api { status, code, .. }) => {
            assert_eq!(status, 404);
            assert_eq!(code, "revision_not_found");
        }
        other => unreachable!("expected revision_not_found, got {other:?}"),
    }

    harness.server.abort();
}
