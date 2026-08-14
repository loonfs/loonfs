//! HTTP stat, revision history, and content reads by inode id.

use crate::common::http_split_support::{replace_file_options, test_config};
use crate::common::start_server;
use loonfs_api::{ApiError, ErrorCode, InodeId, RevisionNo};
use loonfs_client::{
    ClientError, CreateDirectoryOptions, DeleteOptions, MoveOptions, NamespacePath, PutFileOptions,
};
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use serde_json::Value;
use tempfile::tempdir;

fn assert_api_code<T: std::fmt::Debug>(
    result: Result<T, ClientError>,
    expected_status: u16,
    code: ErrorCode,
) {
    match result.expect_err("request should fail") {
        ClientError::Api {
            status,
            code: actual,
            ..
        } => {
            assert_eq!(status, expected_status);
            assert_eq!(actual, code.as_str());
            assert_ne!(actual, ErrorCode::PathNotFound.as_str());
        }
        other => unreachable!("expected API error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_stat_inode_tracks_renames_and_revision_reads_survive_deletion() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "http-inode-rename",
        "http-inode-rename",
    ))
    .await;
    let namespace = namespace_id("demo");
    let actor = loonfs_test_support::test_actor();
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let before = NamespacePath::parse("demo", "/before.txt").expect("before path");
    let after = NamespacePath::parse("demo", "/after.txt").expect("after path");
    harness
        .client
        .put_file_bytes(&before, b"one", &PutFileOptions::new(actor.clone()))
        .await
        .expect("put revision one");
    harness
        .client
        .put_file_bytes(&before, b"two", &replace_file_options())
        .await
        .expect("put revision two");

    let by_path = harness
        .client
        .stat_path(&before, &Default::default())
        .await
        .expect("stat path");
    let inode_id = by_path.inode_id;
    assert_eq!(
        harness
            .client
            .stat_inode(&namespace, inode_id, &Default::default())
            .await
            .expect("stat inode before rename"),
        by_path
    );
    assert_eq!(
        harness
            .client
            .get_file_revision_bytes_by_inode(&namespace, inode_id, RevisionNo(1))
            .await
            .expect("read inode revision before rename"),
        b"one"
    );

    harness
        .client
        .move_path(&before, &after, &MoveOptions::new(actor.clone()))
        .await
        .expect("rename file");
    let renamed = harness
        .client
        .stat_inode(&namespace, inode_id, &Default::default())
        .await
        .expect("stat inode after rename");
    assert_eq!(renamed.inode_id, inode_id);
    assert_eq!(renamed.path.as_str(), "/after.txt");
    assert_eq!(
        renamed,
        harness
            .client
            .stat_path(&after, &Default::default())
            .await
            .expect("stat renamed path")
    );
    let revisions = harness
        .client
        .list_file_revisions_by_inode_page(&namespace, inode_id, Some(1), None)
        .await
        .expect("first inode revision page");
    assert_eq!(revisions.inode_id, inode_id);
    assert_eq!(revisions.revisions.len(), 1);
    assert!(revisions.next_cursor.is_some());

    let raw: Value = serde_json::from_reader(
        raw_agent()
            .get(&format!(
                "{}/v0/namespaces/{namespace}/inodes/{}/revisions",
                harness.server_url,
                loonfs_api::public_inode_id::encode(inode_id)
            ))
            .set("authorization", "Bearer test-token")
            .call()
            .expect("raw revision listing")
            .into_reader(),
    )
    .expect("decode raw revision listing");
    assert!(raw.get("path").is_none());

    harness
        .client
        .delete_path(&after, &DeleteOptions::new(actor))
        .await
        .expect("delete file");
    assert_api_code(
        harness
            .client
            .stat_inode(&namespace, inode_id, &Default::default())
            .await,
        404,
        ErrorCode::InodeNotFound,
    );
    assert_eq!(
        harness
            .client
            .get_file_revision_bytes_by_inode(&namespace, inode_id, RevisionNo(2))
            .await
            .expect("read retained deleted revision"),
        b"two"
    );
    assert_eq!(
        harness
            .client
            .list_file_revisions_by_inode_page(&namespace, inode_id, None, None)
            .await
            .expect("list retained deleted revisions")
            .revisions
            .len(),
        2
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_inode_read_errors_use_identity_codes_and_root_is_nameless() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "http-inode-errors",
        "http-inode-errors",
    ))
    .await;
    let namespace = namespace_id("demo");
    let actor = loonfs_test_support::test_actor();
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let root = harness
        .client
        .stat_inode(&namespace, InodeId(1), &Default::default())
        .await
        .expect("stat root inode");
    assert_eq!(root.path.as_str(), "/");
    assert_eq!(root.parent_inode_id, None);
    assert_eq!(root.display_name, None);

    let directory = NamespacePath::parse("demo", "/docs").expect("directory path");
    harness
        .client
        .create_directory(&directory, &CreateDirectoryOptions::new(actor.clone()))
        .await
        .expect("create directory");
    let directory_id = harness
        .client
        .stat_path(&directory, &Default::default())
        .await
        .expect("stat directory")
        .inode_id;
    assert_api_code(
        harness
            .client
            .list_file_revisions_by_inode_page(&namespace, directory_id, None, None)
            .await,
        409,
        ErrorCode::PathConflict,
    );
    for result in [
        harness
            .client
            .stat_inode(&namespace, InodeId(u64::MAX), &Default::default())
            .await
            .map(|_| ()),
        harness
            .client
            .get_file_revision_bytes_by_inode(&namespace, InodeId(u64::MAX), RevisionNo(1))
            .await
            .map(|_| ()),
    ] {
        assert_api_code(result, 404, ErrorCode::InodeNotFound);
    }

    let file = NamespacePath::parse("demo", "/file.txt").expect("file path");
    harness
        .client
        .put_file_bytes(&file, b"body", &PutFileOptions::new(actor))
        .await
        .expect("put file");
    let file_id = harness
        .client
        .stat_path(&file, &Default::default())
        .await
        .expect("stat file")
        .inode_id;
    assert_api_code(
        harness
            .client
            .get_file_revision_bytes_by_inode(&namespace, file_id, RevisionNo(999))
            .await,
        404,
        ErrorCode::RevisionNotFound,
    );

    let deleted_namespace = namespace_id("deleted");
    harness
        .client
        .create_namespace(&deleted_namespace)
        .await
        .expect("create deleted namespace");
    harness
        .client
        .delete_namespace(&deleted_namespace, None)
        .await
        .expect("delete namespace");
    assert_api_code(
        harness
            .client
            .stat_inode(&deleted_namespace, InodeId(1), &Default::default())
            .await,
        410,
        ErrorCode::NamespaceDeleted,
    );

    let strict_body = raw_agent()
        .post(&format!(
            "{}/v0/namespaces/{namespace}/inodes/{}/revisions/1/downloads",
            harness.server_url,
            loonfs_api::public_inode_id::encode(file_id)
        ))
        .set("authorization", "Bearer test-token")
        .send_json(serde_json::json!({ "path": "/file.txt" }))
        .expect_err("inode download body rejects fields");
    let ureq::Error::Status(status, response) = strict_body else {
        unreachable!("expected status response")
    };
    assert_eq!(status, 400);
    let error: ApiError =
        serde_json::from_reader(response.into_reader()).expect("decode strict-body error");
    assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());

    assert_api_code(
        harness
            .client
            .begin_download_by_inode(&namespace, file_id, RevisionNo(1))
            .await,
        501,
        ErrorCode::NotSupported,
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inode_routes_reject_noncanonical_ids_after_authorization() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "http-inode-codec",
        "http-inode-codec",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let actor = loonfs_test_support::test_actor();
    let mut inode_27_path = None;
    for index in 0..26 {
        let path = NamespacePath::parse("demo", &format!("/file-{index}.txt")).expect("seed path");
        harness
            .client
            .put_file_bytes(&path, b"body", &PutFileOptions::new(actor.clone()))
            .await
            .expect("seed inode");
        inode_27_path = Some(path);
    }
    let inode_27_path = inode_27_path.expect("seeded file");
    assert_eq!(
        harness
            .client
            .stat_path(&inode_27_path, &Default::default())
            .await
            .expect("stat ino_27")
            .inode_id,
        InodeId(27)
    );
    for suffix in ["", "/revisions", "/revisions/1/content"] {
        raw_agent()
            .get(&format!(
                "{}/v0/namespaces/demo/inodes/ino_27{suffix}",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .call()
            .expect("canonical ino_27 route");
    }

    let routes = [
        ("GET", ""),
        ("GET", "/revisions"),
        ("GET", "/revisions/1/content"),
        ("POST", "/revisions/1/downloads"),
    ];
    for malformed in ["27", "ino_027", "ino_0", "INO_27"] {
        for (method, suffix) in routes {
            let url = format!(
                "{}/v0/namespaces/demo/inodes/{malformed}{suffix}",
                harness.server_url
            );
            let request = raw_agent()
                .request(method, &url)
                .set("authorization", "Bearer test-token");
            let result = if method == "POST" {
                request.send_json(serde_json::json!({}))
            } else {
                request.call()
            };
            let ureq::Error::Status(status, response) =
                result.expect_err("malformed inode route should fail")
            else {
                unreachable!("expected status response")
            };
            assert_eq!(status, 400, "{method} {url}");
            let error: ApiError = serde_json::from_reader(response.into_reader())
                .expect("decode malformed-inode error");
            assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
            assert!(
                error.message.starts_with("path.inode_id"),
                "{}",
                error.message
            );
            assert!(
                error
                    .message
                    .contains("must match `ino_<decimal>` with no leading zeroes"),
                "{}",
                error.message
            );

            let unauthorized = raw_agent().request(method, &url);
            let result = if method == "POST" {
                unauthorized.send_json(serde_json::json!({}))
            } else {
                unauthorized.call()
            };
            let ureq::Error::Status(status, response) =
                result.expect_err("unauthorized malformed inode route should fail")
            else {
                unreachable!("expected status response")
            };
            assert_eq!(status, 401, "{method} {url}");
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode unauthorized error");
            assert_eq!(error.code, ErrorCode::Unauthorized.as_str());
        }
    }

    harness.server.abort();
}
