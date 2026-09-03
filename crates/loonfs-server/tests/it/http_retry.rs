//! HTTP idempotency, replay, and writer fencing behavior.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs_api::v0::{
    AdvanceRetentionRequest, CreateCheckpointRequest, MaintenanceRunRequest, MaintenanceRunResponse,
};
use loonfs_api::{
    AbsolutePath, ActorId, ActorRef, ChangeSeq, CommitId, CommitRequest, DestinationBehavior,
    FilesystemOperation, RevisionNo,
};
use loonfs_client::{
    ClientError, CopyOptions, CreateDirectoryOptions, DeleteOptions, MoveOptions, NamespacePath,
    PutFileOptions,
};
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_operation_rejects_same_commit_id_with_different_payload() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current-conflict",
        "http-current-conflict",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let commit_id = CommitId::parse("req-phase-2a-conflict").expect("valid commit id");
    let first = harness
        .client
        .put_file_bytes(
            &NamespacePath::parse("demo", "/first.txt").expect("first target"),
            b"first payload\n",
            &PutFileOptions {
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(commit_id.clone()),
                    message: Some("first commit".to_owned()),
                },
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("first put");

    match harness
        .client
        .put_file_bytes(
            &NamespacePath::parse("demo", "/second.txt").expect("second target"),
            b"second payload\n",
            &PutFileOptions {
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(commit_id.clone()),
                    message: Some("second commit".to_owned()),
                },
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
    {
        Err(ClientError::Api {
            code,
            request_id,
            details,
            ..
        }) => {
            assert_eq!(code, "commit_id_reuse_conflict");
            // The error carries the caller's reconciliation identity as
            // structured fields, not prose (API spec, "Standard error
            // contract"). The server decided this against the durable
            // receipt, so it reports where the id landed too — the one read
            // a retry needs, instead of a search of the feed.
            let details = details.expect("structured details");
            assert_eq!(details.commit_id, Some(commit_id));
            assert_eq!(details.committed_seq, Some(first.committed_seq));
            let request_id = request_id.expect("request id");
            assert!(request_id.starts_with("req_"), "got `{request_id}`");
        }
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_commit_id_is_idempotent_and_conflicts_on_different_bytes() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-put",
        "http-put",
    ))
    .await;

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/retry.txt").expect("target");
    let commit_id = CommitId::parse("req-v1-put").expect("valid commit id");

    // A put's identity is which content object it attaches, so a retry
    // resends the reference the first attempt used. Stage once, then commit
    // that same reference twice.
    let staged =
        stage_uploaded_content(&harness.client, &namespace_id("demo"), b"stable bytes\n").await;
    let token = staged
        .content_token
        .clone()
        .expect("completion returns a content token");
    let commit_request = |content_ref, token| CommitRequest {
        commit_id: commit_id.clone(),
        actor: loonfs_test_support::test_actor(),
        message: None,
        content_tokens: vec![token],
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/retry.txt").expect("path"),
            content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        }],
    };

    let first = harness
        .client
        .create_commit(
            &namespace_id("demo"),
            &commit_request(staged.content_ref.clone(), token.clone()),
        )
        .await
        .expect("first put");
    assert!(first.committed_seq.0 >= 1);

    let repeated = harness
        .client
        .create_commit(
            &namespace_id("demo"),
            &commit_request(staged.content_ref.clone(), token),
        )
        .await
        .expect("repeat put");
    assert_eq!(repeated, first);

    // Re-running the put under the same commit id uploads a second content
    // object, so the server sees a different commit and rejects it. The
    // client resolves that by reading back what the commit id actually
    // committed: same bytes, so the logical operation had already
    // succeeded, and the original answer stands. The duplicate object is
    // referenced by nothing and content collection reclaims it.
    let reuploaded = harness
        .client
        .put_file_bytes(
            &target,
            b"stable bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(commit_id.clone()),
                    message: None,
                },
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
        .await
        .expect("re-uploading identical bytes under a used commit id is idempotent");
    assert_eq!(reuploaded, first);

    let entry = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat path");
    assert_eq!(entry.head_seq, first.committed_seq);
    let bytes = harness
        .client
        .get_file_bytes(&target, &Default::default())
        .await
        .expect("read file");
    assert_eq!(bytes, b"stable bytes\n");

    // Retry reconciliation includes the actor in the fingerprint. Identical
    // content submitted by a different actor must still conflict.
    let different_actor = ActorRef::service(ActorId::parse("retry-worker").expect("actor id"));
    match harness
        .client
        .put_file_bytes(
            &target,
            b"stable bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: different_actor,
                    commit_id: Some(commit_id.clone()),
                    message: None,
                },
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
        .await
    {
        Err(ClientError::Api { code, .. }) => {
            assert_eq!(code, "commit_id_reuse_conflict")
        }
        other => panic!("expected attributed retry conflict, got {other:?}"),
    }

    // Different bytes under the same commit id is a different operation,
    // not a retry, and stays a conflict.
    match harness
        .client
        .put_file_bytes(
            &target,
            b"different bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(commit_id),
                    message: None,
                },
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
        .await
    {
        Err(ClientError::Api { code, .. }) => {
            assert_eq!(code, "commit_id_reuse_conflict")
        }
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_conflict_stands_when_only_the_message_changed() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-message",
        "http-message",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/message.txt").expect("target");
    let commit_id = CommitId::parse("req-message-put").expect("valid commit id");
    let options = |message: &str| PutFileOptions {
        behavior: DestinationBehavior::Replace,
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: Some(commit_id.clone()),
            message: Some(message.to_owned()),
        },
        expected_inode_id: None,
        expected_revision_no: None,
    };

    let first = harness
        .client
        .put_file_bytes(&target, b"stable bytes\n", &options("import batch"))
        .await
        .expect("first put");
    let replay = harness
        .client
        .put_file_bytes(&target, b"stable bytes\n", &options("import batch"))
        .await
        .expect("re-uploading an identical request is idempotent");
    assert_eq!(replay, first);

    match harness
        .client
        .put_file_bytes(&target, b"stable bytes\n", &options("second thoughts"))
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "commit_id_reuse_conflict"),
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    // Absent and empty are different messages too: the fingerprint takes
    // the annotation as given, so `null` and `""` are different commits and
    // the client compares them the same way.
    match harness
        .client
        .put_file_bytes(&target, b"stable bytes\n", &{
            let mut options = options("unused");
            options.commit.message = None;
            options
        })
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "commit_id_reuse_conflict"),
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    let changes = harness
        .client
        .list_changes(&namespace, ChangeSeq(0), &Default::default())
        .await
        .expect("list changes");
    let committed = changes
        .changes
        .iter()
        .find(|change| change.committed_seq == first.committed_seq)
        .expect("the committed change is on the feed");
    assert_eq!(
        committed.message.as_deref(),
        Some("import batch"),
        "the refused rerun did not rewrite the annotation that landed"
    );
    let entry = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_conflict_stands_when_only_the_path_changed() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-path",
        "http-path",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let commit_id = CommitId::parse("req-path-put").expect("valid commit id");
    let options = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: Some(commit_id.clone()),
            message: Some("import batch".to_owned()),
        },
        expected_inode_id: None,
        expected_revision_no: None,
    };

    let first_target = NamespacePath::parse("demo", "/a.txt").expect("first target");
    let first = harness
        .client
        .put_file_bytes(&first_target, b"stable bytes\n", &options)
        .await
        .expect("first put");

    let second_target = NamespacePath::parse("demo", "/b.txt").expect("second target");
    match harness
        .client
        .put_file_bytes(&second_target, b"stable bytes\n", &options)
        .await
    {
        Err(ClientError::Api { code, details, .. }) => {
            assert_eq!(code, "commit_id_reuse_conflict");
            // The receipt named both halves of what landed, so the client
            // had everything it needed and still refused.
            let details = details.expect("structured details");
            assert_eq!(details.committed_seq, Some(first.committed_seq));
            let fingerprint = details
                .committed_fingerprint
                .expect("the receipt's semantic identity");
            assert!(fingerprint.starts_with("v1:sha256:"), "got `{fingerprint}`");
        }
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    match harness
        .client
        .get_path_entry(&second_target, &Default::default())
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
        other => panic!("the refused rerun wrote nothing, got {other:?}"),
    }
    let entry = harness
        .client
        .get_path_entry(&first_target, &Default::default())
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_conflict_stands_when_only_a_guard_changed() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-guard",
        "http-guard",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/guard.txt").expect("target");
    let commit_id = CommitId::parse("req-guard-put").expect("valid commit id");
    let replacing = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: Some(commit_id.clone()),
            message: None,
        },
        expected_inode_id: None,
        expected_revision_no: None,
    };

    let first = harness
        .client
        .put_file_bytes(&target, b"stable bytes\n", &replacing)
        .await
        .expect("first put");
    let observed = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat path");

    match harness
        .client
        .put_file_bytes(
            &target,
            b"stable bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                ..replacing.clone()
            },
        )
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "commit_id_reuse_conflict"),
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    match harness
        .client
        .put_file_bytes(
            &target,
            b"stable bytes\n",
            &PutFileOptions {
                expected_inode_id: Some(observed.inode_id),
                expected_revision_no: Some(RevisionNo(1)),
                ..replacing
            },
        )
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "commit_id_reuse_conflict"),
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    let entry = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "neither refused rerun published a revision"
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_single_put_does_not_replay_a_multi_operation_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-batch",
        "http-batch",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/batch.txt").expect("target");
    let commit_id = CommitId::parse("req-batch-put").expect("valid commit id");

    let staged = stage_uploaded_content(&harness.client, &namespace, b"stable bytes\n").await;
    let first = harness
        .client
        .create_commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id.clone(),
                actor: loonfs_test_support::test_actor(),
                message: None,
                content_tokens: vec![content_token(&staged)],
                operations: vec![
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/batch.txt").expect("path"),
                        content_ref: staged.content_ref.clone(),
                        behavior: DestinationBehavior::Replace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                    FilesystemOperation::CreateDirectory {
                        path: AbsolutePath::parse("/reports").expect("path"),
                        parents: true,
                    },
                ],
            },
        )
        .await
        .expect("first two-operation commit");

    match harness
        .client
        .put_file_bytes(
            &target,
            b"stable bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(commit_id),
                    message: None,
                },
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "commit_id_reuse_conflict"),
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    let entry = harness
        .client
        .get_path_entry(&target, &Default::default())
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_and_mkdir_conflict_when_only_the_message_changed() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-message-anchor",
        "http-message-anchor",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    // A commit the caller built: the request that reaches the server is
    // identical apart from its message.
    let commit_id = CommitId::parse("req-message-commit").expect("valid commit id");
    let commit_request = |message: &str| {
        CommitRequest::single(
            commit_id.clone(),
            loonfs_test_support::test_actor(),
            Some(message.to_owned()),
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/direct").expect("path"),
                parents: true,
            },
        )
    };
    let first = harness
        .client
        .create_commit(&namespace, &commit_request("one"))
        .await
        .expect("first commit");
    let replay = harness
        .client
        .create_commit(&namespace, &commit_request("one"))
        .await
        .expect("an identical retry replays");
    assert_eq!(replay, first);
    match harness
        .client
        .create_commit(&namespace, &commit_request("two"))
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "commit_id_reuse_conflict"),
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    // And the same through the convenience call.
    let pinned = NamespacePath::parse("demo", "/pinned").expect("pinned target");
    let mkdir_options = |message: &str| CreateDirectoryOptions {
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: Some(CommitId::parse("req-message-mkdir").expect("valid commit id")),
            message: Some(message.to_owned()),
        },
        parents: true,
    };
    let first = harness
        .client
        .create_directory(&pinned, &mkdir_options("one"))
        .await
        .expect("first mkdir");
    let replay = harness
        .client
        .create_directory(&pinned, &mkdir_options("one"))
        .await
        .expect("an identical retry replays");
    assert_eq!(replay, first);
    match harness
        .client
        .create_directory(&pinned, &mkdir_options("two"))
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "commit_id_reuse_conflict"),
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_conflict_stands_when_retention_trimmed_the_committed_seq() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-trimmed",
        "http-trimmed",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/trimmed.txt").expect("target");
    let commit_id = CommitId::parse("req-trimmed-put").expect("valid commit id");
    let options = || PutFileOptions {
        behavior: DestinationBehavior::Replace,
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: Some(commit_id.clone()),
            message: None,
        },
        expected_inode_id: None,
        expected_revision_no: None,
    };

    let first = harness
        .client
        .put_file_bytes(&target, b"stable bytes\n", &options())
        .await
        .expect("first put");

    // Pin the head, then give up incremental replay below it.
    harness
        .client
        .create_checkpoint(
            &namespace,
            &CreateCheckpointRequest {
                name: "trimmed".to_owned(),
                ttl_ms: None,
            },
        )
        .await
        .expect("create checkpoint");
    let advanced = harness
        .client
        .run_maintenance(
            &namespace,
            &MaintenanceRunRequest::Retention(AdvanceRetentionRequest {}),
        )
        .await
        .expect("advance retention floor");
    let MaintenanceRunResponse::Retention(advanced) = advanced else {
        panic!("retention request returned a different response")
    };
    assert!(
        advanced.retention_floor_seq >= first.committed_seq,
        "the floor must cover the commit for this to test anything: floor {:?}, commit {:?}",
        advanced.retention_floor_seq,
        first.committed_seq
    );

    match harness
        .client
        .put_file_bytes(&target, b"stable bytes\n", &options())
        .await
    {
        Err(ClientError::Api { code, details, .. }) => {
            assert_eq!(code, "commit_id_reuse_conflict");
            // The server still knows where the id landed; the feed just
            // cannot answer for that sequence any more.
            let details = details.expect("structured details");
            assert_eq!(details.committed_seq, Some(first.committed_seq));
        }
        other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    let bytes = harness
        .client
        .get_file_bytes(&target, &Default::default())
        .await
        .expect("read file");
    assert_eq!(
        bytes, b"stable bytes\n",
        "the refused rerun changed nothing"
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_delete_move_and_copy_commit_ids_are_idempotent() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-ops",
        "http-ops",
    ))
    .await;

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let source = NamespacePath::parse("demo", "/docs/source.txt").expect("source");
    harness
        .client
        .put_file_bytes(&source, b"source bytes\n", &replace_file_options())
        .await
        .expect("seed source");

    let copied = NamespacePath::parse("demo", "/docs/copied.txt").expect("copied");
    let copy_first = harness
        .client
        .copy_path(
            &source,
            &copied,
            &CopyOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("req-v1-copy").expect("valid commit id")),
                    message: None,
                },
                expected_destination_inode_id: None,
                expected_destination_revision_no: None,
            },
        )
        .await
        .expect("copy first");
    let copy_repeated = harness
        .client
        .copy_path(
            &source,
            &copied,
            &CopyOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("req-v1-copy").expect("valid commit id")),
                    message: None,
                },
                expected_destination_inode_id: None,
                expected_destination_revision_no: None,
            },
        )
        .await
        .expect("copy repeat");
    assert_eq!(copy_repeated, copy_first);
    let source_entry = harness
        .client
        .get_path_entry(&source, &Default::default())
        .await
        .expect("source stat");
    let copied_entry = harness
        .client
        .get_path_entry(&copied, &Default::default())
        .await
        .expect("copied stat");
    assert_ne!(source_entry.inode_id, copied_entry.inode_id);
    assert_eq!(source_entry.content_ref(), copied_entry.content_ref());

    let moved = NamespacePath::parse("demo", "/docs/moved.txt").expect("moved");
    let move_first = harness
        .client
        .move_path(
            &copied,
            &moved,
            &MoveOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("req-v1-move").expect("valid commit id")),
                    message: None,
                },
                expected_destination_inode_id: None,
                expected_destination_revision_no: None,
            },
        )
        .await
        .expect("move first");
    let move_repeated = harness
        .client
        .move_path(
            &copied,
            &moved,
            &MoveOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("req-v1-move").expect("valid commit id")),
                    message: None,
                },
                expected_destination_inode_id: None,
                expected_destination_revision_no: None,
            },
        )
        .await
        .expect("move repeat");
    assert_eq!(move_repeated, move_first);
    match harness
        .client
        .get_path_entry(&copied, &Default::default())
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
        other => panic!("expected path_not_found for moved-from path, got {other:?}"),
    }
    let moved_entry = harness
        .client
        .get_path_entry(&moved, &Default::default())
        .await
        .expect("moved stat");
    assert_eq!(moved_entry.inode_id, copied_entry.inode_id);

    let delete_first = harness
        .client
        .delete_path(
            &moved,
            &DeleteOptions {
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("req-v1-delete").expect("valid commit id")),
                    message: None,
                },
                ..DeleteOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("delete first");
    let delete_repeated = harness
        .client
        .delete_path(
            &moved,
            &DeleteOptions {
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("req-v1-delete").expect("valid commit id")),
                    message: None,
                },
                ..DeleteOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("delete repeat");
    assert_eq!(delete_repeated, delete_first);
    match harness
        .client
        .get_path_entry(&moved, &Default::default())
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
        other => panic!("expected path_not_found for deleted path, got {other:?}"),
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_servers_share_one_store_with_last_writer_wins_fencing() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let server_a = start_server(test_config(
        store_root.clone(),
        "loonfs-server-a",
        "two-server-smoke",
    ))
    .await;
    let server_b = start_server(test_config(
        store_root,
        "loonfs-server-b",
        "two-server-smoke",
    ))
    .await;
    let client_a = server_a.client.clone();
    let client_b = server_b.client.clone();

    client_a
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let host_a_target = NamespacePath::parse("demo", "/docs/host-a.txt").expect("host a target");
    client_a
        .put_file_bytes(&host_a_target, b"host a\n", &replace_file_options())
        .await
        .expect("host a write");

    // Server B's first semantic write acquires the epoch immediately:
    // there is no lease to wait out, only last-writer-wins fencing.
    let host_b_target = NamespacePath::parse("demo", "/docs/host-b.txt").expect("host b target");
    let moved = client_b
        .move_path(
            &host_a_target,
            &host_b_target,
            &MoveOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: None,
                    message: None,
                },
                expected_destination_inode_id: None,
                expected_destination_revision_no: None,
            },
        )
        .await
        .expect("host b takes over on first write");
    assert!(
        moved.committed_seq.0 >= 2,
        "expected later commit seq, got {}",
        moved.committed_seq.0
    );

    // Server A's session is fenced terminally: its writes fail with
    // `writer_fenced` and keep failing, with no silent reacquisition.
    let host_c_target = NamespacePath::parse("demo", "/docs/host-c.txt").expect("host c target");
    for attempt in 0..2 {
        match client_a
            .put_file_bytes(&host_c_target, b"host a again\n", &replace_file_options())
            .await
        {
            Err(ClientError::Api {
                code,
                message,
                details,
                ..
            }) => {
                assert_eq!(code, "writer_fenced", "attempt {attempt}");
                // The details name the winner and when it acquired. Writer
                // ids are process labels, so two runs on one machine can
                // share one; the stamp is what separates them, and carrying
                // it structurally means a caller never parses the message.
                let details = details.expect("a fenced error carries structured details");
                assert_eq!(
                    details.active_writer.as_deref(),
                    Some("loonfs-server-b"),
                    "attempt {attempt}"
                );
                let acquired_at_ms = details
                    .active_acquired_at_ms
                    .expect("fenced details carry the winner's acquisition stamp");
                assert!(
                    message.contains(&format!(
                        "(writer `loonfs-server-b`, acquired at {acquired_at_ms} ms)"
                    )),
                    "the structured stamp should be the one the message renders: {message}"
                );
            }
            other => panic!("expected writer_fenced on attempt {attempt}, got {other:?}"),
        }
    }

    // Fencing gates writes only; server A still reads the moved file.
    let host_b_entry = client_a
        .get_path_entry(&host_b_target, &Default::default())
        .await
        .expect("stat host b file");
    assert_eq!(host_b_entry.head_seq.0, moved.committed_seq.0);
    let host_b_bytes = client_a
        .get_file_bytes(&host_b_target, &Default::default())
        .await
        .expect("read host b file");
    assert_eq!(host_b_bytes, b"host a\n");

    server_a.server.abort();
    server_b.server.abort();
}
