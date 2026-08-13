//! The one wire commit surface: batches commit atomically, a failing
//! operation names its position, and replay shares one fingerprint domain
//! with the embedded runtime.

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs::publish::CommitRequest as CoreCommitRequest;
use loonfs::{CreateNamespaceOptions, FsWriter, ListChangesOptions, StoreConfig};
use loonfs_api::{
    v0::CommittedChange, AbsolutePath, ApiError, ChangeSeq, CommitId, CommitRequest, ContentRef,
    DeleteDirectoryBehavior, DestinationBehavior, ErrorCode, FilesystemOperation, RevisionNo,
};
use loonfs_client::{ClientError, NamespacePath};
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

const REPORTS_DIR: &str = "/reports";
const FIRST_FILE: &str = "/reports/january.txt";
const SECOND_FILE: &str = "/reports/february.txt";
const FIRST_BYTES: &[u8] = b"january numbers";
const SECOND_BYTES: &[u8] = b"february numbers";

fn absolute(path: &str) -> AbsolutePath {
    AbsolutePath::parse(path).expect("valid absolute path")
}

fn commit_id(value: &str) -> CommitId {
    CommitId::parse(value).expect("valid commit id")
}

/// The comparable content of one committed change: everything the feed
/// promises except the wall-clock stamp, which is observational.
/// The parts of a change two transports must agree on.
///
/// Content identity is left out on purpose: each transport staged its own
/// content objects, so their ids differ by construction. The rest of the
/// reference — size and checksums — stays in, so identical bytes still have
/// to produce identical evidence.
fn change_identity(change: &CommittedChange) -> (ChangeSeq, String, Option<String>, String) {
    let mut events = serde_json::to_value(&change.events).expect("serialize events");
    for event in events.as_array_mut().expect("events array") {
        if let Some(content_ref) = event.get_mut("content_ref") {
            content_ref["content_id"] = serde_json::Value::from("<normalized>");
        }
    }
    (
        change.committed_seq,
        change.commit_id.to_string(),
        change.message.clone(),
        events.to_string(),
    )
}

/// The directory-then-two-files batch.
///
/// Both transports below build their operations from this one helper, which
/// they can because there is one operation language: the served arm and the
/// embedded arm differ only in the content each staged.
fn batch(first: &ContentRef, second: &ContentRef) -> Vec<FilesystemOperation> {
    vec![
        FilesystemOperation::CreateDirectory {
            path: absolute(REPORTS_DIR),
            parents: false,
        },
        FilesystemOperation::PutFile {
            path: absolute(FIRST_FILE),
            content_ref: first.clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        },
        FilesystemOperation::PutFile {
            path: absolute(SECOND_FILE),
            content_ref: second.clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        },
    ]
}

/// One batch, two transports: the same three operations submitted over HTTP
/// and embedded produce the same single commit and the same ordered events.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_commits_once_and_matches_the_same_batch_embedded() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let harness = start_server(test_config(
        store_root.clone(),
        "loonfs-server-batch",
        "http-commits",
    ))
    .await;

    // Two namespaces in one store: writer epochs are per namespace, so the
    // embedded arm and the served arm never fence each other.
    let remote_ns = namespace_id("remote");
    harness
        .client
        .create_namespace(&remote_ns)
        .await
        .expect("create remote namespace");
    let first = stage_uploaded_content(&harness.client, &remote_ns, FIRST_BYTES).await;
    let second = stage_uploaded_content(&harness.client, &remote_ns, SECOND_BYTES).await;

    let committed = harness
        .client
        .commit(
            &remote_ns,
            &CommitRequest {
                commit_id: commit_id("batch-one"),
                message: Some("import the reports".to_owned()),
                content_tokens: vec![
                    validated_content_token(&first),
                    validated_content_token(&second),
                ],
                operations: batch(&first.content_ref, &second.content_ref),
            },
        )
        .await
        .expect("batch commits");
    // Three operations, one commit: the namespace advanced exactly once.
    assert_eq!(committed.committed_seq, ChangeSeq(1));
    assert_eq!(committed.commit_id.as_str(), "batch-one");

    let remote_changes = harness
        .client
        .list_changes(&remote_ns, ChangeSeq(0), None)
        .await
        .expect("remote changes");
    assert_eq!(remote_changes.changes.len(), 1, "{remote_changes:?}");
    // One event per operation, in request order: the directory, then the
    // two files created under it.
    assert_eq!(remote_changes.changes[0].events.len(), 3);

    // Every path the batch named is visible, and only because the whole
    // batch committed.
    for (path, bytes) in [(FIRST_FILE, FIRST_BYTES), (SECOND_FILE, SECOND_BYTES)] {
        let spec = NamespacePath::parse("remote", path).expect("path");
        assert_eq!(
            harness
                .client
                .get_file_bytes(&spec)
                .await
                .expect("batch file readable"),
            bytes
        );
    }

    // The same batch, submitted embedded against the same store.
    let embedded_ns = namespace_id("embedded");
    let writer = FsWriter::builder(StoreConfig::LocalFs {
        root: store_root.display().to_string(),
        key_prefix: Some("http-commits".to_owned()),
    })
    .writer_id("loonfs-embedded-batch")
    .build()
    .await
    .expect("embedded writer");
    writer
        .create_namespace(&embedded_ns, CreateNamespaceOptions::default())
        .await
        .expect("create embedded namespace");
    let first_prepared = writer
        .prepare_file_bytes(&embedded_ns, FIRST_BYTES)
        .await
        .expect("prepare first");
    let second_prepared = writer
        .prepare_file_bytes(&embedded_ns, SECOND_BYTES)
        .await
        .expect("prepare second");
    let embedded_committed = writer
        .commit_prepared(
            &embedded_ns,
            CoreCommitRequest {
                commit_id: commit_id("batch-one"),
                message: Some("import the reports".to_owned()),
                operations: batch(first_prepared.content_ref(), second_prepared.content_ref()),
            },
            vec![first_prepared, second_prepared],
        )
        .await
        .expect("embedded batch commits");
    assert_eq!(embedded_committed.committed_seq, committed.committed_seq);

    let embedded_changes = writer
        .reader()
        .list_changes(&embedded_ns, ChangeSeq(0), ListChangesOptions::default())
        .await
        .expect("embedded changes");

    // The parity claim: the two transports produced the same commit —
    // same sequence, same id, same annotation, same ordered events.
    assert_eq!(
        remote_changes
            .changes
            .iter()
            .map(change_identity)
            .collect::<Vec<_>>(),
        embedded_changes
            .changes
            .iter()
            .map(change_identity)
            .collect::<Vec<_>>()
    );

    writer
        .shutdown()
        .await
        .expect("settle embedded background work");
    harness.server.abort();
}

/// A batch is all-or-nothing: the operation that stops it names its own
/// position, and nothing the batch would have written is visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_operation_names_its_position_and_commits_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-batch-failure",
        "http-commit-failure",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let staged = stage_uploaded_content(&harness.client, &namespace, FIRST_BYTES).await;

    // Operations 0 and 1 are valid and depend on each other; operation 2
    // deletes a path that was never bound.
    let error = harness
        .client
        .commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("batch-stops-at-two"),
                message: None,
                content_tokens: vec![validated_content_token(&staged)],
                operations: vec![
                    FilesystemOperation::CreateDirectory {
                        path: absolute(REPORTS_DIR),
                        parents: false,
                    },
                    FilesystemOperation::PutFile {
                        path: absolute(FIRST_FILE),
                        content_ref: staged.content_ref.clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_revision_no: None,
                    },
                    FilesystemOperation::DeletePath {
                        path: absolute("/never-existed.txt"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                        expected_inode_id: None,
                    },
                ],
            },
        )
        .await
        .expect_err("the third operation has nothing to delete");

    match error {
        ClientError::Api {
            status,
            code,
            details,
            ..
        } => {
            // The code stays the failing operation's own; the position is
            // what batching adds.
            assert_eq!(status, 404);
            assert_eq!(code, ErrorCode::PathNotFound.as_str());
            let details = details.expect("failed batch carries details");
            assert_eq!(details.operation_index, Some(2));
            assert_eq!(
                details.commit_id.as_ref().map(CommitId::as_str),
                Some("batch-stops-at-two")
            );
        }
        other => unreachable!("expected path_not_found with a position, got {other:?}"),
    }

    // Nothing the batch would have written became visible, and the head
    // never advanced.
    for path in [REPORTS_DIR, FIRST_FILE] {
        let spec = NamespacePath::parse("demo", path).expect("path");
        let missing = harness
            .client
            .stat_path(&spec, &Default::default())
            .await
            .expect_err("the aborted batch wrote nothing");
        match missing {
            ClientError::Api { code, .. } => assert_eq!(code, ErrorCode::PathNotFound.as_str()),
            other => unreachable!("expected path_not_found, got {other:?}"),
        }
    }
    assert_eq!(
        harness
            .client
            .namespace_status(&namespace)
            .await
            .expect("status")
            .head_seq,
        ChangeSeq(0)
    );

    harness.server.abort();
}

/// An empty operation list is the one shape the language does not accept;
/// the wire surfaces core's own classification for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_operation_list_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-empty-batch",
        "http-empty-batch",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let error = harness
        .client
        .commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("empty-batch"),
                message: None,
                content_tokens: Vec::new(),
                operations: Vec::new(),
            },
        )
        .await
        .expect_err("an empty request has nothing to commit");
    match error {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 400);
            assert_eq!(code, ErrorCode::InvalidRequest.as_str());
        }
        other => unreachable!("expected invalid_request, got {other:?}"),
    }
    assert_eq!(
        harness
            .client
            .namespace_status(&namespace)
            .await
            .expect("status")
            .head_seq,
        ChangeSeq(0)
    );

    harness.server.abort();
}

/// The root is readable but never a mutation target, alone or inside a
/// batch, and rejecting it commits nothing.
///
/// The planners own the rule, so the rejection is attributed exactly like
/// every other planning failure: a batch names the operation that stopped
/// it, a one-operation request names nothing, and both echo the commit id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_root_path_is_rejected_as_a_mutation_target() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-root-mutation",
        "http-root-mutation",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let alone = harness
        .client
        .commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("root-alone"),
                message: None,
                content_tokens: Vec::new(),
                operations: vec![FilesystemOperation::CreateDirectory {
                    path: absolute("/"),
                    parents: false,
                }],
            },
        )
        .await
        .expect_err("the root cannot be created");
    match alone {
        ClientError::Api {
            status,
            code,
            details,
            ..
        } => {
            assert_eq!(status, 400);
            assert_eq!(code, ErrorCode::InvalidRequest.as_str());
            let details = details.expect("a failed commit carries details");
            // One operation has one place to fail, so nothing disambiguates it.
            assert_eq!(
                details.operation_index, None,
                "a one-operation request names no position"
            );
            assert_eq!(
                details.commit_id.as_ref().map(CommitId::as_str),
                Some("root-alone")
            );
        }
        other => unreachable!("expected invalid_request, got {other:?}"),
    }

    let in_batch = harness
        .client
        .commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("root-in-batch"),
                message: None,
                content_tokens: Vec::new(),
                operations: vec![
                    FilesystemOperation::CreateDirectory {
                        path: absolute(REPORTS_DIR),
                        parents: false,
                    },
                    FilesystemOperation::DeletePath {
                        path: absolute("/"),
                        behavior: DeleteDirectoryBehavior::Recursive,
                        expected_inode_id: None,
                    },
                ],
            },
        )
        .await
        .expect_err("the root cannot be deleted");
    match in_batch {
        ClientError::Api {
            status,
            code,
            details,
            ..
        } => {
            assert_eq!(status, 400);
            assert_eq!(code, ErrorCode::InvalidRequest.as_str());
            let details = details.expect("a failed commit carries details");
            // Planning stops at the root operation, so the batch names its
            // position like it names every other failure's.
            assert_eq!(
                details.operation_index,
                Some(1),
                "the batch names the operation that stopped it"
            );
            assert_eq!(
                details.commit_id.as_ref().map(CommitId::as_str),
                Some("root-in-batch")
            );
        }
        other => unreachable!("expected invalid_request, got {other:?}"),
    }

    // The valid first operation of the rejected batch did not land either.
    assert_eq!(
        harness
            .client
            .namespace_status(&namespace)
            .await
            .expect("status")
            .head_seq,
        ChangeSeq(0)
    );

    // The rule is the planners', so it is answered where every other path
    // rule is: against a namespace that exists. A root mutation aimed at a
    // namespace that does not answers for the namespace instead.
    let unknown = harness
        .client
        .commit(
            &namespace_id("missing"),
            &CommitRequest {
                commit_id: commit_id("root-unknown-namespace"),
                message: None,
                content_tokens: Vec::new(),
                operations: vec![FilesystemOperation::CreateDirectory {
                    path: absolute("/"),
                    parents: false,
                }],
            },
        )
        .await
        .expect_err("the namespace does not exist");
    match unknown {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 404);
            assert_eq!(code, ErrorCode::NamespaceNotFound.as_str());
        }
        other => unreachable!("expected namespace_not_found, got {other:?}"),
    }

    harness.server.abort();
}

/// Replay over the wire: the same id with the same batch returns the
/// original receipt, and the same id with a different batch conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_replays_under_its_commit_id() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-batch-replay",
        "http-commit-replay",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let batch = |ops: Vec<FilesystemOperation>| CommitRequest {
        commit_id: commit_id("replayed-batch"),
        message: Some("two directories".to_owned()),
        content_tokens: Vec::new(),
        operations: ops,
    };
    let operations = vec![
        FilesystemOperation::CreateDirectory {
            path: absolute(REPORTS_DIR),
            parents: false,
        },
        FilesystemOperation::CreateDirectory {
            path: absolute("/reports/2026"),
            parents: false,
        },
    ];

    let first = harness
        .client
        .commit(&namespace, &batch(operations.clone()))
        .await
        .expect("batch commits");
    let replayed = harness
        .client
        .commit(&namespace, &batch(operations.clone()))
        .await
        .expect("identical resubmission replays");
    assert_eq!(replayed, first);
    assert_eq!(
        harness
            .client
            .namespace_status(&namespace)
            .await
            .expect("status")
            .head_seq,
        first.committed_seq,
        "the replay committed nothing new"
    );

    // Dropping the second operation is a different commit under the same
    // id, so the id is spent.
    let conflict = harness
        .client
        .commit(&namespace, &batch(operations[..1].to_vec()))
        .await
        .expect_err("a different batch cannot reuse the id");
    match conflict {
        ClientError::Api { code, .. } => {
            assert_eq!(code, ErrorCode::CommitIdReuseConflict.as_str());
        }
        other => unreachable!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    harness.server.abort();
}

/// One fingerprint domain across transports: a batch committed embedded
/// replays over HTTP under the same commit id, and a different batch under
/// that id conflicts over HTTP just as it would embedded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_id_used_embedded_replays_over_http() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let namespace = namespace_id("shared");

    let operations = || {
        vec![
            FilesystemOperation::CreateDirectory {
                path: absolute(REPORTS_DIR),
                parents: false,
            },
            FilesystemOperation::CreateDirectory {
                path: absolute("/reports/2026"),
                parents: false,
            },
            FilesystemOperation::MovePath {
                from_path: absolute("/reports/2026"),
                to_path: absolute("/reports/2025"),
                behavior: DestinationBehavior::NoReplace,
            },
        ]
    };

    // Committed embedded first, then the writer goes away: the served
    // writer acquires its own epoch afterward and finds the receipt.
    let embedded_receipt = {
        let writer = FsWriter::builder(StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: Some("http-cross-transport".to_owned()),
        })
        .writer_id("loonfs-embedded-cross")
        .build()
        .await
        .expect("embedded writer");
        writer
            .create_namespace(&namespace, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let receipt = writer
            .commit(
                &namespace,
                CoreCommitRequest {
                    commit_id: commit_id("crosses-transports"),
                    message: Some("shaped once".to_owned()),
                    operations: operations(),
                },
            )
            .await
            .expect("embedded batch commits");
        writer
            .shutdown()
            .await
            .expect("settle embedded background work");
        receipt
    };

    let harness = start_server(test_config(
        store_root.clone(),
        "loonfs-server-cross",
        "http-cross-transport",
    ))
    .await;

    // The same commit, written in the wire language under the same id.
    let wire_operations = vec![
        FilesystemOperation::CreateDirectory {
            path: absolute(REPORTS_DIR),
            parents: false,
        },
        FilesystemOperation::CreateDirectory {
            path: absolute("/reports/2026"),
            parents: false,
        },
        FilesystemOperation::MovePath {
            from_path: absolute("/reports/2026"),
            to_path: absolute("/reports/2025"),
            behavior: DestinationBehavior::NoReplace,
        },
    ];
    let replayed = harness
        .client
        .commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("crosses-transports"),
                message: Some("shaped once".to_owned()),
                content_tokens: Vec::new(),
                operations: wire_operations.clone(),
            },
        )
        .await
        .expect("the embedded commit replays over http");
    assert_eq!(replayed, embedded_receipt);
    assert_eq!(
        harness
            .client
            .namespace_status(&namespace)
            .await
            .expect("status")
            .head_seq,
        embedded_receipt.committed_seq,
        "the cross-transport replay committed nothing new"
    );

    // And the conflict crosses too: a different batch under the spent id
    // fails over HTTP on a receipt the embedded runtime wrote.
    let conflict = harness
        .client
        .commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("crosses-transports"),
                message: Some("shaped once".to_owned()),
                content_tokens: Vec::new(),
                operations: wire_operations[..2].to_vec(),
            },
        )
        .await
        .expect_err("a different batch cannot reuse the embedded id");
    match conflict {
        ClientError::Api { code, .. } => {
            assert_eq!(code, ErrorCode::CommitIdReuseConflict.as_str());
        }
        other => unreachable!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    harness.server.abort();
}

/// A misspelled guard fails the request rather than the precondition.
///
/// Every optimistic-concurrency guard on a commit is an optional field, so
/// before the commit body rejected unknown fields a typo decoded to `None`
/// and the write applied unguarded — a lost update reported as a 200. The two
/// bodies below differ only in how `expected_revision_no` is spelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_misspelled_commit_guard_is_rejected_rather_than_dropped() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-strict-commit",
        "http-strict-commit",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let first = stage_uploaded_content(&harness.client, &namespace, FIRST_BYTES).await;
    let second = stage_uploaded_content(&harness.client, &namespace, SECOND_BYTES).await;

    harness
        .client
        .commit(
            &namespace,
            &CommitRequest {
                commit_id: commit_id("guarded-create"),
                message: None,
                content_tokens: vec![validated_content_token(&first)],
                operations: vec![FilesystemOperation::PutFile {
                    path: absolute(FIRST_FILE),
                    content_ref: first.content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                    expected_revision_no: None,
                }],
            },
        )
        .await
        .expect("the file is created at revision 1");

    // One replace, spelled two ways. Both name the revision that is actually
    // current, so the spelling of the guard is the only difference.
    let replace = |commit: &str, guard: &str| {
        let mut put = serde_json::json!({
            "kind": "put_file",
            "path": FIRST_FILE,
            "content_ref": second.content_ref.clone(),
            "behavior": "replace"
        });
        put[guard] = serde_json::json!(1);
        serde_json::json!({
            "commit_id": commit,
            "content_tokens": [validated_content_token(&second)],
            "operations": [put]
        })
    };

    let ureq::Error::Status(status, response) = *send_commit_json(
        &harness.server_url,
        &namespace,
        &replace("misspelled-guard", "expected_revsion_no"),
    )
    .expect_err("a misspelled guard is not a commit this API accepts") else {
        unreachable!("a rejected commit body returns an HTTP status");
    };
    assert_eq!(status, 400);
    let error: ApiError =
        serde_json::from_reader(response.into_reader()).expect("API error envelope");
    assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());

    // The rejected body wrote nothing: the file is still the one the guarded
    // create published.
    let unchanged = harness
        .client
        .stat_path(
            &NamespacePath::parse("demo", FIRST_FILE).expect("path"),
            &Default::default(),
        )
        .await
        .expect("the file is still there");
    assert_eq!(unchanged.revision_no(), Some(RevisionNo(1)));
    assert_eq!(unchanged.content_ref(), Some(&first.content_ref));

    // Spelled correctly the same body commits, which is what says the typo
    // was the only thing wrong with it.
    send_commit_json(
        &harness.server_url,
        &namespace,
        &replace("spelled-guard", "expected_revision_no"),
    )
    .expect("the guard spelled correctly commits");
    let replaced = harness
        .client
        .stat_path(
            &NamespacePath::parse("demo", FIRST_FILE).expect("path"),
            &Default::default(),
        )
        .await
        .expect("the replace landed");
    assert_eq!(replaced.revision_no(), Some(RevisionNo(2)));
    assert_eq!(replaced.content_ref(), Some(&second.content_ref));

    harness.server.abort();
}
