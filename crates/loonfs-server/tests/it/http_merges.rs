//! Merging a fork through the HTTP route and the client: the merge's commit,
//! its conflicts, its retries, and the history it needs.

use crate::common::http_split_support::test_config;
use crate::common::{start_server, TestServer};
use loonfs::test_support::maintenance_past_the_grace_window;
use loonfs_client::{ClientError, MergeForkOptions, NamespacePath};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{
    ActorId, ChangeSeq, Commit, CommitId, ErrorDetails, MergeConflictKind, NamespaceId,
};
use tempfile::{tempdir, TempDir};

/// A server with `main`, which has `/docs/a.txt`, `/docs/b.txt`, and
/// `/old/x.txt`, and `fork`, a fork of `main`.
struct Family {
    _directory: TempDir,
    harness: TestServer,
    main: NamespaceId,
    fork: NamespaceId,
}

fn actor() -> ActorId {
    loonfs_test_support::test_actor()
}

fn path(namespace: &NamespaceId, path: &str) -> NamespacePath {
    NamespacePath::parse(namespace.as_str(), path).expect("path")
}

impl Family {
    async fn new(name: &str) -> Self {
        let directory = tempdir().expect("tempdir");
        let harness = start_server(test_config(directory.path().join("store"), name, name)).await;
        let main = namespace_id("main");
        let fork = namespace_id("fork");
        harness
            .client
            .create_namespace(&main, &actor())
            .await
            .expect("create main");
        for file in ["/docs/a.txt", "/docs/b.txt", "/old/x.txt"] {
            harness
                .client
                .put_file(&path(&main, file), file.as_bytes(), &actor())
                .await
                .expect("seed main");
        }
        harness
            .client
            .fork_namespace(&main, &fork, &actor())
            .await
            .expect("fork");
        Self {
            _directory: directory,
            harness,
            main,
            fork,
        }
    }

    async fn merge(&self, commit_id: &str) -> loonfs_client::Result<Commit> {
        self.harness
            .client
            .merge_fork_with_options(
                &self.main,
                &self.fork,
                &actor(),
                &MergeForkOptions {
                    commit_id: Some(CommitId::parse(commit_id).expect("commit id")),
                    ..MergeForkOptions::default()
                },
            )
            .await
    }

    async fn read(&self, namespace: &NamespaceId, file: &str) -> Option<Vec<u8>> {
        self.harness
            .client
            .read_file(&path(namespace, file))
            .await
            .ok()
    }

    /// Returns the sequence of `main` that the fork was made at.
    async fn fork_point(&self) -> ChangeSeq {
        self.harness
            .client
            .get_namespace(&self.fork)
            .await
            .expect("the fork")
            .fork_basis
            .expect("a fork has a basis")
            .source_head_seq
    }

    /// Folds `namespace` and advances its retention floor to the folded
    /// head, and returns the floor.
    async fn advance_floor(&self, namespace: &NamespaceId) -> ChangeSeq {
        let maintenance = maintenance_past_the_grace_window(self.harness.store.clone()).await;
        maintenance.fold_wal(namespace).await.expect("fold");
        maintenance
            .advance_retention_floor(namespace)
            .await
            .expect("advance the floor")
            .retention_floor_seq
    }
}

/// Returns the code and details of an API error.
fn api_error(error: ClientError) -> (String, ErrorDetails) {
    match error {
        ClientError::Api { response, .. } => (
            response.code,
            response.details.map(|details| *details).unwrap_or_default(),
        ),
        other => panic!("expected an API error, got {other:?}"),
    }
}

/// Checks that a merge of creates, an edit, a move, and a delete commits
/// them in `main` as one commit with the merge record, and that a retry with
/// the commit ID returns the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merge_commits_the_forks_change_and_a_retry_returns_it() {
    let family = Family::new("http-merge").await;
    let client = &family.harness.client;
    let fork = &family.fork;
    client
        .put_file(&path(fork, "/new/c.txt"), b"c", &actor())
        .await
        .expect("create");
    client
        .put_file_with_options(
            &path(fork, "/docs/a.txt"),
            b"edited",
            &actor(),
            &crate::common::http_split_support::replace_file_options(),
        )
        .await
        .expect("edit");
    client
        .move_path(
            &path(fork, "/docs/b.txt"),
            &path(fork, "/new/b.txt"),
            &actor(),
        )
        .await
        .expect("move");
    client
        .delete_path_with_options(
            &path(fork, "/old"),
            &actor(),
            &loonfs_client::DeleteOptions {
                behavior: loonfs_types::DeleteDirectoryBehavior::Recursive,
                ..loonfs_client::DeleteOptions::default()
            },
        )
        .await
        .expect("delete");

    let merged = family.merge("merge").await.expect("merge");
    let record = merged.merge.as_ref().expect("merge record");
    assert_eq!(record.source_namespace_id, family.fork);
    assert_eq!(
        family.read(&family.main, "/new/c.txt").await.as_deref(),
        Some(&b"c"[..])
    );
    assert_eq!(
        family.read(&family.main, "/docs/a.txt").await.as_deref(),
        Some(&b"edited"[..])
    );
    assert_eq!(family.read(&family.main, "/docs/b.txt").await, None);
    assert_eq!(
        family.read(&family.main, "/new/b.txt").await.as_deref(),
        Some(&b"/docs/b.txt"[..])
    );
    assert_eq!(family.read(&family.main, "/old/x.txt").await, None);

    let retried = family.merge("merge").await.expect("a retry");
    assert_eq!(retried, merged);
    family.harness.server.abort();
}

/// Checks that a change that conflicts with `main` returns `merge_conflict`
/// with the conflict count and the first conflict, and commits nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_conflict_returns_the_first_conflict_and_commits_nothing() {
    let family = Family::new("http-merge-conflict").await;
    let client = &family.harness.client;
    let replace = crate::common::http_split_support::replace_file_options();
    for (namespace, bytes) in [(&family.main, &b"theirs"[..]), (&family.fork, &b"ours"[..])] {
        for file in ["/docs/a.txt", "/docs/b.txt"] {
            client
                .put_file_with_options(&path(namespace, file), bytes, &actor(), &replace)
                .await
                .expect("edit");
        }
    }
    let head = client
        .get_namespace(&family.main)
        .await
        .expect("main")
        .head_seq;

    let (code, details) = api_error(family.merge("conflict").await.expect_err("conflict"));
    assert_eq!(code, "merge_conflict");
    assert_eq!(details.conflict_count, Some(2));
    let conflict = details.conflict.expect("the first conflict");
    assert_eq!(conflict.kind, MergeConflictKind::Content);
    let paths = [
        conflict.base.as_ref().map(|version| version.path.as_str()),
        conflict.ours.as_ref().map(|version| version.path.as_str()),
        conflict
            .theirs
            .as_ref()
            .map(|version| version.path.as_str()),
    ];
    let path = paths[0].expect("a base");
    assert_eq!(paths, [Some(path); 3]);
    assert!(conflict
        .ours
        .as_ref()
        .and_then(|ours| ours.revision.as_ref())
        .is_some());
    assert_eq!(
        client
            .get_namespace(&family.main)
            .await
            .expect("main")
            .head_seq,
        head
    );
    family.harness.server.abort();
}

/// Checks that a merge that needs history below a retention floor returns
/// `rebootstrap_required`, and that the details say which floor: `main`'s
/// floor above the fork point, or the fork's own floor above the range's
/// start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merge_below_either_floor_returns_rebootstrap_required() {
    let family = Family::new("http-merge-main-floor").await;
    let client = &family.harness.client;
    let fork_point = family.fork_point().await;
    client
        .put_file(&path(&family.fork, "/f.txt"), b"f", &actor())
        .await
        .expect("fork");
    client
        .put_file(&path(&family.main, "/m.txt"), b"m", &actor())
        .await
        .expect("main");
    let floor = family.advance_floor(&family.main).await;
    assert!(floor > fork_point);
    let (code, details) = api_error(family.merge("main-floor").await.expect_err("main's floor"));
    assert_eq!(code, "rebootstrap_required");
    assert_eq!(details.fork_point_seq, Some(fork_point));
    assert_eq!(details.retention_floor_seq, Some(floor));
    family.harness.server.abort();

    let family = Family::new("http-merge-fork-floor").await;
    let client = &family.harness.client;
    let fork_point = family.fork_point().await;
    for file in ["/f.txt", "/g.txt"] {
        client
            .put_file(&path(&family.fork, file), b"f", &actor())
            .await
            .expect("fork");
    }
    let floor = family.advance_floor(&family.fork).await;
    assert!(floor > fork_point);
    let (code, details) = api_error(family.merge("fork-floor").await.expect_err("fork's floor"));
    assert_eq!(code, "rebootstrap_required");
    assert_eq!(details.source_namespace_id, Some(family.fork.clone()));
    assert_eq!(details.actual_after_seq, Some(fork_point));
    assert_eq!(details.retention_floor_seq, Some(floor));
    assert_eq!(details.fork_point_seq, None);
    family.harness.server.abort();
}

/// Checks that a namespace that isn't a fork of `main`, one that doesn't
/// exist, and a fork that was deleted each return `invalid_request` with
/// `param` set to the request's field, and not a code that reports `main` as
/// missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_that_isnt_a_live_fork_returns_invalid_request() {
    let family = Family::new("http-merge-not-a-fork").await;
    let client = &family.harness.client;
    let other = namespace_id("other");
    client
        .create_namespace(&other, &actor())
        .await
        .expect("create other");
    client
        .delete_namespace(&family.fork)
        .await
        .expect("delete the fork");
    for source in [&other, &namespace_id("missing"), &family.fork] {
        let error = client
            .merge_fork(&family.main, source, &actor())
            .await
            .expect_err("not a live fork");
        match error {
            ClientError::Api { response, .. } => {
                assert_eq!(response.code, "invalid_request", "{source}");
                assert_eq!(
                    response.param.as_deref(),
                    Some("/source_namespace_id"),
                    "{source}"
                );
            }
            other => panic!("expected an API error, got {other:?}"),
        }
    }
    client
        .put_file(&path(&family.main, "/after.txt"), b"after", &actor())
        .await
        .expect("main still takes writes");
    family.harness.server.abort();
}
