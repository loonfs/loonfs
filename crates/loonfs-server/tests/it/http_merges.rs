//! Merging and rebasing a fork through the HTTP routes and the client: the
//! merge's commit, its conflicts, its retries, the history it needs, where
//! the next merge starts, and a rebase's new fork and conflict report.

use crate::common::http_split_support::test_config;
use crate::common::{start_server, TestServer};
use loonfs::test_support::maintenance_past_the_grace_window;
use loonfs_client::{ClientError, MergeForkOptions, NamespacePath};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{
    ActorId, ChangeSeq, Commit, CommitId, ErrorDetails, MergeConflictKind, NamespaceId,
    NamespaceRebasedFrom, NextMergeRangeResponse, RebaseForkResponse, FEATURE_MERGES,
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
        Self::with_merge_allowance(name, None).await
    }

    /// Starts the server with `max_merge_bytes` set to `allowance`, or to
    /// its default when `allowance` is `None`.
    async fn with_merge_allowance(name: &str, allowance: Option<usize>) -> Self {
        let directory = tempdir().expect("tempdir");
        let mut config = test_config(directory.path().join("store"), name, name);
        config.max_merge_bytes = allowance;
        let harness = start_server(config).await;
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

    async fn rebase(
        &self,
        new_fork: &NamespaceId,
        commit_id: &str,
    ) -> loonfs_client::Result<RebaseForkResponse> {
        self.harness
            .client
            .rebase_fork_with_options(
                &self.main,
                &self.fork,
                new_fork,
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

/// Checks that the default merge allowance advertises `filesystem.merges`,
/// that a merge of creates, an edit, a move, and a delete commits them in
/// `main` as one commit with the merge record, and that a retry with the
/// commit ID returns the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merge_commits_the_forks_change_and_a_retry_returns_it() {
    let family = Family::new("http-merge").await;
    let client = &family.harness.client;
    let capabilities = client.get_capabilities().await.expect("capabilities");
    assert!(capabilities.supports(FEATURE_MERGES));
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

/// Checks that a merge allowance of zero turns merges off. The capability
/// document leaves out `filesystem.merges`, and the merge, rebase, and
/// next-range routes return 501 `not_supported` with that key in its
/// `feature` field, and the rebase route creates no new fork.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_allowance_turns_the_merge_routes_and_their_key_off() {
    let family = Family::with_merge_allowance("http-merge-off", Some(0)).await;
    let capabilities = family
        .harness
        .client
        .get_capabilities()
        .await
        .expect("capabilities");
    assert!(!capabilities.features.contains_key(FEATURE_MERGES));
    let new_fork = namespace_id("rebase");
    let refused = [
        family
            .merge("off")
            .await
            .map(drop)
            .expect_err("merges are off"),
        family
            .rebase(&new_fork, "off")
            .await
            .map(drop)
            .expect_err("rebases are off"),
        family
            .harness
            .client
            .next_merge_range(&family.main, &family.fork)
            .await
            .map(drop)
            .expect_err("the next-range query is off"),
    ];
    for error in refused {
        match error {
            ClientError::Api { status, response } => {
                assert_eq!(status, 501);
                assert_eq!(response.code, "not_supported");
                assert_eq!(response.feature.as_deref(), Some(FEATURE_MERGES));
            }
            other => panic!("expected an API error, got {other:?}"),
        }
    }
    family
        .harness
        .client
        .get_namespace(&new_fork)
        .await
        .expect_err("the refused rebase creates no new fork");
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

/// Checks the next-range query route. Before a merge, the next merge starts
/// after the fork point, and after a merge, it starts after the merge's
/// range. Once `main`'s floor is above the fork point, the response reports
/// the `rebootstrap_required` details in place of a start. A source that
/// isn't a fork of `main` returns `invalid_request` with `param` set to
/// `source`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_next_range_query_reports_where_the_next_merge_starts() {
    let family = Family::new("http-merge-next-range").await;
    let client = &family.harness.client;
    let fork_point = family.fork_point().await;
    let written = client
        .put_file(&path(&family.fork, "/f.txt"), b"f", &actor())
        .await
        .expect("fork");
    let range = client
        .next_merge_range(&family.main, &family.fork)
        .await
        .expect("the range before a merge");
    assert_eq!(
        range,
        NextMergeRangeResponse {
            merge: None,
            next_after_seq: Some(fork_point),
            fork_point_seq: fork_point,
            source_head_seq: written.committed_seq,
            rebootstrap_required: None,
        }
    );

    let merged = family.merge("merge").await.expect("merge");
    let range = client
        .next_merge_range(&family.main, &family.fork)
        .await
        .expect("the range after a merge");
    assert_eq!(range.merge, merged.merge);
    assert_eq!(range.next_after_seq, Some(written.committed_seq));

    let floor = family.advance_floor(&family.main).await;
    assert!(floor > fork_point);
    let range = client
        .next_merge_range(&family.main, &family.fork)
        .await
        .expect("the range below main's floor");
    assert_eq!(range.next_after_seq, None);
    let details = range.rebootstrap_required.expect("the floor's details");
    assert_eq!(details.fork_point_seq, Some(fork_point));
    assert_eq!(details.retention_floor_seq, Some(floor));

    let (code, param) = match client
        .next_merge_range(&family.main, &family.main)
        .await
        .expect_err("main isn't a fork of itself")
    {
        ClientError::Api { response, .. } => (response.code, response.param),
        other => panic!("expected an API error, got {other:?}"),
    };
    assert_eq!(code, "invalid_request");
    assert_eq!(param.as_deref(), Some("source"));
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

/// Checks the rebase route: a fork whose merge conflicts rebases into a new
/// fork of `main`, which records `rebased_from`, applies the change that
/// doesn't conflict, and reports the one that does. A retry with the commit
/// ID returns the new fork without a report, a namespace that isn't a rebase
/// of the fork returns `invalid_request`, and the new fork then merges into
/// `main`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebase_replays_into_a_new_fork_and_a_retry_returns_no_report() {
    let family = Family::new("http-rebase").await;
    let client = &family.harness.client;
    let replace = crate::common::http_split_support::replace_file_options();
    let fork_point = family.fork_point().await;
    client
        .put_file_with_options(
            &path(&family.main, "/docs/a.txt"),
            b"theirs",
            &actor(),
            &replace,
        )
        .await
        .expect("main's edit");
    for file in ["/docs/a.txt", "/docs/b.txt"] {
        client
            .put_file_with_options(&path(&family.fork, file), b"ours", &actor(), &replace)
            .await
            .expect("the fork's edit");
    }
    let (code, _) = api_error(family.merge("merge").await.expect_err("conflict"));
    assert_eq!(code, "merge_conflict");

    let new_fork = namespace_id("rebase");
    let rebased = family.rebase(&new_fork, "rebase").await.expect("rebase");
    let basis = rebased.namespace.fork_basis.as_ref().expect("a fork basis");
    assert_eq!(basis.source_namespace_id, family.main);
    assert_eq!(
        basis.rebased_from,
        Some(NamespaceRebasedFrom {
            source_namespace_id: family.fork.clone(),
            resume_after_seq: fork_point,
        })
    );
    let conflicts = rebased.conflicts.clone().expect("a conflict report");
    assert_eq!(
        conflicts
            .iter()
            .map(|conflict| (
                conflict.kind,
                conflict
                    .theirs
                    .as_ref()
                    .map(|version| version.path.as_str())
            ))
            .collect::<Vec<_>>(),
        [(MergeConflictKind::Content, Some("/docs/a.txt"))]
    );
    assert_eq!(
        family.read(&new_fork, "/docs/a.txt").await.as_deref(),
        Some(&b"theirs"[..])
    );
    assert_eq!(
        family.read(&new_fork, "/docs/b.txt").await.as_deref(),
        Some(&b"ours"[..])
    );

    let retried = family.rebase(&new_fork, "rebase").await.expect("a retry");
    assert_eq!(retried.conflicts, None);
    assert_eq!(retried.namespace.head_seq, rebased.namespace.head_seq);

    let (code, details) = match client
        .rebase_fork(&family.main, &family.fork, &family.fork, &actor())
        .await
        .expect_err("the fork isn't a rebase of itself")
    {
        ClientError::Api { response, .. } => (response.code, response.param),
        other => panic!("expected an API error, got {other:?}"),
    };
    assert_eq!(code, "invalid_request");
    assert_eq!(details.as_deref(), Some("/new_namespace_id"));

    let merged = client
        .merge_fork(&family.main, &new_fork, &actor())
        .await
        .expect("the new fork merges into main");
    assert_eq!(
        merged.merge.map(|merge| merge.source_namespace_id),
        Some(new_fork)
    );
    assert_eq!(
        family.read(&family.main, "/docs/b.txt").await.as_deref(),
        Some(&b"ours"[..])
    );
    family.harness.server.abort();
}
