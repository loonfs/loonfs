//! A fork of a head with an unfolded WAL tail folds the tail into its own
//! manifest 2, keeps that manifest with its fork point snapshot, and leaves
//! the source's manifests and tail as they were, unless the tail has a
//! commit older than the max age.

use crate::common::*;
use loonfs::{ListChangesOptions, LoonFs, ReadAt};
use loonfs_core::limits::MAX_TAIL_COMMIT_AGE_MS;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_types::{CommitSeq, ManifestNo, NamespaceDiagnostics, NamespaceId, RevisionNo};
use std::sync::Arc;
use tempfile::tempdir;

/// A source whose current manifest folds `/kept.txt`, and whose WAL tail
/// replaces `/file.txt` and appends to `/log.txt`, and a fork of its head.
struct ForkedTail {
    temp_dir: tempfile::TempDir,
    fs: TestRuntime,
    source: NamespaceId,
    fork: NamespaceId,
    /// The source's diagnostics before the fork.
    before: NamespaceDiagnostics,
}

async fn forked_tail() -> ForkedTail {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "fork-tail").await;
    let actor = loonfs_test_support::test_actor();
    let [source, fork] = ["source", "fork"].map(namespace_id);
    fs.create_namespace(&source, &actor)
        .await
        .expect("create the source");
    for (path, bytes) in [("/kept.txt", "kept"), ("/log.txt", "a")] {
        fs.put_file(&source, path, bytes.as_bytes(), &actor)
            .await
            .expect("put a folded file");
    }
    fs.maintenance.fold_wal(&source).await.expect("fold");
    fs.put_file(&source, "/file.txt", b"first", &actor)
        .await
        .expect("put the first revision");
    let replace = loonfs::PutFileOptions {
        behavior: loonfs::DestinationBehavior::Replace,
        ..Default::default()
    };
    fs.put_file_with_options(&source, "/file.txt", b"second", &actor, &replace)
        .await
        .expect("replace the first revision in the tail");
    fs.namespace_writer(&source)
        .expect("the source's handle")
        .append_file("/log.txt", b"b", &actor)
        .await
        .expect("append in the tail");
    let before = fs.maintenance.diagnostics(&source).await.expect("source");
    assert!(before.wal_tail_objects > 0);
    fs.writer
        .fork_namespace(&source, &fork, &actor)
        .await
        .expect("fork the head");
    ForkedTail {
        temp_dir,
        fs,
        source,
        fork,
        before,
    }
}

/// Checks that a fork of a head with a WAL tail leaves the source's manifest
/// and tail unchanged, reads the same files as the source, and is active at
/// the fork point with its fork point snapshot on manifest 2, and that its
/// first own commit and fold work.
#[tokio::test]
async fn a_fork_folds_the_sources_tail_into_itself_and_the_source_publishes_nothing() {
    let forked = forked_tail().await;
    let fs = &forked.fs;
    let source = fs
        .maintenance
        .diagnostics(&forked.source)
        .await
        .expect("source");
    assert_eq!(
        (
            source.current_manifest_no,
            source.head_seq,
            source.wal_tail_objects
        ),
        (
            forked.before.current_manifest_no,
            forked.before.head_seq,
            forked.before.wal_tail_objects
        ),
        "the source publishes nothing"
    );
    let fork = fs
        .maintenance
        .diagnostics(&forked.fork)
        .await
        .expect("fork");
    let basis = fork.fork_basis.clone().expect("a fork basis");
    assert_eq!(basis.fork_point_seq, source.head_seq);
    assert_eq!(fork.head_seq, source.head_seq);
    assert_eq!(fork.retention_floor_seq, source.head_seq);
    assert_eq!(fork.wal_tail_objects, 0);
    assert_eq!(fork.current_manifest_no, Some(ManifestNo(3)));
    assert_eq!(basis.fork_point_snapshot_id.manifest_no(), ManifestNo(2));
    for path in ["/kept.txt", "/file.txt", "/log.txt"] {
        let read = |namespace_id: &NamespaceId| {
            let namespace = fs.reader.namespace(namespace_id);
            async move { namespace.read_file(path).await.expect(path).bytes }
        };
        assert_eq!(
            read(&forked.fork).await,
            read(&forked.source).await,
            "{path}"
        );
    }

    let actor = loonfs_test_support::test_actor();
    let own = fs
        .put_file(&forked.fork, "/own.txt", b"own", &actor)
        .await
        .expect("the fork's first commit");
    assert_eq!(own.committed_seq.0, source.head_seq.0 + 1);
    fs.maintenance
        .fold_wal(&forked.fork)
        .await
        .expect("the fork's first fold");
    let reader = loonfs::test_support::fresh_reader(store(forked.temp_dir.path())).await;
    let fork_reader = reader.namespace(&forked.fork);
    assert_eq!(
        fork_reader.read_file("/own.txt").await.expect("own").bytes,
        b"own"
    );
    assert_eq!(
        fork_reader.read_file("/log.txt").await.expect("log").bytes,
        b"ab"
    );
}

/// Checks that the fork's fold of the source's tail and the source's own
/// later fold each write a span object of the tail's pieces, and that each
/// namespace reads the bytes through its own layout.
#[tokio::test]
async fn the_forks_fold_and_the_sources_fold_write_their_own_span_objects() {
    let forked = forked_tail().await;
    let fs = &forked.fs;
    let read = fs
        .reader
        .namespace(&forked.source)
        .read_file("/file.txt")
        .await
        .expect("read the source");
    let content_id = read
        .entry
        .content_ref()
        .expect("file content")
        .content_id
        .clone();
    let objects = store(forked.temp_dir.path());
    let pool_id = loonfs_core::test_support::load_pool_id(&objects, &forked.source).await;
    let forks = loonfs_core::test_support::span_object_keys(&objects, &pool_id, &content_id).await;
    assert_eq!(forks.len(), 1, "the fork's fold writes the source's pieces");
    fs.maintenance
        .fold_wal(&forked.source)
        .await
        .expect("fold the source");
    let both = loonfs_core::test_support::span_object_keys(&objects, &pool_id, &content_id).await;
    assert_eq!(
        both.len(),
        2,
        "the source's fold writes a span object of its own"
    );
    assert!(both.contains(&forks[0]));
    let reader = loonfs::test_support::fresh_reader(objects.clone()).await;
    for namespace_id in [&forked.source, &forked.fork] {
        assert_eq!(
            reader
                .namespace(namespace_id)
                .read_file("/file.txt")
                .await
                .expect("read")
                .bytes,
            b"second"
        );
    }
}

/// Checks that a read of the fork through its fork point snapshot returns
/// the source's state at the fork point: the source's change feed up to the
/// fork point, the current revision, and the revision that a later commit in
/// the tail replaced.
#[tokio::test]
async fn a_fork_point_snapshot_reads_the_source_at_the_fork_point() {
    let forked = forked_tail().await;
    let fs = &forked.fs;
    let fork = fs
        .maintenance
        .diagnostics(&forked.fork)
        .await
        .expect("fork");
    let snapshot_id = fork
        .fork_basis
        .expect("a fork basis")
        .fork_point_snapshot_id;
    let feed = |namespace_id: &NamespaceId, at: ReadAt| {
        let namespace = fs.reader.namespace(namespace_id);
        async move {
            namespace
                .list_changes_with_options(
                    CommitSeq(0),
                    &ListChangesOptions {
                        at,
                        ..ListChangesOptions::default()
                    },
                )
                .collect_up_to(100)
                .await
                .expect("the change feed")
                .into_iter()
                .map(|commit| (commit.commit_id, commit.committed_seq, commit.events.len()))
                .collect::<Vec<_>>()
        }
    };
    let source_feed = feed(&forked.source, ReadAt::default()).await;
    assert_eq!(
        feed(&forked.fork, ReadAt::Snapshot(snapshot_id.clone())).await,
        source_feed
    );
    assert_eq!(source_feed.last().expect("a commit").1, fork.head_seq);
    let at_fork_point = fs
        .reader
        .namespace(&forked.fork)
        .read_view_at_snapshot(&snapshot_id)
        .await
        .expect("a view at the fork point");
    let current = at_fork_point.read_file("/file.txt").await.expect("read");
    assert_eq!(current.bytes, b"second");
    assert_eq!(
        at_fork_point
            .read_file_revision_by_inode(current.entry.inode_id, RevisionNo(1), 1024)
            .await
            .expect("the revision that the tail replaced"),
        b"first"
    );
}

/// Checks that a fork of a user snapshot has no tail to fold, so its fork
/// point snapshot keeps its manifest 1, and a read through it returns the
/// snapshot's state.
#[tokio::test]
async fn a_fork_of_a_user_snapshot_keeps_its_fork_point_snapshot_on_manifest_1() {
    let forked = forked_tail().await;
    let fs = &forked.fs;
    let snapshot = fs
        .create_snapshot(&forked.source)
        .await
        .expect("a user snapshot of the source");
    let target = namespace_id("snapshot-fork");
    let created = fs
        .writer
        .fork_namespace_with_options(
            &forked.source,
            &target,
            &loonfs_test_support::test_actor(),
            &loonfs::ForkNamespaceOptions {
                snapshot_id: Some(snapshot.snapshot_id.clone()),
            },
        )
        .await
        .expect("fork the snapshot");
    let basis = created.fork_basis.expect("a fork basis");
    assert_eq!(basis.fork_point_seq, snapshot.captured_seq);
    assert_eq!(basis.fork_point_snapshot_id.manifest_no(), ManifestNo(1));
    let view = fs
        .reader
        .namespace(&target)
        .read_view_at_snapshot(&basis.fork_point_snapshot_id)
        .await
        .expect("a view at the fork point");
    assert_eq!(
        view.read_file("/file.txt").await.expect("read").bytes,
        b"second"
    );
}

/// Checks that a fork of a head folds from the source's WAL tail while the
/// tail's oldest commit is at most [`MAX_TAIL_COMMIT_AGE_MS`] old, and folds
/// the source first once that commit is older. The runtime's wall clock is a
/// manual clock, which stamps the source's commits, and the source's first
/// commit stays in its tail through the first fork, so it's the tail's
/// oldest commit at the second.
#[tokio::test]
async fn a_fork_folds_a_source_whose_tail_has_a_commit_older_than_the_max_age() {
    let temp_dir = tempdir().expect("tempdir");
    let clock = Arc::new(ManualClock::at_system_time());
    let runtime = LoonFs::builder_with_store(store(temp_dir.path()))
        .writer_id(writer_id("fork-age"))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("runtime");
    let actor = loonfs_test_support::test_actor();
    let [source_id, young_id, old_id] = ["source", "young", "old"].map(namespace_id);
    runtime
        .create_namespace(&source_id, &actor)
        .await
        .expect("create the source");
    let source = runtime.open_namespace(&source_id).expect("open the source");
    source
        .put_file("/a.txt", b"a", &actor)
        .await
        .expect("a commit in the source's tail");
    let maintenance = runtime.maintenance(writer_id("fork-age"));
    let source_manifest_no = || async {
        maintenance
            .diagnostics(&source_id)
            .await
            .expect("the source's diagnostics")
            .current_manifest_no
    };
    let before = source_manifest_no().await;

    clock.advance_ms(MAX_TAIL_COMMIT_AGE_MS);
    let young = runtime
        .fork_namespace(&source_id, &young_id, &actor)
        .await
        .expect("the first fork");
    assert_eq!(
        source_manifest_no().await,
        before,
        "a tail whose commits are at most the max age old isn't folded"
    );
    let young = young.fork_basis.expect("a fork basis");
    assert_eq!(young.fork_point_snapshot_id.manifest_no(), ManifestNo(2));

    source
        .put_file("/b.txt", b"b", &actor)
        .await
        .expect("a second commit in the source's tail");
    clock.advance_ms(1);
    let old = runtime
        .fork_namespace(&source_id, &old_id, &actor)
        .await
        .expect("the second fork");
    let after = source_manifest_no().await;
    assert!(after > before, "the fork folds the source: {after:?}");
    let old = old.fork_basis.expect("a fork basis");
    assert_eq!(
        old.fork_point_snapshot_id.manifest_no(),
        ManifestNo(1),
        "the fork starts from the manifest that the source's fold published"
    );
    let fork = runtime.open_namespace(&old_id).expect("open the fork");
    for (path, bytes) in [("/a.txt", b"a"), ("/b.txt", b"b")] {
        assert_eq!(fork.read_file(path).await.expect(path).bytes, bytes);
    }
}

/// Checks that a fork checks the age of the WAL tail that it folds, and not
/// the tail at the position that its runtime holds. Runtime A writes and
/// folds the source, so its writer position has an empty tail. Runtime B
/// then commits to the source, and the commit grows older than
/// [`MAX_TAIL_COMMIT_AGE_MS`]. A's fork reads the source's head again past
/// its stale position, finds B's old commit, and folds the source first, so
/// the fork starts from the manifest that the fold published.
#[tokio::test]
async fn a_fork_from_a_stale_position_checks_the_tail_that_it_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let clock = Arc::new(ManualClock::at_system_time());
    let runtime = |writer| {
        LoonFs::builder_with_store(store(temp_dir.path()))
            .writer_id(writer_id(writer))
            .wall_clock(clock.clone())
            .build()
    };
    let a = runtime("fork-age-a").await.expect("runtime A");
    let b = runtime("fork-age-b").await.expect("runtime B");
    let actor = loonfs_test_support::test_actor();
    let [source_id, fork_id] = ["source", "fork"].map(namespace_id);
    a.create_namespace(&source_id, &actor)
        .await
        .expect("create the source");
    a.open_namespace(&source_id)
        .expect("open the source in A")
        .put_file("/a.txt", b"a", &actor)
        .await
        .expect("A's commit");
    let maintenance = a.maintenance(writer_id("fork-age-a"));
    maintenance
        .fold_wal(&source_id)
        .await
        .expect("fold A's commit");
    b.open_namespace(&source_id)
        .expect("open the source in B")
        .put_file("/b.txt", b"b", &actor)
        .await
        .expect("B's commit");
    let before = maintenance
        .diagnostics(&source_id)
        .await
        .expect("the source's diagnostics")
        .current_manifest_no;

    clock.advance_ms(MAX_TAIL_COMMIT_AGE_MS + 1);
    let forked = a
        .fork_namespace(&source_id, &fork_id, &actor)
        .await
        .expect("the fork");
    let after = maintenance
        .diagnostics(&source_id)
        .await
        .expect("the source's diagnostics")
        .current_manifest_no;
    assert!(after > before, "the fork folds the source: {after:?}");
    assert_eq!(
        forked
            .fork_basis
            .expect("a fork basis")
            .fork_point_snapshot_id
            .manifest_no(),
        ManifestNo(1),
        "the fork starts from the manifest that the source's fold published"
    );
    let fork = a.open_namespace(&fork_id).expect("open the fork");
    for (path, bytes) in [("/a.txt", b"a"), ("/b.txt", b"b")] {
        assert_eq!(fork.read_file(path).await.expect(path).bytes, bytes);
    }
}
