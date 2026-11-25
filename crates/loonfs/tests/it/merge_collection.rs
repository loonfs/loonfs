//! A pool collection that reads the members' roots while a merge of a fork
//! into its source commits.
//!
//! A merge publishes a reference to the fork's content into the source and
//! then deletes the snapshot that its plan took on the fork. A collection
//! that read the source's roots before the merge commits, and reads the fork's
//! roots after a compaction removed the merged revision from the fork, finds
//! that content only through the deleted snapshot, whose record stays until
//! the fork's own collection deletes the record a grace window after the
//! delete.
//!
//! The collection runs through `PoolStore::gc` at the manual clock's time,
//! because the content age rule compares that time with the store's file
//! times, and the content has to be past the grace window.

use crate::common::store;
use loonfs::test_support::fresh_reader;
use loonfs::{
    AdvanceRetentionFloorOptions, CommitSeq, DestinationBehavior, FileRevision, LoonFs,
    Maintenance, MergeForkOptions, Namespace, NamespaceId, PoolGcResponse, PoolId, PutFileOptions,
    RetentionTarget, SharedObjectStore, Writable,
};
use loonfs_core::limits::{CONTENT_RECLAMATION_GRACE_WINDOW_MS, GC_GRACE_WINDOW_MS};
use loonfs_core::test_support::WriterAt;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::clock::{bounded, ManualClock};
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationSelector};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const HOUR_MS: u64 = 60 * 60 * 1000;

/// How long a test waits for the collection to park or to finish before it
/// fails.
const WAIT_BOUND: Duration = Duration::from_secs(60);

/// A source namespace and its fork, where the fork wrote `/f` twice, each
/// version folded into its own delta run, and the clock is past the upload
/// sessions' grace window.
struct Family {
    directory: TempDir,
    store: SharedObjectStore,
    clock: Arc<ManualClock>,
    maintenance: Maintenance,
    source: Namespace<Writable>,
    source_id: NamespaceId,
    fork_id: NamespaceId,
    pool_id: PoolId,
    v1_seq: CommitSeq,
    v2_seq: CommitSeq,
    v1_content_key: String,
    // The handles above belong to this runtime, which the family keeps open.
    _writer: LoonFs<Writable>,
}

async fn family() -> Family {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = store(directory.path());
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(writer_id("merge-collection"))
        .wall_clock(clock.clone())
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            // Each version is a content object, not inline bytes.
            inline_content: loonfs::InlineContentPolicy {
                inline_content_threshold_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        })
        .build()
        .await
        .expect("build writer");
    let maintenance = writer.maintenance(writer_id("merge-collection"));
    let actor = loonfs_test_support::test_actor();
    // The pool's member list is listed in key order, so the collection reads
    // the source's roots before the fork's.
    let source_id = namespace_id("a-source");
    let fork_id = namespace_id("b-fork");
    writer
        .create_namespace(&source_id, &actor)
        .await
        .expect("create the source");
    writer
        .fork_namespace(&source_id, &fork_id, &actor)
        .await
        .expect("fork the source");
    let fork = writer.open_namespace(&fork_id).expect("open the fork");
    let v1_seq = fork
        .put_file("/f", b"version 1", &actor)
        .await
        .expect("put version 1")
        .committed_seq;
    maintenance
        .fold_wal(&fork_id)
        .await
        .expect("fold version 1");
    let v2_seq = fork
        .put_file_with_options(
            "/f",
            b"version 2",
            &actor,
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("put version 2")
        .committed_seq;
    maintenance
        .fold_wal(&fork_id)
        .await
        .expect("fold version 2");
    let pool_id = loonfs_core::test_support::load_pool_id(store.as_ref(), &fork_id).await;
    let v1_content_id = fork_revisions(&store, &fork_id)
        .await
        .into_iter()
        .find(|revision| revision.committed_seq == v1_seq)
        .expect("the fork lists version 1")
        .content_ref
        .content_id;
    let family = Family {
        v1_content_key: loonfs_objectstore::keys::content_blob(&pool_id, &v1_content_id),
        source: writer.open_namespace(&source_id).expect("open the source"),
        directory,
        store,
        clock,
        maintenance,
        source_id,
        fork_id,
        pool_id,
        v1_seq,
        v2_seq,
        _writer: writer,
    };
    // A completed upload session roots its content until a collection a
    // reclamation grace window after its completion deletes its records, so
    // this collection leaves the revisions' layouts as the only roots.
    family
        .clock
        .advance_ms(CONTENT_RECLAMATION_GRACE_WINDOW_MS + HOUR_MS);
    let sessions = family.collect(family.store.clone()).await;
    assert_eq!(sessions.deleted.upload_sessions, 2);
    assert_eq!(sessions.deleted.content_objects, 0);
    family
}

/// Returns the retained revisions of the fork's `/f`, as a reader with
/// nothing cached lists them.
async fn fork_revisions(store: &SharedObjectStore, fork_id: &NamespaceId) -> Vec<FileRevision> {
    fresh_reader(store.clone())
        .await
        .namespace(fork_id)
        .list_file_revisions("/f")
        .page()
        .await
        .expect("list the fork's revisions of /f")
        .revisions
}

/// Runs one collection of `pool_id` over `store` at `now_ms`.
async fn collect_pool(store: SharedObjectStore, pool_id: PoolId, now_ms: u64) -> PoolGcResponse {
    loonfs_core::test_support::pool_store(store.as_ref(), &pool_id)
        .gc(
            None,
            &WriterAt {
                writer_id: writer_id("merge-collection-gc"),
                now_ms,
            }
            .clock(),
        )
        .await
        .expect("collect the pool")
}

impl Family {
    async fn collect(&self, store: SharedObjectStore) -> PoolGcResponse {
        collect_pool(store, self.pool_id.clone(), self.clock.now_ms()).await
    }

    /// Merges the fork into the source through version 1, advances the fork's
    /// retention floor to version 2, and compacts the fork until version 2 is
    /// the fork's one revision of `/f`.
    async fn merge_and_compact_the_fork(&self) {
        self.source
            .merge_fork_with_options(
                &self.fork_id,
                &loonfs_test_support::test_actor(),
                &MergeForkOptions {
                    through_seq: Some(self.v1_seq),
                    ..Default::default()
                },
            )
            .await
            .expect("the merge commits");
        let advanced = self
            .maintenance
            .advance_retention_floor_with_options(
                &self.fork_id,
                &AdvanceRetentionFloorOptions {
                    target: RetentionTarget::Seq(self.v2_seq),
                },
            )
            .await
            .expect("advance the fork's retention floor");
        assert_eq!(advanced.retention_floor_seq, self.v2_seq);
        loonfs_core::test_support::drain_compaction(
            self.store.as_ref(),
            &self.fork_id,
            loonfs_core::test_support::one_delta_run_policy(),
        )
        .await;
        assert_eq!(
            fork_revisions(&self.store, &self.fork_id)
                .await
                .iter()
                .map(|revision| revision.committed_seq)
                .collect::<Vec<_>>(),
            vec![self.v2_seq],
            "the compaction removes version 1 from the fork"
        );
    }

    /// Merges and compacts the fork while a collection of the pool is parked
    /// at its first read of the fork, after its read of the source's roots,
    /// and returns the collection's response.
    async fn collect_while_merging(&self) -> PoolGcResponse {
        let gate = Arc::new(BlockingStore::new(
            LocalFsStore::new(self.directory.path()).expect("create local-fs store"),
            KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&self.fork_id)),
            OperationSelector::Read,
        ));
        gate.block_next();
        let collection = tokio::spawn(collect_pool(
            gate.clone() as SharedObjectStore,
            self.pool_id.clone(),
            self.clock.now_ms(),
        ));
        bounded(WAIT_BOUND, gate.wait_until_blocked())
            .await
            .expect("the collection reads the fork");
        self.merge_and_compact_the_fork().await;
        gate.release();
        bounded(WAIT_BOUND, collection)
            .await
            .expect("the collection finishes")
            .expect("join the collection")
    }

    /// Checks that `report` comes from a content sweep that deleted nothing,
    /// and that a reader with nothing cached reads version 1 in the source.
    async fn assert_merged_content_kept(&self, report: &PoolGcResponse) {
        assert!(
            report.skipped.is_empty(),
            "the content sweep runs: {report:?}"
        );
        assert!(report.retained.referenced > 0, "{report:?}");
        assert_eq!(report.deleted.content_objects, 0, "{report:?}");
        assert!(
            self.store
                .head(&self.v1_content_key)
                .await
                .expect("head version 1's content")
                .is_some(),
            "version 1's content object stays"
        );
        let read = fresh_reader(self.store.clone())
            .await
            .namespace(&self.source_id)
            .read_file("/f")
            .await
            .expect("read the merged file in the source");
        assert_eq!(read.bytes, b"version 1");
    }
}

#[tokio::test]
async fn a_merge_that_commits_while_a_pool_collection_reads_the_members_keeps_its_content() {
    let family = family().await;
    let report = family.collect_while_merging().await;
    assert_eq!(report.retained.referenced, 2, "{report:?}");
    family.assert_merged_content_kept(&report).await;
}

#[tokio::test]
async fn a_pool_collection_after_a_merge_keeps_the_merged_content() {
    let family = family().await;
    family.merge_and_compact_the_fork().await;
    let report = family.collect(family.store.clone()).await;
    family.assert_merged_content_kept(&report).await;
}

#[tokio::test]
async fn a_pool_collection_after_the_merge_snapshots_grace_window_keeps_the_merged_content() {
    let family = family().await;
    let report = family.collect_while_merging().await;
    family.assert_merged_content_kept(&report).await;
    family.clock.advance_ms(GC_GRACE_WINDOW_MS + HOUR_MS);
    let fork_collection = family
        .maintenance
        .gc(&family.fork_id)
        .await
        .expect("collect the fork");
    assert_eq!(
        fork_collection.deleted_snapshots_by_owner.user, 1,
        "the fork's collection deletes the merge's snapshot record"
    );
    let report = family.collect(family.store.clone()).await;
    family.assert_merged_content_kept(&report).await;
}
