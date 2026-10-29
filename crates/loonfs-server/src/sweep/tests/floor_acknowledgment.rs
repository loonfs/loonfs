//! The acknowledgment of the retention floor that each full pass makes
//! before its retention advance.

use super::retention::{retention_config, START_MS, WINDOW_MS};
use super::{local_store, SweepServer};
use crate::sweep::SweepCall;
use loonfs::{ChangeSeq, ErrorCode, NamespaceId};
use loonfs_http::WriterSessions as _;
use loonfs_objectstore::layout::DurableObjectFamily;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationSelector};
use loonfs_test_support::test_actor;
use loonfs_types::{WriterEpoch, WriterId};
use std::sync::Arc;
use tempfile::tempdir;

impl SweepServer {
    /// The namespace's retention floor and acknowledged floor.
    async fn floors(&self, namespace_id: &NamespaceId) -> (ChangeSeq, ChangeSeq) {
        let diagnostics = self.diagnostics(namespace_id).await;
        (
            diagnostics.retention_floor_seq,
            diagnostics.acknowledged_floor_seq,
        )
    }

    /// The current manifest's writer epoch, and the writer ID in its writer
    /// block.
    async fn writer_block(&self, namespace_id: &NamespaceId) -> (WriterEpoch, Option<WriterId>) {
        let manifest =
            loonfs_core::test_support::namespace_store(&*self.runtime.object_store(), namespace_id)
                .load_current_manifest()
                .await
                .expect("load the current manifest");
        let payload = manifest.payload();
        (
            payload.writer_epoch,
            payload
                .writer
                .as_ref()
                .map(|writer| writer.writer_id.clone()),
        )
    }

    fn acknowledgment_failures(&self) -> u64 {
        self.visit_failures(SweepCall::RetentionAcknowledgment)
    }
}

/// Checks that a full pass fences a session that this server holds and that
/// committed nothing since the previous pass. The session's next request
/// fails with `writer_fenced`, the host stops holding the session, the
/// request after it commits under a new epoch, and the acknowledged floor
/// equals the retention floor.
#[tokio::test]
async fn a_pass_fences_a_held_session_that_committed_nothing_since_the_previous_pass() {
    let directory = tempdir().expect("tempdir");
    let config = retention_config(Some(WINDOW_MS));
    let server = SweepServer::start(local_store(directory.path()), &config, START_MS).await;
    let namespace_id = namespace_id("held");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let namespaces = &server.sweep.inner.namespaces;
    let held = namespaces
        .open(&namespace_id)
        .await
        .expect("hold the session");
    let floor = held
        .put_file("/old.txt", b"history", &test_actor())
        .await
        .expect("write a file")
        .committed_seq;
    server.pass_after(2 * WINDOW_MS).await;
    assert_eq!(server.floors(&namespace_id).await, (floor, ChangeSeq(0)));
    let (epoch, _) = server.writer_block(&namespace_id).await;

    server.pass_after(config.full_sweep_interval_ms).await;
    assert_eq!(server.floors(&namespace_id).await, (floor, floor));
    assert_eq!(
        server.writer_block(&namespace_id).await,
        (
            WriterEpoch(epoch.0 + 1),
            Some(writer_id("sweep-server-maintenance"))
        ),
        "the pass claims the next epoch under the maintenance handle's writer ID"
    );
    assert_eq!(server.acknowledgment_failures(), 0);
    assert!(namespaces.entry(&namespace_id).is_some());

    let error = held
        .put_file("/fenced.txt", b"refused", &test_actor())
        .await
        .expect_err("the held session is fenced");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    // The HTTP layer makes this call when a request fails with
    // `writer_fenced`.
    assert!(namespaces.forget_if_fenced(&namespace_id, &error));
    assert!(namespaces.entry(&namespace_id).is_none());
    drop(held);
    namespaces
        .open(&namespace_id)
        .await
        .expect("open a new session")
        .put_file("/after.txt", b"accepted", &test_actor())
        .await
        .expect("the new session commits");
    assert_eq!(
        server.writer_block(&namespace_id).await.0,
        WriterEpoch(epoch.0 + 2)
    );
}

/// Checks that each pass acknowledges the floor before its advance. The
/// first pass that advances the floors fences nothing, and the next pass
/// fences the namespace that nobody wrote to and not the namespace whose
/// writer committed in between.
#[tokio::test]
async fn the_next_pass_fences_only_the_namespace_that_nobody_wrote_to() {
    let directory = tempdir().expect("tempdir");
    let config = retention_config(Some(WINDOW_MS));
    let server = SweepServer::start(local_store(directory.path()), &config, START_MS).await;
    let [idle, written] = ["idle", "written"].map(namespace_id);
    let mut floors = Vec::new();
    let mut epochs = Vec::new();
    for namespace_id in [&idle, &written] {
        server
            .runtime
            .create_namespace(namespace_id, &test_actor())
            .await
            .expect("create the namespace");
        floors.push(server.commit(namespace_id, "/old.txt").await);
        epochs.push(server.writer_block(namespace_id).await.0);
    }

    server.pass_after(2 * WINDOW_MS).await;
    for ((namespace_id, floor), epoch) in [&idle, &written].into_iter().zip(&floors).zip(&epochs) {
        assert_eq!(
            server.floors(namespace_id).await,
            (*floor, ChangeSeq(0)),
            "`{namespace_id}`: the pass advanced the floor after its acknowledgment"
        );
        assert_eq!(
            server.writer_block(namespace_id).await.0,
            *epoch,
            "`{namespace_id}`: the floor before the advance was acknowledged, so no fence"
        );
    }
    server.commit(&written, "/new.txt").await;
    let written_epoch = server.writer_block(&written).await.0;

    server.pass_after(config.full_sweep_interval_ms).await;
    assert_eq!(server.floors(&idle).await, (floors[0], floors[0]));
    assert_eq!(
        server.writer_block(&idle).await.0,
        WriterEpoch(epochs[0].0 + 1),
        "nobody wrote to `idle`, so the pass fenced it"
    );
    assert_eq!(server.floors(&written).await, (floors[1], floors[1]));
    assert_eq!(
        server.writer_block(&written).await.0,
        written_epoch,
        "the writer's commit acknowledged the floor of `written`, so no fence"
    );
    assert_eq!(server.acknowledgment_failures(), 0);
}

/// Checks that a store that refuses the fence's put leaves both floors as
/// they were, that each full pass tries the acknowledgment once and records
/// one failure, and that the first pass after the store recovers
/// acknowledges the floor.
#[tokio::test]
async fn a_refused_fence_fails_once_per_full_pass_until_one_is_written() {
    let directory = tempdir().expect("tempdir");
    // Only a writer session's commit or a fence creates a WAL object, and
    // nothing commits after the seed.
    let store = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::family(DurableObjectFamily::WalObject),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::PermissionDenied("expired credentials".to_owned()),
    ));
    let config = retention_config(Some(WINDOW_MS));
    let server = SweepServer::start(store.clone(), &config, START_MS).await;
    let namespace_id = namespace_id("refused");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let floor = server.commit(&namespace_id, "/old.txt").await;
    server.pass_after(2 * WINDOW_MS).await;
    assert_eq!(server.floors(&namespace_id).await, (floor, ChangeSeq(0)));
    store.fail_all();

    for passes in 1..=3 {
        server.pass_after(config.full_sweep_interval_ms).await;
        assert_eq!(
            server.acknowledgment_failures(),
            passes,
            "each full pass tries the acknowledgment once"
        );
        assert_eq!(server.floors(&namespace_id).await, (floor, ChangeSeq(0)));
    }
    assert_eq!(
        server.visit_failures(SweepCall::Retention),
        0,
        "the advance still runs"
    );

    store.clear();
    server.pass_after(config.full_sweep_interval_ms).await;
    assert_eq!(server.floors(&namespace_id).await, (floor, floor));
    assert_eq!(server.acknowledgment_failures(), 3);
}
