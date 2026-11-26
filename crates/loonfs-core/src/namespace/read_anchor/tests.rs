//! Discoveries that find a newer manifest after their WAL read.

use crate::commit::engine::NamespaceCommitEngine;
use crate::wal::tests::publish;
use loonfs_objectstore::keys::wal_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    KeyPredicate, NewerManifestStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::{CommitSeq, NamespaceId};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::TempDir;

/// A namespace whose tail has a fence and `commits` commits, written by a
/// writer session that the fixture keeps.
struct Fixture {
    directory: TempDir,
    namespace_id: NamespaceId,
    writer: NamespaceCommitEngine<LocalFsStore>,
}

async fn fixture(name: &str, commits: usize) -> Fixture {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse(name).expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let context = crate::test_support::writer_at("writer", 1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("create");
    let mut writer = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store, &namespace_id),
        context.writer_id,
    );
    for index in 0..commits {
        publish(&mut writer, &format!("commit-{index}"))
            .await
            .expect("commit");
    }
    Fixture {
        directory,
        namespace_id,
        writer,
    }
}

impl Fixture {
    /// Returns a store of the fixture's objects that no test wrapper
    /// intercepts.
    fn store(&self) -> LocalFsStore {
        LocalFsStore::new(self.directory.path()).expect("store")
    }

    /// Returns a store that records every request, over a store that
    /// publishes the next manifest before every GET of a WAL object once the
    /// test arms it.
    fn publishing_before_wal_gets(&self) -> RecordingStore<NewerManifestStore<LocalFsStore>> {
        RecordingStore::new(
            NewerManifestStore::new(
                self.store(),
                Arc::new(self.store()),
                self.namespace_id.clone(),
                KeyPredicate::prefix(wal_prefix(&self.namespace_id)),
                OperationSelector::Get,
            ),
            KeyPredicate::any(),
        )
    }
}

/// Returns the keys of the WAL objects that `requests` read.
fn wal_gets(namespace_id: &NamespaceId, requests: &[RecordedOperation]) -> Vec<String> {
    requests
        .iter()
        .filter(|request| matches!(request, RecordedOperation::Get { .. }))
        .map(RecordedOperation::key)
        .filter(|key| key.starts_with(&wal_prefix(namespace_id)))
        .map(str::to_owned)
        .collect()
}

/// Checks that a retention advance from a process without a writer session
/// creates its floor object when another process publishes a manifest before
/// every GET of the advance's discovery, so that the discovery finds a newer
/// manifest after every WAL read, and that the discovery reads each WAL
/// number once.
#[tokio::test]
async fn an_advance_creates_its_floor_object_when_every_wal_get_is_followed_by_a_newer_manifest() {
    let fixture = fixture("busy-advance", 10).await;
    let planned = crate::test_support::namespace_store(fixture.store(), &fixture.namespace_id)
        .load_current_manifest()
        .await
        .expect("load the planned manifest");
    let store = fixture.publishing_before_wal_gets();
    store.inner().arm();
    let advanced = crate::test_support::namespace_store(&store, &fixture.namespace_id)
        .publish_floor_object(&planned, CommitSeq(5))
        .await
        .expect("the advance creates its floor object");
    assert_eq!(advanced.retention_floor_seq, CommitSeq(5));

    let requests = store.take();
    let wal_gets = wal_gets(&fixture.namespace_id, &requests);
    let distinct = wal_gets.iter().collect::<BTreeSet<_>>();
    assert_eq!(distinct.len(), wal_gets.len(), "{wal_gets:?}");
    assert_eq!(store.inner().published(), wal_gets.len());
    assert!(
        store.inner().published() > crate::limits::MAX_CONTENTION_ATTEMPTS,
        "more newer manifests than a discovery has passes, {}",
        store.inner().published()
    );
    drop(fixture.writer);
}
