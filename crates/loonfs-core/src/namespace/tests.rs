//! Namespace installation, numbered publication, and fork ownership contracts.

use crate::commit::engine::NamespaceCommitEngine;
use crate::manifest::tests::control_requests;
use crate::test_support::WriterAt;
use crate::wal::tests::publish;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{
    keys::{hint, metadata_manifest_object, metadata_manifest_prefix, pool_record, wal_prefix},
    local_fs_store::LocalFsStore,
    ObjectStore,
};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector, RecordedOperation,
    RecordingStore,
};
use loonfs_types::format::control::PoolPayload;
use loonfs_types::options::AttributeInclusion;
use loonfs_types::{ErrorCode, ManifestNo, NamespaceId};
use std::assert_matches;
use tempfile::tempdir;

impl<S: ObjectStore> crate::namespace::store::NamespaceStore<S> {
    /// Acquires the namespace's writer epoch for `writer_id`, as a test that
    /// needs a fence without a writer session does.
    pub(crate) async fn acquire_writer_epoch(
        &self,
        writer_id: &loonfs_types::WriterId,
    ) -> crate::error::Result<crate::namespace::writer_epoch::AcquiredWriter> {
        Ok(self.acquire_writer(writer_id).await?.writer)
    }
}

fn context() -> WriterAt {
    crate::test_support::writer_at("writer", 1_000)
}

/// Returns the key of every pool record in `store`.
async fn pool_record_keys<S: ObjectStore>(store: &S) -> Vec<String> {
    store
        .list_prefix("pools/")
        .await
        .expect("list the pools")
        .into_iter()
        .filter(|key| {
            parse_object_key(key).is_some_and(|parsed| parsed.family() == DurableObjectFamily::Pool)
        })
        .collect()
}

#[test]
fn an_acl_namespace_begins_with_the_root_grants_as_its_root_access_row() {
    use super::bootstrap::bootstrap_metadata_state;
    use crate::metadata::AccessRevisionRecord;
    use loonfs_types::{
        AccessGrants, AccessRevisionNo, AccessRights, ActorId, CommitSeq, NamespaceAccess,
        PrincipalId, PrincipalScope, ROOT_INODE_ID,
    };

    let root_grants = AccessGrants::new(std::collections::BTreeMap::from([(
        PrincipalId::parse("prn_root").expect("principal id"),
        AccessRights::ADMIN,
    )]))
    .expect("root grants");
    let state = bootstrap_metadata_state(
        1_000,
        &NamespaceAccess::Acl {
            principal_scope: PrincipalScope::parse("org_test").expect("principal scope"),
            root_grants: root_grants.clone(),
        },
    );
    assert_eq!(
        state.access_revisions(),
        &[AccessRevisionRecord {
            inode_id: ROOT_INODE_ID,
            access_revision_no: AccessRevisionNo(0),
            committed_seq: CommitSeq(0),
            commit_id: loonfs_types::format::control::genesis_commit_id(),
            delta_index: 0,
            committed_by: ActorId::loonfs(),
            committed_at_ms: 1_000,
            boundary: false,
            grants: root_grants,
        }]
    );
    let state = bootstrap_metadata_state(1_000, &NamespaceAccess::Unrestricted {});
    assert!(state.access_revisions().is_empty());
}

#[tokio::test]
async fn two_creations_race_at_manifest_one() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(metadata_manifest_object(&namespace_id, &ManifestNo(1))),
        OperationSelector::PutCreateIfAbsent,
    );
    store.block_next();
    let context = context();
    let namespace = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
    let (loser, winner) = futures::join!(namespace.create(), async {
        store.wait_until_blocked().await;
        let result =
            crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &context)
                .create()
                .await;
        store.release();
        result
    });
    let winner = winner.expect("winner");
    assert_eq!(loser.expect_err("loser").code(), ErrorCode::NamespaceExists);
    assert_eq!(
        pool_record_keys(&store).await,
        vec![pool_record(&winner.pool_id)],
        "the loser deletes its own pool record"
    );
}

/// Checks that a create and a fork each return the state that their manifest
/// 1 records, and that neither reads the new namespace: the put of manifest 1
/// decides whether it exists. A create writes its pool record after the hint
/// and before manifest 1, and a fork writes no pool record.
#[tokio::test]
async fn a_new_namespace_returns_the_state_its_first_manifest_records() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let created = crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .create()
        .await
        .expect("create");
    let operations = store.take();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| matches!(operation, RecordedOperation::Put { .. }))
            .map(RecordedOperation::key)
            .collect::<Vec<_>>(),
        [
            hint(&source),
            pool_record(&created.pool_id),
            metadata_manifest_object(&source, &ManifestNo(1)),
        ]
    );
    assert_eq!(
        control_requests(&source, &operations),
        vec![
            ("put", hint(&source)),
            ("put", metadata_manifest_object(&source, &ManifestNo(1))),
        ]
    );
    assert_eq!(
        crate::test_support::pool_store(&store, &created.pool_id)
            .load_pool_record()
            .await
            .expect("load the pool record"),
        Some(PoolPayload {
            pool_id: created.pool_id.clone(),
            first_member_namespace_id: source.clone(),
            created_at_ms: created.created_at_ms,
        })
    );
    assert_eq!(
        created,
        crate::test_support::namespace_store(&store, &source)
            .load_metadata()
            .await
            .expect("load the created namespace")
    );
    publish(
        &mut NamespaceCommitEngine::new(
            crate::test_support::namespace_store(&store, &source),
            context().writer_id,
        ),
        "inherited",
    )
    .await
    .expect("source data");
    store.reset();
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context());
    let forked = fixture
        .engine()
        .fork_namespace(
            &target,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    let operations = store.take();
    assert_eq!(
        control_requests(&target, &operations),
        vec![
            ("put", hint(&target)),
            ("put", metadata_manifest_object(&target, &ManifestNo(1))),
        ]
    );
    assert!(operations
        .iter()
        .all(|operation| !operation.key().starts_with("pools/")));
    assert!(forked.fork_basis.is_some());
    assert_eq!(
        forked,
        crate::test_support::namespace_store(&store, &target)
            .load_metadata()
            .await
            .expect("load the fork")
    );
}

/// Checks that identical bytes at manifest 1 confirm a create and a fork
/// after a lost response, and that a create of the existing namespace with
/// `allow_existing` deletes its own pool record.
#[tokio::test]
async fn an_ambiguous_first_manifest_confirms_a_create_and_a_fork() {
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let source_key = metadata_manifest_object(&source, &ManifestNo(1));
    let target_key = metadata_manifest_object(&target, &ManifestNo(1));
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new(move |key| key == source_key || key == target_key),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("response lost".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    let actor_id = loonfs_test_support::test_actor();
    let created = crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .create()
        .await
        .expect("the pool ID proves authorship from matching bytes");
    assert_eq!(store.attempts(), 1);
    let adopted = crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .engine()
        .bootstrap_namespace(
            &actor_id,
            &crate::options::CreateNamespaceOptions {
                allow_existing: true,
                ..Default::default()
            },
        )
        .await
        .expect("allow existing returns the created namespace");
    assert_eq!(adopted, created);
    assert_eq!(
        pool_record_keys(&store).await,
        vec![pool_record(&created.pool_id)]
    );
    store.fail_next(1);
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context());
    let forked = fixture
        .engine()
        .fork_namespace(
            &target,
            &actor_id,
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork snapshot proves authorship from matching bytes");
    assert!(forked.fork_basis.is_some());
    assert_eq!(store.attempts(), 1);
    assert_eq!(store.remaining(), 0);
}

/// Checks that a create whose pool record put has an unknown outcome sends the
/// put again after its read-back finds no record, and that a create whose
/// manifest put has an unknown outcome keeps its pool record and leaves the
/// hint, so a later create of the ID succeeds.
#[tokio::test]
async fn a_lost_pool_record_put_is_sent_again_and_a_lost_manifest_put_keeps_the_pool_record() {
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let manifest_key = metadata_manifest_object(&namespace_id, &ManifestNo(1));
    for (keys, creates) in [
        (KeyPredicate::family(DurableObjectFamily::Pool), true),
        (KeyPredicate::exact(&manifest_key), false),
    ] {
        let directory = tempdir().expect("directory");
        let store = FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            keys,
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("request lost".to_owned()),
        );
        store.fail_next(1);
        let created = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context())
            .create()
            .await;
        assert_eq!(pool_record_keys(store.inner()).await.len(), 1);
        for (key, exists) in [(hint(&namespace_id), true), (manifest_key.clone(), creates)] {
            let stored = store.inner().head(&key).await.expect("head");
            assert_eq!(stored.is_some(), exists, "`{key}`");
        }
        if creates {
            created.expect("the create sends the pool record put again");
        } else {
            assert_matches!(
                created.expect_err("the create fails"),
                crate::CoreError::OutcomeUnknown { object_key, .. } if object_key == manifest_key
            );
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context())
                .create()
                .await
                .expect("a later create of the ID succeeds");
        }
    }
}

#[tokio::test]
async fn nested_forks_read_copied_runs_without_source_control_reads() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let nested = NamespaceId::parse("nested").expect("nested");
    crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .create()
        .await
        .expect("create");
    publish(
        &mut NamespaceCommitEngine::new(
            crate::test_support::namespace_store(&store, &source),
            context().writer_id,
        ),
        "inherited",
    )
    .await
    .expect("source data");
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context());
    fixture
        .engine()
        .fork_namespace(
            &target,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    let target_manifest = crate::test_support::namespace_store(&store, &target)
        .load_current_manifest()
        .await
        .expect("target manifest");
    assert!(target_manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .all(|segment| segment.owner_namespace_id == source));
    publish(
        &mut NamespaceCommitEngine::new(
            crate::test_support::namespace_store(&store, &target),
            context().writer_id,
        ),
        "child",
    )
    .await
    .expect("unfolded child data");
    let fixture = crate::test_support::NamespaceFixture::new(&store, &target, &context());
    fixture
        .engine()
        .fork_namespace(
            &nested,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("nested fork");
    let manifest = crate::test_support::namespace_store(&store, &nested)
        .load_current_manifest()
        .await
        .expect("nested manifest");
    let owners = manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(|segment| segment.owner_namespace_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        owners,
        [source.clone(), target.clone()].into_iter().collect()
    );
    let source_hint = hint(&source);
    let target_hint = hint(&target);
    let source_manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&source);
    let target_manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&target);
    let store = RecordingStore::new(
        store,
        KeyPredicate::new(move |key| {
            key == source_hint
                || key == target_hint
                || key.starts_with(&source_manifests)
                || key.starts_with(&target_manifests)
        }),
    );
    let namespace = crate::test_support::namespace_store(&store, &nested);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("nested view");
    for path in ["/inherited", "/child"] {
        view.resolve_path(
            path,
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("inherited path");
    }
    assert_eq!(store.count(OperationSelector::Any), 0);
}

#[tokio::test]
async fn a_pending_hint_cannot_name_a_manifest_collected_after_its_replacement() {
    use loonfs_test_support::stores::MetadataMapStore;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("hint-gc").expect("namespace");
    let grace = crate::limits::GC_MIN_GRACE_WINDOW_MS;
    let options = crate::gc::GcOptions {
        grace_window_ms: grace,
    };
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let store = MetadataMapStore::new(
        store,
        KeyPredicate::new(|key| {
            loonfs_objectstore::layout::manifest_no_of(key)
                .is_some_and(|number| number >= ManifestNo(3))
        }),
        move |mut metadata| {
            metadata.last_modified_at_ms = Some(grace + 1);
            metadata
        },
    );
    let store = BlockingStore::new(
        store,
        KeyPredicate::hint(&namespace_id),
        OperationSelector::CompareAndSwap,
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context())
        .create()
        .await
        .expect("create");
    let known = crate::test_support::namespace_store(&store, &namespace_id)
        .load_hint()
        .await
        .expect("initial hint");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context().writer_id,
    );
    publish(&mut engine, "seed").await.expect("seed");
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let (hint_result, ()) =
        futures::join!(namespace.raise_hint(ManifestNo(2), Some(known)), async {
            store.wait_until_blocked().await;
            crate::test_support::namespace_store(store.inner(), &namespace_id)
                .fold_wal()
                .await
                .expect("replace manifest");
            // A manifest after the fold, so the fold's manifest isn't current.
            crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &context())
                .engine()
                .set_retention_window(std::num::NonZeroU64::new(60_000))
                .await
                .expect("set the retention window");
            // The runtime that published the fold and the window raises the
            // hint after them, while the raise to manifest 2 is still pending.
            crate::test_support::namespace_store(store.inner(), &namespace_id)
                .raise_hint(ManifestNo(4), None)
                .await
                .expect("raise the hint past the pending raise");
            let aged = WriterAt {
                now_ms: grace + 1,
                ..context()
            };
            crate::test_support::namespace_store(store.inner(), &namespace_id)
                .gc(&options, &aged.clock())
                .await
                .expect("collect during pending hint");
            store.release();
        });
    // The pending raise's compare-and-swap loses to the newer raise, and the
    // raise makes no second attempt.
    std::assert_matches!(
        hint_result.expect_err("the pending raise loses its write"),
        crate::CoreError::StoreContention { .. }
    );
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("discover past lagging hint");
    assert_eq!(current.manifest().manifest_no, ManifestNo(4));
    crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_metadata_view()
        .await
        .expect("read committed state");
    let known = crate::test_support::namespace_store(&store, &namespace_id)
        .load_hint()
        .await
        .expect("hint");
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint(ManifestNo(1), Some(known))
        .await
        .expect("refresh hint");
    let aged = WriterAt {
        now_ms: 2 * grace + 2,
        ..context()
    };
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options, &aged.clock())
        .await
        .expect("collect after grace");
    assert!(store
        .head(&metadata_manifest_object(&namespace_id, &ManifestNo(2)))
        .await
        .expect("old manifest")
        .is_none());
}

#[tokio::test]
async fn fork_into_a_deleted_id_leaves_no_source_snapshot() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let source = NamespaceId::parse("source").expect("namespace");
    let target = NamespaceId::parse("deleted-target").expect("namespace");
    let context = context();
    for namespace_id in [&source, &target] {
        crate::test_support::NamespaceFixture::new(&store, namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
    }
    NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &target),
        context.writer_id.clone(),
    )
    .delete_namespace(Default::default())
    .await
    .expect("delete target");
    store.reset();
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context);
    let error = fixture
        .engine()
        .fork_namespace(
            &target,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect_err("deleted target");
    assert_eq!(error.code(), ErrorCode::NamespaceDeleted);
    // The fork writes its fork snapshot, has the put of the target's hint
    // refused, reads the tombstone, and deletes the snapshot.
    assert_eq!(store.counts().create_if_absent_puts, 2);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert_eq!(store.counts().deletes, 1);
    assert!(store
        .list_prefix(&loonfs_objectstore::keys::snapshot_prefix(&source))
        .await
        .expect("fork snapshots")
        .is_empty());
}

#[tokio::test]
async fn a_fork_that_loses_target_publication_deletes_its_source_snapshot() {
    for deleted in [false, true] {
        let directory = tempdir().expect("directory");
        let source = NamespaceId::parse("source").expect("source");
        let target = NamespaceId::parse("target").expect("target");
        let snapshot_prefix = loonfs_objectstore::keys::snapshot_prefix(&source);
        let store = BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(&snapshot_prefix),
            OperationSelector::PutCreateIfAbsent,
        );
        let setup = context();
        let actor = loonfs_test_support::test_actor();
        crate::test_support::NamespaceFixture::new(&store, &source, &setup)
            .create()
            .await
            .expect("source");
        store.block_next();
        let engine = crate::test_support::NamespaceFixture::new(&store, &source, &setup).engine();
        let fork = engine.fork_namespace(
            &target,
            &actor,
            crate::test_support::folded_manifest(&engine.namespace)
                .await
                .expect("fold the WAL tail"),
        );
        let competing_create = async {
            store.wait_until_blocked().await;
            crate::test_support::NamespaceFixture::new(store.inner(), &target, &setup)
                .create()
                .await
                .expect("competing create");
            if deleted {
                crate::test_support::NamespaceFixture::new(store.inner(), &target, &setup)
                    .engine()
                    .delete_namespace(Default::default())
                    .await
                    .expect("competing delete");
            }
            store.release();
        };
        let (result, ()) = tokio::join!(fork, competing_create);
        assert_eq!(
            result.expect_err("lost fork").code(),
            if deleted {
                ErrorCode::NamespaceDeleted
            } else {
                ErrorCode::NamespaceExists
            }
        );
        assert!(store
            .list_prefix(&snapshot_prefix)
            .await
            .expect("fork snapshots")
            .is_empty());
    }
}

/// Checks that a writer session's deletion over unfolded commits moves the
/// session's position onto the fold's manifest, so the attempt that
/// publishes the tombstone takes the head from the position and reads
/// neither the hint nor the WAL between the fold and the tombstone.
#[tokio::test]
async fn a_deletion_publishes_its_tombstone_from_the_position_its_fold_moved() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("deleted-after-fold").expect("namespace");
    let context = context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
    );
    publish(&mut engine, "first").await.expect("first commit");
    let last = publish(&mut engine, "second").await.expect("second commit");
    store.reset();

    let deleted = engine
        .delete_namespace(Default::default())
        .await
        .expect("delete");

    assert_eq!(deleted.head_seq, last.committed_seq);
    let operations = store.take();
    let manifest_prefix = metadata_manifest_prefix(&namespace_id);
    let manifest_puts = operations
        .iter()
        .enumerate()
        .filter(|(_, operation)| {
            matches!(operation, RecordedOperation::Put { key, .. } if key.starts_with(&manifest_prefix))
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let [fold, tombstone] = manifest_puts[..] else {
        panic!("the fold and the tombstone, got {operations:?}");
    };
    let (hint, wal_prefix) = (hint(&namespace_id), wal_prefix(&namespace_id));
    let between = &operations[fold + 1..tombstone];
    assert!(
        !between
            .iter()
            .any(|operation| operation.key() == hint || operation.key().starts_with(&wal_prefix)),
        "{between:?}"
    );
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert!(current.payload().status.is_deleted());
    assert_eq!(current.object_key, operations[tombstone].key());
}
