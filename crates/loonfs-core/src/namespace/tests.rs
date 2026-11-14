//! Namespace installation, numbered publication, and fork ownership contracts.

use crate::commit::engine::NamespaceCommitEngine;
use crate::limits::OPERATION_SNAPSHOT_TTL_MS;
use crate::manifest::tests::control_requests;
use crate::test_support::WriterAt;
use crate::wal::tests::publish;
use crate::CoreError;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{
    keys::{boundary, metadata_manifest_object, metadata_manifest_prefix, pool_member, wal_prefix},
    local_fs_store::LocalFsStore,
    ObjectStore,
};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector, RecordedOperation,
    RecordingStore,
};
use loonfs_types::format::control::{NamespaceStatus, SnapshotOwner, SnapshotPayload};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::options::AttributeInclusion;
use loonfs_types::{ErrorCode, ManifestNo, NamespaceId, SnapshotId};
use std::assert_matches;
use tempfile::tempdir;

/// The store operations of the create in
/// `a_new_namespace_is_looked_up_before_its_first_write`: 3 for the lookup, 2
/// for manifest 1, 1 for the member entry, and 2 for manifest 2.
const CREATE_STORE_OPERATIONS: usize = 8;

/// The store operations of the fork in
/// `a_new_namespace_is_looked_up_before_its_first_write`, with the fold of
/// the source's WAL tail before it: 16 for the fold, and 31 for the fork, of
/// which 16 are the two checks of the snapshot that the fork is built from
/// and 1 is the member entry.
const FORK_STORE_OPERATIONS: usize = 47;

/// The store operations of the deletion in
/// `a_delete_of_an_initializing_namespace_publishes_the_tombstone_without_a_writer_session`.
const INITIALIZING_DELETE_STORE_OPERATIONS: usize = 10;

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

/// Returns the key of every member entry in `store`.
async fn member_entry_keys<S: ObjectStore>(store: &S) -> Vec<String> {
    store
        .list_prefix("pools/")
        .await
        .expect("list the pools")
        .into_iter()
        .filter(|key| {
            parse_object_key(key)
                .is_some_and(|parsed| parsed.family() == DurableObjectFamily::PoolMember)
        })
        .collect()
}

/// Does what a create of `namespace_id` with `options` does through manifest
/// 1 in the store at `root`, and stops there, because the store refuses the
/// member entry's put. Returns manifest 1.
async fn half_done_create(
    root: &std::path::Path,
    namespace_id: &NamespaceId,
    options: &loonfs_types::options::CreateNamespaceOptions,
) -> NamespaceManifestPayload {
    let store = LocalFsStore::new(root).expect("store");
    let refusing = FailStore::new(
        LocalFsStore::new(root).expect("store"),
        KeyPredicate::family(DurableObjectFamily::PoolMember),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::PermissionDenied("refused".to_owned()),
    );
    refusing.fail_next(1);
    crate::test_support::NamespaceFixture::new(&refusing, namespace_id, &context())
        .create_with_options(options)
        .await
        .expect_err("the create stops at the member entry");
    let manifests = manifest_payloads(&store, namespace_id).await;
    let [manifest_1] = &manifests[..] else {
        panic!("manifest 1 alone, got {manifests:?}");
    };
    assert_eq!(manifest_1.status, NamespaceStatus::Initializing {});
    manifest_1.clone()
}

/// Returns every manifest of `namespace_id`, from manifest 1 up to the first
/// number without a manifest.
async fn manifest_payloads<S: ObjectStore>(
    store: S,
    namespace_id: &NamespaceId,
) -> Vec<NamespaceManifestPayload> {
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    let mut manifests = Vec::new();
    for manifest_no in (1..).map(ManifestNo) {
        let Some(manifest) = namespace
            .load_manifest_by_no(manifest_no)
            .await
            .expect("load a manifest")
        else {
            return manifests;
        };
        manifests.push(manifest.payload().clone());
    }
    manifests
}

/// Returns the snapshot records of `namespace_id`, in key order.
async fn snapshot_records<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<SnapshotPayload> {
    let mut records = Vec::new();
    for key in store
        .list_prefix(&loonfs_objectstore::keys::snapshot_prefix(namespace_id))
        .await
        .expect("list the snapshots")
    {
        records.push(
            crate::snapshot::record::load_snapshot_record_at_key(store, &key)
                .await
                .expect("load a snapshot record")
                .state,
        );
    }
    records
}

/// Returns the fork snapshots of `source`.
async fn fork_snapshots<S: ObjectStore>(store: &S, source: &NamespaceId) -> Vec<SnapshotId> {
    snapshot_records(store, source)
        .await
        .into_iter()
        .filter(|record| matches!(record.owner, SnapshotOwner::Fork { .. }))
        .map(|record| record.snapshot_id)
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

/// Checks that a create with the same request as a create that's waiting on
/// its member entry finishes the namespace, that both creates return the
/// same pool ID, and that a create with another retention window returns
/// `namespace_exists` while the first create still finishes the namespace.
#[tokio::test]
async fn a_create_finishes_a_concurrent_create_with_the_same_request_and_refuses_other_options() {
    let other_window = loonfs_types::options::CreateNamespaceOptions {
        retention_window_ms: std::num::NonZeroU64::new(7 * 24 * 60 * 60 * 1_000),
        ..Default::default()
    };
    for (second_options, second_finishes) in [(Default::default(), true), (other_window, false)] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("created").expect("namespace");
        let store = BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::family(DurableObjectFamily::PoolMember),
            OperationSelector::PutCreateIfAbsent,
        );
        store.block_next();
        let context = context();
        let namespace = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
        let (first, second) = futures::join!(namespace.create(), async {
            store.wait_until_blocked().await;
            let result =
                crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &context)
                    .create_with_options(&second_options)
                    .await;
            store.release();
            result
        });
        let first = first.expect("the first create finishes the namespace");
        if second_finishes {
            assert_eq!(second.expect("the second create finishes it too"), first);
        } else {
            assert_matches!(second, Err(CoreError::NamespaceExists { .. }));
        }
        assert_eq!(
            member_entry_keys(&store).await,
            vec![pool_member(&first.pool_id, &namespace_id)]
        );
        let manifests = manifest_payloads(&store, &namespace_id).await;
        assert_eq!(
            manifests
                .iter()
                .map(|manifest| manifest.status)
                .collect::<Vec<_>>(),
            [NamespaceStatus::Initializing {}, NamespaceStatus::Active {}]
        );
    }
}

/// Checks that a create and a fork each look for the new namespace before
/// their first write, publish manifest 1 with its put and one read of the
/// manifest boundary, write the member entry, publish manifest 2, and return
/// the state that manifest 2 records. A fork also writes a temporary snapshot
/// before manifest 1, and its fork snapshot after the member entry.
#[tokio::test]
async fn a_new_namespace_is_looked_up_before_its_first_write() {
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
    let member_entry = pool_member(&created.pool_id, &source);
    assert_eq!(
        operations
            .iter()
            .map(RecordedOperation::key)
            .collect::<Vec<_>>(),
        [
            boundary(&source),
            metadata_manifest_prefix(&source),
            boundary(&source),
            metadata_manifest_object(&source, &ManifestNo(1)),
            boundary(&source),
            member_entry.clone(),
            metadata_manifest_object(&source, &ManifestNo(2)),
            boundary(&source),
        ]
    );
    assert_eq!(operations.len(), CREATE_STORE_OPERATIONS);
    assert_matches!(&operations[5], RecordedOperation::Put { .. });
    let lookup = |namespace_id: &NamespaceId| {
        vec![
            ("get", boundary(namespace_id)),
            ("list", metadata_manifest_prefix(namespace_id)),
            ("get", boundary(namespace_id)),
        ]
    };
    let first_manifest = |namespace_id: &NamespaceId| {
        vec![
            (
                "put",
                metadata_manifest_object(namespace_id, &ManifestNo(1)),
            ),
            ("get", boundary(namespace_id)),
        ]
    };
    let second_manifest = |namespace_id: &NamespaceId| {
        vec![
            (
                "put",
                metadata_manifest_object(namespace_id, &ManifestNo(2)),
            ),
            ("get", boundary(namespace_id)),
        ]
    };
    assert_eq!(
        control_requests(&source, &operations),
        [
            lookup(&source),
            first_manifest(&source),
            second_manifest(&source)
        ]
        .concat()
    );
    let entry = loonfs_types::format::control::decode_control_object::<
        loonfs_types::format::control::PoolMemberPayload,
    >(
        &store
            .get(&member_entry, None)
            .await
            .expect("get the member entry")
            .expect("a member entry"),
        loonfs_types::format::control::ControlObjectKind::PoolMember,
    )
    .expect("decode the member entry");
    assert_eq!(
        entry.payload(),
        &loonfs_types::format::control::PoolMemberPayload {
            pool_id: created.pool_id.clone(),
            namespace_id: source.clone(),
        }
    );
    let manifests = manifest_payloads(&store, &source).await;
    assert_eq!(
        manifests
            .iter()
            .map(|manifest| manifest.status)
            .collect::<Vec<_>>(),
        [NamespaceStatus::Initializing {}, NamespaceStatus::Active {}]
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
    let manifest_2 = metadata_manifest_object(&target, &ManifestNo(2));
    assert_eq!(
        control_requests(&target, &operations),
        [
            lookup(&target),
            first_manifest(&target),
            second_manifest(&target),
        ]
        .concat()
    );
    assert_eq!(operations.len(), FORK_STORE_OPERATIONS, "{operations:?}");
    let snapshots = loonfs_objectstore::keys::snapshot_prefix(&source);
    let snapshot_puts = operations
        .iter()
        .enumerate()
        .filter_map(|(index, operation)| match operation {
            RecordedOperation::Put { key, .. } if key.starts_with(&snapshots) => Some((index, key)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [(temporary_put, temporary_key), (fork_put, fork_key)] = snapshot_puts[..] else {
        panic!("a temporary snapshot and a fork snapshot, got {operations:?}");
    };
    let position = |key: &str| {
        operations
            .iter()
            .position(|operation| operation.key() == key)
            .expect(key)
    };
    let target_boundary_reads = operations
        .iter()
        .enumerate()
        .filter(|(_, operation)| operation.key() == boundary(&target))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let target_entry = position(&pool_member(&created.pool_id, &target));
    assert!(
        temporary_put < position(&metadata_manifest_object(&target, &ManifestNo(1)))
            && target_boundary_reads[2] + 1 == target_entry
            && target_entry < fork_put
            && fork_put < position(&manifest_2),
        "{operations:?}"
    );
    let record = |key: &str| {
        let store = &store;
        let key = key.to_owned();
        async move {
            crate::snapshot::record::load_snapshot_record_at_key(store, &key)
                .await
                .expect("load the snapshot record")
                .state
        }
    };
    let temporary = record(temporary_key).await;
    assert_eq!(
        temporary.owner,
        SnapshotOwner::User {
            name: None,
            expires_at_ms: Some(temporary.created_at_ms + OPERATION_SNAPSHOT_TTL_MS),
            deleted_at_ms: None,
        }
    );
    assert_eq!(
        record(fork_key).await.owner,
        SnapshotOwner::Fork {
            target_namespace_id: target.clone(),
        }
    );
    assert_eq!(
        operations
            .iter()
            .filter(|operation| operation.key().starts_with("pools/"))
            .map(RecordedOperation::key)
            .collect::<Vec<_>>(),
        [pool_member(&created.pool_id, &target)]
    );
    let manifests = manifest_payloads(&store, &target).await;
    let [manifest_1, manifest_2] = &manifests[..] else {
        panic!("manifests 1 and 2, got {manifests:?}");
    };
    assert_eq!(manifest_1.status, NamespaceStatus::Initializing {});
    assert_eq!(
        manifest_2,
        &NamespaceManifestPayload {
            manifest_no: ManifestNo(2),
            ..manifest_1.activated()
        }
    );
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
/// after a lost response, that both then publish manifest 2, and that a
/// create of the existing namespace with `allow_existing` returns the
/// namespace and writes no member entry.
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
            &loonfs_types::options::CreateNamespaceOptions {
                allow_existing: true,
                ..Default::default()
            },
        )
        .await
        .expect("allow existing returns the created namespace");
    assert_eq!(adopted, created);
    assert_eq!(
        member_entry_keys(&store).await,
        vec![pool_member(&created.pool_id, &source)]
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
    let manifests = manifest_payloads(&store, &target).await;
    assert_eq!(
        manifests
            .iter()
            .map(|manifest| manifest.status)
            .collect::<Vec<_>>(),
        [NamespaceStatus::Initializing {}, NamespaceStatus::Active {}]
    );
}

/// Checks that a create whose member entry put has an unknown outcome sends
/// the put again after its read-back finds no entry, and that a create whose
/// manifest 1 put has an unknown outcome writes no entry, so a later create
/// with the same request finishes the namespace.
#[tokio::test]
async fn a_lost_member_entry_put_is_sent_again_and_a_lost_manifest_one_put_leaves_an_initializing_namespace(
) {
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let manifest_key = metadata_manifest_object(&namespace_id, &ManifestNo(1));
    for (keys, creates) in [
        (KeyPredicate::family(DurableObjectFamily::PoolMember), true),
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
        if creates {
            created.expect("the create sends the member entry put again");
        } else {
            assert_matches!(
                created.expect_err("the create fails"),
                crate::CoreError::OutcomeUnknown { object_key, .. } if object_key == manifest_key
            );
            assert!(member_entry_keys(store.inner()).await.is_empty());
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context())
                .create()
                .await
                .expect("a later create with the same request finishes the namespace");
        }
        assert_eq!(member_entry_keys(store.inner()).await.len(), 1);
        let manifests = manifest_payloads(store.inner(), &namespace_id).await;
        assert_eq!(
            manifests
                .iter()
                .map(|manifest| manifest.status)
                .collect::<Vec<_>>(),
            [NamespaceStatus::Initializing {}, NamespaceStatus::Active {}]
        );
    }
}

/// Checks that a create call with the request of a create that stopped after
/// manifest 1 writes the member entry in the pool that manifest 1 records,
/// and publishes manifest 2.
#[tokio::test]
async fn a_create_that_stopped_after_manifest_one_is_finished_by_the_same_request() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let manifest_1 = half_done_create(directory.path(), &namespace_id, &Default::default()).await;
    assert!(member_entry_keys(&store).await.is_empty());
    let created = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context())
        .create()
        .await
        .expect("finish the create");
    assert_eq!(created.pool_id, manifest_1.pool_id);
    assert_eq!(
        member_entry_keys(&store).await,
        [pool_member(&manifest_1.pool_id, &namespace_id)]
    );
    assert_eq!(
        manifest_payloads(&store, &namespace_id).await,
        [
            manifest_1.clone(),
            NamespaceManifestPayload {
                manifest_no: ManifestNo(2),
                ..manifest_1.activated()
            }
        ]
    );
    assert_eq!(
        created,
        crate::test_support::namespace_store(&store, &namespace_id)
            .load_metadata()
            .await
            .expect("load the created namespace")
    );
}

/// Checks that a create can't finish an initializing fork and a fork can't
/// finish an initializing create, and that neither writes anything to the
/// namespace.
#[tokio::test]
async fn a_create_never_finishes_a_fork_and_a_fork_never_finishes_a_create() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (source, target) = source_and_target();
    let (_, fork_manifest_1) =
        crate::test_support::half_done_fork(&store, &source, &target, &context()).await;
    for allow_existing in [false, true] {
        let created = crate::test_support::NamespaceFixture::new(&store, &target, &context())
            .create_with_options(&loonfs_types::options::CreateNamespaceOptions {
                allow_existing,
                ..Default::default()
            })
            .await;
        assert_matches!(created, Err(CoreError::NamespaceExists { .. }));
    }
    assert_eq!(
        manifest_payloads(&store, &target).await,
        std::slice::from_ref(&fork_manifest_1)
    );
    let created = NamespaceId::parse("created").expect("namespace");
    let create_manifest_1 = half_done_create(directory.path(), &created, &Default::default()).await;
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context());
    let forked = fixture
        .engine()
        .fork_namespace(
            &created,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await;
    assert_matches!(forked, Err(CoreError::NamespaceExists { .. }));
    assert_eq!(
        manifest_payloads(&store, &created).await,
        [create_manifest_1]
    );
    assert_eq!(
        member_entry_keys(&store).await,
        [pool_member(&fork_manifest_1.pool_id, &source)],
        "only the source has a member entry"
    );
    assert!(fork_snapshots(&store, &source).await.is_empty());
}

/// Checks that a create with `allow_existing` and another access mode or
/// retention window returns `namespace_exists` for an initializing create,
/// because an initializing namespace isn't an existing one, and writes
/// nothing to it.
#[tokio::test]
async fn a_create_with_allow_existing_returns_namespace_exists_for_an_initializing_namespace_with_other_options(
) {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let manifest_1 = half_done_create(directory.path(), &namespace_id, &Default::default()).await;
    let other_access = loonfs_types::options::CreateNamespaceOptions {
        allow_existing: true,
        access: loonfs_types::NamespaceAccess::Acl {
            principal_scope: loonfs_types::PrincipalScope::parse("org_test").expect("scope"),
            root_grants: loonfs_types::AccessGrants::new(std::collections::BTreeMap::from([(
                loonfs_types::PrincipalId::parse("prn_root").expect("principal id"),
                loonfs_types::AccessRights::ADMIN,
            )]))
            .expect("root grants"),
        },
        ..Default::default()
    };
    let other_window = loonfs_types::options::CreateNamespaceOptions {
        allow_existing: true,
        retention_window_ms: std::num::NonZeroU64::new(7 * 24 * 60 * 60 * 1_000),
        ..Default::default()
    };
    for options in [other_access, other_window] {
        let created = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context())
            .create_with_options(&options)
            .await;
        assert_matches!(created, Err(CoreError::NamespaceExists { .. }));
    }
    assert_eq!(manifest_payloads(&store, &namespace_id).await, [manifest_1]);
    assert!(member_entry_keys(&store).await.is_empty());
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
    let source_boundary = boundary(&source);
    let target_boundary = boundary(&target);
    let source_manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&source);
    let target_manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&target);
    let store = RecordingStore::new(
        store,
        KeyPredicate::new(move |key| {
            key == source_boundary
                || key == target_boundary
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
    // The fork finds the target's tombstone before its first write, so it
    // writes and deletes nothing.
    assert_eq!(store.counts().create_if_absent_puts, 0);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert_eq!(store.counts().deletes, 0);
    assert!(store
        .list_prefix(&loonfs_objectstore::keys::snapshot_prefix(&source))
        .await
        .expect("fork snapshots")
        .is_empty());
}

/// Checks that a fork whose manifest 1 loses to a create, or to a create and a
/// delete, writes no fork snapshot and leaves its temporary snapshot to
/// expire.
#[tokio::test]
async fn a_fork_that_loses_target_publication_writes_no_fork_snapshot() {
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
        let records = snapshot_records(&store, &source).await;
        let [temporary] = &records[..] else {
            panic!("only the temporary snapshot, got {records:?}");
        };
        assert_matches!(
            temporary.owner,
            SnapshotOwner::User {
                name: None,
                expires_at_ms: Some(_),
                deleted_at_ms: None,
            }
        );
    }
}

/// Checks that a writer session's deletion over unfolded commits moves the
/// session's position onto the fold's manifest, so the attempt that
/// publishes the tombstone takes the head from the position. Between the
/// fold and the tombstone, the deletion reads no WAL object, and it reads
/// the manifest boundary only right after the fold's put and after its probe
/// for the fold's manifest.
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
    let (boundary, wal_prefix) = (boundary(&namespace_id), wal_prefix(&namespace_id));
    let between = &operations[fold + 1..tombstone];
    assert!(
        between.len() > 2
            && [&between[0], &between[between.len() - 1]]
                .iter()
                .all(|read| read.key() == boundary)
            && !between[1..between.len() - 1].iter().any(|operation| {
                operation.key() == boundary || operation.key().starts_with(&wal_prefix)
            }),
        "{between:?}"
    );
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert!(current.payload().status.is_deleted());
    assert_eq!(current.object_key, operations[tombstone].key());
}

fn source_and_target() -> (NamespaceId, NamespaceId) {
    (
        NamespaceId::parse("source").expect("source"),
        NamespaceId::parse("target").expect("target"),
    )
}

/// Checks that a fork call with the request of a fork that stopped after
/// manifest 1 writes the fork snapshot that manifest 1 records and publishes
/// manifest 2.
#[tokio::test]
async fn a_fork_that_stopped_after_manifest_one_is_finished_by_the_same_request() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (source, target) = source_and_target();
    let (_, manifest_1) =
        crate::test_support::half_done_fork(&store, &source, &target, &context()).await;
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
        .expect("finish the fork");
    let basis = manifest_1.fork_basis.as_ref().expect("a fork basis");
    assert_eq!(
        fork_snapshots(&store, &source).await,
        std::slice::from_ref(&basis.source_snapshot_id)
    );
    assert_eq!(
        manifest_payloads(&store, &target).await,
        [
            manifest_1.clone(),
            NamespaceManifestPayload {
                manifest_no: ManifestNo(2),
                ..manifest_1.activated()
            }
        ]
    );
    assert_eq!(
        forked,
        crate::test_support::namespace_store(&store, &target)
            .load_metadata()
            .await
            .expect("load the fork")
    );
}

/// Checks that a fork from another source and a fork at another snapshot
/// can't finish an initializing fork, and write nothing to it.
#[tokio::test]
async fn a_fork_request_that_differs_from_manifest_one_returns_namespace_exists() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (source, target) = source_and_target();
    let other = NamespaceId::parse("other").expect("other");
    let actor = loonfs_test_support::test_actor();
    let (_, manifest_1) =
        crate::test_support::half_done_fork(&store, &source, &target, &context()).await;
    let other_fixture = crate::test_support::NamespaceFixture::new(&store, &other, &context());
    other_fixture
        .create()
        .await
        .expect("create the other source");
    let from_other = other_fixture
        .engine()
        .fork_namespace(
            &target,
            &actor,
            crate::test_support::folded_manifest(&other_fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await;
    assert_matches!(from_other, Err(CoreError::NamespaceExists { .. }));
    let source_fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context());
    let named = source_fixture
        .namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some("named".to_owned()),
                expires_at_ms: None,
                deleted_at_ms: None,
            },
            crate::test_support::folded_manifest(&source_fixture.namespace)
                .await
                .expect("fold the WAL tail"),
            &context().clock(),
        )
        .await
        .expect("take a named snapshot");
    let at_named = source_fixture
        .engine()
        .fork_namespace_at_snapshot(&target, &actor, &named.snapshot_id)
        .await;
    assert_matches!(at_named, Err(CoreError::NamespaceExists { .. }));
    assert_eq!(manifest_payloads(&store, &target).await, [manifest_1]);
    assert!(fork_snapshots(&store, &source).await.is_empty());
}

/// Checks that LoonFS treats an initializing namespace as missing for reads,
/// commits, folds, and forks from it, while a maintenance read of its current
/// manifest still loads manifest 1.
#[tokio::test]
async fn an_initializing_namespace_is_not_found_except_by_create_fork_and_delete() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (source, target) = source_and_target();
    let (_, manifest_1) =
        crate::test_support::half_done_fork(&store, &source, &target, &context()).await;
    let namespace = crate::test_support::namespace_store(&store, &target);
    assert_matches!(
        namespace.load_metadata().await,
        Err(CoreError::NamespaceNotFound { .. })
    );
    let mut engine = NamespaceCommitEngine::new(namespace.clone(), context().writer_id);
    assert_matches!(
        publish(&mut engine, "refused").await,
        Err(CoreError::NamespaceNotFound { .. })
    );
    // A snapshot and a fork of the head fold the WAL tail first.
    assert_matches!(
        crate::test_support::folded_manifest(&namespace).await,
        Err(CoreError::NamespaceNotFound { .. })
    );
    let fixture = crate::test_support::NamespaceFixture::new(&store, &target, &context());
    let snapshot = fixture
        .engine()
        .create_snapshot(
            None,
            None,
            namespace
                .load_current_manifest()
                .await
                .expect("current")
                .manifest(),
        )
        .await;
    assert_matches!(snapshot, Err(CoreError::SnapshotUnavailable(_)));
    let fork = fixture
        .engine()
        .fork_namespace_at_snapshot(
            &NamespaceId::parse("nested").expect("nested"),
            &loonfs_test_support::test_actor(),
            &SnapshotId::generate(ManifestNo(1)),
        )
        .await;
    assert_matches!(fork, Err(CoreError::NamespaceNotFound { .. }));
    assert_eq!(
        namespace
            .load_current_manifest()
            .await
            .expect("a maintenance read of the current manifest")
            .payload(),
        &manifest_1
    );
    assert_eq!(manifest_payloads(&store, &target).await, [manifest_1]);
    assert!(snapshot_records(&store, &target).await.is_empty());
}

/// Checks that a delete of an initializing namespace publishes its tombstone
/// as manifest 2, with no claim manifest, fence, or other WAL object, and
/// checks `expected_head_seq` against manifest 1 first.
#[tokio::test]
async fn a_delete_of_an_initializing_namespace_publishes_the_tombstone_without_a_writer_session() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (source, target) = source_and_target();
    let (_, manifest_1) =
        crate::test_support::half_done_fork(&store, &source, &target, &context()).await;
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &target),
        context().writer_id,
    );
    let stale = engine
        .delete_namespace(loonfs_types::options::DeleteNamespaceOptions {
            expected_head_seq: Some(loonfs_types::CommitSeq(manifest_1.head_seq.0 + 1)),
        })
        .await;
    assert_matches!(stale, Err(CoreError::StaleHeadPrecondition { .. }));
    store.reset();
    let deleted = engine
        .delete_namespace(Default::default())
        .await
        .expect("delete the initializing namespace");
    let operations = store.take();
    assert_eq!(
        operations.len(),
        INITIALIZING_DELETE_STORE_OPERATIONS,
        "{operations:?}"
    );
    assert_eq!(deleted.head_seq, manifest_1.head_seq);
    let manifest_2 = metadata_manifest_object(&target, &ManifestNo(2));
    assert_eq!(
        store
            .list_prefix(&loonfs_objectstore::keys::namespace_prefix(&target))
            .await
            .expect("list the target"),
        [
            metadata_manifest_object(&target, &ManifestNo(1)),
            manifest_2
        ]
    );
    let manifests = manifest_payloads(&store, &target).await;
    let deleted_at_ms = manifests[1]
        .status
        .deleted_at_ms()
        .expect("manifest 2 is a tombstone");
    assert_eq!(
        manifests,
        [
            manifest_1.clone(),
            NamespaceManifestPayload {
                manifest_no: ManifestNo(2),
                ..manifest_1.tombstone(deleted_at_ms)
            }
        ]
    );
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context());
    let fork = fixture
        .engine()
        .fork_namespace(
            &target,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await;
    assert_matches!(fork, Err(CoreError::NamespaceDeleted { .. }));
}

/// Checks that a call that finishes a fork after the fork's temporary
/// snapshot expired returns `snapshot_not_found`, publishes no manifest, and
/// deletes no fork snapshot, whether or not a call wrote the fork snapshot
/// before the snapshot expired.
#[tokio::test]
async fn a_finishing_fork_whose_snapshot_ended_returns_snapshot_not_found_and_leaves_the_namespace_initializing(
) {
    for fork_snapshot_written in [false, true] {
        let directory = tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let (source, target) = source_and_target();
        let (_, manifest_1) =
            crate::test_support::half_done_fork(&store, &source, &target, &context()).await;
        if fork_snapshot_written {
            crate::test_support::write_fork_snapshot(&store, &manifest_1).await;
        }
        let later =
            crate::test_support::writer_at("writer", context().now_ms + OPERATION_SNAPSHOT_TTL_MS);
        let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &later);
        let finished = fixture
            .engine()
            .fork_namespace(
                &target,
                &loonfs_test_support::test_actor(),
                crate::test_support::folded_manifest(&fixture.namespace)
                    .await
                    .expect("fold the WAL tail"),
            )
            .await;
        assert_matches!(finished, Err(CoreError::SnapshotNotFound { .. }));
        assert_eq!(manifest_payloads(&store, &target).await, [manifest_1]);
        assert_eq!(
            fork_snapshots(&store, &source).await.len(),
            usize::from(fork_snapshot_written)
        );
    }
}

/// Checks that a call that finishes a fork of a deleted source returns
/// `namespace_deleted` and publishes no manifest.
#[tokio::test]
async fn a_finishing_fork_whose_source_was_deleted_returns_namespace_deleted() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (source, target) = source_and_target();
    let (temporary, manifest_1) =
        crate::test_support::half_done_fork(&store, &source, &target, &context()).await;
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context());
    fixture
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete the source");
    let finished = fixture
        .engine()
        .fork_namespace(
            &target,
            &loonfs_test_support::test_actor(),
            temporary.manifest(),
        )
        .await;
    assert_matches!(finished, Err(CoreError::NamespaceDeleted { .. }));
    assert_eq!(manifest_payloads(&store, &target).await, [manifest_1]);
    assert!(fork_snapshots(&store, &source).await.is_empty());
}

/// Checks that a call that finishes a fork, and whose manifest 2 put loses to
/// another call's identical manifest 2, returns success without another
/// attempt, and that its put of a fork snapshot that another call wrote keeps
/// that call's record.
#[tokio::test]
async fn a_finisher_whose_manifest_two_put_loses_to_another_finisher_returns_success() {
    let directory = tempdir().expect("directory");
    let (source, target) = source_and_target();
    let manifest_2_key = metadata_manifest_object(&target, &ManifestNo(2));
    let store = BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::exact(&manifest_2_key),
        OperationSelector::PutCreateIfAbsent,
    );
    let (_, manifest_1) =
        crate::test_support::half_done_fork(&store, &source, &target, &context()).await;
    crate::test_support::write_fork_snapshot(&store, &manifest_1).await;
    let written = snapshot_records(&store, &source).await;
    store.inner().reset();
    store.block_next();
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context());
    let finisher = fixture.engine();
    let other_finisher = async {
        store.wait_until_blocked().await;
        let namespace = crate::test_support::namespace_store(store.inner(), &target);
        let crate::namespace::control::NewestManifest::Manifest(current) = namespace
            .find_newest_manifest()
            .await
            .expect("find manifest 1")
        else {
            panic!("manifest 1 is the newest manifest");
        };
        namespace
            .update_manifest(Some(current), &context().clock(), |current| async move {
                Ok(crate::manifest::publish::ManifestChange::Next(
                    Box::new(current.payload().activated()),
                    (),
                ))
            })
            .await
            .expect("publish manifest 2");
        store.release();
    };
    let actor = loonfs_test_support::test_actor();
    let head = manifest_1
        .fork_basis
        .as_ref()
        .expect("a fork basis")
        .manifest
        .clone();
    let (finished, ()) = tokio::join!(
        finisher.fork_namespace(&target, &actor, head),
        other_finisher
    );
    let finished = finished.expect("the finisher returns success");
    let manifest_2_puts = store
        .inner()
        .take()
        .into_iter()
        .filter(|operation| {
            matches!(operation, RecordedOperation::Put { key, .. } if key.starts_with(&metadata_manifest_prefix(&target)))
        })
        .map(|operation| operation.key().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(manifest_2_puts, [manifest_2_key.clone(), manifest_2_key]);
    assert_eq!(
        manifest_payloads(&store, &target).await,
        [
            manifest_1.clone(),
            NamespaceManifestPayload {
                manifest_no: ManifestNo(2),
                ..manifest_1.activated()
            }
        ]
    );
    assert_eq!(snapshot_records(&store, &source).await, written);
    assert_eq!(
        finished,
        crate::test_support::namespace_store(&store, &target)
            .load_metadata()
            .await
            .expect("load the fork")
    );
}
