//! Namespace installation, numbered publication, and fork ownership contracts.

use super::{bootstrap::bootstrap_namespace, control::load_current_manifest, fork::fork_namespace};
use crate::commit_engine::NamespaceCommitEngine;
use crate::context::MutationContext;
use crate::path::read::load_current_metadata_view;
use crate::wal::tests::publish;
use loonfs_api::{AttributeInclusion, ErrorCode, ManifestNo, NamespaceId, WalNo, WriterId};
use loonfs_objectstore::{
    keys::{hint, metadata_manifest_object},
    local_fs_store::LocalFsStore,
    ObjectStore,
};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass, RecordingStore,
};
use tempfile::tempdir;

fn context() -> MutationContext {
    MutationContext {
        writer_id: WriterId::parse("writer").expect("writer"),
        now_ms: 1_000,
    }
}

#[test]
fn an_acl_namespace_begins_with_the_root_grants_as_its_root_access_row() {
    use super::bootstrap::bootstrap_metadata_state;
    use crate::metadata::AccessRevisionRecord;
    use loonfs_api::{
        AccessGrants, AccessRevisionNo, AccessRights, ActorId, ChangeSeq, NamespaceAccess,
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
            committed_seq: ChangeSeq(0),
            commit_id: loonfs_api::wire::control::genesis_commit_id(),
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
async fn creation_installs_hint_and_manifest_then_reads_genesis() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
    let manifest = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    let payload = manifest.envelope.payload();
    assert_eq!(payload.manifest_no, ManifestNo(1));
    assert!(payload.runs.is_empty());
    assert_eq!(payload.folded_wal_no, WalNo(0));
    let puts = store
        .snapshot()
        .into_iter()
        .filter(|operation| {
            matches!(
                operation,
                loonfs_test_support::stores::RecordedOperation::Put { .. }
            )
        })
        .map(|operation| operation.key().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        puts,
        vec![
            hint(&namespace_id),
            metadata_manifest_object(&namespace_id, &ManifestNo(1))
        ]
    );
    assert_eq!(store.counts().create_if_absent_puts, 2);
    let root = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view")
        .resolve_path(
            "/",
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("root");
    assert_eq!(root.inode_id, loonfs_api::ROOT_INODE_ID);
}

#[tokio::test]
async fn two_creations_race_at_manifest_one() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(metadata_manifest_object(&namespace_id, &ManifestNo(1))),
        OperationClass::PutCreateIfAbsent,
    );
    store.block_next();
    let context = context();
    let actor_id = loonfs_test_support::test_actor();
    let (loser, winner) = futures::join!(
        bootstrap_namespace(
            &store,
            &namespace_id,
            &context,
            &actor_id,
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false
        ),
        async {
            store.wait_until_blocked().await;
            let result = bootstrap_namespace(
                store.inner(),
                &namespace_id,
                &context,
                &loonfs_test_support::test_actor(),
                &loonfs_api::NamespaceAccess::Unrestricted {},
                false,
            )
            .await;
            store.release();
            result
        }
    );
    winner.expect("winner");
    assert_eq!(loser.expect_err("loser").code(), ErrorCode::NamespaceExists);
}

/// Lands the first manifest-1 put, lets another writer take the namespace,
/// then reports the put's outcome as unknown.
#[derive(Debug)]
struct LostCreateResponseStore {
    inner: LocalFsStore,
    namespace_id: NamespaceId,
    fired: std::sync::Mutex<bool>,
}

#[async_trait::async_trait]
impl ObjectStore for LostCreateResponseStore {
    loonfs_test_support::delegate_object_store!(self => self.inner; except put);

    async fn put(
        &self,
        key: &str,
        bytes: bytes::Bytes,
        mode: loonfs_objectstore::PutMode,
    ) -> Result<loonfs_objectstore::ObjectMetadata, loonfs_objectstore::ObjectStoreError> {
        let first = key == metadata_manifest_object(&self.namespace_id, &ManifestNo(1))
            && !std::mem::replace(
                &mut *self
                    .fired
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                true,
            );
        let landed = self.inner.put(key, bytes, mode).await?;
        if !first {
            return Ok(landed);
        }
        super::writer_epoch::acquire_writer_epoch(&self.inner, &self.namespace_id, &context())
            .await
            .expect("another writer takes the namespace");
        Err(loonfs_objectstore::ObjectStoreError::transport(
            key,
            "response lost",
        ))
    }
}

#[tokio::test]
async fn a_plain_create_whose_response_was_lost_cannot_claim_an_advanced_namespace() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("lost-response").expect("namespace");
    let store = LostCreateResponseStore {
        inner: LocalFsStore::new(directory.path()).expect("store"),
        namespace_id: namespace_id.clone(),
        fired: std::sync::Mutex::new(false),
    };
    let error = bootstrap_namespace(
        &store,
        &namespace_id,
        &context(),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect_err("identical first manifests do not prove who created the namespace");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    let current = load_current_manifest(&store.inner, &namespace_id)
        .await
        .expect("current manifest");
    assert_eq!(current.envelope.payload().manifest_no, ManifestNo(2));
}

#[tokio::test]
async fn an_ambiguous_first_manifest_confirms_only_a_forks_creation() {
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let source_key = metadata_manifest_object(&source, &ManifestNo(1));
    let target_key = metadata_manifest_object(&target, &ManifestNo(1));
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new(move |key| key == source_key || key == target_key),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("response lost".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(2);
    let actor_id = loonfs_test_support::test_actor();
    let error = bootstrap_namespace(
        &store,
        &source,
        &context(),
        &actor_id,
        &loonfs_api::NamespaceAccess::unrestricted(),
        false,
    )
    .await
    .expect_err("plain create cannot prove authorship from matching bytes");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    bootstrap_namespace(
        &store,
        &source,
        &context(),
        &actor_id,
        &loonfs_api::NamespaceAccess::unrestricted(),
        true,
    )
    .await
    .expect("allow existing returns the landed namespace");
    let created = fork_namespace(&store, &source, &target, &actor_id, None, &context())
        .await
        .expect("fork source pin proves authorship from matching bytes");
    assert!(created.fork_basis.is_some());
    assert_eq!(store.attempts(), 2);
    assert_eq!(store.remaining(), 0);
}

#[tokio::test]
async fn nested_forks_read_copied_runs_without_source_control_reads() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let nested = NamespaceId::parse("nested").expect("nested");
    bootstrap_namespace(
        &store,
        &source,
        &context(),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
    publish(
        &mut NamespaceCommitEngine::new(source.clone()),
        &store,
        "inherited",
    )
    .await
    .expect("source data");
    fork_namespace(
        &store,
        &source,
        &target,
        &loonfs_test_support::test_actor(),
        None,
        &context(),
    )
    .await
    .expect("fork");
    let target_manifest = load_current_manifest(&store, &target)
        .await
        .expect("target manifest");
    assert_eq!(target_manifest.state.manifest.manifest_no, ManifestNo(1));
    assert!(target_manifest
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .all(|segment| segment.owner_namespace_id == source));
    publish(
        &mut NamespaceCommitEngine::new(target.clone()),
        &store,
        "child",
    )
    .await
    .expect("unflushed child data");
    fork_namespace(
        &store,
        &target,
        &nested,
        &loonfs_test_support::test_actor(),
        None,
        &context(),
    )
    .await
    .expect("nested fork");
    let manifest = load_current_manifest(&store, &nested)
        .await
        .expect("nested manifest");
    assert_eq!(manifest.state.manifest.manifest_no, ManifestNo(1));
    let owners = manifest
        .envelope
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
    let view = load_current_metadata_view(&store, &nested)
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
    assert_eq!(store.count(OperationClass::Any), 0);
}

#[tokio::test]
async fn a_pending_hint_cannot_name_a_manifest_collected_after_its_replacement() {
    use loonfs_test_support::stores::MetadataMapStore;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("hint-gc").expect("namespace");
    let grace = crate::limits::GC_MIN_GRACE_WINDOW_MS;
    let config = crate::gc::GcConfig {
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
            metadata.last_modified_ms = Some(grace + 1);
            metadata
        },
    );
    let store = BlockingStore::new(
        store,
        KeyPredicate::hint(&namespace_id),
        OperationClass::CompareAndSwap,
    );
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut engine, &store, "seed").await.expect("seed");
    store.block_next();
    let (hint_result, ()) = futures::join!(
        super::control::raise_namespace_hint(&store, &namespace_id, WalNo(2), None),
        async {
            store.wait_until_blocked().await;
            crate::checkpoint::flush_wal(store.inner(), &namespace_id)
                .await
                .expect("replace manifest");
            crate::checkpoint::advance_retention_floor(store.inner(), &namespace_id)
                .await
                .expect("advance floor");
            let aged = MutationContext {
                now_ms: grace + 1,
                ..context()
            };
            crate::gc::gc_namespace(store.inner(), &namespace_id, &config, &aged)
                .await
                .expect("collect during pending hint");
            assert!(store
                .inner()
                .head(&metadata_manifest_object(&namespace_id, &ManifestNo(2)))
                .await
                .expect("predecessor")
                .is_some());
            store.release();
        }
    );
    hint_result.expect("finish pending hint");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("discover past lagging hint");
    assert_eq!(current.state.manifest.manifest_no, ManifestNo(4));
    load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("read committed state");
    super::control::raise_namespace_hint(&store, &namespace_id, WalNo(2), None)
        .await
        .expect("refresh hint");
    let aged = MutationContext {
        now_ms: 2 * grace + 2,
        ..context()
    };
    crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect("collect after grace");
    assert!(store
        .head(&metadata_manifest_object(&namespace_id, &ManifestNo(2)))
        .await
        .expect("old manifest")
        .is_none());
}

#[tokio::test]
async fn fork_into_a_deleted_id_writes_no_source_pin() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let source = NamespaceId::parse("source").expect("namespace");
    let target = NamespaceId::parse("deleted-target").expect("namespace");
    let context = context();
    for namespace_id in [&source, &target] {
        bootstrap_namespace(
            &store,
            namespace_id,
            &context,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::unrestricted(),
            false,
        )
        .await
        .expect("bootstrap");
    }
    NamespaceCommitEngine::new(target.clone())
        .delete_namespace(&store, Default::default(), &context)
        .await
        .expect("delete target");
    store.reset();
    let error = fork_namespace(
        &store,
        &source,
        &target,
        &loonfs_test_support::test_actor(),
        None,
        &context,
    )
    .await
    .expect_err("deleted target");
    assert_eq!(error.code(), ErrorCode::NamespaceDeleted);
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert_eq!(store.counts().deletes, 0);
    assert!(store
        .list_prefix(&loonfs_objectstore::keys::checkpoint_prefix(&source))
        .await
        .expect("source pins")
        .is_empty());
}
