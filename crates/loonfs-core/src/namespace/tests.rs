//! Namespace installation, numbered publication, and fork ownership contracts.

use crate::commit_engine::NamespaceCommitEngine;
use crate::context::MutationContext;
use crate::manifest::tests::control_requests;
use crate::wal::tests::publish;
use loonfs_objectstore::{
    keys::{hint, metadata_manifest_object},
    local_fs_store::LocalFsStore,
    ObjectStore,
};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector, RecordingStore,
};
use loonfs_types::{AttributeInclusion, ErrorCode, ManifestNo, NamespaceId};
use tempfile::tempdir;

fn context() -> MutationContext {
    crate::test_support::mutation_context("writer", 1_000)
}

#[test]
fn an_acl_namespace_begins_with_the_root_grants_as_its_root_access_row() {
    use super::bootstrap::bootstrap_metadata_state;
    use crate::metadata::AccessRevisionRecord;
    use loonfs_types::{
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
    winner.expect("winner");
    assert_eq!(loser.expect_err("loser").code(), ErrorCode::NamespaceExists);
}

/// Checks that a create and a fork each return the state that their manifest
/// 1 records, and that neither reads the new namespace after its absence
/// check.
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
    assert_eq!(
        control_requests(&source, &store.take()),
        vec![
            ("get", hint(&source)),
            ("put", hint(&source)),
            ("put", metadata_manifest_object(&source, &ManifestNo(1))),
        ]
    );
    assert_eq!(
        created,
        crate::test_support::namespace_store(&store, &source)
            .load_metadata()
            .await
            .expect("load the created namespace")
    );
    publish(
        &mut NamespaceCommitEngine::new(crate::test_support::namespace_store(&store, &source)),
        "inherited",
    )
    .await
    .expect("source data");
    store.reset();
    let forked = crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .engine()
        .fork_namespace(&target, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    assert_eq!(
        control_requests(&target, &store.take()),
        vec![
            ("get", hint(&target)),
            ("put", hint(&target)),
            ("put", metadata_manifest_object(&target, &ManifestNo(1))),
        ]
    );
    assert!(forked.response.fork_basis.is_some());
    assert_eq!(
        forked.response,
        crate::test_support::namespace_store(&store, &target)
            .load_metadata()
            .await
            .expect("load the fork")
    );
    // The fork folded the source's tail, and the report includes the fold's
    // manifest so the runtime can raise the source's hint.
    assert_eq!(
        forked.published.map(|published| published.manifest),
        Some(
            crate::test_support::namespace_store(&store, &source)
                .load_current_manifest()
                .await
                .expect("source manifest")
                .state
                .manifest()
        )
    );
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
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("response lost".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(2);
    let actor_id = loonfs_test_support::test_actor();
    let error = crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .create()
        .await
        .expect_err("plain create cannot prove authorship from matching bytes");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    let existing = crate::test_support::namespace_store(&store, &source)
        .load_metadata()
        .await
        .expect("landed namespace");
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
        .expect("allow existing returns the landed namespace");
    assert_eq!(adopted, existing);
    let created = crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .engine()
        .fork_namespace(&target, &actor_id, None)
        .await
        .expect("fork source pin proves authorship from matching bytes");
    assert!(created.response.fork_basis.is_some());
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
    crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .create()
        .await
        .expect("create");
    publish(
        &mut NamespaceCommitEngine::new(crate::test_support::namespace_store(&store, &source)),
        "inherited",
    )
    .await
    .expect("source data");
    crate::test_support::NamespaceFixture::new(&store, &source, &context())
        .engine()
        .fork_namespace(&target, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    let target_manifest = crate::test_support::namespace_store(&store, &target)
        .load_current_manifest()
        .await
        .expect("target manifest");
    assert!(target_manifest
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .all(|segment| segment.owner_namespace_id == source));
    publish(
        &mut NamespaceCommitEngine::new(crate::test_support::namespace_store(&store, &target)),
        "child",
    )
    .await
    .expect("unfolded child data");
    crate::test_support::NamespaceFixture::new(&store, &target, &context())
        .engine()
        .fork_namespace(&nested, &loonfs_test_support::test_actor(), None)
        .await
        .expect("nested fork");
    let manifest = crate::test_support::namespace_store(&store, &nested)
        .load_current_manifest()
        .await
        .expect("nested manifest");
    let owners = manifest
        .state
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
        ..Default::default()
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
    let mut engine =
        NamespaceCommitEngine::new(crate::test_support::namespace_store(&store, &namespace_id));
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
            crate::test_support::NamespaceFixture::new(
                store.inner(),
                &namespace_id,
                &crate::test_support::mutation_context(
                    "test-writer",
                    crate::test_support::grace_elapsed_after(context().now_ms),
                ),
            )
            .engine()
            .advance_retention_floor(crate::manifest::RetentionTarget::Head)
            .await
            .expect("advance floor");
            // The runtime that published the fold and the advance raises the
            // hint after them, while the raise to manifest 2 is still pending.
            crate::test_support::namespace_store(store.inner(), &namespace_id)
                .raise_hint(ManifestNo(4), None)
                .await
                .expect("raise the hint past the pending raise");
            let aged = MutationContext {
                now_ms: grace + 1,
                ..context()
            };
            crate::test_support::namespace_store(store.inner(), &namespace_id)
                .gc(None, &options, &aged)
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
    assert_eq!(current.state.manifest().manifest_no, ManifestNo(4));
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
    let aged = MutationContext {
        now_ms: 2 * grace + 2,
        ..context()
    };
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &aged)
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
        crate::test_support::NamespaceFixture::new(&store, namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
    }
    NamespaceCommitEngine::new(crate::test_support::namespace_store(&store, &target))
        .delete_namespace(Default::default(), &context)
        .await
        .expect("delete target");
    store.reset();
    let error = crate::test_support::NamespaceFixture::new(&store, &source, &context)
        .engine()
        .fork_namespace(&target, &loonfs_test_support::test_actor(), None)
        .await
        .expect_err("deleted target");
    assert_eq!(error.code(), ErrorCode::NamespaceDeleted);
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert_eq!(store.counts().deletes, 0);
    assert!(store
        .list_prefix(&loonfs_objectstore::keys::pin_prefix(&source))
        .await
        .expect("source pins")
        .is_empty());
}

#[tokio::test]
async fn a_fork_that_loses_target_publication_deletes_its_source_pin() {
    for deleted in [false, true] {
        let directory = tempdir().expect("directory");
        let source = NamespaceId::parse("source").expect("source");
        let target = NamespaceId::parse("target").expect("target");
        let pin_prefix = loonfs_objectstore::keys::pin_prefix(&source);
        let store = BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(&pin_prefix),
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
        let fork = engine.fork_namespace(&target, &actor, None);
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
            .list_prefix(&pin_prefix)
            .await
            .expect("source pins")
            .is_empty());
    }
}
