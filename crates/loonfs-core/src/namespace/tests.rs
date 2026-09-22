//! Namespace installation, numbered publication, and fork ownership contracts.

use super::{
    bootstrap::bootstrap_namespace,
    control::{load_current_manifest, load_namespace_hint},
    fork::fork_namespace,
    read_anchor::load_read_anchor,
};
use crate::checkpoint::record::load_checkpoint_record;
use crate::commit_engine::NamespaceCommitEngine;
use crate::context::MutationContext;
use crate::path::read::load_current_metadata_view;
use crate::wal::tests::publish;
use loonfs_api::wire::control::CheckpointOwner;
use loonfs_api::{
    AttributeInclusion, CheckpointId, ErrorCode, ManifestNo, NamespaceGeneration, NamespaceId,
    WalNo, WriterId,
};
use loonfs_objectstore::{
    keys::{content_store, hint, metadata_manifest_object, wal_segment_prefix},
    layout::wal_no_of,
    local_fs_store::LocalFsStore,
    ObjectStore,
};
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass, RecordingStore};
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
        ChangeSeq(0),
    );
    assert_eq!(
        state.access_revisions(),
        &[AccessRevisionRecord {
            inode_id: ROOT_INODE_ID,
            access_revision_no: AccessRevisionNo(0),
            committed_seq: ChangeSeq(0),
            commit_id: loonfs_api::wire::control::genesis_commit_id(),
            delta_index: 0,
            updated_by: ActorId::loonfs(),
            updated_at_ms: 1_000,
            boundary: false,
            grants: root_grants,
        }]
    );
    let state = bootstrap_metadata_state(1_000, &NamespaceAccess::Unrestricted {}, ChangeSeq(0));
    assert!(state.access_revisions().is_empty());
}

#[tokio::test]
async fn creation_installs_descriptor_hint_and_manifest_then_reads_genesis() {
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
    assert_eq!(payload.last_folded_wal_no, WalNo(0));
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
            content_store(&payload.content_store_id),
            hint(&namespace_id),
            metadata_manifest_object(&namespace_id, &ManifestNo(1))
        ]
    );
    assert_eq!(store.counts().create_if_absent_puts, 3);
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
async fn recreating_a_deleted_namespace_publishes_an_empty_next_generation() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("recreated").expect("namespace");
    let context = context();
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
    crate::test_support::ops::write_file_bytes(
        &store,
        &namespace_id,
        "/one.txt",
        b"one",
        &context,
        None,
    )
    .await
    .expect("write first file");
    crate::test_support::ops::write_file_bytes(
        &store,
        &namespace_id,
        "/two.txt",
        b"two",
        &context,
        None,
    )
    .await
    .expect("write second file");
    NamespaceCommitEngine::new(namespace_id.clone())
        .delete_namespace(&store, Default::default(), &context)
        .await
        .expect("delete");
    let tombstone_anchor = load_read_anchor(&store, &namespace_id)
        .await
        .expect("tombstone anchor");
    let tombstone = tombstone_anchor.manifest.envelope.payload().clone();
    let tombstone_ref = tombstone_anchor.manifest.state.manifest.clone();
    let wal_keys = store
        .list_prefix(&wal_segment_prefix(&namespace_id))
        .await
        .expect("list wal objects");
    let wal_tip = wal_keys
        .iter()
        .filter_map(|key| wal_no_of(key))
        .max()
        .expect("the deleted generation published wal objects");
    assert_eq!(tombstone.last_folded_wal_no, wal_tip);
    for key in &wal_keys {
        store.delete(key).await.expect("collect wal object");
    }

    let recreated = bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("recreate");
    assert_eq!(recreated.generation, NamespaceGeneration(2));

    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("recreated manifest");
    let payload = current.envelope.payload();
    assert_eq!(
        payload.manifest_no,
        tombstone.manifest_no.successor().expect("next manifest")
    );
    assert_eq!(payload.generation, NamespaceGeneration(2));
    assert_eq!(payload.generation_first_manifest_no, payload.manifest_no);
    let genesis_seq = tombstone.head_seq.successor().expect("next sequence");
    assert_eq!(payload.head_seq, genesis_seq);
    assert_eq!(payload.base_seq, genesis_seq);
    assert_eq!(payload.retention_floor_seq, genesis_seq);
    assert!(payload.next_inode_id >= tombstone.next_inode_id);
    assert!(payload.writer_epoch > tombstone.writer_epoch);
    assert_ne!(payload.content_store_id, tombstone.content_store_id);
    assert!(payload.runs.is_empty());
    assert_eq!(payload.last_folded_wal_no, wal_tip);

    let retired_id = CheckpointId::retired(&namespace_id, tombstone.manifest_no);
    let retired = load_checkpoint_record(&store, &namespace_id, &retired_id)
        .await
        .expect("load retired pin")
        .expect("retired pin");
    assert_eq!(retired.state.owner, CheckpointOwner::Retired {});
    assert_eq!(retired.state.manifest(), tombstone_ref);
    let raised = load_namespace_hint(&store, &namespace_id)
        .await
        .expect("raised hint");
    assert_eq!(raised.state.manifest_no, payload.manifest_no);
    assert_eq!(raised.state.wal_no, wal_tip);

    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("recreated view");
    let page = view
        .list_path_page(
            "/",
            loonfs_api::PageRequest {
                limit: loonfs_test_support::ids::page_limit(10),
                cursor: None,
            },
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("list root");
    assert!(page.items.is_empty());
    let error = crate::protocol::list_changes_after(
        &view,
        tombstone.head_seq,
        loonfs_test_support::ids::page_limit(10),
    )
    .await
    .expect_err("old generation cursor");
    assert!(matches!(
        error,
        crate::error::CoreError::RebootstrapRequired {
            after_seq,
            retention_floor_seq,
        } if after_seq == tombstone.head_seq && retention_floor_seq == genesis_seq
    ));
}

#[tokio::test]
async fn recreating_a_namespace_fences_the_session_that_deleted_it() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("fenced-recreation").expect("namespace");
    let context = context();
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut engine, &store, "before-delete")
        .await
        .expect("commit");
    engine
        .delete_namespace(&store, Default::default(), &context)
        .await
        .expect("delete");
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("recreate");

    let error = publish(&mut engine, &store, "after-recreation")
        .await
        .expect_err("old session is fenced");
    assert_eq!(error.code(), ErrorCode::WriterFenced);
}
