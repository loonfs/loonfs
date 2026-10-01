//! Publication recovery must not clean up resources a manifest may name.

use crate::common::{control, default_page_limit, GrepHost};
use loonfs::{LoonFs, SharedObjectStore};
use loonfs_grep::keyspace::manifest_key;
use loonfs_grep::manifest::{load_current_grep_manifest, GrepIndexStatus};
use loonfs_grep::{GramIndexBuildPolicy, GREP_GC_GRACE_WINDOW_MS};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use loonfs_types::{ErrorCode, ManifestNo, PageRequest};
use std::num::NonZeroUsize;
use std::sync::Arc;

#[tokio::test]
async fn a_collected_publication_does_not_abandon_a_successors_backfill_checkpoint() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("publication-recovery");
    let writer = LoonFs::builder_with_store(base.clone())
        .writer_id("publication-recovery")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for path in ["/first", "/second"] {
        namespace
            .put_file(path, b"needle\n", &loonfs_test_support::test_actor())
            .await
            .expect("file");
    }

    let first_key = manifest_key(&namespace_id, &ManifestNo(1));
    let failing = FailStore::new(
        base.clone(),
        KeyPredicate::exact(first_key.clone()),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("lost enable acknowledgement".to_owned()),
    )
    .apply_then_fail();
    failing.fail_next(1);
    let blocking = Arc::new(BlockingStore::new(
        failing,
        KeyPredicate::exact(first_key.clone()),
        OperationClass::Get,
    ));
    let uncertain_store: SharedObjectStore = blocking.clone();
    let uncertain = GrepHost::new(&uncertain_store, "uncertain-enable").await;
    let successor = GrepHost::new(&base, "successor").await;
    blocking.block_next();

    let enable = uncertain.worker.enable(&namespace_id);
    let advance = async {
        blocking.wait_until_blocked().await;
        successor
            .worker
            .build_step(
                &namespace_id,
                GramIndexBuildPolicy {
                    max_files_per_step: NonZeroUsize::MIN,
                    ..Default::default()
                },
            )
            .await
            .expect("publish a partial backfill");
        let current =
            load_current_grep_manifest(&base, &namespace_id, crate::common::observation())
                .await
                .expect("load successor")
                .expect("enabled");
        assert_eq!(current.manifest_no(), ManifestNo(2));
        let checkpoint_id = match current.manifest_state().status() {
            GrepIndexStatus::Backfilling { checkpoint_id, .. } => Some(checkpoint_id.clone()),
            _ => None,
        }
        .expect("the successor must still need its checkpoint");
        let modified = base
            .head(&manifest_key(&namespace_id, &ManifestNo(2)))
            .await
            .expect("head")
            .expect("successor exists")
            .last_modified_ms
            .expect("timestamp");
        // Advance GC's explicit clock past manifest retention without sleeping.
        successor
            .worker
            .garbage_collect_namespace(&namespace_id, modified + GREP_GC_GRACE_WINDOW_MS + 1)
            .await
            .expect("collect the old manifest");
        assert!(base.get(&first_key, None).await.expect("first").is_none());
        blocking.release();
        checkpoint_id
    };
    let (outcome, checkpoint_id) = tokio::join!(enable, advance);
    assert!(
        control::pin(&base, &namespace_id, &checkpoint_id)
            .await
            .is_some(),
        "uncertain publication cleanup must preserve the successor's checkpoint"
    );
    assert!(matches!(outcome, Err(error) if error.code() == ErrorCode::OutcomeUnknown));
}

#[tokio::test]
async fn an_enable_whose_put_reads_back_absent_fails_and_keeps_its_checkpoint() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("unknown-enable");
    let writer = LoonFs::builder_with_store(base.clone())
        .writer_id("unknown-enable")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let failing = FailStore::new(
        base.clone(),
        KeyPredicate::exact(manifest_key(&namespace_id, &ManifestNo(1))),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("lost enable request".to_owned()),
    );
    failing.fail_next(1);
    let failing: SharedObjectStore = Arc::new(failing);
    let host = GrepHost::new(&failing, "unknown-enable").await;

    let outcome = host.worker.enable(&namespace_id).await;

    assert!(matches!(outcome, Err(error) if error.code() == ErrorCode::OutcomeUnknown));
    assert!(
        load_current_grep_manifest(&base, &namespace_id, crate::common::observation())
            .await
            .expect("load")
            .is_none()
    );
    let checkpoints = host
        .maintenance
        .list_checkpoints(&namespace_id)
        .page(PageRequest {
            limit: default_page_limit(),
            cursor: None,
        })
        .await
        .expect("list checkpoints")
        .checkpoints;
    assert_eq!(
        checkpoints.len(),
        1,
        "an unknown outcome must keep the backfill checkpoint the put may name"
    );
}
