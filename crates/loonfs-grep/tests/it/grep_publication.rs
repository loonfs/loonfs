//! Publication recovery must not clean up resources retained by a successor.

use crate::common::{control, GrepHost};
use loonfs::{CreateNamespaceOptions, FsWriter, PutFileOptions, SharedObjectStore};
use loonfs_api::ManifestNo;
use loonfs_grep::keyspace::manifest_key;
use loonfs_grep::manifest::{load_current_grep_manifest, GrepIndexStatus};
use loonfs_grep::{GramIndexBuildPolicy, GrepError, GREP_GC_GRACE_WINDOW_MS};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::num::NonZeroUsize;
use std::sync::Arc;

#[tokio::test]
async fn a_collected_publication_does_not_abandon_a_successors_backfill_checkpoint() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("publication-recovery");
    let writer = FsWriter::builder_with_store(base.clone())
        .writer_id("publication-recovery")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    for path in ["/first", "/second"] {
        writer
            .put_file_bytes(
                &namespace_id,
                path,
                b"needle\n",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
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
        let current = load_current_grep_manifest(&base, &namespace_id)
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
        control::checkpoint_record(&base, &namespace_id, &checkpoint_id)
            .await
            .is_some(),
        "uncertain publication cleanup must preserve the successor's checkpoint"
    );
    assert!(matches!(outcome, Err(GrepError::StoreUnavailable { .. })));
}
