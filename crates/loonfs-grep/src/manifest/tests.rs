//! Publication confirmation at the metadata budget boundary.

use super::*;
use loonfs::{Deadline, StoreFailureClass, METADATA_PUBLICATION_BUDGET_MS};
use loonfs_api::{ManifestNo, RunNo};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationClass, RecordingStore,
};
use std::sync::Arc;

async fn publication_returning_at_budget(drop_acknowledgement: bool) {
    for elapsed_ms in [
        METADATA_PUBLICATION_BUDGET_MS,
        METADATA_PUBLICATION_BUDGET_MS + 1,
    ] {
        let directory = tempfile::tempdir().expect("directory");
        let namespace_id = namespace_id("budget");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let timer = Arc::new(ManualClock::new(0));
        let deadline = Deadline::start(timer.clone());
        let state = |manifest_no| {
            GrepManifestState::new(
                namespace_id.clone(),
                manifest_no,
                GrepIndexStatus::Disabled {},
                GrepIndexState {
                    next_run_no: RunNo(0),
                    reorganize: None,
                },
                Vec::new(),
            )
            .expect("manifest")
        };
        let first = publish_grep_manifest(&store, None, &state(ManifestNo(1)), &deadline)
            .await
            .expect("first");
        let object_key = crate::keyspace::manifest_key(&namespace_id, &ManifestNo(2));
        store.reset();
        let store =
            MetadataMapStore::new(store, KeyPredicate::exact(&object_key), move |metadata| {
                timer.advance_ms(elapsed_ms);
                metadata
            });
        let store = FailStore::new(
            store,
            KeyPredicate::exact(&object_key),
            OperationClass::PutCreateIfAbsent,
            InjectedError::Transport("lost acknowledgement".to_owned()),
        )
        .apply_then_fail();
        if drop_acknowledgement {
            store.fail_next(1);
        }
        let outcome =
            publish_grep_manifest(&store, Some(&first), &state(ManifestNo(2)), &deadline).await;
        if elapsed_ms > METADATA_PUBLICATION_BUDGET_MS {
            assert!(matches!(outcome, Err(crate::GrepError::StoreUnavailable {
                object_key: actual_key, message, class: StoreFailureClass::RetryableTransport,
            }) if actual_key == object_key && message == format!(
                "manifest publication outcome is unknown after {elapsed_ms}ms (budget {METADATA_PUBLICATION_BUDGET_MS}ms)"
            )));
            assert_eq!(store.inner().inner().counts().compare_and_swaps, 0);
        } else {
            assert_eq!(outcome.expect("within budget").manifest_no(), ManifestNo(2));
            assert_eq!(store.inner().inner().counts().compare_and_swaps, 1);
        }
        assert_eq!(store.remaining(), 0);
        assert_eq!(store.inner().inner().counts().create_if_absent_puts, 1);
        if drop_acknowledgement {
            assert!(store
                .inner()
                .inner()
                .take_gets()
                .iter()
                .any(|(key, _)| key == &object_key));
        }
    }
}

#[tokio::test]
async fn a_successful_manifest_put_must_return_within_its_budget() {
    publication_returning_at_budget(false).await;
}

#[tokio::test]
async fn an_ambiguous_manifest_read_back_must_finish_within_its_budget() {
    publication_returning_at_budget(true).await;
}
