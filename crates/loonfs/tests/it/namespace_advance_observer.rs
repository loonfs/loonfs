//! Namespace-advance hint delivery.

#![allow(clippy::panic)]
// The panicking observer under test is written as a `panic!` closure.

use loonfs::{
    CreateNamespaceOptions, FsBackgroundWork, FsWriter, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceStepConclusion, MaintenanceStepReport, NamespaceAdvanceHint,
    PutFileOptions, Result, SharedObjectStore,
};
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[tokio::test]
async fn registered_observer_sees_one_hint_per_publication() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observer_hints = observed.clone();
    let writer = FsWriter::builder_with_store(store)
        .writer_id("observer-writer")
        .min_publish_interval_ms(0)
        .namespace_advance_observer(move |hint| {
            observer_hints
                .lock()
                .expect("observer hints lock poisoned")
                .push(hint);
        })
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse("observer").expect("namespace id");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    assert!(
        observed
            .lock()
            .expect("observer hints lock poisoned")
            .is_empty(),
        "namespace bootstrap is not a committed mutation publication"
    );

    let response = writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"observer needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish file");
    assert_eq!(response.committed_seq, ChangeSeq(1));
    assert_eq!(
        *observed.lock().expect("observer hints lock poisoned"),
        vec![NamespaceAdvanceHint {
            namespace_id,
            through_seq: ChangeSeq(1),
        }]
    );
    writer.shutdown().await.expect("shutdown");
}

const SUBSCRIBING_JOB: MaintenanceJobId = MaintenanceJobId::new("namespace-advance-subscriber");

/// A job that says publications concern it, and records which namespaces it
/// was stepped for.
#[derive(Default)]
struct SubscribingJob {
    steps: Mutex<Vec<NamespaceId>>,
}

impl SubscribingJob {
    fn stepped(&self) -> Vec<NamespaceId> {
        self.steps.lock().expect("steps lock poisoned").clone()
    }
}

#[async_trait::async_trait]
impl MaintenanceJob for SubscribingJob {
    fn id(&self) -> MaintenanceJobId {
        SUBSCRIBING_JOB
    }

    fn nudged_by_publications(&self) -> bool {
        true
    }

    async fn step(
        &self,
        namespace_id: &NamespaceId,
        _continuation: Option<&str>,
    ) -> Result<MaintenanceStepReport> {
        self.steps
            .lock()
            .expect("steps lock poisoned")
            .push(namespace_id.clone());
        Ok(MaintenanceStepReport::concluded(
            MaintenanceStepConclusion::Idle,
        ))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }
}

#[tokio::test]
async fn an_observer_panic_leaves_the_commit_the_publisher_and_maintenance_intact() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let writer = FsWriter::builder_with_store(store)
        .writer_id("observer-panic-writer")
        .min_publish_interval_ms(0)
        .background_work(FsBackgroundWork::Enabled)
        .namespace_advance_observer(|_| panic!("observer failure"))
        .build()
        .await
        .expect("writer");
    let job = Arc::new(SubscribingJob::default());
    writer
        .register_maintenance_job(job.clone())
        .expect("register the subscribing job");
    let namespace_id = NamespaceId::parse("observer-panic").expect("namespace id");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    let first = writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"first\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("the commit was already durable when the observer panicked");
    assert_eq!(first.committed_seq, ChangeSeq(1));
    assert_eq!(
        writer
            .reader()
            .get_file_bytes(&namespace_id, "/note.txt")
            .await
            .expect("read file")
            .bytes,
        b"first\n"
    );

    let second = writer
        .put_file_bytes(
            &namespace_id,
            "/second.txt",
            b"second\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("the namespace publisher keeps publishing");
    assert_eq!(second.committed_seq, ChangeSeq(2));

    writer
        .flush_background()
        .await
        .expect("settle the nudged job");
    assert!(
        job.stepped().contains(&namespace_id),
        "a publication nudges its subscribers whatever the host observer does"
    );
    writer
        .shutdown()
        .await
        .expect("an observer panic is not a publication panic");
}
