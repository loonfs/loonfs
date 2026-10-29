//! Manifest publications that build on a live session's writer position,
//! which put the position's successor without a request first and read
//! neither the hint nor a WAL object.

use super::*;
use crate::InlineContentPolicy;
use loonfs_test_support::stores::RecordedOperation;

/// A store that records every request that reaches it, under a gate that
/// parks every request for the namespace's hint once a test arms it, so a
/// publication that read the hint would never return.
type GatedStore = BlockingStore<RecordingStore<LocalFsStore>>;

struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<GatedStore>,
    writer: crate::LoonFs<crate::Writable>,
    namespace_id: NamespaceId,
    namespace: crate::Namespace<crate::Writable>,
}

impl Fixture {
    /// Creates a writer built by `configure`, with its namespace created and
    /// opened, and the session's first commit published.
    ///
    /// The writer's monotonic timer doesn't move, so the hint raise after the
    /// session's claim is the last one: every later publication is less than
    /// `HINT_RAISE_INTERVAL_MS` after it, and skips its raise.
    async fn new(
        configure: impl FnOnce(
            crate::LoonFsBuilder<crate::Writable>,
        ) -> crate::LoonFsBuilder<crate::Writable>,
    ) -> Self {
        let directory = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("basis").expect("namespace id");
        let store = Arc::new(BlockingStore::new(
            RecordingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::any(),
            ),
            KeyPredicate::hint(&namespace_id),
            OperationSelector::Any,
        ));
        let builder = crate::LoonFs::builder_with_store(store.clone() as SharedStore)
            .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
            .config(crate::RuntimeConfig {
                min_publish_interval_ms: 0,
                ..crate::RuntimeConfig::default()
            })
            .monotonic_timer(Arc::new(ManualClock::new(0)));
        let writer = configure(builder).build().await.expect("build writer");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "first", "first",
            )))
            .await
            .expect("the session's first commit");
        // The claim's hint raise runs in the session's worker after the
        // commit returns.
        writer
            .drain()
            .await
            .expect("the claim's hint raise finishes");
        Self {
            _directory: directory,
            store,
            writer,
            namespace_id,
            namespace,
        }
    }

    /// Parks every later request for the hint, and clears the request log.
    fn park_hint_requests(&self) {
        self.store.arm();
        self.store.inner().reset();
    }

    fn manifest(&self, manifest_no: ManifestNo) -> String {
        loonfs_objectstore::keys::metadata_manifest_object(&self.namespace_id, &manifest_no)
    }

    /// Returns the logged manifest requests, and checks that the log has no
    /// request for the hint and no read of a WAL object.
    fn manifest_requests(&self) -> Vec<(&'static str, String)> {
        let operations = self.store.inner().take();
        let wal = wal_prefix(&self.namespace_id);
        let hint = loonfs_objectstore::keys::hint(&self.namespace_id);
        assert!(
            !operations.iter().any(|operation| operation.key() == hint
                || (operation.key().starts_with(&wal)
                    && !matches!(operation, RecordedOperation::Put { .. }))),
            "{operations:?}"
        );
        let manifests = metadata_manifest_prefix(&self.namespace_id);
        operations
            .iter()
            .filter(|operation| operation.key().starts_with(&manifests))
            .map(|operation| {
                let kind = match operation {
                    RecordedOperation::Head { .. } => "head",
                    RecordedOperation::Put { .. } => "put",
                    _ => "get",
                };
                (kind, operation.key().to_owned())
            })
            .collect()
    }

    async fn current_manifest_no(&self) -> ManifestNo {
        current_manifest_no(self.store.inner(), &self.namespace_id).await
    }

    async fn shut_down(self) {
        self.store.release();
        self.writer.shutdown().await.expect("shut down writer");
    }
}

/// Checks that an explicit fold in a runtime with the namespace's live
/// session folds the session's position, and puts the position's successor
/// without a request first and without reading the hint.
#[tokio::test]
async fn an_explicit_fold_from_a_live_session_puts_on_its_basis_and_reads_no_hint() {
    let fixture = Fixture::new(|builder| builder).await;
    fixture.writer.drain().await.expect("drain");
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    fixture.park_hint_requests();
    let folded = bounded(
        Duration::from_secs(10),
        fixture
            .writer
            .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
            .fold_wal(&fixture.namespace_id),
    )
    .await
    .expect("the fold reads no hint")
    .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    assert_eq!(folded.manifest_no, next);
    assert_eq!(
        fixture.manifest_requests(),
        vec![("put", fixture.manifest(next))]
    );
    fixture.shut_down().await;
}

/// Checks that the session's fold at the write stop puts the position's
/// successor without a request first and without reading the hint, and that
/// the commit waiting for the fold then commits.
#[tokio::test]
async fn a_fold_at_the_write_stop_puts_on_its_basis_and_reads_no_hint() {
    let fixture = Fixture::new(|builder| builder).await;
    let publisher = fixture.namespace.session().publisher.clone();
    let tail_objects = || async {
        publisher
            .engine
            .lock()
            .await
            .engine
            .as_ref()
            .and_then(NamespaceCommitEngine::wal_tail_objects)
            .unwrap_or(0)
    };
    // No fold starts while the test has every fold permit, so the session
    // commits until its tail reaches the write stop.
    let permits = hold_every_fold_permit(&fixture.writer).await;
    for fill in 0.. {
        if tail_objects().await >= MAX_UNFOLDED_WAL_OBJECTS {
            break;
        }
        let name = format!("fill-{fill}");
        fixture
            .namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(&name, &name)))
            .await
            .expect("fill the tail to the write stop");
    }
    let namespace = fixture.namespace.clone();
    let commit = tokio::spawn(async move {
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "at-the-stop",
                "at-the-stop",
            )))
            .await
    });
    bounded(Duration::from_secs(10), async {
        while queued_candidates(&publisher_state(&publisher)) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the commit waits at the write stop");
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    fixture.park_hint_requests();
    drop(permits);
    bounded(Duration::from_secs(10), commit)
        .await
        .expect("the fold reads no hint, and the commit settles")
        .expect("join the commit")
        .expect("the commit is committed after the fold");
    let requests = fixture.manifest_requests();
    assert_eq!(
        requests[..1],
        [("put", fixture.manifest(next))],
        "{requests:?}"
    );
    fixture.shut_down().await;
}

/// Takes every compaction permit, and then writes files until a write's fold
/// makes the session's compaction wait for a permit, and returns the permits.
async fn write_until_compaction_waits<'a>(
    fixture: &'a Fixture,
    files: &mut usize,
) -> Vec<BudgetPermit<'a>> {
    let budget = fixture.writer.execution_budget();
    let publisher = fixture.namespace.session().publisher.clone();
    let mut permits = Vec::new();
    for _ in 0..crate::DEFAULT_MAX_CONCURRENT_COMPACTIONS.get() {
        permits.push(budget.compaction_permit().await);
    }
    for _ in 0..32 {
        *files += 1;
        fixture
            .namespace
            .put_file(
                &format!("/file-{files}"),
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put a file");
        fixture
            .namespace
            .wait_for_fold()
            .await
            .expect("the write's fold settles");
        // The fold starts the session's compaction before its task exits,
        // and the compaction either finds nothing due and exits, or waits
        // for a permit.
        let waits = bounded(Duration::from_secs(10), async {
            tokio::select! {
                finished = publisher.wait_for_compaction() => {
                    finished.expect("the compaction finds nothing due");
                    false
                }
                () = async {
                    while budget.stats().compactions_waiting == 0 {
                        tokio::task::yield_now().await;
                    }
                } => true,
            }
        })
        .await
        .expect("the session's compaction settles or waits");
        if waits {
            return permits;
        }
    }
    panic!("each write's fold adds a run, so compaction should become due");
}

/// Checks that a session's compaction unit, after the runtime's compactor
/// claim, plans from the session's position, and puts the position's
/// successor without a request first and without reading the hint.
#[tokio::test]
async fn a_session_compaction_unit_puts_on_its_basis_and_reads_no_hint() {
    // The fold threshold is one inline byte, so each write is folded.
    let fixture = Fixture::new(|builder| {
        builder.config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            inline_content: InlineContentPolicy {
                inline_content_fold_at_bytes: 1,
                ..InlineContentPolicy::default()
            },
            ..crate::RuntimeConfig::default()
        })
    })
    .await;
    let mut files = 0;
    // The first unit claims the compactor epoch, which loads the current
    // manifest through the hint.
    drop(write_until_compaction_waits(&fixture, &mut files).await);
    fixture
        .writer
        .drain()
        .await
        .expect("the first unit settles");
    let permits = write_until_compaction_waits(&fixture, &mut files).await;
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    fixture.park_hint_requests();
    drop(permits);
    bounded(Duration::from_secs(10), fixture.writer.drain())
        .await
        .expect("the compaction reads no hint")
        .expect("drain");
    let requests = fixture.manifest_requests();
    assert_eq!(
        requests[..1],
        [("put", fixture.manifest(next))],
        "{requests:?}"
    );
    fixture.shut_down().await;
}
