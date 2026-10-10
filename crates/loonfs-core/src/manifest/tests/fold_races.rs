//! A fold that loses its manifest number to another publication.

use super::fold::{fold_wal_tail, FoldedWalTail};
use super::*;
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::manifest::claim_compactor;
use crate::namespace::control::CurrentManifest;
use crate::publish::{InlineContent, WalFoldInput};
use loonfs_objectstore::keys::wal_prefix;
use loonfs_objectstore::layout::{parse_object_key, wal_no_of, DurableObjectFamily};
use loonfs_test_support::stores::{RecordedOperation, StoreCounts};
use loonfs_types::{CompactorEpoch, ContentId, DeleteDirectoryBehavior, FoldWalOutcome};
use std::path::Path;

/// A manifest another process publishes while a fold waits at its
/// manifest put.
#[derive(Clone)]
enum Rival {
    CompactorClaim,
    Merge,
    RetentionAdvance,
    Fold(Box<WalFoldInput>),
}

impl Rival {
    async fn publish(&self, store: &LocalFsStore, namespace_id: &NamespaceId) {
        let merge_memory = tokio::sync::Semaphore::new(32 * 1024 * 1024);
        match self {
            Self::CompactorClaim => {
                claim_compactor(store, namespace_id)
                    .await
                    .expect("claim the compactor");
            }
            Self::Merge => {
                let mut merges = 0;
                loop {
                    match compaction_step(
                        store,
                        namespace_id,
                        CompactorEpoch(0),
                        MetadataLsmPolicy::default(),
                        MetadataCompactionPolicy::CompactImmediately,
                        Arc::default(),
                    )
                    .await
                    .expect("merge")
                    {
                        CompactionStepOutcome::UnitPublished { .. } => merges += 1,
                        CompactionStepOutcome::NotNeeded { .. } => break,
                        other => panic!("expected a merge, got {other:?}"),
                    }
                }
                assert!(merges > 0, "the merge rival should publish");
            }
            Self::RetentionAdvance => {
                advance_retention_floor(store, None, namespace_id, RetentionTarget::Head)
                    .await
                    .expect("advance the floor");
            }
            Self::Fold(input) => {
                let folded = fold_wal_tail(
                    store,
                    None,
                    namespace_id,
                    Some(WalFoldInput::clone(input)),
                    &deadline(),
                    &merge_memory,
                )
                .await
                .expect("rival fold");
                assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
            }
        }
    }
}

struct Writer {
    directory: tempfile::TempDir,
    store: Arc<RecordingStore<LocalFsStore>>,
    engine: NamespaceCommitEngine,
    context: MutationContext,
    namespace_id: NamespaceId,
}

impl Writer {
    async fn open() -> Self {
        let directory = tempdir().expect("directory");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let namespace_id = NamespaceId::parse("fold-race").expect("namespace");
        let context = test_context();
        create(&store, &namespace_id, &context)
            .await
            .expect("create");
        Self {
            engine: NamespaceCommitEngine::with_unshared_head_state(namespace_id.clone()),
            directory,
            store,
            context,
            namespace_id,
        }
    }

    fn other_process(&self) -> LocalFsStore {
        LocalFsStore::new(self.directory.path()).expect("store")
    }

    /// Copies the store so the same history can be driven another way.
    fn twin(&self) -> (tempfile::TempDir, LocalFsStore) {
        let twin = tempdir().expect("twin directory");
        copy_directory(self.directory.path(), twin.path());
        let store = LocalFsStore::new(twin.path()).expect("twin store");
        (twin, store)
    }

    /// Commits `operations`, then one inline file at each of `files`.
    async fn commit(
        &mut self,
        commit_id: &str,
        mut operations: Vec<FilesystemOperation>,
        files: &[&str],
    ) {
        let mut values = Vec::new();
        for path in files {
            let value = InlineContent::new(
                self.namespace_id.clone(),
                ContentId::generate(),
                Bytes::copy_from_slice(path.as_bytes()),
                loonfs_types::ChecksumAlgorithm::Crc64nvme,
            );
            operations.push(FilesystemOperation::PutFile {
                path: AbsolutePath::parse(*path).expect("path"),
                content_ref: Some(value.content_ref().clone()),
                inline_content: None,
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            });
            values.push(value);
        }
        let candidate = CommitCandidate::with_inline_content(
            CommitRequest {
                commit_id: CommitId::parse(commit_id).expect("commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                preconditions: Vec::new(),
                operations,
            },
            Vec::new(),
            values,
        );
        self.engine
            .publish_batch(&*self.store, [candidate], &self.context, &deadline())
            .await
            .results
            .pop()
            .expect("one result")
            .expect("commit");
    }

    async fn fold(&mut self) -> FoldedWalTail {
        self.fold_against(&[]).await
    }

    /// Folds the writer's tail. The fold's first manifest put waits while
    /// the first rival publishes from another process, its next put waits
    /// for the next rival, and so on. The request log is cleared after each
    /// rival.
    async fn fold_against(&mut self, rivals: &[Rival]) -> FoldedWalTail {
        let merge_memory = tokio::sync::Semaphore::new(32 * 1024 * 1024);
        let input = self.engine.begin_wal_fold().expect("tail");
        let basis = input.basis.manifest_no();
        let other = self.other_process();
        let mut store: Arc<dyn ObjectStore> = self.store.clone();
        let mut gates = Vec::new();
        for number in 1..=rivals.len() as u64 {
            let gate = Arc::new(BlockingStore::new(
                store,
                KeyPredicate::exact(metadata_manifest_object(
                    &self.namespace_id,
                    &ManifestNo(basis.0 + number),
                )),
                OperationClass::PutCreateIfAbsent,
            ));
            gate.block_next();
            gates.push(Arc::clone(&gate));
            store = gate;
        }
        let deadline = deadline();
        let (folded, ()) = futures::join!(
            fold_wal_tail(
                &*store,
                None,
                &self.namespace_id,
                Some(input),
                &deadline,
                &merge_memory,
            ),
            async {
                for (gate, rival) in gates.iter().zip(rivals) {
                    gate.wait_until_blocked().await;
                    rival.publish(&other, &self.namespace_id).await;
                    self.store.reset();
                    gate.release();
                }
            }
        );
        let folded = folded.expect("fold");
        self.engine.record_wal_fold(Some(&folded));
        folded
    }

    fn wal_reads(&self, operations: &[RecordedOperation]) -> BTreeSet<u64> {
        operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation,
                    RecordedOperation::Head { .. }
                        | RecordedOperation::Get { .. }
                        | RecordedOperation::GetWithMetadata { .. }
                ) && operation.key().starts_with(&wal_prefix(&self.namespace_id))
            })
            .map(|operation| wal_no_of(operation.key()).expect("WAL number").0)
            .collect()
    }
}

fn deadline() -> Deadline {
    Deadline::start(Arc::new(StdMonotonicTimer::default()))
}

fn copy_directory(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).expect("read directory") {
        let entry = entry.expect("directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            std::fs::create_dir_all(&target).expect("create directory");
            copy_directory(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

fn is_content(operation: &RecordedOperation) -> bool {
    parse_object_key(operation.key())
        .is_some_and(|key| key.family() == DurableObjectFamily::ContentBlob)
}

/// The manifest without segment identities, which differ between any two
/// builds of the same rows.
fn layout(
    manifest: &CurrentManifest,
) -> (
    NamespaceManifestPayload,
    Vec<(RunNo, ChangeSeq, RunTier, usize)>,
) {
    let payload = manifest.envelope.payload();
    (
        NamespaceManifestPayload {
            runs: Vec::new(),
            ..payload.clone()
        },
        payload
            .runs
            .iter()
            .map(|run| (run.run_no, run.run_seq, run.tier, run.segments.len()))
            .collect(),
    )
}

/// Request counts without byte totals, which differ with the digits of
/// manifest numbers and epochs.
fn request_counts(store: &RecordingStore<LocalFsStore>) -> StoreCounts {
    StoreCounts {
        read_bytes: 0,
        written_bytes: 0,
        ..store.counts()
    }
}

#[tokio::test]
async fn a_fold_that_loses_to_a_compaction_reads_no_wal_object() {
    let merge_memory = tokio::sync::Semaphore::new(32 * 1024 * 1024);
    for rival in [Rival::Merge, Rival::CompactorClaim, Rival::RetentionAdvance] {
        let mut writer = Writer::open().await;
        writer
            .commit(
                "first",
                vec![FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/dir").expect("path"),
                    parents: false,
                }],
                &["/dir/a"],
            )
            .await;
        writer.fold().await;
        writer.commit("second", Vec::new(), &["/b"]).await;
        writer.fold().await;
        // The deletion's root inode is in the runs a merge rewrites.
        writer
            .commit(
                "third",
                vec![FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/dir").expect("path"),
                    behavior: DeleteDirectoryBehavior::Recursive,
                    expected_inode_id: None,
                }],
                &["/c"],
            )
            .await;
        let (_twin_directory, twin) = writer.twin();

        let held = writer.fold_against(std::slice::from_ref(&rival)).await;
        let after_park = writer.store.take();
        assert!(writer.wal_reads(&after_park).is_empty(), "{after_park:?}");
        assert!(!after_park.iter().any(is_content), "{after_park:?}");

        rival.publish(&twin, &writer.namespace_id).await;
        let cold = fold_wal_tail(
            &twin,
            None,
            &writer.namespace_id,
            None,
            &deadline(),
            &merge_memory,
        )
        .await
        .expect("cold fold");
        assert_eq!(held.response.outcome, FoldWalOutcome::Published);
        assert_eq!(held.response, cold.response);
        let held = load_current_projection(&*writer.store, &writer.namespace_id)
            .await
            .expect("held fold result");
        let cold = load_current_projection(&twin, &writer.namespace_id)
            .await
            .expect("cold fold result");
        assert_eq!(held.head, cold.head);
        assert_eq!(held.metadata_state, cold.metadata_state);
        assert_eq!(layout(&held.manifest), layout(&cold.manifest));
    }
}

#[tokio::test]
async fn a_fold_that_loses_to_a_fold_still_takes_the_cold_path() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let older = writer.engine.wal_fold_input().expect("tail");
    writer.commit("second", Vec::new(), &["/b"]).await;
    let head = writer.engine.wal_fold_input().expect("tail").head;

    let folded = writer
        .fold_against(&[Rival::Fold(Box::new(older.clone()))])
        .await;
    let after_park = writer.store.take();
    assert!(
        writer.wal_reads(&after_park).contains(&head.wal_no.0),
        "{after_park:?}"
    );
    assert!(!writer.wal_reads(&after_park).contains(&older.head.wal_no.0));
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    assert_eq!(
        folded.response.manifest_no,
        ManifestNo(older.basis.manifest_no().0 + 2)
    );
    assert_eq!(folded.response.manifest_head_seq, head.seq);
}

#[tokio::test]
async fn a_fold_survives_repeated_compaction_publications() {
    // The first attempt and every retry each lose to one claim.
    for (rivals, cold) in [
        (CONTENTION_RETRY_LIMIT, false),
        (CONTENTION_RETRY_LIMIT + 1, true),
    ] {
        let mut writer = Writer::open().await;
        writer.commit("first", Vec::new(), &["/a"]).await;
        let basis = writer
            .engine
            .wal_fold_input()
            .expect("tail")
            .basis
            .manifest_no();
        let folded = writer
            .fold_against(&vec![Rival::CompactorClaim; rivals])
            .await;
        let after_park = writer.store.take();
        assert_eq!(!writer.wal_reads(&after_park).is_empty(), cold);
        assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
        assert_eq!(
            folded.response.manifest_no,
            ManifestNo(basis.0 + rivals as u64 + 1)
        );
    }
}

#[tokio::test]
async fn a_retried_fold_leaves_the_session_where_an_unraced_fold_does() {
    let mut costs = Vec::new();
    for rivals in [Vec::new(), vec![Rival::CompactorClaim]] {
        let mut writer = Writer::open().await;
        writer.commit("first", Vec::new(), &["/a"]).await;
        writer.store.reset();
        let folded = writer.fold_against(&rivals).await;
        let operations = writer.store.take();
        assert!(writer.wal_reads(&operations).is_empty(), "{operations:?}");
        assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
        assert_eq!(
            writer.engine.wal_fold_input().expect("tail").basis,
            folded.basis
        );
        writer.commit("second", Vec::new(), &["/b"]).await;
        let publish = request_counts(&writer.store);
        let hint = hint(&writer.namespace_id);
        let operations = writer.store.take();
        assert!(!operations.iter().any(|operation| operation.key() == hint));
        writer.fold().await;
        let fold = request_counts(&writer.store);
        let operations = writer.store.take();
        assert!(writer.wal_reads(&operations).is_empty(), "{operations:?}");
        costs.push((publish, fold));
    }
    assert_eq!(costs[0], costs[1]);
    // The receipt check reads the basis manifest and one segment; the commit
    // is one WAL put.
    assert_eq!(
        costs[1].0,
        StoreCounts {
            gets: 2,
            puts: 1,
            create_if_absent_puts: 1,
            ..StoreCounts::default()
        }
    );
}

#[tokio::test]
async fn a_checkpoint_that_loses_its_fold_to_a_compaction_reads_no_wal_object() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let basis = writer
        .engine
        .wal_fold_input()
        .expect("tail")
        .basis
        .manifest_no();
    let other = writer.other_process();
    let gate = BlockingStore::new(
        writer.store.clone(),
        KeyPredicate::manifest(&writer.namespace_id),
        OperationClass::PutCreateIfAbsent,
    );
    gate.block_next();
    let (checkpoint, ()) = futures::join!(
        create_checkpoint(&gate, &writer.namespace_id, &writer.context),
        async {
            gate.wait_until_blocked().await;
            Rival::CompactorClaim
                .publish(&other, &writer.namespace_id)
                .await;
            writer.store.reset();
            gate.release();
        }
    );
    let checkpoint = checkpoint.expect("checkpoint");
    let after_park = writer.store.take();
    assert!(writer.wal_reads(&after_park).is_empty(), "{after_park:?}");
    let pin = load_pin(
        &*writer.store,
        &writer.namespace_id,
        &checkpoint.checkpoint_id,
    )
    .await
    .expect("pin record")
    .expect("pin");
    assert_eq!(pin.state.manifest().manifest_no, ManifestNo(basis.0 + 2));
}
