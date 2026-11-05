//! A run of steps that a host drives with `GrepSteps::step`: the
//! reorganizations that it runs while builds are due, and the index reads
//! of its build steps while a writer keeps committing.

use crate::common::{control, GrepHost};
use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::{CommitId, DestinationBehavior, LoonFs, Namespace, SharedObjectStore, Writable};
use loonfs_grep::entry::{grep_extension_name, GrepIndexStatus};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepEnableOutcome, GrepProgress, GrepReorganizeOutcome,
};
use loonfs_objectstore::keys::extension_segment_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, nonzero_usize};
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_types::{AbsolutePath, NamespaceId};
use std::assert_matches;
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::TempDir;

/// The level of a delta run in grep's entry.
const DELTA_LEVEL: u32 = 0;

/// A namespace with a grep host whose store records the reads of the
/// namespace's grep segments.
struct StepsFixture {
    _directory: TempDir,
    segment_reads: Arc<RecordingStore<LocalFsStore>>,
    store: SharedObjectStore,
    host: GrepHost,
    namespace_id: NamespaceId,
    namespace: Namespace<Writable>,
}

impl StepsFixture {
    async fn new(name: &str) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let namespace_id = namespace_id(name);
        let segment_reads = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(extension_segment_prefix(
                &namespace_id,
                &grep_extension_name(),
            )),
        ));
        let store: SharedObjectStore = segment_reads.clone();
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id(loonfs_test_support::ids::writer_id(name))
            .config(loonfs::RuntimeConfig {
                min_publish_interval_ms: 0,
                ..loonfs::RuntimeConfig::default()
            })
            .build()
            .await
            .expect("writer");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create the namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open the namespace");
        let host = GrepHost::new(&store, &format!("{name}-grep")).await;
        Self {
            _directory: directory,
            segment_reads,
            store,
            host,
            namespace_id,
            namespace,
        }
    }

    /// Writes `count` files under `/{directory}/` in one commit, each with
    /// about 300 bytes of words of its own, so that each build step's run
    /// has a filter block that isn't inline in its descriptor.
    async fn commit_files(&self, directory: usize, count: usize) {
        let mut operations = Vec::with_capacity(count);
        let mut prepared = Vec::with_capacity(count);
        for index in 0..count {
            let path = format!("/dir-{directory:03}/file-{index:03}.txt");
            let content = self
                .namespace
                .prepare_content(words(directory * count + index).as_bytes())
                .await
                .expect("prepare the content");
            operations.push(FilesystemOperation::PutFile {
                path: AbsolutePath::parse(path).expect("path"),
                content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            });
            prepared.push(content);
        }
        self.namespace
            .commit_prepared(
                CommitIntent {
                    commit_id: CommitId::generate(),
                    actor_id: loonfs_test_support::test_actor(),
                    subject: None,
                    message: None,
                    operations,
                    preconditions: Vec::new(),
                },
                prepared,
            )
            .await
            .expect("commit the files");
    }

    /// Copies the `count` files under `/dir-000/` to `/copy-{copy}/` in one
    /// commit. A copy keeps its source's content value.
    async fn copy_files(&self, copy: usize, count: usize) {
        let mut operations = vec![FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(format!("/copy-{copy}")).expect("path"),
            parents: false,
        }];
        operations.extend((0..count).map(|index| FilesystemOperation::CopyPath {
            source_path:
                AbsolutePath::parse(format!("/dir-000/file-{index:03}.txt")).expect("path"),
            destination_path:
                AbsolutePath::parse(format!("/copy-{copy}/file-{index:03}.txt")).expect("path"),
            precondition: Default::default(),
        }));
        self.namespace
            .commit(CommitIntent {
                commit_id: CommitId::generate(),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations,
                preconditions: Vec::new(),
            })
            .await
            .expect("commit the copies");
    }

    /// Returns the published entry's status and its number of delta runs.
    async fn published(&self) -> (GrepIndexStatus, usize) {
        let entry = control::grep_entry(&self.store, &self.namespace_id)
            .await
            .expect("grep's entry");
        let delta_runs = entry
            .runs()
            .iter()
            .filter(|run| run.level == DELTA_LEVEL)
            .count();
        (entry.status().clone(), delta_runs)
    }
}

/// Returns about 300 bytes of lowercase words that depend on `seed`.
fn words(seed: usize) -> String {
    // Setting the low bit keeps the state nonzero, because xorshift never
    // leaves a zero state.
    let mut state = u64::try_from(seed)
        .expect("a seed fits u64")
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        | 1;
    let mut text = String::new();
    while text.len() < 300 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for letter in 0..3 + state % 6 {
            text.push(char::from(
                b'a' + u8::try_from((state >> (letter * 5)) % 26).expect("a letter"),
            ));
        }
        text.push(' ');
    }
    text
}

/// Checks that a run of steps runs a reorganize step as soon as the delta
/// level is full, while the backfill still has files to index, so the entry
/// never lists more delta runs than the policy's limit.
#[tokio::test]
async fn a_run_of_steps_reorganizes_a_full_delta_level_while_builds_are_due() {
    let fixture = StepsFixture::new("full-delta-level").await;
    fixture.commit_files(0, 8).await;
    assert_matches!(
        fixture.host.worker.enable(&fixture.namespace_id).await,
        Ok(GrepEnableOutcome::Enabled { .. })
    );
    let policy = GramIndexBuildPolicy {
        max_files_per_step: nonzero_usize(1),
        max_delta_runs: nonzero_usize(2),
        ..GramIndexBuildPolicy::default()
    };
    let mut steps = fixture.host.worker.steps(&fixture.namespace_id, 0);
    let mut reorganized_while_backfilling = 0;
    for _ in 0..64 {
        let (before, _) = fixture.published().await;
        match steps.step(policy).await.expect("step") {
            GrepProgress::Built(GrepBuildOutcome::Indexed { .. }) => {}
            GrepProgress::Built(GrepBuildOutcome::UpToDate { .. }) => break,
            GrepProgress::Reorganized(GrepReorganizeOutcome::UnitWritten { .. }) => {
                if matches!(before, GrepIndexStatus::Backfilling { .. }) {
                    reorganized_while_backfilling += 1;
                }
            }
            other => panic!("unexpected step outcome {other:?}"),
        }
        let (_, delta_runs) = fixture.published().await;
        assert!(delta_runs <= 2, "{delta_runs} delta runs");
    }
    assert!(
        reorganized_while_backfilling >= 3,
        "{reorganized_while_backfilling} reorganizations of eight runs at a limit of two"
    );
    assert_matches!(
        fixture.published().await,
        (GrepIndexStatus::Active { .. }, 0)
    );
}

/// Checks that a build step's reads of the index's segments stay bounded
/// while a writer keeps committing. The run of steps reorganizes the delta
/// level when it's full, and a build step reads each of the entry's
/// segments about once, so the reads don't grow with the runs that the
/// build steps wrote.
#[tokio::test]
async fn build_step_index_reads_stay_bounded_while_a_writer_keeps_committing() {
    const ROUNDS: usize = 32;
    const FILES: usize = 32;
    let fixture = StepsFixture::new("bounded-step-reads").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable grep");
    let policy = GramIndexBuildPolicy {
        max_files_per_step: nonzero_usize(FILES),
        max_delta_runs: nonzero_usize(4),
        max_mid_runs: nonzero_usize(4),
        ..GramIndexBuildPolicy::default()
    };
    let mut steps = fixture.host.worker.steps(&fixture.namespace_id, 0);
    let mut builds = 0;
    let mut most_reads = 0;
    for round in 0..ROUNDS {
        fixture.commit_files(round, FILES).await;
        for _ in 0..16 {
            fixture.segment_reads.reset();
            match steps.step(policy).await.expect("step") {
                GrepProgress::Built(GrepBuildOutcome::Indexed { .. }) => {
                    builds += 1;
                    most_reads = most_reads.max(fixture.segment_reads.take_gets().len());
                }
                GrepProgress::Built(GrepBuildOutcome::UpToDate { .. }) => break,
                GrepProgress::Reorganized(GrepReorganizeOutcome::UnitWritten { .. }) => {}
                other => panic!("unexpected step outcome {other:?}"),
            }
        }
    }
    assert_eq!(builds, ROUNDS, "one build step for each commit");
    // A build step here reads at most 10 segment objects. A loop of build
    // steps without reorganize steps would read one more each round, and 31
    // in the last round.
    assert!(
        most_reads <= 16,
        "a build step read {most_reads} segment objects"
    );
}

/// Checks that a worker's build steps read index segments through one cache
/// of the worker's. Two build steps that look up the same content values in
/// the same runs read each `texts` block of those runs from the store once.
#[tokio::test]
async fn two_build_steps_over_the_same_runs_read_each_texts_block_once() {
    let fixture = StepsFixture::new("worker-block-cache").await;
    fixture.commit_files(0, 64).await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable grep");
    let backfilled: BTreeSet<String> = control::grep_entry(&fixture.store, &fixture.namespace_id)
        .await
        .expect("grep's entry")
        .segments()
        .map(|segment| {
            loonfs_objectstore::keys::extension_segment(
                &fixture.namespace_id,
                &grep_extension_name(),
                &segment.segment_id,
            )
        })
        .collect();
    let mut steps = fixture.host.worker.steps(&fixture.namespace_id, 0);
    let mut backfill_gets = Vec::new();
    // Each commit copies the backfilled files, so each build step looks up
    // every backfilled value in the backfill's runs.
    for copy in 1..=2 {
        fixture.copy_files(copy, 64).await;
        fixture.segment_reads.reset();
        assert_matches!(
            steps.build_step(GramIndexBuildPolicy::default()).await,
            Ok(GrepBuildOutcome::Indexed {
                indexed_revisions: 0,
                skipped_revisions: 64,
                ..
            })
        );
        let gets = fixture.segment_reads.take_gets();
        backfill_gets.push(
            gets.into_iter()
                .filter(|(key, _)| backfilled.contains(key))
                .collect::<Vec<_>>(),
        );
    }
    assert!(
        !backfill_gets[0].is_empty(),
        "the first build step reads the backfill's `texts` blocks"
    );
    assert_eq!(
        backfill_gets[1],
        Vec::new(),
        "the second build step reads the same blocks from the worker's cache"
    );
}
