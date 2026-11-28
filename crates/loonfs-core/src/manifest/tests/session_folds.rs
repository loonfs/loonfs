//! A writer session's fold: the requests that it sends, and where it
//! leaves the session's position.

use super::fold::{FoldWalResponse, FoldedWalTail};
use super::*;
use crate::publish::InlineContent;
use crate::FoldWalOutcome;
use loonfs_types::{ContentId, WalNo, WriterEpoch};

pub(super) struct Writer {
    _directory: tempfile::TempDir,
    pub(super) store: Arc<RecordingStore<LocalFsStore>>,
    pub(super) engine: NamespaceCommitEngine<Arc<RecordingStore<LocalFsStore>>>,
    pub(super) context: WriterAt,
    pub(super) namespace_id: NamespaceId,
}

impl Writer {
    async fn open() -> Self {
        Self::open_namespace("fold-race").await
    }

    /// Creates the namespace `namespace` in a new store, and returns a
    /// writer session of it that hasn't claimed yet.
    pub(super) async fn open_namespace(namespace: &str) -> Self {
        let directory = tempdir().expect("directory");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let namespace_id = NamespaceId::parse(namespace).expect("namespace");
        let context = test_context();
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("create");
        Self {
            engine: NamespaceCommitEngine::new(
                crate::test_support::namespace_store(Arc::clone(&store), &namespace_id),
                context.writer_id.clone(),
            ),
            _directory: directory,
            store,
            context,
            namespace_id,
        }
    }

    /// Commits `operations`, then one inline file at each of `files`.
    pub(super) async fn commit(
        &mut self,
        commit_id: &str,
        mut operations: Vec<FilesystemOperation>,
        files: &[&str],
    ) {
        let mut values = Vec::new();
        for path in files {
            let value = InlineContent::new(
                ContentId::generate(),
                Bytes::copy_from_slice(path.as_bytes()),
                loonfs_types::ChecksumAlgorithm::Crc64nvme,
            );
            operations.push(FilesystemOperation::PutFile {
                path: AbsolutePath::parse(*path).expect("path"),
                content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            });
            values.push(value);
        }
        let candidate = CommitCandidate::with_inline_content(
            CommitIntent {
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
            .publish_batch_alone([candidate], &self.context.clock())
            .await
            .results
            .pop()
            .expect("one result")
            .expect("commit");
    }
}

pub(super) fn deadline() -> OperationClock {
    OperationClock::start(0, Arc::new(StdMonotonicTimer::default()))
}

/// Checks that a fold from a session's position whose manifest is in the
/// segment cache publishes on the position's manifest with its put and no
/// request for the manifest, the manifest boundary or the manifest's
/// successor before the put, and reads the boundary once after the put.
#[tokio::test]
async fn a_fold_from_a_fresh_position_puts_on_its_basis_with_no_request_first() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let input = writer.engine.writer_position().expect("tail");
    let next = input
        .read_basis()
        .basis
        .manifest_no()
        .successor()
        .expect("next manifest number");
    let namespace = crate::test_support::namespace_store_with_segment_cache(
        &*writer.store,
        &writer.namespace_id,
    );
    crate::manifest::load_referenced_manifest(
        &*writer.store,
        Some(&namespace.segment_cache),
        input.read_basis().basis.manifest(),
    )
    .await
    .expect("the session's manifest");
    writer.store.reset();
    let folded = namespace
        .fold_wal_tail_in_new_lane(&input, &deadline())
        .await
        .expect("fold");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    assert_eq!(
        control_requests(&writer.namespace_id, &writer.store.take()),
        vec![
            ("put", metadata_manifest_object(&writer.namespace_id, &next)),
            ("get", boundary(&writer.namespace_id)),
        ]
    );
}

/// Checks the requests of a session's fold in a namespace with a segment
/// cache, as a runtime's namespace has one. The session that made the commit
/// folds from the position it has: the new run's segment puts, the manifest
/// put, and one read of the manifest boundary. A new session of the same
/// writer, with nothing cached, first loads its position as a commit does
/// (the discovery of the head and the tail's WAL objects), and then takes
/// the position's manifest from the cache that the load filled.
#[tokio::test]
async fn a_session_fold_sends_these_requests_in_order() {
    for new_session in [false, true] {
        let mut writer = Writer::open().await;
        let (store, namespace_id) = (Arc::clone(&writer.store), writer.namespace_id.clone());
        let namespace = || {
            crate::test_support::namespace_store_with_segment_cache(
                Arc::clone(&store),
                &namespace_id,
            )
        };
        writer.engine = NamespaceCommitEngine::new(namespace(), writer.context.writer_id.clone());
        writer.commit("first", Vec::new(), &["/a"]).await;
        let current = crate::test_support::namespace_store(&*writer.store, &writer.namespace_id)
            .load_current_manifest()
            .await
            .expect("the current manifest");
        let mut session = NamespaceCommitEngine::with_acquired_session(
            namespace(),
            writer.context.writer_id.clone(),
            crate::namespace::writer_epoch::AcquiredWriter {
                writer_id: writer.context.writer_id.clone(),
                writer_epoch: current.payload().writer_epoch,
            },
            crate::namespace::Held::new(
                current,
                crate::test_support::observed_now(),
                Default::default(),
            ),
        );
        writer.store.reset();
        let folded = if new_session {
            session.fold().await
        } else {
            writer.engine.fold().await
        }
        .expect("fold");
        assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
        let mut lines = loonfs_test_support::stores::request_lines(&writer.store.take());
        let load = if new_session {
            // The WAL reads of the load run concurrently, so they're
            // compared in key order.
            lines[4..7].sort();
            vec![
                "get fold-race Boundary".to_owned(),
                "list namespaces/fold-race/manifests/".to_owned(),
                "get fold-race MetadataManifest 00000000000000000003".to_owned(),
                "get fold-race Boundary".to_owned(),
                "get fold-race WalObject 00000000000000000001".to_owned(),
                "get fold-race WalObject 00000000000000000002".to_owned(),
                "get fold-race WalObject 00000000000000000003".to_owned(),
                "head fold-race MetadataManifest 00000000000000000004".to_owned(),
                "get fold-race Boundary".to_owned(),
            ]
        } else {
            Vec::new()
        };
        // The fold assembles the tail's inline file into a content object,
        // writes the new run's segments, and puts manifest 4.
        let fold = [
            vec!["assemble pool ContentBlob".to_owned()],
            vec!["put fold-race MetadataSegment".to_owned(); 7],
            vec![
                "put fold-race MetadataManifest 00000000000000000004".to_owned(),
                "get fold-race Boundary".to_owned(),
            ],
        ]
        .concat();
        assert_eq!(
            lines,
            [load, fold].concat(),
            "a fold in a new session: {new_session}"
        );
    }
}

/// Checks that a session's position moves to the manifest that covers its
/// fold's tail under the session's writer epoch, whether the fold published
/// that manifest or found it current, that a manifest of another epoch, or
/// one that folds a WAL number past the session's head, drops the position,
/// and that a failed fold leaves the position where it was.
#[tokio::test]
async fn a_fold_moves_the_position_only_to_a_fold_of_its_own_head_and_epoch() {
    enum Position {
        Moves,
        Drops,
        Stays,
    }
    for (epoch_after, wal_after, outcome, expected) in [
        (0, 0, Some(FoldWalOutcome::Published), Position::Moves),
        (1, 0, Some(FoldWalOutcome::Published), Position::Drops),
        (0, 1, Some(FoldWalOutcome::Published), Position::Drops),
        (0, 0, Some(FoldWalOutcome::AlreadyCurrent), Position::Moves),
        (0, 1, Some(FoldWalOutcome::AlreadyCurrent), Position::Drops),
        (0, 0, None, Position::Stays),
    ] {
        let mut writer = Writer::open().await;
        writer.commit("first", Vec::new(), &["/a"]).await;
        let input = writer.engine.writer_position().expect("tail");
        let before = input.read_basis().basis.clone();
        let folded = crate::test_support::namespace_store(&*writer.store, &writer.namespace_id)
            .fold_wal_tail_in_new_lane(&input, &deadline())
            .await
            .expect("fold");
        let current = if (epoch_after, wal_after) == (0, 0) {
            folded.current.clone()
        } else {
            let payload = folded.current.manifest().payload();
            crate::test_support::held_manifest(
                loonfs_types::format::manifest::NamespaceManifestPayload {
                    writer_epoch: WriterEpoch(payload.writer_epoch.0 + epoch_after),
                    folded_wal_no: WalNo(payload.folded_wal_no.0 + wal_after),
                    ..payload.clone()
                },
                folded.current.checked().clone(),
            )
        };
        let recorded = outcome.map(|outcome| FoldedWalTail {
            response: FoldWalResponse {
                outcome,
                ..folded.response.clone()
            },
            current,
        });
        // As the publisher does, a fold that ended reports the manifest that
        // covers its tail, and a fold that failed reports nothing.
        if let Some(recorded) = recorded {
            writer.engine.move_position_onto(recorded.current);
        }
        let position = writer.engine.writer_position();
        let folded_manifest = folded.current.manifest().manifest();
        let expected = match expected {
            Position::Moves => Some(&folded_manifest),
            Position::Drops => None,
            Position::Stays => Some(before.manifest()),
        };
        assert_eq!(
            position
                .as_ref()
                .map(|position| position.read_basis().basis.manifest()),
            expected,
            "epoch +{epoch_after}, WAL number +{wal_after}, {outcome:?}"
        );
    }
}
