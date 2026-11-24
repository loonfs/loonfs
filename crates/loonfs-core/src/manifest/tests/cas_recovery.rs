//! Numbered publication and retention contracts.

use super::*;
use std::assert_matches;

/// Checks that a manifest put that returns after its publication budget,
/// at a number above the manifest boundary, is published: the boundary read
/// after the put is the check of the put, and no budget check follows it.
#[tokio::test]
async fn a_manifest_put_returning_after_its_budget_is_published() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("manifest-return-budget").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let mut payload = current.payload().clone();
    payload.manifest_no = payload.manifest_no.successor().expect("next manifest");
    let object_key = metadata_manifest_object(&namespace_id, &payload.manifest_no);
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let deadline = OperationClock::start(0, timer.clone());
    store.reset();
    let store = loonfs_test_support::stores::MetadataMapStore::new(
        store,
        KeyPredicate::exact(&object_key),
        move |metadata| {
            timer.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
            metadata
        },
    );
    let outcome = publish_once_within(&store, payload, &deadline)
        .await
        .expect("a late put above the boundary");
    assert_matches!(outcome, ManifestPublicationOutcome::Published(_));
    let operations = store.inner().snapshot();
    assert_eq!(
        operations
            .iter()
            .rev()
            .map(|operation| operation.key().to_owned())
            .take(2)
            .collect::<Vec<_>>(),
        [boundary(&namespace_id), object_key]
    );
}

#[tokio::test]
async fn a_manifest_put_that_writes_nothing_and_loses_its_answer_has_an_unknown_outcome() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let mut payload = current.payload().clone();
    payload.manifest_no = payload.manifest_no.successor().expect("next manifest");
    let object_key = metadata_manifest_object(&namespace_id, &payload.manifest_no);
    let store = FailStore::new(
        store,
        KeyPredicate::exact(&object_key),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost request".to_owned()),
    );
    store.fail_next(1);
    let error = publish_once(&store, payload)
        .await
        .expect_err("a put nobody can confirm has no answer");
    assert_eq!(error.code(), ErrorCode::OutcomeUnknown);
    assert_matches!(
        error,
        CoreError::OutcomeUnknown { object_key: actual_key, .. } if actual_key == object_key
    );
    assert!(store
        .inner()
        .head(&object_key)
        .await
        .expect("head")
        .is_none());
}

/// Checks that a manifest put whose response is lost, and whose read-back
/// ends after the publication budget, is settled by the read-back: the
/// manifest found at its number is its own, for a fork's first manifest and
/// for a successor.
#[tokio::test]
async fn an_ambiguous_manifest_read_back_after_its_budget_settles_the_put() {
    use crate::limits::METADATA_PUBLICATION_BUDGET_MS;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_test_support::stores::MetadataMapStore;
    use loonfs_types::format::control::ForkBasis;

    for fork in [false, true] {
        let elapsed_ms = METADATA_PUBLICATION_BUDGET_MS + 1;
        let directory = tempdir().expect("directory");
        let source = NamespaceId::parse("source").expect("namespace");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        crate::test_support::NamespaceFixture::new(&store, &source, &test_context())
            .create()
            .await
            .expect("create");
        let current = crate::test_support::namespace_store(&store, &source)
            .load_current_manifest()
            .await
            .expect("current");
        let mut payload = current.payload().clone();
        if fork {
            payload.namespace_id = NamespaceId::parse("fork").expect("namespace");
            payload.fork_basis = Some(ForkBasis {
                manifest: current.manifest(),
                fork_point_seq: current.manifest().head_seq,
                source_snapshot_id: SnapshotId::generate(current.manifest().manifest_no),
                forked_from_snapshot_id: SnapshotId::generate(current.manifest().manifest_no),
                fork_point_snapshot_id: SnapshotId::generate(ManifestNo(1)),
                rebased_from: None,
            });
            payload.status = loonfs_types::format::control::NamespaceStatus::Initializing {};
            payload.manifest_no = ManifestNo(1);
        } else {
            payload.manifest_no = ManifestNo(3);
        }
        let object_key = metadata_manifest_object(&payload.namespace_id, &payload.manifest_no);
        let timer = Arc::new(ManualClock::new(0));
        let deadline = OperationClock::start(0, timer.clone());
        store.reset();
        let store =
            MetadataMapStore::new(store, KeyPredicate::exact(&object_key), move |metadata| {
                timer.advance_ms(elapsed_ms);
                metadata
            });
        let store = FailStore::new(
            store,
            KeyPredicate::exact(&object_key),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost acknowledgement".to_owned()),
        )
        .apply_then_fail();
        store.fail_next(1);
        // A fork's first manifest is created, and any other manifest
        // is published over its predecessor.
        let outcome = if fork {
            let namespace = crate::test_support::namespace_store(&store, &payload.namespace_id);
            namespace
                .publish_first_manifest(payload, &deadline)
                .await
                .map(|_| None)
        } else {
            publish_once_within(&store, payload, &deadline)
                .await
                .map(Some)
        };
        if fork {
            assert_matches!(outcome, Ok(None));
        } else {
            assert_matches!(outcome, Ok(Some(ManifestPublicationOutcome::Published(_))));
        }
        assert_eq!(store.remaining(), 0);
        assert_eq!(store.inner().inner().counts().create_if_absent_puts, 1);
        assert_eq!(store.inner().inner().counts().compare_and_swaps, 0);
        assert!(store
            .inner()
            .inner()
            .take_gets()
            .iter()
            .any(|(key, _)| key == &object_key));
    }
}

/// Checks that of two retention window changes with the same content, which
/// store different bytes, the change whose put loses reports `Lost`.
#[tokio::test]
async fn window_changes_with_the_same_content_report_the_loser_as_lost() {
    for race_at_put in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        bootstrap_namespace(&store, &namespace_id, &test_context())
            .await
            .expect("bootstrap");
        let current = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("current");
        let mut payload = current.payload().clone();
        payload.manifest_no = payload.manifest_no.successor().expect("next manifest");
        payload.retention_window_ms =
            std::num::NonZeroU64::new(2 * crate::limits::GC_GRACE_WINDOW_MS);
        let change = payload;
        let blocked = BlockingStore::new(
            store.clone(),
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::Put,
        );
        let (losing, winning) = if race_at_put {
            blocked.block_next();
            let losing = publish_once(&blocked, change.clone());
            let winning = async {
                blocked.wait_until_blocked().await;
                let outcome = publish_once(&store, change.clone()).await;
                blocked.release();
                outcome
            };
            futures::join!(losing, winning)
        } else {
            let winning = publish_once(&store, change.clone()).await;
            (publish_once(&store, change).await, winning)
        };
        assert_matches!(
            winning.expect("winner"),
            ManifestPublicationOutcome::Published(_)
        );
        assert_matches!(losing.expect("loser"), ManifestPublicationOutcome::Lost(_));
    }
}

#[tokio::test]
async fn publishers_racing_one_number_load_the_winner_and_retry_when_needed() {
    for newer_head in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::manifest(&namespace_id),
        ));
        let context = test_context();
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        crate::test_support::namespace_store(&store, &namespace_id)
            .acquire_writer_epoch(&context.writer_id)
            .await
            .expect("acquire");
        let current = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("current");
        let predecessor = current.manifest().manifest_no;
        let mut payload = current.payload().clone();
        payload.manifest_no = predecessor.successor().expect("next number");
        let first = payload.clone();
        if newer_head {
            let mut engine = crate::commit::engine::NamespaceCommitEngine::with_acquired_session(
                crate::test_support::namespace_store(&store, &namespace_id),
                context.writer_id.clone(),
                crate::namespace::writer_epoch::AcquiredWriter {
                    writer_id: context.writer_id.clone(),
                    writer_epoch: current.payload().writer_epoch,
                },
            );
            engine
                .publish_batch(
                    vec![crate::commit::engine::CommitCandidate::new(
                        crate::path::write::CommitIntent {
                            commit_id: loonfs_types::CommitId::generate(),
                            actor_id: loonfs_test_support::ids::test_actor(),
                            subject: None,
                            message: None,
                            preconditions: Vec::new(),
                            operations: vec![
                                crate::path::write::FilesystemOperation::CreateDirectory {
                                    path: loonfs_types::AbsolutePath::parse("/file").expect("path"),
                                    parents: false,
                                },
                            ],
                        },
                    )],
                    &context.clock(),
                )
                .await
                .results
                .remove(0)
                .expect("write");
            let projection = load_current_projection(&store, &namespace_id)
                .await
                .expect("projection");
            payload = build_namespace_manifest_from_metadata_state(
                &store,
                &namespace_id,
                ManifestMetadataSource {
                    head: &projection.head,
                    basis_manifest_no: Some(predecessor),
                    retention_floor_seq: CommitSeq(0),
                    metadata_state: &projection.metadata_state,
                },
                MetadataRunPolicy::default(),
                payload.manifest_no,
            )
            .await
            .expect("build")
            .into_payload();
        } else {
            payload.next_run_no = RunNo(payload.next_run_no.0 + 1);
        }
        let second = payload;
        store.reset();
        let blocked = BlockingStore::new(
            store.clone(),
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::Put,
        );
        blocked.block_next();
        let losing = publish_once(&blocked, second.clone());
        let winning = async {
            blocked.wait_until_blocked().await;
            let result = publish_once(&store, first).await;
            blocked.release();
            result
        };
        let (losing, winning) = futures::join!(losing, winning);
        assert_matches!(
            winning.expect("winner"),
            ManifestPublicationOutcome::Published(_)
        );
        let losing = losing.expect("loser loads winner");
        assert_eq!(store.counts().create_if_absent_puts, 2);
        assert_eq!(store.counts().compare_and_swaps, 0);
        // Each publisher finds the newest manifest with one listing first.
        assert_eq!(store.counts().lists, 2);
        // Each publisher loads the current manifest, and the loser loads the
        // winner.
        assert!(store.counts().gets >= 3);
        let keys = store
            .inner()
            .list_prefix(&metadata_manifest_prefix(&namespace_id))
            .await
            .expect("list for assertion");
        // The create's two manifests, the claim, and the winner's manifest.
        assert_eq!(keys.len(), 4);
        if newer_head {
            assert_matches!(losing, ManifestPublicationOutcome::Lost(_));
            let mut retry = second;
            retry.manifest_no = retry.manifest_no.successor().expect("next number");
            assert_matches!(
                publish_once(&store, retry).await.expect("publish retry"),
                ManifestPublicationOutcome::Published(_)
            );
        } else {
            assert_matches!(losing, ManifestPublicationOutcome::Lost(_));
        }
    }
}

/// Checks that an advance without a writer session creates a floor object
/// and then folds it, each with put-if-absent, and claims no epoch. The
/// fold of a tail with the floor object and no commits publishes a manifest
/// that changes nothing but the floor and the folded WAL number, and
/// writers read the new floor.
#[tokio::test]
async fn an_advance_without_a_session_creates_a_floor_object_and_folds_it() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/file", b"data", None)
        .await
        .expect("write");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    store.reset();
    let advanced = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance")
    .response;
    // The floor object and the fold.
    assert_eq!(store.counts().create_if_absent_puts, 2);
    assert_eq!(store.counts().compare_and_swaps, 0);
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let after = namespace.load_current_manifest().await.expect("current");
    let mut expected = before.payload().clone();
    expected.manifest_no = expected.manifest_no.successor().expect("next");
    expected.folded_wal_no = expected.folded_wal_no.successor().expect("next");
    expected.retention_floor_seq = before.payload().head_seq;
    expected.publication_id = after.payload().publication_id.clone();
    assert_eq!(after.payload(), &expected);
    assert_eq!(advanced.retention_floor_seq, before.payload().head_seq);
    let expected = after.payload().clone();
    let anchor = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("writer floor");
    assert_eq!(anchor.retention_floor_seq(), expected.head_seq);
    store.reset();
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("already advanced");
    assert_eq!(store.counts().puts, 0);
}

/// Checks that a manifest put that succeeds, followed by a read of the
/// manifest boundary that fails, returns `outcome_unknown` for the
/// manifest's key.
#[tokio::test]
async fn a_failed_boundary_read_after_a_put_has_an_unknown_outcome() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(boundary(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("boundary read failed".to_owned()),
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("bootstrap");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let basis = namespace
        .load_current_basis(crate::test_support::observed_now())
        .await
        .expect("current");
    let mut payload = basis.manifest().payload().clone();
    payload.manifest_no = ManifestNo(3);
    store.fail_next(1);
    let outcome = namespace
        .publish_manifest_from(
            payload,
            &OperationClock::start(0, crate::test_support::std_timer()),
            Some(&basis),
        )
        .await;
    let object_key = metadata_manifest_object(&namespace_id, &ManifestNo(3));
    assert_matches!(
        outcome,
        Err(CoreError::OutcomeUnknown { object_key: actual, .. }) if actual == object_key
    );
    assert_eq!(store.remaining(), 0);
    assert!(store
        .inner()
        .head(&object_key)
        .await
        .expect("the put")
        .is_some());
}

#[tokio::test]
async fn manifest_publication_recovers_an_ambiguous_put() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let mut payload = current.payload().clone();
    payload.manifest_no = payload.manifest_no.successor().expect("next");
    let candidate_manifest_no = payload.manifest_no;
    let store = FailStore::new(
        store,
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::Put,
        InjectedError::Transport("lost acknowledgment".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    let outcome = publish_once(&store, payload).await.expect("published");
    // The manifest that the read-back finds has the put's publication ID.
    assert_matches!(outcome, ManifestPublicationOutcome::Published(_));
    assert_eq!(
        crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("discover")
            .manifest()
            .manifest_no,
        candidate_manifest_no
    );
}

#[tokio::test]
async fn a_manifest_update_stops_after_the_contention_limit() {
    use super::super::publish::ManifestChange;

    for rival_publishes in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let store = LocalFsStore::new(directory.path()).expect("store");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
            .create()
            .await
            .expect("bootstrap");
        let deadline = OperationClock::start(0, Arc::new(StdMonotonicTimer::default()));
        let mut attempts = 0;
        let result = crate::test_support::namespace_store(&store, &namespace_id)
            .update_manifest(
                None,
                &deadline,
                |current: crate::namespace::control::LoadedManifest| {
                    attempts += 1;
                    let store = &store;
                    let payload = current.payload().clone();
                    async move {
                        if !rival_publishes {
                            return Ok(ManifestChange::Again);
                        }
                        let mut rival = payload.clone();
                        rival.manifest_no = rival.manifest_no.successor().expect("next number");
                        let key = metadata_manifest_object(&rival.namespace_id, &rival.manifest_no);
                        store
                            .put_if_absent(&key, Bytes::from(encode_manifest(rival).into_bytes()))
                            .await
                            .expect("rival manifest");
                        Ok(ManifestChange::Next(Box::new(payload), ()))
                    }
                },
            )
            .await;
        let Err(CoreError::StoreContention { object_key }) = result else {
            panic!("expected StoreContention, got {:?}", result.map(|_| ()));
        };
        assert_eq!(attempts, crate::limits::MAX_CONTENTION_ATTEMPTS);
        assert!(
            object_key.starts_with(&metadata_manifest_prefix(&namespace_id)),
            "{object_key}"
        );
    }
}

#[tokio::test]
async fn a_fold_whose_manifest_put_succeeds_without_an_answer_reports_published() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost acknowledgement".to_owned()),
    )
    .apply_then_fail();
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/file", b"data", None)
        .await
        .expect("write");
    let predecessor = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current")
        .manifest()
        .manifest_no;
    store.fail_next(1);
    let folded = crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    assert_eq!(store.remaining(), 0);
    assert_eq!(folded.outcome, crate::FoldWalOutcome::Published);
    assert_eq!(
        folded.manifest_no,
        predecessor.successor().expect("next number")
    );
}

#[tokio::test]
async fn a_snapshot_losing_manifest_publication_keeps_the_winner() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/file", b"data", None)
        .await
        .expect("write");
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::Put,
    );
    blocked.block_next();
    let losing = create_snapshot(&blocked, &namespace_id, &context);
    let winning = async {
        blocked.wait_until_blocked().await;
        let snapshot = create_snapshot(&store, &namespace_id, &context).await;
        blocked.release();
        snapshot
    };
    let (losing, winning) = futures::join!(losing, winning);
    let losing = losing.expect("losing publication still keeps the winner");
    let winning = winning.expect("winner");
    let losing = crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&losing.snapshot_id)
        .await
        .expect("record")
        .expect("present");
    let winning = crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&winning.snapshot_id)
        .await
        .expect("record")
        .expect("present");
    assert_eq!(losing.state.manifest(), winning.state.manifest());
    let statistics = crate::test_support::namespace_store(&store, &namespace_id)
        .load_statistics()
        .await
        .expect("statistics");
    assert_eq!(
        statistics.activity,
        loonfs_types::format::manifest::ManifestActivity {
            content_bytes: loonfs_types::format::manifest::ActivityCounter::parse(4)
                .expect("activity"),
            file_revisions: loonfs_types::format::manifest::ActivityCounter::parse(1)
                .expect("activity"),
            mutations: loonfs_types::format::manifest::ActivityCounter::parse(1).expect("activity"),
        }
    );
}

#[tokio::test]
async fn namespace_status_and_change_feed_report_the_floor_at_the_head_after_an_advance() {
    // Retention publishes the floor independently of the head. A reader must
    // not report a floor newer than its head.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .write_file_bytes("/docs/hello.txt", b"hello\n", None)
        .await
        .expect("write hello");
    create_snapshot(&inner, &namespace_id, &context)
        .await
        .expect("create snapshot");
    crate::test_support::NamespaceFixture::new(
        &inner,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance retention");

    let namespace = crate::test_support::namespace_store(&inner, &namespace_id)
        .load_metadata()
        .await
        .expect("status");
    assert_eq!(namespace.head_seq, CommitSeq(1));
    assert_eq!(namespace.retention_floor_seq, CommitSeq(1));

    let namespace = crate::test_support::namespace_store(&inner, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load feed view");
    let changes = list_changes_after(
        &view,
        loonfs_types::ChangeFeedPosition::after(CommitSeq(1)),
        EffectiveLimit::new(NonZeroU32::new(10).expect("nonzero")),
        loonfs_types::options::CommitSplitting::Whole,
    )
    .await
    .expect("change feed");
    assert_eq!(changes.head_seq, CommitSeq(1));
    assert!(changes.changes.is_empty());
}

/// Checks that a retention window change whose put was written but lost its
/// response finds its own publication ID at the read-back, and publishes
/// once.
#[tokio::test]
async fn an_ambiguous_window_change_confirms_its_own_put() {
    use loonfs_test_support::stores::{FailStore, InjectedError};

    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("window-change").expect("namespace");
    let change_key = metadata_manifest_object(&namespace_id, &ManifestNo(3));
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new(move |key| key == change_key),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("response lost".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    // The change was written, and the read-back finds the change's own
    // publication ID, so the change publishes manifest 3 and no other.
    let changed = crate::test_support::change_retention_window(
        &crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine(),
    )
    .await;
    assert_eq!(store.remaining(), 0, "the change's put lost its response");
    assert_eq!(changed.manifest().payload().manifest_no, ManifestNo(3));
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert_eq!(current.manifest(), changed.manifest().manifest());
}

/// Checks that two puts of one payload at one number store different bytes,
/// which differ only in the publication ID.
#[tokio::test]
async fn two_puts_of_one_payload_store_different_bytes() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let basis = namespace
        .load_current_basis(crate::test_support::observed_now())
        .await
        .expect("current");
    let mut payload = basis.manifest().payload().clone();
    payload.manifest_no = payload.manifest_no.successor().expect("next manifest");
    let mut published = Vec::new();
    for _ in 0..2 {
        let outcome = namespace
            .publish_manifest_from(
                payload.clone(),
                &OperationClock::start(0, crate::test_support::std_timer()),
                Some(&basis),
            )
            .await
            .expect("publish");
        let ManifestPublicationOutcome::Published(outcome) = outcome else {
            panic!("the put is published, got {outcome:?}");
        };
        published.push(outcome.into_manifest());
        // The number is free again for the second put.
        store
            .delete(&metadata_manifest_object(
                &namespace_id,
                &payload.manifest_no,
            ))
            .await
            .expect("delete");
    }
    let [first, second] = &published[..] else {
        panic!("two publications");
    };
    assert_ne!(first.manifest(), second.manifest());
    assert_ne!(
        first.payload().publication_id,
        second.payload().publication_id
    );
    let mut second_payload = second.payload().clone();
    second_payload.publication_id = first.payload().publication_id.clone();
    assert_eq!(first.payload(), &second_payload);
}

/// Checks that a successor built as a copy of its predecessor's payload is
/// published with a publication ID of its own.
#[tokio::test]
async fn a_successor_that_copies_its_predecessor_gets_its_own_publication_id() {
    use super::super::publish::ManifestChange;

    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let predecessor = namespace.load_current_manifest().await.expect("current");
    let update = namespace
        .update_manifest(
            None,
            &OperationClock::start(0, crate::test_support::std_timer()),
            |current: crate::namespace::control::LoadedManifest| async move {
                Ok(ManifestChange::Next(
                    Box::new(current.payload().clone()),
                    (),
                ))
            },
        )
        .await
        .expect("publish a copy");
    assert!(update.published);
    let successor = namespace.load_current_manifest().await.expect("current");
    assert_eq!(successor.manifest(), update.current.manifest().manifest());
    assert_eq!(
        successor.payload().manifest_no,
        predecessor.payload().manifest_no.successor().expect("next")
    );
    assert_ne!(
        successor.payload().publication_id,
        predecessor.payload().publication_id
    );
}

/// Checks that a fold whose manifest put loses its response, and that then
/// finds another process's fold at its number, ends on that fold's manifest
/// with the check of the position it folded from, not with the start of its
/// own clock.
///
/// A third manifest was published before the fold's clock started, so no
/// check at that start could have found the other fold the newest. A writer
/// position that took the start of the clock would let an eventual read
/// skip the check that finds the third manifest.
#[tokio::test]
async fn a_fold_that_finds_another_fold_after_a_lost_put_keeps_its_position_check() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let context = test_context();
    let local = LocalFsStore::new(directory.path()).expect("store");
    bootstrap_namespace(&local, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&local, &namespace_id, &context)
        .append_wal_objects(2)
        .await
        .expect("WAL tail");
    let basis_no = crate::test_support::namespace_store(&local, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current")
        .payload()
        .manifest_no;
    let head = crate::test_support::namespace_store(&local, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    let position_checked = crate::test_support::observed_now();
    let position = crate::test_support::namespace_store(&local, &namespace_id)
        .load_publish_metadata_view(
            crate::namespace::writer_epoch::AcquiredWriter {
                writer_id: context.writer_id.clone(),
                writer_epoch: head.writer_epoch,
            },
            None,
            None,
            position_checked.clone(),
        )
        .await
        .expect("position")
        .into_position();
    // Another process folds the tail, and then publishes a manifest above
    // that fold, before this fold's clock starts.
    crate::test_support::namespace_store(&local, &namespace_id)
        .fold_wal()
        .await
        .expect("another process's fold");
    crate::test_support::change_retention_window(
        &crate::test_support::NamespaceFixture::new(&local, &namespace_id, &context).engine(),
    )
    .await;
    let other_fold = basis_no.successor().expect("next number");
    assert_eq!(
        crate::test_support::namespace_store(&local, &namespace_id)
            .load_current_manifest()
            .await
            .expect("current")
            .payload()
            .manifest_no,
        other_fold.successor().expect("next number")
    );

    let store = FailStore::new(
        local,
        KeyPredicate::exact(metadata_manifest_object(&namespace_id, &other_fold)),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost request".to_owned()),
    );
    store.fail_next(1);
    let folded = crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal_tail(
            Some(position),
            &crate::time::OperationClock::start(
                0,
                Arc::new(crate::time::StdMonotonicTimer::default()),
            ),
        )
        .await
        .expect("fold");
    assert_eq!(store.remaining(), 0, "the fold's put lost its response");
    assert_eq!(
        folded.response.outcome,
        crate::FoldWalOutcome::AlreadyCurrent
    );
    assert_eq!(folded.current.manifest_no(), other_fold);
    assert_eq!(
        folded.current.checked(),
        &position_checked,
        "the found fold keeps the check of the position's manifest"
    );
}
