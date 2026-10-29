//! Numbered publication, discovery, and retention contracts.

use super::*;
use loonfs_objectstore::ListedObject;
use loonfs_test_support::stores::replace_object;
use loonfs_types::format::control::{encode_control_state, ControlObjectKind, HintPayload};
use std::assert_matches;

#[tokio::test]
async fn a_manifest_put_returning_after_its_budget_has_an_unknown_outcome() {
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
    let manifest = encode_manifest(payload);
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
    let error = publish_once_within(&store, manifest, &deadline)
        .await
        .expect_err("late put must not acknowledge publication");
    assert_matches!(
        error,
        CoreError::OutcomeUnknown { object_key: actual_key, .. } if actual_key == object_key
    );
    assert_eq!(store.inner().counts().create_if_absent_puts, 1);
    assert_eq!(store.inner().counts().compare_and_swaps, 0);
    let operations = store.inner().snapshot();
    let last = operations.last().expect("numbered put");
    assert_matches!(
        last,
        loonfs_test_support::stores::RecordedOperation::Put {
            mode: loonfs_objectstore::PutMode::CreateIfAbsent,
            ..
        }
    );
    assert_eq!(last.key(), object_key);
    assert!(store
        .inner()
        .inner()
        .head(&object_key)
        .await
        .expect("landed manifest")
        .is_some());
}

#[tokio::test]
async fn a_manifest_put_that_lands_nowhere_and_loses_its_answer_has_an_unknown_outcome() {
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
    let error = publish_once(&store, encode_manifest(payload))
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

#[tokio::test]
async fn an_ambiguous_manifest_read_back_must_finish_within_its_budget() {
    use crate::limits::METADATA_PUBLICATION_BUDGET_MS;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_test_support::stores::MetadataMapStore;
    use loonfs_types::format::control::ForkBasis;

    for fork in [false, true] {
        for elapsed_ms in [
            METADATA_PUBLICATION_BUDGET_MS,
            METADATA_PUBLICATION_BUDGET_MS + 1,
        ] {
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
                    source_pin_id: PinId::generate(ManifestNo(1)),
                });
            } else {
                payload.manifest_no = ManifestNo(2);
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
                crate::test_support::namespace_store(&store, &payload.namespace_id)
                    .create_manifest(payload, &deadline)
                    .await
                    .map(|()| None)
            } else {
                publish_once_within(&store, encode_manifest(payload), &deadline)
                    .await
                    .map(Some)
            };
            if elapsed_ms > METADATA_PUBLICATION_BUDGET_MS {
                assert_matches!(
                    outcome,
                    Err(CoreError::OutcomeUnknown {
                        object_key: actual_key,
                        message,
                    }) if actual_key == object_key && message == format!(
                        "the publication took {elapsed_ms}ms, over its {METADATA_PUBLICATION_BUDGET_MS}ms budget"
                    )
                );
            } else if fork {
                assert_matches!(outcome, Ok(None));
            } else {
                assert_matches!(
                    outcome,
                    Ok(Some(ManifestPublicationOutcome::CoveredByCurrent(_)))
                );
            }
            assert_eq!(store.remaining(), 0);
            // A creation also writes the hint.
            assert_eq!(
                store.inner().inner().counts().create_if_absent_puts,
                1 + usize::from(fork)
            );
            assert_eq!(store.inner().inner().counts().compare_and_swaps, 0);
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
async fn identical_compactor_claims_report_the_loser_as_covered() {
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
        payload.compactor_epoch.0 += 1;
        let claim = encode_manifest(payload);
        let blocked = BlockingStore::new(
            store.clone(),
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::Put,
        );
        let (losing, winning) = if race_at_put {
            blocked.block_next();
            let losing = publish_once(&blocked, claim.clone());
            let winning = async {
                blocked.wait_until_blocked().await;
                let outcome = publish_once(&store, claim.clone()).await;
                blocked.release();
                outcome
            };
            futures::join!(losing, winning)
        } else {
            let winning = publish_once(&store, claim.clone()).await;
            (publish_once(&store, claim).await, winning)
        };
        assert_matches!(
            winning.expect("winner"),
            ManifestPublicationOutcome::Published(_)
        );
        assert_matches!(
            losing.expect("loser"),
            ManifestPublicationOutcome::CoveredByCurrent(_)
        );
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
        let first = encode_manifest(payload.clone());
        if newer_head {
            let session = Arc::new(std::sync::Mutex::new(
                crate::commit::engine::WriterSessionStatus::Acquired(
                    crate::namespace::writer_epoch::AcquiredWriter {
                        writer_id: context.writer_id.clone(),
                        writer_epoch: current.payload().writer_epoch,
                    },
                ),
            ));
            let mut engine = crate::commit::engine::NamespaceCommitEngine::new(
                crate::test_support::namespace_store(&store, &namespace_id),
                context.writer_id.clone(),
            )
            .writer_session(session);
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
                    retention_floor_seq: ChangeSeq(0),
                    metadata_state: &projection.metadata_state,
                },
                MetadataRunPolicy::default(),
                payload.manifest_no,
            )
            .await
            .expect("build")
            .into_payload();
        } else {
            payload.next_inode_id = InodeId(payload.next_inode_id.0 + 1);
        }
        let second = encode_manifest(payload);
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
        assert_eq!(store.counts().lists, 0);
        assert!(store.counts().gets >= 4);
        let keys = store
            .inner()
            .list_prefix(&metadata_manifest_prefix(&namespace_id))
            .await
            .expect("list for assertion");
        assert_eq!(keys.len(), 3);
        if newer_head {
            assert_matches!(losing, ManifestPublicationOutcome::PredecessorChanged(_));
            let mut retry = second.into_parts().0.into_payload();
            retry.manifest_no = retry.manifest_no.successor().expect("next number");
            assert_matches!(
                publish_once(&store, encode_manifest(retry))
                    .await
                    .expect("publish retry"),
                ManifestPublicationOutcome::Published(_)
            );
        } else {
            assert_matches!(losing, ManifestPublicationOutcome::CoveredByCurrent(_));
        }
    }
}

/// Points the namespace's hint at `manifest_no`, discovers the current
/// manifest, and returns it with the discovery's requests for the hint and
/// the manifests.
async fn discover_from_hint(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
    manifest_no: u64,
) -> (ManifestNo, Vec<(&'static str, String)>) {
    let bytes = encode_control_state(
        ControlObjectKind::Hint,
        &HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(manifest_no),
        },
    )
    .expect("hint");
    replace_object(store, &hint(namespace_id), Bytes::from(bytes))
        .await
        .expect("rewind hint");
    store.reset();
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("discover");
    (
        current.manifest().manifest_no,
        control_requests(namespace_id, &store.take()),
    )
}

/// Checks that discovery loads the hinted manifest and sends a HEAD request
/// for the number after it, which ends discovery when the number is missing.
/// When it exists, discovery probes the numbers above it a window at a time
/// with HEAD requests, loads only the newest manifest found, and reads the
/// hint again. A tombstone that the probe finds ends discovery, and a
/// missing hint reads as an absent namespace.
#[tokio::test]
async fn discovery_probes_windows_above_the_hint_and_a_missing_hint_reads_as_absent() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("bootstrap");
    for _ in 2..=11 {
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .claim_compactor()
        .await
        .expect("publish a claim");
    }
    let window = u64::try_from(crate::limits::MANIFEST_PROBE_WINDOW).expect("a small window");
    let hint = hint(&namespace_id);
    let get = |number| {
        (
            "get",
            metadata_manifest_object(&namespace_id, &ManifestNo(number)),
        )
    };
    let heads = |first: u64, last: u64| {
        (first..=last)
            .map(|number| {
                (
                    "head",
                    metadata_manifest_object(&namespace_id, &ManifestNo(number)),
                )
            })
            .collect::<Vec<_>>()
    };
    // A current hint costs one HEAD request. A hint 1 or 3 manifests behind
    // costs that HEAD request and one window above it, and a hint 9
    // manifests behind costs two windows, because the first one ends at a
    // manifest that exists.
    for (hinted, probed_through) in [
        (11, 12),
        (10, 11 + window),
        (8, 9 + window),
        (2, 3 + 2 * window),
    ] {
        let mut expected = vec![("get", hint.clone()), get(hinted)];
        expected.extend(heads(hinted + 1, probed_through));
        if hinted != 11 {
            expected.push(get(11));
        }
        expected.push(("get", hint.clone()));
        assert_eq!(
            discover_from_hint(&store, &namespace_id, hinted).await,
            (ManifestNo(11), expected),
            "a hint at manifest {hinted}"
        );
    }

    let ((), tombstone, _) = crate::test_support::namespace_store(&store, &namespace_id)
        .update_manifest(
            None,
            &OperationClock::start(0, crate::test_support::std_timer()),
            crate::limits::MAX_CONTENTION_ATTEMPTS,
            |current: crate::namespace::control::LoadedManifest| async move {
                let mut payload = current.payload().clone();
                payload.status =
                    loonfs_types::format::control::NamespaceStatus::Deleted { deleted_at_ms: 1 };
                Ok(super::super::publish::ManifestChange::Next(
                    Box::new(payload),
                    (),
                ))
            },
        )
        .await
        .expect("publish a tombstone");
    assert_eq!(tombstone.manifest().manifest_no, ManifestNo(12));
    let mut expected = vec![("get", hint.clone()), get(8)];
    expected.extend(heads(9, 9 + window));
    expected.push(get(12));
    assert_eq!(
        discover_from_hint(&store, &namespace_id, 8).await,
        (ManifestNo(12), expected),
        "the tombstone ends discovery without another read of the hint"
    );

    store.delete(&hint).await.expect("delete hint");
    store.reset();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_metadata_view()
        .await
        .err()
        .expect("missing hint");
    assert_eq!(error.code(), ErrorCode::NamespaceNotFound);
    assert_eq!(store.counts().puts, 0);
}

#[tokio::test]
async fn retention_publishes_only_a_number_and_floor_change_and_writers_read_it() {
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
    // The advance publishes its manifest and leaves the hint for the
    // runtime to raise.
    assert_eq!(store.counts().create_if_absent_puts, 1);
    assert_eq!(store.counts().compare_and_swaps, 0);
    let after = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let mut expected = before.payload().clone();
    expected.manifest_no = expected.manifest_no.successor().expect("next");
    expected.retention_floor_seq = expected.head_seq;
    assert_eq!(after.payload(), &expected);
    assert_eq!(advanced.retention_floor_seq, expected.head_seq);
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
    let candidate = encode_manifest(payload);
    let candidate_manifest_no = candidate.envelope().payload().manifest_no;
    let store = FailStore::new(
        store,
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::Put,
        InjectedError::Transport("lost acknowledgment".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    let outcome = publish_once(&store, candidate).await.expect("published");
    // The put succeeded, but a rival could have written the same bytes, so
    // the read-back counts as the current manifest.
    assert_matches!(outcome, ManifestPublicationOutcome::CoveredByCurrent(_));
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
async fn a_manifest_update_returns_the_hint_it_loaded_and_leaves_it_unraised() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::hint(&namespace_id),
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("bootstrap");
    let before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_hint()
        .await
        .expect("stored hint");
    store.reset();
    let deadline = OperationClock::start(0, Arc::new(StdMonotonicTimer::default()));
    let ((), published, hint) = crate::test_support::namespace_store(&store, &namespace_id)
        .update_manifest(
            None,
            &deadline,
            crate::limits::MAX_CONTENTION_ATTEMPTS,
            |current: crate::namespace::control::LoadedManifest| async move {
                Ok(super::super::publish::ManifestChange::Next(
                    Box::new(current.payload().clone()),
                    (),
                ))
            },
        )
        .await
        .expect("publish");
    assert_eq!(published.manifest().manifest_no, ManifestNo(2));
    assert_eq!(hint.as_ref(), Some(&before));
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert_eq!(
        hint,
        Some(
            crate::test_support::namespace_store(&store, &namespace_id)
                .load_hint()
                .await
                .expect("stored hint")
        )
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
                crate::limits::MAX_CONTENTION_ATTEMPTS,
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
async fn a_fold_whose_manifest_put_lands_without_an_answer_reports_published() {
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
async fn a_checkpoint_losing_manifest_publication_pins_the_winner() {
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
    let losing = create_checkpoint(&blocked, &namespace_id, &context);
    let winning = async {
        blocked.wait_until_blocked().await;
        let checkpoint = create_checkpoint(&store, &namespace_id, &context).await;
        blocked.release();
        checkpoint
    };
    let (losing, winning) = futures::join!(losing, winning);
    let losing = losing.expect("losing publication still pins the winner");
    let winning = winning.expect("winner");
    let losing = crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&losing.checkpoint_id)
        .await
        .expect("record")
        .expect("present");
    let winning = crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&winning.checkpoint_id)
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

#[derive(Debug)]
struct StaleObjectOnceStore {
    inner: LocalFsStore,
    key: String,
    stale: std::sync::Mutex<Option<ObjectBody>>,
}

#[async_trait]
impl ObjectStore for StaleObjectOnceStore {
    loonfs_test_support::delegate_object_store!(self; buffered);

    fn checksum_algorithm(&self) -> loonfs_types::ChecksumAlgorithm {
        self.inner.checksum_algorithm()
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        if key == self.key {
            if let Some(stale) = self.stale.lock().expect("stale object lock").take() {
                return Ok(Some(stale));
            }
        }
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_entries_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ListedObject, ObjectStoreError>> {
        self.inner.list_entries_from_stream(prefix, start_after)
    }

    async fn list_child_prefixes(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: loonfs_types::EffectiveLimit,
    ) -> Result<loonfs_types::Page<String, String>, ObjectStoreError> {
        self.inner
            .list_child_prefixes(prefix, start_after, limit)
            .await
    }
}

#[tokio::test]
async fn namespace_status_and_change_feed_reload_a_head_behind_the_floor() {
    // Retention publishes the floor independently of the head. A reader that
    // straddles those writes must not report a floor newer than its head.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let stale_head = inner
        .get_with_metadata(&hint(&namespace_id))
        .await
        .expect("read bootstrap head")
        .expect("bootstrap head exists");

    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .write_file_bytes("/docs/hello.txt", b"hello\n", None)
        .await
        .expect("write hello");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("create checkpoint");
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

    let status_store = StaleObjectOnceStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("status store"),
        key: hint(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_head.clone())),
    };
    let namespace = crate::test_support::namespace_store(&status_store, &namespace_id)
        .load_metadata()
        .await
        .expect("status reloads the stale head");
    assert_eq!(namespace.head_seq, ChangeSeq(1));
    assert_eq!(namespace.retention_floor_seq, ChangeSeq(1));

    let feed_store = StaleObjectOnceStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("change-feed store"),
        key: hint(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_head)),
    };
    let namespace = crate::test_support::namespace_store(&feed_store, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load feed view");
    let changes = list_changes_after(
        &view,
        ChangeSeq(1),
        EffectiveLimit::new(NonZeroU32::new(10).expect("nonzero")),
    )
    .await
    .expect("change feed reloads the stale head");
    assert_eq!(changes.head_seq, ChangeSeq(1));
    assert!(changes.changes.is_empty());
}

#[tokio::test]
async fn an_ambiguous_compactor_claim_retries_instead_of_confirming() {
    use loonfs_test_support::stores::{FailStore, InjectedError};

    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("claimed").expect("namespace");
    let claim_key = metadata_manifest_object(&namespace_id, &ManifestNo(2));
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new(move |key| key == claim_key),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("response lost".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    // The claim landed, but a rival could have written the same bytes, so the
    // claimant reads it as the rival's and takes the next epoch.
    let epoch = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .claim_compactor()
    .await
    .expect("claim")
    .response;
    assert_eq!(epoch, loonfs_types::CompactorEpoch(2));
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert_eq!(current.payload().manifest_no, ManifestNo(3));
    assert_eq!(
        current.payload().compactor_epoch,
        loonfs_types::CompactorEpoch(2)
    );
}
