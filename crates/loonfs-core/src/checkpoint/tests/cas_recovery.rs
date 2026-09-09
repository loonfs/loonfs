//! Numbered publication, discovery, and retention contracts.

use super::*;
use loonfs_api::wire::control::{encode_control_state, ControlObjectKind, HintState};

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
        crate::namespace::bootstrap::bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        crate::namespace::writer_epoch::acquire_writer_epoch(&store, &namespace_id, &context)
            .await
            .expect("acquire");
        let current = load_current_manifest(&store, &namespace_id)
            .await
            .expect("current");
        let predecessor = current.state.manifest.manifest_no;
        let mut payload = current.envelope.payload().clone();
        payload.manifest_no = predecessor.successor().expect("next number");
        let first = encode_manifest(payload.clone()).expect("candidate");
        if newer_head {
            let session = Arc::new(std::sync::Mutex::new(
                crate::commit_engine::WriterSessionState::Acquired(
                    loonfs_api::wire::control::AcquiredWriter {
                        writer_id: context.writer_id.clone(),
                        writer_epoch: current.envelope.payload().writer_epoch,
                    },
                ),
            ));
            let mut engine = crate::commit_engine::NamespaceCommitEngine::new(namespace_id.clone())
                .writer_session(session);
            engine
                .publish_batch(
                    &store,
                    vec![crate::commit_engine::CommitCandidate::new(
                        crate::path::write::CommitRequest {
                            commit_id: loonfs_api::CommitId::generate(),
                            actor_id: loonfs_test_support::ids::test_actor(),
                            message: None,
                            assertions: Vec::new(),
                            operations: vec![
                                crate::path::write::FilesystemOperation::CreateDirectory {
                                    path: loonfs_api::AbsolutePath::parse("/file").expect("path"),
                                    parents: false,
                                },
                            ],
                        },
                    )],
                    &context,
                    &crate::protocol::PublishTailOptions::default(),
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
                MetadataLsmPolicy::default(),
                payload.manifest_no,
            )
            .await
            .expect("build")
            .into_payload();
        } else {
            payload.next_inode_id = InodeId(payload.next_inode_id.0 + 1);
        }
        let second = encode_manifest(payload).expect("candidate");
        store.reset();
        let blocked = BlockingStore::new(
            store.clone(),
            KeyPredicate::manifest(&namespace_id),
            OperationClass::Put,
        );
        blocked.block_next();
        let losing = publish_manifest(&blocked, &namespace_id, &second, Some(predecessor));
        let winning = async {
            blocked.wait_until_blocked().await;
            let result = publish_manifest(&store, &namespace_id, &first, Some(predecessor)).await;
            blocked.release();
            result
        };
        let (losing, winning) = futures::join!(losing, winning);
        assert!(matches!(
            winning.expect("winner"),
            ManifestPublicationOutcome::Published(_)
        ));
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
            assert!(matches!(
                losing,
                ManifestPublicationOutcome::PredecessorChanged(_)
            ));
            let mut retry = second.into_payload();
            retry.manifest_no = retry.manifest_no.successor().expect("next number");
            assert!(matches!(
                publish_manifest(
                    &store,
                    &namespace_id,
                    &encode_manifest(retry).expect("retry"),
                    Some(first.payload().manifest_no)
                )
                .await
                .expect("publish retry"),
                ManifestPublicationOutcome::Published(_)
            ));
        } else {
            assert!(matches!(
                losing,
                ManifestPublicationOutcome::CoveredByCurrent(_)
            ));
        }
    }
}

#[tokio::test]
async fn a_lagging_hint_probes_forward_and_a_missing_hint_reads_as_absent() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let context = test_context();
    crate::namespace::bootstrap::bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/file", b"data", &context, None)
        .await
        .expect("write");
    flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("flush");
    let expected = load_current_manifest(&store, &namespace_id)
        .await
        .expect("current");
    for manifest_no in [ManifestNo(1), ManifestNo(2)] {
        let bytes = encode_control_state(
            ControlObjectKind::Hint,
            &HintState {
                wal_no: loonfs_api::WalNo(0),
                namespace_id: namespace_id.clone(),
                manifest_no,
            },
        )
        .expect("hint");
        store
            .put_overwrite(&hint(&namespace_id), Bytes::from(bytes))
            .await
            .expect("rewind hint");
        store.reset();
        let actual = load_current_manifest(&store, &namespace_id)
            .await
            .expect("discover");
        assert_eq!(actual.state, expected.state);
        assert_eq!(
            store.counts().gets,
            (expected.state.manifest.manifest_no.0 - manifest_no.0
                + 1
                + u64::from(manifest_no.0 > 0)) as usize
        );
        assert_eq!(store.counts().lists, 0);
        assert_eq!(store.counts().puts, 0);
    }
    store
        .delete(&hint(&namespace_id))
        .await
        .expect("delete hint");
    store.reset();
    let error = load_current_metadata_view(&store, &namespace_id)
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
    crate::namespace::bootstrap::bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/file", b"data", &context, None)
        .await
        .expect("write");
    flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("flush");
    let before = load_current_manifest(&store, &namespace_id)
        .await
        .expect("current");
    store.reset();
    let advanced = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance");
    assert_eq!(store.counts().create_if_absent_puts, 1);
    assert_eq!(store.counts().overwrite_puts, 0);
    assert_eq!(store.counts().compare_and_swaps, 1);
    let after = load_current_manifest(&store, &namespace_id)
        .await
        .expect("current");
    let mut expected = before.envelope.into_payload();
    expected.manifest_no = expected.manifest_no.successor().expect("next");
    expected.retention_floor_seq = expected.head_seq;
    expected.retention_floor_wal_no = expected.last_folded_wal_no;
    assert_eq!(after.envelope.payload(), &expected);
    assert_eq!(advanced.retention_floor_seq, expected.head_seq);
    let (_, writer_floor) =
        crate::namespace::control_snapshot::load_head_and_retention_floor(&store, &namespace_id)
            .await
            .expect("writer floor");
    assert_eq!(writer_floor, expected.head_seq);
    store.reset();
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("already advanced");
    assert_eq!(store.counts().puts, 0);
}

#[tokio::test]
async fn manifest_publication_recovers_an_ambiguous_put_and_tolerates_a_failed_hint_put() {
    for fail_hint in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let context = test_context();
        crate::namespace::bootstrap::bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        let current = load_current_manifest(&store, &namespace_id)
            .await
            .expect("current");
        let mut payload = current.envelope.into_payload();
        payload.manifest_no = payload.manifest_no.successor().expect("next");
        let candidate = encode_manifest(payload).expect("candidate");
        let keys = if fail_hint {
            KeyPredicate::hint(&namespace_id)
        } else {
            KeyPredicate::manifest(&namespace_id)
        };
        let store = FailStore::new(
            store,
            keys,
            OperationClass::Put,
            InjectedError::Transport("lost acknowledgment".to_owned()),
        )
        .apply_then_fail();
        store.fail_next(1);
        assert!(matches!(
            publish_manifest(
                &store,
                &namespace_id,
                &candidate,
                Some(current.state.manifest.manifest_no)
            )
            .await
            .expect("published"),
            ManifestPublicationOutcome::Published(_)
        ));
        assert_eq!(
            load_current_manifest(&store, &namespace_id)
                .await
                .expect("discover")
                .state
                .manifest
                .manifest_no,
            candidate.payload().manifest_no
        );
    }
}

#[tokio::test]
async fn a_checkpoint_losing_manifest_publication_pins_the_winner() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let context = test_context();
    crate::namespace::bootstrap::bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/file", b"data", &context, None)
        .await
        .expect("write");
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::manifest(&namespace_id),
        OperationClass::Put,
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
    let losing = load_checkpoint_record(&store, &namespace_id, &losing.checkpoint_id)
        .await
        .expect("record")
        .expect("present");
    let winning = load_checkpoint_record(&store, &namespace_id, &winning.checkpoint_id)
        .await
        .expect("record")
        .expect("present");
    assert_eq!(losing.state.manifest(), winning.state.manifest());
}

#[derive(Debug)]
struct StaleObjectOnceStore {
    inner: LocalFsStore,
    key: String,
    stale: std::sync::Mutex<Option<ObjectBody>>,
}

#[async_trait]
impl ObjectStore for StaleObjectOnceStore {
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

#[tokio::test]
async fn read_anchor_reloads_the_head_when_the_root_is_ahead() {
    // A reader that loads a stale head next to a fresher root must reload
    // the head instead of treating the pair as corruption.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    crate::namespace::bootstrap::bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stale_head = inner
        .get_with_metadata(&hint(&namespace_id))
        .await
        .expect("read bootstrap head")
        .expect("bootstrap head exists");

    write_file_bytes(
        &inner,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let store = StaleObjectOnceStore {
        inner,
        key: hint(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_head)),
    };
    let projection = load_current_projection(&store, &namespace_id)
        .await
        .expect("read anchor resolves the stale-head race by reloading");
    assert_eq!(projection.head.seq, ChangeSeq(1));
    assert_eq!(projection.root.manifest.manifest_head_seq, ChangeSeq(1));
}

#[tokio::test]
async fn namespace_status_and_change_feed_reload_a_head_behind_the_floor() {
    // Retention publishes the floor independently of the head. A reader that
    // straddles those writes must not report a floor newer than its head.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    crate::namespace::bootstrap::bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stale_head = inner
        .get_with_metadata(&hint(&namespace_id))
        .await
        .expect("read bootstrap head")
        .expect("bootstrap head exists");

    write_file_bytes(
        &inner,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    advance_retention_floor(&inner, &namespace_id, &context)
        .await
        .expect("advance retention");

    let status_store = StaleObjectOnceStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("status store"),
        key: hint(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_head.clone())),
    };
    let namespace = crate::namespace::status::load_namespace(&status_store, &namespace_id)
        .await
        .expect("status reloads the stale head");
    assert_eq!(namespace.head_seq, ChangeSeq(1));
    assert_eq!(namespace.retention_floor_seq, ChangeSeq(1));

    let feed_store = StaleObjectOnceStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("change-feed store"),
        key: hint(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_head)),
    };
    let changes = list_changes_after(
        &feed_store,
        &namespace_id,
        ChangeSeq(1),
        EffectiveLimit::new(NonZeroU32::new(10).expect("nonzero")),
    )
    .await
    .expect("change feed reloads the stale head");
    assert_eq!(changes.through_seq, ChangeSeq(1));
    assert!(changes.changes.is_empty());
}
