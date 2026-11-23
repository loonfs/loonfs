//! A panic in one commit's preparation, which the core contains to that
//! commit and the publisher records.

use super::*;
use loonfs_core::publish::InlineContent;
use loonfs_test_support::stores::PanicStore;

fn request(commit_id: &str, operations: Vec<FilesystemOperation>) -> CommitIntent {
    CommitIntent {
        commit_id: CommitId::parse(commit_id).expect("commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations,
    }
}

fn path(path: &str) -> AbsolutePath {
    AbsolutePath::parse(path).expect("path")
}

fn append(at: &str, bytes: &[u8]) -> FilesystemOperation {
    FilesystemOperation::AppendFile {
        path: path(at),
        inline_content: bytes.to_vec(),
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

/// Wraps `inner` in a store that panics on a content object's assembly
/// while the test arms it.
fn panicking_store<S>(inner: S) -> PanicStore<S> {
    PanicStore::panicking_on(inner, |operation| {
        matches!(operation.kind(), OperationKind::Assemble { .. })
    })
}

/// The commits that put `/a`, whose bytes stay in the WAL tail, and copy it
/// to `/b` with one more byte appended to the chain they share. An append
/// to `/a` then starts a new chain from `/a`'s revision, and its
/// preparation writes `/a`'s bytes from the tail to a content object.
fn setup_candidates() -> Vec<CommitCandidate> {
    let value = InlineContent::new(
        loonfs_types::ContentId::generate(),
        Bytes::from_static(b"hello"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let put = FilesystemOperation::PutFile {
        path: path("/a"),
        content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    };
    let copy = FilesystemOperation::CopyPath {
        source_path: path("/a"),
        destination_path: path("/b"),
        precondition: Default::default(),
    };
    vec![
        CommitCandidate::with_inline_content(request("put", vec![put]), Vec::new(), vec![value]),
        CommitCandidate::new(request("copy", vec![copy])),
        CommitCandidate::new(request("grow", vec![append("/b", b"!")])),
    ]
}

/// The commit whose preparation panics while the store is armed.
fn panicking() -> CommitCandidate {
    CommitCandidate::new(request("panics", vec![append("/a", b"?")]))
}

/// Checks that `error` is the error of a commit whose preparation panicked,
/// and that the text that the caller sees includes none of the panic's
/// message.
fn assert_preparation_panicked(error: &Error) {
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert_matches!(
        error,
        Error::Core(CoreError::CommitPreparationPanicked(panic))
            if panic.starts_with("injected panic")
    );
    let fixed = "the server hit a bug while it prepared the commit";
    assert_eq!(error.to_string(), fixed);
    assert_eq!(error.public_message(), fixed);
    assert_eq!(error.to_error_response().message, fixed);
}

/// Checks that a commit whose preparation panics returns `server_error` to
/// its caller after one preparation, that the writer records the panic in
/// `loonfs.publisher.panics` and in the count that a drain reports, and
/// that the namespace keeps committing.
#[tokio::test]
async fn a_panic_in_a_commits_preparation_fails_it_and_is_recorded() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(panicking_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
    ));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = test_writer_with_cache(
        store.clone() as SharedStore,
        MetadataCache::default(),
        recorder.clone(),
    )
    .await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for candidate in setup_candidates() {
        namespace
            .commit_candidate(candidate)
            .await
            .expect("setup commit");
    }

    store.arm();
    let error = namespace
        .commit_candidate(panicking())
        .await
        .expect_err("the preparation panicked");
    assert_preparation_panicked(&error);
    assert_eq!(
        store.attempts(),
        1,
        "the publisher doesn't prepare it again"
    );
    assert_eq!(counter(&recorder, "loonfs.publisher.panics"), 1);

    store.disarm();
    namespace
        .commit_candidate(panicking())
        .await
        .expect("the same commit once the store stops panicking");
    let drain_error = writer
        .drain()
        .await
        .expect_err("drain reports the contained panic");
    assert!(
        drain_error
            .to_string()
            .contains("1 publisher task(s) panicked"),
        "{drain_error}"
    );
}

/// Checks that a commit whose preparation panics gets `server_error` after
/// one preparation, whether it comes before or after a commit that the
/// batch accepted, and whether the batch's WAL put is then refused or has
/// an unknown outcome. The put's failure doesn't change the panicking
/// commit's result, the publisher doesn't prepare it again, and
/// `loonfs.publisher.panics` records the panic once. The neighbor commits
/// when the publisher publishes the batch again.
#[tokio::test]
async fn a_failed_put_after_a_panic_in_preparation_publishes_the_batch_again() {
    let refused = || InjectedError::PreconditionFailed;
    let lost = || InjectedError::Transport("lost WAL put".to_owned());
    for (panicking_first, injected) in [
        (true, refused()),
        (true, lost()),
        (false, refused()),
        (false, lost()),
    ] {
        let case = format!("panicking first: {panicking_first}, put error: {injected:?}");
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let namespace_prefix = loonfs_objectstore::keys::namespace_prefix(&namespace_id);
        let store = Arc::new(panicking_store(FailStore::matching(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            move |operation| {
                operation.key().starts_with(&namespace_prefix)
                    && matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent } if is_publication(bytes))
            },
            injected,
        )));
        let runtime = test_runtime(store.clone());
        create_namespace(&runtime, &namespace_id).await;
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let publisher = standalone_publisher_with(
            &namespace_id,
            &runtime,
            TestCore {
                metrics_recorder: Some(recorder.clone()),
                ..TestCore::unpaced()
            },
        );
        for candidate in setup_candidates() {
            let label = candidate.commit_id().to_string();
            let receiver = try_admit_candidate(&publisher, &namespace_id, candidate)
                .expect("admit setup commit");
            recv_commit(receiver, &label).await;
        }

        store.arm();
        store.inner().fail_next(1);
        let admit_panicking = || {
            try_admit_candidate(&publisher, &namespace_id, panicking())
                .expect("admit the panicking commit")
        };
        let admit_neighbor = || {
            admit_commit(
                &publisher,
                &namespace_id,
                create_directory_request("neighbor", "neighbor"),
            )
        };
        let (failed, neighbor) = if panicking_first {
            let failed = admit_panicking();
            (failed, admit_neighbor())
        } else {
            let neighbor = admit_neighbor();
            (admit_panicking(), neighbor)
        };
        let error = failed
            .await
            .expect("panicking receiver")
            .expect_err("the panicking commit fails");
        assert_preparation_panicked(&error);
        recv_commit(neighbor, "neighbor").await;
        assert_eq!(store.attempts(), 1, "{case}: prepared once");
        assert_eq!(counter(&recorder, "loonfs.publisher.panics"), 1, "{case}");
        assert_eq!(
            store.inner().attempts(),
            2,
            "{case}: one WAL put in each attempt"
        );
    }
}
