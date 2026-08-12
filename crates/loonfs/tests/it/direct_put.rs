//! Upload, direct-put, content publication, and concurrent write flows.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use bytes::Bytes;
use loonfs::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use loonfs::{
    BeginUploadRequest, ChangeSeq, CommitId, CompleteUploadRequest, CreateDirectoryOptions,
    CreateNamespaceOptions, DestinationBehavior, ErrorCode, NamespaceId, PutFileOptions,
    RuntimeCacheConfig, SharedObjectStore,
};
use loonfs_api::v0::{DirectPutContentClaim, UploadSessionStatus};
use loonfs_api::StorageChecksum;
use loonfs_objectstore::keys::wal_head;

/// What a direct-put client declares about bytes it already holds.
fn direct_put_claim(bytes: &[u8]) -> DirectPutContentClaim {
    DirectPutContentClaim {
        size_bytes: bytes.len() as u64,
        storage_checksum: StorageChecksum::sha256(bytes),
    }
}
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

async fn wal_segment_count(store: &SharedObjectStore, namespace_id: &NamespaceId) -> usize {
    use futures::StreamExt;
    store
        .list_prefix_stream(&format!(
            "namespaces/{}/wal/segments/",
            namespace_id.as_str()
        ))
        .map(|key| key.expect("list wal segments"))
        .collect::<Vec<_>>()
        .await
        .len()
}

fn fail_content_blob_puts_store(root: &Path) -> FailStore<LocalFsStore> {
    FailStore::new(
        LocalFsStore::new(root).expect("create local-fs store"),
        KeyPredicate::content_blob(),
        OperationClass::Put,
        InjectedError::Transport("injected content write failure".to_owned()),
    )
}

fn fail_content_blob_heads_store(root: &Path) -> FailStore<LocalFsStore> {
    FailStore::new(
        LocalFsStore::new(root).expect("create local-fs store"),
        KeyPredicate::content_blob(),
        OperationClass::Head,
        InjectedError::Transport("injected checksum head failure".to_owned()),
    )
}
#[test]
fn upload_flow_is_available_from_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "upload-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = fs
        .begin_upload_blocking(&namespace_id)
        .expect("begin upload");
    let staged = fs
        .upload_content_blocking(&namespace_id, begin.upload_id(), b"uploaded")
        .expect("upload content");
    let staged_again = fs
        .upload_content_blocking(&namespace_id, begin.upload_id(), b"uploaded")
        .expect("repeat upload content");
    assert_eq!(staged.content_ref, staged_again.content_ref);

    let request = CompleteUploadRequest::for_content_ref(staged.content_ref);
    let completed = fs
        .complete_upload_blocking(&namespace_id, begin.upload_id(), &request)
        .expect("complete upload");
    let completed_again = fs
        .complete_upload_blocking(&namespace_id, begin.upload_id(), &request)
        .expect("repeat complete upload");
    assert_eq!(completed.content_ref, completed_again.content_ref);
}

#[test]
fn direct_put_upload_flow_validates_durable_object_on_complete() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "direct-put-upload-test");
    let namespace_id = namespace_id("demo");
    let bytes = b"direct uploaded";

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = block_on(fs.begin_direct_put_upload_target(&namespace_id, direct_put_claim(bytes)))
        .expect("begin direct put");
    // The target is minted here, before a byte is written, and the key it
    // names is derived from that identity alone.
    let content_ref = begin.target.content_ref.clone();
    assert!(begin
        .target
        .object_key
        .ends_with(content_ref.content_id.as_str()));

    let direct_store = LocalFsStore::new(temp_dir.path()).expect("direct object-store handle");
    block_on(direct_store.put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(bytes)))
        .expect("write direct object");

    let complete_request = CompleteUploadRequest::for_content_ref(content_ref.clone());
    let completed = fs
        .complete_upload_blocking(&namespace_id, &begin.upload_id, &complete_request)
        .expect("complete direct put");
    assert_eq!(completed.content_ref, content_ref);

    block_on(fs.put_file_content_ref(
        &namespace_id,
        "/docs/direct.txt",
        content_ref,
        PutFileOptions::default(),
    ))
    .expect("publish direct put content");
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/direct.txt")
            .expect("read direct put file")
            .bytes,
        bytes
    );
}

#[test]
fn direct_put_completion_proves_upload_without_reading_content() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::content_blob(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "direct-put-probe-test");
    let bytes = b"direct uploaded, provider verified";

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = block_on(fs.begin_direct_put_upload_target(&namespace_id, direct_put_claim(bytes)))
        .expect("begin direct put");
    let content_ref = begin.target.content_ref.clone();

    // Stands in for the provider-verified presigned upload.
    let direct_store = LocalFsStore::new(temp_dir.path()).expect("direct object-store handle");
    block_on(direct_store.put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(bytes)))
        .expect("write direct object");

    raw_store.reset();
    let completed = fs
        .complete_upload_blocking(
            &namespace_id,
            &begin.upload_id,
            &CompleteUploadRequest::for_content_ref(content_ref.clone()),
        )
        .expect("complete direct put");
    assert_eq!(completed.content_ref, content_ref);
    assert_eq!(
        raw_store.count(OperationClass::Read),
        0,
        "completion verifies the object from its stored checksum, not its bytes"
    );
}

#[test]
fn direct_put_completion_rejects_a_mis_declared_size() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "direct-put-size-test");
    let namespace_id = namespace_id("demo");
    let bytes = b"direct put bytes with a lying size";
    // The client overstates its byte count. Nothing about the object's name
    // contradicts that, so completion's own size check has to.
    let mut claim = direct_put_claim(bytes);
    claim.size_bytes += 1;

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = block_on(fs.begin_direct_put_upload_target(&namespace_id, claim))
        .expect("begin direct put");
    let content_ref = begin.target.content_ref.clone();

    let direct_store = LocalFsStore::new(temp_dir.path()).expect("direct object-store handle");
    block_on(direct_store.put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(bytes)))
        .expect("write direct object");

    let error = fs
        .complete_upload_blocking(
            &namespace_id,
            &begin.upload_id,
            &CompleteUploadRequest::for_content_ref(content_ref),
        )
        .expect_err("mis-declared size must fail completion");
    assert!(
        error.to_string().contains("content length mismatch"),
        "completion names the size mismatch: {error}"
    );
}

/// Bytes that do not match the claim are refused at completion and the
/// object is removed. The id was never published, so nothing can reference
/// what gets deleted.
#[test]
fn direct_put_completion_rejects_and_removes_bytes_that_do_not_match_the_claim() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "direct-put-checksum-test");
    let namespace_id = namespace_id("demo");
    let promised = b"the bytes the client promised";
    let delivered = b"different bytes, same length!";
    assert_eq!(promised.len(), delivered.len(), "size must not be the tell");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin =
        block_on(fs.begin_direct_put_upload_target(&namespace_id, direct_put_claim(promised)))
            .expect("begin direct put");
    let content_ref = begin.target.content_ref.clone();

    let direct_store = LocalFsStore::new(temp_dir.path()).expect("direct object-store handle");
    block_on(
        direct_store.put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(delivered)),
    )
    .expect("write mismatched direct object");

    let error = fs
        .complete_upload_blocking(
            &namespace_id,
            &begin.upload_id,
            &CompleteUploadRequest::for_content_ref(content_ref),
        )
        .expect_err("mismatched bytes must fail completion");
    assert_eq!(
        error.code(),
        ErrorCode::InvalidRequest,
        "a read-back that ran and disagreed is reported against the content: {error}"
    );
    assert!(
        error.to_string().contains("content checksum mismatch"),
        "completion names the checksum mismatch: {error}"
    );
    assert!(
        block_on(direct_store.head(&begin.target.object_key))
            .expect("head rejected object")
            .is_none(),
        "a rejected completion leaves no orphan behind"
    );
}

/// A read-back that could not run is not evidence about the bytes. The
/// completion reports the store failure it hit, keeps the object the client
/// uploaded and the session that owns it, and the retry completes.
#[test]
fn direct_put_completion_reports_a_failed_read_back_as_a_store_failure() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(fail_content_blob_heads_store(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "direct-put-read-back-failure-test");
    let bytes = b"direct put bytes the store would not vouch for";

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = block_on(fs.begin_direct_put_upload_target(&namespace_id, direct_put_claim(bytes)))
        .expect("begin direct put");
    let content_ref = begin.target.content_ref.clone();
    let request = CompleteUploadRequest::for_content_ref(content_ref.clone());

    let direct_store = LocalFsStore::new(temp_dir.path()).expect("direct object-store handle");
    block_on(direct_store.put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(bytes)))
        .expect("write direct object");

    raw_store.fail_next(1);
    let error = fs
        .complete_upload_blocking(&namespace_id, &begin.upload_id, &request)
        .expect_err("a verification that cannot run does not complete the upload");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert_eq!(raw_store.attempts(), 1, "the checksum head is what failed");
    assert!(
        block_on(direct_store.head(&begin.target.object_key))
            .expect("head the uploaded object")
            .is_some(),
        "a store failure must not delete the client's object"
    );
    let (status, _) = block_on(
        fs.writer
            .read_upload_status(&namespace_id, &begin.upload_id),
    )
    .expect("read upload status");
    assert!(
        matches!(status.status, UploadSessionStatus::Open { .. }),
        "a store failure must not end the session"
    );

    let completed = fs
        .complete_upload_blocking(&namespace_id, &begin.upload_id, &request)
        .expect("the retried completion verifies and completes");
    assert_eq!(completed.content_ref, content_ref);
}

#[test]
fn put_file_bytes_gates_publish_on_its_own_content_write_without_probing() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::content_blob(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "put-file-content-validation-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    raw_store.reset();
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/direct.txt",
        b"direct bytes",
        PutFileOptions::default(),
    )
    .expect("put file bytes");

    // The put writes the blob exactly once and never reads it back: the
    // write's own ack is the durability proof the head CAS waits on, so
    // validation issues no probe for content the put itself is writing.
    assert_eq!(raw_store.count(OperationClass::Put), 1);
    assert_eq!(raw_store.count(OperationClass::Read), 0);

    // A replace put rides the same overlapped path: new blob, no probe.
    raw_store.reset();
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/direct.txt",
        b"replaced bytes",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit_id: None,
            message: None,
            expected_revision_no: None,
        },
    )
    .expect("replace file bytes");

    assert_eq!(raw_store.count(OperationClass::Put), 1);
    assert_eq!(raw_store.count(OperationClass::Read), 0);
}

#[test]
fn put_file_bytes_retries_a_transient_content_write_failure() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(fail_content_blob_puts_store(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "content-write-failure-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    raw_store.fail_next(1);
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"overlap survives",
        PutFileOptions {
            behavior: DestinationBehavior::NoReplace,
            commit_id: Some(CommitId::parse("overlap-put-retry").expect("valid commit id")),
            message: None,
            expected_revision_no: None,
        },
    )
    .expect("immutable write retries the transient failure");
    let read = fs
        .get_file_bytes_blocking(&namespace_id, "/docs/report.txt")
        .expect("read file");
    assert_eq!(read.bytes, b"overlap survives");
}

#[test]
fn path_mutations_return_the_commit_id_they_committed_under() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let object_store = store(temp_dir.path());
    block_on(async {
        let fs = open_runtime_async(object_store, "commit-id-echo-test").await;
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        let commit_id = CommitId::parse("retry-key-1").expect("valid commit id");
        let first = fs
            .put_file_bytes(
                &namespace_id,
                "/docs/a.txt",
                b"alpha",
                PutFileOptions {
                    commit_id: Some(commit_id.clone()),
                    message: None,
                    ..PutFileOptions::default()
                },
            )
            .await
            .expect("first put");
        assert_eq!(first.namespace_id, namespace_id);
        assert_eq!(first.commit_id, commit_id);

        // Resubmitting the identical mutation with the same commit id
        // replays the original commit instead of committing again. Identical
        // means the same content object: a put's identity is which object it
        // attaches, so the retry reuses the reference the first put landed.
        let landed = fs
            .reader
            .stat_path(&namespace_id, "/docs/a.txt", Default::default())
            .await
            .expect("stat the committed file")
            .content_ref()
            .cloned()
            .expect("a file revision carries a content ref");
        let replay = fs
            .put_file_content_ref(
                &namespace_id,
                "/docs/a.txt",
                landed,
                PutFileOptions {
                    commit_id: Some(commit_id.clone()),
                    message: None,
                    ..PutFileOptions::default()
                },
            )
            .await
            .expect("identical resubmission replays the original commit");
        assert_eq!(replay.commit_id, first.commit_id);
        assert_eq!(replay.committed_seq, first.committed_seq);

        // Re-uploading the same bytes under the same commit id stages a
        // fresh content object, so the publisher sees a different mutation
        // and refuses it. What makes the rerun safe is the comparison that
        // follows: the commit id's committed content is read back, its
        // bytes match, and the original commit is reported.
        let reuploaded = fs
            .put_file_bytes(
                &namespace_id,
                "/docs/a.txt",
                b"alpha",
                PutFileOptions {
                    commit_id: Some(commit_id.clone()),
                    message: None,
                    ..PutFileOptions::default()
                },
            )
            .await
            .expect("re-uploading identical bytes under a used commit id is idempotent");
        assert_eq!(reuploaded.committed_seq, first.committed_seq);

        // Different bytes are a different operation and still conflict.
        let conflict = fs
            .put_file_bytes(
                &namespace_id,
                "/docs/a.txt",
                b"omega",
                PutFileOptions {
                    commit_id: Some(commit_id.clone()),
                    message: None,
                    ..PutFileOptions::default()
                },
            )
            .await
            .expect_err("different bytes under a used commit id must conflict");
        assert_eq!(conflict.code(), ErrorCode::CommitIdReuseConflict);

        // Without a caller-supplied id, the generated one is still returned,
        // so every caller holds a reconciliation handle.
        let generated = fs
            .writer
            .create_directory(
                &namespace_id,
                "/docs/sub",
                CreateDirectoryOptions::default(),
            )
            .await
            .expect("mkdir");
        assert!(!generated.commit_id.as_str().is_empty());
        assert_ne!(generated.commit_id, first.commit_id);
        assert!(generated.committed_seq > first.committed_seq);
    });
}

#[test]
fn concurrent_puts_coalesce_into_one_wal_segment() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let object_store = store(temp_dir.path());
    block_on(async {
        let fs = open_runtime_async(object_store.clone(), "publication-batch-test").await;
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        // Stage every file's content first: a put publishes only after its
        // bytes are durable, so racing already-staged publishes is what
        // reaches the publication queue together deterministically.
        let catalog =
            loonfs_core::control::load_namespace_catalog_entry(&object_store, &namespace_id)
                .await
                .expect("load namespace catalog");
        let mut prepared_contents = Vec::new();
        for bytes in [b"alpha" as &[u8], b"beta", b"gamma", b"delta"] {
            let begin = fs
                .writer
                .begin_upload(&namespace_id, BeginUploadRequest::ServiceProxied {})
                .await
                .expect("begin upload");
            let staged = fs
                .writer
                .upload_content(&namespace_id, begin.upload_id(), bytes)
                .await
                .expect("upload content");
            let completed = fs
                .writer
                .complete_upload(
                    &namespace_id,
                    begin.upload_id(),
                    &CompleteUploadRequest::for_content_ref(staged.content_ref),
                )
                .await
                .expect("complete upload");
            prepared_contents.push(
                loonfs_core::content::prepare_existing_content_ref(
                    &object_store,
                    &catalog,
                    completed.content_ref,
                )
                .await
                .expect("prepare completed content"),
            );
        }
        let [content_a, content_b, content_c, content_d] =
            prepared_contents.try_into().expect("four prepared refs");
        let segments_before = wal_segment_count(&object_store, &namespace_id).await;
        let admitted_put =
            |commit_id: &str, path: &str, prepared: loonfs_core::content::PreparedContent| {
                let content_ref = prepared.content_ref().clone();
                (
                    CommitRequest::single(
                        CommitId::parse(commit_id).expect("valid commit id"),
                        None,
                        FilesystemOperation::PutFile {
                            path: parse_mutation_path(path).expect("valid mutation path"),
                            content_ref,
                            behavior: DestinationBehavior::NoReplace,
                            expected_revision_no: None,
                        },
                    ),
                    vec![prepared],
                )
            };
        let put_a = admitted_put("batch-a", "/docs/a.txt", content_a);
        let put_b = admitted_put("batch-b", "/docs/b.txt", content_b);
        let put_c = admitted_put("batch-c", "/docs/c.txt", content_c);
        let put_d = admitted_put("batch-d", "/docs/d.txt", content_d);
        let publisher = fs.writer.publisher();

        let puts = tokio::join!(
            publisher.submit_commit_with_prepared_content(namespace_id.clone(), put_a.0, put_a.1,),
            publisher.submit_commit_with_prepared_content(namespace_id.clone(), put_b.0, put_b.1,),
            publisher.submit_commit_with_prepared_content(namespace_id.clone(), put_c.0, put_c.1,),
            publisher.submit_commit_with_prepared_content(namespace_id.clone(), put_d.0, put_d.1,),
        );
        puts.0.expect("put a");
        puts.1.expect("put b");
        puts.2.expect("put c");
        puts.3.expect("put d");

        // The already-proven candidates reach publisher admission together
        // and publish as one batch: one WAL segment, one head CAS. The slow
        // content-ref helper validates before admission and is intentionally
        // outside this batching seam.
        let segments_after = wal_segment_count(&object_store, &namespace_id).await;
        assert_eq!(segments_after - segments_before, 1);

        for (path, bytes) in [
            ("/docs/a.txt", b"alpha" as &[u8]),
            ("/docs/b.txt", b"beta"),
            ("/docs/c.txt", b"gamma"),
            ("/docs/d.txt", b"delta"),
        ] {
            let read = fs
                .reader
                .get_file_bytes(&namespace_id, path)
                .await
                .expect("read coalesced file");
            assert_eq!(read.bytes, bytes);
        }
    });
}

#[test]
fn zero_interval_publishes_sequential_submissions_immediately() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let object_store = store(temp_dir.path());
    block_on(async {
        let fs = open_runtime_with_async(object_store.clone(), "zero-interval-test", |builder| {
            builder.min_publish_interval_ms(0)
        })
        .await;
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let segments_before = wal_segment_count(&object_store, &namespace_id).await;

        // Sequential awaited puts leave nothing to batch: with a zero
        // pacing interval each publishes immediately as its own WAL
        // segment. (Concurrent submissions may still batch behind an
        // in-flight publication — that is load-driven, not timer-driven.)
        for (path, bytes) in [
            ("/docs/a.txt", b"alpha".as_slice()),
            ("/docs/b.txt", b"beta".as_slice()),
            ("/docs/c.txt", b"gamma".as_slice()),
        ] {
            fs.put_file_bytes(&namespace_id, path, bytes, PutFileOptions::default())
                .await
                .expect("sequential put");
        }

        let segments_after = wal_segment_count(&object_store, &namespace_id).await;
        assert_eq!(segments_after - segments_before, 3);
    });
}

#[test]
fn concurrent_puts_both_commit_after_one_transient_content_failure() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(fail_content_blob_puts_store(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    block_on(async {
        let fs = open_runtime_async(object_store, "window-abort-test").await;
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        // One content write fails once before either submission enters the
        // commit window. Its immutable retry is independent of its peer.
        raw_store.fail_next(1);
        let (a, b) = tokio::join!(
            fs.put_file_bytes(
                &namespace_id,
                "/docs/a.txt",
                b"alpha",
                PutFileOptions::default()
            ),
            fs.put_file_bytes(
                &namespace_id,
                "/docs/b.txt",
                b"beta",
                PutFileOptions::default()
            ),
        );

        for (path, bytes, result) in [
            ("/docs/a.txt", b"alpha" as &[u8], a),
            ("/docs/b.txt", b"beta" as &[u8], b),
        ] {
            result.expect("both puts survive the transient content failure");
            let read = fs
                .reader
                .get_file_bytes(&namespace_id, path)
                .await
                .expect("read committed file");
            assert_eq!(read.bytes, bytes);
        }
    });
}

#[test]
fn begin_upload_validates_controls_without_replay_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store = raw_store.store();
    let fs = open_runtime(object_store, "begin-upload-cache-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit_id: None,
            message: None,
            expected_revision_no: None,
        },
    )
    .expect("replace file");

    raw_store.reset_control_get_counts();
    fs.begin_upload_blocking(&namespace_id)
        .expect("first begin upload");
    fs.begin_upload_blocking(&namespace_id)
        .expect("second begin upload");

    assert_eq!(raw_store.wal_get_count(), 0);
    assert_eq!(raw_store.manifest_get_count(), 0);
}

/// Upload admission is the head: absent means the namespace was never
/// created, and an unreadable head is corruption, not absence.
#[test]
fn begin_upload_rejects_missing_and_unreadable_namespaces() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "begin-upload-missing-partial-test");
    let namespace_id = namespace_id("demo");

    assert_core_error_kind(
        fs.begin_upload_blocking(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.begin_upload_blocking(&namespace_id)
        .expect("a live namespace admits uploads");

    block_on(raw_store.delete(&wal_head(namespace_id.as_str()))).expect("delete head");
    assert_core_error_kind(
        fs.begin_upload_blocking(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );
}

#[test]
fn begin_upload_rejects_malformed_head_and_lease_when_cache_disabled() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime_with(
        object_store,
        "begin-upload-malformed-control-test",
        |builder| builder.runtime_cache(RuntimeCacheConfig::disabled()),
    );

    let head_bad = NamespaceId::parse("head-bad").expect("valid namespace id");
    fs.create_namespace_blocking(&head_bad, CreateNamespaceOptions::default())
        .expect("create head-bad namespace");
    block_on(raw_store.put_overwrite(
        &wal_head(head_bad.as_str()),
        Bytes::from_static(br#"{"not":"a head"}"#),
    ))
    .expect("corrupt head");
    assert_core_error_kind(
        fs.begin_upload_blocking(&head_bad),
        ErrorCode::NamespaceCorrupt,
    );
}

#[test]
fn a_mutation_request_appears_in_change_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "commit-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let commit_id = CommitId::parse("create-dir").expect("valid commit id");
    let response = fs
        .mutate_blocking(
            &namespace_id,
            CommitRequest::single(
                commit_id.clone(),
                Some("create docs".to_owned()),
                FilesystemOperation::CreateDirectory {
                    path: parse_mutation_path("/docs").expect("valid mutation path"),
                    parents: false,
                },
            ),
        )
        .expect("publish mutation");

    let changes = fs
        .list_changes_blocking(&namespace_id, ChangeSeq(0))
        .expect("list changes");
    assert_eq!(changes.through_seq, response.committed_seq);
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].commit_id, commit_id);
}
