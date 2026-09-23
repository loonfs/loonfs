//! Inline publication, retry identity, admission, and flush contracts.

use super::*;
use crate::checkpoint::{flush_wal, fold_wal_tail};
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::path::read::load_current_metadata_view;
use crate::storage::content::store_bytes_as_content;
use bytes::Bytes;
use loonfs_api::v0::PathEntryKind;
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, WalDelta};
use loonfs_api::{
    AbsolutePath, AttributeInclusion, ContentRef, DestinationBehavior, FlushWalOutcome, WriterId,
};
use loonfs_objectstore::keys::wal_segment_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};

async fn setup() -> (
    tempfile::TempDir,
    Arc<RecordingStore<LocalFsStore>>,
    NamespaceCommitEngine,
    MutationContext,
) {
    let directory = tempfile::tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let namespace_id = NamespaceId::parse("inline").expect("namespace");
    let context = MutationContext {
        writer_id: WriterId::parse("writer").expect("writer"),
        now_ms: 1_000,
    };
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    let engine = NamespaceCommitEngine::new(namespace_id);
    engine
        .session_writer_epoch(&store, &context)
        .await
        .expect("acquire writer");
    store.reset();
    (directory, store, engine, context)
}

fn inline(namespace_id: &NamespaceId, bytes: Bytes) -> InlineContent {
    InlineContent::new(namespace_id.clone(), ContentId::generate(), bytes)
}

fn put(path: &str, content_ref: &ContentRef) -> FilesystemOperation {
    FilesystemOperation::PutFile {
        path: AbsolutePath::parse(path).expect("path"),
        content_ref: Some(content_ref.clone()),
        inline_content: None,
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

fn candidate(commit_id: &str, values: Vec<InlineContent>) -> CommitCandidate {
    CommitCandidate::with_inline_content(
        CommitRequest {
            commit_id: CommitId::parse(commit_id).expect("commit"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            preconditions: Vec::new(),
            operations: values
                .iter()
                .enumerate()
                .map(|(index, value)| put(&format!("/{commit_id}-{index}"), value.content_ref()))
                .collect(),
        },
        Vec::new(),
        values,
    )
}

async fn publish(
    engine: &mut NamespaceCommitEngine,
    store: &RecordingStore<LocalFsStore>,
    context: &MutationContext,
    candidate: CommitCandidate,
) -> Result<Commit> {
    engine
        .publish_batch(store, [candidate], context, &PublishTailOptions::default())
        .await
        .results
        .pop()
        .expect("result")
}

fn assert_no_writes(store: &RecordingStore<LocalFsStore>) {
    let counts = store.counts();
    assert_eq!(counts.puts, 0);
    assert_eq!(counts.compare_and_swaps, 0);
    assert_eq!(counts.deletes, 0);
}

#[tokio::test]
async fn inline_publication_writes_only_wal_and_replays_metadata_in_entry_order() {
    let (_directory, store, mut engine, context) = setup().await;
    let values = vec![
        inline(&engine.namespace_id, Bytes::from_static(b"first")),
        inline(&engine.namespace_id, Bytes::from_static(b"second")),
        inline(&engine.namespace_id, Bytes::new()),
    ];
    let mut candidate = candidate("publish", values.clone());
    candidate.inline_content.reverse();
    publish(&mut engine, &store, &context, candidate.clone())
        .await
        .expect("publish");
    assert_eq!(store.counts().puts, 1);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert_eq!(store.counts().deletes, 0);
    let key = store
        .snapshot()
        .into_iter()
        .find_map(|operation| match operation {
            RecordedOperation::Put { key, .. } => Some(key),
            _ => None,
        })
        .expect("WAL put");
    assert!(key.starts_with(&wal_segment_prefix(&engine.namespace_id)));
    let bytes = store
        .get(&key, None)
        .await
        .expect("get WAL")
        .expect("WAL exists");
    let segment = decode_wal_segment_envelope_zstd(&bytes).expect("decode WAL");
    let record = &segment.payload().records[0];
    assert_eq!(record.inline_content.len(), values.len());
    for (entry, value) in record.inline_content.iter().zip(values.iter().rev()) {
        assert_eq!(entry.content_id, value.content_ref().content_id);
        assert_eq!(entry.bytes.as_slice(), value.bytes().as_ref());
        let expected_reference = value.content_ref();
        assert!(record.deltas.iter().any(|delta| {
            matches!(
                &delta.delta,
                WalDelta::AppendFileRevision { content_ref, .. }
                    if content_ref == expected_reference
            )
        }));
    }
    let reader = load_current_metadata_view(&store, &engine.namespace_id)
        .await
        .expect("fresh reader");
    for (index, value) in values.iter().enumerate() {
        let entry = reader
            .resolve_path(
                &format!("/publish-{index}"),
                AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect("stat");
        assert!(
            matches!(entry.kind, PathEntryKind::File { content_ref, .. } if content_ref == *value.content_ref())
        );
    }
}

#[tokio::test]
async fn inline_retry_identity_uses_bytes_and_distinguishes_staged_content() {
    let (_directory, store, mut engine, context) = setup().await;
    let original = inline(&engine.namespace_id, Bytes::from_static(b"hello"));
    let first = candidate("retry", vec![original.clone()]);
    let committed = publish(&mut engine, &store, &context, first.clone())
        .await
        .expect("publish");
    let retry = candidate(
        "retry",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"hello"))],
    );
    assert_ne!(
        first.inline_content[0].content_ref().content_id,
        retry.inline_content[0].content_ref().content_id
    );
    store.reset();
    assert_eq!(
        publish(&mut engine, &store, &context, retry.clone())
            .await
            .expect("replay"),
        committed
    );
    let mut invalid_retry = retry;
    invalid_retry
        .inline_content
        .push(invalid_retry.inline_content[0].clone());
    assert_eq!(
        publish(&mut engine, &store, &context, invalid_retry)
            .await
            .expect("receipt precedes content validation"),
        committed
    );
    let changed = candidate(
        "retry",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"other"))],
    );
    assert!(matches!(
        publish(&mut engine, &store, &context, changed).await,
        Err(CoreError::CommitIdReuseConflict { .. })
    ));
    assert_no_writes(&store);

    let staged = store_bytes_as_content(&store, &engine.namespace_id, b"hello")
        .await
        .expect("stage");
    let proof = PreparedContent::for_durable_content_write(
        engine.namespace_id.clone(),
        staged.content_ref().clone(),
    );
    let inline = InlineContent::new(
        engine.namespace_id.clone(),
        staged.content_ref().content_id.clone(),
        Bytes::from_static(b"hello"),
    );
    let inline_candidate = candidate("forms", vec![inline]);
    let staged_candidate = CommitCandidate::prepared(inline_candidate.request.clone(), vec![proof]);
    publish(&mut engine, &store, &context, inline_candidate)
        .await
        .expect("inline publish");
    store.reset();
    assert!(matches!(
        publish(&mut engine, &store, &context, staged_candidate).await,
        Err(CoreError::CommitIdReuseConflict { .. })
    ));
    assert_no_writes(&store);
}

#[tokio::test]
async fn invalid_inline_candidates_write_nothing() {
    let (_directory, store, mut engine, context) = setup().await;
    let value = inline(&engine.namespace_id, Bytes::from_static(b"value"));
    let mut unreferenced = candidate("unreferenced", vec![value.clone()]);
    unreferenced
        .inline_content
        .push(inline(&engine.namespace_id, Bytes::from_static(b"extra")));
    let duplicate = candidate("duplicate", vec![value.clone(), value]);
    let foreign = candidate(
        "foreign",
        vec![inline(
            &NamespaceId::parse("other").expect("namespace"),
            Bytes::from_static(b"value"),
        )],
    );
    let oversized = candidate(
        "oversized",
        vec![inline(
            &engine.namespace_id,
            Bytes::from(vec![0; MAX_WAL_INLINE_CONTENT_BYTES + 1]),
        )],
    );
    let excessive_total = candidate(
        "total",
        (0..=MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES / MAX_WAL_INLINE_CONTENT_BYTES)
            .map(|_| {
                inline(
                    &engine.namespace_id,
                    Bytes::from(vec![0; MAX_WAL_INLINE_CONTENT_BYTES]),
                )
            })
            .collect(),
    );
    let staged = store_bytes_as_content(&store, &engine.namespace_id, b"staged")
        .await
        .expect("stage");
    let different = InlineContent::new(
        engine.namespace_id.clone(),
        staged.content_ref().content_id.clone(),
        Bytes::from_static(b"inline bytes"),
    );
    let mut mismatched = candidate("mismatched", vec![different]);
    mismatched
        .request
        .operations
        .push(put("/staged", staged.content_ref()));
    mismatched.content =
        ContentPreparation::Ready(vec![PreparedContent::for_durable_content_write(
            engine.namespace_id.clone(),
            staged.content_ref().clone(),
        )]);
    let mut wrong_checksum = mismatched.clone();
    if let FilesystemOperation::PutFile {
        content_ref: Some(content_ref),
        ..
    } = &mut wrong_checksum.request.operations[0]
    {
        content_ref.checksum = loonfs_api::Checksum::crc32c(b"inline bytes");
    }
    for candidate in [
        unreferenced,
        duplicate,
        foreign,
        oversized,
        excessive_total,
        mismatched,
        wrong_checksum,
    ] {
        store.reset();
        assert!(matches!(
            publish(&mut engine, &store, &context, candidate).await,
            Err(CoreError::InvalidCommitRequest(_))
        ));
        assert_no_writes(&store);
    }
}

#[tokio::test]
async fn inline_tail_replay_matches_publication_and_materializes_before_metadata() {
    let (_directory, store, mut engine, context) = setup().await;
    let directory = CommitCandidate::new(CommitRequest::single(
        CommitId::parse("directory").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/directory").expect("path"),
            parents: false,
        },
    ));
    publish(&mut engine, &store, &context, directory)
        .await
        .expect("directory");
    assert_eq!(
        flush_wal(&store, &engine.namespace_id)
            .await
            .expect("non-inline flush")
            .outcome,
        FlushWalOutcome::Published
    );
    engine.invalidate_projection();
    let mut last = None;
    let mut values = Vec::new();
    for (name, bytes) in [
        ("first", Bytes::from_static(b"first")),
        ("second", Bytes::from_static(b"second")),
        ("empty", Bytes::new()),
    ] {
        let value = inline(&engine.namespace_id, bytes);
        values.push(value.clone());
        let candidate = candidate(name, vec![value]);
        publish(&mut engine, &store, &context, candidate.clone())
            .await
            .expect("publish");
        last = Some(candidate);
    }
    let advanced = engine.wal_fold_input().expect("projection");
    assert_eq!(advanced.tail_state.inline_bytes(), 11);
    let wal_before = store
        .list_prefix(&wal_segment_prefix(&engine.namespace_id))
        .await
        .expect("list WAL");
    engine.invalidate_projection();
    store.reset();
    publish(&mut engine, &store, &context, last.expect("last candidate"))
        .await
        .expect("replay after reload");
    let reloaded = engine.wal_fold_input().expect("reloaded projection");
    assert_eq!(reloaded.tail_state, advanced.tail_state);
    assert_no_writes(&store);
    let folded_wal_no = advanced.head.wal_no;
    for (index, input) in [Some(advanced), Some(reloaded), None]
        .into_iter()
        .enumerate()
    {
        store.reset();
        let flushed = fold_wal_tail(
            &store,
            None,
            &engine.namespace_id,
            input,
            &StdMonotonicTimer::default(),
        )
        .await
        .expect("flush inline content");
        if index == 0 {
            assert_eq!(flushed.outcome, FlushWalOutcome::Published);
            fold_tests::assert_content_before_metadata(&store, values.len());
            for value in &values {
                let key = crate::storage::content::content_object_key_for_ref(value.content_ref())
                    .expect("content key");
                assert_eq!(
                    store
                        .get(&key, None)
                        .await
                        .expect("get content")
                        .expect("content"),
                    value.bytes().as_ref()
                );
            }
        } else {
            assert_eq!(flushed.outcome, FlushWalOutcome::AlreadyCurrent);
            assert_no_writes(&store);
        }
        let manifest =
            crate::namespace::control::load_current_manifest(&store, &engine.namespace_id)
                .await
                .expect("manifest");
        assert_eq!(manifest.envelope.payload().folded_wal_no, folded_wal_no);
        assert_eq!(
            store
                .list_prefix(&wal_segment_prefix(&engine.namespace_id))
                .await
                .expect("WAL remains"),
            wal_before
        );
    }
}

#[path = "commit_engine_inline_read_tests.rs"]
mod read_tests;

#[path = "commit_engine_inline_fold_tests.rs"]
mod fold_tests;
