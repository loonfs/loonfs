//! Reads of published inline content and projection accounting.

#![allow(clippy::panic)]

use super::*;
use crate::block_cache::DecodedBlock;
use crate::cache::{
    MetadataSegmentCache, WalTailProjectionCache, WalTailProjectionCacheConfig,
    WalTailProjectionCacheKey,
};
use crate::namespace::read_anchor::load_head_and_metadata_basis;
use crate::storage::content::{ContentLocation, DurableContentValidationError};
use crate::{NamespaceEngine, RuntimeReadContext};
use loonfs_api::{DestinationPrecondition, RevisionNo, WalNo};
use loonfs_objectstore::keys::{content_blob, wal_segment};
use loonfs_objectstore::PutMode;
use std::num::NonZeroU64;

fn read_context(head: NamespaceReadState, basis: MetadataBasis) -> RuntimeReadContext {
    RuntimeReadContext {
        head,
        basis,
        segment_cache: Arc::new(MetadataSegmentCache::new(Default::default())),
        tail_cache: Arc::new(WalTailProjectionCache::new(
            WalTailProjectionCacheConfig {
                max_entries: 16,
                max_rows: usize::MAX,
                max_decoded_bytes: usize::MAX,
            },
            None,
        )),
    }
}

async fn fresh_context(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
) -> RuntimeReadContext {
    let loaded = load_head_and_metadata_basis(store, namespace_id)
        .await
        .expect("read basis");
    read_context(loaded.head, loaded.basis)
}

fn cache_key(context: &RuntimeReadContext) -> WalTailProjectionCacheKey {
    WalTailProjectionCacheKey {
        namespace_id: context.head.namespace_id.clone(),
        manifest_no: context.basis.manifest_no(),
        manifest_head_seq: context.basis.manifest().head_seq,
        head_seq: context.head.seq,
    }
}

fn assert_no_content_requests(store: &RecordingStore<LocalFsStore>) {
    assert!(store.snapshot().iter().all(|operation| !matches!(loonfs_objectstore::layout::parse_object_key(operation.key()), Some(key) if key.family() == loonfs_objectstore::layout::DurableObjectFamily::ContentBlob)), "{:?}", store.snapshot());
}

#[tokio::test]
async fn every_read_verifies_tail_content_without_requesting_an_object() {
    let (_directory, store, mut publisher, mutation_context) = setup().await;
    let values = vec![
        inline(&publisher.namespace_id, Bytes::from_static(b"inline bytes")),
        inline(&publisher.namespace_id, Bytes::new()),
    ];
    publish(
        &mut publisher,
        &store,
        &mutation_context,
        candidate("read", values.clone()),
    )
    .await
    .expect("publish");
    let engine = NamespaceEngine::reader(&store, publisher.namespace_id.clone());
    for (index, value) in values.iter().enumerate() {
        let path = format!("/read-{index}");
        let inode_id = load_current_metadata_view(&store, &publisher.namespace_id)
            .await
            .expect("view")
            .resolve_path(
                &path,
                AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect("entry")
            .inode_id;
        for method in [
            "path",
            "revision",
            "inode",
            "speculative",
            "resolved",
            "stream",
            "resumed",
            "inode_stream",
            "reference",
        ] {
            let context = fresh_context(&store, &publisher.namespace_id).await;
            store.reset();
            let bytes = match method {
                "path" => {
                    engine
                        .get_file(&path, &context, None)
                        .await
                        .expect("path read")
                        .bytes
                }
                "revision" => {
                    engine
                        .get_file_revision(&path, RevisionNo(1), &context, None)
                        .await
                        .expect("revision read")
                        .bytes
                }
                "inode" => engine
                    .get_file_revision_for_inode(inode_id, RevisionNo(1), &context, None)
                    .await
                    .expect("inode read"),
                "speculative" | "resolved" => {
                    let target = engine
                        .resolve_file_content(&path, &context, None)
                        .await
                        .expect("resolve");
                    assert!(matches!(target.location, ContentLocation::Tail { .. }));
                    if method == "speculative" {
                        engine
                            .get_speculative_file_content(&target)
                            .await
                            .expect("speculative read")
                    } else {
                        engine
                            .get_resolved_file_content(&target)
                            .await
                            .expect("resolved read")
                    }
                }
                "stream" | "resumed" | "inode_stream" => {
                    let start_offset = if method == "resumed" {
                        value.bytes().len().min(3)
                    } else {
                        0
                    };
                    let mut stream = if method == "inode_stream" {
                        engine
                            .read_file_revision_stream_by_inode(inode_id, RevisionNo(1), &context)
                            .await
                            .expect("inode stream")
                    } else {
                        engine
                            .read_file_stream(
                                &path,
                                &context,
                                Some(RevisionNo(1)),
                                NonZeroU64::new(2).expect("chunk size"),
                                start_offset as u64,
                            )
                            .await
                            .expect("stream")
                    };
                    let mut bytes = value.bytes()[..start_offset].to_vec();
                    if start_offset != 0 {
                        assert!(matches!(
                            stream.next_chunk().await,
                            Err(CoreError::ResumePrefixIncomplete { .. })
                        ));
                        stream.fold_resumed_prefix(&bytes).expect("fold prefix");
                    }
                    while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
                        bytes.extend_from_slice(&chunk);
                    }
                    bytes
                }
                "reference" => engine
                    .read_content_ref(value.content_ref(), u64::MAX, &context)
                    .await
                    .expect("reference read"),
                _ => panic!("unknown read method"),
            };
            assert_eq!(bytes.as_slice(), value.bytes().as_ref(), "{method}");
            assert_no_content_requests(&store);
            store.reset();
            assert_eq!(
                engine
                    .read_content_ref(value.content_ref(), u64::MAX, &context)
                    .await
                    .expect("cached reference"),
                bytes
            );
            assert!(
                store.snapshot().is_empty(),
                "a cached reference needs no metadata request"
            );
        }
    }
}

#[tokio::test]
async fn published_projection_reads_without_replay_and_counts_inline_bytes() {
    let (_directory, store, mut publisher, mutation_context) = setup().await;
    let value = inline(&publisher.namespace_id, Bytes::from_static(b"owned bytes"));
    let result = publisher
        .publish_batch(
            &store,
            [candidate("owned", vec![value.clone()])],
            &mutation_context,
            &PublishTailOptions::default(),
        )
        .await;
    assert!(result.results[0].is_ok());
    let state = result.resulting_read_state.expect("published state");
    let context = read_context(state.head, state.basis);
    let metadata_bytes = state.tail.rows.decoded_bytes();
    assert_eq!(
        state.tail.weight().bytes,
        metadata_bytes + value.bytes().len()
    );
    assert_eq!(
        publisher
            .retained_tail_weight()
            .expect("weight")
            .decoded_bytes,
        metadata_bytes + value.bytes().len()
    );
    let cloned = state.tail.as_ref().clone();
    assert_eq!(
        state
            .tail
            .inline_content(&value.content_ref().content_id)
            .expect("bytes")
            .as_ptr(),
        cloned
            .inline_content(&value.content_ref().content_id)
            .expect("cloned bytes")
            .as_ptr()
    );
    context.tail_cache.insert(cache_key(&context), state.tail);
    assert_eq!(
        context.tail_cache.stats().cached_decoded_bytes,
        metadata_bytes + value.bytes().len()
    );
    let engine = NamespaceEngine::reader(&store, publisher.namespace_id.clone());
    store.reset();
    assert_eq!(
        engine
            .get_file("/owned-0", &context, None)
            .await
            .expect("own read")
            .bytes,
        value.bytes().as_ref()
    );
    assert_eq!(
        engine
            .read_content_ref(value.content_ref(), u64::MAX, &context)
            .await
            .expect("own reference"),
        value.bytes().as_ref()
    );
    assert!(store.snapshot().iter().all(|operation| !operation
        .key()
        .starts_with(&wal_segment_prefix(&publisher.namespace_id))));
    assert_no_content_requests(&store);
}

#[tokio::test]
async fn a_copy_and_an_advanced_reader_keep_earlier_inline_content() {
    let (_directory, store, mut publisher, mutation_context) = setup().await;
    let value = inline(&publisher.namespace_id, Bytes::from_static(b"copied"));
    publish(
        &mut publisher,
        &store,
        &mutation_context,
        candidate("source", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let mut context = fresh_context(&store, &publisher.namespace_id).await;
    let engine = NamespaceEngine::reader(&store, publisher.namespace_id.clone());
    engine
        .get_file("/source-0", &context, None)
        .await
        .expect("cache first tail");
    let copy = CommitCandidate::new(CommitRequest::single(
        CommitId::parse("copy").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/source-0").expect("source"),
            destination_path: AbsolutePath::parse("/copy").expect("destination"),
            precondition: DestinationPrecondition::default(),
        },
    ));
    publish(&mut publisher, &store, &mutation_context, copy)
        .await
        .expect("copy");
    store.reset();
    assert!(crate::wal::probe_namespace_wal(&store, &mut context)
        .await
        .expect("advance"));
    assert_eq!(
        engine
            .get_file("/copy", &context, None)
            .await
            .expect("read copy")
            .bytes,
        value.bytes().as_ref()
    );
    assert_eq!(
        engine
            .get_file("/source-0", &context, None)
            .await
            .expect("read source")
            .bytes,
        value.bytes().as_ref()
    );
    assert_no_content_requests(&store);
    assert_eq!(
        context
            .tail_cache
            .get(&cache_key(&context))
            .expect("advanced tail"),
        publisher.wal_fold_input().expect("publish tail").tail_state
    );
}

#[tokio::test]
async fn foreign_references_resolve_to_objects_and_object_downloads_do_not_write() {
    let (_directory, store, mut publisher, mutation_context) = setup().await;
    let value = inline(&publisher.namespace_id, Bytes::from_static(b"local"));
    publish(
        &mut publisher,
        &store,
        &mutation_context,
        candidate("local", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let context = fresh_context(&store, &publisher.namespace_id).await;
    let engine = NamespaceEngine::reader(&store, publisher.namespace_id.clone());
    let foreign = ContentRef::blob_v1(
        NamespaceId::parse("foreign").expect("owner"),
        loonfs_api::NamespaceGeneration(1),
        value.content_ref().content_id.clone(),
        b"foreign",
    );
    let key = content_blob(
        &foreign.owner_namespace_id,
        foreign.owner_generation,
        &foreign.content_id,
    );
    store
        .put(
            &key,
            Bytes::from_static(b"foreign"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("foreign object");
    store.reset();
    assert_eq!(
        engine
            .read_content_ref(&foreign, u64::MAX, &context)
            .await
            .expect("foreign read"),
        b"foreign"
    );
    assert_eq!(store.snapshot().len(), 1);
    assert_eq!(store.snapshot()[0].key(), key);
    let view = load_current_metadata_view(&store, &publisher.namespace_id)
        .await
        .expect("view");
    assert!(matches!(
        view.resolve_content_location(&foreign)
            .expect("foreign location"),
        ContentLocation::Object { .. }
    ));
    assert_no_writes(&store);
    let stored = store_bytes_as_content(&store, &publisher.namespace_id, b"object")
        .await
        .expect("store object");
    let candidate = CommitCandidate::prepared(
        CommitRequest::single(
            CommitId::parse("object").expect("commit"),
            loonfs_test_support::test_actor(),
            None,
            put("/object", stored.content_ref()),
        ),
        vec![PreparedContent::for_durable_content_write(
            publisher.namespace_id.clone(),
            stored.content_ref().clone(),
        )],
    );
    publish(&mut publisher, &store, &mutation_context, candidate)
        .await
        .expect("publish object");
    let context = fresh_context(&store, &publisher.namespace_id).await;
    let path_target = engine
        .direct_download_target("/object", None, &context)
        .await
        .expect("object download");
    let inode_id = engine
        .resolve_file_content("/object", &context, None)
        .await
        .expect("object entry")
        .entry
        .inode_id;
    let inode_target = engine
        .direct_download_target_by_inode(inode_id, RevisionNo(1), &context)
        .await
        .expect("inode download");
    assert_eq!(path_target.object_key, stored.object_key());
    assert_eq!(inode_target.object_key, stored.object_key());
}

#[tokio::test]
async fn direct_downloads_materialize_once_and_do_not_write_after_a_flush() {
    for fold_first in [false, true] {
        let (_directory, store, mut publisher, mutation_context) = setup().await;
        let value = inline(&publisher.namespace_id, Bytes::from_static(b"download"));
        publish(
            &mut publisher,
            &store,
            &mutation_context,
            candidate("download", vec![value.clone()]),
        )
        .await
        .expect("publish");
        if fold_first {
            flush_wal(&store, &publisher.namespace_id)
                .await
                .expect("flush");
        }
        let context = fresh_context(&store, &publisher.namespace_id).await;
        let engine = NamespaceEngine::reader(&store, publisher.namespace_id.clone());
        let inode_id = engine
            .resolve_file_content("/download-0", &context, None)
            .await
            .expect("resolve")
            .entry
            .inode_id;
        let key = content_blob(
            &publisher.namespace_id,
            value.content_ref().owner_generation,
            &value.content_ref().content_id,
        );
        store.reset();
        for _ in 0..2 {
            let target = engine
                .direct_download_target("/download-0", None, &context)
                .await
                .expect("path download");
            let inode_target = engine
                .direct_download_target_by_inode(inode_id, RevisionNo(1), &context)
                .await
                .expect("inode download");
            assert_eq!(target.object_key, key);
            assert_eq!(inode_target.object_key, key);
            assert_eq!(store.counts().puts, usize::from(!fold_first));
        }
        let writes: Vec<_> = store
            .snapshot()
            .into_iter()
            .filter(|operation| matches!(operation, RecordedOperation::Put { .. }))
            .collect();
        if let Some(write) = writes.first() {
            assert_eq!(write.key(), key);
        }
        assert_eq!(
            store.get(&key, None).await.expect("get").expect("object"),
            value.bytes().as_ref()
        );
    }
}

#[tokio::test]
async fn refused_materialization_leaves_proxied_content_readable() {
    use loonfs_test_support::stores::{FailStore, InjectedError, OperationClass};
    let (_directory, store, mut publisher, mutation_context) = setup().await;
    let value = inline(&publisher.namespace_id, Bytes::from_static(b"readable"));
    publish(
        &mut publisher,
        &store,
        &mutation_context,
        candidate("denied", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let context = fresh_context(&store, &publisher.namespace_id).await;
    let failing = FailStore::new(
        store.clone(),
        KeyPredicate::content_blob(),
        OperationClass::Put,
        InjectedError::PermissionDenied("read-only credentials".to_owned()),
    );
    failing.fail_all();
    let engine = NamespaceEngine::reader(&failing, publisher.namespace_id.clone());
    let inode_id = engine
        .resolve_file_content("/denied-0", &context, None)
        .await
        .expect("resolve")
        .entry
        .inode_id;
    store.reset();
    let errors = [
        engine
            .direct_download_target("/denied-0", None, &context)
            .await
            .expect_err("write denied"),
        engine
            .direct_download_target_by_inode(inode_id, RevisionNo(1), &context)
            .await
            .expect_err("write denied"),
    ];
    for error in errors {
        assert_eq!(error.code(), loonfs_api::ErrorCode::ContentNotMaterialized);
        assert_eq!(error.kind(), loonfs_api::ErrorKind::Unavailable);
    }
    assert_eq!(failing.attempts(), 2);
    assert_no_writes(&store);
    store.reset();
    assert_eq!(
        engine
            .get_file("/denied-0", &context, None)
            .await
            .expect("proxied read")
            .bytes,
        value.bytes().as_ref()
    );
    assert_no_content_requests(&store);
}

// This test inserts a checksum mismatch through the WAL codec.
#[allow(clippy::disallowed_methods)]
#[tokio::test]
async fn inline_checksum_failures_match_object_validation() {
    let (_directory, store, mut publisher, mutation_context) = setup().await;
    let value = inline(&publisher.namespace_id, Bytes::from_static(b"right"));
    publish(
        &mut publisher,
        &store,
        &mutation_context,
        candidate("valid", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let input = publisher.wal_fold_input().expect("head");
    let bytes = store
        .get(
            &wal_segment(&publisher.namespace_id, &input.head.wal_no),
            None,
        )
        .await
        .expect("get WAL")
        .expect("WAL");
    let mut payload = decode_wal_segment_envelope_zstd(&bytes)
        .expect("decode")
        .into_payload();
    payload.wal_no = WalNo(input.head.wal_no.0 + 1);
    payload.prior_head_seq = input.head.seq;
    payload.head_seq = ChangeSeq(input.head.seq.0 + 1);
    let mut corrupt_ref = value.content_ref().clone();
    corrupt_ref.content_id = ContentId::generate();
    let record = &mut payload.records[0];
    record.seq = payload.head_seq;
    record.commit_id = CommitId::parse("corrupt").expect("commit");
    record
        .deltas
        .retain(|delta| matches!(delta.delta, WalDelta::AppendFileRevision { .. }));
    for delta in &mut record.deltas {
        if let WalDelta::AppendFileRevision {
            revision_no,
            content_ref,
            ..
        } = &mut delta.delta
        {
            *revision_no = RevisionNo(2);
            *content_ref = corrupt_ref.clone();
        }
    }
    record.inline_content[0].content_id = corrupt_ref.content_id.clone();
    record.inline_content[0].bytes = b"wrong".to_vec();
    payload.head_commit_id = payload.records[0].commit_id.clone();
    let key = wal_segment(&publisher.namespace_id, &payload.wal_no);
    let bytes = loonfs_api::wire::wal::encode_wal_segment_envelope_zstd(payload)
        .expect("codec does not hash")
        .into_bytes();
    store
        .put(&key, Bytes::from(bytes), PutMode::CreateIfAbsent)
        .await
        .expect("next WAL");
    let context = fresh_context(&store, &publisher.namespace_id).await;
    let engine = NamespaceEngine::reader(&store, publisher.namespace_id.clone());
    store.reset();
    let error = engine
        .get_file("/valid-0", &context, None)
        .await
        .expect_err("checksum mismatch");
    assert_no_content_requests(&store);
    store.reset();
    let download_error = engine
        .direct_download_target("/valid-0", None, &context)
        .await
        .expect_err("corrupt download");
    assert_eq!(download_error.to_string(), error.to_string());
    assert_no_content_requests(&store);
    let flush_error = flush_wal(&store, &publisher.namespace_id)
        .await
        .expect_err("corrupt tail");
    assert_eq!(flush_error.code(), loonfs_api::ErrorCode::NamespaceCorrupt);
    assert_eq!(flush_error.to_string(), error.to_string());
    assert_no_writes(&store);
    let key = content_blob(
        &publisher.namespace_id,
        corrupt_ref.owner_generation,
        &corrupt_ref.content_id,
    );
    store
        .put(&key, Bytes::from_static(b"wrong"), PutMode::CreateIfAbsent)
        .await
        .expect("bad object");
    let expected = ContentLocation::Object { object_key: key }
        .get_bytes(&store, &corrupt_ref)
        .await
        .expect_err("object mismatch");
    assert!(matches!(
        expected,
        DurableContentValidationError::ContentChecksumMismatch { .. }
    ));
    assert_eq!(error.to_string(), CoreError::from(expected).to_string());
}

#[tokio::test]
async fn folded_inline_values_remain_readable_after_all_folded_wal_is_deleted() {
    let (_directory, store, mut publisher, context) = setup().await;
    let values = vec![
        inline(&publisher.namespace_id, Bytes::new()),
        inline(&publisher.namespace_id, Bytes::from_static(b"materialized")),
    ];
    publish(
        &mut publisher,
        &store,
        &context,
        candidate("fold", values.clone()),
    )
    .await
    .expect("publish");
    let flushed = flush_wal(&store, &publisher.namespace_id)
        .await
        .expect("flush");
    assert_eq!(flushed.outcome, FlushWalOutcome::Published);
    for delete_wal in [false, true] {
        if delete_wal {
            for object in store
                .list_prefix(&wal_segment_prefix(&publisher.namespace_id))
                .await
                .expect("WAL")
            {
                store.delete(&object).await.expect("delete folded WAL");
            }
        }
        let context = fresh_context(&store, &publisher.namespace_id).await;
        let engine = NamespaceEngine::reader(&store, publisher.namespace_id.clone());
        for (index, value) in values.iter().enumerate() {
            let path = format!("/fold-{index}");
            let target = engine
                .resolve_file_content(&path, &context, None)
                .await
                .expect("target");
            assert!(matches!(target.location, ContentLocation::Object { .. }));
            store.reset();
            assert_eq!(
                engine
                    .get_file(&path, &context, None)
                    .await
                    .expect("read")
                    .bytes,
                value.bytes().as_ref()
            );
            assert!(store.snapshot().iter().any(|operation| matches!(operation, RecordedOperation::Get { key, .. } if key == target.location.object_key())));
        }
    }
}
