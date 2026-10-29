//! Reads of published inline content and projection accounting.

use super::*;
use crate::cache::{
    DecodedBlockCacheStats, HeadAndWalTailCache, MetadataSegmentCache, WalTailCacheKey,
};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use crate::storage::content::{ContentLocation, DurableContentValidationError, StreamRange};
use crate::{NamespaceEngine, RuntimeReadContext};
use loonfs_objectstore::keys::{content_blob, wal_object};
use loonfs_objectstore::PutMode;
use loonfs_types::options::StatOptions;
use loonfs_types::{DestinationPrecondition, RevisionNo, WalNo};
use std::assert_matches;
use std::num::NonZeroU64;

fn read_context(head: NamespaceReadState, basis: MetadataBasis) -> RuntimeReadContext {
    RuntimeReadContext {
        read_basis: crate::control::ReadBasis { head, basis },
        tail: None,
    }
}

async fn fresh_context(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
) -> RuntimeReadContext {
    let loaded = crate::test_support::namespace_store(store, namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("read basis");
    let basis = loaded.basis();
    read_context(loaded.read_state, basis)
}

/// Returns the namespace's components with caches that keep every segment
/// block and WAL tail, so reads and probes share what they load.
fn caching_namespace<'a>(
    store: &'a RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
) -> crate::NamespaceStore<&'a RecordingStore<LocalFsStore>> {
    crate::NamespaceStore {
        segment_cache: Arc::new(MetadataSegmentCache::unshared(usize::MAX)),
        head_and_wal_tail_cache: Arc::new(HeadAndWalTailCache::unshared(usize::MAX)),
        ..crate::test_support::namespace_store(store, namespace_id)
    }
}

fn cache_key(context: &RuntimeReadContext) -> WalTailCacheKey {
    WalTailCacheKey {
        namespace_id: context.read_basis.head.namespace_id.clone(),
        manifest_no: context.read_basis.basis.manifest_no(),
        head_seq: context.read_basis.head.seq,
    }
}

fn assert_no_content_requests(store: &RecordingStore<LocalFsStore>) {
    assert!(store.snapshot().iter().all(|operation| !matches!(loonfs_objectstore::layout::parse_object_key(operation.key()), Some(key) if key.family() == loonfs_objectstore::layout::DurableObjectFamily::ContentBlob)), "{:?}", store.snapshot());
}

#[tokio::test]
async fn every_read_verifies_tail_content_without_requesting_an_object() {
    let (_directory, store, mut publisher, writer) = setup().await;
    let values = vec![
        inline(
            &publisher.namespace.namespace_id,
            Bytes::from_static(b"inline bytes"),
        ),
        inline(&publisher.namespace.namespace_id, Bytes::new()),
    ];
    publish(&mut publisher, &writer, candidate("read", values.clone()))
        .await
        .expect("publish");
    let engine =
        NamespaceEngine::reader(caching_namespace(&store, &publisher.namespace.namespace_id));
    for (index, value) in values.iter().enumerate() {
        let path = format!("/read-{index}");
        let inode_id =
            crate::test_support::namespace_store(&store, &publisher.namespace.namespace_id)
                .load_current_metadata_view()
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
            "stream",
            "resumed",
            "inode_stream",
            "reference",
        ] {
            let context = fresh_context(&store, &publisher.namespace.namespace_id).await;
            store.reset();
            let bytes = match method {
                "path" => {
                    engine
                        .read_file(&path, &context, None)
                        .await
                        .expect("path read")
                        .bytes
                }
                "revision" => {
                    engine
                        .read_file_revision(&path, RevisionNo(1), &context, None)
                        .await
                        .expect("revision read")
                        .bytes
                }
                "inode" => engine
                    .read_file_revision_by_inode(inode_id, RevisionNo(1), &context, None)
                    .await
                    .expect("inode read"),
                "stream" | "resumed" | "inode_stream" => {
                    let start_offset = if method == "resumed" {
                        value.bytes().len().min(3)
                    } else {
                        0
                    };
                    let mut stream = if method == "inode_stream" {
                        engine
                            .read_file_stream_by_inode(
                                inode_id,
                                Some(RevisionNo(1)),
                                &context,
                                NonZeroU64::new(2).expect("chunk size"),
                                StreamRange::Whole {
                                    start_offset: start_offset as u64,
                                },
                            )
                            .await
                            .expect("inode stream")
                    } else {
                        engine
                            .read_file_stream(
                                &path,
                                &context,
                                Some(RevisionNo(1)),
                                NonZeroU64::new(2).expect("chunk size"),
                                StreamRange::Whole {
                                    start_offset: start_offset as u64,
                                },
                            )
                            .await
                            .expect("stream")
                    };
                    let mut bytes = value.bytes()[..start_offset].to_vec();
                    if start_offset != 0 {
                        assert_matches!(
                            stream.next_chunk().await,
                            Err(CoreError::ResumePrefixIncomplete { .. })
                        );
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
    let (_directory, store, mut publisher, writer) = setup().await;
    let value = inline(
        &publisher.namespace.namespace_id,
        Bytes::from_static(b"owned bytes"),
    );
    let result = publisher
        .publish_batch([candidate("owned", vec![value.clone()])], &writer.clock())
        .await;
    assert!(result.results[0].is_ok());
    let state = publisher.read_position().expect("published state");
    let tail = state.tail;
    let context = RuntimeReadContext {
        read_basis: state.read_basis,
        tail: Some(Arc::clone(&tail)),
    };
    let tail_bytes = tail.decoded_bytes();
    assert_eq!(tail.inline_bytes(), value.bytes().len());
    assert!(
        tail_bytes
            >= tail
                .chunks()
                .iter()
                .map(|chunk| chunk.rows.decoded_bytes())
                .sum::<usize>()
                + value.bytes().len()
    );
    let cloned = tail.as_ref().clone();
    assert_eq!(
        tail.content(value.content_ref()).expect("bytes").pieces[0]
            .bytes
            .as_ptr(),
        cloned
            .content(value.content_ref())
            .expect("cloned bytes")
            .pieces[0]
            .bytes
            .as_ptr()
    );
    let namespace = caching_namespace(&store, &publisher.namespace.namespace_id);
    let engine = NamespaceEngine::reader(namespace.clone());
    store.reset();
    assert_eq!(
        engine
            .read_file("/owned-0", &context, None)
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
        .starts_with(&wal_prefix(&publisher.namespace.namespace_id))));
    assert_no_content_requests(&store);
    assert_eq!(
        namespace.head_and_wal_tail_cache.tail_stats(),
        DecodedBlockCacheStats::default(),
        "reads from the position's tail neither look up nor insert a tail"
    );

    let old_tail = Arc::as_ptr(&tail) as usize;
    // Holding the tail here would make the publish copy it.
    drop((tail, context));
    let later = inline(
        &publisher.namespace.namespace_id,
        Bytes::from_static(b"later"),
    );
    let published = publisher
        .publish_batch([candidate("later", vec![later])], &writer.clock())
        .await;
    assert!(published.results[0].is_ok());
    let later = publisher.read_position().expect("later state");
    assert_eq!(
        Arc::as_ptr(&later.tail) as usize,
        old_tail,
        "the publish extends the position's tail in place"
    );
}

#[tokio::test]
async fn a_publish_shares_every_earlier_chunk_with_a_tail_a_reader_holds() {
    let (_directory, _store, mut publisher, writer) = setup().await;
    let mut tails = Vec::new();
    for name in ["first", "second"] {
        let value = inline(
            &publisher.namespace.namespace_id,
            Bytes::from_static(b"bytes"),
        );
        let published = publisher
            .publish_batch([candidate(name, vec![value])], &writer.clock())
            .await;
        assert!(published.results[0].is_ok());
        tails.push(publisher.read_position().expect("published state").tail);
    }
    let (held, extended) = (&tails[0], &tails[1]);
    assert!(
        !Arc::ptr_eq(held, extended),
        "a held tail makes the publish extend a copy of its chunk list"
    );
    assert_eq!(extended.chunks().len(), held.chunks().len() + 1);
    assert!(held
        .chunks()
        .iter()
        .zip(extended.chunks())
        .all(|(held, extended)| Arc::ptr_eq(held, extended)));
}

#[tokio::test]
async fn a_copy_and_an_advanced_reader_keep_earlier_inline_content() {
    let (_directory, store, mut publisher, writer) = setup().await;
    let value = inline(
        &publisher.namespace.namespace_id,
        Bytes::from_static(b"copied"),
    );
    publish(
        &mut publisher,
        &writer,
        candidate("source", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let mut context = fresh_context(&store, &publisher.namespace.namespace_id).await;
    let namespace = caching_namespace(&store, &publisher.namespace.namespace_id);
    let engine = NamespaceEngine::reader(namespace.clone());
    engine
        .read_file("/source-0", &context, None)
        .await
        .expect("cache first tail");
    let copy = CommitCandidate::new(CommitIntent::single(
        CommitId::parse("copy").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/source-0").expect("source"),
            destination_path: AbsolutePath::parse("/copy").expect("destination"),
            precondition: DestinationPrecondition::default(),
        },
    ));
    publish(&mut publisher, &writer, copy).await.expect("copy");
    store.reset();
    assert!(crate::wal::probe_namespace_wal(
        &store,
        &namespace.head_and_wal_tail_cache,
        &mut context
    )
    .await
    .expect("advance"));
    assert_eq!(
        engine
            .read_file("/copy", &context, None)
            .await
            .expect("read copy")
            .bytes,
        value.bytes().as_ref()
    );
    assert_eq!(
        engine
            .read_file("/source-0", &context, None)
            .await
            .expect("read source")
            .bytes,
        value.bytes().as_ref()
    );
    assert_no_content_requests(&store);
    assert_eq!(
        namespace
            .head_and_wal_tail_cache
            .get_tail(&cache_key(&context))
            .expect("advanced tail"),
        publisher.writer_position().expect("publish tail").tail
    );

    let later = inline(
        &publisher.namespace.namespace_id,
        Bytes::from_static(b"later"),
    );
    publish(&mut publisher, &writer, candidate("later", vec![later]))
        .await
        .expect("later publish");
    let newest = publisher.writer_position().expect("publish tail");
    namespace.head_and_wal_tail_cache.insert_tail(
        WalTailCacheKey {
            head_seq: newest.read_basis.head.seq,
            ..cache_key(&context)
        },
        Arc::clone(&newest.tail),
    );
    assert!(crate::wal::probe_namespace_wal(
        &store,
        &namespace.head_and_wal_tail_cache,
        &mut context
    )
    .await
    .expect("advance one object"));
    assert!(
        Arc::ptr_eq(
            &namespace
                .head_and_wal_tail_cache
                .get_tail(&cache_key(&context))
                .expect("newest tail"),
            &newest.tail
        ),
        "a reader one object behind takes the cached tail instead of replaying onto its own"
    );
}

#[tokio::test]
async fn foreign_references_require_a_layout_and_uploaded_downloads_do_not_write() {
    let (_directory, store, mut publisher, writer) = setup().await;
    let value = inline(
        &publisher.namespace.namespace_id,
        Bytes::from_static(b"local"),
    );
    publish(
        &mut publisher,
        &writer,
        candidate("local", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let context = fresh_context(&store, &publisher.namespace.namespace_id).await;
    let engine =
        NamespaceEngine::reader(caching_namespace(&store, &publisher.namespace.namespace_id));
    let foreign = ContentRef::blob_v1(
        NamespaceId::parse("foreign").expect("owner"),
        value.content_ref().content_id.clone(),
        b"foreign",
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let key = content_blob(&foreign.owner_namespace_id, &foreign.content_id);
    store
        .put(
            &key,
            Bytes::from_static(b"foreign"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("foreign object");
    store.reset();
    assert_matches!(
        engine.read_content_ref(&foreign, u64::MAX, &context).await,
        Err(CoreError::NamespaceCorrupt(_))
    );
    assert_no_content_requests(&store);
    assert_no_writes(&store);
    let stored = crate::test_support::namespace_store(&store, &publisher.namespace.namespace_id)
        .store_bytes_as_content(b"object")
        .await
        .expect("store object");
    let candidate = CommitCandidate::prepared(
        CommitIntent::single(
            CommitId::parse("object").expect("commit"),
            loonfs_test_support::test_actor(),
            None,
            put("/object", stored.content_ref()),
        ),
        vec![PreparedContent::for_durable_content_write(
            stored.content_ref().clone(),
        )],
    );
    publish(&mut publisher, &writer, candidate)
        .await
        .expect("publish object");
    let context = fresh_context(&store, &publisher.namespace.namespace_id).await;
    let path_target = engine
        .direct_download_target("/object", None, 0, &context)
        .await
        .expect("object download");
    let inode_id = engine
        .resolve_path("/object", StatOptions::default(), &context)
        .await
        .expect("object entry")
        .inode_id;
    let inode_target = engine
        .direct_download_target_by_inode(inode_id, Some(RevisionNo(1)), 0, &context)
        .await
        .expect("inode download");
    assert_eq!(path_target.ranges[0].object_key, stored.object_key());
    assert_eq!(inode_target.ranges[0].object_key, stored.object_key());
}

// This test inserts a checksum mismatch through the WAL codec.
#[allow(clippy::disallowed_methods)]
#[tokio::test]
async fn inline_checksum_failures_match_object_validation() {
    let (_directory, store, mut publisher, writer) = setup().await;
    let value = inline(
        &publisher.namespace.namespace_id,
        Bytes::from_static(b"right"),
    );
    publish(
        &mut publisher,
        &writer,
        candidate("valid", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let input = publisher.writer_position().expect("head");
    let bytes = store
        .get(
            &wal_object(
                &publisher.namespace.namespace_id,
                &input.read_basis.head.wal_no,
            ),
            None,
        )
        .await
        .expect("get WAL")
        .expect("WAL");
    let mut payload = decode_wal_object_envelope_zstd(&bytes)
        .expect("decode")
        .into_payload();
    payload.wal_no = WalNo(input.read_basis.head.wal_no.0 + 1);
    payload.head_seq = ChangeSeq(input.read_basis.head.seq.0 + 1);
    let mut corrupt_ref = value.content_ref().clone();
    corrupt_ref.content_id = ContentId::generate();
    let record = &mut payload.records[0];
    record.committed_seq = payload.head_seq;
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
    let key = wal_object(&publisher.namespace.namespace_id, &payload.wal_no);
    let bytes = loonfs_types::format::wal::encode_wal_object_envelope_zstd(payload)
        .expect("codec does not hash")
        .into_bytes();
    store
        .put(&key, Bytes::from(bytes), PutMode::CreateIfAbsent)
        .await
        .expect("next WAL");
    let context = fresh_context(&store, &publisher.namespace.namespace_id).await;
    let engine =
        NamespaceEngine::reader(caching_namespace(&store, &publisher.namespace.namespace_id));
    store.reset();
    let error = engine
        .read_file("/valid-0", &context, None)
        .await
        .expect_err("checksum mismatch");
    assert_no_content_requests(&store);
    store.reset();
    let download_error = engine
        .direct_download_target("/valid-0", None, 0, &context)
        .await
        .expect_err("corrupt download");
    assert_eq!(download_error.to_string(), error.to_string());
    assert_no_writes(&store);
    assert_eq!(store.counts().heads, 1);
    assert_eq!(store.counts().gets, 0);
    let fold_error =
        crate::test_support::namespace_store(&store, &publisher.namespace.namespace_id)
            .fold_wal()
            .await
            .expect_err("corrupt tail");
    assert_eq!(fold_error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
    assert_eq!(fold_error.to_string(), error.to_string());
    assert_no_writes(&store);
    let key = content_blob(&publisher.namespace.namespace_id, &corrupt_ref.content_id);
    store
        .put(&key, Bytes::from_static(b"wrong"), PutMode::CreateIfAbsent)
        .await
        .expect("bad object");
    let expected = ContentLocation::whole(&corrupt_ref)
        .read_bytes(&store, None, &corrupt_ref)
        .await
        .expect_err("object mismatch");
    assert_matches!(
        expected,
        DurableContentValidationError::ContentChecksumMismatch { .. }
    );
    assert_eq!(error.to_string(), CoreError::from(expected).to_string());
}

#[tokio::test]
async fn folded_inline_values_remain_readable_after_all_folded_wal_is_deleted() {
    let (_directory, store, mut publisher, context) = setup().await;
    let values = vec![
        inline(&publisher.namespace.namespace_id, Bytes::new()),
        inline(
            &publisher.namespace.namespace_id,
            Bytes::from_static(b"materialized"),
        ),
    ];
    publish(&mut publisher, &context, candidate("fold", values.clone()))
        .await
        .expect("publish");
    let folded = crate::test_support::namespace_store(&store, &publisher.namespace.namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    for delete_wal in [false, true] {
        if delete_wal {
            for object in store
                .list_prefix(&wal_prefix(&publisher.namespace.namespace_id))
                .await
                .expect("WAL")
            {
                store.delete(&object).await.expect("delete folded WAL");
            }
        }
        let context = fresh_context(&store, &publisher.namespace.namespace_id).await;
        let engine =
            NamespaceEngine::reader(caching_namespace(&store, &publisher.namespace.namespace_id));
        for (index, value) in values.iter().enumerate() {
            let path = format!("/fold-{index}");
            let location = engine
                .resolve_content_location(value.content_ref(), &context)
                .await
                .expect("location");
            assert!(!location.has_pieces());
            store.reset();
            assert_eq!(
                engine
                    .read_file(&path, &context, None)
                    .await
                    .expect("read")
                    .bytes,
                value.bytes().as_ref()
            );
            let expected_ranges = if value.bytes().is_empty() {
                Vec::new()
            } else {
                vec![(
                    location.object_key().to_owned(),
                    Some((0, value.bytes().len() as u64)),
                )]
            };
            assert_eq!(content_gets(&store), expected_ranges);
            assert_eq!(store.counts().heads, 0);
        }
    }
}

fn content_gets(store: &RecordingStore<LocalFsStore>) -> Vec<(String, Option<(u64, u64)>)> {
    store
        .take_gets()
        .into_iter()
        .filter(|(key, _)| {
            matches!(
                loonfs_objectstore::layout::parse_object_key(key),
                Some(key) if key.family() == loonfs_objectstore::layout::DurableObjectFamily::ContentBlob
            )
        })
        .collect()
}

#[tokio::test]
async fn a_reference_with_unfolded_pieces_reads_its_layout_then_the_pieces() {
    let (_directory, store, mut publisher, context) = setup().await;
    let namespace_id = publisher.namespace.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut publisher, &context, b"hello").await;
    commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_id,
        b"hello world",
        5,
        None,
    )
    .await;
    let key = content_blob(&namespace_id, &original.content_id);
    let engine =
        NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id));
    let read_context = fresh_context(&store, &namespace_id).await;

    store.reset();
    let buffered = engine
        .read_file("/file-0", &read_context, None)
        .await
        .expect("buffered read")
        .bytes;
    assert_eq!(buffered, b"hello world");
    assert_eq!(content_gets(&store), [(key.clone(), Some((0, 5)))]);

    let mut stream = engine
        .read_file_stream(
            "/file-0",
            &read_context,
            None,
            NonZeroU64::new(4).expect("chunk size"),
            StreamRange::default(),
        )
        .await
        .expect("stream");
    let mut streamed = Vec::new();
    while let Some(chunk) = stream.next_chunk().await.expect("verified chunk") {
        streamed.extend_from_slice(&chunk);
    }
    assert_eq!(streamed, b"hello world");

    // A ranged read gets only the part of the layout that the range covers,
    // takes the rest from the pieces, and writes nothing.
    for (start_offset, length, expected, expected_gets) in [
        (1, 3, &b"ell"[..], vec![(key.clone(), Some((1, 4)))]),
        (6, 3, &b"wor"[..], Vec::new()),
        (3, 5, &b"lo wo"[..], vec![(key.clone(), Some((3, 5)))]),
        (8, 100, &b"rld"[..], Vec::new()),
    ] {
        store.reset();
        let range = loonfs_types::options::ReadRange {
            start_offset,
            length: NonZeroU64::new(length),
        };
        let mut stream = engine
            .read_file_stream(
                "/file-0",
                &read_context,
                None,
                NonZeroU64::new(4).expect("chunk size"),
                StreamRange::Range(range),
            )
            .await
            .expect("ranged stream");
        let mut ranged = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("ranged chunk") {
            ranged.extend_from_slice(&chunk);
        }
        assert_eq!(ranged, expected, "{range:?}");
        assert_eq!(store.counts().puts, 0, "{range:?}");
        assert_eq!(content_gets(&store), expected_gets, "{range:?}");
    }
}

#[tokio::test]
async fn a_reference_older_than_its_object_reads_only_its_prefix() {
    let (_directory, store, mut publisher, context) = setup().await;
    let namespace_id = publisher.namespace.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut publisher, &context, b"hello").await;
    commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_id,
        b"hello world",
        5,
        None,
    )
    .await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the append");
    let key = loonfs_objectstore::keys::content_span(&namespace_id, &original.content_id, 0, 11);

    store.reset();
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(1)).await,
        b"hello"
    );
    assert_eq!(content_gets(&store), [(key.clone(), Some((0, 5)))]);
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(2)).await,
        b"hello world"
    );
}
