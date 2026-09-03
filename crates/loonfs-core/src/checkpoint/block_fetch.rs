//! Fetching, decoding, and cache publication for SST index and filter blocks.
//!
//! Index, filter, and point data loads consult the per-view memo, the decoded
//! block cache, and the node-local cache of stored bytes. Their fetched
//! sections, including whole-small-segment loads, are offered to the local
//! tier. Wide data spans skip that tier because its awaited point reads lose
//! the coalescing provided by ranged store GETs.

use super::block_load::SessionBlockMemo;
use super::cache::{
    DecodedMetadataSegmentBlock, MetadataSegmentBlockKind, MetadataSegmentCache,
    MetadataSegmentCacheKey,
};
use super::data_block_load::decoded_data_cache_block;
use super::error::ManifestLoadError;
use super::runs::{MetadataRunManifest, MAX_MATERIALIZED_TABLE_LOADS, OPEN_PREFETCH_ROW_FAMILIES};
use super::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockKey, StoredMetadataBlockKind,
};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_api::wire::hex::hex_decode_bytes;
use loonfs_api::wire::manifest::MetadataSegmentRef;
use loonfs_api::wire::manifest::RunTier;
use loonfs_api::wire::sst_blocks::{
    decode_data_block, decode_filter_block, decode_index_block, BlockHandle, SegmentFilter,
    SegmentIndexEntry, SstBlockCodecError,
};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::{ByteRange, ObjectStore};
use std::sync::Arc;

#[derive(Clone)]
struct SegmentTailPrefetch {
    descriptor: MetadataSegmentRef,
    want: MetadataSegmentBlockKind,
}

fn select_segment_tail_prefetches(
    segment_cache: &MetadataSegmentCache,
    memo: &SessionBlockMemo,
    runs: &[MetadataRunManifest],
) -> Vec<SegmentTailPrefetch> {
    let max_stored_bytes = u64::try_from(segment_cache.open_prefetch_max_stored_bytes())
        .expect("a usize stored-byte budget should fit in u64");
    let mut selected = Vec::new();
    let mut selected_stored_bytes = 0_u64;
    for family in OPEN_PREFETCH_ROW_FAMILIES {
        for tier in [RunTier::Base, RunTier::Delta] {
            for run in runs.iter().filter(|run| run.tier == tier) {
                let Some(family_segments) = run
                    .segments
                    .iter()
                    .find(|segments| segments.family == family)
                else {
                    continue;
                };
                for descriptor in &family_segments.segments {
                    let (want, handle, stored_bytes) = if descriptor.filter_inline.is_some() {
                        (
                            MetadataSegmentBlockKind::Index,
                            descriptor.index_block,
                            u64::from(descriptor.index_block.stored_len),
                        )
                    } else {
                        (
                            MetadataSegmentBlockKind::Filter,
                            descriptor.filter_block,
                            u64::from(descriptor.filter_block.stored_len)
                                + u64::from(descriptor.index_block.stored_len),
                        )
                    };
                    let cache_key = segment_block_cache_key(descriptor, want, handle.offset);
                    if memo.get(&cache_key).is_some() || segment_cache.contains(&cache_key) {
                        continue;
                    }
                    let next_stored_bytes = selected_stored_bytes
                        .checked_add(stored_bytes)
                        .expect("selected segment tail bytes should fit in u64");
                    if next_stored_bytes > max_stored_bytes {
                        return selected;
                    }
                    selected_stored_bytes = next_stored_bytes;
                    selected.push(SegmentTailPrefetch {
                        descriptor: descriptor.clone(),
                        want,
                    });
                }
            }
        }
    }
    selected
}

pub(super) async fn prefetch_segment_tails<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: &MetadataSegmentCache,
    memo: &SessionBlockMemo,
    runs: &[MetadataRunManifest],
) {
    let selected = select_segment_tail_prefetches(segment_cache, memo, runs);
    let mut failed_requests = 0_usize;
    let mut first_error = None;
    let results = futures::stream::iter(selected)
        .map(|selected| async move {
            load_and_publish_segment_sections(
                store,
                Some(segment_cache),
                memo,
                &selected.descriptor,
                selected.want,
            )
            .await
        })
        .buffer_unordered(MAX_MATERIALIZED_TABLE_LOADS)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        if let Err(error) = result {
            failed_requests += 1;
            first_error.get_or_insert_with(|| error.to_string());
        }
    }
    if let Some(first_error) = first_error {
        tracing::debug!(
            failed_requests,
            %first_error,
            "metadata segment tail prefetch requests failed"
        );
    }
}

pub(super) fn segment_block_cache_key(
    descriptor: &MetadataSegmentRef,
    block_kind: MetadataSegmentBlockKind,
    block_offset: u64,
) -> MetadataSegmentCacheKey {
    MetadataSegmentCacheKey {
        identity: descriptor.object_checksum.clone(),
        block_kind,
        block_offset,
    }
}

/// The local stored-block cache's key for one section of a segment.
///
/// The identity is the segment's object checksum, the same immutable bytes
/// the decoded cache keys by, and the handle's offset locates the section
/// inside the object.
fn stored_block_key(
    descriptor: &MetadataSegmentRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
) -> StoredMetadataBlockKey {
    StoredMetadataBlockKey {
        object_checksum: descriptor.object_checksum.clone(),
        kind,
        offset: handle.offset,
    }
}

/// Returns the stored-block cache associated with the decoded segment cache.
/// Maintenance reads pass no segment cache and therefore use neither cache.
fn stored_block_cache(
    segment_cache: Option<&MetadataSegmentCache>,
) -> Option<&Arc<dyn StoredMetadataBlockCache>> {
    segment_cache?.stored_block_cache()
}

/// Answers one section from the local stored-block cache, when it holds
/// bytes for it that decode.
///
/// The bytes go to the same decoder the store's bytes go to, so a hit is
/// checksummed and structure-checked exactly as a fetch is. A hit that does
/// not decode says nothing about the segment — object storage still holds
/// the authority — so the entry is dropped and the caller falls through to
/// one ordinary fetch, which reports corruption if the store's bytes fail
/// too. Nothing retries the local tier.
pub(super) async fn stored_block_section<T>(
    segment_cache: Option<&MetadataSegmentCache>,
    descriptor: &MetadataSegmentRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
    decode: impl FnOnce(&[u8], &BlockHandle) -> Result<T, SstBlockCodecError>,
) -> Option<T> {
    let cache = stored_block_cache(segment_cache)?;
    let key = stored_block_key(descriptor, kind, handle);
    let bytes = cache.get(&key).await?;
    // The decoder checks the stored length too; checking it first keeps a
    // truncated entry from reaching the decompressor at all.
    let decoded = if bytes.len() == handle.stored_len as usize {
        decode(&bytes, handle).map_err(|error| error.to_string())
    } else {
        Err(format!(
            "entry holds {} bytes, expected {}",
            bytes.len(),
            handle.stored_len
        ))
    };
    match decoded {
        Ok(decoded) => Some(decoded),
        Err(reason) => {
            // The read still succeeds from the store, but bytes this tier
            // wrote and read back no longer decode: the local disk is
            // returning something other than what it was given. That is a
            // hardware or filesystem problem an operator has to know about,
            // so it is a warning rather than a debug line.
            tracing::warn!(
                object_key = %metadata_segment_object_key(descriptor),
                "local block cache entry did not decode, refetching from the store: {reason}"
            );
            cache.invalidate(&key);
            None
        }
    }
}

/// Offers one section's stored bytes to the local stored-block cache.
///
/// Every section a fetch produced is offered under its own key, so a later
/// read of any one of them needs no GET. The bytes are copied rather than
/// sliced out of the fetch buffer: a slice would keep the whole fetched span
/// alive for as long as the cache held any one section of it.
pub(super) fn offer_stored_block(
    segment_cache: Option<&MetadataSegmentCache>,
    descriptor: &MetadataSegmentRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
    stored: &[u8],
) {
    let Some(cache) = stored_block_cache(segment_cache) else {
        return;
    };
    cache.insert(
        stored_block_key(descriptor, kind, handle),
        Bytes::copy_from_slice(stored),
    );
}

/// Fetches exactly the byte range a handle names.
pub(super) async fn load_section_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, ManifestLoadError> {
    let end_exclusive =
        offset
            .checked_add(len)
            .ok_or_else(|| ManifestLoadError::SegmentDescriptorMismatch {
                object_key: object_key.to_owned(),
                message: "the named section runs past the address space".to_owned(),
            })?;
    let Some(bytes) = store
        .get(
            object_key,
            Some(ByteRange {
                start_inclusive: offset,
                end_exclusive,
            }),
        )
        .await
        .map_err(|err| ManifestLoadError::ReadSegment {
            object_key: object_key.to_owned(),
            message: err.public_message().into_owned(),
        })?
    else {
        return Err(ManifestLoadError::MissingSegment {
            object_key: object_key.to_owned(),
        });
    };
    if bytes.len() as u64 != len {
        return Err(ManifestLoadError::ReadSegment {
            object_key: object_key.to_owned(),
            message: format!("ranged read returned {} bytes, expected {len}", bytes.len()),
        });
    }
    Ok(bytes.to_vec())
}

pub(super) fn segment_codec_error(
    object_key: &str,
    err: impl std::fmt::Display,
) -> ManifestLoadError {
    ManifestLoadError::SegmentCodec {
        object_key: object_key.to_owned(),
        message: err.to_string(),
    }
}

/// Largest segment object fetched whole on first touch, in stored bytes.
/// Below this, splitting the index and data reads into separate ranged GETs
/// costs more round-trips than the whole object costs bytes; one GET
/// publishes every section to the memo and shared cache. Sized to catch
/// delta-run segments (one or two data blocks) while leaving base segments
/// on the per-section path.
const WHOLE_SEGMENT_LOAD_MAX_BYTES: u64 = 128 * 1024;

/// A segment object's total stored length: the index block is the last
/// section, so it ends the object.
pub(super) fn segment_object_len(descriptor: &MetadataSegmentRef) -> u64 {
    descriptor.index_block.offset + u64::from(descriptor.index_block.stored_len)
}

/// Fetches the byte span that answers one filter or index load with a single
/// GET, decoding and publishing every section the span covers: the whole
/// object when it is small (index, filter, and all data blocks), otherwise
/// everything from the requested section to the end of the object — for a
/// filter that is the filter plus the index that directly follows it
/// (manifest loading rejects any other layout), and for an index exactly the
/// index, which ends the object. Returns the requested block.
///
/// Every section the span covers is also offered to the local stored-block
/// cache in its stored form, so what one GET produced is what a later read
/// finds there.
async fn load_and_publish_segment_sections<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
    want: MetadataSegmentBlockKind,
) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
    let object_key = metadata_segment_object_key(descriptor);
    let filter_handle = descriptor.filter_block;
    let index_handle = descriptor.index_block;
    let object_len = segment_object_len(descriptor);
    let fetch_whole_object = object_len <= WHOLE_SEGMENT_LOAD_MAX_BYTES;
    let fetch_offset = match want {
        MetadataSegmentBlockKind::Data | MetadataSegmentBlockKind::Manifest => {
            return Err(segment_codec_error(
                &object_key,
                "segment section fetch supports only filter and index blocks",
            ));
        }
        _ if fetch_whole_object => 0,
        MetadataSegmentBlockKind::Filter => filter_handle.offset,
        MetadataSegmentBlockKind::Index => index_handle.offset,
    };
    let bytes =
        load_section_bytes(store, &object_key, fetch_offset, object_len - fetch_offset).await?;
    let section = |handle: &BlockHandle| -> Option<&[u8]> {
        let start = usize::try_from(handle.offset.checked_sub(fetch_offset)?).ok()?;
        bytes.get(start..start + handle.stored_len as usize)
    };

    let index_entries = match section(&index_handle) {
        Some(stored) => {
            offer_stored_block(
                segment_cache,
                descriptor,
                StoredMetadataBlockKind::Index,
                &index_handle,
                stored,
            );
            let entries = Arc::new(
                decode_index_block(stored, &index_handle)
                    .map_err(|err| segment_codec_error(&object_key, err))?,
            );
            let block = DecodedMetadataSegmentBlock::Index {
                decoded_bytes: index_handle.decoded_len as usize,
                entries: Arc::clone(&entries),
            };
            publish_segment_block(
                segment_cache,
                memo,
                segment_block_cache_key(
                    descriptor,
                    MetadataSegmentBlockKind::Index,
                    index_handle.offset,
                ),
                &block,
            );
            Some(block)
        }
        None => None,
    };
    let filter_block = match section(&filter_handle) {
        Some(stored) => {
            offer_stored_block(
                segment_cache,
                descriptor,
                StoredMetadataBlockKind::Filter,
                &filter_handle,
                stored,
            );
            let filter = decode_filter_block(stored, &filter_handle)
                .map_err(|err| segment_codec_error(&object_key, err))?;
            let block = DecodedMetadataSegmentBlock::Filter {
                decoded_bytes: filter_handle.decoded_len as usize,
                filter: Arc::new(filter),
            };
            publish_segment_block(
                segment_cache,
                memo,
                segment_block_cache_key(
                    descriptor,
                    MetadataSegmentBlockKind::Filter,
                    filter_handle.offset,
                ),
                &block,
            );
            Some(block)
        }
        None => None,
    };
    if fetch_whole_object {
        if let Some(DecodedMetadataSegmentBlock::Index { entries, .. }) = &index_entries {
            for entry in entries.iter() {
                let Some(stored) = section(&entry.block) else {
                    return Err(segment_codec_error(
                        &object_key,
                        "data block outside the segment object bounds",
                    ));
                };
                offer_stored_block(
                    segment_cache,
                    descriptor,
                    StoredMetadataBlockKind::Data,
                    &entry.block,
                    stored,
                );
                let decoded = decode_data_block(stored, &entry.block)
                    .map_err(|err| segment_codec_error(&object_key, err))?;
                let block = decoded_data_cache_block(decoded);
                publish_segment_block(
                    segment_cache,
                    memo,
                    segment_block_cache_key(
                        descriptor,
                        MetadataSegmentBlockKind::Data,
                        entry.block.offset,
                    ),
                    &block,
                );
            }
        }
    }

    let wanted = match want {
        MetadataSegmentBlockKind::Filter => filter_block,
        _ => index_entries,
    };
    wanted.ok_or_else(|| {
        segment_codec_error(
            &object_key,
            "requested section outside the segment object bounds",
        )
    })
}

fn publish_segment_block(
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    cache_key: MetadataSegmentCacheKey,
    block: &DecodedMetadataSegmentBlock,
) {
    memo.record(&cache_key, block);
    if let Some(cache) = segment_cache {
        cache.insert(cache_key, block.clone());
    }
}

pub(super) async fn load_segment_index<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    load_segment_index_inner(store, segment_cache, memo, descriptor, true).await
}

/// Loads only the index section even for a small segment. Reorganization
/// uses this to account data-block decoded bytes before any row payload is
/// decoded; the normal lookup path keeps its whole-small-segment shortcut.
pub(super) async fn load_segment_index_for_reorganization<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    load_segment_index_inner(store, segment_cache, memo, descriptor, false).await
}

async fn load_segment_index_inner<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
    load_small_segment_whole: bool,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    let handle = descriptor.index_block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Index, handle.offset);
    if let Some(DecodedMetadataSegmentBlock::Index { entries, .. }) = memo.get(&cache_key) {
        return Ok(entries);
    }
    let fetch = || async {
        // Between the decoded cache above and the store below: a local copy
        // of the same stored bytes. A hit answers this section and nothing
        // else, so the sibling sections a fetch would have published are
        // published only when a fetch happens.
        if let Some(entries) = stored_block_section(
            segment_cache,
            descriptor,
            StoredMetadataBlockKind::Index,
            &handle,
            decode_index_block,
        )
        .await
        {
            return Ok(DecodedMetadataSegmentBlock::Index {
                decoded_bytes: handle.decoded_len as usize,
                entries: Arc::new(entries),
            });
        }
        if load_small_segment_whole {
            load_and_publish_segment_sections(
                store,
                segment_cache,
                memo,
                descriptor,
                MetadataSegmentBlockKind::Index,
            )
            .await
        } else {
            let object_key = metadata_segment_object_key(descriptor);
            let stored = load_section_bytes(
                store,
                &object_key,
                handle.offset,
                u64::from(handle.stored_len),
            )
            .await?;
            let entries = decode_index_block(&stored, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?;
            Ok(DecodedMetadataSegmentBlock::Index {
                decoded_bytes: handle.decoded_len as usize,
                entries: Arc::new(entries),
            })
        }
    };
    let block = match segment_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    block.into_index(&metadata_segment_object_key(descriptor))
}

/// Loads a segment's bloom filter block: the cheap pre-index check a lookup
/// consults to skip the segment entirely. A copy inlined in the manifest
/// descriptor answers without any object fetch; otherwise the local
/// stored-block cache answers if it holds the section, and only failing that
/// does one ranged GET cover the filter together with the adjacent index
/// block (and the whole object when it is small), so an admitted lookup does
/// not pay a second round-trip for the index.
pub(super) async fn load_segment_filter<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
) -> Result<Arc<SegmentFilter>, ManifestLoadError> {
    let handle = descriptor.filter_block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Filter, handle.offset);
    if let Some(DecodedMetadataSegmentBlock::Filter { filter, .. }) = memo.get(&cache_key) {
        return Ok(filter);
    }
    if let Some(inline) = &descriptor.filter_inline {
        let object_key = metadata_segment_object_key(descriptor);
        let bytes = hex_decode_bytes(inline)
            .map_err(|err| segment_codec_error(&object_key, format!("inline filter: {err}")))?;
        // The handle names and verifies the durable filter block; decoding
        // the inline copy against it proves the two are byte-identical.
        let filter = Arc::new(
            decode_filter_block(&bytes, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?,
        );
        let block = DecodedMetadataSegmentBlock::Filter {
            decoded_bytes: handle.decoded_len as usize,
            filter: Arc::clone(&filter),
        };
        publish_segment_block(segment_cache, memo, cache_key, &block);
        return Ok(filter);
    }
    let fetch = || async {
        // Reached only when the descriptor carried no inline copy: an
        // inlined filter is answered above and never touches this tier.
        if let Some(filter) = stored_block_section(
            segment_cache,
            descriptor,
            StoredMetadataBlockKind::Filter,
            &handle,
            decode_filter_block,
        )
        .await
        {
            return Ok(DecodedMetadataSegmentBlock::Filter {
                decoded_bytes: handle.decoded_len as usize,
                filter: Arc::new(filter),
            });
        }
        load_and_publish_segment_sections(
            store,
            segment_cache,
            memo,
            descriptor,
            MetadataSegmentBlockKind::Filter,
        )
        .await
    };
    let block = match segment_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    block.into_filter(&metadata_segment_object_key(descriptor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::cache::MetadataSegmentCacheConfig;
    use crate::checkpoint::runs::MetadataFamilySegments;
    use loonfs_api::wire::manifest::MetadataRowFamily;
    use loonfs_api::{ChangeSeq, MetadataSegmentId, NamespaceId, RunNo};

    fn descriptor(
        id: u32,
        family: MetadataRowFamily,
        filter_stored_len: u32,
        index_stored_len: u32,
    ) -> MetadataSegmentRef {
        MetadataSegmentRef {
            owner_namespace_id: NamespaceId::parse("prefetch").expect("namespace id"),
            segment_id: MetadataSegmentId::parse(format!("seg_{id:032x}")).expect("segment id"),
            compaction_job_id: None,
            family,
            segment_index: 0,
            row_count: 1,
            min_row_key: "a".to_owned(),
            max_row_key: "z".to_owned(),
            index_block: BlockHandle {
                offset: u64::from(filter_stored_len),
                stored_len: index_stored_len,
                decoded_len: index_stored_len,
                crc32c: 0,
            },
            filter_block: BlockHandle {
                offset: 0,
                stored_len: filter_stored_len,
                decoded_len: filter_stored_len,
                crc32c: 0,
            },
            filter_inline: Some(String::new()),
            object_checksum: format!("checksum-{id}"),
        }
    }

    fn run(
        run_no: u64,
        tier: RunTier,
        descriptors: Vec<MetadataSegmentRef>,
    ) -> MetadataRunManifest {
        let mut segments = Vec::new();
        for descriptor in descriptors {
            let family = descriptor.family;
            segments.push(MetadataFamilySegments {
                family,
                segments: vec![descriptor],
            });
        }
        MetadataRunManifest {
            run_no: RunNo(run_no),
            run_seq: ChangeSeq(run_no),
            tier,
            segments,
        }
    }

    #[test]
    fn segment_tail_selection_honors_priority_budget_and_memo_hits() {
        let delta_bind = descriptor(1, MetadataRowFamily::DirentryBinds, 1, 4);
        let base_bind = descriptor(2, MetadataRowFamily::DirentryBinds, 1, 3);
        let inode = descriptor(3, MetadataRowFamily::Inodes, 1, 3);
        let revision = descriptor(4, MetadataRowFamily::Revisions, 1, 1);
        let runs = vec![
            run(1, RunTier::Delta, vec![delta_bind]),
            run(2, RunTier::Base, vec![base_bind.clone(), inode, revision]),
        ];
        let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig {
            open_prefetch_max_stored_bytes: 7,
            ..MetadataSegmentCacheConfig::default()
        });
        let memo = SessionBlockMemo::default();

        let selected = select_segment_tail_prefetches(&cache, &memo, &runs);
        assert_eq!(
            selected
                .iter()
                .map(|selected| selected.descriptor.segment_id.as_str())
                .collect::<Vec<_>>(),
            [
                "seg_00000000000000000000000000000002",
                "seg_00000000000000000000000000000001",
            ]
        );

        let base_bind_key = segment_block_cache_key(
            &base_bind,
            MetadataSegmentBlockKind::Index,
            base_bind.index_block.offset,
        );
        memo.record(
            &base_bind_key,
            &DecodedMetadataSegmentBlock::Index {
                entries: Arc::new(Vec::new()),
                decoded_bytes: 0,
            },
        );
        let selected = select_segment_tail_prefetches(&cache, &memo, &runs);
        assert_eq!(
            selected
                .iter()
                .map(|selected| selected.descriptor.segment_id.as_str())
                .collect::<Vec<_>>(),
            [
                "seg_00000000000000000000000000000001",
                "seg_00000000000000000000000000000003",
            ]
        );
    }
}
