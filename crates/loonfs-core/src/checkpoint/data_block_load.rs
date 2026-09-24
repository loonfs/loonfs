//! Fetching, decoding, coalescing, and cache publication for SST data blocks.
//!
//! Point loads consult the per-view memo, the decoded block cache, and the
//! node-local cache of stored bytes. Span loads skip the stored-byte tier
//! because it answers one block per awaited read, turning a wide scan into
//! tens of thousands of serial point reads. Spans use the decoded caches and
//! coalesced store GETs, and they do not populate the stored-byte tier.

use super::block_fetch::{
    load_section_bytes, offer_stored_block, segment_block_cache_key, segment_codec_error,
    stored_block_section,
};
use super::block_load::SessionBlockMemo;
use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentBlockKind, MetadataSegmentCache};
use super::error::ManifestLoadError;
use super::stored_block_cache::StoredMetadataBlockKind;
use loonfs_api::wire::manifest::{
    AccessRevisionRecord, ActiveDeletionRecord, ActiveDeletionRowAction, AttributesRevisionRecord,
    CommitReceiptRecord, ContentPublicationRecord, DeletedBinding, DirentryBindingRecord,
    InodeRecord, MetadataRow, MetadataSegmentRef, RevisionRecord, SubtreeTombstoneRecord,
    TombstoneRowAction,
};
use loonfs_api::wire::sst_blocks::{decode_data_block, DecodedDataBlock, SegmentIndexEntry};
use loonfs_api::wire::wal::{WalCommitPayload, WalDelta};
use loonfs_api::ActorId;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

/// Longest single ranged GET issued while bulk-reading a block span; longer
/// spans split into consecutive requests.
const MAX_BULK_LOAD_BYTES: u64 = 4 * 1024 * 1024;

pub(super) async fn load_segment_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entry: &SegmentIndexEntry,
) -> Result<Arc<DecodedDataBlock>, ManifestLoadError> {
    let handle = entry.block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Data, handle.offset);
    if let Some(DecodedMetadataSegmentBlock::Data { block, .. }) =
        memo.and_then(|memo| memo.get(&cache_key))
    {
        return Ok(block);
    }
    let fetch = || async {
        // Between the decoded cache above and the store below: a local copy
        // of the same stored bytes.
        if let Some(decoded) = stored_block_section(
            segment_cache,
            descriptor,
            StoredMetadataBlockKind::Data,
            &handle,
            decode_data_block,
        )
        .await
        {
            return Ok(decoded_data_cache_block(decoded));
        }
        let object_key = metadata_segment_object_key(descriptor);
        let bytes = load_section_bytes(
            store,
            &object_key,
            handle.offset,
            handle.stored_bytes as u64,
        )
        .await?;
        offer_stored_block(
            segment_cache,
            descriptor,
            StoredMetadataBlockKind::Data,
            &handle,
            &bytes,
        );
        Ok(decoded_data_cache_block(
            decode_data_block(&bytes, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?,
        ))
    };
    let block = match segment_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    if let Some(memo) = memo {
        memo.record(&cache_key, &block);
    }
    block.into_data(&metadata_segment_object_key(descriptor))
}

pub(super) fn decoded_data_cache_block(block: DecodedDataBlock) -> DecodedMetadataSegmentBlock {
    DecodedMetadataSegmentBlock::Data {
        decoded_bytes: decoded_manifest_block_weight(&block),
        block: Arc::new(block),
    }
}

/// Bulk path for wide selections: resolve each block against the memo and
/// shared decoded cache, group the blocks neither answered into consecutive
/// spans, and fetch each span with coalesced ranged GETs instead of one
/// request per block. Duplicate concurrent span fetches are possible and
/// benign; the narrow path keeps single-flight for the hot point lookups.
pub(super) async fn load_segment_data_block_span<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    let mut blocks: Vec<Option<Arc<DecodedDataBlock>>> = vec![None; entries.len()];
    // One probe key reused across the span: a fresh key per block would
    // clone the segment checksum once per block on every warm scan.
    let mut probe_key = segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Data, 0);
    for (position, entry) in entries.iter().enumerate() {
        let handle = entry.block;
        probe_key.block_offset = handle.offset;
        if let Some(DecodedMetadataSegmentBlock::Data { block, .. }) =
            memo.and_then(|memo| memo.get(&probe_key))
        {
            blocks[position] = Some(block);
            continue;
        }
        if let Some(cache) = segment_cache {
            if let Some(DecodedMetadataSegmentBlock::Data {
                block,
                decoded_bytes,
            }) = cache.get(&probe_key)
            {
                if let Some(memo) = memo {
                    // Keep shared hits just like point loads, so this view's
                    // later scans do not repeat shared-cache recency work.
                    memo.record(
                        &probe_key,
                        &DecodedMetadataSegmentBlock::Data {
                            block: Arc::clone(&block),
                            decoded_bytes,
                        },
                    );
                }
                blocks[position] = Some(block);
                continue;
            }
        }
    }

    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut cursor = 0;
    while cursor < entries.len() {
        if blocks[cursor].is_some() {
            cursor += 1;
            continue;
        }
        let start = cursor;
        let mut span_bytes = 0u64;
        while cursor < entries.len()
            && blocks[cursor].is_none()
            && span_bytes + u64::from(entries[cursor].block.stored_bytes) <= MAX_BULK_LOAD_BYTES
        {
            span_bytes += u64::from(entries[cursor].block.stored_bytes);
            cursor += 1;
        }
        // A single block larger than the fetch cap still fetches alone.
        if cursor == start {
            cursor += 1;
        }
        spans.push((start, cursor));
    }

    let mut span_decodes = vec![None; spans.len()];
    futures::future::try_join_all(spans.iter().zip(&mut span_decodes).map(
        |((start, end), winner_decodes)| {
            let span = &entries[*start..*end];
            async move {
                // Single-flight on the span's first block: the winning fetch
                // keeps what it decoded and publishes the whole span. A
                // concurrent loser resolves the remaining blocks from caches.
                let first_key = segment_block_cache_key(
                    descriptor,
                    MetadataSegmentBlockKind::Data,
                    span[0].block.offset,
                );
                let fetch = || async move {
                    load_and_publish_span(
                        store,
                        segment_cache,
                        memo,
                        descriptor,
                        span,
                        winner_decodes,
                    )
                    .await
                };
                match segment_cache {
                    Some(cache) => {
                        cache
                            .get_or_load(&first_key, || async {
                                Ok(fetch()
                                    .await?
                                    .expect("a cached span should return its first block"))
                            })
                            .await?;
                    }
                    None => {
                        fetch().await?;
                    }
                }
                Ok::<_, ManifestLoadError>(())
            }
        },
    ))
    .await?;

    for ((start, end), winner_decodes) in spans.iter().zip(span_decodes) {
        let Some(winner_decodes) = winner_decodes else {
            continue;
        };
        assert_eq!(
            winner_decodes.len(),
            end - start,
            "a span winner should retain every block it decoded"
        );
        for (slot, block) in blocks[*start..*end].iter_mut().zip(winner_decodes) {
            *slot = Some(block);
        }
    }

    // Only single-flight losers still need to resolve what the winner
    // published, with the existing point-load fallback on cache eviction.
    for (position, entry) in entries.iter().enumerate() {
        if blocks[position].is_some() {
            continue;
        }
        blocks[position] =
            Some(load_segment_data_block(store, segment_cache, memo, descriptor, entry).await?);
    }

    Ok(blocks
        .into_iter()
        .map(|block| block.expect("every selected block should be resolved above"))
        .collect())
}

/// Fetches one contiguous span with a single ranged GET, decodes every
/// block, keeps the winner's decodes, and publishes them to the per-view memo
/// and (when populating) the shared cache. Returns the first block's cache
/// entry for the single-flight cell.
async fn load_and_publish_span<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    span: &[SegmentIndexEntry],
    winner_decodes: &mut Option<Vec<Arc<DecodedDataBlock>>>,
) -> Result<Option<DecodedMetadataSegmentBlock>, ManifestLoadError> {
    let first = &span[0].block;
    let last = &span[span.len() - 1].block;
    let span_len = last.offset + u64::from(last.stored_bytes) - first.offset;
    let object_key = metadata_segment_object_key(descriptor);
    let bytes = load_section_bytes(store, &object_key, first.offset, span_len).await?;
    let mut first_block = None;
    let mut retained = Vec::with_capacity(span.len());
    for entry in span {
        let handle = entry.block;
        let begin = (handle.offset - first.offset) as usize;
        let stored = &bytes[begin..begin + handle.stored_bytes as usize];
        let decoded = Arc::new(
            decode_data_block(stored, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?,
        );
        if memo.is_some() || segment_cache.is_some() {
            let cache_key =
                segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Data, handle.offset);
            let cache_block = DecodedMetadataSegmentBlock::Data {
                decoded_bytes: decoded_manifest_block_weight(&decoded),
                block: Arc::clone(&decoded),
            };
            if let Some(memo) = memo {
                memo.record(&cache_key, &cache_block);
            }
            if let Some(cache) = segment_cache {
                // The first block is what the single-flight cell publishes;
                // inserting it here too keeps the whole span uniformly cached.
                cache.insert(cache_key, cache_block.clone());
            }
            if first_block.is_none() {
                first_block = Some(cache_block);
            }
        }
        retained.push(decoded);
    }
    *winner_decodes = Some(retained);
    Ok(first_block)
}

pub(super) fn decoded_manifest_block_weight(block: &DecodedDataBlock) -> usize {
    // Approximate decoded map/vector bookkeeping beyond owned row payloads.
    const BLOCK_ENTRY_OVERHEAD: usize = 64;
    const BLOCK_ALLOCATION_OVERHEAD: usize = 128;
    let row_weight = block
        .rows
        .iter()
        .zip(&block.row_keys)
        .map(|(row, row_key)| BLOCK_ENTRY_OVERHEAD + row_key.len() + row.decoded_weight())
        .sum::<usize>();
    row_weight.saturating_add(BLOCK_ALLOCATION_OVERHEAD)
}

/// The approximate decoded size of one row. The block cache and the
/// WAL-tail projection both charge rows with this.
pub(crate) trait DecodedRowWeight {
    fn decoded_weight(&self) -> usize;
}

// Approximate inline row storage and heap allocation metadata.
const FIXED_ROW_OVERHEAD: usize = 32;
const ALLOCATED_ROW_OVERHEAD: usize = 96;

fn actor_bytes(actor: &ActorId) -> usize {
    actor.as_str().len()
}

fn binding_bytes(binding: &DeletedBinding) -> usize {
    binding.name_key.as_str().len() + binding.display_name.as_str().len()
}

fn content_ref_bytes(content_ref: &loonfs_api::ContentRef) -> usize {
    content_ref.content_id.as_str().len() + content_ref.checksum.value.len()
}

impl DecodedRowWeight for MetadataRow {
    fn decoded_weight(&self) -> usize {
        match self {
            MetadataRow::Inode(record) => record.decoded_weight(),
            MetadataRow::DirentryBinding(record) => record.decoded_weight(),
            MetadataRow::FileRevision(record) => record.decoded_weight(),
            MetadataRow::Tombstone(record) => record.decoded_weight(),
            MetadataRow::ActiveDeletion(record) => record.decoded_weight(),
            MetadataRow::CommitReceipt(record) => record.decoded_weight(),
            MetadataRow::Commit(record) => record.decoded_weight(),
            MetadataRow::ContentPublication(record) => record.decoded_weight(),
            MetadataRow::AttributesRevision(record) => record.decoded_weight(),
            MetadataRow::AccessRevision(record) => record.decoded_weight(),
        }
    }
}

impl DecodedRowWeight for InodeRecord {
    fn decoded_weight(&self) -> usize {
        ALLOCATED_ROW_OVERHEAD + self.commit_id.as_str().len() + actor_bytes(&self.committed_by)
    }
}

impl DecodedRowWeight for DirentryBindingRecord {
    fn decoded_weight(&self) -> usize {
        ALLOCATED_ROW_OVERHEAD
            + self.name_key.as_str().len()
            + self.display_name().map_or(0, |name| name.as_str().len())
    }
}

impl DecodedRowWeight for RevisionRecord {
    fn decoded_weight(&self) -> usize {
        ALLOCATED_ROW_OVERHEAD
            + self.commit_id.as_str().len()
            + actor_bytes(&self.committed_by)
            + content_ref_bytes(&self.content_ref)
    }
}

impl DecodedRowWeight for SubtreeTombstoneRecord {
    fn decoded_weight(&self) -> usize {
        let action_bytes = match &self.action {
            TombstoneRowAction::Set { deleted_binding } => binding_bytes(deleted_binding),
            TombstoneRowAction::Revoke { .. } => 0,
        };
        ALLOCATED_ROW_OVERHEAD
            + self.commit_id.as_str().len()
            + actor_bytes(&self.committed_by)
            + action_bytes
    }
}

impl DecodedRowWeight for ActiveDeletionRecord {
    fn decoded_weight(&self) -> usize {
        match &self.action {
            ActiveDeletionRowAction::Listed {
                deleted_by,
                deleted_binding,
                ..
            } => ALLOCATED_ROW_OVERHEAD + actor_bytes(deleted_by) + binding_bytes(deleted_binding),
            ActiveDeletionRowAction::Removed { .. } => FIXED_ROW_OVERHEAD,
        }
    }
}

impl DecodedRowWeight for ContentPublicationRecord {
    fn decoded_weight(&self) -> usize {
        ALLOCATED_ROW_OVERHEAD + self.content_id.as_str().len()
    }
}

impl DecodedRowWeight for CommitReceiptRecord {
    fn decoded_weight(&self) -> usize {
        ALLOCATED_ROW_OVERHEAD
            + self.commit_id.as_str().len()
            + self.semantic_commit_fingerprint.as_str().len()
    }
}

impl DecodedRowWeight for WalCommitPayload {
    fn decoded_weight(&self) -> usize {
        let delta_bytes = self
            .deltas
            .iter()
            .map(|delta| {
                let variable_bytes = match &delta.delta {
                    WalDelta::CreateInode { .. } | WalDelta::RevokeSubtreeTombstone { .. } => 0,
                    WalDelta::BindDirentry {
                        name_key,
                        display_name,
                        ..
                    }
                    | WalDelta::UnbindDirentry {
                        name_key,
                        display_name,
                        ..
                    } => name_key.as_str().len() + display_name.as_str().len(),
                    WalDelta::AppendFileRevision { content_ref, .. } => {
                        content_ref_bytes(content_ref)
                    }
                    WalDelta::TombstoneSubtree {
                        deleted_binding, ..
                    } => binding_bytes(deleted_binding),
                    WalDelta::AppendAttributesRevision { attributes, .. } => {
                        attributes.logical_bytes()
                    }
                    WalDelta::AppendAccessRevision { grants, .. } => grants.logical_bytes(),
                };
                ALLOCATED_ROW_OVERHEAD + variable_bytes
            })
            .fold(0usize, usize::saturating_add);
        FIXED_ROW_OVERHEAD
            + ALLOCATED_ROW_OVERHEAD
            + self.commit_id.as_str().len()
            + actor_bytes(&self.committed_by)
            + self.semantic_commit_fingerprint.as_str().len()
            + self.message.as_ref().map_or(0, String::len)
            + delta_bytes
    }
}

impl DecodedRowWeight for AttributesRevisionRecord {
    fn decoded_weight(&self) -> usize {
        ALLOCATED_ROW_OVERHEAD
            + self.commit_id.as_str().len()
            + actor_bytes(&self.committed_by)
            + self.attributes.logical_bytes()
    }
}

impl DecodedRowWeight for AccessRevisionRecord {
    fn decoded_weight(&self) -> usize {
        ALLOCATED_ROW_OVERHEAD
            + self.commit_id.as_str().len()
            + actor_bytes(&self.committed_by)
            + self.grants.logical_bytes()
    }
}
