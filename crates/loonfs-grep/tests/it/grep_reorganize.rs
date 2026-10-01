//! Store requests and row preservation during grep reorganization.

use crate::common::GrepHost;
use bytes::Bytes;
use loonfs::engine::{Deadline, SegmentBlockKind};
use loonfs::SharedObjectStore;
use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_index_block, BuiltSegmentBlocks, SegmentBlocksBuilder,
};
use loonfs_api::{ChangeSeq, IndexSegmentId, InodeId, ManifestNo, NamespaceId, RevisionNo, RunNo};
use loonfs_grep::codec::{Gram, GramPosting, IndexRow};
use loonfs_grep::keyspace::{segment_key, segments_prefix};
use loonfs_grep::manifest::{
    load_current_grep_manifest, publish_grep_manifest, GrepIndexState, GrepIndexStatus,
    GrepManifestState, GrepSegmentRef,
};
use loonfs_grep::{
    DecodedGrepBlock, GramIndexBuildPolicy, GrepBlockCacheKey, GrepReorganizeOutcome,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::nonzero_usize;
use loonfs_test_support::stores::{ConcurrencyWatchStore, KeyPredicate, RecordingStore};
use std::sync::Arc;
use tempfile::tempdir;

fn many_block_segment() -> (Vec<IndexRow>, BuiltSegmentBlocks) {
    let mut state = 1u64;
    let mut rows = Vec::new();
    let mut builder = SegmentBlocksBuilder::default();
    for gram in 0..1024u32 {
        let mut inode_id = InodeId(0);
        let postings = (0..256)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                inode_id = InodeId(inode_id.0 + (state & 0xffff) + 1);
                GramPosting {
                    inode_id,
                    revision_no: RevisionNo((state >> 32).max(1)),
                }
            })
            .collect::<Vec<_>>();
        let bytes = gram.to_be_bytes();
        let row = IndexRow::gram_postings(Gram([bytes[1], bytes[2], bytes[3]]), &postings)
            .expect("valid posting row");
        builder
            .push(&row.row_key(), &row.filter_key(), &row)
            .expect("append row");
        rows.push(row);
    }
    (rows, builder.finish().expect("build segment"))
}

#[tokio::test]
async fn reorganization_reads_one_span_per_refill_and_preserves_rows() {
    let cold_data_gets = reorganize_with_cache(false).await;
    let warm_data_gets = reorganize_with_cache(true).await;
    assert_eq!(warm_data_gets, cold_data_gets);
}

async fn reorganize_with_cache(warm_cache: bool) -> usize {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("grep-span-refills").expect("namespace id");
    let keys = KeyPredicate::prefix(segments_prefix(&namespace_id));
    let watched = Arc::new(ConcurrencyWatchStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        keys.clone(),
    ));
    let recording = Arc::new(RecordingStore::new(Arc::clone(&watched), keys));
    let store: SharedObjectStore = recording.clone();
    let (expected_rows, built) = many_block_segment();
    let index = decode_index_block(&built.bytes[built.index.offset as usize..], &built.index)
        .expect("decode input index");
    assert!(index.len() >= 24, "each refill should cover many blocks");
    assert!((128 * 1024..2 * 1024 * 1024).contains(&built.filter.offset));
    assert!(
        index
            .iter()
            .map(|entry| u64::from(entry.block.decoded_bytes))
            .sum::<u64>()
            < 8 * 1024 * 1024
    );

    let mut segments = Vec::new();
    for run in 0..8 {
        let segment_id = IndexSegmentId::parse(format!("idx_{run:032x}")).expect("segment id");
        store
            .put(
                &segment_key(&namespace_id, &segment_id),
                Bytes::from(built.bytes.clone()),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write input");
        segments.push(GrepSegmentRef {
            segment_id,
            run_no: RunNo(run),
            level: 0,
            row_count: built.row_count,
            min_row_key: built.min_row_key.clone(),
            max_row_key: built.max_row_key.clone(),
            index_block: built.index,
            filter_block: built.filter,
            filter_inline: built.inline_filter_hex(),
        });
    }
    let state = GrepManifestState::new(
        namespace_id.clone(),
        ManifestNo(1),
        GrepIndexStatus::Active {
            built_through_seq: ChangeSeq(0),
            next_event_index: 0,
        },
        GrepIndexState {
            next_run_no: RunNo(8),
            reorganize: None,
        },
        segments.clone(),
    )
    .expect("valid manifest");
    publish_grep_manifest(
        &store,
        None,
        &state,
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .expect("publish inputs");
    let host = GrepHost::new(&store, "grep-span-refills").await;
    if warm_cache {
        for (position, segment) in segments.iter().enumerate() {
            if position % 2 == 0 {
                host.block_cache.insert(
                    GrepBlockCacheKey {
                        identity: segment.segment_id.to_string(),
                        block_kind: SegmentBlockKind::Index,
                        block_offset: built.index.offset,
                    },
                    DecodedGrepBlock::Index {
                        entries: Arc::new(index.clone()),
                        decoded_bytes: built.index.decoded_bytes as usize,
                    },
                );
            }
            for entry in index.iter().step_by(2) {
                let start = entry.block.offset as usize;
                let block = decode_data_block_rows::<IndexRow>(
                    &built.bytes[start..start + entry.block.stored_bytes as usize],
                    &entry.block,
                )
                .expect("decode cached input block");
                host.block_cache.insert(
                    GrepBlockCacheKey {
                        identity: segment.segment_id.to_string(),
                        block_kind: SegmentBlockKind::Data,
                        block_offset: entry.block.offset,
                    },
                    DecodedGrepBlock::Data {
                        block: Arc::new(block),
                        decoded_bytes: entry.block.decoded_bytes as usize,
                    },
                );
            }
        }
    }
    let cache_before = host.block_cache.stats();
    assert_eq!(
        cache_before.inserts,
        if warm_cache {
            4 + 8 * index.len().div_ceil(2)
        } else {
            0
        }
    );
    assert_eq!(cache_before.evictions, 0);
    recording.reset();
    let outcome = host
        .worker
        .reorganize_step(
            &namespace_id,
            GramIndexBuildPolicy {
                max_delta_runs: nonzero_usize(8),
                max_decoded_input_rows_per_step: nonzero_usize(16_384),
                ..Default::default()
            },
        )
        .await
        .expect("reorganize");
    assert!(
        matches!(outcome, GrepReorganizeOutcome::UnitPublished { merged_rows, completed: true, .. } if merged_rows == built.row_count * 8)
    );
    assert_eq!(host.block_cache.stats(), cache_before);
    let reads = watched.reads();
    assert!(reads.peak_in_flight <= 8, "{reads:?}");
    let gets = recording.take_gets();
    let data_gets = gets
        .iter()
        .filter(|(_, range)| range.is_some_and(|(start, _)| start == 0))
        .count();
    assert_eq!(gets.len(), 16, "eight index reads and eight data spans");
    for segment in segments {
        let key = segment_key(&namespace_id, &segment.segment_id);
        let ranges: Vec<_> = gets
            .iter()
            .filter(|(object_key, _)| object_key == &key)
            .map(|(_, range)| *range)
            .collect();
        assert_eq!(
            ranges,
            vec![
                Some((built.index.offset, built.bytes.len() as u64)),
                Some((0, built.filter.offset))
            ]
        );
    }

    let current = load_current_grep_manifest(&store, &namespace_id, crate::common::observation())
        .await
        .expect("load output manifest")
        .expect("output manifest");
    let mut actual_rows = Vec::new();
    for segment in current.manifest_state().segments() {
        assert_eq!(segment.level, 1);
        let bytes = store
            .get(&segment_key(&namespace_id, &segment.segment_id), None)
            .await
            .expect("read output")
            .expect("output exists");
        let index = decode_index_block(
            &bytes[segment.index_block.offset as usize..],
            &segment.index_block,
        )
        .expect("decode output index");
        for entry in index {
            let start = entry.block.offset as usize;
            let block = decode_data_block_rows::<IndexRow>(
                &bytes[start..start + entry.block.stored_bytes as usize],
                &entry.block,
            )
            .expect("decode output block");
            actual_rows.extend(block.rows);
        }
    }
    assert_eq!(actual_rows, expected_rows);
    data_gets
}
