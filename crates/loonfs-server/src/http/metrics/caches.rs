//! The cache stats that a scrape writes as metric families: the metadata
//! cache, its SSD tier, and the grep block cache.

use super::prometheus::{write_counter, write_gauge, SeriesLabels, NO_LABELS};
use loonfs::engine::DecodedBlockCacheStats;
use loonfs::metrics::{RESULT_HIT, RESULT_MISS};
use loonfs::{MetadataCacheStats, SsdSegmentBlocksStats};

/// Returns the `hit` and `miss` series of a lookup counter.
fn lookups(hits: usize, misses: usize) -> [(SeriesLabels<'static>, usize); 2] {
    [
        (&[("result", RESULT_HIT)], hits),
        (&[("result", RESULT_MISS)], misses),
    ]
}

/// The counters of one tier of the segment cache.
struct SegmentTierCounts {
    hits: usize,
    misses: usize,
    inserts: usize,
    evictions: usize,
    rejections: usize,
}

/// The label sets of one tier's series: the tier alone, and the tier with
/// each lookup result.
type TierLabels = [SeriesLabels<'static>; 3];

const MEMORY_TIER: TierLabels = [
    &[("tier", "memory")],
    &[("tier", "memory"), ("result", RESULT_HIT)],
    &[("tier", "memory"), ("result", RESULT_MISS)],
];

const SSD_TIER: TierLabels = [
    &[("tier", "ssd")],
    &[("tier", "ssd"), ("result", RESULT_HIT)],
    &[("tier", "ssd"), ("result", RESULT_MISS)],
];

/// Writes the segment cache's counters, with one series for each tier
/// under a `tier` label, and the segment cache's gauges.
///
/// The SSD tier's series appear only on a server with an SSD tier, because
/// zeros would suggest that the server has an idle SSD tier.
fn render_segment_cache(
    rendered: &mut String,
    memory: SegmentTierCounts,
    ssd: Option<SsdSegmentBlocksStats>,
    retained_decoded_bytes: usize,
) {
    let mut tiers = vec![(MEMORY_TIER, memory)];
    if let Some(ssd) = ssd {
        let counts = SegmentTierCounts {
            hits: ssd.hits,
            misses: ssd.misses,
            inserts: ssd.inserts,
            evictions: ssd.evictions,
            rejections: ssd.rejections,
        };
        tiers.push((SSD_TIER, counts));
    }
    let gets: Vec<_> = tiers
        .iter()
        .flat_map(|([_, hit, miss], counts)| [(*hit, counts.hits), (*miss, counts.misses)])
        .collect();
    write_counter(
        rendered,
        "loonfs_segment_cache_gets",
        "Metadata segment cache lookups, by tier and outcome",
        &gets,
    );
    let per_tier = |count: fn(&SegmentTierCounts) -> usize| -> Vec<_> {
        tiers
            .iter()
            .map(|([tier, ..], counts)| (*tier, count(counts)))
            .collect()
    };
    write_counter(
        rendered,
        "loonfs_segment_cache_inserts",
        "Blocks inserted into a tier of the metadata segment cache",
        &per_tier(|counts| counts.inserts),
    );
    write_counter(
        rendered,
        "loonfs_segment_cache_evictions",
        "Blocks evicted from a tier of the metadata segment cache",
        &per_tier(|counts| counts.evictions),
    );
    write_counter(
        rendered,
        "loonfs_segment_cache_rejections",
        "Blocks that a tier of the metadata segment cache didn't keep",
        &per_tier(|counts| counts.rejections),
    );
    write_gauge(
        rendered,
        "loonfs_segment_cache_retained_decoded_bytes",
        "Bytes currently charged to metadata segment blocks in memory, including keys and per-entry bookkeeping",
        retained_decoded_bytes,
    );
    if let Some(ssd) = ssd {
        write_gauge(
            rendered,
            "loonfs_segment_cache_retained_stored_bytes",
            "Bytes currently charged to metadata segment blocks on the SSD tier, at least 4 KiB for each block, including per-entry bookkeeping",
            ssd.stored_bytes,
        );
    }
}

/// Writes the metadata cache's stats as counters and gauges, in one family
/// for each cache.
///
/// The stats are the one source of these values, and the runtime's recorder
/// doesn't register them. Listing every field makes this code fail to
/// compile when `MetadataCacheStats` gains a field, which ensures new
/// statistics are considered for export. The fields bound to `_` aren't
/// exported.
pub(super) fn render_metadata_cache(rendered: &mut String, stats: &MetadataCacheStats) {
    let MetadataCacheStats {
        segment_hits,
        segment_misses,
        segment_inserts,
        segment_evictions,
        segment_rejections,
        segment_filter_skips,
        segment_filter_false_positives,
        segment_bytes,
        segment_ssd,
        manifest_hits,
        manifest_misses,
        manifest_inserts,
        manifest_evictions,
        manifest_rejections,
        manifest_bytes,
        head_hits,
        head_misses,
        head_evictions,
        head_evicted_bytes,
        head_rejections,
        head_rejected_bytes,
        head_bytes,
        wal_tail_hits,
        wal_tail_misses,
        wal_tail_inserts,
        wal_tail_evictions,
        wal_tail_evicted_bytes,
        wal_tail_rejections,
        wal_tail_rejected_bytes,
        wal_tail_bytes,
        content_object_hits,
        content_object_misses,
        content_object_inserts,
        content_object_evictions,
        content_object_bytes,
        completed_upload_session_hits: _,
        completed_upload_session_misses: _,
        completed_upload_session_evictions: _,
        completed_upload_session_bytes: _,
    } = *stats;
    render_segment_cache(
        rendered,
        SegmentTierCounts {
            hits: segment_hits,
            misses: segment_misses,
            inserts: segment_inserts,
            evictions: segment_evictions,
            rejections: segment_rejections,
        },
        segment_ssd,
        segment_bytes,
    );
    for (name, description, hits, misses) in [
        (
            "loonfs_manifest_cache_gets",
            "Manifest cache lookups, by outcome",
            manifest_hits,
            manifest_misses,
        ),
        (
            "loonfs_head_cache_gets",
            "Namespace head cache lookups, by outcome",
            head_hits,
            head_misses,
        ),
        (
            "loonfs_wal_tail_cache_gets",
            "WAL-tail cache lookups, by outcome",
            wal_tail_hits,
            wal_tail_misses,
        ),
        (
            "loonfs_content_object_cache_gets",
            "Content object cache lookups, by outcome",
            content_object_hits,
            content_object_misses,
        ),
    ] {
        write_counter(rendered, name, description, &lookups(hits, misses));
    }
    for (name, description, value) in [
        (
            "loonfs_segment_cache_filter_skips",
            "Segments skipped after their filter ruled out a lookup",
            segment_filter_skips,
        ),
        (
            "loonfs_segment_cache_filter_false_positives",
            "Filter admissions that matched no metadata rows",
            segment_filter_false_positives,
        ),
        (
            "loonfs_manifest_cache_inserts",
            "Manifests inserted into the manifest cache",
            manifest_inserts,
        ),
        (
            "loonfs_manifest_cache_evictions",
            "Manifests evicted to stay within the manifest budget",
            manifest_evictions,
        ),
        (
            "loonfs_manifest_cache_rejections",
            "Manifests heavier than the whole manifest budget",
            manifest_rejections,
        ),
        (
            "loonfs_head_cache_evictions",
            "Namespace heads evicted to stay within the head budget",
            head_evictions,
        ),
        (
            "loonfs_head_cache_evicted_decoded_bytes",
            "Bytes charged to evicted namespace heads, including keys and per-entry bookkeeping",
            head_evicted_bytes,
        ),
        (
            "loonfs_head_cache_rejections",
            "Namespace heads heavier than the whole head budget",
            head_rejections,
        ),
        (
            "loonfs_head_cache_rejected_decoded_bytes",
            "Bytes charged to rejected namespace heads, including keys and per-entry bookkeeping",
            head_rejected_bytes,
        ),
        (
            "loonfs_wal_tail_cache_inserts",
            "WAL-tail projections inserted into the WAL-tail cache",
            wal_tail_inserts,
        ),
        (
            "loonfs_wal_tail_cache_evictions",
            "WAL-tail projections evicted to stay within the WAL-tail budget",
            wal_tail_evictions,
        ),
        (
            "loonfs_wal_tail_cache_evicted_decoded_bytes",
            "Bytes charged to evicted WAL-tail projections, including keys and per-entry bookkeeping",
            wal_tail_evicted_bytes,
        ),
        (
            "loonfs_wal_tail_cache_rejections",
            "WAL-tail projections heavier than the whole WAL-tail budget",
            wal_tail_rejections,
        ),
        (
            "loonfs_wal_tail_cache_rejected_decoded_bytes",
            "Bytes charged to rejected WAL-tail projections, including keys and per-entry bookkeeping",
            wal_tail_rejected_bytes,
        ),
        (
            "loonfs_content_object_cache_inserts",
            "Content objects inserted into the content object cache",
            content_object_inserts,
        ),
        (
            "loonfs_content_object_cache_evictions",
            "Content objects evicted to stay within the content object budget",
            content_object_evictions,
        ),
    ] {
        write_counter(rendered, name, description, &[(NO_LABELS, value)]);
    }
    for (name, description, value) in [
        (
            "loonfs_manifest_cache_retained_decoded_bytes",
            "Bytes currently charged to manifests, including keys and per-entry bookkeeping",
            manifest_bytes,
        ),
        (
            "loonfs_head_cache_retained_decoded_bytes",
            "Bytes currently charged to namespace heads, including keys and per-entry bookkeeping",
            head_bytes,
        ),
        (
            "loonfs_wal_tail_cache_retained_decoded_bytes",
            "Bytes currently charged to WAL-tail projections, including keys and per-entry bookkeeping",
            wal_tail_bytes,
        ),
        (
            "loonfs_content_object_cache_retained_decoded_bytes",
            "Bytes currently charged to content objects, including keys and per-entry bookkeeping",
            content_object_bytes,
        ),
    ] {
        write_gauge(rendered, name, description, value);
    }
}

/// Writes the grep cache's stats as counters and a gauge.
///
/// Listing every field makes this code fail to compile when
/// `DecodedBlockCacheStats` gains a field, which ensures new statistics are
/// considered for export. The fields bound to `_` aren't exported.
pub(super) fn render_grep_block_cache(rendered: &mut String, stats: &DecodedBlockCacheStats) {
    let DecodedBlockCacheStats {
        hits,
        misses,
        inserts,
        evictions,
        evicted_decoded_bytes: _,
        rejections: _,
        rejected_decoded_bytes: _,
        cached_decoded_bytes,
    } = *stats;
    write_counter(
        rendered,
        "loonfs_grep_cache_gets",
        "Grep cache lookups, by outcome",
        &lookups(hits, misses),
    );
    write_counter(
        rendered,
        "loonfs_grep_cache_inserts",
        "Decoded grep manifests and blocks inserted into the grep cache",
        &[(NO_LABELS, inserts)],
    );
    write_counter(
        rendered,
        "loonfs_grep_cache_evictions",
        "Decoded grep manifests and blocks evicted to stay within the grep cache budget",
        &[(NO_LABELS, evictions)],
    );
    write_gauge(
        rendered,
        "loonfs_grep_cache_retained_decoded_bytes",
        "Bytes currently charged to decoded grep manifests and blocks, including keys and per-entry bookkeeping",
        cached_decoded_bytes,
    );
}
