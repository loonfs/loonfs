//! The cache stats that a scrape writes as metric families: the metadata
//! cache, its SSD tier, and the grep block cache.

use super::prometheus::{write_counter, write_gauge, SeriesLabels, NO_LABELS};
use loonfs::engine::DecodedBlockCacheStats;
use loonfs::metrics::{RESULT_HIT, RESULT_MISS};
use loonfs::{MetadataCacheStats, SegmentFilterStats, SsdSegmentBlocksStats};

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

/// The names of one cache's metric families, and the nouns that their
/// descriptions use.
struct CacheNames {
    /// The cache's part of each metric name, such as `manifest` in
    /// `loonfs_manifest_cache_gets`.
    metric: &'static str,
    /// The cache, as the description of its lookups starts.
    lookups: &'static str,
    /// The cache's entries, at the start of a description.
    entries_title: &'static str,
    /// The cache's entries, inside a description.
    entries: &'static str,
    /// The cache, as the descriptions of its inserts and its budget name it.
    budget: &'static str,
}

const MANIFESTS: CacheNames = CacheNames {
    metric: "manifest",
    lookups: "Manifest",
    entries_title: "Manifests",
    entries: "manifests",
    budget: "manifest",
};

const HEADS: CacheNames = CacheNames {
    metric: "head",
    lookups: "Namespace head",
    entries_title: "Namespace heads",
    entries: "namespace heads",
    budget: "head",
};

const WAL_TAILS: CacheNames = CacheNames {
    metric: "wal_tail",
    lookups: "WAL-tail",
    entries_title: "WAL-tail projections",
    entries: "WAL-tail projections",
    budget: "WAL-tail",
};

const CONTENT_OBJECTS: CacheNames = CacheNames {
    metric: "content_object",
    lookups: "Content object",
    entries_title: "Content objects",
    entries: "content objects",
    budget: "content object",
};

const GREP_BLOCKS: CacheNames = CacheNames {
    metric: "grep",
    lookups: "Grep",
    entries_title: "Decoded grep manifests and blocks",
    entries: "decoded grep manifests and blocks",
    budget: "grep",
};

/// Writes one cache's lookups, inserts, evictions, and rejections as
/// counters, and the bytes it keeps as a gauge.
///
/// Listing every field makes this code fail to compile when
/// `DecodedBlockCacheStats` gains a field, which ensures new statistics are
/// considered for export.
fn render_cache(rendered: &mut String, names: &CacheNames, stats: &DecodedBlockCacheStats) {
    let DecodedBlockCacheStats {
        hits,
        misses,
        inserts,
        evictions,
        evicted_decoded_bytes,
        rejections,
        rejected_decoded_bytes,
        cached_decoded_bytes,
    } = *stats;
    let CacheNames {
        metric,
        lookups: lookups_label,
        entries_title,
        entries,
        budget,
    } = names;
    write_counter(
        rendered,
        &format!("loonfs_{metric}_cache_gets"),
        &format!("{lookups_label} cache lookups, by outcome"),
        &lookups(hits, misses),
    );
    for (family, description, value) in [
        (
            "inserts",
            format!("{entries_title} inserted into the {budget} cache"),
            inserts,
        ),
        (
            "evictions",
            format!("{entries_title} evicted to stay within the {budget} budget"),
            evictions,
        ),
        (
            "evicted_decoded_bytes",
            format!("Bytes charged to evicted {entries}, including keys and per-entry bookkeeping"),
            evicted_decoded_bytes,
        ),
        (
            "rejections",
            format!("{entries_title} heavier than the whole {budget} budget"),
            rejections,
        ),
        (
            "rejected_decoded_bytes",
            format!(
                "Bytes charged to rejected {entries}, including keys and per-entry bookkeeping"
            ),
            rejected_decoded_bytes,
        ),
    ] {
        write_counter(
            rendered,
            &format!("loonfs_{metric}_cache_{family}"),
            &description,
            &[(NO_LABELS, value)],
        );
    }
    write_gauge(
        rendered,
        &format!("loonfs_{metric}_cache_retained_decoded_bytes"),
        &format!("Bytes currently charged to {entries}, including keys and per-entry bookkeeping"),
        cached_decoded_bytes,
    );
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
        segment_blocks,
        segment_filter:
            SegmentFilterStats {
                skips,
                false_positives,
            },
        segment_ssd,
        manifests,
        heads,
        wal_tails,
        content_objects,
        completed_upload_sessions: _,
    } = *stats;
    render_segment_cache(
        rendered,
        SegmentTierCounts {
            hits: segment_blocks.hits,
            misses: segment_blocks.misses,
            inserts: segment_blocks.inserts,
            evictions: segment_blocks.evictions,
            rejections: segment_blocks.rejections,
        },
        segment_ssd,
        segment_blocks.cached_decoded_bytes,
    );
    for (name, description, value) in [
        (
            "loonfs_segment_cache_filter_skips",
            "Segments skipped after their filter ruled out a lookup",
            skips,
        ),
        (
            "loonfs_segment_cache_filter_false_positives",
            "Filter admissions that matched no metadata rows",
            false_positives,
        ),
    ] {
        write_counter(rendered, name, description, &[(NO_LABELS, value)]);
    }
    for (names, stats) in [
        (&MANIFESTS, manifests),
        (&HEADS, heads),
        (&WAL_TAILS, wal_tails),
        (&CONTENT_OBJECTS, content_objects),
    ] {
        render_cache(rendered, names, &stats);
    }
}

/// Writes the grep cache's stats as counters and a gauge.
pub(super) fn render_grep_block_cache(rendered: &mut String, stats: &DecodedBlockCacheStats) {
    render_cache(rendered, &GREP_BLOCKS, stats);
}
