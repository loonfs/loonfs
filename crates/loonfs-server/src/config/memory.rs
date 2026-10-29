//! Memory sizing: the fixed costs, the headroom, and the pools that the
//! server sizes under its memory limit.

use super::validate::validate_positive;
use super::ServerConfig;
use crate::{Result, ServerError};
use loonfs::{MetadataCache, SsdSegmentBlocks};
use loonfs_types::config::ConfigFieldError;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

const MIB: usize = 1024 * 1024;

/// The memory that glibc keeps on Linux in a quiet phase beyond what the same
/// workload uses on macOS.
///
/// The value is for `MALLOC_ARENA_MAX=2`, `MALLOC_TRIM_THRESHOLD_=131072`, and
/// `MALLOC_MMAP_THRESHOLD_=131072`, which the server image sets. In the lab's
/// `local-workloads-10k` run with those settings (`20261020T052717Z`), the app
/// drive's quiet phase stayed at 266 MiB against 76 MiB on macOS, and the
/// peaks were 10% to 32% above macOS (122 MiB against 111 MiB for sessions
/// turns, 323 MiB against 256 MiB for the app drive, and 298 MiB against
/// 226 MiB for the sandbox). With glibc's defaults (`20261020T024314Z`), those
/// four phases reached 307, 153, 769, and 715 MiB. The headroom is this floor
/// plus a quarter of the pools, which covers each of those peaks.
const ALLOCATOR_HEADROOM_FLOOR: usize = 200 * MIB;

// The label of the SSD tier's term in `fixed` states these three values.
const _: () = assert!(
    loonfs::SSD_WRITE_QUEUE_BYTES == 64 * MIB
        && loonfs::ASSUMED_STORED_BLOCK_BYTES == 4 * 1024
        && loonfs::SSD_INDEX_ENTRY_BYTES == 168
);

/// Where a memory term's size came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemorySource {
    /// The config sets the size.
    Set,
    /// The server derived the size from the memory limit.
    Derived,
    /// The size is the runtime's default.
    Default,
}

impl MemorySource {
    /// Returns the source's spelling in the `server memory` log line.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Set => "set",
            Self::Derived => "derived",
            Self::Default => "default",
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemoryTerm {
    name: &'static str,
    pub(crate) bytes: usize,
    source: MemorySource,
}

impl MemoryTerm {
    fn new(
        name: &'static str,
        explicit: Option<usize>,
        derived: Option<usize>,
        default: usize,
    ) -> Self {
        let (bytes, source) = match (explicit, derived) {
            (Some(bytes), _) => (bytes, MemorySource::Set),
            (None, Some(bytes)) => (bytes, MemorySource::Derived),
            (None, None) => (default, MemorySource::Default),
        };
        Self {
            name,
            bytes,
            source,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemorySizing {
    limit: Option<MemoryTerm>,
    fixed: [(&'static str, u128); 7],
    headroom: usize,
    managed: usize,
    segment: MemoryTerm,
    manifest: MemoryTerm,
    head: MemoryTerm,
    wal_tail: MemoryTerm,
    content_object: MemoryTerm,
    pub(crate) grep: MemoryTerm,
    pub(super) read_working: MemoryTerm,
    pub(super) admitted: MemoryTerm,
    pub(super) merge_input: MemoryTerm,
    compactions: usize,
    serves_grep: bool,
}

impl MemorySizing {
    fn terms(&self) -> [(&MemoryTerm, usize); 9] {
        [
            (&self.segment, 1),
            (&self.manifest, 1),
            (&self.head, 1),
            (&self.wal_tail, 1),
            (&self.content_object, 1),
            (&self.grep, usize::from(self.serves_grep)),
            (&self.read_working, 1),
            (&self.admitted, 1),
            (&self.merge_input, self.compactions),
        ]
    }

    /// Builds the metadata cache with the sized limits, and with `ssd` as
    /// the SSD tier of its segment blocks if the config has one.
    pub(crate) fn metadata_cache(&self, ssd: Option<SsdSegmentBlocks>) -> MetadataCache {
        let builder = MetadataCache::builder()
            .max_segment_bytes(self.segment.bytes)
            .max_manifest_bytes(self.manifest.bytes)
            .max_head_bytes(self.head.bytes)
            .max_wal_tail_bytes(self.wal_tail.bytes)
            .max_content_object_bytes(self.content_object.bytes);
        match ssd {
            Some(ssd) => builder.ssd_segment_blocks(ssd),
            None => builder,
        }
        .build()
    }

    pub(crate) fn log(&self) {
        if let Some(limit) = &self.limit {
            tracing::info!(
                term = limit.name,
                bytes = limit.bytes,
                source = limit.source.as_str(),
                "server memory"
            );
            for (term, bytes) in self.fixed.into_iter().chain([
                ("headroom", self.headroom as u128),
                ("managed", self.managed as u128),
            ]) {
                tracing::info!(term, bytes = %bytes, source = "derived", "server memory");
            }
        } else {
            tracing::info!("server memory is unsized; no memory limit was configured or found");
        }
        for (term, count) in self.terms() {
            tracing::info!(
                term = term.name,
                bytes = term.bytes,
                source = term.source.as_str(),
                count,
                "server memory"
            );
        }
    }

    fn validate(&self) -> Result<()> {
        let Some(limit) = &self.limit else {
            return Ok(());
        };
        let fixed: u128 = self.fixed.iter().map(|(_, bytes)| bytes).sum();
        let pools = self.terms().iter().fold(0u128, |total, (term, count)| {
            total.saturating_add(term.bytes as u128 * *count as u128)
        });
        let total = fixed
            .saturating_add(self.headroom as u128)
            .saturating_add(pools);
        if self.managed >= 256 * MIB && total <= limit.bytes as u128 {
            return Ok(());
        }
        let mut terms: Vec<String> = self
            .fixed
            .iter()
            .map(|(name, bytes)| format!("{name} = {bytes}"))
            .collect();
        terms.extend(
            self.terms()
                .map(|(term, count)| format!("`{}` = {} x {count}", term.name, term.bytes)),
        );
        Err(ServerError::Config(ConfigFieldError::Invalid {
            field: "memory_limit_bytes",
            reason: format!(
                "memory terms must fit the limit and managed memory must be at least {} bytes; limit = {}, headroom = {}, managed = {}, total = {}; {}",
                256 * MIB, limit.bytes, self.headroom, self.managed, total, terms.join(", ")
            ),
        }))
    }
}

impl ServerConfig {
    /// Sizes the server's memory pools under `memory_limit_bytes`, or
    /// under `cgroup_limit` when the config sets no limit, and checks that
    /// the pools fit.
    pub(super) fn memory_sizing(&self, cgroup_limit: Option<usize>) -> Result<MemorySizing> {
        let limit = self.memory_limit_bytes.or(cgroup_limit);
        let folds = self
            .max_concurrent_folds
            .unwrap_or(loonfs::DEFAULT_MAX_CONCURRENT_FOLDS)
            .get();
        let compactions = self
            .max_concurrent_compactions
            .unwrap_or(loonfs::DEFAULT_MAX_CONCURRENT_COMPACTIONS)
            .get();
        let content_writes = self
            .max_concurrent_content_writes
            .unwrap_or(loonfs::DEFAULT_MAX_CONCURRENT_CONTENT_WRITES)
            .get();
        let fixed = self.fixed_memory_terms(folds, compactions, content_writes);
        let fixed_bytes: u128 = fixed.iter().map(|(_, bytes)| bytes).sum();
        // The pools share four fifths of what the limit has left after
        // `fixed` and the floor, so the fifth that the headroom takes is a
        // quarter of the pools.
        let headroom = limit.map_or(0, |limit| {
            let rest =
                (limit as u128).saturating_sub(fixed_bytes + ALLOCATOR_HEADROOM_FLOOR as u128);
            ALLOCATOR_HEADROOM_FLOOR + (rest / 5) as usize
        });
        // The budgets of manifests and heads aren't shares of managed memory,
        // because the memory that those entries need depends on the
        // namespaces, not on the host.
        let manifest = MemoryTerm::new(
            "metadata_cache.max_manifest_bytes",
            self.metadata_cache.max_manifest_bytes,
            None,
            loonfs::DEFAULT_MAX_MANIFEST_BYTES,
        );
        let head = MemoryTerm::new(
            "metadata_cache.max_head_bytes",
            self.metadata_cache.max_head_bytes,
            None,
            loonfs::DEFAULT_MAX_HEAD_BYTES,
        );
        let managed = (limit.unwrap_or(0) as u128).saturating_sub(
            fixed_bytes + headroom as u128 + manifest.bytes as u128 + head.bytes as u128,
        ) as usize;
        let share =
            |percent: usize| limit.map(|_| (managed as u128 * percent as u128 / 100) as usize);
        let serves_grep = self.grep.mode.serves();
        let memory = MemorySizing {
            limit: limit.map(|bytes| {
                MemoryTerm::new(
                    "memory_limit_bytes",
                    self.memory_limit_bytes,
                    Some(bytes),
                    0,
                )
            }),
            fixed,
            headroom,
            managed,
            segment: MemoryTerm::new(
                "metadata_cache.max_segment_bytes",
                self.metadata_cache.max_segment_bytes,
                share(if serves_grep { 30 } else { 40 }),
                loonfs::DEFAULT_MAX_SEGMENT_BYTES,
            ),
            manifest,
            head,
            wal_tail: MemoryTerm::new(
                "metadata_cache.max_wal_tail_bytes",
                self.metadata_cache.max_wal_tail_bytes,
                share(10),
                loonfs::DEFAULT_MAX_WAL_TAIL_BYTES,
            ),
            content_object: MemoryTerm::new(
                "metadata_cache.max_content_object_bytes",
                self.metadata_cache.max_content_object_bytes,
                share(10),
                loonfs::DEFAULT_MAX_CONTENT_OBJECT_BYTES,
            ),
            grep: MemoryTerm::new(
                "grep.max_cache_bytes",
                self.grep.max_cache_bytes,
                share(if serves_grep { 10 } else { 0 }),
                loonfs_grep::DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
            ),
            read_working: MemoryTerm::new(
                "max_read_working_bytes",
                self.max_read_working_bytes,
                share(20),
                loonfs::DEFAULT_MAX_READ_WORKING_BYTES,
            ),
            admitted: MemoryTerm::new(
                "max_admitted_bytes",
                self.max_admitted_bytes.map(NonZeroUsize::get),
                share(10),
                64 * MIB,
            ),
            merge_input: MemoryTerm::new(
                "max_merge_input_bytes",
                self.max_merge_input_bytes,
                share(10).map(|bytes| bytes / compactions),
                loonfs_types::format::segment_blocks::DEFAULT_MAX_COMPACTION_INPUT_BYTES.get(),
            ),
            compactions,
            serves_grep,
        };
        memory.validate()?;
        validate_positive(
            "max_merge_input_bytes",
            memory.merge_input.bytes as u64,
            None,
        )?;
        Ok(memory)
    }

    /// Returns the memory terms that don't depend on the memory limit: the
    /// process, connections, transfers, segment output, content writes,
    /// completed upload sessions, and the SSD tier's index.
    fn fixed_memory_terms(
        &self,
        folds: usize,
        compactions: usize,
        content_writes: usize,
    ) -> [(&'static str, u128); 7] {
        [
            ("process", (128 * MIB) as u128),
            (
                "connections (`max_connections` x 472 KiB)",
                self.max_connections.get() as u128 * 472 * 1024,
            ),
            (
                "transfers ((`max_concurrent_uploads` + `max_concurrent_downloads`) x 8 MiB)",
                (self.max_concurrent_uploads.get() as u128
                    + self.max_concurrent_downloads.get() as u128)
                    * (8 * MIB) as u128,
            ),
            (
                "segment output ((`max_concurrent_folds` + `max_concurrent_compactions`) x 32 MiB)",
                (folds as u128 + compactions as u128)
                    * loonfs::MAX_PENDING_SEGMENT_BYTES as u128,
            ),
            (
                "content writes (`max_concurrent_content_writes` x 16 MiB, or 72 MiB on R2)",
                content_writes as u128
                    * u128::from(self.store.kind().max_assembly_buffer_bytes()),
            ),
            (
                "completed upload sessions",
                loonfs::DEFAULT_MAX_COMPLETED_UPLOAD_SESSION_BYTES as u128,
            ),
            (
                "ssd tier (64 MiB write queue + `metadata_cache.ssd.max_segment_bytes` / 4 KiB x 168 B)",
                self.metadata_cache.ssd.as_ref().map_or(0, |ssd| {
                    SsdSegmentBlocks::memory_bytes(ssd.max_segment_bytes) as u128
                }),
            ),
        ]
    }
}

/// Reads the first cgroup memory limit that one of `paths` sets, or
/// `None` when none sets a finite limit.
pub(super) fn read_cgroup_memory_limit(paths: &[&Path]) -> Option<usize> {
    paths.iter().find_map(|path| {
        let bytes = fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()?;
        (bytes < 1 << 60)
            .then(|| usize::try_from(bytes).ok())
            .flatten()
    })
}

#[cfg(test)]
mod tests;
