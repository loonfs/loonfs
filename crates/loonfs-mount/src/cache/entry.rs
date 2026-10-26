//! One cache file's entry: its state, partial or complete, and the bytes it
//! stores.

use crate::cache_file::CacheFile;
use crate::ranges::RangeSet;
use loonfs_types::{Checksum, ContentRef};
use std::ops::Range;
use std::sync::Arc;

/// A prefix of a cache file with a verified checksum, so verifying
/// the file after an append reads only the appended bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedPrefix {
    pub(crate) size_bytes: u64,
    pub(crate) checksum: Checksum,
}

/// Whether a partial file's verification is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verification {
    Idle,
    Running,
}

/// Whether a partial file is being filled for the first time, or again after
/// its checksum didn't match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FillAttempt {
    First,
    Refetch,
}

/// A cache file that's being filled.
#[derive(Debug)]
pub(crate) struct PartialFile {
    /// The revision that the file fills.
    pub(crate) target: ContentRef,
    pub(crate) present: RangeSet,
    /// The ranges that a queued or running fetch writes.
    pub(crate) requested: RangeSet,
    pub(crate) verified_prefix: Option<VerifiedPrefix>,
    pub(crate) attempt: FillAttempt,
    pub(crate) verification: Verification,
}

/// The state of one cache file.
#[derive(Debug)]
pub(crate) enum CacheEntryState {
    Partial(PartialFile),
    /// The file is complete up to `size_bytes`, and those bytes match
    /// `checksum`.
    Complete {
        size_bytes: u64,
        checksum: Checksum,
    },
    /// The file didn't match its checksum after a refetch, so reads of it
    /// fail until the last descriptor that uses it closes.
    Failed,
}

impl CacheEntryState {
    /// Returns the partial file, or `None` for a complete or failed file.
    pub(crate) fn partial_mut(&mut self) -> Option<&mut PartialFile> {
        match self {
            Self::Partial(partial) => Some(partial),
            Self::Complete { .. } | Self::Failed => None,
        }
    }
}

/// One cache file and its state.
#[derive(Debug)]
pub(crate) struct CacheEntry {
    pub(crate) file: Arc<CacheFile>,
    pub(crate) state: CacheEntryState,
    /// When the entry was last used, as a count of uses, for eviction.
    pub(super) last_used: u64,
}

impl CacheEntry {
    /// Returns whether reads can have the bytes of `range`. A file that's
    /// fetched again after a mismatch has none until it matches.
    pub(crate) fn contains(&self, range: &Range<u64>) -> bool {
        match &self.state {
            CacheEntryState::Partial(partial) => {
                partial.attempt == FillAttempt::First && partial.present.contains(range)
            }
            CacheEntryState::Complete { size_bytes, .. } => range.end <= *size_bytes,
            CacheEntryState::Failed => false,
        }
    }

    /// Returns whether the file is being fetched again after a mismatch, so
    /// its reads wait for every byte of the file.
    pub(crate) fn is_refetching(&self) -> bool {
        matches!(&self.state, CacheEntryState::Partial(partial) if partial.attempt == FillAttempt::Refetch)
    }

    /// Returns the bytes that the file occupies: a partial file's present
    /// ranges, or a complete file's size.
    pub(crate) fn stored_bytes(&self) -> u64 {
        match &self.state {
            CacheEntryState::Partial(partial) => partial.present.len_bytes(),
            CacheEntryState::Complete { size_bytes, .. } => *size_bytes,
            CacheEntryState::Failed => 0,
        }
    }
}
