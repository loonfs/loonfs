//! Content search (grep) request and response shapes: the `query/v0`
//! plane's first operation (API spec, "Content search").

use crate::{ChangeSeq, InodeId, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::xxh64;

/// One content-search request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepRequest {
    /// The pattern, in the Rust `regex` crate's dialect (no backreferences
    /// or lookaround). Patterns that require no literal bytes are rejected
    /// with `query_unindexable` unless `allow_scan` is set.
    pub pattern: String,
    /// Match case-insensitively. Verification is exact; the index remains
    /// consulted through its case-folded grams.
    #[serde(default)]
    pub case_insensitive: bool,
    /// Restrict matches to files under this absolute path prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
    /// Resume cursor from a previous page. The cursor resumes strictly
    /// after the last candidate the issuing page finished scanning and is
    /// bound to that page's request; each page is evaluated against the
    /// namespace head at page time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Maximum matches per page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// When the unindexed tail exceeds the scan budget, return
    /// indexed-only results (reported via `tail_scanned: false`) instead
    /// of failing with `index_lagging`.
    #[serde(default)]
    pub allow_stale: bool,
    /// Permit a capped exhaustive scan when the pattern yields no required
    /// grams. Refused beyond the server's scan budget.
    #[serde(default)]
    pub allow_scan: bool,
}

impl GrepRequest {
    /// Fingerprint of the fields that select results, binding cursors to
    /// the request that issued them. Not a durable format: cursors are
    /// opaque and short-lived, so this may change between builds.
    pub fn fingerprint(&self) -> u64 {
        let mut seed = xxh64(self.pattern.as_bytes(), 0);
        seed = xxh64(self.path_prefix.as_deref().unwrap_or("").as_bytes(), seed);
        let flags = [
            u8::from(self.case_insensitive),
            u8::from(self.allow_stale),
            u8::from(self.allow_scan),
        ];
        xxh64(&flags, seed)
    }
}

/// One line-oriented match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepMatch {
    /// The file's absolute path, derived at the snapshot.
    pub absolute_path: String,
    /// Durable identity of the matched file.
    pub inode_id: InodeId,
    /// The matched revision (the newest visible one at the snapshot).
    pub revision_no: RevisionNo,
    /// One-based line number of the match.
    pub line_number: u64,
    /// Byte offset of the match within the file.
    pub byte_offset: u64,
    /// The matching line, truncated to the server's line cap.
    pub line: String,
    /// True when `line` was truncated.
    #[serde(default)]
    pub line_truncated: bool,
}

/// One content-search page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepResponse {
    /// Namespace searched.
    pub namespace_id: NamespaceId,
    /// Sequence this page was evaluated at. Pages are evaluated against
    /// the namespace head at page time; the cursor is an ordering resume,
    /// not a snapshot pin.
    pub head_seq: ChangeSeq,
    /// Commits at or below this sequence were answered from the index.
    pub built_through_seq: ChangeSeq,
    /// True when revisions after `built_through_seq` were scanned
    /// exhaustively; false only when `allow_stale` skipped them.
    pub tail_scanned: bool,
    /// Matches in ascending `(inode_id, byte_offset)` order. A page may
    /// return fewer matches than its limit and still carry a cursor: the
    /// per-page verified-candidate budget bounds how much content one
    /// request reads, whatever the plan's false-positive rate.
    pub matches: Vec<GrepMatch>,
    /// Present when another page follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Result of enabling the gram index on a namespace (admin plane).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EnableGramsIndexResponse {
    /// Namespace the feature entry was published for.
    pub namespace_id: NamespaceId,
    /// Backfill covers commits at or below this sequence; later commits
    /// arrive through WAL replay once backfill completes.
    pub built_through_seq: ChangeSeq,
    /// True when the namespace already carried the feature entry.
    pub already_enabled: bool,
}

/// Result of disabling the gram index on a namespace (admin plane).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DisableGramsIndexResponse {
    /// Namespace the feature entry was removed from.
    pub namespace_id: NamespaceId,
    /// False when the namespace had no feature entry to remove.
    pub was_enabled: bool,
}

/// Result of one explicit gram-index garbage-collection pass (admin plane).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepGcResponse {
    /// Namespace whose grep-owned keyspace was inspected.
    pub namespace_id: NamespaceId,
    /// Unreferenced grep segments deleted after the grace window.
    pub deleted_segments: u64,
    /// Other unreferenced grep objects deleted after the grace window.
    pub deleted_other_objects: u64,
    /// Whether an absent or tombstoned namespace had extension state reaped.
    pub namespace_reaped: bool,
    /// Young or concurrently revived candidates retained by the pass.
    pub retained_candidates: u64,
    /// Whether unreadable namespace or grep state forced conservative retention.
    pub namespace_degraded: bool,
}
