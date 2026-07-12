//! Content search (grep) request and response shapes: the `query/v0`
//! plane's first operation (API spec, "Content search").

use crate::{ChangeSeq, InodeId, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

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
    /// Resume cursor from a previous page; pages share one snapshot.
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
    /// Snapshot sequence every page of this search reads.
    pub head_seq: ChangeSeq,
    /// Commits at or below this sequence were answered from the index.
    pub built_through_seq: ChangeSeq,
    /// True when revisions after `built_through_seq` were scanned
    /// exhaustively; false only when `allow_stale` skipped them.
    pub tail_scanned: bool,
    /// Matches in ascending `(inode_id, byte_offset)` order.
    pub matches: Vec<GrepMatch>,
    /// Present when another page follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
