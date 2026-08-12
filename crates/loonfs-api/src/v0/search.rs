//! Content search (grep) request and response shapes: the `query/v0`
//! plane's first operation (API spec, "Content search").

use crate::{AbsolutePath, ChangeSeq, CheckpointId, InodeId, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::xxh64;

/// One content-search request.
///
/// Every field but `pattern` is optional, and each one selects results. A
/// misspelled `case_insensitive` or `path_prefix` would decode to the default
/// and answer a different search than the caller asked for, with no sign that
/// anything was dropped, so unknown fields are rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct GrepRequest {
    /// The pattern, in the Rust `regex` crate's dialect (no backreferences
    /// or lookaround). Patterns that require no literal bytes are rejected
    /// with `query_unindexable` unless `allow_scan` is set.
    pub pattern: String,
    /// Match case-insensitively. Verification is exact; the index remains
    /// consulted through its case-folded grams.
    #[serde(default)]
    pub case_insensitive: bool,
    /// Restrict matches to files under this complete absolute path, resolved
    /// to a directory inode before candidates are filtered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<AbsolutePath>,
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
        seed = xxh64(
            self.path_prefix
                .as_ref()
                .map(AbsolutePath::as_str)
                .unwrap_or("")
                .as_bytes(),
            seed,
        );
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
    pub absolute_path: AbsolutePath,
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

/// Where a namespace's grep index is in its lifecycle.
///
/// Each phase contains only the fields valid for that phase. `Backfilling`
/// reports its target and current position. `Steady` reports how far the
/// index has been built. Clients should treat a namespace as searchable only
/// when the index is in the `Steady` phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum GrepIndexLifecycle {
    /// No index is maintained for this namespace.
    Disabled,
    /// The initial walk over a pinned checkpoint is running. Nothing is
    /// searchable yet.
    Backfilling {
        /// Namespace sequence the pinned checkpoint captured. Reaching it
        /// is what completes the backfill.
        target_seq: ChangeSeq,
        /// Inode the walk resumes strictly after. Absent before the first
        /// page.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor_inode_id: Option<InodeId>,
        /// Checkpoint pinning the state being walked.
        checkpoint_id: CheckpointId,
    },
    /// The index follows the change feed. Commits at or below the watermark
    /// are searchable.
    Steady {
        /// Sequence of the commit at the index cursor.
        built_through_seq: ChangeSeq,
        /// Offset of the next change event within `built_through_seq`, or
        /// zero when the whole commit is represented.
        #[serde(default, skip_serializing_if = "is_zero")]
        next_event_index: u32,
    },
}

impl GrepIndexLifecycle {
    /// Whether every commit at or below `target_seq` is represented.
    ///
    /// A watermark inside a commit (`next_event_index` above zero) has that
    /// commit only partly indexed, so it counts as reached only for earlier
    /// sequences.
    pub fn is_built_through(&self, target_seq: ChangeSeq) -> bool {
        match self {
            Self::Disabled | Self::Backfilling { .. } => false,
            Self::Steady {
                built_through_seq,
                next_event_index,
            } => {
                *built_through_seq > target_seq
                    || (*built_through_seq == target_seq && *next_event_index == 0)
            }
        }
    }
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// Result of enabling the grep index on a namespace (admin plane).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EnableGrepIndexResponse {
    /// Namespace whose grep root was enabled.
    pub namespace_id: NamespaceId,
    /// True when the namespace already carried an enabled grep root.
    pub already_enabled: bool,
    /// The durable lifecycle this call published or found.
    pub state: GrepIndexLifecycle,
}

/// The namespace's grep-index lifecycle and its cheap bookkeeping (admin
/// plane).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepIndexStatusResponse {
    /// Namespace the status describes.
    pub namespace_id: NamespaceId,
    /// Where the index is in its lifecycle.
    pub state: GrepIndexLifecycle,
    /// Next logical run ordinal the index will allocate.
    pub next_run_ordinal: u64,
    /// True while a partitioned segment reorganization is in progress.
    pub reorganize_pending: bool,
}

/// Result of disabling the grep index on a namespace (admin plane).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DisableGrepIndexResponse {
    /// Namespace whose grep root was disabled.
    pub namespace_id: NamespaceId,
    /// False when the namespace had no enabled grep root.
    pub was_enabled: bool,
}

/// One explicit grep-index garbage-collection pass (admin plane).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct GrepGcRequest {
    /// Reads this pass may spend before returning with a `next_cursor`.
    /// Omit to take the same per-pass default the runtime's own collection
    /// takes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// Opaque resume token returned as `next_cursor` by an earlier pass
    /// against the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Result of one explicit grep-index garbage-collection pass (admin plane).
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
    /// Present when the budget stopped the pass with keys left to examine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grep_paths_keep_the_plain_string_wire_shape() {
        let request = GrepRequest {
            pattern: "needle".to_owned(),
            case_insensitive: false,
            path_prefix: Some(AbsolutePath::parse("/docs").expect("path prefix")),
            cursor: None,
            limit: None,
            allow_stale: false,
            allow_scan: false,
        };
        assert_eq!(
            serde_json::to_value(request).expect("serialize grep request"),
            serde_json::json!({
                "pattern": "needle",
                "case_insensitive": false,
                "path_prefix": "/docs",
                "allow_stale": false,
                "allow_scan": false
            })
        );

        let found = GrepMatch {
            absolute_path: AbsolutePath::parse("/docs/a.txt").expect("match path"),
            inode_id: InodeId(2),
            revision_no: RevisionNo(3),
            line_number: 4,
            byte_offset: 5,
            line: "needle".to_owned(),
            line_truncated: false,
        };
        assert_eq!(
            serde_json::to_value(found).expect("serialize grep match"),
            serde_json::json!({
                "absolute_path": "/docs/a.txt",
                "inode_id": 2,
                "revision_no": 3,
                "line_number": 4,
                "byte_offset": 5,
                "line": "needle",
                "line_truncated": false
            })
        );
    }

    #[test]
    fn the_lifecycle_phases_never_share_a_sequence_field() {
        let backfilling = GrepIndexLifecycle::Backfilling {
            target_seq: ChangeSeq(9),
            cursor_inode_id: Some(InodeId(4)),
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                .expect("checkpoint id"),
        };
        assert_eq!(
            serde_json::to_value(&backfilling).expect("serialize backfilling"),
            serde_json::json!({
                "phase": "backfilling",
                "target_seq": 9,
                "cursor_inode_id": 4,
                "checkpoint_id": "chk_00000000000000000000000000000009"
            }),
            "a backfill reports its target and its walk, never a watermark"
        );

        assert_eq!(
            serde_json::to_value(GrepIndexLifecycle::Steady {
                built_through_seq: ChangeSeq(9),
                next_event_index: 0,
            })
            .expect("serialize steady"),
            serde_json::json!({"phase": "steady", "built_through_seq": 9}),
            "a steady index reports its watermark and no target"
        );

        assert_eq!(
            serde_json::to_value(GrepIndexLifecycle::Disabled).expect("serialize disabled"),
            serde_json::json!({"phase": "disabled"})
        );
    }

    #[test]
    fn only_a_steady_index_has_built_through_a_sequence() {
        let backfilling = GrepIndexLifecycle::Backfilling {
            target_seq: ChangeSeq(9),
            cursor_inode_id: None,
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                .expect("checkpoint id"),
        };
        assert!(
            !backfilling.is_built_through(ChangeSeq(0)),
            "a backfill has indexed nothing until it turns steady"
        );
        assert!(!GrepIndexLifecycle::Disabled.is_built_through(ChangeSeq(0)));

        let steady = |built_through_seq, next_event_index| GrepIndexLifecycle::Steady {
            built_through_seq,
            next_event_index,
        };
        assert!(steady(ChangeSeq(9), 0).is_built_through(ChangeSeq(9)));
        assert!(steady(ChangeSeq(9), 0).is_built_through(ChangeSeq(8)));
        assert!(!steady(ChangeSeq(9), 0).is_built_through(ChangeSeq(10)));
        // A watermark inside a commit leaves the rest of that commit
        // unindexed, so only earlier sequences count as reached.
        assert!(!steady(ChangeSeq(9), 3).is_built_through(ChangeSeq(9)));
        assert!(steady(ChangeSeq(9), 3).is_built_through(ChangeSeq(8)));
    }

    #[test]
    fn grep_path_prefix_validates_during_deserialization() {
        let encoded = serde_json::json!({
            "pattern": "needle",
            "path_prefix": "relative/path"
        });

        assert!(serde_json::from_value::<GrepRequest>(encoded).is_err());
    }

    /// Every optional field on a search body selects results, so a typo would
    /// answer a different question than the caller asked and say nothing
    /// about it.
    #[test]
    fn search_request_bodies_reject_unknown_fields() {
        serde_json::from_value::<GrepRequest>(serde_json::json!({
            "pattern": "needle",
            "case_insensitive": true,
            "path_prefix": "/docs",
            "limit": 10,
            "allow_stale": true,
            "allow_scan": true
        }))
        .expect("the same body without a typo decodes");

        for body in [
            serde_json::json!({"pattern": "needle", "case_insensitve": true}),
            serde_json::json!({"pattern": "needle", "caseInsensitive": true}),
            serde_json::json!({"pattern": "needle", "pathPrefix": "/docs"}),
            serde_json::json!({"pattern": "needle", "allow_scans": true}),
        ] {
            assert!(
                serde_json::from_value::<GrepRequest>(body.clone()).is_err(),
                "an unknown field decoded instead of failing the search: {body}"
            );
        }

        serde_json::from_value::<GrepGcRequest>(serde_json::json!({"max_objects": 8}))
            .expect("the same collection body without a typo decodes");
        assert!(
            serde_json::from_value::<GrepGcRequest>(serde_json::json!({"maxObjects": 8})).is_err()
        );
    }
}
