//! Content search requests and responses for the v0 HTTP API.

use crate::{AbsolutePath, ChangeSeq, CheckpointId, InodeId, NamespaceId, RevisionNo, RunNo};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::xxh64;

/// One content-search request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepRequest {
    /// The Rust `regex` pattern, without backreferences or lookaround, limited to
    /// 1,024 UTF-8 bytes and requiring `allow_scan` when it has no required literal
    /// bytes.
    pub pattern: String,
    /// Whether matching ignores case.
    pub case_insensitive: bool,
    /// The absolute directory path that limits matching to its descendants.
    pub path_prefix: Option<AbsolutePath>,
    /// The cursor from the previous page, bound to that request and evaluated against
    /// the current namespace head.
    pub cursor: Option<String>,
    /// Whether index lag returns indexed-only results with `tail_scanned: false`
    /// instead of `index_lagging`.
    pub allow_stale: bool,
    /// Whether patterns without required literal bytes may use a scan capped by the
    /// server's budget.
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
                .map_or("", AbsolutePath::as_str)
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
    pub path: AbsolutePath,
    /// Durable identity of the matched file.
    #[serde(with = "crate::public_inode_id")]
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
    pub line_truncated: bool,
}

/// One content-search page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepResponse {
    /// Namespace searched.
    pub namespace_id: NamespaceId,
    /// The namespace head sequence used to evaluate this page.
    pub head_seq: ChangeSeq,
    /// Commits at or below this sequence were answered from the index.
    pub built_through_seq: ChangeSeq,
    /// Whether revisions after `built_through_seq` were scanned exhaustively.
    pub tail_scanned: bool,
    /// The matches in ascending `(inode_id, byte_offset)` order.
    pub matches: Vec<GrepMatch>,
    /// Present when another page follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// The grep index lifecycle for a namespace.
///
/// A namespace is searchable only when the lifecycle is `Active`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GrepIndexLifecycle {
    /// No index is maintained for this namespace.
    Disabled,
    /// An initial scan of a pinned checkpoint that is not yet searchable.
    Backfilling {
        /// The namespace sequence that completes the backfill when reached.
        target_seq: ChangeSeq,
        /// The inode after which the scan resumes, or `None` before the first page.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        cursor_inode_id: Option<InodeId>,
        /// Checkpoint pinning the state being walked.
        checkpoint_id: CheckpointId,
    },
    /// An index following the change feed through its searchable watermark.
    Active {
        /// Sequence of the commit at the index cursor.
        built_through_seq: ChangeSeq,
        /// The next change-event offset within `built_through_seq`, or zero when the
        /// whole commit is indexed.
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
            Self::Active {
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

/// The maintenance status of a namespace's grep index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepIndex {
    /// Namespace the status describes.
    pub namespace_id: NamespaceId,
    /// Where the index is in its lifecycle.
    #[serde(flatten)]
    pub lifecycle: GrepIndexLifecycle,
    /// Run number the index allocates next.
    pub next_run_no: RunNo,
    /// True while a partitioned segment reorganization is in progress.
    pub reorganize_pending: bool,
}

/// One explicit grep index garbage-collection pass (maintenance API group).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct GrepGcRequest {
    /// The maximum reads for this pass, or `None` for the server default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// The opaque `next_cursor` returned by an earlier pass for the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Result of one explicit grep index garbage-collection pass (maintenance API group).
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
        let found = GrepMatch {
            path: AbsolutePath::parse("/docs/a.txt").expect("match path"),
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
                "path": "/docs/a.txt",
                "inode_id": "ino_2",
                "revision_no": 3,
                "line_number": 4,
                "byte_offset": 5,
                "line": "needle",
                "line_truncated": false
            })
        );
    }

    #[test]
    fn lifecycle_statuses_never_share_a_sequence_field() {
        let backfilling = GrepIndexLifecycle::Backfilling {
            target_seq: ChangeSeq(9),
            cursor_inode_id: Some(InodeId(4)),
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                .expect("checkpoint id"),
        };
        assert_eq!(
            serde_json::to_value(&backfilling).expect("serialize backfilling"),
            serde_json::json!({
                "status": "backfilling",
                "target_seq": 9,
                "cursor_inode_id": "ino_4",
                "checkpoint_id": "chk_00000000000000000000000000000009"
            }),
            "a backfill reports its target and its walk, never a watermark"
        );

        assert_eq!(
            serde_json::to_value(GrepIndexLifecycle::Active {
                built_through_seq: ChangeSeq(9),
                next_event_index: 0,
            })
            .expect("serialize active"),
            serde_json::json!({"status": "active", "built_through_seq": 9}),
            "an active index reports its watermark and no target"
        );

        assert_eq!(
            serde_json::to_value(GrepIndexLifecycle::Disabled).expect("serialize disabled"),
            serde_json::json!({"status": "disabled"})
        );
    }

    #[test]
    fn only_an_active_index_has_built_through_a_sequence() {
        let backfilling = GrepIndexLifecycle::Backfilling {
            target_seq: ChangeSeq(9),
            cursor_inode_id: None,
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                .expect("checkpoint id"),
        };
        assert!(
            !backfilling.is_built_through(ChangeSeq(0)),
            "a backfill has indexed nothing until it turns active"
        );
        assert!(!GrepIndexLifecycle::Disabled.is_built_through(ChangeSeq(0)));

        let active = |built_through_seq, next_event_index| GrepIndexLifecycle::Active {
            built_through_seq,
            next_event_index,
        };
        assert!(active(ChangeSeq(9), 0).is_built_through(ChangeSeq(9)));
        assert!(active(ChangeSeq(9), 0).is_built_through(ChangeSeq(8)));
        assert!(!active(ChangeSeq(9), 0).is_built_through(ChangeSeq(10)));
        // A watermark inside a commit leaves the rest of that commit
        // unindexed, so only earlier sequences count as reached.
        assert!(!active(ChangeSeq(9), 3).is_built_through(ChangeSeq(9)));
        assert!(active(ChangeSeq(9), 3).is_built_through(ChangeSeq(8)));
    }

    #[test]
    fn grep_index_status_flattens_its_lifecycle() {
        assert_eq!(
            serde_json::to_value(GrepIndex {
                namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                lifecycle: GrepIndexLifecycle::Active {
                    built_through_seq: ChangeSeq(12),
                    next_event_index: 0,
                },
                next_run_no: RunNo(3),
                reorganize_pending: false,
            })
            .expect("serialize active status"),
            serde_json::json!({
                "namespace_id": "demo",
                "status": "active",
                "built_through_seq": 12,
                "next_run_no": 3,
                "reorganize_pending": false
            })
        );

        assert_eq!(
            serde_json::to_value(GrepIndex {
                namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                lifecycle: GrepIndexLifecycle::Backfilling {
                    target_seq: ChangeSeq(12),
                    cursor_inode_id: Some(InodeId(4)),
                    checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000009")
                        .expect("checkpoint id"),
                },
                next_run_no: RunNo(1),
                reorganize_pending: false,
            })
            .expect("serialize backfilling status"),
            serde_json::json!({
                "namespace_id": "demo",
                "status": "backfilling",
                "target_seq": 12,
                "cursor_inode_id": "ino_4",
                "checkpoint_id": "chk_00000000000000000000000000000009",
                "next_run_no": 1,
                "reorganize_pending": false
            })
        );
    }

    #[test]
    fn grep_gc_request_bodies_reject_unknown_fields() {
        serde_json::from_value::<GrepGcRequest>(serde_json::json!({"max_objects": 8}))
            .expect("the same collection body without a typo decodes");
        assert!(
            serde_json::from_value::<GrepGcRequest>(serde_json::json!({"maxObjects": 8})).is_err()
        );
    }
}
