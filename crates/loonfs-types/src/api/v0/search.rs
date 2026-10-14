//! Content search requests and responses for the v0 HTTP API.

use crate::{AbsolutePath, ChangeSeq, InodeId, NamespaceId, PinId, RevisionNo, RunNo};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::xxh64;

/// One content search request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepRequest {
    /// The search pattern, in Rust `regex` syntax without backreferences or lookaround. The pattern
    /// can be at most 1,024 UTF-8 bytes, and a pattern with no required literal bytes needs
    /// `allow_scan`.
    pub pattern: String,
    /// Whether matching ignores case.
    pub case_insensitive: bool,
    /// The absolute directory path that limits matching to its descendants. It can't be combined
    /// with `inode_id`.
    pub path_prefix: Option<AbsolutePath>,
    /// The inode that limits matching to its descendants. It can't be combined with `path_prefix`.
    pub inode_id: Option<InodeId>,
    /// The cursor from the previous page. The cursor is tied to the request that returned it, and
    /// the server evaluates it against the current namespace head.
    pub cursor: Option<String>,
    /// Whether a lagging index returns results from the index alone, with `tail_scanned: false`,
    /// instead of returning `index_lagging`.
    pub allow_stale: bool,
    /// Whether a pattern without required literal bytes can use a scan, which the server's budget
    /// limits.
    pub allow_scan: bool,
}

impl GrepRequest {
    /// Computes the fingerprint of the fields that select results, binding
    /// cursors to the request that issued them.
    ///
    /// Not a durable format: cursors are opaque and short-lived, so this may
    /// change between builds.
    pub fn fingerprint(&self) -> u64 {
        let mut seed = xxh64(self.pattern.as_bytes(), 0);
        seed = xxh64(
            self.path_prefix
                .as_ref()
                .map_or("", AbsolutePath::as_str)
                .as_bytes(),
            seed,
        );
        seed = xxh64(
            &self.inode_id.map_or(0, |inode_id| inode_id.0).to_le_bytes(),
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

/// One match and the line that contains it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepMatch {
    /// The file's absolute path in the view that the search read.
    #[serde(deserialize_with = "crate::path::deserialize_derived_path")]
    pub path: AbsolutePath,
    /// The matched file's durable inode ID.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// The matched revision, which is the file's newest revision visible to the search.
    pub revision_no: RevisionNo,
    /// The match's line number, counting from 1.
    pub line_number: u64,
    /// The match's byte offset within the file.
    pub byte_offset: u64,
    /// The matching line, truncated to the server's maximum line length.
    pub line: String,
    /// Whether the server truncated `line`.
    pub line_truncated: bool,
}

/// One page of content search results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepResponse {
    /// The namespace that was searched.
    pub namespace_id: NamespaceId,
    /// The namespace head sequence that the server used to evaluate this page.
    pub head_seq: ChangeSeq,
    /// The last sequence that the index covers. The server searched the commits at or below this
    /// sequence through the index.
    pub built_through_seq: ChangeSeq,
    /// Whether the server scanned every revision after `built_through_seq`.
    pub tail_scanned: bool,
    /// The matches, in ascending `(inode_id, byte_offset)` order.
    pub matches: Vec<GrepMatch>,
    /// The cursor for the next page. It's present when another page follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// The status of a namespace's grep index.
///
/// A namespace can be searched only when the status is `Active`.
///
/// The durable record of this status is `GrepIndexStatus` in `loonfs-grep`. The two types share a
/// name because they describe the same status. Their fields differ because each type follows the
/// conventions of its own surface. This response tags variants with `status` and writes inode IDs
/// in their public `ino_` form, and the durable record tags variants with `kind` and writes inode
/// IDs as numbers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GrepIndexStatus {
    /// No index is maintained for this namespace.
    Disabled,
    /// An index that is running its initial scan of a pinned checkpoint and can't be searched yet.
    Backfilling {
        /// The namespace sequence that completes the backfill when the index reaches it.
        captured_seq: ChangeSeq,
        /// The inode that the scan resumes after. It's absent before the first page.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        cursor_inode_id: Option<InodeId>,
        /// The checkpoint that pins the state being scanned.
        checkpoint_id: PinId,
    },
    /// An index that follows the change feed and can be searched through its watermark.
    Active {
        /// The sequence of the commit at the index cursor.
        built_through_seq: ChangeSeq,
        /// The offset of the next change event within `built_through_seq`. It's zero when the whole
        /// commit is indexed.
        next_event_index: u32,
    },
    /// A status from a newer server. This variant doesn't include the status's own fields.
    #[serde(other)]
    Unknown,
}

impl GrepIndexStatus {
    /// Reports whether every commit at or below `target_seq` is represented.
    ///
    /// A watermark inside a commit (`next_event_index` above zero) has that
    /// commit only partly indexed, so it counts as reached only for earlier
    /// sequences.
    pub fn is_built_through(&self, target_seq: ChangeSeq) -> bool {
        match self {
            Self::Disabled | Self::Backfilling { .. } | Self::Unknown => false,
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

/// The maintenance status of a namespace's grep index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GrepIndex {
    /// The namespace that the status describes.
    pub namespace_id: NamespaceId,
    /// The index's status, with the fields that the status records.
    #[serde(flatten)]
    pub status: GrepIndexStatus,
    /// The next run number that the index allocates.
    pub next_run_no: RunNo,
    /// Whether a partitioned segment reorganization is in progress.
    pub reorganize_pending: bool,
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
        let backfilling = GrepIndexStatus::Backfilling {
            captured_seq: ChangeSeq(9),
            cursor_inode_id: Some(InodeId(4)),
            checkpoint_id: PinId::parse("pin_00000000000000000001-0000000000000009")
                .expect("checkpoint id"),
        };
        assert_eq!(
            serde_json::to_value(&backfilling).expect("serialize backfilling"),
            serde_json::json!({
                "status": "backfilling",
                "captured_seq": 9,
                "cursor_inode_id": "ino_4",
                "checkpoint_id": "pin_00000000000000000001-0000000000000009"
            }),
            "a backfill reports its target and its walk, never a watermark"
        );

        assert_eq!(
            serde_json::to_value(GrepIndexStatus::Active {
                built_through_seq: ChangeSeq(9),
                next_event_index: 0,
            })
            .expect("serialize active"),
            serde_json::json!({"status": "active", "built_through_seq": 9, "next_event_index": 0}),
            "an active index reports its watermark and no target"
        );

        assert_eq!(
            serde_json::to_value(GrepIndexStatus::Disabled).expect("serialize disabled"),
            serde_json::json!({"status": "disabled"})
        );
    }

    #[test]
    fn only_an_active_index_has_built_through_a_sequence() {
        let backfilling = GrepIndexStatus::Backfilling {
            captured_seq: ChangeSeq(9),
            cursor_inode_id: None,
            checkpoint_id: PinId::parse("pin_00000000000000000001-0000000000000009")
                .expect("checkpoint id"),
        };
        assert!(
            !backfilling.is_built_through(ChangeSeq(0)),
            "a backfill has indexed nothing until it turns active"
        );
        assert!(!GrepIndexStatus::Disabled.is_built_through(ChangeSeq(0)));

        let active = |built_through_seq, next_event_index| GrepIndexStatus::Active {
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
    fn a_grep_index_status_from_a_newer_server_decodes_as_unknown() {
        let index: GrepIndex = serde_json::from_value(serde_json::json!({
            "namespace_id": "demo",
            "status": "rebuilding",
            "rebuild_seq": 9,
            "next_run_no": 3,
            "reorganize_pending": false
        }))
        .expect("a newer status decodes");
        assert_eq!(index.status, GrepIndexStatus::Unknown);
        assert!(!index.status.is_built_through(ChangeSeq(0)));
    }

    #[test]
    fn grep_index_status_flattens_its_lifecycle() {
        assert_eq!(
            serde_json::to_value(GrepIndex {
                namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                status: GrepIndexStatus::Active {
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
                "next_event_index": 0,
                "next_run_no": 3,
                "reorganize_pending": false
            })
        );

        assert_eq!(
            serde_json::to_value(GrepIndex {
                namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                status: GrepIndexStatus::Backfilling {
                    captured_seq: ChangeSeq(12),
                    cursor_inode_id: Some(InodeId(4)),
                    checkpoint_id: PinId::parse("pin_00000000000000000001-0000000000000009")
                        .expect("checkpoint id"),
                },
                next_run_no: RunNo(1),
                reorganize_pending: false,
            })
            .expect("serialize backfilling status"),
            serde_json::json!({
                "namespace_id": "demo",
                "status": "backfilling",
                "captured_seq": 12,
                "cursor_inode_id": "ino_4",
                "checkpoint_id": "pin_00000000000000000001-0000000000000009",
                "next_run_no": 1,
                "reorganize_pending": false
            })
        );
    }
}
