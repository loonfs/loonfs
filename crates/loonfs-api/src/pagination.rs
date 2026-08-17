//! Pagination: page-size policy, typed page envelopes, and the opaque
//! cursors each paginated endpoint round-trips.

use crate::capability::{LIMIT_PAGINATION_DEFAULT, LIMIT_PAGINATION_MAX};
use crate::{ChangeSeq, InodeId, NameKey, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use thiserror::Error;

/// Contract page size for endpoints that omit a caller-supplied limit.
///
/// This value is deliberately fixed and advertised through capabilities.
pub const DEFAULT_PAGE_LIMIT: u32 = 1_000;
/// Contract maximum accepted page size.
///
/// This value is deliberately fixed and advertised through capabilities.
pub const DEFAULT_MAX_PAGE_LIMIT: u32 = 1_000;

/// Wire cursor format version.
pub const PAGE_CURSOR_VERSION: u8 = 1;

/// A validated page size selected from a caller request and a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EffectiveLimit(NonZeroU32);

impl EffectiveLimit {
    /// Creates an effective limit from a non-zero value.
    pub fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// Returns the numeric page size.
    pub fn get(self) -> u32 {
        self.0.get()
    }

    /// Returns the page size as a `usize` for vector reservations and counters.
    pub fn as_usize(self) -> usize {
        self.0.get() as usize
    }

    /// Returns the number of items an engine should try to read to detect a next page.
    pub fn limit_plus_one(self) -> usize {
        self.as_usize().saturating_add(1)
    }

    /// Truncates an overfilled page and builds a cursor from its last row.
    pub fn finish_page<R, C>(self, rows: &mut Vec<R>, cursor: impl FnOnce(&R) -> C) -> Option<C> {
        if rows.len() <= self.as_usize() {
            return None;
        }
        rows.truncate(self.as_usize());
        rows.last().map(cursor)
    }
}

/// Fixed pagination contract for endpoints with potentially unbounded results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaginationPolicy {
    default_limit: NonZeroU32,
    max_limit: NonZeroU32,
}

impl PaginationPolicy {
    /// Returns the page size applied when callers omit `limit`.
    pub fn default_limit(self) -> NonZeroU32 {
        self.default_limit
    }

    /// Returns the largest accepted caller-supplied `limit`.
    pub fn max_limit(self) -> NonZeroU32 {
        self.max_limit
    }

    /// Resolves a caller-supplied limit into the enforced page size.
    pub fn resolve_limit(self, requested: Option<u32>) -> Result<EffectiveLimit, LimitError> {
        match requested {
            None => Ok(EffectiveLimit(self.default_limit)),
            Some(value) if value > self.max_limit.get() => Err(LimitError::ExceedsMax {
                requested: value,
                max_limit: self.max_limit.get(),
            }),
            Some(value) => NonZeroU32::new(value)
                .map(EffectiveLimit)
                .ok_or(LimitError::Zero),
        }
    }

    /// Returns the advisory capability-document limits for this policy.
    pub fn capability_limits(self) -> BTreeMap<String, u64> {
        BTreeMap::from([
            (
                LIMIT_PAGINATION_DEFAULT.to_owned(),
                u64::from(self.default_limit.get()),
            ),
            (
                LIMIT_PAGINATION_MAX.to_owned(),
                u64::from(self.max_limit.get()),
            ),
        ])
    }
}

impl Default for PaginationPolicy {
    fn default() -> Self {
        // These are protocol constants, not configuration defaults. Keeping
        // them together here makes every consumer enforce and advertise the
        // same deliberate contract.
        let default_limit = const { NonZeroU32::new(DEFAULT_PAGE_LIMIT).unwrap() };
        let max_limit = const { NonZeroU32::new(DEFAULT_MAX_PAGE_LIMIT).unwrap() };
        Self {
            default_limit,
            max_limit,
        }
    }
}

/// Invalid caller-supplied page size.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum LimitError {
    /// The caller supplied `limit=0`.
    #[error("limit must be greater than zero")]
    Zero,
    /// The caller supplied a limit larger than the active policy allows.
    #[error("limit `{requested}` exceeds max limit `{max_limit}`")]
    ExceedsMax {
        /// Page size supplied by the caller.
        requested: u32,
        /// Largest page size allowed by the active policy.
        max_limit: u32,
    },
}

/// Typed request envelope for internal runtime/core page methods.
///
/// This is not a direct wire response type. HTTP handlers parse public query
/// fields into this shape after validating `limit` and decoding `cursor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest<C> {
    /// Enforced page size.
    pub limit: EffectiveLimit,
    /// Optional decoded endpoint cursor.
    pub cursor: Option<C>,
}

/// Typed result envelope for internal runtime/core page methods.
///
/// This is not a direct wire response type. HTTP handlers encode
/// `next_cursor` into the public response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T, C> {
    /// Returned items.
    pub items: Vec<T>,
    /// Cursor for the next page, if another page is available.
    pub next_cursor: Option<C>,
}

/// Cursor for one directory listing position.
///
/// Directory pagination advances in canonical `name_key` order. The cursor
/// is an ordering resume, not a snapshot pin: any head at or past `head_seq`
/// serves the next page, resuming strictly after `last_name_key` — the same
/// forward-only drift grep cursors tolerate.
///
/// The cursor intentionally contains only the minting head (`head_seq`),
/// listed directory identity (`directory_inode_id`), and resume position
/// (`last_name_key`). HTTP clients must pass the URL namespace and `path` on
/// every page. Runtime/server code resolves that path at the current head
/// and rejects the cursor unless it names `directory_inode_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryPageCursor {
    /// Head sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// Directory inode resolved at `head_seq`.
    // The wire field is frozen as `dir_inode_id` in page cursor version 1.
    #[serde(rename = "dir_inode_id")]
    pub directory_inode_id: InodeId,
    /// Last canonical name key returned to the client.
    pub last_name_key: NameKey,
}

impl PageCursor for DirectoryPageCursor {
    const KIND: &'static str = "directory";
}

/// Cursor for one file revision listing position.
///
/// Revision pagination advances in newest-first revision order for one file
/// inode. Like directory and grep cursors, it is an ordering resume that
/// tolerates forward head drift; it includes the minting head plus the last
/// returned row's complete ordering identity so ties stay unambiguous.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRevisionsPageCursor {
    /// Head sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// File inode whose revisions are being listed.
    pub inode_id: InodeId,
    /// Last revision number returned to the client.
    pub last_revision_no: RevisionNo,
    /// Namespace sequence that created the last returned revision.
    pub last_committed_seq: ChangeSeq,
    /// WAL delta index that created the last returned revision.
    pub last_revision_delta_index: u32,
}

impl PageCursor for FileRevisionsPageCursor {
    const KIND: &'static str = "file_revisions";
}

/// Cursor for one trash listing position.
///
/// Trash pagination advances oldest deletion first, in ascending
/// `(deleted_at_seq, root_inode_id)` order — the order the derived
/// active-deletion family is keyed in. Like every cursor, it is an ordering
/// resume tolerating forward head drift: the next page evaluates at whatever
/// head is loaded and continues strictly after the deletion generation named
/// here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrashPageCursor {
    /// Head sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// Commit sequence of the deletion the previous page ended on.
    pub last_deleted_at_seq: ChangeSeq,
    /// Deleted root inode the previous page ended on.
    pub last_root_inode_id: InodeId,
}

impl PageCursor for TrashPageCursor {
    const KIND: &'static str = "trash";
}

/// Cursor for one content-search (grep) snapshot.
///
/// Matches advance in ascending `(inode_id, byte_offset)` order: candidate
/// files by durable inode identity, match positions within a file by byte
/// offset. The cursor resumes strictly after the last returned match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepPageCursor {
    /// Sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// Inode of the last candidate the issuing page finished scanning.
    pub last_inode_id: InodeId,
    /// Byte offset of the last returned match within that file, or
    /// `u64::MAX` when the file was fully scanned (budget stops and
    /// matchless candidates resume at the next inode).
    pub last_byte_offset: u64,
    /// Fingerprint of the request (pattern, flags, scope) that issued the
    /// cursor; a cursor replayed under a different request is rejected
    /// instead of silently skipping results.
    pub fingerprint: u64,
}

impl PageCursor for GrepPageCursor {
    const KIND: &'static str = "grep";
}

/// One paginated endpoint's cursor.
///
/// Cursors are opaque to clients: hex-encoded JSON carrying the endpoint's
/// [`KIND`](Self::KIND) and the format version, so a cursor replayed against
/// the wrong endpoint or an older build is rejected rather than misread.
pub trait PageCursor: Serialize + serde::de::DeserializeOwned {
    /// Frozen endpoint discriminator written into the encoded cursor.
    const KIND: &'static str;
}

#[derive(Serialize, Deserialize)]
struct CursorEnvelope<C> {
    // The wire field is frozen as `v` in page cursor version 1.
    #[serde(rename = "v")]
    version: u8,
    kind: String,
    #[serde(flatten)]
    cursor: C,
}

/// Encodes a cursor as the opaque string clients round-trip.
pub fn encode_cursor<C: PageCursor>(cursor: &C) -> Result<String, PageCursorError> {
    let bytes = serde_json::to_vec(&CursorEnvelope {
        version: PAGE_CURSOR_VERSION,
        kind: C::KIND.to_owned(),
        cursor,
    })
    .map_err(|error| PageCursorError::InvalidJson(error.to_string()))?;
    Ok(crate::hex::hex_encode_bytes(&bytes))
}

/// Version and endpoint, read before the body so a cursor from another
/// endpoint reports `WrongKind` rather than a missing-field decode error.
#[derive(Deserialize)]
struct CursorHeader {
    #[serde(rename = "v")]
    version: u8,
    kind: String,
}

/// Decodes a cursor issued by [`encode_cursor`] for the same endpoint.
pub fn decode_cursor<C: PageCursor>(value: &str) -> Result<C, PageCursorError> {
    let bytes =
        crate::hex::hex_decode_bytes(value).map_err(|_| PageCursorError::InvalidEncoding)?;
    let header: CursorHeader = serde_json::from_slice(&bytes)
        .map_err(|error| PageCursorError::InvalidJson(error.to_string()))?;
    if header.version != PAGE_CURSOR_VERSION {
        return Err(PageCursorError::UnsupportedVersion {
            expected: PAGE_CURSOR_VERSION,
            actual: header.version,
        });
    }
    if header.kind != C::KIND {
        return Err(PageCursorError::WrongKind {
            expected: C::KIND,
            actual: header.kind,
        });
    }
    let envelope: CursorEnvelope<C> = serde_json::from_slice(&bytes)
        .map_err(|error| PageCursorError::InvalidJson(error.to_string()))?;
    Ok(envelope.cursor)
}

/// A cursor that resumes an enumeration of one namespace's own keyspace.
///
/// Maintenance passes walk keys rather than rows, and their cursors are
/// enumeration shortcuts and nothing else: a pass re-reads whatever
/// authorizes the work it does, whatever position it resumed from, so a
/// cursor that is lost or refused costs a repeated walk and never a wrong
/// decision. What the binding buys is that a token minted for another
/// namespace, another job, or another key family is refused instead of
/// quietly skipping the keys between here and wherever it points.
pub trait NamespaceCursor: PageCursor {
    /// Namespace whose keyspace this cursor walks.
    fn namespace_id(&self) -> &NamespaceId;

    /// Key the enumeration stopped at, or `None` at the start.
    fn last_key(&self) -> Option<&str>;

    /// Prefix every key this cursor may name lies under.
    fn key_prefix(&self) -> String;
}

/// Decodes a cursor issued for `expected_namespace_id`'s own keyspace.
pub fn decode_namespace_cursor<C: NamespaceCursor>(
    token: &str,
    expected_namespace_id: &NamespaceId,
) -> Result<C, NamespaceCursorError> {
    let cursor: C = decode_cursor(token)?;
    if cursor.namespace_id() != expected_namespace_id {
        return Err(NamespaceCursorError::ForeignNamespace);
    }
    let prefix = cursor.key_prefix();
    if cursor
        .last_key()
        .is_some_and(|key| !key.starts_with(&prefix))
    {
        return Err(NamespaceCursorError::OutsideKeyspace);
    }
    Ok(cursor)
}

/// Why a namespace-bound cursor cannot resume the enumeration replaying it.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum NamespaceCursorError {
    /// Not a cursor this enumeration issued: unreadable, or from another
    /// endpoint, job, or cursor version.
    #[error(transparent)]
    Malformed(#[from] PageCursorError),
    /// A cursor for a different namespace than the one replaying it.
    #[error("cursor belongs to a different namespace")]
    ForeignNamespace,
    /// A cursor naming a key outside the prefix its enumeration walks.
    #[error("cursor names a key outside the enumeration it resumes")]
    OutsideKeyspace,
}

/// Invalid opaque page cursor.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum PageCursorError {
    /// The cursor was not hex-encoded JSON.
    #[error("invalid page cursor encoding")]
    InvalidEncoding,
    /// The cursor JSON did not match a supported cursor shape.
    #[error("invalid page cursor JSON: {0}")]
    InvalidJson(String),
    /// The cursor was valid, but for a different paginated endpoint.
    #[error("page cursor kind `{actual}` cannot be used as `{expected}` cursor")]
    WrongKind {
        /// Cursor kind accepted by the endpoint doing the decoding.
        expected: &'static str,
        /// Cursor kind recovered from the caller's opaque token.
        actual: String,
    },
    /// The cursor format version is not supported by this build.
    #[error("unsupported page cursor version `{actual}`; expected `{expected}`")]
    UnsupportedVersion {
        /// Cursor format version this build can decode.
        expected: u8,
        /// Version embedded in the caller's opaque token.
        actual: u8,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_resolves_omitted_limit_to_default() {
        let policy = PaginationPolicy::default();
        let limit = policy.resolve_limit(None).expect("default limit");

        assert_eq!(limit.get(), DEFAULT_PAGE_LIMIT);
        assert_eq!(limit.limit_plus_one(), 1_001);
    }

    #[test]
    fn policy_rejects_invalid_limits() {
        let policy = PaginationPolicy::default();

        assert_eq!(policy.resolve_limit(Some(0)), Err(LimitError::Zero));
        assert_eq!(
            policy.resolve_limit(Some(DEFAULT_MAX_PAGE_LIMIT + 1)),
            Err(LimitError::ExceedsMax {
                requested: DEFAULT_MAX_PAGE_LIMIT + 1,
                max_limit: DEFAULT_MAX_PAGE_LIMIT,
            })
        );
    }

    #[test]
    fn policy_exports_capability_limits() {
        let limits = PaginationPolicy::default().capability_limits();

        assert_eq!(
            limits.get(LIMIT_PAGINATION_DEFAULT),
            Some(&u64::from(DEFAULT_PAGE_LIMIT))
        );
        assert_eq!(
            limits.get(LIMIT_PAGINATION_MAX),
            Some(&u64::from(DEFAULT_MAX_PAGE_LIMIT))
        );
    }

    #[test]
    fn directory_cursor_round_trips() {
        let cursor = DirectoryPageCursor {
            head_seq: ChangeSeq(11),
            directory_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        };

        let encoded = encode_cursor(&cursor).expect("encode cursor");
        let decoded: DirectoryPageCursor = decode_cursor(&encoded).expect("decode cursor");

        assert_eq!(decoded, cursor);
    }

    #[test]
    fn file_revisions_cursor_round_trips() {
        let cursor = FileRevisionsPageCursor {
            head_seq: ChangeSeq(11),
            inode_id: InodeId(7),
            last_revision_no: RevisionNo(5),
            last_committed_seq: ChangeSeq(10),
            last_revision_delta_index: 3,
        };

        let encoded = encode_cursor(&cursor).expect("encode cursor");
        let decoded: FileRevisionsPageCursor = decode_cursor(&encoded).expect("decode cursor");

        assert_eq!(decoded, cursor);
    }

    #[test]
    fn trash_cursor_round_trips() {
        let cursor = TrashPageCursor {
            head_seq: ChangeSeq(11),
            last_deleted_at_seq: ChangeSeq(10),
            last_root_inode_id: InodeId(7),
        };

        let encoded = encode_cursor(&cursor).expect("encode cursor");
        let decoded: TrashPageCursor = decode_cursor(&encoded).expect("decode cursor");

        assert_eq!(decoded, cursor);
    }

    #[test]
    fn grep_cursor_round_trips() {
        let cursor = GrepPageCursor {
            head_seq: ChangeSeq(11),
            last_inode_id: InodeId(7),
            last_byte_offset: 13,
            fingerprint: 17,
        };

        let encoded = encode_cursor(&cursor).expect("encode cursor");
        let decoded: GrepPageCursor = decode_cursor(&encoded).expect("decode cursor");

        assert_eq!(decoded, cursor);
    }

    #[test]
    fn cursor_kind_must_match_decoder() {
        let cursor = FileRevisionsPageCursor {
            head_seq: ChangeSeq(11),
            inode_id: InodeId(7),
            last_revision_no: RevisionNo(5),
            last_committed_seq: ChangeSeq(10),
            last_revision_delta_index: 3,
        };
        let encoded = encode_cursor(&cursor).expect("encode cursor");

        assert_eq!(
            decode_cursor::<DirectoryPageCursor>(&encoded),
            Err(PageCursorError::WrongKind {
                expected: "directory",
                actual: "file_revisions".to_owned(),
            })
        );
    }

    #[test]
    fn malformed_cursor_is_invalid_encoding() {
        assert_eq!(
            decode_cursor::<DirectoryPageCursor>("not-hex"),
            Err(PageCursorError::InvalidEncoding)
        );
    }

    #[test]
    fn unsupported_cursor_version_is_rejected() {
        let bytes = serde_json::to_vec(&CursorEnvelope {
            version: PAGE_CURSOR_VERSION + 1,
            kind: DirectoryPageCursor::KIND.to_owned(),
            cursor: DirectoryPageCursor {
                head_seq: ChangeSeq(11),
                directory_inode_id: InodeId(7),
                last_name_key: NameKey::parse("plan.md").expect("name key"),
            },
        })
        .expect("encode cursor");
        let encoded = crate::hex::hex_encode_bytes(&bytes);

        assert_eq!(
            decode_cursor::<DirectoryPageCursor>(&encoded),
            Err(PageCursorError::UnsupportedVersion {
                expected: PAGE_CURSOR_VERSION,
                actual: PAGE_CURSOR_VERSION + 1,
            })
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestNamespaceCursor {
        namespace_id: NamespaceId,
        last_key: String,
    }

    impl PageCursor for TestNamespaceCursor {
        const KIND: &'static str = "test_namespace";
    }

    impl NamespaceCursor for TestNamespaceCursor {
        fn namespace_id(&self) -> &NamespaceId {
            &self.namespace_id
        }

        fn last_key(&self) -> Option<&str> {
            Some(&self.last_key)
        }

        fn key_prefix(&self) -> String {
            format!("namespaces/{}/items/", self.namespace_id)
        }
    }

    #[test]
    fn namespace_cursor_accepts_its_namespace_and_keyspace() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let cursor = TestNamespaceCursor {
            namespace_id: namespace_id.clone(),
            last_key: "namespaces/demo/items/item-42".to_owned(),
        };
        let encoded = encode_cursor(&cursor).expect("encode cursor");

        assert_eq!(
            decode_namespace_cursor::<TestNamespaceCursor>(&encoded, &namespace_id)
                .expect("decode namespace cursor"),
            cursor
        );
    }

    #[test]
    fn namespace_cursor_rejects_a_different_namespace() {
        let cursor = TestNamespaceCursor {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            last_key: "namespaces/demo/items/item-42".to_owned(),
        };
        let encoded = encode_cursor(&cursor).expect("encode cursor");

        assert_eq!(
            decode_namespace_cursor::<TestNamespaceCursor>(
                &encoded,
                &NamespaceId::parse("other").expect("other namespace id")
            ),
            Err(NamespaceCursorError::ForeignNamespace)
        );
    }

    #[test]
    fn namespace_cursor_rejects_a_key_outside_its_keyspace() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let cursor = TestNamespaceCursor {
            namespace_id: namespace_id.clone(),
            last_key: "namespaces/demo/checkpoints/checkpoint-42".to_owned(),
        };
        let encoded = encode_cursor(&cursor).expect("encode cursor");

        assert_eq!(
            decode_namespace_cursor::<TestNamespaceCursor>(&encoded, &namespace_id),
            Err(NamespaceCursorError::OutsideKeyspace)
        );
    }
}
