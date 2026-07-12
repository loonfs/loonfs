use crate::{ChangeSeq, InodeId, NameKey, RevisionNo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use thiserror::Error;

/// Advisory capability key for the default page size applied when callers omit `limit`.
pub const LIMIT_PAGINATION_DEFAULT: &str = "pagination.default_limit";
/// Advisory capability key for the largest page size accepted by a deployment.
pub const LIMIT_PAGINATION_MAX: &str = "pagination.max_limit";

/// Default page size for endpoints that can return unbounded result sets.
pub const DEFAULT_PAGE_LIMIT: u32 = 1_000;
/// Default maximum accepted page size.
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
}

/// Deployment or namespace policy for paginated endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaginationPolicy {
    default_limit: NonZeroU32,
    max_limit: NonZeroU32,
}

impl PaginationPolicy {
    /// Creates a policy, requiring the default to be no larger than the max.
    pub fn new(
        default_limit: NonZeroU32,
        max_limit: NonZeroU32,
    ) -> Result<Self, PaginationPolicyError> {
        if default_limit > max_limit {
            return Err(PaginationPolicyError::DefaultExceedsMax {
                default_limit: default_limit.get(),
                max_limit: max_limit.get(),
            });
        }
        Ok(Self {
            default_limit,
            max_limit,
        })
    }

    /// Creates a policy from raw integers.
    pub fn from_values(default_limit: u32, max_limit: u32) -> Result<Self, PaginationPolicyError> {
        let default_limit =
            NonZeroU32::new(default_limit).ok_or(PaginationPolicyError::ZeroDefaultLimit)?;
        let max_limit = NonZeroU32::new(max_limit).ok_or(PaginationPolicyError::ZeroMaxLimit)?;
        Self::new(default_limit, max_limit)
    }

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
            Some(0) => Err(LimitError::Zero),
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
        let default_limit = hard_coded_nonzero(DEFAULT_PAGE_LIMIT);
        let max_limit = hard_coded_nonzero(DEFAULT_MAX_PAGE_LIMIT);
        Self {
            default_limit,
            max_limit,
        }
    }
}

/// Invalid pagination policy configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PaginationPolicyError {
    /// The configured default limit was zero.
    #[error("pagination default limit must be greater than zero")]
    ZeroDefaultLimit,
    /// The configured max limit was zero.
    #[error("pagination max limit must be greater than zero")]
    ZeroMaxLimit,
    /// The configured default limit exceeded the configured max.
    #[error("pagination default limit `{default_limit}` exceeds max limit `{max_limit}`")]
    DefaultExceedsMax { default_limit: u32, max_limit: u32 },
}

/// Invalid caller-supplied page size.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LimitError {
    /// The caller supplied `limit=0`.
    #[error("limit must be greater than zero")]
    Zero,
    /// The caller supplied a limit larger than the active policy allows.
    #[error("limit `{requested}` exceeds max limit `{max_limit}`")]
    ExceedsMax { requested: u32, max_limit: u32 },
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

/// Cursor for one directory listing snapshot.
///
/// Directory pagination advances in canonical `name_key` order.
///
/// The cursor intentionally contains only the snapshot (`head_seq`), listed
/// directory identity (`dir_inode_id`), and resume position (`last_name_key`).
/// HTTP clients must pass the URL namespace and `path` on every page.
/// Runtime/server code resolves that path at `head_seq` and rejects the cursor
/// unless it names `dir_inode_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryPageCursor {
    /// Snapshot sequence captured by the first page.
    pub head_seq: ChangeSeq,
    /// Directory inode resolved at `head_seq`.
    pub dir_inode_id: InodeId,
    /// Last canonical name key returned to the client.
    pub last_name_key: NameKey,
}

/// Cursor for one file revision listing snapshot.
///
/// Revision pagination advances in newest-first revision order for one file
/// inode. The cursor includes the snapshot head plus the last returned row's
/// complete ordering identity so ties stay unambiguous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRevisionsPageCursor {
    /// Snapshot sequence captured by the first page.
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

/// Cursor for one content-search (grep) snapshot.
///
/// Matches advance in ascending `(inode_id, byte_offset)` order: candidate
/// files by durable inode identity, match positions within a file by byte
/// offset. The cursor resumes strictly after the last returned match.
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EncodedPageCursor {
    Directory {
        v: u8,
        head_seq: ChangeSeq,
        dir_inode_id: InodeId,
        last_name_key: NameKey,
    },
    FileRevisions {
        v: u8,
        head_seq: ChangeSeq,
        inode_id: InodeId,
        last_revision_no: RevisionNo,
        last_committed_seq: ChangeSeq,
        last_revision_delta_index: u32,
    },
    Grep {
        v: u8,
        head_seq: ChangeSeq,
        last_inode_id: InodeId,
        last_byte_offset: u64,
        fingerprint: u64,
    },
}

impl EncodedPageCursor {
    fn version(&self) -> u8 {
        match self {
            Self::Directory { v, .. } | Self::FileRevisions { v, .. } | Self::Grep { v, .. } => *v,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Directory { .. } => "directory",
            Self::FileRevisions { .. } => "file_revisions",
            Self::Grep { .. } => "grep",
        }
    }

    fn validate_version(&self) -> Result<(), PageCursorError> {
        let actual = self.version();
        if actual == PAGE_CURSOR_VERSION {
            Ok(())
        } else {
            Err(PageCursorError::UnsupportedVersion {
                expected: PAGE_CURSOR_VERSION,
                actual,
            })
        }
    }
}

/// Encodes a directory-list cursor as an opaque string for clients.
pub fn encode_directory_cursor(cursor: &DirectoryPageCursor) -> Result<String, PageCursorError> {
    encode_cursor(&EncodedPageCursor::Directory {
        v: PAGE_CURSOR_VERSION,
        head_seq: cursor.head_seq,
        dir_inode_id: cursor.dir_inode_id,
        last_name_key: cursor.last_name_key.clone(),
    })
}

/// Decodes a directory-list cursor returned by [`encode_directory_cursor`].
pub fn decode_directory_cursor(value: &str) -> Result<DirectoryPageCursor, PageCursorError> {
    match decode_cursor(value)? {
        EncodedPageCursor::Directory {
            head_seq,
            dir_inode_id,
            last_name_key,
            ..
        } => Ok(DirectoryPageCursor {
            head_seq,
            dir_inode_id,
            last_name_key,
        }),
        other => Err(PageCursorError::WrongKind {
            expected: "directory",
            actual: other.kind(),
        }),
    }
}

/// Encodes a file-revisions cursor as an opaque string for clients.
pub fn encode_file_revisions_cursor(
    cursor: &FileRevisionsPageCursor,
) -> Result<String, PageCursorError> {
    encode_cursor(&EncodedPageCursor::FileRevisions {
        v: PAGE_CURSOR_VERSION,
        head_seq: cursor.head_seq,
        inode_id: cursor.inode_id,
        last_revision_no: cursor.last_revision_no,
        last_committed_seq: cursor.last_committed_seq,
        last_revision_delta_index: cursor.last_revision_delta_index,
    })
}

/// Decodes a file-revisions cursor returned by [`encode_file_revisions_cursor`].
pub fn decode_file_revisions_cursor(
    value: &str,
) -> Result<FileRevisionsPageCursor, PageCursorError> {
    match decode_cursor(value)? {
        EncodedPageCursor::FileRevisions {
            head_seq,
            inode_id,
            last_revision_no,
            last_committed_seq,
            last_revision_delta_index,
            ..
        } => Ok(FileRevisionsPageCursor {
            head_seq,
            inode_id,
            last_revision_no,
            last_committed_seq,
            last_revision_delta_index,
        }),
        other => Err(PageCursorError::WrongKind {
            expected: "file_revisions",
            actual: other.kind(),
        }),
    }
}

/// Encodes a grep cursor as an opaque string for clients.
pub fn encode_grep_cursor(cursor: &GrepPageCursor) -> Result<String, PageCursorError> {
    encode_cursor(&EncodedPageCursor::Grep {
        v: PAGE_CURSOR_VERSION,
        head_seq: cursor.head_seq,
        last_inode_id: cursor.last_inode_id,
        last_byte_offset: cursor.last_byte_offset,
        fingerprint: cursor.fingerprint,
    })
}

/// Decodes a grep cursor returned by [`encode_grep_cursor`].
pub fn decode_grep_cursor(value: &str) -> Result<GrepPageCursor, PageCursorError> {
    match decode_cursor(value)? {
        EncodedPageCursor::Grep {
            head_seq,
            last_inode_id,
            last_byte_offset,
            fingerprint,
            ..
        } => Ok(GrepPageCursor {
            head_seq,
            last_inode_id,
            last_byte_offset,
            fingerprint,
        }),
        other => Err(PageCursorError::WrongKind {
            expected: "grep",
            actual: other.kind(),
        }),
    }
}

fn encode_cursor(cursor: &EncodedPageCursor) -> Result<String, PageCursorError> {
    let bytes = serde_json::to_vec(cursor)
        .map_err(|error| PageCursorError::InvalidJson(error.to_string()))?;
    Ok(hex_encode(&bytes))
}

fn decode_cursor(value: &str) -> Result<EncodedPageCursor, PageCursorError> {
    let bytes = hex_decode(value)?;
    let cursor: EncodedPageCursor = serde_json::from_slice(&bytes)
        .map_err(|error| PageCursorError::InvalidJson(error.to_string()))?;
    cursor.validate_version()?;
    Ok(cursor)
}

/// Invalid opaque page cursor.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
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
        expected: &'static str,
        actual: &'static str,
    },
    /// The cursor format version is not supported by this build.
    #[error("unsupported page cursor version `{actual}`; expected `{expected}`")]
    UnsupportedVersion { expected: u8, actual: u8 },
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str) -> Result<Vec<u8>, PageCursorError> {
    if value.len() % 2 != 0 {
        return Err(PageCursorError::InvalidEncoding);
    }
    let mut decoded = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn hex_value(byte: u8) -> Result<u8, PageCursorError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(PageCursorError::InvalidEncoding),
    }
}

fn hard_coded_nonzero(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => unreachable!("hard-coded pagination limits must be non-zero"),
    }
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
    fn policy_rejects_default_above_max() {
        assert_eq!(
            PaginationPolicy::from_values(10, 5),
            Err(PaginationPolicyError::DefaultExceedsMax {
                default_limit: 10,
                max_limit: 5,
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
            dir_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        };

        let encoded = encode_directory_cursor(&cursor).expect("encode cursor");
        let decoded = decode_directory_cursor(&encoded).expect("decode cursor");

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

        let encoded = encode_file_revisions_cursor(&cursor).expect("encode cursor");
        let decoded = decode_file_revisions_cursor(&encoded).expect("decode cursor");

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
        let encoded = encode_file_revisions_cursor(&cursor).expect("encode cursor");

        assert_eq!(
            decode_directory_cursor(&encoded),
            Err(PageCursorError::WrongKind {
                expected: "directory",
                actual: "file_revisions",
            })
        );
    }

    #[test]
    fn malformed_cursor_is_invalid_encoding() {
        assert_eq!(
            decode_directory_cursor("not-hex"),
            Err(PageCursorError::InvalidEncoding)
        );
    }

    #[test]
    fn unsupported_cursor_version_is_rejected() {
        let encoded = encode_cursor(&EncodedPageCursor::Directory {
            v: PAGE_CURSOR_VERSION + 1,
            head_seq: ChangeSeq(11),
            dir_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        })
        .expect("encode cursor");

        assert_eq!(
            decode_directory_cursor(&encoded),
            Err(PageCursorError::UnsupportedVersion {
                expected: PAGE_CURSOR_VERSION,
                actual: PAGE_CURSOR_VERSION + 1,
            })
        );
    }
}
