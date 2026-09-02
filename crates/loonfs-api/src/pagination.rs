//! Page-size policy, page envelopes, and opaque cursors for paginated endpoints.

use crate::capability::{LIMIT_PAGINATION_DEFAULT, LIMIT_PAGINATION_MAX};
use crate::{ChangeSeq, InodeId, NameKey, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use thiserror::Error;

/// A response that carries items and a continuation position.
pub trait PagedResponse: Send + 'static {
    /// One item in the response.
    type Item: Send + 'static;
    /// The position passed to the next request.
    type Cursor: Clone + Send + 'static;

    /// Returns the response items for collection or splitting.
    fn items_mut(&mut self) -> &mut Vec<Self::Item>;

    /// Returns the response items.
    fn items(&self) -> &[Self::Item];

    /// Returns the next request position.
    fn next_cursor(&self) -> Option<Self::Cursor>;

    /// Appends a later page and adopts its continuation metadata.
    fn absorb(&mut self, later: Self);
}

enum PagerState<C> {
    NotStarted,
    More(C),
    Done,
}

type PageFuture<P, E> = Pin<Box<dyn Future<Output = Result<P, E>> + Send>>;
type PageFetcher<P, E> =
    Box<dyn FnMut(Option<<P as PagedResponse>::Cursor>) -> PageFuture<P, E> + Send>;

/// Fetches pages and retains unused items between bounded collections.
#[must_use]
pub struct Pager<P: PagedResponse, E> {
    fetch: PageFetcher<P, E>,
    state: PagerState<P::Cursor>,
    pending: Option<P>,
}

impl<P: PagedResponse, E> Pager<P, E> {
    /// Creates a pager beginning at `cursor`.
    pub fn new<F, Fut>(cursor: Option<P::Cursor>, mut fetch: F) -> Self
    where
        F: FnMut(Option<P::Cursor>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<P, E>> + Send + 'static,
    {
        let state = match cursor {
            Some(cursor) => PagerState::More(cursor),
            None => PagerState::NotStarted,
        };
        Self {
            fetch: Box::new(move |cursor| Box::pin(fetch(cursor))),
            state,
            pending: None,
        }
    }

    /// Returns the next page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<P, E>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        let cursor = match &self.state {
            PagerState::NotStarted => None,
            PagerState::More(cursor) => Some(cursor.clone()),
            PagerState::Done => return None,
        };
        let page = (self.fetch)(cursor).await;
        if let Ok(page) = &page {
            self.state = match page.next_cursor() {
                Some(cursor) => PagerState::More(cursor),
                None => PagerState::Done,
            };
        }
        Some(page)
    }

    /// Returns at most `max_items` items.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<P::Item>, E> {
        let mut items = Vec::new();
        while items.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let page_items = page.items_mut();
            let take = (max_items - items.len()).min(page_items.len());
            if take < page_items.len() {
                let remaining = page_items.split_off(take);
                items.append(page_items);
                *page.items_mut() = remaining;
                self.pending = Some(page);
                break;
            }
            items.append(page_items);
        }
        Ok(items)
    }
}

macro_rules! string_cursor_response {
    ($response:path, $item:ty, $field:ident $(, $metadata:ident)*) => {
        impl PagedResponse for $response {
            type Item = $item;
            type Cursor = String;

            fn items_mut(&mut self) -> &mut Vec<Self::Item> {
                &mut self.$field
            }

            fn items(&self) -> &[Self::Item] {
                &self.$field
            }

            fn next_cursor(&self) -> Option<Self::Cursor> {
                self.next_cursor.clone()
            }

            fn absorb(&mut self, mut later: Self) {
                $(self.$metadata = later.$metadata;)*
                self.$field.append(&mut later.$field);
                self.next_cursor = later.next_cursor;
            }
        }
    };
}

string_cursor_response!(
    crate::ListPathEntriesResponse,
    crate::PathEntry,
    entries,
    head_seq
);
string_cursor_response!(
    crate::ListInodeChildrenResponse,
    crate::PathEntry,
    entries,
    head_seq
);
string_cursor_response!(
    crate::ListFileRevisionsResponse,
    crate::FileRevision,
    revisions,
    head_seq
);
string_cursor_response!(
    crate::ListTrashResponse,
    crate::TrashEntry,
    entries,
    head_seq
);
string_cursor_response!(
    crate::ListCheckpointsResponse,
    crate::Checkpoint,
    checkpoints
);
string_cursor_response!(
    crate::v0::ListSnapshotsResponse,
    crate::v0::SnapshotSummary,
    snapshots
);

impl PagedResponse for crate::v0::ListChangesResponse {
    type Item = crate::v0::CommittedChange;
    type Cursor = ChangeSeq;

    fn items_mut(&mut self) -> &mut Vec<Self::Item> {
        &mut self.changes
    }

    fn items(&self) -> &[Self::Item] {
        &self.changes
    }

    fn next_cursor(&self) -> Option<Self::Cursor> {
        self.next_after_seq
    }

    fn absorb(&mut self, mut later: Self) {
        self.through_seq = later.through_seq;
        self.next_after_seq = later.next_after_seq;
        self.changes.append(&mut later.changes);
    }
}

/// Contract page size for endpoints that omit a caller-supplied limit.
///
/// This value is deliberately fixed and advertised through capabilities.
pub const DEFAULT_PAGE_LIMIT: u32 = 1_000;
/// Contract maximum accepted page size.
///
/// This value is deliberately fixed and advertised through capabilities.
pub const DEFAULT_MAX_PAGE_LIMIT: u32 = 1_000;

/// Format version written into every encoded cursor.
pub const PAGE_CURSOR_FORMAT_VERSION: u8 = 1;

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

/// A typed page request for internal runtime and core methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest<C> {
    /// Enforced page size.
    pub limit: EffectiveLimit,
    /// Optional decoded endpoint cursor.
    pub cursor: Option<C>,
}

/// A typed page result for internal runtime and core methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T, C> {
    /// Returned items.
    pub items: Vec<T>,
    /// Cursor for the next page, if another page is available.
    pub next_cursor: Option<C>,
}

/// A cursor that resumes one directory listing after `last_name_key`.
///
/// Snapshot cursors can resume only against the same snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryPageCursor {
    /// Head sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// The snapshot that issued this cursor, or `None` for a live read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<crate::CheckpointId>,
    /// Directory inode resolved at `head_seq`.
    pub directory_inode_id: InodeId,
    /// Last canonical name key returned to the client.
    pub last_name_key: NameKey,
}

impl PageCursor for DirectoryPageCursor {
    const KIND: &'static str = "directory";
}

/// A cursor that resumes a newest-first revision listing for one file.
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

/// A cursor that resumes an oldest-first trash listing after one deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrashPageCursor {
    /// Head sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// Commit sequence of the deletion the previous page ended on.
    pub last_deletion_seq: ChangeSeq,
    /// Deleted root inode the previous page ended on.
    pub last_root_inode_id: InodeId,
}

impl PageCursor for TrashPageCursor {
    const KIND: &'static str = "trash";
}

/// A cursor that resumes content search after one `(inode_id, byte_offset)` position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepPageCursor {
    /// Sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// Inode of the last candidate the issuing page finished scanning.
    pub last_inode_id: InodeId,
    /// The last match offset, or `u64::MAX` when the file was fully scanned.
    pub last_byte_offset: u64,
    /// The fingerprint of the pattern, flags, and scope that issued the cursor.
    pub fingerprint: u64,
}

impl PageCursor for GrepPageCursor {
    const KIND: &'static str = "grep";
}

/// A serializable cursor with an endpoint discriminator.
pub trait PageCursor: Serialize + serde::de::DeserializeOwned {
    /// Frozen endpoint discriminator written into the encoded cursor.
    const KIND: &'static str;
}

#[derive(Serialize, Deserialize)]
struct OpaqueTokenEnvelope<T> {
    format_version: u8,
    kind: String,
    #[serde(flatten)]
    token: T,
}

/// A serializable value with a frozen opaque-token discriminator.
pub trait OpaqueToken: Serialize + serde::de::DeserializeOwned {
    /// Frozen discriminator written into the encoded token.
    const KIND: &'static str;
}

impl<C: PageCursor> OpaqueToken for C {
    const KIND: &'static str = C::KIND;
}

/// Encodes a value as lowercase hexadecimal JSON with a version and kind.
pub fn encode_token<T: OpaqueToken>(
    token: &T,
    format_version: u8,
) -> Result<String, serde_json::Error> {
    serde_json::to_vec(&OpaqueTokenEnvelope {
        format_version,
        kind: T::KIND.to_owned(),
        token,
    })
    .map(|bytes| crate::hex::hex_encode_bytes(&bytes))
}

/// Encodes a cursor as the opaque string clients round-trip.
pub fn encode_cursor<C: PageCursor>(cursor: &C) -> Result<String, PageCursorError> {
    encode_token(cursor, PAGE_CURSOR_FORMAT_VERSION)
        .map_err(|error| PageCursorError::InvalidJson(error.to_string()))
}

/// Version and endpoint, read before the body so a cursor from another
/// endpoint reports `WrongKind` rather than a missing-field decode error.
#[derive(Deserialize)]
struct CursorHeader {
    format_version: u8,
    kind: String,
}

/// Decodes a lowercase hexadecimal JSON token with the expected version and kind.
pub fn decode_token<T: OpaqueToken>(
    value: &str,
    supported_version: u8,
) -> Result<T, OpaqueTokenError> {
    let bytes =
        crate::hex::hex_decode_bytes(value).map_err(|_| OpaqueTokenError::InvalidEncoding)?;
    let header: CursorHeader = serde_json::from_slice(&bytes)
        .map_err(|error| OpaqueTokenError::InvalidJson(error.to_string()))?;
    if header.format_version != supported_version {
        return Err(OpaqueTokenError::UnsupportedVersion {
            expected: supported_version,
            actual: header.format_version,
        });
    }
    if header.kind != T::KIND {
        return Err(OpaqueTokenError::WrongKind {
            expected: T::KIND,
            actual: header.kind,
        });
    }
    let envelope: OpaqueTokenEnvelope<T> = serde_json::from_slice(&bytes)
        .map_err(|error| OpaqueTokenError::InvalidJson(error.to_string()))?;
    Ok(envelope.token)
}

/// Decodes a cursor issued by [`encode_cursor`] for the same endpoint.
pub fn decode_cursor<C: PageCursor>(value: &str) -> Result<C, PageCursorError> {
    decode_token(value, PAGE_CURSOR_FORMAT_VERSION).map_err(PageCursorError::from)
}

/// A cursor bound to one namespace keyspace.
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
    /// The cursor is unreadable or belongs to another endpoint, job, or version.
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

/// Invalid opaque token encoding, version, kind, or body.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum OpaqueTokenError {
    /// The token was not lowercase hexadecimal JSON.
    #[error("invalid opaque token encoding")]
    InvalidEncoding,
    /// The token JSON did not match the expected shape.
    #[error("invalid opaque token JSON: {0}")]
    InvalidJson(String),
    /// The token belongs to another family.
    #[error("opaque token kind `{actual}` cannot be used as `{expected}` token")]
    WrongKind {
        /// Token kind accepted by the decoder.
        expected: &'static str,
        /// Token kind recovered from the encoded value.
        actual: String,
    },
    /// The token format version is not supported by this build.
    #[error("unsupported opaque token version `{actual}`; expected `{expected}`")]
    UnsupportedVersion {
        /// Token version this build can decode.
        expected: u8,
        /// Version embedded in the encoded value.
        actual: u8,
    },
}

impl From<OpaqueTokenError> for PageCursorError {
    fn from(error: OpaqueTokenError) -> Self {
        match error {
            OpaqueTokenError::InvalidEncoding => Self::InvalidEncoding,
            OpaqueTokenError::InvalidJson(message) => Self::InvalidJson(message),
            OpaqueTokenError::WrongKind { expected, actual } => {
                Self::WrongKind { expected, actual }
            }
            OpaqueTokenError::UnsupportedVersion { expected, actual } => {
                Self::UnsupportedVersion { expected, actual }
            }
        }
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
    fn policy_exports_capability_limits() {
        let limits = PaginationPolicy::default().capability_limits();

        assert_eq!(
            limits.get("pagination.default_limit"),
            Some(&u64::from(DEFAULT_PAGE_LIMIT))
        );
        assert_eq!(
            limits.get("pagination.max_limit"),
            Some(&u64::from(DEFAULT_MAX_PAGE_LIMIT))
        );
    }

    #[test]
    fn directory_cursor_round_trips() {
        let cursor = DirectoryPageCursor {
            head_seq: ChangeSeq(11),
            snapshot_id: Some(
                crate::CheckpointId::parse("chk_00000000000000000000000000000001")
                    .expect("snapshot id"),
            ),
            directory_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        };

        let encoded = encode_cursor(&cursor).expect("encode cursor");
        let decoded: DirectoryPageCursor = decode_cursor(&encoded).expect("decode cursor");

        assert_eq!(decoded, cursor);
    }

    #[test]
    fn live_directory_cursor_omits_the_additive_snapshot_field() {
        let cursor = DirectoryPageCursor {
            head_seq: ChangeSeq(11),
            snapshot_id: None,
            directory_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        };

        let encoded = encode_cursor(&cursor).expect("encode cursor");
        let bytes = crate::hex::hex_decode_bytes(&encoded).expect("decode hex");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("decode JSON");

        assert!(json.get("snapshot_id").is_none());
        assert_eq!(
            decode_cursor::<DirectoryPageCursor>(&encoded).expect("decode cursor"),
            cursor
        );
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
            last_deletion_seq: ChangeSeq(10),
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
        let bytes = serde_json::to_vec(&OpaqueTokenEnvelope {
            format_version: PAGE_CURSOR_FORMAT_VERSION + 1,
            kind: <DirectoryPageCursor as PageCursor>::KIND.to_owned(),
            token: DirectoryPageCursor {
                head_seq: ChangeSeq(11),
                snapshot_id: None,
                directory_inode_id: InodeId(7),
                last_name_key: NameKey::parse("plan.md").expect("name key"),
            },
        })
        .expect("encode cursor");
        let encoded = crate::hex::hex_encode_bytes(&bytes);

        assert_eq!(
            decode_cursor::<DirectoryPageCursor>(&encoded),
            Err(PageCursorError::UnsupportedVersion {
                expected: PAGE_CURSOR_FORMAT_VERSION,
                actual: PAGE_CURSOR_FORMAT_VERSION + 1,
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
