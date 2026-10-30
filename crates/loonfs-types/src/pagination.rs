//! Page sizes, page envelopes, and opaque cursors for paginated endpoints.

use crate::{ChangeSeq, InodeId, NameKey, RevisionNo};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use thiserror::Error;

/// A response that contains items and a continuation position.
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
type PageLoader<P, E> =
    Box<dyn FnMut(PageRequest<<P as PagedResponse>::Cursor>) -> PageFuture<P, E> + Send>;

/// Fetches pages and retains unused items between bounded collections.
///
/// The pager keeps the listing's position. It starts at the first page, or
/// at the position set with [`Pager::cursor`], and passes each page's next
/// cursor to the next request. Each request asks for [`Pager::page_limit`]
/// items, or for fewer when [`Pager::max_items`] leaves fewer to load.
#[must_use]
pub struct Pager<P: PagedResponse, E> {
    load_page: PageLoader<P, E>,
    state: PagerState<P::Cursor>,
    pending: Option<P>,
    page_limit: EffectiveLimit,
    unloaded_items: usize,
}

impl<P: PagedResponse, E> Pager<P, E> {
    /// Creates a pager over the pages `load_page` returns, at the default
    /// page size and with no total limit.
    ///
    /// A request without a cursor asks `load_page` for the first page.
    pub fn new<F, Fut>(mut load_page: F) -> Self
    where
        F: FnMut(PageRequest<P::Cursor>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<P, E>> + Send + 'static,
    {
        Self {
            load_page: Box::new(move |request| Box::pin(load_page(request))),
            state: PagerState::NotStarted,
            pending: None,
            page_limit: EffectiveLimit::DEFAULT,
            unloaded_items: usize::MAX,
        }
    }

    /// Creates a pager whose `load_page` receives a clone of `state` with
    /// each request, at the default page size and with no total limit.
    ///
    /// `load_page` is a function rather than a closure, so everything a page
    /// request needs travels in `state`.
    pub fn with_state<S, Fut>(state: S, load_page: fn(S, PageRequest<P::Cursor>) -> Fut) -> Self
    where
        S: Clone + Send + 'static,
        Fut: Future<Output = Result<P, E>> + Send + 'static,
    {
        Self::new(move |request| load_page(state.clone(), request))
    }

    /// Starts the listing at `cursor`, a position that an earlier page
    /// returned, or at the first page when it is `None`.
    pub fn cursor(mut self, cursor: Option<P::Cursor>) -> Self {
        self.state = match cursor {
            Some(cursor) => PagerState::More(cursor),
            None => PagerState::NotStarted,
        };
        self.pending = None;
        self
    }

    /// Asks for at most `page_limit` items in each request.
    pub fn page_limit(mut self, page_limit: EffectiveLimit) -> Self {
        self.page_limit = page_limit;
        self
    }

    /// Ends the listing once the pager has loaded `max_items` items, and
    /// asks each request for no more items than are left.
    pub fn max_items(mut self, max_items: usize) -> Self {
        self.unloaded_items = max_items;
        self
    }

    /// Returns the next page, or `None` after the listing's last page or once
    /// the pager has loaded `max_items` items.
    pub async fn next(&mut self) -> Option<Result<P, E>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        let cursor = match &self.state {
            PagerState::NotStarted => None,
            PagerState::More(cursor) => Some(cursor.clone()),
            PagerState::Done => return None,
        };
        // A total too large for `u32` leaves more items than any page contains.
        let unloaded_items = u32::try_from(self.unloaded_items).unwrap_or(u32::MAX);
        let limit = NonZeroU32::new(unloaded_items.min(self.page_limit.get()))?;
        let page = (self.load_page)(PageRequest {
            limit: EffectiveLimit(limit),
            cursor,
        })
        .await;
        if let Ok(page) = &page {
            self.unloaded_items = self.unloaded_items.saturating_sub(page.items().len());
            self.state = match page.next_cursor() {
                Some(cursor) => PagerState::More(cursor),
                None => PagerState::Done,
            };
        }
        Some(page)
    }

    /// Returns the pager's next page: the page at its cursor, or the first
    /// page when it has none.
    ///
    /// # Panics
    ///
    /// Panics when the pager has no page left to load, because it returned
    /// the listing's last page or loaded `max_items` items.
    pub async fn page(mut self) -> Result<P, E> {
        self.next()
            .await
            .expect("a pager asked for one page should have a page left to load")
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
string_cursor_response!(crate::ListSnapshotsResponse, crate::Snapshot, snapshots);

impl PagedResponse for crate::GrepResponse {
    type Item = crate::GrepMatch;
    type Cursor = String;

    fn items_mut(&mut self) -> &mut Vec<Self::Item> {
        &mut self.matches
    }

    fn items(&self) -> &[Self::Item] {
        &self.matches
    }

    fn next_cursor(&self) -> Option<Self::Cursor> {
        self.next_cursor.clone()
    }

    fn absorb(&mut self, mut later: Self) {
        self.head_seq = later.head_seq;
        self.built_through_seq = later.built_through_seq;
        self.tail_scanned &= later.tail_scanned;
        self.matches.append(&mut later.matches);
        self.next_cursor = later.next_cursor;
    }
}

impl PagedResponse for crate::ListChangesResponse {
    type Item = crate::Commit;
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
        self.head_seq = later.head_seq;
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
pub const MAX_PAGE_LIMIT: u32 = 1_000;

/// Format version written into every encoded cursor.
pub const PAGE_CURSOR_FORMAT_VERSION: u8 = 1;

/// A validated page size selected from a caller request and a policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EffectiveLimit(NonZeroU32);

impl EffectiveLimit {
    /// The page size applied when a caller omits `limit`.
    pub const DEFAULT: Self =
        Self(NonZeroU32::new(DEFAULT_PAGE_LIMIT).expect("the default page limit is nonzero"));

    /// The largest page size that a caller can request.
    pub const MAX: Self =
        Self(NonZeroU32::new(MAX_PAGE_LIMIT).expect("the maximum page limit is nonzero"));

    /// Creates an effective limit from a non-zero value.
    pub fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// Resolves a caller-supplied limit into the enforced page size, which
    /// is [`Self::DEFAULT`] when the caller omits it.
    pub fn from_requested(requested: Option<u32>) -> Result<Self, LimitError> {
        match requested {
            None => Ok(Self::DEFAULT),
            Some(value) if value > MAX_PAGE_LIMIT => Err(LimitError::ExceedsMax {
                requested: value,
                max_limit: MAX_PAGE_LIMIT,
            }),
            Some(value) => NonZeroU32::new(value).map(Self).ok_or(LimitError::Zero),
        }
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

/// One page of a listing, named by its size and its position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest<C> {
    /// Enforced page size.
    pub limit: EffectiveLimit,
    /// Where the page starts, or `None` for the first page.
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
/// A cursor from a read of a snapshot resumes only against the same
/// snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryPageCursor {
    /// Head sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// The snapshot whose read issued this cursor, or `None` for a live
    /// read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_id: Option<crate::PinId>,
    /// Directory inode resolved at `head_seq`.
    pub directory_inode_id: InodeId,
    /// Last canonical name key returned to the client.
    pub last_name_key: NameKey,
}

impl PageCursor for DirectoryPageCursor {
    const KIND: &'static str = "directory";
}

/// A cursor that resumes a recursive directory listing after the last
/// returned entry.
///
/// A cursor from a read of a snapshot resumes only against the same
/// snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryTreePageCursor {
    /// Head sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// The snapshot whose read issued this cursor, or `None` for a live
    /// read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_id: Option<crate::PinId>,
    /// The walk's position in each directory from the listed directory down,
    /// one frame per directory.
    pub frames: Vec<DirectoryTreeFrame>,
}

impl PageCursor for DirectoryTreePageCursor {
    const KIND: &'static str = "directory_tree";
}

/// One frame of a recursive listing's position: a directory, and the last
/// name key the walk returned from that directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryTreeFrame {
    /// Directory inode of this frame, a child of the previous frame's
    /// directory.
    pub directory_inode_id: InodeId,
    /// Last canonical name key returned from this directory, or `None` when
    /// the walk entered the directory without returning any of its children
    /// yet.
    ///
    /// Only the last frame can omit the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name_key: Option<NameKey>,
}

/// A cursor that resumes a directory listing, of one directory's children
/// or of its whole subtree.
///
/// Each kind of listing accepts only its own kind of cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPageCursor {
    /// Resumes a listing of one directory's children.
    Directory(DirectoryPageCursor),
    /// Resumes a recursive listing of a directory's subtree.
    DirectoryTree(DirectoryTreePageCursor),
}

impl ListPageCursor {
    /// Decodes a cursor of the kind that a listing with `options` issues.
    pub fn decode(
        value: &str,
        options: &crate::options::ListOptions,
    ) -> Result<Self, OpaqueTokenError> {
        match options.recursive {
            false => decode_cursor(value).map(Self::Directory),
            true => decode_cursor(value).map(Self::DirectoryTree),
        }
    }

    /// Encodes the cursor as the opaque string clients round-trip.
    pub fn encode(&self) -> Result<String, OpaqueTokenError> {
        match self {
            Self::Directory(cursor) => encode_cursor(cursor),
            Self::DirectoryTree(cursor) => encode_cursor(cursor),
        }
    }

    /// Returns the head sequence the issuing page was evaluated at.
    pub fn head_seq(&self) -> ChangeSeq {
        match self {
            Self::Directory(cursor) => cursor.head_seq,
            Self::DirectoryTree(cursor) => cursor.head_seq,
        }
    }

    /// Returns the snapshot whose read issued the cursor, or `None` for a
    /// live read.
    pub fn pin_id(&self) -> Option<&crate::PinId> {
        match self {
            Self::Directory(cursor) => cursor.pin_id.as_ref(),
            Self::DirectoryTree(cursor) => cursor.pin_id.as_ref(),
        }
    }

    /// Returns the cursor's pin, so the read that issues the cursor can set it.
    pub fn pin_id_mut(&mut self) -> &mut Option<crate::PinId> {
        match self {
            Self::Directory(cursor) => &mut cursor.pin_id,
            Self::DirectoryTree(cursor) => &mut cursor.pin_id,
        }
    }
}

/// A cursor that resumes a newest-first revision listing for one file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// A cursor that resumes content search at one position in candidate order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepPageCursor {
    /// Sequence the issuing page was evaluated at.
    pub head_seq: ChangeSeq,
    /// Inode of the last candidate the issuing page reached.
    pub last_inode_id: InodeId,
    /// Where the next page resumes in `last_inode_id`.
    pub resume: GrepCursorPosition,
    /// The fingerprint of the pattern, flags, and scope that issued the cursor.
    pub fingerprint: u64,
}

/// Where a grep cursor resumes in its last candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GrepCursorPosition {
    /// After the match at `byte_offset`, so the next page continues in the
    /// same file.
    AfterMatch {
        /// Byte offset of the last returned match.
        byte_offset: u64,
    },
    /// After the whole file.
    ///
    /// The braces make serde reject a stray field; a unit variant would
    /// silently accept and discard one.
    AfterFile {},
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
#[serde(deny_unknown_fields)]
struct OpaqueTokenEnvelope<T> {
    format_version: u8,
    kind: String,
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

/// Spells bytes in the opaque-token alphabet, unpadded base64url.
pub fn encode_token_bytes(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Reads bytes spelled by [`encode_token_bytes`].
///
/// Returns [`OpaqueTokenError::InvalidEncoding`] for padding, a character
/// outside the base64url alphabet, or a final character with stray low bits.
pub fn decode_token_bytes(value: &str) -> Result<Vec<u8>, OpaqueTokenError> {
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| OpaqueTokenError::InvalidEncoding)
}

/// Encodes a value as unpadded base64url JSON with a version and kind.
pub fn encode_token<T: OpaqueToken>(
    token: &T,
    format_version: u8,
) -> Result<String, serde_json::Error> {
    serde_json::to_vec(&OpaqueTokenEnvelope {
        format_version,
        kind: T::KIND.to_owned(),
        token,
    })
    .map(|bytes| encode_token_bytes(&bytes))
}

/// Encodes a cursor as the opaque string clients round-trip.
pub fn encode_cursor<C: PageCursor>(cursor: &C) -> Result<String, OpaqueTokenError> {
    encode_token(cursor, PAGE_CURSOR_FORMAT_VERSION)
        .map_err(|error| OpaqueTokenError::InvalidJson(error.to_string()))
}

/// Version and endpoint, read before the body so a cursor from another
/// endpoint reports `WrongKind` rather than a missing-field decode error.
#[derive(Deserialize)]
struct CursorHeader {
    format_version: u8,
    kind: String,
}

/// Decodes an unpadded base64url JSON token with the expected version and kind.
pub fn decode_token<T: OpaqueToken>(
    value: &str,
    supported_version: u8,
) -> Result<T, OpaqueTokenError> {
    let bytes = decode_token_bytes(value)?;
    let header: CursorHeader = serde_json::from_slice(&bytes).map_err(|error| {
        // Every hex string is also base64url, so a hex token decodes here, but to
        // bytes that are not JSON. Bytes that don't parse as JSON at all are an
        // encoding failure; a well-formed document of the wrong shape is not.
        if error.is_syntax() || error.is_eof() {
            OpaqueTokenError::InvalidEncoding
        } else {
            OpaqueTokenError::InvalidJson(error.to_string())
        }
    })?;
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
pub fn decode_cursor<C: PageCursor>(value: &str) -> Result<C, OpaqueTokenError> {
    decode_token(value, PAGE_CURSOR_FORMAT_VERSION)
}

/// Invalid opaque token encoding, version, kind, or body.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum OpaqueTokenError {
    /// The token was not unpadded base64url, or its bytes were not a JSON document.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;
    use std::convert::Infallible;
    use std::task::{Context, Poll, Waker};

    #[derive(Debug, PartialEq, Eq)]
    struct NumberPage {
        numbers: Vec<u32>,
        next_cursor: Option<u32>,
    }

    impl PagedResponse for NumberPage {
        type Item = u32;
        type Cursor = u32;

        fn items_mut(&mut self) -> &mut Vec<u32> {
            &mut self.numbers
        }

        fn items(&self) -> &[u32] {
            &self.numbers
        }

        fn next_cursor(&self) -> Option<u32> {
            self.next_cursor
        }

        fn absorb(&mut self, mut later: Self) {
            self.numbers.append(&mut later.numbers);
            self.next_cursor = later.next_cursor;
        }
    }

    /// Serves the numbers below `count`, at most `max_page_items` to a page.
    fn numbers(count: u32, max_page_items: usize) -> Pager<NumberPage, Infallible> {
        Pager::new(move |request: PageRequest<u32>| {
            let start = request.cursor.map_or(0, |cursor| cursor + 1);
            let numbers: Vec<u32> = (start..count)
                .take(request.limit.as_usize().min(max_page_items))
                .collect();
            let next_cursor = numbers.last().copied().filter(|last| last + 1 < count);
            std::future::ready(Ok(NumberPage {
                numbers,
                next_cursor,
            }))
        })
    }

    fn poll_once<T>(future: impl Future<Output = T>) -> Poll<T> {
        std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    fn limit(value: u32) -> EffectiveLimit {
        EffectiveLimit::new(NonZeroU32::new(value).expect("nonzero limit"))
    }

    /// Returns every page `pager` loads, in order.
    fn pages(mut pager: Pager<NumberPage, Infallible>) -> Vec<Vec<u32>> {
        let mut pages = Vec::new();
        loop {
            let Poll::Ready(next) = poll_once(pager.next()) else {
                panic!("a loader that is always ready should never leave a page pending");
            };
            let Some(page) = next else {
                return pages;
            };
            pages.push(page.expect("an infallible page").numbers);
        }
    }

    #[test]
    fn page_returns_the_page_at_the_cursor_with_the_page_limit() {
        let pager = numbers(10, usize::MAX).cursor(Some(4)).page_limit(limit(3));

        assert_eq!(
            poll_once(pager.page()),
            Poll::Ready(Ok(NumberPage {
                numbers: vec![5, 6, 7],
                next_cursor: Some(7),
            }))
        );
    }

    #[test]
    fn a_page_limit_and_max_items_size_each_request_and_end_the_listing() {
        // Each case is the listing's length, the most items the loader puts
        // on one page, the page limit, the total limit, and the length of
        // each page.
        let cases = [
            (20, usize::MAX, 10, 3, vec![3]),
            (20, usize::MAX, 3, 7, vec![3, 3, 1]),
            (5, usize::MAX, 2, usize::MAX, vec![2, 2, 1]),
            (5, usize::MAX, 2, 2, vec![2]),
            (5, usize::MAX, 3, 99, vec![3, 2]),
            (20, 2, 3, 7, vec![2, 2, 2, 1]),
            (20, usize::MAX, 3, 0, vec![]),
        ];
        for (count, max_page_items, page_limit, max_items, lengths) in cases {
            let pages = pages(
                numbers(count, max_page_items)
                    .page_limit(limit(page_limit))
                    .max_items(max_items),
            );
            let case = format!(
                "{count} items, at most {max_page_items} a page, page limit {page_limit}, \
                 total limit {max_items}"
            );
            assert_eq!(
                pages.iter().map(Vec::len).collect::<Vec<_>>(),
                lengths,
                "{case}"
            );
            let total = u32::try_from(lengths.iter().sum::<usize>()).expect("a small total");
            assert_eq!(pages.concat(), (0..total).collect::<Vec<_>>(), "{case}");
        }
    }

    #[test]
    fn collect_up_to_keeps_unused_entries_for_the_next_call() {
        let mut pager = numbers(3, 2);

        assert_eq!(poll_once(pager.collect_up_to(1)), Poll::Ready(Ok(vec![0])));
        assert_eq!(
            poll_once(pager.next()),
            Poll::Ready(Some(Ok(NumberPage {
                numbers: vec![1],
                next_cursor: Some(1),
            })))
        );
        assert_eq!(
            poll_once(pager.next()),
            Poll::Ready(Some(Ok(NumberPage {
                numbers: vec![2],
                next_cursor: None,
            })))
        );
        assert_eq!(poll_once(pager.next()), Poll::Ready(None));
    }

    #[test]
    fn an_omitted_limit_resolves_to_the_default() {
        let limit = EffectiveLimit::from_requested(None).expect("default limit");

        assert_eq!(limit.get(), DEFAULT_PAGE_LIMIT);
        assert_eq!(limit.limit_plus_one(), 1_001);
    }

    #[test]
    fn invalid_limits_are_rejected() {
        assert_eq!(
            EffectiveLimit::from_requested(Some(0)),
            Err(LimitError::Zero)
        );
        assert_eq!(
            EffectiveLimit::from_requested(Some(MAX_PAGE_LIMIT + 1)),
            Err(LimitError::ExceedsMax {
                requested: MAX_PAGE_LIMIT + 1,
                max_limit: MAX_PAGE_LIMIT,
            })
        );
    }

    fn assert_cursor_round_trips<C: PageCursor + PartialEq + std::fmt::Debug>(cursor: C) {
        let encoded = encode_cursor(&cursor).expect("encode cursor");
        assert_eq!(decode_cursor::<C>(&encoded).expect("decode cursor"), cursor);
    }

    #[test]
    fn every_cursor_kind_round_trips() {
        assert_cursor_round_trips(DirectoryPageCursor {
            head_seq: ChangeSeq(11),
            pin_id: Some(
                crate::PinId::parse("pin_00000000000000000001-0000000000000001").expect("pin id"),
            ),
            directory_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        });
        assert_cursor_round_trips(DirectoryTreePageCursor {
            head_seq: ChangeSeq(11),
            pin_id: None,
            frames: vec![
                DirectoryTreeFrame {
                    directory_inode_id: crate::ROOT_INODE_ID,
                    last_name_key: Some(NameKey::parse("src").expect("name key")),
                },
                DirectoryTreeFrame {
                    directory_inode_id: InodeId(7),
                    last_name_key: None,
                },
            ],
        });
        assert_cursor_round_trips(FileRevisionsPageCursor {
            head_seq: ChangeSeq(11),
            inode_id: InodeId(7),
            last_revision_no: RevisionNo(5),
            last_committed_seq: ChangeSeq(10),
            last_revision_delta_index: 3,
        });
        assert_cursor_round_trips(TrashPageCursor {
            head_seq: ChangeSeq(11),
            last_deletion_seq: ChangeSeq(10),
            last_root_inode_id: InodeId(7),
        });
        for resume in [
            GrepCursorPosition::AfterMatch { byte_offset: 13 },
            GrepCursorPosition::AfterFile {},
        ] {
            assert_cursor_round_trips(GrepPageCursor {
                head_seq: ChangeSeq(11),
                last_inode_id: InodeId(7),
                resume,
                fingerprint: 17,
            });
        }
    }

    #[test]
    fn a_live_directory_cursor_omits_the_pin_field_and_rejects_unknown_fields() {
        let cursor = DirectoryPageCursor {
            head_seq: ChangeSeq(11),
            pin_id: None,
            directory_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        };

        let encoded = encode_cursor(&cursor).expect("encode cursor");
        let bytes = decode_token_bytes(&encoded).expect("decode base64url");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("decode JSON");

        assert!(json["token"].is_object());
        assert!(json["token"].get("pin_id").is_none());
        assert_eq!(
            decode_cursor::<DirectoryPageCursor>(&encoded).expect("decode cursor"),
            cursor
        );
        let mut extra_envelope_field = json.clone();
        extra_envelope_field["extra"] = true.into();
        let mut extra_token_field = json;
        extra_token_field["token"]["extra"] = true.into();
        for edited in [extra_envelope_field, extra_token_field] {
            let edited = encode_token_bytes(&serde_json::to_vec(&edited).expect("encode JSON"));
            assert_matches!(
                decode_cursor::<DirectoryPageCursor>(&edited),
                Err(OpaqueTokenError::InvalidJson(message)) if message.contains("unknown field")
            );
        }
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
            Err(OpaqueTokenError::WrongKind {
                expected: "directory",
                actual: "file_revisions".to_owned(),
            })
        );
    }

    /// Checks that a listing accepts only the cursor kind that its own kind
    /// of listing issues, so a cursor from a listing that isn't recursive
    /// can't resume a recursive one, or the reverse.
    #[test]
    fn a_list_cursor_decodes_only_for_the_kind_of_listing_that_issued_it() {
        let directory = ListPageCursor::Directory(DirectoryPageCursor {
            head_seq: ChangeSeq(11),
            pin_id: None,
            directory_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        });
        let directory_tree = ListPageCursor::DirectoryTree(DirectoryTreePageCursor {
            head_seq: ChangeSeq(11),
            pin_id: None,
            frames: vec![DirectoryTreeFrame {
                directory_inode_id: InodeId(7),
                last_name_key: Some(NameKey::parse("plan.md").expect("name key")),
            }],
        });
        let options = |recursive| crate::options::ListOptions {
            recursive,
            ..Default::default()
        };

        for (cursor, recursive, kind, other_kind) in [
            (&directory, false, "directory", "directory_tree"),
            (&directory_tree, true, "directory_tree", "directory"),
        ] {
            let encoded = cursor.encode().expect("encode cursor");
            assert_eq!(
                ListPageCursor::decode(&encoded, &options(recursive)).as_ref(),
                Ok(cursor)
            );
            assert_eq!(
                ListPageCursor::decode(&encoded, &options(!recursive)),
                Err(OpaqueTokenError::WrongKind {
                    expected: other_kind,
                    actual: kind.to_owned(),
                })
            );
        }
    }

    #[test]
    fn a_cursor_that_is_not_unpadded_base64url_json_is_invalid_encoding() {
        let cursor = DirectoryPageCursor {
            head_seq: ChangeSeq(11),
            pin_id: None,
            directory_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.txt").expect("name key"),
        };
        let json = decode_token_bytes(&encode_cursor(&cursor).expect("encode cursor"))
            .expect("decode base64url");
        let padded = base64::engine::general_purpose::URL_SAFE.encode(&json);
        assert!(padded.ends_with('='), "the sample JSON should need padding");
        let old_hex = crate::hex::hex_encode_bytes(&json);

        for value in ["not base64url!", padded.as_str(), old_hex.as_str()] {
            assert_eq!(
                decode_cursor::<DirectoryPageCursor>(value),
                Err(OpaqueTokenError::InvalidEncoding),
                "{value}"
            );
        }
    }

    #[test]
    fn unsupported_cursor_version_is_rejected() {
        let bytes = serde_json::to_vec(&OpaqueTokenEnvelope {
            format_version: PAGE_CURSOR_FORMAT_VERSION + 1,
            kind: <DirectoryPageCursor as PageCursor>::KIND.to_owned(),
            token: DirectoryPageCursor {
                head_seq: ChangeSeq(11),
                pin_id: None,
                directory_inode_id: InodeId(7),
                last_name_key: NameKey::parse("plan.md").expect("name key"),
            },
        })
        .expect("encode cursor");
        let encoded = encode_token_bytes(&bytes);

        assert_eq!(
            decode_cursor::<DirectoryPageCursor>(&encoded),
            Err(OpaqueTokenError::UnsupportedVersion {
                expected: PAGE_CURSOR_FORMAT_VERSION,
                actual: PAGE_CURSOR_FORMAT_VERSION + 1,
            })
        );
    }
}
