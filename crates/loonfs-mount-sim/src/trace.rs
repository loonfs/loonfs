//! The trace of every request that the simulated server receives.

use crate::faults::ContentReadFault;
use loonfs_types::{ContentId, ErrorCode};

/// The operation that a request asked for, named after its operation ID in the API specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    /// `GET /v0/capabilities`.
    GetCapabilities,
    /// `GET /v0/namespaces/{ns}`.
    GetNamespace,
    /// `GET .../filesystem/entry`.
    GetPathEntry,
    /// `GET .../inodes/{inode_id}`.
    GetInode,
    /// `GET .../filesystem/entries`.
    ListPathEntries,
    /// `GET .../inodes/{inode_id}/children`.
    ListInodeChildren,
    /// `GET .../filesystem/content`.
    GetFileContent,
    /// `GET .../inodes/{inode_id}/content`.
    GetFileContentByInode,
    /// `GET .../inodes/{inode_id}/revisions/{revision_no}/content`.
    GetFileRevisionContentByInode,
    /// `POST .../filesystem/downloads`.
    CreateDownload,
    /// `POST .../inodes/{inode_id}/downloads`.
    CreateDownloadByInode,
    /// `POST .../inodes/{inode_id}/revisions/{revision_no}/downloads`.
    CreateRevisionDownloadByInode,
    /// A `GET` of one signed range of a download grant. In a real deployment, this request goes to
    /// the object store.
    ReadDownloadRange,
    /// `GET .../changes`.
    ListChanges,
    /// `POST .../commits`.
    CreateCommit,
    /// `POST .../snapshots`.
    CreateSnapshot,
    /// `GET .../snapshots/{snapshot_id}`.
    GetSnapshot,
    /// `POST .../snapshots/{snapshot_id}/extend`.
    ExtendSnapshot,
    /// `DELETE .../snapshots/{snapshot_id}`.
    DeleteSnapshot,
    /// `POST .../uploads`.
    CreateUpload,
    /// `GET .../uploads/{upload_id}`.
    GetUpload,
    /// `PUT .../uploads/{upload_id}/content`.
    PutUploadContent,
    /// `POST .../uploads/{upload_id}/complete`.
    CompleteUpload,
    /// `POST .../uploads/{upload_id}/abort`.
    AbortUpload,
    /// A request that matched no route the simulation serves.
    Unrouted,
}

impl Operation {
    /// Reports whether the operation returns file bytes: a read through the server or a signed
    /// range of a download grant.
    pub fn is_content_read(self) -> bool {
        matches!(
            self,
            Self::GetFileContent
                | Self::GetFileContentByInode
                | Self::GetFileRevisionContentByInode
                | Self::ReadDownloadRange
        )
    }
}

/// The bytes of a file that one content read returned, from `start_offset` up to but not
/// including `end_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// The offset of the first byte returned.
    pub start_offset: u64,
    /// The offset after the last byte returned.
    pub end_offset: u64,
}

impl ByteRange {
    /// Returns the number of bytes in the range.
    pub fn length(&self) -> u64 {
        self.end_offset - self.start_offset
    }
}

/// A fault hook that changed how a response reached the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectedFault {
    /// The commit stood, and the client received a transport failure instead of its response.
    ResponseDropped,
    /// The response waited until the test released a [`Hold`](crate::Hold).
    ResponseHeld,
    /// The commit stayed out of the change feed until the test released a [`Hold`](crate::Hold).
    HiddenFromFeed,
    /// The content read failed in the way the fault describes.
    ContentRead(ContentReadFault),
}

/// One request that the simulated server received, and how it responded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestRecord {
    /// The request's position in the trace, counting from 0.
    pub step: u64,
    /// The virtual time at which the server handled the request, in Unix milliseconds.
    pub at_ms: u64,
    /// The HTTP method.
    pub method: http::Method,
    /// The operation the request asked for.
    pub operation: Operation,
    /// The URL path, without the query string.
    pub path: String,
    /// The decoded query parameters, in request order.
    pub query: Vec<(String, String)>,
    /// The request's `Range` header, as the client sent it.
    pub range_header: Option<String>,
    /// The file bytes that a successful content read returned.
    pub byte_range: Option<ByteRange>,
    /// The content whose bytes a successful content read returned.
    pub content_id: Option<ContentId>,
    /// The status of the response that the server built.
    pub status: u16,
    /// The error code, when the response was an error.
    pub error_code: Option<ErrorCode>,
    /// The fault hook that changed the response, if any.
    pub fault: Option<InjectedFault>,
}

impl RequestRecord {
    /// Returns the value of a query parameter.
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// The requests recorded so far, bounded by the configured capacity.
#[derive(Debug)]
pub(crate) struct Trace {
    records: Vec<RequestRecord>,
    capacity: usize,
    next_step: u64,
}

impl Trace {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            records: Vec::new(),
            capacity,
            next_step: 0,
        }
    }

    /// Returns the next request's step. The trace checks its capacity here, before the server
    /// handles the request, so a full trace doesn't stop a request halfway.
    pub(crate) fn next_step(&mut self) -> u64 {
        assert!(
            self.records.len() < self.capacity,
            "the request trace is full at {} records; drain it with `take_trace`",
            self.capacity
        );
        let step = self.next_step;
        self.next_step += 1;
        step
    }

    pub(crate) fn record(&mut self, record: RequestRecord) {
        self.records.push(record);
    }

    pub(crate) fn snapshot(&self) -> Vec<RequestRecord> {
        self.records.clone()
    }

    pub(crate) fn take(&mut self) -> Vec<RequestRecord> {
        std::mem::take(&mut self.records)
    }
}
