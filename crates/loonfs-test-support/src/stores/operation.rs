//! Object-store operation descriptions used by wrapper predicates and logs.

use bytes::Bytes;
use loonfs_objectstore::{AssemblySource, ByteRange, PutMode};
use loonfs_types::Checksum;

use super::Outcome;

/// A broad selector a test wrapper uses to choose the operations it acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationSelector {
    /// Every operation.
    Any,
    /// Metadata-only reads.
    Head,
    /// Byte reads through `get`.
    Get,
    /// Full reads through `get_with_metadata`.
    GetWithMetadata,
    /// Either byte-read form.
    Read,
    /// Any `put` mode, assemblies, and compare-and-swap calls.
    Put,
    /// Create-if-absent puts.
    PutCreateIfAbsent,
    /// Compare-and-swap calls and CAS-mode puts.
    CompareAndSwap,
    /// Deletes.
    Delete,
    /// Prefix-list calls.
    List,
}

impl OperationSelector {
    /// Every selector, in declaration order.
    pub const ALL: [Self; 10] = [
        Self::Any,
        Self::Head,
        Self::Get,
        Self::GetWithMetadata,
        Self::Read,
        Self::Put,
        Self::PutCreateIfAbsent,
        Self::CompareAndSwap,
        Self::Delete,
        Self::List,
    ];

    /// Reports whether `kind` is one of the operations this selector names.
    pub fn matches(self, kind: &OperationKind<'_>) -> bool {
        match self {
            Self::Any => true,
            Self::Head => matches!(kind, OperationKind::Head),
            Self::Get => matches!(kind, OperationKind::Get { .. }),
            Self::GetWithMetadata => matches!(kind, OperationKind::GetWithMetadata),
            Self::Read => matches!(
                kind,
                OperationKind::Get { .. } | OperationKind::GetWithMetadata
            ),
            Self::Put => matches!(
                kind,
                OperationKind::Put { .. }
                    | OperationKind::PutStreamed { .. }
                    | OperationKind::Assemble { .. }
                    | OperationKind::CompareAndSwap { .. }
            ),
            Self::PutCreateIfAbsent => matches!(
                kind,
                OperationKind::Put {
                    mode: PutMode::CreateIfAbsent,
                    ..
                } | OperationKind::PutStreamed {
                    mode: PutMode::CreateIfAbsent,
                } | OperationKind::Assemble { .. }
            ),
            Self::CompareAndSwap => matches!(
                kind,
                OperationKind::CompareAndSwap { .. }
                    | OperationKind::Put {
                        mode: PutMode::CompareAndSwap { .. },
                        ..
                    }
                    | OperationKind::PutStreamed {
                        mode: PutMode::CompareAndSwap { .. },
                    }
            ),
            Self::Delete => matches!(kind, OperationKind::Delete),
            Self::List => matches!(kind, OperationKind::List),
        }
    }
}

/// Details of one intercepted object-store operation.
#[derive(Debug)]
pub struct OperationContext<'a> {
    key: &'a str,
    kind: OperationKind<'a>,
}

impl<'a> OperationContext<'a> {
    pub(crate) fn new(key: &'a str, kind: OperationKind<'a>) -> Self {
        Self { key, kind }
    }

    /// Returns the addressed object key or list prefix.
    pub fn key(&self) -> &str {
        self.key
    }

    /// Returns the exact operation details.
    pub fn kind(&self) -> &OperationKind<'a> {
        &self.kind
    }

    /// Copies this borrowed operation into a log entry.
    pub fn to_owned(&self, outcome: &Outcome) -> RecordedOperation {
        match &self.kind {
            OperationKind::Head => RecordedOperation::Head {
                key: self.key.to_owned(),
            },
            OperationKind::Get { range } => RecordedOperation::Get {
                key: self.key.to_owned(),
                range: (*range).cloned(),
                result_bytes: outcome.bytes().unwrap_or(0),
            },
            OperationKind::GetWithMetadata => RecordedOperation::GetWithMetadata {
                key: self.key.to_owned(),
                result_bytes: outcome.bytes().unwrap_or(0),
            },
            OperationKind::Put { bytes, mode } => RecordedOperation::Put {
                key: self.key.to_owned(),
                mode: (*mode).clone(),
                bytes: bytes.len(),
            },
            OperationKind::PutStreamed { mode } => RecordedOperation::PutStreamed {
                key: self.key.to_owned(),
                mode: (*mode).clone(),
                bytes: outcome.streamed_bytes(),
            },
            OperationKind::Assemble {
                sources,
                tail,
                expected,
            } => RecordedOperation::Assemble {
                key: self.key.to_owned(),
                sources: sources.to_vec(),
                bytes: tail.iter().map(Bytes::len).sum(),
                expected: (*expected).clone(),
            },
            OperationKind::CompareAndSwap { bytes, .. } => RecordedOperation::CompareAndSwap {
                key: self.key.to_owned(),
                bytes: bytes.len(),
            },
            OperationKind::Delete => RecordedOperation::Delete {
                key: self.key.to_owned(),
            },
            OperationKind::List => RecordedOperation::List {
                prefix: self.key.to_owned(),
            },
        }
    }
}

/// Borrowed details of one exact object-store operation.
#[derive(Debug)]
pub enum OperationKind<'a> {
    /// A `head` call.
    Head,
    /// A `get` call.
    Get {
        /// Requested byte range.
        range: Option<&'a ByteRange>,
    },
    /// A `get_with_metadata` call.
    GetWithMetadata,
    /// A `put` call.
    Put {
        /// Bytes supplied by the caller.
        bytes: &'a Bytes,
        /// Write mode supplied by the caller.
        mode: &'a PutMode,
    },
    /// A `put_streamed` call.
    PutStreamed {
        /// Write mode supplied by the caller.
        mode: &'a PutMode,
    },
    /// A create from source ranges and tail bytes.
    Assemble {
        /// Selected objects and ranges.
        sources: &'a [AssemblySource],
        /// Bytes following the sources.
        tail: &'a [Bytes],
        /// Whole result checksum.
        expected: &'a Checksum,
    },
    /// A `compare_and_swap` call.
    CompareAndSwap {
        /// Expected current etag.
        expected_etag: &'a str,
        /// Bytes supplied by the caller.
        bytes: &'a Bytes,
    },
    /// A `delete` call.
    Delete,
    /// A prefix listing: a `list_entries_from_stream` call or one
    /// `list_child_prefixes` page.
    List,
}

/// One owned operation-log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedOperation {
    /// A metadata-only read.
    Head { key: String },
    /// A byte read.
    Get {
        key: String,
        range: Option<ByteRange>,
        result_bytes: usize,
    },
    /// A full-object read.
    GetWithMetadata { key: String, result_bytes: usize },
    /// A put.
    Put {
        key: String,
        mode: PutMode,
        bytes: usize,
    },
    /// A streamed put.
    PutStreamed {
        key: String,
        mode: PutMode,
        bytes: Option<u64>,
    },
    /// A create from source ranges and tail bytes.
    Assemble {
        key: String,
        sources: Vec<AssemblySource>,
        bytes: usize,
        expected: Checksum,
    },
    /// A compare-and-swap call.
    CompareAndSwap { key: String, bytes: usize },
    /// A delete.
    Delete { key: String },
    /// A prefix-list call.
    List { prefix: String },
}

/// Sorts each run of consecutive HEAD requests in `operations` by key.
///
/// A manifest probe sends a window of HEAD requests at once, and a log
/// records them in the order they finish, so a test that compares a log
/// with an expected sequence sorts the runs first.
pub fn sort_head_runs(operations: &mut [RecordedOperation]) {
    let head = |operation: &RecordedOperation| matches!(operation, RecordedOperation::Head { .. });
    for run in operations.chunk_by_mut(|left, right| head(left) && head(right)) {
        run.sort_by(|left, right| left.key().cmp(right.key()));
    }
}

impl RecordedOperation {
    /// Returns the addressed key or list prefix.
    pub fn key(&self) -> &str {
        match self {
            Self::Head { key }
            | Self::Get { key, .. }
            | Self::GetWithMetadata { key, .. }
            | Self::Put { key, .. }
            | Self::PutStreamed { key, .. }
            | Self::Assemble { key, .. }
            | Self::CompareAndSwap { key, .. }
            | Self::Delete { key } => key,
            Self::List { prefix } => prefix,
        }
    }
}
