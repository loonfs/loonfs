//! Object-store operation descriptions used by wrapper predicates.

use bytes::Bytes;
use loonfs_objectstore::{ByteRange, PutMode};

/// A broad operation class selected by a test wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationClass {
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
    /// Any `put` mode.
    Put,
    /// Overwriting puts.
    PutOverwrite,
    /// Create-if-absent puts.
    PutCreateIfAbsent,
    /// Compare-and-swap calls and CAS-mode puts.
    CompareAndSwap,
    /// Deletes.
    Delete,
    /// Prefix-list calls.
    List,
}

impl OperationClass {
    pub(crate) fn matches(self, kind: &OperationKind<'_>) -> bool {
        match self {
            Self::Any => true,
            Self::Head => matches!(kind, OperationKind::Head),
            Self::Get => matches!(kind, OperationKind::Get { .. }),
            Self::GetWithMetadata => matches!(kind, OperationKind::GetWithMetadata),
            Self::Read => matches!(
                kind,
                OperationKind::Get { .. } | OperationKind::GetWithMetadata
            ),
            Self::Put => matches!(kind, OperationKind::Put { .. }),
            Self::PutOverwrite => matches!(
                kind,
                OperationKind::Put {
                    mode: PutMode::Overwrite,
                    ..
                }
            ),
            Self::PutCreateIfAbsent => matches!(
                kind,
                OperationKind::Put {
                    mode: PutMode::CreateIfAbsent,
                    ..
                }
            ),
            Self::CompareAndSwap => matches!(
                kind,
                OperationKind::CompareAndSwap { .. }
                    | OperationKind::Put {
                        mode: PutMode::CompareAndSwap { .. },
                        ..
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
    /// A `compare_and_swap` call.
    CompareAndSwap {
        /// Expected current etag.
        expected_etag: &'a str,
        /// Bytes supplied by the caller.
        bytes: &'a Bytes,
    },
    /// A `delete` call.
    Delete,
    /// A `list_prefix_stream` call.
    List,
}
