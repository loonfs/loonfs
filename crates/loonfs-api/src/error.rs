//! Stable machine-readable error codes and their caller-action categories.

use std::fmt;

/// The broad caller or operator action required for an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Fix the request before retrying.
    InvalidRequest,
    /// The request is unauthorized and needs different credentials.
    Unauthorized,
    /// The request body exceeds the operation's size limit and must be smaller.
    ContentTooLarge,
    /// The object store rejected credentials that the operator must fix.
    StoragePermissionDenied,
    /// The deployment does not implement the operation and clients must check its capabilities.
    NotSupported,
    /// The requested object does not exist and requires another target or refreshed state.
    NotFound,
    /// The route does not accept this HTTP method.
    MethodNotAllowed,
    /// The target was deleted and its ID is permanently retired.
    Gone,
    /// The create target already exists and requires another ID unless the request is idempotent.
    AlreadyExists,
    /// The request conflicts with current namespace state and requires refreshed state
    /// before retrying.
    Conflict,
    /// The server cancelled work after its deadline, requiring mutation
    /// reconciliation before retrying.
    DeadlineExceeded,
    /// An unavailable condition that may require retry or maintenance.
    Unavailable,
    /// The operation may have committed and requires retry with the same commit ID or
    /// reconciliation.
    OutcomeUnknown,
    /// Durable state is malformed and requires operator repair.
    DataCorruption,
    /// LoonFS encountered an internal failure that should be reported with details.
    Internal,
}

/// Declares the complete wire error-code registry in one place.
///
/// One `Variant => "wire_string"` line emits the enum variant, its
/// [`ErrorCode::ALL`] entry (in registry order), its `as_str` arm, its
/// `parse` arm, and the string-backed serde impls — so registering a new
/// code is one line here plus a [`ErrorCode::kind`] arm and an api.md row.
macro_rules! error_codes {
    (@count) => { 0 };
    (@count $head:ident $($tail:ident)*) => { 1 + error_codes!(@count $($tail)*) };
    ($($variant:ident => $wire:literal),+ $(,)?) => {
        /// A stable machine-readable error reason.
        ///
        /// Clients must tolerate unrecognized codes.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum ErrorCode {
            $(
                #[doc = concat!("Carries the stable wire code `", $wire, "`.")]
                $variant,
            )+
        }

        impl ErrorCode {
            /// Every registered code, in registry order.
            pub const ALL: [ErrorCode; error_codes!(@count $($variant)+)] =
                [$(ErrorCode::$variant,)+];

            /// Returns the stable wire string for this code.
            pub fn as_str(self) -> &'static str {
                match self {
                    $(ErrorCode::$variant => $wire,)+
                }
            }

            /// Parses a registered code string, returning `None` for codes this
            /// build does not know (clients must tolerate those).
            pub fn parse(value: &str) -> Option<ErrorCode> {
                match value {
                    $($wire => Some(ErrorCode::$variant),)+
                    _ => None,
                }
            }
        }

        impl serde::Serialize for ErrorCode {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for ErrorCode {
            // Strict: unknown codes fail to deserialize. Wire structs carry
            // codes as plain strings (`ApiError::code`) precisely so unknown
            // codes stay tolerated; deserialize into `ErrorCode` only where
            // strictness is intended.
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                ErrorCode::parse(&value).ok_or_else(|| {
                    serde::de::Error::custom(format_args!("unknown error code `{value}`"))
                })
            }
        }
    };
}

error_codes! {
    InvalidRequest => "invalid_request",
    Unauthorized => "unauthorized",
    StoragePermissionDenied => "storage_permission_denied",
    ContentTooLarge => "content_too_large",
    NotSupported => "not_supported",
    RouteNotFound => "route_not_found",
    MethodNotAllowed => "method_not_allowed",
    NamespaceNotFound => "namespace_not_found",
    NamespaceDeleted => "namespace_deleted",
    NamespaceExists => "namespace_exists",
    SnapshotNotFound => "snapshot_not_found",
    SnapshotGone => "snapshot_gone",
    SnapshotQuotaExceeded => "snapshot_quota_exceeded",
    ContentNotPrepared => "content_not_prepared",
    PathNotFound => "path_not_found",
    InodeNotFound => "inode_not_found",
    RevisionNotFound => "revision_not_found",
    PathConflict => "path_conflict",
    DirectoryNotEmpty => "directory_not_empty",
    StaleHead => "stale_head",
    StaleRevision => "stale_revision",
    StaleAttributes => "stale_attributes",
    BindingGenerationMismatch => "binding_generation_mismatch",
    NotDeleted => "not_deleted",
    WriterFenced => "writer_fenced",
    WouldCycle => "would_cycle",
    CommitIdReuseConflict => "commit_id_reuse_conflict",
    CommitOutcomeUnknown => "commit_outcome_unknown",
    CommitQueueFull => "commit_queue_full",
    ServerBusy => "server_busy",
    ShuttingDown => "shutting_down",
    DeadlineExceeded => "deadline_exceeded",
    CheckpointUnavailable => "checkpoint_unavailable",
    MaintenanceRequired => "maintenance_required",
    UploadNotFound => "upload_not_found",
    UploadAlreadyCompleted => "upload_already_completed",
    UploadContentConflict => "upload_content_conflict",
    RebootstrapRequired => "rebootstrap_required",
    QueryUnindexable => "query_unindexable",
    IndexLagging => "index_lagging",
    IndexCorrupt => "index_corrupt",
    NamespaceCorrupt => "namespace_corrupt",
    ServerError => "server_error",
}

impl ErrorCode {
    /// Returns the caller-action category for this code.
    ///
    /// The kind agrees with the HTTP status the api.md error table documents
    /// for the code; the server derives the served status from it.
    pub fn kind(self) -> ErrorKind {
        match self {
            // A pattern with no required grams is a property of the request,
            // not the namespace: the caller rewrites the pattern or opts
            // into a capped scan.
            ErrorCode::InvalidRequest | ErrorCode::QueryUnindexable => ErrorKind::InvalidRequest,
            ErrorCode::Unauthorized => ErrorKind::Unauthorized,
            // This is a deployment storage failure, not a caller
            // authorization failure.
            ErrorCode::StoragePermissionDenied => ErrorKind::StoragePermissionDenied,
            ErrorCode::ContentTooLarge => ErrorKind::ContentTooLarge,
            ErrorCode::NotSupported => ErrorKind::NotSupported,
            ErrorCode::NamespaceNotFound
            | ErrorCode::SnapshotNotFound
            | ErrorCode::PathNotFound
            | ErrorCode::InodeNotFound
            | ErrorCode::RevisionNotFound
            | ErrorCode::UploadNotFound
            | ErrorCode::RouteNotFound => ErrorKind::NotFound,
            ErrorCode::MethodNotAllowed => ErrorKind::MethodNotAllowed,
            ErrorCode::NamespaceDeleted | ErrorCode::SnapshotGone => ErrorKind::Gone,
            ErrorCode::NamespaceExists => ErrorKind::AlreadyExists,
            ErrorCode::DeadlineExceeded => ErrorKind::DeadlineExceeded,
            ErrorCode::CommitQueueFull
            | ErrorCode::ServerBusy
            | ErrorCode::ShuttingDown
            | ErrorCode::CheckpointUnavailable
            | ErrorCode::IndexLagging
            | ErrorCode::MaintenanceRequired => ErrorKind::Unavailable,
            ErrorCode::CommitOutcomeUnknown => ErrorKind::OutcomeUnknown,
            ErrorCode::IndexCorrupt | ErrorCode::NamespaceCorrupt => ErrorKind::DataCorruption,
            ErrorCode::ServerError => ErrorKind::Internal,
            // The spec deliberately surfaces precondition failures
            // (`stale_revision`, `stale_head`, `commit_id_reuse_conflict`) as
            // 409 resource-state conflicts, not 412 (api.md, "Standard error
            // contract").
            ErrorCode::ContentNotPrepared
            | ErrorCode::PathConflict
            | ErrorCode::DirectoryNotEmpty
            | ErrorCode::StaleHead
            | ErrorCode::StaleRevision
            // An attribute update was decided against a different attribute
            // revision than the one it wrote from, whether the caller stated
            // that revision or the update's own guard observed it.
            | ErrorCode::StaleAttributes
            | ErrorCode::BindingGenerationMismatch
            // Undelete's target is not the root of a live deletion: a
            // state conflict, resolved by re-reading namespace state.
            | ErrorCode::NotDeleted
            | ErrorCode::WriterFenced
            | ErrorCode::WouldCycle
            | ErrorCode::CommitIdReuseConflict
            | ErrorCode::UploadAlreadyCompleted
            | ErrorCode::UploadContentConflict
            | ErrorCode::RebootstrapRequired
            | ErrorCode::SnapshotQuotaExceeded => ErrorKind::Conflict,
        }
    }

    /// Returns whether this condition can clear without caller or operator action.
    ///
    /// This predicate is deliberately narrower than [`ErrorKind::Unavailable`]:
    /// it includes only admission pressure and shutdown handoff that settle on
    /// their own. Transport failures are classified separately by clients.
    /// Reconciliation, request changes, and maintenance are caller or operator
    /// actions and therefore return `false` here.
    pub fn retryable_without_operator_action(self) -> bool {
        match self {
            ErrorCode::CommitQueueFull | ErrorCode::ServerBusy | ErrorCode::ShuttingDown => true,
            ErrorCode::InvalidRequest
            | ErrorCode::Unauthorized
            | ErrorCode::StoragePermissionDenied
            | ErrorCode::ContentTooLarge
            | ErrorCode::NotSupported
            | ErrorCode::RouteNotFound
            | ErrorCode::MethodNotAllowed
            | ErrorCode::NamespaceNotFound
            | ErrorCode::NamespaceDeleted
            | ErrorCode::NamespaceExists
            | ErrorCode::SnapshotNotFound
            | ErrorCode::SnapshotGone
            | ErrorCode::SnapshotQuotaExceeded
            | ErrorCode::ContentNotPrepared
            | ErrorCode::PathNotFound
            | ErrorCode::InodeNotFound
            | ErrorCode::RevisionNotFound
            | ErrorCode::PathConflict
            | ErrorCode::DirectoryNotEmpty
            | ErrorCode::StaleHead
            | ErrorCode::StaleRevision
            | ErrorCode::StaleAttributes
            | ErrorCode::BindingGenerationMismatch
            | ErrorCode::NotDeleted
            | ErrorCode::WriterFenced
            | ErrorCode::WouldCycle
            | ErrorCode::CommitIdReuseConflict
            | ErrorCode::CommitOutcomeUnknown
            | ErrorCode::DeadlineExceeded
            | ErrorCode::CheckpointUnavailable
            | ErrorCode::MaintenanceRequired
            | ErrorCode::UploadNotFound
            | ErrorCode::UploadAlreadyCompleted
            | ErrorCode::UploadContentConflict
            | ErrorCode::RebootstrapRequired
            | ErrorCode::QueryUnindexable
            | ErrorCode::IndexLagging
            | ErrorCode::IndexCorrupt
            | ErrorCode::NamespaceCorrupt
            | ErrorCode::ServerError => false,
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::ErrorCode;

    #[test]
    fn error_codes_serde_uses_the_wire_strings() {
        for code in ErrorCode::ALL {
            let value = serde_json::to_value(code).expect("serialize error code");
            assert_eq!(value, serde_json::Value::String(code.as_str().to_owned()));
            let parsed: ErrorCode = serde_json::from_value(value).expect("deserialize error code");
            assert_eq!(parsed, code);
        }
        assert!(serde_json::from_str::<ErrorCode>("\"not_a_code\"").is_err());
    }

    #[test]
    fn retryability_is_limited_to_self_clearing_admission_conditions() {
        let retryable: Vec<_> = ErrorCode::ALL
            .into_iter()
            .filter(|code| code.retryable_without_operator_action())
            .collect();

        assert_eq!(
            retryable,
            [
                ErrorCode::CommitQueueFull,
                ErrorCode::ServerBusy,
                ErrorCode::ShuttingDown,
            ]
        );
    }
}
