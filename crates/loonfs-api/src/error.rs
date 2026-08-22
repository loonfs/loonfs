//! The wire error registry: every stable machine-readable error code and
//! its caller-action category.

use std::fmt;

/// Broad error category for caller or operator action.
///
/// Each kind implies one served HTTP status (`status_for_error_kind` in
/// `loonfs-server`); a code's kind and its documented status in the API spec
/// must agree in spirit, and the server's spec-table sync test enforces the
/// composition exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Fix the request before retrying.
    InvalidRequest,
    /// The request was not authorized. Fix credentials before retrying.
    Unauthorized,
    /// The request body exceeds the deployment's size limit for this
    /// operation. Send a smaller payload (for uploads, prefer `direct_put`);
    /// retrying unchanged will not succeed.
    ContentTooLarge,
    /// The backing object store rejected the deployment's credentials. The
    /// operator must fix the credentials or bucket policy before retrying.
    StoragePermissionDenied,
    /// The deployment does not implement this operation. Gate on the
    /// capability document instead of retrying.
    NotSupported,
    /// The requested object does not exist. Refresh state or choose another target.
    NotFound,
    /// The path routes somewhere, but not for this HTTP method. Fix the
    /// request; retrying unchanged will not succeed.
    MethodNotAllowed,
    /// The target was deleted and its id is permanently retired. Do not
    /// retry; choose another target.
    Gone,
    /// The create target already exists. Pick another id or treat this as idempotent.
    AlreadyExists,
    /// The request raced with current namespace state, or a caller-supplied
    /// precondition (base revision, expected head) no longer holds. Re-read
    /// fresh state, re-plan, and retry if desired.
    Conflict,
    /// The server cancelled work that exceeded its configured request
    /// deadline. Reconcile any mutation before deciding whether to retry.
    DeadlineExceeded,
    /// Status grouping for conditions served as unavailable.
    ///
    /// This kind is not a retry predicate: some grouped conditions require
    /// maintenance before another attempt can succeed. Use
    /// [`ErrorCode::retryable_without_operator_action`] for that decision.
    Unavailable,
    /// The operation may have committed: its acknowledgment was lost. Retry
    /// with the same commit id or reconcile against namespace state; do not
    /// assume failure.
    OutcomeUnknown,
    /// Durable state is malformed. Treat this as operator or repair work.
    DataCorruption,
    /// LoonFS hit an internal failure. Capture details and report it.
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
        /// Stable machine-readable error reason.
        ///
        /// This is the complete registry of `code` values carried by
        /// [`ApiError`](crate::ApiError) bodies and embedded errors. Codes are
        /// permanent once released: the API spec documents each code's meaning and HTTP
        /// status, and clients must tolerate codes they do not recognize.
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
    ContentNotPrepared => "content_not_prepared",
    PathNotFound => "path_not_found",
    InodeNotFound => "inode_not_found",
    RevisionNotFound => "revision_not_found",
    PathConflict => "path_conflict",
    DirectoryNotEmpty => "directory_not_empty",
    StaleHead => "stale_head",
    StaleRevision => "stale_revision",
    StaleAttributes => "stale_attributes",
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
            | ErrorCode::PathNotFound
            | ErrorCode::InodeNotFound
            | ErrorCode::RevisionNotFound
            | ErrorCode::UploadNotFound
            | ErrorCode::RouteNotFound => ErrorKind::NotFound,
            ErrorCode::MethodNotAllowed => ErrorKind::MethodNotAllowed,
            ErrorCode::NamespaceDeleted => ErrorKind::Gone,
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
            // Undelete's target is not the root of a live deletion: a
            // state conflict, resolved by re-reading namespace state.
            | ErrorCode::NotDeleted
            | ErrorCode::WriterFenced
            | ErrorCode::WouldCycle
            | ErrorCode::CommitIdReuseConflict
            | ErrorCode::UploadAlreadyCompleted
            | ErrorCode::UploadContentConflict
            | ErrorCode::RebootstrapRequired => ErrorKind::Conflict,
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
            | ErrorCode::ContentNotPrepared
            | ErrorCode::PathNotFound
            | ErrorCode::InodeNotFound
            | ErrorCode::RevisionNotFound
            | ErrorCode::PathConflict
            | ErrorCode::DirectoryNotEmpty
            | ErrorCode::StaleHead
            | ErrorCode::StaleRevision
            | ErrorCode::StaleAttributes
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
    fn error_codes_round_trip_through_their_strings() {
        for code in ErrorCode::ALL {
            assert_eq!(ErrorCode::parse(code.as_str()), Some(code));
        }
        assert_eq!(ErrorCode::parse("not_a_code"), None);
    }

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
