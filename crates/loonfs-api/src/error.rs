use std::fmt;

/// Broad error category for caller or operator action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Fix the request before retrying.
    InvalidRequest,
    /// The request was not authorized. Fix credentials before retrying.
    Unauthorized,
    /// The caller may not perform this operation. Request access; retrying
    /// unchanged will not succeed.
    PermissionDenied,
    /// The deployment does not implement this operation. Gate on the
    /// capability document instead of retrying.
    NotSupported,
    /// The requested object does not exist. Refresh state or choose another target.
    NotFound,
    /// The create target already exists. Pick another id or treat this as idempotent.
    AlreadyExists,
    /// The request raced with current namespace state. Re-read and retry if desired.
    Conflict,
    /// A caller-supplied precondition was false. Re-plan against fresh state.
    PreconditionFailed,
    /// The system is temporarily unavailable. Back off and retry.
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

/// Stable machine-readable error reason.
///
/// This is the complete registry of `code` values carried by
/// [`ApiError`](crate::ApiError) bodies and embedded errors. Codes are
/// permanent once released: the API spec documents each code's meaning and HTTP
/// status, and clients must tolerate codes they do not recognize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    InvalidRequest,
    Unauthorized,
    NotSupported,
    NamespaceNotFound,
    NamespaceDeleted,
    NamespaceExists,
    NamespacePartial,
    PathNotFound,
    RevisionNotFound,
    PathConflict,
    DirectoryNotEmpty,
    StaleHead,
    StaleRevision,
    TombstoneConflict,
    WriterFenced,
    WouldCycle,
    CommitIdReuseConflict,
    CommitOutcomeUnknown,
    CommitQueueFull,
    CheckpointUnavailable,
    MaintenanceRequired,
    UploadNotFound,
    UploadAlreadyCompleted,
    UploadContentConflict,
    RebootstrapRequired,
    NamespaceCorrupt,
    ServerError,
}

impl ErrorCode {
    /// Every registered code, in registry order.
    pub const ALL: [ErrorCode; 27] = [
        ErrorCode::InvalidRequest,
        ErrorCode::Unauthorized,
        ErrorCode::NotSupported,
        ErrorCode::NamespaceNotFound,
        ErrorCode::NamespaceDeleted,
        ErrorCode::NamespaceExists,
        ErrorCode::NamespacePartial,
        ErrorCode::PathNotFound,
        ErrorCode::RevisionNotFound,
        ErrorCode::PathConflict,
        ErrorCode::DirectoryNotEmpty,
        ErrorCode::StaleHead,
        ErrorCode::StaleRevision,
        ErrorCode::TombstoneConflict,
        ErrorCode::WriterFenced,
        ErrorCode::WouldCycle,
        ErrorCode::CommitIdReuseConflict,
        ErrorCode::CommitOutcomeUnknown,
        ErrorCode::CommitQueueFull,
        ErrorCode::CheckpointUnavailable,
        ErrorCode::MaintenanceRequired,
        ErrorCode::UploadNotFound,
        ErrorCode::UploadAlreadyCompleted,
        ErrorCode::UploadContentConflict,
        ErrorCode::RebootstrapRequired,
        ErrorCode::NamespaceCorrupt,
        ErrorCode::ServerError,
    ];

    pub fn kind(self) -> ErrorKind {
        match self {
            ErrorCode::InvalidRequest => ErrorKind::InvalidRequest,
            ErrorCode::Unauthorized => ErrorKind::Unauthorized,
            ErrorCode::NotSupported => ErrorKind::NotSupported,
            ErrorCode::NamespaceNotFound
            | ErrorCode::NamespaceDeleted
            | ErrorCode::PathNotFound
            | ErrorCode::RevisionNotFound
            | ErrorCode::UploadNotFound => ErrorKind::NotFound,
            ErrorCode::NamespaceExists => ErrorKind::AlreadyExists,
            ErrorCode::StaleRevision => ErrorKind::PreconditionFailed,
            ErrorCode::CommitQueueFull
            | ErrorCode::CheckpointUnavailable
            | ErrorCode::MaintenanceRequired => ErrorKind::Unavailable,
            ErrorCode::CommitOutcomeUnknown => ErrorKind::OutcomeUnknown,
            ErrorCode::NamespaceCorrupt => ErrorKind::DataCorruption,
            ErrorCode::ServerError => ErrorKind::Internal,
            ErrorCode::NamespacePartial
            | ErrorCode::PathConflict
            | ErrorCode::DirectoryNotEmpty
            | ErrorCode::StaleHead
            | ErrorCode::TombstoneConflict
            | ErrorCode::WriterFenced
            | ErrorCode::WouldCycle
            | ErrorCode::CommitIdReuseConflict
            | ErrorCode::UploadAlreadyCompleted
            | ErrorCode::UploadContentConflict
            | ErrorCode::RebootstrapRequired => ErrorKind::Conflict,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidRequest => "invalid_request",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::NotSupported => "not_supported",
            ErrorCode::NamespaceNotFound => "namespace_not_found",
            ErrorCode::NamespaceDeleted => "namespace_deleted",
            ErrorCode::NamespaceExists => "namespace_exists",
            ErrorCode::NamespacePartial => "namespace_partial",
            ErrorCode::PathNotFound => "path_not_found",
            ErrorCode::RevisionNotFound => "revision_not_found",
            ErrorCode::PathConflict => "path_conflict",
            ErrorCode::DirectoryNotEmpty => "directory_not_empty",
            ErrorCode::StaleHead => "stale_head",
            ErrorCode::StaleRevision => "stale_revision",
            ErrorCode::TombstoneConflict => "tombstone_conflict",
            ErrorCode::WriterFenced => "writer_fenced",
            ErrorCode::WouldCycle => "would_cycle",
            ErrorCode::CommitIdReuseConflict => "commit_id_reuse_conflict",
            ErrorCode::CommitOutcomeUnknown => "commit_outcome_unknown",
            ErrorCode::CommitQueueFull => "commit_queue_full",
            ErrorCode::CheckpointUnavailable => "checkpoint_unavailable",
            ErrorCode::MaintenanceRequired => "maintenance_required",
            ErrorCode::UploadNotFound => "upload_not_found",
            ErrorCode::UploadAlreadyCompleted => "upload_already_completed",
            ErrorCode::UploadContentConflict => "upload_content_conflict",
            ErrorCode::RebootstrapRequired => "rebootstrap_required",
            ErrorCode::NamespaceCorrupt => "namespace_corrupt",
            ErrorCode::ServerError => "server_error",
        }
    }

    /// Parses a registered code string, returning `None` for codes this
    /// build does not know (clients must tolerate those).
    pub fn parse(value: &str) -> Option<ErrorCode> {
        ErrorCode::ALL
            .into_iter()
            .find(|code| code.as_str() == value)
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
}
