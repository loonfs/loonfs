//! Public grep failures and their wire-code classification.

use crate::root::GrepRootError;
use loonfs_api::{ErrorCode, ErrorKind};
use loonfs_core::{Error as CoreError, StoreFailureClass};
use thiserror::Error;

/// Failure returned by grep queries or maintenance.
///
/// The variants preserve only distinctions that change caller or operator
/// action. Detailed root loading and publication failures remain available
/// internally as [`GrepRootError`].
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum GrepError {
    /// The namespace has no active grep root.
    #[error("feature `grep.index` is not materialized on this namespace")]
    NotEnabled,
    /// The namespace's grep backfill has not completed.
    #[error("feature `grep.index` is not materialized on this namespace")]
    Backfilling,
    /// The backing provider could not serve grep-owned state.
    #[error("object-store operation failed for grep state `{object_key}`: {message}")]
    StoreUnavailable {
        /// Grep-owned object or prefix the provider operation targeted.
        object_key: String,
        /// Provider failure text without a repeated object key.
        message: String,
        /// Classification retained from the provider boundary.
        class: StoreFailureClass,
    },
    /// Grep's rebuildable derived state failed validation.
    #[error("grep index is corrupt: {message}; disable and re-enable grep to rebuild it")]
    CorruptIndex {
        /// Root, manifest, or segment validation failure.
        message: String,
    },
    /// A grep root compare-and-swap lost to another publisher.
    #[error("grep root publication conflict for `{object_key}`; retry")]
    PublicationConflict {
        /// Mutable grep root whose publication raced.
        object_key: String,
    },
    /// A genuine core namespace failure encountered while grep read core state.
    #[error(transparent)]
    Core(#[from] CoreError),
}

impl GrepError {
    /// Returns the caller-action category for this failure.
    pub fn kind(&self) -> ErrorKind {
        self.code().kind()
    }

    /// Returns the stable wire code for this failure.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::NotEnabled | Self::Backfilling => ErrorCode::NotSupported,
            Self::StoreUnavailable { class, .. } => match class {
                StoreFailureClass::PermissionDenied => ErrorCode::PermissionDenied,
                StoreFailureClass::Other => ErrorCode::ServerError,
            },
            Self::CorruptIndex { .. } => ErrorCode::IndexCorrupt,
            Self::PublicationConflict { .. } => ErrorCode::StaleHead,
            Self::Core(error) => error.code(),
        }
    }
}

impl From<GrepRootError> for GrepError {
    fn from(error: GrepRootError) -> Self {
        match error {
            GrepRootError::Store {
                object_key,
                message,
                class,
            } => Self::StoreUnavailable {
                object_key,
                message,
                class,
            },
            GrepRootError::MissingEtag { object_key } => Self::StoreUnavailable {
                object_key,
                message: "grep root has no etag for compare-and-swap".to_owned(),
                class: StoreFailureClass::Other,
            },
            GrepRootError::Conflict { object_key } => Self::PublicationConflict { object_key },
            error @ (GrepRootError::Corrupt { .. }
            | GrepRootError::MissingManifest { .. }
            | GrepRootError::IdentityMismatch { .. }
            | GrepRootError::AdvanceIdentityMismatch { .. }) => Self::CorruptIndex {
                message: error.to_string(),
            },
        }
    }
}

/// Result type used by grep query and maintenance entrypoints.
pub type Result<T> = std::result::Result<T, GrepError>;

#[cfg(test)]
mod tests {
    use super::{ErrorCode, GrepError, StoreFailureClass};

    #[test]
    fn store_failure_class_survives_the_grep_boundary() {
        for (class, expected) in [
            (
                StoreFailureClass::PermissionDenied,
                ErrorCode::PermissionDenied,
            ),
            (StoreFailureClass::Other, ErrorCode::ServerError),
        ] {
            let error = GrepError::StoreUnavailable {
                object_key: "namespaces/demo/extensions/grep/root.json".to_owned(),
                message: "provider failure".to_owned(),
                class,
            };
            assert_eq!(error.code(), expected);
        }
    }
}
