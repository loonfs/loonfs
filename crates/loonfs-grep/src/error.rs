//! Public grep failures and their wire-code classification.

use crate::root::GrepRootError;
use loonfs::{CoreError, RuntimeError, StoreFailureClass};
use loonfs_api::{ErrorCode, ErrorKind};
use thiserror::Error;

/// Failure returned by grep queries or maintenance.
///
/// The variants preserve only distinctions that change caller or operator
/// action. Detailed root loading and publication failures remain available
/// internally as [`GrepRootError`].
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum GrepError {
    // Both messages name the capability key clients gate on (`query.grep`),
    // the same one capability discovery advertises and the `feature` field
    // of a `not_supported` response carries.
    /// The namespace has no active grep root.
    #[error("feature `query.grep` is not enabled on this namespace")]
    NotEnabled,
    /// The namespace's grep backfill has not completed.
    #[error(
        "feature `query.grep` is enabled but its backfill has not completed on this namespace"
    )]
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
    /// A genuine runtime failure encountered while grep read or wrote
    /// through the filesystem handles.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

/// Grep names the runtime's own error vocabulary for the conditions it
/// shares with every other reader — an invalid query, an unusable cursor, a
/// lost basis — so one code means one thing whoever produced it.
impl From<CoreError> for GrepError {
    fn from(error: CoreError) -> Self {
        Self::Runtime(RuntimeError::Core(error))
    }
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
                _ => ErrorCode::ServerError,
            },
            Self::CorruptIndex { .. } => ErrorCode::IndexCorrupt,
            Self::PublicationConflict { .. } => ErrorCode::StaleHead,
            Self::Runtime(error) => error.code(),
        }
    }

    /// Returns an error message safe to show to users.
    pub fn public_message(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::StoreUnavailable { class, .. } => class.public_message(),
            Self::Runtime(error) => error.public_message(),
            Self::PublicationConflict { .. } => {
                std::borrow::Cow::Borrowed("grep index publication conflict; retry")
            }
            _ => std::borrow::Cow::Owned(self.to_string()),
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
