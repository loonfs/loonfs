//! Public grep failures and their wire-code classification.

use crate::entry::{GrepEntryError, GREP_ENTRY_VERSION};
use loonfs::CoreError;
use loonfs_objectstore::ObjectStoreError;
use loonfs_objectstore::ObjectStoreErrorKind;
use loonfs_types::{ErrorCode, ErrorKind};
use thiserror::Error;

/// Failure returned by grep queries or maintenance.
///
/// The variants preserve only distinctions that change caller or operator
/// action.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum GrepError {
    // Both messages name the capability key clients gate on (`query.grep`),
    // the same one capability discovery advertises and the `feature` field
    // of a `not_supported` response includes.
    /// The namespace's manifest has no enabled grep entry.
    #[error("feature `query.grep` is not enabled on this namespace")]
    NotEnabled,
    /// The namespace's grep backfill has not completed.
    #[error(
        "feature `query.grep` is enabled but its backfill has not completed on this namespace"
    )]
    Backfilling,
    /// The pattern does not parse as a regular expression, or it can't be
    /// planned against the index.
    #[error("invalid search query: {0}")]
    InvalidPattern(String),
    /// The pattern has no run of literal bytes for the trigram index, and the
    /// query cannot search without the index: `allow_scan` is off, or the
    /// scan would pass its file budget.
    #[error("the pattern requires no literal bytes and cannot use the index: {0}")]
    QueryUnindexable(String),
    /// The commits the index has not reached change more files than one
    /// query scans, and the query did not set `allow_stale`.
    #[error(
        "the grep index trails the head by {behind_commits} commits, past the \
         exhaustive-scan budget; run maintenance or set allow_stale"
    )]
    IndexLagging {
        /// Commits between the position the index was built through and the
        /// head the query reads.
        behind_commits: u64,
    },
    /// The backing provider could not serve grep-owned state.
    #[error("object-store operation failed for grep state `{object_key}`: {message}")]
    StoreUnavailable {
        /// Grep-owned object or prefix the provider operation targeted.
        object_key: String,
        /// Provider failure text without a repeated object key.
        message: String,
        /// Classification retained from the provider boundary.
        kind: ObjectStoreErrorKind,
    },
    /// Grep's rebuildable derived state failed validation.
    #[error("grep index is corrupt: {message}; disable and re-enable grep to rebuild it")]
    IndexCorrupt {
        /// Entry or segment validation failure.
        message: String,
    },
    /// Grep's entry has a format version that this build doesn't read. The
    /// build doesn't disable such an entry either, so it never discards an
    /// index that it can't read.
    #[error(
        "grep index has format version `{version}`, and this build reads version `{current}`; \
         query or disable the index with a build that reads version `{version}`",
        current = GREP_ENTRY_VERSION
    )]
    UnsupportedVersion {
        /// The version of the entry.
        version: u32,
    },
    /// A genuine runtime failure encountered while grep read or wrote
    /// through the filesystem handles, or published its entry.
    #[error(transparent)]
    Runtime(#[from] loonfs::Error),
}

/// Grep names the runtime's own error vocabulary for the conditions it
/// shares with every other reader — an unusable cursor, a lost basis — so
/// one code means one thing whoever produced it.
impl From<CoreError> for GrepError {
    fn from(error: CoreError) -> Self {
        Self::Runtime(loonfs::Error::Core(error))
    }
}

impl GrepError {
    /// Builds the grep error for a failed object-store operation on
    /// `object_key`.
    ///
    /// An immutable key that contains a different object means that grep's
    /// derived state is corrupt, so that error becomes
    /// [`GrepError::IndexCorrupt`].
    pub(crate) fn store(object_key: impl Into<String>, error: &ObjectStoreError) -> Self {
        match error {
            ObjectStoreError::DifferentObject { object_key: key } => Self::IndexCorrupt {
                message: format!("immutable object `{key}` already exists with different bytes"),
            },
            error => Self::StoreUnavailable {
                object_key: object_key.into(),
                message: error.public_message().into_owned(),
                kind: error.kind(),
            },
        }
    }

    /// Builds the grep error for an invalid grep entry in `location`, where
    /// `error` says why the entry is invalid.
    ///
    /// The error for an entry of another version is
    /// [`GrepError::UnsupportedVersion`]. The error for any other invalid
    /// entry is [`GrepError::IndexCorrupt`], and disabling grep replaces that
    /// entry.
    pub(crate) fn invalid_entry(location: impl std::fmt::Display, error: GrepEntryError) -> Self {
        match error {
            GrepEntryError::UnsupportedVersion { version } => Self::UnsupportedVersion { version },
            error => Self::IndexCorrupt {
                message: format!("the grep entry of {location} is invalid: {error}"),
            },
        }
    }

    /// Preserves public error fields for embedded and HTTP callers.
    pub fn to_error_response(&self) -> loonfs_types::ErrorResponse {
        if let Self::Runtime(error) = self {
            return error.to_error_response();
        }
        loonfs_types::ErrorResponse {
            code: self.code().as_str().to_owned(),
            message: self.public_message().into_owned(),
            param: matches!(self, Self::InvalidPattern(_)).then(|| "pattern".to_owned()),
            feature: matches!(self, Self::NotEnabled | Self::Backfilling)
                .then(|| loonfs_types::FEATURE_QUERY_GREP.to_owned()),
            request_id: None,
            details: None,
        }
    }

    /// Returns the caller-action category for this failure.
    pub fn kind(&self) -> ErrorKind {
        self.code().kind()
    }

    /// Returns the stable wire code for this failure.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::NotEnabled | Self::Backfilling => ErrorCode::NotSupported,
            Self::InvalidPattern(_) => ErrorCode::InvalidRequest,
            Self::QueryUnindexable(_) => ErrorCode::QueryUnindexable,
            Self::IndexLagging { .. } => ErrorCode::IndexLagging,
            Self::StoreUnavailable { kind, .. } => loonfs::classify_store_failure(*kind),
            Self::IndexCorrupt { .. } | Self::UnsupportedVersion { .. } => ErrorCode::IndexCorrupt,
            Self::Runtime(error) => error.code(),
        }
    }

    /// Returns an error message safe to show to users.
    pub fn public_message(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Self::StoreUnavailable { kind, .. } => kind.public_message(),
            Self::Runtime(error) => error.public_message(),
            _ => std::borrow::Cow::Owned(self.to_string()),
        }
    }
}

/// Result type used by grep query and maintenance entrypoints.
pub type Result<T> = std::result::Result<T, GrepError>;

#[cfg(test)]
mod tests {
    use super::{ErrorCode, GrepError, ObjectStoreError, ObjectStoreErrorKind};

    #[test]
    fn store_failure_kind_survives_the_grep_boundary() {
        for (kind, expected) in [
            (
                ObjectStoreErrorKind::PermissionDenied,
                ErrorCode::StoragePermissionDenied,
            ),
            (ObjectStoreErrorKind::InvalidRequest, ErrorCode::ServerError),
            (ObjectStoreErrorKind::Other, ErrorCode::ServerError),
        ] {
            let error = GrepError::StoreUnavailable {
                object_key: "namespaces/demo/extensions/grep/segments/idx_00000000000000000000000001.sst.zst".to_owned(),
                message: "provider failure".to_owned(),
                kind,
            };
            assert_eq!(error.code(), expected);
        }

        let key = "namespaces/demo/extensions/grep/segments/seg";
        let different = ObjectStoreError::DifferentObject {
            object_key: key.to_owned(),
        };
        assert_eq!(
            GrepError::store(key, &different).code(),
            ErrorCode::IndexCorrupt
        );
    }
}
