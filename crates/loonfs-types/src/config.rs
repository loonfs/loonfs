//! Field checks shared by every crate that validates a config file or a
//! config built in code: the server, the store config, the client, and the
//! CLI.

use http::Uri;
use thiserror::Error;

/// A config field that is missing or has an invalid value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConfigFieldError {
    /// A required field is absent or blank.
    #[error("missing `{field}`")]
    Missing {
        /// Path of the field, rooted where the caller's config is.
        field: &'static str,
    },
    /// A present field violates its contract.
    #[error("invalid `{field}`: {reason}")]
    Invalid {
        /// Path of the field, rooted where the caller's config is.
        field: &'static str,
        /// Why the value is invalid, without repeating the field path.
        reason: String,
    },
}

impl ConfigFieldError {
    /// Returns the path of the offending field.
    pub fn field(&self) -> &'static str {
        match self {
            Self::Missing { field } | Self::Invalid { field, .. } => field,
        }
    }
}

/// Refuses a required field whose value is blank.
pub fn validate_non_empty(field: &'static str, value: &str) -> Result<(), ConfigFieldError> {
    if value.trim().is_empty() {
        Err(ConfigFieldError::Missing { field })
    } else {
        Ok(())
    }
}

/// Parses an absolute `http` or `https` URL. A blank value is reported as
/// missing.
pub fn validate_absolute_http_url(
    field: &'static str,
    value: &str,
) -> Result<Uri, ConfigFieldError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ConfigFieldError::Missing { field });
    }

    let uri: Uri =
        trimmed
            .parse()
            .map_err(|err: http::uri::InvalidUri| ConfigFieldError::Invalid {
                field,
                reason: err.to_string(),
            })?;

    match uri.scheme_str() {
        Some("http" | "https") => {}
        Some(other) => {
            return Err(ConfigFieldError::Invalid {
                field,
                reason: format!("scheme must be http or https, got `{other}`"),
            });
        }
        None => {
            return Err(ConfigFieldError::Invalid {
                field,
                reason: "must be an absolute http or https URL".to_owned(),
            });
        }
    }

    if uri.authority().is_none() {
        return Err(ConfigFieldError::Invalid {
            field,
            reason: "must be an absolute http or https URL".to_owned(),
        });
    }

    Ok(uri)
}

/// Returns `value` unless it is absent or blank.
pub fn non_blank(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}
