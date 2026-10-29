//! [`CliError`]: the structured failure every command surfaces.

use crate::config::NAMESPACE_ENV;
use loonfs_client::ClientError;
use loonfs_types::config::ConfigFieldError;
use loonfs_types::{ErrorCode, ErrorResponse};
use serde::{Deserialize, Serialize};

macro_rules! cli_error_codes {
    (@count) => { 0 };
    (@count $head:ident $($tail:ident)*) => { 1 + cli_error_codes!(@count $($tail)*) };
    ($($variant:ident => $value:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) enum CliErrorCode {
            $($variant,)+
        }

        impl CliErrorCode {
            #[cfg(test)]
            pub(crate) const ALL: [Self; cli_error_codes!(@count $($variant)+)] =
                [$(Self::$variant,)+];

            pub(crate) fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)+
                }
            }

            fn parse(value: &str) -> Option<Self> {
                match value {
                    $($value => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

cli_error_codes! {
    IoError => "io_error",
    InvalidConfig => "invalid_config",
    ProfileNotFound => "profile_not_found",
    NoDefaultProfile => "no_default_profile",
    NoDefaultNamespace => "no_default_namespace",
    ProfileAlreadyExists => "profile_already_exists",
    ConfigAlreadyExists => "config_already_exists",
    NonInteractiveInputRequired => "non_interactive_input_required",
    DestinationExists => "destination_exists",
    InvalidUsage => "invalid_usage",
    JsonNotSupportedForStreaming => "json_not_supported_for_streaming",
    Cancelled => "cancelled",
    ClientError => "client_error",
}

/// Structured failure surfaced by every CLI command (`--json` renders it verbatim).
///
/// The failure is the wire error body, so its `code` is either a shared API
/// error code or a code local to the backend or CLI.
///
/// API codes pass through unchanged so embedded and remote profiles report the
/// same code. Validation after argument parsing uses `invalid_request`.
/// Parser errors use the CLI-local `invalid_usage` code. Other local codes are
/// created by the constructors below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct CliError(ErrorResponse);

/// The result of a CLI operation.
pub(crate) type Result<T, E = CliError> = std::result::Result<T, E>;

impl std::ops::Deref for CliError {
    type Target = ErrorResponse;

    fn deref(&self) -> &ErrorResponse {
        &self.0
    }
}

impl std::ops::DerefMut for CliError {
    fn deref_mut(&mut self) -> &mut ErrorResponse {
        &mut self.0
    }
}

impl CliError {
    /// An `io_error` that names the file it concerns, so a failure inside a
    /// loop over many files stays attributable.
    pub(crate) fn io_for_path(path: &std::path::Path, error: std::io::Error) -> Self {
        Self::new(
            CliErrorCode::IoError.as_str(),
            format!("i/o error for `{}`: {error}", path.display()),
        )
    }

    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self(ErrorResponse {
            code: code.into(),
            feature: None,
            message: message.into(),
            param: None,
            request_id: None,
            details: None,
        })
    }

    pub(crate) fn invalid_config(message: impl Into<String>) -> Self {
        Self::new(CliErrorCode::InvalidConfig.as_str(), message)
    }

    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest.as_str(), message)
    }

    pub(crate) fn client_error(message: impl Into<String>) -> Self {
        Self::new(CliErrorCode::ClientError.as_str(), message)
    }

    pub(crate) fn io_error(message: impl Into<String>) -> Self {
        Self::new(CliErrorCode::IoError.as_str(), message)
    }

    pub(crate) fn with_param(mut self, param: impl Into<String>) -> Self {
        self.0.param = Some(param.into());
        self
    }

    pub(crate) fn profile_not_found(name: &str) -> Self {
        Self::new(
            CliErrorCode::ProfileNotFound.as_str(),
            format!("profile `{name}` not found"),
        )
    }

    pub(crate) fn no_default_profile() -> Self {
        Self::new(
            CliErrorCode::NoDefaultProfile.as_str(),
            "no default profile is set; use `profile use` or `--profile`",
        )
    }

    pub(crate) fn no_default_namespace(profile: &str) -> Self {
        Self::new(
            CliErrorCode::NoDefaultNamespace.as_str(),
            format!(
                "no default namespace is set for profile `{profile}`; use `--namespace`, `{NAMESPACE_ENV}`, or `loonfs use <namespace>`; create one with `loonfs namespace create <namespace>`"
            ),
        )
    }

    /// Whether namespace resolution found no configured default.
    pub(crate) fn is_no_default_namespace(&self) -> bool {
        CliErrorCode::parse(&self.code) == Some(CliErrorCode::NoDefaultNamespace)
    }

    pub(crate) fn profile_already_exists(name: &str) -> Self {
        Self::new(
            CliErrorCode::ProfileAlreadyExists.as_str(),
            format!("profile `{name}` already exists"),
        )
    }

    pub(crate) fn config_already_exists(path: &str) -> Self {
        Self::new(
            CliErrorCode::ConfigAlreadyExists.as_str(),
            format!(
                "config file already exists at `{path}`. use `loonfs profile create` to create a new profile, `loonfs profile update` to modify an existing profile, or `loonfs profile use` to change the default profile"
            ),
        )
    }

    /// Interactive input is required but unavailable; `requirement` says
    /// what to pass instead.
    pub(crate) fn non_interactive_input_required(requirement: impl Into<String>) -> Self {
        Self::new(
            CliErrorCode::NonInteractiveInputRequired.as_str(),
            requirement,
        )
    }

    pub(crate) fn non_interactive_field_required(field: &str) -> Self {
        Self::non_interactive_input_required(format!(
            "missing required `{field}` while `--no-input` is active"
        ))
        .with_param(format!("--{field}"))
    }

    pub(crate) fn destination_exists(path: &std::path::Path) -> Self {
        Self::new(
            CliErrorCode::DestinationExists.as_str(),
            format!(
                "local file `{}` already exists; pass `--force` to overwrite",
                path.display()
            ),
        )
    }

    /// A command line rejected by the argument parser. Validation that runs
    /// after parsing uses `invalid_request` instead.
    pub(crate) fn invalid_usage(message: impl Into<String>) -> Self {
        Self::new(CliErrorCode::InvalidUsage.as_str(), message)
    }

    pub(crate) fn json_not_supported() -> Self {
        Self::new(
            CliErrorCode::JsonNotSupportedForStreaming.as_str(),
            "`--json` is not supported for this command",
        )
    }

    pub(crate) fn io(error: std::io::Error) -> Self {
        Self::new(
            CliErrorCode::IoError.as_str(),
            format!("i/o error: {error}"),
        )
    }

    pub(crate) fn cancelled() -> Self {
        Self::new(CliErrorCode::Cancelled.as_str(), "operation cancelled")
    }
}

impl From<ConfigFieldError> for CliError {
    fn from(error: ConfigFieldError) -> Self {
        Self::invalid_config(error.to_string())
    }
}

impl From<ClientError> for CliError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::ConfigIo(message) | ClientError::ConfigDecode(message) => {
                Self::invalid_config(message)
            }
            ClientError::ConfigField(error) => error.into(),
            ClientError::InvalidNamespacePath(message) => Self::invalid_request(message),
            ClientError::Http(message)
            | ClientError::Json(message)
            | ClientError::Protocol(message) => Self::client_error(message),
            ClientError::Api { response, .. } => Self(*response),
            error @ ClientError::UploadTooLarge { .. } => {
                Self::new(ErrorCode::ContentTooLarge.as_str(), error.to_string())
            }
            ClientError::Io(message) => Self::io_error(format!("i/o error: {message}")),
            // `ClientError` is non-exhaustive across crate boundaries, so future
            // variants map to a generic transport failure. Add an explicit arm when a
            // new variant needs a more specific CLI code.
            other => Self::client_error(other.to_string()),
        }
    }
}

impl From<ErrorResponse> for CliError {
    fn from(error: ErrorResponse) -> Self {
        Self(error)
    }
}

impl From<loonfs::Error> for CliError {
    fn from(error: loonfs::Error) -> Self {
        error.to_error_response().into()
    }
}

impl From<loonfs_grep::GrepError> for CliError {
    fn from(error: loonfs_grep::GrepError) -> Self {
        error.to_error_response().into()
    }
}

#[cfg(test)]
mod tests {
    use super::CliErrorCode;

    #[test]
    fn cli_error_code_values_are_pinned() {
        for code in CliErrorCode::ALL {
            assert_eq!(code.as_str(), snake_case(&format!("{code:?}")));
        }
    }

    fn snake_case(value: &str) -> String {
        let mut result = String::with_capacity(value.len());
        for (index, character) in value.chars().enumerate() {
            if index > 0 && character.is_ascii_uppercase() {
                result.push('_');
            }
            result.push(character.to_ascii_lowercase());
        }
        result
    }
}
