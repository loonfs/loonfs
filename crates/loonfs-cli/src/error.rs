//! [`CliError`]: the structured failure every command surfaces.

use crate::config::NAMESPACE_ENV;
use loonfs_api::ErrorCode;
use serde::Serialize;

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
    RuntimeError => "runtime_error",
}

/// Structured failure surfaced by every CLI command (`--json` renders it verbatim).
///
/// `code` is either a shared API error code or a code local to the backend or
/// CLI.
///
/// API codes pass through unchanged so embedded and remote profiles report the
/// same code. Validation after argument parsing uses `invalid_request`.
/// Parser errors use the CLI-local `invalid_usage` code. Other local codes are
/// created by the constructors below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CliError {
    pub code: String,
    /// Feature key for `not_supported` errors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
    pub message: String,
    /// Identifies the invalid input. Body fields use JSON Pointer paths;
    /// query and path parameters use their names; CLI errors use the flag or
    /// argument as written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
    /// Correlation id the server assigned to the failed request; absent for
    /// embedded and local failures, which have no server hop.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Structured context for the code, when the backend carried any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Box<loonfs_api::ErrorDetails>>,
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
        Self {
            code: code.into(),
            feature: None,
            message: message.into(),
            param: None,
            request_id: None,
            details: None,
        }
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

    pub(crate) fn runtime_error(message: impl Into<String>) -> Self {
        Self::new(CliErrorCode::RuntimeError.as_str(), message)
    }

    pub(crate) fn with_param(mut self, param: impl Into<String>) -> Self {
        self.param = Some(param.into());
        self
    }

    pub(crate) fn with_invalid_request_param(self, param: impl Into<String>) -> Self {
        if self.code == ErrorCode::InvalidRequest.as_str() {
            self.with_param(param)
        } else {
            self
        }
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
                "no default namespace is set for profile `{profile}`; use `--namespace`, `{NAMESPACE_ENV}`, or `loonfs use <namespace>`"
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
                "local file `{}` already exists; pass --force to overwrite",
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
