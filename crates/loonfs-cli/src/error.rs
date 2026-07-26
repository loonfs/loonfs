//! [`CliError`]: the structured failure every command surfaces.

use serde::{Deserialize, Serialize};

/// Structured failure surfaced by every CLI command (`--json` renders it verbatim).
///
/// `code` draws from exactly three namespaces:
///
/// - **Registry codes** ([`loonfs_api::ErrorCode`]) pass through verbatim from
///   whichever backend produced them, so embedded and remote profiles surface
///   the same code for the same failure. Never restate a registry code as a
///   string literal; use `ErrorCode::X.as_str()` or an error's `code()`.
/// - **Backend-local codes** ([`loonfs_client::backend::BackendError`]) pass
///   through verbatim from the backend seam: `invalid_config`,
///   `invalid_input`, `client_error`, `io_error`, and `runtime_error`.
/// - **CLI-local codes** cover failures that never reach a backend. The
///   complete list, each owned by a constructor below, is: `invalid_config`,
///   `invalid_input`, `profile_not_found`, `no_default_profile`,
///   `no_default_namespace`, `profile_already_exists`,
///   `config_already_exists`, `destination_exists`,
///   `non_interactive_input_required`, `json_not_supported_for_streaming`,
///   `io_error`, and `cancelled`. Overlaps with backend-local codes are
///   deliberate: the same string means the same thing on both sides of the
///   seam — a local configuration failure is `invalid_config` whether the
///   CLI or the backend seam caught it, and the registry codes are reserved
///   for failures a server could also produce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CliError {
    pub code: String,
    pub message: String,
    /// Correlation id the server assigned to the failed request; absent for
    /// embedded and local failures, which have no server hop.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub request_id: Option<String>,
    /// Structured context for the code, when the backend carried any.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub details: Option<Box<loonfs_api::ErrorDetails>>,
}

impl From<loonfs_client::backend::BackendError> for CliError {
    /// Backend failures pass through verbatim: the seam already carries the
    /// registry or backend-local code, message, and any server diagnostics
    /// this CLI reports.
    fn from(error: loonfs_client::backend::BackendError) -> Self {
        Self {
            code: error.code,
            message: error.message,
            request_id: error.request_id,
            details: error.details,
        }
    }
}

impl CliError {
    /// An `io_error` that names the file it concerns, so a failure inside a
    /// loop over many files stays attributable.
    pub(crate) fn io_for_path(path: &std::path::Path, error: std::io::Error) -> Self {
        Self::new(
            "io_error",
            format!("i/o error for `{}`: {error}", path.display()),
        )
    }

    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            request_id: None,
            details: None,
        }
    }

    pub(crate) fn invalid_config(message: impl Into<String>) -> Self {
        Self::new("invalid_config", message)
    }

    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self::new("invalid_input", message)
    }

    pub(crate) fn profile_not_found(name: &str) -> Self {
        Self::new("profile_not_found", format!("profile `{name}` not found"))
    }

    pub(crate) fn no_default_profile() -> Self {
        Self::new(
            "no_default_profile",
            "no default profile is set; use `profile use` or `--profile`",
        )
    }

    pub(crate) fn no_default_namespace(profile: &str) -> Self {
        Self::new(
            "no_default_namespace",
            format!(
                "no default namespace is set for profile `{profile}`; use `loon use <namespace>` or `--namespace`"
            ),
        )
    }

    pub(crate) fn profile_already_exists(name: &str) -> Self {
        Self::new(
            "profile_already_exists",
            format!("profile `{name}` already exists"),
        )
    }

    pub(crate) fn config_already_exists(path: &str) -> Self {
        Self::new(
            "config_already_exists",
            format!(
                "config file already exists at `{path}`. use `loon profile create` to create a new profile, `loon profile update` to modify an existing profile, or `loon profile use` to change the default profile"
            ),
        )
    }

    /// Interactive input is required but unavailable; `requirement` says
    /// what to pass instead.
    pub(crate) fn non_interactive_input_required(requirement: impl Into<String>) -> Self {
        Self::new("non_interactive_input_required", requirement)
    }

    pub(crate) fn non_interactive_field_required(field: &str) -> Self {
        Self::non_interactive_input_required(format!(
            "missing required `{field}` while `--no-input` is active"
        ))
    }

    pub(crate) fn destination_exists(path: &std::path::Path) -> Self {
        Self::new(
            "destination_exists",
            format!(
                "local file `{}` already exists; pass --force to overwrite",
                path.display()
            ),
        )
    }

    pub(crate) fn json_not_supported_for_streaming() -> Self {
        Self::new(
            "json_not_supported_for_streaming",
            "streaming commands do not support `--json`",
        )
    }

    pub(crate) fn io(error: std::io::Error) -> Self {
        Self::new("io_error", format!("i/o error: {error}"))
    }

    pub(crate) fn cancelled() -> Self {
        Self::new("cancelled", "operation cancelled")
    }
}
