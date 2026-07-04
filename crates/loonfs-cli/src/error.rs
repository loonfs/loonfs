use loonfs_api::ErrorCode;
use serde::{Deserialize, Serialize};

/// Structured failure surfaced by every CLI command (`--json` renders it verbatim).
///
/// `code` draws from exactly two namespaces:
///
/// - **Registry codes** ([`loonfs_api::ErrorCode`]) pass through verbatim from
///   whichever backend produced them, so embedded and remote profiles surface
///   the same code for the same failure. Never restate a registry code as a
///   string literal; use `ErrorCode::X.as_str()` or an error's `code()`.
///   (`invalid_config` deliberately reports the registry `invalid_request`
///   code: it is the code the server serves for configuration mistakes.)
/// - **CLI-local codes** cover failures that never reach a backend. The
///   complete list, each owned by a constructor below, is: `invalid_input`,
///   `profile_not_found`, `no_default_profile`, `no_default_namespace`,
///   `profile_already_exists`, `config_already_exists`,
///   `non_interactive_input_required`, `json_not_supported_for_streaming`,
///   `client_error`, `io_error`, `runtime_error`, and `cancelled`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CliError {
    pub code: String,
    pub message: String,
}

impl CliError {
    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub(crate) fn invalid_config(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest.as_str(), message)
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

    pub(crate) fn non_interactive_input_required(field: &str) -> Self {
        Self::new(
            "non_interactive_input_required",
            format!("missing required `{field}` while `--no-input` is active"),
        )
    }

    pub(crate) fn json_not_supported_for_streaming() -> Self {
        Self::new(
            "json_not_supported_for_streaming",
            "streaming commands do not support `--json`",
        )
    }

    pub(crate) fn client_error(message: impl Into<String>) -> Self {
        Self::new("client_error", message)
    }

    pub(crate) fn io(error: std::io::Error) -> Self {
        Self::new("io_error", format!("i/o error: {error}"))
    }

    pub(crate) fn runtime_error(message: impl Into<String>) -> Self {
        Self::new("runtime_error", message)
    }

    pub(crate) fn cancelled() -> Self {
        Self::new("cancelled", "operation cancelled")
    }
}
