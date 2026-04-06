use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliError {
    pub code: String,
    pub message: String,
}

impl CliError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::new("invalid_config", message)
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new("invalid_input", message)
    }

    pub fn profile_not_found(name: &str) -> Self {
        Self::new("profile_not_found", format!("profile `{name}` not found"))
    }

    pub fn profile_already_exists(name: &str) -> Self {
        Self::new(
            "profile_already_exists",
            format!("profile `{name}` already exists"),
        )
    }

    pub fn json_not_supported_for_streaming() -> Self {
        Self::new(
            "json_not_supported_for_streaming",
            "streaming commands do not support `--json`",
        )
    }

    pub fn client_error(message: impl Into<String>) -> Self {
        Self::new("client_error", message)
    }

    pub fn io(error: std::io::Error) -> Self {
        Self::new("io_error", format!("i/o error: {error}"))
    }
}
