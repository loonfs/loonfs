//! Environment variables used by more than one LoonFS process.

/// Bearer token for remote API requests.
pub const AUTH_TOKEN_ENV: &str = "LOONFS_AUTH_TOKEN";
/// Secret used to sign content-transfer tokens.
pub const CONTENT_TOKEN_SECRET_ENV: &str = "LOONFS_CONTENT_TOKEN_SECRET";
