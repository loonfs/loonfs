//! Environment-variable names shared across LoonFS process surfaces.

/// Supplies the bearer token used to authenticate remote API requests.
pub const AUTH_TOKEN_ENV: &str = "LOONFS_AUTH_TOKEN";
/// Supplies the secret used to sign content-transfer tokens.
pub const CONTENT_TOKEN_SECRET_ENV: &str = "LOONFS_CONTENT_TOKEN_SECRET";
