//! A string wrapper that keeps credential material out of logs and debug
//! output.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The placeholder printed in place of secret material.
const REDACTED: &str = "<redacted>";

/// A secret string such as an access key, token, or signing secret.
///
/// `Debug` and `Display` both print `<redacted>` so secrets never leak
/// through logging, tracing, or error formatting. Call [`SecretString::expose`]
/// at the sites that genuinely need the raw value (request signing, provider
/// builders, config persistence).
///
/// Serde serialization is transparent and **writes the actual secret** —
/// config files need the real value round-tripped — so never serialize a
/// secret-bearing struct into logs or display output; use a redacted copy
/// (see [`SecretString::masked`]) instead.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    /// Wraps a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns whether the secret contains only whitespace.
    pub fn is_blank(&self) -> bool {
        self.0.trim().is_empty()
    }

    /// Returns the raw secret. Keep the exposure site as small as possible.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Returns a copy whose *stored value* is the redaction placeholder.
    ///
    /// Use this to build display-safe copies of config structs that are
    /// subsequently serialized (serde serialization is transparent and would
    /// otherwise write the real secret).
    pub fn masked(&self) -> Self {
        Self(REDACTED.to_owned())
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::SecretString;

    #[test]
    fn debug_and_display_redact_the_value() {
        let secret = SecretString::new("super-secret-value");

        assert_eq!(format!("{secret:?}"), "<redacted>");
        assert_eq!(format!("{secret}"), "<redacted>");
        assert_eq!(secret.expose(), "super-secret-value");
    }

    #[test]
    fn blank_detection_ignores_surrounding_whitespace() {
        assert!(SecretString::new(" \t\n").is_blank());
        assert!(!SecretString::new(" secret ").is_blank());
    }

    #[test]
    fn masked_replaces_the_stored_value() {
        let secret = SecretString::new("super-secret-value");

        let masked = secret.masked();

        assert_eq!(masked.expose(), "<redacted>");
    }

    #[test]
    fn serde_round_trips_the_raw_value() {
        let secret = SecretString::new("super-secret-value");

        let encoded = serde_json::to_string(&secret).expect("serialize secret");
        assert_eq!(encoded, "\"super-secret-value\"");

        let decoded: SecretString = serde_json::from_str(&encoded).expect("deserialize secret");
        assert_eq!(decoded, secret);
    }
}
