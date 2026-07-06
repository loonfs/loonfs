//! A string wrapper that keeps credential material out of logs and debug
//! output.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The placeholder printed in place of secret material.
const REDACTED: &str = "<redacted>";

/// A secret string such as an access key, token, or signing secret.
///
/// `Debug` and `Display` redact. Serde intentionally preserves the real value
/// for config round-trips, so do not serialize secret-bearing structs into logs.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    /// Wraps a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
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
