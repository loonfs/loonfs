use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Unique identifier for a namespace (a logical sync boundary).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(pub String);

impl NamespaceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn looks_generated(value: &str) -> bool {
        let Some(rest) = value.strip_prefix("ns_") else {
            return false;
        };
        rest.len() == 32
            && rest
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    }
}

impl From<&str> for NamespaceId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for NamespaceId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl Serialize for NamespaceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NamespaceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(String::deserialize(deserializer)?))
    }
}

/// Numeric identity of a file, directory, or mount within a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InodeId(pub u64);

/// Normalized lookup key for one namespace name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NamespaceNameKey(pub String);

impl NamespaceNameKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Monotonically increasing file revision counter within an inode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RevisionNo(pub u64);

/// Monotonically increasing namespace commit sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChangeSeq(pub u64);

/// Fencing token for write-lease concurrency control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FenceToken(pub u64);

/// Name-policy-derived directory entry name used as a lookup key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NameKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InodeKind {
    File,
    Dir,
    Mount,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictDisposition {
    KeepRequestedName,
    RenameLoser { deterministic_suffix: String },
    ConflictCopy { deterministic_suffix: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for NamespaceNameKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "inode-{}", self.0)
    }
}

pub fn normalize_namespace_name(value: &str) -> Result<(String, NamespaceNameKey), String> {
    const MAX_NAMESPACE_NAME_LEN: usize = 128;

    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("namespace name must not be empty".to_owned());
    }
    if trimmed.len() > MAX_NAMESPACE_NAME_LEN {
        return Err(format!(
            "namespace name must be at most {MAX_NAMESPACE_NAME_LEN} characters"
        ));
    }
    if !trimmed
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(
            "namespace name must contain only ASCII letters, digits, `.`, `-`, or `_`".to_owned(),
        );
    }

    let name_key = NamespaceNameKey(trimmed.to_ascii_lowercase());
    if name_key.as_str().starts_with("ns_") {
        return Err("namespace name must not use the reserved `ns_` prefix".to_owned());
    }

    Ok((trimmed.to_owned(), name_key))
}

#[cfg(test)]
mod tests {
    use super::{normalize_namespace_name, NamespaceId};

    #[test]
    fn generated_namespace_ids_still_use_exact_shape_matching() {
        assert!(NamespaceId::looks_generated(
            "ns_0123456789abcdef0123456789abcdef"
        ));
        assert!(!NamespaceId::looks_generated("ns_demo"));
        assert!(!NamespaceId::looks_generated("demo"));
    }

    #[test]
    fn namespace_names_reserve_full_ns_prefix() {
        assert!(normalize_namespace_name("ns_demo").is_err());
        assert!(normalize_namespace_name("NS_demo").is_err());
        assert!(normalize_namespace_name("ns_0123456789abcdef0123456789abcdef").is_err());

        let (name, key) =
            normalize_namespace_name("Demo.01_Test-Name").expect("valid namespace name");
        assert_eq!(name, "Demo.01_Test-Name");
        assert_eq!(key.as_str(), "demo.01_test-name");
    }

    #[test]
    fn namespace_names_reject_characters_outside_ascii_letters_digits_period_dash_and_underscore() {
        assert!(normalize_namespace_name("demo space").is_err());
        assert!(normalize_namespace_name("demo/space").is_err());
        assert!(normalize_namespace_name("demo:space").is_err());
        assert!(normalize_namespace_name("d\u{00e9}mo").is_err());
    }

    #[test]
    fn namespace_names_reject_values_longer_than_128_characters() {
        let too_long = "a".repeat(129);
        assert!(normalize_namespace_name(&too_long).is_err());

        let max = "a".repeat(128);
        assert!(normalize_namespace_name(&max).is_ok());
    }
}
