use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

const SERVER_GENERATED_ID_BODY_LEN: usize = 32;

/// Unique identifier for a namespace (a logical sync boundary).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(pub String);

/// Unique identifier for an immutable content store.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentStoreId(pub String);

/// Client-supplied stable identifier for one logical commit.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommitId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid namespace_id {value:?}: {reason}")]
pub struct NamespaceIdValidationError {
    value: String,
    reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid commit_id {value:?}: {reason}")]
pub struct CommitIdValidationError {
    value: String,
    reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid generated id {value:?}: {reason}")]
pub struct GeneratedIdValidationError {
    value: String,
    reason: String,
}

impl NamespaceIdValidationError {
    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

impl CommitIdValidationError {
    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

impl GeneratedIdValidationError {
    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl NamespaceId {
    /// Constructs a namespace id without validation. Use [`NamespaceId::parse`]
    /// for user-supplied or durable-boundary input.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, NamespaceIdValidationError> {
        let value = value.as_ref();
        validate_namespace_id(value)?;
        Ok(Self(value.to_owned()))
    }

    pub fn try_new(value: impl Into<String>) -> Result<Self, NamespaceIdValidationError> {
        let value = value.into();
        validate_namespace_id(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl CommitId {
    /// Constructs a commit id without validation. Use [`CommitId::parse`]
    /// for user-supplied or durable-boundary input.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, CommitIdValidationError> {
        let value = value.as_ref();
        validate_commit_id(value)?;
        Ok(Self(value.to_owned()))
    }

    pub fn try_new(value: impl Into<String>) -> Result<Self, CommitIdValidationError> {
        let value = value.into();
        validate_commit_id(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn generate() -> Self {
        Self(generated_id("c"))
    }
}

/// Generates a project-standard opaque durable identifier.
///
/// Generated server-side IDs use an underscore prefix plus a 32-character
/// lowercase UUID-simple body, such as `cs_<32hex>` or `seg_<32hex>`.
pub fn generated_id(prefix: &'static str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

pub fn generate_upload_id() -> String {
    generated_id("upl")
}

pub fn generate_wal_segment_id() -> String {
    generated_id("seg")
}

pub fn validate_upload_id(value: impl AsRef<str>) -> Result<(), GeneratedIdValidationError> {
    validate_generated_id("upl", value.as_ref())
}

pub fn validate_wal_segment_id(value: impl AsRef<str>) -> Result<(), GeneratedIdValidationError> {
    validate_generated_id("seg", value.as_ref())
}

pub fn validate_generated_id(
    prefix: &'static str,
    value: &str,
) -> Result<(), GeneratedIdValidationError> {
    let expected_prefix = format!("{prefix}_");
    let Some(body) = value.strip_prefix(&expected_prefix) else {
        return Err(generated_id_error(
            value,
            format!("must start with `{expected_prefix}`"),
        ));
    };
    if body.len() != SERVER_GENERATED_ID_BODY_LEN {
        return Err(generated_id_error(
            value,
            format!("body must be {SERVER_GENERATED_ID_BODY_LEN} lowercase hex characters"),
        ));
    }
    if !body
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(generated_id_error(
            value,
            "body must contain only lowercase hex characters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_namespace_id(value: &str) -> Result<(), NamespaceIdValidationError> {
    validate_id_grammar(value).map_err(|reason| namespace_id_error(value, reason))
}

fn validate_commit_id(value: &str) -> Result<(), CommitIdValidationError> {
    validate_id_grammar(value).map_err(|reason| commit_id_error(value, reason))
}

fn validate_id_grammar(value: &str) -> Result<(), &'static str> {
    if value.is_empty() {
        return Err("must not be empty");
    }
    if value.len() > 128 {
        return Err("must be 128 bytes or fewer");
    }
    if value.trim() != value {
        return Err("must not have leading or trailing whitespace");
    }
    if matches!(value, "." | "..") {
        return Err("must not be `.` or `..`");
    }

    let mut chars = value.chars();
    let first = chars
        .next()
        .expect("empty id returned before char validation");
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err("must start with a lowercase ASCII letter or digit");
    }
    if !chars.all(is_allowed_id_tail_char) {
        return Err("must contain only lowercase ASCII letters, digits, `.`, `_`, or `-`");
    }

    Ok(())
}

fn is_allowed_id_tail_char(ch: char) -> bool {
    ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
}

fn namespace_id_error(value: &str, reason: &'static str) -> NamespaceIdValidationError {
    NamespaceIdValidationError {
        value: value.to_owned(),
        reason,
    }
}

fn commit_id_error(value: &str, reason: &'static str) -> CommitIdValidationError {
    CommitIdValidationError {
        value: value.to_owned(),
        reason,
    }
}

fn generated_id_error(value: &str, reason: String) -> GeneratedIdValidationError {
    GeneratedIdValidationError {
        value: value.to_owned(),
        reason,
    }
}

impl ContentStoreId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn generate() -> Self {
        Self(generated_id("cs"))
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, GeneratedIdValidationError> {
        let value = value.as_ref();
        validate_generated_id("cs", value)?;
        Ok(Self(value.to_owned()))
    }

    pub fn try_new(value: impl Into<String>) -> Result<Self, GeneratedIdValidationError> {
        let value = value.into();
        validate_generated_id("cs", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
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

impl From<&str> for ContentStoreId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ContentStoreId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for CommitId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for CommitId {
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

impl Serialize for ContentStoreId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl Serialize for CommitId {
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

impl<'de> Deserialize<'de> for CommitId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        CommitId::try_new(value).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for ContentStoreId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        ContentStoreId::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Numeric identity of a file, directory, or mount within a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InodeId(pub u64);

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

impl fmt::Display for ContentStoreId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for CommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "inode-{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        validate_upload_id, validate_wal_segment_id, CommitId, ContentStoreId, NamespaceId,
    };

    #[test]
    fn namespace_id_parse_accepts_allowed_grammar() {
        let long_id = format!("a{}", "b".repeat(127));
        for value in ["demo", "demo-1", "demo_1", "demo.v1", &long_id] {
            let parsed = NamespaceId::parse(value).expect("valid namespace_id");
            assert_eq!(parsed.as_str(), value);
        }
    }

    #[test]
    fn namespace_id_parse_rejects_invalid_values() {
        let long_id = format!("a{}", "b".repeat(128));
        for value in [
            "", "/", "a/b", ".", "..", " demo", "demo ", "demo\n", "demo?", "demo#", "demo%",
            "Demo", &long_id,
        ] {
            assert!(
                NamespaceId::parse(value).is_err(),
                "expected invalid namespace_id {value:?}"
            );
        }
    }

    #[test]
    fn namespace_id_unchecked_construction_remains_available() {
        let namespace_id = NamespaceId::from("invalid/name");

        assert_eq!(namespace_id.as_str(), "invalid/name");
    }

    #[test]
    fn commit_id_parse_uses_same_allowed_grammar() {
        let parsed = CommitId::parse("c_demo-1").expect("valid commit_id");

        assert_eq!(parsed.as_str(), "c_demo-1");
        assert!(CommitId::parse("c/demo").is_err());
        assert!(CommitId::parse("C_demo").is_err());
    }

    #[test]
    fn generated_content_store_id_parse_requires_prefix_and_lower_hex_body() {
        let parsed = ContentStoreId::parse("cs_00000000000000000000000000000001")
            .expect("valid content store id");

        assert_eq!(parsed.as_str(), "cs_00000000000000000000000000000001");
        let hyphenated_content_store_id = ["cs", "1"].join("-");
        for value in [
            hyphenated_content_store_id.as_str(),
            "upl_00000000000000000000000000000001",
            "content-stores/foo",
            "cs_",
            "cs_abcdef",
            "cs_0000000000000000000000000000000",
            "cs_000000000000000000000000000000001",
            "cs_ABCDEF00000000000000000000000000",
            "cs_0000000000000000000000000000000g",
            " cs_00000000000000000000000000000001",
            "cs_00000000000000000000000000000001 ",
        ] {
            assert!(
                ContentStoreId::parse(value).is_err(),
                "expected invalid content store id {value:?}"
            );
        }
    }

    #[test]
    fn generated_upload_and_wal_segment_validators_reject_hyphenated_ids() {
        assert!(validate_upload_id("upl_00000000000000000000000000000001").is_ok());
        assert!(validate_wal_segment_id("seg_00000000000000000000000000000001").is_ok());
        assert!(validate_upload_id(["upl", "123"].join("-")).is_err());
        assert!(validate_wal_segment_id(["seg", "123"].join("-")).is_err());
    }
}
