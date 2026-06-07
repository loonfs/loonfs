use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

const SERVER_GENERATED_ID_BODY_LEN: usize = 32;

/// Durable id for one namespace.
///
/// A namespace is one filesystem history. This id is not a display name and
/// should not be reused after destruction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(String);

/// Durable id for an immutable content store.
///
/// Content stores own file bytes. Namespaces point at content stores.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentStoreId(String);

/// Client-supplied idempotency key for one logical commit.
///
/// Reuse the same `CommitId` when retrying the same request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommitId(String);

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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid name_key {value:?}: {reason}")]
pub struct NameKeyValidationError {
    value: String,
    reason: &'static str,
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

impl NameKeyValidationError {
    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

impl NamespaceId {
    /// Parses and validates a namespace id.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, NamespaceIdValidationError> {
        let value = value.as_ref();
        validate_namespace_id(value)?;
        Ok(Self(value.to_owned()))
    }

    /// Builds a namespace id from an owned string after validation.
    pub fn try_new(value: impl Into<String>) -> Result<Self, NamespaceIdValidationError> {
        let value = value.into();
        validate_namespace_id(&value)?;
        Ok(Self(value))
    }

    /// Returns the serialized namespace id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl CommitId {
    /// Parses and validates a commit id.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, CommitIdValidationError> {
        let value = value.as_ref();
        validate_commit_id(value)?;
        Ok(Self(value.to_owned()))
    }

    /// Builds a commit id from an owned string after validation.
    pub fn try_new(value: impl Into<String>) -> Result<Self, CommitIdValidationError> {
        let value = value.into();
        validate_commit_id(&value)?;
        Ok(Self(value))
    }

    /// Returns the serialized commit id.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Generates a valid random commit id.
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

pub fn generate_metadata_table_id() -> String {
    generated_id("tbl")
}

pub fn generate_gc_pin_id() -> String {
    generated_id("pin")
}

pub fn generate_checkpoint_id() -> String {
    generated_id("chk")
}

pub fn validate_upload_id(value: impl AsRef<str>) -> Result<(), GeneratedIdValidationError> {
    validate_generated_id("upl", value.as_ref())
}

pub fn validate_wal_segment_id(value: impl AsRef<str>) -> Result<(), GeneratedIdValidationError> {
    validate_generated_id("seg", value.as_ref())
}

pub fn validate_metadata_table_id(
    value: impl AsRef<str>,
) -> Result<(), GeneratedIdValidationError> {
    validate_generated_id("tbl", value.as_ref())
}

pub fn validate_gc_pin_id(value: impl AsRef<str>) -> Result<(), GeneratedIdValidationError> {
    validate_generated_id("pin", value.as_ref())
}

pub fn validate_checkpoint_id(value: impl AsRef<str>) -> Result<(), GeneratedIdValidationError> {
    validate_generated_id("chk", value.as_ref())
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

fn validate_name_key(value: &str) -> Result<(), NameKeyValidationError> {
    if value.is_empty() {
        return Err(name_key_error(value, "must not be empty"));
    }
    if value.contains('/') {
        return Err(name_key_error(value, "must not contain `/`"));
    }
    if matches!(value, "." | "..") {
        return Err(name_key_error(value, "must not be `.` or `..`"));
    }
    Ok(())
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

fn name_key_error(value: &str, reason: &'static str) -> NameKeyValidationError {
    NameKeyValidationError {
        value: value.to_owned(),
        reason,
    }
}

impl ContentStoreId {
    /// Generates a valid random content-store id.
    pub fn generate() -> Self {
        Self(generated_id("cs"))
    }

    /// Parses and validates a content-store id.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, GeneratedIdValidationError> {
        let value = value.as_ref();
        validate_generated_id("cs", value)?;
        Ok(Self(value.to_owned()))
    }

    /// Builds a content-store id from an owned string after validation.
    pub fn try_new(value: impl Into<String>) -> Result<Self, GeneratedIdValidationError> {
        let value = value.into();
        validate_generated_id("cs", &value)?;
        Ok(Self(value))
    }

    /// Returns the serialized content-store id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl NameKey {
    /// Parses and validates a stored name key.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, NameKeyValidationError> {
        let value = value.as_ref();
        validate_name_key(value)?;
        Ok(Self(value.to_owned()))
    }

    /// Builds a name key from an owned string after validation.
    pub fn try_new(value: impl Into<String>) -> Result<Self, NameKeyValidationError> {
        let value = value.into();
        validate_name_key(&value)?;
        Ok(Self(value))
    }

    /// Computes the lookup key for a display name under a namespace name policy.
    pub fn for_display_name(policy: crate::NamePolicy, display_name: &crate::DisplayName) -> Self {
        Self(crate::name_key_for_display_name(
            policy,
            display_name.as_str(),
        ))
    }

    /// Returns the stored lookup key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for NamespaceId {
    type Error = NamespaceIdValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for NamespaceId {
    type Error = NamespaceIdValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl TryFrom<&str> for ContentStoreId {
    type Error = GeneratedIdValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for ContentStoreId {
    type Error = GeneratedIdValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl TryFrom<&str> for CommitId {
    type Error = CommitIdValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for CommitId {
    type Error = CommitIdValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl AsRef<str> for NamespaceId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for ContentStoreId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for CommitId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for NameKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for NamespaceId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for ContentStoreId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for CommitId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for NameKey {
    fn borrow(&self) -> &str {
        self.as_str()
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

impl Serialize for NameKey {
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
        let value = String::deserialize(deserializer)?;
        NamespaceId::try_new(value).map_err(serde::de::Error::custom)
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

impl<'de> Deserialize<'de> for NameKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        NameKey::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Numeric identity of a file or directory within a namespace.
///
/// Inodes are stable across renames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InodeId(pub u64);

/// Monotonically increasing file revision counter within one file inode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RevisionNo(pub u64);

/// Monotonically increasing namespace commit sequence number.
///
/// This is the global visibility order for a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChangeSeq(pub u64);

/// Fencing token for write-lease concurrency control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FenceToken(pub u64);

/// Name-policy-derived directory entry key.
///
/// Use this for exact name preconditions. Keep user-facing spelling in
/// `DisplayName`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NameKey(String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Filesystem item kind.
pub enum InodeKind {
    /// File with revision history.
    File,
    /// Directory with child bindings.
    Dir,
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

impl fmt::Display for NameKey {
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
        generate_checkpoint_id, generate_gc_pin_id, generate_metadata_table_id, generate_upload_id,
        generate_wal_segment_id, validate_checkpoint_id, validate_gc_pin_id,
        validate_metadata_table_id, validate_upload_id, validate_wal_segment_id, CommitId,
        ContentStoreId, NameKey, NamespaceId,
    };
    use std::collections::BTreeSet;

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
    fn commit_id_parse_uses_same_allowed_grammar() {
        let parsed = CommitId::parse("c_demo-1").expect("valid commit_id");

        assert_eq!(parsed.as_str(), "c_demo-1");
        assert!(CommitId::parse("c/demo").is_err());
        assert!(CommitId::parse("C_demo").is_err());
    }

    #[test]
    fn identity_try_from_validates_values() {
        assert_eq!(
            NamespaceId::try_from("demo")
                .expect("valid namespace id")
                .as_str(),
            "demo"
        );
        assert_eq!(
            CommitId::try_from("commit-1")
                .expect("valid commit id")
                .as_str(),
            "commit-1"
        );
        assert_eq!(
            ContentStoreId::try_from("cs_00000000000000000000000000000001")
                .expect("valid content store id")
                .as_str(),
            "cs_00000000000000000000000000000001"
        );

        assert!(NamespaceId::try_from("invalid/name").is_err());
        assert!(CommitId::try_from("invalid/name").is_err());
        assert!(ContentStoreId::try_from("cs_0000000000000000000000000000000g").is_err());
    }

    #[test]
    fn identity_deserialize_validates_values() {
        let namespace_id: NamespaceId =
            serde_json::from_str(r#""demo""#).expect("valid namespace id json");
        assert_eq!(namespace_id.as_str(), "demo");
        let commit_id: CommitId =
            serde_json::from_str(r#""commit-1""#).expect("valid commit id json");
        assert_eq!(commit_id.as_str(), "commit-1");
        let content_store_id: ContentStoreId =
            serde_json::from_str(r#""cs_00000000000000000000000000000001""#)
                .expect("valid content store id json");
        assert_eq!(
            content_store_id.as_str(),
            "cs_00000000000000000000000000000001"
        );

        let namespace_error = serde_json::from_str::<NamespaceId>(r#""invalid/name""#)
            .expect_err("invalid namespace id json");
        assert!(namespace_error.to_string().contains("namespace_id"));
        let commit_error = serde_json::from_str::<CommitId>(r#""invalid/name""#)
            .expect_err("invalid commit id json");
        assert!(commit_error.to_string().contains("commit_id"));
        let content_store_error =
            serde_json::from_str::<ContentStoreId>(r#""cs_0000000000000000000000000000000g""#)
                .expect_err("invalid content store id json");
        assert!(content_store_error.to_string().contains("generated id"));
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
    fn generated_upload_wal_segment_table_and_pin_validators_reject_hyphenated_ids() {
        assert!(validate_upload_id("upl_00000000000000000000000000000001").is_ok());
        assert!(validate_wal_segment_id("seg_00000000000000000000000000000001").is_ok());
        assert!(validate_metadata_table_id("tbl_00000000000000000000000000000001").is_ok());
        assert!(validate_gc_pin_id("pin_00000000000000000000000000000001").is_ok());
        assert!(validate_checkpoint_id("chk_00000000000000000000000000000001").is_ok());
        assert!(validate_upload_id(["upl", "123"].join("-")).is_err());
        assert!(validate_wal_segment_id(["seg", "123"].join("-")).is_err());
        assert!(validate_metadata_table_id(["tbl", "123"].join("-")).is_err());
        assert!(validate_gc_pin_id(["pin", "123"].join("-")).is_err());
        assert!(validate_checkpoint_id(["chk", "123"].join("-")).is_err());
    }

    #[test]
    fn generated_runtime_ids_use_lower_hex_uuid_bodies() {
        let upload_id = generate_upload_id();
        let wal_segment_id = generate_wal_segment_id();
        let metadata_table_id = generate_metadata_table_id();
        let gc_pin_id = generate_gc_pin_id();
        let checkpoint_id = generate_checkpoint_id();

        assert_generated_id_shape(&upload_id, "upl");
        assert_generated_id_shape(&wal_segment_id, "seg");
        assert_generated_id_shape(&metadata_table_id, "tbl");
        assert_generated_id_shape(&gc_pin_id, "pin");
        assert_generated_id_shape(&checkpoint_id, "chk");
        assert!(validate_upload_id(&upload_id).is_ok());
        assert!(validate_wal_segment_id(&wal_segment_id).is_ok());
        assert!(validate_metadata_table_id(&metadata_table_id).is_ok());
        assert!(validate_gc_pin_id(&gc_pin_id).is_ok());
        assert!(validate_checkpoint_id(&checkpoint_id).is_ok());
    }

    #[test]
    fn generated_wal_segment_ids_are_not_reused_across_samples() {
        let mut ids = BTreeSet::new();
        for _ in 0..128 {
            let id = generate_wal_segment_id();
            assert!(
                ids.insert(id.clone()),
                "generated duplicate WAL segment id {id}"
            );
        }
    }

    fn assert_generated_id_shape(value: &str, prefix: &str) {
        let expected_prefix = format!("{prefix}_");
        let body = value
            .strip_prefix(&expected_prefix)
            .expect("generated id prefix");
        assert_eq!(body.len(), 32);
        assert!(
            body.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "generated id body must be lowercase hex: {value}"
        );
    }

    #[test]
    fn name_key_parse_rejects_invalid_values() {
        assert_eq!(
            NameKey::parse("").expect_err("empty").reason(),
            "must not be empty"
        );
        assert_eq!(
            NameKey::parse("a/b").expect_err("slash").reason(),
            "must not contain `/`"
        );
        assert_eq!(
            NameKey::parse(".").expect_err("dot").reason(),
            "must not be `.` or `..`"
        );
    }

    #[test]
    fn name_key_serializes_as_string_and_validates_deserialize() {
        let name_key = NameKey::parse("report.txt").expect("valid name key");

        assert_eq!(
            serde_json::to_string(&name_key).expect("serialize name key"),
            "\"report.txt\""
        );
        assert_eq!(
            serde_json::from_str::<NameKey>("\"report.txt\"").expect("deserialize name key"),
            name_key
        );
        assert!(serde_json::from_str::<NameKey>("\"a/b\"").is_err());
    }
}
