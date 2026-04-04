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

/// Numeric identity of a file, directory, symlink, or mount within a namespace.
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

/// Case-normalized directory entry name used as a lookup key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NameKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InodeKind {
    File,
    Dir,
    Symlink,
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

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "inode-{}", self.0)
    }
}
