#![forbid(unsafe_code)]

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::fmt::{self, Write as _};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InodeId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RevisionNo(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChangeSeq(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FenceToken(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NameKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

pub const CONTROL_OBJECT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    NamespaceHead,
    NamespaceLease,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadState {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    pub snapshot_hint_seq: Option<ChangeSeq>,
    pub retention_floor_seq: ChangeSeq,
}

impl HeadState {
    pub fn initial(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            seq: ChangeSeq(0),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(1),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseState {
    pub namespace_id: NamespaceId,
    pub holder_id: String,
    pub fence_token: FenceToken,
    pub lease_expires_at_ms: u64,
}

impl LeaseState {
    pub fn is_valid_at(&self, now_ms: u64) -> bool {
        self.lease_expires_at_ms > now_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlObjectEnvelope<T> {
    pub kind: ControlObjectKind,
    pub format_version: u32,
    pub writer_version: String,
    pub payload_checksum_sha256: String,
    pub state: T,
}

impl<T> ControlObjectEnvelope<T>
where
    T: Serialize,
{
    pub fn from_state(
        kind: ControlObjectKind,
        writer_version: impl Into<String>,
        state: T,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            kind,
            format_version: CONTROL_OBJECT_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum_sha256: payload_checksum_sha256(&state)?,
            state,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, serde_json::Error> {
        Ok(self.payload_checksum_sha256 == payload_checksum_sha256(&self.state)?)
    }
}

pub type HeadStateEnvelope = ControlObjectEnvelope<HeadState>;
pub type LeaseStateEnvelope = ControlObjectEnvelope<LeaseState>;

pub fn payload_checksum_sha256<T>(value: &T) -> Result<String, serde_json::Error>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);

    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String should not fail");
    }

    Ok(encoded)
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

#[cfg(test)]
mod tests {
    use super::{
        payload_checksum_sha256, ChangeSeq, ControlObjectEnvelope, ControlObjectKind, FenceToken,
        HeadState, InodeId, LeaseState, NamespaceId, CONTROL_OBJECT_FORMAT_VERSION,
    };

    #[test]
    fn namespace_id_serializes_as_string() {
        let namespace_id = NamespaceId::from("ns-home");
        let json = serde_json::to_string(&namespace_id).expect("serialize namespace id");
        let round_trip: NamespaceId =
            serde_json::from_str(&json).expect("deserialize namespace id");

        assert_eq!(json, "\"ns-home\"");
        assert_eq!(round_trip, namespace_id);
    }

    #[test]
    fn control_object_envelope_computes_payload_checksum() {
        let state = HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(41),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };

        let envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "test-writer",
            state,
        )
        .expect("build envelope");

        assert_eq!(envelope.kind, ControlObjectKind::NamespaceHead);
        assert_eq!(envelope.format_version, CONTROL_OBJECT_FORMAT_VERSION);
        assert!(envelope
            .has_valid_payload_checksum()
            .expect("recompute payload checksum"));
    }

    #[test]
    fn lease_state_expiration_is_explicit() {
        let lease = LeaseState {
            namespace_id: NamespaceId::from("ns-1"),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(8),
            lease_expires_at_ms: 1_000,
        };

        assert!(lease.is_valid_at(999));
        assert!(!lease.is_valid_at(1_000));
    }

    #[test]
    fn checksum_helper_matches_envelope_value() {
        let state = LeaseState {
            namespace_id: NamespaceId::from("ns-1"),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(8),
            lease_expires_at_ms: 1_000,
        };

        let envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "test-writer",
            state,
        )
        .expect("build envelope");

        assert_eq!(
            envelope.payload_checksum_sha256,
            payload_checksum_sha256(&envelope.state).expect("recompute checksum")
        );
    }
}
