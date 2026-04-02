use crate::digest::sha256_hex;
use loon_types::{ChangeSeq, HeadState, LeaseState, NamespaceId};
use serde::{Deserialize, Serialize};

pub const CONTROL_OBJECT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    NamespaceHead,
    NamespaceLease,
    NamespaceProgress,
    QueueShard,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressState {
    pub namespace_id: NamespaceId,
    pub work_class: String,
    pub through_seq: ChangeSeq,
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
pub type ProgressStateEnvelope = ControlObjectEnvelope<ProgressState>;

pub fn payload_checksum_sha256<T>(value: &T) -> Result<String, serde_json::Error>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    Ok(sha256_hex(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_types::{FenceToken, InodeId};

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

        let envelope =
            ControlObjectEnvelope::from_state(ControlObjectKind::NamespaceHead, "test-writer", state)
                .expect("build envelope");

        assert_eq!(envelope.kind, ControlObjectKind::NamespaceHead);
        assert_eq!(envelope.format_version, CONTROL_OBJECT_FORMAT_VERSION);
        assert!(envelope
            .has_valid_payload_checksum()
            .expect("recompute payload checksum"));
    }

    #[test]
    fn checksum_helper_matches_envelope_value() {
        let state = LeaseState {
            namespace_id: NamespaceId::from("ns-1"),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(8),
            lease_expires_at_ms: 1_000,
        };

        let envelope =
            ControlObjectEnvelope::from_state(ControlObjectKind::NamespaceLease, "test-writer", state)
                .expect("build envelope");

        assert_eq!(
            envelope.payload_checksum_sha256,
            payload_checksum_sha256(&envelope.state).expect("recompute checksum")
        );
    }

    #[test]
    fn progress_state_envelope_round_trips_through_json() {
        let state = ProgressState {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildListingIndex".to_owned(),
            through_seq: ChangeSeq(42),
        };
        let envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceProgress,
            "test-writer",
            state.clone(),
        )
        .expect("build progress envelope");
        let encoded = serde_json::to_vec(&envelope).expect("encode progress envelope");
        let decoded: ControlObjectEnvelope<ProgressState> =
            serde_json::from_slice(&encoded).expect("decode progress envelope");

        assert_eq!(decoded.kind, ControlObjectKind::NamespaceProgress);
        assert_eq!(decoded.format_version, CONTROL_OBJECT_FORMAT_VERSION);
        assert_eq!(decoded.state, state);
        assert!(decoded
            .has_valid_payload_checksum()
            .expect("recompute progress checksum"));
    }
}
