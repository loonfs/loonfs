//! Encodes opaque API tokens for binding positions.

use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::{decode_token, encode_token, BindingVersion, NamespaceId, OpaqueToken};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const BINDING_VERSION_FORMAT_VERSION: u8 = 1;

pub(crate) fn encode(position: DeltaPosition, namespace_id: &NamespaceId) -> BindingVersion {
    let encoded = encode_token(
        &BindingVersionEnvelope {
            namespace_id: namespace_id.clone(),
            position,
        },
        BINDING_VERSION_FORMAT_VERSION,
    )
    .expect("binding position should contain only serializable fields");
    BindingVersion::parse(encoded).expect("opaque token encoder should emit base64url")
}

pub(crate) fn decode(
    value: &BindingVersion,
    expected_namespace_id: &NamespaceId,
) -> Result<DeltaPosition, InvalidBindingVersion> {
    let envelope: BindingVersionEnvelope =
        decode_token(value.as_str(), BINDING_VERSION_FORMAT_VERSION)
            .map_err(|_| InvalidBindingVersion)?;
    if envelope.namespace_id != *expected_namespace_id {
        return Err(InvalidBindingVersion);
    }
    Ok(envelope.position)
}

#[derive(Serialize, Deserialize)]
struct BindingVersionEnvelope {
    namespace_id: NamespaceId,
    position: DeltaPosition,
}

impl OpaqueToken for BindingVersionEnvelope {
    const KIND: &'static str = "binding_version";
}

#[derive(Debug, Error)]
#[error("binding version is malformed or belongs to another namespace")]
pub(crate) struct InvalidBindingVersion;

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_types::ChangeSeq;

    #[test]
    fn a_binding_version_matches_its_golden_bytes() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let position = DeltaPosition {
            seq: ChangeSeq(12),
            delta_index: 3,
        };
        let token = encode(position, &namespace_id);
        crate::test_support::assert_matches_golden_token("binding_version.txt", token.as_str());
        assert_eq!(
            decode(&token, &namespace_id).expect("decode binding version"),
            position
        );
    }
}
