//! Encodes opaque API tokens for binding positions.

use loonfs_api::wire::manifest::DeltaPosition;
use loonfs_api::{decode_token, encode_token, BindingVersion, NamespaceId, OpaqueToken};
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
    BindingVersion::parse(encoded).expect("opaque token encoder should emit lowercase hex")
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
