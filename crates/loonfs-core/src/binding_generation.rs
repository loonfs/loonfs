//! Encodes opaque API tokens for binding positions.

use loonfs_api::wire::manifest::DeltaPosition;
use loonfs_api::{
    decode_token, encode_token, BindingGeneration, ChangeSeq, NamespaceId, OpaqueToken,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const BINDING_GENERATION_FORMAT_VERSION: u8 = 1;

pub(crate) fn encode(position: DeltaPosition, namespace_id: &NamespaceId) -> BindingGeneration {
    let encoded = encode_token(
        &BindingGenerationEnvelope {
            namespace_id: namespace_id.clone(),
            bind_seq: position.seq,
            bind_delta_index: position.delta_index,
        },
        BINDING_GENERATION_FORMAT_VERSION,
    )
    .expect("binding position should contain only serializable fields");
    BindingGeneration::parse(encoded).expect("opaque token encoder should emit lowercase hex")
}

pub(crate) fn decode(
    value: &BindingGeneration,
    expected_namespace_id: &NamespaceId,
) -> Result<DeltaPosition, InvalidBindingGeneration> {
    let envelope: BindingGenerationEnvelope =
        decode_token(value.as_str(), BINDING_GENERATION_FORMAT_VERSION)
            .map_err(|_| InvalidBindingGeneration)?;
    if envelope.namespace_id != *expected_namespace_id {
        return Err(InvalidBindingGeneration);
    }
    Ok(DeltaPosition {
        seq: envelope.bind_seq,
        delta_index: envelope.bind_delta_index,
    })
}

#[derive(Serialize, Deserialize)]
struct BindingGenerationEnvelope {
    namespace_id: NamespaceId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
}

impl OpaqueToken for BindingGenerationEnvelope {
    const KIND: &'static str = "binding_generation";
}

#[derive(Debug, Error)]
#[error("binding generation is malformed or belongs to another namespace")]
pub(crate) struct InvalidBindingGeneration;
