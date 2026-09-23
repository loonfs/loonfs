//! Encodes opaque identifiers for parent/name bindings.

use loonfs_api::{
    decode_token, encode_token, BindingGeneration as BindingGenerationToken, ChangeSeq,
    NamespaceId, OpaqueToken,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const BINDING_GENERATION_FORMAT_VERSION: u8 = 1;
const KIND: &str = "binding_generation";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingGeneration {
    pub(crate) bind_seq: ChangeSeq,
    pub(crate) bind_delta_index: u32,
}

impl BindingGeneration {
    pub(crate) fn encode(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<BindingGenerationToken, serde_json::Error> {
        let encoded = encode_token(
            &BindingGenerationEnvelope {
                namespace_id: namespace_id.clone(),
                generation: *self,
            },
            BINDING_GENERATION_FORMAT_VERSION,
        )?;
        Ok(BindingGenerationToken::parse(encoded)
            .expect("opaque token encoder should emit lowercase hex"))
    }

    pub(crate) fn decode(
        value: &BindingGenerationToken,
        expected_namespace_id: &NamespaceId,
    ) -> Result<Self, InvalidBindingGeneration> {
        let envelope: BindingGenerationEnvelope =
            decode_token(value.as_str(), BINDING_GENERATION_FORMAT_VERSION)
                .map_err(|_| InvalidBindingGeneration)?;
        if envelope.namespace_id != *expected_namespace_id {
            return Err(InvalidBindingGeneration);
        }
        Ok(envelope.generation)
    }
}

#[derive(Serialize, Deserialize)]
struct BindingGenerationEnvelope {
    namespace_id: NamespaceId,
    #[serde(flatten)]
    generation: BindingGeneration,
}

impl OpaqueToken for BindingGenerationEnvelope {
    const KIND: &'static str = KIND;
}

#[derive(Debug, Error)]
#[error("binding generation is malformed or belongs to another namespace")]
pub(crate) struct InvalidBindingGeneration;
