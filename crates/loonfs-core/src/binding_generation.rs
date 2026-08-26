//! Encodes opaque identifiers for parent/name bindings.

use loonfs_api::{ChangeSeq, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const FORMAT_VERSION: u8 = 1;
const KIND: &str = "binding_generation";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingGeneration {
    pub(crate) bind_seq: ChangeSeq,
    pub(crate) bind_delta_index: u32,
}

impl BindingGeneration {
    pub(crate) fn encode(&self, namespace_id: &NamespaceId) -> Result<String, serde_json::Error> {
        let bytes = serde_json::to_vec(&BindingGenerationEnvelope {
            format_version: FORMAT_VERSION,
            kind: KIND,
            namespace_id,
            generation: *self,
        })?;
        Ok(loonfs_api::wire::hex::hex_encode_bytes(&bytes))
    }

    pub(crate) fn decode(
        value: &str,
        expected_namespace_id: &NamespaceId,
    ) -> Result<Self, InvalidBindingGeneration> {
        let bytes =
            loonfs_api::wire::hex::hex_decode_bytes(value).map_err(|_| InvalidBindingGeneration)?;
        let envelope: DecodedBindingGenerationEnvelope =
            serde_json::from_slice(&bytes).map_err(|_| InvalidBindingGeneration)?;
        if envelope.format_version != FORMAT_VERSION
            || envelope.kind != KIND
            || envelope.namespace_id != *expected_namespace_id
        {
            return Err(InvalidBindingGeneration);
        }
        Ok(envelope.generation)
    }
}

#[derive(Serialize)]
struct BindingGenerationEnvelope<'a> {
    format_version: u8,
    kind: &'static str,
    namespace_id: &'a NamespaceId,
    #[serde(flatten)]
    generation: BindingGeneration,
}

#[derive(Deserialize)]
struct DecodedBindingGenerationEnvelope {
    format_version: u8,
    kind: String,
    namespace_id: NamespaceId,
    #[serde(flatten)]
    generation: BindingGeneration,
}

#[derive(Debug, Error)]
#[error("binding generation is malformed or belongs to another namespace")]
pub(crate) struct InvalidBindingGeneration;
