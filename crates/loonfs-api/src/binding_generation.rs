//! Binding generations: which generation of a parent/name binding a read
//! observed, and the opaque token that reports it.

use crate::{ChangeSeq, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Format version written into every encoded binding generation.
pub const BINDING_GENERATION_FORMAT_VERSION: u8 = 1;

/// Frozen discriminator written into every encoded binding generation, so a
/// token minted elsewhere is rejected rather than misread.
const BINDING_GENERATION_KIND: &str = "binding_generation";

/// One generation of a directory binding: the commit sequence that bound an
/// inode under a parent and name, and the delta position that disambiguates
/// the binding within that commit.
///
/// Creating, moving, and undeleting an entry each mint a new generation;
/// content and attribute writes never do. Clients see a generation only as
/// the opaque token [`encode`](Self::encode) mints, because the pair inside
/// it is a storage-layout detail no client may order or do arithmetic on. A
/// client compares a token it holds with the token a later read reports: an
/// unequal token means the name now names a different binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingGeneration {
    /// Commit sequence that created the binding.
    pub bind_seq: ChangeSeq,
    /// Delta position that disambiguates the binding within `bind_seq`.
    pub bind_delta_index: u32,
}

impl BindingGeneration {
    /// Encodes the generation as the opaque token clients round-trip, bound
    /// to the namespace whose binding it names.
    pub fn encode(&self, namespace_id: &NamespaceId) -> Result<String, BindingGenerationError> {
        let bytes = serde_json::to_vec(&BindingGenerationEnvelope {
            format_version: BINDING_GENERATION_FORMAT_VERSION,
            kind: BINDING_GENERATION_KIND.to_owned(),
            namespace_id,
            generation: *self,
        })
        .map_err(|error| BindingGenerationError::InvalidJson(error.to_string()))?;
        Ok(crate::hex::hex_encode_bytes(&bytes))
    }

    /// Decodes a token issued by [`encode`](Self::encode) for
    /// `expected_namespace_id`.
    pub fn decode(
        token: &str,
        expected_namespace_id: &NamespaceId,
    ) -> Result<Self, BindingGenerationError> {
        let bytes = crate::hex::hex_decode_bytes(token)
            .map_err(|_| BindingGenerationError::InvalidEncoding)?;
        let header: BindingGenerationHeader = serde_json::from_slice(&bytes)
            .map_err(|error| BindingGenerationError::InvalidJson(error.to_string()))?;
        if header.format_version != BINDING_GENERATION_FORMAT_VERSION {
            return Err(BindingGenerationError::UnsupportedVersion {
                expected: BINDING_GENERATION_FORMAT_VERSION,
                actual: header.format_version,
            });
        }
        if header.kind != BINDING_GENERATION_KIND {
            return Err(BindingGenerationError::WrongKind {
                actual: header.kind,
            });
        }
        let envelope: BindingGenerationEnvelope<NamespaceId> = serde_json::from_slice(&bytes)
            .map_err(|error| BindingGenerationError::InvalidJson(error.to_string()))?;
        if &envelope.namespace_id != expected_namespace_id {
            return Err(BindingGenerationError::ForeignNamespace);
        }
        Ok(envelope.generation)
    }
}

#[derive(Serialize, Deserialize)]
struct BindingGenerationEnvelope<N> {
    format_version: u8,
    kind: String,
    namespace_id: N,
    #[serde(flatten)]
    generation: BindingGeneration,
}

/// Version and kind, read before the body so a token minted by another
/// codec reports `WrongKind` rather than a missing-field decode error.
#[derive(Deserialize)]
struct BindingGenerationHeader {
    format_version: u8,
    kind: String,
}

/// Why an opaque binding-generation token cannot be read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum BindingGenerationError {
    /// The token was not hex-encoded JSON.
    #[error("invalid binding generation encoding")]
    InvalidEncoding,
    /// The token JSON did not match the binding-generation shape.
    #[error("invalid binding generation JSON: {0}")]
    InvalidJson(String),
    /// The token was readable, but it is not a binding generation.
    #[error("token kind `{actual}` is not a binding generation")]
    WrongKind {
        /// Kind recovered from the caller's opaque token.
        actual: String,
    },
    /// A token minted for a different namespace than the one replaying it.
    #[error("binding generation belongs to a different namespace")]
    ForeignNamespace,
    /// The token format version is not supported by this build.
    #[error("unsupported binding generation version `{actual}`; expected `{expected}`")]
    UnsupportedVersion {
        /// Format version this build can decode.
        expected: u8,
        /// Version embedded in the caller's opaque token.
        actual: u8,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode_cursor, DirectoryPageCursor, InodeId, NameKey};

    fn namespace_id(value: &str) -> NamespaceId {
        NamespaceId::parse(value).expect("namespace id")
    }

    #[test]
    fn binding_generation_round_trips() {
        let namespace_id = namespace_id("demo");
        let generation = BindingGeneration {
            bind_seq: ChangeSeq(11),
            bind_delta_index: 3,
        };

        let token = generation.encode(&namespace_id).expect("encode generation");

        assert_eq!(
            BindingGeneration::decode(&token, &namespace_id).expect("decode generation"),
            generation
        );
    }

    #[test]
    fn a_generation_from_another_namespace_is_rejected() {
        let token = BindingGeneration {
            bind_seq: ChangeSeq(11),
            bind_delta_index: 3,
        }
        .encode(&namespace_id("demo"))
        .expect("encode generation");

        assert_eq!(
            BindingGeneration::decode(&token, &namespace_id("other")),
            Err(BindingGenerationError::ForeignNamespace)
        );
    }

    #[test]
    fn a_malformed_generation_is_rejected() {
        assert_eq!(
            BindingGeneration::decode("not-hex", &namespace_id("demo")),
            Err(BindingGenerationError::InvalidEncoding)
        );
    }

    #[test]
    fn a_token_from_another_codec_is_rejected() {
        let cursor = encode_cursor(&DirectoryPageCursor {
            head_seq: ChangeSeq(11),
            directory_inode_id: InodeId(7),
            last_name_key: NameKey::parse("plan.md").expect("name key"),
        })
        .expect("encode cursor");

        assert_eq!(
            BindingGeneration::decode(&cursor, &namespace_id("demo")),
            Err(BindingGenerationError::WrongKind {
                actual: "directory".to_owned(),
            })
        );
    }
}
