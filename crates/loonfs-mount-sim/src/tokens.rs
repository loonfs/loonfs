//! The simulation's opaque tokens: binding versions, and the content tokens that completed
//! uploads issue.

use loonfs_model::metadata::DeltaPosition;
use loonfs_types::api::v0::ContentToken;
use loonfs_types::{
    decode_token, encode_token, BindingVersion, ChangeSeq, ContentRef, NamespaceId, OpaqueToken,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const BINDING_VERSION_FORMAT_VERSION: u8 = 1;

/// What a binding version encodes: the namespace that issued it, and the position of the bind
/// delta that created the binding.
#[derive(Serialize, Deserialize)]
struct BindingVersionToken {
    namespace_id: NamespaceId,
    seq: ChangeSeq,
    delta_index: u32,
}

impl OpaqueToken for BindingVersionToken {
    const KIND: &'static str = "sim_binding_version";
}

/// Encodes the binding version of the binding that the bind delta at `position` created.
pub(crate) fn encode_binding_version(
    namespace_id: &NamespaceId,
    position: DeltaPosition,
) -> BindingVersion {
    let token = encode_token(
        &BindingVersionToken {
            namespace_id: namespace_id.clone(),
            seq: position.seq,
            delta_index: position.delta_index,
        },
        BINDING_VERSION_FORMAT_VERSION,
    )
    .expect("a binding position should encode as JSON");
    BindingVersion::parse(token).expect("an opaque token should be base64url")
}

/// Decodes a binding version, or returns `None` for a malformed token or one that another
/// namespace issued.
pub(crate) fn decode_binding_version(
    namespace_id: &NamespaceId,
    version: &BindingVersion,
) -> Option<DeltaPosition> {
    let token: BindingVersionToken =
        decode_token(version.as_str(), BINDING_VERSION_FORMAT_VERSION).ok()?;
    (token.namespace_id == *namespace_id).then_some(DeltaPosition {
        seq: token.seq,
        delta_index: token.delta_index,
    })
}

/// The content tokens that the server has issued.
#[derive(Debug, Default)]
pub(crate) struct ContentTokens {
    issued: BTreeMap<String, IssuedToken>,
}

#[derive(Debug)]
struct IssuedToken {
    namespace_id: NamespaceId,
    content_ref: ContentRef,
    expires_at_ms: u64,
}

impl ContentTokens {
    pub(crate) fn issue(
        &mut self,
        token: String,
        namespace_id: &NamespaceId,
        content_ref: &ContentRef,
        expires_at_ms: u64,
    ) -> ContentToken {
        self.issued.insert(
            token.clone(),
            IssuedToken {
                namespace_id: namespace_id.clone(),
                content_ref: content_ref.clone(),
                expires_at_ms,
            },
        );
        ContentToken {
            content_ref: content_ref.clone(),
            token,
        }
    }

    /// Returns whether one of `tokens` proves that `content_ref` was prepared for
    /// `namespace_id`, and hasn't expired at `now_ms`.
    pub(crate) fn proves(
        &self,
        tokens: &[ContentToken],
        namespace_id: &NamespaceId,
        content_ref: &ContentRef,
        now_ms: u64,
    ) -> bool {
        tokens
            .iter()
            .filter(|token| token.content_ref == *content_ref)
            .any(|token| {
                self.issued.get(&token.token).is_some_and(|issued| {
                    issued.namespace_id == *namespace_id
                        && issued.content_ref == *content_ref
                        && now_ms < issued.expires_at_ms
                })
            })
    }
}
