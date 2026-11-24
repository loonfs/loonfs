//! Signed part targets for a direct multipart upload, and the part checksum
//! rule that completion checks again.

use super::session::{ensure_session_actor, ensure_session_open};
use crate::engine::NamespaceEngine;
use crate::engine::RuntimeReadContext;
use crate::error::{CoreError, Result};
use crate::limits::{MAX_MULTIPART_PARTS, MAX_SIGNED_PARTS_PER_REQUEST};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{UploadSessionMode, UploadSessionPayload};
use loonfs_types::{ActorId, Checksum, ChecksumAlgorithm, UploadId, UploadPartChecksumClaim};

/// One part a server integration is about to sign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartTarget {
    pub part_number: u32,
    pub checksum: Checksum,
}

/// Everything a server integration needs to sign one wave of part uploads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartTargets {
    pub object_key: String,
    pub provider_upload_id: String,
    pub parts: Vec<MultipartPartTarget>,
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Validates a group of multipart parts and returns the information needed
    /// to sign their upload URLs.
    ///
    /// Part state remains client-managed. This method writes no durable state.
    pub async fn direct_multipart_part_targets(
        &self,
        context: &RuntimeReadContext,
        upload_id: &UploadId,
        requested: &[UploadPartChecksumClaim],
        actor_id: &ActorId,
    ) -> Result<MultipartPartTargets> {
        let now_ms = self.now_ms();
        let head = context.read_basis().head();
        if requested.is_empty() {
            return Err(CoreError::InvalidUploadContent(
                "a part-signing request names at least one part".to_owned(),
            ));
        }
        if requested.len() > MAX_SIGNED_PARTS_PER_REQUEST {
            return Err(CoreError::InvalidUploadContent(format!(
                "a part-signing request names at most {MAX_SIGNED_PARTS_PER_REQUEST} parts"
            )));
        }
        let state = self
            .namespace
            .load_upload_session_state(head, upload_id)
            .await?;
        ensure_session_actor(&state.session, actor_id)?;
        ensure_session_open(&state, now_ms)?;
        let session = &state.session;
        let (provider_upload_id, checksum_algorithm) = multipart_session_upload(session)?;

        let mut parts = Vec::with_capacity(requested.len());
        for claim in requested {
            // The only bound is the provider's own part-number range: the
            // session never learned how long the payload would be, so there is
            // no part count to check against.
            if claim.part_number == 0 || claim.part_number > MAX_MULTIPART_PARTS {
                return Err(CoreError::InvalidUploadContent(format!(
                    "part {} is outside the provider's 1..={MAX_MULTIPART_PARTS} part range",
                    claim.part_number
                )));
            }
            parts.push(MultipartPartTarget {
                part_number: claim.part_number,
                checksum: validate_upload_checksum(&claim.checksum, checksum_algorithm)?.clone(),
            });
        }

        Ok(MultipartPartTargets {
            object_key: content_blob(&head.pool_id, &session.content_id),
            provider_upload_id: provider_upload_id.to_owned(),
            parts,
        })
    }
}

/// Returns the provider upload ID for a direct multipart session.
///
/// Other upload modes return an invalid-upload error.
pub(crate) fn multipart_session_upload(
    session: &UploadSessionPayload,
) -> Result<(&str, ChecksumAlgorithm)> {
    match &session.mode {
        UploadSessionMode::DirectMultipart {
            provider_upload_id,
            checksum_algorithm,
            ..
        } => Ok((provider_upload_id, *checksum_algorithm)),
        UploadSessionMode::ServiceProxied {} | UploadSessionMode::DirectPut { .. } => {
            Err(CoreError::InvalidUploadContent(
                "this upload session is not a direct_multipart upload".to_owned(),
            ))
        }
    }
}

pub(crate) fn validate_upload_checksum(
    checksum: &Checksum,
    required_algorithm: ChecksumAlgorithm,
) -> Result<&Checksum> {
    if checksum.algorithm() != required_algorithm {
        return Err(CoreError::InvalidUploadContent(format!(
            "checksum algorithm `{}` does not match the session requirement `{required_algorithm}`",
            checksum.algorithm()
        )));
    }
    Ok(checksum)
}
