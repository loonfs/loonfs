//! Upload sessions through the server (`service_proxied`): begin, content, complete, read, and
//! abort.

use crate::content_store::ChainKey;
use crate::error::{MountSimError, Result};
use crate::state::ServerState;
use bytes::Bytes;
use loonfs_types::api::v0::{
    CompleteUploadBody, CreateUploadBody, UploadMode, UploadSession, UploadSessionStatus,
};
use loonfs_types::{
    ChecksumAlgorithm, ContentId, ContentRef, ErrorCode, NamespaceId, UploadId,
    FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
};
use std::collections::BTreeMap;

/// The checksum algorithm of the simulated store, which every content reference uses.
pub(crate) const STORE_CHECKSUM_ALGORITHM: ChecksumAlgorithm = ChecksumAlgorithm::Crc64nvme;

/// Every upload session the server has begun.
#[derive(Debug, Default)]
pub(crate) struct Uploads {
    sessions: BTreeMap<UploadId, UploadRecord>,
}

#[derive(Debug)]
struct UploadRecord {
    namespace_id: NamespaceId,
    content_id: ContentId,
    expires_at_ms: u64,
    status: UploadStatus,
}

#[derive(Debug)]
enum UploadStatus {
    Open {
        staged: Option<ContentRef>,
    },
    Completed {
        completed_at_ms: u64,
        content_ref: ContentRef,
    },
    Aborted {
        aborted_at_ms: u64,
    },
}

pub(crate) fn create_upload(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    body: &CreateUploadBody,
) -> Result<UploadSession> {
    state.namespace(namespace_id)?;
    let feature = match body {
        CreateUploadBody::ServiceProxied {} => None,
        CreateUploadBody::DirectPut { .. } => Some(FEATURE_UPLOADS_DIRECT_PUT),
        CreateUploadBody::DirectMultipart { .. } => Some(FEATURE_UPLOADS_DIRECT_MULTIPART),
    };
    if let Some(feature) = feature {
        return Err(MountSimError::new(
            ErrorCode::NotSupported,
            "the simulated server accepts `service_proxied` uploads only",
        )
        .with_feature(feature));
    }
    let upload_id = state.ids.upload_id();
    let record = UploadRecord {
        namespace_id: namespace_id.clone(),
        content_id: state.ids.content_id(),
        expires_at_ms: state
            .now_ms()
            .saturating_add(state.config.upload_session_lease_ms),
        status: UploadStatus::Open { staged: None },
    };
    state.uploads.sessions.insert(upload_id.clone(), record);
    Ok(session(state, &upload_id))
}

pub(crate) fn put_upload_content(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    bytes: &Bytes,
) -> Result<UploadSession> {
    let max_bytes = state.config.max_proxied_upload_bytes;
    if u64::try_from(bytes.len()).expect("a body length fits in u64") > max_bytes {
        return Err(MountSimError::new(
            ErrorCode::ContentTooLarge,
            format!("the body is larger than the proxied upload limit of {max_bytes} bytes"),
        ));
    }
    let now_ms = state.now_ms();
    state.namespace(namespace_id)?;
    let record = find_record(&mut state.uploads, namespace_id, upload_id)?;
    let content_ref = ContentRef::blob_v1(
        namespace_id.clone(),
        record.content_id.clone(),
        bytes,
        STORE_CHECKSUM_ALGORITHM,
    );
    let expired = now_ms >= record.expires_at_ms;
    let staged = match &mut record.status {
        UploadStatus::Open { staged } if !expired => staged,
        UploadStatus::Open { .. } | UploadStatus::Aborted { .. } => {
            return Err(upload_not_found(upload_id))
        }
        UploadStatus::Completed { .. } => return Err(already_completed()),
    };
    match staged {
        Some(staged) if *staged == content_ref => {}
        Some(_) => {
            return Err(MountSimError::new(
                ErrorCode::UploadContentConflict,
                "different bytes are already staged in this upload session",
            ))
        }
        None => {
            *staged = Some(content_ref.clone());
            state
                .content
                .put(ChainKey::of(&content_ref), bytes.to_vec());
        }
    }
    Ok(session(state, upload_id))
}

pub(crate) fn complete_upload(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    body: &CompleteUploadBody,
) -> Result<UploadSession> {
    let now_ms = state.now_ms();
    state.namespace(namespace_id)?;
    let record = find_record(&mut state.uploads, namespace_id, upload_id)?;
    if body.mode() != UploadMode::ServiceProxied {
        return Err(MountSimError::invalid_request(
            "the completion's `mode` doesn't match the session's `service_proxied` mode",
        )
        .with_param("/mode"));
    }
    let expired = now_ms >= record.expires_at_ms;
    // Completing a completed session returns its stored result, with a new token.
    let completed = match &record.status {
        UploadStatus::Completed { .. } => None,
        UploadStatus::Aborted { .. } => return Err(upload_not_found(upload_id)),
        UploadStatus::Open { .. } if expired => return Err(upload_not_found(upload_id)),
        UploadStatus::Open { staged: None } => {
            return Err(MountSimError::invalid_request(
                "the session has no staged content to complete",
            ))
        }
        UploadStatus::Open {
            staged: Some(content_ref),
        } => Some(content_ref.clone()),
    };
    if let Some(content_ref) = completed {
        record.status = UploadStatus::Completed {
            completed_at_ms: now_ms,
            content_ref,
        };
    }
    Ok(session(state, upload_id))
}

pub(crate) fn get_upload(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<UploadSession> {
    state.namespace(namespace_id)?;
    find_record(&mut state.uploads, namespace_id, upload_id)?;
    Ok(session(state, upload_id))
}

pub(crate) fn abort_upload(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<UploadSession> {
    let now_ms = state.now_ms();
    state.namespace(namespace_id)?;
    let record = find_record(&mut state.uploads, namespace_id, upload_id)?;
    match record.status {
        UploadStatus::Completed { .. } => {
            return Err(MountSimError::new(
                ErrorCode::UploadAlreadyCompleted,
                "a completed upload session can't be aborted",
            ))
        }
        UploadStatus::Aborted { .. } => {}
        UploadStatus::Open { .. } => {
            record.status = UploadStatus::Aborted {
                aborted_at_ms: now_ms,
            };
        }
    }
    Ok(session(state, upload_id))
}

fn find_record<'a>(
    uploads: &'a mut Uploads,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<&'a mut UploadRecord> {
    uploads
        .sessions
        .get_mut(upload_id)
        .filter(|record| record.namespace_id == *namespace_id)
        .ok_or_else(|| upload_not_found(upload_id))
}

fn already_completed() -> MountSimError {
    MountSimError::new(
        ErrorCode::UploadAlreadyCompleted,
        "the upload session is already completed",
    )
}

/// Builds the session object, with a new content token for a completed session.
fn session(state: &mut ServerState, upload_id: &UploadId) -> UploadSession {
    let record = &state.uploads.sessions[upload_id];
    let namespace_id = record.namespace_id.clone();
    let status = match &record.status {
        UploadStatus::Open { staged } => UploadSessionStatus::Open {
            expires_at_ms: record.expires_at_ms,
            checksum_algorithm: STORE_CHECKSUM_ALGORITHM,
            part_size_bytes: None,
            access: None,
            content_ref: staged.clone(),
        },
        UploadStatus::Completed {
            completed_at_ms,
            content_ref,
        } => {
            let (completed_at_ms, content_ref) = (*completed_at_ms, content_ref.clone());
            let token = state.ids.content_token();
            let expires_at_ms = state
                .now_ms()
                .saturating_add(state.config.content_token_ttl_ms);
            let content_token =
                state
                    .content_tokens
                    .issue(token, &namespace_id, &content_ref, expires_at_ms);
            UploadSessionStatus::Completed {
                completed_at_ms,
                content_ref,
                content_token: Some(content_token),
            }
        }
        UploadStatus::Aborted { aborted_at_ms } => UploadSessionStatus::Aborted {
            aborted_at_ms: *aborted_at_ms,
        },
    };
    UploadSession {
        namespace_id,
        upload_id: upload_id.clone(),
        mode: UploadMode::ServiceProxied,
        status,
    }
}

fn upload_not_found(upload_id: &UploadId) -> MountSimError {
    MountSimError::new(
        ErrorCode::UploadNotFound,
        format!("no open upload session `{upload_id}`"),
    )
}
