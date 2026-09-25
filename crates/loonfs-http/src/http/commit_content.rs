//! Validates wire content sources and prepares inline values for planning.

use super::error::ApiResponseError;
use loonfs::publish::InlineContent;
use loonfs_api::{
    ContentId, ErrorCode, FilesystemOperation, NamespaceId, FEATURE_COMMIT_INLINE_CONTENT,
};

pub(super) fn prepare_inline_content(
    namespace_id: &NamespaceId,
    operations: &mut [FilesystemOperation],
    threshold: Option<usize>,
) -> Result<Vec<InlineContent>, ApiResponseError> {
    let mut values = Vec::new();
    for (index, operation) in operations.iter_mut().enumerate() {
        let (content_ref, inline_content) = match operation {
            FilesystemOperation::PutFile {
                content_ref,
                inline_content,
                ..
            }
            | FilesystemOperation::CreateFileByInode {
                content_ref,
                inline_content,
                ..
            }
            | FilesystemOperation::PutFileRevisionByInode {
                content_ref,
                inline_content,
                ..
            } => (content_ref, inline_content),
            _ => continue,
        };
        if content_ref.is_some() == inline_content.is_some() {
            return Err(ApiResponseError::new(
                ErrorCode::InvalidRequest,
                "exactly one of `content_ref` and `inline_content` is required",
            )
            .with_param(format!("/operations/{index}")));
        }
        let Some(bytes) = inline_content.take() else {
            continue;
        };
        let threshold = threshold.ok_or_else(|| {
            ApiResponseError::not_supported(
                FEATURE_COMMIT_INLINE_CONTENT,
                "inline content is disabled",
            )
        })?;
        if bytes.len() > threshold {
            return Err(ApiResponseError::new(
                ErrorCode::InvalidRequest,
                &format!(
                    "inline content is {} bytes; maximum is {threshold}",
                    bytes.len()
                ),
            )
            .with_param(format!("/operations/{index}/inline_content")));
        }
        let value = InlineContent::new(namespace_id.clone(), ContentId::generate(), bytes.into());
        *content_ref = Some(value.content_ref().clone());
        values.push(value);
    }
    Ok(values)
}
