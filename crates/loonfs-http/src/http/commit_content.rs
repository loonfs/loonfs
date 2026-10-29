//! Prepares the inline content of a commit request for planning.

use super::error::ApiResponseError;
use loonfs::publish::InlineContent;
use loonfs_types::{
    ContentId, ErrorCode, FileContent, FilesystemOperation, NamespaceId,
    FEATURE_COMMIT_INLINE_CONTENT,
};

pub(super) fn prepare_inline_content(
    namespace_id: &NamespaceId,
    operations: &mut [FilesystemOperation],
    threshold: Option<usize>,
    algorithm: loonfs_types::ChecksumAlgorithm,
) -> Result<Vec<InlineContent>, ApiResponseError> {
    let mut values = Vec::new();
    for (index, operation) in operations.iter_mut().enumerate() {
        let content = match operation {
            FilesystemOperation::PutFile { content, .. }
            | FilesystemOperation::CreateFileByInode { content, .. }
            | FilesystemOperation::PutFileRevisionByInode { content, .. } => content,
            _ => continue,
        };
        let FileContent::Inline(bytes) = content else {
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
        let value = InlineContent::new(
            namespace_id.clone(),
            ContentId::generate(),
            std::mem::take(bytes).into(),
            algorithm,
        );
        *content = FileContent::Ref(value.content_ref().clone());
        values.push(value);
    }
    Ok(values)
}
