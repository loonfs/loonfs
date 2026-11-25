//! Prepares the inline content of a commit request for planning.

use super::error::ApiResponseError;
use loonfs::publish::InlineContent;
use loonfs_types::{ContentId, ErrorCode, FileContent, FilesystemOperation};

pub(super) fn prepare_inline_content(
    operations: &mut [FilesystemOperation],
    threshold: usize,
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
        if bytes.len() > threshold {
            return Err(ApiResponseError::new(
                ErrorCode::ContentTooLarge,
                &format!(
                    "inline content is {} bytes; maximum is {threshold}",
                    bytes.len()
                ),
            )
            .with_param(format!("/operations/{index}/inline_content")));
        }
        let value = InlineContent::new(
            ContentId::generate(),
            std::mem::take(bytes).into(),
            algorithm,
        );
        *content = FileContent::Ref(value.content_ref().clone());
        values.push(value);
    }
    Ok(values)
}
