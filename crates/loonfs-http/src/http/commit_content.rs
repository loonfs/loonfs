//! Prepares the inline and imported content of a commit request for
//! planning.

use super::error::ApiResponseError;
use crate::WriterSessions;
use loonfs::publish::{InlineContent, PreparedContent};
use loonfs::{Namespace, Writable};
use loonfs_types::{
    ContentId, ErrorCode, FileContent, FilesystemOperation, FEATURE_COMMIT_INLINE_CONTENT,
};

pub(super) fn prepare_inline_content(
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
            ContentId::generate(),
            std::mem::take(bytes).into(),
            algorithm,
        );
        *content = FileContent::Ref {
            content_ref: value.content_ref().clone(),
            source_namespace_id: None,
        };
        values.push(value);
    }
    Ok(values)
}

/// Imports the content of each operation that names a source namespace
/// (`source_namespace_id`), and replaces the operation's reference with the
/// one that the commit writes.
///
/// An import from a member of the same fork family keeps the reference, and
/// an import from another fork family copies the bytes under a new content
/// ID. `namespace` acts for the request's subject, which must be an
/// administrator of each source.
pub(super) async fn prepare_imported_content(
    namespaces: &dyn WriterSessions,
    namespace: &Namespace<Writable>,
    operations: &mut [FilesystemOperation],
) -> Result<Vec<PreparedContent>, ApiResponseError> {
    let mut prepared = Vec::new();
    for operation in operations.iter_mut() {
        let content = match operation {
            FilesystemOperation::PutFile { content, .. }
            | FilesystemOperation::CreateFileByInode { content, .. }
            | FilesystemOperation::PutFileRevisionByInode { content, .. } => content,
            _ => continue,
        };
        let FileContent::Ref {
            content_ref,
            source_namespace_id: Some(source_namespace_id),
        } = content
        else {
            continue;
        };
        let imported = namespace
            .prepare_content_ref(content_ref.clone(), source_namespace_id)
            .await
            .map_err(ApiResponseError::for_namespace_writer(
                namespaces,
                namespace.id(),
            ))?;
        *content = FileContent::Ref {
            content_ref: imported.content_ref().clone(),
            source_namespace_id: None,
        };
        prepared.push(imported);
    }
    Ok(prepared)
}
