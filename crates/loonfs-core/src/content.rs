//! Public facade over the durable content storage helpers.

// Reading content by reference is `NamespaceEngine::read_content_ref`: it
// resolves the namespace's content store from the pinned head and applies
// the caller's byte budget, so no consumer needs the raw store read.
pub use crate::protocol::CompletedUpload;
pub use crate::storage::content::{
    prepare_existing_content_ref, prepare_stored_content, store_bytes_as_content,
    store_bytes_as_content_with_store_id, store_stream_as_content_with_store_id,
    DurableContentValidationError, StoredContent,
};
pub use crate::storage::content_admission::{
    mint_content_token, verify_content_token, CompletedUploadReceipt, ContentTokenError,
    PreparedContent,
};
