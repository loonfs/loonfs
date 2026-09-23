//! Public facade over the durable content storage helpers.

pub use crate::protocol::CompletedUpload;
pub use crate::storage::content::DurableContentValidationError;
#[cfg(any(test, feature = "test-support"))]
pub use crate::storage::content::{
    prepare_existing_content_ref, prepare_stored_content, store_bytes_as_content, StoredContent,
};
pub use crate::storage::content_admission::{
    mint_content_token, verify_content_token, CompletedUploadReceipt, ContentTokenError,
    PreparedContent,
};
pub use crate::storage::content_location::ContentLocation;
