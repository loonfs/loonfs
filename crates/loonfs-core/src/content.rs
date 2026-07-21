//! Public facade over the durable content storage helpers.

pub use crate::storage::content::{
    prepare_existing_content_ref, prepare_stored_content, read_durable_content_bytes,
    store_bytes_as_content, store_bytes_as_content_with_store_id,
    validate_durable_content_reference, DurableContentValidationError, ReadDurableContent,
    StoredContent, ValidatedDurableContent,
};
pub use crate::storage::content_admission::{
    mint_content_token, verify_content_token, ContentTokenError, PreparedContent,
};
