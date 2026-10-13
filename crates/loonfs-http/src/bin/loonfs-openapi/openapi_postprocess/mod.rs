//! OpenAPI document rewrites that utoipa cannot express at the handler or schema.

pub(crate) mod document;
mod error;
pub(crate) mod operations;
pub(crate) mod proxy;
mod value;

pub(crate) use document::openapi_documents_pretty;
pub(crate) use error::OpenapiPostprocessError;
