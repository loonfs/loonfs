//! OpenAPI document rewrites that utoipa cannot express at the handler or schema.

pub(crate) mod document;
mod error;
pub(crate) mod operations;
mod value;

pub(crate) use document::openapi_json_pretty;
pub(crate) use error::OpenapiPostprocessError;
