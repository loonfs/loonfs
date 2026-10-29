//! HTTP contract routes and request middleware over host-supplied handles.

mod commit_content;
mod download_body;
mod error;
mod extractors;
mod handlers_downloads;
mod handlers_filesystem;
mod handlers_inodes;
mod handlers_namespace;
mod handlers_query;
mod handlers_store;
mod handlers_uploads;
pub(crate) mod metrics;
mod middleware;
#[cfg(feature = "openapi")]
mod openapi;
mod page_response;
mod query_params;
pub(crate) mod request_limit;
mod response_frames;
mod router;
#[cfg(test)]
mod tests;
pub(crate) mod usage;

pub use self::error::error_response;
pub(crate) use self::error::ApiResponseError;
pub use self::middleware::{authenticate_routes, observe_routes, panic_message};
#[cfg(feature = "openapi")]
pub use self::openapi::openapi_document;
pub use self::router::router;

use self::extractors::{
    acquire_download_permit, AppJson, AppPath, AppQuery, NamespaceIdPath, NoBody, NoQuery,
    OptionalAppJson, UploadBodyBytes, UploadBodyStream, UploadControlJson,
    MAX_COMPLETION_BODY_BYTES, MAX_JSON_BODY_BYTES, MAX_UPLOAD_CONTROL_BODY_BYTES,
};
use crate::BindingState;
