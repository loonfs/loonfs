//! The LoonFS HTTP API as a library: the routes and handlers that serve a
//! LoonFS runtime, and the OpenAPI document that describes them.
//! Hosts own configuration, listeners, shutdown, and operational routes.

mod http;
mod state;

pub use http::metrics::HttpMetrics;
#[cfg(feature = "openapi")]
pub use http::openapi_document;
pub use http::request_limit::RequestLimit;
pub use http::{api_error_response, authenticate_routes, observe_routes, router};
pub use state::{
    AuthPolicy, BindingOptions, BindingState, HeldNamespace, Namespaces,
    DEFAULT_MAX_CONCURRENT_DOWNLOADS, DEFAULT_MAX_CONCURRENT_UPLOADS, DEFAULT_REQUEST_DEADLINE_MS,
};
