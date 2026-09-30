//! The representative LoonFS HTTP binding over a LoonFS runtime.
//! Hosts own configuration, listeners, shutdown, and operational routes.

mod http;
mod state;

pub use http::metrics::HttpMetrics;
#[cfg(feature = "openapi")]
pub use http::openapi_document;
pub use http::{api_error_response, authenticate_routes, observe_routes, router};
pub use state::{
    AuthPolicy, BindingOptions, BindingState, GrepMaintenance, Namespaces,
    DEFAULT_MAX_CONCURRENT_DOWNLOADS, DEFAULT_MAX_CONCURRENT_UPLOADS, DEFAULT_REQUEST_DEADLINE_MS,
};
