//! The LoonFS HTTP API as a library: the routes and handlers that serve a
//! LoonFS runtime, and the OpenAPI document that describes them.
//! Hosts own configuration, listeners, shutdown, and operational routes.

mod gc;
mod http;
mod state;

pub use gc::gc_namespace;
pub use http::metrics::HttpMetrics;
#[cfg(feature = "openapi")]
pub use http::openapi_document;
pub use http::request_limit::RequestLimit;
pub use http::{authenticate_routes, error_response, observe_routes, router};
pub use state::{
    transfer_permits, AuthPolicy, BindingConfig, BindingState, GrepBinding, HeldNamespace, JobMode,
    Namespaces, SweepRetry, DEFAULT_MAX_CONCURRENT_DOWNLOADS, DEFAULT_MAX_CONCURRENT_UPLOADS,
    DEFAULT_REQUEST_DEADLINE_MS,
};
