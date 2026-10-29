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
pub use http::usage::{UsageRecorder, UsageReport};
pub use http::{authenticate_routes, error_response, observe_routes, panic_message, router};
pub use state::{
    AuthPolicy, BindingConfig, BindingHandles, BindingState, GrepBinding, HeldNamespace, HostState,
    JobMode, Namespaces, WriterSessions, DEFAULT_MAX_CONCURRENT_DOWNLOADS,
    DEFAULT_MAX_CONCURRENT_UPLOADS, DEFAULT_REQUEST_DEADLINE_MS,
};
