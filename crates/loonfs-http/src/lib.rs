//! The representative LoonFS HTTP binding over runtime handles.
//! Hosts own configuration, listeners, shutdown, and operational routes.

mod http;
mod state;

pub use http::metrics::HttpMetrics;
#[cfg(feature = "openapi")]
pub use http::openapi_document;
pub use http::{api_error_response, authenticate_routes, observe_routes, router, RouterSurface};
pub use state::{AuthPolicy, BindingOptions, BindingState, GrepMaintenance};
