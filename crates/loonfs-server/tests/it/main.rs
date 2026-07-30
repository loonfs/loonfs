//! One binary for the crate's integration tests: every former
//! `tests/<name>.rs` file is a module here, so the suite links once and
//! runs its tests as threads instead of as separate processes.

mod common;
mod direct_put_real_provider;
mod grep_modes;
mod http_admin;
mod http_auth;
mod http_commits;
mod http_limits;
mod http_pagination;
mod http_paths;
mod http_retry;
mod http_smoke;
mod http_uploads;

// The OpenAPI document only exists behind the feature that generates it.
#[cfg(feature = "openapi")]
mod openapi;
