//! One binary for the crate's integration tests: every former
//! `tests/<name>.rs` file is a module here, so the suite links once and
//! runs its tests as threads instead of as separate processes.

mod check_config;
mod common;
mod direct_put_real_provider;
mod grep_modes;
mod http_admin;
mod http_attributes;
mod http_attribution;
mod http_auth;
mod http_commits;
mod http_inodes;
mod http_limits;
mod http_metrics;
mod http_pagination;
mod http_paths;
mod http_retry;
mod http_smoke;
mod http_snapshots;
mod http_tls;
mod http_uploads;
mod local_cache;

// The OpenAPI document only exists behind the feature that generates it.
#[cfg(feature = "openapi")]
mod openapi;
