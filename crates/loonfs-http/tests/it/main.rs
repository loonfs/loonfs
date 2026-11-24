//! OpenAPI contract checks, and the maintenance scheduler through the
//! crate's public API.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

#[cfg(feature = "openapi")]
mod openapi;
mod scheduler;
