//! Shared test helpers for the object-store boundary.
//!
//! This crate contains reusable fault-injection stores, instrumentation,
//! runtime helpers, pagination constructors, and HTTP setup. It depends only
//! on `loonfs-api` and `loonfs-objectstore`, which lets `loonfs-core` use it
//! without a dependency cycle. Helpers that require higher-level crate types
//! stay in the crate that owns those types.

pub mod block_on;
pub mod http;
pub mod ids;
pub mod stores;

pub use ids::test_actor;
