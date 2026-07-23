//! Shared test helpers at the object-store boundary.
//!
//! This crate contains broadly reusable fault-injection stores, request
//! instrumentation, runtime helpers, pagination constructors, and HTTP test
//! setup. It depends only on `loonfs-api` and `loonfs-objectstore` among LoonFS
//! crates so `loonfs-core` tests can use it without a development-dependency
//! cycle. Helpers that require `loonfs-core`, `loonfs`, `loonfs-grep`, or
//! `loonfs-server` types do not belong here; they stay canonicalized inside
//! the crate that owns those types. Provider-specific conformance helpers also
//! remain in `loonfs-objectstore`.

pub mod block_on;
pub mod http;
pub mod ids;
pub mod stores;
