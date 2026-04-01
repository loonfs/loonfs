//! Thin server integration crate for the LoonDB mutation pipeline.
//!
//! The active surface is [`mutation`], which composes `loon-core` and related crates into the
//! authoritative create/replace execution path. The binary and HTTP shells are intentionally
//! quarantined during the semantic-core reset.

#![forbid(unsafe_code)]

pub mod core;
pub(crate) mod genesis;
pub mod mutation;
pub mod objectstore;
pub mod ops;
pub mod queue;
