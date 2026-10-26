//! One binary for the crate's integration tests, which mount a namespace of
//! the simulated server through the real client and check what the engine
//! shows and what it asks the server for.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod admission;
mod dependencies;
mod feed;
mod reads;
mod support;
