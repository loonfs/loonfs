//! One binary for the crate's integration tests, which mount a scripted
//! namespace through the client and check what the engine shows and asks
//! for.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod admission;
mod dependencies;
mod feed;
mod namespace;
mod reads;
mod service;
