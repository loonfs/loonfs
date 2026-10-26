//! Integration tests of the simulated server: its wire shapes, commits, change feed, listings,
//! content reads, and snapshots.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod commits;
mod content;
mod feed;
mod listing;
mod snapshots;
mod support;
mod wire;
