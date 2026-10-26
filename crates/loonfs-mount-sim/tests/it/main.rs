//! Integration tests of the simulated server: its wire shapes, commits, change feed, listings,
//! content reads, snapshots, fault hooks, and the real client running against it.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod client;
mod commits;
mod content;
mod faults;
mod feed;
mod listing;
mod snapshots;
mod support;
mod wire;
