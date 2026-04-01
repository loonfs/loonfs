//! Shared IDs, enums, and plain data structures used across the LoonDB workspace.
//!
//! This is the foundation crate with no internal dependencies. All types are pure data:
//! serializable, cloneable, and side-effect free. Identity types like [`InodeId`],
//! [`NamespaceId`], [`ChangeSeq`], and [`FenceToken`] are the vocabulary shared by every
//! other crate in the workspace.

#![forbid(unsafe_code)]

mod checkpoint;
mod client;
mod conflict;
mod content;
mod control;
mod digest;
mod ids;
mod wal;

pub use checkpoint::*;
pub use client::*;
pub use conflict::*;
pub use content::*;
pub use control::*;
pub use digest::sha256_digest;
pub use ids::*;
pub use wal::*;

#[cfg(test)]
mod tests;
