//! Namespace control helpers.
//!
//! A namespace is one durable filesystem history. Most callers should use
//! [`crate::NamespaceEngine`]; these helpers are for runtime and admin code
//! that needs direct namespace inspection.

pub(crate) mod bootstrap;
pub(crate) mod catalog;
pub(crate) mod control;
pub(crate) mod delete;
pub(crate) mod fork;
pub(crate) mod repair;
pub(crate) mod status;
pub(crate) mod writer_epoch;

pub use bootstrap::BootstrapNamespaceError;
pub use loonfs_api::wire::control::{HeadState, HeadStateEnvelope};
pub use repair::repair_namespace;
