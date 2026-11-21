//! Durable upload sessions for starting, staging, completing, and aborting
//! content uploads, including direct-to-provider transfers.
//!
//! A session starts open and ends as either completed or aborted. The
//! put-if-absent that creates its outcome record determines the result (see
//! [`records`]). Provider cleanup runs only after that durable transition.
//!
//! Every uploaded content object is created through a session. Before
//! metadata references the object, the session record is the durable owner
//! and status that garbage collection checks.

pub(crate) mod abort;
pub(crate) mod begin;
pub(crate) mod complete;
pub(crate) mod direct;
pub(crate) mod proxied;
pub(crate) mod records;
pub(crate) mod session;
pub(crate) mod staging;
pub(crate) mod status;
#[cfg(test)]
mod tests;

pub use self::begin::{DirectMultipartUploadTarget, DirectPutUploadTarget};
pub use self::direct::{MultipartPartTarget, MultipartPartTargets};
pub use self::status::UploadSessionView;
