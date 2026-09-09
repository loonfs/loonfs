//! Shared loading and validation for mutable durable control objects.

mod error;
mod load;

pub use error::ControlObjectLoadError;
pub use load::LoadedControl;
pub(crate) use load::{expect_identity_field, expect_namespace, load_control_object};
