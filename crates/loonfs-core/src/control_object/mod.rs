//! Shared loading and validation for mutable durable control objects.

mod error;
mod load;

pub use error::ControlObjectLoadError;
pub(crate) use load::{
    core_control_load_error, expect_identity_field, expect_namespace, load_control_object,
    LoadedControl,
};
