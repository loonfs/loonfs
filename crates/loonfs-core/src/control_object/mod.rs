//! Shared loading and validation for mutable durable control objects.

mod error;
mod load;

pub use error::ControlObjectLoadError;
pub(crate) use load::{
    core_control_load_error, expect_foreign_fork_basis, expect_identity_field, expect_namespace,
    expect_own_manifest, load_control_object, LoadedControl,
};
