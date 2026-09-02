//! Scoped environment changes for tests.

use std::ffi::{OsStr, OsString};

/// Restores an environment variable when dropped.
#[derive(Debug)]
pub struct EnvGuard {
    name: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    /// Sets an environment variable until the guard is dropped.
    pub fn set(name: &'static str, value: impl AsRef<OsStr>) -> Self {
        let previous = std::env::var_os(name);
        std::env::set_var(name, value);
        Self { name, previous }
    }

    /// Removes an environment variable until the guard is dropped.
    pub fn unset(name: &'static str) -> Self {
        let previous = std::env::var_os(name);
        std::env::remove_var(name);
        Self { name, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
    }
}
