//! Path parsing, reading, and mutation planning over namespace metadata.

pub(crate) mod helpers;
pub(crate) mod read;
pub(crate) mod write;

pub use helpers::{ensure_mutation_path, parse_mutation_path};
