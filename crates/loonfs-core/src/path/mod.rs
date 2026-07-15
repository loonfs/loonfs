//! Path parsing, reading, and mutation planning over namespace metadata.

pub(crate) mod helpers;
pub(crate) mod query;
pub(crate) mod read;
pub(crate) mod write;

pub use helpers::{parse_mutation_path, validate_path_for_mutation};
