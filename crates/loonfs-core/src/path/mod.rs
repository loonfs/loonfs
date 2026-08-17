//! Path parsing, reading, and mutation planning over namespace metadata.

pub(crate) mod mutation_path;
pub(crate) mod read;
pub(crate) mod write;

pub use mutation_path::parse_mutation_path;
