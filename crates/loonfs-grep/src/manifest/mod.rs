//! Grep hints, numbered manifests, and publication.

mod codec;
mod error;
mod state;
mod store;

#[cfg(test)]
mod tests;

pub use codec::{
    decode_grep_hint, decode_grep_manifest, encode_grep_hint, encode_grep_manifest,
    GrepManifestEnvelope,
};
pub use error::{GrepEnvelopeCodecError, GrepManifestError, GrepManifestStateError};
pub use state::{
    ChangeFeedResume, GrepHint, GrepIndexState, GrepIndexStatus, GrepManifestState,
    GrepReorganizeState, GrepSegmentRef,
};
pub use store::{
    load_current_grep_manifest, load_grep_manifest, publish_grep_manifest, raise_grep_hint,
    LoadedGrepHint, LoadedGrepManifest,
};
