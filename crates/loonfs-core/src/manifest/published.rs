//! A manifest that a writer session's commit engine moves its position to:
//! one that a maintenance call on this runtime published, or one that a read
//! found above the position under the session's writer epoch.

use crate::namespace::control::CurrentManifest;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::{ChangeSeq, WalNo};

/// A manifest above a writer session's position, under the session's writer
/// epoch.
///
/// The session's commit engine moves its position to the manifest instead
/// of reloading the namespace (see
/// [`NamespaceCommitEngine::record_manifest_published`](crate::publish::NamespaceCommitEngine::record_manifest_published)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishedManifest {
    /// A manifest at its predecessor's head, which a compaction, a compactor
    /// claim, or a retention advance publishes. It folds no WAL objects, and
    /// it copies every field of its predecessor that replay reads.
    SameHead(ManifestRef),
    /// A manifest that a fold published. It folds the WAL through
    /// `folded_wal_no`.
    Fold {
        manifest: ManifestRef,
        folded_wal_no: WalNo,
    },
}

impl PublishedManifest {
    /// Returns `manifest` as a position whose basis is at `basis_head_seq`
    /// moves onto it: a manifest at that head, or a fold through the
    /// manifest's folded WAL number.
    pub fn above(manifest: &CurrentManifest, basis_head_seq: ChangeSeq) -> Self {
        let reference = manifest.manifest();
        if reference.head_seq == basis_head_seq {
            Self::SameHead(reference)
        } else {
            Self::Fold {
                manifest: reference,
                folded_wal_no: manifest.folded_wal_no(),
            }
        }
    }

    /// Returns the reference to the published manifest.
    pub fn manifest(&self) -> &ManifestRef {
        match self {
            Self::SameHead(manifest) | Self::Fold { manifest, .. } => manifest,
        }
    }
}

/// A maintenance call's response, and the manifest the call published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationReport<T> {
    pub response: T,
    /// The manifest this call published, or `None` when it published none
    /// (for example, a fold of a tail that a manifest already covered).
    pub published: Option<PublishedManifest>,
}

impl<T> PublicationReport<T> {
    /// Replaces the response and keeps the published manifest.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> PublicationReport<U> {
        PublicationReport {
            response: map(self.response),
            published: self.published,
        }
    }
}
