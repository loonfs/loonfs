//! Every hand-written wire or durable spelling agrees with serde's.
//!
//! These enums spell their values twice: once through `rename_all` for
//! serde, and once in an `as_str` or `Display` that keys, labels, and
//! messages use. A rename on one side alone would split the two spellings.

use loonfs_types::format::control::ControlObjectKind;
use loonfs_types::format::manifest::{MetadataRowFamily, NamespaceManifestKind};
use loonfs_types::format::wal::WalEnvelopeKind;
use loonfs_types::UploadMode;
use loonfs_types::{
    AccessRight, ChecksumAlgorithm, ContentRefKind, EntryInodeKind, InodeKind, MetadataFamilyGroup,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

fn assert_spelling<T>(value: T, spelling: &str)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let expected = serde_json::Value::from(spelling);
    assert_eq!(
        serde_json::to_value(&value).expect("serialize the value"),
        expected,
        "{value:?}"
    );
    assert_eq!(
        serde_json::from_value::<T>(expected).expect("deserialize the spelling"),
        value,
        "{spelling}"
    );
}

#[test]
fn every_hand_written_spelling_matches_serde() {
    for right in AccessRight::ALL {
        assert_spelling(right, right.as_str());
    }
    for kind in ControlObjectKind::ALL {
        assert_spelling(kind, kind.as_str());
        assert_eq!(ControlObjectKind::parse(kind.as_str()), Some(kind));
    }
    assert_eq!(ControlObjectKind::parse("not_a_kind"), None);
    for group in MetadataFamilyGroup::ALL {
        assert_spelling(group, group.as_str());
    }
    for family in MetadataRowFamily::ALL {
        assert_spelling(family, family.as_str());
    }
    assert_spelling(
        WalEnvelopeKind::WalObject,
        WalEnvelopeKind::WalObject.as_str(),
    );
    assert_spelling(
        NamespaceManifestKind::Manifest,
        NamespaceManifestKind::Manifest.as_str(),
    );
    for kind in [InodeKind::File, InodeKind::Directory] {
        assert_spelling(kind, kind.as_str());
        assert_eq!(kind.to_string(), kind.as_str());
    }
    for algorithm in [ChecksumAlgorithm::Crc64nvme, ChecksumAlgorithm::Crc32c] {
        assert_spelling(algorithm, algorithm.as_str());
        assert_eq!(algorithm.to_string(), algorithm.as_str());
    }
    for mode in [
        UploadMode::ServiceProxied,
        UploadMode::DirectPut,
        UploadMode::DirectMultipart,
    ] {
        assert_spelling(mode, mode.as_str());
    }
    assert_spelling(ContentRefKind::BlobV1, &ContentRefKind::BlobV1.to_string());
    for kind in [
        EntryInodeKind::File,
        EntryInodeKind::Directory,
        EntryInodeKind::Unknown,
    ] {
        assert_spelling(kind, &kind.to_string());
    }
}
