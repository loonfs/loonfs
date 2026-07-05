//! Registry of invariant identifiers recorded by validation and replay.
//!
//! An id exists here iff some production code pushes it into a
//! `checked_invariants` ledger. Wire names are stable strings, so re-adding
//! an id later is purely additive; do not reserve ids for machinery that
//! does not exist yet.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

macro_rules! define_invariant_ids {
    ($(($const_name:ident, $wire_name:literal)),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum InvariantId {
            $($const_name,)+
        }

        impl InvariantId {
            pub const ALL: &'static [Self] = &[
                $(Self::$const_name,)+
            ];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$const_name => $wire_name,)+
                }
            }
        }

        impl FromStr for InvariantId {
            type Err = UnknownInvariantId;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($wire_name => Ok(Self::$const_name),)+
                    _ => Err(UnknownInvariantId(value.to_owned())),
                }
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownInvariantId(String);

impl fmt::Display for UnknownInvariantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown invariant id `{}`", self.0)
    }
}

impl std::error::Error for UnknownInvariantId {}

impl fmt::Display for InvariantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for InvariantId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for InvariantId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

define_invariant_ids! {
    // Namespace core commit frame invariants.
    (StaleWriterCannotPublish, "stale_writer_cannot_publish"),
    (NextInodeIdIsMonotonic, "next_inode_id_is_monotonic"),
    (CreateMutationConsumesNextInodeId, "create_mutation_consumes_next_inode_id"),
    (CreateFileRequiresDurableContent, "create_file_requires_durable_content"),
    (ReplaceFileRequiresDurableContent, "replace_file_requires_durable_content"),
    (RestoreRevisionRequiresDurableContent, "restore_revision_requires_durable_content"),
    (SubtreeTombstoneBlocksDescendantMutation, "subtree_tombstone_blocks_descendant_mutation"),

    // Namespace core WAL replay invariants.
    (WalPayloadChecksumMatchesPayload, "wal_payload_checksum_matches_payload"),
    (WalKeyMatchesSegmentSeqRange, "wal_key_matches_segment_seq_range"),
    (HeadPublishRequiresDurableWal, "head_publish_requires_durable_wal"),
    (WalReplayRequiresMatchingNamespace, "wal_replay_requires_matching_namespace"),
    (WalReplayRequiresMatchingBaseHeadSeq, "wal_replay_requires_matching_base_head_seq"),
    (WalTailSeqIsContiguous, "wal_tail_seq_is_contiguous"),
    (WalReplayAppliesMetadataRows, "wal_replay_applies_metadata_rows"),

    // Content object file invariants.
    (WholeFileContentRefKindIsSupported, "whole_file_content_ref_kind_is_supported"),
    (WholeFileContentObjectKeyMatchesDigest, "whole_file_content_object_key_matches_digest"),
    (WholeFileContentSizeMatchesRef, "whole_file_content_size_matches_ref"),
    (WholeFileContentDigestMatchesRef, "whole_file_content_digest_matches_ref"),

    // Normalized WAL delta apply invariants.
    (CreateInodeWritesInodeRow, "create_inode_writes_inode_row"),
    (BindDirentryWritesDirentryBindRow, "bind_direntry_writes_direntry_bind_row"),
    (UnbindDirentryWritesUnbindRow, "unbind_direntry_writes_unbind_row"),
    (AppendFileRevisionWritesRevisionRow, "append_file_revision_writes_revision_row"),
    (TombstoneSubtreeWritesTombstoneRow, "tombstone_subtree_writes_tombstone_row"),
    (WalReplayRecordsCommitReceipt, "wal_replay_records_commit_receipt"),
}

#[cfg(test)]
mod tests {
    use super::InvariantId;
    use std::collections::BTreeSet;

    #[test]
    fn invariant_ids_round_trip_as_stable_strings() {
        for id in InvariantId::ALL {
            let encoded = serde_json::to_string(id).expect("serialize invariant id");
            let decoded: InvariantId =
                serde_json::from_str(&encoded).expect("deserialize invariant id");
            assert_eq!(*id, decoded);
            assert_eq!(encoded, format!("\"{}\"", id.as_str()));
        }
    }

    #[test]
    fn invariant_report_fields_serialize_as_stable_strings() {
        #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct InvariantReport {
            checked_invariants: Vec<InvariantId>,
        }

        let report = InvariantReport {
            checked_invariants: vec![
                InvariantId::WalPayloadChecksumMatchesPayload,
                InvariantId::HeadPublishRequiresDurableWal,
            ],
        };

        let encoded = serde_json::to_string(&report).expect("serialize report");
        assert_eq!(
            encoded,
            "{\"checked_invariants\":[\"wal_payload_checksum_matches_payload\",\"head_publish_requires_durable_wal\"]}"
        );

        let decoded: InvariantReport = serde_json::from_str(&encoded).expect("deserialize report");
        assert_eq!(decoded, report);
    }

    #[test]
    fn invariant_ids_have_no_duplicate_strings() {
        let names = InvariantId::ALL
            .iter()
            .map(|id| id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), InvariantId::ALL.len());
    }

    #[test]
    fn unknown_invariant_ids_fail_deserialization() {
        let error =
            serde_json::from_str::<InvariantId>("\"not_a_real_invariant\"").expect_err("unknown");
        assert!(error.to_string().contains("unknown invariant id"));
    }
}
