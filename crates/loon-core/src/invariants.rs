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
    (HeadAndLeaseFenceTokensAgree, "head_and_lease_fence_tokens_agree"),
    (NextInodeIdIsMonotonic, "next_inode_id_is_monotonic"),
    (CreateMutationConsumesNextInodeId, "create_mutation_consumes_next_inode_id"),
    (CreateFileRequiresDurableContent, "create_file_requires_durable_content"),
    (ReplaceFileRequiresDurableContent, "replace_file_requires_durable_content"),
    (RestoreRevisionRequiresDurableContent, "restore_revision_requires_durable_content"),
    (SubtreeTombstoneBlocksDescendantMutation, "subtree_tombstone_blocks_descendant_mutation"),

    // Namespace core metadata apply invariants.
    (CreateDirWritesInodeAndDirentryRows, "create_dir_writes_inode_and_direntry_rows"),
    (CreateFileWritesInodeDirentryAndInitialRevision, "create_file_writes_inode_direntry_and_initial_revision"),
    (ReplaceFileAppendsNewRevisionHead, "replace_file_appends_new_revision_head"),
    (RestoreCreatesNewRevisionHead, "restore_creates_new_revision_head"),
    (DeleteFileWritesTombstoneRow, "delete_file_writes_tombstone_row"),
    (RenameAppendsNewDirentryBinding, "rename_appends_new_direntry_binding"),
    (DeleteSubtreeWritesTombstoneRow, "delete_subtree_writes_tombstone_row"),

    // Namespace core WAL replay invariants.
    (WalPayloadChecksumMatchesPayload, "wal_payload_checksum_matches_payload"),
    (WalKeyMatchesSegmentSeqRange, "wal_key_matches_segment_seq_range"),
    (HeadPublishRequiresDurableWal, "head_publish_requires_durable_wal"),
    (WalReplayRequiresMatchingNamespace, "wal_replay_requires_matching_namespace"),
    (WalReplayRequiresMatchingBaseHeadSeq, "wal_replay_requires_matching_base_head_seq"),
    (WalTailSeqIsContiguous, "wal_tail_seq_is_contiguous"),
    (WalReplayAppliesMetadataRows, "wal_replay_applies_metadata_rows"),

    // Namespace core manifest replay invariants.
    (NamespaceManifestChecksumMatchesPayload, "namespace_manifest_checksum_matches_payload"),
    (NamespaceManifestKeyMatchesSeq, "namespace_manifest_key_matches_seq"),
    (NamespaceManifestMustBeVerified, "namespace_manifest_must_be_verified"),
    (ManifestReplayRequiresAllManifestSegments, "manifest_replay_requires_all_manifest_segments"),
    (MetadataFileRefMatchesPayload, "manifest_segment_descriptor_matches_payload"),
    (MetadataSstRowsRestoreBasisMetadata, "manifest_segment_rows_restore_basis_metadata"),
    (ManifestPlusWalTailReproducesHead, "manifest_plus_wal_tail_reproduces_head"),
    (ManifestPlusWalTailReproducesMetadata, "manifest_plus_wal_tail_reproduces_metadata"),

    // Background work progress invariants.
    (ProgressObjectChecksumMatchesPayload, "progress_object_checksum_matches_payload"),
    (ProgressObjectKeyMatchesNamespaceAndWorkClass, "progress_object_key_matches_namespace_and_work_class"),
    (ProgressThroughSeqAdvancesMonotonically, "progress_through_seq_advances_monotonically"),

    // Background work queue shard object invariants.
    (QueueShardChecksumMatchesPayload, "queue_shard_checksum_matches_payload"),
    (QueueShardKeyMatchesShardId, "queue_shard_key_matches_shard_id"),
    (QueueShardCasProtectsUpdates, "queue_shard_cas_protects_updates"),

    // Background work queue mutation invariants.
    (LostEnqueueRepairEnqueuesWhenHeadOutpacesProgress, "lost_enqueue_repair_enqueues_when_head_outpaces_progress"),
    (ManifestRepairDedupeKeyIsNamespaceScoped, "manifest_repair_dedupe_key_is_namespace_scoped"),
    (ManifestRepairClaimedJobGetsFollowUp, "manifest_repair_claimed_job_gets_follow_up"),
    (BrokerLeaseTakeoverIncrementsEpoch, "broker_lease_takeover_increments_epoch"),
    (ActiveBrokerLeaseRequiredForShardMutation, "active_broker_lease_required_for_shard_mutation"),
    (ClaimTimeoutAllowsSteal, "claim_timeout_allows_steal"),
    (WorkerHeartbeatRequiresMatchingClaimToken, "worker_heartbeat_requires_matching_claim_token"),
    (StaleClaimTokenCannotComplete, "stale_claim_token_cannot_complete"),
    (StolenJobCompletesOnce, "stolen_job_completes_once"),

    // Background work manifest head publish invariants.
    (ManifestPublishRequiresVerifiedManifest, "manifest_publish_requires_verified_manifest"),
    (ManifestHintSeqAdvancesMonotonically, "manifest_hint_seq_advances_monotonically"),
    (RetentionFloorSeqAdvancesMonotonically, "retention_floor_seq_advances_monotonically"),
    (RetentionFloorSeqRequiresManifestCoverage, "retention_floor_seq_requires_manifest_coverage"),
    (RetentionFloorSeqRequiresDerivedProgress, "retention_floor_seq_requires_derived_progress"),
    (RetentionFloorSeqRespectsPolicyGate, "retention_floor_seq_respects_policy_gate"),

    // Content object file invariants.
    (WholeFileContentRefKindIsSupported, "whole_file_content_ref_kind_is_supported"),
    (WholeFileContentObjectKeyMatchesDigest, "whole_file_content_object_key_matches_digest"),
    (WholeFileContentSizeMatchesRef, "whole_file_content_size_matches_ref"),
    (WholeFileContentDigestMatchesRef, "whole_file_content_digest_matches_ref"),

    // Manifest object immutable invariants.
    (MetadataSstPayloadChecksumMatchesPayload, "manifest_segment_payload_checksum_matches_payload"),
    (MetadataSegmentKeyMatchesFamilyAndIndex, "manifest_segment_key_matches_family_and_index"),
    (VerifiedNamespaceManifestRequiresDurableSegments, "verified_namespace_manifest_requires_durable_segments"),
    (NamespaceManifestPreservesHeadSummary, "namespace_manifest_preserves_head_summary"),
    (NamespaceManifestPreservesBasisMetadata, "namespace_manifest_preserves_basis_metadata"),

    // Client transfer download invariants.
    (DownloadTransferByteRangeAdvancesMonotonically, "download_transfer_byte_range_advances_monotonically"),
    (DownloadTransferResetRecordsDurableIssue, "download_transfer_reset_records_durable_issue"),
    (DownloadCompletionClearsTransferLedger, "download_completion_clears_transfer_ledger"),
    (DownloadMaterializationUpdatesLocalStateAndSyncAnchor, "download_materialization_updates_local_state_and_sync_anchor"),

    // Client transfer inode upload invariants.
    (InodeUploadContentProgressAdvancesMonotonically, "inode_upload_content_progress_advances_monotonically"),
    (InodeUploadDispatchWaitsForStagedContent, "inode_upload_dispatch_waits_for_staged_content"),
    (InodeUploadRetryReusesPendingInodeMutation, "inode_upload_retry_reuses_pending_inode_mutation"),
    (InodeUploadCompletionClearsTransferLedger, "inode_upload_completion_clears_transfer_ledger"),
    (InodeUploadTransferResetRecordsDurableIssue, "inode_upload_transfer_reset_records_durable_issue"),

    // Client transfer local-only upload invariants.
    (LocalOnlyUploadContentProgressAdvancesMonotonically, "local_only_upload_content_progress_advances_monotonically"),
    (LocalOnlyUploadDispatchWaitsForStagedContent, "local_only_upload_dispatch_waits_for_staged_content"),
    (LocalOnlyUploadRetryReusesPendingClientMutation, "local_only_upload_retry_reuses_pending_client_mutation"),
    (LocalOnlyUploadCompletionClearsTempTransferLedger, "local_only_upload_completion_clears_temp_transfer_ledger"),
    (LocalOnlyUploadBindClearsTempIssueAndTransferLedger, "local_only_upload_bind_clears_temp_issue_and_transfer_ledger"),
    (LocalOnlyUploadTransferResetRecordsDurableIssue, "local_only_upload_transfer_reset_records_durable_issue"),

    // Client reconciliation invariants.
    (SameInodeConflictPreservesLoserArtifact, "same_inode_conflict_preserves_loser_artifact"),
    (SameInodeConflictKeepsCanonicalPath, "same_inode_conflict_keeps_canonical_path"),
    (DeleteVsEditConflictPreservesLoserArtifact, "delete_vs_edit_conflict_preserves_loser_artifact"),
    (DeleteVsEditConflictRemovesCanonicalPath, "delete_vs_edit_conflict_removes_canonical_path"),
    (RenameVsEditConflictPreservesLoserArtifact, "rename_vs_edit_conflict_preserves_loser_artifact"),
    (RenameVsEditConflictConvergesRemoteWinner, "rename_vs_edit_conflict_converges_remote_winner"),
    (PathBindingCollisionPreservesLoserArtifact, "path_binding_collision_preserves_loser_artifact"),
    (PathBindingCollisionKeepsWinnerCanonicalPath, "path_binding_collision_keeps_winner_canonical_path"),
    (SubtreeDeleteConflictPreservesLoserArtifact, "subtree_delete_conflict_preserves_loser_artifact"),
    (SubtreeDeleteConflictPreservesFullSubtreeEntries, "subtree_delete_conflict_preserves_full_subtree_entries"),
    (SubtreeDeleteConflictRemovesCanonicalPath, "subtree_delete_conflict_removes_canonical_path"),
    (SubtreeDeleteConflictClearsPreservedTempRows, "subtree_delete_conflict_clears_preserved_temp_rows"),
    (SubtreeRenameConflictPreservesLoserArtifact, "subtree_rename_conflict_preserves_loser_artifact"),
    (SubtreeRenameConflictRevertsBoundDescendantsToWinnerState, "subtree_rename_conflict_reverts_bound_descendants_to_winner_state"),
    (SubtreeRenameConflictKeepsWinnerCanonicalPath, "subtree_rename_conflict_keeps_winner_canonical_path"),
    (SubtreeRenameConflictClearsPreservedTempRows, "subtree_rename_conflict_clears_preserved_temp_rows"),
    (SubtreeConflictArtifactKeyMatchesNamespaceAndId, "subtree_conflict_artifact_key_matches_namespace_and_id"),
    (SubtreeConflictArtifactEntriesAreDurable, "subtree_conflict_artifact_entries_are_durable"),
    (StablePathsDefaultDoesNotMaterializeVisibleSibling, "stable_paths_default_does_not_materialize_visible_sibling"),
    (StablePathsDefaultDoesNotMaterializeVisibleSiblingTree, "stable_paths_default_does_not_materialize_visible_sibling_tree"),
    (ConflictArtifactKeyMatchesNamespaceAndId, "conflict_artifact_key_matches_namespace_and_id"),
    (ConflictArtifactLoserContentIsDurable, "conflict_artifact_loser_content_is_durable"),
    (RemoteObservationConvergenceClearsDirtyAndPlannedAction, "remote_observation_convergence_clears_dirty_and_planned_action"),
    (RemoteObservationConvergenceClearsPendingInodeMutation, "remote_observation_convergence_clears_pending_inode_mutation"),
    (RemoteObservationConvergenceAdvancesSyncAnchor, "remote_observation_convergence_advances_sync_anchor"),
    (RemoteObservationLateBindEstablishesRemoteLocalAndAnchor, "remote_observation_late_bind_establishes_remote_local_and_anchor"),
    (RemoteObservationLateBindClearsTempLocalState, "remote_observation_late_bind_clears_temp_local_state"),
    (RemoteObservationLateBindClearsTempTransferAndIssueRows, "remote_observation_late_bind_clears_temp_transfer_and_issue_rows"),
    (RemoteObservationLateBindRetainsPendingClientMutationUntilResponse, "remote_observation_late_bind_retains_pending_client_mutation_until_response"),
    (RemoteObservationAmbiguousBindRecordsDurableIssue, "remote_observation_ambiguous_bind_records_durable_issue"),
    (RemoteObservationAmbiguousBindAvoidsPartialMigration, "remote_observation_ambiguous_bind_avoids_partial_migration"),
    (RemoteObservationActiveUploadPreservesTransferAndPendingInodeMutation, "remote_observation_active_upload_preserves_transfer_and_pending_inode_mutation"),
    (RemoteObservationActiveDownloadPreservesTransferLedger, "remote_observation_active_download_preserves_transfer_ledger"),
    (RemoteOnlyMaterializationWaitsForParentMaterialization, "remote_only_materialization_waits_for_parent_materialization"),
    (RemoteOnlyDirectoryMaterializationUpdatesLocalStateAndSyncAnchor, "remote_only_directory_materialization_updates_local_state_and_sync_anchor"),
    (RemoteOnlyDirectoryMaterializationClearsPlannedAction, "remote_only_directory_materialization_clears_planned_action"),
    (RemoteOnlyDirectoryMaterializationFailureRecordsDurableIssue, "remote_only_directory_materialization_failure_records_durable_issue"),
    (RemoteDeletePlansApplyRemoteDelete, "remote_delete_plans_apply_remote_delete"),
    (ApplyRemoteDeletePreservesRemoteTombstone, "apply_remote_delete_preserves_remote_tombstone"),
    (ApplyRemoteDeleteClearsLocalStateAndSyncAnchor, "apply_remote_delete_clears_local_state_and_sync_anchor"),
    (ApplyRemoteDeleteClearsPlannedAction, "apply_remote_delete_clears_planned_action"),
    (ApplyRemoteDeleteFailureRecordsDurableIssue, "apply_remote_delete_failure_records_durable_issue"),
    (RemoteSubtreeDeletePlansApplyRemoteSubtreeDelete, "remote_subtree_delete_plans_apply_remote_subtree_delete"),
    (ApplyRemoteSubtreeDeletePreservesRootRemoteTombstone, "apply_remote_subtree_delete_preserves_root_remote_tombstone"),
    (ApplyRemoteSubtreeDeleteClearsDescendantRemoteRows, "apply_remote_subtree_delete_clears_descendant_remote_rows"),
    (ApplyRemoteSubtreeDeleteClearsRemoteOnlyDescendants, "apply_remote_subtree_delete_clears_remote_only_descendants"),
    (ApplyRemoteSubtreeDeleteClearsLocalStateAndSyncAnchorForSubtree, "apply_remote_subtree_delete_clears_local_state_and_sync_anchor_for_subtree"),
    (ApplyRemoteSubtreeDeleteClearsSubtreePlannedActions, "apply_remote_subtree_delete_clears_subtree_planned_actions"),
    (ApplyRemoteSubtreeDeleteFailureRecordsDurableIssue, "apply_remote_subtree_delete_failure_records_durable_issue"),
    (RemoteSubtreePathChangePlansApplyRemoteSubtreeRename, "remote_subtree_path_change_plans_apply_remote_subtree_rename"),
    (MaterializedTargetParentUnblocksApplyRemoteSubtreeRename, "materialized_target_parent_unblocks_apply_remote_subtree_rename"),
    (ApplyRemoteSubtreeRenameUpdatesRootLocalStateAndSyncAnchor, "apply_remote_subtree_rename_updates_root_local_state_and_sync_anchor"),
    (ApplyRemoteSubtreeRenamePreservesDescendantDurableState, "apply_remote_subtree_rename_preserves_descendant_durable_state"),
    (ApplyRemoteSubtreeRenamePreservesRemoteOnlyDescendants, "apply_remote_subtree_rename_preserves_remote_only_descendants"),
    (ApplyRemoteSubtreeRenameClearsRootPlannedAction, "apply_remote_subtree_rename_clears_root_planned_action"),
    (ApplyRemoteSubtreeRenameFailureRecordsDurableIssue, "apply_remote_subtree_rename_failure_records_durable_issue"),
    (RemotePathChangePlansApplyRemoteRename, "remote_path_change_plans_apply_remote_rename"),
    (MaterializedTargetParentUnblocksApplyRemoteRename, "materialized_target_parent_unblocks_apply_remote_rename"),
    (ApplyRemoteRenameUpdatesLocalStateAndSyncAnchor, "apply_remote_rename_updates_local_state_and_sync_anchor"),
    (ApplyRemoteRenameClearsPlannedAction, "apply_remote_rename_clears_planned_action"),
    (ApplyRemoteRenameFailureRecordsDurableIssue, "apply_remote_rename_failure_records_durable_issue"),

    // Client conflict artifact recovery invariants.
    (ConflictArtifactDiscoveryCachesNamespaceArtifacts, "conflict_artifact_discovery_caches_namespace_artifacts"),
    (ConflictArtifactShowReportsImplicitActiveState, "conflict_artifact_show_reports_implicit_active_state"),
    (ConflictArtifactUnarchiveRestoresActiveVisibility, "conflict_artifact_unarchive_restores_active_visibility"),
    (ConflictArtifactRestoreDoesNotChangeArchiveState, "conflict_artifact_restore_does_not_change_archive_state"),
    (FileConflictArtifactRestoreReproducesLoserContent, "file_conflict_artifact_restore_reproduces_loser_content"),
    (FileConflictArtifactRestoreKeepsCanonicalPathUntouched, "file_conflict_artifact_restore_keeps_canonical_path_untouched"),
    (SubtreeConflictArtifactRestoreReproducesFullLoserTree, "subtree_conflict_artifact_restore_reproduces_full_loser_tree"),
    (SubtreeConflictArtifactRestoreUsesDeterministicEntryOrder, "subtree_conflict_artifact_restore_uses_deterministic_entry_order"),
    (SubtreeConflictArtifactRestoreKeepsCanonicalTreeUntouched, "subtree_conflict_artifact_restore_keeps_canonical_tree_untouched"),
    (ConflictArtifactRestoreRequiresExplicitAbsentDestination, "conflict_artifact_restore_requires_explicit_absent_destination"),

    // Simulation interleaving invariants.
    (ClientRetryReusesPendingRequestAfterDelayedResponse, "client_retry_reuses_pending_request_after_delayed_response"),
    (DuplicateResponseIsIdempotent, "duplicate_response_is_idempotent"),
    (LateRemoteObservationDoesNotDuplicateWinnerApply, "late_remote_observation_does_not_duplicate_winner_apply"),
    (ResponseAfterNewerObservationIsIdempotent, "response_after_newer_observation_is_idempotent"),
    (SimTraceOrderIsSeedStable, "sim_trace_order_is_seed_stable"),
    (StaleWriterPublishRemainsFencedAfterHandover, "stale_writer_publish_remains_fenced_after_handover"),
    (StaleWriterFenceSurvivesInflightClientRequest, "stale_writer_fence_survives_inflight_client_request"),
    (ManifestPublishWaitsForRequiredProgressUnderInterleaving, "manifest_publish_waits_for_required_progress_under_interleaving"),
    (ManifestPublishPreservesMonotonicHeadSummaryUnderInterleaving, "manifest_publish_preserves_monotonic_head_summary_under_interleaving"),
    (RepairLostEnqueueTracksLatestVisibleHeadSeq, "repair_lost_enqueue_tracks_latest_visible_head_seq"),
    (ManifestPublishUsesLatestVisibleHeadAfterClientServerAdvance, "manifest_publish_uses_latest_visible_head_after_client_server_advance"),
    (RepairLostEnqueueTracksLatestVisibleHeadAfterClientServerAdvance, "repair_lost_enqueue_tracks_latest_visible_head_after_client_server_advance"),
    (QueueSimTraceOrderIsSeedStable, "queue_sim_trace_order_is_seed_stable"),
    (BackgroundSimTraceOrderIsSeedStable, "background_sim_trace_order_is_seed_stable"),
    (UnifiedNamespaceSimTraceOrderIsSeedStable, "unified_namespace_sim_trace_order_is_seed_stable"),

    // Production invariant report IDs that were previously only in the flat catalogue.
    (NoOrphanedLiveEntry, "no_orphaned_live_entry"),
    (VisibleRevisionPointsToDurableContent, "visible_revision_points_to_durable_content"),

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
