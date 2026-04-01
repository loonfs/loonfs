use loon_client::state_db::{AppliedRemoteObservation, SqliteStateDb, StateDbError};
use loon_server::objectstore::ObjectStore;
use loon_server::ops::{
    load_namespace_state_summary, translate_authoritative_state_to_remote_observations,
    NamespaceStateSummaryError, RemoteObservationTranslationError,
};
use loon_types::{ChangeSeq, NamespaceId, ObservedRemoteInode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeObservationImportReport {
    pub namespace_id: NamespaceId,
    pub authoritative_head_seq: ChangeSeq,
    pub translated_observation_count: usize,
    pub bound_local_only_count: usize,
    pub converged_bound_inode_count: usize,
    pub discovered_remote_only_count: usize,
    pub recorded_conflict_or_error_count: usize,
    pub updated_bound_remote_state_count: usize,
    pub ignored_stale_count: usize,
    pub ignored_unmatched_count: usize,
}

#[derive(Debug, Error)]
pub enum AuthoritativeObservationImportError {
    #[error("failed to load authoritative namespace state summary: {0}")]
    LoadSummary(#[from] NamespaceStateSummaryError),
    #[error("failed to translate authoritative namespace state into remote observations: {0}")]
    Translate(#[from] RemoteObservationTranslationError),
    #[error("failed to apply authoritative remote observations batch: {0}")]
    Apply(#[from] StateDbError),
}

pub fn import_authoritative_remote_observations<S: ObjectStore>(
    db: &mut SqliteStateDb,
    store: &S,
    namespace_id: &NamespaceId,
    applied_at_ms: u64,
) -> Result<AuthoritativeObservationImportReport, AuthoritativeObservationImportError> {
    let summary = load_namespace_state_summary(store, namespace_id)?;
    let observations = translate_authoritative_state_to_remote_observations(store, namespace_id)?;
    Ok(apply_translated_authoritative_remote_observations(
        db,
        namespace_id,
        summary.head.seq,
        &observations,
        applied_at_ms,
    )?)
}

fn apply_translated_authoritative_remote_observations(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    authoritative_head_seq: ChangeSeq,
    observations: &[ObservedRemoteInode],
    applied_at_ms: u64,
) -> Result<AuthoritativeObservationImportReport, StateDbError> {
    let outcomes = db.apply_remote_observations_batch(observations, applied_at_ms)?;
    Ok(summarize_authoritative_import_outcomes(
        namespace_id,
        authoritative_head_seq,
        observations.len(),
        &outcomes,
    ))
}

fn summarize_authoritative_import_outcomes(
    namespace_id: &NamespaceId,
    authoritative_head_seq: ChangeSeq,
    translated_observation_count: usize,
    outcomes: &[AppliedRemoteObservation],
) -> AuthoritativeObservationImportReport {
    let mut report = AuthoritativeObservationImportReport {
        namespace_id: namespace_id.clone(),
        authoritative_head_seq,
        translated_observation_count,
        bound_local_only_count: 0,
        converged_bound_inode_count: 0,
        discovered_remote_only_count: 0,
        recorded_conflict_or_error_count: 0,
        updated_bound_remote_state_count: 0,
        ignored_stale_count: 0,
        ignored_unmatched_count: 0,
    };
    for outcome in outcomes {
        match outcome {
            AppliedRemoteObservation::BoundLocalOnly(_) => report.bound_local_only_count += 1,
            AppliedRemoteObservation::ConvergedBoundInode(_) => {
                report.converged_bound_inode_count += 1;
            }
            AppliedRemoteObservation::DiscoveredRemoteOnly { .. } => {
                report.discovered_remote_only_count += 1;
            }
            AppliedRemoteObservation::RecordedConflictOrError { .. } => {
                report.recorded_conflict_or_error_count += 1;
            }
            AppliedRemoteObservation::UpdatedBoundRemoteState { .. } => {
                report.updated_bound_remote_state_count += 1;
            }
            AppliedRemoteObservation::IgnoredStale { .. } => report.ignored_stale_count += 1,
            AppliedRemoteObservation::IgnoredUnmatched { .. } => {
                report.ignored_unmatched_count += 1;
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::{
        apply_translated_authoritative_remote_observations,
        import_authoritative_remote_observations, AuthoritativeObservationImportError,
    };
    use loon_client::state_db::{
        LocalFileStateRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
    };
    use loon_server::core::checkpoint::prepare_checkpoint;
    use loon_server::core::metadata::{
        DirentryRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
    };
    use loon_server::objectstore::fs::LocalFsStore;
    use loon_server::objectstore::keys::{blob, content_manifest, namespace_head, namespace_lease};
    use loon_server::objectstore::ObjectStore;
    use loon_types::{
        sha256_digest, ChangeSeq, ContentBlockDescriptor, ContentManifestEnvelope,
        ContentManifestPayload, ControlObjectKind, FenceToken, HeadState, HeadStateEnvelope,
        InodeId, InodeKind, LeaseState, LeaseStateEnvelope, NamespaceId, ObservedRemoteInode,
        RevisionNo,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn authoritative_import_discovers_visible_namespace_state_into_fresh_client_db() {
        let temp_dir = unique_temp_dir("ops-import-fresh");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let snapshot = metadata_with_single_file(&namespace_id, ChangeSeq(1), "hello.txt", b"v1\n");
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(1), &snapshot.metadata);

        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client DB");
        let report = import_authoritative_remote_observations(
            &mut db,
            &store,
            &namespace_id,
            1_700_000_800_000,
        )
        .expect("import authoritative observations");

        assert_eq!(report.authoritative_head_seq, ChangeSeq(1));
        assert_eq!(report.translated_observation_count, 2);
        assert_eq!(report.discovered_remote_only_count, 2);
        let summary = db
            .load_namespace_state_summary(&namespace_id)
            .expect("load namespace summary after import");
        assert_eq!(summary.remote_state.len(), 2);
        assert!(summary.remote_state.iter().any(|row| {
            row.inode_id == InodeId(2)
                && row.display_name == "hello.txt"
                && row.content_digest.as_deref() == Some(snapshot.file_digest.as_str())
        }));
    }

    #[test]
    fn authoritative_import_is_idempotent_for_repeated_head() {
        let temp_dir = unique_temp_dir("ops-import-repeat");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let snapshot = metadata_with_single_file(&namespace_id, ChangeSeq(1), "hello.txt", b"v1\n");
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(1), &snapshot.metadata);

        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client DB");
        import_authoritative_remote_observations(&mut db, &store, &namespace_id, 1_700_000_800_000)
            .expect("initial import");
        let repeated = import_authoritative_remote_observations(
            &mut db,
            &store,
            &namespace_id,
            1_700_000_800_100,
        )
        .expect("repeat import");

        assert_eq!(repeated.translated_observation_count, 2);
        assert_eq!(repeated.ignored_stale_count, 2);
        assert_eq!(repeated.discovered_remote_only_count, 0);
    }

    #[test]
    fn authoritative_import_after_replace_updates_bound_remote_state() {
        let temp_dir = unique_temp_dir("ops-import-replace");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let initial = metadata_with_single_file(&namespace_id, ChangeSeq(1), "hello.txt", b"v1\n");
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(1), &initial.metadata);

        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client DB");
        seed_bound_client_state(&mut db, &namespace_id, &initial, ChangeSeq(1), "hello.txt");

        let replaced = metadata_with_single_file(&namespace_id, ChangeSeq(2), "hello.txt", b"v2\n");
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(2), &replaced.metadata);

        let report = import_authoritative_remote_observations(
            &mut db,
            &store,
            &namespace_id,
            1_700_000_800_200,
        )
        .expect("import replaced authoritative state");

        assert_eq!(report.converged_bound_inode_count, 1);
        assert_eq!(report.updated_bound_remote_state_count, 1);
        let summary = db
            .load_namespace_state_summary(&namespace_id)
            .expect("load namespace summary after replace");
        let remote = summary
            .remote_state
            .iter()
            .find(|row| row.inode_id == InodeId(2))
            .expect("file remote row");
        assert_eq!(
            remote.content_digest.as_deref(),
            Some(replaced.file_digest.as_str())
        );
    }

    #[test]
    fn authoritative_import_after_rename_updates_bound_remote_state() {
        let temp_dir = unique_temp_dir("ops-import-rename");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let initial = metadata_with_single_file(&namespace_id, ChangeSeq(1), "hello.txt", b"v1\n");
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(1), &initial.metadata);

        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client DB");
        seed_bound_client_state(&mut db, &namespace_id, &initial, ChangeSeq(1), "hello.txt");

        let renamed =
            metadata_with_single_file(&namespace_id, ChangeSeq(2), "renamed.txt", b"v1\n");
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(2), &renamed.metadata);

        let report = import_authoritative_remote_observations(
            &mut db,
            &store,
            &namespace_id,
            1_700_000_800_300,
        )
        .expect("import renamed authoritative state");

        assert_eq!(report.converged_bound_inode_count, 1);
        assert_eq!(report.updated_bound_remote_state_count, 1);
        let summary = db
            .load_namespace_state_summary(&namespace_id)
            .expect("load namespace summary after rename");
        let remote = summary
            .remote_state
            .iter()
            .find(|row| row.inode_id == InodeId(2))
            .expect("file remote row");
        assert_eq!(remote.display_name, "renamed.txt");
    }

    #[test]
    fn authoritative_import_after_delete_marks_bound_remote_state_deleted() {
        let temp_dir = unique_temp_dir("ops-import-delete");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let initial = metadata_with_single_file(&namespace_id, ChangeSeq(1), "hello.txt", b"v1\n");
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(1), &initial.metadata);

        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client DB");
        seed_bound_client_state(&mut db, &namespace_id, &initial, ChangeSeq(1), "hello.txt");

        let deleted =
            metadata_with_deleted_file(&namespace_id, ChangeSeq(2), &initial.manifest_digest);
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(2), &deleted.metadata);

        let report = import_authoritative_remote_observations(
            &mut db,
            &store,
            &namespace_id,
            1_700_000_800_400,
        )
        .expect("import deleted authoritative state");

        assert_eq!(report.converged_bound_inode_count, 1);
        assert_eq!(report.updated_bound_remote_state_count, 1);
        let summary = db
            .load_namespace_state_summary(&namespace_id)
            .expect("load namespace summary after delete");
        let remote = summary
            .remote_state
            .iter()
            .find(|row| row.inode_id == InodeId(2))
            .expect("file remote row");
        assert!(remote.is_deleted);
    }

    #[test]
    fn authoritative_import_translation_failure_leaves_client_db_unchanged() {
        let temp_dir = unique_temp_dir("ops-import-translation-failure");
        let store = LocalFsStore::new(&temp_dir).expect("create store");
        let namespace_id = NamespaceId::from("demo");
        let broken = metadata_with_missing_manifest(&namespace_id, ChangeSeq(1), "hello.txt");
        seed_authoritative_snapshot(&store, &namespace_id, ChangeSeq(1), &broken);

        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client DB");
        let before = db
            .load_namespace_state_summary(&namespace_id)
            .expect("load empty client state");

        let error = import_authoritative_remote_observations(
            &mut db,
            &store,
            &namespace_id,
            1_700_000_800_500,
        )
        .expect_err("missing manifest should fail translation");

        assert!(matches!(
            error,
            AuthoritativeObservationImportError::Translate(_)
        ));
        assert_eq!(
            db.load_namespace_state_summary(&namespace_id)
                .expect("load client state after failed import"),
            before
        );
    }

    #[test]
    fn authoritative_import_apply_failure_leaves_client_db_unchanged() {
        let temp_dir = unique_temp_dir("ops-import-apply-failure");
        let namespace_id = NamespaceId::from("demo");
        let mut db = SqliteStateDb::open(temp_dir.join("client.sqlite3")).expect("open client DB");
        let before = db
            .load_namespace_state_summary(&namespace_id)
            .expect("load empty client state");

        let error = apply_translated_authoritative_remote_observations(
            &mut db,
            &namespace_id,
            ChangeSeq(2),
            &[
                ObservedRemoteInode {
                    namespace_id: namespace_id.clone(),
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    observed_seq: ChangeSeq(2),
                    revision_no: RevisionNo(1),
                    content_digest: None,
                    content_manifest_digest: None,
                    parent_inode_id: None,
                    display_name: String::new(),
                    is_deleted: false,
                },
                ObservedRemoteInode {
                    namespace_id: NamespaceId::from("other"),
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    observed_seq: ChangeSeq(2),
                    revision_no: RevisionNo(1),
                    content_digest: Some("sha256:other".to_owned()),
                    content_manifest_digest: Some("sha256:manifest-other".to_owned()),
                    parent_inode_id: Some(InodeId(1)),
                    display_name: "other.txt".to_owned(),
                    is_deleted: false,
                },
            ],
            1_700_000_800_600,
        )
        .expect_err("mixed namespace observations should fail apply");

        assert!(matches!(
            error,
            loon_client::state_db::StateDbError::RemoteObservationBatchNamespaceMismatch { .. }
        ));
        assert_eq!(
            db.load_namespace_state_summary(&namespace_id)
                .expect("load client state after failed apply"),
            before
        );
    }

    #[derive(Debug, Clone)]
    struct SeededSnapshot {
        metadata: MetadataState,
        file_digest: String,
        manifest_digest: String,
    }

    fn metadata_with_single_file(
        namespace_id: &NamespaceId,
        seq: ChangeSeq,
        display_name: &str,
        contents: &[u8],
    ) -> SeededSnapshot {
        let content = uploaded_content(namespace_id, contents);
        SeededSnapshot {
            metadata: MetadataState {
                inodes: vec![
                    InodeRecord {
                        inode_id: InodeId(1),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(0),
                    },
                    InodeRecord {
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::File,
                        created_seq: ChangeSeq(1),
                    },
                ],
                direntries: vec![DirentryRecord {
                    parent_inode_id: InodeId(1),
                    name_key: display_name.to_owned(),
                    display_name: display_name.to_owned(),
                    child_inode_id: InodeId(2),
                    bind_seq: seq,
                    bind_op_index: 0,
                }],
                revisions: vec![RevisionRecord {
                    inode_id: InodeId(2),
                    revision_no: RevisionNo(seq.0.max(1)),
                    committed_seq: seq,
                    revision_op_index: 0,
                    content_manifest_digest: content.manifest_digest.clone(),
                }],
                subtree_tombstones: Vec::new(),
            },
            file_digest: content.file_digest,
            manifest_digest: content.manifest_digest,
        }
    }

    fn metadata_with_deleted_file(
        _namespace_id: &NamespaceId,
        seq: ChangeSeq,
        manifest_digest: &str,
    ) -> SeededSnapshot {
        SeededSnapshot {
            metadata: MetadataState {
                inodes: vec![
                    InodeRecord {
                        inode_id: InodeId(1),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(0),
                    },
                    InodeRecord {
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::File,
                        created_seq: ChangeSeq(1),
                    },
                ],
                direntries: Vec::new(),
                revisions: vec![RevisionRecord {
                    inode_id: InodeId(2),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(1),
                    revision_op_index: 0,
                    content_manifest_digest: manifest_digest.to_owned(),
                }],
                subtree_tombstones: vec![SubtreeTombstoneRecord {
                    root_inode_id: InodeId(2),
                    tombstone_seq: seq,
                    tombstone_op_index: 0,
                }],
            },
            file_digest: String::new(),
            manifest_digest: manifest_digest.to_owned(),
        }
    }

    fn metadata_with_missing_manifest(
        _namespace_id: &NamespaceId,
        seq: ChangeSeq,
        display_name: &str,
    ) -> MetadataState {
        MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
            ],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(1),
                name_key: display_name.to_owned(),
                display_name: display_name.to_owned(),
                child_inode_id: InodeId(2),
                bind_seq: seq,
                bind_op_index: 0,
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(2),
                revision_no: RevisionNo(1),
                committed_seq: seq,
                revision_op_index: 0,
                content_manifest_digest: "sha256:missing-manifest".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
    }

    fn seed_authoritative_snapshot(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        seq: ChangeSeq,
        metadata_state: &MetadataState,
    ) {
        write_manifests_for_metadata(store, namespace_id, metadata_state);
        let head = HeadState {
            namespace_id: namespace_id.clone(),
            seq,
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(100),
            snapshot_hint_seq: Some(seq),
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(0),
            lease_expires_at_ms: 60_000,
        };
        let checkpoint =
            prepare_checkpoint(&head, metadata_state, "loon-ops-test").expect("prepare checkpoint");
        store
            .put_overwrite(
                &checkpoint.manifest.object_key,
                &checkpoint.manifest.encoded_bytes,
            )
            .expect("write checkpoint manifest");
        for segment in &checkpoint.segments {
            store
                .put_overwrite(&segment.object_key, &segment.encoded_bytes)
                .expect("write checkpoint segment");
        }

        let head_envelope =
            HeadStateEnvelope::from_state(ControlObjectKind::NamespaceHead, "loon-ops-test", head)
                .expect("encode head envelope");
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "loon-ops-test",
            lease,
        )
        .expect("encode lease envelope");
        store
            .put_overwrite(
                &namespace_head(namespace_id.as_str()),
                &serde_json::to_vec(&head_envelope).expect("serialize head envelope"),
            )
            .expect("write head object");
        store
            .put_overwrite(
                &namespace_lease(namespace_id.as_str()),
                &serde_json::to_vec(&lease_envelope).expect("serialize lease envelope"),
            )
            .expect("write lease object");
    }

    fn write_manifests_for_metadata(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        metadata_state: &MetadataState,
    ) {
        let known_contents = [
            uploaded_content(namespace_id, b"v1\n"),
            uploaded_content(namespace_id, b"v2\n"),
        ];
        for revision in &metadata_state.revisions {
            if let Some(content) = known_contents
                .iter()
                .find(|content| content.manifest_digest == revision.content_manifest_digest)
            {
                store
                    .put_overwrite(
                        &content_manifest(namespace_id.as_str(), &content.manifest_digest),
                        &content.manifest_bytes,
                    )
                    .expect("write content manifest");
                for (block_digest, block_bytes) in &content.blocks {
                    store
                        .put_overwrite(&blob(namespace_id.as_str(), block_digest), block_bytes)
                        .expect("write content block");
                }
            }
        }
    }

    fn seed_bound_client_state(
        db: &mut SqliteStateDb,
        namespace_id: &NamespaceId,
        snapshot: &SeededSnapshot,
        observed_seq: ChangeSeq,
        display_name: &str,
    ) {
        db.planner_transaction("seed-bound-client-state", |tx| {
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                observed_seq,
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: String::new(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                content_digest: None,
                parent_inode_id: None,
                display_name: String::new(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_800_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                synced_seq: observed_seq,
                revision_no: RevisionNo(1),
                content_digest: None,
                content_manifest_digest: None,
                parent_inode_id: None,
                display_name: String::new(),
            })?;
            tx.upsert_remote_file(&RemoteFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                observed_seq,
                revision_no: RevisionNo(observed_seq.0.max(1)),
                content_digest: Some(snapshot.file_digest.clone()),
                content_manifest_digest: Some(snapshot.manifest_digest.clone()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
                is_deleted: false,
            })?;
            tx.upsert_local_file(&LocalFileStateRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                content_digest: Some(snapshot.file_digest.clone()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
                exists_on_disk: true,
                dirty: false,
                last_local_change_ms: 1_700_000_800_000,
            })?;
            tx.upsert_sync_anchor(&SyncAnchorRow {
                namespace_id: namespace_id.clone(),
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                synced_seq: observed_seq,
                revision_no: RevisionNo(observed_seq.0.max(1)),
                content_digest: Some(snapshot.file_digest.clone()),
                content_manifest_digest: Some(snapshot.manifest_digest.clone()),
                parent_inode_id: Some(InodeId(1)),
                display_name: display_name.to_owned(),
            })?;
            Ok(())
        })
        .expect("seed bound client state");
    }

    struct UploadedContent {
        file_digest: String,
        manifest_digest: String,
        manifest_bytes: Vec<u8>,
        blocks: Vec<(String, Vec<u8>)>,
    }

    fn uploaded_content(namespace_id: &NamespaceId, bytes: &[u8]) -> UploadedContent {
        let file_digest = sha256_digest(bytes);
        let block_digest = sha256_digest(bytes);
        let manifest = ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: namespace_id.clone(),
            file_size_bytes: bytes.len() as u64,
            file_digest_sha256: file_digest.clone(),
            block_size_bytes: bytes.len() as u64,
            blocks: vec![ContentBlockDescriptor {
                content_digest_sha256: block_digest.clone(),
                plaintext_size_bytes: bytes.len() as u64,
            }],
        })
        .expect("build content manifest");
        let manifest_bytes = serde_json::to_vec(&manifest).expect("serialize content manifest");
        let manifest_digest = sha256_digest(&manifest_bytes);
        UploadedContent {
            file_digest,
            manifest_digest,
            manifest_bytes,
            blocks: vec![(block_digest, bytes.to_vec())],
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("loondb-ops-import-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
