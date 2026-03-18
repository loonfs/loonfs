use loon_core::checkpoint::{StoredCheckpointManifest, StoredCheckpointSegment};
use loon_core::commit::CommitRequest;
use loon_core::metadata::{
    DirentryRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
};
use loon_core::wal::PreparedWalCommit;
use loon_core::wal::StoredWalObject;
use loon_objectstore::keys::{snapshot_manifest, wal_commit};
use loon_types::{
    checkpoint_page_checksum_sha256, decode_checkpoint_manifest_json,
    decode_checkpoint_segment_envelope_zstd, decode_wal_commit_envelope_zstd, ChangeSeq,
    CheckpointRow, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope, CheckpointTableFamily,
    HeadState, InodeId, InodeKind, LeaseState, RevisionNo, WalCommitEnvelope, WalOp,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NamespaceCoreInvariantReport {
    pub checks: Vec<InvariantCheck>,
}

impl NamespaceCoreInvariantReport {
    pub fn check(&self, name: &str) -> Option<&InvariantCheck> {
        self.checks.iter().find(|check| check.name == name)
    }

    pub fn passed_names(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| check.passed)
            .map(|check| check.name.clone())
            .collect()
    }

    pub fn render_trace_lines(&self, label: &str) -> Vec<String> {
        self.checks
            .iter()
            .map(|check| {
                format!(
                    "invariants[{label}] {}={} detail={}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CommitInvariantInputs<'a> {
    pub request: &'a CommitRequest,
    pub before_head: &'a HeadState,
    pub before_lease: &'a LeaseState,
    pub before_metadata: &'a MetadataState,
    pub prepared_wal: &'a PreparedWalCommit,
    pub after_head: &'a HeadState,
    pub after_metadata: &'a MetadataState,
    pub now_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct WalReplayInvariantInputs<'a> {
    pub expected_namespace: &'a str,
    pub basis_head: &'a HeadState,
    pub basis_metadata: &'a MetadataState,
    pub wal_objects: &'a [StoredWalObject],
    pub after_head: &'a HeadState,
    pub after_metadata: &'a MetadataState,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckpointReplayInvariantInputs<'a> {
    pub expected_namespace: &'a str,
    pub stored_manifest: &'a StoredCheckpointManifest,
    pub stored_segments: &'a [StoredCheckpointSegment],
    pub basis_head: &'a HeadState,
    pub basis_metadata: &'a MetadataState,
    pub wal_objects: &'a [StoredWalObject],
    pub after_head: &'a HeadState,
    pub after_metadata: &'a MetadataState,
}

pub fn evaluate_namespace_commit_invariants(
    inputs: CommitInvariantInputs<'_>,
) -> NamespaceCoreInvariantReport {
    let mut checks = Vec::new();
    let wal_payload = &inputs.prepared_wal.envelope.payload;
    let committed_seq = wal_payload.seq;

    checks.push(InvariantCheck {
        name: "stale_writer_cannot_publish".to_owned(),
        passed: inputs.request.writer_fence_token == inputs.before_head.active_fence_token
            && inputs.request.writer_fence_token == inputs.before_lease.fence_token
            && inputs.request.writer_id == inputs.before_lease.holder_id
            && inputs.before_lease.is_valid_at(inputs.now_ms),
        detail: format!(
            "request_fence={} head_fence={} lease_fence={} holder={} lease_valid_at_now={}",
            inputs.request.writer_fence_token.0,
            inputs.before_head.active_fence_token.0,
            inputs.before_lease.fence_token.0,
            inputs.before_lease.holder_id,
            inputs.before_lease.is_valid_at(inputs.now_ms)
        ),
    });
    checks.push(InvariantCheck {
        name: "head_and_lease_fence_tokens_agree".to_owned(),
        passed: inputs.before_head.namespace_id == inputs.before_lease.namespace_id
            && inputs.before_head.active_fence_token == inputs.before_lease.fence_token,
        detail: format!(
            "head_ns={} lease_ns={} head_fence={} lease_fence={}",
            inputs.before_head.namespace_id,
            inputs.before_lease.namespace_id,
            inputs.before_head.active_fence_token.0,
            inputs.before_lease.fence_token.0
        ),
    });
    checks.push(InvariantCheck {
        name: "next_inode_id_is_monotonic".to_owned(),
        passed: inputs.after_head.next_inode_id.0 >= inputs.before_head.next_inode_id.0,
        detail: format!(
            "before_next_inode={} after_next_inode={}",
            inputs.before_head.next_inode_id.0, inputs.after_head.next_inode_id.0
        ),
    });
    checks.push(check_create_mutation_consumes_next_inode_id(
        inputs.before_head.next_inode_id,
        &wal_payload.ops,
        inputs.after_head.next_inode_id,
    ));
    checks.push(check_requires_durable_content(
        "create_file_requires_durable_content",
        &wal_payload.ops,
        inputs.after_metadata,
        |op| match op {
            WalOp::CreateFile {
                op_index,
                inode_id,
                content_manifest_digest,
                ..
            } => Some(inputs.after_metadata.revisions.iter().any(|revision| {
                revision.inode_id == *inode_id
                    && revision.revision_no == RevisionNo(1)
                    && revision.committed_seq == committed_seq
                    && revision.revision_op_index == *op_index
                    && revision.content_manifest_digest == *content_manifest_digest
            })),
            _ => None,
        },
    ));
    checks.push(check_requires_durable_content(
        "replace_file_requires_durable_content",
        &wal_payload.ops,
        inputs.after_metadata,
        |op| match op {
            WalOp::ReplaceFile {
                op_index,
                inode_id,
                base_revision,
                content_manifest_digest,
            } => Some(inputs.after_metadata.revisions.iter().any(|revision| {
                revision.inode_id == *inode_id
                    && revision.revision_no == RevisionNo(base_revision.0 + 1)
                    && revision.committed_seq == committed_seq
                    && revision.revision_op_index == *op_index
                    && revision.content_manifest_digest == *content_manifest_digest
            })),
            _ => None,
        },
    ));
    checks.push(check_subtree_tombstone_blocks_descendant_mutation(
        inputs.before_metadata,
        committed_seq,
        &wal_payload.ops,
    ));
    checks.extend(evaluate_metadata_apply_invariants(
        inputs.before_metadata,
        &sequenced_ops_for_commit(committed_seq, &wal_payload.ops),
        inputs.after_metadata,
    ));

    NamespaceCoreInvariantReport { checks }
}

pub fn evaluate_namespace_wal_replay_invariants(
    inputs: WalReplayInvariantInputs<'_>,
) -> NamespaceCoreInvariantReport {
    let decoded = decode_wal_tail(inputs.wal_objects);
    let sequenced_ops = match decoded.as_ref() {
        Ok(decoded) => flatten_wal_ops(decoded),
        Err(_) => Vec::new(),
    };
    let metadata_apply_checks = match decoded.as_ref() {
        Ok(_) => evaluate_metadata_apply_invariants(
            inputs.basis_metadata,
            &sequenced_ops,
            inputs.after_metadata,
        ),
        Err(error) => metadata_apply_checks_decode_failure(error),
    };

    let mut checks = vec![
        wal_payload_checksum_check(&decoded),
        wal_key_matches_committed_seq_check(&decoded),
        head_publish_requires_durable_wal_check(
            inputs.basis_head,
            inputs.after_head,
            decoded.as_ref().ok().map(Vec::as_slice),
        ),
        wal_replay_requires_matching_namespace_check(inputs.expected_namespace, &decoded),
        wal_replay_requires_matching_base_head_seq_check(inputs.basis_head, &decoded),
        wal_tail_seq_is_contiguous_check(inputs.basis_head, &decoded),
        wal_replay_applies_metadata_rows_check(
            inputs.basis_head,
            inputs.basis_metadata,
            inputs.after_head,
            inputs.after_metadata,
            decoded.as_ref().ok().map(Vec::as_slice),
        ),
    ];
    checks.extend(metadata_apply_checks);

    NamespaceCoreInvariantReport { checks }
}

pub fn evaluate_namespace_checkpoint_replay_invariants(
    inputs: CheckpointReplayInvariantInputs<'_>,
) -> NamespaceCoreInvariantReport {
    let manifest = decode_checkpoint_manifest_json(&inputs.stored_manifest.encoded_bytes)
        .map_err(|err| err.to_string());
    let decoded_segments = decode_checkpoint_segments(inputs.stored_segments);
    let reconstructed_basis =
        reconstruct_checkpoint_metadata(inputs.stored_segments, manifest.as_ref().ok());
    let replay_report = evaluate_namespace_wal_replay_invariants(WalReplayInvariantInputs {
        expected_namespace: inputs.expected_namespace,
        basis_head: inputs.basis_head,
        basis_metadata: inputs.basis_metadata,
        wal_objects: inputs.wal_objects,
        after_head: inputs.after_head,
        after_metadata: inputs.after_metadata,
    });

    let mut checks = vec![
        InvariantCheck {
            name: "checkpoint_manifest_checksum_matches_payload".to_owned(),
            passed: manifest.is_ok(),
            detail: match &manifest {
                Ok(decoded) => format!(
                    "manifest_checksum_verified checkpoint_seq={}",
                    decoded.payload.checkpoint_seq.0
                ),
                Err(error) => format!("manifest_decode_failed error={error}"),
            },
        },
        checkpoint_manifest_key_matches_seq_check(inputs.stored_manifest, manifest.as_ref().ok()),
        checkpoint_manifest_must_be_verified_check(manifest.as_ref().ok()),
        checkpoint_replay_requires_all_manifest_segments_check(
            inputs.stored_segments,
            manifest.as_ref().ok(),
        ),
        checkpoint_segment_descriptor_matches_payload_check(
            inputs.stored_segments,
            manifest.as_ref().ok(),
            decoded_segments.as_ref().ok().map(Vec::as_slice),
        ),
        checkpoint_segment_rows_restore_basis_metadata_check(
            reconstructed_basis.as_ref().ok(),
            inputs.basis_metadata,
        ),
        checkpoint_plus_wal_tail_reproduces_head_check(
            inputs.basis_head,
            inputs.after_head,
            replay_report.check("head_publish_requires_durable_wal"),
            replay_report.check("wal_tail_seq_is_contiguous"),
            inputs.wal_objects,
        ),
        checkpoint_plus_wal_tail_reproduces_metadata_check(
            replay_report.check("wal_replay_applies_metadata_rows"),
            inputs.after_metadata,
        ),
    ];
    checks.extend(replay_report.checks);

    NamespaceCoreInvariantReport { checks }
}

fn metadata_apply_checks_decode_failure(error: &str) -> Vec<InvariantCheck> {
    [
        "create_dir_writes_inode_and_direntry_rows",
        "create_file_writes_inode_direntry_and_initial_revision",
        "replace_file_appends_new_revision_head",
        "rename_appends_new_direntry_binding",
        "delete_subtree_writes_tombstone_row",
        "restore_creates_new_revision_head",
    ]
    .into_iter()
    .map(|name| InvariantCheck {
        name: name.to_owned(),
        passed: false,
        detail: format!("wal_decode_failed error={error}"),
    })
    .collect()
}

fn sequenced_ops_for_commit<'a>(seq: ChangeSeq, ops: &'a [WalOp]) -> Vec<SequencedWalOp<'a>> {
    ops.iter().map(|op| SequencedWalOp { seq, op }).collect()
}

fn flatten_wal_ops<'a>(decoded: &'a [DecodedWalObject]) -> Vec<SequencedWalOp<'a>> {
    let mut out = Vec::new();
    for object in decoded {
        out.extend(object.envelope.payload.ops.iter().map(|op| SequencedWalOp {
            seq: object.envelope.payload.seq,
            op,
        }));
    }
    out
}

fn evaluate_metadata_apply_invariants(
    before_metadata: &MetadataState,
    ops: &[SequencedWalOp<'_>],
    after_metadata: &MetadataState,
) -> Vec<InvariantCheck> {
    let mut states = BTreeMap::<&'static str, AggregateCheck>::from([
        (
            "create_dir_writes_inode_and_direntry_rows",
            AggregateCheck::not_applicable("no create_dir wal op"),
        ),
        (
            "create_file_writes_inode_direntry_and_initial_revision",
            AggregateCheck::not_applicable("no create_file wal op"),
        ),
        (
            "replace_file_appends_new_revision_head",
            AggregateCheck::not_applicable("no replace_file wal op"),
        ),
        (
            "rename_appends_new_direntry_binding",
            AggregateCheck::not_applicable("no rename wal op"),
        ),
        (
            "delete_subtree_writes_tombstone_row",
            AggregateCheck::not_applicable("no delete_subtree wal op"),
        ),
        (
            "restore_creates_new_revision_head",
            AggregateCheck::not_applicable("no restore_revision wal op"),
        ),
    ]);

    let mut metadata_state = before_metadata.clone();
    for sequenced in ops {
        match sequenced.op {
            WalOp::CreateDir {
                op_index,
                inode_id,
                parent_inode,
                display_name,
            } => {
                let passed = after_metadata.inodes.iter().any(|inode| {
                    inode.inode_id == *inode_id
                        && inode.inode_kind == InodeKind::Dir
                        && inode.created_seq == sequenced.seq
                }) && after_metadata.direntries.iter().any(|direntry| {
                    direntry.parent_inode_id == *parent_inode
                        && direntry.name_key == *display_name
                        && direntry.display_name == *display_name
                        && direntry.child_inode_id == *inode_id
                        && direntry.bind_seq == sequenced.seq
                        && direntry.bind_op_index == *op_index
                });
                states
                    .get_mut("create_dir_writes_inode_and_direntry_rows")
                    .expect("create_dir invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} parent={} name={}",
                            sequenced.seq.0, op_index, inode_id.0, parent_inode.0, display_name
                        ),
                    );
            }
            WalOp::CreateFile {
                op_index,
                inode_id,
                parent_inode,
                display_name,
                content_manifest_digest,
            } => {
                let passed = after_metadata.inodes.iter().any(|inode| {
                    inode.inode_id == *inode_id
                        && inode.inode_kind == InodeKind::File
                        && inode.created_seq == sequenced.seq
                }) && after_metadata.direntries.iter().any(|direntry| {
                    direntry.parent_inode_id == *parent_inode
                        && direntry.name_key == *display_name
                        && direntry.display_name == *display_name
                        && direntry.child_inode_id == *inode_id
                        && direntry.bind_seq == sequenced.seq
                        && direntry.bind_op_index == *op_index
                }) && after_metadata.revisions.iter().any(|revision| {
                    revision.inode_id == *inode_id
                        && revision.revision_no == RevisionNo(1)
                        && revision.committed_seq == sequenced.seq
                        && revision.revision_op_index == *op_index
                        && revision.content_manifest_digest == *content_manifest_digest
                });
                states
                    .get_mut("create_file_writes_inode_direntry_and_initial_revision")
                    .expect("create_file invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} parent={} name={} digest={}",
                            sequenced.seq.0,
                            op_index,
                            inode_id.0,
                            parent_inode.0,
                            display_name,
                            content_manifest_digest
                        ),
                    );
            }
            WalOp::ReplaceFile {
                op_index,
                inode_id,
                base_revision,
                content_manifest_digest,
            } => {
                let next_revision = RevisionNo(base_revision.0.saturating_add(1));
                let passed = after_metadata.revisions.iter().any(|revision| {
                    revision.inode_id == *inode_id
                        && revision.revision_no == next_revision
                        && revision.committed_seq == sequenced.seq
                        && revision.revision_op_index == *op_index
                        && revision.content_manifest_digest == *content_manifest_digest
                });
                states
                    .get_mut("replace_file_appends_new_revision_head")
                    .expect("replace_file invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} base_revision={} next_revision={} digest={}",
                            sequenced.seq.0,
                            op_index,
                            inode_id.0,
                            base_revision.0,
                            next_revision.0,
                            content_manifest_digest
                        ),
                    );
            }
            WalOp::Rename {
                op_index,
                inode_id,
                new_parent_inode,
                new_display_name,
            } => {
                let passed = after_metadata.direntries.iter().any(|direntry| {
                    direntry.parent_inode_id == *new_parent_inode
                        && direntry.name_key == *new_display_name
                        && direntry.display_name == *new_display_name
                        && direntry.child_inode_id == *inode_id
                        && direntry.bind_seq == sequenced.seq
                        && direntry.bind_op_index == *op_index
                });
                states
                    .get_mut("rename_appends_new_direntry_binding")
                    .expect("rename invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} new_parent={} new_name={}",
                            sequenced.seq.0,
                            op_index,
                            inode_id.0,
                            new_parent_inode.0,
                            new_display_name
                        ),
                    );
            }
            WalOp::DeleteSubtree {
                op_index,
                root_inode,
            } => {
                let passed = after_metadata.subtree_tombstones.iter().any(|tombstone| {
                    tombstone.root_inode_id == *root_inode
                        && tombstone.tombstone_seq == sequenced.seq
                        && tombstone.tombstone_op_index == *op_index
                });
                states
                    .get_mut("delete_subtree_writes_tombstone_row")
                    .expect("delete invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} root_inode={}",
                            sequenced.seq.0, op_index, root_inode.0
                        ),
                    );
            }
            WalOp::RestoreRevision {
                op_index,
                inode_id,
                base_revision,
                restore_from_revision,
            } => {
                let next_revision = RevisionNo(base_revision.0.saturating_add(1));
                let source_digest = metadata_state
                    .revision_at_seq(*inode_id, *restore_from_revision, sequenced.seq)
                    .map(|revision| revision.content_manifest_digest);
                let passed = source_digest.as_ref().is_some_and(|digest| {
                    after_metadata.revisions.iter().any(|revision| {
                        revision.inode_id == *inode_id
                            && revision.revision_no == next_revision
                            && revision.committed_seq == sequenced.seq
                            && revision.revision_op_index == *op_index
                            && revision.content_manifest_digest == *digest
                    })
                });
                states
                    .get_mut("restore_creates_new_revision_head")
                    .expect("restore invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} base_revision={} restore_from={} source_found={}",
                            sequenced.seq.0,
                            op_index,
                            inode_id.0,
                            base_revision.0,
                            restore_from_revision.0,
                            source_digest.is_some()
                        ),
                    );
            }
        }

        if let Ok(applied) = metadata_state
            .apply_committed_wal_ops(sequenced.seq, std::slice::from_ref(sequenced.op))
        {
            metadata_state = applied.metadata_state;
        }
    }

    states
        .into_iter()
        .map(|(name, aggregate)| aggregate.finish(name))
        .collect()
}

fn check_create_mutation_consumes_next_inode_id(
    initial_next_inode_id: InodeId,
    ops: &[WalOp],
    resulting_next_inode_id: InodeId,
) -> InvariantCheck {
    let create_ids = ops
        .iter()
        .filter_map(|op| match op {
            WalOp::CreateDir { inode_id, .. } | WalOp::CreateFile { inode_id, .. } => {
                Some(*inode_id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    if create_ids.is_empty() {
        return InvariantCheck {
            name: "create_mutation_consumes_next_inode_id".to_owned(),
            passed: false,
            detail: "no create ops".to_owned(),
        };
    }

    let contiguous = create_ids.iter().enumerate().all(|(offset, inode_id)| {
        initial_next_inode_id
            .0
            .checked_add(offset as u64)
            .is_some_and(|expected| expected == inode_id.0)
    });
    let expected_next = initial_next_inode_id
        .0
        .checked_add(create_ids.len() as u64)
        .unwrap_or(u64::MAX);

    InvariantCheck {
        name: "create_mutation_consumes_next_inode_id".to_owned(),
        passed: contiguous && resulting_next_inode_id.0 == expected_next,
        detail: format!(
            "initial_next_inode={} allocated={:?} resulting_next_inode={} expected_next_inode={}",
            initial_next_inode_id.0,
            create_ids.iter().map(|inode| inode.0).collect::<Vec<_>>(),
            resulting_next_inode_id.0,
            expected_next
        ),
    }
}

fn check_requires_durable_content<F>(
    name: &'static str,
    ops: &[WalOp],
    _after_metadata: &MetadataState,
    mut evaluator: F,
) -> InvariantCheck
where
    F: FnMut(&WalOp) -> Option<bool>,
{
    let mut applicable = false;
    let mut passed = true;
    let mut details = Vec::new();

    for op in ops {
        if let Some(op_passed) = evaluator(op) {
            applicable = true;
            passed &= op_passed;
            details.push(format!("op={op:?} passed={op_passed}"));
        }
    }

    InvariantCheck {
        name: name.to_owned(),
        passed: applicable && passed,
        detail: if applicable {
            details.join("; ")
        } else {
            "no matching op".to_owned()
        },
    }
}

fn check_subtree_tombstone_blocks_descendant_mutation(
    before_metadata: &MetadataState,
    committed_seq: ChangeSeq,
    ops: &[WalOp],
) -> InvariantCheck {
    let mut metadata_state = before_metadata.clone();
    let mut applicable = false;
    let mut passed = true;
    let mut details = Vec::new();

    for op in ops {
        let result = match op {
            WalOp::CreateDir { parent_inode, .. } | WalOp::CreateFile { parent_inode, .. } => {
                applicable = true;
                let covering =
                    metadata_state.covering_subtree_tombstone(*parent_inode, committed_seq);
                (
                    covering.is_none(),
                    format!(
                        "op={op:?} parent_inode={} covering_root={:?}",
                        parent_inode.0,
                        covering.as_ref().map(|record| record.root_inode_id.0)
                    ),
                )
            }
            WalOp::ReplaceFile { inode_id, .. } | WalOp::RestoreRevision { inode_id, .. } => {
                applicable = true;
                let covering = metadata_state.covering_subtree_tombstone(*inode_id, committed_seq);
                (
                    covering.is_none(),
                    format!(
                        "op={op:?} inode={} covering_root={:?}",
                        inode_id.0,
                        covering.as_ref().map(|record| record.root_inode_id.0)
                    ),
                )
            }
            WalOp::Rename {
                inode_id,
                new_parent_inode,
                ..
            } => {
                applicable = true;
                let inode_covering =
                    metadata_state.covering_subtree_tombstone(*inode_id, committed_seq);
                let parent_covering =
                    metadata_state.covering_subtree_tombstone(*new_parent_inode, committed_seq);
                (
                    inode_covering.is_none() && parent_covering.is_none(),
                    format!(
                        "op={op:?} inode_covering={:?} parent_covering={:?}",
                        inode_covering.as_ref().map(|record| record.root_inode_id.0),
                        parent_covering
                            .as_ref()
                            .map(|record| record.root_inode_id.0)
                    ),
                )
            }
            WalOp::DeleteSubtree { root_inode, .. } => {
                applicable = true;
                let covering =
                    metadata_state.covering_subtree_tombstone(*root_inode, committed_seq);
                (
                    covering.is_none(),
                    format!(
                        "op={op:?} root_inode={} covering_root={:?}",
                        root_inode.0,
                        covering.as_ref().map(|record| record.root_inode_id.0)
                    ),
                )
            }
        };
        passed &= result.0;
        details.push(result.1);

        if let Ok(applied) =
            metadata_state.apply_committed_wal_ops(committed_seq, std::slice::from_ref(op))
        {
            metadata_state = applied.metadata_state;
        }
    }

    InvariantCheck {
        name: "subtree_tombstone_blocks_descendant_mutation".to_owned(),
        passed: applicable && passed,
        detail: if applicable {
            details.join("; ")
        } else {
            "no mutation with ancestor/tombstone coverage rules".to_owned()
        },
    }
}

fn wal_payload_checksum_check(decoded: &Result<Vec<DecodedWalObject>, String>) -> InvariantCheck {
    match decoded {
        Ok(objects) => InvariantCheck {
            name: "wal_payload_checksum_matches_payload".to_owned(),
            passed: true,
            detail: format!("decoded_wal_objects={}", objects.len()),
        },
        Err(error) => InvariantCheck {
            name: "wal_payload_checksum_matches_payload".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn wal_key_matches_committed_seq_check(
    decoded: &Result<Vec<DecodedWalObject>, String>,
) -> InvariantCheck {
    match decoded {
        Ok(objects) => {
            let mismatches = objects
                .iter()
                .filter_map(|object| {
                    let expected = wal_commit(
                        object.envelope.payload.namespace_id.as_str(),
                        object.envelope.payload.seq.0,
                        &object.envelope.payload.commit_id,
                    );
                    (expected != object.object_key)
                        .then(|| format!("expected={expected} actual={}", object.object_key))
                })
                .collect::<Vec<_>>();
            InvariantCheck {
                name: "wal_key_matches_committed_seq".to_owned(),
                passed: mismatches.is_empty(),
                detail: if mismatches.is_empty() {
                    format!("validated_object_keys={}", objects.len())
                } else {
                    mismatches.join("; ")
                },
            }
        }
        Err(error) => InvariantCheck {
            name: "wal_key_matches_committed_seq".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn head_publish_requires_durable_wal_check(
    basis_head: &HeadState,
    after_head: &HeadState,
    decoded: Option<&[DecodedWalObject]>,
) -> InvariantCheck {
    let advanced = after_head.seq > basis_head.seq;
    let covered = match decoded {
        Some(objects) if !objects.is_empty() => objects
            .last()
            .map(|object| object.envelope.payload.seq == after_head.seq)
            .unwrap_or(false),
        Some(_) => after_head.seq == basis_head.seq,
        None => false,
    };

    InvariantCheck {
        name: "head_publish_requires_durable_wal".to_owned(),
        passed: (!advanced && decoded.is_some()) || (advanced && covered),
        detail: format!(
            "basis_seq={} after_seq={} wal_tail_len={} covered={covered}",
            basis_head.seq.0,
            after_head.seq.0,
            decoded.map_or(0, |objects| objects.len())
        ),
    }
}

fn wal_replay_requires_matching_namespace_check(
    expected_namespace: &str,
    decoded: &Result<Vec<DecodedWalObject>, String>,
) -> InvariantCheck {
    match decoded {
        Ok(objects) => {
            let mismatches = objects
                .iter()
                .filter(|object| {
                    object.envelope.payload.namespace_id.as_str() != expected_namespace
                })
                .map(|object| object.envelope.payload.namespace_id.to_string())
                .collect::<Vec<_>>();
            InvariantCheck {
                name: "wal_replay_requires_matching_namespace".to_owned(),
                passed: mismatches.is_empty(),
                detail: if mismatches.is_empty() {
                    format!("expected_namespace={expected_namespace}")
                } else {
                    format!("unexpected_namespaces={mismatches:?}")
                },
            }
        }
        Err(error) => InvariantCheck {
            name: "wal_replay_requires_matching_namespace".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn wal_replay_requires_matching_base_head_seq_check(
    basis_head: &HeadState,
    decoded: &Result<Vec<DecodedWalObject>, String>,
) -> InvariantCheck {
    match decoded {
        Ok(objects) => {
            let mut current_seq = basis_head.seq;
            let mut mismatches = Vec::new();
            for object in objects {
                if object.envelope.payload.base_head_seq != current_seq {
                    mismatches.push(format!(
                        "seq={} expected_base={} actual_base={}",
                        object.envelope.payload.seq.0,
                        current_seq.0,
                        object.envelope.payload.base_head_seq.0
                    ));
                }
                current_seq = object.envelope.payload.seq;
            }
            InvariantCheck {
                name: "wal_replay_requires_matching_base_head_seq".to_owned(),
                passed: mismatches.is_empty(),
                detail: if mismatches.is_empty() {
                    format!(
                        "basis_seq={} wal_objects={}",
                        basis_head.seq.0,
                        objects.len()
                    )
                } else {
                    mismatches.join("; ")
                },
            }
        }
        Err(error) => InvariantCheck {
            name: "wal_replay_requires_matching_base_head_seq".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn wal_tail_seq_is_contiguous_check(
    basis_head: &HeadState,
    decoded: &Result<Vec<DecodedWalObject>, String>,
) -> InvariantCheck {
    match decoded {
        Ok(objects) => {
            let mut expected_seq = ChangeSeq(basis_head.seq.0.saturating_add(1));
            let mut mismatches = Vec::new();
            for object in objects {
                if object.envelope.payload.seq != expected_seq {
                    mismatches.push(format!(
                        "expected_seq={} actual_seq={}",
                        expected_seq.0, object.envelope.payload.seq.0
                    ));
                }
                expected_seq = ChangeSeq(object.envelope.payload.seq.0.saturating_add(1));
            }
            InvariantCheck {
                name: "wal_tail_seq_is_contiguous".to_owned(),
                passed: mismatches.is_empty(),
                detail: if mismatches.is_empty() {
                    format!(
                        "basis_seq={} wal_objects={}",
                        basis_head.seq.0,
                        objects.len()
                    )
                } else {
                    mismatches.join("; ")
                },
            }
        }
        Err(error) => InvariantCheck {
            name: "wal_tail_seq_is_contiguous".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn wal_replay_applies_metadata_rows_check(
    basis_head: &HeadState,
    basis_metadata: &MetadataState,
    after_head: &HeadState,
    after_metadata: &MetadataState,
    decoded: Option<&[DecodedWalObject]>,
) -> InvariantCheck {
    match decoded {
        Some(objects) => match replay_wal_tail_locally(basis_head, basis_metadata, objects) {
            Ok((replayed_head, replayed_metadata)) => InvariantCheck {
                name: "wal_replay_applies_metadata_rows".to_owned(),
                passed: replayed_head == *after_head && replayed_metadata == *after_metadata,
                detail: format!(
                    "replayed_head_seq={} after_head_seq={} replayed_metadata_matches={}",
                    replayed_head.seq.0,
                    after_head.seq.0,
                    replayed_metadata == *after_metadata
                ),
            },
            Err(error) => InvariantCheck {
                name: "wal_replay_applies_metadata_rows".to_owned(),
                passed: false,
                detail: error,
            },
        },
        None => InvariantCheck {
            name: "wal_replay_applies_metadata_rows".to_owned(),
            passed: false,
            detail: "wal_decode_failed".to_owned(),
        },
    }
}

fn checkpoint_manifest_key_matches_seq_check(
    stored_manifest: &StoredCheckpointManifest,
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
) -> InvariantCheck {
    match manifest {
        Some(manifest) => {
            let expected = snapshot_manifest(
                manifest.payload.namespace_id.as_str(),
                manifest.payload.checkpoint_seq.0,
            );
            InvariantCheck {
                name: "checkpoint_manifest_key_matches_seq".to_owned(),
                passed: stored_manifest.object_key == expected,
                detail: format!(
                    "expected={} actual={}",
                    expected, stored_manifest.object_key
                ),
            }
        }
        None => InvariantCheck {
            name: "checkpoint_manifest_key_matches_seq".to_owned(),
            passed: false,
            detail: "manifest_decode_failed".to_owned(),
        },
    }
}

fn checkpoint_manifest_must_be_verified_check(
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
) -> InvariantCheck {
    match manifest {
        Some(manifest) => InvariantCheck {
            name: "checkpoint_manifest_must_be_verified".to_owned(),
            passed: manifest.payload.verified,
            detail: format!("verified={}", manifest.payload.verified),
        },
        None => InvariantCheck {
            name: "checkpoint_manifest_must_be_verified".to_owned(),
            passed: false,
            detail: "manifest_decode_failed".to_owned(),
        },
    }
}

fn checkpoint_replay_requires_all_manifest_segments_check(
    stored_segments: &[StoredCheckpointSegment],
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
) -> InvariantCheck {
    match manifest {
        Some(manifest) => {
            let expected = manifest
                .payload
                .tables
                .iter()
                .flat_map(|table| {
                    table
                        .segments
                        .iter()
                        .map(|segment| segment.object_key.clone())
                })
                .collect::<BTreeSet<_>>();
            let actual = stored_segments
                .iter()
                .map(|segment| segment.object_key.clone())
                .collect::<BTreeSet<_>>();
            InvariantCheck {
                name: "checkpoint_replay_requires_all_manifest_segments".to_owned(),
                passed: expected == actual,
                detail: format!("expected={expected:?} actual={actual:?}"),
            }
        }
        None => InvariantCheck {
            name: "checkpoint_replay_requires_all_manifest_segments".to_owned(),
            passed: false,
            detail: "manifest_decode_failed".to_owned(),
        },
    }
}

fn checkpoint_segment_descriptor_matches_payload_check(
    stored_segments: &[StoredCheckpointSegment],
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
    decoded_segments: Option<&[DecodedCheckpointSegment]>,
) -> InvariantCheck {
    let Some(manifest) = manifest else {
        return InvariantCheck {
            name: "checkpoint_segment_descriptor_matches_payload".to_owned(),
            passed: false,
            detail: "manifest_decode_failed".to_owned(),
        };
    };
    let Some(decoded_segments) = decoded_segments else {
        return InvariantCheck {
            name: "checkpoint_segment_descriptor_matches_payload".to_owned(),
            passed: false,
            detail: "segment_decode_failed".to_owned(),
        };
    };

    let by_key = decoded_segments
        .iter()
        .map(|segment| (segment.object_key.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    let mut mismatches = Vec::new();

    for table in &manifest.payload.tables {
        for expected in &table.segments {
            let Some(decoded) = by_key.get(expected.object_key.as_str()) else {
                mismatches.push(format!("missing_segment={}", expected.object_key));
                continue;
            };
            let actual = match checkpoint_segment_descriptor_from_payload(
                &decoded.object_key,
                &decoded.envelope,
            ) {
                Ok(actual) => actual,
                Err(error) => {
                    mismatches.push(format!(
                        "descriptor_build_failed key={} error={error}",
                        expected.object_key
                    ));
                    continue;
                }
            };
            if &actual != expected {
                mismatches.push(format!(
                    "descriptor_mismatch key={} expected={expected:?} actual={actual:?}",
                    expected.object_key
                ));
            }
        }
    }

    let unexpected = stored_segments
        .iter()
        .filter(|segment| {
            !manifest
                .payload
                .tables
                .iter()
                .flat_map(|table| table.segments.iter())
                .any(|expected| expected.object_key == segment.object_key)
        })
        .map(|segment| segment.object_key.clone())
        .collect::<Vec<_>>();
    if !unexpected.is_empty() {
        mismatches.push(format!("unexpected_segments={unexpected:?}"));
    }

    InvariantCheck {
        name: "checkpoint_segment_descriptor_matches_payload".to_owned(),
        passed: mismatches.is_empty(),
        detail: if mismatches.is_empty() {
            format!("validated_segments={}", decoded_segments.len())
        } else {
            mismatches.join("; ")
        },
    }
}

fn checkpoint_segment_rows_restore_basis_metadata_check(
    reconstructed_basis: Option<&MetadataState>,
    expected_basis: &MetadataState,
) -> InvariantCheck {
    match reconstructed_basis {
        Some(reconstructed_basis) => InvariantCheck {
            name: "checkpoint_segment_rows_restore_basis_metadata".to_owned(),
            passed: reconstructed_basis == expected_basis,
            detail: format!(
                "reconstructed_matches_expected={}",
                reconstructed_basis == expected_basis
            ),
        },
        None => InvariantCheck {
            name: "checkpoint_segment_rows_restore_basis_metadata".to_owned(),
            passed: false,
            detail: "basis_reconstruction_failed".to_owned(),
        },
    }
}

fn checkpoint_plus_wal_tail_reproduces_head_check(
    basis_head: &HeadState,
    after_head: &HeadState,
    durable_wal: Option<&InvariantCheck>,
    contiguous: Option<&InvariantCheck>,
    wal_objects: &[StoredWalObject],
) -> InvariantCheck {
    let pass = durable_wal.is_some_and(|check| check.passed)
        && contiguous.is_some_and(|check| check.passed)
        && (!wal_objects.is_empty() || basis_head.seq == after_head.seq);
    InvariantCheck {
        name: "checkpoint_plus_wal_tail_reproduces_head".to_owned(),
        passed: pass,
        detail: format!(
            "basis_seq={} after_seq={} wal_objects={} durable_wal_passed={} contiguous_passed={}",
            basis_head.seq.0,
            after_head.seq.0,
            wal_objects.len(),
            durable_wal.is_some_and(|check| check.passed),
            contiguous.is_some_and(|check| check.passed)
        ),
    }
}

fn checkpoint_plus_wal_tail_reproduces_metadata_check(
    wal_replay_applies: Option<&InvariantCheck>,
    after_metadata: &MetadataState,
) -> InvariantCheck {
    InvariantCheck {
        name: "checkpoint_plus_wal_tail_reproduces_metadata".to_owned(),
        passed: wal_replay_applies.is_some_and(|check| check.passed),
        detail: format!(
            "metadata_rows_replayed={} final_row_counts=(inodes={}, direntries={}, revisions={}, tombstones={})",
            wal_replay_applies.is_some_and(|check| check.passed),
            after_metadata.inodes.len(),
            after_metadata.direntries.len(),
            after_metadata.revisions.len(),
            after_metadata.subtree_tombstones.len()
        ),
    }
}

fn decode_wal_tail(wal_objects: &[StoredWalObject]) -> Result<Vec<DecodedWalObject>, String> {
    wal_objects
        .iter()
        .map(|object| {
            decode_wal_commit_envelope_zstd(&object.encoded_bytes)
                .map(|envelope| DecodedWalObject {
                    object_key: object.object_key.clone(),
                    envelope,
                })
                .map_err(|err| err.to_string())
        })
        .collect()
}

fn decode_checkpoint_segments(
    stored_segments: &[StoredCheckpointSegment],
) -> Result<Vec<DecodedCheckpointSegment>, String> {
    stored_segments
        .iter()
        .map(|segment| {
            decode_checkpoint_segment_envelope_zstd(&segment.encoded_bytes)
                .map(|envelope| DecodedCheckpointSegment {
                    object_key: segment.object_key.clone(),
                    envelope,
                })
                .map_err(|err| err.to_string())
        })
        .collect()
}

fn reconstruct_checkpoint_metadata(
    stored_segments: &[StoredCheckpointSegment],
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
) -> Result<MetadataState, String> {
    let Some(manifest) = manifest else {
        return Err("manifest_decode_failed".to_owned());
    };
    let decoded_segments = decode_checkpoint_segments(stored_segments)?;
    let by_key = decoded_segments
        .iter()
        .map(|segment| (segment.object_key.as_str(), &segment.envelope))
        .collect::<BTreeMap<_, _>>();
    let mut metadata = MetadataState::default();

    for table in &manifest.payload.tables {
        for descriptor in &table.segments {
            let envelope = by_key
                .get(descriptor.object_key.as_str())
                .ok_or_else(|| format!("missing_segment={}", descriptor.object_key))?;
            for page in &envelope.payload.pages {
                if page.row_keys.len() != page.rows.len() {
                    return Err(format!(
                        "page_row_key_mismatch key={} page_index={}",
                        descriptor.object_key, page.page_index
                    ));
                }
                let actual_row_keys = page
                    .rows
                    .iter()
                    .map(CheckpointRow::row_key)
                    .collect::<Vec<_>>();
                if page.row_keys != actual_row_keys {
                    return Err(format!(
                        "page_row_key_mismatch key={} page_index={}",
                        descriptor.object_key, page.page_index
                    ));
                }

                for row in &page.rows {
                    match row {
                        CheckpointRow::Inode {
                            inode_id,
                            inode_kind,
                            created_seq,
                        } => {
                            if table.family != CheckpointTableFamily::Inodes {
                                return Err(format!(
                                    "row_family_mismatch key={} expected_family={:?} row={row:?}",
                                    descriptor.object_key, table.family
                                ));
                            }
                            metadata.inodes.push(InodeRecord {
                                inode_id: *inode_id,
                                inode_kind: inode_kind.clone(),
                                created_seq: *created_seq,
                            });
                        }
                        CheckpointRow::Direntry {
                            parent_inode_id,
                            name_key,
                            display_name,
                            child_inode_id,
                            bind_seq,
                            bind_op_index,
                        } => {
                            if table.family != CheckpointTableFamily::Direntries {
                                return Err(format!(
                                    "row_family_mismatch key={} expected_family={:?} row={row:?}",
                                    descriptor.object_key, table.family
                                ));
                            }
                            metadata.direntries.push(DirentryRecord {
                                parent_inode_id: *parent_inode_id,
                                name_key: name_key.clone(),
                                display_name: display_name.clone(),
                                child_inode_id: *child_inode_id,
                                bind_seq: *bind_seq,
                                bind_op_index: *bind_op_index,
                            });
                        }
                        CheckpointRow::Revision {
                            inode_id,
                            revision_no,
                            committed_seq,
                            revision_op_index,
                            content_manifest_digest,
                        } => {
                            if table.family != CheckpointTableFamily::Revisions {
                                return Err(format!(
                                    "row_family_mismatch key={} expected_family={:?} row={row:?}",
                                    descriptor.object_key, table.family
                                ));
                            }
                            metadata.revisions.push(RevisionRecord {
                                inode_id: *inode_id,
                                revision_no: *revision_no,
                                committed_seq: *committed_seq,
                                revision_op_index: *revision_op_index,
                                content_manifest_digest: content_manifest_digest.clone(),
                            });
                        }
                        CheckpointRow::Tombstone {
                            root_inode_id,
                            tombstone_seq,
                            tombstone_op_index,
                        } => {
                            if table.family != CheckpointTableFamily::Tombstones {
                                return Err(format!(
                                    "row_family_mismatch key={} expected_family={:?} row={row:?}",
                                    descriptor.object_key, table.family
                                ));
                            }
                            metadata.subtree_tombstones.push(SubtreeTombstoneRecord {
                                root_inode_id: *root_inode_id,
                                tombstone_seq: *tombstone_seq,
                                tombstone_op_index: *tombstone_op_index,
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(metadata)
}

fn replay_wal_tail_locally(
    basis_head: &HeadState,
    basis_metadata: &MetadataState,
    decoded: &[DecodedWalObject],
) -> Result<(HeadState, MetadataState), String> {
    let mut head = basis_head.clone();
    let mut metadata = basis_metadata.clone();

    for object in decoded {
        if object.envelope.payload.namespace_id != head.namespace_id {
            return Err(format!(
                "namespace_mismatch expected={} actual={}",
                head.namespace_id, object.envelope.payload.namespace_id
            ));
        }
        if object.envelope.payload.base_head_seq != head.seq {
            return Err(format!(
                "base_head_seq_mismatch expected={} actual={}",
                head.seq.0, object.envelope.payload.base_head_seq.0
            ));
        }
        if object.envelope.payload.seq != ChangeSeq(head.seq.0.saturating_add(1)) {
            return Err(format!(
                "non_contiguous_seq expected={} actual={}",
                head.seq.0.saturating_add(1),
                object.envelope.payload.seq.0
            ));
        }
        let applied = metadata
            .apply_committed_wal_ops(object.envelope.payload.seq, &object.envelope.payload.ops)
            .map_err(|err| format!("metadata_apply_failed error={err:?}"))?;
        metadata = applied.metadata_state;
        head = HeadState {
            namespace_id: head.namespace_id.clone(),
            seq: object.envelope.payload.seq,
            active_fence_token: object.envelope.payload.writer_fence_token,
            next_inode_id: next_inode_after_ops(head.next_inode_id, &object.envelope.payload.ops),
            snapshot_hint_seq: head.snapshot_hint_seq,
            retention_floor_seq: head.retention_floor_seq,
        };
    }

    Ok((head, metadata))
}

fn next_inode_after_ops(current: InodeId, ops: &[WalOp]) -> InodeId {
    let create_count = ops
        .iter()
        .filter(|op| matches!(op, WalOp::CreateDir { .. } | WalOp::CreateFile { .. }))
        .count() as u64;
    InodeId(current.0.saturating_add(create_count))
}

fn checkpoint_segment_descriptor_from_payload(
    object_key: &str,
    envelope: &CheckpointSegmentEnvelope,
) -> Result<CheckpointSegmentDescriptor, String> {
    Ok(CheckpointSegmentDescriptor {
        object_key: object_key.to_owned(),
        segment_index: envelope.payload.segment_index,
        row_count: envelope.payload.row_count,
        min_key: envelope.payload.min_key.clone(),
        max_key: envelope.payload.max_key.clone(),
        payload_checksum_sha256: envelope.payload_checksum_sha256.clone(),
        page_checksums_sha256: envelope
            .payload
            .pages
            .iter()
            .map(checkpoint_page_checksum_sha256)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.to_string())?,
    })
}

#[derive(Debug, Clone)]
struct SequencedWalOp<'a> {
    seq: ChangeSeq,
    op: &'a WalOp,
}

#[derive(Debug, Clone)]
struct DecodedWalObject {
    object_key: String,
    envelope: WalCommitEnvelope,
}

#[derive(Debug, Clone)]
struct DecodedCheckpointSegment {
    object_key: String,
    envelope: CheckpointSegmentEnvelope,
}

#[derive(Debug, Clone)]
struct AggregateCheck {
    applicable: bool,
    passed: bool,
    details: Vec<String>,
}

impl AggregateCheck {
    fn not_applicable(detail: &str) -> Self {
        Self {
            applicable: false,
            passed: false,
            details: vec![detail.to_owned()],
        }
    }

    fn record(&mut self, passed: bool, detail: String) {
        if !self.applicable {
            self.passed = true;
        }
        self.applicable = true;
        if self.details.len() == 1 && self.details[0].starts_with("no ") {
            self.details.clear();
        }
        self.passed &= passed;
        self.details.push(detail);
    }

    fn finish(self, name: &str) -> InvariantCheck {
        InvariantCheck {
            name: name.to_owned(),
            passed: self.applicable && self.passed,
            detail: self.details.join("; "),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate_namespace_checkpoint_replay_invariants, evaluate_namespace_commit_invariants,
        evaluate_namespace_wal_replay_invariants, CheckpointReplayInvariantInputs,
        CommitInvariantInputs, WalReplayInvariantInputs,
    };
    use loon_core::checkpoint::{StoredCheckpointManifest, StoredCheckpointSegment};
    use loon_core::commit::{
        build_commit_plan, prepare_commit_head_publish, CommitOp, CommitRequest,
        CommitValidationContext, Precondition,
    };
    use loon_core::metadata::{DirentryRecord, InodeRecord, MetadataState, RevisionRecord};
    use loon_core::wal::{prepare_wal_commit, StoredWalObject};
    use loon_objectstore::keys::{snapshot_manifest, snapshot_table, SnapshotTableFamily};
    use loon_types::{
        checkpoint_page_checksum_sha256, encode_checkpoint_manifest_json,
        encode_checkpoint_segment_envelope_zstd, ChangeSeq, CheckpointManifestEnvelope,
        CheckpointManifestPayload, CheckpointPage, CheckpointRow, CheckpointSegmentDescriptor,
        CheckpointSegmentEnvelope, CheckpointSegmentPayload, CheckpointTableFamily,
        CheckpointTableManifest, FenceToken, HeadState, InodeId, InodeKind, LeaseState,
        NamespaceId, RevisionNo,
    };

    const NOW_MS: u64 = 1_500;
    const TEST_WRITER_VERSION: &str = "loon-testkit-invariants";

    #[test]
    fn commit_invariant_report_marks_create_file_row_write_as_passed() {
        let namespace_id = NamespaceId::new("ns-test");
        let before_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(5),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(7),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let before_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(9),
            lease_expires_at_ms: NOW_MS + 100,
        };
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id: "req-1".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(9),
            planned_head_seq: before_head.seq,
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:abc".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(before_head.seq),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "note.txt".to_owned(),
                },
            ],
        };
        let before_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        };
        let plan = build_commit_plan(
            &request,
            &CommitValidationContext {
                head: before_head.clone(),
                lease: before_lease.clone(),
                now_ms: NOW_MS,
                metadata_state: before_metadata.clone(),
            },
        )
        .expect("plan");
        let prepared_wal = prepare_wal_commit(&request, &plan, TEST_WRITER_VERSION).expect("wal");
        let after_metadata = before_metadata
            .apply_committed_wal_ops(plan.next_seq, &prepared_wal.envelope.payload.ops)
            .expect("apply metadata")
            .metadata_state;
        let after_head = prepare_commit_head_publish(&before_head, &plan, TEST_WRITER_VERSION)
            .expect("head publish")
            .resulting_head;

        let report = evaluate_namespace_commit_invariants(CommitInvariantInputs {
            request: &request,
            before_head: &before_head,
            before_lease: &before_lease,
            before_metadata: &before_metadata,
            prepared_wal: &prepared_wal,
            after_head: &after_head,
            after_metadata: &after_metadata,
            now_ms: NOW_MS,
        });

        assert!(
            report
                .check("create_file_writes_inode_direntry_and_initial_revision")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn commit_invariant_report_marks_create_file_row_write_as_failed_when_revision_missing() {
        let namespace_id = NamespaceId::new("ns-test");
        let before_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(5),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(7),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let before_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(9),
            lease_expires_at_ms: NOW_MS + 100,
        };
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id: "req-1".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(9),
            planned_head_seq: before_head.seq,
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:abc".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(before_head.seq)],
        };
        let before_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        };
        let plan = build_commit_plan(
            &request,
            &CommitValidationContext {
                head: before_head.clone(),
                lease: before_lease.clone(),
                now_ms: NOW_MS,
                metadata_state: before_metadata.clone(),
            },
        )
        .expect("plan");
        let prepared_wal = prepare_wal_commit(&request, &plan, TEST_WRITER_VERSION).expect("wal");
        let after_head = prepare_commit_head_publish(&before_head, &plan, TEST_WRITER_VERSION)
            .expect("head publish")
            .resulting_head;
        let mut after_metadata = before_metadata
            .apply_committed_wal_ops(plan.next_seq, &prepared_wal.envelope.payload.ops)
            .expect("apply metadata")
            .metadata_state;
        after_metadata.revisions.clear();

        let report = evaluate_namespace_commit_invariants(CommitInvariantInputs {
            request: &request,
            before_head: &before_head,
            before_lease: &before_lease,
            before_metadata: &before_metadata,
            prepared_wal: &prepared_wal,
            after_head: &after_head,
            after_metadata: &after_metadata,
            now_ms: NOW_MS,
        });

        assert!(
            !report
                .check("create_file_writes_inode_direntry_and_initial_revision")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn wal_replay_invariant_report_marks_metadata_apply_as_passed() {
        let namespace_id = NamespaceId::new("ns-wal");
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(10),
            active_fence_token: FenceToken(4),
            next_inode_id: InodeId(20),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let basis_metadata = MetadataState::default();
        let wal = simple_replace_wal_object(&namespace_id, basis_head.seq, RevisionNo(1));
        let decoded = super::decode_wal_tail(std::slice::from_ref(&wal)).expect("decode wal");
        let (after_head, after_metadata) =
            super::replay_wal_tail_locally(&basis_head, &basis_metadata, &decoded).expect("replay");

        let report = evaluate_namespace_wal_replay_invariants(WalReplayInvariantInputs {
            expected_namespace: namespace_id.as_str(),
            basis_head: &basis_head,
            basis_metadata: &basis_metadata,
            wal_objects: &[StoredWalObject {
                object_key: wal.object_key.clone(),
                encoded_bytes: wal.encoded_bytes.clone(),
            }],
            after_head: &after_head,
            after_metadata: &after_metadata,
        });

        assert!(
            report
                .check("wal_replay_applies_metadata_rows")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn wal_replay_invariant_report_marks_metadata_apply_as_failed_when_final_state_diverges() {
        let namespace_id = NamespaceId::new("ns-wal");
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(10),
            active_fence_token: FenceToken(4),
            next_inode_id: InodeId(20),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let basis_metadata = MetadataState::default();
        let wal = simple_replace_wal_object(&namespace_id, basis_head.seq, RevisionNo(1));
        let decoded = super::decode_wal_tail(std::slice::from_ref(&wal)).expect("decode wal");
        let (after_head, mut after_metadata) =
            super::replay_wal_tail_locally(&basis_head, &basis_metadata, &decoded).expect("replay");
        after_metadata.revisions.clear();

        let report = evaluate_namespace_wal_replay_invariants(WalReplayInvariantInputs {
            expected_namespace: namespace_id.as_str(),
            basis_head: &basis_head,
            basis_metadata: &basis_metadata,
            wal_objects: &[StoredWalObject {
                object_key: wal.object_key.clone(),
                encoded_bytes: wal.encoded_bytes.clone(),
            }],
            after_head: &after_head,
            after_metadata: &after_metadata,
        });

        assert!(
            !report
                .check("wal_replay_applies_metadata_rows")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn checkpoint_replay_invariant_report_marks_basis_restore_as_passed() {
        let namespace_id = NamespaceId::new("ns-checkpoint");
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(30),
            active_fence_token: FenceToken(11),
            next_inode_id: InodeId(50),
            snapshot_hint_seq: Some(ChangeSeq(30)),
            retention_floor_seq: ChangeSeq(10),
        };
        let basis_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(9),
                bind_seq: ChangeSeq(30),
                bind_op_index: 0,
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(9),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(30),
                revision_op_index: 0,
                content_manifest_digest: "sha256:checkpoint".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        };
        let fixture = checkpoint_fixture(&namespace_id, &basis_head, &basis_metadata);

        let report =
            evaluate_namespace_checkpoint_replay_invariants(CheckpointReplayInvariantInputs {
                expected_namespace: namespace_id.as_str(),
                stored_manifest: &fixture.0,
                stored_segments: &fixture.1,
                basis_head: &basis_head,
                basis_metadata: &basis_metadata,
                wal_objects: &[],
                after_head: &basis_head,
                after_metadata: &basis_metadata,
            });

        assert!(
            report
                .check("checkpoint_segment_rows_restore_basis_metadata")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn checkpoint_replay_invariant_report_marks_basis_restore_as_failed_when_basis_diverges() {
        let namespace_id = NamespaceId::new("ns-checkpoint");
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(30),
            active_fence_token: FenceToken(11),
            next_inode_id: InodeId(50),
            snapshot_hint_seq: Some(ChangeSeq(30)),
            retention_floor_seq: ChangeSeq(10),
        };
        let basis_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        };
        let fixture = checkpoint_fixture(&namespace_id, &basis_head, &basis_metadata);
        let mismatched_basis = MetadataState::default();

        let report =
            evaluate_namespace_checkpoint_replay_invariants(CheckpointReplayInvariantInputs {
                expected_namespace: namespace_id.as_str(),
                stored_manifest: &fixture.0,
                stored_segments: &fixture.1,
                basis_head: &basis_head,
                basis_metadata: &mismatched_basis,
                wal_objects: &[],
                after_head: &basis_head,
                after_metadata: &mismatched_basis,
            });

        assert!(
            !report
                .check("checkpoint_segment_rows_restore_basis_metadata")
                .expect("check")
                .passed
        );
    }

    fn simple_replace_wal_object(
        namespace_id: &NamespaceId,
        base_head_seq: ChangeSeq,
        base_revision: RevisionNo,
    ) -> StoredWalObject {
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id: "wal-req".to_owned(),
            writer_id: "writer".to_owned(),
            writer_fence_token: FenceToken(5),
            planned_head_seq: base_head_seq,
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(9),
                base_revision,
                content_manifest_digest: "sha256:new".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(base_head_seq)],
        };
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: base_head_seq,
            active_fence_token: FenceToken(5),
            next_inode_id: InodeId(20),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer".to_owned(),
            fence_token: FenceToken(5),
            lease_expires_at_ms: NOW_MS + 100,
        };
        let basis_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(9),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: vec![RevisionRecord {
                inode_id: InodeId(9),
                revision_no: base_revision,
                committed_seq: base_head_seq,
                revision_op_index: 0,
                content_manifest_digest: "sha256:old".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        };
        let plan = build_commit_plan(
            &request,
            &CommitValidationContext {
                head: basis_head,
                lease,
                now_ms: NOW_MS,
                metadata_state: basis_metadata,
            },
        )
        .expect("plan");
        let prepared = prepare_wal_commit(&request, &plan, TEST_WRITER_VERSION).expect("wal");
        StoredWalObject {
            object_key: prepared.object_key,
            encoded_bytes: prepared.encoded_bytes,
        }
    }

    fn checkpoint_fixture(
        namespace_id: &NamespaceId,
        basis_head: &HeadState,
        basis_metadata: &MetadataState,
    ) -> (StoredCheckpointManifest, Vec<StoredCheckpointSegment>) {
        let rows = vec![
            CheckpointRow::Inode {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            },
            CheckpointRow::Direntry {
                parent_inode_id: InodeId(2),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(9),
                bind_seq: ChangeSeq(30),
                bind_op_index: 0,
            },
            CheckpointRow::Revision {
                inode_id: InodeId(9),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(30),
                revision_op_index: 0,
                content_manifest_digest: "sha256:checkpoint".to_owned(),
            },
        ];
        let mut segments = Vec::new();
        let mut descriptors = Vec::new();
        for (family, row) in [
            (CheckpointTableFamily::Inodes, rows[0].clone()),
            (CheckpointTableFamily::Direntries, rows[1].clone()),
            (CheckpointTableFamily::Revisions, rows[2].clone()),
        ] {
            let key = snapshot_table(
                namespace_id.as_str(),
                basis_head.seq.0,
                match family {
                    CheckpointTableFamily::Inodes => SnapshotTableFamily::Inodes,
                    CheckpointTableFamily::Direntries => SnapshotTableFamily::Direntries,
                    CheckpointTableFamily::Revisions => SnapshotTableFamily::Revisions,
                    CheckpointTableFamily::Tombstones => SnapshotTableFamily::Tombstones,
                },
                0,
            );
            let page = CheckpointPage {
                page_index: 0,
                min_key: row.row_key(),
                max_key: row.row_key(),
                row_keys: vec![row.row_key()],
                rows: vec![row],
            };
            let payload = CheckpointSegmentPayload {
                namespace_id: namespace_id.clone(),
                checkpoint_seq: basis_head.seq,
                family,
                segment_index: 0,
                row_count: 1,
                min_key: page.min_key.clone(),
                max_key: page.max_key.clone(),
                pages: vec![page],
            };
            let envelope =
                CheckpointSegmentEnvelope::from_payload(TEST_WRITER_VERSION, payload.clone())
                    .expect("segment envelope");
            let descriptor = CheckpointSegmentDescriptor {
                object_key: key.clone(),
                segment_index: 0,
                row_count: 1,
                min_key: payload.min_key.clone(),
                max_key: payload.max_key.clone(),
                payload_checksum_sha256: envelope.payload_checksum_sha256.clone(),
                page_checksums_sha256: envelope
                    .payload
                    .pages
                    .iter()
                    .map(checkpoint_page_checksum_sha256)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("page checksums"),
            };
            segments.push(StoredCheckpointSegment {
                object_key: key.clone(),
                encoded_bytes: encode_checkpoint_segment_envelope_zstd(&envelope)
                    .expect("encode segment"),
            });
            descriptors.push(CheckpointTableManifest {
                family,
                segments: vec![descriptor],
            });
        }

        let manifest = CheckpointManifestEnvelope::from_payload(
            TEST_WRITER_VERSION,
            CheckpointManifestPayload {
                namespace_id: namespace_id.clone(),
                checkpoint_seq: basis_head.seq,
                active_fence_token: basis_head.active_fence_token,
                next_inode_id: basis_head.next_inode_id,
                retention_floor_seq: basis_head.retention_floor_seq,
                verified: true,
                tables: descriptors,
            },
        )
        .expect("manifest envelope");

        let stored_manifest = StoredCheckpointManifest {
            object_key: snapshot_manifest(namespace_id.as_str(), basis_head.seq.0),
            encoded_bytes: encode_checkpoint_manifest_json(&manifest).expect("encode manifest"),
        };

        let _ = basis_metadata;
        (stored_manifest, segments)
    }
}
