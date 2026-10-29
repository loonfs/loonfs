//! Namespace creation and individual path mutations for tests, as methods of
//! [`NamespaceFixture`], and the test hooks of [`NamespaceStore`] that write
//! below the commit path.

use super::NamespaceFixture;
use crate::commit::engine::CommitCandidate;
use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use crate::path::mutation_path::parse_mutation_path;
use crate::path::write::{CommitIntent, FilesystemOperation};
use crate::storage::content_admission::PreparedContent;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_types::format::wal::{
    encode_wal_object_envelope_zstd, WalCommitDelta, WalCommitPayload, WalDelta, WalInlineContent,
    WalObjectPayload,
};
use loonfs_types::{
    AbsolutePath, ActorId, ChangeSeq, Commit, CommitId, DeleteDirectoryBehavior,
    DestinationBehavior, NamespaceMetadata, RevisionNo,
};

/// The actor every test commit records. It's the actor that
/// `loonfs_test_support::test_actor` returns, spelled here because the
/// `test-support` feature can't depend on that unpublished crate.
fn test_actor() -> ActorId {
    ActorId::parse("test-actor").expect("the test actor id should parse")
}

impl<S: ObjectStore + Clone> NamespaceFixture<S> {
    /// Creates the namespace with the default options.
    pub async fn create(&self) -> Result<NamespaceMetadata> {
        self.create_with_options(&crate::options::CreateNamespaceOptions::default())
            .await
    }

    /// Creates the namespace with `options`.
    pub async fn create_with_options(
        &self,
        options: &crate::options::CreateNamespaceOptions,
    ) -> Result<NamespaceMetadata> {
        self.engine()
            .bootstrap_namespace(&test_actor(), options)
            .await
    }

    /// Publishes `candidates` as one batch through a fresh commit engine, and
    /// publishes again each candidate whose attempt the store settled as
    /// retryable.
    pub async fn publish_candidates(
        &self,
        candidates: Vec<CommitCandidate>,
    ) -> Vec<Result<Commit>> {
        self.engine()
            .commit_engine()
            .publish_batch_until_settled(candidates, &self.namespace.start_clock())
            .await
    }

    /// Publishes one operation as a whole request with `prepared_content`:
    /// one operation, one commit ID, no message.
    pub async fn submit_operation(
        &self,
        commit_id: CommitId,
        operation: FilesystemOperation,
        prepared_content: Vec<PreparedContent>,
    ) -> Result<Commit> {
        let request = CommitIntent::single(commit_id, test_actor(), None, operation);
        let candidate = if prepared_content.is_empty() {
            CommitCandidate::new(request)
        } else {
            CommitCandidate::prepared(request, prepared_content)
        };
        let mut results = self.publish_candidates(vec![candidate]).await;
        assert_eq!(
            results.len(),
            1,
            "a single-candidate batch should return one result"
        );
        results
            .pop()
            .expect("a single-candidate batch should hold one result")
    }

    /// Stages `bytes` as content and puts them at `absolute_path` with
    /// `behavior`, under `commit_id` or a new commit ID.
    pub async fn put_file(
        &self,
        absolute_path: &str,
        bytes: &[u8],
        behavior: DestinationBehavior,
        commit_id: Option<&CommitId>,
    ) -> Result<Commit> {
        let prepared_content = self
            .store_file_bytes_before_metadata_publish(absolute_path, bytes)
            .await?;
        let content_ref = prepared_content.content_ref().clone();
        self.submit_operation(
            normalized_commit_id(commit_id),
            FilesystemOperation::PutFile {
                path: parse_mutation_path(absolute_path)?,
                content: loonfs_types::FileContent::Ref(content_ref),
                behavior,
                expected_inode_id: None,
                expected_revision_no: None,
            },
            vec![prepared_content],
        )
        .await
    }

    /// Puts `bytes` at `absolute_path`, replacing any file there.
    pub async fn write_file_bytes(
        &self,
        absolute_path: &str,
        bytes: &[u8],
        commit_id: Option<&CommitId>,
    ) -> Result<Commit> {
        self.put_file(
            absolute_path,
            bytes,
            DestinationBehavior::Replace,
            commit_id,
        )
        .await
    }

    /// Writes several files in one commit, for fixtures that need many rows but
    /// not a commit per row.
    pub async fn write_files_bytes(
        &self,
        absolute_paths: &[String],
        bytes: &[u8],
    ) -> Result<Commit> {
        let mut operations = Vec::with_capacity(absolute_paths.len());
        let mut prepared = Vec::with_capacity(absolute_paths.len());
        for absolute_path in absolute_paths {
            let content = self
                .store_file_bytes_before_metadata_publish(absolute_path, bytes)
                .await?;
            operations.push(FilesystemOperation::PutFile {
                path: parse_mutation_path(absolute_path)?,
                content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            });
            prepared.push(content);
        }
        let request = CommitIntent {
            commit_id: CommitId::generate(),
            actor_id: test_actor(),
            subject: None,
            message: None,
            operations,
            preconditions: Vec::new(),
        };
        self.publish_candidates(vec![CommitCandidate::prepared(request, prepared)])
            .await
            .pop()
            .expect("single-candidate batch should hold exactly one result")
    }

    /// Deletes `absolute_path`, recursively for a directory.
    pub async fn delete_path(
        &self,
        absolute_path: &str,
        commit_id: Option<&CommitId>,
    ) -> Result<Commit> {
        self.submit_operation(
            normalized_commit_id(commit_id),
            FilesystemOperation::DeletePath {
                path: parse_mutation_path(absolute_path)?,
                behavior: DeleteDirectoryBehavior::Recursive,
                expected_inode_id: None,
            },
            Vec::new(),
        )
        .await
    }

    /// Moves `source_path` to `destination_path`, which must not exist.
    pub async fn move_path(
        &self,
        source_path: &str,
        destination_path: &str,
        commit_id: Option<&CommitId>,
    ) -> Result<Commit> {
        self.submit_operation(
            normalized_commit_id(commit_id),
            FilesystemOperation::MovePath {
                source_path: parse_mutation_path(source_path)?,
                destination_path: parse_mutation_path(destination_path)?,
                precondition: loonfs_types::DestinationPrecondition {
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            },
            Vec::new(),
        )
        .await
    }

    /// Restores revision `source_revision_no` of the file at `absolute_path`.
    pub async fn restore_file_revision(
        &self,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        commit_id: Option<&CommitId>,
    ) -> Result<Commit> {
        self.submit_operation(
            normalized_commit_id(commit_id),
            FilesystemOperation::RestoreRevision {
                path: parse_mutation_path(absolute_path)?,
                source_revision_no,
            },
            Vec::new(),
        )
        .await
    }

    /// Appends one small commit per WAL object without folding the tail.
    pub async fn append_wal_objects(&self, count: u64) -> Result<()> {
        let mut engine = self.engine().commit_engine();
        for _ in 0..count {
            let commit_id = CommitId::generate();
            let path = AbsolutePath::parse(format!("/wal-tail-{commit_id}"))
                .expect("a generated WAL-tail test path should parse");
            let request = CommitIntent::single(
                commit_id,
                ActorId::loonfs(),
                None,
                FilesystemOperation::CreateDirectory {
                    path,
                    parents: false,
                },
            );
            let mut results = engine
                .publish_batch(
                    vec![CommitCandidate::new(request)],
                    &self.namespace.start_clock(),
                )
                .await
                .results;
            assert_eq!(
                results.len(),
                1,
                "a single-candidate test publish should return one result"
            );
            results
                .pop()
                .expect("a single-candidate test publish should hold one result")?;
        }
        Ok(())
    }
}

fn normalized_commit_id(commit_id: Option<&CommitId>) -> CommitId {
    commit_id.cloned().unwrap_or_else(CommitId::generate)
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Raises the namespace's hint to its current manifest, as the runtime
    /// that published that manifest does after the publication. A test hook.
    ///
    /// A publication in core leaves the hint where it was, and a runtime skips
    /// a raise less than `HINT_RAISE_INTERVAL_MS` after its last one, so a test
    /// that needs garbage collection or discovery to see a current hint calls
    /// this.
    pub async fn raise_hint_to_current_manifest(&self) {
        let current = self
            .load_current_manifest()
            .await
            .expect("load the current manifest")
            .manifest()
            .manifest_no;
        self.raise_hint(current, None)
            .await
            .expect("raise the hint");
    }

    /// Publishes `deltas` and `inline_content` as one commit in the next WAL
    /// object, without planning, so a test controls the exact deltas and
    /// pieces. A test hook.
    #[allow(
        clippy::disallowed_methods,
        reason = "a test writes the next numbered WAL object directly"
    )]
    pub async fn append_wal_commit(
        &self,
        deltas: Vec<WalDelta>,
        inline_content: Vec<WalInlineContent>,
    ) -> Result<ChangeSeq> {
        let head = self.load_read_state().await?;
        let committed_seq = ChangeSeq(head.seq.0 + 1);
        let wal_no = loonfs_types::WalNo(head.wal_no.0 + 1);
        let payload = WalObjectPayload {
            namespace_id: self.namespace_id.clone(),
            wal_no,
            writer_epoch: head.writer_epoch,
            head_seq: committed_seq,
            next_inode_id: head.next_inode_id,
            basis_floor_seq: head.basis_floor_seq,
            records: vec![WalCommitPayload {
                committed_seq,
                commit_id: CommitId::generate(),
                committed_by: test_actor(),
                semantic_commit_fingerprint: serde_json::from_str(&format!(
                    r#""v1:sha256:{:064x}""#,
                    committed_seq.0
                ))
                .expect("fingerprint"),
                committed_at_ms: 0,
                message: None,
                deltas: (0_u32..)
                    .zip(deltas)
                    .map(|(semantic_operation_index, delta)| WalCommitDelta {
                        semantic_operation_index,
                        delta,
                    })
                    .collect(),
                inline_content,
            }],
        };
        let object =
            encode_wal_object_envelope_zstd(payload).expect("a test WAL object should encode");
        let key = loonfs_objectstore::keys::wal_object(&self.namespace_id, &wal_no);
        self.store
            .put(&key, object.into_bytes().into(), PutMode::CreateIfAbsent)
            .await
            .map_err(|error| CoreError::store(&key, &error))?;
        Ok(committed_seq)
    }
}
