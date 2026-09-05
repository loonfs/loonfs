//! Commit submission and path-oriented filesystem mutations.

use super::*;
use crate::transport::SendPolicy;
use crate::uploads::staging::{PreparedContent, UploadContinuity};

fn commit_id_or_generated(commit: &CommitOptions) -> CommitId {
    commit.commit_id.clone().unwrap_or_else(CommitId::generate)
}

impl Client {
    /// Applies a commit atomically, preserving operation order.
    ///
    /// The convenience methods below are the one-element case of this call.
    /// Operations that introduce new external content carry their proofs in
    /// the request's `content_tokens`; stage the bytes with the upload
    /// methods first.
    pub async fn create_commit(
        &self,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
    ) -> Result<ApiCommitResponse> {
        let url = format!("{}/v0/namespaces/{namespace_id}/commits", self.base_url);
        // The request's commit id resolves an ambiguous resend through a durable receipt.
        self.request_json::<_, ApiCommitResponse>(self.post(&url), Some(request), SendPolicy::Retry)
            .await
    }

    /// Uploads bytes and commits them at a path.
    ///
    /// Each call uploads a new content object. To retry publication, retain
    /// the result of [`Self::prepare_file_bytes`] and call
    /// [`Self::put_file_prepared`] with the same explicit commit ID and options.
    pub async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<ApiCommitResponse> {
        let staged = self
            .stage_bytes_as_content_ref(spec.namespace(), bytes)
            .await?;
        self.commit_staged_file(spec, staged, options, None).await
    }

    /// Uploads a payload read once from its source and commits it at a path.
    ///
    /// The source is read once in bounded chunks and is never assembled in
    /// memory. Memory use depends on the transport window, not payload size.
    ///
    /// Direct multipart uploads support at most 10,000 parts. Payloads larger
    /// than `part_size_bytes × 10_000` require a larger configured part size.
    ///
    /// Each call uploads new content. For retries, retain the result of
    /// [`Self::prepare_file_stream`] and use [`Self::put_file_prepared`].
    pub async fn put_file_stream(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
    ) -> Result<ApiCommitResponse> {
        self.put_file_stream_continuing(spec, source, options, UploadContinuity::default())
            .await
    }

    /// Uploads a stream with optional multipart resume state.
    ///
    /// The journal records the complete commit request before submission for
    /// every transport. Multipart uploads also record session geometry and parts.
    /// Save the request and replay it with [`Self::create_commit`] after an
    /// interruption; it carries the original content reference and commit ID.
    ///
    /// A resumed multipart attempt still receives the source from the
    /// beginning because the final checksum covers the complete object.
    pub async fn put_file_stream_resumable(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
        journal: &dyn PutFileJournal,
        resume: Option<&MultipartUploadResume>,
    ) -> Result<ApiCommitResponse> {
        self.put_file_stream_continuing(
            spec,
            source,
            options,
            UploadContinuity {
                resume,
                journal: Some(journal),
            },
        )
        .await
    }

    async fn put_file_stream_continuing(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
        continuity: UploadContinuity<'_>,
    ) -> Result<ApiCommitResponse> {
        let staged = self
            .stage_source_as_content_ref(spec.namespace(), source, continuity)
            .await?;
        self.commit_staged_file(spec, staged, options, continuity.journal)
            .await
    }

    /// Uploads bytes and returns content ready for publication.
    ///
    /// Retain this value and an explicit commit ID to retry
    /// [`Self::put_file_prepared`] without uploading again. Unpublished content
    /// expires after the upload's receipt horizon.
    pub async fn prepare_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        self.stage_bytes_as_content_ref(namespace_id, bytes).await
    }

    /// Uploads a stream once, in bounded chunks, for later publication.
    ///
    /// The result has the same retry contract as [`Self::prepare_file_bytes`].
    pub async fn prepare_file_stream(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
    ) -> Result<PreparedContent> {
        self.stage_source_as_content_ref(namespace_id, source, UploadContinuity::default())
            .await
    }

    /// Publishes already-prepared content without uploading it again.
    ///
    /// Retry with a clone of the prepared content, the same explicit
    /// `options.commit.commit_id`, and unchanged options. A missing commit ID
    /// generates a new one on each call. Replay is bounded by server receipt
    /// retention. For process recovery, use [`Self::put_file_stream_resumable`]
    /// to save the full request before submission.
    pub async fn put_file_prepared(
        &self,
        spec: &NamespacePath,
        prepared_content: PreparedContent,
        options: &PutFileOptions,
    ) -> Result<ApiCommitResponse> {
        self.commit_staged_file(spec, prepared_content, options, None)
            .await
    }

    /// Commits content after an upload completed but its file commit did not.
    ///
    /// Call [`Self::get_upload`] to recover the content reference and token.
    /// This method then commits the existing content without uploading it
    /// again. An optional journal saves the complete request before submission.
    pub async fn commit_completed_upload(
        &self,
        spec: &NamespacePath,
        content_ref: ContentRef,
        content_token: Option<ContentToken>,
        options: &PutFileOptions,
        journal: Option<&dyn PutFileJournal>,
    ) -> Result<ApiCommitResponse> {
        let staged = PreparedContent {
            content_token,
            content_ref,
        };
        self.commit_staged_file(spec, staged, options, journal)
            .await
    }

    /// Saves an exact request when journaled, then submits it unchanged.
    async fn commit_staged_file(
        &self,
        spec: &NamespacePath,
        staged: PreparedContent,
        options: &PutFileOptions,
        journal: Option<&dyn PutFileJournal>,
    ) -> Result<ApiCommitResponse> {
        let commit_id = commit_id_or_generated(&options.commit);
        let request = CommitRequest {
            commit_id: commit_id.clone(),
            actor: options.commit.actor.clone(),
            message: options.commit.message.clone(),
            content_tokens: staged.content_token.into_iter().collect(),
            operations: vec![FilesystemOperation::PutFile {
                path: spec.absolute_path().clone(),
                content_ref: staged.content_ref,
                behavior: options.behavior,
                expected_inode_id: options.expected_inode_id,
                expected_revision_no: options.expected_revision_no,
            }],
        };
        if let Some(journal) = journal {
            journal.commit_prepared(&request).map_err(|error| {
                ClientError::Io(format!(
                    "could not record file commit `{commit_id}` before submission: {error}"
                ))
            })?;
        }
        self.create_commit(spec.namespace(), &request).await
    }

    /// Creates a directory at the requested path.
    pub async fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &CreateDirectoryOptions,
    ) -> Result<ApiCommitResponse> {
        self.create_commit(
            spec.namespace(),
            &CommitRequest::single(
                commit_id_or_generated(&options.commit),
                options.commit.actor.clone(),
                options.commit.message.clone(),
                FilesystemOperation::CreateDirectory {
                    path: spec.absolute_path().clone(),
                    parents: options.parents,
                },
            ),
        )
        .await
    }

    /// Deletes the requested path.
    pub async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<ApiCommitResponse> {
        self.create_commit(
            spec.namespace(),
            &CommitRequest::single(
                commit_id_or_generated(&options.commit),
                options.commit.actor.clone(),
                options.commit.message.clone(),
                FilesystemOperation::DeletePath {
                    path: spec.absolute_path().clone(),
                    behavior: options.behavior,
                    expected_inode_id: options.expected_inode_id,
                },
            ),
        )
        .await
    }

    /// Writes and removes attributes on the inode a path resolves to. The
    /// target may be a file or a directory.
    pub async fn update_attributes(
        &self,
        spec: &NamespacePath,
        options: &UpdateAttributesOptions,
    ) -> Result<ApiCommitResponse> {
        self.create_commit(
            spec.namespace(),
            &CommitRequest::single(
                commit_id_or_generated(&options.commit),
                options.commit.actor.clone(),
                options.commit.message.clone(),
                FilesystemOperation::UpdateAttributes {
                    path: spec.absolute_path().clone(),
                    set: options.set.clone(),
                    remove: options.remove.clone(),
                    expected_inode_id: options.expected_inode_id,
                    expected_attributes_revision_no: options.expected_attributes_revision_no,
                },
            ),
        )
        .await
    }

    /// Moves a path within one namespace.
    pub async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &MoveOptions,
    ) -> Result<ApiCommitResponse> {
        if from.namespace() != to.namespace() {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot move across namespaces: {} -> {}",
                from.namespace(),
                to.namespace()
            )));
        }
        self.create_commit(
            from.namespace(),
            &CommitRequest::single(
                commit_id_or_generated(&options.commit),
                options.commit.actor.clone(),
                options.commit.message.clone(),
                FilesystemOperation::MovePath {
                    from_path: from.absolute_path().clone(),
                    to_path: to.absolute_path().clone(),
                    guard: loonfs_api::DestinationGuard {
                        behavior: options.behavior,
                        expected_inode_id: options.expected_destination_inode_id,
                        expected_revision_no: options.expected_destination_revision_no,
                    },
                },
            ),
        )
        .await
    }

    /// Copies a path within one namespace.
    pub async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &CopyOptions,
    ) -> Result<ApiCommitResponse> {
        if from.namespace() != to.namespace() {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot copy across namespaces: {} -> {}",
                from.namespace(),
                to.namespace()
            )));
        }
        self.create_commit(
            from.namespace(),
            &CommitRequest::single(
                commit_id_or_generated(&options.commit),
                options.commit.actor.clone(),
                options.commit.message.clone(),
                FilesystemOperation::CopyPath {
                    from_path: from.absolute_path().clone(),
                    to_path: to.absolute_path().clone(),
                    guard: loonfs_api::DestinationGuard {
                        behavior: options.behavior,
                        expected_inode_id: options.expected_destination_inode_id,
                        expected_revision_no: options.expected_destination_revision_no,
                    },
                },
            ),
        )
        .await
    }

    /// Restores a deleted file or subtree, optionally at a new path.
    pub async fn undelete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        path: Option<&AbsolutePath>,
        options: &UndeleteOptions,
    ) -> Result<ApiCommitResponse> {
        // An absent destination restores in place: the entry re-binds under
        // the parent and name its deletion recorded.
        self.create_commit(
            namespace_id,
            &CommitRequest::single(
                commit_id_or_generated(&options.commit),
                options.commit.actor.clone(),
                options.commit.message.clone(),
                FilesystemOperation::Undelete {
                    inode_id,
                    deletion_seq,
                    path: path.cloned(),
                },
            ),
        )
        .await
    }

    /// Makes an earlier file revision the current revision.
    pub async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &RestoreRevisionOptions,
    ) -> Result<ApiCommitResponse> {
        self.create_commit(
            spec.namespace(),
            &CommitRequest::single(
                commit_id_or_generated(&options.commit),
                options.commit.actor.clone(),
                options.commit.message.clone(),
                FilesystemOperation::RestoreRevision {
                    path: spec.absolute_path().clone(),
                    source_revision_no,
                },
            ),
        )
        .await
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;
    use crate::transport::{retryable_transport_failure, DEFAULT};
    use loonfs_api::{ContentId, ErrorCode};
    use std::fs;
    use tempfile::tempdir;

    fn test_content_ref(bytes: &[u8]) -> ContentRef {
        ContentRef::blob_v1(ContentId::generate(), bytes)
    }

    #[test]
    fn construction_validates_config_like_load_does() {
        let error = super::Client::new(super::ClientConfig {
            server_url: "ftp://example.com".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect_err("ftp scheme must be rejected");
        assert!(
            matches!(
                &error,
                super::ClientError::ConfigValidation {
                    field: "server_url",
                    ..
                }
            ),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn client_and_config_debug_redact_the_auth_token() {
        let raw_token = "client-debug-secret";
        let config = ClientConfig {
            server_url: "https://example.com".to_owned(),
            auth_token: Some(raw_token.into()),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        };

        let config_debug = format!("{config:?}");
        assert!(!config_debug.contains(raw_token), "{config_debug}");
        assert!(config_debug.contains("<redacted>"), "{config_debug}");

        let client = Client::new(config).expect("valid client config");
        let client_debug = format!("{client:?}");
        assert!(!client_debug.contains(raw_token), "{client_debug}");
        assert!(client_debug.contains("<redacted>"), "{client_debug}");
    }

    #[test]
    fn an_unusable_ca_bundle_fails_construction_and_names_the_path() {
        let dir = tempdir().expect("tempdir");
        let missing = dir.path().join("absent.crt");
        let garbage = dir.path().join("garbage.crt");
        fs::write(&garbage, b"this is not a certificate\n").expect("write garbage");

        for path in [missing, garbage] {
            let display = path.display().to_string();
            let error = super::Client::new(super::ClientConfig {
                server_url: "https://example.com".to_owned(),
                auth_token: None,
                request_timeout_ms: None,
                disable_transient_retry: false,
                ca_cert_path: Some(display.clone()),
            })
            .expect_err("unusable ca bundle");
            match &error {
                super::ClientError::ConfigValidation {
                    field: "ca_cert_path",
                    reason,
                } => assert!(
                    reason.contains(&display),
                    "the reason must name the path, got: {reason}"
                ),
                other => panic!("unexpected error: {other:?}"),
            }
        }
    }

    #[test]
    fn client_config_rejects_unknown_keys() {
        let error = toml::from_str::<ClientConfig>(
            "server_url = \"http://localhost:1\"\nauth_tokn = \"oops\"\n",
        )
        .expect_err("unknown key must fail decode");
        assert!(error.to_string().contains("auth_tokn"), "{error}");

        let config: ClientConfig =
            toml::from_str("server_url = \"http://localhost:1\"\n").expect("minimal config");
        assert!(config.auth_token.is_none());
    }
    #[test]
    fn retryable_transport_failure_covers_transport_and_unavailability_only() {
        let api = |code: &str| ClientError::Api {
            status: 503,
            code: code.to_owned(),
            feature: None,
            message: String::new(),
            param: None,
            request_id: None,
            details: None,
        };
        assert!(retryable_transport_failure(
            true,
            &ClientError::Http("reset".to_owned())
        ));
        assert!(retryable_transport_failure(false, &api("server_busy")));
        assert!(retryable_transport_failure(
            false,
            &api("commit_queue_full")
        ));
        assert!(retryable_transport_failure(false, &api("shutting_down")));
        assert!(!retryable_transport_failure(false, &api("server_error")));
        assert!(!retryable_transport_failure(
            false,
            &api("checkpoint_unavailable")
        ));
        assert!(!retryable_transport_failure(
            false,
            &api("maintenance_required")
        ));
        assert!(!retryable_transport_failure(false, &api("index_lagging")));
        // A server deadline uses 503 but is not safe to retry automatically.
        // The caller must first determine whether a mutation completed.
        assert!(!retryable_transport_failure(
            false,
            &api("deadline_exceeded")
        ));
        assert!(!retryable_transport_failure(
            false,
            &ClientError::Http("http status 502 with a non-envelope body".to_owned())
        ));
    }

    #[tokio::test]
    async fn transport_failures_resend_up_to_the_attempt_cap() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let max_attempts = DEFAULT.max_retries as usize + 1;
        let transport = crate::transport::test_transport::failures(max_attempts);
        let retrying = Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config");
        let error = retrying
            .get_namespace(&namespace_id)
            .await
            .expect_err("dropped connections must fail");
        assert!(matches!(error, ClientError::Http(_)), "{error:?}");
        assert_eq!(transport.attempts(), max_attempts);
        drop(transport);

        let transport = crate::transport::test_transport::failures(1);
        let single_shot = Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: true,
            ca_cert_path: None,
        })
        .expect("valid client config");
        single_shot
            .get_namespace(&namespace_id)
            .await
            .expect_err("dropped connection must fail without retry");
        assert_eq!(transport.attempts(), 1);
    }

    fn retry_policy_client() -> Client {
        Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config")
    }

    /// Installs a transport that fails once then succeeds, so a call that
    /// stops after one attempt surfaces the failure and a call that retries
    /// would succeed instead.
    fn single_attempt_probe() -> (crate::transport::test_transport::Guard, Client) {
        (
            crate::transport::test_transport::failure_then_success(b"{}".to_vec()),
            retry_policy_client(),
        )
    }

    fn assert_single_attempt<T>(
        result: Result<T>,
        transport: &crate::transport::test_transport::Guard,
    ) {
        assert!(
            matches!(result, Err(ClientError::Http(_))),
            "expected the first transport failure to surface"
        );
        assert_eq!(transport.attempts(), 1);
    }

    #[tokio::test]
    async fn retry_policy_lifecycle_mutations_are_single_attempt() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let fork_id = NamespaceId::parse("fork").expect("valid id");

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(client.create_namespace(&namespace_id).await, &transport);
        drop(transport);

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client.fork_namespace(&namespace_id, &fork_id).await,
            &transport,
        );
        drop(transport);

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client
                .delete_namespace(&namespace_id, Some(ChangeSeq(7)))
                .await,
            &transport,
        );
    }

    #[tokio::test]
    async fn retry_policy_matches_snapshot_and_maintenance_operation_classes() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client
                .create_snapshot(&namespace_id, "backup", 10_000)
                .await,
            &transport,
        );
        drop(transport);

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client
                .create_checkpoint(
                    &namespace_id,
                    &CreateCheckpointRequest {
                        name: "backup".to_owned(),
                        ttl_ms: None,
                    },
                )
                .await,
            &transport,
        );
        drop(transport);

        let response = NamespaceDiagnostics {
            namespace_id: namespace_id.clone(),
            head_seq: ChangeSeq(3),
            retention_floor_seq: ChangeSeq(1),
            current_manifest_no: None,
            wal_tail_segments: 2,
            live_snapshots: 3,
            live_checkpoints: 4,
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("safe maintenance read should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[tokio::test]
    async fn retry_policy_commit_id_filesystem_mutation_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let commit_id =
            CommitId::parse("c_00000000000000000000000000000001").expect("valid commit id");
        let response = ApiCommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: commit_id.clone(),
            committed_seq: ChangeSeq(1),
            committed_by: loonfs_test_support::test_actor(),
            committed_at_ms: 1_752_624_000_000,
            message: None,
            events: Some(Vec::new()),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();
        let spec = NamespacePath::parse("demo", "/docs").expect("valid namespace path");

        let actual = client
            .create_directory(&spec, &{
                let mut options = CreateDirectoryOptions::new(loonfs_test_support::test_actor());
                options.commit.commit_id = Some(commit_id);
                options
            })
            .await
            .expect("commit-id mutation should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[tokio::test]
    async fn retry_policy_read_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let response = Namespace {
            namespace_id: namespace_id.clone(),
            head_seq: ChangeSeq(0),
            retention_floor_seq: ChangeSeq(0),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .get_namespace(&namespace_id)
            .await
            .expect("read should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[tokio::test]
    async fn retry_policy_upload_begins_are_single_attempt() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client
                .create_upload(&namespace_id, &BeginUploadRequest::ServiceProxied {})
                .await,
            &transport,
        );
        drop(transport);

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client
                .create_direct_put_upload(&namespace_id, Some(b"direct".len() as u64))
                .await,
            &transport,
        );
    }

    #[tokio::test]
    async fn retry_policy_presigned_upload_is_single_attempt() {
        let transport = crate::transport::test_transport::failure_then_success(Vec::new());
        let client = retry_policy_client();
        let access = ObjectTransferAccess::PresignedUrl {
            method: "PUT".to_owned(),
            url: "http://example.invalid/upload".to_owned(),
            headers: std::collections::BTreeMap::new(),
            expires_at_ms: 1,
        };

        let result = client.upload_via_presigned_url(&access, b"direct").await;

        assert!(matches!(result, Err(ClientError::Http(_))), "{result:?}");
        assert_eq!(transport.attempts(), 1);
    }

    #[tokio::test]
    async fn retry_policy_proxied_upload_content_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let upload_id = loonfs_api::UploadId::parse("upl_00000000000000000000000000000001")
            .expect("valid upload id");
        let response = UploadContentResponse {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            content_ref: test_content_ref(b"content"),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .put_upload_content(&namespace_id, &upload_id, b"content")
            .await
            .expect("identical content staging should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[tokio::test]
    async fn retry_policy_upload_completion_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let upload_id = loonfs_api::UploadId::parse("upl_00000000000000000000000000000001")
            .expect("valid upload id");
        let content_ref = test_content_ref(b"content");
        let response = UploadSession {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            mode: loonfs_api::v0::UploadMode::ServiceProxied,
            status: UploadSessionStatus::Completed {
                completed_at_ms: 1,
                content_ref: content_ref.clone(),
                content_token: None,
            },
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .complete_upload(
                &namespace_id,
                &upload_id,
                &CompleteUploadRequest::ServiceProxied {},
            )
            .await
            .expect("completed-session replay should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[tokio::test]
    async fn staging_rejects_a_non_completed_completion_response() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let upload_id =
            UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id");
        let statuses = [
            (
                "open",
                UploadSessionStatus::Open {
                    expires_at_ms: 1_000,
                },
            ),
            (
                "aborted",
                UploadSessionStatus::Aborted {
                    aborted_at_ms: 2_000,
                },
            ),
        ];

        for (status_name, status) in statuses {
            let response = UploadSession {
                namespace_id: namespace_id.clone(),
                upload_id: upload_id.clone(),
                mode: loonfs_api::v0::UploadMode::ServiceProxied,
                status,
            };
            let transport = crate::transport::test_transport::script([
                crate::transport::test_transport::Outcome::Success(
                    serde_json::to_vec(&response).expect("serialize response"),
                ),
            ]);
            let client = retry_policy_client();

            let error = client
                .complete_staged(&namespace_id, &upload_id)
                .await
                .expect_err("a completion response must report completed");
            assert!(
                matches!(
                    &error,
                    ClientError::Protocol(message)
                        if message.contains(upload_id.as_str()) && message.contains(status_name)
                ),
                "expected protocol error, got {error:?}"
            );
            assert_eq!(transport.attempts(), 1);
        }
    }

    #[test]
    fn status_errors_keep_the_status_when_the_body_is_not_the_envelope() {
        let error = crate::transport::map_status_error(502, b"<html>upstream error</html>");

        let ClientError::Http(message) = error else {
            panic!("expected Http error, got {error:?}");
        };
        assert!(message.contains("502"), "{message}");
        assert!(message.contains("non-envelope body"), "{message}");
    }

    fn api_error(status: u16, code: &str) -> ClientError {
        ClientError::Api {
            status,
            code: code.to_owned(),
            feature: None,
            message: "test".to_owned(),
            param: None,
            request_id: None,
            details: None,
        }
    }

    #[test]
    fn api_errors_expose_known_registry_codes() {
        for (wire, expected) in [
            ("stale_revision", ErrorCode::StaleRevision),
            ("content_not_prepared", ErrorCode::ContentNotPrepared),
            ("namespace_deleted", ErrorCode::NamespaceDeleted),
            ("commit_outcome_unknown", ErrorCode::CommitOutcomeUnknown),
            ("index_corrupt", ErrorCode::IndexCorrupt),
        ] {
            assert_eq!(api_error(500, wire).code(), Some(expected));
        }
    }

    #[test]
    fn api_errors_tolerate_unknown_registry_codes() {
        assert_eq!(api_error(503, "code_from_a_newer_server").code(), None);
        assert_eq!(
            ClientError::Http("connection refused".to_owned()).code(),
            None
        );
    }

    #[test]
    fn load_rejects_blank_auth_token() {
        let path = write_config(
            r#"
server_url = "http://127.0.0.1:9400"
auth_token = "   "
"#,
        );

        let error = ClientConfig::load(&path).expect_err("blank auth token");

        assert!(
            matches!(error, ClientError::ConfigValidation { field, .. } if field == "auth_token"),
            "expected config validation error, got {error:?}"
        );
    }

    #[test]
    fn load_preserves_missing_file_as_config_io() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("missing.toml");

        let error = ClientConfig::load(&path).expect_err("missing config");

        assert!(matches!(error, ClientError::ConfigIo(_)));
    }

    #[test]
    fn load_preserves_decode_error() {
        let path = write_config("server_url = [");

        let error = ClientConfig::load(&path).expect_err("decode error");

        assert!(matches!(error, ClientError::ConfigDecode(_)));
    }

    #[test]
    fn namespace_path_parse_rejects_invalid_namespace_id() {
        for namespace in ["bad/name", "Demo", "..", "demo?"] {
            assert!(
                matches!(
                    NamespacePath::parse(namespace, "/notes.txt"),
                    Err(ClientError::InvalidNamespacePath(_))
                ),
                "expected invalid namespace path for id {namespace:?}"
            );
        }
    }

    fn write_config(contents: &str) -> std::path::PathBuf {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("client.toml");
        fs::write(&path, contents).expect("write config");
        let _ = temp_dir.keep();
        path
    }
}
