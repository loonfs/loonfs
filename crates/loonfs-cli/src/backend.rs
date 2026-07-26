//! Embedded implementation of the CLI's shared [`Backend`] seam.

use crate::backend_error::{map_namespace_scoped_runtime_error, map_runtime_error};
use crate::render::write_stderr_warning;
use async_trait::async_trait;
use loonfs::{
    ChangesResponse, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse, DeleteOptions,
    FsAdmin, FsReader, FsWriter, ListChangesOptions, MaintenanceStepOptions, MoveOptions,
    PutFileOptions, RestoreRevisionOptions, RuntimeError, UndeleteOptions,
};
use loonfs_api::{
    v0::{DisableGrepIndexResponse, EnableGrepIndexResponse, RepairNamespaceResponse},
    AuthoritativePathEntry, ChangeSeq, CheckpointId, CommitId, CommitResponse,
    CreateCheckpointRequest, CreateCheckpointResponse, DestinationBehavior, EffectiveLimit,
    ErrorCode, GrepRequest, GrepResponse, InodeId, ListFileRevisionsResponse,
    MaintenanceStepRequest, MaintenanceStepResponse, NamespaceId, NamespaceStatusResponse,
    NamespaceSummary, PaginationPolicy, ReleaseCheckpointResponse, RevisionNo,
};
use loonfs_client::{
    CreateDirectoryOptions as ClientCreateDirectoryOptions, DeleteOptions as ClientDeleteOptions,
    NamespacePath, PutFileOptions as ClientPutFileOptions,
};

#[cfg(test)]
pub(crate) use crate::resolve::EmbeddedTarget;
pub(crate) use crate::resolve::ResolvedTarget;
pub(crate) use loonfs_client::backend::{Backend, BackendError, RemoteBackend};

/// Purpose-specific handles over one shared store client: reads go through
/// the reader, mutations through the writer, and maintenance through the
/// admin handle. The embedded writer runs `FsBackgroundWork::Enabled` — the
/// same policy as the reference server — so a publish that crosses the WAL
/// threshold schedules its own maintenance step, and every mutation settles
/// scheduled work before the one-shot process exits. A publish gated on
/// `maintenance_required` waits for the step that same gated publish
/// scheduled, then resubmits, so embedded writes recover from WAL debt
/// instead of hard-stopping. `loon admin` commands remain the explicit path
/// for everything else (GC, retention, forced steps).
pub(crate) struct EmbeddedBackend {
    pub(super) writer: FsWriter,
    pub(super) reader: FsReader,
    pub(super) admin: FsAdmin,
}

/// How many times a gated publish resubmits after settling the maintenance
/// step it scheduled. One recovery is the normal case; the second covers a
/// step that raced another writer's debt. Past that the error surfaces.
const MAX_MAINTENANCE_RECOVERIES: usize = 2;

impl EmbeddedBackend {
    /// Waits out writer-scheduled maintenance so a one-shot command never
    /// exits (tearing down the runtime) while a step is mid-flight. A settle
    /// failure after a committed mutation is reported as a warning on
    /// stderr, never as the mutation's outcome — the commit landed.
    async fn settle_background_work_after<T>(
        &self,
        result: Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        match (result, self.writer.wait_for_background_work().await) {
            (result, Ok(())) => result,
            (Ok(value), Err(error)) => {
                write_stderr_warning(format_args!(
                    "background maintenance did not settle cleanly: {error}"
                ));
                Ok(value)
            }
            (Err(error), Err(_)) => Err(error),
        }
    }

    /// Runs one mutation with `maintenance_required` recovery: a gated
    /// publish observes the oversized WAL tail and schedules its own
    /// recovery step (the writer policy is `Enabled`), so settle that step
    /// and resubmit. A gated attempt commits nothing, so the resubmission
    /// cannot double-apply.
    async fn publish_with_maintenance_recovery<T, F, Fut>(
        &self,
        namespace_id: &NamespaceId,
        attempt: F,
    ) -> Result<T, BackendError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, RuntimeError>>,
    {
        let mut result = attempt().await;
        for _ in 0..MAX_MAINTENANCE_RECOVERIES {
            let gated = matches!(
                &result,
                Err(RuntimeError::Core(error))
                    if matches!(error.code(), ErrorCode::MaintenanceRequired)
            );
            if !gated {
                break;
            }
            self.writer
                .wait_for_background_work()
                .await
                .map_err(map_runtime_error)?;
            result = attempt().await;
        }
        let result =
            result.map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error));
        self.settle_background_work_after(result).await
    }
}

#[async_trait]
impl Backend for EmbeddedBackend {
    async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
        let result = self
            .writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .map_err(map_runtime_error);
        self.settle_background_work_after(result).await
    }

    async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse, BackendError> {
        let options = DeleteNamespaceOptions { expected_head_seq };
        let result = self
            .writer
            .delete_namespace(namespace_id, options)
            .await
            .map_err(map_runtime_error);
        self.settle_background_work_after(result).await
    }

    async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary, BackendError> {
        let result = self
            .writer
            .fork_namespace(source_namespace_id, new_namespace_id)
            .await
            .map_err(map_runtime_error);
        self.settle_background_work_after(result).await
    }

    async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        self.admin
            .namespace_status(namespace_id)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, BackendError> {
        Ok(self
            .reader
            .list_path_entries_all(spec.namespace(), spec.absolute_path().as_str())
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))?
            .entries)
    }

    async fn stat_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<AuthoritativePathEntry, BackendError> {
        self.reader
            .stat_path(spec.namespace(), spec.absolute_path().as_str())
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))
    }

    async fn get_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError> {
        let result = self
            .reader
            .get_file_bytes(spec.namespace(), spec.absolute_path().as_str())
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))?;
        Ok(result.bytes)
    }

    async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError> {
        self.reader
            .grep(namespace_id, request)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGrepIndexResponse, BackendError> {
        self.admin
            .enable_grep_index(namespace_id)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse, BackendError> {
        self.admin
            .disable_grep_index(namespace_id)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError> {
        let result = self
            .reader
            .get_file_revision_bytes(spec.namespace(), spec.absolute_path().as_str(), revision_no)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))?;
        Ok(result.bytes)
    }

    async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError> {
        let request = loonfs_api::PageRequest {
            limit: resolve_cli_page_limit(limit)?,
            cursor: cursor
                .map(loonfs_api::decode_cursor)
                .transpose()
                .map_err(|error| {
                    BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                })?,
        };
        self.reader
            .list_file_revisions_page(spec.namespace(), spec.absolute_path().as_str(), request)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(spec.namespace(), error))
    }

    async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &ClientPutFileOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.put_file_bytes(
                spec.namespace(),
                spec.absolute_path().as_str(),
                bytes,
                PutFileOptions {
                    behavior: options.behavior,
                    commit_id: options.commit_id.clone(),
                },
            )
        })
        .await
    }

    async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &ClientDeleteOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.delete_path(
                spec.namespace(),
                spec.absolute_path().as_str(),
                DeleteOptions {
                    behavior: options.behavior,
                    commit_id: options.commit_id.clone(),
                    expected_inode_id: options.expected_inode_id,
                },
            )
        })
        .await
    }

    async fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &ClientCreateDirectoryOptions,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.create_directory(
                spec.namespace(),
                spec.absolute_path().as_str(),
                CreateDirectoryOptions {
                    commit_id: options.commit_id.clone(),
                    parents: options.parents,
                },
            )
        })
        .await
    }

    async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(from.namespace(), || {
            self.writer.move_path(
                from.namespace(),
                from.absolute_path().as_str(),
                to.absolute_path().as_str(),
                MoveOptions {
                    behavior,
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: DestinationBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(from.namespace(), || {
            self.writer.copy_path(
                from.namespace(),
                from.absolute_path().as_str(),
                to.absolute_path().as_str(),
                CopyOptions {
                    behavior,
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.restore_file_revision(
                spec.namespace(),
                spec.absolute_path().as_str(),
                source_revision_no,
                RestoreRevisionOptions {
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    async fn undelete(
        &self,
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        self.publish_with_maintenance_recovery(spec.namespace(), || {
            self.writer.undelete(
                spec.namespace(),
                inode_id,
                deleted_at_seq,
                spec.absolute_path().as_str(),
                UndeleteOptions {
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    // The admin methods mirror the server handlers' error scoping exactly:
    // checkpoint/retention map runtime errors unscoped, the change feed is
    // namespace-scoped. Parity keeps embedded and remote outputs identical.

    async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, BackendError> {
        self.admin
            .create_checkpoint(namespace_id, CreateCheckpointOptions::from_request(request))
            .await
            .map_err(map_runtime_error)
    }

    async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse, BackendError> {
        self.admin
            .release_checkpoint(namespace_id, checkpoint_id)
            .await
            .map_err(map_runtime_error)
    }

    async fn maintenance_step(
        &self,
        namespace_id: &NamespaceId,
        request: MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse, BackendError> {
        let options = MaintenanceStepOptions::from_request(request);
        self.admin
            .maintenance_step_namespace(namespace_id, options)
            .await
            .map_err(map_runtime_error)
    }

    async fn repair_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<RepairNamespaceResponse, BackendError> {
        self.admin
            .repair_namespace(namespace_id)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError> {
        let limit = resolve_cli_page_limit(limit)?;
        self.reader
            .list_changes(
                namespace_id,
                after_seq,
                ListChangesOptions { limit: Some(limit) },
            )
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }
}

fn resolve_cli_page_limit(limit: Option<u32>) -> Result<EffectiveLimit, BackendError> {
    // The server maps this same policy error to `invalid_request`; embedded
    // mode must report the identical registry code for the identical failure.
    PaginationPolicy::default()
        .resolve_limit(limit)
        .map_err(|error| BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string()))
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::{map_runtime_error, resolve_cli_page_limit, Backend, EmbeddedTarget};
    use crate::config::StoreConfig;
    use loonfs::{
        BootstrapNamespaceError, CoreError, CreateNamespaceOptions, FsBackgroundWork, FsWriter,
        GrepError, PutFileOptions, RuntimeError, SharedObjectStore,
    };
    use loonfs_api::{
        ChangeSeq, CreateCheckpointRequest, DestinationBehavior, ErrorCode, InodeId, NamespaceId,
        RevisionNo,
    };
    use loonfs_client::{NamespacePath, PutFileOptions as ClientPutFileOptions};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn namespace_id(value: &str) -> NamespaceId {
        NamespaceId::parse(value).expect("valid namespace id")
    }

    #[test]
    fn map_core_error_surfaces_registry_codes_verbatim() {
        let error = map_runtime_error(RuntimeError::Core(CoreError::RevisionNotFound {
            inode_id: InodeId(42),
            revision_no: RevisionNo(7),
        }));

        assert_eq!(error.code, ErrorCode::RevisionNotFound.as_str());

        let error = map_runtime_error(RuntimeError::Core(CoreError::ContentPreparation(
            loonfs::publish::ContentPreparationError::ContentNotPrepared {
                content_ref_digest: "abc123".to_owned(),
            },
        )));
        assert_eq!(error.code, ErrorCode::ContentNotPrepared.as_str());
        assert!(error.message.contains("abc123"));
    }

    #[test]
    fn map_grep_error_preserves_embedded_remote_code_parity() {
        for (error, expected) in [
            (GrepError::NotEnabled, ErrorCode::NotSupported),
            (
                GrepError::CorruptIndex {
                    message: "bad pointer".to_owned(),
                },
                ErrorCode::IndexCorrupt,
            ),
            (
                GrepError::PublicationConflict {
                    object_key: "namespaces/demo/extensions/grep/root.json".to_owned(),
                },
                ErrorCode::StaleHead,
            ),
        ] {
            assert_eq!(
                map_runtime_error(RuntimeError::Grep(error)).code,
                expected.as_str()
            );
        }
    }

    #[test]
    fn page_limit_errors_report_the_registry_code_the_server_serves() {
        // `--limit 0` fails the same PaginationPolicy check in both modes;
        // embedded mode must answer `invalid_request` like the server, not a
        // CLI-local `invalid_input` rewrite.
        let error = resolve_cli_page_limit(Some(0)).expect_err("zero limit is invalid");
        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    }

    #[test]
    fn map_core_error_does_not_rewrite_invalid_id_codes() {
        // Embedded mode must report the same code the server serves for the
        // identical failure, not a CLI-local `invalid_input` rewrite.
        let invalid_id = NamespaceId::parse("bad/name").expect_err("invalid namespace id");
        let error = map_runtime_error(RuntimeError::Core(CoreError::InvalidNamespaceId(
            invalid_id,
        )));

        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    }

    #[test]
    fn map_bootstrap_error_surfaces_registry_codes_verbatim() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let error = map_runtime_error(RuntimeError::Bootstrap(
            BootstrapNamespaceError::NamespaceAlreadyExists { namespace_id },
        ));

        assert_eq!(error.code, ErrorCode::NamespaceExists.as_str());
        assert!(error.message.contains("already exists"));
    }

    #[tokio::test]
    async fn embedded_backend_put_returns_the_commit_id_it_committed_under() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .create_namespace(&namespace_id("demo"))
            .await
            .expect("create namespace");

        let response = target
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/file.txt").expect("namespace path"),
                b"hello",
                &ClientPutFileOptions::default(),
            )
            .await
            .expect("put file");
        assert!(!response.commit_id.as_str().trim().is_empty());

        let changes = target
            .backend
            .list_changes(&namespace_id("demo"), ChangeSeq(0), None)
            .await
            .expect("list changes");
        assert_eq!(changes.changes.len(), 1);
        assert_eq!(changes.changes[0].commit_id, response.commit_id);
    }

    #[tokio::test]
    async fn embedded_admin_methods_surface_registry_codes_for_missing_namespaces() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None)
            .await
            .expect("build embedded target");

        let checkpoint = target
            .backend
            .create_checkpoint(
                &namespace_id("missing"),
                CreateCheckpointRequest {
                    name: "nightly".to_owned(),
                    ttl_ms: None,
                },
            )
            .await
            .expect_err("checkpoint on missing namespace");
        assert_eq!(checkpoint.code, ErrorCode::NamespaceNotFound.as_str());

        let changes = target
            .backend
            .list_changes(&namespace_id("missing"), ChangeSeq(0), None)
            .await
            .expect_err("changes on missing namespace");
        assert_eq!(changes.code, ErrorCode::NamespaceNotFound.as_str());
        assert_eq!(changes.message, "namespace `missing` does not exist");
    }

    #[tokio::test]
    async fn embedded_writes_never_stall_at_the_wal_backpressure_cap() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .create_namespace(&namespace_id("demo"))
            .await
            .expect("create namespace");

        // More publishes than the WAL backpressure cap: the Enabled policy
        // must keep stepping the tail down so no write ever stalls on
        // `maintenance_required` (each stall used to require a manual
        // `loon admin step`).
        for index in 0..140 {
            target
                .backend
                .put_file_bytes(
                    &NamespacePath::parse("demo", &format!("/files/f{index}.txt"))
                        .expect("namespace path"),
                    b"payload",
                    &ClientPutFileOptions::default(),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("put {index} failed: {} {}", error.code, error.message)
                });
        }
    }

    #[tokio::test]
    async fn embedded_writes_recover_from_preexisting_wal_debt() {
        let temp_dir = tempdir().expect("create temp dir");
        let store_config = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };

        // Accumulate WAL debt the way pre-fix builds did: a ManualOnly
        // writer publishes until the backpressure gate refuses the next
        // publish outright.
        let store: SharedObjectStore = Arc::new(
            store_config
                .configured_object_store()
                .expect("configure store"),
        );
        let writer = FsWriter::builder_with_store(store)
            .writer_id("debt-builder")
            .writer_version("test")
            .background_work(FsBackgroundWork::ManualOnly)
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("build debt writer");
        let namespace = NamespaceId::parse("demo").expect("namespace id");
        writer
            .create_namespace(&namespace, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let mut stalled = false;
        for index in 0..200 {
            let result = writer
                .put_file_bytes(
                    &namespace,
                    &format!("/files/f{index}.txt"),
                    b"payload",
                    PutFileOptions {
                        behavior: DestinationBehavior::NoReplace,
                        commit_id: None,
                    },
                )
                .await;
            match result {
                Ok(_) => {}
                Err(RuntimeError::Core(error))
                    if matches!(error.code(), ErrorCode::MaintenanceRequired) =>
                {
                    stalled = true;
                    break;
                }
                Err(error) => panic!("unexpected stall error: {error}"),
            }
        }
        assert!(stalled, "ManualOnly writer never hit the backpressure cap");

        // The embedded backend digs itself out: the gated publish schedules
        // its own step, the backend settles it and resubmits.
        let target = EmbeddedTarget::new(&store_config, None, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/recovered.txt").expect("namespace path"),
                b"payload",
                &ClientPutFileOptions::default(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("recovery put failed: {} {}", error.code, error.message)
            });
    }
    #[tokio::test]
    async fn a_fenced_put_fails_terminally_and_names_both_sessions() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        // Two backends over one store model two concurrent `loon` processes:
        // the writer id is shared (the CLI defaults it to the hostname) and
        // only the session ids differ, so without session identity in the
        // fence a user reads the error as their machine fencing itself.
        let first = EmbeddedTarget::new(&store, Some("shared-host"), None)
            .await
            .expect("build first embedded target");
        first
            .backend
            .create_namespace(&namespace_id("demo"))
            .await
            .expect("create namespace");
        first
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/one.txt").expect("namespace path"),
                b"one",
                &ClientPutFileOptions::default(),
            )
            .await
            .expect("first put acquires the epoch");

        let rival = EmbeddedTarget::new(&store, Some("shared-host"), None)
            .await
            .expect("build rival embedded target");
        rival
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/two.txt").expect("namespace path"),
                b"two",
                &ClientPutFileOptions::default(),
            )
            .await
            .expect("rival put takes the epoch over");

        // Fenced sessions are terminal — no silent reacquisition, matching
        // remote mode and the core contract. The error carries both session
        // identities so the loser is diagnosable, and the failed put
        // committed nothing.
        let error = first
            .backend
            .put_file_bytes(
                &NamespacePath::parse("demo", "/three.txt").expect("namespace path"),
                b"three",
                &ClientPutFileOptions::default(),
            )
            .await
            .expect_err("a fenced session is terminal");
        assert_eq!(error.code, ErrorCode::WriterFenced.as_str());
        assert!(
            error.message.contains("was fenced by epoch"),
            "{}",
            error.message
        );
        assert_eq!(
            error.message.matches("session `wrs_").count(),
            2,
            "both sessions named: {}",
            error.message
        );

        let missing = rival
            .backend
            .stat_path(&NamespacePath::parse("demo", "/three.txt").expect("namespace path"))
            .await
            .expect_err("the fenced put committed nothing");
        assert_eq!(missing.code, ErrorCode::PathNotFound.as_str());
    }
}
