use super::*;
use crate::status::{CacheStatus, MountMethod, Readiness, ReflinkSupport, Transports};

fn namespace_id() -> NamespaceId {
    NamespaceId::parse("demo").expect("a namespace ID")
}

fn path() -> AbsolutePath {
    AbsolutePath::parse("/work/project").expect("an absolute path")
}

fn status() -> Status {
    Status {
        namespace_id: namespace_id(),
        path: path(),
        access: MountAccess::ReadOnly,
        readiness: Readiness::Ready,
        mount_method: Some(MountMethod::MountSyscall),
        applied_seq: ChangeSeq(42),
        dirty_items: 0,
        oldest_unpublished_change_age_ms: None,
        pending_batch: None,
        uploads_in_flight: 0,
        cache: CacheStatus::default(),
        entries: 7,
        max_entries: 100_000,
        reflinks: ReflinkSupport::Unsupported,
        inline_limit_bytes: 0,
        transports: Transports::default(),
        conflicts: Vec::new(),
        refused_publications: Vec::new(),
        publication_stop: None,
        warnings: Vec::new(),
        feed_error: None,
        dropped_invalidations: 0,
    }
}

fn mount_request() -> MountRequest {
    MountRequest {
        directory: PathBuf::from("/mnt/project"),
        namespace_id: namespace_id(),
        path: path(),
        access: MountAccess::ReadOnly,
        state_directory: PathBuf::from("/home/ada/.local/state/loonfs"),
        options: MountOptions {
            max_entries: Some(500),
            poll_interval_ms: Some(100),
            uid: Some(1000),
            ..MountOptions::default()
        },
        profile: DaemonProfile {
            server_url: "http://127.0.0.1:8080".to_owned(),
            auth_token: Some(SecretString::new("token")),
            ca_cert_path: None,
            actor_id: ActorId::parse("loonfs-cli").expect("an actor ID"),
            subject: Subject::from_parts(
                Some("corp"),
                Some("ada"),
                Some("loonfs-cli"),
                Some("ada"),
            )
            .expect("a subject"),
        },
    }
}

/// Checks that every request and response survives one line of the
/// protocol, as a request that the daemon reads or a response that a
/// command reads.
#[test]
fn every_message_round_trips_through_one_line() {
    let directory = PathBuf::from("/mnt/project");
    let requests = [
        DaemonRequest::Mount(Box::new(mount_request())),
        DaemonRequest::Unmount(UnmountRequest {
            directory: directory.clone(),
            force: true,
        }),
        DaemonRequest::Sync(DirectoryRequest {
            directory: directory.clone(),
        }),
        DaemonRequest::Refresh(RefreshRequest {
            directory: directory.clone(),
            at_least: Some(ChangeSeq(9)),
        }),
        DaemonRequest::Status(StatusRequest { directory: None }),
        DaemonRequest::List,
        DaemonRequest::Shutdown,
    ];
    assert_eq!(requests.len(), DaemonRequest::OPERATIONS.len());
    for request in requests {
        let line = encode_line(&request);
        let encoded: serde_json::Value = serde_json::from_slice(&line).expect("a JSON line");
        let operation = encoded["op"].as_str().expect("an operation");
        assert!(
            DaemonRequest::OPERATIONS.contains(&operation),
            "{operation}"
        );
        assert_eq!(line.last(), Some(&b'\n'));
        assert_eq!(
            line.iter().filter(|&&byte| byte == b'\n').count(),
            1,
            "a message is one line"
        );
        assert_eq!(decode_request(&line), Ok(request));
    }

    let report = MountStatusReport {
        directory: directory.clone(),
        status: status(),
    };
    let responses = [
        DaemonResponse::Progress {
            step: MountStep::Admitting,
        },
        DaemonResponse::Mounted(Box::new(MountedReport {
            outcome: MountOutcome::AlreadyMounted,
            mount: report.clone(),
        })),
        DaemonResponse::Unmounted(UnmountedReport {
            directory: directory.clone(),
            outcome: UnmountOutcome::Unmounted {
                state: StateDisposal::RemovedAfterRelease,
            },
            state_directory: PathBuf::from("/state/mounts/1"),
        }),
        DaemonResponse::Refreshed {
            applied_seq: ChangeSeq(9),
        },
        DaemonResponse::Statuses {
            mounts: vec![report],
        },
        DaemonResponse::Mounts {
            mounts: vec![MountListing {
                directory,
                namespace_id: namespace_id(),
                path: path(),
                phase: MountPhase::Releasing,
            }],
        },
        DaemonResponse::ShuttingDown,
        DaemonResponse::Error(DaemonFailure::new(
            DaemonErrorCode::DirectoryBusy,
            "directory is busy",
        )),
    ];
    for response in responses {
        let line = encode_line(&response);
        assert_eq!(
            decode_response(&line).expect("the response decodes"),
            response
        );
    }
}

/// Checks that the token reaches the daemon, because the daemon builds the
/// mount's client from it, and that `Debug` still redacts it.
#[test]
fn a_mount_request_includes_the_token_and_debug_redacts_it() {
    let request = DaemonRequest::Mount(Box::new(mount_request()));
    let line = String::from_utf8(encode_line(&request)).expect("a UTF-8 line");
    assert!(line.contains(r#""auth_token":"token""#), "{line}");
    assert!(!format!("{request:?}").contains("\"token\""));
}

/// Checks that an `op` that no request has is an unknown operation, and that
/// a known `op` with bad fields is an invalid request.
#[test]
fn an_unknown_operation_is_reported_apart_from_an_invalid_request() {
    let unknown = decode_request(br#"{"op":"remount","directory":"/mnt"}"#)
        .expect_err("an unknown operation");
    assert_eq!(unknown.code, DaemonErrorCode::UnknownOperation);
    assert!(
        unknown.message.contains("\"remount\""),
        "{}",
        unknown.message
    );

    for line in [
        &br#"{"op":"unmount","directory":"/mnt","force":true,"lazy":true}"#[..],
        br#"{"op":"refresh"}"#,
        br#"{"directory":"/mnt"}"#,
        b"not json",
    ] {
        let invalid = decode_request(line).expect_err("an invalid request");
        assert_eq!(invalid.code, DaemonErrorCode::InvalidRequest, "{line:?}");
    }
}

/// Checks that a command reads a failure code from a newer daemon as
/// `unknown`, instead of failing to read the response.
#[test]
fn a_failure_code_from_a_newer_daemon_decodes_as_unknown() {
    let response =
        decode_response(br#"{"kind":"error","code":"quota_exceeded","message":"too many"}"#)
            .expect("the response decodes");
    assert_eq!(
        response,
        DaemonResponse::Error(DaemonFailure::new(DaemonErrorCode::Unknown, "too many"))
    );
}

/// Checks that options left out keep the engine's defaults.
#[test]
fn mount_options_override_only_the_settings_they_set() {
    let defaults = MountConfig::new(
        namespace_id(),
        path(),
        MountAccess::ReadOnly,
        PathBuf::from("/state"),
    );
    let mut config = defaults.clone();
    mount_request().options.apply(&mut config);
    assert_eq!(config.max_entries, 500);
    assert_eq!(config.poll_interval_ms, 100);
    assert_eq!(config.uid, Some(1000));
    assert_eq!(config.gid, None);
    assert_eq!(config.max_cache_bytes, defaults.max_cache_bytes);
    assert_eq!(config.min_free_bytes, defaults.min_free_bytes);
}
