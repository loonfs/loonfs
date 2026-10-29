//! The server config file's decoding and checks, through the public
//! loaders.

use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs_server::{
    load_server_config, JobMode, MetadataCacheConfig, MetadataCacheSsdConfig, ReadConsistency,
    ServerConfig, ServerError, StoreConfig, TlsServerConfig, TransportConfig,
};
use loonfs_test_support::EnvGuard;
use loonfs_types::config::ConfigFieldError;
use loonfs_types::env::AUTH_TOKEN_ENV;
use std::fs;
use std::num::NonZeroUsize;
use tempfile::tempdir;

#[test]
fn ambient_credential_sources_survive_loading_with_environment_credentials_set() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"

[store.credentials]
kind = "ambient"
"#,
    );
    let access_key = EnvGuard::set("AWS_ACCESS_KEY_ID", "parity-access");
    let secret_key = EnvGuard::set("AWS_SECRET_ACCESS_KEY", "parity-secret");
    let config = load_server_config(&path)
        .expect("load server config")
        .config()
        .clone();
    drop((access_key, secret_key));
    assert_eq!(config.store.credentials_kind(), Some("ambient"));
}

#[test]
fn every_maintenance_mode_names_its_serving_and_scheduling_behavior() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let config = load_server_config(&path)
        .expect("valid config")
        .config()
        .clone();
    assert_eq!(config.maintenance, JobMode::ServeAndMaintain);
    assert!(config.maintenance.serves());
    assert!(config.maintenance.maintains());

    for (spelling, mode, serves, maintains) in [
        ("disabled", JobMode::Disabled, false, false),
        ("serve_only", JobMode::ServeOnly, true, false),
        ("maintain_only", JobMode::MaintainOnly, false, true),
        ("serve_and_maintain", JobMode::ServeAndMaintain, true, true),
    ] {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
maintenance = "{spelling}"

[grep]
mode = "{spelling}"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));
        let config = load_server_config(&path)
            .expect("valid config")
            .config()
            .clone();
        assert_eq!(config.maintenance, mode);
        assert_eq!(config.grep.mode, mode);
        assert_eq!(config.maintenance.serves(), serves);
        assert_eq!(config.maintenance.maintains(), maintains);
    }
}

#[test]
fn an_unknown_maintenance_word_is_rejected() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
maintenance = "sometimes"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let error = load_server_config(&path).expect_err("unknown mode must not load");
    match error {
        ServerError::ConfigDecode(message) => {
            assert!(message.contains("serve_and_maintain"), "{message}");
            assert!(message.contains("serve_only"), "{message}");
            assert!(message.contains("maintain_only"), "{message}");
            assert!(message.contains("disabled"), "{message}");
        }
        other => panic!("expected decode error naming the modes, got {other:?}"),
    }
}

#[test]
fn load_rejects_invalid_bind() {
    let path = write_config(
        r#"
bind = "bad-bind"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let error = load_server_config(&path).expect_err("invalid bind");

    assert_invalid_field(error, "bind");
}

#[test]
fn load_rejects_blank_writer_id() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "   "

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let error = load_server_config(&path).expect_err("blank writer fields");

    let ServerError::ConfigDecode(message) = &error else {
        panic!("expected a decode error for the blank writer id, got {error:?}");
    };
    assert!(message.contains("must not be blank"), "{message}");
}

#[test]
fn load_rejects_blank_provider_required_fields() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "cloudflare_r2"
bucket = " "
account_id = "account"
endpoint_url = "https://example.com"

[store.credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
    );

    let error = load_server_config(&path).expect_err("blank bucket");

    assert_missing_field(error, "store.bucket");
}

#[test]
fn load_rejects_invalid_endpoint_urls() {
    let aws_path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "ftp://example.com"
key_prefix = "demo"
force_path_style = false

[store.credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
    );
    let r2_path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "not a url"
key_prefix = "demo"

[store.credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
    );
    let aws_error = load_server_config(&aws_path).expect_err("invalid aws endpoint");
    let r2_error = load_server_config(&r2_path).expect_err("invalid r2 endpoint");

    assert_invalid_field(aws_error, "store.endpoint_url");
    assert_invalid_field(r2_error, "store.endpoint_url");
}

#[test]
fn load_rejects_blank_gcs_bucket() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "gcp_gcs"
bucket = " "
key_prefix = "demo"

[store.credentials]
kind = "service_account_file"
path = "/tmp/service-account.json"
"#,
    );

    let error = load_server_config(&path).expect_err("blank gcs bucket");

    assert_missing_field(error, "store.bucket");
}

#[test]
fn load_rejects_a_blank_bearer_token() {
    // Prevent an ambient token from filling the blank value.
    let _auth_token = EnvGuard::unset(AUTH_TOKEN_ENV);
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[auth]
kind = "bearer_token"
token = "   "

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let error = load_server_config(&path).expect_err("blank bearer token");

    assert_missing_field(error, "auth.token");
}

#[test]
fn load_rejects_non_loopback_bind_without_an_auth_table() {
    // Remove any ambient token so these configs remain invalid.
    let _auth_token = EnvGuard::unset(AUTH_TOKEN_ENV);
    for bind in ["0.0.0.0:9400", "[::]:9400", "10.1.2.3:9400"] {
        let path = write_config(&format!(
            r#"
bind = "{bind}"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));

        let error = load_server_config(&path).expect_err("open network bind");

        assert_invalid_field(error, "auth");
    }
}

#[test]
fn an_open_auth_table_permits_an_open_bind() {
    let path = write_config(
        r#"
bind = "0.0.0.0:9400"
writer_id = "loonfs-server"

[auth]
kind = "open"

[transport]
kind = "plaintext_behind_proxy"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    load_server_config(&path).expect("explicitly-open config loads");
}

#[test]
fn loopback_bind_without_an_auth_table_is_allowed() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    load_server_config(&path).expect("loopback-only config loads");
}

#[test]
fn load_rejects_non_loopback_bind_without_a_transport_table() {
    for bind in ["0.0.0.0:9400", "[::]:9400", "10.1.2.3:9400"] {
        let path = write_config(&format!(
            r#"
bind = "{bind}"
writer_id = "loonfs-server"

[auth]
kind = "bearer_token"
token = "dev-token"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));

        let error = load_server_config(&path).expect_err("plaintext network bind");

        assert_invalid_field(error, "transport");
    }
}

#[test]
fn a_proxy_transport_permits_a_plaintext_network_bind() {
    let path = write_config(
        r#"
bind = "0.0.0.0:9400"
writer_id = "loonfs-server"

[auth]
kind = "bearer_token"
token = "dev-token"

[transport]
kind = "plaintext_behind_proxy"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    load_server_config(&path).expect("proxy-terminated config loads");
}

#[test]
fn tls_satisfies_the_network_bind_requirement() {
    let path = write_config(
        r#"
bind = "0.0.0.0:9400"
writer_id = "loonfs-server"

[auth]
kind = "bearer_token"
token = "dev-token"

[transport]
kind = "tls"
cert_path = "/etc/loonfs/tls/server.crt"
key_path = "/etc/loonfs/tls/server.key"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let config = load_server_config(&path)
        .expect("tls-terminating config loads")
        .config()
        .clone();

    assert_eq!(
        config.transport,
        Some(TransportConfig::Tls(TlsServerConfig {
            cert_path: "/etc/loonfs/tls/server.crt".to_owned(),
            key_path: "/etc/loonfs/tls/server.key".to_owned(),
        }))
    );
}

#[test]
fn load_rejects_blank_tls_paths() {
    for (cert_path, key_path, field) in [
        (" ", "/etc/loonfs/tls/server.key", "transport.cert_path"),
        ("/etc/loonfs/tls/server.crt", "", "transport.key_path"),
    ] {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[transport]
kind = "tls"
cert_path = "{cert_path}"
key_path = "{key_path}"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));

        let error = load_server_config(&path).expect_err("blank tls path");

        assert_missing_field(error, field);
    }
}

#[test]
fn load_rejects_unknown_tls_keys() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[transport]
kind = "tls"
cert_path = "/etc/loonfs/tls/server.crt"
key_path = "/etc/loonfs/tls/server.key"
client_ca_path = "/etc/loonfs/tls/clients.crt"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    match load_server_config(&path).expect_err("unknown tls key") {
        ServerError::ConfigDecode(message) => assert!(
            message.contains("client_ca_path"),
            "decode error must name the unknown key, got: {message}"
        ),
        other => panic!("expected a decode error, got {other:?}"),
    }
}

#[test]
fn max_upload_bytes_defaults_and_rejects_zero() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let config = load_server_config(&path)
        .expect("valid config")
        .config()
        .clone();
    assert_eq!(config.max_upload_bytes, 256 * 1024 * 1024);

    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
max_upload_bytes = 0

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let error = load_server_config(&path).expect_err("zero upload limit");
    assert_invalid_field(error, "max_upload_bytes");
}

#[test]
fn request_and_shutdown_deadlines_default_and_reject_zero() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let config = load_server_config(&path)
        .expect("valid config")
        .config()
        .clone();
    assert_eq!(config.request_deadline_ms, 60_000);
    assert_eq!(config.shutdown_deadline_ms, 600_000);

    for field in ["request_deadline_ms", "shutdown_deadline_ms"] {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
{field} = 0

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));
        let error = load_server_config(&path).expect_err("zero deadline must be rejected");
        assert_invalid_field(error, field);
    }
}

#[test]
fn transfer_bounds_default_and_reject_zero() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let config = load_server_config(&path)
        .expect("valid config")
        .config()
        .clone();
    assert_eq!(config.max_download_bytes, 256 * 1024 * 1024);
    assert_eq!(
        config.max_concurrent_reads.get(),
        loonfs::DEFAULT_MAX_CONCURRENT_READS
    );
    assert_eq!(
        config.max_concurrent_folds.get(),
        loonfs::DEFAULT_MAX_CONCURRENT_FOLDS
    );
    assert_eq!(
        config.max_concurrent_compactions.get(),
        loonfs::DEFAULT_MAX_CONCURRENT_COMPACTIONS
    );
    assert_eq!(
        config.max_concurrent_content_writes.get(),
        loonfs::DEFAULT_MAX_CONCURRENT_CONTENT_WRITES
    );
    for field in ["max_in_flight_requests", "max_connections"] {
        let mut oversized = config.clone();
        let value = NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS + 1)
            .expect("a bound past the permit limit is nonzero");
        match field {
            "max_in_flight_requests" => oversized.max_in_flight_requests = value,
            _ => oversized.max_connections = value,
        }
        assert_invalid_field(oversized.validate().expect_err("oversized bound"), field);
    }
    assert_eq!(config.max_in_flight_requests.get(), 256);
    assert_eq!(config.max_connections.get(), 1024);
    assert_eq!(config.max_concurrent_uploads.get(), 8);
    assert_eq!(config.max_concurrent_downloads.get(), 16);
    assert_eq!(config.max_concurrent_maintenance.get(), 8);
    assert_eq!(config.tick_interval_ms, 5_000);
    assert_eq!(config.maintenance_interval_ms, 300_000);
    assert_eq!(config.gc_interval_ms, 3_600_000);
    assert_eq!(config.full_sweep_interval_ms, 86_400_000);
    assert_eq!(config.idle_session_close_after_ms, 1_800_000);

    for field in [
        "max_download_bytes",
        "tick_interval_ms",
        "maintenance_interval_ms",
        "gc_interval_ms",
        "full_sweep_interval_ms",
        "idle_session_close_after_ms",
        "max_merge_input_bytes",
    ] {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
{field} = 0

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));
        let error = load_server_config(&path).expect_err("zero bound must be rejected");
        assert_invalid_field(error, field);
    }
    for field in [
        "max_concurrent_reads",
        "max_in_flight_requests",
        "max_connections",
        "max_concurrent_folds",
        "max_concurrent_compactions",
        "max_concurrent_content_writes",
        "max_concurrent_uploads",
        "max_concurrent_downloads",
        "max_concurrent_maintenance",
    ] {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
{field} = 0

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));
        match load_server_config(&path).expect_err("a zero count must be rejected") {
            ServerError::ConfigDecode(message) => assert!(
                message.contains(field) && message.contains("expected a nonzero usize"),
                "the decode error should identify `{field}` and say why: {message}"
            ),
            other => panic!("expected a decode error for `{field}`, got {other:?}"),
        }
    }

    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[grep]
mode = "serve_and_maintain"
max_content_bytes_per_step = 0

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let error = load_server_config(&path).expect_err("zero grep bound must be rejected");
    assert_invalid_field(error, "grep");
}

#[test]
fn idle_fold_period_defaults_to_fifteen_minutes_and_accepts_zero() {
    for (setting, expected) in [
        ("", 900_000),
        ("idle_fold_after_ms = 60000", 60_000),
        ("idle_fold_after_ms = 0", 0),
    ] {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
{setting}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));
        let config = load_server_config(&path)
            .expect("valid config")
            .config()
            .clone();
        assert_eq!(config.idle_fold_after_ms, expected, "{setting:?}");
    }
}

#[test]
fn a_retention_window_is_optional_and_must_be_positive() {
    let config = |setting: &str| {
        write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
{setting}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ))
    };
    for (setting, expected) in [
        ("", None),
        (
            "retention_window_ms = 2592000000",
            std::num::NonZeroU64::new(2_592_000_000),
        ),
    ] {
        let loaded = load_server_config(config(setting))
            .expect("valid config")
            .config()
            .clone();
        assert_eq!(loaded.retention_window_ms, expected, "{setting:?}");
    }
    match load_server_config(config("retention_window_ms = 0"))
        .expect_err("a zero window must be rejected")
    {
        ServerError::ConfigDecode(message) => assert!(
            message.contains("retention_window_ms") && message.contains("expected a nonzero u64"),
            "the decode error should identify `retention_window_ms` and say why: {message}"
        ),
        other => panic!("expected a decode error, got {other:?}"),
    }
}

#[test]
fn server_config_debug_redacts_secrets() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
content_token_secret = "debug-content-token-secret"
writer_id = "loonfs-server"

[auth]
kind = "bearer_token"
token = "debug-auth-token"

[store]
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"
key_prefix = "demo"
force_path_style = false

[store.credentials]
kind = "static"
access_key_id = "debug-access-key-id"
secret_access_key = "debug-secret-access-key"
session_token = "debug-session-token"
"#,
    );
    let config = load_server_config(&path)
        .expect("load config")
        .config()
        .clone();

    let rendered = format!("{config:?}");

    assert!(!rendered.contains("debug-auth-token"));
    assert!(!rendered.contains("debug-content-token-secret"));
    assert!(!rendered.contains("debug-access-key-id"));
    assert!(!rendered.contains("debug-secret-access-key"));
    assert!(!rendered.contains("debug-session-token"));
}

#[test]
fn static_store_credentials_are_preserved_as_static() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "aws_s3"
bucket = "bucket"
region = "us-east-1"

[store.credentials]
kind = "static"
access_key_id = "file-access"
secret_access_key = "file-secret"
"#,
    );

    let config = load_server_config(&path)
        .expect("load config")
        .config()
        .clone();
    match config.store {
        StoreConfig::AwsS3(store) => {
            let loonfs_objectstore::AwsS3Credentials::Static {
                access_key_id,
                secret_access_key,
                ..
            } = store.credentials
            else {
                panic!("expected static credentials")
            };
            assert_eq!(access_key_id.expose(), "file-access");
            assert_eq!(secret_access_key.expose(), "file-secret");
        }
        other => panic!("expected an aws-s3 store, got {other:?}"),
    }
}

#[test]
fn load_rejects_unknown_keys_at_every_level() {
    let top_level = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
lease_duration = 60000

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let retired_level = write_config(
        r#"
bind = "0.0.0.0:9400"
writer_id = "loonfs-server"
allow_remote_without_tls = true

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let store_level = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
key_prefiks = "typo"
"#,
    );
    let metadata_cache_level = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[metadata_cache]
max_wal_tail_byte = 2

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );
    let grep_level = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[grep]
mode = "serve_and_maintain"
max_files_per_stepp = 3

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    for (path, typo) in [
        (top_level, "lease_duration"),
        (retired_level, "allow_remote_without_tls"),
        (store_level, "key_prefiks"),
        (metadata_cache_level, "max_wal_tail_byte"),
        (grep_level, "max_files_per_stepp"),
    ] {
        let error = load_server_config(&path).expect_err("typo'd key must be rejected");
        match error {
            ServerError::ConfigDecode(message) => {
                assert!(
                    message.contains(typo),
                    "decode error must name `{typo}`, got: {message}"
                );
            }
            other => panic!("expected decode error naming {typo}, got {other:?}"),
        }
    }
}

/// Checks that a manifest check interval or a maximum staleness above the
/// revalidation bound fails to load and fails `validate`, and that the
/// bound itself loads.
#[test]
fn read_intervals_above_the_revalidation_bound_are_rejected() {
    let bound = READ_REVALIDATION_BOUND_MS;
    for field in ["manifest_revalidation_interval_ms", "max_staleness_ms"] {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
{field} = {}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
            bound + 1
        ));
        let error = load_server_config(&path).expect_err("a setting above the bound");
        assert_invalid_field(error, field);
    }

    let path = write_config(&format!(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
manifest_revalidation_interval_ms = {bound}
max_staleness_ms = {bound}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
    ));
    let config = load_server_config(&path)
        .expect("the bound itself loads")
        .config()
        .clone();
    assert_eq!(config.manifest_revalidation_interval_ms, Some(bound));
    assert_eq!(config.max_staleness_ms, Some(bound));
    for field in ["manifest_revalidation_interval_ms", "max_staleness_ms"] {
        let mut oversized = config.clone();
        match field {
            "manifest_revalidation_interval_ms" => {
                oversized.manifest_revalidation_interval_ms = Some(bound + 1)
            }
            _ => oversized.max_staleness_ms = Some(bound + 1),
        }
        assert_invalid_field(
            oversized.validate().expect_err("a setting above the bound"),
            field,
        );
    }
}

#[test]
fn load_uses_default_metadata_cache_when_omitted() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let config = load_server_config(&path)
        .expect("load config")
        .config()
        .clone();
    assert_eq!(config.metadata_cache, MetadataCacheConfig::default());
    assert_eq!(config.manifest_revalidation_interval_ms, None);
    assert_eq!(config.max_staleness_ms, None);
    assert_eq!(config.consistency, ReadConsistency::Eventual);
    assert_eq!(config.max_read_working_bytes, None);
}

#[test]
fn load_applies_metadata_cache_and_read_settings() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
manifest_revalidation_interval_ms = 250
max_staleness_ms = 5000
consistency = "strong"
max_read_working_bytes = 8192

[metadata_cache]
max_segment_bytes = 16384
max_manifest_bytes = 2048
max_head_bytes = 1024
max_wal_tail_bytes = 4096
max_content_object_bytes = 3072

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let config = load_server_config(&path)
        .expect("load config")
        .config()
        .clone();
    assert_eq!(config.manifest_revalidation_interval_ms, Some(250));
    assert_eq!(config.max_staleness_ms, Some(5000));
    assert_eq!(config.consistency, ReadConsistency::Strong);
    assert_eq!(config.max_read_working_bytes, Some(8192));
    assert_eq!(
        config.metadata_cache,
        MetadataCacheConfig {
            max_segment_bytes: Some(16384),
            max_manifest_bytes: Some(2048),
            max_head_bytes: Some(1024),
            max_wal_tail_bytes: Some(4096),
            max_content_object_bytes: Some(3072),
            ssd: None,
        }
    );
}

#[test]
fn load_accepts_zero_metadata_cache_limits() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[metadata_cache]
max_segment_bytes = 0
max_manifest_bytes = 0
max_head_bytes = 0
max_wal_tail_bytes = 0
max_content_object_bytes = 0

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let config = load_server_config(&path)
        .expect("load config")
        .config()
        .clone();
    assert_eq!(
        config.metadata_cache,
        MetadataCacheConfig {
            max_segment_bytes: Some(0),
            max_manifest_bytes: Some(0),
            max_head_bytes: Some(0),
            max_wal_tail_bytes: Some(0),
            max_content_object_bytes: Some(0),
            ssd: None,
        }
    );
}

/// Checks that a config without `[metadata_cache.ssd]` has no SSD tier,
/// that the table needs both of its settings, and that the server
/// rejects a zero budget and an empty directory.
#[test]
fn the_ssd_table_is_optional_and_needs_both_settings() {
    let config = |ssd: &str| {
        write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

{ssd}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ))
    };
    let absent = load_server_config(config(""))
        .expect("no ssd table")
        .config()
        .clone();
    assert_eq!(absent.metadata_cache.ssd, None);

    let present = load_server_config(config(
        "[metadata_cache.ssd]\ndirectory = \"/var/lib/loonfs/ssd\"\nmax_segment_bytes = 1073741824",
    ))
    .expect("an ssd table with both settings")
    .config()
    .clone();
    assert_eq!(
        present.metadata_cache.ssd,
        Some(MetadataCacheSsdConfig {
            directory: "/var/lib/loonfs/ssd".to_owned(),
            max_segment_bytes: NonZeroUsize::new(1 << 30).expect("nonzero"),
        })
    );

    for (ssd, field, reason) in [
        (
            "max_segment_bytes = 1073741824",
            "directory",
            "missing field",
        ),
        ("directory = \"/ssd\"", "max_segment_bytes", "missing field"),
        (
            "directory = \"/ssd\"\nmax_segment_bytes = 0",
            "max_segment_bytes",
            "expected a nonzero usize",
        ),
    ] {
        match load_server_config(config(&format!("[metadata_cache.ssd]\n{ssd}"))) {
            Err(ServerError::ConfigDecode(message)) => assert!(
                message.contains(field) && message.contains(reason),
                "the decode error should identify `{field}` and say why: {message}"
            ),
            other => panic!("expected a decode error for `{field}`, got {other:?}"),
        }
    }
    let empty = config("[metadata_cache.ssd]\ndirectory = \" \"\nmax_segment_bytes = 1");
    assert_missing_field(
        load_server_config(empty).expect_err("an empty directory"),
        "metadata_cache.ssd.directory",
    );
}

#[test]
fn an_omitted_grep_table_composes_no_grep() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let config = load_server_config(&path)
        .expect("load config")
        .config()
        .clone();
    assert_eq!(config.grep.mode, JobMode::Disabled);
}

#[test]
fn a_grep_table_without_a_mode_is_rejected() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[grep]

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let error = load_server_config(&path).expect_err("mode is required");
    assert!(error.to_string().contains("mode"), "{error}");
}

#[test]
fn the_grep_step_limit_defaults_to_two_and_refuses_zero() {
    let load = |setting: &str| {
        load_server_config(write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[grep]
mode = "serve_and_maintain"
{setting}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        )))
    };

    let default = load("").expect("load the default step limit");
    assert_eq!(default.config().grep.worker.max_concurrent_steps, 2);
    let configured = load("max_concurrent_steps = 5").expect("load a step limit");
    assert_eq!(configured.config().grep.worker.max_concurrent_steps, 5);
    match load("max_concurrent_steps = 0") {
        Err(ServerError::Config(ConfigFieldError::Invalid {
            field: "grep",
            reason,
        })) => assert!(reason.contains("max_concurrent_steps"), "{reason}"),
        other => panic!("expected a zero step limit to be refused, got {other:?}"),
    }
}

#[test]
fn load_applies_grep_mode_and_policy() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[grep]
mode = "serve_only"
max_files_per_step = 1000
max_content_bytes_per_step = 536870912

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let grep = load_server_config(&path)
        .expect("load config")
        .config()
        .grep;
    assert_eq!(grep.mode, JobMode::ServeOnly);
    let policy = grep
        .worker
        .build_policy()
        .expect("valid configured grep policy");
    assert_eq!(policy.max_files_per_step.get(), 1000);
    assert_eq!(policy.max_content_bytes_per_step.get(), 536_870_912);
    let defaults = loonfs_grep::GramIndexBuildPolicy::default();
    assert_eq!(policy.max_rows_per_segment, defaults.max_rows_per_segment);
    assert_eq!(policy.max_delta_runs, defaults.max_delta_runs);
    assert_eq!(policy.max_mid_runs, defaults.max_mid_runs);
    assert_eq!(
        policy.max_decoded_input_rows_per_step,
        defaults.max_decoded_input_rows_per_step
    );
}

#[test]
fn grep_rejects_retired_engine_tuning_keys() {
    for key in [
        "max_rows_per_segment",
        "max_delta_runs",
        "max_mid_runs",
        "max_decoded_input_rows_per_step",
    ] {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
[grep]
mode = "serve_and_maintain"
{key} = 1
[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));
        let error = load_server_config(&path).expect_err("engine tuning keys must be rejected");
        assert!(error.to_string().contains(key), "{error}");
    }
}

#[test]
fn unknown_config_tables_fail_decode() {
    // Unknown tables fail through the config's strict parsing.
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[gram_index_build]
max_files_per_step = 4

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let error = load_server_config(&path).expect_err("unknown table must fail");
    match error {
        ServerError::ConfigDecode(message) => {
            assert!(message.contains("gram_index_build"), "{message}");
        }
        other => panic!("expected decode error, got {other:?}"),
    }
}

#[test]
fn load_rejects_negative_metadata_cache_limits_as_decode_error() {
    let path = write_config(
        r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[metadata_cache]
max_wal_tail_bytes = -1

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    );

    let error = load_server_config(&path).expect_err("negative byte limit");
    match error {
        ServerError::ConfigDecode(_) => {}
        other => panic!("expected decode error, got {other:?}"),
    }
}

#[test]
fn server_config_toml_round_trip_preserves_values() {
    let mut config: ServerConfig =
        toml::from_str(include_str!("../../config/local-fs.example.toml"))
            .expect("example config should parse");

    for threshold in [
        config.inline_content.inline_content_threshold_bytes,
        Some(0),
        None,
    ] {
        config.inline_content.inline_content_threshold_bytes = threshold;
        let serialized = toml::to_string(&config).expect("server config should serialize");
        let decoded: ServerConfig =
            toml::from_str(&serialized).expect("serialized server config should parse");
        assert_eq!(decoded, config);
    }
}

#[test]
fn server_example_configs_parse_and_validate() {
    let configs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config");
    let mut examples = 0usize;
    for entry in fs::read_dir(configs_dir).expect("read config directory") {
        let path = entry.expect("read config entry").path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.ends_with(".example.toml") {
            continue;
        }
        let contents = fs::read_to_string(&path).expect("read example config");
        let config: ServerConfig =
            toml::from_str(&contents).unwrap_or_else(|err| panic!("{name} must parse: {err}"));
        config
            .validate()
            .unwrap_or_else(|err| panic!("{name} must validate: {err}"));
        examples += 1;
    }
    assert!(
        examples >= 4,
        "expected at least 4 server example configs, found {examples}"
    );
}

fn write_config(contents: &str) -> std::path::PathBuf {
    let contents = if contents.contains("content_token_secret") {
        contents.to_owned()
    } else {
        contents.replacen(
            "writer_id",
            "content_token_secret = \"dev-content-token-secret\"\nwriter_id",
            1,
        )
    };
    write_config_verbatim(&contents)
}

fn write_config_verbatim(contents: &str) -> std::path::PathBuf {
    let temp_dir = tempdir().expect("tempdir");
    let path = temp_dir.path().join("server.toml");
    fs::write(&path, contents).expect("write config");
    let _ = temp_dir.keep();
    path
}

fn assert_invalid_field(error: ServerError, field: &'static str) {
    match error {
        ServerError::Config(ConfigFieldError::Invalid { field: actual, .. }) => {
            assert_eq!(actual, field)
        }
        other => panic!("expected invalid field error for {field}, got {other:?}"),
    }
}

fn assert_missing_field(error: ServerError, field: &'static str) {
    match error {
        ServerError::Config(ConfigFieldError::Missing { field: actual }) => {
            assert_eq!(actual, field)
        }
        other => panic!("expected missing field error for {field}, got {other:?}"),
    }
}
