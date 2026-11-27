//! Config tables and environment fallbacks that the loaders don't expose.

use super::{parse_server_config, AuthConfig, ServerConfig};
use crate::ServerError;
use loonfs_test_support::EnvGuard;
use loonfs_types::config::ConfigFieldError;
use loonfs_types::env::CONTENT_TOKEN_SECRET_ENV;
use loonfs_types::{SecretString, GENERATED_ID_BODY_BYTES, MAX_WRITER_ID_BYTES};

#[test]
fn inline_content_defaults_and_overrides_keep_the_threshold_a_number() {
    let defaults = loonfs::InlineContentPolicy::default();
    assert_eq!(defaults.inline_content_threshold_bytes, 64 * 1024);
    assert_eq!(defaults.inline_content_wal_object_budget_bytes, 1024 * 1024);
    assert_eq!(defaults.inline_content_tail_limit_bytes, 32 * 1024 * 1024);
    for (source, threshold) in [
        ("", 64 * 1024),
        ("inline_content_wal_object_budget_bytes = 1024", 64 * 1024),
        ("inline_content_threshold_bytes = 4096", 4096),
        ("inline_content_threshold_bytes = 0", 0),
    ] {
        let overrides: loonfs::InlineContentPolicy = toml::from_str(source).expect("overrides");
        assert_eq!(overrides.inline_content_threshold_bytes, threshold);
    }
    for source in [
        "inline_content_threshold_bytes = true",
        "inline_content_threshold_bytes = false",
    ] {
        toml::from_str::<loonfs::InlineContentPolicy>(source)
            .expect_err("a boolean threshold should be rejected");
    }
    toml::from_str::<loonfs::InlineContentPolicy>("inline_content_fold_at_bytes = 1")
        .expect_err("the fold trigger isn't a setting");
}

#[test]
fn publication_overrides_are_strict_and_positive() {
    let defaults: loonfs::PublicationPolicy = toml::from_str("").expect("empty overrides");
    assert_eq!(defaults, loonfs::PublicationPolicy::default());
    for field in [
        "max_requests_per_namespace",
        "max_estimated_bytes_per_namespace",
    ] {
        toml::from_str::<loonfs::PublicationPolicy>(&format!("{field} = 0"))
            .expect_err(&format!("`{field} = 0` should be rejected"));
    }
    toml::from_str::<loonfs::PublicationPolicy>("max_requsets = 10")
        .expect_err("a misspelled field should be rejected");
    let overrides: loonfs::PublicationPolicy =
        toml::from_str("max_requests_per_namespace = 3").expect("overrides");
    assert_eq!(overrides.max_requests_per_namespace.get(), 3);
    assert_eq!(
        overrides.max_estimated_bytes_per_namespace,
        loonfs::PublicationPolicy::default().max_estimated_bytes_per_namespace
    );
}

/// Checks that the write-stop wait is half the request deadline, so a
/// commit that waits for the whole write-stop wait returns
/// `maintenance_required` before the server cancels its request at the
/// deadline with `outcome_unknown`.
#[test]
fn the_write_stop_wait_is_half_the_request_deadline() {
    let config = parse_server_config(
        r#"
bind = "127.0.0.1:9400"
content_token_secret = "dev-content-token-secret"
writer_id = "loonfs-server"
request_deadline_ms = 10000

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    )
    .expect("valid config");
    assert_eq!(
        config
            .config()
            .runtime_config()
            .publication
            .write_stop_wait_budget_ms,
        5_000
    );
}

/// Checks that the write-stop wait isn't a setting of the `[publication]`
/// table.
#[test]
fn the_write_stop_wait_is_not_a_setting() {
    let error = parse_server_config(
        r#"
bind = "127.0.0.1:9400"
content_token_secret = "dev-content-token-secret"
writer_id = "loonfs-server"

[publication]
write_stop_wait_budget_ms = 1000

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    )
    .expect_err("an unknown publication key");
    assert!(
        error.to_string().contains("write_stop_wait_budget_ms"),
        "{error}"
    );
}

/// Checks that an absent fold age takes the runtime's default, that a set
/// one reaches the runtime, and that zero and a fold age at
/// `MAX_TAIL_COMMIT_AGE_MS` are refused.
#[test]
fn the_runtime_takes_the_fold_age_or_its_default_below_the_tail_commit_age_bound() {
    let config = |setting: &str| {
        parse_server_config(&format!(
            r#"
bind = "127.0.0.1:9400"
content_token_secret = "dev-content-token-secret"
writer_id = "loonfs-server"
{setting}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ))
    };
    let bound = loonfs::MAX_TAIL_COMMIT_AGE_MS;
    for (setting, expected) in [
        ("".to_owned(), loonfs::DEFAULT_FOLD_AGE_MS.get()),
        ("fold_age_ms = 60000".to_owned(), 60_000),
        (format!("fold_age_ms = {}", bound - 1), bound - 1),
    ] {
        assert_eq!(
            config(&setting)
                .expect("valid config")
                .config()
                .runtime_config()
                .fold_age_ms
                .get(),
            expected,
            "{setting:?}"
        );
    }
    let error = config("fold_age_ms = 0").expect_err("a zero fold age");
    assert!(
        matches!(&error, ServerError::ConfigDecode(message) if message.contains("fold_age_ms")),
        "{error:?}"
    );
    assert_invalid_field(
        config(&format!("fold_age_ms = {bound}")).expect_err("a fold age at the bound"),
        "fold_age_ms",
    );
}

/// Checks that a server's writer ID is the configured `writer_id`, a hyphen,
/// and an ID that each validation generates, so two servers started from one
/// config have different writer IDs, and that a `writer_id` too long to take
/// the suffix fails validation.
#[test]
fn a_server_writer_id_is_the_configured_prefix_and_a_generated_suffix() {
    let source = |writer_id: &str| {
        format!(
            r#"
bind = "127.0.0.1:9400"
content_token_secret = "dev-content-token-secret"
writer_id = "{writer_id}"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        )
    };
    let [first, second] =
        [(); 2].map(|()| parse_server_config(&source("loonfs-server")).expect("valid config"));
    for config in [&first, &second] {
        let suffix = config
            .writer_id
            .as_str()
            .strip_prefix("loonfs-server-srv_")
            .expect("the writer ID starts with the configured prefix");
        assert_eq!(suffix.len(), GENERATED_ID_BODY_BYTES, "{suffix}");
        assert_eq!(config.config().writer_id.as_str(), "loonfs-server");
        assert_eq!(
            config.maintenance_writer_id.as_str(),
            format!("{}-maintenance", config.writer_id)
        );
    }
    assert_ne!(first.writer_id, second.writer_id);

    // The self-hosting guide states this limit.
    let longest_prefix =
        MAX_WRITER_ID_BYTES - "-srv_".len() - GENERATED_ID_BODY_BYTES - "-maintenance".len();
    assert_eq!(longest_prefix, 213);
    parse_server_config(&source(&"w".repeat(longest_prefix))).expect("the longest prefix");
    let too_long = "w".repeat(longest_prefix + 1);
    match parse_server_config(&source(&too_long)).expect_err("a prefix too long for the suffix") {
        ServerError::Config(ConfigFieldError::Invalid { field, reason }) => {
            assert_eq!(field, "writer_id");
            assert!(
                reason.starts_with(&format!("{too_long:?} must be 213 bytes or fewer")),
                "{reason}"
            );
        }
        other => panic!("expected an invalid `writer_id`, got {other:?}"),
    }
}

#[test]
fn env_fallbacks_fill_blank_or_unset_secrets() {
    let mut config: ServerConfig = toml::from_str(
        r#"
bind = "127.0.0.1:9400"
content_token_secret = "file-content-token-secret"
writer_id = "loonfs-server"

[auth]
kind = "bearer_token"
token = "file-auth-token"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
    )
    .expect("parse config");
    let env = |config: &mut ServerConfig| {
        config.apply_env_fallbacks(
            Some("env-auth-token".to_owned()),
            Some("env-content-token-secret".to_owned()),
        );
    };
    let bearer_token = |token: &str| {
        Some(AuthConfig::BearerToken {
            token: SecretString::new(token.to_owned()),
        })
    };

    // File values win over the environment.
    env(&mut config);
    assert_eq!(config.auth, bearer_token("file-auth-token"));
    assert_eq!(
        config.content_token_secret.expose(),
        "file-content-token-secret"
    );

    // The environment fills a token and a secret that the file left out or
    // left blank.
    for auth in [None, bearer_token("   ")] {
        config.auth = auth;
        config.content_token_secret = SecretString::new("   ".to_owned());
        env(&mut config);
        assert_eq!(config.auth, bearer_token("env-auth-token"));
        assert_eq!(
            config.content_token_secret.expose(),
            "env-content-token-secret"
        );
    }

    // An open server takes no token from the environment.
    config.auth = Some(AuthConfig::Open);
    env(&mut config);
    assert_eq!(config.auth, Some(AuthConfig::Open));

    // Blank environment values are ignored.
    config.auth = None;
    config.content_token_secret = SecretString::default();
    config.apply_env_fallbacks(Some("   ".to_owned()), Some(String::new()));
    assert!(config.auth.is_none());
    assert!(config.content_token_secret.expose().is_empty());
}

#[test]
fn load_accepts_config_without_content_token_secret_field() {
    // `content_token_secret` may come from LOONFS_CONTENT_TOKEN_SECRET
    // instead of the file; omitting both must still fail validation.
    let source = r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#;

    let mut config: ServerConfig =
        toml::from_str(source).expect("config without content_token_secret parses");
    assert!(config.content_token_secret.expose().is_empty());

    config.apply_env_fallbacks(None, Some("env-content-token-secret".to_owned()));
    assert_eq!(
        config.content_token_secret.expose(),
        "env-content-token-secret"
    );

    // Without the env fallback the load path reports the missing field.
    let _content_token_secret = EnvGuard::unset(CONTENT_TOKEN_SECRET_ENV);
    match parse_server_config(source).expect_err("missing content token secret") {
        ServerError::Config(ConfigFieldError::Missing { field }) => {
            assert_eq!(field, "content_token_secret");
        }
        other => panic!("expected a missing content_token_secret, got {other:?}"),
    }
}

/// Checks that each invalid inline content setting fails validation, and
/// that the error identifies the setting by its path in the config file.
#[test]
fn validate_rejects_invalid_inline_content_settings() {
    for setting in [
        "inline_content_threshold_bytes",
        "inline_content_wal_object_budget_bytes",
        "inline_content_tail_limit_bytes",
    ] {
        let value = match setting {
            "inline_content_threshold_bytes" => {
                loonfs_types::format::wal::MAX_WAL_INLINE_CONTENT_BYTES + 1
            }
            _ => 0,
        };
        let source = format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"
content_token_secret = "test-secret"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"

[inline_content]
{setting} = {value}
"#
        );
        let field = format!("inline_content.{setting}");
        match parse_server_config(&source).expect_err("an invalid inline content setting") {
            ServerError::Config(ConfigFieldError::Invalid { field: actual, .. }) => {
                assert_eq!(actual, field)
            }
            other => panic!("expected invalid field error for {field}, got {other:?}"),
        }
    }
}

pub(super) fn assert_invalid_field(error: ServerError, field: &'static str) {
    match error {
        ServerError::Config(ConfigFieldError::Invalid { field: actual, .. }) => {
            assert_eq!(actual, field)
        }
        other => panic!("expected invalid field error for {field}, got {other:?}"),
    }
}
