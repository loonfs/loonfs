//! Config tables and environment fallbacks that the loaders don't expose.

use super::{parse_server_config, AuthConfig, ServerConfig};
use crate::ServerError;
use loonfs_test_support::EnvGuard;
use loonfs_types::config::ConfigFieldError;
use loonfs_types::env::CONTENT_TOKEN_SECRET_ENV;
use loonfs_types::SecretString;

#[test]
fn inline_content_defaults_and_overrides_preserve_explicit_disablement() {
    let defaults = loonfs::InlineContentPolicy::default();
    assert_eq!(defaults.inline_content_threshold_bytes, Some(64 * 1024));
    assert_eq!(defaults.inline_content_fold_at_bytes, 2 * 1024 * 1024);
    assert_eq!(defaults.inline_content_wal_object_budget_bytes, 1024 * 1024);
    assert_eq!(defaults.inline_content_tail_limit_bytes, 32 * 1024 * 1024);
    for (source, threshold) in [
        ("", Some(64 * 1024)),
        (
            "inline_content_wal_object_budget_bytes = 1024",
            Some(64 * 1024),
        ),
        ("inline_content_threshold_bytes = 4096", Some(4096)),
        ("inline_content_threshold_bytes = 0", Some(0)),
        ("inline_content_threshold_bytes = false", None),
    ] {
        let overrides: loonfs::InlineContentPolicy = toml::from_str(source).expect("overrides");
        assert_eq!(overrides.inline_content_threshold_bytes, threshold);
    }
    toml::from_str::<loonfs::InlineContentPolicy>("inline_content_threshold_bytes = true")
        .expect_err("a boolean threshold should be rejected");
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
/// `maintenance_required` before its request returns `deadline_exceeded`.
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

/// Checks that an absent idle fold age takes the runtime's default, and that
/// zero turns the rule off.
#[test]
fn the_runtime_takes_the_idle_fold_age_or_its_default() {
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
        .expect("valid config")
    };
    for (setting, expected) in [
        ("", Some(loonfs::DEFAULT_IDLE_FOLD_AFTER_MS)),
        (
            "idle_fold_after_ms = 60000",
            std::num::NonZeroU64::new(60_000),
        ),
        ("idle_fold_after_ms = 0", None),
    ] {
        assert_eq!(
            config(setting).config().runtime_config().idle_fold_after_ms,
            expected,
            "{setting:?}"
        );
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

pub(super) fn assert_invalid_field(error: ServerError, field: &'static str) {
    match error {
        ServerError::Config(ConfigFieldError::Invalid { field: actual, .. }) => {
            assert_eq!(actual, field)
        }
        other => panic!("expected invalid field error for {field}, got {other:?}"),
    }
}
