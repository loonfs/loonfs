//! Test-only support shared by object-store unit suites.

use crate::timing::MonotonicTimer;
use loonfs_test_support::EnvGuard;
use std::sync::atomic::{AtomicU64, Ordering};

/// Azurite's published development-account key.
pub(crate) const AZURITE_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// Deterministic timer that advances a fixed step per reading.
#[derive(Debug)]
pub(crate) struct SteppingTimer {
    now_ms: AtomicU64,
    step_ms: u64,
}

impl SteppingTimer {
    pub(crate) fn new(step_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(0),
            step_ms,
        }
    }
}

impl MonotonicTimer for SteppingTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.now_ms.fetch_add(self.step_ms, Ordering::SeqCst)
    }
}

/// A service-account JSON whose RSA key is real enough to sign with and
/// belongs to nobody: it was generated for this repository and names a
/// project that does not exist.
///
/// The GCS adapters parse this file at construction, so a placeholder string
/// where the private key goes no longer stands in for one. `disable_oauth`
/// keeps the provider client from reaching for a token it will never need.
pub(crate) const GCS_FIXTURE_SERVICE_ACCOUNT_KEY: &str = r#"{"type":"service_account","project_id":"loonfs-tests","private_key_id":"0123456789abcdef0123456789abcdef01234567","private_key":"-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCqCPHn1DoDzQWc\nQ1RyndwGugUhuWxYV7GhyutLbFVs2+40StJf6UYGJgkKLIaJ5o9X5fSVzHbTnI90\ngA7Hcl3LeVmUBqf3Vzt1ZQaWxWSRrcCnqtvkh3VJ4o4wp/KVRx10o+9MZ5+s5BvB\n9HTnkjzYuz7RwqYPj9oayXWB3Zy4Ba8+yFKc29X77TbdAtUeeaSywi8MOUmUOyzN\nj4/1bniDkj7efPy0nA2m+Rlv+Jgfygs7WLXu3cscCQa2/JMSyfle1tRD+OEVac5y\nzfPu7X6AHNsYPwOjFTtZxAQxcNTjM1Icw+D9001XTe3NiHkTVe1n3L5oVx5VtgsM\nOc3wdLh9AgMBAAECggEADlajw5dvbvuegf9hgyrRr5WHMkFXJBn9BjY84kbX606e\nhzVaCTF8MK+Lapq3m7BgHRrspacwzAZzSHE2DdaUl0B779Ih3ucxweQLirJJmUlM\nKjdrxJkxqFHdCLhY6gKttrTOTKSeX+96ccAiDZcU33fmw7yE0WIhk8mySYm9Gf1p\nasLr2tyxIdAt0rGN6DtlHNGI0gzZU7QZT/rUErLmhsD2dSiACU/mOvJNUB37Prhp\nMhNasQUZMB74duNXliM/NzLg2VEZkXgahXbGkyzMjt5b/lmF7EGR4W9/2AqeWPDl\n4OkVC2GXCXwLgNbNK6UlCSQlAIh1eX3eaENax2QfNwKBgQDi0t1Ok3zzHc69MRCN\nf5fmKsLPd41aJbjEva5a+t/HW9SK4gvKlf5YBb4mmogT5gqDRqhH2A2tD6wh1+Or\nd5y2ffZPkRkr14XWbKlIgnTAJZhMETkzlHQ3VgTaUmQuPQWpu3i/2m2Q9MxkNJQa\nOVnb136/CFKmWhzIM+ymd/3SJwKBgQC/6BG2bwXPCp7OB8k13a5JlK7b3AXx9Cz8\nzJcQ0qzWXjdtnfJbPULem+bWpoBdd7ha4odOgM7bZ08tvjG8EG0ZoYbn3ZvIHWkf\nZREkmuR0iAKVthjQvTHb7CauTgQiAKhT09+YKj8zFQc5X2h//2AR69hlwyAxNGWj\nvrgoG5HauwKBgGHt1lyhctXoLaUjNNlSmDtohNlb7WxZUu+mUUu4ersw24/mzl51\n6e0I9bLnDw9AR5OsAuWZ0zW/yXqHIiWaq89ijOCHbc2u7HrKSUAkCtIWqS1WVlL9\nqjtl6Qx1fAk2kWZZqWVzodBu0HwG81ZrIm+3F2LU7hIiX8DUIj0xGyYLAoGBALcN\nNUAQfLj2B269bIduEh5rrbNYF2+omvT0bjCE1IqSSkrMO24ebFeM3E7peU4usXI3\n3BrcsPQFgjg+0I/0Fy04r0ciUsM6kph4vjZtbPde+SA3F0qc/R8rDeZ70mNgvy9e\nzUwHGEuwhjiKslJNlSTjE4JV8rIcqcrcVCslySWbAoGAcpdodegs1nrDYXgfG/Uc\nG5R+zoAKdoHLtPWyAkVjXMC9qzMhhXcqqtsoKsZs9LbDiwM7+ANjdU9OcPAtw+c6\nSMF4yP4CnQLvdNRiJ7W4VcAYI4fC7Yvlc0cgz8BAMtVJOTuhOwwLs08kJRk6qVLe\nxZ1xhCJtVIW6SUf+s5lL6xE=\n-----END PRIVATE KEY-----\n","client_email":"loonfs-presign-fixture@loonfs-tests.iam.gserviceaccount.com","client_id":"000000000000000000000","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","disable_oauth":true}"#;

/// The service account [`GCS_FIXTURE_SERVICE_ACCOUNT_KEY`] names, which the
/// signer writes into the credential query parameter.
pub(crate) const GCS_FIXTURE_CLIENT_EMAIL: &str =
    "loonfs-presign-fixture@loonfs-tests.iam.gserviceaccount.com";

/// Writes the fixture key to a fresh temporary file and returns its path,
/// because every GCS constructor takes the key as a path to read.
///
/// Returns the temporary directory and the path to the fixture key file.
/// Keep the directory alive while using the path; dropping it deletes the file.
pub(crate) fn gcs_fixture_service_account_key_file(
    label: &str,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("loonfs-objectstore-{label}-"))
        .tempdir()
        .expect("a temporary fixture directory should be creatable");
    let path = dir.path().join("service-account.json");
    std::fs::write(&path, GCS_FIXTURE_SERVICE_ACCOUNT_KEY)
        .expect("the fixture service account should be writable");
    (dir, path)
}

/// Serializes tests that modify AWS environment variables.
static AWS_ENVIRONMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Locks AWS environment access for a test.
pub(crate) async fn aws_environment_lock() -> tokio::sync::MutexGuard<'static, ()> {
    AWS_ENVIRONMENT.lock().await
}

/// Replaces ambient AWS credential sources for a test.
pub(crate) fn isolated_aws_environment(
    tempdir: &tempfile::TempDir,
    environment_credentials: Option<(&str, &str, Option<&str>)>,
) -> Vec<EnvGuard> {
    let credentials_file = tempdir.path().join("credentials");
    let config_file = tempdir.path().join("config");
    std::fs::write(&credentials_file, "").expect("write empty credentials file");
    std::fs::write(&config_file, "").expect("write empty config file");

    let mut environment = match environment_credentials {
        Some((access_key_id, secret_access_key, session_token)) => vec![
            EnvGuard::set("AWS_ACCESS_KEY_ID", access_key_id),
            EnvGuard::set("AWS_SECRET_ACCESS_KEY", secret_access_key),
            match session_token {
                Some(session_token) => EnvGuard::set("AWS_SESSION_TOKEN", session_token),
                None => EnvGuard::unset("AWS_SESSION_TOKEN"),
            },
        ],
        None => vec![
            EnvGuard::unset("AWS_ACCESS_KEY_ID"),
            EnvGuard::unset("AWS_SECRET_ACCESS_KEY"),
            EnvGuard::unset("AWS_SESSION_TOKEN"),
        ],
    };
    environment.extend([
        EnvGuard::set("AWS_SHARED_CREDENTIALS_FILE", credentials_file),
        EnvGuard::set("AWS_CONFIG_FILE", config_file),
        EnvGuard::set("AWS_PROFILE", "loonfs-test"),
        EnvGuard::set("AWS_REGION", "us-east-1"),
        EnvGuard::set("AWS_EC2_METADATA_DISABLED", "true"),
        EnvGuard::unset("AWS_WEB_IDENTITY_TOKEN_FILE"),
        EnvGuard::unset("AWS_ROLE_ARN"),
        EnvGuard::unset("AWS_ROLE_SESSION_NAME"),
        EnvGuard::unset("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"),
        EnvGuard::unset("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
        EnvGuard::unset("AWS_CONTAINER_AUTHORIZATION_TOKEN"),
        EnvGuard::unset("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"),
    ]);
    if std::env::var_os("SSL_CERT_FILE").is_none()
        && std::path::Path::new("/etc/ssl/cert.pem").is_file()
    {
        environment.push(EnvGuard::set("SSL_CERT_FILE", "/etc/ssl/cert.pem"));
    }
    environment
}
