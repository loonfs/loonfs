//! AWS credential sources shared by provider requests and presigners.

use crate::store_config::AwsS3Credentials;
use crate::ObjectStoreError;
use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use loonfs_api::SecretString;
use object_store::aws::AwsCredential;
use object_store::client::CredentialProvider;
use std::fmt;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// One credential snapshot used to sign one AWS operation.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AwsSigningCredentials {
    pub(crate) access_key_id: SecretString,
    pub(crate) secret_access_key: SecretString,
    pub(crate) session_token: Option<SecretString>,
}

impl fmt::Debug for AwsSigningCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsSigningCredentials")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[async_trait]
pub(crate) trait AwsCredentialsSource: Send + Sync + fmt::Debug {
    async fn credentials(&self) -> Result<AwsSigningCredentials, ObjectStoreError>;
}

pub(crate) type SharedAwsCredentialsSource = Arc<dyn AwsCredentialsSource>;

/// Builds the runtime source selected by the AWS S3 configuration.
pub(crate) fn aws_credentials_source(
    credentials: &AwsS3Credentials,
) -> Result<SharedAwsCredentialsSource, ObjectStoreError> {
    match credentials {
        AwsS3Credentials::Ambient {} => Ok(Arc::new(AmbientAwsCredentialsSource::default())),
        AwsS3Credentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            if access_key_id.expose().trim().is_empty() {
                return Err(ObjectStoreError::Configuration(
                    "access key id must not be empty".to_owned(),
                ));
            }
            if secret_access_key.expose().trim().is_empty() {
                return Err(ObjectStoreError::Configuration(
                    "secret access key must not be empty".to_owned(),
                ));
            }
            if session_token
                .as_ref()
                .is_some_and(|token| token.expose().trim().is_empty())
            {
                return Err(ObjectStoreError::Configuration(
                    "session token must not be empty".to_owned(),
                ));
            }
            Ok(Arc::new(StaticAwsCredentialsSource::new(
                AwsSigningCredentials {
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                    session_token: session_token.clone(),
                },
            )))
        }
    }
}

/// Builds a source for an already resolved S3-compatible credential set.
pub(crate) fn static_aws_credentials_source(
    access_key_id: SecretString,
    secret_access_key: SecretString,
    session_token: Option<SecretString>,
) -> SharedAwsCredentialsSource {
    Arc::new(StaticAwsCredentialsSource::new(AwsSigningCredentials {
        access_key_id,
        secret_access_key,
        session_token,
    }))
}

#[derive(Clone)]
struct StaticAwsCredentialsSource {
    credentials: AwsSigningCredentials,
}

impl StaticAwsCredentialsSource {
    fn new(credentials: AwsSigningCredentials) -> Self {
        Self { credentials }
    }
}

impl fmt::Debug for StaticAwsCredentialsSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticAwsCredentialsSource")
            .field("credentials", &"<redacted>")
            .finish()
    }
}

#[async_trait]
impl AwsCredentialsSource for StaticAwsCredentialsSource {
    async fn credentials(&self) -> Result<AwsSigningCredentials, ObjectStoreError> {
        Ok(self.credentials.clone())
    }
}

#[derive(Default)]
struct AmbientAwsCredentialsSource {
    provider: OnceCell<SharedCredentialsProvider>,
}

impl fmt::Debug for AmbientAwsCredentialsSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmbientAwsCredentialsSource")
            .finish_non_exhaustive()
    }
}

impl AmbientAwsCredentialsSource {
    async fn provider(&self) -> &SharedCredentialsProvider {
        self.provider
            .get_or_init(|| async {
                let chain = DefaultCredentialsChain::builder().build().await;
                SharedCredentialsProvider::new(chain)
            })
            .await
    }
}

#[async_trait]
impl AwsCredentialsSource for AmbientAwsCredentialsSource {
    async fn credentials(&self) -> Result<AwsSigningCredentials, ObjectStoreError> {
        let credentials = self
            .provider()
            .await
            .provide_credentials()
            .await
            .map_err(|_| {
                ObjectStoreError::Configuration(
                    "could not resolve `store.credentials` through the standard AWS credential chain"
                        .to_owned(),
                )
            })?;
        Ok(AwsSigningCredentials {
            access_key_id: SecretString::new(credentials.access_key_id()),
            secret_access_key: SecretString::new(credentials.secret_access_key()),
            session_token: credentials.session_token().map(SecretString::new),
        })
    }
}

/// Adapts the shared LoonFS source to the provider client's credential API.
#[derive(Clone)]
pub(crate) struct ObjectStoreAwsCredentialProvider {
    source: SharedAwsCredentialsSource,
}

impl ObjectStoreAwsCredentialProvider {
    pub(crate) fn new(source: SharedAwsCredentialsSource) -> Self {
        Self { source }
    }
}

impl fmt::Debug for ObjectStoreAwsCredentialProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreAwsCredentialProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CredentialProvider for ObjectStoreAwsCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credentials =
            self.source
                .credentials()
                .await
                .map_err(|source| object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(source),
                })?;
        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id.expose().to_owned(),
            secret_key: credentials.secret_access_key.expose().to_owned(),
            token: credentials
                .session_token
                .as_ref()
                .map(|token| token.expose().to_owned()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::aws_credentials_source;
    use crate::{AwsS3Credentials, ObjectStoreError};
    use loonfs_api::SecretString;
    use std::ffi::OsString;
    use std::fs;

    static ENVIRONMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct EnvGuard {
        name: &'static str,
        previous: Option<OsString>,
    }

    impl EnvGuard {
        fn set(name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, previous }
        }

        fn unset(name: &'static str) -> Self {
            let previous = std::env::var_os(name);
            std::env::remove_var(name);
            Self { name, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }

    fn isolated_aws_environment(
        tempdir: &tempfile::TempDir,
        environment_credentials: Option<(&str, &str, Option<&str>)>,
    ) -> Vec<EnvGuard> {
        let credentials_file = tempdir.path().join("credentials");
        let config_file = tempdir.path().join("config");
        fs::write(&credentials_file, "").expect("write empty credentials file");
        fs::write(&config_file, "").expect("write empty config file");

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

    #[tokio::test(flavor = "current_thread")]
    async fn environment_pair_resolves_through_the_ambient_source() {
        let _lock = ENVIRONMENT.lock().await;
        let tempdir = tempfile::tempdir().expect("create temporary AWS config directory");
        let _environment = isolated_aws_environment(
            &tempdir,
            Some((
                "environment-access",
                "environment-secret",
                Some("environment-session"),
            )),
        );

        let source = aws_credentials_source(&AwsS3Credentials::Ambient {})
            .expect("construct ambient source");
        let credentials = source
            .credentials()
            .await
            .expect("resolve environment pair");

        assert_eq!(credentials.access_key_id.expose(), "environment-access");
        assert_eq!(credentials.secret_access_key.expose(), "environment-secret");
        assert_eq!(
            credentials.session_token.as_ref().map(SecretString::expose),
            Some("environment-session")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shared_credentials_file_and_profile_resolve_through_the_ambient_source() {
        let _lock = ENVIRONMENT.lock().await;
        let tempdir = tempfile::tempdir().expect("create temporary AWS config directory");
        let environment = isolated_aws_environment(&tempdir, None);
        fs::write(
            tempdir.path().join("credentials"),
            "[loonfs-test]\naws_access_key_id = profile-access\naws_secret_access_key = profile-secret\naws_session_token = profile-session\n",
        )
        .expect("write shared credentials file");

        let source = aws_credentials_source(&AwsS3Credentials::Ambient {})
            .expect("construct ambient source");
        let credentials = source
            .credentials()
            .await
            .expect("resolve profile credentials");

        assert_eq!(credentials.access_key_id.expose(), "profile-access");
        assert_eq!(credentials.secret_access_key.expose(), "profile-secret");
        assert_eq!(
            credentials.session_token.as_ref().map(SecretString::expose),
            Some("profile-session")
        );
        drop(environment);
    }

    #[tokio::test]
    async fn static_credentials_are_returned_exactly() {
        let source = aws_credentials_source(&AwsS3Credentials::Static {
            access_key_id: "static-access".into(),
            secret_access_key: "static-secret".into(),
            session_token: Some("static-session".into()),
        })
        .expect("construct static source");

        let credentials = source
            .credentials()
            .await
            .expect("resolve static credentials");

        assert_eq!(credentials.access_key_id.expose(), "static-access");
        assert_eq!(credentials.secret_access_key.expose(), "static-secret");
        assert_eq!(
            credentials.session_token.as_ref().map(SecretString::expose),
            Some("static-session")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_empty_chain_reports_only_the_configuration_field() {
        let _lock = ENVIRONMENT.lock().await;
        let tempdir = tempfile::tempdir().expect("create temporary AWS config directory");
        let _environment = isolated_aws_environment(&tempdir, None);
        let source = aws_credentials_source(&AwsS3Credentials::Ambient {})
            .expect("construct ambient source");

        let error = source
            .credentials()
            .await
            .expect_err("isolated chain has no credentials");

        assert!(matches!(error, ObjectStoreError::Configuration(_)));
        assert_eq!(
            error.to_string(),
            "invalid object store configuration: could not resolve `store.credentials` through the standard AWS credential chain"
        );
        let public = error.public_message();
        assert!(public.contains("credentials"), "{public}");
        for provider_detail in ["Environment", "Profile", "WebIdentity", "Ecs", "Imds"] {
            assert!(!public.contains(provider_detail), "{public}");
        }
    }
}
