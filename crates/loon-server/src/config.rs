use loon_objectstore::r2::R2StoreConfig;
use loon_objectstore::s3::AwsS3StoreConfig;
use loon_objectstore::ConfiguredObjectStore;
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    pub auth_token: Option<String>,
    pub writer_id: String,
    pub writer_version: String,
    pub lease_duration_ms: u64,
    pub store: StoreConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum StoreConfig {
    LocalFs {
        root: String,
        key_prefix: Option<String>,
    },
    AwsS3 {
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        key_prefix: Option<String>,
        force_path_style: Option<bool>,
    },
    CloudflareR2 {
        bucket: String,
        account_id: String,
        endpoint_url: String,
        access_key_id: String,
        secret_access_key: String,
        key_prefix: Option<String>,
    },
}

impl ServerConfig {
    pub fn object_store(&self) -> Result<ConfiguredObjectStore, String> {
        match &self.store {
            StoreConfig::LocalFs { root, key_prefix } => {
                ConfiguredObjectStore::local_fs(root, key_prefix.as_deref())
                    .map_err(|err| err.to_string())
            }
            StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url,
                access_key_id,
                secret_access_key,
                session_token,
                key_prefix,
                force_path_style,
            } => ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                session_token: session_token.clone(),
                key_prefix: key_prefix.clone(),
                force_path_style: force_path_style.unwrap_or(false),
            })
            .map_err(|err| err.to_string()),
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                key_prefix,
            } => ConfiguredObjectStore::cloudflare_r2(R2StoreConfig {
                bucket: bucket.clone(),
                account_id: account_id.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                key_prefix: key_prefix.clone(),
            })
            .map_err(|err| err.to_string()),
        }
    }
}

pub fn load_server_config(path: impl AsRef<Path>) -> Result<ServerConfig, String> {
    let bytes = fs::read(path.as_ref()).map_err(|err| err.to_string())?;
    toml::from_str(std::str::from_utf8(&bytes).map_err(|err| err.to_string())?)
        .map_err(|err| err.to_string())
}
