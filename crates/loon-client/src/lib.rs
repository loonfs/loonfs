#![forbid(unsafe_code)]

use http::Uri;
use loon_api::{
    ApiError, AuthoritativePathEntry, ChangeSeq, CopyEntryRequest, CreateNamespaceRequest,
    ListNamespacesResponse, MoveEntryRequest, MutationResult, NamespaceSummary,
};
use serde::Deserialize;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Debug, Clone, Deserialize)]
pub struct ClientConfig {
    pub server_url: String,
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    auth_token: Option<String>,
    agent: ureq::Agent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPathResult {
    pub destination: PathBuf,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutPathResult {
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePath {
    pub namespace: String,
    pub absolute_path: String,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to read config: {0}")]
    ConfigIo(String),
    #[error("failed to decode config: {0}")]
    ConfigDecode(String),
    #[error("missing `{field}`")]
    MissingConfigField { field: &'static str },
    #[error("invalid `{field}`: {reason}")]
    ConfigValidation { field: &'static str, reason: String },
    #[error("invalid namespace path `{0}`")]
    InvalidNamespacePath(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("server returned {status} {code}: {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
    #[error("i/o error: {0}")]
    Io(String),
    #[error("json error: {0}")]
    Json(String),
}

impl ClientConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let bytes =
            fs::read(path.as_ref()).map_err(|err| ClientError::ConfigIo(err.to_string()))?;
        let config: Self = toml::from_str(
            std::str::from_utf8(&bytes)
                .map_err(|err| ClientError::ConfigDecode(err.to_string()))?,
        )
        .map_err(|err| ClientError::ConfigDecode(err.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ClientError> {
        validate_absolute_http_url("server_url", &self.server_url)?;
        if let Some(token) = &self.auth_token {
            if token.trim().is_empty() {
                return Err(ClientError::ConfigValidation {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                });
            }
        }
        Ok(())
    }
}

impl Client {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            base_url: config.server_url.trim().trim_end_matches('/').to_owned(),
            auth_token: config.auth_token,
            agent: ureq::AgentBuilder::new().build(),
        }
    }

    pub fn create_namespace(&self, name: &str) -> Result<NamespaceSummary, ClientError> {
        let url = format!("{}/v1/namespaces", self.base_url);
        self.request_json::<_, NamespaceSummary>(
            self.agent.post(&url),
            Some(&CreateNamespaceRequest {
                name: name.to_owned(),
            }),
        )
    }

    pub fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>, ClientError> {
        let url = format!("{}/v1/namespaces", self.base_url);
        Ok(self
            .request_json::<(), ListNamespacesResponse>(self.agent.get(&url), None)?
            .namespaces)
    }

    pub fn list_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, ClientError> {
        let url = format!(
            "{}/v1/namespaces/{}/entries?path={}",
            self.base_url,
            spec.namespace,
            urlencoding::encode(&spec.absolute_path)
        );
        self.request_json::<(), Vec<AuthoritativePathEntry>>(self.agent.get(&url), None)
    }

    pub fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, ClientError> {
        let url = format!(
            "{}/v1/namespaces/{}/stat?path={}",
            self.base_url,
            spec.namespace,
            urlencoding::encode(&spec.absolute_path)
        );
        self.request_json::<(), AuthoritativePathEntry>(self.agent.get(&url), None)
    }

    pub fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, ClientError> {
        let url = format!(
            "{}/v1/namespaces/{}/content?path={}",
            self.base_url,
            spec.namespace,
            urlencoding::encode(&spec.absolute_path)
        );
        let request = self.authenticated(self.agent.get(&url));
        let response = request.call().map_err(|err| self.map_error(err))?;
        let mut reader = response.into_reader();
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut bytes)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        Ok(bytes)
    }

    pub fn healthz(&self) -> Result<(), ClientError> {
        let url = format!("{}/healthz", self.base_url);
        let request = self.authenticated(self.agent.get(&url));
        request.call().map_err(|err| self.map_error(err))?;
        Ok(())
    }

    pub fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, ClientError> {
        let url = format!(
            "{}/v1/namespaces/{}/content?path={}&force={}",
            self.base_url,
            spec.namespace,
            urlencoding::encode(&spec.absolute_path),
            if force { "true" } else { "false" }
        );
        let request = self
            .authenticated(self.agent.put(&url))
            .set("content-type", "application/octet-stream");
        let response = request
            .send_bytes(bytes)
            .map_err(|err| self.map_error(err))?;
        serde_json::from_reader(response.into_reader())
            .map_err(|err| ClientError::Json(err.to_string()))
    }

    pub fn write_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
    ) -> Result<MutationResult, ClientError> {
        self.put_file_bytes(spec, bytes, true)
    }

    pub fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, ClientError> {
        let url = format!(
            "{}/v1/namespaces/{}/entries?path={}",
            self.base_url,
            spec.namespace,
            urlencoding::encode(&spec.absolute_path)
        );
        let request = self.authenticated(self.agent.delete(&url));
        let response = request.call().map_err(|err| self.map_error(err))?;
        serde_json::from_reader(response.into_reader())
            .map_err(|err| ClientError::Json(err.to_string()))
    }

    pub fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, ClientError> {
        if from.namespace != to.namespace {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot move across namespaces: {} -> {}",
                from.namespace, to.namespace
            )));
        }
        let url = format!("{}/v1/namespaces/{}/move", self.base_url, from.namespace);
        self.request_json::<_, MutationResult>(
            self.agent.post(&url),
            Some(&MoveEntryRequest {
                request_id: Uuid::new_v4().to_string(),
                from_path: from.absolute_path.clone(),
                to_path: to.absolute_path.clone(),
            }),
        )
    }

    pub fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, ClientError> {
        if from.namespace != to.namespace {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot copy across namespaces: {} -> {}",
                from.namespace, to.namespace
            )));
        }
        let url = format!("{}/v1/namespaces/{}/copy", self.base_url, from.namespace);
        self.request_json::<_, MutationResult>(
            self.agent.post(&url),
            Some(&CopyEntryRequest {
                request_id: Uuid::new_v4().to_string(),
                from_path: from.absolute_path.clone(),
                to_path: to.absolute_path.clone(),
            }),
        )
    }

    pub fn get_to_path(
        &self,
        spec: &NamespacePath,
        destination: impl AsRef<Path>,
    ) -> Result<GetPathResult, ClientError> {
        let destination = destination.as_ref();
        let entry = self.stat_path(spec)?;
        match entry.inode_kind {
            loon_api::InodeKind::File => {
                let bytes = self.read_file_bytes(spec)?;
                let target = if destination.is_dir() {
                    destination.join(file_name_for_path(&spec.absolute_path)?)
                } else {
                    destination.to_path_buf()
                };
                let bytes_written = write_local_file(&target, &bytes)?;
                Ok(GetPathResult {
                    destination: target,
                    bytes_written,
                })
            }
            loon_api::InodeKind::Dir => {
                let bytes_written = self.get_directory(spec, destination)?;
                Ok(GetPathResult {
                    destination: destination.to_path_buf(),
                    bytes_written,
                })
            }
            kind => Err(ClientError::InvalidNamespacePath(format!(
                "unsupported inode kind for get: {kind:?}"
            ))),
        }
    }

    pub fn put_from_path(
        &self,
        source: impl AsRef<Path>,
        spec: &NamespacePath,
    ) -> Result<PutPathResult, ClientError> {
        let source = source.as_ref();
        if source.is_file() {
            let bytes = fs::read(source).map_err(|err| ClientError::Io(err.to_string()))?;
            let result = self.write_file_bytes(spec, &bytes)?;
            return Ok(PutPathResult {
                committed_seq: result.committed_seq,
            });
        }
        if !source.is_dir() {
            return Err(ClientError::Io(format!(
                "local path is neither file nor directory: {}",
                source.display()
            )));
        }

        let mut last_result = None;
        for entry in WalkDir::new(source).into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(source)
                .map_err(|err| ClientError::Io(err.to_string()))?;
            let remote_path = join_remote_path(&spec.absolute_path, relative)?;
            let target = NamespacePath {
                namespace: spec.namespace.clone(),
                absolute_path: remote_path,
            };
            let bytes = fs::read(entry.path()).map_err(|err| ClientError::Io(err.to_string()))?;
            last_result = Some(self.write_file_bytes(&target, &bytes)?);
        }

        let Some(result) = last_result else {
            return Err(ClientError::Io(format!(
                "local directory does not contain any files: {}",
                source.display()
            )));
        };

        Ok(PutPathResult {
            committed_seq: result.committed_seq,
        })
    }

    fn get_directory(&self, spec: &NamespacePath, destination: &Path) -> Result<u64, ClientError> {
        fs::create_dir_all(destination).map_err(|err| ClientError::Io(err.to_string()))?;
        let mut bytes_written = 0;
        for entry in self.list_path(spec)? {
            let child_spec = NamespacePath {
                namespace: spec.namespace.clone(),
                absolute_path: entry.absolute_path.clone(),
            };
            let child_dest = destination.join(if entry.display_name.is_empty() {
                file_name_for_path(&entry.absolute_path)?
            } else {
                entry.display_name.clone()
            });
            match entry.inode_kind {
                loon_api::InodeKind::Dir => {
                    bytes_written += self.get_directory(&child_spec, &child_dest)?;
                }
                loon_api::InodeKind::File => {
                    let bytes = self.read_file_bytes(&child_spec)?;
                    bytes_written += write_local_file(&child_dest, &bytes)?;
                }
                _ => {}
            }
        }
        Ok(bytes_written)
    }

    fn request_json<Req, Resp>(
        &self,
        request: ureq::Request,
        body: Option<&Req>,
    ) -> Result<Resp, ClientError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let request = self.authenticated(request);
        let response = match body {
            Some(body) => request.send_json(body).map_err(|err| self.map_error(err))?,
            None => request.call().map_err(|err| self.map_error(err))?,
        };
        serde_json::from_reader(response.into_reader())
            .map_err(|err| ClientError::Json(err.to_string()))
    }

    fn authenticated(&self, request: ureq::Request) -> ureq::Request {
        match &self.auth_token {
            Some(token) => request.set("authorization", &format!("Bearer {token}")),
            None => request,
        }
    }

    fn map_error(&self, error: ureq::Error) -> ClientError {
        match error {
            ureq::Error::Status(status, response) => {
                let parsed = serde_json::from_reader::<_, ApiError>(response.into_reader());
                match parsed {
                    Ok(body) => ClientError::Api {
                        status,
                        code: body.code,
                        message: body.message,
                    },
                    Err(err) => ClientError::Http(err.to_string()),
                }
            }
            ureq::Error::Transport(err) => ClientError::Http(err.to_string()),
        }
    }
}

impl NamespacePath {
    pub fn parse(value: &str) -> Result<Self, ClientError> {
        let (namespace, path) = value
            .split_once(':')
            .ok_or_else(|| ClientError::InvalidNamespacePath(value.to_owned()))?;
        if namespace.trim().is_empty() || !path.starts_with('/') {
            return Err(ClientError::InvalidNamespacePath(value.to_owned()));
        }
        Ok(Self {
            namespace: namespace.to_owned(),
            absolute_path: path.to_owned(),
        })
    }
}

fn file_name_for_path(path: &str) -> Result<String, ClientError> {
    path.rsplit('/')
        .find(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ClientError::InvalidNamespacePath(path.to_owned()))
}

fn join_remote_path(base: &str, relative: &Path) -> Result<String, ClientError> {
    let mut path = PathBuf::from(base.trim_end_matches('/'));
    path.push(relative);
    let rendered = format!("/{}", path.display().to_string().trim_start_matches('/'));
    if rendered.contains('\\') {
        return Err(ClientError::InvalidNamespacePath(rendered));
    }
    Ok(rendered)
}

fn write_local_file(path: &Path, bytes: &[u8]) -> Result<u64, ClientError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| ClientError::Io(err.to_string()))?;
    }
    let mut file = fs::File::create(path).map_err(|err| ClientError::Io(err.to_string()))?;
    file.write_all(bytes)
        .map_err(|err| ClientError::Io(err.to_string()))?;
    Ok(bytes.len() as u64)
}

fn validate_absolute_http_url(field: &'static str, value: &str) -> Result<(), ClientError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ClientError::MissingConfigField { field });
    }

    let uri: Uri =
        trimmed
            .parse()
            .map_err(|err: http::uri::InvalidUri| ClientError::ConfigValidation {
                field,
                reason: err.to_string(),
            })?;

    match uri.scheme_str() {
        Some("http" | "https") => {}
        Some(other) => {
            return Err(ClientError::ConfigValidation {
                field,
                reason: format!("scheme must be http or https, got `{other}`"),
            });
        }
        None => {
            return Err(ClientError::ConfigValidation {
                field,
                reason: "must be an absolute http or https URL".to_owned(),
            });
        }
    }

    if uri.authority().is_none() {
        return Err(ClientError::ConfigValidation {
            field,
            reason: "must be an absolute http or https URL".to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ClientConfig, ClientError};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn load_rejects_invalid_server_url() {
        let path = write_config(
            r#"
server_url = "ftp://example.com"
auth_token = "dev-token"
"#,
        );

        let error = ClientConfig::load(&path).expect_err("invalid server url");

        match error {
            ClientError::ConfigValidation { field, .. } => assert_eq!(field, "server_url"),
            other => panic!("expected config validation error, got {other:?}"),
        }
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

        match error {
            ClientError::ConfigValidation { field, .. } => assert_eq!(field, "auth_token"),
            other => panic!("expected config validation error, got {other:?}"),
        }
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

    fn write_config(contents: &str) -> std::path::PathBuf {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("client.toml");
        fs::write(&path, contents).expect("write config");
        let _ = temp_dir.keep();
        path
    }
}
