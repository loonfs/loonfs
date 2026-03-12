use crate::error::ObjectStoreError;
use crate::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
};
use crate::{ByteRange, ObjectMetadata, ObjectStore, PutMode};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use http::header::{IF_MATCH, IF_NONE_MATCH};
use http::HeaderValue;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3CompatibleConfig {
    pub provider_name: &'static str,
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub key_prefix: Option<String>,
    pub force_path_style: bool,
}

pub(crate) struct S3CompatibleStore {
    provider_name: &'static str,
    bucket: String,
    key_prefix: Option<String>,
    client: Client,
}

impl fmt::Debug for S3CompatibleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3CompatibleStore")
            .field("provider_name", &self.provider_name)
            .field("bucket", &self.bucket)
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

impl S3CompatibleStore {
    pub(crate) fn new(config: S3CompatibleConfig) -> Result<Self, ObjectStoreError> {
        if config.bucket.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
                "bucket must not be empty".to_owned(),
            ));
        }
        if config.region.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
                "region must not be empty".to_owned(),
            ));
        }
        if config.access_key_id.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
                "access key id must not be empty".to_owned(),
            ));
        }
        if config.secret_access_key.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
                "secret access key must not be empty".to_owned(),
            ));
        }

        let key_prefix = normalize_key_prefix(config.key_prefix.as_deref())?;
        let credentials = Credentials::new(
            config.access_key_id,
            config.secret_access_key,
            config.session_token,
            None,
            "loondb-objectstore",
        );
        let mut builder = aws_sdk_s3::config::Builder::new()
            .region(Region::new(config.region))
            .credentials_provider(credentials)
            .force_path_style(config.force_path_style);
        if let Some(endpoint_url) = config.endpoint_url {
            builder = builder.endpoint_url(endpoint_url);
        }

        Ok(Self {
            provider_name: config.provider_name,
            bucket: config.bucket,
            key_prefix,
            client: Client::from_conf(builder.build()),
        })
    }

    fn scoped_key(&self, key: &str) -> Result<String, ObjectStoreError> {
        scope_object_key(self.key_prefix.as_deref(), key)
    }

    fn scoped_list_prefix(&self, prefix: &str) -> Result<String, ObjectStoreError> {
        scope_list_prefix(self.key_prefix.as_deref(), prefix)
    }

    fn run_async<F, T>(&self, future: F) -> Result<T, ObjectStoreError>
    where
        F: std::future::Future<Output = Result<T, ObjectStoreError>>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;
        runtime.block_on(future)
    }

    async fn head_scoped(
        &self,
        scoped_key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(scoped_key)
            .send()
            .await
        {
            Ok(output) => Ok(Some(ObjectMetadata {
                etag: output.e_tag().map(ToOwned::to_owned),
                size_bytes: output.content_length().try_into().unwrap_or_default(),
            })),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(map_sdk_error(err)),
        }
    }

    async fn put_scoped(
        &self,
        scoped_key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let result = match mode {
            PutMode::Overwrite => {
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(scoped_key)
                    .content_length(bytes.len() as i64)
                    .body(ByteStream::from(bytes.to_vec()))
                    .send()
                    .await
            }
            PutMode::CreateIfAbsent => {
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(scoped_key)
                    .content_length(bytes.len() as i64)
                    .body(ByteStream::from(bytes.to_vec()))
                    .customize()
                    .await
                    .map_err(map_sdk_error)?
                    .mutate_request(|request| {
                        request
                            .headers_mut()
                            .insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
                    })
                    .send()
                    .await
            }
            PutMode::CompareAndSwap { expected_etag } => {
                let if_match = HeaderValue::from_str(&expected_etag).map_err(|err| {
                    ObjectStoreError::Transport(format!(
                        "invalid expected etag for If-Match header: {err}"
                    ))
                })?;

                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(scoped_key)
                    .content_length(bytes.len() as i64)
                    .body(ByteStream::from(bytes.to_vec()))
                    .customize()
                    .await
                    .map_err(map_sdk_error)?
                    .mutate_request(move |request| {
                        request.headers_mut().insert(IF_MATCH, if_match.clone());
                    })
                    .send()
                    .await
            }
        };

        match result {
            Ok(_) => self.head_scoped(scoped_key).await?.ok_or_else(|| {
                ObjectStoreError::Transport(format!(
                    "provider {} reported success for {} but object head is missing",
                    self.provider_name, scoped_key
                ))
            }),
            Err(err) if is_precondition_failure(&err) => Err(ObjectStoreError::PreconditionFailed),
            Err(err) => Err(map_sdk_error(err)),
        }
    }
}

impl ObjectStore for S3CompatibleStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let scoped_key = self.scoped_key(key)?;
        self.run_async(async { self.head_scoped(&scoped_key).await })
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let scoped_key = self.scoped_key(key)?;
        self.run_async(async {
            let range_header = match range {
                None => None,
                Some(range) => {
                    let metadata = match self.head_scoped(&scoped_key).await? {
                        Some(metadata) => metadata,
                        None => return Ok(None),
                    };
                    if range.end_exclusive < range.start_inclusive
                        || range.start_inclusive > metadata.size_bytes
                    {
                        return Err(ObjectStoreError::InvalidRange);
                    }

                    let bounded_end = range.end_exclusive.min(metadata.size_bytes);
                    if bounded_end == range.start_inclusive {
                        return Ok(Some(Vec::new()));
                    }

                    Some(format!(
                        "bytes={}-{}",
                        range.start_inclusive,
                        bounded_end.saturating_sub(1)
                    ))
                }
            };

            let mut request = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&scoped_key);
            if let Some(range_header) = range_header {
                request = request.range(range_header);
            }

            match request.send().await {
                Ok(output) => {
                    let collected = output
                        .body
                        .collect()
                        .await
                        .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;
                    Ok(Some(collected.into_bytes().to_vec()))
                }
                Err(err) if is_not_found(&err) => Ok(None),
                Err(err) if is_invalid_range(&err) => Err(ObjectStoreError::InvalidRange),
                Err(err) => Err(map_sdk_error(err)),
            }
        })
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let scoped_key = self.scoped_key(key)?;
        self.run_async(async { self.put_scoped(&scoped_key, bytes, mode).await })
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let scoped_key = self.scoped_key(key)?;
        self.run_async(async {
            match self
                .client
                .delete_object()
                .bucket(&self.bucket)
                .key(&scoped_key)
                .send()
                .await
            {
                Ok(_) => Ok(()),
                Err(err) if is_not_found(&err) => Ok(()),
                Err(err) => Err(map_sdk_error(err)),
            }
        })
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let scoped_prefix = self.scoped_list_prefix(prefix)?;
        self.run_async(async {
            let mut keys = Vec::new();
            let mut continuation_token = None;

            loop {
                let mut request = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&scoped_prefix);
                if let Some(token) = continuation_token.as_deref() {
                    request = request.continuation_token(token);
                }

                let output = request.send().await.map_err(map_sdk_error)?;
                if let Some(objects) = output.contents() {
                    for object in objects {
                        if let Some(key) = object.key() {
                            match self.key_prefix.as_deref() {
                                Some(key_prefix) => {
                                    if let Some(unscoped) =
                                        unscope_listed_key(Some(key_prefix), key)
                                    {
                                        keys.push(unscoped);
                                    }
                                }
                                None => keys.push(key.to_owned()),
                            }
                        }
                    }
                }

                if !output.is_truncated() {
                    break;
                }
                continuation_token = output.next_continuation_token().map(ToOwned::to_owned);
            }

            keys.sort();
            Ok(keys)
        })
    }
}

fn is_not_found<E, R>(err: &SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    matches!(
        service_error_code(err),
        Some("NotFound" | "NoSuchKey" | "404")
    )
}

fn is_invalid_range<E, R>(err: &SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    matches!(service_error_code(err), Some("InvalidRange"))
}

fn is_precondition_failure<E, R>(err: &SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    matches!(
        service_error_code(err),
        Some("PreconditionFailed" | "ConditionalRequestConflict")
    )
}

fn service_error_code<'a, E, R>(err: &'a SdkError<E, R>) -> Option<&'a str>
where
    E: ProvideErrorMetadata,
{
    err.code()
}

fn map_sdk_error<E, R>(err: SdkError<E, R>) -> ObjectStoreError
where
    E: ProvideErrorMetadata + fmt::Debug,
    R: fmt::Debug,
{
    if is_precondition_failure(&err) {
        return ObjectStoreError::PreconditionFailed;
    }
    if is_not_found(&err) {
        return ObjectStoreError::NotFound;
    }
    if is_invalid_range(&err) {
        return ObjectStoreError::InvalidRange;
    }

    let mut details = Vec::new();
    if let Some(code) = err.code() {
        details.push(format!("code={code}"));
    }
    if let Some(message) = err.message() {
        details.push(format!("message={message}"));
    }

    let detail_suffix = if details.is_empty() {
        String::new()
    } else {
        format!(" ({})", details.join(", "))
    };

    ObjectStoreError::Transport(format!("{err:?}{detail_suffix}"))
}
