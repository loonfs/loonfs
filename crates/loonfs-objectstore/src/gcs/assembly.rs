//! Create-only GCS media uploads, compose, and cleanup of a compose's
//! intermediate objects.

use super::{gcs_object, GcsRequestSigner, GCS_JSON_OBJECTS};
use crate::assembly::{
    combined_checksum, precondition_failed_when_missing, read_verified, resolve_sources,
    tail_bytes, verify_expected, ResolvedSource, READ_BYTES,
};
use crate::keys::content_span;
use crate::layout::{content_id_of, parse_object_key, KeyPrefix};
use crate::object_store::invalid_request;
use crate::presign::percent_encode_segment;
use crate::provider_limits::PROVIDER_MULTIPART_THRESHOLD_BYTES;
use crate::signed_request::SignedResponse;
use crate::signed_upload::{AbortUploadOnDrop, PartReader, ProviderSigner};
use crate::{AssemblySource, ByteRange, ObjectMetadata, ObjectStoreError, Result};
use bytes::Bytes;
use futures::{stream, FutureExt, StreamExt};
use http::header::CONTENT_TYPE;
use loonfs_types::{Checksum, ChecksumAlgorithm, ContentId, PoolId, SpanObjectId};
use object_store::client::HttpRequestBody;

/// Most source objects one GCS compose request may list, a limit GCS sets.
/// An assembly with more sources composes them in groups of this many, so
/// `n` sources take about `n / 31` compose requests.
const GCS_MAX_COMPOSE_SOURCES: usize = 32;

/// The intermediate objects that one assembly creates, each with the guard
/// that deletes it if the assembly is abandoned.
///
/// Each intermediate is a span object of the destination's chain with a fresh
/// span object ID, which no layout lists, so pool collection deletes an
/// intermediate that a crash leaves behind, like any other unreferenced
/// content object.
struct Intermediates {
    pool_id: PoolId,
    content_id: ContentId,
    created: Vec<(String, AbortUploadOnDrop)>,
}

impl Intermediates {
    /// Starts the intermediates of an assembly of the content object `key`.
    fn new(key: &str) -> Result<Self> {
        let pool_id = parse_object_key(key).and_then(|parsed| match parsed.prefix() {
            KeyPrefix::Pool(pool_id) => PoolId::parse(pool_id).ok(),
            KeyPrefix::Namespace(_) => None,
        });
        match (pool_id, content_id_of(key)) {
            (Some(pool_id), Some(content_id)) => Ok(Self {
                pool_id,
                content_id,
                created: Vec::new(),
            }),
            _ => Err(ObjectStoreError::InvalidKey {
                object_key: key.to_owned(),
                message: "an assembled object must be a content object".to_owned(),
            }),
        }
    }

    /// Returns the key of a new intermediate, and arms the guard that deletes
    /// the object.
    fn next_name(&mut self, signer: &GcsRequestSigner) -> String {
        let name = content_span(&self.pool_id, &self.content_id, &SpanObjectId::generate());
        self.created
            .push((name.clone(), signer.intermediate_cleanup(&name)));
        name
    }
}

impl GcsRequestSigner {
    fn compose_source(&self, key: &str, metadata: &ObjectMetadata) -> Result<serde_json::Value> {
        let name = self.key_scope.object_key(key)?;
        let generation = crate::required_etag(key, metadata.etag.clone())?;
        Ok(serde_json::json!({
            "name": name,
            "objectPreconditions": { "ifGenerationMatch": generation },
        }))
    }

    async fn compose(&self, key: &str, sources: Vec<serde_json::Value>) -> Result<ObjectMetadata> {
        let url = format!(
            "{GCS_JSON_OBJECTS}/{}/o/{}/compose?ifGenerationMatch=0",
            percent_encode_segment(&self.bucket),
            self.object_name(key)?
        );
        let body = serde_json::json!({ "sourceObjects": sources, "destination": {} }).to_string();
        // A retried compose whose earlier attempt succeeded fails its
        // `ifGenerationMatch=0` precondition. For the destination, the
        // assembly's stored-checksum check then accepts the object.
        let response = self
            .retry
            .run(key, "compose", 0, || {
                let request = http::Request::post(&url).header(CONTENT_TYPE, "application/json");
                self.send_authorized(key, request, body.clone().into())
            })
            .await?;
        gcs_object(key, &response.body)
    }

    async fn get_range(&self, key: &str, generation: &str, range: ByteRange) -> Result<Bytes> {
        let url = format!(
            "{GCS_JSON_OBJECTS}/{}/o/{}?alt=media&generation={generation}",
            percent_encode_segment(&self.bucket),
            self.object_name(key)?
        );
        let bytes = range
            .http_range_header()
            .ok_or_else(|| invalid_request(key, "a ranged read needs a nonempty range"))?;
        let response = self
            .retry
            .run(key, "get_range", 0, || {
                let request = http::Request::get(&url).header(http::header::RANGE, &bytes);
                self.send_authorized(key, request, HttpRequestBody::empty())
            })
            .await
            .map_err(precondition_failed_when_missing)?;
        Ok(response.body)
    }

    /// Copies the resolved range of `source` into the new object `intermediate`.
    async fn upload_range(
        &self,
        intermediate: &str,
        source: &ResolvedSource<'_>,
    ) -> Result<ObjectMetadata> {
        let generation = source.etag()?;
        let key = source.source.key.as_str();
        let body = read_verified(source, |range| self.get_range(key, &generation, range)).boxed();
        let mut reader = PartReader::new(body, loonfs_types::usize_from_u64(READ_BYTES));
        let head = reader.next_part().await?.unwrap_or_default();
        self.put_if_absent(intermediate, head, reader, 1).await
    }

    fn intermediate_cleanup(&self, key: &str) -> AbortUploadOnDrop {
        let signer = self.clone();
        let intermediate = key.to_owned();
        AbortUploadOnDrop::new(
            key,
            self.io_runtime.clone(),
            async move {
                signer.delete_intermediate(&intermediate).await;
                Ok(())
            }
            .boxed(),
        )
    }

    // A failed delete leaves the intermediate for pool collection, which
    // deletes it as an unreferenced content object.
    async fn delete_intermediate(&self, intermediate: &str) {
        let deleted: Result<SignedResponse> = async {
            let url = format!(
                "{GCS_JSON_OBJECTS}/{}/o/{}",
                percent_encode_segment(&self.bucket),
                self.object_name(intermediate)?,
            );
            self.send_authorized(
                intermediate,
                http::Request::delete(url),
                HttpRequestBody::empty(),
            )
            .await
        }
        .await;
        if deleted.is_err() {
            tracing::warn!(
                object_key = intermediate,
                operation = "delete",
                "failed to delete an assembly's intermediate object; it stays until pool collection deletes it",
            );
        }
    }

    pub(super) async fn assemble_objects(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let mut intermediates = Intermediates::new(key)?;
        let resolved = resolve_sources(sources)?;
        for source in &resolved {
            let source = source.source;
            let checksum = source.metadata.checksum.as_ref().ok_or_else(|| {
                ObjectStoreError::StoredChecksumMissing {
                    object_key: source.key.clone(),
                }
            })?;
            if source.range.is_none() {
                verify_expected(&source.key, &source.checksum, checksum)?;
            }
        }
        let actual = combined_checksum(
            ChecksumAlgorithm::Crc32c,
            resolved
                .iter()
                .map(|source| (&source.source.checksum, source.range.len_bytes())),
            &tail,
        )
        .ok_or_else(|| ObjectStoreError::ChecksumMismatch {
            object_key: key.to_owned(),
        })?;
        verify_expected(key, expected, &actual)?;
        let size_bytes = resolved
            .iter()
            .map(|source| source.range.len_bytes())
            .sum::<u64>()
            + tail_bytes(&tail);
        if size_bytes < PROVIDER_MULTIPART_THRESHOLD_BYTES {
            return self.create_assembly(key, &resolved, tail, expected).await;
        }
        let result = self
            .compose_assembly(key, &resolved, tail, expected, &mut intermediates)
            .await;
        for (name, mut cleanup) in intermediates.created {
            self.delete_intermediate(&name).await;
            cleanup.disarm();
        }
        result.map_err(precondition_failed_when_missing)
    }

    async fn create_assembly(
        &self,
        key: &str,
        sources: &[ResolvedSource<'_>],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let mut bytes = Vec::new();
        for source in sources {
            let generation = source.etag()?;
            let source_key = source.source.key.as_str();
            let mut chunks = read_verified(source, |range| {
                self.get_range(source_key, &generation, range)
            })
            .boxed();
            while let Some(chunk) = chunks.next().await {
                bytes.extend_from_slice(&chunk?);
            }
        }
        for piece in tail {
            bytes.extend_from_slice(&piece);
        }
        verify_expected(key, expected, &Checksum::crc32c(&bytes))?;
        self.create_checked(key, bytes.into(), expected).await
    }

    async fn compose_assembly(
        &self,
        key: &str,
        sources: &[ResolvedSource<'_>],
        tail: Vec<Bytes>,
        expected: &Checksum,
        intermediates: &mut Intermediates,
    ) -> Result<ObjectMetadata> {
        let mut composed = Vec::with_capacity(sources.len());
        for source in sources {
            if source.source.range.is_some() {
                let name = intermediates.next_name(self);
                let uploaded = self.upload_range(&name, source).await?;
                composed.push((name, uploaded));
            } else {
                composed.push((source.source.key.clone(), source.source.metadata.clone()));
            }
        }
        let stored = composed
            .iter()
            .map(|(name, metadata)| {
                let checksum = metadata.checksum.as_ref().ok_or_else(|| {
                    ObjectStoreError::StoredChecksumMissing {
                        object_key: name.clone(),
                    }
                })?;
                Ok((checksum, metadata.size_bytes))
            })
            .collect::<Result<Vec<_>>>()?;
        let checksum = combined_checksum(ChecksumAlgorithm::Crc32c, stored.into_iter(), &tail)
            .ok_or_else(|| ObjectStoreError::ChecksumMismatch {
                object_key: key.to_owned(),
            })?;
        verify_expected(key, expected, &checksum)?;
        let mut objects = composed
            .iter()
            .map(|(name, metadata)| self.compose_source(name, metadata))
            .collect::<Result<Vec<_>>>()?;
        if !tail.is_empty() || objects.is_empty() {
            let name = intermediates.next_name(self);
            let mut reader = PartReader::new(
                stream::iter(tail.into_iter().map(Ok)).boxed(),
                loonfs_types::usize_from_u64(READ_BYTES),
            );
            let head = reader.next_part().await?.unwrap_or_default();
            let metadata = self.put_if_absent(&name, head, reader, 1).await?;
            objects.push(self.compose_source(&name, &metadata)?);
        }
        while objects.len() > GCS_MAX_COMPOSE_SOURCES {
            let mut next = Vec::new();
            for group in objects.chunks(GCS_MAX_COMPOSE_SOURCES) {
                let name = intermediates.next_name(self);
                let metadata = self.compose(&name, group.to_vec()).await?;
                next.push(self.compose_source(&name, &metadata)?);
            }
            objects = next;
        }
        let metadata = self.compose(key, objects).await?;
        if metadata.checksum.as_ref() != Some(expected) {
            // The composed object stays for collection; no writer deletes an
            // immutable key another writer may have accepted.
            return Err(ObjectStoreError::ChecksumMismatch {
                object_key: key.to_owned(),
            });
        }
        Ok(metadata)
    }
}
