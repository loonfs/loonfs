//! Create-only GCS media uploads, compose, and temporary object cleanup.

use super::{gcs_object, precondition_failed_when_missing, GcsRequestSigner, GCS_JSON_OBJECTS};
use crate::assembly::{check_expected, combined_checksum, source_range, READ_BYTES};
use crate::keys::temporary_object;
use crate::keyspace::scope_object_key;
use crate::layout::parse_object_key;
use crate::presign::percent_encode_segment;
use crate::provider_object_store::{
    AbortUploadOnDrop, MultipartController, PartReader, PROVIDER_MULTIPART_THRESHOLD_BYTES,
};
use crate::signed_request::SignedResponse;
use crate::{AssemblySource, ByteRange, ObjectMetadata, ObjectStoreError, Result};
use bytes::Bytes;
use futures::{stream, FutureExt, StreamExt};
use http::header::CONTENT_TYPE;
use loonfs_types::{Checksum, ChecksumAlgorithm, NamespaceId};
use object_store::client::HttpRequestBody;

impl GcsRequestSigner {
    async fn object(&self, key: &str) -> Result<ObjectMetadata> {
        let url = format!(
            "{GCS_JSON_OBJECTS}/{}/o/{}",
            percent_encode_segment(&self.bucket),
            self.object_name(key)?
        );
        self.send_authorized(key, http::Request::get(url), HttpRequestBody::empty())
            .await
            .and_then(|response| gcs_object(key, &response.body))
            .map_err(precondition_failed_when_missing)
    }

    fn compose_source(&self, key: &str, metadata: &ObjectMetadata) -> Result<serde_json::Value> {
        let name = scope_object_key(self.key_prefix.as_deref(), key)?;
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
        let body = serde_json::json!({ "sourceObjects": sources, "destination": {} });
        let request = http::Request::post(url).header(CONTENT_TYPE, "application/json");
        let response = self
            .send_authorized(key, request, body.to_string().into())
            .await?;
        gcs_object(key, &response.body)
    }

    async fn get_range(&self, key: &str, generation: &str, range: &ByteRange) -> Result<Bytes> {
        let url = format!(
            "{GCS_JSON_OBJECTS}/{}/o/{}?alt=media&generation={generation}",
            percent_encode_segment(&self.bucket),
            self.object_name(key)?
        );
        let request = http::Request::get(url).header(
            http::header::RANGE,
            format!(
                "bytes={}-{}",
                range.start_inclusive,
                range.end_exclusive - 1
            ),
        );
        let response = self
            .send_authorized(key, request, HttpRequestBody::empty())
            .await
            .map_err(precondition_failed_when_missing)?;
        if response.body.len() as u64 != range.end_exclusive - range.start_inclusive {
            return Err(crate::assembly::missing_source(key));
        }
        Ok(response.body)
    }

    async fn upload_range(
        &self,
        temporary: &str,
        source: &AssemblySource,
        metadata: &ObjectMetadata,
        range: ByteRange,
    ) -> Result<ObjectMetadata> {
        if range.start_inclusive == range.end_exclusive {
            check_expected(
                &source.key,
                &source.checksum,
                &Checksum::compute(source.checksum.algorithm, &[]),
            )?;
        }
        let generation = metadata
            .etag
            .as_deref()
            .ok_or_else(|| crate::assembly::missing_source(&source.key))?;
        let state = loonfs_types::StreamingChecksum::for_algorithm(source.checksum.algorithm);
        let body = stream::try_unfold(
            (range.start_inclusive, state),
            |(offset, mut checksum)| async move {
                if offset == range.end_exclusive {
                    return Ok(None);
                }
                let end = (offset + READ_BYTES).min(range.end_exclusive);
                let bytes = self
                    .get_range(
                        &source.key,
                        generation,
                        &ByteRange {
                            start_inclusive: offset,
                            end_exclusive: end,
                        },
                    )
                    .await?;
                checksum.update(&bytes);
                if end == range.end_exclusive {
                    check_expected(
                        &source.key,
                        &source.checksum,
                        &std::mem::replace(
                            &mut checksum,
                            loonfs_types::StreamingChecksum::for_algorithm(
                                source.checksum.algorithm,
                            ),
                        )
                        .finish(),
                    )?;
                }
                Ok::<_, ObjectStoreError>(Some((bytes, (end, checksum))))
            },
        )
        .boxed();
        let mut reader = PartReader::new(body, READ_BYTES as usize);
        let head = reader.next_part().await?.unwrap_or_default();
        self.put_if_absent(temporary, head, reader, 1).await
    }

    fn temporary_cleanup(&self, key: &str) -> AbortUploadOnDrop {
        let signer = self.clone();
        let temporary = key.to_owned();
        AbortUploadOnDrop::new(
            key,
            async move {
                signer.delete_temporary(&temporary).await;
                Ok(())
            }
            .boxed(),
        )
    }

    // Failed cleanup leaves the object for a sweep of the temporary family.
    async fn delete_temporary(&self, temporary: &str) {
        let deleted: Result<SignedResponse> = async {
            let url = format!(
                "{GCS_JSON_OBJECTS}/{}/o/{}",
                percent_encode_segment(&self.bucket),
                self.object_name(temporary)?,
            );
            self.send_authorized(
                temporary,
                http::Request::delete(url),
                HttpRequestBody::empty(),
            )
            .await
        }
        .await;
        if deleted.is_err() {
            tracing::warn!(
                object_key = temporary,
                operation = "delete",
                "failed to delete an assembly temporary object; it stays until a sweep collects it",
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
        crate::assembly::check_algorithms(ChecksumAlgorithm::Crc32c, key, sources, expected)?;
        let namespace_id = parse_object_key(key)
            .and_then(|parsed| NamespaceId::parse(parsed.owner_namespace_id()).ok())
            .ok_or_else(|| ObjectStoreError::InvalidKey {
                object_key: key.to_owned(),
                message: "an assembled object must belong to a namespace".to_owned(),
            })?;
        let mut resolved = Vec::with_capacity(sources.len());
        for source in sources {
            let metadata = self.object(&source.key).await?;
            let range = source_range(source, metadata.size_bytes)?;
            let checksum = metadata.checksum.as_ref().ok_or_else(|| {
                ObjectStoreError::StoredChecksumMissing {
                    object_key: source.key.clone(),
                }
            })?;
            if source.range.is_none() {
                check_expected(&source.key, &source.checksum, checksum)?;
            }
            resolved.push((metadata, range));
        }
        let actual = combined_checksum(
            ChecksumAlgorithm::Crc32c,
            sources.iter().zip(&resolved).map(|(source, (_, range))| {
                (
                    &source.checksum,
                    range.end_exclusive - range.start_inclusive,
                )
            }),
            &tail,
        )
        .ok_or_else(|| ObjectStoreError::ChecksumMismatch {
            object_key: key.to_owned(),
        })?;
        check_expected(key, expected, &actual)?;
        let size_bytes = resolved
            .iter()
            .map(|(_, range)| range.end_exclusive - range.start_inclusive)
            .sum::<u64>()
            + tail.iter().map(|piece| piece.len() as u64).sum::<u64>();
        if size_bytes < PROVIDER_MULTIPART_THRESHOLD_BYTES {
            return self
                .create_assembly(key, sources, &resolved, tail, expected)
                .await;
        }
        let mut temporary = Vec::new();
        let result = self
            .compose_assembly(
                key,
                sources,
                resolved,
                tail,
                expected,
                &namespace_id,
                &mut temporary,
            )
            .await;
        for (name, mut cleanup) in temporary {
            self.delete_temporary(&name).await;
            cleanup.disarm();
        }
        result.map_err(precondition_failed_when_missing)
    }
    async fn create_assembly(
        &self,
        key: &str,
        sources: &[AssemblySource],
        resolved: &[(ObjectMetadata, ByteRange)],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let mut bytes = Vec::new();
        for (source, (metadata, range)) in sources.iter().zip(resolved) {
            let generation = crate::required_etag(&source.key, metadata.etag.clone())?;
            let start = bytes.len();
            let mut offset = range.start_inclusive;
            while offset < range.end_exclusive {
                let end = (offset + READ_BYTES).min(range.end_exclusive);
                let chunk = self
                    .get_range(
                        &source.key,
                        &generation,
                        &ByteRange {
                            start_inclusive: offset,
                            end_exclusive: end,
                        },
                    )
                    .await?;
                bytes.extend_from_slice(&chunk);
                offset = end;
            }
            check_expected(
                &source.key,
                &source.checksum,
                &Checksum::crc32c(&bytes[start..]),
            )?;
        }
        for piece in tail {
            bytes.extend_from_slice(&piece);
        }
        check_expected(key, expected, &Checksum::crc32c(&bytes))?;
        let mut rest = PartReader::new(stream::empty().boxed(), 1);
        rest.next_part().await?;
        self.put_if_absent(key, bytes.into(), rest, 1).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn compose_assembly(
        &self,
        key: &str,
        sources: &[AssemblySource],
        resolved: Vec<(ObjectMetadata, ByteRange)>,
        tail: Vec<Bytes>,
        expected: &Checksum,
        namespace_id: &NamespaceId,
        temporary: &mut Vec<(String, AbortUploadOnDrop)>,
    ) -> Result<ObjectMetadata> {
        let mut objects = Vec::new();
        let mut checksum = Checksum::crc32c(&[]);
        for (source, (metadata, range)) in sources.iter().zip(resolved) {
            let (name, metadata) = if source.range.is_some() {
                let name = temporary_object(namespace_id);
                temporary.push((name.clone(), self.temporary_cleanup(&name)));
                let uploaded = self.upload_range(&name, source, &metadata, range).await?;
                (name, uploaded)
            } else {
                (source.key.clone(), metadata)
            };
            let crc = metadata.checksum.as_ref().ok_or_else(|| {
                ObjectStoreError::StoredChecksumMissing {
                    object_key: name.clone(),
                }
            })?;
            checksum = checksum
                .crc_combine(crc, metadata.size_bytes)
                .ok_or_else(|| ObjectStoreError::ChecksumMismatch {
                    object_key: name.clone(),
                })?;
            objects.push(self.compose_source(&name, &metadata)?);
        }
        for piece in &tail {
            checksum = checksum
                .crc_combine(&Checksum::crc32c(piece), piece.len() as u64)
                .expect("CRC-32C values should combine");
        }
        check_expected(key, expected, &checksum)?;
        if !tail.is_empty() || objects.is_empty() {
            let name = temporary_object(namespace_id);
            temporary.push((name.clone(), self.temporary_cleanup(&name)));
            let mut reader = PartReader::new(
                stream::iter(tail.into_iter().map(Ok)).boxed(),
                READ_BYTES as usize,
            );
            let head = reader.next_part().await?.unwrap_or_default();
            let metadata = self.put_if_absent(&name, head, reader, 1).await?;
            objects.push(self.compose_source(&name, &metadata)?);
        }
        while objects.len() > 32 {
            let mut next = Vec::new();
            for group in objects.chunks(32) {
                let name = temporary_object(namespace_id);
                temporary.push((name.clone(), self.temporary_cleanup(&name)));
                let metadata = self.compose(&name, group.to_vec()).await?;
                next.push(self.compose_source(&name, &metadata)?);
            }
            objects = next;
        }
        let metadata = self.compose(key, objects).await?;
        let actual = metadata.checksum.as_ref();
        if actual != Some(expected) {
            self.delete_temporary(key).await;
            return Err(ObjectStoreError::ChecksumMismatch {
                object_key: key.to_owned(),
            });
        }
        Ok(metadata)
    }
}
