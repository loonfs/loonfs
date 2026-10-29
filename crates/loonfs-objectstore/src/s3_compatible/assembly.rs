//! Create-only puts, multipart copy, and bounded reads for S3 and R2 assemblies.

use super::S3RequestSigner;
use crate::assembly::{
    combined_checksum, plan_parts, read_chunks, read_verified, resolve_sources, tail_bytes,
    verify_expected, ResolvedSource,
};
use crate::object_store::invalid_request;
use crate::provider_limits::{PROVIDER_MAX_MULTIPART_PARTS, PROVIDER_MULTIPART_THRESHOLD_BYTES};
use crate::signed_upload::ProviderSigner;
use crate::{AssemblySource, ByteRange, ObjectMetadata, ObjectStoreError, Result};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_types::{Checksum, ChecksumAlgorithm};

impl S3RequestSigner {
    pub(super) async fn assemble_parts(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let resolved = resolve_sources(sources)?;
        for source in &resolved {
            source.etag()?;
        }
        let size_bytes = resolved
            .iter()
            .map(|source| source.range.len_bytes())
            .sum::<u64>()
            + tail_bytes(&tail);
        if size_bytes < PROVIDER_MULTIPART_THRESHOLD_BYTES {
            self.assemble_single(key, &resolved, &tail, size_bytes, expected)
                .await
        } else {
            self.assemble_multipart(key, &resolved, &tail, size_bytes, expected)
                .await
        }
    }

    async fn assemble_single(
        &self,
        key: &str,
        sources: &[ResolvedSource<'_>],
        tail: &[Bytes],
        size_bytes: u64,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let whole = ByteRange {
            start_inclusive: 0,
            end_exclusive: size_bytes,
        };
        let bytes = self.read_assembly_part(sources, tail, &whole).await?;
        verify_expected(key, expected, &Checksum::crc64nvme(&bytes))?;
        self.create_checked(key, bytes, expected).await
    }

    async fn assemble_multipart(
        &self,
        key: &str,
        sources: &[ResolvedSource<'_>],
        tail: &[Bytes],
        size_bytes: u64,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let lengths: Vec<_> = sources
            .iter()
            .map(|source| source.range.len_bytes())
            .collect();
        let plan = plan_parts(&lengths, tail_bytes(tail), self.kind.part_sizing());
        if plan.len() > PROVIDER_MAX_MULTIPART_PARTS as usize {
            return Err(invalid_request(
                key,
                "assembly exceeds the provider part limit",
            ));
        }
        let upload_id = self.create_upload(key).await?;
        let mut abort_on_drop = self.abort_on_drop(key, &upload_id);
        let mut parts = Vec::with_capacity(plan.len());
        for planned in &plan {
            let number = u32::try_from(parts.len())
                .expect("a plan within the provider part limit should fit u32")
                + 1;
            let part = if let Some(index) = planned.source {
                let source = &sources[index];
                let start: u64 = lengths[..index].iter().sum();
                let range = ByteRange {
                    start_inclusive: source.range.start_inclusive + planned.range.start_inclusive
                        - start,
                    end_exclusive: source.range.start_inclusive + planned.range.end_exclusive
                        - start,
                };
                self.copy_part(key, &upload_id, number, source, &range)
                    .await?
            } else {
                let bytes = self
                    .read_assembly_part(sources, tail, &planned.range)
                    .await?;
                self.upload_part(key, &upload_id, number, bytes).await?
            };
            parts.push(part);
        }
        if self.kind.copy_part_reports_checksum() {
            let crc = combined_checksum(
                ChecksumAlgorithm::Crc64nvme,
                parts.iter().zip(&plan).map(|(part, planned)| {
                    (
                        part.checksum
                            .as_ref()
                            .expect("S3 parts should carry checksums"),
                        planned.range.len_bytes(),
                    )
                }),
                &[],
            )
            .expect("multipart parts should have valid CRC-64/NVME checksums");
            verify_expected(key, expected, &crc)?;
        }
        let etag = self
            .complete_started_upload(key, &upload_id, &parts, Some(expected))
            .await?;
        abort_on_drop.disarm();
        if self.kind.completion_checked_by_head() {
            return self.verify_completed_assembly(key, expected).await;
        }
        Ok(ObjectMetadata {
            etag: Some(etag),
            version: None,
            size_bytes,
            last_modified_at_ms: None,
            checksum: Some(expected.clone()),
        })
    }

    async fn verify_completed_assembly(
        &self,
        key: &str,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let metadata =
            self.head_metadata(key)
                .await?
                .ok_or_else(|| ObjectStoreError::NotFound {
                    object_key: key.to_owned(),
                })?;
        let error = match &metadata.checksum {
            Some(actual) if actual == expected => return Ok(metadata),
            Some(_) => ObjectStoreError::ChecksumMismatch {
                object_key: key.to_owned(),
            },
            None => ObjectStoreError::StoredChecksumMissing {
                object_key: key.to_owned(),
            },
        };
        // The object stays for collection. A writer never deletes an immutable
        // key: another writer with the correct expectation may already have
        // accepted this object, and nothing names an unaccepted one.
        Err(error)
    }

    /// Reads the bytes of the assembly that `part` selects, from the sources
    /// and the tail it overlaps. A source that the part covers whole is
    /// checked against its checksum.
    async fn read_assembly_part(
        &self,
        sources: &[ResolvedSource<'_>],
        tail: &[Bytes],
        part: &ByteRange,
    ) -> Result<Bytes> {
        let mut bytes = Vec::with_capacity(loonfs_types::usize_from_u64(part.len_bytes()));
        let mut start = 0;
        for source in sources {
            let end = start + source.range.len_bytes();
            let first = start.max(part.start_inclusive);
            let last = end.min(part.end_exclusive);
            let key = source.source.key.as_str();
            let etag = source.etag()?;
            let read = |range| self.get_range(key, range, &etag);
            let mut chunks = if part.start_inclusive <= start && part.end_exclusive >= end {
                read_verified(source, read).boxed()
            } else if first < last {
                let range = ByteRange {
                    start_inclusive: source.range.start_inclusive + first - start,
                    end_exclusive: source.range.start_inclusive + last - start,
                };
                read_chunks(key, range, read).boxed()
            } else {
                start = end;
                continue;
            };
            while let Some(chunk) = chunks.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            start = end;
        }
        for piece in tail {
            let end = start + piece.len() as u64;
            let first = start.max(part.start_inclusive);
            let last = end.min(part.end_exclusive);
            if first < last {
                let first = loonfs_types::usize_from_u64(first - start);
                let last = loonfs_types::usize_from_u64(last - start);
                bytes.extend_from_slice(&piece[first..last]);
            }
            start = end;
        }
        Ok(bytes.into())
    }
}
