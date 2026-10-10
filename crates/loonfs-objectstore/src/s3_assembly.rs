//! Multipart copy and bounded reads for S3 and R2 assemblies.

use super::{assembled_crc64nvme, S3RequestSigner};
use crate::assembly::{check_expected, plan_parts, source_range, READ_BYTES};
use crate::provider_object_store::MAX_PROVIDER_MULTIPART_PARTS;
use crate::{
    AssemblySource, ByteRange, ConfiguredObjectStoreKind, ObjectMetadata, ObjectStoreError,
    PutMode, Result,
};
use bytes::Bytes;
use loonfs_types::{Checksum, ChecksumAlgorithm};

impl S3RequestSigner {
    pub(super) async fn assemble_parts(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Bytes,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        if expected.algorithm == ChecksumAlgorithm::Crc32c {
            return Err(ObjectStoreError::Unsupported(
                "s3 assembly requires sha256 or crc64nvme",
            ));
        }
        let mut ranges = Vec::with_capacity(sources.len());
        let mut etags = Vec::with_capacity(sources.len());
        for source in sources {
            let (length, etag) = self
                .head_object(&source.key)
                .await?
                .ok_or_else(|| crate::assembly::missing_source(&source.key))?;
            ranges.push(source_range(source, length)?);
            etags.push(etag);
        }
        let lengths: Vec<_> = ranges
            .iter()
            .map(|range| range.end_exclusive - range.start_inclusive)
            .collect();
        let size_bytes = lengths.iter().sum::<u64>() + tail.len() as u64;
        let plan = plan_parts(
            &lengths,
            tail.len() as u64,
            self.kind == ConfiguredObjectStoreKind::CloudflareR2,
        );
        if plan.len() > MAX_PROVIDER_MULTIPART_PARTS {
            return Err(ObjectStoreError::InvalidContentRef(
                "assembly exceeds the provider part limit".to_owned(),
            ));
        }
        let sha256 = (expected.algorithm == ChecksumAlgorithm::Sha256).then_some(expected);
        let upload_id = self.create_upload(key, sha256).await?;
        let mut abort_on_drop = self.abort_on_drop(key, &upload_id);
        let mut parts = Vec::with_capacity(plan.len());
        for planned in &plan {
            let number = parts.len() as u32 + 1;
            let part = if let Some(index) = planned.source {
                let start: u64 = lengths[..index].iter().sum();
                let range = ByteRange {
                    start_inclusive: ranges[index].start_inclusive + planned.range.start_inclusive
                        - start,
                    end_exclusive: ranges[index].start_inclusive + planned.range.end_exclusive
                        - start,
                };
                self.copy_part(
                    key,
                    &upload_id,
                    number,
                    &sources[index].key,
                    &range,
                    &etags[index],
                )
                .await?
            } else {
                let bytes = self
                    .read_assembly_part(sources, &ranges, &etags, &tail, &planned.range)
                    .await?;
                self.upload_part(key, &upload_id, number, bytes).await?
            };
            parts.push(part);
        }
        if parts.is_empty() {
            parts.push(self.upload_part(key, &upload_id, 1, Bytes::new()).await?);
        }
        let crc = assembled_crc64nvme(parts.iter().zip(&plan).map(|(part, planned)| {
            (
                &part.checksum,
                planned.range.end_exclusive - planned.range.start_inclusive,
            )
        }))
        .unwrap_or_else(|| Checksum::crc64nvme(&[]));
        if expected.algorithm == ChecksumAlgorithm::Crc64nvme {
            check_expected(key, expected, &crc)?;
        }
        let etag = self
            .finish_upload(
                key,
                &upload_id,
                &parts,
                Some(&crc),
                &PutMode::CreateIfAbsent,
            )
            .await?;
        abort_on_drop.disarm();
        Ok(ObjectMetadata {
            etag: Some(etag),
            version: None,
            size_bytes,
            last_modified_ms: None,
            attestation: Some(sha256.cloned().unwrap_or(crc)),
        })
    }

    async fn read_assembly_part(
        &self,
        sources: &[AssemblySource],
        ranges: &[ByteRange],
        etags: &[String],
        tail: &Bytes,
        part: &ByteRange,
    ) -> Result<Bytes> {
        let mut bytes = Vec::with_capacity((part.end_exclusive - part.start_inclusive) as usize);
        let mut start = 0;
        for ((source, range), etag) in sources.iter().zip(ranges).zip(etags) {
            let end = start + range.end_exclusive - range.start_inclusive;
            let mut offset = start.max(part.start_inclusive);
            let last = end.min(part.end_exclusive);
            while offset < last {
                let chunk_end = (offset + READ_BYTES).min(last);
                let read_range = ByteRange {
                    start_inclusive: range.start_inclusive + offset - start,
                    end_exclusive: range.start_inclusive + chunk_end - start,
                };
                let chunk = self.get_range(&source.key, &read_range, etag).await?;
                if chunk.len() as u64 != chunk_end - offset {
                    return Err(crate::assembly::missing_source(&source.key));
                }
                bytes.extend_from_slice(&chunk);
                offset = chunk_end;
            }
            start = end;
        }
        if part.end_exclusive > start {
            bytes.extend_from_slice(
                &tail[(part.start_inclusive.saturating_sub(start)) as usize
                    ..(part.end_exclusive - start) as usize],
            );
        }
        Ok(bytes.into())
    }
}
