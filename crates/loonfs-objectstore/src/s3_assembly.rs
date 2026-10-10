//! Create-only puts, multipart copy, and bounded reads for S3 and R2 assemblies.

use super::{assembled_crc64nvme, S3RequestSigner, MULTIPART_CONTROL_TTL};
use crate::assembly::{check_expected, plan_parts, source_range, READ_BYTES};
use crate::presign::{base64_crc64nvme, DirectPutIssuer, PresignedPutRequest};
use crate::provider_object_store::{
    MAX_PROVIDER_MULTIPART_PARTS, PROVIDER_MULTIPART_THRESHOLD_BYTES,
};
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
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        crate::assembly::check_algorithms(ChecksumAlgorithm::Crc64nvme, key, sources, expected)?;
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
        let size_bytes =
            lengths.iter().sum::<u64>() + tail.iter().map(|piece| piece.len() as u64).sum::<u64>();
        if size_bytes < PROVIDER_MULTIPART_THRESHOLD_BYTES {
            let bytes = self
                .read_assembly_part(
                    sources,
                    &ranges,
                    &etags,
                    &tail,
                    &ByteRange {
                        start_inclusive: 0,
                        end_exclusive: size_bytes,
                    },
                )
                .await?;
            check_expected(key, expected, &Checksum::crc64nvme(&bytes))?;
            return self.create_assembly(key, bytes, expected).await;
        }
        let plan = plan_parts(
            &lengths,
            tail.iter().map(|piece| piece.len() as u64).sum::<u64>(),
            self.kind == ConfiguredObjectStoreKind::CloudflareR2,
        );
        if plan.len() > MAX_PROVIDER_MULTIPART_PARTS {
            return Err(ObjectStoreError::InvalidContentRef(
                "assembly exceeds the provider part limit".to_owned(),
            ));
        }
        let upload_id = self.create_upload(key).await?;
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
        let crc = assembled_crc64nvme(parts.iter().zip(&plan).map(|(part, planned)| {
            (
                &part.checksum,
                planned.range.end_exclusive - planned.range.start_inclusive,
            )
        }))
        .expect("nonempty multipart parts should have valid CRC-64/NVME checksums");
        check_expected(key, expected, &crc)?;
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
            checksum: Some(crc),
        })
    }

    async fn create_assembly(
        &self,
        key: &str,
        bytes: Bytes,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let mut signed = self
            .request_signer
            .presign_put(
                PresignedPutRequest {
                    object_key: key,
                    expires_in: MULTIPART_CONTROL_TTL,
                },
                Self::signing_time(),
            )
            .await?;
        signed.headers.insert(
            "x-amz-checksum-crc64nvme".to_owned(),
            base64_crc64nvme(expected)?,
        );
        let size_bytes = bytes.len() as u64;
        let response = self.send_checked(key, signed, bytes.into()).await?;
        let etag = response
            .headers
            .get(http::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        Ok(ObjectMetadata {
            etag,
            version: None,
            size_bytes,
            last_modified_ms: None,
            checksum: Some(expected.clone()),
        })
    }

    async fn read_assembly_part(
        &self,
        sources: &[AssemblySource],
        ranges: &[ByteRange],
        etags: &[String],
        tail: &[Bytes],
        part: &ByteRange,
    ) -> Result<Bytes> {
        let mut bytes = Vec::with_capacity((part.end_exclusive - part.start_inclusive) as usize);
        let mut start = 0;
        for ((source, range), etag) in sources.iter().zip(ranges).zip(etags) {
            let end = start + range.end_exclusive - range.start_inclusive;
            let mut offset = start.max(part.start_inclusive);
            let last = end.min(part.end_exclusive);
            let first_byte = bytes.len();
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
            if part.start_inclusive <= start && part.end_exclusive >= end {
                check_expected(
                    &source.key,
                    &source.checksum,
                    &Checksum::crc64nvme(&bytes[first_byte..]),
                )?;
            }
            start = end;
        }
        for piece in tail {
            let end = start + piece.len() as u64;
            let first = start.max(part.start_inclusive);
            let last = end.min(part.end_exclusive);
            if first < last {
                bytes.extend_from_slice(&piece[(first - start) as usize..(last - start) as usize]);
            }
            start = end;
        }
        Ok(bytes.into())
    }
}
