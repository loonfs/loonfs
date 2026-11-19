//! Create-only commits, block copies, and bounded reads for Azure
//! assemblies.

use super::{block_id, AzureRequestSigner, Block};
use crate::assembly::{
    combined_checksum, plan_parts, read_assembly_part, resolve_sources, tail_bytes,
    verify_expected, ResolvedSource,
};
use crate::configured::ConfiguredObjectStoreKind;
use crate::object_store::invalid_request;
use crate::provider_limits::{PROVIDER_MAX_MULTIPART_PARTS, PROVIDER_MULTIPART_THRESHOLD_BYTES};
use crate::signed_upload::ProviderSigner;
use crate::{AssemblySource, ByteRange, ObjectMetadata, Result};
use bytes::Bytes;
use loonfs_types::{Checksum, ChecksumAlgorithm};

impl AzureRequestSigner {
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
            self.assemble_blocks(key, &resolved, &tail, size_bytes, expected)
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
        let bytes = read_assembly_part(self, sources, tail, &whole).await?;
        verify_expected(key, expected, &Checksum::crc64nvme(&bytes))?;
        self.create_checked(key, bytes, expected).await
    }

    /// Copies each source run of at least the provider's smallest part into
    /// blocks of at most [`super::MAX_BLOCK_BYTES`], uploads the small runs
    /// and the tail as blocks, and commits the blocks once their combined
    /// CRC-64/NVME matches `expected`.
    async fn assemble_blocks(
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
        let plan = plan_parts(
            &lengths,
            tail_bytes(tail),
            ConfiguredObjectStoreKind::AzureBlob.part_sizing(),
        );
        if plan.len() > PROVIDER_MAX_MULTIPART_PARTS as usize {
            return Err(invalid_request(
                key,
                "assembly exceeds the provider part limit",
            ));
        }
        let upload = loonfs_types::generated_id("blk");
        let mut blocks: Vec<Block> = Vec::with_capacity(plan.len());
        for (index, planned) in plan.iter().enumerate() {
            let id = block_id(&upload, index);
            let block = if let Some(source_index) = planned.source {
                let source = &sources[source_index];
                let start: u64 = lengths[..source_index].iter().sum();
                let range = ByteRange {
                    start_inclusive: source.range.start_inclusive + planned.range.start_inclusive
                        - start,
                    end_exclusive: source.range.start_inclusive + planned.range.end_exclusive
                        - start,
                };
                self.copy_block(key, id, source, &range).await?
            } else {
                let bytes = read_assembly_part(self, sources, tail, &planned.range).await?;
                self.put_block(key, id, bytes).await?
            };
            blocks.push(block);
        }
        let crc = combined_checksum(
            ChecksumAlgorithm::Crc64nvme,
            blocks
                .iter()
                .zip(&plan)
                .map(|(block, planned)| (&block.checksum, planned.range.len_bytes())),
            &[],
        )
        .expect("blocks should have valid CRC-64/NVME checksums");
        verify_expected(key, expected, &crc)?;
        self.put_block_list(key, &blocks, size_bytes, expected)
            .await
    }
}
