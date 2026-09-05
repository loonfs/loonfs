//! Durable sorted mark tables used by resumable namespace garbage collection.

use crate::manifest::MetadataSegmentRef;
use crate::{ChangeSeq, GcMarkTableId, GcRunId, NamespaceId};
use serde::{Deserialize, Serialize};

/// An immutable sorted table, stored as consecutively numbered pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcMarkTable {
    /// Unique identity; pages are never overwritten or reused by another table.
    pub table_id: GcMarkTableId,
    /// Number of pages, starting at page zero.
    pub page_count: u64,
    /// Number of distinct keys in the table.
    pub entry_count: u64,
}

/// One sorted entry in a GC mark table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcMarkEntry {
    /// Index key. The collector validates its correspondence with the value.
    pub key: String,
    /// What the key protects or asks the marking phase to inspect.
    pub value: GcMarkValue,
}

/// A reference or a revision-segment scan needed to finish marking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GcMarkValue {
    /// An object key that the completed mark set protects.
    Object {},
    /// Verified manifest identity; deduplicates pins that share a basis.
    Manifest {
        /// Exact immutable manifest reference.
        manifest: crate::control::ManifestRef,
    },
    /// A content identity referenced by retained metadata or WAL.
    Content {},
    /// An active checkpoint whose basis object was absent when inspected.
    MissingBasisCheckpoint {},
    /// A manifest whose object is absent; distinct from a verified reference.
    MissingManifest {},
    /// An immutable revision segment whose content references must be marked.
    RevisionSegment {
        /// Verified manifest descriptor needed to read the segment directly.
        segment: MetadataSegmentRef,
        /// Tightest sequence upper bound supplied by a protected manifest.
        max_seq: ChangeSeq,
    },
}

/// One independently verified page of a sorted GC mark table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcMarkPage {
    /// Namespace being collected.
    pub namespace_id: NamespaceId,
    /// Collection whose progress owns this page.
    pub gc_run_id: GcRunId,
    /// Sorted table containing the page.
    pub table_id: GcMarkTableId,
    /// Zero-based position in the table.
    pub page_no: u64,
    /// Strictly increasing keys. Pages are nonempty and bounded by the codec.
    pub entries: Vec<GcMarkEntry>,
}

/// Resume position within one immutable sorted input table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcMarkPosition {
    /// Page containing the next entry, or the table's page count at EOF.
    pub page_no: u64,
    /// Zero-based entry within that page.
    pub entry_no: u32,
}

/// Maximum entries in one independently decoded mark page.
pub const GC_MARK_PAGE_ENTRIES: usize = 512;
/// Maximum encoded page size; oversized pages are rejected before decoding.
pub const GC_MARK_PAGE_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Frozen kind of an immutable GC mark page.
pub const GC_MARK_PAGE_KIND: &str = "gc_mark_page";
/// Format version shared by the mark-page reader and writer.
pub const GC_MARK_PAGE_VERSION: u32 = 1;

/// Encodes one nonempty, strictly sorted page in the shared JSON envelope.
pub fn encode_gc_mark_page(
    page: GcMarkPage,
) -> Result<crate::envelope::EncodedEnvelope<GcMarkPage>, crate::envelope::EnvelopeCodecError> {
    validate_page(&page)?;
    let encoded =
        crate::envelope::encode_json_envelope(GC_MARK_PAGE_KIND, GC_MARK_PAGE_VERSION, page)?;
    validate_page_bytes(encoded.as_bytes())?;
    Ok(encoded)
}

/// Verifies the envelope and the page's strict key order and size bound.
pub fn decode_gc_mark_page(
    bytes: &[u8],
) -> Result<crate::envelope::VerifiedEnvelope<GcMarkPage>, crate::envelope::EnvelopeCodecError> {
    validate_page_bytes(bytes)?;
    let envelope = crate::envelope::decode_json_envelope(bytes, GC_MARK_PAGE_VERSION, |kind| {
        crate::envelope::verify_kind(GC_MARK_PAGE_KIND, kind)
    })?;
    validate_page(envelope.payload())?;
    Ok(envelope)
}

fn validate_page_bytes(bytes: &[u8]) -> Result<(), crate::envelope::EnvelopeCodecError> {
    if bytes.len() > GC_MARK_PAGE_MAX_BYTES {
        return Err(crate::envelope::EnvelopeCodecError::PayloadDecode(
            "GC mark page exceeds 8 MiB".to_owned(),
        ));
    }
    Ok(())
}

fn validate_page(page: &GcMarkPage) -> Result<(), crate::envelope::EnvelopeCodecError> {
    if page.entries.is_empty()
        || page.entries.len() > GC_MARK_PAGE_ENTRIES
        || page
            .entries
            .windows(2)
            .any(|pair| pair[0].key >= pair[1].key)
    {
        return Err(crate::envelope::EnvelopeCodecError::PayloadDecode(
            "GC mark page must contain 1..=512 strictly increasing keys".to_owned(),
        ));
    }
    Ok(())
}

/// Binary carry tables accumulated while marking.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcMarkIndex {
    /// At most one table per binary merge level, low levels first.
    pub levels: Vec<Option<GcMarkTable>>,
    /// A bounded merge that must finish before another source is marked.
    pub merge: Option<GcMarkMerge>,
}

/// Durable positions for a two-table merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcMarkMerge {
    /// Complete immutable input tables.
    pub inputs: [GcMarkTable; 2],
    /// Next input entries; updated only after the output page is confirmed.
    pub positions: [GcMarkPosition; 2],
    /// Output pages confirmed so far; not a complete mark table until EOF.
    pub output: GcMarkTable,
    /// Merge level that receives the completed output.
    pub output_level: u32,
}

/// Server-owned progress for the namespace's one active collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcRunState {
    /// Namespace being collected.
    pub namespace_id: NamespaceId,
    /// Identity carried by client continuation tokens.
    pub gc_run_id: GcRunId,
    /// Monotonic progress number advanced by each successful step CAS.
    pub step_no: u64,
    /// Fixed clock for all eligibility decisions, including resumed calls.
    pub started_at_ms: u64,
    /// Fixed grace policy selected when the run was reserved.
    pub grace_window_ms: u64,
    /// Durable work remaining. Only CAS can advance it.
    pub phase: GcPhase,
}

/// Collection phases; sweeping is representable only after marking finishes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GcPhase {
    /// Reserve the run before reading roots, excluding another new sweep.
    Starting {},
    /// Discover roots and build sorted reference tables.
    Marking {
        /// Root scan and pending merge.
        work: Box<GcMarkWork>,
    },
    /// Read each protected revision segment once, one data block per step.
    Revisions {
        /// Fixed retention summary.
        roots: GcRoots,
        /// Sealed object marks, including revision scan tasks.
        objects: GcMarkTable,
        /// Next entry in the sealed object table.
        position: GcMarkPosition,
        /// Next data block in the current revision segment.
        block_no: u64,
        /// Content marks and their pending merge.
        content: GcMarkIndex,
    },
    /// Merge content references into the object index before sweeping.
    Sealing {
        /// Fixed retention summary.
        roots: GcRoots,
        /// Final object/content union.
        index: GcMarkIndex,
    },
    /// Decide candidates against one complete, immutable mark table.
    Sweeping {
        /// Fixed retention summary.
        roots: GcRoots,
        /// Complete, immutable deletion evidence.
        table: GcMarkTable,
        /// Next candidate family to enumerate.
        family: GcCandidateFamily,
        /// Last key inspected, exclusive on resume.
        last_key: Option<String>,
    },
    /// Reap scratch pages, including abandoned output from older runs.
    Cleaning {
        /// Last scratch key decided.
        last_key: Option<String>,
    },
    /// A subsequent call without this run's token may start a new collection.
    Complete {},
}

/// Small root summary retained alongside the disk-backed reference index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcRoots {
    /// Content store used by this namespace's upload sessions.
    pub content_store_id: crate::ContentStoreId,
    /// Deleted namespaces keep published content permanently at launch.
    pub namespace_deleted: bool,
    /// Incomplete root reads forbid metadata and content reclamation.
    pub degraded: bool,
    /// Historical reference boundary selected using the fixed run clock.
    pub anchor: GcReferenceAnchor,
}

/// Evidence that an old object has also been unreferenced long enough.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GcReferenceAnchor {
    /// Namespace never materialized metadata, or has no live readers.
    NotNeeded {},
    /// A fully aged manifest generation could not be established.
    Missing {},
    /// Lowest head sequence among the protected generation's candidates.
    Manifest {
        /// Lowest materialized sequence across the selected generation.
        head_seq: ChangeSeq,
    },
}

/// Root discovery and its bounded merge state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcMarkWork {
    /// Retention facts carried to the sweep.
    pub roots: GcRoots,
    /// Completed tables plus at most one unfinished merge.
    pub index: GcMarkIndex,
    /// Next source object to inspect.
    pub source: GcMarkSource,
    /// WAL floor frozen with the namespace controls.
    pub floor_seq: ChangeSeq,
    /// Next older WAL pointer. Cleared only upon reaching the exact floor.
    pub wal_tip: Option<crate::control::WalSegmentPointer>,
}

/// Source scan positions contain no growing lists of root identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GcMarkSource {
    /// The namespace's owned root, when present.
    Root {
        /// Owned basis frozen from the namespace controls.
        manifest: Option<crate::control::ManifestRef>,
    },
    /// Readable checkpoint records protect their immutable bases until swept.
    Checkpoints {
        /// Last checkpoint record inspected.
        last_key: Option<String>,
    },
    /// Find the newest generation whose surviving candidates are all aged.
    AnchorDiscovery {
        /// Last key inspected, exclusive on resume.
        last_key: Option<String>,
        /// Whether any recognizable manifest candidate was listed.
        candidate_seen: bool,
        /// Aged candidates in the current generation.
        current: Option<GcManifestRange>,
        /// Previous complete aged generation.
        aged: Option<GcManifestRange>,
    },
    /// Protect every candidate in the selected generation.
    AnchorManifests {
        /// Selected inclusive manifest range.
        range: GcManifestRange,
        /// Last key inspected, exclusive on resume.
        last_key: Option<String>,
    },
    /// Follow and validate one retained WAL segment per step.
    Wal {},
    /// Finish outstanding table merges.
    Done {},
}

/// Inclusive key range for one immutable manifest generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcManifestRange {
    /// Shared publication generation.
    pub manifest_no: crate::ManifestNo,
    /// First surviving aged candidate.
    pub first_key: String,
    /// Last surviving aged candidate.
    pub last_key: String,
}

/// Durable sweep order; data precedes the records that protect it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GcCandidateFamily {
    /// Immutable commit segments.
    WalSegments,
    /// Immutable metadata segments.
    MetadataSegments,
    /// Compaction output and publication protection records.
    CompactionStaging,
    /// Immutable metadata manifests.
    Manifests,
    /// Mutable pin records.
    Checkpoints,
    /// Mutable upload records and unpublished content.
    UploadSessions,
}

impl GcCandidateFamily {
    /// Data families precede the mutable records that protect them.
    pub const ALL: [Self; 6] = [
        Self::WalSegments,
        Self::MetadataSegments,
        Self::CompactionStaging,
        Self::Manifests,
        Self::Checkpoints,
        Self::UploadSessions,
    ];

    /// This family's position in [`Self::ALL`], which is the sweep order.
    pub fn index(self) -> usize {
        match self {
            Self::WalSegments => 0,
            Self::MetadataSegments => 1,
            Self::CompactionStaging => 2,
            Self::Manifests => 3,
            Self::Checkpoints => 4,
            Self::UploadSessions => 5,
        }
    }
}
