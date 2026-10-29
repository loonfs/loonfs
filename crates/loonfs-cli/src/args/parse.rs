//! Value parsers for the IDs and ordinals that flags accept.

use loonfs_types::{
    AccessRevisionNo, AttributesRevisionNo, ChangeSeq, InodeId, PublicOrdinalRangeError, RevisionNo,
};

pub(crate) fn parse_public_inode_id(value: &str) -> Result<InodeId, String> {
    loonfs_types::public_inode_id::decode(value).map_err(|error| error.to_string())
}

/// Parses a decimal ordinal and checks that it is a public integer.
fn parse_public_ordinal<T>(
    value: &str,
    constructor: fn(u64) -> Result<T, PublicOrdinalRangeError>,
) -> Result<T, String> {
    let value: u64 = value.parse().map_err(|error| format!("{error}"))?;
    constructor(value).map_err(|error| error.to_string())
}

pub(crate) fn parse_change_seq(value: &str) -> Result<ChangeSeq, String> {
    parse_public_ordinal(value, ChangeSeq::parse)
}

pub(crate) fn parse_revision_no(value: &str) -> Result<RevisionNo, String> {
    parse_public_ordinal(value, RevisionNo::parse)
}

pub(crate) fn parse_attributes_revision_no(value: &str) -> Result<AttributesRevisionNo, String> {
    parse_public_ordinal(value, AttributesRevisionNo::parse)
}

pub(crate) fn parse_access_revision_no(value: &str) -> Result<AccessRevisionNo, String> {
    parse_public_ordinal(value, AccessRevisionNo::parse)
}
