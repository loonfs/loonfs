//! ID newtypes that no domain module defines, with the validators and limits that ID types
//! share.
//!
//! The macros in `id_macros.rs` generate every ID type, including the ID types in other
//! modules.

use crate::hex::{hex_encode_bytes, is_lower_hex_byte};
use crate::id_macros::{numeric_id, string_id, validation_error};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// The characters of a generated ID body, in the order of the 5-bit values
/// that they spell.
const GENERATED_ID_ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Length of a generated ID body: two zero bits and a 128-bit UUID, at 5 bits
/// per ASCII character.
pub const GENERATED_ID_BODY_BYTES: usize = 26;

validation_error!(
    NamespaceIdValidationError,
    "invalid namespace_id {value:?}: {reason}"
);
validation_error!(
    CommitIdValidationError,
    "invalid commit_id {value:?}: {reason}"
);
validation_error!(
    GeneratedIdValidationError,
    "invalid generated id {value:?}: {reason}"
);
validation_error!(
    NameKeyValidationError,
    "invalid name_key {value:?}: {reason}"
);
validation_error!(
    WriterIdValidationError,
    "invalid writer_id {value:?}: {reason}"
);
validation_error!(
    BindingVersionValidationError,
    "invalid binding_version {value:?}: {reason}"
);

/// Generates a project-standard opaque durable identifier.
///
/// The ID is the prefix, an underscore, and the 26-character lowercase
/// Crockford base 32 spelling of a random version 4 UUID, such as
/// `upl_3s0drmj8em84yb4mnetqkz2j0x`.
///
/// Every generated ID type has a `generate()` constructor that calls this
/// function, except `InodeId`, whose `generate()` draws a version 7 UUID
/// through `random_uuid_v7`. This function stays public for free-form
/// generated labels that have no validated ID type, such as a server's
/// per-request ID.
pub fn generated_id(prefix: &'static str) -> String {
    format!(
        "{prefix}_{}",
        encode_base_32(random_uuid_v4(), GENERATED_ID_BODY_BYTES)
    )
}

/// Lists, in order, every string of `width` characters that a generated ID
/// body can start with.
///
/// Every generated ID body starts with exactly one of the strings. Width 0
/// lists the empty string. The first character takes 8 values, because it
/// spells the two zero bits and the top three bits of the UUID, and each later
/// character takes 32.
pub fn generated_id_body_prefixes(width: usize) -> impl ExactSizeIterator<Item = String> {
    let count = match width {
        0 => 1,
        _ => u32::try_from(width - 1)
            .ok()
            .and_then(|later| 32_usize.checked_pow(later))
            .and_then(|later| later.checked_mul(8))
            .expect("the prefix count should fit in a `usize`"),
    };
    (0..count).map(move |index| encode_base_32(u128::from(crate::u64_from_usize(index)), width))
}

/// Draws a random version 4 UUID.
pub(crate) fn random_uuid_v4() -> u128 {
    let mut bytes = random_bytes::<16>();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    u128::from_be_bytes(bytes)
}

/// Draws a version 7 UUID whose top 48 bits are the low 48 bits of
/// `created_at_ms`, a time in milliseconds since the Unix epoch.
pub(crate) fn random_uuid_v7(created_at_ms: u64) -> u128 {
    let mut bytes = random_bytes::<16>();
    bytes[..6].copy_from_slice(&created_at_ms.to_be_bytes()[2..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    u128::from_be_bytes(bytes)
}

/// Draws `N` fresh random bytes.
fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0_u8; N];
    getrandom::fill(&mut bytes).expect("the system random generator should be available");
    bytes
}

/// Spells the low `5 * chars` bits of `value`, most significant first, as one
/// character for each 5 bits.
fn encode_base_32(value: u128, chars: usize) -> String {
    (0..chars)
        .rev()
        .map(|position| {
            let digit = u8::try_from((value >> (5 * position)) & 0x1f)
                .expect("a 5-bit value should fit in a byte");
            char::from(GENERATED_ID_ALPHABET[usize::from(digit)])
        })
        .collect()
}

/// Spells a 128-bit value as a generated ID body.
pub(crate) fn encode_generated_id_body(value: u128) -> String {
    encode_base_32(value, GENERATED_ID_BODY_BYTES)
}

/// Returns the 128-bit value that a generated ID body spells, or `None` when
/// `body` isn't a valid body.
pub(crate) fn decode_generated_id_body(body: &str) -> Option<u128> {
    if generated_id_body_error(body).is_some() {
        return None;
    }
    body.bytes().try_fold(0_u128, |value, byte| {
        let digit = GENERATED_ID_ALPHABET
            .iter()
            .position(|character| *character == byte)?;
        Some((value << 5) | u128::try_from(digit).ok()?)
    })
}

fn validate_generated_id(
    prefix: &'static str,
    value: &str,
) -> Result<(), GeneratedIdValidationError> {
    let expected_prefix = format!("{prefix}_");
    let Some(body) = value.strip_prefix(&expected_prefix) else {
        return Err(GeneratedIdValidationError::new(
            value,
            format!("must start with `{expected_prefix}`"),
        ));
    };
    match generated_id_body_error(body) {
        Some(reason) => Err(GeneratedIdValidationError::new(value, reason)),
        None => Ok(()),
    }
}

/// Returns why `body` isn't a generated ID body, or `None` when it is one.
fn generated_id_body_error(body: &str) -> Option<String> {
    if body.len() != GENERATED_ID_BODY_BYTES
        || !body
            .bytes()
            .all(|byte| GENERATED_ID_ALPHABET.contains(&byte))
    {
        return Some(format!(
            "body must be {GENERATED_ID_BODY_BYTES} characters of the lowercase Crockford base 32 \
             alphabet `0123456789abcdefghjkmnpqrstvwxyz`"
        ));
    }
    // The first character spells the two zero bits and the top three bits of
    // the UUID, so a character above `7` would spell a value wider than 128
    // bits.
    if !body.starts_with(|first: char| first <= '7') {
        return Some("body must start with a character from `0` through `7`".to_owned());
    }
    None
}

fn validate_namespace_id(value: &str) -> Result<(), NamespaceIdValidationError> {
    validate_id_grammar(value).map_err(|reason| NamespaceIdValidationError::new(value, reason))?;
    // System tooling (for example the object-store doctor probes) writes
    // under namespace slots that must never collide with user namespaces.
    if value.starts_with("loonfs-") {
        return Err(NamespaceIdValidationError::new(
            value,
            "the `loonfs-` prefix is reserved for LoonFS system namespaces",
        ));
    }
    Ok(())
}

fn validate_commit_id(value: &str) -> Result<(), CommitIdValidationError> {
    validate_id_grammar(value).map_err(|reason| CommitIdValidationError::new(value, reason))
}

/// Maximum name-key length in UTF-8 bytes
/// ([Names and paths](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#14-names-and-paths)), three times the display-name
/// cap, so every key derived from a valid name fits.
pub const MAX_NAME_KEY_BYTES: usize = 768;
const _: () = assert!(MAX_NAME_KEY_BYTES >= 3 * crate::path::MAX_DISPLAY_NAME_BYTES);
/// Maximum validated namespace and commit id length in UTF-8 bytes.
pub const MAX_ID_BYTES: usize = 128;

fn validate_name_key(value: &str) -> Result<(), NameKeyValidationError> {
    if value.is_empty() {
        return Err(NameKeyValidationError::new(value, "must not be empty"));
    }
    if value.contains('/') {
        return Err(NameKeyValidationError::new(value, "must not contain `/`"));
    }
    if matches!(value, "." | "..") {
        return Err(NameKeyValidationError::new(
            value,
            "must not be `.` or `..`",
        ));
    }
    if value.chars().any(|character| character.is_control()) {
        return Err(NameKeyValidationError::new(
            value,
            "must not contain control characters",
        ));
    }
    if value.len() > MAX_NAME_KEY_BYTES {
        // An oversized or hostile name must not ride along in error payloads that serialize onto the wire.
        return Err(NameKeyValidationError::new(
            "",
            format!("exceeds the maximum name key length of {MAX_NAME_KEY_BYTES} bytes"),
        ));
    }
    Ok(())
}

fn validate_id_grammar(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("must not be empty".to_owned());
    }
    if value.len() > MAX_ID_BYTES {
        return Err(format!("must be {MAX_ID_BYTES} bytes or fewer"));
    }
    if value.trim() != value {
        return Err("must not have leading or trailing whitespace".to_owned());
    }
    if matches!(value, "." | "..") {
        return Err("must not be `.` or `..`".to_owned());
    }

    let mut chars = value.chars();
    let first = chars
        .next()
        .expect("an ID that passed the empty check should have a first character");
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err("must start with a lowercase ASCII letter or digit".to_owned());
    }
    if !chars.all(is_allowed_id_tail_char) {
        return Err(
            "must contain only lowercase ASCII letters, digits, `.`, `_`, or `-`".to_owned(),
        );
    }

    Ok(())
}

fn is_allowed_id_tail_char(ch: char) -> bool {
    ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
}

string_id! {
    /// The durable ID of one namespace.
    ///
    /// Each namespace ID refers to one namespace lifetime, and it isn't a display name. Its
    /// serialized form is 1 to 128 lowercase ASCII letters, digits, dots, underscores, or hyphens,
    /// and it starts with a letter or digit. The `loonfs-` prefix is reserved for system use.
    NamespaceId,
    error = NamespaceIdValidationError,
    validate = validate_namespace_id,
    schema(
        // The `loonfs-` reservation stays in the description: a lookahead
        // would state it, but portable pattern dialects (RE2, most SDK
        // generators) reject lookaheads, so the pattern is the grammar only.
        pattern = r"^[a-z0-9][a-z0-9._-]{0,127}$",
        example = "demo"
    )
}

/// Most bytes one writer ID may contain.
pub const MAX_WRITER_ID_BYTES: usize = 256;

string_id! {
    /// A stable writer label that the embedding process supplies.
    WriterId,
    error = WriterIdValidationError,
    validate = |value: &str| {
        if value.trim().is_empty() {
            return Err(WriterIdValidationError::new(value, "must not be blank"));
        }
        if value.len() > MAX_WRITER_ID_BYTES {
            return Err(WriterIdValidationError::new(
                value,
                format!("must be {MAX_WRITER_ID_BYTES} bytes or fewer"),
            ));
        }
        Ok(())
    }
}

string_id! {
    /// An opaque token for one version of a parent and name binding.
    BindingVersion,
    error = BindingVersionValidationError,
    validate = |value: &str| {
        if value.is_empty() {
            return Err(BindingVersionValidationError::new(value, "must not be empty"));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(BindingVersionValidationError::new(
                value,
                "must contain only base64url characters, without padding",
            ));
        }
        Ok(())
    },
    schema(pattern = r"^[A-Za-z0-9_-]+$")
}

string_id! {
    /// The client's idempotency key for one logical commit.
    ///
    /// Reuse the same commit ID when you retry the same request. A commit ID is 1 to 128 lowercase
    /// ASCII letters, digits, dots, underscores, or hyphens, and it starts with a letter or digit.
    /// Generated commit IDs have the form `c_` followed by 26 lowercase Crockford base 32
    /// characters, but callers can supply any value that follows these rules.
    CommitId,
    error = CommitIdValidationError,
    validate = validate_commit_id,
    schema(
        pattern = r"^[a-z0-9][a-z0-9._-]{0,127}$",
        example = "c_7kn71d9dq885x91hejz3gvf9p0"
    )
}

impl CommitId {
    /// Generates a valid random commit id.
    pub fn generate() -> Self {
        Self(generated_id("c"))
    }
}

string_id! {
    /// The durable ID of one pin.
    ///
    /// The manifest number in the ID determines which namespace manifest the pin protects.
    PinId,
    error = GeneratedIdValidationError,
    validate = validate_pin_id,
    schema(
        pattern = r"^pin_[0-9]{20}-[0-9a-f]{16}$",
        example = "pin_00000000000000000001-0000000000000002"
    )
}

impl PinId {
    /// Generates a new pin for the given manifest number.
    pub fn generate(manifest_no: ManifestNo) -> Self {
        let entropy = hex_encode_bytes(&random_bytes::<8>());
        Self::parse(format!("pin_{:020}-{entropy}", manifest_no.0))
            .expect("the pinned manifest number should be valid")
    }

    /// Returns the manifest number encoded in this id.
    pub fn manifest_no(&self) -> ManifestNo {
        ManifestNo(
            self.0[4..24]
                .parse()
                .expect("a pin id should contain a manifest number"),
        )
    }
}

fn validate_pin_id(value: &str) -> Result<(), GeneratedIdValidationError> {
    let valid = value
        .strip_prefix("pin_")
        .and_then(|body| body.split_once('-'))
        .is_some_and(|(number, entropy)| {
            number.len() == 20
                && number.bytes().all(|byte| byte.is_ascii_digit())
                && number
                    .parse::<u64>()
                    .ok()
                    .and_then(|number| ManifestNo::parse(number).ok())
                    .is_some_and(|number| number.0 > 0)
                && entropy.len() == 16
                && entropy.bytes().all(is_lower_hex_byte)
        });
    if !valid {
        return Err(GeneratedIdValidationError::new(value, "must be `pin_` followed by a twenty-digit positive manifest number, `-`, and sixteen lowercase hex characters".to_owned()));
    }
    Ok(())
}

string_id! {
    /// The durable ID of one upload session.
    UploadId,
    prefix = "upl",
    schema(
        pattern = r"^upl_[0-7][0-9a-hjkmnp-tv-z]{25}$",
        example = "upl_2dhwp939xk9r7srv8t5cz5yw4c"
    )
}

string_id! {
    /// The random identity of one immutable content object.
    ContentId,
    prefix = "con",
    schema(
        pattern = r"^con_[0-7][0-9a-hjkmnp-tv-z]{25}$",
        example = "con_4z59p0wjvx9a8b2frdhhf6mat1"
    )
}

string_id! {
    /// Durable id for one metadata segment.
    MetadataSegmentId,
    prefix = "seg"
}

string_id! {
    /// Identifies one streaming metadata compaction job for log correlation only.
    MetadataCompactionId,
    prefix = "cmp"
}

string_id! {
    /// Durable id for one derived-index segment file.
    IndexSegmentId,
    prefix = "idx"
}

string_id! {
    /// The ID of one run of the store contract probe.
    ///
    /// The probe stores its temporary objects under a key prefix that includes this ID.
    ProbeRunId,
    prefix = "probe",
    schema(
        pattern = r"^probe_[0-7][0-9a-hjkmnp-tv-z]{25}$",
        example = "probe_3w7td1ypsd9hq8m3rv7nf7s6hb"
    )
}

string_id! {
    /// The directory entry key that LoonFS derives from a display name.
    ///
    /// Use the name key for exact name preconditions, and use the display name for the spelling
    /// that users see.
    NameKey,
    error = NameKeyValidationError,
    validate = validate_name_key,
    schema(example = "report.txt")
}

impl NameKey {
    /// Computes the lookup key for a display name.
    ///
    /// Every key fits [`MAX_NAME_KEY_BYTES`]: normalization and case folding
    /// expand a code point at most threefold in bytes, and a display name has
    /// at most [`crate::path::MAX_DISPLAY_NAME_BYTES`].
    pub fn for_display_name(display_name: &crate::DisplayName) -> Self {
        Self(crate::name_key_for_display_name(display_name.as_str()))
    }
}

/// Maximum value for an ordinal exposed through the API.
///
/// This is `2^53 - 1`, the largest integer JSON clients can represent
/// without losing precision.
pub const MAX_PUBLIC_INTEGER: u64 = 9_007_199_254_740_991;

/// Returned when an ordinal exceeds [`MAX_PUBLIC_INTEGER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicOrdinalRangeError;

impl fmt::Display for PublicOrdinalRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "must be an integer from 0 through {MAX_PUBLIC_INTEGER}")
    }
}

impl std::error::Error for PublicOrdinalRangeError {}

/// Returns the next ordinal, or `None` if the value is already at the limit.
pub fn next_public_ordinal(current: u64) -> Option<u64> {
    current
        .checked_add(1)
        .filter(|next| *next <= MAX_PUBLIC_INTEGER)
}

numeric_id! {
    /// The number of one revision of a file's content.
    RevisionNo,
    public_ordinal,
    schema_description = "The number of one revision of a file's content. It increases with every new revision of the file, whether from a put, an append, a replacing copy, or a restore."
}

numeric_id! {
    /// The sequence number of one namespace commit.
    ///
    /// Commits become visible in the order of these numbers.
    ChangeSeq,
    public_ordinal,
    schema_description = "The sequence number of one namespace commit. Commits become visible in the order of these numbers."
}

numeric_id! {
    /// Contiguous WAL object number within one namespace.
    WalNo,
    public_ordinal,
    schema_description = "Contiguous WAL object number within one namespace."
}

numeric_id! {
    /// The number of one of a namespace's manifests. Later manifests have higher numbers.
    ///
    /// The manifest number can increase when metadata changes, even if no namespace commit is
    /// written.
    ManifestNo,
    public_ordinal,
    schema_description = "The number of one of a namespace's manifests. Later manifests have higher numbers, and the number can increase when metadata changes, even if no namespace commit is written."
}

numeric_id! {
    /// The number of one run among the runs that a manifest lists.
    ///
    /// A run is the set of segments that one producer wrote together. The namespace manifest and
    /// the grep manifest each keep their own increasing counter, so a run number means nothing
    /// outside the manifest that assigned it.
    RunNo,
    public_ordinal,
    schema_description = "The number of one run among the runs that a manifest lists. A run is the set of segments that one producer wrote together."
}

numeric_id! {
    /// The counter that LoonFS uses to reject writes from an older writer.
    WriterEpoch,
    public_ordinal,
    schema_description = "The counter that LoonFS uses to reject writes from an older writer."
}

numeric_id! {
    /// Counter used to reject publications from an older compactor.
    CompactorEpoch
}

#[cfg(test)]
mod tests {
    use super::{
        next_public_ordinal, BindingVersion, ChangeSeq, CommitId, ContentId, ManifestNo,
        MetadataSegmentId, NameKey, NamespaceId, PinId, RevisionNo, RunNo, UploadId, WalNo,
        WriterEpoch, WriterId, MAX_PUBLIC_INTEGER,
    };
    use crate::AttributesRevisionNo;
    use std::collections::BTreeSet;

    #[test]
    fn public_ordinal_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        assert_eq!(
            next_public_ordinal(MAX_PUBLIC_INTEGER - 1),
            Some(MAX_PUBLIC_INTEGER)
        );
        assert_eq!(next_public_ordinal(MAX_PUBLIC_INTEGER), None);
    }

    #[test]
    fn public_ordinal_inputs_must_fit_the_json_safe_integer_range() {
        macro_rules! assert_range {
            ($type:ty) => {{
                let constructed = <$type>::parse(MAX_PUBLIC_INTEGER)
                    .expect("construct the maximum public ordinal");
                assert_eq!(constructed.0, MAX_PUBLIC_INTEGER);

                let construction_error = <$type>::parse(MAX_PUBLIC_INTEGER + 1)
                    .expect_err("reject a value above the public limit");
                assert_eq!(
                    construction_error.to_string(),
                    "must be an integer from 0 through 9007199254740991"
                );

                let maximum = serde_json::from_str::<$type>(&MAX_PUBLIC_INTEGER.to_string())
                    .expect("deserialize the maximum public ordinal");
                assert_eq!(maximum.0, MAX_PUBLIC_INTEGER);

                let error = serde_json::from_str::<$type>(&(MAX_PUBLIC_INTEGER + 1).to_string())
                    .expect_err("ordinal above the public range");
                assert!(
                    error
                        .to_string()
                        .contains("must be an integer from 0 through 9007199254740991"),
                    "unexpected range error: {error}"
                );
            }};
        }

        assert_range!(RevisionNo);
        assert_range!(ChangeSeq);
        assert_range!(AttributesRevisionNo);
        assert_range!(ManifestNo);
        assert_range!(WalNo);
        assert_range!(RunNo);
        assert_range!(WriterEpoch);

        assert_eq!(
            serde_json::from_str::<crate::CompactorEpoch>(&u64::MAX.to_string())
                .expect("compactor epochs retain the full u64 range"),
            crate::CompactorEpoch(u64::MAX)
        );
    }

    #[test]
    fn namespace_id_parse_accepts_allowed_grammar() {
        let long_id = format!("a{}", "b".repeat(127));
        for value in ["demo", "demo-1", "demo_1", "demo.v1", &long_id] {
            let parsed = NamespaceId::parse(value).expect("valid namespace_id");
            assert_eq!(parsed.as_str(), value);
        }
    }

    #[test]
    fn namespace_id_parse_rejects_invalid_values() {
        let long_id = format!("a{}", "b".repeat(128));
        for value in [
            "", "/", "a/b", ".", "..", " demo", "demo ", "demo\n", "demo?", "demo#", "demo%",
            "Demo", &long_id,
        ] {
            NamespaceId::parse(value)
                .expect_err(&format!("expected invalid namespace_id {value:?}"));
        }
    }

    #[test]
    fn namespace_id_parse_rejects_reserved_system_prefix() {
        NamespaceId::parse("loonfs-doctor-abc")
            .expect_err("the reserved `loonfs-` prefix should be rejected");
        NamespaceId::parse("loonfs-").expect_err("the bare reserved prefix should be rejected");
        // The reservation is a prefix rule, not a substring rule.
        assert_eq!(
            NamespaceId::parse("my-loonfs-notes")
                .expect("non-prefixed use is allowed")
                .as_str(),
            "my-loonfs-notes"
        );
        // Commit ids share the base grammar but not the reservation.
        assert!(CommitId::parse("loonfs-retry-1").is_ok());
    }

    #[test]
    fn writer_id_rejects_blank_and_oversized_text() {
        for value in ["", " ", "\n", " \t "] {
            WriterId::parse(value).expect_err(&format!("accepted {value:?}"));
        }
        WriterId::parse("w".repeat(super::MAX_WRITER_ID_BYTES)).expect("the limit is inclusive");
        assert_eq!(
            WriterId::parse("w".repeat(super::MAX_WRITER_ID_BYTES + 1))
                .expect_err("over the limit")
                .reason(),
            "must be 256 bytes or fewer"
        );
    }

    #[test]
    fn binding_version_requires_nonempty_unpadded_base64url() {
        for value in ["", "YWJj=", "a+b", "a/b", "a.b", "a b"] {
            BindingVersion::parse(value).expect_err(&format!("accepted {value:?}"));
        }
        for value in ["0", "0123456789abcdef", "AZaz09-_"] {
            assert_eq!(
                BindingVersion::parse(value)
                    .expect("valid binding version")
                    .as_str(),
                value
            );
        }
    }

    #[test]
    fn identity_try_from_validates_values() {
        assert_eq!(
            NamespaceId::try_from("demo")
                .expect("valid namespace id")
                .as_str(),
            "demo"
        );
        assert_eq!(
            CommitId::try_from("commit-1")
                .expect("valid commit id")
                .as_str(),
            "commit-1"
        );
        assert_eq!(
            PinId::try_from("pin_00000000000000000001-0000000000000001")
                .expect("valid pin id")
                .as_str(),
            "pin_00000000000000000001-0000000000000001"
        );
        assert_eq!(
            NameKey::try_from("report.txt".to_owned())
                .expect("valid name key")
                .as_str(),
            "report.txt"
        );

        NamespaceId::try_from("invalid/name")
            .expect_err("a namespace ID with a slash should be rejected");
        CommitId::try_from("invalid/name")
            .expect_err("a commit ID with a slash should be rejected");
        PinId::try_from("pin_00000000000000000001-000000000000000g")
            .expect_err("a pin ID with a non-hex digit should be rejected");
        NameKey::try_from("a/b").expect_err("a name key with a slash should be rejected");
    }

    #[test]
    fn identity_deserialize_validates_values() {
        let namespace_id: NamespaceId =
            serde_json::from_str(r#""demo""#).expect("valid namespace id json");
        assert_eq!(namespace_id.as_str(), "demo");
        let commit_id: CommitId =
            serde_json::from_str(r#""commit-1""#).expect("valid commit id json");
        assert_eq!(commit_id.as_str(), "commit-1");
        let pin_id: PinId = serde_json::from_str(r#""pin_00000000000000000001-0000000000000001""#)
            .expect("valid pin id json");
        assert_eq!(pin_id.as_str(), "pin_00000000000000000001-0000000000000001");

        let namespace_error = serde_json::from_str::<NamespaceId>(r#""invalid/name""#)
            .expect_err("invalid namespace id json");
        assert!(namespace_error.to_string().contains("namespace_id"));
        let commit_error = serde_json::from_str::<CommitId>(r#""invalid/name""#)
            .expect_err("invalid commit id json");
        assert!(commit_error.to_string().contains("commit_id"));
        let pin_error =
            serde_json::from_str::<PinId>(r#""pin_00000000000000000001-000000000000000g""#)
                .expect_err("invalid pin id json");
        assert!(pin_error.to_string().contains("generated id"));
    }

    #[test]
    fn generated_upload_wal_metadata_segment_and_pin_ids_reject_hyphenated_ids() {
        assert!(UploadId::parse("upl_00000000000000000000000001").is_ok());
        assert!(MetadataSegmentId::parse("seg_00000000000000000000000001").is_ok());
        assert!(PinId::parse("pin_00000000000000000001-0000000000000001").is_ok());
        UploadId::parse(["upl", "123"].join("-"))
            .expect_err("a hyphenated upload ID should be rejected");
        // The two positional families are told apart by their prefix, never
        // by context.
        MetadataSegmentId::parse(["seg", "123"].join("-"))
            .expect_err("a hyphenated segment ID should be rejected");
        PinId::parse(["chk", "123"].join("-"))
            .expect_err("a pin ID with another prefix should be rejected");
    }

    #[test]
    fn generated_runtime_ids_spell_version_4_uuids_in_base_32() {
        assert_eq!(
            super::encode_base_32(0x7903_7149_21d4_413c_b252_aed5_e7f1_481d, 26),
            "3s0drmj8em84yb4mnetqkz2j0x"
        );
        assert_eq!(
            super::encode_base_32(u128::MAX, 26),
            "7zzzzzzzzzzzzzzzzzzzzzzzzz"
        );
        let upload_id = UploadId::generate();
        let metadata_segment_id = MetadataSegmentId::generate();
        let pin_id = PinId::generate(ManifestNo(1));

        assert_generated_id_shape(upload_id.as_str(), "upl");
        assert_generated_id_shape(metadata_segment_id.as_str(), "seg");
        assert_eq!(
            super::generated_id_body_prefixes(0).collect::<Vec<_>>(),
            [""]
        );
        assert_eq!(
            super::generated_id_body_prefixes(1).collect::<Vec<_>>(),
            ["0", "1", "2", "3", "4", "5", "6", "7"]
        );
        let prefixes: Vec<_> = super::generated_id_body_prefixes(2).collect();
        assert_eq!(prefixes.len(), 256);
        assert!(prefixes.is_sorted());
        assert_eq!(
            prefixes
                .iter()
                .filter(|prefix| upload_id.as_str()[4..].starts_with(prefix.as_str()))
                .count(),
            1
        );
        assert_eq!(pin_id.manifest_no(), ManifestNo(1));
        assert!(UploadId::parse(upload_id.as_str()).is_ok());
        assert!(MetadataSegmentId::parse(metadata_segment_id.as_str()).is_ok());
        assert!(PinId::parse(pin_id.as_str()).is_ok());
    }

    #[test]
    fn pin_ids_order_and_validate_their_manifest_numbers() {
        let first = PinId::parse("pin_00000000000000000009-ffffffffffffffff").expect("pin");
        let second = PinId::parse("pin_00000000000000000010-0000000000000000").expect("pin");
        assert!(first < second);
        assert_eq!(second.manifest_no(), ManifestNo(10));
        let decoded: PinId =
            serde_json::from_str(&serde_json::to_string(&second).expect("serialize pin id"))
                .expect("decode pin id");
        assert_eq!(decoded, second);
        for invalid in [
            "pin_00000000000000000000-0000000000000000".to_owned(),
            format!("pin_{:020}-0000000000000000", MAX_PUBLIC_INTEGER + 1),
            "pin_00000000000000000001-000000000000000G".to_owned(),
            "pin_1-0000000000000000".to_owned(),
            "pin_00000000000000000001-00000000000000000".to_owned(),
        ] {
            PinId::parse(&invalid).expect_err(&format!("{invalid} should be rejected"));
        }
    }

    #[test]
    fn generated_content_ids_are_unique() {
        let mut ids = BTreeSet::new();
        for _ in 0..512 {
            let id = ContentId::generate();
            assert_generated_id_shape(id.as_str(), "con");
            assert!(
                ids.insert(id.clone()),
                "generated duplicate content id {id}"
            );
        }
    }

    #[test]
    fn content_id_parse_requires_the_generated_id_shape() {
        for valid in [
            "con_3s0drmj8em84yb4mnetqkz2j0x",
            "con_7zzzzzzzzzzzzzzzzzzzzzzzzz",
        ] {
            assert!(ContentId::parse(valid).is_ok(), "{valid}");
        }
        for invalid in [
            "upl_3s0drmj8em84yb4mnetqkz2j0x",
            "con_3S0DRMJ8EM84YB4MNETQKZ2J0X",
            "con_8s0drmj8em84yb4mnetqkz2j0x",
            "con_3s0drmj8em84yb4mnetqkz2j0",
            "con_3s0drmj8em84yb4mnetqkz2j0xx",
            "con_3s0drmj8em84yb4mnetqkz2j0i",
            "con_3s0drmj8em84yb4mnetqkz2j0u",
            "con_0123456789abcdef0123456789abcdef",
        ] {
            ContentId::parse(invalid).expect_err(invalid);
        }
    }

    fn assert_generated_id_shape(value: &str, prefix: &str) {
        let expected_prefix = format!("{prefix}_");
        let body = value
            .strip_prefix(&expected_prefix)
            .expect("generated id prefix");
        let uuid = body.bytes().fold(0_u128, |uuid, byte| {
            let digit = super::GENERATED_ID_ALPHABET
                .iter()
                .position(|candidate| *candidate == byte)
                .expect("a base 32 character");
            (uuid << 5) | digit as u128
        });
        assert_eq!(super::encode_base_32(uuid, 26), body);
        assert_eq!((uuid >> 76) & 0xf, 4, "version 4: {value}");
        assert_eq!((uuid >> 62) & 0x3, 0b10, "RFC 9562 variant: {value}");
    }

    #[test]
    fn name_key_parse_rejects_invalid_values() {
        assert_eq!(
            NameKey::parse("").expect_err("empty").reason(),
            "must not be empty"
        );
        assert_eq!(
            NameKey::parse("a/b").expect_err("slash").reason(),
            "must not contain `/`"
        );
        assert_eq!(
            NameKey::parse(".").expect_err("dot").reason(),
            "must not be `.` or `..`"
        );
        assert_eq!(
            NameKey::parse("a\u{0}b").expect_err("control").reason(),
            "must not contain control characters"
        );
        NameKey::parse("k".repeat(super::MAX_NAME_KEY_BYTES)).expect("cap is inclusive");
        assert_eq!(
            NameKey::parse("k".repeat(super::MAX_NAME_KEY_BYTES + 1))
                .expect_err("over cap")
                .reason(),
            "exceeds the maximum name key length of 768 bytes"
        );
    }

    #[test]
    fn name_key_serializes_as_string_and_validates_deserialize() {
        let name_key = NameKey::parse("report.txt").expect("valid name key");

        assert_eq!(
            serde_json::to_string(&name_key).expect("serialize name key"),
            "\"report.txt\""
        );
        assert_eq!(
            serde_json::from_str::<NameKey>("\"report.txt\"").expect("deserialize name key"),
            name_key
        );
        serde_json::from_str::<NameKey>("\"a/b\"")
            .expect_err("a name key with a slash should be rejected");
    }
}
