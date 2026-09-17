//! Types for access grants on inodes.
//!
//! A principal is an application-assigned identity such as a user, a group,
//! or a public audience. LoonFS stores principal ids as opaque strings and
//! never resolves them.

use crate::ids::{numeric_id, string_id, validation_error};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;

const MAX_PRINCIPAL_ID_BYTES: usize = 256;
/// Most principals one access row may name.
pub const MAX_ACCESS_GRANT_ENTRIES: usize = 1_000;
/// Most bytes of principal ids one access row may hold, summed over its entries.
pub const MAX_ACCESS_GRANTS_PRINCIPAL_BYTES: usize = 65_536;

validation_error!(
    PrincipalIdValidationError,
    "invalid principal_id {value:?}: {reason}"
);

validation_error!(
    PrincipalScopeValidationError,
    "invalid principal_scope {value:?}: {reason}"
);

string_id! {
    /// A validated principal identifier supplied by the application.
    ///
    /// Principal IDs contain 1 to 256 visible ASCII characters (0x21 through 0x7E).
    PrincipalId,
    error = PrincipalIdValidationError,
    validate = validate_principal_id,
    schema(
        description = "Stable opaque principal id containing 1 to 256 visible ASCII characters.",
        pattern = r"^[\x21-\x7E]{1,256}$",
        example = "prn_8f3c"
    )
}

string_id! {
    /// The identity domain a namespace's principal ids belong to.
    ///
    /// A deployment that did not issue a namespace's grants refuses to
    /// interpret them by comparing this value. Same grammar as a principal id.
    PrincipalScope,
    error = PrincipalScopeValidationError,
    validate = validate_principal_scope,
    schema(
        description = "Opaque identity-domain id containing 1 to 256 visible ASCII characters.",
        pattern = r"^[\x21-\x7E]{1,256}$",
        example = "org_acme"
    )
}

numeric_id! {
    /// Monotonic per-inode access revision.
    AccessRevisionNo,
    public_ordinal,
    schema_description = "Monotonic per-inode access revision. It increases with every accepted access update."
}

fn visible_ascii_reason(value: &str) -> Option<&'static str> {
    if value.is_empty() {
        return Some("must not be empty");
    }
    if value.len() > MAX_PRINCIPAL_ID_BYTES {
        return Some("must be 256 bytes or fewer");
    }
    if !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return Some("must contain only visible ASCII characters");
    }
    None
}

fn validate_principal_id(value: &str) -> Result<(), PrincipalIdValidationError> {
    visible_ascii_reason(value).map_or(Ok(()), |reason| {
        Err(PrincipalIdValidationError::new(value, reason))
    })
}

fn validate_principal_scope(value: &str) -> Result<(), PrincipalScopeValidationError> {
    visible_ascii_reason(value).map_or(Ok(()), |reason| {
        Err(PrincipalScopeValidationError::new(value, reason))
    })
}

/// One right a grant can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum AccessRight {
    /// Read the item: names and kinds of a directory's entries, or a file's current bytes.
    Read,
    /// Read older revisions and snapshots.
    History,
    /// Change the item: new revisions and attributes.
    Write,
    /// Add an entry to a directory.
    Create,
    /// Remove an entry from a directory.
    Remove,
    /// Grant a subset of one's own rights to others.
    Share,
    /// Change any grant or the boundary.
    Manage,
    /// Every right on every inode. Valid only on the root inode's row.
    Admin,
}

impl AccessRight {
    /// Every right, in the order the durable encoding lists them.
    pub const ALL: [Self; 8] = [
        Self::Read,
        Self::History,
        Self::Write,
        Self::Create,
        Self::Remove,
        Self::Share,
        Self::Manage,
        Self::Admin,
    ];

    /// Returns the serialized name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::History => "history",
            Self::Write => "write",
            Self::Create => "create",
            Self::Remove => "remove",
            Self::Share => "share",
            Self::Manage => "manage",
            Self::Admin => "admin",
        }
    }

    const fn bit(self) -> u8 {
        1 << (self as u8)
    }
}

/// A set of rights. Encoded as a sorted list of distinct right names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = Vec<AccessRight>))]
pub struct AccessRights(u8);

impl AccessRights {
    /// The set with no rights.
    pub const EMPTY: Self = Self(0);

    /// Whether the set holds `right`.
    pub fn contains(self, right: AccessRight) -> bool {
        self.0 & right.bit() != 0
    }

    /// Adds `right` to the set.
    pub fn insert(&mut self, right: AccessRight) {
        self.0 |= right.bit();
    }

    /// Rights in either set.
    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Rights in `self` that `other` lacks.
    pub fn difference(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// Whether every right in `self` is also in `other`.
    pub fn is_subset_of(self, other: Self) -> bool {
        self.0 & !other.0 == 0
    }

    /// Whether the set holds no rights.
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The rights in encoding order.
    pub fn iter(self) -> impl Iterator<Item = AccessRight> {
        AccessRight::ALL
            .into_iter()
            .filter(move |right| self.contains(*right))
    }
}

impl FromIterator<AccessRight> for AccessRights {
    fn from_iter<I: IntoIterator<Item = AccessRight>>(rights: I) -> Self {
        rights.into_iter().fold(Self::EMPTY, |mut set, right| {
            set.insert(right);
            set
        })
    }
}

impl Serialize for AccessRights {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for AccessRights {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let names = Vec::<AccessRight>::deserialize(deserializer)?;
        let mut rights = Self::EMPTY;
        for right in names {
            if rights.contains(right) {
                return Err(serde::de::Error::custom(format!(
                    "duplicate right `{}`",
                    right.as_str()
                )));
            }
            rights.insert(right);
        }
        Ok(rights)
    }
}

/// A validated map from principal to rights, limited to
/// [`MAX_ACCESS_GRANT_ENTRIES`] entries and [`MAX_ACCESS_GRANTS_PRINCIPAL_BYTES`]
/// bytes of principal ids. No entry has an empty set of rights.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = std::collections::BTreeMap<String, AccessRights>))]
#[serde(transparent)]
pub struct AccessGrants(BTreeMap<PrincipalId, AccessRights>);

impl AccessGrants {
    /// Validates `entries` against the caps and the no-empty-rights rule.
    pub fn new(entries: BTreeMap<PrincipalId, AccessRights>) -> Result<Self, AccessGrantsError> {
        if entries.len() > MAX_ACCESS_GRANT_ENTRIES {
            return Err(AccessGrantsError::TooManyEntries {
                entries: entries.len(),
            });
        }
        if let Some((principal_id, _)) = entries.iter().find(|(_, rights)| rights.is_empty()) {
            return Err(AccessGrantsError::EmptyRights {
                principal_id: principal_id.clone(),
            });
        }
        let principal_bytes = entries.keys().map(|id| id.as_str().len()).sum::<usize>();
        if principal_bytes > MAX_ACCESS_GRANTS_PRINCIPAL_BYTES {
            return Err(AccessGrantsError::TooManyPrincipalBytes { principal_bytes });
        }
        Ok(Self(entries))
    }

    /// The rights granted to `principal_id`, empty when it has no entry.
    pub fn get(&self, principal_id: &PrincipalId) -> AccessRights {
        self.0.get(principal_id).copied().unwrap_or_default()
    }

    /// The entries in principal order.
    pub fn iter(&self) -> impl Iterator<Item = (&PrincipalId, AccessRights)> {
        self.0.iter().map(|(id, rights)| (id, *rights))
    }

    /// Number of principals named.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no principal is named.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The underlying map.
    pub fn as_map(&self) -> &BTreeMap<PrincipalId, AccessRights> {
        &self.0
    }

    /// Bytes this map accounts for when decoded: every principal id plus one
    /// byte per right.
    pub fn logical_bytes(&self) -> usize {
        self.0
            .iter()
            .map(|(id, rights)| id.as_str().len() + rights.iter().count())
            .sum()
    }
}

impl TryFrom<BTreeMap<PrincipalId, AccessRights>> for AccessGrants {
    type Error = AccessGrantsError;

    fn try_from(entries: BTreeMap<PrincipalId, AccessRights>) -> Result<Self, Self::Error> {
        Self::new(entries)
    }
}

impl<'de> Deserialize<'de> for AccessGrants {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = BTreeMap::<PrincipalId, AccessRights>::deserialize(deserializer)?;
        Self::new(entries).map_err(serde::de::Error::custom)
    }
}

/// Why a grant map was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AccessGrantsError {
    /// More principals than [`MAX_ACCESS_GRANT_ENTRIES`].
    #[error("access grants name {entries} principals, which exceeds the maximum of {MAX_ACCESS_GRANT_ENTRIES}")]
    TooManyEntries {
        /// Principals the map named.
        entries: usize,
    },
    /// More principal id bytes than [`MAX_ACCESS_GRANTS_PRINCIPAL_BYTES`].
    #[error("access grants hold {principal_bytes} bytes of principal ids, which exceeds the maximum of {MAX_ACCESS_GRANTS_PRINCIPAL_BYTES} bytes")]
    TooManyPrincipalBytes {
        /// Bytes of principal ids the map held.
        principal_bytes: usize,
    },
    /// An entry granted nothing.
    #[error("access grant for `{principal_id}` carries no rights")]
    EmptyRights {
        /// The principal with the empty entry.
        principal_id: PrincipalId,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        AccessGrants, AccessGrantsError, AccessRight, AccessRights, PrincipalId,
        MAX_ACCESS_GRANTS_PRINCIPAL_BYTES, MAX_ACCESS_GRANT_ENTRIES,
    };
    use std::collections::BTreeMap;

    #[test]
    fn access_rights_encode_as_sorted_names_and_reject_repeats_and_unknown_names() {
        let rights: AccessRights = [AccessRight::Manage, AccessRight::Read]
            .into_iter()
            .collect();
        assert_eq!(
            serde_json::to_string(&rights).expect("serialize rights"),
            r#"["read","manage"]"#
        );
        assert!(serde_json::from_str::<AccessRights>(r#"["read","read"]"#).is_err());
        assert!(serde_json::from_str::<AccessRights>(r#"["owner"]"#).is_err());
        let empty = serde_json::to_string(&AccessRights::EMPTY).expect("serialize empty rights");
        assert_eq!(empty, "[]");
        assert_eq!(
            serde_json::from_str::<AccessRights>(&empty).expect("empty rights"),
            AccessRights::EMPTY
        );
    }

    #[test]
    fn access_grants_reject_empty_rights_and_oversized_maps() {
        let principal = PrincipalId::parse("prn_ada").expect("principal");
        let read: AccessRights = [AccessRight::Read].into_iter().collect();
        let long_id_count = MAX_ACCESS_GRANTS_PRINCIPAL_BYTES / 256 + 1;
        assert!(long_id_count < MAX_ACCESS_GRANT_ENTRIES);
        for (entries, expected) in [
            (
                BTreeMap::from([(principal.clone(), AccessRights::EMPTY)]),
                AccessGrantsError::EmptyRights {
                    principal_id: principal,
                },
            ),
            (
                (0..=MAX_ACCESS_GRANT_ENTRIES)
                    .map(|index| {
                        (
                            PrincipalId::parse(format!("prn_{index}")).expect("principal"),
                            read,
                        )
                    })
                    .collect(),
                AccessGrantsError::TooManyEntries {
                    entries: MAX_ACCESS_GRANT_ENTRIES + 1,
                },
            ),
            (
                (0..long_id_count)
                    .map(|index| {
                        (
                            PrincipalId::parse(format!("{index:0256}")).expect("principal"),
                            read,
                        )
                    })
                    .collect(),
                AccessGrantsError::TooManyPrincipalBytes {
                    principal_bytes: long_id_count * 256,
                },
            ),
        ] {
            assert_eq!(
                AccessGrants::new(entries).expect_err("invalid grants"),
                expected
            );
        }
    }
}
