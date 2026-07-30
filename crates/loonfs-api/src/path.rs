//! The absolute-path grammar: parsing, components, and display names.

use crate::ids::string_id;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Canonical absolute path plus its parsed components.
///
/// Richer than the string-id newtypes (it carries segments), so it exposes
/// only `Display`/`AsRef<str>` on top of its structural accessors instead of
/// the full `string_id!` suite.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AbsolutePath {
    normalized: String,
    components: Vec<PathComponent>,
}

/// One path segment as stored, preserving display spelling.
///
/// Components are only produced by parsing an [`AbsolutePath`] or joining a
/// [`DisplayName`], so there is no fallible string constructor.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathComponent(String);

string_id! {
    /// User-facing spelling of one path component.
    DisplayName,
    error = PathError,
    validate = validate_display_name
}

/// Maximum stored display-name length in UTF-8 bytes: the 255-byte
/// component cap of mainstream filesystems (ext4, APFS, NTFS components)
/// and drives. Names are stored as given, so the cap applies to the bytes
/// as given.
pub const MAX_DISPLAY_NAME_BYTES: usize = 255;

/// Largest canonical absolute path, in UTF-8 bytes. Bounded so every real
/// filesystem, archive format, and sync client can materialize any stored
/// tree; per-component limits alone allowed paths no target could hold.
pub const MAX_PATH_BYTES: usize = 4_096;

/// Deepest directory nesting one path may express.
pub const MAX_PATH_DEPTH: usize = 128;

/// Describes why caller-supplied path or display-name text is not admissible.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PathError {
    /// Reports an empty string where an absolute path was required.
    #[error("absolute path must not be empty")]
    EmptyPath,
    /// Reports a path that does not begin at the namespace root.
    #[error("path `{path:?}` is not absolute")]
    RelativePath {
        /// Rejected path, preserved for an escaped diagnostic.
        path: String,
    },
    /// Reports an explicit current-directory component, which normalization never accepts.
    #[error("path `{path:?}` contains `.` component")]
    DotComponent {
        /// Rejected path, preserved for an escaped diagnostic.
        path: String,
    },
    /// Reports a parent-directory component, which could otherwise escape the requested path.
    #[error("path `{path:?}` contains `..` component")]
    ParentComponent {
        /// Rejected path, preserved for an escaped diagnostic.
        path: String,
    },
    /// Reports an empty string where one stored path component was required.
    #[error("display name must not be empty")]
    EmptyDisplayName,
    /// Reports a display name containing the path-component separator.
    #[error("display name `{display_name:?}` contains `/`")]
    DisplayNameContainsSeparator {
        /// Rejected component spelling, preserved for an escaped diagnostic.
        display_name: String,
    },
    /// Reports `.` or `..`, whose navigation meaning prevents storing them as names.
    #[error("display name `{display_name:?}` is reserved")]
    ReservedDisplayName {
        /// Reserved spelling supplied by the caller.
        display_name: String,
    },
    /// Reports a Unicode control character that cannot appear in a stored display name.
    #[error("display name contains control character U+{code_point:04X}")]
    DisplayNameContainsControlCharacter {
        /// Unicode scalar value of the first rejected control character.
        code_point: u32,
    },
    /// Reports a display name exceeding the stored UTF-8 component limit.
    #[error("display name is {byte_length} bytes; the maximum is {MAX_DISPLAY_NAME_BYTES} bytes")]
    DisplayNameTooLong {
        /// UTF-8 byte length of the rejected display name.
        byte_length: usize,
    },
    /// Reports a path exceeding the total canonical byte bound.
    #[error("path is {byte_length} bytes; the maximum is {MAX_PATH_BYTES} bytes")]
    PathTooLong {
        /// UTF-8 byte length of the rejected canonical path.
        byte_length: usize,
    },
    /// Reports a path nested deeper than the depth bound.
    #[error("path has {depth} components; the maximum is {MAX_PATH_DEPTH}")]
    PathTooDeep {
        /// Component count of the rejected path.
        depth: usize,
    },
    /// Reports a display name no portable target filesystem can hold.
    #[error("display name `{display_name}` {reason}")]
    UnportableDisplayName {
        /// The rejected spelling.
        display_name: String,
        /// Which portability rule it broke.
        reason: &'static str,
    },
    /// Reports a valid display spelling whose canonical lookup key exceeds its durable bound.
    #[error(
        "display name folds to a {byte_length}-byte name key; the maximum is \
         {max} bytes",
        max = crate::ids::MAX_NAME_KEY_BYTES
    )]
    FoldedNameKeyTooLong {
        /// UTF-8 byte length after normalization and case folding.
        byte_length: usize,
    },
}

impl AbsolutePath {
    /// Parses a canonical absolute path while preserving each component's display spelling.
    ///
    /// Empty and relative paths, repeated or trailing separators, explicit `.`
    /// or `..` components, and components outside the [`DisplayName`] grammar
    /// are rejected.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, PathError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(PathError::EmptyPath);
        }
        if !value.starts_with('/') {
            return Err(PathError::RelativePath {
                path: value.to_owned(),
            });
        }
        if value == "/" {
            return Ok(Self::root());
        }

        let mut components = Vec::new();
        for component in value[1..].split('/') {
            if component.is_empty() {
                return Err(PathError::EmptyDisplayName);
            }
            if component == "." {
                return Err(PathError::DotComponent {
                    path: value.to_owned(),
                });
            }
            if component == ".." {
                return Err(PathError::ParentComponent {
                    path: value.to_owned(),
                });
            }
            // Every component must satisfy the display-name grammar: path
            // parsing is the other door components enter through, and
            // [`PathComponent::to_display_name`] converts without re-parsing.
            validate_display_name(component)?;
            components.push(PathComponent(component.to_owned()));
        }
        validate_path_bounds(value.len(), components.len())?;

        Ok(Self::from_components(components))
    }

    /// Constructs the namespace root path without parsing caller input.
    pub fn root() -> Self {
        Self {
            normalized: "/".to_owned(),
            components: Vec::new(),
        }
    }

    /// Returns the canonical absolute spelling, with `/` as the sole root representation.
    pub fn as_str(&self) -> &str {
        &self.normalized
    }

    /// Reports whether the path has no components.
    pub fn is_root(&self) -> bool {
        self.components.is_empty()
    }

    /// Returns components in root-to-leaf order with their original display spelling.
    pub fn components(&self) -> &[PathComponent] {
        &self.components
    }

    /// Returns the path one component above this one, or `None` at the root.
    pub fn parent(&self) -> Option<Self> {
        if self.is_root() {
            return None;
        }
        if self.components.len() == 1 {
            return Some(Self::root());
        }

        Some(Self::from_components(
            self.components[..self.components.len() - 1].to_vec(),
        ))
    }

    /// Returns the leaf component, or `None` when this path is the root.
    pub fn final_component(&self) -> Option<&PathComponent> {
        self.components.last()
    }

    /// Appends an already-validated display name without changing existing component spelling.
    pub fn join(&self, display_name: &DisplayName) -> Self {
        let mut components = self.components.clone();
        components.push(PathComponent(display_name.as_str().to_owned()));
        Self::from_components(components)
    }

    fn from_components(components: Vec<PathComponent>) -> Self {
        let normalized = normalized_path(&components);
        Self {
            normalized,
            components,
        }
    }
}

impl AsRef<str> for AbsolutePath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for AbsolutePath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl PartialEq<&str> for AbsolutePath {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl fmt::Display for AbsolutePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.normalized)
    }
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for AbsolutePath {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::String)
            .description(Some(
                "Validated complete absolute namespace path, serialized as a plain string.",
            ))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for AbsolutePath {}

impl Serialize for AbsolutePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.normalized)
    }
}

impl<'de> Deserialize<'de> for AbsolutePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

fn normalized_path(components: &[PathComponent]) -> String {
    if components.is_empty() {
        "/".to_owned()
    } else {
        format!(
            "/{}",
            components
                .iter()
                .map(PathComponent::as_str)
                .collect::<Vec<_>>()
                .join("/")
        )
    }
}

impl PathComponent {
    /// Returns the display spelling retained when the containing path was parsed.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Copies this parser-validated component into the equivalent `DisplayName`.
    pub fn to_display_name(&self) -> DisplayName {
        DisplayName(self.0.clone())
    }
}

impl AsRef<str> for PathComponent {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for PathComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate_display_name(value: &str) -> Result<(), PathError> {
    if value.is_empty() {
        return Err(PathError::EmptyDisplayName);
    }
    if value.contains('/') {
        return Err(PathError::DisplayNameContainsSeparator {
            display_name: value.to_owned(),
        });
    }
    if value == "." || value == ".." {
        return Err(PathError::ReservedDisplayName {
            display_name: value.to_owned(),
        });
    }
    if let Some(control) = value.chars().find(|character| character.is_control()) {
        return Err(PathError::DisplayNameContainsControlCharacter {
            code_point: control as u32,
        });
    }
    if value.len() > MAX_DISPLAY_NAME_BYTES {
        return Err(PathError::DisplayNameTooLong {
            byte_length: value.len(),
        });
    }
    // Portability floor: names every target filesystem can hold. Windows
    // cannot materialize trailing dots or spaces or its reserved device
    // names, and an all-whitespace name is invisible in every listing.
    // Rejecting here is free pre-release; loosening later is compatible,
    // tightening later would strand stored names.
    if value.chars().all(char::is_whitespace) {
        return Err(PathError::UnportableDisplayName {
            display_name: value.to_owned(),
            reason: "is entirely whitespace",
        });
    }
    if value.ends_with(' ') {
        return Err(PathError::UnportableDisplayName {
            display_name: value.to_owned(),
            reason: "ends with a space, which Windows cannot store",
        });
    }
    if value.ends_with('.') {
        return Err(PathError::UnportableDisplayName {
            display_name: value.to_owned(),
            reason: "ends with a dot, which Windows cannot store",
        });
    }
    if is_windows_reserved_device_name(value) {
        return Err(PathError::UnportableDisplayName {
            display_name: value.to_owned(),
            reason: "is a Windows reserved device name",
        });
    }
    // Every stored name key is derived from an admitted display name, and
    // the derivation site treats an invalid derived key as an invariant
    // violation — so admission must guarantee the derived key stays within
    // the name-key grammar. v0 folds one way for every namespace; if a
    // second rule ever arrives this check moves to the boundary that knows
    // the namespace.
    let folded_length = crate::name_policy::name_key_for_display_name(value).len();
    if folded_length > crate::ids::MAX_NAME_KEY_BYTES {
        return Err(PathError::FoldedNameKeyTooLong {
            byte_length: folded_length,
        });
    }
    Ok(())
}

fn validate_path_bounds(byte_length: usize, depth: usize) -> Result<(), PathError> {
    if byte_length > MAX_PATH_BYTES {
        return Err(PathError::PathTooLong { byte_length });
    }
    if depth > MAX_PATH_DEPTH {
        return Err(PathError::PathTooDeep { depth });
    }
    Ok(())
}

/// Device names Windows reserves regardless of extension or letter case:
/// a file called `CON`, `con.txt`, or `Com1.log` cannot exist there.
fn is_windows_reserved_device_name(value: &str) -> bool {
    let stem = value.split('.').next().unwrap_or(value);
    let stem = stem.trim_end_matches(' ');
    let upper = stem.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && upper[3..].chars().all(|digit| digit.is_ascii_digit())
            && &upper[3..] != "0")
}

impl PathError {
    /// Returns rejected path text suitable for an API error, omitting hostile or oversized names.
    pub fn invalid_path_input(&self) -> &str {
        match self {
            Self::EmptyPath => "",
            Self::RelativePath { path }
            | Self::DotComponent { path }
            | Self::ParentComponent { path } => path,
            Self::EmptyDisplayName => "",
            Self::DisplayNameContainsSeparator { display_name }
            | Self::ReservedDisplayName { display_name } => display_name,
            Self::UnportableDisplayName { display_name, .. } => display_name,
            // Length and control failures do not carry the offending name:
            // an oversized or hostile name must not ride along in error
            // payloads that serialize onto the wire.
            Self::DisplayNameContainsControlCharacter { .. }
            | Self::DisplayNameTooLong { .. }
            | Self::FoldedNameKeyTooLong { .. }
            | Self::PathTooLong { .. }
            | Self::PathTooDeep { .. } => "",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AbsolutePath, DisplayName, PathError};
    use crate::{name_key_for_display_name, NameKey};

    #[test]
    fn unportable_names_are_rejected() {
        for name in [
            "   ",
            "report ",
            "archive.",
            "CON",
            "con.txt",
            "Com1.log",
            "lpt9",
            "aux.files.d",
        ] {
            assert!(
                DisplayName::parse(name).is_err(),
                "`{name}` should be rejected"
            );
        }
        // Names that merely resemble the reserved set stay legal.
        for name in ["CONSOLE", "com10", "lpt10.txt", ".hidden", "a.b"] {
            assert!(
                DisplayName::parse(name).is_ok(),
                "`{name}` should be accepted"
            );
        }
    }

    #[test]
    fn paths_are_bounded_in_bytes_and_depth() {
        let deep = format!("/{}", vec!["d"; super::MAX_PATH_DEPTH + 1].join("/"));
        assert!(matches!(
            AbsolutePath::parse(&deep),
            Err(PathError::PathTooDeep { .. })
        ));
        let long_component = "a".repeat(200);
        let mut long = String::new();
        while long.len() <= super::MAX_PATH_BYTES {
            long.push('/');
            long.push_str(&long_component);
        }
        assert!(matches!(
            AbsolutePath::parse(&long),
            Err(PathError::PathTooLong { .. })
        ));
        let fine = format!("/{}", vec!["d"; super::MAX_PATH_DEPTH].join("/"));
        assert!(AbsolutePath::parse(&fine).is_ok());
    }

    #[test]
    fn absolute_path_root_is_valid() {
        let path = AbsolutePath::parse("/").expect("root should parse");

        assert_eq!(path.as_str(), "/");
        assert!(path.is_root());
        assert!(path.components().is_empty());
        assert!(path.parent().is_none());
        assert!(path.final_component().is_none());
    }

    #[test]
    fn absolute_path_rejects_dot_and_dotdot_components() {
        assert!(matches!(
            AbsolutePath::parse("/docs/./a.txt"),
            Err(PathError::DotComponent { .. })
        ));
        assert!(matches!(
            AbsolutePath::parse("/docs/../a.txt"),
            Err(PathError::ParentComponent { .. })
        ));
    }

    #[test]
    fn absolute_path_rejects_noncanonical_spellings() {
        assert_eq!(AbsolutePath::parse("//a"), Err(PathError::EmptyDisplayName));
        assert_eq!(
            AbsolutePath::parse("/a//b"),
            Err(PathError::EmptyDisplayName)
        );
        assert_eq!(AbsolutePath::parse("/a/"), Err(PathError::EmptyDisplayName));
        assert!(matches!(
            AbsolutePath::parse("a"),
            Err(PathError::RelativePath { .. })
        ));
        assert_eq!(AbsolutePath::parse(""), Err(PathError::EmptyPath));
    }

    #[test]
    fn absolute_path_serde_is_a_validated_plain_string() {
        let path = AbsolutePath::parse("/Docs/ReadMe.TXT").expect("path should parse");

        assert_eq!(
            serde_json::to_string(&path).expect("serialize path"),
            r#""/Docs/ReadMe.TXT""#
        );
        assert_eq!(
            serde_json::from_str::<AbsolutePath>(r#""/Docs/ReadMe.TXT""#)
                .expect("deserialize path"),
            path
        );
        assert!(serde_json::from_str::<AbsolutePath>(r#""relative/path""#).is_err());
    }

    #[test]
    fn absolute_path_parent_final_component_and_join_preserve_display_spelling() {
        let path = AbsolutePath::parse("/Docs/ReadMe.TXT").expect("path should parse");
        let parent = path.parent().expect("non-root path should have parent");

        assert_eq!(parent.as_str(), "/Docs");
        assert_eq!(
            path.final_component()
                .expect("non-root path has a final component")
                .as_str(),
            "ReadMe.TXT"
        );
        assert_eq!(
            parent
                .join(&DisplayName::parse("Child.TXT").expect("display name should parse"))
                .as_str(),
            "/Docs/Child.TXT"
        );
    }

    #[test]
    fn display_name_rejects_invalid_spellings() {
        assert_eq!(DisplayName::parse(""), Err(PathError::EmptyDisplayName));
        assert!(matches!(
            DisplayName::parse("a/b"),
            Err(PathError::DisplayNameContainsSeparator { .. })
        ));
        assert!(matches!(
            DisplayName::parse("."),
            Err(PathError::ReservedDisplayName { .. })
        ));
    }

    #[test]
    fn display_name_rejects_control_characters() {
        assert_eq!(
            DisplayName::parse("a\u{0}b"),
            Err(PathError::DisplayNameContainsControlCharacter { code_point: 0 })
        );
        assert_eq!(
            DisplayName::parse("line\nbreak"),
            Err(PathError::DisplayNameContainsControlCharacter { code_point: 0x0A })
        );
        assert_eq!(
            DisplayName::parse("c1\u{85}"),
            Err(PathError::DisplayNameContainsControlCharacter { code_point: 0x85 })
        );
        // Format characters are not controls; names keep them as given.
        DisplayName::parse("bidi\u{202E}name").expect("format characters are allowed");
    }

    #[test]
    fn display_name_enforces_the_byte_cap_as_stored() {
        DisplayName::parse("a".repeat(super::MAX_DISPLAY_NAME_BYTES))
            .expect("255 bytes is the maximum, inclusive");
        assert_eq!(
            DisplayName::parse("a".repeat(super::MAX_DISPLAY_NAME_BYTES + 1)),
            Err(PathError::DisplayNameTooLong { byte_length: 256 })
        );
        // The cap counts bytes, not characters: 128 two-byte characters
        // exceed it.
        assert_eq!(
            DisplayName::parse("é".repeat(128)),
            Err(PathError::DisplayNameTooLong { byte_length: 256 })
        );
    }

    #[test]
    fn maximal_casefold_expansion_stays_within_the_name_key_cap() {
        // U+0390 case-folds to three code points (six bytes from two): the
        // worst byte expansion in the fold tables. A maximum-length name of
        // it folds to 762 bytes, inside the 768-byte key cap — the
        // headroom [`crate::ids::MAX_NAME_KEY_BYTES`] documents.
        let display_name =
            DisplayName::parse("\u{0390}".repeat(127)).expect("maximal expander parses");
        let key = NameKey::for_display_name(&display_name);
        assert!(key.as_str().len() <= crate::ids::MAX_NAME_KEY_BYTES);
    }

    #[test]
    fn absolute_path_components_satisfy_the_display_name_grammar() {
        assert!(matches!(
            AbsolutePath::parse("/docs/bad\u{0}name"),
            Err(PathError::DisplayNameContainsControlCharacter { code_point: 0 })
        ));
        assert!(matches!(
            AbsolutePath::parse(format!("/docs/{}", "a".repeat(256))),
            Err(PathError::DisplayNameTooLong { .. })
        ));
    }

    #[test]
    fn name_key_matches_folding_helper() {
        let display_name = DisplayName::parse("Cafe\u{301}.TXT").expect("display name");
        let key = NameKey::for_display_name(&display_name);

        assert_eq!(
            key.as_str(),
            name_key_for_display_name(display_name.as_str())
        );
    }
}
