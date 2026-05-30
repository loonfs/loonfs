use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AbsolutePath {
    normalized: String,
    components: Vec<PathComponent>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathComponent(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DisplayName(String);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PathError {
    #[error("absolute path must not be empty")]
    EmptyPath,
    #[error("path `{path}` is not absolute")]
    RelativePath { path: String },
    #[error("path `{path}` contains `.` component")]
    DotComponent { path: String },
    #[error("path `{path}` contains `..` component")]
    ParentComponent { path: String },
    #[error("display name must not be empty")]
    EmptyDisplayName,
    #[error("display name `{display_name}` contains `/`")]
    DisplayNameContainsSeparator { display_name: String },
    #[error("display name `{display_name}` is reserved")]
    ReservedDisplayName { display_name: String },
}

impl AbsolutePath {
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

        let mut components = Vec::new();
        for component in value.split('/') {
            if component.is_empty() {
                continue;
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
            components.push(PathComponent(component.to_owned()));
        }

        Ok(Self::from_components(components))
    }

    pub fn root() -> Self {
        Self {
            normalized: "/".to_owned(),
            components: Vec::new(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.normalized
    }

    pub fn is_root(&self) -> bool {
        self.components.is_empty()
    }

    pub fn components(&self) -> &[PathComponent] {
        &self.components
    }

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

    pub fn final_component(&self) -> Option<&PathComponent> {
        self.components.last()
    }

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
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn to_display_name(&self) -> DisplayName {
        DisplayName(self.0.clone())
    }
}

impl DisplayName {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, PathError> {
        let value = value.as_ref();
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
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl PathError {
    pub fn invalid_path_input(&self) -> &str {
        match self {
            Self::EmptyPath => "",
            Self::RelativePath { path }
            | Self::DotComponent { path }
            | Self::ParentComponent { path } => path,
            Self::EmptyDisplayName => "",
            Self::DisplayNameContainsSeparator { display_name }
            | Self::ReservedDisplayName { display_name } => display_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AbsolutePath, DisplayName, PathError};
    use crate::{name_key_for_display_name, NameKey, NamePolicy};

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
    fn absolute_path_rejects_relative_empty_dot_and_dotdot() {
        assert_eq!(AbsolutePath::parse(""), Err(PathError::EmptyPath));
        assert!(matches!(
            AbsolutePath::parse("docs"),
            Err(PathError::RelativePath { .. })
        ));
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
    fn absolute_path_normalizes_repeated_and_trailing_separators() {
        let path = AbsolutePath::parse("//docs///A.txt/").expect("path should parse");

        assert_eq!(path.as_str(), "/docs/A.txt");
        assert_eq!(
            path.components()
                .iter()
                .map(|component| component.as_str())
                .collect::<Vec<_>>(),
            vec!["docs", "A.txt"]
        );
    }

    #[test]
    fn absolute_path_parent_final_component_and_join_preserve_display_spelling() {
        let path = AbsolutePath::parse("/Docs/ReadMe.TXT").expect("path should parse");
        let parent = path.parent().expect("non-root path should have parent");

        assert_eq!(parent.as_str(), "/Docs");
        assert_eq!(path.final_component().unwrap().as_str(), "ReadMe.TXT");
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
    fn name_key_matches_name_policy_helper() {
        let display_name = DisplayName::parse("Cafe\u{301}.TXT").expect("display name");
        let key = NameKey::for_display_name(NamePolicy::NfcCasefoldV0, &display_name);

        assert_eq!(
            key.as_str(),
            name_key_for_display_name(NamePolicy::NfcCasefoldV0, display_name.as_str())
        );
    }
}
