//! Types for identifying who made a commit.
//!
//! Use a stable ID such as `usr_8f3c`, rather than an email address or display
//! name. Profile changes should not change the actor recorded in file history.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

const MAX_ACTOR_ID_BYTES: usize = 256;

/// Identifies the user, service, or system responsible for a commit.
///
/// LoonFS stores this value as provided. It does not authenticate the actor or
/// look up profile information.
// This type also appears in request bodies, so it rejects unknown fields in
// every context. Add new actor kinds instead of new fields. This is not
// rustdoc because it describes storage behavior, not the public API.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ActorRef {
    /// The type of actor.
    pub kind: ActorKind,
    /// A stable identifier supplied by the application.
    pub id: ActorId,
}

impl ActorRef {
    /// Creates a user actor.
    pub fn user(id: ActorId) -> Self {
        Self {
            kind: ActorKind::User,
            id,
        }
    }

    /// Creates a service actor.
    pub fn service(id: ActorId) -> Self {
        Self {
            kind: ActorKind::Service,
            id,
        }
    }

    /// Creates a system actor.
    pub fn system(id: ActorId) -> Self {
        Self {
            kind: ActorKind::System,
            id,
        }
    }

    /// Returns the actor used when LoonFS creates a namespace root.
    pub fn loonfs_system() -> Self {
        Self::system(ActorId::parse("loonfs").expect("`loonfs` should be a valid actor id"))
    }
}

/// The type of actor responsible for a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    /// A user of the application.
    User,
    /// An application, integration, or background worker.
    ///
    /// Use [`ActorKind::User`] when a service acts on behalf of a known user.
    Service,
    /// System activity that changes filesystem data.
    ///
    /// Maintenance that does not create a commit has no actor.
    System,
}

impl ActorKind {
    /// Returns the value used in serialized actor references.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Service => "service",
            Self::System => "system",
        }
    }
}

/// A validated actor identifier supplied by the application.
///
/// Actor IDs may use the syntax of the application's identity system. They
/// must contain between 1 and 256 UTF-8 bytes, must not begin or end with
/// whitespace, and must not contain control characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActorId(String);

impl ActorId {
    /// Parses and validates an actor ID.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ActorIdValidationError> {
        let value = value.as_ref();
        validate_actor_id(value)?;
        Ok(Self(value.to_owned()))
    }

    /// Returns the actor ID as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ActorId {
    type Error = ActorIdValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for ActorId {
    type Error = ActorIdValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl std::str::FromStr for ActorId {
    type Err = ActorIdValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl AsRef<str> for ActorId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for ActorId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for ActorId {
    #[allow(
        deprecated,
        reason = "the published schema uses the requested singular example field"
    )]
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::String)
            .description(Some(
                "Opaque hosting-platform actor id: non-empty, at most 256 UTF-8 bytes, without leading or trailing whitespace or control characters.",
            ))
            .example(Some(serde_json::json!("usr_8f3c")))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for ActorId {}

impl Serialize for ActorId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ActorId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

/// An error returned when an actor ID is invalid.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid actor_id {value:?}: {reason}")]
pub struct ActorIdValidationError {
    value: String,
    reason: String,
}

impl ActorIdValidationError {
    /// Returns the rejected input.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the reason the input was rejected.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

fn validate_actor_id(value: &str) -> Result<(), ActorIdValidationError> {
    if value.is_empty() {
        return Err(actor_id_error(value, "must not be empty"));
    }
    if value.len() > MAX_ACTOR_ID_BYTES {
        return Err(actor_id_error(
            value,
            &format!("must be {MAX_ACTOR_ID_BYTES} bytes or fewer"),
        ));
    }
    if value.trim() != value {
        return Err(actor_id_error(
            value,
            "must not have leading or trailing whitespace",
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(actor_id_error(value, "must not contain control characters"));
    }
    Ok(())
}

fn actor_id_error(value: &str, reason: &str) -> ActorIdValidationError {
    ActorIdValidationError {
        value: value.to_owned(),
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{ActorId, ActorKind, ActorRef};

    #[test]
    fn actor_kind_serializes_as_snake_case_strings() {
        for (kind, json) in [
            (ActorKind::User, r#""user""#),
            (ActorKind::Service, r#""service""#),
            (ActorKind::System, r#""system""#),
        ] {
            assert_eq!(serde_json::to_string(&kind).expect("serialize kind"), json);
            assert_eq!(
                serde_json::from_str::<ActorKind>(json).expect("deserialize kind"),
                kind
            );
        }
    }

    #[test]
    fn actor_ref_has_the_exact_wire_shape() {
        let json = r#"{"kind":"user","id":"usr_8f3c"}"#;
        let actor = ActorRef::user(ActorId::parse("usr_8f3c").expect("valid actor id"));

        assert_eq!(
            serde_json::to_string(&actor).expect("serialize actor"),
            json
        );
        assert_eq!(
            serde_json::from_str::<ActorRef>(json).expect("deserialize actor"),
            actor
        );
    }

    #[test]
    fn actor_id_rejects_invalid_values_with_stable_reasons() {
        let too_long = "x".repeat(257);
        for (value, reason) in [
            ("", "must not be empty"),
            (&too_long, "must be 256 bytes or fewer"),
            (" actor", "must not have leading or trailing whitespace"),
            ("actor ", "must not have leading or trailing whitespace"),
            ("actor\nid", "must not contain control characters"),
            ("actor\0id", "must not contain control characters"),
            ("actor\u{7f}id", "must not contain control characters"),
        ] {
            let error = ActorId::parse(value).expect_err("invalid actor id");
            assert_eq!(error.value(), value);
            assert_eq!(error.reason(), reason);
        }
    }

    #[test]
    fn actor_id_error_escapes_hostile_input() {
        let error = ActorId::parse("actor\nid").expect_err("control character");

        assert_eq!(
            error.to_string(),
            r#"invalid actor_id "actor\nid": must not contain control characters"#
        );
    }

    #[test]
    fn actor_id_accepts_external_syntax_and_round_trips() {
        let exactly_256_bytes = "x".repeat(256);
        for value in [
            "auth0|64abc",
            "AAD:uPn@Example",
            "123e4567-e89b-12d3-a456-426614174000",
            &exactly_256_bytes,
        ] {
            let parsed = ActorId::parse(value).expect("valid external actor id");
            assert_eq!(parsed.as_str(), value);
            assert_eq!(parsed.to_string(), value);
            assert_eq!(ActorId::try_from(value).expect("try_from actor id"), parsed);
            assert_eq!(value.parse::<ActorId>().expect("from_str actor id"), parsed);

            let json = serde_json::to_string(&parsed).expect("serialize actor id");
            assert_eq!(
                serde_json::from_str::<ActorId>(&json).expect("deserialize actor id"),
                parsed
            );
        }
    }

    #[test]
    fn actor_id_utf8_limit_counts_bytes_not_characters() {
        let exactly_256_bytes = "é".repeat(128);
        let too_long = format!("{exactly_256_bytes}a");

        ActorId::parse(&exactly_256_bytes).expect("256-byte unicode actor id");
        assert_eq!(
            ActorId::parse(&too_long)
                .expect_err("257-byte unicode actor id")
                .reason(),
            "must be 256 bytes or fewer"
        );
    }

    #[test]
    fn actor_ref_rejects_unknown_kind_and_fields() {
        assert!(serde_json::from_str::<ActorRef>(r#"{"kind":"robot","id":"x"}"#).is_err());
        assert!(
            serde_json::from_str::<ActorRef>(r#"{"kind":"user","id":"x","name":"Ada"}"#).is_err()
        );
    }

    #[test]
    fn actor_ref_convenience_constructors_select_the_kind() {
        let id = ActorId::parse("actor").expect("valid actor id");

        assert_eq!(ActorRef::user(id.clone()).kind, ActorKind::User);
        assert_eq!(ActorRef::service(id.clone()).kind, ActorKind::Service);
        assert_eq!(ActorRef::system(id).kind, ActorKind::System);
        assert_eq!(
            ActorRef::loonfs_system(),
            ActorRef::system(ActorId::parse("loonfs").expect("valid bootstrap actor id"))
        );
    }

    #[cfg(feature = "openapi")]
    #[test]
    fn actor_vocabulary_registers_openapi_schemas() {
        #[derive(utoipa::OpenApi)]
        #[openapi(components(schemas(ActorRef, ActorKind, ActorId)))]
        struct ActorVocabularyOpenApi;

        let document = serde_json::to_value(<ActorVocabularyOpenApi as utoipa::OpenApi>::openapi())
            .expect("serialize actor vocabulary OpenAPI document");
        let schemas = document
            .pointer("/components/schemas")
            .and_then(serde_json::Value::as_object)
            .expect("actor vocabulary schemas");

        for name in ["ActorRef", "ActorKind", "ActorId"] {
            assert!(schemas.contains_key(name), "missing `{name}` schema");
        }
    }
}
