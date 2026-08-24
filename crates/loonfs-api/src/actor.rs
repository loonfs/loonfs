//! Types for identifying who made a commit.
//!
//! Use a stable ID such as `usr_8f3c`, rather than an email address or display
//! name. Profile changes should not change the actor recorded in file history.

use crate::ids::{string_id, validation_error};
use serde::{Deserialize, Serialize};
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

validation_error!(
    ActorIdValidationError,
    "invalid actor_id {value:?}: {reason}"
);

string_id! {
    /// A validated actor identifier supplied by the application.
    ///
    /// Actor IDs may use the syntax of the application's identity system. They
    /// must contain between 1 and 256 UTF-8 bytes, must not begin or end with
    /// whitespace, and must not contain control characters.
    ActorId,
    error = ActorIdValidationError,
    validate = validate_actor_id,
    schema(
        description = "Opaque hosting-platform actor id: non-empty, at most 256 UTF-8 bytes, without leading or trailing whitespace or control characters.",
        example = "usr_8f3c"
    )
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
}
