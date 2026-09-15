//! Types for identifying who made a commit.
//!
//! Use a stable ID such as `usr_8f3c`, rather than an email address or display
//! name. Profile changes should not change the actor recorded in file history.

use crate::ids::{string_id, validation_error};
use thiserror::Error;

const MAX_ACTOR_ID_BYTES: usize = 256;

validation_error!(
    ActorIdValidationError,
    "invalid actor_id {value:?}: {reason}"
);

string_id! {
    /// A validated actor identifier supplied by the application.
    ///
    /// Actor IDs contain 1 to 256 visible ASCII characters (0x21 through 0x7E).
    ActorId,
    error = ActorIdValidationError,
    validate = validate_actor_id,
    schema(
        description = "Stable opaque actor id containing 1 to 256 visible ASCII characters.",
        pattern = r"^[\x21-\x7E]{1,256}$",
        example = "usr_8f3c"
    )
}

impl ActorId {
    /// Returns the id recorded when LoonFS creates a namespace root.
    pub fn loonfs() -> Self {
        Self::parse("loonfs").expect("`loonfs` should be a valid actor id")
    }
}

fn validate_actor_id(value: &str) -> Result<(), ActorIdValidationError> {
    if value.is_empty() {
        return Err(ActorIdValidationError::new(value, "must not be empty"));
    }
    if value.len() > MAX_ACTOR_ID_BYTES {
        return Err(ActorIdValidationError::new(
            value,
            format!("must be {MAX_ACTOR_ID_BYTES} bytes or fewer"),
        ));
    }
    if !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return Err(ActorIdValidationError::new(
            value,
            "must contain only visible ASCII characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ActorId;

    #[test]
    fn actor_id_rejects_invalid_values_with_stable_reasons() {
        let too_long = "x".repeat(257);
        for (value, reason) in [
            ("", "must not be empty"),
            (&too_long, "must be 256 bytes or fewer"),
            (" actor", "must contain only visible ASCII characters"),
            ("actor ", "must contain only visible ASCII characters"),
            ("actor id", "must contain only visible ASCII characters"),
            ("actor-雪", "must contain only visible ASCII characters"),
            ("actor\nid", "must contain only visible ASCII characters"),
            ("actor\0id", "must contain only visible ASCII characters"),
            (
                "actor\u{7f}id",
                "must contain only visible ASCII characters",
            ),
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
            r#"invalid actor_id "actor\nid": must contain only visible ASCII characters"#
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
}
