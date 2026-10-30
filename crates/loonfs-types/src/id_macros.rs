//! The macros that generate ID newtypes and their validation errors.

macro_rules! validation_error {
    ($name:ident, $message:literal) => {
        /// Describes why supplied text does not satisfy this identifier's validation contract.
        #[derive(Debug, Clone, PartialEq, Eq, Error)]
        #[error($message)]
        pub struct $name {
            value: String,
            reason: String,
        }

        impl $name {
            pub(crate) fn new(value: &str, reason: impl Into<String>) -> Self {
                Self {
                    value: value.to_owned(),
                    reason: reason.into(),
                }
            }

            /// Returns the rejected input, or an empty string when echoing it would be unsafe.
            pub fn value(&self) -> &str {
                &self.value
            }

            /// Returns the specific grammar rule the rejected input violated.
            pub fn reason(&self) -> &str {
                &self.reason
            }
        }
    };
}

/// Defines a validated string-id newtype.
///
/// Every string id gets the same surface: `parse` (the only fallible
/// constructor), `as_str`, `TryFrom<&str>`/`TryFrom<String>`/`FromStr`
/// (all delegating to `parse`), `AsRef<str>`, `Borrow<str>`, `Display`
/// (the plain inner string), and serde as a plain string with validation
/// on deserialize.
///
/// Two forms:
/// - `string_id!(Name, error = ErrType, validate = validator)` uses a custom
///   `fn(&str) -> Result<(), ErrType>` validator.
/// - `string_id!(Name, prefix = "xyz")` validates the project-standard
///   server-generated shape, `xyz_` and 26 lowercase Crockford base 32
///   characters, and adds a `generate()` constructor.
///
/// Either form may end with `schema(...)` metadata. Its optional `pattern`
/// and `example` are added to the OpenAPI string schema when that feature is
/// enabled.
///
/// Type-specific constructors that the macro cannot express (for example
/// `CommitId::generate` or `NameKey::for_display_name`) are in a separate
/// `impl` block next to the invocation.
macro_rules! string_id {
    (
        $(#[$meta:meta])*
        $name:ident,
        error = $error:ty,
        validate = $validate:expr
        $(, schema($($schema:tt)+))?
        $(,)?
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
        #[cfg_attr(
            feature = "openapi",
            schema(value_type = String $(, $($schema)+)?)
        )]
        pub struct $name(String);

        impl $name {
            /// Parses and validates the id from its serialized form.
            pub fn parse(value: impl AsRef<str>) -> Result<Self, $error> {
                let value = value.as_ref();
                ($validate)(value)?;
                Ok(Self(value.to_owned()))
            }

            /// Returns the serialized id.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl std::str::FromStr for $name {
            type Err = $error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl std::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                // Validates the decoded string and keeps it, so a decode is
                // one allocation.
                let validated: Result<(), $error> = ($validate)(value.as_str());
                validated.map_err(serde::de::Error::custom)?;
                Ok(Self(value))
            }
        }
    };
    (
        $(#[$meta:meta])*
        $name:ident,
        prefix = $prefix:literal
        $(, schema($($schema:tt)+))?
        $(,)?
    ) => {
        string_id! {
            $(#[$meta])*
            $name,
            error = GeneratedIdValidationError,
            validate = |value: &str| validate_generated_id($prefix, value)
            $(, schema($($schema)+))?
        }

        impl $name {
            /// Generates a valid random id.
            pub fn generate() -> Self {
                Self(generated_id($prefix))
            }

            /// Returns the ID's 26-character body, the spelling after its
            /// prefix and underscore, which row keys use.
            pub fn body(&self) -> &str {
                self.0
                    .strip_prefix(concat!($prefix, "_"))
                    .expect("a generated id should start with its prefix")
            }
        }
    };
}

/// Defines a numeric (`u64`) id newtype.
///
/// Every numeric id gets `Copy`, ordering and hashing derives, `From<u64>`,
/// `Display` as the plain inner number, and serde as a plain number. The
/// inner field stays public: numeric ids are constructed positionally
/// (`ChangeSeq(7)`) and read via `.0`.
macro_rules! numeric_id {
    (
        $(#[$meta:meta])*
        $name:ident,
        public_ordinal,
        schema_description = $schema_description:literal
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
        pub struct $name(pub u64);

        impl $name {
            /// Validates a numeric value before using it as an ordinal.
            ///
            /// Deserialization calls this method automatically. Code that
            /// receives a raw integer through another interface must call it
            /// explicitly. Direct tuple construction and `From<u64>` are for
            /// values that have already been validated.
            pub fn parse(value: u64) -> Result<Self, $crate::PublicOrdinalRangeError> {
                if value > $crate::MAX_PUBLIC_INTEGER {
                    return Err($crate::PublicOrdinalRangeError);
                }
                Ok(Self(value))
            }

            /// Returns the next ordinal, or an error at the public maximum.
            pub fn successor(self) -> Result<Self, $crate::PublicOrdinalRangeError> {
                $crate::next_public_ordinal(self.0)
                    .map(Self)
                    .ok_or($crate::PublicOrdinalRangeError)
            }
        }

        #[cfg(feature = "openapi")]
        impl utoipa::PartialSchema for $name {
            fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Object::builder()
                    .schema_type(utoipa::openapi::schema::Type::Integer)
                    .format(Some(utoipa::openapi::SchemaFormat::KnownFormat(
                        utoipa::openapi::KnownFormat::Int64,
                    )))
                    .minimum(Some(0u64))
                    .maximum(Some($crate::MAX_PUBLIC_INTEGER))
                    .description(Some($schema_description))
                    .into()
            }
        }

        #[cfg(feature = "openapi")]
        impl utoipa::ToSchema for $name {}

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <u64 as serde::Deserialize>::deserialize(deserializer)?;
                Self::parse(value).map_err(serde::de::Error::custom)
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
    (
        $(#[$meta:meta])*
        $name:ident
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        pub struct $name(pub u64);

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

pub(crate) use numeric_id;
pub(crate) use string_id;
pub(crate) use validation_error;
