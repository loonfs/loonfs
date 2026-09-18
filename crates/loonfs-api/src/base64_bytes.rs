//! Base64 encoding for optional bytes in JSON requests.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub(crate) fn serialize<S: Serializer>(
    bytes: &Option<Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    bytes
        .as_ref()
        .map(|bytes| STANDARD.encode(bytes))
        .serialize(serializer)
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|encoded| STANDARD.decode(encoded).map_err(serde::de::Error::custom))
        .transpose()
}
