//! Base64 encoding for bytes in JSON requests.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub(crate) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    encode(bytes).serialize(serializer)
}

pub(crate) fn encode(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    STANDARD
        .decode(String::deserialize(deserializer)?)
        .map_err(serde::de::Error::custom)
}

pub(crate) mod option {
    use super::STANDARD;
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer};

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|encoded| STANDARD.decode(encoded).map_err(serde::de::Error::custom))
            .transpose()
    }
}
