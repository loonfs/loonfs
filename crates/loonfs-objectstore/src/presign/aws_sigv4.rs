//! AWS Signature Version 4 primitives used to presign S3-compatible URLs.

use crate::object_store::Result;
use crate::ObjectStoreError;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AwsSigV4Dates {
    pub(crate) short_date: String,
    pub(crate) amz_date: String,
}

pub(crate) fn aws_dates(object_key: &str, time: SystemTime) -> Result<AwsSigV4Dates> {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            ObjectStoreError::transport(
                object_key,
                format!("system time is before unix epoch: {err}"),
            )
        })?
        .as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    let short_date = format!("{year:04}{month:02}{day:02}");

    Ok(AwsSigV4Dates {
        amz_date: format!("{short_date}T{hour:02}{minute:02}{second:02}Z"),
        short_date,
    })
}

pub(crate) fn canonical_query_string(query: &BTreeMap<String, String>) -> String {
    query
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                percent_encode_query(key),
                percent_encode_query(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

pub(crate) fn percent_encode_path(value: &str) -> String {
    value
        .split('/')
        .map(percent_encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn percent_encode_segment(value: &str) -> String {
    percent_encode_bytes(value.as_bytes())
}

pub(crate) fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    loonfs_api::wire::hex::hex_encode_bytes(bytes)
}

fn percent_encode_query(value: &str) -> String {
    percent_encode_bytes(value.as_bytes())
}

fn percent_encode_bytes(bytes: &[u8]) -> String {
    let mut out = String::new();
    for byte in bytes {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

// Howard Hinnant's civil date conversion, with days counted from 1970-01-01.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    (y + if m <= 2 { 1 } else { 0 }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 4231 test vectors pin the HMAC-SHA256 construction across
    /// implementation changes.
    #[test]
    fn hmac_sha256_matches_rfc_4231_vectors() {
        let case_one = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(
            hex_lower(&case_one),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        let case_two = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            hex_lower(&case_two),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}
