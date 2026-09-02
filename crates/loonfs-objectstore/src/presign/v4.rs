//! Version-4 signing primitives, shared by the two schemes this crate signs.
//!
//! AWS `AWS4-HMAC-SHA256` and Google `GOOG4-RSA-SHA256` are the same
//! construction: the same canonical-request shape, the same timestamp
//! spellings, the same percent-encoding, the same `<date>/<location>/<service>/<terminator>`
//! credential scope. They differ in the literals each writes into those slots
//! and in the primitive that produces the signature. Only the parts that are
//! genuinely identical live here; each scheme's own literals and signature
//! primitive stay in its own presigner.

use crate::object_store::Result;
use crate::presign::{PresignedUrl, MAX_PRESIGN_EXPIRY};
use crate::ObjectStoreError;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

/// The two date spellings a V4 signature carries: the credential scope's day
/// and the request's full timestamp, which must agree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SigningDates {
    pub(crate) short_date: String,
    pub(crate) timestamp: String,
}

pub(crate) struct V4Scheme {
    pub(crate) algorithm: &'static str,
    pub(crate) query_prefix: &'static str,
    pub(crate) credential: String,
    pub(crate) credential_scope: String,
    pub(crate) extra_query: BTreeMap<String, String>,
    pub(crate) signing_dates: SigningDates,
}

pub(crate) struct V4Endpoint {
    pub(crate) scheme: String,
    pub(crate) host: String,
    pub(crate) canonical_uri: String,
}

pub(crate) struct V4RequestParts<'a> {
    pub(crate) object_key: &'a str,
    pub(crate) operation_query: BTreeMap<String, String>,
    pub(crate) required_headers: BTreeMap<String, String>,
}

pub(crate) fn presign_v4(
    scheme: V4Scheme,
    endpoint: V4Endpoint,
    method: &str,
    request_parts: V4RequestParts<'_>,
    expires_in: Duration,
    now: SystemTime,
    sign: impl FnOnce(&[u8]) -> Result<String>,
) -> Result<PresignedUrl> {
    if expires_in.is_zero() {
        return Err(ObjectStoreError::Configuration(
            "presigned URL expiry must be greater than zero".to_owned(),
        ));
    }
    if expires_in.as_secs() > MAX_PRESIGN_EXPIRY {
        return Err(ObjectStoreError::Configuration(format!(
            "presigned URL expiry must not exceed {MAX_PRESIGN_EXPIRY} seconds"
        )));
    }

    let mut headers_to_sign = BTreeMap::from([("host".to_owned(), endpoint.host.clone())]);
    for (name, value) in &request_parts.required_headers {
        headers_to_sign.insert(name.to_ascii_lowercase(), normalize_header_value(value));
    }
    let signed_headers = headers_to_sign
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .join(";");
    let mut canonical_headers = String::new();
    for (name, value) in &headers_to_sign {
        writeln!(&mut canonical_headers, "{name}:{value}")
            .expect("writing to a String should not fail");
    }

    let mut query = request_parts.operation_query;
    query.extend(scheme.extra_query);
    query.extend([
        (
            format!("{}-Algorithm", scheme.query_prefix),
            scheme.algorithm.to_owned(),
        ),
        (
            format!("{}-Credential", scheme.query_prefix),
            scheme.credential,
        ),
        (
            format!("{}-Date", scheme.query_prefix),
            scheme.signing_dates.timestamp.clone(),
        ),
        (
            format!("{}-Expires", scheme.query_prefix),
            expires_in.as_secs().to_string(),
        ),
        (
            format!("{}-SignedHeaders", scheme.query_prefix),
            signed_headers.clone(),
        ),
    ]);
    let canonical_query = canonical_query_string(&query);
    let canonical_request = format!(
        "{method}\n{}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD",
        endpoint.canonical_uri
    );
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}",
        scheme.algorithm,
        scheme.signing_dates.timestamp,
        scheme.credential_scope,
        hex_lower(&Sha256::digest(canonical_request.as_bytes()))
    );
    let signature = sign(string_to_sign.as_bytes())?;
    let url = format!(
        "{}://{}{}?{}&{}-Signature={}",
        endpoint.scheme,
        endpoint.host,
        endpoint.canonical_uri,
        canonical_query,
        scheme.query_prefix,
        signature
    );
    let expires_at_ms = unix_ms(request_parts.object_key, now)? + expires_in.as_millis() as u64;

    Ok(PresignedUrl {
        method: method.to_owned(),
        url,
        headers: request_parts.required_headers,
        expires_at_ms,
    })
}

pub(crate) fn signing_dates(object_key: &str, time: SystemTime) -> Result<SigningDates> {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map_err(|err| before_unix_epoch(object_key, err))?
        .as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    let short_date = format!("{year:04}{month:02}{day:02}");

    Ok(SigningDates {
        timestamp: format!("{short_date}T{hour:02}{minute:02}{second:02}Z"),
        short_date,
    })
}

/// Unix milliseconds for a signing instant, used to report when an issued
/// capability stops working.
pub(crate) fn unix_ms(object_key: &str, time: SystemTime) -> Result<u64> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|err| before_unix_epoch(object_key, err))?;
    Ok(duration.as_millis() as u64)
}

fn before_unix_epoch(object_key: &str, err: SystemTimeError) -> ObjectStoreError {
    ObjectStoreError::transport(
        object_key,
        format!("system time is before unix epoch: {err}"),
    )
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
