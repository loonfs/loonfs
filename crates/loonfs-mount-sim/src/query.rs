//! Query parameters: decoding, rejecting unknown parameters, and the read-point parameters that
//! every read route shares.

use crate::error::{MountSimError, Result};
use loonfs_types::{AbsolutePath, PinId, DEFAULT_MAX_PAGE_LIMIT, DEFAULT_PAGE_LIMIT};

/// The largest `max_staleness_ms` a read accepts (api.md, "Read consistency").
pub(crate) const READ_REVALIDATION_BOUND_MS: u64 = 1_155_000;

/// The decoded query parameters of one request, in request order.
#[derive(Debug, Clone, Default)]
pub(crate) struct Query {
    pairs: Vec<(String, String)>,
}

/// What a read reads: the live head, or the state that a snapshot captured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadPoint {
    Live,
    Snapshot(PinId),
}

impl Query {
    pub(crate) fn parse(raw: Option<&str>) -> Self {
        Self {
            pairs: raw
                .map(|raw| {
                    form_urlencoded::parse(raw.as_bytes())
                        .into_owned()
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    pub(crate) fn pairs(&self) -> &[(String, String)] {
        &self.pairs
    }

    /// Rejects a parameter that the route doesn't declare, and a parameter given twice.
    pub(crate) fn ensure_only(&self, allowed: &[&str]) -> Result<()> {
        for (index, (name, _)) in self.pairs.iter().enumerate() {
            if !allowed.contains(&name.as_str()) {
                return Err(MountSimError::invalid_request(format!(
                    "unknown query parameter `{name}`"
                ))
                .with_param(name.as_str()));
            }
            if self.pairs[..index]
                .iter()
                .any(|(earlier, _)| earlier == name)
            {
                return Err(MountSimError::invalid_request(format!(
                    "query parameter `{name}` is given more than once"
                ))
                .with_param(name.as_str()));
            }
        }
        Ok(())
    }

    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    pub(crate) fn path(&self, name: &str) -> Result<AbsolutePath> {
        let value = self.get(name).ok_or_else(|| missing(name))?;
        AbsolutePath::parse(value).map_err(|error| {
            MountSimError::invalid_request(format!("invalid `{name}`: {error}")).with_param(name)
        })
    }

    pub(crate) fn bool(&self, name: &str, default: bool) -> Result<bool> {
        match self.get(name) {
            None => Ok(default),
            Some("true") => Ok(true),
            Some("false") => Ok(false),
            Some(value) => Err(MountSimError::invalid_request(format!(
                "`{name}` must be `true` or `false`, not {value:?}"
            ))
            .with_param(name)),
        }
    }

    pub(crate) fn u64(&self, name: &str) -> Result<Option<u64>> {
        self.get(name)
            .map(|value| {
                value.parse().map_err(|_| {
                    MountSimError::invalid_request(format!(
                        "`{name}` must be a non-negative integer, not {value:?}"
                    ))
                    .with_param(name)
                })
            })
            .transpose()
    }

    pub(crate) fn required_u64(&self, name: &str) -> Result<u64> {
        self.u64(name)?.ok_or_else(|| missing(name))
    }

    /// Returns the page size: `limit`, or the default page size when it's absent.
    pub(crate) fn limit(&self) -> Result<usize> {
        let limit = self.u64("limit")?.unwrap_or(u64::from(DEFAULT_PAGE_LIMIT));
        if limit == 0 || limit > u64::from(DEFAULT_MAX_PAGE_LIMIT) {
            return Err(MountSimError::invalid_request(format!(
                "`limit` must be from 1 to {DEFAULT_MAX_PAGE_LIMIT}"
            ))
            .with_param("limit"));
        }
        Ok(usize::try_from(limit).expect("a page limit fits in usize"))
    }

    /// Reads `snapshot_id`, `consistency`, and `max_staleness_ms`, and checks that they're
    /// consistent. The simulation reads every live read at the newest head, so it validates the
    /// consistency parameters and then ignores them.
    pub(crate) fn read_point(&self) -> Result<ReadPoint> {
        let consistency = self.get("consistency");
        match consistency {
            None | Some("strong" | "eventual") => {}
            Some(value) => {
                return Err(MountSimError::invalid_request(format!(
                    "`consistency` must be `strong` or `eventual`, not {value:?}"
                ))
                .with_param("consistency"))
            }
        }
        if let Some(staleness) = self.u64("max_staleness_ms")? {
            if consistency != Some("eventual") {
                return Err(MountSimError::invalid_request(
                    "`max_staleness_ms` requires `consistency=eventual`",
                )
                .with_param("max_staleness_ms"));
            }
            if staleness > READ_REVALIDATION_BOUND_MS {
                return Err(MountSimError::invalid_request(format!(
                    "`max_staleness_ms` can't exceed {READ_REVALIDATION_BOUND_MS}"
                ))
                .with_param("max_staleness_ms"));
            }
        }
        let Some(snapshot_id) = self.get("snapshot_id") else {
            return Ok(ReadPoint::Live);
        };
        let snapshot_id = PinId::parse(snapshot_id).map_err(|error| {
            MountSimError::invalid_request(error.to_string()).with_param("snapshot_id")
        })?;
        if consistency.is_some() {
            return Err(MountSimError::invalid_request(
                "`consistency` can't be combined with `snapshot_id`",
            )
            .with_param("consistency"));
        }
        Ok(ReadPoint::Snapshot(snapshot_id))
    }
}

fn missing(name: &str) -> MountSimError {
    MountSimError::invalid_request(format!("missing required query parameter `{name}`"))
        .with_param(name)
}
