//! One expiring credential value per shared AWS source, refreshed on demand.

use aws_credential_types::provider::{error::CredentialsError, ProvideCredentials};
use aws_credential_types::Credentials;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

// Refresh before expiry, without a background task or another provider timeout.
const REFRESH_WINDOW: Duration = Duration::from_secs(60);
// Providers can return the same nearly expired credentials or fail to refresh.
// Reuse a still-valid value briefly instead of fetching again on every request.
const REFRESH_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Default)]
pub(super) struct CredentialCache {
    entry: RwLock<Option<Entry>>,
    // Once observed, preserve uncached lookup behavior for this source's lifetime.
    uncached: AtomicBool,
}

struct Entry {
    credentials: Credentials,
    expires_at: SystemTime,
    refresh_at: SystemTime,
    retry_after: SystemTime,
}

impl Entry {
    fn cached(&self, now: SystemTime, valid_until: Option<SystemTime>) -> Option<Credentials> {
        let sufficient_lifetime = valid_until.is_none_or(|until| until <= self.expires_at);
        (now < self.expires_at
            && ((now < self.refresh_at && sufficient_lifetime) || now < self.retry_after))
            .then(|| self.credentials.clone())
    }

    fn defer_refresh(&mut self, now: SystemTime) {
        self.retry_after = now
            .checked_add(REFRESH_RETRY_DELAY)
            .unwrap_or(self.expires_at)
            .min(self.expires_at);
    }
}

impl CredentialCache {
    // A requested signing lifetime can trigger an earlier refresh. The caller
    // must still check the returned expiry: the provider may return the same
    // short-lived value, and the retry backoff also applies to presigners.
    pub(super) async fn get(
        &self,
        provider: &(impl ProvideCredentials + ?Sized),
        now: impl Fn() -> SystemTime + Send,
        valid_until: Option<SystemTime>,
    ) -> Result<Credentials, CredentialsError> {
        let entry = if self.uncached.load(Ordering::Acquire) {
            None
        } else {
            if let Some(credentials) = self
                .entry
                .read()
                .await
                .as_ref()
                .and_then(|entry| entry.cached(now(), valid_until))
            {
                return Ok(credentials);
            }

            // A cancelled refresh preserves the previous value. Recheck both
            // cache and mode after waiting: discovery may have disabled caching.
            let entry = self.entry.write().await;
            if self.uncached.load(Ordering::Acquire) {
                None
            } else {
                if let Some(credentials) = entry
                    .as_ref()
                    .and_then(|entry| entry.cached(now(), valid_until))
                {
                    return Ok(credentials);
                }
                Some(entry)
            }
        };
        let loaded = provider.provide_credentials().await;
        let now = now();
        let loaded = loaded.and_then(|credentials| {
            if credentials.expiry().is_some_and(|expiry| expiry <= now) {
                Err(CredentialsError::provider_error(
                    "resolved AWS credentials have expired",
                ))
            } else {
                Ok(credentials)
            }
        });

        let Some(mut entry) = entry else {
            return loaded;
        };
        match loaded {
            Ok(credentials) => {
                // Preserve the existing lookup/rotation behavior of ambient
                // sources which supply no expiry, including environment keys.
                *entry = credentials.expiry().map(|expires_at| {
                    let mut next = Entry {
                        credentials: credentials.clone(),
                        expires_at,
                        refresh_at: expires_at.checked_sub(REFRESH_WINDOW).unwrap_or(now),
                        retry_after: now,
                    };
                    next.defer_refresh(now);
                    next
                });
                if entry.is_none() {
                    self.uncached.store(true, Ordering::Release);
                }
                Ok(credentials)
            }
            Err(error) => {
                if let Some(previous) = entry.as_mut().filter(|entry| now < entry.expires_at) {
                    previous.defer_refresh(now);
                    return Ok(previous.credentials.clone());
                }
                // Never extend expiry or cache errors when no valid value exists.
                *entry = None;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests;
