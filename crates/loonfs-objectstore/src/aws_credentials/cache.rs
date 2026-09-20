//! One expiring credential value per shared AWS source, refreshed on demand.

use aws_credential_types::provider::{error::CredentialsError, ProvideCredentials};
use aws_credential_types::Credentials;
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
}

struct Entry {
    credentials: Credentials,
    expires_at: SystemTime,
    refresh_at: SystemTime,
}

impl Entry {
    fn cached(&self, now: SystemTime) -> Option<Credentials> {
        (now < self.refresh_at && now < self.expires_at).then(|| self.credentials.clone())
    }

    fn defer_refresh(&mut self, now: SystemTime) {
        self.refresh_at = now
            .checked_add(REFRESH_RETRY_DELAY)
            .unwrap_or(self.expires_at)
            .min(self.expires_at);
    }
}

impl CredentialCache {
    pub(super) async fn get(
        &self,
        provider: &(impl ProvideCredentials + ?Sized),
        now: impl Fn() -> SystemTime + Send,
    ) -> Result<Credentials, CredentialsError> {
        if let Some(credentials) = self
            .entry
            .read()
            .await
            .as_ref()
            .and_then(|entry| entry.cached(now()))
        {
            return Ok(credentials);
        }

        // Hold only this source's write lock while refreshing. A cancelled load
        // drops the guard without removing the previous value. Waiting callers
        // recheck the cache and the current time after acquiring the lock.
        let mut entry = self.entry.write().await;
        if let Some(credentials) = entry.as_ref().and_then(|entry| entry.cached(now())) {
            return Ok(credentials);
        }
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

        match loaded {
            Ok(credentials) => {
                // Preserve the existing lookup/rotation behavior of ambient
                // sources which supply no expiry, including environment keys.
                *entry = credentials.expiry().map(|expires_at| {
                    let mut next = Entry {
                        credentials: credentials.clone(),
                        expires_at,
                        refresh_at: expires_at.checked_sub(REFRESH_WINDOW).unwrap_or(now),
                    };
                    if next.refresh_at <= now {
                        next.defer_refresh(now);
                    }
                    next
                });
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
