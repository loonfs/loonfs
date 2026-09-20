use super::CredentialCache;
use aws_credential_types::provider::{error::CredentialsError, future, ProvideCredentials};
use aws_credential_types::Credentials;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_700_000_000 + seconds)
}

#[derive(Clone, Default)]
struct Clock(Arc<AtomicU64>);

impl Clock {
    fn now(&self) -> SystemTime {
        at(self.0.load(Ordering::SeqCst))
    }

    fn set(&self, seconds: u64) {
        self.0.store(seconds, Ordering::SeqCst);
    }
}

fn credentials(version: &str, expires_at: Option<u64>) -> Credentials {
    Credentials::new(
        format!("access-{version}"),
        format!("secret-{version}"),
        Some(format!("token-{version}")),
        expires_at.map(at),
        "synthetic",
    )
}

enum Step {
    Return(Credentials),
    Fail,
    Wait(Arc<Semaphore>, Credentials),
}

struct Provider {
    steps: Mutex<VecDeque<Step>>,
    calls: AtomicUsize,
    started: Semaphore,
}

impl fmt::Debug for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Provider").finish_non_exhaustive()
    }
}

impl Provider {
    fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            steps: Mutex::new(steps.into_iter().collect()),
            calls: AtomicUsize::new(0),
            started: Semaphore::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ProvideCredentials for Provider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let step = self
                .steps
                .lock()
                .expect("steps")
                .pop_front()
                .expect("unexpected lookup");
            self.started.add_permits(1);
            match step {
                Step::Return(credentials) => Ok(credentials),
                Step::Fail => Err(CredentialsError::provider_error(
                    "synthetic provider failure",
                )),
                Step::Wait(gate, credentials) => {
                    gate.acquire().await.expect("open gate").forget();
                    Ok(credentials)
                }
            }
        })
    }
}

async fn get(cache: &CredentialCache, provider: &Provider, clock: &Clock) -> Credentials {
    cache
        .get(provider, || clock.now())
        .await
        .expect("resolve credentials")
}

#[tokio::test]
async fn concurrent_cold_and_warm_reads_share_one_lookup_and_keep_native_expiry() {
    let cache = Arc::new(CredentialCache::default());
    let gate = Arc::new(Semaphore::new(0));
    let provider = Arc::new(Provider::new([Step::Wait(
        gate.clone(),
        credentials("one", Some(3600)),
    )]));
    let clock = Clock::default();
    let readers = (0..64)
        .map(|_| {
            let (cache, provider, clock) = (cache.clone(), provider.clone(), clock.clone());
            tokio::spawn(async move { get(&cache, &provider, &clock).await })
        })
        .collect::<Vec<_>>();
    provider
        .started
        .acquire()
        .await
        .expect("lookup started")
        .forget();
    gate.add_permits(1);
    for reader in readers {
        let value = reader.await.expect("reader");
        assert_eq!(value.access_key_id(), "access-one");
        assert_eq!(value.expiry(), Some(at(3600)));
    }
    for _ in 0..100 {
        assert_eq!(
            get(&cache, &provider, &clock).await.session_token(),
            Some("token-one")
        );
    }
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn approaching_expiry_refreshes_once_for_concurrent_callers_and_changes_keys() {
    let cache = Arc::new(CredentialCache::default());
    let gate = Arc::new(Semaphore::new(0));
    let provider = Arc::new(Provider::new([
        Step::Return(credentials("one", Some(120))),
        Step::Wait(gate.clone(), credentials("two", Some(3600))),
    ]));
    let clock = Clock::default();
    get(&cache, &provider, &clock).await;
    provider
        .started
        .acquire()
        .await
        .expect("first lookup")
        .forget();
    clock.set(59);
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-one"
    );
    clock.set(60);
    let readers = (0..32)
        .map(|_| {
            let (cache, provider, clock) = (cache.clone(), provider.clone(), clock.clone());
            tokio::spawn(async move { get(&cache, &provider, &clock).await })
        })
        .collect::<Vec<_>>();
    provider
        .started
        .acquire()
        .await
        .expect("refresh started")
        .forget();
    gate.add_permits(1);
    for reader in readers {
        let value = reader.await.expect("reader");
        assert_eq!(value.access_key_id(), "access-two");
        assert_eq!(value.secret_access_key(), "secret-two");
        assert_eq!(value.session_token(), Some("token-two"));
    }
    assert_eq!(provider.calls(), 2);
}

#[tokio::test]
async fn refresh_failure_reuses_only_unexpired_credentials_with_bounded_retry_backoff() {
    let cache = CredentialCache::default();
    let provider = Provider::new([
        Step::Return(credentials("one", Some(120))),
        Step::Fail,
        Step::Fail,
        Step::Fail,
        Step::Return(credentials("two", Some(3600))),
    ]);
    let clock = Clock::default();
    get(&cache, &provider, &clock).await;
    clock.set(60);
    for _ in 0..32 {
        assert_eq!(
            get(&cache, &provider, &clock).await.access_key_id(),
            "access-one"
        );
    }
    assert_eq!(provider.calls(), 2);
    clock.set(119);
    assert_eq!(get(&cache, &provider, &clock).await.expiry(), Some(at(120)));
    clock.set(120);
    assert!(cache.get(&provider, || clock.now()).await.is_err());
    assert_eq!(provider.calls(), 4);
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-two"
    );
}

#[tokio::test]
async fn unchanged_near_expiry_response_does_not_cause_a_lookup_per_request() {
    let cache = CredentialCache::default();
    let provider = Provider::new([
        Step::Return(credentials("one", Some(120))),
        Step::Return(credentials("one", Some(120))),
        Step::Return(credentials("two", Some(3600))),
    ]);
    let clock = Clock::default();
    get(&cache, &provider, &clock).await;
    clock.set(60);
    for _ in 0..32 {
        get(&cache, &provider, &clock).await;
    }
    assert_eq!(provider.calls(), 2);
    clock.set(61);
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-two"
    );
}

#[tokio::test]
async fn credentials_without_expiry_remain_uncached_including_after_an_expiring_value() {
    let cache = CredentialCache::default();
    let provider = Provider::new([
        Step::Return(credentials("one", Some(120))),
        Step::Return(credentials("two", None)),
        Step::Return(credentials("three", None)),
    ]);
    let clock = Clock::default();
    get(&cache, &provider, &clock).await;
    clock.set(60);
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-two"
    );
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-three"
    );
    assert_eq!(provider.calls(), 3);
}

#[tokio::test]
async fn expired_results_and_cold_errors_are_rejected_without_poisoning_the_cache() {
    let cache = CredentialCache::default();
    let provider = Provider::new([
        Step::Fail,
        Step::Return(credentials("expired", Some(0))),
        Step::Return(credentials("valid", Some(3600))),
    ]);
    let clock = Clock::default();
    assert!(cache.get(&provider, || clock.now()).await.is_err());
    assert!(cache.get(&provider, || clock.now()).await.is_err());
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-valid"
    );
    assert_eq!(provider.calls(), 3);
}

#[tokio::test]
async fn cancellation_releases_the_refresh_lock_and_preserves_the_old_value() {
    let cache = Arc::new(CredentialCache::default());
    let provider = Arc::new(Provider::new([
        Step::Return(credentials("old", Some(120))),
        Step::Wait(
            Arc::new(Semaphore::new(0)),
            credentials("cancelled", Some(3600)),
        ),
        Step::Fail,
        Step::Return(credentials("new", Some(3600))),
    ]));
    let clock = Clock::default();
    get(&cache, &provider, &clock).await;
    provider
        .started
        .acquire()
        .await
        .expect("initial load")
        .forget();
    clock.set(60);
    let task = {
        let (cache, provider, clock) = (cache.clone(), provider.clone(), clock.clone());
        tokio::spawn(async move { get(&cache, &provider, &clock).await })
    };
    provider
        .started
        .acquire()
        .await
        .expect("refresh started")
        .forget();
    task.abort();
    assert!(task.await.expect_err("cancelled").is_cancelled());
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-old"
    );
    clock.set(61);
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-new"
    );
}

#[tokio::test]
async fn cancellation_of_initial_lookup_does_not_block_the_next_caller() {
    let cache = Arc::new(CredentialCache::default());
    let provider = Arc::new(Provider::new([
        Step::Wait(
            Arc::new(Semaphore::new(0)),
            credentials("cancelled", Some(3600)),
        ),
        Step::Return(credentials("valid", Some(3600))),
    ]));
    let clock = Clock::default();
    let task = {
        let (cache, provider, clock) = (cache.clone(), provider.clone(), clock.clone());
        tokio::spawn(async move { get(&cache, &provider, &clock).await })
    };
    provider
        .started
        .acquire()
        .await
        .expect("load started")
        .forget();
    task.abort();
    assert!(task.await.expect_err("cancelled").is_cancelled());
    assert_eq!(
        get(&cache, &provider, &clock).await.access_key_id(),
        "access-valid"
    );
}

#[tokio::test]
async fn expiry_is_checked_after_a_suspended_provider_lookup() {
    let cache = Arc::new(CredentialCache::default());
    let gate = Arc::new(Semaphore::new(0));
    let provider = Arc::new(Provider::new([Step::Wait(
        gate.clone(),
        credentials("old", Some(1)),
    )]));
    let clock = Clock::default();
    let task = {
        let (cache, provider, clock) = (cache.clone(), provider.clone(), clock.clone());
        tokio::spawn(async move { cache.get(provider.as_ref(), || clock.now()).await })
    };
    provider
        .started
        .acquire()
        .await
        .expect("load started")
        .forget();
    clock.set(1);
    gate.add_permits(1);
    assert!(task.await.expect("lookup completed").is_err());
}

#[tokio::test]
async fn distinct_sources_do_not_share_cached_identities() {
    let clock = Clock::default();
    let first = CredentialCache::default();
    let second = CredentialCache::default();
    let a = Provider::new([Step::Return(credentials("a", Some(3600)))]);
    let b = Provider::new([Step::Return(credentials("b", Some(3600)))]);
    assert_eq!(get(&first, &a, &clock).await.access_key_id(), "access-a");
    assert_eq!(get(&second, &b, &clock).await.access_key_id(), "access-b");
    assert_eq!(get(&first, &a, &clock).await.access_key_id(), "access-a");
}
