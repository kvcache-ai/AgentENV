//! Leases that let one node at a time write a shared document on Alibaba OSS,
//! which cannot replace an object only if it is unchanged.
//!
//! Alibaba OSS appends to an object only at its current length, so of two
//! appends at one length exactly one lands. A document's lock is an appendable
//! object of JSON lines under `locks/`, a prefix older nodes never list. Each
//! line is a [`Frame`] that claims the lock for one attempt, naming the offset
//! it expects to land at and the holder it replaces. A frame counts only if it
//! landed there while that holder held the lock, and the last frame that
//! counts holds it, so appends made from an outdated view never take the lock.
//! Releasing deletes the lock object.
//!
//! A holder that dies never releases, so a contender that has seen the same
//! holder for [`LEASE`] takes the lock over. Nothing can stop a holder that is
//! merely slow, so holders bound themselves: every write a holder makes starts
//! before [`Lease::deadline`], [`MARGIN`] before any contender may take over,
//! and ends within it. Each node measures time on its own monotonic clock.

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::warn;
use uuid::Uuid;

use super::client::{OssClient, WRITE_ATTEMPT_TIMEOUT};
use super::documents::Backoff;
use crate::repository::{RepositoryError, RepositoryResult};

/// How long a contender waits on one holder before taking its lock over.
const LEASE: Duration = Duration::from_secs(30);
/// How long before a contender may take over a holder's writes must start.
const MARGIN: Duration = Duration::from_secs(10);
// A write may take a second attempt after refreshing credentials.
const _: () = assert!(MARGIN.as_secs() > 2 * WRITE_ATTEMPT_TIMEOUT.as_secs());
/// Long enough to outwait one holder that died, and contention after it.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(75);
const LOCKS_PREFIX: &str = "locks/";

/// The lock object operations a lease needs.
#[async_trait]
pub(super) trait LockStore: Send + Sync {
    /// The object at `key`; `None` when absent.
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>>;
    /// See [`OssClient::append`].
    async fn append(&self, key: &str, data: Vec<u8>) -> Result<Option<u64>>;
    /// See [`OssClient::delete_before`].
    async fn delete_before(&self, key: &str, deadline: Instant) -> Result<()>;
}

#[async_trait]
impl LockStore for OssClient {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.get_bytes(key).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(error) if Self::is_not_found_error(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn append(&self, key: &str, data: Vec<u8>) -> Result<Option<u64>> {
        OssClient::append(self, key, data).await
    }

    async fn delete_before(&self, key: &str, deadline: Instant) -> Result<()> {
        OssClient::delete_before(self, key, deadline).await
    }
}

/// One line of a lock object.
#[derive(Debug, Serialize, Deserialize)]
struct Frame {
    /// The offset the frame must land at to count.
    at: u64,
    /// The holder the frame replaces; `None` when the lock is free.
    prev: Option<String>,
    /// The attempt that appended the frame.
    token: String,
}

/// What a lock object shows.
#[derive(Debug, Default)]
struct LockState {
    /// The object's length; 0 when absent.
    len: u64,
    holder: Option<String>,
}

impl LockState {
    fn parse(object: &[u8]) -> Self {
        let mut state = Self::default();
        for line in object.split_inclusive(|byte| *byte == b'\n') {
            if let Ok(frame) = serde_json::from_slice::<Frame>(line) {
                if frame.at == state.len && frame.prev == state.holder {
                    state.holder = Some(frame.token);
                }
            }
            state.len += line.len() as u64;
        }
        state
    }

    /// The frame by which `token` takes the lock from this state.
    fn claim(&self, token: &str) -> Vec<u8> {
        let frame = Frame {
            at: self.len,
            prev: self.holder.clone(),
            token: token.to_owned(),
        };
        let mut line = serde_json::to_vec(&frame).expect("lock frames serialize");
        line.push(b'\n');
        line
    }
}

/// A held lock. Dropping it without [`release`] leaves it to run out.
#[derive(Debug)]
pub(super) struct Lease {
    key: String,
    deadline: Instant,
}

impl Lease {
    /// `started` is when the append that took the lock was sent.
    fn new(key: String, started: Instant) -> Self {
        Self {
            key,
            deadline: started + LEASE - MARGIN,
        }
    }

    /// Writes made under this lease must start before this.
    pub(super) fn deadline(&self) -> Instant {
        self.deadline
    }
}

/// Takes the lock of the document at `document`, giving up after
/// [`ACQUIRE_TIMEOUT`]. A claim that lands after that runs out.
pub(super) async fn acquire<S: LockStore + ?Sized>(
    store: &S,
    document: &str,
) -> RepositoryResult<Lease> {
    let key = format!("{LOCKS_PREFIX}{document}");
    match tokio::time::timeout(ACQUIRE_TIMEOUT, take(store, &key)).await {
        Ok(taken) => taken,
        Err(_) => Err(RepositoryError::InvalidRequest {
            reason: format!("'{key}' stayed locked for {ACQUIRE_TIMEOUT:?}"),
        }),
    }
}

async fn take<S: LockStore + ?Sized>(store: &S, key: &str) -> RepositoryResult<Lease> {
    let mut backoff = Backoff::new();
    // Every append is a separate attempt, so a holder knows which of its
    // appends took the lock, and when it was sent.
    let mut attempts: Vec<(String, Instant)> = Vec::new();
    // The holder this node has seen without a break, and since when.
    let mut waiting_on: Option<(String, Instant)> = None;
    // Optimistically, the lock is free and its object absent.
    let mut state = LockState::default();
    loop {
        let token = Uuid::now_v7().to_string();
        let sent = Instant::now();
        attempts.push((token.clone(), sent));
        let landed = store
            .append(key, state.claim(&token))
            .await
            .map_err(|error| RepositoryError::backend(format!("append to '{key}'"), error))?;
        // Landing at the start of an absent object leaves this frame alone
        // in it. Anywhere else, the object may have been deleted and grown
        // back since it was read, so only a new read tells.
        if state.len == 0 && landed == Some(0) {
            return Ok(Lease::new(key.to_owned(), sent));
        }
        loop {
            state = match store.read(key).await {
                Ok(object) => LockState::parse(object.as_deref().unwrap_or_default()),
                Err(error) => return Err(RepositoryError::backend(format!("read '{key}'"), error)),
            };
            let Some(holder) = state.holder.as_deref() else {
                break;
            };
            if let Some((_, sent)) = attempts.iter().find(|(token, _)| token == holder) {
                return Ok(Lease::new(key.to_owned(), *sent));
            }
            let since = match &waiting_on {
                Some((seen, since)) if seen == holder => *since,
                _ => waiting_on.insert((holder.to_owned(), Instant::now())).1,
            };
            if since.elapsed() >= LEASE {
                warn!(key, holder, "taking over a lock its holder kept too long");
                break;
            }
            backoff.wait().await;
        }
    }
}

/// Releases `lease`, unless a contender may take it over before the release
/// lands; then the lease runs out instead.
pub(super) async fn release<S: LockStore + ?Sized>(store: &S, lease: Lease) {
    if let Err(error) = store.delete_before(&lease.key, lease.deadline).await {
        warn!(key = %lease.key, error = %format!("{error:#}"), "left a lease to run out");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    /// How long each request to [`FakeStore`] takes, so requests overlap.
    const LATENCY: Duration = Duration::from_millis(5);

    /// Lock objects in memory, appended to like opendal does on Alibaba OSS.
    #[derive(Default)]
    struct FakeStore {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        /// Makes the next append land but report that it did not.
        lose_next_reply: AtomicBool,
        /// Makes appends never complete.
        hang: AtomicBool,
    }

    impl FakeStore {
        fn len(&self, key: &str) -> u64 {
            let objects = self.objects.lock().unwrap();
            objects.get(key).map_or(0, |object| object.len() as u64)
        }
    }

    #[async_trait]
    impl LockStore for FakeStore {
        async fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
            tokio::time::sleep(LATENCY).await;
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }

        async fn append(&self, key: &str, data: Vec<u8>) -> Result<Option<u64>> {
            if self.hang.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            // Reads the length, then appends there unless the object grew.
            tokio::time::sleep(LATENCY).await;
            let offset = self.len(key);
            tokio::time::sleep(LATENCY).await;
            if self.len(key) != offset {
                return Ok(None);
            }
            let mut objects = self.objects.lock().unwrap();
            objects.entry(key.to_owned()).or_default().extend(data);
            if self.lose_next_reply.swap(false, Ordering::SeqCst) {
                return Ok(None);
            }
            Ok(Some(offset))
        }

        async fn delete_before(&self, key: &str, deadline: Instant) -> Result<()> {
            anyhow::ensure!(Instant::now() < deadline, "deadline passed");
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_writers_lose_no_update() {
        let store = Arc::new(FakeStore::default());
        let document = Arc::new(Mutex::new(0));
        let writers: Vec<_> = (0..16)
            .map(|_| {
                let (store, document) = (Arc::clone(&store), Arc::clone(&document));
                tokio::spawn(async move {
                    let lease = acquire(store.as_ref(), "doc.json").await?;
                    let read = *document.lock().unwrap();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    assert!(Instant::now() < lease.deadline());
                    *document.lock().unwrap() = read + 1;
                    release(store.as_ref(), lease).await;
                    RepositoryResult::Ok(())
                })
            })
            .collect();
        for writer in writers {
            writer.await.unwrap().unwrap();
        }
        assert_eq!(*document.lock().unwrap(), 16);
        assert!(store.objects.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_live_holder_keeps_the_lock_until_it_releases() {
        let store = Arc::new(FakeStore::default());
        let held = acquire(store.as_ref(), "doc.json").await.unwrap();
        let waiter = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                acquire(store.as_ref(), "doc.json").await.unwrap();
                Instant::now()
            }
        });
        tokio::time::sleep_until(held.deadline() - Duration::from_millis(1)).await;
        assert!(!waiter.is_finished());
        let released = Instant::now();
        release(store.as_ref(), held).await;
        assert!(waiter.await.unwrap() - released < Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn a_lock_whose_holder_died_is_taken_over_after_the_lease() {
        let store = FakeStore::default();
        drop(acquire(&store, "doc.json").await.unwrap());
        let started = Instant::now();
        let lease = acquire(&store, "doc.json").await.unwrap();
        assert!(started.elapsed() >= LEASE);
        assert!(started.elapsed() < LEASE + Duration::from_secs(1));
        assert!(lease.deadline() > Instant::now());
        release(&store, lease).await;
        assert!(store.objects.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn an_append_reported_lost_still_takes_the_lock() {
        let store = FakeStore::default();
        store.lose_next_reply.store(true, Ordering::SeqCst);
        let started = Instant::now();
        let lease = acquire(&store, "doc.json").await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(lease.deadline(), started + LEASE - MARGIN);
    }

    #[tokio::test(start_paused = true)]
    async fn acquiring_gives_up_on_a_store_that_hangs() {
        let store = FakeStore::default();
        store.hang.store(true, Ordering::SeqCst);
        let started = Instant::now();
        assert!(acquire(&store, "doc.json").await.is_err());
        assert_eq!(started.elapsed(), ACQUIRE_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_past_its_deadline_is_left_to_run_out() {
        let store = FakeStore::default();
        let lease = acquire(&store, "doc.json").await.unwrap();
        tokio::time::sleep_until(lease.deadline()).await;
        release(&store, lease).await;
        assert_eq!(store.objects.lock().unwrap().len(), 1);
    }

    #[test]
    fn only_frames_landing_where_they_claim_hold_the_lock() {
        let free = LockState::default();
        let mut object = free.claim("a");
        let held = LockState::parse(&object);
        assert_eq!(held.holder.as_deref(), Some("a"));

        // Claimed from an outdated view, so it lands past its offset.
        object.extend(free.claim("b"));
        assert_eq!(LockState::parse(&object).holder.as_deref(), Some("a"));

        // Claimed from a lock object that was deleted and grew back to the
        // same length under another holder.
        let recreated = LockState::parse(&free.claim("c"));
        assert_eq!(recreated.len, held.len);
        let mut object = free.claim("a");
        object.extend(recreated.claim("d"));
        let state = LockState::parse(&object);
        assert_eq!(state.holder.as_deref(), Some("a"));

        object.extend(state.claim("e"));
        assert_eq!(LockState::parse(&object).holder.as_deref(), Some("e"));
    }
}
