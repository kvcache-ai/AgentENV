//! Process-wide pool of QPL jobs shared by every `QplCompressor`, for both
//! compression and decompression.
//!
//! The pool probes IAA once: every entry is a hardware job if the probe
//! succeeds, otherwise every entry is a software job. There is no per-call
//! fallback between the two paths yet.

use std::sync::atomic::{fence, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use crossbeam_channel::{Receiver, Sender, TryRecvError};

use super::job::{self, DeviceBuf, Job, Path, QplError};
use crate::compression::zfile::CompressOptions;

/// Jobs per process unless [`set_size`] chose another number.
const DEFAULT_SIZE: usize = 256;
/// Most jobs on the software path, which is only a fallback when IAA is absent.
const SOFTWARE_MAX_SIZE: usize = 256;
/// How long a waiter parks before checking again whether it may create an
/// entry; a discarded entry frees a slot without waking anyone.
const WAIT_SLICE: Duration = Duration::from_millis(1);
/// Log once per call when a whole batch has not been available this long.
const BATCH_WARN_AFTER: Duration = Duration::from_secs(1);

static SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_SIZE);
static GLOBAL: OnceLock<QplJobPool> = OnceLock::new();

/// Sets how many jobs the process-wide pool holds; only before it exists.
pub(super) fn set_size(size: usize) -> Result<()> {
    ensure!(GLOBAL.get().is_none(), "QPL job pool already created");
    SIZE.store(size, Ordering::Relaxed);
    Ok(())
}

/// The process-wide pool, created and prewarmed on first use.
pub(super) fn global() -> &'static QplJobPool {
    GLOBAL.get_or_init(|| QplJobPool::new(SIZE.load(Ordering::Relaxed)))
}

/// Bounce buffer length per hardware entry: the worst-case output of one
/// `DEFAULT_BLOCK_SIZE` block.
fn bounce_len() -> usize {
    job::qpl_compress_bound(CompressOptions::DEFAULT_BLOCK_SIZE) as usize
}

pub(super) enum Entry {
    /// The device writes into `bounce` instead of caller buffers that may not
    /// be faulted in.
    Hardware {
        job: Job,
        bounce: DeviceBuf,
    },
    Software {
        job: Job,
    },
}

impl Entry {
    fn new(path: Path) -> Result<Self, QplError> {
        let job = Job::new(path)?;
        Ok(match path {
            Path::Hardware => Self::Hardware {
                job,
                bounce: DeviceBuf::new_locked(bounce_len()),
            },
            Path::Software => Self::Software { job },
        })
    }
}

pub(super) struct QplJobPool {
    path: Path,
    /// Free entries in groups: a batch goes back as one group, and a take
    /// splits off only what it needs.
    free_tx: Sender<Vec<Entry>>,
    free_rx: Receiver<Vec<Entry>>,
    /// Entries in existence, free or leased; never above the channel capacity,
    /// so neither is the number of free groups.
    created: AtomicUsize,
    /// Held while taking entries, so two takers cannot each hold part of what
    /// they need; `returned` waits on it.
    batch: Mutex<()>,
    /// Signalled when entries come back while a taker waits.
    returned: Condvar,
    /// Takers waiting on `returned`; returns skip the wakeup while it is zero.
    waiting: AtomicUsize,
}

impl QplJobPool {
    /// Probes IAA with the first hardware entry, falling back to the software
    /// path for the whole pool if that fails, then creates every entry.
    fn new(size: usize) -> Self {
        let (path, first) = match Entry::new(Path::Hardware) {
            Ok(entry) => (Path::Hardware, Some(entry)),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "IAA hardware path unavailable; QPL job pool uses the software path"
                );
                (Path::Software, None)
            }
        };
        let capacity = match path {
            Path::Hardware => size,
            Path::Software => size.min(SOFTWARE_MAX_SIZE),
        };
        let pool = Self::empty(path, capacity);
        let entries: Vec<Entry> = first.into_iter().collect();
        pool.created.store(entries.len(), Ordering::Relaxed);
        pool.prewarm(entries);
        tracing::info!(
            ?path,
            created = pool.created.load(Ordering::Relaxed),
            capacity,
            "QPL job pool ready"
        );
        pool
    }

    /// Creates entries up to capacity so callers never pay for job setup, and
    /// frees them in groups of a full hardware batch.
    fn prewarm(&self, mut entries: Vec<Entry>) {
        while self.reserve() {
            match Entry::new(self.path) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    self.created.fetch_sub(1, Ordering::Relaxed);
                    tracing::warn!(
                        %error,
                        "QPL job pool prewarm stopped early; the rest are created on demand"
                    );
                    break;
                }
            }
        }
        while !entries.is_empty() {
            let group = entries.split_off(entries.len().saturating_sub(super::HW_NBATCH));
            self.push(group);
        }
    }

    fn empty(path: Path, capacity: usize) -> Self {
        assert!(capacity > 0, "QPL job pool capacity must be non-zero");
        let (free_tx, free_rx) = crossbeam_channel::bounded(capacity);
        Self {
            path,
            free_tx,
            free_rx,
            created: AtomicUsize::new(0),
            batch: Mutex::new(()),
            returned: Condvar::new(),
            waiting: AtomicUsize::new(0),
        }
    }

    pub(super) fn path(&self) -> Path {
        self.path
    }

    /// Most entries the pool holds at once, free or leased.
    pub(super) fn capacity(&self) -> usize {
        self.free_tx.capacity().expect("bounded channel")
    }

    /// Takes `n` entries at once, or none: while fewer are available it
    /// returns what it took and sleeps until entries come back.
    pub(super) fn take_batch(&self, n: usize) -> Result<Batch<'_>> {
        let capacity = self.capacity();
        assert!(
            (1..=capacity).contains(&n),
            "batch of {n} outside 1..={capacity} (QPL job pool capacity)"
        );
        let mut guard = self.batch.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(taken) = self.try_take_batch(n).transpose() {
            return taken;
        }
        // Counted before the re-check below, so a return either lands before
        // that re-check or sees this waiter and wakes it.
        self.waiting.fetch_add(1, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let started = Instant::now();
        let mut warned = false;
        let taken = loop {
            if let Some(taken) = self.try_take_batch(n).transpose() {
                break taken;
            }
            if !warned && started.elapsed() >= BATCH_WARN_AFTER {
                tracing::warn!(n, "QPL job pool cannot supply a whole batch; still waiting");
                warned = true;
            }
            guard = self
                .returned
                .wait_timeout(guard, WAIT_SLICE)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        };
        self.waiting.fetch_sub(1, Ordering::SeqCst);
        taken
    }

    /// Must be called with `batch` held.
    fn try_take_batch(&self, n: usize) -> Result<Option<Batch<'_>>> {
        let mut entries: Vec<Entry> = Vec::new();
        while entries.len() < n {
            match self.free_rx.try_recv() {
                Ok(group) if entries.is_empty() => entries = group,
                Ok(group) => entries.extend(group),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => unreachable!("the pool owns its sender"),
            }
        }
        if entries.len() > n {
            self.give_back_locked(entries.split_off(n));
        }
        while entries.len() < n && self.reserve() {
            match Entry::new(self.path) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    self.created.fetch_sub(1, Ordering::Relaxed);
                    self.give_back_locked(entries);
                    return Err(error.into());
                }
            }
        }
        if entries.len() < n {
            self.give_back_locked(entries);
            return Ok(None);
        }
        Ok(Some(Batch {
            pool: self,
            entries,
        }))
    }

    /// Frees `group` without holding `batch`.
    fn give_back(&self, group: Vec<Entry>) {
        if group.is_empty() {
            return;
        }
        self.push(group);
        // Pairs with the waiter's increment of `waiting` before its re-check.
        fence(Ordering::SeqCst);
        if self.waiting.load(Ordering::SeqCst) > 0 {
            // Once we hold `batch`, every counted waiter is asleep on `returned`.
            drop(self.batch.lock().unwrap_or_else(PoisonError::into_inner));
            self.returned.notify_one();
        }
    }

    /// Frees `group` while holding `batch`.
    fn give_back_locked(&self, group: Vec<Entry>) {
        if group.is_empty() {
            return;
        }
        self.push(group);
        if self.waiting.load(Ordering::SeqCst) > 0 {
            self.returned.notify_one();
        }
    }

    fn push(&self, group: Vec<Entry>) {
        let n = group.len();
        // Cannot fail while `created` stays within capacity; never block here.
        if self.free_tx.try_send(group).is_err() {
            self.created.fetch_sub(n, Ordering::Relaxed);
            tracing::warn!(
                n,
                "QPL job pool is unexpectedly full; dropped returned jobs"
            );
        }
    }

    /// Claims room for one more entry if the pool is below capacity.
    fn reserve(&self) -> bool {
        let capacity = self.capacity();
        self.created
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < capacity).then_some(n + 1)
            })
            .is_ok()
    }
}

/// Exclusive use of a group of entries; returns them to the pool together on
/// drop.
pub(super) struct Batch<'p> {
    pool: &'p QplJobPool,
    entries: Vec<Entry>,
}

impl Batch<'_> {
    pub(super) fn entries_mut(&mut self) -> &mut [Entry] {
        &mut self.entries
    }

    /// Frees the entries at ascending `indices` instead of returning them,
    /// after an error left their jobs in an unknown state. A later take
    /// creates replacements.
    pub(super) fn discard(&mut self, indices: &[usize]) {
        for &i in indices.iter().rev() {
            self.entries.swap_remove(i);
        }
        self.pool
            .created
            .fetch_sub(indices.len(), Ordering::Relaxed);
    }
}

impl Drop for Batch<'_> {
    fn drop(&mut self) {
        self.pool.give_back(std::mem::take(&mut self.entries));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_batch_splits_groups_and_waits_for_returns() {
        let pool = QplJobPool::new(4);
        let one = pool.take_batch(1).expect("take 1");
        let three = pool.take_batch(3).expect("take 3");
        assert_eq!(pool.created.load(Ordering::Relaxed), 4);
        drop(one);
        std::thread::scope(|s| {
            let waiter = s.spawn(|| pool.take_batch(4).map(|batch| batch.entries.len()));
            // One entry is free; the waiter needs the other three back.
            std::thread::sleep(Duration::from_millis(20));
            assert!(!waiter.is_finished());
            drop(three);
            assert_eq!(waiter.join().expect("waiter").expect("take 4"), 4);
        });
    }
}
