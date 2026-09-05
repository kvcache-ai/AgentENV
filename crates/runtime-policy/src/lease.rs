use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// One funded authorization for an exact physical activation. A sequence is
/// monotonic within that activation; an equal sequence is an identical replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionLease {
    pub activation_id: Uuid,
    pub operation_id: Uuid,
    pub sequence: u64,
    pub expires_at_unix_ms: u64,
}

pub fn unix_millis(now: SystemTime) -> Result<u64> {
    Ok(u64::try_from(now.duration_since(UNIX_EPOCH)?.as_millis())?)
}

impl ExecutionLease {
    pub fn remaining(&self, now: SystemTime) -> Result<Duration> {
        let remaining = self.expires_at_unix_ms.saturating_sub(unix_millis(now)?);
        if remaining == 0 || self.activation_id.is_nil() || self.operation_id.is_nil() {
            bail!("execution lease is expired or has no identity");
        }
        Ok(Duration::from_millis(remaining))
    }
}

/// Wall-clock rollback cannot buy runtime: every accepted absolute deadline
/// also has a monotonic cutoff fixed when that authorization was accepted.
pub struct LeaseState {
    lease: ExecutionLease,
    monotonic_deadline: Instant,
}

impl LeaseState {
    pub fn new(lease: ExecutionLease, wall: SystemTime, mono: Instant) -> Result<Self> {
        let monotonic_deadline = mono
            .checked_add(lease.remaining(wall)?)
            .ok_or_else(|| anyhow::anyhow!("execution deadline overflows monotonic time"))?;
        Ok(Self {
            lease,
            monotonic_deadline,
        })
    }

    pub fn lease(&self) -> ExecutionLease {
        self.lease
    }

    pub fn expired(&self, wall: SystemTime, mono: Instant) -> bool {
        mono >= self.monotonic_deadline || self.lease.remaining(wall).is_err()
    }

    pub fn renew(&mut self, lease: ExecutionLease, wall: SystemTime, mono: Instant) -> Result<()> {
        if self.expired(wall, mono) {
            bail!("expired execution cannot be renewed");
        }
        if lease.activation_id != self.lease.activation_id || lease.sequence < self.lease.sequence {
            bail!("execution lease lost its activation fence");
        }
        if lease.sequence == self.lease.sequence {
            if lease != self.lease {
                bail!("execution lease replay changed its payload");
            }
            return Ok(());
        }
        let mut next = Self::new(lease, wall, mono)?;
        // A later sequence may add only the newly funded absolute time.
        // Recomputing solely from a rolled-back wall clock would extend it.
        let previous = self.lease.expires_at_unix_ms;
        let bounded = if lease.expires_at_unix_ms >= previous {
            self.monotonic_deadline
                .checked_add(Duration::from_millis(lease.expires_at_unix_ms - previous))
        } else {
            self.monotonic_deadline
                .checked_sub(Duration::from_millis(previous - lease.expires_at_unix_ms))
        }
        .ok_or_else(|| anyhow::anyhow!("renewal deadline overflows monotonic time"))?;
        next.monotonic_deadline = next.monotonic_deadline.min(bounded);
        *self = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn lease() -> ExecutionLease {
        ExecutionLease {
            activation_id: Uuid::now_v7(),
            operation_id: Uuid::now_v7(),
            sequence: 0,
            expires_at_unix_ms: 10_000,
        }
    }
    #[test]
    fn delayed_expired_and_mutated_replays_never_extend_execution() {
        let first = lease();
        let mono = Instant::now();
        let mut state = LeaseState::new(first, UNIX_EPOCH + Duration::from_secs(5), mono).unwrap();
        state
            .renew(
                first,
                UNIX_EPOCH + Duration::from_secs(8),
                mono + Duration::from_secs(3),
            )
            .unwrap();
        assert!(state.expired(UNIX_EPOCH, mono + Duration::from_secs(5)));
        assert!(state
            .renew(
                ExecutionLease {
                    expires_at_unix_ms: 20_000,
                    ..first
                },
                UNIX_EPOCH + Duration::from_secs(9),
                mono
            )
            .is_err());
        assert!(state
            .renew(
                ExecutionLease {
                    sequence: 1,
                    expires_at_unix_ms: 20_000,
                    ..first
                },
                UNIX_EPOCH + Duration::from_secs(10),
                mono
            )
            .is_err());
    }
    #[test]
    fn renewal_is_bound_to_one_activation_and_a_monotonic_sequence() {
        let first = lease();
        let mono = Instant::now();
        let wall = UNIX_EPOCH + Duration::from_secs(5);
        let mut state = LeaseState::new(first, wall, mono).unwrap();
        assert!(state
            .renew(
                ExecutionLease {
                    activation_id: Uuid::now_v7(),
                    sequence: 1,
                    ..first
                },
                wall,
                mono
            )
            .is_err());
        let next = ExecutionLease {
            sequence: 1,
            expires_at_unix_ms: 12_000,
            ..first
        };
        state.renew(next, wall, mono).unwrap();
        assert!(state.renew(first, wall, mono).is_err());
        assert_eq!(state.lease(), next);
    }
    #[test]
    fn a_new_sequence_cannot_rebase_a_rolled_back_clock() {
        let first = lease();
        let mono = Instant::now();
        let mut state = LeaseState::new(first, UNIX_EPOCH + Duration::from_secs(5), mono).unwrap();
        state
            .renew(
                ExecutionLease {
                    sequence: 1,
                    expires_at_unix_ms: 12_000,
                    ..first
                },
                UNIX_EPOCH,
                mono + Duration::from_secs(2),
            )
            .unwrap();
        assert!(!state.expired(UNIX_EPOCH, mono + Duration::from_secs(6)));
        assert!(state.expired(UNIX_EPOCH, mono + Duration::from_secs(7)));
    }
}
