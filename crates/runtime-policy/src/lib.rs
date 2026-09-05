//! Shared execution authorization and node admission arithmetic.
//! Scheduling reports are hints; this policy is enforced at the target node.

mod lease;
pub mod watchdog;

pub use lease::{unix_millis, ExecutionLease, LeaseState};

use anyhow::{bail, Result};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct Reservation {
    pub memory_bytes: u64,
    pub maximum_memory_bytes: u64,
    pub disk_bytes: u64,
    pub starting: bool,
}

/// All operations run under the node owner's mutex. Kernel limit writes must
/// succeed before releasing a reservation or transferring its memory budget.
#[derive(Debug)]
pub struct Budget {
    pub memory_bytes: u64,
    pub starting_limit: usize,
    pub reservations: HashMap<Uuid, Reservation>,
}

impl Budget {
    pub fn new(memory_bytes: u64, starting_limit: usize) -> Result<Self> {
        if memory_bytes == 0 || starting_limit == 0 {
            bail!("node admission needs positive memory and startup capacity");
        }
        Ok(Self {
            memory_bytes,
            starting_limit,
            reservations: HashMap::new(),
        })
    }

    pub fn available_memory(&self) -> u64 {
        self.memory_bytes.saturating_sub(
            self.reservations
                .values()
                .fold(0u64, |total, item| total.saturating_add(item.memory_bytes)),
        )
    }

    pub fn admit(
        &mut self,
        id: Uuid,
        reservation: Reservation,
        memory_available: u64,
        disk_available: u64,
    ) -> Result<()> {
        if self.reservations.contains_key(&id) {
            bail!("runtime already holds node capacity");
        }
        if self
            .reservations
            .values()
            .filter(|item| item.starting)
            .count()
            >= self.starting_limit
        {
            bail!("node startup capacity is exhausted");
        }
        if reservation.memory_bytes == 0
            || reservation.memory_bytes > reservation.maximum_memory_bytes
            || reservation.memory_bytes > self.available_memory()
            || reservation.memory_bytes > memory_available
        {
            bail!("node resident memory capacity is exhausted");
        }
        let reserved_disk = self
            .reservations
            .values()
            .fold(0u64, |total, item| total.saturating_add(item.disk_bytes));
        if reservation.disk_bytes > disk_available.saturating_sub(reserved_disk) {
            bail!("node disk capacity is exhausted");
        }
        self.reservations.insert(id, reservation);
        Ok(())
    }

    pub fn growth(
        &self,
        id: Uuid,
        desired: u64,
        memory_available: u64,
        disk_available: u64,
    ) -> Option<u64> {
        let item = self.reservations.get(&id)?;
        let reserved_disk = self
            .reservations
            .values()
            .fold(0u64, |total, item| total.saturating_add(item.disk_bytes));
        let added = desired
            .min(item.maximum_memory_bytes)
            .saturating_sub(item.memory_bytes)
            .min(self.available_memory())
            .min(memory_available)
            .min(disk_available.saturating_sub(reserved_disk));
        (added > 0).then_some(item.memory_bytes + added)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_admissions_share_pending_memory_disk_and_startup_capacity() {
        let mut budget = Budget::new(10, 2).unwrap();
        let a = Uuid::now_v7();
        let request = Reservation {
            memory_bytes: 4,
            maximum_memory_bytes: 9,
            disk_bytes: 5,
            starting: true,
        };
        budget.admit(a, request, 10, 10).unwrap();
        assert!(budget.admit(Uuid::now_v7(), request, 10, 9).is_err());
        budget.admit(Uuid::now_v7(), request, 10, 10).unwrap();
        assert!(budget.admit(Uuid::now_v7(), request, 10, 100).is_err());
        assert_eq!(budget.growth(a, 9, 100, 100), Some(6));
        assert_eq!(budget.growth(a, 9, 1, 100), Some(5));
        assert_eq!(budget.growth(a, 9, 100, 10), None);
        assert!(budget.admit(a, request, 100, 100).is_err());
    }
}
