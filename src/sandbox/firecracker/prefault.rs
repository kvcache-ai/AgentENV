//! Restore-time pre-fault decision logic.
//!
//! This module validates persisted working-set metadata before the restore
//! path submits it through the generated Firecracker client.

use super::manifest::{GuestMemoryRegion, GuestMemoryWorkingSet, GuestMemoryWorkingSetLimits};
use crate::virtualization::VirtualizationMode;

/// The Firecracker KVM pre-fault API exists only on x86_64 KVM. Keep this
/// decision ahead of any guest-memory API request so PVM/ARM restores do not
/// parse or call APIs they cannot use.
pub(crate) fn prefault_supported(is_x86_64: bool, virtualization_mode: VirtualizationMode) -> bool {
    is_x86_64 && virtualization_mode == VirtualizationMode::Kvm
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PrefaultSkipReason {
    Disabled,
    UnsupportedArchitecture,
    NoWorkingSet,
    EmptyWorkingSet,
    InvalidWorkingSet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PrefaultPlan {
    Skip(PrefaultSkipReason),
    Request {
        ranges: Vec<super::manifest::GuestMemoryRange>,
        bytes: u64,
    },
}

/// Build a validated all-or-nothing pre-fault plan. A malformed range, budget
/// overrun, or RAM-layout mismatch skips the complete hint instead of truncating
/// arbitrary GPA ranges.
pub(crate) fn build_prefault_plan(
    enabled: bool,
    is_x86_64: bool,
    working_set: Option<&GuestMemoryWorkingSet>,
    regions: &[GuestMemoryRegion],
    limits: GuestMemoryWorkingSetLimits,
) -> PrefaultPlan {
    if !enabled {
        return PrefaultPlan::Skip(PrefaultSkipReason::Disabled);
    }
    if !is_x86_64 {
        return PrefaultPlan::Skip(PrefaultSkipReason::UnsupportedArchitecture);
    }
    let Some(working_set) = working_set else {
        return PrefaultPlan::Skip(PrefaultSkipReason::NoWorkingSet);
    };
    if working_set.ranges.is_empty() {
        return PrefaultPlan::Skip(PrefaultSkipReason::EmptyWorkingSet);
    }
    if working_set.validate_for_regions(regions, limits).is_err() {
        return PrefaultPlan::Skip(PrefaultSkipReason::InvalidWorkingSet);
    }
    let bytes = working_set
        .total_bytes()
        .expect("working-set validation already checked total bytes");
    PrefaultPlan::Request {
        ranges: working_set.ranges.clone(),
        bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::firecracker::manifest::{GuestMemoryRange, GUEST_MEMORY_PAGE_SIZE};

    fn region() -> GuestMemoryRegion {
        GuestMemoryRegion {
            base_host_virt_addr: 0x1000_0000,
            guest_phys_addr: 0,
            size: 0x4000,
            page_size: GUEST_MEMORY_PAGE_SIZE,
        }
    }

    fn limits() -> GuestMemoryWorkingSetLimits {
        GuestMemoryWorkingSetLimits {
            max_bytes: 0x4000,
            max_ranges: 4,
            max_guest_memory_ratio_percent: 100,
        }
    }

    #[test]
    fn no_metadata_or_empty_ranges_never_issue_request() {
        assert_eq!(
            build_prefault_plan(true, true, None, &[region()], limits()),
            PrefaultPlan::Skip(PrefaultSkipReason::NoWorkingSet)
        );
        assert_eq!(
            build_prefault_plan(
                true,
                true,
                Some(&GuestMemoryWorkingSet::new(vec![])),
                &[region()],
                limits(),
            ),
            PrefaultPlan::Skip(PrefaultSkipReason::EmptyWorkingSet)
        );
    }

    #[test]
    fn invalid_metadata_skips_prefault() {
        let invalid = GuestMemoryWorkingSet::new(vec![GuestMemoryRange {
            gpa: 0x4000,
            size: GUEST_MEMORY_PAGE_SIZE,
        }]);
        assert_eq!(
            build_prefault_plan(true, true, Some(&invalid), &[region()], limits()),
            PrefaultPlan::Skip(PrefaultSkipReason::InvalidWorkingSet)
        );
    }

    #[test]
    fn valid_request_requires_enabled_x86_and_api() {
        let working_set = GuestMemoryWorkingSet::new(vec![GuestMemoryRange {
            gpa: 0,
            size: GUEST_MEMORY_PAGE_SIZE,
        }]);
        assert_eq!(
            build_prefault_plan(false, true, Some(&working_set), &[region()], limits()),
            PrefaultPlan::Skip(PrefaultSkipReason::Disabled)
        );
        assert_eq!(
            build_prefault_plan(true, false, Some(&working_set), &[region()], limits()),
            PrefaultPlan::Skip(PrefaultSkipReason::UnsupportedArchitecture)
        );
        assert!(matches!(
            build_prefault_plan(true, true, Some(&working_set), &[region()], limits()),
            PrefaultPlan::Request { bytes: 4096, .. }
        ));
    }

    #[test]
    fn support_gate_requires_x86_kvm() {
        assert!(prefault_supported(true, VirtualizationMode::Kvm));
        assert!(!prefault_supported(false, VirtualizationMode::Kvm));
        assert!(!prefault_supported(true, VirtualizationMode::Pvm));
    }
}
