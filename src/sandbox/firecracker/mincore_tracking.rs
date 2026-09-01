//! Host-independent conversion of Firecracker mincore ranges to GPA metadata.
//!
//! Firecracker reports mincore residency in its contiguous snapshot-image
//! layout.  This module deliberately requires each input range to stay inside
//! one region before translating it to GPA, so x86 MMIO holes cannot be
//! accidentally elided.

use anyhow::{ensure, Context, Result};

use super::manifest::{
    GuestMemoryRange, GuestMemoryWorkingSet, GuestMemoryWorkingSetLimits, GUEST_MEMORY_PAGE_SIZE,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GuestMemoryImageRegion {
    pub image_offset: u64,
    pub guest_phys_addr: u64,
    pub size: u64,
    pub page_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResidentMemoryRange {
    pub image_offset: u64,
    pub length: u64,
}

/// Translate Firecracker's snapshot-image ranges into sorted, deduplicated,
/// coalesced GPA ranges. The caller must publish no metadata on an error.
pub(crate) fn resident_ranges_to_working_set(
    resident: &[ResidentMemoryRange],
    regions: &[GuestMemoryImageRegion],
    limits: GuestMemoryWorkingSetLimits,
) -> Result<GuestMemoryWorkingSet> {
    validate_image_regions(regions)?;
    let mut ranges = Vec::with_capacity(resident.len());
    for range in resident {
        ensure!(range.length != 0, "resident range has zero length");
        ensure!(
            range.image_offset % GUEST_MEMORY_PAGE_SIZE == 0
                && range.length % GUEST_MEMORY_PAGE_SIZE == 0,
            "resident range {:#x}+{} is not 4 KiB aligned",
            range.image_offset,
            range.length
        );
        let end = range
            .image_offset
            .checked_add(range.length)
            .context("resident image range overflows u64")?;
        let region = regions
            .iter()
            .find(|region| {
                let region_end = region.image_offset.checked_add(region.size);
                range.image_offset >= region.image_offset && region_end.is_some_and(|e| end <= e)
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "resident image range {:#x}..{:#x} crosses or is outside a guest-memory region",
                    range.image_offset,
                    end
                )
            })?;
        let delta = range.image_offset - region.image_offset;
        let gpa = region
            .guest_phys_addr
            .checked_add(delta)
            .context("resident GPA start overflows u64")?;
        gpa.checked_add(range.length)
            .context("resident GPA end overflows u64")?;
        ranges.push(GuestMemoryRange {
            gpa,
            size: range.length,
        });
    }

    ranges.sort_by_key(|range| range.gpa);
    let mut coalesced: Vec<GuestMemoryRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = coalesced.last_mut() {
            let previous_end = previous
                .gpa
                .checked_add(previous.size)
                .context("coalesced GPA range overflows u64")?;
            if range.gpa <= previous_end {
                let range_end = range
                    .gpa
                    .checked_add(range.size)
                    .context("resident GPA end overflows u64")?;
                if range_end > previous_end {
                    previous.size = range_end - previous.gpa;
                }
                continue;
            }
        }
        coalesced.push(range);
    }
    let working_set = GuestMemoryWorkingSet::new(coalesced);
    let ram_regions = regions
        .iter()
        .map(|region| super::manifest::GuestMemoryRegion {
            // This value is only used by common RAM-layout validation, which
            // needs distinct monotonic regions but never persists HVA data.
            base_host_virt_addr: region.image_offset,
            guest_phys_addr: region.guest_phys_addr,
            size: region.size,
            page_size: region.page_size,
        })
        .collect::<Vec<_>>();
    working_set.validate_for_regions(&ram_regions, limits)?;
    Ok(working_set)
}

fn validate_image_regions(regions: &[GuestMemoryImageRegion]) -> Result<()> {
    ensure!(
        !regions.is_empty(),
        "no guest-memory regions returned by Firecracker"
    );
    for region in regions {
        ensure!(region.size != 0, "guest-memory image region has zero size");
        ensure!(
            region.page_size == GUEST_MEMORY_PAGE_SIZE,
            "guest-memory image region has unsupported page size {}",
            region.page_size
        );
        ensure!(
            region.image_offset % GUEST_MEMORY_PAGE_SIZE == 0
                && region.guest_phys_addr % GUEST_MEMORY_PAGE_SIZE == 0
                && region.size % GUEST_MEMORY_PAGE_SIZE == 0,
            "guest-memory image region is not 4 KiB aligned"
        );
        region
            .image_offset
            .checked_add(region.size)
            .context("guest-memory image region offset overflows u64")?;
        region
            .guest_phys_addr
            .checked_add(region.size)
            .context("guest-memory GPA region overflows u64")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::firecracker::manifest::MINCORE_RESIDENCY_TRACKER;

    fn limits() -> GuestMemoryWorkingSetLimits {
        GuestMemoryWorkingSetLimits {
            max_bytes: 0x20_000,
            max_ranges: 8,
            max_guest_memory_ratio_percent: 100,
        }
    }
    fn regions() -> Vec<GuestMemoryImageRegion> {
        vec![
            GuestMemoryImageRegion {
                image_offset: 0,
                guest_phys_addr: 0,
                size: 0x4000,
                page_size: 4096,
            },
            GuestMemoryImageRegion {
                image_offset: 0x4000,
                guest_phys_addr: 0x1_0000_0000,
                size: 0x4000,
                page_size: 4096,
            },
        ]
    }

    #[test]
    fn converts_a_single_region() -> Result<()> {
        let set = resident_ranges_to_working_set(
            &[ResidentMemoryRange {
                image_offset: 0x1000,
                length: 0x2000,
            }],
            &regions(),
            limits(),
        )?;
        assert_eq!(set.tracker, MINCORE_RESIDENCY_TRACKER);
        assert_eq!(
            set.ranges,
            vec![GuestMemoryRange {
                gpa: 0x1000,
                size: 0x2000
            }]
        );
        Ok(())
    }

    #[test]
    fn preserves_the_x86_mmio_hole() -> Result<()> {
        let set = resident_ranges_to_working_set(
            &[ResidentMemoryRange {
                image_offset: 0x4000,
                length: 0x1000,
            }],
            &regions(),
            limits(),
        )?;
        assert_eq!(
            set.ranges,
            vec![GuestMemoryRange {
                gpa: 0x1_0000_0000,
                size: 0x1000
            }]
        );
        Ok(())
    }

    #[test]
    fn rejects_cross_region_and_unaligned_ranges() {
        assert!(resident_ranges_to_working_set(
            &[ResidentMemoryRange {
                image_offset: 0x3000,
                length: 0x2000
            }],
            &regions(),
            limits()
        )
        .is_err());
        assert!(resident_ranges_to_working_set(
            &[ResidentMemoryRange {
                image_offset: 1,
                length: 0x1000
            }],
            &regions(),
            limits()
        )
        .is_err());
    }

    #[test]
    fn sorts_deduplicates_and_coalesces_only_true_gpa_contiguity() -> Result<()> {
        let set = resident_ranges_to_working_set(
            &[
                ResidentMemoryRange {
                    image_offset: 0x2000,
                    length: 0x1000,
                },
                ResidentMemoryRange {
                    image_offset: 0x1000,
                    length: 0x2000,
                },
                ResidentMemoryRange {
                    image_offset: 0x4000,
                    length: 0x1000,
                },
            ],
            &regions(),
            limits(),
        )?;
        assert_eq!(
            set.ranges,
            vec![
                GuestMemoryRange {
                    gpa: 0x1000,
                    size: 0x2000
                },
                GuestMemoryRange {
                    gpa: 0x1_0000_0000,
                    size: 0x1000
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn rejects_overflow_and_budget_overrun() {
        let overflow = vec![GuestMemoryImageRegion {
            image_offset: 0,
            guest_phys_addr: u64::MAX - 0xfff,
            size: 0x1000,
            page_size: 4096,
        }];
        assert!(resident_ranges_to_working_set(
            &[ResidentMemoryRange {
                image_offset: 0,
                length: 0x1000
            }],
            &overflow,
            limits()
        )
        .is_err());
        let tight = GuestMemoryWorkingSetLimits {
            max_bytes: 0x1000,
            ..limits()
        };
        assert!(resident_ranges_to_working_set(
            &[ResidentMemoryRange {
                image_offset: 0,
                length: 0x2000
            }],
            &regions(),
            tight
        )
        .is_err());
    }
}
