use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::sandbox::ExtraDrive;

pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Manifest describing the on-disk layout of a Firecracker snapshot.
///
/// This is intentionally decoupled from in-memory snapshot representations.
/// Snapshot-layer retrieve artifacts based on the manifest during snapshot
/// publication, and reconstruct the manifest with hydrated paths during snapshot resolution.
///
/// All paths in the manifest should be absolute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerSnapshotManifest {
    /// Schema/version marker for persisted manifest format.
    pub version: u32,
    pub vm_state: FirecrackerVmStateArtifacts,
    pub memory: FirecrackerMemoryArtifacts,
    pub rootfs: FirecrackerRootfsArtifacts,
    pub attached_drives: Vec<FirecrackerAttachedDriveArtifacts>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerVmStateArtifacts {
    #[serde(skip)]
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerMemoryArtifacts {
    #[serde(skip)]
    pub image_config_path: PathBuf,
    pub virtual_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_set: Option<GuestMemoryWorkingSet>,
}

pub const GUEST_MEMORY_PAGE_SIZE: u64 = 4096;
pub const GUEST_MEMORY_WORKING_SET_VERSION: u32 = 1;
pub const MINCORE_RESIDENCY_TRACKER: &str = "mincore-residency";

/// A Firecracker guest-RAM mapping. GPA is deliberately distinct from snapshot
/// image offsets, which can be contiguous across holes such as x86 MMIO space.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuestMemoryRegion {
    pub base_host_virt_addr: u64,
    pub guest_phys_addr: u64,
    pub size: u64,
    pub page_size: u64,
}

/// A GPA range used for restore-time KVM pre-fault.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuestMemoryRange {
    pub gpa: u64,
    pub size: u64,
}

/// Optional host-independent working set captured during template profiling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuestMemoryWorkingSet {
    pub version: u32,
    pub page_size: u64,
    pub tracker: String,
    pub observation_window: String,
    pub ranges: Vec<GuestMemoryRange>,
}

/// Limits used while accepting profiling output and before sending pre-fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestMemoryWorkingSetLimits {
    pub max_bytes: u64,
    pub max_ranges: usize,
    pub max_guest_memory_ratio_percent: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerRootfsArtifacts {
    #[serde(skip)]
    pub image_config_path: PathBuf,
    pub virtual_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerAttachedDriveArtifacts {
    pub drive_id: String,
    pub read_only: bool,
    #[serde(default)]
    pub mount_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<PathBuf>,
    pub virtual_size: u64,
    #[serde(skip)]
    pub image_config_path: PathBuf,
}

impl GuestMemoryWorkingSet {
    pub fn new(ranges: Vec<GuestMemoryRange>) -> Self {
        Self {
            version: GUEST_MEMORY_WORKING_SET_VERSION,
            page_size: GUEST_MEMORY_PAGE_SIZE,
            tracker: MINCORE_RESIDENCY_TRACKER.to_string(),
            observation_window: "snapshot-resume-to-envd-ready".to_string(),
            ranges,
        }
    }

    pub fn total_bytes(&self) -> Result<u64> {
        self.ranges.iter().try_fold(0_u64, |total, range| {
            total
                .checked_add(range.size)
                .context("working-set byte count overflows u64")
        })
    }

    /// Validate persisted fields which do not depend on a particular restored VM.
    pub fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.version == GUEST_MEMORY_WORKING_SET_VERSION,
            "unsupported guest-memory working-set version {}",
            self.version
        );
        ensure!(
            self.page_size == GUEST_MEMORY_PAGE_SIZE,
            "guest-memory working set requires 4 KiB pages, got {}",
            self.page_size
        );
        ensure!(
            self.tracker == MINCORE_RESIDENCY_TRACKER,
            "unsupported guest-memory working-set tracker {:?}",
            self.tracker
        );
        ensure!(
            self.observation_window == "snapshot-resume-to-envd-ready",
            "unsupported guest-memory working-set observation window {:?}",
            self.observation_window
        );

        let mut previous_end = None;
        for range in &self.ranges {
            ensure!(
                range.gpa % GUEST_MEMORY_PAGE_SIZE == 0,
                "working-set GPA {:#x} is not 4 KiB aligned",
                range.gpa
            );
            ensure!(
                range.size != 0 && range.size % GUEST_MEMORY_PAGE_SIZE == 0,
                "working-set range at GPA {:#x} has invalid size {}",
                range.gpa,
                range.size
            );
            let end = range
                .gpa
                .checked_add(range.size)
                .context("working-set GPA range overflows u64")?;
            if let Some(previous_end) = previous_end {
                ensure!(
                    previous_end < range.gpa,
                    "working-set ranges must be sorted, non-overlapping, and coalesced"
                );
            }
            previous_end = Some(end);
        }
        Ok(())
    }

    /// Validate metadata against the RAM layout actually returned by Firecracker.
    pub fn validate_for_regions(
        &self,
        regions: &[GuestMemoryRegion],
        limits: GuestMemoryWorkingSetLimits,
    ) -> Result<()> {
        self.validate_shape()?;
        validate_guest_memory_regions(regions)?;
        ensure!(
            self.ranges.len() <= limits.max_ranges,
            "working-set range count {} exceeds configured maximum {}",
            self.ranges.len(),
            limits.max_ranges
        );
        let total_bytes = self.total_bytes()?;
        ensure!(
            total_bytes <= limits.max_bytes,
            "working-set byte count {} exceeds configured maximum {}",
            total_bytes,
            limits.max_bytes
        );
        ensure!(
            limits.max_guest_memory_ratio_percent <= 100,
            "working-set guest-memory ratio limit must be at most 100"
        );
        let guest_bytes = regions.iter().try_fold(0_u64, |total, region| {
            total
                .checked_add(region.size)
                .context("guest RAM total overflows u64")
        })?;
        ensure!(
            u128::from(total_bytes) * 100
                <= u128::from(guest_bytes) * u128::from(limits.max_guest_memory_ratio_percent),
            "working-set byte count {} exceeds {}% of guest RAM {}",
            total_bytes,
            limits.max_guest_memory_ratio_percent,
            guest_bytes
        );
        for range in &self.ranges {
            let end = range
                .gpa
                .checked_add(range.size)
                .context("working-set GPA range overflows u64")?;
            let inside_guest_ram = regions.iter().any(|region| {
                let region_end = region
                    .guest_phys_addr
                    .checked_add(region.size)
                    .expect("region overflow was checked before working-set validation");
                range.gpa >= region.guest_phys_addr && end <= region_end
            });
            ensure!(
                inside_guest_ram,
                "working-set range {:#x}..{:#x} is outside Firecracker guest RAM",
                range.gpa,
                end
            );
        }
        Ok(())
    }
}

pub(crate) fn validate_guest_memory_regions(regions: &[GuestMemoryRegion]) -> Result<()> {
    ensure!(
        !regions.is_empty(),
        "Firecracker returned no guest-memory regions"
    );
    for region in regions {
        ensure!(
            region.page_size == GUEST_MEMORY_PAGE_SIZE,
            "only normal 4 KiB guest pages are supported; Firecracker reported {}",
            region.page_size
        );
        ensure!(
            region.size != 0 && region.size % GUEST_MEMORY_PAGE_SIZE == 0,
            "guest-memory region size must be a non-zero multiple of 4 KiB"
        );
        ensure!(
            region.base_host_virt_addr % GUEST_MEMORY_PAGE_SIZE == 0
                && region.guest_phys_addr % GUEST_MEMORY_PAGE_SIZE == 0,
            "guest-memory HVA and GPA starts must be 4 KiB aligned"
        );
        region
            .base_host_virt_addr
            .checked_add(region.size)
            .context("guest-memory HVA range overflows u64")?;
        region
            .guest_phys_addr
            .checked_add(region.size)
            .context("guest-memory GPA range overflows u64")?;
    }
    let mut hvas = regions.to_vec();
    hvas.sort_by_key(|region| region.base_host_virt_addr);
    for pair in hvas.windows(2) {
        let end = pair[0]
            .base_host_virt_addr
            .checked_add(pair[0].size)
            .expect("region overflow was checked");
        ensure!(
            end <= pair[1].base_host_virt_addr,
            "Firecracker returned overlapping guest-memory HVA regions"
        );
    }
    let mut gpas = regions.to_vec();
    gpas.sort_by_key(|region| region.guest_phys_addr);
    for pair in gpas.windows(2) {
        let end = pair[0]
            .guest_phys_addr
            .checked_add(pair[0].size)
            .expect("region overflow was checked");
        ensure!(
            end <= pair[1].guest_phys_addr,
            "Firecracker returned overlapping guest-memory GPA regions"
        );
    }
    Ok(())
}

impl FirecrackerSnapshotManifest {
    pub fn new(
        vm_state_path: impl Into<PathBuf>,
        mem_image_config_path: impl Into<PathBuf>,
        mem_virtual_size: u64,
        rootfs_image_config_path: impl Into<PathBuf>,
        rootfs_virtual_size: u64,
        attached_drives: &[ExtraDrive],
    ) -> Result<Self> {
        Self {
            version: MANIFEST_FORMAT_VERSION,
            vm_state: FirecrackerVmStateArtifacts {
                path: vm_state_path.into(),
            },
            memory: FirecrackerMemoryArtifacts {
                image_config_path: mem_image_config_path.into(),
                virtual_size: mem_virtual_size,
                working_set: None,
            },
            rootfs: FirecrackerRootfsArtifacts {
                image_config_path: rootfs_image_config_path.into(),
                virtual_size: rootfs_virtual_size,
            },
            attached_drives: Vec::new(),
        }
        .with_extra_drives(attached_drives)
    }
    /// Attach a syntactically valid profiling result before the manifest is
    /// published. Region-specific validation is repeated against the restored
    /// VM immediately before pre-fault.
    pub fn with_working_set(&self, working_set: GuestMemoryWorkingSet) -> Result<Self> {
        working_set.validate_shape()?;
        let mut new = self.clone();
        new.memory.working_set = Some(working_set);
        Ok(new)
    }

    pub fn extra_drives(&self) -> Vec<ExtraDrive> {
        self.attached_drives
            .iter()
            .map(|drive| ExtraDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                image_config_path: drive.image_config_path.clone(),
                read_only: drive.read_only,
                virtual_size: Some(drive.virtual_size),
                mount_path: crate::sandbox::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| ExtraDrive::default_mount_path(&drive.drive_id)),
                sub_path: drive.sub_path.clone(),
            })
            .collect()
    }

    pub fn with_extra_drives(&self, extra_drives: &[ExtraDrive]) -> Result<Self> {
        let mut new = self.clone();
        new.attached_drives = extra_drives
            .iter()
            .map(|drive| {
                let virtual_size = drive.virtual_size().ok_or_else(|| {
                    anyhow::anyhow!(
                        "snapshot attached drive '{}' virtual size must be known",
                        drive.drive_id()
                    )
                })?;
                if virtual_size == 0 {
                    bail!(
                        "snapshot attached drive '{}' virtual size must be non-zero",
                        drive.drive_id()
                    );
                }
                Ok(FirecrackerAttachedDriveArtifacts {
                    drive_id: drive.drive_id().to_string(),
                    read_only: drive.read_only(),
                    mount_path: drive.mount_path().to_path_buf(),
                    sub_path: drive.sub_path().map(Path::to_path_buf),
                    virtual_size,
                    image_config_path: drive.image_config_path().to_path_buf(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(new)
    }
}

#[cfg(test)]
#[doc(hidden)]
impl FirecrackerSnapshotManifest {
    pub(crate) fn for_test(
        rootfs_virtual_size: u64,
        attached_drives: &[ExtraDrive],
    ) -> FirecrackerSnapshotManifest {
        let mut manifest = FirecrackerSnapshotManifest::new(
            "vm_state.bin",
            "mem_image.json",
            0,
            "rootfs/image.json",
            rootfs_virtual_size,
            attached_drives,
        )
        .expect("test snapshot attached drive virtual size must be known");

        for drive in &mut manifest.attached_drives {
            drive.image_config_path = PathBuf::from("drives")
                .join(&drive.drive_id)
                .join("image.json");
        }

        manifest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attached_drive_virtual_size_is_required() {
        let err = serde_json::from_value::<FirecrackerAttachedDriveArtifacts>(serde_json::json!({
            "driveId": "data",
            "readOnly": true,
            "mountPath": "/mnt/data"
        }))
        .expect_err("attached drive artifact should require virtualSize");

        assert!(err.to_string().contains("virtualSize"));
    }

    #[test]
    fn attached_drive_virtual_size_is_serialized_and_mapped_to_runtime_input() {
        let known = FirecrackerAttachedDriveArtifacts {
            drive_id: "data".to_string(),
            read_only: true,
            mount_path: PathBuf::from("/mnt/data"),
            sub_path: None,
            virtual_size: 4096,
            image_config_path: PathBuf::from("drives/data/image.json"),
        };

        let known_json = serde_json::to_value(&known).unwrap();
        assert_eq!(known_json["virtualSize"], serde_json::json!(4096));

        let manifest = FirecrackerSnapshotManifest {
            version: MANIFEST_FORMAT_VERSION,
            vm_state: FirecrackerVmStateArtifacts {
                path: PathBuf::from("vm_state.bin"),
            },
            memory: FirecrackerMemoryArtifacts {
                image_config_path: PathBuf::from("mem_image.json"),
                virtual_size: 4096,
                working_set: None,
            },
            rootfs: FirecrackerRootfsArtifacts {
                image_config_path: PathBuf::from("rootfs/image.json"),
                virtual_size: 4096,
            },
            attached_drives: vec![known],
        };

        let drives = manifest.extra_drives();
        assert_eq!(drives[0].virtual_size(), Some(4096));
    }

    #[test]
    fn new_rejects_attached_drive_without_virtual_size() {
        let drive = ExtraDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/tmp/data/image.json"),
            read_only: true,
            mount_path: ExtraDrive::default_mount_path("data"),
            virtual_size: None,
            sub_path: None,
        };

        let err = FirecrackerSnapshotManifest::new(
            "vm_state.bin",
            "mem_image.json",
            4096,
            "rootfs/image.json",
            4096,
            &[drive],
        )
        .expect_err("snapshot attached drive virtual size should be required");

        assert!(err.to_string().contains("virtual size must be known"));
    }

    #[test]
    fn with_extra_drives_rejects_zero_virtual_size() {
        let manifest = FirecrackerSnapshotManifest::new(
            "vm_state.bin",
            "mem_image.json",
            4096,
            "rootfs/image.json",
            4096,
            &[],
        )
        .expect("empty attached drives should be valid");
        let drive = ExtraDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/tmp/data/image.json"),
            read_only: true,
            mount_path: ExtraDrive::default_mount_path("data"),
            virtual_size: Some(0),
            sub_path: None,
        };

        let err = manifest
            .with_extra_drives(&[drive])
            .expect_err("snapshot attached drive virtual size should be non-zero");

        assert!(err.to_string().contains("virtual size must be non-zero"));
    }

    fn regions_with_mmio_hole() -> Vec<GuestMemoryRegion> {
        vec![
            GuestMemoryRegion {
                base_host_virt_addr: 0x1000_0000,
                guest_phys_addr: 0,
                size: 0x2000,
                page_size: GUEST_MEMORY_PAGE_SIZE,
            },
            GuestMemoryRegion {
                base_host_virt_addr: 0x2000_0000,
                guest_phys_addr: 0x1_0000_0000,
                size: 0x2000,
                page_size: GUEST_MEMORY_PAGE_SIZE,
            },
        ]
    }

    fn working_set_limits() -> GuestMemoryWorkingSetLimits {
        GuestMemoryWorkingSetLimits {
            max_bytes: 0x4000,
            max_ranges: 4,
            max_guest_memory_ratio_percent: 100,
        }
    }

    #[test]
    fn working_set_is_optional_and_old_manifests_remain_compatible() -> Result<()> {
        let manifest = FirecrackerSnapshotManifest::new(
            "vm_state.bin",
            "mem_image.json",
            0x4000,
            "rootfs/image.json",
            0x4000,
            &[],
        )?;
        let old_json = serde_json::to_value(&manifest)?;
        assert!(old_json["memory"].get("workingSet").is_none());
        let decoded: FirecrackerSnapshotManifest = serde_json::from_value(old_json)?;
        assert!(decoded.memory.working_set.is_none());

        let working_set = GuestMemoryWorkingSet::new(vec![GuestMemoryRange {
            gpa: 0,
            size: GUEST_MEMORY_PAGE_SIZE,
        }]);
        let with_working_set = manifest.with_working_set(working_set.clone())?;
        let json = serde_json::to_value(&with_working_set)?;
        assert_eq!(json["memory"]["workingSet"]["pageSize"], 4096);
        let round_trip: FirecrackerSnapshotManifest = serde_json::from_value(json)?;
        assert_eq!(round_trip.memory.working_set, Some(working_set));
        Ok(())
    }

    #[test]
    fn working_set_rejects_malformed_ranges() {
        for ranges in [
            vec![GuestMemoryRange { gpa: 1, size: 4096 }],
            vec![GuestMemoryRange { gpa: 0, size: 0 }],
            vec![GuestMemoryRange { gpa: 0, size: 4097 }],
            vec![GuestMemoryRange {
                gpa: u64::MAX - 4095,
                size: 4096,
            }],
            vec![
                GuestMemoryRange {
                    gpa: 0x1000,
                    size: 0x1000,
                },
                GuestMemoryRange {
                    gpa: 0,
                    size: 0x1000,
                },
            ],
            vec![
                GuestMemoryRange {
                    gpa: 0,
                    size: 0x1000,
                },
                GuestMemoryRange {
                    gpa: 0x1000,
                    size: 0x1000,
                },
            ],
        ] {
            assert!(GuestMemoryWorkingSet::new(ranges).validate_shape().is_err());
        }
    }

    #[test]
    fn working_set_allows_non_contiguous_ranges_when_they_are_not_coalescible() -> Result<()> {
        let working_set = GuestMemoryWorkingSet::new(vec![
            GuestMemoryRange {
                gpa: 0,
                size: GUEST_MEMORY_PAGE_SIZE,
            },
            // The one-page gap means these ranges are not GPA-contiguous and
            // therefore must remain separate rather than being rejected.
            GuestMemoryRange {
                gpa: 2 * GUEST_MEMORY_PAGE_SIZE,
                size: GUEST_MEMORY_PAGE_SIZE,
            },
        ]);
        let regions = [GuestMemoryRegion {
            base_host_virt_addr: 0x1000_0000,
            guest_phys_addr: 0,
            size: 3 * GUEST_MEMORY_PAGE_SIZE,
            page_size: GUEST_MEMORY_PAGE_SIZE,
        }];
        working_set.validate_shape()?;
        working_set.validate_for_regions(&regions, working_set_limits())?;
        Ok(())
    }

    #[test]
    fn working_set_accepts_empty_and_rejects_holes_and_budgets() -> Result<()> {
        let empty = GuestMemoryWorkingSet::new(vec![]);
        empty.validate_for_regions(&regions_with_mmio_hole(), working_set_limits())?;

        let in_hole = GuestMemoryWorkingSet::new(vec![GuestMemoryRange {
            gpa: 0x4000,
            size: 0x1000,
        }]);
        assert!(in_hole
            .validate_for_regions(&regions_with_mmio_hole(), working_set_limits())
            .is_err());

        let over_budget = GuestMemoryWorkingSet::new(vec![GuestMemoryRange {
            gpa: 0,
            size: 0x2000,
        }]);
        let limits = GuestMemoryWorkingSetLimits {
            max_bytes: 0x1000,
            ..working_set_limits()
        };
        assert!(over_budget
            .validate_for_regions(&regions_with_mmio_hole(), limits)
            .is_err());
        Ok(())
    }

    #[test]
    fn guest_memory_regions_reject_overlap() {
        let mut regions = regions_with_mmio_hole();
        regions[1].guest_phys_addr = 0x1000;
        assert!(validate_guest_memory_regions(&regions).is_err());
    }
}
