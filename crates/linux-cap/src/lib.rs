use std::fs;
use std::io;

pub const CAP_NET_ADMIN: i32 = 12;
pub const CAP_NET_RAW: i32 = 13;
pub const CAP_SYS_ADMIN: i32 = 21;

const PR_CAP_AMBIENT: libc::c_int = 47;
const PR_CAP_AMBIENT_RAISE: libc::c_ulong = 2;
const PR_CAP_AMBIENT_CLEAR_ALL: libc::c_ulong = 4;
const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;
const CAPABILITY_SET_COUNT: usize = 2;
const CAPABILITIES_PER_SET: usize = u32::BITS as usize;
const MAX_CAPABILITY: usize = CAPABILITY_SET_COUNT * CAPABILITIES_PER_SET;

#[repr(C)]
struct CapabilityHeader {
    version: u32,
    pid: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CapabilityData {
    effective: u32,
    permitted: u32,
    inheritable: u32,
}

fn invalid_capability() -> io::Error {
    io::Error::from_raw_os_error(libc::EINVAL)
}

fn capability_masks(capabilities: &[i32]) -> io::Result<[u32; CAPABILITY_SET_COUNT]> {
    let mut masks = [0u32; CAPABILITY_SET_COUNT];
    for capability in capabilities {
        let capability = usize::try_from(*capability).map_err(|_| invalid_capability())?;
        if capability >= MAX_CAPABILITY {
            return Err(invalid_capability());
        }
        masks[capability / CAPABILITIES_PER_SET] |= 1u32 << (capability % CAPABILITIES_PER_SET);
    }
    Ok(masks)
}

fn capability_mask(capability: i32) -> io::Result<u64> {
    let capability = usize::try_from(capability).map_err(|_| invalid_capability())?;
    if capability >= MAX_CAPABILITY {
        return Err(invalid_capability());
    }
    Ok(1u64 << capability)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapabilitySets {
    inheritable: u64,
    permitted: u64,
    effective: u64,
}

impl CapabilitySets {
    pub fn from_proc_status(status: &str) -> io::Result<Self> {
        fn field(status: &str, name: &str) -> io::Result<u64> {
            let value = status
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{name} is missing from /proc/self/status"),
                    )
                })?;
            u64::from_str_radix(value.trim(), 16)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
        }

        Ok(Self {
            inheritable: field(status, "CapInh:")?,
            permitted: field(status, "CapPrm:")?,
            effective: field(status, "CapEff:")?,
        })
    }

    pub fn current() -> io::Result<Self> {
        Self::from_proc_status(&fs::read_to_string("/proc/self/status")?)
    }

    /// Return whether a capability can be delegated through the ambient set:
    /// it must be inheritable, permitted, and currently effective.
    pub fn is_delegable(self, capability: i32) -> io::Result<bool> {
        let mask = capability_mask(capability)?;
        Ok(
            self.inheritable & mask != 0
                && self.permitted & mask != 0
                && self.effective & mask != 0,
        )
    }

    pub fn effective(self, capability: i32) -> io::Result<bool> {
        Ok(self.effective & capability_mask(capability)? != 0)
    }
}

/// Return whether every capability is present in the inheritable, permitted,
/// and effective sets of the current process.
pub fn has_capabilities(capabilities: &[i32]) -> io::Result<bool> {
    let sets = CapabilitySets::current()?;
    capabilities.iter().try_fold(true, |present, capability| {
        sets.is_delegable(*capability)
            .map(|contains| present && contains)
    })
}

/// Return whether every capability is effective in the current process.
pub fn has_effective_capabilities(capabilities: &[i32]) -> io::Result<bool> {
    let sets = CapabilitySets::current()?;
    capabilities.iter().try_fold(true, |present, capability| {
        sets.effective(*capability)
            .map(|effective| present && effective)
    })
}

/// Clear all ambient capabilities from the current process.
pub fn clear_ambient_capabilities() -> io::Result<()> {
    // SAFETY: prctl is called with the documented PR_CAP_AMBIENT_CLEAR_ALL
    // operation and no pointer arguments.
    let rc = unsafe { libc::prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn set_capability_sets(capabilities: &[i32]) -> io::Result<()> {
    let masks = capability_masks(capabilities)?;
    let header = CapabilityHeader {
        version: LINUX_CAPABILITY_VERSION_3,
        pid: 0,
    };
    let data = masks.map(|mask| CapabilityData {
        effective: mask,
        permitted: mask,
        inheritable: mask,
    });
    // SAFETY: capset receives a version-3 header and the required two-element
    // data array, both valid for the duration of the syscall.
    let rc = unsafe { libc::syscall(libc::SYS_capset, &header, data.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn raise_ambient_capability(capability: i32) -> io::Result<()> {
    capability_mask(capability)?;
    // SAFETY: prctl is called with the documented PR_CAP_AMBIENT_RAISE
    // operation and a validated Linux capability number.
    let rc = unsafe {
        libc::prctl(
            PR_CAP_AMBIENT,
            PR_CAP_AMBIENT_RAISE,
            capability as libc::c_ulong,
            0,
            0,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Replace the current process capability sets with exactly `capabilities`
/// and raise those capabilities into the ambient set for the next `exec`.
/// The capability bounding set is intentionally left unchanged.
///
/// This function performs only validation and Linux syscalls, so callers may
/// use it from a `pre_exec` closure.
pub fn configure_current_process_capabilities(capabilities: &[i32]) -> io::Result<()> {
    capability_masks(capabilities)?;
    clear_ambient_capabilities()?;
    set_capability_sets(capabilities)?;
    for capability in capabilities {
        raise_ambient_capability(*capability)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_masks_for_both_capability_words() {
        assert_eq!(
            capability_masks(&[0, 31, 32, 63]).unwrap(),
            [0x8000_0001; 2]
        );
    }

    #[test]
    fn rejects_capabilities_outside_version_three_range() {
        for capability in [-1, 64] {
            assert_eq!(
                capability_masks(&[capability]).unwrap_err().raw_os_error(),
                Some(libc::EINVAL)
            );
        }
    }

    #[test]
    fn parses_and_queries_capability_sets() {
        let sets = CapabilitySets::from_proc_status(
            "CapInh:\t0000000000001000\nCapPrm:\t0000000000201000\nCapEff:\t0000000000201000\n",
        )
        .unwrap();
        assert!(sets.is_delegable(CAP_NET_ADMIN).unwrap());
        assert!(!sets.is_delegable(CAP_SYS_ADMIN).unwrap());
        assert!(sets.effective(CAP_SYS_ADMIN).unwrap());
    }
}
