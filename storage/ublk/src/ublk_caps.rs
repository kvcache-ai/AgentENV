//! ublk capability constants.
//!
//! ublk control commands such as GET_FEATURES are submitted through io_uring
//! on `/dev/ublk-control`; see `UVMUblkCtrl::get_features`.

/// io_uring command to update the virtual size of a ublk device.
///
/// Linux uapi `include/uapi/linux/ublk_cmd.h` defines this as
/// `_IOWR('u', 0x15, struct ublksrv_ctrl_cmd)`.
pub const UBLK_U_CMD_UPDATE_SIZE: u32 = (3 << 30)
    | ((std::mem::size_of::<ublk_sys::ublksrv_ctrl_cmd>() as u32) << 16)
    | (('u' as u32) << 8)
    | 0x15;

/// Feature flag indicating support for dynamic size updates.
///
/// Linux uapi `include/uapi/linux/ublk_cmd.h` defines this as `(1ULL << 10)`.
/// Bit 0 is `UBLK_F_SUPPORT_ZERO_COPY`, so this must not use `0x0001`.
///
/// If this bit is set in the flags returned by `UVMUblkCtrl::get_features`,
/// the device supports `UBLK_U_CMD_UPDATE_SIZE`.
pub const UBLK_F_UPDATE_SIZE: u64 = 1 << 10;

/// Feature flags that keep a device across its ublk server exiting: I/O
/// queues until a new server attaches with user recovery, and outstanding
/// I/O is reissued to it.
///
/// A recoverable device needs both bits in `UVMUblkCtrl::get_features`.
pub const USER_RECOVERY_FLAGS: u64 =
    (ublk_sys::UBLK_F_USER_RECOVERY | ublk_sys::UBLK_F_USER_RECOVERY_REISSUE) as u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ioctl_constants_are_nonzero() {
        assert_ne!(UBLK_U_CMD_UPDATE_SIZE, 0);
        assert_ne!(UBLK_F_UPDATE_SIZE, 0);
    }

    #[test]
    fn update_size_command_matches_kernel_encoding() {
        assert_eq!(UBLK_U_CMD_UPDATE_SIZE, 0xC020_7515);
    }

    #[test]
    fn feature_flag_matches_kernel_bit() {
        assert_eq!(UBLK_F_UPDATE_SIZE, 1 << 10);
    }

    #[test]
    fn user_recovery_flags_match_kernel_bits() {
        assert_eq!(USER_RECOVERY_FLAGS, (1 << 3) | (1 << 4));
    }
}
