//! Whether the filesystem's notion of "unwritten" is trustworthy.

/// Whether this platform guarantees that regions of a file which were never
/// written are reported as holes by `seek_data` / `seek_hole`.
///
/// Code that reconstructs "which blocks were written" from a file's extent map
/// — see `lsmt::file::create_mappings_from_sparse` — depends on this. Getting it
/// wrong is not a performance problem: a file that reports its *entire* range as
/// data yields a mapping claiming every block belongs to the upper layer, which
/// masks the lower layers with zeros.
///
/// # Linux
///
/// True. A region with no allocated extent is a hole, mechanically.
///
/// # macOS / APFS
///
/// False. Sparseness on APFS is a filesystem heuristic that the application
/// cannot control — Apple's own description is that "APFS decides which are
/// created as sparse files, and that can't be directly manipulated by the app or
/// the user". Two measured consequences:
///
/// - There is a minimum file size below which APFS does not bother: the first
///   write collapses the file to fully allocated, after which an extent scan
///   reports one extent spanning the whole file. Measured on this Apple silicon
///   host the threshold sits between 16 MiB and 24 MiB; published reports put it
///   at 8 KiB on one Intel machine and 16 MiB on an M1, so it is neither
///   documented nor portable.
/// - Holes must be produced by seeking past them. Writing zeros does not create
///   one, so "unwritten" and "reads as zero" are not the same thing.
///
/// `F_PUNCHHOLE` does reliably create reportable holes (that part is
/// documented and measured), so this is specifically about inferring history
/// from an extent map, not about hole punching.
pub fn sparse_extents_are_reliable() -> bool {
    cfg!(target_os = "linux")
}
