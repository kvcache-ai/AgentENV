use serde::{Deserialize, Serialize};

/// File name of the per-snapshot memory prefetch artifact stored under
/// `artifacts/{snapshot_id}/` in the snapshot repository.
pub const MEMORY_PREFETCH_ARTIFACT: &str = "memory-prefetch.json";

/// Hard caps applied when consuming a prefetch manifest. The prefetch is
/// disabled for the snapshot when either cap is exceeded (with a warning).
pub const MAX_PREFETCH_RANGES: usize = 4096;
pub const MAX_PREFETCH_BYTES: u64 = 256 * 1024 * 1024;

/// Current manifest format version. Unknown versions must be ignored by
/// consumers (fall back to the plain demand-read path).
pub const MEMORY_PREFETCH_VERSION: u32 = 1;

/// Parse and validate a prefetch manifest body. Returns the GPA ranges when
/// the manifest is usable, or `None` when it is malformed, has an unknown
/// version, is empty, or exceeds the caps (any of which silently disables
/// the prefetch). Ranges are normalized (sorted and coalesced) before being
/// returned, so unsorted or overlapping input cannot cause duplicate reads.
pub fn parse_prefetch_manifest(bytes: &[u8]) -> Option<Vec<(u64, u64)>> {
    let file: MemoryPrefetchFile = serde_json::from_slice(bytes).ok()?;
    if file.version != MEMORY_PREFETCH_VERSION || file.ranges.is_empty() {
        return None;
    }
    if file.ranges.len() > MAX_PREFETCH_RANGES {
        return None;
    }
    // Reject ranges that would overflow `start + len` downstream
    // (e.g. corrupt `start` near u64::MAX).
    if file
        .ranges
        .iter()
        .any(|(start, len)| start.checked_add(*len).is_none())
    {
        return None;
    }
    let total_bytes = file
        .ranges
        .iter()
        .try_fold(0u64, |acc, (_, len)| acc.checked_add(*len))?;
    if total_bytes > MAX_PREFETCH_BYTES {
        return None;
    }
    Some(normalize_ranges(file.ranges))
}

/// Sort ranges by start and coalesce overlapping or adjacent ones, so the
/// consumer always sees sorted, non-overlapping ranges regardless of the
/// order the producer emitted them in.
fn normalize_ranges(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (start, len) in ranges {
        if len == 0 {
            continue;
        }
        if let Some((last_start, last_len)) = out.last_mut() {
            let last_end = *last_start + *last_len;
            if start <= last_end {
                *last_len = (*last_len).max(start + len - *last_start);
                continue;
            }
        }
        out.push((start, len));
    }
    out
}

/// Pages of a specific guest process (envd) that were resident at capture
/// time, as guest-physical address ranges. Used to bulk-prefetch those pages
/// into the remote-block cache before the guest resumes.
///
/// Note: the ranges are guest *physical* addresses, which equal the memory
/// image's virtual offsets only for single-region Firecracker guests (the
/// default mem size, up to 3328 MiB). Larger guests have a second region at
/// GPA 64 GiB that is laid out sequentially in the memory file, so pages
/// there would warm wrong offsets — harmless for this best-effort prefetch,
/// but worth knowing if the memory layout ever changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryPrefetchFile {
    pub version: u32,
    /// (gpa_start, len), page-aligned, sorted, non-overlapping.
    pub ranges: Vec<(u64, u64)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefetch_file_roundtrip() {
        let file = MemoryPrefetchFile {
            version: MEMORY_PREFETCH_VERSION,
            ranges: vec![(4096, 8192)],
        };
        let s = serde_json::to_string(&file).unwrap();
        let back: MemoryPrefetchFile = serde_json::from_str(&s).unwrap();
        assert_eq!(back.version, MEMORY_PREFETCH_VERSION);
        assert_eq!(back.ranges, vec![(4096, 8192)]);
    }

    #[test]
    fn parse_prefetch_manifest_normalizes_unsorted_overlapping_ranges() {
        let bytes = br#"{"version":1,"ranges":[[8192,4096],[4096,8192],[12288,4096],[0,0]]}"#;
        assert_eq!(parse_prefetch_manifest(bytes), Some(vec![(4096, 12288)]));
    }

    #[test]
    fn parse_prefetch_manifest_accepts_valid_manifest() {
        let bytes = br#"{"version":1,"ranges":[[4096,8192],[16384,4096]]}"#;
        assert_eq!(
            parse_prefetch_manifest(bytes),
            Some(vec![(4096, 8192), (16384, 4096)])
        );
    }

    #[test]
    fn parse_prefetch_manifest_rejects_bad_inputs() {
        // malformed JSON
        assert!(parse_prefetch_manifest(b"not json").is_none());
        // unknown version
        assert!(parse_prefetch_manifest(br#"{"version":99,"ranges":[[4096,4096]]}"#).is_none());
        // empty ranges
        assert!(parse_prefetch_manifest(br#"{"version":1,"ranges":[]}"#).is_none());
        // too many ranges
        let too_many = MemoryPrefetchFile {
            version: MEMORY_PREFETCH_VERSION,
            ranges: (0..=MAX_PREFETCH_RANGES as u64)
                .map(|i| (i * 4096, 4096))
                .collect(),
        };
        let bytes = serde_json::to_vec(&too_many).unwrap();
        assert!(parse_prefetch_manifest(&bytes).is_none());
        // total bytes overflow / over cap
        assert!(parse_prefetch_manifest(
            br#"{"version":1,"ranges":[[4096,18446744073709551615]]}"#
        )
        .is_none());
        // start + len overflow
        assert!(parse_prefetch_manifest(
            br#"{"version":1,"ranges":[[18446744073709547520,8192]]}"#
        )
        .is_none());
        assert!(parse_prefetch_manifest(
            br#"{"version":1,"ranges":[[4096,268435457],[268439552,4096]]}"#
        )
        .is_none());
    }
}
