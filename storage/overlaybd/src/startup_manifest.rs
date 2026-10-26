//! Startup manifest: a tiny first-touch page manifest that carries WHERE
//! the hot pages are instead of the hot bytes themselves. Pages are merged
//! into address-contiguous ranges for compactness, and the range table is
//! sorted by each range's earliest first-touch rank — table position IS the
//! first-need order. The resume side translates the table into final-object
//! blocks, merges adjacent blocks into large runs, and delivers runs in
//! this order. Manifests with an unknown format version are ignored
//! (resume proceeds on-demand).
//!
//! Layout (little-endian):
//!   header (32B) | prefix pages | ranges
//! Header: magic "AENVMF01" | version u32 | header_bytes u32 |
//!         mem_virtual_size u64 | prefix_page_count u32 | range_count u32
//! Prefix pages: prefix_page_count × u64 page offsets delivered ahead of
//!         the range table (unused by current producers).
//! Ranges: range_count × (start u64, len u64), sorted by earliest
//!         first-touch rank.

use anyhow::{ensure, Context, Result};

pub const MANIFEST_MAGIC: [u8; 8] = *b"AENVMF01";
pub const MANIFEST_FORMAT_VERSION: u32 = 4;
pub const MANIFEST_HEADER_BYTES: usize = 32;
/// Sanity caps bounding parser allocations.
pub const MAX_PREFIX_PAGES: u32 = 1 << 20;
pub const MAX_RANGES: u32 = 1 << 20;

const PAGE_BYTES: u64 = crate::startup_pack::PACK_PAGE_BYTES;

/// A parsed startup manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartupManifest {
    pub mem_virtual_size: u64,
    /// Page offsets delivered ahead of the range table (empty for current
    /// producers).
    pub prefix_pages: Vec<u64>,
    /// Merged `(offset, len)` ranges sorted by earliest first-touch rank.
    pub ranges: Vec<(u64, u64)>,
}

fn validate_page_offset(mem_virtual_size: u64, offset: u64) -> Result<()> {
    ensure!(
        offset.is_multiple_of(PAGE_BYTES),
        "page offset {offset:#x} is not page-aligned"
    );
    let end = offset
        .checked_add(PAGE_BYTES)
        .context("page offset overflow")?;
    ensure!(
        end <= mem_virtual_size,
        "page offset {offset:#x} exceeds memory image size {mem_virtual_size:#x}"
    );
    Ok(())
}

fn validate_ranges(mem_virtual_size: u64, ranges: &[(u64, u64)]) -> Result<()> {
    let mut sorted = ranges.to_vec();
    sorted.sort_unstable();
    let mut last_end = 0u64;
    for (start, len) in sorted {
        ensure!(start >= last_end, "ranges overlap");
        ensure!(
            len > 0 && len.is_multiple_of(PAGE_BYTES),
            "range length {len:#x} is not page-aligned"
        );
        validate_page_offset(mem_virtual_size, start)?;
        let end = start.checked_add(len).context("range overflow")?;
        ensure!(
            end <= mem_virtual_size,
            "range {start:#x}+{len:#x} exceeds memory image size"
        );
        last_end = end;
    }
    Ok(())
}

/// Build a manifest from the first-touch page offsets (deduplicated, in
/// order): pages merge into address-contiguous ranges, and the range table
/// is sorted by each range's earliest first-touch rank, so table position
/// carries the first-need order for every range.
pub fn build_manifest(mem_virtual_size: u64, page_offsets: &[u64]) -> Result<StartupManifest> {
    ensure!(!page_offsets.is_empty(), "page list is empty");
    for &offset in page_offsets {
        validate_page_offset(mem_virtual_size, offset)?;
    }
    // Rank = position in the first-touch list; pages are already deduped.
    let mut ranked: Vec<(u64, u64)> = page_offsets
        .iter()
        .enumerate()
        .map(|(rank, &offset)| (offset, rank as u64))
        .collect();
    ranked.sort_unstable_by_key(|(offset, _)| *offset);
    let mut merged: Vec<(u64, u64, u64)> = Vec::new();
    for (offset, rank) in ranked {
        match merged.last_mut() {
            Some((start, len, r)) if *start + *len == offset => {
                *len += PAGE_BYTES;
                *r = (*r).min(rank);
            }
            _ => merged.push((offset, PAGE_BYTES, rank)),
        }
    }
    merged.sort_by_key(|(_, _, rank)| *rank);
    Ok(StartupManifest {
        mem_virtual_size,
        prefix_pages: Vec::new(),
        ranges: merged
            .into_iter()
            .map(|(start, len, _)| (start, len))
            .collect(),
    })
}

/// Encode a manifest (header + prefix pages + ranges).
pub fn encode_manifest(manifest: &StartupManifest) -> Result<Vec<u8>> {
    ensure!(
        manifest.prefix_pages.len() <= MAX_PREFIX_PAGES as usize,
        "too many prefix pages"
    );
    ensure!(
        manifest.ranges.len() <= MAX_RANGES as usize,
        "too many ranges"
    );
    for &offset in &manifest.prefix_pages {
        validate_page_offset(manifest.mem_virtual_size, offset)?;
    }
    validate_ranges(manifest.mem_virtual_size, &manifest.ranges)?;

    let mut out = Vec::with_capacity(
        MANIFEST_HEADER_BYTES + manifest.prefix_pages.len() * 8 + manifest.ranges.len() * 16,
    );
    out.extend_from_slice(&MANIFEST_MAGIC);
    out.extend_from_slice(&MANIFEST_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&(MANIFEST_HEADER_BYTES as u32).to_le_bytes());
    out.extend_from_slice(&manifest.mem_virtual_size.to_le_bytes());
    out.extend_from_slice(&(manifest.prefix_pages.len() as u32).to_le_bytes());
    out.extend_from_slice(&(manifest.ranges.len() as u32).to_le_bytes());
    for &offset in &manifest.prefix_pages {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    for &(start, len) in &manifest.ranges {
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    Ok(out)
}

/// Parse and validate a manifest.
pub fn decode_manifest(bytes: &[u8]) -> Result<StartupManifest> {
    ensure!(
        bytes.len() >= MANIFEST_HEADER_BYTES,
        "manifest too short: {} bytes",
        bytes.len()
    );
    ensure!(bytes[..8] == MANIFEST_MAGIC, "manifest magic mismatch");
    let version = u32::from_le_bytes(bytes[8..12].try_into()?);
    ensure!(
        version == MANIFEST_FORMAT_VERSION,
        "unsupported manifest version {version}"
    );
    let header_bytes = u32::from_le_bytes(bytes[12..16].try_into()?);
    ensure!(
        header_bytes as usize == MANIFEST_HEADER_BYTES,
        "unexpected manifest header size {header_bytes}"
    );
    let mem_virtual_size = u64::from_le_bytes(bytes[16..24].try_into()?);
    let prefix_page_count = u32::from_le_bytes(bytes[24..28].try_into()?);
    let range_count = u32::from_le_bytes(bytes[28..32].try_into()?);
    ensure!(
        prefix_page_count <= MAX_PREFIX_PAGES && range_count <= MAX_RANGES,
        "manifest counts out of range"
    );
    let expected = MANIFEST_HEADER_BYTES
        .checked_add(prefix_page_count as usize * 8)
        .and_then(|v| v.checked_add(range_count as usize * 16))
        .context("manifest length overflow")?;
    ensure!(
        bytes.len() == expected,
        "manifest length mismatch: expect {expected}, got {}",
        bytes.len()
    );

    let mut cursor = MANIFEST_HEADER_BYTES;
    let mut prefix_pages = Vec::with_capacity(prefix_page_count as usize);
    for _ in 0..prefix_page_count {
        let offset = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into()?);
        validate_page_offset(mem_virtual_size, offset)?;
        prefix_pages.push(offset);
        cursor += 8;
    }
    let mut ranges = Vec::with_capacity(range_count as usize);
    for _ in 0..range_count {
        let start = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into()?);
        let len = u64::from_le_bytes(bytes[cursor + 8..cursor + 16].try_into()?);
        ranges.push((start, len));
        cursor += 16;
    }
    validate_ranges(mem_virtual_size, &ranges)?;
    Ok(StartupManifest {
        mem_virtual_size,
        prefix_pages,
        ranges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_merges_by_address_and_orders_by_first_touch_rank() {
        // First-touch order: 0x8000, then a contiguous run 0x1000..0x4000,
        // then 0xA000. Address merging collapses the run; rank sorting puts
        // the earliest range (0x8000) first without address-sorting.
        let offsets = [0x8000u64, 0x1000, 0x2000, 0x3000, 0xA000];
        let manifest = build_manifest(1 << 30, &offsets).expect("build");
        assert!(manifest.prefix_pages.is_empty());
        assert_eq!(
            manifest.ranges,
            vec![(0x8000, 0x1000), (0x1000, 0x3000), (0xA000, 0x1000)]
        );
        let bytes = encode_manifest(&manifest).expect("encode");
        assert_eq!(decode_manifest(&bytes).expect("decode"), manifest);

        // A range whose pages are touched at scattered times carries the
        // earliest rank: page 0x2000 first, then 0x1000, then 0x3000 — the
        // merged run (0x1000..0x4000) leads the table.
        let scattered = [0x2000u64, 0x1000, 0x3000];
        let manifest = build_manifest(1 << 30, &scattered).expect("build");
        assert_eq!(manifest.ranges, vec![(0x1000, 0x3000)]);
    }

    #[test]
    fn manifest_validates() {
        // empty page list / misaligned / past image end
        assert!(build_manifest(1 << 30, &[]).is_err());
        assert!(build_manifest(1 << 30, &[0x1001]).is_err());
        assert!(build_manifest(0x2000, &[0x2000]).is_err());
        let manifest = build_manifest(1 << 30, &[0x8000u64, 0x1000]).unwrap();
        let bytes = encode_manifest(&manifest).unwrap();
        // bad magic
        let mut bad = bytes.clone();
        bad[0] = b'X';
        assert!(decode_manifest(&bad).is_err());
        // unknown version
        let mut bad = bytes.clone();
        bad[8] = 2;
        assert!(decode_manifest(&bad).is_err());
        // truncated
        assert!(decode_manifest(&bytes[..bytes.len() - 1]).is_err());
        // trailing garbage
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(decode_manifest(&bad).is_err());
        // tampered range count
        let mut bad = bytes.clone();
        bad[28..32].copy_from_slice(&9u32.to_le_bytes());
        assert!(decode_manifest(&bad).is_err());
        // overlapping ranges are rejected even though the table is not
        // address-sorted
        let overlapping = StartupManifest {
            mem_virtual_size: 1 << 30,
            prefix_pages: vec![],
            ranges: vec![(0x2000, 0x2000), (0x1000, 0x2000)],
        };
        assert!(encode_manifest(&overlapping).is_err());
    }
}
