//! Shared constants and the first-touch trace codec of the startup memory
//! domain. The trace is the ordered, deduplicated page-offset list the
//! recorder observes; the manifest publisher expands it into the uploaded
//! page list, and the manifest prefetch plans it onto the final layer
//! objects. It never leaves the node as-is.

use anyhow::{ensure, Context, Result};

/// Guest page granularity of the recorded first-touch list.
pub const PACK_PAGE_BYTES: u64 = 4096;
/// Recorder page cap: 1 GiB of covered memory.
pub const MAX_PACK_PAGES: u32 = 262_144;

// ── First-touch trace ───────────────────────────────────────────────────────

pub const TRACE_MAGIC: [u8; 8] = *b"AENVTRC1";
pub const TRACE_FORMAT_VERSION: u32 = 1;
pub const TRACE_HEADER_BYTES: usize = 32;
/// Trace page-list cap (bounds parser allocation): 8 MiB of index bytes.
pub const MAX_TRACE_PAGES: u32 = 1 << 20;

const TRACE_OFF_VERSION: usize = 8;
const TRACE_OFF_HEADER_BYTES: usize = 12;
const TRACE_OFF_MEM_VSIZE: usize = 16;
const TRACE_OFF_PAGE_COUNT: usize = 24;
const TRACE_OFF_RESERVED: usize = 28;

/// Encode a first-touch trace: the ordered, deduplicated page offsets the
/// recorder observed, plus the memory image size they belong to.
pub fn encode_trace(mem_virtual_size: u64, page_offsets: &[u64]) -> Result<Vec<u8>> {
    ensure!(!page_offsets.is_empty(), "trace page list is empty");
    ensure!(
        page_offsets.len() <= MAX_TRACE_PAGES as usize,
        "trace page count {} out of range",
        page_offsets.len()
    );
    let mut out = Vec::with_capacity(TRACE_HEADER_BYTES + page_offsets.len() * 8);
    out.extend_from_slice(&TRACE_MAGIC);
    out.extend_from_slice(&TRACE_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&(TRACE_HEADER_BYTES as u32).to_le_bytes());
    out.extend_from_slice(&mem_virtual_size.to_le_bytes());
    out.extend_from_slice(&(page_offsets.len() as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    for &offset in page_offsets {
        validate_trace_offset(mem_virtual_size, offset)?;
        out.extend_from_slice(&offset.to_le_bytes());
    }
    Ok(out)
}

fn validate_trace_offset(mem_virtual_size: u64, offset: u64) -> Result<()> {
    ensure!(
        offset.is_multiple_of(PACK_PAGE_BYTES),
        "trace offset {offset:#x} is not page-aligned"
    );
    let end = offset
        .checked_add(PACK_PAGE_BYTES)
        .context("trace offset overflows u64")?;
    ensure!(
        end <= mem_virtual_size,
        "trace offset {offset:#x} exceeds memory image size {mem_virtual_size:#x}"
    );
    Ok(())
}

/// Decode and validate a trace. Returns the memory image size and the
/// ordered page offsets.
pub fn decode_trace(bytes: &[u8]) -> Result<(u64, Vec<u64>)> {
    ensure!(
        bytes.len() >= TRACE_HEADER_BYTES,
        "trace too short: {} bytes",
        bytes.len()
    );
    ensure!(bytes[..8] == TRACE_MAGIC, "trace magic mismatch");
    let version = u32::from_le_bytes(
        bytes[TRACE_OFF_VERSION..TRACE_OFF_VERSION + 4]
            .try_into()
            .expect("fixed-size slice"),
    );
    ensure!(
        version == TRACE_FORMAT_VERSION,
        "unsupported trace version {version}"
    );
    let header_bytes = u32::from_le_bytes(
        bytes[TRACE_OFF_HEADER_BYTES..TRACE_OFF_HEADER_BYTES + 4]
            .try_into()
            .expect("fixed-size slice"),
    );
    ensure!(
        header_bytes as usize == TRACE_HEADER_BYTES,
        "unexpected trace header size {header_bytes}"
    );
    let mem_virtual_size = u64::from_le_bytes(
        bytes[TRACE_OFF_MEM_VSIZE..TRACE_OFF_MEM_VSIZE + 8]
            .try_into()
            .expect("fixed-size slice"),
    );
    let page_count = u32::from_le_bytes(
        bytes[TRACE_OFF_PAGE_COUNT..TRACE_OFF_PAGE_COUNT + 4]
            .try_into()
            .expect("fixed-size slice"),
    );
    ensure!(
        bytes[TRACE_OFF_RESERVED..TRACE_HEADER_BYTES]
            .iter()
            .all(|b| *b == 0),
        "trace reserved header bytes are non-zero"
    );
    ensure!(
        page_count > 0 && page_count <= MAX_TRACE_PAGES,
        "trace page count {page_count} out of range"
    );
    let expected = TRACE_HEADER_BYTES + page_count as usize * 8;
    ensure!(
        bytes.len() == expected,
        "trace length mismatch: expect {expected}, got {}",
        bytes.len()
    );
    let mut offsets = Vec::with_capacity(page_count as usize);
    for chunk in bytes[TRACE_HEADER_BYTES..].as_chunks::<8>().0 {
        let offset = u64::from_le_bytes(*chunk);
        validate_trace_offset(mem_virtual_size, offset)?;
        offsets.push(offset);
    }
    Ok((mem_virtual_size, offsets))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_roundtrip_and_validation() {
        let offsets = vec![0u64, 3 * PACK_PAGE_BYTES, 7 * PACK_PAGE_BYTES];
        let bytes = encode_trace(1 << 30, &offsets).expect("encode");
        let (mem_virtual_size, decoded) = decode_trace(&bytes).expect("decode");
        assert_eq!(mem_virtual_size, 1 << 30);
        assert_eq!(decoded, offsets);

        // Unaligned offset is rejected.
        assert!(encode_trace(1 << 30, &[123]).is_err());
        // Out-of-range offset is rejected.
        assert!(encode_trace(1 << 20, &[(1 << 20) - PACK_PAGE_BYTES + 1]).is_err());
        // Truncated body is rejected.
        assert!(decode_trace(&bytes[..bytes.len() - 8]).is_err());
        // Bad magic is rejected.
        let mut bad = bytes.clone();
        bad[0] ^= 0xFF;
        assert!(decode_trace(&bad).is_err());
    }
}
