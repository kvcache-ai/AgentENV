use crate::lsmt::index::SegmentMapping;
use std::fmt;
use uuid::Uuid;
use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// A zero mapping without physical backing. Physical offsets are stored in
/// sectors using 55 bits; reserve the all-ones value in memory and on disk.
pub const NO_PHYSICAL_OFFSET: u64 = DiskSegmentMapping::MOFFSET_MASK;

#[repr(C, packed)]
#[derive(Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout, Unaligned, Default)]
pub struct DiskSegmentMapping {
    pub data_low: U64,
    pub data_high: U64,
}

impl DiskSegmentMapping {
    const OFFSET_MASK: u64 = (1 << 50) - 1;
    const LENGTH_MASK: u64 = (1 << 14) - 1;

    const MOFFSET_MASK: u64 = (1 << 55) - 1;
    const ZEROED_MASK: u64 = 1;
    const TAG_MASK: u64 = 0xFF; // 8 bits

    pub fn from_memory(m: &SegmentMapping) -> Self {
        let offset = m.offset() & Self::OFFSET_MASK;
        let length = (m.length() as u64) & Self::LENGTH_MASK;

        // Low: offset(0..50) | length(50..64)
        let low = offset | (length << 50);

        let moffset = m.moffset & Self::MOFFSET_MASK;
        let zeroed = if m.zeroed { 1u64 } else { 0u64 };
        let tag = (m.tag as u64) & Self::TAG_MASK;

        // High: moffset(0..55) | zeroed(55..56) | tag(56..64)
        let high = moffset | (zeroed << 55) | (tag << 56);

        Self {
            data_low: U64::new(low),
            data_high: U64::new(high),
        }
    }

    pub fn to_memory(&self) -> SegmentMapping {
        let low = self.data_low.get();
        let high = self.data_high.get();

        let offset = low & Self::OFFSET_MASK;
        let length = (low >> 50) & Self::LENGTH_MASK;

        let moffset = high & Self::MOFFSET_MASK;
        let zeroed = ((high >> 55) & Self::ZEROED_MASK) != 0;
        let tag = (high >> 56) as u8;

        SegmentMapping::new(offset, length as u32, moffset, zeroed, tag)
    }
}

impl fmt::Debug for DiskSegmentMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let m = self.to_memory();
        write!(
            f,
            "DiskMapping {{ offset: {}, len: {}, moffset: {}, zeroed: {}, tag: {} }}",
            m.offset(),
            m.length(),
            m.moffset,
            m.zeroed,
            m.tag
        )
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout, Unaligned)]
pub struct HeaderTrailer {
    /// Magic Number: "LSMT\0\1\2"
    pub magic0: U64,
    /// Second Magic (UUID bytes)
    pub magic1: [u8; 16],
    /// sizeof(HeaderTrailer)
    pub size: U32,
    /// Flags (Header/Trailer, Data/Index, Sealed, Sparse, Hybrid)
    pub flags: U32,

    pub index_offset: U64,
    pub index_size: U64,
    pub virtual_size: U64,

    pub uuid: [u8; 37],
    pub parent_uuid: [u8; 37],

    pub reserved: U16,
    pub version: u8,
    pub sub_version: u8,

    pub user_tag: [u8; 256],
}

const _: () = assert!(std::mem::size_of::<HeaderTrailer>() == 390);

impl Default for HeaderTrailer {
    fn default() -> Self {
        Self {
            magic0: U64::default(),
            magic1: [0; 16],
            size: U32::new(std::mem::size_of::<HeaderTrailer>() as u32),
            flags: U32::default(),
            index_offset: U64::default(),
            index_size: U64::default(),
            virtual_size: U64::default(),
            uuid: [0; 37],
            parent_uuid: [0; 37],
            reserved: U16::default(),
            version: 1,
            sub_version: 1,
            user_tag: [0; 256],
        }
    }
}

impl HeaderTrailer {
    pub const SPACE: u64 = 4096;

    /// Magic 0: "LSMT\0\1\2" -> 0x00020100544d534c (Little Endian)
    pub const MAGIC0: u64 = 0x00020100544d534c;

    pub const MAGIC1: [u8; 16] = [
        0x65, 0x7e, 0x63, 0xd2, // d2637e65 (LE reversed?) -> C++ header says 0xd2637e65
        0x94, 0x44, // 4494
        0x08, 0x4c, // 4c08
        0xa2, 0xd2, // d2a2
        0xc8, 0xec, 0x4f, 0xcf, 0xae, 0x8a, // node
    ];

    // Flags Masks
    const FLAG_SHIFT_HEADER: u32 = 0;
    const FLAG_SHIFT_TYPE: u32 = 1; // 1:data, 0:index
    const FLAG_SHIFT_SEALED: u32 = 2; // 1:sealed
    const FLAG_SHIFT_SPARSE_RW: u32 = 4;
    const FLAG_SHIFT_HYBRID_RW: u32 = 5;

    pub fn new() -> Self {
        Self {
            magic0: U64::new(Self::MAGIC0),
            magic1: Self::MAGIC1,
            ..Self::default()
        }
    }

    pub fn verify_magic(&self) -> bool {
        self.magic0.get() == Self::MAGIC0 && self.magic1 == Self::MAGIC1
    }

    fn flags_val(&self) -> u32 {
        self.flags.get()
    }

    fn update_flags<F>(&mut self, f: F)
    where
        F: FnOnce(u32) -> u32,
    {
        let val = self.flags.get();
        self.flags.set(f(val));
    }

    pub fn is_header(&self) -> bool {
        (self.flags_val() & (1 << Self::FLAG_SHIFT_HEADER)) != 0
    }

    pub fn is_trailer(&self) -> bool {
        !self.is_header()
    }

    pub fn is_data_file(&self) -> bool {
        (self.flags_val() & (1 << Self::FLAG_SHIFT_TYPE)) != 0
    }

    pub fn is_sealed(&self) -> bool {
        (self.flags_val() & (1 << Self::FLAG_SHIFT_SEALED)) != 0
    }

    pub fn is_sparse_rw(&self) -> bool {
        (self.flags_val() & (1 << Self::FLAG_SHIFT_SPARSE_RW)) != 0
    }

    pub fn is_hybrid_rw(&self) -> bool {
        (self.flags_val() & (1 << Self::FLAG_SHIFT_HYBRID_RW)) != 0
    }
    pub fn set_trailer(&mut self) {
        self.flags.set(self.flags.get() & !1);
    }

    pub fn set_header(&mut self) {
        self.update_flags(|f| f | (1 << Self::FLAG_SHIFT_HEADER));
    }

    pub fn set_data_file(&mut self) {
        self.update_flags(|f| f | (1 << Self::FLAG_SHIFT_TYPE));
    }

    pub fn set_index_file(&mut self) {
        self.update_flags(|f| f & !(1 << Self::FLAG_SHIFT_TYPE));
    }

    pub fn set_unsealed_index_offset(&mut self) {
        self.index_offset.set(Self::SPACE);
    }

    pub fn set_sealed(&mut self) {
        self.update_flags(|f| f | (1 << Self::FLAG_SHIFT_SEALED));
    }

    pub fn set_sparse_rw(&mut self) {
        self.update_flags(|f| f | (1 << Self::FLAG_SHIFT_SPARSE_RW));
    }

    pub fn set_hybrid_rw(&mut self) {
        self.update_flags(|f| f | (1 << Self::FLAG_SHIFT_HYBRID_RW));
    }

    pub fn set_uuid(&mut self, uuid: &Uuid) {
        let s = uuid.hyphenated().to_string();
        let bytes = s.as_bytes();
        self.uuid[..36].copy_from_slice(bytes);
        self.uuid[36] = 0;
    }
}

impl fmt::Debug for HeaderTrailer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let magic0 = self.magic0.get();
        let size = self.size.get();
        let flags = self.flags.get();
        let index_offset = self.index_offset.get();
        let index_size = self.index_size.get();
        let virtual_size = self.virtual_size.get();

        f.debug_struct("HeaderTrailer")
            .field("magic0", &format_args!("{:#x}", magic0))
            .field("size", &size)
            .field("flags", &format_args!("{:#b}", flags))
            .field("index_offset", &index_offset)
            .field("index_size", &index_size)
            .field("virtual_size", &virtual_size)
            .field("is_header", &self.is_header())
            .field("is_sealed", &self.is_sealed())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapping_physical_offset_roundtrip() {
        assert_eq!(std::mem::size_of::<DiskSegmentMapping>(), 16);
        for mapping in [
            SegmentMapping::new(2048, 8, 16384, false, 7),
            SegmentMapping::new(2048, 8, 16384, true, 7),
            SegmentMapping::new(2048, 8, NO_PHYSICAL_OFFSET, true, 7),
            SegmentMapping::new(0, 1, NO_PHYSICAL_OFFSET - 1, false, 0),
        ] {
            let disk = DiskSegmentMapping::from_memory(&mapping);
            assert_eq!(disk.to_memory(), mapping);
        }
    }

    #[test]
    fn test_header_layout() {
        assert_eq!(std::mem::size_of::<HeaderTrailer>(), 390);
        assert_eq!(std::mem::align_of::<HeaderTrailer>(), 1);
    }

    #[test]
    fn test_magic() {
        let h = HeaderTrailer::new();

        assert_eq!(h.magic0.get(), 0x00020100544d534c);

        assert_eq!(h.magic1, HeaderTrailer::MAGIC1);
    }

    #[test]
    fn test_header_trailer_default_values() {
        let h = HeaderTrailer::new();
        assert_eq!(h.version, 1);
        assert_eq!(h.index_size.get(), 0);
        assert_eq!(h.virtual_size.get(), 0);
    }
}
