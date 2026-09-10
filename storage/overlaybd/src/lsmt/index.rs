use anyhow::{bail, Result};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;

use super::format::NO_PHYSICAL_OFFSET;

/// Represents a range in the virtual file
/// The unit of `offset` and `length` are sector/block (i.e., 512B)
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct Segment {
    /// in units of ALIGNMENT
    pub offset: u64,
    /// in units of ALIGNMENT
    pub length: u32,
}

impl Segment {
    pub const MAX_LENGTH: u32 = (1 << 14) - 1;

    pub fn new(offset: u64, length: u32) -> Self {
        Self { offset, length }
    }

    pub fn end(&self) -> u64 {
        self.offset + self.length as u64
    }

    pub fn forward_offset_to(&mut self, x: u64) -> u64 {
        if x <= self.offset {
            return 0;
        }
        let delta = x - self.offset;
        if delta >= self.length as u64 {
            self.length = 0;
            self.offset = x;
            delta
        } else {
            self.length -= delta as u32;
            self.offset = x;
            delta
        }
    }

    pub fn backward_end_to(&mut self, x: u64) {
        if x > self.offset {
            let new_len = x - self.offset;
            self.length = new_len as u32;
        } else {
            self.length = 0;
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub struct SegmentMapping {
    pub segment: Segment,
    /// Physical offset in sectors, or NO_PHYSICAL_OFFSET for an unbacked zero.
    /// A backed zero retains this range but still reads as zero.
    pub moffset: u64,
    pub zeroed: bool,
    pub tag: u8,
}

impl SegmentMapping {
    /// - `offset`: Offset with respect to virtual size, in units of ALIGNMENT
    /// - `length`: The size of this segement, in units of ALIGNMENT
    /// - `moffset`: The physical offset of this segement in corresponding layer file, in units of
    ///   ALIGNMENT
    /// - `tag`: An identifier of the layer that this segment locates at.
    pub fn new(offset: u64, length: u32, moffset: u64, zeroed: bool, tag: u8) -> Self {
        Self {
            segment: Segment::new(offset, length),
            moffset,
            zeroed,
            tag,
        }
    }

    pub fn offset(&self) -> u64 {
        self.segment.offset
    }
    pub fn length(&self) -> u32 {
        self.segment.length
    }
    pub fn end(&self) -> u64 {
        self.segment.end()
    }

    /// Whether this mapping retains a physical range. This does not imply
    /// that the range is readable data: `zeroed` controls read visibility.
    pub fn has_physical_range(&self) -> bool {
        self.moffset != NO_PHYSICAL_OFFSET
    }

    /// Physical end, or NO_PHYSICAL_OFFSET for an unbacked zero.
    pub fn mend(&self) -> u64 {
        if self.has_physical_range() {
            self.moffset + self.segment.length as u64
        } else {
            NO_PHYSICAL_OFFSET
        }
    }

    pub fn forward_offset_to(&mut self, x: u64) {
        let delta = self.segment.forward_offset_to(x);

        if self.has_physical_range() && delta > 0 {
            self.moffset += delta;
        }
    }

    pub fn backward_end_to(&mut self, x: u64) {
        self.segment.backward_end_to(x);
    }

    fn can_merge_with(&self, next: &Self) -> bool {
        self.end() == next.offset()
            && self.zeroed == next.zeroed
            && self.tag == next.tag
            && self.has_physical_range() == next.has_physical_range()
            && self.mend() == next.moffset
            && u64::from(self.length()) + u64::from(next.length()) <= u64::from(Segment::MAX_LENGTH)
    }
}

impl Ord for SegmentMapping {
    fn cmp(&self, other: &Self) -> Ordering {
        self.segment.offset.cmp(&other.segment.offset)
    }
}

impl PartialOrd for SegmentMapping {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn compress_raw_index(mapping: &mut Vec<SegmentMapping>) -> usize {
    let n = mapping.len();
    if n < 2 {
        return n;
    }

    let mut i = 0;
    for j in 1..n {
        let m_i = mapping[i];
        let m_j = mapping[j];

        if m_i.can_merge_with(&m_j) {
            mapping[i].segment.length += m_j.length();
        } else {
            i += 1;
            mapping[i] = m_j;
        }
    }

    let compressed_size = i + 1;
    mapping.truncate(compressed_size);
    compressed_size
}

pub fn compress_raw_index_predict(mapping: &[SegmentMapping]) -> usize {
    let n = mapping.len();
    if n < 2 {
        return n;
    }

    let mut i = 0;
    let mut m = mapping[0];

    for item in mapping.iter().take(n).skip(1) {
        if m.can_merge_with(item) {
            m.segment.length += item.length();
        } else {
            m = *item;
            i += 1;
        }
    }

    i + 1
}

pub trait LogIndex: Send + Sync {
    fn lookup(&self, segment: Segment, dst: &mut Vec<SegmentMapping>) -> usize;
}

#[derive(Debug, Default, Clone)]
pub struct ReadOnlyIndex {
    mappings: Vec<SegmentMapping>,
}

impl ReadOnlyIndex {
    pub fn new(mut mappings: Vec<SegmentMapping>) -> Self {
        // Lower layers never lend physical space to a writable upper. Legacy
        // zero offsets are arbitrary placeholders; normalize before lookup or
        // merge can interpret them as backed zeros. This constructor is also
        // used when loading a premerged index artifact. RW replay must not do
        // this, since it must preserve backed-zero offsets across reopen.
        for mapping in &mut mappings {
            if mapping.zeroed {
                mapping.moffset = NO_PHYSICAL_OFFSET;
            }
        }
        Self { mappings }
    }

    pub fn merge(indexes: &[&ReadOnlyIndex]) -> Self {
        let mut mi = MutableIndex::new();

        for (i, idx) in indexes.iter().enumerate().rev() {
            for m in &idx.mappings {
                let mut m_new = *m;
                // tag = 0 means the newest/top layer
                // The smaller the tag is, the newer the data is
                m_new.tag = i as u8;
                mi.insert(m_new);
            }
        }

        ReadOnlyIndex::new(mi.dump())
    }

    pub fn mappings(&self) -> &[SegmentMapping] {
        &self.mappings
    }
}

impl LogIndex for ReadOnlyIndex {
    fn lookup(&self, query: Segment, dst: &mut Vec<SegmentMapping>) -> usize {
        if query.length == 0 || self.mappings.is_empty() {
            return 0;
        }
        let end_offset = query.end();

        let idx = self.mappings.partition_point(|m| m.end() <= query.offset);

        let start_len = dst.len();

        for m in self.mappings.iter().skip(idx) {
            if m.offset() >= end_offset {
                break;
            }

            let mut mp = *m;
            if mp.offset() < query.offset {
                mp.forward_offset_to(query.offset);
            }
            if mp.end() > end_offset {
                mp.backward_end_to(end_offset);
            }

            if mp.length() > 0 {
                dst.push(mp);
            }
        }

        dst.len() - start_len
    }
}

pub trait BptConfig: Copy + Ord + Default + 'static {
    const KEYS_PER_NODE: usize;
    const MAX_LEVEL: usize;
    fn nodes_per_level() -> &'static [usize];
    fn level_start_id() -> &'static [usize];
    fn from_offset(offset: u64) -> Self;
    fn to_offset(self) -> u64;
}

impl BptConfig for u64 {
    const KEYS_PER_NODE: usize = 8;
    const MAX_LEVEL: usize = 10;

    fn nodes_per_level() -> &'static [usize] {
        &[
            8, 72, 648, 5832, 52488, 472392, 4251528, 38263752, 344373768, 3099363912,
        ]
    }
    fn level_start_id() -> &'static [usize] {
        &[
            0, 8, 80, 728, 6560, 59048, 531440, 4782968, 43046720, 387420488,
        ]
    }
    fn from_offset(offset: u64) -> Self {
        offset
    }
    fn to_offset(self) -> u64 {
        self
    }
}

impl BptConfig for u32 {
    const KEYS_PER_NODE: usize = 16;
    const MAX_LEVEL: usize = 7;

    fn nodes_per_level() -> &'static [usize] {
        &[16, 272, 4624, 78608, 1336336, 22717712, 386200304]
    }
    fn level_start_id() -> &'static [usize] {
        &[0, 16, 288, 4912, 83520, 1419856, 24137568]
    }
    fn from_offset(offset: u64) -> Self {
        offset as u32
    }
    fn to_offset(self) -> u64 {
        self as u64
    }
}

#[derive(Debug)]
pub struct LinearizedBptree<K: BptConfig> {
    node: Vec<K>,
    depth: i32,
    _marker: PhantomData<K>,
}

impl<K: BptConfig> Default for LinearizedBptree<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: BptConfig> LinearizedBptree<K> {
    pub fn new() -> Self {
        Self {
            node: Vec::new(),
            depth: -1,
            _marker: PhantomData,
        }
    }

    pub fn build(&mut self, mapping: &[SegmentMapping]) -> Result<()> {
        if mapping.is_empty() {
            bail!("empty mapping");
        }

        let mapping_size = mapping.len();
        self.depth = -1;

        let nodes_per_level = K::nodes_per_level();
        for (i, node_count) in nodes_per_level.iter().enumerate().take(K::MAX_LEVEL) {
            if *node_count >= mapping_size {
                self.depth = (i + 1) as i32;
                break;
            }
        }

        if self.depth == -1 {
            bail!("too many mappings");
        }

        let level_start_id = K::level_start_id();
        let depth_idx = (self.depth - 1) as usize;

        let n_raw = level_start_id[depth_idx] + mapping_size + K::KEYS_PER_NODE - 1;
        let n = (n_raw / K::KEYS_PER_NODE) * K::KEYS_PER_NODE;

        self.node = vec![K::from_offset(u64::MAX); n];

        let leaf_start = level_start_id[depth_idx];
        for (p, m) in (leaf_start..).zip(mapping.iter()) {
            self.node[p] = K::from_offset(m.offset());
        }

        let keys_per_node = K::KEYS_PER_NODE;
        let mut g = keys_per_node;
        let leaf_size = nodes_per_level[depth_idx]; // This is effectively capacity at this level

        for level in (1..self.depth).rev() {
            let lvl_idx = level as usize; // 1-based level to index
            let mut pos = level_start_id[lvl_idx - 1]; // Current level start

            let step = g * (keys_per_node + 1);

            let mut i = 0;
            while i < leaf_size {
                for j in 1..=keys_per_node {
                    let lower_id = leaf_start + i + g * j;
                    if lower_id < n {
                        self.node[pos] = self.node[lower_id];
                    } else {
                        self.node[pos] = K::from_offset(u64::MAX);
                    }
                    pos += 1;
                }
                i += step;
            }
            g *= keys_per_node + 1;
        }

        Ok(())
    }

    #[inline(always)]
    fn inner_search(base: &[K], x: K) -> usize {
        let mut mask = 0u32;
        for (i, item) in base.iter().enumerate().take(K::KEYS_PER_NODE) {
            if *item <= x {
                mask |= 1 << i;
            }
        }
        mask.count_ones() as usize
    }

    pub fn search(&self, x_val: u64) -> usize {
        if self.depth <= 0 {
            return 0;
        }
        let x = K::from_offset(x_val);
        let mut res = 0;
        let keys = K::KEYS_PER_NODE;
        let level_start = K::level_start_id();

        for _i in (2..=self.depth).rev() {
            let node_base = &self.node[res..];
            let c = Self::inner_search(node_base, x);
            // C++: res = (KEYS+1)*res + (c+1)*KEYS;
            res = (keys + 1) * res + (c + 1) * keys;
        }

        let node_base = &self.node[res..];
        res += Self::inner_search(node_base, x);

        if res > 0 {
            let depth_idx = (self.depth - 1) as usize;
            let start_id = level_start[depth_idx];
            if res > start_id {
                return res - start_id - 1;
            }
        }
        0
    }
}

pub struct IndexLBPT<K: BptConfig> {
    mappings: Vec<SegmentMapping>, // Ownership of mappings
    lbpt: LinearizedBptree<K>,
    virtual_size: u64,
}

impl<K: BptConfig> IndexLBPT<K> {
    pub fn new(mappings: Vec<SegmentMapping>, virtual_size: u64) -> Self {
        let mut lbpt = LinearizedBptree::new();
        lbpt.build(&mappings).expect("failed to build LBPT");

        Self {
            mappings,
            lbpt,
            virtual_size,
        }
    }
}

impl<K: BptConfig + Send + Sync> LogIndex for IndexLBPT<K> {
    fn lookup(&self, query: Segment, dst: &mut Vec<SegmentMapping>) -> usize {
        if query.length == 0 || self.mappings.is_empty() || self.virtual_size == 0 {
            return 0;
        }

        let start_len = dst.len();

        let idx = if self.lbpt.depth != -1 {
            let mut i = self.lbpt.search(query.offset);
            if i < self.mappings.len() && self.mappings[i].end() <= query.offset {
                i += 1;
            }
            i
        } else {
            self.mappings.partition_point(|m| m.end() <= query.offset)
        };

        let end_offset = query.end();
        for m in self.mappings.iter().skip(idx) {
            if m.offset() >= end_offset {
                break;
            }

            let mut mp = *m;
            if mp.offset() < query.offset {
                mp.forward_offset_to(query.offset);
            }
            if mp.end() > end_offset {
                mp.backward_end_to(end_offset);
            }

            if mp.length() > 0 {
                dst.push(mp);
            }
        }

        dst.len() - start_len
    }
}

#[derive(Debug, Default)]
pub struct MutableIndex {
    mappings: BTreeSet<SegmentMapping>,
}

impl MutableIndex {
    pub fn new() -> Self {
        Self {
            mappings: BTreeSet::new(),
        }
    }

    pub fn insert(&mut self, m: SegmentMapping) {
        if m.length() == 0 {
            return;
        }

        let mut to_remove = Vec::new();

        let scan_end_key = SegmentMapping {
            segment: Segment::new(m.end(), 0),
            moffset: 0,
            zeroed: false,
            tag: 0,
        };

        for other in self.mappings.range(..scan_end_key).rev() {
            if other.end() <= m.offset() {
                break;
            }
            to_remove.push(*other);
        }

        for old in to_remove {
            self.mappings.remove(&old);

            if old.offset() < m.offset() {
                // old overlay with m like:
                // old: [============
                // m  :      [=============]
                let mut left = old;
                left.backward_end_to(m.offset());
                if left.length() > 0 {
                    self.mappings.insert(left);
                }
            }

            if old.end() > m.end() {
                // old overlay with m like:
                // old:          ==========]
                // m  :      [=========]
                let mut right = old;
                right.forward_offset_to(m.end());
                if right.length() > 0 {
                    self.mappings.insert(right);
                }
            }
        }

        self.mappings.insert(m);
    }

    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty()
    }

    pub fn dump(&self) -> Vec<SegmentMapping> {
        self.mappings.iter().cloned().collect()
    }
}

impl LogIndex for MutableIndex {
    fn lookup(&self, query: Segment, dst: &mut Vec<SegmentMapping>) -> usize {
        if query.length == 0 {
            return 0;
        }
        let end_offset = query.end();
        let start_len = dst.len();

        let query_start_key = SegmentMapping::new(query.offset, 0, 0, false, 0);
        if let Some(left) = self.mappings.range(..query_start_key).next_back() {
            if left.end() > query.offset {
                let mut mp = *left;
                if mp.offset() < query.offset {
                    mp.forward_offset_to(query.offset);
                }
                if mp.end() > end_offset {
                    mp.backward_end_to(end_offset);
                }
                if mp.length() > 0 {
                    dst.push(mp);
                }
            }
        }

        let scan_end_key = SegmentMapping::new(end_offset, 0, 0, false, 0);
        for m in self.mappings.range(query_start_key..scan_end_key) {
            let mut mp = *m;
            // 此时 mp.offset >= query.offset，无需前向裁剪
            if mp.end() > end_offset {
                mp.backward_end_to(end_offset);
            }
            if mp.length() > 0 {
                dst.push(mp);
            }
        }

        dst.len() - start_len
    }
}

pub struct ComboIndex {
    pub upper: MutableIndex,
    pub lower: Option<Arc<ReadOnlyIndex>>,
}

impl ComboIndex {
    pub fn new(
        mut upper: MutableIndex,
        lower: Option<Arc<ReadOnlyIndex>>,
        ro_layers_count: u8,
    ) -> Self {
        if ro_layers_count > 0 {
            // FIXME: why do not alter upper directly?
            let old_mappings: Vec<_> = upper.mappings.iter().cloned().collect();
            upper.mappings.clear();
            for mut m in old_mappings {
                m.tag += ro_layers_count;
                upper.mappings.insert(m);
            }
        }
        Self { upper, lower }
    }

    pub fn insert(&mut self, m: SegmentMapping) {
        self.upper.insert(m);
    }

    pub fn upper_len(&self) -> usize {
        self.upper.len()
    }
}

impl LogIndex for ComboIndex {
    fn lookup(&self, query: Segment, dst: &mut Vec<SegmentMapping>) -> usize {
        if query.length == 0 {
            return 0;
        }

        let start_len = dst.len();

        let mut upper_results = Vec::new();
        self.upper.lookup(query, &mut upper_results);

        if self.lower.is_none() {
            dst.extend(upper_results);
            return dst.len() - start_len;
        }

        let lower = self.lower.as_ref().unwrap();

        let mut current_pos = query.offset;
        let end_pos = query.end();

        for m in upper_results {
            if m.offset() > current_pos {
                let gap_len = (m.offset() - current_pos) as u32;
                let gap_segment = Segment::new(current_pos, gap_len);
                lower.lookup(gap_segment, dst);
            }

            dst.push(m);

            current_pos = m.end();
        }

        if current_pos < end_pos {
            let gap_len = (end_pos - current_pos) as u32;
            let gap_segment = Segment::new(current_pos, gap_len);
            lower.lookup(gap_segment, dst);
        }

        dst.len() - start_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_mappings_eq(actual: &[SegmentMapping], expected: &[SegmentMapping]) {
        assert_eq!(
            actual.len(),
            expected.len(),
            "Count mismatch.\nActual: {:?}\nExpected: {:?}",
            actual,
            expected
        );
        for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
            assert_eq!(a, e, "Mismatch at index {}", i);
        }
    }

    /// TEST(Index, lookup)
    #[test]
    fn test_readonly_lookup() {
        let mappings = vec![
            SegmentMapping::new(0, 10, 0, false, 0),
            SegmentMapping::new(10, 10, 50, false, 0),
            SegmentMapping::new(100, 10, 20, false, 0),
        ];
        let idx = ReadOnlyIndex::new(mappings);

        // Case 1: normal overlap
        let mut out = Vec::new();
        idx.lookup(Segment::new(5, 10), &mut out);
        let expected = vec![
            SegmentMapping::new(5, 5, 5, false, 0),
            SegmentMapping::new(10, 5, 50, false, 0),
        ];
        assert_mappings_eq(&out, &expected);

        // Case 2: partial middle overlap
        out.clear();
        idx.lookup(Segment::new(16, 10), &mut out); // [16, 26) intersects [10, 20) at [16, 20)
        let expected = vec![SegmentMapping::new(16, 4, 56, false, 0)];
        assert_mappings_eq(&out, &expected);

        // Case 3: gap
        out.clear();
        idx.lookup(Segment::new(26, 10), &mut out); // [26, 36) in gap
        assert!(out.is_empty());

        // Case 4: multiple segments
        out.clear();
        idx.lookup(Segment::new(6, 100), &mut out); // [6, 106)
        let expected = vec![
            SegmentMapping::new(6, 4, 6, false, 0),
            SegmentMapping::new(10, 10, 50, false, 0),
            SegmentMapping::new(100, 6, 20, false, 0),
        ];
        assert_mappings_eq(&out, &expected);
    }

    /// TEST(Index0, insert)
    #[test]
    fn test_mutable_insert() {
        let mut idx = MutableIndex::new();

        idx.insert(SegmentMapping::new(0, 20, 0, false, 0));
        idx.insert(SegmentMapping::new(10, 15, 50, false, 0));
        idx.insert(SegmentMapping::new(30, 100, 20, false, 0));
        idx.insert(SegmentMapping::new(5, 10, 3, false, 0));
        idx.insert(SegmentMapping::new(40, 10, 123, false, 0));
        idx.insert(SegmentMapping::new(200, 10, 2133, false, 0));
        idx.insert(SegmentMapping::new(150, 100, 21, false, 0));

        let expected = vec![
            SegmentMapping::new(0, 5, 0, false, 0),
            SegmentMapping::new(5, 10, 3, false, 0),
            SegmentMapping::new(15, 10, 55, false, 0),
            SegmentMapping::new(30, 10, 20, false, 0),
            SegmentMapping::new(40, 10, 123, false, 0),
            SegmentMapping::new(50, 80, 40, false, 0),
            SegmentMapping::new(150, 100, 21, false, 0),
        ];

        let actual = idx.dump();
        assert_mappings_eq(&actual, &expected);
    }

    fn sm(offset: u64, length: u32, moffset: u64) -> SegmentMapping {
        SegmentMapping::new(offset, length, moffset, false, 0)
    }

    fn sm_tag(offset: u64, length: u32, moffset: u64, tag: u8) -> SegmentMapping {
        SegmentMapping::new(offset, length, moffset, false, tag)
    }

    fn run_merge_combo_test(indexes: &[&ReadOnlyIndex], expected: &[SegmentMapping]) {
        let n = indexes.len();
        if n == 0 {
            return;
        }

        let mut idx0 = MutableIndex::new();
        for m in &indexes[0].mappings {
            idx0.insert(*m);
        }

        let mi = if n > 1 {
            Some(Arc::new(ReadOnlyIndex::merge(&indexes[1..])))
        } else {
            None
        };

        let ro_layers_count = (n - 1) as u8;
        let ci = ComboIndex::new(idx0, mi, ro_layers_count);

        let mut out = Vec::new();
        let count = ci.lookup(Segment::new(0, 10000), &mut out);

        assert_eq!(count, expected.len(), "Result count mismatch");
        assert_mappings_eq(&out, expected);
    }

    /// TEST(Index, merge)
    #[test]
    fn test_index_merge() {
        let idx0 = ReadOnlyIndex::new(vec![sm(5, 5, 0), sm(10, 10, 50), sm(100, 10, 20)]);

        let idx1 = ReadOnlyIndex::new(vec![
            sm(0, 1, 7),
            sm(2, 4, 5),
            sm(15, 10, 22),
            sm(30, 15, 89),
            sm(87, 50, 32),
            sm(150, 10, 84),
        ]);

        let idx2 = ReadOnlyIndex::new(vec![
            sm(1, 3, 134),
            sm(8, 4, 873),
            sm(18, 72, 320),
            sm(100, 100, 4893),
            sm(1000, 1000, 39823),
        ]);

        let idx3 = ReadOnlyIndex::new(vec![
            sm(23, 10, 0),
            sm(65, 10, 50),
            sm(89, 10, 20),
            sm(230, 43, 432),
            sm(1999, 31, 2393),
        ]);

        let all_indexes = [&idx0, &idx1, &idx2, &idx3];

        // --- Case 1: Merge 2 layers (idx0, idx1) ---
        // idx0 (Upper) -> Tag += 1 => Tag 1
        // idx1 (Lower) -> Tag 0
        run_merge_combo_test(
            &all_indexes[0..2],
            &[
                sm_tag(0, 1, 7, 0),
                sm_tag(2, 3, 5, 0),
                sm_tag(5, 5, 0, 1),
                sm_tag(10, 10, 50, 1),
                sm_tag(20, 5, 27, 0),
                sm_tag(30, 15, 89, 0),
                sm_tag(87, 13, 32, 0),
                sm_tag(100, 10, 20, 1),
                sm_tag(110, 27, 55, 0),
                sm_tag(150, 10, 84, 0),
            ],
        );

        // --- Case 2: Merge 3 layers (idx0, idx1, idx2) ---
        // idx0 (Upper) -> Tag += 2 => Tag 2
        // Lower (idx1, idx2): idx1 -> Tag 0, idx2 -> Tag 1
        run_merge_combo_test(
            &all_indexes[0..3],
            &[
                sm_tag(0, 1, 7, 0),
                sm_tag(1, 1, 134, 1),
                sm_tag(2, 3, 5, 0),
                sm_tag(5, 5, 0, 2),
                sm_tag(10, 10, 50, 2),
                sm_tag(20, 5, 27, 0),
                sm_tag(25, 5, 327, 1),
                sm_tag(30, 15, 89, 0),
                sm_tag(45, 42, 347, 1),
                sm_tag(87, 13, 32, 0),
                sm_tag(100, 10, 20, 2),
                sm_tag(110, 27, 55, 0),
                sm_tag(137, 13, 4930, 1),
                sm_tag(150, 10, 84, 0),
                sm_tag(160, 40, 4953, 1),
                sm_tag(1000, 1000, 39823, 1),
            ],
        );

        // --- Case 3: Merge 4 layers (idx0, idx1, idx2, idx3) ---
        // idx0 (Upper) -> Tag += 3 => Tag 3
        // Lower: idx1 -> 0, idx2 -> 1, idx3 -> 2
        run_merge_combo_test(
            &all_indexes[0..4],
            &[
                sm_tag(0, 1, 7, 0),
                sm_tag(1, 1, 134, 1),
                sm_tag(2, 3, 5, 0),
                sm_tag(5, 5, 0, 3),
                sm_tag(10, 10, 50, 3),
                sm_tag(20, 5, 27, 0),
                sm_tag(25, 5, 327, 1),
                sm_tag(30, 15, 89, 0),
                sm_tag(45, 42, 347, 1),
                sm_tag(87, 13, 32, 0),
                sm_tag(100, 10, 20, 3),
                sm_tag(110, 27, 55, 0),
                sm_tag(137, 13, 4930, 1),
                sm_tag(150, 10, 84, 0),
                sm_tag(160, 40, 4953, 1),
                sm_tag(230, 43, 432, 2),
                sm_tag(1000, 1000, 39823, 1),
                sm_tag(2000, 30, 2394, 2),
            ],
        );
    }

    fn run_compress_test(src: &[SegmentMapping], expected_rst: &[SegmentMapping]) {
        let n2 = expected_rst.len();

        // 1. Test Predict
        let n1cp = compress_raw_index_predict(src);
        assert_eq!(n1cp, n2, "Predict size mismatch");

        // 2. Test Compress
        let mut src_vec = src.to_vec();
        let n1c = compress_raw_index(&mut src_vec);
        assert_eq!(n1c, n2, "Compress size mismatch");

        // 3. Verify Content
        assert_mappings_eq(&src_vec, expected_rst);
    }

    /// TEST(Index, compress)
    #[test]
    fn test_index_compress() {
        // Case 1: Simple merge
        // {{5, 5, 0}, {10, 10, 5}, {100, 10, 20}}
        // -> {{5, 15, 0}, {100, 10, 20}}
        run_compress_test(
            &[sm(5, 5, 0), sm(10, 10, 5), sm(100, 10, 20)],
            &[sm(5, 15, 0), sm(100, 10, 20)],
        );

        // Case 2: Broken by physical offset (moffset)
        // {{5, 5, 0}, {10, 10, 5}, {20, 10, 15}, {100, 10, 20}}
        // -> {{5, 25, 0}, {100, 10, 20}}
        // Note: {10, 10, 5} ends at offset 20, mend 15. {20, 10, 15} starts at offset 20, moffset 15.
        // They are contiguous both logically and physically.
        run_compress_test(
            &[sm(5, 5, 0), sm(10, 10, 5), sm(20, 10, 15), sm(100, 10, 20)],
            &[sm(5, 25, 0), sm(100, 10, 20)],
        );

        // Case 3: Broken by Tag
        // {{5, 5, 0}, {10, 10, 5}, {20, 10, 15, 1}, {100, 10, 20}}
        // -> {{5, 15, 0}, {20, 10, 15, 1}, {100, 10, 20}}
        // The 3rd element has tag 1, so it cannot merge with the 2nd (tag 0).
        run_compress_test(
            &[
                sm(5, 5, 0),
                sm(10, 10, 5),
                sm_tag(20, 10, 15, 1),
                sm(100, 10, 20),
            ],
            &[sm(5, 15, 0), sm_tag(20, 10, 15, 1), sm(100, 10, 20)],
        );

        // Case 4: Broken by Tag (middle element)
        // {{5, 5, 0}, {10, 10, 5, 3}, {20, 10, 15, 3}, {30, 10, 20}}
        // -> {{5, 5, 0}, {10, 20, 5, 3}, {30, 10, 20}}
        // {10, 10, 5, 3} and {20, 10, 15, 3} merge because offsets, moffsets, and tags match.
        run_compress_test(
            &[
                sm(5, 5, 0),
                sm_tag(10, 10, 5, 3),
                sm_tag(20, 10, 15, 3),
                sm(30, 10, 20),
            ],
            &[sm(5, 5, 0), sm_tag(10, 20, 5, 3), sm(30, 10, 20)],
        );

        // Case 5: Broken by physical offset discontinuity (Review Fix Verification)
        // {{0, 10, 100}, {10, 10, 200}} -> No Merge
        // Segment 1 mend = 110, Segment 2 moffset = 200. Not contiguous.
        run_compress_test(
            &[sm(0, 10, 100), sm(10, 10, 200)],
            &[sm(0, 10, 100), sm(10, 10, 200)],
        );
    }

    #[test]
    fn test_index_lbpt() {
        // Construct a mapping list
        let mappings = vec![
            SegmentMapping::new(0, 10, 0, false, 0),
            SegmentMapping::new(10, 10, 50, false, 0),
            SegmentMapping::new(100, 10, 20, false, 0),
            SegmentMapping::new(200, 50, 1000, false, 0),
        ];

        // Create IndexLBPT (using u32 keys internally if offsets fit, or u64)
        // Here we test u64 variant explicitly
        let idx = IndexLBPT::<u64>::new(mappings.clone(), 10000);

        // Test Lookup
        let mut out = Vec::new();

        // Case 1: Middle overlap
        idx.lookup(Segment::new(5, 10), &mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], SegmentMapping::new(5, 5, 5, false, 0));
        assert_eq!(out[1], SegmentMapping::new(10, 5, 50, false, 0));

        // Case 2: Exact segment
        out.clear();
        idx.lookup(Segment::new(100, 10), &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], SegmentMapping::new(100, 10, 20, false, 0));

        // Case 3: Gap
        out.clear();
        idx.lookup(Segment::new(50, 10), &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn test_segment_boundary_operations() {
        let mut seg = Segment::new(10, 20);
        assert_eq!(seg.end(), 30);

        // forward_offset_to: before offset
        let delta = seg.forward_offset_to(5);
        assert_eq!(delta, 0);
        assert_eq!(seg.offset, 10);
        assert_eq!(seg.length, 20);

        // forward_offset_to: within segment
        let delta = seg.forward_offset_to(15);
        assert_eq!(delta, 5);
        assert_eq!(seg.offset, 15);
        assert_eq!(seg.length, 15);

        // forward_offset_to: beyond end
        let delta = seg.forward_offset_to(50);
        assert_eq!(delta, 35);
        assert_eq!(seg.offset, 50);
        assert_eq!(seg.length, 0);

        // backward_end_to
        let mut seg2 = Segment::new(10, 20);
        seg2.backward_end_to(20);
        assert_eq!(seg2.length, 10);

        seg2.backward_end_to(5);
        assert_eq!(seg2.length, 0);
    }

    #[test]
    fn test_segment_mapping_boundary() {
        let mut m = SegmentMapping::new(10, 20, 100, false, 0);
        assert_eq!(m.end(), 30);
        assert_eq!(m.mend(), 120);

        m.forward_offset_to(15);
        assert_eq!(m.offset(), 15);
        assert_eq!(m.length(), 15);
        assert_eq!(m.moffset, 105);

        // A backed zero clips its physical range just like ordinary data.
        let mut m2 = SegmentMapping::new(10, 20, 100, true, 0);
        assert!(m2.has_physical_range());
        assert_eq!(m2.mend(), 120);
        m2.forward_offset_to(15);
        assert_eq!(m2.moffset, 105);
        m2.backward_end_to(20);
        assert_eq!(m2.length(), 5);
        assert_eq!(m2.mend(), 110);

        // An unbacked zero never does arithmetic on the marker.
        let mut m3 = SegmentMapping::new(10, 20, NO_PHYSICAL_OFFSET, true, 0);
        assert!(!m3.has_physical_range());
        m3.forward_offset_to(15);
        m3.backward_end_to(20);
        assert_eq!(m3.length(), 5);
        assert_eq!(m3.moffset, NO_PHYSICAL_OFFSET);
        assert_eq!(m3.mend(), NO_PHYSICAL_OFFSET);
        m3.forward_offset_to(u64::MAX);
        assert_eq!(m3.length(), 0);
        assert_eq!(m3.moffset, NO_PHYSICAL_OFFSET);
    }

    #[test]
    fn test_mutable_index_splits_backed_and_unbacked_zeros() {
        for physical_offset in [100, NO_PHYSICAL_OFFSET] {
            let mut idx = MutableIndex::new();
            idx.insert(SegmentMapping::new(10, 20, physical_offset, true, 0));
            idx.insert(sm(15, 5, 500));
            let right_offset = if physical_offset == NO_PHYSICAL_OFFSET {
                NO_PHYSICAL_OFFSET
            } else {
                physical_offset + 10
            };
            assert_eq!(
                idx.dump(),
                vec![
                    SegmentMapping::new(10, 5, physical_offset, true, 0),
                    sm(15, 5, 500),
                    SegmentMapping::new(20, 10, right_offset, true, 0),
                ]
            );
        }
    }

    #[test]
    fn test_readonly_index_normalizes_zeros_before_merge_and_lookup() {
        let old_zero = SegmentMapping::new(10, 20, 100, true, 0);
        let lower = ReadOnlyIndex::new(vec![old_zero]);
        let top = ReadOnlyIndex::new(vec![sm(15, 5, 500)]);
        let merged = ReadOnlyIndex::merge(&[&top, &lower]);
        let mut out = Vec::new();
        merged.lookup(Segment::new(12, 12), &mut out);
        assert_eq!(
            out,
            vec![
                SegmentMapping::new(12, 3, NO_PHYSICAL_OFFSET, true, 1),
                sm(15, 5, 500),
                SegmentMapping::new(20, 4, NO_PHYSICAL_OFFSET, true, 1),
            ]
        );
        assert_eq!(old_zero.moffset, 100);
    }

    #[test]
    fn test_readonly_index_edge_cases() {
        // Empty index
        let idx = ReadOnlyIndex::new(vec![]);
        let mut out = Vec::new();
        let n = idx.lookup(Segment::new(0, 10), &mut out);
        assert_eq!(n, 0);
        assert!(out.is_empty());

        // Single mapping
        let idx = ReadOnlyIndex::new(vec![sm(10, 10, 0)]);
        out.clear();
        idx.lookup(Segment::new(5, 20), &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], sm(10, 10, 0));

        // Zero-length query
        out.clear();
        idx.lookup(Segment::new(10, 0), &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn test_mutable_index_edge_cases() {
        let mut idx = MutableIndex::new();

        // Insert zero-length segment (should be ignored)
        idx.insert(SegmentMapping::new(10, 0, 100, false, 0));
        assert_eq!(idx.len(), 0);

        // Complete overlap
        idx.insert(sm(10, 20, 100));
        idx.insert(sm(10, 20, 200));
        assert_eq!(idx.len(), 1);
        let dump = idx.dump();
        assert_eq!(dump[0], sm(10, 20, 200));

        // Partial overlap - left side
        let mut idx2 = MutableIndex::new();
        idx2.insert(sm(10, 20, 100));
        idx2.insert(sm(5, 10, 200));
        let dump = idx2.dump();
        assert_eq!(dump.len(), 2);
        assert_eq!(dump[0], sm(5, 10, 200));
        assert_eq!(dump[1], sm(15, 15, 105));

        // Partial overlap - right side
        let mut idx3 = MutableIndex::new();
        idx3.insert(sm(10, 20, 100));
        idx3.insert(sm(25, 10, 200));
        let dump = idx3.dump();
        assert_eq!(dump.len(), 2);
        assert_eq!(dump[0], sm(10, 15, 100));
        assert_eq!(dump[1], sm(25, 10, 200));
    }

    #[test]
    fn test_combo_index_gaps() {
        let lower = Arc::new(ReadOnlyIndex::new(vec![
            sm(0, 10, 0),
            sm(20, 10, 100),
            sm(40, 10, 200),
        ]));
        let mut upper = MutableIndex::new();
        upper.insert(sm(5, 5, 500));
        upper.insert(sm(25, 5, 600));

        let ci = ComboIndex::new(upper, Some(lower), 1);
        let mut out = Vec::new();
        ci.lookup(Segment::new(0, 50), &mut out);

        // Should have: lower[0,5), upper[5,10), lower[10,20), lower[20,25), upper[25,30), lower[30,40), lower[40,50)
        assert!(!out.is_empty());
        assert_eq!(out[0].offset(), 0);
    }

    #[test]
    fn test_compress_with_zeroed_segments() {
        let mut mappings = vec![
            SegmentMapping::new(0, 10, NO_PHYSICAL_OFFSET, true, 0),
            SegmentMapping::new(10, 10, NO_PHYSICAL_OFFSET, true, 0),
        ];
        assert_eq!(compress_raw_index_predict(&mappings), 1);
        let n = compress_raw_index(&mut mappings);
        assert_eq!(n, 1);
        assert_eq!(
            mappings[0],
            SegmentMapping::new(0, 20, NO_PHYSICAL_OFFSET, true, 0)
        );
    }

    #[test]
    fn test_compress_backed_zeros_requires_contiguous_physical_ranges() {
        let first = SegmentMapping::new(0, 10, 100, true, 0);
        let cases = [
            (SegmentMapping::new(10, 10, 110, true, 0), 1),
            (SegmentMapping::new(10, 10, 100, true, 0), 2),
            (SegmentMapping::new(10, 10, 200, true, 0), 2),
            (SegmentMapping::new(10, 10, 110, false, 0), 2),
            (SegmentMapping::new(10, 10, 110, true, 1), 2),
            (SegmentMapping::new(10, 10, NO_PHYSICAL_OFFSET, true, 0), 2),
        ];
        for (second, expected_len) in cases {
            let mut mappings = vec![first, second];
            assert_eq!(compress_raw_index_predict(&mappings), expected_len);
            assert_eq!(compress_raw_index(&mut mappings), expected_len);
        }
        // A physical end equal to the sentinel must not make a backed zero
        // merge with an adjacent unbacked zero.
        let mut mappings = vec![
            SegmentMapping::new(0, 1, NO_PHYSICAL_OFFSET - 1, true, 0),
            SegmentMapping::new(1, 1, NO_PHYSICAL_OFFSET, true, 0),
        ];
        assert_eq!(compress_raw_index_predict(&mappings), 2);
        assert_eq!(compress_raw_index(&mut mappings), 2);
        mappings.reverse();
        mappings[0].segment.offset = 0;
        mappings[1].segment.offset = 1;
        assert_eq!(compress_raw_index(&mut mappings), 2);
    }

    #[test]
    fn test_compress_zero_max_length_boundary() {
        let max_len = Segment::MAX_LENGTH;
        for physical_offset in [100, NO_PHYSICAL_OFFSET] {
            let next_offset = if physical_offset == NO_PHYSICAL_OFFSET {
                NO_PHYSICAL_OFFSET
            } else {
                physical_offset + u64::from(max_len)
            };
            let mut mappings = vec![
                SegmentMapping::new(0, max_len, physical_offset, true, 0),
                SegmentMapping::new(u64::from(max_len), 1, next_offset, true, 0),
            ];
            assert_eq!(compress_raw_index_predict(&mappings), 2);
            assert_eq!(compress_raw_index(&mut mappings), 2);
        }
    }

    #[test]
    fn test_compress_max_length_boundary() {
        let max_len = Segment::MAX_LENGTH;
        let mut mappings = vec![
            SegmentMapping::new(0, max_len, 0, false, 0),
            SegmentMapping::new(max_len as u64, 10, max_len as u64, false, 0),
        ];
        let n = compress_raw_index(&mut mappings);
        assert_eq!(n, 2); // Cannot merge due to MAX_LENGTH constraint
    }
}
