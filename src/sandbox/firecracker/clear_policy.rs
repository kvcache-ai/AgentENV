//! Dynamic incremental dirty-memory CLEAR policy for Local Firecracker snapshots.
//!
//! Tracks the cumulative 4 KiB dirty-page union (`D_cum`) across a Firecracker
//! process lifecycle so each incremental Local snapshot can measure its true
//! overlap ratio `D_inc / D_cum`. When consecutive captures overwrite most of
//! the cumulative working set (making layering unrewarding), the policy
//! temporarily pauses CLEAR for a bounded cumulative span before probing again.

use firecracker_client::models::DirtyMemoryRanges;

pub(super) const DEFAULT_INCREMENTAL_LOCAL_CLEAR_MAX_RATIO: f64 = 0.75;

const PAGE_SIZE_BYTES: u64 = 4096;
const HIGH_RATIO_STREAK_TO_FALLBACK: u8 = 2;
const CUMULATIVE_FALLBACK_SPAN: u8 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClearPolicyMode {
    Incremental,
    Cumulative { remaining: u8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClearDecisionReason {
    Disabled,
    Unsupported,
    IncrementalClear,
    ProbeClear,
    LayerGuardAdvance,
    CumulativeFallback,
}

impl ClearDecisionReason {
    pub(super) fn as_metric_label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Unsupported => "unsupported",
            Self::IncrementalClear => "clear",
            Self::ProbeClear => "probe_clear",
            Self::LayerGuardAdvance => "layer_guard_advance",
            Self::CumulativeFallback => "cumulative_fallback",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct LocalClearPlan {
    pub(super) advance_baseline: bool,
    pub(super) clear_after: bool,
    pub(super) reason: ClearDecisionReason,
    pub(super) dirty_bytes: u64,
    pub(super) d_cum_bytes: u64,
    pub(super) overlap_ratio: f64,
    pub(super) is_incremental_observation: bool,
}

#[derive(Debug)]
pub(super) struct DynamicClearPolicy {
    enabled: bool,
    max_ratio: f64,
    supported: bool,
    mode: ClearPolicyMode,
    consecutive_high_ratio: u8,
    last_capture_cleared: bool,
    page_union: Vec<u64>,
    cum_pages: u64,
}

impl DynamicClearPolicy {
    pub(super) fn new(enabled: bool, max_ratio: f64) -> Self {
        Self {
            enabled,
            max_ratio,
            supported: true,
            mode: ClearPolicyMode::Incremental,
            consecutive_high_ratio: 0,
            last_capture_cleared: false,
            page_union: Vec::new(),
            cum_pages: 0,
        }
    }

    pub(super) fn is_active(&self) -> bool {
        self.enabled && self.supported
    }

    /// Reset per-process dirty-page union and fallback state when the
    /// Firecracker process stops or starts fresh from a snapshot.
    pub(super) fn reset(&mut self) {
        self.mode = ClearPolicyMode::Incremental;
        self.consecutive_high_ratio = 0;
        self.last_capture_cleared = false;
        self.page_union.clear();
        self.cum_pages = 0;
    }

    /// Permanently disable CLEAR on this sandbox instance when the host
    /// Firecracker binary does not expose `PUT /vm/dirty-memory-ranges/clear`.
    pub(super) fn mark_unsupported(&mut self) {
        self.supported = false;
        self.last_capture_cleared = false;
        self.page_union.clear();
        self.page_union.shrink_to_fit();
        self.cum_pages = 0;
        metrics::counter!(
            "agentenv_memory_snapshot_clear_decisions_total",
            "decision" => ClearDecisionReason::Unsupported.as_metric_label(),
        )
        .increment(1);
    }

    /// Compute the CLEAR decision for a Local snapshot without mutating policy
    /// state (call [`Self::commit_local_capture`] only after capture succeeds).
    pub(super) fn plan_local_capture(
        &self,
        dirty: &DirtyMemoryRanges,
        would_compact_next_delta: bool,
        pending_paused_upper: bool,
    ) -> LocalClearPlan {
        if !self.enabled {
            return LocalClearPlan {
                advance_baseline: false,
                clear_after: false,
                reason: ClearDecisionReason::Disabled,
                dirty_bytes: 0,
                d_cum_bytes: 0,
                overlap_ratio: 0.0,
                is_incremental_observation: false,
            };
        }
        if !self.supported {
            return LocalClearPlan {
                advance_baseline: false,
                clear_after: false,
                reason: ClearDecisionReason::Unsupported,
                dirty_bytes: 0,
                d_cum_bytes: 0,
                overlap_ratio: 0.0,
                is_incremental_observation: false,
            };
        }

        let (dirty_pages, new_pages) = self.project_dirty_pages(dirty);
        let d_cum_pages = self.cum_pages.saturating_add(new_pages);
        let dirty_bytes = dirty_pages.saturating_mul(PAGE_SIZE_BYTES);
        let d_cum_bytes = d_cum_pages.saturating_mul(PAGE_SIZE_BYTES);
        let overlap_ratio = if d_cum_bytes == 0 {
            0.0
        } else {
            (dirty_bytes as f64 / d_cum_bytes as f64).clamp(0.0, 1.0)
        };
        let is_incremental_observation = self.last_capture_cleared;

        if dirty_pages == 0 {
            let reason = match self.mode {
                ClearPolicyMode::Incremental => ClearDecisionReason::IncrementalClear,
                ClearPolicyMode::Cumulative { .. } => ClearDecisionReason::CumulativeFallback,
            };
            return LocalClearPlan {
                advance_baseline: false,
                clear_after: false,
                reason,
                dirty_bytes,
                d_cum_bytes,
                overlap_ratio,
                is_incremental_observation,
            };
        }

        let (advance_baseline, clear_after, reason) = match self.mode {
            ClearPolicyMode::Incremental => (true, true, ClearDecisionReason::IncrementalClear),
            ClearPolicyMode::Cumulative { remaining: 0 } => {
                (true, true, ClearDecisionReason::ProbeClear)
            }
            ClearPolicyMode::Cumulative { remaining: _ } => {
                if would_compact_next_delta && !pending_paused_upper {
                    // Advance and compact the runtime stack once rather than
                    // triggering repeated non-advancing compactions on every
                    // subsequent sibling snapshot.
                    (true, true, ClearDecisionReason::LayerGuardAdvance)
                } else {
                    (false, false, ClearDecisionReason::CumulativeFallback)
                }
            }
        };

        LocalClearPlan {
            advance_baseline,
            clear_after,
            reason,
            dirty_bytes,
            d_cum_bytes,
            overlap_ratio,
            is_incremental_observation,
        }
    }

    /// Commit policy state transitions and emit observability metrics after a
    /// Local snapshot has been durably captured.
    pub(super) fn commit_local_capture(
        &mut self,
        dirty: &DirtyMemoryRanges,
        plan: &LocalClearPlan,
    ) {
        if matches!(
            plan.reason,
            ClearDecisionReason::Disabled | ClearDecisionReason::Unsupported
        ) {
            self.last_capture_cleared = false;
            metrics::counter!(
                "agentenv_memory_snapshot_clear_decisions_total",
                "decision" => plan.reason.as_metric_label(),
            )
            .increment(1);
            return;
        }

        self.absorb_dirty_pages(dirty);

        match plan.reason {
            ClearDecisionReason::Disabled | ClearDecisionReason::Unsupported => unreachable!(),
            ClearDecisionReason::CumulativeFallback => {
                if let ClearPolicyMode::Cumulative { remaining } = &mut self.mode {
                    *remaining = remaining.saturating_sub(1);
                }
                self.last_capture_cleared = false;
            }
            ClearDecisionReason::ProbeClear => {
                self.mode = ClearPolicyMode::Incremental;
                self.consecutive_high_ratio = 0;
                self.last_capture_cleared = true;
            }
            ClearDecisionReason::LayerGuardAdvance => {
                if let ClearPolicyMode::Cumulative { remaining } = &mut self.mode {
                    *remaining = remaining.saturating_sub(1);
                }
                self.last_capture_cleared = true;
            }
            ClearDecisionReason::IncrementalClear => {
                if plan.dirty_bytes == 0 {
                    self.consecutive_high_ratio = 0;
                } else {
                    self.last_capture_cleared = true;
                    if plan.is_incremental_observation {
                        if plan.overlap_ratio >= self.max_ratio {
                            self.consecutive_high_ratio =
                                self.consecutive_high_ratio.saturating_add(1);
                            if self.consecutive_high_ratio >= HIGH_RATIO_STREAK_TO_FALLBACK {
                                self.mode = ClearPolicyMode::Cumulative {
                                    remaining: CUMULATIVE_FALLBACK_SPAN,
                                };
                                self.consecutive_high_ratio = 0;
                            }
                        } else {
                            self.consecutive_high_ratio = 0;
                        }
                    }
                }
            }
        }

        metrics::histogram!("agentenv_memory_snapshot_dirty_exported_bytes")
            .record(plan.dirty_bytes as f64);
        metrics::histogram!("agentenv_memory_snapshot_dirty_cumulative_bytes")
            .record(plan.d_cum_bytes as f64);
        if plan.is_incremental_observation {
            metrics::histogram!("agentenv_memory_snapshot_dirty_incremental_bytes")
                .record(plan.dirty_bytes as f64);
            if plan.d_cum_bytes > 0 {
                metrics::histogram!("agentenv_memory_snapshot_dirty_overlap_ratio")
                    .record(plan.overlap_ratio);
            }
        }
        metrics::counter!(
            "agentenv_memory_snapshot_clear_decisions_total",
            "decision" => plan.reason.as_metric_label(),
        )
        .increment(1);
    }

    /// Record dirty pages observed during an operation that did not clear
    /// Firecracker's dirty bitmap (e.g. pause or Distributed snapshot).
    pub(super) fn record_dirty_without_clear(&mut self, dirty: &DirtyMemoryRanges) {
        self.last_capture_cleared = false;
        if !self.is_active() {
            return;
        }
        self.absorb_dirty_pages(dirty);
    }

    fn project_dirty_pages(&self, dirty: &DirtyMemoryRanges) -> (u64, u64) {
        let mut inc_pages = 0u64;
        let mut new_pages = 0u64;
        for range in &dirty.ranges {
            let Ok(offset) = u64::try_from(range.image_offset) else {
                continue;
            };
            let Ok(length) = u64::try_from(range.length) else {
                continue;
            };
            let start_page = offset / PAGE_SIZE_BYTES;
            let page_count = length / PAGE_SIZE_BYTES;
            inc_pages = inc_pages.saturating_add(page_count);
            for page in start_page..start_page.saturating_add(page_count) {
                let word_idx = (page / 64) as usize;
                let mask = 1u64 << (page % 64);
                let already_set = self
                    .page_union
                    .get(word_idx)
                    .is_some_and(|word| (word & mask) != 0);
                if !already_set {
                    new_pages += 1;
                }
            }
        }
        (inc_pages, new_pages)
    }

    fn absorb_dirty_pages(&mut self, dirty: &DirtyMemoryRanges) {
        let memory_pages = u64::try_from(dirty.memory_size)
            .unwrap_or(0)
            .div_ceil(PAGE_SIZE_BYTES);
        let needed_words = memory_pages.div_ceil(64) as usize;
        if self.page_union.len() < needed_words {
            self.page_union.resize(needed_words, 0);
        }
        for range in &dirty.ranges {
            let Ok(offset) = u64::try_from(range.image_offset) else {
                continue;
            };
            let Ok(length) = u64::try_from(range.length) else {
                continue;
            };
            let start_page = offset / PAGE_SIZE_BYTES;
            let page_count = length / PAGE_SIZE_BYTES;
            let end_page = start_page.saturating_add(page_count);
            let end_words = end_page.div_ceil(64) as usize;
            if self.page_union.len() < end_words {
                self.page_union.resize(end_words, 0);
            }
            for page in start_page..end_page {
                let word_idx = (page / 64) as usize;
                let mask = 1u64 << (page % 64);
                let word = &mut self.page_union[word_idx];
                if (*word & mask) == 0 {
                    *word |= mask;
                    self.cum_pages += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use firecracker_client::models::DirtyMemoryRange;

    fn dirty_range_mib(memory_mib: u64, offset_mib: u64, len_mib: u64) -> DirtyMemoryRanges {
        let mib = 1024 * 1024i64;
        DirtyMemoryRanges {
            page_size: 4096,
            memory_size: memory_mib as i64 * mib,
            ranges: if len_mib == 0 {
                Vec::new()
            } else {
                vec![DirtyMemoryRange {
                    base_host_virt_addr: 0x1000_0000 + offset_mib as i64 * mib,
                    image_offset: offset_mib as i64 * mib,
                    length: len_mib as i64 * mib,
                }]
            },
        }
    }

    #[test]
    fn low_overlap_stays_in_incremental_mode() {
        let mut policy = DynamicClearPolicy::new(true, 0.75);
        // Round 1 establishes a 200 MiB cumulative union.
        let r1 = dirty_range_mib(512, 0, 200);
        let p1 = policy.plan_local_capture(&r1, false, false);
        assert_eq!(p1.reason, ClearDecisionReason::IncrementalClear);
        assert!(p1.advance_baseline && p1.clear_after);
        assert!(!p1.is_incremental_observation);
        policy.commit_local_capture(&r1, &p1);

        // Subsequent rounds dirty 100 MiB inside the same 200 MiB (50% < 75%).
        let r_mid = dirty_range_mib(512, 0, 100);
        for _ in 0..10 {
            let plan = policy.plan_local_capture(&r_mid, false, false);
            assert_eq!(plan.reason, ClearDecisionReason::IncrementalClear);
            assert!(plan.is_incremental_observation);
            assert!((plan.overlap_ratio - 0.50).abs() < 1e-6);
            policy.commit_local_capture(&r_mid, &plan);
        }
    }

    #[test]
    fn two_consecutive_high_ratio_observations_trigger_eight_round_fallback_and_probe() {
        let mut policy = DynamicClearPolicy::new(true, 0.75);

        // Round 1: initial 400 MiB (non-incremental observation, does not count toward streak).
        let full = dirty_range_mib(512, 0, 400);
        let p1 = policy.plan_local_capture(&full, false, false);
        assert_eq!(p1.reason, ClearDecisionReason::IncrementalClear);
        policy.commit_local_capture(&full, &p1);

        // Round 2 & 3: 320 MiB / 400 MiB = 80% >= 75% (streak 1 then 2 -> enters fallback).
        let high = dirty_range_mib(512, 0, 320);
        for _ in 0..2 {
            let p = policy.plan_local_capture(&high, false, false);
            assert_eq!(p.reason, ClearDecisionReason::IncrementalClear);
            policy.commit_local_capture(&high, &p);
        }

        // Next 8 rounds run in cumulative fallback without advancing or clearing.
        // Only the first fallback round follows a cleared capture; subsequent
        // fallback rounds observe cumulative dirty pages (not single-round deltas).
        for round in 0..CUMULATIVE_FALLBACK_SPAN {
            let p = policy.plan_local_capture(&full, false, false);
            assert_eq!(p.reason, ClearDecisionReason::CumulativeFallback);
            assert!(!p.advance_baseline && !p.clear_after);
            assert_eq!(p.is_incremental_observation, round == 0);
            policy.commit_local_capture(&full, &p);
        }

        // Round after fallback span runs a single probe CLEAR and returns to Incremental.
        let probe = policy.plan_local_capture(&full, false, false);
        assert_eq!(probe.reason, ClearDecisionReason::ProbeClear);
        assert!(!probe.is_incremental_observation);
        assert!(probe.advance_baseline && probe.clear_after);
        policy.commit_local_capture(&full, &probe);

        // If workload cooled down (40 MiB / 400 MiB = 10%), stay in Incremental.
        let cool = dirty_range_mib(512, 0, 40);
        let after_probe = policy.plan_local_capture(&cool, false, false);
        assert_eq!(after_probe.reason, ClearDecisionReason::IncrementalClear);
        assert!(after_probe.is_incremental_observation);
        assert!((after_probe.overlap_ratio - 0.10).abs() < 1e-6);
        policy.commit_local_capture(&cool, &after_probe);
    }

    #[test]
    fn failed_capture_leaves_policy_state_unchanged() {
        let policy = DynamicClearPolicy::new(true, 0.75);
        let r1 = dirty_range_mib(512, 0, 200);
        let p1 = policy.plan_local_capture(&r1, false, false);
        // Simulate failure: do NOT call commit_local_capture.
        assert_eq!(p1.d_cum_bytes, 200 * 1024 * 1024);
        assert_eq!(policy.cum_pages, 0);
        assert!(!policy.last_capture_cleared);
    }

    #[test]
    fn layer_guard_forces_single_advance_during_cumulative_fallback() {
        let mut policy = DynamicClearPolicy::new(true, 0.75);
        let full = dirty_range_mib(512, 0, 400);
        for _ in 0..3 {
            let p = policy.plan_local_capture(&full, false, false);
            policy.commit_local_capture(&full, &p);
        }
        // Now in Cumulative { remaining: 8 }. When would_compact_next_delta is true,
        // force one advancing capture instead of repeated non-advancing compactions.
        let guard = policy.plan_local_capture(&full, true, false);
        assert_eq!(guard.reason, ClearDecisionReason::LayerGuardAdvance);
        assert!(guard.advance_baseline && guard.clear_after);
        policy.commit_local_capture(&full, &guard);

        // Next round without hitting compaction budget remains in CumulativeFallback.
        let next = policy.plan_local_capture(&full, false, false);
        assert_eq!(next.reason, ClearDecisionReason::CumulativeFallback);
    }

    #[test]
    fn disabled_unsupported_and_reset_behaviors() {
        let mut disabled = DynamicClearPolicy::new(false, 0.75);
        let d = dirty_range_mib(512, 0, 64);
        let p = disabled.plan_local_capture(&d, false, false);
        assert_eq!(p.reason, ClearDecisionReason::Disabled);
        assert!(!p.advance_baseline && !p.clear_after);
        assert_eq!(p.dirty_bytes, 0);
        assert_eq!(p.d_cum_bytes, 0);
        disabled.commit_local_capture(&d, &p);
        disabled.record_dirty_without_clear(&d);
        assert!(disabled.page_union.is_empty());
        assert_eq!(disabled.cum_pages, 0);

        let mut policy = DynamicClearPolicy::new(true, 0.75);
        let p1 = policy.plan_local_capture(&d, false, false);
        policy.commit_local_capture(&d, &p1);
        assert!(policy.cum_pages > 0);
        policy.mark_unsupported();
        assert!(policy.page_union.is_empty());
        assert_eq!(policy.cum_pages, 0);
        let p2 = policy.plan_local_capture(&d, false, false);
        assert_eq!(p2.reason, ClearDecisionReason::Unsupported);
        assert!(!p2.advance_baseline && !p2.clear_after);
        policy.commit_local_capture(&d, &p2);
        policy.record_dirty_without_clear(&d);
        assert!(policy.page_union.is_empty());
        assert_eq!(policy.cum_pages, 0);

        // Reset clears dirty union/mode while keeping unsupported flag.
        policy.reset();
        assert_eq!(policy.cum_pages, 0);
        assert_eq!(
            policy.plan_local_capture(&d, false, false).reason,
            ClearDecisionReason::Unsupported
        );
    }

    #[test]
    fn zero_dirty_pages_skips_baseline_advance_and_clear() {
        let mut policy = DynamicClearPolicy::new(true, 0.75);
        let empty = dirty_range_mib(512, 0, 0);
        let plan = policy.plan_local_capture(&empty, false, false);
        assert_eq!(plan.reason, ClearDecisionReason::IncrementalClear);
        assert!(!plan.advance_baseline && !plan.clear_after);
        policy.commit_local_capture(&empty, &plan);
        assert!(!policy.last_capture_cleared);
    }
}
