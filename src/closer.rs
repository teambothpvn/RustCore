//! Closer: cold thread deep-copy + bounded VecDeque history (Phase 5).
//! Worker never blocks: it moves a finished HotSlot snapshot into the closer
//! inbox (SPSC or mpsc); closer owns the deep copy + footprint pages.

use crate::core::HotSlot;
use std::collections::VecDeque;

pub const MAX_CLOSED: usize = 200;

#[derive(Clone)]
pub struct ClosedBar {
    pub series_idx: usize,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub d_open: i64,
    pub d_high: i64,
    pub d_low: i64,
    pub d_close: i64,
    pub count: u64,
    pub vol: i64,
    pub open_ts: i64,
    pub close_ts: i64,
    pub poc_level: i64,
}

impl ClosedBar {
    pub fn from_slot(series_idx: usize, s: &HotSlot, poc_level: i64) -> Self {
        Self {
            series_idx,
            open: s.open,
            high: s.high,
            low: s.low,
            close: s.close,
            d_open: s.d_open,
            d_high: s.d_high,
            d_low: s.d_low,
            d_close: s.d_close,
            count: s.count,
            vol: s.vol,
            open_ts: s.open_ts,
            close_ts: s.close_ts,
            poc_level,
        }
    }
}

pub struct Closer {
    pub history: Vec<VecDeque<ClosedBar>>,
    pub handoff_count: u64,
    pub drop_count: u64,
}

impl Closer {
    pub fn new(num_series: usize) -> Self {
        Self {
            history: (0..num_series).map(|_| VecDeque::with_capacity(MAX_CLOSED)).collect(),
            handoff_count: 0,
            drop_count: 0,
        }
    }

    /// Cold: accept one closed bar. O(1) evict oldest when full.
    pub fn accept(&mut self, bar: ClosedBar) {
        let q = &mut self.history[bar.series_idx];
        if q.len() >= MAX_CLOSED {
            q.pop_front();
            self.drop_count += 1;
        }
        q.push_back(bar);
        self.handoff_count += 1;
    }

    pub fn last(&self, series_idx: usize) -> Option<&ClosedBar> {
        self.history[series_idx].back()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_evict() {
        let mut c = Closer::new(1);
        for i in 0..(MAX_CLOSED + 10) {
            c.accept(ClosedBar {
                series_idx: 0,
                open: i as i64,
                high: 0,
                low: 0,
                close: 0,
                d_open: 0,
                d_high: 0,
                d_low: 0,
                d_close: 0,
                count: 0,
                vol: 0,
                open_ts: 0,
                close_ts: i as i64,
                poc_level: 0,
            });
        }
        assert_eq!(c.history[0].len(), MAX_CLOSED);
        assert_eq!(c.drop_count, 10);
    }
}
