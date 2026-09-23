//! Core hot path: SoA flat slots, bitmask dispatch, branchless OHLC+delta fold.
//! Inline close (no RCU handoff in Phase 4). Seqlock bracket covers ONLY mutation.

use crate::footprint::Footprint;

#[repr(C, align(64))]
pub struct HotSlot {
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
    pub has_bar: bool,
    _pad: [u8; 7],
}
const _: () = assert!(std::mem::size_of::<HotSlot>() == 128);

pub struct Core {
    pub slots: Vec<HotSlot>,
    pub active_mask: u32,
    pub fps: Vec<Option<Footprint>>,
}

impl Core {
    pub fn new(n: usize) -> Self {
        Self {
            slots: (0..n)
                .map(|_| HotSlot {
                    open: 0,
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
                    close_ts: 0,
                    has_bar: false,
                    _pad: [0; 7],
                })
                .collect(),
            active_mask: 0,
            fps: (0..n).map(|_| None).collect(),
        }
    }

    /// Fold one trade into slot `i`. Branchless after first-trade init.
    #[inline(always)]
    pub fn fold_trade(&mut self, i: usize, price: i64, qty: i64, ts: i64, is_buy: bool) {
        let s = &mut self.slots[i];
        if !s.has_bar {
            s.open = price;
            s.high = price;
            s.low = price;
            s.close = price;
            let q = if is_buy { qty } else { -qty };
            s.d_open = q;
            s.d_high = q;
            s.d_low = q;
            s.d_close = q;
            s.count = 1;
            s.vol = qty;
            s.open_ts = ts;
            s.close_ts = ts;
            s.has_bar = true;
        } else {
            s.high = s.high.max(price);
            s.low = s.low.min(price);
            s.close = price;
            let q = if is_buy { qty } else { -qty };
            s.d_close += q;
            s.d_high = s.d_high.max(s.d_close);
            s.d_low = s.d_low.min(s.d_close);
            s.count += 1;
            s.vol += qty;
            s.close_ts = ts;
        }
        if let Some(fp) = self.fps[i].as_mut() {
            fp.add(price, qty, is_buy);
        }
    }

    /// Dispatch over active bitmask with ctz (no active_count atomic, no thunk ptr).
    #[inline]
    pub fn fold_active(&mut self, price: i64, qty: i64, ts: i64, is_buy: bool) {
        let mut mask = self.active_mask;
        while mask != 0 {
            let i = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            self.fold_trade(i, price, qty, ts, is_buy);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fold_ohlc_delta() {
        let mut c = Core::new(2);
        c.active_mask = 0b01;
        c.fold_active(100, 5, 1, true);
        c.fold_active(110, 3, 2, false);
        assert_eq!(c.slots[0].high, 110);
        assert_eq!(c.slots[0].d_close, 2);
    }
}
