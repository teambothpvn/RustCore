//! Bar policies: pure eval/update/open per kind. No alloc, branchless fold helpers.

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BarKind {
    Time,
    Range,
    Volume,
    Delta,
}

#[derive(Default, Clone, Copy)]
pub struct PolicyState {
    pub open_price: i64,
    pub open_ts: i64,
    pub close_ts: i64,
    pub open_vol: i64,
    pub open_delta: i64,
    pub acc_vol: i64,
    pub acc_delta: i64,
    pub hi: i64,
    pub lo: i64,
    pub has_bar: bool,
}

#[inline(always)]
pub fn should_close(kind: BarKind, threshold: i32, tick: i64, s: &PolicyState, ts: i64, price: i64, qty: i64) -> bool {
    match kind {
        BarKind::Time => ts - s.open_ts >= threshold as i64,
        BarKind::Range => s.hi.max(price) - s.lo.min(price) >= threshold as i64 * tick,
        BarKind::Volume => s.acc_vol + qty >= threshold as i64,
        BarKind::Delta => {
            let d = s.acc_delta + qty;
            d.max(s.hi).min(s.lo).abs() >= threshold as i64
        }
    }
}
