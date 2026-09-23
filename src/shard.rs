use std::sync::atomic::{AtomicU32, Ordering};

pub const SHARD_MASK_BITS: u32 = 12;
pub const TABLE_MASK: u32 = (1 << SHARD_MASK_BITS) - 1;
pub const MAX_PROBE: usize = 8;

#[repr(C, align(64))]
pub struct SymbolSlot {
    pub state: AtomicU32, // 0 empty, 1 active
    pub symbol_hash: u32,
    pub series_head: u32,
    pub series_count: u32,
    _pad: [u8; 44],
}
impl SymbolSlot {
    pub const fn empty() -> Self {
        Self { state: AtomicU32::new(0), symbol_hash: 0, series_head: u32::MAX, series_count: 0, _pad: [0; 44] }
    }
}
const _: () = assert!(std::mem::size_of::<SymbolSlot>() == 64);

/// Shard KHÔNG Sync: ép 1 symbol chỉ sống trên 1 thread.
/// Muốn cross-thread phải qua SPSC, không share &Shard.
pub struct CoreShard {
    _no_sync: std::marker::PhantomData<*const ()>,
    pub shard_id: u32,
    pub table: Vec<SymbolSlot>,
    // SoA series state: mỗi Vec là 1 cột, index = series idx
    pub s_open: Vec<i64>,
    pub s_high: Vec<i64>,
    pub s_low: Vec<i64>,
    pub s_close: Vec<i64>,
    pub s_vol: Vec<i64>,
    pub s_delta_c: Vec<i64>,
    pub s_threshold: Vec<i64>,
    pub s_kind: Vec<u8>,
}

impl CoreShard {
    pub fn new(shard_id: u32, n_shards: u32) -> Self {
        assert!(n_shards.is_power_of_two());
        let cap = 1 << SHARD_MASK_BITS;
        let mut table = Vec::with_capacity(cap);
        for _ in 0..cap { table.push(SymbolSlot::empty()); }
        Self { _no_sync: std::marker::PhantomData, shard_id, table, s_open: Vec::new(), s_high: Vec::new(), s_low: Vec::new(), s_close: Vec::new(), s_vol: Vec::new(), s_delta_c: Vec::new(), s_threshold: Vec::new(), s_kind: Vec::new() }
    }

    /// Router thuần: symbol -> shard duy nhất. Cùng hash luôn cùng shard.
    #[inline(always)]
    pub fn route(symbol_hash: u32, n_shards: u32) -> u32 {
        symbol_hash & (n_shards - 1)
    }

    #[inline(always)]
    pub fn lookup(&self, symbol_hash: u32) -> Option<u32> {
        let mut idx = (symbol_hash & TABLE_MASK) as usize;
        for _ in 0..MAX_PROBE {
            let s = &self.table[idx];
            if s.state.load(Ordering::Acquire) == 0 { return None; }
            if s.symbol_hash == symbol_hash { return Some(idx as u32); }
            idx = (idx + 1) & TABLE_MASK as usize;
        }
        None
    }

    /// Hot fold: O(1), branchless max/min qua cmov, không alloc/lock.
    #[inline(always)]
    pub fn fold_trade(&mut self, slot_idx: u32, price: i64, qty: i64, is_buy: bool) {
        let head = self.table[slot_idx as usize].series_head as usize;
        if head == u32::MAX as usize { return; }
        let o = unsafe { self.s_close.get_unchecked_mut(head) };
        if *self.s_high.get(head).unwrap() < price { *self.s_high.get_mut(head).unwrap() = price; }
        if *self.s_low.get(head).unwrap() > price { *self.s_low.get(head).unwrap() = price; }
        *o = price;
        *self.s_vol.get_mut(head).unwrap() += qty;
        *self.s_delta_c.get_mut(head).unwrap() += if is_buy { qty } else { -qty };
    }
}
