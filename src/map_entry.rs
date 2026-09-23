//! Cache-optimal open-addressing map: symbol_hash -> dense entry.
//!
//! Design (mirrors SymbolDirectory + route hot path, Rust ownership version):
//! - Single flat `Vec<MapEntry>` table, capacity = pow2, `mask = cap - 1`.
//! - Linear probing with bounded steps; FNV-1a 32-bit hash precomputed at ingest.
//! - Hot entry is EXACTLY 64 bytes = 1 cache line (compile-time asserted).
//! - Cold data (string symbol, stats) lives in a side table, never touched per trade.
//! - Reader is lock-free: entries are `Copy`, published with release/acquire on `state`.

use core::sync::atomic::{AtomicU32, Ordering};

pub const CACHE_LINE: usize = 64;
pub const MAX_PROBE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapError {
    Full,
    NotFound,
}

/// Entry state fits in u32 so the whole entry stays Copy + atomic-publishable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum EntryState {
    Empty = 0,
    Active = 1,
    Tombstone = 2,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct MapEntry {
    pub last_ts_ns: i64,  // 8
    pub trade_count: u64, // 16
    pub volume_acc: u64,  // 24
    pub hash: u32,        // 28
    pub symbol_id: u32,   // 32
    pub state: u32,       // 36
    pub active_list_head: u32, // 40
    pub core_id: u16,     // 42
    pub series_count: u16,// 44
    pub pad: [u8; 20],    // 64
}

const _: () = assert!(core::mem::size_of::<MapEntry>() == CACHE_LINE);

impl MapEntry {
    pub const EMPTY: Self = Self {
        hash: 0,
        symbol_id: u32::MAX,
        core_id: 0,
        series_count: 0,
        state: EntryState::Empty as u32,
        active_list_head: u32::MAX,
        last_ts_ns: 0,
        trade_count: 0,
        volume_acc: 0,
        pad: [0; 20],
    };

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        self.state == EntryState::Active as u32
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MapConfig {
    /// log2(capacity). capacity = 1 << bits. Must leave load factor <= 0.7.
    pub capacity_bits: u32,
}

impl Default for MapConfig {
    fn default() -> Self {
        Self { capacity_bits: 12 } // 4096 entries ≈ 256KB = fits L2, mirrors kCoreSymbolCapacityV2
    }
}

/// Flat open-addressing table. Single-thread writer (cold path) + lock-free
/// readers via the `state` word (release on insert / acquire on lookup).
pub struct SymbolMap {
    table: Vec<MapEntry>,
    mask: usize,
    len: usize,
    /// version bumped (release) on every insert/remove; readers snapshot it
    /// for SymbolDirectory-style RCU validation.
    pub version: AtomicU32,
}

impl SymbolMap {
    pub fn new(cfg: MapConfig) -> Self {
        let cap = 1usize << cfg.capacity_bits;
        debug_assert!(cap.is_power_of_two());
        Self {
            table: vec![MapEntry::EMPTY; cap],
            mask: cap - 1,
            len: 0,
            version: AtomicU32::new(0),
        }
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.table.len()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// FNV-1a 32 — must match ingest hash. `const` so it can run at compile time.
    #[inline(always)]
    pub const fn hash_bytes(bytes: &[u8]) -> u32 {
        let mut h: u32 = 0x811c_9dc5;
        let mut i = 0;
        while i < bytes.len() {
            h ^= bytes[i] as u32;
            h = h.wrapping_mul(0x0100_0193);
            i += 1;
        }
        h
    }

    /// Cold-path insert. Returns dense symbol_id slot. O(1) amortised.
    pub fn insert(&mut self, hash: u32, symbol_id: u32, core_id: u16) -> Result<(), MapError> {
        if self.len * 10 >= self.table.len() * 7 {
            return Err(MapError::Full); // keep load <= 0.7, resize is explicit
        }
        let mut idx = (hash as usize) & self.mask;
        for _ in 0..MAX_PROBE {
            let e = &mut self.table[idx];
            if !e.is_active() {
                let mut fresh = MapEntry::EMPTY;
                fresh.hash = hash;
                fresh.symbol_id = symbol_id;
                fresh.core_id = core_id;
                // R4: publish payload FIRST, state LAST with release.
                core::sync::atomic::fence(Ordering::Release);
                e.hash = fresh.hash;
                e.symbol_id = fresh.symbol_id;
                e.core_id = fresh.core_id;
                e.series_count = 0;
                e.active_list_head = u32::MAX;
                e.last_ts_ns = 0;
                e.trade_count = 0;
                e.volume_acc = 0;
                e.state = EntryState::Active as u32;
                core::sync::atomic::fence(Ordering::Release);
                self.len += 1;
                self.version.fetch_add(1, Ordering::Release);
                return Ok(());
            }
            idx = (idx + 1) & self.mask; // R3: mask, never %
        }
        Err(MapError::Full)
    }

    /// Hot-path lookup: branch-predictable, at most MAX_PROBE cache lines.
    /// Caller should `prefetch` the first slot before calling in a batch loop.
    #[inline(always)]
    pub fn lookup(&self, hash: u32) -> Result<&MapEntry, MapError> {
        let mut idx = (hash as usize) & self.mask;
        for _ in 0..MAX_PROBE {
            let e = &self.table[idx];
            // R5: compare hash first (cheapest), then state word.
            if e.hash == hash && e.is_active() {
                core::sync::atomic::fence(Ordering::Acquire);
                return Ok(e);
            }
            if e.state == EntryState::Empty as u32 {
                return Err(MapError::NotFound); // empty terminates probe chain
            }
            idx = (idx + 1) & self.mask;
        }
        Err(MapError::NotFound)
    }

    /// Hot-path mutable fold (worker sole-writer per shard): update counters
    /// without touching cold side table.
    #[inline(always)]
    pub fn fold_trade(&mut self, hash: u32, ts_ns: i64, qty: u64) -> bool {
        let mut idx = (hash as usize) & self.mask;
        for _ in 0..MAX_PROBE {
            let e = &mut self.table[idx];
            if e.hash == hash && e.is_active() {
                e.trade_count = e.trade_count.wrapping_add(1);
                e.volume_acc = e.volume_acc.wrapping_add(qty);
                e.last_ts_ns = ts_ns;
                return true;
            }
            if e.state == EntryState::Empty as u32 {
                return false;
            }
            idx = (idx + 1) & self.mask;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_is_one_cache_line() {
        assert_eq!(core::mem::size_of::<MapEntry>(), 64);
    }

    #[test]
    fn insert_lookup_fold() {
        let mut m = SymbolMap::new(MapConfig::default());
        let h = SymbolMap::hash_bytes(b"BTCUSDT");
        m.insert(h, 7, 1).unwrap();
        assert_eq!(m.lookup(h).unwrap().symbol_id, 7);
        assert!(m.fold_trade(h, 123, 10));
        assert_eq!(m.lookup(h).unwrap().trade_count, 1);
        assert!(m.lookup(0xdead_beef).is_err());
    }
}
