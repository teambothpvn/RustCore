//! Directory: hash -> dense id interning (cold) + lock-free snapshot (hot).
//! Invariant ROUTER-1: each symbol_hash maps to exactly ONE dense_id forever
//! (until explicit remove). Invariant ROUTER-2: dense_id % num_cores is stable,
//! so each symbol always lands on exactly ONE core/thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Default)]
pub struct Snapshot {
    /// flat table pow2: slot = hash & mask; 0 = empty, else dense_id+1
    pub table: Arc<Vec<u32>>,
    /// parallel hashes for collision verification during probe
    pub hashes: Arc<Vec<u32>>,
    pub mask: u32,
    pub num_symbols: u32,
}

pub struct Directory {
    // cold mutable interning map (only touched on subscribe/unsubscribe, under lock)
    map: std::collections::HashMap<u32, u32>,
    next_dense: u32,
    snapshot: Arc<Snapshot>,
    version: AtomicU64,
}

impl Directory {
    pub fn new_pow2(bits: u32) -> Self {
        let cap = 1usize << bits;
        Self {
            map: std::collections::HashMap::new(),
            next_dense: 0,
            snapshot: Arc::new(Snapshot {
                table: Arc::new(vec![0; cap]),
                hashes: Arc::new(vec![0; cap]),
                mask: (cap as u32) - 1,
                num_symbols: 0,
            }),
            version: AtomicU64::new(0),
        }
    }

    /// Cold: intern hash -> dense (idempotent). Rebuilds flat snapshot once.
    pub fn intern(&mut self, hash: u32) -> u32 {
        if let Some(&d) = self.map.get(&hash) {
            return d;
        }
        let d = self.next_dense;
        self.next_dense += 1;
        self.map.insert(hash, d);
        self.rebuild();
        d
    }

    fn rebuild(&mut self) {
        let cap = self.snapshot.table.len();
        let mut t = vec![0u32; cap];
        let mut hh = vec![0u32; cap];
        let mask = (cap as u32) - 1;
        for (&h, &d) in self.map.iter() {
            let mut i = (h & mask) as usize;
            loop {
                if t[i] == 0 {
                    t[i] = d + 1;
                    hh[i] = h;
                    break;
                }
                i = (i + 1) & (cap - 1);
            }
        }
        self.snapshot = Arc::new(Snapshot {
            table: Arc::new(t),
            hashes: Arc::new(hh),
            mask,
            num_symbols: self.map.len() as u32,
        });
        self.version.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub fn snapshot(&self) -> Arc<Snapshot> {
        Arc::clone(&self.snapshot)
    }
}

impl Snapshot {
    /// Hot: hash -> dense in one flat probe (no HashMap, no String).
    #[inline]
    pub fn dense_of(&self, hash: u32) -> Option<u32> {
        let mut i = (hash & self.mask) as usize;
        for _ in 0..8 {
            let v = self.table[i];
            if v == 0 {
                return None; // empty terminates probe chain
            }
            if self.hashes[i] == hash {
                return Some(v - 1);
            }
            i = (i + 1) & (self.mask as usize);
        }
        None
    }
}

/// Route: each dense id belongs to exactly ONE core. N must be pow2 for mask.
#[inline(always)]
pub fn core_of(dense_id: u32, num_cores_pow2_mask: u32) -> u32 {
    dense_id & num_cores_pow2_mask
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn one_symbol_one_core() {
        let mut d = Directory::new_pow2(4);
        let a = d.intern(123);
        let b = d.intern(456);
        assert_eq!(d.intern(123), a); // stable
        let snap = d.snapshot();
        assert_eq!(snap.dense_of(123), Some(a));
        assert_eq!(snap.dense_of(456), Some(b));
        // ROUTER-2: stable core
        let mask = 3u32; // 4 cores
        assert_eq!(core_of(a, mask), a & mask);
    }
}
