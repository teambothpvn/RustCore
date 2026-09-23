//! Footprint radix: page-table lookup + per-core page pool + per-block bitmap.
//! Decode: level = floor_div(price-center, tick); page = level>>10; block = (level>>6)&15; slot = level&63.

pub const PAGES: usize = 128;
pub const BLOCKS: usize = 16;
pub const SLOTS: usize = 64;

#[derive(Clone, Copy, Default)]
pub struct Level {
    pub buy_vol: i64,
    pub sell_vol: i64,
    pub count: u32,
}

pub struct Block {
    pub version: u32,
    pub non_empty: u64, // bit i = slot i has data
    pub levels: [Level; SLOTS],
}

pub struct PageData {
    pub blocks: [Block; BLOCKS],
}

pub struct PagePool {
    free: Vec<Box<PageData>>,
}

pub struct Footprint {
    pub center: i64,
    pub tick: i64,
    pub page_table: [Option<usize>; PAGES], // index into `pages`
    pub pages: Vec<Box<PageData>>,
    pub pool: PagePool,
    pub poc_level: i64,
    pub poc_vol: i64,
    pub version: u32,
}

fn floor_div(a: i64, b: i64) -> i64 {
    let d = a / b;
    let r = a % b;
    if r != 0 && ((r < 0) != (b < 0)) {
        d - 1
    } else {
        d
    }
}

impl PagePool {
    pub fn new() -> Self {
        Self { free: Vec::with_capacity(64) }
    }
    fn get(&mut self) -> Box<PageData> {
        self.free.pop().unwrap_or_else(|| {
            let blocks: [Block; BLOCKS] = std::array::from_fn(|_| Block {
                version: 0,
                non_empty: 0,
                levels: [Level::default(); SLOTS],
            });
            Box::new(PageData { blocks })
        })
    }
    fn put(&mut self, mut p: Box<PageData>) {
        for b in p.blocks.iter_mut() {
            b.version = 0;
            b.non_empty = 0;
        }
        if self.free.len() < 64 {
            self.free.push(p);
        }
    }
}

impl Footprint {
    pub fn new(center: i64, tick: i64) -> Self {
        Self {
            center,
            tick: tick.max(1),
            page_table: [None; PAGES],
            pages: Vec::new(),
            pool: PagePool::new(),
            poc_level: 0,
            poc_vol: -1,
            version: 0,
        }
    }

    #[inline(always)]
    pub fn add(&mut self, price: i64, qty: i64, is_buy: bool) {
        let level = floor_div(price - self.center, self.tick);
        let page = ((level >> 10) & (PAGES as i64 - 1)) as usize;
        let block = ((level >> 6) & 15) as usize;
        let slot = (level & 63) as usize;
        let pi = match self.page_table[page] {
            Some(i) => i,
            None => {
                let p = self.pool.get();
                self.pages.push(p);
                let i = self.pages.len() - 1;
                self.page_table[page] = Some(i);
                i
            }
        };
        let b = &mut self.pages[pi].blocks[block];
        self.version += 1;
        b.version = self.version;
        b.non_empty |= 1u64 << slot;
        let lv = &mut b.levels[slot];
        if is_buy {
            lv.buy_vol += qty;
        } else {
            lv.sell_vol += qty;
        }
        lv.count += 1;
        let tot = lv.buy_vol + lv.sell_vol;
        if tot > self.poc_vol {
            self.poc_vol = tot;
            self.poc_level = level;
        }
    }

    /// Emit only non-empty slots via bitmap walk (sparse, not full 64 scan copy).
    pub fn deltas_since(&self, out: &mut Vec<(i64, Level)>, last_version: u32) {
        out.clear();
        for pg in self.pages.iter() {
            for b in pg.blocks.iter() {
                if b.version <= last_version {
                    continue;
                }
                let mut bits = b.non_empty;
                while bits != 0 {
                    let s = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    out.push((s as i64, b.levels[s]));
                }
            }
        }
    }

    pub fn reset(&mut self) {
        for p in self.pages.drain(..) {
            self.pool.put(p);
        }
        self.page_table = [None; PAGES];
        self.poc_vol = -1;
        self.version = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sparse_emit() {
        let mut fp = Footprint::new(1000, 10);
        fp.add(1000, 5, true);
        fp.add(1000, 3, false);
        let mut out = Vec::new();
        fp.deltas_since(&mut out, 0);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1.buy_vol, 5);
    }
}
