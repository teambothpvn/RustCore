//! Worker: sole consumer of exactly ONE SPSC + Router stamping dense_id.
//! ROUTER-1/2 enforced: Router::dispatch stamps dense then pushes to producers[target].

use crate::core::Core;
use crate::directory::{core_of, Snapshot};
use crate::ingest::Trade;
use crate::spsc::{Consumer, Producer, BATCH};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Router {
    pub producers: Vec<Producer<Trade>>,
    pub snapshot: Arc<Snapshot>,
    pub core_mask: u32,
}

impl Router {
    pub fn new(producers: Vec<Producer<Trade>>, snapshot: Arc<Snapshot>) -> Self {
        let mask = (producers.len() as u32).wrapping_sub(1);
        debug_assert!(producers.len().is_power_of_two());
        Self { producers, snapshot, core_mask: mask }
    }

    /// Stamp dense_id then deliver to exactly ONE core queue. Returns core index or None.
    #[inline]
    pub fn dispatch(&self, t: &mut Trade) -> Option<usize> {
        let d = self.snapshot.dense_of(t.symbol_hash)?;
        t.dense_id = d;
        let c = core_of(d, self.core_mask) as usize;
        let n = self.producers[c].try_push_batch(std::slice::from_ref(t));
        if n == 1 {
            Some(c)
        } else {
            None
        }
    }

    pub fn update_snapshot(&mut self, s: Arc<Snapshot>) {
        self.snapshot = s;
    }

    pub fn into_producers(self) -> Vec<Producer<Trade>> {
        self.producers
    }
}

pub struct Worker {
    pub core_id: u32,
    pub core: Core,
    pub consumer: Consumer<Trade>,
    pub stop: Arc<AtomicBool>,
}

impl Worker {
    /// Batch drain loop: 1 acquire+release per 64, fold on local buf.
    pub fn run(&mut self) {
        let mut buf = [crate::ingest::EMPTY_TRADE; BATCH];
        let mut spins = 0u32;
        loop {
            if self.stop.load(Ordering::Relaxed) {
                // drain remainder before exit — never drop queued trades
                loop {
                    let n = self.consumer.try_pop_batch(&mut buf);
                    if n == 0 {
                        break;
                    }
                    for t in &buf[..n] {
                        if !t.valid {
                            continue;
                        }
                        let is_buy = !t.is_buyer_maker;
                        self.core.fold_active(t.price, t.qty, t.ts_ns, is_buy);
                    }
                }
                break;
            }
            let n = self.consumer.try_pop_batch(&mut buf);
            if n == 0 {
                spins += 1;
                if spins < 1000 {
                    std::hint::spin_loop();
                } else if spins < 2000 {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(std::time::Duration::from_micros(50));
                    spins = 0;
                }
                continue;
            }
            spins = 0;
            for t in &buf[..n] {
                if !t.valid {
                    continue;
                }
                let is_buy = !t.is_buyer_maker;
                self.core.fold_active(t.price, t.qty, t.ts_ns, is_buy);
            }
        }
    }

    /// Single-step drain for tests (no parking).
    pub fn drain_once(&mut self) -> usize {
        let mut buf = [crate::ingest::EMPTY_TRADE; BATCH];
        let n = self.consumer.try_pop_batch(&mut buf);
        for t in &buf[..n] {
            if t.valid {
                self.core.fold_active(t.price, t.qty, t.ts_ns, !t.is_buyer_maker);
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::Directory;
    use crate::ingest::parse_trade_payload;
    use crate::spsc::Ring;

    #[test]
    fn e2e_symbol_to_one_core() {
        // 2 cores, 2 symbols -> each symbol lands on exactly one queue.
        let (p0, c0) = Ring::<Trade>::with_capacity_pow2(4);
        let (p1, c1) = Ring::<Trade>::with_capacity_pow2(4);
        let mut dir = Directory::new_pow2(4);
        let j1 = r#"{"s":"BTCUSDT","p":"100.0","q":"1.0","T":1000,"m":false}"#;
        let j2 = r#"{"s":"ETHUSDT","p":"50.0","q":"2.0","T":1000,"m":true}"#;
        let h1 = parse_trade_payload(j1, 10).symbol_hash;
        let h2 = parse_trade_payload(j2, 10).symbol_hash;
        let d1 = dir.intern(h1);
        let d2 = dir.intern(h2);
        let snap = dir.snapshot();
        let router = Router::new(vec![p0, p1], snap);
        let mut t1 = parse_trade_payload(j1, 10);
        let mut t2 = parse_trade_payload(j2, 10);
        let c_1 = router.dispatch(&mut t1).unwrap();
        let c_2 = router.dispatch(&mut t2).unwrap();
        assert_eq!(c_1, (d1 & 1) as usize);
        assert_eq!(c_2, (d2 & 1) as usize);
        assert_eq!(t1.dense_id, d1);
        // workers drain their own queue only
        let stop = Arc::new(AtomicBool::new(false));
        let mut w0 = Worker { core_id: 0, core: Core::new(4), consumer: c0, stop: Arc::clone(&stop) };
        let mut w1 = Worker { core_id: 1, core: Core::new(4), consumer: c1, stop };
        w0.core.active_mask = 0xF;
        w1.core.active_mask = 0xF;
        let n0 = w0.drain_once();
        let n1 = w1.drain_once();
        assert_eq!(n0 + n1, 2); // no loss, no duplication
    }
}
