//! Hub: owns per-core SPSC rings + cores + closer. Routes by dense_id.
//! Each symbol -> exactly one core (core_of). Hub holds Producers, workers hold Consumers.

use crate::closer::{Closer, ClosedBar};
use crate::core::Core;
use crate::directory::{core_of, Directory};
use crate::ingest::Trade;
use crate::spsc::{Consumer, Producer, Ring};
use crate::worker::{Router, Worker};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

pub struct Hub {
    pub num_cores: usize,
    pub cores: Vec<Core>,
    pub closer: Closer,
    producers: Vec<Producer<Trade>>,
    consumers: Vec<Option<Consumer<Trade>>>,
    stop: Arc<AtomicBool>,
    pub dir: Directory,
}

impl Hub {
    pub fn new(num_cores: usize, slots_per_core: usize, num_series: usize) -> Self {
        assert!(num_cores.is_power_of_two());
        let mut producers = Vec::with_capacity(num_cores);
        let mut consumers: Vec<Option<Consumer<Trade>>> = Vec::with_capacity(num_cores);
        for _ in 0..num_cores {
            let (p, c) = Ring::<Trade>::with_capacity_pow2(10); // 1024 depth
            producers.push(p);
            consumers.push(Some(c));
        }
        Self {
            num_cores,
            cores: (0..num_cores).map(|_| Core::new(slots_per_core)).collect(),
            closer: Closer::new(num_series),
            producers,
            consumers,
            stop: Arc::new(AtomicBool::new(false)),
            dir: Directory::new_pow2(12),
        }
    }

    /// Cold: subscribe symbol string -> dense id (stable forever).
    pub fn subscribe(&mut self, symbol: &str) -> u32 {
        let h = crate::ingest::hash_symbol(symbol.as_bytes());
        self.dir.intern(h)
    }

    #[inline]
    pub fn core_of(&self, dense: u32) -> usize {
        core_of(dense, (self.num_cores as u32) - 1) as usize
    }

    /// Ingest one parsed trade: stamp dense via snapshot then push to exactly one queue.
    #[inline]
    pub fn ingest(&mut self, t: &mut Trade) -> Option<usize> {
        let snap = self.dir.snapshot();
        let router = Router::new(std::mem::take(&mut self.producers), snap);
        let r = router.dispatch(t);
        self.producers = router.into_producers();
        r
    }

    pub fn close_series_bar(&mut self, core_idx: usize, series_idx: usize, poc: i64) {
        let slot = &self.cores[core_idx].slots[series_idx % self.cores[core_idx].slots.len()];
        let bar = ClosedBar::from_slot(series_idx, slot, poc);
        self.closer.accept(bar);
    }

    pub fn stop_workers(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn spawn_worker(&mut self, core_idx: usize) -> Worker {
        let consumer = self.consumers[core_idx].take().expect("worker already spawned");        let mut core = Core::new(self.cores[core_idx].slots.len());
        core.active_mask = self.cores[core_idx].active_mask;
        Worker { core_id: core_idx as u32, core, consumer, stop: Arc::clone(&self.stop) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::parse_trade_payload;
    #[test]
    fn hub_routes_one_core() {
        let mut hub = Hub::new(2, 4, 4);
        let d = hub.subscribe("BTCUSDT");
        assert_eq!(hub.core_of(d), (d & 1) as usize);
        let mut t = parse_trade_payload(r#"{"s":"BTCUSDT","p":"100.0","q":"2.0","T":1000,"m":false}"#, 10);
        // stamp hash matches subscribed symbol
        t.symbol_hash = crate::ingest::hash_symbol(b"BTCUSDT");
        let c = hub.ingest(&mut t).unwrap();
        assert_eq!(c, hub.core_of(d));
    }
}
