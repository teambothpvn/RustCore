# PLAN.md — C++ -> Rust migration (step by step)

## Phase 0 — harness (1-2 days)
- [ ] Install Rust toolchain in WSL (`rustup`), `cargo test` green on `map_entry`.
- [ ] Add `criterion` benches + `cargo-miri`, `loom`, `crossbeam-queue`, `arc-swap` deps.
- [ ] Golden vectors: dump C++ `xu_ly_trade` outputs (OHLC/delta/footprint) for 10k trades -> `tests/vectors/`.

## Phase 1 — leaf modules (no deps)
1. `common` (decimal/symboltoint64) 2. `route` (core_of) 3. `policy` (4 kinds eval/update).
- Each: port + unit test + bench vs C++ logic. No threads yet.

## Phase 2 — spsc + directory (concurrency primitives)
4. `spsc` (Producer/Consumer split, CachePadded head/tail, Miri+loom).
5. `directory` (ArcSwap RCU snapshot). Test: swap under load, readers never block.

## Phase 3 — footprint radix (heaviest algorithm)
6. `footprint`: center+level+page/block/slot, lazy pages, version blocks, POC/COT O(1).
- Equivalence test vs C++ `FootprintRadixV3` on same trade stream (bit-exact POC/COT/volumes).

## Phase 4 — core hot path (integrate, NO RCU double-buffer yet per requirement)
7. `core`: SoA hot slots, active-list Vec, `fold_trade` zero-alloc. Close = build snapshot INLINE (no handoff).
- Perf gate: p50 per-trade <= C++ (criterion + perf cache-misses).

## Phase 5 — closer + hub + worker (add backpressure later)
8. `closer`: cold thread deep-copy + bounded VecDeque(200). 9. `hub`: owns rings, routes. 10. `worker`: sole-writer loop + stop flag.
- Only THEN add SPSC handoff/triple-buffer if Phase-4 inline-close shows latency spikes.

## Phase 6 — ingest + exchange + gateway
11. `ingest` parse+validate, 12. `exchange` exchangeInfo load, 13. `gateway` axum REST+WS reading via seqlock only.

## Phase 7 — parity + cutover
- [ ] 49 C++ Unitest equivalents green, sanitizer-equivalent (miri/loom) green.
- [ ] 90s demo parity: same bars as C++ binary. Then flip default binary to Rust.

## Explicitly DROPPED per your requirement
- RCU triple-buffer / double-buffer: replaced by inline close first, handoff only if measured needed.
- `unordered_map`/`HashMap` in hot path: replaced by flat Vec + mask / PHF for static symbols.
