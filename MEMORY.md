# MEMORY.md — cache/struct principles (Rust = C++)

## Layout
- L1: every hot struct `#[repr(C, align(64))]`, `size_of % 64 == 0` test. Field order 8B->4B->2B->1B. Bools -> `u32` bitmask.
- L2: SoA for scans (`prices: Vec<i64>` not `Vec<Trade>`); hot/cold split (`HotBar` 64B / `ColdCfg` elsewhere).
- L3: flat `Vec` pow2 cap + `idx & mask`; open-addressing linear probe MAX_PROBE=8, load<=0.7; small maps (<32) use linear search, not hash.
- L4: footprint radix lazy pages `Option<Box<[Block;16]>>`; closed history `VecDeque` bounded append-only (close_ts monotonic, no ordered insert).

## Blocking/Tiling/Prefetch
- Tiling: any scan >L1 (32KB) tiled to fit L1; batch trades 64-256 per fence/push.
- Prefetch: `_mm_prefetch(ptr, _MM_HINT_T0)` 4-8 iters ahead ONLY for predictable streams; measure with `perf stat cache-misses`.
- Branchless: `max/min`, `cmov` via `Ord::max`, masks; fast-path first; no `likely` on stable Rust (use PGO `-Cprofile-generate/use`).

## Atomics/false-sharing
- `CachePadded<T>(T)` for every atomic touched by 2 threads (head/tail, seqlock seq). Ban `SeqCst` in hot path (grep CI).
- Seqlock: writer `fetch_add(1, Relaxed)` x2 + `fence(Release)`; reader `load(Acquire)` + retry<=64 with `spin_loop_hint` backoff.
- RCU: `arc-swap::ArcSwap<Dir>`; per-core sharding `id & (N-1)`; pin worker to core.

## Alloc/TLB
- Zero-alloc hot: `&mut Vec` reuse (thread-local), arena `bumpalo` for temp copies, pool retired `Box<FullBar>`.
- Huge pages for tables >MB; NUMA-local alloc; `perf` validation mandatory.
