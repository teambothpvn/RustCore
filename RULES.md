# RustCore — Cursor Rules (authoritative)

## 1. Entry points (C++ -> Rust)
| # | C++ entry | Rust target | Notes |
|---|---|---|---|
| E1 | `engine/src/main.cpp` (demo CLI) | `rust-core/src/main.rs` + `apps/demo` | parse args, load exchangeInfo, run demo_seconds |
| E2 | `BnStreamAdapter3::parse_trade_payload` (simdjson) | `ingest::parse_trade_payload` (serde_json/simd-json) | pure fn, testable seam |
| E3 | `BarEngineCoreV2::xu_ly_trade` | `core::Core::fold_trade` | THE hot path |
| E4 | `process_one_series<RuntimeT>` + active-list thunk | `core::fold_one_series` monomorphized per `BarKind` | no dyn, no vtable |
| E5 | `BarCloserV2` cold thread + `accept_full_closed_snapshot` | `closer::Closer` thread + mpsc | owns deep-copy |
| E6 | Gateway REST `/api/*` + `/ws` push | `gateway` (axum) thread, seqlock reads only | never touches hot mutex |
| E7 | `WorkerController` spin loop + stop_token | `worker::Worker` + `AtomicBool`/`CancellationToken` | 1 worker : 1 core, pin affinity |

## 2. Modules (C++ -> Rust crate layout)
```
rust-core/src/
  ingest/   <- stream/bn_stream_3 + common/decimal_utils + symboltoint64
  directory/<- directory/symbol_directory (ArcSwap RCU)
  route/    <- route/route (core_of)
  spsc/     <- spsc/spsc_ring + spsc_queue_per_core (Producer/Consumer split)
  policy/   <- engine_v2/bar_policy_v2 (Time/Range/Volume/Delta, const eval)
  footprint/<- engine_v2/footprint_radix_v3 (radix page table, lazy pages)
  core/     <- engine_v2/bar_engine_core_v2 + types (SoA hot, active-list)
  closer/   <- engine_v2/bar_closer_v2 (cold deep-copy + bounded ring)
  hub/      <- engine_v2/bar_engine_hub_v2 (owns rings, routes)
  worker/   <- worker/worker (sole-writer loop)
  gateway/  <- gateway/gateway_v1 + client/sv_client_request
  exchange/ <- sv_exchange/sv_exchange (exchangeInfo load)
```

## 3. Algorithms per module
- ingest: simd parse -> validate price/qty/ts>0 -> hash symbol -> stamp dense id (RCU snapshot).
- directory: immutable snapshot + atomic swap (RCU); reader holds Arc.
- route: `core = id & (N-1)` (N pow2) else `% N`; static, no rebalance.
- spsc: head/tail cacheline-split, mask, 1-slot-waste, batch drain.
- policy: pure `eval/update/open` per kind, no alloc.
- footprint: `level=floor_div(price-center,tick)` once; page/block/slot decode by shifts; lazy page alloc; per-block version++; POC/COT O(1).
- core: active-list dispatch (N~1) -> OHLC+delta fold -> footprint.update -> policy.update -> maybe close.
- closer: SPSC handoff Box<FullBar> -> deep-copy -> push bounded VecDeque(200) -> return ownership.
- gateway: seqlock even/odd read + version-block delta push.

## R-HOT (hot-path law, must pass CI grep)
- R1: `fold_trade` = 0 alloc, 0 String, 0 HashMap, 0 mutex, 0 dyn, 0 SeqCst.
- R2: no `%` in hot path (mask only); no virtual; `#[inline(always)]` on fold/update.
- R3: seqlock bracket covers ONLY bar_core mutation + index flip. SPSC push OUTSIDE.
- R4: every hot struct has `size_of` test; atomics wrapped in `CachePadded`.
- R5: every `unsafe` has `// SAFETY:` + Miri test.

## R-MEM (memory/cache, detail in MEMORY.md)
- M1 hot/cold split, SoA for scans, 64B pack, bitmask > bools.
- M2 flat Vec, pow2 cap, lazy pages, arena for temp, huge pages for big tables.
- M3 prefetch only batched predictable streams; batch 64-256 trades per fence/syscall.

## R-STYLE (detail in STYLE.md)
- English infra / Vietnamese domain identifiers preserved (`dong_bar`, `gia`, `khoi_luong`).
- Functions <40 lines, early-return, no nesting >3. Errors via `Result`, never panic in lib.
- Doc comment on every `pub fn` with complexity (O(1)/O(N)) + safety contract.
