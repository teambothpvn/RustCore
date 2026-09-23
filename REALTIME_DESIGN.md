# RustCore realtime redesign — 1 core-thread : N symbols, 1 symbol → 1 thread duy nhất

## Bất biến cốt lõi (compiler ép, không phải comment)

- Mỗi `symbol_id` thuộc đúng 1 `CoreShard`. `CoreShard` là `!Sync` (không share giữa thread).
  Muốn gửi trade vào shard khác phải qua SPSC — không có đường tắt.
- Worker là sole-owner của mọi `SeriesState`. Reader chỉ thấy snapshot qua seqlock copy.
- Hot path: 0 alloc, 0 HashMap, 0 String, 0 SeqCst, 0 dyn, 0 lock.

## Layout

- `SymbolTable`: Vec phẳng `SymbolSlot` 64B, index = `hash & mask`, linear probe ≤ 8.
- Mỗi slot: `state: AtomicU32` (Empty/Filling/Active), `symbol_hash`, `series_count`,
  `series_head: u32` (index vào SeriesPool), padding tới 64B.
- `SeriesPool`: SoA — 4 Vec song song `open/high/low/close: Vec<i64>`,
  `vol, trades, delta_o/h/l/c: Vec<i64>`, `policy_kind: Vec<u8>`, `threshold: Vec<i64>`.
  Mỗi series là 1 hàng, scan/fold chạm đúng line cần.
- `FootprintPool`: page-table lazy `Option<Box<Page>>>`, page 4KB, block 64 level.
- Ingress: 1 SPSC RX mỗi shard (`rtrb` / `crossbeam ArrayQueue` hoặc ring tự viết).
  Router ở thread ingest làm duy nhất `shard = hash & (N-1)` rồi `push`.
  Vì hash(symbol) cố định → symbol không bao giờ vào 2 thread.

## Thuật toán hot-path mới (thay active-list C++)

```
for batch in rx.pop_burst(64):
    prefetch(table[hash_next])
    slot = lookup_inline(hash)      // mask + probe ≤8, branchless
    s = series_head[slot]
    fold_soa(s, price, qty, side)   // O(1), cmov max/min, atomic không có
    if crossed(threshold[s]): close_inline(s) // swap buffer index, push close-event SPSC TX
```

- Batch 64: chia đều cost acquire/fence, prefetch có tác dụng.
- `close` không copy footprint: chỉ flip `hot_idx ^ 1`, gửi index cũ qua TX cho closer.
- Reader seqlock: `seq odd? retry : copy 64B + fence + recheck`.

## Thủ thuật Rust áp dụng

- `#[repr(C,align(64))]`, `CachePadded<AtomicU32>`, assert size compile-time.
- `Ordering::Relaxed` cho counter worker-private, `Acquire/Release` cho handoff, cấm SeqCst.
- `#[inline(always)]` fold, `max/min` thay if (cmov), mask thay %.
- `MaybeUninit` + `UnsafeCell` chỉ trong ring/page alloc, ngoài safe 100%.
- `loom` test interleave SPSC, `miri` test page lifetime.
