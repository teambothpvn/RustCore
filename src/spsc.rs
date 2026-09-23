use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

#[repr(align(64))]
struct CachePadded<T>(T);

pub const BATCH: usize = 64;

pub struct Producer<T: Copy> {
    ring: *mut Ring<T>,
}
pub struct Consumer<T: Copy> {
    ring: *mut Ring<T>,
}

pub struct Ring<T: Copy> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: u64,
    head: CachePadded<AtomicU64>,
    _pad0: [u8; 64],
    tail: CachePadded<AtomicU64>,
    _pad1: [u8; 64],
    overflow: CachePadded<AtomicU64>,
}

unsafe impl<T: Copy + Send> Send for Ring<T> {}
unsafe impl<T: Copy + Send> Sync for Ring<T> {}
unsafe impl<T: Copy + Send> Send for Producer<T> {}
unsafe impl<T: Copy + Send> Send for Consumer<T> {}

impl<T: Copy> Ring<T> {
    pub fn with_capacity_pow2(bits: u32) -> (Producer<T>, Consumer<T>) {
        let cap = 1usize << bits;
        let mut v: Vec<UnsafeCell<MaybeUninit<T>>> = Vec::with_capacity(cap);
        v.resize_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));
        let ring = Box::new(Self {
            buf: v.into_boxed_slice(),
            mask: (cap as u64) - 1,
            head: CachePadded(AtomicU64::new(0)),
            _pad0: [0; 64],
            tail: CachePadded(AtomicU64::new(0)),
            _pad1: [0; 64],
            overflow: CachePadded(AtomicU64::new(0)),
        });
        let ptr = Box::into_raw(ring);
        (Producer { ring: ptr }, Consumer { ring: ptr })
    }
}

// SAFETY: single producer pushes, single consumer pops; slots handed off via
// head/tail acquire/release. Batch APIs do exactly one acquire + one release.
impl<T: Copy> Producer<T> {
    #[inline]
    pub fn try_push_batch(&self, items: &[T]) -> usize {
        unsafe {
            let r = &*self.ring;
            let head = r.head.0.load(Ordering::Relaxed);
            let tail = r.tail.0.load(Ordering::Acquire);
            let free = (r.buf.len() as u64).wrapping_sub(head.wrapping_sub(tail)) - 1;
            let n = (items.len() as u64).min(free) as usize;
            if n < items.len() {
                r.overflow.0.fetch_add((items.len() - n) as u64, Ordering::Relaxed);
            }
            for (i, it) in items[..n].iter().enumerate() {
                let idx = (head.wrapping_add(i as u64) & r.mask) as usize;
                (*r.buf[idx].get()).write(*it);
            }
            std::sync::atomic::fence(Ordering::Release);
            r.head.0.store(head.wrapping_add(n as u64), Ordering::Relaxed);
            n
        }
    }
}

impl<T: Copy> Consumer<T> {
    /// Drain up to out.len() items with one acquire + one release.
    #[inline]
    pub fn try_pop_batch(&self, out: &mut [T]) -> usize {
        unsafe {
            let r = &*self.ring;
            let head = r.head.0.load(Ordering::Acquire);
            let tail = r.tail.0.load(Ordering::Relaxed);
            let avail = head.wrapping_sub(tail).min(out.len() as u64) as usize;
            for i in 0..avail {
                let idx = (tail.wrapping_add(i as u64) & r.mask) as usize;
                out[i] = (*r.buf[idx].get()).assume_init();
            }
            std::sync::atomic::fence(Ordering::Release);
            r.tail.0.store(tail.wrapping_add(avail as u64), Ordering::Relaxed);
            avail
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn batch_roundtrip() {
        let (p, c) = Ring::<u64>::with_capacity_pow2(4);
        assert_eq!(p.try_push_batch(&[1, 2, 3]), 3);
        let mut out = [0u64; BATCH];
        assert_eq!(c.try_pop_batch(&mut out[..8]), 3);
        assert_eq!(&out[..3], &[1, 2, 3]);
    }
}
