//! Gateway read: seqlock even/odd open-bar read (ADR-0004 port).
//! Writer (worker) brackets mutation with begin/end; readers never take a lock.

use std::sync::atomic::{AtomicU32, Ordering};

pub struct SeqSlot<T: Copy> {
    seq: AtomicU32,
    data: std::cell::UnsafeCell<T>,
}

unsafe impl<T: Copy + Send> Send for SeqSlot<T> {}
unsafe impl<T: Copy + Send> Sync for SeqSlot<T> {}

impl<T: Copy> SeqSlot<T> {
    pub fn new(v: T) -> Self {
        Self { seq: AtomicU32::new(0), data: std::cell::UnsafeCell::new(v) }
    }
    #[inline]
    pub fn begin_write(&self) {
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s.wrapping_add(1), Ordering::Relaxed);
        std::sync::atomic::fence(Ordering::Release);
    }
    #[inline]
    pub fn end_write(&self) {
        std::sync::atomic::fence(Ordering::Release);
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s.wrapping_add(1), Ordering::Release);
    }
    #[inline]
    pub fn write(&self, v: T) {
        self.begin_write();
        unsafe { *self.data.get() = v; }
        self.end_write();
    }
    pub fn read(&self) -> Option<T> {
        for _ in 0..64 {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let v = unsafe { *self.data.get() };
            std::sync::atomic::fence(Ordering::Acquire);
            if self.seq.load(Ordering::Acquire) == s1 {
                return Some(v);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seqlock_stable() {
        let s = SeqSlot::new(0u64);
        s.write(42);
        assert_eq!(s.read(), Some(42));
    }
}
