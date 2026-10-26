//! A single-producer single-consumer (SPSC) lock-free queue.
//!
//! Data moves between exactly two threads through a fixed-size, power-of-two
//! ring buffer. Synchronisation uses only atomics (`Acquire`/`Release`); there
//! are no mutexes, read-write locks, or condition variables. A full queue
//! rejects pushes with `Err(Full(item))` and an empty queue yields `None`.

use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt;
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;

/// Aligns (and therefore pads) `T` to its own cache line.
///
/// 128 bytes rather than 64: Intel's adjacent-line prefetcher pulls cache
/// lines in pairs, so two values 64 bytes apart can still false-share.
#[repr(align(128))]
struct CachePadded<T>(T);

impl<T> CachePadded<T> {
    const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// Returned by `push` when the queue is full; hands the rejected item back.
#[derive(PartialEq, Eq)]
pub struct Full<T>(pub T);

impl<T> Full<T> {
    /// Recovers the item that could not be pushed.
    pub fn into_inner(self) -> T {
        self.0
    }
}

// Manual impls so that `T` needn't be `Debug`: the message never shows the item.
impl<T> fmt::Debug for Full<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Full(..)")
    }
}

impl<T> fmt::Display for Full<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("queue is full")
    }
}

impl<T> Error for Full<T> {}

/// Shared ring buffer of exactly `N` slots; `N` must be a power of two.
///
/// `head` and `tail` are free-running counters that wrap at `usize::MAX`, not
/// slot indices: the slot for a position is `pos & MASK`. The queue is empty
/// when `head == tail` and full when `head.wrapping_sub(tail) == N`, so all `N`
/// slots are usable. Slots in `tail..head` are initialised, all others are not.
///
/// Ownership of a slot is handed back and forth through the counters: only the
/// producer writes `head` and only the consumer writes `tail`.
// TODO(step 4): drop this allow once `Producer`/`Consumer` use it.
#[allow(dead_code)]
struct Ring<T, const N: usize> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Next position to write; written only by the producer.
    head: CachePadded<AtomicUsize>,
    /// Next position to read; written only by the consumer.
    tail: CachePadded<AtomicUsize>,
}

// SAFETY: a `Ring` is only accessed through the single `Producer` and single
// `Consumer` handles (step 4). They never touch the same slot at the same time:
// a slot is written by the producer before it publishes `head` with `Release`,
// and read by the consumer only after it observes that `head` with `Acquire`;
// symmetrically the producer reuses a slot only after observing the consumer's
// `Release` store of `tail`. Items cross threads, hence `T: Send`.
unsafe impl<T: Send, const N: usize> Send for Ring<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for Ring<T, N> {}

#[allow(dead_code)]
impl<T, const N: usize> Ring<T, N> {
    /// Bitmask turning a position into a slot index. Evaluating it rejects, at
    /// compile time, any `N` that is not a power of two (including 0).
    const MASK: usize = {
        assert!(N.is_power_of_two(), "capacity N must be a power of two");
        N - 1
    };

    fn new() -> Self {
        let _ = Self::MASK;
        Self {
            buf: (0..N)
                .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
                .collect(),
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Moves `item` into the slot for `pos`.
    ///
    /// # Safety
    /// The caller must have exclusive access to that slot, which must not
    /// currently hold an initialised value (it would be overwritten, not dropped).
    unsafe fn write(&self, pos: usize, item: T) {
        (*self.buf[pos & Self::MASK].get()).write(item);
    }

    /// Moves the value out of the slot for `pos`, leaving it uninitialised.
    ///
    /// # Safety
    /// The caller must have exclusive access to that slot, which must hold an
    /// initialised value that is not read again before being rewritten.
    unsafe fn read(&self, pos: usize) -> T {
        (*self.buf[pos & Self::MASK].get()).assume_init_read()
    }
}

impl<T, const N: usize> Drop for Ring<T, N> {
    fn drop(&mut self) {
        if !mem::needs_drop::<T>() {
            return;
        }
        // `&mut self` means no other thread can be using the ring any more.
        let head = *self.head.get_mut();
        let mut pos = *self.tail.get_mut();
        while pos != head {
            // SAFETY: slots in `tail..head` are initialised and, being popped
            // here for the last time, are never read again.
            unsafe { self.buf[pos & Self::MASK].get_mut().assume_init_drop() };
            pos = pos.wrapping_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::mem::{align_of, size_of};
    use std::rc::Rc;
    use std::sync::atomic::Ordering;

    #[test]
    fn cache_padded_occupies_whole_lines() {
        assert_eq!(align_of::<CachePadded<u8>>(), 128);
        assert_eq!(size_of::<CachePadded<u8>>(), 128);
        assert_eq!(size_of::<CachePadded<[u8; 129]>>(), 256);
    }

    #[test]
    fn cache_padded_neighbours_do_not_share_a_line() {
        let pair = [CachePadded::new(0u64), CachePadded::new(0u64)];
        let a = &*pair[0] as *const u64 as usize;
        let b = &*pair[1] as *const u64 as usize;
        assert!(b - a >= 128);
    }

    #[test]
    fn cache_padded_derefs() {
        let mut v = CachePadded::new(5u32);
        *v += 1;
        assert_eq!(*v, 6);
    }

    #[test]
    fn full_returns_the_item() {
        let err = Full(String::from("hello"));
        assert_eq!(err.into_inner(), "hello");
    }

    #[test]
    fn full_formats_without_requiring_debug_on_item() {
        struct Opaque;
        let err = Full(Opaque);
        assert_eq!(format!("{err:?}"), "Full(..)");
        assert_eq!(err.to_string(), "queue is full");
        let _: &dyn Error = &err;
    }

    /// Counts its own drops through a shared cell.
    struct Tracked(Rc<Cell<usize>>);

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    /// Writes `n` tracked items at consecutive positions starting at `start`,
    /// as a producer would, and publishes `head`/`tail` to match.
    fn filled_ring<const N: usize>(
        start: usize,
        n: usize,
        drops: &Rc<Cell<usize>>,
    ) -> Ring<Tracked, N> {
        let ring = Ring::<Tracked, N>::new();
        ring.tail.store(start, Ordering::Relaxed);
        for i in 0..n {
            unsafe { ring.write(start.wrapping_add(i), Tracked(drops.clone())) };
        }
        ring.head.store(start.wrapping_add(n), Ordering::Relaxed);
        ring
    }

    #[test]
    fn ring_is_send_and_sync_for_send_items() {
        fn assert_send_sync<X: Send + Sync>() {}
        assert_send_sync::<Ring<u64, 8>>();
        assert_send_sync::<Ring<Box<String>, 8>>();
    }

    #[test]
    fn ring_write_then_read_round_trips() {
        let ring = Ring::<String, 4>::new();
        unsafe {
            ring.write(0, "a".into());
            ring.write(5, "b".into()); // 5 & 3 == 1
            assert_eq!(ring.read(0), "a");
            assert_eq!(ring.read(5), "b");
        }
    }

    #[test]
    fn ring_drop_drops_unread_items_once() {
        let drops = Rc::new(Cell::new(0));
        drop(filled_ring::<8>(0, 3, &drops));
        assert_eq!(drops.get(), 3);
    }

    #[test]
    fn ring_drop_skips_items_already_read() {
        let drops = Rc::new(Cell::new(0));
        let ring = filled_ring::<8>(0, 4, &drops);
        // Consumer pops two items: move them out and advance `tail`.
        unsafe {
            drop(ring.read(0));
            drop(ring.read(1));
        }
        ring.tail.store(2, Ordering::Relaxed);
        assert_eq!(drops.get(), 2);
        drop(ring);
        assert_eq!(drops.get(), 4);
    }

    #[test]
    fn ring_drop_handles_counters_wrapping_past_usize_max() {
        let drops = Rc::new(Cell::new(0));
        // Positions MAX-1, MAX, 0, 1 map to slots 2, 3, 0, 1: `tail..head`
        // spans the counter wrap and fills the ring exactly.
        drop(filled_ring::<4>(usize::MAX - 1, 4, &drops));
        assert_eq!(drops.get(), 4);
    }

    #[test]
    fn ring_drop_of_empty_and_full_rings() {
        let drops = Rc::new(Cell::new(0));
        drop(filled_ring::<4>(0, 0, &drops));
        assert_eq!(drops.get(), 0);
        drop(filled_ring::<4>(0, 4, &drops));
        assert_eq!(drops.get(), 4);
    }

    #[test]
    fn ring_of_one_slot() {
        let drops = Rc::new(Cell::new(0));
        drop(filled_ring::<1>(7, 1, &drops));
        assert_eq!(drops.get(), 1);
    }
}
