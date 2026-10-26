//! A single-producer single-consumer (SPSC) lock-free queue.
//!
//! Data moves between exactly two threads through a fixed-size, power-of-two
//! ring buffer. Synchronisation uses only atomics (`Acquire`/`Release`); there
//! are no mutexes, read-write locks, or condition variables. A full queue
//! rejects pushes with `Err(Full(item))` and an empty queue yields `None`.

use std::error::Error;
use std::fmt;
use std::ops::{Deref, DerefMut};

/// Aligns (and therefore pads) `T` to its own cache line.
///
/// 128 bytes rather than 64: Intel's adjacent-line prefetcher pulls cache
/// lines in pairs, so two values 64 bytes apart can still false-share.
// TODO(step 3): drop this allow once `Ring` uses it.
#[allow(dead_code)]
#[repr(align(128))]
struct CachePadded<T>(T);

#[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, size_of};

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
}
