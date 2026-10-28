//! Two-thread stress tests: every item must arrive exactly once, in order.
//!
//! Run with `cargo test --release` for the full 10M-item workload; debug
//! builds use a smaller one so a plain `cargo test` stays quick.

use std::hint;
use std::thread;

use basilisk::{channel, Full};

const ITEMS: u64 = if cfg!(debug_assertions) {
    200_000
} else {
    10_000_000
};

/// Sends `0..ITEMS` through a queue of `N` slots and checks the receiving end.
///
/// Items are `Box<u64>` so each one owns heap memory: a double drop or a
/// use-after-free shows up as a crash (or under Miri/ASan), not a quiet pass.
fn run<const N: usize>() {
    let (mut tx, mut rx) = channel::<Box<u64>, N>();

    let producer = thread::spawn(move || {
        for i in 0..ITEMS {
            let mut item = Box::new(i);
            // Full hands the item back, so retry with the same allocation.
            while let Err(Full(back)) = tx.push(item) {
                item = back;
                hint::spin_loop();
            }
        }
    });

    let consumer = thread::spawn(move || {
        for expected in 0..ITEMS {
            let item = loop {
                match rx.pop() {
                    Some(item) => break item,
                    None => hint::spin_loop(),
                }
            };
            assert_eq!(*item, expected, "item lost, duplicated or reordered");
        }
        assert!(rx.pop().is_none(), "queue should be drained");
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

#[test]
fn one_slot() {
    run::<1>();
}

#[test]
fn two_slots() {
    run::<2>();
}

#[test]
fn small_ring() {
    run::<64>();
}

#[test]
fn large_ring() {
    run::<65536>();
}
