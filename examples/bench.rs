//! Throughput benchmark: basilisk vs `std::sync::mpsc::sync_channel`.
//!
//! Run with `cargo run --release --example bench`. Numbers depend heavily on
//! which cores the two threads land on, so compare runs on the same machine.

use std::hint;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use basilisk::{channel, Full};

const ITEMS: u64 = 20_000_000;
const RUNS: usize = 5;
const CAPACITY: usize = 1024;

fn basilisk_run() -> f64 {
    let (mut tx, mut rx) = channel::<u64, CAPACITY>();
    let start = Instant::now();
    let producer = thread::spawn(move || {
        for i in 0..ITEMS {
            let mut item = i;
            while let Err(Full(back)) = tx.push(item) {
                item = back;
                hint::spin_loop();
            }
        }
    });
    let consumer = thread::spawn(move || {
        let mut sum = 0u64;
        for _ in 0..ITEMS {
            sum += loop {
                match rx.pop() {
                    Some(v) => break v,
                    None => hint::spin_loop(),
                }
            };
        }
        sum
    });
    producer.join().unwrap();
    let sum = consumer.join().unwrap();
    assert_eq!(sum, ITEMS * (ITEMS - 1) / 2);
    start.elapsed().as_secs_f64()
}

fn mpsc_run() -> f64 {
    let (tx, rx) = mpsc::sync_channel::<u64>(CAPACITY);
    let start = Instant::now();
    let producer = thread::spawn(move || {
        for i in 0..ITEMS {
            tx.send(i).unwrap();
        }
    });
    let consumer = thread::spawn(move || {
        let mut sum = 0u64;
        for _ in 0..ITEMS {
            sum += rx.recv().unwrap();
        }
        sum
    });
    producer.join().unwrap();
    let sum = consumer.join().unwrap();
    assert_eq!(sum, ITEMS * (ITEMS - 1) / 2);
    start.elapsed().as_secs_f64()
}

fn report(name: &str, run: fn() -> f64) {
    let mut secs: Vec<f64> = (0..RUNS).map(|_| run()).collect();
    secs.sort_by(f64::total_cmp);
    let median = secs[RUNS / 2];
    println!(
        "{name:<22} median {:>7.1} ms  {:>7.1} M items/s  (best {:.1} ms)",
        median * 1e3,
        ITEMS as f64 / median / 1e6,
        secs[0] * 1e3,
    );
}

fn main() {
    println!("{ITEMS} u64 items, capacity {CAPACITY}, {RUNS} runs each");
    report("basilisk", basilisk_run);
    report("std mpsc sync_channel", mpsc_run);
}
