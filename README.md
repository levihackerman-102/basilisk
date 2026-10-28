# basilisk

A single-producer single-consumer lock-free queue in Rust. It uses a
power-of-two ring buffer, atomic `head`/`tail` counters with `Acquire`/`Release`
ordering, and cache-line padding. There are no mutexes, read-write locks or
condition variables, and no dependencies.

```rust
let (mut tx, mut rx) = basilisk::channel::<u64, 1024>();
tx.push(1).unwrap();        // Err(Full(item)) when the queue is full
assert_eq!(rx.pop(), Some(1)); // None when it is empty
```

`Producer` and `Consumer` are `Send` but not `Clone`, so the single-producer,
single-consumer rule is enforced by the types. The ordering rationale is in the
crate docs (`cargo doc --open`).

```sh
cargo test                           # unit tests and a small stress test
cargo test --release                 # full 10M-item stress test
cargo run --release --example bench  # throughput vs std mpsc
```

## Known gaps

Things the current tests and benchmark do not cover, to revisit later.

### Correctness checking

- **Miri has not been run.** It needs nightly, and only stable was installed.
  Try `rustup toolchain install nightly --component miri`, then
  `cargo +nightly miri test`. Use a much smaller `ITEMS` in `tests/stress.rs`
  under Miri, which is very slow.
- **Memory orderings are only tested on x86.** x86 is strongly ordered, so a
  too-weak ordering could pass every test here and still fail on ARM. Options:
  run the stress test on an ARM machine, or model-check with `loom`.
- **No ThreadSanitizer or AddressSanitizer run** (both need nightly).
- **Stress tests have no timeout.** A lost-wakeup-style bug would hang the
  test instead of failing it.
- **All unit tests are single-threaded.** Concurrency is only exercised by
  `tests/stress.rs`.

### Benchmark (`examples/bench.rs`)

- **Results are noisy.** Median and best differ by about 30-70% because
  throughput depends on which cores the two threads land on. Pin with
  `taskset -c A,B`, and compare SMT siblings against separate cores.
- **It spins, so it favours basilisk.** `sync_channel` blocks and sleeps,
  which saves CPU. basilisk burns a core while waiting.
- **Narrow workload:** `u64` items, capacity 1024, one machine, five runs. Try
  small capacities, larger items, and `Box<T>` payloads.
- **`get_unchecked` is inconclusive.** Two pinned comparisons contradicted
  each other (199M vs 51M, then 303M vs 380M items/s). Safe indexing stays
  until a quieter benchmark (more runs, or `criterion`) shows a real gap.
- **False sharing was never measured.** Remove `CachePadded` from `head` and
  `tail` and compare, to see the effect directly. Also compare 64 vs 128
  byte alignment.

### Design limits

- **No close or disconnect signal.** Dropping one handle does not tell the
  other. `pop` just returns `None` forever once the queue is empty.
- **No blocking API.** Callers must spin or sleep themselves.
- **No batch push or pop.** These could amortise the atomic operations.
- **The `Acquire` on the producer's `tail` refresh** is a deliberate
  deviation from the original spec, which said `Relaxed`. Its rationale is in
  the crate docs. A loom test would be the strongest evidence for it.
