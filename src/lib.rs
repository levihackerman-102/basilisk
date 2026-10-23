//! A single-producer single-consumer (SPSC) lock-free queue.
//!
//! Data moves between exactly two threads through a fixed-size, power-of-two
//! ring buffer. Synchronisation uses only atomics (`Acquire`/`Release`); there
//! are no mutexes, read-write locks, or condition variables. A full queue
//! rejects pushes with `Err(Full(item))` and an empty queue yields `None`.
