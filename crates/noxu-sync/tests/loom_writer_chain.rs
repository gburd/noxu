// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![cfg(loom)]

// Loom model of the NoxuRawRwLock WRITER wake-chain, using loom's blocking
// primitives (NOT a spin-wait) so loom's deadlock detector catches a stranded
// writer directly.
//
// The real lock parks writers in `futex_wait(&write_futex, gen)` and wakes
// them with `write_futex.fetch_add(1); futex_wake(&write_futex, 1)`. We model
// that futex faithfully with a loom Mutex+Condvar keyed on the generation
// counter: `fwait(gen)` blocks until the generation moves; `fwake()` bumps the
// generation and notifies. This is a correct model of a futex (compare value,
// block, wake-by-value-change) and, crucially, uses loom's real blocking so a
// never-woken writer is a loom DEADLOCK, not an unbounded spin loom rejects.
//
// The lock's STATE logic (WRITE_LOCKED, WRITERS_PARKED, acquire-to-contended)
// is transcribed from raw_rwlock.rs. Selected by cfg:
//   --cfg lost_wake_bug : slow-path acquire does NOT re-establish
//                         WRITERS_PARKED -> the wake chain breaks and a writer
//                         strands -> loom reports a deadlock.
//   default (fix)       : acquire re-establishes WRITERS_PARKED -> every
//                         writer terminates.

use loom::sync::atomic::{AtomicU32, Ordering};
use loom::sync::{Arc, Condvar, Mutex};
use loom::thread;

const WRITE_LOCKED: u32 = 1 << 30;
const WRITERS_PARKED: u32 = 1 << 29;
const READERS_MASK: u32 = WRITERS_PARKED - 1;
const ONE_READER: u32 = 1;

struct Lock {
    state: AtomicU32,
    // Futex model for write_futex: generation + condvar.
    wgen: Mutex<u32>,
    wcv: Condvar,
}

impl Lock {
    fn new(initial: u32) -> Self {
        Lock {
            state: AtomicU32::new(initial),
            wgen: Mutex::new(0),
            wcv: Condvar::new(),
        }
    }
    // Model of futex_wait(&write_futex, seen): block until the generation
    // differs from `seen`.
    fn fwait(&self, seen: u32) {
        let mut g = self.wgen.lock().unwrap();
        while *g == seen {
            g = self.wcv.wait(g).unwrap();
        }
    }
    // Model of notify_one_writer: bump generation, wake one.
    fn fwake(&self) {
        let mut g = self.wgen.lock().unwrap();
        *g = g.wrapping_add(1);
        self.wcv.notify_one();
    }
    fn wgen_load(&self) -> u32 {
        *self.wgen.lock().unwrap()
    }

    // unlock_exclusive: clear WL|WP, wake one writer iff prev had WP.
    fn unlock_exclusive(&self) {
        let prev = self
            .state
            .fetch_and(!(WRITE_LOCKED | WRITERS_PARKED), Ordering::Release);
        if prev & WRITERS_PARKED != 0 {
            self.fwake();
        }
    }

    // lock_exclusive (full): fast CAS 0->WL, else slow park loop.
    fn lock_exclusive(&self) {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            return;
        }
        // slow path (no spin budget in the model; go straight to the reserve
        // loop, which is where the chain lives).
        loop {
            let seen = self.wgen_load();
            let s = self.state.load(Ordering::Relaxed);
            if s & WRITE_LOCKED == 0 {
                let newv = if cfg!(lost_wake_bug) {
                    s | WRITE_LOCKED // BUG: does not re-establish WRITERS_PARKED
                } else {
                    s | WRITE_LOCKED | WRITERS_PARKED // FIX: acquire-to-contended
                };
                if self
                    .state
                    .compare_exchange(
                        s,
                        newv,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    // readers==0 in this model (no readers hold), so this is
                    // write_held immediately.
                    return;
                }
                continue;
            }
            // park: set WRITERS_PARKED then futex_wait on the generation.
            if s & WRITERS_PARKED == 0
                && self
                    .state
                    .compare_exchange(
                        s,
                        s | WRITERS_PARKED,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_err()
            {
                continue;
            }
            self.fwait(seen);
        }
    }
}

fn model(n_writers: usize) {
    // Start write_held: the first thread is the initial holder; the rest
    // contend. Model the holder as thread 0 which just releases.
    let lk = Arc::new(Lock::new(WRITE_LOCKED));
    let mut hs = Vec::new();

    // Initial holder releases.
    {
        let lk = Arc::clone(&lk);
        hs.push(thread::spawn(move || {
            lk.unlock_exclusive();
        }));
    }
    // Contending writers: each acquires once and releases once. If any strands
    // (never woken), loom reports a deadlock (no runnable thread).
    for _ in 0..n_writers {
        let lk = Arc::clone(&lk);
        hs.push(thread::spawn(move || {
            lk.lock_exclusive();
            lk.unlock_exclusive();
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    // All threads terminated: no writer stranded. (A strand would have made
    // one join block forever, which loom detects as a deadlock before here.)
    let final_state = lk.state.load(Ordering::Relaxed);
    assert_eq!(
        final_state & (WRITE_LOCKED | READERS_MASK),
        0,
        "lock not free after all writers released: {final_state:#x}"
    );
}

#[test]
fn two_writers_chain_terminates() {
    loom::model(|| model(2));
}

#[test]
fn three_writers_chain_terminates() {
    loom::model(|| model(3));
}
