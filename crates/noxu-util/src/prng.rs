// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Seeded pseudo-random number generator for deterministic simulation testing
//! (DST).
//!
//! `Prng` is a `xorshift64*` generator — small, fast, and fully
//! deterministic: the entire sequence is a pure function of the seed.  The
//! DST harness draws *every* fault decision (which fault, where, how much)
//! from one `Prng` so that a single `NOXU_DST_SEED` reproduces an exact run.
//!
//! This is **not** a cryptographic RNG and must never be used for anything
//! security-sensitive — it exists solely to make test runs reproducible.
//!
//! There is no `rand` dependency in production code; this is the only PRNG and
//! it lives only on the DST/test path.

/// A seeded `xorshift64*` pseudo-random generator.
///
/// Deterministic: the same seed always yields the same sequence.  A seed of
/// `0` is remapped to a non-zero constant (xorshift cannot leave the zero
/// state).
#[derive(Debug, Clone)]
pub struct Prng {
    state: u64,
}

impl Prng {
    /// Create a generator from a 64-bit seed.
    ///
    /// `0` is remapped to a fixed non-zero constant because `xorshift64`
    /// degenerates to all-zeros if the state ever reaches `0`.
    pub fn new(seed: u64) -> Self {
        Prng { state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed } }
    }

    /// Draw the next 64-bit value and advance the state.
    pub fn next_u64(&mut self) -> u64 {
        // xorshift64* (Vigna 2016): xorshift64 mixing step then a
        // multiplicative output scramble.
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Draw a value uniformly in `[0, n)`.  Returns `0` if `n == 0`.
    ///
    /// Uses the high bits via a widening multiply (Lemire's method) which is
    /// faster and less biased than modulo for the small `n` the harness uses.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        let x = self.next_u64() as u128;
        ((x * n as u128) >> 64) as u64
    }

    /// Draw a `bool` that is `true` with probability `numer / denom`.
    ///
    /// `denom == 0` is treated as "never" and returns `false`.
    pub fn chance(&mut self, numer: u64, denom: u64) -> bool {
        if denom == 0 {
            return false;
        }
        self.below(denom) < numer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Prng::new(0xDEAD_BEEF);
        let mut b = Prng::new(0xDEAD_BEEF);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seed_diverges() {
        let mut a = Prng::new(1);
        let mut b = Prng::new(2);
        let seq_a: Vec<u64> = (0..16).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..16).map(|_| b.next_u64()).collect();
        assert_ne!(seq_a, seq_b);
    }

    #[test]
    fn zero_seed_is_not_degenerate() {
        let mut p = Prng::new(0);
        // A degenerate (zero-state) generator returns 0 forever.
        let any_nonzero = (0..8).any(|_| p.next_u64() != 0);
        assert!(any_nonzero);
    }

    #[test]
    fn below_is_in_range() {
        let mut p = Prng::new(42);
        for _ in 0..10_000 {
            let v = p.below(7);
            assert!(v < 7);
        }
        assert_eq!(p.below(0), 0);
        assert_eq!(p.below(1), 0);
    }

    #[test]
    fn chance_extremes() {
        let mut p = Prng::new(99);
        // 0/denom is never, denom/denom is always.
        for _ in 0..100 {
            assert!(!p.chance(0, 10));
            assert!(p.chance(10, 10));
            assert!(!p.chance(1, 0));
        }
    }

    #[test]
    fn chance_is_roughly_fair() {
        let mut p = Prng::new(7);
        let n = 100_000;
        let hits = (0..n).filter(|_| p.chance(1, 2)).count();
        // Within ~3% of 50% over 100k draws.
        let frac = hits as f64 / n as f64;
        assert!((frac - 0.5).abs() < 0.03, "frac={frac}");
    }
}
