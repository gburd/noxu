#![no_main]

//! Packed integer encoding fuzz test.
//!
//! Verifies:
//! - Round-trip correctness for packed i32 and i64.
//! - Round-trip correctness for sorted i32, i64, f32, f64.
//! - Sorted encoding preserves ordering for pairs of values.
//! - No panics on any valid input.

use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;
use noxu_util::packed::{
    read_packed_i32, read_packed_i64, read_sorted_f32, read_sorted_f64,
    read_sorted_i32, read_sorted_i64, write_packed_i32, write_packed_i64,
    write_sorted_f32, write_sorted_f64, write_sorted_i32, write_sorted_i64,
};
use std::io::Cursor;

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    /// First i32 value for round-trip and ordering tests.
    a_i32: i32,
    /// Second i32 value for ordering comparison.
    b_i32: i32,
    /// First i64 value for round-trip and ordering tests.
    a_i64: i64,
    /// Second i64 value for ordering comparison.
    b_i64: i64,
    /// First f32 value (as bits, to get full range including NaN).
    a_f32_bits: u32,
    /// Second f32 value (as bits).
    b_f32_bits: u32,
    /// First f64 value (as bits, to get full range including NaN).
    a_f64_bits: u64,
    /// Second f64 value (as bits).
    b_f64_bits: u64,
    /// Raw bytes to feed into the decoders (should not panic, just error).
    raw_bytes: Vec<u8>,
}

fuzz_target!(|input: FuzzInput| {
    // ---- Packed i32 round-trip ----
    {
        let mut buf = Vec::new();
        write_packed_i32(&mut buf, input.a_i32).unwrap();
        let decoded = read_packed_i32(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(
            input.a_i32, decoded,
            "packed i32 round-trip failed for {}",
            input.a_i32
        );
    }

    // ---- Packed i64 round-trip ----
    {
        let mut buf = Vec::new();
        write_packed_i64(&mut buf, input.a_i64).unwrap();
        let decoded = read_packed_i64(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(
            input.a_i64, decoded,
            "packed i64 round-trip failed for {}",
            input.a_i64
        );
    }

    // ---- Sorted i32 round-trip ----
    {
        let mut buf = Vec::new();
        write_sorted_i32(&mut buf, input.a_i32).unwrap();
        let decoded = read_sorted_i32(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(
            input.a_i32, decoded,
            "sorted i32 round-trip failed for {}",
            input.a_i32
        );
    }

    // ---- Sorted i32 ordering ----
    {
        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        write_sorted_i32(&mut buf_a, input.a_i32).unwrap();
        write_sorted_i32(&mut buf_b, input.b_i32).unwrap();

        match input.a_i32.cmp(&input.b_i32) {
            std::cmp::Ordering::Less => assert!(
                buf_a < buf_b,
                "sorted i32 ordering: {} < {} but bytes {:?} >= {:?}",
                input.a_i32,
                input.b_i32,
                buf_a,
                buf_b
            ),
            std::cmp::Ordering::Greater => assert!(
                buf_a > buf_b,
                "sorted i32 ordering: {} > {} but bytes {:?} <= {:?}",
                input.a_i32,
                input.b_i32,
                buf_a,
                buf_b
            ),
            std::cmp::Ordering::Equal => assert_eq!(
                buf_a, buf_b,
                "sorted i32 ordering: {} == {} but bytes differ",
                input.a_i32, input.b_i32
            ),
        }
    }

    // ---- Sorted i64 round-trip ----
    {
        let mut buf = Vec::new();
        write_sorted_i64(&mut buf, input.a_i64).unwrap();
        let decoded = read_sorted_i64(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(
            input.a_i64, decoded,
            "sorted i64 round-trip failed for {}",
            input.a_i64
        );
    }

    // ---- Sorted i64 ordering ----
    {
        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        write_sorted_i64(&mut buf_a, input.a_i64).unwrap();
        write_sorted_i64(&mut buf_b, input.b_i64).unwrap();

        match input.a_i64.cmp(&input.b_i64) {
            std::cmp::Ordering::Less => assert!(
                buf_a < buf_b,
                "sorted i64 ordering: {} < {} but bytes {:?} >= {:?}",
                input.a_i64,
                input.b_i64,
                buf_a,
                buf_b
            ),
            std::cmp::Ordering::Greater => assert!(
                buf_a > buf_b,
                "sorted i64 ordering: {} > {} but bytes {:?} <= {:?}",
                input.a_i64,
                input.b_i64,
                buf_a,
                buf_b
            ),
            std::cmp::Ordering::Equal => assert_eq!(
                buf_a, buf_b,
                "sorted i64 ordering: {} == {} but bytes differ",
                input.a_i64, input.b_i64
            ),
        }
    }

    // ---- Sorted f32 round-trip (skip NaN since NaN != NaN) ----
    {
        let val = f32::from_bits(input.a_f32_bits);
        let mut buf = Vec::new();
        write_sorted_f32(&mut buf, val).unwrap();
        let decoded = read_sorted_f32(&mut Cursor::new(&buf)).unwrap();
        if !val.is_nan() {
            assert_eq!(
                val, decoded,
                "sorted f32 round-trip failed for {}",
                val
            );
        } else {
            // For NaN, just verify the decoded value is also NaN.
            assert!(
                decoded.is_nan(),
                "sorted f32 round-trip: NaN encoded but decoded to {}",
                decoded
            );
        }
    }

    // ---- Sorted f32 ordering (for non-NaN finite values) ----
    {
        let a = f32::from_bits(input.a_f32_bits);
        let b = f32::from_bits(input.b_f32_bits);
        // Only check ordering for non-NaN values.
        if !a.is_nan() && !b.is_nan() {
            let mut buf_a = Vec::new();
            let mut buf_b = Vec::new();
            write_sorted_f32(&mut buf_a, a).unwrap();
            write_sorted_f32(&mut buf_b, b).unwrap();

            if a < b {
                assert!(
                    buf_a <= buf_b,
                    "sorted f32 ordering: {} < {} but bytes {:?} > {:?}",
                    a, b, buf_a, buf_b
                );
            } else if a > b {
                assert!(
                    buf_a >= buf_b,
                    "sorted f32 ordering: {} > {} but bytes {:?} < {:?}",
                    a, b, buf_a, buf_b
                );
            }
        }
    }

    // ---- Sorted f64 round-trip (skip NaN since NaN != NaN) ----
    {
        let val = f64::from_bits(input.a_f64_bits);
        let mut buf = Vec::new();
        write_sorted_f64(&mut buf, val).unwrap();
        let decoded = read_sorted_f64(&mut Cursor::new(&buf)).unwrap();
        if !val.is_nan() {
            assert_eq!(
                val, decoded,
                "sorted f64 round-trip failed for {}",
                val
            );
        } else {
            assert!(
                decoded.is_nan(),
                "sorted f64 round-trip: NaN encoded but decoded to {}",
                decoded
            );
        }
    }

    // ---- Sorted f64 ordering (for non-NaN values) ----
    {
        let a = f64::from_bits(input.a_f64_bits);
        let b = f64::from_bits(input.b_f64_bits);
        if !a.is_nan() && !b.is_nan() {
            let mut buf_a = Vec::new();
            let mut buf_b = Vec::new();
            write_sorted_f64(&mut buf_a, a).unwrap();
            write_sorted_f64(&mut buf_b, b).unwrap();

            if a < b {
                assert!(
                    buf_a <= buf_b,
                    "sorted f64 ordering: {} < {} but bytes {:?} > {:?}",
                    a, b, buf_a, buf_b
                );
            } else if a > b {
                assert!(
                    buf_a >= buf_b,
                    "sorted f64 ordering: {} > {} but bytes {:?} < {:?}",
                    a, b, buf_a, buf_b
                );
            }
        }
    }

    // ---- Decoder robustness: feed raw bytes, should not panic ----
    if !input.raw_bytes.is_empty() {
        let _ = read_packed_i32(&mut Cursor::new(&input.raw_bytes));
        let _ = read_packed_i64(&mut Cursor::new(&input.raw_bytes));

        if input.raw_bytes.len() >= 4 {
            let _ = read_sorted_i32(&mut Cursor::new(&input.raw_bytes));
            let _ = read_sorted_f32(&mut Cursor::new(&input.raw_bytes));
        }
        if input.raw_bytes.len() >= 8 {
            let _ = read_sorted_i64(&mut Cursor::new(&input.raw_bytes));
            let _ = read_sorted_f64(&mut Cursor::new(&input.raw_bytes));
        }
    }
});
