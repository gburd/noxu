//! Property-based tests for noxu-bind using proptest.

use noxu_bind::{
    EntryBinding, IntBinding, LongBinding, SortedDoubleBinding,
    SortedFloatBinding, StringBinding, TupleInput, TupleOutput,
};
use noxu_db::DatabaseEntry;
use proptest::prelude::*;

proptest! {
    // 1. Int binding round-trip: for any i32, decode(encode(v)) == v.
    #[test]
    fn prop_int_binding_round_trip(v: i32) {
        let binding = IntBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&v, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        prop_assert_eq!(v, result);
    }

    // 2. Long binding round-trip: for any i64, decode(encode(v)) == v.
    #[test]
    fn prop_long_binding_round_trip(v: i64) {
        let binding = LongBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&v, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        prop_assert_eq!(v, result);
    }

    // 3. String binding round-trip: for any String (without null bytes), decode(encode(v)) == v.
    //    Null bytes would interfere with the null-terminated encoding.
    #[test]
    fn prop_string_binding_round_trip(v in "[^\x00]*") {
        let binding = StringBinding::new();
        let s = v;
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&s, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        prop_assert_eq!(s, result);
    }

    // 4. Float binding round-trip: for any non-NaN f32, decode(encode(v)) == v.
    //    Using sorted float binding which supports sortable encoding.
    #[test]
    fn prop_sorted_float_round_trip(v: f32) {
        prop_assume!(!v.is_nan());
        let mut out = TupleOutput::new();
        out.write_sorted_float(v);
        let mut input = TupleInput::new(out.as_bytes());
        let result = input.read_sorted_float().unwrap();
        prop_assert_eq!(v.to_bits(), result.to_bits());
    }

    // 5. Double binding round-trip: for any non-NaN f64, decode(encode(v)) == v.
    //    Using sorted double binding which supports sortable encoding.
    #[test]
    fn prop_sorted_double_round_trip(v: f64) {
        prop_assume!(!v.is_nan());
        let mut out = TupleOutput::new();
        out.write_sorted_double(v);
        let mut input = TupleInput::new(out.as_bytes());
        let result = input.read_sorted_double().unwrap();
        prop_assert_eq!(v.to_bits(), result.to_bits());
    }

    // 6. Sorted int encoding order: for a < b, encoded(a) < encoded(b) lexicographically.
    #[test]
    fn prop_sorted_int_encoding_order(a: i32, b: i32) {
        prop_assume!(a < b);
        let binding = IntBinding::new();

        let mut entry_a = DatabaseEntry::new();
        binding.object_to_entry(&a, &mut entry_a).unwrap();
        let bytes_a = entry_a.data().to_vec();

        let mut entry_b = DatabaseEntry::new();
        binding.object_to_entry(&b, &mut entry_b).unwrap();
        let bytes_b = entry_b.data().to_vec();

        prop_assert!(
            bytes_a < bytes_b,
            "encoded({}) = {:?} should be < encoded({}) = {:?}",
            a, bytes_a, b, bytes_b
        );
    }

    // 7. Sorted double encoding order: for a < b (both non-NaN), encoded(a) < encoded(b) lex.
    #[test]
    fn prop_sorted_double_encoding_order(a: f64, b: f64) {
        prop_assume!(!a.is_nan() && !b.is_nan() && a < b);
        let binding = SortedDoubleBinding::new();

        let mut entry_a = DatabaseEntry::new();
        binding.object_to_entry(&a, &mut entry_a).unwrap();
        let bytes_a = entry_a.data().to_vec();

        let mut entry_b = DatabaseEntry::new();
        binding.object_to_entry(&b, &mut entry_b).unwrap();
        let bytes_b = entry_b.data().to_vec();

        prop_assert!(
            bytes_a < bytes_b,
            "encoded({}) = {:?} should be < encoded({}) = {:?}",
            a, bytes_a, b, bytes_b
        );
    }

    // Additional: Sorted float encoding order.
    #[test]
    fn prop_sorted_float_encoding_order(a: f32, b: f32) {
        prop_assume!(!a.is_nan() && !b.is_nan() && a < b);
        let binding = SortedFloatBinding::new();

        let mut entry_a = DatabaseEntry::new();
        binding.object_to_entry(&a, &mut entry_a).unwrap();
        let bytes_a = entry_a.data().to_vec();

        let mut entry_b = DatabaseEntry::new();
        binding.object_to_entry(&b, &mut entry_b).unwrap();
        let bytes_b = entry_b.data().to_vec();

        prop_assert!(
            bytes_a < bytes_b,
            "encoded({}) = {:?} should be < encoded({}) = {:?}",
            a, bytes_a, b, bytes_b
        );
    }

    // Additional: TupleOutput/TupleInput i32 round-trip.
    #[test]
    fn prop_tuple_i32_round_trip(v: i32) {
        let mut out = TupleOutput::new();
        out.write_i32(v);
        let mut input = TupleInput::new(out.as_bytes());
        let result = input.read_i32().unwrap();
        prop_assert_eq!(v, result);
    }

    // Additional: TupleOutput/TupleInput i64 round-trip.
    #[test]
    fn prop_tuple_i64_round_trip(v: i64) {
        let mut out = TupleOutput::new();
        out.write_i64(v);
        let mut input = TupleInput::new(out.as_bytes());
        let result = input.read_i64().unwrap();
        prop_assert_eq!(v, result);
    }

    // Additional: Packed int round-trip.
    #[test]
    fn prop_packed_int_round_trip(v: i32) {
        let mut out = TupleOutput::new();
        out.write_packed_int(v);
        let mut input = TupleInput::new(out.as_bytes());
        let result = input.read_packed_int().unwrap();
        prop_assert_eq!(v, result);
    }

    // Additional: Packed long round-trip.
    #[test]
    fn prop_packed_long_round_trip(v: i64) {
        let mut out = TupleOutput::new();
        out.write_packed_long(v);
        let mut input = TupleInput::new(out.as_bytes());
        let result = input.read_packed_long().unwrap();
        prop_assert_eq!(v, result);
    }
}
