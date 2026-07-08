//! Property-based tests for noxu-bind using Hegel / hegeltest.

use hegel::generators;
use noxu_bind::{
    EntryBinding, IntBinding, LongBinding, SortedDoubleBinding,
    SortedFloatBinding, StringBinding, TupleInput, TupleOutput,
};
use noxu_db::DatabaseEntry;

use noxu_bind::tuple::sort_key::SortKey;

// 1. Int binding round-trip: for any i32, decode(encode(v)) == v.
#[hegel::test]
fn prop_int_binding_round_trip(tc: hegel::TestCase) {
    let v = tc.draw(generators::integers::<i32>());
    let binding = IntBinding::new();
    let mut entry = DatabaseEntry::new();
    binding.object_to_entry(&v, &mut entry).unwrap();
    let result = binding.entry_to_object(&entry).unwrap();
    assert_eq!(v, result);
}

// 2. Long binding round-trip: for any i64, decode(encode(v)) == v.
#[hegel::test]
fn prop_long_binding_round_trip(tc: hegel::TestCase) {
    let v = tc.draw(generators::integers::<i64>());
    let binding = LongBinding::new();
    let mut entry = DatabaseEntry::new();
    binding.object_to_entry(&v, &mut entry).unwrap();
    let result = binding.entry_to_object(&entry).unwrap();
    assert_eq!(v, result);
}

// 3. String binding round-trip: for any String (without null bytes), decode(encode(v)) == v.
//    Null bytes would interfere with the null-terminated encoding.
#[hegel::test]
fn prop_string_binding_round_trip(tc: hegel::TestCase) {
    let s = tc.draw(generators::from_regex(r"[^\x00]*").fullmatch(true));
    let binding = StringBinding::new();
    let mut entry = DatabaseEntry::new();
    binding.object_to_entry(&s, &mut entry).unwrap();
    let result = binding.entry_to_object(&entry).unwrap();
    assert_eq!(s, result);
}

// 4. Float binding round-trip: for any non-NaN f32, decode(encode(v)) == v.
//    Using sorted float binding which supports sortable encoding.
#[hegel::test]
fn prop_sorted_float_round_trip(tc: hegel::TestCase) {
    let v = tc.draw(generators::floats::<f32>());
    tc.assume(!v.is_nan());
    let mut out = TupleOutput::new();
    out.write_sorted_float(v);
    let mut input = TupleInput::new(out.as_bytes());
    let result = input.read_sorted_float().unwrap();
    assert_eq!(v.to_bits(), result.to_bits());
}

// 5. Double binding round-trip: for any non-NaN f64, decode(encode(v)) == v.
//    Using sorted double binding which supports sortable encoding.
#[hegel::test]
fn prop_sorted_double_round_trip(tc: hegel::TestCase) {
    let v = tc.draw(generators::floats::<f64>());
    tc.assume(!v.is_nan());
    let mut out = TupleOutput::new();
    out.write_sorted_double(v);
    let mut input = TupleInput::new(out.as_bytes());
    let result = input.read_sorted_double().unwrap();
    assert_eq!(v.to_bits(), result.to_bits());
}

// 6. Sorted int encoding order: for a < b, encoded(a) < encoded(b) lexicographically.
#[hegel::test]
fn prop_sorted_int_encoding_order(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i32>());
    let b = tc.draw(generators::integers::<i32>());
    tc.assume(a < b);
    let binding = IntBinding::new();

    let mut entry_a = DatabaseEntry::new();
    binding.object_to_entry(&a, &mut entry_a).unwrap();
    let bytes_a = entry_a.data().to_vec();

    let mut entry_b = DatabaseEntry::new();
    binding.object_to_entry(&b, &mut entry_b).unwrap();
    let bytes_b = entry_b.data().to_vec();

    assert!(
        bytes_a < bytes_b,
        "encoded({a}) = {bytes_a:?} should be < encoded({b}) = {bytes_b:?}"
    );
}

// 7. Sorted double encoding order: for a < b (both non-NaN), encoded(a) < encoded(b) lex.
#[hegel::test]
fn prop_sorted_double_encoding_order(tc: hegel::TestCase) {
    let a = tc.draw(generators::floats::<f64>());
    let b = tc.draw(generators::floats::<f64>());
    tc.assume(!a.is_nan() && !b.is_nan() && a < b);
    let binding = SortedDoubleBinding::new();

    let mut entry_a = DatabaseEntry::new();
    binding.object_to_entry(&a, &mut entry_a).unwrap();
    let bytes_a = entry_a.data().to_vec();

    let mut entry_b = DatabaseEntry::new();
    binding.object_to_entry(&b, &mut entry_b).unwrap();
    let bytes_b = entry_b.data().to_vec();

    assert!(
        bytes_a < bytes_b,
        "encoded({a}) = {bytes_a:?} should be < encoded({b}) = {bytes_b:?}"
    );
}

// Additional: Sorted float encoding order.
#[hegel::test]
fn prop_sorted_float_encoding_order(tc: hegel::TestCase) {
    let a = tc.draw(generators::floats::<f32>());
    let b = tc.draw(generators::floats::<f32>());
    tc.assume(!a.is_nan() && !b.is_nan() && a < b);
    let binding = SortedFloatBinding::new();

    let mut entry_a = DatabaseEntry::new();
    binding.object_to_entry(&a, &mut entry_a).unwrap();
    let bytes_a = entry_a.data().to_vec();

    let mut entry_b = DatabaseEntry::new();
    binding.object_to_entry(&b, &mut entry_b).unwrap();
    let bytes_b = entry_b.data().to_vec();

    assert!(
        bytes_a < bytes_b,
        "encoded({a}) = {bytes_a:?} should be < encoded({b}) = {bytes_b:?}"
    );
}

// Additional: TupleOutput/TupleInput i32 round-trip.
#[hegel::test]
fn prop_tuple_i32_round_trip(tc: hegel::TestCase) {
    let v = tc.draw(generators::integers::<i32>());
    let mut out = TupleOutput::new();
    out.write_i32(v);
    let mut input = TupleInput::new(out.as_bytes());
    let result = input.read_i32().unwrap();
    assert_eq!(v, result);
}

// Additional: TupleOutput/TupleInput i64 round-trip.
#[hegel::test]
fn prop_tuple_i64_round_trip(tc: hegel::TestCase) {
    let v = tc.draw(generators::integers::<i64>());
    let mut out = TupleOutput::new();
    out.write_i64(v);
    let mut input = TupleInput::new(out.as_bytes());
    let result = input.read_i64().unwrap();
    assert_eq!(v, result);
}

// Additional: Packed int round-trip.
#[hegel::test]
fn prop_packed_int_round_trip(tc: hegel::TestCase) {
    let v = tc.draw(generators::integers::<i32>());
    let mut out = TupleOutput::new();
    out.write_packed_int(v);
    let mut input = TupleInput::new(out.as_bytes());
    let result = input.read_packed_int().unwrap();
    assert_eq!(v, result);
}

// Additional: Packed long round-trip.
#[hegel::test]
fn prop_packed_long_round_trip(tc: hegel::TestCase) {
    let v = tc.draw(generators::integers::<i64>());
    let mut out = TupleOutput::new();
    out.write_packed_long(v);
    let mut input = TupleInput::new(out.as_bytes());
    let result = input.read_packed_long().unwrap();
    assert_eq!(v, result);
}

// =====================================================================
// SortKey reverse properties.
//
// For each SortKey impl, encode-then-decode round-trips, AND the encoded
// bytes preserve order: a < b implies encode(a) < encode(b)
// lexicographically.  These bias toward "for any byte sequence that
// successfully decodes, re-encoding produces the same bytes".
// =====================================================================

// 1. SortKey<u32> roundtrip + read-then-write idempotence.
#[hegel::test(test_cases = 256)]
fn prop_sort_key_u32_decode_then_encode(tc: hegel::TestCase) {
    let v = tc.draw(generators::integers::<u32>());
    let mut out = TupleOutput::new();
    v.encode_sort_key(&mut out);
    let bytes = out.into_vec();

    let mut inp = TupleInput::new(&bytes);
    let decoded = u32::decode_sort_key(&mut inp).unwrap();
    assert_eq!(decoded, v);

    // Re-encode the decoded value: must produce identical bytes.
    let mut out2 = TupleOutput::new();
    decoded.encode_sort_key(&mut out2);
    assert_eq!(out2.into_vec(), bytes);
}

// 2. SortKey<i64> roundtrip + read-then-write idempotence.
#[hegel::test(test_cases = 256)]
fn prop_sort_key_i64_decode_then_encode(tc: hegel::TestCase) {
    let v = tc.draw(generators::integers::<i64>());
    let mut out = TupleOutput::new();
    v.encode_sort_key(&mut out);
    let bytes = out.into_vec();

    let mut inp = TupleInput::new(&bytes);
    let decoded = i64::decode_sort_key(&mut inp).unwrap();
    assert_eq!(decoded, v);

    let mut out2 = TupleOutput::new();
    decoded.encode_sort_key(&mut out2);
    assert_eq!(out2.into_vec(), bytes);
}

// 3. SortKey<i32> ordering: a < b iff encode(a) < encode(b).
//    Bidirectional check: tests the "iff" of the trait contract.
#[hegel::test(test_cases = 256)]
fn prop_sort_key_i32_order_iff(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i32>());
    let b = tc.draw(generators::integers::<i32>());
    let mut out_a = TupleOutput::new();
    a.encode_sort_key(&mut out_a);
    let mut out_b = TupleOutput::new();
    b.encode_sort_key(&mut out_b);
    let ba = out_a.into_vec();
    let bb = out_b.into_vec();
    assert_eq!(
        a.cmp(&b),
        ba.cmp(&bb),
        "i32 sort-key ordering disagrees: a={a}, b={b}"
    );
}

// 4. SortKey<i16> ordering: a < b iff encode(a) < encode(b).
#[hegel::test(test_cases = 256)]
fn prop_sort_key_i16_order_iff(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i16>());
    let b = tc.draw(generators::integers::<i16>());
    let mut out_a = TupleOutput::new();
    a.encode_sort_key(&mut out_a);
    let mut out_b = TupleOutput::new();
    b.encode_sort_key(&mut out_b);
    assert_eq!(a.cmp(&b), out_a.into_vec().cmp(&out_b.into_vec()));
}

// 5. SortKey<Vec<u8>> roundtrip with null-byte escaping.
//    A byte sequence containing 0x00 must round-trip; the escape
//    encoding is the only way to embed null bytes.
#[hegel::test(test_cases = 256)]
fn prop_sort_key_bytes_roundtrip(tc: hegel::TestCase) {
    let v = tc.draw(generators::binary().max_size(63));
    let mut out = TupleOutput::new();
    v.encode_sort_key(&mut out);
    let bytes = out.into_vec();

    let mut inp = TupleInput::new(&bytes);
    let decoded = <Vec<u8>>::decode_sort_key(&mut inp).unwrap();
    assert_eq!(&decoded, &v);

    // Reverse direction: re-encode decoded value yields same bytes.
    let mut out2 = TupleOutput::new();
    decoded.encode_sort_key(&mut out2);
    assert_eq!(out2.into_vec(), bytes);
}

// 6. SortKey<Vec<u8>> ordering preserves byte order.
#[hegel::test(test_cases = 256)]
fn prop_sort_key_bytes_order_preserving(tc: hegel::TestCase) {
    let a = tc.draw(generators::binary().max_size(31));
    let b = tc.draw(generators::binary().max_size(31));
    let mut out_a = TupleOutput::new();
    a.encode_sort_key(&mut out_a);
    let mut out_b = TupleOutput::new();
    b.encode_sort_key(&mut out_b);
    assert_eq!(
        a.cmp(&b),
        out_a.into_vec().cmp(&out_b.into_vec()),
        "Vec<u8> sort-key ordering disagrees"
    );
}
