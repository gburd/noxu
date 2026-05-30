//! Type-specific tuple bindings for primitive types.
//!
//! Each binding implements `EntryBinding<T>` and `TupleBinding<T>` for a
//! specific primitive type, using the sortable encoding from `TupleOutput`
//! and `TupleInput`.
//!
//! Primitive type bindings for key encoding.

use crate::db::DatabaseEntry;

use crate::bind::entry_binding::EntryBinding;
use crate::bind::error::Result;
use crate::bind::tuple::tuple_binding::TupleBinding;
use crate::bind::tuple::tuple_input::TupleInput;
use crate::bind::tuple::tuple_output::TupleOutput;

// ---------------------------------------------------------------------------
// Macro to reduce boilerplate for simple bindings
// ---------------------------------------------------------------------------

macro_rules! impl_primitive_binding {
    (
        $(#[$meta:meta])*
        $name:ident, $ty:ty,
        write: $write_method:ident,
        read: $read_method:ident
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, Default)]
        pub struct $name;

        impl $name {
            /// Creates a new binding instance.
            pub fn new() -> Self {
                Self
            }
        }

        impl EntryBinding<$ty> for $name {
            fn entry_to_object(&self, entry: &DatabaseEntry) -> Result<$ty> {
                let mut input = Self::entry_to_input(entry);
                self.tuple_to_object(&mut input)
            }

            fn object_to_entry(&self, object: &$ty, entry: &mut DatabaseEntry) -> Result<()> {
                let mut output = TupleOutput::new();
                self.object_to_tuple(object, &mut output)?;
                entry.set_data_vec(output.into_vec());
                Ok(())
            }
        }

        impl TupleBinding<$ty> for $name {
            fn tuple_to_object(&self, input: &mut TupleInput) -> Result<$ty> {
                input.$read_method()
            }

            fn object_to_tuple(&self, object: &$ty, output: &mut TupleOutput) -> Result<()> {
                output.$write_method(*object);
                Ok(())
            }
        }
    };
}

impl_primitive_binding!(
    /// Binding for `bool` values.
    ///
    /// Encodes as a single byte: 1 for true, 0 for false.
    ///
    ///
    BoolBinding, bool,
    write: write_bool,
    read: read_bool
);

impl_primitive_binding!(
    /// Binding for unsigned `u8` byte values.
    ///
    /// Stored as a single raw byte.
    ///
    /// Unsigned byte binding.
    ByteBinding, u8,
    write: write_u8,
    read: read_u8
);

impl_primitive_binding!(
    /// Binding for signed `i16` (short) values.
    ///
    /// Uses big-endian encoding with sign bit flipped for sortable ordering.
    ///
    ///
    ShortBinding, i16,
    write: write_i16,
    read: read_i16
);

impl_primitive_binding!(
    /// Binding for signed `i32` (int) values.
    ///
    /// Uses big-endian encoding with sign bit flipped for sortable ordering.
    ///
    ///
    IntBinding, i32,
    write: write_i32,
    read: read_i32
);

impl_primitive_binding!(
    /// Binding for signed `i64` (long) values.
    ///
    /// Uses big-endian encoding with sign bit flipped for sortable ordering.
    ///
    ///
    LongBinding, i64,
    write: write_i64,
    read: read_i64
);

impl_primitive_binding!(
    /// Binding for `f32` (float) values using unsorted encoding.
    ///
    /// Stored as raw IEEE 754 big-endian bits. The byte representation does NOT
    /// sort in the same order as the float values. Use `SortedFloatBinding` for
    /// sortable keys.
    ///
    ///
    FloatBinding, f32,
    write: write_float,
    read: read_float
);

impl_primitive_binding!(
    /// Binding for `f64` (double) values using unsorted encoding.
    ///
    /// Stored as raw IEEE 754 big-endian bits. The byte representation does NOT
    /// sort in the same order as the double values. Use `SortedDoubleBinding` for
    /// sortable keys.
    ///
    ///
    DoubleBinding, f64,
    write: write_double,
    read: read_double
);

impl_primitive_binding!(
    /// Binding for `f32` (float) values using sortable encoding.
    ///
    /// Uses sign-bit manipulation so the byte representation sorts in the
    /// same order as the float values.
    ///
    ///
    SortedFloatBinding, f32,
    write: write_sorted_float,
    read: read_sorted_float
);

impl_primitive_binding!(
    /// Binding for `f64` (double) values using sortable encoding.
    ///
    /// Uses sign-bit manipulation so the byte representation sorts in the
    /// same order as the double values.
    ///
    ///
    SortedDoubleBinding, f64,
    write: write_sorted_double,
    read: read_sorted_double
);

impl_primitive_binding!(
    /// Binding for `i32` values using packed (variable-length) encoding.
    ///
    /// Values in [-119, 119] are stored in a single byte. Larger values use
    /// 2-5 bytes. This encoding is compact but NOT sortable.
    ///
    ///
    PackedIntBinding, i32,
    write: write_packed_int,
    read: read_packed_int
);

impl_primitive_binding!(
    /// Binding for `i64` values using packed (variable-length) encoding.
    ///
    /// Values in [-119, 119] are stored in a single byte. Larger values use
    /// 2-9 bytes. This encoding is compact but NOT sortable.
    ///
    ///
    PackedLongBinding, i64,
    write: write_packed_long,
    read: read_packed_long
);

impl_primitive_binding!(
    /// Binding for `i32` values using sorted packed (variable-length,
    /// order-preserving) encoding.
    ///
    /// Values in [-119, 120] are stored in a single byte. Larger values use
    /// 2-5 bytes. Unlike `PackedIntBinding`, the byte representation DOES
    /// sort in the same order as the integer values, making this suitable for
    /// database keys when compactness is also desired.
    ///
    ///
    SortedPackedIntBinding, i32,
    write: write_sorted_packed_int,
    read: read_sorted_packed_int
);

impl_primitive_binding!(
    /// Binding for `i64` values using sorted packed (variable-length,
    /// order-preserving) encoding.
    ///
    /// Values in [-119, 120] are stored in a single byte. Larger values use
    /// 2-9 bytes. Unlike `PackedLongBinding`, the byte representation DOES
    /// sort in the same order as the integer values, making this suitable for
    /// database keys when compactness is also desired.
    ///
    ///
    SortedPackedLongBinding, i64,
    write: write_sorted_packed_long,
    read: read_sorted_packed_long
);

impl_primitive_binding!(
    /// Binding for `u16` values representing Java `char` (16-bit Unicode code points).
    ///
    /// Encodes as two big-endian bytes. This matches Java's `writeChar` /
    /// `readChar` in `DataOutputStream` / `DataInputStream`. Sort order is
    /// unsigned numeric (U+0000 < U+0001 < ... < U+FFFF).
    ///
    /// In Rust, this is represented as `u16` rather than Rust's `char` because
    /// Java `char` covers the full [0, 65535] range including surrogate halves
    /// which are not valid Unicode scalar values in Rust.
    ///
    ///
    CharBinding, u16,
    write: write_char,
    read: read_char
);

// ---------------------------------------------------------------------------
// StringBinding  -  requires special handling (reference vs owned)
// ---------------------------------------------------------------------------

/// Binding for `String` values using null-terminated UTF-8 encoding.
///
/// Strings are stored as their UTF-8 bytes followed by a null terminator byte.
///
///
#[derive(Debug, Clone, Copy, Default)]
pub struct StringBinding;

impl StringBinding {
    /// Creates a new `StringBinding`.
    pub fn new() -> Self {
        Self
    }
}

impl EntryBinding<String> for StringBinding {
    fn entry_to_object(&self, entry: &DatabaseEntry) -> Result<String> {
        let mut input = Self::entry_to_input(entry);
        self.tuple_to_object(&mut input)
    }

    fn object_to_entry(
        &self,
        object: &String,
        entry: &mut DatabaseEntry,
    ) -> Result<()> {
        let mut output = TupleOutput::new();
        self.object_to_tuple(object, &mut output)?;
        entry.set_data_vec(output.into_vec());
        Ok(())
    }
}

impl TupleBinding<String> for StringBinding {
    fn tuple_to_object(&self, input: &mut TupleInput) -> Result<String> {
        input.read_string()
    }

    fn object_to_tuple(
        &self,
        object: &String,
        output: &mut TupleOutput,
    ) -> Result<()> {
        output.write_string(object);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bool_binding_round_trip() {
        let binding = BoolBinding::new();
        for val in [true, false] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_byte_binding_round_trip() {
        let binding = ByteBinding::new();
        for val in [0u8, 1, 127, 128, 255] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_short_binding_round_trip() {
        let binding = ShortBinding::new();
        for val in [i16::MIN, -1, 0, 1, i16::MAX] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_int_binding_round_trip() {
        let binding = IntBinding::new();
        for val in [i32::MIN, -1, 0, 1, i32::MAX] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_long_binding_round_trip() {
        let binding = LongBinding::new();
        for val in [i64::MIN, -1, 0, 1, i64::MAX] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_float_binding_round_trip() {
        let binding = FloatBinding::new();
        for val in [-1.5f32, 0.0, 1.5, f32::MAX, f32::MIN] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_double_binding_round_trip() {
        let binding = DoubleBinding::new();
        for val in [-1.5f64, 0.0, 1.5, f64::MAX, f64::MIN] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_sorted_float_binding_round_trip() {
        let binding = SortedFloatBinding::new();
        for val in [-1.5f32, -0.0, 0.0, 1.5, f32::MAX, f32::MIN] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            // -0.0 and 0.0 have different bits but compare equal
            assert_eq!(val.to_bits(), result.to_bits(), "failed for {}", val);
        }
    }

    #[test]
    fn test_sorted_double_binding_round_trip() {
        let binding = SortedDoubleBinding::new();
        for val in [-1.5f64, -0.0, 0.0, 1.5, f64::MAX, f64::MIN] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val.to_bits(), result.to_bits(), "failed for {}", val);
        }
    }

    #[test]
    fn test_packed_int_binding_round_trip() {
        let binding = PackedIntBinding::new();
        for val in [i32::MIN, -120, -119, -1, 0, 1, 119, 120, i32::MAX] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result, "failed for {}", val);
        }
    }

    #[test]
    fn test_packed_long_binding_round_trip() {
        let binding = PackedLongBinding::new();
        for val in [i64::MIN, -120, -119, -1, 0, 1, 119, 120, i64::MAX] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result, "failed for {}", val);
        }
    }

    #[test]
    fn test_string_binding_round_trip() {
        let binding = StringBinding::new();
        for val in ["", "hello", "world", "unicode: \u{1F600}"] {
            let s = val.to_string();
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&s, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(s, result);
        }
    }

    #[test]
    fn test_string_binding_embedded_null() {
        let binding = StringBinding::new();
        // String with a single embedded null byte
        let s = "hello\x00world".to_string();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&s, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert_eq!(s, result);
    }

    #[test]
    fn test_string_binding_multiple_embedded_nulls() {
        let binding = StringBinding::new();
        let s = "\x00\x00\x00".to_string();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&s, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert_eq!(s, result);
    }

    #[test]
    fn test_string_binding_null_at_start_and_end() {
        let binding = StringBinding::new();
        let s = "\x00hello\x00".to_string();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&s, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert_eq!(s, result);
    }

    #[test]
    fn test_string_binding_null_escape_encoded_correctly() {
        // Verify that embedded 0x00 is encoded as [0x00, 0x01], not [0x00]
        let binding = StringBinding::new();
        let s = "a\x00b".to_string();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&s, &mut entry).unwrap();
        let raw = entry.data();
        // Expected: 'a', 0x00, 0x01, 'b', 0x00, 0x00
        assert_eq!(raw, &[b'a', 0x00, 0x01, b'b', 0x00, 0x00]);
    }

    #[test]
    fn test_string_binding_terminator_encoded_correctly() {
        // Empty string should encode as just the two-byte terminator [0x00, 0x00]
        let binding = StringBinding::new();
        let s = String::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&s, &mut entry).unwrap();
        let raw = entry.data();
        assert_eq!(raw, &[0x00, 0x00]);
    }

    #[test]
    fn test_string_binding_two_strings_in_tuple() {
        // Two consecutive strings in a TupleOutput, both readable back
        use crate::bind::tuple::tuple_input::TupleInput;
        use crate::bind::tuple::tuple_output::TupleOutput;
        let mut out = TupleOutput::new();
        out.write_string("foo\x00bar");
        out.write_string("baz");
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "foo\x00bar");
        assert_eq!(inp.read_string().unwrap(), "baz");
    }

    // -----------------------------------------------------------------------
    // Sort ordering tests
    // -----------------------------------------------------------------------

    fn encoded_bytes<T>(binding: &impl EntryBinding<T>, val: &T) -> Vec<u8> {
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(val, &mut entry).unwrap();
        entry.data().to_vec()
    }

    #[test]
    fn test_short_binding_sort_order() {
        let binding = ShortBinding::new();
        let values: Vec<i16> = vec![i16::MIN, -100, -1, 0, 1, 100, i16::MAX];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "{} should sort before {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_int_binding_sort_order() {
        let binding = IntBinding::new();
        let values: Vec<i32> = vec![i32::MIN, -100, -1, 0, 1, 100, i32::MAX];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "{} should sort before {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_long_binding_sort_order() {
        let binding = LongBinding::new();
        let values: Vec<i64> = vec![i64::MIN, -100, -1, 0, 1, 100, i64::MAX];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "{} should sort before {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_sorted_float_binding_sort_order() {
        let binding = SortedFloatBinding::new();
        let values: Vec<f32> = vec![
            f32::NEG_INFINITY,
            f32::MIN,
            -100.0,
            -1.0,
            -f32::MIN_POSITIVE,
            0.0,
            f32::MIN_POSITIVE,
            1.0,
            100.0,
            f32::MAX,
            f32::INFINITY,
        ];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "{} (encoded {:?}) should sort before {} (encoded {:?})",
                values[i],
                encoded[i],
                values[i + 1],
                encoded[i + 1]
            );
        }
    }

    #[test]
    fn test_sorted_double_binding_sort_order() {
        let binding = SortedDoubleBinding::new();
        let values: Vec<f64> = vec![
            f64::NEG_INFINITY,
            f64::MIN,
            -100.0,
            -1.0,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            100.0,
            f64::MAX,
            f64::INFINITY,
        ];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "{} should sort before {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_string_binding_sort_order() {
        let binding = StringBinding::new();
        let values: Vec<String> = vec![
            "".to_string(),
            "a".to_string(),
            "ab".to_string(),
            "b".to_string(),
            "z".to_string(),
        ];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "{:?} should sort before {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    // -----------------------------------------------------------------------
    // Edge case tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_packed_int_boundary_values() {
        let binding = PackedIntBinding::new();
        // Test boundary values around the single-byte range
        for val in [-120, -119, 119, 120, 256, -257, 65536, -65537] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result, "boundary test failed for {}", val);
        }
    }

    #[test]
    fn test_packed_long_boundary_values() {
        let binding = PackedLongBinding::new();
        for val in [-120i64, -119, 119, 120, 256, -257, 1 << 32, -(1 << 32)] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result, "boundary test failed for {}", val);
        }
    }

    #[test]
    fn test_sorted_float_nan() {
        // NaN should round-trip (bits preserved)
        let binding = SortedFloatBinding::new();
        let val = f32::NAN;
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&val, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert!(result.is_nan());
    }

    #[test]
    fn test_sorted_double_nan() {
        let binding = SortedDoubleBinding::new();
        let val = f64::NAN;
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&val, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert!(result.is_nan());
    }

    #[test]
    fn test_sorted_float_negative_zero() {
        let binding = SortedFloatBinding::new();
        let val = -0.0f32;
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&val, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        // -0.0 should preserve its bit pattern
        assert_eq!(val.to_bits(), result.to_bits());
    }

    #[test]
    fn test_sorted_double_negative_zero() {
        let binding = SortedDoubleBinding::new();
        let val = -0.0f64;
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&val, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert_eq!(val.to_bits(), result.to_bits());
    }

    #[test]
    fn test_int_binding_encoded_size() {
        let binding = IntBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&42i32, &mut entry).unwrap();
        assert_eq!(entry.data().len(), 4);
    }

    #[test]
    fn test_long_binding_encoded_size() {
        let binding = LongBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&42i64, &mut entry).unwrap();
        assert_eq!(entry.data().len(), 8);
    }

    #[test]
    fn test_packed_int_compact_size() {
        let binding = PackedIntBinding::new();
        // Values in [-119, 119] should be 1 byte
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&0i32, &mut entry).unwrap();
        assert_eq!(entry.data().len(), 1);

        binding.object_to_entry(&119i32, &mut entry).unwrap();
        assert_eq!(entry.data().len(), 1);

        binding.object_to_entry(&(-119i32), &mut entry).unwrap();
        assert_eq!(entry.data().len(), 1);

        // 120 should need more than 1 byte
        binding.object_to_entry(&120i32, &mut entry).unwrap();
        assert!(entry.data().len() > 1);
    }

    // -----------------------------------------------------------------------
    // SortedPackedIntBinding tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_packed_int_binding_round_trip() {
        let binding = SortedPackedIntBinding::new();
        for val in [i32::MIN, -120, -119, -1, 0, 1, 119, 120, 121, i32::MAX] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result, "round-trip failed for {}", val);
        }
    }

    #[test]
    fn test_sorted_packed_int_binding_sort_order() {
        let binding = SortedPackedIntBinding::new();
        let values: Vec<i32> = vec![
            i32::MIN,
            -1_000_000,
            -120,
            -119,
            -1,
            0,
            1,
            119,
            120,
            121,
            1_000_000,
            i32::MAX,
        ];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {} (encoded {:?}) should sort before {} (encoded {:?})",
                values[i],
                encoded[i],
                values[i + 1],
                encoded[i + 1]
            );
        }
    }

    #[test]
    fn test_sorted_packed_int_single_byte_range() {
        // Values in [-119, 120] must encode to exactly 1 byte
        let binding = SortedPackedIntBinding::new();
        for val in (-119..=120i32).step_by(1) {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                1,
                "value {} should encode in 1 byte",
                val
            );
        }
    }

    #[test]
    fn test_sorted_packed_int_multi_byte() {
        let binding = SortedPackedIntBinding::new();
        for val in [121i32, -120, i32::MAX, i32::MIN] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert!(
                entry.data().len() > 1,
                "value {} should encode in more than 1 byte",
                val
            );
        }
    }

    // -----------------------------------------------------------------------
    // SortedPackedLongBinding tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_packed_long_binding_round_trip() {
        let binding = SortedPackedLongBinding::new();
        for val in [
            i64::MIN,
            -1_000_000_000_000i64,
            -120,
            -119,
            -1,
            0,
            1,
            119,
            120,
            121,
            1_000_000_000_000i64,
            i64::MAX,
        ] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result, "round-trip failed for {}", val);
        }
    }

    #[test]
    fn test_sorted_packed_long_binding_sort_order() {
        let binding = SortedPackedLongBinding::new();
        let values: Vec<i64> = vec![
            i64::MIN,
            -1_000_000_000_000,
            -120,
            -119,
            -1,
            0,
            1,
            119,
            120,
            121,
            1_000_000_000_000,
            i64::MAX,
        ];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {} (encoded {:?}) should sort before {} (encoded {:?})",
                values[i],
                encoded[i],
                values[i + 1],
                encoded[i + 1]
            );
        }
    }

    #[test]
    fn test_sorted_packed_long_single_byte_range() {
        // Values in [-119, 120] must encode to exactly 1 byte
        let binding = SortedPackedLongBinding::new();
        for val in (-119..=120i64).step_by(1) {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                1,
                "value {} should encode in 1 byte",
                val
            );
        }
    }

    #[test]
    fn test_sorted_packed_long_multi_byte() {
        let binding = SortedPackedLongBinding::new();
        for val in [121i64, -120, i64::MAX, i64::MIN] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert!(
                entry.data().len() > 1,
                "value {} should encode in more than 1 byte",
                val
            );
        }
    }

    // -----------------------------------------------------------------------
    // CharBinding tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_char_binding_round_trip() {
        let binding = CharBinding::new();
        for val in
            [0u16, 1, 'A' as u16, 'a' as u16, 0x00FF, 0x0100, 0xD800, 0xFFFF]
        {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            let result = binding.entry_to_object(&entry).unwrap();
            assert_eq!(val, result, "round-trip failed for U+{:04X}", val);
        }
    }

    #[test]
    fn test_char_binding_encoded_size() {
        let binding = CharBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&(b'A' as u16), &mut entry).unwrap();
        assert_eq!(
            entry.data().len(),
            2,
            "char should always encode to 2 bytes"
        );
    }

    #[test]
    fn test_char_binding_big_endian_encoding() {
        // 'A' is U+0041. Big-endian: [0x00, 0x41]
        let binding = CharBinding::new();
        let val = b'A' as u16; // 0x0041
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&val, &mut entry).unwrap();
        assert_eq!(
            entry.data(),
            &[0x00, 0x41],
            "char 'A' (U+0041) should encode as [0x00, 0x41]"
        );
    }

    #[test]
    fn test_char_binding_high_codepoint_encoding() {
        // U+FF41 (FULLWIDTH LATIN SMALL LETTER A). Big-endian: [0xFF, 0x41]
        let binding = CharBinding::new();
        let val: u16 = 0xFF41;
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&val, &mut entry).unwrap();
        assert_eq!(
            entry.data(),
            &[0xFF, 0x41],
            "U+FF41 should encode as [0xFF, 0x41]"
        );
    }

    #[test]
    fn test_char_binding_sort_order() {
        // u16 sort order should match byte-wise comparison (big-endian is unsigned)
        let binding = CharBinding::new();
        let values: Vec<u16> = vec![
            0x0000, 0x0041, 0x007F, 0x0080, 0x00FF, 0x0100, 0xD800, 0xFFFF,
        ];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: U+{:04X} should sort before U+{:04X}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_sorted_packed_int_sort_order_vs_fixed_int() {
        // Sorted packed int and fixed-width int should agree on sort order
        let spb = SortedPackedIntBinding::new();
        let ib = IntBinding::new();
        let values: Vec<i32> = vec![-1000, -1, 0, 1, 1000];
        // Both should produce monotonically increasing byte sequences
        let sp_encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&spb, v)).collect();
        let i_encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&ib, v)).collect();
        for i in 0..sp_encoded.len() - 1 {
            assert!(
                sp_encoded[i] < sp_encoded[i + 1],
                "SortedPackedIntBinding sort order violated for {} < {}",
                values[i],
                values[i + 1]
            );
            assert!(
                i_encoded[i] < i_encoded[i + 1],
                "IntBinding sort order violated for {} < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_i8_sort_order() {
        let values: Vec<i8> = vec![-128, -1, 0, 1, 127];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| {
                let mut out = TupleOutput::new();
                out.write_i8(v);
                out.to_vec()
            })
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "i8 sort order: {} should be < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    // -----------------------------------------------------------------------
    // Ported from TupleBindingTest: byte-size assertions per binding type
    // -----------------------------------------------------------------------

    /// TupleBindingTest: BoolBinding encodes to exactly 1 byte.
    #[test]
    fn test_bool_binding_encoded_size() {
        let binding = BoolBinding::new();
        for val in [true, false] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                1,
                "bool {} should encode to 1 byte",
                val
            );
        }
    }

    /// TupleBindingTest: ByteBinding encodes to exactly 1 byte.
    #[test]
    fn test_byte_binding_encoded_size() {
        let binding = ByteBinding::new();
        for val in [0u8, 1, 123, 255] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                1,
                "u8 {} should encode to 1 byte",
                val
            );
        }
    }

    /// TupleBindingTest: ShortBinding encodes to exactly 2 bytes.
    #[test]
    fn test_short_binding_encoded_size() {
        let binding = ShortBinding::new();
        for val in [i16::MIN, -1i16, 0, 1, 123, i16::MAX] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                2,
                "i16 {} should encode to 2 bytes",
                val
            );
        }
    }

    /// TupleBindingTest: FloatBinding encodes to exactly 4 bytes.
    #[test]
    fn test_float_binding_encoded_size() {
        let binding = FloatBinding::new();
        for val in [0.0f32, 1.0, -1.0, 123.123, f32::MAX, f32::MIN, f32::NAN] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                4,
                "f32 {} should encode to 4 bytes",
                val
            );
        }
    }

    /// TupleBindingTest: DoubleBinding encodes to exactly 8 bytes.
    #[test]
    fn test_double_binding_encoded_size() {
        let binding = DoubleBinding::new();
        for val in [0.0f64, 1.0, -1.0, 123.123, f64::MAX, f64::MIN, f64::NAN] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                8,
                "f64 {} should encode to 8 bytes",
                val
            );
        }
    }

    /// TupleBindingTest: SortedFloatBinding encodes to exactly 4 bytes.
    #[test]
    fn test_sorted_float_binding_encoded_size() {
        let binding = SortedFloatBinding::new();
        for val in
            [0.0f32, 1.0, -1.0, 123.123, f32::MAX, f32::NEG_INFINITY, f32::NAN]
        {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                4,
                "sorted_f32 {} should encode to 4 bytes",
                val
            );
        }
    }

    /// TupleBindingTest: SortedDoubleBinding encodes to exactly 8 bytes.
    #[test]
    fn test_sorted_double_binding_encoded_size() {
        let binding = SortedDoubleBinding::new();
        for val in
            [0.0f64, 1.0, -1.0, 123.123, f64::MAX, f64::NEG_INFINITY, f64::NAN]
        {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                8,
                "sorted_f64 {} should encode to 8 bytes",
                val
            );
        }
    }

    /// TupleBindingTest: CharBinding encodes to exactly 2 bytes.
    #[test]
    fn test_char_binding_encoded_size_variants() {
        let binding = CharBinding::new();
        for val in [0u16, b'a' as u16, 0x7F, 0x00FF, 0x0100, 0xFFFF] {
            let mut entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut entry).unwrap();
            assert_eq!(
                entry.data().len(),
                2,
                "char U+{:04X} should encode to 2 bytes",
                val
            );
        }
    }

    /// TupleBindingTest: StringBinding "abc" encodes to 5 bytes (3 UTF-8 + 2-byte terminator).
    #[test]
    fn test_string_binding_abc_size() {
        let binding = StringBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&"abc".to_string(), &mut entry).unwrap();
        // Rust uses 2-byte null terminator, so "abc" = 3 + 2 = 5 bytes
        assert_eq!(entry.data().len(), 5);
    }

    /// TupleBindingTest: StringBinding for null-equivalent (empty) encodes to 2 bytes.
    #[test]
    fn test_string_binding_empty_size() {
        let binding = StringBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&String::new(), &mut entry).unwrap();
        // Just the 2-byte null terminator
        assert_eq!(entry.data().len(), 2);
    }

    /// TupleBindingTest: nested binding — write prefix, value, suffix; read all back.
    /// Ported from TupleBindingTest.forMoreCoverageTest.
    #[test]
    fn test_nested_binding_coverage_int() {
        use crate::bind::tuple::tuple_binding::TupleBinding;
        let binding = IntBinding::new();
        let mut out = TupleOutput::new();
        out.write_string("abc");
        binding.object_to_tuple(&123i32, &mut out).unwrap();
        out.write_string("xyz");

        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        let val: i32 = binding.tuple_to_object(&mut inp).unwrap();
        assert_eq!(val, 123);
        assert_eq!(inp.read_string().unwrap(), "xyz");
        assert_eq!(inp.available(), 0);
    }

    /// TupleBindingTest: nested binding for long.
    #[test]
    fn test_nested_binding_coverage_long() {
        use crate::bind::tuple::tuple_binding::TupleBinding;
        let binding = LongBinding::new();
        let mut out = TupleOutput::new();
        out.write_string("abc");
        binding.object_to_tuple(&123i64, &mut out).unwrap();
        out.write_string("xyz");

        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        let val: i64 = binding.tuple_to_object(&mut inp).unwrap();
        assert_eq!(val, 123);
        assert_eq!(inp.read_string().unwrap(), "xyz");
        assert_eq!(inp.available(), 0);
    }

    /// TupleBindingTest: nested binding for bool.
    #[test]
    fn test_nested_binding_coverage_bool() {
        use crate::bind::tuple::tuple_binding::TupleBinding;
        let binding = BoolBinding::new();
        let mut out = TupleOutput::new();
        out.write_string("abc");
        binding.object_to_tuple(&true, &mut out).unwrap();
        out.write_string("xyz");

        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        let val: bool = binding.tuple_to_object(&mut inp).unwrap();
        assert!(val);
        assert_eq!(inp.read_string().unwrap(), "xyz");
        assert_eq!(inp.available(), 0);
    }

    /// TupleBindingTest: nested binding for sorted float.
    #[test]
    fn test_nested_binding_coverage_sorted_float() {
        use crate::bind::tuple::tuple_binding::TupleBinding;
        let binding = SortedFloatBinding::new();
        let val = 123.123f32;
        let mut out = TupleOutput::new();
        out.write_string("abc");
        binding.object_to_tuple(&val, &mut out).unwrap();
        out.write_string("xyz");

        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        let got: f32 = binding.tuple_to_object(&mut inp).unwrap();
        assert_eq!(got.to_bits(), val.to_bits());
        assert_eq!(inp.read_string().unwrap(), "xyz");
        assert_eq!(inp.available(), 0);
    }

    /// TupleBindingTest: nested binding for sorted double.
    #[test]
    fn test_nested_binding_coverage_sorted_double() {
        use crate::bind::tuple::tuple_binding::TupleBinding;
        let binding = SortedDoubleBinding::new();
        let val = 123.123f64;
        let mut out = TupleOutput::new();
        out.write_string("abc");
        binding.object_to_tuple(&val, &mut out).unwrap();
        out.write_string("xyz");

        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        let got: f64 = binding.tuple_to_object(&mut inp).unwrap();
        assert_eq!(got.to_bits(), val.to_bits());
        assert_eq!(inp.read_string().unwrap(), "xyz");
        assert_eq!(inp.available(), 0);
    }

    /// TupleBindingTest: nested binding for packed int.
    #[test]
    fn test_nested_binding_coverage_packed_int() {
        use crate::bind::tuple::tuple_binding::TupleBinding;
        let binding = PackedIntBinding::new();
        let mut out = TupleOutput::new();
        out.write_string("abc");
        binding.object_to_tuple(&1234i32, &mut out).unwrap();
        out.write_string("xyz");

        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        let val: i32 = binding.tuple_to_object(&mut inp).unwrap();
        assert_eq!(val, 1234);
        assert_eq!(inp.read_string().unwrap(), "xyz");
        assert_eq!(inp.available(), 0);
    }

    /// TupleBindingTest: nested binding for packed long.
    #[test]
    fn test_nested_binding_coverage_packed_long() {
        use crate::bind::tuple::tuple_binding::TupleBinding;
        let binding = PackedLongBinding::new();
        let mut out = TupleOutput::new();
        out.write_string("abc");
        binding.object_to_tuple(&1234i64, &mut out).unwrap();
        out.write_string("xyz");

        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        let val: i64 = binding.tuple_to_object(&mut inp).unwrap();
        assert_eq!(val, 1234);
        assert_eq!(inp.read_string().unwrap(), "xyz");
        assert_eq!(inp.available(), 0);
    }

    /// TupleBindingTest: PackedIntBinding 1234 encodes correctly.
    ///
    /// `PackedIntegerBinding.intToEntry(1234)` produces 5 bytes because uses
    /// an unsigned packed format that always uses fixed-width headers. Our Rust port
    /// uses a signed variable-length format where 1234 = 119 + 1115, which fits in
    /// 2 value bytes → 3 bytes total (1 header + 2 value). We test our actual encoding.
    #[test]
    fn test_packed_int_binding_1234_size() {
        let binding = PackedIntBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&1234i32, &mut entry).unwrap();
        // Our signed variable-length format: 1234 - 119 = 1115, fits in 2 bytes → 3 bytes
        assert_eq!(
            entry.data().len(),
            3,
            "1234 should encode to 3 bytes (header + 2 value bytes)"
        );
        // Round-trip correctness
        let got = binding.entry_to_object(&entry).unwrap();
        assert_eq!(got, 1234i32);
    }

    /// TupleBindingTest: PackedLongBinding 1234 encodes correctly.
    ///
    /// `PackedLongBinding.longToEntry(1234)` produces 9 bytes (fixed-width unsigned
    /// format). Our Rust port uses a signed variable-length format: 1234 - 119 = 1115,
    /// fits in 2 bytes → 3 bytes total (1 header + 2 value).
    #[test]
    fn test_packed_long_binding_1234_size() {
        let binding = PackedLongBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&1234i64, &mut entry).unwrap();
        // Our signed variable-length format: 1234 - 119 = 1115, fits in 2 bytes → 3 bytes
        assert_eq!(
            entry.data().len(),
            3,
            "1234L should encode to 3 bytes (header + 2 value bytes)"
        );
        let got = binding.entry_to_object(&entry).unwrap();
        assert_eq!(got, 1234i64);
    }

    /// TupleBindingTest: SortedPackedIntBinding 1234 encodes correctly.
    ///
    /// produces 5 bytes for 1234 in SortedPackedIntegerBinding. Our Rust implementation
    /// encodes 1234 in the sorted packed format: it is above the 1-byte threshold (120),
    /// so it uses a 2-byte encoding (1 header + 1 value byte for values up to 0xFF+121=376),
    /// except 1234 = 0xFF + 122 + remainder, needing 3 bytes (header + 2 value bytes).
    #[test]
    fn test_sorted_packed_int_binding_1234_size() {
        let binding = SortedPackedIntBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&1234i32, &mut entry).unwrap();
        // 1234 > 0xFF + 121 = 376, so needs 3 bytes in sorted packed format
        assert_eq!(
            entry.data().len(),
            3,
            "1234 should encode to 3 bytes in SortedPackedIntBinding"
        );
        let got = binding.entry_to_object(&entry).unwrap();
        assert_eq!(got, 1234i32);
    }

    /// TupleBindingTest: SortedPackedLongBinding 1234 encodes correctly.
    #[test]
    fn test_sorted_packed_long_binding_1234_size() {
        let binding = SortedPackedLongBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&1234i64, &mut entry).unwrap();
        // 1234 > 0xFF + 121 = 376, so needs 3 bytes in sorted packed format
        assert_eq!(
            entry.data().len(),
            3,
            "1234L should encode to 3 bytes in SortedPackedLongBinding"
        );
        let got = binding.entry_to_object(&entry).unwrap();
        assert_eq!(got, 1234i64);
    }

    // -----------------------------------------------------------------------
    // Ported from SerialBindingTest: primitive type round-trips via EntryBinding
    // -----------------------------------------------------------------------

    /// SerialBindingTest.testPrimitiveBindings: all primitive types via their bindings.
    #[test]
    fn test_all_primitive_bindings_round_trip() {
        // bool
        {
            let b = BoolBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&true, &mut e).unwrap();
            assert!(b.entry_to_object(&e).unwrap());
        }
        // u8 (byte)
        {
            let b = ByteBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&123u8, &mut e).unwrap();
            assert_eq!(b.entry_to_object(&e).unwrap(), 123u8);
        }
        // i16 (short)
        {
            let b = ShortBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&123i16, &mut e).unwrap();
            assert_eq!(b.entry_to_object(&e).unwrap(), 123i16);
        }
        // i32 (int)
        {
            let b = IntBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&123i32, &mut e).unwrap();
            assert_eq!(b.entry_to_object(&e).unwrap(), 123i32);
        }
        // i64 (long)
        {
            let b = LongBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&123i64, &mut e).unwrap();
            assert_eq!(b.entry_to_object(&e).unwrap(), 123i64);
        }
        // f32 (float)
        {
            let b = FloatBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&123.123f32, &mut e).unwrap();
            let got = b.entry_to_object(&e).unwrap();
            assert!((got - 123.123f32).abs() < 1e-3);
        }
        // f64 (double)
        {
            let b = DoubleBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&123.123f64, &mut e).unwrap();
            let got = b.entry_to_object(&e).unwrap();
            assert!((got - 123.123f64).abs() < 1e-9);
        }
        // u16 (char)
        {
            let b = CharBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&(b'a' as u16), &mut e).unwrap();
            assert_eq!(b.entry_to_object(&e).unwrap(), b'a' as u16);
        }
        // String
        {
            let b = StringBinding::new();
            let mut e = DatabaseEntry::new();
            b.object_to_entry(&"abc".to_string(), &mut e).unwrap();
            assert_eq!(b.entry_to_object(&e).unwrap(), "abc".to_string());
        }
    }

    // -----------------------------------------------------------------------
    // Ported from TupleOrderingTest: string ordering with sorted sequence
    // -----------------------------------------------------------------------

    /// TupleOrderingTest.testString: a sorted sequence of strings sorts correctly.
    #[test]
    fn test_string_ordering_sorted_sequence() {
        let binding = StringBinding::new();
        // Sequence taken from TupleOrderingTest DATA array (subset — no embedded
        // chars beyond ASCII here since our format is UTF-8, not Java modified UTF-8).
        let data = vec![
            "".to_string(),
            "\u{0001}".to_string(),
            "\u{0002}".to_string(),
            "A".to_string(),
            "a".to_string(),
            "ab".to_string(),
            "b".to_string(),
            "bb".to_string(),
            "bba".to_string(),
            "c".to_string(),
        ];
        let encoded: Vec<Vec<u8>> =
            data.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "string ordering violated: {:?} should sort before {:?}",
                data[i],
                data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testFixedString / testBytes: raw bytes ordering.
    #[test]
    fn test_u8_binding_ordering() {
        // Raw u8 values sort in ascending numeric order.
        let binding = ByteBinding::new();
        let values: Vec<u8> = vec![0, 1, 0x7F, 0xFF];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| encoded_bytes(&binding, v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "u8 binding ordering violated: {} should sort before {}",
                values[i],
                values[i + 1]
            );
        }
    }
}
