//! Minimal binary serde Serializer and Deserializer.
//!
//! A compact, self-contained binary format replacing Java's `java.io.Serializable`.
//! Uses big-endian encoding for numeric types, length-prefixed strings and byte
//! arrays, and variant-index-based enum encoding.
//!
//! ## Format
//!
//! - **bool**: 1 byte (0 = false, 1 = true)
//! - **u8/i8**: 1 byte
//! - **u16/i16..u64/i64**: fixed-width big-endian
//! - **u128/i128**: 16 bytes big-endian
//! - **f32/f64**: IEEE 754 big-endian
//! - **char**: 4 bytes (u32 big-endian)
//! - **string**: 4-byte length (u32 BE) + UTF-8 bytes
//! - **bytes**: 4-byte length (u32 BE) + raw bytes
//! - **Option**: 1 byte tag (0=None, 1=Some) + value if Some
//! - **Sequence/Tuple**: 4-byte count (u32 BE) + elements
//! - **Map**: 4-byte count (u32 BE) + key-value pairs
//! - **Struct**: fields serialized in order (no length prefix)
//! - **Enum**: 4-byte variant index (u32 BE) + variant fields
//! - **Unit**: 0 bytes
//!
//! ## Required dependencies (to be added to Cargo.toml)
//!
//! ```toml
//! serde = { version = "1", features = ["derive"] }
//! ```

use std::fmt;

use serde::de::{
    self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use serde::ser::{
    self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Deserialize, Serialize};

use crate::BindError;
use crate::Result;

// ---------------------------------------------------------------------------
// Serializer
// ---------------------------------------------------------------------------

/// A minimal binary serializer implementing `serde::Serializer`.
///
/// Writes values to an internal byte buffer using a compact big-endian format.
pub struct SimpleSerializer {
    output: Vec<u8>,
}

impl SimpleSerializer {
    /// Creates a new serializer with an empty output buffer.
    pub fn new() -> Self {
        Self { output: Vec::new() }
    }

    /// Creates a new serializer with a pre-allocated buffer.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { output: Vec::with_capacity(capacity) }
    }

    /// Consumes the serializer and returns the serialized bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.output
    }
}

impl Default for SimpleSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializes a value to bytes using the simple binary format.
pub fn to_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut serializer = SimpleSerializer::new();
    value.serialize(&mut serializer)?;
    Ok(serializer.into_bytes())
}

/// Deserializes a value from bytes using the simple binary format.
pub fn from_bytes<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    let mut deserializer = SimpleDeserializer::new(bytes);
    let value = T::deserialize(&mut deserializer)?;
    if !deserializer.remaining().is_empty() {
        return Err(BindError::InvalidData(format!(
            "trailing {} bytes after deserialization",
            deserializer.remaining().len()
        )));
    }
    Ok(value)
}

// -- serde error bridge --

/// Serialization/deserialization error wrapper for the serde trait implementations.
///
/// Wraps [`BindError`] and implements `serde::ser::Error` and `serde::de::Error`.
#[derive(Debug)]
pub struct SerError(BindError);

impl fmt::Display for SerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SerError {}

impl ser::Error for SerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SerError(BindError::InvalidData(msg.to_string()))
    }
}

impl de::Error for SerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SerError(BindError::InvalidData(msg.to_string()))
    }
}

impl From<BindError> for SerError {
    fn from(e: BindError) -> Self {
        SerError(e)
    }
}

impl From<SerError> for BindError {
    fn from(e: SerError) -> Self {
        e.0
    }
}

// -- Serializer impl --

impl ser::Serializer for &mut SimpleSerializer {
    type Ok = ();
    type Error = SerError;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> std::result::Result<(), SerError> {
        self.output.push(if v { 1 } else { 0 });
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> std::result::Result<(), SerError> {
        self.output.push(v as u8);
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_i128(self, v: i128) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> std::result::Result<(), SerError> {
        self.output.push(v);
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_char(self, v: char) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&(v as u32).to_be_bytes());
        Ok(())
    }

    fn serialize_str(self, v: &str) -> std::result::Result<(), SerError> {
        let len = v.len() as u32;
        self.output.extend_from_slice(&len.to_be_bytes());
        self.output.extend_from_slice(v.as_bytes());
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<(), SerError> {
        let len = v.len() as u32;
        self.output.extend_from_slice(&len.to_be_bytes());
        self.output.extend_from_slice(v);
        Ok(())
    }

    fn serialize_none(self) -> std::result::Result<(), SerError> {
        self.output.push(0);
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(
        self,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        self.output.push(1);
        value.serialize(self)
    }

    fn serialize_unit(self) -> std::result::Result<(), SerError> {
        Ok(())
    }

    fn serialize_unit_struct(
        self,
        _name: &'static str,
    ) -> std::result::Result<(), SerError> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&variant_index.to_be_bytes());
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        self.output.extend_from_slice(&variant_index.to_be_bytes());
        value.serialize(self)
    }

    fn serialize_seq(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, SerError> {
        let count = len.ok_or_else(|| {
            SerError(BindError::UnsupportedType(
                "sequences must have known length".to_string(),
            ))
        })? as u32;
        self.output.extend_from_slice(&count.to_be_bytes());
        Ok(self)
    }

    fn serialize_tuple(
        self,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTuple, SerError> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, SerError> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, SerError> {
        self.output.extend_from_slice(&variant_index.to_be_bytes());
        Ok(self)
    }

    fn serialize_map(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, SerError> {
        let count = len.ok_or_else(|| {
            SerError(BindError::UnsupportedType(
                "maps must have known length".to_string(),
            ))
        })? as u32;
        self.output.extend_from_slice(&count.to_be_bytes());
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeStruct, SerError> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, SerError> {
        self.output.extend_from_slice(&variant_index.to_be_bytes());
        Ok(self)
    }
}

impl SerializeSeq for &mut SimpleSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_element<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<(), SerError> {
        Ok(())
    }
}

impl SerializeTuple for &mut SimpleSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_element<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<(), SerError> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut SimpleSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<(), SerError> {
        Ok(())
    }
}

impl SerializeTupleVariant for &mut SimpleSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<(), SerError> {
        Ok(())
    }
}

impl SerializeMap for &mut SimpleSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_key<T: ?Sized + Serialize>(
        &mut self,
        key: &T,
    ) -> std::result::Result<(), SerError> {
        key.serialize(&mut **self)
    }

    fn serialize_value<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<(), SerError> {
        Ok(())
    }
}

impl SerializeStruct for &mut SimpleSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<(), SerError> {
        Ok(())
    }
}

impl SerializeStructVariant for &mut SimpleSerializer {
    type Ok = ();
    type Error = SerError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> std::result::Result<(), SerError> {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<(), SerError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Deserializer
// ---------------------------------------------------------------------------

/// A minimal binary deserializer implementing `serde::Deserializer`.
///
/// Reads values from a byte slice using the compact big-endian format
/// produced by [`SimpleSerializer`].
pub struct SimpleDeserializer<'de> {
    input: &'de [u8],
    pos: usize,
}

impl<'de> SimpleDeserializer<'de> {
    /// Creates a new deserializer from a byte slice.
    pub fn new(input: &'de [u8]) -> Self {
        Self { input, pos: 0 }
    }

    /// Returns the remaining unread bytes.
    pub fn remaining(&self) -> &'de [u8] {
        &self.input[self.pos..]
    }

    fn read_bytes(
        &mut self,
        n: usize,
    ) -> std::result::Result<&'de [u8], SerError> {
        if self.pos + n > self.input.len() {
            return Err(SerError(BindError::BufferUnderflow {
                needed: n,
                available: self.input.len() - self.pos,
            }));
        }
        let slice = &self.input[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u8(&mut self) -> std::result::Result<u8, SerError> {
        let b = self.read_bytes(1)?;
        Ok(b[0])
    }

    fn read_u16(&mut self) -> std::result::Result<u16, SerError> {
        let b = self.read_bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn read_u32(&mut self) -> std::result::Result<u32, SerError> {
        let b = self.read_bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_u64(&mut self) -> std::result::Result<u64, SerError> {
        let b = self.read_bytes(8)?;
        Ok(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    fn read_u128(&mut self) -> std::result::Result<u128, SerError> {
        let b = self.read_bytes(16)?;
        let mut arr = [0u8; 16];
        arr.copy_from_slice(b);
        Ok(u128::from_be_bytes(arr))
    }
}

impl<'de> de::Deserializer<'de> for &mut SimpleDeserializer<'de> {
    type Error = SerError;

    fn deserialize_any<V: Visitor<'de>>(
        self,
        _visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        Err(SerError(BindError::UnsupportedType(
            "deserialize_any is not supported; use a typed deserialize method"
                .to_string(),
        )))
    }

    fn deserialize_bool<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let b = self.read_u8()?;
        visitor.visit_bool(b != 0)
    }

    fn deserialize_i8<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let b = self.read_u8()?;
        visitor.visit_i8(b as i8)
    }

    fn deserialize_i16<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u16()?;
        visitor.visit_i16(v as i16)
    }

    fn deserialize_i32<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u32()?;
        visitor.visit_i32(v as i32)
    }

    fn deserialize_i64<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u64()?;
        visitor.visit_i64(v as i64)
    }

    fn deserialize_i128<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u128()?;
        visitor.visit_i128(v as i128)
    }

    fn deserialize_u8<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let b = self.read_u8()?;
        visitor.visit_u8(b)
    }

    fn deserialize_u16<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u16()?;
        visitor.visit_u16(v)
    }

    fn deserialize_u32<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u32()?;
        visitor.visit_u32(v)
    }

    fn deserialize_u64<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u64()?;
        visitor.visit_u64(v)
    }

    fn deserialize_u128<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u128()?;
        visitor.visit_u128(v)
    }

    fn deserialize_f32<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u32()?;
        visitor.visit_f32(f32::from_bits(v))
    }

    fn deserialize_f64<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u64()?;
        visitor.visit_f64(f64::from_bits(v))
    }

    fn deserialize_char<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let v = self.read_u32()?;
        let c = char::from_u32(v).ok_or_else(|| {
            SerError(BindError::InvalidData(format!(
                "invalid char code point: {}",
                v
            )))
        })?;
        visitor.visit_char(c)
    }

    fn deserialize_str<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        let s = std::str::from_utf8(bytes)
            .map_err(|e| SerError(BindError::StringEncoding(e.to_string())))?;
        visitor.visit_borrowed_str(s)
    }

    fn deserialize_string<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        visitor.visit_borrowed_bytes(bytes)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let tag = self.read_u8()?;
        match tag {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            _ => Err(SerError(BindError::InvalidData(format!(
                "invalid option tag: {}",
                tag
            )))),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let count = self.read_u32()? as usize;
        visitor.visit_seq(CountedAccess { de: self, remaining: count })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_seq(CountedAccess { de: self, remaining: len })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_seq(CountedAccess { de: self, remaining: len })
    }

    fn deserialize_map<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let count = self.read_u32()? as usize;
        visitor.visit_map(CountedAccess { de: self, remaining: count })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_seq(CountedAccess { de: self, remaining: fields.len() })
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_enum(EnumDeserializer { de: self })
    }

    fn deserialize_identifier<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        let idx = self.read_u32()?;
        visitor.visit_u32(idx)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(
        self,
        _visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        Err(SerError(BindError::UnsupportedType(
            "deserialize_ignored_any is not supported".to_string(),
        )))
    }
}

// -- sequence / map access --

struct CountedAccess<'a, 'de> {
    de: &'a mut SimpleDeserializer<'de>,
    remaining: usize,
}

impl<'de, 'a> SeqAccess<'de> for CountedAccess<'a, 'de> {
    type Error = SerError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> std::result::Result<Option<T::Value>, SerError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
}

impl<'de, 'a> MapAccess<'de> for CountedAccess<'a, 'de> {
    type Error = SerError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> std::result::Result<Option<K::Value>, SerError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> std::result::Result<V::Value, SerError> {
        seed.deserialize(&mut *self.de)
    }
}

// -- enum access --

struct EnumDeserializer<'a, 'de> {
    de: &'a mut SimpleDeserializer<'de>,
}

impl<'de, 'a> EnumAccess<'de> for EnumDeserializer<'a, 'de> {
    type Error = SerError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> std::result::Result<(V::Value, Self::Variant), SerError> {
        let idx = self.de.read_u32()?;
        let val = seed.deserialize(u32_into_deserializer(idx))?;
        Ok((val, self))
    }
}

struct U32Deserializer(u32);

fn u32_into_deserializer(v: u32) -> U32Deserializer {
    U32Deserializer(v)
}

impl<'de> de::Deserializer<'de> for U32Deserializer {
    type Error = SerError;

    fn deserialize_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_u32(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

impl<'de, 'a> VariantAccess<'de> for EnumDeserializer<'a, 'de> {
    type Error = SerError;

    fn unit_variant(self) -> std::result::Result<(), SerError> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> std::result::Result<T::Value, SerError> {
        seed.deserialize(self.de)
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor.visit_seq(CountedAccess { de: self.de, remaining: len })
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, SerError> {
        visitor
            .visit_seq(CountedAccess { de: self.de, remaining: fields.len() })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    fn round_trip<
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + fmt::Debug,
    >(
        val: &T,
    ) {
        let bytes = to_bytes(val).expect("serialize");
        let decoded: T = from_bytes(&bytes).expect("deserialize");
        assert_eq!(&decoded, val);
    }

    #[test]
    fn test_bool_true() {
        round_trip(&true);
    }

    #[test]
    fn test_bool_false() {
        round_trip(&false);
    }

    #[test]
    fn test_u8() {
        round_trip(&42u8);
        round_trip(&0u8);
        round_trip(&255u8);
    }

    #[test]
    fn test_i8() {
        round_trip(&-1i8);
        round_trip(&127i8);
        round_trip(&-128i8);
    }

    #[test]
    fn test_u16() {
        round_trip(&0u16);
        round_trip(&12345u16);
        round_trip(&u16::MAX);
    }

    #[test]
    fn test_i16() {
        round_trip(&-1i16);
        round_trip(&i16::MIN);
        round_trip(&i16::MAX);
    }

    #[test]
    fn test_u32() {
        round_trip(&0u32);
        round_trip(&1_000_000u32);
        round_trip(&u32::MAX);
    }

    #[test]
    fn test_i32() {
        round_trip(&-42i32);
        round_trip(&i32::MIN);
        round_trip(&i32::MAX);
    }

    #[test]
    fn test_u64() {
        round_trip(&0u64);
        round_trip(&u64::MAX);
    }

    #[test]
    fn test_i64() {
        round_trip(&-99i64);
        round_trip(&i64::MIN);
        round_trip(&i64::MAX);
    }

    #[test]
    fn test_u128() {
        round_trip(&0u128);
        round_trip(&u128::MAX);
    }

    #[test]
    fn test_i128() {
        round_trip(&-1i128);
        round_trip(&i128::MAX);
    }

    #[test]
    fn test_f32() {
        round_trip(&3.14f32);
        round_trip(&0.0f32);
        round_trip(&f32::NEG_INFINITY);
    }

    #[test]
    fn test_f64() {
        round_trip(&2.71828f64);
        round_trip(&f64::MAX);
        round_trip(&f64::MIN);
    }

    #[test]
    fn test_char() {
        round_trip(&'a');
        round_trip(&'\u{1F600}'); // emoji
        round_trip(&'\0');
    }

    #[test]
    fn test_string_empty() {
        round_trip(&String::new());
    }

    #[test]
    fn test_string_ascii() {
        round_trip(&"hello world".to_string());
    }

    #[test]
    fn test_string_unicode() {
        round_trip(&"cafe\u{0301} \u{1F600}".to_string());
    }

    #[test]
    fn test_option_none() {
        let v: Option<u32> = None;
        round_trip(&v);
    }

    #[test]
    fn test_option_some() {
        round_trip(&Some(42u32));
        round_trip(&Some("hello".to_string()));
    }

    #[test]
    fn test_vec_empty() {
        let v: Vec<u32> = vec![];
        round_trip(&v);
    }

    #[test]
    fn test_vec_ints() {
        round_trip(&vec![1u32, 2, 3, 4, 5]);
    }

    #[test]
    fn test_vec_strings() {
        round_trip(&vec!["a".to_string(), "bb".to_string(), "ccc".to_string()]);
    }

    #[test]
    fn test_tuple() {
        round_trip(&(1u32, 2u64, true));
    }

    #[test]
    fn test_nested_option() {
        let v: Option<Option<u32>> = Some(Some(42));
        round_trip(&v);
        let v2: Option<Option<u32>> = Some(None);
        round_trip(&v2);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Simple {
        x: u32,
        y: String,
    }

    #[test]
    fn test_struct_simple() {
        round_trip(&Simple { x: 42, y: "hello".to_string() });
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Nested {
        inner: Simple,
        flag: bool,
    }

    #[test]
    fn test_struct_nested() {
        round_trip(&Nested {
            inner: Simple { x: 100, y: "nested".to_string() },
            flag: true,
        });
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct WithOption {
        name: String,
        value: Option<u64>,
    }

    #[test]
    fn test_struct_with_option() {
        round_trip(&WithOption { name: "test".to_string(), value: Some(999) });
        round_trip(&WithOption { name: "empty".to_string(), value: None });
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Color {
        Red,
        Green,
        Blue,
    }

    #[test]
    fn test_enum_unit_variants() {
        round_trip(&Color::Red);
        round_trip(&Color::Green);
        round_trip(&Color::Blue);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Shape {
        Circle(f64),
        Rectangle(f64, f64),
        Named { name: String, sides: u32 },
    }

    #[test]
    fn test_enum_newtype_variant() {
        round_trip(&Shape::Circle(3.14));
    }

    #[test]
    fn test_enum_tuple_variant() {
        round_trip(&Shape::Rectangle(10.0, 20.0));
    }

    #[test]
    fn test_enum_struct_variant() {
        round_trip(&Shape::Named { name: "pentagon".to_string(), sides: 5 });
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct UnitStruct;

    #[test]
    fn test_unit_struct() {
        round_trip(&UnitStruct);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct NewtypeStruct(u64);

    #[test]
    fn test_newtype_struct() {
        round_trip(&NewtypeStruct(42));
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TupleStruct(u32, String, bool);

    #[test]
    fn test_tuple_struct() {
        round_trip(&TupleStruct(1, "x".to_string(), false));
    }

    #[test]
    fn test_vec_of_structs() {
        let v = vec![
            Simple { x: 1, y: "a".to_string() },
            Simple { x: 2, y: "b".to_string() },
        ];
        round_trip(&v);
    }

    #[test]
    fn test_hashmap() {
        // HashMap ordering is non-deterministic, so we test with a single entry
        // for deterministic round-trip, or we just check that the result matches.
        let mut map = HashMap::new();
        map.insert("key".to_string(), 42u32);
        let bytes = to_bytes(&map).expect("serialize");
        let decoded: HashMap<String, u32> =
            from_bytes(&bytes).expect("deserialize");
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_hashmap_multi() {
        let mut map = HashMap::new();
        map.insert(1u32, "one".to_string());
        map.insert(2u32, "two".to_string());
        map.insert(3u32, "three".to_string());
        let bytes = to_bytes(&map).expect("serialize");
        let decoded: HashMap<u32, String> =
            from_bytes(&bytes).expect("deserialize");
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_buffer_underflow() {
        let result: std::result::Result<u32, _> = from_bytes(&[0, 0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_trailing_bytes_error() {
        let bytes = to_bytes(&42u32).expect("serialize");
        let mut padded = bytes.clone();
        padded.push(0xFF);
        let result: std::result::Result<u32, _> = from_bytes(&padded);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_bytes() {
        let result: std::result::Result<u32, _> = from_bytes(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_bytes_serialization() {
        // serde_bytes style: Vec<u8> serializes as a sequence, not bytes
        // We test Vec<u8> round-trip (which uses seq encoding)
        round_trip(&vec![1u8, 2u8, 3u8]);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Complex {
        id: u64,
        name: String,
        tags: Vec<String>,
        metadata: Option<Nested>,
        color: Color,
    }

    #[test]
    fn test_complex_struct() {
        round_trip(&Complex {
            id: 12345,
            name: "complex test".to_string(),
            tags: vec!["a".to_string(), "b".to_string()],
            metadata: Some(Nested {
                inner: Simple { x: 7, y: "deep".to_string() },
                flag: false,
            }),
            color: Color::Blue,
        });
    }

    #[test]
    fn test_complex_struct_none_metadata() {
        round_trip(&Complex {
            id: 0,
            name: String::new(),
            tags: vec![],
            metadata: None,
            color: Color::Red,
        });
    }

    #[test]
    fn test_bool_encoding() {
        let bytes = to_bytes(&true).unwrap();
        assert_eq!(bytes, vec![1]);
        let bytes = to_bytes(&false).unwrap();
        assert_eq!(bytes, vec![0]);
    }

    #[test]
    fn test_u32_encoding_big_endian() {
        let bytes = to_bytes(&0x01020304u32).unwrap();
        assert_eq!(bytes, vec![0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_string_encoding_format() {
        let bytes = to_bytes(&"hi".to_string()).unwrap();
        // 4-byte length (2) + 2 bytes "hi"
        assert_eq!(bytes, vec![0, 0, 0, 2, b'h', b'i']);
    }

    #[test]
    fn test_option_encoding_format() {
        let bytes_none = to_bytes(&Option::<u8>::None).unwrap();
        assert_eq!(bytes_none, vec![0]);

        let bytes_some = to_bytes(&Some(42u8)).unwrap();
        assert_eq!(bytes_some, vec![1, 42]);
    }

    // ── constructor / factory coverage ───────────────────────────────────────

    #[test]
    fn test_with_capacity() {
        let mut ser = SimpleSerializer::with_capacity(64);
        ser.output.push(0x42);
        let bytes = ser.into_bytes();
        assert_eq!(bytes, vec![0x42]);
    }

    #[test]
    fn test_default_serializer() {
        let ser = SimpleSerializer::default();
        let bytes = ser.into_bytes();
        assert!(bytes.is_empty());
    }

    // ── f32 / f64 edge cases ─────────────────────────────────────────────────

    #[test]
    fn test_f32_nan_round_trip() {
        // NaN != NaN by IEEE 754, so we compare the bit patterns directly.
        let bytes = to_bytes(&f32::NAN).unwrap();
        assert_eq!(bytes.len(), 4);
        let bits = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert!(f32::from_bits(bits).is_nan());
    }

    #[test]
    fn test_f64_nan_round_trip() {
        let bytes = to_bytes(&f64::NAN).unwrap();
        assert_eq!(bytes.len(), 8);
        let bits = u64::from_be_bytes(bytes.try_into().unwrap());
        assert!(f64::from_bits(bits).is_nan());
    }

    #[test]
    fn test_f32_infinity_round_trip() {
        round_trip(&f32::INFINITY);
    }

    #[test]
    fn test_f64_infinity_round_trip() {
        round_trip(&f64::INFINITY);
        round_trip(&f64::NEG_INFINITY);
    }

    #[test]
    fn test_f32_zero_round_trip() {
        round_trip(&0.0f32);
        round_trip(&-0.0f32);
    }

    #[test]
    fn test_f64_zero_round_trip() {
        round_trip(&0.0f64);
    }

    // ── error paths ───────────────────────────────────────────────────────────

    #[test]
    fn test_deserialize_any_returns_error() {
        use serde::de::Deserializer;
        use serde::de::Visitor;
        struct AnyVisitor;
        impl<'de> Visitor<'de> for AnyVisitor {
            type Value = ();
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "anything")
            }
        }
        let mut de = SimpleDeserializer::new(&[]);
        let result = de.deserialize_any(AnyVisitor);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_ignored_any_returns_error() {
        use serde::de::Deserializer;
        use serde::de::IgnoredAny;
        let mut de = SimpleDeserializer::new(&[]);
        let result = de.deserialize_ignored_any(serde::de::IgnoredAny);
        assert!(result.is_err());
        let _ = result; // suppress unused-result warning
        // just verify it fails
        let mut de2 = SimpleDeserializer::new(&[]);
        assert!(de2.deserialize_ignored_any(IgnoredAny).is_err());
    }

    #[test]
    fn test_invalid_option_tag() {
        // Tag value 2 is invalid (only 0=None, 1=Some are valid).
        let bad_bytes = vec![2u8];
        let result: std::result::Result<Option<u8>, _> = from_bytes(&bad_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_char_code_point() {
        // Write an invalid Unicode scalar value (0xD800 is a surrogate).
        let bad_char_bytes = 0xD800u32.to_be_bytes();
        let result: std::result::Result<char, _> = from_bytes(&bad_char_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_seq_with_none_length_is_error() {
        // Sequences without known length should fail serialization.
        // We cannot easily trigger this through the normal serde derive path,
        // but we can call the serializer method directly.
        let mut ser = SimpleSerializer::new();
        use serde::Serializer;
        let result = ser.serialize_seq(None);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_map_with_none_length_is_error() {
        let mut ser = SimpleSerializer::new();
        use serde::Serializer;
        let result = ser.serialize_map(None);
        assert!(result.is_err());
    }

    // ── numeric encoding big-endian byte order ────────────────────────────────

    #[test]
    fn test_i32_encoding_big_endian() {
        let bytes = to_bytes(&-1i32).unwrap();
        assert_eq!(bytes, vec![0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_i64_encoding_big_endian() {
        let bytes = to_bytes(&1i64).unwrap();
        assert_eq!(bytes, vec![0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn test_u128_encoding_length() {
        let bytes = to_bytes(&u128::MAX).unwrap();
        assert_eq!(bytes.len(), 16);
        assert!(bytes.iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn test_i128_encoding_big_endian() {
        let bytes = to_bytes(&0i128).unwrap();
        assert_eq!(bytes.len(), 16);
        assert!(bytes.iter().all(|&b| b == 0));
    }

    // ── char encoding ────────────────────────────────────────────────────────

    #[test]
    fn test_char_encoding_format() {
        let bytes = to_bytes(&'A').unwrap();
        assert_eq!(bytes, vec![0, 0, 0, 0x41]); // 'A' = 0x41
    }

    #[test]
    fn test_char_null_byte() {
        round_trip(&'\0');
        let bytes = to_bytes(&'\0').unwrap();
        assert_eq!(bytes, vec![0, 0, 0, 0]);
    }

    // ── Ported from SerialBindingTest ─────────────────────────────────────────

    /// SerialBindingTest.testPrimitiveBindings: all primitive types via serde.
    #[test]
    fn test_serial_primitive_bindings_all_types() {
        round_trip(&"abc".to_string());
        round_trip(&true);
        round_trip(&false);
        round_trip(&42u8);
        round_trip(&123i16);
        round_trip(&123i32);
        round_trip(&123i64);
        round_trip(&(b'a' as u32)); // char as u32
        // f32 / f64 — compare bits since NaN != NaN
        let f32_bytes = to_bytes(&123.123f32).unwrap();
        let f32_got: f32 = from_bytes(&f32_bytes).unwrap();
        assert!((f32_got - 123.123f32).abs() < 1e-3);
        let f64_bytes = to_bytes(&123.123f64).unwrap();
        let f64_got: f64 = from_bytes(&f64_bytes).unwrap();
        assert!((f64_got - 123.123f64).abs() < 1e-9);
    }

    /// SerialBindingTest.testNullObjects: None serializes and deserializes correctly.
    #[test]
    fn test_serial_null_objects() {
        let none_u32: Option<u32> = None;
        let bytes = to_bytes(&none_u32).unwrap();
        assert!(!bytes.is_empty());
        let got: Option<u32> = from_bytes(&bytes).unwrap();
        assert_eq!(got, None);

        let none_str: Option<String> = None;
        round_trip(&none_str);
    }

    /// SerialBindingTest: round-trip for complex nested objects.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct SerialRecord {
        id: u64,
        name: String,
        value: f64,
        tags: Vec<String>,
        parent: Option<Box<SerialSimple>>,
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct SerialSimple {
        x: i32,
        label: String,
    }

    #[test]
    fn test_serial_complex_object_round_trip() {
        let record = SerialRecord {
            id: 99999,
            name: "test record".to_string(),
            value: 3.14159,
            tags: vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
            parent: Some(Box::new(SerialSimple { x: -42, label: "parent".to_string() })),
        };
        round_trip(&record);
    }

    #[test]
    fn test_serial_complex_object_none_parent() {
        let record = SerialRecord {
            id: 0,
            name: String::new(),
            value: 0.0,
            tags: vec![],
            parent: None,
        };
        round_trip(&record);
    }

    /// SerialBindingTest: round-trip for deeply nested struct.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Level3 { z: u8 }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Level2 { inner: Level3, s: String }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Level1 { mid: Level2, count: u32 }

    #[test]
    fn test_serial_deeply_nested_struct() {
        let v = Level1 {
            mid: Level2 {
                inner: Level3 { z: 255 },
                s: "deep".to_string(),
            },
            count: 1_000_000,
        };
        round_trip(&v);
    }

    /// SerialBindingTest: byte size is non-zero for all types.
    #[test]
    fn test_serial_non_zero_sizes() {
        assert!(!to_bytes(&true).unwrap().is_empty());
        assert!(!to_bytes(&42u8).unwrap().is_empty());
        assert!(!to_bytes(&123i16).unwrap().is_empty());
        assert!(!to_bytes(&123i32).unwrap().is_empty());
        assert!(!to_bytes(&123i64).unwrap().is_empty());
        assert!(!to_bytes(&3.14f32).unwrap().is_empty());
        assert!(!to_bytes(&3.14f64).unwrap().is_empty());
        assert!(!to_bytes(&"hello".to_string()).unwrap().is_empty());
    }

    // ── nested / complex struct round-trips ──────────────────────────────────

    #[test]
    fn test_deeply_nested_option() {
        let v: Option<Option<Option<u32>>> = Some(Some(Some(7)));
        round_trip(&v);
        let none3: Option<Option<Option<u32>>> = None;
        round_trip(&none3);
    }

    #[test]
    fn test_vec_of_options() {
        let v: Vec<Option<u64>> = vec![Some(1), None, Some(3)];
        round_trip(&v);
    }

    #[test]
    fn test_tuple_struct_round_trip() {
        // TupleStruct is already defined above; re-exercise it.
        round_trip(&TupleStruct(999, "tuple".to_string(), true));
    }

    #[test]
    fn test_newtype_struct_round_trip() {
        round_trip(&NewtypeStruct(u64::MAX));
        round_trip(&NewtypeStruct(0));
    }

    // ── SerError display and error conversion ─────────────────────────────────

    #[test]
    fn test_ser_error_display() {
        use serde::ser::Error as _;
        let e = SerError::custom("test error message");
        let s = format!("{}", e);
        assert!(s.contains("test error message"));
    }

    #[test]
    fn test_ser_error_from_bind_error() {
        let bind_err = BindError::InvalidData("oops".to_string());
        let ser_err = SerError::from(bind_err);
        let bind_back = BindError::from(ser_err);
        let s = format!("{}", bind_back);
        assert!(s.contains("oops"));
    }

    #[test]
    fn test_de_error_custom() {
        use serde::de::Error as _;
        let e = SerError::custom("de error");
        let s = format!("{}", e);
        assert!(s.contains("de error"));
    }

    // ── SimpleDeserializer::remaining ────────────────────────────────────────

    #[test]
    fn test_deserializer_remaining_after_partial_read() {
        let bytes = to_bytes(&42u32).unwrap(); // 4 bytes
        let mut de = SimpleDeserializer::new(&bytes);
        // Read 2 bytes manually via read_bytes.
        de.read_bytes(2).unwrap();
        assert_eq!(de.remaining().len(), 2);
    }

    #[test]
    fn test_deserializer_remaining_empty_at_end() {
        let bytes = to_bytes(&1u8).unwrap(); // 1 byte
        let v: u8 = from_bytes(&bytes).unwrap();
        assert_eq!(v, 1);
        // after from_bytes the deserializer consumed all bytes
    }

    // ── unit / unit_struct / unit_variant ─────────────────────────────────────

    #[test]
    fn test_unit_serialization() {
        let bytes = to_bytes(&()).unwrap();
        assert!(bytes.is_empty());
        let _: () = from_bytes(&bytes).unwrap();
    }

    #[test]
    fn test_unit_struct_serialization() {
        round_trip(&UnitStruct);
        let bytes = to_bytes(&UnitStruct).unwrap();
        assert!(bytes.is_empty());
    }

    // ── newtype variant ───────────────────────────────────────────────────────

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Wrapper {
        Val(u32),
        Pair(u16, u16),
    }

    #[test]
    fn test_newtype_variant_round_trip() {
        round_trip(&Wrapper::Val(123));
    }

    #[test]
    fn test_tuple_variant_round_trip() {
        round_trip(&Wrapper::Pair(10, 20));
    }
}
