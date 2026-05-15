//! RESP2 protocol parser and serializer.
//!
//! Implements the Redis Serialization Protocol (version 2) used by redis-cli
//! and all standard Redis client libraries.

use bytes::{Buf, Bytes, BytesMut};

/// A value in the RESP2 protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// Simple string: `+OK\r\n`
    SimpleString(String),
    /// Error: `-ERR message\r\n`
    Error(String),
    /// Integer: `:42\r\n`
    Integer(i64),
    /// Bulk string: `$5\r\nhello\r\n` or null bulk string `$-1\r\n`
    BulkString(Option<Bytes>),
    /// Array: `*2\r\n...` or null array `*-1\r\n`
    Array(Vec<RespValue>),
    /// Explicit null (serialized as `$-1\r\n`).
    Null,
}

impl RespValue {
    /// Create a simple string response.
    pub fn ok() -> Self {
        Self::SimpleString("OK".to_string())
    }

    /// Create a simple string response with custom text.
    pub fn simple(s: impl Into<String>) -> Self {
        Self::SimpleString(s.into())
    }

    /// Create an error response.
    pub fn error(msg: impl Into<String>) -> Self {
        Self::Error(msg.into())
    }

    /// Create an integer response.
    pub fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Create a bulk string response.
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Self::BulkString(Some(data.into()))
    }

    /// Create a null bulk string response.
    pub fn null() -> Self {
        Self::Null
    }

    /// Create an array response.
    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(items)
    }

    /// Serialize this value into bytes suitable for writing to a client.
    pub fn serialize(&self) -> Bytes {
        let mut buf = Vec::with_capacity(64);
        self.write_to(&mut buf);
        Bytes::from(buf)
    }

    /// Write the serialized form into a buffer.
    fn write_to(&self, buf: &mut Vec<u8>) {
        match self {
            Self::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::Error(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::BulkString(Some(data)) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Self::BulkString(None) | Self::Null => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Self::Array(items) => {
                buf.push(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.write_to(buf);
                }
            }
        }
    }
}

/// Attempt to parse one complete RESP value from the buffer.
///
/// Returns `Some(value)` and advances the buffer past the consumed bytes if a
/// complete frame is available. Returns `None` if more data is needed (partial
/// read). Returns `Some(RespValue::Error(...))` on protocol violations so the
/// caller can send an error response and close the connection.
pub fn parse_resp(buf: &mut BytesMut) -> Option<RespValue> {
    if buf.is_empty() {
        return None;
    }

    // Peek at the type byte without consuming.
    let type_byte = buf[0];

    match type_byte {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        _ => {
            // Inline command support: treat any line not starting with a RESP
            // type prefix as a space-separated inline command (redis-cli sends
            // these when used interactively without RESP framing).
            parse_inline(buf)
        }
    }
}

/// Parse an inline (non-RESP) command line.
fn parse_inline(buf: &mut BytesMut) -> Option<RespValue> {
    let crlf = find_crlf(buf)?;
    let line = &buf[..crlf];
    let parts: Vec<RespValue> = line
        .split(|&b| b == b' ')
        .filter(|s| !s.is_empty())
        .map(|s| RespValue::BulkString(Some(Bytes::copy_from_slice(s))))
        .collect();
    buf.advance(crlf + 2);
    Some(RespValue::Array(parts))
}

/// Parse a simple string (`+...\r\n`).
fn parse_simple_string(buf: &mut BytesMut) -> Option<RespValue> {
    let crlf = find_crlf(buf)?;
    let s = String::from_utf8_lossy(&buf[1..crlf]).to_string();
    buf.advance(crlf + 2);
    Some(RespValue::SimpleString(s))
}

/// Parse an error (`-...\r\n`).
fn parse_error(buf: &mut BytesMut) -> Option<RespValue> {
    let crlf = find_crlf(buf)?;
    let s = String::from_utf8_lossy(&buf[1..crlf]).to_string();
    buf.advance(crlf + 2);
    Some(RespValue::Error(s))
}

/// Parse an integer (`:...\r\n`).
fn parse_integer(buf: &mut BytesMut) -> Option<RespValue> {
    let crlf = find_crlf(buf)?;
    let s = std::str::from_utf8(&buf[1..crlf]).ok()?;
    let n: i64 = s.parse().ok()?;
    buf.advance(crlf + 2);
    Some(RespValue::Integer(n))
}

/// Parse a bulk string (`$len\r\ndata\r\n` or `$-1\r\n`).
fn parse_bulk_string(buf: &mut BytesMut) -> Option<RespValue> {
    let crlf = find_crlf(buf)?;
    let len_str = std::str::from_utf8(&buf[1..crlf]).ok()?;
    let len: i64 = len_str.parse().ok()?;

    if len < 0 {
        buf.advance(crlf + 2);
        return Some(RespValue::Null);
    }

    let len = len as usize;
    let total_needed = crlf + 2 + len + 2; // header + data + trailing \r\n
    if buf.len() < total_needed {
        return None; // Need more data.
    }

    let data = Bytes::copy_from_slice(&buf[crlf + 2..crlf + 2 + len]);
    buf.advance(total_needed);
    Some(RespValue::BulkString(Some(data)))
}

/// Parse an array (`*count\r\n...`).
fn parse_array(buf: &mut BytesMut) -> Option<RespValue> {
    let crlf = find_crlf(buf)?;
    let count_str = std::str::from_utf8(&buf[1..crlf]).ok()?;
    let count: i64 = count_str.parse().ok()?;

    if count < 0 {
        buf.advance(crlf + 2);
        return Some(RespValue::Null);
    }

    let count = count as usize;

    // We need to speculatively parse `count` elements. If any element is
    // incomplete, we must NOT consume anything and return None.
    // Strategy: work on a clone of the buffer to avoid partial consumption.
    let mut temp = buf.clone();
    temp.advance(crlf + 2);

    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        match parse_resp(&mut temp) {
            Some(val) => items.push(val),
            None => return None, // Incomplete; wait for more data.
        }
    }

    // All elements parsed successfully. Now advance the real buffer.
    let consumed = buf.len() - temp.len();
    buf.advance(consumed);
    Some(RespValue::Array(items))
}

/// Find the position of `\r\n` in the buffer, returning the index of `\r`.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    (0..buf.len().saturating_sub(1)).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let val = parse_resp(&mut buf).unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_error() {
        let mut buf = BytesMut::from("-ERR unknown command\r\n");
        let val = parse_resp(&mut buf).unwrap();
        assert_eq!(val, RespValue::Error("ERR unknown command".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = BytesMut::from(":1000\r\n");
        let val = parse_resp(&mut buf).unwrap();
        assert_eq!(val, RespValue::Integer(1000));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let val = parse_resp(&mut buf).unwrap();
        assert_eq!(val, RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let val = parse_resp(&mut buf).unwrap();
        assert_eq!(val, RespValue::Null);
    }

    #[test]
    fn test_parse_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let val = parse_resp(&mut buf).unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from("foo"))),
                RespValue::BulkString(Some(Bytes::from("bar"))),
            ])
        );
    }

    #[test]
    fn test_parse_incomplete() {
        let mut buf = BytesMut::from("$5\r\nhel");
        assert!(parse_resp(&mut buf).is_none());
        // Buffer should not be consumed.
        assert_eq!(buf.len(), 7);
    }

    #[test]
    fn test_serialize_roundtrip() {
        let val = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from("SET"))),
            RespValue::BulkString(Some(Bytes::from("key"))),
            RespValue::BulkString(Some(Bytes::from("value"))),
        ]);
        let serialized = val.serialize();
        let mut buf = BytesMut::from(serialized.as_ref());
        let parsed = parse_resp(&mut buf).unwrap();
        assert_eq!(parsed, val);
    }

    #[test]
    fn test_inline_command() {
        let mut buf = BytesMut::from("PING\r\n");
        let val = parse_resp(&mut buf).unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![RespValue::BulkString(Some(Bytes::from("PING")))])
        );
    }
}
