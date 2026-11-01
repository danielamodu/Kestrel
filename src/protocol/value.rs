//! RESP2 value model: simple strings, errors, integers, bulk strings, arrays.
//!
//! The model follows RESP2 with one representational addition: an array wraps
//! `Option<Vec<RespValue>>` so the RESP2 *null array* (`*-1\r\n`) is
//! distinguishable from an empty array (`*0\r\n`). Null bulk strings are
//! already representable as `BulkString(None)`.

/// A single RESP2 value.
///
/// Bulk string payloads are byte-oriented (`Vec<u8>`) so they are binary safe:
/// they may contain `\r`, `\n`, NUL bytes, or anything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// `+<text>\r\n`
    SimpleString(String),
    /// `-<text>\r\n`
    Error(String),
    /// `:<number>\r\n`
    Integer(i64),
    /// `$<len>\r\n<payload>\r\n`, or `$-1\r\n` for `None` (null bulk string).
    BulkString(Option<Vec<u8>>),
    /// `*<count>\r\n<elements...>`, or `*-1\r\n` for `None` (null array).
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Builds a bulk string from any byte container.
    pub fn bulk(data: impl Into<Vec<u8>>) -> Self {
        Self::BulkString(Some(data.into()))
    }

    /// Builds a null bulk string (`$-1\r\n`).
    pub fn null_bulk() -> Self {
        Self::BulkString(None)
    }

    /// Builds an array with the given elements.
    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(Some(items))
    }

    /// Builds a null array (`*-1\r\n`).
    pub fn null_array() -> Self {
        Self::Array(None)
    }

    /// Returns `true` for the two RESP2 null forms (null bulk string, null array).
    pub fn is_null(&self) -> bool {
        matches!(self, Self::BulkString(None) | Self::Array(None))
    }

    /// Returns the payload of a non-null bulk string.
    ///
    /// Command interpretation works exclusively with this accessor; every other
    /// RESP2 element type is rejected or coerced by the command layer (M2), not
    /// here.
    pub fn as_bulk(&self) -> Option<&[u8]> {
        match self {
            Self::BulkString(Some(data)) => Some(data),
            _ => None,
        }
    }

    /// Returns the one-byte RESP type marker used on the wire.
    pub fn type_marker(&self) -> u8 {
        match self {
            Self::SimpleString(_) => b'+',
            Self::Error(_) => b'-',
            Self::Integer(_) => b':',
            Self::BulkString(_) => b'$',
            Self::Array(_) => b'*',
        }
    }
}

