//! Fatal RESP2 framing errors.
//!
//! These are *protocol* errors, not command errors: the connection cannot resync
//! on the byte stream, so the server replies with an error and closes it
//! (`docs/ARCHITECTURE.md` §3, error taxonomy).
//!
//! `Display` yields the text that follows the leading `-ERR ` in a RESP error
//! reply, so a server writes `format!("-ERR {error}\r\n")`. The two limit
//! messages intentionally match the architecture wording exactly:
//! `-ERR value is too long` and `-ERR protocol error: array too large`.

use std::fmt;

/// A fatal, deterministic RESP2 framing/limit failure.
///
/// Variants carry the offending value and the active limit where that is useful
/// for diagnostics and tests. All variants are safe to expose in an error reply
/// — none of them echo arbitrary client bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// A top-level frame did not begin with `*`. RESP2 inline commands are not
    /// supported (architecture §3), and requests must be arrays.
    ExpectedArray { found: u8 },
    /// A top-level frame was the null array `*-1\r\n`, which is not a command.
    NullCommandArray,
    /// An unrecognized RESP type marker inside an array.
    InvalidTypeMarker { marker: u8 },
    /// A numeric header (length or count) contained non-digit bytes.
    InvalidInteger { field: &'static str },
    /// A numeric header did not fit in `i64` (too many digits or out of range).
    IntegerOverflow { field: &'static str },
    /// A length/count was below `-1`, which RESP2 does not define.
    InvalidLength { field: &'static str, value: i64 },
    /// A bulk string exceeded `max_bulk_len`.
    BulkTooLong { len: u64, limit: u64 },
    /// An array exceeded `max_array_len`.
    ArrayTooLarge { len: u64, limit: u64 },
    /// A request (or the retained partial-request buffer) exceeded
    /// `max_request_size`.
    RequestTooLarge { size: u64, limit: u64 },
    /// Arrays were nested deeper than `max_nesting_depth`.
    NestingTooDeep { depth: usize, limit: usize },
    /// A line-terminated value contained a bare `\r` or `\n`.
    InvalidLineTerminator,
    /// A bulk-string payload was not followed by `\r\n`.
    MissingBulkTerminator,
    /// A text value (simple string / error) was not valid UTF-8.
    InvalidUtf8 { field: &'static str },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedArray { found } => {
                write!(f, "protocol error: expected array, got byte {found:#04x}")
            }
            Self::NullCommandArray => {
                write!(f, "protocol error: null array is not a valid command")
            }
            Self::InvalidTypeMarker { marker } => {
                write!(f, "protocol error: invalid type byte {marker:#04x}")
            }
            Self::InvalidInteger { field } => {
                write!(f, "protocol error: invalid integer in {field}")
            }
            Self::IntegerOverflow { field } => {
                write!(f, "protocol error: integer overflow in {field}")
            }
            Self::InvalidLength { field, value } => {
                write!(f, "protocol error: invalid length {value} in {field}")
            }
            Self::BulkTooLong { .. } => write!(f, "value is too long"),
            Self::ArrayTooLarge { .. } => write!(f, "protocol error: array too large"),
            Self::RequestTooLarge { .. } => write!(f, "protocol error: request too large"),
            Self::NestingTooDeep { .. } => write!(f, "protocol error: nesting too deep"),
            Self::InvalidLineTerminator => write!(f, "protocol error: expected CRLF"),
            Self::MissingBulkTerminator => {
                write!(f, "protocol error: bulk string not terminated by CRLF")
            }
            Self::InvalidUtf8 { field } => {
                write!(f, "protocol error: invalid UTF-8 in {field}")
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

