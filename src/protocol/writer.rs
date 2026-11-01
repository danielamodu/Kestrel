//! RESP2 serializer for server replies.
//!
//! Encoding is total: every [`RespValue`] has exactly one wire form, and no
//! input can corrupt framing.
//!
//! * Bulk strings are written verbatim, so arbitrary bytes (including `\r`,
//!   `\n` and NUL) round-trip exactly.
//! * `BulkString(None)` → `$-1\r\n`, `Array(None)` → `*-1\r\n`.
//! * `SimpleString` / `Error` text is line-based RESP, so embedded `\r` and
//!   `\n` are replaced with spaces. Such text cannot otherwise be represented
//!   without breaking framing (a response-splitting hazard); replacing keeps
//!   the output well formed and deterministic.

use super::value::RespValue;

/// Appends the RESP2 encoding of `value` to `out`.
///
/// Recursion follows the value's nesting; values produced by the parser are
/// depth-bounded by `ProtocolLimits::max_nesting_depth`.
pub fn encode_into(value: &RespValue, out: &mut Vec<u8>) {
    match value {
        RespValue::SimpleString(text) => encode_line(b'+', text, out),
        RespValue::Error(text) => encode_line(b'-', text, out),
        RespValue::Integer(number) => {
            out.push(b':');
            out.extend_from_slice(number.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(None) => out.extend_from_slice(b"$-1\r\n"),
        RespValue::BulkString(Some(payload)) => {
            out.push(b'$');
            out.extend_from_slice(payload.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Array(None) => out.extend_from_slice(b"*-1\r\n"),
        RespValue::Array(Some(items)) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_into(item, out);
            }
        }
    }
}

/// Returns the RESP2 encoding of `value`.
pub fn encode(value: &RespValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(value, &mut out);
    out
}

/// Writes `<prefix><text>\r\n`, replacing CR/LF so framing stays intact.
fn encode_line(prefix: u8, text: &str, out: &mut Vec<u8>) {
    out.push(prefix);
    for byte in text.bytes() {
        out.push(if byte == b'\r' || byte == b'\n' {
            b' '
        } else {
            byte
        });
    }
    out.extend_from_slice(b"\r\n");
}

