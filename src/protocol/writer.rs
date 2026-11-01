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
//!
//! # Recursion safety
//!
//! Encoding is **iterative**. An explicit [`EncodeTask`] stack on the heap
//! replaces call-stack recursion, so a manually constructed, arbitrarily deep
//! `RespValue` cannot exhaust the call stack during encoding. Work memory is
//! `O(nesting depth)`.
//!
//! Note that dropping such a value still uses Rust's derived recursive `Drop`;
//! values obtained from [`crate::protocol::RespParser`] are depth-bounded by
//! `ProtocolLimits::max_nesting_depth`.

use super::value::RespValue;

/// Work item for the iterative encoder.
enum EncodeTask<'a> {
    /// Write this value (and, for non-null arrays, queue its elements).
    Value(&'a RespValue),
    /// Emit the remaining elements of an array, in order.
    Elements(std::slice::Iter<'a, RespValue>),
}

/// Appends the RESP2 encoding of `value` to `out`.
///
/// Iterative by construction: nesting depth consumes heap, not call-stack,
/// frames. Output is identical to a recursive depth-first encoding.
pub fn encode_into(value: &RespValue, out: &mut Vec<u8>) {
    let mut tasks = vec![EncodeTask::Value(value)];

    while let Some(task) = tasks.pop() {
        match task {
            EncodeTask::Value(RespValue::SimpleString(text)) => encode_line(b'+', text, out),
            EncodeTask::Value(RespValue::Error(text)) => encode_line(b'-', text, out),
            EncodeTask::Value(RespValue::Integer(number)) => {
                out.push(b':');
                out.extend_from_slice(number.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            EncodeTask::Value(RespValue::BulkString(None)) => out.extend_from_slice(b"$-1\r\n"),
            EncodeTask::Value(RespValue::BulkString(Some(payload))) => {
                out.push(b'$');
                out.extend_from_slice(payload.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(payload);
                out.extend_from_slice(b"\r\n");
            }
            EncodeTask::Value(RespValue::Array(None)) => out.extend_from_slice(b"*-1\r\n"),
            EncodeTask::Value(RespValue::Array(Some(items))) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                tasks.push(EncodeTask::Elements(items.iter()));
            }
            EncodeTask::Elements(mut iter) => {
                if let Some(item) = iter.next() {
                    // Array continuation is popped *after* the element, so
                    // elements are emitted left to right.
                    tasks.push(EncodeTask::Elements(iter));
                    tasks.push(EncodeTask::Value(item));
                }
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
