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

#[cfg(test)]
mod tests {
    use super::{encode, encode_into};
    use crate::protocol::value::RespValue;

    #[test]
    fn encodes_all_types_exactly() {
        let cases: Vec<(RespValue, &[u8])> = vec![
            (RespValue::SimpleString("OK".into()), b"+OK\r\n"),
            (RespValue::Error("ERR boom".into()), b"-ERR boom\r\n"),
            (RespValue::Integer(0), b":0\r\n"),
            (RespValue::Integer(-42), b":-42\r\n"),
            (RespValue::BulkString(None), b"$-1\r\n"),
            (RespValue::BulkString(Some(Vec::new())), b"$0\r\n\r\n"),
            (
                RespValue::BulkString(Some(b"hey".to_vec())),
                b"$3\r\nhey\r\n",
            ),
            (RespValue::Array(None), b"*-1\r\n"),
            (RespValue::Array(Some(Vec::new())), b"*0\r\n"),
            (
                RespValue::Array(Some(vec![
                    RespValue::Integer(1),
                    RespValue::BulkString(None),
                ])),
                b"*2\r\n:1\r\n$-1\r\n",
            ),
            (
                RespValue::Array(Some(vec![RespValue::Array(Some(vec![
                    RespValue::SimpleString("x".into()),
                ]))])),
                b"*1\r\n*1\r\n+x\r\n",
            ),
        ];

        for (value, expected) in cases {
            assert_eq!(encode(&value), expected, "value: {value:?}");
        }
    }

    #[test]
    fn bulk_payloads_are_binary_safe() {
        let payload = b"a\r\n\0b".to_vec();
        let mut expected = b"$5\r\n".to_vec();
        expected.extend_from_slice(&payload);
        expected.extend_from_slice(b"\r\n");

        assert_eq!(
            encode(&RespValue::BulkString(Some(payload))),
            expected.as_slice()
        );
    }

    #[test]
    fn encode_into_appends() {
        let mut out = b"prefix".to_vec();
        encode_into(&RespValue::Integer(7), &mut out);
        assert_eq!(out, b"prefix:7\r\n");
    }

    #[test]
    fn line_values_cannot_break_framing() {
        assert_eq!(
            encode(&RespValue::SimpleString("a\r\nb".into())),
            b"+a  b\r\n"
        );
        assert_eq!(
            encode(&RespValue::Error("ERR x\ny".into())),
            b"-ERR x y\r\n"
        );
    }

    #[test]
    fn array_element_order_is_preserved() {
        let value = RespValue::Array(Some(vec![
            RespValue::Integer(1),
            RespValue::Array(Some(vec![RespValue::Integer(2), RespValue::Integer(3)])),
            RespValue::Integer(4),
        ]));
        assert_eq!(encode(&value), b"*3\r\n:1\r\n*2\r\n:2\r\n:3\r\n:4\r\n");
    }

    /// A recursive encoder would need one call frame per nesting level, so the
    /// encode call runs on a deliberately small (64 KiB) stack. With
    /// `DEPTH = 10_000` a recursive encoder overflows immediately; the
    /// iterative one succeeds.
    ///
    /// The value itself is built and dropped on the default test-thread stack
    /// because `RespValue` uses Rust's derived, recursive `Drop`. That is a
    /// property of the data structure, not of the encoder: dropping an
    /// arbitrarily deep manual tree still costs stack, which is why
    /// parser-produced values are capped by `max_nesting_depth`.
    #[test]
    fn deeply_nested_value_encodes_on_a_small_stack() {
        const DEPTH: usize = 10_000;
        const LARGE_STACK: usize = 64 * 1024 * 1024;
        const SMALL_STACK: usize = 64 * 1024;

        // The tree is built and dropped on a thread with a large stack, since
        // `RespValue`'s derived `Drop` is recursive. Only `encode` runs on the
        // small stack, which is what this test is about.
        let encoded = std::thread::scope(|scope| {
            std::thread::Builder::new()
                .stack_size(LARGE_STACK)
                .spawn_scoped(scope, || {
                    let mut value = RespValue::Integer(7);
                    for _ in 0..DEPTH {
                        value = RespValue::Array(Some(vec![value]));
                    }

                    let encoded = std::thread::scope(|inner| {
                        std::thread::Builder::new()
                            .stack_size(SMALL_STACK)
                            .spawn_scoped(inner, || encode(&value))
                            .expect("spawn encoder thread")
                            .join()
                            .expect("encoder thread panicked")
                    });
                    // `value` drops here, on the large stack.
                    encoded
                })
                .expect("spawn builder thread")
                .join()
                .expect("builder thread panicked")
        });

        let mut expected = b"*1\r\n".repeat(DEPTH);
        expected.extend_from_slice(b":7\r\n");
        assert_eq!(encoded, expected);
    }
}
