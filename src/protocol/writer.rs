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
}
