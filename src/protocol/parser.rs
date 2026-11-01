//! Incremental RESP2 parser producing batched command events.
//!
//! # Design
//!
//! The parser owns a byte buffer holding the *unconsumed remainder* of the
//! stream. `feed` appends new bytes, then repeatedly parses one top-level
//! request from the front of the buffer:
//!
//! * a complete request is appended to the output batch and the cursor moves on;
//! * a syntactically valid but truncated request stops the scan — the bytes are
//!   retained for the next `feed` and `Incomplete` is returned;
//! * malformed input is fatal: the bytes already parsed in this call are
//!   returned as a batch (so no valid command is silently dropped) and the
//!   error is retained for `take_error`.
//!
//! Only after the scan does the buffer get compacted (one `drain` per `feed`),
//! which keeps pipelined workloads linear instead of quadratic in the buffer.
//!
//! # State transitions
//!
//! ```text
//!            complete frame(s), no error
//!   Ready ───────────────────────────────► Ready      (event: Commands)
//!     │  truncated frame
//!     ├───────────────────────────────────► Ready      (event: Incomplete)
//!     │  malformed frame
//!     └───────────────────────────────────► Failed     (event: ProtocolError,
//!                                                       or Commands if a
//!                                                       non-empty batch was
//!                                                       parsed first)
//!   Failed ─────── feed ─────────────────► Failed     (event: ProtocolError)
//!   Failed ─────── take_error ───────────► Failed     (returns the error once)
//! ```
//!
//! `Failed` is terminal: there is no way to resynchronize a byte stream after
//! framing has been lost, so `feed` keeps reporting the same error. A caller
//! must discard the parser (and its connection) after observing it.
//!
//! # Element types
//!
//! Inside a command array the parser accepts every RESP2 type (including nested
//! and null arrays) because element validation is command semantics, not
//! framing. Command interpretation (M2) is responsible for rejecting anything
//! it cannot use.
//!
//! # Cost
//!
//! Each byte is scanned once for a complete request. A *truncated* request is
//! re-examined from its start on every `feed`, so a slowly delivered large
//! nested array is re-parsed per chunk. The work per feed is bounded by
//! `max_array_len` and `max_request_size`; a resumable cursor could remove the
//! repeat work later without changing this API.

use super::error::ProtocolError;
use super::limits::ProtocolLimits;
use super::value::RespValue;

/// The result of feeding bytes to [`RespParser`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserEvent {
    /// One or more complete commands, in stream order.
    ///
    /// Produced only when at least one complete command was parsed. If the same
    /// batch was followed by malformed bytes, the error is retained and can be
    /// recovered with [`RespParser::take_error`].
    Commands(Vec<Vec<RespValue>>),
    /// The buffer ends mid-request; feed more bytes.
    Incomplete,
    /// Fatal framing error. The parser is unusable afterwards.
    ProtocolError(ProtocolError),
}

/// Incremental, binary-safe RESP2 request parser.
///
/// The parser is transport-agnostic: it never touches I/O, timers, storage or
/// command semantics. Requests are top-level arrays whose elements may be any
/// RESP2 value; element-type validation belongs to the command layer.
#[derive(Debug)]
pub struct RespParser {
    /// Unconsumed bytes (a partial request, normally).
    buffer: Vec<u8>,
    limits: ProtocolLimits,
    /// Set once framing has been lost; also the error replayed by `feed`.
    failure: Option<ProtocolError>,
    /// Whether `take_error` has already handed the failure out.
    failure_taken: bool,
}

impl RespParser {
    /// Creates a parser with [`ProtocolLimits::default`].
    pub fn new() -> Self {
        Self::with_limits(ProtocolLimits::default())
    }

    /// Creates a parser with explicit limits.
    pub fn with_limits(limits: ProtocolLimits) -> Self {
        Self {
            buffer: Vec::new(),
            limits,
            failure: None,
            failure_taken: false,
        }
    }

    /// The limits this parser enforces.
    pub fn limits(&self) -> ProtocolLimits {
        self.limits
    }

    /// Feeds a chunk of bytes and returns the parser event for them.
    ///
    /// The chunk may split a request at any byte offset, contain any number of
    /// complete requests, or do both. Bytes belonging to a truncated trailing
    /// request are retained internally and re-examined on the next call.
    ///
    /// Once a fatal error has been observed, every later call returns
    /// `ProtocolError` with the same error instead of pretending to resume.
    pub fn feed(&mut self, data: &[u8]) -> ParserEvent {
        // Terminal state: never attempt to parse after framing was lost.
        if let Some(err) = &self.failure {
            return ParserEvent::ProtocolError(err.clone());
        }

        self.buffer.extend_from_slice(data);
        if self.buffer.is_empty() {
            return ParserEvent::Incomplete;
        }

        let mut cursor = 0usize;
        let mut batch: Vec<Vec<RespValue>> = Vec::new();
        let mut failure: Option<ProtocolError> = None;

        {
            let buf = &self.buffer[..];
            while cursor < buf.len() {
                let frame_start = cursor;

                // Requests must be arrays; RESP2 inline commands are excluded.
                if buf[frame_start] != b'*' {
                    failure = Some(ProtocolError::ExpectedArray {
                        found: buf[frame_start],
                    });
                    break;
                }

                match parse_value(buf, cursor, frame_start, 0, &self.limits) {
                    Ok(Some((value, next))) => {
                        let frame_len = (next - frame_start) as u64;
                        if frame_len > self.limits.max_request_size {
                            failure = Some(ProtocolError::RequestTooLarge {
                                size: frame_len,
                                limit: self.limits.max_request_size,
                            });
                            break;
                        }

                        // The leading `*` guarantees an array here; a null array
                        // cannot be a command.
                        match value {
                            RespValue::Array(Some(elements)) => {
                                batch.push(elements);
                                cursor = next;
                            }
                            RespValue::Array(None) => {
                                failure = Some(ProtocolError::NullCommandArray);
                                break;
                            }
                            // Unreachable given the marker check above, but keep
                            // the parser total instead of panicking.
                            other => {
                                failure = Some(ProtocolError::ExpectedArray {
                                    found: other.type_marker(),
                                });
                                break;
                            }
                        }
                    }
                    // Structurally valid prefix: retain it and wait for more.
                    Ok(None) => break,
                    Err(err) => {
                        failure = Some(err);
                        break;
                    }
                }
            }
        }

        self.buffer.drain(..cursor);

        if let Some(err) = failure {
            // Framing is lost: drop any retained bytes, including the malformed
            // frame, and latch the error.
            self.buffer.clear();
            self.failure = Some(err.clone());
            if batch.is_empty() {
                return ParserEvent::ProtocolError(err);
            }
            // Valid commands preceded the malformed bytes: emit them first; the
            // caller drains the latched error with `take_error`.
            return ParserEvent::Commands(batch);
        }

        if !batch.is_empty() {
            return ParserEvent::Commands(batch);
        }

        // Nothing complete and nothing malformed: enforce the buffer-growth
        // ceiling so a client cannot make us hold an unbounded partial request.
        let buffered = self.buffer.len() as u64;
        if buffered > self.limits.max_request_size {
            let err = ProtocolError::RequestTooLarge {
                size: buffered,
                limit: self.limits.max_request_size,
            };
            self.buffer.clear();
            self.failure = Some(err.clone());
            return ParserEvent::ProtocolError(err);
        }

        ParserEvent::Incomplete
    }

    /// Drains the fatal error recorded after a `Commands` batch was emitted.
    ///
    /// Returns `Some` exactly once for a given failure. The parser itself stays
    /// in the failed state; this only clears the "not yet reported" flag.
    pub fn take_error(&mut self) -> Option<ProtocolError> {
        match &self.failure {
            Some(err) if !self.failure_taken => {
                self.failure_taken = true;
                Some(err.clone())
            }
            _ => None,
        }
    }

    /// Returns `true` once a fatal error has been latched.
    pub fn is_failed(&self) -> bool {
        self.failure.is_some()
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses one RESP2 value starting at `pos`.
///
/// `frame_start` is the offset of the enclosing top-level request; it is used to
/// account for `max_request_size` *before* a payload is allocated.
///
/// Returns `Ok(None)` when the bytes are a valid but incomplete prefix, and
/// `Err` only for genuinely malformed input or a limit violation.
fn parse_value(
    buf: &[u8],
    pos: usize,
    frame_start: usize,
    depth: usize,
    limits: &ProtocolLimits,
) -> Result<Option<(RespValue, usize)>, ProtocolError> {
    let Some(&marker) = buf.get(pos) else {
        return Ok(None);
    };

    match marker {
        b'+' => match parse_line(buf, pos + 1)? {
            Some((line, next)) => {
                let text = std::str::from_utf8(line)
                    .map_err(|_| ProtocolError::InvalidUtf8 {
                        field: "simple string",
                    })?
                    .to_owned();
                Ok(Some((RespValue::SimpleString(text), next)))
            }
            None => Ok(None),
        },
        b'-' => match parse_line(buf, pos + 1)? {
            Some((line, next)) => {
                let text = std::str::from_utf8(line)
                    .map_err(|_| ProtocolError::InvalidUtf8 { field: "error" })?
                    .to_owned();
                Ok(Some((RespValue::Error(text), next)))
            }
            None => Ok(None),
        },
        b':' => match parse_integer(buf, pos + 1, "integer")? {
            Some((value, next)) => Ok(Some((RespValue::Integer(value), next))),
            None => Ok(None),
        },
        b'$' => parse_bulk(buf, pos, frame_start, limits),
        b'*' => parse_array(buf, pos, frame_start, depth, limits),
        _ => Err(ProtocolError::InvalidTypeMarker { marker }),
    }
}

/// Parses a bulk string at `pos` (which points at `$`).
fn parse_bulk(
    buf: &[u8],
    pos: usize,
    frame_start: usize,
    limits: &ProtocolLimits,
) -> Result<Option<(RespValue, usize)>, ProtocolError> {
    let Some((len, payload_start)) = parse_integer(buf, pos + 1, "bulk length")? else {
        return Ok(None);
    };

    if len == -1 {
        return Ok(Some((RespValue::BulkString(None), payload_start)));
    }
    if len < -1 {
        return Err(ProtocolError::InvalidLength {
            field: "bulk length",
            value: len,
        });
    }

    let len = len as u64;
    // Limit check happens before any allocation or slice arithmetic.
    if len > limits.max_bulk_len {
        return Err(ProtocolError::BulkTooLong {
            len,
            limit: limits.max_bulk_len,
        });
    }

    let header_len = (payload_start - frame_start) as u64;
    let total = header_len
        .saturating_add(len)
        .saturating_add(2 /* trailing CRLF */);
    if total > limits.max_request_size {
        return Err(ProtocolError::RequestTooLarge {
            size: total,
            limit: limits.max_request_size,
        });
    }

    let len = usize::try_from(len).map_err(|_| ProtocolError::RequestTooLarge {
        size: u64::MAX,
        limit: limits.max_request_size,
    })?;
    let payload_end = payload_start
        .checked_add(len)
        .ok_or(ProtocolError::RequestTooLarge {
            size: u64::MAX,
            limit: limits.max_request_size,
        })?;
    let frame_end = payload_end
        .checked_add(2)
        .ok_or(ProtocolError::RequestTooLarge {
            size: u64::MAX,
            limit: limits.max_request_size,
        })?;

    // Not enough bytes yet: an incomplete payload is not an error.
    if frame_end > buf.len() {
        return Ok(None);
    }
    if buf[payload_end] != b'\r' || buf[payload_end + 1] != b'\n' {
        return Err(ProtocolError::MissingBulkTerminator);
    }

    let payload = buf[payload_start..payload_end].to_vec();
    Ok(Some((RespValue::BulkString(Some(payload)), frame_end)))
}

/// Parses an array at `pos` (which points at `*`).
fn parse_array(
    buf: &[u8],
    pos: usize,
    frame_start: usize,
    depth: usize,
    limits: &ProtocolLimits,
) -> Result<Option<(RespValue, usize)>, ProtocolError> {
    let Some((count, mut cursor)) = parse_integer(buf, pos + 1, "array length")? else {
        return Ok(None);
    };

    if count == -1 {
        return Ok(Some((RespValue::Array(None), cursor)));
    }
    if count < -1 {
        return Err(ProtocolError::InvalidLength {
            field: "array length",
            value: count,
        });
    }

    let count = count as u64;
    if count > limits.max_array_len {
        return Err(ProtocolError::ArrayTooLarge {
            len: count,
            limit: limits.max_array_len,
        });
    }
    if depth >= limits.max_nesting_depth {
        return Err(ProtocolError::NestingTooDeep {
            depth: depth + 1,
            limit: limits.max_nesting_depth,
        });
    }

    // A conservative pre-allocation: never trust the declared count for a large
    // upfront reservation, but reserve the common small-array case cheaply.
    let count = count as usize;
    let mut items = Vec::with_capacity(count.min(32));
    for _ in 0..count {
        match parse_value(buf, cursor, frame_start, depth + 1, limits)? {
            Some((item, next)) => {
                items.push(item);
                cursor = next;
            }
            // A partially received element invalidates the whole array for this
            // call; the caller retains the bytes and retries from the start.
            None => return Ok(None),
        }
    }

    Ok(Some((RespValue::Array(Some(items)), cursor)))
}

/// Parses a decimal `i64` header terminated by CRLF.
///
/// Returns `Ok(None)` when the terminator has not arrived yet. Digits are
/// accumulated in `i128` with a hard 20-digit cap, so no input length can
/// overflow the accumulator and no scan is unbounded. A cap of 20 digits covers
/// every `i64` (including `i64::MIN`, whose magnitude has 19 digits).
fn parse_integer(
    buf: &[u8],
    start: usize,
    field: &'static str,
) -> Result<Option<(i64, usize)>, ProtocolError> {
    let mut cursor = start;
    let mut negative = false;

    if cursor < buf.len() && buf[cursor] == b'-' {
        negative = true;
        cursor += 1;
    }

    let digits_start = cursor;
    let mut magnitude: i128 = 0;

    while cursor < buf.len() {
        match buf[cursor] {
            digit @ b'0'..=b'9' => {
                if cursor - digits_start >= 20 {
                    return Err(ProtocolError::IntegerOverflow { field });
                }
                magnitude = magnitude * 10 + i128::from(digit - b'0');
                cursor += 1;
            }
            b'\r' => {
                if cursor + 1 >= buf.len() {
                    return Ok(None);
                }
                if buf[cursor + 1] != b'\n' {
                    return Err(ProtocolError::InvalidLineTerminator);
                }
                if cursor == digits_start {
                    return Err(ProtocolError::InvalidInteger { field });
                }
                let signed = if negative { -magnitude } else { magnitude };
                let value =
                    i64::try_from(signed).map_err(|_| ProtocolError::IntegerOverflow { field })?;
                return Ok(Some((value, cursor + 2)));
            }
            _ => return Err(ProtocolError::InvalidInteger { field }),
        }
    }

    Ok(None)
}

/// Parses a CRLF-terminated line, returning its bytes and the offset just past
/// the `\r\n`.
///
/// A bare `\r` not followed by `\n`, or a bare `\n`, is malformed rather than
/// merely incomplete — the input can never become valid.
fn parse_line(buf: &[u8], start: usize) -> Result<Option<(&[u8], usize)>, ProtocolError> {
    let mut cursor = start;
    while cursor < buf.len() {
        match buf[cursor] {
            b'\r' => {
                if cursor + 1 >= buf.len() {
                    return Ok(None);
                }
                if buf[cursor + 1] != b'\n' {
                    return Err(ProtocolError::InvalidLineTerminator);
                }
                return Ok(Some((&buf[start..cursor], cursor + 2)));
            }
            b'\n' => return Err(ProtocolError::InvalidLineTerminator),
            _ => cursor += 1,
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::{ParserEvent, RespParser};
    use crate::protocol::error::ProtocolError;
    use crate::protocol::value::RespValue;

    fn cmd(args: &[&[u8]]) -> Vec<RespValue> {
        args.iter()
            .map(|arg| RespValue::BulkString(Some(arg.to_vec())))
            .collect()
    }

    #[test]
    fn parses_one_command() {
        let mut parser = RespParser::new();
        let event = parser.feed(b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");
        assert_eq!(event, ParserEvent::Commands(vec![cmd(&[b"ECHO", b"hi"])]));
        assert_eq!(parser.take_error(), None);
    }

    #[test]
    fn retains_partial_frames_across_feeds() {
        let mut parser = RespParser::new();
        assert_eq!(parser.feed(b"*1\r\n$4\r\nPI"), ParserEvent::Incomplete);
        assert_eq!(parser.take_error(), None);
        assert_eq!(
            parser.feed(b"NG\r\n"),
            ParserEvent::Commands(vec![cmd(&[b"PING"])])
        );
    }

    #[test]
    fn batches_pipelined_commands() {
        let mut parser = RespParser::new();
        let event = parser.feed(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
        assert_eq!(
            event,
            ParserEvent::Commands(vec![cmd(&[b"PING"]), cmd(&[b"PING"])])
        );
    }

    #[test]
    fn emits_valid_batch_then_latches_error() {
        let mut parser = RespParser::new();
        let event = parser.feed(b"*1\r\n$4\r\nPING\r\n*x\r\n");
        assert_eq!(event, ParserEvent::Commands(vec![cmd(&[b"PING"])]));
        assert_eq!(
            parser.take_error(),
            Some(ProtocolError::InvalidInteger {
                field: "array length"
            })
        );
        assert_eq!(parser.take_error(), None);
        assert!(matches!(
            parser.feed(b"*1\r\n$4\r\nPING\r\n"),
            ParserEvent::ProtocolError(_)
        ));
    }

    #[test]
    fn null_top_level_array_is_rejected() {
        let mut parser = RespParser::new();
        assert_eq!(
            parser.feed(b"*-1\r\n"),
            ParserEvent::ProtocolError(ProtocolError::NullCommandArray)
        );
    }

    #[test]
    fn non_array_top_level_is_rejected() {
        let mut parser = RespParser::new();
        assert_eq!(
            parser.feed(b"+OK\r\n"),
            ParserEvent::ProtocolError(ProtocolError::ExpectedArray { found: b'+' })
        );
    }

    #[test]
    fn applies_limits_before_allocating() {
        use crate::protocol::limits::ProtocolLimits;

        let limits = ProtocolLimits {
            max_bulk_len: 4,
            max_array_len: 2,
            max_request_size: 64,
            max_nesting_depth: 8,
        };
        let mut parser = RespParser::with_limits(limits);
        assert_eq!(
            parser.feed(b"*1\r\n$5\r\nhello\r\n"),
            ParserEvent::ProtocolError(ProtocolError::BulkTooLong { len: 5, limit: 4 })
        );

        let mut parser = RespParser::with_limits(limits);
        assert_eq!(
            parser.feed(b"*3\r\n:1\r\n:2\r\n:3\r\n"),
            ParserEvent::ProtocolError(ProtocolError::ArrayTooLarge { len: 3, limit: 2 })
        );
    }

    #[test]
    fn empty_feed_is_incomplete() {
        let mut parser = RespParser::new();
        assert_eq!(parser.feed(b""), ParserEvent::Incomplete);
        assert!(!parser.is_failed());
    }
}
