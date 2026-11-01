//! Incremental RESP2 parser producing batched command events.
//!
//! # Design
//!
//! Parsing state is split between parsed values and raw bytes:
//!
//! * `stack` holds arrays that have been started; every element that completed
//!   is retained here as a [`RespValue`] and is never parsed again.
//! * `buffer` holds only the bytes of the *current incomplete element* (for
//!   example `$12\r\nab`, whose payload has not fully arrived).
//!
//! `feed` resumes exactly where the previous call stopped, so a request that
//! arrives one byte at a time costs `O(total bytes)`, not `O(bytes²)`. Each
//! bulk payload is copied once, when it is complete. Line-based elements
//! (`+`, `-`) additionally remember how far they have been scanned, so a long
//! line is not rescanned from its start on every feed.
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
//! # Size semantics
//!
//! Three quantities are deliberately kept separate:
//!
//! * **Per-command size** — `max_request_size` bounds the encoded size of *one*
//!   top-level request. `request_bytes` accumulates every byte of the request
//!   currently being parsed, including elements already moved into `stack`.
//! * **Aggregate input size** — one `feed` call may carry any number of
//!   pipelined commands whose combined length exceeds `max_request_size`. Each
//!   command is accounted independently; the aggregate is never rejected.
//! * **Retained-buffer size** — `buffer` only holds the current incomplete
//!   element, and `request_bytes + buffer.len()` is checked against
//!   `max_request_size`, so an unbounded partial request cannot accumulate.
//!
//! # Element types
//!
//! Inside a command array the parser accepts every RESP2 type (including nested
//! and null arrays) because element validation is command semantics, not
//! framing. Command interpretation (M2) is responsible for rejecting anything
//! it cannot use.

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

/// A partially parsed array: elements retained so far plus the number of
/// elements still expected.
#[derive(Debug)]
struct ArrayFrame {
    items: Vec<RespValue>,
    remaining: usize,
}

impl ArrayFrame {
    fn new(count: usize) -> Self {
        // Never trust a declared count for a large upfront reservation; reserve
        // only a small common case and let `push` grow as elements arrive.
        Self {
            items: Vec::with_capacity(count.min(32)),
            remaining: count,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ParseStats {
    /// Total bulk payload bytes copied into values. Each payload must be copied
    /// exactly once regardless of feed fragmentation.
    payload_bytes_copied: u64,
    /// Total bytes examined by line scanning. Must stay linear in input size.
    line_bytes_scanned: u64,
}

/// Incremental, binary-safe RESP2 request parser.
///
/// The parser is transport-agnostic: it never touches I/O, timers, storage or
/// command semantics. Requests are top-level arrays whose elements may be any
/// RESP2 value.
#[derive(Debug)]
pub struct RespParser {
    /// Bytes of the current incomplete element (plus nothing else).
    buffer: Vec<u8>,
    limits: ProtocolLimits,
    /// Arrays started but not yet closed.
    stack: Vec<ArrayFrame>,
    /// Encoded bytes consumed for the current top-level request.
    request_bytes: u64,
    /// Absolute offset in `buffer` already searched for a line terminator.
    line_scanned: usize,
    /// Set once framing has been lost; also the error replayed by `feed`.
    failure: Option<ProtocolError>,
    /// Whether `take_error` has already handed the failure out.
    failure_taken: bool,
    #[cfg(test)]
    stats: ParseStats,
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
            stack: Vec::new(),
            request_bytes: 0,
            line_scanned: 0,
            failure: None,
            failure_taken: false,
            #[cfg(test)]
            stats: ParseStats::default(),
        }
    }

    /// The limits this parser enforces.
    pub fn limits(&self) -> ProtocolLimits {
        self.limits
    }

    /// Returns `true` once a fatal error has been latched.
    pub fn is_failed(&self) -> bool {
        self.failure.is_some()
    }

    /// Feeds a chunk of bytes and returns the parser event for them.
    ///
    /// The chunk may split a request at any byte offset, contain any number of
    /// complete requests, or do both. Bytes belonging to a truncated trailing
    /// element are retained internally and re-examined on the next call.
    ///
    /// Once a fatal error has been observed, every later call returns
    /// `ProtocolError` with the same error instead of pretending to resume.
    pub fn feed(&mut self, data: &[u8]) -> ParserEvent {
        if let Some(err) = &self.failure {
            return ParserEvent::ProtocolError(err.clone());
        }

        self.buffer.extend_from_slice(data);

        let mut cursor = 0usize;
        let mut batch: Vec<Vec<RespValue>> = Vec::new();
        let mut failure: Option<ProtocolError> = None;

        loop {
            let buf = &self.buffer[..];

            if self.stack.is_empty() {
                // Between commands: a top-level request must start with `*`.
                if cursor >= buf.len() {
                    break;
                }
                if buf[cursor] != b'*' {
                    failure = Some(ProtocolError::ExpectedArray { found: buf[cursor] });
                    break;
                }
                match parse_array_header(buf, cursor, 0, self.limits) {
                    Ok(Some((-1, _))) => {
                        failure = Some(ProtocolError::NullCommandArray);
                        break;
                    }
                    Ok(Some((count, next))) => {
                        match account(0, cursor, next, self.limits) {
                            Ok(total) => self.request_bytes = total,
                            Err(err) => {
                                failure = Some(err);
                                break;
                            }
                        }
                        self.stack.push(ArrayFrame::new(count as usize));
                        self.line_scanned = 0;
                        cursor = next;
                    }
                    Ok(None) => break,
                    Err(err) => {
                        failure = Some(err);
                        break;
                    }
                }
                continue;
            }

            // Close arrays whose declared element count has been reached. A
            // close can cascade (empty arrays inside arrays), so re-check.
            if self.stack.last().is_some_and(|frame| frame.remaining == 0) {
                if let Some(frame) = self.stack.pop() {
                    if let Some(parent) = self.stack.last_mut() {
                        parent.items.push(RespValue::Array(Some(frame.items)));
                        parent.remaining -= 1;
                    } else {
                        // One complete top-level request.
                        if self.request_bytes > self.limits.max_request_size {
                            failure = Some(ProtocolError::RequestTooLarge {
                                size: self.request_bytes,
                                limit: self.limits.max_request_size,
                            });
                            break;
                        }
                        batch.push(frame.items);
                        self.request_bytes = 0;
                    }
                }
                self.line_scanned = 0;
                continue;
            }

            if cursor >= buf.len() {
                break;
            }

            let element_start = cursor;
            let marker = buf[element_start];

            if marker == b'*' {
                let depth = self.stack.len();
                match parse_array_header(buf, element_start, depth, self.limits) {
                    Ok(Some((count, next))) => {
                        match account(self.request_bytes, element_start, next, self.limits) {
                            Ok(total) => self.request_bytes = total,
                            Err(err) => {
                                failure = Some(err);
                                break;
                            }
                        }
                        cursor = next;
                        if count == -1 {
                            if let Some(frame) = self.stack.last_mut() {
                                frame.items.push(RespValue::Array(None));
                                frame.remaining -= 1;
                            }
                        } else {
                            self.stack.push(ArrayFrame::new(count as usize));
                        }
                        self.line_scanned = 0;
                    }
                    Ok(None) => break,
                    Err(err) => {
                        failure = Some(err);
                        break;
                    }
                }
                continue;
            }

            let is_line = marker == b'+' || marker == b'-';
            let scan_from = if is_line {
                // Re-examine the previous last byte: it may have been a lone
                // `\r` whose `\n` has only just arrived.
                self.line_scanned.saturating_sub(1).max(element_start + 1)
            } else {
                element_start + 1
            };

            match parse_scalar(
                buf,
                element_start,
                self.request_bytes,
                scan_from,
                self.limits,
            ) {
                Ok(Some((value, next))) => {
                    match account(self.request_bytes, element_start, next, self.limits) {
                        Ok(total) => self.request_bytes = total,
                        Err(err) => {
                            failure = Some(err);
                            break;
                        }
                    }
                    #[cfg(test)]
                    {
                        if let RespValue::BulkString(Some(payload)) = &value {
                            self.stats.payload_bytes_copied += payload.len() as u64;
                        }
                        if is_line {
                            self.stats.line_bytes_scanned += next.saturating_sub(scan_from) as u64;
                        }
                    }
                    cursor = next;
                    if let Some(frame) = self.stack.last_mut() {
                        frame.items.push(value);
                        frame.remaining -= 1;
                    }
                    self.line_scanned = 0;
                }
                Ok(None) => {
                    if is_line {
                        self.line_scanned = buf.len();
                        #[cfg(test)]
                        {
                            self.stats.line_bytes_scanned +=
                                buf.len().saturating_sub(scan_from) as u64;
                        }
                    }
                    break;
                }
                Err(err) => {
                    failure = Some(err);
                    break;
                }
            }
        }

        self.buffer.drain(..cursor);
        self.line_scanned = self.line_scanned.saturating_sub(cursor);

        if let Some(err) = failure {
            self.clear_after_failure();
            self.failure = Some(err.clone());
            if batch.is_empty() {
                return ParserEvent::ProtocolError(err);
            }
            // Valid commands preceded the malformed bytes: emit them first; the
            // caller drains the latched error with `take_error`.
            return ParserEvent::Commands(batch);
        }

        // Retained-buffer bound, checked even when a batch was returned so an
        // oversized partial request cannot linger between feeds.
        let pending = self.request_bytes.saturating_add(self.buffer.len() as u64);
        if pending > self.limits.max_request_size {
            let err = ProtocolError::RequestTooLarge {
                size: pending,
                limit: self.limits.max_request_size,
            };
            self.clear_after_failure();
            self.failure = Some(err.clone());
            if batch.is_empty() {
                return ParserEvent::ProtocolError(err);
            }
            return ParserEvent::Commands(batch);
        }

        if !batch.is_empty() {
            return ParserEvent::Commands(batch);
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

    fn clear_after_failure(&mut self) {
        self.buffer.clear();
        self.stack.clear();
        self.request_bytes = 0;
        self.line_scanned = 0;
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Adds the encoded length of `buf[start..next]` to the current request and
/// rejects the request if it now exceeds `max_request_size`.
fn account(
    request_bytes: u64,
    start: usize,
    next: usize,
    limits: ProtocolLimits,
) -> Result<u64, ProtocolError> {
    let total = request_bytes.saturating_add((next - start) as u64);
    if total > limits.max_request_size {
        return Err(ProtocolError::RequestTooLarge {
            size: total,
            limit: limits.max_request_size,
        });
    }
    Ok(total)
}

/// Parses an array header at `pos` (which points at `*`).
///
/// Returns the declared element count (or `-1` for a null array) and the offset
/// just past the header. `depth` is the number of enclosing arrays; the new
/// array's level is `depth + 1`.
fn parse_array_header(
    buf: &[u8],
    pos: usize,
    depth: usize,
    limits: ProtocolLimits,
) -> Result<Option<(i64, usize)>, ProtocolError> {
    let Some((count, next)) = parse_integer(buf, pos + 1, "array length")? else {
        return Ok(None);
    };

    if count == -1 {
        // Null arrays are values, not containers, so they do not add depth.
        return Ok(Some((-1, next)));
    }
    if count < -1 {
        return Err(ProtocolError::InvalidLength {
            field: "array length",
            value: count,
        });
    }

    let count_u64 = count as u64;
    if count_u64 > limits.max_array_len {
        return Err(ProtocolError::ArrayTooLarge {
            len: count_u64,
            limit: limits.max_array_len,
        });
    }
    if depth >= limits.max_nesting_depth {
        return Err(ProtocolError::NestingTooDeep {
            depth: depth + 1,
            limit: limits.max_nesting_depth,
        });
    }

    Ok(Some((count, next)))
}

/// Parses one non-array element at `pos`: simple string, error, integer, or
/// bulk string (including the null bulk string).
fn parse_scalar(
    buf: &[u8],
    pos: usize,
    request_bytes: u64,
    scan_from: usize,
    limits: ProtocolLimits,
) -> Result<Option<(RespValue, usize)>, ProtocolError> {
    let Some(&marker) = buf.get(pos) else {
        return Ok(None);
    };

    match marker {
        b'+' | b'-' => {
            let Some((line, next)) = parse_line(buf, pos + 1, scan_from)? else {
                return Ok(None);
            };
            // Check the size before allocating the owned string.
            let total = request_bytes.saturating_add((next - pos) as u64);
            if total > limits.max_request_size {
                return Err(ProtocolError::RequestTooLarge {
                    size: total,
                    limit: limits.max_request_size,
                });
            }
            let text = std::str::from_utf8(line)
                .map_err(|_| ProtocolError::InvalidUtf8 {
                    field: if marker == b'+' {
                        "simple string"
                    } else {
                        "error"
                    },
                })?
                .to_owned();
            let value = if marker == b'+' {
                RespValue::SimpleString(text)
            } else {
                RespValue::Error(text)
            };
            Ok(Some((value, next)))
        }
        b':' => {
            let Some((number, next)) = parse_integer(buf, pos + 1, "integer")? else {
                return Ok(None);
            };
            let total = request_bytes.saturating_add((next - pos) as u64);
            if total > limits.max_request_size {
                return Err(ProtocolError::RequestTooLarge {
                    size: total,
                    limit: limits.max_request_size,
                });
            }
            Ok(Some((RespValue::Integer(number), next)))
        }
        b'$' => parse_bulk(buf, pos, request_bytes, limits),
        _ => Err(ProtocolError::InvalidTypeMarker { marker }),
    }
}

/// Parses a bulk string at `pos` (which points at `$`).
fn parse_bulk(
    buf: &[u8],
    pos: usize,
    request_bytes: u64,
    limits: ProtocolLimits,
) -> Result<Option<(RespValue, usize)>, ProtocolError> {
    let Some((len, payload_start)) = parse_integer(buf, pos + 1, "bulk length")? else {
        return Ok(None);
    };

    if len == -1 {
        let total = request_bytes.saturating_add((payload_start - pos) as u64);
        if total > limits.max_request_size {
            return Err(ProtocolError::RequestTooLarge {
                size: total,
                limit: limits.max_request_size,
            });
        }
        return Ok(Some((RespValue::BulkString(None), payload_start)));
    }
    if len < -1 {
        return Err(ProtocolError::InvalidLength {
            field: "bulk length",
            value: len,
        });
    }

    let len_u64 = len as u64;
    // Limit check happens before any allocation or slice arithmetic.
    if len_u64 > limits.max_bulk_len {
        return Err(ProtocolError::BulkTooLong {
            len: len_u64,
            limit: limits.max_bulk_len,
        });
    }

    // Account for the whole element before allocating: the declared payload
    // determines the final request size, so an over-limit request is rejected
    // as soon as its header arrives.
    let total = request_bytes
        .saturating_add((payload_start - pos) as u64)
        .saturating_add(len_u64)
        .saturating_add(2 /* trailing CRLF */);
    if total > limits.max_request_size {
        return Err(ProtocolError::RequestTooLarge {
            size: total,
            limit: limits.max_request_size,
        });
    }

    let len = usize::try_from(len_u64).map_err(|_| ProtocolError::RequestTooLarge {
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
/// `scan_from` is where this call must start looking; earlier bytes were
/// examined by a previous call and contain no terminator. A bare `\r` not
/// followed by `\n`, or a bare `\n`, is malformed rather than merely incomplete
/// — the input can never become valid.
fn parse_line(
    buf: &[u8],
    start: usize,
    scan_from: usize,
) -> Result<Option<(&[u8], usize)>, ProtocolError> {
    let mut cursor = scan_from.max(start);
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
    use crate::protocol::limits::ProtocolLimits;
    use crate::protocol::value::RespValue;
    use crate::protocol::writer::encode;

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

    /// Regression for quadratic re-parsing: every bulk payload must be copied
    /// exactly once no matter how finely the frame is fragmented. The old
    /// restart-from-scratch parser re-copied all completed elements on every
    /// feed, which this count would expose immediately.
    #[test]
    fn large_array_fed_byte_by_byte_copies_each_payload_once() {
        let payloads: Vec<Vec<u8>> = (0..128)
            .map(|index| vec![b'a' + (index % 26) as u8; 256])
            .collect();
        let elements: Vec<RespValue> = payloads
            .iter()
            .map(|payload| RespValue::BulkString(Some(payload.clone())))
            .collect();
        let frame = encode(&RespValue::Array(Some(elements.clone())));

        let mut parser = RespParser::new();
        let mut batches = Vec::new();
        for byte in &frame {
            if let ParserEvent::Commands(mut batch) = parser.feed(std::slice::from_ref(byte)) {
                batches.append(&mut batch);
            }
        }

        assert_eq!(batches, vec![elements]);
        let total: u64 = payloads.iter().map(|p| p.len() as u64).sum();
        assert_eq!(
            parser.stats.payload_bytes_copied, total,
            "each payload byte must be copied exactly once"
        );
    }

    /// Regression for quadratic line scanning: a long `+` element fed one byte
    /// at a time must be scanned a linear number of bytes in total.
    #[test]
    fn long_line_fed_byte_by_byte_is_scanned_once() {
        const TEXT_LEN: usize = 64 * 1024;
        let text = "a".repeat(TEXT_LEN);
        let frame = format!("*1\r\n+{text}\r\n");

        let mut parser = RespParser::new();
        let mut batches = Vec::new();
        for byte in frame.as_bytes() {
            if let ParserEvent::Commands(mut batch) = parser.feed(std::slice::from_ref(byte)) {
                batches.append(&mut batch);
            }
        }

        assert_eq!(batches, vec![vec![RespValue::SimpleString(text)]]);
        // A restart-from-scratch scan would examine ~TEXT_LEN²/2 bytes.
        assert!(
            parser.stats.line_bytes_scanned < 3 * TEXT_LEN as u64,
            "line scanning examined {} bytes",
            parser.stats.line_bytes_scanned
        );
    }

    /// Each pipelined command gets its own size budget.
    #[test]
    fn pipelined_commands_are_accounted_per_command() {
        let limits = ProtocolLimits {
            max_bulk_len: 4,
            max_array_len: 2,
            max_request_size: 14,
            max_nesting_depth: 4,
        };
        let frame = b"*1\r\n$4\r\nabcd\r\n"; // exactly 14 bytes
        let mut stream = Vec::new();
        for _ in 0..3 {
            stream.extend_from_slice(frame);
        }

        let mut parser = RespParser::with_limits(limits);
        assert_eq!(
            parser.feed(&stream),
            ParserEvent::Commands(vec![cmd(&[b"abcd"]); 3])
        );
        assert_eq!(parser.take_error(), None);
    }

    /// An oversized *partial* request must be reported even when a valid batch
    /// was emitted in the same call.
    #[test]
    fn oversized_partial_after_valid_batch_is_latched() {
        let limits = ProtocolLimits {
            max_bulk_len: 16,
            max_array_len: 4,
            max_request_size: 16,
            max_nesting_depth: 4,
        };
        let mut data = b"*1\r\n+ok\r\n".to_vec(); // 9 bytes, valid command
        data.extend_from_slice(b"*1\r\n+"); // second request starts
        data.extend(std::iter::repeat_n(b'a', 20)); // partial line: 4 + 21 bytes

        let mut parser = RespParser::with_limits(limits);
        assert_eq!(
            parser.feed(&data),
            ParserEvent::Commands(vec![vec![RespValue::SimpleString("ok".into())]])
        );
        assert_eq!(
            parser.take_error(),
            Some(ProtocolError::RequestTooLarge {
                size: 25,
                limit: 16
            })
        );
        assert_eq!(parser.take_error(), None);
    }
}
