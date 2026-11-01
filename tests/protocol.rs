//! Protocol-layer integration tests.
//!
//! These exercise the public RESP2 surface (`RespParser`, `encode`) through
//! byte slices only — no sockets, no runtime, no commands, no storage.

use kestrel::protocol::writer::{encode, encode_into};
use kestrel::protocol::{ParserEvent, ProtocolError, ProtocolLimits, RespParser, RespValue};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bulk(data: &[u8]) -> RespValue {
    RespValue::BulkString(Some(data.to_vec()))
}

fn command(args: &[&[u8]]) -> Vec<RespValue> {
    args.iter().map(|arg| bulk(arg)).collect()
}

fn command_frame(args: &[&[u8]]) -> Vec<u8> {
    encode(&RespValue::Array(Some(command(args))))
}

/// Feeds `data` in `chunk`-byte pieces, collecting batches and any error.
///
/// Any latched error is drained so callers observe a consistent parser state.
fn feed_chunked(
    parser: &mut RespParser,
    data: &[u8],
    chunk: usize,
) -> (Vec<Vec<RespValue>>, Option<ProtocolError>) {
    let mut batches = Vec::new();
    let mut error = None;

    'outer: for piece in data.chunks(chunk.max(1)) {
        match parser.feed(piece) {
            ParserEvent::Commands(mut batch) => {
                batches.append(&mut batch);
                // The same chunk may have continued with malformed bytes.
                if let Some(err) = parser.take_error() {
                    error = Some(err);
                    break 'outer;
                }
            }
            ParserEvent::Incomplete => {}
            ParserEvent::ProtocolError(err) => {
                let drained = parser.take_error();
                debug_assert_eq!(drained.as_ref(), Some(&err));
                error = Some(err);
                break 'outer;
            }
        }
    }

    (batches, error)
}

/// Feeds a stream one byte at a time.
fn feed_bytewise(
    parser: &mut RespParser,
    data: &[u8],
) -> (Vec<Vec<RespValue>>, Option<ProtocolError>) {
    feed_chunked(parser, data, 1)
}

/// A deterministic xorshift PRNG so property-style tests are reproducible and
/// need no external dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // A zero state would be a fixed point for xorshift.
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }
}

// ---------------------------------------------------------------------------
// Value coverage & round-trips
// ---------------------------------------------------------------------------

#[test]
fn every_resp2_type_round_trips() {
    let items = vec![
        RespValue::SimpleString("OK".into()),
        RespValue::SimpleString("héllo".into()),
        RespValue::Error("ERR boom".into()),
        RespValue::Integer(0),
        RespValue::Integer(i64::MAX),
        RespValue::Integer(i64::MIN),
        RespValue::BulkString(None),
        RespValue::BulkString(Some(Vec::new())),
        bulk(b"\r\n\0"),
        RespValue::Array(None),
        RespValue::Array(Some(Vec::new())),
        RespValue::Array(Some(vec![
            RespValue::Integer(7),
            RespValue::Array(None),
            RespValue::BulkString(None),
        ])),
    ];

    let bytes = encode(&RespValue::Array(Some(items.clone())));
    let mut parser = RespParser::new();
    assert_eq!(parser.feed(&bytes), ParserEvent::Commands(vec![items]));
    assert_eq!(parser.take_error(), None);
}

#[test]
fn empty_bulk_string_is_distinct_from_null_bulk_string() {
    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(b"*2\r\n$0\r\n\r\n$-1\r\n"),
        ParserEvent::Commands(vec![vec![bulk(b""), RespValue::BulkString(None)]])
    );
    assert!(!bulk(b"").is_null());
    assert_eq!(encode(&bulk(b"")), b"$0\r\n\r\n");
    assert_eq!(encode(&RespValue::BulkString(None)), b"$-1\r\n");
}

#[test]
fn null_array_element_is_preserved() {
    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(b"*2\r\n*-1\r\n:5\r\n"),
        ParserEvent::Commands(vec![vec![RespValue::Array(None), RespValue::Integer(5)]])
    );
}

#[test]
fn empty_command_array_is_protocol_valid() {
    // Arity is a command-level concern, not a framing concern.
    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(b"*0\r\n"),
        ParserEvent::Commands(vec![Vec::new()])
    );
}

#[test]
fn binary_payload_with_crlf_and_nul_round_trips() {
    let payload: Vec<u8> = b"a\r\n\0b\r\n\0".to_vec();
    let frame = encode(&RespValue::Array(Some(vec![bulk(b"ECHO"), bulk(&payload)])));

    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(&frame),
        ParserEvent::Commands(vec![vec![bulk(b"ECHO"), bulk(&payload)]])
    );
}

#[test]
fn binary_payload_survives_bytewise_feeding() {
    let payload: Vec<u8> = b"\r\n\0\r\n\0".to_vec();
    let frame = encode(&RespValue::Array(Some(vec![bulk(b"ECHO"), bulk(&payload)])));

    let mut parser = RespParser::new();
    let (batches, error) = feed_bytewise(&mut parser, &frame);
    assert_eq!(error, None);
    assert_eq!(batches, vec![vec![bulk(b"ECHO"), bulk(&payload)]]);
}

#[test]
fn writer_framing_is_exact_for_every_type() {
    let cases: Vec<(RespValue, &[u8])> = vec![
        (RespValue::SimpleString("OK".into()), b"+OK\r\n"),
        (RespValue::Error("ERR nope".into()), b"-ERR nope\r\n"),
        (RespValue::Integer(-1), b":-1\r\n"),
        (RespValue::BulkString(None), b"$-1\r\n"),
        (RespValue::BulkString(Some(b"xy".to_vec())), b"$2\r\nxy\r\n"),
        (RespValue::Array(None), b"*-1\r\n"),
        (RespValue::Array(Some(Vec::new())), b"*0\r\n"),
        (
            RespValue::Array(Some(vec![RespValue::SimpleString("A".into())])),
            b"*1\r\n+A\r\n",
        ),
    ];

    for (value, expected) in cases {
        assert_eq!(encode(&value), expected, "value: {value:?}");
    }
}

#[test]
fn encode_into_appends_to_an_existing_buffer() {
    let mut out = vec![b'x'];
    encode_into(
        &RespValue::Array(Some(vec![RespValue::Integer(1)])),
        &mut out,
    );
    assert_eq!(out, b"x*1\r\n:1\r\n");
}

// ---------------------------------------------------------------------------
// Fragmentation, pipelining, and stream ordering
// ---------------------------------------------------------------------------

#[test]
fn partial_input_at_every_byte_boundary() {
    let frame = RespValue::Array(Some(vec![
        bulk(b"SET"),
        bulk(b"key"),
        bulk(b"a\r\n\0b"),
        RespValue::Integer(-7),
    ]));
    let RespValue::Array(Some(expected)) = frame.clone() else {
        unreachable!("constructed above");
    };
    let bytes = encode(&frame);

    for split in 0..=bytes.len() {
        let mut parser = RespParser::new();
        let mut batches = Vec::new();

        for part in [&bytes[..split], &bytes[split..]] {
            match parser.feed(part) {
                ParserEvent::Commands(mut batch) => batches.append(&mut batch),
                ParserEvent::Incomplete => {}
                ParserEvent::ProtocolError(err) => panic!("split {split}: {err}"),
            }
        }

        assert_eq!(batches, vec![expected.clone()], "split {split}");
        assert_eq!(parser.take_error(), None, "split {split}");
    }
}

#[test]
fn truncated_frames_are_incomplete_not_errors() {
    let bytes = command_frame(&[b"PING"]);

    for split in 0..bytes.len() {
        let mut parser = RespParser::new();
        assert_eq!(
            parser.feed(&bytes[..split]),
            ParserEvent::Incomplete,
            "split {split}"
        );
        assert_eq!(parser.take_error(), None, "split {split}");
        assert!(!parser.is_failed(), "split {split}");
    }
}

#[test]
fn pipelined_commands_are_batched_in_order() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&command_frame(&[b"PING"]));
    bytes.extend_from_slice(&command_frame(&[b"SET", b"k", b"a\r\nb"]));
    bytes.extend_from_slice(&command_frame(&[b"ECHO", b"\0"]));

    let expected = vec![
        command(&[b"PING"]),
        command(&[b"SET", b"k", b"a\r\nb"]),
        command(&[b"ECHO", b"\0"]),
    ];

    let mut parser = RespParser::new();
    assert_eq!(parser.feed(&bytes), ParserEvent::Commands(expected));
}

#[test]
fn pipelined_stream_survives_every_split() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&command_frame(&[b"PING"]));
    bytes.extend_from_slice(&command_frame(&[b"GET", b"missing"]));
    bytes.extend_from_slice(&command_frame(&[b"ECHO", b"a\r\nb"]));

    let expected = vec![
        command(&[b"PING"]),
        command(&[b"GET", b"missing"]),
        command(&[b"ECHO", b"a\r\nb"]),
    ];

    for split in 0..=bytes.len() {
        let mut parser = RespParser::new();
        let mut batches = Vec::new();
        for part in [&bytes[..split], &bytes[split..]] {
            if let ParserEvent::Commands(mut batch) = parser.feed(part) {
                batches.append(&mut batch);
            }
        }
        assert_eq!(batches, expected, "split {split}");
        assert!(!parser.is_failed(), "split {split}");
    }
}

#[test]
fn complete_commands_survive_a_trailing_half_command() {
    let mut bytes = command_frame(&[b"PING"]);
    bytes.extend_from_slice(b"*2\r\n$4\r\nECHO\r\n$5\r\nhel");

    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(&bytes),
        ParserEvent::Commands(vec![command(&[b"PING"])])
    );
    assert_eq!(
        parser.feed(b"lo\r\n"),
        ParserEvent::Commands(vec![command(&[b"ECHO", b"hello"])])
    );
    assert_eq!(parser.take_error(), None);
}

#[test]
fn take_error_is_drained_once_after_a_valid_batch() {
    let mut parser = RespParser::new();
    // One valid command immediately followed by an invalid array header.
    let event = parser.feed(b"*1\r\n$4\r\nPING\r\n*x\r\n");
    assert_eq!(event, ParserEvent::Commands(vec![command(&[b"PING"])]));

    assert_eq!(
        parser.take_error(),
        Some(ProtocolError::InvalidInteger {
            field: "array length"
        })
    );
    assert_eq!(parser.take_error(), None);
    assert!(parser.is_failed());

    // A failed parser never pretends to recover.
    assert_eq!(
        parser.feed(b"*1\r\n$4\r\nPING\r\n"),
        ParserEvent::ProtocolError(ProtocolError::InvalidInteger {
            field: "array length"
        })
    );
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

#[test]
fn malformed_inputs_are_rejected_deterministically() {
    let cases: Vec<(&[u8], ProtocolError)> = vec![
        (
            b"*x\r\n",
            ProtocolError::InvalidInteger {
                field: "array length",
            },
        ),
        (
            b"*\r\n",
            ProtocolError::InvalidInteger {
                field: "array length",
            },
        ),
        (
            b"*+1\r\n",
            ProtocolError::InvalidInteger {
                field: "array length",
            },
        ),
        (
            b"*1\r\n$\r\n",
            ProtocolError::InvalidInteger {
                field: "bulk length",
            },
        ),
        (
            b"*1\r\n$5\r\nhello\rX",
            ProtocolError::MissingBulkTerminator,
        ),
        (b"*1\r\n$\rX\r\n", ProtocolError::InvalidLineTerminator),
        (
            b"*1\r\n$-2\r\n",
            ProtocolError::InvalidLength {
                field: "bulk length",
                value: -2,
            },
        ),
        (
            b"*1\r\n$\r\n",
            ProtocolError::InvalidInteger {
                field: "bulk length",
            },
        ),
        (b"*-1\r\n", ProtocolError::NullCommandArray),
        (
            b"*-2\r\n",
            ProtocolError::InvalidLength {
                field: "array length",
                value: -2,
            },
        ),
        (b"+OK\r\n", ProtocolError::ExpectedArray { found: b'+' }),
        (b"PING\r\n", ProtocolError::ExpectedArray { found: b'P' }),
        (
            b"*1\r\n#x\r\n",
            ProtocolError::InvalidTypeMarker { marker: b'#' },
        ),
        (
            b"*1\r\n\xff\r\n",
            ProtocolError::InvalidTypeMarker { marker: 0xff },
        ),
    ];

    for (input, expected) in cases {
        let mut parser = RespParser::new();
        assert_eq!(
            parser.feed(input),
            ParserEvent::ProtocolError(expected.clone()),
            "input: {input:?}"
        );
        assert_eq!(parser.take_error(), Some(expected), "input: {input:?}");
    }
}

#[test]
fn valid_command_before_malformed_input_is_not_discarded() {
    let mut parser = RespParser::new();
    // Valid PING, then two bytes that begin no legal frame.
    let event = parser.feed(b"*1\r\n$4\r\nPING\r\n\r\n");
    assert_eq!(event, ParserEvent::Commands(vec![command(&[b"PING"])]));
    assert_eq!(
        parser.take_error(),
        Some(ProtocolError::ExpectedArray { found: b'\r' })
    );
}

#[test]
fn malformed_input_after_a_pipeline_returns_the_whole_batch() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&command_frame(&[b"PING"]));
    bytes.extend_from_slice(&command_frame(&[b"ECHO", b"one"]));
    bytes.extend_from_slice(&command_frame(&[b"ECHO", b"two"]));
    bytes.extend_from_slice(b"*x\r\n");

    let expected = vec![
        command(&[b"PING"]),
        command(&[b"ECHO", b"one"]),
        command(&[b"ECHO", b"two"]),
    ];

    let mut parser = RespParser::new();
    let event = parser.feed(&bytes);
    let ParserEvent::Commands(batch) = event else {
        panic!("expected the valid batch, got {event:?}");
    };
    assert_eq!(batch, expected);
    assert_eq!(
        parser.take_error(),
        Some(ProtocolError::InvalidInteger {
            field: "array length"
        })
    );
    assert_eq!(parser.take_error(), None);
}

#[test]
fn negative_and_overflowing_numbers_are_classified() {
    // i64::MIN magnitude is representable; it is a length, so it is invalid.
    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(b"*1\r\n$-9223372036854775808\r\n"),
        ParserEvent::ProtocolError(ProtocolError::InvalidLength {
            field: "bulk length",
            value: i64::MIN,
        })
    );

    // One past i64::MAX.
    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(b"*1\r\n$9223372036854775808\r\n"),
        ParserEvent::ProtocolError(ProtocolError::IntegerOverflow {
            field: "bulk length"
        })
    );

    // More than 20 digits is rejected without scanning to the terminator.
    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(b"*1\r\n$99999999999999999999999\r\n"),
        ParserEvent::ProtocolError(ProtocolError::IntegerOverflow {
            field: "bulk length"
        })
    );

    // Oversized array count.
    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(b"*9999999999999999999\r\n"),
        ParserEvent::ProtocolError(ProtocolError::IntegerOverflow {
            field: "array length"
        })
    );

    // `-0` is zero: a legal empty bulk string.
    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(b"*1\r\n$-0\r\n\r\n"),
        ParserEvent::Commands(vec![vec![bulk(b"")]])
    );
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

fn tiny_limits() -> ProtocolLimits {
    ProtocolLimits {
        max_bulk_len: 4,
        max_array_len: 3,
        max_request_size: 64,
        max_nesting_depth: 3,
    }
}

#[test]
fn bulk_length_limit_is_inclusive() {
    let mut parser = RespParser::with_limits(tiny_limits());
    assert_eq!(
        parser.feed(b"*1\r\n$4\r\nabcd\r\n"),
        ParserEvent::Commands(vec![vec![bulk(b"abcd")]])
    );

    let mut parser = RespParser::with_limits(tiny_limits());
    assert_eq!(
        parser.feed(b"*1\r\n$5\r\nabcde\r\n"),
        ParserEvent::ProtocolError(ProtocolError::BulkTooLong { len: 5, limit: 4 })
    );
}

#[test]
fn array_length_limit_is_inclusive() {
    let mut parser = RespParser::with_limits(tiny_limits());
    assert_eq!(
        parser.feed(b"*3\r\n:1\r\n:2\r\n:3\r\n"),
        ParserEvent::Commands(vec![vec![
            RespValue::Integer(1),
            RespValue::Integer(2),
            RespValue::Integer(3),
        ]])
    );

    let mut parser = RespParser::with_limits(tiny_limits());
    assert_eq!(
        parser.feed(b"*4\r\n:1\r\n:2\r\n:3\r\n:4\r\n"),
        ParserEvent::ProtocolError(ProtocolError::ArrayTooLarge { len: 4, limit: 3 })
    );
}

#[test]
fn request_size_limit_accounts_for_framing() {
    let limits = ProtocolLimits {
        max_request_size: 14,
        ..tiny_limits()
    };
    // `*1\r\n` + `$4\r\n` + 4 payload + `\r\n` = exactly 14.
    let mut parser = RespParser::with_limits(limits);
    assert_eq!(
        parser.feed(b"*1\r\n$4\r\nabcd\r\n"),
        ParserEvent::Commands(vec![vec![bulk(b"abcd")]])
    );

    let limits = ProtocolLimits {
        max_request_size: 13,
        ..tiny_limits()
    };
    let mut parser = RespParser::with_limits(limits);
    assert_eq!(
        parser.feed(b"*1\r\n$4\r\nabcd\r\n"),
        ParserEvent::ProtocolError(ProtocolError::RequestTooLarge {
            size: 14,
            limit: 13
        })
    );
}

#[test]
fn request_buffer_growth_is_bounded() {
    let limits = ProtocolLimits {
        max_request_size: 8,
        ..ProtocolLimits::default()
    };
    let mut parser = RespParser::with_limits(limits);

    // 12 bytes of a header that will never terminate: bounded before CRLF.
    assert_eq!(
        parser.feed(b"*1\r\n$1000000"),
        ParserEvent::ProtocolError(ProtocolError::RequestTooLarge { size: 12, limit: 8 })
    );
    assert!(parser.is_failed());
}

#[test]
fn nesting_depth_limit_is_enforced() {
    let limits = ProtocolLimits {
        max_nesting_depth: 2,
        ..ProtocolLimits::default()
    };

    // Level 2 is allowed.
    let mut parser = RespParser::with_limits(limits);
    assert_eq!(
        parser.feed(b"*1\r\n*1\r\n:1\r\n"),
        ParserEvent::Commands(vec![vec![RespValue::Array(Some(vec![
            RespValue::Integer(1)
        ]))]])
    );

    // Level 3 is not.
    let mut parser = RespParser::with_limits(limits);
    assert_eq!(
        parser.feed(b"*1\r\n*1\r\n*1\r\n:1\r\n"),
        ParserEvent::ProtocolError(ProtocolError::NestingTooDeep { depth: 3, limit: 2 })
    );
}

#[test]
fn default_depth_limit_blocks_deep_nesting() {
    // 129 nested single-element arrays: cheap on the wire, must not recurse
    // without bound.
    let mut bytes = Vec::new();
    for _ in 0..129 {
        bytes.extend_from_slice(b"*1\r\n");
    }
    bytes.extend_from_slice(b":1\r\n");

    let mut parser = RespParser::new();
    assert_eq!(
        parser.feed(&bytes),
        ParserEvent::ProtocolError(ProtocolError::NestingTooDeep {
            depth: 129,
            limit: 128
        })
    );
}

// ---------------------------------------------------------------------------
// Request-size semantics
//
// `max_request_size` bounds one top-level command. It is deliberately *not* a
// bound on how much input a caller may feed at once.
// ---------------------------------------------------------------------------

/// Handy limits for the cases below: a 14-byte command fits exactly.
fn tight_request_limits(max_request_size: u64) -> ProtocolLimits {
    ProtocolLimits {
        max_bulk_len: 64,
        max_array_len: 4,
        max_request_size,
        max_nesting_depth: 8,
    }
}

/// `PING`-sized frame, exactly 14 bytes.
const SMALL_COMMAND: &[u8] = b"*1\r\n$4\r\nabcd\r\n";

/// A 200-command pipeline is ~2.8 KiB. The aggregate is far past the 14-byte
/// per-command limit and must still be accepted.
#[test]
fn pipelined_commands_are_limited_per_command_not_in_aggregate() {
    let mut stream = Vec::new();
    for _ in 0..200 {
        stream.extend_from_slice(SMALL_COMMAND);
    }
    assert!(stream.len() > 14 * 100);

    let mut parser = RespParser::with_limits(tight_request_limits(14));
    assert_eq!(
        parser.feed(&stream),
        ParserEvent::Commands(vec![vec![bulk(b"abcd")]; 200])
    );
    assert_eq!(parser.take_error(), None);
}

/// Fragmentation must not change accounting: every split of a 3-command
/// pipeline at the per-command limit stays valid.
#[test]
fn pipelined_commands_at_the_limit_survive_every_split() {
    let mut stream = Vec::new();
    for _ in 0..3 {
        stream.extend_from_slice(SMALL_COMMAND);
    }
    let expected = vec![vec![bulk(b"abcd")]; 3];

    for split in 0..=stream.len() {
        let mut parser = RespParser::with_limits(tight_request_limits(14));
        let mut batches = Vec::new();
        for part in [&stream[..split], &stream[split..]] {
            if let ParserEvent::Commands(mut batch) = parser.feed(part) {
                batches.append(&mut batch);
            }
        }
        assert_eq!(batches, expected, "split {split}");
        assert!(!parser.is_failed(), "split {split}");
        assert_eq!(parser.take_error(), None, "split {split}");
    }
}

/// The aggregate limit does not apply, but an individually oversized command
/// is still rejected — after the valid prefix has been delivered.
#[test]
fn oversized_command_in_a_pipeline_is_rejected_after_the_valid_prefix() {
    let mut stream = SMALL_COMMAND.to_vec();
    stream.extend_from_slice(b"*2\r\n$3\r\nabc\r\n$3\r\nabc\r\n"); // 22 bytes

    let mut parser = RespParser::with_limits(tight_request_limits(14));
    assert_eq!(
        parser.feed(&stream),
        ParserEvent::Commands(vec![vec![bulk(b"abcd")]])
    );
    assert_eq!(
        parser.take_error(),
        Some(ProtocolError::RequestTooLarge {
            size: 22,
            limit: 14
        })
    );
    assert_eq!(parser.take_error(), None);
}

/// An incomplete request is bounded while it is still buffered: 16 bytes is
/// allowed, 17 is not.
#[test]
fn retained_partial_request_respects_the_limit_boundary() {
    let mut parser = RespParser::with_limits(tight_request_limits(16));

    let mut at_limit = b"*1\r\n+".to_vec();
    at_limit.extend(std::iter::repeat_n(b'a', 11));
    assert_eq!(at_limit.len(), 16);
    assert_eq!(parser.feed(&at_limit), ParserEvent::Incomplete);
    assert!(!parser.is_failed());

    assert_eq!(
        parser.feed(b"a"),
        ParserEvent::ProtocolError(ProtocolError::RequestTooLarge {
            size: 17,
            limit: 16
        })
    );
}

/// Growth across many feeds is charged once per new byte, so a large request
/// split into 1-byte feeds still completes at the exact boundary.
#[test]
fn large_partial_request_accounts_bytes_once_across_feeds() {
    const PAYLOAD: usize = 4096;
    let mut frame = format!("*1\r\n${PAYLOAD}\r\n").into_bytes();
    frame.extend(std::iter::repeat_n(b'z', PAYLOAD));
    frame.extend_from_slice(b"\r\n");
    let limit = frame.len() as u64;

    let mut parser = RespParser::with_limits(ProtocolLimits {
        max_bulk_len: limit,
        max_array_len: 4,
        max_request_size: limit,
        max_nesting_depth: 8,
    });
    let mut batches = Vec::new();
    for byte in &frame {
        if let ParserEvent::Commands(mut batch) = parser.feed(std::slice::from_ref(byte)) {
            batches.append(&mut batch);
        }
    }

    assert_eq!(batches, vec![vec![bulk(&vec![b'z'; PAYLOAD])]]);
    assert!(!parser.is_failed());
}

// ---------------------------------------------------------------------------
// Randomized / property-style coverage
// ---------------------------------------------------------------------------

#[test]
fn random_commands_round_trip_under_random_chunking() {
    let mut rng = Rng::new(0x5eed_1234_abcd_0001);

    for iteration in 0..300 {
        let arg_count = 1 + rng.below(4);
        let args: Vec<Vec<u8>> = (0..arg_count)
            .map(|_| {
                let len = rng.below(13);
                rng.bytes(len)
            })
            .collect();
        let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();

        let expected = command(&refs);
        let frame = encode(&RespValue::Array(Some(expected.clone())));

        let mut parser = RespParser::new();
        let chunk = 1 + rng.below(7);
        let (batches, error) = feed_chunked(&mut parser, &frame, chunk);

        assert_eq!(error, None, "iteration {iteration}");
        assert_eq!(batches, vec![expected], "iteration {iteration}");
        assert_eq!(parser.take_error(), None, "iteration {iteration}");
    }
}

#[test]
fn random_bytes_never_panic_and_parsed_values_are_reencodable() {
    let mut rng = Rng::new(0x0bad_c0de_0000_0001);

    for iteration in 0..500 {
        let len = rng.below(65);
        let data = rng.bytes(len);
        let chunk = 1 + rng.below(5);

        let mut parser = RespParser::new();
        let (batches, error) = feed_chunked(&mut parser, &data, chunk);

        // Every emitted command must survive an encode → parse round trip.
        for duplicate in &batches {
            let frame = encode(&RespValue::Array(Some(duplicate.clone())));
            let mut again = RespParser::new();
            assert_eq!(
                again.feed(&frame),
                ParserEvent::Commands(vec![duplicate.clone()]),
                "iteration {iteration}, data: {data:?}"
            );
        }

        // A failed parser must keep reporting the same error; a successful one
        // must not have latched anything.
        if let Some(err) = error {
            assert!(parser.is_failed(), "iteration {iteration}");
            assert!(matches!(
                parser.feed(b""),
                ParserEvent::ProtocolError(replayed) if replayed == err
            ));
        } else {
            assert!(!parser.is_failed(), "iteration {iteration}");
            assert_eq!(parser.take_error(), None, "iteration {iteration}");
        }
    }
}

#[test]
fn random_fragments_never_lose_commands() {
    let mut rng = Rng::new(0xfeed_face_0000_0007);

    for iteration in 0..200 {
        let count = 1 + rng.below(5);
        let mut expected = Vec::new();
        let mut stream = Vec::new();

        for _ in 0..count {
            let arg_len = rng.below(9);
            let args = [b"ECHO".to_vec(), rng.bytes(arg_len)];
            let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            expected.push(command(&refs));
            stream.extend_from_slice(&encode(&RespValue::Array(Some(command(&refs)))));
        }

        let chunk = 1 + rng.below(4);
        let mut parser = RespParser::new();
        let (batches, error) = feed_chunked(&mut parser, &stream, chunk);

        assert_eq!(error, None, "iteration {iteration}");
        assert_eq!(batches, expected, "iteration {iteration}");
    }
}
