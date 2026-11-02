//! Command-layer integration tests.
//!
//! These drive the real M1 parser and writer around the M2 executor:
//! wire bytes → `RespParser` → `Command` → `Engine` → `encode`. No sockets and
//! no runtime, but every assertion is on the exact bytes a client would see.

use kestrel::commands::Engine;
use kestrel::protocol::writer::encode;
use kestrel::protocol::{ParserEvent, RespParser, RespValue};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parses one command frame from wire bytes.
fn decode(frame: &[u8]) -> Vec<RespValue> {
    let mut parser = RespParser::new();
    match parser.feed(frame) {
        ParserEvent::Commands(mut batch) if batch.len() == 1 => batch.remove(0),
        other => panic!("expected exactly one command, got {other:?}"),
    }
}

/// Encodes a command array in RESP2 so tests can build frames from parts.
fn frame(parts: &[&[u8]]) -> Vec<u8> {
    let elements: Vec<RespValue> = parts
        .iter()
        .map(|part| RespValue::BulkString(Some(part.to_vec())))
        .collect();
    encode(&RespValue::Array(Some(elements)))
}

/// Executes one wire frame and returns the encoded reply bytes.
fn round_trip(engine: &Engine, frame: &[u8], now: Instant) -> Vec<u8> {
    encode(&engine.execute_args(&decode(frame), now))
}

/// Executes one wire frame and returns the reply as text (test replies are
/// ASCII or UTF-8 in every assertion below).
fn reply(engine: &Engine, frame: &[u8], now: Instant) -> String {
    String::from_utf8(round_trip(engine, frame, now)).expect("reply is valid UTF-8")
}

fn stored(engine: &Engine) -> usize {
    match engine.store().read() {
        Ok(store) => store.len(),
        Err(_) => panic!("store lock poisoned"),
    }
}

// ---------------------------------------------------------------------------
// Happy paths
// ---------------------------------------------------------------------------

#[test]
fn ping_and_echo_over_the_wire() {
    let engine = Engine::new();
    let now = Instant::now();

    assert_eq!(reply(&engine, b"*1\r\n$4\r\nPING\r\n", now), "+PONG\r\n");
    assert_eq!(
        reply(&engine, &frame(&[b"PING", b"hello"]), now),
        "$5\r\nhello\r\n"
    );
    assert_eq!(
        reply(&engine, &frame(&[b"ECHO", b"hi"]), now),
        "$2\r\nhi\r\n"
    );
}

#[test]
fn set_get_del_exists_over_the_wire() {
    let engine = Engine::new();
    let now = Instant::now();

    assert_eq!(
        reply(&engine, &frame(&[b"SET", b"k", b"v"]), now),
        "+OK\r\n"
    );
    assert_eq!(reply(&engine, &frame(&[b"GET", b"k"]), now), "$1\r\nv\r\n");
    assert_eq!(reply(&engine, &frame(&[b"EXISTS", b"k"]), now), ":1\r\n");
    assert_eq!(reply(&engine, &frame(&[b"DEL", b"k"]), now), ":1\r\n");
    assert_eq!(reply(&engine, &frame(&[b"GET", b"k"]), now), "$-1\r\n");
    assert_eq!(reply(&engine, &frame(&[b"EXISTS", b"k"]), now), ":0\r\n");
}

#[test]
fn get_of_a_missing_key_is_a_null_bulk_string() {
    let engine = Engine::new();
    assert_eq!(
        reply(&engine, &frame(&[b"GET", b"nope"]), Instant::now()),
        "$-1\r\n"
    );
}

#[test]
fn command_names_are_case_insensitive() {
    let engine = Engine::new();
    let now = Instant::now();

    assert_eq!(
        reply(&engine, &frame(&[b"set", b"k", b"v"]), now),
        "+OK\r\n"
    );
    assert_eq!(reply(&engine, &frame(&[b"GeT", b"k"]), now), "$1\r\nv\r\n");
    assert_eq!(reply(&engine, &frame(&[b"exists", b"k"]), now), ":1\r\n");
    assert_eq!(reply(&engine, &frame(&[b"del", b"k"]), now), ":1\r\n");
}

#[test]
fn binary_keys_and_values_round_trip_through_the_wire() {
    let engine = Engine::new();
    let now = Instant::now();
    let key: &[u8] = b"k\r\n\0\xff";
    let value: &[u8] = b"v\r\n\0\xfe\x01";

    assert_eq!(
        reply(&engine, &frame(&[b"SET", key, value]), now),
        "+OK\r\n"
    );
    assert_eq!(
        round_trip(&engine, &frame(&[b"GET", key]), now),
        frame_bulk(value)
    );
    assert_eq!(
        round_trip(&engine, &frame(&[b"ECHO", value]), now),
        frame_bulk(value)
    );
}

fn frame_bulk(payload: &[u8]) -> Vec<u8> {
    encode(&RespValue::BulkString(Some(payload.to_vec())))
}

// ---------------------------------------------------------------------------
// Expiry
// ---------------------------------------------------------------------------

#[test]
fn set_ex_expires_at_the_second_boundary() {
    let engine = Engine::new();
    let now = Instant::now();
    assert_eq!(
        reply(&engine, &frame(&[b"SET", b"k", b"v", b"EX", b"1"]), now),
        "+OK\r\n"
    );

    let just_before = now + Duration::from_millis(999);
    assert_eq!(
        reply(&engine, &frame(&[b"GET", b"k"]), just_before),
        "$1\r\nv\r\n"
    );

    let at_deadline = now + Duration::from_secs(1);
    assert_eq!(
        reply(&engine, &frame(&[b"GET", b"k"]), at_deadline),
        "$-1\r\n"
    );
}

#[test]
fn set_px_expires_at_the_millisecond_boundary() {
    let engine = Engine::new();
    let now = Instant::now();
    assert_eq!(
        reply(&engine, &frame(&[b"SET", b"k", b"v", b"PX", b"250"]), now),
        "+OK\r\n"
    );

    assert_eq!(
        reply(
            &engine,
            &frame(&[b"GET", b"k"]),
            now + Duration::from_millis(249)
        ),
        "$1\r\nv\r\n"
    );
    assert_eq!(
        reply(
            &engine,
            &frame(&[b"GET", b"k"]),
            now + Duration::from_millis(250)
        ),
        "$-1\r\n"
    );
}

#[test]
fn expired_keys_are_absent_and_removed_for_get_del_and_exists() {
    let engine = Engine::new();
    let now = Instant::now();
    let later = now + Duration::from_secs(10);

    engine.execute_args(&decode(&frame(&[b"SET", b"a", b"1", b"PX", b"5"])), now);
    engine.execute_args(&decode(&frame(&[b"SET", b"b", b"2", b"PX", b"5"])), now);
    assert_eq!(stored(&engine), 2);

    assert_eq!(reply(&engine, &frame(&[b"GET", b"a"]), later), "$-1\r\n");
    assert_eq!(stored(&engine), 1, "GET must remove the expired entry");

    assert_eq!(reply(&engine, &frame(&[b"EXISTS", b"b"]), later), ":0\r\n");
    assert_eq!(stored(&engine), 0, "EXISTS must remove the expired entry");

    // DEL treats an expired key as absent: it removes it but does not count it.
    engine.execute_args(&decode(&frame(&[b"SET", b"c", b"3", b"PX", b"5"])), now);
    assert_eq!(reply(&engine, &frame(&[b"DEL", b"c"]), later), ":0\r\n");
    assert_eq!(stored(&engine), 0);
}

#[test]
fn a_plain_set_clears_a_previous_expiry() {
    let engine = Engine::new();
    let now = Instant::now();

    engine.execute_args(&decode(&frame(&[b"SET", b"k", b"v1", b"PX", b"10"])), now);
    engine.execute_args(&decode(&frame(&[b"SET", b"k", b"v2"])), now);

    assert_eq!(
        reply(
            &engine,
            &frame(&[b"GET", b"k"]),
            now + Duration::from_secs(3600)
        ),
        "$2\r\nv2\r\n"
    );
}

#[test]
fn set_with_a_long_ttl_does_not_overflow_a_deadline() {
    let engine = Engine::new();
    let now = Instant::now();

    // A very large but representable TTL must succeed (or, if the platform
    // cannot represent the deadline, be rejected cleanly — never panic).
    let reply = reply(
        &engine,
        &frame(&[b"SET", b"k", b"v", b"PX", b"9223372036854775807"]),
        now,
    );
    assert!(
        reply == "+OK\r\n" || reply.starts_with("-ERR invalid expire time"),
        "unexpected reply: {reply}"
    );
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[test]
fn command_errors_are_error_replies_and_the_stream_continues() {
    let engine = Engine::new();
    let now = Instant::now();

    let mut parser = RespParser::new();
    // One unknown command followed by a valid one in the same read.
    let event = parser.feed(b"*1\r\n$5\r\nNOPE!\r\n*1\r\n$4\r\nPING\r\n");
    let ParserEvent::Commands(batch) = event else {
        panic!("expected a batch, got {event:?}");
    };
    assert_eq!(batch.len(), 2, "a command error is not a protocol error");

    let replies: Vec<Vec<u8>> = batch
        .into_iter()
        .map(|args| encode(&engine.execute_args(&args, now)))
        .collect();

    assert_eq!(
        String::from_utf8_lossy(&replies[0]),
        "-ERR unknown command 'NOPE!'\r\n"
    );
    assert_eq!(String::from_utf8_lossy(&replies[1]), "+PONG\r\n");
}

#[test]
fn empty_command_array_is_a_command_error() {
    let engine = Engine::new();
    assert_eq!(
        reply(&engine, b"*0\r\n", Instant::now()),
        "-ERR empty command\r\n"
    );
}

#[test]
fn unknown_commands_report_their_name() {
    let engine = Engine::new();
    assert_eq!(
        reply(&engine, &frame(&[b"FLUSHALL"]), Instant::now()),
        "-ERR unknown command 'FLUSHALL'\r\n"
    );
}

#[test]
fn arity_errors_are_reported_per_command() {
    let engine = Engine::new();
    let now = Instant::now();

    let cases: &[(&[&[u8]], &str)] = &[
        (
            &[b"PING", b"a", b"b"],
            "-ERR wrong number of arguments for 'ping' command\r\n",
        ),
        (
            &[b"ECHO"],
            "-ERR wrong number of arguments for 'echo' command\r\n",
        ),
        (
            &[b"GET"],
            "-ERR wrong number of arguments for 'get' command\r\n",
        ),
        (
            &[b"SET", b"k"],
            "-ERR wrong number of arguments for 'set' command\r\n",
        ),
        (
            &[b"DEL"],
            "-ERR wrong number of arguments for 'del' command\r\n",
        ),
        (
            &[b"EXISTS"],
            "-ERR wrong number of arguments for 'exists' command\r\n",
        ),
    ];

    for (parts, expected) in cases {
        assert_eq!(
            reply(&engine, &frame(parts), now),
            *expected,
            "parts: {parts:?}"
        );
    }
}

#[test]
fn unsupported_set_options_are_syntax_errors() {
    let engine = Engine::new();
    let now = Instant::now();

    for option in [b"NX".as_slice(), b"XX", b"KEEPTTL", b"GET", b"EXAT"] {
        assert_eq!(
            reply(&engine, &frame(&[b"SET", b"k", b"v", option, b"1"]), now),
            "-ERR syntax error\r\n",
            "option: {:?}",
            String::from_utf8_lossy(option)
        );
    }
    assert_eq!(stored(&engine), 0, "rejected SETs must not store anything");
}

#[test]
fn invalid_expire_values_are_rejected() {
    let engine = Engine::new();
    let now = Instant::now();

    for amount in [b"0".as_slice(), b"-1", b"-9223372036854775808"] {
        assert_eq!(
            reply(&engine, &frame(&[b"SET", b"k", b"v", b"EX", amount]), now),
            "-ERR invalid expire time in 'set' command\r\n",
            "amount: {:?}",
            String::from_utf8_lossy(amount)
        );
    }

    for amount in [
        b"abc".as_slice(),
        b"1.5",
        b"+5",
        b"",
        b"9999999999999999999999",
    ] {
        assert_eq!(
            reply(&engine, &frame(&[b"SET", b"k", b"v", b"PX", amount]), now),
            "-ERR value is not an integer or out of range\r\n",
            "amount: {:?}",
            String::from_utf8_lossy(amount)
        );
    }

    assert_eq!(stored(&engine), 0);
}

#[test]
fn non_bulk_arguments_are_rejected_without_coercion() {
    let engine = Engine::new();
    let now = Instant::now();

    let get_integer = encode(&RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"GET".to_vec())),
        RespValue::Integer(1),
    ])));
    assert_eq!(
        String::from_utf8_lossy(&round_trip(&engine, &get_integer, now)),
        "-ERR invalid argument type\r\n"
    );

    let get_null = encode(&RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"GET".to_vec())),
        RespValue::BulkString(None),
    ])));
    assert_eq!(
        String::from_utf8_lossy(&round_trip(&engine, &get_null, now)),
        "-ERR invalid argument type\r\n"
    );

    let get_nested = encode(&RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"GET".to_vec())),
        RespValue::Array(Some(vec![RespValue::bulk("k")])),
    ])));
    assert_eq!(
        String::from_utf8_lossy(&round_trip(&engine, &get_nested, now)),
        "-ERR invalid argument type\r\n"
    );
}

#[test]
fn a_non_bulk_command_name_is_rejected() {
    let engine = Engine::new();
    let frame = encode(&RespValue::Array(Some(vec![RespValue::Integer(7)])));

    assert_eq!(
        String::from_utf8_lossy(&round_trip(&engine, &frame, Instant::now())),
        "-ERR invalid command name\r\n"
    );
}

#[test]
fn command_errors_do_not_mutate_unrelated_keys() {
    let engine = Engine::new();
    let now = Instant::now();
    engine.execute_args(&decode(&frame(&[b"SET", b"keep", b"value"])), now);

    let bad_commands: &[&[&[u8]]] = &[
        &[b"SET", b"keep", b"changed", b"NX", b"1"],
        &[b"SET", b"keep", b"changed", b"PX", b"0"],
        &[b"SET", b"keep", b"changed", b"PX", b"not-a-number"],
        &[b"DEL"],
        &[b"GET"],
    ];

    for parts in bad_commands {
        assert!(
            reply(&engine, &frame(parts), now).starts_with("-ERR "),
            "parts: {parts:?}"
        );
        assert_eq!(
            reply(&engine, &frame(&[b"GET", b"keep"]), now),
            "$5\r\nvalue\r\n",
            "unrelated key changed by {parts:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

#[test]
fn a_pipelined_batch_executes_in_order_over_the_wire() {
    let engine = Engine::new();
    let now = Instant::now();

    let mut stream = Vec::new();
    for parts in [
        vec![b"SET".as_slice(), b"a", b"1"],
        vec![b"SET", b"b", b"2"],
        vec![b"GET", b"a"],
        vec![b"EXISTS", b"a", b"b", b"missing"],
        vec![b"DEL", b"a", b"b"],
        vec![b"GET", b"a"],
    ] {
        stream.extend_from_slice(&frame(&parts));
    }

    let mut parser = RespParser::new();
    let ParserEvent::Commands(batch) = parser.feed(&stream) else {
        panic!("expected one batch for the whole pipeline");
    };
    assert_eq!(batch.len(), 6);

    let mut wire = Vec::new();
    for args in batch {
        wire.extend_from_slice(&encode(&engine.execute_args(&args, now)));
    }

    assert_eq!(
        String::from_utf8_lossy(&wire),
        "+OK\r\n+OK\r\n$1\r\n1\r\n:2\r\n:2\r\n$-1\r\n"
    );
    assert_eq!(stored(&engine), 0);
}

#[test]
fn a_pipeline_that_mixes_errors_and_successes_keeps_going() {
    let engine = Engine::new();
    let now = Instant::now();

    let mut stream = Vec::new();
    for parts in [
        vec![b"SET".as_slice(), b"k", b"v"],
        vec![b"GET"],
        vec![b"GET", b"k"],
        vec![b"BOGUS"],
    ] {
        stream.extend_from_slice(&frame(&parts));
    }

    let mut parser = RespParser::new();
    let ParserEvent::Commands(batch) = parser.feed(&stream) else {
        panic!("expected one batch");
    };

    let replies: Vec<String> = batch
        .into_iter()
        .map(|args| String::from_utf8(encode(&engine.execute_args(&args, now))).expect("utf-8"))
        .collect();

    assert_eq!(replies[0], "+OK\r\n");
    assert_eq!(
        replies[1],
        "-ERR wrong number of arguments for 'get' command\r\n"
    );
    assert_eq!(replies[2], "$1\r\nv\r\n");
    assert_eq!(replies[3], "-ERR unknown command 'BOGUS'\r\n");
}
