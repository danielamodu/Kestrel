//! Command enum, interpreted from normalized protocol arguments.
//!
//! Parsing is a pure function of the decoded RESP2 array: it validates arity,
//! argument types, option structure, and expire integers, and returns a
//! [`Command`]. It performs no I/O and touches no storage, so it can be tested
//! exhaustively without a store or a connection.

use std::time::Duration;

use crate::protocol::RespValue;

use super::error::CommandError;

/// A validated command.
///
/// Every variant carries owned bytes, so a caller may drop the RESP values that
/// produced it immediately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// `PING` or `PING <message>`.
    Ping(Option<Vec<u8>>),
    /// `ECHO <message>`.
    Echo(Vec<u8>),
    /// `GET <key>`.
    Get(Vec<u8>),
    /// `SET <key> <value> [EX <seconds> | PX <milliseconds>]`.
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        /// Relative time-to-live, already validated as positive and converted
        /// to milliseconds. `None` means the key never expires.
        expire: Option<Duration>,
    },
    /// `DEL <key>...`.
    Del(Vec<Vec<u8>>),
    /// `EXISTS <key>...`.
    Exists(Vec<Vec<u8>>),
}

impl Command {
    /// Interprets a decoded command array.
    ///
    /// Element 0 names the command (ASCII case-insensitive); the rest are
    /// arguments. Only non-null bulk strings are accepted as names and
    /// arguments: no RESP value is coerced to bytes implicitly.
    pub fn parse(args: &[RespValue]) -> Result<Self, CommandError> {
        let Some((name, rest)) = args.split_first() else {
            return Err(CommandError::EmptyCommand);
        };
        let name = name.as_bulk().ok_or(CommandError::InvalidCommandName)?;

        match name.to_ascii_uppercase().as_slice() {
            b"PING" => match rest {
                [] => Ok(Self::Ping(None)),
                [message] => Ok(Self::Ping(Some(bulk(message)?))),
                _ => Err(CommandError::WrongArity { command: "ping" }),
            },
            b"ECHO" => match rest {
                [message] => Ok(Self::Echo(bulk(message)?)),
                _ => Err(CommandError::WrongArity { command: "echo" }),
            },
            b"GET" => match rest {
                [key] => Ok(Self::Get(bulk(key)?)),
                _ => Err(CommandError::WrongArity { command: "get" }),
            },
            b"SET" => parse_set(rest),
            b"DEL" => {
                if rest.is_empty() {
                    return Err(CommandError::WrongArity { command: "del" });
                }
                Ok(Self::Del(bulk_list(rest)?))
            }
            b"EXISTS" => {
                if rest.is_empty() {
                    return Err(CommandError::WrongArity { command: "exists" });
                }
                Ok(Self::Exists(bulk_list(rest)?))
            }
            _ => Err(CommandError::UnknownCommand(
                String::from_utf8_lossy(name).into_owned(),
            )),
        }
    }
}

/// `SET <key> <value> [EX <seconds> | PX <milliseconds>]`.
fn parse_set(rest: &[RespValue]) -> Result<Command, CommandError> {
    let (key, value, option_parts) = match rest {
        [key, value] => (bulk(key)?, bulk(value)?, None),
        // Options must arrive as a pair. A stray token is a syntax error, not
        // an arity error, because the command shape is recognisable.
        [key, value, option, amount] => (bulk(key)?, bulk(value)?, Some((option, amount))),
        [] | [_] => return Err(CommandError::WrongArity { command: "set" }),
        _ => return Err(CommandError::Syntax),
    };

    let expire = match option_parts {
        None => None,
        Some((option, amount)) => {
            let millis = match option_bytes(option)?.to_ascii_uppercase().as_slice() {
                b"EX" => expire_amount(amount)?
                    .checked_mul(1000)
                    .ok_or(CommandError::InvalidExpire { command: "set" })?,
                b"PX" => expire_amount(amount)?,
                // NX, XX, KEEPTTL, GET and anything else are out of scope.
                _ => return Err(CommandError::Syntax),
            };
            Some(Duration::from_millis(millis))
        }
    };

    Ok(Command::Set { key, value, expire })
}

/// A command name or option token: must be a non-null bulk string.
fn option_bytes(value: &RespValue) -> Result<&[u8], CommandError> {
    value.as_bulk().ok_or(CommandError::InvalidArgumentType)
}

/// A command argument: must be a non-null bulk string.
fn bulk(value: &RespValue) -> Result<Vec<u8>, CommandError> {
    option_bytes(value).map(<[u8]>::to_vec)
}

fn bulk_list(values: &[RespValue]) -> Result<Vec<Vec<u8>>, CommandError> {
    values.iter().map(bulk).collect()
}

/// Parses a positive expire amount (seconds for `EX`, milliseconds for `PX`).
///
/// Zero and negative values are rejected before any deadline arithmetic, so a
/// zero TTL can never reach the store through the command layer.
fn expire_amount(value: &RespValue) -> Result<u64, CommandError> {
    let bytes = option_bytes(value)?;
    let amount = parse_i64(bytes).ok_or(CommandError::NotAnInteger)?;

    if amount <= 0 {
        return Err(CommandError::InvalidExpire { command: "set" });
    }
    Ok(amount as u64)
}

/// Strict decimal `i64` parser: optional `-`, at least one ASCII digit, and no
/// other byte. Rejects `+5`, `1.5`, ` 5`, `5 `, empty input, and overflow.
fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let (negative, digits) = match bytes {
        [b'-', rest @ ..] => (true, rest),
        _ => (false, bytes),
    };
    if digits.is_empty() {
        return None;
    }

    let mut magnitude: u64 = 0;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return None;
        }
        magnitude = magnitude
            .checked_mul(10)?
            .checked_add(u64::from(byte - b'0'))?;
    }

    if negative {
        if magnitude > i64::MIN.unsigned_abs() {
            return None;
        }
        i64::try_from(-i128::from(magnitude)).ok()
    } else {
        i64::try_from(magnitude).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::Command;
    use crate::commands::error::CommandError;
    use crate::protocol::RespValue;
    use std::time::Duration;

    fn bulk(bytes: &[u8]) -> RespValue {
        RespValue::BulkString(Some(bytes.to_vec()))
    }

    fn parse(parts: &[&[u8]]) -> Result<Command, CommandError> {
        let args: Vec<RespValue> = parts.iter().map(|part| bulk(part)).collect();
        Command::parse(&args)
    }

    #[test]
    fn parses_every_supported_command() {
        assert_eq!(parse(&[b"PING"]), Ok(Command::Ping(None)));
        assert_eq!(
            parse(&[b"PING", b"hello"]),
            Ok(Command::Ping(Some(b"hello".to_vec())))
        );
        assert_eq!(parse(&[b"ECHO", b"hi"]), Ok(Command::Echo(b"hi".to_vec())));
        assert_eq!(parse(&[b"GET", b"k"]), Ok(Command::Get(b"k".to_vec())));
        assert_eq!(
            parse(&[b"SET", b"k", b"v"]),
            Ok(Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire: None,
            })
        );
        assert_eq!(
            parse(&[b"SET", b"k", b"v", b"EX", b"2"]),
            Ok(Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire: Some(Duration::from_secs(2)),
            })
        );
        assert_eq!(
            parse(&[b"SET", b"k", b"v", b"PX", b"250"]),
            Ok(Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire: Some(Duration::from_millis(250)),
            })
        );
        assert_eq!(
            parse(&[b"DEL", b"a", b"b"]),
            Ok(Command::Del(vec![b"a".to_vec(), b"b".to_vec()]))
        );
        assert_eq!(
            parse(&[b"EXISTS", b"a"]),
            Ok(Command::Exists(vec![b"a".to_vec()]))
        );
    }

    #[test]
    fn command_names_and_options_are_case_insensitive() {
        assert_eq!(parse(&[b"ping"]), Ok(Command::Ping(None)));
        assert_eq!(
            parse(&[b"PiNg", b"x"]),
            Ok(Command::Ping(Some(b"x".to_vec())))
        );
        assert_eq!(parse(&[b"gEt", b"k"]), Ok(Command::Get(b"k".to_vec())));

        let expected = Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            expire: Some(Duration::from_secs(1)),
        };
        assert_eq!(
            parse(&[b"set", b"k", b"v", b"ex", b"1"]),
            Ok(expected.clone())
        );
        assert_eq!(parse(&[b"SET", b"k", b"v", b"Ex", b"1"]), Ok(expected));
    }

    #[test]
    fn empty_array_is_an_empty_command_error() {
        assert_eq!(Command::parse(&[]), Err(CommandError::EmptyCommand));
    }

    #[test]
    fn a_non_bulk_command_name_is_rejected() {
        let args = [RespValue::Integer(1), bulk(b"k")];
        assert_eq!(Command::parse(&args), Err(CommandError::InvalidCommandName));

        let args = [RespValue::BulkString(None), bulk(b"k")];
        assert_eq!(Command::parse(&args), Err(CommandError::InvalidCommandName));

        let args = [RespValue::array(vec![bulk(b"PING")])];
        assert_eq!(Command::parse(&args), Err(CommandError::InvalidCommandName));
    }

    #[test]
    fn unknown_commands_are_reported_with_their_name() {
        assert_eq!(
            parse(&[b"NOPE"]),
            Err(CommandError::UnknownCommand("NOPE".into()))
        );
        assert_eq!(
            parse(&[b"getx", b"k"]),
            Err(CommandError::UnknownCommand("getx".into()))
        );
    }

    #[test]
    fn arity_is_validated_for_every_command() {
        let cases: &[(&[&[u8]], &'static str)] = &[
            (&[b"PING", b"a", b"b"], "ping"),
            (&[b"ECHO"], "echo"),
            (&[b"ECHO", b"a", b"b"], "echo"),
            (&[b"GET"], "get"),
            (&[b"GET", b"a", b"b"], "get"),
            (&[b"SET"], "set"),
            (&[b"SET", b"k"], "set"),
            (&[b"DEL"], "del"),
            (&[b"EXISTS"], "exists"),
        ];

        for (parts, command) in cases {
            assert_eq!(
                parse(parts),
                Err(CommandError::WrongArity { command }),
                "parts: {parts:?}"
            );
        }
    }

    #[test]
    fn malformed_set_option_structures_are_syntax_errors() {
        let cases: &[&[&[u8]]] = &[
            &[b"SET", b"k", b"v", b"EX"],
            &[b"SET", b"k", b"v", b"EX", b"1", b"extra"],
            &[b"SET", b"k", b"v", b"NX"],
            &[b"SET", b"k", b"v", b"XX"],
            &[b"SET", b"k", b"v", b"KEEPTTL"],
            &[b"SET", b"k", b"v", b"GET"],
            &[b"SET", b"k", b"v", b"EXAT", b"1"],
        ];

        for parts in cases {
            assert_eq!(parse(parts), Err(CommandError::Syntax), "parts: {parts:?}");
        }
    }

    #[test]
    fn unsupported_options_are_rejected_before_their_amount_is_parsed() {
        // The option token wins: a bad option is a syntax error even when the
        // trailing token is not an integer.
        assert_eq!(
            parse(&[b"SET", b"k", b"v", b"NX", b"abc"]),
            Err(CommandError::Syntax)
        );
    }

    #[test]
    fn expire_values_must_be_positive_integers() {
        let not_integers: &[&[u8]] = &[b"", b"abc", b"1.5", b"+5", b" 5", b"5 ", b"0x10", b"1 0"];
        for value in not_integers {
            assert_eq!(
                parse(&[b"SET", b"k", b"v", b"EX", value]),
                Err(CommandError::NotAnInteger),
                "value: {value:?}"
            );
        }

        // Overflow beyond i64 is not an integer for our purposes.
        assert_eq!(
            parse(&[b"SET", b"k", b"v", b"PX", b"99999999999999999999999"]),
            Err(CommandError::NotAnInteger)
        );

        let non_positive: &[&[u8]] = &[b"0", b"-1", b"-9223372036854775808"];
        for value in non_positive {
            assert_eq!(
                parse(&[b"SET", b"k", b"v", b"PX", value]),
                Err(CommandError::InvalidExpire { command: "set" }),
                "value: {value:?}"
            );
        }
    }

    #[test]
    fn ex_seconds_that_overflow_milliseconds_are_rejected() {
        // i64::MAX seconds * 1000 overflows i64 milliseconds.
        assert_eq!(
            parse(&[b"SET", b"k", b"v", b"EX", b"9223372036854775807"]),
            Err(CommandError::InvalidExpire { command: "set" })
        );
        // The largest representable millisecond count is still accepted.
        assert!(matches!(
            parse(&[b"SET", b"k", b"v", b"PX", b"9223372036854775807"]),
            Ok(Command::Set {
                expire: Some(_),
                ..
            })
        ));
    }

    #[test]
    fn arguments_must_be_bulk_strings() {
        let args = [bulk(b"GET"), RespValue::Integer(1)];
        assert_eq!(
            Command::parse(&args),
            Err(CommandError::InvalidArgumentType)
        );

        let args = [bulk(b"GET"), RespValue::BulkString(None)];
        assert_eq!(
            Command::parse(&args),
            Err(CommandError::InvalidArgumentType)
        );

        let args = [bulk(b"SET"), bulk(b"k"), RespValue::array(Vec::new())];
        assert_eq!(
            Command::parse(&args),
            Err(CommandError::InvalidArgumentType)
        );

        let args = [
            bulk(b"DEL"),
            bulk(b"a"),
            RespValue::SimpleString("b".into()),
        ];
        assert_eq!(
            Command::parse(&args),
            Err(CommandError::InvalidArgumentType)
        );
    }

    #[test]
    fn binary_keys_and_values_are_preserved_exactly() {
        let key = b"k\r\n\0\xff".to_vec();
        let value = b"v\r\n\0\xfe".to_vec();
        let args = [
            bulk(b"SET"),
            RespValue::BulkString(Some(key.clone())),
            RespValue::BulkString(Some(value.clone())),
        ];

        assert_eq!(
            Command::parse(&args),
            Ok(Command::Set {
                key,
                value,
                expire: None
            })
        );
    }

    #[test]
    fn the_integer_parser_handles_the_full_i64_range() {
        assert_eq!(super::parse_i64(b"0"), Some(0));
        assert_eq!(super::parse_i64(b"42"), Some(42));
        assert_eq!(super::parse_i64(b"-42"), Some(-42));
        assert_eq!(super::parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(super::parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(super::parse_i64(b"9223372036854775808"), None);
        assert_eq!(super::parse_i64(b"-9223372036854775809"), None);
        assert_eq!(super::parse_i64(b"-"), None);
        assert_eq!(super::parse_i64(b"--1"), None);
    }
}
