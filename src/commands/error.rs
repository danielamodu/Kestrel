//! Command errors (recoverable): unknown command, arity, invalid expire, etc.
//!
//! These are *command* errors, not protocol errors: the caller turns them into
//! a RESP error value and keeps the connection open. Every variant has a fixed,
//! deterministic message; exact Redis wording is not required, but the mapping
//! below is documented in `docs/ARCHITECTURE.md` §5.

use std::fmt;

/// A recoverable command-layer failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    /// The command array had no elements at all (`*0\r\n`).
    EmptyCommand,
    /// The first element was not a bulk string, so it cannot name a command.
    InvalidCommandName,
    /// The name was well formed but not a supported command.
    UnknownCommand(String),
    /// The element count does not match the command's grammar.
    WrongArity { command: &'static str },
    /// An argument was a RESP2 value that is not a binary string (null, integer,
    /// array), and command arguments are never coerced.
    InvalidArgumentType,
    /// The option structure was malformed (for example `SET k v EX` without a
    /// value, or an unsupported option).
    Syntax,
    /// An integer argument was syntactically invalid or outside `i64`.
    NotAnInteger,
    /// An expiry was zero, negative, or too large to represent as a deadline.
    InvalidExpire { command: &'static str },
}

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCommand => write!(formatter, "ERR empty command"),
            Self::InvalidCommandName => write!(formatter, "ERR invalid command name"),
            Self::UnknownCommand(name) => write!(formatter, "ERR unknown command '{name}'"),
            Self::WrongArity { command } => {
                write!(
                    formatter,
                    "ERR wrong number of arguments for '{command}' command"
                )
            }
            Self::InvalidArgumentType => write!(formatter, "ERR invalid argument type"),
            Self::Syntax => write!(formatter, "ERR syntax error"),
            Self::NotAnInteger => {
                write!(formatter, "ERR value is not an integer or out of range")
            }
            Self::InvalidExpire { command } => {
                write!(formatter, "ERR invalid expire time in '{command}' command")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CommandError;

    #[test]
    fn messages_are_stable() {
        let cases = [
            (CommandError::EmptyCommand, "ERR empty command"),
            (CommandError::InvalidCommandName, "ERR invalid command name"),
            (
                CommandError::UnknownCommand("NOPE".into()),
                "ERR unknown command 'NOPE'",
            ),
            (
                CommandError::WrongArity { command: "echo" },
                "ERR wrong number of arguments for 'echo' command",
            ),
            (
                CommandError::InvalidArgumentType,
                "ERR invalid argument type",
            ),
            (CommandError::Syntax, "ERR syntax error"),
            (
                CommandError::NotAnInteger,
                "ERR value is not an integer or out of range",
            ),
            (
                CommandError::InvalidExpire { command: "set" },
                "ERR invalid expire time in 'set' command",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }
}
