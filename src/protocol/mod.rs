//! RESP2 wire protocol: value model, parser, writer, limits, and error types.
//!
//! The module owns framing and encoding only. It has no knowledge of commands,
//! storage, networking, or timeouts.

pub mod error;
pub mod limits;
pub mod parser;
pub mod value;
pub mod writer;

pub use error::ProtocolError;
pub use limits::ProtocolLimits;
pub use parser::{ParserEvent, RespParser};
pub use value::RespValue;
