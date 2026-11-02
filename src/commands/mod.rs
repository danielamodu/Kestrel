//! Command definitions, dispatch, and error taxonomy.

pub mod command;
pub mod error;
pub mod handlers;

pub use command::Command;
pub use error::CommandError;
