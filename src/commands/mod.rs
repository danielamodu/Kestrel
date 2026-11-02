//! Command definitions, dispatch, and error taxonomy.
//!
//! Layering: [`command`] validates arguments and produces a [`Command`];
//! [`handlers`] implements each command against a plain [`crate::store::Store`];
//! [`engine`] applies the locking strategy and is the seam the server will call.

pub mod command;
pub mod engine;
pub mod error;
pub mod handlers;

pub use command::Command;
pub use engine::Engine;
pub use error::CommandError;
