//! Kestrel — a Redis-compatible in-memory data store.
//!
//! Module boundaries: `protocol` (wire format), `store` (state), `commands`
//! (semantics), `server` (I/O). See `docs/ARCHITECTURE.md` for the design.

pub mod commands;
pub mod protocol;
pub mod server;
pub mod store;
