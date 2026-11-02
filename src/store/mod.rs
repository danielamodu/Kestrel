//! In-memory key-value store with lazy expiry semantics.
//!
//! The store is transport-agnostic and synchronous: it owns no locks, no
//! timers, and no I/O. Command execution (`crate::commands`) supplies the
//! current `Instant` and the lock.

// `store::store` mirrors the module layout approved in docs/ARCHITECTURE.md.
#[allow(clippy::module_inception)]
pub mod store;
pub mod value;

pub use store::{Lookup, Store};
pub use value::StoredValue;
