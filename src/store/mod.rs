//! In-memory key-value store with lazy expiry semantics.

// `store::store` mirrors the module layout approved in docs/ARCHITECTURE.md.
#[allow(clippy::module_inception)]
pub mod store;
pub mod value;
