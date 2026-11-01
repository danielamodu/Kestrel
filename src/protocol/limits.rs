//! Configurable protocol limits.
//!
//! Every limit is enforced *before* the parser allocates the corresponding
//! storage, so malformed or adversarial input is rejected without unbounded
//! allocation.

/// Hard bounds applied while parsing (and while growing the parser buffer).
///
/// The defaults match `docs/ARCHITECTURE.md` §4 (Redis `proto-max-bulk-len`
/// and `proto-max-array-size` defaults) plus one implementation-required
/// addition: `max_nesting_depth`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolLimits {
    /// Maximum length of a single bulk-string payload, in bytes.
    pub max_bulk_len: u64,
    /// Maximum number of elements in a single array.
    pub max_array_len: u64,
    /// Maximum encoded size of one top-level request, in bytes. Also caps how
    /// far the parser's retained (incomplete-request) buffer may grow.
    pub max_request_size: u64,
    /// Maximum array nesting depth.
    ///
    /// Added beyond the architecture table: parsing is recursive, so an
    /// unbounded depth is a stack-exhaustion risk even when the total request
    /// size is small (`*1\r\n` repeated). 128 levels is far beyond anything a
    /// command uses.
    pub max_nesting_depth: usize,
}

impl ProtocolLimits {
    /// A `ProtocolLimits` with the documented defaults.
    pub const DEFAULT: ProtocolLimits = ProtocolLimits {
        max_bulk_len: 512 * 1024 * 1024,
        max_array_len: 1024,
        max_request_size: 512 * 1024 * 1024,
        max_nesting_depth: 128,
    };
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::ProtocolLimits;

    #[test]
    fn defaults_match_the_architecture() {
        let limits = ProtocolLimits::default();
        assert_eq!(limits.max_bulk_len, 512 * 1024 * 1024);
        assert_eq!(limits.max_array_len, 1024);
        assert_eq!(limits.max_request_size, 512 * 1024 * 1024);
        assert_eq!(limits.max_nesting_depth, 128);
    }
}
