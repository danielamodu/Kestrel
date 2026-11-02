//! Stored entries: a byte payload plus an optional absolute expiry deadline.
//!
//! Deadlines are absolute [`Instant`]s supplied by the caller (see
//! `docs/ARCHITECTURE.md` §5). The store never reads the clock itself, which
//! keeps expiry deterministic in tests and keeps this module free of async or
//! timer machinery.

use std::time::Instant;

/// One key's payload and optional deadline.
///
/// Both the key and the payload are raw bytes; nothing here assumes UTF-8.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredValue {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl StoredValue {
    /// Builds an entry with an optional absolute deadline.
    pub fn new(value: Vec<u8>, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }

    /// The stored payload.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// The absolute deadline, if this entry expires.
    pub fn expires_at(&self) -> Option<Instant> {
        self.expires_at
    }

    /// Returns `true` once `now` has reached the deadline.
    ///
    /// The deadline is exclusive: the value is live while `now < expires_at`
    /// and expired at `now == expires_at` (and later).
    pub fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|deadline| now >= deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::StoredValue;
    use std::time::{Duration, Instant};

    #[test]
    fn entries_without_a_deadline_never_expire() {
        let entry = StoredValue::new(b"v".to_vec(), None);
        assert!(!entry.is_expired(Instant::now()));
        assert_eq!(entry.expires_at(), None);
    }

    #[test]
    fn the_deadline_is_exclusive() {
        let now = Instant::now();
        let deadline = now + Duration::from_millis(10);
        let entry = StoredValue::new(b"v".to_vec(), Some(deadline));

        assert!(!entry.is_expired(now));
        assert!(!entry.is_expired(deadline - Duration::from_nanos(1)));
        assert!(entry.is_expired(deadline));
        assert!(entry.is_expired(deadline + Duration::from_secs(1)));
    }

    #[test]
    fn payload_is_byte_exact() {
        let payload = b"a\r\n\0b".to_vec();
        let entry = StoredValue::new(payload.clone(), None);
        assert_eq!(entry.value(), payload.as_slice());
    }
}
