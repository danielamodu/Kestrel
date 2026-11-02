//! Byte-keyed map with optional absolute expirations and lazy removal.
//!
//! The store is a plain, synchronous data structure: it owns no locks, performs
//! no I/O, and never consults the clock. Every operation takes the current
//! `Instant`, so expiry is fully deterministic in tests.

use std::collections::HashMap;
use std::time::Instant;

use super::value::StoredValue;

/// Result of a non-mutating read.
///
/// Distinguishing `Expired` from `Absent` lets a caller holding a read lock know
/// that a removal is due without upgrading the read to a write itself (see
/// [`crate::commands::Engine`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup<'a> {
    /// The key exists and is live.
    Present(&'a [u8]),
    /// The key has never been set (or was removed).
    Absent,
    /// The key exists but its deadline has passed; it reads as absent and is
    /// due for removal.
    Expired,
}

/// In-memory key-value store.
///
/// Keys and values are raw bytes; there are no encoding assumptions.
#[derive(Debug, Default)]
pub struct Store {
    entries: HashMap<Vec<u8>, StoredValue>,
}

impl Store {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries currently retained.
    ///
    /// Expired entries that have not yet been observed are still counted; this
    /// is a diagnostic, not a visibility rule.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` when no entries are retained.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Inserts or overwrites `key`.
    ///
    /// Any previous value *and* deadline are replaced, so a plain `SET` clears
    /// a TTL set earlier (Redis `SET` semantics without `KEEPTTL`).
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<Instant>) {
        self.entries
            .insert(key, StoredValue::new(value, expires_at));
    }

    /// Reads a key without mutating the store.
    ///
    /// Expired entries are reported as [`Lookup::Expired`], never as
    /// [`Lookup::Present`].
    pub fn lookup(&self, key: &[u8], now: Instant) -> Lookup<'_> {
        match self.entries.get(key) {
            None => Lookup::Absent,
            Some(entry) if entry.is_expired(now) => Lookup::Expired,
            Some(entry) => Lookup::Present(entry.value()),
        }
    }

    /// Reads a live value, removing the entry if it has expired.
    ///
    /// Returns an owned copy so the caller does not hold a borrow across other
    /// store operations (for example, the next command under the same lock).
    pub fn get(&mut self, key: &[u8], now: Instant) -> Option<Vec<u8>> {
        if let Some(entry) = self.entries.get(key) {
            if entry.is_expired(now) {
                self.entries.remove(key);
                return None;
            }
            return Some(entry.value().to_vec());
        }
        None
    }

    /// Removes `key` only if it is currently expired.
    ///
    /// This is the write half of the two-phase read path: a reader that saw
    /// [`Lookup::Expired`] asks the store to drop the entry, and the expiry is
    /// re-checked here under the write lock so a concurrently refreshed key is
    /// never deleted.
    pub fn remove_if_expired(&mut self, key: &[u8], now: Instant) -> bool {
        let expired = self
            .entries
            .get(key)
            .is_some_and(|entry| entry.is_expired(now));

        if expired {
            self.entries.remove(key);
        }
        expired
    }

    /// Removes every listed key that currently exists, live or expired.
    ///
    /// Returns the number of *live* keys removed. Expired keys are purged but
    /// not counted, matching "expired keys behave as absent". Duplicate keys
    /// count once because the second removal finds nothing.
    pub fn del(&mut self, keys: &[Vec<u8>], now: Instant) -> usize {
        let mut removed = 0usize;

        for key in keys {
            let expired = self.entries.get(key).map(|entry| entry.is_expired(now));

            match expired {
                Some(true) => {
                    self.entries.remove(key);
                }
                Some(false) => {
                    self.entries.remove(key);
                    removed += 1;
                }
                None => {}
            }
        }

        removed
    }

    /// Counts listed keys that are live.
    ///
    /// Duplicates are counted once per occurrence, matching Redis
    /// (`EXISTS k k` returns `2` when `k` is live).
    pub fn exists(&self, keys: &[Vec<u8>], now: Instant) -> usize {
        keys.iter()
            .filter(|key| matches!(self.lookup(key, now), Lookup::Present(_)))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::{Lookup, Store};
    use std::time::{Duration, Instant};

    fn key(bytes: &[u8]) -> Vec<u8> {
        bytes.to_vec()
    }

    #[test]
    fn insert_lookup_and_overwrite() {
        let now = Instant::now();
        let mut store = Store::new();

        assert_eq!(store.lookup(b"k", now), Lookup::Absent);
        store.set(key(b"k"), b"first".to_vec(), None);
        assert_eq!(store.lookup(b"k", now), Lookup::Present(b"first"));
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        store.set(key(b"k"), b"second".to_vec(), None);
        assert_eq!(store.lookup(b"k", now), Lookup::Present(b"second"));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn binary_keys_and_values_round_trip() {
        let now = Instant::now();
        let mut store = Store::new();
        let binary_key = b"k\r\n\0\xff".to_vec();
        let binary_value = b"v\r\n\0\xfe\x01".to_vec();

        store.set(binary_key.clone(), binary_value.clone(), None);

        assert_eq!(
            store.lookup(&binary_key, now),
            Lookup::Present(binary_value.as_slice())
        );
        assert_eq!(store.get(&binary_key, now), Some(binary_value));
        assert_eq!(store.lookup(b"k", now), Lookup::Absent);
    }

    #[test]
    fn get_removes_expired_entries() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"k"), b"v".to_vec(), Some(now));

        assert_eq!(store.lookup(b"k", now), Lookup::Expired);
        assert_eq!(store.get(b"k", now), None);
        assert!(store.is_empty(), "expired entry must be removed on access");
    }

    #[test]
    fn get_returns_live_values_and_keeps_entries() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"k"), b"v".to_vec(), Some(now + Duration::from_secs(1)));

        assert_eq!(store.get(b"k", now), Some(b"v".to_vec()));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn remove_if_expired_only_removes_expired_entries() {
        let now = Instant::now();
        let mut store = Store::new();

        store.set(
            key(b"live"),
            b"v".to_vec(),
            Some(now + Duration::from_secs(1)),
        );
        assert!(!store.remove_if_expired(b"live", now));
        assert_eq!(store.len(), 1);

        store.set(key(b"dead"), b"v".to_vec(), Some(now));
        assert!(store.remove_if_expired(b"dead", now));
        assert_eq!(store.len(), 1);

        assert!(!store.remove_if_expired(b"missing", now));
    }

    #[test]
    fn del_counts_live_keys_only() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"a"), b"1".to_vec(), None);
        store.set(key(b"b"), b"2".to_vec(), Some(now));
        store.set(key(b"c"), b"3".to_vec(), Some(now + Duration::from_secs(1)));

        // `b` is expired: removed but not counted.
        assert_eq!(store.del(&[key(b"a"), key(b"b"), key(b"missing")], now), 1);
        assert_eq!(store.len(), 1);
        assert_eq!(store.lookup(b"a", now), Lookup::Absent);
        assert_eq!(store.lookup(b"b", now), Lookup::Absent);
        assert_eq!(store.lookup(b"c", now), Lookup::Present(b"3"));
    }

    #[test]
    fn del_does_not_double_count_duplicate_keys() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"k"), b"v".to_vec(), None);

        assert_eq!(store.del(&[key(b"k"), key(b"k")], now), 1);
        assert!(store.is_empty());
    }

    #[test]
    fn exists_counts_live_keys_and_duplicates() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"a"), b"1".to_vec(), None);
        store.set(key(b"b"), b"2".to_vec(), Some(now));

        assert_eq!(
            store.exists(&[key(b"a"), key(b"b"), key(b"missing")], now),
            1
        );
        assert_eq!(store.exists(&[key(b"a"), key(b"a")], now), 2);

        // `exists` is a pure read: the expired entry is still retained.
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn entries_can_have_independent_deadlines() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(
            key(b"short"),
            b"1".to_vec(),
            Some(now + Duration::from_millis(10)),
        );
        store.set(
            key(b"long"),
            b"2".to_vec(),
            Some(now + Duration::from_millis(20)),
        );
        store.set(key(b"forever"), b"3".to_vec(), None);

        let after_short = now + Duration::from_millis(15);
        assert_eq!(store.lookup(b"short", after_short), Lookup::Expired);
        assert_eq!(store.lookup(b"long", after_short), Lookup::Present(b"2"));
        assert_eq!(store.lookup(b"forever", after_short), Lookup::Present(b"3"));

        let after_long = now + Duration::from_millis(20);
        assert_eq!(store.lookup(b"long", after_long), Lookup::Expired);
        assert_eq!(store.lookup(b"forever", after_long), Lookup::Present(b"3"));
    }

    #[test]
    fn a_zero_duration_deadline_is_immediately_unavailable() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"k"), b"v".to_vec(), Some(now));

        assert_eq!(store.lookup(b"k", now), Lookup::Expired);
        assert_eq!(store.get(b"k", now), None);
        assert!(store.is_empty());
    }

    #[test]
    fn setting_without_a_deadline_clears_a_previous_one() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"k"), b"v1".to_vec(), Some(now));

        store.set(key(b"k"), b"v2".to_vec(), None);
        assert_eq!(
            store.lookup(b"k", now + Duration::from_secs(3600)),
            Lookup::Present(b"v2")
        );
    }
}
