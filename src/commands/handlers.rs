//! Command semantics against a plain [`Store`].
//!
//! Handlers take no locks and never read the clock: the store and the current
//! `Instant` are inputs. [`crate::commands::Engine`] supplies both under the
//! lock mode matching the command, which keeps these functions deterministic
//! and directly testable.

use std::time::{Duration, Instant};

use crate::protocol::RespValue;
use crate::store::{Lookup, Store};

use super::error::CommandError;

/// Builds the RESP error reply for a command error.
pub fn error(error: &CommandError) -> RespValue {
    RespValue::Error(error.to_string())
}

/// `PING` / `PING <message>`.
pub fn ping(message: Option<&[u8]>) -> RespValue {
    match message {
        None => RespValue::SimpleString("PONG".into()),
        Some(payload) => RespValue::BulkString(Some(payload.to_vec())),
    }
}

/// `ECHO <message>`.
pub fn echo(message: &[u8]) -> RespValue {
    RespValue::BulkString(Some(message.to_vec()))
}

/// Reply for a `GET` lookup: the value, or a null bulk string when the key is
/// absent or expired.
pub fn get(lookup: Lookup<'_>) -> RespValue {
    match lookup {
        Lookup::Present(value) => RespValue::BulkString(Some(value.to_vec())),
        Lookup::Absent | Lookup::Expired => RespValue::BulkString(None),
    }
}

/// `SET <key> <value> [...]`.
///
/// The deadline is `now + expire`. If that addition overflows the `Instant`
/// range the command is rejected and nothing is stored, so an extreme TTL can
/// never wrap into "already expired" or panic.
pub fn set(
    store: &mut Store,
    key: Vec<u8>,
    value: Vec<u8>,
    expire: Option<Duration>,
    now: Instant,
) -> RespValue {
    let expires_at = match expire {
        None => None,
        Some(duration) => match now.checked_add(duration) {
            Some(deadline) => Some(deadline),
            None => return error(&CommandError::InvalidExpire { command: "set" }),
        },
    };

    store.set(key, value, expires_at);
    RespValue::SimpleString("OK".into())
}

/// `DEL <key>...`.
pub fn del(store: &mut Store, keys: &[Vec<u8>], now: Instant) -> RespValue {
    RespValue::Integer(store.del(keys, now) as i64)
}

/// `EXISTS <key>...`.
///
/// Returns the reply and the keys found expired, so a caller that only holds a
/// read lock can schedule their removal. The count itself never includes them.
pub fn exists(store: &Store, keys: &[Vec<u8>], now: Instant) -> (RespValue, Vec<Vec<u8>>) {
    let mut present = 0usize;
    let mut expired = Vec::new();

    for key in keys {
        match store.lookup(key, now) {
            Lookup::Present(_) => present += 1,
            Lookup::Expired => expired.push(key.clone()),
            Lookup::Absent => {}
        }
    }

    (RespValue::Integer(present as i64), expired)
}

#[cfg(test)]
mod tests {
    use super::{del, echo, error, exists, get, ping, set};
    use crate::commands::error::CommandError;
    use crate::protocol::RespValue;
    use crate::store::{Lookup, Store};
    use std::time::{Duration, Instant};

    fn key(bytes: &[u8]) -> Vec<u8> {
        bytes.to_vec()
    }

    #[test]
    fn ping_and_echo_are_byte_exact() {
        assert_eq!(ping(None), RespValue::SimpleString("PONG".into()));
        assert_eq!(
            ping(Some(b"hello")),
            RespValue::BulkString(Some(b"hello".to_vec()))
        );
        assert_eq!(
            echo(b"a\r\n\0b"),
            RespValue::BulkString(Some(b"a\r\n\0b".to_vec()))
        );
    }

    #[test]
    fn get_maps_every_lookup_state() {
        assert_eq!(
            get(Lookup::Present(b"v")),
            RespValue::BulkString(Some(b"v".to_vec()))
        );
        assert_eq!(get(Lookup::Absent), RespValue::BulkString(None));
        assert_eq!(get(Lookup::Expired), RespValue::BulkString(None));
    }

    #[test]
    fn set_stores_binary_values_and_returns_ok() {
        let now = Instant::now();
        let mut store = Store::new();
        let k = b"k\r\n\0".to_vec();
        let v = b"v\r\n\0\xff".to_vec();

        assert_eq!(
            set(&mut store, k.clone(), v.clone(), None, now),
            RespValue::SimpleString("OK".into())
        );
        assert_eq!(store.lookup(&k, now), Lookup::Present(v.as_slice()));
    }

    #[test]
    fn set_with_expire_is_visible_before_the_deadline_and_gone_after() {
        let now = Instant::now();
        let mut store = Store::new();
        set(
            &mut store,
            key(b"k"),
            b"v".to_vec(),
            Some(Duration::from_millis(100)),
            now,
        );

        assert_eq!(
            store.lookup(b"k", now + Duration::from_millis(99)),
            Lookup::Present(b"v")
        );
        assert_eq!(
            store.lookup(b"k", now + Duration::from_millis(100)),
            Lookup::Expired
        );
    }

    #[test]
    fn a_zero_duration_expire_is_immediately_unavailable() {
        let now = Instant::now();
        let mut store = Store::new();
        set(
            &mut store,
            key(b"k"),
            b"v".to_vec(),
            Some(Duration::ZERO),
            now,
        );

        assert_eq!(store.lookup(b"k", now), Lookup::Expired);
        assert_eq!(store.get(b"k", now), None);
        assert!(store.is_empty());
    }

    #[test]
    fn an_expire_that_overflows_the_instant_range_is_rejected() {
        let now = Instant::now();
        let mut store = Store::new();

        let reply = set(
            &mut store,
            key(b"k"),
            b"v".to_vec(),
            Some(Duration::from_secs(u64::MAX)),
            now,
        );

        assert_eq!(
            reply,
            RespValue::Error(CommandError::InvalidExpire { command: "set" }.to_string())
        );
        assert!(store.is_empty(), "a rejected SET must not store anything");
    }

    #[test]
    fn del_counts_only_live_keys() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"a"), b"1".to_vec(), None);
        store.set(key(b"b"), b"2".to_vec(), Some(now));

        assert_eq!(
            del(&mut store, &[key(b"a"), key(b"b"), key(b"c")], now),
            RespValue::Integer(1)
        );
        assert!(store.is_empty());
    }

    #[test]
    fn exists_counts_live_keys_and_reports_expired_ones() {
        let now = Instant::now();
        let mut store = Store::new();
        store.set(key(b"a"), b"1".to_vec(), None);
        store.set(key(b"b"), b"2".to_vec(), Some(now));

        let (reply, expired) = exists(&store, &[key(b"a"), key(b"b"), key(b"c")], now);

        assert_eq!(reply, RespValue::Integer(1));
        assert_eq!(expired, vec![key(b"b")]);
        // The handler is a pure read: removal is the caller's job.
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn errors_render_as_resp_errors() {
        assert_eq!(
            error(&CommandError::Syntax),
            RespValue::Error("ERR syntax error".into())
        );
    }
}
