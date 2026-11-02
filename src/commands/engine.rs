//! Lock-aware command executor.
//!
//! [`Engine`] is the seam the TCP server (M3) will call: it owns the shared
//! `Arc<RwLock<Store>>`, chooses the lock mode per command, and turns command
//! errors into RESP error values. Everything is synchronous; there is no I/O,
//! no `.await`, and no lock is held beyond a single command's work.
//!
//! # Lock strategy
//!
//! * `PING`, `ECHO` never touch the store.
//! * `GET`, `EXISTS` take a read lock. If they observe an expired entry they
//!   release the read lock, then take a short write lock to remove it, so reads
//!   stay read-mostly and expired entries are still "removed when encountered".
//! * `SET`, `DEL` take a write lock for the whole command.
//!
//! A poisoned lock is a fail-closed condition: every command that needs the
//! store returns an error reply and the store is never touched.

use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::protocol::RespValue;
use crate::store::{Lookup, Store};

use super::command::Command;
use super::handlers;

/// Shared store plus command dispatch.
#[derive(Debug, Clone)]
pub struct Engine {
    store: Arc<RwLock<Store>>,
}

impl Engine {
    /// Creates an engine with an empty store.
    pub fn new() -> Self {
        Self::from_shared(Arc::new(RwLock::new(Store::new())))
    }

    /// Shares an existing store (for example one configured by the server).
    pub fn from_shared(store: Arc<RwLock<Store>>) -> Self {
        Self { store }
    }

    /// The underlying shared store, for observability and server wiring.
    pub fn store(&self) -> &Arc<RwLock<Store>> {
        &self.store
    }

    /// Parses a decoded command array and executes it.
    ///
    /// Every command error (unknown command, arity, invalid option, invalid
    /// integer) becomes a RESP error value. It is never a protocol error and
    /// never a panic, so the connection stays usable.
    pub fn execute_args(&self, args: &[RespValue], now: Instant) -> RespValue {
        match Command::parse(args) {
            Ok(command) => self.execute(command, now),
            Err(error) => handlers::error(&error),
        }
    }

    /// Executes an already validated command.
    pub fn execute(&self, command: Command, now: Instant) -> RespValue {
        match command {
            Command::Ping(message) => handlers::ping(message.as_deref()),
            Command::Echo(message) => handlers::echo(&message),
            Command::Get(key) => self.get(&key, now),
            Command::Exists(keys) => self.exists(&keys, now),
            Command::Set { key, value, expire } => {
                let Ok(mut store) = self.store.write() else {
                    return poisoned();
                };
                handlers::set(&mut store, key, value, expire, now)
            }
            Command::Del(keys) => {
                let Ok(mut store) = self.store.write() else {
                    return poisoned();
                };
                handlers::del(&mut store, &keys, now)
            }
        }
    }

    /// `GET` under a read lock, purging an expired entry with a short write.
    fn get(&self, key: &[u8], now: Instant) -> RespValue {
        let (reply, expired) = {
            let Ok(store) = self.store.read() else {
                return poisoned();
            };
            let lookup = store.lookup(key, now);
            let expired = matches!(lookup, Lookup::Expired);
            (handlers::get(lookup), expired)
        };

        if expired && !self.purge(key, now) {
            return poisoned();
        }
        reply
    }

    /// `EXISTS` under a read lock, purging expired keys afterwards.
    fn exists(&self, keys: &[Vec<u8>], now: Instant) -> RespValue {
        let (reply, expired) = {
            let Ok(store) = self.store.read() else {
                return poisoned();
            };
            handlers::exists(&store, keys, now)
        };

        if expired.is_empty() {
            return reply;
        }
        let Ok(mut store) = self.store.write() else {
            return poisoned();
        };
        for key in &expired {
            // Re-checks expiry under the write lock: a concurrently refreshed
            // key is never removed.
            store.remove_if_expired(key, now);
        }
        reply
    }

    /// Removes `key` if it is still expired. Returns `false` on poison.
    fn purge(&self, key: &[u8], now: Instant) -> bool {
        match self.store.write() {
            Ok(mut store) => {
                store.remove_if_expired(key, now);
                true
            }
            Err(_) => false,
        }
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

/// Fail-closed reply for a poisoned store lock.
fn poisoned() -> RespValue {
    RespValue::Error("ERR store unavailable (lock poisoned)".into())
}

#[cfg(test)]
mod tests {
    use super::Engine;
    use crate::protocol::RespValue;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    fn args(parts: &[&[u8]]) -> Vec<RespValue> {
        parts
            .iter()
            .map(|part| RespValue::BulkString(Some(part.to_vec())))
            .collect()
    }

    fn len(engine: &Engine) -> usize {
        match engine.store().read() {
            Ok(store) => store.len(),
            Err(_) => panic!("store lock poisoned"),
        }
    }

    #[test]
    fn executes_commands_end_to_end() {
        let now = Instant::now();
        let engine = Engine::new();

        assert_eq!(
            engine.execute_args(&args(&[b"PING"]), now),
            RespValue::SimpleString("PONG".into())
        );
        assert_eq!(
            engine.execute_args(&args(&[b"ECHO", b"hi"]), now),
            RespValue::BulkString(Some(b"hi".to_vec()))
        );
        assert_eq!(
            engine.execute_args(&args(&[b"SET", b"k", b"v"]), now),
            RespValue::SimpleString("OK".into())
        );
        assert_eq!(
            engine.execute_args(&args(&[b"GET", b"k"]), now),
            RespValue::BulkString(Some(b"v".to_vec()))
        );
        assert_eq!(
            engine.execute_args(&args(&[b"EXISTS", b"k"]), now),
            RespValue::Integer(1)
        );
        assert_eq!(
            engine.execute_args(&args(&[b"DEL", b"k"]), now),
            RespValue::Integer(1)
        );
        assert_eq!(
            engine.execute_args(&args(&[b"GET", b"k"]), now),
            RespValue::BulkString(None)
        );
    }

    #[test]
    fn get_purges_an_expired_entry() {
        let now = Instant::now();
        let engine = Engine::new();
        engine.execute_args(&args(&[b"SET", b"k", b"v", b"PX", b"10"]), now);
        assert_eq!(len(&engine), 1);

        let reply = engine.execute_args(&args(&[b"GET", b"k"]), now + Duration::from_millis(10));

        assert_eq!(reply, RespValue::BulkString(None));
        assert_eq!(len(&engine), 0, "GET must remove the expired entry");
    }

    #[test]
    fn exists_purges_expired_entries() {
        let now = Instant::now();
        let engine = Engine::new();
        engine.execute_args(&args(&[b"SET", b"a", b"1"]), now);
        engine.execute_args(&args(&[b"SET", b"b", b"2", b"PX", b"10"]), now);
        engine.execute_args(&args(&[b"SET", b"c", b"3", b"PX", b"10"]), now);

        let reply = engine.execute_args(
            &args(&[b"EXISTS", b"a", b"b", b"c"]),
            now + Duration::from_millis(10),
        );

        assert_eq!(reply, RespValue::Integer(1));
        assert_eq!(len(&engine), 1, "EXISTS must purge expired entries");
    }

    #[test]
    fn del_removes_expired_entries_without_counting_them() {
        let now = Instant::now();
        let engine = Engine::new();
        engine.execute_args(&args(&[b"SET", b"a", b"1"]), now);
        engine.execute_args(&args(&[b"SET", b"b", b"2", b"PX", b"10"]), now);

        let reply = engine.execute_args(
            &args(&[b"DEL", b"a", b"b"]),
            now + Duration::from_millis(10),
        );

        assert_eq!(reply, RespValue::Integer(1));
        assert_eq!(len(&engine), 0);
    }

    #[test]
    fn command_errors_are_resp_errors_and_never_panic() {
        let now = Instant::now();
        let engine = Engine::new();
        engine.execute_args(&args(&[b"SET", b"keep", b"value"]), now);

        let malformed: Vec<Vec<RespValue>> = vec![
            Vec::new(),
            args(&[b"NOPE"]),
            args(&[b"GET"]),
            args(&[b"GET", b"a", b"b"]),
            args(&[b"SET", b"k", b"v", b"NX"]),
            args(&[b"SET", b"k", b"v", b"PX", b"0"]),
            args(&[b"SET", b"k", b"v", b"PX", b"abc"]),
            vec![RespValue::BulkString(None)],
            vec![RespValue::Integer(1)],
            vec![RespValue::array(vec![RespValue::bulk("GET")])],
            vec![
                RespValue::bulk("GET"),
                RespValue::Array(Some(vec![RespValue::bulk("k")])),
            ],
        ];

        for command in malformed {
            let reply = engine.execute_args(&command, now);
            assert!(
                matches!(reply, RespValue::Error(_)),
                "expected an error reply for {command:?}, got {reply:?}"
            );
        }

        // Unrelated state is untouched by rejected commands.
        assert_eq!(
            engine.execute_args(&args(&[b"GET", b"keep"]), now),
            RespValue::BulkString(Some(b"value".to_vec()))
        );
        assert_eq!(len(&engine), 1);
    }

    #[test]
    fn a_poisoned_lock_fails_closed() {
        let engine = Engine::new();
        let shared = engine.store().clone();

        let poisoner = std::thread::spawn(move || {
            let _guard = shared.write().expect("write lock");
            panic!("poison the store");
        });
        assert!(poisoner.join().is_err(), "poisoner must panic");

        let now = Instant::now();
        let store_commands: [&[&[u8]]; 4] = [
            &[b"GET", b"k"],
            &[b"SET", b"k", b"v"],
            &[b"DEL", b"k"],
            &[b"EXISTS", b"k"],
        ];

        for command in store_commands {
            match engine.execute_args(&args(command), now) {
                RespValue::Error(message) => {
                    assert_eq!(message, "ERR store unavailable (lock poisoned)");
                }
                other => panic!("expected fail-closed error for {command:?}, got {other:?}"),
            }
        }

        // Commands that never touch stored state keep working.
        assert_eq!(
            engine.execute_args(&args(&[b"PING"]), now),
            RespValue::SimpleString("PONG".into())
        );
    }

    #[test]
    fn concurrent_reads_and_writes_complete_without_deadlock() {
        const WORKERS: usize = 8;
        const OPERATIONS: usize = 200;

        let engine = Engine::new();
        let (done_tx, done_rx) = mpsc::channel();
        let mut handles = Vec::new();

        for worker in 0..WORKERS {
            let engine = engine.clone();
            let done_tx = done_tx.clone();

            handles.push(std::thread::spawn(move || {
                let now = Instant::now();

                for operation in 0..OPERATIONS {
                    let key = format!("k{}", (worker + operation) % 16);
                    let command: Vec<RespValue> = match operation % 5 {
                        0 => args(&[b"SET", key.as_bytes(), b"v"]),
                        1 => args(&[b"GET", key.as_bytes()]),
                        2 => args(&[b"EXISTS", key.as_bytes()]),
                        3 => args(&[b"DEL", key.as_bytes()]),
                        _ => args(&[b"PING"]),
                    };
                    let _ = engine.execute_args(&command, now);
                }

                done_tx.send(()).expect("report completion");
            }));
        }
        drop(done_tx);

        for _ in 0..WORKERS {
            done_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("worker did not finish: deadlock?");
        }
        for handle in handles {
            handle.join().expect("worker panicked");
        }
    }

    #[test]
    fn concurrent_expiry_and_lookup_are_consistent() {
        const KEYS: usize = 64;
        const READERS: usize = 8;

        let engine = Engine::new();
        let base = Instant::now();

        for index in 0..KEYS {
            let key = format!("k{index}");
            engine.execute_args(&args(&[b"SET", key.as_bytes(), b"v", b"PX", b"1000"]), base);
        }
        assert_eq!(len(&engine), KEYS);

        let expired_at = base + Duration::from_secs(2);
        let (done_tx, done_rx) = mpsc::channel();
        let mut handles = Vec::new();

        for _ in 0..READERS {
            let engine = engine.clone();
            let done_tx = done_tx.clone();

            handles.push(std::thread::spawn(move || {
                for index in 0..KEYS {
                    let key = format!("k{index}");
                    assert_eq!(
                        engine.execute_args(&args(&[b"GET", key.as_bytes()]), expired_at),
                        RespValue::BulkString(None)
                    );
                    assert_eq!(
                        engine.execute_args(&args(&[b"EXISTS", key.as_bytes()]), expired_at),
                        RespValue::Integer(0)
                    );
                }
                done_tx.send(()).expect("report completion");
            }));
        }
        drop(done_tx);

        for _ in 0..READERS {
            done_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("reader did not finish: deadlock?");
        }
        for handle in handles {
            handle.join().expect("reader panicked");
        }

        assert_eq!(len(&engine), 0, "every expired key must be purged");
    }

    #[test]
    fn engines_sharing_one_store_observe_the_same_data() {
        let now = Instant::now();
        let writer = Engine::new();
        let reader = Engine::from_shared(writer.store().clone());

        writer.execute_args(&args(&[b"SET", b"shared", b"value"]), now);

        assert_eq!(
            reader.execute_args(&args(&[b"GET", b"shared"]), now),
            RespValue::BulkString(Some(b"value".to_vec()))
        );
    }
}
