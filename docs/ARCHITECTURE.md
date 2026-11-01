# Architecture Proposal — Redis-Compatible In-Memory Data Store

- **Status:** Revision 2 (final approval pending) — proposal (pre-implementation). No implementation code has been written yet.
- **Date:** 2026-11-01
- **Repo:** `C:\Users\USER\Desktop\Kestrel` (currently empty — greenfield)
- **Audience:** Project owner + AI-assisted development loop

---

## 0. Repository & Environment Inspection

| Finding | Detail |
|---|---|
| Repo contents | None (0 entries, including hidden) |
| VCS | None (not a git repo) |
| Rust toolchain | **Not installed** (`rustc`/`cargo`/`rustup` missing from PATH; no `~/.cargo`, no `~/.rustup`) |
| Existing manifests/sources | None |
| Shell | PowerShell |

**Implication:** Milestone 0 begins with installing the Rust toolchain via rustup (stable channel), then `cargo init`. The design below assumes a standard Cargo layout and does not presuppose any existing code to preserve.

---

## 1. Scope

### In scope (v1)

- TCP server (`127.0.0.1:6379` by default, configurable bind address).
- RESP2 protocol only.
- Commands: `PING`, `ECHO`, `GET`, `SET`, `DEL`, `EXISTS`, plus `SET key value EX|PX ttl` expiry options.
- Multiple concurrent clients.
- In-memory key-value storage with lazy expiration.
- Graceful handling of malformed and incomplete requests.

### Explicitly out of scope (v1 — decisions to be made before inclusion)

Persistence (RDB/AOF), eviction / `maxmemory` policies, `AUTH`, `SELECT` / multiple logical DBs, RESP3, TLS, Unix domain socket, `INCR`/`DECR`/`TTL`/`SCAN`/`FLUSHDB`, `MULTI`/`EXEC`, replication, clustering, protocol compression.

---

## 2. Modular Architecture

**Recommendation:** single crate, module-per-concern, with a `lib` + thin `bin` split (small upfront cost, large testability win).

```
src/
  main.rs            # composition root: runtime wiring, server construction, run()
  lib.rs             # crate root; re-exports public modules
  protocol/
    mod.rs
    value.rs         # RespValue enum + encodable/decodable types
    parser.rs        # incremental RESP2 parser
    writer.rs        # RESP2 serializer
    error.rs         # ProtocolError
  store/
    mod.rs
    store.rs         # Store: HashMap + expiry
    value.rs         # StoredValue (value + expires_at)
  commands/
    mod.rs
    command.rs       # Command enum (interpreted from protocol args)
    handlers.rs      # dispatch + per-command handlers
    error.rs         # CommandError (recoverable) taxonomy
  server/
    mod.rs           # Server: listener + accept loop + lifecycle
    connection.rs    # per-connection task: read → parse → execute → write → close
```

### Module responsibilities & interfaces

| Module | Responsibility | Public design surface | Does not own |
|---|---|---|---|
| `protocol` | Wire framing & encoding only | See §3 | Business semantics, storage |
| `store` | State + expiry | See §5 | Network, protocol, commands |
| `commands` | Interpretation + execution | See §5 | Wire encoding (consumes/produces `RespValue`) |
| `server` | I/O lifecycle | `Server::{bind, run}` | Parsing, semantics, storage internals |

#### `protocol` module (Revision 1)

```rust
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>), // None = nil
    Array(Vec<RespValue>),
}

pub enum ParserEvent {
    /// Batch of complete commands parsed from the fed bytes, in order.
    /// Produced only when at least one complete command was parsed.
    Commands(Vec<Vec<RespValue>>),
    /// Buffer ends mid-command; more bytes are required.
    Incomplete,
    /// Fatal framing error.
    ProtocolError(ProtocolError),
}

impl RespParser {
    pub fn new() -> Self;
    /// Feed bytes; returns events. Complete commands are batched into
    /// `Commands`; any trailing partial command is retained in the parser
    /// buffer (unconsumed bytes preserved).
    /// If the fed bytes contain valid complete commands followed by malformed
    /// data, the valid batch is emitted and the fatal error is retained until
    /// drained via `take_error`.
    pub fn feed(&mut self, data: &[u8]) -> ParserEvent;
    /// Drain a fatal error recorded after a valid batch was emitted.
    pub fn take_error(&mut self) -> Option<ProtocolError>;
}
```

#### `store` module (Revision 1)

```rust
pub struct StoredValue { pub value: Vec<u8>, pub expires_at: Option<Instant> }
pub struct Store { entries: HashMap<Vec<u8>, StoredValue> }
impl Store {
    pub fn get(&self, key: &[u8], now: Instant) -> Option<Vec<u8>>;
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<Instant>);
    pub fn del(&mut self, keys: &[Vec<u8>]) -> usize;
    pub fn exists(&self, keys: &[Vec<u8>]) -> usize;
}
```

#### `commands` module (Revision 1)

```rust
pub enum Command {
    Ping,
    Echo(Vec<u8>),
    Get(Vec<u8>),
    Set { key: Vec<u8>, value: Vec<u8>, expire_ms: Option<u64> },
    Del(Vec<Vec<u8>>),
    Exists(Vec<Vec<u8>>),
}
pub fn execute(cmd: Command, store: &mut Store, now: Instant) -> Result<RespValue, CommandError>;
```

---

## 3. Networking, RESP2 Protocol & Command Parsing

### Transport & parser event flow

- Tokio multi-thread runtime; listener accept loop; per-connection task via `tokio::spawn`.
- Per-connection task loop:
  - `read` chunk → `parser.feed(chunk)`.
  - On `Commands(batch)`: process **all** commands in the batch (drains every complete command available in the read), then continue reading.
  - On `Incomplete`: arm the incomplete-request timeout, await more data.
  - On `ProtocolError`: reply error, close connection.
- Because `feed` retains any unconsumed partial-command bytes in its internal buffer, a chunk containing `CMD1\r\n` + half of `CMD2\r\n` yields one `Commands([cmd1])` event followed by `Incomplete` on subsequent feeds; bytes are preserved across feeds.

### Incomplete-request timeout semantics (Final approval)

- The timeout is an **absolute deadline**, captured when the parser first transitions to the `Incomplete` state for a request.
- Incoming fragments do **not** reset the deadline; the deadline is fixed at first detection and remains in effect until cleared.
- The deadline is cleared once a complete command is emitted (`Commands` event).
- The connection is closed if the current time reaches the deadline.

### Malformed input following valid commands (Final approval)

- Parser scans fed bytes left-to-right.
- Valid complete commands are emitted as a batch via `Commands`.
- If malformed data follows one or more valid commands within the same read, the valid batch is emitted and the fatal error is retained (drainable via `take_error`).
- The connection processes the batch in order, then observes the recorded error and closes. No valid command is silently discarded.

### RESP2 encoding rules (v1)

- Requests: arrays only (`*<n>\r\n`). RESP2 inline commands deliberately excluded (documented simplification; RESP3 later may add).
- Elements: simple string, bulk string, integer. Args normalized to bytes (`Vec<u8>`).
- Command arrays carry the command name as the first element, followed by arguments.
- Replies:
  - `PING` → `+PONG\r\n` (simple status)
  - `ECHO <s>` → bulk string of arg; missing arg → `-ERR wrong number of arguments for 'echo' command`
  - `GET <k>` → bulk string value, or `$-1\r\n` (nil) if missing/expired
  - `SET <k> <v> [EX <s> | PX <ms>]` → `+OK\r\n` / `-ERR value is not an integer or out of range` / `-ERR invalid expire time in 'set' command`
  - `DEL <k>...` → `:N\r\n` (integer deleted count)
  - `EXISTS <k>...` → `:N\r\n` (integer existing count)

### Command parsing semantics (Revision 1)

- Command names are matched **ASCII case-insensitively** (canonicalized to uppercase) before dispatch.
- Unknown commands → `-ERR unknown command '<name>'`.
- Arity violations → `-ERR wrong number of arguments for '<cmd>' command`.
- Expiration options (`SET key value EX <sec>` / `SET key value PX <ms>`): numeric arguments validated as positive integers within range; failures yield `-ERR value is not an integer or out of range` or `-ERR invalid expire time in 'set' command`.
- Invalid numeric values produce error replies; the implementation never panics on malformed input.

### Error taxonomy (settled design decision)

- **Protocol errors** (framing, limits, malformed structure): reply with error, then close connection. Deterministic, testable.
- **Command errors** (unknown command, arity, invalid expire): reply `-ERR ...`, keep connection open.

---

## 4. Protocol Limits & Parser Buffer (Revision 1)

Configurable limits (server config / parser configuration):

| Limit | Default | Rationale |
|---|---|---|
| `max_bulk_len` | 512 MiB | Redis `proto-max-bulk-len` default |
| `max_array_len` | 1024 | Redis `proto-max-array-size` default |
| `max_request_size` | 512 MiB | Per top-level request ceiling (configurable) |

`max_request_size` scopes exactly one encoded top-level request (one command array, from its leading `*` through its final `\r\n`). It is deliberately **not** a ceiling on how much input a single read or `feed` call may carry: pipelined commands are accounted one at a time, so a batch of many individually valid commands is always accepted regardless of aggregate length. The retained buffer is bounded separately: it holds only the current incomplete element, and its length plus the bytes already accounted for the request in progress is checked against `max_request_size`.

Parser buffer: `Vec<u8>` with incremental growth, bounded by `max_request_size`; per-element limit checks applied before copying element bodies. Violations → `ProtocolError` (→ error reply + close):
- bulk length > `max_bulk_len` → `-ERR value is too long`
- array element count > `max_array_len` → `-ERR protocol error: array too large`
- buffered request exceeding `max_request_size` → `-ERR protocol error: request too large`

---

## 5. Storage Engine & Command Execution Model

- Storage: `HashMap<Vec<u8>, StoredValue>`; no persistence, no eviction in v1.
- Expiry: `expires_at: Option<Instant>`; **lazy expiry** (check on access) with `now: Instant` passed in (injected → deterministic tests). `SET` overwrites expiry.
- Execution: synchronous handler under store lock; single lock acquisition per command, no nested locks (deadlock-free by construction). Read lock for `GET`/`EXISTS`; write lock for `SET`/`DEL`. Future `MULTI/EXEC` batches under one write lock.

---

## 6. Concurrency & Memory Management

- Tokio multi-thread runtime; listener accept loop; per-connection tasks; store behind `Arc<std::sync::RwLock<Store>>` (**Revision 1**: std RwLock, replacing tokio RwLock).
- Handlers are synchronous; guard is acquired, handler run, guard released — **no lock held across an `.await`** (trivially satisfied; std RwLock guards are not async-aware and are never held across suspension points).
- On lock poisoning: fail closed (close/decline service for the command context); poisoning is never swallowed silently.

### Connection admission limit (Final approval)

- Server configuration includes `max_connections: usize`.
- Default: **1024** concurrent client connections.
- Behavior when the limit is reached: incoming connections at/over the limit are closed immediately on accept without spawning a handler task; the active count is decremented on connection close.

---

## 7. Testing Strategy

- **Unit — protocol:** byte-level parser tests incl. partial feeds, **multiple commands in one feed**, complete-then-incomplete sequences, binary payloads containing `\r\n`, nil bulk, malformed arrays, writer round-trips, limit enforcement.
- **Unit — store/commands:** happy paths, expiry via injected clock, binary values round-trip.
- **Integration (real TCP, `127.0.0.1:0`):** full happy paths, `EX`/`PX`, error replies, malformed → close, incomplete → timeout close, concurrent clients, bind failure.
- **Determinism:** injected clock for expiry; tokio paused time (via `tokio::time` + `test-util`) for timeout tests.

---

## 8. Difficult-to-Change Decisions

1. RESP version pinned to RESP2 now.
2. Error taxonomy (fatal vs. recoverable) settled now.
3. Reply encodings pinned at implementation (incl. `PING` simple status).
4. **Value representation frozen to byte-oriented (`Vec<u8>`) storage keys/values (Revision 1).** Future value types (hashes/sets) require internal redesign; settled now.
5. Single global namespace (no `SELECT`).
6. Concurrency shape settled now (std RwLock, sync execution).
7. **Parser batch/event API settled now (Revision 1):** batch `Commands`, `Incomplete`, `ProtocolError`; feed preserves remainder; retained-error handling via `take_error` for valid-batch-then-malformed ordering. *(added/revised)*
8. **Timeout policy settled now (Revision 1):** incomplete-request timeout only; armed while parser `Incomplete`; default 30s, configurable; no general idle timeout by default.

---

## 9. Dependencies

- **Runtime:** `tokio` with features `["net", "rt", "rt-multi-thread", "macros", "io-util", "time"]`.
- **Dev:** `tokio` with feature `["test-util"]`.
- **Justification:** `time` enables the incomplete-request timeout machinery and paused-time testing; `test-util` enables deterministic timeout/command tests. No `serde`, `bytes`, or `thiserror` unless a later milestone requires them.

---

## 10. Milestones

- **M0:** install Rust via rustup; `cargo init`; module skeleton; fmt/clippy clean.
- **M1:** protocol core — `RespValue`, parser (batch/limits), writer, error — + unit tests.
- **M2:** store + `Command` enum + handlers + dispatch — + unit tests.
- **M3:** TCP server — integration tests over loopback (happy path).
- **M4:** malformed/incomplete handling, concurrency/failure tests, graceful shutdown, timeouts.
- **M5:** hardening (limits already designed in M1), optional new commands.

---

## 11. Risks & Trade-offs

- Blocking/async boundary: sync handlers under `std::sync::RwLock` OK for v1; future blocking ops require offload plan (revisit when async commands arrive).
- Parser complexity: arrays + binary payloads + partial reads + limits; mitigated by byte-level unit tests.
- Expiry semantics: lazy-only v1 (untouched expired keys linger); decision approved for v1.
- Concurrency: single lock per command → simple & atomic; acceptable at v1 scale.

---

## 12. Revised Decision Log — Final Approval

Final list of decisions (incorporating Revision 1 and Revision 2 changes), for final approval before implementation:

1. **Tokio runtime & features:** multi-thread runtime; features `net`, `rt`, `rt-multi-thread`, `macros`, `io-util`, `time`; dev feature `test-util`. *(updated)*
2. **Crate shape:** single crate + lib/bin split. *(unchanged)*
3. **RESP version:** RESP2 only (v1). *(unchanged)*
4. **Value representation:** byte-oriented (`Vec<u8>` key & value). *(revised)*
5. **Error taxonomy:** protocol → reply+close; command → reply+keep open. *(unchanged)*
6. **Expiry:** lazy-only (v1). *(unchanged)*
7. **Concurrency:** per-connection tasks + `Arc<std::sync::RwLock<Store>>`; synchronous execution; no lock across `.await`; fail closed on poison. *(revised: tokio RwLock → std RwLock)*
8. **Namespace:** single global store, no `SELECT`. *(unchanged)*
9. **PING reply:** simple status `+PONG`. *(unchanged)*
10. **Parser API:** batch-oriented events (`Commands`/`Incomplete`/`ProtocolError`); `feed` preserves unconsumed partial-command bytes; valid-batch-then-malformed ordering via retained error + `take_error`. *(added/revised)*
11. **Timeout policy:** incomplete-request timeout only; armed while parser `Incomplete`; **absolute deadline** from first `Incomplete`, never reset by fragments, cleared on command completion; close on expiry. *(added/revised: Final approval)*
12. **Protocol limits:** `max_bulk_len` 512 MiB; `max_array_len` 1024; `max_request_size` 512 MiB (all configurable); violations → protocol errors. *(added)*
13. **Command-name handling:** ASCII case-insensitive dispatch. *(added)*
14. **Malformed-after-valid handling:** when a single read contains valid complete commands followed by malformed input, valid batch is emitted first and processed in order, then protocol error returned and connection closed; no valid command silently discarded. *(added: Final approval)*
15. **Connection limit:** configurable `max_connections` on server config; default **1024**; at-limit connections closed on accept. *(added: Final approval)*

---

## 13. Weak Assumptions Challenged

- **"Redis-compatible"** — scope pinned to commands above; later additions must preserve pinned reply/error semantics.
- **"Basic expiration"** — resolved via approved lazy-only decision.
- **"Meaningful subset"** — `Command` enum as extension surface mitigates `INCR` later.
- **Malformed/incomplete handling** — binary-safe bulk payloads handled via length-prefix design.

---

*Next step after final approval: Milestone 0 — install Rust via rustup, scaffold crate, begin M1 (protocol core).*
