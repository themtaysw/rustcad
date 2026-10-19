# Decisions

Short notes on trade-offs. Newest at the bottom.

## j1939: signal values are `Option<f64>`
`f32` has ~7 significant digits, not enough for latitude at 1e-7 deg resolution with a -210 offset.
`f64` is used for every signal so the API stays uniform. `None` means the sender said
"not available" or "error", so callers never see garbage numbers like 8031.875 rpm.

## j1939: reserved, error and not-available raw values all decode to `None`
J1939-71 splits the top of each range into reserved (0xFB..0xFD), error (0xFE) and
not available (0xFF). The agent only needs "is there a usable value", so all three collapse to `None`.
If error reporting becomes a requirement, `Spn::decode` can return a three-state enum instead.

## j1939: payloads must be exactly 8 bytes
All supported PGNs are single-frame, 8-byte messages. A shorter or longer frame is reported as
`DecodeError::InvalidLength` instead of being padded or truncated, so a broken sender shows up
in the decode error counter rather than as silently wrong values.

## j1939: unknown PGN is an error variant, not a panic
`decode` returns `Err(DecodeError::UnknownPgn)`. A real bus carries many PGNs we do not care about,
so the caller counts and skips them. The PGN is checked before the length, so unknown traffic
is always classified as unknown regardless of its size.

## j1939: encode clamps out-of-range values
`encode` rounds to the nearest raw step and clamps to the valid range. `None`, NaN and infinity
are written as not available (0xFF..). The simulator can never put an error or reserved
pattern on the bus by accident.

## j1939: round trips are tested within half a resolution step
"encode then decode equals input" only holds for values that are exact multiples of the resolution.
Tests assert exact bytes for hand-computed frames and |decoded - input| <= resolution / 2 for
arbitrary values. 0.05 and 1e-7 are not exact in binary floating point, so `==` would be flaky.

## j1939: SPN byte positions are 1-based
`Spn { byte: 4, .. }` matches the J1939-71 tables (EEC1 engine speed is "bytes 4-5").
The `- 1` to get a slice index lives in one place, so reviewing a signal against the spec
needs no mental arithmetic.

## j1939: CAN ID bits above 28 are ignored
`CanId::from_raw` takes a `u32` and ignores bits 29-31 instead of returning an error.
Socket APIs often keep flag bits there (for example the extended-frame flag in Linux `can_id`),
so the agent does not have to strip them first.

## dev: Linux VM via Lima with its own cargo target directory
SocketCAN only exists on Linux, so `deploy/lima-can.yaml` defines an Ubuntu VM with `vcan`
and can-utils. The repo is mounted writable at the same path, but the VM builds into
`~/target` (`CARGO_TARGET_DIR`). Sharing `target/` with macOS would mix incompatible artifacts
and force full rebuilds on every switch.

## tractor-sim: time is counted in ticks, not wall-clock
The model advances by a fixed `dt = 1 / rate_hz` and each PGN is sent every N ticks.
The simulation is deterministic for a given `--seed`, so tests check exact frame counts.
The main loop sleeps until absolute deadlines (`start + n * tick`) so the rate does not drift.

## tractor-sim: send rates
EEC1 goes out on every tick (default 20 Hz). CCVS and LFE use 100 ms and ET1 1 s, like the
J1939-71 defaults. Vehicle position uses 1 s instead of the 5 s default, typical for a GPS
receiver and gives a smoother track. A period shorter than one tick is sent every tick.

## tractor-sim: what `--garbage-rate` injects
Each tick, with the given probability, one extra frame of a random kind: a proprietary PGN
(0xFF00-0xFFFF, never one we decode) with random data, an EEC1 frame shorter than 8 bytes,
or a known PGN with all 0xFF ("not available"). These hit exactly the three paths the agent
must survive: `UnknownPgn`, `InvalidLength` and `None` values.

## tractor-sim: `--dry-run` writes candump log format
Without a CAN interface (macOS, CI) the simulator prints `(ts) iface ID#DATA` lines to stdout,
the same format as `candump -l`. The agent's replay source reads that format, so fixtures can
be generated without a VM.

## edge-agent: `FrameSource` uses `-> impl Future + Send`, not `async fn` or `Box<dyn>`
Async functions in traits are stable, but a plain `async fn` does not promise a `Send` future,
and `tokio::spawn` on the multi-threaded runtime needs one. Spelling out `impl Future + Send`
keeps that promise in the trait while implementations still write `async fn`. The source type
is a generic parameter, so there is no `async-trait` crate and no boxing; `main` picks the
concrete source once at startup.

## edge-agent: pipeline stages are tasks joined by bounded channels
Reader, decoder and logger run as separate tasks connected by `mpsc` channels with a capacity
of 1024. A bounded channel gives backpressure: a slow downstream stage makes the reader wait
instead of growing memory without limit. Shutdown follows the channels: when the source ends,
the reader drops its sender, the decoder drains and drops its sender, and so on.

## edge-agent: bad input is skipped and counted, never fatal
A broken replay line is logged at `warn` and skipped: malformed, not UTF-8, longer than 256
bytes, or a timestamp outside 1970..9999. Unknown PGNs are counted and logged at `debug`, since
a real bus is full of them. Frames with the wrong length are counted and logged at `warn`,
because they point at a broken sender; only the first 20 are logged one by one, the rest only
show in the health line, so a broken sender at bus speed does not wear out the SD card with log
writes. "Not available" values decode fine but produce no `Reading`.

## edge-agent: frame timestamps
Replayed frames keep their log timestamp, so a replay produces the same aggregated samples as
the original run. Live SocketCAN frames are stamped with `SystemTime::now()` when read. Kernel
receive timestamps (`SO_TIMESTAMP`) would be more precise but are not needed at a 1 s
aggregation interval.

## edge-agent: the replay fixture is a real recording
`tests/fixtures/sim-40s.log` was recorded with `candump -L vcan0` in the VM while
`tractor-sim --seed 42 --garbage-rate 0.05` ran, not generated by the simulator's own
formatter. The test therefore also checks that our parser reads what can-utils actually writes.
It asserts exact counts: 1711 frames, 6 unknown PGNs, 11 with the wrong length.

## receiver: `(device_id, outbox_id, seq)` is the primary key, inserts use `ON CONFLICT DO NOTHING`
Delivery is at-least-once: the agent retries a batch whenever it does not see a 2xx, including
when the receiver stored it but the response got lost. The key makes a retry harmless. The whole
batch goes in as one `INSERT ... SELECT FROM unnest(arrays)` statement: one round trip, and
`rowcount` gives the exact number of new rows. The response reports `inserted` and `duplicates`.

## receiver: batches are recorded too
A `batches` table keeps `(device_id, outbox_id, batch_id)` with record count and the agent's
`dropped_records` counter, also with `ON CONFLICT DO NOTHING`. A retried batch therefore does not
count the same drops twice, and there is a record of what each device sent.

## receiver: validation errors do not echo the input
FastAPI's default 422 response includes the rejected input. A `NaN` value then crashes the JSON
encoder and turns a client error into a 500, and a bad 10k-record batch would be sent back in
full. A custom handler returns only location, message and type.

## receiver: schema is created at startup
`schema.sql` uses `CREATE ... IF NOT EXISTS` and runs when the app starts. Fine for a single
table pair; a real service would use a migration tool (Alembic, sqitch) instead.

## receiver: isolated compose project
`docker-compose.yml` sets `name: rustcad` and binds Postgres to 127.0.0.1:55432 and the receiver
to 127.0.0.1:18000, so it can run next to other compose projects without port or volume
clashes. Tests use a separate `telemetry_test` database on the same server and never touch
dev data.

## edge-agent: aggregation by reading time, rpm min/max as extra signals
Readings are bucketed by the second of their own timestamp, not by wall-clock arrival, so a
replay produces the same samples as the live run. Every signal keeps its last value per second.
Engine speed also emits `engine_speed_rpm_min` and `engine_speed_rpm_max` as separate signals,
which fits the receiver's `(signal, value)` schema without changing it. A bucket closes when a
reading for another second arrives (in either direction, so a clock jump never stalls it),
after 2 s without input, or when input ends.

Known limitation: readings are keyed by signal only, not by source address. If two ECUs send
the same PGN (on a real tractor CCVS can come from more than one), their values are merged
into one series. The fix is to key by `(signal, source_address)` and carry the address to the
receiver, or to configure which address is trusted for each signal.

## edge-agent: outbox rows are deleted on ack, `seq` uses AUTOINCREMENT
Every row in the outbox is unsent, so "mark as sent" is `DELETE WHERE seq <= last_acked`.
A plain `INTEGER PRIMARY KEY` would hand out the same number again after the newest rows are
deleted; `AUTOINCREMENT` keeps the high-water mark in `sqlite_sequence`, so `seq` survives
restarts and is never reused. The restart test checks exactly that.

## edge-agent: SQLite in WAL mode with `synchronous=FULL`
A tractor is switched off with the key, without a shutdown. FULL fsyncs every commit, so an
acknowledged write survives power loss. The cost is one fsync per append (about once per
second at our rate), which is nothing. WAL lets reads proceed while a write is in progress.

## edge-agent: one connection behind `Arc<Mutex<_>>`, calls in `spawn_blocking`
The writer and the uplink share one `rusqlite::Connection`. It is `Send` but not `Sync`, so a
`Mutex` serializes access and `Arc` gives both tasks ownership. Every call runs in
`spawn_blocking` so a slow fsync never blocks the async worker threads. Two connections would
allow more parallelism but make the cap and depth bookkeeping racy.

## edge-agent: outbox schema is versioned
The schema version lives in SQLite's `user_version`. A new file gets the tables and version 1
in one transaction; a file with a higher version (written by a newer agent) or with tables but
no version (not an outbox) is refused instead of being modified. Agent state is one row in a
`state` table (`CHECK (id = 1)`) holding `outbox_id` and `dropped_records`, rather than a
key-value table with mixed value types. A schema change bumps the version and adds a migration.

## edge-agent: impossible timestamps never enter the outbox
The receiver needs RFC 3339 timestamps, which end at year 9999. The replay parser rejects
anything outside 1970..9999, using `checked_add` so a huge number cannot overflow
`SystemTime`. `Outbox::append` checks again, because a live clock can be wrong too: such
samples are not stored and count as dropped. A record the uplink cannot send would otherwise
sit at the head of the queue and block it forever.

## edge-agent: outbox cap drops the oldest records
When the outbox exceeds its cap, the oldest rows are deleted and `dropped_records` in the
`state` row is incremented in the same transaction. The next successful batch reports the counter
and the ack subtracts exactly the reported amount, so drops that happen while a batch is in
flight are not lost. Dropping oldest keeps the most recent state, which is what an operator
looks at first. If a batch in flight gets trimmed, those records are both delivered and counted
as dropped; the counter can over-report, never under-report.

## edge-agent: uplink retries forever, ack only after 2xx
Network errors, timeouts and 5xx are retried with exponential backoff (500 ms doubling to 60 s)
with equal jitter, so a fleet that lost coverage together does not retry in lockstep. 4xx are
retried too, at error level: a wrong token is a config problem, and dropping data would not
fix it. A batch that failed is kept in memory and resent exactly as it was, instead of being
read from the outbox again: the writer keeps appending during the backoff, so a re-read batch
would have grown, got a new id (`first-last` seq range) and made the receiver count its
`dropped_records` twice. A test resends after two lost responses and compares the bodies.

## edge-agent: shutdown never aborts an upload in flight
Shutdown stops the reader; every stage then drains in order, so all readings reach the
outbox. The uplink stops waiting and backing off, but lets a running request finish, then makes
a final attempt bounded by `final_flush_timeout`. An early version cancelled the request
instead; the receiver had already stored the batch, and the restart test caught it being sent
again in full.

## edge-agent: a failing stage stops the whole pipeline
Stages are awaited in pipeline order, so on their own a dead uplink (say, a SQLite read error)
would leave the agent reading the bus and filling the outbox with nobody uploading. Every
stage that can fail is spawned through `spawn_stage`, which cancels the shared shutdown token
on error: the pipeline drains, the process exits non-zero and systemd restarts it. Panics in
tasks are re-raised with `resume_unwind`, so the original message is not lost behind a
`JoinError`.

## edge-agent: config is defaults < TOML file < `EDGE_*` environment
The file holds the per-device setup, the environment holds what differs per deployment or must
stay out of files (the token, via systemd's `EnvironmentFile`). Loading is hand-written with
`toml` + serde instead of a layering crate: ten fields, explicit, easy to review. Unknown keys
are rejected so a typo like `batch_sise` fails at startup instead of silently using a default.
Validation reports every problem at once. `Config::load` takes the env lookup as a function,
so tests never modify the real process environment (`set_var` is `unsafe` in edition 2024).

## edge-agent: the token is a `Secret` type
`Secret` wraps the token and prints `<redacted>` in `Debug`, so `info!(?config)` at startup
can log the whole config without leaking it. The uplink config keeps the `Secret` too; the raw
value is only read in `bearer_auth` where the request is built.

## edge-agent: health line with live atomic counters
Every `health_interval_secs` (default 30 s) the agent logs frames/s, unknown PGNs, decode
errors, samples, outbox depth, upload failures and time since the last successful upload.
Tasks bump `AtomicU64` counters in a shared `Arc<Metrics>` with `Relaxed` ordering: each
counter stands alone and is only read for logging. Unknown PGNs are counted apart from decode
errors because a real bus is full of PGNs we do not decode; only wrong lengths mean a broken
sender. The same counters produce the summary at exit, so there is one source of numbers. The
outbox depth is mirrored in an atomic, so the health task never waits behind an fsync.

## ops: SIGTERM and SIGINT both start the graceful shutdown
systemd stops services with SIGTERM, a terminal sends SIGINT; both cancel the same token. The
reader stops, the pipeline drains into the outbox, the uplink makes its final attempt, the
process exits 0. A second signal exits immediately with the usual 128 + signal number (130 for
SIGINT, 143 for SIGTERM). Both handlers are registered once at startup, so a second signal can
never arrive in a gap where the default handler is active. `TimeoutStopSec=30` in the unit
is longer than `final_flush_secs` so systemd never has to SIGKILL a clean shutdown.

## ops: systemd unit with `DynamicUser` and `StateDirectory`
`DynamicUser=yes` gives the agent its own unprivileged user without a `useradd` step, and
`StateDirectory=edge-agent` creates `/var/lib/edge-agent` owned by that user, which is the only
writable path (`ProtectSystem=strict`). `RestrictAddressFamilies` allows CAN, IP, Unix and
netlink sockets and nothing else. `Restart=on-failure` brings the agent back after a crash or
SIGKILL; tested in the VM by killing it mid-run, with no seq gaps in Postgres afterwards.
Readings still in memory at that moment (at most the current second) are lost; everything that
reached the outbox is not.

## ops: release profile
`lto = true`, `codegen-units = 1`, `strip = true` and `panic = "abort"`: edge-agent is 6.7 MB on
aarch64, most of it TLS (rustls with aws-lc) and bundled SQLite. `opt-level = "z"` would shrink it
further at the cost of speed; not needed at this size. With `panic = "abort"` a bug kills the
process at once and systemd restarts it, which SQLite handles fine.

## edge-agent + receiver: every outbox file has its own `outbox_id`
`seq` is unique only within one outbox file. A device that gets a fresh outbox (reinstall,
deleted state dir, replaced SD card) under the same `device_id` starts at seq 1 again; with
the old key `(device_id, seq)` the receiver dropped those records as duplicates (seen in the
quickstart test: `inserted=5 duplicates=3` on a device that already had seq 1-3).

The outbox generates a random UUID v4 when the file is created and keeps it in its `state`
row. The id is sent as `outbox_id` with every batch, and
the receiver's keys are `(device_id, outbox_id, seq)` and `(device_id, outbox_id, batch_id)`.
A UUID instead of a counter or a boot id: nothing has to be coordinated with the receiver,
and it changes exactly when the seq numbering restarts. `/devices/{id}/latest` still looks
at all outboxes of a device and picks the newest `ts`. `outbox_id` is a required field, so an
older agent without it gets 422 until it is updated.

`schema.sql` only runs `CREATE TABLE IF NOT EXISTS`, so an existing dev database keeps the
old tables and every ingest fails. Reset it with
`docker compose -f cloud/docker-compose.yml down -v` (project `rustcad` only, which deletes
the stored telemetry), or migrate by hand:

```sql
ALTER TABLE telemetry ADD COLUMN outbox_id text NOT NULL DEFAULT 'legacy';
ALTER TABLE telemetry ALTER COLUMN outbox_id DROP DEFAULT;
ALTER TABLE telemetry DROP CONSTRAINT telemetry_pkey, ADD PRIMARY KEY (device_id, outbox_id, seq);
ALTER TABLE batches ADD COLUMN outbox_id text NOT NULL DEFAULT 'legacy';
ALTER TABLE batches ALTER COLUMN outbox_id DROP DEFAULT;
ALTER TABLE batches DROP CONSTRAINT batches_pkey, ADD PRIMARY KEY (device_id, outbox_id, batch_id);
```

The pytest fixture drops and recreates `telemetry_test` every session for the same reason.

## ci: GitHub Actions on Linux
One job runs `cargo fmt --check`, clippy with `-D warnings` and the tests on ubuntu, so the
SocketCAN code behind `cfg(target_os = "linux")` is compiled and linted, which it is not on a
Mac. A second job runs ruff, pytest against a Postgres service container and `docker build`
for the receiver. `--locked` / `uv sync --locked` make CI fail if a lock file is out of date.
A third job builds with Rust 1.89, the `rust-version` in `Cargo.toml`, found by building with
older toolchains: 1.88 fails because `uuid` needs 1.89. `cargo doc` runs with `-D warnings`.

## rust: lints are part of the build
`[workspace.lints]` forbids `unsafe` and turns on a selection of pedantic clippy lints, most
importantly the `cast_*` family: every `as` that could truncate, wrap or lose a sign is either
replaced (`try_from`, `From`, rusqlite's checked `u64` conversion) or carries an
`#[expect(..., reason = "...")]` that says why it is safe. `expect` instead of `allow` makes the
compiler complain if the justification ever stops being needed. `j1939`, the reusable library,
also has `#![warn(missing_docs)]`.
