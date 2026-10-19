# rustcad: tractor telemetry edge agent

A Rust agent for a Linux edge device on agricultural machinery. It reads SAE J1939 messages
from the CAN bus, turns them into one sample per signal per second, stores them in a local
SQLite outbox and ships them to a small cloud receiver whenever there is connectivity. Tractors
work in fields with patchy mobile coverage and get switched off with the key, so the agent is
built offline-first: nothing is lost because the network is down or the power is cut, and
nothing is stored twice because a request was retried. The CAN bus is virtual (`vcan0`) and a
simulator plays the tractor, so no hardware is needed.

[![CI](https://github.com/themtaysw/rustcad/actions/workflows/ci.yml/badge.svg)](https://github.com/themtaysw/rustcad/actions/workflows/ci.yml)

## Architecture

```
tractor-sim ──CAN frames──> vcan0 ──> edge-agent ──HTTP(S) batches──> receiver ──> Postgres
                                         │
                                      SQLite outbox
                                   (store and forward)
```

Inside `edge-agent`, every stage is a tokio task connected to the next by a bounded channel:

```
FrameSource ─> decoder ─> aggregator ─> outbox writer ─> SQLite <─ uplink ─> POST /ingest
(SocketCAN or   (j1939)    (1 sample/s     (one tx per             (batches, backoff,
 candump replay)            per signal)     chunk)                  ack after 2xx)
```

| Path | What |
|---|---|
| `crates/j1939` | Library: 29-bit CAN ID parsing, decode/encode of EEC1, ET1, LFE, CCVS, vehicle position. Pure, no IO. |
| `crates/tractor-sim` | Binary: a tractor warming up and working a field near Prostějov, writing J1939 frames to a CAN interface. |
| `crates/edge-agent` | Binary + lib: read, decode, aggregate, outbox, uplink. |
| `cloud/receiver` | Python FastAPI ingest service with Postgres dedupe. |
| `cloud/docker-compose.yml` | Postgres + receiver (compose project `rustcad`). |
| `deploy/` | Lima VM definition, `setup-vcan.sh`, systemd unit, example config. |
| `docs/decisions.md` | Every trade-off, 2-4 lines each. |
| `docs/j1939-can-id.md` | How the 29-bit J1939 CAN ID is laid out. |

## Quickstart

You need Rust, Docker (or OrbStack), and on macOS [Lima](https://lima-vm.io) for a Linux VM,
because SocketCAN only exists on Linux. [uv](https://docs.astral.sh/uv/) is needed to run the
receiver tests.

**1. Receiver and Postgres** (on the host):

```sh
docker compose -f cloud/docker-compose.yml up -d --build --wait
```

The receiver listens on `127.0.0.1:18000` with the dev token `dev-token`, Postgres on
`127.0.0.1:55432`.

**2. Linux VM with a virtual CAN bus.** On macOS:

```sh
limactl start --name=can --mount "$PWD:w" deploy/lima-can.yaml
limactl shell can
```

The VM gets can-utils and Rust, mounts the repo at the same path and builds into its own
`~/target`. On a Linux host skip this step. Then, in the VM (or on Linux):

```sh
./deploy/setup-vcan.sh
```

**3. Simulator** (VM, first shell):

```sh
cargo run --release -p tractor-sim -- --garbage-rate 0.02
```

`candump vcan0` shows the frames. After 30 s of idling the tractor starts working the field.

**4. Agent** (VM, second shell). From the VM the host is `host.lima.internal`:

```sh
EDGE_RECEIVER_TOKEN=dev-token EDGE_RECEIVER_URL=http://host.lima.internal:18000 \
  cargo run --release -p edge-agent
```

**5. Latest values** (host):

```sh
curl -s localhost:18000/devices/tractor-1/latest -H 'Authorization: Bearer dev-token'
```

Stop the receiver for a while (`docker compose -f cloud/docker-compose.yml stop receiver`),
start it again, and check that `seq` has no gaps:

```sh
docker compose -f cloud/docker-compose.yml exec postgres psql -U telemetry \
  -c "SELECT count(*), min(seq), max(seq) FROM telemetry WHERE device_id = 'tractor-1'"
```

**Without a VM**, the agent can replay a recorded CAN log on any OS:

```sh
EDGE_RECEIVER_TOKEN=dev-token cargo run -p edge-agent -- \
  --replay crates/edge-agent/tests/fixtures/sim-40s.log --realtime
```

## How reliability works

- **Aggregation.** EEC1 arrives at 20 Hz, but nobody needs every frame in the cloud. The
  aggregator keeps the last value of each signal per second, plus min and max for engine
  speed (`engine_speed_rpm_min` / `_max`), so short spikes are not hidden. About 8 samples
  per second leave the device instead of ~42 frames.
- **Outbox first.** Samples go into SQLite before any upload is attempted. The uplink only
  reads from the outbox. The database runs in WAL mode with `synchronous=FULL`, so a write
  that was committed survives a power cut.
- **`seq` is durable and never reused.** Each record gets `seq` from an `AUTOINCREMENT` key.
  SQLite remembers the highest value ever handed out, so even after all rows were uploaded and
  deleted, and the agent restarted, numbering continues where it stopped.
- **At-least-once delivery, deduped by the receiver.** A batch is deleted from the outbox only
  after the receiver answers 2xx. If the answer is lost, the batch is sent again. The receiver
  stores records with `ON CONFLICT (device_id, seq) DO NOTHING`, so a resend changes nothing.
  The batch id is the seq range, so a resent batch has the same id.
- **Retries never give up.** Network errors, timeouts, 5xx and even 4xx are retried with
  exponential backoff (0.5 s doubling to 60 s) and jitter, so a fleet coming back into coverage
  together does not hit the receiver in lockstep. A 4xx is logged as an error, since it
  usually means a wrong token, but the data stays.
- **Graceful shutdown.** On SIGTERM or SIGINT the agent stops reading, lets every stage drain
  into the outbox and makes one final upload attempt (`final_flush_secs`). A request already
  in flight is never aborted. Whatever is not uploaded stays in the outbox for the next start.
- **When the disk cap is hit.** The outbox holds at most `outbox_cap` records (default
  500 000, about 17 hours offline at 8 samples per second). Beyond that the oldest records are deleted and a
  `dropped_records` counter is incremented in the same transaction. The next successful batch
  reports the counter to the receiver, which stores it per batch. Dropping the oldest keeps the
  most recent state, which is what an operator looks at first. The loss is visible, not silent.
- **Crashes.** Under systemd, `Restart=on-failure` brings the agent back. What is already in
  the outbox is safe; only readings still in memory (at most the current second) are lost.

These properties are covered by tests (`crates/edge-agent/tests/pipeline.rs`), which replay
a real `candump` recording through the whole agent into a fake receiver:

- **every sample arrives exactly once**, one per signal and second;
- **receiver outage**: 503s and "stored but the response was lost" (500). After dedupe every
  `seq` arrived and none is missing;
- **restart**: three runs over the same outbox (network up, network down, network back). Seqs
  continue without gaps or reuse;
- **outbox cap**: overflow drops the oldest and the next batch reports exactly how many.

They were also checked live in the VM: the receiver was stopped for 15 s while the
simulator ran, and the agent was killed with `kill -9` under systemd. Postgres afterwards had
every `seq` from 1 to the last one, with no gaps.

## Configuration

Built-in defaults, overridden by a TOML file (`--config`), overridden by `EDGE_*` environment
variables. Unknown keys are rejected and all problems are reported at once. See
`deploy/config.example.toml`.

| Key | Env | Default |
|---|---|---|
| `device_id` | `EDGE_DEVICE_ID` | `tractor-1` |
| `iface` | `EDGE_IFACE` | `vcan0` |
| `receiver_url` | `EDGE_RECEIVER_URL` | `http://127.0.0.1:18000` |
| `receiver_token` | `EDGE_RECEIVER_TOKEN` | required |
| `batch_size` | `EDGE_BATCH_SIZE` | `500` (max 10 000) |
| `flush_interval_secs` | `EDGE_FLUSH_INTERVAL_SECS` | `5` |
| `final_flush_secs` | `EDGE_FINAL_FLUSH_SECS` | `10` |
| `outbox_path` | `EDGE_OUTBOX_PATH` | `edge-outbox.db` |
| `outbox_cap` | `EDGE_OUTBOX_CAP` | `500000` |
| `health_interval_secs` | `EDGE_HEALTH_INTERVAL_SECS` | `30` |

Every `health_interval_secs` the agent logs one line:

```
INFO health frames_per_sec=42.4 unknown_pgns=1 decode_errors=4 samples=157 outbox_depth=16 upload_failures=0 last_upload=1s ago
```

Log verbosity follows `RUST_LOG` (default `info`).

## Running as a service

`deploy/edge-agent.service` runs the agent with `DynamicUser=yes` (its own unprivileged
user), `StateDirectory=edge-agent` for the outbox (the only writable path),
`Restart=on-failure`, SIGTERM on stop with `TimeoutStopSec` above the final flush time, and
sandboxing that allows only CAN, IP, Unix and netlink sockets. The token goes in
`/etc/edge-agent/env` (mode 0600) via `EnvironmentFile`, so it is not in the config file and
never in logs. Installation steps are at the top of the unit file.

The release profile (`lto`, one codegen unit, `strip`, `panic = "abort"`) gives a 6.7 MB
`edge-agent` binary on aarch64, most of it rustls and bundled SQLite.

## Tests and CI

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test

cd cloud/receiver
docker compose -f ../docker-compose.yml up -d --wait postgres
uv run pytest
```

The receiver tests use their own `telemetry_test` database and never touch dev data.
GitHub Actions (`.github/workflows/ci.yml`) runs fmt, clippy and tests for the workspace on
Linux, so the SocketCAN code is compiled too, and ruff, pytest against a Postgres service, and
a Docker build for the receiver.

## Trade-offs and what would change in production

- **Auth.** One shared bearer token for all devices. Production would use per-device
  credentials, ideally mTLS with a client certificate per device, so a single stolen device
  can be revoked without touching the rest of the fleet.
- **TLS.** The agent speaks HTTPS through rustls, but locally everything is plain HTTP.
  Production would also pin the receiver's CA, so a compromised public CA cannot intercept
  traffic.
- **Device provisioning.** `device_id` and the token are written by hand. A fleet needs a
  provisioning step that issues identity and credentials at manufacturing or first boot.
- **OTA updates.** Not covered. A real device needs signed, atomic updates with rollback
  (A/B partitions, e.g. RAUC or Mender), because a bricked agent in a field is expensive.
- **Time sync.** Timestamps come from the system clock. Edge devices often have no RTC
  battery and no network at boot, so the clock can be wrong by years. Production would take
  time from GPS (J1939 or NMEA), mark samples recorded before sync, and correct them on the
  receiver.
- **Fresh outbox, old numbers.** `seq` starts at 1 in every new outbox file. A device that
  gets a new outbox under the same `device_id` (reinstall, replaced SD card) would have its
  first records dropped by the receiver as duplicates of old ones. The fix is an `outbox_id`
  generated with the outbox and part of the dedupe key; see `docs/decisions.md`.
- **Schema migrations.** The receiver creates its tables at startup with `IF NOT EXISTS`.
  A real service would use a migration tool.
- **Transport.** HTTP batches are simple and easy to debug. MQTT (QoS 1) would push data with
  less overhead and give a command channel back to the device.
- **Protocol coverage.** Five single-frame PGNs. A real tractor needs many more, multi-packet
  messages (J1939 transport protocol) and diagnostic messages (DM1).
- **Observability.** A log line every 30 s. A fleet needs metrics shipped centrally
  (Prometheus or piggybacked on uploads) to see which devices are stuck or offline.

## How AI was used

The project was built with Claude Code as a pair programmer. I wrote the spec and the
milestone plan. Claude wrote the code one milestone at a time. I reviewed every diff, ran and
tested it myself, asked about anything I could not explain, and made each commit.

Things that had to be caught and corrected along the way:

- Graceful shutdown first cancelled the upload request in flight. The receiver had already
  stored the batch, so it was sent again in full. The restart test caught the duplicates;
  shutdown now only interrupts waiting and backoff.
- An `.await` inside a `tracing::warn!` call held non-`Send` values across the await, so the
  uplink task could not be spawned.
- A `NaN` value made the receiver crash with 500 instead of 422, because FastAPI's default
  validation error echoes the input back as JSON. A test caught it.
- A plain `INTEGER PRIMARY KEY` would reuse `seq` after deleting the newest rows; the outbox
  uses `AUTOINCREMENT` and a test proves it.
- Test assumptions that did not match the real behaviour (batch sizes, timing) had to be
  rewritten to test the property, not the incidental numbers.
