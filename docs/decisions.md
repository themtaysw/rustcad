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
