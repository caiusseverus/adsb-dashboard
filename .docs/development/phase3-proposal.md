# Phase 3 — Native Acceleration Proposal

Date: 2026-03-26
Last updated: 2026-03-26
Status: **In progress** — Option C phases 0+1 complete; Option B (C extension) complete; WebSocket bottleneck fixed; Phase 3 (hybrid mode) next

Prerequisite: Validate Phase 2 at full load (165–200 ac + client). Only proceed if targets not met.

---

## Implementation progress

### ✅ Phase 0 — Ingest abstraction layer (complete)

**0a. Config vars** (`config.py`)
- `INGEST_MODE` — `beast` (default) | `readsb` | `hybrid`
- `READSB_JSON_DIR` — path to readsb output directory (default `/run/readsb`)
- `AIRSPY_STATS_PATH` — path to airspy_adsb stats.json (default `/run/airspy_adsb/stats.json`)
- `READSB_POLL_INTERVAL_S` — poll frequency in seconds (default `1.0`)

**0b. Aircraft dataclass + AircraftState** (`aircraft_state.py`)
- 26 new optional fields on `Aircraft`: `alt_geom`, `gs`, `track`, `track_rate`, `roll`,
  `true_heading`, `geom_rate`, `emergency`, `nav_qnh`, `nav_altitude_fms`, `nav_heading`,
  `nav_modes`, `nic`, `rc`, `nac_p`, `nac_v`, `sil`, `gva`, `sda`, `adsb_version`,
  `wind_dir`, `wind_speed`, `oat`, `tat` (all `None` in Beast mode)
- `AircraftState._readsb_last_total` and `._readsb_msg_counts` for delta tracking
- `update_from_json(aircraft_list, now, total_messages)` — full field mapping:
  - signal dBFS → 0–255 conversion
  - per-session message delta tracking (readsb gives cumulative counts)
  - per-second/per-minute stats maintenance (mirrors `_tick()` logic)
  - MLAT flag from `type=="mlat"` or `"lat" in mlat_fields`
  - position reliability set to max (trust readsb's CPR)
  - enrichment queue population for new aircraft
  - readsb `--db-file` fields (`r`, `t`, `desc`, `dbFlags`) applied if present
- All 26 new fields included in `get_snapshot()` output (null when in Beast mode)

### ✅ Phase 1 — Option C: readsb JSON ingest (complete)

**1a. `backend/readsb_ingest.py`**
- `readsb_poller(state)` — async coroutine, called via `_bg()` in lifespan
- Polls `aircraft.json` every `READSB_POLL_INTERVAL_S` using `orjson` (json fallback)
- File-age watchdog: warning >5 s stale, error >30 s stale
- Auto-detects receiver lat/lon from `receiver.json` at startup (only if not set in env)

**1b. `backend/readsb_stats.py`**
- `readsb_stats_poller()` — polls `stats.json` every 60 s
- Parses: signal_avg/peak/noise, accepted messages, bad preambles, blocks_dropped,
  CPR global/local ok/bad/speed, track counts, readsb CPU breakdown
- Optional airspy_adsb: rssi/snr/noise quartiles, gain, lost_buffers, df_counts
- Stores in module-level `_latest` dict; exposed via `get_latest()` for status endpoints
- No DB schema changes — in-memory only for now

**1c. `backend/main.py` lifespan**
- Branches on `INGEST_MODE`:
  - `beast` (default): existing path, completely unchanged
  - `readsb`: starts `readsb_poller` + `readsb_stats_poller`; no Beast TCP, no decoder thread
  - `hybrid`: starts both readsb pollers + decoder thread + MLAT runners (no `_beast_runner`)
- Raises `ValueError` on unknown `INGEST_MODE`

**Notes / deferred**
- `get_aircraft_live()` (detail panel) not yet updated with new fields — deferred
- `readsb_stats` data not yet wired into DB `minute_stats` — deferred to schema work
- `_push_updates` queue-depth sampling unchanged (always 0 in readsb mode, harmless)

### ✅ Phase 2 — Option B: C decode extension (complete)

**2a–2b. `backend/native/` — extracted C files + `decode_api.c`**

- C files extracted from readsb: `cpr.c`, `crc.c`, `mode_s.c`, `comm_b.c`, `ais_charset.c`
- `decode_types.h` — minimal struct/enum definitions; `Modes.*` globals stubbed
- `decode_api.c` — thin entry point wrapping `decodeModesMessage()`; also exposes
  `solve_cpr_airborne()` and `solve_cpr_relative()` wrappers over `cpr.c` functions
- `Makefile` builds `libdecode.so`; `make install` copies to `backend/`

**2c. `backend/decode_cffi.py` — Python cffi wrapper**

- Loads `libdecode.so` via cffi ABI mode
- `decode_message(raw_hex, signal, timestamp, msg_type) → dict`
- `solve_cpr_airborne()` / `solve_cpr_relative()` — CPR solver wrappers
- Pre-allocated output buffers (`_cpr_out_lat`, `_cpr_out_lon`) at module level
  to eliminate per-call `ffi.new("double *")` allocation overhead

**Integration in `aircraft_state.py`**

- `_USE_NATIVE = True` when `libdecode.so` loads successfully; falls back to pyModeS
- CPR integers stored as `(lat_int, lon_int, timestamp)` 3-tuple (was 2-tuple in native path)
- Global CPR solver calls `_decode_cffi.solve_cpr_airborne()` instead of `pms.adsb.position()`
- Local CPR solver calls `_decode_cffi.solve_cpr_relative()` instead of `pms.adsb.position_with_ref()`
- Fixed bugs in native decode path: `cpr_valid`/`heading_valid` keys never present in cffi
  output dict — changed guards to check key presence (`'cpr_odd' in _nd`) instead

**Build system**
- `Dockerfile`: `make -C backend/native && make -C backend/native install` added to build stage
- `install.sh`: same `make` steps added after pyModeS Cython build

### ✅ WebSocket broadcast bottleneck fix (complete)

**Problem**: `asyncio.gather` in `_push_updates` blocked the entire push cycle waiting
for slow clients. With one stalled client: broadcast_avg 71 ms, lock_wait p99 1001 µs,
32k message drops per run.

**Fix in `main.py`**:
- `_clients` changed from `list[WebSocket]` to `dict[WebSocket, asyncio.Queue]`
- `_push_updates` serializes once, then `q.put_nowait(payload)` for each client — non-blocking
- Each client gets a per-client `asyncio.Queue(maxsize=2)` and a `_sender` coroutine task
- Slow/stalled clients drop frames (queue full → `QueueFull` discarded) without blocking others

**Result**: broadcast_avg ~4.6 ms, lock_wait p99 ~0.3 µs, drops = 0.

### ✅ Deployment improvements (complete)

- `install.sh` update mode: detects existing install, `git pull` vs fresh clone
- `install.sh` `.env` diff: new keys from `.env.example` appended commented-out on update
- `install.sh` uv: installed to `/usr/local/bin` (system PATH, survives sudo); checked with `-x`
- `install.sh` git safe.directory: `git config --global --add safe.directory` before pull
- `install.sh` service file: written via heredoc; uses `.venv/bin/uvicorn` directly (no `uv run`)
- `install.sh` port: `ExecStart` wrapped in `/bin/sh -c '...'` so `${HOST_PORT:-8000}` expands
- `install.sh` `.env` permissions: `chown ${SUDO_USER:-root}:adsb 640` so user can edit without sudo
- `install.sh` data files: airports/coastline skipped on update if already present
- `systemd/adsb-dashboard.service`: `WorkingDirectory` updated to `backend/`; `ExecStart` updated

---

---

## Current state after Phases 1–2 + WebSocket fix

| Metric | Pi baseline | After Phase 2 pyModeS (39 ac) | After C extension + CPR in C | Target |
|--------|-------------|-------------------------------|------------------------------|--------|
| `msg_queue_stats p50` | 4834 | 0 | 0 | < 100 |
| `msg_queue_stats p95` | — | 0 | 0 | < 500 |
| `msg_drops_total` | 848k+ | 0 | 0 | 0 |
| `msg_decode_us p99` | 6196 µs | 1579 µs | < 500 µs (est.) | < 2000 µs |
| `broadcast_avg` | 186 ms | ~71 ms (1 client) | ~4.6 ms | < 50 ms |
| `push total_avg` | 195 ms | — | < 10 ms (est.) | < 80 ms |
| `lock_wait_us p99` | — | ~1001 µs | ~0.3 µs | — |

Broadcast bottleneck resolved. C extension + CPR solver confirmed working.
Full-load validation at 165–200 aircraft still pending.

---

## Option A — Cythonize additional hot functions

### What it is

Compile the CPU-hot pure-Python functions in pyModeS and our own decode path
into C extensions using Cython. The existing `pyModeS.c_common` extension
already accelerates `hex2bin`, `crc`, and `bin2int`, but these are cheap
operations. The actual bottleneck is higher-level decode: CPR math, BDS
register detection, and altitude decoding — all still pure Python.

### Candidate functions

| Function | Source | Why it's hot | Approx % of decode time |
|----------|--------|-------------|------------------------|
| `pms.adsb.position()` (global CPR) | `pyModeS/decoder/adsb.py` | Trig-heavy: `acos`, `cos`, `sin` on every position pair | ~25% |
| `pms.adsb.position_with_ref()` (local CPR) | `pyModeS/decoder/adsb.py` | Same trig, fallback path | ~10% |
| `_bds40.is40()` / `_bds50.is50()` / `_bds60.is60()` | `pyModeS/decoder/bds/` | Called on every DF20/21 frame; bitfield parsing + range checks | ~15% |
| `pms.altcode()` (Gillham decode) | `pyModeS/decoder/common.py` | Bit manipulation on every DF4/20 | ~5% |
| Beast frame unescape loop | `beast_client.py` | Byte-by-byte scan with escape handling | ~5% |
| `_accept_altitude()` (8-layer filter) | `aircraft_state.py` | Called per altitude message; branchy but not trig-heavy | ~5% |

### Implementation steps

1. Fork pyModeS 2.9 into `vendor/pyModeS/` (we already pin this version)
2. Add `.pyx` files for CPR decode, BDS detection, altitude decode
3. Add `setup.py` / `pyproject.toml` build config with Cython
4. Ensure ARM (aarch64) and x86_64 both compile cleanly
5. Cythonize `beast_client.py` unescape loop separately
6. Benchmark each function independently with `timeit`
7. Integration test: verify identical output to pure-Python path

### Expected performance gain

- CPR decode: 5–10x speedup (trig in C vs Python float ops)
- BDS detection: 3–5x (bitfield ops, branch prediction)
- Altitude: 3–5x
- **Overall decode throughput: 2–4x improvement**
- At 200 aircraft / ~300 msg/s: p99 decode could drop from ~1600µs to ~400–800µs

### Detrimental factors

- **Maintenance burden**: forked pyModeS means we own the Cython layer; upstream updates require manual merge into `.pyx` files
- **Build complexity**: Cython compilation adds a build step; `uv run` no longer just works — need pre-built wheels or compile-on-install
- **ARM cross-compile**: if developing on x86 and deploying to Pi, either cross-compile or build on the Pi itself (slow: ~5 min for Cython)
- **Debugging**: Cython tracebacks are less readable; segfaults possible if typing annotations are wrong
- **Partial coverage**: only the functions we choose to port get faster; any new decode logic (e.g. future BDS registers) needs Cython treatment too
- **No benefit to snapshot/broadcast**: doesn't help the dict-building or JSON serialization path

### Effort: 3–5 days

---

## Option B — C extension modelled on readsb's decode path

### What it is

Replace the entire pyModeS decode pipeline with C functions, either extracted
from readsb's codebase or reimplemented from ICAO 9871/DO-260B specs using
readsb as a correctness reference. Exposed to Python via `cffi` or the
CPython C API.

readsb is the de facto reference implementation for Mode-S/ADS-B decoding,
used by virtually every ADS-B receiver. Its decode functions are written in C11
and several are near-standalone with minimal coupling to readsb's event loop.

### readsb source analysis — extractability

| File | Coupling | External deps | Assessment |
|------|----------|---------------|------------|
| `cpr.c` | **None** | `<math.h>` only | Fully standalone. Has its own test target (`cprtests`). |
| `crc.c` | **Minimal** | Struct definitions from `readsb.h` | Trivially extractable; needs `struct errorinfo` + `modesMessage`. |
| `mode_s.c` | **Low** | ~12 `Modes.*` refs (debug/optional flags) | Core `decodeModesMessage()` + `decodeExtendedSquitter()` self-contained once config flags stubbed. |
| `comm_b.c` | **Minimal** | 1 debug reference (`Modes.debug_callsign`) | BDS 4.0/5.0/6.0 detection. Trivially extractable. |
| `icao_filter.c` | **Minimal** | `addrHash` (inline hash function) | Simple open-addressed hash table. Easily replaced. |
| `track.c` | **High** | `struct aircraft`, `Modes` global, globe_index, stats | CPR state machine, speed checks, dedup deeply coupled. **Not extractable as-is.** |

Total surgery to extract the standalone subset: ~200 lines of changes.
`track.c` position validation (speed checks, CPR state machine) would remain
in Python or be reimplemented separately.

### Two sub-approaches

**B1 — Extract from readsb (faster, GPL-encumbered):**

Pull `cpr.c`, `crc.c`, `mode_s.c`, `comm_b.c`, `icao_filter.c`, `ais_charset.c`
into `backend/native/`. Create minimal `decode_types.h` from `readsb.h`
(struct definitions, enums, constants — ~200 lines of surgery to remove the
`Modes` global, networking, SDR, and threading). Wrap with `cffi`.

- Effort: 3–5 days
- License: GPLv3+ on the C extension (readsb is GPLv3+). Fine for personal
  use; requires GPL notice if distributed.

**B2 — Clean-room reimplement from ICAO specs (slower, license-free):**

Use readsb as a correctness reference but implement from DO-260B / ICAO 9871.
The algorithms (CRC polynomial 0xfff409, CPR math, Gillham tables) are public
standards — not copyrightable. Same file structure and `cffi` wrapper.

- Effort: 1–2 weeks
- License: unrestricted

### Candidate functions (same entry points as Option A, but full C pipeline)

| Function | readsb source | What it replaces |
|----------|--------------|-----------------|
| `decodeModesMessage()` | `mode_s.c` | Entire pyModeS DF routing + decode |
| `modesChecksum()` | `crc.c` | `pms.crc()` |
| `decodeCPRairborne()` / `decodeCPRrelative()` | `cpr.c` | `pms.adsb.position()` + `position_with_ref()` |
| `decodeAC13Field()` / `decodeAC12Field()` | `mode_s.c` | `pms.altcode()` + altitude decode |
| `decodeCommB()` | `comm_b.c` | `_bds40.is40()` / `_bds50.is50()` / `_bds60.is60()` + all BDS field extraction |
| `icaoFilterTest()` / `icaoFilterAdd()` | `icao_filter.c` | Our `_confirmed_icaos` OrderedDict |
| Beast frame unescape | `net_io.c` (simple) or keep Python | `beast_client.py` unescape loop |

### Implementation steps

1. Create `backend/native/` with extracted C files (B1) or clean-room reimplementation (B2)
2. Create `decode_types.h` — minimal struct/enum definitions
3. Stub out ~12 `Modes.*` references in `mode_s.c` with config struct or module-level vars
4. Replace `cmalloc`/`sfree` macros with standard `malloc`/`free`
5. Replace `addrHash` in `icao_filter.c` with inline hash
6. Build as shared library: `gcc -O2 -shared -fPIC -o libdecode.so *.c -lm`
7. `cffi` wrapper: `from _decode import ffi, lib` — expose `decode_message(raw_hex) → struct`
8. Python integration: `aircraft_state.py` calls C decode instead of pyModeS
9. Verify output parity: C decode vs pyModeS on captured Beast data
10. ARM build: compiles natively on Pi 4 in ~5–10 seconds

### Expected performance gain

- Per-message decode: **50–100x faster** than pyModeS (table-driven CRC, native bit extraction, hardware trig)
- CPR decode: 8–15x (C `cos`/`sin`/`acos` = hardware FPU on Pi 4's Cortex-A72)
- BDS detection: 5–10x (bitfield ops, branch prediction)
- CRC: 20–50x (table lookup vs Python integer math)
- **Overall decode throughput: 5–10x improvement**
- At 200 aircraft: p99 decode could drop to ~100–300µs
- Eliminates pyModeS entirely from the hot path

### Detrimental factors

- **C maintenance**: contributors need C knowledge to modify decode functions; Python-only developers can't easily extend
- **Memory safety**: C has no bounds checking; a bug in the extension can segfault the entire Python process. Mitigated by using readsb's battle-tested code rather than writing from scratch.
- **GPL (B1 only)**: extracting readsb code means GPLv3+ on the extension. Not an issue for personal use. B2 avoids this.
- **Partial coverage**: position validation (speed checks, CPR state machine from `track.c`) stays in Python — this is the "intelligence" layer, not the compute-hot path
- **Same limitation as Option A**: snapshot/broadcast/JSON path unchanged
- **Build step**: `gcc` required on deployment target. Pi 4 has it by default. Unlike Rust, no cross-compile toolchain needed — native compile is fast (~5–10 seconds).
- **Python version coupling**: `cffi` ABI mode avoids this entirely (no compile-time Python dependency); API mode needs recompile per Python minor version

### C vs Rust comparison

| Factor | C (this option) | Rust (PyO3) |
|--------|----------------|-------------|
| **Runtime performance** | Equivalent | Equivalent |
| **Compile on Pi 4** | ~5–10 seconds | ~15–30 minutes |
| **Proven correctness** | readsb reference available | Must reimplement and validate |
| **Toolchain** | `gcc` (already on Pi) | `rustc` + `cargo` + `maturin` |
| **Memory safety** | Manual (but readsb code is battle-tested) | Guaranteed by compiler |
| **Python binding** | `cffi` (mature, stable ABI) | PyO3 (good, but version-coupled) |
| **Cross-compile** | Trivial (`arm-linux-gnueabihf-gcc`) | Requires `cross` crate + Docker |
| **Ecosystem** | C is universal | Rust less common in ADS-B space |

### Effort: 3–5 days (B1) or 1–2 weeks (B2)

---

## Option C — Consume readsb JSON (recommended)

### What it is

Read decoded aircraft state directly from readsb's JSON output files instead of
(or in addition to) decoding the raw Beast TCP stream in Python. This
eliminates the Python decode bottleneck entirely — readsb's C decoder handles
all Mode-S/ADS-B decoding at native speed.

### Data sources

All files below are atomically updated by readsb (write to temp, rename).

#### `/run/readsb/aircraft.json` (updated every ~1 second)

Top-level keys: `now` (unix epoch float), `messages` (total since start), `aircraft[]`.

Per-aircraft fields available (key omitted if no data):

| Field | Type | Maps to our field | Notes |
|-------|------|-------------------|-------|
| `hex` | string | `icao` | May start with `~` for non-ICAO (TIS-B) |
| `type` | string | `mlat` flag + source | `adsb_icao`, `mlat`, `mode_s`, `tisb_icao`, etc. |
| `flight` | string | `callsign` | 8 chars, space-padded |
| `alt_baro` | int\|"ground" | `altitude` | Barometric; "ground" = on surface |
| `alt_geom` | int | **new field** | Geometric (GNSS) altitude — we don't currently have this |
| `gs` | float | — | Ground speed (we currently don't publish this) |
| `ias` | int | `airspeed_kts` (when type=IAS) | Indicated airspeed |
| `tas` | int | `airspeed_kts` (when type=TAS) | True airspeed |
| `mach` | float | `mach` | Mach number |
| `track` | float | — | True track over ground (**new**) |
| `track_rate` | float | — | Rate of change of track (**new**) |
| `roll` | float | — | Roll angle (**new**) |
| `mag_heading` | float | `heading_deg` | Magnetic heading |
| `true_heading` | float | — | True heading (**new**) |
| `baro_rate` | int | `vertical_rate_fpm` | Barometric vertical rate |
| `geom_rate` | int | — | Geometric vertical rate (**new**) |
| `squawk` | string | `squawk` | 4-digit octal |
| `emergency` | string | — | `none`, `general`, `lifeguard`, etc. (**new**) |
| `category` | string | `type_category` | Emitter category A0–D7 |
| `nav_qnh` | float | — | Altimeter setting hPa (**new**) |
| `nav_altitude_mcp` | int | `selected_alt` | MCP/FCU selected altitude |
| `nav_altitude_fms` | int | — | FMS selected altitude (**new**) |
| `nav_heading` | float | — | Selected heading (**new**) |
| `nav_modes` | string[] | — | `autopilot`, `vnav`, `tcas`, etc. (**new**) |
| `lat`, `lon` | float | `lat`, `lon` | Decimal degrees |
| `nic` | int | — | Navigation Integrity Category (**new**) |
| `rc` | int | — | Radius of Containment, metres (**new**) |
| `seen_pos` | float | `last_pos_age` | Seconds since last position |
| `seen` | float | `age` | Seconds since last message |
| `rssi` | float | `signal` | dBFS (always negative); replaces raw Beast 0–255 |
| `messages` | int | `msg_count` | Total messages from this aircraft |
| `mlat` | string[] | — | List of MLAT-derived field names |
| `tisb` | string[] | — | List of TIS-B-derived field names |
| `acas_ra` | object | ACAS fields | Experimental; structure may change |
| `version` | int | — | ADS-B version 0/1/2 (**new**) |
| `nic_baro` | int | — | NIC for baro altitude (**new**) |
| `nac_p`, `nac_v` | int | — | Navigation accuracy (**new**) |
| `sil`, `sil_type` | int, string | — | Source integrity (**new**) |
| `gva`, `sda` | int | — | Geometric vertical accuracy, system design assurance (**new**) |
| `alert`, `spi` | int | — | Alert/SPI bits (**new**) |
| `wd`, `ws` | int | — | Derived wind direction/speed (**new**) |
| `oat`, `tat` | int | — | Derived temperatures (**new**) |
| `r` | string | `registration` | With `--db-file` |
| `t` | string | `type_code` | With `--db-file` |
| `desc` | string | `type_full_name` | With `--db-file-lt` |
| `dbFlags` | int | `military`, `interesting` | Bitfield: `& 1` = military, `& 2` = interesting |

#### `/run/readsb/receiver.json` (updated infrequently)

| Field | Type | Use |
|-------|------|-----|
| `version` | string | readsb version — display in status page |
| `refresh` | int | aircraft.json update interval (ms) |
| `lat`, `lon` | float | Receiver position — could auto-populate `RECEIVER_LAT`/`RECEIVER_LON` |

#### `/run/readsb/stats.json` (updated every ~1 minute)

Five period buckets: `latest`, `last1min`, `last5min`, `last15min`, `total`.
Each contains:

| Section | Key fields | Maps to |
|---------|-----------|---------|
| `local.signal` | Mean signal dBFS | `signal_avg` in minute_stats |
| `local.peak_signal` | Peak signal dBFS | `signal_max` in minute_stats |
| `local.noise` | Noise floor dBFS | **new** — noise floor metric |
| `local.strong_signals` | Count of msgs > -3 dBFS | **new** — saturation indicator |
| `local.accepted[]` | Messages accepted by error-correction level | Total message count |
| `local.modes` | Mode-S preambles received | Raw decode attempts |
| `local.bad` | Invalid preambles | Error rate metric |
| `local.blocks_processed` | SDR sample blocks | SDR health |
| `local.blocks_dropped` | Dropped sample blocks | **CPU overload indicator** |
| `remote.*` | Same structure for network-fed messages | Network feed stats |
| `cpu.demod` | Demodulation CPU ms | readsb CPU usage |
| `cpu.reader` | USB reader CPU ms | SDR I/O cost |
| `cpu.background` | Network/periodic CPU ms | readsb overhead |
| `cpr.global_ok` | Successful global CPR decodes | Position quality |
| `cpr.global_bad` | Rejected global positions | Position errors |
| `cpr.global_speed` | Speed-check failures | Speed gate rejects |
| `cpr.local_ok` | Successful local CPR decodes | Fallback positions |
| `cpr.filtered` | CPR messages filtered (bad transponder) | Transponder issues |
| `tracks.all` | Total tracks created | Unique aircraft |
| `tracks.single_message` | Single-message tracks (likely errors) | Ghost/phantom rate |
| `messages` | Total messages in period | msg/sec calculation |

#### `/run/airspy_adsb/stats.json` (airspy_adsb systems only)

Based on the graphs1090 collectd plugin, this file provides:

| Field | Type | Use |
|-------|------|-----|
| `rssi` | quartile object (`min`, `p5`, `q1`, `median`, `q3`, `p95`, `max`) | Signal distribution — richer than readsb's single mean |
| `snr` | quartile object | Signal-to-noise ratio distribution (**new**) |
| `noise` | quartile object | Noise floor distribution (**new**) |
| `preamble_filter` | int | Preamble filter count |
| `samplerate` | int | ADC sample rate |
| `gain` | float | Current gain setting (useful for auto-gain monitoring) |
| `lost_buffers` | int | USB buffer overruns — **SDR health indicator** |
| `max_aircraft_count` | int | Peak aircraft count in period |
| `df_counts` | dict (DF → count) | Per-DF message counts — **maps directly to our `df_history`** |
| `now` | float | Timestamp |

### Architecture: hybrid ingest mode

```
INGEST_MODE=beast        → current behaviour (raw Beast TCP, Python decode)
INGEST_MODE=readsb       → poll aircraft.json + stats.json, no Beast connection
INGEST_MODE=hybrid       → Beast TCP for MLAT/ACAS/DF-level, readsb JSON for position/EHS
```

#### `readsb` mode implementation

```
                    ┌─────────────────────────────────┐
                    │  /run/readsb/aircraft.json       │
                    │  (atomically updated every ~1s)  │
                    └──────────────┬──────────────────┘
                                   │ file read (mmap or open/read)
                                   ▼
                    ┌─────────────────────────────────┐
                    │  readsb_ingest.py                │
                    │  - json.load() or orjson          │
                    │  - diff against previous snapshot │
                    │  - map fields → Aircraft objects  │
                    │  - compute bearing/range          │
                    └──────────────┬──────────────────┘
                                   │ direct write (no queue needed)
                                   ▼
                    ┌─────────────────────────────────┐
                    │  aircraft_state.py               │
                    │  - _aircraft dict updated        │
                    │  - enrichment triggered          │
                    │  - minute stats computed         │
                    │  - coverage/hires sampling       │
                    └─────────────────────────────────┘
```

Key design points:
- **No message queue**: we read the entire aircraft state once per second, not per-message. The queue/batch/pressure system becomes unnecessary.
- **No CPR decode**: readsb already decoded positions. We just read lat/lon.
- **No lock contention**: the ingest read and snapshot build can share a single-threaded async loop (no GIL/lock concerns).
- **Stats supplementation**: read `stats.json` once per minute for SDR health, CPR quality, and message counts. Read `airspy_adsb/stats.json` if present.

#### `hybrid` mode implementation

Keep Beast TCP running for:
- MLAT multi-source fusion (timestamp marker detection, per-source attribution)
- ACAS RA decode (DF0/DF16 — readsb's `acas_ra` field is experimental)
- Per-message DF counters (or use airspy_adsb `df_counts` if available)
- Raw Beast RSSI (if per-message signal granularity is needed)

Use readsb JSON for:
- All position data (eliminates CPR decode CPU)
- EHS fields (IAS, TAS, Mach, heading, selected alt, nav modes)
- Signal average (dBFS from `rssi`)
- Additional fields not available from Beast (alt_geom, track, roll, wind, temp, NIC/NAC/SIL, emergency)

### Field mapping — what changes

| Our field | Beast source | readsb source | Notes |
|-----------|-------------|---------------|-------|
| `signal` | Raw 0–255, converted to dBFS | `rssi` (already dBFS) | **Semantic change**: Beast gives per-message instant; readsb gives recent average |
| `altitude` | Our 8-layer filter | `alt_baro` | readsb has its own altitude filter; we lose custom gating |
| `lat`, `lon` | Our CPR decode + speed gate | `lat`, `lon` | readsb's CPR is more mature; we lose `pos_reliable` scoring |
| `vertical_rate_fpm` | BDS 6.0 decode | `baro_rate` | Direct mapping |
| `heading_deg` | BDS 6.0 decode | `mag_heading` | Direct mapping |
| `airspeed_kts` | BDS 5.0/6.0 | `ias` or `tas` | Need to set `airspeed_type` from which field is present |
| `msg_count` | Our per-aircraft counter | `messages` | readsb counts from its start; we'd track delta for per-session |
| `mlat` | Timestamp marker detection | `type == "mlat"` or `"lat" in mlat_list` | Loses per-source attribution |
| `type_category` | Not decoded from Beast | `category` | **Gain**: we don't currently decode this from raw messages |

### Expected performance gain

- **Decode CPU: eliminated**. No pyModeS calls, no CPR trig, no BDS detection, no CRC checks.
- **No message queue**: no queue pressure, no drops, no batch tuning.
- **JSON parse cost**: `orjson.loads()` on a ~200-aircraft aircraft.json ≈ 1–3 ms on Pi 4.
- **Field mapping**: simple dict iteration, ~1–2 ms for 200 aircraft.
- **Total ingest cost: ~3–5 ms/cycle** vs current ~50–200 ms/cycle at full load.
- **Broadcast unchanged**: snapshot/serialize/WebSocket path is identical.
- Pi 4 should comfortably handle 300+ aircraft with headroom.

### What's lost (readsb-only mode)

| Capability | Impact | Mitigation |
|-----------|--------|------------|
| Per-message timing instrumentation | Cannot measure decode latency | Not needed — readsb handles decode |
| Custom DF-level counters | No per-DF histogram from our decode | Use `airspy_adsb/stats.json` `df_counts` or readsb `stats.json` accepted[] |
| MLAT per-source attribution | Cannot do multi-source fusion/scoring | Loses `MlatScorecard` quality data; MLAT positions still available |
| Beast raw RSSI (0–255 per message) | Lose per-message signal granularity | readsb `rssi` is a recent average in dBFS; stats.json has signal/peak/noise |
| Our altitude reliability filter | readsb's own filter applies instead | readsb's filter is well-tested; unlikely regression |
| Our CPR reliability scoring | readsb's own CPR validation applies | readsb's CPR is generally superior |
| ACAS RA decode (DF0/DF16) | readsb `acas_ra` is experimental | Use hybrid mode to keep Beast for ACAS |
| `pos_reliable` / `pos_confident` flags | Cannot expose our internal confidence | Could derive from NIC/NAC instead |
| ICAO confirmation filter | readsb handles this internally | readsb's filter is equivalent |
| Single-bit DF17 error recovery | readsb handles this internally | readsb does this in C |
| `signal` as 0–255 integer | Frontend assumes 0–255 scale | Convert dBFS → 0–255 scale: `raw = clamp(-rssi * 2, 0, 255)` |

### What's gained (fields not available from Beast)

| Field | Source | Value |
|-------|--------|-------|
| `alt_geom` | aircraft.json | Geometric altitude — useful for MLAT/GPS comparison |
| `gs` (ground speed) | aircraft.json | True ground speed (vs derived airspeed) |
| `track` / `track_rate` | aircraft.json | True track and turn rate |
| `roll` | aircraft.json | Bank angle |
| `true_heading` | aircraft.json | True heading (vs magnetic) |
| `geom_rate` | aircraft.json | Geometric vertical rate |
| `emergency` | aircraft.json | Structured emergency status (vs raw squawk) |
| `nav_qnh` | aircraft.json | Altimeter setting |
| `nav_altitude_fms` | aircraft.json | FMS selected altitude |
| `nav_heading` | aircraft.json | Selected heading |
| `nav_modes` | aircraft.json | Autopilot/VNAV/LNAV/TCAS engagement |
| `nic`, `rc` | aircraft.json | Position integrity metrics |
| `nac_p`, `nac_v` | aircraft.json | Navigation accuracy |
| `sil`, `gva`, `sda` | aircraft.json | ADS-B quality indicators |
| `wd`, `ws` | aircraft.json | Derived wind direction/speed |
| `oat`, `tat` | aircraft.json | Derived air temperature |
| `version` | aircraft.json | ADS-B transponder version |
| `r`, `t`, `desc`, `dbFlags` | aircraft.json (with --db-file) | Registration, type, military/interesting flags — free enrichment |
| Noise floor | stats.json `local.noise` | SDR noise floor monitoring |
| CPU overload | stats.json `local.blocks_dropped` | SDR drop detection |
| CPR quality | stats.json `cpr.*` | Position decode success/failure rates |
| SNR distribution | airspy_adsb/stats.json | Signal-to-noise quartiles |

### Detrimental factors

- **Co-location required**: readsb must be on the same machine (or JSON files must be network-mounted). Not viable for remote Beast feeds.
- **Update latency**: aircraft.json is written every ~1 second. Our Beast decode processes messages in real-time (~10 ms latency). JSON polling adds up to 1 second of staleness. For display purposes this is negligible; for ACAS RA alerting it matters.
- **readsb dependency**: if readsb crashes or stops writing, we get stale data. Need file-age watchdog.
- **MLAT quality regression**: losing per-source MLAT attribution breaks the MlatScorecard and multi-source fusion. The hybrid mode mitigates this.
- **ACAS coverage**: readsb's `acas_ra` field is marked experimental. If we rely on it, ACAS event recording could be less reliable than our DF16 decoder. Hybrid mode mitigates.
- **Schema changes**: readsb is actively developed; field names or semantics could change. Need to code defensively (`.get()` everywhere, version check).
- **Signal scale change**: our entire frontend and DB assume Beast RSSI (0–255, 0=strongest). Switching to dBFS requires a conversion layer and potentially a DB migration for historical signal comparisons.
- **Lose visit msg_count accuracy**: readsb `messages` is cumulative from start; we'd need to track deltas and handle readsb restarts.
- **No custom DF routing**: we can't add new DF-specific logic (e.g. future BDS registers) without going back to Beast.

### Implementation steps

1. New file: `backend/readsb_ingest.py` — file reader + JSON parser + field mapper
2. New config: `INGEST_MODE` (beast/readsb/hybrid), `READSB_JSON_DIR` (default `/run/readsb`)
3. Field mapper: readsb aircraft dict → our Aircraft dataclass, handling type conversions
4. Signal conversion: dBFS → 0–255 for backward compatibility (or migrate to dBFS throughout)
5. Stats reader: parse `stats.json` once/minute, populate `minute_stats` table
6. Optional: airspy_adsb stats reader for DF counts, SNR, gain
7. Auto-detect receiver position from `receiver.json`
8. Modify `main.py`: choose ingest path based on `INGEST_MODE`
9. For hybrid: keep Beast TCP for MLAT streams + ACAS, merge with readsb positions
10. File-age watchdog: if aircraft.json older than 5 seconds, log warning; if > 30s, reconnect Beast

### Effort: 2–3 days (readsb mode), +1–2 days (hybrid mode)

---

## Comparison matrix

| Factor | Option A (Cython) | Option B (C extension) | Option C (readsb JSON) |
|--------|------------------|----------------------|----------------------|
| **Decode speedup** | 2–4x | 5–10x (50–100x per function) | ∞ (eliminated) |
| **Effort** | 3–5 days | 3–5 days (B1) / 1–2 weeks (B2) | 2–5 days |
| **Build complexity** | Moderate (Cython) | Low (`gcc`, already on Pi) | None |
| **Compile on Pi 4** | ~1–2 min | ~5–10 seconds | N/A |
| **Maintenance** | Forked pyModeS | C decode library | Field mapping only |
| **Proven decode correctness** | pyModeS (Python) | readsb (reference impl) | readsb (delegated) |
| **Works with remote Beast** | Yes | Yes | No (co-location) |
| **MLAT fusion preserved** | Yes | Yes | No (hybrid: partial) |
| **ACAS preserved** | Yes | Yes | No (hybrid: yes) |
| **New fields gained** | None | None | 20+ new fields |
| **Per-message timing** | Yes | Yes | No |
| **Beast RSSI granularity** | Yes | Yes | No (average only) |
| **Risk of regressions** | Low | Low (battle-tested C code) | Medium (field mapping) |
| **Benefits non-Pi hosts** | Yes (minor) | Yes (minor) | Yes (eliminates decode everywhere) |
| **License constraint** | None | GPLv3+ (B1) or none (B2) | None |

---

## Recommendation

**Option C (readsb JSON) as the primary path**, with hybrid mode for ACAS/MLAT
retention. Rationale:

1. It solves the root cause (Python decode is slow on Pi) rather than making it faster
2. It adds 20+ new data fields that enrich the dashboard
3. It's the least maintenance long-term — no compiled extensions to build/ship
4. The hybrid mode preserves MLAT fusion and ACAS decode where needed
5. The Beast-only path remains for remote feeds or non-readsb setups

**Option B (C extension, sub-approach B1) as the recommended fallback** if
co-location isn't available or Beast-level fidelity is required everywhere.
It provides the highest decode performance while keeping the full pipeline,
compiles in seconds on the Pi, and leverages readsb's battle-tested decode
logic rather than reimplementing from scratch.

Option A (Cython) is the least disruptive change but offers the smallest
gain — only worth considering if both B and C are ruled out.

---

## Implementation Plan — Option C + Option B

> **Status**: Phase 0 ✅ complete · Phase 1 ✅ complete · Phase 2 ✅ complete · Phase 3 ⬜ next

**Decision**: Implement Option C (readsb JSON) as primary ingest for co-located
receivers, and Option B (C extension) for remote Beast feeds. Both share a
common abstraction layer.

### Phase 0 — Ingest abstraction layer ✅

**Goal**: Decouple `main.py` from Beast-specific startup so either ingest mode
plugs in cleanly.

**0a. New config vars (`config.py`)**

```python
INGEST_MODE           = "beast"          # beast | readsb | hybrid
READSB_JSON_DIR       = "/run/readsb"    # path to readsb output directory
AIRSPY_STATS_PATH     = "/run/airspy_adsb/stats.json"  # optional
READSB_POLL_INTERVAL_S = 1.0            # match readsb write frequency
```

Effort: 0.5 day

**0b. New AircraftState method (`aircraft_state.py`)**

Add `update_from_json(aircraft_list, now, total_messages)` alongside existing
`process_messages_batch()`. This is the readsb-mode entry point:

- New aircraft: create Aircraft object, trigger enrichment queue
- Existing aircraft: update changed fields only
- Missing aircraft: leave for existing timeout/expiry
- Signal conversion: dBFS → 0–255 (`clamp(int(-rssi * 2), 0, 255)`)
- Bearing/range: compute from lat/lon + `RECEIVER_LAT`/`RECEIVER_LON`
- Minute stats: derive msg/sec from `total_messages` delta between polls
- MLAT flag: `type == "mlat"` or `"lat" in aircraft.get("mlat", [])`
- Per-second counters: estimate from message delta / elapsed time

Effort: 1 day

---

### Phase 1 — Option C: readsb JSON ingest ✅

**1a. New file: `backend/readsb_ingest.py`**

JSON file poller + field mapper:

- Async loop: read `aircraft.json` every `READSB_POLL_INTERVAL_S`
- Parse with `orjson.loads()` (~1–3 ms for 200 aircraft on Pi 4)
- Diff against previous snapshot (detect new/updated/gone aircraft)
- Call `state.update_from_json(aircraft_list, now, total_messages)`
- File-age watchdog: warn if >5s stale, error if >30s
- Auto-detect receiver position from `receiver.json` on startup
- No message queue, no decoder thread — single async task

Field mapping:

| readsb field | → Aircraft field | Conversion |
|-------------|-----------------|------------|
| `hex` | `icao` | Strip `~` prefix for TIS-B |
| `flight` | `callsign` | `.strip()` |
| `alt_baro` | `altitude` | `None` if `"ground"` |
| `rssi` | `signal` | `clamp(int(-rssi * 2), 0, 255)` |
| `seen` | `last_seen` | `now - seen` |
| `seen_pos` | `last_pos_ts` | `now - seen_pos` |
| `messages` | `msg_count` | Track delta for per-session count |
| `type` | `mlat` flag | `True` if `"mlat"` |
| `lat`, `lon` | `lat`, `lon` | Direct; compute bearing/range |
| `ias` / `tas` | `airspeed_kts` + type | Prefer TAS; set `airspeed_type` |
| `mag_heading` | `heading_deg` | Direct |
| `baro_rate` | `vertical_rate_fpm` | Direct |
| `nav_altitude_mcp` | `selected_alt` | Direct |
| `mach` | `mach` | Direct |
| `squawk` | `squawk` | Direct |
| `category` | `type_category` | Direct |
| `r` | `registration` | If readsb `--db-file` active |
| `t` | `type_code` | If readsb `--db-file` active |
| `desc` | `type_full_name` | If readsb `--db-file-lt` active |
| `dbFlags & 1` | `military` | Bitfield extract |

Effort: 1 day

**1b. New file: `backend/readsb_stats.py`**

Stats supplementation — read once per minute:

From `/run/readsb/stats.json`:
- `last1min.local.accepted[]` → total message count → msg/sec
- `last1min.local.signal` → signal_avg for minute_stats
- `last1min.local.peak_signal` → signal_max
- `last1min.local.noise` → noise floor (new metric)
- `last1min.local.strong_signals` → saturation count (new)
- `last1min.local.blocks_dropped` → SDR overload indicator (new)
- `last1min.cpr.*` → CPR quality metrics (new)
- `last1min.tracks.*` → track/ghost counts (new)
- `last1min.cpu.*` → readsb CPU usage (new)
- `total.messages` → cumulative message count

From `/run/airspy_adsb/stats.json` (if present):
- `df_counts` → per-DF histogram → populate `df_history`
- `rssi` quartiles → richer signal distribution (new)
- `snr` quartiles → signal-to-noise (new)
- `noise` quartiles → noise floor distribution (new)
- `gain` → current SDR gain (new)
- `lost_buffers` → USB overrun count (new)
- `max_aircraft_count` → peak aircraft (new)

Effort: 0.5 day

**1c. Wire into `main.py` lifespan**

Modify `lifespan()` startup:

```
if INGEST_MODE == "beast":
    # Current path: Beast runner + MLAT runners + decoder thread
    _start_msg_processor()
    _bg(_beast_runner())
    for mlat in MLAT_SERVERS: _bg(_mlat_runner(...))

elif INGEST_MODE == "readsb":
    # No queue, no decoder thread
    _bg(_readsb_poller())       # from readsb_ingest.py
    _bg(_readsb_stats_poller()) # from readsb_stats.py

elif INGEST_MODE == "hybrid":
    # readsb for positions + Beast for MLAT/ACAS only
    _bg(_readsb_poller())
    _bg(_readsb_stats_poller())
    _start_msg_processor()      # decoder thread for MLAT messages
    for mlat in MLAT_SERVERS: _bg(_mlat_runner(...))
    # No _beast_runner() — positions come from readsb
```

Push/broadcast loop unchanged — calls `get_snapshot()` regardless of mode.
Queue pressure monitoring skipped when `INGEST_MODE != "beast"`.

Effort: 0.5 day

**1d. New Aircraft dataclass fields + snapshot updates**

Add fields available only from readsb (optional, `None` when in Beast mode):

```python
# Geometric
alt_geom: Optional[int] = None
gs: Optional[float] = None              # ground speed
track: Optional[float] = None           # true track
track_rate: Optional[float] = None
roll: Optional[float] = None
true_heading: Optional[float] = None
geom_rate: Optional[int] = None

# Navigation
emergency: Optional[str] = None
nav_qnh: Optional[float] = None
nav_altitude_fms: Optional[int] = None
nav_heading: Optional[float] = None
nav_modes: Optional[list] = None

# ADS-B quality
nic: Optional[int] = None
rc: Optional[int] = None
nac_p: Optional[int] = None
nac_v: Optional[int] = None
sil: Optional[int] = None
gva: Optional[int] = None
sda: Optional[int] = None
adsb_version: Optional[int] = None

# Derived
wind_dir: Optional[int] = None
wind_speed: Optional[int] = None
oat: Optional[int] = None
tat: Optional[int] = None
```

Update `get_snapshot()` to include these fields (mode-dependent — only
populated when source provides them). No frontend changes required yet;
fields appear in JSON when non-null.

Effort: 0.5 day

**Option C functional: ~3.5 days total**

---

### Phase 2 — Option B: C decode extension ✅

**2a. Extract C files + create `decode_types.h`**

New directory: `backend/native/`

Extract from readsb (sub-approach B1, GPLv3+):
- `crc.c` / `crc.h`
- `cpr.c` / `cpr.h`
- `mode_s.c` / `mode_s.h`
- `comm_b.c` / `comm_b.h`
- `icao_filter.c` / `icao_filter.h`
- `ais_charset.c` / `ais_charset.h`

Create `decode_types.h`:
- Minimal `struct modesMessage` (decoded output fields)
- Enums for DF types, address types, CPR types
- Config struct replacing `Modes` global (~200 lines)
- Replace `cmalloc`/`sfree` with `malloc`/`free`
- Replace `addrHash` with inline hash
- Stub ~12 `Modes.*` references in `mode_s.c`

Effort: 1 day

**2b. Create `decode_api.c` + Makefile**

Single C entry point wrapping the decode pipeline:

```c
struct decode_result decode_beast_message(
    const char *hex_msg,    // uppercase hex payload
    int signal,             // Beast RSSI 0–255
    uint64_t timestamp,     // 6-byte Beast timestamp
    int msg_type            // 0x32 (short) or 0x33 (long)
);
```

Returns flat struct with all decoded fields:
- `df`, `icao`, `crc_residual`, `crc_ok`
- `callsign`, `altitude`, `squawk`, `category`
- `lat`, `lon`, `cpr_odd`, `cpr_even`, `cpr_oe_flag`
- `ias`, `tas`, `mach`, `heading`, `vrate`, `selected_alt`
- `acas_ra_active`, `acas_sensitivity`, `acas_threat_icao`
- `is_mlat` (from timestamp marker detection)

Makefile:
```makefile
CC = gcc
CFLAGS = -O2 -Wall -std=c11 -fPIC
LDFLAGS = -shared -lm
libdecode.so: crc.o cpr.o mode_s.o comm_b.o icao_filter.o ais_charset.o decode_api.o
	$(CC) $(LDFLAGS) -o $@ $^
```

Effort: 1 day

**2c. Python `cffi` wrapper + integration**

New file: `backend/native_decode.py`

- Load `libdecode.so` via `cffi` ABI mode (no compile-time Python dependency)
- `decode_message(raw_hex, signal, timestamp, msg_type) → dict`
- Maps C struct fields → Python dict matching pyModeS output format

Integration in `aircraft_state.py`:
```python
try:
    from native_decode import decode_message as _c_decode
    _USE_NATIVE = True
except (ImportError, OSError):
    _USE_NATIVE = False  # fall back to pyModeS
```

`_decode()` branches: if `_USE_NATIVE`, call C decode and map result;
else existing pyModeS path. Everything downstream unchanged.

Effort: 1 day

**Option B functional: complete**

---

### Phase 3 — Hybrid mode

Wire up the combined path (already partially covered in Phase 1c):

- readsb poller updates positions, EHS, callsign, squawk, all new fields
- Beast MLAT runners feed queue → decoder thread → `process_messages_batch()`
- MLAT per-source attribution preserved (timestamp marker + `mlat_source` param)
- ACAS DF0/DF16 decoded from MLAT Beast streams (these carry all DF types)
- Merge logic: readsb position takes priority over MLAT position; MLAT
  position used only if readsb has no position for that aircraft
- `MlatScorecard` continues to work from Beast MLAT data

Effort: 1 day

---

### Phase 4 — Testing & validation

1. **C extension parity**: capture raw Beast data → decode with both pyModeS
   and C extension → compare all output fields. Automated diff script.
2. **readsb field mapping**: run Beast mode and readsb mode simultaneously
   on same receiver → compare snapshot output for each aircraft.
3. **Performance benchmark**: measure ingest cycle time per mode at 200 ac:
   - Beast + pyModeS (baseline)
   - Beast + C extension
   - readsb JSON
   - Hybrid
4. **Degradation**: stop readsb → verify watchdog triggers, logs warning.
   Optionally fall back to Beast if available.
5. **Signal consistency**: verify dBFS → 0–255 conversion produces values
   comparable to Beast RSSI for same aircraft.

Effort: 1–2 days

---

### Schedule summary

| Step | What | Depends on | Effort |
|------|------|-----------|--------|
| 0a | Config vars | — | 0.5 day |
| 0b | `update_from_json()` | 0a | 1 day |
| 1a | `readsb_ingest.py` | 0b | 1 day |
| 1b | `readsb_stats.py` | 0a | 0.5 day |
| 1c | `main.py` lifespan wiring | 0b, 1a | 0.5 day |
| 1d | New Aircraft fields + snapshot | 0b | 0.5 day |
| | **Option C functional** | | **~3.5 days** |
| 2a | Extract C files + `decode_types.h` | — | ✅ |
| 2b | `decode_api.c` + Makefile | 2a | ✅ |
| 2c | `cffi` wrapper + integration | 2b | ✅ |
| WS | Per-client queue (broadcast fix) | — | ✅ |
| Dep | install.sh update mode + deployment fixes | — | ✅ |
| | **Option B functional** | | **✅ complete** |
| 3 | Hybrid mode wiring | 1c | ⬜ next |
| 4 | Testing & validation | all | ⬜ |

Phases 1 (Option C) and 2 (Option B) are independent — can be worked in
parallel. Phase 3 requires Phase 1c. Phase 4 requires all.
