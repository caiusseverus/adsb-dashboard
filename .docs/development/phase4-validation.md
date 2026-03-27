# Phase 4 — Validation & Testing

Date: 2026-03-27
Last updated: 2026-03-27
Status: **In progress** — D1–D3 complete; T1–T5 scripts written; T1–T5 pending live run; T6 pending high traffic

Prerequisite: Phase 3 (hybrid mode) complete and deployed. Full-load traffic (165–200 aircraft) needed for T6 only; all other tests can be run at any traffic level.

---

## Goals

1. Confirm the C extension produces bit-identical output to pyModeS on all DF types.
2. Confirm readsb JSON field mapping is correct and complete relative to Beast mode.
3. Confirm hybrid mode correctly merges readsb positions with Beast MLAT/ACAS data.
4. Confirm all watchdogs, fallbacks, and error paths behave as designed.
5. Establish a baseline performance benchmark across all four ingest paths.
6. Resolve deferred items from Phases 1–3 before moving on.

---

## Implementation progress

### ✅ D1 — `get_aircraft_live()` updated with new fields (complete)

**Commit:** `703a76e`
**File:** `backend/aircraft_state.py`
All 26 readsb fields (alt_geom, gs, track, roll, nav_modes, NIC/NAC/SIL, etc.) added to
`get_aircraft_live()` return dict. Null in Beast mode, populated in readsb/hybrid.

### ✅ D2 — `readsb_stats` wired into `minute_stats` DB (complete)

**Commit:** `703a76e`
**Files:** `backend/db.py`, `backend/readsb_stats.py`
New columns added to `minute_stats` with auto-migration on startup:
`noise_dbfs`, `blocks_dropped`, `cpr_global_ok`, `cpr_global_bad`, `cpr_local_ok`,
`tracks_all`, `tracks_single_msg`.
`readsb_stats_poller()` calls `stats_db.write_readsb_stats()` after each 60s poll via
`UPDATE` (no-op if the minute row hasn't been written yet — harmless).

### ✅ D3 — Queue telemetry gated behind `INGEST_MODE == "beast"` (complete)

**Commit:** `703a76e`
**File:** `backend/main.py`
Queue depth sampling in `_push_updates` now skipped in readsb/hybrid modes.
Perf telemetry no longer reports misleading zeros.

### ✅ T1 — C extension parity script (complete)

**Commit:** `cc1089c`
**Script:** `tools/parity_check.py`
Reads a captured Beast binary file, decodes each frame with both pyModeS and the C
extension, compares all output fields. CRC-corrected frames that pyModeS rejects are
counted separately (expected, not a failure).

**To run:**
```bash
# Capture corpus (60–120 s):
nc <BEAST_HOST> 30005 > corpus.beast

# Run from backend/:
cd backend && uv run python ../tools/parity_check.py ../corpus.beast
cd backend && uv run python ../tools/parity_check.py ../corpus.beast --verbose
```

**Status:** Script written and imports verified. Pending live corpus capture and run.

### ✅ T2 — readsb vs Beast snapshot diff script (complete)

**Commit:** `7965191`
**Script:** `tools/snapshot_diff.py`
Polls `/api/stats` from two instances every 5 s, matches aircraft by ICAO, reports
per-field divergences with per-field tolerances. Checks readsb-only fields are non-null
when `pos_confident=True`. Aircraft younger than `--min-age` (default 10 s) skipped.

**To run:**
```bash
# Start second instance in readsb mode:
INGEST_MODE=readsb cd backend && uv run uvicorn main:app --port 8001

# Run diff (12 rounds = 60 s):
python tools/snapshot_diff.py --rounds 12

# Continuous with per-aircraft detail:
python tools/snapshot_diff.py --verbose
```

**Status:** Script written. Pending live two-instance run.

### ✅ T3 — Hybrid mode validation script (complete)

**Commit:** `73356a3`
**Script:** `tools/hybrid_check.py`
Checks a running hybrid-mode instance for: readsb-only fields populated on positioned
aircraft, MLAT aircraft have `mlat_source`, `/api/mlat/fixes` returns data, Beast queue
not accumulating (D3 fix), broadcast within 50 ms target. Runs N rounds with configurable
interval.

**To run:**
```bash
INGEST_MODE=hybrid cd backend && uv run uvicorn main:app --reload
python tools/hybrid_check.py [--url URL] [--rounds 3]
```

**Status:** Script written. Pending live hybrid-mode run with active MLAT server.

### ✅ T4 — Watchdog and degradation test runner (complete)

**Commit:** `c08a4e1`
**Script:** `tools/watchdog_check.py`
- **T4c** (automated): verifies `ValueError` raised on unknown `INGEST_MODE`. **Confirmed passing.**
- **T4a** (guided): prompts tester to stop readsb, checks aircraft table freezes, verifies log
  warnings fire within 5 s (WARNING) and 30 s (ERROR), verifies recovery after restart.
- **T4b** (guided): prompts tester to drop Beast TCP, verifies reconnect logged, verifies
  readsb positions continue in hybrid mode during Beast outage.

**To run:**
```bash
# Automated only (T4c):
cd backend && uv run python ../tools/watchdog_check.py --auto-only

# All tests including guided:
cd backend && uv run python ../tools/watchdog_check.py
```

**Status:** T4c confirmed passing. T4a/T4b pending live guided run.

### ✅ T5 — Signal conversion consistency script (complete)

**Commit:** `e3be8c6`
**Script:** `tools/signal_check.py`
- Formula verification (no live system needed): checks `clamp(int(-rssi_dbfs * 2), 0, 255)`
  against known dBFS→beast_equiv pairs. **Confirmed passing.**
- Single-instance mode: validates signal range, no systematic clamping.
- Two-instance mode: compares signal values across beast and readsb instances, per-aircraft
  table, mean delta across rounds. Tolerance: ±30 units (per-message vs rolling average).

**To run:**
```bash
# Formula check only (always works):
python tools/signal_check.py --single http://localhost:9999  # (URL need not be live)

# Single instance:
python tools/signal_check.py --single http://localhost:8000

# Two instances:
python tools/signal_check.py --beast http://localhost:8000 --readsb http://localhost:8001
```

**Status:** Formula verification confirmed passing. Live comparison pending two-instance run.

---

## Test areas

### T1 — C extension decode parity

**Script:** `tools/parity_check.py`

Fields compared per DF type:

| DF | Fields compared |
|----|-----------------|
| DF17/18 (ADS-B) | `df`, `icao`, `crc_ok`, `callsign` (tc1–4), `baro_alt` (tc9–22), `cpr_odd/lat/lon` (tc9–22), `heading`, `speed` (tc19) |
| DF4/20 | `df`, `icao`, `crc_ok`, `baro_alt` |
| DF5/21 | `df`, `icao`, `crc_ok`, `squawk` |
| DF11 | `df`, `icao`, `crc_ok` |
| DF0/16 | `df`, `icao`, `crc_ok`, `acas_ra_valid` |

Tolerance: exact match on all fields except `heading` (±1°), `speed` (±2 kt), `baro_alt` (±25 ft).
CRC-corrected frames rejected by pyModeS are counted separately — not a failure.

**Pass criteria:** Zero unexpected divergences. Exit code 0.

---

### T2 — readsb vs Beast field mapping

**Script:** `tools/snapshot_diff.py`

Fields compared with tolerances:

| Field | Tolerance | Notes |
|-------|-----------|-------|
| `icao`, `callsign`, `squawk`, `military`, `mlat` | exact | |
| `altitude` | ±25 ft | different filter chains |
| `signal` | ±30 units | per-message vs rolling average |
| `lat`, `lon` | ±0.002° | CPR decode timing |
| `heading_deg` | ±2° | |
| `vertical_rate_fpm` | ±128 fpm | BDS 6.0 quantisation |
| `airspeed_kts` | ±5 kt | IAS/TAS disambiguation |

readsb-only fields (`alt_geom`, `gs`, `track`, `nic`, `nac_p`, `nac_v`, `sil`) checked
non-null when `pos_confident=True`.

**Pass criteria:** No unexpected field absences. No systematic offset outside tolerance. Exit code 0.

---

### T3 — Hybrid mode validation

**Script:** `tools/hybrid_check.py`

Checks (per round):
1. readsb-only fields populated on ≥80% of positioned ADS-B aircraft
2. MLAT aircraft have `mlat_source` set
3. `/api/mlat/fixes` returns fixes from configured sources
4. `msg_queue p95` is 0 or absent in perf telemetry (D3)
5. `broadcast_avg` < 50 ms
6. `total_messages` > 0 (readsb stats providing counts)

**Pass criteria:** No failures across all rounds. Warnings acceptable for warm-up period. Exit code 0.

---

### T4 — Watchdog and degradation

**Script:** `tools/watchdog_check.py`

| Sub-test | Method | Status |
|----------|--------|--------|
| T4c — unknown INGEST_MODE | Automated | ✅ Confirmed passing |
| T4a — readsb stale | Guided interactive | Pending |
| T4b — Beast disconnect | Guided interactive | Pending |

Expected timings (T4a): WARNING within 5 s of stop, ERROR within 30 s, recovery within 2 poll cycles (~2 s) after restart.
Expected timings (T4b): Beast reconnect within 5 s (connection refused) or 30 s (timeout).

---

### T5 — Signal conversion consistency

**Script:** `tools/signal_check.py`

| Check | Method | Status |
|-------|--------|--------|
| Formula correctness | Automated (no live system) | ✅ Confirmed passing |
| Single-instance range check | Live `/api/stats` | Pending |
| Two-instance comparison | Two live instances | Pending |

**Pass criteria:** Formula correct. Mean delta ≤ 30 units across rounds. No systematic clamping. Exit code 0.

---

### T6 — Performance benchmark

**Requires high traffic (165–200 aircraft). Run when traffic allows.**

Measure ingest cycle time and broadcast latency across all four paths via `/api/debug/perf`.

| Metric | Target |
|--------|--------|
| `msg_decode_us p99` (Beast + C ext) | < 500 µs |
| `msg_decode_us p99` (Beast + pyModeS) | < 2000 µs |
| `broadcast_avg` (all modes) | < 50 ms |
| `lock_wait_us p99` (Beast / hybrid) | < 100 µs |
| `msg_queue_stats p95` (Beast / hybrid) | < 100 |
| `msg_drops_total` (all modes) | 0 |
| readsb poll cycle time | < 10 ms |

Modes to benchmark (10 min each at peak traffic):

| Mode | Config |
|------|--------|
| Beast + pyModeS (baseline) | `INGEST_MODE=beast`, rename `libdecode.so` temporarily |
| Beast + C extension | `INGEST_MODE=beast` |
| readsb JSON | `INGEST_MODE=readsb` |
| Hybrid | `INGEST_MODE=hybrid` |

Compare against Pi baseline from Phase 3 proposal: `msg_queue p95=4834`, `broadcast_avg=186 ms`.

**Status:** Pending high-traffic window (165–200 aircraft).

---

## Pass/fail summary

| Test | Automated? | Status |
|------|-----------|--------|
| D1 — live endpoint fields | Code change | ✅ Complete (commit 703a76e) |
| D2 — readsb_stats → DB | Code change | ✅ Complete (commit 703a76e) |
| D3 — queue telemetry fix | Code change | ✅ Complete (commit 703a76e) |
| T1 — C extension parity | Script written | ⬜ Pending corpus capture |
| T2 — snapshot diff | Script written | ⬜ Pending two-instance run |
| T3 — hybrid validation | Script written | ⬜ Pending hybrid + MLAT run |
| T4c — unknown INGEST_MODE | Automated | ✅ Confirmed passing |
| T4a — readsb stale | Guided script | ⬜ Pending |
| T4b — Beast disconnect | Guided script | ⬜ Pending |
| T5 formula | Automated | ✅ Confirmed passing |
| T5 live comparison | Script written | ⬜ Pending two-instance run |
| T6 — performance benchmark | Manual | ⬜ Blocked on 165–200 ac traffic |

**Phase 4 complete when:** T1–T5 all pass (including guided steps), T6 run at ≥165 aircraft with all targets met.
