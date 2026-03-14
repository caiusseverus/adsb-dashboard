# Coverage / Track Stabilization Fix Plan

This document proposes targeted fixes for two observed behaviors:

1. Spurious entry positions when aircraft first appear.
2. Vertical altitude jumps at fixed lateral position in 3D coverage/trails.

## Problem A: Spurious entry positions on new tracks

### Root cause
At track start, ADS-B position decode can fall back to local CPR (`position_with_ref`) before an even/odd global pair is available. That early local decode is sometimes wrong-zone but still inside the current broad plausibility gate.

### Proposed fixes

#### A1) Suppress first-position publication until confidence is established
**Backend (`aircraft_state.py`)**
- Add a `position_confident` flag (or `position_confidence_state`) on `Aircraft`.
- Set confidence when either:
  - global CPR succeeds (`pos_global=True`), or
  - two consecutive local decodes are mutually plausible (small range/bearing delta and implied groundspeed cap).
- Do not publish `lat/lon/range/bearing` to snapshot until confidence is true.

**Why:** Avoid exposing unstable bootstrapping points to live coverage views.

#### A2) Tighten first-local decode plausibility
**Backend (`aircraft_state.py`)**
- Keep the 500 nm hard cap, but add an additional *entry-stage* cap for local-only decode, e.g. 250–300 nm (configurable).
- Add implied speed check against last accepted position for all ADS-B writes (not only MLAT).

**Why:** Wrong-zone local CPR often implies huge jumps that can be rejected with motion sanity checks.

#### A3) Frontend guardrail for live 3D trails
**Frontend (`CoveragePage.jsx`)**
- When appending live trail points, require `ac.pos_global || ac.mlat` before accepting points.

**Why:** Backend `track_store` already does this; align live 3D trail ingestion to the same reliability contract.

---

## Problem B: Vertical altitude jumps at fixed XY

### Root cause
When lateral position goes stale (edge-of-coverage / landing), altitude may continue updating from DF4/DF20 surveillance replies. This creates a stack of points at nearly identical bearing/range with varying altitude.

### Proposed fixes

#### B1) Freshness-gate altitude persistence into coverage samples
**Backend (`main.py` + `aircraft_state.py`)**
- Track separate timestamps:
  - `last_pos_ts`: time of accepted position write.
  - `last_alt_ts`: time of accepted altitude write.
- For `coverage_samples` writes, only include altitude when:
  - `now - last_pos_ts <= POS_FRESH_S` and
  - `now - last_alt_ts <= ALT_FRESH_S`.
- Otherwise write `altitude=NULL` for that sample.

**Why:** Preserves lateral coverage without inventing vertical geometry from stale XY.

#### B2) Stronger source-consistency for altitude while position stale
**Backend (`aircraft_state.py`)**
- If position is stale (older than threshold), block SURV altitude overwrites unless they are close to last ADS-B altitude (small delta) or corroborated by repeated consistent SURV frames.
- Optionally lower `_ALT_MAX_RATE_FPM` for stale-position contexts.

**Why:** Reduces lone DF4/DF20 spikes when aircraft is weak/fading.

#### B3) 3D trail segment breaker on vertical-only jumps
**Frontend (`CoveragePage.jsx`)**
- Extend `isTrailSegmentValid(a, b)` with:
  - max altitude delta per second (e.g. 4,000–6,000 fpm equivalent), and/or
  - break when XY move is tiny but |Δalt| is large.

**Why:** Prevent rendering vertical “poles” even if backend occasionally emits noisy altitude.

---

## Suggested implementation order (low risk first)

1. **Frontend A3 + B3** (visual safeguards, minimal backend risk).
2. **Backend B1** (data quality gate for persisted coverage).
3. **Backend A2** (local-CPR entry plausibility tightening).
4. **Backend A1 + B2** (state-machine level confidence and source-consistency).

---

## Config knobs to add

- `ADSb_LOCAL_ENTRY_MAX_RANGE_NM` (default 300)
- `ADSb_MAX_IMPLIED_SPEED_KT` (default 750)
- `POS_FRESH_S` (default 15)
- `ALT_FRESH_S` (default 20)
- `TRAIL_MAX_ALT_RATE_FPM` (frontend/build-time constant initially)

---

## Validation plan

1. **Replay test corpus** of known edge-entry tracks:
   - Verify first plotted point is near final stabilized path.
2. **Synthetic stale-XY scenario**:
   - Feed fixed position + varying DF4/DF20 altitude.
   - Confirm coverage samples store `altitude=NULL` once position stales.
3. **3D trail visual check**:
   - Confirm no vertical poles when aircraft drops out and only altitude changes.
4. **Regression checks**:
   - MLAT tracks still appear quickly.
   - Legitimate climbs/descents remain visible when position is fresh.

