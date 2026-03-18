# CPR Position & Altitude Filtering Improvements

**Date:** 2026-03-18
**Status:** Approved
**Scope:** `backend/aircraft_state.py` constants and two functions

---

## Problem

### 1. CPR arc artifacts on coverage map

Aircraft are occasionally plotted at an incorrect location and appear to sweep in a circumferential arc before snapping to their correct position. Bad positions persist to `coverage_samples` and are visible in the 24-hour high-res timelapse and date timelapse.

**Root cause:** Local CPR decode fires before global decode is established, using the receiver position as the reference point. If the aircraft is in a different CPR longitude zone than the receiver, the decoded positions are offset (typically ~3.6° longitude). Consecutive wrong-zone positions are internally consistent with each other — the speed gate compares position N against position N-1, so it never fires. Reliability scores (`pos_reliable_odd/even`) accumulate to ≥2.0 via local decode alone. Positions are published to snapshots and written to `coverage_samples`. The fast-track boost (promotes reliability to 2.0 when within 27nm of last known) self-reinforces the bad zone because it uses the wrong local-decode position as its reference.

Affects: ADS-B aircraft only. MLAT bypasses CPR entirely.

### 2. Altitude jumps at coverage edges

Aircraft occasionally show altitude jumps (wrong altitude briefly published) when entering or leaving coverage. Less frequently seen mid-flight.

**Root cause A — wide fallback window:** When no vertical rate data is available, Layer 5 of `_accept_altitude()` falls back to ±12,500 fpm. This is effectively no filter and passes most physically implausible altitude changes.

**Root cause B — `good_crc` override bypasses rate check:** Layer 6 of `_accept_altitude()` allows a single CRC-clean ADS-B message to override established altitude history regardless of how implausible the implied rate is (`good_crc >= alt_reliable → accept unconditionally`). At coverage edges, signal degrades and bit errors that survive CRC correction produce CRC-clean messages with corrupt altitude values. These hit this override path and teleport the reported altitude.

---

## Design

### Fix 1 — Gate position publication on global decode

**File:** `backend/aircraft_state.py`

**Change A — `_pos_reliable()` (line 443):**

Add `and ac.pos_global` to the return condition:

```python
return (ac.pos_reliable_odd  >= _POS_RELIABLE_PUBLISH and
        ac.pos_reliable_even >= _POS_RELIABLE_PUBLISH and
        ac.pos_global)
```

Local decode continues to run and reliability scores continue to accumulate, but no position is ever published to snapshots or written to `coverage_samples` until at least one global CPR decode pair has confirmed the absolute position.

**Change B — `_accept_adsb_position()` fast-track gate (line 526):**

Change:
```python
if ac.lat is not None and ac.lon is not None:
```
to:
```python
if ac.lat is not None and ac.lon is not None and ac.pos_global:
```

The fast-track (promotes reliability to 2.0 when within 27nm of last known) must be gated on `pos_global`. Without this gate, the fast-track uses a wrong local-decode position as its reference, self-reinforcing the bad zone on every subsequent local decode and making recovery harder.

**Trade-off:** Aircraft heard with only one CPR frame type indefinitely (extremely rare — requires one 1090 MHz polarisation to be consistently blocked) would never publish a position. This is correct behaviour: without a global decode, absolute position cannot be reliably determined.

---

### Fix 2 — Tighten altitude filter

**File:** `backend/aircraft_state.py`

**Change A — Tighten no-vrate fallback constants:**

```python
_ALT_DEFAULT_MAX_FPM = 3_000   # was 12_500
_ALT_DEFAULT_MIN_FPM = -3_000  # was -12_500
_ALT_HARD_MAX_FPM    = 6_000   # new — physical ceiling for override path
```

Rationale: most civil aircraft do not exceed ~3,000 fpm. Aircraft that do (military, aerobatic, fast jets) will have vertical rate data from BDS 6,0 EHS, which opens the Layer 5 window dynamically around the reported rate. The fallback only applies when no vrate data is available.

**Change B — Add physical ceiling to `good_crc` and `source` override paths (Layer 6):**

```python
elif good_crc >= ac.alt_reliable and (fpm == 0 or abs(fpm) <= _ALT_HARD_MAX_FPM):
    accept = True
    reset_reliable = True
elif source > (ac._alt_source or MsgSource.INVALID) and (fpm == 0 or abs(fpm) <= _ALT_HARD_MAX_FPM):
    accept = True
    reset_reliable = True
```

The `fpm == 0` guard preserves the existing behaviour for the first altitude on a new aircraft (`ac.altitude is None → delta = 0 → fpm = 0`), which must remain unconditionally accepted.

**Effect:** A CRC-clean message with a physically impossible implied climb/descent rate no longer overrides established altitude history. The ±6,000 fpm hard ceiling is generous enough to accommodate rapid climbs/descents but prevents corrupt messages from teleporting altitude.

---

## Scope

- **Files changed:** `backend/aircraft_state.py` only
- **Lines changed:** ~6 lines across constants block, `_pos_reliable()`, `_accept_adsb_position()`, and `_accept_altitude()`
- **No database schema changes**
- **No frontend changes**
- **No new config variables**
- **No API changes**

---

## Testing

Manual verification after deployment:
1. Monitor live map and 24h timelapse for arc artifacts over 24–48 hours
2. Monitor altitude jumps in `AircraftTable` and historical data
3. Check `pos_global` flag is False for new aircraft until first global decode (add debug log if needed)
4. Confirm MLAT aircraft are unaffected (bypass CPR path entirely)
