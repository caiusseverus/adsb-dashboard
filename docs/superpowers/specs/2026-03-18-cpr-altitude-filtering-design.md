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

**Change B — `_accept_adsb_position()` speed check gate (line 503):**

Change:
```python
elif ac.lat is not None and ac.lon is not None and ac.last_pos_ts > 0:
```
to:
```python
elif ac.lat is not None and ac.lon is not None and ac.last_pos_ts > 0 and ac.pos_global:
```

Without this gate, local decode writes a wrong-zone position to `ac.lat/ac.lon`. When the first correct global decode fires, the speed check compares it against the wrong-zone reference — a jump of ~120nm at mid-latitudes, which exceeds the 1500kt ceiling and causes the correct position to be rejected. Gating on `pos_global` means: before global decode is established, no speed check runs. Once global decode confirms the absolute position, the speed gate activates normally against a known-correct reference.

Note: wrong-zone positions continue to be written to `ac.lat/ac.lon` during the pre-global phase (they are used as the local decode reference internally, keeping consecutive local decodes self-consistent). This is acceptable because: (a) Change A suppresses them from snapshots and `coverage_samples`, and (b) once global decode fires with the correct absolute position, all subsequent speed checks use that correct reference.

**Change C — `_accept_adsb_position()` fast-track gate (line 526):**

Change:
```python
if ac.lat is not None and ac.lon is not None:
```
to:
```python
if ac.lat is not None and ac.lon is not None and ac.pos_global:
```

The fast-track (promotes reliability to 2.0 when within 27nm of last known) must be gated on `pos_global`. Without this gate, the fast-track uses a wrong local-decode position as its reference, self-reinforcing the bad zone on every subsequent local decode and making recovery harder. Note: Change B already ensures the fast-track is only reachable when `pos_global` is True (the speed check returns early otherwise), so Change C is belt-and-suspenders — but it is included for clarity and resilience against future refactoring.

**Trade-off:** Aircraft heard with only one CPR frame type indefinitely (extremely rare — requires one 1090 MHz polarisation to be consistently blocked) would never publish a position. This is correct behaviour: without a global decode, absolute position cannot be reliably determined.

**Timeout interaction:** The existing 3600s timeout path (line 496–502) already resets `pos_global = False`. After a timeout reset, the first position accepted will be a local decode, `pos_global` will be False, and the speed check will be skipped — this is correct and desired. The aircraft must re-establish from a new global decode pair before positions are published again.

---

### Fix 2 — Tighten altitude filter

**File:** `backend/aircraft_state.py`

**Change A — Tighten no-vrate fallback constants:**

Update the two existing constants at lines 81–82 and insert `_ALT_HARD_MAX_FPM` immediately after:
```python
_ALT_DEFAULT_MAX_FPM = 3_000   # was 12_500
_ALT_DEFAULT_MIN_FPM = -3_000  # was -12_500
_ALT_HARD_MAX_FPM    = 6_000   # new — physical ceiling for Layer 6 override paths only
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

**Effect:** A CRC-clean message with a physically impossible implied climb/descent rate no longer overrides established altitude history. The ±6,000 fpm hard ceiling is generous enough to accommodate rapid climbs/descents but prevents corrupt messages from teleporting altitude. A message from a better source with an implausible fpm falls through to Layer 8 (penalty) rather than being silently ignored — the same outcome as any other rejected message.

---

## Scope

- **Files changed:** `backend/aircraft_state.py` only
- **Lines changed:** ~8 lines across constants block, `_pos_reliable()`, `_accept_adsb_position()` (speed check + fast-track), and `_accept_altitude()`
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
5. Verify a new aircraft appears on the map promptly once it has transmitted a valid even+odd CPR pair within 10 seconds (may not be the first two messages — could be message 2 or 3 depending on frame order) — temporarily add a debug log on the `pos_from_global = True` branch in `_accept_adsb_position()` to make the `pos_global` transition observable
