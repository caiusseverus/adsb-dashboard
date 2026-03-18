# CPR Position & Altitude Filtering Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two bugs in `aircraft_state.py`: (1) CPR arc artifacts caused by local-decode positions being published before global decode is established, and (2) altitude jumps at coverage edges caused by an over-wide fallback filter window and an override path that bypasses the rate check.

**Architecture:** All changes are in `backend/aircraft_state.py`. Three one-line edits for the position fix (gating `_pos_reliable()`, the speed check, and the fast-track on `pos_global`). Two changes for the altitude fix (tightening constants and adding a physical ceiling to Layer 6 override paths). Tests go in a new file `backend/tests/test_position_altitude_filtering.py`.

**Tech Stack:** Python 3.10, pytest, `uv run` test runner. No new dependencies.

---

## File Map

| Action | Path | What changes |
|--------|------|-------------|
| Modify | `backend/aircraft_state.py:81-82` | Update `_ALT_DEFAULT_MAX/MIN_FPM` constants; add `_ALT_HARD_MAX_FPM` |
| Modify | `backend/aircraft_state.py:443` | Add `and ac.pos_global` to `_pos_reliable()` return |
| Modify | `backend/aircraft_state.py:503` | Add `and ac.pos_global` to speed check gate |
| Modify | `backend/aircraft_state.py:526` | Add `and ac.pos_global` to fast-track gate |
| Modify | `backend/aircraft_state.py:358-365` | Add `(fpm == 0 or abs(fpm) <= _ALT_HARD_MAX_FPM)` to Layer 6 overrides |
| Create | `backend/tests/test_position_altitude_filtering.py` | New test file |

---

## Context You Need Before Starting

### How to run tests
```bash
cd /home/keith/claude/adsb-dashboard
uv run --directory backend pytest tests/test_position_altitude_filtering.py -v
```

To run all backend tests:
```bash
uv run --directory backend pytest -v
```

### How the private functions are accessed in tests
The test files in `backend/tests/` add the parent dir to the path:
```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
```
Then import directly: `from aircraft_state import Aircraft, _pos_reliable, _accept_adsb_position, _accept_altitude, MsgSource`

### How to create a minimal Aircraft for testing
`Aircraft` is a dataclass in `aircraft_state.py`. Many fields have defaults. The minimal set needed per function is listed in each task below. Use `Aircraft(icao="ABC123")` as the base — all other fields default to `None`, `False`, or `0.0` as appropriate.

### Key constants (current values, before your changes)
```python
_POS_RELIABLE_PUBLISH = 2.0   # both odd+even must reach this to publish position
_ALT_DEFAULT_MAX_FPM  = 12500  # you will change to 3000
_ALT_DEFAULT_MIN_FPM  = -12500 # you will change to -3000
_ALT_LOW_DELTA_FT     = 300    # delta below this → unconditional accept (don't change)
_ALT_RELIABLE_PUBLISH = 2      # minimum alt_reliable to publish (don't change)
```

### Layer 6 of `_accept_altitude()` — current code at lines 353–365
```python
accept = False
reset_reliable = False
if abs(delta) < _ALT_LOW_DELTA_FT:
    accept = True                    # tiny delta: unconditional
elif fpm != 0 and min_fpm <= fpm <= max_fpm:
    accept = True                    # rate consistent with reported vrate
elif good_crc >= ac.alt_reliable:
    accept = True                    # high-confidence source overrides history
    reset_reliable = True
elif source > (ac._alt_source or MsgSource.INVALID):
    accept = True                    # better source than current
    reset_reliable = True
```

---

## Task 1: Tests for Fix 1 — CPR position gate

**Files:**
- Create: `backend/tests/test_position_altitude_filtering.py`

### What these tests verify
- `_pos_reliable()` returns False when `pos_global=False`, even if reliability scores are high
- `_pos_reliable()` returns True once `pos_global=True` and scores meet threshold
- `_accept_adsb_position()` skips the speed check when `pos_global=False` (so a large jump from a wrong-zone local position doesn't block the first correct global decode)
- `_accept_adsb_position()` skips the fast-track when `pos_global=False`

- [ ] **Step 1: Create the test file with position tests**

```python
"""
Tests for CPR position gate (Fix 1) and altitude filter tightening (Fix 2).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from aircraft_state import Aircraft, MsgSource, _pos_reliable, _accept_adsb_position, _accept_altitude


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_ac(**kwargs) -> Aircraft:
    """Create a minimal Aircraft for testing. All unset fields use dataclass defaults."""
    return Aircraft(icao="ABC123", **kwargs)


# ---------------------------------------------------------------------------
# Fix 1: _pos_reliable() requires pos_global=True
# ---------------------------------------------------------------------------

class TestPosReliableRequiresGlobal:

    def test_returns_false_when_pos_global_false_even_with_high_scores(self):
        """Local-decode-only aircraft must not be published even if reliability is high."""
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=4.0,
            pos_reliable_even=4.0,
            pos_global=False,   # no global decode yet
        )
        assert _pos_reliable(ac) is False

    def test_returns_true_when_pos_global_true_and_scores_meet_threshold(self):
        """Normal case: global decode established, both scores >= 2.0."""
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=2.0,
            pos_reliable_even=2.0,
            pos_global=True,
        )
        assert _pos_reliable(ac) is True

    def test_returns_false_when_lat_is_none(self):
        """No position at all → False."""
        ac = make_ac(pos_global=True, pos_reliable_odd=4.0, pos_reliable_even=4.0)
        assert _pos_reliable(ac) is False

    def test_mlat_bypasses_pos_global_check(self):
        """MLAT positions are validated by network geometry, not CPR — always trusted."""
        ac = make_ac(lat=51.5, lon=-0.1, mlat=True, pos_global=False)
        assert _pos_reliable(ac) is True

    def test_returns_false_when_scores_below_threshold(self):
        """pos_global=True but scores not yet at publish threshold."""
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=1.9,
            pos_reliable_even=2.0,
            pos_global=True,
        )
        assert _pos_reliable(ac) is False


# ---------------------------------------------------------------------------
# Fix 1: _accept_adsb_position() speed check gated on pos_global
# ---------------------------------------------------------------------------

class TestAcceptAdsbPositionSpeedGate:

    def test_speed_check_skipped_when_pos_global_false(self):
        """
        When pos_global=False, the speed check must be skipped so that the first
        correct global decode is not rejected for being 'too far' from a wrong-zone
        local-decode position stored in ac.lat/ac.lon.

        Simulate: ac has a wrong-zone local position at (51.5, -0.1).
        A correct global decode arrives at (51.5, 3.5) — ~130nm away, which would
        fail the speed gate if checked. With pos_global=False the speed check
        must be bypassed and the new position written.
        """
        ac = make_ac(
            lat=51.5, lon=-0.1,      # wrong-zone local position
            pos_reliable_odd=1.0,
            pos_reliable_even=0.0,
            pos_global=False,
            last_pos_ts=1000.0,
        )
        # Correct global decode 130nm away, 1 second later
        _accept_adsb_position(ac, lat=51.5, lon=3.5, pos_from_global=True,
                               cpr_odd=False, now=1001.0)
        # Position must be accepted and written
        assert ac.lon == pytest.approx(3.5, abs=0.001)
        assert ac.pos_global is True

    def test_speed_check_active_when_pos_global_true(self):
        """
        After global decode is established, the speed check must reject
        physically impossible position jumps.
        """
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=3.0,
            pos_reliable_even=3.0,
            pos_global=True,
            last_pos_ts=1000.0,
        )
        original_lon = ac.lon
        # Position 5000nm away in 1 second — physically impossible
        _accept_adsb_position(ac, lat=51.5, lon=70.0, pos_from_global=True,
                               cpr_odd=False, now=1001.0)
        # Must be rejected: lon unchanged
        assert ac.lon == pytest.approx(original_lon, abs=0.001)


# ---------------------------------------------------------------------------
# Fix 1: _accept_adsb_position() fast-track gated on pos_global
# ---------------------------------------------------------------------------

class TestAcceptAdsbPositionFastTrack:

    def test_fast_track_not_applied_when_pos_global_false(self):
        """
        Fast-track would promote pos_reliable to 2.0 if within 27nm of last known.
        With pos_global=False, the 'last known' is a wrong-zone position — fast-track
        must not fire.
        """
        ac = make_ac(
            lat=51.5, lon=-0.1,       # wrong-zone position
            pos_reliable_odd=0.0,
            pos_reliable_even=0.0,
            pos_global=False,
            last_pos_ts=1000.0,
        )
        # New position within 27nm of the wrong-zone position (so fast-track would fire)
        _accept_adsb_position(ac, lat=51.5, lon=-0.05, pos_from_global=False,
                               cpr_odd=True, now=1001.0)
        # Fast-track must NOT have fired: score should be 0.0 + 1.0 = 1.0, not 2.0
        assert ac.pos_reliable_odd == pytest.approx(1.0, abs=0.01)

    def test_fast_track_fires_when_pos_global_true(self):
        """
        After global decode, fast-track should promote score to 2.0 when within 27nm.
        """
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=0.0,
            pos_reliable_even=0.0,
            pos_global=True,
            last_pos_ts=1000.0,
        )
        _accept_adsb_position(ac, lat=51.5, lon=-0.05, pos_from_global=True,
                               cpr_odd=True, now=1001.0)
        # Fast-track must have promoted odd score to at least 2.0
        assert ac.pos_reliable_odd >= 2.0
```

- [ ] **Step 2: Run to confirm tests fail (functions not yet changed)**

```bash
cd /home/keith/claude/adsb-dashboard
uv run --directory backend pytest tests/test_position_altitude_filtering.py::TestPosReliableRequiresGlobal tests/test_position_altitude_filtering.py::TestAcceptAdsbPositionSpeedGate tests/test_position_altitude_filtering.py::TestAcceptAdsbPositionFastTrack -v
```

Expected: `test_returns_false_when_pos_global_false_even_with_high_scores` FAILS (currently returns True), `test_speed_check_skipped_when_pos_global_false` FAILS (speed gate fires and rejects the correct position), `test_fast_track_not_applied_when_pos_global_false` FAILS (fast-track fires and promotes to 2.0).

---

## Task 2: Implement Fix 1 — CPR position gate

**Files:**
- Modify: `backend/aircraft_state.py`

- [ ] **Step 1: Update `_pos_reliable()` — add `and ac.pos_global`**

Find the return statement in `_pos_reliable()` (around line 443). It currently reads:
```python
    return (ac.pos_reliable_odd  >= _POS_RELIABLE_PUBLISH and
            ac.pos_reliable_even >= _POS_RELIABLE_PUBLISH)
```

Change to:
```python
    return (ac.pos_reliable_odd  >= _POS_RELIABLE_PUBLISH and
            ac.pos_reliable_even >= _POS_RELIABLE_PUBLISH and
            ac.pos_global)
```

- [ ] **Step 2: Update speed check gate in `_accept_adsb_position()` — add `and ac.pos_global`**

Find the `elif` at the start of the speed check block (around line 503). It currently reads:
```python
    elif ac.lat is not None and ac.lon is not None and ac.last_pos_ts > 0:
```

Change to:
```python
    elif ac.lat is not None and ac.lon is not None and ac.last_pos_ts > 0 and ac.pos_global:
```

- [ ] **Step 3: Update fast-track gate in `_accept_adsb_position()` — add `and ac.pos_global`**

Find the fast-track `if` (around line 526). It currently reads:
```python
    if ac.lat is not None and ac.lon is not None:
```

Change to:
```python
    if ac.lat is not None and ac.lon is not None and ac.pos_global:
```

- [ ] **Step 4: Run position tests — expect all pass**

```bash
cd /home/keith/claude/adsb-dashboard
uv run --directory backend pytest tests/test_position_altitude_filtering.py::TestPosReliableRequiresGlobal tests/test_position_altitude_filtering.py::TestAcceptAdsbPositionSpeedGate tests/test_position_altitude_filtering.py::TestAcceptAdsbPositionFastTrack -v
```

Expected: All 7 tests PASS.

- [ ] **Step 5: Run full test suite — no regressions**

```bash
uv run --directory backend pytest -v
```

Expected: All tests pass (52 existing + 7 new).

- [ ] **Step 6: Commit**

```bash
git add backend/aircraft_state.py backend/tests/test_position_altitude_filtering.py
git commit -m "fix: gate CPR position publication on global decode to eliminate arc artifacts"
```

---

## Task 3: Tests for Fix 2 — Altitude filter tightening

**Files:**
- Modify: `backend/tests/test_position_altitude_filtering.py` (append tests)

### What these tests verify
- When no vrate is known, altitude jumps larger than ±3000 fpm are rejected (new tighter window)
- The `good_crc >= alt_reliable` override path rejects physically impossible altitude jumps (>6000 fpm)
- The `source >` override path also rejects physically impossible altitude jumps
- First altitude on a new aircraft (no prior altitude) is still always accepted
- Small altitude changes (<300ft) are still always accepted regardless

- [ ] **Step 1: Append altitude tests to the test file**

Add the following class to `backend/tests/test_position_altitude_filtering.py`:

```python
# ---------------------------------------------------------------------------
# Fix 2: _accept_altitude() — tighter no-vrate window and override ceiling
# ---------------------------------------------------------------------------

class TestAcceptAltitude:

    def _make_alt_ac(self, altitude=None, alt_reliable=0, alt_ts=0.0,
                     alt_source=None, mlat=False):
        """Minimal Aircraft for altitude testing."""
        ac = make_ac(mlat=mlat)
        ac.altitude = altitude
        ac.alt_reliable = alt_reliable
        ac._alt_ts = alt_ts
        ac._alt_source = alt_source
        ac._vrate_baro_fpm = None
        ac._vrate_baro_ts = 0.0
        ac._vrate_geom_fpm = None
        ac._vrate_geom_ts = 0.0
        ac.max_altitude = altitude
        ac.last_alt_ts = alt_ts
        return ac

    # --- First altitude always accepted ---

    def test_first_altitude_accepted(self):
        """No prior altitude → delta=0 → unconditional accept."""
        ac = self._make_alt_ac(altitude=None)
        result = _accept_altitude(ac, alt=35000, source=MsgSource.ADSB,
                                   crc_clean=True, now=1000.0)
        assert result is True
        assert ac.altitude == 35000

    # --- Small delta always accepted ---

    def test_small_delta_accepted_regardless_of_rate(self):
        """Delta < 300ft is unconditionally accepted (no rate check)."""
        ac = self._make_alt_ac(altitude=35000, alt_reliable=10, alt_ts=999.0)
        result = _accept_altitude(ac, alt=35200, source=MsgSource.ADSB,
                                   crc_clean=True, now=1000.0)
        assert result is True
        assert ac.altitude == 35200

    # --- Tighter no-vrate fallback window (was ±12500, now ±3000 fpm) ---

    def test_implausible_rate_rejected_when_no_vrate(self):
        """
        Large altitude jump with no vrate data.
        Implied rate: (35000-25000)ft / 1s * 600 / (10+10) = 300,000 fpm >> 3000.
        Must be rejected.
        """
        ac = self._make_alt_ac(altitude=35000, alt_reliable=10, alt_ts=999.0)
        # No vrate data set (both _vrate_baro_fpm and _vrate_geom_fpm are None)
        result = _accept_altitude(ac, alt=25000, source=MsgSource.MODE_S,
                                   crc_clean=False, now=1000.0)
        assert result is False
        assert ac.altitude == 35000  # unchanged

    def test_plausible_rate_accepted_when_no_vrate(self):
        """
        Gentle climb within the 3000 fpm window should still be accepted.
        Delta 300ft over 30s → implied rate = 300*600/(300+10) ≈ 581 fpm < 3000.
        """
        ac = self._make_alt_ac(altitude=35000, alt_reliable=10, alt_ts=970.0)
        result = _accept_altitude(ac, alt=35300, source=MsgSource.ADSB,
                                   crc_clean=True, now=1000.0)
        assert result is True
        assert ac.altitude == 35300

    # --- good_crc override ceiling (was unlimited, now ≤ 6000 fpm) ---

    def test_good_crc_override_blocked_for_impossible_rate(self):
        """
        CRC-clean ADS-B message (good_crc=20) must not override history if
        implied rate > 6000 fpm. This is the coverage-edge teleport scenario.

        Setup: aircraft at 35000ft, alt_reliable=5 (good_crc=20 > 5, so override
        path fires in old code). New message: 25000ft 1 second later.
        Rate ≈ 300,000 fpm >> 6000 → must be rejected even though good_crc > alt_reliable.
        """
        ac = self._make_alt_ac(altitude=35000, alt_reliable=5, alt_ts=999.0,
                                alt_source=MsgSource.ADSB)
        result = _accept_altitude(ac, alt=25000, source=MsgSource.ADSB,
                                   crc_clean=True, now=1000.0)
        assert result is False
        assert ac.altitude == 35000

    def test_good_crc_override_allowed_for_plausible_rate(self):
        """
        CRC-clean override path must still work for physically plausible rates.
        Delta 300ft over 30s → ~581 fpm < 6000 fpm → override allowed.
        """
        ac = self._make_alt_ac(altitude=35000, alt_reliable=3, alt_ts=970.0,
                                alt_source=MsgSource.MODE_S)
        # ADSB source with clean CRC → good_crc=7 (>2s since last update)
        # good_crc(7) > alt_reliable(3) → override path. Rate plausible → accept.
        result = _accept_altitude(ac, alt=35300, source=MsgSource.ADSB,
                                   crc_clean=True, now=1000.0)
        assert result is True
        assert ac.altitude == 35300

    # --- source override ceiling ---

    def test_source_override_blocked_for_impossible_rate(self):
        """
        Better source (ADSB > MODE_S) must not override if rate > 6000 fpm.
        """
        ac = self._make_alt_ac(altitude=35000, alt_reliable=15, alt_ts=999.0,
                                alt_source=MsgSource.MODE_S)
        result = _accept_altitude(ac, alt=25000, source=MsgSource.ADSB,
                                   crc_clean=True, now=1000.0)
        assert result is False
        assert ac.altitude == 35000
```

- [ ] **Step 2: Run to confirm altitude tests fail**

```bash
cd /home/keith/claude/adsb-dashboard
uv run --directory backend pytest tests/test_position_altitude_filtering.py::TestAcceptAltitude -v
```

Expected: `test_implausible_rate_rejected_when_no_vrate`, `test_good_crc_override_blocked_for_impossible_rate`, and `test_source_override_blocked_for_impossible_rate` FAIL (old wide window and override paths let them through). Others pass.

---

## Task 4: Implement Fix 2 — Altitude filter tightening

**Files:**
- Modify: `backend/aircraft_state.py`

- [ ] **Step 1: Update the three altitude constants at lines 81–83**

Find the lines:
```python
_ALT_DEFAULT_MAX_FPM    = 12500   # default rate ceiling when no vertical rate known
_ALT_DEFAULT_MIN_FPM    = -12500
```

Change to (and add `_ALT_HARD_MAX_FPM` immediately after):
```python
_ALT_DEFAULT_MAX_FPM    = 3_000   # default rate ceiling when no vertical rate known (was 12500)
_ALT_DEFAULT_MIN_FPM    = -3_000  # (was -12500)
_ALT_HARD_MAX_FPM       = 6_000   # physical ceiling for Layer 6 override paths only
```

- [ ] **Step 2: Update Layer 6 override paths in `_accept_altitude()` (around lines 360–365)**

Find:
```python
    elif good_crc >= ac.alt_reliable:
        accept = True                    # high-confidence source overrides history
        reset_reliable = True
    elif source > (ac._alt_source or MsgSource.INVALID):
        accept = True                    # better source than current
        reset_reliable = True
```

Change to:
```python
    elif good_crc >= ac.alt_reliable and (fpm == 0 or abs(fpm) <= _ALT_HARD_MAX_FPM):
        accept = True                    # high-confidence source overrides history
        reset_reliable = True
    elif source > (ac._alt_source or MsgSource.INVALID) and (fpm == 0 or abs(fpm) <= _ALT_HARD_MAX_FPM):
        accept = True                    # better source than current
        reset_reliable = True
```

Note: `fpm == 0` when `ac.altitude is None` (first altitude for a new aircraft, delta forced to 0) — this preserves unconditional accept for the first altitude on a new aircraft.

- [ ] **Step 3: Run altitude tests — expect all pass**

```bash
cd /home/keith/claude/adsb-dashboard
uv run --directory backend pytest tests/test_position_altitude_filtering.py::TestAcceptAltitude -v
```

Expected: All 7 altitude tests PASS.

- [ ] **Step 4: Run full test suite — no regressions**

```bash
uv run --directory backend pytest -v
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/aircraft_state.py backend/tests/test_position_altitude_filtering.py
git commit -m "fix: tighten altitude filter — narrow no-vrate window and cap override paths"
```
