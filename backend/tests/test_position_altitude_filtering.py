"""
Tests for CPR position gate (Fix 1) and altitude filter tightening (Fix 2).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from aircraft_state import (
    Aircraft,
    AircraftState,
    AdsbPositionSample,
    MsgSource,
    _pos_reliable,
    _accept_adsb_position,
    _accept_altitude,
)


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
            last_pos_ts=900.0,   # 100s ago so 1.87nm implies ~67kt — passes speed gate
        )
        _accept_adsb_position(ac, lat=51.5, lon=-0.05, pos_from_global=True,
                               cpr_odd=True, now=1000.0)
        # Fast-track must have promoted odd score to at least 2.0
        assert ac.pos_reliable_odd >= 2.0


class TestAdsbPositionHistory:

    def test_accept_adsb_position_records_timestamped_history(self):
        ac = make_ac(
            gs=240.0,
            track=90.0,
            airspeed_kts=230,
            heading_deg=95.0,
        )

        _accept_adsb_position(ac, lat=51.5, lon=-0.1, pos_from_global=True,
                               cpr_odd=False, now=1000.0)

        assert len(ac.adsb_position_history) == 1
        sample = ac.adsb_position_history[0]
        assert sample.ts == pytest.approx(1000.0)
        assert sample.lat == pytest.approx(51.5, abs=0.001)
        assert sample.lon == pytest.approx(-0.1, abs=0.001)
        assert sample.groundspeed_kts == pytest.approx(240.0)
        assert sample.track_deg == pytest.approx(90.0)

    def test_get_aircraft_position_history_returns_recent_samples(self):
        state = AircraftState()
        ac = make_ac()
        now = 1000.0
        ac.adsb_position_history.append(
            AdsbPositionSample(now, 51.5, -0.1, 220.0, 100.0, 98.0, 210)
        )
        state._aircraft[ac.icao] = ac

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr("aircraft_state.time.time", lambda: now + 1.0)
        try:
            history = state.get_aircraft_position_history(ac.icao, window_s=3600.0)
        finally:
            monkeypatch.undo()

        assert len(history) == 1
        assert history[0]["ts"] == pytest.approx(now)
        assert history[0]["groundspeed_kts"] == pytest.approx(220.0)


class TestSnapshotPublication:

    def test_snapshot_hides_local_only_adsb_position_until_global_decode(self):
        state = AircraftState()
        ac = make_ac(
            lat=51.5, lon=-0.1,
            range_nm=12.3, bearing_deg=180.0,
            pos_reliable_odd=4.0, pos_reliable_even=4.0,
            pos_global=False,
        )
        state._aircraft[ac.icao] = ac

        snapshot = state.get_snapshot()
        live = state.get_aircraft_live(ac.icao)

        assert snapshot["aircraft"][0]["lat"] is None
        assert snapshot["aircraft"][0]["lon"] is None
        assert snapshot["aircraft"][0]["range_nm"] is None
        assert snapshot["aircraft"][0]["bearing_deg"] is None
        assert live["lat"] is None
        assert live["lon"] is None

    def test_snapshot_keeps_mlat_positions_visible(self):
        state = AircraftState()
        ac = make_ac(
            lat=51.5, lon=-0.1,
            range_nm=12.3, bearing_deg=180.0,
            mlat=True,
        )
        state._aircraft[ac.icao] = ac

        snapshot = state.get_snapshot()

        assert snapshot["aircraft"][0]["lat"] == pytest.approx(51.5, abs=0.001)
        assert snapshot["aircraft"][0]["lon"] == pytest.approx(-0.1, abs=0.001)


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
        # ADSB source with clean CRC → good_crc computed inside _accept_altitude
        # good_crc > alt_reliable(3) → override path. Rate plausible → accept.
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
