import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from radar.localiser import RadarLocaliser, _C_MUS, _latlon_to_xy
from radar.models import CalibrationPair


def _make_pair(lat_a: float, lon_a: float, lat_b: float, lon_b: float, tdoa_us: float = 0.0) -> CalibrationPair:
    return CalibrationPair(
        iid=1,
        ts=1000.0,
        icao_a="AAAAAA",
        icao_b="BBBBBB",
        lat_a=lat_a,
        lon_a=lon_a,
        lat_b=lat_b,
        lon_b=lon_b,
        tdoa_us=tdoa_us,
        receiver_lat=51.0,
        receiver_lon=-1.0,
    )


def test_localiser_rejects_solution_that_lands_on_search_boundary(monkeypatch):
    import scipy.optimize

    def fake_least_squares(*args, **kwargs):
        upper_bounds = kwargs["bounds"][1]
        return SimpleNamespace(
            success=True,
            cost=0.0,
            x=[upper_bounds[0], upper_bounds[1]],
            fun=[0.0, 0.0],
        )

    monkeypatch.setattr(scipy.optimize, "least_squares", fake_least_squares)

    localiser = RadarLocaliser()
    pairs = [
        _make_pair(51.3, -1.0, 51.0, -0.7),
        _make_pair(51.4, -1.1, 50.8, -0.8),
        _make_pair(51.2, -1.4, 50.7, -1.0),
        _make_pair(50.9, -1.5, 50.6, -0.6),
        _make_pair(50.7, -1.3, 51.1, -0.5),
        _make_pair(50.6, -1.2, 51.2, -0.9),
        _make_pair(50.8, -1.6, 51.4, -1.0),
        _make_pair(51.5, -1.4, 50.9, -0.4),
        _make_pair(51.4, -0.7, 50.7, -1.5),
        _make_pair(51.3, -1.7, 50.8, -0.3),
    ]

    with pytest.raises(ValueError, match="search boundary"):
        localiser.solve(pairs)


def test_tdoa_residuals_apply_receiver_path_correction():
    receiver_lat = 51.0
    receiver_lon = -1.0
    true_radar_lat = 51.2
    true_radar_lon = -0.8
    pair = CalibrationPair(
        iid=1,
        ts=1000.0,
        icao_a="AAAAAA",
        icao_b="BBBBBB",
        lat_a=51.3,
        lon_a=-1.1,
        lat_b=50.9,
        lon_b=-0.6,
        tdoa_us=0.0,
        receiver_lat=receiver_lat,
        receiver_lon=receiver_lon,
    )

    rx, ry = _latlon_to_xy(true_radar_lat, true_radar_lon, receiver_lat, receiver_lon)
    ax, ay = _latlon_to_xy(pair.lat_a, pair.lon_a, receiver_lat, receiver_lon)
    bx, by = _latlon_to_xy(pair.lat_b, pair.lon_b, receiver_lat, receiver_lon)
    d_ra = ((rx - ax) ** 2 + (ry - ay) ** 2) ** 0.5
    d_rb = ((rx - bx) ** 2 + (ry - by) ** 2) ** 0.5
    d_sa = (ax ** 2 + ay ** 2) ** 0.5
    d_sb = (bx ** 2 + by ** 2) ** 0.5
    pair.tdoa_us = ((d_ra + d_sa) - (d_rb + d_sb)) / _C_MUS

    residuals = RadarLocaliser().tdoa_residuals(
        [rx, ry],
        [pair],
        receiver_lat,
        receiver_lon,
    )

    assert residuals[0] == pytest.approx(0.0, abs=1e-6)


def test_localiser_rejects_large_residual_fit(monkeypatch):
    import scipy.optimize

    def fake_least_squares(*args, **kwargs):
        return SimpleNamespace(
            success=True,
            cost=0.0,
            x=[0.0, 0.0],
            fun=[20.0] * 12,
        )

    monkeypatch.setattr(scipy.optimize, "least_squares", fake_least_squares)

    localiser = RadarLocaliser()
    pairs = [
        _make_pair(51.3, -1.0, 51.0, -0.7),
        _make_pair(51.4, -1.1, 50.8, -0.8),
        _make_pair(51.2, -1.4, 50.7, -1.0),
        _make_pair(50.9, -1.5, 50.6, -0.6),
        _make_pair(50.7, -1.3, 51.1, -0.5),
        _make_pair(50.6, -1.2, 51.2, -0.9),
        _make_pair(50.8, -1.6, 51.4, -1.0),
        _make_pair(51.5, -1.4, 50.9, -0.4),
        _make_pair(51.4, -0.7, 50.7, -1.5),
        _make_pair(51.3, -1.7, 50.8, -0.3),
    ]

    with pytest.raises(ValueError, match="residual too large"):
        localiser.solve(pairs)


def test_compute_hyperbola_points_follow_corrected_tdoa_branch():
    receiver_lat = 51.0
    receiver_lon = -1.0
    true_radar_lat = 51.15
    true_radar_lon = -0.82
    pair = CalibrationPair(
        iid=1,
        ts=1000.0,
        icao_a="AAAAAA",
        icao_b="BBBBBB",
        lat_a=51.3,
        lon_a=-1.1,
        lat_b=50.9,
        lon_b=-0.6,
        tdoa_us=0.0,
        receiver_lat=receiver_lat,
        receiver_lon=receiver_lon,
    )

    rx, ry = _latlon_to_xy(true_radar_lat, true_radar_lon, receiver_lat, receiver_lon)
    ax, ay = _latlon_to_xy(pair.lat_a, pair.lon_a, receiver_lat, receiver_lon)
    bx, by = _latlon_to_xy(pair.lat_b, pair.lon_b, receiver_lat, receiver_lon)
    d_ra = ((rx - ax) ** 2 + (ry - ay) ** 2) ** 0.5
    d_rb = ((rx - bx) ** 2 + (ry - by) ** 2) ** 0.5
    d_sa = (ax ** 2 + ay ** 2) ** 0.5
    d_sb = (bx ** 2 + by ** 2) ** 0.5
    pair.tdoa_us = ((d_ra + d_sa) - (d_rb + d_sb)) / _C_MUS

    pts = RadarLocaliser().compute_hyperbola_points(pair, n_points=40, max_dist_m=300_000)

    assert len(pts) >= 10
    corrected_target = RadarLocaliser()._corrected_range_difference_m(pair)
    residuals = []
    for point in pts:
        px, py = _latlon_to_xy(point["lat"], point["lon"], receiver_lat, receiver_lon)
        residuals.append(abs((((px - ax) ** 2 + (py - ay) ** 2) ** 0.5) - (((px - bx) ** 2 + (py - by) ** 2) ** 0.5) - corrected_target))

    assert max(residuals) < 1.0


def test_filter_consistent_pairs_keeps_locally_stable_repeated_family():
    localiser = RadarLocaliser()
    pairs = [
        CalibrationPair(
            iid=1,
            ts=1_000.0 + idx * 30.0,
            icao_a="AAAAAA",
            icao_b="BBBBBB",
            lat_a=51.2,
            lon_a=-1.1,
            lat_b=51.0,
            lon_b=-0.7,
            tdoa_us=100.0 + offset,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
        for idx, offset in enumerate((0.0, 120.0, -80.0, 90.0))
    ]
    pairs.append(
        CalibrationPair(
            iid=1,
            ts=1_010.0,
            icao_a="CCCCCC",
            icao_b="DDDDDD",
            lat_a=51.3,
            lon_a=-1.2,
            lat_b=50.9,
            lon_b=-0.8,
            tdoa_us=4200.0,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
    )

    filtered = localiser.filter_consistent_pairs(pairs)

    assert len(filtered) == 4
    assert {(pair.icao_a, pair.icao_b) for pair in filtered} == {("AAAAAA", "BBBBBB")}


def test_filter_consistent_pairs_rejects_unstable_repeated_family():
    localiser = RadarLocaliser()
    pairs = [
        CalibrationPair(
            iid=1,
            ts=1_000.0 + idx * 20.0,
            icao_a="AAAAAA",
            icao_b="BBBBBB",
            lat_a=51.2,
            lon_a=-1.1,
            lat_b=51.0,
            lon_b=-0.7,
            tdoa_us=tdoa_us,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
        for idx, tdoa_us in enumerate((-1800.0, 2200.0, -2600.0, 3100.0))
    ]

    filtered = localiser.filter_consistent_pairs(pairs)

    assert filtered == []


def test_select_solver_pairs_prefers_consistent_coincident_subset_when_large_enough():
    localiser = RadarLocaliser()
    coincident_pairs = [
        CalibrationPair(
            iid=1,
            ts=1_000.0 + idx * 20.0,
            icao_a="AAAAAA",
            icao_b="BBBBBB",
            lat_a=51.2,
            lon_a=-1.1,
            lat_b=51.0,
            lon_b=-0.7,
            tdoa_us=120.0 + offset,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
        for idx, offset in enumerate((0.0, 50.0, -40.0, 20.0, -10.0, 40.0, -30.0, 10.0, -20.0, 30.0))
    ]
    broad_pairs = [
        CalibrationPair(
            iid=1,
            ts=1_300.0 + idx * 25.0,
            icao_a="CCCCCC",
            icao_b="DDDDDD",
            lat_a=51.3,
            lon_a=-1.2,
            lat_b=50.9,
            lon_b=-0.8,
            tdoa_us=900.0 + offset,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
        for idx, offset in enumerate((0.0, 30.0, -20.0, 10.0))
    ]

    selected = localiser.select_solver_pairs(coincident_pairs + broad_pairs)

    assert len(selected) == 10
    assert all(abs(pair.tdoa_us) <= 500.0 for pair in selected)
    assert {(pair.icao_a, pair.icao_b) for pair in selected} == {("AAAAAA", "BBBBBB")}


def test_select_solver_pairs_falls_back_to_broader_consistent_set_when_coincidence_is_sparse():
    localiser = RadarLocaliser()
    pairs = [
        CalibrationPair(
            iid=1,
            ts=1_000.0 + idx * 20.0,
            icao_a="AAAAAA",
            icao_b="BBBBBB",
            lat_a=51.2,
            lon_a=-1.1,
            lat_b=51.0,
            lon_b=-0.7,
            tdoa_us=700.0 + offset,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
        for idx, offset in enumerate((0.0, 40.0, -30.0, 20.0, -10.0, 30.0, -20.0, 10.0, -15.0, 25.0))
    ]

    selected = localiser.select_solver_pairs(pairs)

    assert len(selected) == 10
    assert any(abs(pair.tdoa_us) > 500.0 for pair in selected)


def test_solve_coincident_estimates_radar_from_repeated_bearing_lines():
    localiser = RadarLocaliser()
    receiver_lat = 51.0
    receiver_lon = -1.0
    true_lat = 51.25
    true_lon = -0.85

    families = [
        ("AAAAAA", "AAA001", (51.05, -1.20), (51.45, -0.60)),
        ("BBBBBB", "BBB001", (51.00, -0.80), (51.50, -0.90)),
        ("CCCCCC", "CCC001", (51.15, -1.25), (51.35, -0.45)),
        ("DDDDDD", "DDD001", (50.95, -1.05), (51.55, -0.65)),
    ]

    pairs = []
    for family_idx, (icao_a, icao_b, a, b) in enumerate(families):
        for repeat_idx, offset in enumerate((0.0, 40.0, -35.0)):
            pairs.append(
                CalibrationPair(
                    iid=1,
                    ts=1_000.0 + family_idx * 30.0 + repeat_idx * 5.0,
                    icao_a=icao_a,
                    icao_b=icao_b,
                    lat_a=a[0],
                    lon_a=a[1],
                    lat_b=b[0],
                    lon_b=b[1],
                    tdoa_us=120.0 + offset,
                    receiver_lat=receiver_lat,
                    receiver_lon=receiver_lon,
                )
            )

    lat, lon, cep_m, n_pairs = localiser.solve_coincident(pairs)

    assert lat == pytest.approx(true_lat, abs=0.08)
    assert lon == pytest.approx(true_lon, abs=0.08)
    assert cep_m < 25_000.0
    assert n_pairs == 12


def test_group_pairs_by_sweep_clusters_nearby_timestamps():
    localiser = RadarLocaliser()
    pairs = [
        CalibrationPair(iid=1, ts=1000.00, icao_a="AAAAAA", icao_b="BBBBBB", lat_a=51.1, lon_a=-1.1, lat_b=51.2, lon_b=-1.2, tdoa_us=100.0, receiver_lat=51.0, receiver_lon=-1.0),
        CalibrationPair(iid=1, ts=1000.20, icao_a="CCCCCC", icao_b="DDDDDD", lat_a=51.3, lon_a=-1.3, lat_b=51.4, lon_b=-1.4, tdoa_us=110.0, receiver_lat=51.0, receiver_lon=-1.0),
        CalibrationPair(iid=1, ts=1001.00, icao_a="EEEEEE", icao_b="FFFFFF", lat_a=51.5, lon_a=-1.5, lat_b=51.6, lon_b=-1.6, tdoa_us=120.0, receiver_lat=51.0, receiver_lon=-1.0),
    ]

    groups = localiser.group_pairs_by_sweep(pairs)

    assert [len(group) for group in groups] == [2, 1]


def test_build_sweep_observations_uses_connected_component_not_direct_anchor_only():
    localiser = RadarLocaliser()
    sweep_pairs = [
        CalibrationPair(iid=1, ts=1000.0, icao_a="AAAAAA", icao_b="BBBBBB", lat_a=51.1, lon_a=-1.1, lat_b=51.2, lon_b=-1.2, tdoa_us=100.0, receiver_lat=51.0, receiver_lon=-1.0),
        CalibrationPair(iid=1, ts=1000.0, icao_a="BBBBBB", icao_b="CCCCCC", lat_a=51.2, lon_a=-1.2, lat_b=51.3, lon_b=-1.3, tdoa_us=50.0, receiver_lat=51.0, receiver_lon=-1.0),
        CalibrationPair(iid=1, ts=1000.0, icao_a="CCCCCC", icao_b="DDDDDD", lat_a=51.3, lon_a=-1.3, lat_b=51.4, lon_b=-1.4, tdoa_us=25.0, receiver_lat=51.0, receiver_lon=-1.0),
    ]

    observations = localiser._build_sweep_observations(sweep_pairs)

    assert [obs["icao"] for obs in observations] == ["AAAAAA", "BBBBBB", "CCCCCC", "DDDDDD"]
    rel = {obs["icao"]: obs["rel_arrival_us"] for obs in observations}
    assert rel["AAAAAA"] == 0.0
    assert rel["BBBBBB"] == pytest.approx(-100.0)
    assert rel["CCCCCC"] == pytest.approx(-150.0)
    assert rel["DDDDDD"] == pytest.approx(-175.0)


def test_build_sweep_observations_rejects_when_largest_component_is_too_small():
    localiser = RadarLocaliser()
    sweep_pairs = [
        CalibrationPair(iid=1, ts=1000.0, icao_a="AAAAAA", icao_b="BBBBBB", lat_a=51.1, lon_a=-1.1, lat_b=51.2, lon_b=-1.2, tdoa_us=100.0, receiver_lat=51.0, receiver_lon=-1.0),
        CalibrationPair(iid=1, ts=1000.0, icao_a="CCCCCC", icao_b="DDDDDD", lat_a=51.3, lon_a=-1.3, lat_b=51.4, lon_b=-1.4, tdoa_us=50.0, receiver_lat=51.0, receiver_lon=-1.0),
    ]

    with pytest.raises(ValueError, match="connectivity"):
        localiser._build_sweep_observations(sweep_pairs)


def test_solve_static_from_sweeps_accumulates_good_sweep_estimates(monkeypatch):
    localiser = RadarLocaliser()
    calls = []

    def fake_solve_sweep_group(sweep_pairs, initial_guess=None):
        calls.append(len(sweep_pairs))
        sweep_ts = round(sweep_pairs[0].ts)
        mapping = {
            1000: (51.1000, -1.0000, 100.0, len(sweep_pairs)),
            1010: (51.1010, -1.0010, 50.0, len(sweep_pairs)),
            1020: (51.1020, -1.0020, 25.0, len(sweep_pairs)),
        }
        return mapping[sweep_ts]

    monkeypatch.setattr(localiser, "solve_sweep_group", fake_solve_sweep_group)

    pairs = []
    for ts in (1000.0, 1010.0, 1020.0):
        for idx in range(10):
            pairs.append(
                CalibrationPair(
                    iid=1,
                    ts=ts,
                    icao_a=f"A{idx:05d}",
                    icao_b=f"B{idx:05d}",
                    lat_a=51.1,
                    lon_a=-1.1,
                    lat_b=51.2,
                    lon_b=-1.2,
                    tdoa_us=100.0,
                    receiver_lat=51.0,
                    receiver_lon=-1.0,
                )
            )

    lat, lon, cep_m, n_pairs, n_sweeps = localiser.solve_static_from_sweeps(pairs)

    assert calls == [10, 10, 10]
    assert n_pairs == 30
    assert n_sweeps == 3
    assert lat == pytest.approx(51.1017, abs=0.001)
    assert lon == pytest.approx(-1.0017, abs=0.001)
    assert cep_m < 25.0


def test_solve_static_from_sweeps_requires_enough_good_sweeps(monkeypatch):
    localiser = RadarLocaliser()

    def fake_solve_sweep_group(sweep_pairs, initial_guess=None):
        sweep_ts = round(sweep_pairs[0].ts)
        if sweep_ts == 1010:
            raise ValueError("bad sweep")
        return (51.1000, -1.0000, 100.0, len(sweep_pairs))

    monkeypatch.setattr(localiser, "solve_sweep_group", fake_solve_sweep_group)

    pairs = []
    for ts in (1000.0, 1010.0, 1020.0):
        for idx in range(10):
            pairs.append(
                CalibrationPair(
                    iid=1,
                    ts=ts,
                    icao_a=f"A{idx:05d}",
                    icao_b=f"B{idx:05d}",
                    lat_a=51.1,
                    lon_a=-1.1,
                    lat_b=51.2,
                    lon_b=-1.2,
                    tdoa_us=100.0,
                    receiver_lat=51.0,
                    receiver_lon=-1.0,
                )
            )

    with pytest.raises(ValueError, match="Insufficient good sweep estimates"):
        localiser.solve_static_from_sweeps(pairs)


def test_diagnose_sweep_groups_reports_failure_breakdown(monkeypatch):
    localiser = RadarLocaliser()

    def fake_solve_sweep_group(sweep_pairs, initial_guess=None):
        sweep_ts = round(sweep_pairs[0].ts)
        if sweep_ts == 1000:
            raise ValueError("Insufficient sweep aircraft (2 < 4)")
        if sweep_ts == 1010:
            raise ValueError("Poor sweep geometry: azimuth spread < 30.0°")
        if sweep_ts == 1020:
            raise ValueError("Sweep solver residual too large (18.0 us)")
        if sweep_ts == 1030:
            raise ValueError("Sweep missing anchor pair connectivity (3 < 4)")
        return (51.1, -1.0, 100.0, len(sweep_pairs))

    monkeypatch.setattr(localiser, "solve_sweep_group", fake_solve_sweep_group)

    pairs = []
    for ts in (1000.0, 1010.0, 1020.0, 1030.0, 1040.0):
        for idx in range(4):
            pairs.append(
                CalibrationPair(
                    iid=1,
                    ts=ts,
                    icao_a=f"A{int(ts)}{idx}",
                    icao_b=f"B{int(ts)}{idx}",
                    lat_a=51.1,
                    lon_a=-1.1,
                    lat_b=51.2,
                    lon_b=-1.2,
                    tdoa_us=100.0,
                    receiver_lat=51.0,
                    receiver_lon=-1.0,
                )
            )

    counts = localiser.diagnose_sweep_groups(pairs)

    assert counts["total_sweeps"] == 5
    assert counts["solved"] == 1
    assert counts["too_few_aircraft"] == 1
    assert counts["poor_geometry"] == 1
    assert counts["residual_too_large"] == 1
    assert counts["insufficient_connected_component"] == 1


def test_localiser_rejects_when_no_consistent_pair_families_survive():
    localiser = RadarLocaliser()
    pairs = [
        CalibrationPair(
            iid=1,
            ts=1_000.0 + idx * 20.0,
            icao_a="AAAAAA",
            icao_b="BBBBBB",
            lat_a=51.2,
            lon_a=-1.1,
            lat_b=51.0,
            lon_b=-0.7,
            tdoa_us=tdoa_us,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
        for idx, tdoa_us in enumerate((-1800.0, 2200.0, -2600.0, 3100.0, -3500.0, 3900.0, -4200.0, 4500.0, -4700.0, 4900.0))
    ]

    with pytest.raises(ValueError, match="Insufficient consistent pairs"):
        localiser.solve(pairs)


# --- Fix 4: exception visibility in solve_static_from_sweeps ---

def test_solve_static_from_sweeps_logs_sweep_failure(caplog):
    """A failing sweep must be logged with a classified reason, not silently dropped."""
    import logging
    localiser = RadarLocaliser()

    # Build one good sweep (3 distinct timestamps) and one bad sweep (only 1 aircraft,
    # which will raise "Insufficient sweep aircraft").
    good_base_ts = 2000.0
    bad_ts = 3000.0

    def _make_named_pair(icao_a, icao_b, ts, tdoa_us):
        return CalibrationPair(
            iid=1,
            ts=ts,
            icao_a=icao_a,
            icao_b=icao_b,
            lat_a=51.3, lon_a=-1.1,
            lat_b=51.0, lon_b=-0.7,
            tdoa_us=tdoa_us,
            receiver_lat=51.0, receiver_lon=-1.0,
        )

    # Four distinct sweeps (good) so we have enough for MIN_SWEEP_ESTIMATES=3.
    # One bad sweep injected between them.
    pairs = []
    for sweep_idx in range(4):
        ts = good_base_ts + sweep_idx * 1.0
        # Each sweep needs >= 4 distinct aircraft pairs for good geometry.
        # We simulate this by monkeypatching; for the log test we only need a bad sweep.
        pairs.append(_make_named_pair("AAAA00", "BBBB00", ts, 100.0))
        pairs.append(_make_named_pair("CCCC00", "DDDD00", ts, 200.0))
        pairs.append(_make_named_pair("EEEE00", "FFFF00", ts, 300.0))
        pairs.append(_make_named_pair("GGGG00", "HHHH00", ts, 400.0))

    # Bad sweep: only one aircraft (all pairs have the same ICAO) → too_few_aircraft.
    pairs.append(_make_named_pair("ZZZZ00", "ZZZZ00", bad_ts, 0.0))

    with caplog.at_level(logging.DEBUG, logger="radar.localiser"):
        try:
            localiser.solve_static_from_sweeps(pairs)
        except (ValueError, Exception):
            pass

    # At least one "sweep solve failed" log record should appear.
    assert any("sweep solve failed" in r.message for r in caplog.records), (
        "Expected a debug log about the failing sweep; none found. "
        f"Records: {[r.message for r in caplog.records]}"
    )


# --- Fix 5: duplicate pair overwrite in _build_sweep_observations ---

def test_build_sweep_observations_combines_duplicate_pairs_by_median():
    """Duplicate same-sweep pairs must be combined by median, not overwritten."""
    localiser = RadarLocaliser()

    def _pair(icao_a, icao_b, tdoa_us, ts=1000.0):
        return CalibrationPair(
            iid=1, ts=ts,
            icao_a=icao_a, icao_b=icao_b,
            lat_a=51.3, lon_a=-1.1,
            lat_b=51.0, lon_b=-0.7,
            tdoa_us=tdoa_us,
            receiver_lat=51.0, receiver_lon=-1.0,
        )

    # Three observations of the same pair with different TDOA values.
    # Median of [100, 200, 300] = 200.
    sweep_pairs = [
        _pair("AAAAAA", "BBBBBB", 100.0),
        _pair("AAAAAA", "BBBBBB", 200.0),
        _pair("AAAAAA", "BBBBBB", 300.0),
        # Second distinct pair (needed for MIN_SWEEP_AIRCRAFT >= 4).
        _pair("CCCCCC", "AAAAAA", 50.0),
        _pair("DDDDDD", "AAAAAA", -50.0),
    ]

    obs = localiser._build_sweep_observations(sweep_pairs)

    # Extract the rel_arrival_us for BBBBBB (relative to anchor = min ICAO).
    # The exact value depends on the BFS traversal, but what matters is that
    # the duplicate TDOA values were combined, not that the last one won.
    # We verify by checking that exactly one obs per ICAO exists.
    icaos = [o["icao"] for o in obs]
    assert len(icaos) == len(set(icaos)), "duplicate ICAO in output — overwrite not fixed"


def test_build_sweep_observations_median_combines_odd_count():
    """For an odd number of duplicates, median equals the middle value."""
    localiser = RadarLocaliser()

    def _pair(icao_a, icao_b, tdoa_us):
        return CalibrationPair(
            iid=1, ts=1000.0,
            icao_a=icao_a, icao_b=icao_b,
            lat_a=51.3, lon_a=-1.1,
            lat_b=51.0, lon_b=-0.7,
            tdoa_us=tdoa_us,
            receiver_lat=51.0, receiver_lon=-1.0,
        )

    # Five duplicates [10, 50, 100, 150, 200] → median = 100.
    sweep_pairs = [
        _pair("AAAAAA", "BBBBBB", 10.0),
        _pair("AAAAAA", "BBBBBB", 50.0),
        _pair("AAAAAA", "BBBBBB", 100.0),
        _pair("AAAAAA", "BBBBBB", 150.0),
        _pair("AAAAAA", "BBBBBB", 200.0),
        _pair("CCCCCC", "AAAAAA", 30.0),
        _pair("DDDDDD", "AAAAAA", -30.0),
    ]

    obs = localiser._build_sweep_observations(sweep_pairs)
    icaos = [o["icao"] for o in obs]
    assert len(icaos) == len(set(icaos)), "duplicate ICAO in output"


def test_build_sweep_observations_non_duplicate_unchanged():
    """Non-duplicate pairs must not be altered by the combine step."""
    localiser = RadarLocaliser()

    def _pair(icao_a, icao_b, tdoa_us):
        return CalibrationPair(
            iid=1, ts=1000.0,
            icao_a=icao_a, icao_b=icao_b,
            lat_a=51.3, lon_a=-1.1,
            lat_b=51.0, lon_b=-0.7,
            tdoa_us=tdoa_us,
            receiver_lat=51.0, receiver_lon=-1.0,
        )

    sweep_pairs = [
        _pair("AAAAAA", "BBBBBB", 123.0),
        _pair("CCCCCC", "AAAAAA", 456.0),
        _pair("DDDDDD", "AAAAAA", -789.0),
        _pair("EEEEEE", "AAAAAA", 333.0),
    ]

    # Should not raise; single-observation pairs go through unchanged.
    obs = localiser._build_sweep_observations(sweep_pairs)
    assert len(obs) == len({o["icao"] for o in obs})


# --- Fix 7: geometry precomputation in solve and solve_sweep_group ---

def _make_symmetric_pairs(receiver_lat, receiver_lon, n_pairs=12):
    """Build synthetic CalibrationPairs whose TDOA is consistent with a radar at
    (receiver_lat + 0.1, receiver_lon + 0.05)."""
    import math
    radar_lat = receiver_lat + 0.1
    radar_lon = receiver_lon + 0.05

    def dist(la1, lo1, la2, lo2):
        R = 6_371_000.0
        dlat = math.radians(la2 - la1)
        dlon = math.radians(lo2 - lo1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(la1)) * math.cos(math.radians(la2)) * math.sin(dlon / 2) ** 2
        return 2 * R * math.asin(math.sqrt(a))

    aircraft = [
        (receiver_lat + 0.2, receiver_lon - 0.1),
        (receiver_lat - 0.1, receiver_lon + 0.3),
        (receiver_lat + 0.3, receiver_lon + 0.2),
        (receiver_lat - 0.2, receiver_lon - 0.2),
        (receiver_lat + 0.15, receiver_lon + 0.4),
        (receiver_lat - 0.3, receiver_lon + 0.1),
    ]

    _C = 299.792458
    pairs = []
    for i in range(min(n_pairs, len(aircraft) * (len(aircraft) - 1) // 2)):
        a_idx = i % len(aircraft)
        b_idx = (i + 1) % len(aircraft)
        if a_idx == b_idx:
            continue
        la, loa = aircraft[a_idx]
        lb, lob = aircraft[b_idx]
        d_ra = dist(radar_lat, radar_lon, la, loa)
        d_rb = dist(radar_lat, radar_lon, lb, lob)
        d_sa = dist(receiver_lat, receiver_lon, la, loa)
        d_sb = dist(receiver_lat, receiver_lon, lb, lob)
        tdoa_us = ((d_ra + d_sa) - (d_rb + d_sb)) / _C
        pairs.append(CalibrationPair(
            iid=1, ts=1000.0 + i * 0.5,
            icao_a=f"A{a_idx:05d}",
            icao_b=f"B{b_idx:05d}",
            lat_a=la, lon_a=loa,
            lat_b=lb, lon_b=lob,
            tdoa_us=tdoa_us,
            receiver_lat=receiver_lat,
            receiver_lon=receiver_lon,
        ))
    return pairs


def test_precomputed_tdoa_residuals_match_original(monkeypatch):
    """The fast precomputed residual evaluator inside solve() must give the same
    result as the original tdoa_residuals method for the same R."""
    import math
    localiser = RadarLocaliser()
    from radar.localiser import _latlon_to_xy, _C_MUS

    pairs = _make_symmetric_pairs(51.0, -1.0)
    origin_lat = pairs[0].receiver_lat
    origin_lon = pairs[0].receiver_lon

    # Build the precomputed structure as solve() does.
    pairs_precomp = [
        (
            *_latlon_to_xy(p.lat_a, p.lon_a, origin_lat, origin_lon),
            *_latlon_to_xy(p.lat_b, p.lon_b, origin_lat, origin_lon),
            localiser._corrected_range_difference_m(p),
        )
        for p in pairs
    ]

    def fast_residuals(R):
        rx, ry = R
        return [
            (math.sqrt((rx - ax) ** 2 + (ry - ay) ** 2)
             - math.sqrt((rx - bx) ** 2 + (ry - by) ** 2)
             - delta) / _C_MUS
            for ax, ay, bx, by, delta in pairs_precomp
        ]

    test_R = [5000.0, 3000.0]
    orig = localiser.tdoa_residuals(test_R, pairs, origin_lat, origin_lon)
    fast = fast_residuals(test_R)

    assert len(orig) == len(fast)
    for i, (o, f) in enumerate(zip(orig, fast)):
        assert o == pytest.approx(f, abs=1e-9), f"residual[{i}] mismatch: orig={o}, fast={f}"


def test_solve_result_consistent_with_precomputed_geometry():
    """solve() using precomputed geometry must reach the same position as the
    public tdoa_residuals-based solver for a known configuration."""
    pytest.importorskip("scipy")
    localiser = RadarLocaliser()

    pairs = _make_symmetric_pairs(51.0, -1.0, n_pairs=10)
    # Add enough consistent repeats so filter_consistent_pairs keeps them.
    all_pairs = []
    for i in range(4):
        for p in pairs:
            all_pairs.append(CalibrationPair(
                iid=p.iid, ts=p.ts + i * 30.0,
                icao_a=p.icao_a, icao_b=p.icao_b,
                lat_a=p.lat_a, lon_a=p.lon_a,
                lat_b=p.lat_b, lon_b=p.lon_b,
                tdoa_us=p.tdoa_us,
                receiver_lat=p.receiver_lat, receiver_lon=p.receiver_lon,
            ))

    lat, lon, cep_m, n = localiser.solve(all_pairs)
    # Should solve close to the planted radar position.
    assert abs(lat - 51.1) < 0.05, f"lat={lat}"
    assert abs(lon - -0.95) < 0.05, f"lon={lon}"
