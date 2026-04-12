"""
Unit tests for StatsDB registry upsert logic.

Uses a temp file DB so each test gets a clean schema.
The sighting_count increment rule (only when gap > 3600s) is the most
subtle logic in the upsert and is exercised thoroughly here.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
import sqlite3

import config
from datetime import datetime, timezone
from radar.models import CalibrationPair


@pytest.fixture()
def db(tmp_path, monkeypatch):
    """Provide a StatsDB instance backed by a temp file, with HOME_COUNTRY cleared."""
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr(config, "HOME_COUNTRY", "")
    # Import after patching so _init_schema uses the temp path
    import importlib
    import db as db_module
    importlib.reload(db_module)
    instance = db_module.StatsDB()
    return instance


def _read_aircraft(db_instance, icao: str) -> dict | None:
    """Helper: read a single aircraft row as a plain dict."""
    with db_instance._connect() as conn:
        row = conn.execute(
            "SELECT * FROM aircraft_registry WHERE icao = ?", (icao,)
        ).fetchone()
    return dict(row) if row else None


def _upsert(db_instance, ac: dict, ts: int) -> None:
    """Helper: upsert an aircraft inside a committed transaction."""
    with db_instance._connect() as conn:
        db_instance._upsert_aircraft(conn, ac, ts)


# ---------------------------------------------------------------------------
# Basic insert / round-trip
# ---------------------------------------------------------------------------

class TestUpsertBasic:
    def test_new_aircraft_inserted(self, db):
        ac = {"icao": "AABBCC", "registration": "G-TEST", "type_code": "B738",
              "type_category": "A3", "military": 0, "country": "United Kingdom"}
        _upsert(db, ac, 1000)
        row = _read_aircraft(db, "AABBCC")
        assert row is not None
        assert row["registration"] == "G-TEST"
        assert row["type_code"] == "B738"
        assert row["sighting_count"] == 1
        assert row["first_seen"] == 1000
        assert row["last_seen"] == 1000

    def test_fields_stored_correctly(self, db):
        ac = {"icao": "112233", "registration": "N12345", "type_code": "C172",
              "military": 0, "country": "United States",
              "operator": "Private", "manufacturer": "Cessna", "year": "2005",
              "lat": 51.5, "lon": -0.1}
        _upsert(db, ac, 2000)
        row = _read_aircraft(db, "112233")
        assert row["operator"] == "Private"
        assert row["manufacturer"] == "Cessna"
        assert row["year"] == "2005"
        assert abs(row["lat"] - 51.5) < 0.001
        assert abs(row["lon"] - (-0.1)) < 0.001


# ---------------------------------------------------------------------------
# sighting_count increment logic
# ---------------------------------------------------------------------------

class TestSightingCount:
    def test_same_session_no_increment(self, db):
        """Second upsert within 3600s must NOT increment sighting_count."""
        ac = {"icao": "AABBCC", "registration": "G-TEST", "military": 0}
        _upsert(db, ac, 1000)
        _upsert(db, ac, 1000 + 3600)  # exactly 3600 — not > 3600
        row = _read_aircraft(db, "AABBCC")
        assert row["sighting_count"] == 1

    def test_new_session_increments(self, db):
        """Second upsert more than 3600s later MUST increment sighting_count."""
        ac = {"icao": "AABBCC", "registration": "G-TEST", "military": 0}
        _upsert(db, ac, 1000)
        _upsert(db, ac, 1000 + 3601)
        row = _read_aircraft(db, "AABBCC")
        assert row["sighting_count"] == 2

    def test_multiple_sessions_count_correctly(self, db):
        ac = {"icao": "AABBCC", "registration": "G-TEST", "military": 0}
        base = 10000
        _upsert(db, ac, base)
        _upsert(db, ac, base + 3601)   # +1 → 2
        _upsert(db, ac, base + 7202)   # +1 → 3
        _upsert(db, ac, base + 7500)   # within 298s of previous → still 3
        row = _read_aircraft(db, "AABBCC")
        assert row["sighting_count"] == 3

    def test_last_seen_always_updated(self, db):
        ac = {"icao": "AABBCC", "military": 0}
        _upsert(db, ac, 1000)
        _upsert(db, ac, 2000)
        row = _read_aircraft(db, "AABBCC")
        assert row["last_seen"] == 2000

    def test_first_seen_not_overwritten(self, db):
        ac = {"icao": "AABBCC", "military": 0}
        _upsert(db, ac, 1000)
        _upsert(db, ac, 5000)
        row = _read_aircraft(db, "AABBCC")
        assert row["first_seen"] == 1000  # unchanged


# ---------------------------------------------------------------------------
# COALESCE update logic — existing values not overwritten by None
# ---------------------------------------------------------------------------

class TestCoalesceUpdate:
    def test_registration_not_overwritten_by_none(self, db):
        _upsert(db, {"icao": "AABBCC", "registration": "G-TEST", "military": 0}, 1000)
        _upsert(db, {"icao": "AABBCC", "military": 0}, 5000)
        row = _read_aircraft(db, "AABBCC")
        assert row["registration"] == "G-TEST"

    def test_type_code_not_overwritten_by_none(self, db):
        _upsert(db, {"icao": "AABBCC", "type_code": "B738", "military": 0}, 1000)
        _upsert(db, {"icao": "AABBCC", "military": 0}, 5000)
        row = _read_aircraft(db, "AABBCC")
        assert row["type_code"] == "B738"


class TestRadarCalibration:
    def test_load_calibration_pairs_dedupes_duplicate_observations(self, db):
        pair = CalibrationPair(
            iid=7,
            ts=datetime.now(timezone.utc).timestamp(),
            icao_a="AAAAAA",
            icao_b="BBBBBB",
            lat_a=51.23456,
            lon_a=-1.23456,
            lat_b=52.34567,
            lon_b=-2.34567,
            tdoa_us=12.3456,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
        db.insert_calibration_pair(pair)
        db.insert_calibration_pair(pair)

        rows = db.load_calibration_pairs(7)

        assert len(rows) == 1
        assert rows[0]["icao_a"] == "AAAAAA"
        assert rows[0]["icao_b"] == "BBBBBB"

    def test_insert_calibration_pairs_batches_multiple_rows(self, db):
        ts = datetime.now(timezone.utc).timestamp()
        pairs = [
            CalibrationPair(
                iid=7,
                ts=ts,
                icao_a="AAAAAA",
                icao_b="BBBBBB",
                lat_a=51.23456,
                lon_a=-1.23456,
                lat_b=52.34567,
                lon_b=-2.34567,
                tdoa_us=12.3456,
                receiver_lat=51.0,
                receiver_lon=-1.0,
            ),
            CalibrationPair(
                iid=7,
                ts=ts + 1.0,
                icao_a="CCCCCC",
                icao_b="DDDDDD",
                lat_a=51.33456,
                lon_a=-1.33456,
                lat_b=52.44567,
                lon_b=-2.44567,
                tdoa_us=-6.789,
                receiver_lat=51.0,
                receiver_lon=-1.0,
            ),
        ]

        db.insert_calibration_pairs(pairs)

        rows = db.load_calibration_pairs(7)

        assert len(rows) == 2
        assert {(row["icao_a"], row["icao_b"]) for row in rows} == {
            ("AAAAAA", "BBBBBB"),
            ("CCCCCC", "DDDDDD"),
        }

    def test_count_calibration_pairs_uses_recent_iid_window(self, db):
        ts = datetime.now(timezone.utc).timestamp()
        db.insert_calibration_pairs([
            CalibrationPair(
                iid=7,
                ts=ts,
                icao_a="AAAAAA",
                icao_b="BBBBBB",
                lat_a=51.23456,
                lon_a=-1.23456,
                lat_b=52.34567,
                lon_b=-2.34567,
                tdoa_us=12.3456,
                receiver_lat=51.0,
                receiver_lon=-1.0,
            ),
            CalibrationPair(
                iid=7,
                ts=ts - 91 * 86400,
                icao_a="CCCCCC",
                icao_b="DDDDDD",
                lat_a=51.33456,
                lon_a=-1.33456,
                lat_b=52.44567,
                lon_b=-2.44567,
                tdoa_us=-6.789,
                receiver_lat=51.0,
                receiver_lon=-1.0,
            ),
            CalibrationPair(
                iid=8,
                ts=ts,
                icao_a="EEEEEE",
                icao_b="FFFFFF",
                lat_a=51.43456,
                lon_a=-1.43456,
                lat_b=52.54567,
                lon_b=-2.54567,
                tdoa_us=3.21,
                receiver_lat=51.0,
                receiver_lon=-1.0,
            ),
        ])

        assert db.count_calibration_pairs(7) == 1
        assert db.count_calibration_pairs(8) == 1
        assert db.count_calibration_pairs(9) == 0

    def test_clear_radar_learning_deletes_models_and_calibration_rows(self, db):
        pair = CalibrationPair(
            iid=7,
            ts=datetime.now(timezone.utc).timestamp(),
            icao_a="AAAAAA",
            icao_b="BBBBBB",
            lat_a=51.23456,
            lon_a=-1.23456,
            lat_b=52.34567,
            lon_b=-2.34567,
            tdoa_us=12.3456,
            receiver_lat=51.0,
            receiver_lon=-1.0,
        )
        db.insert_calibration_pair(pair)
        with db._connect() as conn:
            conn.execute(
                """
                INSERT INTO radar_iids
                    (iid, status, period_s, last_updated, primary_support_count, secondary_support_count)
                VALUES (?,?,?,?,?,?)
                """,
                (7, "SINGLE_RADAR", 4.0, datetime.now(timezone.utc).timestamp(), 8, 0),
            )

        deleted = db.clear_radar_learning()

        assert deleted == {"radar_iids_deleted": 1, "calibration_deleted": 1}
        with db._connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM radar_iids").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM radar_calibration").fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Ghost purge
# ---------------------------------------------------------------------------

class TestGhostPurge:
    def test_purge_removes_unenriched_single_sighting(self, db):
        _upsert(db, {"icao": "DEAD00", "military": 0}, 1000)
        db.purge_ghost_aircraft()
        assert _read_aircraft(db, "DEAD00") is None

    def test_purge_keeps_aircraft_with_registration(self, db):
        _upsert(db, {"icao": "BEEF01", "registration": "G-REAL", "military": 0}, 1000)
        db.purge_ghost_aircraft()
        assert _read_aircraft(db, "BEEF01") is not None

    def test_purge_keeps_multi_sighting_aircraft(self, db):
        _upsert(db, {"icao": "CAFE02", "military": 0}, 1000)
        with db._connect() as conn:
            conn.execute("UPDATE aircraft_registry SET sighting_count=2 WHERE icao=?", ("CAFE02",))
        db.purge_ghost_aircraft()
        assert _read_aircraft(db, "CAFE02") is not None


class TestPositionDecodeRate:
    def test_prefers_adsb_when_aircraft_had_both_adsb_and_mlat_positions(self, db):
        day = "2026-03-28"

        def _ts(hour: int) -> int:
            return int(datetime(2026, 3, 28, hour, 0, tzinfo=timezone.utc).timestamp())

        with db._connect() as conn:
            conn.executemany(
                "INSERT INTO daily_aircraft_seen (date, icao, mlat, had_pos) VALUES (?,?,?,?)",
                [
                    (day, "ADSB01", 0, 1),
                    (day, "MIXED1", 1, 1),
                    (day, "MLAT01", 1, 1),
                    (day, "NOPOS1", 0, 0),
                ],
            )
            conn.executemany(
                """
                INSERT INTO coverage_samples
                    (ts, icao, bearing_deg, range_nm, altitude, signal, mlat)
                VALUES (?,?,?,?,?,?,?)
                """,
                [
                    (_ts(1), "ADSB01", 10.0, 20.0, 10000, 80, 0),
                    (_ts(2), "MIXED1", 20.0, 30.0, 11000, 82, 1),
                    (_ts(3), "MIXED1", 20.0, 31.0, 11100, 78, 0),
                    (_ts(4), "MLAT01", 30.0, 40.0, 12000, 75, 1),
                ],
            )

        rows = db.query_position_decode_rate(days=365)
        row = next(r for r in rows if r["date"] == day)

        assert row["adsb_pct"] == 50.0
        assert row["mlat_pct"] == 25.0
        assert row["no_pos_pct"] == 25.0
        assert round(row["adsb_pct"] + row["mlat_pct"] + row["no_pos_pct"], 1) == 100.0


class TestForwardModelFields:
    """Verify FM fields persist through DB upsert and reload cycle."""

    def test_fm_fields_round_trip_through_db(self, db):
        from radar.models import RadarIID
        from radar.sweep import RadarState

        # Create a model with FM fields
        model = RadarIID(
            iid=42,
            status="SINGLE_RADAR",
            period_s=4.0,
            rpm=14.93,
            last_updated=1000.0,
            fm_lat=51.4706,
            fm_lon=-0.4619,
            fm_cep_m=2500.0,
            fm_source="airport_prior",
        )
        db.upsert_radar_iid(model)

        # Reload and verify FM fields survive
        rows = db.load_radar_iids()
        assert len(rows) == 1
        row = rows[0]
        assert row["fm_lat"] == 51.4706
        assert row["fm_lon"] == -0.4619
        assert row["fm_cep_m"] == 2500.0
        assert row["fm_source"] == "airport_prior"

        # Verify RadarState.load_from_db restores FM fields
        state = RadarState()
        state.load_from_db(rows)
        loaded = state._models[42]
        assert loaded.fm_lat == 51.4706
        assert loaded.fm_lon == -0.4619
        assert loaded.fm_cep_m == 2500.0
        assert loaded.fm_source == "airport_prior"

    def test_update_forward_model_location_stores_fm_fields(self):
        from radar.sweep import RadarState

        state = RadarState()
        state.update_forward_model_location(
            iid=7,
            lat=52.1234,
            lon=-1.5678,
            cep_m=3000.0,
            n_observations=450,
            window_s=600.0,
            source="airport_prior",
        )

        model = state._models[7]
        assert model.fm_lat == 52.1234
        assert model.fm_lon == -1.5678
        assert model.fm_cep_m == 3000.0
        assert model.fm_source == "airport_prior"


class TestRadarResolutionControlFields:
    """Verify localisation control fields persist through DB and state load."""

    def test_locked_position_fields_round_trip_through_db(self, db):
        from radar.models import RadarIID
        from radar.sweep import RadarState

        model = RadarIID(
            iid=62,
            resolution_mode="locked_position",
            status="SINGLE_RADAR",
            manual_lat=51.5033,
            manual_lon=-0.1195,
            manual_note="Pinned from satellite photo",
            manual_updated_ts=2000.0,
            last_updated=2100.0,
        )
        db.upsert_radar_iid(model)

        rows = db.load_radar_iids()
        assert len(rows) == 1
        row = rows[0]
        assert row["resolution_mode"] == "locked_position"
        assert row["manual_lat"] == 51.5033
        assert row["manual_lon"] == -0.1195
        assert row["manual_note"] == "Pinned from satellite photo"
        assert row["manual_updated_ts"] == 2000.0

        state = RadarState()
        state.load_from_db(rows)
        loaded = state._models[62]
        assert loaded.resolution_mode == "locked_position"
        assert loaded.manual_lat == 51.5033
        assert loaded.manual_lon == -0.1195
        assert loaded.manual_note == "Pinned from satellite photo"
        assert loaded.manual_updated_ts == 2000.0

    def test_locked_unresolvable_fields_round_trip_through_db(self, db):
        from radar.models import RadarIID
        from radar.sweep import RadarState

        model = RadarIID(
            iid=91,
            resolution_mode="locked_unresolvable",
            status="INSUFFICIENT_DATA",
            unresolvable_reason="No coherent pulse family after visual inspection",
            unresolvable_updated_ts=3000.0,
            last_updated=3100.0,
        )
        db.upsert_radar_iid(model)

        rows = db.load_radar_iids()
        assert len(rows) == 1
        row = rows[0]
        assert row["resolution_mode"] == "locked_unresolvable"
        assert row["unresolvable_reason"] == "No coherent pulse family after visual inspection"
        assert row["unresolvable_updated_ts"] == 3000.0

        state = RadarState()
        state.load_from_db(rows)
        loaded = state._models[91]
        assert loaded.resolution_mode == "locked_unresolvable"
        assert loaded.unresolvable_reason == "No coherent pulse family after visual inspection"
        assert loaded.unresolvable_updated_ts == 3000.0


class TestCoincidentIlluminationFields:
    def test_ci_fields_round_trip_through_db(self, db):
        from radar.models import RadarIID
        from radar.sweep import RadarState

        model = RadarIID(
            iid=77,
            status="SINGLE_RADAR",
            ci_lat=51.4001,
            ci_lon=-1.4002,
            ci_cep_m=1800.0,
            ci_source="coincident_intersection",
            ci_n_pairs=12,
            ci_last_updated=4000.0,
        )
        db.upsert_radar_iid(model)

        rows = db.load_radar_iids()
        assert len(rows) == 1
        row = rows[0]
        assert row["ci_lat"] == 51.4001
        assert row["ci_lon"] == -1.4002
        assert row["ci_cep_m"] == 1800.0
        assert row["ci_source"] == "coincident_intersection"
        assert row["ci_n_pairs"] == 12
        assert row["ci_last_updated"] == 4000.0

        state = RadarState()
        state.load_from_db(rows)
        loaded = state._models[77]
        assert loaded.ci_lat == 51.4001
        assert loaded.ci_lon == -1.4002
        assert loaded.ci_cep_m == 1800.0
        assert loaded.ci_source == "coincident_intersection"
        assert loaded.ci_n_pairs == 12
        assert loaded.ci_last_updated == 4000.0

    def test_upsert_radar_iids_batches_multiple_models(self, db):
        from radar.models import RadarIID

        models = [
            RadarIID(iid=10, status="SINGLE_RADAR", period_s=4.0, last_updated=1000.0),
            RadarIID(iid=11, status="LIKELY_SINGLE", period_s=5.0, last_updated=1001.0),
        ]

        db.upsert_radar_iids(models)

        rows = db.load_radar_iids()
        assert {row["iid"] for row in rows} == {10, 11}


class TestLockRetry:
    def test_write_visits_retries_transient_database_lock(self, db, monkeypatch):
        real_connect = db._connect
        calls = {"count": 0}

        class _LockingConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def executemany(self, *args, **kwargs):
                calls["count"] += 1
                if calls["count"] == 1:
                    raise sqlite3.OperationalError("database is locked")
                return real_connect().executemany(*args, **kwargs)

        monkeypatch.setattr(db, "_connect", lambda: _LockingConnection() if calls["count"] == 0 else real_connect())

        ids = db.write_visits([
            ("AABBCC", 1, 2, "TEST1", "7000", 12000, 10),
            ("DDEEFF", 3, 4, "TEST2", "7001", 15000, 11),
        ])

        assert calls["count"] == 1
        assert len(ids) == 2
        rows = db.query_visits("AABBCC")
        assert len(rows) == 1

    def test_upsert_radar_iid_retries_transient_database_lock(self, db, monkeypatch):
        from radar.models import RadarIID

        real_connect = db._connect
        calls = {"count": 0}

        class _LockingConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *args, **kwargs):
                calls["count"] += 1
                if calls["count"] == 1:
                    raise sqlite3.OperationalError("database is locked")
                return real_connect().execute(*args, **kwargs)

        monkeypatch.setattr(db, "_connect", lambda: _LockingConnection() if calls["count"] == 0 else real_connect())

        model = RadarIID(
            iid=7,
            status="SINGLE_RADAR",
            period_s=4.0,
            rpm=15.0,
            last_updated=1000.0,
        )
        db.upsert_radar_iid(model)

        assert calls["count"] == 1
        rows = db.load_radar_iids()
        assert len(rows) == 1
        assert rows[0]["iid"] == 7

    def test_fm_convergence_history_bounded(self):
        from radar.sweep import RadarState

        state = RadarState()
        for i in range(60):
            state.update_forward_model_location(
                iid=1, lat=50.0 + i * 0.001, lon=-1.0,
                cep_m=1000.0, n_observations=100, window_s=600.0,
            )

        assert len(state._models[1].fm_convergence_history) == 50


class TestSignalSemantics:
    def test_query_scatter_returns_canonical_dbfs(self, db):
        now_ts = int(datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc).timestamp())
        with db._connect() as conn:
            conn.execute(
                """
                INSERT INTO minute_stats
                    (ts, msg_min, msg_max, msg_mean, ac_total, ac_civil, ac_military,
                     signal_avg, signal_min, signal_max, ac_with_pos, ac_mlat)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (now_ts, 1, 2, 3.0, 4, 3, 1, 50.0, 20.0, 80.0, 2, 0),
            )

        rows = db.query_receiver_scatter(days=365)

        assert rows == [{"ts": now_ts, "ac": 4, "msgs": 180, "signal": -14.2}]

    def test_query_skyview_points_returns_canonical_dbfs(self, db):
        now_ts = int(datetime.now(timezone.utc).timestamp())
        db.write_coverage_tuples([
            (now_ts, "ABC123", 123.4, 45.6, 12000, 84, 0),
        ])

        payload = db.query_skyview_points(hours=24)

        assert payload["hours"] == 24
        assert payload["points"] == [[123.4, 45.6, 12000, -9.6]]
