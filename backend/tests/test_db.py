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
from pathlib import Path

import config


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
