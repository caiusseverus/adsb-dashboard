"""Position quality checker comparing internal state with readsb aircraft.json."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import urllib.request
from collections import deque
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import APIRouter, HTTPException

import config

router = APIRouter(prefix="/api/position-quality")
log = logging.getLogger(__name__)

_state = None
_checker = None

_R_NM = 3440.065


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return _R_NM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class PositionQualityChecker:
    def __init__(self, state: Any):
        self.state = state
        self._lock = Lock()
        self._rows: dict[str, dict] = {}
        self._history: dict[str, deque] = {}
        self._last_update: float | None = None

    def _read_readsb_json(self) -> dict[str, Any] | None:
        payload: bytes | None = None
        src = None
        local_path = Path(config.READSB_AIRCRAFT_JSON_PATH)
        if local_path.exists():
            try:
                payload = local_path.read_bytes()
                src = str(local_path)
            except Exception:
                payload = None

        if payload is None:
            try:
                with urllib.request.urlopen(config.READSB_AIRCRAFT_JSON_URL, timeout=1.5) as resp:
                    payload = resp.read()
                    src = config.READSB_AIRCRAFT_JSON_URL
            except Exception:
                return None

        try:
            data = json.loads(payload)
            data["_source"] = src
            return data
        except Exception:
            return None

    def _index_readsb(self, payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        by_icao: dict[str, dict[str, Any]] = {}
        for item in payload.get("aircraft", []):
            icao = (item.get("hex") or "").upper()
            lat = item.get("lat")
            lon = item.get("lon")
            if not icao or lat is None or lon is None:
                continue
            by_icao[icao] = item
        return by_icao

    def tick(self) -> None:
        readsb_payload = self._read_readsb_json()
        if readsb_payload is None:
            return
        readsb_by_icao = self._index_readsb(readsb_payload)
        snapshot = self.state.get_snapshot()
        now = time.time()

        next_rows: dict[str, dict] = {}
        next_history_seen: set[str] = set()
        for ac in snapshot.get("aircraft", []):
            icao = (ac.get("icao") or "").upper()
            if not icao:
                continue
            readsb = readsb_by_icao.get(icao)
            if not readsb:
                continue

            ilat, ilon = ac.get("lat"), ac.get("lon")
            rlat, rlon = readsb.get("lat"), readsb.get("lon")
            if ilat is None or ilon is None or rlat is None or rlon is None:
                continue

            horiz_err_m = round(_haversine_nm(ilat, ilon, rlat, rlon) * 1852, 1)
            ialt = ac.get("altitude")
            ralt = readsb.get("alt_baro")
            if ralt == "ground":
                ralt = 0
            if ralt is None:
                ralt = readsb.get("alt_geom")
            alt_delta_ft = None
            if isinstance(ialt, (int, float)) and isinstance(ralt, (int, float)):
                alt_delta_ft = int(round(ialt - ralt))

            row = {
                "icao": icao,
                "callsign": (ac.get("callsign") or readsb.get("flight") or "").strip(),
                "internal": {
                    "lat": ilat,
                    "lon": ilon,
                    "altitude": ialt,
                    "source": "internal",
                },
                "readsb": {
                    "lat": rlat,
                    "lon": rlon,
                    "altitude": ralt,
                    "seen": readsb.get("seen"),
                    "source": readsb_payload.get("_source"),
                },
                "horizontal_error_m": horiz_err_m,
                "altitude_delta_ft": alt_delta_ft,
                "updated_at": now,
            }
            next_rows[icao] = row
            hist = self._history.setdefault(icao, deque(maxlen=300))
            hist.append(
                {
                    "ts": now,
                    "internal_altitude": ialt,
                    "readsb_altitude": ralt,
                    "internal_lat": ilat,
                    "internal_lon": ilon,
                    "readsb_lat": rlat,
                    "readsb_lon": rlon,
                    "horizontal_error_m": horiz_err_m,
                    "altitude_delta_ft": alt_delta_ft,
                }
            )
            next_history_seen.add(icao)

        with self._lock:
            self._rows = next_rows
            self._last_update = now
            stale = [icao for icao in self._history if icao not in next_history_seen]
            for icao in stale:
                if self._history[icao] and (now - self._history[icao][-1]["ts"]) > 600:
                    del self._history[icao]

    def list_rows(self) -> dict[str, Any]:
        with self._lock:
            rows = list(self._rows.values())
            updated_at = self._last_update
        rows.sort(key=lambda r: r.get("horizontal_error_m", 0), reverse=True)
        return {"updated_at": updated_at, "aircraft": rows}

    def detail(self, icao: str) -> dict[str, Any]:
        icao = icao.upper()
        with self._lock:
            row = self._rows.get(icao)
            hist = list(self._history.get(icao, []))
        if row is None:
            raise KeyError(icao)

        valid_err = [p["horizontal_error_m"] for p in hist if p.get("horizontal_error_m") is not None]
        valid_alt = [abs(p["altitude_delta_ft"]) for p in hist if p.get("altitude_delta_ft") is not None]
        summary = {
            "samples": len(hist),
            "avg_horizontal_error_m": round(sum(valid_err) / len(valid_err), 1) if valid_err else None,
            "max_horizontal_error_m": round(max(valid_err), 1) if valid_err else None,
            "avg_abs_altitude_delta_ft": round(sum(valid_alt) / len(valid_alt), 1) if valid_alt else None,
            "max_abs_altitude_delta_ft": max(valid_alt) if valid_alt else None,
        }
        return {"current": row, "history": hist, "summary": summary}


async def run_position_quality_checker(checker: PositionQualityChecker) -> None:
    while True:
        await asyncio.to_thread(checker.tick)
        await asyncio.sleep(1)


@router.get("")
async def list_position_quality() -> dict[str, Any]:
    if _checker is None:
        return {"updated_at": None, "aircraft": []}
    return await asyncio.to_thread(_checker.list_rows)


@router.get("/{icao}")
async def position_quality_detail(icao: str) -> dict[str, Any]:
    if _checker is None:
        raise HTTPException(status_code=503, detail="position quality checker unavailable")
    try:
        return await asyncio.to_thread(_checker.detail, icao)
    except KeyError:
        raise HTTPException(status_code=404, detail="aircraft not found") from None
