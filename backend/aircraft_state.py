"""
Thread-safe in-memory state for live aircraft and message-rate tracking.
"""

import math
import time
import threading
import logging
import re
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pyModeS as pms
from pyModeS.decoder.bds import bds40 as _bds40, bds50 as _bds50, bds60 as _bds60

import acas as acas_decoder
import config
import enrichment
from db import INTERESTING_TYPE_CODES
from utils import country_from_registration

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Performance timing stores — written by process_message and _push_updates,
# read by /api/debug/perf.  deques are thread-safe for single-writer appends.
# ---------------------------------------------------------------------------
# Per-message total processing time in seconds (includes lock acquisition + decode)
msg_timings: deque[float] = deque(maxlen=2000)
# Per-_push_updates invocation breakdown: {loop_ms, broadcast_ms, total_ms, ac_count}
push_timings: deque[dict] = deque(maxlen=120)

_R_NM = 3440.065  # Earth radius in nautical miles
# Beast timestamp value used by mlat-client for all synthesized positions.
# Bytes: FF 00 4D 4C 41 54  ("FF00" + "MLAT" in ASCII).
# Aggregators recognise this as non-real so they don't ingest MLAT positions as ADS-B.
# We use it as ground truth: any Beast frame with this timestamp is MLAT-derived,
# regardless of which TCP stream delivered it.
_MLAT_TS_MARKER: int = int.from_bytes(b'\xff\x00MLAT', 'big')  # 0xFF004D4C4154
_ICAO_HEX_RE = re.compile(r"^[0-9A-F]{6}$")
_ACAS_CONFIRM_WINDOW_S = 12
_ACAS_MIN_CONFIRM_FRAMES = 2

# Altitude filtering constants
_ALT_MIN_FT = -1_000          # below this is a bad decode (subterranean)
_ALT_MAX_FT = 60_000          # above this is a bad decode (service ceiling)
_ALT_MAX_RATE_FPM = 8_000     # max realistic climb/descent rate — rejects spikes
_ALT_SOURCE_STALE_S = 15.0    # after this many seconds, lower-priority source may overwrite


def _accept_altitude(ac: "Aircraft", alt: int, source: str, now: float) -> bool:
    """Return True and update ac.altitude if alt passes range + rate-of-change + priority checks.

    source: "ADS-B" (DF17/18, CRC-verified) or "SURV" (DF4/20, parity-masked).
    ADS-B is preferred; SURV is only accepted when ADS-B is stale or absent.
    """
    # Range gate — reject physically impossible values
    if not (_ALT_MIN_FT <= alt <= _ALT_MAX_FT):
        return False

    # Source priority: don't let SURV overwrite a recent ADS-B altitude
    if source == "SURV" and ac._alt_source == "ADS-B":
        age = now - ac._alt_source_ts
        if age < _ALT_SOURCE_STALE_S:
            return False

    # Rate-of-change gate — reject spikes that exceed realistic climb/descent
    if ac.altitude is not None and ac._alt_source_ts > 0:
        dt_min = (now - ac._alt_source_ts) / 60.0
        if dt_min > 0:
            rate = abs(alt - ac.altitude) / dt_min
            if rate > _ALT_MAX_RATE_FPM:
                return False

    ac.altitude = alt
    ac._alt_source = source
    ac._alt_source_ts = now
    return True

def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """True bearing from (lat1,lon1) to (lat2,lon2) in degrees (0=N, 90=E)."""
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(math.radians(lat2))
    y = (math.cos(math.radians(lat1)) * math.sin(math.radians(lat2))
         - math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(dlon))
    return (math.degrees(math.atan2(x, y)) + 360) % 360

def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return _R_NM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _log_enrichment(icao: str, ac, adsbx: dict | None, op_source: str) -> None:
    log.info(
        "[enrich] %-8s  NEW     adsbx=%-4s  reg=%-9s type=%-6s year=%-4s  "
        "op=%-30s [%s]  country=%-4s  mil=%s",
        icao,
        "hit" if adsbx else "miss",
        ac.registration or "—",
        ac.type_code or "—",
        ac.year or "—",
        repr(ac.operator) if ac.operator else "—",
        op_source,
        ac.country or "—",
        "Y" if ac.military else "N",
    )


def _apply_hexdb_data(ac, data: dict) -> None:
    """Apply a hexdb.io response to an Aircraft object (call while holding state lock).
    Fills any fields that are still missing — registration, type, type_desc, and operator."""
    if not data:
        return
    if not ac.registration:
        ac.registration = (data.get("Registration") or "").strip() or None
    if not ac.type_code:
        ac.type_code = (data.get("ICAOTypeCode") or "").strip() or None
        if ac.type_code:
            ti = enrichment.db.get_type_info(ac.type_code)
            if ti:
                ac.type_full_name = ti.get("name") or ac.type_full_name
                ac.type_category  = ti.get("desc") or ac.type_category
                ac.wtc            = ti.get("wtc")  or ac.wtc
    if not ac.type_desc:
        mfr  = (data.get("Manufacturer") or "").strip()
        typ  = (data.get("Type") or "").strip()
        ac.type_desc = (f"{mfr} {typ}".strip()) or None
        if not ac.manufacturer and mfr:
            ac.manufacturer = mfr
    if not ac.operator:
        # For non-military aircraft, OperatorFlagCode is an ICAO airline designator —
        # look it up in operators.js for a clean airline name.
        # Skip this for military aircraft: hexdb often puts the type code there (e.g.
        # "LYNX"), which can match an unrelated airline entry with the wrong country.
        if not ac.military:
            flag_code = (data.get("OperatorFlagCode") or "").strip()
            if flag_code:
                op = enrichment.db.get_operator(flag_code)
                if op:
                    ac.operator = op.get("n")
                    if not ac.country:
                        ac.country = op.get("c")
                    return
        owners = (data.get("RegisteredOwners") or "").strip()
        if owners:
            ac.operator = owners


@dataclass
class Aircraft:
    icao: str
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    callsign: Optional[str] = None
    altitude: Optional[int] = None
    squawk: Optional[str] = None
    signal: Optional[int] = None   # last RSSI byte from Beast
    msg_count: int = 0
    registration: Optional[str] = None
    type_code: Optional[str] = None
    type_desc: Optional[str] = None
    type_full_name: Optional[str] = None  # long name from icao_aircraft_types2
    type_category: Optional[str] = None   # desc code e.g. "L1J", "H1T"
    wtc: Optional[str] = None             # wake turbulence category: L/M/H/J
    military: bool = False
    operator: Optional[str] = None
    country: Optional[str] = None
    year: Optional[str] = None            # manufacture year (ADSBExchange)
    manufacturer: Optional[str] = None    # e.g. "Boeing", "Airbus"
    # EHS (Enhanced Mode-S) fields — decoded from DF20/21 Comm-B replies
    airspeed_kts:      Optional[int]   = None
    airspeed_type:     Optional[str]   = None   # "IAS" or "TAS"
    heading_deg:       Optional[float] = None
    vertical_rate_fpm: Optional[int]   = None
    mach:              Optional[float] = None
    selected_alt:      Optional[int]   = None
    lat: Optional[float] = None           # last known latitude (WGS-84)
    lon: Optional[float] = None           # last known longitude (WGS-84)
    range_nm: Optional[float] = None      # distance from receiver (nautical miles)
    bearing_deg: Optional[float] = None   # bearing from receiver (degrees true)
    cpr_even: Optional[tuple] = field(default=None, repr=False)   # (raw_msg, timestamp)
    cpr_odd:  Optional[tuple] = field(default=None, repr=False)
    pos_global: bool = False  # True once a global CPR decode (even+odd pair) has succeeded
    sighting_count: int = 1               # from aircraft_registry; 1 = unique (never seen before)
    # MLAT
    has_adsb: bool = False               # True once a DF17 WITHOUT the MLAT timestamp is seen
                                         # (guarantees a real ADS-B transponder; blocks MLAT tag)
    mlat: bool = False                    # True if position is MLAT-derived (timestamp-confirmed)
    mlat_source: Optional[str] = None    # name of the MLAT server that established the position
    mlat_msg_count: int = 0              # Beast frames with the MLAT timestamp marker
    # Altitude source tracking — for priority and rate-of-change filtering
    _alt_source: Optional[str]  = field(default=None, repr=False)  # "ADS-B" | "SURV"
    _alt_source_ts: float       = field(default=0.0,  repr=False)  # monotonic time of last alt update
    # ACAS/TCAS fields
    acas_ra_active:     bool           = False
    acas_ra_desc:       Optional[str]  = None
    acas_ra_corrective: bool           = False
    acas_sensitivity:   Optional[int]  = None
    acas_threat_icao:   Optional[str]  = None
    acas_ra_ts:         Optional[float]= None


class AircraftState:
    def __init__(self, aircraft_timeout: int = 60):
        self._timeout = aircraft_timeout
        self._lock = threading.Lock()
        self._aircraft: dict[str, Aircraft] = {}

        # Per-second counters (rolling 60-second window for current rate)
        self._sec_counts: deque[tuple[int, int]] = deque(maxlen=60)
        self._cur_sec: int = int(time.time())
        self._cur_sec_count: int = 0

        # Per-minute stats (rolling 60-minute window for chart)
        # Each entry: (minute, msg_min, msg_max, msg_mean, ac_total, ac_civil, ac_military,
        #              sig_avg, sig_min, sig_max)
        self._min_stats: deque[tuple] = deque(maxlen=60)
        self._cur_min: int = int(time.time() // 60)
        self._cur_min_sec_counts: list[int] = []
        self._cur_min_signals: list[int] = []        # Beast RSSI bytes seen this minute
        self._cur_min_df_counts: dict[int, int] = {} # DF type → message count this minute
        # Rolling DF history (parallel to _min_stats)
        self._min_df_stats: deque[tuple] = deque(maxlen=60)  # (minute, df_counts_dict)

        self._total: int = 0
        # MLAT stats (parallel to the regular per-sec/per-min counters)
        self._mlat_total: int = 0
        self._cur_sec_mlat_count: int = 0
        self._mlat_sec_counts: deque[tuple[int, int]] = deque(maxlen=60)
        self._cur_min_mlat_count: int = 0
        self._min_mlat_counts: deque[tuple[int, int]] = deque(maxlen=60)  # (minute, count)

        self._adsbx_queue: set[str] = set()  # ICAOs awaiting ADSBx enrichment (off decode thread)
        self._hexdb_queue: set[str] = set()  # ICAOs awaiting hexdb.io operator lookup

        # ACAS event queue: drained by main._db_writer() every minute
        self._pending_acas_events: deque[dict] = deque(maxlen=500)
        self._last_acas_ts: dict[str, tuple[float, str]] = {}  # icao → (ts, ra_desc)
        # Require repeated matching DF16 RA frames before persisting an event;
        # suppresses single-frame parity/noise artefacts.
        self._acas_candidates: dict[str, tuple[str, float, int]] = {}  # icao -> (signature, ts, count)

        # Unique aircraft seen today (resets at midnight; seeded from DB on startup)
        self._today_date: str = date.today().isoformat()
        self._today_icaos: set[str] = set()
        self._today_mil_icaos: set[str] = set()

        # sighting_count seed from DB (icao -> count); used when creating new Aircraft objects
        self._sighting_counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_message(self, msg: dict, mlat_source: Optional[str] = None) -> None:
        """Process a decoded Beast message.

        mlat_source: name of the MLAT server this message came from, or None for
        the primary Beast stream.

        MLAT detection uses the Beast timestamp marker (0xFF004D4C4154) as ground
        truth.  This works on any stream — including the regular port 30005 feed
        where readsb echoes synthesized MLAT positions back.  ADS-B frames forwarded
        by the mlat-client carry their original real timestamp and are NOT tagged MLAT.
        """
        raw: str = msg["raw"]
        signal: int = msg.get("signal", 0)
        timestamp: int = msg.get("timestamp", 0)
        now = time.time()

        # Timestamp-based MLAT detection: definitive regardless of stream.
        if timestamp == _MLAT_TS_MARKER:
            # Synthesized MLAT frame — keep the supplied source name, or fall back
            # to "mlat" for frames detected on the regular stream via timestamp only.
            if mlat_source is None:
                mlat_source = "mlat"
        else:
            # Real timestamp → genuine ADS-B (or Mode-S without position).
            # Clear any stream-level MLAT hint: the mlat-client forwards real ADS-B
            # frames with their original timestamps, so stream origin is not reliable.
            mlat_source = None

        mlat = mlat_source is not None

        t0 = time.perf_counter()
        with self._lock:
            self._total += 1
            if mlat:
                self._mlat_total += 1
            self._tick(now, mlat=mlat)
            self._decode(raw, signal, now, mlat=mlat, mlat_source=mlat_source)
        msg_timings.append(time.perf_counter() - t0)

    def expire_aircraft(self) -> None:
        now = time.time()
        with self._lock:
            stale = [icao for icao, ac in self._aircraft.items()
                     if now - ac.last_seen > self._timeout]
            for icao in stale:
                del self._aircraft[icao]

    def init_today(self, icaos: set[str], mil_icaos: set[str]) -> None:
        """Seed today's unique-aircraft sets from DB on startup (persistence across restarts)."""
        with self._lock:
            self._today_icaos = set(icaos)
            self._today_mil_icaos = set(mil_icaos)

    def seed_sighting_counts(self, counts: dict[str, int]) -> None:
        """Seed sighting_count from DB so new Aircraft objects get correct values."""
        with self._lock:
            self._sighting_counts = counts

    def seed_hexdb_queue(self, icaos: list[str]) -> None:
        """Add a batch of ICAOs to the hexdb re-enrichment queue."""
        with self._lock:
            self._hexdb_queue.update(icaos)

    def update_sighting_counts(self, counts: dict[str, int]) -> None:
        """Refresh in-memory sighting_count for live aircraft after a DB write."""
        with self._lock:
            for icao, count in counts.items():
                ac = self._aircraft.get(icao)
                if ac:
                    ac.sighting_count = count
                self._sighting_counts[icao] = count

    def pop_hexdb_queue(self, max_n: int = 10) -> set[str]:
        """Return up to max_n ICAOs awaiting hexdb lookup; removes them from the queue."""
        with self._lock:
            batch = set(list(self._hexdb_queue)[:max_n])
            self._hexdb_queue -= batch
            return batch

    def pop_acas_events(self) -> list[dict]:
        """Return and clear all pending ACAS events (called every minute by db writer)."""
        with self._lock:
            evts = list(self._pending_acas_events)
            self._pending_acas_events.clear()
            return evts

    def get_aircraft_live(self, icao: str) -> dict | None:
        """Return a point-in-time snapshot of a single aircraft, or None if not tracked."""
        now = time.time()
        with self._lock:
            ac = self._aircraft.get(icao)
            if ac is None:
                return None
            return {
                "icao":          ac.icao,
                "callsign":      ac.callsign,
                "altitude":      ac.altitude,
                "squawk":        ac.squawk,
                "signal":        ac.signal,
                "msg_count":     ac.msg_count,
                "age":           round(now - ac.last_seen, 1),
                "registration":  ac.registration,
                "type_code":     ac.type_code,
                "type_desc":     ac.type_desc,
                "type_full_name": ac.type_full_name,
                "type_category": ac.type_category,
                "wtc":           ac.wtc,
                "military":      ac.military,
                "operator":      ac.operator,
                "country":       ac.country,
                "year":          ac.year,
                "manufacturer":  ac.manufacturer,
                "lat":              ac.lat,
                "lon":              ac.lon,
                "range_nm":         ac.range_nm,
                "bearing_deg":      ac.bearing_deg,
                "airspeed_kts":     ac.airspeed_kts,
                "airspeed_type":    ac.airspeed_type,
                "heading_deg":      ac.heading_deg,
                "vertical_rate_fpm": ac.vertical_rate_fpm,
                "mach":             ac.mach,
                "selected_alt":       ac.selected_alt,
                "sighting_count":  ac.sighting_count,
                "mlat":            ac.mlat,
                "mlat_source":       ac.mlat_source,
                "mlat_msg_count":    ac.mlat_msg_count,
                "acas_ra_active":     ac.acas_ra_ts is not None and (now - ac.acas_ra_ts) < 60,
                "acas_ra_desc":       ac.acas_ra_desc,
                "acas_ra_corrective": ac.acas_ra_corrective,
                "acas_threat_icao":   ac.acas_threat_icao,
                "acas_sensitivity":   ac.acas_sensitivity,
            }

    def pop_adsbx_queue(self, max_n: int = 20) -> set[str]:
        """Return up to max_n ICAOs awaiting ADSBx enrichment; removes them from the queue."""
        with self._lock:
            batch = set(list(self._adsbx_queue)[:max_n])
            self._adsbx_queue -= batch
            return batch

    def apply_adsbx(self, icao: str, adsbx: dict | None) -> None:
        """Apply ADSBx enrichment to a newly-seen aircraft (called from background task).

        Only uses in-memory lookups (get_type_info = dict, country_from_registration =
        in-memory), so it is safe to call from the asyncio event loop thread.
        """
        with self._lock:
            ac = self._aircraft.get(icao)
            if ac is None:
                return  # aircraft expired while queued

            if adsbx:
                if not ac.registration:
                    ac.registration = (adsbx.get("reg") or "").strip() or None
                if not ac.type_code:
                    ac.type_code = (adsbx.get("icaotype") or "").strip() or None
                if ac.type_code and not ac.type_full_name:
                    ti = enrichment.db.get_type_info(ac.type_code)
                    if ti:
                        ac.type_full_name = ti.get("name") or None
                        ac.type_category  = ti.get("desc") or None
                        ac.wtc            = ti.get("wtc")  or None
                if not ac.type_desc:
                    mfr   = (adsbx.get("manufacturer") or "").strip()
                    model = (adsbx.get("model") or "").strip()
                    ac.type_desc = (f"{mfr} {model}".strip()) or None
                if not ac.manufacturer:
                    mfr = (adsbx.get("manufacturer") or "").strip()
                    ac.manufacturer = mfr or None
                if not ac.year:
                    ac.year = (adsbx.get("year") or "").strip() or None
                if not ac.operator and adsbx.get("ownop"):
                    ac.operator = adsbx["ownop"]

                # ADSBx is authoritative for the military flag
                ac.military = bool(adsbx.get("mil"))
                if ac.military:
                    self._today_mil_icaos.add(icao)
                    if not ac.year and ac.registration:
                        ac.year = enrichment.extract_us_mil_serial_year(ac.registration)

                # Registration-prefix country overrides ICAO block for civil aircraft
                if ac.registration and not ac.military:
                    reg_country = country_from_registration(ac.registration)
                    if reg_country:
                        ac.country = reg_country

            # Queue hexdb HTTP lookup if any key field is still missing
            if not ac.operator or not ac.registration or not ac.type_code:
                self._hexdb_queue.add(icao)

            if config.DEBUG_ENRICHMENT == 1:
                op_source = (
                    "adsbx" if adsbx and adsbx.get("ownop") and ac.operator == adsbx.get("ownop")
                    else "queued"
                )
                _log_enrichment(icao, ac, adsbx, op_source)

    def apply_hexdb(self, icao: str, data: dict) -> None:
        """Apply a hexdb.io response to a live aircraft (fills any still-missing fields)."""
        with self._lock:
            ac = self._aircraft.get(icao)
            if ac is None:
                return
            _apply_hexdb_data(ac, data)

    def get_snapshot(self) -> dict:
        now = time.time()
        with self._lock:
            # Average msg/sec over the last 10 completed seconds
            recent = list(self._sec_counts)[-10:]
            msg_per_sec = round(sum(c for _, c in recent) / max(len(recent), 1), 1)

            # MLAT msg/sec (same rolling 10-second window)
            mlat_recent = list(self._mlat_sec_counts)[-10:]
            mlat_per_sec = round(sum(c for _, c in mlat_recent) / max(len(mlat_recent), 1), 1)

            # Rate history for the chart (completed minutes + current partial minute)
            secs = list(self._cur_min_sec_counts)
            if secs:
                cur_mn, cur_mx, cur_me = min(secs), max(secs), round(sum(secs) / len(secs), 1)
            else:
                cur_mn = cur_mx = cur_me = 0.0
            cur_total = len(self._aircraft)
            cur_mil = sum(1 for ac in self._aircraft.values() if ac.military)
            cur_with_pos = sum(1 for ac in self._aircraft.values() if ac.lat is not None)
            cur_mlat_pos = sum(1 for ac in self._aircraft.values() if ac.mlat and ac.lat is not None)
            cur_sigs = self._cur_min_signals
            cur_sig_avg = round(sum(cur_sigs) / len(cur_sigs), 1) if cur_sigs else None
            cur_stats = (self._cur_min, cur_mn, cur_mx, cur_me,
                         cur_total, cur_total - cur_mil, cur_mil,
                         cur_sig_avg, min(cur_sigs) if cur_sigs else None,
                         max(cur_sigs) if cur_sigs else None,
                         cur_with_pos, cur_mlat_pos)
            rate_history = list(self._min_stats) + [cur_stats]
            df_history = list(self._min_df_stats) + [(self._cur_min, dict(self._cur_min_df_counts))]
            mlat_history = list(self._min_mlat_counts) + [(self._cur_min, self._cur_min_mlat_count)]

            aircraft_list = [
                {
                    "icao": ac.icao,
                    "callsign": ac.callsign,
                    "altitude": ac.altitude,
                    "squawk": ac.squawk,
                    "signal": ac.signal,
                    "msg_count": ac.msg_count,
                    "age": round(now - ac.last_seen, 1),
                    "registration": ac.registration,
                    "type_code": ac.type_code,
                    "type_desc": ac.type_desc,
                    "type_full_name": ac.type_full_name,
                    "type_category": ac.type_category,
                    "wtc": ac.wtc,
                    "military": ac.military,
                    "operator": ac.operator,
                    "country": ac.country,
                    "year": ac.year,
                    "manufacturer": ac.manufacturer,
                    "lat":              ac.lat,
                    "lon":              ac.lon,
                    "range_nm":         ac.range_nm,
                    "bearing_deg":      ac.bearing_deg,
                    "airspeed_kts":     ac.airspeed_kts,
                    "airspeed_type":    ac.airspeed_type,
                    "heading_deg":      ac.heading_deg,
                    "vertical_rate_fpm": ac.vertical_rate_fpm,
                    "mach":             ac.mach,
                    "selected_alt":     ac.selected_alt,
                    "interesting":        bool(ac.type_code and ac.type_code.upper() in INTERESTING_TYPE_CODES),
                    "sighting_count":     ac.sighting_count,
                    "mlat":              ac.mlat,
                    "mlat_source":       ac.mlat_source,
                    "mlat_msg_count":    ac.mlat_msg_count,
                    "acas_ra_active":     ac.acas_ra_ts is not None and (now - ac.acas_ra_ts) < 60,
                    "acas_ra_desc":       ac.acas_ra_desc,
                    "acas_ra_corrective": ac.acas_ra_corrective,
                    "acas_threat_icao":   ac.acas_threat_icao,
                    "acas_sensitivity":   ac.acas_sensitivity,
                    "pos_global":         ac.pos_global,
                }
                for ac in self._aircraft.values()
            ]

        live_military = sum(1 for ac in self._aircraft.values() if ac.military)
        mlat_aircraft_count = sum(1 for ac in self._aircraft.values() if ac.mlat)

        return {
            "aircraft_count": len(aircraft_list),
            "live_military": live_military,
            "msg_per_sec": msg_per_sec,
            "total_messages": self._total,
            "mlat_total": self._mlat_total,
            "mlat_per_sec": mlat_per_sec,
            "mlat_aircraft_count": mlat_aircraft_count,
            "unique_today": len(self._today_icaos),
            "unique_today_military": len(self._today_mil_icaos),
            "adsbx_queue_size": len(self._adsbx_queue),
            "hexdb_queue_size": len(self._hexdb_queue),
            "aircraft": sorted(aircraft_list, key=lambda x: x["msg_count"], reverse=True),
            "rate_history": [
                {"minute": m, "min": mn, "max": mx, "mean": me,
                 "ac_total": t, "ac_civil": c, "ac_military": mil,
                 "signal_avg": sa, "signal_min": sn, "signal_max": sx,
                 "ac_with_pos": wp, "ac_mlat": ml}
                for m, mn, mx, me, t, c, mil, sa, sn, sx, wp, ml in rate_history[-60:]
            ],
            "df_history": [
                {"minute": m, "counts": {str(k): v for k, v in counts.items()}}
                for m, counts in df_history[-60:]
            ],
            "mlat_history": [
                {"minute": m, "count": c}
                for m, c in mlat_history[-60:]
            ],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _tick(self, now: float, mlat: bool = False) -> None:
        sec = int(now)
        if sec != self._cur_sec:
            self._sec_counts.append((self._cur_sec, self._cur_sec_count))
            self._mlat_sec_counts.append((self._cur_sec, self._cur_sec_mlat_count))
            self._cur_min_sec_counts.append(self._cur_sec_count)
            self._cur_sec = sec
            self._cur_sec_count = 0
            self._cur_sec_mlat_count = 0
        self._cur_sec_count += 1
        if mlat:
            self._cur_sec_mlat_count += 1

        minute = int(now // 60)
        if minute != self._cur_min:
            secs = self._cur_min_sec_counts
            if secs:
                mn, mx, me = min(secs), max(secs), round(sum(secs) / len(secs), 1)
            else:
                mn = mx = me = 0.0
            total = len(self._aircraft)
            mil = sum(1 for ac in self._aircraft.values() if ac.military)
            with_pos = sum(1 for ac in self._aircraft.values() if ac.lat is not None)
            mlat_pos = sum(1 for ac in self._aircraft.values() if ac.mlat and ac.lat is not None)
            sigs = self._cur_min_signals
            sig_avg = round(sum(sigs) / len(sigs), 1) if sigs else None
            sig_min = min(sigs) if sigs else None
            sig_max = max(sigs) if sigs else None
            self._min_stats.append(
                (self._cur_min, mn, mx, me, total, total - mil, mil,
                 sig_avg, sig_min, sig_max, with_pos, mlat_pos)
            )
            self._min_df_stats.append((self._cur_min, dict(self._cur_min_df_counts)))
            self._min_mlat_counts.append((self._cur_min, self._cur_min_mlat_count))
            self._cur_min = minute
            self._cur_min_sec_counts = []
            self._cur_min_signals = []
            self._cur_min_df_counts = {}
            self._cur_min_mlat_count = 0
        if mlat:
            self._cur_min_mlat_count += 1

    def _decode(self, raw: str, signal: int, now: float, mlat: bool = False, mlat_source: Optional[str] = None) -> None:
        if len(raw) < 14:          # Too short to contain an ICAO address
            return

        try:
            df = pms.df(raw)
        except Exception:
            return

        # --- Extract ICAO ---
        icao: Optional[str] = None
        try:
            if df in (17, 18, 11):
                icao = pms.icao(raw)
            elif df in (0, 4, 5, 16, 20, 21):
                icao = pms.icao(raw)   # pyModeS recovers it from CRC
        except Exception:
            pass

        if not icao:
            return
        icao = icao.upper()

        # --- Update or create aircraft record ---
        if icao not in self._aircraft:
            ac = Aircraft(icao=icao, sighting_count=self._sighting_counts.get(icao, 1))

            # Fast in-memory lookups only — no SQLite I/O on the decode thread.
            # get_country_by_icao: binary search; get_hexdb_cached: LRU lookup.
            ac.country = enrichment.db.get_country_by_icao(icao)
            hexdb_cached = enrichment.db.get_hexdb_cached(icao)
            if hexdb_cached:
                _apply_hexdb_data(ac, hexdb_cached)

            # ADSBx enrichment (SQLite) is deferred to _adsbx_task so the decode
            # thread is never blocked on I/O.
            self._adsbx_queue.add(icao)

            self._aircraft[icao] = ac

        ac = self._aircraft[icao]
        ac.last_seen = now
        ac.msg_count += 1
        ac.signal = signal
        if mlat:
            # MLAT-timestamp frame.  Only tag if we haven't confirmed a real ADS-B
            # transponder: some clients stamp ALL forwarded messages (including genuine
            # ADS-B DF17) with the MLAT marker.  has_adsb (set below by non-MLAT DF17)
            # acts as the tiebreak.
            if not ac.has_adsb:
                ac.mlat = True
                # Prefer a named server source over the auto-detected "mlat" label.
                if mlat_source != "mlat" or ac.mlat_source is None:
                    ac.mlat_source = mlat_source
            ac.mlat_msg_count += 1
        elif df == 17:
            # Non-MLAT-timestamp DF17 = genuine ADS-B transponder.  This is the only
            # frame type we can be sure is NOT an MLAT injection (synthesized MLAT
            # frames always carry the MLAT timestamp; real transponders never do).
            ac.has_adsb = True
            ac.mlat = False
            ac.mlat_source = None

        # Track unique aircraft seen today; reset sets at midnight
        today = date.today().isoformat()
        if today != self._today_date:
            self._today_icaos.clear()
            self._today_mil_icaos.clear()
            self._today_date = today
        self._today_icaos.add(icao)
        if ac.military:
            self._today_mil_icaos.add(icao)

        # Accumulate per-minute signal and DF stats
        if signal:
            self._cur_min_signals.append(signal)
        self._cur_min_df_counts[df] = self._cur_min_df_counts.get(df, 0) + 1

        # --- Decode ADS-B (DF 17 and DF 18) ---
        # DF17: Extended Squitter with true 24-bit CRC — highest integrity source.
        # DF18: TIS-B / ADS-R rebroadcast — same message structure, same altitude quality.
        if df in (17, 18) and len(raw) == 28:
            try:
                tc = pms.adsb.typecode(raw)
            except Exception:
                return

            if tc is None:
                return

            # Identification (callsign)
            if 1 <= tc <= 4:
                try:
                    cs = pms.adsb.callsign(raw)
                    if cs:
                        ac.callsign = cs.strip().rstrip('_')
                        if ac.callsign and not ac.operator:
                            op = enrichment.db.get_operator(ac.callsign[:3])
                            if op:
                                ac.operator = op.get("n")
                                if not ac.country:
                                    ac.country = op.get("c")
                except Exception:
                    pass

            # Airborne position – altitude + CPR lat/lon
            elif 9 <= tc <= 18 or 20 <= tc <= 22:
                try:
                    alt = pms.adsb.altitude(raw)
                    if alt is not None:
                        _accept_altitude(ac, int(alt), "ADS-B", now)
                except Exception:
                    pass
                try:
                    oe = pms.adsb.oe_flag(raw)
                    if oe == 0:
                        ac.cpr_even = (raw, now)
                    else:
                        ac.cpr_odd = (raw, now)

                    # Global decode is always preferred: uses even+odd pair and is
                    # geometrically unambiguous — no zone-selection dependency on
                    # reference longitude.  Local decode can pick the wrong CPR
                    # longitude zone when the receiver is more than ~5° west of the
                    # aircraft (e.g. western UK receiver + aircraft over the North Sea).
                    pos = None
                    pos_from_global = False
                    if (ac.cpr_even and ac.cpr_odd
                            and abs(ac.cpr_even[1] - ac.cpr_odd[1]) < 10):
                        pos = pms.adsb.position(
                            ac.cpr_even[0], ac.cpr_odd[0],
                            ac.cpr_even[1], ac.cpr_odd[1],
                        )
                        if pos is not None:
                            pos_from_global = True

                    # Local decode fallback: only when no pair is available yet.
                    # Use the aircraft's own last position as reference (safer than
                    # receiver coords — same zone guaranteed once ac.lat is trusted).
                    # Receiver coords used only for the very first frame of a new aircraft.
                    if pos is None:
                        ref_lat = ac.lat if ac.lat is not None else config.RECEIVER_LAT
                        ref_lon = ac.lon if ac.lon is not None else config.RECEIVER_LON
                        if ref_lat is not None and ref_lon is not None:
                            pos = pms.adsb.position_with_ref(raw, ref_lat, ref_lon)

                    if pos:
                        lat, lon = pos
                        if -90 <= lat <= 90 and -180 <= lon <= 180:
                            # Plausibility check: reject positions impossibly far from
                            # receiver (catches wrong-zone decodes on first local decode)
                            if (config.RECEIVER_LAT is not None
                                    and config.RECEIVER_LON is not None
                                    and _haversine_nm(
                                        config.RECEIVER_LAT, config.RECEIVER_LON,
                                        lat, lon) > 500):
                                pass  # discard — beyond ADS-B range
                            else:
                                ac.lat = round(lat, 5)
                                ac.lon = round(lon, 5)
                                if pos_from_global:
                                    ac.pos_global = True
                                if config.RECEIVER_LAT is not None and config.RECEIVER_LON is not None:
                                    ac.range_nm = round(_haversine_nm(
                                        config.RECEIVER_LAT, config.RECEIVER_LON,
                                        ac.lat, ac.lon,
                                    ), 1)
                                    ac.bearing_deg = round(_bearing_deg(
                                        config.RECEIVER_LAT, config.RECEIVER_LON,
                                        ac.lat, ac.lon,
                                    ), 1)
                except Exception:
                    pass

        # --- Altitude from surveillance altitude reply (DF 4) ---
        # Lower integrity than ADS-B: parity is XOR-masked with aircraft address.
        elif df == 4 and len(raw) == 14:
            try:
                alt = pms.altcode(raw)
                if alt is not None:
                    _accept_altitude(ac, int(alt), "SURV", now)
            except Exception:
                pass

        # --- DF 20/21: Comm-B replies (28 hex chars / 112 bits) ---
        elif df in (20, 21) and len(raw) == 28:
            # Altitude from DF20 — same parity integrity as DF4
            if df == 20:
                try:
                    alt = pms.altcode(raw)
                    if alt is not None:
                        _accept_altitude(ac, int(alt), "SURV", now)
                except Exception:
                    pass

            # EHS decode: direct is40/is50/is60 checks — ~5x faster than pms.bds.infer()
            # which speculatively tries ~20 registers. Checks are mutually exclusive in practice.
            if _bds40.is40(raw):
                # Selected altitude (autopilot target)
                try:
                    sel = pms.commb.selalt40mcp(raw)
                    if sel is not None:
                        ac.selected_alt = int(sel)
                except Exception:
                    pass

            elif _bds50.is50(raw):
                # TAS + track angle + roll
                try:
                    tas = pms.commb.tas50(raw)
                    if tas is not None:
                        ac.airspeed_kts = int(round(tas))
                        ac.airspeed_type = "TAS"
                except Exception:
                    pass
                try:
                    trk = pms.commb.trk50(raw)
                    if trk is not None:
                        ac.heading_deg = round(float(trk), 1)
                except Exception:
                    pass

            elif _bds60.is60(raw):
                # IAS + Mach + magnetic heading + baro vertical rate
                try:
                    ias = pms.commb.ias60(raw)
                    if ias is not None:
                        ac.airspeed_kts = int(round(ias))
                        ac.airspeed_type = "IAS"
                except Exception:
                    pass
                try:
                    mach = pms.commb.mach60(raw)
                    if mach is not None:
                        ac.mach = round(float(mach), 3)
                except Exception:
                    pass
                try:
                    hdg = pms.commb.hdg60(raw)
                    if hdg is not None:
                        ac.heading_deg = round(float(hdg), 1)
                except Exception:
                    pass
                try:
                    vr = pms.commb.vr60baro(raw)
                    if vr is not None:
                        ac.vertical_rate_fpm = int(round(vr))
                except Exception:
                    pass

        # --- DF16: Long Air-Air Surveillance (ACAS RA in MV field) ---
        # Altitude from DF16 is intentionally not used — DF16 is an ACAS air-to-air
        # message and its parity is unreliable for altitude extraction.
        elif df == 16 and len(raw) == 28:
            try:
                result = acas_decoder.decode_df16_mv(raw)
                if result and result.get("ara_active"):
                    self._apply_acas(ac, result, now)
            except Exception:
                pass

        # --- DF0: Short Air-Air Surveillance (sensitivity level only) ---
        elif df == 0 and len(raw) == 14:
            try:
                sl = acas_decoder.decode_df0_sensitivity(raw)
                if sl is not None:
                    ac.acas_sensitivity = sl
            except Exception:
                pass

        # --- Squawk from identity reply (DF 5 / DF 21) ---
        if df in (5, 21):
            try:
                sq = pms.idcode(raw)
                if sq:
                    ac.squawk = sq
            except Exception:
                pass

    def _apply_acas(self, ac: "Aircraft", result: dict, now: float) -> None:
        """Apply a decoded ACAS RA to the aircraft and enqueue a DB event.
        Deduplicates: events within 30s with the same description are skipped.

        DF16 air-air messages are AP/DP parity protected, so occasional one-off bit
        errors can appear as false RA activations. Persist only after repeated matching
        frames in a short window.
        """
        ra_desc = result["ra_description"]
        threat_icao = self._sanitize_threat_icao(result.get("threat_icao"), ac.icao, now)

        sig = self._acas_signature(result, threat_icao)
        prev = self._acas_candidates.get(ac.icao)
        if prev and prev[0] == sig and (now - prev[1]) <= _ACAS_CONFIRM_WINDOW_S:
            confirm_count = prev[2] + 1
        else:
            confirm_count = 1
        self._acas_candidates[ac.icao] = (sig, now, confirm_count)

        if confirm_count < _ACAS_MIN_CONFIRM_FRAMES:
            return

        # Only update live badge fields once confirmation threshold is met
        ac.acas_ra_desc       = ra_desc
        ac.acas_ra_corrective = result.get("ra_corrective", False)
        ac.acas_threat_icao   = threat_icao
        if result.get("sensitivity_level") is not None:
            ac.acas_sensitivity = result["sensitivity_level"]
        ac.acas_ra_ts = now

        last = self._last_acas_ts.get(ac.icao)
        if last and (now - last[0]) < 30 and last[1] == ra_desc:
            return  # duplicate

        self._last_acas_ts[ac.icao] = (now, ra_desc)

        if config.DEBUG_ENRICHMENT >= 1:
            log.info(
                "[acas]   %-8s  %-35s  tti=%d  threat=%-8s  alt=%-8s  sl=%s%s",
                ac.icao,
                ra_desc,
                result.get("tti", 0),
                threat_icao or "—",
                f"{ac.altitude}ft" if ac.altitude is not None else "?",
                result.get("sensitivity_level") if result.get("sensitivity_level") is not None else "?",
                "  MTE" if result.get("mte") else "",
            )

        self._pending_acas_events.append({
            "ts":              int(now),
            "icao":            ac.icao,
            "ra_description":  ra_desc,
            "ra_corrective":   int(result.get("ra_corrective", False)),
            "ra_sense":        result.get("ra_sense"),
            "ara_bits":        result.get("ara_bits"),
            "rac_bits":        result.get("rac_bits"),
            "rat":             int(result.get("rat", False)),
            "mte":             int(result.get("mte", False)),
            "tti":             result.get("tti", 0),
            "threat_icao":     threat_icao,
            "threat_alt":      result.get("threat_alt"),
            "threat_range_nm": result.get("threat_range_nm"),
            "threat_bearing_deg": result.get("threat_bearing_deg"),
            "sensitivity_level":  result.get("sensitivity_level"),
            "altitude":        ac.altitude,
        })

    def _acas_signature(self, result: dict, threat_icao: Optional[str]) -> str:
        """Return a compact signature used to corroborate repeated RA frames."""
        return "|".join((
            str(result.get("ara_bits") or ""),
            str(result.get("rac_bits") or ""),
            str(int(result.get("rat", False))),
            str(int(result.get("mte", False))),
            str(result.get("tti", 0)),
            threat_icao or "",
        ))

    def _sanitize_threat_icao(self, threat_icao: Optional[str], own_icao: str, now: float) -> Optional[str]:
        """Filter implausible DF16 threat ICAOs caused by uncorrectable bit errors.

        DF16 (and other AP/DP parity messages) do not carry an explicit ICAO field, so a
        single bit error can produce a plausible-looking but bogus 24-bit address.
        Keep threat ICAO only when it has at least one corroboration signal:
          - currently tracked locally, or
          - present in cached aircraft datasets (ADSBx/tar1090/hexdb).
        Also reject malformed/self/unallocated ICAO blocks.
        """
        if not threat_icao:
            return None

        code = threat_icao.upper()
        if not _ICAO_HEX_RE.fullmatch(code):
            return None
        if code == own_icao:
            return None
        if enrichment.db.get_country_by_icao(code) is None:
            return None

        seen_live = False
        thr = self._aircraft.get(code)
        if thr and (now - thr.last_seen) <= 120:
            seen_live = True

        known_cached = any((
            enrichment.db.get_adsbx(code),
            enrichment.db.get_tar1090_cached(code),
            enrichment.db.get_hexdb_cached(code),
        ))

        if not (seen_live or known_cached):
            return None

        return code
