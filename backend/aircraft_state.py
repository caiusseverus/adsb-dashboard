"""
Thread-safe in-memory state for live aircraft and message-rate tracking.
"""

import math
import time
import threading
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pyModeS as pms

import acas as acas_decoder
import config
import enrichment
from db import INTERESTING_TYPE_CODES
from utils import country_from_registration

log = logging.getLogger(__name__)

_R_NM = 3440.065  # Earth radius in nautical miles

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
        "[enrichment] NEW %-8s  reg=%-8s type=%-6s year=%-4s  adsbx=%-4s  "
        "operator=%s [%s]  country=%s  mil=%s",
        icao,
        ac.registration or "—",
        ac.type_code or "—",
        ac.year or "—",
        "hit" if adsbx else "miss",
        repr(ac.operator) if ac.operator else "—",
        op_source,
        ac.country or "—",
        ac.military,
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
    sighting_count: int = 1               # from aircraft_registry; 1 = unique (never seen before)
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
        self._hexdb_queue: set[str] = set()  # ICAOs awaiting hexdb.io operator lookup

        # ACAS event queue: drained by main._db_writer() every minute
        self._pending_acas_events: deque[dict] = deque(maxlen=500)
        self._last_acas_ts: dict[str, tuple[float, str]] = {}  # icao → (ts, ra_desc)

        # Unique aircraft seen today (resets at midnight; seeded from DB on startup)
        self._today_date: str = date.today().isoformat()
        self._today_icaos: set[str] = set()
        self._today_mil_icaos: set[str] = set()

        # sighting_count seed from DB (icao -> count); used when creating new Aircraft objects
        self._sighting_counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_message(self, msg: dict) -> None:
        raw: str = msg["raw"]
        signal: int = msg.get("signal", 0)
        now = time.time()

        with self._lock:
            self._total += 1
            self._tick(now)
            self._decode(raw, signal, now)

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
                "sighting_count":     ac.sighting_count,
                "acas_ra_active":     ac.acas_ra_active,
                "acas_ra_desc":       ac.acas_ra_desc,
                "acas_ra_corrective": ac.acas_ra_corrective,
                "acas_threat_icao":   ac.acas_threat_icao,
                "acas_sensitivity":   ac.acas_sensitivity,
            }

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

            # Rate history for the chart (completed minutes + current partial minute)
            secs = list(self._cur_min_sec_counts)
            if secs:
                cur_mn, cur_mx, cur_me = min(secs), max(secs), round(sum(secs) / len(secs), 1)
            else:
                cur_mn = cur_mx = cur_me = 0.0
            cur_total = len(self._aircraft)
            cur_mil = sum(1 for ac in self._aircraft.values() if ac.military)
            cur_sigs = self._cur_min_signals
            cur_sig_avg = round(sum(cur_sigs) / len(cur_sigs), 1) if cur_sigs else None
            cur_stats = (self._cur_min, cur_mn, cur_mx, cur_me,
                         cur_total, cur_total - cur_mil, cur_mil,
                         cur_sig_avg, min(cur_sigs) if cur_sigs else None,
                         max(cur_sigs) if cur_sigs else None)
            rate_history = list(self._min_stats) + [cur_stats]
            df_history = list(self._min_df_stats) + [(self._cur_min, dict(self._cur_min_df_counts))]

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
                    "acas_ra_active":     ac.acas_ra_active,
                    "acas_ra_desc":       ac.acas_ra_desc,
                    "acas_ra_corrective": ac.acas_ra_corrective,
                    "acas_threat_icao":   ac.acas_threat_icao,
                    "acas_sensitivity":   ac.acas_sensitivity,
                }
                for ac in self._aircraft.values()
            ]

        live_military = sum(1 for ac in self._aircraft.values() if ac.military)

        return {
            "aircraft_count": len(aircraft_list),
            "live_military": live_military,
            "msg_per_sec": msg_per_sec,
            "total_messages": self._total,
            "unique_today": len(self._today_icaos),
            "unique_today_military": len(self._today_mil_icaos),
            "hexdb_queue_size": len(self._hexdb_queue),
            "aircraft": sorted(aircraft_list, key=lambda x: x["msg_count"], reverse=True),
            "rate_history": [
                {"minute": m, "min": mn, "max": mx, "mean": me,
                 "ac_total": t, "ac_civil": c, "ac_military": mil,
                 "signal_avg": sa, "signal_min": sn, "signal_max": sx}
                for m, mn, mx, me, t, c, mil, sa, sn, sx in rate_history[-60:]
            ],
            "df_history": [
                {"minute": m, "counts": counts}
                for m, counts in df_history[-60:]
            ],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _tick(self, now: float) -> None:
        sec = int(now)
        if sec != self._cur_sec:
            self._sec_counts.append((self._cur_sec, self._cur_sec_count))
            self._cur_min_sec_counts.append(self._cur_sec_count)
            self._cur_sec = sec
            self._cur_sec_count = 0
        self._cur_sec_count += 1

        minute = int(now // 60)
        if minute != self._cur_min:
            secs = self._cur_min_sec_counts
            if secs:
                mn, mx, me = min(secs), max(secs), round(sum(secs) / len(secs), 1)
            else:
                mn = mx = me = 0.0
            total = len(self._aircraft)
            mil = sum(1 for ac in self._aircraft.values() if ac.military)
            sigs = self._cur_min_signals
            sig_avg = round(sum(sigs) / len(sigs), 1) if sigs else None
            sig_min = min(sigs) if sigs else None
            sig_max = max(sigs) if sigs else None
            self._min_stats.append(
                (self._cur_min, mn, mx, me, total, total - mil, mil,
                 sig_avg, sig_min, sig_max)
            )
            self._min_df_stats.append((self._cur_min, dict(self._cur_min_df_counts)))
            self._cur_min = minute
            self._cur_min_sec_counts = []
            self._cur_min_signals = []
            self._cur_min_df_counts = {}

    def _decode(self, raw: str, signal: int, now: float) -> None:
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

            # Primary enrichment: ADSBExchange
            adsbx = enrichment.db.get_adsbx(icao)
            if adsbx:
                ac.registration = adsbx.get("reg") or None
                ac.type_code    = adsbx.get("icaotype") or None
                ac.year         = adsbx.get("year") or None
                mfr   = adsbx.get("manufacturer") or ""
                model = adsbx.get("model") or ""
                ac.manufacturer = mfr or None
                ac.type_desc = (f"{mfr} {model}".strip()) or None
                if adsbx.get("ownop"):
                    ac.operator = adsbx["ownop"]

            # Type details: WTC + full name from tar1090-db type files
            if ac.type_code:
                ti = enrichment.db.get_type_info(ac.type_code)
                if ti:
                    ac.type_full_name = ti.get("name") or None
                    ac.type_category  = ti.get("desc") or None
                    ac.wtc            = ti.get("wtc")  or None

            ac.military = enrichment.db.is_military(icao)
            ac.country  = enrichment.db.get_country_by_icao(icao)

            # Apply any previously cached hexdb data immediately (no HTTP, no wait)
            hexdb_cached = enrichment.db.get_hexdb_cached(icao)
            if hexdb_cached:
                _apply_hexdb_data(ac, hexdb_cached)

            # Determine operator source for debug logging
            if adsbx and adsbx.get("ownop") and ac.operator == adsbx["ownop"]:
                op_source = "adsbx"
            elif hexdb_cached and ac.operator:
                op_source = "hexdb-cache"
            elif ac.operator:
                op_source = "unknown"
            else:
                op_source = "queued"

            # Registration prefix overrides ICAO-block country (more accurate for GA)
            if ac.registration:
                reg_country = country_from_registration(ac.registration)
                if reg_country:
                    ac.country = reg_country

            # Queue hexdb HTTP lookup if any key field is still missing
            if not ac.operator or not ac.registration or not ac.type_code:
                self._hexdb_queue.add(icao)

            if config.DEBUG_ENRICHMENT:
                _log_enrichment(icao, ac, adsbx, op_source)

            self._aircraft[icao] = ac

        ac = self._aircraft[icao]
        ac.last_seen = now
        ac.msg_count += 1
        ac.signal = signal

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

        # --- Decode ADS-B (DF 17) ---
        if df == 17 and len(raw) == 28:
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
                        ac.altitude = alt
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
                    if (ac.cpr_even and ac.cpr_odd
                            and abs(ac.cpr_even[1] - ac.cpr_odd[1]) < 10):
                        pos = pms.adsb.position(
                            ac.cpr_even[0], ac.cpr_odd[0],
                            ac.cpr_even[1], ac.cpr_odd[1],
                        )

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
        elif df == 4 and len(raw) == 14:
            try:
                alt = pms.altcode(raw)
                if alt is not None:
                    ac.altitude = alt
            except Exception:
                pass

        # --- DF 20/21: Comm-B replies (28 hex chars / 112 bits) ---
        elif df in (20, 21) and len(raw) == 28:
            # Altitude from DF20
            if df == 20:
                try:
                    alt = pms.altcode(raw)
                    if alt is not None:
                        ac.altitude = alt
                except Exception:
                    pass

            # EHS decode: infer BDS register and extract data
            try:
                bds = pms.bds.infer(raw)
            except Exception:
                bds = None

            if bds == "BDS40":
                # Selected altitude (autopilot target)
                try:
                    sel = pms.commb.selalt40mcp(raw)
                    if sel is not None:
                        ac.selected_alt = int(sel)
                except Exception:
                    pass

            elif bds == "BDS50":
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

            elif bds == "BDS60":
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
        elif df == 16 and len(raw) == 28:
            try:
                alt = pms.altcode(raw)
                if alt is not None:
                    ac.altitude = alt
            except Exception:
                pass
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
        Deduplicates: events within 30s with the same description are skipped."""
        ra_desc = result["ra_description"]
        last = self._last_acas_ts.get(ac.icao)
        if last and (now - last[0]) < 30 and last[1] == ra_desc:
            return  # duplicate

        ac.acas_ra_active     = True
        ac.acas_ra_desc       = ra_desc
        ac.acas_ra_corrective = result.get("ra_corrective", False)
        ac.acas_threat_icao   = result.get("threat_icao")
        if result.get("sensitivity_level") is not None:
            ac.acas_sensitivity = result["sensitivity_level"]
        ac.acas_ra_ts = now

        self._last_acas_ts[ac.icao] = (now, ra_desc)

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
            "threat_icao":     result.get("threat_icao"),
            "threat_alt":      result.get("threat_alt"),
            "threat_range_nm": result.get("threat_range_nm"),
            "threat_bearing_deg": result.get("threat_bearing_deg"),
            "sensitivity_level":  result.get("sensitivity_level"),
            "altitude":        ac.altitude,
        })
