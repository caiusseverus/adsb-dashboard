"""
decode_cffi.py — Python cffi wrapper for libdecode.so.

Provides decode_message() as a drop-in replacement for the pyModeS calls
in aircraft_state.py.  Returns a plain dict with the same keys the current
decoder produces, or None if the message was rejected.

The shared library is expected at:
  1. $LIBDECODE_PATH  (override for packaging / cross-compile)
  2. <this file's dir>/native/libdecode.so  (dev default)
  3. <this file's dir>/libdecode.so          (make install target)
"""

import os
import logging
from pathlib import Path
from cffi import FFI

log = logging.getLogger(__name__)

# ── ABI declaration ────────────────────────────────────────────────────────

_CDEF = """
typedef struct {
    int      df;
    unsigned addr;
    int      correctedbits;
    int      addrtype;

    bool     callsign_valid;
    char     callsign[16];

    bool     baro_alt_valid;
    int      baro_alt;
    int      airground;

    bool     squawk_valid;
    unsigned squawk;

    bool     cpr_valid;
    bool     cpr_odd;
    int      cpr_type;
    unsigned cpr_lat;
    unsigned cpr_lon;

    bool     heading_valid;
    float    heading;
    int      heading_type;
    bool     ias_valid;
    unsigned ias;
    bool     tas_valid;
    unsigned tas;
    bool     mach_valid;
    double   mach;
    bool     baro_rate_valid;
    int      baro_rate;

    bool     nav_altitude_mcp_valid;
    unsigned nav_altitude_mcp;
    bool     nav_altitude_fms_valid;
    unsigned nav_altitude_fms;
    bool     nav_heading_valid;
    float    nav_heading;
    bool     nav_modes_valid;
    unsigned nav_modes;

    bool          acas_ra_valid;
    unsigned char MV[7];

    bool     category_valid;
    unsigned category;

    bool     emergency_valid;
    int      emergency;

    int      iid;
} decode_result_t;

typedef struct {
    uint8_t  msg_type;
    uint64_t timestamp;
    uint8_t  signal;
    uint8_t  msg_len;
    uint8_t  payload[14];
} beast_frame_t;

typedef struct {
    uint8_t  data[65536];
    uint32_t len;
} beast_parser_t;

typedef struct radar_burst_processor_t radar_burst_processor_t;

typedef struct {
    uint32_t icao;
    double arrival_us;
    double signal_dbfs;
    bool has_signal;
} radar_burst_event_t;

typedef struct {
    uint32_t icao;
    double burst_centroid_us;
    double burst_signal_dbfs;
    bool has_signal;
    double trigger_arrival_us;
} radar_fired_burst_t;

void decode_init(void);
void decode_cleanup(void);
int  decode_message(const uint8_t *msg_bytes, int msg_len,
                    uint8_t signal, uint64_t timestamp,
                    decode_result_t *result);
void beast_parser_init(beast_parser_t *parser);
int  beast_parse_chunk(beast_parser_t *parser,
                       const uint8_t *chunk, uint32_t chunk_len,
                       beast_frame_t *out_frames, int max_frames,
                       uint32_t *malformed_bytes);
radar_burst_processor_t *radar_burst_processor_create(void);
void radar_burst_processor_destroy(radar_burst_processor_t *processor);
int radar_burst_processor_process(radar_burst_processor_t *processor,
                                  const radar_burst_event_t *events,
                                  int event_count,
                                  double burst_gap_us,
                                  radar_fired_burst_t *out_bursts,
                                  int max_out_bursts);
int radar_burst_processor_matches_dominant_period(radar_burst_processor_t *processor,
                                                  uint32_t icao,
                                                  double period_s,
                                                  int min_bursts,
                                                  double tolerance);
int radar_burst_processor_matches_phase_family(radar_burst_processor_t *processor,
                                               uint32_t ref_icao,
                                               double ref_arrival_us,
                                               uint32_t icao,
                                               double burst_centroid_us,
                                               double period_s,
                                               int min_history,
                                               double tolerance_us);
uint32_t radar_burst_processor_select_reference(radar_burst_processor_t *processor,
                                                double period_s,
                                                double now_us,
                                                int min_bursts_for_ref,
                                                double recency_periods,
                                                double hysteresis,
                                                uint32_t current_ref_icao);

int solve_cpr_airborne(int even_cprlat, int even_cprlon,
                       int odd_cprlat,  int odd_cprlon,
                       int fflag,
                       double *out_lat, double *out_lon);

int solve_cpr_relative(double reflat,  double reflon,
                       int cprlat,     int cprlon,
                       int fflag,      int surface,
                       double *out_lat, double *out_lon);
"""

# ── Library loading ────────────────────────────────────────────────────────

def _find_lib() -> str:
    env = os.environ.get("LIBDECODE_PATH")
    if env:
        return env
    here = Path(__file__).parent
    for candidate in (here / "native" / "libdecode.so", here / "libdecode.so"):
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(
        "libdecode.so not found. Run `make` in backend/native/ first."
    )


_ffi = FFI()
_ffi.cdef(_CDEF)

_lib = None   # loaded lazily on first call
_has_beast_parser = None

# Pre-allocated output buffers for CPR solvers.
# Reused on every call — safe because the decode thread is single-threaded.
_cpr_out_lat = _ffi.new("double *")
_cpr_out_lon = _ffi.new("double *")

def _get_lib():
    global _lib
    if _lib is None:
        path = _find_lib()
        _lib = _ffi.dlopen(path)
        _lib.decode_init()
        log.info("decode_cffi: loaded %s", path)
    return _lib


def has_beast_parser() -> bool:
    global _has_beast_parser
    if _has_beast_parser is None:
        lib = _get_lib()
        _has_beast_parser = all(
            hasattr(lib, name)
            for name in ("beast_parser_init", "beast_parse_chunk")
        )
    return bool(_has_beast_parser)


# ── Public API ─────────────────────────────────────────────────────────────

def decode_message(msg_bytes: bytes, signal: int = 0,
                   timestamp: int = 0) -> dict | None:
    """
    Decode one raw Mode-S payload (7 or 14 bytes).

    Parameters
    ----------
    msg_bytes : raw bytes from Beast frame (after unescaping)
    signal    : Beast amplitude byte (0 = weakest, 255 = strongest)
    timestamp : 48-bit Beast clock value

    Returns
    -------
    dict with decoded fields, or None if the message was rejected.
    """
    msg_len = len(msg_bytes)
    if msg_len not in (7, 14):
        return None

    lib = _get_lib()
    result = _ffi.new("decode_result_t *")
    buf    = _ffi.from_buffer(msg_bytes)

    rc = lib.decode_message(buf, msg_len, signal & 0xFF, timestamp & 0xFFFFFFFFFFFF, result)
    if rc != 0:
        return None

    r = result
    out: dict = {
        "df":           r.df,
        "addr":         r.addr,
        "correctedbits": r.correctedbits,
        "addrtype":     r.addrtype,
        "airground":    r.airground,
    }

    if r.callsign_valid:
        out["callsign"] = _ffi.string(r.callsign).decode("ascii", errors="replace").strip()

    if r.baro_alt_valid:
        out["baro_alt"] = r.baro_alt

    if r.squawk_valid:
        # squawk is stored as hex-encoded integer e.g. 0x7700 → "7700"
        out["squawk"] = f"{r.squawk:04X}"

    if r.cpr_valid:
        out["cpr_odd"]  = bool(r.cpr_odd)
        out["cpr_type"] = r.cpr_type
        out["cpr_lat"]  = r.cpr_lat
        out["cpr_lon"]  = r.cpr_lon

    if r.heading_valid:
        out["heading"]      = float(r.heading)
        out["heading_type"] = r.heading_type

    if r.ias_valid:
        out["ias"] = r.ias

    if r.tas_valid:
        out["tas"] = r.tas

    if r.mach_valid:
        out["mach"] = float(r.mach)

    if r.baro_rate_valid:
        out["baro_rate"] = r.baro_rate

    if r.nav_altitude_mcp_valid:
        out["nav_altitude_mcp"] = r.nav_altitude_mcp

    if r.nav_altitude_fms_valid:
        out["nav_altitude_fms"] = r.nav_altitude_fms

    if r.nav_heading_valid:
        out["nav_heading"] = float(r.nav_heading)

    if r.nav_modes_valid:
        out["nav_modes"] = r.nav_modes

    if r.acas_ra_valid:
        out["acas_ra"] = bytes(_ffi.buffer(r.MV, 7))

    if r.category_valid:
        out["category"] = r.category

    if r.emergency_valid:
        out["emergency"] = r.emergency

    # DF11 interrogator identifier (0 for all other DFs)
    out["iid"] = int(r.iid)

    return out


def solve_cpr_airborne(even_cprlat: int, even_cprlon: int,
                       odd_cprlat: int, odd_cprlon: int,
                       fflag: int) -> tuple | None:
    """Global CPR decode from an even+odd frame pair.

    fflag = 0 if the most-recently-received frame is even, 1 if odd.
    Returns (lat, lon) on success, None on failure (ambiguous zone, etc.).
    """
    rc = _get_lib().solve_cpr_airborne(even_cprlat, even_cprlon,
                                       odd_cprlat,  odd_cprlon,
                                       fflag, _cpr_out_lat, _cpr_out_lon)
    return (_cpr_out_lat[0], _cpr_out_lon[0]) if rc == 0 else None


def solve_cpr_relative(reflat: float, reflon: float,
                       cprlat: int, cprlon: int,
                       fflag: int, surface: int = 0) -> tuple | None:
    """Local CPR decode using a reference position.

    surface = 0 for airborne (default), 1 for surface movement.
    Returns (lat, lon) on success, None on failure.
    """
    rc = _get_lib().solve_cpr_relative(reflat, reflon, cprlat, cprlon,
                                       fflag, surface,
                                       _cpr_out_lat, _cpr_out_lon)
    return (_cpr_out_lat[0], _cpr_out_lon[0]) if rc == 0 else None


def cleanup() -> None:
    """Release CRC tables. Call once at process exit (optional)."""
    global _lib
    if _lib is not None:
        _lib.decode_cleanup()
        _lib = None


class BeastParser:
    """Stateful wrapper around the native Beast stream parser."""

    def __init__(self, max_frames: int = 256):
        if max_frames <= 0:
            raise ValueError("max_frames must be > 0")
        self._parser = _ffi.new("beast_parser_t *")
        self._frames = _ffi.new("beast_frame_t[]", max_frames)
        self._malformed = _ffi.new("uint32_t *")
        self._max_frames = max_frames
        _get_lib().beast_parser_init(self._parser)

    def parse_chunk(self, chunk: bytes) -> tuple[list[dict], int]:
        if not chunk:
            return [], 0
        buf = _ffi.from_buffer(chunk)
        count = _get_lib().beast_parse_chunk(
            self._parser,
            buf,
            len(chunk),
            self._frames,
            self._max_frames,
            self._malformed,
        )
        out: list[dict] = []
        for i in range(count):
            frame = self._frames[i]
            payload = bytes(_ffi.buffer(frame.payload, frame.msg_len))
            out.append({
                "raw": payload,
                "timestamp": int(frame.timestamp),
                "signal": int(frame.signal),
                "type": int(frame.msg_type),
            })
        return out, int(self._malformed[0])


class RadarBurstProcessor:
    """Stateful native helper for DF11 burst accumulation and firing."""

    def __init__(self, max_events: int = 256, max_bursts: int = 4096):
        if max_events <= 0 or max_bursts <= 0:
            raise ValueError("max_events and max_bursts must be > 0")
        self._processor = _get_lib().radar_burst_processor_create()
        if self._processor == _ffi.NULL:
            raise MemoryError("failed to allocate radar burst processor")
        self._events = _ffi.new("radar_burst_event_t[]", max_events)
        self._bursts = _ffi.new("radar_fired_burst_t[]", max_bursts)
        self._max_events = max_events
        self._max_bursts = max_bursts

    def __del__(self):
        processor = getattr(self, "_processor", None)
        if processor not in (None, _ffi.NULL):
            try:
                _get_lib().radar_burst_processor_destroy(processor)
            except Exception:
                pass
            self._processor = _ffi.NULL

    def _ensure_capacity(self, event_count: int) -> None:
        required_events = max(1, event_count)
        required_bursts = max(self._max_bursts, required_events * 16, 4096)
        if required_events <= self._max_events and required_bursts <= self._max_bursts:
            return
        new_max_events = max(required_events, self._max_events * 2)
        new_max_bursts = max(required_bursts, self._max_bursts * 2)
        self._events = _ffi.new("radar_burst_event_t[]", new_max_events)
        self._bursts = _ffi.new("radar_fired_burst_t[]", new_max_bursts)
        self._max_events = new_max_events
        self._max_bursts = new_max_bursts

    def process_batch(
        self,
        events: list[tuple[float, str, float | None]],
        burst_gap_us: float,
    ) -> list[dict]:
        if not events:
            return []
        self._ensure_capacity(len(events))
        for i, (arrival_us, icao_hex, signal_dbfs) in enumerate(events):
            event = self._events[i]
            event.icao = int(icao_hex, 16)
            event.arrival_us = arrival_us
            if signal_dbfs is None:
                event.has_signal = False
                event.signal_dbfs = 0.0
            else:
                event.has_signal = True
                event.signal_dbfs = float(signal_dbfs)

        count = _get_lib().radar_burst_processor_process(
            self._processor,
            self._events,
            len(events),
            float(burst_gap_us),
            self._bursts,
            self._max_bursts,
        )
        out: list[dict] = []
        for i in range(count):
            burst = self._bursts[i]
            out.append({
                "icao": f"{int(burst.icao):06X}",
                "burst_centroid_us": float(burst.burst_centroid_us),
                "burst_signal": float(burst.burst_signal_dbfs) if bool(burst.has_signal) else None,
                "trigger_arrival_us": float(burst.trigger_arrival_us),
            })
        return out

    def matches_dominant_period(
        self,
        icao_hex: str,
        period_s: float,
        min_bursts: int,
        tolerance: float,
    ) -> bool:
        return bool(_get_lib().radar_burst_processor_matches_dominant_period(
            self._processor,
            int(icao_hex, 16),
            float(period_s),
            int(min_bursts),
            float(tolerance),
        ))

    def matches_phase_family(
        self,
        ref_icao_hex: str,
        ref_arrival_us: float,
        icao_hex: str,
        burst_centroid_us: float,
        period_s: float,
        min_history: int,
        tolerance_us: float,
    ) -> bool:
        return bool(_get_lib().radar_burst_processor_matches_phase_family(
            self._processor,
            int(ref_icao_hex, 16),
            float(ref_arrival_us),
            int(icao_hex, 16),
            float(burst_centroid_us),
            float(period_s),
            int(min_history),
            float(tolerance_us),
        ))

    def select_reference(
        self,
        period_s: float,
        now_us: float,
        min_bursts_for_ref: int,
        recency_periods: float,
        hysteresis: float,
        current_ref_icao: str | None,
    ) -> str | None:
        current_ref = int(current_ref_icao, 16) if current_ref_icao else 0
        selected = int(_get_lib().radar_burst_processor_select_reference(
            self._processor,
            float(period_s),
            float(now_us),
            int(min_bursts_for_ref),
            float(recency_periods),
            float(hysteresis),
            current_ref,
        ))
        if selected == 0:
            return None
        return f"{selected:06X}"
