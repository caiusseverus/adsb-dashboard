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
    bool     geom_alt_valid;
    int      geom_alt;
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
} decode_result_t;

void decode_init(void);
void decode_cleanup(void);
int  decode_message(const uint8_t *msg_bytes, int msg_len,
                    uint8_t signal, uint64_t timestamp,
                    decode_result_t *result);
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

def _get_lib():
    global _lib
    if _lib is None:
        path = _find_lib()
        _lib = _ffi.dlopen(path)
        _lib.decode_init()
        log.info("decode_cffi: loaded %s", path)
    return _lib


# ── Public API ─────────────────────────────────────────────────────────────

def decode_message(msg_bytes: bytes, signal: int = 0,
                   timestamp: int = 0) -> dict | None:
    """
    Decode one raw Mode-S payload (7 or 14 bytes).

    Parameters
    ----------
    msg_bytes : raw bytes from Beast frame (after unescaping)
    signal    : Beast RSSI byte (0 = strongest, 255 = weakest)
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
    if r.geom_alt_valid:
        out["geom_alt"] = r.geom_alt

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

    return out


def cleanup() -> None:
    """Release CRC tables. Call once at process exit (optional)."""
    global _lib
    if _lib is not None:
        _lib.decode_cleanup()
        _lib = None
