"""
Thread-safe in-memory state for live aircraft and message-rate tracking.
"""

import math
import time
import threading
import logging
import re
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from datetime import date
from enum import IntEnum
from typing import NamedTuple, Optional

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
_HEXDB_QUEUE_MAX = 600      # hard ceiling: startup batch + live overflow
_SIGHTING_LRU_MAX = 10_000  # ~800 KB worst case; evicts oldest on busy long-running sites

# ---------------------------------------------------------------------------
# Message source classification (mirrors readsb SOURCE_* in track.h)
# Higher value = higher integrity.  Used by altitude and position filters
# to decide whether a new reading can override the current one.
# ---------------------------------------------------------------------------
class MsgSource(IntEnum):
    INVALID        = 0
    MODE_AC        = 1
    MLAT           = 2
    MODE_S         = 3   # DF0/4/5/16/20/21 Address/Parity, ICAO filter hit
    MODE_S_CHECKED = 4   # DF11, clean or single-bit-corrected CRC
    ADSR           = 5   # DF18
    ADSB           = 6   # DF17 (clean or corrected — good_crc differentiates)

# ICAO filter — confirmed addresses from DF17/18 clean CRC.
# Address/Parity DFs (0/4/5/16/20/21) are only decoded if the ICAO
# appears here — prevents ghost aircraft from CRC-syndrome collisions.
_CONFIRMED_ICAO_MAX    = 20_000   # LRU cap (readsb: icaoFilterAdd)
_ICAO_FILTER_EXPIRY_S  = 60.0     # evict if not refreshed within 60 s (readsb: icaoFilterExpire)
_AP_DFS = frozenset({0, 4, 5, 16, 20, 21})

# Altitude filtering constants (mirrors readsb track.h / updateAltitude())
_ALT_MIN_FT             = -1_000  # below this is a bad decode (subterranean)
_ALT_MAX_FT             = 60_000  # above this is a bad decode (service ceiling)
_ALT_RELIABLE_MAX       = 20      # ALTITUDE_BARO_RELIABLE_MAX
_ALT_RELIABLE_PUBLISH   = 2       # minimum alt_reliable to include altitude in snapshot output
_ALT_LOW_DELTA_FT       = 300     # below this delta, accept unconditionally
_ALT_DEFAULT_MAX_FPM    = 12500   # default rate ceiling when no vertical rate known
_ALT_DEFAULT_MIN_FPM    = -12500
_ALT_RATE_TOLERANCE_FPM = 1500    # ± window around reported vertical rate
_ALT_RATE_AGE_SLOP_MAX  = 11000   # max extra fpm slop added for stale rate data
_ALT_QBIT_CEILING_FT    = 50175   # above this, Q-bit encoding becomes unreliable
_ALT_STALE_WINDOW_S     = 30.0    # beyond this, alt_reliable is capped to 0
_ALT_FAST_UPDATE_S      = 2.0     # CRC=0 + age < this → good_crc = MAX

# MLAT quality constants
_MLAT_MIN_DT_S        = 0.5   # minimum fix interval for speed-based spike detection
_MLAT_MAX_SPEED_KT    = 750   # implied groundspeed ceiling (kt) — above this = spike
_MLAT_FIX_MAXLEN      = 20    # rolling fix-buffer depth per source (~10–20 s at 1–2 Hz)
_MLAT_STALE_EVICT_S   = 120   # evict a source's per-aircraft state after this silence (s)
_MLAT_ALIGN_WINDOW    = 2.0   # max age difference (s) for cross-source residual pairing
_MLAT_RESIDUAL_MAXLEN = 30    # rolling residual buffer depth per source
_MLAT_FUSION_WINDOW   = 4.0   # max fix age (s) for inclusion in weighted fusion

# ADS-B position reliability constants (mirrors readsb track.c pos_reliable)
_POS_RELIABLE_MAX              = 4.0    # ceiling for the reliability score
_POS_RELIABLE_DECAY            = 0.26   # per-failure penalty
_POS_RELIABLE_PUBLISH          = 1.0    # minimum score to include lat/lon in snapshot output
_POS_RELIABLE_RESET_TIMEOUT_S  = 3600.0 # 60-min silence → reset scores
_ADSB_MAX_SPEED_KT             = 1500   # hard ceiling for ADS-B speed check
_SPEED_UNCERTAINTY_KT_PER_S    = 3.0    # ceiling widens +3 kt per second elapsed
_MLAT_FORCE_INTERVAL_S         = 30.0   # mlatForce: min interval between force-accepts
_MLAT_FORCE_DISTANCE_NM        = 13.5   # mlatForce: min distance from ADS-B pos (25 km)

# CPR duplicate detection constants
_CPR_DUP_WINDOW_S   = 2.0    # window for considering identical frames as duplicates
_CPR_DUP_CACHE_LEN  = 6      # max recent CPR frames cached per aircraft

# Position discard cache constants (prevents double-penalising the same bad position)
_DISCARD_CACHE_LEN  = 4
_DISCARD_CACHE_TTL  = 4.0    # seconds

# ── Kalman filter constants ────────────────────────────────────────────────
# 2-D constant-velocity filter; state = [east_m, north_m, ve_m/s, vn_m/s]
_KF_SIGMA_A  = 5.0       # m/s² process noise (typical aircraft manoeuvring accel)
_KF_SIGMA_M0 = 50.0      # m    base MLAT measurement noise (quality=1.0 source)
_KF_GATE_D2  = 13.816    # chi² innovations gate, 99.9 % threshold, 2 DOF
_KF_R_EARTH  = 6_371_000.0


# ── Kalman helper functions ────────────────────────────────────────────────

def _enu(lat: float, lon: float, ref_lat: float, ref_lon: float) -> tuple[float, float]:
    """Lat/lon → approximate ENU metres relative to reference point.
    Accurate to <0.1 % for offsets up to ~50 km — sufficient for MLAT.
    """
    north = math.radians(lat - ref_lat) * _KF_R_EARTH
    east  = math.radians(lon - ref_lon) * math.cos(math.radians(ref_lat)) * _KF_R_EARTH
    return east, north


def _latlon_from_enu(east: float, north: float,
                     ref_lat: float, ref_lon: float) -> tuple[float, float]:
    lat = ref_lat + math.degrees(north / _KF_R_EARTH)
    lon = ref_lon + math.degrees(east / (_KF_R_EARTH * math.cos(math.radians(ref_lat))))
    return lat, lon


def _kf_init(e: float, n: float) -> tuple[list, list]:
    """Initialise state and covariance at a known ENU position, zero velocity."""
    x = [e, n, 0.0, 0.0]
    P0p = 500.0 ** 2   # 500 m initial position uncertainty (1-sigma)
    P0v = 150.0 ** 2   # 150 m/s (~300 kt) initial velocity uncertainty
    P = [P0p, 0.0, 0.0, 0.0,
         0.0, P0p, 0.0, 0.0,
         0.0, 0.0, P0v, 0.0,
         0.0, 0.0, 0.0, P0v]
    return x, P


def _kf_predict(x: list, P: list, dt: float) -> tuple[list, list]:
    """Constant-velocity predict step.  P is 4×4 stored row-major (16 floats)."""
    dt2 = dt * dt
    dt3 = dt2 * dt
    dt4 = dt2 * dt2
    # x_pred = F * x
    x_p = [x[0] + x[2]*dt, x[1] + x[3]*dt, x[2], x[3]]
    # F*P*F' — expanded analytically, exploiting F sparsity
    FPFt = [
        P[0]  + dt*(P[8]+P[2])  + dt2*P[10],   # (0,0) e,e
        P[1]  + dt*(P[9]+P[3])  + dt2*P[11],   # (0,1) e,n
        P[2]  + dt*P[10],                        # (0,2) e,ve
        P[3]  + dt*P[11],                        # (0,3) e,vn
        P[4]  + dt*(P[12]+P[6]) + dt2*P[14],   # (1,0) n,e
        P[5]  + dt*(P[13]+P[7]) + dt2*P[15],   # (1,1) n,n
        P[6]  + dt*P[14],                        # (1,2) n,ve
        P[7]  + dt*P[15],                        # (1,3) n,vn
        P[8]  + dt*P[10],                        # (2,0) ve,e
        P[9]  + dt*P[11],                        # (2,1) ve,n
        P[10],                                    # (2,2) ve,ve
        P[11],                                    # (2,3) ve,vn
        P[12] + dt*P[14],                        # (3,0) vn,e
        P[13] + dt*P[15],                        # (3,1) vn,n
        P[14],                                    # (3,2) vn,ve
        P[15],                                    # (3,3) vn,vn
    ]
    # Add process noise Q (constant-velocity model, σ_a²)
    sa2 = _KF_SIGMA_A * _KF_SIGMA_A
    FPFt[0]  += sa2 * dt4 / 4   # e,e
    FPFt[5]  += sa2 * dt4 / 4   # n,n
    FPFt[10] += sa2 * dt2        # ve,ve
    FPFt[15] += sa2 * dt2        # vn,vn
    FPFt[2]  += sa2 * dt3 / 2   # e,ve
    FPFt[8]  += sa2 * dt3 / 2   # ve,e
    FPFt[7]  += sa2 * dt3 / 2   # n,vn
    FPFt[13] += sa2 * dt3 / 2   # vn,n
    return x_p, FPFt


def _kf_update(x_p: list, P_p: list,
               e_meas: float, n_meas: float,
               quality: float) -> tuple[list, list, bool]:
    """Kalman measurement update with chi-squared innovations gate.

    Measurement noise R = (σ_m0 / quality)² — poor-geometry sources get
    larger noise and therefore contribute less to the fused estimate.
    Returns (x_new, P_new, accepted).
    """
    sigma_m = _KF_SIGMA_M0 / max(quality, 0.1)
    R = sigma_m * sigma_m

    # Innovation y = z − H·x_pred  (H extracts position rows)
    y0 = e_meas - x_p[0]
    y1 = n_meas - x_p[1]

    # S = H·P·H' + R  (top-left 2×2 of P, plus R on diagonal)
    S00 = P_p[0] + R;  S01 = P_p[1]
    S10 = P_p[4];      S11 = P_p[5] + R

    det_S = S00*S11 - S01*S10
    if abs(det_S) < 1.0:          # degenerate — skip update
        return x_p, P_p, False

    Si00 =  S11 / det_S;  Si01 = -S01 / det_S
    Si10 = -S10 / det_S;  Si11 =  S00 / det_S

    # Chi-squared innovations gate: d² = y' S⁻¹ y
    d2 = Si00*y0*y0 + (Si01+Si10)*y0*y1 + Si11*y1*y1
    if d2 > _KF_GATE_D2:
        return x_p, P_p, False     # outlier — reject measurement

    # Kalman gain K = P·H'·S⁻¹  (4×2, stored as two column vectors K0, K1)
    K0 = [P_p[i*4]*Si00 + P_p[i*4+1]*Si10 for i in range(4)]
    K1 = [P_p[i*4]*Si01 + P_p[i*4+1]*Si11 for i in range(4)]

    x_new = [x_p[i] + K0[i]*y0 + K1[i]*y1 for i in range(4)]
    # P_new = (I − K·H)·P  →  P[i,j] − K0[i]·P[0,j] − K1[i]·P[1,j]
    P_new = [P_p[i*4+j] - K0[i]*P_p[j] - K1[i]*P_p[4+j]
             for i in range(4) for j in range(4)]
    return x_new, P_new, True


def _kalman_init(ac: "Aircraft", lat: float, lon: float, now: float) -> None:
    """Initialise (or re-initialise) the per-aircraft Kalman state."""
    x, P = _kf_init(0.0, 0.0)   # ENU origin = current position
    ac.kalman_state = {"x": x, "P": P, "ref_lat": lat, "ref_lon": lon, "ts": now}


def _kalman_update_position(ac: "Aircraft", lat: float, lon: float,
                             now: float, quality: float) -> tuple[float, float]:
    """Sequential predict+update cycle for one incoming MLAT fix.

    Each source fix is treated as an independent measurement; quality score
    controls measurement noise so weaker sources contribute proportionally less.
    Always returns the current Kalman position estimate.
    """
    ks = ac.kalman_state
    dt = now - ks["ts"]

    x_p, P_p = _kf_predict(ks["x"], ks["P"], dt)

    e_meas, n_meas = _enu(lat, lon, ks["ref_lat"], ks["ref_lon"])
    x_new, P_new, _ = _kf_update(x_p, P_p, e_meas, n_meas, quality)

    ks["x"] = x_new
    ks["P"] = P_new
    ks["ts"] = now

    return _latlon_from_enu(x_new[0], x_new[1], ks["ref_lat"], ks["ref_lon"])


class MlatFix(NamedTuple):
    """A single MLAT position fix from one network."""
    ts:  float   # time.time() when received
    lat: float
    lon: float


def _alt_baro_reliable(ac: "Aircraft") -> bool:
    """Mirror of readsb altBaroReliable(): alt_reliable >= ALT_RELIABLE_PUBLISH."""
    return ac.alt_reliable >= _ALT_RELIABLE_PUBLISH


def _penalise_alt_reliable(ac: "Aircraft", good_crc: int = 0) -> None:
    """Decrement alt_reliable and invalidate source on floor breach."""
    ac.alt_reliable -= (good_crc + 1)
    if ac.alt_reliable <= 0:
        ac.alt_reliable = 0
        ac._alt_source = None


def _accept_altitude(ac: "Aircraft", alt: int, source: "MsgSource",
                     crc_clean: bool, now: float) -> bool:
    """Update ac.altitude if alt passes the readsb-equivalent 8-layer filter.

    Maintains alt_reliable score (0..ALT_RELIABLE_MAX).  Altitude is only
    published in snapshots when alt_reliable >= ALT_RELIABLE_PUBLISH (2).
    Mirrors readsb updateAltitude() in track.c.
    """
    # Layer 0: range gate
    if not (_ALT_MIN_FT <= alt <= _ALT_MAX_FT):
        return False

    # Layer 1: MLAT veto
    # mlat-client copies baro alt from a secondary reply on another receiver —
    # not from the MLAT geometry. readsb: "terrible altitude source, ignore".
    if source == MsgSource.MLAT:
        return False

    # Layer 2: Q-bit anomaly above 50,175 ft
    # Gillham/Q-bit encoding produces invalid values above this ceiling.
    # Apply to MODE_S only (ADS-B TC9-22 uses its own encoding).
    if source == MsgSource.MODE_S and alt > _ALT_QBIT_CEILING_FT:
        _penalise_alt_reliable(ac)
        return False

    # Layer 3: compute good_crc — message confidence score
    # Mirrors readsb: mm->crc == 0 gates the high-confidence path.
    baro_age_s = (now - ac._alt_ts) if ac._alt_ts > 0 else _ALT_STALE_WINDOW_S
    good_crc = 0
    if crc_clean and source > MsgSource.MODE_S_CHECKED:
        # CRC residual zero + ADSB/ADSR source — highest confidence
        if baro_age_s < _ALT_FAST_UPDATE_S:
            good_crc = _ALT_RELIABLE_MAX       # 20: rapid clean ADS-B update
        else:
            good_crc = _ALT_RELIABLE_MAX // 3  # 7: clean but infrequent

    # Layer 4: cap alt_reliable by data staleness when delta is large
    delta = (alt - ac.altitude) if ac.altitude is not None else 0
    fpm = 0
    if abs(delta) >= _ALT_LOW_DELTA_FT:
        # Implied rate (readsb formula: delta*600 / (baroAge_100ms + 10))
        baro_age_units = baro_age_s * 10 + 10  # prevent div/0
        fpm = delta * 600.0 / baro_age_units
        if baro_age_s < _ALT_STALE_WINDOW_S:
            stale_cap = int(_ALT_RELIABLE_MAX * (1.0 - baro_age_s / _ALT_STALE_WINDOW_S))
            ac.alt_reliable = min(ac.alt_reliable, stale_cap)
        else:
            ac.alt_reliable = 0

    # Layer 5: dynamic vertical rate window
    min_fpm = _ALT_DEFAULT_MIN_FPM
    max_fpm = _ALT_DEFAULT_MAX_FPM
    if abs(delta) >= _ALT_LOW_DELTA_FT:
        geom_age_ms = (now - ac._vrate_geom_ts) * 1000 if ac._vrate_geom_fpm is not None else float('inf')
        baro_age_ms = (now - ac._vrate_baro_ts) * 1000 if ac._vrate_baro_fpm is not None else float('inf')
        vrate = None
        if ac._vrate_geom_fpm is not None and geom_age_ms < baro_age_ms:
            vrate = ac._vrate_geom_fpm
            slop = min(_ALT_RATE_AGE_SLOP_MAX, int(geom_age_ms / 2))
        elif ac._vrate_baro_fpm is not None:
            vrate = ac._vrate_baro_fpm
            slop = min(_ALT_RATE_AGE_SLOP_MAX, int(baro_age_ms / 2))
        if vrate is not None:
            min_fpm = vrate - _ALT_RATE_TOLERANCE_FPM - slop
            max_fpm = vrate + _ALT_RATE_TOLERANCE_FPM + slop

    # Layer 6: accept/reject decision (four paths)
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

    # Layer 7: accept path — score increment
    if accept:
        if reset_reliable:
            ac.alt_reliable = 0
        # MODE_S anti-inflate: infrequent alt on MLAT target should not build confidence
        if source == MsgSource.MODE_S and ac.mlat and baro_age_s > 5.0:
            score_add = -1
        else:
            score_add = good_crc + 1
        ac.alt_reliable = max(0, min(_ALT_RELIABLE_MAX, ac.alt_reliable + score_add))
        ac.altitude = alt
        ac._alt_source = source
        ac._alt_ts = now
        ac.last_alt_ts = now  # kept for snapshot last_alt_age / coverage gate
        if ac.max_altitude is None or alt > ac.max_altitude:
            ac.max_altitude = alt
        return True

    # Layer 8: discard path — cumulative penalty
    _penalise_alt_reliable(ac, good_crc)
    return False

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


def _update_range_bearing(ac: "Aircraft") -> None:
    """Recompute range_nm and bearing_deg from ac.lat/lon and receiver coords."""
    if config.RECEIVER_LAT is not None and config.RECEIVER_LON is not None:
        ac.range_nm   = round(_haversine_nm(config.RECEIVER_LAT, config.RECEIVER_LON, ac.lat, ac.lon), 1)
        ac.bearing_deg = round(_bearing_deg(config.RECEIVER_LAT, config.RECEIVER_LON, ac.lat, ac.lon), 1)


def _is_cpr_duplicate(ac: "Aircraft", raw: str, oe: int, now: float) -> bool:
    """Return True if an identical CPR frame has been seen within _CPR_DUP_WINDOW_S.

    Same raw bytes means the same transponder encoding was received more than once
    (e.g. reflected signal or two nearby feeders forwarding the same frame).
    Duplicate frames should not update CPR pairing or pos_reliable (readsb:
    cpr_duplicate_check in track.c).
    """
    cutoff = now - _CPR_DUP_WINDOW_S
    fresh = [(r, o, ts) for r, o, ts in ac._cpr_recent if ts >= cutoff]
    for r, o, _ in fresh:
        if r == raw and o == oe:
            ac._cpr_recent = fresh
            return True
    fresh.append((raw, oe, now))
    if len(fresh) > _CPR_DUP_CACHE_LEN:
        fresh.pop(0)
    ac._cpr_recent = fresh
    return False


def _pos_reliable(ac: "Aircraft") -> bool:
    """Mirror of readsb posReliable() in track.h.

    MLAT positions bypass the CPR reliability threshold — they are validated
    by the MLAT network geometry, not by CPR pair confirmation.
    """
    if ac.lat is None:
        return False
    if ac.mlat:
        return True
    return (ac.pos_reliable_odd  >= _POS_RELIABLE_PUBLISH and
            ac.pos_reliable_even >= _POS_RELIABLE_PUBLISH)


def _in_discard_cache(ac: "Aircraft", lat: float, lon: float, now: float) -> bool:
    return any(
        lt == lat and ln == lon and now - ts < _DISCARD_CACHE_TTL
        for lt, ln, ts in ac._discard_cache
    )


def _add_to_discard_cache(ac: "Aircraft", lat: float, lon: float, now: float) -> None:
    ac._discard_cache.append((lat, lon, now))
    if len(ac._discard_cache) > _DISCARD_CACHE_LEN:
        ac._discard_cache.pop(0)


def _accept_adsb_position(ac: "Aircraft", lat: float, lon: float,
                          pos_from_global: bool, cpr_odd: bool, now: float) -> None:
    """Write a validated ADS-B CPR position to the aircraft record.

    Applies a speed-check gate (readsb track.c speed_check) and maintains a
    soft reliability score (pos_reliable_odd/even).  A failed check penalises
    the score; when score reaches zero the CPR state is reset so the aircraft
    must re-establish position from a fresh even+odd pair.  lat/lon are still
    stored (needed as reference for local CPR decode) but suppressed in snapshot
    output until pos_reliable reaches _POS_RELIABLE_PUBLISH.
    """
    if ac.lat is not None and ac.lon is not None and ac.last_pos_ts > 0:
        elapsed_s = now - ac.last_pos_ts
        if elapsed_s > 0:
            dist_nm = _haversine_nm(ac.lat, ac.lon, lat, lon)
            # Ceiling widens with time to account for position uncertainty (+3 kt/s)
            ceiling_kt = _ADSB_MAX_SPEED_KT + _SPEED_UNCERTAINTY_KT_PER_S * elapsed_s
            implied_kt = (dist_nm / elapsed_s) * 3600
            if implied_kt > ceiling_kt:
                if not _in_discard_cache(ac, lat, lon, now):
                    ac.pos_reliable_odd  = max(0.0, ac.pos_reliable_odd  - _POS_RELIABLE_DECAY)
                    ac.pos_reliable_even = max(0.0, ac.pos_reliable_even - _POS_RELIABLE_DECAY)
                    if ac.pos_reliable_odd < _POS_RELIABLE_DECAY or ac.pos_reliable_even < _POS_RELIABLE_DECAY:
                        # Sustained failures: reset CPR state
                        ac.pos_reliable_odd  = 0.0
                        ac.pos_reliable_even = 0.0
                        ac.cpr_even  = None
                        ac.cpr_odd   = None
                        ac.pos_global = False
                    _add_to_discard_cache(ac, lat, lon, now)
                return  # discard this position

    # Position accepted — increment reliability score for the frame that triggered decode
    if cpr_odd:
        ac.pos_reliable_odd  = min(ac.pos_reliable_odd  + 1.0, _POS_RELIABLE_MAX)
    else:
        ac.pos_reliable_even = min(ac.pos_reliable_even + 1.0, _POS_RELIABLE_MAX)
    ac.lat = round(lat, 5)
    ac.lon = round(lon, 5)
    ac.last_pos_ts = now
    if pos_from_global:
        ac.pos_global = True
    _update_range_bearing(ac)


def _fuse_ecef(candidates: list[tuple[float, float, float]]) -> tuple[float, float]:
    """ECEF weighted centroid of (lat_deg, lon_deg, weight) candidates.

    Correct for any lat/lon separation; at MLAT disagreement distances (< 10 nm)
    the result is nearly identical to a simple weighted average of lat/lon, but
    the ECEF approach is correct by construction and costs negligible extra compute.
    """
    total_w = sum(w for _, _, w in candidates)
    fx = fy = fz = 0.0
    for lat, lon, w in candidates:
        rlat, rlon = math.radians(lat), math.radians(lon)
        fx += math.cos(rlat) * math.cos(rlon) * w / total_w
        fy += math.cos(rlat) * math.sin(rlon) * w / total_w
        fz += math.sin(rlat)                  * w / total_w
    r = math.sqrt(fx * fx + fy * fy + fz * fz)
    return math.degrees(math.asin(fz / r)), math.degrees(math.atan2(fy, fx))


def _compute_cross_source_residuals(ac: "Aircraft", source: str, lat: float, lon: float, now: float) -> None:
    """Record inter-network position residuals against all other sources with recent fixes.

    Called only for non-spiked fixes so residual distributions reflect real quality,
    not position noise from bad fixes.  Both the new source and each counterpart
    share the residual symmetrically.
    """
    for src, buf in ac.mlat_fixes.items():
        if src == source or not buf:
            continue
        other = buf[-1]
        if now - other.ts > _MLAT_ALIGN_WINDOW:
            continue   # too stale for a meaningful comparison
        residual_nm = _haversine_nm(lat, lon, other.lat, other.lon)
        for s in (source, src):
            if s not in ac.mlat_residuals:
                ac.mlat_residuals[s] = deque(maxlen=_MLAT_RESIDUAL_MAXLEN)
            ac.mlat_residuals[s].append(residual_nm)


def _update_quality_score(ac: "Aircraft", source: str) -> None:
    """Recompute quality score combining spike rate (60%) and median inter-source residual (40%).

    When fewer than 3 residual samples are available the score is spike-rate-only,
    giving each source a fair chance before cross-source data accumulates.
    """
    total = ac.mlat_fix_counts[source] + sum(ac.mlat_spike_counts[source].values())
    spike_score = ac.mlat_fix_counts[source] / total if total > 0 else 1.0

    residuals = ac.mlat_residuals.get(source)
    if residuals and len(residuals) >= 3:
        median_nm = sorted(residuals)[len(residuals) // 2]
        # 0 nm → 1.0, 0.5 nm → 0.67, 1 nm → 0.50, 2 nm → 0.33
        residual_score = 1.0 / (1.0 + median_nm)
        quality = 0.6 * spike_score + 0.4 * residual_score
    else:
        quality = spike_score

    ac.mlat_quality_scores[source] = round(quality, 3)


def _select_output_position(
    ac: "Aircraft", cur_lat: float, cur_lon: float, is_spike: bool, now: float
) -> tuple[float, float] | None:
    """Choose the output position under the active MLAT_FUSION mode.

    Returns (lat, lon) to write to ac.lat/ac.lon, or None to skip the write.

    spike_filter — reject spiked fixes; clean fixes written as-is.
    weighted     — ECEF weighted centroid of all sources with recent buffered fixes;
                   falls back to current fix if no other source has a recent fix.
    """
    fusion = config.MLAT_FUSION

    if fusion == "spike_filter":
        return None if is_spike else (cur_lat, cur_lon)

    if fusion == "weighted":
        candidates: list[tuple[float, float, float]] = []
        for src, buf in ac.mlat_fixes.items():
            if not buf:
                continue
            fix = buf[-1]
            if now - fix.ts > _MLAT_FUSION_WINDOW:
                continue
            w = ac.mlat_quality_scores.get(src, 1.0)
            if w > 0:
                candidates.append((fix.lat, fix.lon, w))
        if len(candidates) >= 2:
            return _fuse_ecef(candidates)
        if candidates:
            return candidates[0][0], candidates[0][1]
        # No buffered fixes yet — use current fix if it is not a spike
        return None if is_spike else (cur_lat, cur_lon)

    return None


def _record_mlat_fix(ac: "Aircraft", source: str, lat: float, lon: float, now: float) -> None:
    """Buffer an MLAT fix, run spike detection, and write position.

    Phase A (MLAT_FUSION=none): last-write-wins, no behaviour change.
    Phase B (spike_filter / weighted): position write is gated on quality.
    """
    # Initialise per-source state on first fix from this source
    if source not in ac.mlat_fixes:
        ac.mlat_fixes[source]          = deque(maxlen=_MLAT_FIX_MAXLEN)
        ac.mlat_spike_counts[source]   = {}
        ac.mlat_fix_counts[source]     = 0
        ac.mlat_quality_scores[source] = 1.0

    buf      = ac.mlat_fixes[source]
    prev_ts  = ac.mlat_last_fix_ts.get(source)
    is_spike = False

    if prev_ts is not None and buf:
        if now <= prev_ts:
            # Non-monotonic timestamp
            ac.mlat_spike_counts[source]["nonmonotonic"] = (
                ac.mlat_spike_counts[source].get("nonmonotonic", 0) + 1
            )
            is_spike = True
        else:
            dt = now - prev_ts
            if dt < _MLAT_MIN_DT_S:
                # Gap too small for a reliable speed estimate — skip check, don't count as spike
                pass
            else:
                prev = buf[-1]
                speed_kt = (_haversine_nm(prev.lat, prev.lon, lat, lon) / dt) * 3600
                if speed_kt > _MLAT_MAX_SPEED_KT:
                    ac.mlat_spike_counts[source]["speed"] = (
                        ac.mlat_spike_counts[source].get("speed", 0) + 1
                    )
                    is_spike = True

    if not is_spike:
        buf.append(MlatFix(ts=now, lat=lat, lon=lon))
        ac.mlat_fix_counts[source] += 1

    # Update rolling quality score: fraction of non-spiked attempted fixes
    total_attempts = ac.mlat_fix_counts[source] + sum(ac.mlat_spike_counts[source].values())
    if total_attempts > 0:
        ac.mlat_quality_scores[source] = round(ac.mlat_fix_counts[source] / total_attempts, 3)

    ac.mlat_last_fix_ts[source] = now

    # Cross-source residuals feed the scorecard regardless of fusion mode.
    if not is_spike:
        _compute_cross_source_residuals(ac, source, lat, lon, now)
    _update_quality_score(ac, source)

    # Evict sources that have been silent for too long to prevent dict growth
    stale = [s for s, ts in ac.mlat_last_fix_ts.items()
             if now - ts > _MLAT_STALE_EVICT_S and s != source]
    for s in stale:
        ac.mlat_fixes.pop(s, None)
        ac.mlat_spike_counts.pop(s, None)
        ac.mlat_fix_counts.pop(s, None)
        ac.mlat_quality_scores.pop(s, None)
        ac.mlat_last_fix_ts.pop(s, None)
        ac.mlat_residuals.pop(s, None)

    # Position write — gated by MLAT_FUSION mode
    if config.MLAT_FUSION == "none":
        # Phase A behaviour: always write, last-write-wins
        ac.lat = round(lat, 5)
        ac.lon = round(lon, 5)
        ac.last_pos_ts = now
        _update_range_bearing(ac)
    elif config.MLAT_FUSION == "kalman":
        if not is_spike:
            quality = ac.mlat_quality_scores.get(source, 1.0)
            ks = ac.kalman_state
            if ks is None or (now - ks["ts"]) > 60.0:
                # First fix or aircraft was silent > 60 s — (re-)initialise
                _kalman_init(ac, lat, lon, now)
                ac.lat, ac.lon = round(lat, 5), round(lon, 5)
            else:
                lat_k, lon_k = _kalman_update_position(ac, lat, lon, now, quality)
                ac.lat, ac.lon = round(lat_k, 5), round(lon_k, 5)
            ac.last_pos_ts = now
            _update_range_bearing(ac)
    else:
        pos = _select_output_position(ac, lat, lon, is_spike, now)
        if pos is not None:
            ac.lat = round(pos[0], 5)
            ac.lon = round(pos[1], 5)
            ac.last_pos_ts = now
            _update_range_bearing(ac)


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


@dataclass(slots=True)
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
    # Soft position reliability score (readsb: pos_reliable_odd/even in track.c).
    # Incremented by +1.0 on each accepted position; penalised by -0.26 on speed-check
    # failure; CPR state reset when either falls to zero.  lat/lon suppressed in snapshot
    # output until score reaches _POS_RELIABLE_PUBLISH (avoids wrong-zone first-position errors).
    pos_reliable_odd:    float = 0.0
    pos_reliable_even:   float = 0.0
    # Discard cache: prevents double-penalising the same bad position on retransmit
    _discard_cache:      list  = field(default_factory=list, repr=False)
    # Rolling cache of recent CPR raw frames for duplicate detection.
    # Entries: (raw_hex, oe_flag, timestamp).  Bounded by _CPR_DUP_CACHE_LEN.
    _cpr_recent:         list  = field(default_factory=list, repr=False)
    # mlatForce: timestamp of last forced MLAT position accept (ADS-B→MLAT transition)
    _last_mlat_force_ts: float = field(default=0.0, repr=False)
    sighting_count: int = 1               # from aircraft_registry; 1 = unique (never seen before)
    max_altitude: Optional[int] = None    # highest altitude seen this visit
    # MLAT
    has_adsb: bool = False               # True once a DF17 WITHOUT the MLAT timestamp is seen
                                         # (guarantees a real ADS-B transponder; blocks MLAT tag)
    mlat: bool = False                    # True if position is MLAT-derived (timestamp-confirmed)
    mlat_source: Optional[str] = None    # name of the MLAT server that established the position
    mlat_msg_count: int = 0              # Beast frames with the MLAT timestamp marker
    last_pos_ts: float = 0.0             # unix ts of last accepted position update (snapshot)
    last_alt_ts: float = 0.0             # unix ts of last accepted altitude update (snapshot)
    # Altitude reliability score (readsb: alt_reliable / updateAltitude())
    alt_reliable:    int   = field(default=0,   repr=False)  # 0..ALT_RELIABLE_MAX
    _alt_ts:         float = field(default=0.0, repr=False)  # time of last baro_alt write
    _alt_source:     Optional["MsgSource"] = field(default=None, repr=False)
    # Vertical rate fields for the dynamic rate window (Layer 5 of altitude filter)
    _vrate_baro_fpm: Optional[int]   = field(default=None, repr=False)
    _vrate_geom_fpm: Optional[int]   = field(default=None, repr=False)
    _vrate_baro_ts:  float           = field(default=0.0,  repr=False)
    _vrate_geom_ts:  float           = field(default=0.0,  repr=False)
    # ACAS/TCAS fields
    acas_ra_active:     bool           = False
    acas_ra_desc:       Optional[str]  = None
    acas_ra_corrective: bool           = False
    acas_sensitivity:   Optional[int]  = None
    acas_threat_icao:   Optional[str]  = None
    acas_ra_ts:         Optional[float]= None
    # MLAT per-source quality tracking — populated by _record_mlat_fix()
    # source → deque[MlatFix] (rolling fix buffer per network)
    mlat_fixes:          dict = field(default_factory=dict)
    # source → {reason: count} — nonmonotonic / speed spike counters
    mlat_spike_counts:   dict = field(default_factory=dict)
    # source → int — count of non-spiked (accepted) fixes
    mlat_fix_counts:     dict = field(default_factory=dict)
    # source → float [0.0–1.0] — rolling quality score (good fixes / total)
    mlat_quality_scores: dict = field(default_factory=dict)
    # source → float — timestamp of most recent fix (for stale eviction)
    mlat_last_fix_ts:    dict = field(default_factory=dict)
    # source → deque[float] — rolling inter-network position residuals in nm (Phase B)
    mlat_residuals:      dict = field(default_factory=dict)
    # Kalman filter state — populated on first MLAT fix when MLAT_FUSION=kalman
    # dict: {x, P, ref_lat, ref_lon, ts}  or None before first fix
    kalman_state:        Optional[dict] = None


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

        # sighting_count seed from DB (icao -> count); bounded LRU so long-tail
        # historical ICAOs are evicted rather than accumulating indefinitely
        self._sighting_counts: OrderedDict[str, int] = OrderedDict()

        # Confirmed-ICAO filter (equivalent to readsb icao_filter.c).
        # Populated from DF17/18 (clean CRC, authoritative ICAO).
        # Address/Parity DFs (0/4/5/16/20/21) are only decoded if the ICAO
        # appears here — prevents ghost aircraft from CRC-syndrome collisions.
        # Value is the last-seen timestamp for expiry pruning.
        self._confirmed_icaos: OrderedDict[str, float] = OrderedDict()

        # History deque cache for get_snapshot() — the completed-minute deques only
        # change once per minute, so we rebuild the list copies at most 1×/min instead
        # of 60×/min (once per second).
        self._snapshot_history_minute: int = -1
        self._snapshot_history_cache: tuple | None = None  # (rate_list, df_list, mlat_list)

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

    def expire_aircraft(self) -> list["Aircraft"]:
        """Remove stale aircraft and return them for visit logging."""
        now = time.time()
        expired = []
        with self._lock:
            stale = [icao for icao, ac in self._aircraft.items()
                     if now - ac.last_seen > self._timeout]
            for icao in stale:
                expired.append(self._aircraft.pop(icao))

            # Prune ICAO filter: evict entries not refreshed within expiry window.
            # Mirrors readsb icaoFilterExpire() which runs every ~60 s.
            icao_cutoff = now - _ICAO_FILTER_EXPIRY_S
            stale_icaos = [k for k, ts in self._confirmed_icaos.items()
                           if ts < icao_cutoff]
            for k in stale_icaos:
                del self._confirmed_icaos[k]

            # 60-minute pos_reliable timeout: aircraft silent for an hour gets
            # reliability reset so the next position must re-establish trust.
            for ac in self._aircraft.values():
                if (ac.pos_reliable_odd != 0.0 or ac.pos_reliable_even != 0.0):
                    if ac.last_pos_ts > 0 and (now - ac.last_pos_ts) > _POS_RELIABLE_RESET_TIMEOUT_S:
                        ac.pos_reliable_odd  = 0.0
                        ac.pos_reliable_even = 0.0
        return expired

    def init_today(self, icaos: set[str], mil_icaos: set[str]) -> None:
        """Seed today's unique-aircraft sets from DB on startup (persistence across restarts)."""
        with self._lock:
            self._today_icaos = set(icaos)
            self._today_mil_icaos = set(mil_icaos)

    def seed_sighting_counts(self, counts: dict[str, int]) -> None:
        """Seed sighting_count from DB so new Aircraft objects get correct values."""
        with self._lock:
            self._sighting_counts = OrderedDict(counts)

    def seed_hexdb_queue(self, icaos: list[str]) -> None:
        """Add a batch of ICAOs to the hexdb re-enrichment queue."""
        with self._lock:
            for icao in icaos:
                if len(self._hexdb_queue) >= _HEXDB_QUEUE_MAX:
                    break
                self._hexdb_queue.add(icao)

    def update_sighting_counts(self, counts: dict[str, int]) -> None:
        """Refresh in-memory sighting_count for live aircraft after a DB write."""
        with self._lock:
            for icao, count in counts.items():
                ac = self._aircraft.get(icao)
                if ac:
                    ac.sighting_count = count
                self._sighting_counts[icao] = count
                self._sighting_counts.move_to_end(icao)
                while len(self._sighting_counts) > _SIGHTING_LRU_MAX:
                    self._sighting_counts.popitem(last=False)

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
                "altitude":      ac.altitude if _alt_baro_reliable(ac) else None,
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
                "pos_global":        ac.pos_global,
                "pos_reliable_odd":  ac.pos_reliable_odd,
                "pos_reliable_even": ac.pos_reliable_even,
                "pos_confident":     _pos_reliable(ac),
                "last_pos_age":    round(now - ac.last_pos_ts, 1) if ac.last_pos_ts > 0 else None,
                "last_alt_age":    round(now - ac.last_alt_ts, 1) if ac.last_alt_ts > 0 else None,
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
                if len(self._hexdb_queue) < _HEXDB_QUEUE_MAX:
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
            # Single pass: collect all per-aircraft counters and the aircraft list.
            # Replaces four separate O(N) scans that previously ran inside the lock.
            cur_mil = cur_with_pos = cur_mlat_pos = mlat_aircraft_count = 0
            for ac in self._aircraft.values():
                if ac.military:
                    cur_mil += 1
                if ac.lat is not None:
                    cur_with_pos += 1
                if ac.mlat:
                    mlat_aircraft_count += 1
                    if ac.lat is not None:
                        cur_mlat_pos += 1
            live_military = cur_mil

            cur_sigs = self._cur_min_signals
            cur_sig_avg = round(sum(cur_sigs) / len(cur_sigs), 1) if cur_sigs else None
            cur_stats = (self._cur_min, cur_mn, cur_mx, cur_me,
                         cur_total, cur_total - cur_mil, cur_mil,
                         cur_sig_avg, min(cur_sigs) if cur_sigs else None,
                         max(cur_sigs) if cur_sigs else None,
                         cur_with_pos, cur_mlat_pos)

            # Rebuild history list copies only when the minute rolls over.
            # At 1 Hz this avoids 3 deque→list copies per second (59 of 60 are free).
            if self._snapshot_history_minute != self._cur_min or self._snapshot_history_cache is None:
                self._snapshot_history_cache = (
                    list(self._min_stats),
                    list(self._min_df_stats),
                    list(self._min_mlat_counts),
                )
                self._snapshot_history_minute = self._cur_min
            hist_min_stats, hist_df_stats, hist_mlat_counts = self._snapshot_history_cache
            rate_history = hist_min_stats + [cur_stats]
            df_history = hist_df_stats + [(self._cur_min, dict(self._cur_min_df_counts))]
            mlat_history = hist_mlat_counts + [(self._cur_min, self._cur_min_mlat_count)]

            aircraft_list = [
                {
                    "icao": ac.icao,
                    "callsign": ac.callsign,
                    "altitude": ac.altitude if _alt_baro_reliable(ac) else None,
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
                    "last_pos_age":      round(now - ac.last_pos_ts, 1) if ac.last_pos_ts > 0 else None,
                    "last_alt_age":      round(now - ac.last_alt_ts, 1) if ac.last_alt_ts > 0 else None,
                    "mlat_quality":      dict(ac.mlat_quality_scores),
                    "mlat_sources": {
                        src: {
                            "fixes":           len(buf),
                            "spikes":          sum(ac.mlat_spike_counts.get(src, {}).values()),
                            "spike_detail":    dict(ac.mlat_spike_counts.get(src, {})),
                            "median_residual": (
                                round(sorted(ac.mlat_residuals[src])[len(ac.mlat_residuals[src]) // 2], 3)
                                if src in ac.mlat_residuals and len(ac.mlat_residuals[src]) >= 3
                                else None
                            ),
                        }
                        for src, buf in ac.mlat_fixes.items()
                    },
                    "acas_ra_active":     ac.acas_ra_ts is not None and (now - ac.acas_ra_ts) < 60,
                    "acas_ra_desc":       ac.acas_ra_desc,
                    "acas_ra_corrective": ac.acas_ra_corrective,
                    "acas_threat_icao":   ac.acas_threat_icao,
                    "acas_sensitivity":   ac.acas_sensitivity,
                    "pos_global":         ac.pos_global,
                    "pos_reliable_odd":   ac.pos_reliable_odd,
                    "pos_reliable_even":  ac.pos_reliable_even,
                    "pos_confident":      _pos_reliable(ac),
                }
                for ac in self._aircraft.values()
            ]

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

        # --- CRC check + ICAO extraction + source classification ---
        icao: Optional[str] = None
        crc_clean = False
        source = MsgSource.INVALID
        try:
            if df in (17, 18):
                # DF17/18: true CRC covers entire message; residual 0 = clean.
                # pyModeS crc() returns the 24-bit remainder on the original bytes.
                crc_residual = pms.crc(raw)
                crc_clean = (crc_residual == 0)
                icao = pms.icao(raw)
                if icao:
                    icao_up = icao.upper()
                    # Register as confirmed ICAO (readsb: icaoFilterAdd)
                    self._confirmed_icaos[icao_up] = now
                    self._confirmed_icaos.move_to_end(icao_up)
                    if len(self._confirmed_icaos) > _CONFIRMED_ICAO_MAX:
                        self._confirmed_icaos.popitem(last=False)
                    source = MsgSource.ADSR if df == 18 else MsgSource.ADSB
            elif df == 11:
                icao = pms.icao(raw)
                # DF11 CRC includes Interrogator ID in lower 7 bits — less
                # reliable for ICAO confirmation; do NOT add to confirmed set.
                source = MsgSource.MODE_S_CHECKED
            elif df in _AP_DFS:
                # Address/Parity: CRC syndrome IS the ICAO address.
                # Only accept if previously confirmed by a DF17/18.
                icao = pms.icao(raw)
                if not icao or icao.upper() not in self._confirmed_icaos:
                    return
                # Refresh LRU timestamp
                icao_up = icao.upper()
                self._confirmed_icaos[icao_up] = now
                self._confirmed_icaos.move_to_end(icao_up)
                source = MsgSource.MODE_S
        except Exception:
            pass

        if not icao:
            return
        icao = icao.upper()

        # MLAT-timestamp frame overrides source classification
        if mlat:
            source = MsgSource.MLAT

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
                        _accept_altitude(ac, int(alt), source, crc_clean, now)
                except Exception:
                    pass
                try:
                    oe = pms.adsb.oe_flag(raw)

                    # Duplicate check: same raw frame within _CPR_DUP_WINDOW_S means
                    # multiple receivers forwarded the same transponder transmission.
                    # Skip CPR pairing and pos_reliable update to avoid inflating
                    # confidence at multi-feeder/reflection sites (readsb:
                    # cpr_duplicate_check in track.c).
                    if not _is_cpr_duplicate(ac, raw, oe, now):
                        if oe == 0:
                            ac.cpr_even = (raw, now)
                        else:
                            ac.cpr_odd = (raw, now)

                        # Global decode is always preferred: geometrically unambiguous.
                        # Local decode can pick the wrong CPR longitude zone when the
                        # receiver is west of the aircraft (e.g. UK receiver + aircraft
                        # over the North Sea).
                        pos = None
                        pos_from_global = False
                        global_bad = False
                        if (ac.cpr_even and ac.cpr_odd
                                and abs(ac.cpr_even[1] - ac.cpr_odd[1]) < 10):
                            pos = pms.adsb.position(
                                ac.cpr_even[0], ac.cpr_odd[0],
                                ac.cpr_even[1], ac.cpr_odd[1],
                            )
                            if pos is not None:
                                pos_from_global = True
                            elif ac.pos_global:
                                # Established aircraft + None from global most likely
                                # means bad data (-2), not zone mismatch (-1).
                                # Block local fallback to avoid wrong-zone write.
                                global_bad = True

                        # Local decode fallback: only when global was not attempted
                        # or failed with a likely zone mismatch (not bad data).
                        if not pos_from_global and not global_bad:
                            ref_lat = ac.lat if ac.lat is not None else config.RECEIVER_LAT
                            ref_lon = ac.lon if ac.lon is not None else config.RECEIVER_LON
                            if ref_lat is not None and ref_lon is not None:
                                pos = pms.adsb.position_with_ref(raw, ref_lat, ref_lon)

                        if pos:
                            lat, lon = pos
                            if -90 <= lat <= 90 and -180 <= lon <= 180:
                                # Range gate: reject positions beyond receiver range
                                if (config.RECEIVER_LAT is not None
                                        and config.RECEIVER_LON is not None
                                        and _haversine_nm(
                                            config.RECEIVER_LAT, config.RECEIVER_LON,
                                            lat, lon) > config.MAX_RANGE_NM):
                                    pass  # discard — beyond ADS-B range
                                elif mlat and not ac.has_adsb:
                                    # Genuine MLAT position: buffer, spike-detect, fuse.
                                    # MLAT force: if ADS-B position is stale and MLAT
                                    # fix is far away, force-accept to transition.
                                    if (ac.has_adsb
                                            and ac.lat is not None
                                            and now - ac._last_mlat_force_ts > _MLAT_FORCE_INTERVAL_S
                                            and _haversine_nm(ac.lat, ac.lon, lat, lon) > _MLAT_FORCE_DISTANCE_NM):
                                        ac._last_mlat_force_ts = now
                                        ac.pos_reliable_odd  = _POS_RELIABLE_PUBLISH
                                        ac.pos_reliable_even = _POS_RELIABLE_PUBLISH
                                    _record_mlat_fix(ac, mlat_source or "mlat", lat, lon, now)
                                elif not mlat:
                                    _accept_adsb_position(ac, lat, lon, pos_from_global, oe == 1, now)
                except Exception:
                    pass

        # --- Altitude from surveillance altitude reply (DF 4) ---
        # Lower integrity than ADS-B: parity is XOR-masked with aircraft address.
        # Suppressed for MLAT frames: mlat-client copies baro alt from a secondary
        # reply on a different receiver — unreliable and potentially from a
        # different aircraft (readsb track.c: "terrible altitude source, ignore").
        elif df == 4 and len(raw) == 14:
            if not mlat:
                try:
                    alt = pms.altcode(raw)
                    if alt is not None:
                        _accept_altitude(ac, int(alt), MsgSource.MODE_S, False, now)
                except Exception:
                    pass

        # --- DF 20/21: Comm-B replies (28 hex chars / 112 bits) ---
        elif df in (20, 21) and len(raw) == 28:
            # Altitude from DF20 — same parity integrity as DF4; same MLAT suppression.
            if df == 20 and not mlat:
                try:
                    alt = pms.altcode(raw)
                    if alt is not None:
                        _accept_altitude(ac, int(alt), MsgSource.MODE_S, False, now)
                except Exception:
                    pass

            # EHS decode: direct is40/is50/is60 checks — ~5x faster than pms.bds.infer()
            # which speculatively tries ~20 registers. Checks are mutually exclusive in practice.
            if _bds40.is40(raw):
                # Selected altitude (autopilot target) — altitude field; suppress for MLAT.
                if not mlat:
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
                if not mlat:  # baro vertical rate is altitude-derived; suppress for MLAT
                    try:
                        vr = pms.commb.vr60baro(raw)
                        if vr is not None:
                            ac.vertical_rate_fpm = int(round(vr))
                            # Feed altitude filter dynamic rate window
                            ac._vrate_baro_fpm = int(round(vr))
                            ac._vrate_baro_ts  = now
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
