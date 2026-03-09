"""
ACAS/TCAS Resolution Advisory decoder and API router.

Decodes BDS 3.0 (ACAS RA) data from DF16 (Long Air-Air Surveillance) messages.
Ground receivers capture these opportunistically; events are a lower bound on
actual TCAS activations in the coverage area.

Bit layout matches readsb (wiedehopf/readsb json_out.c ~line 246).
All bit positions are 1-indexed from the start of the 7-byte (56-bit) MV field;
in 0-indexed Python terms subtract 1 (e.g. bit 9 → mv_bin[8]).

Both DF16 MV and Comm-B BDS 3.0 MB fields share the same layout:
  byte 0 (bits 1-8):  capability/register prefix — not used for ACAS data
  byte 1+ (bit 9 onwards): ACAS payload

  Bit 9  (mv_bin[8])  — ARA active: 1 = RA has been generated
  Bit 10 (mv_bin[9])  — Corrective: 1 = corrective, 0 = preventive
  Bit 11 (mv_bin[10]) — Downward sense: 1 = descend, 0 = climb
  Bit 12 (mv_bin[11]) — Increase rate
  Bit 13 (mv_bin[12]) — Sense reversal
  Bit 14 (mv_bin[13]) — Altitude crossing
  Bit 15 (mv_bin[14]) — Positive RA: 1 = positive action required
  Bits 23-26 (mv_bin[22:26]) — RAC (Resolution Advisory Complement)
  Bit 27 (mv_bin[26]) — RAT: RA terminated / Clear of Conflict
  Bit 28 (mv_bin[27]) — MTE: Multiple Threat Encounter
  Bits 29-30 (mv_bin[28:30]) — TTI: 01=Mode-S threat ICAO, 10=non-Mode-S
  Bits 31-54 (mv_bin[30:54]) — Threat identity (24 bits)
"""

import asyncio
import logging
from fastapi import APIRouter, Query

from db import stats_db
from utils import format_operator

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/acas")


# ---------------------------------------------------------------------------
# Bit extraction helpers
# ---------------------------------------------------------------------------

def _hex2bin(hex_str: str) -> str:
    """Convert hex string to zero-padded binary string."""
    if not hex_str or len(hex_str) % 2 != 0:
        raise ValueError(f"Invalid hex string length: {len(hex_str)!r}")
    n = int(hex_str, 16)
    return bin(n)[2:].zfill(len(hex_str) * 4)


# ---------------------------------------------------------------------------
# Core decoder — shared by DF16 and Comm-B BDS 3.0
# ---------------------------------------------------------------------------

def _decode_acas_mv(mv_bin: str, sensitivity_level: int | None = None) -> dict | None:
    """Decode a 56-bit ACAS MV/MB field.

    Expects the full 56-bit binary string including the leading capability/
    register byte (bits 1-8). ACAS data starts at bit 9 (mv_bin[8]).

    Returns None if:
      - No active RA (ARA bit = 0)
      - RAT = 1 (Clear of Conflict — RA terminated, not worth logging)
      - SL = 0 (ACAS in standby, cannot generate real RAs)
      - TTI = 3 (reserved, invalid)
    """
    if len(mv_bin) != 56:
        return None

    # SL=0 → ACAS standby, no valid RA possible
    if sensitivity_level is not None and sensitivity_level == 0:
        return None

    ara = mv_bin[8] == '1'   # bit 9: ARA active
    rat = mv_bin[26] == '1'  # bit 27: RA terminated / Clear of Conflict

    if rat or not ara:
        return None

    tti = int(mv_bin[28:30], 2)
    if tti == 3:              # reserved — invalid
        return None

    # --- Decode ARA sense bits (bits 10-15, mv_bin[9:15]) ---
    corr     = mv_bin[9]  == '1'  # bit 10: corrective (1) vs preventive (0)
    down     = mv_bin[10] == '1'  # bit 11: downward sense
    increase = mv_bin[11] == '1'  # bit 12: increase rate
    reversal = mv_bin[12] == '1'  # bit 13: sense reversal
    crossing = mv_bin[13] == '1'  # bit 14: altitude crossing
    positive = mv_bin[14] == '1'  # bit 15: positive RA (action required)

    mte      = mv_bin[27] == '1'  # bit 28: multiple threat encounter
    rac_bits = mv_bin[22:26]      # bits 23-26

    # --- Build RA sense and description (mirrors readsb logic) ---
    if corr and positive:
        direction = "Descend" if down else "Climb"
        parts = [direction]
        if crossing: parts.append("crossing")
        if increase: parts.append("increase rate")
        ra_sense = ", ".join(parts)
        ra_corrective = True
    elif corr:
        # Negative corrective: reduce vertical rate (down bit indicates advisory sense, not motion)
        ra_sense = "Reduce vertical rate"
        ra_corrective = True
    else:
        # Preventive — maintain current vertical speed
        ra_sense = "Preventive"
        ra_corrective = False

    ra_description = ra_sense
    if corr and reversal:
        ra_description += " — reversal"
    if mte:
        ra_description += " (MTE)"

    # --- RAC complement bits ---
    rac_dont_pass_below = rac_bits[0] == '1'
    rac_dont_pass_above = rac_bits[1] == '1'
    rac_dont_turn_left  = rac_bits[2] == '1'
    rac_dont_turn_right = rac_bits[3] == '1'

    # --- Threat identity (bits 31-54, mv_bin[30:54]) ---
    threat_icao = threat_alt = threat_range_nm = threat_bearing_deg = None
    threat_bits = mv_bin[30:54]

    if tti == 1:
        val = int(threat_bits, 2)
        if 0 < val < 0xFFFFFF:      # filter all-zeros / all-ones
            threat_icao = f"{val:06X}"
    elif tti == 2:
        # 24-bit non-Mode-S field: 12 bits altitude, 6 bits range, 6 bits bearing
        threat_alt         = int(threat_bits[0:12], 2)
        threat_range_nm    = round(int(threat_bits[12:18], 2) * 0.1, 1)
        threat_bearing_deg = round(int(threat_bits[18:24], 2) * 360 / 64, 1)

    return {
        "ara_active":          True,
        "ra_corrective":       ra_corrective,
        "ra_sense":            ra_sense,
        "ra_description":      ra_description,
        "rac_dont_pass_below": rac_dont_pass_below,
        "rac_dont_pass_above": rac_dont_pass_above,
        "rac_dont_turn_left":  rac_dont_turn_left,
        "rac_dont_turn_right": rac_dont_turn_right,
        "mte":                 mte,
        "tti":                 tti,
        "threat_icao":         threat_icao,
        "threat_alt":          threat_alt,
        "threat_range_nm":     threat_range_nm,
        "threat_bearing_deg":  threat_bearing_deg,
        "sensitivity_level":   sensitivity_level,
        "ara_bits":            mv_bin[8:22],   # bits 9-22 for debug
        "rac_bits":            rac_bits,
    }


# ---------------------------------------------------------------------------
# Public decoders
# ---------------------------------------------------------------------------

def decode_df16_mv(msg: str) -> dict | None:
    """Decode DF16 (28-char hex). Extract SL from header, then decode MV field."""
    if len(msg) != 28:
        return None
    try:
        bits = _hex2bin(msg)
        sensitivity_level = int(bits[5:8], 2)   # DF16 SL: bits 6-8
        mv_bin = _hex2bin(msg[8:22])             # 56-bit MV field
    except Exception:
        return None

    return _decode_acas_mv(mv_bin, sensitivity_level)


def decode_df0_sensitivity(msg: str) -> int | None:
    """DF0 (14-char hex). Extract Sensitivity Level (SL) = bits 6-8, values 0-7."""
    if len(msg) != 14:
        return None
    try:
        return int(_hex2bin(msg)[5:8], 2)
    except Exception:
        return None


def decode_bds30(msg: str) -> dict | None:
    """Decode BDS 3.0 from a Comm-B message (DF20/21, 28-char hex).

    MB field = hex chars 8-21. First byte is BDS register code 0x30,
    followed by ACAS data at bit 9 — same layout as DF16 MV field.
    """
    if len(msg) != 28:
        return None
    try:
        mb_bin = _hex2bin(msg[8:22])
    except Exception:
        return None

    if mb_bin[0:8] != '00110000':   # verify BDS register = 0x30
        return None

    return _decode_acas_mv(mb_bin, sensitivity_level=None)


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

def _fmt_row(row: dict) -> dict:
    row = dict(row)
    row["operator_display"]        = format_operator(row.get("operator"))
    row["threat_operator_display"] = format_operator(row.get("threat_operator"))
    return row


@router.get("/events")
async def get_acas_events(
    days:  int = Query(30, ge=1, le=365),
    limit: int = Query(200, ge=1, le=1000),
) -> list[dict]:
    rows = await asyncio.to_thread(stats_db.query_acas_events, days, limit)
    return [_fmt_row(r) for r in rows]


@router.get("/stats")
async def get_acas_stats(
    days: int = Query(30, ge=1, le=365),
) -> dict:
    return await asyncio.to_thread(stats_db.query_acas_stats, days)


@router.get("/timeline")
async def get_acas_timeline(
    days: int = Query(30, ge=1, le=365),
) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_acas_timeline, days)


@router.get("/context/{event_id}")
async def get_acas_context(event_id: int) -> dict:
    return await asyncio.to_thread(stats_db.query_acas_context, event_id)


@router.get("/aircraft/{icao}")
async def get_acas_for_icao(
    icao:  str,
    limit: int = Query(10, ge=1, le=100),
) -> list[dict]:
    rows = await asyncio.to_thread(stats_db.query_acas_for_icao, icao.upper(), limit)
    return [_fmt_row(r) for r in rows]
