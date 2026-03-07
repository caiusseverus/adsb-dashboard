"""
ACAS/TCAS Resolution Advisory decoder and API router.

Decodes BDS 3.0 (ACAS RA) data from DF16 (Long Air-Air Surveillance) messages.
Ground receivers capture these opportunistically; events are a lower bound on
actual TCAS activations in the coverage area.

IMPORTANT — bit offset difference between DF16 and Comm-B:
  Comm-B MB field (DF20/21): first byte is BDS register code (0x30),
    so ACAS data begins at bit 8 of the MB field.
  DF16 MV field: NO register header — ACAS data begins at bit 0.
  Using the wrong offset (8 instead of 0) causes ~50% false-positive rate
  because the ARA-active check fires on a random bit.
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
    n = int(hex_str, 16)
    return bin(n)[2:].zfill(len(hex_str) * 4)


# ---------------------------------------------------------------------------
# Decoders
# ---------------------------------------------------------------------------

def decode_df0_sensitivity(msg: str) -> int | None:
    """DF0 (14-char hex). Extract Sensitivity Level (SL) = bits 6-8, values 0-7."""
    if len(msg) != 14:
        return None
    try:
        return int(_hex2bin(msg)[5:8], 2)
    except Exception:
        return None


def _decode_acas_mv(mv_bin: str, sensitivity_level: int | None = None) -> dict | None:
    """Decode a 56-bit ACAS MV field (DF16) where data starts at bit 0.

    DF16 MV field layout (ICAO Annex 10, no register-byte header):
      [0:14]  ARA — Active Resolution Advisory (14 bits)
                bit 0: RA active
                bit 1: 0=corrective, 1=preventive
                bits 2-5: corrective sense (up, down, crossing-up, crossing-down)
                bit 6: reversal / sense change
      [14:18] RAC — RA Complement (4 bits)
      [18]    RAT — RA Terminated
      [19]    MTE — Multiple Threat Encounter
      [20:22] TTI — Threat Type Indicator (01=Mode-S ICAO, 10=non-Mode-S)
      [22:46] Threat Identity (24 bits)
    """
    if len(mv_bin) < 46:
        return None

    # Bit 0: RA active indicator — must be 1 for a real RA
    if mv_bin[0] == '0':
        return None

    # SL=0 means ACAS is in standby / not operational; genuine RAs can't fire
    if sensitivity_level is not None and sensitivity_level == 0:
        return None

    ara_bits = mv_bin[0:14]
    rac_bits = mv_bin[14:18]
    rat      = mv_bin[18] == '1'
    mte      = mv_bin[19] == '1'
    tti      = int(mv_bin[20:22], 2)

    # TTI=3 is reserved/invalid
    if tti == 3:
        return None

    threat_bits = mv_bin[22:46]

    # --- ARA sense ---
    ra_corrective = mv_bin[1] == '0'   # bit 1: 0=corrective, 1=preventive
    reversal      = mv_bin[6] == '1'   # bit 6: sense change / reversal

    # --- RAC ---
    rac_dont_pass_below = rac_bits[0] == '1'
    rac_dont_pass_above = rac_bits[1] == '1'
    rac_dont_turn_left  = rac_bits[2] == '1'
    rac_dont_turn_right = rac_bits[3] == '1'

    # --- Corrective sense bits (bits 2-5 of ARA) ---
    if ra_corrective:
        sense_bits = mv_bin[2:6]
        if   sense_bits == '1000': ra_sense = "Climb"
        elif sense_bits == '0100': ra_sense = "Descend"
        elif sense_bits == '0010': ra_sense = "Climb — crossing"
        elif sense_bits == '0001': ra_sense = "Descend — crossing"
        else:
            upward   = mv_bin[2] == '1' or mv_bin[4] == '1'
            downward = mv_bin[3] == '1' or mv_bin[5] == '1'
            if upward and not downward:   ra_sense = "Climb"
            elif downward and not upward: ra_sense = "Descend"
            else:                         ra_sense = "Unknown"
        ra_description = ra_sense
        if reversal:
            ra_description += " — reversal"
    else:
        if   rac_dont_pass_below:                        ra_sense = "Do not pass below"
        elif rac_dont_pass_above:                        ra_sense = "Do not pass above"
        elif rac_dont_turn_left and rac_dont_turn_right: ra_sense = "Do not turn"
        elif rac_dont_turn_left:                         ra_sense = "Do not turn left"
        elif rac_dont_turn_right:                        ra_sense = "Do not turn right"
        else:                                            ra_sense = "Preventive"
        ra_description = ra_sense

    if mte:
        ra_description += " (MTE)"

    # --- Threat identity ---
    threat_icao = threat_alt = threat_range_nm = threat_bearing_deg = None

    if tti == 1:
        val = int(threat_bits[:24], 2)
        # Filter degenerate values (all-zeros or all-ones are not real ICAO addresses)
        if 0 < val < 0xFFFFFF:
            threat_icao = f"{val:06X}"
    elif tti == 2:
        threat_alt         = int(threat_bits[0:13], 2)
        threat_range_nm    = round(int(threat_bits[13:19], 2) * 0.1, 1)
        threat_bearing_deg = round(int(threat_bits[19:25], 2) * 360 / 64, 1)

    return {
        "ara_active":          True,
        "ra_corrective":       ra_corrective,
        "ra_sense":            ra_sense,
        "ra_description":      ra_description,
        "rac_dont_pass_below": rac_dont_pass_below,
        "rac_dont_pass_above": rac_dont_pass_above,
        "rac_dont_turn_left":  rac_dont_turn_left,
        "rac_dont_turn_right": rac_dont_turn_right,
        "rat":                 rat,
        "mte":                 mte,
        "tti":                 tti,
        "threat_icao":         threat_icao,
        "threat_alt":          threat_alt,
        "threat_range_nm":     threat_range_nm,
        "threat_bearing_deg":  threat_bearing_deg,
        "sensitivity_level":   sensitivity_level,
        "ara_bits":            ara_bits,
        "rac_bits":            rac_bits,
    }


def decode_df16_mv(msg: str) -> dict | None:
    """Decode DF16 (28-char hex). Extracts SL then decodes MV field as ACAS data."""
    if len(msg) != 28:
        return None
    try:
        bits = _hex2bin(msg)
        sensitivity_level = int(bits[5:8], 2)
        # MV field = hex chars 8-21 (56 bits, starting after the first 8 chars / 32 bits)
        mv_bin = _hex2bin(msg[8:22])
    except Exception:
        return None

    return _decode_acas_mv(mv_bin, sensitivity_level)


def decode_bds30(msg: str) -> dict | None:
    """Decode BDS 3.0 from a Comm-B message (DF20/21, 28-char hex).

    In Comm-B the MB field (hex chars 8-21) begins with a 1-byte BDS register
    code (0x30), so the actual ACAS data starts at bit 8 of the MB field.
    This is DIFFERENT from DF16 — do not use this for DF16 decoding.
    """
    if len(msg) != 28:
        return None
    try:
        mb_bin = _hex2bin(msg[8:22])  # 56-bit MB field
    except Exception:
        return None

    # First byte should be register 0x30 = 00110000
    if mb_bin[0:8] != '00110000':
        return None

    # ACAS data begins at bit 8 (after the register byte)
    return _decode_acas_mv(mb_bin[8:], sensitivity_level=None)


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

def _fmt_row(row: dict) -> dict:
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
