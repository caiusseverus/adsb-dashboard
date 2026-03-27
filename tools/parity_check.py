#!/usr/bin/env python3
"""
T1 — C extension decode parity check.

Reads a captured Beast TCP binary file, decodes each Mode-S frame with
both the C extension (decode_cffi) and pyModeS, then reports any field
divergences.

Usage:
    # Capture a corpus first (60–120 seconds of traffic):
    nc <BEAST_HOST> 30005 > corpus.beast

    # Run from the backend directory so imports resolve:
    cd backend && uv run python ../tools/parity_check.py ../corpus.beast [--verbose]

Fields compared per DF type:
    DF4/5/20/21  → df, icao, crc_ok, altitude (DF4/20), squawk (DF5/21)
    DF11         → df, icao, crc_ok
    DF17/18      → df, icao, crc_ok, callsign (tc1-4), altitude (tc9-22),
                   cpr_odd/cpr_lat/cpr_lon (tc9-22), heading (tc19),
                   speed (tc19)
    DF0/16       → df, icao, crc_ok, acas_ra_valid
"""

import sys
import argparse
import struct
from pathlib import Path
from collections import defaultdict

# Add backend/ to path so decode_cffi and pyModeS resolve correctly.
# The script lives in tools/; backend/ is one level up.
_BACKEND = Path(__file__).resolve().parent.parent / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# ── Imports from backend ───────────────────────────────────────────────────────
try:
    import pyModeS as pms
    from pyModeS.decoder.bds import bds40 as _bds40
except ImportError:
    sys.exit("pyModeS not found — run from backend/ directory via: uv run python ...")

try:
    import decode_cffi
    decode_cffi._get_lib()   # eagerly load so errors surface immediately
    HAVE_C_LIB = True
except Exception as exc:
    sys.exit(f"decode_cffi not available: {exc}\nRun: make -C backend/native && make -C backend/native install")

# ── Beast frame constants ──────────────────────────────────────────────────────
_MSG_LEN  = {0x31: 2, 0x32: 7, 0x33: 14}
_SKIP_DFS = {0x31}    # Mode-AC: no ICAO, skip


# ── Beast corpus reader ────────────────────────────────────────────────────────

def _unescape(buf: bytearray, start: int, needed: int):
    """Return (bytes, end_pos) | (None, None) if short | (False, None) on framing error."""
    result = bytearray()
    pos = start
    while len(result) < needed:
        if pos >= len(buf):
            return None, None
        b = buf[pos]
        if b == 0x1A:
            if pos + 1 >= len(buf):
                return None, None
            if buf[pos + 1] == 0x1A:
                result.append(0x1A)
                pos += 2
            else:
                return False, None
        else:
            result.append(b)
            pos += 1
    return bytes(result), pos


def read_beast_frames(path: Path):
    """Yield (msg_type, timestamp, signal, raw_bytes) for each valid Mode-S frame."""
    buf = bytearray(path.read_bytes())
    while len(buf) >= 2:
        if buf[0] != 0x1A:
            idx = buf.find(0x1A)
            if idx == -1:
                break
            del buf[:idx]
            continue

        msg_type = buf[1]
        if msg_type not in _MSG_LEN:
            del buf[:1]
            continue

        needed = 6 + 1 + _MSG_LEN[msg_type]
        data, end = _unescape(buf, 2, needed)
        if data is None:
            break
        if data is False:
            del buf[:1]
            continue

        del buf[:end]
        if msg_type == 0x31:   # Mode-AC, skip
            continue

        timestamp = int.from_bytes(data[:6], "big")
        signal    = data[6]
        payload   = data[7:]
        yield msg_type, timestamp, signal, payload


# ── pyModeS decode (field extraction mirroring what aircraft_state uses) ──────

def _decode_pymodes(raw_hex: str) -> dict | None:
    """Return field dict from pyModeS, or None if rejected."""
    try:
        df = pms.df(raw_hex)
    except Exception:
        return None

    try:
        icao = pms.icao(raw_hex)
    except Exception:
        icao = None

    # CRC
    try:
        crc_ok = (pms.crc(raw_hex) == 0) if df in (17, 18, 11) else None
    except Exception:
        crc_ok = None

    out = {
        "df":     df,
        "icao":   icao.upper() if icao else None,
        "crc_ok": crc_ok,
    }

    try:
        if df in (17, 18):
            tc = pms.adsb.typecode(raw_hex)
            out["tc"] = tc

            if tc is not None and 1 <= tc <= 4:
                cs = pms.adsb.callsign(raw_hex)
                if cs:
                    out["callsign"] = cs.strip().rstrip("_")

            elif tc is not None and (9 <= tc <= 18 or 20 <= tc <= 22):
                alt = pms.adsb.altitude(raw_hex)
                if alt is not None:
                    out["baro_alt"] = int(alt)
                # CPR integers from raw bits
                mb = pms.hex2bin(raw_hex)
                me = mb[32:88]        # 56-bit message element
                out["cpr_odd"] = int(me[21])
                out["cpr_lat"] = int(me[22:39], 2)
                out["cpr_lon"] = int(me[39:56], 2)

            elif tc == 19:
                try:
                    speed, angle, vrate, spd_type = pms.adsb.velocity(raw_hex)
                    if speed is not None:
                        out["speed"] = round(speed)
                    if angle is not None:
                        out["heading"] = round(angle, 1)
                except Exception:
                    pass

        elif df in (4, 20):
            try:
                alt = pms.altcode(raw_hex)
                if alt is not None:
                    out["baro_alt"] = int(alt)
            except Exception:
                pass

        elif df in (5, 21):
            try:
                sq = pms.idcode(raw_hex)
                if sq:
                    out["squawk"] = sq
            except Exception:
                pass

        elif df in (0, 16):
            out["acas_ra_valid"] = False   # pyModeS doesn't decode RA content

    except Exception:
        pass   # partial decode is fine — we compare what we got

    return out


# ── C extension decode (normalise to same field names) ────────────────────────

def _decode_c(raw_bytes: bytes, signal: int, timestamp: int) -> dict | None:
    """Return field dict from C extension, or None if rejected."""
    nd = decode_cffi.decode_message(raw_bytes, signal, timestamp)
    if nd is None:
        return None

    df   = nd.get("df")
    addr = nd.get("addr")
    icao = f"{addr:06X}" if addr else None
    # C extension corrects CRC errors (correctedbits > 0) — treat as crc_ok=True
    # only when there was no correction needed.
    crc_ok = (nd.get("correctedbits", 0) == 0) if df in (17, 18, 11) else None

    out = {
        "df":     df,
        "icao":   icao,
        "crc_ok": crc_ok,
        "correctedbits": nd.get("correctedbits", 0),
    }

    if "callsign" in nd:
        out["callsign"] = nd["callsign"].strip()
    if "baro_alt" in nd:
        out["baro_alt"] = nd["baro_alt"]
    if "squawk" in nd:
        out["squawk"] = nd["squawk"]
    if "cpr_odd" in nd:
        out["cpr_odd"] = int(nd["cpr_odd"])
        out["cpr_lat"] = nd["cpr_lat"]
        out["cpr_lon"] = nd["cpr_lon"]
    if "heading" in nd:
        out["heading"] = round(nd["heading"], 1)
    if "ias" in nd:
        out["speed"] = nd["ias"]
    elif "tas" in nd:
        out["speed"] = nd["tas"]
    if "acas_ra" in nd:
        out["acas_ra_valid"] = True

    return out


# ── Comparison logic ───────────────────────────────────────────────────────────

# Fields that require exact match
_EXACT_FIELDS = ("df", "icao", "crc_ok", "callsign", "squawk",
                 "cpr_odd", "cpr_lat", "cpr_lon", "acas_ra_valid")
# Fields that are compared with a tolerance
_APPROX_FIELDS = {
    "baro_alt": 25,      # ft — Gillham decode can differ by one step
    "heading":  1.0,     # degrees
    "speed":    2,       # kt — rounding differences
}


def compare(raw_hex: str, pm: dict, ce: dict, verbose: bool) -> list[str]:
    """Return list of mismatch strings (empty = clean)."""
    mismatches = []

    for field in _EXACT_FIELDS:
        pv = pm.get(field)
        cv = ce.get(field)
        if pv is None and cv is None:
            continue
        if pv != cv:
            mismatches.append(f"  {field}: pyModeS={pv!r}  C={cv!r}")

    for field, tol in _APPROX_FIELDS.items():
        pv = pm.get(field)
        cv = ce.get(field)
        if pv is None and cv is None:
            continue
        if pv is None or cv is None:
            mismatches.append(f"  {field}: pyModeS={pv!r}  C={cv!r}  (one side None)")
        elif abs(pv - cv) > tol:
            mismatches.append(f"  {field}: pyModeS={pv}  C={cv}  (delta {abs(pv-cv)} > tol {tol})")

    return mismatches


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Beast decode parity: pyModeS vs C extension")
    ap.add_argument("corpus", type=Path, help="Captured Beast binary file")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="Print details for every decoded frame, not just mismatches")
    ap.add_argument("--limit", type=int, default=0,
                    help="Stop after N frames (0 = no limit)")
    args = ap.parse_args()

    if not args.corpus.exists():
        sys.exit(f"File not found: {args.corpus}")

    stats = defaultdict(int)
    mismatches_by_df = defaultdict(list)

    frames = list(read_beast_frames(args.corpus))
    total_frames = len(frames)
    print(f"Corpus: {args.corpus}  ({total_frames} Mode-S frames)")
    print()

    limit = args.limit or total_frames
    for i, (msg_type, ts, signal, payload) in enumerate(frames[:limit]):
        raw_hex = payload.hex().upper()
        stats["total"] += 1

        pm = _decode_pymodes(raw_hex)
        ce = _decode_c(payload, signal, ts)

        df_label = f"DF{pm['df'] if pm else (ce['df'] if ce else '?')}"

        if pm is None and ce is None:
            stats["both_rejected"] += 1
            if args.verbose:
                print(f"[{i:6d}] {raw_hex[:28]}…  both rejected")
            continue

        if pm is None and ce is not None:
            # C accepted a frame pyModeS rejected — usually CRC correction
            if ce.get("correctedbits", 0) > 0:
                stats["c_corrected_only"] += 1
            else:
                stats["c_accepted_pymodes_rejected"] += 1
                mismatches_by_df[df_label].append(
                    f"  C accepted (correctedbits=0) but pyModeS rejected: {raw_hex[:28]}"
                )
            continue

        if pm is not None and ce is None:
            stats["pymodes_accepted_c_rejected"] += 1
            mismatches_by_df[df_label].append(
                f"  pyModeS accepted but C rejected: {raw_hex[:28]}"
            )
            continue

        # Both accepted — compare fields
        stats["both_accepted"] += 1
        mm = compare(raw_hex, pm, ce, args.verbose)

        if mm:
            stats["field_mismatches"] += 1
            mismatches_by_df[df_label].append(
                f"  {raw_hex[:28]}  pyModeS_df={pm.get('df')} C_df={ce.get('df')}\n"
                + "\n".join(mm)
            )
            if args.verbose:
                print(f"[{i:6d}] MISMATCH {raw_hex[:28]}")
                for m in mm:
                    print(m)
        elif args.verbose:
            print(f"[{i:6d}] OK  {df_label}  icao={pm.get('icao')}  {raw_hex[:28]}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Frames processed        : {stats['total']}")
    print(f"  Both accepted           : {stats['both_accepted']}")
    print(f"  Both rejected           : {stats['both_rejected']}")
    print(f"  C corrected, pms rejected: {stats['c_corrected_only']}  (CRC correction — expected)")
    print(f"  C accepted, pms rejected : {stats['c_accepted_pymodes_rejected']}  (unexpected)")
    print(f"  pyModeS accepted, C rej  : {stats['pymodes_accepted_c_rejected']}  (unexpected)")
    print(f"  Field mismatches        : {stats['field_mismatches']}")
    print()

    total_unexpected = (
        stats["c_accepted_pymodes_rejected"]
        + stats["pymodes_accepted_c_rejected"]
        + stats["field_mismatches"]
    )

    if total_unexpected == 0:
        print("PASS — zero unexpected divergences.")
    else:
        print(f"FAIL — {total_unexpected} unexpected divergences:")
        for df_label, items in sorted(mismatches_by_df.items()):
            print(f"\n  {df_label} ({len(items)} issues):")
            for item in items[:10]:    # cap per-DF output
                print(item)
            if len(items) > 10:
                print(f"  … and {len(items) - 10} more")

    return 0 if total_unexpected == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
