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
    DF17/18  → df, icao, crc_ok, callsign (tc1-4), baro_alt (tc9-22),
               cpr_odd/cpr_lat/cpr_lon (tc9-22), squawk (tc28),
               heading+speed (tc19 subtypes 3/4 airspeed only)
    DF4/20   → df, icao, baro_alt
    DF5/21   → df, icao, squawk
    DF11     → df, icao  (crc_ok skipped — PI field semantics differ)
    DF0/16   → df, icao, acas_ra_valid (only when True)

Expected differences (not counted as failures):
    DF0/4/5/20/21 C-rejected  — C requires ICAO confirmation for AP frames;
                                 no state in this script so all AP frames
                                 are rejected by C. pyModeS has no such filter.
    DF11 crc_ok               — DF11 uses PI (parity/interrogator) field, not
                                 zero-residual CRC. pyModeS returns PI value;
                                 C handles PI correctly. Not comparable.
    C decodes extra fields    — C decodes DF0 altitude, BDS 5.0/6.0 heading/
                                 speed in DF20/21, TC28 squawk. pyModeS doesn't.
                                 "C decodes more" is not a bug.
    DF17 TC19 subtypes 1/2    — pyModeS returns ground speed; C returns nothing
                                 (only produces ias/tas for subtypes 3/4).
                                 Different decode scope, not a bug.
"""

import sys
import argparse
from pathlib import Path
from collections import defaultdict

_BACKEND = Path(__file__).resolve().parent.parent / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

try:
    import pyModeS as pms
except ImportError:
    sys.exit("pyModeS not found — run from backend/ directory via: uv run python ...")

try:
    import decode_cffi
    decode_cffi._get_lib()
    HAVE_C_LIB = True
except Exception as exc:
    sys.exit(f"decode_cffi not available: {exc}\nRun: make -C backend/native && make -C backend/native install")

# ── DFs that use AP (Address/Parity) field ────────────────────────────────────
# C requires ICAO confirmation for these; without state, all are rejected.
# "pyModeS accepted, C rejected" for these DFs is expected behaviour.
_AP_FIELD_DFS = {0, 4, 5, 20, 21}

# ── Beast frame reader ────────────────────────────────────────────────────────

_MSG_LEN = {0x31: 2, 0x32: 7, 0x33: 14}


def _unescape(buf: bytearray, start: int, needed: int):
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
        if msg_type == 0x31:
            continue
        timestamp = int.from_bytes(data[:6], "big")
        signal    = data[6]
        payload   = data[7:]
        yield msg_type, timestamp, signal, payload


# ── pyModeS decode ────────────────────────────────────────────────────────────

def _decode_pymodes(raw_hex: str) -> dict | None:
    try:
        df = pms.df(raw_hex)
    except Exception:
        return None

    try:
        icao = pms.icao(raw_hex)
    except Exception:
        icao = None

    # DF17/18: zero-residual CRC — comparable.
    # DF11: PI field (ICAO XOR interrogator ID) — not zero-residual, skip.
    # AP frames (DF0/4/5/20/21): AP = ICAO XOR CRC — not comparable.
    crc_ok = None
    if df in (17, 18):
        try:
            crc_ok = (pms.crc(raw_hex) == 0)
        except Exception:
            pass

    out: dict = {
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
                mb  = pms.hex2bin(raw_hex)
                me  = mb[32:88]
                out["cpr_odd"] = int(me[21])
                out["cpr_lat"] = int(me[22:39], 2)
                out["cpr_lon"] = int(me[39:56], 2)

            elif tc == 19:
                # velocity() returns (speed, angle, vrate, spd_type)
                # spd_type: 'GS' for subtypes 1/2, 'AS' for subtypes 3/4
                # C only produces ias/tas for subtypes 3/4, so only compare
                # when pyModeS is also returning airspeed.
                try:
                    speed, angle, vrate, spd_type = pms.adsb.velocity(raw_hex)
                    out["vel_spd_type"] = spd_type   # 'GS' or 'AS'
                    if speed is not None and spd_type == "AS":
                        out["speed"] = round(speed)
                    if angle is not None and spd_type == "AS":
                        out["heading"] = round(angle, 1)
                except Exception:
                    pass

            elif tc == 28:
                # Aircraft status message — contains emergency squawk.
                # Extract from raw bits: ME bits 9-21 (13 bits) = squawk.
                # ME = raw_hex bits 33-88; squawk is at ME bits 9-21.
                try:
                    mb = pms.hex2bin(raw_hex)
                    me = mb[32:88]
                    subtype = int(me[5:8], 2)
                    if subtype == 1:   # emergency/priority status
                        # ME bits 11-23: C1 A1 C2 A2 C4 A4 [M] B1 D1 B2 D2 B4 D4
                        # Bits 8-10 = emergency state; bit 17 = marker (M), skipped.
                        sq_bits = me[11:24]
                        sq_val  = int(sq_bits, 2)
                        c1 = (sq_val >> 12) & 1
                        a1 = (sq_val >> 11) & 1
                        c2 = (sq_val >> 10) & 1
                        a2 = (sq_val >> 9)  & 1
                        c4 = (sq_val >> 8)  & 1
                        a4 = (sq_val >> 7)  & 1
                        # bit 6 = M (marker), skip
                        b1 = (sq_val >> 5)  & 1
                        d1 = (sq_val >> 4)  & 1
                        b2 = (sq_val >> 3)  & 1
                        d2 = (sq_val >> 2)  & 1
                        b4 = (sq_val >> 1)  & 1
                        d4 = (sq_val >> 0)  & 1
                        a  = (a4 << 2) | (a2 << 1) | a1
                        b  = (b4 << 2) | (b2 << 1) | b1
                        c  = (c4 << 2) | (c2 << 1) | c1
                        d  = (d4 << 2) | (d2 << 1) | d1
                        out["squawk"] = f"{a}{b}{c}{d}"
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

        # DF0/16: acas_ra_valid only flagged when C says True — do not set False here.
        # DF11: nothing beyond df/icao to compare.

    except Exception:
        pass

    return out


# ── C extension decode ────────────────────────────────────────────────────────

def _decode_c(raw_bytes: bytes, signal: int, timestamp: int) -> dict | None:
    nd = decode_cffi.decode_message(raw_bytes, signal, timestamp)
    if nd is None:
        return None

    df   = nd.get("df")
    addr = nd.get("addr")
    icao = f"{addr:06X}" if addr else None

    # crc_ok only meaningful for DF17/18 (zero-residual)
    crc_ok = None
    if df in (17, 18):
        crc_ok = (nd.get("correctedbits", 0) == 0)

    out: dict = {
        "df":            df,
        "icao":          icao,
        "crc_ok":        crc_ok,
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
    # ias/tas = airspeed subtypes 3/4 only — matches pyModeS "AS" path
    if "ias" in nd:
        out["speed"] = nd["ias"]
    elif "tas" in nd:
        out["speed"] = nd["tas"]
    if "acas_ra" in nd:
        out["acas_ra_valid"] = True

    return out


# ── Comparison ────────────────────────────────────────────────────────────────

_EXACT_FIELDS  = ("df", "icao", "crc_ok", "callsign", "squawk",
                  "cpr_odd", "cpr_lat", "cpr_lon", "acas_ra_valid")
_APPROX_FIELDS = {
    "baro_alt": 25,    # ft
    "heading":  1.0,   # degrees
    "speed":    2,     # kt
}

# Fields where "C has value, pyModeS has None" means C decodes more — not a bug.
# Only flag when pyModeS has a value that C contradicts.
_C_EXTRA_OK_FIELDS = {"baro_alt", "heading", "speed", "callsign", "squawk"}


def compare(pm: dict, ce: dict) -> list[str]:
    """Return list of genuine mismatch strings (empty = clean)."""
    df = pm.get("df")
    mismatches = []

    for field in _EXACT_FIELDS:
        pv = pm.get(field)
        cv = ce.get(field)
        if pv is None and cv is None:
            continue
        # acas_ra_valid: only a mismatch if pyModeS says True but C doesn't,
        # or C says True but pyModeS says False explicitly.
        # "pyModeS absent, C True" = C found something pyModeS missed — flag it.
        # "pyModeS False, C absent" = no RA on either side — not a mismatch.
        if field == "acas_ra_valid":
            if pv is False and cv is None:
                continue   # both say no RA
            if pv is None and cv is None:
                continue
        # For fields where C decodes more, only flag if pyModeS has a value that
        # differs — not if pyModeS is simply absent.
        if field in _C_EXTRA_OK_FIELDS and pv is None and cv is not None:
            continue
        if pv != cv:
            mismatches.append(f"  {field}: pyModeS={pv!r}  C={cv!r}")

    for field, tol in _APPROX_FIELDS.items():
        pv = pm.get(field)
        cv = ce.get(field)
        if pv is None and cv is None:
            continue
        # C decodes more — only flag if pyModeS has a conflicting value
        if pv is None and cv is not None:
            continue
        if pv is not None and cv is None:
            # pyModeS decoded something C didn't — flag only for DF17/18
            # where both should decode; not for AP frames or DF0
            if df in (17, 18):
                mismatches.append(f"  {field}: pyModeS={pv}  C=None  (C missed)")
            continue
        if abs(pv - cv) > tol:
            mismatches.append(
                f"  {field}: pyModeS={pv}  C={cv}  (Δ{abs(pv-cv)} > tol {tol})"
            )

    return mismatches


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Beast decode parity: pyModeS vs C extension")
    ap.add_argument("corpus", type=Path, help="Captured Beast binary file")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="Print details for every frame, not just failures")
    ap.add_argument("--limit", type=int, default=0,
                    help="Stop after N frames (0 = no limit)")
    args = ap.parse_args()

    if not args.corpus.exists():
        sys.exit(f"File not found: {args.corpus}")

    stats             = defaultdict(int)
    mismatches_by_df  = defaultdict(list)

    frames = list(read_beast_frames(args.corpus))
    print(f"Corpus: {args.corpus}  ({len(frames)} Mode-S frames)")
    print()

    limit = args.limit or len(frames)
    for i, (msg_type, ts, signal, payload) in enumerate(frames[:limit]):
        raw_hex = payload.hex().upper()
        stats["total"] += 1

        pm = _decode_pymodes(raw_hex)
        ce = _decode_c(payload, signal, ts)

        df_val   = (pm or ce or {}).get("df")
        df_label = f"DF{df_val}" if df_val is not None else "DF?"

        # ── Both rejected ─────────────────────────────────────────────────
        if pm is None and ce is None:
            stats["both_rejected"] += 1
            if args.verbose:
                print(f"[{i:6d}] both_rejected  {raw_hex[:28]}")
            continue

        # ── C accepted, pyModeS rejected ──────────────────────────────────
        if pm is None and ce is not None:
            if ce.get("correctedbits", 0) > 0:
                stats["c_corrected_only"] += 1
                if args.verbose:
                    print(f"[{i:6d}] c_corrected  {df_label}  {raw_hex[:28]}")
            else:
                stats["c_accepted_pymodes_rejected"] += 1
                mismatches_by_df[df_label].append(
                    f"  C accepted (correctedbits=0) but pyModeS rejected: {raw_hex[:28]}"
                )
            continue

        # ── pyModeS accepted, C rejected ──────────────────────────────────
        if pm is not None and ce is None:
            if df_val in _AP_FIELD_DFS:
                # Expected: C requires ICAO confirmation for AP frames.
                # Without state this always happens.
                stats["c_ap_icao_filter"] += 1
                if args.verbose:
                    print(f"[{i:6d}] c_icao_filter  {df_label}  {raw_hex[:28]}")
            else:
                stats["pymodes_accepted_c_rejected"] += 1
                mismatches_by_df[df_label].append(
                    f"  pyModeS accepted but C rejected (unexpected): {raw_hex[:28]}"
                )
            continue

        # ── Both accepted — compare fields ────────────────────────────────
        stats["both_accepted"] += 1
        mm = compare(pm, ce)

        if mm:
            stats["field_mismatches"] += 1
            mismatches_by_df[df_label].append(
                f"  {raw_hex[:28]}  df={df_val}\n" + "\n".join(mm)
            )
            if args.verbose:
                print(f"[{i:6d}] MISMATCH  {df_label}  {raw_hex[:28]}")
                for m in mm:
                    print(m)
        elif args.verbose:
            print(f"[{i:6d}] OK  {df_label}  icao={pm.get('icao')}  {raw_hex[:28]}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Frames processed              : {stats['total']}")
    print(f"  Both accepted                 : {stats['both_accepted']}")
    print(f"  Both rejected                 : {stats['both_rejected']}")
    print(f"  C corrected, pyModeS rejected : {stats['c_corrected_only']}  (CRC correction — expected)")
    print(f"  C AP/ICAO filter              : {stats['c_ap_icao_filter']}  (no state — expected)")
    print(f"  C accepted, pyModeS rejected  : {stats['c_accepted_pymodes_rejected']}  (unexpected)")
    print(f"  pyModeS accepted, C rejected  : {stats['pymodes_accepted_c_rejected']}  (unexpected)")
    print(f"  Field mismatches              : {stats['field_mismatches']}")
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
            for item in items[:10]:
                print(item)
            if len(items) > 10:
                print(f"  … and {len(items) - 10} more")

    return 0 if total_unexpected == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
