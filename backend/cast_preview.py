#!/usr/bin/env python3
"""Generate a preview cast image for layout testing.

Usage:
    uv run --directory backend python cast_preview.py
    uv run --directory backend python cast_preview.py --output out.jpg
    uv run --directory backend python cast_preview.py --icao 4008F6
    uv run --directory backend python cast_preview.py --route
    uv run --directory backend python cast_preview.py --military
    uv run --directory backend python cast_preview.py --emergency
    uv run --directory backend python cast_preview.py --no-photo
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))

MOCK = {
    "icao":          "3C6444",
    "callsign":      "DLH123",
    "registration":  "D-AIBB",
    "type_code":     "A20N",
    "type_desc":     "A320neo",
    "type_full_name": "Airbus A320 Neo",
    "operator":      "Lufthansa",
    "altitude":      35000,
    "range_nm":      42.3,
    "bearing_deg":   137.0,
    "squawk":        "1234",
    "military":      False,
    "sighting_count": 14,
}

MOCK_ROUTE = ("EGKK", "London Gatwick", "EDDF", "Frankfurt Airport")
DEFAULT_API_BASE = os.environ.get("CAST_PREVIEW_API_BASE", "http://localhost:8000")


def fetch_live_aircraft(api_base: str, icao: str) -> dict:
    """Fetch merged aircraft detail from the local backend and reshape it for cast."""
    url = f"{api_base.rstrip('/')}/api/aircraft/{icao.upper()}"
    req = urllib.request.Request(url, headers={"User-Agent": "adsb-dashboard-cast-preview/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"Failed to fetch {icao.upper()} from {url}: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"Failed to reach backend at {url}: {exc.reason}") from exc

    live = data.get("live") or {}
    history = data.get("history") or {}
    return {
        "icao": data.get("icao") or icao.upper(),
        "callsign": live.get("callsign"),
        "registration": data.get("registration"),
        "type_code": data.get("type_code"),
        "type_desc": data.get("type_desc") or data.get("type_category"),
        "type_full_name": data.get("type_full_name"),
        "operator": data.get("operator"),
        "altitude": live.get("altitude"),
        "range_nm": live.get("range_nm"),
        "bearing_deg": live.get("bearing_deg"),
        "squawk": live.get("squawk"),
        "military": bool(data.get("military")),
        "sighting_count": history.get("sighting_count"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a cast preview image")
    parser.add_argument("--icao", help="load a live aircraft from the local backend by ICAO hex")
    parser.add_argument(
        "--api-base",
        default=DEFAULT_API_BASE,
        help=f"backend base URL for --icao lookups (default: {DEFAULT_API_BASE})",
    )
    parser.add_argument("--output",    default="preview.jpg", help="output file path")
    parser.add_argument("--military",  action="store_true",   help="set military flag")
    parser.add_argument("--emergency", action="store_true",   help="set squawk 7700")
    parser.add_argument("--route",     action="store_true",   help="inject mock route")
    parser.add_argument("--no-photo",  action="store_true",   help="skip photo fetch")
    args = parser.parse_args()

    import cast

    ac = fetch_live_aircraft(args.api_base, args.icao) if args.icao else dict(MOCK)
    if args.military:
        ac["military"] = True
    if args.emergency:
        ac["squawk"] = "7700"
    if args.route:
        cast._get_route = lambda icao: MOCK_ROUTE
    if args.no_photo:
        cast._fetch_photo = lambda icao: None

    print(f"Rendering {ac['icao']} ({ac.get('callsign')})…")
    jpeg = cast.render_display_image(ac)

    with open(args.output, "wb") as fh:
        fh.write(jpeg)
    print(f"Saved {len(jpeg):,} bytes → {args.output}")


if __name__ == "__main__":
    main()
