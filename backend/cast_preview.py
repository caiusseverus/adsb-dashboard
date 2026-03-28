#!/usr/bin/env python3
"""Generate a preview cast image for layout testing.

Usage:
    uv run --directory backend python cast_preview.py
    uv run --directory backend python cast_preview.py --output out.jpg
    uv run --directory backend python cast_preview.py --route
    uv run --directory backend python cast_preview.py --military
    uv run --directory backend python cast_preview.py --emergency
    uv run --directory backend python cast_preview.py --no-photo
"""
import argparse
import os
import sys

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
}

MOCK_ROUTE = ("EGKK", "London Gatwick", "EDDF", "Frankfurt Airport")


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a cast preview image")
    parser.add_argument("--output",    default="preview.jpg", help="output file path")
    parser.add_argument("--military",  action="store_true",   help="set military flag")
    parser.add_argument("--emergency", action="store_true",   help="set squawk 7700")
    parser.add_argument("--route",     action="store_true",   help="inject mock route")
    parser.add_argument("--no-photo",  action="store_true",   help="skip photo fetch")
    args = parser.parse_args()

    import cast

    ac = dict(MOCK)
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
