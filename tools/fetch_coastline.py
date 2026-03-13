#!/usr/bin/env python3
"""
Download Natural Earth 50m coastline + country borders and save as compact JSON.

Run once from the project root:
    python tools/fetch_coastline.py

Output: backend/data/coastline.json
  List of polylines: [[[lat, lon], ...], ...]
  Coordinates rounded to 4 decimal places (~11 m precision).
"""

import json
import pathlib
import urllib.request

SOURCES = [
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_coastline.geojson",
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_boundary_lines_land.geojson",
]

OUT = pathlib.Path(__file__).parent.parent / "backend" / "data" / "coastline.json"


def extract_lines(geojson: dict) -> list:
    lines = []
    for feature in geojson.get("features", []):
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [])
        if geom["type"] == "LineString":
            lines.append([[round(lat, 4), round(lon, 4)] for lon, lat in coords])
        elif geom["type"] == "MultiLineString":
            for seg in coords:
                lines.append([[round(lat, 4), round(lon, 4)] for lon, lat in seg])
    return lines


def main():
    all_lines = []
    for url in SOURCES:
        name = url.split("/")[-1]
        print(f"Downloading {name} ...")
        with urllib.request.urlopen(url, timeout=30) as r:
            data = json.load(r)
        lines = extract_lines(data)
        print(f"  → {len(lines)} polylines, {sum(len(l) for l in lines)} points")
        all_lines.extend(lines)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(all_lines, f, separators=(",", ":"))

    total_pts = sum(len(l) for l in all_lines)
    size_kb = OUT.stat().st_size // 1024
    print(f"\nSaved {len(all_lines)} polylines / {total_pts} points → {OUT} ({size_kb} KB)")
    print("Restart the backend to pick up the new file.")


if __name__ == "__main__":
    main()
