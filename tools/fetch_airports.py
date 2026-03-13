#!/usr/bin/env python3
"""
Download OurAirports data and save large + medium airports as compact JSON.

Run once from the project root:
    python3 tools/fetch_airports.py

Output: backend/data/airports.json
"""

import csv
import io
import json
import pathlib
import urllib.request

URL = "https://ourairports.com/data/airports.csv"
OUT = pathlib.Path(__file__).parent.parent / "backend" / "data" / "airports.json"
INCLUDE_TYPES = {"large_airport", "medium_airport"}


def main():
    print("Downloading airports.csv from OurAirports …")
    with urllib.request.urlopen(URL, timeout=30) as r:
        content = r.read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))
    airports = []
    for row in reader:
        if row["type"] not in INCLUDE_TYPES:
            continue
        try:
            lat = round(float(row["latitude_deg"]), 4)
            lon = round(float(row["longitude_deg"]), 4)
        except ValueError:
            continue
        airports.append({
            "name": row["name"],
            "iata": row["iata_code"] or None,
            "icao": row["ident"],
            "lat":  lat,
            "lon":  lon,
            "type": row["type"],
        })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(airports, f, separators=(",", ":"))

    large  = sum(1 for a in airports if a["type"] == "large_airport")
    medium = sum(1 for a in airports if a["type"] == "medium_airport")
    size_kb = OUT.stat().st_size // 1024
    print(f"Saved {len(airports)} airports ({large} large, {medium} medium) → {OUT} ({size_kb} KB)")
    print("Restart the backend to pick up the new file.")


if __name__ == "__main__":
    main()
