"""
Terrain elevation API.

GET /api/terrain/grid  — SRTM1 elevation grid centred on RECEIVER_LAT/LON

SRTM source: AWS Mapzen elevation tiles (elevation-tiles-prod.s3.amazonaws.com)
  - No authentication required, no continent routing needed
  - URL: /skadi/{NS_DIR}/{TILE}.hgt.gz  e.g. /skadi/N52/N52W002.hgt.gz
  - SRTM1 format: 3601×3601 int16 big-endian, 1° tiles, 1 arc-second (~30 m)
  - Void sentinel -32768 → -1 (ocean); 0 = actual sea-level land
  - Cached in backend/data/terrain/

Grid builder uses numpy when available (50-100× faster than pure Python).
Falls back to pure Python if numpy is not installed.
"""

import array
import asyncio
import gzip
import logging
import math
import os
import sys
import threading
import urllib.request
from collections import OrderedDict
from pathlib import Path
from typing import Optional

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False

from fastapi import APIRouter, HTTPException, Query

import config

log = logging.getLogger(__name__)
router = APIRouter(tags=["terrain"])

_cache_lock = threading.Lock()

_TERRAIN_DIR = config.DATA_DIR / "terrain"
_TERRAIN_DIR.mkdir(exist_ok=True)

_SRTM_BASE = "https://elevation-tiles-prod.s3.amazonaws.com/skadi"


def _tile_name(lat: int, lon: int) -> str:
    ns = "N" if lat >= 0 else "S"
    ew = "E" if lon >= 0 else "W"
    return f"{ns}{abs(lat):02d}{ew}{abs(lon):03d}"


def _download_tile(lat: int, lon: int) -> Optional[Path]:
    """Download SRTM1 tile .hgt.gz, decompress and cache as .hgt. Returns path or None."""
    name = _tile_name(lat, lon)
    hgt_path = _TERRAIN_DIR / f"{name}.hgt"
    if hgt_path.exists():
        return hgt_path

    ns_dir = f"{'N' if lat >= 0 else 'S'}{abs(lat):02d}"
    url = f"{_SRTM_BASE}/{ns_dir}/{name}.hgt.gz"
    try:
        log.info("Downloading SRTM tile %s", name)
        req = urllib.request.Request(url, headers={"User-Agent": "adsb-dashboard/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            gz_bytes = resp.read()
        hgt_data = gzip.decompress(gz_bytes)
        tmp_path = hgt_path.with_suffix('.tmp')
        tmp_path.write_bytes(hgt_data)
        os.replace(tmp_path, hgt_path)  # atomic rename — safe for concurrent threads
        log.info("Cached SRTM tile %s (%d KB)", name, len(hgt_data) // 1024)
        return hgt_path
    except Exception as exc:
        log.warning("SRTM tile %s download failed: %s", name, exc)
        return None


def _read_hgt(path: Path) -> tuple[array.array, int]:
    """Return compact array of signed int16 elevation values (row-major, N→S, W→E).

    Uses array.array('h') — 2 bytes/element vs ~28 bytes/element for Python tuple.
    HGT files are big-endian; byteswap on little-endian hosts.
    """
    data = path.read_bytes()
    n_sq = len(data) // 2
    n = int(math.isqrt(n_sq))
    arr = array.array('h', data[:n * n * 2])
    if sys.byteorder == 'little':
        arr.byteswap()   # HGT is big-endian
    return arr, n


def _sample_elev(shorts: tuple, n: int, tile_lat: int, tile_lon: int,
                 lat: float, lon: float) -> int:
    """Nearest-neighbour sample. Row 0 = north edge (tile_lat+1), col 0 = west edge."""
    if n == 0:
        return 0
    row = round((tile_lat + 1 - lat) * (n - 1))
    col = round((lon - tile_lon) * (n - 1))
    row = max(0, min(n - 1, row))
    col = max(0, min(n - 1, col))
    v = shorts[row * n + col]
    return -1 if v == -32768 else v  # -1 = ocean/void; 0 = actual sea-level land


# LRU tile cache — capped at 12 tiles (~300 MB) to prevent unbounded growth.
# Each SRTM1 tile is ~25 MB (3601×3601 int16); a 400nm request loads ~20 tiles,
# but tiles overlap between requests so 12 covers typical single-site usage.
_TILE_CACHE_MAX = 12
_tile_cache: OrderedDict = OrderedDict()


def _get_tile(lat: int, lon: int) -> tuple[tuple | None, int | None]:
    key = (lat, lon)
    with _cache_lock:
        if key in _tile_cache:
            # Move to end to mark as most-recently used
            _tile_cache.move_to_end(key)
            return _tile_cache[key]
        path = _download_tile(lat, lon)
        value = (None, None) if path is None else _read_hgt(path)
        _tile_cache[key] = value
        _tile_cache.move_to_end(key)
        # Evict least-recently-used tile when over capacity
        while len(_tile_cache) > _TILE_CACHE_MAX:
            evicted = next(iter(_tile_cache))
            del _tile_cache[evicted]
            log.debug("Evicted SRTM tile %s from LRU cache", evicted)
    return value


def _build_grid_np(
    receiver_lat: float,
    receiver_lon: float,
    radius_nm: float,
    grid_n: int,
    min_radius_nm: float,
) -> dict:
    """Numpy-vectorised grid builder. Groups points by SRTM tile and does bulk
    index lookups via zero-copy frombuffer views — ~50-100× faster than the
    pure-Python version."""
    step_nm = (radius_nm * 2) / (grid_n - 1)
    half = (grid_n - 1) / 2
    nm_per_lat_deg = 60.0
    nm_per_lon_deg = 60.0 * math.cos(math.radians(receiver_lat))

    # Grid offsets from centre (row 0 = north)
    east_nm  = (np.arange(grid_n, dtype=np.float64) - half) * step_nm   # (grid_n,)
    north_nm = (half - np.arange(grid_n, dtype=np.float64)) * step_nm   # (grid_n,)

    # Full lat/lon grid: shape (grid_n, grid_n)
    # np.meshgrid ensures both arrays are fully materialised — required for boolean indexing
    lon, lat = np.meshgrid(
        receiver_lon + east_nm  / nm_per_lon_deg,
        receiver_lat + north_nm / nm_per_lat_deg,
    )

    # Inner-zone mask (set to 0 — used for min_radius masking)
    range_sq    = east_nm[np.newaxis, :] ** 2 + north_nm[:, np.newaxis] ** 2
    inner_mask  = range_sq < (min_radius_nm ** 2)

    tile_lat = np.floor(lat).astype(np.int32)
    tile_lon = np.floor(lon).astype(np.int32)

    result = np.zeros((grid_n, grid_n), dtype=np.int16)

    # Process each unique SRTM tile that covers part of the grid
    pairs        = np.column_stack([tile_lat.ravel(), tile_lon.ravel()])
    unique_tiles = np.unique(pairs, axis=0)

    for tlat_i, tlon_i in unique_tiles:
        tlat, tlon = int(tlat_i), int(tlon_i)
        shorts, n = _get_tile(tlat, tlon)
        if shorts is None or n == 0:
            continue

        mask    = (tile_lat == tlat) & (tile_lon == tlon)
        sub_lat = lat[mask]
        sub_lon = lon[mask]

        # Nearest-neighbour row/col within tile
        rows = np.clip(np.rint((tlat + 1 - sub_lat) * (n - 1)).astype(np.int32), 0, n - 1)
        cols = np.clip(np.rint((sub_lon - tlon)     * (n - 1)).astype(np.int32), 0, n - 1)

        # Zero-copy view of array.array buffer — no data copied until indexing
        tile_np = np.frombuffer(shorts, dtype=np.int16)
        result[mask] = tile_np[rows * n + cols]

    if min_radius_nm > 0:
        result[inner_mask] = 0

    # SRTM void (-32768) → -1 (ocean/void sentinel for frontend)
    result[result == np.int16(-32768)] = -1

    return {
        "grid_n": grid_n,
        "step_nm": float(step_nm),
        "radius_nm": radius_nm,
        "min_radius_nm": min_radius_nm,
        "elevations": result.ravel().tolist(),
        "receiver_lat": receiver_lat,
        "receiver_lon": receiver_lon,
    }


def _build_grid(
    receiver_lat: float,
    receiver_lon: float,
    radius_nm: float,
    grid_n: int,
    min_radius_nm: float,
) -> dict:
    """Pure-Python fallback grid builder (used when numpy is not installed)."""
    step_nm = (radius_nm * 2) / (grid_n - 1)
    half = (grid_n - 1) / 2
    nm_per_lat_deg = 60.0
    nm_per_lon_deg = 60.0 * math.cos(math.radians(receiver_lat))

    elevations: list[int] = []

    for row in range(grid_n):
        for col in range(grid_n):
            east_nm  = (col - half) * step_nm
            north_nm = (half - row) * step_nm

            if math.sqrt(east_nm ** 2 + north_nm ** 2) < min_radius_nm:
                elevations.append(0)
                continue

            lat = receiver_lat + north_nm / nm_per_lat_deg
            lon = receiver_lon + east_nm  / nm_per_lon_deg

            tile_lat = int(math.floor(lat))
            tile_lon = int(math.floor(lon))

            shorts, n = _get_tile(tile_lat, tile_lon)
            if shorts is None:
                elevations.append(0)
            else:
                elevations.append(_sample_elev(shorts, n, tile_lat, tile_lon, lat, lon))

    return {
        "grid_n": grid_n,
        "step_nm": step_nm,
        "radius_nm": radius_nm,
        "min_radius_nm": min_radius_nm,
        "elevations": elevations,
        "receiver_lat": receiver_lat,
        "receiver_lon": receiver_lon,
    }


def _build(receiver_lat, receiver_lon, radius_nm, grid_n, min_radius_nm):
    """Dispatch to numpy or pure-Python builder based on availability."""
    fn = _build_grid_np if _HAS_NUMPY else _build_grid
    return fn(receiver_lat, receiver_lon, radius_nm, grid_n, min_radius_nm)


@router.get("/api/terrain/grid")
async def terrain_grid(
    radius_nm: float = Query(default=300, ge=50, le=600),
    grid_n: int = Query(default=128, ge=32, le=2048),
    min_radius_nm: float = Query(default=0, ge=0),
) -> dict:
    """SRTM elevation grid centred on RECEIVER_LAT/LON."""
    if config.RECEIVER_LAT is None or config.RECEIVER_LON is None:
        raise HTTPException(status_code=400, detail="RECEIVER_LAT/LON not configured")
    return await asyncio.to_thread(
        _build,
        config.RECEIVER_LAT,
        config.RECEIVER_LON,
        radius_nm,
        grid_n,
        min_radius_nm,
    )
