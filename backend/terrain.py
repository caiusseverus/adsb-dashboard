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
from fastapi.responses import JSONResponse, Response

import config
from utils_geo import destination_point

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)  # terrain build progress always visible regardless of DEBUG_LOG
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
    """Download SRTM1 tile and cache as .hgt.gz (compressed). Returns path or None."""
    name = _tile_name(lat, lon)
    gz_path = _TERRAIN_DIR / f"{name}.hgt.gz"
    if gz_path.exists():
        return gz_path

    # Migrate legacy uncompressed .hgt → .hgt.gz
    hgt_path = _TERRAIN_DIR / f"{name}.hgt"
    if hgt_path.exists():
        log.info("Compressing SRTM tile %s", name)
        compressed = gzip.compress(hgt_path.read_bytes())
        tmp = gz_path.with_suffix('.tmp')
        tmp.write_bytes(compressed)
        os.replace(tmp, gz_path)
        hgt_path.unlink()
        log.info("Compressed SRTM tile %s (%.0f KB on disk)", name, len(compressed) / 1024)
        return gz_path

    ns_dir = f"{'N' if lat >= 0 else 'S'}{abs(lat):02d}"
    url = f"{_SRTM_BASE}/{ns_dir}/{name}.hgt.gz"
    try:
        log.info("Downloading SRTM tile %s", name)
        req = urllib.request.Request(url, headers={"User-Agent": "adsb-dashboard/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            gz_bytes = resp.read()
        tmp = gz_path.with_suffix('.tmp')
        tmp.write_bytes(gz_bytes)
        os.replace(tmp, gz_path)  # atomic rename — safe for concurrent threads
        log.info("Cached SRTM tile %s (%.0f KB compressed)", name, len(gz_bytes) / 1024)
        return gz_path
    except Exception as exc:
        log.warning("SRTM tile %s download failed: %s", name, exc)
        return None


def _read_hgt(path: Path) -> tuple[array.array, int]:
    """Return compact array of signed int16 elevation values (row-major, N→S, W→E).

    Uses array.array('h') — 2 bytes/element vs ~28 bytes/element for Python tuple.
    HGT files are big-endian; byteswap on little-endian hosts.
    Accepts both .hgt and .hgt.gz paths.
    """
    data = path.read_bytes()
    if path.suffix == '.gz':
        data = gzip.decompress(data)
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
) -> "np.ndarray":
    """Numpy-vectorised grid builder. Returns int16 numpy array (grid_n×grid_n), N→S row-major.
    Groups points by SRTM tile and does bulk index lookups via zero-copy frombuffer views."""
    half = (grid_n - 1) / 2
    step_nm = (radius_nm * 2) / (grid_n - 1)
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

    n_tiles = len(unique_tiles)
    for ti, (tlat_i, tlon_i) in enumerate(unique_tiles, 1):
        tlat, tlon = int(tlat_i), int(tlon_i)
        log.info("Terrain: tile %d/%d (%s)", ti, n_tiles, _tile_name(tlat, tlon))
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

    return result


def _build_grid(
    receiver_lat: float,
    receiver_lon: float,
    radius_nm: float,
    grid_n: int,
    min_radius_nm: float,
) -> list[int]:
    """Pure-Python fallback grid builder (used when numpy is not installed).
    Returns list of int16 elevation values (row-major, N→S, W→E)."""
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

    return elevations


# ── Grid disk cache ───────────────────────────────────────────────────────────

def _fmt_coord(v: float) -> str:
    """Encode a lat/lon float as a safe filename component."""
    return f"{v:.5f}".replace('.', 'd').replace('-', 'm')


def _cache_path(lat: float, lon: float, radius_nm: float, grid_n: int, min_radius_nm: float) -> Path:
    mr = f"mr{int(min_radius_nm)}" if min_radius_nm else "mr0"
    return _TERRAIN_DIR / f"grid_{grid_n}_{int(radius_nm)}_{mr}_{_fmt_coord(lat)}_{_fmt_coord(lon)}.bin.gz"


_cleanup_done: set[tuple] = set()
_cleanup_lock = threading.Lock()


def _cleanup_stale_cache(lat: float, lon: float) -> None:
    """Delete grid cache files that don't match the current receiver coordinates."""
    coord_suffix = f"_{_fmt_coord(lat)}_{_fmt_coord(lon)}"
    for f in _TERRAIN_DIR.glob("grid_*.bin.gz"):
        if not f.name.endswith(coord_suffix + ".bin.gz"):
            try:
                f.unlink()
                log.info("Removed stale terrain cache: %s", f.name)
            except OSError as exc:
                log.warning("Could not remove stale terrain cache %s: %s", f.name, exc)
    # Remove legacy uncompressed .bin files with wrong coordinates
    for f in _TERRAIN_DIR.glob("grid_*.bin"):
        if not f.name.endswith(coord_suffix + ".bin"):
            try:
                f.unlink()
                log.info("Removed stale terrain cache: %s", f.name)
            except OSError as exc:
                log.warning("Could not remove stale terrain cache %s: %s", f.name, exc)


def _migrate_hgt_files() -> None:
    """Compress any uncompressed .hgt tiles left over from before this change."""
    for hgt in _TERRAIN_DIR.glob("*.hgt"):
        gz_path = hgt.with_name(hgt.name + '.gz')
        if gz_path.exists():
            hgt.unlink()
            continue
        try:
            log.info("Compressing SRTM tile %s", hgt.stem)
            compressed = gzip.compress(hgt.read_bytes())
            tmp = gz_path.with_name(gz_path.name + '.tmp')
            tmp.write_bytes(compressed)
            os.replace(tmp, gz_path)
            hgt.unlink()
            log.info("Compressed %s → %s (%.0f KB)", hgt.name, gz_path.name, len(compressed) / 1024)
        except OSError as exc:
            log.warning("Could not compress %s: %s", hgt.name, exc)


def _ensure_cleanup(lat: float, lon: float) -> None:
    key = (round(lat, 5), round(lon, 5))
    with _cleanup_lock:
        if key in _cleanup_done:
            return
        _cleanup_stale_cache(lat, lon)
        _migrate_hgt_files()
        _cleanup_done.add(key)


def _build(receiver_lat: float, receiver_lon: float, radius_nm: float,
           grid_n: int, min_radius_nm: float) -> tuple[bytes, dict]:
    """Return elevation grid as (raw int16 LE bytes, metadata dict).
    Builds once and caches to disk; subsequent calls read the cache file directly."""
    _ensure_cleanup(receiver_lat, receiver_lon)

    step_nm = (radius_nm * 2) / (grid_n - 1)
    metadata = {
        "grid_n":        grid_n,
        "step_nm":       step_nm,
        "radius_nm":     radius_nm,
        "min_radius_nm": min_radius_nm,
        "receiver_lat":  receiver_lat,
        "receiver_lon":  receiver_lon,
    }

    cache = _cache_path(receiver_lat, receiver_lon, radius_nm, grid_n, min_radius_nm)
    if cache.exists():
        log.debug("Terrain cache hit: %s", cache.name)
        return gzip.decompress(cache.read_bytes()), metadata

    # Migrate legacy uncompressed .bin → .bin.gz (cache.stem strips the .gz suffix)
    raw = _migrate_legacy_terrain_cache(cache)
    if raw is not None:
        return raw, metadata

    log.info("Building terrain grid %dx%d r=%.0fnm — result will be cached", grid_n, grid_n, radius_nm)
    elev_bytes = _build_and_cache_terrain(
        receiver_lat, receiver_lon, radius_nm, grid_n, min_radius_nm, cache
    )
    return elev_bytes, metadata


def _migrate_legacy_terrain_cache(cache) -> bytes | None:
    """Compress a legacy uncompressed .bin cache file to .bin.gz in place.

    Returns the raw elevation bytes if migration occurred, or None if no legacy file exists.
    The .bin.gz path is derived from the .bin.gz cache path (cache.stem strips the .gz suffix).
    """
    legacy = cache.with_name(cache.stem)
    if not legacy.exists():
        return None
    log.info("Compressing terrain cache %s", legacy.name)
    raw = legacy.read_bytes()
    tmp = cache.with_suffix('.tmp')
    tmp.write_bytes(gzip.compress(raw))
    os.replace(tmp, cache)
    legacy.unlink()
    log.info("Compressed terrain cache: %s (%.0f KB on disk)", cache.name, cache.stat().st_size / 1024)
    return raw


def _build_and_cache_terrain(
    receiver_lat: float, receiver_lon: float,
    radius_nm: float, grid_n: int, min_radius_nm: float,
    cache,
) -> bytes:
    """Build an elevation grid, compress it, persist to cache, and return the raw bytes."""
    if _HAS_NUMPY:
        arr = _build_grid_np(receiver_lat, receiver_lon, radius_nm, grid_n, min_radius_nm)
        # astype('<i2') is a no-op on LE hosts (Pi, x86) — one copy via tobytes()
        elev_bytes = arr.astype('<i2').ravel().tobytes()
    else:
        elevations = _build_grid(receiver_lat, receiver_lon, radius_nm, grid_n, min_radius_nm)
        a = array.array('h', elevations)
        if sys.byteorder == 'big':
            a.byteswap()
        elev_bytes = a.tobytes()

    tmp = cache.with_suffix('.tmp')
    try:
        tmp.write_bytes(gzip.compress(elev_bytes))
        os.replace(tmp, cache)
        log.info("Terrain cache saved: %s (%.0f KB on disk)", cache.name, cache.stat().st_size / 1024)
    except OSError as exc:
        log.warning("Could not write terrain cache %s: %s", cache.name, exc)
    return elev_bytes


@router.get("/api/terrain/grid")
async def terrain_grid(
    radius_nm: float = Query(default=300, ge=50, le=600),
    grid_n: int = Query(default=128, ge=32, le=2048),
    min_radius_nm: float = Query(default=0, ge=0),
):
    """SRTM elevation grid centred on RECEIVER_LAT/LON.
    Returns raw little-endian int16 bytes. Grid metadata is in response headers."""
    if not config.TERRAIN_ENABLED:
        raise HTTPException(status_code=503, detail="Terrain disabled (TERRAIN_ENABLED=false)")
    if config.RECEIVER_LAT is None or config.RECEIVER_LON is None:
        raise HTTPException(status_code=400, detail="RECEIVER_LAT/LON not configured")

    elev_bytes, meta = await asyncio.to_thread(
        _build,
        config.RECEIVER_LAT,
        config.RECEIVER_LON,
        radius_nm,
        grid_n,
        min_radius_nm,
    )

    return Response(
        content=elev_bytes,
        media_type="application/octet-stream",
        headers={
            "X-Grid-N":        str(meta["grid_n"]),
            "X-Step-Nm":       str(meta["step_nm"]),
            "X-Radius-Nm":     str(meta["radius_nm"]),
            "X-Receiver-Lat":  str(meta["receiver_lat"]),
            "X-Receiver-Lon":  str(meta["receiver_lon"]),
        },
    )


def _get_elevation_m(lat: float, lon: float) -> int:
    """Sample SRTM elevation at a single point. Returns metres; 0 for ocean/void/missing."""
    tile_lat = int(math.floor(lat))
    tile_lon = int(math.floor(lon))
    shorts, n = _get_tile(tile_lat, tile_lon)
    if shorts is None or n is None:
        return 0
    v = _sample_elev(shorts, n, tile_lat, tile_lon, lat, lon)
    return max(0, v)  # treat ocean/void (-1) as 0


def _horizon_cache_path(lat: float, lon: float) -> Path:
    return _TERRAIN_DIR / f"horizon_{lat:.6f}_{lon:.6f}.json"


def _compute_horizon(
    receiver_lat: float,
    receiver_lon: float,
    max_dist_nm: float = 100.0,
    step_nm: float = 0.5,
) -> dict:
    """Walk radially outward in 1° azimuth steps and find the maximum terrain elevation
    angle for each azimuth. Returns dict with 'elevations' (360 floats, degrees) and
    'receiver_elev_m'. Result is cached to disk keyed by receiver location."""
    import json
    cache = _horizon_cache_path(receiver_lat, receiver_lon)
    if cache.exists():
        try:
            return json.loads(cache.read_text())
        except Exception:
            pass  # corrupt cache — recompute

    receiver_elev = _get_elevation_m(receiver_lat, receiver_lon)
    elevations: list[float] = []

    steps = int(max_dist_nm / step_nm)
    distances = [(i + 1) * step_nm for i in range(steps)]

    for az in range(360):
        max_angle = 0.0
        for dist_nm in distances:
            lat2, lon2 = destination_point(receiver_lat, receiver_lon, az, dist_nm)
            terrain_m = _get_elevation_m(lat2, lon2)
            delta_m = terrain_m - receiver_elev
            dist_m = dist_nm * 1852.0
            angle = math.degrees(math.atan2(delta_m, dist_m))
            if angle > max_angle:
                max_angle = angle
        elevations.append(round(max_angle, 3))

    result = {"elevations": elevations, "receiver_elev_m": receiver_elev}
    try:
        cache.write_text(json.dumps(result))
        log.info("Terrain horizon cached: %s", cache.name)
    except OSError as exc:
        log.warning("Could not write horizon cache: %s", exc)
    return result


_RANGES_NM = [25, 50, 75, 100]


def _ranges_cache_path(lat: float, lon: float) -> Path:
    tag = "_".join(str(r) for r in _RANGES_NM)
    return _TERRAIN_DIR / f"horizon_ranges_{tag}_{lat:.6f}_{lon:.6f}.json"


def _compute_ranges_horizon(receiver_lat: float, receiver_lon: float) -> dict:
    """Compute cumulative-max terrain elevation angle per azimuth for each range band.

    Returns four 360-element profiles (one per entry in _RANGES_NM).  Each profile
    contains the maximum elevation angle (degrees) seen from the receiver to that
    range, for every 1° azimuth step.  Because the max is cumulative, each profile
    is guaranteed to be >= all previous profiles — so back-to-front canvas rendering
    produces the correct layered pseudo-3D silhouette.

    Cached to disk; first run takes a few seconds.
    """
    import json
    cache = _ranges_cache_path(receiver_lat, receiver_lon)
    if cache.exists():
        try:
            return json.loads(cache.read_text())
        except Exception:
            pass

    receiver_elev = _get_elevation_m(receiver_lat, receiver_lon)

    # Walk from 0 to max range in 0.5 nm steps; record cumulative max at each
    # range checkpoint.
    max_range = _RANGES_NM[-1]
    step_nm   = 0.5
    steps     = int(max_range / step_nm)
    distances = [(i + 1) * step_nm for i in range(steps)]

    profiles: list[list[float]] = [[] for _ in _RANGES_NM]

    for az in range(360):
        running_max = 0.0
        ring_idx    = 0
        for dist_nm in distances:
            lat2, lon2 = destination_point(receiver_lat, receiver_lon, az, dist_nm)
            terrain_m  = _get_elevation_m(lat2, lon2)
            delta_m    = terrain_m - receiver_elev
            dist_m     = dist_nm * 1852.0
            angle      = math.degrees(math.atan2(delta_m, dist_m))
            if angle > running_max:
                running_max = angle
            # Record at each range checkpoint (may record multiple if step crosses)
            while ring_idx < len(_RANGES_NM) and dist_nm >= _RANGES_NM[ring_idx]:
                profiles[ring_idx].append(round(running_max, 3))
                ring_idx += 1
        # Fill any remaining rings with the final max (shouldn't happen normally)
        while ring_idx < len(_RANGES_NM):
            profiles[ring_idx].append(round(running_max, 3))
            ring_idx += 1

    result = {
        "ranges":        _RANGES_NM,
        "profiles":      profiles,
        "receiver_elev_m": receiver_elev,
    }
    try:
        cache.write_text(json.dumps(result))
        log.info("Terrain ranges cached: %s", cache.name)
    except OSError as exc:
        log.warning("Could not write ranges cache: %s", exc)
    return result


@router.get("/api/terrain/ranges")
async def terrain_ranges():
    """Terrain horizon profiles for layered pseudo-3D rendering on Sky View.

    Returns four 360-element elevation-angle profiles (one per range band in
    _RANGES_NM = [25, 50, 75, 100] nm).  Each profile contains cumulative-max
    elevation angles, so drawing them back-to-front on a canvas produces a
    realistic pseudo-3D layered silhouette with hidden-surface removal.

    Response: {ranges: [int×4], profiles: [[float×360]×4], receiver_elev_m: float}
    """
    if not config.TERRAIN_ENABLED:
        raise HTTPException(status_code=503, detail="Terrain disabled")
    if config.RECEIVER_LAT is None or config.RECEIVER_LON is None:
        raise HTTPException(status_code=400, detail="RECEIVER_LAT/LON not configured")

    data = await asyncio.to_thread(
        _compute_ranges_horizon,
        config.RECEIVER_LAT,
        config.RECEIVER_LON,
    )
    return JSONResponse(data)


@router.get("/api/terrain/horizon")
async def terrain_horizon():
    """Terrain horizon elevation angle for each azimuth degree (0–359°).
    Returns {elevations: [float×360], receiver_elev_m: float}.
    Computed from SRTM data and cached to disk."""
    if not config.TERRAIN_ENABLED:
        raise HTTPException(status_code=503, detail="Terrain disabled (TERRAIN_ENABLED=false)")
    if config.RECEIVER_LAT is None or config.RECEIVER_LON is None:
        raise HTTPException(status_code=400, detail="RECEIVER_LAT/LON not configured")

    data = await asyncio.to_thread(
        _compute_horizon,
        config.RECEIVER_LAT,
        config.RECEIVER_LON,
    )
    return JSONResponse(data)


def prewarm_cache(receiver_lat: float, receiver_lon: float,
                  radius_nm: float = 400, grid_n: int = 512) -> None:
    """Build and cache the standard-resolution terrain grid at startup.
    No-op if the cache file already exists or TERRAIN_ENABLED is false."""
    if not config.TERRAIN_ENABLED:
        return
    cache = _cache_path(receiver_lat, receiver_lon, radius_nm, grid_n, 0)
    if cache.exists():
        log.info("Terrain cache already present: %s", cache.name)
        return
    log.info("Pre-warming terrain grid %dx%d r=%.0fnm — this may take a while on first run",
             grid_n, grid_n, radius_nm)
    _build(receiver_lat, receiver_lon, radius_nm, grid_n, 0)
