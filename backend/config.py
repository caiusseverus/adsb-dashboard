import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

BEAST_HOST: str = os.getenv("BEAST_HOST", "localhost")
BEAST_PORT: int = int(os.getenv("BEAST_PORT", "30005"))
AIRCRAFT_TIMEOUT: int = int(os.getenv("AIRCRAFT_TIMEOUT", "60"))


def _parse_mlat_servers(val: str, default_host: str) -> list[tuple[str, str, int]]:
    """Parse MLAT_SERVERS env var into list of (name, host, port).

    Two formats are supported per entry:
      Name@host:port  — explicit host (required when host differs from BEAST_HOST)
      Name:port       — host defaults to BEAST_HOST

    Example: "ADSBx@adsbpi:30158,FlightAware:30105,Airplanes:30157"
      → ('ADSBx', 'adsbpi', 30158), ('FlightAware', 'adsbpi', 30105), ('Airplanes', 'adsbpi', 30157)
    """
    servers: list[tuple[str, str, int]] = []
    for i, entry in enumerate(val.split(","), 1):
        entry = entry.strip()
        if not entry:
            continue
        try:
            if "@" in entry:
                # Name@host:port
                name, hostport = entry.split("@", 1)
                host, port_str = hostport.rsplit(":", 1)
            else:
                # Name:port  — use default_host
                name, port_str = entry.rsplit(":", 1)
                host = default_host
            servers.append((name.strip(), host.strip(), int(port_str.strip())))
        except (ValueError, AttributeError):
            import logging
            logging.getLogger(__name__).warning(
                "MLAT_SERVERS: skipping malformed entry %d %r", i, entry
            )
    return servers


_mlat_servers_raw = os.getenv("MLAT_SERVERS", "")
# Backward-compat: honour legacy MLAT_HOST / MLAT_PORT if MLAT_SERVERS not set
if not _mlat_servers_raw:
    _legacy_host = os.getenv("MLAT_HOST", "")
    _legacy_port = os.getenv("MLAT_PORT", "30105")
    if _legacy_host:
        _mlat_servers_raw = f"mlat@{_legacy_host}:{_legacy_port}"

MLAT_SERVERS: list[tuple[str, str, int]] = _parse_mlat_servers(
    _mlat_servers_raw, default_host=os.getenv("BEAST_HOST", "localhost")
)

_rlat = os.getenv("RECEIVER_LAT")
_rlon = os.getenv("RECEIVER_LON")
RECEIVER_LAT: Optional[float] = float(_rlat) if _rlat else None
RECEIVER_LON: Optional[float] = float(_rlon) if _rlon else None
def _parse_alt_ft(val: str) -> float:
    """Parse altitude: '32m' → feet, '106ft' → feet, bare number → feet."""
    v = val.strip().lower()
    if v.endswith('m'):
        return float(v[:-1]) * 3.28084
    if v.endswith('ft'):
        return float(v[:-2])
    return float(v)

RECEIVER_ALT_FT: float = _parse_alt_ft(os.getenv("RECEIVER_ALT_FT", "0"))

# DEBUG_ENRICHMENT: 0=off, 1=all (enrichment + ACAS), 2=ACAS only
# Accepts integer (0/1/2) or boolean-style string (true/false)
def _parse_debug_level(val: str) -> int:
    if val.lower() in ("true", "yes"):
        return 1
    if val.lower() in ("false", "no", ""):
        return 0
    return int(val)
DEBUG_ENRICHMENT: int = _parse_debug_level(os.getenv("DEBUG_ENRICHMENT", "0"))

HOME_COUNTRY: str = os.getenv("HOME_COUNTRY", "")
RARE_THRESHOLD: int = int(os.getenv("RARE_THRESHOLD", "5"))
# Minimum messages before an aircraft is written to the registry.
# With the ICAO filter enabled, this is a secondary persistence gate —
# mainly guards against very brief sightings (e.g. a single reflected DF17).
# Set to 0 to disable filtering. ADSBex/hexdb hit bypasses this check.
GHOST_FILTER_MSGS: int = int(os.getenv("GHOST_FILTER_MSGS", "2"))
MINUTE_STATS_RETENTION_DAYS: int = int(os.getenv("MINUTE_STATS_RETENTION_DAYS", "30"))

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH: Path = Path(os.getenv("DB_PATH", str(DATA_DIR / "adsb.db")))

_backup_raw = os.getenv("BACKUP_PATH", "")
BACKUP_PATH: Optional[Path] = Path(_backup_raw) if _backup_raw else None
BACKUP_RETAIN: int = int(os.getenv("BACKUP_RETAIN", "7"))

# Allowed CORS origins. Defaults to the Vite dev server only; not needed in
# production because the frontend is served from the same origin as the backend.
_cors_raw = os.getenv("CORS_ORIGINS", "http://localhost:5173")
CORS_ORIGINS: list[str] = [o.strip() for o in _cors_raw.split(",") if o.strip()]

# ---------------------------------------------------------------------------
# Notifications (ntfy.sh and/or SMTP email)
# ---------------------------------------------------------------------------
NTFY_URL: str = os.getenv("NTFY_URL", "")          # e.g. https://ntfy.sh/my-topic

NOTIFY_EMAIL_TO:   str = os.getenv("NOTIFY_EMAIL_TO",   "")
NOTIFY_EMAIL_FROM: str = os.getenv("NOTIFY_EMAIL_FROM", "")
NOTIFY_SMTP_HOST:  str = os.getenv("NOTIFY_SMTP_HOST",  "localhost")
NOTIFY_SMTP_PORT:  int = int(os.getenv("NOTIFY_SMTP_PORT", "587"))
NOTIFY_SMTP_USER:  str = os.getenv("NOTIFY_SMTP_USER",  "")
NOTIFY_SMTP_PASS:  str = os.getenv("NOTIFY_SMTP_PASS",  "")

# Trigger switches — defaults: emergency on, everything else off
def _bool(key: str, default: bool) -> bool:
    v = os.getenv(key, "")
    if not v:
        return default
    return v.lower() not in ("0", "false", "no")

NOTIFY_EMERGENCY:    bool = _bool("NOTIFY_EMERGENCY",    True)
NOTIFY_ACAS:         bool = _bool("NOTIFY_ACAS",         False)
NOTIFY_MILITARY:     bool = _bool("NOTIFY_MILITARY",     False)
NOTIFY_INTERESTING:  bool = _bool("NOTIFY_INTERESTING",  False)

DEBUG_LOG: bool = _bool("DEBUG_LOG", False)  # set true to enable verbose logging

# ---------------------------------------------------------------------------
# SQLite / Pi tuning
# ---------------------------------------------------------------------------
# synchronous=NORMAL is safe with WAL and eliminates most fsync() calls.
# Use FULL only if the Pi's power supply is unreliable.
SQLITE_SYNCHRONOUS: str = os.getenv("SQLITE_SYNCHRONOUS", "NORMAL")

# How often to run the full-table rarity recalculation (seconds).
# The rare flag changes only when a new type appears; daily is more than enough.
# Default: 6 hours (21600 s). Set to 3600 for hourly, 86400 for daily.
RARITY_RECALC_SECONDS: float = float(os.getenv("RARITY_RECALC_SECONDS", "21600"))

# MLAT position quality and fusion mode.
# none         — Phase A: last-write-wins, no position change (default, safe)
# spike_filter — reject fixes whose implied groundspeed exceeds threshold
# weighted     — ECEF weighted centroid of recent fixes from all active sources
MLAT_FUSION: str = os.getenv("MLAT_FUSION", "none").lower()

# ADS-B / coverage quality gates
# Additional range cap for first local-CPR positions (before global pairing).
ADSB_LOCAL_ENTRY_MAX_RANGE_NM: float = float(os.getenv("ADSB_LOCAL_ENTRY_MAX_RANGE_NM", "300"))
# Maximum implied groundspeed for position-to-position plausibility checks.
ADSB_MAX_IMPLIED_SPEED_KT: float = float(os.getenv("ADSB_MAX_IMPLIED_SPEED_KT", "750"))
# Freshness gates used when persisting coverage altitude values.
POS_FRESH_S: float = float(os.getenv("POS_FRESH_S", "15"))
ALT_FRESH_S: float = float(os.getenv("ALT_FRESH_S", "20"))

# How often to flush buffered aircraft_registry upserts to disk (seconds).
# Buffers write_minute()'s per-aircraft upserts so SD writes happen at most
# once per interval rather than once per minute per aircraft.
REGISTRY_FLUSH_SECONDS: float = float(os.getenv("REGISTRY_FLUSH_SECONDS", "300"))

# readsb aircraft JSON source for position QA checker.
READSB_AIRCRAFT_JSON_PATH: str = os.getenv("READSB_AIRCRAFT_JSON_PATH", "/run/readsb/aircraft.json")
READSB_AIRCRAFT_JSON_URL: str = os.getenv("READSB_AIRCRAFT_JSON_URL", "http://adsbpi.local/tar1090/data/aircraft.json")

# ---------------------------------------------------------------------------
# Ingest mode (Phase 3)
# ---------------------------------------------------------------------------
# beast   — decode raw Beast TCP stream (current behaviour, default)
# readsb  — poll readsb JSON files; no Beast connection
# hybrid  — readsb JSON for positions + Beast/MLAT for ACAS/raw DF counts
INGEST_MODE: str = os.getenv("INGEST_MODE", "beast").lower()
# Directory written by readsb (aircraft.json, stats.json, receiver.json).
READSB_JSON_DIR: str = os.getenv("READSB_JSON_DIR", "/run/readsb")
# Path to airspy_adsb stats.json — optional, only present on Airspy SDR installs.
AIRSPY_STATS_PATH: str = os.getenv("AIRSPY_STATS_PATH", "/run/airspy_adsb/stats.json")
# How often to poll aircraft.json (seconds); should match readsb's --write-json interval.
READSB_POLL_INTERVAL_S: float = float(os.getenv("READSB_POLL_INTERVAL_S", "1.0"))

# Maximum range (nm) from the receiver for accepted ADS-B positions.
# Mirrors readsb's receiver_range config; 300 nm is a typical ADS-B horizon.
MAX_RANGE_NM: float = float(os.getenv("MAX_RANGE_NM", "300"))

# ---------------------------------------------------------------------------
# Hi-res timelapse buffer
# ---------------------------------------------------------------------------
# How long to retain in-memory position samples.  12 h is the default
# (down from the original 24 h) to halve worst-case RAM usage.
HIRES_MAX_AGE_S: int = int(os.getenv("HIRES_MAX_AGE_S", "43200"))
# Hard cap on total points across all ICAOs.  When hit, new samples are
# dropped until old data ages out.  500 k points ≈ 100–200 MB Python RSS
# depending on GC pressure; reduce if running on a constrained device.
# Set to 0 or leave unset to disable the cap and rely solely on age-based eviction.
_hmp = os.getenv("HIRES_MAX_POINTS", "")
HIRES_MAX_POINTS: int | None = int(_hmp) if _hmp.strip() and int(_hmp) > 0 else None

# Per-client WebSocket send timeout.  Clients that can't accept a payload
# within this window are disconnected — prevents one stalled browser from
# blocking the broadcast to all other clients.
WS_SEND_TIMEOUT_S: float = float(os.getenv("WS_SEND_TIMEOUT_S", "2.0"))

# How often to push a snapshot to WebSocket clients (seconds).
# Default 1.0 s.  Pi deployments can set 1.5 or 2.0 to halve broadcast CPU.
PUSH_INTERVAL_S: float = float(os.getenv("PUSH_INTERVAL_S", "1.0"))

# Force snapshot mode for broadcast, overriding the memory-policy auto-detection.
# Valid values: full, reduced, thin.  Leave empty to use auto-detection (default).
SNAPSHOT_MODE_OVERRIDE: str = os.getenv("SNAPSHOT_MODE_OVERRIDE", "").lower()

# Decoder batch size: baseline number of messages drained from the queue per
# lock acquisition. Under backlog the worker may adapt upward to the max below.
# Higher values reduce queue/GIL overhead at the cost of longer per-batch
# latency. Set to 1 to restore near single-message behaviour when adaptive
# growth is also disabled.
DECODE_BATCH_SIZE: int = int(os.getenv("DECODE_BATCH_SIZE", "16"))
DECODE_BATCH_SIZE_MAX: int = int(os.getenv("DECODE_BATCH_SIZE_MAX", "128"))
DECODE_BATCH_BACKLOG_THRESHOLD: int = int(os.getenv("DECODE_BATCH_BACKLOG_THRESHOLD", "64"))
RADAR_UPDATE_MAX_IIDS: int = int(os.getenv("RADAR_UPDATE_MAX_IIDS", "8"))
RADAR_UPDATE_BUDGET_MS: float = float(os.getenv("RADAR_UPDATE_BUDGET_MS", "750"))
RADAR_IID_WS_REBUILD_INTERVAL_S: float = float(os.getenv("RADAR_IID_WS_REBUILD_INTERVAL_S", "5.0"))
RADAR_COINCIDENT_BACKGROUND_ENABLED: bool = _bool("RADAR_COINCIDENT_BACKGROUND_ENABLED", False)

# Enable radar diagnostics endpoints and hot-path debug instrumentation.
# When False (default): flash events, burst sync timeline, and other
# diagnostics-only structures are not populated, saving CPU and memory.
# Set to True (or "1") on development/server deployments for full diagnostics.
RADAR_DIAGNOSTICS: bool = _bool("RADAR_DIAGNOSTICS", False)

# Live radar sync refinement feature flags.
# period refinement: derive a small correction to period_s from the residual
# slope of recent multi-aircraft burst observations, on top of the aggregate
# estimator's base period.
RADAR_SYNC_PERIOD_REFINE_ENABLED: bool = _bool("RADAR_SYNC_PERIOD_REFINE_ENABLED", True)
# waveform correction: learn an empirical phase-in-rotation residual waveform
# and subtract it when predicting bearing from arrival time.
RADAR_SYNC_WAVEFORM_ENABLED: bool = _bool("RADAR_SYNC_WAVEFORM_ENABLED", True)
# propagation delay: subtract aircraft-to-receiver (and, where available,
# radar-to-aircraft) light-time from observation arrival timestamps before
# using them in the sync model.
RADAR_SYNC_PROP_DELAY_ENABLED: bool = _bool("RADAR_SYNC_PROP_DELAY_ENABLED", True)
# aircraft motion compensation: subtract the first-order beam-crossing shift
# caused by aircraft angular motion relative to the radar.  Phase prediction
# and period fitting are separately gated so diagnostics can keep comparing
# both paths even if one side is disabled.
RADAR_SYNC_MOTION_COMP_PHASE_ENABLED: bool = _bool("RADAR_SYNC_MOTION_COMP_PHASE_ENABLED", True)
RADAR_SYNC_MOTION_COMP_FIT_ENABLED: bool = _bool("RADAR_SYNC_MOTION_COMP_FIT_ENABLED", True)
# Number of circular bins for the phase-in-rotation waveform model.
RADAR_SYNC_WAVEFORM_BIN_COUNT: int = int(os.getenv("RADAR_SYNC_WAVEFORM_BIN_COUNT", "24"))

# ---------------------------------------------------------------------------
# Stage 3 — aircraft localisation from known radar bearings
# ---------------------------------------------------------------------------
STAGE3_ENABLED: bool = _bool("STAGE3_ENABLED", False)
STAGE3_MIN_CALIBRATION_SAMPLES: int = int(os.getenv("STAGE3_MIN_CALIBRATION_SAMPLES", "10"))
STAGE3_MIN_RADARS_FOR_FIX: int = int(os.getenv("STAGE3_MIN_RADARS_FOR_FIX", "2"))
STAGE3_MAX_CEP_M: float = float(os.getenv("STAGE3_MAX_CEP_M", "50000"))
STAGE3_CALIBRATION_INTERVAL_S: float = float(os.getenv("STAGE3_CALIBRATION_INTERVAL_S", "60"))
STAGE3_LOCALISATION_INTERVAL_S: float = float(os.getenv("STAGE3_LOCALISATION_INTERVAL_S", "10"))
STAGE3_STABLE_CALIBRATION_SAMPLES: int = int(os.getenv("STAGE3_STABLE_CALIBRATION_SAMPLES", "50"))
# Live-path operational settings
STAGE3_MAX_TARGETS_PER_CYCLE: int = int(os.getenv("STAGE3_MAX_TARGETS_PER_CYCLE", "20"))
STAGE3_MAX_RAYS_PER_TARGET: int = int(os.getenv("STAGE3_MAX_RAYS_PER_TARGET", "6"))
STAGE3_MAX_PAIRWISE_INTERSECTIONS: int = int(os.getenv("STAGE3_MAX_PAIRWISE_INTERSECTIONS", "50"))
STAGE3_MAX_SOLVER_ITERS: int = int(os.getenv("STAGE3_MAX_SOLVER_ITERS", "10"))
STAGE3_RAY_RETENTION_S: float = float(os.getenv("STAGE3_RAY_RETENTION_S", "30"))
STAGE3_LIVE_DETECTION_RETENTION_S: float = float(os.getenv("STAGE3_LIVE_DETECTION_RETENTION_S", "30"))
STAGE3_MIN_SYNC_QUALITY: float = float(os.getenv("STAGE3_MIN_SYNC_QUALITY", "0.3"))
STAGE3_USE_DF11: bool = _bool("STAGE3_USE_DF11", True)

# Queue-depth thresholds for CPU-pressure-triggered snapshot degradation.
# When the median queue depth over a 6-cycle window exceeds a threshold the
# snapshot mode escalates (same levels as memory pressure: elevated/high/critical).
# Set a threshold to 0 to disable that level.  Defaults are tuned for Pi 4.
QUEUE_PRESSURE_ELEVATED: int = int(os.getenv("QUEUE_PRESSURE_ELEVATED", "200"))
QUEUE_PRESSURE_HIGH:     int = int(os.getenv("QUEUE_PRESSURE_HIGH",     "1000"))
QUEUE_PRESSURE_CRITICAL: int = int(os.getenv("QUEUE_PRESSURE_CRITICAL", "3000"))

# Enable adaptive memory pressure policy (reads /proc/meminfo every 10 s).
# Set to "false" to lock the system to normal/full-retention mode regardless
# of available RAM.  Has no effect on non-Linux hosts.
MEMORY_POLICY_ENABLED: bool = os.getenv("MEMORY_POLICY_ENABLED", "true").lower() not in ("false", "0", "no")

# Enable SRTM terrain downloads and the terrain overlay in the 3D coverage view.
# Set to "false" on systems with limited storage (SRTM tiles are ~25 MB each;
# a full 400 nm radius requires up to ~350 tiles).  When disabled, the terrain
# button is hidden in the UI and no tile downloads or grid processing occur.
TERRAIN_ENABLED: bool = os.getenv("TERRAIN_ENABLED", "true").lower() not in ("false", "0", "no")
