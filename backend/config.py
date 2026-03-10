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
# Helps filter bogus ICAO addresses from CRC decoding errors.
# Set to 0 to disable filtering. ADSBex/hexdb hit bypasses this check.
GHOST_FILTER_MSGS: int = int(os.getenv("GHOST_FILTER_MSGS", "5"))
MINUTE_STATS_RETENTION_DAYS: int = int(os.getenv("MINUTE_STATS_RETENTION_DAYS", "30"))

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH: Path = Path(os.getenv("DB_PATH", str(DATA_DIR / "adsb.db")))

# Allowed CORS origins. Defaults to the Vite dev server only; not needed in
# production because the frontend is served from the same origin as the backend.
_cors_raw = os.getenv("CORS_ORIGINS", "http://localhost:5173")
CORS_ORIGINS: list[str] = [o.strip() for o in _cors_raw.split(",") if o.strip()]
