import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

BEAST_HOST: str = os.getenv("BEAST_HOST", "localhost")
BEAST_PORT: int = int(os.getenv("BEAST_PORT", "30005"))
AIRCRAFT_TIMEOUT: int = int(os.getenv("AIRCRAFT_TIMEOUT", "60"))

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
