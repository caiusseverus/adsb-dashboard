"""
Aircraft enrichment database.

Primary source (bulk download, cached):
  - ADSBExchange basic-ac-db — reg, type, owner/operator, manufacture year, military flag
    Global coverage. Updated every 7 days.

Supplementary sources (bulk download, cached):
  - tar1090-db operators.js         — ICAO callsign prefix → airline name + country
  - tar1090-db icao_aircraft_types.js  — type code → WTC + category
  - tar1090-db icao_aircraft_types2.js — type code → full name

Fallback source (on-demand API, results cached):
  - hexdb.io — registered owners for aircraft absent from ADSBExchange

Operator resolution order (each step overrides the previous):
  1. ADSBExchange ownop       — set at aircraft creation
  2. hexdb.io RegisteredOwners / OperatorFlagCode — set asynchronously (background task)
  3. operators.js callsign lookup — set when callsign decoded (cleanest airline names)
"""

import bisect
import gzip
import io
import json
import logging
import re
import time
import urllib.request
from typing import Optional


import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ICAO 24-bit address block → registration country
# Source: https://www.aerotransport.org/html/ICAO_hex_decode.html
# Sorted by start address; unallocated/reserved blocks omitted.
# ---------------------------------------------------------------------------
_ICAO_COUNTRY_RANGES: list[tuple[int, int, str]] = [
    (0x004000, 0x0043FF, "Zimbabwe"),
    (0x006000, 0x006FFF, "Mozambique"),
    (0x008000, 0x00FFFF, "South Africa"),
    (0x010000, 0x017FFF, "Egypt"),
    (0x018000, 0x01FFFF, "Libya"),
    (0x020000, 0x027FFF, "Morocco"),
    (0x028000, 0x02FFFF, "Tunisia"),
    (0x030000, 0x0303FF, "Botswana"),
    (0x032000, 0x032FFF, "Burundi"),
    (0x034000, 0x034FFF, "Cameroon"),
    (0x035000, 0x0353FF, "Comoros"),
    (0x036000, 0x036FFF, "Congo"),
    (0x038000, 0x038FFF, "Côte d'Ivoire"),
    (0x03E000, 0x03EFFF, "Gabon"),
    (0x040000, 0x040FFF, "Ethiopia"),
    (0x042000, 0x042FFF, "Equatorial Guinea"),
    (0x044000, 0x044FFF, "Ghana"),
    (0x046000, 0x046FFF, "Guinea"),
    (0x048000, 0x0483FF, "Guinea-Bissau"),
    (0x04A000, 0x04A3FF, "Lesotho"),
    (0x04C000, 0x04CFFF, "Kenya"),
    (0x050000, 0x050FFF, "Liberia"),
    (0x054000, 0x054FFF, "Madagascar"),
    (0x058000, 0x058FFF, "Malawi"),
    (0x05A000, 0x05A3FF, "Maldives"),
    (0x05C000, 0x05CFFF, "Mali"),
    (0x05E000, 0x05E3FF, "Mauritania"),
    (0x060000, 0x0603FF, "Mauritius"),
    (0x062000, 0x062FFF, "Niger"),
    (0x064000, 0x064FFF, "Nigeria"),
    (0x068000, 0x068FFF, "Uganda"),
    (0x06A000, 0x06A3FF, "Qatar"),
    (0x06C000, 0x06CFFF, "Central African Republic"),
    (0x06E000, 0x06EFFF, "Rwanda"),
    (0x070000, 0x070FFF, "Senegal"),
    (0x074000, 0x0743FF, "Seychelles"),
    (0x076000, 0x0763FF, "Sierra Leone"),
    (0x078000, 0x078FFF, "Somalia"),
    (0x07A000, 0x07A3FF, "Eswatini"),
    (0x07C000, 0x07CFFF, "Sudan"),
    (0x080000, 0x080FFF, "Tanzania"),
    (0x084000, 0x084FFF, "Chad"),
    (0x088000, 0x088FFF, "Togo"),
    (0x08A000, 0x08AFFF, "Zambia"),
    (0x08C000, 0x08CFFF, "DR Congo"),
    (0x090000, 0x090FFF, "Angola"),
    (0x094000, 0x0943FF, "Benin"),
    (0x096000, 0x0963FF, "Cape Verde"),
    (0x098000, 0x0983FF, "Djibouti"),
    (0x09A000, 0x09AFFF, "Gambia"),
    (0x09C000, 0x09CFFF, "Burkina Faso"),
    (0x09E000, 0x09E3FF, "São Tomé and Príncipe"),
    (0x0A0000, 0x0A7FFF, "Algeria"),
    (0x0A8000, 0x0A8FFF, "Bahamas"),
    (0x0AA000, 0x0AA3FF, "Barbados"),
    (0x0AB000, 0x0AB3FF, "Belize"),
    (0x0AC000, 0x0ACFFF, "Colombia"),
    (0x0AE000, 0x0AEFFF, "Costa Rica"),
    (0x0B0000, 0x0B0FFF, "Cuba"),
    (0x0B2000, 0x0B2FFF, "El Salvador"),
    (0x0B4000, 0x0B4FFF, "Guatemala"),
    (0x0B6000, 0x0B6FFF, "Guyana"),
    (0x0B8000, 0x0B8FFF, "Haiti"),
    (0x0BA000, 0x0BAFFF, "Honduras"),
    (0x0BC000, 0x0BC3FF, "St. Vincent and Grenadines"),
    (0x0BE000, 0x0BEFFF, "Jamaica"),
    (0x0C0000, 0x0C0FFF, "Nicaragua"),
    (0x0C2000, 0x0C2FFF, "Panama"),
    (0x0C4000, 0x0C4FFF, "Dominican Republic"),
    (0x0C6000, 0x0C6FFF, "Trinidad and Tobago"),
    (0x0C8000, 0x0C8FFF, "Suriname"),
    (0x0CA000, 0x0CA3FF, "Antigua and Barbuda"),
    (0x0CC000, 0x0CC3FF, "Grenada"),
    (0x0D0000, 0x0D7FFF, "Mexico"),
    (0x0D8000, 0x0DFFFF, "Venezuela"),
    (0x100000, 0x1FFFFF, "Russia"),
    (0x201000, 0x2013FF, "Namibia"),
    (0x202000, 0x2023FF, "Eritrea"),
    (0x300000, 0x33FFFF, "Italy"),
    (0x340000, 0x37FFFF, "Spain"),
    (0x380000, 0x3BFFFF, "France"),
    (0x3C0000, 0x3FFFFF, "Germany"),
    (0x400000, 0x43FFFF, "United Kingdom"),
    (0x440000, 0x447FFF, "Austria"),
    (0x448000, 0x44FFFF, "Belgium"),
    (0x450000, 0x457FFF, "Bulgaria"),
    (0x458000, 0x45FFFF, "Denmark"),
    (0x460000, 0x467FFF, "Finland"),
    (0x468000, 0x46FFFF, "Greece"),
    (0x470000, 0x477FFF, "Hungary"),
    (0x478000, 0x47FFFF, "Norway"),
    (0x480000, 0x487FFF, "Netherlands"),
    (0x488000, 0x48FFFF, "Poland"),
    (0x490000, 0x497FFF, "Portugal"),
    (0x498000, 0x49FFFF, "Czech Republic"),
    (0x4A0000, 0x4A7FFF, "Romania"),
    (0x4A8000, 0x4AFFFF, "Sweden"),
    (0x4B0000, 0x4B7FFF, "Switzerland"),
    (0x4B8000, 0x4BFFFF, "Turkey"),
    (0x4C0000, 0x4C7FFF, "Serbia"),
    (0x4C8000, 0x4C83FF, "Cyprus"),
    (0x4CA000, 0x4CAFFF, "Ireland"),
    (0x4CC000, 0x4CCFFF, "Iceland"),
    (0x4D0000, 0x4D03FF, "Luxembourg"),
    (0x4D2000, 0x4D23FF, "Malta"),
    (0x4D4000, 0x4D43FF, "Monaco"),
    (0x500000, 0x5004FF, "San Marino"),
    (0x501000, 0x5013FF, "Albania"),
    (0x501C00, 0x501FFF, "Croatia"),
    (0x502C00, 0x502FFF, "Latvia"),
    (0x503C00, 0x503FFF, "Lithuania"),
    (0x504C00, 0x504FFF, "Moldova"),
    (0x505C00, 0x505FFF, "Slovakia"),
    (0x506C00, 0x506FFF, "Slovenia"),
    (0x507C00, 0x507FFF, "Uzbekistan"),
    (0x508000, 0x50FFFF, "Ukraine"),
    (0x510000, 0x5103FF, "Belarus"),
    (0x511000, 0x5113FF, "Estonia"),
    (0x512000, 0x5123FF, "North Macedonia"),
    (0x513000, 0x5133FF, "Bosnia and Herzegovina"),
    (0x514000, 0x5143FF, "Georgia"),
    (0x515000, 0x5153FF, "Tajikistan"),
    (0x600000, 0x6003FF, "Armenia"),
    (0x600800, 0x600BFF, "Azerbaijan"),
    (0x601000, 0x6013FF, "Kyrgyzstan"),
    (0x601800, 0x601BFF, "Turkmenistan"),
    (0x680000, 0x6803FF, "Bhutan"),
    (0x681000, 0x6813FF, "Micronesia"),
    (0x682000, 0x6823FF, "Mongolia"),
    (0x683000, 0x6833FF, "Kazakhstan"),
    (0x684000, 0x6843FF, "Palau"),
    (0x700000, 0x700FFF, "Afghanistan"),
    (0x702000, 0x702FFF, "Bangladesh"),
    (0x704000, 0x704FFF, "Myanmar"),
    (0x706000, 0x706FFF, "Kuwait"),
    (0x708000, 0x708FFF, "Laos"),
    (0x70A000, 0x70AFFF, "Nepal"),
    (0x70C000, 0x70C3FF, "Oman"),
    (0x70E000, 0x70EFFF, "Cambodia"),
    (0x710000, 0x717FFF, "Saudi Arabia"),
    (0x718000, 0x71FFFF, "South Korea"),
    (0x720000, 0x727FFF, "North Korea"),
    (0x728000, 0x72FFFF, "Iraq"),
    (0x730000, 0x737FFF, "Iran"),
    (0x738000, 0x73FFFF, "Israel"),
    (0x740000, 0x747FFF, "Jordan"),
    (0x748000, 0x74FFFF, "Lebanon"),
    (0x750000, 0x757FFF, "Malaysia"),
    (0x758000, 0x75FFFF, "Philippines"),
    (0x760000, 0x767FFF, "Pakistan"),
    (0x768000, 0x76FFFF, "Singapore"),
    (0x770000, 0x777FFF, "Sri Lanka"),
    (0x778000, 0x77FFFF, "Syria"),
    (0x780000, 0x7BFFFF, "China"),
    (0x7C0000, 0x7FFFFF, "Australia"),
    (0x800000, 0x83FFFF, "India"),
    (0x840000, 0x87FFFF, "Japan"),
    (0x880000, 0x887FFF, "Thailand"),
    (0x888000, 0x88FFFF, "Vietnam"),
    (0x890000, 0x890FFF, "Yemen"),
    (0x894000, 0x894FFF, "Bahrain"),
    (0x895000, 0x8953FF, "Brunei"),
    (0x896000, 0x896FFF, "United Arab Emirates"),
    (0x897000, 0x8973FF, "Solomon Islands"),
    (0x898000, 0x898FFF, "Papua New Guinea"),
    (0x899000, 0x8993FF, "Taiwan"),
    (0x8A0000, 0x8A7FFF, "Indonesia"),
    (0x900000, 0x9003FF, "Marshall Islands"),
    (0x901000, 0x9013FF, "Cook Islands"),
    (0x902000, 0x9023FF, "Samoa"),
    (0xA00000, 0xAFFFFF, "United States"),
    (0xC00000, 0xC3FFFF, "Canada"),
    (0xC80000, 0xC87FFF, "New Zealand"),
    (0xC88000, 0xC88FFF, "Fiji"),
    (0xC8A000, 0xC8A3FF, "Nauru"),
    (0xC8C000, 0xC8C3FF, "Saint Lucia"),
    (0xC8D000, 0xC8D3FF, "Tonga"),
    (0xC8E000, 0xC8E3FF, "Kiribati"),
    (0xC90000, 0xC903FF, "Vanuatu"),
    (0xE00000, 0xE3FFFF, "Argentina"),
    (0xE40000, 0xE7FFFF, "Brazil"),
    (0xE80000, 0xE80FFF, "Chile"),
    (0xE84000, 0xE84FFF, "Ecuador"),
    (0xE88000, 0xE88FFF, "Paraguay"),
    (0xE8C000, 0xE8CFFF, "Peru"),
    (0xE90000, 0xE90FFF, "Uruguay"),
    (0xE94000, 0xE94FFF, "Bolivia"),
]

_CR_LO   = [r[0] for r in _ICAO_COUNTRY_RANGES]
_CR_HI   = [r[1] for r in _ICAO_COUNTRY_RANGES]
_CR_NAME = [r[2] for r in _ICAO_COUNTRY_RANGES]

# ---------------------------------------------------------------------------
# URL / cache constants
# ---------------------------------------------------------------------------

# ADSBExchange (primary per-aircraft data)
_ADSBX_URL   = "https://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
_ADSBX_RAW   = "adsbx_db.json.gz"    # raw downloaded file
_ADSBX_CACHE = "adsbx_cache.json.gz" # processed compact cache
_ADSBX_MAX_AGE = 7 * 86400           # re-download after 7 days

# tar1090-db supplementary files (operators + type info + per-aircraft shards)
_DB_BASE_URL   = "https://github.com/wiedehopf/tar1090-db/raw/refs/heads/master/db"
_OPERATORS_URL = f"{_DB_BASE_URL}/operators.js"
_TYPES_URL     = f"{_DB_BASE_URL}/icao_aircraft_types.js"
_TYPES2_URL    = f"{_DB_BASE_URL}/icao_aircraft_types2.js"
_AUX_MAX_AGE        = 30 * 86400     # re-download aux files after 30 days
_TAR1090_SHARD_AGE  = 30 * 86400     # re-download shard files after 30 days

# hexdb.io (on-demand fallback)
_HEXDB_BASE       = "https://hexdb.io/api/v1/aircraft"
_HEXDB_CACHE_FILE = "hexdb_cache.json.gz"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "adsb-dashboard"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _decompress(raw: bytes) -> str:
    return gzip.decompress(raw).decode("utf-8")



def _cache_age(filename: str) -> float:
    """Return seconds since a cache file was last modified, or infinity if absent."""
    path = config.DATA_DIR / filename
    return time.time() - path.stat().st_mtime if path.exists() else float("inf")


# ---------------------------------------------------------------------------
# EnrichmentDB
# ---------------------------------------------------------------------------

class EnrichmentDB:
    def __init__(self) -> None:
        # ADSBExchange: ICAO_UPPER → {reg, icaotype, ownop, year, mil, manufacturer, model}
        self._adsbx: dict[str, dict] = {}
        # tar1090-db auxiliary
        self._operators: dict[str, dict] = {}
        self._type_info: dict[str, dict] = {}
        # tar1090-db per-aircraft shards: shard_key (e.g. "40") → {icao_suffix → entry}
        # Loaded on demand; empty-dict sentinel means the shard was tried and had no data.
        self._tar1090_shards: dict[str, dict] = {}
        # hexdb.io persistent cache: ICAO_UPPER → response dict (successes only)
        self._hexdb_cache: dict[str, dict] = {}
        # ICAOs that returned no data this session — not persisted, retried on restart
        self._hexdb_session_misses: set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_or_download(self) -> None:
        """Load all enrichment data from cache, downloading anything missing."""
        self._load_or_download_adsbx()
        for fname, url in [
            ("operators.js",            _OPERATORS_URL),
            ("icao_aircraft_types.js",  _TYPES_URL),
            ("icao_aircraft_types2.js", _TYPES2_URL),
        ]:
            if (config.DATA_DIR / fname).exists():
                self._load_aux_file(fname)
            else:
                self._download_aux(fname, url)
        self._load_hexdb_cache()

    def check_for_updates(self) -> None:
        """Re-download stale files: ADSBExchange after 7 days, aux files after 30 days."""
        if _cache_age(_ADSBX_CACHE) > _ADSBX_MAX_AGE:
            log.info("Enrichment: ADSBExchange cache stale — re-downloading")
            self._download_adsbx()
        for fname, url in [
            ("operators.js",            _OPERATORS_URL),
            ("icao_aircraft_types.js",  _TYPES_URL),
            ("icao_aircraft_types2.js", _TYPES2_URL),
        ]:
            if _cache_age(fname) > _AUX_MAX_AGE:
                log.info("Enrichment: %s stale — re-downloading", fname)
                self._download_aux(fname, url)

    def get_adsbx(self, icao: str) -> Optional[dict]:
        """Return ADSBExchange record for an ICAO, or None."""
        return self._adsbx.get(icao.upper())

    def get_operator(self, prefix: str) -> Optional[dict]:
        """Look up 3-letter ICAO airline designator; returns operator dict or None."""
        return self._operators.get(prefix.upper())

    def get_type_info(self, type_code: str) -> Optional[dict]:
        """Returns {"name": str, "desc": str, "wtc": str} or None."""
        return self._type_info.get(type_code.upper()) if type_code else None

    def is_military(self, icao: str) -> bool:
        """Return True if ADSBExchange flags this aircraft military."""
        adsbx = self._adsbx.get(icao.upper())
        return bool(adsbx and adsbx.get("mil"))

    def get_country_by_icao(self, icao: str) -> Optional[str]:
        """Return registration country from ICAO address block allocation, or None."""
        try:
            val = int(icao, 16)
        except ValueError:
            return None
        idx = bisect.bisect_right(_CR_LO, val) - 1
        if idx < 0:
            return None
        return _CR_NAME[idx] if val <= _CR_HI[idx] else None

    def get_hexdb_cached(self, icao: str) -> Optional[dict]:
        """Return a previously cached successful hexdb lookup, or None (no HTTP call)."""
        return self._hexdb_cache.get(icao.upper())

    def get_tar1090_cached(self, icao: str) -> Optional[dict]:
        """Return tar1090-db data if the relevant shard is already in memory, else None.
        Never triggers a download — safe to call from hot paths."""
        key = icao.upper()
        shard = key[:2].lower()
        shard_data = self._tar1090_shards.get(shard)
        if shard_data is None:
            return None
        return self._tar1090_lookup(key, shard_data)

    def get_tar1090(self, icao: str) -> Optional[dict]:
        """Return tar1090-db data for an aircraft, downloading the shard if needed.
        Intended to be called via asyncio.to_thread (blocking I/O)."""
        key = icao.upper()
        shard = key[:2].lower()
        if shard not in self._tar1090_shards:
            self._load_tar1090_shard(shard)
        shard_data = self._tar1090_shards.get(shard)
        if not shard_data:
            return None
        return self._tar1090_lookup(key, shard_data)

    def _tar1090_lookup(self, icao_upper: str, shard_data: dict) -> Optional[dict]:
        """Extract aircraft entry from a loaded shard and normalise to hexdb field names."""
        suffix = icao_upper[2:]  # last 4 chars, e.g. "6DA1"
        entry = shard_data.get(suffix) or shard_data.get(suffix.lower())
        if not entry or not isinstance(entry, list):
            return None
        reg   = entry[0] if len(entry) > 0 else ""
        tcode = entry[1] if len(entry) > 1 else ""
        owner = entry[2] if len(entry) > 2 else ""
        if not any([reg, tcode, owner]):
            return None
        return {
            "Registration":    reg   or "",
            "ICAOTypeCode":    tcode or "",
            "RegisteredOwners": owner or "",
        }

    def _load_tar1090_shard(self, shard: str) -> None:
        """Download (or load from cache) a tar1090-db shard file and store in memory."""
        cache_file = f"tar1090_shard_{shard}.json.gz"
        if (config.DATA_DIR / cache_file).exists():
            if _cache_age(cache_file) < _TAR1090_SHARD_AGE:
                try:
                    raw = gzip.decompress((config.DATA_DIR / cache_file).read_bytes())
                    self._tar1090_shards[shard] = json.loads(raw.decode("utf-8"))
                    log.debug("tar1090 shard %s loaded from cache (%d entries)", shard, len(self._tar1090_shards[shard]))
                    return
                except Exception as exc:
                    log.warning("tar1090 shard cache %s unreadable: %s — re-downloading", shard, exc)
        url = f"{_DB_BASE_URL}/{shard}.js"
        try:
            raw = _fetch(url, timeout=15)
            text = _decompress(raw)
            data = json.loads(text)
            self._tar1090_shards[shard] = data
            log.debug("tar1090 shard %s downloaded (%d entries)", shard, len(data))
            try:
                (config.DATA_DIR / cache_file).write_bytes(
                    gzip.compress(json.dumps(data).encode("utf-8"))
                )
            except Exception as exc:
                log.warning("tar1090 shard %s cache write failed: %s", shard, exc)
        except Exception as exc:
            log.debug("tar1090 shard %s not available: %s", shard, exc)
            self._tar1090_shards[shard] = {}  # sentinel: tried, no data

    def lookup_hexdb(self, icao: str) -> Optional[dict]:
        """
        Synchronous hexdb.io aircraft lookup with persistent cache.
        Intended to be called via asyncio.to_thread.
        Only successful lookups are persisted; failures are tracked in-memory
        only and retried on the next app restart.
        """
        key = icao.upper()
        if key in self._hexdb_cache:
            return self._hexdb_cache[key]
        if key in self._hexdb_session_misses:
            return None
        try:
            data = json.loads(_fetch(f"{_HEXDB_BASE}/{icao.lower()}"))
        except Exception as exc:
            log.debug("hexdb lookup failed for %s: %s", icao, exc)
            self._hexdb_session_misses.add(key)
            return None
        if not data:
            self._hexdb_session_misses.add(key)
            return None
        self._hexdb_cache[key] = data
        self._save_hexdb_cache()
        return data

    def force_lookup_hexdb(self, icao: str) -> Optional[dict]:
        """Force a fresh hexdb.io HTTP request, bypassing cache.
        Updates the persistent cache with the result.
        Use for manual refresh — do not call from the background rate-limited queue."""
        key = icao.upper()
        try:
            data = json.loads(_fetch(f"{_HEXDB_BASE}/{icao.lower()}"))
        except Exception as exc:
            log.debug("hexdb force lookup failed for %s: %s", icao, exc)
            return None
        if not data:
            return None
        self._hexdb_cache[key] = data
        self._hexdb_session_misses.discard(key)
        self._save_hexdb_cache()
        return data

    # ------------------------------------------------------------------
    # ADSBExchange
    # ------------------------------------------------------------------

    def _load_or_download_adsbx(self) -> None:
        if (config.DATA_DIR / _ADSBX_CACHE).exists():
            self._load_adsbx_cache()
        elif (config.DATA_DIR / _ADSBX_RAW).exists():
            log.info("Enrichment: parsing cached ADSBExchange raw file")
            self._parse_adsbx_raw((config.DATA_DIR / _ADSBX_RAW).read_bytes())
        else:
            self._download_adsbx()

    def _download_adsbx(self) -> None:
        log.info("Enrichment: downloading ADSBExchange database…")
        try:
            raw = _fetch(_ADSBX_URL, timeout=120)
            (config.DATA_DIR / _ADSBX_RAW).write_bytes(raw)
            log.info("Enrichment: ADSBExchange downloaded (%d bytes)", len(raw))
            self._parse_adsbx_raw(raw)
        except Exception as exc:
            log.error("Enrichment: failed to download ADSBExchange: %s", exc)

    def _parse_adsbx_raw(self, raw_gz: bytes) -> None:
        data: dict[str, dict] = {}
        try:
            with gzip.open(io.BytesIO(raw_gz)) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    icao = (rec.get("icao") or "").upper()
                    if len(icao) != 6:
                        continue
                    data[icao] = {
                        "reg":          rec.get("reg") or "",
                        "icaotype":     rec.get("icaotype") or "",
                        "ownop":        rec.get("ownop") or "",
                        "year":         rec.get("year") or "",
                        "mil":          bool(rec.get("mil", False)),
                        "manufacturer": rec.get("manufacturer") or "",
                        "model":        rec.get("model") or "",
                        "short_type":   rec.get("short_type") or "",
                    }
        except Exception as exc:
            log.error("Enrichment: failed to parse ADSBExchange data: %s", exc)
            return
        self._adsbx = data
        log.info("Enrichment: loaded %d ADSBExchange records", len(data))
        try:
            (config.DATA_DIR / _ADSBX_CACHE).write_bytes(
                gzip.compress(json.dumps(data).encode("utf-8"))
            )
            log.info("Enrichment: ADSBExchange cache written")
        except Exception as exc:
            log.warning("Enrichment: could not write ADSBExchange cache: %s", exc)

    def _load_adsbx_cache(self) -> None:
        try:
            raw = (config.DATA_DIR / _ADSBX_CACHE).read_bytes()
            self._adsbx = json.loads(gzip.decompress(raw).decode("utf-8"))
            log.info("Enrichment: loaded %d ADSBExchange records from cache", len(self._adsbx))
        except Exception as exc:
            log.warning("Enrichment: ADSBExchange cache load failed (%s) — re-downloading", exc)
            self._download_adsbx()

    # ------------------------------------------------------------------
    # tar1090-db auxiliary files (operators + type info)
    # ------------------------------------------------------------------

    def _load_aux_file(self, fname: str) -> None:
        raw = (config.DATA_DIR / fname).read_bytes()
        text = _decompress(raw)
        if fname == "operators.js":
            self._parse_operators(text)
        elif fname == "icao_aircraft_types.js":
            self._parse_types(text)
        elif fname == "icao_aircraft_types2.js":
            self._parse_types2(text)

    def _download_aux(self, fname: str, url: str) -> None:
        log.info("Enrichment: downloading %s", url)
        try:
            raw = _fetch(url)
            (config.DATA_DIR / fname).write_bytes(raw)
            self._load_aux_file(fname)
            log.info("Enrichment: %s downloaded (%d bytes)", fname, len(raw))
        except Exception as exc:
            log.error("Enrichment: failed to download %s: %s", fname, exc)

    def _parse_operators(self, text: str) -> None:
        self._operators = json.loads(text)
        log.info("Enrichment: loaded %d operator records", len(self._operators))

    def _parse_types(self, text: str) -> None:
        data = json.loads(text)
        for type_code, entry in data.items():
            key = type_code.upper()
            if key not in self._type_info:
                self._type_info[key] = {}
            self._type_info[key].setdefault("desc", entry.get("desc", ""))
            self._type_info[key].setdefault("wtc",  entry.get("wtc", ""))
        log.info("Enrichment: loaded %d type records (types.js)", len(data))

    def _parse_types2(self, text: str) -> None:
        data = json.loads(text)
        for type_code, entry in data.items():
            key = type_code.upper()
            if key not in self._type_info:
                self._type_info[key] = {}
            if isinstance(entry, list) and len(entry) >= 3:
                self._type_info[key]["name"] = entry[0]
                self._type_info[key]["desc"] = entry[1]
                self._type_info[key]["wtc"]  = entry[2]
        log.info("Enrichment: loaded %d type records (types2.js)", len(data))

    # ------------------------------------------------------------------
    # hexdb.io cache
    # ------------------------------------------------------------------

    def _load_hexdb_cache(self) -> None:
        path = config.DATA_DIR / _HEXDB_CACHE_FILE
        if not path.exists():
            return
        try:
            raw = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
            # Strip any empty-dict failure entries written by older versions
            self._hexdb_cache = {k: v for k, v in raw.items() if v}
            log.info("Enrichment: loaded %d hexdb cache entries", len(self._hexdb_cache))
        except Exception as exc:
            log.warning("Enrichment: hexdb cache load failed: %s", exc)

    def _save_hexdb_cache(self) -> None:
        try:
            (config.DATA_DIR / _HEXDB_CACHE_FILE).write_bytes(
                gzip.compress(json.dumps(self._hexdb_cache).encode("utf-8"))
            )
        except Exception as exc:
            log.warning("Enrichment: could not save hexdb cache: %s", exc)


_US_MIL_SERIAL_RE = re.compile(r'^\d{2}-\d+$')


def extract_us_mil_serial_year(registration: str) -> Optional[str]:
    """Extract manufacture year from a US military aircraft serial number.

    US military serials use the format YY-NNNN where YY is the two-digit
    fiscal year (e.g. '06-6160' → 2006, '99-0001' → 1999).
    Year expansion: 00–50 → 2000–2050, 51–99 → 1951–1999.
    Returns a 4-digit year string, or None if the registration doesn't match.
    """
    if not registration:
        return None
    if not _US_MIL_SERIAL_RE.match(registration.strip()):
        return None
    yy = int(registration.split('-')[0])
    return str(2000 + yy if yy <= 50 else 1900 + yy)


# Module-level singleton
db = EnrichmentDB()
