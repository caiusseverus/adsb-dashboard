"""
SQLite persistence layer for ADS-B Dashboard.

Stores per-minute stats, per-day rollups, aircraft registry with notable flags.
All public methods are synchronous and safe to call via asyncio.to_thread().
"""

import logging
import sqlite3
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone

import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Curated "interesting" type codes
# Add any type code here to flag matching aircraft as interesting.
# ---------------------------------------------------------------------------
INTERESTING_TYPE_CODES: frozenset[str] = frozenset({
    # -----------------------------------------------------------------------
    # WWII warbirds — British
    # -----------------------------------------------------------------------
    "SPIT", "HURI", "LANC", "MOSQ", "MOS",  "BLEN", "BEAU",
    "TYPH", "TEMP", "HALI", "STIR", "WELD", "SUND",
    # WWII warbirds — American
    "P51",  "P47",  "B17",  "B24",  "B29",  "C47",  "F4U",
    "TBF",  "SBD",  "P38",  "P40",  "T6",
    # WWII warbirds — German/other
    "BF09", "FW19", "JU52", "HE11", "ME62",

    # -----------------------------------------------------------------------
    # Classic piston airliners (rare in service)
    # -----------------------------------------------------------------------
    "DC3",  "DC4",  "DC6",  "DC7",  "L049", "L749", "L1049",

    # -----------------------------------------------------------------------
    # Early jets & classic airliners (increasingly rare)
    # -----------------------------------------------------------------------
    "CONC",                          # Concorde (museum/static only)
    "B703", "B720",                  # Boeing 707/720
    "DC8",  "VC10", "TRID",          # DC-8, VC-10, Trident
    "B721", "B722",                  # Boeing 727-100/200
    "DC9",                           # Douglas DC-9
    "B741", "B742", "B743", "B74S",  # Boeing 747-100/200/300/SP
    "B748",                          # Boeing 747-8
    "MD11",                          # McDonnell Douglas MD-11
    "DC10",                          # McDonnell Douglas DC-10
    "F27",  "F50",  "F100",          # Fokker 27 / 50 / 100
    "BA46",                          # BAe 146 / Avro RJ
    "C141",                          # Lockheed C-141 Starlifter

    # -----------------------------------------------------------------------
    # Strategic airlifters & heavy military transport
    # -----------------------------------------------------------------------
    "A124", "A225",                  # Antonov An-124/225
    "AN22", "AN12", "AN26", "AN72",
    "AN32",                          # Antonov series
    "IL76", "IL78", "IL62", "IL96",  # Ilyushin series
    "C17",  "C5M",                   # C-17 Globemaster, C-5 Galaxy
    "C130", "C30J",                  # C-130 Hercules / C-130J
    "C27J", "CN35", "C295",          # C-27J, CN-235, C-295
    "A400",                          # Airbus A400M Atlas
    "L382",                          # Lockheed L-382 (civil C-130)
    "Y20",                           # Xian Y-20
    "CL44",                          # Canadair CL-44

    # -----------------------------------------------------------------------
    # Military tankers
    # -----------------------------------------------------------------------
    "K35R", "K35E",                  # KC-135 Stratotanker variants
    "KC10",                          # KC-10 Extender
    "KC46",                          # KC-46 Pegasus
    "KDC1",                          # KC-767

    # -----------------------------------------------------------------------
    # AEW&C / radar aircraft
    # -----------------------------------------------------------------------
    "E3TF",                          # E-3 Sentry AWACS
    "E767",                          # E-767 AWACS
    "E2",   "E2C",                   # E-2 Hawkeye
    "E737",                          # Boeing E-7A Wedgetail

    # -----------------------------------------------------------------------
    # SIGINT / electronic intelligence / maritime patrol
    # -----------------------------------------------------------------------
    "R135",                          # RC-135 Rivet Joint / Stratotanker
    "RC12",                          # RC-12 Guardrail
    "EP3E",                          # EP-3E Aries II
    "P3",                            # P-3 Orion
    "P8",                            # P-8 Poseidon

    # -----------------------------------------------------------------------
    # Strategic / reconnaissance / UAV
    # -----------------------------------------------------------------------
    "U2",                            # U-2 Dragon Lady
    "SR71",                          # SR-71 Blackbird (museum/research)
    "RQ4",                           # RQ-4 Global Hawk
    "MQ9",                           # MQ-9 Reaper
    "MQ1",                           # MQ-1 Predator

    # -----------------------------------------------------------------------
    # Strategic bombers
    # -----------------------------------------------------------------------
    "B52",                           # B-52 Stratofortress
    "B1B",                           # B-1B Lancer
    "B2",                            # B-2 Spirit
    "TU95",                          # Tupolev Tu-95 Bear
    "T160",                          # Tupolev Tu-160 Blackjack

    # -----------------------------------------------------------------------
    # Rare / oversized cargo
    # -----------------------------------------------------------------------
    "BLCF",                          # Boeing 747 Dreamlifter
    "A3ST",                          # Airbus Beluga / BelugaXL

    # -----------------------------------------------------------------------
    # Notable helicopters
    # -----------------------------------------------------------------------
    "MI26",                          # Mil Mi-26 Halo
    "MI8",  "MI17",                  # Mil Mi-8 / Mi-17
    "KA32", "KA27",                  # Kamov Ka-32 / Ka-27
    "CH47",                          # CH-47 Chinook
    "UH60",                          # UH-60 Black Hawk
    "S61",  "S64",                   # Sikorsky S-61 / S-64 Skycrane
    "EH10",                          # AgustaWestland AW101 Merlin
    "NH90",                          # NHIndustries NH90

    # -----------------------------------------------------------------------
    # Military trainers (often seen on ADS-B during sorties)
    # -----------------------------------------------------------------------
    "T33",  "T38",                   # T-33 / T-38 Talon
    "F86",  "MIG15","MIG21",         # Classic jets
    "HAWK",                          # BAE Hawk
    "L39",                           # Aero L-39 Albatros
    "PC21", "PC9",                   # Pilatus PC-21 / PC-9
    "VULT",                          # Vultee BT-13 Valiant

    # -----------------------------------------------------------------------
    # Rare / vintage turboprops
    # -----------------------------------------------------------------------
    "DHC3", "DHC4", "DHC6", "DHC7",  # DHC Otter, Caribou, Twin Otter, Dash 7
    "F27",                            # Fokker F27 Friendship (duplicate-safe)
    "P180",                           # Piaggio P.180 Avanti (distinctive pusher)

    # -----------------------------------------------------------------------
    # Other notable / unusual
    # -----------------------------------------------------------------------
    "BALL",                           # Balloon
})


class StatsDB:
    def __init__(self) -> None:
        self._last_written_ts: int = 0
        self._init_schema()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(config.DB_PATH), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS minute_stats (
                    ts          INTEGER PRIMARY KEY,
                    msg_min     REAL    NOT NULL,
                    msg_max     REAL    NOT NULL,
                    msg_mean    REAL    NOT NULL,
                    ac_total    INTEGER NOT NULL,
                    ac_civil    INTEGER NOT NULL,
                    ac_military INTEGER NOT NULL,
                    signal_avg  REAL,
                    signal_min  REAL,
                    signal_max  REAL
                );

                CREATE TABLE IF NOT EXISTS minute_df_counts (
                    ts    INTEGER NOT NULL,
                    df    INTEGER NOT NULL,
                    count INTEGER NOT NULL,
                    PRIMARY KEY (ts, df)
                );

                CREATE TABLE IF NOT EXISTS minute_type_counts (
                    ts        INTEGER NOT NULL,
                    type_code TEXT    NOT NULL,
                    count     INTEGER NOT NULL,
                    PRIMARY KEY (ts, type_code)
                );

                CREATE TABLE IF NOT EXISTS minute_operator_counts (
                    ts       INTEGER NOT NULL,
                    operator TEXT    NOT NULL,
                    count    INTEGER NOT NULL,
                    PRIMARY KEY (ts, operator)
                );

                CREATE TABLE IF NOT EXISTS daily_aircraft_seen (
                    date TEXT NOT NULL,
                    icao TEXT NOT NULL,
                    PRIMARY KEY (date, icao)
                );

                CREATE TABLE IF NOT EXISTS day_stats (
                    date             TEXT PRIMARY KEY,
                    msg_total        INTEGER NOT NULL DEFAULT 0,
                    msg_max          REAL    NOT NULL DEFAULT 0,
                    ac_peak          INTEGER NOT NULL DEFAULT 0,
                    ac_civil_peak    INTEGER NOT NULL DEFAULT 0,
                    ac_military_peak INTEGER NOT NULL DEFAULT 0,
                    unique_aircraft  INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS day_type_counts (
                    date      TEXT    NOT NULL,
                    type_code TEXT    NOT NULL,
                    count     INTEGER NOT NULL,
                    PRIMARY KEY (date, type_code)
                );

                CREATE TABLE IF NOT EXISTS coverage_samples (
                    ts          INTEGER NOT NULL,
                    icao        TEXT    NOT NULL,
                    bearing_deg REAL    NOT NULL,
                    range_nm    REAL    NOT NULL,
                    altitude    INTEGER,
                    signal      INTEGER,
                    PRIMARY KEY (ts, icao)
                );
                CREATE INDEX IF NOT EXISTS coverage_samples_ts
                    ON coverage_samples(ts);

                CREATE TABLE IF NOT EXISTS aircraft_registry (
                    icao             TEXT    PRIMARY KEY,
                    first_seen       INTEGER NOT NULL,
                    last_seen        INTEGER NOT NULL,
                    sighting_count   INTEGER NOT NULL DEFAULT 1,
                    registration     TEXT,
                    type_code        TEXT,
                    type_category    TEXT,
                    military         INTEGER NOT NULL DEFAULT 0,
                    country          TEXT,
                    foreign_military INTEGER NOT NULL DEFAULT 0,
                    interesting      INTEGER NOT NULL DEFAULT 0,
                    rare             INTEGER NOT NULL DEFAULT 1,
                    first_seen_flag  INTEGER NOT NULL DEFAULT 0,
                    lat              REAL,
                    lon              REAL,
                    operator         TEXT,
                    manufacturer     TEXT,
                    year             TEXT
                );
            """)
        # Migrate existing databases that predate first_seen_flag
        try:
            with self._connect() as conn:
                conn.execute(
                    "ALTER TABLE aircraft_registry ADD COLUMN first_seen_flag INTEGER NOT NULL DEFAULT 0"
                )
            log.info("DB: migrated aircraft_registry — added first_seen_flag")
        except Exception:
            pass  # column already exists

        # Migrate existing databases that predate signal columns in minute_stats
        with self._connect() as conn:
            for col in ("signal_avg REAL", "signal_min REAL", "signal_max REAL",
                        "ac_with_pos INTEGER", "ac_mlat INTEGER"):
                try:
                    conn.execute(f"ALTER TABLE minute_stats ADD COLUMN {col}")
                except Exception:
                    pass  # column already exists

        # Migrate: add lat/lon/operator/manufacturer/year to aircraft_registry
        with self._connect() as conn:
            for col in ("lat REAL", "lon REAL", "operator TEXT", "manufacturer TEXT", "year TEXT"):
                try:
                    conn.execute(f"ALTER TABLE aircraft_registry ADD COLUMN {col}")
                except Exception:
                    pass  # column already exists

        # acas_events table (new — added for ACAS/TCAS RA logging)
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS acas_events (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts                 INTEGER NOT NULL,
                    icao               TEXT    NOT NULL,
                    ra_description     TEXT    NOT NULL,
                    ra_corrective      INTEGER NOT NULL DEFAULT 0,
                    ra_sense           TEXT,
                    ara_bits           TEXT,
                    rac_bits           TEXT,
                    rat                INTEGER NOT NULL DEFAULT 0,
                    mte                INTEGER NOT NULL DEFAULT 0,
                    tti                INTEGER NOT NULL DEFAULT 0,
                    threat_icao        TEXT,
                    threat_alt         INTEGER,
                    threat_range_nm    REAL,
                    threat_bearing_deg REAL,
                    sensitivity_level  INTEGER,
                    altitude           INTEGER
                );
                CREATE INDEX IF NOT EXISTS acas_events_ts
                    ON acas_events(ts);
                CREATE INDEX IF NOT EXISTS acas_events_icao
                    ON acas_events(icao);
                CREATE INDEX IF NOT EXISTS acas_events_threat_icao
                    ON acas_events(threat_icao)
                    WHERE threat_icao IS NOT NULL;
            """)

        log.info("DB: schema ready at %s", config.DB_PATH)

    # ------------------------------------------------------------------
    # Write path (called every minute via asyncio.to_thread)
    # ------------------------------------------------------------------

    def write_minute(self, snapshot: dict) -> None:
        """Persist completed minute stats and current aircraft state."""
        history = snapshot.get("rate_history", [])
        aircraft = snapshot.get("aircraft", [])
        now_ts = int(time.time())
        today = date.today().isoformat()

        with self._connect() as conn:
            # Write all newly completed minute buckets (everything except the last,
            # which is the still-accumulating current minute)
            for entry in history[:-1]:
                ts = entry["minute"] * 60
                if ts <= self._last_written_ts:
                    continue
                conn.execute(
                    "INSERT OR REPLACE INTO minute_stats"
                    " (ts,msg_min,msg_max,msg_mean,ac_total,ac_civil,ac_military,"
                    "signal_avg,signal_min,signal_max,ac_with_pos,ac_mlat)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (ts, entry["min"], entry["max"], entry["mean"],
                     entry["ac_total"], entry["ac_civil"], entry["ac_military"],
                     entry.get("signal_avg"), entry.get("signal_min"), entry.get("signal_max"),
                     entry.get("ac_with_pos"), entry.get("ac_mlat")),
                )
                self._last_written_ts = ts

            # Write DF counts for completed minutes
            for df_entry in snapshot.get("df_history", [])[:-1]:
                ts = df_entry["minute"] * 60
                for df_type, count in df_entry["counts"].items():
                    conn.execute(
                        "INSERT OR REPLACE INTO minute_df_counts VALUES (?,?,?)",
                        (ts, int(df_type), count),
                    )

            # Write type and operator counts for the current snapshot
            if history:
                cur_ts = history[-1]["minute"] * 60
                type_counts = Counter(
                    a["type_code"] for a in aircraft if a.get("type_code")
                )
                for type_code, count in type_counts.items():
                    conn.execute(
                        "INSERT OR REPLACE INTO minute_type_counts VALUES (?,?,?)",
                        (cur_ts, type_code, count),
                    )
                operator_counts = Counter(
                    a["operator"] for a in aircraft if a.get("operator")
                )
                for operator, count in operator_counts.items():
                    conn.execute(
                        "INSERT OR REPLACE INTO minute_operator_counts VALUES (?,?,?)",
                        (cur_ts, operator, count),
                    )

            # Daily aircraft seen (deduped by date + icao)
            for ac in aircraft:
                conn.execute(
                    "INSERT OR IGNORE INTO daily_aircraft_seen VALUES (?,?)",
                    (today, ac["icao"]),
                )

            # Aircraft registry upserts
            for ac in aircraft:
                self._upsert_aircraft(conn, ac, now_ts)

        # Recalculate type-based rarity flags outside the main write transaction
        self.recalculate_type_rarity()

    def _upsert_aircraft(self, conn: sqlite3.Connection, ac: dict, now_ts: int) -> None:
        home = (config.HOME_COUNTRY or "").lower()
        foreign_mil = bool(
            ac.get("military") and home and ac.get("country")
            and ac["country"].lower() != home
        )
        interesting = bool(
            ac.get("type_code") and ac["type_code"].upper() in INTERESTING_TYPE_CODES
        )
        # On UPDATE, country is kept from registry for military aircraft (their serials
        # don't follow civil prefix rules, so a stale live-state value must not overwrite
        # a manually corrected entry).  foreign_military is recomputed from whichever
        # country value will actually be stored, so a stale live-state country cannot
        # flip it back to the wrong value.
        conn.execute("""
            INSERT INTO aircraft_registry
                (icao, first_seen, last_seen, sighting_count,
                 registration, type_code, type_category,
                 military, country, foreign_military, interesting, rare, first_seen_flag,
                 lat, lon, operator, manufacturer, year)
            VALUES (?,?,?,1, ?,?,?, ?,?,?,?,1, 1, ?,?, ?,?,?)
            ON CONFLICT(icao) DO UPDATE SET
                last_seen        = excluded.last_seen,
                sighting_count   = sighting_count +
                    CASE WHEN excluded.last_seen - last_seen > 3600 THEN 1 ELSE 0 END,
                registration     = COALESCE(excluded.registration, registration),
                type_code        = COALESCE(excluded.type_code, type_code),
                type_category    = COALESCE(excluded.type_category, type_category),
                military         = excluded.military,
                country          = COALESCE(excluded.country, country),
                foreign_military = CASE
                    WHEN excluded.military = 0 THEN 0
                    WHEN ? = '' THEN 0
                    ELSE CASE
                        WHEN LOWER(COALESCE(excluded.country, country)) != ?
                        THEN 1 ELSE 0 END
                    END,
                interesting      = excluded.interesting,
                lat              = COALESCE(excluded.lat, lat),
                lon              = COALESCE(excluded.lon, lon),
                operator         = COALESCE(excluded.operator, operator),
                manufacturer     = COALESCE(excluded.manufacturer, manufacturer),
                year             = COALESCE(excluded.year, year)
        """, (
            ac["icao"], now_ts, now_ts,
            ac.get("registration"), ac.get("type_code"), ac.get("type_category"),
            int(bool(ac.get("military"))), ac.get("country"),
            int(foreign_mil), int(interesting),
            ac.get("lat"), ac.get("lon"),
            ac.get("operator"), ac.get("manufacturer"), ac.get("year"),
            # extra params for foreign_military recomputation in UPDATE path:
            home, home,
        ))

    # ------------------------------------------------------------------
    # Day rollup
    # ------------------------------------------------------------------

    def rollup_missed_days(self) -> None:
        """On startup: roll up any completed days not yet in day_stats."""
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(date) FROM day_stats").fetchone()
            last_rolled = row[0] if row[0] else None

            if last_rolled is None:
                row = conn.execute(
                    "SELECT MIN(date(ts, 'unixepoch')) FROM minute_stats"
                ).fetchone()
                if not row[0]:
                    return
                start = date.fromisoformat(row[0])
            else:
                start = date.fromisoformat(last_rolled) + timedelta(days=1)

            yesterday = date.today() - timedelta(days=1)
            if start > yesterday:
                return

            current = start
            while current <= yesterday:
                self._rollup_day(conn, current.isoformat())
                current += timedelta(days=1)

            log.info("DB: rolled up %d day(s) (%s → %s)",
                     (yesterday - start).days + 1, start, yesterday)

    def rollup_yesterday(self) -> None:
        """Roll up yesterday's data; called at day boundary."""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        with self._connect() as conn:
            self._rollup_day(conn, yesterday)
        log.info("DB: rolled up %s", yesterday)

    def _rollup_day(self, conn: sqlite3.Connection, date_str: str) -> None:
        conn.execute("""
            INSERT OR REPLACE INTO day_stats
                (date, msg_total, msg_max, ac_peak, ac_civil_peak, ac_military_peak, unique_aircraft)
            SELECT
                ?,
                CAST(SUM(msg_mean * 60) AS INTEGER),
                MAX(msg_max),
                MAX(ac_total),
                MAX(ac_civil),
                MAX(ac_military),
                (SELECT COUNT(*) FROM daily_aircraft_seen WHERE date = ?)
            FROM minute_stats
            WHERE date(ts, 'unixepoch') = ?
        """, (date_str, date_str, date_str))

        conn.execute("""
            INSERT OR REPLACE INTO day_type_counts (date, type_code, count)
            SELECT date(ts, 'unixepoch'), type_code, MAX(count)
            FROM minute_type_counts
            WHERE date(ts, 'unixepoch') = ?
            GROUP BY type_code
        """, (date_str,))

    # ------------------------------------------------------------------
    # Prune
    # ------------------------------------------------------------------

    def prune(self) -> None:
        """Delete short-term data older than the retention window."""
        cutoff_ts = int(
            (datetime.now(timezone.utc) - timedelta(days=config.MINUTE_STATS_RETENTION_DAYS))
            .timestamp()
        )
        cutoff_date = (
            date.today() - timedelta(days=config.MINUTE_STATS_RETENTION_DAYS)
        ).isoformat()
        coverage_cutoff_ts = int(
            (datetime.now(timezone.utc) - timedelta(days=90)).timestamp()
        )
        with self._connect() as conn:
            conn.execute("DELETE FROM minute_stats WHERE ts < ?", (cutoff_ts,))
            conn.execute("DELETE FROM minute_df_counts WHERE ts < ?", (cutoff_ts,))
            conn.execute("DELETE FROM minute_type_counts WHERE ts < ?", (cutoff_ts,))
            conn.execute("DELETE FROM minute_operator_counts WHERE ts < ?", (cutoff_ts,))
            conn.execute("DELETE FROM daily_aircraft_seen WHERE date < ?", (cutoff_date,))
            conn.execute("DELETE FROM coverage_samples WHERE ts < ?", (coverage_cutoff_ts,))
            conn.execute("DELETE FROM acas_events WHERE ts < ?", (coverage_cutoff_ts,))
        log.info("DB: pruned data older than %s", cutoff_date)

    # ------------------------------------------------------------------
    # Coverage samples
    # ------------------------------------------------------------------

    def write_coverage(self, samples: list[dict]) -> None:
        """Persist one coverage sample per aircraft that has a position.
        Called from the minute write task. Each sample: {icao, ts, bearing_deg,
        range_nm, altitude, signal}."""
        if not samples:
            return
        with self._connect() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO coverage_samples "
                "(ts, icao, bearing_deg, range_nm, altitude, signal) "
                "VALUES (:ts, :icao, :bearing_deg, :range_nm, :altitude, :signal)",
                samples,
            )

    def query_polar(self, days: int, max_points: int = 8000) -> list[dict]:
        """Coverage samples for the last N days, downsampled to max_points for polar scatter plot."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM coverage_samples WHERE ts >= ?", (cutoff,)
            ).fetchone()[0]
            stride = max(1, -(-total // max_points))  # ceiling division → result ≤ max_points
            rows = conn.execute("""
                SELECT bearing_deg, range_nm, altitude, signal
                FROM coverage_samples
                WHERE ts >= ? AND (rowid % ?) = 0
                ORDER BY ts
            """, (cutoff, stride)).fetchall()
        return [{"bearing": round(r["bearing_deg"], 1),
                 "range":   round(r["range_nm"], 1),
                 "alt":     r["altitude"],
                 "signal":  r["signal"]} for r in rows]

    def query_max_range_by_bearing(self, days: int, sectors: int = 72) -> list[dict]:
        """Max range in each azimuth sector (default 5° sectors → 72 buckets)."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        sector_width = 360 / sectors
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT CAST(bearing_deg / {sector_width} AS INTEGER) AS sector,
                       MAX(range_nm) AS max_range
                FROM coverage_samples
                WHERE ts >= ?
                GROUP BY sector
                ORDER BY sector
            """, (cutoff,)).fetchall()
        return [{"bearing": round(r["sector"] * sector_width + sector_width / 2, 1),
                 "max_range": round(r["max_range"], 1)} for r in rows]

    # ------------------------------------------------------------------
    # ACAS events (called via asyncio.to_thread from main.py / acas.py)
    # ------------------------------------------------------------------

    def write_acas_events(self, events: list[dict]) -> None:
        """Persist a batch of ACAS RA events."""
        if not events:
            return
        with self._connect() as conn:
            conn.executemany("""
                INSERT INTO acas_events
                    (ts, icao, ra_description, ra_corrective, ra_sense,
                     ara_bits, rac_bits, rat, mte, tti,
                     threat_icao, threat_alt, threat_range_nm, threat_bearing_deg,
                     sensitivity_level, altitude)
                VALUES
                    (:ts, :icao, :ra_description, :ra_corrective, :ra_sense,
                     :ara_bits, :rac_bits, :rat, :mte, :tti,
                     :threat_icao, :threat_alt, :threat_range_nm, :threat_bearing_deg,
                     :sensitivity_level, :altitude)
            """, events)

    def query_acas_events(self, days: int, limit: int) -> list[sqlite3.Row]:
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT e.*,
                       r.registration,  r.type_code,   r.operator,  r.country,
                       t.registration AS threat_reg,
                       t.type_code    AS threat_type_code,
                       t.operator     AS threat_operator,
                       t.country      AS threat_country
                FROM acas_events e
                LEFT JOIN aircraft_registry r ON e.icao       = r.icao
                LEFT JOIN aircraft_registry t ON e.threat_icao = t.icao
                WHERE e.ts >= ?
                ORDER BY e.ts DESC
                LIMIT ?
            """, (cutoff, limit)).fetchall()
        return [dict(r) for r in rows]

    def query_acas_stats(self, days: int) -> dict:
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            totals = conn.execute("""
                SELECT COUNT(*) AS total,
                       SUM(ra_corrective) AS corrective
                FROM acas_events WHERE ts >= ?
            """, (cutoff,)).fetchone()

            by_sense = conn.execute("""
                SELECT ra_sense, COUNT(*) AS cnt
                FROM acas_events WHERE ts >= ?
                GROUP BY ra_sense ORDER BY cnt DESC
            """, (cutoff,)).fetchall()

            by_country = conn.execute("""
                SELECT r.country, COUNT(*) AS cnt
                FROM acas_events e
                LEFT JOIN aircraft_registry r ON e.icao = r.icao
                WHERE e.ts >= ? AND r.country IS NOT NULL
                GROUP BY r.country ORDER BY cnt DESC LIMIT 10
            """, (cutoff,)).fetchall()

            by_type = conn.execute("""
                SELECT r.type_code, COUNT(*) AS cnt
                FROM acas_events e
                LEFT JOIN aircraft_registry r ON e.icao = r.icao
                WHERE e.ts >= ? AND r.type_code IS NOT NULL
                GROUP BY r.type_code ORDER BY cnt DESC LIMIT 10
            """, (cutoff,)).fetchall()

            by_operator = conn.execute("""
                SELECT r.operator, COUNT(*) AS cnt
                FROM acas_events e
                LEFT JOIN aircraft_registry r ON e.icao = r.icao
                WHERE e.ts >= ? AND r.operator IS NOT NULL
                GROUP BY r.operator ORDER BY cnt DESC LIMIT 10
            """, (cutoff,)).fetchall()

        total    = totals["total"]    or 0
        correct  = totals["corrective"] or 0
        return {
            "total":       total,
            "corrective":  correct,
            "preventive":  total - correct,
            "by_sense":    [{"ra_sense": r["ra_sense"],  "count": r["cnt"]} for r in by_sense],
            "by_country":  [{"country":  r["country"],   "count": r["cnt"]} for r in by_country],
            "by_type":     [{"type_code": r["type_code"], "count": r["cnt"]} for r in by_type],
            "by_operator": [{"operator": r["operator"],  "count": r["cnt"]} for r in by_operator],
        }

    def query_acas_timeline(self, days: int) -> list[dict]:
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT date(ts, 'unixepoch') AS day,
                       COUNT(*) AS total,
                       SUM(ra_corrective) AS corrective
                FROM acas_events
                WHERE ts >= ?
                GROUP BY day
                ORDER BY day
            """, (cutoff,)).fetchall()
        return [
            {
                "day":        r["day"],
                "total":      r["total"],
                "corrective": r["corrective"] or 0,
                "preventive": r["total"] - (r["corrective"] or 0),
            }
            for r in rows
        ]

    def query_acas_context(self, event_id: int) -> dict:
        with self._connect() as conn:
            ev = conn.execute(
                "SELECT * FROM acas_events WHERE id = ?", (event_id,)
            ).fetchone()
            if not ev:
                return {}
            ev = dict(ev)
            icaos = [ev["icao"]]
            if ev.get("threat_icao"):
                icaos.append(ev["threat_icao"])
            window_lo = ev["ts"] - 120
            window_hi = ev["ts"] + 120
            placeholders = ",".join("?" * len(icaos))
            track_rows = conn.execute(f"""
                SELECT ts, icao, altitude
                FROM coverage_samples
                WHERE icao IN ({placeholders})
                  AND ts BETWEEN ? AND ?
                ORDER BY ts
            """, (*icaos, window_lo, window_hi)).fetchall()

        return {
            "event":  ev,
            "tracks": [dict(r) for r in track_rows],
        }

    def query_acas_for_icao(self, icao: str, limit: int) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT e.*,
                       t.registration AS threat_reg,
                       t.type_code    AS threat_type_code,
                       t.operator     AS threat_operator
                FROM acas_events e
                LEFT JOIN aircraft_registry t ON e.threat_icao = t.icao
                WHERE e.icao = ? OR e.threat_icao = ?
                ORDER BY e.ts DESC
                LIMIT ?
            """, (icao, icao, limit)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # History queries (called via asyncio.to_thread from history.py)
    # ------------------------------------------------------------------

    def query_heatmap(self, col: str, days: int, bucket_mins: int = 15) -> list[dict]:
        cutoff = int(
            (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
        )
        bucket_secs = bucket_mins * 60
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT date(ts, 'unixepoch')    AS day,
                       (ts % 86400) / {bucket_secs} AS bucket,
                       AVG({col})               AS value
                FROM minute_stats
                WHERE ts >= ?
                GROUP BY day, bucket
                ORDER BY day, bucket
            """, (cutoff,)).fetchall()
        return [{"day": r["day"], "bucket": r["bucket"], "value": round(r["value"] or 0, 2)}
                for r in rows]

    def query_heatmap_type(self, type_code: str, days: int, bucket_mins: int = 15) -> list[dict]:
        cutoff = int(
            (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
        )
        bucket_secs = bucket_mins * 60
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT date(ts, 'unixepoch')    AS day,
                       (ts % 86400) / {bucket_secs} AS bucket,
                       MAX(count)               AS value
                FROM minute_type_counts
                WHERE ts >= ? AND type_code = ?
                GROUP BY day, bucket
                ORDER BY day, bucket
            """, (cutoff, type_code)).fetchall()
        return [{"day": r["day"], "bucket": r["bucket"], "value": r["value"] or 0}
                for r in rows]

    def query_heatmap_operator(self, operator: str, days: int, bucket_mins: int = 15) -> list[dict]:
        cutoff = int(
            (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
        )
        bucket_secs = bucket_mins * 60
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT date(ts, 'unixepoch')    AS day,
                       (ts % 86400) / {bucket_secs} AS bucket,
                       MAX(count)               AS value
                FROM minute_operator_counts
                WHERE ts >= ? AND operator = ?
                GROUP BY day, bucket
                ORDER BY day, bucket
            """, (cutoff, operator)).fetchall()
        return [{"day": r["day"], "bucket": r["bucket"], "value": r["value"] or 0}
                for r in rows]

    def query_calendar(self, col: str, months: int) -> list[dict]:
        cutoff = (date.today() - timedelta(days=months * 30)).isoformat()
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT date, {col} AS value
                FROM day_stats
                WHERE date >= ?
                ORDER BY date
            """, (cutoff,)).fetchall()
        return [{"date": r["date"], "value": r["value"] or 0} for r in rows]

    def query_trend(self, days: int) -> list[dict]:
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT date, ac_peak, ac_civil_peak, ac_military_peak
                FROM day_stats
                WHERE date >= ?
                ORDER BY date
            """, (cutoff,)).fetchall()
        return [{"date": r["date"],
                 "total": r["ac_civil_peak"] + r["ac_military_peak"],
                 "civil": r["ac_civil_peak"], "military": r["ac_military_peak"]}
                for r in rows]

    def query_heatmap_options(self) -> dict:
        """Return distinct type codes and operators seen, ordered by frequency."""
        with self._connect() as conn:
            types = conn.execute("""
                SELECT type_code, SUM(count) AS total
                FROM minute_type_counts
                GROUP BY type_code
                ORDER BY total DESC
                LIMIT 200
            """).fetchall()
            operators = conn.execute("""
                SELECT operator, SUM(count) AS total
                FROM minute_operator_counts
                GROUP BY operator
                ORDER BY total DESC
                LIMIT 200
            """).fetchall()
        return {
            "types":     [r["type_code"] for r in types],
            "operators": [r["operator"]  for r in operators],
        }

    # ------------------------------------------------------------------
    # Receiver stats queries
    # ------------------------------------------------------------------

    def query_receiver_scatter(self, days: int) -> list[dict]:
        """Per-minute (ac_total, msg_mean, signal_avg) for scatter plot."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT ts, ac_total, msg_mean, signal_avg
                FROM minute_stats
                WHERE ts >= ? AND signal_avg IS NOT NULL
                ORDER BY ts
            """, (cutoff,)).fetchall()
        return [{"ts": r["ts"], "ac": r["ac_total"],
                 "msgs": round(r["msg_mean"] * 60),
                 "signal": round(r["signal_avg"], 1)} for r in rows]

    def query_signal_percentiles(self, days: int) -> list[dict]:
        """Hourly signal strength percentiles (10th/50th/90th) over the last N days.

        Beast RSSI bytes are stored raw (0=strongest, 255=weakest). We sort them
        ascending per hour so that:
          p10 raw → strongest 10% of signals → high % strength (best reception)
          p90 raw → weakest 10% of signals  → low % strength (coverage edge)
        Both are returned as signal-strength % (inverted from raw).
        """
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT (ts / 3600) * 3600 AS hour, signal_avg
                FROM minute_stats
                WHERE ts >= ? AND signal_avg IS NOT NULL
                ORDER BY hour, signal_avg
            """, (cutoff,)).fetchall()

        from collections import defaultdict
        by_hour: dict = defaultdict(list)
        for r in rows:
            by_hour[r["hour"]].append(r["signal_avg"])

        # Beast RSSI byte → dBFS: signal_dBFS = -(raw / 2)
        # Smaller raw byte = stronger signal = less negative dBFS
        def to_dbfs(raw):
            return round(-raw / 2.0, 1)

        def percentile(vals, p):
            idx = max(0, min(len(vals) - 1, int(p / 100 * len(vals))))
            return to_dbfs(vals[idx])

        result = []
        for hour in sorted(by_hour.keys()):
            vals = by_hour[hour]  # sorted ascending: small raw = strong signal
            result.append({
                "ts":     hour,
                "strong": percentile(vals, 10),  # p10 raw = strongest → near 0 dBFS
                "median": percentile(vals, 50),
                "weak":   percentile(vals, 90),  # p90 raw = weakest → large negative dBFS
            })
        return result

    def query_df_breakdown(self, days: int) -> list[dict]:
        """Daily DF type message counts over the last N days."""
        cutoff_date = (date.today() - timedelta(days=days)).isoformat()
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT date(ts, 'unixepoch') AS day, df, SUM(count) AS total
                FROM minute_df_counts
                WHERE date(ts, 'unixepoch') >= ?
                GROUP BY day, df
                ORDER BY day, df
            """, (cutoff_date,)).fetchall()
        # Pivot into {day: {df: count}} then flatten to list
        by_day: dict[str, dict] = {}
        for r in rows:
            by_day.setdefault(r["day"], {})[str(r["df"])] = r["total"]
        return [{"date": d, "counts": counts} for d, counts in sorted(by_day.items())]

    def query_heatmap_group(
        self,
        type_codes: list[str] | None = None,
        category_prefix: str | None = None,
        days: int = 30,
        bucket_mins: int = 15,
    ) -> list[dict]:
        """Hourly/15-min heatmap of aircraft count for a type group."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        bucket_secs = bucket_mins * 60
        with self._connect() as conn:
            if category_prefix:
                rows = conn.execute(f"""
                    SELECT date(ts, 'unixepoch')      AS day,
                           (ts % 86400) / {bucket_secs} AS bucket,
                           SUM(count)                 AS value
                    FROM minute_type_counts
                    WHERE ts >= ?
                      AND type_code IN (
                          SELECT DISTINCT type_code FROM aircraft_registry
                          WHERE type_category LIKE ?
                      )
                    GROUP BY day, bucket
                    ORDER BY day, bucket
                """, (cutoff, category_prefix + '%')).fetchall()
            elif type_codes:
                placeholders = ','.join('?' * len(type_codes))
                rows = conn.execute(f"""
                    SELECT date(ts, 'unixepoch')      AS day,
                           (ts % 86400) / {bucket_secs} AS bucket,
                           SUM(count)                 AS value
                    FROM minute_type_counts
                    WHERE ts >= ? AND type_code IN ({placeholders})
                    GROUP BY day, bucket
                    ORDER BY day, bucket
                """, [cutoff] + list(type_codes)).fetchall()
            else:
                return []
        return [{"day": r["day"], "bucket": r["bucket"], "value": r["value"] or 0}
                for r in rows]

    def query_heatmap_df(self, df: int | None, days: int, bucket_mins: int = 60) -> list[dict]:
        """Hourly/15-min heatmap of message count. df=None sums all DF types."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        bucket_secs = bucket_mins * 60
        with self._connect() as conn:
            if df is None:
                rows = conn.execute(f"""
                    SELECT date(ts, 'unixepoch')      AS day,
                           (ts % 86400) / {bucket_secs} AS bucket,
                           SUM(count)                 AS value
                    FROM minute_df_counts
                    WHERE ts >= ?
                    GROUP BY day, bucket
                    ORDER BY day, bucket
                """, (cutoff,)).fetchall()
            else:
                rows = conn.execute(f"""
                    SELECT date(ts, 'unixepoch')      AS day,
                           (ts % 86400) / {bucket_secs} AS bucket,
                           SUM(count)                 AS value
                    FROM minute_df_counts
                    WHERE ts >= ? AND df = ?
                    GROUP BY day, bucket
                    ORDER BY day, bucket
                """, (cutoff, df)).fetchall()
        return [{"day": r["day"], "bucket": r["bucket"], "value": r["value"] or 0}
                for r in rows]

    def query_today_icaos(self, today: str) -> dict:
        """Return sets of all and military ICAOs seen today (for restart persistence)."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT das.icao, COALESCE(ar.military, 0) AS military
                FROM daily_aircraft_seen das
                LEFT JOIN aircraft_registry ar ON das.icao = ar.icao
                WHERE das.date = ?
            """, (today,)).fetchall()
        all_icaos = {r["icao"] for r in rows}
        mil_icaos = {r["icao"] for r in rows if r["military"]}
        return {"all": all_icaos, "military": mil_icaos}

    def query_new_aircraft_per_day(self, months: int) -> list[dict]:
        """Count of aircraft first seen on each day."""
        cutoff = (date.today() - timedelta(days=months * 30)).isoformat()
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT date(first_seen, 'unixepoch') AS date, COUNT(*) AS value
                FROM aircraft_registry
                WHERE date(first_seen, 'unixepoch') >= ?
                GROUP BY date
                ORDER BY date
            """, (cutoff,)).fetchall()
        return [{"date": r["date"], "value": r["value"]} for r in rows]

    def query_military_aircraft_per_day(self, months: int) -> list[dict]:
        """Count of unique military aircraft seen each day."""
        cutoff = (date.today() - timedelta(days=months * 30)).isoformat()
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT das.date, COUNT(*) AS value
                FROM daily_aircraft_seen das
                JOIN aircraft_registry ar ON das.icao = ar.icao
                WHERE das.date >= ? AND ar.military = 1
                GROUP BY das.date
                ORDER BY das.date
            """, (cutoff,)).fetchall()
        return [{"date": r["date"], "value": r["value"]} for r in rows]

    def query_notable_sightings_per_day(self, months: int) -> list[dict]:
        """Count of unique notable aircraft seen each day."""
        cutoff = (date.today() - timedelta(days=months * 30)).isoformat()
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT das.date, COUNT(*) AS value
                FROM daily_aircraft_seen das
                JOIN aircraft_registry ar ON das.icao = ar.icao
                WHERE das.date >= ?
                  AND (ar.foreign_military OR ar.interesting OR ar.rare OR ar.first_seen_flag)
                GROUP BY das.date
                ORDER BY das.date
            """, (cutoff,)).fetchall()
        return [{"date": r["date"], "value": r["value"]} for r in rows]

    def query_receiver_baseline(self) -> list[dict]:
        """30-day rolling baseline: average ac_total and msg_mean by hour-of-day (0–23)."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT (ts % 86400) / 3600 AS hour_of_day,
                       AVG(ac_total)       AS ac_avg,
                       AVG(msg_mean)       AS msg_avg,
                       AVG(signal_avg)     AS sig_avg
                FROM minute_stats
                WHERE ts >= ?
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """, (cutoff,)).fetchall()
        return [{"hour": r["hour_of_day"],
                 "ac_avg":  round(r["ac_avg"] or 0, 1),
                 "msg_avg": round(r["msg_avg"] or 0, 1),
                 "sig_avg": round(r["sig_avg"] or 0, 1) if r["sig_avg"] else None}
                for r in rows]

    def get_aircraft(self, icao: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM aircraft_registry WHERE icao = ?", (icao,)
            ).fetchone()
        return dict(row) if row else None

    def recalculate_type_rarity(self) -> None:
        """Bulk-update the `rare` and `first_seen_flag` flags for all aircraft.
        rare=1 when the type has fewer than RARE_THRESHOLD distinct aircraft in the registry.
        first_seen_flag=1 when sighting_count=1 (unique sighting — never seen more than once).
        Called from write_minute() so it runs every minute — the CTE keeps it fast."""
        with self._connect() as conn:
            conn.execute("""
                WITH type_counts AS (
                    SELECT type_code, COUNT(*) AS tc
                    FROM aircraft_registry
                    WHERE type_code IS NOT NULL AND type_code != ''
                    GROUP BY type_code
                )
                UPDATE aircraft_registry
                SET rare = CASE
                    WHEN type_code IS NULL OR type_code = '' THEN 0
                    WHEN (SELECT tc FROM type_counts WHERE type_code = aircraft_registry.type_code) < ?
                         THEN 1
                    ELSE 0
                END,
                first_seen_flag = CASE WHEN sighting_count = 1 THEN 1 ELSE 0 END
            """, (config.RARE_THRESHOLD,))

    # ------------------------------------------------------------------
    # Fleet queries
    # ------------------------------------------------------------------

    def query_fleet_summary(self, since_ts: int | None = None) -> dict:
        where = "WHERE last_seen >= ?" if since_ts is not None else ""
        params = (since_ts,) if since_ts is not None else ()
        with self._connect() as conn:
            row = conn.execute(f"""
                SELECT
                    COUNT(*)                                              AS total,
                    SUM(CASE WHEN registration IS NOT NULL THEN 1 ELSE 0 END) AS with_registration,
                    SUM(military)                                         AS military,
                    SUM(foreign_military)                                 AS foreign_military,
                    SUM(interesting)                                      AS interesting,
                    SUM(rare)                                             AS rare,
                    SUM(first_seen_flag)                                  AS first_seen
                FROM aircraft_registry
                {where}
            """, params).fetchone()
        return dict(row) if row else {}

    def query_fleet_types(self, limit: int = 20, military: int | None = None,
                          since_ts: int | None = None) -> list[dict]:
        mil_clause = "" if military is None else f"AND military = {int(military)}"
        params: list = []
        since_clause = ""
        if since_ts is not None:
            since_clause = "AND last_seen >= ?"
            params.append(since_ts)
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT type_code, COUNT(*) AS count
                FROM aircraft_registry
                WHERE type_code IS NOT NULL AND type_code != '' {mil_clause} {since_clause}
                GROUP BY type_code
                ORDER BY count DESC
                LIMIT ?
            """, params).fetchall()
        return [{"type_code": r["type_code"], "count": r["count"]} for r in rows]

    def query_fleet_operators(self, limit: int = 20, since_ts: int | None = None,
                              military: int | None = None) -> list[dict]:
        params: list = []
        since_clause = ""
        if since_ts is not None:
            since_clause = "AND last_seen >= ?"
            params.append(since_ts)
        params.append(limit)

        if military == 1:
            # Include all military aircraft; derive a display label for those without operator
            sql = f"""
                SELECT
                  COALESCE(operator,
                    CASE WHEN country IS NOT NULL AND country != ''
                         THEN country || ' (unidentified)'
                         ELSE 'Unknown military'
                    END
                  ) AS operator,
                  COUNT(*) AS count,
                  SUM(military) AS military_count
                FROM aircraft_registry
                WHERE military = 1 {since_clause}
                GROUP BY 1
                ORDER BY count DESC
                LIMIT ?
            """
        elif military == 0:
            sql = f"""
                SELECT operator, COUNT(*) AS count, SUM(military) AS military_count
                FROM aircraft_registry
                WHERE operator IS NOT NULL AND operator != '' AND military = 0 {since_clause}
                GROUP BY operator
                ORDER BY count DESC
                LIMIT ?
            """
        else:
            sql = f"""
                SELECT operator, COUNT(*) AS count, SUM(military) AS military_count
                FROM aircraft_registry
                WHERE operator IS NOT NULL AND operator != '' {since_clause}
                GROUP BY operator
                ORDER BY count DESC
                LIMIT ?
            """
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [{"operator": r["operator"], "count": r["count"],
                 "military_count": r["military_count"] or 0} for r in rows]

    def query_fleet_countries(self, limit: int = 25, military: int | None = None,
                              since_ts: int | None = None) -> list[dict]:
        mil_clause = "" if military is None else f"AND military = {int(military)}"
        params: list = []
        since_clause = ""
        if since_ts is not None:
            since_clause = "AND last_seen >= ?"
            params.append(since_ts)
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT country, COUNT(*) AS count, SUM(military) AS military_count
                FROM aircraft_registry
                WHERE country IS NOT NULL AND country != '' {mil_clause} {since_clause}
                GROUP BY country
                ORDER BY count DESC
                LIMIT ?
            """, params).fetchall()
        return [{"country": r["country"], "count": r["count"],
                 "military_count": r["military_count"] or 0} for r in rows]

    def query_fleet_categories(self, military: int | None = None,
                               since_ts: int | None = None) -> list[dict]:
        mil_clause = "" if military is None else f"AND military = {int(military)}"
        params: list = []
        since_clause = ""
        if since_ts is not None:
            since_clause = "AND last_seen >= ?"
            params.append(since_ts)
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT type_category, COUNT(*) AS count
                FROM aircraft_registry
                WHERE type_category IS NOT NULL AND type_category != '' {mil_clause} {since_clause}
                GROUP BY type_category
                ORDER BY count DESC
            """, params).fetchall()
        return [{"type_category": r["type_category"], "count": r["count"]} for r in rows]

    def query_fleet_ages(self, since_ts: int | None = None) -> list[dict]:
        params: list = []
        since_clause = ""
        if since_ts is not None:
            since_clause = "AND last_seen >= ?"
            params.append(since_ts)
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT year, COUNT(*) AS count
                FROM aircraft_registry
                WHERE year IS NOT NULL AND year != ''
                  AND CAST(year AS INTEGER) BETWEEN 1940 AND 2030
                  {since_clause}
                GROUP BY year
                ORDER BY year
            """, params).fetchall()
        return [{"year": r["year"], "count": r["count"]} for r in rows]

    def query_notable(self, limit: int, flag: str, days: int | None = None) -> list[dict]:
        # flag is validated by caller against a whitelist
        if flag == "all":
            where = "ar.foreign_military OR ar.interesting OR ar.rare OR ar.first_seen_flag"
        elif flag == "home_military":
            where = "ar.military AND NOT ar.foreign_military"
        else:
            where = f"ar.{flag}"
        params: list = []
        if days is not None:
            cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
            where += " AND ar.last_seen >= ?"
            params.append(cutoff_ts)
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(f"""
                WITH type_counts AS (
                    SELECT type_code, COUNT(*) AS tc
                    FROM aircraft_registry
                    WHERE type_code IS NOT NULL AND type_code != ''
                    GROUP BY type_code
                )
                SELECT ar.icao, ar.registration, ar.type_code, ar.type_category,
                       ar.military, ar.country, ar.operator, ar.manufacturer, ar.year,
                       ar.foreign_military, ar.interesting, ar.rare, ar.first_seen_flag,
                       ar.first_seen, ar.last_seen, ar.sighting_count,
                       COALESCE(tc.tc, 1)          AS type_count,
                       1.0 / COALESCE(tc.tc, 1)   AS type_rarity
                FROM aircraft_registry ar
                LEFT JOIN type_counts tc ON ar.type_code = tc.type_code
                WHERE {where}
                ORDER BY type_rarity DESC, ar.last_seen DESC
                LIMIT ?
            """, params).fetchall()
        return [dict(r) for r in rows]

    def query_unique_sightings(self, limit: int, days: int | None = None) -> list[dict]:
        """Aircraft with sighting_count = 1 (seen in only one session ever).
        Optional days filter restricts to aircraft last seen within that window."""
        params: list = []
        extra_where = ""
        if days is not None:
            cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
            extra_where = "AND ar.last_seen >= ?"
            params.append(cutoff_ts)
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(f"""
                WITH type_counts AS (
                    SELECT type_code, COUNT(*) AS tc
                    FROM aircraft_registry
                    WHERE type_code IS NOT NULL AND type_code != ''
                    GROUP BY type_code
                )
                SELECT ar.icao, ar.registration, ar.type_code, ar.type_category,
                       ar.military, ar.country, ar.operator, ar.manufacturer, ar.year,
                       ar.foreign_military, ar.interesting, ar.rare, ar.first_seen_flag,
                       ar.first_seen, ar.last_seen, ar.sighting_count,
                       COALESCE(tc.tc, 1)         AS type_count,
                       1.0 / COALESCE(tc.tc, 1)  AS type_rarity
                FROM aircraft_registry ar
                LEFT JOIN type_counts tc ON ar.type_code = tc.type_code
                WHERE ar.sighting_count = 1
                {extra_where}
                ORDER BY ar.last_seen DESC
                LIMIT ?
            """, params).fetchall()
        return [dict(r) for r in rows]

    def query_calendar_group(
        self, months: int,
        type_codes: list[str] | None = None,
        category_prefix: str | None = None,
    ) -> list[dict]:
        """Daily count of unique aircraft from a type group (by type codes or category prefix)."""
        cutoff = (date.today() - timedelta(days=months * 30)).isoformat()
        with self._connect() as conn:
            if category_prefix:
                rows = conn.execute("""
                    SELECT das.date, COUNT(DISTINCT das.icao) AS value
                    FROM daily_aircraft_seen das
                    JOIN aircraft_registry ar ON das.icao = ar.icao
                    WHERE das.date >= ? AND ar.type_category LIKE ?
                    GROUP BY das.date
                    ORDER BY das.date
                """, (cutoff, category_prefix + '%')).fetchall()
            elif type_codes:
                placeholders = ','.join('?' * len(type_codes))
                rows = conn.execute(f"""
                    SELECT das.date, COUNT(DISTINCT das.icao) AS value
                    FROM daily_aircraft_seen das
                    JOIN aircraft_registry ar ON das.icao = ar.icao
                    WHERE das.date >= ? AND ar.type_code IN ({placeholders})
                    GROUP BY das.date
                    ORDER BY das.date
                """, [cutoff] + list(type_codes)).fetchall()
            else:
                return []
        return [{"date": r["date"], "value": r["value"]} for r in rows]

    def query_polar_bins(self, days: int, bearing_sectors: int = 32) -> dict:
        """Bin coverage samples into a polar grid for heatmap rendering.
        Range bands are always 25 nm wide so they align with the ring labels.
        Returns aggregated cell counts rather than individual points."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        sector_width = 360.0 / bearing_sectors
        with self._connect() as conn:
            total_samples = conn.execute(
                "SELECT COUNT(*) FROM coverage_samples WHERE ts >= ?", (cutoff,)
            ).fetchone()[0]
            if not total_samples:
                return {"bins": [], "max_range": 0, "band_nm": 25,
                        "sectors": bearing_sectors, "bands": 0}
            # Use 95th percentile of range to exclude bogus outlier decodes
            p95_offset = max(0, int(0.95 * total_samples) - 1)
            max_range_row = conn.execute(
                "SELECT range_nm FROM coverage_samples WHERE ts >= ? "
                "ORDER BY range_nm ASC LIMIT 1 OFFSET ?",
                (cutoff, p95_offset)
            ).fetchone()[0]
            if not max_range_row:
                return {"bins": [], "max_range": 0, "band_nm": 25,
                        "sectors": bearing_sectors, "bands": 0}
            import math
            # Round up to nearest 25 nm so bands align exactly with ring labels
            band_nm = 25
            max_range = math.ceil(max_range_row / band_nm) * band_nm
            range_bands = max_range // band_nm
            rows = conn.execute("""
                SELECT CAST(bearing_deg / ? AS INTEGER)  AS b,
                       CAST(range_nm    / ? AS INTEGER)  AS r,
                       COUNT(*)                          AS count
                FROM coverage_samples
                WHERE ts >= ?
                GROUP BY b, r
                ORDER BY b, r
            """, (sector_width, band_nm, cutoff)).fetchall()
        return {
            "bins": [{"b": r["b"], "r": min(r["r"], range_bands - 1), "count": r["count"]}
                     for r in rows],
            "max_range": max_range,
            "band_nm": band_nm,
            "sectors": bearing_sectors,
            "bands": range_bands,
        }

    def query_range_percentiles(self, days: int, sectors: int = 36) -> list[dict]:
        """Range percentiles (p50/p90/p95) per 10° bearing sector, computed in SQL."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        sector_width = 360.0 / sectors
        with self._connect() as conn:
            rows = conn.execute("""
                WITH ranked AS (
                    SELECT CAST(bearing_deg / ? AS INTEGER) AS sector,
                           range_nm,
                           ROW_NUMBER() OVER (PARTITION BY CAST(bearing_deg / ? AS INTEGER)
                                              ORDER BY range_nm)        AS rn,
                           COUNT(*)    OVER (PARTITION BY CAST(bearing_deg / ? AS INTEGER)) AS total
                    FROM coverage_samples
                    WHERE ts >= ?
                )
                SELECT sector,
                       COUNT(*) AS count,
                       MIN(CASE WHEN rn * 100 >= total * 50 THEN range_nm END) AS p50,
                       MIN(CASE WHEN rn * 100 >= total * 90 THEN range_nm END) AS p90,
                       MIN(CASE WHEN rn * 100 >= total * 95 THEN range_nm END) AS p95
                FROM ranked
                GROUP BY sector
                ORDER BY sector
            """, (sector_width, sector_width, sector_width, cutoff)).fetchall()

        result = []
        for r in rows:
            result.append({
                "bearing": round(r["sector"] * sector_width + sector_width / 2, 1),
                "p50": round(r["p50"], 1) if r["p50"] is not None else None,
                "p90": round(r["p90"], 1) if r["p90"] is not None else None,
                "p95": round(r["p95"], 1) if r["p95"] is not None else None,
                "count": r["count"],
            })
        return result

    def query_distributions(self) -> dict:
        """Percentile stats for key metrics across 1d, 7d, 30d windows.
        Returns p5/p25/p50/p75/p95 + mean + n for each metric/window.
        Signal values are returned as dBFS (negative; 0 = strongest)."""
        now = int(datetime.now(timezone.utc).timestamp())
        cutoffs = {"1d": now - 86400, "7d": now - 7 * 86400, "30d": now - 30 * 86400, "365d": now - 365 * 86400}

        def pct_sql(conn, col, table, cutoff, extra=""):
            row = conn.execute(f"""
                WITH ranked AS (
                    SELECT {col} AS v,
                           ROW_NUMBER() OVER (ORDER BY {col}) AS rn,
                           COUNT(*)    OVER ()               AS n
                    FROM {table}
                    WHERE ts >= ? {extra}
                )
                SELECT COUNT(*) AS n,
                       AVG(v)   AS mean,
                       MIN(CASE WHEN rn * 100 >= n *  5 THEN v END) AS p5,
                       MIN(CASE WHEN rn * 100 >= n * 25 THEN v END) AS p25,
                       MIN(CASE WHEN rn * 100 >= n * 50 THEN v END) AS p50,
                       MIN(CASE WHEN rn * 100 >= n * 75 THEN v END) AS p75,
                       MIN(CASE WHEN rn * 100 >= n * 95 THEN v END) AS p95
                FROM ranked
            """, (cutoff,)).fetchone()
            if not row or not row["n"]:
                return None
            return {k: (round(row[k], 2) if row[k] is not None else None)
                    for k in ("n", "mean", "p5", "p25", "p50", "p75", "p95")}

        def to_dbfs(d):
            """Convert a percentile dict from raw RSSI bytes to dBFS."""
            if d is None:
                return None
            return {k: (round(-v / 2, 1) if k != "n" and v is not None else v)
                    for k, v in d.items()}

        result: dict = {}
        with self._connect() as conn:
            for window, cutoff in cutoffs.items():
                msgs   = pct_sql(conn, "msg_mean",   "minute_stats",    cutoff)
                ac     = pct_sql(conn, "ac_total",   "minute_stats",    cutoff)
                sig    = to_dbfs(pct_sql(conn, "signal_avg", "minute_stats",    cutoff,
                                         "AND signal_avg IS NOT NULL"))
                rng    = pct_sql(conn, "range_nm",   "coverage_samples", cutoff, "AND range_nm > 0")
                for metric, val in (("msgs", msgs), ("aircraft", ac), ("signal", sig), ("range", rng)):
                    result.setdefault(metric, {})[window] = val
        return result

    def query_unique_aircraft_per_day(self, days: int) -> list[dict]:
        """Daily count of unique aircraft seen, from day_stats."""
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT date, unique_aircraft
                FROM day_stats
                WHERE date >= ?
                ORDER BY date
            """, (cutoff,)).fetchall()
        return [{"date": r["date"], "count": r["unique_aircraft"]} for r in rows]

    def query_completeness(self, days: int) -> list[dict]:
        """Daily reception completeness: % of the day's 1440 minutes that had data."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT date(ts, 'unixepoch') AS day,
                       MIN(100.0, ROUND(COUNT(*) * 100.0 / 1440, 1)) AS pct
                FROM minute_stats
                WHERE ts >= ?
                GROUP BY day
                ORDER BY day
            """, (cutoff,)).fetchall()
        return [{"date": r["day"], "pct": r["pct"]} for r in rows]

    def query_position_decode_rate(self, days: int) -> list[dict]:
        """Daily position decode breakdown: adsb_pct, mlat_pct, no_pos_pct.
        Uses ac_with_pos/ac_mlat columns when available, else falls back to
        coverage_samples join (pos only, no MLAT split)."""
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            # New data: minutes that have the ac_with_pos column populated
            new_rows = conn.execute("""
                SELECT date(ts, 'unixepoch') AS day,
                       ROUND(AVG(CASE WHEN ac_total > 0
                             THEN ac_with_pos * 100.0 / ac_total END), 1) AS pos_pct,
                       ROUND(AVG(CASE WHEN ac_total > 0
                             THEN ac_mlat * 100.0 / ac_total END), 1) AS mlat_pct
                FROM minute_stats
                WHERE ts >= ? AND ac_with_pos IS NOT NULL AND ac_total > 0
                GROUP BY day ORDER BY day
            """, (cutoff,)).fetchall()
            new_days = {r["day"] for r in new_rows}

            # Legacy data: use coverage_samples join, no MLAT split
            old_rows = conn.execute("""
                WITH daily_ac AS (
                    SELECT date(ts, 'unixepoch') AS day,
                           SUM(ac_total)         AS total_ac_mins
                    FROM minute_stats
                    WHERE ts >= ? AND ac_total > 0
                    GROUP BY day
                ),
                daily_pos AS (
                    SELECT date(cs.ts, 'unixepoch') AS day,
                           COUNT(*)                 AS pos_ac_mins
                    FROM coverage_samples cs WHERE cs.ts >= ?
                    GROUP BY day
                )
                SELECT a.day,
                       MIN(100.0, ROUND(COALESCE(p.pos_ac_mins, 0) * 100.0 / a.total_ac_mins, 1)) AS pos_pct
                FROM daily_ac a
                LEFT JOIN daily_pos p ON a.day = p.day
                ORDER BY a.day
            """, (cutoff, cutoff)).fetchall()

        new_by_day = {r["day"]: {"pos_pct": r["pos_pct"], "mlat_pct": r["mlat_pct"]}
                      for r in new_rows}
        result = []
        for r in old_rows:
            day = r["day"]
            if day in new_by_day:
                d = new_by_day[day]
                result.append({"date": day, "pos_pct": d["pos_pct"], "mlat_pct": d["mlat_pct"]})
            else:
                result.append({"date": day, "pos_pct": r["pos_pct"], "mlat_pct": None})
        return result

    def query_military_icaos(self) -> list[str]:
        """Return all ICAO addresses flagged military in the registry."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT icao FROM aircraft_registry WHERE military = 1"
            ).fetchall()
            return [r["icao"] for r in rows]

    def fix_military_countries(self, corrections: dict, home_country: str) -> None:
        """Bulk-update country and foreign_military for military aircraft using ICAO block data.
        Called at startup to repair entries written before the registration-prefix bug was fixed."""
        home = (home_country or "").lower()
        with self._connect() as conn:
            for icao, country in corrections.items():
                foreign_mil = int(bool(home and country and country.lower() != home))
                conn.execute(
                    "UPDATE aircraft_registry SET country = ?, foreign_military = ? WHERE icao = ?",
                    (country, foreign_mil, icao),
                )
        log.info("DB: corrected country/foreign_military for %d military aircraft", len(corrections))

    def get_aircraft_registry_entry(self, icao: str) -> dict | None:
        """Return raw aircraft_registry row as a dict, or None if not found."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM aircraft_registry WHERE icao = ?", (icao.upper(),)
            ).fetchone()
            return dict(row) if row else None

    def update_aircraft_field(self, icao: str, field: str, value) -> None:
        """Overwrite a single field in aircraft_registry (field already validated by router).
        When country or military changes, foreign_military is recalculated immediately."""
        icao = icao.upper()
        with self._connect() as conn:
            conn.execute(
                f"UPDATE aircraft_registry SET {field} = ? WHERE icao = ?",
                (value, icao),
            )
            # Recalculate foreign_military whenever country or military changes
            if field in ("country", "military"):
                home = (config.HOME_COUNTRY or "").lower()
                conn.execute("""
                    UPDATE aircraft_registry
                    SET foreign_military = CASE
                        WHEN military = 1
                             AND ? != ''
                             AND country IS NOT NULL
                             AND LOWER(country) != ?
                        THEN 1 ELSE 0 END
                    WHERE icao = ?
                """, (home, home, icao))

    def update_aircraft_enrichment(
        self, icao: str,
        registration: str | None,
        type_code: str | None,
        type_category: str | None,
        operator: str | None,
        manufacturer: str | None,
    ) -> None:
        """Write hexdb-sourced enrichment to registry, only filling NULL columns.
        Called for both live and offline aircraft after a hexdb lookup completes."""
        with self._connect() as conn:
            conn.execute("""
                UPDATE aircraft_registry SET
                    registration  = COALESCE(registration, ?),
                    type_code     = COALESCE(type_code, ?),
                    type_category = COALESCE(type_category, ?),
                    operator      = COALESCE(operator, ?),
                    manufacturer  = COALESCE(manufacturer, ?)
                WHERE icao = ?
            """, (registration, type_code, type_category, operator, manufacturer, icao))

    def force_update_aircraft_enrichment(
        self, icao: str,
        registration: str | None,
        type_code: str | None,
        type_category: str | None,
        operator: str | None,
        manufacturer: str | None,
        year: str | None,
        country: str | None,
    ) -> None:
        """Overwrite enrichment fields unconditionally (used by manual refresh)."""
        with self._connect() as conn:
            conn.execute("""
                UPDATE aircraft_registry SET
                    registration  = COALESCE(?, registration),
                    type_code     = COALESCE(?, type_code),
                    type_category = COALESCE(?, type_category),
                    operator      = COALESCE(?, operator),
                    manufacturer  = COALESCE(?, manufacturer),
                    year          = COALESCE(?, year),
                    country       = COALESCE(?, country)
                WHERE icao = ?
            """, (registration, type_code, type_category, operator,
                  manufacturer, year, country, icao))

    def purge_ghost_aircraft(self) -> int:
        """Delete registry entries that look like bogus CRC decodes:
        single sighting with no enrichment data (no registration, type, or operator).
        Also removes their daily_aircraft_seen rows."""
        with self._connect() as conn:
            cursor = conn.execute("""
                DELETE FROM aircraft_registry
                WHERE sighting_count = 1
                  AND registration IS NULL
                  AND type_code IS NULL
                  AND operator IS NULL
            """)
            deleted = cursor.rowcount
            if deleted:
                conn.execute("""
                    DELETE FROM daily_aircraft_seen
                    WHERE icao NOT IN (SELECT icao FROM aircraft_registry)
                """)
        if deleted:
            log.info("DB: purged %d ghost aircraft entries", deleted)
        return deleted

    def query_top_aircraft(self, limit: int = 20, since_ts: int | None = None, military: int | None = None) -> list[dict]:
        """Top aircraft by sighting count, optionally within a time window or filtered by military flag."""
        params: list = []
        clauses = []
        if since_ts is not None:
            clauses.append("last_seen >= ?")
            params.append(since_ts)
        if military is not None:
            clauses.append("military = ?")
            params.append(military)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(f"""
                SELECT icao, registration, type_code, operator, country, military,
                       sighting_count, first_seen, last_seen
                FROM aircraft_registry
                {where}
                ORDER BY sighting_count DESC
                LIMIT ?
            """, params).fetchall()
        return [dict(r) for r in rows]

    def query_azimuth_elevation(self, days: int, max_points: int = 4000) -> list[dict]:
        """Bearing vs elevation angle scatter, coloured by range.
        Elevation computed from altitude and slant range. Downsampled to max_points."""
        import math
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as conn:
            total = conn.execute("""
                SELECT COUNT(*) FROM coverage_samples
                WHERE ts >= ? AND range_nm > 0 AND altitude IS NOT NULL AND altitude > 0
            """, (cutoff,)).fetchone()[0]
            stride = max(1, -(-total // max_points))  # ceiling division → result ≤ max_points
            rows = conn.execute("""
                SELECT bearing_deg, range_nm, altitude
                FROM coverage_samples
                WHERE ts >= ? AND range_nm > 0 AND altitude IS NOT NULL AND altitude > 0
                  AND (rowid % ?) = 0
            """, (cutoff, stride)).fetchall()
        result = []
        for r in rows:
            alt_m   = r["altitude"] * 0.3048
            range_m = r["range_nm"] * 1852.0
            el = math.degrees(math.atan2(alt_m, range_m))
            result.append({
                "bearing":   round(r["bearing_deg"], 1),
                "elevation": round(el, 1),
                "range":     round(r["range_nm"], 1),
            })
        return result

    def query_needs_enrichment(self, limit: int = 500) -> list[str]:
        """ICAOs with missing operator/registration/type, ordered by most recently seen."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT icao FROM aircraft_registry
                WHERE operator IS NULL OR registration IS NULL OR type_code IS NULL
                ORDER BY last_seen DESC
                LIMIT ?
            """, (limit,)).fetchall()
        return [r["icao"] for r in rows]

    def query_all_sighting_counts(self) -> dict[str, int]:
        """Return {icao: sighting_count} for all aircraft in the registry."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT icao, sighting_count FROM aircraft_registry"
            ).fetchall()
        return {r["icao"]: r["sighting_count"] for r in rows}

    def query_sighting_counts_for_icaos(self, icaos: list[str]) -> dict[str, int]:
        """Return {icao: sighting_count} for the given ICAO list."""
        if not icaos:
            return {}
        placeholders = ','.join('?' * len(icaos))
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT icao, sighting_count FROM aircraft_registry WHERE icao IN ({placeholders})",
                icaos,
            ).fetchall()
        return {r["icao"]: r["sighting_count"] for r in rows}

    def query_status(self) -> dict:
        """DB size, per-table row counts, date ranges, and retention policy summary."""
        import os
        db_size = os.path.getsize(str(config.DB_PATH)) if config.DB_PATH.exists() else 0

        tables = [
            ("minute_stats",          "ts",        True,  config.MINUTE_STATS_RETENTION_DAYS),
            ("minute_df_counts",      "ts",        True,  config.MINUTE_STATS_RETENTION_DAYS),
            ("minute_type_counts",    "ts",        True,  config.MINUTE_STATS_RETENTION_DAYS),
            ("minute_operator_counts","ts",        True,  config.MINUTE_STATS_RETENTION_DAYS),
            ("daily_aircraft_seen",   "date",      True,  config.MINUTE_STATS_RETENTION_DAYS),
            ("day_stats",             "date",      False, None),
            ("aircraft_registry",     None,        False, None),
            ("coverage_samples",      "ts",        True,  90),
            ("acas_events",           "ts",        True,  90),
        ]

        result = []
        with self._connect() as conn:
            for tbl, ts_col, expires, ret_days in tables:
                row_count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                oldest = newest = None
                if ts_col and row_count:
                    bounds = conn.execute(
                        f"SELECT MIN({ts_col}), MAX({ts_col}) FROM {tbl}"
                    ).fetchone()
                    oldest, newest = bounds[0], bounds[1]
                result.append({
                    "table":       tbl,
                    "rows":        row_count,
                    "oldest":      oldest,
                    "newest":      newest,
                    "expires":     expires,
                    "retain_days": ret_days,
                })

        return {
            "db_size_bytes": db_size,
            "tables": result,
            "config": {
                "minute_stats_retention_days": config.MINUTE_STATS_RETENTION_DAYS,
                "coverage_retention_days":     90,
                "acas_retention_days":         90,
                "ghost_filter_msgs":           config.GHOST_FILTER_MSGS,
                "rare_threshold":              config.RARE_THRESHOLD,
            },
        }


# Module-level singleton
stats_db = StatsDB()
