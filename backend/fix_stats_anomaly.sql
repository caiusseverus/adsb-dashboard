-- Fix stats anomalies from Pi overload periods.
-- 2026-03-19: ghost aircraft accumulation; ac_total clamped to graphs1090 RRD values.
-- 2026-03-26: 3 minutes with impossible message rates; rows deleted.
--
-- Back up before running:
--   cp backend/data/adsb.db backend/data/adsb.db.bak

BEGIN;

-- ============================================================
-- 2026-03-19: clamp ac_total / ac_civil to RRD values
-- Bad window: 01:14–14:38 UTC
-- RRD step: 900 s; bucket assigned via ((ts/900)+1)*900
-- ac_military set to 0 (split unrecoverable; military count is
-- typically 0–3 so the error is negligible)
-- ============================================================
UPDATE minute_stats
SET
  ac_total    = CASE ((ts / 900) + 1) * 900
    WHEN 1773882900 THEN 14
    WHEN 1773883800 THEN 12
    WHEN 1773884700 THEN 13
    WHEN 1773885600 THEN 11
    WHEN 1773886500 THEN 15
    WHEN 1773887400 THEN 24
    WHEN 1773888300 THEN 26
    WHEN 1773889200 THEN 24
    WHEN 1773890100 THEN 29
    WHEN 1773891000 THEN 32
    WHEN 1773891900 THEN 39
    WHEN 1773892800 THEN 43
    WHEN 1773893700 THEN 52
    WHEN 1773894600 THEN 58
    WHEN 1773895500 THEN 57
    WHEN 1773896400 THEN 53
    WHEN 1773897300 THEN 66
    WHEN 1773898200 THEN 82
    WHEN 1773899100 THEN 94
    WHEN 1773900000 THEN 99
    WHEN 1773900900 THEN 121
    WHEN 1773901800 THEN 170
    WHEN 1773902700 THEN 228
    WHEN 1773903600 THEN 249
    WHEN 1773904500 THEN 258
    WHEN 1773905400 THEN 252
    WHEN 1773906300 THEN 266
    WHEN 1773907200 THEN 260
    WHEN 1773908100 THEN 268
    WHEN 1773909000 THEN 252
    WHEN 1773909900 THEN 249
    WHEN 1773910800 THEN 254
    WHEN 1773911700 THEN 264
    WHEN 1773912600 THEN 283
    WHEN 1773913500 THEN 275
    WHEN 1773914400 THEN 281
    WHEN 1773915300 THEN 290
    WHEN 1773916200 THEN 305
    WHEN 1773917100 THEN 300
    WHEN 1773918000 THEN 309
    WHEN 1773918900 THEN 316
    WHEN 1773919800 THEN 322
    WHEN 1773920700 THEN 334
    WHEN 1773921600 THEN 324
    WHEN 1773922500 THEN 319
    WHEN 1773923400 THEN 334
    WHEN 1773924300 THEN 334
    WHEN 1773925200 THEN 310
    WHEN 1773926100 THEN 296
    WHEN 1773927000 THEN 288
    WHEN 1773927900 THEN 281
    WHEN 1773928800 THEN 279
    WHEN 1773929700 THEN 297
    WHEN 1773930600 THEN 295
    WHEN 1773931500 THEN 286
    ELSE ac_total
  END,
  ac_civil    = CASE ((ts / 900) + 1) * 900
    WHEN 1773882900 THEN 14
    WHEN 1773883800 THEN 12
    WHEN 1773884700 THEN 13
    WHEN 1773885600 THEN 11
    WHEN 1773886500 THEN 15
    WHEN 1773887400 THEN 24
    WHEN 1773888300 THEN 26
    WHEN 1773889200 THEN 24
    WHEN 1773890100 THEN 29
    WHEN 1773891000 THEN 32
    WHEN 1773891900 THEN 39
    WHEN 1773892800 THEN 43
    WHEN 1773893700 THEN 52
    WHEN 1773894600 THEN 58
    WHEN 1773895500 THEN 57
    WHEN 1773896400 THEN 53
    WHEN 1773897300 THEN 66
    WHEN 1773898200 THEN 82
    WHEN 1773899100 THEN 94
    WHEN 1773900000 THEN 99
    WHEN 1773900900 THEN 121
    WHEN 1773901800 THEN 170
    WHEN 1773902700 THEN 228
    WHEN 1773903600 THEN 249
    WHEN 1773904500 THEN 258
    WHEN 1773905400 THEN 252
    WHEN 1773906300 THEN 266
    WHEN 1773907200 THEN 260
    WHEN 1773908100 THEN 268
    WHEN 1773909000 THEN 252
    WHEN 1773909900 THEN 249
    WHEN 1773910800 THEN 254
    WHEN 1773911700 THEN 264
    WHEN 1773912600 THEN 283
    WHEN 1773913500 THEN 275
    WHEN 1773914400 THEN 281
    WHEN 1773915300 THEN 290
    WHEN 1773916200 THEN 305
    WHEN 1773917100 THEN 300
    WHEN 1773918000 THEN 309
    WHEN 1773918900 THEN 316
    WHEN 1773919800 THEN 322
    WHEN 1773920700 THEN 334
    WHEN 1773921600 THEN 324
    WHEN 1773922500 THEN 319
    WHEN 1773923400 THEN 334
    WHEN 1773924300 THEN 334
    WHEN 1773925200 THEN 310
    WHEN 1773926100 THEN 296
    WHEN 1773927000 THEN 288
    WHEN 1773927900 THEN 281
    WHEN 1773928800 THEN 279
    WHEN 1773929700 THEN 297
    WHEN 1773930600 THEN 295
    WHEN 1773931500 THEN 286
    ELSE ac_civil
  END,
  ac_military = 0
WHERE ts >= 1773882840   -- 2026-03-19 01:14:00 UTC
  AND ts <= 1773931080;  -- 2026-03-19 14:38:00 UTC

-- ============================================================
-- 2026-03-19: delete inflated type/operator counts for bad window
-- No external reference exists to reconstruct the split, so
-- deletion is preferable to wrong data. day_type_counts is
-- rebuilt below from the surviving good minutes.
-- ============================================================
DELETE FROM minute_type_counts
  WHERE ts >= 1773882840 AND ts <= 1773931080;

DELETE FROM minute_operator_counts
  WHERE ts >= 1773882840 AND ts <= 1773931080;

-- ============================================================
-- 2026-03-26: delete 3 minutes with impossible message rates
-- ============================================================
DELETE FROM minute_stats
  WHERE ts IN (
    CAST(strftime('%s', '2026-03-26 01:53:00') AS INTEGER),
    CAST(strftime('%s', '2026-03-26 01:57:00') AS INTEGER),
    CAST(strftime('%s', '2026-03-26 02:08:00') AS INTEGER)
  );

DELETE FROM minute_df_counts
  WHERE ts IN (
    CAST(strftime('%s', '2026-03-26 01:53:00') AS INTEGER),
    CAST(strftime('%s', '2026-03-26 01:57:00') AS INTEGER),
    CAST(strftime('%s', '2026-03-26 02:08:00') AS INTEGER)
  );

DELETE FROM minute_type_counts
  WHERE ts IN (
    CAST(strftime('%s', '2026-03-26 01:53:00') AS INTEGER),
    CAST(strftime('%s', '2026-03-26 01:57:00') AS INTEGER),
    CAST(strftime('%s', '2026-03-26 02:08:00') AS INTEGER)
  );

DELETE FROM minute_operator_counts
  WHERE ts IN (
    CAST(strftime('%s', '2026-03-26 01:53:00') AS INTEGER),
    CAST(strftime('%s', '2026-03-26 01:57:00') AS INTEGER),
    CAST(strftime('%s', '2026-03-26 02:08:00') AS INTEGER)
  );

-- ============================================================
-- Rebuild day_stats and day_type_counts for both affected dates
-- (mirrors _rollup_day logic from db.py)
-- ============================================================
DELETE FROM day_type_counts WHERE date = '2026-03-19';
INSERT INTO day_type_counts (date, type_code, count)
SELECT '2026-03-19', type_code, MAX(count)
FROM minute_type_counts
WHERE date(ts, 'unixepoch') = '2026-03-19'
GROUP BY type_code;

INSERT OR REPLACE INTO day_stats
    (date, msg_total, msg_max, ac_peak, ac_civil_peak, ac_military_peak, unique_aircraft)
SELECT '2026-03-19',
    CAST(SUM(msg_mean * 60) AS INTEGER),
    (SELECT MAX(msg_mean) FROM (
        SELECT msg_mean,
               ROW_NUMBER() OVER (ORDER BY msg_mean) AS rn,
               COUNT(*) OVER () AS n
        FROM minute_stats WHERE date(ts, 'unixepoch') = '2026-03-19'
    ) WHERE rn * 100 <= n * 95),
    MAX(ac_total), MAX(ac_civil), MAX(ac_military),
    (SELECT COUNT(*) FROM daily_aircraft_seen WHERE date = '2026-03-19')
FROM minute_stats WHERE date(ts, 'unixepoch') = '2026-03-19';

INSERT OR REPLACE INTO day_stats
    (date, msg_total, msg_max, ac_peak, ac_civil_peak, ac_military_peak, unique_aircraft)
SELECT '2026-03-26',
    CAST(SUM(msg_mean * 60) AS INTEGER),
    (SELECT MAX(msg_mean) FROM (
        SELECT msg_mean,
               ROW_NUMBER() OVER (ORDER BY msg_mean) AS rn,
               COUNT(*) OVER () AS n
        FROM minute_stats WHERE date(ts, 'unixepoch') = '2026-03-26'
    ) WHERE rn * 100 <= n * 95),
    MAX(ac_total), MAX(ac_civil), MAX(ac_military),
    (SELECT COUNT(*) FROM daily_aircraft_seen WHERE date = '2026-03-26')
FROM minute_stats WHERE date(ts, 'unixepoch') = '2026-03-26';

COMMIT;
