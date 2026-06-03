-- diagnostic_conversion.sql
-- Engine: Teradata NATIVE (DDWV01 CR_APP chain only, no GA4, no Trino).
-- Purpose: check whether the O2P conversion signal "hard stops" because of a DATA frontier
--   (completion lag / load recency) rather than a real end of conversions.
--   O2P success requires a COMPLETED app (prod_app_compl_dt NOT NULL). Recent apps still in
--   the pipeline are excluded -> the most recent deployment's vintage tail looks flat and will
--   backfill as those apps complete. Older deployments look full because they're past maturation.
--
-- prod_app_dt        = application submitted date (drives vintage_day)
-- prod_app_compl_dt  = completion/approval date (the gate; null = still pending)


-- STATEMENT 1 — overall conversion data frontier + completion-lag check
SELECT
    MAX(d.prod_app_dt)                                              AS max_app_dt,
    MAX(d.prod_app_compl_dt)                                        AS max_compl_dt,
    COUNT(*)                                                        AS n_apps,
    COUNT(d.prod_app_compl_dt)                                      AS n_completed,
    COUNT(CASE WHEN d.prod_app_dt >= DATE '2026-05-01' THEN 1 END)  AS apps_since_may1,
    COUNT(CASE WHEN d.prod_app_dt >= DATE '2026-05-01'
                AND d.prod_app_compl_dt IS NULL THEN 1 END)         AS since_may1_still_pending
FROM DDWV01.OVRL_CR_APP b
JOIN DDWV01.CR_APP_PROD d
    ON d.cr_app_id = b.cr_app_id AND d.sys_src_id = b.sys_src_id
WHERE b.app_typ = 'P'
  AND d.appl_for_prod_typ IN ('40','41','43')
  AND d.prod_app_dt >= DATE '2026-04-01'
;
-- Read:
--   max_compl_dt ~ early May          -> frontier is the completion/load date (backfills over time)
--   apps_since_may1 high, most pending -> completion lag (apps exist, not yet completed)
--   max_app_dt ~ early May            -> table itself not loaded past then (pure recency lag)


-- STATEMENT 2 — daily taper: submitted vs completed apps by date (see where it falls off)
SELECT
    d.prod_app_dt               AS app_dt,
    COUNT(*)                    AS apps_submitted,
    COUNT(d.prod_app_compl_dt)  AS apps_completed
FROM DDWV01.OVRL_CR_APP b
JOIN DDWV01.CR_APP_PROD d
    ON d.cr_app_id = b.cr_app_id AND d.sys_src_id = b.sys_src_id
WHERE b.app_typ = 'P'
  AND d.appl_for_prod_typ IN ('40','41','43')
  AND d.prod_app_dt >= DATE '2026-04-01'
GROUP BY 1
ORDER BY 1
;


-- STATEMENT 3 — RAW frontier: CR_APP_PROD alone, no join, no app_typ / product-type filters.
-- Isolates whether May-5 is a table-wide load frontier or just the P / 40-41-43 subset.
-- If max_app_dt_raw runs past May 5, the cap is the filter/product mix, not table recency.
SELECT
    MAX(prod_app_dt)       AS max_app_dt_raw,
    MAX(prod_app_compl_dt) AS max_compl_dt_raw,
    COUNT(*)               AS n_rows
FROM DDWV01.CR_APP_PROD
WHERE prod_app_dt >= DATE '2026-04-01'
;


-- STATEMENT 4 — discover OVRL_CR_APP's columns (find its date field; not guessing the name).
-- Look for an app/decision/submit date column, then MAX() it for that table's own frontier.
SELECT ColumnName, ColumnType
FROM DBC.ColumnsV
WHERE DatabaseName = 'DDWV01' AND TableName = 'OVRL_CR_APP'
ORDER BY ColumnId
;


-- STATEMENT 5 — is OVRL_CR_APP the join limiter?  (no date column needed)
-- Of CR_APP_PROD rows submitted AFTER May 5, how many have a matching OVRL_CR_APP row.
-- If prod_rows_after_may5 > 0 but matched_in_ovrl ~ 0 -> OVRL_CR_APP lacks the recent rows
--   and is what caps the joined query at May 5.
SELECT
    COUNT(*)            AS prod_rows_after_may5,
    COUNT(b.cr_app_id)  AS matched_in_ovrl
FROM DDWV01.CR_APP_PROD d
LEFT JOIN DDWV01.OVRL_CR_APP b
    ON b.cr_app_id = d.cr_app_id AND b.sys_src_id = d.sys_src_id
WHERE d.prod_app_dt > DATE '2026-05-05'
;


-- STATEMENT 6 — OVRL_CR_APP own frontier (date cols confirmed via St4).
-- CAPTR_DT = load/capture date (ETL recency), APP_CREAT_DT = application created,
-- APPL_COMPL_DT = completion. MAX of each = where this table ends.
SELECT
    MAX(captr_dt)                                                AS max_captr_dt,
    MAX(app_creat_dt)                                            AS max_app_creat_dt,
    MAX(appl_compl_dt)                                           AS max_appl_compl_dt,
    COUNT(*)                                                     AS n_rows,
    COUNT(CASE WHEN app_creat_dt > DATE '2026-05-05' THEN 1 END) AS rows_after_may5
FROM DDWV01.OVRL_CR_APP
WHERE app_creat_dt >= DATE '2026-04-01'
;
-- Read:
--   max_captr_dt ~ today but max_app_creat_dt ~ May 5  -> table fresh, but upstream apps lag
--   max_captr_dt ~ May 5                               -> table itself not refreshed since then
--   rows_after_may5 = 0                                -> OVRL_CR_APP caps the join at May 5


-- STATEMENT 7 — DAILY (_DLY) frontier. These should be ~yesterday (the fix).
SELECT 'OVRL_CR_APP_DLY' AS tbl,
       MAX(captr_dt)      AS max_captr_dt,
       MAX(app_creat_dt)  AS max_app_creat_dt,
       MAX(appl_compl_dt) AS max_appl_compl_dt
FROM DDWV01.OVRL_CR_APP_DLY
WHERE captr_dt >= DATE '2026-05-01'
;
SELECT 'CR_APP_PROD_DLY' AS tbl,
       MAX(captr_dt)          AS max_captr_dt,
       MAX(prod_app_dt)       AS max_prod_app_dt,
       MAX(prod_app_compl_dt) AS max_prod_app_compl_dt
FROM DDWV01.CR_APP_PROD_DLY
WHERE captr_dt >= DATE '2026-05-01'
;


-- STATEMENT 8 — snapshot grain + PERSISTENCE: does a completed April app still appear in the
-- LATEST daily snapshot? Decides the dedup strategy for the production rewrite.
--   april_apps_in_latest > 0 (≈ April volume) -> apps persist -> dedup = latest captr_dt snapshot
--   april_apps_in_latest ~ 0                   -> apps drop out -> dedup = latest captr_dt PER app
SELECT
    (SELECT MAX(captr_dt) FROM DDWV01.OVRL_CR_APP_DLY)                       AS latest_snap,
    COUNT(*)                                                                AS rows_in_latest_snap,
    COUNT(DISTINCT cr_app_id)                                               AS distinct_apps_in_latest_snap,
    COUNT(CASE WHEN app_creat_dt BETWEEN DATE '2026-04-01' AND DATE '2026-04-30' THEN 1 END) AS april_apps_in_latest
FROM DDWV01.OVRL_CR_APP_DLY
WHERE captr_dt = (SELECT MAX(captr_dt) FROM DDWV01.OVRL_CR_APP_DLY)
;


-- STATEMENT 9 — does CR_APP_PROD_DLY have a snap_dt (cumulative) alongside captr_dt (delta)?
SELECT ColumnName, ColumnType
FROM DBC.ColumnsV
WHERE DatabaseName = 'DDWV01' AND TableName = 'CR_APP_PROD_DLY'
ORDER BY ColumnId
;

-- STATEMENT 10 — is captr_dt a DELTA or CUMULATIVE? app-date spread per recent captr_dt.
--   (bounded to June; the unbounded MAX-over-whole-table subquery spooled, so it's removed)
--   for the latest day: min_app_dt ~ same day -> DELTA;  min_app_dt back months -> CUMULATIVE
SELECT captr_dt,
       COUNT(*)          AS rows_on_captr,
       MIN(prod_app_dt)  AS min_app_dt,
       MAX(prod_app_dt)  AS max_app_dt
FROM DDWV01.CR_APP_PROD_DLY
WHERE captr_dt >= DATE '2026-06-01'
GROUP BY captr_dt
ORDER BY captr_dt
;
