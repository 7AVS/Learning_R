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
