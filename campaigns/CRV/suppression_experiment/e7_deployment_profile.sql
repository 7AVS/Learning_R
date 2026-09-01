-- ============================================================================
-- e7 — deployment cadence + offer-window profile
-- CRV deploys near-daily (11 tactic ids Aug 14-28; e1 proved each account
-- appears ONCE across them — rolling slices, not re-decisioning).
-- Decision this query answers (ONE read, ~11 rows — one per deployment day):
--   1) Volume per deployment slice (does the universe accumulate evenly?)
--   2) treatmt_end_dt structure: one shared end date vs rolling per-slice
--      (defines what "offer window" means for maturity/90-day convention)
--   3) TG mix per slice (holdout drawn every slice, or front-loaded?)
-- Rerun after September deployments load to see cycle behavior (do accounts
-- reappear? frozen-cell CTE in e4-e6 already guards if they do).
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================

SELECT
    TRUNC(treatmt_strt_dt, 'MON')       AS cohort_month,
    treatmt_strt_dt                     AS deploy_dt,
    COUNT(DISTINCT tactic_id)           AS tactic_ids,
    COUNT(*)                            AS row_ct,
    COUNT(DISTINCT visa_acct_no)        AS acct_ct,          -- = row_ct if still no repeats
    COUNT(DISTINCT treatmt_end_dt)      AS distinct_end_dts,
    MIN(treatmt_end_dt)                 AS min_end_dt,
    MAX(treatmt_end_dt)                 AS max_end_dt,
    SUM(CASE WHEN tst_grp_cd = 'TG8' THEN 1 ELSE 0 END) AS tg8_ct,
    SUM(CASE WHEN tst_grp_cd = 'TG4' THEN 1 ELSE 0 END) AS tg4_ct,
    SUM(CASE WHEN tst_grp_cd = 'TG1' THEN 1 ELSE 0 END) AS tg1_ct
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'
  AND treatmt_strt_dt >= DATE '2026-08-14'
GROUP BY 1, 2
ORDER BY 2
