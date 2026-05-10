-- PCQ Q1 26 -- Dashboard reconciliation diagnostic
-- Goal: figure out why our totals exceed the dashboard's.
-- Each block is independent. Run them one at a time and read the result.
-- Filters mirror the main pcq_q1_26_strategy_trend.sql, applying
-- the strictest dashboard-style cut (TPA + Period-ASC) so we are
-- comparing the same population.

DATABASE DL_MR_PROD;


-- ============================================================
-- BLOCK 1: side-by-side count definitions
-- One row, many columns. Compare each column against the
-- dashboard's deployed/approved numbers. The column that
-- matches reveals the rule the dashboard is using.
--
-- total_rows                  = our current "deployed"
-- distinct_clients            = if dashboard dedupes by clnt_no
-- distinct_accts              = if dashboard dedupes by acct_no
-- approved_rows               = our current "approved"
-- approved_with_acct          = approved AND acct_no populated
-- distinct_approved_clients   = clnt-deduped approved
-- distinct_approved_accts     = acct-deduped approved
-- approved_within_90d / 60d   = if dashboard caps to attribution window
-- ============================================================
SELECT
  COUNT(*)                                                                                AS total_rows,
  COUNT(DISTINCT clnt_no)                                                                 AS distinct_clients,
  COUNT(DISTINCT acct_no)                                                                 AS distinct_accts,
  SUM(CASE WHEN app_approved = 1 THEN 1 ELSE 0 END)                                       AS approved_rows,
  SUM(CASE WHEN app_approved = 1 AND acct_no IS NOT NULL THEN 1 ELSE 0 END)               AS approved_with_acct,
  COUNT(DISTINCT CASE WHEN app_approved = 1 THEN clnt_no END)                             AS distinct_approved_clients,
  COUNT(DISTINCT CASE WHEN app_approved = 1 THEN acct_no END)                             AS distinct_approved_accts,
  SUM(CASE WHEN app_approved = 1 AND (response_dt - treatmt_start_dt) <= 90 THEN 1 ELSE 0 END) AS approved_within_90d,
  SUM(CASE WHEN app_approved = 1 AND (response_dt - treatmt_start_dt) <= 60 THEN 1 ELSE 0 END) AS approved_within_60d
FROM cards_tpa_pcq_decision_resp
WHERE treatmt_start_dt >= DATE '2025-11-01'
  AND tpa_ita = 'TPA'
  AND asc_on_app_source = 'Period-ASC';


-- ============================================================
-- BLOCK 2: distinct values of app_approved
-- Confirms the field is strictly 0/1/NULL. If it can be 2 or
-- higher, our SUM is double-counting. Expected: just 0, 1, NULL.
-- ============================================================
SELECT
  app_approved,
  COUNT(*) AS n_rows
FROM cards_tpa_pcq_decision_resp
WHERE treatmt_start_dt >= DATE '2025-11-01'
GROUP BY app_approved
ORDER BY app_approved;


-- ============================================================
-- BLOCK 3: how many rows does each client have in the cohort?
-- If most clients have only 1 row -> no duplication, look elsewhere.
-- If many have 2/3/4 rows -> multi-wave duplication is the cause.
-- The right column to compare against the dashboard depends on this.
-- ============================================================
SELECT
  rows_per_client,
  COUNT(*) AS n_clients_with_this_count
FROM (
  SELECT clnt_no, COUNT(*) AS rows_per_client
  FROM cards_tpa_pcq_decision_resp
  WHERE treatmt_start_dt >= DATE '2025-11-01'
    AND tpa_ita = 'TPA'
    AND asc_on_app_source = 'Period-ASC'
  GROUP BY clnt_no
) t
GROUP BY rows_per_client
ORDER BY rows_per_client;


-- ============================================================
-- BLOCK 4: per-wave totals
-- Breaks the headline numbers down by treatmt_start_dt.
-- If one wave dominates the gap (e.g., a re-deployment wave
-- that fires the same clients again), it stands out here.
-- ============================================================
SELECT
  treatmt_start_dt,
  COUNT(*)                                            AS rows,
  COUNT(DISTINCT clnt_no)                             AS distinct_clients,
  SUM(CASE WHEN app_approved = 1 THEN 1 ELSE 0 END)   AS approved
FROM cards_tpa_pcq_decision_resp
WHERE treatmt_start_dt >= DATE '2025-11-01'
  AND tpa_ita = 'TPA'
  AND asc_on_app_source = 'Period-ASC'
GROUP BY treatmt_start_dt
ORDER BY treatmt_start_dt;


-- ============================================================
-- BLOCK 5: top 20 clients with the most deployments
-- Sample of the duplicate clients so you can see the pattern --
-- e.g., same clnt_no across 4 waves with 1 approval. Tells you
-- whether dedup should pick latest wave, first wave, or only-approved-wave.
-- ============================================================
SELECT TOP 20
  clnt_no,
  COUNT(*)                                            AS n_rows,
  MIN(treatmt_start_dt)                               AS first_wave,
  MAX(treatmt_start_dt)                               AS last_wave,
  SUM(CASE WHEN app_approved = 1 THEN 1 ELSE 0 END)   AS times_approved,
  COUNT(DISTINCT acct_no)                             AS distinct_accts
FROM cards_tpa_pcq_decision_resp
WHERE treatmt_start_dt >= DATE '2025-11-01'
  AND tpa_ita = 'TPA'
  AND asc_on_app_source = 'Period-ASC'
GROUP BY clnt_no
HAVING COUNT(*) > 1
ORDER BY n_rows DESC;
