-- value_capture/blocks/pcl_sales_modal_block.sql
-- NOTE: this file stays PER-COHORT / START-DATE windowed for granular presentation. The quarterly
--   partner-sheet rollup (value_capture_report.sql) uses a DIFFERENT window (treatment END date) and
--   a client-level first-touch dedup across the whole quarter -- do not sum this file's output across
--   cohort_month, it will double-count multi-cohort clients. See value_capture_report.sql's header.
-- Value-capture interchange-contract rows for PCL Sales Modal.
-- STRICT RE-GRAIN of campaigns/sales_modal/pcl/p9_vcl_full_measurement.sql's population/arm/success
--   logic (pop/pop1 CTEs copied verbatim below: same window, same arm CASE on report_groups_period,
--   same first-deployment ROW_NUMBER dedup, same responder_cli success flag). p9's GA4 exposure/
--   dismiss join (modal/per_client/segmented CTEs) is DROPPED here — this is an ITT arm contrast,
--   exposure not needed for the interchange contract.
-- NO NEW MEASUREMENT LOGIC: this is a re-aggregation of p9's population to a coarser grain.
--   Reconciliation — p9's own output (grouped by cohort_month, arm, strategy, product_current,
--   product_grouping_current, decile, engagement, exposure_bin) summed over every dimension EXCEPT
--   cohort_month and arm reproduces this block's counts exactly:
--     test_clients    (arm=challenger) = SUM(p9.clients)            WHERE arm='challenger', per cohort_month
--     test_successes  (arm=challenger) = SUM(p9.converted_clients)  WHERE arm='challenger', per cohort_month
--     control_clients (arm=champion)   = SUM(p9.clients)            WHERE arm='champion',   per cohort_month
--     control_successes(arm=champion)  = SUM(p9.converted_clients)  WHERE arm='champion',   per cohort_month
--   This holds because p9's per_client CTE groups by clnt_no first (one row per client per cell), so
--   its cells partition the population with no double-count or gap.
-- Engine: TERADATA-DIRECT (bare table name, NO catalog prefix -- do NOT run through Starburst).
--   Curated table DL_MR_PROD.cards_pli_decision_resp. Counts only.
-- 9881-safe: clnt_no raw, uncast (no Teradata ROUND pushdown), same as p9.
-- cohort_month kept per repo hard rule; pooling across cohort_month happens downstream in
--   value_capture_report.sql (also Teradata-direct).

WITH pop AS (
  SELECT
    clnt_no,                                    -- raw, uncast (no Teradata ROUND pushdown)
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    responder_cli,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn   -- first deployment, per p9
  FROM DL_MR_PROD.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'   -- EDIT POINT: window
),
pop1 AS (
  SELECT clnt_no, arm, responder_cli,
         CAST(CAST(treatmt_strt_dt AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7)) AS cohort_month
  FROM pop WHERE rn = 1
),
by_cohort AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT CASE WHEN arm = 'challenger' THEN clnt_no END)                       AS test_clients,
    COUNT(DISTINCT CASE WHEN arm = 'challenger' AND responder_cli = 1 THEN clnt_no END)  AS test_successes,
    COUNT(DISTINCT CASE WHEN arm = 'champion'   THEN clnt_no END)                        AS control_clients,
    COUNT(DISTINCT CASE WHEN arm = 'champion'   AND responder_cli = 1 THEN clnt_no END)  AS control_successes
  FROM pop1
  GROUP BY cohort_month
),
pop_window AS (
  -- one-row aggregate, CROSS JOINed below (Teradata is unreliable with scalar subselects in a
  -- CTE's SELECT list, unlike the Trino version this replaces)
  SELECT MIN(treatmt_strt_dt) AS trt_start_dt, MAX(treatmt_strt_dt) AS trt_end_dt
  FROM pop
)
SELECT
  'PCL'                                                     AS mne,
  'Sales Modal (served) vs BAU (not served)'                 AS test_desc,
  w.trt_start_dt                                             AS trt_start_dt,   -- MIN of window dates actually in data
  w.trt_end_dt                                               AS trt_end_dt,     -- MAX of window dates actually in data
  'Credit limit increase accepted'                           AS success_name,
  'overall'                                                   AS stratum,
  by_cohort.cohort_month,
  test_clients,
  test_successes,
  control_clients,
  control_successes
FROM by_cohort
CROSS JOIN pop_window w
ORDER BY cohort_month;
