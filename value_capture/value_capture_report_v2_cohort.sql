-- value_capture/value_capture_report_v2_cohort.sql
-- Cohort-month-grain sibling of value_capture_report_v2.sql. SQL counts clients only -- display
-- formatting, p-value, and partner-template mapping are Andre's job in Excel (REDESIGN_SPEC.md).
-- Engine: TERADATA-DIRECT, bare table names, no catalog prefix -- do NOT run through Starburst.
--
-- *** CRITICAL: DO NOT SUM THIS FILE'S ROWS ACROSS cohort_month TO REPRODUCE THE QUARTERLY NUMBER. ***
-- There is NO cross-cohort dedup here -- a client who appears in two cohorts (e.g. redeployed in both
-- May and June) is counted in BOTH cohort rows. Summing n_test/x_test/etc. across cohort_month
-- double-counts that client. The quarterly, client-deduped figure is value_capture_report_v2.sql,
-- which does the first-touch collapse across the whole window. This file is for per-cohort trend
-- reading only.
--
-- Grain: COHORT MONTH, no cross-cohort dedup -- treatmt_end_dt BETWEEN '2026-05-01' AND '2026-07-31'
--   (same in-scope population as the quarterly file, kept so both files cover the same clients), but
--   cohort_month is derived from the TREATMENT START date (treatmt_strt_dt / treatmt_start_dt), per
--   repo hard rule (cohort grain = treatmt_strt_dt). cohort_month CAST pattern copied verbatim from
--   blocks/pcl_sales_modal_block.sql and blocks/pcq_ms_block.sql (both already validated in-env). A
--   client is counted once PER COHORT they appear in -- the *_ft first-touch ROW_NUMBER machinery
--   from the quarterly file is REMOVED entirely. Success is scoped to the client's own rows WITHIN
--   that cohort (MAX over clnt_no, cohort_month), not ever-success across all cohorts.
-- all_rows contract (10 cols, teammate hook-up point): mne | cohort_month | arm_test | arm_ctrl
--   | n_test | x_test | n_ctrl | x_ctrl | n_unmapped | n_arm_conflict. arm_test/arm_ctrl = raw source
--   codes, never invented prose. n_unmapped = per-cohort distinct clients whose arm couldn't be
--   resolved (excluded from measurement). n_arm_conflict = per-cohort distinct clients seen in BOTH
--   arms within that same cohort_month (informational; still counted in both arms' cells).
-- PCQ stratifies by decile INTERNALLY (Simpson's-paradox guard): one row per (cohort_month, decile)
--   into all_rows, pooled by the stats layer into one row per (mne, cohort_month, arm_test, arm_ctrl)
--   -- decile itself never appears; emitted counts are simple sums while lift is MH-weighted, so the
--   two won't exactly reconcile for PCQ (expected, same as quarterly). n_unmapped/n_arm_conflict are
--   computed ONCE per (campaign, cohort_month) (not per decile), then LEFT JOINed onto every decile
--   row for that cohort identically; the pooling step aggregates them with MAX, not SUM, so the
--   decile fan-out never inflates them.
-- Stats: MH-weighted two-proportion z-test. sig flags = z-threshold checks (80/90/95%), no CDF/p-value
--   (Andre gets p via Excel NORM.S.DIST).

WITH

-- ---- PCL: population/arm/success flags verbatim from v2 quarterly, minus first-touch -------------
pcl_win AS (
  SELECT clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'test'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'control' END AS arm_role,
    responder_cli,
    CAST(CAST(treatmt_strt_dt AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7)) AS cohort_month
  FROM DL_MR_PROD.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window
),
pcl_succ AS (
  -- success scoped to the client's OWN rows within that cohort -- MAX per (clnt_no, cohort_month)
  SELECT clnt_no, cohort_month,
    MAX(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END) AS ever_responder_cli,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcl_win GROUP BY clnt_no, cohort_month
),
pcl_client AS (
  SELECT DISTINCT b.clnt_no, b.cohort_month, b.arm_role, s.ever_responder_cli, s.n_arms
  FROM pcl_win b JOIN pcl_succ s ON s.clnt_no = b.clnt_no AND s.cohort_month = b.cohort_month
),
pcl_cells AS (
  SELECT cohort_month,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                           AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_responder_cli = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                           AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_responder_cli = 1 THEN clnt_no END) AS control_successes
  FROM pcl_client GROUP BY cohort_month
),
pcl_conflict AS (
  -- clients seen in both arms WITHIN the same cohort_month; pcl_succ is already one row per
  -- (clnt_no, cohort_month), so COUNT(*) is exact -- no DISTINCT needed
  SELECT cohort_month, COUNT(*) AS n_arm_conflict
  FROM pcl_succ WHERE n_arms > 1 GROUP BY cohort_month
),
pcl_rows AS (
  -- UNION-width guard: first branch of all_rows below, VARCHAR widths here fix every later branch.
  SELECT CAST('PCL' AS VARCHAR(20)) AS mne,
    cells.cohort_month,
    CAST('R____WMS' AS VARCHAR(30)) AS arm_test, CAST('R____NMS' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(0 AS BIGINT) AS n_unmapped,                        -- no unmapped-code concept for PCL
    CAST(COALESCE(conf.n_arm_conflict, 0) AS BIGINT) AS n_arm_conflict
  FROM pcl_cells cells LEFT JOIN pcl_conflict conf ON conf.cohort_month = cells.cohort_month
),

-- ---- PCQ: population/arm/success flags verbatim from v2 quarterly, minus first-touch. Arm map is
-- an INLINE CASE -- never a FROM-less SELECT/UNION arm_map CTE; that pattern aborted Teradata before.
pcq_win AS (
  SELECT r.clnt_no, TRIM(r.test_group_latest) AS test_group_latest,
    CASE WHEN TRIM(r.test_group_latest) = 'NG3_CHMP'               THEN 'control'
         WHEN TRIM(r.test_group_latest) IN ('NG3_CHLN','NG3_CHLG') THEN 'test' END AS arm_role,
    CAST(r.model_score_decile AS VARCHAR(10)) AS decile,
    r.app_approved, r.asc_on_app_source,
    CAST(CAST(r.treatmt_start_dt AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7)) AS cohort_month
  FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
  WHERE r.decsn_year = 2026 AND r.tpa_ita = 'TPA'
    AND r.treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window
),
pcq_succ AS (
  -- success scoped to the client's OWN rows within that cohort -- MAX per (clnt_no, cohort_month)
  SELECT clnt_no, cohort_month,
    MAX(CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN 1 ELSE 0 END) AS ever_approved_asc,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcq_win GROUP BY clnt_no, cohort_month
),
pcq_client AS (
  SELECT DISTINCT b.clnt_no, b.cohort_month, b.arm_role, b.decile, s.ever_approved_asc, s.n_arms
  FROM pcq_win b JOIN pcq_succ s ON s.clnt_no = b.clnt_no AND s.cohort_month = b.cohort_month
  WHERE b.arm_role IS NOT NULL
),
pcq_cells AS (
  -- one row per (cohort_month, decile) -- internal stratification only, decile never reaches the output
  SELECT cohort_month, decile,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                          AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_approved_asc = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                          AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_approved_asc = 1 THEN clnt_no END) AS control_successes
  FROM pcq_client GROUP BY cohort_month, decile
),
pcq_unmapped AS (
  -- per (campaign, cohort_month), NOT per decile: distinct clients in that cohort whose
  -- test_group_latest never resolved to an arm. Computed once from pcq_win before stratification.
  SELECT cohort_month, COUNT(DISTINCT clnt_no) AS n_unmapped
  FROM pcq_win WHERE arm_role IS NULL GROUP BY cohort_month
),
pcq_conflict AS (
  -- per (campaign, cohort_month), NOT per decile: clients seen in both arms within that cohort_month
  SELECT cohort_month, COUNT(*) AS n_arm_conflict
  FROM pcq_succ WHERE n_arms > 1 GROUP BY cohort_month
),
pcq_rows AS (
  -- success = app_approved gated on Period-ASC (canon: numerator only, denominator = all targeted)
  SELECT CAST('PCQ' AS VARCHAR(20)) AS mne,
    cells.cohort_month,
    CAST('NG3_CHLN+NG3_CHLG' AS VARCHAR(30)) AS arm_test, CAST('NG3_CHMP' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(COALESCE(u.n_unmapped, 0) AS BIGINT) AS n_unmapped,
    CAST(COALESCE(c.n_arm_conflict, 0) AS BIGINT) AS n_arm_conflict
  FROM pcq_cells cells
  LEFT JOIN pcq_unmapped u ON u.cohort_month = cells.cohort_month
  LEFT JOIN pcq_conflict c ON c.cohort_month = cells.cohort_month
  -- fan-out note: decile rows within a cohort share identical n_unmapped/n_arm_conflict (per-cohort
  -- scalar, not per-decile) -- pooling below MUST aggregate them with MAX, never SUM
),

-- ==== TEAMMATE HOOK-UP POINT: add a new campaign as one more UNION ALL branch, same 10 columns ====
all_rows AS (
  SELECT * FROM pcl_rows
  UNION ALL SELECT * FROM pcq_rows
  -- UNION ALL SELECT * FROM <next_campaign>_rows
),

-- ---- stats layer: campaign-agnostic, sees only the contract. MH-weighted two-proportion z-test ----
strata_base AS (
  SELECT mne, cohort_month, arm_test, arm_ctrl, n_test AS n1, x_test AS x1, n_ctrl AS n0, x_ctrl AS x0,
    n_unmapped, n_arm_conflict,
    CAST(x_test AS FLOAT)/NULLIF(CAST(n_test AS FLOAT),0) - CAST(x_ctrl AS FLOAT)/NULLIF(CAST(n_ctrl AS FLOAT),0) AS d,
    (CAST(n_test AS FLOAT)*CAST(n_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS w,
    (CAST(x_test AS FLOAT)+CAST(x_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS pbar
  FROM all_rows
),
strata_v AS (
  SELECT mne, cohort_month, arm_test, arm_ctrl, n1, x1, n0, x0, n_unmapped, n_arm_conflict, d, w,
    pbar * (1 - pbar) * (
      (CASE WHEN n1 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n1 END) +
      (CASE WHEN n0 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n0 END)
    ) AS v
  FROM strata_base
),
pooled AS (
  -- GROUP BY collapses PCQ's per-decile strata into one MH-weighted row per (mne, cohort_month,
  -- arm_test, arm_ctrl); PCL has one stratum already. n_unmapped/n_arm_conflict are identical
  -- across every decile row for a given (campaign, cohort_month) (see pcq_rows note above), so MAX
  -- returns the correct scalar without decile-count inflation.
  SELECT mne, cohort_month, arm_test, arm_ctrl,
    SUM(n1) AS n_test, SUM(x1) AS x_test, SUM(n0) AS n_ctrl, SUM(x0) AS x_ctrl,
    SUM(w * d) / NULLIF(SUM(w), 0) AS lift, SQRT(SUM(w * w * v)) / NULLIF(SUM(w), 0) AS se,
    MAX(n_unmapped) AS n_unmapped, MAX(n_arm_conflict) AS n_arm_conflict
  FROM strata_v GROUP BY mne, cohort_month, arm_test, arm_ctrl
),
stats AS (
  SELECT mne, cohort_month, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift, n_unmapped, n_arm_conflict,
    CASE WHEN se IS NULL OR se = 0 THEN NULL ELSE lift / se END AS z
  FROM pooled
)

-- ---- final output: one row per (campaign, cohort_month), stats + diagnostic columns together -------
-- rate_test/rate_ctrl are CRUDE (pooled counts). lift_pp for a stratified campaign (PCQ) is
-- MH-WEIGHTED across deciles, so rate_test - rate_ctrl will NOT equal lift_pp there. Expected.
-- Single SELECT, no UNION ALL at this level -- ORDER BY column names is safe here (the "positional
-- only" rule applies to a query with UNION ALL at the outer level, which this no longer has).
SELECT
  mne, cohort_month, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl,
  100.0 * CAST(x_test AS FLOAT) / NULLIF(CAST(n_test AS FLOAT), 0) AS rate_test_pct,
  100.0 * CAST(x_ctrl AS FLOAT) / NULLIF(CAST(n_ctrl AS FLOAT), 0) AS rate_ctrl_pct,
  lift * 100 AS lift_pp, z,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.2816 THEN 'Y' ELSE 'N' END AS sig80,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.6449 THEN 'Y' ELSE 'N' END AS sig90,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.9600 THEN 'Y' ELSE 'N' END AS sig95,
  n_unmapped, n_arm_conflict
FROM stats
ORDER BY mne, cohort_month;
