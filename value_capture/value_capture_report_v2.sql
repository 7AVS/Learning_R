-- value_capture/value_capture_report_v2.sql
-- Teradata-direct rebuild of value_capture_report.sql (v1). SQL counts clients only -- display
-- formatting, p-value, and partner-template mapping are Andre's job in Excel (REDESIGN_SPEC.md).
-- Engine: TERADATA-DIRECT, bare table names, no catalog prefix -- do NOT run through Starburst.
-- Grain: QUARTERLY, client-deduped -- treatmt_end_dt BETWEEN '2026-05-01' AND '2026-07-31',
--   first-touch collapse per clnt_no, ever-success over all in-window rows (v1 logic, unchanged).
-- all_rows contract (9 cols, teammate hook-up point): mne | arm_test | arm_ctrl | n_test | x_test
--   | n_ctrl | x_ctrl | n_unmapped | n_arm_conflict. arm_test/arm_ctrl = raw source codes, never
--   invented prose. n_unmapped = distinct in-window clients whose arm couldn't be resolved
--   (excluded from measurement). n_arm_conflict = distinct clients seen in both arms across waves,
--   resolved to first-touch (still counted once in n_test/n_ctrl -- this is informational).
-- PCQ stratifies by decile INTERNALLY (Simpson's-paradox guard): one row per decile into all_rows,
--   pooled by the stats layer into one output row -- decile itself never appears; emitted counts are
--   simple sums while lift is MH-weighted, so the two won't exactly reconcile for PCQ (expected).
--   n_unmapped/n_arm_conflict are computed ONCE per campaign (not per decile), then CROSS JOINed
--   onto every decile row identically; the pooling step aggregates them with MAX, not SUM, so the
--   decile fan-out never inflates them.
-- Stats: MH-weighted two-proportion z-test. sig flags = z-threshold checks (80/90/95%), no CDF/p-value
--   (Andre gets p via Excel NORM.S.DIST).

WITH

-- ---- PCL: population/arm/success flags verbatim from v1 -------------------------------------------
pcl_win AS (
  SELECT clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'test'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'control' END AS arm_role,
    decile,                                     -- first-touch tie-break only; PCL is single-stratum
    responder_cli, treatmt_strt_dt, treatmt_end_dt
  FROM DL_MR_PROD.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window
),
pcl_ft AS (
  SELECT clnt_no, arm_role,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt ASC, decile ASC) AS rn
  FROM pcl_win
),
pcl_succ AS (
  SELECT clnt_no, MAX(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END) AS ever_responder_cli,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcl_win GROUP BY clnt_no
),
pcl_client AS (
  SELECT f.clnt_no, f.arm_role, s.ever_responder_cli, s.n_arms
  FROM pcl_ft f JOIN pcl_succ s ON s.clnt_no = f.clnt_no WHERE f.rn = 1
),
pcl_cells AS (
  SELECT
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                           AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_responder_cli = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                           AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_responder_cli = 1 THEN clnt_no END) AS control_successes
  FROM pcl_client
),
pcl_conflict AS (
  -- clients seen in both arms, resolved to first-touch; PCL is already one row per client here
  SELECT COUNT(*) AS n_arm_conflict FROM pcl_client WHERE n_arms > 1
),
pcl_rows AS (
  -- UNION-width guard: first branch of all_rows below, VARCHAR widths here fix every later branch.
  SELECT CAST('PCL' AS VARCHAR(20)) AS mne,
    CAST('R____WMS' AS VARCHAR(30)) AS arm_test, CAST('R____NMS' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(0 AS BIGINT) AS n_unmapped,                        -- no unmapped-code concept for PCL
    CAST(conf.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM pcl_cells cells CROSS JOIN pcl_conflict conf
),

-- ---- PCQ: population/arm/success flags verbatim from v1. Arm map is an INLINE CASE -- never a
-- FROM-less SELECT/UNION arm_map CTE; that pattern aborted Teradata previously. --------------------
pcq_win AS (
  SELECT r.clnt_no, TRIM(r.test_group_latest) AS test_group_latest,
    CASE WHEN TRIM(r.test_group_latest) = 'NG3_CHMP'               THEN 'control'
         WHEN TRIM(r.test_group_latest) IN ('NG3_CHLN','NG3_CHLG') THEN 'test' END AS arm_role,
    CAST(r.model_score_decile AS VARCHAR(10)) AS decile,
    r.treatmt_start_dt, r.treatmt_end_dt, r.app_approved, r.asc_on_app_source
  FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
  WHERE r.decsn_year = 2026 AND r.tpa_ita = 'TPA'
    AND r.treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window
),
pcq_ft AS (
  SELECT clnt_no, arm_role, decile,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_start_dt ASC, decile ASC) AS rn
  FROM pcq_win WHERE arm_role IS NOT NULL
),
pcq_succ AS (
  SELECT clnt_no,
    MAX(CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN 1 ELSE 0 END) AS ever_approved_asc,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcq_win GROUP BY clnt_no
),
pcq_client AS (
  SELECT f.clnt_no, f.arm_role, f.decile, s.ever_approved_asc, s.n_arms
  FROM pcq_ft f JOIN pcq_succ s ON s.clnt_no = f.clnt_no WHERE f.rn = 1
),
pcq_cells AS (
  -- one row per decile -- internal stratification only, decile never reaches the output
  SELECT decile,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                          AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_approved_asc = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                          AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_approved_asc = 1 THEN clnt_no END) AS control_successes
  FROM pcq_client GROUP BY decile
),
pcq_unmapped AS (
  -- campaign-level, NOT per decile: distinct clients whose test_group_latest never resolved to an
  -- arm (NG3_CHLD and anything else unrecognised). Computed once from pcq_win before stratification.
  SELECT COUNT(DISTINCT clnt_no) AS n_unmapped FROM pcq_win WHERE arm_role IS NULL
),
pcq_conflict AS (
  -- campaign-level, NOT per decile: clients seen in both arms across waves, resolved to first-touch
  SELECT COUNT(*) AS n_arm_conflict FROM pcq_client WHERE n_arms > 1
),
pcq_rows AS (
  -- success = app_approved gated on Period-ASC (canon: numerator only, denominator = all targeted)
  SELECT CAST('PCQ' AS VARCHAR(20)) AS mne,
    CAST('NG3_CHLN+NG3_CHLG' AS VARCHAR(30)) AS arm_test, CAST('NG3_CHMP' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(u.n_unmapped AS BIGINT) AS n_unmapped, CAST(c.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM pcq_cells cells CROSS JOIN pcq_unmapped u CROSS JOIN pcq_conflict c
  -- NOTE: this CROSS JOIN repeats the campaign-level n_unmapped/n_arm_conflict onto every decile
  -- row identically (fan-out is intentional here) -- the pooled CTE below MUST aggregate these two
  -- columns with MAX, never SUM, or the decile count would inflate them.
),

-- ==== TEAMMATE HOOK-UP POINT: add a new campaign as one more UNION ALL branch, same 9 columns ====
all_rows AS (
  SELECT * FROM pcl_rows
  UNION ALL SELECT * FROM pcq_rows
  -- UNION ALL SELECT * FROM <next_campaign>_rows
),

-- ---- stats layer: campaign-agnostic, sees only the contract. MH-weighted two-proportion z-test ----
strata_base AS (
  SELECT mne, arm_test, arm_ctrl, n_test AS n1, x_test AS x1, n_ctrl AS n0, x_ctrl AS x0,
    n_unmapped, n_arm_conflict,
    CAST(x_test AS FLOAT)/NULLIF(CAST(n_test AS FLOAT),0) - CAST(x_ctrl AS FLOAT)/NULLIF(CAST(n_ctrl AS FLOAT),0) AS d,
    (CAST(n_test AS FLOAT)*CAST(n_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS w,
    (CAST(x_test AS FLOAT)+CAST(x_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS pbar
  FROM all_rows
),
strata_v AS (
  SELECT mne, arm_test, arm_ctrl, n1, x1, n0, x0, n_unmapped, n_arm_conflict, d, w,
    pbar * (1 - pbar) * (
      (CASE WHEN n1 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n1 END) +
      (CASE WHEN n0 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n0 END)
    ) AS v
  FROM strata_base
),
pooled AS (
  -- GROUP BY collapses PCQ's per-decile strata into one MH-weighted row; PCL has one stratum already.
  -- n_unmapped/n_arm_conflict are identical across every decile row for a given campaign (see
  -- pcq_rows note above), so MAX returns the correct campaign scalar without decile-count inflation.
  SELECT mne, arm_test, arm_ctrl,
    SUM(n1) AS n_test, SUM(x1) AS x_test, SUM(n0) AS n_ctrl, SUM(x0) AS x_ctrl,
    SUM(w * d) / NULLIF(SUM(w), 0) AS lift, SQRT(SUM(w * w * v)) / NULLIF(SUM(w), 0) AS se,
    MAX(n_unmapped) AS n_unmapped, MAX(n_arm_conflict) AS n_arm_conflict
  FROM strata_v GROUP BY mne, arm_test, arm_ctrl
),
stats AS (
  SELECT mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift, n_unmapped, n_arm_conflict,
    CASE WHEN se IS NULL OR se = 0 THEN NULL ELSE lift / se END AS z
  FROM pooled
)

-- ---- final output: one row per campaign, stats + diagnostic columns together -----------------------
-- rate_test/rate_ctrl are CRUDE (pooled counts). lift_pp for a stratified campaign (PCQ) is
-- MH-WEIGHTED across deciles, so rate_test - rate_ctrl will NOT equal lift_pp there. Expected.
-- Single SELECT, no UNION ALL at this level -- ORDER BY column names is safe here (the "positional
-- only" rule applies to a query with UNION ALL at the outer level, which this no longer has).
SELECT
  mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl,
  100.0 * CAST(x_test AS FLOAT) / NULLIF(CAST(n_test AS FLOAT), 0) AS rate_test_pct,
  100.0 * CAST(x_ctrl AS FLOAT) / NULLIF(CAST(n_ctrl AS FLOAT), 0) AS rate_ctrl_pct,
  lift * 100 AS lift_pp, z,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.2816 THEN 'Y' ELSE 'N' END AS sig80,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.6449 THEN 'Y' ELSE 'N' END AS sig90,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.9600 THEN 'Y' ELSE 'N' END AS sig95,
  n_unmapped, n_arm_conflict
FROM stats
ORDER BY mne, arm_test;
